use crate::error::{QuePaxaError, Result};
use crate::network::batch::BatchService;
use crate::network::metrics::NetworkMetrics;
use crate::network::transport::{read_frame, write_frame};
use crate::network::wire::{
    DeploymentId, NodeRequest, NodeResponse, PeerSender, RequestBody, ResponseBody, Submission,
    SubmissionOutcome, WIRE_VERSION, WireError, WireErrorCode,
};
use crate::proposer::RecorderClient;
use crate::types::Decision;
use crate::types::ReplicaId;
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_util::sync::CancellationToken;

const DEFAULT_MAX_FRAME: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerIdentity {
    Replica(ReplicaId),
    Client([u8; 16]),
}

impl PeerIdentity {
    fn matches_sender(&self, sender: &PeerSender) -> bool {
        matches!(
            (self, sender),
            (Self::Replica(left), PeerSender::Replica(right)) if left == right
        ) || matches!(
            (self, sender),
            (Self::Client(left), PeerSender::Client(right)) if left == right
        )
    }
}

/// Live certificate-to-protocol-identity allowlist. TLS chain validation still
/// comes from the rustls configuration; this registry supports leaf rotation
/// and immediate revocation without restarting the node listener.
#[derive(Clone)]
pub struct PeerRegistry {
    peers: Arc<RwLock<BTreeMap<Vec<u8>, PeerIdentity>>>,
}

/// Hot-swappable rustls server configuration. Replacements affect new
/// connections; existing TLS sessions finish under the configuration with
/// which they were accepted.
#[derive(Clone)]
pub struct ServerTlsRegistry {
    config: Arc<RwLock<Arc<ServerConfig>>>,
}

impl ServerTlsRegistry {
    fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
        }
    }

    pub fn replace(&self, config: Arc<ServerConfig>) {
        *self
            .config
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = config;
    }

    fn current(&self) -> Arc<ServerConfig> {
        Arc::clone(
            &self
                .config
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

impl PeerRegistry {
    pub fn new(peers: BTreeMap<Vec<u8>, PeerIdentity>) -> Result<Self> {
        if peers.is_empty() {
            return Err(QuePaxaError::TransportError(
                "at least one authenticated peer certificate is required".into(),
            ));
        }
        Ok(Self {
            peers: Arc::new(RwLock::new(peers)),
        })
    }

    pub fn replace(&self, peers: BTreeMap<Vec<u8>, PeerIdentity>) -> Result<()> {
        if peers.is_empty() {
            return Err(QuePaxaError::TransportError(
                "peer certificate registry cannot be empty".into(),
            ));
        }
        *self
            .peers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = peers;
        Ok(())
    }

    pub fn authorize(&self, certificate_der: Vec<u8>, identity: PeerIdentity) {
        self.peers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(certificate_der, identity);
    }

    pub fn revoke(&self, certificate_der: &[u8]) -> bool {
        self.peers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(certificate_der)
            .is_some()
    }

    pub fn snapshot(&self) -> BTreeMap<Vec<u8>, PeerIdentity> {
        self.peers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn identity_for(&self, certificate_der: &[u8]) -> Option<PeerIdentity> {
        self.peers
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(certificate_der)
            .cloned()
    }
}

pub type BoxSubmissionFuture<'a, V> = BoxFuture<'a, Result<SubmissionOutcome<V>>>;

pub trait SubmissionHandler<V>: Send + Sync + 'static {
    fn submit(&self, submission: Submission<V>) -> BoxSubmissionFuture<'_, V>;

    fn receive_decisions(&self, _decisions: Vec<Decision<V>>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

pub struct FnSubmissionHandler<F>(pub F);

impl<V, F> SubmissionHandler<V> for FnSubmissionHandler<F>
where
    V: 'static,
    F: Fn(Submission<V>) -> BoxSubmissionFuture<'static, V> + Send + Sync + 'static,
{
    fn submit(&self, submission: Submission<V>) -> BoxSubmissionFuture<'_, V> {
        (self.0)(submission)
    }
}

pub struct NetworkNodeServer<V, C, H> {
    listener: TcpListener,
    deployment: DeploymentId,
    tls: ServerTlsRegistry,
    peers: PeerRegistry,
    recorder: Arc<Mutex<C>>,
    submission_handler: Arc<H>,
    batch_service: Option<Arc<dyn BatchService<V>>>,
    max_frame: usize,
    max_connections: usize,
    metrics: Arc<NetworkMetrics>,
    marker: std::marker::PhantomData<fn(V)>,
}

impl<V, C, H> NetworkNodeServer<V, C, H>
where
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    C: RecorderClient<V> + Send + 'static,
    H: SubmissionHandler<V>,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn bind(
        address: SocketAddr,
        deployment: DeploymentId,
        tls: Arc<ServerConfig>,
        peers_by_certificate: BTreeMap<Vec<u8>, PeerIdentity>,
        recorder: C,
        submission_handler: H,
        metrics: Arc<NetworkMetrics>,
    ) -> Result<Self> {
        let peers = PeerRegistry::new(peers_by_certificate)?;
        let listener = TcpListener::bind(address).await.map_err(|error| {
            QuePaxaError::TransportError(format!("could not bind node listener: {error}"))
        })?;
        Ok(Self {
            listener,
            deployment,
            tls: ServerTlsRegistry::new(tls),
            peers,
            recorder: Arc::new(Mutex::new(recorder)),
            submission_handler: Arc::new(submission_handler),
            batch_service: None,
            max_frame: DEFAULT_MAX_FRAME,
            max_connections: 1024,
            metrics,
            marker: std::marker::PhantomData,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(|error| {
            QuePaxaError::TransportError(format!("could not read node listener address: {error}"))
        })
    }

    pub fn peer_registry(&self) -> PeerRegistry {
        self.peers.clone()
    }

    pub fn tls_registry(&self) -> ServerTlsRegistry {
        self.tls.clone()
    }

    pub fn with_max_frame(mut self, max_frame: usize) -> Result<Self> {
        if max_frame < 128 || max_frame > u32::MAX as usize {
            return Err(QuePaxaError::TransportError(
                "maximum frame must be between 128 bytes and u32::MAX".into(),
            ));
        }
        self.max_frame = max_frame;
        Ok(self)
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Result<Self> {
        if max_connections == 0 {
            return Err(QuePaxaError::TransportError(
                "maximum concurrent connections must be non-zero".into(),
            ));
        }
        self.max_connections = max_connections;
        Ok(self)
    }

    /// Enables authenticated batch dissemination on this node. Clients and
    /// replicas may publish verified batches; only replicas may fetch them.
    pub fn with_batch_service<B>(mut self, service: B) -> Self
    where
        B: BatchService<V>,
    {
        self.batch_service = Some(Arc::new(service));
        self
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let mut connections = JoinSet::new();
        let connection_limit = Arc::new(Semaphore::new(self.max_connections));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = self.listener.accept() => {
                    let (stream, peer_address) = accepted.map_err(|error| {
                        QuePaxaError::TransportError(format!("node accept failed: {error}"))
                    })?;
                    self.metrics.connection_accepted();
                    let Ok(permit) = Arc::clone(&connection_limit).try_acquire_owned() else {
                        self.metrics.request_failed();
                        tracing::warn!(%peer_address, "node connection limit reached");
                        continue;
                    };
                    let context = ConnectionContext {
                        deployment: self.deployment,
                        acceptor: TlsAcceptor::from(self.tls.current()),
                        peers: self.peers.clone(),
                        recorder: Arc::clone(&self.recorder),
                        submission_handler: Arc::clone(&self.submission_handler),
                        batch_service: self.batch_service.clone(),
                        max_frame: self.max_frame,
                        metrics: Arc::clone(&self.metrics),
                        marker: std::marker::PhantomData,
                    };
                    connections.spawn(async move {
                        let _permit = permit;
                        if let Err(error) = context.serve(stream).await {
                            tracing::warn!(%peer_address, %error, "node connection failed");
                        }
                    });
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        tracing::warn!(%error, "node connection task panicked");
                    }
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        Ok(())
    }
}

struct ConnectionContext<V, C, H> {
    deployment: DeploymentId,
    acceptor: TlsAcceptor,
    peers: PeerRegistry,
    recorder: Arc<Mutex<C>>,
    submission_handler: Arc<H>,
    batch_service: Option<Arc<dyn BatchService<V>>>,
    max_frame: usize,
    metrics: Arc<NetworkMetrics>,
    marker: std::marker::PhantomData<fn(V)>,
}

impl<V, C, H> ConnectionContext<V, C, H>
where
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    C: RecorderClient<V> + Send + 'static,
    H: SubmissionHandler<V>,
{
    async fn serve(&self, stream: TcpStream) -> Result<()> {
        let mut stream = self.acceptor.accept(stream).await.map_err(|error| {
            self.metrics.authentication_failed();
            QuePaxaError::TransportError(format!("mutual TLS handshake failed: {error}"))
        })?;
        let peer_certificate = stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certificates| certificates.first())
            .ok_or_else(|| {
                self.metrics.authentication_failed();
                QuePaxaError::TransportError("TLS peer did not present a certificate".into())
            })?;
        let peer = self
            .peers
            .identity_for(peer_certificate.as_ref())
            .ok_or_else(|| {
                self.metrics.authentication_failed();
                QuePaxaError::TransportError(
                    "TLS peer certificate is not bound to a configured identity".into(),
                )
            })?;

        let request: NodeRequest<V> =
            read_frame(&mut stream, self.max_frame, &self.metrics).await?;
        let request_id = request.request_id;
        let response = match self.dispatch(peer, request).await {
            Ok(body) => body,
            Err(error) => {
                self.metrics.request_failed();
                ResponseBody::Error(error_to_wire(error))
            }
        };
        write_frame(
            &mut stream,
            &NodeResponse {
                version: WIRE_VERSION,
                deployment: self.deployment,
                request_id,
                body: response,
            },
            self.max_frame,
            &self.metrics,
        )
        .await?;
        Ok(())
    }

    async fn dispatch(
        &self,
        peer: PeerIdentity,
        request: NodeRequest<V>,
    ) -> Result<ResponseBody<V>> {
        if request.version != WIRE_VERSION {
            return Err(QuePaxaError::TransportError(format!(
                "unsupported wire version {}",
                request.version
            )));
        }
        if request.deployment != self.deployment {
            return Err(QuePaxaError::ConfigurationMismatch);
        }
        if !peer.matches_sender(&request.sender) {
            return Err(QuePaxaError::TransportError(
                "authenticated peer does not match the protocol sender".into(),
            ));
        }

        match (peer, request.body) {
            (PeerIdentity::Replica(replica), RequestBody::Record(record)) => {
                if record.sender != replica {
                    return Err(QuePaxaError::InvalidSender(record.sender));
                }
                let recorder = Arc::clone(&self.recorder);
                let reply = tokio::task::spawn_blocking(move || {
                    recorder
                        .lock()
                        .map_err(|_| {
                            QuePaxaError::TransportError("recorder lock was poisoned".into())
                        })?
                        .record(record)
                })
                .await
                .map_err(|error| {
                    QuePaxaError::TransportError(format!("recorder task failed: {error}"))
                })??;
                Ok(ResponseBody::Record(reply))
            }
            (PeerIdentity::Replica(_), RequestBody::Status(slot)) => {
                let recorder = Arc::clone(&self.recorder);
                let step = tokio::task::spawn_blocking(move || {
                    recorder
                        .lock()
                        .map_err(|_| {
                            QuePaxaError::TransportError("recorder lock was poisoned".into())
                        })?
                        .status(slot)
                })
                .await
                .map_err(|error| {
                    QuePaxaError::TransportError(format!("recorder task failed: {error}"))
                })??;
                Ok(ResponseBody::Status(step))
            }
            (PeerIdentity::Replica(_), RequestBody::InformDecisions(decisions)) => {
                let handler_decisions = decisions.clone();
                let recorder = Arc::clone(&self.recorder);
                tokio::task::spawn_blocking(move || {
                    recorder
                        .lock()
                        .map_err(|_| {
                            QuePaxaError::TransportError("recorder lock was poisoned".into())
                        })?
                        .inform_decisions(&decisions)
                })
                .await
                .map_err(|error| {
                    QuePaxaError::TransportError(format!("recorder task failed: {error}"))
                })??;
                self.submission_handler
                    .receive_decisions(handler_decisions)
                    .await?;
                Ok(ResponseBody::Ack)
            }
            (PeerIdentity::Replica(_), RequestBody::InstallMembership(change)) => {
                let recorder = Arc::clone(&self.recorder);
                tokio::task::spawn_blocking(move || {
                    recorder
                        .lock()
                        .map_err(|_| {
                            QuePaxaError::TransportError("recorder lock was poisoned".into())
                        })?
                        .install_membership(change)
                })
                .await
                .map_err(|error| {
                    QuePaxaError::TransportError(format!(
                        "membership installation task failed: {error}"
                    ))
                })??;
                Ok(ResponseBody::Ack)
            }
            (
                PeerIdentity::Replica(_) | PeerIdentity::Client(_),
                RequestBody::PublishBatches(batches),
            ) => {
                let service = self.batch_service.clone().ok_or_else(|| {
                    QuePaxaError::TransportError("batch service is not configured".into())
                })?;
                tokio::task::spawn_blocking(move || service.publish(batches))
                    .await
                    .map_err(|error| {
                        QuePaxaError::TransportError(format!("batch publish task failed: {error}"))
                    })??;
                Ok(ResponseBody::Ack)
            }
            (PeerIdentity::Replica(_), RequestBody::FetchBatches(value_ids)) => {
                let service = self.batch_service.clone().ok_or_else(|| {
                    QuePaxaError::TransportError("batch service is not configured".into())
                })?;
                let batches = tokio::task::spawn_blocking(move || service.fetch(value_ids))
                    .await
                    .map_err(|error| {
                        QuePaxaError::TransportError(format!("batch fetch task failed: {error}"))
                    })??;
                Ok(ResponseBody::Batches(batches))
            }
            (PeerIdentity::Client(client_id), RequestBody::Submit(submission)) => {
                if submission.client_id != client_id || submission.value_ids.is_empty() {
                    return Err(QuePaxaError::InvalidProposal(
                        "submission identity does not match its authenticated client".into(),
                    ));
                }
                self.submission_handler
                    .submit(submission)
                    .await
                    .map(ResponseBody::Submission)
            }
            (_, RequestBody::Ping) => Ok(ResponseBody::Pong),
            _ => Err(QuePaxaError::TransportError(
                "authenticated peer is not authorized for this request type".into(),
            )),
        }
    }
}

fn error_to_wire(error: QuePaxaError) -> WireError {
    let code = match error {
        QuePaxaError::InvalidSender(_)
        | QuePaxaError::InvalidRecorderReply { .. }
        | QuePaxaError::DuplicateRecorderReply(_) => WireErrorCode::Authentication,
        QuePaxaError::ConfigurationMismatch
        | QuePaxaError::InvalidReconfiguration(_)
        | QuePaxaError::ConfigurationEpochOverflow
        | QuePaxaError::InvalidQuorum { .. }
        | QuePaxaError::UnknownReplica(_) => WireErrorCode::Configuration,
        QuePaxaError::ResourceLimit { .. } | QuePaxaError::StepLimitExceeded { .. } => {
            WireErrorCode::ResourceLimit
        }
        QuePaxaError::StorageError(_) => WireErrorCode::Storage,
        QuePaxaError::TransportError(_) | QuePaxaError::QuorumNotReached { .. } => {
            WireErrorCode::Unavailable
        }
        QuePaxaError::ConflictingDecision { .. } => WireErrorCode::Conflict,
        QuePaxaError::ScheduleMismatch { .. } => WireErrorCode::ScheduleMismatch,
        QuePaxaError::SlotPruned { .. } => WireErrorCode::Pruned,
        QuePaxaError::EmptyCluster
        | QuePaxaError::EmptyProposal
        | QuePaxaError::EmptyDecision
        | QuePaxaError::MissingProposal
        | QuePaxaError::MissingValue
        | QuePaxaError::DuplicateReplica(_)
        | QuePaxaError::InvalidStep { .. }
        | QuePaxaError::InvalidProposal(_)
        | QuePaxaError::SlotAlreadyDecided { .. } => WireErrorCode::InvalidRequest,
        QuePaxaError::StepOverflow | QuePaxaError::SlotOverflow | QuePaxaError::PolicyError(_) => {
            WireErrorCode::Internal
        }
    };
    WireError {
        code,
        message: error.to_string(),
    }
}
