use crate::error::{QuePaxaError, Result};
use crate::network::async_proposer::AsyncRecorderClient;
use crate::network::batch::{Batch, BatchVerifier};
use crate::network::metrics::NetworkMetrics;
use crate::network::wire::{
    DeploymentId, NodeRequest, NodeResponse, PeerSender, RequestBody, ResponseBody, Submission,
    SubmissionOutcome, WIRE_VERSION, WireError, WireErrorCode,
};
use crate::store::ValueFetcher;
use crate::types::{Decision, MembershipChange, RecordReply, RecordRequest, ReplicaId, SlotIndex};
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::pki_types::ServerName;

const DEFAULT_MAX_FRAME: usize = 8 * 1024 * 1024;
/// Transport-level bound on one RPC. This is not a consensus timeout: an
/// expired call is simply retried by the quorum layer, never treated as a
/// leader-failure signal. It restores the paper's eventual-delivery assumption
/// when a TCP connection wedges without erroring.
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Maps a remote wire error onto the local error type, preserving the typed
/// safety signals that quorum collection must not retry past.
fn wire_error_to_local(error: WireError, slot: Option<SlotIndex>) -> QuePaxaError {
    match (error.code, slot) {
        (WireErrorCode::Conflict, Some(slot)) => QuePaxaError::ConflictingDecision { slot },
        (WireErrorCode::ScheduleMismatch, Some(slot)) => QuePaxaError::ScheduleMismatch { slot },
        (WireErrorCode::Pruned, Some(slot)) => QuePaxaError::SlotPruned { slot },
        _ => QuePaxaError::TransportError(format!(
            "remote node rejected request ({:?}): {}",
            error.code, error.message
        )),
    }
}

#[derive(Clone)]
struct TlsRpcClient {
    deployment: DeploymentId,
    sender: PeerSender,
    address: SocketAddr,
    server_name: String,
    expected_peer_certificate: Vec<u8>,
    tls: Arc<ClientConfig>,
    next_request_id: Arc<AtomicU64>,
    max_frame: usize,
    rpc_timeout: Duration,
    metrics: Arc<NetworkMetrics>,
}

impl TlsRpcClient {
    async fn call<V>(&self, body: RequestBody<V>) -> Result<ResponseBody<V>>
    where
        V: Serialize + DeserializeOwned,
    {
        tokio::time::timeout(self.rpc_timeout, self.call_inner(body))
            .await
            .map_err(|_| {
                QuePaxaError::TransportError(format!(
                    "recorder RPC to {} exceeded the transport timeout",
                    self.address
                ))
            })?
    }

    async fn call_inner<V>(&self, body: RequestBody<V>) -> Result<ResponseBody<V>>
    where
        V: Serialize + DeserializeOwned,
    {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request = NodeRequest {
            version: WIRE_VERSION,
            deployment: self.deployment,
            request_id,
            sender: self.sender.clone(),
            body,
        };
        let stream = TcpStream::connect(self.address).await.map_err(|error| {
            QuePaxaError::TransportError(format!(
                "could not connect to recorder {}: {error}",
                self.address
            ))
        })?;
        let server_name = ServerName::try_from(self.server_name.clone()).map_err(|error| {
            QuePaxaError::TransportError(format!("invalid TLS server name: {error}"))
        })?;
        let mut stream = TlsConnector::from(Arc::clone(&self.tls))
            .connect(server_name, stream)
            .await
            .map_err(|error| {
                QuePaxaError::TransportError(format!("TLS connection failed: {error}"))
            })?;
        let peer_certificate = stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certificates| certificates.first())
            .ok_or_else(|| {
                QuePaxaError::TransportError("TLS peer did not present a certificate".into())
            })?;
        if peer_certificate.as_ref() != self.expected_peer_certificate {
            return Err(QuePaxaError::TransportError(
                "TLS peer certificate does not match the configured recorder".into(),
            ));
        }

        write_frame(&mut stream, &request, self.max_frame, &self.metrics).await?;
        let response: NodeResponse<V> =
            read_frame(&mut stream, self.max_frame, &self.metrics).await?;
        if response.version != WIRE_VERSION
            || response.deployment != self.deployment
            || response.request_id != request_id
        {
            return Err(QuePaxaError::TransportError(
                "network response envelope does not match the request".into(),
            ));
        }
        Ok(response.body)
    }
}

#[derive(Clone)]
pub struct TlsRecorderClient {
    recorder_id: ReplicaId,
    rpc: TlsRpcClient,
}

impl TlsRecorderClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_replica: ReplicaId,
        recorder_id: ReplicaId,
        deployment: DeploymentId,
        address: SocketAddr,
        server_name: impl Into<String>,
        expected_peer_certificate: Vec<u8>,
        tls: Arc<ClientConfig>,
        metrics: Arc<NetworkMetrics>,
    ) -> Self {
        Self {
            recorder_id,
            rpc: TlsRpcClient {
                deployment,
                sender: PeerSender::Replica(local_replica),
                address,
                server_name: server_name.into(),
                expected_peer_certificate,
                tls,
                next_request_id: Arc::new(AtomicU64::new(1)),
                max_frame: DEFAULT_MAX_FRAME,
                rpc_timeout: DEFAULT_RPC_TIMEOUT,
                metrics,
            },
        }
    }

    pub fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.rpc.max_frame = max_frame;
        self
    }

    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc.rpc_timeout = timeout;
        self
    }

    pub async fn ping<V>(&self) -> Result<()>
    where
        V: Serialize + DeserializeOwned,
    {
        match self.rpc.call::<V>(RequestBody::Ping).await? {
            ResponseBody::Pong => Ok(()),
            ResponseBody::Error(error) => Err(wire_error_to_local(error, None)),
            _ => Err(QuePaxaError::TransportError(
                "node returned an unexpected health response".into(),
            )),
        }
    }
}

impl<V> AsyncRecorderClient<V> for TlsRecorderClient
where
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn id(&self) -> ReplicaId {
        self.recorder_id
    }

    fn record(&self, request: RecordRequest<V>) -> BoxFuture<'_, Result<RecordReply<V>>> {
        Box::pin(async move {
            let slot = request.slot;
            match self.rpc.call(RequestBody::Record(request)).await? {
                ResponseBody::Record(reply) => Ok(reply),
                ResponseBody::Error(error) => Err(wire_error_to_local(error, Some(slot))),
                _ => Err(QuePaxaError::TransportError(
                    "recorder returned an unexpected response type".into(),
                )),
            }
        })
    }

    fn inform_decisions(&self, decisions: Vec<Decision<V>>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let slot = decisions.first().map(|decision| decision.slot);
            match self
                .rpc
                .call(RequestBody::InformDecisions(decisions))
                .await?
            {
                ResponseBody::Ack => Ok(()),
                ResponseBody::Error(error) => Err(wire_error_to_local(error, slot)),
                _ => Err(QuePaxaError::TransportError(
                    "recorder returned an unexpected decision acknowledgement".into(),
                )),
            }
        })
    }

    fn status(&self, slot: SlotIndex) -> BoxFuture<'_, Result<Option<crate::Step>>> {
        Box::pin(async move {
            match self.rpc.call(RequestBody::<V>::Status(slot)).await? {
                ResponseBody::Status(step) => Ok(step),
                ResponseBody::Error(error) => Err(wire_error_to_local(error, Some(slot))),
                _ => Err(QuePaxaError::TransportError(
                    "recorder returned an unexpected status response".into(),
                )),
            }
        })
    }

    fn install_membership(&self, change: MembershipChange<V>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let slot = change.anchor.slot;
            match self
                .rpc
                .call(RequestBody::InstallMembership(change))
                .await?
            {
                ResponseBody::Ack => Ok(()),
                ResponseBody::Error(error) => Err(wire_error_to_local(error, Some(slot))),
                _ => Err(QuePaxaError::TransportError(
                    "recorder returned an unexpected membership acknowledgement".into(),
                )),
            }
        })
    }
}

/// mTLS client for the batch publish/fetch service. Constructors bind the
/// protocol sender to the certificate identity configured in the TLS client.
#[derive(Clone)]
pub struct TlsBatchClient {
    rpc: TlsRpcClient,
}

/// Publishes each batch to several authenticated nodes and returns only after
/// the configured durability threshold acknowledges it.
pub struct TlsBatchPublisher {
    clients: Vec<TlsBatchClient>,
    required_acknowledgements: usize,
}

impl TlsBatchPublisher {
    pub fn new(clients: Vec<TlsBatchClient>, required_acknowledgements: usize) -> Result<Self> {
        if clients.is_empty()
            || required_acknowledgements == 0
            || required_acknowledgements > clients.len()
        {
            return Err(QuePaxaError::InvalidQuorum {
                replicas: clients.len(),
                quorum: required_acknowledgements,
            });
        }
        Ok(Self {
            clients,
            required_acknowledgements,
        })
    }

    pub fn all(clients: Vec<TlsBatchClient>) -> Result<Self> {
        let required = clients.len();
        Self::new(clients, required)
    }

    pub async fn publish<V>(&self, batches: Vec<Batch<V>>) -> Result<()>
    where
        V: Clone + Serialize + DeserializeOwned,
    {
        let mut pending = FuturesUnordered::new();
        for client in &self.clients {
            pending.push(client.publish(batches.clone()));
        }
        let mut acknowledged = 0;
        while let Some(result) = pending.next().await {
            if result.is_ok() {
                acknowledged += 1;
                if acknowledged == self.required_acknowledgements {
                    return Ok(());
                }
            }
            if acknowledged + pending.len() < self.required_acknowledgements {
                break;
            }
        }
        Err(QuePaxaError::QuorumNotReached {
            needed: self.required_acknowledgements,
            received: acknowledged,
        })
    }
}

impl TlsBatchClient {
    #[allow(clippy::too_many_arguments)]
    pub fn for_replica(
        local_replica: ReplicaId,
        deployment: DeploymentId,
        address: SocketAddr,
        server_name: impl Into<String>,
        expected_peer_certificate: Vec<u8>,
        tls: Arc<ClientConfig>,
        metrics: Arc<NetworkMetrics>,
    ) -> Self {
        Self::new(
            PeerSender::Replica(local_replica),
            deployment,
            address,
            server_name,
            expected_peer_certificate,
            tls,
            metrics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn for_client(
        client_id: [u8; 16],
        deployment: DeploymentId,
        address: SocketAddr,
        server_name: impl Into<String>,
        expected_peer_certificate: Vec<u8>,
        tls: Arc<ClientConfig>,
        metrics: Arc<NetworkMetrics>,
    ) -> Self {
        Self::new(
            PeerSender::Client(client_id),
            deployment,
            address,
            server_name,
            expected_peer_certificate,
            tls,
            metrics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        sender: PeerSender,
        deployment: DeploymentId,
        address: SocketAddr,
        server_name: impl Into<String>,
        expected_peer_certificate: Vec<u8>,
        tls: Arc<ClientConfig>,
        metrics: Arc<NetworkMetrics>,
    ) -> Self {
        Self {
            rpc: TlsRpcClient {
                deployment,
                sender,
                address,
                server_name: server_name.into(),
                expected_peer_certificate,
                tls,
                next_request_id: Arc::new(AtomicU64::new(1)),
                max_frame: DEFAULT_MAX_FRAME,
                rpc_timeout: DEFAULT_RPC_TIMEOUT,
                metrics,
            },
        }
    }

    pub fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.rpc.max_frame = max_frame;
        self
    }

    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc.rpc_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    pub async fn publish<V>(&self, batches: Vec<Batch<V>>) -> Result<()>
    where
        V: Serialize + DeserializeOwned,
    {
        match self.rpc.call(RequestBody::PublishBatches(batches)).await? {
            ResponseBody::Ack => Ok(()),
            ResponseBody::Error(error) => Err(wire_error_to_local(error, None)),
            _ => Err(QuePaxaError::TransportError(
                "node returned an unexpected batch publish response".into(),
            )),
        }
    }

    /// Fetch is authorized by servers only for replica identities.
    pub async fn fetch<V>(&self, value_ids: Vec<V>) -> Result<Vec<Batch<V>>>
    where
        V: Serialize + DeserializeOwned,
    {
        match self.rpc.call(RequestBody::FetchBatches(value_ids)).await? {
            ResponseBody::Batches(batches) => Ok(batches),
            ResponseBody::Error(error) => Err(wire_error_to_local(error, None)),
            _ => Err(QuePaxaError::TransportError(
                "node returned an unexpected batch fetch response".into(),
            )),
        }
    }
}

/// Synchronous availability fetcher backed by the authenticated async batch
/// client. Recorder servers already run availability checks in a blocking
/// worker, so using the supplied Tokio handle here does not block an executor
/// worker thread. Payloads are verified again locally before becoming
/// available to consensus.
pub struct TlsBatchFetcher<V, F> {
    clients: Vec<TlsBatchClient>,
    runtime: tokio::runtime::Handle,
    verifier: F,
    marker: PhantomData<fn(V)>,
}

impl<V, F> TlsBatchFetcher<V, F> {
    pub fn new(client: TlsBatchClient, runtime: tokio::runtime::Handle, verifier: F) -> Self {
        Self {
            clients: vec![client],
            runtime,
            verifier,
            marker: PhantomData,
        }
    }

    pub fn with_fallbacks(mut self, clients: impl IntoIterator<Item = TlsBatchClient>) -> Self {
        self.clients.extend(clients);
        self
    }
}

impl<V, F> ValueFetcher<V, Vec<u8>> for TlsBatchFetcher<V, F>
where
    V: Clone + Ord + Serialize + DeserializeOwned + Send + 'static,
    F: BatchVerifier<V> + Send,
{
    fn fetch_values(&mut self, value_ids: &[V]) -> Result<Vec<(V, Vec<u8>)>> {
        let mut last_error = QuePaxaError::MissingValue;
        for client in &self.clients {
            let batches = match self.runtime.block_on(client.fetch(value_ids.to_vec())) {
                Ok(batches) => batches,
                Err(error) => {
                    last_error = error;
                    continue;
                }
            };
            let mut returned = Vec::with_capacity(batches.len());
            let mut valid = true;
            for batch in batches {
                if !value_ids.contains(&batch.value_id)
                    || returned
                        .iter()
                        .any(|(value_id, _)| value_id == &batch.value_id)
                    || self
                        .verifier
                        .verify(&batch.value_id, &batch.payload)
                        .is_err()
                {
                    valid = false;
                    break;
                }
                returned.push((batch.value_id, batch.payload));
            }
            if valid
                && value_ids
                    .iter()
                    .all(|requested| returned.iter().any(|(value_id, _)| value_id == requested))
            {
                return Ok(returned);
            }
            last_error = QuePaxaError::MissingValue;
        }
        Err(last_error)
    }
}

#[derive(Clone)]
pub struct TlsSubmitClient {
    client_id: [u8; 16],
    rpc: TlsRpcClient,
}

impl TlsSubmitClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_id: [u8; 16],
        deployment: DeploymentId,
        address: SocketAddr,
        server_name: impl Into<String>,
        expected_peer_certificate: Vec<u8>,
        tls: Arc<ClientConfig>,
        metrics: Arc<NetworkMetrics>,
    ) -> Self {
        Self {
            client_id,
            rpc: TlsRpcClient {
                deployment,
                sender: PeerSender::Client(client_id),
                address,
                server_name: server_name.into(),
                expected_peer_certificate,
                tls,
                next_request_id: Arc::new(AtomicU64::new(1)),
                max_frame: DEFAULT_MAX_FRAME,
                rpc_timeout: DEFAULT_RPC_TIMEOUT,
                metrics,
            },
        }
    }

    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc.rpc_timeout = timeout;
        self
    }

    pub async fn submit<V>(
        &self,
        request_id: u64,
        value_ids: Vec<V>,
    ) -> Result<SubmissionOutcome<V>>
    where
        V: Serialize + DeserializeOwned,
    {
        let submission = Submission {
            client_id: self.client_id,
            request_id,
            value_ids,
        };
        match self.rpc.call(RequestBody::Submit(submission)).await? {
            ResponseBody::Submission(outcome) => Ok(outcome),
            ResponseBody::Error(error) => Err(wire_error_to_local(error, None)),
            _ => Err(QuePaxaError::TransportError(
                "node returned an unexpected submission response".into(),
            )),
        }
    }

    pub async fn ping<V>(&self) -> Result<()>
    where
        V: Serialize + DeserializeOwned,
    {
        match self.rpc.call::<V>(RequestBody::Ping).await? {
            ResponseBody::Pong => Ok(()),
            ResponseBody::Error(error) => Err(wire_error_to_local(error, None)),
            _ => Err(QuePaxaError::TransportError(
                "node returned an unexpected health response".into(),
            )),
        }
    }
}

pub(crate) async fn write_frame<T, W>(
    writer: &mut W,
    value: &T,
    max_frame: usize,
    metrics: &NetworkMetrics,
) -> Result<usize>
where
    T: Serialize,
    W: AsyncWrite + Unpin,
{
    let bytes = postcard::to_allocvec(value).map_err(|error| {
        QuePaxaError::TransportError(format!("could not encode network frame: {error}"))
    })?;
    if bytes.is_empty() || bytes.len() > max_frame || bytes.len() > u32::MAX as usize {
        return Err(QuePaxaError::TransportError(format!(
            "network frame size {} is outside the accepted range",
            bytes.len()
        )));
    }
    writer
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|error| {
            QuePaxaError::TransportError(format!("could not write frame length: {error}"))
        })?;
    writer.write_all(&bytes).await.map_err(|error| {
        QuePaxaError::TransportError(format!("could not write frame body: {error}"))
    })?;
    writer
        .flush()
        .await
        .map_err(|error| QuePaxaError::TransportError(format!("could not flush frame: {error}")))?;
    metrics.response_sent(bytes.len() + 4);
    Ok(bytes.len() + 4)
}

pub(crate) async fn read_frame<T, R>(
    reader: &mut R,
    max_frame: usize,
    metrics: &NetworkMetrics,
) -> Result<T>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin,
{
    let length = reader.read_u32().await.map_err(|error| {
        QuePaxaError::TransportError(format!("could not read frame length: {error}"))
    })? as usize;
    if length == 0 || length > max_frame {
        return Err(QuePaxaError::TransportError(format!(
            "network frame size {length} is outside the accepted range"
        )));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await.map_err(|error| {
        QuePaxaError::TransportError(format!("could not read frame body: {error}"))
    })?;
    metrics.request_received(length + 4);
    postcard::from_bytes(&bytes).map_err(|error| {
        QuePaxaError::TransportError(format!("could not decode network frame: {error}"))
    })
}
