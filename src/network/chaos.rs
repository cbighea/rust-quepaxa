use crate::error::{QuePaxaError, Result};
use crate::network::async_proposer::AsyncRecorderClient;
use crate::network::report::write_json_atomic;
use crate::types::{Decision, MembershipChange, RecordReply, RecordRequest, ReplicaId, SlotIndex};
use futures_util::future::BoxFuture;
use serde::Serialize;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Deterministic WAN link shaping applied independently to both directions of
/// every proxied TCP connection. Whole-connection faults preserve TCP/TLS
/// stream semantics; byte duplication or byte reordering would only corrupt
/// TLS and therefore belong in [`RpcChaosProfile`] instead.
#[derive(Debug, Clone)]
pub struct WanLinkProfile {
    pub seed: u64,
    pub base_delay: Duration,
    pub jitter: Duration,
    pub bandwidth_bytes_per_second: Option<u64>,
    /// Deterministically reject every Nth connection. `None` disables it.
    pub fail_every: Option<u64>,
    /// Close a connection after forwarding this many bytes in one direction.
    pub reset_after_bytes: Option<u64>,
    /// Stop forwarding, without closing, after this many bytes. Useful for
    /// stalled-RPC and asymmetric-partition experiments.
    pub blackhole_after_bytes: Option<u64>,
}

impl WanLinkProfile {
    pub fn validate(&self) -> Result<()> {
        if self.bandwidth_bytes_per_second == Some(0)
            || self.fail_every == Some(0)
            || self.reset_after_bytes == Some(0)
            || self.blackhole_after_bytes == Some(0)
        {
            return Err(QuePaxaError::InvalidProposal(
                "WAN profile rates and fault intervals must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

impl Default for WanLinkProfile {
    fn default() -> Self {
        Self {
            seed: 1,
            base_delay: Duration::ZERO,
            jitter: Duration::ZERO,
            bandwidth_bytes_per_second: None,
            fail_every: None,
            reset_after_bytes: None,
            blackhole_after_bytes: None,
        }
    }
}

/// Transparent TCP proxy suitable for putting between the existing mTLS
/// clients and servers. Given the same profile and connection order, it
/// produces the same delays and connection faults on every run.
pub struct ReproducibleWanProxy {
    listener: TcpListener,
    target: SocketAddr,
    profile: WanLinkProfile,
    next_connection: AtomicU64,
    stats: Arc<WanProxyCounters>,
}

#[derive(Default)]
struct WanProxyCounters {
    accepted_connections: AtomicU64,
    failed_connections: AtomicU64,
    forwarded_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct WanProxyStats {
    pub accepted_connections: u64,
    pub failed_connections: u64,
    pub forwarded_bytes: u64,
}

#[derive(Clone)]
pub struct WanProxyObserver {
    counters: Arc<WanProxyCounters>,
}

impl WanProxyObserver {
    pub fn snapshot(&self) -> WanProxyStats {
        WanProxyStats {
            accepted_connections: self.counters.accepted_connections.load(Ordering::Relaxed),
            failed_connections: self.counters.failed_connections.load(Ordering::Relaxed),
            forwarded_bytes: self.counters.forwarded_bytes.load(Ordering::Relaxed),
        }
    }
}

impl ReproducibleWanProxy {
    pub async fn bind(
        listen: SocketAddr,
        target: SocketAddr,
        profile: WanLinkProfile,
    ) -> Result<Self> {
        profile.validate()?;
        let listener = TcpListener::bind(listen).await.map_err(|error| {
            QuePaxaError::TransportError(format!("could not bind chaos proxy: {error}"))
        })?;
        Ok(Self {
            listener,
            target,
            profile,
            next_connection: AtomicU64::new(1),
            stats: Arc::new(WanProxyCounters::default()),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(|error| {
            QuePaxaError::TransportError(format!("could not read chaos proxy address: {error}"))
        })
    }

    pub fn observer(&self) -> WanProxyObserver {
        WanProxyObserver {
            counters: Arc::clone(&self.stats),
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = self.listener.accept() => {
                    let (client, _) = accepted.map_err(|error| {
                        QuePaxaError::TransportError(format!("chaos proxy accept failed: {error}"))
                    })?;
                    let connection = self.next_connection.fetch_add(1, Ordering::Relaxed);
                    self.stats.accepted_connections.fetch_add(1, Ordering::Relaxed);
                    let target = self.target;
                    let profile = self.profile.clone();
                    let stats = Arc::clone(&self.stats);
                    connections.spawn(async move {
                        if proxy_connection(client, target, profile, connection, Arc::clone(&stats)).await.is_err() {
                            stats.failed_connections.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        return Err(QuePaxaError::TransportError(format!(
                            "chaos proxy task panicked: {error}"
                        )));
                    }
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        Ok(())
    }
}

async fn proxy_connection(
    client: TcpStream,
    target: SocketAddr,
    profile: WanLinkProfile,
    connection: u64,
    stats: Arc<WanProxyCounters>,
) -> Result<()> {
    if profile
        .fail_every
        .is_some_and(|interval| connection % interval == 0)
    {
        stats.failed_connections.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    let server = TcpStream::connect(target).await.map_err(|error| {
        QuePaxaError::TransportError(format!("chaos proxy target connect failed: {error}"))
    })?;
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    let outbound = copy_shaped(
        client_read,
        server_write,
        profile.clone(),
        mix64(profile.seed ^ connection ^ 0xa5a5_a5a5_a5a5_a5a5),
        Arc::clone(&stats),
    );
    let inbound_seed = mix64(profile.seed ^ connection ^ 0x5a5a_5a5a_5a5a_5a5a);
    let inbound = copy_shaped(server_read, client_write, profile, inbound_seed, stats);
    tokio::select! {
        result = outbound => result,
        result = inbound => result,
    }
}

async fn copy_shaped<R, W>(
    mut reader: R,
    mut writer: W,
    profile: WanLinkProfile,
    mut random: u64,
    stats: Arc<WanProxyCounters>,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer).await.map_err(|error| {
            QuePaxaError::TransportError(format!("chaos proxy read failed: {error}"))
        })?;
        if count == 0 {
            writer.shutdown().await.map_err(|error| {
                QuePaxaError::TransportError(format!("chaos proxy shutdown failed: {error}"))
            })?;
            return Ok(());
        }
        total = total.saturating_add(count as u64);
        if profile
            .blackhole_after_bytes
            .is_some_and(|limit| total >= limit)
        {
            std::future::pending::<()>().await;
        }
        if profile
            .reset_after_bytes
            .is_some_and(|limit| total >= limit)
        {
            return Ok(());
        }
        random = xorshift(random);
        let jitter = duration_mod(profile.jitter, random);
        let bandwidth_delay = profile
            .bandwidth_bytes_per_second
            .map_or(Duration::ZERO, |rate| {
                Duration::from_nanos(
                    ((count as u128 * 1_000_000_000_u128) / rate as u128).min(u64::MAX as u128)
                        as u64,
                )
            });
        tokio::time::sleep(
            profile
                .base_delay
                .saturating_add(jitter)
                .saturating_add(bandwidth_delay),
        )
        .await;
        writer.write_all(&buffer[..count]).await.map_err(|error| {
            QuePaxaError::TransportError(format!("chaos proxy write failed: {error}"))
        })?;
        stats
            .forwarded_bytes
            .fetch_add(count as u64, Ordering::Relaxed);
        writer.flush().await.map_err(|error| {
            QuePaxaError::TransportError(format!("chaos proxy flush failed: {error}"))
        })?;
    }
}

#[derive(Debug, Clone)]
pub struct WanHarnessLink {
    pub name: String,
    pub listen: SocketAddr,
    pub target: SocketAddr,
    pub profile: WanLinkProfile,
}

struct BoundWanHarnessLink {
    specification: WanHarnessLink,
    bound_address: SocketAddr,
    observer: WanProxyObserver,
    proxy: ReproducibleWanProxy,
}

pub struct ReproducibleWanHarness {
    links: Vec<BoundWanHarnessLink>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WanLinkReport {
    pub name: String,
    pub listen: String,
    pub target: String,
    pub seed: u64,
    pub stats: WanProxyStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WanHarnessReport {
    pub elapsed_millis: u64,
    pub links: Vec<WanLinkReport>,
}

impl WanHarnessReport {
    pub fn write_json(&self, path: impl Into<PathBuf>) -> Result<()> {
        write_json_atomic(self, path, "WAN report")
    }
}

impl ReproducibleWanHarness {
    pub async fn bind(links: Vec<WanHarnessLink>) -> Result<Self> {
        if links.is_empty() {
            return Err(QuePaxaError::InvalidProposal(
                "WAN harness requires at least one link".into(),
            ));
        }
        let names = links
            .iter()
            .map(|link| link.name.clone())
            .collect::<BTreeSet<_>>();
        if names.len() != links.len() || names.contains("") {
            return Err(QuePaxaError::InvalidProposal(
                "WAN harness link names must be non-empty and unique".into(),
            ));
        }
        let mut bound = Vec::with_capacity(links.len());
        for specification in links {
            let proxy = ReproducibleWanProxy::bind(
                specification.listen,
                specification.target,
                specification.profile.clone(),
            )
            .await?;
            bound.push(BoundWanHarnessLink {
                bound_address: proxy.local_addr()?,
                observer: proxy.observer(),
                proxy,
                specification,
            });
        }
        Ok(Self { links: bound })
    }

    pub fn endpoints(&self) -> Vec<(String, SocketAddr)> {
        self.links
            .iter()
            .map(|link| (link.specification.name.clone(), link.bound_address))
            .collect()
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<WanHarnessReport> {
        let started = std::time::Instant::now();
        let reports = self
            .links
            .iter()
            .map(|link| {
                (
                    link.specification.clone(),
                    link.bound_address,
                    link.observer.clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut tasks = JoinSet::new();
        for link in self.links {
            let shutdown = shutdown.clone();
            tasks.spawn(link.proxy.run(shutdown));
        }
        while let Some(result) = tasks.join_next().await {
            result.map_err(|error| {
                QuePaxaError::TransportError(format!("WAN harness task failed: {error}"))
            })??;
        }
        Ok(WanHarnessReport {
            elapsed_millis: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            links: reports
                .into_iter()
                .map(|(specification, bound_address, observer)| WanLinkReport {
                    name: specification.name,
                    listen: bound_address.to_string(),
                    target: specification.target.to_string(),
                    seed: specification.profile.seed,
                    stats: observer.snapshot(),
                })
                .collect(),
        })
    }
}

/// Decoded-RPC chaos settings. Delay creates deterministic cross-connection
/// reordering, while duplication exercises recorder idempotence without
/// corrupting the encrypted TCP stream.
#[derive(Debug, Clone)]
pub struct RpcChaosProfile {
    pub seed: u64,
    pub base_delay: Duration,
    pub jitter: Duration,
    pub drop_every: Option<u64>,
    pub duplicate_every: Option<u64>,
}

impl Default for RpcChaosProfile {
    fn default() -> Self {
        Self {
            seed: 1,
            base_delay: Duration::ZERO,
            jitter: Duration::ZERO,
            drop_every: None,
            duplicate_every: None,
        }
    }
}

/// Reproducible decoded-RPC fault layer. Wrapping a `TlsRecorderClient` keeps
/// authentication and encryption intact while adding faults that TCP itself
/// intentionally masks.
pub struct ChaosRecorderClient<C> {
    inner: Arc<C>,
    profile: RpcChaosProfile,
    sequence: Arc<AtomicU64>,
}

impl<C> Clone for ChaosRecorderClient<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            profile: self.profile.clone(),
            sequence: Arc::clone(&self.sequence),
        }
    }
}

impl<C> ChaosRecorderClient<C> {
    pub fn new(inner: C, profile: RpcChaosProfile) -> Result<Self> {
        if profile.drop_every == Some(0) || profile.duplicate_every == Some(0) {
            return Err(QuePaxaError::InvalidProposal(
                "RPC chaos intervals must be non-zero".into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(inner),
            profile,
            sequence: Arc::new(AtomicU64::new(1)),
        })
    }

    fn next(&self) -> (u64, Duration) {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let jitter = duration_mod(self.profile.jitter, mix64(self.profile.seed ^ sequence));
        (sequence, self.profile.base_delay + jitter)
    }
}

impl<V, C> AsyncRecorderClient<V> for ChaosRecorderClient<C>
where
    V: Clone + Send + Sync + 'static,
    C: AsyncRecorderClient<V> + 'static,
{
    fn id(&self) -> ReplicaId {
        self.inner.id()
    }

    fn record(&self, request: RecordRequest<V>) -> BoxFuture<'_, Result<RecordReply<V>>> {
        let (sequence, delay) = self.next();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            if self
                .profile
                .drop_every
                .is_some_and(|interval| sequence % interval == 0)
            {
                return Err(QuePaxaError::TransportError(
                    "RPC dropped by reproducible chaos profile".into(),
                ));
            }
            let duplicate = self
                .profile
                .duplicate_every
                .is_some_and(|interval| sequence % interval == 0);
            let reply = self.inner.record(request.clone()).await?;
            if duplicate {
                let _ = self.inner.record(request).await;
            }
            Ok(reply)
        })
    }

    fn inform_decisions(&self, decisions: Vec<Decision<V>>) -> BoxFuture<'_, Result<()>> {
        let (_, delay) = self.next();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            self.inner.inform_decisions(decisions).await
        })
    }

    fn status(&self, slot: SlotIndex) -> BoxFuture<'_, Result<Option<crate::Step>>> {
        let (_, delay) = self.next();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            self.inner.status(slot).await
        })
    }

    fn install_membership(&self, change: MembershipChange<V>) -> BoxFuture<'_, Result<()>> {
        let (_, delay) = self.next();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            self.inner.install_membership(change).await
        })
    }
}

fn duration_mod(max: Duration, random: u64) -> Duration {
    let nanos = max.as_nanos();
    if nanos == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos((random as u128 % (nanos + 1)).min(u64::MAX as u128) as u64)
    }
}

fn xorshift(mut value: u64) -> u64 {
    value = value.max(1);
    value ^= value << 13;
    value ^= value >> 7;
    value ^ (value << 17)
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
