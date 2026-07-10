//! Authenticated, versioned, asynchronous networking for QuePaxa nodes.

mod async_proposer;
mod batch;
mod chaos;
mod codec;
mod dedupe;
mod load;
mod metrics;
mod node_runtime;
mod report;
mod server;
mod tls;
mod transport;
mod wire;

pub use async_proposer::{AsyncProposerCore, AsyncRecorderClient};
pub use batch::{
    AuthenticatedBatchService, Batch, BatchService, BatchServiceLimits, BatchStore, BatchVerifier,
    FileBatchStore, InMemoryBatchStore,
};
pub use chaos::{
    ChaosRecorderClient, ReproducibleWanHarness, ReproducibleWanProxy, RpcChaosProfile,
    WanHarnessLink, WanHarnessReport, WanLinkProfile, WanLinkReport, WanProxyObserver,
    WanProxyStats,
};
pub use codec::{
    PostcardRecorderCodec, PostcardRuntimeCodec, PostcardStateTransferCodec, STORAGE_VERSION,
};
pub use dedupe::{
    DeduplicatingStateMachine, DeduplicatingSubmissionHandler, ExactlyOnceExecutor,
    FileExactlyOnceExecutor, FileSubmissionJournal, InMemoryExactlyOnceExecutor,
    InMemorySubmissionJournal, SubmissionJournal,
};
pub use load::{
    OpenLoopLoadConfig, OpenLoopLoadGenerator, OpenLoopLoadReport, U64SubmissionClient,
};
pub use metrics::{NetworkMetrics, NetworkMetricsSnapshot};
pub use node_runtime::NetworkConsensusHandler;
pub use server::{
    BoxSubmissionFuture, FnSubmissionHandler, NetworkNodeServer, PeerIdentity, PeerRegistry,
    ServerTlsRegistry, SubmissionHandler,
};
pub use tls::{MutualTlsConfigs, TlsIdentity};
pub use transport::{
    TlsBatchClient, TlsBatchFetcher, TlsBatchPublisher, TlsRecorderClient, TlsSubmitClient,
};
pub use wire::{
    DeploymentId, NodeRequest, NodeResponse, RequestBody, ResponseBody, Submission,
    SubmissionOutcome, WIRE_VERSION, WireError,
};
