//! Rust-native QuePaxa consensus core.
//!
//! This crate exposes the reusable algorithm pieces without copying the Go
//! prototype's transport, protobuf, benchmark, or command-line structure.

pub mod crash;
pub mod error;
pub mod isr;
#[cfg(feature = "network")]
pub mod network;
pub mod policy;
pub mod proposer;
pub mod recorder;
pub mod replica;
pub mod runtime;
pub mod store;
pub mod types;

pub use crash::{CrashInjector, CrashPoint, NoopCrashInjector};
pub use error::{QuePaxaError, Result};
pub use isr::{IntervalSummaryRegister, IntervalSummarySnapshot};
pub use policy::{
    AdaptivePolicy, FixedLeaderPolicy, HedgingPolicy, LastWinnerPolicy, LeaderlessPolicy,
    RoundRobinPolicy,
};
pub use proposer::{
    OsRandom, PrioritySource, ProposerConfig, ProposerCore, RecorderClient, RecorderHandle,
    XorShift64,
};
pub use recorder::{
    DurableRecorderCore, FileRecorderStore, InMemoryRecorderStore, RecorderCodec, RecorderConfig,
    RecorderCore, RecorderLimits, RecorderSnapshot, RecorderStateStore,
};
pub use replica::{LogSlot, ReplicaConfig, ReplicaCore, ReplicaSnapshot};
pub use runtime::{
    AdaptiveHedgingConfig, ClientNotifier, EpochSchedule, EpochStat, EpochTuner, FileRuntimeStore,
    InMemoryRuntimeStore, NoopClientNotifier, NoopStateMachine, PendingProposalSnapshot,
    ProtocolIdentity, ReplicaRuntime, ReplicaRuntimeConfig, RuntimeCodec, RuntimePoll,
    RuntimeSnapshot, RuntimeStateStore, StateMachine, StateTransferSnapshot,
};
pub use store::{
    AllowAllAvailability, FetchingAvailability, InMemoryValueStore, ValueAvailability,
    ValueFetcher, ValueStore,
};
pub use types::{
    ClusterIdentity, Decision, LaneId, MembershipChange, MembershipCommand, Priority, Proposal,
    ProposalKey, RecordReply, RecordRequest, RecordSummary, ReplicaId, SlotIndex, Step,
};
