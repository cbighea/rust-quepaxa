use crate::types::{ReplicaId, SlotIndex};
use std::fmt::{self, Display, Formatter};

pub type Result<T> = std::result::Result<T, QuePaxaError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuePaxaError {
    EmptyCluster,
    EmptyProposal,
    EmptyDecision,
    MissingProposal,
    MissingValue,
    DuplicateReplica(ReplicaId),
    UnknownReplica(ReplicaId),
    InvalidSender(ReplicaId),
    InvalidRecorderReply {
        expected: ReplicaId,
        received: ReplicaId,
    },
    DuplicateRecorderReply(ReplicaId),
    InvalidQuorum {
        replicas: usize,
        quorum: usize,
    },
    QuorumNotReached {
        needed: usize,
        received: usize,
    },
    InvalidStep {
        step: u64,
        limit: u64,
    },
    StepOverflow,
    SlotOverflow,
    ResourceLimit {
        resource: &'static str,
        limit: usize,
    },
    InvalidProposal(String),
    TransportError(String),
    StorageError(String),
    ConfigurationMismatch,
    ConfigurationEpochOverflow,
    InvalidReconfiguration(String),
    ConflictingDecision {
        slot: SlotIndex,
    },
    /// A recorder has already bound this slot to a different round-one
    /// leader. Continuing could grant the reserved priority to two proposers.
    ScheduleMismatch {
        slot: SlotIndex,
    },
    SlotAlreadyDecided {
        slot: SlotIndex,
    },
    /// The slot was decided, checkpointed, and pruned cluster-wide. The caller
    /// is too far behind to re-run consensus and must obtain the decision (or
    /// an application checkpoint) through state transfer.
    SlotPruned {
        slot: SlotIndex,
    },
    StepLimitExceeded {
        limit: u64,
    },
    PolicyError(String),
}

impl Display for QuePaxaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCluster => f.write_str("cluster must contain at least one replica"),
            Self::EmptyProposal => f.write_str("proposal must contain at least one value id"),
            Self::EmptyDecision => f.write_str("decision must contain at least one value id"),
            Self::MissingProposal => f.write_str("recorder reply did not contain a proposal"),
            Self::MissingValue => f.write_str("required value is missing from the value store"),
            Self::DuplicateReplica(replica) => write!(f, "duplicate replica id {replica}"),
            Self::UnknownReplica(replica) => write!(f, "unknown replica id {replica}"),
            Self::InvalidSender(replica) => {
                write!(f, "replica id {replica} is not an authorized sender")
            }
            Self::InvalidRecorderReply { expected, received } => write!(
                f,
                "recorder reply identity mismatch: expected {expected}, received {received}"
            ),
            Self::DuplicateRecorderReply(replica) => {
                write!(f, "duplicate reply from recorder {replica}")
            }
            Self::InvalidQuorum { replicas, quorum } => {
                write!(f, "invalid quorum {quorum} for {replicas} replicas")
            }
            Self::QuorumNotReached { needed, received } => {
                write!(
                    f,
                    "quorum not reached: needed {needed}, received {received}"
                )
            }
            Self::InvalidStep { step, limit } => {
                write!(
                    f,
                    "logical step {step} is outside the accepted range through {limit}"
                )
            }
            Self::StepOverflow => f.write_str("logical step counter overflowed"),
            Self::SlotOverflow => f.write_str("slot index overflowed"),
            Self::ResourceLimit { resource, limit } => {
                write!(f, "{resource} exceeds its configured limit of {limit}")
            }
            Self::InvalidProposal(message) => f.write_str(message),
            Self::TransportError(message) => f.write_str(message),
            Self::StorageError(message) => f.write_str(message),
            Self::ConfigurationMismatch => {
                f.write_str("replica configuration does not match the local cluster identity")
            }
            Self::ConfigurationEpochOverflow => {
                f.write_str("membership configuration epoch overflowed")
            }
            Self::InvalidReconfiguration(message) => f.write_str(message),
            Self::ConflictingDecision { slot } => write!(f, "conflicting decision for slot {slot}"),
            Self::ScheduleMismatch { slot } => {
                write!(f, "round-one leader schedule mismatch for slot {slot}")
            }
            Self::SlotAlreadyDecided { slot } => write!(f, "slot {slot} is already decided"),
            Self::SlotPruned { slot } => write!(
                f,
                "slot {slot} was pruned after checkpointing; state transfer is required"
            ),
            Self::StepLimitExceeded { limit } => {
                write!(f, "proposer exceeded the configured step limit {limit}")
            }
            Self::PolicyError(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for QuePaxaError {}
