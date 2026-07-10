use crate::network::batch::Batch;
use crate::types::{
    Decision, MembershipChange, RecordReply, RecordRequest, ReplicaId, SlotIndex, Step,
};
use serde::{Deserialize, Serialize};

pub const WIRE_VERSION: u16 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeploymentId(pub [u8; 16]);

impl DeploymentId {
    pub const fn from_u128(value: u128) -> Self {
        Self(value.to_be_bytes())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerSender {
    Replica(ReplicaId),
    Client([u8; 16]),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Submission<V> {
    pub client_id: [u8; 16],
    pub request_id: u64,
    pub value_ids: Vec<V>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionOutcome<V> {
    Accepted,
    Committed(Decision<V>),
    Duplicate(Option<Decision<V>>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestBody<V> {
    Record(RecordRequest<V>),
    Status(SlotIndex),
    InformDecisions(Vec<Decision<V>>),
    InstallMembership(MembershipChange<V>),
    PublishBatches(Vec<Batch<V>>),
    FetchBatches(Vec<V>),
    Submit(Submission<V>),
    Ping,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRequest<V> {
    pub version: u16,
    pub deployment: DeploymentId,
    pub request_id: u64,
    pub sender: PeerSender,
    pub body: RequestBody<V>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseBody<V> {
    Record(RecordReply<V>),
    Status(Option<Step>),
    Submission(SubmissionOutcome<V>),
    Batches(Vec<Batch<V>>),
    Ack,
    Pong,
    Error(WireError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeResponse<V> {
    pub version: u16,
    pub deployment: DeploymentId,
    pub request_id: u64,
    pub body: ResponseBody<V>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireError {
    pub code: WireErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireErrorCode {
    Authentication,
    Configuration,
    InvalidRequest,
    ResourceLimit,
    Storage,
    Unavailable,
    Internal,
    /// The peer holds a different decision for the requested slot. This is a
    /// safety alarm and must never be retried past.
    Conflict,
    /// The recorder has already bound the slot to a different round-one leader.
    ScheduleMismatch,
    /// The requested slot was decided, checkpointed, and pruned; the caller
    /// must state-transfer instead of re-running consensus.
    Pruned,
}
