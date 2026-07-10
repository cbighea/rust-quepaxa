use crate::error::{QuePaxaError, Result};
use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaId(u64);

impl ReplicaId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for ReplicaId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl Display for ReplicaId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// One stable or joint-consensus membership epoch.
///
/// Stable epochs require the configured `n - f` quorum. Joint epochs require
/// an `n - f` quorum from both the old and new voter sets. That dual-quorum
/// rule is the safety boundary which permits membership changes without ever
/// switching directly between two disjoint quorums.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterIdentity {
    /// Union of voters which are active in this epoch. In a stable epoch this
    /// is exactly the current voter set.
    members: Vec<ReplicaId>,
    /// Fault budget for the current (new, during a joint epoch) voter set.
    max_faults: usize,
    #[cfg_attr(feature = "network", serde(default))]
    epoch: u64,
    #[cfg_attr(feature = "network", serde(default))]
    joint: Option<Box<JointMembership>>,
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct JointMembership {
    old_members: Vec<ReplicaId>,
    old_max_faults: usize,
    new_members: Vec<ReplicaId>,
}

impl ClusterIdentity {
    pub fn new(members: impl IntoIterator<Item = ReplicaId>, max_faults: usize) -> Result<Self> {
        Self::stable(0, members, max_faults)
    }

    /// Creates an explicitly versioned stable membership. Epochs must advance
    /// by one for each stable -> joint or joint -> stable transition.
    pub fn stable(
        epoch: u64,
        members: impl IntoIterator<Item = ReplicaId>,
        max_faults: usize,
    ) -> Result<Self> {
        let members = normalize_members(members)?;
        validate_fault_tolerance(members.len(), max_faults)?;
        Ok(Self {
            members,
            max_faults,
            epoch,
            joint: None,
        })
    }

    /// Begins a joint epoch whose active voters are the union of the current
    /// stable membership and `new_members`.
    pub fn begin_joint(
        &self,
        new_members: impl IntoIterator<Item = ReplicaId>,
        new_max_faults: usize,
    ) -> Result<Self> {
        if self.is_joint() {
            return Err(QuePaxaError::InvalidReconfiguration(
                "cannot begin a second reconfiguration from a joint epoch".into(),
            ));
        }
        let new_members = normalize_members(new_members)?;
        validate_fault_tolerance(new_members.len(), new_max_faults)?;
        let epoch = self
            .epoch
            .checked_add(1)
            .ok_or(QuePaxaError::ConfigurationEpochOverflow)?;
        let old_members = self.members.clone();
        let mut members = old_members
            .iter()
            .chain(&new_members)
            .copied()
            .collect::<Vec<_>>();
        members.sort();
        members.dedup();
        Ok(Self {
            members,
            max_faults: new_max_faults,
            epoch,
            joint: Some(Box::new(JointMembership {
                old_members,
                old_max_faults: self.max_faults,
                new_members,
            })),
        })
    }

    /// Finalizes a joint epoch, retaining only its new voter set.
    pub fn finalize_joint(&self) -> Result<Self> {
        if !self.is_joint() {
            return Err(QuePaxaError::InvalidReconfiguration(
                "only a joint epoch can be finalized".into(),
            ));
        }
        Self::stable(
            self.epoch
                .checked_add(1)
                .ok_or(QuePaxaError::ConfigurationEpochOverflow)?,
            self.joint
                .as_ref()
                .expect("a joint epoch has joint membership")
                .new_members
                .clone(),
            self.max_faults,
        )
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn is_joint(&self) -> bool {
        self.joint.is_some()
    }

    pub fn members(&self) -> &[ReplicaId] {
        &self.members
    }

    /// The membership that remains after a joint epoch is finalized.
    pub fn current_members(&self) -> &[ReplicaId] {
        if self.is_joint() {
            &self
                .joint
                .as_ref()
                .expect("a joint epoch has joint membership")
                .new_members
        } else {
            &self.members
        }
    }

    pub fn old_members(&self) -> Option<&[ReplicaId]> {
        self.joint
            .as_ref()
            .map(|joint| joint.old_members.as_slice())
    }

    pub fn contains(&self, replica: ReplicaId) -> bool {
        self.members.binary_search(&replica).is_ok()
    }

    pub fn max_faults(&self) -> usize {
        self.max_faults
    }

    pub fn quorum_size(&self) -> usize {
        if let Some(joint) = &self.joint {
            let old_quorum = joint.old_members.len() - joint.old_max_faults;
            let new_quorum = joint.new_members.len() - self.max_faults;
            let shared = joint
                .old_members
                .iter()
                .filter(|member| joint.new_members.binary_search(member).is_ok())
                .count()
                .min(old_quorum)
                .min(new_quorum);
            old_quorum + new_quorum - shared
        } else {
            self.members.len() - self.max_faults
        }
    }

    /// Returns true only if `voters` contains a stable quorum, or both
    /// component quorums for a joint epoch.
    pub fn has_quorum<'a>(&self, voters: impl IntoIterator<Item = &'a ReplicaId>) -> bool {
        let voters = voters.into_iter().copied().collect::<BTreeSet<_>>();
        if let Some(joint) = &self.joint {
            let old = joint
                .old_members
                .iter()
                .filter(|member| voters.contains(member))
                .count();
            let new = joint
                .new_members
                .iter()
                .filter(|member| voters.contains(member))
                .count();
            old >= joint.old_members.len() - joint.old_max_faults
                && new >= joint.new_members.len() - self.max_faults
        } else {
            voters.len() >= self.quorum_size() && voters.iter().all(|member| self.contains(*member))
        }
    }

    /// Validates the only two legal membership transitions.
    pub fn is_successor_of(&self, previous: &Self) -> bool {
        if !self.is_well_formed()
            || !previous.is_well_formed()
            || previous.epoch.checked_add(1) != Some(self.epoch)
        {
            return false;
        }
        if let Some(joint) = &self.joint {
            !previous.is_joint()
                && joint.old_members == previous.members
                && joint.old_max_faults == previous.max_faults
        } else {
            previous.joint.as_ref().is_some_and(|joint| {
                self.members == joint.new_members && self.max_faults == previous.max_faults
            })
        }
    }

    fn is_well_formed(&self) -> bool {
        if !members_are_normalized(&self.members) {
            return false;
        }
        let Some(joint) = &self.joint else {
            return validate_fault_tolerance(self.members.len(), self.max_faults).is_ok();
        };
        if !members_are_normalized(&joint.old_members)
            || !members_are_normalized(&joint.new_members)
            || validate_fault_tolerance(joint.old_members.len(), joint.old_max_faults).is_err()
            || validate_fault_tolerance(joint.new_members.len(), self.max_faults).is_err()
        {
            return false;
        }
        let mut union = joint
            .old_members
            .iter()
            .chain(&joint.new_members)
            .copied()
            .collect::<Vec<_>>();
        union.sort();
        union.dedup();
        union == self.members
    }
}

fn members_are_normalized(members: &[ReplicaId]) -> bool {
    !members.is_empty() && members.windows(2).all(|pair| pair[0] < pair[1])
}

fn normalize_members(members: impl IntoIterator<Item = ReplicaId>) -> Result<Vec<ReplicaId>> {
    let mut members = members.into_iter().collect::<Vec<_>>();
    members.sort();
    if members.is_empty() {
        return Err(QuePaxaError::EmptyCluster);
    }
    if let Some(duplicate) = members
        .windows(2)
        .find_map(|pair| (pair[0] == pair[1]).then_some(pair[0]))
    {
        return Err(QuePaxaError::DuplicateReplica(duplicate));
    }
    Ok(members)
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LaneId(u64);

impl LaneId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for LaneId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SlotIndex(u64);

impl SlotIndex {
    pub const GENESIS: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl From<u64> for SlotIndex {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl Display for SlotIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Step(u64);

impl Step {
    pub const INITIAL: Self = Self(0);
    pub const ROUND_ONE_PHASE_ZERO: Self = Self(4);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn checked_from_round_phase(round: u64, phase: u8) -> Option<Self> {
        round
            .checked_mul(4)
            .and_then(|step| step.checked_add(phase as u64))
            .map(Self)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn phase(self) -> u8 {
        (self.0 % 4) as u8
    }

    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    pub fn is_next_after(self, previous: Self) -> bool {
        previous.0.checked_add(1) == Some(self.0)
    }
}

impl From<u64> for Step {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl Display for Step {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Priority(u64);

impl Priority {
    pub const MIN: Self = Self(0);
    pub const MAX: Self = Self(u64::MAX);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn is_leader_priority(self) -> bool {
        self.0 == Self::MAX.0
    }
}

impl From<u64> for Priority {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProposalKey {
    pub priority: Priority,
    pub proposer_id: ReplicaId,
    pub lane_id: LaneId,
}

impl ProposalKey {
    /// Creates a proposal key.
    ///
    /// A `(priority, proposer_id, lane_id)` tuple must identify at most one
    /// value for a slot. The proposer RNG is expected to draw independent
    /// priorities, while `proposer_id` and `lane_id` break real priority ties.
    pub const fn new(priority: Priority, proposer_id: ReplicaId, lane_id: LaneId) -> Self {
        Self {
            priority,
            proposer_id,
            lane_id,
        }
    }
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal<V> {
    pub key: ProposalKey,
    pub value_ids: Vec<V>,
}

impl<V> Proposal<V> {
    pub fn new(key: ProposalKey, value_ids: Vec<V>) -> Result<Self> {
        if value_ids.is_empty() {
            return Err(QuePaxaError::EmptyProposal);
        }
        Ok(Self { key, value_ids })
    }
}

impl<V: Clone> Proposal<V> {
    pub fn with_priority(&self, priority: Priority) -> Self {
        let mut proposal = self.clone();
        proposal.key.priority = priority;
        proposal
    }
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision<V> {
    pub slot: SlotIndex,
    pub value_ids: Vec<V>,
    pub proposer: ReplicaId,
    pub decided_step: Step,
}

/// Canonical application command for a membership transition. Networked
/// deployments should agree on [`sha256_id`](Self::sha256_id) as the value ID;
/// this makes the exact target configuration part of the committed command.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipCommand {
    pub next: ClusterIdentity,
}

impl MembershipCommand {
    pub fn new(next: ClusterIdentity) -> Self {
        Self { next }
    }

    /// Dependency-free canonical encoding used as the input to a deployment's
    /// content hash or signature scheme.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = b"quepaxa-membership-command-v1".to_vec();
        encode_cluster(&self.next, &mut bytes)?;
        Ok(bytes)
    }

    #[cfg(feature = "network")]
    pub fn sha256_id(&self) -> Result<[u8; 32]> {
        use sha2::{Digest, Sha256};
        Ok(Sha256::digest(self.canonical_bytes()?).into())
    }

    /// Binds a SHA-256-addressed command to the exact decision that committed
    /// it. This is the recommended network control-plane path.
    #[cfg(feature = "network")]
    pub fn bind(self, anchor: Decision<[u8; 32]>) -> Result<MembershipChange<[u8; 32]>> {
        let command_id = self.sha256_id()?;
        MembershipChange::new(anchor, self.next, command_id)
    }
}

fn encode_cluster(cluster: &ClusterIdentity, bytes: &mut Vec<u8>) -> Result<()> {
    bytes.extend_from_slice(&cluster.epoch.to_be_bytes());
    encode_usize(cluster.max_faults, bytes)?;
    encode_members(&cluster.members, bytes)?;
    match &cluster.joint {
        Some(joint) => {
            bytes.push(1);
            encode_usize(joint.old_max_faults, bytes)?;
            encode_members(&joint.old_members, bytes)?;
            encode_members(&joint.new_members, bytes)?;
        }
        None => bytes.push(0),
    }
    Ok(())
}

fn encode_members(members: &[ReplicaId], bytes: &mut Vec<u8>) -> Result<()> {
    encode_usize(members.len(), bytes)?;
    for member in members {
        bytes.extend_from_slice(&member.get().to_be_bytes());
    }
    Ok(())
}

fn encode_usize(value: usize, bytes: &mut Vec<u8>) -> Result<()> {
    let value = u64::try_from(value).map_err(|_| {
        QuePaxaError::InvalidReconfiguration(
            "membership field is too large for its canonical encoding".into(),
        )
    })?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

/// A membership transition anchored to a decision committed by the previous
/// configuration. The transition becomes active only after `anchor.slot` and
/// callers must drain all in-flight slots before installing it.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipChange<V> {
    pub anchor: Decision<V>,
    pub next: ClusterIdentity,
    /// The value in `anchor` whose content names `next`.
    pub command_id: V,
}

impl<V: Eq> MembershipChange<V> {
    pub fn new(anchor: Decision<V>, next: ClusterIdentity, command_id: V) -> Result<Self> {
        if !anchor.value_ids.contains(&command_id) {
            return Err(QuePaxaError::InvalidReconfiguration(
                "membership command ID is not present in its anchor decision".into(),
            ));
        }
        Ok(Self {
            anchor,
            next,
            command_id,
        })
    }

    pub fn validate_binding(&self) -> Result<()> {
        if self.anchor.value_ids.contains(&self.command_id) {
            Ok(())
        } else {
            Err(QuePaxaError::InvalidReconfiguration(
                "membership command ID is not present in its anchor decision".into(),
            ))
        }
    }
}

impl<V> Decision<V> {
    pub fn new(
        slot: SlotIndex,
        value_ids: Vec<V>,
        proposer: ReplicaId,
        decided_step: Step,
    ) -> Result<Self> {
        if value_ids.is_empty() {
            return Err(QuePaxaError::EmptyDecision);
        }
        Ok(Self {
            slot,
            value_ids,
            proposer,
            decided_step,
        })
    }
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordRequest<V> {
    pub sender: ReplicaId,
    pub slot: SlotIndex,
    /// The proposer's locally agreed round-one leader for this slot. Recorders
    /// durably bind the first value they accept and reject a different value,
    /// so intersecting quorums cannot grant reserved priority to two leaders.
    pub round_one_leader: Option<ReplicaId>,
    pub step: Step,
    pub proposal: Proposal<V>,
    pub known_decisions: Vec<Decision<V>>,
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordSummary<V> {
    pub step: Step,
    pub first: Option<Proposal<V>>,
    pub prior_aggregate: Option<Proposal<V>>,
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordReply<V> {
    pub recorder: ReplicaId,
    pub cluster: ClusterIdentity,
    pub summary: RecordSummary<V>,
    /// The recorder's retained decision for the requested slot, if any. A
    /// lagging proposer adopts it directly instead of re-running rounds.
    pub decision: Option<Decision<V>>,
}

pub(crate) fn best_proposal<V: Clone>(
    left: Option<Proposal<V>>,
    right: Proposal<V>,
) -> Proposal<V> {
    match left {
        Some(current) if current.key >= right.key => current,
        _ => right,
    }
}

pub(crate) fn max_proposal_by_key<V: Clone, I>(proposals: I) -> Option<Proposal<V>>
where
    I: IntoIterator<Item = Proposal<V>>,
{
    proposals
        .into_iter()
        .fold(None, |best, proposal| Some(best_proposal(best, proposal)))
}

pub(crate) fn validate_fault_tolerance(replica_count: usize, max_faults: usize) -> Result<()> {
    let required = max_faults
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| QuePaxaError::InvalidProposal("fault tolerance is too large".into()))?;
    if replica_count < required {
        return Err(QuePaxaError::InvalidProposal(format!(
            "{replica_count} replicas cannot tolerate {max_faults} crash faults; require at least {required}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proposal_key_ties_keep_existing_proposal() {
        let key = ProposalKey::new(Priority::new(5), ReplicaId::new(1), LaneId::new(1));
        let first = Proposal::new(key, vec![1]).unwrap();
        let second = Proposal::new(key, vec![2]).unwrap();

        let best = best_proposal(Some(first.clone()), second.clone());
        assert_eq!(best.value_ids, vec![1]);

        let folded = max_proposal_by_key([first, second]).unwrap();
        assert_eq!(folded.value_ids, vec![1]);
    }

    #[test]
    fn joint_membership_requires_both_component_quorums() {
        let stable = ClusterIdentity::stable(7, (1..=3).map(ReplicaId::new), 1).unwrap();
        let joint = stable.begin_joint((3..=5).map(ReplicaId::new), 1).unwrap();

        assert!(joint.is_joint());
        assert_eq!(joint.epoch(), 8);
        assert_eq!(
            joint.members(),
            &(1..=5).map(ReplicaId::new).collect::<Vec<_>>()
        );
        assert!(!joint.has_quorum([ReplicaId::new(1), ReplicaId::new(2)].iter()));
        assert!(
            !joint.has_quorum([ReplicaId::new(1), ReplicaId::new(2), ReplicaId::new(3)].iter())
        );
        assert!(joint.has_quorum([ReplicaId::new(1), ReplicaId::new(3), ReplicaId::new(4)].iter()));

        let finalized = joint.finalize_joint().unwrap();
        assert!(!finalized.is_joint());
        assert_eq!(finalized.epoch(), 9);
        assert_eq!(
            finalized.members(),
            &[ReplicaId::new(3), ReplicaId::new(4), ReplicaId::new(5)]
        );
        assert!(finalized.is_successor_of(&joint));
    }

    #[test]
    fn all_valid_stable_quorums_intersect_through_nine_voters() {
        for replica_count in 1_u32..=9 {
            for max_faults in 0..replica_count.div_ceil(2) {
                if validate_fault_tolerance(replica_count as usize, max_faults as usize).is_err() {
                    continue;
                }
                let quorum_size = replica_count - max_faults;
                let quorums = (0_u16..(1_u16 << replica_count))
                    .filter(|mask| mask.count_ones() >= quorum_size)
                    .collect::<Vec<_>>();
                for left in &quorums {
                    for right in &quorums {
                        assert_ne!(left & right, 0);
                    }
                }
            }
        }
    }

    #[test]
    fn membership_change_names_a_value_in_the_anchor() {
        let anchor = Decision::new(
            SlotIndex::new(1),
            vec![10_u64],
            ReplicaId::new(1),
            Step::ROUND_ONE_PHASE_ZERO,
        )
        .unwrap();
        let next = ClusterIdentity::new([ReplicaId::new(1)], 0)
            .unwrap()
            .begin_joint([ReplicaId::new(1)], 0)
            .unwrap();
        assert!(MembershipChange::new(anchor.clone(), next.clone(), 11).is_err());
        assert!(MembershipChange::new(anchor, next, 10).is_ok());
    }

    #[cfg(feature = "network")]
    #[test]
    fn canonical_membership_command_binds_its_configuration_digest() {
        let stable = ClusterIdentity::new((1..=3).map(ReplicaId::new), 1).unwrap();
        let first = MembershipCommand::new(
            stable
                .begin_joint([ReplicaId::new(1), ReplicaId::new(4), ReplicaId::new(5)], 1)
                .unwrap(),
        );
        let second = MembershipCommand::new(
            stable
                .begin_joint([ReplicaId::new(1), ReplicaId::new(4), ReplicaId::new(6)], 1)
                .unwrap(),
        );
        assert_ne!(first.sha256_id().unwrap(), second.sha256_id().unwrap());
        let command_id = first.sha256_id().unwrap();
        let anchor = Decision::new(
            SlotIndex::new(1),
            vec![command_id],
            ReplicaId::new(1),
            Step::ROUND_ONE_PHASE_ZERO,
        )
        .unwrap();
        let change = first.bind(anchor).unwrap();
        assert_eq!(change.command_id, command_id);
    }
}
