use crate::error::{QuePaxaError, Result};
use crate::types::{ReplicaId, SlotIndex};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

pub trait HedgingPolicy {
    fn leader_sequence(
        &self,
        slot: SlotIndex,
        replicas: &[ReplicaId],
        previous_winner: Option<ReplicaId>,
    ) -> Result<Vec<ReplicaId>>;

    fn delay_for(
        &self,
        replica: ReplicaId,
        sequence: &[ReplicaId],
        base_delay: Duration,
    ) -> Result<Duration> {
        let position = sequence
            .iter()
            .position(|candidate| *candidate == replica)
            .ok_or(QuePaxaError::UnknownReplica(replica))?;
        Ok(base_delay * position as u32)
    }
}

#[derive(Debug, Clone, Default)]
pub struct FixedLeaderPolicy;

impl HedgingPolicy for FixedLeaderPolicy {
    fn leader_sequence(
        &self,
        _slot: SlotIndex,
        replicas: &[ReplicaId],
        _previous_winner: Option<ReplicaId>,
    ) -> Result<Vec<ReplicaId>> {
        validate_replicas(replicas)?;
        Ok(replicas.to_vec())
    }
}

#[derive(Debug, Clone)]
pub struct RoundRobinPolicy {
    epoch_size: u64,
}

impl RoundRobinPolicy {
    pub fn new(epoch_size: u64) -> Self {
        Self {
            epoch_size: epoch_size.max(1),
        }
    }
}

impl HedgingPolicy for RoundRobinPolicy {
    fn leader_sequence(
        &self,
        slot: SlotIndex,
        replicas: &[ReplicaId],
        _previous_winner: Option<ReplicaId>,
    ) -> Result<Vec<ReplicaId>> {
        validate_replicas(replicas)?;
        let offset = ((slot.get() / self.epoch_size) as usize) % replicas.len();
        Ok(rotated(replicas, offset))
    }
}

#[derive(Debug, Clone, Default)]
pub struct LeaderlessPolicy;

impl HedgingPolicy for LeaderlessPolicy {
    fn leader_sequence(
        &self,
        _slot: SlotIndex,
        replicas: &[ReplicaId],
        _previous_winner: Option<ReplicaId>,
    ) -> Result<Vec<ReplicaId>> {
        validate_replicas(replicas)?;
        Ok(replicas.to_vec())
    }

    fn delay_for(
        &self,
        replica: ReplicaId,
        sequence: &[ReplicaId],
        _base_delay: Duration,
    ) -> Result<Duration> {
        if !sequence.contains(&replica) {
            return Err(QuePaxaError::UnknownReplica(replica));
        }
        Ok(Duration::ZERO)
    }
}

#[derive(Debug, Clone, Default)]
pub struct LastWinnerPolicy;

impl HedgingPolicy for LastWinnerPolicy {
    fn leader_sequence(
        &self,
        _slot: SlotIndex,
        replicas: &[ReplicaId],
        previous_winner: Option<ReplicaId>,
    ) -> Result<Vec<ReplicaId>> {
        validate_replicas(replicas)?;
        let Some(winner) = previous_winner else {
            return Ok(replicas.to_vec());
        };
        let offset = replicas
            .iter()
            .position(|replica| *replica == winner)
            .ok_or(QuePaxaError::UnknownReplica(winner))?;
        Ok(rotated(replicas, offset))
    }
}

/// Ranks leaders by epoch durations supplied by one agreed control plane.
///
/// SAFETY: the samples fed into this policy are local observations, so two
/// replicas that each run their own instance will compute different leader
/// sequences. Installing divergent schedules gives two replicas the reserved
/// round-one priority for the same slot, which can decide conflicting values.
/// Use this policy only from a single control plane that distributes one
/// agreed schedule to every replica — or prefer
/// `ReplicaRuntimeConfig::with_auto_schedules`, which derives identical
/// schedules on every replica from the committed log. Runtime code therefore
/// does not call `record_epoch`; this type is intentionally an offline policy.
#[derive(Debug, Clone)]
pub struct AdaptivePolicy {
    epoch_size: u64,
    samples: BTreeMap<ReplicaId, VecDeque<Duration>>,
    sample_window: usize,
    reexplore_every_epochs: u64,
}

impl AdaptivePolicy {
    pub fn new(epoch_size: u64) -> Self {
        Self {
            epoch_size: epoch_size.max(1),
            samples: BTreeMap::new(),
            sample_window: 16,
            reexplore_every_epochs: 32,
        }
    }

    /// Bounds how much old performance data can dilute new re-exploration
    /// samples after network conditions change.
    pub fn with_sample_window(mut self, epochs_per_leader: usize) -> Self {
        self.sample_window = epochs_per_leader.max(1);
        self
    }

    /// Periodically puts a different replica first even after exploitation
    /// begins, so a recovered or newly fast replica is eventually sampled.
    pub fn with_reexploration_interval(mut self, epochs: u64) -> Self {
        self.reexplore_every_epochs = epochs.max(1);
        self
    }

    pub fn record_epoch(&mut self, leader: ReplicaId, duration: Duration) {
        let samples = self.samples.entry(leader).or_default();
        samples.push_back(duration);
        while samples.len() > self.sample_window {
            samples.pop_front();
        }
    }

    fn average_duration(&self, replica: ReplicaId) -> Option<Duration> {
        let samples = self.samples.get(&replica)?;
        let total = samples.iter().map(Duration::as_nanos).sum::<u128>();
        Some(Duration::from_nanos((total / samples.len() as u128) as u64))
    }
}

impl HedgingPolicy for AdaptivePolicy {
    fn leader_sequence(
        &self,
        slot: SlotIndex,
        replicas: &[ReplicaId],
        _previous_winner: Option<ReplicaId>,
    ) -> Result<Vec<ReplicaId>> {
        validate_replicas(replicas)?;
        let epoch = slot.get() / self.epoch_size;
        let warmup_epochs = 2 * replicas.len() as u64 + 1;

        if epoch < warmup_epochs {
            let offset = (epoch as usize) % replicas.len();
            return Ok(rotated(replicas, offset));
        }

        let mut ranked = replicas.to_vec();
        ranked.sort_by_key(|replica| {
            (
                self.average_duration(*replica)
                    .unwrap_or(Duration::from_secs(u64::MAX / 2)),
                *replica,
            )
        });
        let exploitation_epoch = epoch - warmup_epochs;
        if exploitation_epoch > 0 && exploitation_epoch % self.reexplore_every_epochs == 0 {
            let candidate =
                ((exploitation_epoch / self.reexplore_every_epochs) as usize) % replicas.len();
            let replica = replicas[candidate];
            let position = ranked
                .iter()
                .position(|candidate| *candidate == replica)
                .expect("validated replicas are present in the ranking");
            let candidate = ranked.remove(position);
            ranked.insert(0, candidate);
        }
        Ok(ranked)
    }
}

fn validate_replicas(replicas: &[ReplicaId]) -> Result<()> {
    if replicas.is_empty() {
        return Err(QuePaxaError::EmptyCluster);
    }
    let unique = replicas.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != replicas.len() {
        let duplicate = replicas
            .iter()
            .find(|replica| {
                replicas
                    .iter()
                    .filter(|candidate| candidate == replica)
                    .count()
                    > 1
            })
            .copied()
            .expect("duplicate exists");
        return Err(QuePaxaError::DuplicateReplica(duplicate));
    }
    Ok(())
}

pub(crate) fn rotated(replicas: &[ReplicaId], offset: usize) -> Vec<ReplicaId> {
    replicas[offset..]
        .iter()
        .chain(replicas[..offset].iter())
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replicas() -> Vec<ReplicaId> {
        (1..=5).map(ReplicaId::new).collect()
    }

    #[test]
    fn fixed_policy_preserves_order() {
        let policy = FixedLeaderPolicy;
        assert_eq!(
            policy
                .leader_sequence(SlotIndex::new(10), &replicas(), None)
                .unwrap(),
            replicas()
        );
    }

    #[test]
    fn round_robin_policy_rotates_by_epoch() {
        let policy = RoundRobinPolicy::new(10);
        assert_eq!(
            policy
                .leader_sequence(SlotIndex::new(20), &replicas(), None)
                .unwrap()[0],
            ReplicaId::new(3)
        );
    }

    #[test]
    fn leaderless_policy_has_zero_delay_for_every_replica() {
        let policy = LeaderlessPolicy;
        let sequence = policy
            .leader_sequence(SlotIndex::new(1), &replicas(), None)
            .unwrap();

        assert_eq!(
            policy
                .delay_for(ReplicaId::new(4), &sequence, Duration::from_millis(10))
                .unwrap(),
            Duration::ZERO
        );
    }

    #[test]
    fn last_winner_policy_starts_with_previous_winner() {
        let policy = LastWinnerPolicy;
        assert_eq!(
            policy
                .leader_sequence(SlotIndex::new(9), &replicas(), Some(ReplicaId::new(4)))
                .unwrap()[0],
            ReplicaId::new(4)
        );
    }

    #[test]
    fn adaptive_policy_explores_then_ranks_by_average_duration() {
        let mut policy = AdaptivePolicy::new(10);
        policy.record_epoch(ReplicaId::new(3), Duration::from_millis(5));
        policy.record_epoch(ReplicaId::new(1), Duration::from_millis(20));
        policy.record_epoch(ReplicaId::new(2), Duration::from_millis(10));

        assert_eq!(
            policy
                .leader_sequence(SlotIndex::new(10), &replicas(), None)
                .unwrap()[0],
            ReplicaId::new(2)
        );
        assert_eq!(
            policy
                .leader_sequence(SlotIndex::new(200), &replicas(), None)
                .unwrap()[0],
            ReplicaId::new(3)
        );
    }

    #[test]
    fn adaptive_policy_periodically_reexplores_a_non_leader() {
        let mut policy = AdaptivePolicy::new(10).with_reexploration_interval(4);
        for replica in replicas() {
            policy.record_epoch(replica, Duration::from_millis(replica.get() * 10));
        }
        let warmup_epochs = 2 * replicas().len() as u64 + 1;

        assert_eq!(
            policy
                .leader_sequence(SlotIndex::new((warmup_epochs + 4) * 10), &replicas(), None,)
                .unwrap()[0],
            ReplicaId::new(2)
        );
    }

    #[test]
    fn adaptive_policy_ages_out_stale_latency_samples() {
        let mut policy = AdaptivePolicy::new(1).with_sample_window(2);
        policy.record_epoch(ReplicaId::new(1), Duration::from_secs(10));
        policy.record_epoch(ReplicaId::new(1), Duration::from_millis(2));
        policy.record_epoch(ReplicaId::new(1), Duration::from_millis(2));
        policy.record_epoch(ReplicaId::new(2), Duration::from_millis(3));

        assert_eq!(
            policy
                .leader_sequence(SlotIndex::new(100), &replicas(), None)
                .unwrap()[0],
            ReplicaId::new(1)
        );
    }
}
