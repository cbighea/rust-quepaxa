use crate::error::{QuePaxaError, Result};
use crate::types::{Decision, SlotIndex};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, Clone, Copy)]
pub struct ReplicaConfig {
    pub batch_size: usize,
    pub pipeline_len: usize,
    pub max_pending_values: usize,
    pub max_log_slots: usize,
    /// Bounds the durable set used to suppress retry submissions carrying an
    /// already-seen value ID.
    pub max_tracked_value_ids: usize,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            pipeline_len: 1,
            max_pending_values: 65_536,
            max_log_slots: 4_096,
            max_tracked_value_ids: 1_048_576,
        }
    }
}

#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSlot<V> {
    pub proposed: Vec<V>,
    pub decision: Option<Decision<V>>,
    pub committed: bool,
}

/// Durable local state required to resume the replica pipeline after restart.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "network",
    serde(bound(
        serialize = "V: serde::Serialize",
        deserialize = "V: Ord + serde::Deserialize<'de>"
    ))
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSnapshot<V> {
    pub log: BTreeMap<SlotIndex, LogSlot<V>>,
    pub pending: Vec<V>,
    pub last_proposed: SlotIndex,
    pub next_commit: SlotIndex,
    pub checkpointed_through: SlotIndex,
    pub seen_value_ids: BTreeSet<V>,
}

impl<V> Default for LogSlot<V> {
    fn default() -> Self {
        Self {
            proposed: Vec::new(),
            decision: None,
            committed: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaCore<V> {
    config: ReplicaConfig,
    log: BTreeMap<SlotIndex, LogSlot<V>>,
    pending: VecDeque<V>,
    last_proposed: SlotIndex,
    next_commit: SlotIndex,
    checkpointed_through: SlotIndex,
    seen_value_ids: BTreeSet<V>,
}

impl<V: Clone + Ord> ReplicaCore<V> {
    pub fn new(config: ReplicaConfig) -> Self {
        let mut log = BTreeMap::new();
        log.insert(
            SlotIndex::GENESIS,
            LogSlot {
                proposed: Vec::new(),
                decision: None,
                committed: true,
            },
        );

        Self {
            config,
            log,
            pending: VecDeque::new(),
            last_proposed: SlotIndex::GENESIS,
            next_commit: SlotIndex::new(1),
            checkpointed_through: SlotIndex::GENESIS,
            seen_value_ids: BTreeSet::new(),
        }
    }

    /// Restores a replica pipeline from a durable snapshot.
    pub fn restore(config: ReplicaConfig, snapshot: ReplicaSnapshot<V>) -> Result<Self> {
        let retained_slots = snapshot
            .log
            .keys()
            .filter(|slot| **slot != SlotIndex::GENESIS)
            .count();
        if snapshot.pending.len() > config.max_pending_values {
            return Err(QuePaxaError::ResourceLimit {
                resource: "pending value IDs",
                limit: config.max_pending_values,
            });
        }
        if retained_slots > config.max_log_slots {
            return Err(QuePaxaError::ResourceLimit {
                resource: "retained log slots",
                limit: config.max_log_slots,
            });
        }
        if snapshot.seen_value_ids.len() > config.max_tracked_value_ids {
            return Err(QuePaxaError::ResourceLimit {
                resource: "tracked value IDs",
                limit: config.max_tracked_value_ids,
            });
        }
        if snapshot
            .pending
            .iter()
            .chain(snapshot.log.values().flat_map(|entry| {
                entry.proposed.iter().chain(
                    entry
                        .decision
                        .iter()
                        .flat_map(|decision| &decision.value_ids),
                )
            }))
            .any(|value_id| !snapshot.seen_value_ids.contains(value_id))
        {
            return Err(QuePaxaError::InvalidProposal(
                "replica snapshot contains an untracked value ID".into(),
            ));
        }
        if !snapshot
            .log
            .get(&SlotIndex::GENESIS)
            .is_some_and(|slot| slot.committed)
            || snapshot.next_commit <= snapshot.checkpointed_through
        {
            return Err(QuePaxaError::InvalidProposal(
                "replica snapshot is missing its committed genesis slot".into(),
            ));
        }
        let mut slot = snapshot
            .checkpointed_through
            .checked_next()
            .ok_or(QuePaxaError::SlotOverflow)?;
        while slot < snapshot.next_commit {
            if !snapshot
                .log
                .get(&slot)
                .is_some_and(|entry| entry.committed && entry.decision.is_some())
            {
                return Err(QuePaxaError::InvalidProposal(
                    "replica snapshot skips a committed slot after its checkpoint".into(),
                ));
            }
            slot = slot.checked_next().ok_or(QuePaxaError::SlotOverflow)?;
        }
        if snapshot.log.iter().any(|(slot, entry)| {
            *slot != SlotIndex::GENESIS && *slot >= snapshot.next_commit && entry.committed
        }) {
            return Err(QuePaxaError::InvalidProposal(
                "replica snapshot marks a future slot committed".into(),
            ));
        }

        Ok(Self {
            config,
            log: snapshot.log,
            pending: snapshot.pending.into(),
            last_proposed: snapshot.last_proposed,
            next_commit: snapshot.next_commit,
            checkpointed_through: snapshot.checkpointed_through,
            seen_value_ids: snapshot.seen_value_ids,
        })
    }

    pub fn snapshot(&self) -> ReplicaSnapshot<V> {
        ReplicaSnapshot {
            log: self.log.clone(),
            pending: self.pending.iter().cloned().collect(),
            last_proposed: self.last_proposed,
            next_commit: self.next_commit,
            checkpointed_through: self.checkpointed_through,
            seen_value_ids: self.seen_value_ids.clone(),
        }
    }

    pub fn enqueue<I>(&mut self, value_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = V>,
    {
        let mut unique = BTreeSet::new();
        let value_ids = value_ids
            .into_iter()
            .filter(|value_id| {
                !self.seen_value_ids.contains(value_id) && unique.insert(value_id.clone())
            })
            .collect::<Vec<_>>();
        if self.pending.len() + value_ids.len() > self.config.max_pending_values {
            return Err(QuePaxaError::ResourceLimit {
                resource: "pending value IDs",
                limit: self.config.max_pending_values,
            });
        }
        if self.seen_value_ids.len() + value_ids.len() > self.config.max_tracked_value_ids {
            return Err(QuePaxaError::ResourceLimit {
                resource: "tracked value IDs",
                limit: self.config.max_tracked_value_ids,
            });
        }
        self.seen_value_ids.extend(value_ids.iter().cloned());
        self.pending.extend(value_ids);
        Ok(())
    }

    pub fn next_proposal(&mut self) -> Result<Option<(SlotIndex, Vec<V>)>> {
        if self.inflight_count() >= self.config.pipeline_len || self.pending.is_empty() {
            return Ok(None);
        }

        let slot = self.next_open_slot()?;
        let take = self.config.batch_size.min(self.pending.len()).max(1);
        let values = self.pending.drain(..take).collect::<Vec<_>>();

        self.note_proposed(slot, values.clone())?;
        Ok(Some((slot, values)))
    }

    pub fn note_proposed(&mut self, slot: SlotIndex, value_ids: Vec<V>) -> Result<()> {
        if value_ids.is_empty() {
            return Err(QuePaxaError::EmptyProposal);
        }
        let unique = value_ids.iter().cloned().collect::<BTreeSet<_>>();
        if unique.len() != value_ids.len() {
            return Err(QuePaxaError::InvalidProposal(
                "proposal contains duplicate value IDs".into(),
            ));
        }
        let new_value_ids = unique
            .into_iter()
            .filter(|value_id| !self.seen_value_ids.contains(value_id))
            .collect::<Vec<_>>();
        if self.seen_value_ids.len() + new_value_ids.len() > self.config.max_tracked_value_ids {
            return Err(QuePaxaError::ResourceLimit {
                resource: "tracked value IDs",
                limit: self.config.max_tracked_value_ids,
            });
        }
        self.ensure_log_capacity(slot)?;
        self.seen_value_ids.extend(new_value_ids);
        self.last_proposed = self.last_proposed.max(slot);
        self.log.entry(slot).or_default().proposed = value_ids;
        Ok(())
    }

    pub fn apply_decision(&mut self, decision: Decision<V>) -> Result<Vec<Decision<V>>> {
        if decision.value_ids.is_empty() {
            return Err(QuePaxaError::EmptyDecision);
        }
        if decision
            .value_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .len()
            != decision.value_ids.len()
        {
            return Err(QuePaxaError::InvalidProposal(
                "decision contains duplicate value IDs".into(),
            ));
        }
        let new_value_ids = decision
            .value_ids
            .iter()
            .filter(|value_id| !self.seen_value_ids.contains(*value_id))
            .cloned()
            .collect::<BTreeSet<_>>();
        if self.seen_value_ids.len() + new_value_ids.len() > self.config.max_tracked_value_ids {
            return Err(QuePaxaError::ResourceLimit {
                resource: "tracked value IDs",
                limit: self.config.max_tracked_value_ids,
            });
        }
        let slot_index = decision.slot;
        self.ensure_log_capacity(slot_index)?;
        {
            let slot = self.log.entry(slot_index).or_default();

            if let Some(existing) = &slot.decision {
                if existing.value_ids != decision.value_ids {
                    return Err(QuePaxaError::ConflictingDecision { slot: slot_index });
                }
                return Ok(Vec::new());
            }

            if !slot.proposed.is_empty() && slot.proposed != decision.value_ids {
                let decided = decision.value_ids.iter().cloned().collect::<BTreeSet<_>>();
                let mut requeue = slot
                    .proposed
                    .iter()
                    .filter(|value| !decided.contains(*value))
                    .cloned()
                    .collect::<Vec<_>>();
                if self.pending.len() + requeue.len() > self.config.max_pending_values {
                    return Err(QuePaxaError::ResourceLimit {
                        resource: "pending value IDs",
                        limit: self.config.max_pending_values,
                    });
                }
                while let Some(value) = requeue.pop() {
                    self.pending.push_front(value);
                }
            }

            slot.decision = Some(decision);
        }

        self.seen_value_ids.extend(new_value_ids);
        self.commit_ready()
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn committed_through(&self) -> SlotIndex {
        SlotIndex::new(self.next_commit.get().saturating_sub(1))
    }

    pub fn checkpointed_through(&self) -> SlotIndex {
        self.checkpointed_through
    }

    pub fn slot(&self, slot: SlotIndex) -> Option<&LogSlot<V>> {
        self.log.get(&slot)
    }

    /// Drops only committed slots after the embedding state machine has
    /// checkpointed them.
    pub fn prune_through(&mut self, slot: SlotIndex) {
        let through = slot
            .min(self.committed_through())
            .max(self.checkpointed_through);
        self.log.retain(|index, entry| {
            *index == SlotIndex::GENESIS || *index > through || !entry.committed
        });
        self.checkpointed_through = through;
    }

    fn next_open_slot(&self) -> Result<SlotIndex> {
        // Propose into the lowest undecided slot we have not already claimed,
        // starting from the commit frontier. This re-drives gap slots left by
        // a crashed proposer instead of skipping past them forever.
        let mut slot = self.next_commit;
        while self
            .log
            .get(&slot)
            .is_some_and(|entry| entry.decision.is_some() || !entry.proposed.is_empty())
        {
            slot = slot.checked_next().ok_or(QuePaxaError::SlotOverflow)?;
        }
        Ok(slot)
    }

    fn inflight_count(&self) -> usize {
        self.log
            .iter()
            .filter(|(slot, entry)| {
                **slot >= self.next_commit && !entry.committed && !entry.proposed.is_empty()
            })
            .count()
    }

    fn commit_ready(&mut self) -> Result<Vec<Decision<V>>> {
        let mut committed = Vec::new();

        while let Some(slot) = self.log.get_mut(&self.next_commit) {
            if slot.committed {
                self.next_commit = self
                    .next_commit
                    .checked_next()
                    .ok_or(QuePaxaError::SlotOverflow)?;
                continue;
            }

            let Some(decision) = slot.decision.clone() else {
                break;
            };

            slot.committed = true;
            slot.proposed.clear();
            committed.push(decision);
            self.next_commit = self
                .next_commit
                .checked_next()
                .ok_or(QuePaxaError::SlotOverflow)?;
        }

        Ok(committed)
    }

    fn ensure_log_capacity(&self, slot: SlotIndex) -> Result<()> {
        if !self.log.contains_key(&slot)
            && self
                .log
                .iter()
                .filter(|(index, _)| **index != SlotIndex::GENESIS)
                .count()
                >= self.config.max_log_slots
        {
            return Err(QuePaxaError::ResourceLimit {
                resource: "retained log slots",
                limit: self.config.max_log_slots,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ReplicaId, Step};

    fn decision(slot: u64, values: Vec<u64>) -> Decision<u64> {
        Decision::new(
            SlotIndex::new(slot),
            values,
            ReplicaId::new(1),
            Step::new(4),
        )
        .unwrap()
    }

    #[test]
    fn commits_decided_slots_in_order() {
        let mut replica = ReplicaCore::new(ReplicaConfig {
            batch_size: 2,
            pipeline_len: 2,
            ..ReplicaConfig::default()
        });
        replica.enqueue([1, 2, 3, 4]).unwrap();

        assert_eq!(
            replica.next_proposal().unwrap(),
            Some((SlotIndex::new(1), vec![1, 2]))
        );
        assert_eq!(
            replica.next_proposal().unwrap(),
            Some((SlotIndex::new(2), vec![3, 4]))
        );

        assert!(
            replica
                .apply_decision(decision(2, vec![3, 4]))
                .unwrap()
                .is_empty()
        );
        let committed = replica.apply_decision(decision(1, vec![1, 2])).unwrap();

        assert_eq!(committed.len(), 2);
        assert_eq!(committed[0].slot, SlotIndex::new(1));
        assert_eq!(committed[1].slot, SlotIndex::new(2));
        assert_eq!(replica.committed_through(), SlotIndex::new(2));
    }

    #[test]
    fn requeues_losing_proposal_values() {
        let mut replica = ReplicaCore::new(ReplicaConfig {
            batch_size: 2,
            pipeline_len: 1,
            ..ReplicaConfig::default()
        });
        replica
            .note_proposed(SlotIndex::new(1), vec![1, 2])
            .unwrap();

        let committed = replica.apply_decision(decision(1, vec![2])).unwrap();

        assert_eq!(committed.len(), 1);
        assert_eq!(replica.pending_len(), 1);
        assert_eq!(
            replica.next_proposal().unwrap(),
            Some((SlotIndex::new(2), vec![1]))
        );
    }

    #[test]
    fn restore_rejects_a_snapshot_that_skips_a_committed_slot() {
        let mut log = BTreeMap::new();
        log.insert(
            SlotIndex::GENESIS,
            LogSlot {
                committed: true,
                ..LogSlot::default()
            },
        );
        let snapshot = ReplicaSnapshot {
            log,
            pending: Vec::<u64>::new(),
            last_proposed: SlotIndex::GENESIS,
            next_commit: SlotIndex::new(2),
            checkpointed_through: SlotIndex::GENESIS,
            seen_value_ids: BTreeSet::new(),
        };

        assert!(matches!(
            ReplicaCore::restore(ReplicaConfig::default(), snapshot),
            Err(QuePaxaError::InvalidProposal(_))
        ));
    }

    #[test]
    fn duplicate_submissions_remain_suppressed_after_restart() {
        let mut replica = ReplicaCore::new(ReplicaConfig::default());
        replica.enqueue([7, 7]).unwrap();
        assert_eq!(replica.pending_len(), 1);

        let mut restored =
            ReplicaCore::restore(ReplicaConfig::default(), replica.snapshot()).unwrap();
        restored.enqueue([7]).unwrap();

        assert_eq!(restored.pending_len(), 1);
        assert_eq!(
            restored.next_proposal().unwrap(),
            Some((SlotIndex::new(1), vec![7]))
        );
    }
}
