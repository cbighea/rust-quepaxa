use crate::types::{Proposal, RecordSummary, Step, best_proposal};

/// Serializable state for one interval summary register.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalSummarySnapshot<V> {
    pub step: Step,
    pub first: Option<Proposal<V>>,
    pub current_aggregate: Option<Proposal<V>>,
    pub prior_aggregate: Option<Proposal<V>>,
}

#[derive(Debug, Clone)]
pub struct IntervalSummaryRegister<V> {
    step: Step,
    first: Option<Proposal<V>>,
    current_aggregate: Option<Proposal<V>>,
    prior_aggregate: Option<Proposal<V>>,
}

impl<V> Default for IntervalSummaryRegister<V> {
    fn default() -> Self {
        Self {
            step: Step::INITIAL,
            first: None,
            current_aggregate: None,
            prior_aggregate: None,
        }
    }
}

impl<V: Clone> IntervalSummaryRegister<V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn step(&self) -> Step {
        self.step
    }

    pub fn snapshot(&self) -> IntervalSummarySnapshot<V> {
        IntervalSummarySnapshot {
            step: self.step,
            first: self.first.clone(),
            current_aggregate: self.current_aggregate.clone(),
            prior_aggregate: self.prior_aggregate.clone(),
        }
    }

    pub fn restore(snapshot: IntervalSummarySnapshot<V>) -> Self {
        Self {
            step: snapshot.step,
            first: snapshot.first,
            current_aggregate: snapshot.current_aggregate,
            prior_aggregate: snapshot.prior_aggregate,
        }
    }

    pub fn record(&mut self, step: Step, proposal: Proposal<V>) -> RecordSummary<V> {
        if step == self.step {
            self.first.get_or_insert_with(|| proposal.clone());
            self.current_aggregate = Some(best_proposal(self.current_aggregate.take(), proposal));
        } else if step > self.step {
            self.prior_aggregate = if step.is_next_after(self.step) {
                self.current_aggregate.take()
            } else {
                None
            };
            self.step = step;
            self.first = Some(proposal.clone());
            self.current_aggregate = Some(proposal);
        }

        RecordSummary {
            step: self.step,
            first: self.first.clone(),
            prior_aggregate: self.prior_aggregate.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LaneId, Priority, ProposalKey, ReplicaId};

    fn proposal(priority: u64, value: u64) -> Proposal<u64> {
        Proposal::new(
            ProposalKey::new(
                Priority::new(priority),
                ReplicaId::new(priority),
                LaneId::new(1),
            ),
            vec![value],
        )
        .unwrap()
    }

    #[test]
    fn same_step_keeps_first_and_aggregates_best() {
        let mut isr = IntervalSummaryRegister::new();

        let first = isr.record(Step::new(4), proposal(3, 30));
        let second = isr.record(Step::new(4), proposal(7, 70));

        assert_eq!(first.first.unwrap().value_ids, vec![30]);
        assert_eq!(second.first.unwrap().value_ids, vec![30]);
        assert_eq!(second.step, Step::new(4));
    }

    #[test]
    fn step_advance_exposes_prior_aggregate() {
        let mut isr = IntervalSummaryRegister::new();
        isr.record(Step::new(4), proposal(3, 30));
        isr.record(Step::new(4), proposal(7, 70));

        let reply = isr.record(Step::new(5), proposal(2, 20));

        assert_eq!(reply.step, Step::new(5));
        assert_eq!(reply.first.unwrap().value_ids, vec![20]);
        assert_eq!(reply.prior_aggregate.unwrap().value_ids, vec![70]);
    }

    #[test]
    fn skipped_step_resets_prior_aggregate() {
        let mut isr = IntervalSummaryRegister::new();
        isr.record(Step::new(4), proposal(3, 30));

        let reply = isr.record(Step::new(6), proposal(2, 20));

        assert_eq!(reply.step, Step::new(6));
        assert!(reply.prior_aggregate.is_none());
    }

    #[test]
    fn stale_step_does_not_change_state() {
        let mut isr = IntervalSummaryRegister::new();
        isr.record(Step::new(5), proposal(5, 50));

        let reply = isr.record(Step::new(4), proposal(9, 90));

        assert_eq!(reply.step, Step::new(5));
        assert_eq!(reply.first.unwrap().value_ids, vec![50]);
    }
}
