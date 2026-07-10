use rust_quepaxa::{
    Decision, LaneId, Priority, Proposal, ProposalKey, QuePaxaError, RecordReply, RecordRequest,
    RecorderCore, ReplicaId, SlotIndex, Step,
};

const QUORUM: usize = 2;
const MAX_EVENTS: usize = 10_000;

#[derive(Clone)]
struct Delivery {
    proposer: usize,
    recorder: usize,
    generation: u64,
    request: RecordRequest<u64>,
}

struct SimulatedProposer {
    id: ReplicaId,
    leader: ReplicaId,
    step: Step,
    proposal: Proposal<u64>,
    generation: u64,
    replies: Vec<RecordReply<u64>>,
    decision: Option<Decision<u64>>,
    halted: bool,
    seed: u64,
}

impl SimulatedProposer {
    fn new(id: u64, leader: u64, value: u64, seed: u64) -> Self {
        let id = ReplicaId::new(id);
        Self {
            id,
            leader: ReplicaId::new(leader),
            step: Step::ROUND_ONE_PHASE_ZERO,
            proposal: Proposal::new(
                ProposalKey::new(Priority::MAX, id, LaneId::new(1)),
                vec![value],
            )
            .unwrap(),
            generation: 0,
            replies: Vec::new(),
            decision: None,
            halted: false,
            seed,
        }
    }

    fn start_round(&mut self, proposer: usize, recorder_count: usize) -> Vec<Delivery> {
        if self.decision.is_some() || self.halted {
            return Vec::new();
        }
        self.generation += 1;
        self.replies.clear();
        (0..recorder_count)
            .map(|recorder| {
                let proposal = if self.step.phase() == 0
                    && (self.step > Step::ROUND_ONE_PHASE_ZERO || self.leader != self.id)
                {
                    self.proposal.with_priority(self.priority_for(recorder))
                } else {
                    self.proposal.clone()
                };
                Delivery {
                    proposer,
                    recorder,
                    generation: self.generation,
                    request: RecordRequest {
                        sender: self.id,
                        slot: SlotIndex::new(1),
                        round_one_leader: Some(self.leader),
                        step: self.step,
                        proposal,
                        known_decisions: Vec::new(),
                    },
                }
            })
            .collect()
    }

    fn priority_for(&self, recorder: usize) -> Priority {
        let mixed = mix(self.seed
            ^ self.id.get().wrapping_mul(0x9e37_79b9_7f4a_7c15)
            ^ self.step.get().wrapping_mul(0xbf58_476d_1ce4_e5b9)
            ^ recorder as u64);
        Priority::new(mixed % (Priority::MAX.get() - 1) + 1)
    }

    fn receive(
        &mut self,
        generation: u64,
        result: Result<RecordReply<u64>, QuePaxaError>,
        proposer: usize,
        recorder_count: usize,
    ) -> Vec<Delivery> {
        if generation != self.generation || self.decision.is_some() || self.halted {
            return Vec::new();
        }
        match result {
            Ok(reply) => self.replies.push(reply),
            Err(QuePaxaError::ScheduleMismatch { .. })
            | Err(QuePaxaError::ConflictingDecision { .. })
            | Err(QuePaxaError::SlotPruned { .. }) => {
                self.halted = true;
                return Vec::new();
            }
            Err(_) => return Vec::new(),
        }
        if self.replies.len() < QUORUM {
            return Vec::new();
        }
        self.advance();
        self.start_round(proposer, recorder_count)
    }

    fn advance(&mut self) {
        if let Some(decision) = self.replies.iter().find_map(|reply| reply.decision.clone()) {
            self.decision = Some(decision);
            return;
        }
        if self
            .replies
            .iter()
            .all(|reply| reply.summary.step == self.step)
        {
            match self.step.phase() {
                0 => {
                    if let Some(winner) = self.fast_path_winner() {
                        self.decision = Some(
                            Decision::new(
                                SlotIndex::new(1),
                                winner.value_ids,
                                winner.key.proposer_id,
                                self.step,
                            )
                            .unwrap(),
                        );
                        return;
                    }
                    self.proposal = max_proposal(
                        self.replies
                            .iter()
                            .filter_map(|reply| reply.summary.first.clone()),
                    )
                    .unwrap();
                }
                1 => {}
                2 => {
                    if max_proposal(
                        self.replies
                            .iter()
                            .filter_map(|reply| reply.summary.prior_aggregate.clone()),
                    )
                    .is_some_and(|proposal| proposal == self.proposal)
                    {
                        self.decision = Some(
                            Decision::new(
                                SlotIndex::new(1),
                                self.proposal.value_ids.clone(),
                                self.proposal.key.proposer_id,
                                self.step,
                            )
                            .unwrap(),
                        );
                        return;
                    }
                }
                3 => {
                    if let Some(proposal) = max_proposal(
                        self.replies
                            .iter()
                            .filter_map(|reply| reply.summary.prior_aggregate.clone()),
                    ) {
                        self.proposal = proposal;
                    }
                }
                _ => unreachable!(),
            }
            self.step = self.step.checked_next().unwrap();
        } else if let Some(reply) = self.replies.iter().max_by_key(|reply| reply.summary.step)
            && reply.summary.step > self.step
        {
            self.step = reply.summary.step;
            self.proposal = reply.summary.first.clone().unwrap();
        }
        if self.step.get() > 128 {
            self.halted = true;
        }
    }

    fn fast_path_winner(&self) -> Option<Proposal<u64>> {
        if self.step != Step::ROUND_ONE_PHASE_ZERO {
            return None;
        }
        let winner = self.replies.first()?.summary.first.as_ref()?;
        (winner.key.priority == Priority::MAX
            && winner.key.proposer_id == self.leader
            && self.replies.iter().all(|reply| {
                reply
                    .summary
                    .first
                    .as_ref()
                    .is_some_and(|proposal| proposal.key == winner.key)
            }))
        .then(|| winner.clone())
    }
}

fn max_proposal(proposals: impl IntoIterator<Item = Proposal<u64>>) -> Option<Proposal<u64>> {
    proposals.into_iter().max_by_key(|proposal| proposal.key)
}

fn run(seed: u64, leaders: [u64; 2]) -> [Option<Decision<u64>>; 2] {
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let mut recorders = members
        .iter()
        .map(|id| RecorderCore::permissive_for_tests(*id, members.clone()).unwrap())
        .collect::<Vec<_>>();
    let mut proposers = [
        SimulatedProposer::new(1, leaders[0], 10, seed),
        SimulatedProposer::new(2, leaders[1], 20, seed ^ 0xa5a5_a5a5_a5a5_a5a5),
    ];
    let mut events = Vec::new();
    for (index, proposer) in proposers.iter_mut().enumerate() {
        events.extend(proposer.start_round(index, recorders.len()));
    }
    let mut random = seed.max(1);
    for _ in 0..MAX_EVENTS {
        if events.is_empty()
            || proposers
                .iter()
                .all(|proposer| proposer.decision.is_some() || proposer.halted)
        {
            break;
        }
        random = mix(random);
        let event = events.swap_remove(random as usize % events.len());
        let result = recorders[event.recorder].record(event.request);
        let next = proposers[event.proposer].receive(
            event.generation,
            result,
            event.proposer,
            recorders.len(),
        );
        events.extend(next);
    }
    [proposers[0].decision.clone(), proposers[1].decision.clone()]
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[test]
fn randomized_record_call_interleavings_preserve_agreement() {
    for seed in 1..=10_000 {
        let leader = seed % 2 + 1;
        let decisions = run(seed, [leader, leader]);
        let first = decisions[0]
            .as_ref()
            .unwrap_or_else(|| panic!("first proposer did not decide for seed {seed}"));
        let second = decisions[1]
            .as_ref()
            .unwrap_or_else(|| panic!("second proposer did not decide for seed {seed}"));
        assert_eq!(first.value_ids, second.value_ids, "seed {seed}");
    }
}

#[test]
fn divergent_leader_schedules_cannot_produce_conflicting_decisions() {
    for seed in 1..=2_000 {
        let decisions = run(seed, [1, 2]);
        if let [Some(first), Some(second)] = &decisions {
            assert_eq!(first.value_ids, second.value_ids, "seed {seed}");
        }
    }
}
