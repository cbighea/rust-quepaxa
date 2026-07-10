use rust_quepaxa::{
    AdaptivePolicy, ClusterIdentity, Decision, FixedLeaderPolicy, HedgingPolicy, LaneId,
    LastWinnerPolicy, LeaderlessPolicy, Priority, PrioritySource, Proposal, ProposalKey,
    ProposerCore, QuePaxaError, RecordReply, RecordRequest, RecordSummary, RecorderClient,
    RecorderConfig, RecorderCore, RecorderHandle, RecorderLimits, ReplicaConfig, ReplicaCore,
    ReplicaId, RoundRobinPolicy, SlotIndex, Step,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone)]
struct FixedPriorities {
    next: u64,
}

impl PrioritySource for FixedPriorities {
    fn next_below(&mut self, _exclusive_upper_bound: Priority) -> rust_quepaxa::Result<Priority> {
        let value = self.next;
        self.next += 1;
        Ok(Priority::new(value.max(1)))
    }
}

fn ids(count: u64) -> Vec<ReplicaId> {
    (1..=count).map(ReplicaId::new).collect()
}

fn core(id: ReplicaId, members: &[ReplicaId]) -> RecorderCore<u64> {
    RecorderCore::permissive_for_tests(id, members.to_vec()).unwrap()
}

fn recorders(count: u64) -> Vec<RecorderHandle<RecorderCore<u64>>> {
    let members = ids(count);
    members
        .iter()
        .map(|id| RecorderHandle::new(*id, core(*id, &members)))
        .collect()
}

struct PartitionedRecorder {
    inner: RecorderCore<u64>,
    reachable: bool,
}

impl RecorderClient<u64> for PartitionedRecorder {
    fn record(&mut self, request: RecordRequest<u64>) -> rust_quepaxa::Result<RecordReply<u64>> {
        if !self.reachable {
            return Err(QuePaxaError::TransportError("simulated partition".into()));
        }
        self.inner.record(request)
    }
}

fn partitioned_recorders(
    count: u64,
    reachable: &[usize],
) -> Vec<RecorderHandle<PartitionedRecorder>> {
    let members = ids(count);
    members
        .iter()
        .enumerate()
        .map(|(index, id)| {
            RecorderHandle::new(
                *id,
                PartitionedRecorder {
                    inner: core(*id, &members),
                    reachable: reachable.contains(&index),
                },
            )
        })
        .collect()
}

#[test]
fn leader_decides_in_one_round_trip() {
    let recorders = recorders(3);
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    );

    let decision = proposer
        .propose(
            SlotIndex::new(1),
            vec![10],
            Some(ReplicaId::new(1)),
            &recorders,
            &[],
        )
        .unwrap();

    assert_eq!(decision.value_ids, vec![10]);
    assert_eq!(decision.decided_step, Step::ROUND_ONE_PHASE_ZERO);
}

#[test]
fn leaderless_quorum_decides_with_two_faults_of_five() {
    for _ in 0..32 {
        let recorders = partitioned_recorders(5, &[0, 1, 2]);
        let mut proposer = ProposerCore::with_rng(
            ReplicaId::new(1),
            LaneId::new(1),
            FixedPriorities { next: 1 },
        );

        let decision = proposer
            .propose(SlotIndex::new(1), vec![11], None, &recorders, &[])
            .unwrap();

        assert_eq!(decision.value_ids, vec![11]);
        assert_eq!(decision.decided_step, Step::new(6));
    }
}

#[test]
fn later_proposer_converges_on_fast_path_decision() {
    let recorders = recorders(3);
    let mut first = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    );
    let first_decision = first
        .propose(
            SlotIndex::new(1),
            vec![1],
            Some(ReplicaId::new(1)),
            &recorders,
            &[],
        )
        .unwrap();

    let mut second = ProposerCore::with_rng(
        ReplicaId::new(2),
        LaneId::new(1),
        FixedPriorities { next: 100 },
    );
    let second_decision = second
        .propose(
            SlotIndex::new(1),
            vec![2],
            Some(ReplicaId::new(1)),
            &recorders,
            &[],
        )
        .unwrap();

    assert_eq!(first_decision.value_ids, second_decision.value_ids);
}

struct BlockingRecorder {
    inner: RecorderCore<u64>,
    block: bool,
    release: Arc<AtomicBool>,
}

impl RecorderClient<u64> for BlockingRecorder {
    fn record(&mut self, request: RecordRequest<u64>) -> rust_quepaxa::Result<RecordReply<u64>> {
        if self.block {
            while !self.release.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
        }
        self.inner.record(request)
    }
}

#[test]
fn blocked_recorder_cannot_prevent_parallel_quorum() {
    let members = ids(3);
    let release = Arc::new(AtomicBool::new(false));
    let recorders = vec![
        RecorderHandle::new(
            ReplicaId::new(1),
            BlockingRecorder {
                inner: core(ReplicaId::new(1), &members),
                block: true,
                release: Arc::clone(&release),
            },
        ),
        RecorderHandle::new(
            ReplicaId::new(2),
            BlockingRecorder {
                inner: core(ReplicaId::new(2), &members),
                block: false,
                release: Arc::clone(&release),
            },
        ),
        RecorderHandle::new(
            ReplicaId::new(3),
            BlockingRecorder {
                inner: core(ReplicaId::new(3), &members),
                block: false,
                release: Arc::clone(&release),
            },
        ),
    ];
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(2),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    );

    let decision = proposer
        .propose(
            SlotIndex::new(1),
            vec![20],
            Some(ReplicaId::new(2)),
            &recorders,
            &[],
        )
        .unwrap();
    release.store(true, Ordering::Release);

    assert_eq!(decision.value_ids, vec![20]);
}

#[test]
fn duplicate_recorder_id_is_rejected_before_a_quorum_is_counted() {
    let members = ids(3);
    let repeated = RecorderHandle::new(ReplicaId::new(1), core(ReplicaId::new(1), &members));
    let recorders = vec![
        repeated.clone(),
        repeated,
        RecorderHandle::new(ReplicaId::new(3), core(ReplicaId::new(3), &members)),
    ];
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    );

    assert_eq!(
        proposer
            .propose(
                SlotIndex::new(1),
                vec![1],
                Some(ReplicaId::new(1)),
                &recorders,
                &[],
            )
            .unwrap_err(),
        QuePaxaError::DuplicateReplica(ReplicaId::new(1))
    );
}

#[derive(Debug)]
struct ForgedReply;

impl RecorderClient<u64> for ForgedReply {
    fn record(&mut self, request: RecordRequest<u64>) -> rust_quepaxa::Result<RecordReply<u64>> {
        Ok(RecordReply {
            recorder: ReplicaId::new(99),
            cluster: ClusterIdentity::new([ReplicaId::new(1)], 0).unwrap(),
            summary: RecordSummary {
                step: request.step,
                first: Some(request.proposal),
                prior_aggregate: None,
            },
            decision: None,
        })
    }
}

#[test]
fn forged_recorder_identity_is_rejected() {
    let recorders = vec![
        RecorderHandle::new(ReplicaId::new(1), ForgedReply),
        RecorderHandle::new(ReplicaId::new(2), ForgedReply),
        RecorderHandle::new(ReplicaId::new(3), ForgedReply),
    ];
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    );

    assert!(matches!(
        proposer.propose(
            SlotIndex::new(1),
            vec![1],
            Some(ReplicaId::new(1)),
            &recorders,
            &[],
        ),
        Err(QuePaxaError::InvalidRecorderReply { .. })
    ));
}

#[test]
fn recorder_rejects_unauthorized_sender_and_large_steps() {
    let members = ids(3);
    let mut recorder = core(ReplicaId::new(1), &members);
    let proposal = Proposal::new(
        ProposalKey::new(Priority::new(1), ReplicaId::new(2), LaneId::new(1)),
        vec![7],
    )
    .unwrap();

    assert_eq!(
        recorder
            .record(RecordRequest {
                sender: ReplicaId::new(9),
                slot: SlotIndex::new(1),
                round_one_leader: Some(ReplicaId::new(1)),
                step: Step::ROUND_ONE_PHASE_ZERO,
                proposal: proposal.clone(),
                known_decisions: Vec::new(),
            })
            .unwrap_err(),
        QuePaxaError::InvalidSender(ReplicaId::new(9))
    );
    assert_eq!(
        recorder
            .record_from(
                ReplicaId::new(1),
                RecordRequest {
                    sender: ReplicaId::new(2),
                    slot: SlotIndex::new(1),
                    round_one_leader: Some(ReplicaId::new(1)),
                    step: Step::ROUND_ONE_PHASE_ZERO,
                    proposal: proposal.clone(),
                    known_decisions: Vec::new(),
                },
            )
            .unwrap_err(),
        QuePaxaError::InvalidSender(ReplicaId::new(2))
    );
    assert!(matches!(
        recorder.record(RecordRequest {
            sender: ReplicaId::new(2),
            slot: SlotIndex::new(1),
            round_one_leader: Some(ReplicaId::new(1)),
            step: Step::new(1028),
            proposal,
            known_decisions: Vec::new(),
        }),
        Err(QuePaxaError::InvalidStep { .. })
    ));
}

struct MissingAvailability;

impl rust_quepaxa::ValueAvailability<u64> for MissingAvailability {
    fn ensure_available(&self, _value_ids: &[u64]) -> rust_quepaxa::Result<()> {
        Err(QuePaxaError::MissingValue)
    }
}

#[test]
fn recorder_fails_closed_when_payload_is_missing() {
    let members = ids(1);
    let mut recorder = RecorderCore::new(
        RecorderConfig::new(ReplicaId::new(1), members, 0).unwrap(),
        Arc::new(MissingAvailability),
    );
    let proposal = Proposal::new(
        ProposalKey::new(Priority::new(1), ReplicaId::new(1), LaneId::new(1)),
        vec![7],
    )
    .unwrap();

    assert_eq!(
        recorder
            .record(RecordRequest {
                sender: ReplicaId::new(1),
                slot: SlotIndex::new(1),
                round_one_leader: Some(ReplicaId::new(1)),
                step: Step::ROUND_ONE_PHASE_ZERO,
                proposal,
                known_decisions: Vec::new(),
            })
            .unwrap_err(),
        QuePaxaError::MissingValue
    );
}

#[test]
fn recorder_bounds_active_slots_and_supports_checkpoint_pruning() {
    let members = ids(1);
    let limits = RecorderLimits {
        max_active_slots: 1,
        ..RecorderLimits::default()
    };
    let mut recorder = RecorderCore::new(
        RecorderConfig::new(ReplicaId::new(1), members, 0)
            .unwrap()
            .with_limits(limits)
            .unwrap(),
        Arc::new(rust_quepaxa::AllowAllAvailability),
    );
    let request = |slot| RecordRequest {
        sender: ReplicaId::new(1),
        slot: SlotIndex::new(slot),
        round_one_leader: Some(ReplicaId::new(1)),
        step: Step::ROUND_ONE_PHASE_ZERO,
        proposal: Proposal::new(
            ProposalKey::new(Priority::new(1), ReplicaId::new(1), LaneId::new(1)),
            vec![slot],
        )
        .unwrap(),
        known_decisions: Vec::new(),
    };

    recorder.record(request(1)).unwrap();
    assert!(matches!(
        recorder.record(request(2)),
        Err(QuePaxaError::ResourceLimit { .. })
    ));
    recorder.prune_through(SlotIndex::new(1));
    recorder.record(request(2)).unwrap();
}

#[test]
fn policies_cover_expected_leader_modes() {
    let replicas = ids(5);
    let fixed = FixedLeaderPolicy;
    let round_robin = RoundRobinPolicy::new(10);
    let leaderless = LeaderlessPolicy;
    let last_winner = LastWinnerPolicy;
    let mut adaptive = AdaptivePolicy::new(10);
    adaptive.record_epoch(ReplicaId::new(5), Duration::from_millis(1));
    adaptive.record_epoch(ReplicaId::new(1), Duration::from_millis(10));

    assert_eq!(
        fixed
            .leader_sequence(SlotIndex::new(1), &replicas, None)
            .unwrap()[0],
        ReplicaId::new(1)
    );
    assert_eq!(
        round_robin
            .leader_sequence(SlotIndex::new(10), &replicas, None)
            .unwrap()[0],
        ReplicaId::new(2)
    );
    let sequence = leaderless
        .leader_sequence(SlotIndex::new(1), &replicas, None)
        .unwrap();
    assert_eq!(
        leaderless
            .delay_for(ReplicaId::new(3), &sequence, Duration::from_millis(5))
            .unwrap(),
        Duration::ZERO
    );
    assert_eq!(
        last_winner
            .leader_sequence(SlotIndex::new(1), &replicas, Some(ReplicaId::new(4)))
            .unwrap()[0],
        ReplicaId::new(4)
    );
    assert_eq!(
        adaptive
            .leader_sequence(SlotIndex::new(200), &replicas, None)
            .unwrap()[0],
        ReplicaId::new(5)
    );
}

#[test]
fn replica_pipeline_commits_in_order_and_requeues_losing_values() {
    let mut replica = ReplicaCore::new(ReplicaConfig {
        batch_size: 1,
        pipeline_len: 3,
        ..ReplicaConfig::default()
    });
    replica.enqueue([10, 20, 30, 40]).unwrap();
    assert_eq!(
        replica.next_proposal().unwrap(),
        Some((SlotIndex::new(1), vec![10]))
    );
    assert_eq!(
        replica.next_proposal().unwrap(),
        Some((SlotIndex::new(2), vec![20]))
    );
    assert_eq!(
        replica.next_proposal().unwrap(),
        Some((SlotIndex::new(3), vec![30]))
    );

    for (slot, value) in [(2, 20), (3, 31)] {
        assert!(
            replica
                .apply_decision(
                    Decision::new(
                        SlotIndex::new(slot),
                        vec![value],
                        ReplicaId::new(2),
                        Step::new(6)
                    )
                    .unwrap(),
                )
                .unwrap()
                .is_empty()
        );
    }
    let committed = replica
        .apply_decision(
            Decision::new(SlotIndex::new(1), vec![99], ReplicaId::new(3), Step::new(6)).unwrap(),
        )
        .unwrap();
    assert_eq!(committed.len(), 3);
    assert_eq!(replica.pending_len(), 3);
}

#[test]
fn replica_enforces_pending_and_log_retention_limits() {
    let mut replica = ReplicaCore::new(ReplicaConfig {
        batch_size: 1,
        pipeline_len: 1,
        max_pending_values: 2,
        max_log_slots: 1,
        max_tracked_value_ids: 2,
    });

    replica.enqueue([1, 2]).unwrap();
    assert!(matches!(
        replica.enqueue([3]),
        Err(QuePaxaError::ResourceLimit { .. })
    ));
    let (slot, _) = replica.next_proposal().unwrap().unwrap();
    assert_eq!(slot, SlotIndex::new(1));
    assert!(matches!(
        replica.note_proposed(SlotIndex::new(2), vec![2]),
        Err(QuePaxaError::ResourceLimit { .. })
    ));
}

#[test]
fn logical_counters_do_not_wrap() {
    assert!(Step::new(u64::MAX).checked_next().is_none());
    assert!(SlotIndex::new(u64::MAX).checked_next().is_none());
}
