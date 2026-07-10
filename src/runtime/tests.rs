use super::*;
use crate::proposer::PrioritySource;
use crate::recorder::{RecorderConfig, RecorderCore};
use crate::types::{ClusterIdentity, Priority, RecordRequest, Step};

#[derive(Clone)]
struct FixedPriorities {
    next: u64,
}

impl PrioritySource for FixedPriorities {
    fn next_below(&mut self, _exclusive_upper_bound: Priority) -> Result<Priority> {
        let priority = Priority::new(self.next.max(1));
        self.next += 1;
        Ok(priority)
    }
}

#[derive(Default)]
struct RecordingStateMachine {
    executed: Vec<Decision<u64>>,
    imported_checkpoint: Vec<u8>,
}

impl StateMachine<u64> for RecordingStateMachine {
    fn execute(&mut self, decision: &Decision<u64>) -> Result<()> {
        self.executed.push(decision.clone());
        Ok(())
    }

    fn export_checkpoint(&mut self, through: SlotIndex) -> Result<Vec<u8>> {
        Ok(through.get().to_be_bytes().to_vec())
    }

    fn import_checkpoint(&mut self, _through: SlotIndex, checkpoint: &[u8]) -> Result<()> {
        self.imported_checkpoint = checkpoint.to_vec();
        Ok(())
    }
}

#[derive(Default)]
struct RecordingNotifier {
    notified: Vec<Decision<u64>>,
}

impl ClientNotifier<u64> for RecordingNotifier {
    fn committed(&mut self, decision: &Decision<u64>) -> Result<()> {
        self.notified.push(decision.clone());
        Ok(())
    }
}

type TestStore = Arc<Mutex<InMemoryRuntimeStore<u64>>>;
type TestRuntime = ReplicaRuntime<
    u64,
    RecorderCore<u64>,
    FixedPriorities,
    TestStore,
    RecordingStateMachine,
    RecordingNotifier,
>;

fn members() -> Vec<ReplicaId> {
    (1..=3).map(ReplicaId::new).collect()
}

fn recorders(members: &[ReplicaId]) -> Vec<RecorderHandle<RecorderCore<u64>>> {
    members
        .iter()
        .map(|id| {
            RecorderHandle::new(
                *id,
                RecorderCore::permissive_for_tests(*id, members.to_vec()).unwrap(),
            )
        })
        .collect()
}

fn runtime(
    replica_id: ReplicaId,
    recorders: Vec<RecorderHandle<RecorderCore<u64>>>,
    state_store: TestStore,
    base_hedge_delay: Duration,
) -> TestRuntime {
    let members = members();
    ReplicaRuntime::new(
        ReplicaRuntimeConfig::new(
            replica_id,
            LaneId::new(1),
            members,
            1,
            ReplicaConfig {
                batch_size: 1,
                pipeline_len: 2,
                ..ReplicaConfig::default()
            },
            16,
            base_hedge_delay,
        )
        .unwrap(),
        ProposerCore::with_rng(replica_id, LaneId::new(1), FixedPriorities { next: 1 }),
        recorders,
        state_store,
        RecordingStateMachine::default(),
        RecordingNotifier::default(),
    )
    .unwrap()
}

fn install_first_epoch(runtime: &mut TestRuntime) {
    runtime
        .install_epoch_schedule(EpochSchedule::new(0, members()).unwrap())
        .unwrap();
}

#[test]
fn runtime_submits_executes_notifies_and_disseminates() {
    let members = members();
    let recorders = recorders(&members);
    let store = Arc::new(Mutex::new(InMemoryRuntimeStore::default()));
    let mut runtime = runtime(
        ReplicaId::new(1),
        recorders.clone(),
        Arc::clone(&store),
        Duration::ZERO,
    );
    install_first_epoch(&mut runtime);
    runtime.submit([10]).unwrap();

    assert!(matches!(
        runtime.run_once().unwrap(),
        RuntimePoll::Committed(_)
    ));
    assert_eq!(runtime.state_machine().executed.len(), 1);
    assert_eq!(runtime.notifier().notified.len(), 1);
    assert_eq!(
        runtime.decision(SlotIndex::new(1)).unwrap().value_ids,
        vec![10]
    );
    for recorder in &recorders {
        assert_eq!(
            recorder
                .with_client(|core| core.decisions().count())
                .unwrap(),
            1
        );
    }
    assert!(store.lock().unwrap().snapshot().is_some());
}

#[test]
fn remote_decision_suppresses_a_later_hedged_proposal() {
    let members = members();
    let recorders = recorders(&members);
    let store = Arc::new(Mutex::new(InMemoryRuntimeStore::default()));
    let mut runtime = runtime(
        ReplicaId::new(2),
        recorders,
        Arc::clone(&store),
        Duration::from_secs(1),
    );
    install_first_epoch(&mut runtime);
    runtime.submit([20]).unwrap();
    let now = Instant::now();

    assert!(matches!(
        runtime.poll(now).unwrap(),
        RuntimePoll::Waiting { .. }
    ));
    let decision = Decision::new(
        SlotIndex::new(1),
        vec![99],
        ReplicaId::new(1),
        Step::ROUND_ONE_PHASE_ZERO,
    )
    .unwrap();
    let committed = runtime.receive_decision(decision).unwrap();

    assert_eq!(committed.len(), 1);
    assert_eq!(runtime.state_machine().executed[0].value_ids, vec![99]);
    assert_eq!(runtime.notifier().notified[0].value_ids, vec![99]);
}

#[test]
fn recorder_progress_defers_a_hedged_proposal_by_another_interval() {
    let members = members();
    let recorders = recorders(&members);
    recorders[0]
        .with_client(|core| {
            core.record(RecordRequest {
                sender: ReplicaId::new(1),
                slot: SlotIndex::new(1),
                round_one_leader: Some(ReplicaId::new(1)),
                step: Step::ROUND_ONE_PHASE_ZERO,
                proposal: crate::Proposal::new(
                    crate::ProposalKey::new(Priority::MAX, ReplicaId::new(1), LaneId::new(1)),
                    vec![99],
                )
                .unwrap(),
                known_decisions: Vec::new(),
            })
            .unwrap();
        })
        .unwrap();
    let mut runtime = runtime(
        ReplicaId::new(2),
        recorders,
        Arc::new(Mutex::new(InMemoryRuntimeStore::default())),
        Duration::from_millis(10),
    );
    install_first_epoch(&mut runtime);
    runtime.submit([20]).unwrap();
    let now = Instant::now();
    let first_deadline = match runtime.poll(now).unwrap() {
        RuntimePoll::Waiting { until, .. } => until,
        outcome => panic!("unexpected poll result: {outcome:?}"),
    };
    let second_deadline = match runtime.poll(first_deadline).unwrap() {
        RuntimePoll::Waiting { until, .. } => until,
        outcome => panic!("unexpected poll result: {outcome:?}"),
    };
    assert!(second_deadline > first_deadline);
}

#[test]
fn configured_noop_recovers_an_idle_commit_gap() {
    let members = members();
    let mut runtime = runtime(
        ReplicaId::new(2),
        recorders(&members),
        Arc::new(Mutex::new(InMemoryRuntimeStore::default())),
        Duration::ZERO,
    )
    .with_noop_value(0);
    install_first_epoch(&mut runtime);
    runtime
        .receive_decision(
            Decision::new(
                SlotIndex::new(2),
                vec![22],
                ReplicaId::new(1),
                Step::ROUND_ONE_PHASE_ZERO,
            )
            .unwrap(),
        )
        .unwrap();

    let committed = match runtime.poll(Instant::now()).unwrap() {
        RuntimePoll::Committed(committed) => committed,
        outcome => panic!("unexpected poll result: {outcome:?}"),
    };
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].slot, SlotIndex::new(1));
    assert_eq!(committed[0].value_ids, vec![0]);
    assert_eq!(committed[1].slot, SlotIndex::new(2));
}

#[test]
fn runtime_recovers_persisted_state_and_can_continue_after_checkpoint() {
    let members = members();
    let initial_recorders = recorders(&members);
    let store = Arc::new(Mutex::new(InMemoryRuntimeStore::default()));
    let mut first = runtime(
        ReplicaId::new(1),
        initial_recorders,
        Arc::clone(&store),
        Duration::ZERO,
    );
    install_first_epoch(&mut first);
    first.submit([30]).unwrap();
    first.run_once().unwrap();
    drop(first);

    let mut recovered = runtime(
        ReplicaId::new(1),
        recorders(&members),
        Arc::clone(&store),
        Duration::ZERO,
    );
    assert!(recovered.recover().unwrap().is_empty());
    assert!(recovered.state_machine().executed.is_empty());
    assert_eq!(
        recovered.decision(SlotIndex::new(1)).unwrap().value_ids,
        vec![30]
    );

    recovered.checkpoint_through(SlotIndex::new(1)).unwrap();
    drop(recovered);

    let mut after_checkpoint = runtime(
        ReplicaId::new(1),
        recorders(&members),
        Arc::clone(&store),
        Duration::ZERO,
    );
    after_checkpoint.submit([31]).unwrap();
    after_checkpoint.run_once().unwrap();
    assert_eq!(
        after_checkpoint
            .decision(SlotIndex::new(2))
            .unwrap()
            .value_ids,
        vec![31]
    );
}

#[test]
fn checkpoint_requires_full_dissemination() {
    let members = members();
    let mut runtime = runtime(
        ReplicaId::new(1),
        recorders(&members),
        Arc::new(Mutex::new(InMemoryRuntimeStore::default())),
        Duration::ZERO,
    );
    install_first_epoch(&mut runtime);
    runtime.submit([1]).unwrap();
    runtime.run_once().unwrap();
    runtime
        .announced_to
        .get_mut(&SlotIndex::new(1))
        .unwrap()
        .remove(&ReplicaId::new(3));
    runtime.recorders.pop();

    assert!(matches!(
        runtime.checkpoint_through(SlotIndex::new(1)),
        Err(QuePaxaError::QuorumNotReached {
            needed: 3,
            received
        }) if received < 3
    ));
}

#[test]
fn state_transfer_installs_application_and_consensus_checkpoints() {
    let members = members();
    let mut source = runtime(
        ReplicaId::new(1),
        recorders(&members),
        Arc::new(Mutex::new(InMemoryRuntimeStore::default())),
        Duration::ZERO,
    );
    install_first_epoch(&mut source);
    source.submit([30]).unwrap();
    source.run_once().unwrap();
    let transfer = source.create_state_transfer(SlotIndex::new(1)).unwrap();
    assert_eq!(transfer.checkpointed_value_ids, vec![30]);
    assert!(source.decision(SlotIndex::new(1)).is_none());

    let destination_recorders = recorders(&members);
    for recorder in &destination_recorders {
        recorder
            .with_client(|core| core.install_state_transfer_floor(SlotIndex::new(1)))
            .unwrap();
    }
    let mut destination = runtime(
        ReplicaId::new(2),
        destination_recorders,
        Arc::new(Mutex::new(InMemoryRuntimeStore::default())),
        Duration::ZERO,
    );
    destination.install_state_transfer(transfer).unwrap();
    assert_eq!(
        destination.replica().checkpointed_through(),
        SlotIndex::new(1)
    );
    assert_eq!(
        destination.state_machine().imported_checkpoint,
        1_u64.to_be_bytes()
    );
    destination.submit([31]).unwrap();
    destination.run_once().unwrap();
    assert_eq!(
        destination.decision(SlotIndex::new(2)).unwrap().value_ids,
        vec![31]
    );
}

#[test]
fn automatic_epoch_tuning_is_deterministic_from_committed_history() {
    let members = members();
    let mut first = EpochTuner::new(2, members.clone(), true);
    let mut second = EpochTuner::new(2, members, true);
    for slot in 1..=11 {
        let step = Step::new(if slot % 3 == 0 { 8 } else { 4 });
        first.note_committed(SlotIndex::new(slot), step).unwrap();
        second.note_committed(SlotIndex::new(slot), step).unwrap();
    }
    assert_eq!(
        first.ensure_schedule(7).unwrap(),
        second.ensure_schedule(7).unwrap()
    );
}

#[test]
fn automatic_epoch_tuning_periodically_reexplores() {
    let mut tuner = EpochTuner::new(1, members(), true);
    for slot in 1..=39 {
        tuner
            .note_committed(SlotIndex::new(slot), Step::ROUND_ONE_PHASE_ZERO)
            .unwrap();
    }

    assert_eq!(
        tuner.ensure_schedule(39).unwrap().leader(),
        ReplicaId::new(2)
    );
}

#[test]
fn adaptive_hedging_uses_wall_clock_completion_samples() {
    let members = members();
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members.clone(),
        1,
        ReplicaConfig {
            batch_size: 1,
            pipeline_len: 1,
            ..ReplicaConfig::default()
        },
        16,
        Duration::from_millis(500),
    )
    .unwrap()
    .with_adaptive_hedging(
        AdaptiveHedgingConfig::new(Duration::from_millis(1), Duration::from_millis(500)).unwrap(),
    );
    let mut runtime = ReplicaRuntime::new(
        config,
        ProposerCore::with_rng(
            ReplicaId::new(1),
            LaneId::new(1),
            FixedPriorities { next: 1 },
        ),
        recorders(&members),
        Arc::new(Mutex::new(InMemoryRuntimeStore::default())),
        RecordingStateMachine::default(),
        RecordingNotifier::default(),
    )
    .unwrap();
    install_first_epoch(&mut runtime);
    runtime.submit([77]).unwrap();
    runtime.run_once().unwrap();

    assert!(runtime.current_hedge_delay() < Duration::from_millis(500));
    assert!(runtime.current_hedge_delay() >= Duration::from_millis(1));
}

#[test]
fn retained_decisions_do_not_overflow_consensus_requests() {
    let state_store = Arc::new(Mutex::new(InMemoryRuntimeStore::default()));
    let mut runtime = runtime(
        ReplicaId::new(1),
        recorders(&members()),
        state_store,
        Duration::ZERO,
    );
    install_first_epoch(&mut runtime);
    for epoch in 1..=18 {
        runtime
            .install_epoch_schedule(EpochSchedule::new(epoch, members()).unwrap())
            .unwrap();
    }

    for value in 1..=300 {
        runtime.submit([value]).unwrap();
        assert!(matches!(
            runtime.run_once().unwrap(),
            RuntimePoll::Committed(_)
        ));
    }

    assert!(runtime.decision(SlotIndex::new(300)).is_some());
}

#[test]
fn runtime_moves_through_joint_consensus_before_finalizing_membership() {
    let old_members = members();
    let old_recorders = recorders(&old_members);
    let mut runtime = runtime(
        ReplicaId::new(1),
        old_recorders.clone(),
        Arc::new(Mutex::new(InMemoryRuntimeStore::default())),
        Duration::ZERO,
    );
    install_first_epoch(&mut runtime);
    runtime.submit([100]).unwrap();
    runtime.run_once().unwrap();
    let first = runtime.decision(SlotIndex::new(1)).unwrap().clone();

    let joint = runtime
        .config()
        .cluster_identity()
        .begin_joint([ReplicaId::new(1), ReplicaId::new(4), ReplicaId::new(5)], 1)
        .unwrap();
    let mut joint_recorders = old_recorders;
    for id in [ReplicaId::new(4), ReplicaId::new(5)] {
        joint_recorders.push(RecorderHandle::new(
            id,
            RecorderCore::new(
                RecorderConfig::from_cluster(id, joint.clone()).unwrap(),
                Arc::new(crate::AllowAllAvailability),
            ),
        ));
    }
    runtime
        .install_membership(
            MembershipChange::new(first, joint.clone(), 100).unwrap(),
            joint_recorders,
        )
        .unwrap();
    assert!(runtime.config().cluster_identity().is_joint());
    runtime
        .install_epoch_schedule(EpochSchedule::new(0, joint.members().to_vec()).unwrap())
        .unwrap();
    runtime.submit([200]).unwrap();
    runtime.run_once().unwrap();
    let second = runtime.decision(SlotIndex::new(2)).unwrap().clone();

    let stable = joint.finalize_joint().unwrap();
    let final_recorders = runtime
        .recorders
        .iter()
        .filter(|recorder| stable.contains(recorder.id()))
        .cloned()
        .collect::<Vec<_>>();
    runtime
        .install_membership(
            MembershipChange::new(second, stable.clone(), 200).unwrap(),
            final_recorders,
        )
        .unwrap();
    assert!(!runtime.config().cluster_identity().is_joint());
    assert_eq!(runtime.config().members(), stable.members());
    runtime
        .install_epoch_schedule(EpochSchedule::new(0, stable.members().to_vec()).unwrap())
        .unwrap();
    runtime.submit([300]).unwrap();
    runtime.run_once().unwrap();
    assert_eq!(
        runtime.decision(SlotIndex::new(3)).unwrap().value_ids,
        vec![300]
    );
}

#[test]
fn runtime_rejects_a_schedule_that_does_not_match_membership() {
    let members = members();
    let store = Arc::new(Mutex::new(InMemoryRuntimeStore::default()));
    let mut runtime = runtime(
        ReplicaId::new(1),
        recorders(&members),
        Arc::clone(&store),
        Duration::ZERO,
    );

    assert!(matches!(
        runtime.install_epoch_schedule(EpochSchedule::new(0, vec![ReplicaId::new(1)]).unwrap()),
        Err(QuePaxaError::PolicyError(_))
    ));
}

#[test]
fn runtime_rejects_a_proposer_for_a_different_replica() {
    let members = members();
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members.clone(),
        1,
        ReplicaConfig::default(),
        16,
        Duration::ZERO,
    )
    .unwrap();

    assert!(matches!(
        ReplicaRuntime::new(
            config,
            ProposerCore::with_rng(
                ReplicaId::new(2),
                LaneId::new(1),
                FixedPriorities { next: 1 },
            ),
            recorders(&members),
            Arc::new(Mutex::new(InMemoryRuntimeStore::default())),
            RecordingStateMachine::default(),
            RecordingNotifier::default(),
        ),
        Err(QuePaxaError::InvalidProposal(_))
    ));
}

#[test]
fn runtime_enforces_the_configured_fault_budget_and_quorum() {
    assert!(matches!(
        ReplicaRuntimeConfig::new(
            ReplicaId::new(1),
            LaneId::new(1),
            members(),
            2,
            ReplicaConfig::default(),
            16,
            Duration::ZERO,
        ),
        Err(QuePaxaError::InvalidProposal(_))
    ));

    let members = (1..=5).map(ReplicaId::new).collect::<Vec<_>>();
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members,
        1,
        ReplicaConfig::default(),
        16,
        Duration::ZERO,
    )
    .unwrap();
    assert_eq!(config.quorum_size(), 4);
}

#[test]
fn snapshot_restore_requires_a_committed_genesis_slot() {
    let snapshot = ReplicaSnapshot {
        log: BTreeMap::new(),
        pending: Vec::<u64>::new(),
        last_proposed: SlotIndex::GENESIS,
        next_commit: SlotIndex::new(1),
        checkpointed_through: SlotIndex::GENESIS,
        seen_value_ids: BTreeSet::new(),
    };

    assert!(matches!(
        ReplicaCore::restore(ReplicaConfig::default(), snapshot),
        Err(QuePaxaError::InvalidProposal(_))
    ));
}

#[test]
fn recovery_replays_a_persisted_but_unexecuted_decision() {
    let members = members();
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members.clone(),
        1,
        ReplicaConfig::default(),
        16,
        Duration::ZERO,
    )
    .unwrap();
    let decision = Decision::new(
        SlotIndex::new(1),
        vec![40],
        ReplicaId::new(1),
        Step::ROUND_ONE_PHASE_ZERO,
    )
    .unwrap();
    let mut replica = ReplicaCore::new(config.replica);
    replica.apply_decision(decision.clone()).unwrap();
    let store = Arc::new(Mutex::new(InMemoryRuntimeStore {
        snapshot: Some(RuntimeSnapshot {
            cluster: config.cluster_identity().clone(),
            protocol: config.protocol_identity(),
            replica: replica.snapshot(),
            decisions: BTreeMap::from([(decision.slot, decision)]),
            schedules: BTreeMap::from([(0, EpochSchedule::new(0, members.clone()).unwrap())]),
            pending: BTreeMap::new(),
            executed: BTreeSet::new(),
            notified: BTreeSet::new(),
            announced_to: BTreeMap::new(),
            epoch_stats: BTreeMap::new(),
            stats_through: SlotIndex::GENESIS,
        }),
    }));
    let mut runtime = runtime(
        ReplicaId::new(1),
        recorders(&members),
        Arc::clone(&store),
        Duration::ZERO,
    );

    let delivered = runtime.recover().unwrap();
    assert_eq!(delivered.len(), 1);
    assert_eq!(runtime.state_machine().executed[0].value_ids, vec![40]);
    assert_eq!(runtime.notifier().notified[0].value_ids, vec![40]);
}

#[test]
fn runtime_rejects_snapshot_execution_metadata_without_a_decision() {
    let members = members();
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members.clone(),
        1,
        ReplicaConfig::default(),
        16,
        Duration::ZERO,
    )
    .unwrap();
    let store = Arc::new(Mutex::new(InMemoryRuntimeStore {
        snapshot: Some(RuntimeSnapshot {
            cluster: config.cluster_identity().clone(),
            protocol: config.protocol_identity(),
            replica: ReplicaCore::<u64>::new(config.replica).snapshot(),
            decisions: BTreeMap::new(),
            schedules: BTreeMap::new(),
            pending: BTreeMap::new(),
            executed: BTreeSet::from([SlotIndex::new(1)]),
            notified: BTreeSet::new(),
            announced_to: BTreeMap::new(),
            epoch_stats: BTreeMap::new(),
            stats_through: SlotIndex::GENESIS,
        }),
    }));

    assert!(matches!(
        ReplicaRuntime::new(
            config,
            ProposerCore::with_rng(
                ReplicaId::new(1),
                LaneId::new(1),
                FixedPriorities { next: 1 },
            ),
            recorders(&members),
            store,
            RecordingStateMachine::default(),
            RecordingNotifier::default(),
        ),
        Err(QuePaxaError::InvalidProposal(_))
    ));
}

#[test]
fn runtime_rejects_a_snapshot_from_a_different_cluster() {
    let members = members();
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members.clone(),
        1,
        ReplicaConfig::default(),
        16,
        Duration::ZERO,
    )
    .unwrap();
    let store = Arc::new(Mutex::new(InMemoryRuntimeStore {
        snapshot: Some(RuntimeSnapshot {
            cluster: ClusterIdentity::new([ReplicaId::new(1)], 0).unwrap(),
            protocol: config.protocol_identity(),
            replica: ReplicaCore::<u64>::new(config.replica).snapshot(),
            decisions: BTreeMap::new(),
            schedules: BTreeMap::new(),
            pending: BTreeMap::new(),
            executed: BTreeSet::new(),
            notified: BTreeSet::new(),
            announced_to: BTreeMap::new(),
            epoch_stats: BTreeMap::new(),
            stats_through: SlotIndex::GENESIS,
        }),
    }));

    assert!(matches!(
        ReplicaRuntime::new(
            config,
            ProposerCore::with_rng(
                ReplicaId::new(1),
                LaneId::new(1),
                FixedPriorities { next: 1 },
            ),
            recorders(&members),
            store,
            RecordingStateMachine::default(),
            RecordingNotifier::default(),
        ),
        Err(QuePaxaError::ConfigurationMismatch)
    ));
}

#[test]
fn runtime_rejects_a_snapshot_with_different_epoch_configuration() {
    let members = members();
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members.clone(),
        1,
        ReplicaConfig::default(),
        16,
        Duration::ZERO,
    )
    .unwrap();
    let store = Arc::new(Mutex::new(InMemoryRuntimeStore {
        snapshot: Some(RuntimeSnapshot {
            cluster: config.cluster_identity().clone(),
            protocol: ProtocolIdentity {
                epoch_size: 8,
                auto_schedules: false,
            },
            replica: ReplicaCore::<u64>::new(config.replica).snapshot(),
            decisions: BTreeMap::new(),
            schedules: BTreeMap::new(),
            pending: BTreeMap::new(),
            executed: BTreeSet::new(),
            notified: BTreeSet::new(),
            announced_to: BTreeMap::new(),
            epoch_stats: BTreeMap::new(),
            stats_through: SlotIndex::GENESIS,
        }),
    }));

    assert!(matches!(
        ReplicaRuntime::new(
            config,
            ProposerCore::with_rng(
                ReplicaId::new(1),
                LaneId::new(1),
                FixedPriorities { next: 1 },
            ),
            recorders(&members),
            store,
            RecordingStateMachine::default(),
            RecordingNotifier::default(),
        ),
        Err(QuePaxaError::ConfigurationMismatch)
    ));
}
