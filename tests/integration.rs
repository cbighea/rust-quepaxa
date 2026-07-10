use rust_quepaxa::{
    ClusterIdentity, FetchingAvailability, InMemoryValueStore, LaneId, Priority, PrioritySource,
    Proposal, ProposalKey, ProposerConfig, ProposerCore, QuePaxaError, RecordReply, RecordRequest,
    RecordSummary, RecorderClient, RecorderConfig, RecorderCore, RecorderHandle, ReplicaConfig,
    ReplicaCore, ReplicaId, Result, SlotIndex, Step, ValueAvailability, ValueFetcher, ValueStore,
    XorShift64,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

type InMemoryRecorder = RecorderHandle<RecorderCore<u64>>;
type SharedStore = Arc<Mutex<InMemoryValueStore<u64, &'static str>>>;

#[derive(Default)]
struct StaticFetcher {
    values: BTreeMap<u64, &'static str>,
}

impl ValueFetcher<u64, &'static str> for StaticFetcher {
    fn fetch_values(&mut self, value_ids: &[u64]) -> Result<Vec<(u64, &'static str)>> {
        Ok(value_ids
            .iter()
            .filter_map(|value_id| self.values.get(value_id).map(|value| (*value_id, *value)))
            .collect())
    }
}

#[derive(Debug, Clone)]
struct FixedPriorities {
    next: u64,
}

impl PrioritySource for FixedPriorities {
    fn next_below(&mut self, exclusive_upper_bound: Priority) -> Result<Priority> {
        let priority = Priority::new(self.next.clamp(1, exclusive_upper_bound.get() - 1));
        self.next += 1;
        Ok(priority)
    }
}

fn members(count: u64) -> Vec<ReplicaId> {
    (1..=count).map(ReplicaId::new).collect()
}

fn strict_recorders(
    count: u64,
    values: BTreeMap<u64, &'static str>,
) -> (Vec<InMemoryRecorder>, SharedStore) {
    let members = members(count);
    let store = Arc::new(Mutex::new(InMemoryValueStore::new()));
    let availability: Arc<dyn ValueAvailability<u64>> = Arc::new(FetchingAvailability::new(
        Arc::clone(&store),
        StaticFetcher { values },
    ));
    let recorders = members
        .iter()
        .map(|id| {
            RecorderHandle::new(
                *id,
                RecorderCore::new(
                    RecorderConfig::new(*id, members.clone(), (members.len() - 1) / 2).unwrap(),
                    Arc::clone(&availability),
                ),
            )
        })
        .collect();

    (recorders, store)
}

fn permissive_recorders(count: u64) -> Vec<InMemoryRecorder> {
    let members = members(count);
    members
        .iter()
        .map(|id| {
            RecorderHandle::new(
                *id,
                RecorderCore::permissive_for_tests(*id, members.clone()).unwrap(),
            )
        })
        .collect()
}

#[test]
fn strict_cluster_runs_the_full_replica_lifecycle() {
    let (recorders, store) = strict_recorders(
        3,
        BTreeMap::from([(10, "first payload"), (20, "second payload")]),
    );
    let mut replica = ReplicaCore::new(ReplicaConfig {
        batch_size: 1,
        pipeline_len: 2,
        ..ReplicaConfig::default()
    });
    replica.enqueue([10, 20]).unwrap();
    let (first_slot, first_values) = replica.next_proposal().unwrap().unwrap();
    let (second_slot, second_values) = replica.next_proposal().unwrap().unwrap();
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    );

    let first_decision = proposer
        .propose(
            first_slot,
            first_values,
            Some(ReplicaId::new(1)),
            &recorders,
            &[],
        )
        .unwrap();
    for recorder in &recorders {
        recorder
            .with_client(|core| core.inform_decisions(std::slice::from_ref(&first_decision)))
            .unwrap()
            .unwrap();
    }
    let second_decision = proposer
        .propose(
            second_slot,
            second_values,
            Some(ReplicaId::new(1)),
            &recorders,
            std::slice::from_ref(&first_decision),
        )
        .unwrap();

    assert!(replica.apply_decision(second_decision).unwrap().is_empty());
    let committed = replica.apply_decision(first_decision).unwrap();
    assert_eq!(
        committed
            .iter()
            .map(|decision| decision.slot)
            .collect::<Vec<_>>(),
        vec![SlotIndex::new(1), SlotIndex::new(2)]
    );
    assert!(store.lock().unwrap().contains(&10));
    assert!(store.lock().unwrap().contains(&20));
    for recorder in &recorders {
        assert_eq!(
            recorder
                .with_client(|core| core.decisions().count())
                .unwrap(),
            1
        );
    }

    replica.prune_through(second_slot);
    assert!(replica.slot(first_slot).is_none());
    for recorder in &recorders {
        assert_eq!(
            recorder
                .with_client(|core| {
                    core.prune_through(second_slot);
                    core.decisions().count()
                })
                .unwrap(),
            0
        );
    }
}

struct PartitionedRecorder {
    inner: RecorderCore<u64>,
    reachable: bool,
}

struct MismatchedClusterRecorder {
    inner: RecorderCore<u64>,
}

struct SelectiveFailureRecorder {
    inner: RecorderCore<u64>,
    failure: Option<QuePaxaError>,
}

impl RecorderClient<u64> for SelectiveFailureRecorder {
    fn record(&mut self, request: RecordRequest<u64>) -> Result<RecordReply<u64>> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        self.inner.record(request)
    }
}

struct TimeoutRecorder {
    inner: RecorderCore<u64>,
    blocks: bool,
    release: Arc<AtomicBool>,
}

impl RecorderClient<u64> for TimeoutRecorder {
    fn record(&mut self, request: RecordRequest<u64>) -> Result<RecordReply<u64>> {
        while self.blocks && !self.release.load(Ordering::Acquire) {
            thread::yield_now();
        }
        self.inner.record(request)
    }
}

impl RecorderClient<u64> for MismatchedClusterRecorder {
    fn record(&mut self, request: RecordRequest<u64>) -> Result<RecordReply<u64>> {
        let mut reply = self.inner.record(request)?;
        reply.cluster = ClusterIdentity::new([ReplicaId::new(1)], 0).unwrap();
        Ok(reply)
    }
}

impl RecorderClient<u64> for PartitionedRecorder {
    fn record(&mut self, request: RecordRequest<u64>) -> Result<RecordReply<u64>> {
        if self.reachable {
            self.inner.record(request)
        } else {
            Err(QuePaxaError::TransportError("simulated partition".into()))
        }
    }
}

#[test]
fn proposer_reports_when_a_quorum_is_unavailable() {
    let members = members(5);
    let recorders = members
        .iter()
        .enumerate()
        .map(|(index, id)| {
            RecorderHandle::new(
                *id,
                PartitionedRecorder {
                    inner: RecorderCore::permissive_for_tests(*id, members.clone()).unwrap(),
                    reachable: index < 2,
                },
            )
        })
        .collect::<Vec<_>>();
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
        Err(QuePaxaError::QuorumNotReached { needed: 3, .. })
    ));
}

#[test]
fn one_recorder_resource_error_does_not_block_a_healthy_quorum() {
    let members = members(3);
    let recorders = members
        .iter()
        .enumerate()
        .map(|(index, id)| {
            RecorderHandle::new(
                *id,
                SelectiveFailureRecorder {
                    inner: RecorderCore::permissive_for_tests(*id, members.clone()).unwrap(),
                    failure: (index == 0).then_some(QuePaxaError::ResourceLimit {
                        resource: "test recorder",
                        limit: 1,
                    }),
                },
            )
        })
        .collect::<Vec<_>>();
    let decision = ProposerCore::with_rng(
        ReplicaId::new(2),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    )
    .propose(
        SlotIndex::new(1),
        vec![20],
        Some(ReplicaId::new(2)),
        &recorders,
        &[],
    )
    .unwrap();
    assert_eq!(decision.value_ids, vec![20]);
}

#[test]
fn synchronous_quorum_wait_is_transport_bounded() {
    let members = members(3);
    let release = Arc::new(AtomicBool::new(false));
    let recorders = members
        .iter()
        .enumerate()
        .map(|(index, id)| {
            RecorderHandle::new(
                *id,
                TimeoutRecorder {
                    inner: RecorderCore::permissive_for_tests(*id, members.clone()).unwrap(),
                    blocks: index < 2,
                    release: Arc::clone(&release),
                },
            )
        })
        .collect::<Vec<_>>();
    let start = Instant::now();
    let result = ProposerCore::with_rng(
        ReplicaId::new(3),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    )
    .with_rpc_timeout(Duration::from_millis(20))
    .propose(
        SlotIndex::new(1),
        vec![30],
        Some(ReplicaId::new(3)),
        &recorders,
        &[],
    );
    release.store(true, Ordering::Release);
    assert!(matches!(result, Err(QuePaxaError::QuorumNotReached { .. })));
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[test]
fn configured_n_minus_f_quorum_does_not_fall_back_to_a_smaller_majority() {
    let members = members(5);
    let recorders = members
        .iter()
        .enumerate()
        .map(|(index, id)| {
            RecorderHandle::new(
                *id,
                PartitionedRecorder {
                    inner: RecorderCore::permissive_for_tests(*id, members.clone()).unwrap(),
                    reachable: index < 3,
                },
            )
        })
        .collect::<Vec<_>>();
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    )
    .with_quorum(4);

    assert!(matches!(
        proposer.propose(
            SlotIndex::new(1),
            vec![1],
            Some(ReplicaId::new(1)),
            &recorders,
            &[],
        ),
        Err(QuePaxaError::QuorumNotReached { needed: 4, .. })
    ));
}

#[test]
fn proposer_rejects_recorder_replies_from_a_different_cluster() {
    let members = members(3);
    let recorders = members
        .iter()
        .map(|id| {
            RecorderHandle::new(
                *id,
                MismatchedClusterRecorder {
                    inner: RecorderCore::permissive_for_tests(*id, members.clone()).unwrap(),
                },
            )
        })
        .collect::<Vec<_>>();
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    )
    .with_cluster(ClusterIdentity::new(members, 1).unwrap());

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
        QuePaxaError::ConfigurationMismatch
    );
}

#[test]
fn proposer_adopts_a_higher_recorder_step_before_deciding() {
    let (recorders, _) = strict_recorders(3, BTreeMap::from([(2, "new"), (99, "prior")]));
    let prior = Proposal::new(
        ProposalKey::new(Priority::new(7), ReplicaId::new(1), LaneId::new(1)),
        vec![99],
    )
    .unwrap();
    for recorder in &recorders {
        recorder
            .with_client(|core| {
                core.record(RecordRequest {
                    sender: ReplicaId::new(1),
                    slot: SlotIndex::new(1),
                    round_one_leader: Some(ReplicaId::new(1)),
                    step: Step::new(8),
                    proposal: prior.clone(),
                    known_decisions: Vec::new(),
                })
                .unwrap();
            })
            .unwrap();
    }

    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(2),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    );
    let decision = proposer
        .propose(
            SlotIndex::new(1),
            vec![2],
            Some(ReplicaId::new(1)),
            &recorders,
            &[],
        )
        .unwrap();

    assert_eq!(decision.value_ids, vec![99]);
    assert_eq!(decision.decided_step, Step::new(10));
}

struct NeverDecides {
    id: ReplicaId,
}

impl RecorderClient<u64> for NeverDecides {
    fn record(&mut self, request: RecordRequest<u64>) -> Result<RecordReply<u64>> {
        Ok(RecordReply {
            recorder: self.id,
            cluster: ClusterIdentity::new([self.id], 0).unwrap(),
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
fn proposer_stops_at_its_configured_step_limit() {
    let recorders = members(3)
        .into_iter()
        .map(|id| RecorderHandle::new(id, NeverDecides { id }))
        .collect::<Vec<_>>();
    let mut proposer = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    )
    .with_config(ProposerConfig {
        max_steps: 7,
        ..ProposerConfig::default()
    });

    assert_eq!(
        proposer
            .propose(SlotIndex::new(1), vec![1], None, &recorders, &[])
            .unwrap_err(),
        QuePaxaError::StepLimitExceeded { limit: 7 }
    );
}

#[test]
fn seeded_competing_proposers_never_decide_conflicting_values() {
    for seed in 1..=32 {
        let recorders = permissive_recorders(5);
        let members = members(5);
        let leader = members[seed as usize % members.len()];
        let proposer_order = (0..3)
            .map(|offset| members[(seed as usize + offset) % members.len()])
            .collect::<Vec<_>>();
        let mut expected = None;

        for (attempt, replica_id) in proposer_order.into_iter().enumerate() {
            let mut proposer = ProposerCore::with_rng(
                replica_id,
                LaneId::new(1),
                XorShift64::new_for_stream(seed, replica_id, LaneId::new(1)),
            );
            let decision = proposer
                .propose(
                    SlotIndex::new(1),
                    vec![seed * 10 + attempt as u64],
                    Some(leader),
                    &recorders,
                    &[],
                )
                .unwrap();

            if let Some(value_ids) = &expected {
                assert_eq!(&decision.value_ids, value_ids, "seed {seed}");
            } else {
                expected = Some(decision.value_ids);
            }
        }
    }
}

struct DelayedRecorder {
    inner: RecorderCore<u64>,
    delay: Duration,
}

impl RecorderClient<u64> for DelayedRecorder {
    fn record(&mut self, request: RecordRequest<u64>) -> Result<RecordReply<u64>> {
        thread::sleep(self.delay);
        self.inner.record(request)
    }
}

#[test]
fn reordered_recorder_replies_preserve_single_slot_safety() {
    let members = members(5);
    let delays = [4, 0, 3, 1, 2].map(Duration::from_millis);
    let recorders = members
        .iter()
        .zip(delays)
        .map(|(id, delay)| {
            RecorderHandle::new(
                *id,
                DelayedRecorder {
                    inner: RecorderCore::permissive_for_tests(*id, members.clone()).unwrap(),
                    delay,
                },
            )
        })
        .collect::<Vec<_>>();
    let leader = ReplicaId::new(2);

    let first = ProposerCore::with_rng(
        ReplicaId::new(1),
        LaneId::new(1),
        FixedPriorities { next: 1 },
    )
    .propose(SlotIndex::new(1), vec![10], Some(leader), &recorders, &[])
    .unwrap();
    thread::sleep(Duration::from_millis(10));
    let second = ProposerCore::with_rng(
        ReplicaId::new(3),
        LaneId::new(1),
        FixedPriorities { next: 10 },
    )
    .propose(SlotIndex::new(1), vec![20], Some(leader), &recorders, &[])
    .unwrap();

    assert_eq!(first.value_ids, second.value_ids);
}
