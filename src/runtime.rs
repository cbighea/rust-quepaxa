//! Runtime orchestration for a single QuePaxa replica.
//!
//! `ReplicaRuntime` owns the local proposer pipeline, durable runtime state,
//! hedged activation, decision dissemination, state-machine execution, and
//! client notification. Network authentication and the agreement protocol for
//! epoch schedules remain pluggable deployment concerns: callers must install
//! the same agreed [`EpochSchedule`] at every replica.

use crate::crash::{CrashInjector, CrashPoint, NoopCrashInjector};
use crate::error::{QuePaxaError, Result};
use crate::policy::HedgingPolicy;
use crate::proposer::{OsRandom, PrioritySource, ProposerCore, RecorderClient, RecorderHandle};
use crate::replica::{ReplicaConfig, ReplicaCore, ReplicaSnapshot};
use crate::types::{
    ClusterIdentity, Decision, LaneId, MembershipChange, ReplicaId, SlotIndex, Step,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

mod schedule;

pub(crate) use schedule::HedgeDelayTuner;
pub use schedule::{
    AdaptiveHedgingConfig, EpochSchedule, EpochStat, EpochTuner, ProtocolIdentity,
    ReplicaRuntimeConfig,
};

/// Serializable state for an armed, but not yet completed, local proposal.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingProposalSnapshot<V> {
    pub value_ids: Vec<V>,
}

/// All state the runtime must persist atomically with its local replica log.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "network",
    serde(bound(
        serialize = "V: serde::Serialize",
        deserialize = "V: Ord + serde::Deserialize<'de>"
    ))
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSnapshot<V> {
    pub cluster: ClusterIdentity,
    pub protocol: ProtocolIdentity,
    pub replica: ReplicaSnapshot<V>,
    pub decisions: BTreeMap<SlotIndex, Decision<V>>,
    pub schedules: BTreeMap<u64, EpochSchedule>,
    pub pending: BTreeMap<SlotIndex, PendingProposalSnapshot<V>>,
    pub executed: BTreeSet<SlotIndex>,
    pub notified: BTreeSet<SlotIndex>,
    pub announced_to: BTreeMap<SlotIndex, BTreeSet<ReplicaId>>,
    #[cfg_attr(feature = "network", serde(default))]
    pub epoch_stats: BTreeMap<u64, EpochStat>,
    #[cfg_attr(feature = "network", serde(default))]
    pub stats_through: SlotIndex,
}

/// Portable state used to bring a replica that is behind the pruning floor
/// back into the cluster. The application checkpoint and consensus snapshot
/// describe the same committed prefix.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "network",
    serde(bound(
        serialize = "V: serde::Serialize",
        deserialize = "V: Ord + serde::Deserialize<'de>"
    ))
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateTransferSnapshot<V> {
    pub through: SlotIndex,
    /// Unique batch/value IDs covered by the durable checkpoint. Batch stores
    /// consume this list directly instead of trusting callers to reconstruct a
    /// safe pruning set after consensus history has been removed.
    #[cfg_attr(feature = "network", serde(default))]
    pub checkpointed_value_ids: Vec<V>,
    pub application_checkpoint: Vec<u8>,
    pub runtime: RuntimeSnapshot<V>,
}

/// Persists and restores [`RuntimeSnapshot`] atomically.
pub trait RuntimeStateStore<V> {
    fn load(&mut self) -> Result<Option<RuntimeSnapshot<V>>>;
    fn save(&mut self, snapshot: &RuntimeSnapshot<V>) -> Result<()>;
}

/// Codec used by [`FileRuntimeStore`]. Applications select the value encoding.
pub trait RuntimeCodec<V> {
    fn encode(&self, snapshot: &RuntimeSnapshot<V>) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<RuntimeSnapshot<V>>;
}

/// A crash-safe file store when paired with an application-provided codec.
pub struct FileRuntimeStore<V, C> {
    path: PathBuf,
    codec: C,
    marker: PhantomData<fn(V)>,
    crash_injector: Arc<dyn CrashInjector>,
}

impl<V, C> FileRuntimeStore<V, C> {
    pub fn new(path: impl Into<PathBuf>, codec: C) -> Self {
        Self {
            path: path.into(),
            codec,
            marker: PhantomData,
            crash_injector: Arc::new(NoopCrashInjector),
        }
    }

    pub fn with_crash_injector<I>(mut self, injector: I) -> Self
    where
        I: CrashInjector,
    {
        self.crash_injector = Arc::new(injector);
        self
    }
}

impl<V, C: RuntimeCodec<V>> RuntimeStateStore<V> for FileRuntimeStore<V, C> {
    fn load(&mut self) -> Result<Option<RuntimeSnapshot<V>>> {
        match fs::read(&self.path) {
            Ok(bytes) => self.codec.decode(&bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(QuePaxaError::StorageError(format!(
                "could not read runtime state {}: {error}",
                self.path.display()
            ))),
        }
    }

    fn save(&mut self, snapshot: &RuntimeSnapshot<V>) -> Result<()> {
        let bytes = self.codec.encode(snapshot)?;
        let temporary = self.path.with_extension("runtime.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                QuePaxaError::StorageError(format!(
                    "could not open runtime state {}: {error}",
                    temporary.display()
                ))
            })?;
        self.crash_injector
            .reached(CrashPoint::RuntimeTemporaryOpened)?;
        file.write_all(&bytes).map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not write runtime state {}: {error}",
                temporary.display()
            ))
        })?;
        self.crash_injector
            .reached(CrashPoint::RuntimeTemporaryWritten)?;
        file.sync_all().map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not sync runtime state {}: {error}",
                temporary.display()
            ))
        })?;
        self.crash_injector
            .reached(CrashPoint::RuntimeTemporarySynced)?;
        fs::rename(&temporary, &self.path).map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not replace runtime state {}: {error}",
                self.path.display()
            ))
        })?;
        self.crash_injector.reached(CrashPoint::RuntimeRenamed)?;
        sync_parent(&self.path)?;
        self.crash_injector
            .reached(CrashPoint::RuntimeDirectorySynced)
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not sync runtime state directory {}: {error}",
                parent.display()
            ))
        })
}

/// Test and single-process store for runtime state.
#[derive(Debug, Default)]
pub struct InMemoryRuntimeStore<V> {
    snapshot: Option<RuntimeSnapshot<V>>,
}

impl<V> InMemoryRuntimeStore<V> {
    pub fn snapshot(&self) -> Option<&RuntimeSnapshot<V>> {
        self.snapshot.as_ref()
    }
}

impl<V: Clone> RuntimeStateStore<V> for InMemoryRuntimeStore<V> {
    fn load(&mut self) -> Result<Option<RuntimeSnapshot<V>>> {
        Ok(self.snapshot.clone())
    }

    fn save(&mut self, snapshot: &RuntimeSnapshot<V>) -> Result<()> {
        self.snapshot = Some(snapshot.clone());
        Ok(())
    }
}

impl<V, S> RuntimeStateStore<V> for Arc<Mutex<S>>
where
    S: RuntimeStateStore<V>,
{
    fn load(&mut self) -> Result<Option<RuntimeSnapshot<V>>> {
        self.lock()
            .map_err(|_| {
                QuePaxaError::StorageError("runtime state store lock was poisoned".into())
            })?
            .load()
    }

    fn save(&mut self, snapshot: &RuntimeSnapshot<V>) -> Result<()> {
        self.lock()
            .map_err(|_| {
                QuePaxaError::StorageError("runtime state store lock was poisoned".into())
            })?
            .save(snapshot)
    }
}

/// Applies committed decisions to the embedding application.
///
/// Implementations must be idempotent by `Decision::slot`, because storage and
/// application state cannot be committed atomically by this generic library.
/// Deployments accepting retries through multiple replicas must also dedupe
/// globally unique value IDs: consensus may legitimately place independently
/// submitted copies of the same client command in different slots.
pub trait StateMachine<V> {
    fn execute(&mut self, decision: &Decision<V>) -> Result<()>;

    /// Creates a durable application checkpoint covering `through`. The
    /// returned bytes are carried with a consensus state-transfer snapshot.
    fn export_checkpoint(&mut self, _through: SlotIndex) -> Result<Vec<u8>> {
        Err(QuePaxaError::StorageError(
            "the state machine does not implement checkpoint export".into(),
        ))
    }

    /// Installs a checkpoint received through state transfer. Implementations
    /// must be durable and idempotent because application and generic runtime
    /// storage cannot share a transaction in this library.
    fn import_checkpoint(&mut self, _through: SlotIndex, _checkpoint: &[u8]) -> Result<()> {
        Err(QuePaxaError::StorageError(
            "the state machine does not implement checkpoint import".into(),
        ))
    }
}

/// Reports committed decisions to clients or an application completion queue.
/// Implementations should also tolerate duplicate attempts after restart.
pub trait ClientNotifier<V> {
    fn committed(&mut self, decision: &Decision<V>) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct NoopStateMachine;

impl<V> StateMachine<V> for NoopStateMachine {
    fn execute(&mut self, _decision: &Decision<V>) -> Result<()> {
        Ok(())
    }

    fn export_checkpoint(&mut self, _through: SlotIndex) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn import_checkpoint(&mut self, _through: SlotIndex, checkpoint: &[u8]) -> Result<()> {
        if checkpoint.is_empty() {
            Ok(())
        } else {
            Err(QuePaxaError::StorageError(
                "the no-op state machine accepts only an empty checkpoint".into(),
            ))
        }
    }
}

#[derive(Debug, Default)]
pub struct NoopClientNotifier;

impl<V> ClientNotifier<V> for NoopClientNotifier {
    fn committed(&mut self, _decision: &Decision<V>) -> Result<()> {
        Ok(())
    }
}

/// The next operation requested by the runtime event loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimePoll<V> {
    Idle,
    Waiting { slot: SlotIndex, until: Instant },
    Committed(Vec<Decision<V>>),
}

struct PendingProposal<V> {
    value_ids: Vec<V>,
    ready_at: Option<Instant>,
    /// Highest slot step observed while hedging. Progress since the last
    /// observation is evidence another proposer is driving the slot.
    last_observed_step: Option<Step>,
}

/// A complete local runtime for one QuePaxa proposer role.
pub struct ReplicaRuntime<
    V,
    C,
    R = OsRandom,
    S = InMemoryRuntimeStore<V>,
    E = NoopStateMachine,
    N = NoopClientNotifier,
> {
    config: ReplicaRuntimeConfig,
    replica: ReplicaCore<V>,
    proposer: ProposerCore<R>,
    recorders: Vec<RecorderHandle<C>>,
    state_store: S,
    state_machine: E,
    notifier: N,
    decisions: BTreeMap<SlotIndex, Decision<V>>,
    tuner: EpochTuner,
    pending: BTreeMap<SlotIndex, PendingProposal<V>>,
    executed: BTreeSet<SlotIndex>,
    notified: BTreeSet<SlotIndex>,
    announced_to: BTreeMap<SlotIndex, BTreeSet<ReplicaId>>,
    noop_value: Option<V>,
    hedge_delay: HedgeDelayTuner,
}

impl<V, C, R, S, E, N> ReplicaRuntime<V, C, R, S, E, N>
where
    V: Clone + Ord + Send + 'static,
    C: RecorderClient<V> + 'static,
    R: PrioritySource,
    S: RuntimeStateStore<V>,
    E: StateMachine<V>,
    N: ClientNotifier<V>,
{
    pub fn new(
        config: ReplicaRuntimeConfig,
        mut proposer: ProposerCore<R>,
        recorders: Vec<RecorderHandle<C>>,
        mut state_store: S,
        state_machine: E,
        notifier: N,
    ) -> Result<Self> {
        validate_recorders(&config, &recorders)?;
        if proposer.replica_id() != config.replica_id || proposer.lane_id() != config.lane_id {
            return Err(QuePaxaError::InvalidProposal(
                "runtime proposer identity does not match the runtime configuration".into(),
            ));
        }
        proposer.configure_cluster(config.cluster.clone());
        let snapshot = state_store.load()?;
        let (replica, decisions, tuner, pending, executed, notified, announced_to) =
            if let Some(snapshot) = snapshot {
                if snapshot.cluster != config.cluster
                    || snapshot.protocol != config.protocol_identity()
                {
                    return Err(QuePaxaError::ConfigurationMismatch);
                }
                let pending = snapshot
                    .pending
                    .into_iter()
                    .map(|(slot, pending)| {
                        (
                            slot,
                            PendingProposal {
                                value_ids: pending.value_ids,
                                ready_at: None,
                                last_observed_step: None,
                            },
                        )
                    })
                    .collect();
                (
                    ReplicaCore::restore(config.replica, snapshot.replica)?,
                    snapshot.decisions,
                    EpochTuner::restore(
                        config.epoch_size,
                        config.members().to_vec(),
                        config.auto_schedules,
                        snapshot.schedules,
                        snapshot.epoch_stats,
                        snapshot.stats_through,
                    )?,
                    pending,
                    snapshot.executed,
                    snapshot.notified,
                    snapshot.announced_to,
                )
            } else {
                (
                    ReplicaCore::new(config.replica),
                    BTreeMap::new(),
                    EpochTuner::new(
                        config.epoch_size,
                        config.members().to_vec(),
                        config.auto_schedules,
                    ),
                    BTreeMap::new(),
                    BTreeSet::new(),
                    BTreeSet::new(),
                    BTreeMap::new(),
                )
            };
        let hedge_delay =
            HedgeDelayTuner::new(config.base_hedge_delay, config.adaptive_hedging().cloned());
        let runtime = Self {
            config,
            replica,
            proposer,
            recorders,
            state_store,
            state_machine,
            notifier,
            decisions,
            tuner,
            pending,
            executed,
            notified,
            announced_to,
            noop_value: None,
            hedge_delay,
        };
        runtime.validate_restored_state()?;
        Ok(runtime)
    }

    /// Configures a designated no-op value ID used to fill a gap slot when the
    /// commit frontier is blocked and no client values are pending. The
    /// application's state machine must treat this value as a no-op. Without
    /// it, an idle cluster can stall behind a slot whose only proposer died.
    pub fn with_noop_value(mut self, value: V) -> Self {
        self.noop_value = Some(value);
        self
    }

    pub fn config(&self) -> &ReplicaRuntimeConfig {
        &self.config
    }

    pub fn current_hedge_delay(&self) -> Duration {
        self.hedge_delay.current()
    }

    pub fn replica(&self) -> &ReplicaCore<V> {
        &self.replica
    }

    pub fn state_store(&self) -> &S {
        &self.state_store
    }

    pub fn state_machine(&self) -> &E {
        &self.state_machine
    }

    pub fn notifier(&self) -> &N {
        &self.notifier
    }

    /// Enqueues values submitted by a client or front-end. The caller must
    /// ensure the value IDs have been disseminated to the configured replicas.
    pub fn submit<I>(&mut self, value_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = V>,
    {
        self.replica.enqueue(value_ids)?;
        self.persist()
    }

    /// Installs an epoch schedule received from the deployment's agreed
    /// control plane. A schedule cannot be changed once installed.
    ///
    /// SAFETY: every replica must install the identical schedule for an epoch.
    /// Two replicas that disagree on the round-one leader can decide
    /// conflicting values for one slot. Prefer
    /// [`ReplicaRuntimeConfig::with_auto_schedules`], which derives schedules
    /// deterministically from the committed log and needs no external
    /// agreement.
    pub fn install_epoch_schedule(&mut self, schedule: EpochSchedule) -> Result<()> {
        self.validate_schedule(&schedule)?;
        if self.tuner.install(schedule)? {
            self.persist()?;
        }
        Ok(())
    }

    /// Applies the next consensus-anchored stable/joint membership epoch.
    ///
    /// The anchor must be the current committed frontier and every later slot
    /// must be drained. Every recorder in `next_recorders` acknowledges the
    /// transition durably before this runtime persists and activates it. A
    /// joining recorder may already have the target epoch after state transfer;
    /// installation is idempotent in that case.
    pub fn install_membership(
        &mut self,
        change: MembershipChange<V>,
        next_recorders: Vec<RecorderHandle<C>>,
    ) -> Result<()> {
        change.validate_binding()?;
        if change.next == *self.config.cluster_identity() {
            validate_recorders_for_cluster(&change.next, &next_recorders)?;
            self.recorders = next_recorders;
            return Ok(());
        }
        if !change.next.is_successor_of(self.config.cluster_identity()) {
            return Err(QuePaxaError::InvalidReconfiguration(
                "membership transition does not follow the runtime configuration epoch".into(),
            ));
        }
        if self.replica.committed_through() != change.anchor.slot
            || self.decisions.get(&change.anchor.slot) != Some(&change.anchor)
            || !self.pending.is_empty()
            || self
                .replica
                .snapshot()
                .log
                .keys()
                .any(|slot| *slot > change.anchor.slot)
        {
            return Err(QuePaxaError::InvalidReconfiguration(
                "membership changes require an exact committed anchor and a drained pipeline"
                    .into(),
            ));
        }
        self.flush_decision_notifications()?;
        self.ensure_fully_announced(change.anchor.slot)?;
        validate_recorders_for_cluster(&change.next, &next_recorders)?;

        for recorder in &next_recorders {
            let request = change.clone();
            recorder.call_with_timeout(self.config.transport_timeout, move |client| {
                client.install_membership(request)
            })?;
        }

        let old_config = self.config.clone();
        let old_recorders = self.recorders.clone();
        let old_tuner = self.tuner.clone();
        let old_announced_to = self.announced_to.clone();
        let active = change
            .next
            .members()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        for recipients in self.announced_to.values_mut() {
            recipients.retain(|replica| active.contains(replica));
        }
        self.config.install_cluster(change.next.clone());
        self.proposer.configure_cluster(change.next.clone());
        self.recorders = next_recorders;
        self.tuner
            .reconfigure(change.next.members().to_vec(), change.anchor.slot);
        if let Err(error) = self.persist() {
            self.config = old_config;
            self.proposer
                .configure_cluster(self.config.cluster_identity().clone());
            self.recorders = old_recorders;
            self.tuner = old_tuner;
            self.announced_to = old_announced_to;
            return Err(error);
        }
        Ok(())
    }

    /// Builds and installs a deterministic schedule from a local policy. Use
    /// this only after the resulting sequence has been agreed by every replica.
    pub fn install_policy_epoch<P: HedgingPolicy>(
        &mut self,
        epoch: u64,
        policy: &P,
        previous_winner: Option<ReplicaId>,
    ) -> Result<EpochSchedule> {
        let schedule = EpochSchedule::from_policy(&self.config, epoch, policy, previous_winner)?;
        self.install_epoch_schedule(schedule.clone())?;
        Ok(schedule)
    }

    /// Arms all locally available pipeline slots and assigns each the local
    /// hedging deadline from its agreed epoch schedule.
    pub fn arm_available(&mut self, now: Instant) -> Result<usize> {
        let mut changed = false;
        while let Some((slot, value_ids)) = self.replica.next_proposal()? {
            self.pending.insert(
                slot,
                PendingProposal {
                    value_ids,
                    ready_at: None,
                    last_observed_step: None,
                },
            );
            changed = true;
        }
        changed |= self.arm_gap_noop()?;
        if changed {
            self.persist()?;
        }
        self.assign_deadlines(now)?;
        Ok(self.pending.len())
    }

    /// Arms a no-op proposal for the slot blocking the commit frontier when
    /// later decisions exist but no client values are pending to re-drive it.
    /// The no-op waits its ordinary hedging delay, so under normal conditions
    /// the gap's original proposer or an earlier scheduled replica wins first.
    fn arm_gap_noop(&mut self) -> Result<bool> {
        let Some(noop) = self.noop_value.clone() else {
            return Ok(false);
        };
        if !self.pending.is_empty() {
            return Ok(false);
        }
        let gap = self
            .replica
            .committed_through()
            .checked_next()
            .ok_or(QuePaxaError::SlotOverflow)?;
        let later_decision_exists = self
            .decisions
            .range((std::ops::Bound::Excluded(gap), std::ops::Bound::Unbounded))
            .next()
            .is_some();
        let gap_is_open = !self.decisions.contains_key(&gap)
            && self
                .replica
                .slot(gap)
                .is_none_or(|entry| entry.decision.is_none() && entry.proposed.is_empty());
        if !later_decision_exists || !gap_is_open {
            return Ok(false);
        }
        self.replica.note_proposed(gap, vec![noop.clone()])?;
        self.pending.insert(
            gap,
            PendingProposal {
                value_ids: vec![noop],
                ready_at: None,
                last_observed_step: None,
            },
        );
        Ok(true)
    }

    /// Runs one ready local proposal. Event-loop integrations can instead use
    /// [`poll`](Self::poll) to wait without blocking their executor.
    pub fn run_once(&mut self) -> Result<RuntimePoll<V>> {
        match self.poll(Instant::now())? {
            RuntimePoll::Waiting { until, .. } => {
                let delay = until.saturating_duration_since(Instant::now());
                if !delay.is_zero() {
                    thread::sleep(delay);
                }
                self.poll(Instant::now())
            }
            outcome => Ok(outcome),
        }
    }

    /// Polls the runtime without sleeping. `Waiting` reports the next local
    /// hedging deadline; an incoming decision may be supplied before then via
    /// [`receive_decision`](Self::receive_decision) to suppress this proposal.
    pub fn poll(&mut self, now: Instant) -> Result<RuntimePoll<V>> {
        // Decision dissemination is deliberately separate from consensus
        // requests. Keeping it here makes retries automatic without attaching
        // the entire retained decision log to every phase-zero request.
        self.flush_decision_notifications()?;
        self.arm_available(now)?;
        loop {
            let Some((slot, deadline)) = self.next_deadline() else {
                return Ok(RuntimePoll::Idle);
            };
            if deadline > now {
                return Ok(RuntimePoll::Waiting {
                    slot,
                    until: deadline,
                });
            }

            let schedule = self.ensure_schedule_for(slot)?;
            // Enlightened procrastination: a hedged proposer that observes the
            // slot's step advancing defers for another hedging interval rather
            // than duplicating a live proposer's work.
            if self.defer_for_progress(slot, &schedule, now) {
                continue;
            }

            let value_ids = self
                .pending
                .get(&slot)
                .expect("a pending deadline belongs to a pending proposal")
                .value_ids
                .clone();
            let leader = schedule.leader();
            let started = Instant::now();
            let decision =
                self.proposer
                    .propose(slot, value_ids, Some(leader), &self.recorders, &[])?;
            self.hedge_delay.observe(started.elapsed());
            let committed = self.accept_decision(decision)?;
            return Ok(RuntimePoll::Committed(committed));
        }
    }

    /// Returns true when the slot's proposal was deferred because another
    /// proposer is observably driving the slot forward.
    fn defer_for_progress(
        &mut self,
        slot: SlotIndex,
        schedule: &EpochSchedule,
        now: Instant,
    ) -> bool {
        let hedge_delay = self.hedge_delay.current();
        if hedge_delay.is_zero() {
            return false;
        }
        let is_scheduled_first = schedule.proposers.first() == Some(&self.config.replica_id);
        if is_scheduled_first {
            return false;
        }
        let observed = self.probe_slot_step(slot);
        let Some(pending) = self.pending.get_mut(&slot) else {
            return false;
        };
        let progressed = match (observed, pending.last_observed_step) {
            (Some(step), None) => step >= Step::ROUND_ONE_PHASE_ZERO,
            (Some(step), Some(previous)) => step > previous,
            (None, _) => false,
        };
        if progressed && let Some(deadline) = now.checked_add(hedge_delay) {
            pending.last_observed_step = observed;
            pending.ready_at = Some(deadline);
            return true;
        }
        false
    }

    /// Best-effort, read-only probe of the slot's highest step across idle
    /// recorder endpoints. Errors and busy endpoints are simply skipped.
    fn probe_slot_step(&self, slot: SlotIndex) -> Option<Step> {
        let mut highest = None;
        for recorder in &self.recorders {
            if !recorder.is_idle() {
                continue;
            }
            if let Ok(Some(step)) = recorder
                .call_with_timeout(self.config.transport_timeout, move |client| {
                    client.status(slot)
                })
            {
                highest = Some(highest.map_or(step, |current: Step| current.max(step)));
            }
        }
        highest
    }

    /// Accepts a decision received through the authenticated replica transport.
    /// The runtime persists it, executes all newly contiguous decisions, and
    /// retries best-effort dissemination to recorder endpoints.
    pub fn receive_decision(&mut self, decision: Decision<V>) -> Result<Vec<Decision<V>>> {
        self.accept_decision(decision)
    }

    /// Replays committed-but-undelivered work after restoring durable state.
    pub fn recover(&mut self) -> Result<Vec<Decision<V>>> {
        let delivered = self.dispatch_committed()?;
        self.flush_decision_notifications()?;
        Ok(delivered)
    }

    /// Drops local protocol state only after the application state machine has
    /// durably checkpointed every slot through `slot`.
    pub fn checkpoint_through(&mut self, slot: SlotIndex) -> Result<()> {
        self.flush_decision_notifications()?;
        if slot > self.replica.committed_through() {
            return Err(QuePaxaError::InvalidProposal(
                "cannot checkpoint beyond the contiguous committed prefix".into(),
            ));
        }
        if self.pending.range(..=slot).next().is_some() {
            return Err(QuePaxaError::InvalidProposal(
                "cannot checkpoint a slot with a pending local proposal".into(),
            ));
        }
        self.ensure_fully_announced(slot)?;

        self.replica.prune_through(slot);
        self.decisions.retain(|index, _| *index > slot);
        self.executed.retain(|index| *index > slot);
        self.notified.retain(|index| *index > slot);
        self.announced_to.retain(|index, _| *index > slot);
        self.persist()
    }

    /// Creates an application checkpoint and matching consensus snapshot that
    /// can be installed by a replica which has fallen behind the pruning floor.
    /// All decisions through `slot` must first be acknowledged by every member.
    pub fn create_state_transfer(&mut self, slot: SlotIndex) -> Result<StateTransferSnapshot<V>> {
        self.flush_decision_notifications()?;
        self.ensure_fully_announced(slot)?;
        let checkpointed_value_ids = self
            .decisions
            .range(..=slot)
            .flat_map(|(_, decision)| decision.value_ids.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let application_checkpoint = self.state_machine.export_checkpoint(slot)?;
        self.checkpoint_through(slot)?;
        Ok(StateTransferSnapshot {
            through: slot,
            checkpointed_value_ids,
            application_checkpoint,
            runtime: self.runtime_snapshot(),
        })
    }

    /// Installs a portable application/consensus checkpoint before the runtime
    /// begins serving traffic. Application checkpoint import must be durable
    /// and idempotent so callers can retry after a process crash.
    pub fn install_state_transfer(&mut self, transfer: StateTransferSnapshot<V>) -> Result<()> {
        if transfer.through != transfer.runtime.replica.checkpointed_through {
            return Err(QuePaxaError::InvalidProposal(
                "state-transfer checkpoint does not match the runtime pruning floor".into(),
            ));
        }
        let restored = restore_runtime_snapshot(&self.config, &transfer.runtime)?;
        self.state_machine
            .import_checkpoint(transfer.through, &transfer.application_checkpoint)?;
        self.state_store.save(&transfer.runtime)?;
        self.replica = restored.replica;
        self.decisions = restored.decisions;
        self.tuner = restored.tuner;
        self.pending = restored.pending;
        self.executed = restored.executed;
        self.notified = restored.notified;
        self.announced_to = restored.announced_to;
        Ok(())
    }

    pub fn decision(&self, slot: SlotIndex) -> Option<&Decision<V>> {
        self.decisions.get(&slot)
    }

    fn accept_decision(&mut self, decision: Decision<V>) -> Result<Vec<Decision<V>>> {
        self.validate_decision(&decision)?;
        if decision.slot <= self.replica.checkpointed_through() {
            return Ok(Vec::new());
        }
        if let Some(existing) = self.decisions.get(&decision.slot) {
            if existing.value_ids != decision.value_ids {
                return Err(QuePaxaError::ConflictingDecision {
                    slot: decision.slot,
                });
            }
        } else {
            self.replica.apply_decision(decision.clone())?;
            self.pending.remove(&decision.slot);
            self.announced_to.entry(decision.slot).or_default();
            self.decisions.insert(decision.slot, decision);
            self.persist()?;
        }

        let delivered = self.dispatch_committed()?;
        self.flush_decision_notifications()?;
        Ok(delivered)
    }

    fn dispatch_committed(&mut self) -> Result<Vec<Decision<V>>> {
        let committed_through = self.replica.committed_through();
        let decisions = self
            .decisions
            .range(..=committed_through)
            .map(|(_, decision)| decision.clone())
            .collect::<Vec<_>>();
        let mut delivered = Vec::new();

        // Feed committed outcomes into the leader-tuning statistics before
        // execution so every replica derives future schedules from the same
        // committed prefix.
        let mut stats_changed = false;
        for decision in &decisions {
            stats_changed |= self
                .tuner
                .note_committed(decision.slot, decision.decided_step)?;
        }
        if stats_changed {
            self.persist()?;
        }

        for decision in decisions {
            if !self.executed.contains(&decision.slot) {
                self.state_machine.execute(&decision)?;
                self.executed.insert(decision.slot);
                self.persist()?;
            }
            if !self.notified.contains(&decision.slot) {
                self.notifier.committed(&decision)?;
                self.notified.insert(decision.slot);
                self.persist()?;
                delivered.push(decision);
            }
        }
        Ok(delivered)
    }

    fn flush_decision_notifications(&mut self) -> Result<()> {
        let announcements = self
            .decisions
            .iter()
            .map(|(slot, decision)| (*slot, decision.clone()))
            .collect::<Vec<_>>();
        let mut changed = false;

        for (slot, decision) in announcements {
            for recorder in &self.recorders {
                if self
                    .announced_to
                    .get(&slot)
                    .is_some_and(|announced| announced.contains(&recorder.id()))
                {
                    continue;
                }
                let announcement = decision.clone();
                if recorder
                    .call_with_timeout(self.config.transport_timeout, move |client| {
                        client.inform_decisions(std::slice::from_ref(&announcement))
                    })
                    .is_ok()
                {
                    self.announced_to
                        .entry(slot)
                        .or_default()
                        .insert(recorder.id());
                    changed = true;
                }
            }
        }
        if changed {
            self.persist()?;
        }
        Ok(())
    }

    fn ensure_fully_announced(&self, through: SlotIndex) -> Result<()> {
        let members = self
            .config
            .members()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if self.decisions.range(..=through).any(|(slot, _)| {
            self.announced_to
                .get(slot)
                .is_none_or(|recipients| recipients != &members)
        }) {
            return Err(QuePaxaError::QuorumNotReached {
                needed: members.len(),
                received: self
                    .decisions
                    .range(..=through)
                    .filter_map(|(slot, _)| self.announced_to.get(slot).map(BTreeSet::len))
                    .min()
                    .unwrap_or(0),
            });
        }
        Ok(())
    }

    fn assign_deadlines(&mut self, now: Instant) -> Result<()> {
        let unarmed = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.ready_at.is_none())
            .map(|(slot, _)| *slot)
            .collect::<Vec<_>>();
        let mut deadlines = Vec::with_capacity(unarmed.len());
        for slot in unarmed {
            let schedule = self.ensure_schedule_for(slot)?;
            deadlines.push((slot, self.deadline_for(&schedule, now)?));
        }
        for (slot, deadline) in deadlines {
            self.pending
                .get_mut(&slot)
                .expect("an unarmed proposal remains pending")
                .ready_at = Some(deadline);
        }
        Ok(())
    }

    fn deadline_for(&self, schedule: &EpochSchedule, now: Instant) -> Result<Instant> {
        let position = schedule
            .proposers
            .iter()
            .position(|replica| *replica == self.config.replica_id)
            .expect("validated schedules include the local replica");
        let position = u32::try_from(position).map_err(|_| {
            QuePaxaError::PolicyError("hedging schedule is too large for a duration".into())
        })?;
        let delay = self
            .hedge_delay
            .current()
            .checked_mul(position)
            .ok_or_else(|| QuePaxaError::PolicyError("hedging delay overflowed".into()))?;
        now.checked_add(delay)
            .ok_or_else(|| QuePaxaError::PolicyError("hedging deadline overflowed".into()))
    }

    fn next_deadline(&self) -> Option<(SlotIndex, Instant)> {
        self.pending
            .iter()
            .filter_map(|(slot, pending)| pending.ready_at.map(|deadline| (*slot, deadline)))
            .min_by_key(|(_, deadline)| *deadline)
    }

    fn ensure_schedule_for(&mut self, slot: SlotIndex) -> Result<EpochSchedule> {
        let epoch = self.config.epoch_for(slot);
        self.tuner.ensure_schedule(epoch)
    }

    fn validate_restored_state(&self) -> Result<()> {
        for schedule in self.tuner.schedules().values() {
            self.validate_schedule(schedule)?;
        }
        for decision in self.decisions.values() {
            self.validate_decision(decision)?;
        }
        for slot in self.pending.keys() {
            if self.decisions.contains_key(slot) {
                return Err(QuePaxaError::InvalidProposal(
                    "runtime snapshot has both a decision and a pending proposal for one slot"
                        .into(),
                ));
            }
            if *slot <= self.replica.committed_through() {
                return Err(QuePaxaError::InvalidProposal(
                    "runtime snapshot has a pending proposal in the committed prefix".into(),
                ));
            }
        }
        let decision_slots = self.decisions.keys().copied().collect::<BTreeSet<_>>();
        if !self.executed.is_subset(&decision_slots)
            || !self.notified.is_subset(&decision_slots)
            || !self.notified.is_subset(&self.executed)
        {
            return Err(QuePaxaError::InvalidProposal(
                "runtime snapshot has execution or notification metadata without a decision".into(),
            ));
        }
        let members = self
            .config
            .members()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        for (slot, recipients) in &self.announced_to {
            if !decision_slots.contains(slot) || !recipients.is_subset(&members) {
                return Err(QuePaxaError::InvalidProposal(
                    "runtime snapshot has invalid decision dissemination metadata".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_schedule(&self, schedule: &EpochSchedule) -> Result<()> {
        let configured = self
            .config
            .members()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let scheduled = schedule.proposers.iter().copied().collect::<BTreeSet<_>>();
        if scheduled.len() != schedule.proposers.len() {
            return Err(QuePaxaError::PolicyError(
                "an epoch schedule contains duplicate proposers".into(),
            ));
        }
        if scheduled != configured {
            return Err(QuePaxaError::PolicyError(
                "an epoch schedule must contain every configured replica exactly once".into(),
            ));
        }
        Ok(())
    }

    fn validate_decision(&self, decision: &Decision<V>) -> Result<()> {
        if decision.value_ids.is_empty() {
            return Err(QuePaxaError::EmptyDecision);
        }
        if !self.config.cluster.contains(decision.proposer) {
            return Err(QuePaxaError::InvalidSender(decision.proposer));
        }
        if decision
            .value_ids
            .iter()
            .enumerate()
            .any(|(index, value)| decision.value_ids[..index].contains(value))
        {
            return Err(QuePaxaError::InvalidProposal(
                "decision contains duplicate value IDs".into(),
            ));
        }
        Ok(())
    }

    fn runtime_snapshot(&self) -> RuntimeSnapshot<V> {
        let pending = self
            .pending
            .iter()
            .map(|(slot, pending)| {
                (
                    *slot,
                    PendingProposalSnapshot {
                        value_ids: pending.value_ids.clone(),
                    },
                )
            })
            .collect();
        RuntimeSnapshot {
            cluster: self.config.cluster.clone(),
            protocol: self.config.protocol_identity(),
            replica: self.replica.snapshot(),
            decisions: self.decisions.clone(),
            schedules: self.tuner.schedules().clone(),
            pending,
            executed: self.executed.clone(),
            notified: self.notified.clone(),
            announced_to: self.announced_to.clone(),
            epoch_stats: self.tuner.stats().clone(),
            stats_through: self.tuner.stats_through(),
        }
    }

    fn persist(&mut self) -> Result<()> {
        self.state_store.save(&self.runtime_snapshot())
    }
}

struct RestoredRuntimeState<V> {
    replica: ReplicaCore<V>,
    decisions: BTreeMap<SlotIndex, Decision<V>>,
    tuner: EpochTuner,
    pending: BTreeMap<SlotIndex, PendingProposal<V>>,
    executed: BTreeSet<SlotIndex>,
    notified: BTreeSet<SlotIndex>,
    announced_to: BTreeMap<SlotIndex, BTreeSet<ReplicaId>>,
}

fn restore_runtime_snapshot<V>(
    config: &ReplicaRuntimeConfig,
    snapshot: &RuntimeSnapshot<V>,
) -> Result<RestoredRuntimeState<V>>
where
    V: Clone + Ord,
{
    if snapshot.cluster != *config.cluster_identity()
        || snapshot.protocol != config.protocol_identity()
    {
        return Err(QuePaxaError::ConfigurationMismatch);
    }
    let replica = ReplicaCore::restore(config.replica, snapshot.replica.clone())?;
    let tuner = EpochTuner::restore(
        config.epoch_size,
        config.members().to_vec(),
        config.auto_schedules(),
        snapshot.schedules.clone(),
        snapshot.epoch_stats.clone(),
        snapshot.stats_through,
    )?;
    let pending = snapshot
        .pending
        .iter()
        .map(|(slot, proposal)| {
            (
                *slot,
                PendingProposal {
                    value_ids: proposal.value_ids.clone(),
                    ready_at: None,
                    last_observed_step: None,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    validate_runtime_components(
        config,
        &replica,
        &snapshot.decisions,
        &tuner,
        &pending,
        &snapshot.executed,
        &snapshot.notified,
        &snapshot.announced_to,
    )?;
    Ok(RestoredRuntimeState {
        replica,
        decisions: snapshot.decisions.clone(),
        tuner,
        pending,
        executed: snapshot.executed.clone(),
        notified: snapshot.notified.clone(),
        announced_to: snapshot.announced_to.clone(),
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_runtime_components<V>(
    config: &ReplicaRuntimeConfig,
    replica: &ReplicaCore<V>,
    decisions: &BTreeMap<SlotIndex, Decision<V>>,
    tuner: &EpochTuner,
    pending: &BTreeMap<SlotIndex, PendingProposal<V>>,
    executed: &BTreeSet<SlotIndex>,
    notified: &BTreeSet<SlotIndex>,
    announced_to: &BTreeMap<SlotIndex, BTreeSet<ReplicaId>>,
) -> Result<()>
where
    V: Clone + Ord,
{
    let members = config.members().iter().copied().collect::<BTreeSet<_>>();
    for schedule in tuner.schedules().values() {
        let scheduled = schedule.proposers.iter().copied().collect::<BTreeSet<_>>();
        if scheduled.len() != schedule.proposers.len() || scheduled != members {
            return Err(QuePaxaError::PolicyError(
                "an epoch schedule must contain every configured replica exactly once".into(),
            ));
        }
    }
    for (slot, decision) in decisions {
        if decision.value_ids.is_empty() || !config.cluster.contains(decision.proposer) {
            return Err(QuePaxaError::InvalidProposal(
                "runtime snapshot contains an invalid decision".into(),
            ));
        }
        if decision
            .value_ids
            .iter()
            .enumerate()
            .any(|(index, value)| decision.value_ids[..index].contains(value))
        {
            return Err(QuePaxaError::InvalidProposal(
                "decision contains duplicate value IDs".into(),
            ));
        }
        if *slot != decision.slot
            || replica
                .slot(*slot)
                .and_then(|entry| entry.decision.as_ref())
                .is_none_or(|stored| stored.value_ids != decision.value_ids)
        {
            return Err(QuePaxaError::InvalidProposal(
                "runtime decision does not match the replica log".into(),
            ));
        }
    }
    for slot in pending.keys() {
        if decisions.contains_key(slot)
            || *slot <= replica.committed_through()
            || replica
                .slot(*slot)
                .is_none_or(|entry| entry.decision.is_some() || entry.proposed.is_empty())
        {
            return Err(QuePaxaError::InvalidProposal(
                "runtime snapshot has an invalid pending proposal".into(),
            ));
        }
    }
    let decision_slots = decisions.keys().copied().collect::<BTreeSet<_>>();
    if !executed.is_subset(&decision_slots)
        || !notified.is_subset(&decision_slots)
        || !notified.is_subset(executed)
    {
        return Err(QuePaxaError::InvalidProposal(
            "runtime snapshot has execution or notification metadata without a decision".into(),
        ));
    }
    for (slot, recipients) in announced_to {
        if !decision_slots.contains(slot) || !recipients.is_subset(&members) {
            return Err(QuePaxaError::InvalidProposal(
                "runtime snapshot has invalid decision dissemination metadata".into(),
            ));
        }
    }
    Ok(())
}

fn validate_recorders<C>(
    config: &ReplicaRuntimeConfig,
    recorders: &[RecorderHandle<C>],
) -> Result<()> {
    let configured = config.members().iter().copied().collect::<BTreeSet<_>>();
    let actual = recorders
        .iter()
        .map(RecorderHandle::id)
        .collect::<BTreeSet<_>>();
    if actual.len() != recorders.len() {
        return Err(QuePaxaError::DuplicateReplica(
            recorders
                .iter()
                .find(|recorder| {
                    recorders
                        .iter()
                        .filter(|candidate| candidate.id() == recorder.id())
                        .count()
                        > 1
                })
                .expect("a duplicate recorder exists")
                .id(),
        ));
    }
    if actual != configured {
        return Err(QuePaxaError::PolicyError(
            "runtime recorder handles must match the configured membership exactly".into(),
        ));
    }
    Ok(())
}

fn validate_recorders_for_cluster<C>(
    cluster: &ClusterIdentity,
    recorders: &[RecorderHandle<C>],
) -> Result<()> {
    let configured = cluster.members().iter().copied().collect::<BTreeSet<_>>();
    let actual = recorders
        .iter()
        .map(RecorderHandle::id)
        .collect::<BTreeSet<_>>();
    if actual.len() != recorders.len() {
        return Err(QuePaxaError::InvalidReconfiguration(
            "replacement recorder handles contain duplicate identities".into(),
        ));
    }
    if actual != configured {
        return Err(QuePaxaError::ConfigurationMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
