use crate::crash::{CrashInjector, CrashPoint, NoopCrashInjector};
use crate::error::{QuePaxaError, Result};
use crate::isr::{IntervalSummaryRegister, IntervalSummarySnapshot};
use crate::proposer::RecorderClient;
use crate::store::{AllowAllAvailability, ValueAvailability};
use crate::types::{
    ClusterIdentity, Decision, MembershipChange, RecordReply, RecordRequest, ReplicaId, SlotIndex,
    Step,
};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy)]
pub struct RecorderLimits {
    pub max_active_slots: usize,
    pub max_decisions: usize,
    pub max_values_per_proposal: usize,
    pub max_known_decisions: usize,
    pub max_step: Step,
}

impl Default for RecorderLimits {
    fn default() -> Self {
        Self {
            max_active_slots: 4096,
            max_decisions: 4096,
            max_values_per_proposal: 1024,
            max_known_decisions: 256,
            // 256 complete four-phase rounds. Reconfigure or checkpoint before
            // raising this bound, rather than accepting an attacker-controlled step.
            max_step: Step::new(1027),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecorderConfig {
    id: ReplicaId,
    cluster: ClusterIdentity,
    pub limits: RecorderLimits,
}

impl RecorderConfig {
    pub fn new(
        id: ReplicaId,
        members: impl IntoIterator<Item = ReplicaId>,
        max_faults: usize,
    ) -> Result<Self> {
        let cluster = ClusterIdentity::new(members, max_faults)?;
        if !cluster.contains(id) {
            return Err(QuePaxaError::UnknownReplica(id));
        }
        Ok(Self {
            id,
            cluster,
            limits: RecorderLimits::default(),
        })
    }

    pub fn with_limits(mut self, limits: RecorderLimits) -> Result<Self> {
        if limits.max_active_slots == 0
            || limits.max_decisions == 0
            || limits.max_values_per_proposal == 0
            || limits.max_known_decisions == 0
            || limits.max_step < Step::ROUND_ONE_PHASE_ZERO
        {
            return Err(QuePaxaError::InvalidProposal(
                "recorder limits must be non-zero and include the initial protocol step".into(),
            ));
        }
        self.limits = limits;
        Ok(self)
    }

    /// Creates a recorder directly in a versioned stable or joint epoch. This
    /// is used for a joining replica after it has installed state transfer.
    pub fn from_cluster(id: ReplicaId, cluster: ClusterIdentity) -> Result<Self> {
        if !cluster.contains(id) {
            return Err(QuePaxaError::UnknownReplica(id));
        }
        Ok(Self {
            id,
            cluster,
            limits: RecorderLimits::default(),
        })
    }

    pub fn id(&self) -> ReplicaId {
        self.id
    }

    pub fn contains(&self, replica: ReplicaId) -> bool {
        self.cluster.contains(replica)
    }

    pub fn max_faults(&self) -> usize {
        self.cluster.max_faults()
    }

    pub fn quorum_size(&self) -> usize {
        self.cluster.quorum_size()
    }

    pub fn cluster_identity(&self) -> &ClusterIdentity {
        &self.cluster
    }

    fn install_cluster(&mut self, cluster: ClusterIdentity) {
        self.cluster = cluster;
    }
}

/// Durable consensus state owned by one recorder.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecorderSnapshot<V> {
    pub cluster: ClusterIdentity,
    pub slots: BTreeMap<SlotIndex, IntervalSummarySnapshot<V>>,
    /// Round-one leader bound to each active slot. This is persisted alongside
    /// the ISR so a restart cannot forget a schedule already accepted.
    #[cfg_attr(feature = "network", serde(default))]
    pub round_one_leaders: BTreeMap<SlotIndex, Option<ReplicaId>>,
    pub decisions: BTreeMap<SlotIndex, Decision<V>>,
    /// Slots at or below this index were decided and pruned. Requests for them
    /// are rejected instead of silently re-running consensus on empty state.
    #[cfg_attr(feature = "network", serde(default))]
    pub pruned_through: SlotIndex,
}

/// Persists recorder state before a protocol reply is acknowledged.
pub trait RecorderStateStore<V> {
    fn load(&mut self) -> Result<Option<RecorderSnapshot<V>>>;
    fn save(&mut self, snapshot: &RecorderSnapshot<V>) -> Result<()>;
}

/// Codec used by [`FileRecorderStore`]. Applications choose the value encoding.
pub trait RecorderCodec<V> {
    fn encode(&self, snapshot: &RecorderSnapshot<V>) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<RecorderSnapshot<V>>;
}

/// Crash-safe recorder snapshot storage when paired with an application codec.
pub struct FileRecorderStore<V, C> {
    path: PathBuf,
    codec: C,
    marker: PhantomData<fn(V)>,
    crash_injector: Arc<dyn CrashInjector>,
}

impl<V, C> FileRecorderStore<V, C> {
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

impl<V, C: RecorderCodec<V>> RecorderStateStore<V> for FileRecorderStore<V, C> {
    fn load(&mut self) -> Result<Option<RecorderSnapshot<V>>> {
        match fs::read(&self.path) {
            Ok(bytes) => self.codec.decode(&bytes).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(QuePaxaError::StorageError(format!(
                "could not read recorder state {}: {error}",
                self.path.display()
            ))),
        }
    }

    fn save(&mut self, snapshot: &RecorderSnapshot<V>) -> Result<()> {
        let bytes = self.codec.encode(snapshot)?;
        let temporary = self.path.with_extension("recorder.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                QuePaxaError::StorageError(format!(
                    "could not open recorder state {}: {error}",
                    temporary.display()
                ))
            })?;
        self.crash_injector
            .reached(CrashPoint::RecorderTemporaryOpened)?;
        file.write_all(&bytes).map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not write recorder state {}: {error}",
                temporary.display()
            ))
        })?;
        self.crash_injector
            .reached(CrashPoint::RecorderTemporaryWritten)?;
        file.sync_all().map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not sync recorder state {}: {error}",
                temporary.display()
            ))
        })?;
        self.crash_injector
            .reached(CrashPoint::RecorderTemporarySynced)?;
        fs::rename(&temporary, &self.path).map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not replace recorder state {}: {error}",
                self.path.display()
            ))
        })?;
        self.crash_injector.reached(CrashPoint::RecorderRenamed)?;
        sync_parent(&self.path)?;
        self.crash_injector
            .reached(CrashPoint::RecorderDirectorySynced)
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
                "could not sync recorder state directory {}: {error}",
                parent.display()
            ))
        })
}

#[derive(Debug, Default)]
pub struct InMemoryRecorderStore<V> {
    snapshot: Option<RecorderSnapshot<V>>,
}

impl<V> InMemoryRecorderStore<V> {
    pub fn snapshot(&self) -> Option<&RecorderSnapshot<V>> {
        self.snapshot.as_ref()
    }
}

impl<V: Clone> RecorderStateStore<V> for InMemoryRecorderStore<V> {
    fn load(&mut self) -> Result<Option<RecorderSnapshot<V>>> {
        Ok(self.snapshot.clone())
    }

    fn save(&mut self, snapshot: &RecorderSnapshot<V>) -> Result<()> {
        self.snapshot = Some(snapshot.clone());
        Ok(())
    }
}

impl<V, S> RecorderStateStore<V> for Arc<Mutex<S>>
where
    S: RecorderStateStore<V>,
{
    fn load(&mut self) -> Result<Option<RecorderSnapshot<V>>> {
        self.lock()
            .map_err(|_| {
                QuePaxaError::StorageError("recorder state store lock was poisoned".into())
            })?
            .load()
    }

    fn save(&mut self, snapshot: &RecorderSnapshot<V>) -> Result<()> {
        self.lock()
            .map_err(|_| {
                QuePaxaError::StorageError("recorder state store lock was poisoned".into())
            })?
            .save(snapshot)
    }
}

pub struct RecorderCore<V> {
    config: RecorderConfig,
    availability: Arc<dyn ValueAvailability<V>>,
    slots: BTreeMap<SlotIndex, IntervalSummaryRegister<V>>,
    round_one_leaders: BTreeMap<SlotIndex, Option<ReplicaId>>,
    decisions: BTreeMap<SlotIndex, Decision<V>>,
    pruned_through: SlotIndex,
}

impl<V: Clone + Eq> RecorderCore<V> {
    pub fn new(config: RecorderConfig, availability: Arc<dyn ValueAvailability<V>>) -> Self {
        Self {
            config,
            availability,
            slots: BTreeMap::new(),
            round_one_leaders: BTreeMap::new(),
            decisions: BTreeMap::new(),
            pruned_through: SlotIndex::GENESIS,
        }
    }

    /// Restores recorder state written by [`RecorderStateStore`]. Corrupt or
    /// cross-cluster snapshots are rejected before the recorder serves RPCs.
    pub fn restore(
        config: RecorderConfig,
        availability: Arc<dyn ValueAvailability<V>>,
        snapshot: RecorderSnapshot<V>,
    ) -> Result<Self> {
        if snapshot.cluster != *config.cluster_identity() {
            return Err(QuePaxaError::ConfigurationMismatch);
        }
        if snapshot.slots.len() > config.limits.max_active_slots {
            return Err(QuePaxaError::ResourceLimit {
                resource: "active recorder slots",
                limit: config.limits.max_active_slots,
            });
        }
        if snapshot.decisions.len() > config.limits.max_decisions {
            return Err(QuePaxaError::ResourceLimit {
                resource: "retained decisions",
                limit: config.limits.max_decisions,
            });
        }

        let mut recorder = Self::new(config, availability);
        for (slot, decision) in &snapshot.decisions {
            if *slot != decision.slot || *slot <= snapshot.pruned_through {
                return Err(QuePaxaError::InvalidProposal(
                    "recorder snapshot decision key does not match its slot or pruned floor".into(),
                ));
            }
            recorder.validate_decision(decision)?;
        }
        if snapshot
            .slots
            .keys()
            .any(|slot| *slot <= snapshot.pruned_through)
        {
            return Err(QuePaxaError::InvalidProposal(
                "recorder snapshot retains a slot at or below its pruned floor".into(),
            ));
        }
        if snapshot.round_one_leaders.keys().ne(snapshot.slots.keys()) {
            return Err(QuePaxaError::InvalidProposal(
                "recorder snapshot leader bindings do not match its active slots".into(),
            ));
        }
        if snapshot
            .round_one_leaders
            .values()
            .flatten()
            .any(|leader| !recorder.config.contains(*leader))
        {
            return Err(QuePaxaError::InvalidProposal(
                "recorder snapshot binds a slot to an unknown leader".into(),
            ));
        }
        for state in snapshot.slots.values() {
            recorder.validate_isr_snapshot(state)?;
        }
        recorder.restore_snapshot_state(snapshot);
        Ok(recorder)
    }

    pub fn snapshot(&self) -> RecorderSnapshot<V> {
        RecorderSnapshot {
            cluster: self.config.cluster_identity().clone(),
            slots: self
                .slots
                .iter()
                .map(|(slot, state)| (*slot, state.snapshot()))
                .collect(),
            round_one_leaders: self.round_one_leaders.clone(),
            decisions: self.decisions.clone(),
            pruned_through: self.pruned_through,
        }
    }

    /// Creates an in-memory recorder for tests only. Production callers must
    /// provide a real availability guard to `new`.
    pub fn permissive_for_tests(id: ReplicaId, members: Vec<ReplicaId>) -> Result<Self> {
        let max_faults = members.len().saturating_sub(1) / 2;
        Ok(Self::new(
            RecorderConfig::new(id, members, max_faults)?,
            Arc::new(AllowAllAvailability),
        ))
    }

    pub fn id(&self) -> ReplicaId {
        self.config.id()
    }

    pub fn decisions(&self) -> impl Iterator<Item = &Decision<V>> {
        self.decisions.values()
    }

    /// Drops finalized state and raises the durable pruned floor.
    ///
    /// Safety contract: prune a slot only after every replica has learned its
    /// decision (or after a cluster-wide application checkpoint covering it).
    /// A proposer that still asks about a pruned slot receives [`QuePaxaError::SlotPruned`]
    /// instead of silently re-running consensus on empty state, which could
    /// otherwise decide a conflicting value.
    pub fn prune_through(&mut self, slot: SlotIndex) {
        self.slots.retain(|index, _| *index > slot);
        self.round_one_leaders.retain(|index, _| *index > slot);
        self.decisions.retain(|index, _| *index > slot);
        self.pruned_through = self.pruned_through.max(slot);
    }

    pub fn pruned_through(&self) -> SlotIndex {
        self.pruned_through
    }

    /// Advances this recorder to the pruning floor installed by an application
    /// state transfer. Active ISR and retained decision state at or below the
    /// transferred checkpoint is superseded by that checkpoint.
    pub fn install_state_transfer_floor(&mut self, slot: SlotIndex) {
        self.prune_through(slot);
    }

    /// Read-only progress probe used for hedging suppression. Reports the
    /// slot's current logical step without mutating any consensus state.
    pub fn status(&self, slot: SlotIndex) -> Option<Step> {
        self.slots.get(&slot).map(IntervalSummaryRegister::step)
    }

    pub fn inform_decision(&mut self, decision: Decision<V>) -> Result<()> {
        self.validate_decision(&decision)?;
        if decision.slot <= self.pruned_through {
            // The checkpoint already subsumes this decision. A delayed retry
            // is idempotent and must not recreate state below the durable floor.
            return Ok(());
        }
        if let Some(existing) = self.decisions.get(&decision.slot) {
            if existing.value_ids != decision.value_ids {
                return Err(QuePaxaError::ConflictingDecision {
                    slot: decision.slot,
                });
            }
            return Ok(());
        }
        if self.decisions.len() == self.config.limits.max_decisions {
            return Err(QuePaxaError::ResourceLimit {
                resource: "retained decisions",
                limit: self.config.limits.max_decisions,
            });
        }

        self.decisions.insert(decision.slot, decision);
        Ok(())
    }

    /// Installs the next stable/joint membership after the anchoring decision
    /// is durably known. Reconfiguration is a barrier: no active slot may be
    /// newer than the anchor when the transition is applied.
    pub fn install_membership(&mut self, change: MembershipChange<V>) -> Result<()> {
        change.validate_binding()?;
        if *self.config.cluster_identity() == change.next {
            return Ok(());
        }
        if !change.next.is_successor_of(self.config.cluster_identity()) {
            return Err(QuePaxaError::InvalidReconfiguration(
                "membership transition does not follow the local configuration epoch".into(),
            ));
        }
        let Some(anchor) = self.decisions.get(&change.anchor.slot) else {
            return Err(QuePaxaError::InvalidReconfiguration(
                "membership anchor decision is not retained by this recorder".into(),
            ));
        };
        if anchor != &change.anchor {
            return Err(QuePaxaError::ConflictingDecision {
                slot: change.anchor.slot,
            });
        }
        if self.slots.keys().any(|slot| *slot > change.anchor.slot)
            || self.decisions.keys().any(|slot| *slot > change.anchor.slot)
        {
            return Err(QuePaxaError::InvalidReconfiguration(
                "membership changes require all later consensus slots to be drained".into(),
            ));
        }
        self.config.install_cluster(change.next);
        Ok(())
    }

    fn validate_decision(&self, decision: &Decision<V>) -> Result<()> {
        if decision.value_ids.is_empty() {
            return Err(QuePaxaError::EmptyDecision);
        }
        if !self.config.contains(decision.proposer) {
            return Err(QuePaxaError::InvalidSender(decision.proposer));
        }
        if decision.decided_step > self.config.limits.max_step {
            return Err(QuePaxaError::InvalidStep {
                step: decision.decided_step.get(),
                limit: self.config.limits.max_step.get(),
            });
        }
        if decision.value_ids.len() > self.config.limits.max_values_per_proposal {
            return Err(QuePaxaError::ResourceLimit {
                resource: "decision value IDs",
                limit: self.config.limits.max_values_per_proposal,
            });
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

    fn validate_isr_snapshot(&self, state: &IntervalSummarySnapshot<V>) -> Result<()> {
        if state.step < Step::ROUND_ONE_PHASE_ZERO || state.step > self.config.limits.max_step {
            return Err(QuePaxaError::InvalidStep {
                step: state.step.get(),
                limit: self.config.limits.max_step.get(),
            });
        }
        if state.first.is_none() || state.current_aggregate.is_none() {
            return Err(QuePaxaError::MissingProposal);
        }
        for proposal in [
            state.first.as_ref(),
            state.current_aggregate.as_ref(),
            state.prior_aggregate.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            self.validate_proposal(proposal)?;
        }
        Ok(())
    }

    fn validate_proposal(&self, proposal: &crate::types::Proposal<V>) -> Result<()> {
        if !self.config.contains(proposal.key.proposer_id) {
            return Err(QuePaxaError::InvalidSender(proposal.key.proposer_id));
        }
        if proposal.value_ids.is_empty() {
            return Err(QuePaxaError::EmptyProposal);
        }
        if proposal.value_ids.len() > self.config.limits.max_values_per_proposal {
            return Err(QuePaxaError::ResourceLimit {
                resource: "proposal value IDs",
                limit: self.config.limits.max_values_per_proposal,
            });
        }
        if proposal
            .value_ids
            .iter()
            .enumerate()
            .any(|(index, value)| proposal.value_ids[..index].contains(value))
        {
            return Err(QuePaxaError::InvalidProposal(
                "proposal contains duplicate value IDs".into(),
            ));
        }
        Ok(())
    }

    fn restore_snapshot_state(&mut self, snapshot: RecorderSnapshot<V>) {
        self.slots = snapshot
            .slots
            .into_iter()
            .map(|(slot, state)| (slot, IntervalSummaryRegister::restore(state)))
            .collect();
        self.round_one_leaders = snapshot.round_one_leaders;
        self.decisions = snapshot.decisions;
        self.pruned_through = snapshot.pruned_through;
    }

    pub fn record(&mut self, request: RecordRequest<V>) -> Result<RecordReply<V>> {
        self.validate_request(&request)?;
        if request.slot <= self.pruned_through {
            return Err(QuePaxaError::SlotPruned { slot: request.slot });
        }
        if self
            .round_one_leaders
            .get(&request.slot)
            .is_some_and(|leader| *leader != request.round_one_leader)
        {
            return Err(QuePaxaError::ScheduleMismatch { slot: request.slot });
        }
        self.availability
            .ensure_available(&request.proposal.value_ids)?;

        for decision in request.known_decisions {
            self.inform_decision(decision)?;
        }

        if !self.slots.contains_key(&request.slot)
            && self.slots.len() == self.config.limits.max_active_slots
        {
            return Err(QuePaxaError::ResourceLimit {
                resource: "active recorder slots",
                limit: self.config.limits.max_active_slots,
            });
        }

        self.round_one_leaders
            .entry(request.slot)
            .or_insert(request.round_one_leader);
        let slot = self.slots.entry(request.slot).or_default();
        let reply = slot.record(request.step, request.proposal);
        Ok(RecordReply {
            recorder: self.config.id(),
            cluster: self.config.cluster_identity().clone(),
            summary: reply,
            decision: self.decisions.get(&request.slot).cloned(),
        })
    }

    /// Records a request received over an authenticated transport. The server
    /// adapter must derive `authenticated_sender` from its peer credentials,
    /// never from request data supplied by the network.
    pub fn record_from(
        &mut self,
        authenticated_sender: ReplicaId,
        request: RecordRequest<V>,
    ) -> Result<RecordReply<V>> {
        if authenticated_sender != request.sender {
            return Err(QuePaxaError::InvalidSender(request.sender));
        }
        self.record(request)
    }

    fn validate_request(&self, request: &RecordRequest<V>) -> Result<()> {
        if !self.config.contains(request.sender) {
            return Err(QuePaxaError::InvalidSender(request.sender));
        }
        if let Some(leader) = request.round_one_leader
            && !self.config.contains(leader)
        {
            return Err(QuePaxaError::UnknownReplica(leader));
        }
        self.validate_proposal(&request.proposal)?;
        if request.step < Step::ROUND_ONE_PHASE_ZERO || request.step > self.config.limits.max_step {
            return Err(QuePaxaError::InvalidStep {
                step: request.step.get(),
                limit: self.config.limits.max_step.get(),
            });
        }
        if request.known_decisions.len() > self.config.limits.max_known_decisions {
            return Err(QuePaxaError::ResourceLimit {
                resource: "known decisions in one request",
                limit: self.config.limits.max_known_decisions,
            });
        }
        Ok(())
    }
}

/// Recorder wrapper that makes every acknowledged mutation durable.
///
/// A failed save rolls the in-memory core back to its last durable snapshot,
/// so callers may retry without exposing state that was never persisted.
pub struct DurableRecorderCore<V, S> {
    core: RecorderCore<V>,
    store: S,
}

impl<V, S> DurableRecorderCore<V, S>
where
    V: Clone + Eq,
    S: RecorderStateStore<V>,
{
    pub fn new(
        config: RecorderConfig,
        availability: Arc<dyn ValueAvailability<V>>,
        mut store: S,
    ) -> Result<Self> {
        let core = match store.load()? {
            Some(snapshot) => RecorderCore::restore(config, availability, snapshot)?,
            None => RecorderCore::new(config, availability),
        };
        Ok(Self { core, store })
    }

    pub fn id(&self) -> ReplicaId {
        self.core.id()
    }

    pub fn core(&self) -> &RecorderCore<V> {
        &self.core
    }

    pub fn state_store(&self) -> &S {
        &self.store
    }

    pub fn record(&mut self, request: RecordRequest<V>) -> Result<RecordReply<V>> {
        self.mutate(|core| core.record(request))
    }

    pub fn record_from(
        &mut self,
        authenticated_sender: ReplicaId,
        request: RecordRequest<V>,
    ) -> Result<RecordReply<V>> {
        self.mutate(|core| core.record_from(authenticated_sender, request))
    }

    pub fn inform_decision(&mut self, decision: Decision<V>) -> Result<()> {
        self.mutate(|core| core.inform_decision(decision))
    }

    pub fn prune_through(&mut self, slot: SlotIndex) -> Result<()> {
        self.mutate(|core| {
            core.prune_through(slot);
            Ok(())
        })
    }

    pub fn install_state_transfer_floor(&mut self, slot: SlotIndex) -> Result<()> {
        self.mutate(|core| {
            core.install_state_transfer_floor(slot);
            Ok(())
        })
    }

    pub fn install_membership(&mut self, change: MembershipChange<V>) -> Result<()> {
        self.mutate(|core| core.install_membership(change))
    }

    fn mutate<T>(&mut self, mutation: impl FnOnce(&mut RecorderCore<V>) -> Result<T>) -> Result<T> {
        let before = self.core.snapshot();
        let output = match mutation(&mut self.core) {
            Ok(output) => output,
            Err(error) => {
                self.core.restore_snapshot_state(before);
                return Err(error);
            }
        };
        let after = self.core.snapshot();
        if let Err(error) = self.store.save(&after) {
            self.core.restore_snapshot_state(before);
            return Err(error);
        }
        Ok(output)
    }
}

impl<V, S> RecorderClient<V> for DurableRecorderCore<V, S>
where
    V: Clone + Eq + Send,
    S: RecorderStateStore<V> + Send,
{
    fn record(&mut self, request: RecordRequest<V>) -> Result<RecordReply<V>> {
        DurableRecorderCore::record(self, request)
    }

    fn inform_decisions(&mut self, decisions: &[Decision<V>]) -> Result<()> {
        self.mutate(|core| {
            for decision in decisions {
                core.inform_decision(decision.clone())?;
            }
            Ok(())
        })
    }

    fn status(&mut self, slot: SlotIndex) -> Result<Option<Step>> {
        Ok(self.core.status(slot))
    }

    fn install_membership(&mut self, change: MembershipChange<V>) -> Result<()> {
        DurableRecorderCore::install_membership(self, change)
    }
}

impl<V: Clone + Eq + Send> RecorderClient<V> for RecorderCore<V> {
    fn record(&mut self, request: RecordRequest<V>) -> Result<RecordReply<V>> {
        RecorderCore::record(self, request)
    }

    fn inform_decisions(&mut self, decisions: &[Decision<V>]) -> Result<()> {
        for decision in decisions {
            self.inform_decision(decision.clone())?;
        }
        Ok(())
    }

    fn status(&mut self, slot: SlotIndex) -> Result<Option<Step>> {
        Ok(RecorderCore::status(self, slot))
    }

    fn install_membership(&mut self, change: MembershipChange<V>) -> Result<()> {
        RecorderCore::install_membership(self, change)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LaneId, Priority, Proposal, ProposalKey};

    fn request(value: u64) -> RecordRequest<u64> {
        RecordRequest {
            sender: ReplicaId::new(1),
            slot: SlotIndex::new(1),
            round_one_leader: Some(ReplicaId::new(1)),
            step: Step::ROUND_ONE_PHASE_ZERO,
            proposal: Proposal::new(
                ProposalKey::new(Priority::MAX, ReplicaId::new(1), LaneId::new(1)),
                vec![value],
            )
            .unwrap(),
            known_decisions: Vec::new(),
        }
    }

    fn request_with_leader(value: u64, leader: ReplicaId) -> RecordRequest<u64> {
        RecordRequest {
            round_one_leader: Some(leader),
            ..request(value)
        }
    }

    #[test]
    fn recorder_config_rejects_an_unsupported_fault_budget() {
        assert!(matches!(
            RecorderConfig::new(
                ReplicaId::new(1),
                [ReplicaId::new(1), ReplicaId::new(2), ReplicaId::new(3)],
                2,
            ),
            Err(QuePaxaError::InvalidProposal(_))
        ));
    }

    #[test]
    fn recorder_config_exposes_the_paper_quorum() {
        let config =
            RecorderConfig::new(ReplicaId::new(1), (1..=5).map(ReplicaId::new), 1).unwrap();

        assert_eq!(config.max_faults(), 1);
        assert_eq!(config.quorum_size(), 4);
    }

    #[test]
    fn durable_recorder_restores_consensus_state() {
        let members = vec![ReplicaId::new(1)];
        let store = Arc::new(Mutex::new(InMemoryRecorderStore::default()));
        let mut first = DurableRecorderCore::new(
            RecorderConfig::new(ReplicaId::new(1), members.clone(), 0).unwrap(),
            Arc::new(AllowAllAvailability),
            Arc::clone(&store),
        )
        .unwrap();
        first.record(request(10)).unwrap();
        drop(first);

        let mut restored = DurableRecorderCore::new(
            RecorderConfig::new(ReplicaId::new(1), members, 0).unwrap(),
            Arc::new(AllowAllAvailability),
            store,
        )
        .unwrap();
        let reply = restored.record(request(20)).unwrap();

        assert_eq!(reply.summary.first.unwrap().value_ids, vec![10]);
    }

    struct FailingStore;

    impl RecorderStateStore<u64> for FailingStore {
        fn load(&mut self) -> Result<Option<RecorderSnapshot<u64>>> {
            Ok(None)
        }

        fn save(&mut self, _snapshot: &RecorderSnapshot<u64>) -> Result<()> {
            Err(QuePaxaError::StorageError("simulated disk failure".into()))
        }
    }

    #[test]
    fn durable_recorder_does_not_acknowledge_or_retain_an_unpersisted_write() {
        let mut recorder = DurableRecorderCore::new(
            RecorderConfig::new(ReplicaId::new(1), [ReplicaId::new(1)], 0).unwrap(),
            Arc::new(AllowAllAvailability),
            FailingStore,
        )
        .unwrap();

        assert!(matches!(
            recorder.record(request(10)),
            Err(QuePaxaError::StorageError(_))
        ));
        assert!(recorder.core().snapshot().slots.is_empty());
    }

    #[test]
    fn durable_pruned_floor_rejects_old_records_and_ignores_late_decisions() {
        let members = vec![ReplicaId::new(1)];
        let store = Arc::new(Mutex::new(InMemoryRecorderStore::default()));
        let mut recorder = DurableRecorderCore::new(
            RecorderConfig::new(ReplicaId::new(1), members.clone(), 0).unwrap(),
            Arc::new(AllowAllAvailability),
            Arc::clone(&store),
        )
        .unwrap();
        recorder.record(request(10)).unwrap();
        recorder.prune_through(SlotIndex::new(1)).unwrap();
        recorder
            .inform_decision(
                Decision::new(
                    SlotIndex::new(1),
                    vec![10],
                    ReplicaId::new(1),
                    Step::ROUND_ONE_PHASE_ZERO,
                )
                .unwrap(),
            )
            .unwrap();
        assert!(recorder.core().snapshot().decisions.is_empty());
        drop(recorder);

        let mut restored = DurableRecorderCore::new(
            RecorderConfig::new(ReplicaId::new(1), members, 0).unwrap(),
            Arc::new(AllowAllAvailability),
            store,
        )
        .unwrap();
        assert_eq!(
            restored.record(request(20)).unwrap_err(),
            QuePaxaError::SlotPruned {
                slot: SlotIndex::new(1)
            }
        );
    }

    #[test]
    fn leader_binding_survives_recorder_restart() {
        let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
        let store = Arc::new(Mutex::new(InMemoryRecorderStore::default()));
        let config = || RecorderConfig::new(ReplicaId::new(1), members.clone(), 1).unwrap();
        let mut recorder =
            DurableRecorderCore::new(config(), Arc::new(AllowAllAvailability), Arc::clone(&store))
                .unwrap();
        recorder
            .record(request_with_leader(10, ReplicaId::new(1)))
            .unwrap();
        drop(recorder);

        let mut restored =
            DurableRecorderCore::new(config(), Arc::new(AllowAllAvailability), store).unwrap();
        assert_eq!(
            restored
                .record(request_with_leader(20, ReplicaId::new(2)))
                .unwrap_err(),
            QuePaxaError::ScheduleMismatch {
                slot: SlotIndex::new(1)
            }
        );
    }

    #[test]
    fn membership_change_is_anchored_and_persisted() {
        let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
        let stable = ClusterIdentity::stable(4, members.clone(), 1).unwrap();
        let joint = stable.begin_joint((2..=4).map(ReplicaId::new), 1).unwrap();
        let store = Arc::new(Mutex::new(InMemoryRecorderStore::default()));
        let mut recorder = DurableRecorderCore::new(
            RecorderConfig::from_cluster(ReplicaId::new(2), stable.clone()).unwrap(),
            Arc::new(AllowAllAvailability),
            Arc::clone(&store),
        )
        .unwrap();
        let anchor = Decision::new(
            SlotIndex::new(1),
            vec![99],
            ReplicaId::new(1),
            Step::ROUND_ONE_PHASE_ZERO,
        )
        .unwrap();
        recorder.inform_decision(anchor.clone()).unwrap();
        recorder
            .install_membership(MembershipChange::new(anchor, joint.clone(), 99).unwrap())
            .unwrap();
        drop(recorder);

        let restored = DurableRecorderCore::new(
            RecorderConfig::from_cluster(ReplicaId::new(2), joint.clone()).unwrap(),
            Arc::new(AllowAllAvailability),
            store,
        )
        .unwrap();
        assert_eq!(restored.core().snapshot().cluster, joint);
    }
}
