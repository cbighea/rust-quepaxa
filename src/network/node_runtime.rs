use crate::error::{QuePaxaError, Result};
use crate::network::async_proposer::{AsyncProposerCore, AsyncRecorderClient};
use crate::network::metrics::NetworkMetrics;
use crate::network::server::{BoxSubmissionFuture, SubmissionHandler};
use crate::network::wire::{Submission, SubmissionOutcome};
use crate::replica::ReplicaCore;
use crate::runtime::{
    EpochSchedule, EpochTuner, HedgeDelayTuner, PendingProposalSnapshot, ReplicaRuntimeConfig,
    RuntimeSnapshot, RuntimeStateStore, StateMachine, StateTransferSnapshot,
};
use crate::types::{Decision, MembershipChange, ReplicaId, SlotIndex};
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct ConsensusState<V> {
    replica: ReplicaCore<V>,
    decisions: BTreeMap<SlotIndex, Decision<V>>,
    pending: BTreeMap<SlotIndex, Vec<V>>,
    executed: BTreeSet<SlotIndex>,
    tuner: EpochTuner,
    /// When each pending slot was first armed; hedging delays count from here.
    /// Not persisted: a restart simply restarts the local hedging clock.
    armed_at: BTreeMap<SlotIndex, Instant>,
    /// Highest step seen by this node while suppressing a hedged proposal.
    observed_steps: BTreeMap<SlotIndex, crate::Step>,
    hedge_delay: HedgeDelayTuner,
}

/// Async submission runtime used by [`crate::network::NetworkNodeServer`].
///
/// Different server connections may drive different slots concurrently. Local
/// state transitions and durable snapshots remain serialized, while network
/// quorum waits do not hold the state lock.
///
/// The round-one leader for each slot comes from its epoch schedule: install
/// agreed schedules with [`install_epoch_schedule`](Self::install_epoch_schedule),
/// or enable [`ReplicaRuntimeConfig::with_auto_schedules`] to derive them
/// deterministically from the committed log. Non-leader replicas wait their
/// hedging delay (position in the schedule times the base delay) before
/// proposing, and stand down when a decision arrives first.
pub struct NetworkConsensusHandler<V, C, S, E> {
    config: RwLock<ReplicaRuntimeConfig>,
    recorders: RwLock<Arc<Vec<C>>>,
    membership_gate: tokio::sync::RwLock<()>,
    state: tokio::sync::Mutex<ConsensusState<V>>,
    state_store: Arc<Mutex<S>>,
    state_machine: Arc<Mutex<E>>,
    metrics: Arc<NetworkMetrics>,
    progress: tokio::sync::Notify,
    noop_value: Option<V>,
}

impl<V, C, S, E> NetworkConsensusHandler<V, C, S, E>
where
    V: Clone + Ord + Send + Sync + 'static,
    C: AsyncRecorderClient<V> + 'static,
    S: RuntimeStateStore<V> + Send + 'static,
    E: StateMachine<V> + Send + 'static,
{
    pub async fn new(
        config: ReplicaRuntimeConfig,
        recorders: Vec<C>,
        state_store: S,
        state_machine: E,
        metrics: Arc<NetworkMetrics>,
    ) -> Result<Self> {
        let expected = config.members().iter().copied().collect::<BTreeSet<_>>();
        let actual = recorders
            .iter()
            .map(AsyncRecorderClient::id)
            .collect::<BTreeSet<_>>();
        if actual != expected || recorders.len() != expected.len() {
            return Err(QuePaxaError::ConfigurationMismatch);
        }

        let state_store = Arc::new(Mutex::new(state_store));
        let loaded = load_store(Arc::clone(&state_store)).await?;
        let state = match loaded {
            Some(snapshot) => {
                if snapshot.cluster != *config.cluster_identity()
                    || snapshot.protocol != config.protocol_identity()
                {
                    return Err(QuePaxaError::ConfigurationMismatch);
                }
                let pending = snapshot
                    .pending
                    .into_iter()
                    .map(|(slot, proposal)| (slot, proposal.value_ids))
                    .collect();
                ConsensusState {
                    replica: ReplicaCore::restore(config.replica, snapshot.replica)?,
                    decisions: snapshot.decisions,
                    pending,
                    executed: snapshot.executed,
                    tuner: EpochTuner::restore(
                        config.epoch_size,
                        config.members().to_vec(),
                        config.auto_schedules(),
                        snapshot.schedules,
                        snapshot.epoch_stats,
                        snapshot.stats_through,
                    )?,
                    armed_at: BTreeMap::new(),
                    observed_steps: BTreeMap::new(),
                    hedge_delay: HedgeDelayTuner::new(
                        config.base_hedge_delay,
                        config.adaptive_hedging().cloned(),
                    ),
                }
            }
            None => ConsensusState {
                replica: ReplicaCore::new(config.replica),
                decisions: BTreeMap::new(),
                pending: BTreeMap::new(),
                executed: BTreeSet::new(),
                tuner: EpochTuner::new(
                    config.epoch_size,
                    config.members().to_vec(),
                    config.auto_schedules(),
                ),
                armed_at: BTreeMap::new(),
                observed_steps: BTreeMap::new(),
                hedge_delay: HedgeDelayTuner::new(
                    config.base_hedge_delay,
                    config.adaptive_hedging().cloned(),
                ),
            },
        };
        validate_state(&config, &state)?;

        Ok(Self {
            config: RwLock::new(config),
            recorders: RwLock::new(Arc::new(recorders)),
            membership_gate: tokio::sync::RwLock::new(()),
            state: tokio::sync::Mutex::new(state),
            state_store,
            state_machine: Arc::new(Mutex::new(state_machine)),
            metrics,
            progress: tokio::sync::Notify::new(),
            noop_value: None,
        })
    }

    /// Configures the value ID used to recover an idle gap in the committed
    /// prefix. The application state machine must treat this value as a no-op.
    pub fn with_noop_value(mut self, value: V) -> Self {
        self.noop_value = Some(value);
        self
    }

    pub async fn current_hedge_delay(&self) -> Duration {
        self.state.lock().await.hedge_delay.current()
    }

    fn current_config(&self) -> ReplicaRuntimeConfig {
        self.config
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn current_recorders(&self) -> Arc<Vec<C>> {
        Arc::clone(
            &self
                .recorders
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    /// Installs an externally agreed epoch schedule. Every replica must
    /// install the identical schedule for an epoch; prefer
    /// [`ReplicaRuntimeConfig::with_auto_schedules`], which needs no external
    /// agreement. Rejected in auto mode.
    pub async fn install_epoch_schedule(&self, schedule: EpochSchedule) -> Result<()> {
        let _membership = self.membership_gate.read().await;
        let scheduled = schedule.proposers.iter().copied().collect::<BTreeSet<_>>();
        let config = self.current_config();
        let configured = config.members().iter().copied().collect::<BTreeSet<_>>();
        if scheduled != configured || schedule.proposers.len() != configured.len() {
            return Err(QuePaxaError::PolicyError(
                "an epoch schedule must contain every configured replica exactly once".into(),
            ));
        }
        let mut state = self.state.lock().await;
        let before = state.clone();
        if state.tuner.install(schedule)? {
            let snapshot = self.snapshot(&state);
            if let Err(error) = save_store(Arc::clone(&self.state_store), snapshot).await {
                *state = before;
                return Err(error);
            }
        }
        Ok(())
    }

    /// Installs a drained, consensus-anchored stable/joint membership epoch
    /// without restarting the async node runtime.
    pub async fn install_membership(
        &self,
        change: MembershipChange<V>,
        next_recorders: Vec<C>,
    ) -> Result<()> {
        let _membership = self.membership_gate.write().await;
        change.validate_binding()?;
        let previous = self.current_config();
        if change.next == *previous.cluster_identity() {
            validate_async_recorders(&change.next, &next_recorders)?;
            *self
                .recorders
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(next_recorders);
            return Ok(());
        }
        if !change.next.is_successor_of(previous.cluster_identity()) {
            return Err(QuePaxaError::InvalidReconfiguration(
                "membership transition does not follow the network runtime epoch".into(),
            ));
        }
        validate_async_recorders(&change.next, &next_recorders)?;

        let retained_decisions = {
            let state = self.state.lock().await;
            if state.replica.committed_through() != change.anchor.slot
                || state.decisions.get(&change.anchor.slot) != Some(&change.anchor)
                || !state.pending.is_empty()
                || state
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
            state.decisions.values().cloned().collect::<Vec<_>>()
        };
        self.disseminate_all(retained_decisions).await?;

        let mut installs = FuturesUnordered::new();
        for recorder in &next_recorders {
            installs.push(recorder.install_membership(change.clone()));
        }
        let mut acknowledgements = 0;
        while let Some(result) = installs.next().await {
            result?;
            acknowledgements += 1;
        }
        drop(installs);
        if acknowledgements != next_recorders.len() {
            return Err(QuePaxaError::QuorumNotReached {
                needed: next_recorders.len(),
                received: acknowledgements,
            });
        }

        let mut state = self.state.lock().await;
        let before = state.clone();
        state
            .tuner
            .reconfigure(change.next.members().to_vec(), change.anchor.slot);
        let mut next_config = previous.clone();
        next_config.install_cluster(change.next.clone());
        *self
            .config
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next_config;
        let old_recorders = {
            let mut recorders = self
                .recorders
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::replace(&mut *recorders, Arc::new(next_recorders))
        };
        let snapshot = self.snapshot(&state);
        if let Err(error) = save_store(Arc::clone(&self.state_store), snapshot).await {
            *state = before;
            *self
                .config
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = previous;
            *self
                .recorders
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = old_recorders;
            return Err(error);
        }
        self.progress.notify_waiters();
        Ok(())
    }

    pub async fn decision(&self, slot: SlotIndex) -> Option<Decision<V>> {
        self.state.lock().await.decisions.get(&slot).cloned()
    }

    /// Creates a portable application/consensus checkpoint after every member
    /// has acknowledged all decisions through `slot`.
    pub async fn create_state_transfer(&self, slot: SlotIndex) -> Result<StateTransferSnapshot<V>> {
        let _membership = self.membership_gate.read().await;
        let decisions = {
            let state = self.state.lock().await;
            if slot > state.replica.committed_through() {
                return Err(QuePaxaError::InvalidProposal(
                    "cannot checkpoint beyond the contiguous committed prefix".into(),
                ));
            }
            if state.pending.range(..=slot).next().is_some() {
                return Err(QuePaxaError::InvalidProposal(
                    "cannot checkpoint a slot with a pending local proposal".into(),
                ));
            }
            state
                .decisions
                .range(..=slot)
                .map(|(_, decision)| decision.clone())
                .collect::<Vec<_>>()
        };
        let checkpointed_value_ids = decisions
            .iter()
            .flat_map(|decision| decision.value_ids.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self.disseminate_all(decisions).await?;

        let machine = Arc::clone(&self.state_machine);
        let application_checkpoint = tokio::task::spawn_blocking(move || {
            machine
                .lock()
                .map_err(|_| QuePaxaError::StorageError("state machine lock was poisoned".into()))?
                .export_checkpoint(slot)
        })
        .await
        .map_err(|error| {
            QuePaxaError::StorageError(format!("state machine checkpoint task failed: {error}"))
        })??;

        let mut state = self.state.lock().await;
        if slot > state.replica.committed_through() || state.pending.range(..=slot).next().is_some()
        {
            return Err(QuePaxaError::InvalidProposal(
                "runtime changed while preparing its state-transfer checkpoint".into(),
            ));
        }
        let before = state.clone();
        state.replica.prune_through(slot);
        state.decisions.retain(|index, _| *index > slot);
        state.executed.retain(|index| *index > slot);
        state.pending.retain(|index, _| *index > slot);
        state.armed_at.retain(|index, _| *index > slot);
        state.observed_steps.retain(|index, _| *index > slot);
        let runtime = self.snapshot(&state);
        if let Err(error) = save_store(Arc::clone(&self.state_store), runtime.clone()).await {
            *state = before;
            return Err(error);
        }
        Ok(StateTransferSnapshot {
            through: slot,
            checkpointed_value_ids,
            application_checkpoint,
            runtime,
        })
    }

    /// Installs a state-transfer snapshot before the node begins serving
    /// traffic. Application import must be durable and idempotent.
    pub async fn install_state_transfer(&self, transfer: StateTransferSnapshot<V>) -> Result<()> {
        let _membership = self.membership_gate.read().await;
        let config = self.current_config();
        if transfer.through != transfer.runtime.replica.checkpointed_through
            || transfer.runtime.cluster != *config.cluster_identity()
            || transfer.runtime.protocol != config.protocol_identity()
        {
            return Err(QuePaxaError::ConfigurationMismatch);
        }
        let pending = transfer
            .runtime
            .pending
            .iter()
            .map(|(slot, proposal)| (*slot, proposal.value_ids.clone()))
            .collect();
        let replacement = ConsensusState {
            replica: ReplicaCore::restore(config.replica, transfer.runtime.replica.clone())?,
            decisions: transfer.runtime.decisions.clone(),
            pending,
            executed: transfer.runtime.executed.clone(),
            tuner: EpochTuner::restore(
                config.epoch_size,
                config.members().to_vec(),
                config.auto_schedules(),
                transfer.runtime.schedules.clone(),
                transfer.runtime.epoch_stats.clone(),
                transfer.runtime.stats_through,
            )?,
            armed_at: BTreeMap::new(),
            observed_steps: BTreeMap::new(),
            hedge_delay: HedgeDelayTuner::new(
                config.base_hedge_delay,
                config.adaptive_hedging().cloned(),
            ),
        };
        validate_state(&config, &replacement)?;

        let mut state = self.state.lock().await;
        let machine = Arc::clone(&self.state_machine);
        let through = transfer.through;
        let application_checkpoint = transfer.application_checkpoint.clone();
        tokio::task::spawn_blocking(move || {
            machine
                .lock()
                .map_err(|_| QuePaxaError::StorageError("state machine lock was poisoned".into()))?
                .import_checkpoint(through, &application_checkpoint)
        })
        .await
        .map_err(|error| {
            QuePaxaError::StorageError(format!("state machine import task failed: {error}"))
        })??;
        save_store(Arc::clone(&self.state_store), transfer.runtime).await?;
        *state = replacement;
        self.progress.notify_waiters();
        Ok(())
    }

    pub async fn recover(&self) -> Result<Vec<Decision<V>>> {
        let _membership = self.membership_gate.read().await;
        self.recover_inner().await
    }

    async fn recover_inner(&self) -> Result<Vec<Decision<V>>> {
        let pending = {
            let mut state = self.state.lock().await;
            let before = state.clone();
            if self.arm_gap_noop(&mut state)?
                && let Err(error) =
                    save_store(Arc::clone(&self.state_store), self.snapshot(&state)).await
            {
                *state = before;
                return Err(error);
            }
            self.execute_ready(&mut state).await?;
            state
                .pending
                .iter()
                .map(|(slot, values)| (*slot, values.clone()))
                .collect::<Vec<_>>()
        };
        let mut proposals = FuturesUnordered::new();
        for (slot, values) in pending {
            proposals.push(async move {
                let Some(leader) = self.wait_for_turn(slot).await? else {
                    return Ok(None);
                };
                self.propose_slot(slot, values, leader).await.map(Some)
            });
        }
        let mut recovered = Vec::new();
        while let Some(result) = proposals.next().await {
            if let Some(decision) = result? {
                recovered.push(decision);
            }
        }
        Ok(recovered)
    }

    async fn submit_inner(&self, submission: Submission<V>) -> Result<SubmissionOutcome<V>> {
        let _membership = self.membership_gate.read().await;
        let config = self.current_config();
        if submission.value_ids.len() > config.replica.batch_size {
            return Err(QuePaxaError::ResourceLimit {
                resource: "values in one network submission",
                limit: config.replica.batch_size,
            });
        }
        if submission
            .value_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .len()
            != submission.value_ids.len()
        {
            return Err(QuePaxaError::InvalidProposal(
                "network submission contains duplicate value IDs".into(),
            ));
        }
        {
            let mut state = self.state.lock().await;
            let before = state.clone();
            state.replica.enqueue(submission.value_ids.clone())?;
            let snapshot = self.snapshot(&state);
            if let Err(error) = save_store(Arc::clone(&self.state_store), snapshot).await {
                *state = before;
                return Err(error);
            }
        }

        loop {
            let notified = self.progress.notified();
            let proposal = {
                let mut state = self.state.lock().await;
                if let Some(decision) = decision_for(&state, &submission.value_ids) {
                    return Ok(SubmissionOutcome::Committed(decision));
                }
                if let Some((slot, values)) = state.pending.iter().find(|(_, values)| {
                    submission
                        .value_ids
                        .iter()
                        .all(|value_id| values.contains(value_id))
                }) {
                    Some((*slot, values.clone()))
                } else {
                    let before = state.clone();
                    if let Some((slot, values)) = state.replica.next_proposal()? {
                        state.pending.insert(slot, values.clone());
                        if let Err(error) =
                            save_store(Arc::clone(&self.state_store), self.snapshot(&state)).await
                        {
                            *state = before;
                            return Err(error);
                        }
                        Some((slot, values))
                    } else {
                        None
                    }
                }
            };

            if let Some((slot, values)) = proposal {
                if let Some(leader) = self.wait_for_turn(slot).await? {
                    self.propose_slot(slot, values, leader).await?;
                }
            } else {
                notified.await;
            }
        }
    }

    /// Waits for this replica's scheduled hedge position, repeatedly deferring
    /// while recorder steps show that an earlier proposer is still progressing.
    async fn wait_for_turn(&self, slot: SlotIndex) -> Result<Option<ReplicaId>> {
        loop {
            let notified = self.progress.notified();
            let (leader, delay) = {
                let mut state = self.state.lock().await;
                if !state.pending.contains_key(&slot) {
                    return Ok(None);
                }
                self.hedge_for(&mut state, slot)?
            };
            if !delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = notified => continue,
                }
            }
            if self.defer_for_progress(slot, leader).await? {
                let hedge_delay = self.current_hedge_delay().await;
                let notified = self.progress.notified();
                tokio::select! {
                    _ = tokio::time::sleep(hedge_delay) => {}
                    _ = notified => {}
                }
                continue;
            }
            if self.state.lock().await.pending.contains_key(&slot) {
                return Ok(Some(leader));
            }
            return Ok(None);
        }
    }

    async fn defer_for_progress(&self, slot: SlotIndex, leader: ReplicaId) -> Result<bool> {
        let hedge_delay = self.current_hedge_delay().await;
        let config = self.current_config();
        if hedge_delay.is_zero() || config.replica_id() == leader {
            return Ok(false);
        }
        let recorders = self.current_recorders();
        let mut probes = FuturesUnordered::new();
        for recorder in recorders.iter() {
            probes.push(recorder.status(slot));
        }
        let mut highest = None;
        while let Some(result) = probes.next().await {
            if let Ok(Some(step)) = result {
                highest = Some(highest.map_or(step, |current: crate::Step| current.max(step)));
            }
        }
        let Some(highest) = highest else {
            return Ok(false);
        };
        let mut state = self.state.lock().await;
        if !state.pending.contains_key(&slot) {
            return Ok(false);
        }
        let progressed = state
            .observed_steps
            .get(&slot)
            .is_none_or(|previous| highest > *previous);
        if progressed {
            state.observed_steps.insert(slot, highest);
        }
        Ok(progressed)
    }

    /// Returns the slot's agreed round-one leader and the remaining local
    /// hedging delay based on this replica's position in the epoch schedule.
    fn hedge_for(
        &self,
        state: &mut ConsensusState<V>,
        slot: SlotIndex,
    ) -> Result<(ReplicaId, Duration)> {
        let config = self.current_config();
        let epoch = config.epoch_for(slot);
        let schedule = state.tuner.ensure_schedule(epoch)?;
        let leader = schedule.leader();
        let position = schedule
            .proposers
            .iter()
            .position(|replica| *replica == config.replica_id())
            .ok_or_else(|| {
                QuePaxaError::PolicyError(
                    "the epoch schedule does not include the local replica".into(),
                )
            })?;
        let position = u32::try_from(position).map_err(|_| {
            QuePaxaError::PolicyError("hedging schedule is too large for a duration".into())
        })?;
        let full_delay = state
            .hedge_delay
            .current()
            .checked_mul(position)
            .ok_or_else(|| QuePaxaError::PolicyError("hedging delay overflowed".into()))?;
        let armed = *state.armed_at.entry(slot).or_insert_with(Instant::now);
        Ok((leader, full_delay.saturating_sub(armed.elapsed())))
    }

    async fn propose_slot(
        &self,
        slot: SlotIndex,
        values: Vec<V>,
        leader: ReplicaId,
    ) -> Result<Decision<V>> {
        let config = self.current_config();
        let recorders = self.current_recorders();
        let mut proposer = AsyncProposerCore::new(
            config.replica_id(),
            config.lane_id(),
            config.cluster_identity().clone(),
            Arc::clone(&self.metrics),
        )?;
        let started = Instant::now();
        let decision = proposer
            .propose(slot, values, Some(leader), recorders.as_ref())
            .await?;
        self.state
            .lock()
            .await
            .hedge_delay
            .observe(started.elapsed());
        self.accept_decision(decision.clone()).await?;
        if let Err(error) = self.disseminate(decision.clone()).await {
            tracing::warn!(slot = decision.slot.get(), %error, "decision dissemination did not reach a quorum");
        }
        Ok(decision)
    }

    async fn accept_decision(&self, decision: Decision<V>) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(existing) = state.decisions.get(&decision.slot) {
            if existing.value_ids != decision.value_ids {
                return Err(QuePaxaError::ConflictingDecision {
                    slot: decision.slot,
                });
            }
            self.arm_gap_noop(&mut state)?;
            self.execute_ready(&mut state).await?;
            self.progress.notify_waiters();
            return Ok(());
        }
        let before = state.clone();
        let committed = state.replica.apply_decision(decision.clone())?;
        state.pending.remove(&decision.slot);
        state.armed_at.remove(&decision.slot);
        state.observed_steps.remove(&decision.slot);
        state.decisions.insert(decision.slot, decision);
        self.arm_gap_noop(&mut state)?;
        // Record leader-tuning statistics for the newly contiguous committed
        // prefix so auto-derived schedules stay identical on every replica.
        let committed_through = state.replica.committed_through();
        let stat_updates = state
            .decisions
            .range(..=committed_through)
            .filter(|(slot, _)| **slot > state.tuner.stats_through())
            .map(|(slot, decision)| (*slot, decision.decided_step))
            .collect::<Vec<_>>();
        for (slot, decided_step) in stat_updates {
            if let Err(error) = state.tuner.note_committed(slot, decided_step) {
                *state = before;
                return Err(error);
            }
        }
        if let Err(error) = save_store(Arc::clone(&self.state_store), self.snapshot(&state)).await {
            *state = before;
            return Err(error);
        }

        self.execute_decisions(&mut state, committed).await?;
        self.progress.notify_waiters();
        Ok(())
    }

    fn arm_gap_noop(&self, state: &mut ConsensusState<V>) -> Result<bool> {
        let Some(noop) = self.noop_value.clone() else {
            return Ok(false);
        };
        if !state.pending.is_empty() {
            return Ok(false);
        }
        let gap = state
            .replica
            .committed_through()
            .checked_next()
            .ok_or(QuePaxaError::SlotOverflow)?;
        let later_decision_exists = state
            .decisions
            .range((std::ops::Bound::Excluded(gap), std::ops::Bound::Unbounded))
            .next()
            .is_some();
        let gap_is_open = !state.decisions.contains_key(&gap)
            && state
                .replica
                .slot(gap)
                .is_none_or(|entry| entry.decision.is_none() && entry.proposed.is_empty());
        if !later_decision_exists || !gap_is_open {
            return Ok(false);
        }
        state.replica.note_proposed(gap, vec![noop.clone()])?;
        state.pending.insert(gap, vec![noop]);
        state.armed_at.insert(gap, Instant::now());
        Ok(true)
    }

    async fn execute_ready(&self, state: &mut ConsensusState<V>) -> Result<()> {
        let committed_through = state.replica.committed_through();
        let decisions = state
            .decisions
            .range(..=committed_through)
            .filter(|(slot, _)| !state.executed.contains(slot))
            .map(|(_, decision)| decision.clone())
            .collect();
        self.execute_decisions(state, decisions).await
    }

    async fn execute_decisions(
        &self,
        state: &mut ConsensusState<V>,
        decisions: Vec<Decision<V>>,
    ) -> Result<()> {
        for committed_decision in decisions {
            if state.executed.contains(&committed_decision.slot) {
                continue;
            }
            let machine = Arc::clone(&self.state_machine);
            let execution = committed_decision.clone();
            tokio::task::spawn_blocking(move || {
                machine
                    .lock()
                    .map_err(|_| {
                        QuePaxaError::StorageError("state machine lock was poisoned".into())
                    })?
                    .execute(&execution)
            })
            .await
            .map_err(|error| {
                QuePaxaError::StorageError(format!("state machine task failed: {error}"))
            })??;
            state.executed.insert(committed_decision.slot);
            save_store(Arc::clone(&self.state_store), self.snapshot(state)).await?;
        }
        Ok(())
    }

    async fn disseminate(&self, decision: Decision<V>) -> Result<()> {
        let config = self.current_config();
        let recorders = self.current_recorders();
        let mut pending = FuturesUnordered::new();
        for recorder in recorders.iter() {
            let recorder_id = recorder.id();
            let announcement = decision.clone();
            pending.push(async move {
                (
                    recorder_id,
                    recorder.inform_decisions(vec![announcement]).await,
                )
            });
        }
        let mut acknowledgements = BTreeSet::new();
        while let Some((recorder, result)) = pending.next().await {
            if result.is_ok() {
                acknowledgements.insert(recorder);
                if config
                    .cluster_identity()
                    .has_quorum(acknowledgements.iter())
                {
                    self.metrics.quorum_cancelled(pending.len());
                    return Ok(());
                }
            }
        }
        Err(QuePaxaError::QuorumNotReached {
            needed: config.quorum_size(),
            received: acknowledgements.len(),
        })
    }

    async fn disseminate_all(&self, decisions: Vec<Decision<V>>) -> Result<()> {
        if decisions.is_empty() {
            return Ok(());
        }
        let recorders = self.current_recorders();
        let mut pending = FuturesUnordered::new();
        for recorder in recorders.iter() {
            pending.push(recorder.inform_decisions(decisions.clone()));
        }
        let needed = recorders.len();
        let mut acknowledgements = 0;
        while let Some(result) = pending.next().await {
            if result.is_ok() {
                acknowledgements += 1;
            }
        }
        if acknowledgements == needed {
            Ok(())
        } else {
            Err(QuePaxaError::QuorumNotReached {
                needed,
                received: acknowledgements,
            })
        }
    }

    fn snapshot(&self, state: &ConsensusState<V>) -> RuntimeSnapshot<V> {
        let config = self.current_config();
        RuntimeSnapshot {
            cluster: config.cluster_identity().clone(),
            protocol: config.protocol_identity(),
            replica: state.replica.snapshot(),
            decisions: state.decisions.clone(),
            schedules: state.tuner.schedules().clone(),
            pending: state
                .pending
                .iter()
                .map(|(slot, values)| {
                    (
                        *slot,
                        PendingProposalSnapshot {
                            value_ids: values.clone(),
                        },
                    )
                })
                .collect(),
            executed: state.executed.clone(),
            notified: state.executed.clone(),
            announced_to: BTreeMap::new(),
            epoch_stats: state.tuner.stats().clone(),
            stats_through: state.tuner.stats_through(),
        }
    }
}

fn decision_for<V>(state: &ConsensusState<V>, value_ids: &[V]) -> Option<Decision<V>>
where
    V: Clone + Ord,
{
    state
        .decisions
        .values()
        .find(|decision| {
            value_ids
                .iter()
                .all(|value_id| decision.value_ids.contains(value_id))
        })
        .cloned()
}

fn validate_state<V>(config: &ReplicaRuntimeConfig, state: &ConsensusState<V>) -> Result<()>
where
    V: Clone + Ord,
{
    let decision_slots = state.decisions.keys().copied().collect::<BTreeSet<_>>();
    if !state.executed.is_subset(&decision_slots) {
        return Err(QuePaxaError::InvalidProposal(
            "network runtime snapshot executes a slot without a decision".into(),
        ));
    }
    for (slot, decision) in &state.decisions {
        if *slot != decision.slot || !config.cluster_identity().contains(decision.proposer) {
            return Err(QuePaxaError::InvalidProposal(
                "network runtime snapshot contains an invalid decision".into(),
            ));
        }
        if state
            .replica
            .slot(*slot)
            .and_then(|entry| entry.decision.as_ref())
            .is_none_or(|stored| stored.value_ids != decision.value_ids)
        {
            return Err(QuePaxaError::InvalidProposal(
                "network runtime decision does not match the replica log".into(),
            ));
        }
    }
    for (slot, values) in &state.pending {
        if !state
            .replica
            .slot(*slot)
            .is_some_and(|entry| entry.decision.is_none() && entry.proposed == *values)
        {
            return Err(QuePaxaError::InvalidProposal(
                "network runtime pending proposal does not match the replica log".into(),
            ));
        }
    }
    Ok(())
}

fn validate_async_recorders<V, C>(cluster: &crate::ClusterIdentity, recorders: &[C]) -> Result<()>
where
    C: AsyncRecorderClient<V>,
{
    let expected = cluster.members().iter().copied().collect::<BTreeSet<_>>();
    let actual = recorders
        .iter()
        .map(AsyncRecorderClient::id)
        .collect::<BTreeSet<_>>();
    if actual.len() != recorders.len() {
        return Err(QuePaxaError::InvalidReconfiguration(
            "replacement async recorder clients contain duplicate identities".into(),
        ));
    }
    if actual != expected {
        return Err(QuePaxaError::ConfigurationMismatch);
    }
    Ok(())
}

impl<V, C, S, E> SubmissionHandler<V> for NetworkConsensusHandler<V, C, S, E>
where
    V: Clone + Ord + Send + Sync + 'static,
    C: AsyncRecorderClient<V> + 'static,
    S: RuntimeStateStore<V> + Send + 'static,
    E: StateMachine<V> + Send + 'static,
{
    fn submit(&self, submission: Submission<V>) -> BoxSubmissionFuture<'_, V> {
        Box::pin(self.submit_inner(submission))
    }

    fn receive_decisions(&self, decisions: Vec<Decision<V>>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let _membership = self.membership_gate.read().await;
            for decision in decisions {
                self.accept_decision(decision).await?;
            }
            self.recover_inner().await?;
            Ok(())
        })
    }
}

async fn load_store<V, S>(store: Arc<Mutex<S>>) -> Result<Option<RuntimeSnapshot<V>>>
where
    V: Send + 'static,
    S: RuntimeStateStore<V> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        store
            .lock()
            .map_err(|_| QuePaxaError::StorageError("runtime store lock was poisoned".into()))?
            .load()
    })
    .await
    .map_err(|error| QuePaxaError::StorageError(format!("runtime load task failed: {error}")))?
}

async fn save_store<V, S>(store: Arc<Mutex<S>>, snapshot: RuntimeSnapshot<V>) -> Result<()>
where
    V: Send + 'static,
    S: RuntimeStateStore<V> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        store
            .lock()
            .map_err(|_| QuePaxaError::StorageError("runtime store lock was poisoned".into()))?
            .save(&snapshot)
    })
    .await
    .map_err(|error| QuePaxaError::StorageError(format!("runtime save task failed: {error}")))?
}
