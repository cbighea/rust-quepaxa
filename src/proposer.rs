use crate::error::{QuePaxaError, Result};
use crate::types::{
    ClusterIdentity, Decision, LaneId, MembershipChange, Priority, Proposal, ProposalKey,
    RecordReply, RecordRequest, ReplicaId, SlotIndex, Step, max_proposal_by_key,
};
use std::collections::BTreeSet;
use std::fmt;
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::Read;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// A transport endpoint for one authenticated recorder.
///
/// Implementations must bind the endpoint to one configured recorder identity
/// (for example with mTLS) and must not accept requests from untrusted peers.
pub trait RecorderClient<V>: Send {
    fn record(&mut self, request: RecordRequest<V>) -> Result<RecordReply<V>>;

    fn inform_decisions(&mut self, _decisions: &[Decision<V>]) -> Result<()> {
        Ok(())
    }

    /// Read-only probe reporting the recorder's current logical step for a
    /// slot. Hedged proposers use it as evidence that an earlier proposer is
    /// still driving the slot. `Ok(None)` means the endpoint cannot tell, in
    /// which case hedging falls back to activating on schedule.
    fn status(&mut self, _slot: SlotIndex) -> Result<Option<Step>> {
        Ok(None)
    }

    /// Installs a consensus-anchored membership transition. Implementations
    /// must persist the new epoch before acknowledging it.
    fn install_membership(&mut self, _change: MembershipChange<V>) -> Result<()> {
        Err(QuePaxaError::InvalidReconfiguration(
            "recorder client does not support membership changes".into(),
        ))
    }
}

/// Samples a private phase-zero priority below the reserved leader priority.
pub trait PrioritySource {
    fn next_below(&mut self, exclusive_upper_bound: Priority) -> Result<Priority>;
}

/// OS-backed cryptographic priority source used by default.
#[derive(Debug)]
pub struct OsRandom {
    #[cfg(unix)]
    source: File,
}

impl OsRandom {
    pub fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                source: File::open("/dev/urandom").map_err(|error| {
                    QuePaxaError::TransportError(format!("OS randomness unavailable: {error}"))
                })?,
            })
        }
        #[cfg(not(unix))]
        {
            Err(QuePaxaError::TransportError(
                "no OS randomness backend is configured for this platform".into(),
            ))
        }
    }
}

impl PrioritySource for OsRandom {
    fn next_below(&mut self, exclusive_upper_bound: Priority) -> Result<Priority> {
        let upper = exclusive_upper_bound.get();
        if upper <= 1 {
            return Err(QuePaxaError::InvalidProposal(
                "priority range must reserve zero and the leader priority".into(),
            ));
        }

        // Draw uniformly from 1..upper. Rejection sampling avoids modulo bias.
        let range = upper - 1;
        let cutoff = u64::MAX - (u64::MAX % range);
        loop {
            let value = self.next_u64()?;
            if value < cutoff {
                return Ok(Priority::new(value % range + 1));
            }
        }
    }
}

impl OsRandom {
    fn next_u64(&mut self) -> Result<u64> {
        #[cfg(unix)]
        {
            let mut bytes = [0_u8; 8];
            self.source.read_exact(&mut bytes).map_err(|error| {
                QuePaxaError::TransportError(format!("OS randomness unavailable: {error}"))
            })?;
            Ok(u64::from_ne_bytes(bytes))
        }
        #[cfg(not(unix))]
        {
            Err(QuePaxaError::TransportError(
                "no OS randomness backend is configured for this platform".into(),
            ))
        }
    }
}

/// Deterministic priority source intended only for reproducible tests.
#[derive(Debug, Clone)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    pub fn new_for_stream(seed: u64, replica_id: ReplicaId, lane_id: LaneId) -> Self {
        let stream_seed = seed
            ^ replica_id.get().wrapping_mul(0x9e37_79b9_7f4a_7c15)
            ^ lane_id.get().wrapping_mul(0xbf58_476d_1ce4_e5b9);
        Self::new(mix_seed(stream_seed))
    }
}

impl PrioritySource for XorShift64 {
    fn next_below(&mut self, exclusive_upper_bound: Priority) -> Result<Priority> {
        let upper = exclusive_upper_bound.get();
        if upper <= 1 {
            return Err(QuePaxaError::InvalidProposal(
                "priority range must reserve zero and the leader priority".into(),
            ));
        }
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        Ok(Priority::new(x % (upper - 1) + 1))
    }
}

/// A shareable, identity-bound recorder endpoint. At most one outstanding RPC
/// is allowed per endpoint so an unresponsive peer cannot create unbounded
/// detached worker threads.
pub struct RecorderHandle<C> {
    id: ReplicaId,
    client: Arc<Mutex<C>>,
    in_flight: Arc<AtomicBool>,
}

impl<C> Clone for RecorderHandle<C> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            client: Arc::clone(&self.client),
            in_flight: Arc::clone(&self.in_flight),
        }
    }
}

impl<C> fmt::Debug for RecorderHandle<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecorderHandle")
            .field("id", &self.id)
            .finish()
    }
}

impl<C> RecorderHandle<C> {
    pub fn new(id: ReplicaId, client: C) -> Self {
        Self {
            id,
            client: Arc::new(Mutex::new(client)),
            in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn id(&self) -> ReplicaId {
        self.id
    }

    pub fn with_client<T>(&self, operation: impl FnOnce(&mut C) -> T) -> Result<T> {
        let mut client = self.client.lock().map_err(|_| {
            QuePaxaError::TransportError("recorder client lock was poisoned".into())
        })?;
        Ok(operation(&mut client))
    }

    /// Reports whether no RPC is currently outstanding on this endpoint. Used
    /// to skip busy endpoints for optional read-only probes.
    pub fn is_idle(&self) -> bool {
        !self.in_flight.load(Ordering::Acquire)
    }

    fn try_begin(&self) -> bool {
        self.in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn release(&self) {
        self.in_flight.store(false, Ordering::Release);
    }
}

impl<C: Send + 'static> RecorderHandle<C> {
    /// Runs one auxiliary synchronous RPC without allowing a wedged client to
    /// block the runtime thread. A timed-out worker retains the endpoint's
    /// in-flight guard until the underlying client actually returns.
    pub(crate) fn call_with_timeout<T, F>(&self, timeout: Duration, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut C) -> Result<T> + Send + 'static,
    {
        if !self.try_begin() {
            return Err(QuePaxaError::TransportError(format!(
                "recorder {} already has an RPC in flight",
                self.id
            )));
        }
        let client = Arc::clone(&self.client);
        let in_flight = Arc::clone(&self.in_flight);
        let (sender, receiver) = mpsc::channel();
        if thread::Builder::new()
            .name(format!("quepaxa-recorder-aux-{}", self.id))
            .spawn(move || {
                let guard = InFlightGuard(in_flight);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let mut client = client.lock().map_err(|_| {
                        QuePaxaError::TransportError("recorder client lock was poisoned".into())
                    })?;
                    operation(&mut client)
                }))
                .unwrap_or_else(|_| {
                    Err(QuePaxaError::TransportError(
                        "recorder client panicked while handling an RPC".into(),
                    ))
                });
                drop(guard);
                let _ = sender.send(result);
            })
            .is_err()
        {
            self.release();
            return Err(QuePaxaError::TransportError(
                "could not start recorder RPC worker".into(),
            ));
        }
        receiver
            .recv_timeout(timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => QuePaxaError::TransportError(
                    "recorder RPC exceeded its transport timeout".into(),
                ),
                mpsc::RecvTimeoutError::Disconnected => QuePaxaError::TransportError(
                    "recorder RPC worker exited without a response".into(),
                ),
            })?
    }
}

struct InFlightGuard(Arc<AtomicBool>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Debug, Clone)]
pub struct ProposerConfig {
    /// A bounded deployment policy, not a consensus timeout. The default is
    /// 256 complete four-phase rounds; callers should checkpoint/reconfigure
    /// rather than accept arbitrarily large logical clocks from the network.
    pub max_steps: u64,
    /// An optional deployment quorum. When unset, the proposer uses a simple
    /// majority for standalone simulations and backward-compatible callers.
    pub quorum: Option<usize>,
    /// When set, replies must come from recorders with this exact fixed
    /// membership and fault budget.
    pub cluster: Option<ClusterIdentity>,
    /// Overall transport wait bound for one protocol phase. This does not
    /// elect a leader or advance a round; it only prevents a wedged endpoint
    /// from blocking the synchronous adapter forever.
    pub rpc_timeout: Duration,
}

impl Default for ProposerConfig {
    fn default() -> Self {
        Self {
            max_steps: 1027,
            quorum: None,
            cluster: None,
            rpc_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub struct ProposerCore<R = OsRandom> {
    replica_id: ReplicaId,
    lane_id: LaneId,
    rng: R,
    config: ProposerConfig,
}

impl ProposerCore<OsRandom> {
    pub fn new(replica_id: ReplicaId, lane_id: LaneId) -> Result<Self> {
        Ok(Self::with_rng(replica_id, lane_id, OsRandom::new()?))
    }
}

impl<R> ProposerCore<R> {
    pub fn with_rng(replica_id: ReplicaId, lane_id: LaneId, rng: R) -> Self {
        Self {
            replica_id,
            lane_id,
            rng,
            config: ProposerConfig::default(),
        }
    }

    pub fn with_config(mut self, config: ProposerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_quorum(mut self, quorum: usize) -> Self {
        self.config.quorum = Some(quorum);
        self
    }

    pub fn with_cluster(mut self, cluster: ClusterIdentity) -> Self {
        self.config.quorum = Some(cluster.quorum_size());
        self.config.cluster = Some(cluster);
        self
    }

    pub fn with_rpc_timeout(mut self, timeout: Duration) -> Self {
        self.config.rpc_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    pub fn configured_quorum(&self) -> Option<usize> {
        self.config.quorum
    }

    pub(crate) fn configure_cluster(&mut self, cluster: ClusterIdentity) {
        self.config.quorum = Some(cluster.quorum_size());
        self.config.cluster = Some(cluster);
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    pub fn lane_id(&self) -> LaneId {
        self.lane_id
    }
}

impl<R: PrioritySource> ProposerCore<R> {
    /// Runs Algorithm 4 for one slot. `leader` must be the agreed round-one
    /// leader, or `None` for a leaderless first round.
    pub fn propose<V, C>(
        &mut self,
        slot: SlotIndex,
        value_ids: Vec<V>,
        leader: Option<ReplicaId>,
        recorders: &[RecorderHandle<C>],
        known_decisions: &[Decision<V>],
    ) -> Result<Decision<V>>
    where
        V: Clone + Eq + Send + 'static,
        C: RecorderClient<V> + 'static,
    {
        let quorum = validate_recorders(
            recorders,
            self.replica_id,
            self.config.quorum,
            self.config.cluster.as_ref(),
        )?;
        if let Some(leader) = leader {
            if !recorders.iter().any(|recorder| recorder.id() == leader) {
                return Err(QuePaxaError::UnknownReplica(leader));
            }
        }
        for decision in known_decisions {
            if !recorders
                .iter()
                .any(|recorder| recorder.id() == decision.proposer)
            {
                return Err(QuePaxaError::InvalidSender(decision.proposer));
            }
        }

        let mut step = Step::ROUND_ONE_PHASE_ZERO;
        let mut proposal = Proposal::new(
            ProposalKey::new(Priority::MAX, self.replica_id, self.lane_id),
            value_ids,
        )?;
        let mut send_known_decisions = true;

        while step.get() <= self.config.max_steps {
            let mut requests = Vec::with_capacity(recorders.len());
            for recorder in recorders {
                let proposal_for_recorder = if step.phase() == 0
                    && (step > Step::ROUND_ONE_PHASE_ZERO || leader != Some(self.replica_id))
                {
                    proposal.with_priority(self.rng.next_below(Priority::MAX)?)
                } else {
                    proposal.clone()
                };
                requests.push((
                    recorder.clone(),
                    RecordRequest {
                        sender: self.replica_id,
                        slot,
                        round_one_leader: leader,
                        step,
                        proposal: proposal_for_recorder,
                        known_decisions: if send_known_decisions {
                            known_decisions.to_vec()
                        } else {
                            Vec::new()
                        },
                    },
                ));
            }

            let replies = collect_quorum(
                requests,
                quorum,
                self.config.cluster.as_ref(),
                self.config.rpc_timeout,
            )?;
            send_known_decisions = false;

            // A recorder that already stores this slot's decision reports it
            // in its reply; adopt it instead of re-deriving it round by round.
            if let Some(decision) = replies
                .iter()
                .find_map(|reply| reply.decision.as_ref())
                .cloned()
            {
                if decision.slot != slot || decision.value_ids.is_empty() {
                    return Err(QuePaxaError::InvalidProposal(
                        "recorder reported a decision for a different slot".into(),
                    ));
                }
                return Ok(decision);
            }

            if replies.iter().all(|reply| reply.summary.step == step) {
                match step.phase() {
                    0 => {
                        if let Some(winner) = fast_path_winner(step, &replies, leader) {
                            return Decision::new(
                                slot,
                                winner.value_ids,
                                winner.key.proposer_id,
                                step,
                            );
                        }

                        proposal = max_proposal_by_key(
                            replies
                                .iter()
                                .filter_map(|reply| reply.summary.first.clone()),
                        )
                        .ok_or(QuePaxaError::MissingProposal)?;
                    }
                    1 => {}
                    2 => {
                        if let Some(max_prior) = max_proposal_by_key(
                            replies
                                .iter()
                                .filter_map(|reply| reply.summary.prior_aggregate.clone()),
                        ) && proposal == max_prior
                        {
                            return Decision::new(
                                slot,
                                proposal.value_ids,
                                proposal.key.proposer_id,
                                step,
                            );
                        }
                    }
                    3 => {
                        if let Some(max_prior) = max_proposal_by_key(
                            replies
                                .iter()
                                .filter_map(|reply| reply.summary.prior_aggregate.clone()),
                        ) {
                            proposal = max_prior;
                        }
                    }
                    _ => unreachable!("step phase is modulo 4"),
                }

                step = step.checked_next().ok_or(QuePaxaError::StepOverflow)?;
            } else if let Some(reply) = replies.iter().max_by_key(|reply| reply.summary.step)
                && reply.summary.step > step
            {
                step = reply.summary.step;
                proposal = reply
                    .summary
                    .first
                    .clone()
                    .ok_or(QuePaxaError::MissingProposal)?;
            }
        }

        Err(QuePaxaError::StepLimitExceeded {
            limit: self.config.max_steps,
        })
    }
}

fn collect_quorum<V, C>(
    requests: Vec<(RecorderHandle<C>, RecordRequest<V>)>,
    quorum: usize,
    expected_cluster: Option<&ClusterIdentity>,
    rpc_timeout: Duration,
) -> Result<Vec<RecordReply<V>>>
where
    V: Send + 'static,
    C: RecorderClient<V> + 'static,
{
    let (sender, receiver) = mpsc::channel();
    let mut launched = 0;

    for (recorder, request) in requests {
        if !recorder.try_begin() {
            continue;
        }
        let expected = recorder.id();
        let client = Arc::clone(&recorder.client);
        let in_flight = Arc::clone(&recorder.in_flight);
        let reply_sender = sender.clone();
        let spawned = thread::Builder::new()
            .name(format!("quepaxa-recorder-{expected}"))
            .spawn(move || {
                let guard = InFlightGuard(in_flight);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    let mut client = client.lock().map_err(|_| {
                        QuePaxaError::TransportError("recorder client lock was poisoned".into())
                    })?;
                    client.record(request)
                }))
                .unwrap_or_else(|_| {
                    Err(QuePaxaError::TransportError(
                        "recorder client panicked while handling an RPC".into(),
                    ))
                });
                drop(guard);
                let _ = reply_sender.send((expected, result));
            });
        if spawned.is_err() {
            recorder.release();
            continue;
        }
        launched += 1;
    }
    drop(sender);

    if launched < quorum {
        return Err(QuePaxaError::QuorumNotReached {
            needed: quorum,
            received: launched,
        });
    }

    let mut replies = Vec::with_capacity(quorum);
    let mut seen = BTreeSet::new();
    let mut pending = launched;
    let deadline = Instant::now().checked_add(rpc_timeout).ok_or_else(|| {
        QuePaxaError::TransportError("recorder RPC timeout exceeds the clock range".into())
    })?;
    while !quorum_reached(&replies, quorum, expected_cluster) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(QuePaxaError::QuorumNotReached {
                needed: quorum,
                received: replies.len(),
            });
        }
        let (expected, result) = match receiver.recv_timeout(remaining) {
            Ok(reply) => reply,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(QuePaxaError::QuorumNotReached {
                    needed: quorum,
                    received: replies.len(),
                });
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(QuePaxaError::TransportError(
                    "all recorder workers exited before replying".into(),
                ));
            }
        };
        pending -= 1;
        match result {
            Ok(reply) => {
                if reply.recorder != expected {
                    return Err(QuePaxaError::InvalidRecorderReply {
                        expected,
                        received: reply.recorder,
                    });
                }
                if expected_cluster.is_some_and(|cluster| reply.cluster != *cluster) {
                    return Err(QuePaxaError::ConfigurationMismatch);
                }
                if !seen.insert(reply.recorder) {
                    return Err(QuePaxaError::DuplicateRecorderReply(reply.recorder));
                }
                replies.push(reply);
            }
            // Conflicting or pruned slots signal that this proposer must not
            // continue: the first is a safety alarm, the second means the slot
            // was finalized cluster-wide and requires state transfer.
            Err(error @ QuePaxaError::ConflictingDecision { .. }) => return Err(error),
            Err(error @ QuePaxaError::SlotPruned { .. }) => return Err(error),
            Err(error @ QuePaxaError::ScheduleMismatch { .. }) => return Err(error),
            Err(error @ QuePaxaError::ConfigurationMismatch) => return Err(error),
            // Any other per-recorder failure (resource limits, storage,
            // transport, local misconfiguration of one peer) is tolerated as
            // long as the remaining recorders can still form a quorum.
            Err(_) if replies.len() + pending < quorum => {
                return Err(QuePaxaError::QuorumNotReached {
                    needed: quorum,
                    received: replies.len(),
                });
            }
            Err(_) => {}
        }
    }
    Ok(replies)
}

fn validate_recorders<C>(
    recorders: &[RecorderHandle<C>],
    proposer: ReplicaId,
    configured_quorum: Option<usize>,
    expected_cluster: Option<&ClusterIdentity>,
) -> Result<usize> {
    let majority = quorum_size(recorders.len())?;
    let mut members = BTreeSet::new();
    for recorder in recorders {
        if !members.insert(recorder.id()) {
            return Err(QuePaxaError::DuplicateReplica(recorder.id()));
        }
    }
    if !members.contains(&proposer) {
        return Err(QuePaxaError::UnknownReplica(proposer));
    }
    if let Some(cluster) = expected_cluster {
        let configured_members = cluster.members().iter().copied().collect::<BTreeSet<_>>();
        if members != configured_members {
            return Err(QuePaxaError::ConfigurationMismatch);
        }
        if configured_quorum != Some(cluster.quorum_size()) {
            return Err(QuePaxaError::InvalidQuorum {
                replicas: recorders.len(),
                quorum: configured_quorum.unwrap_or(majority),
            });
        }
    }
    match configured_quorum {
        Some(quorum) if quorum < majority || quorum > recorders.len() => {
            Err(QuePaxaError::InvalidQuorum {
                replicas: recorders.len(),
                quorum,
            })
        }
        Some(quorum) => Ok(quorum),
        None => Ok(majority),
    }
}

fn quorum_reached<V>(
    replies: &[RecordReply<V>],
    quorum: usize,
    expected_cluster: Option<&ClusterIdentity>,
) -> bool {
    expected_cluster.map_or_else(
        || replies.len() >= quorum,
        |cluster| cluster.has_quorum(replies.iter().map(|reply| &reply.recorder)),
    )
}

fn fast_path_winner<V: Clone>(
    step: Step,
    replies: &[RecordReply<V>],
    leader: Option<ReplicaId>,
) -> Option<Proposal<V>> {
    if step != Step::ROUND_ONE_PHASE_ZERO {
        return None;
    }

    let winner = replies.first()?.summary.first.as_ref()?;
    if !winner.key.priority.is_leader_priority() {
        return None;
    }
    // The reserved priority is only meaningful when it comes from the agreed
    // round-one leader. Anything else indicates schedule disagreement, so the
    // fast path is refused and the round proceeds through the ordinary phases.
    if leader != Some(winner.key.proposer_id) {
        return None;
    }

    replies
        .iter()
        .all(|reply| {
            reply
                .summary
                .first
                .as_ref()
                .is_some_and(|proposal| proposal.key == winner.key)
        })
        .then(|| winner.clone())
}

fn mix_seed(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub fn quorum_size(replica_count: usize) -> Result<usize> {
    if replica_count == 0 {
        return Err(QuePaxaError::EmptyCluster);
    }
    Ok(replica_count / 2 + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auxiliary_call_releases_endpoint_before_returning() {
        let recorder = RecorderHandle::new(ReplicaId::new(1), ());

        recorder
            .call_with_timeout(Duration::from_secs(1), |_| Ok(()))
            .unwrap();

        assert!(recorder.is_idle());
    }
}
