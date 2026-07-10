use crate::error::{QuePaxaError, Result};
use crate::network::metrics::NetworkMetrics;
use crate::proposer::{OsRandom, PrioritySource};
use crate::types::{
    ClusterIdentity, Decision, LaneId, MembershipChange, Priority, Proposal, ProposalKey,
    RecordReply, RecordRequest, ReplicaId, SlotIndex, Step, max_proposal_by_key,
};
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeSet;
use std::sync::Arc;

pub trait AsyncRecorderClient<V>: Send + Sync {
    fn id(&self) -> ReplicaId;

    fn record(&self, request: RecordRequest<V>) -> BoxFuture<'_, Result<RecordReply<V>>>;

    fn inform_decisions(&self, decisions: Vec<Decision<V>>) -> BoxFuture<'_, Result<()>>;

    fn status(&self, _slot: SlotIndex) -> BoxFuture<'_, Result<Option<Step>>> {
        Box::pin(async { Ok(None) })
    }

    fn install_membership(&self, _change: MembershipChange<V>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            Err(QuePaxaError::InvalidReconfiguration(
                "async recorder client does not support membership changes".into(),
            ))
        })
    }
}

pub struct AsyncProposerCore<R = OsRandom> {
    replica_id: ReplicaId,
    lane_id: LaneId,
    rng: R,
    cluster: ClusterIdentity,
    max_steps: u64,
    metrics: Arc<NetworkMetrics>,
}

impl AsyncProposerCore<OsRandom> {
    pub fn new(
        replica_id: ReplicaId,
        lane_id: LaneId,
        cluster: ClusterIdentity,
        metrics: Arc<NetworkMetrics>,
    ) -> Result<Self> {
        Ok(Self::with_rng(
            replica_id,
            lane_id,
            cluster,
            OsRandom::new()?,
            metrics,
        ))
    }
}

impl<R> AsyncProposerCore<R> {
    pub fn with_rng(
        replica_id: ReplicaId,
        lane_id: LaneId,
        cluster: ClusterIdentity,
        rng: R,
        metrics: Arc<NetworkMetrics>,
    ) -> Self {
        Self {
            replica_id,
            lane_id,
            rng,
            cluster,
            max_steps: 1027,
            metrics,
        }
    }

    pub fn with_max_steps(mut self, max_steps: u64) -> Self {
        self.max_steps = max_steps;
        self
    }
}

impl<R: PrioritySource> AsyncProposerCore<R> {
    pub async fn propose<V, C>(
        &mut self,
        slot: SlotIndex,
        value_ids: Vec<V>,
        leader: Option<ReplicaId>,
        recorders: &[C],
    ) -> Result<Decision<V>>
    where
        V: Clone + Eq + Send + Sync + 'static,
        C: AsyncRecorderClient<V>,
    {
        validate_recorders(recorders, self.replica_id, &self.cluster)?;
        if let Some(leader) = leader
            && !self.cluster.contains(leader)
        {
            return Err(QuePaxaError::UnknownReplica(leader));
        }

        let mut step = Step::ROUND_ONE_PHASE_ZERO;
        let mut proposal = Proposal::new(
            ProposalKey::new(Priority::MAX, self.replica_id, self.lane_id),
            value_ids,
        )?;

        while step.get() <= self.max_steps {
            let mut requests = Vec::with_capacity(recorders.len());
            for _ in recorders {
                let proposal_for_recorder = if step.phase() == 0
                    && (step > Step::ROUND_ONE_PHASE_ZERO || leader != Some(self.replica_id))
                {
                    proposal.with_priority(self.rng.next_below(Priority::MAX)?)
                } else {
                    proposal.clone()
                };
                requests.push(RecordRequest {
                    sender: self.replica_id,
                    slot,
                    round_one_leader: leader,
                    step,
                    proposal: proposal_for_recorder,
                    known_decisions: Vec::new(),
                });
            }

            let replies = collect_quorum(
                recorders,
                requests,
                self.cluster.quorum_size(),
                &self.cluster,
                &self.metrics,
            )
            .await?;

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
            limit: self.max_steps,
        })
    }
}

async fn collect_quorum<V, C>(
    recorders: &[C],
    requests: Vec<RecordRequest<V>>,
    quorum: usize,
    expected_cluster: &ClusterIdentity,
    metrics: &NetworkMetrics,
) -> Result<Vec<RecordReply<V>>>
where
    V: Send + 'static,
    C: AsyncRecorderClient<V>,
{
    let mut pending = FuturesUnordered::new();
    for (recorder, request) in recorders.iter().zip(requests) {
        pending.push(async move { (recorder.id(), recorder.record(request).await) });
    }

    let mut replies = Vec::with_capacity(quorum);
    let mut seen = BTreeSet::new();
    while let Some((expected, result)) = pending.next().await {
        match result {
            Ok(reply) => {
                if reply.recorder != expected {
                    return Err(QuePaxaError::InvalidRecorderReply {
                        expected,
                        received: reply.recorder,
                    });
                }
                if reply.cluster != *expected_cluster {
                    return Err(QuePaxaError::ConfigurationMismatch);
                }
                if !seen.insert(reply.recorder) {
                    return Err(QuePaxaError::DuplicateRecorderReply(reply.recorder));
                }
                replies.push(reply);
                if expected_cluster.has_quorum(replies.iter().map(|reply| &reply.recorder)) {
                    metrics.quorum_cancelled(pending.len());
                    return Ok(replies);
                }
            }
            Err(error @ QuePaxaError::ConfigurationMismatch) => return Err(error),
            // Safety signals: a conflicting decision or a pruned slot means
            // this proposer must stop rather than retry around the recorder.
            Err(error @ QuePaxaError::ConflictingDecision { .. }) => return Err(error),
            Err(error @ QuePaxaError::SlotPruned { .. }) => return Err(error),
            Err(error @ QuePaxaError::ScheduleMismatch { .. }) => return Err(error),
            Err(_) => {}
        }

        if replies.len() + pending.len() < quorum {
            return Err(QuePaxaError::QuorumNotReached {
                needed: quorum,
                received: replies.len(),
            });
        }
    }

    Err(QuePaxaError::QuorumNotReached {
        needed: quorum,
        received: replies.len(),
    })
}

fn validate_recorders<V, C>(
    recorders: &[C],
    proposer: ReplicaId,
    cluster: &ClusterIdentity,
) -> Result<()>
where
    C: AsyncRecorderClient<V>,
{
    if !cluster.contains(proposer) {
        return Err(QuePaxaError::UnknownReplica(proposer));
    }
    let actual = recorders
        .iter()
        .map(AsyncRecorderClient::id)
        .collect::<BTreeSet<_>>();
    let expected = cluster.members().iter().copied().collect::<BTreeSet<_>>();
    if actual.len() != recorders.len() {
        return Err(QuePaxaError::InvalidProposal(
            "async recorder clients contain duplicate identities".into(),
        ));
    }
    if actual != expected {
        return Err(QuePaxaError::ConfigurationMismatch);
    }
    Ok(())
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
