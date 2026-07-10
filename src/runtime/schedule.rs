use super::*;

/// Safety-relevant schedule configuration persisted with runtime state.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolIdentity {
    pub epoch_size: u64,
    pub auto_schedules: bool,
}

/// Local, non-safety-critical tuning parameters for the base hedging delay.
/// Successful consensus wall-clock durations are retained in a rolling
/// window, and the selected percentile becomes the delay for newly armed
/// slots. Leader order still comes from an agreed [`EpochSchedule`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveHedgingConfig {
    min_delay: Duration,
    max_delay: Duration,
    sample_window: usize,
    percentile: u8,
}

impl AdaptiveHedgingConfig {
    pub fn new(min_delay: Duration, max_delay: Duration) -> Result<Self> {
        if min_delay > max_delay {
            return Err(QuePaxaError::PolicyError(
                "adaptive hedge minimum exceeds its maximum".into(),
            ));
        }
        Ok(Self {
            min_delay,
            max_delay,
            sample_window: 64,
            percentile: 90,
        })
    }

    pub fn with_window(mut self, sample_window: usize) -> Result<Self> {
        if sample_window == 0 {
            return Err(QuePaxaError::PolicyError(
                "adaptive hedge sample window must be non-zero".into(),
            ));
        }
        self.sample_window = sample_window;
        Ok(self)
    }

    pub fn with_percentile(mut self, percentile: u8) -> Result<Self> {
        if !(1..=100).contains(&percentile) {
            return Err(QuePaxaError::PolicyError(
                "adaptive hedge percentile must be between 1 and 100".into(),
            ));
        }
        self.percentile = percentile;
        Ok(self)
    }

    pub fn min_delay(&self) -> Duration {
        self.min_delay
    }

    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    pub fn sample_window(&self) -> usize {
        self.sample_window
    }

    pub fn percentile(&self) -> u8 {
        self.percentile
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HedgeDelayTuner {
    current: Duration,
    config: Option<AdaptiveHedgingConfig>,
    samples: VecDeque<Duration>,
}

impl HedgeDelayTuner {
    pub(crate) fn new(base_delay: Duration, config: Option<AdaptiveHedgingConfig>) -> Self {
        let current = config.as_ref().map_or(base_delay, |adaptive| {
            base_delay.clamp(adaptive.min_delay, adaptive.max_delay)
        });
        Self {
            current,
            config,
            samples: VecDeque::new(),
        }
    }

    pub(crate) fn current(&self) -> Duration {
        self.current
    }

    pub(crate) fn observe(&mut self, elapsed: Duration) {
        let Some(config) = &self.config else {
            return;
        };
        self.samples.push_back(elapsed);
        while self.samples.len() > config.sample_window {
            self.samples.pop_front();
        }
        let mut ordered = self.samples.iter().copied().collect::<Vec<_>>();
        ordered.sort_unstable();
        let index = (ordered.len() * usize::from(config.percentile))
            .div_ceil(100)
            .saturating_sub(1)
            .min(ordered.len() - 1);
        self.current = ordered[index].clamp(config.min_delay, config.max_delay);
    }
}

/// Static configuration for one local runtime instance.
#[derive(Debug, Clone)]
pub struct ReplicaRuntimeConfig {
    pub(super) replica_id: ReplicaId,
    pub(super) lane_id: LaneId,
    pub(super) cluster: ClusterIdentity,
    pub replica: ReplicaConfig,
    pub epoch_size: u64,
    pub base_hedge_delay: Duration,
    /// Transport bound for synchronous decision notifications and status
    /// probes. Consensus proposals use `ProposerConfig::rpc_timeout`.
    pub transport_timeout: Duration,
    pub(super) auto_schedules: bool,
    pub(super) adaptive_hedging: Option<AdaptiveHedgingConfig>,
}

impl ReplicaRuntimeConfig {
    pub fn new(
        replica_id: ReplicaId,
        lane_id: LaneId,
        members: Vec<ReplicaId>,
        max_faults: usize,
        replica: ReplicaConfig,
        epoch_size: u64,
        base_hedge_delay: Duration,
    ) -> Result<Self> {
        let cluster = ClusterIdentity::new(members, max_faults)?;
        if !cluster.contains(replica_id) {
            return Err(QuePaxaError::UnknownReplica(replica_id));
        }
        if epoch_size == 0
            || replica.batch_size == 0
            || replica.pipeline_len == 0
            || replica.max_pending_values == 0
            || replica.max_log_slots == 0
            || replica.max_tracked_value_ids == 0
        {
            return Err(QuePaxaError::InvalidProposal(
                "runtime limits and epoch size must be non-zero".into(),
            ));
        }

        Ok(Self {
            replica_id,
            lane_id,
            cluster,
            replica,
            epoch_size,
            base_hedge_delay,
            transport_timeout: Duration::from_secs(30),
            auto_schedules: false,
            adaptive_hedging: None,
        })
    }

    /// Derives every epoch schedule deterministically from the committed log
    /// instead of requiring an external control plane to agree on schedules.
    ///
    /// The first `2n + 1` epochs rotate the leader round-robin (exploration);
    /// later epochs rank replicas by the average decided step of the slots
    /// each one led (exploitation), as in the paper's multi-armed-bandit
    /// tuning. Because the inputs are committed decisions, every replica
    /// derives the identical schedule, which removes the split-brain risk of
    /// divergent manual installs. All replicas must enable the same mode.
    pub fn with_auto_schedules(mut self) -> Result<Self> {
        if self.epoch_size < self.replica.pipeline_len as u64 {
            return Err(QuePaxaError::PolicyError(
                "auto schedules require epoch_size >= pipeline_len so ranking inputs are final"
                    .into(),
            ));
        }
        self.auto_schedules = true;
        Ok(self)
    }

    pub fn auto_schedules(&self) -> bool {
        self.auto_schedules
    }

    pub fn with_transport_timeout(mut self, timeout: Duration) -> Self {
        self.transport_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    /// Tunes the local hedging delay from observed consensus completion time.
    /// This may differ between replicas without affecting safety; the leader
    /// sequence remains agreed separately.
    pub fn with_adaptive_hedging(mut self, config: AdaptiveHedgingConfig) -> Self {
        self.adaptive_hedging = Some(config);
        self
    }

    pub fn adaptive_hedging(&self) -> Option<&AdaptiveHedgingConfig> {
        self.adaptive_hedging.as_ref()
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    pub fn lane_id(&self) -> LaneId {
        self.lane_id
    }

    pub fn members(&self) -> &[ReplicaId] {
        self.cluster.members()
    }

    pub fn max_faults(&self) -> usize {
        self.cluster.max_faults()
    }

    /// The `n - f` quorum required by the QuePaxa deployment model.
    pub fn quorum_size(&self) -> usize {
        self.cluster.quorum_size()
    }

    pub fn cluster_identity(&self) -> &ClusterIdentity {
        &self.cluster
    }

    pub(crate) fn install_cluster(&mut self, cluster: ClusterIdentity) {
        self.cluster = cluster;
    }

    pub fn protocol_identity(&self) -> ProtocolIdentity {
        ProtocolIdentity {
            epoch_size: self.epoch_size,
            auto_schedules: self.auto_schedules,
        }
    }

    pub fn epoch_for(&self, slot: SlotIndex) -> u64 {
        slot.get() / self.epoch_size
    }
}

/// An agreed proposer order for one epoch. The first member is the round-one
/// leader; later members hedge after increasing multiples of the base delay.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochSchedule {
    pub epoch: u64,
    pub proposers: Vec<ReplicaId>,
}

impl EpochSchedule {
    pub fn new(epoch: u64, proposers: Vec<ReplicaId>) -> Result<Self> {
        if proposers.is_empty() {
            return Err(QuePaxaError::EmptyCluster);
        }
        let unique = proposers.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != proposers.len() {
            let duplicate = proposers
                .iter()
                .find(|replica| {
                    proposers
                        .iter()
                        .filter(|candidate| candidate == replica)
                        .count()
                        > 1
                })
                .copied()
                .expect("a duplicate proposer exists");
            return Err(QuePaxaError::DuplicateReplica(duplicate));
        }
        Ok(Self { epoch, proposers })
    }

    /// Produces a schedule proposal from a policy. Callers must distribute and
    /// agree on the resulting schedule before installing it at any runtime.
    pub fn from_policy<P: HedgingPolicy>(
        config: &ReplicaRuntimeConfig,
        epoch: u64,
        policy: &P,
        previous_winner: Option<ReplicaId>,
    ) -> Result<Self> {
        let first_slot = epoch
            .checked_mul(config.epoch_size)
            .ok_or(QuePaxaError::SlotOverflow)?;
        Self::new(
            epoch,
            policy.leader_sequence(
                SlotIndex::new(first_slot),
                config.members(),
                previous_winner,
            )?,
        )
    }

    pub fn leader(&self) -> ReplicaId {
        self.proposers[0]
    }
}

/// Performance of one epoch's scheduled leader, derived purely from committed
/// decisions so that every replica computes identical values.
#[cfg_attr(feature = "network", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochStat {
    pub leader: ReplicaId,
    pub slots: u64,
    /// Sum of `decided_step` over the epoch's committed slots. A fast-path
    /// decision contributes 4; every extra round adds 4 more, so a lower
    /// average means a more effective leader.
    pub decided_step_total: u64,
}

/// Trailing epochs whose statistics feed leader ranking. Fixed so ranking
/// stays a pure function of the committed prefix on every replica, and so
/// stale observations age out as conditions change.
const RETAINED_STAT_EPOCHS: u64 = 64;
const AUTO_REEXPLORE_EPOCHS: u64 = 32;

/// Owns the agreed (or deterministically derived) epoch schedules and the
/// committed-log statistics behind automatic leader tuning.
#[derive(Debug, Clone)]
pub struct EpochTuner {
    epoch_size: u64,
    members: Vec<ReplicaId>,
    auto: bool,
    schedules: BTreeMap<u64, EpochSchedule>,
    stats: BTreeMap<u64, EpochStat>,
    stats_through: SlotIndex,
}

impl EpochTuner {
    pub fn new(epoch_size: u64, members: Vec<ReplicaId>, auto: bool) -> Self {
        Self {
            epoch_size: epoch_size.max(1),
            members,
            auto,
            schedules: BTreeMap::new(),
            stats: BTreeMap::new(),
            stats_through: SlotIndex::GENESIS,
        }
    }

    pub fn restore(
        epoch_size: u64,
        members: Vec<ReplicaId>,
        auto: bool,
        schedules: BTreeMap<u64, EpochSchedule>,
        stats: BTreeMap<u64, EpochStat>,
        stats_through: SlotIndex,
    ) -> Result<Self> {
        let mut tuner = Self::new(epoch_size, members, auto);
        for (epoch, stat) in &stats {
            if !tuner.members.contains(&stat.leader)
                || stat.slots == 0
                || *epoch > stats_through.get() / tuner.epoch_size
            {
                return Err(QuePaxaError::PolicyError(
                    "restored epoch statistics are inconsistent with the committed log".into(),
                ));
            }
        }
        for (epoch, schedule) in &schedules {
            if *epoch != schedule.epoch {
                return Err(QuePaxaError::PolicyError(
                    "restored epoch schedule key does not match its epoch".into(),
                ));
            }
        }
        tuner.schedules = schedules;
        tuner.stats = stats;
        tuner.stats_through = stats_through;
        Ok(tuner)
    }

    pub fn schedules(&self) -> &BTreeMap<u64, EpochSchedule> {
        &self.schedules
    }

    pub fn stats(&self) -> &BTreeMap<u64, EpochStat> {
        &self.stats
    }

    pub fn stats_through(&self) -> SlotIndex {
        self.stats_through
    }

    /// Resets safety-relevant schedules and performance samples at a drained
    /// membership barrier. Slot numbering continues, but no schedule derived
    /// for the prior voter set is reused in the new epoch.
    pub fn reconfigure(&mut self, members: Vec<ReplicaId>, committed_through: SlotIndex) {
        self.members = members;
        self.schedules.clear();
        self.stats.clear();
        self.stats_through = committed_through;
    }

    /// Installs an externally agreed schedule. Rejected in auto mode, where a
    /// divergent manual install could reintroduce dueling round-one leaders.
    pub fn install(&mut self, schedule: EpochSchedule) -> Result<bool> {
        if self.auto {
            return Err(QuePaxaError::PolicyError(
                "auto-scheduled runtimes derive schedules from the committed log".into(),
            ));
        }
        if let Some(existing) = self.schedules.get(&schedule.epoch) {
            if existing != &schedule {
                return Err(QuePaxaError::PolicyError(format!(
                    "epoch {} already has a different agreed schedule",
                    schedule.epoch
                )));
            }
            return Ok(false);
        }
        self.schedules.insert(schedule.epoch, schedule);
        Ok(true)
    }

    /// Returns the schedule for an epoch, deriving and caching it in auto
    /// mode. Every replica derives the same schedule from the same committed
    /// prefix, so no external agreement round is needed.
    pub fn ensure_schedule(&mut self, epoch: u64) -> Result<EpochSchedule> {
        if let Some(schedule) = self.schedules.get(&epoch) {
            return Ok(schedule.clone());
        }
        if !self.auto {
            return Err(QuePaxaError::PolicyError(format!(
                "no agreed schedule is installed for epoch {epoch}"
            )));
        }
        let schedule = self.derive(epoch)?;
        self.schedules.insert(epoch, schedule.clone());
        Ok(schedule)
    }

    /// Records one committed slot's outcome. Must be called in contiguous
    /// slot order; already-recorded slots are ignored.
    pub fn note_committed(&mut self, slot: SlotIndex, decided_step: Step) -> Result<bool> {
        if !self.auto || slot <= self.stats_through {
            return Ok(false);
        }
        if Some(slot) != self.stats_through.checked_next() {
            return Err(QuePaxaError::PolicyError(
                "epoch statistics must be recorded in contiguous slot order".into(),
            ));
        }
        let epoch = slot.get() / self.epoch_size;
        let leader = self.ensure_schedule(epoch)?.leader();
        let stat = self.stats.entry(epoch).or_insert(EpochStat {
            leader,
            slots: 0,
            decided_step_total: 0,
        });
        stat.slots = stat.slots.saturating_add(1);
        stat.decided_step_total = stat.decided_step_total.saturating_add(decided_step.get());
        self.stats_through = slot;
        if let Some(floor) = epoch.checked_sub(RETAINED_STAT_EPOCHS) {
            self.stats.retain(|kept, _| *kept >= floor);
            self.schedules.retain(|kept, _| *kept >= floor);
        }
        Ok(true)
    }

    fn derive(&self, epoch: u64) -> Result<EpochSchedule> {
        let replica_count = self.members.len() as u64;
        let warmup_epochs = 2 * replica_count + 1;
        if epoch < warmup_epochs {
            let offset = (epoch % replica_count) as usize;
            return EpochSchedule::new(epoch, crate::policy::rotated(&self.members, offset));
        }

        // Rank on epochs at least two behind so the inputs are final before
        // any replica can propose in this epoch.
        let window_end = epoch - 2;
        let required = window_end
            .checked_add(1)
            .and_then(|next| next.checked_mul(self.epoch_size))
            .map(|start| SlotIndex::new(start.saturating_sub(1)))
            .ok_or(QuePaxaError::SlotOverflow)?;
        if self.stats_through < required {
            return Err(QuePaxaError::PolicyError(format!(
                "epoch {epoch} schedule requires committed history through slot {required}"
            )));
        }

        let mut totals: BTreeMap<ReplicaId, (u128, u128)> = BTreeMap::new();
        for stat in self.stats.range(..=window_end).map(|(_, stat)| stat) {
            let entry = totals.entry(stat.leader).or_insert((0, 0));
            entry.0 += stat.slots as u128;
            entry.1 += stat.decided_step_total as u128;
        }
        let mut ranked = self.members.clone();
        ranked.sort_by(|left, right| {
            compare_leader_fitness(totals.get(left), totals.get(right)).then(left.cmp(right))
        });
        let exploitation_epoch = epoch - warmup_epochs;
        if exploitation_epoch > 0 && exploitation_epoch % AUTO_REEXPLORE_EPOCHS == 0 {
            let candidate = self.members
                [((exploitation_epoch / AUTO_REEXPLORE_EPOCHS) % replica_count) as usize];
            let position = ranked
                .iter()
                .position(|replica| *replica == candidate)
                .expect("configured members are present in the ranking");
            let candidate = ranked.remove(position);
            ranked.insert(0, candidate);
        }
        EpochSchedule::new(epoch, ranked)
    }
}

/// Orders leaders by average decided step, treating unsampled replicas as
/// worse than any sampled one. Sampled averages compare via cross
/// multiplication to stay exact: total_a / slots_a < total_b / slots_b
/// iff total_a * slots_b < total_b * slots_a.
fn compare_leader_fitness(
    left: Option<&(u128, u128)>,
    right: Option<&(u128, u128)>,
) -> std::cmp::Ordering {
    match (left, right) {
        (Some((slots_a, total_a)), Some((slots_b, total_b))) => {
            (total_a * slots_b).cmp(&(total_b * slots_a))
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}
