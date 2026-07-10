//! A three-recorder deployment exercising the production integration contract.
//!
//! This remains in-process so the example is deterministic, but unlike the
//! minimal example it uses durable recorder/runtime stores, loses one recorder,
//! restarts it, retries a client submission, and restarts the proposer runtime.

use rust_quepaxa::{
    AllowAllAvailability, ClientNotifier, Decision, DurableRecorderCore, EpochSchedule,
    InMemoryRecorderStore, InMemoryRuntimeStore, LaneId, ProposerCore, QuePaxaError, RecordReply,
    RecordRequest, RecorderClient, RecorderConfig, RecorderHandle, ReplicaConfig, ReplicaId,
    ReplicaRuntime, ReplicaRuntimeConfig, Result, RuntimePoll, StateMachine,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const NOOP_VALUE: u64 = u64::MAX;

type RecorderStore = Arc<Mutex<InMemoryRecorderStore<u64>>>;

struct RestartableRecorder {
    config: RecorderConfig,
    store: RecorderStore,
    online: Arc<AtomicBool>,
    core: DurableRecorderCore<u64, RecorderStore>,
}

impl RestartableRecorder {
    fn new(config: RecorderConfig, store: RecorderStore, online: Arc<AtomicBool>) -> Result<Self> {
        let core = DurableRecorderCore::new(
            config.clone(),
            Arc::new(AllowAllAvailability),
            Arc::clone(&store),
        )?;
        Ok(Self {
            config,
            store,
            online,
            core,
        })
    }

    fn restart(&mut self) -> Result<()> {
        self.core = DurableRecorderCore::new(
            self.config.clone(),
            Arc::new(AllowAllAvailability),
            Arc::clone(&self.store),
        )?;
        Ok(())
    }

    fn require_online(&self) -> Result<()> {
        if self.online.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(QuePaxaError::TransportError(
                "simulated recorder outage".into(),
            ))
        }
    }
}

impl RecorderClient<u64> for RestartableRecorder {
    fn record(&mut self, request: RecordRequest<u64>) -> Result<RecordReply<u64>> {
        self.require_online()?;
        self.core.record(request)
    }

    fn inform_decisions(&mut self, decisions: &[Decision<u64>]) -> Result<()> {
        self.require_online()?;
        for decision in decisions {
            self.core.inform_decision(decision.clone())?;
        }
        Ok(())
    }
}

struct CountingStateMachine(Arc<AtomicUsize>);

impl StateMachine<u64> for CountingStateMachine {
    fn execute(&mut self, decision: &Decision<u64>) -> Result<()> {
        if decision.value_ids == [NOOP_VALUE] {
            return Ok(());
        }
        self.0.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

struct NoopNotifier;

impl ClientNotifier<u64> for NoopNotifier {
    fn committed(&mut self, _decision: &Decision<u64>) -> Result<()> {
        Ok(())
    }
}

fn main() -> Result<()> {
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let recorder_stores = (0..3)
        .map(|_| Arc::new(Mutex::new(InMemoryRecorderStore::default())))
        .collect::<Vec<_>>();
    let online = (0..3)
        .map(|_| Arc::new(AtomicBool::new(true)))
        .collect::<Vec<_>>();
    let recorders = members
        .iter()
        .enumerate()
        .map(|(index, id)| {
            Ok(RecorderHandle::new(
                *id,
                RestartableRecorder::new(
                    RecorderConfig::new(*id, members.clone(), 1)?,
                    Arc::clone(&recorder_stores[index]),
                    Arc::clone(&online[index]),
                )?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let runtime_store = Arc::new(Mutex::new(InMemoryRuntimeStore::default()));
    let executions = Arc::new(AtomicUsize::new(0));

    // Lose one recorder. The remaining two still form the configured n-f quorum.
    online[2].store(false, Ordering::Release);
    let config = ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members.clone(),
        1,
        ReplicaConfig::default(),
        64,
        Duration::ZERO,
    )?;
    let mut runtime = ReplicaRuntime::new(
        config.clone(),
        ProposerCore::new(ReplicaId::new(1), LaneId::new(1))?,
        recorders.clone(),
        Arc::clone(&runtime_store),
        CountingStateMachine(Arc::clone(&executions)),
        NoopNotifier,
    )?
    .with_noop_value(NOOP_VALUE);
    runtime.install_epoch_schedule(EpochSchedule::new(0, members)?)?;
    runtime.submit([10])?;
    assert!(matches!(runtime.run_once()?, RuntimePoll::Committed(_)));
    assert_eq!(executions.load(Ordering::Acquire), 1);

    // Restart the failed recorder, then retry pending decision dissemination.
    recorders[2].with_client(|recorder| recorder.restart())??;
    online[2].store(true, Ordering::Release);
    runtime.recover()?;
    assert_eq!(
        recorder_stores[2]
            .lock()
            .unwrap()
            .snapshot()
            .unwrap()
            .decisions
            .len(),
        1
    );

    // A client retry with the same value ID is durably suppressed.
    runtime.submit([10])?;
    assert!(matches!(runtime.run_once()?, RuntimePoll::Idle));
    drop(runtime);

    // Restart the proposer runtime. Already executed work is not replayed.
    let mut recovered = ReplicaRuntime::new(
        config,
        ProposerCore::new(ReplicaId::new(1), LaneId::new(1))?,
        recorders,
        runtime_store,
        CountingStateMachine(Arc::clone(&executions)),
        NoopNotifier,
    )?
    .with_noop_value(NOOP_VALUE);
    recovered.recover()?;
    assert_eq!(executions.load(Ordering::Acquire), 1);

    println!("three-node failure, recorder restart, client retry, and runtime restart succeeded");
    Ok(())
}
