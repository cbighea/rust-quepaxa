//! A minimal in-process QuePaxa cluster.
//!
//! The direct `RecorderHandle`s stand in for authenticated RPC endpoints. A
//! real deployment must replace them with clients bound to verified peer
//! identities and persist the recorder, value-store, and replica state.

use rust_quepaxa::{
    ClientNotifier, EpochSchedule, FetchingAvailability, InMemoryRuntimeStore, InMemoryValueStore,
    LaneId, ProposerCore, RecorderConfig, RecorderCore, RecorderHandle, ReplicaConfig, ReplicaId,
    ReplicaRuntime, ReplicaRuntimeConfig, Result, RuntimePoll, StateMachine, ValueFetcher,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const NOOP_VALUE: u64 = u64::MAX;

struct VerifiedSource {
    values: BTreeMap<u64, String>,
}

impl ValueFetcher<u64, String> for VerifiedSource {
    fn fetch_values(&mut self, value_ids: &[u64]) -> Result<Vec<(u64, String)>> {
        // In a production adapter, this is where authenticated retrieval and
        // value-ID-to-payload verification happen.
        Ok(value_ids
            .iter()
            .filter_map(|value_id| {
                self.values
                    .get(value_id)
                    .map(|payload| (*value_id, payload.clone()))
            })
            .collect())
    }
}

struct PrintingStateMachine;

impl StateMachine<u64> for PrintingStateMachine {
    fn execute(&mut self, decision: &rust_quepaxa::Decision<u64>) -> Result<()> {
        if decision.value_ids == [NOOP_VALUE] {
            return Ok(());
        }
        println!("execute slot {}: {:?}", decision.slot, decision.value_ids);
        Ok(())
    }
}

struct PrintingClientNotifier;

impl ClientNotifier<u64> for PrintingClientNotifier {
    fn committed(&mut self, decision: &rust_quepaxa::Decision<u64>) -> Result<()> {
        println!("notify clients for slot {}", decision.slot);
        Ok(())
    }
}

fn main() -> Result<()> {
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();
    let store = Arc::new(Mutex::new(InMemoryValueStore::new()));
    let availability = Arc::new(FetchingAvailability::new(
        Arc::clone(&store),
        VerifiedSource {
            values: BTreeMap::from([
                (10, "set x = 1".to_owned()),
                (NOOP_VALUE, "no-op".to_owned()),
            ]),
        },
    ));
    let recorders = members
        .iter()
        .map(|id| {
            let config = RecorderConfig::new(*id, members.clone(), 1)?;
            Ok(RecorderHandle::new(
                *id,
                RecorderCore::new(config, availability.clone()),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

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
        config,
        ProposerCore::new(ReplicaId::new(1), LaneId::new(1))?,
        recorders,
        InMemoryRuntimeStore::default(),
        PrintingStateMachine,
        PrintingClientNotifier,
    )?
    .with_noop_value(NOOP_VALUE);
    runtime.install_epoch_schedule(EpochSchedule::new(0, members)?)?;
    runtime.submit([10])?;

    assert!(matches!(runtime.run_once()?, RuntimePoll::Committed(_)));
    Ok(())
}
