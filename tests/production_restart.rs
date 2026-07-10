#![cfg(feature = "network")]

use rust_quepaxa::network::{PostcardRecorderCodec, PostcardRuntimeCodec};
use rust_quepaxa::{
    AllowAllAvailability, DurableRecorderCore, EpochSchedule, FileRecorderStore, FileRuntimeStore,
    LaneId, NoopClientNotifier, NoopStateMachine, ProposerCore, RecorderConfig, RecorderHandle,
    ReplicaConfig, ReplicaId, ReplicaRuntime, ReplicaRuntimeConfig, RuntimePoll, SlotIndex,
    XorShift64,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type DurableRecorder = DurableRecorderCore<u64, FileRecorderStore<u64, PostcardRecorderCodec<u64>>>;

fn scratch_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("quepaxa-restart-{}-{nonce}", std::process::id()))
}

fn durable_recorders(dir: &Path, members: &[ReplicaId]) -> Vec<RecorderHandle<DurableRecorder>> {
    members
        .iter()
        .map(|replica| {
            let store = FileRecorderStore::new(
                dir.join(format!("recorder-{}.state", replica.get())),
                PostcardRecorderCodec::default(),
            );
            let core = DurableRecorderCore::new(
                RecorderConfig::new(*replica, members.to_vec(), 1).unwrap(),
                Arc::new(AllowAllAvailability),
                store,
            )
            .unwrap();
            RecorderHandle::new(*replica, core)
        })
        .collect()
}

fn runtime_config(members: &[ReplicaId]) -> ReplicaRuntimeConfig {
    ReplicaRuntimeConfig::new(
        ReplicaId::new(1),
        LaneId::new(1),
        members.to_vec(),
        1,
        ReplicaConfig {
            batch_size: 1,
            pipeline_len: 1,
            ..ReplicaConfig::default()
        },
        16,
        Duration::ZERO,
    )
    .unwrap()
}

#[test]
fn production_file_stores_restart_and_ignore_an_orphaned_pre_rename_write() {
    let dir = scratch_dir();
    fs::create_dir_all(&dir).unwrap();
    let runtime_path = dir.join("runtime.state");
    let members = (1..=3).map(ReplicaId::new).collect::<Vec<_>>();

    let mut first = ReplicaRuntime::new(
        runtime_config(&members),
        ProposerCore::with_rng(
            ReplicaId::new(1),
            LaneId::new(1),
            XorShift64::new_for_stream(1, ReplicaId::new(1), LaneId::new(1)),
        ),
        durable_recorders(&dir, &members),
        FileRuntimeStore::new(&runtime_path, PostcardRuntimeCodec::default()),
        NoopStateMachine,
        NoopClientNotifier,
    )
    .unwrap();
    first
        .install_epoch_schedule(EpochSchedule::new(0, members.clone()).unwrap())
        .unwrap();
    first.submit([10]).unwrap();
    assert!(matches!(
        first.run_once().unwrap(),
        RuntimePoll::Committed(_)
    ));
    drop(first);

    // FileRuntimeStore writes and syncs this temporary path before rename.
    // An orphan therefore models a process dying immediately before rename;
    // recovery must continue to use the last fully installed snapshot.
    fs::copy(&runtime_path, runtime_path.with_extension("runtime.tmp")).unwrap();

    let mut restarted = ReplicaRuntime::new(
        runtime_config(&members),
        ProposerCore::with_rng(
            ReplicaId::new(1),
            LaneId::new(1),
            XorShift64::new_for_stream(2, ReplicaId::new(1), LaneId::new(1)),
        ),
        durable_recorders(&dir, &members),
        FileRuntimeStore::new(&runtime_path, PostcardRuntimeCodec::default()),
        NoopStateMachine,
        NoopClientNotifier,
    )
    .unwrap();
    assert_eq!(
        restarted.decision(SlotIndex::new(1)).unwrap().value_ids,
        vec![10]
    );
    restarted.submit([11]).unwrap();
    assert!(matches!(
        restarted.run_once().unwrap(),
        RuntimePoll::Committed(_)
    ));
    assert_eq!(
        restarted.decision(SlotIndex::new(2)).unwrap().value_ids,
        vec![11]
    );
    drop(restarted);

    fs::remove_dir_all(dir).unwrap();
}
