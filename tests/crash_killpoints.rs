#![cfg(feature = "network")]

use rust_quepaxa::network::{
    BatchStore, ExactlyOnceExecutor, FileBatchStore, FileExactlyOnceExecutor,
    FileSubmissionJournal, PostcardRecorderCodec, PostcardRuntimeCodec, SubmissionJournal,
    SubmissionOutcome,
};
use rust_quepaxa::{
    AllowAllAvailability, ClusterIdentity, CrashPoint, Decision, FileRecorderStore,
    FileRuntimeStore, PendingProposalSnapshot, ProtocolIdentity, RecorderConfig, RecorderSnapshot,
    RecorderStateStore, ReplicaConfig, ReplicaCore, ReplicaId, Result, RuntimeSnapshot,
    RuntimeStateStore, SlotIndex, Step,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHILD_ENV: &str = "QUEPAXA_CRASH_CHILD";
const DIR_ENV: &str = "QUEPAXA_CRASH_DIR";
const POINT_ENV: &str = "QUEPAXA_CRASH_POINT";

fn scratch_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path =
        std::env::temp_dir().join(format!("quepaxa-killpoints-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn marker_injector(marker: PathBuf, selected: String) -> impl rust_quepaxa::CrashInjector {
    move |point: CrashPoint| {
        if format!("{point:?}") == selected {
            fs::write(&marker, selected.as_bytes()).map_err(|error| {
                rust_quepaxa::QuePaxaError::StorageError(format!(
                    "could not write kill-point marker: {error}"
                ))
            })?;
            File::open(&marker)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    rust_quepaxa::QuePaxaError::StorageError(format!(
                        "could not sync kill-point marker: {error}"
                    ))
                })?;
            loop {
                thread::park();
            }
        }
        Ok(())
    }
}

fn cluster() -> ClusterIdentity {
    ClusterIdentity::new([ReplicaId::new(1)], 0).unwrap()
}

fn recorder_snapshot(value: u64) -> RecorderSnapshot<u64> {
    let config = RecorderConfig::from_cluster(ReplicaId::new(1), cluster()).unwrap();
    let mut core = rust_quepaxa::RecorderCore::new(config, Arc::new(AllowAllAvailability));
    core.inform_decision(
        Decision::new(
            SlotIndex::new(value),
            vec![value],
            ReplicaId::new(1),
            Step::ROUND_ONE_PHASE_ZERO,
        )
        .unwrap(),
    )
    .unwrap();
    core.snapshot()
}

fn runtime_snapshot(value: u64) -> RuntimeSnapshot<u64> {
    RuntimeSnapshot {
        cluster: cluster(),
        protocol: ProtocolIdentity {
            epoch_size: 16,
            auto_schedules: false,
        },
        replica: ReplicaCore::new(ReplicaConfig::default()).snapshot(),
        decisions: BTreeMap::new(),
        schedules: BTreeMap::new(),
        pending: BTreeMap::from([(
            SlotIndex::new(value),
            PendingProposalSnapshot {
                value_ids: vec![value],
            },
        )]),
        executed: BTreeSet::new(),
        notified: BTreeSet::new(),
        announced_to: BTreeMap::new(),
        epoch_stats: BTreeMap::new(),
        stats_through: SlotIndex::GENESIS,
    }
}

fn run_child(kind: &str, point: CrashPoint, dir: &Path) {
    let marker = dir.join("reached");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("subprocess_kill_child")
        .arg("--nocapture")
        .env(CHILD_ENV, kind)
        .env(DIR_ENV, dir)
        .env(POINT_ENV, format!("{point:?}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("kill-point child exited before reaching {point:?}: {status}");
        }
        assert!(Instant::now() < deadline, "child did not reach {point:?}");
        thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success(), "kill-point child was not killed");
}

#[test]
fn subprocess_kill_child() -> Result<()> {
    let Ok(kind) = std::env::var(CHILD_ENV) else {
        return Ok(());
    };
    let dir = PathBuf::from(std::env::var_os(DIR_ENV).unwrap());
    let selected = std::env::var(POINT_ENV).unwrap();
    let injector = marker_injector(dir.join("reached"), selected);
    match kind.as_str() {
        "recorder" => {
            let mut store = FileRecorderStore::new(
                dir.join("recorder.state"),
                PostcardRecorderCodec::default(),
            )
            .with_crash_injector(injector);
            store.save(&recorder_snapshot(2))?;
        }
        "runtime" => {
            let mut store =
                FileRuntimeStore::new(dir.join("runtime.state"), PostcardRuntimeCodec::default())
                    .with_crash_injector(injector);
            store.save(&runtime_snapshot(2))?;
        }
        "batch" => {
            let mut store = FileBatchStore::<u64>::open(dir.join("batches.state"), 1024 * 1024)?
                .with_crash_injector(injector);
            store.put(2, b"new".to_vec())?;
        }
        "submission-journal" => {
            let mut journal = FileSubmissionJournal::<u64>::open(
                dir.join("submission-journal.state"),
                1024 * 1024,
            )?
            .with_crash_injector(injector);
            journal.put([2; 16], 2, SubmissionOutcome::Accepted)?;
        }
        "exactly-once" => {
            let apply =
                |state: &mut BTreeMap<u64, u64>, slot: SlotIndex, value_id: &u64| -> Result<()> {
                    state.insert(*value_id, slot.get());
                    Ok(())
                };
            let mut executor = FileExactlyOnceExecutor::open(
                dir.join("exactly-once.state"),
                1024 * 1024,
                BTreeMap::new(),
                apply,
            )?
            .with_crash_injector(injector);
            executor.execute_once(SlotIndex::new(2), &2)?;
        }
        other => panic!("unknown child kind {other}"),
    }
    panic!("selected kill point was not reached")
}

#[test]
fn literal_subprocess_kills_cover_every_durable_write_boundary() {
    let recorder_points = [
        CrashPoint::RecorderTemporaryOpened,
        CrashPoint::RecorderTemporaryWritten,
        CrashPoint::RecorderTemporarySynced,
        CrashPoint::RecorderRenamed,
        CrashPoint::RecorderDirectorySynced,
    ];
    for point in recorder_points {
        let dir = scratch_dir();
        let path = dir.join("recorder.state");
        FileRecorderStore::new(&path, PostcardRecorderCodec::default())
            .save(&recorder_snapshot(1))
            .unwrap();
        run_child("recorder", point, &dir);
        let restored = FileRecorderStore::new(&path, PostcardRecorderCodec::default())
            .load()
            .unwrap()
            .unwrap();
        assert!(restored == recorder_snapshot(1) || restored == recorder_snapshot(2));
        fs::remove_dir_all(dir).unwrap();
    }

    let runtime_points = [
        CrashPoint::RuntimeTemporaryOpened,
        CrashPoint::RuntimeTemporaryWritten,
        CrashPoint::RuntimeTemporarySynced,
        CrashPoint::RuntimeRenamed,
        CrashPoint::RuntimeDirectorySynced,
    ];
    for point in runtime_points {
        let dir = scratch_dir();
        let path = dir.join("runtime.state");
        FileRuntimeStore::new(&path, PostcardRuntimeCodec::default())
            .save(&runtime_snapshot(1))
            .unwrap();
        run_child("runtime", point, &dir);
        let restored = FileRuntimeStore::new(&path, PostcardRuntimeCodec::default())
            .load()
            .unwrap()
            .unwrap();
        assert!(restored == runtime_snapshot(1) || restored == runtime_snapshot(2));
        fs::remove_dir_all(dir).unwrap();
    }

    let batch_points = [
        CrashPoint::BatchTemporaryOpened,
        CrashPoint::BatchTemporaryWritten,
        CrashPoint::BatchTemporarySynced,
        CrashPoint::BatchRenamed,
        CrashPoint::BatchDirectorySynced,
    ];
    for point in batch_points {
        let dir = scratch_dir();
        let path = dir.join("batches.state");
        FileBatchStore::<u64>::open(&path, 1024 * 1024)
            .unwrap()
            .put(1, b"old".to_vec())
            .unwrap();
        run_child("batch", point, &dir);
        let restored = FileBatchStore::<u64>::open(&path, 1024 * 1024).unwrap();
        assert_eq!(restored.get(&1).unwrap(), Some(b"old".to_vec()));
        let _possibly_committed = restored.get(&2).unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    let journal_points = [
        CrashPoint::SubmissionJournalTemporaryOpened,
        CrashPoint::SubmissionJournalTemporaryWritten,
        CrashPoint::SubmissionJournalTemporarySynced,
        CrashPoint::SubmissionJournalRenamed,
        CrashPoint::SubmissionJournalDirectorySynced,
    ];
    for point in journal_points {
        let dir = scratch_dir();
        let path = dir.join("submission-journal.state");
        FileSubmissionJournal::<u64>::open(&path, 1024 * 1024)
            .unwrap()
            .put([1; 16], 1, SubmissionOutcome::Accepted)
            .unwrap();
        run_child("submission-journal", point, &dir);
        let restored = FileSubmissionJournal::<u64>::open(&path, 1024 * 1024).unwrap();
        assert_eq!(
            restored.get([1; 16], 1).unwrap(),
            Some(SubmissionOutcome::Accepted)
        );
        let _possibly_committed = restored.get([2; 16], 2).unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    let exactly_once_points = [
        CrashPoint::ExactlyOnceTemporaryOpened,
        CrashPoint::ExactlyOnceTemporaryWritten,
        CrashPoint::ExactlyOnceTemporarySynced,
        CrashPoint::ExactlyOnceRenamed,
        CrashPoint::ExactlyOnceDirectorySynced,
    ];
    for point in exactly_once_points {
        let dir = scratch_dir();
        let path = dir.join("exactly-once.state");
        let apply =
            |state: &mut BTreeMap<u64, u64>, slot: SlotIndex, value_id: &u64| -> Result<()> {
                state.insert(*value_id, slot.get());
                Ok(())
            };
        FileExactlyOnceExecutor::open(&path, 1024 * 1024, BTreeMap::new(), apply)
            .unwrap()
            .execute_once(SlotIndex::new(1), &1)
            .unwrap();
        run_child("exactly-once", point, &dir);
        let apply =
            |state: &mut BTreeMap<u64, u64>, slot: SlotIndex, value_id: &u64| -> Result<()> {
                state.insert(*value_id, slot.get());
                Ok(())
            };
        let restored = FileExactlyOnceExecutor::<u64, BTreeMap<u64, u64>, _>::open(
            &path,
            1024 * 1024,
            BTreeMap::new(),
            apply,
        )
        .unwrap();
        assert_eq!(restored.application().get(&1), Some(&1));
        let _possibly_committed = restored.application().get(&2);
        fs::remove_dir_all(dir).unwrap();
    }
}
