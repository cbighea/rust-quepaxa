use crate::crash::{CrashInjector, CrashPoint, NoopCrashInjector};
use crate::error::{QuePaxaError, Result};
use crate::network::server::{BoxSubmissionFuture, SubmissionHandler};
use crate::network::wire::{Submission, SubmissionOutcome};
use crate::runtime::StateMachine;
use crate::types::{Decision, SlotIndex};
use futures_util::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

type SubmissionKey = ([u8; 16], u64);
type RequestLocks = BTreeMap<SubmissionKey, Arc<Mutex<()>>>;
const DEDUPE_STORAGE_VERSION: u16 = 1;
const JOURNAL_CRASH_POINTS: [CrashPoint; 5] = [
    CrashPoint::SubmissionJournalTemporaryOpened,
    CrashPoint::SubmissionJournalTemporaryWritten,
    CrashPoint::SubmissionJournalTemporarySynced,
    CrashPoint::SubmissionJournalRenamed,
    CrashPoint::SubmissionJournalDirectorySynced,
];
const EXACTLY_ONCE_CRASH_POINTS: [CrashPoint; 5] = [
    CrashPoint::ExactlyOnceTemporaryOpened,
    CrashPoint::ExactlyOnceTemporaryWritten,
    CrashPoint::ExactlyOnceTemporarySynced,
    CrashPoint::ExactlyOnceRenamed,
    CrashPoint::ExactlyOnceDirectorySynced,
];

/// Application transaction boundary for cross-replica command deduplication.
///
/// A durable implementation must atomically apply a previously unseen value ID
/// and remember that ID. It returns `true` when it applied the command and
/// `false` when the command had already been applied through another slot.
pub trait ExactlyOnceExecutor<V>: Send {
    fn execute_once(&mut self, slot: SlotIndex, value_id: &V) -> Result<bool>;

    fn export_checkpoint(&mut self, _through: SlotIndex) -> Result<Vec<u8>> {
        Err(QuePaxaError::StorageError(
            "the exactly-once executor does not implement checkpoint export".into(),
        ))
    }

    fn import_checkpoint(&mut self, _through: SlotIndex, _checkpoint: &[u8]) -> Result<()> {
        Err(QuePaxaError::StorageError(
            "the exactly-once executor does not implement checkpoint import".into(),
        ))
    }
}

pub struct DeduplicatingStateMachine<E> {
    executor: E,
}

impl<E> DeduplicatingStateMachine<E> {
    pub fn new(executor: E) -> Self {
        Self { executor }
    }

    pub fn executor(&self) -> &E {
        &self.executor
    }
}

impl<V, E> StateMachine<V> for DeduplicatingStateMachine<E>
where
    E: ExactlyOnceExecutor<V>,
{
    fn execute(&mut self, decision: &Decision<V>) -> Result<()> {
        for value_id in &decision.value_ids {
            self.executor.execute_once(decision.slot, value_id)?;
        }
        Ok(())
    }

    fn export_checkpoint(&mut self, through: SlotIndex) -> Result<Vec<u8>> {
        self.executor.export_checkpoint(through)
    }

    fn import_checkpoint(&mut self, through: SlotIndex, checkpoint: &[u8]) -> Result<()> {
        self.executor.import_checkpoint(through, checkpoint)
    }
}

/// Deterministic reference executor for tests and examples.
pub struct InMemoryExactlyOnceExecutor<V, F> {
    seen: BTreeSet<V>,
    apply: F,
}

impl<V, F> InMemoryExactlyOnceExecutor<V, F> {
    pub fn new(apply: F) -> Self {
        Self {
            seen: BTreeSet::new(),
            apply,
        }
    }

    pub fn seen(&self) -> &BTreeSet<V> {
        &self.seen
    }
}

impl<V, F> ExactlyOnceExecutor<V> for InMemoryExactlyOnceExecutor<V, F>
where
    V: Clone + Ord + Send,
    F: FnMut(SlotIndex, &V) -> Result<()> + Send,
{
    fn execute_once(&mut self, slot: SlotIndex, value_id: &V) -> Result<bool> {
        if self.seen.contains(value_id) {
            return Ok(false);
        }
        (self.apply)(slot, value_id)?;
        self.seen.insert(value_id.clone());
        Ok(true)
    }
}

pub trait SubmissionJournal<V>: Send {
    fn get(&self, client_id: [u8; 16], request_id: u64) -> Result<Option<SubmissionOutcome<V>>>;

    fn put(
        &mut self,
        client_id: [u8; 16],
        request_id: u64,
        outcome: SubmissionOutcome<V>,
    ) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct InMemorySubmissionJournal<V> {
    outcomes: BTreeMap<SubmissionKey, SubmissionOutcome<V>>,
}

impl<V: Clone + PartialEq + Send> SubmissionJournal<V> for InMemorySubmissionJournal<V> {
    fn get(&self, client_id: [u8; 16], request_id: u64) -> Result<Option<SubmissionOutcome<V>>> {
        Ok(self.outcomes.get(&(client_id, request_id)).cloned())
    }

    fn put(
        &mut self,
        client_id: [u8; 16],
        request_id: u64,
        outcome: SubmissionOutcome<V>,
    ) -> Result<()> {
        if let Some(existing) = self.outcomes.get(&(client_id, request_id))
            && existing != &outcome
        {
            return Err(QuePaxaError::InvalidProposal(
                "submission request ID already has a different outcome".into(),
            ));
        }
        self.outcomes.insert((client_id, request_id), outcome);
        Ok(())
    }
}

/// Fsynced request-outcome journal for suppressing same-node client retries
/// across process restarts.
pub struct FileSubmissionJournal<V> {
    path: PathBuf,
    outcomes: BTreeMap<SubmissionKey, SubmissionOutcome<V>>,
    max_snapshot_bytes: usize,
    crash_injector: Arc<dyn CrashInjector>,
}

impl<V> FileSubmissionJournal<V>
where
    V: Clone + Serialize + DeserializeOwned,
{
    pub fn open(path: impl Into<PathBuf>, max_snapshot_bytes: usize) -> Result<Self> {
        let path = path.into();
        let outcomes =
            load_snapshot(&path, max_snapshot_bytes, "submission journal")?.unwrap_or_default();
        Ok(Self {
            path,
            outcomes,
            max_snapshot_bytes,
            crash_injector: Arc::new(NoopCrashInjector),
        })
    }

    pub fn with_crash_injector<I>(mut self, injector: I) -> Self
    where
        I: CrashInjector,
    {
        self.crash_injector = Arc::new(injector);
        self
    }

    pub fn len(&self) -> usize {
        self.outcomes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.outcomes.is_empty()
    }

    fn save(&self, outcomes: &BTreeMap<SubmissionKey, SubmissionOutcome<V>>) -> Result<()> {
        save_snapshot(
            SnapshotTarget {
                path: &self.path,
                temporary_extension: "submission-journal.tmp",
                max_snapshot_bytes: self.max_snapshot_bytes,
                crash_injector: &*self.crash_injector,
                points: JOURNAL_CRASH_POINTS,
                label: "submission journal",
            },
            outcomes,
        )
    }
}

impl<V> SubmissionJournal<V> for FileSubmissionJournal<V>
where
    V: Clone + PartialEq + Serialize + DeserializeOwned + Send,
{
    fn get(&self, client_id: [u8; 16], request_id: u64) -> Result<Option<SubmissionOutcome<V>>> {
        Ok(self.outcomes.get(&(client_id, request_id)).cloned())
    }

    fn put(
        &mut self,
        client_id: [u8; 16],
        request_id: u64,
        outcome: SubmissionOutcome<V>,
    ) -> Result<()> {
        let key = (client_id, request_id);
        if let Some(existing) = self.outcomes.get(&key) {
            if existing != &outcome {
                return Err(QuePaxaError::InvalidProposal(
                    "submission request ID already has a different outcome".into(),
                ));
            }
            return Ok(());
        }
        let mut next = self.outcomes.clone();
        next.insert(key, outcome);
        self.save(&next)?;
        self.outcomes = next;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "V: Serialize, A: Serialize",
    deserialize = "V: Ord + Deserialize<'de>, A: Deserialize<'de>"
))]
struct ExactlyOnceSnapshot<V, A> {
    checkpointed_through: SlotIndex,
    seen: BTreeSet<V>,
    application: A,
}

/// Reference exactly-once executor for snapshotable application state.
///
/// The callback must only mutate the supplied `A`; external side effects would
/// sit outside this file's atomic transaction. Each new value ID and its
/// application mutation are written in one fsynced snapshot before success is
/// returned.
pub struct FileExactlyOnceExecutor<V, A, F> {
    path: PathBuf,
    snapshot: ExactlyOnceSnapshot<V, A>,
    apply: F,
    max_snapshot_bytes: usize,
    crash_injector: Arc<dyn CrashInjector>,
}

impl<V, A, F> FileExactlyOnceExecutor<V, A, F>
where
    V: Clone + Ord + Serialize + DeserializeOwned,
    A: Clone + Serialize + DeserializeOwned,
    F: FnMut(&mut A, SlotIndex, &V) -> Result<()>,
{
    pub fn open(
        path: impl Into<PathBuf>,
        max_snapshot_bytes: usize,
        initial_application: A,
        apply: F,
    ) -> Result<Self> {
        let path = path.into();
        let snapshot = load_snapshot(&path, max_snapshot_bytes, "exactly-once state")?.unwrap_or(
            ExactlyOnceSnapshot {
                checkpointed_through: SlotIndex::GENESIS,
                seen: BTreeSet::new(),
                application: initial_application,
            },
        );
        Ok(Self {
            path,
            snapshot,
            apply,
            max_snapshot_bytes,
            crash_injector: Arc::new(NoopCrashInjector),
        })
    }

    pub fn with_crash_injector<I>(mut self, injector: I) -> Self
    where
        I: CrashInjector,
    {
        self.crash_injector = Arc::new(injector);
        self
    }

    pub fn application(&self) -> &A {
        &self.snapshot.application
    }

    pub fn seen(&self) -> &BTreeSet<V> {
        &self.snapshot.seen
    }

    fn save(&self, snapshot: &ExactlyOnceSnapshot<V, A>) -> Result<()> {
        save_snapshot(
            SnapshotTarget {
                path: &self.path,
                temporary_extension: "exactly-once.tmp",
                max_snapshot_bytes: self.max_snapshot_bytes,
                crash_injector: &*self.crash_injector,
                points: EXACTLY_ONCE_CRASH_POINTS,
                label: "exactly-once state",
            },
            snapshot,
        )
    }
}

impl<V, A, F> ExactlyOnceExecutor<V> for FileExactlyOnceExecutor<V, A, F>
where
    V: Clone + Ord + Serialize + DeserializeOwned + Send,
    A: Clone + Serialize + DeserializeOwned + Send,
    F: FnMut(&mut A, SlotIndex, &V) -> Result<()> + Send,
{
    fn execute_once(&mut self, slot: SlotIndex, value_id: &V) -> Result<bool> {
        if self.snapshot.seen.contains(value_id) {
            return Ok(false);
        }
        let mut next = self.snapshot.clone();
        (self.apply)(&mut next.application, slot, value_id)?;
        next.seen.insert(value_id.clone());
        self.save(&next)?;
        self.snapshot = next;
        Ok(true)
    }

    fn export_checkpoint(&mut self, through: SlotIndex) -> Result<Vec<u8>> {
        let mut next = self.snapshot.clone();
        next.checkpointed_through = through;
        self.save(&next)?;
        self.snapshot = next;
        encode_snapshot(
            &self.snapshot,
            self.max_snapshot_bytes,
            "exactly-once checkpoint",
        )
    }

    fn import_checkpoint(&mut self, through: SlotIndex, checkpoint: &[u8]) -> Result<()> {
        let snapshot: ExactlyOnceSnapshot<V, A> = decode_snapshot(
            checkpoint,
            self.max_snapshot_bytes,
            "exactly-once checkpoint",
        )?;
        if snapshot.checkpointed_through != through {
            return Err(QuePaxaError::ConfigurationMismatch);
        }
        self.save(&snapshot)?;
        self.snapshot = snapshot;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct DedupeStorageEnvelope {
    version: u16,
    checksum: [u8; 32],
    payload: Vec<u8>,
}

fn load_snapshot<T: DeserializeOwned>(
    path: &Path,
    max_snapshot_bytes: usize,
    label: &str,
) -> Result<Option<T>> {
    validate_snapshot_limit(max_snapshot_bytes)?;
    match fs::read(path) {
        Ok(bytes) => decode_snapshot(&bytes, max_snapshot_bytes, label).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(QuePaxaError::StorageError(format!(
            "could not read {label} {}: {error}",
            path.display()
        ))),
    }
}

fn encode_snapshot<T: Serialize>(
    snapshot: &T,
    max_snapshot_bytes: usize,
    label: &str,
) -> Result<Vec<u8>> {
    validate_snapshot_limit(max_snapshot_bytes)?;
    let payload = postcard::to_allocvec(snapshot).map_err(|error| {
        QuePaxaError::StorageError(format!("could not encode {label} payload: {error}"))
    })?;
    let bytes = postcard::to_allocvec(&DedupeStorageEnvelope {
        version: DEDUPE_STORAGE_VERSION,
        checksum: Sha256::digest(&payload).into(),
        payload,
    })
    .map_err(|error| QuePaxaError::StorageError(format!("could not encode {label}: {error}")))?;
    if bytes.len() > max_snapshot_bytes {
        return Err(QuePaxaError::ResourceLimit {
            resource: "durable deduplication bytes",
            limit: max_snapshot_bytes,
        });
    }
    Ok(bytes)
}

fn decode_snapshot<T: DeserializeOwned>(
    bytes: &[u8],
    max_snapshot_bytes: usize,
    label: &str,
) -> Result<T> {
    validate_snapshot_limit(max_snapshot_bytes)?;
    if bytes.is_empty() || bytes.len() > max_snapshot_bytes {
        return Err(QuePaxaError::StorageError(format!(
            "{label} size {} is outside the configured bound",
            bytes.len()
        )));
    }
    let envelope: DedupeStorageEnvelope = postcard::from_bytes(bytes).map_err(|error| {
        QuePaxaError::StorageError(format!("could not decode {label}: {error}"))
    })?;
    if envelope.version != DEDUPE_STORAGE_VERSION {
        return Err(QuePaxaError::StorageError(format!(
            "unsupported {label} version {}",
            envelope.version
        )));
    }
    if Sha256::digest(&envelope.payload).as_slice() != envelope.checksum {
        return Err(QuePaxaError::StorageError(format!(
            "{label} checksum verification failed"
        )));
    }
    postcard::from_bytes(&envelope.payload).map_err(|error| {
        QuePaxaError::StorageError(format!("could not decode {label} payload: {error}"))
    })
}

struct SnapshotTarget<'a> {
    path: &'a Path,
    temporary_extension: &'a str,
    max_snapshot_bytes: usize,
    crash_injector: &'a dyn CrashInjector,
    points: [CrashPoint; 5],
    label: &'a str,
}

fn save_snapshot<T: Serialize>(target: SnapshotTarget<'_>, snapshot: &T) -> Result<()> {
    let bytes = encode_snapshot(snapshot, target.max_snapshot_bytes, target.label)?;
    let temporary = target.path.with_extension(target.temporary_extension);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not open {} {}: {error}",
                target.label,
                temporary.display()
            ))
        })?;
    target.crash_injector.reached(target.points[0])?;
    file.write_all(&bytes).map_err(|error| {
        QuePaxaError::StorageError(format!(
            "could not write {} {}: {error}",
            target.label,
            temporary.display()
        ))
    })?;
    target.crash_injector.reached(target.points[1])?;
    file.sync_all().map_err(|error| {
        QuePaxaError::StorageError(format!(
            "could not sync {} {}: {error}",
            target.label,
            temporary.display()
        ))
    })?;
    target.crash_injector.reached(target.points[2])?;
    fs::rename(&temporary, target.path).map_err(|error| {
        QuePaxaError::StorageError(format!(
            "could not replace {} {}: {error}",
            target.label,
            target.path.display()
        ))
    })?;
    target.crash_injector.reached(target.points[3])?;
    sync_parent(target.path, target.label)?;
    target.crash_injector.reached(target.points[4])
}

fn validate_snapshot_limit(max_snapshot_bytes: usize) -> Result<()> {
    if max_snapshot_bytes == 0 {
        return Err(QuePaxaError::StorageError(
            "maximum deduplication snapshot size must be non-zero".into(),
        ));
    }
    Ok(())
}

fn sync_parent(path: &Path, label: &str) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not sync {label} directory {}: {error}",
                parent.display()
            ))
        })
}

/// Serializes same-node retries around a durable submission journal.
pub struct DeduplicatingSubmissionHandler<H, J> {
    inner: H,
    journal: Mutex<J>,
    request_locks: Mutex<RequestLocks>,
}

impl<H, J> DeduplicatingSubmissionHandler<H, J> {
    pub fn new(inner: H, journal: J) -> Self {
        Self {
            inner,
            journal: Mutex::new(journal),
            request_locks: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<V, H, J> SubmissionHandler<V> for DeduplicatingSubmissionHandler<H, J>
where
    V: Clone + PartialEq + Send + 'static,
    H: SubmissionHandler<V>,
    J: SubmissionJournal<V> + 'static,
{
    fn submit(&self, submission: Submission<V>) -> BoxSubmissionFuture<'_, V> {
        Box::pin(async move {
            let key = (submission.client_id, submission.request_id);
            let request_lock = {
                let mut locks = self.request_locks.lock().await;
                Arc::clone(locks.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))))
            };
            let request_guard = request_lock.lock().await;
            let result = async {
                if let Some(outcome) = self
                    .journal
                    .lock()
                    .await
                    .get(submission.client_id, submission.request_id)?
                {
                    return Ok(SubmissionOutcome::Duplicate(match outcome {
                        SubmissionOutcome::Committed(decision) => Some(decision),
                        SubmissionOutcome::Duplicate(decision) => decision,
                        SubmissionOutcome::Accepted => None,
                    }));
                }
                let client_id = submission.client_id;
                let request_id = submission.request_id;
                let outcome = self.inner.submit(submission).await?;
                self.journal
                    .lock()
                    .await
                    .put(client_id, request_id, outcome.clone())?;
                Ok(outcome)
            }
            .await;
            drop(request_guard);

            let mut locks = self.request_locks.lock().await;
            if Arc::strong_count(&request_lock) == 2
                && locks
                    .get(&key)
                    .is_some_and(|stored| Arc::ptr_eq(stored, &request_lock))
            {
                locks.remove(&key);
            }
            result
        })
    }

    fn receive_decisions(&self, decisions: Vec<Decision<V>>) -> BoxFuture<'_, Result<()>> {
        self.inner.receive_decisions(decisions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ReplicaId, Step};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn scratch_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "quepaxa-{name}-{}-{nonce}.state",
            std::process::id()
        ))
    }

    fn decision(slot: u64, value_id: u64) -> Decision<u64> {
        Decision::new(
            SlotIndex::new(slot),
            vec![value_id],
            ReplicaId::new(1),
            Step::ROUND_ONE_PHASE_ZERO,
        )
        .unwrap()
    }

    #[test]
    fn file_submission_journal_survives_restart_and_rejects_conflicts() {
        let path = scratch_path("submission-journal");
        let mut journal = FileSubmissionJournal::open(&path, 1024 * 1024).unwrap();
        let outcome = SubmissionOutcome::Committed(decision(1, 9));
        journal.put([3; 16], 7, outcome.clone()).unwrap();

        let mut restored = FileSubmissionJournal::open(&path, 1024 * 1024).unwrap();
        assert_eq!(restored.get([3; 16], 7).unwrap(), Some(outcome.clone()));
        restored.put([3; 16], 7, outcome).unwrap();
        assert!(matches!(
            restored.put([3; 16], 7, SubmissionOutcome::Accepted),
            Err(QuePaxaError::InvalidProposal(_))
        ));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_exactly_once_executor_commits_state_and_id_together() {
        let path = scratch_path("exactly-once");
        let apply = |state: &mut BTreeMap<u64, u64>, slot: SlotIndex, value_id: &u64| {
            state.insert(*value_id, slot.get());
            Ok(())
        };
        let mut executor =
            FileExactlyOnceExecutor::open(&path, 1024 * 1024, BTreeMap::new(), apply).unwrap();
        assert!(executor.execute_once(SlotIndex::new(1), &11).unwrap());
        assert!(!executor.execute_once(SlotIndex::new(2), &11).unwrap());
        assert_eq!(executor.application().get(&11), Some(&1));

        let apply = |state: &mut BTreeMap<u64, u64>, slot: SlotIndex, value_id: &u64| {
            state.insert(*value_id, slot.get());
            Ok(())
        };
        let mut restored =
            FileExactlyOnceExecutor::open(&path, 1024 * 1024, BTreeMap::new(), apply).unwrap();
        assert_eq!(restored.application().get(&11), Some(&1));
        assert!(!restored.execute_once(SlotIndex::new(3), &11).unwrap());

        let checkpoint = restored.export_checkpoint(SlotIndex::new(1)).unwrap();
        let target_path = scratch_path("exactly-once-target");
        let apply = |state: &mut BTreeMap<u64, u64>, slot: SlotIndex, value_id: &u64| {
            state.insert(*value_id, slot.get());
            Ok(())
        };
        let mut target =
            FileExactlyOnceExecutor::open(&target_path, 1024 * 1024, BTreeMap::new(), apply)
                .unwrap();
        target
            .import_checkpoint(SlotIndex::new(1), &checkpoint)
            .unwrap();
        assert_eq!(target.application().get(&11), Some(&1));
        assert!(target.seen().contains(&11));

        fs::remove_file(path).unwrap();
        fs::remove_file(target_path).unwrap();
    }

    #[test]
    fn dedupe_snapshots_reject_corruption() {
        let path = scratch_path("submission-corruption");
        let mut journal = FileSubmissionJournal::<u64>::open(&path, 1024 * 1024).unwrap();
        journal
            .put([1; 16], 1, SubmissionOutcome::Accepted)
            .unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            FileSubmissionJournal::<u64>::open(&path, 1024 * 1024),
            Err(QuePaxaError::StorageError(_))
        ));
        fs::remove_file(path).unwrap();
    }
}
