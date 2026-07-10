use crate::crash::{CrashInjector, CrashPoint, NoopCrashInjector};
use crate::error::{QuePaxaError, Result};
use crate::runtime::StateTransferSnapshot;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

const BATCH_STORAGE_VERSION: u16 = 2;

/// One disseminated command batch. `value_id` is the compact value agreed by
/// consensus; `payload` is retained outside the consensus critical path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch<V> {
    pub value_id: V,
    pub payload: Vec<u8>,
}

/// Verifies that a payload is the content named by its value ID. A typical
/// implementation compares `value_id` with a cryptographic digest of
/// `payload`, or verifies an application signature over both.
pub trait BatchVerifier<V>: Send + Sync + 'static {
    fn verify(&self, value_id: &V, payload: &[u8]) -> Result<()>;
}

impl<V, F> BatchVerifier<V> for F
where
    F: Fn(&V, &[u8]) -> Result<()> + Send + Sync + 'static,
{
    fn verify(&self, value_id: &V, payload: &[u8]) -> Result<()> {
        self(value_id, payload)
    }
}

/// Storage contract used by the authenticated batch service.
pub trait BatchStore<V>: Send + 'static {
    fn put(&mut self, value_id: V, payload: Vec<u8>) -> Result<()>;
    fn get(&self, value_id: &V) -> Result<Option<Vec<u8>>>;
    fn remove_many(&mut self, value_ids: &[V]) -> Result<usize>;
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryBatchStore<V> {
    values: BTreeMap<V, Vec<u8>>,
}

impl<V: Ord + Send + 'static> BatchStore<V> for InMemoryBatchStore<V> {
    fn put(&mut self, value_id: V, payload: Vec<u8>) -> Result<()> {
        if let Some(existing) = self.values.get(&value_id) {
            if existing != &payload {
                return Err(QuePaxaError::InvalidProposal(
                    "one batch ID was published with different payloads".into(),
                ));
            }
            return Ok(());
        }
        self.values.insert(value_id, payload);
        Ok(())
    }

    fn get(&self, value_id: &V) -> Result<Option<Vec<u8>>> {
        Ok(self.values.get(value_id).cloned())
    }

    fn remove_many(&mut self, value_ids: &[V]) -> Result<usize> {
        let before = self.values.len();
        self.values
            .retain(|value_id, _| !value_ids.contains(value_id));
        Ok(before - self.values.len())
    }
}

#[derive(Serialize, Deserialize)]
struct BatchStorageEnvelope {
    version: u16,
    checksum: [u8; 32],
    payload: Vec<u8>,
}

/// Atomic, fsynced batch-content store. A conflicting second payload for the
/// same ID is rejected; acknowledged publishes survive process restart.
pub struct FileBatchStore<V> {
    path: PathBuf,
    values: BTreeMap<V, Vec<u8>>,
    max_snapshot_bytes: usize,
    marker: PhantomData<fn(V)>,
    crash_injector: Arc<dyn CrashInjector>,
}

impl<V> FileBatchStore<V>
where
    V: Clone + Ord + Serialize + DeserializeOwned,
{
    pub fn open(path: impl Into<PathBuf>, max_snapshot_bytes: usize) -> Result<Self> {
        if max_snapshot_bytes == 0 {
            return Err(QuePaxaError::StorageError(
                "maximum batch snapshot size must be non-zero".into(),
            ));
        }
        let path = path.into();
        let values = match fs::read(&path) {
            Ok(bytes) => {
                if bytes.is_empty() || bytes.len() > max_snapshot_bytes {
                    return Err(QuePaxaError::StorageError(format!(
                        "batch snapshot size {} is outside the configured bound",
                        bytes.len()
                    )));
                }
                let envelope: BatchStorageEnvelope =
                    postcard::from_bytes(&bytes).map_err(|error| {
                        QuePaxaError::StorageError(format!(
                            "could not decode batch snapshot: {error}"
                        ))
                    })?;
                if envelope.version != BATCH_STORAGE_VERSION {
                    return Err(QuePaxaError::StorageError(format!(
                        "unsupported batch storage version {}",
                        envelope.version
                    )));
                }
                if Sha256::digest(&envelope.payload).as_slice() != envelope.checksum {
                    return Err(QuePaxaError::StorageError(
                        "batch snapshot checksum verification failed".into(),
                    ));
                }
                postcard::from_bytes(&envelope.payload).map_err(|error| {
                    QuePaxaError::StorageError(format!(
                        "could not decode batch snapshot payload: {error}"
                    ))
                })?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => {
                return Err(QuePaxaError::StorageError(format!(
                    "could not read batch snapshot {}: {error}",
                    path.display()
                )));
            }
        };
        Ok(Self {
            path,
            values,
            max_snapshot_bytes,
            marker: PhantomData,
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

    fn save(&self, values: &BTreeMap<V, Vec<u8>>) -> Result<()> {
        let payload = postcard::to_allocvec(values).map_err(|error| {
            QuePaxaError::StorageError(format!("could not encode batch values: {error}"))
        })?;
        let bytes = postcard::to_allocvec(&BatchStorageEnvelope {
            version: BATCH_STORAGE_VERSION,
            checksum: Sha256::digest(&payload).into(),
            payload,
        })
        .map_err(|error| {
            QuePaxaError::StorageError(format!("could not encode batch snapshot: {error}"))
        })?;
        if bytes.len() > self.max_snapshot_bytes {
            return Err(QuePaxaError::ResourceLimit {
                resource: "durable batch bytes",
                limit: self.max_snapshot_bytes,
            });
        }
        let temporary = self.path.with_extension("batches.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                QuePaxaError::StorageError(format!(
                    "could not open batch snapshot {}: {error}",
                    temporary.display()
                ))
            })?;
        self.crash_injector
            .reached(CrashPoint::BatchTemporaryOpened)?;
        file.write_all(&bytes).map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not write batch snapshot {}: {error}",
                temporary.display()
            ))
        })?;
        self.crash_injector
            .reached(CrashPoint::BatchTemporaryWritten)?;
        file.sync_all().map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not sync batch snapshot {}: {error}",
                temporary.display()
            ))
        })?;
        self.crash_injector
            .reached(CrashPoint::BatchTemporarySynced)?;
        fs::rename(&temporary, &self.path).map_err(|error| {
            QuePaxaError::StorageError(format!(
                "could not replace batch snapshot {}: {error}",
                self.path.display()
            ))
        })?;
        self.crash_injector.reached(CrashPoint::BatchRenamed)?;
        sync_parent(&self.path)?;
        self.crash_injector
            .reached(CrashPoint::BatchDirectorySynced)
    }
}

impl<V> BatchStore<V> for FileBatchStore<V>
where
    V: Clone + Ord + Serialize + DeserializeOwned + Send + 'static,
{
    fn put(&mut self, value_id: V, payload: Vec<u8>) -> Result<()> {
        if let Some(existing) = self.values.get(&value_id) {
            if existing != &payload {
                return Err(QuePaxaError::InvalidProposal(
                    "one batch ID was published with different payloads".into(),
                ));
            }
            return Ok(());
        }
        let mut next = self.values.clone();
        next.insert(value_id, payload);
        self.save(&next)?;
        self.values = next;
        Ok(())
    }

    fn get(&self, value_id: &V) -> Result<Option<Vec<u8>>> {
        Ok(self.values.get(value_id).cloned())
    }

    fn remove_many(&mut self, value_ids: &[V]) -> Result<usize> {
        let mut next = self.values.clone();
        let before = next.len();
        next.retain(|value_id, _| !value_ids.contains(value_id));
        let removed = before - next.len();
        if removed > 0 {
            self.save(&next)?;
            self.values = next;
        }
        Ok(removed)
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
                "could not sync batch directory {}: {error}",
                parent.display()
            ))
        })
}

#[derive(Debug, Clone, Copy)]
pub struct BatchServiceLimits {
    pub max_batches_per_request: usize,
    pub max_batch_bytes: usize,
    pub max_fetch_ids: usize,
}

impl Default for BatchServiceLimits {
    fn default() -> Self {
        Self {
            max_batches_per_request: 256,
            max_batch_bytes: 4 * 1024 * 1024,
            max_fetch_ids: 1024,
        }
    }
}

/// Type-erased service used by the network server after mTLS identity checks.
pub trait BatchService<V>: Send + Sync + 'static {
    fn publish(&self, batches: Vec<Batch<V>>) -> Result<()>;
    fn fetch(&self, value_ids: Vec<V>) -> Result<Vec<Batch<V>>>;
}

impl<V, B> BatchService<V> for Arc<B>
where
    B: BatchService<V>,
{
    fn publish(&self, batches: Vec<Batch<V>>) -> Result<()> {
        self.as_ref().publish(batches)
    }

    fn fetch(&self, value_ids: Vec<V>) -> Result<Vec<Batch<V>>> {
        self.as_ref().fetch(value_ids)
    }
}

pub struct AuthenticatedBatchService<V, S, F> {
    store: Mutex<S>,
    verifier: F,
    limits: BatchServiceLimits,
    marker: PhantomData<fn(V)>,
}

impl<V, S, F> AuthenticatedBatchService<V, S, F> {
    pub fn new(store: S, verifier: F, limits: BatchServiceLimits) -> Result<Self> {
        if limits.max_batches_per_request == 0
            || limits.max_batch_bytes == 0
            || limits.max_fetch_ids == 0
        {
            return Err(QuePaxaError::InvalidProposal(
                "batch service limits must be non-zero".into(),
            ));
        }
        Ok(Self {
            store: Mutex::new(store),
            verifier,
            limits,
            marker: PhantomData,
        })
    }
}

impl<V, S, F> AuthenticatedBatchService<V, S, F>
where
    V: Ord + Send + 'static,
    S: BatchStore<V>,
{
    /// Deletes payloads whose decided commands are covered by a durable
    /// application checkpoint. This is deliberately local-only: callers must
    /// establish checkpoint safety before invoking it.
    pub fn prune_checkpointed(&self, value_ids: &[V]) -> Result<usize> {
        self.store
            .lock()
            .map_err(|_| QuePaxaError::StorageError("batch store lock was poisoned".into()))?
            .remove_many(value_ids)
    }

    /// Deletes exactly the values captured while creating a durable matching
    /// application/consensus checkpoint. This prevents retention integrations
    /// from rebuilding an incomplete pruning set after the decision log has
    /// already been compacted.
    pub fn prune_state_transfer(&self, transfer: &StateTransferSnapshot<V>) -> Result<usize> {
        if transfer.through != transfer.runtime.replica.checkpointed_through {
            return Err(QuePaxaError::InvalidProposal(
                "state-transfer checkpoint does not match the runtime pruning floor".into(),
            ));
        }
        self.prune_checkpointed(&transfer.checkpointed_value_ids)
    }
}

impl<V, S, F> BatchService<V> for AuthenticatedBatchService<V, S, F>
where
    V: Clone + Ord + Send + Sync + 'static,
    S: BatchStore<V>,
    F: BatchVerifier<V>,
{
    fn publish(&self, batches: Vec<Batch<V>>) -> Result<()> {
        if batches.is_empty() || batches.len() > self.limits.max_batches_per_request {
            return Err(QuePaxaError::ResourceLimit {
                resource: "batches in one publish request",
                limit: self.limits.max_batches_per_request,
            });
        }
        let mut seen = BTreeSet::new();
        for batch in &batches {
            if batch.payload.is_empty() || batch.payload.len() > self.limits.max_batch_bytes {
                return Err(QuePaxaError::ResourceLimit {
                    resource: "bytes in one published batch",
                    limit: self.limits.max_batch_bytes,
                });
            }
            if !seen.insert(batch.value_id.clone()) {
                return Err(QuePaxaError::InvalidProposal(
                    "batch publish request contains duplicate IDs".into(),
                ));
            }
            self.verifier.verify(&batch.value_id, &batch.payload)?;
        }
        let mut store = self
            .store
            .lock()
            .map_err(|_| QuePaxaError::StorageError("batch store lock was poisoned".into()))?;
        for batch in batches {
            store.put(batch.value_id, batch.payload)?;
        }
        Ok(())
    }

    fn fetch(&self, value_ids: Vec<V>) -> Result<Vec<Batch<V>>> {
        if value_ids.is_empty() || value_ids.len() > self.limits.max_fetch_ids {
            return Err(QuePaxaError::ResourceLimit {
                resource: "IDs in one batch fetch request",
                limit: self.limits.max_fetch_ids,
            });
        }
        if value_ids.iter().cloned().collect::<BTreeSet<_>>().len() != value_ids.len() {
            return Err(QuePaxaError::InvalidProposal(
                "batch fetch request contains duplicate IDs".into(),
            ));
        }
        let store = self
            .store
            .lock()
            .map_err(|_| QuePaxaError::StorageError("batch store lock was poisoned".into()))?;
        value_ids
            .into_iter()
            .map(|value_id| {
                let payload = store.get(&value_id)?.ok_or(QuePaxaError::MissingValue)?;
                Ok(Batch { value_id, payload })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn scratch_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "quepaxa-batch-checksum-{}-{nonce}.state",
            std::process::id()
        ))
    }

    #[test]
    fn durable_batch_store_checksums_and_prunes_atomically() {
        let path = scratch_path();
        let mut store = FileBatchStore::<u64>::open(&path, 1024 * 1024).unwrap();
        store.put(1, b"one".to_vec()).unwrap();
        store.put(2, b"two".to_vec()).unwrap();
        assert_eq!(store.remove_many(&[1]).unwrap(), 1);
        drop(store);

        let restored = FileBatchStore::<u64>::open(&path, 1024 * 1024).unwrap();
        assert_eq!(restored.get(&1).unwrap(), None);
        assert_eq!(restored.get(&2).unwrap(), Some(b"two".to_vec()));
        drop(restored);

        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            FileBatchStore::<u64>::open(&path, 1024 * 1024),
            Err(QuePaxaError::StorageError(_))
        ));
        fs::remove_file(path).unwrap();
    }
}
