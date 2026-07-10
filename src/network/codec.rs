use crate::error::{QuePaxaError, Result};
use crate::recorder::{RecorderCodec, RecorderSnapshot};
use crate::runtime::{RuntimeCodec, RuntimeSnapshot, StateTransferSnapshot};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::marker::PhantomData;

pub const STORAGE_VERSION: u16 = 5;
const DEFAULT_MAX_SNAPSHOT: usize = 64 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct StorageEnvelope {
    version: u16,
    checksum: [u8; 32],
    payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PostcardRecorderCodec<V> {
    max_snapshot_bytes: usize,
    marker: PhantomData<fn(V)>,
}

impl<V> Default for PostcardRecorderCodec<V> {
    fn default() -> Self {
        Self {
            max_snapshot_bytes: DEFAULT_MAX_SNAPSHOT,
            marker: PhantomData,
        }
    }
}

impl<V> PostcardRecorderCodec<V> {
    pub fn with_max_snapshot_bytes(mut self, max_snapshot_bytes: usize) -> Result<Self> {
        self.max_snapshot_bytes = validate_limit(max_snapshot_bytes)?;
        Ok(self)
    }
}

impl<V> RecorderCodec<V> for PostcardRecorderCodec<V>
where
    V: Ord + Serialize + DeserializeOwned,
{
    fn encode(&self, snapshot: &RecorderSnapshot<V>) -> Result<Vec<u8>> {
        encode_snapshot(snapshot, self.max_snapshot_bytes)
    }

    fn decode(&self, bytes: &[u8]) -> Result<RecorderSnapshot<V>> {
        decode_snapshot(bytes, self.max_snapshot_bytes)
    }
}

#[derive(Debug, Clone)]
pub struct PostcardRuntimeCodec<V> {
    max_snapshot_bytes: usize,
    marker: PhantomData<fn(V)>,
}

impl<V> Default for PostcardRuntimeCodec<V> {
    fn default() -> Self {
        Self {
            max_snapshot_bytes: DEFAULT_MAX_SNAPSHOT,
            marker: PhantomData,
        }
    }
}

impl<V> PostcardRuntimeCodec<V> {
    pub fn with_max_snapshot_bytes(mut self, max_snapshot_bytes: usize) -> Result<Self> {
        self.max_snapshot_bytes = validate_limit(max_snapshot_bytes)?;
        Ok(self)
    }
}

impl<V> RuntimeCodec<V> for PostcardRuntimeCodec<V>
where
    V: Ord + Serialize + DeserializeOwned,
{
    fn encode(&self, snapshot: &RuntimeSnapshot<V>) -> Result<Vec<u8>> {
        encode_snapshot(snapshot, self.max_snapshot_bytes)
    }

    fn decode(&self, bytes: &[u8]) -> Result<RuntimeSnapshot<V>> {
        decode_snapshot(bytes, self.max_snapshot_bytes)
    }
}

/// Bounded, versioned codec for portable state-transfer snapshots.
#[derive(Debug, Clone)]
pub struct PostcardStateTransferCodec<V> {
    max_snapshot_bytes: usize,
    marker: PhantomData<fn(V)>,
}

impl<V> Default for PostcardStateTransferCodec<V> {
    fn default() -> Self {
        Self {
            max_snapshot_bytes: DEFAULT_MAX_SNAPSHOT,
            marker: PhantomData,
        }
    }
}

impl<V> PostcardStateTransferCodec<V>
where
    V: Ord + Serialize + DeserializeOwned,
{
    pub fn with_max_snapshot_bytes(mut self, max_snapshot_bytes: usize) -> Result<Self> {
        self.max_snapshot_bytes = validate_limit(max_snapshot_bytes)?;
        Ok(self)
    }

    pub fn encode(&self, snapshot: &StateTransferSnapshot<V>) -> Result<Vec<u8>> {
        encode_snapshot(snapshot, self.max_snapshot_bytes)
    }

    pub fn decode(&self, bytes: &[u8]) -> Result<StateTransferSnapshot<V>> {
        decode_snapshot(bytes, self.max_snapshot_bytes)
    }
}

fn encode_snapshot<T: Serialize>(snapshot: &T, max_bytes: usize) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(snapshot).map_err(|error| {
        QuePaxaError::StorageError(format!("snapshot payload encoding failed: {error}"))
    })?;
    let checksum = Sha256::digest(&payload).into();
    let bytes = postcard::to_allocvec(&StorageEnvelope {
        version: STORAGE_VERSION,
        checksum,
        payload,
    })
    .map_err(|error| QuePaxaError::StorageError(format!("snapshot encoding failed: {error}")))?;
    if bytes.len() > max_bytes {
        return Err(QuePaxaError::StorageError(format!(
            "encoded snapshot is {} bytes, exceeding the configured limit {max_bytes}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn decode_snapshot<T: DeserializeOwned>(bytes: &[u8], max_bytes: usize) -> Result<T> {
    if bytes.is_empty() || bytes.len() > max_bytes {
        return Err(QuePaxaError::StorageError(format!(
            "snapshot size {} is outside the accepted range",
            bytes.len()
        )));
    }
    let envelope: StorageEnvelope = postcard::from_bytes(bytes).map_err(|error| {
        QuePaxaError::StorageError(format!("snapshot decoding failed: {error}"))
    })?;
    if envelope.version != STORAGE_VERSION {
        return Err(QuePaxaError::StorageError(format!(
            "unsupported snapshot version {}",
            envelope.version
        )));
    }
    if Sha256::digest(&envelope.payload).as_slice() != envelope.checksum {
        return Err(QuePaxaError::StorageError(
            "snapshot checksum verification failed".into(),
        ));
    }
    postcard::from_bytes(&envelope.payload).map_err(|error| {
        QuePaxaError::StorageError(format!("snapshot payload decoding failed: {error}"))
    })
}

fn validate_limit(limit: usize) -> Result<usize> {
    if limit == 0 {
        return Err(QuePaxaError::StorageError(
            "maximum snapshot size must be non-zero".into(),
        ));
    }
    Ok(limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::{ReplicaConfig, ReplicaCore};
    use crate::runtime::ProtocolIdentity;
    use crate::types::{ClusterIdentity, ReplicaId, SlotIndex};
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn postcard_codecs_round_trip_versioned_snapshots() {
        let cluster = ClusterIdentity::new([ReplicaId::new(1)], 0).unwrap();
        let recorder = RecorderSnapshot::<u64> {
            cluster: cluster.clone(),
            slots: BTreeMap::new(),
            round_one_leaders: BTreeMap::new(),
            decisions: BTreeMap::new(),
            pruned_through: SlotIndex::GENESIS,
        };
        let recorder_codec = PostcardRecorderCodec::default();
        let recorder_bytes = recorder_codec.encode(&recorder).unwrap();
        assert_eq!(recorder_codec.decode(&recorder_bytes).unwrap(), recorder);

        let runtime = RuntimeSnapshot::<u64> {
            cluster,
            protocol: ProtocolIdentity {
                epoch_size: 16,
                auto_schedules: false,
            },
            replica: ReplicaCore::new(ReplicaConfig::default()).snapshot(),
            decisions: BTreeMap::new(),
            schedules: BTreeMap::new(),
            pending: BTreeMap::new(),
            executed: BTreeSet::new(),
            notified: BTreeSet::new(),
            announced_to: BTreeMap::new(),
            epoch_stats: BTreeMap::new(),
            stats_through: SlotIndex::GENESIS,
        };
        let runtime_codec = PostcardRuntimeCodec::default();
        let runtime_bytes = runtime_codec.encode(&runtime).unwrap();
        assert_eq!(runtime_codec.decode(&runtime_bytes).unwrap(), runtime);

        let transfer = StateTransferSnapshot {
            through: SlotIndex::GENESIS,
            checkpointed_value_ids: vec![7],
            application_checkpoint: vec![1, 2, 3],
            runtime,
        };
        let transfer_codec = PostcardStateTransferCodec::default();
        let transfer_bytes = transfer_codec.encode(&transfer).unwrap();
        assert_eq!(transfer_codec.decode(&transfer_bytes).unwrap(), transfer);
    }

    #[test]
    fn postcard_codec_rejects_an_unknown_storage_version() {
        let snapshot = RecorderSnapshot::<u64> {
            cluster: ClusterIdentity::new([ReplicaId::new(1)], 0).unwrap(),
            slots: BTreeMap::new(),
            round_one_leaders: BTreeMap::new(),
            decisions: BTreeMap::new(),
            pruned_through: SlotIndex::GENESIS,
        };
        let codec = PostcardRecorderCodec::default();
        let mut bytes = codec.encode(&snapshot).unwrap();
        bytes[0] = STORAGE_VERSION as u8 + 1;

        assert!(matches!(
            codec.decode(&bytes),
            Err(QuePaxaError::StorageError(_))
        ));
    }

    #[test]
    fn postcard_codec_rejects_checksum_corruption() {
        let snapshot = RecorderSnapshot::<u64> {
            cluster: ClusterIdentity::new([ReplicaId::new(1)], 0).unwrap(),
            slots: BTreeMap::new(),
            round_one_leaders: BTreeMap::new(),
            decisions: BTreeMap::new(),
            pruned_through: SlotIndex::GENESIS,
        };
        let codec = PostcardRecorderCodec::default();
        let mut bytes = codec.encode(&snapshot).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;

        assert!(matches!(
            codec.decode(&bytes),
            Err(QuePaxaError::StorageError(_))
        ));
    }
}
