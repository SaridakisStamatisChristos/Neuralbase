// SPDX-License-Identifier: Apache-2.0
//! Durable Raft stable storage backed by NeuralBase's RocksDB meta column family.
//!
//! Term, voted-for, log, snapshot-boundary metadata, and active snapshot bytes
//! are written in one RocksDB WriteBatch. Snapshot lifecycle transitions are
//! staged separately with their purpose and exact Raft boundary. Publishing an
//! active snapshot atomically writes state/snapshot and deletes that staging key.

use std::sync::Arc;

use rocksdb::WriteBatch;

use crate::consensus::{PersistentState, RaftPersistenceStore, StagedSnapshot, StagedSnapshotKind};
use crate::storage::{StorageEngine, CF_META};

const RAFT_STATE_KEY: &[u8] = b"raft/core/persistent-state-v1";
const RAFT_SNAPSHOT_KEY: &[u8] = b"raft/core/snapshot-v1";
const RAFT_STAGED_SNAPSHOT_KEY: &[u8] = b"raft/core/snapshot-staged-v1";
const STAGED_MAGIC: &[u8; 4] = b"NBST";
const STAGED_VERSION: u8 = 1;
const STAGED_HEADER_BYTES: usize = 4 + 1 + 1 + 8 + 8;

pub struct RocksDbRaftPersistenceStore {
    engine: Arc<StorageEngine>,
}

impl RocksDbRaftPersistenceStore {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }
}

fn encode_staged_snapshot(snapshot: &StagedSnapshot) -> Vec<u8> {
    let kind = match snapshot.kind {
        StagedSnapshotKind::Creation => 1u8,
        StagedSnapshotKind::Installation => 2u8,
    };
    let mut encoded = Vec::with_capacity(STAGED_HEADER_BYTES + snapshot.data.len());
    encoded.extend_from_slice(STAGED_MAGIC);
    encoded.push(STAGED_VERSION);
    encoded.push(kind);
    encoded.extend_from_slice(&snapshot.last_included_index.to_be_bytes());
    encoded.extend_from_slice(&snapshot.last_included_term.to_be_bytes());
    encoded.extend_from_slice(snapshot.data.as_slice());
    encoded
}

fn decode_staged_snapshot(bytes: &[u8]) -> Result<StagedSnapshot, String> {
    if bytes.len() < STAGED_HEADER_BYTES {
        return Err(format!(
            "truncated staged Raft snapshot metadata: {} bytes",
            bytes.len()
        ));
    }
    if &bytes[..4] != STAGED_MAGIC {
        return Err("invalid staged Raft snapshot magic".to_string());
    }
    if bytes[4] != STAGED_VERSION {
        return Err(format!(
            "unsupported staged Raft snapshot version {}",
            bytes[4]
        ));
    }
    let kind = match bytes[5] {
        1 => StagedSnapshotKind::Creation,
        2 => StagedSnapshotKind::Installation,
        value => return Err(format!("invalid staged Raft snapshot kind {value}")),
    };
    let last_included_index = u64::from_be_bytes(
        bytes[6..14]
            .try_into()
            .map_err(|_| "decode staged snapshot index".to_string())?,
    );
    let last_included_term = u64::from_be_bytes(
        bytes[14..22]
            .try_into()
            .map_err(|_| "decode staged snapshot term".to_string())?,
    );
    Ok(StagedSnapshot {
        kind,
        last_included_index,
        last_included_term,
        data: Arc::new(bytes[STAGED_HEADER_BYTES..].to_vec()),
    })
}

impl RaftPersistenceStore for RocksDbRaftPersistenceStore {
    fn save(&self, state: &PersistentState, snapshot_data: &[u8]) -> Result<(), String> {
        let encoded = serde_json::to_vec(state)
            .map_err(|error| format!("serialize Raft persistent state: {error}"))?;
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| "CF_META unavailable while persisting Raft state".to_string())?;

        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, RAFT_STATE_KEY, encoded);
        batch.put_cf(&meta_cf, RAFT_SNAPSHOT_KEY, snapshot_data);
        batch.delete_cf(&meta_cf, RAFT_STAGED_SNAPSHOT_KEY);
        self.engine
            .write_batch(batch)
            .map_err(|error| format!("persist Raft state to RocksDB: {error}"))
    }

    fn stage_snapshot(&self, snapshot: &StagedSnapshot) -> Result<(), String> {
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| "CF_META unavailable while staging Raft snapshot".to_string())?;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            &meta_cf,
            RAFT_STAGED_SNAPSHOT_KEY,
            encode_staged_snapshot(snapshot),
        );
        self.engine
            .write_batch(batch)
            .map_err(|error| format!("stage Raft snapshot in RocksDB: {error}"))
    }

    fn load_staged_snapshot(&self) -> Result<Option<StagedSnapshot>, String> {
        let meta_cf =
            self.engine.db.cf_handle(CF_META).ok_or_else(|| {
                "CF_META unavailable while loading staged Raft snapshot".to_string()
            })?;
        self.engine
            .db
            .get_cf(&meta_cf, RAFT_STAGED_SNAPSHOT_KEY)
            .map_err(|error| format!("read staged Raft snapshot: {error}"))?
            .map(|bytes| decode_staged_snapshot(&bytes))
            .transpose()
    }

    fn clear_staged_snapshot(&self) -> Result<(), String> {
        let meta_cf =
            self.engine.db.cf_handle(CF_META).ok_or_else(|| {
                "CF_META unavailable while clearing staged Raft snapshot".to_string()
            })?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(&meta_cf, RAFT_STAGED_SNAPSHOT_KEY);
        self.engine
            .write_batch(batch)
            .map_err(|error| format!("clear staged Raft snapshot: {error}"))
    }

    fn load(&self) -> Result<Option<(PersistentState, Vec<u8>)>, String> {
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| "CF_META unavailable while loading Raft state".to_string())?;
        let state_bytes = self
            .engine
            .db
            .get_cf(&meta_cf, RAFT_STATE_KEY)
            .map_err(|error| format!("read Raft persistent state: {error}"))?;
        let snapshot_bytes = self
            .engine
            .db
            .get_cf(&meta_cf, RAFT_SNAPSHOT_KEY)
            .map_err(|error| format!("read Raft snapshot bytes: {error}"))?;

        let Some(state_bytes) = state_bytes else {
            if snapshot_bytes.is_some() {
                return Err("Raft snapshot exists without persistent state".to_string());
            }
            return Ok(None);
        };

        let state: PersistentState = serde_json::from_slice(&state_bytes)
            .map_err(|error| format!("decode Raft persistent state: {error}"))?;
        Ok(Some((state, snapshot_bytes.unwrap_or_default())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn rocksdb_raft_store_roundtrips_term_vote_log_and_snapshot() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let store = RocksDbRaftPersistenceStore::new(engine);

        let mut state = PersistentState::new();
        state.current_term = 11;
        state.voted_for = Some("node-b".to_string());
        state.append(11, b"mutation-1".to_vec());
        state.append(11, b"mutation-2".to_vec());

        store.save(&state, b"snapshot-bytes").unwrap();
        let (loaded, snapshot) = store.load().unwrap().unwrap();
        assert_eq!(loaded.current_term, 11);
        assert_eq!(loaded.voted_for.as_deref(), Some("node-b"));
        assert_eq!(loaded.last_log_index(), 2);
        assert_eq!(loaded.log[1].command, b"mutation-1");
        assert_eq!(loaded.log[2].command, b"mutation-2");
        assert_eq!(snapshot, b"snapshot-bytes");
    }

    #[test]
    fn staged_snapshot_roundtrips_and_is_removed_by_atomic_publish() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
        let staged = StagedSnapshot {
            kind: StagedSnapshotKind::Installation,
            last_included_index: 17,
            last_included_term: 4,
            data: Arc::new(b"candidate".to_vec()),
        };

        store.stage_snapshot(&staged).unwrap();
        assert_eq!(store.load_staged_snapshot().unwrap(), Some(staged));

        store.save(&PersistentState::new(), b"active").unwrap();
        assert!(store.load_staged_snapshot().unwrap().is_none());
        let meta_cf = engine.db.cf_handle(CF_META).unwrap();
        assert_eq!(
            engine
                .db
                .get_cf(&meta_cf, RAFT_SNAPSHOT_KEY)
                .unwrap()
                .as_deref(),
            Some(b"active".as_slice())
        );
    }

    #[test]
    fn clear_staged_snapshot_discards_unpublished_candidate() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let store = RocksDbRaftPersistenceStore::new(engine);
        let staged = StagedSnapshot {
            kind: StagedSnapshotKind::Creation,
            last_included_index: 9,
            last_included_term: 3,
            data: Arc::new(b"candidate".to_vec()),
        };
        store.stage_snapshot(&staged).unwrap();
        store.clear_staged_snapshot().unwrap();
        assert!(store.load_staged_snapshot().unwrap().is_none());
    }

    #[test]
    fn fresh_rocksdb_has_no_raft_state() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let store = RocksDbRaftPersistenceStore::new(engine);
        assert!(store.load().unwrap().is_none());
    }
}
