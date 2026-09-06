// SPDX-License-Identifier: Apache-2.0
//! Durable Raft stable storage backed by NeuralBase's RocksDB meta column family.
//!
//! Term, voted-for, log, snapshot boundary metadata, and snapshot bytes are
//! written in one RocksDB WriteBatch. The store itself reports errors; clustered
//! runtime wiring wraps it in `FailClosedPersistenceStore` so a required
//! persistence failure terminates the Raft node instead of being ignored.

use std::sync::Arc;

use rocksdb::WriteBatch;

use crate::consensus::{PersistentState, RaftPersistenceStore};
use crate::storage::{StorageEngine, CF_META};

const RAFT_STATE_KEY: &[u8] = b"raft/core/persistent-state-v1";
const RAFT_SNAPSHOT_KEY: &[u8] = b"raft/core/snapshot-v1";

pub struct RocksDbRaftPersistenceStore {
    engine: Arc<StorageEngine>,
}

impl RocksDbRaftPersistenceStore {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }
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
        self.engine
            .write_batch(batch)
            .map_err(|error| format!("persist Raft state to RocksDB: {error}"))
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
    fn fresh_rocksdb_has_no_raft_state() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let store = RocksDbRaftPersistenceStore::new(engine);
        assert!(store.load().unwrap().is_none());
    }
}
