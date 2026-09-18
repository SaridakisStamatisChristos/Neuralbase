// SPDX-License-Identifier: Apache-2.0
//! Durable Raft stable storage backed by NeuralBase's RocksDB meta column family.
//!
//! Phase 11 v2 splits the hot Raft persistence path into a compact hard-state
//! record plus one RocksDB key per log entry. Normal leader/follower appends now
//! write only newly appended entries instead of serializing and rewriting the
//! complete Raft log on every command. The historical v1 whole-state blob is
//! retained as a read-only migration source and is removed on the first v2 save.
//! Snapshot bytes remain a separate object and are rewritten only when the
//! snapshot boundary changes.

use std::sync::{Arc, Mutex};

use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};

use crate::consensus::{
    ClusterMembership, LogEntry, PersistentState, RaftPersistenceStore, StagedSnapshot,
    StagedSnapshotKind,
};
use crate::storage::{StorageEngine, CF_META};

/// Historical Phase-2/3 representation. Read only for in-place migration.
const RAFT_STATE_KEY_V1: &[u8] = b"raft/core/persistent-state-v1";
/// Compact v2 hard state. The log itself is stored under `RAFT_LOG_PREFIX_V2`.
const RAFT_HARD_STATE_KEY_V2: &[u8] = b"raft/core/hard-state-v2";
const RAFT_LOG_PREFIX_V2: &[u8] = b"raft/core/log-v2/";
const RAFT_SNAPSHOT_KEY: &[u8] = b"raft/core/snapshot-v1";
const RAFT_STAGED_SNAPSHOT_KEY: &[u8] = b"raft/core/snapshot-staged-v1";

const STAGED_MAGIC: &[u8; 4] = b"NBST";
const STAGED_VERSION: u8 = 1;
const STAGED_HEADER_BYTES: usize = 4 + 1 + 1 + 8 + 8;

const LOG_ENTRY_MAGIC: &[u8; 4] = b"NBL2";
const LOG_ENTRY_VERSION: u8 = 1;
const LOG_ENTRY_HEADER_BYTES: usize = 4 + 1 + 8 + 8 + 4;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PersistedHardStateV2 {
    current_term: u64,
    voted_for: Option<String>,
    snapshot_index: u64,
    snapshot_term: u64,
    membership: Option<ClusterMembership>,
    last_log_index: u64,
    last_log_term: u64,
    snapshot_bytes_len: u64,
}

impl PersistedHardStateV2 {
    fn from_state(state: &PersistentState, snapshot_data: &[u8]) -> Self {
        Self {
            current_term: state.current_term,
            voted_for: state.voted_for.clone(),
            snapshot_index: state.snapshot_index,
            snapshot_term: state.snapshot_term,
            membership: state.membership.clone(),
            last_log_index: state.last_log_index(),
            last_log_term: state.last_log_term(),
            snapshot_bytes_len: snapshot_data.len() as u64,
        }
    }
}

pub struct RocksDbRaftPersistenceStore {
    engine: Arc<StorageEngine>,
    /// Avoid a RocksDB metadata lookup on every hot-path append. The cache is
    /// only published after the corresponding WriteBatch succeeds.
    cached_hard: Mutex<Option<PersistedHardStateV2>>,
}

impl RocksDbRaftPersistenceStore {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self {
            engine,
            cached_hard: Mutex::new(None),
        }
    }

    fn meta_cf(&self) -> Result<Arc<rocksdb::BoundColumnFamily<'_>>, String> {
        self.engine
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| "CF_META unavailable while accessing Raft persistence".to_string())
    }

    fn load_hard_v2(&self) -> Result<Option<PersistedHardStateV2>, String> {
        let meta_cf = self.meta_cf()?;
        self.engine
            .db
            .get_cf(&meta_cf, RAFT_HARD_STATE_KEY_V2)
            .map_err(|error| format!("read Raft v2 hard state: {error}"))?
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(|error| format!("decode Raft v2 hard state: {error}"))
            })
            .transpose()
    }

    fn current_hard_v2(&self) -> Result<Option<PersistedHardStateV2>, String> {
        if let Some(cached) = self
            .cached_hard
            .lock()
            .map_err(|error| format!("Raft hard-state cache poisoned: {error}"))?
            .clone()
        {
            return Ok(Some(cached));
        }
        let loaded = self.load_hard_v2()?;
        if let Some(hard) = &loaded {
            *self
                .cached_hard
                .lock()
                .map_err(|error| format!("Raft hard-state cache poisoned: {error}"))? =
                Some(hard.clone());
        }
        Ok(loaded)
    }

    fn set_cached_hard(&self, hard: PersistedHardStateV2) -> Result<(), String> {
        *self
            .cached_hard
            .lock()
            .map_err(|error| format!("Raft hard-state cache poisoned: {error}"))? = Some(hard);
        Ok(())
    }
}

fn raft_log_key(index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(RAFT_LOG_PREFIX_V2.len() + 8);
    key.extend_from_slice(RAFT_LOG_PREFIX_V2);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn encode_log_entry(entry: &LogEntry) -> Result<Vec<u8>, String> {
    let command_len = u32::try_from(entry.command.len())
        .map_err(|_| format!("Raft log entry {} command exceeds u32", entry.index))?;
    let mut encoded = Vec::with_capacity(LOG_ENTRY_HEADER_BYTES + entry.command.len());
    encoded.extend_from_slice(LOG_ENTRY_MAGIC);
    encoded.push(LOG_ENTRY_VERSION);
    encoded.extend_from_slice(&entry.term.to_be_bytes());
    encoded.extend_from_slice(&entry.index.to_be_bytes());
    encoded.extend_from_slice(&command_len.to_be_bytes());
    encoded.extend_from_slice(&entry.command);
    Ok(encoded)
}

fn decode_log_entry(expected_index: u64, bytes: &[u8]) -> Result<LogEntry, String> {
    if bytes.len() < LOG_ENTRY_HEADER_BYTES {
        return Err(format!(
            "truncated Raft v2 log entry {expected_index}: {} bytes",
            bytes.len()
        ));
    }
    if &bytes[..4] != LOG_ENTRY_MAGIC {
        return Err(format!(
            "invalid Raft v2 log magic at index {expected_index}"
        ));
    }
    if bytes[4] != LOG_ENTRY_VERSION {
        return Err(format!(
            "unsupported Raft v2 log version {} at index {expected_index}",
            bytes[4]
        ));
    }
    let term = u64::from_be_bytes(
        bytes[5..13]
            .try_into()
            .map_err(|_| format!("decode Raft v2 log term at {expected_index}"))?,
    );
    let index = u64::from_be_bytes(
        bytes[13..21]
            .try_into()
            .map_err(|_| format!("decode Raft v2 log index at {expected_index}"))?,
    );
    if index != expected_index {
        return Err(format!(
            "Raft v2 log key/index mismatch: key={expected_index}, entry={index}"
        ));
    }
    let command_len = u32::from_be_bytes(
        bytes[21..25]
            .try_into()
            .map_err(|_| format!("decode Raft v2 command length at {expected_index}"))?,
    ) as usize;
    if bytes.len() != LOG_ENTRY_HEADER_BYTES + command_len {
        return Err(format!(
            "Raft v2 log entry {expected_index} length mismatch: header says {command_len}, total={} bytes",
            bytes.len()
        ));
    }
    Ok(LogEntry {
        term,
        index,
        command: bytes[LOG_ENTRY_HEADER_BYTES..].to_vec(),
    })
}

fn state_entry(state: &PersistentState, index: u64) -> Option<&LogEntry> {
    if index < state.snapshot_index {
        return None;
    }
    let physical = (index - state.snapshot_index) as usize;
    state.log.get(physical)
}

fn put_log_range(
    batch: &mut WriteBatch,
    meta_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
    state: &PersistentState,
    first: u64,
    last: u64,
) -> Result<(), String> {
    if first > last {
        return Ok(());
    }
    for index in first..=last {
        let entry = state_entry(state, index)
            .ok_or_else(|| format!("Raft state missing log entry {index} while persisting v2"))?;
        // The snapshot sentinel is metadata, never a separate v2 log key.
        if index == state.snapshot_index {
            continue;
        }
        batch.put_cf(meta_cf, raft_log_key(index), encode_log_entry(entry)?);
    }
    Ok(())
}

fn delete_log_range(
    batch: &mut WriteBatch,
    meta_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
    first: u64,
    last: u64,
) {
    if first > last {
        return;
    }
    for index in first..=last {
        batch.delete_cf(meta_cf, raft_log_key(index));
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
        let meta_cf = self.meta_cf()?;
        let previous = self.current_hard_v2()?;
        let next = PersistedHardStateV2::from_state(state, snapshot_data);
        let encoded_hard = serde_json::to_vec(&next)
            .map_err(|error| format!("serialize Raft v2 hard state: {error}"))?;

        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, RAFT_HARD_STATE_KEY_V2, encoded_hard);
        // Once v2 is durable the whole-state v1 blob must never be consulted.
        batch.delete_cf(&meta_cf, RAFT_STATE_KEY_V1);
        // A successful publish completes any staged snapshot transition.
        batch.delete_cf(&meta_cf, RAFT_STAGED_SNAPSHOT_KEY);

        match &previous {
            None => {
                // One-time migration/fresh initialization: materialize the suffix once.
                put_log_range(
                    &mut batch,
                    &meta_cf,
                    state,
                    state.snapshot_index.saturating_add(1),
                    state.last_log_index(),
                )?;
                batch.put_cf(&meta_cf, RAFT_SNAPSHOT_KEY, snapshot_data);
            }
            Some(old) if old.snapshot_index != next.snapshot_index => {
                // Snapshot transitions are rare. Rewrite the retained suffix and remove
                // keys that have moved behind the new compacted boundary.
                delete_log_range(
                    &mut batch,
                    &meta_cf,
                    old.snapshot_index.saturating_add(1),
                    old.last_log_index,
                );
                put_log_range(
                    &mut batch,
                    &meta_cf,
                    state,
                    state.snapshot_index.saturating_add(1),
                    state.last_log_index(),
                )?;
                batch.put_cf(&meta_cf, RAFT_SNAPSHOT_KEY, snapshot_data);
            }
            Some(old) => {
                let prefix_still_matches = old.last_log_index <= next.last_log_index
                    && state.term_at(old.last_log_index) == old.last_log_term;

                if prefix_still_matches {
                    // Normal hot path: append only entries the previous durable state did
                    // not contain. Hard-state-only updates write zero log entries.
                    put_log_range(
                        &mut batch,
                        &meta_cf,
                        state,
                        old.last_log_index.saturating_add(1),
                        next.last_log_index,
                    )?;
                } else {
                    // Conflict/truncation is uncommon and correctness dominates. Rebuild
                    // the current suffix rather than trying to infer the conflict point.
                    delete_log_range(
                        &mut batch,
                        &meta_cf,
                        state.snapshot_index.saturating_add(1),
                        old.last_log_index,
                    );
                    put_log_range(
                        &mut batch,
                        &meta_cf,
                        state,
                        state.snapshot_index.saturating_add(1),
                        next.last_log_index,
                    )?;
                }

                // Snapshot bytes are immutable for a fixed Raft snapshot boundary.
                // A length change is nevertheless treated defensively as a publish.
                if old.snapshot_bytes_len != next.snapshot_bytes_len {
                    batch.put_cf(&meta_cf, RAFT_SNAPSHOT_KEY, snapshot_data);
                }
            }
        }

        self.engine
            .write_batch(batch)
            .map_err(|error| format!("persist Raft v2 state to RocksDB: {error}"))?;
        self.set_cached_hard(next)
    }

    fn stage_snapshot(&self, snapshot: &StagedSnapshot) -> Result<(), String> {
        let meta_cf = self.meta_cf()?;
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
        let meta_cf = self.meta_cf()?;
        self.engine
            .db
            .get_cf(&meta_cf, RAFT_STAGED_SNAPSHOT_KEY)
            .map_err(|error| format!("read staged Raft snapshot: {error}"))?
            .map(|bytes| decode_staged_snapshot(&bytes))
            .transpose()
    }

    fn clear_staged_snapshot(&self) -> Result<(), String> {
        let meta_cf = self.meta_cf()?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(&meta_cf, RAFT_STAGED_SNAPSHOT_KEY);
        self.engine
            .write_batch(batch)
            .map_err(|error| format!("clear staged Raft snapshot: {error}"))
    }

    fn load(&self) -> Result<Option<(PersistentState, Vec<u8>)>, String> {
        let meta_cf = self.meta_cf()?;
        if let Some(hard) = self.load_hard_v2()? {
            if hard.last_log_index < hard.snapshot_index {
                return Err(format!(
                    "Raft v2 hard state end {} precedes snapshot boundary {}",
                    hard.last_log_index, hard.snapshot_index
                ));
            }

            let mut log = vec![LogEntry {
                term: hard.snapshot_term,
                index: hard.snapshot_index,
                command: vec![],
            }];
            for index in hard.snapshot_index.saturating_add(1)..=hard.last_log_index {
                let bytes = self
                    .engine
                    .db
                    .get_cf(&meta_cf, raft_log_key(index))
                    .map_err(|error| format!("read Raft v2 log entry {index}: {error}"))?
                    .ok_or_else(|| format!("missing Raft v2 log entry {index}"))?;
                log.push(decode_log_entry(index, &bytes)?);
            }

            let state = PersistentState {
                current_term: hard.current_term,
                voted_for: hard.voted_for.clone(),
                log,
                snapshot_index: hard.snapshot_index,
                snapshot_term: hard.snapshot_term,
                membership: hard.membership.clone(),
            };
            if state.last_log_term() != hard.last_log_term {
                return Err(format!(
                    "Raft v2 hard/log term mismatch at {}: hard={}, log={}",
                    hard.last_log_index,
                    hard.last_log_term,
                    state.last_log_term()
                ));
            }
            let snapshot = self
                .engine
                .db
                .get_cf(&meta_cf, RAFT_SNAPSHOT_KEY)
                .map_err(|error| format!("read Raft snapshot bytes: {error}"))?
                .unwrap_or_default();
            if snapshot.len() as u64 != hard.snapshot_bytes_len {
                return Err(format!(
                    "Raft v2 snapshot length mismatch: hard={}, stored={}",
                    hard.snapshot_bytes_len,
                    snapshot.len()
                ));
            }
            self.set_cached_hard(hard)?;
            return Ok(Some((state, snapshot)));
        }

        // Backward-compatible v1 load. The next save atomically migrates it to v2.
        let state_bytes = self
            .engine
            .db
            .get_cf(&meta_cf, RAFT_STATE_KEY_V1)
            .map_err(|error| format!("read legacy Raft persistent state: {error}"))?;
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
            .map_err(|error| format!("decode legacy Raft persistent state: {error}"))?;
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
        let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));

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

        let meta_cf = engine.db.cf_handle(CF_META).unwrap();
        assert!(engine
            .db
            .get_cf(&meta_cf, RAFT_STATE_KEY_V1)
            .unwrap()
            .is_none());
        assert!(engine
            .db
            .get_cf(&meta_cf, RAFT_HARD_STATE_KEY_V2)
            .unwrap()
            .is_some());
    }

    #[test]
    fn hot_append_adds_one_v2_log_record_without_legacy_blob() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
        let mut state = PersistentState::new();
        state.current_term = 3;
        state.append(3, b"one".to_vec());
        store.save(&state, &[]).unwrap();

        state.append(3, b"two".to_vec());
        store.save(&state, &[]).unwrap();

        let meta_cf = engine.db.cf_handle(CF_META).unwrap();
        assert!(engine
            .db
            .get_cf(&meta_cf, raft_log_key(1))
            .unwrap()
            .is_some());
        assert!(engine
            .db
            .get_cf(&meta_cf, raft_log_key(2))
            .unwrap()
            .is_some());
        assert!(engine
            .db
            .get_cf(&meta_cf, raft_log_key(3))
            .unwrap()
            .is_none());
        assert!(engine
            .db
            .get_cf(&meta_cf, RAFT_STATE_KEY_V1)
            .unwrap()
            .is_none());
        let (loaded, _) = store.load().unwrap().unwrap();
        assert_eq!(loaded.log[1].command, b"one");
        assert_eq!(loaded.log[2].command, b"two");
    }

    #[test]
    fn conflicting_suffix_is_replaced_and_reloads_exactly() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let store = RocksDbRaftPersistenceStore::new(engine);
        let mut state = PersistentState::new();
        state.append(1, b"a".to_vec());
        state.append(1, b"b".to_vec());
        state.append(2, b"old".to_vec());
        store.save(&state, &[]).unwrap();

        state.truncate_and_append(
            1,
            vec![LogEntry {
                term: 3,
                index: 2,
                command: b"new".to_vec(),
            }],
        );
        store.save(&state, &[]).unwrap();
        let (loaded, _) = store.load().unwrap().unwrap();
        assert_eq!(loaded.last_log_index(), 2);
        assert_eq!(loaded.term_at(2), 3);
        assert_eq!(loaded.log[2].command, b"new");
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
