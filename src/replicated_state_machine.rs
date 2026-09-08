// SPDX-License-Identifier: Apache-2.0
//! Deterministic RocksDB-backed state machine for committed replicated SQL and identity.
//!
//! A committed Raft entry advances one durable apply cursor even when the entry
//! is a Raft control command rather than SQL. SQL and identity mutations are
//! decoded into deterministic concrete effects; non-state-machine entries only
//! advance the apply marker. Identity state and the apply cursor are written in
//! one RocksDB WriteBatch so crash/replay follows the same durable boundary as
//! replicated SQL.

use std::sync::Arc;

use rocksdb::WriteBatch;
use thiserror::Error;

use crate::catalog::{InMemoryCatalog, MutableCatalog};
use crate::consensus::rpc::LogEntry;
use crate::hlc::{HlcClock, HlcTimestamp};
use crate::replicated_identity::{
    is_replicated_identity_mutation, IdentityCodecError, ReplicatedIdentityMutation,
    ReplicatedIdentityUser,
};
use crate::replicated_identity_store::{
    IdentityStateError, ReplicatedIdentityState, REPLICATED_IDENTITY_STATE_KEY,
};
use crate::replicated_sql::{
    is_replicated_mutation, MutationCodecError, ReplicatedMutation, ReplicatedRowWrite,
};
use crate::storage::{
    encode_versioned_key, StorageEngine, StorageError, CF_CATALOG, CF_DATA, CF_META,
};
use crate::storage_executor::table_id_for;

const APPLY_STATE_KEY: &[u8] = b"raft/sql/apply-state-v1";
const APPLY_STATE_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplicatedApplyState {
    pub last_applied_index: u64,
    pub last_commit_ts: u64,
}

impl ReplicatedApplyState {
    fn encode(self) -> [u8; APPLY_STATE_BYTES] {
        let mut bytes = [0u8; APPLY_STATE_BYTES];
        bytes[..8].copy_from_slice(&self.last_applied_index.to_be_bytes());
        bytes[8..].copy_from_slice(&self.last_commit_ts.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, ReplicatedSqlApplyError> {
        if bytes.len() != APPLY_STATE_BYTES {
            return Err(ReplicatedSqlApplyError::CorruptApplyState(bytes.len()));
        }
        let index = u64::from_be_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| ReplicatedSqlApplyError::CorruptApplyState(bytes.len()))?,
        );
        let commit_ts = u64::from_be_bytes(
            bytes[8..]
                .try_into()
                .map_err(|_| ReplicatedSqlApplyError::CorruptApplyState(bytes.len()))?,
        );
        Ok(Self {
            last_applied_index: index,
            last_commit_ts: commit_ts,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatedApplyOutcome {
    IgnoredNonSql,
    AlreadyApplied {
        index: u64,
    },
    Applied {
        index: u64,
        command_tag: &'static str,
        affected_rows: Option<u64>,
    },
}

#[derive(Debug, Error)]
pub enum ReplicatedSqlApplyError {
    #[error("invalid replicated mutation: {0}")]
    Codec(#[from] MutationCodecError),
    #[error("invalid replicated identity mutation: {0}")]
    IdentityCodec(#[from] IdentityCodecError),
    #[error("invalid replicated identity state: {0}")]
    IdentityState(#[from] IdentityStateError),
    #[error("replicated identity mutation requires initialized authoritative identity state")]
    IdentityNotInitialized,
    #[error(
        "replicated identity initialization conflicts with existing authoritative identity state"
    )]
    ConflictingIdentityInitialization,
    #[error("storage failure while applying replicated SQL: {0}")]
    Storage(#[from] StorageError),
    #[error("rocksdb failure while reading replicated apply state: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("catalog serialization failed: {0}")]
    CatalogSerialization(#[from] serde_json::Error),
    #[error("corrupt replicated SQL apply state: expected 16 bytes, got {0}")]
    CorruptApplyState(usize),
    #[error(
        "replicated mutation table id mismatch for {table}: command={command_table_id}, derived={derived_table_id}"
    )]
    TableIdMismatch {
        table: String,
        command_table_id: u32,
        derived_table_id: u32,
    },
    #[error(
        "replicated DML timestamp {commit_ts} is not newer than durable timestamp {last_commit_ts}"
    )]
    NonMonotonicCommitTimestamp { commit_ts: u64, last_commit_ts: u64 },
}

pub struct ReplicatedSqlStateMachine {
    engine: Arc<StorageEngine>,
    catalog: Arc<InMemoryCatalog>,
    clock: Arc<HlcClock>,
}

impl ReplicatedSqlStateMachine {
    pub fn new(
        engine: Arc<StorageEngine>,
        catalog: Arc<InMemoryCatalog>,
        clock: Arc<HlcClock>,
    ) -> Result<Self, ReplicatedSqlApplyError> {
        let state_machine = Self {
            engine,
            catalog,
            clock,
        };
        let state = state_machine.load_state()?;
        if state.last_commit_ts != 0 {
            state_machine
                .clock
                .observe_committed(HlcTimestamp::from_u64(state.last_commit_ts));
        }
        Ok(state_machine)
    }

    pub fn apply_log_entry(
        &self,
        entry: &LogEntry,
    ) -> Result<ReplicatedApplyOutcome, ReplicatedSqlApplyError> {
        let previous = self.load_state()?;
        if entry.index <= previous.last_applied_index {
            return Ok(ReplicatedApplyOutcome::AlreadyApplied { index: entry.index });
        }
        if is_replicated_identity_mutation(&entry.command) {
            let mutation = ReplicatedIdentityMutation::decode(&entry.command)?;
            return self.apply_identity(entry.index, mutation, previous);
        }
        if !is_replicated_mutation(&entry.command) {
            self.advance_control_apply(entry.index, previous)?;
            return Ok(ReplicatedApplyOutcome::IgnoredNonSql);
        }
        let mutation = ReplicatedMutation::decode(&entry.command)?;
        self.apply_sql(entry.index, mutation, previous)
    }

    pub fn durable_state(&self) -> Result<ReplicatedApplyState, ReplicatedSqlApplyError> {
        self.load_state()
    }

    fn advance_control_apply(
        &self,
        raft_index: u64,
        previous: ReplicatedApplyState,
    ) -> Result<(), ReplicatedSqlApplyError> {
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .expect("CF_META must exist after StorageEngine::open");
        let mut batch = WriteBatch::default();
        batch.put_cf(
            &meta_cf,
            APPLY_STATE_KEY,
            ReplicatedApplyState {
                last_applied_index: raft_index,
                last_commit_ts: previous.last_commit_ts,
            }
            .encode(),
        );
        self.engine.write_batch(batch)?;
        Ok(())
    }

    fn apply_identity(
        &self,
        raft_index: u64,
        mutation: ReplicatedIdentityMutation,
        previous: ReplicatedApplyState,
    ) -> Result<ReplicatedApplyOutcome, ReplicatedSqlApplyError> {
        let state = match &mutation {
            ReplicatedIdentityMutation::Initialize { users } => {
                let requested = ReplicatedIdentityState::new(users.clone())?;
                match ReplicatedIdentityState::load(&self.engine)? {
                    None => requested,
                    Some(existing) if existing == requested => existing,
                    Some(_) => {
                        return Err(ReplicatedSqlApplyError::ConflictingIdentityInitialization)
                    }
                }
            }
            ReplicatedIdentityMutation::CreateUser {
                username,
                credential,
            } => {
                let mut state = ReplicatedIdentityState::load(&self.engine)?
                    .ok_or(ReplicatedSqlApplyError::IdentityNotInitialized)?;
                state.create_user(ReplicatedIdentityUser {
                    username: username.clone(),
                    credential: credential.clone(),
                })?;
                state
            }
            ReplicatedIdentityMutation::AlterUser {
                username,
                credential,
            } => {
                let mut state = ReplicatedIdentityState::load(&self.engine)?
                    .ok_or(ReplicatedSqlApplyError::IdentityNotInitialized)?;
                state.alter_user(ReplicatedIdentityUser {
                    username: username.clone(),
                    credential: credential.clone(),
                })?;
                state
            }
            ReplicatedIdentityMutation::DropUser {
                username,
                if_exists,
            } => {
                let mut state = ReplicatedIdentityState::load(&self.engine)?
                    .ok_or(ReplicatedSqlApplyError::IdentityNotInitialized)?;
                state.drop_user(username, *if_exists)?;
                state
            }
        };

        let encoded_state = state.encode()?;
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .expect("CF_META must exist after StorageEngine::open");
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, REPLICATED_IDENTITY_STATE_KEY, encoded_state);
        batch.put_cf(
            &meta_cf,
            APPLY_STATE_KEY,
            ReplicatedApplyState {
                last_applied_index: raft_index,
                last_commit_ts: previous.last_commit_ts,
            }
            .encode(),
        );
        self.engine.write_batch(batch)?;

        Ok(ReplicatedApplyOutcome::Applied {
            index: raft_index,
            command_tag: mutation.command_tag(),
            affected_rows: None,
        })
    }

    fn apply_sql(
        &self,
        raft_index: u64,
        mutation: ReplicatedMutation,
        previous: ReplicatedApplyState,
    ) -> Result<ReplicatedApplyOutcome, ReplicatedSqlApplyError> {
        if let Some(commit_ts) = mutation.commit_ts() {
            if commit_ts <= previous.last_commit_ts {
                return Err(ReplicatedSqlApplyError::NonMonotonicCommitTimestamp {
                    commit_ts,
                    last_commit_ts: previous.last_commit_ts,
                });
            }
        }

        validate_table_id(&mutation)?;

        let data_cf = self
            .engine
            .db
            .cf_handle(CF_DATA)
            .expect("CF_DATA must exist after StorageEngine::open");
        let catalog_cf = self
            .engine
            .db
            .cf_handle(CF_CATALOG)
            .expect("CF_CATALOG must exist after StorageEngine::open");
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .expect("CF_META must exist after StorageEngine::open");

        let mut batch = WriteBatch::default();
        match &mutation {
            ReplicatedMutation::CreateTable { schema } => {
                let serialized = serde_json::to_vec(schema)?;
                batch.put_cf(
                    &catalog_cf,
                    schema.name.to_lowercase().as_bytes(),
                    serialized,
                );
            }
            ReplicatedMutation::DropTable {
                table, table_id, ..
            } => {
                batch.delete_cf(&catalog_cf, table.to_lowercase().as_bytes());
                for (raw_key, _) in self.engine.raw_scan_table_versions(*table_id)? {
                    batch.delete_cf(&data_cf, raw_key);
                }
            }
            ReplicatedMutation::InsertRows {
                table_id,
                commit_ts,
                rows,
                ..
            }
            | ReplicatedMutation::UpdateRows {
                table_id,
                commit_ts,
                rows,
                ..
            } => {
                put_rows(&mut batch, &data_cf, *table_id, *commit_ts, rows);
            }
            ReplicatedMutation::DeleteRows {
                table_id,
                commit_ts,
                primary_keys,
                ..
            } => {
                let ts = HlcTimestamp::from_u64(*commit_ts);
                for primary_key in primary_keys {
                    batch.put_cf(
                        &data_cf,
                        encode_versioned_key(*table_id, primary_key, ts),
                        [],
                    );
                }
            }
        }

        let new_commit_ts = mutation.commit_ts().unwrap_or(previous.last_commit_ts);
        let new_state = ReplicatedApplyState {
            last_applied_index: raft_index,
            last_commit_ts: new_commit_ts,
        };
        batch.put_cf(&meta_cf, APPLY_STATE_KEY, new_state.encode());

        self.engine.write_batch(batch)?;

        match &mutation {
            ReplicatedMutation::CreateTable { schema } => self.catalog.create_table(schema.clone()),
            ReplicatedMutation::DropTable { table, .. } => self.catalog.drop_table(table),
            _ => {}
        }

        if let Some(commit_ts) = mutation.commit_ts() {
            self.clock
                .observe_committed(HlcTimestamp::from_u64(commit_ts));
        }

        Ok(ReplicatedApplyOutcome::Applied {
            index: raft_index,
            command_tag: mutation.command_tag(),
            affected_rows: mutation.affected_rows(),
        })
    }

    fn load_state(&self) -> Result<ReplicatedApplyState, ReplicatedSqlApplyError> {
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .expect("CF_META must exist after StorageEngine::open");
        match self.engine.db.get_cf(&meta_cf, APPLY_STATE_KEY)? {
            Some(bytes) => ReplicatedApplyState::decode(&bytes),
            None => Ok(ReplicatedApplyState::default()),
        }
    }
}

fn validate_table_id(mutation: &ReplicatedMutation) -> Result<(), ReplicatedSqlApplyError> {
    let (table, command_table_id) = match mutation {
        ReplicatedMutation::CreateTable { .. } => return Ok(()),
        ReplicatedMutation::DropTable {
            table, table_id, ..
        }
        | ReplicatedMutation::InsertRows {
            table, table_id, ..
        }
        | ReplicatedMutation::UpdateRows {
            table, table_id, ..
        }
        | ReplicatedMutation::DeleteRows {
            table, table_id, ..
        } => (table, *table_id),
    };
    let derived_table_id = table_id_for(table);
    if command_table_id != derived_table_id {
        return Err(ReplicatedSqlApplyError::TableIdMismatch {
            table: table.clone(),
            command_table_id,
            derived_table_id,
        });
    }
    Ok(())
}

fn put_rows(
    batch: &mut WriteBatch,
    data_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
    table_id: u32,
    commit_ts: u64,
    rows: &[ReplicatedRowWrite],
) {
    let ts = HlcTimestamp::from_u64(commit_ts);
    for row in rows {
        batch.put_cf(
            data_cf,
            encode_versioned_key(table_id, &row.primary_key, ts),
            &row.value,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef, TableSchema};
    use crate::replicated_identity::ReplicatedScramCredential;
    use crate::storage_executor::encode_row;
    use tempfile::TempDir;

    fn setup() -> (
        ReplicatedSqlStateMachine,
        Arc<StorageEngine>,
        Arc<InMemoryCatalog>,
        Arc<HlcClock>,
        TempDir,
    ) {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let sm = ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        )
        .unwrap();
        (sm, engine, catalog, clock, dir)
    }

    fn entry(index: u64, mutation: ReplicatedMutation) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            command: mutation.encode().unwrap(),
        }
    }

    fn identity_entry(index: u64, mutation: ReplicatedIdentityMutation) -> LogEntry {
        LogEntry {
            term: 1,
            index,
            command: mutation.encode().unwrap(),
        }
    }

    fn identity_credential(seed: u8) -> ReplicatedScramCredential {
        ReplicatedScramCredential {
            salt: vec![seed; 16],
            iterations: 4_096,
            stored_key: [seed.wrapping_add(1); 32],
            server_key: [seed.wrapping_add(2); 32],
        }
    }

    fn schema(name: &str) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "TEXT".to_string(),
            }],
        }
    }

    #[test]
    fn non_sql_entry_advances_durable_apply_index_without_changing_hlc_floor() {
        let (sm, _engine, _catalog, _clock, _dir) = setup();
        let control = LogEntry {
            term: 1,
            index: 4,
            command: b"raft-control".to_vec(),
        };
        assert_eq!(
            sm.apply_log_entry(&control).unwrap(),
            ReplicatedApplyOutcome::IgnoredNonSql
        );
        let state = sm.durable_state().unwrap();
        assert_eq!(state.last_applied_index, 4);
        assert_eq!(state.last_commit_ts, 0);
        assert_eq!(
            sm.apply_log_entry(&control).unwrap(),
            ReplicatedApplyOutcome::AlreadyApplied { index: 4 }
        );
    }

    #[test]
    fn identity_apply_is_atomic_with_durable_cursor_and_replay_safe() {
        let (sm, engine, _catalog, _clock, _dir) = setup();
        sm.apply_log_entry(&identity_entry(
            1,
            ReplicatedIdentityMutation::Initialize { users: vec![] },
        ))
        .unwrap();

        let create = identity_entry(
            2,
            ReplicatedIdentityMutation::CreateUser {
                username: "alice".to_string(),
                credential: identity_credential(1),
            },
        );
        assert!(matches!(
            sm.apply_log_entry(&create).unwrap(),
            ReplicatedApplyOutcome::Applied {
                index: 2,
                command_tag: "CREATE USER",
                ..
            }
        ));
        let state = ReplicatedIdentityState::load(&engine).unwrap().unwrap();
        assert!(state.contains_user("alice"));
        assert_eq!(sm.durable_state().unwrap().last_applied_index, 2);
        assert_eq!(sm.durable_state().unwrap().last_commit_ts, 0);

        assert_eq!(
            sm.apply_log_entry(&create).unwrap(),
            ReplicatedApplyOutcome::AlreadyApplied { index: 2 }
        );
        assert_eq!(
            ReplicatedIdentityState::load(&engine)
                .unwrap()
                .unwrap()
                .users()
                .len(),
            1
        );

        sm.apply_log_entry(&identity_entry(
            3,
            ReplicatedIdentityMutation::AlterUser {
                username: "alice".to_string(),
                credential: identity_credential(9),
            },
        ))
        .unwrap();
        let altered = ReplicatedIdentityState::load(&engine)
            .unwrap()
            .unwrap()
            .user_record("alice")
            .unwrap()
            .unwrap();
        assert!(matches!(
            altered.credential,
            crate::auth::StoredCredential::ScramSha256(keys) if keys.stored_key == [10; 32]
        ));

        sm.apply_log_entry(&identity_entry(
            4,
            ReplicatedIdentityMutation::DropUser {
                username: "alice".to_string(),
                if_exists: false,
            },
        ))
        .unwrap();
        assert!(!ReplicatedIdentityState::load(&engine)
            .unwrap()
            .unwrap()
            .contains_user("alice"));
        assert_eq!(sm.durable_state().unwrap().last_applied_index, 4);
    }

    #[test]
    fn identity_initialize_retry_is_idempotent_but_conflict_fails_closed() {
        let (sm, engine, _catalog, _clock, _dir) = setup();
        let users = vec![ReplicatedIdentityUser {
            username: "alice".to_string(),
            credential: identity_credential(1),
        }];
        sm.apply_log_entry(&identity_entry(
            1,
            ReplicatedIdentityMutation::Initialize {
                users: users.clone(),
            },
        ))
        .unwrap();
        sm.apply_log_entry(&identity_entry(
            2,
            ReplicatedIdentityMutation::Initialize { users },
        ))
        .unwrap();
        assert_eq!(sm.durable_state().unwrap().last_applied_index, 2);
        assert!(ReplicatedIdentityState::load(&engine)
            .unwrap()
            .unwrap()
            .contains_user("alice"));

        let error = sm
            .apply_log_entry(&identity_entry(
                3,
                ReplicatedIdentityMutation::Initialize {
                    users: vec![ReplicatedIdentityUser {
                        username: "bob".to_string(),
                        credential: identity_credential(2),
                    }],
                },
            ))
            .unwrap_err();
        assert!(matches!(
            error,
            ReplicatedSqlApplyError::ConflictingIdentityInitialization
        ));
        assert_eq!(sm.durable_state().unwrap().last_applied_index, 2);
    }

    #[test]
    fn committed_insert_replay_is_idempotent() {
        let (sm, engine, catalog, clock, _dir) = setup();
        let table = "events";
        let tid = table_id_for(table);

        sm.apply_log_entry(&entry(
            1,
            ReplicatedMutation::CreateTable {
                schema: schema(table),
            },
        ))
        .unwrap();
        assert!(catalog.get_table(table).is_some());

        let commit_ts = HlcTimestamp {
            wall_ms: 10_000,
            logical: 1,
        }
        .to_u64();
        let insert = entry(
            2,
            ReplicatedMutation::InsertRows {
                table: table.to_string(),
                table_id: tid,
                commit_ts,
                rows: vec![ReplicatedRowWrite {
                    primary_key: b"pk1".to_vec(),
                    value: encode_row(&[("id", "1")]),
                }],
            },
        );

        assert!(matches!(
            sm.apply_log_entry(&insert).unwrap(),
            ReplicatedApplyOutcome::Applied { index: 2, .. }
        ));
        assert_eq!(engine.raw_scan_table_versions(tid).unwrap().len(), 1);

        assert_eq!(
            sm.apply_log_entry(&insert).unwrap(),
            ReplicatedApplyOutcome::AlreadyApplied { index: 2 }
        );
        assert_eq!(engine.raw_scan_table_versions(tid).unwrap().len(), 1);
        assert_eq!(sm.durable_state().unwrap().last_applied_index, 2);
        assert!(clock.now() >= HlcTimestamp::from_u64(commit_ts));
    }

    #[test]
    fn update_uses_exact_committed_timestamp_and_value() {
        let (sm, engine, _catalog, _clock, _dir) = setup();
        let table = "items";
        let tid = table_id_for(table);
        sm.apply_log_entry(&entry(
            1,
            ReplicatedMutation::CreateTable {
                schema: schema(table),
            },
        ))
        .unwrap();

        let ts1 = HlcTimestamp {
            wall_ms: 20_000,
            logical: 1,
        }
        .to_u64();
        let ts2 = HlcTimestamp {
            wall_ms: 20_000,
            logical: 2,
        }
        .to_u64();
        sm.apply_log_entry(&entry(
            2,
            ReplicatedMutation::InsertRows {
                table: table.to_string(),
                table_id: tid,
                commit_ts: ts1,
                rows: vec![ReplicatedRowWrite {
                    primary_key: b"pk".to_vec(),
                    value: b"before".to_vec(),
                }],
            },
        ))
        .unwrap();
        sm.apply_log_entry(&entry(
            3,
            ReplicatedMutation::UpdateRows {
                table: table.to_string(),
                table_id: tid,
                commit_ts: ts2,
                rows: vec![ReplicatedRowWrite {
                    primary_key: b"pk".to_vec(),
                    value: b"after".to_vec(),
                }],
            },
        ))
        .unwrap();

        let rows = engine.scan_table(tid, HlcTimestamp::MAX).unwrap();
        assert_eq!(rows, vec![(b"pk".to_vec(), b"after".to_vec())]);
        let versions = engine.raw_scan_table_versions(tid).unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions[1]
            .0
            .ends_with(&HlcTimestamp::from_u64(ts2).to_be_bytes()));
    }

    #[test]
    fn drop_table_durably_removes_schema_and_rows() {
        let (sm, engine, catalog, _clock, _dir) = setup();
        let table = "dropme";
        let tid = table_id_for(table);
        sm.apply_log_entry(&entry(
            1,
            ReplicatedMutation::CreateTable {
                schema: schema(table),
            },
        ))
        .unwrap();
        let ts = HlcTimestamp {
            wall_ms: 30_000,
            logical: 1,
        }
        .to_u64();
        sm.apply_log_entry(&entry(
            2,
            ReplicatedMutation::InsertRows {
                table: table.to_string(),
                table_id: tid,
                commit_ts: ts,
                rows: vec![ReplicatedRowWrite {
                    primary_key: b"pk".to_vec(),
                    value: b"value".to_vec(),
                }],
            },
        ))
        .unwrap();
        sm.apply_log_entry(&entry(
            3,
            ReplicatedMutation::DropTable {
                table: table.to_string(),
                table_id: tid,
            },
        ))
        .unwrap();

        assert!(catalog.get_table(table).is_none());
        assert!(engine.read_catalog_entry(table).unwrap().is_none());
        assert!(engine.raw_scan_table_versions(tid).unwrap().is_empty());
        assert_eq!(sm.durable_state().unwrap().last_applied_index, 3);
    }

    #[test]
    fn restart_restores_durable_commit_timestamp_into_hlc() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let sm = ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        )
        .unwrap();
        let table = "restartable";
        let tid = table_id_for(table);
        sm.apply_log_entry(&entry(
            1,
            ReplicatedMutation::CreateTable {
                schema: schema(table),
            },
        ))
        .unwrap();
        let commit_ts = HlcTimestamp {
            wall_ms: 40_000,
            logical: 7,
        }
        .to_u64();
        sm.apply_log_entry(&entry(
            2,
            ReplicatedMutation::InsertRows {
                table: table.to_string(),
                table_id: tid,
                commit_ts,
                rows: vec![],
            },
        ))
        .unwrap();
        drop(sm);
        drop(clock);

        let recovered_clock = Arc::new(HlcClock::new(500));
        let recovered = ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&recovered_clock),
        )
        .unwrap();
        assert_eq!(recovered.durable_state().unwrap().last_commit_ts, commit_ts);
        assert!(recovered_clock.now() >= HlcTimestamp::from_u64(commit_ts));
    }
}
