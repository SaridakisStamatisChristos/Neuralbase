// SPDX-License-Identifier: Apache-2.0
//! Logical replicated-SQL snapshot export and restore against RocksDB.
//!
//! Export reads the durable SQL apply marker, catalog, and table rows through one
//! RocksDB snapshot so the artifact represents one consistent storage point.
//! Restore validates the complete artifact before mutating storage, then replaces
//! SQL catalog/data plus the durable apply marker in one RocksDB WriteBatch. The
//! in-memory catalog and HLC floor are published only after that batch succeeds.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use rocksdb::WriteBatch;
use thiserror::Error;

use crate::catalog::{InMemoryCatalog, TableSchema};
use crate::hlc::{HlcClock, HlcTimestamp};
use crate::replicated_snapshot::{
    ReplicatedSqlSnapshot, SnapshotCodecError, SnapshotMetadata, SnapshotRow, SnapshotTable,
};
use crate::storage::{
    decode_pk_from_key, decode_ts_from_key, encode_versioned_key, StorageEngine, StorageError,
    CF_CATALOG, CF_DATA, CF_META,
};
use crate::storage_executor::table_id_for;

// This is the durable contract owned by `replicated_state_machine.rs`. Keep the
// bytes/encoding identical: [last_applied_index: u64 BE][last_commit_ts: u64 BE].
const APPLY_STATE_KEY: &[u8] = b"raft/sql/apply-state-v1";
const APPLY_STATE_BYTES: usize = 16;
const INDEX_SENTINEL_PREFIX: &[u8] = b"__idx:";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DurableApplyState {
    last_applied_index: u64,
    last_commit_ts: u64,
}

impl DurableApplyState {
    fn decode(bytes: &[u8]) -> Result<Self, SnapshotManagerError> {
        if bytes.len() != APPLY_STATE_BYTES {
            return Err(SnapshotManagerError::CorruptApplyState(bytes.len()));
        }
        let last_applied_index = u64::from_be_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| SnapshotManagerError::CorruptApplyState(bytes.len()))?,
        );
        let last_commit_ts = u64::from_be_bytes(
            bytes[8..]
                .try_into()
                .map_err(|_| SnapshotManagerError::CorruptApplyState(bytes.len()))?,
        );
        Ok(Self {
            last_applied_index,
            last_commit_ts,
        })
    }

    fn encode(self) -> [u8; APPLY_STATE_BYTES] {
        let mut out = [0u8; APPLY_STATE_BYTES];
        out[..8].copy_from_slice(&self.last_applied_index.to_be_bytes());
        out[8..].copy_from_slice(&self.last_commit_ts.to_be_bytes());
        out
    }
}

#[derive(Debug, Error)]
pub enum SnapshotManagerError {
    #[error("invalid replicated SQL snapshot: {0}")]
    Codec(#[from] SnapshotCodecError),
    #[error("storage failure during replicated SQL snapshot operation: {0}")]
    Storage(#[from] StorageError),
    #[error("rocksdb failure during replicated SQL snapshot operation: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("catalog serialization failure during replicated SQL snapshot operation: {0}")]
    CatalogSerialization(#[from] serde_json::Error),
    #[error("corrupt replicated SQL apply state: expected 16 bytes, got {0}")]
    CorruptApplyState(usize),
    #[error(
        "cannot snapshot Raft index {last_included_index}: durable SQL apply index {latest_sql_apply_index} is newer"
    )]
    ApplyIndexBeyondRequestedSnapshot {
        latest_sql_apply_index: u64,
        last_included_index: u64,
    },
    #[error("catalog contains a non-UTF-8 key")]
    InvalidCatalogKey,
    #[error("catalog key {key:?} does not match normalized schema name {expected:?}")]
    CatalogKeyMismatch { key: String, expected: String },
    #[error("secondary index state is not yet supported by replicated SQL snapshots: {0}")]
    UnsupportedSecondaryIndex(String),
    #[error("table id collision between {first:?} and {second:?}: {table_id}")]
    TableIdCollision {
        table_id: u32,
        first: String,
        second: String,
    },
    #[error("malformed MVCC data key while snapshotting table {table}: {key_len} bytes")]
    MalformedDataKey { table: String, key_len: usize },
    #[error(
        "MVCC version timestamp {row_ts} for table {table} is newer than durable replicated HLC {last_commit_ts}"
    )]
    RowBeyondReplicatedTimestamp {
        table: String,
        row_ts: u64,
        last_commit_ts: u64,
    },
    #[error("table {table} contains row data but durable replicated commit timestamp is zero")]
    RowDataWithoutCommitTimestamp { table: String },
    #[error("snapshot contains a tombstone/empty value as a live row for table {table}")]
    SnapshotContainsTombstone { table: String },
    #[error(
        "restore target contains secondary index column families and cannot be replaced safely"
    )]
    RestoreTargetHasSecondaryIndexes,
    #[error(
        "snapshot SQL apply index {snapshot_index} would regress durable target apply index {existing_index}"
    )]
    RestoreWouldRegressApplyIndex {
        existing_index: u64,
        snapshot_index: u64,
    },
    #[error(
        "snapshot commit timestamp {snapshot_timestamp} would regress durable target timestamp {existing_timestamp}"
    )]
    RestoreWouldRegressCommitTimestamp {
        existing_timestamp: u64,
        snapshot_timestamp: u64,
    },
    #[cfg(test)]
    #[error("injected snapshot restore storage failure")]
    InjectedStorageFailure,
}

pub struct ReplicatedSqlSnapshotManager {
    engine: Arc<StorageEngine>,
    catalog: Arc<InMemoryCatalog>,
    clock: Arc<HlcClock>,
}

impl ReplicatedSqlSnapshotManager {
    pub fn new(
        engine: Arc<StorageEngine>,
        catalog: Arc<InMemoryCatalog>,
        clock: Arc<HlcClock>,
    ) -> Self {
        Self {
            engine,
            catalog,
            clock,
        }
    }

    /// Export one canonical logical SQL snapshot from a single RocksDB snapshot.
    ///
    /// `last_included_index`/`last_included_term` come from Raft. The durable SQL
    /// apply marker may be lower when the Raft prefix also contains non-SQL
    /// commands, but it must never be newer than the requested snapshot index.
    pub fn export(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<Vec<u8>, SnapshotManagerError> {
        let db_snapshot = self.engine.db.snapshot();
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .expect("CF_META must exist after StorageEngine::open");
        let catalog_cf = self
            .engine
            .db
            .cf_handle(CF_CATALOG)
            .expect("CF_CATALOG must exist after StorageEngine::open");
        let data_cf = self
            .engine
            .db
            .cf_handle(CF_DATA)
            .expect("CF_DATA must exist after StorageEngine::open");

        let apply_state = match db_snapshot.get_cf(&meta_cf, APPLY_STATE_KEY)? {
            Some(bytes) => DurableApplyState::decode(&bytes)?,
            None => DurableApplyState::default(),
        };
        if apply_state.last_applied_index > last_included_index {
            return Err(SnapshotManagerError::ApplyIndexBeyondRequestedSnapshot {
                latest_sql_apply_index: apply_state.last_applied_index,
                last_included_index,
            });
        }

        let schemas = read_catalog(&db_snapshot, &catalog_cf)?;
        let mut ids = HashMap::<u32, String>::with_capacity(schemas.len());
        let mut tables = Vec::with_capacity(schemas.len());
        for schema in schemas {
            let table_id = table_id_for(&schema.name);
            if let Some(first) = ids.insert(table_id, schema.name.clone()) {
                return Err(SnapshotManagerError::TableIdCollision {
                    table_id,
                    first,
                    second: schema.name,
                });
            }
            let rows = read_logical_rows(
                &db_snapshot,
                &data_cf,
                &schema.name,
                table_id,
                apply_state.last_commit_ts,
            )?;
            tables.push(SnapshotTable {
                schema,
                table_id,
                rows,
            });
        }

        ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index,
                last_included_term,
                latest_sql_apply_index: apply_state.last_applied_index,
                latest_commit_ts: apply_state.last_commit_ts,
            },
            tables,
            metadata_extension: Vec::new(),
        }
        .encode()
        .map_err(Into::into)
    }

    /// Restore a complete logical SQL snapshot into this storage engine.
    ///
    /// Validation and batch construction happen before any write. SQL data,
    /// catalog, and the durable apply marker are replaced atomically. Only after
    /// the RocksDB batch succeeds are the in-memory catalog and HLC floor updated.
    pub fn restore(&self, bytes: &[u8]) -> Result<SnapshotMetadata, SnapshotManagerError> {
        let snapshot = ReplicatedSqlSnapshot::decode(bytes)?;
        self.restore_decoded(snapshot, |batch| {
            self.engine.write_batch(batch).map_err(Into::into)
        })
    }

    fn restore_decoded<F>(
        &self,
        snapshot: ReplicatedSqlSnapshot,
        writer: F,
    ) -> Result<SnapshotMetadata, SnapshotManagerError>
    where
        F: FnOnce(WriteBatch) -> Result<(), SnapshotManagerError>,
    {
        if !self.engine.list_index_cfs()?.is_empty() {
            return Err(SnapshotManagerError::RestoreTargetHasSecondaryIndexes);
        }

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

        let existing_apply_state = match self.engine.db.get_cf(&meta_cf, APPLY_STATE_KEY)? {
            Some(bytes) => DurableApplyState::decode(&bytes)?,
            None => DurableApplyState::default(),
        };
        if snapshot.metadata.latest_sql_apply_index < existing_apply_state.last_applied_index {
            return Err(SnapshotManagerError::RestoreWouldRegressApplyIndex {
                existing_index: existing_apply_state.last_applied_index,
                snapshot_index: snapshot.metadata.latest_sql_apply_index,
            });
        }
        if snapshot.metadata.latest_commit_ts < existing_apply_state.last_commit_ts {
            return Err(SnapshotManagerError::RestoreWouldRegressCommitTimestamp {
                existing_timestamp: existing_apply_state.last_commit_ts,
                snapshot_timestamp: snapshot.metadata.latest_commit_ts,
            });
        }

        let mut batch = WriteBatch::default();
        delete_all_cf_keys(&self.engine, &data_cf, &mut batch)?;
        delete_all_cf_keys(&self.engine, &catalog_cf, &mut batch)?;

        let mut schemas = Vec::with_capacity(snapshot.tables.len());
        for table in &snapshot.tables {
            if !table.rows.is_empty() && snapshot.metadata.latest_commit_ts == 0 {
                return Err(SnapshotManagerError::RowDataWithoutCommitTimestamp {
                    table: table.schema.name.clone(),
                });
            }
            let serialized = serde_json::to_vec(&table.schema)?;
            batch.put_cf(
                &catalog_cf,
                table.schema.name.to_lowercase().as_bytes(),
                serialized,
            );
            for row in &table.rows {
                if row.value.is_empty() {
                    return Err(SnapshotManagerError::SnapshotContainsTombstone {
                        table: table.schema.name.clone(),
                    });
                }
                batch.put_cf(
                    &data_cf,
                    encode_versioned_key(
                        table.table_id,
                        &row.primary_key,
                        HlcTimestamp::from_u64(snapshot.metadata.latest_commit_ts),
                    ),
                    &row.value,
                );
            }
            schemas.push(table.schema.clone());
        }

        let restored_apply_state = DurableApplyState {
            last_applied_index: snapshot.metadata.latest_sql_apply_index,
            last_commit_ts: snapshot.metadata.latest_commit_ts,
        };
        batch.put_cf(&meta_cf, APPLY_STATE_KEY, restored_apply_state.encode());

        writer(batch)?;

        // Publish volatile state only after the durable replacement succeeded.
        // Runtime catalogs always start with NeuralBase's built-in TPC-H schemas;
        // the logical snapshot contains only durable catalog entries. Rebuild the
        // same baseline and overlay restored durable schemas so snapshot install
        // cannot accidentally remove the built-ins from the serving catalog.
        let mut runtime_schemas = InMemoryCatalog::with_tpch_all_tables().all_tables();
        runtime_schemas.extend(schemas);
        self.catalog.replace_all(runtime_schemas);
        if snapshot.metadata.latest_commit_ts != 0 {
            self.clock
                .observe_committed(HlcTimestamp::from_u64(snapshot.metadata.latest_commit_ts));
        }
        Ok(snapshot.metadata)
    }
}

fn read_catalog<D: rocksdb::DBAccess>(
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, D>,
    catalog_cf: &impl rocksdb::AsColumnFamilyRef,
) -> Result<Vec<TableSchema>, SnapshotManagerError> {
    let mut iter = snapshot.raw_iterator_cf(catalog_cf);
    iter.seek_to_first();
    let mut schemas = BTreeMap::<String, TableSchema>::new();
    while iter.valid() {
        let key = iter.key().expect("valid iterator must have key");
        if key.starts_with(INDEX_SENTINEL_PREFIX) {
            let name = String::from_utf8_lossy(&key[INDEX_SENTINEL_PREFIX.len()..]).to_string();
            return Err(SnapshotManagerError::UnsupportedSecondaryIndex(name));
        }
        let key_text = std::str::from_utf8(key)
            .map_err(|_| SnapshotManagerError::InvalidCatalogKey)?
            .to_string();
        let value = iter.value().expect("valid iterator must have value");
        let schema: TableSchema = serde_json::from_slice(value)?;
        let expected = schema.name.to_lowercase();
        if key_text != expected {
            return Err(SnapshotManagerError::CatalogKeyMismatch {
                key: key_text,
                expected,
            });
        }
        schemas.insert(schema.name.to_lowercase(), schema);
        iter.next();
    }
    Ok(schemas.into_values().collect())
}

fn read_logical_rows<D: rocksdb::DBAccess>(
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, D>,
    data_cf: &impl rocksdb::AsColumnFamilyRef,
    table: &str,
    table_id: u32,
    last_commit_ts: u64,
) -> Result<Vec<SnapshotRow>, SnapshotManagerError> {
    let prefix = table_id.to_be_bytes();
    let mut iter = snapshot.raw_iterator_cf(data_cf);
    iter.seek(prefix);

    let mut latest = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    while iter.valid() {
        let key = iter.key().expect("valid iterator must have key");
        if key.len() < 12 || key[..4] != prefix {
            break;
        }
        let primary_key =
            decode_pk_from_key(key).ok_or_else(|| SnapshotManagerError::MalformedDataKey {
                table: table.to_string(),
                key_len: key.len(),
            })?;
        let row_ts = decode_ts_from_key(key)
            .ok_or_else(|| SnapshotManagerError::MalformedDataKey {
                table: table.to_string(),
                key_len: key.len(),
            })?
            .to_u64();
        if last_commit_ts == 0 {
            return Err(SnapshotManagerError::RowDataWithoutCommitTimestamp {
                table: table.to_string(),
            });
        }
        if row_ts > last_commit_ts {
            return Err(SnapshotManagerError::RowBeyondReplicatedTimestamp {
                table: table.to_string(),
                row_ts,
                last_commit_ts,
            });
        }
        latest.insert(
            primary_key,
            iter.value()
                .expect("valid iterator must have value")
                .to_vec(),
        );
        iter.next();
    }

    Ok(latest
        .into_iter()
        .filter_map(|(primary_key, value)| {
            if value.is_empty() {
                None
            } else {
                Some(SnapshotRow { primary_key, value })
            }
        })
        .collect())
}

fn delete_all_cf_keys(
    engine: &StorageEngine,
    cf: &impl rocksdb::AsColumnFamilyRef,
    batch: &mut WriteBatch,
) -> Result<(), SnapshotManagerError> {
    let mut iter = engine.db.raw_iterator_cf(cf);
    iter.seek_to_first();
    while iter.valid() {
        let key = iter.key().expect("valid iterator must have key").to_vec();
        batch.delete_cf(cf, key);
        iter.next();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Catalog, ColumnDef};
    use crate::consensus::rpc::LogEntry;
    use crate::replicated_sql::{ReplicatedMutation, ReplicatedRowWrite};
    use crate::replicated_state_machine::{ReplicatedApplyOutcome, ReplicatedSqlStateMachine};
    use crate::rocksdb_catalog::RocksDbCatalog;
    use tempfile::TempDir;

    fn schema() -> TableSchema {
        TableSchema {
            name: "items".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                },
                ColumnDef {
                    name: "note".to_string(),
                    data_type: "TEXT".to_string(),
                },
            ],
        }
    }

    fn ts(wall_ms: u64, logical: u16) -> u64 {
        HlcTimestamp { wall_ms, logical }.to_u64()
    }

    fn apply(sm: &ReplicatedSqlStateMachine, index: u64, mutation: ReplicatedMutation) {
        sm.apply_log_entry(&LogEntry {
            term: 1,
            index,
            command: mutation.encode().unwrap(),
        })
        .unwrap();
    }

    fn seed_source(dir: &TempDir) -> (Arc<StorageEngine>, Arc<InMemoryCatalog>, Arc<HlcClock>) {
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let sm = ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        )
        .unwrap();
        let tid = table_id_for("items");

        apply(&sm, 1, ReplicatedMutation::CreateTable { schema: schema() });
        apply(
            &sm,
            2,
            ReplicatedMutation::InsertRows {
                table: "items".to_string(),
                table_id: tid,
                commit_ts: ts(1_000, 1),
                rows: vec![
                    ReplicatedRowWrite {
                        primary_key: b"a".to_vec(),
                        value: b"a-v1".to_vec(),
                    },
                    ReplicatedRowWrite {
                        primary_key: b"b".to_vec(),
                        value: b"b-v1".to_vec(),
                    },
                ],
            },
        );
        apply(
            &sm,
            3,
            ReplicatedMutation::UpdateRows {
                table: "items".to_string(),
                table_id: tid,
                commit_ts: ts(1_000, 2),
                rows: vec![ReplicatedRowWrite {
                    primary_key: b"a".to_vec(),
                    value: b"a-v2".to_vec(),
                }],
            },
        );
        apply(
            &sm,
            4,
            ReplicatedMutation::DeleteRows {
                table: "items".to_string(),
                table_id: tid,
                commit_ts: ts(1_000, 3),
                primary_keys: vec![b"b".to_vec()],
            },
        );
        (engine, catalog, clock)
    }

    fn manager_for(
        dir: &TempDir,
    ) -> (
        ReplicatedSqlSnapshotManager,
        Arc<StorageEngine>,
        Arc<InMemoryCatalog>,
        Arc<HlcClock>,
    ) {
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        (
            ReplicatedSqlSnapshotManager::new(
                Arc::clone(&engine),
                Arc::clone(&catalog),
                Arc::clone(&clock),
            ),
            engine,
            catalog,
            clock,
        )
    }

    #[test]
    fn export_restore_fresh_db_is_logically_and_byte_identical() {
        let source_dir = TempDir::new().unwrap();
        let (source_engine, source_catalog, source_clock) = seed_source(&source_dir);
        let source_manager =
            ReplicatedSqlSnapshotManager::new(source_engine, source_catalog, source_clock);
        let bytes = source_manager.export(5, 2).unwrap();
        let decoded = ReplicatedSqlSnapshot::decode(&bytes).unwrap();
        assert_eq!(decoded.metadata.latest_sql_apply_index, 4);
        assert_eq!(decoded.tables.len(), 1);
        assert_eq!(decoded.tables[0].rows.len(), 1);
        assert_eq!(decoded.tables[0].rows[0].primary_key, b"a");
        assert_eq!(decoded.tables[0].rows[0].value, b"a-v2");

        let target_dir = TempDir::new().unwrap();
        let (target_manager, _engine, catalog, clock) = manager_for(&target_dir);
        assert_eq!(target_manager.restore(&bytes).unwrap(), decoded.metadata);
        assert_eq!(catalog.get_table("items"), Some(schema()));
        assert!(catalog.get_table("lineitem").is_some());
        assert_eq!(clock.now().to_u64(), decoded.metadata.latest_commit_ts);
        assert_eq!(target_manager.export(5, 2).unwrap(), bytes);
    }

    #[test]
    fn restart_after_restore_preserves_snapshot_state() {
        let source_dir = TempDir::new().unwrap();
        let (source_engine, source_catalog, source_clock) = seed_source(&source_dir);
        let source_manager =
            ReplicatedSqlSnapshotManager::new(source_engine, source_catalog, source_clock);
        let bytes = source_manager.export(5, 2).unwrap();

        let target_dir = TempDir::new().unwrap();
        {
            let (target_manager, _engine, _catalog, _clock) = manager_for(&target_dir);
            target_manager.restore(&bytes).unwrap();
        }
        let engine = Arc::new(StorageEngine::open(target_dir.path()).unwrap());
        let disk_catalog = RocksDbCatalog::new(Arc::clone(&engine)).load_all().unwrap();
        let catalog = Arc::new(disk_catalog);
        let clock = Arc::new(HlcClock::new(500));
        let sm = ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        )
        .unwrap();
        assert_eq!(sm.durable_state().unwrap().last_applied_index, 4);
        let restarted = ReplicatedSqlSnapshotManager::new(engine, catalog, clock);
        assert_eq!(restarted.export(5, 2).unwrap(), bytes);
    }

    #[test]
    fn replay_at_or_before_restored_marker_is_idempotent() {
        let source_dir = TempDir::new().unwrap();
        let (source_engine, source_catalog, source_clock) = seed_source(&source_dir);
        let bytes = ReplicatedSqlSnapshotManager::new(source_engine, source_catalog, source_clock)
            .export(5, 2)
            .unwrap();

        let target_dir = TempDir::new().unwrap();
        let (target_manager, engine, catalog, clock) = manager_for(&target_dir);
        target_manager.restore(&bytes).unwrap();
        let before = engine
            .raw_scan_table_versions(table_id_for("items"))
            .unwrap();
        let sm = ReplicatedSqlStateMachine::new(Arc::clone(&engine), catalog, clock).unwrap();
        let outcome = sm
            .apply_log_entry(&LogEntry {
                term: 1,
                index: 3,
                command: ReplicatedMutation::UpdateRows {
                    table: "items".to_string(),
                    table_id: table_id_for("items"),
                    commit_ts: ts(1_000, 2),
                    rows: vec![ReplicatedRowWrite {
                        primary_key: b"a".to_vec(),
                        value: b"should-not-reapply".to_vec(),
                    }],
                }
                .encode()
                .unwrap(),
            })
            .unwrap();
        assert_eq!(outcome, ReplicatedApplyOutcome::AlreadyApplied { index: 3 });
        assert_eq!(
            engine
                .raw_scan_table_versions(table_id_for("items"))
                .unwrap(),
            before
        );
    }

    #[test]
    fn corrupt_snapshot_leaves_existing_target_unchanged() {
        let source_dir = TempDir::new().unwrap();
        let (source_engine, source_catalog, source_clock) = seed_source(&source_dir);
        let mut bytes =
            ReplicatedSqlSnapshotManager::new(source_engine, source_catalog, source_clock)
                .export(5, 2)
                .unwrap();
        bytes[12] ^= 0x80;

        let target_dir = TempDir::new().unwrap();
        let (target_manager, engine, catalog, clock) = manager_for(&target_dir);
        catalog.register_table(TableSchema {
            name: "sentinel".to_string(),
            columns: vec![],
        });
        let before_clock = clock.now();
        assert!(matches!(
            target_manager.restore(&bytes),
            Err(SnapshotManagerError::Codec(
                SnapshotCodecError::ChecksumMismatch
            ))
        ));
        assert!(catalog.get_table("sentinel").is_some());
        assert_eq!(clock.now(), before_clock);
        assert!(engine.list_catalog_keys().unwrap().is_empty());
    }

    #[test]
    fn injected_write_failure_does_not_publish_partial_restore() {
        let source_dir = TempDir::new().unwrap();
        let (source_engine, source_catalog, source_clock) = seed_source(&source_dir);
        let bytes = ReplicatedSqlSnapshotManager::new(source_engine, source_catalog, source_clock)
            .export(5, 2)
            .unwrap();
        let decoded = ReplicatedSqlSnapshot::decode(&bytes).unwrap();

        let target_dir = TempDir::new().unwrap();
        let (target_manager, engine, catalog, clock) = manager_for(&target_dir);
        let existing_schema = TableSchema {
            name: "existing".to_string(),
            columns: vec![],
        };
        RocksDbCatalog::new(Arc::clone(&engine))
            .register_table(&existing_schema)
            .unwrap();
        catalog.register_table(existing_schema.clone());
        let before_clock = clock.now();

        assert!(matches!(
            target_manager.restore_decoded(decoded, |_batch| {
                Err(SnapshotManagerError::InjectedStorageFailure)
            }),
            Err(SnapshotManagerError::InjectedStorageFailure)
        ));
        assert_eq!(
            RocksDbCatalog::new(engine).get_table("existing"),
            Some(existing_schema.clone())
        );
        assert_eq!(catalog.get_table("existing"), Some(existing_schema));
        assert!(catalog.get_table("items").is_none());
        assert_eq!(clock.now(), before_clock);
    }

    #[test]
    fn restore_rejects_apply_or_timestamp_regression_without_mutation() {
        let source_dir = TempDir::new().unwrap();
        let (source_engine, source_catalog, source_clock) = seed_source(&source_dir);
        let bytes = ReplicatedSqlSnapshotManager::new(source_engine, source_catalog, source_clock)
            .export(5, 2)
            .unwrap();
        let decoded = ReplicatedSqlSnapshot::decode(&bytes).unwrap();

        let target_dir = TempDir::new().unwrap();
        let (target_engine, target_catalog, target_clock) = seed_source(&target_dir);
        let target_manager = ReplicatedSqlSnapshotManager::new(
            Arc::clone(&target_engine),
            Arc::clone(&target_catalog),
            target_clock,
        );
        let before = target_engine
            .raw_scan_table_versions(table_id_for("items"))
            .unwrap();

        let mut apply_regression = decoded.clone();
        apply_regression.metadata.latest_sql_apply_index -= 1;
        let apply_regression = apply_regression.encode().unwrap();
        assert!(matches!(
            target_manager.restore(&apply_regression),
            Err(SnapshotManagerError::RestoreWouldRegressApplyIndex {
                existing_index: 4,
                snapshot_index: 3,
            })
        ));

        let mut timestamp_regression = decoded;
        timestamp_regression.metadata.latest_commit_ts = ts(1_000, 2);
        let timestamp_regression = timestamp_regression.encode().unwrap();
        assert!(matches!(
            target_manager.restore(&timestamp_regression),
            Err(SnapshotManagerError::RestoreWouldRegressCommitTimestamp { .. })
        ));

        assert_eq!(
            target_engine
                .raw_scan_table_versions(table_id_for("items"))
                .unwrap(),
            before
        );
        assert_eq!(target_catalog.get_table("items"), Some(schema()));
    }

    #[test]
    fn export_rejects_unreplicated_future_version() {
        let source_dir = TempDir::new().unwrap();
        let (engine, catalog, clock) = seed_source(&source_dir);
        engine
            .write_version(
                table_id_for("items"),
                b"rogue",
                HlcTimestamp::from_u64(ts(2_000, 0)),
                b"rogue",
            )
            .unwrap();
        let manager = ReplicatedSqlSnapshotManager::new(engine, catalog, clock);
        assert!(matches!(
            manager.export(5, 2),
            Err(SnapshotManagerError::RowBeyondReplicatedTimestamp { .. })
        ));
    }

    #[test]
    fn export_rejects_secondary_index_state() {
        let source_dir = TempDir::new().unwrap();
        let (engine, catalog, clock) = seed_source(&source_dir);
        engine.create_index_cf("idx_items_note").unwrap();
        let manager = ReplicatedSqlSnapshotManager::new(engine, catalog, clock);
        assert!(matches!(
            manager.export(5, 2),
            Err(SnapshotManagerError::UnsupportedSecondaryIndex(ref name))
                if name == "idx_items_note"
        ));
    }
}
