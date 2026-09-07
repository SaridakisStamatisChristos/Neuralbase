// SPDX-License-Identifier: Apache-2.0
//! SQL-aware implementation of the Raft state-machine snapshot contract.
//!
//! This adapter keeps snapshot wire-format knowledge out of consensus code. It
//! validates the embedded Raft boundary before durable install staging, delegates
//! canonical SQL export/restore to `ReplicatedSqlSnapshotManager`, and avoids
//! regressing an already-applied SQL suffix when restarting from an older active
//! snapshot.

use std::sync::Arc;

use crate::catalog::InMemoryCatalog;
use crate::consensus::StateMachineSnapshotStore;
use crate::hlc::HlcClock;
use crate::replicated_snapshot::ReplicatedSqlSnapshot;
use crate::replicated_snapshot_manager::ReplicatedSqlSnapshotManager;
use crate::storage::{StorageEngine, CF_META};

const APPLY_STATE_KEY: &[u8] = b"raft/sql/apply-state-v1";
const APPLY_STATE_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, Default)]
struct DurableApplyState {
    last_applied_index: u64,
    last_commit_ts: u64,
}

pub struct ReplicatedSqlSnapshotHooks {
    engine: Arc<StorageEngine>,
    manager: ReplicatedSqlSnapshotManager,
}

impl ReplicatedSqlSnapshotHooks {
    pub fn new(
        engine: Arc<StorageEngine>,
        catalog: Arc<InMemoryCatalog>,
        clock: Arc<HlcClock>,
    ) -> Self {
        Self {
            engine: Arc::clone(&engine),
            manager: ReplicatedSqlSnapshotManager::new(engine, catalog, clock),
        }
    }

    fn durable_apply_state(&self) -> Result<DurableApplyState, String> {
        let meta_cf = self
            .engine
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| "CF_META unavailable while reconciling SQL snapshot".to_string())?;
        let Some(bytes) = self
            .engine
            .db
            .get_cf(&meta_cf, APPLY_STATE_KEY)
            .map_err(|error| format!("read replicated SQL apply state: {error}"))?
        else {
            return Ok(DurableApplyState::default());
        };
        if bytes.len() != APPLY_STATE_BYTES {
            return Err(format!(
                "corrupt replicated SQL apply state while reconciling snapshot: expected {APPLY_STATE_BYTES} bytes, got {}",
                bytes.len()
            ));
        }
        let last_applied_index = u64::from_be_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| "decode replicated SQL apply index".to_string())?,
        );
        let last_commit_ts = u64::from_be_bytes(
            bytes[8..]
                .try_into()
                .map_err(|_| "decode replicated SQL commit timestamp".to_string())?,
        );
        Ok(DurableApplyState {
            last_applied_index,
            last_commit_ts,
        })
    }

    fn decode_and_validate(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: &[u8],
    ) -> Result<ReplicatedSqlSnapshot, String> {
        let decoded = ReplicatedSqlSnapshot::decode(snapshot_data)
            .map_err(|error| format!("decode replicated SQL snapshot: {error}"))?;
        if decoded.metadata.last_included_index != last_included_index
            || decoded.metadata.last_included_term != last_included_term
        {
            return Err(format!(
                "snapshot boundary mismatch: expected index={last_included_index} term={last_included_term}, embedded index={} term={}",
                decoded.metadata.last_included_index, decoded.metadata.last_included_term
            ));
        }

        let current = self.durable_apply_state()?;
        if current.last_applied_index > last_included_index {
            if current.last_commit_ts < decoded.metadata.latest_commit_ts {
                return Err(format!(
                    "local SQL state is ahead of snapshot index but has an older HLC floor: local_index={} local_ts={} snapshot_index={} snapshot_ts={}",
                    current.last_applied_index,
                    current.last_commit_ts,
                    last_included_index,
                    decoded.metadata.latest_commit_ts
                ));
            }
            return Ok(decoded);
        }

        if current.last_applied_index > decoded.metadata.latest_sql_apply_index {
            return Err(format!(
                "local SQL apply index {} lies inside incoming snapshot boundary {} but is newer than the snapshot SQL marker {}",
                current.last_applied_index,
                last_included_index,
                decoded.metadata.latest_sql_apply_index
            ));
        }
        Ok(decoded)
    }
}

impl StateMachineSnapshotStore for ReplicatedSqlSnapshotHooks {
    fn create_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<Vec<u8>, String> {
        self.manager
            .export(last_included_index, last_included_term)
            .map_err(|error| format!("export replicated SQL snapshot: {error}"))
    }

    fn validate_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: &[u8],
    ) -> Result<(), String> {
        self.decode_and_validate(last_included_index, last_included_term, snapshot_data)
            .map(|_| ())
    }

    fn restore_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        snapshot_data: &[u8],
    ) -> Result<(), String> {
        let decoded =
            self.decode_and_validate(last_included_index, last_included_term, snapshot_data)?;
        let current = self.durable_apply_state()?;
        if current.last_applied_index > last_included_index {
            // Restart after a snapshot can legitimately find SQL state that has
            // already applied retained log entries beyond the active snapshot.
            // Replaying those entries is idempotent; restoring the older snapshot
            // would incorrectly erase that durable suffix.
            return Ok(());
        }

        debug_assert!(current.last_applied_index <= decoded.metadata.latest_sql_apply_index);
        self.manager
            .restore(snapshot_data)
            .map(|_| ())
            .map_err(|error| format!("restore replicated SQL snapshot: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, MutableCatalog, TableSchema};
    use crate::replicated_snapshot::{SnapshotMetadata, SnapshotTable};
    use crate::storage_executor::table_id_for;
    use tempfile::TempDir;

    fn schema() -> TableSchema {
        TableSchema {
            name: "hook_items".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
            }],
        }
    }

    fn snapshot_bytes() -> Vec<u8> {
        crate::replicated_snapshot::ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: 3,
                last_included_term: 2,
                latest_sql_apply_index: 0,
                latest_commit_ts: 0,
            },
            tables: vec![SnapshotTable {
                table_id: table_id_for("hook_items"),
                schema: schema(),
                rows: vec![],
            }],
            metadata_extension: vec![],
        }
        .encode()
        .unwrap()
    }

    #[test]
    fn validation_rejects_rpc_boundary_mismatch_before_mutation() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let hooks = ReplicatedSqlSnapshotHooks::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            clock,
        );
        catalog.create_table(schema());

        let error = hooks.validate_snapshot(4, 2, &snapshot_bytes()).unwrap_err();
        assert!(error.contains("snapshot boundary mismatch"));
        assert!(engine.list_catalog_keys().unwrap().is_empty());
    }

    #[test]
    fn restore_rejects_rpc_boundary_mismatch_before_mutation() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let hooks = ReplicatedSqlSnapshotHooks::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            clock,
        );

        let error = hooks.restore_snapshot(4, 2, &snapshot_bytes()).unwrap_err();
        assert!(error.contains("snapshot boundary mismatch"));
        assert!(engine.list_catalog_keys().unwrap().is_empty());
    }
}
