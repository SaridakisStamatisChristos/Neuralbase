// SPDX-License-Identifier: Apache-2.0
//! Crash-recovery evidence for SQL-aware Raft snapshot installation.

use std::sync::Arc;

use neuralbase::catalog::{Catalog, ColumnDef, InMemoryCatalog, TableSchema};
use neuralbase::consensus::{
    ChannelTransport, FailClosedPersistenceStore, RaftNode, RaftPersistenceStore, StagedSnapshot,
    StagedSnapshotKind, StateMachineSnapshotStore,
};
use neuralbase::hlc::{HlcClock, HlcTimestamp};
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_snapshot::{
    ReplicatedSqlSnapshot, SnapshotMetadata, SnapshotRow, SnapshotTable,
};
use neuralbase::replicated_snapshot_hooks::ReplicatedSqlSnapshotHooks;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::{decode_row, encode_row, table_id_for};
use tempfile::TempDir;
use tokio::sync::mpsc;

fn schema() -> TableSchema {
    TableSchema {
        name: "recovered_items".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
            },
        ],
    }
}

fn snapshot_bytes() -> Vec<u8> {
    let table = schema();
    ReplicatedSqlSnapshot {
        metadata: SnapshotMetadata {
            last_included_index: 5,
            last_included_term: 3,
            latest_sql_apply_index: 5,
            latest_commit_ts: 10_000,
        },
        tables: vec![SnapshotTable {
            table_id: table_id_for(&table.name),
            schema: table,
            rows: vec![SnapshotRow {
                primary_key: b"row-1".to_vec(),
                value: encode_row(&[("id", "1"), ("name", "recovered")]),
            }],
        }],
        metadata_extension: vec![],
    }
    .encode()
    .unwrap()
}

#[tokio::test]
async fn interrupted_install_recovers_and_second_restart_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let catalog = Arc::new(InMemoryCatalog::default());
    let clock = Arc::new(HlcClock::new(500));
    let raw_store = Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine)));
    let snapshot = Arc::new(snapshot_bytes());

    raw_store
        .stage_snapshot(&StagedSnapshot {
            kind: StagedSnapshotKind::Installation,
            last_included_index: 5,
            last_included_term: 3,
            data: Arc::clone(&snapshot),
        })
        .unwrap();

    // Simulate the exact crash window: SQL restore has committed durably, but
    // active Raft snapshot state has not yet been published and staging remains.
    let hooks = Arc::new(ReplicatedSqlSnapshotHooks::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    ));
    hooks.restore_snapshot(5, 3, snapshot.as_slice()).unwrap();
    assert!(raw_store.load().unwrap().is_none());
    assert!(raw_store.load_staged_snapshot().unwrap().is_some());

    let table_id = table_id_for("recovered_items");
    assert_eq!(engine.raw_scan_table_versions(table_id).unwrap().len(), 1);

    let bus = ChannelTransport::new_bus();
    let transport =
        Arc::new(ChannelTransport::register("recover-node".to_string(), Arc::clone(&bus)).await);
    let strict_store: Arc<dyn RaftPersistenceStore> =
        Arc::new(FailClosedPersistenceStore::new(raw_store.clone()));
    let (apply_tx, _apply_rx) = mpsc::channel(8);
    let _recovered_node = RaftNode::new("recover-node".to_string(), vec![], transport)
        .with_snapshot_store(hooks)
        .with_persistence(strict_store)
        .with_confirmed_apply_tx(apply_tx);

    let (persisted, active_snapshot) = raw_store.load().unwrap().unwrap();
    assert_eq!(persisted.snapshot_index, 5);
    assert_eq!(persisted.snapshot_term, 3);
    assert_eq!(active_snapshot, snapshot.as_slice());
    assert!(raw_store.load_staged_snapshot().unwrap().is_none());
    assert_eq!(engine.raw_scan_table_versions(table_id).unwrap().len(), 1);
    assert_eq!(catalog.get_table("recovered_items"), Some(schema()));

    let visible = engine.scan_table(table_id, HlcTimestamp::MAX).unwrap();
    assert_eq!(visible.len(), 1);
    let row = decode_row(&visible[0].1).unwrap();
    assert_eq!(row.get("name").map(String::as_str), Some("recovered"));

    let state_machine = ReplicatedSqlStateMachine::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    )
    .unwrap();
    let durable = state_machine.durable_state().unwrap();
    assert_eq!(durable.last_applied_index, 5);
    assert_eq!(durable.last_commit_ts, 10_000);
    assert!(clock.now().to_u64() >= 10_000);

    // A second process-style reconstruction from the now-active snapshot must
    // not create another MVCC row version or leave staging behind.
    let bus2 = ChannelTransport::new_bus();
    let transport2 =
        Arc::new(ChannelTransport::register("recover-node".to_string(), Arc::clone(&bus2)).await);
    let hooks2 = Arc::new(ReplicatedSqlSnapshotHooks::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    ));
    let strict_store2: Arc<dyn RaftPersistenceStore> =
        Arc::new(FailClosedPersistenceStore::new(raw_store.clone()));
    let (apply_tx2, _apply_rx2) = mpsc::channel(8);
    let _second_restart = RaftNode::new("recover-node".to_string(), vec![], transport2)
        .with_snapshot_store(hooks2)
        .with_persistence(strict_store2)
        .with_confirmed_apply_tx(apply_tx2);

    assert_eq!(engine.raw_scan_table_versions(table_id).unwrap().len(), 1);
    assert!(raw_store.load_staged_snapshot().unwrap().is_none());
}
