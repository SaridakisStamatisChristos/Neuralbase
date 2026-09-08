// SPDX-License-Identifier: Apache-2.0
//! Repeated SQL-aware Raft compaction lifecycle evidence.

use std::sync::Arc;
use std::time::Duration;

use neuralbase::binder::{InsertPlan, SqlValue};
use neuralbase::catalog::{InMemoryCatalog, TableSchema};
use neuralbase::consensus::{
    encode_compact_log, ChannelBus, ChannelTransport, ClientCommand, CommittedEntry,
    FailClosedPersistenceStore, RaftNode, RaftPersistenceStore, RaftRole, RaftShared,
    RaftTaskHandle,
};
use neuralbase::hlc::{HlcClock, HlcTimestamp};
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::replicated_snapshot_hooks::ReplicatedSqlSnapshotHooks;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::rocksdb_catalog::RocksDbCatalog;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::table_id_for;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

struct SoloNode {
    dir: TempDir,
    engine: Arc<StorageEngine>,
    catalog: Arc<InMemoryCatalog>,
    clock: Arc<HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
}

impl SoloNode {
    fn gateway(&self) -> ReplicatedSqlGateway {
        ReplicatedSqlGateway::new(
            self.client_tx.clone(),
            Arc::clone(&self.shared),
            Arc::clone(&self.engine),
            Arc::clone(&self.clock),
        )
    }

    async fn shutdown_into_dir(mut self) -> TempDir {
        if let Some(handle) = self.handle.take() {
            handle.shutdown().await;
        }
        self.apply_task.abort();
        let _ = (&mut self.apply_task).await;
        let dir = self.dir;
        drop(self.client_tx);
        drop(self.shared);
        drop(self.engine);
        drop(self.catalog);
        drop(self.clock);
        dir
    }
}

fn schema() -> TableSchema {
    TableSchema {
        name: "cycle_items".to_string(),
        columns: vec![neuralbase::catalog::ColumnDef {
            name: "id".to_string(),
            data_type: "BIGINT".to_string(),
        }],
    }
}

fn insert_plan(id: i64) -> InsertPlan {
    InsertPlan {
        table: schema(),
        columns: vec!["id".to_string()],
        rows: vec![vec![SqlValue::Int(id)]],
    }
}

async fn spawn_solo(bus: ChannelBus, dir: TempDir) -> SoloNode {
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let catalog = Arc::new(RocksDbCatalog::new(Arc::clone(&engine)).load_all().unwrap());
    let clock = Arc::new(HlcClock::new(500));
    let state_machine = Arc::new(
        ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        )
        .unwrap(),
    );
    let snapshot_store = Arc::new(ReplicatedSqlSnapshotHooks::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    ));
    let persistence: Arc<dyn RaftPersistenceStore> = Arc::new(FailClosedPersistenceStore::new(
        Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine))),
    ));
    let transport = Arc::new(ChannelTransport::register("cycle-solo".to_string(), bus).await);
    let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(32);
    let apply_state_machine = Arc::clone(&state_machine);
    let apply_task = tokio::spawn(async move {
        while let Some(committed) = apply_rx.recv().await {
            let result = apply_state_machine
                .apply_log_entry(&committed.entry)
                .map(|_| ())
                .map_err(|error| error.to_string());
            let failed = result.is_err();
            let _ = committed.completion.send(result);
            if failed {
                break;
            }
        }
    });

    let mut raft = RaftNode::new("cycle-solo".to_string(), vec![], transport)
        .with_snapshot_store(snapshot_store)
        .with_persistence(persistence)
        .with_confirmed_apply_tx(apply_tx);
    raft.set_election_timeout_ms(20);
    let (client_tx, shared, handle) = raft.spawn();

    SoloNode {
        dir,
        engine,
        catalog,
        clock,
        client_tx,
        shared,
        handle: Some(handle),
        apply_task,
    }
}

async fn wait_for_leader(node: &SoloNode) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while node.shared.lock().await.role != RaftRole::Leader {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn compact_at(node: &SoloNode, index: u64) -> u64 {
    let (reply, response) = oneshot::channel();
    node.client_tx
        .send(ClientCommand {
            payload: encode_compact_log(index, &[]),
            reply,
        })
        .await
        .unwrap();
    response.await.unwrap().unwrap()
}

#[tokio::test]
async fn repeated_compaction_restart_and_suffix_replay_remain_safe() {
    let bus = ChannelTransport::new_bus();
    let mut node = spawn_solo(Arc::clone(&bus), TempDir::new().unwrap()).await;
    wait_for_leader(&node).await;
    let gateway = node.gateway();

    gateway.create_table(schema()).await.unwrap();
    let first_insert = gateway.insert(&insert_plan(1)).await.unwrap();
    let first_boundary = first_insert.raft_index;
    assert_eq!(compact_at(&node, first_boundary).await, first_boundary);
    let first_persisted = RocksDbRaftPersistenceStore::new(Arc::clone(&node.engine))
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(first_persisted.0.snapshot_index, first_boundary);
    assert!(!first_persisted.1.is_empty());

    let second_insert = gateway.insert(&insert_plan(2)).await.unwrap();
    let second_boundary = second_insert.raft_index;
    assert!(second_boundary > first_boundary);
    assert_eq!(compact_at(&node, second_boundary).await, second_boundary);
    let second_persisted = RocksDbRaftPersistenceStore::new(Arc::clone(&node.engine))
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(second_persisted.0.snapshot_index, second_boundary);
    assert_ne!(second_persisted.1, first_persisted.1);

    let suffix_ack = gateway.insert(&insert_plan(3)).await.unwrap();
    assert!(suffix_ack.raft_index > second_boundary);
    assert_eq!(
        node.engine
            .scan_table(table_id_for("cycle_items"), HlcTimestamp::MAX)
            .unwrap()
            .len(),
        3
    );

    // `ReplicatedSqlGateway` owns an Arc<StorageEngine>. Release it before
    // reopening this same RocksDB directory so the restart exercises a real
    // close/reopen boundary instead of retaining the previous process-local DB
    // handle and tripping RocksDB's LOCK protection.
    drop(gateway);
    let dir = node.shutdown_into_dir().await;
    node = spawn_solo(Arc::clone(&bus), dir).await;
    wait_for_leader(&node).await;

    let restarted = RocksDbRaftPersistenceStore::new(Arc::clone(&node.engine))
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(restarted.0.snapshot_index, second_boundary);
    assert!(restarted.0.last_log_index() >= suffix_ack.raft_index);
    assert_eq!(
        node.engine
            .scan_table(table_id_for("cycle_items"), HlcTimestamp::MAX)
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        node.engine
            .raw_scan_table_versions(table_id_for("cycle_items"))
            .unwrap()
            .len(),
        3
    );

    node.gateway().insert(&insert_plan(4)).await.unwrap();
    assert_eq!(
        node.engine
            .scan_table(table_id_for("cycle_items"), HlcTimestamp::MAX)
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        node.engine
            .raw_scan_table_versions(table_id_for("cycle_items"))
            .unwrap()
            .len(),
        4
    );

    let _dir = node.shutdown_into_dir().await;
}
