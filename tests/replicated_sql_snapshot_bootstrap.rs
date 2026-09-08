// SPDX-License-Identifier: Apache-2.0
//! End-to-end Phase 2 bootstrap/replacement proof over real Raft and RocksDB.
//!
//! The test destroys one fixed member's entire storage, reconstructs that same
//! logical member from a leader SQL snapshot plus retained Raft suffix, verifies
//! serving readiness stays closed while catch-up is incomplete, transfers
//! leadership to the reconstructed member, acknowledges another SQL write there,
//! then restarts it again from its recovered disk and verifies exact convergence.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use neuralbase::binder::{InsertPlan, SqlValue};
use neuralbase::catalog::{Catalog, ColumnDef, InMemoryCatalog, TableSchema};
use neuralbase::consensus::{
    encode_compact_log, ChannelBus, ChannelTransport, ClientCommand, CommittedEntry,
    FailClosedPersistenceStore, RaftNode, RaftPersistenceStore, RaftRole, RaftShared,
    RaftTaskHandle, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::{HlcClock, HlcTimestamp};
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::{ReplicatedGatewayError, ReplicatedSqlGateway};
use neuralbase::replicated_snapshot_hooks::ReplicatedSqlSnapshotHooks;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::rocksdb_catalog::RocksDbCatalog;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::table_id_for;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

const IDS: [&str; 3] = ["snapshot-a", "snapshot-b", "snapshot-c"];
const REPLACEMENT_ELECTION_TIMEOUT_MS: u64 = 250;

struct SnapshotNode {
    id: String,
    dir: TempDir,
    engine: Arc<StorageEngine>,
    catalog: Arc<InMemoryCatalog>,
    clock: Arc<HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    readiness: Arc<AtomicBool>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
}

impl SnapshotNode {
    fn gateway(&self) -> ReplicatedSqlGateway {
        ReplicatedSqlGateway::new_with_readiness(
            self.client_tx.clone(),
            Arc::clone(&self.shared),
            Arc::clone(&self.engine),
            Arc::clone(&self.clock),
            Arc::clone(&self.readiness),
        )
    }

    async fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.shutdown().await;
        }
        self.apply_task.abort();
        let _ = (&mut self.apply_task).await;
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
        drop(self.readiness);
        dir
    }
}

fn schema() -> TableSchema {
    TableSchema {
        name: "snapshot_items".to_string(),
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

async fn spawn_node(
    bus: ChannelBus,
    id: &str,
    election_timeout_ms: u64,
    dir: TempDir,
) -> SnapshotNode {
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    // Mirror production clustered startup: hydrate durable schemas before the
    // Raft snapshot hook decides whether an older active snapshot should be
    // replayed. If durable SQL is already ahead of that snapshot, the hook must
    // preserve the suffix and the catalog must still come from RocksDB.
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

    let transport = Arc::new(ChannelTransport::register(id.to_string(), bus).await);
    let peers = IDS
        .iter()
        .filter(|peer| **peer != id)
        .map(|peer| (*peer).to_string())
        .collect();

    let raw_store: Arc<dyn RaftPersistenceStore> =
        Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine)));
    let strict_store: Arc<dyn RaftPersistenceStore> =
        Arc::new(FailClosedPersistenceStore::new(raw_store));
    let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(APPLY_CHANNEL_CAPACITY);
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

    let mut node = RaftNode::new(id.to_string(), peers, transport)
        .with_snapshot_store(snapshot_store)
        .with_persistence(strict_store)
        .with_confirmed_apply_tx(apply_tx);
    node.set_election_timeout_ms(election_timeout_ms);
    let readiness = node.serving_readiness();
    let (client_tx, shared, handle) = node.spawn();

    SnapshotNode {
        id: id.to_string(),
        dir,
        engine,
        catalog,
        clock,
        client_tx,
        shared,
        readiness,
        handle: Some(handle),
        apply_task,
    }
}

async fn wait_for_leader(nodes: &[SnapshotNode]) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    loop {
        for node in nodes {
            if node.handle.is_some() && node.shared.lock().await.role == RaftRole::Leader {
                return node.id.clone();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_specific_leader(nodes: &[SnapshotNode], expected: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    loop {
        if let Some(node) = nodes.iter().find(|node| node.id == expected) {
            if node.shared.lock().await.role == RaftRole::Leader {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected reconstructed member {expected} did not become leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_rows(nodes: &[SnapshotNode], count: usize) {
    let table_id = table_id_for("snapshot_items");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let mut converged = true;
        for node in nodes {
            let rows = node.engine.scan_table(table_id, HlcTimestamp::MAX).unwrap();
            converged &= rows.len() == count;
        }
        if converged {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "replicated snapshot/bootstrap state did not converge to {count} rows"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_until_ready(node: &SnapshotNode) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while !node.readiness.load(Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "reconstructed member never became serving-ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn compact_at(node: &SnapshotNode, index: u64) -> u64 {
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

fn insert_plan(id: i64, name: &str) -> InsertPlan {
    InsertPlan {
        table: schema(),
        columns: vec!["id".to_string(), "name".to_string()],
        rows: vec![vec![SqlValue::Int(id), SqlValue::Text(name.to_string())]],
    }
}

fn visible_rows(node: &SnapshotNode) -> Vec<(Vec<u8>, Vec<u8>)> {
    node.engine
        .scan_table(table_id_for("snapshot_items"), HlcTimestamp::MAX)
        .unwrap()
}

#[tokio::test]
async fn empty_disk_fixed_member_bootstraps_from_snapshot_suffix_and_survives_failover_restart() {
    let bus = ChannelTransport::new_bus();
    let mut nodes = Vec::new();
    for (ordinal, id) in IDS.iter().enumerate() {
        nodes.push(
            spawn_node(
                Arc::clone(&bus),
                id,
                70 + ordinal as u64 * 25,
                TempDir::new().unwrap(),
            )
            .await,
        );
    }

    let leader_id = wait_for_leader(&nodes).await;
    let leader_pos = nodes.iter().position(|node| node.id == leader_id).unwrap();
    let leader_gateway = nodes[leader_pos].gateway();

    leader_gateway.create_table(schema()).await.unwrap();
    let first_ack = leader_gateway
        .insert(&insert_plan(1, "before-snapshot"))
        .await
        .unwrap();
    wait_for_rows(&nodes, 1).await;

    let boundary = first_ack.raft_index;
    assert!(boundary > 0);
    assert_eq!(compact_at(&nodes[leader_pos], boundary).await, boundary);

    let persisted_leader = RocksDbRaftPersistenceStore::new(Arc::clone(&nodes[leader_pos].engine))
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(persisted_leader.0.snapshot_index, boundary);
    assert!(!persisted_leader.1.is_empty());

    // Choose the first peer in the leader's deterministic peer order. The
    // existing leader-transfer helper targets that same peer later, allowing the
    // reconstructed member to prove it can safely assume leadership.
    let victim_id = IDS.iter().find(|id| **id != leader_id).unwrap().to_string();
    let victim_pos = nodes.iter().position(|node| node.id == victim_id).unwrap();
    let victim = nodes.remove(victim_pos);
    let destroyed_dir = victim.shutdown_into_dir().await;
    drop(destroyed_dir);

    // This write is deliberately after the snapshot and while the victim is
    // absent, forcing bootstrap to install the snapshot and then apply a suffix.
    let leader_pos = nodes.iter().position(|node| node.id == leader_id).unwrap();
    let suffix_ack = nodes[leader_pos]
        .gateway()
        .insert(&insert_plan(2, "after-snapshot"))
        .await
        .unwrap();
    assert!(suffix_ack.raft_index > boundary);
    wait_for_rows(&nodes, 2).await;

    let replacement = spawn_node(
        Arc::clone(&bus),
        &victim_id,
        REPLACEMENT_ELECTION_TIMEOUT_MS,
        TempDir::new().unwrap(),
    )
    .await;
    assert!(!replacement.readiness.load(Ordering::Acquire));
    assert!(matches!(
        replacement.gateway().prepare_mutation().await.unwrap_err(),
        ReplicatedGatewayError::CatchingUp
    ));
    nodes.push(replacement);

    let replacement_pos = nodes.iter().position(|node| node.id == victim_id).unwrap();
    wait_until_ready(&nodes[replacement_pos]).await;
    wait_for_rows(&nodes, 2).await;
    assert!(nodes[replacement_pos]
        .catalog
        .get_table("snapshot_items")
        .is_some());
    assert_eq!(
        visible_rows(&nodes[replacement_pos]),
        visible_rows(nodes.iter().find(|node| node.id == leader_id).unwrap())
    );
    assert!(matches!(
        nodes[replacement_pos]
            .gateway()
            .prepare_mutation()
            .await
            .unwrap_err(),
        ReplicatedGatewayError::NotLeader { .. }
    ));

    // Prove the replacement is not merely readable: transfer leadership to it
    // and acknowledge another replicated SQL write from the reconstructed state.
    let leader_pos = nodes.iter().position(|node| node.id == leader_id).unwrap();
    let transfer_target = nodes[leader_pos]
        .handle
        .as_ref()
        .unwrap()
        .request_leader_transfer()
        .await
        .unwrap();
    assert_eq!(transfer_target, victim_id);
    wait_for_specific_leader(&nodes, &victim_id).await;

    let replacement_pos = nodes.iter().position(|node| node.id == victim_id).unwrap();
    let replacement_ack = nodes[replacement_pos]
        .gateway()
        .insert(&insert_plan(3, "replacement-leader"))
        .await
        .unwrap();
    assert!(replacement_ack.raft_index > suffix_ack.raft_index);
    wait_for_rows(&nodes, 3).await;

    // Kill the replacement after its acknowledged write. Quorum-commit semantics
    // require that write to survive on the two original members.
    let replacement_pos = nodes.iter().position(|node| node.id == victim_id).unwrap();
    let replacement = nodes.remove(replacement_pos);
    let recovered_dir = replacement.shutdown_into_dir().await;
    let new_leader = wait_for_leader(&nodes).await;
    assert_ne!(new_leader, victim_id);
    wait_for_rows(&nodes, 3).await;

    // Restart the same reconstructed member from its recovered disk. Active SQL
    // snapshot restore must not erase the already-applied post-snapshot suffix.
    nodes.push(
        spawn_node(
            Arc::clone(&bus),
            &victim_id,
            REPLACEMENT_ELECTION_TIMEOUT_MS,
            recovered_dir,
        )
        .await,
    );
    let replacement_pos = nodes.iter().position(|node| node.id == victim_id).unwrap();
    wait_until_ready(&nodes[replacement_pos]).await;
    wait_for_rows(&nodes, 3).await;

    let restarted_persistence =
        RocksDbRaftPersistenceStore::new(Arc::clone(&nodes[replacement_pos].engine))
            .load()
            .unwrap()
            .unwrap();
    assert_eq!(restarted_persistence.0.snapshot_index, boundary);
    assert!(restarted_persistence.0.last_log_index() >= replacement_ack.raft_index);
    assert!(!restarted_persistence.1.is_empty());

    let expected = visible_rows(&nodes[0]);
    for node in &nodes {
        assert_eq!(visible_rows(node), expected);
        assert_eq!(
            node.engine
                .raw_scan_table_versions(table_id_for("snapshot_items"))
                .unwrap()
                .len(),
            3
        );
        assert!(node.catalog.get_table("snapshot_items").is_some());
    }

    for node in &mut nodes {
        node.stop().await;
    }
}
