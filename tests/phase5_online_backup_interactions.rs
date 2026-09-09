// SPDX-License-Identifier: Apache-2.0
//! Phase-5 online-backup interaction evidence.
//!
//! These tests intentionally race backup capture with the classes of activity
//! that can move its logical or consensus boundary. A successful artifact must
//! describe exactly one committed boundary; a control-plane race may instead
//! fail closed, but must never publish a partial or mixed-state artifact.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use neuralbase::binder::{InsertPlan, SqlValue};
use neuralbase::catalog::{ColumnDef, InMemoryCatalog, TableSchema};
use neuralbase::consensus::{
    encode_compact_log, encode_membership_change, ChannelBus, ChannelTransport, ClientCommand,
    CommittedEntry, FailClosedPersistenceStore, MembershipChange, RaftNode, RaftPersistenceStore,
    RaftRole, RaftShared, RaftTaskHandle, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::HlcClock;
use neuralbase::offline_backup::verify_backup_file;
use neuralbase::online_backup::{OnlineBackupCoordinator, OnlineBackupError};
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::replicated_identity_snapshot::ReplicatedIdentitySnapshotExtension;
use neuralbase::replicated_snapshot::ReplicatedSqlSnapshot;
use neuralbase::replicated_snapshot_hooks::ReplicatedSqlSnapshotHooks;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

const TIMEOUT: Duration = Duration::from_secs(8);
const VOTERS: [&str; 3] = ["p5-online-a", "p5-online-b", "p5-online-c"];

type Shared = Arc<Mutex<RaftShared>>;

struct ClusterNode {
    _db: TempDir,
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Shared,
    readiness: Arc<AtomicBool>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
    _apply_gate: Option<Arc<Notify>>,
}

impl ClusterNode {
    fn gateway(&self) -> ReplicatedSqlGateway {
        ReplicatedSqlGateway::new_with_readiness(
            self.client_tx.clone(),
            Arc::clone(&self.shared),
            Arc::clone(&self.engine),
            Arc::clone(&self.clock),
            Arc::clone(&self.readiness),
        )
    }

    fn coordinator(&self) -> OnlineBackupCoordinator {
        OnlineBackupCoordinator::new(
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
}

async fn spawn_node(bus: ChannelBus, id: &str, lag_apply: bool) -> ClusterNode {
    let db = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(db.path()).unwrap());
    let catalog = Arc::new(InMemoryCatalog::default());
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
        catalog,
        Arc::clone(&clock),
    ));
    let transport = Arc::new(ChannelTransport::register(id.to_string(), bus).await);
    let peers = VOTERS
        .iter()
        .copied()
        .filter(|peer| *peer != id)
        .map(str::to_string)
        .collect();
    let mut node = RaftNode::new(id.to_string(), peers, transport);

    let raw_store: Arc<dyn RaftPersistenceStore> =
        Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine)));
    let strict_store: Arc<dyn RaftPersistenceStore> =
        Arc::new(FailClosedPersistenceStore::new(raw_store));
    let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(APPLY_CHANNEL_CAPACITY);
    let apply_state_machine = Arc::clone(&state_machine);
    let apply_gate = lag_apply.then(|| Arc::new(Notify::new()));
    let task_gate = apply_gate.as_ref().map(Arc::clone);
    let apply_task = tokio::spawn(async move {
        while let Some(committed) = apply_rx.recv().await {
            if let Some(gate) = &task_gate {
                gate.notified().await;
            }
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

    node = node
        .with_snapshot_store(snapshot_store)
        .with_persistence(strict_store)
        .with_confirmed_apply_tx(apply_tx);
    node.set_election_timeout_ms(if lag_apply { 500 } else { 70 });
    let readiness = node.serving_readiness();
    let (client_tx, shared, handle) = node.spawn();

    ClusterNode {
        _db: db,
        engine,
        clock,
        client_tx,
        shared,
        readiness,
        handle: Some(handle),
        apply_task,
        _apply_gate: apply_gate,
    }
}

async fn cluster(lagged: Option<usize>) -> Vec<ClusterNode> {
    let bus = ChannelTransport::new_bus();
    let mut nodes = Vec::new();
    for (index, id) in VOTERS.iter().enumerate() {
        nodes.push(spawn_node(Arc::clone(&bus), id, lagged == Some(index)).await);
    }
    nodes
}

async fn leader_index(nodes: &[ClusterNode]) -> usize {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if node.handle.is_some()
                && node.readiness.load(Ordering::Acquire)
                && node.shared.lock().await.role == RaftRole::Leader
            {
                return index;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not elect a serving leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn submit(tx: &mpsc::Sender<ClientCommand>, payload: Vec<u8>) -> Result<u64, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ClientCommand {
        payload,
        reply: reply_tx,
    })
    .await
    .map_err(|_| "raft client channel closed".to_string())?;
    tokio::time::timeout(TIMEOUT, reply_rx)
        .await
        .map_err(|_| "raft client command timed out".to_string())?
        .map_err(|_| "raft client reply channel dropped".to_string())?
}

fn schema() -> TableSchema {
    TableSchema {
        name: "items".to_string(),
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

fn insert_plan(id: i64, name: &str) -> InsertPlan {
    InsertPlan {
        table: schema(),
        columns: vec!["id".to_string(), "name".to_string()],
        rows: vec![vec![SqlValue::Int(id), SqlValue::Text(name.to_string())]],
    }
}

fn snapshot_row_count(backup: &neuralbase::backup::NeuralBaseBackup) -> usize {
    let snapshot = ReplicatedSqlSnapshot::decode(&backup.sql_snapshot).unwrap();
    snapshot
        .tables
        .iter()
        .find(|table| table.schema.name == "items")
        .map(|table| table.rows.len())
        .unwrap_or(0)
}

async fn stop_all(nodes: &mut [ClusterNode]) {
    for node in nodes {
        node.stop().await;
    }
}

#[tokio::test]
async fn concurrent_sql_write_is_ordered_by_the_backup_boundary() {
    let mut nodes = cluster(None).await;
    let leader = leader_index(&nodes).await;
    let gateway = nodes[leader].gateway();
    gateway.create_table(schema()).await.unwrap();
    gateway.insert(&insert_plan(1, "before")).await.unwrap();

    let output = TempDir::new().unwrap();
    let destination = output.path().join("concurrent-sql.nbbk");
    let coordinator = nodes[leader].coordinator();
    let second = insert_plan(2, "raced");
    let (backup_result, insert_result) = tokio::join!(
        coordinator.create_online_backup_at(&destination, 1001),
        gateway.insert(&second)
    );
    let manifest = backup_result.unwrap();
    let insert = insert_result.unwrap();
    let backup = verify_backup_file(&destination).unwrap();
    assert_eq!(backup.manifest, manifest);

    let expected_rows = if manifest.metadata.last_included_index >= insert.raft_index {
        2
    } else {
        1
    };
    assert_eq!(snapshot_row_count(&backup), expected_rows);
    stop_all(&mut nodes).await;
}

#[tokio::test]
async fn concurrent_identity_mutation_is_ordered_by_the_backup_boundary() {
    let mut nodes = cluster(None).await;
    let leader = leader_index(&nodes).await;
    let gateway = nodes[leader].gateway();
    gateway.initialize_identity(&[]).await.unwrap();

    let output = TempDir::new().unwrap();
    let destination = output.path().join("concurrent-identity.nbbk");
    let coordinator = nodes[leader].coordinator();
    let (backup_result, create_result) = tokio::join!(
        coordinator.create_online_backup_at(&destination, 1002),
        gateway.create_user("alice", "phase5-race", false)
    );
    let manifest = backup_result.unwrap();
    let create = create_result.unwrap();
    let backup = verify_backup_file(&destination).unwrap();
    let snapshot = ReplicatedSqlSnapshot::decode(&backup.sql_snapshot).unwrap();
    let identity = ReplicatedIdentitySnapshotExtension::decode(&snapshot.metadata_extension).unwrap();
    let has_alice = match identity {
        ReplicatedIdentitySnapshotExtension::Initialized(state) => state.contains_user("alice"),
        ReplicatedIdentitySnapshotExtension::Uninitialized => false,
    };
    assert_eq!(
        has_alice,
        manifest.metadata.last_included_index >= create.raft_index
    );
    stop_all(&mut nodes).await;
}

#[tokio::test]
async fn membership_and_compaction_races_never_publish_torn_artifacts() {
    let mut nodes = cluster(None).await;
    let leader = leader_index(&nodes).await;
    let gateway = nodes[leader].gateway();
    gateway.prepare_mutation().await.unwrap();

    let output = TempDir::new().unwrap();
    let membership_destination = output.path().join("membership-race.nbbk");
    let coordinator = nodes[leader].coordinator();
    let add = encode_membership_change(&MembershipChange::AddLearner(
        "p5-online-fresh-learner".to_string(),
    ));
    let (backup_result, membership_result) = tokio::join!(
        coordinator.create_online_backup_at(&membership_destination, 1003),
        submit(&nodes[leader].client_tx, add)
    );
    let membership_index = membership_result.unwrap();
    match backup_result {
        Ok(manifest) => {
            let backup = verify_backup_file(&membership_destination).unwrap();
            assert_eq!(backup.manifest, manifest);
            assert_eq!(
                backup
                    .membership
                    .learners
                    .contains("p5-online-fresh-learner"),
                manifest.metadata.last_included_index >= membership_index
            );
        }
        Err(error) => {
            assert!(
                matches!(
                    error,
                    OnlineBackupError::MembershipBeyondBoundary { .. }
                        | OnlineBackupError::ConcurrentActivity
                ),
                "unexpected membership-race failure: {error}"
            );
            assert!(
                !membership_destination.exists(),
                "failed membership-raced backup published an artifact"
            );
        }
    }

    let compaction_destination = output.path().join("compaction-race.nbbk");
    let leader = leader_index(&nodes).await;
    let coordinator = nodes[leader].coordinator();
    let compact_target = nodes[leader].shared.lock().await.commit_index;
    let (backup_result, compact_result) = tokio::join!(
        coordinator.create_online_backup_at(&compaction_destination, 1004),
        submit(
            &nodes[leader].client_tx,
            encode_compact_log(compact_target, &[])
        )
    );
    let manifest = backup_result.unwrap();
    compact_result.unwrap();
    let backup = verify_backup_file(&compaction_destination).unwrap();
    assert_eq!(backup.manifest, manifest);
    assert!(manifest.metadata.last_included_index >= compact_target);
    stop_all(&mut nodes).await;
}

#[tokio::test]
async fn leader_backup_remains_available_with_one_apply_lagged_follower() {
    let lagged = 2;
    let mut nodes = cluster(Some(lagged)).await;
    let leader = leader_index(&nodes).await;
    assert_ne!(leader, lagged, "the intentionally stalled node must not lead");
    let gateway = nodes[leader].gateway();
    gateway.create_table(schema()).await.unwrap();
    gateway.insert(&insert_plan(1, "quorum-only")).await.unwrap();

    let output = TempDir::new().unwrap();
    let destination = output.path().join("lagged-follower.nbbk");
    let manifest = nodes[leader]
        .coordinator()
        .create_online_backup_at(&destination, 1005)
        .await
        .unwrap();
    let backup = verify_backup_file(&destination).unwrap();
    assert_eq!(snapshot_row_count(&backup), 1);
    assert!(
        nodes[lagged].shared.lock().await.last_applied < manifest.metadata.last_included_index,
        "test did not actually preserve follower apply lag"
    );
    stop_all(&mut nodes).await;
}

#[tokio::test]
async fn leadership_transfer_race_is_verified_success_or_no_publication() {
    let mut nodes = cluster(None).await;
    let leader = leader_index(&nodes).await;

    // A confirmed barrier requires a quorum replication and therefore proves
    // that at least one stable follower is caught up enough to be an eligible
    // transfer target before we begin the actual backup/transfer race.
    nodes[leader].gateway().prepare_mutation().await.unwrap();

    let output = TempDir::new().unwrap();
    let destination = output.path().join("leadership-transfer.nbbk");
    let coordinator = nodes[leader].coordinator();
    let handle = nodes[leader].handle.as_ref().unwrap();

    let (backup_result, transfer_result) = tokio::join!(
        coordinator.create_online_backup_at(&destination, 1006),
        handle.request_leader_transfer()
    );
    transfer_result.expect("a quorum-caught-up voter must be eligible for transfer");

    match backup_result {
        Ok(manifest) => {
            let backup = verify_backup_file(&destination).unwrap();
            assert_eq!(backup.manifest, manifest);
        }
        Err(_) => {
            assert!(
                !destination.exists(),
                "failed leadership-raced backup published an artifact"
            );
        }
    }
    stop_all(&mut nodes).await;
}
