// SPDX-License-Identifier: Apache-2.0
//! Phase-5 fresh-generation cluster disaster-recovery evidence.
//!
//! The source is a real three-voter RocksDB-backed Raft cluster. After an
//! offline operator backup, recovery restores exactly one designated fresh
//! bootstrap node, admits two entirely fresh learners through the existing
//! snapshot/suffix path, promotes them under joint consensus, rejects reuse of
//! historical source identities, survives leader loss, accepts a new write, and
//! then converges again after a full three-node restart.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use neuralbase::binder::{InsertPlan, SqlValue};
use neuralbase::catalog::{ColumnDef, InMemoryCatalog, TableSchema};
use neuralbase::consensus::{
    encode_membership_change, ChannelBus, ChannelTransport, ClientCommand, CommittedEntry,
    FailClosedPersistenceStore, MembershipChange, RaftNode, RaftPersistenceStore, RaftRole,
    RaftShared, RaftTaskHandle, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::HlcTimestamp;
use neuralbase::offline_backup::{create_offline_backup_at, verify_backup_file};
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::replicated_identity_store::ReplicatedIdentityState;
use neuralbase::replicated_snapshot_hooks::ReplicatedSqlSnapshotHooks;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::restore::restore_new_cluster;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::table_id_for;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

const TIMEOUT: Duration = Duration::from_secs(10);
const SOURCE: [&str; 3] = ["p5-source-a", "p5-source-b", "p5-source-c"];
const RECOVERY: [&str; 3] = ["p5-recovery-a", "p5-recovery-b", "p5-recovery-c"];

type Shared = Arc<Mutex<RaftShared>>;

#[derive(Clone, Copy)]
enum Bootstrap<'a> {
    Voter(&'a [&'a str]),
    Learner(&'a [&'a str]),
}

struct RecoveryNode {
    id: String,
    path: PathBuf,
    engine: Arc<StorageEngine>,
    clock: Arc<neuralbase::hlc::HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Shared,
    readiness: Arc<AtomicBool>,
    handle: Option<RaftTaskHandle>,
    apply_task: Option<tokio::task::JoinHandle<()>>,
}

impl RecoveryNode {
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
        if let Some(task) = self.apply_task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

fn ids_set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| (*id).to_string()).collect()
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

async fn spawn_node(
    bus: ChannelBus,
    id: &str,
    path: &Path,
    bootstrap: Bootstrap<'_>,
    election_timeout_ms: u64,
) -> RecoveryNode {
    let engine = Arc::new(StorageEngine::open(path).unwrap());
    let catalog = Arc::new(InMemoryCatalog::default());
    let clock = Arc::new(neuralbase::hlc::HlcClock::new(500));
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
    let mut node = match bootstrap {
        Bootstrap::Voter(voters) => {
            let peers = voters
                .iter()
                .copied()
                .filter(|peer| *peer != id)
                .map(str::to_string)
                .collect();
            RaftNode::new(id.to_string(), peers, transport)
        }
        Bootstrap::Learner(seeds) => RaftNode::new_learner(
            id.to_string(),
            seeds.iter().map(|seed| (*seed).to_string()).collect(),
            transport,
        )
        .expect("valid recovery learner seed view"),
    };

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

    node = node
        .with_snapshot_store(snapshot_store)
        .with_persistence(strict_store)
        .with_confirmed_apply_tx(apply_tx);
    node.set_election_timeout_ms(election_timeout_ms);
    let readiness = node.serving_readiness();
    let (client_tx, shared, handle) = node.spawn();

    RecoveryNode {
        id: id.to_string(),
        path: path.to_path_buf(),
        engine,
        clock,
        client_tx,
        shared,
        readiness,
        handle: Some(handle),
        apply_task: Some(apply_task),
    }
}

async fn leader_index(nodes: &[RecoveryNode]) -> usize {
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

async fn wait_membership<F>(node: &RecoveryNode, predicate: F)
where
    F: Fn(&RaftShared) -> bool,
{
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if predicate(&*node.shared.lock().await) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "membership did not converge on {}",
            node.id
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_logical_state(
    node: &RecoveryNode,
    expected_rows: usize,
    expected_identity: &ReplicatedIdentityState,
) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let rows = node
            .engine
            .scan_table(table_id_for("items"), HlcTimestamp::MAX)
            .unwrap();
        let identity = ReplicatedIdentityState::load(&node.engine).unwrap();
        if rows.len() == expected_rows && identity.as_ref() == Some(expected_identity) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "logical state did not converge on {}",
            node.id
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn add_and_promote(nodes: &[RecoveryNode], learner_index: usize) {
    let learner_id = nodes[learner_index].id.clone();
    let leader = leader_index(nodes).await;
    submit(
        &nodes[leader].client_tx,
        encode_membership_change(&MembershipChange::AddLearner(learner_id.clone())),
    )
    .await
    .expect("fresh learner admission must commit");

    wait_membership(&nodes[learner_index], |state| {
        state.membership.is_learner(&learner_id)
    })
    .await;

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let leader = leader_index(nodes).await;
        match submit(
            &nodes[leader].client_tx,
            encode_membership_change(&MembershipChange::PromoteLearner(learner_id.clone())),
        )
        .await
        {
            Ok(_) => return,
            Err(error) if error.contains("caught up") => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "learner never became promotable: {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("unexpected promotion failure: {error}"),
        }
    }
}

async fn stop_all(nodes: &mut [RecoveryNode]) {
    for node in nodes {
        node.stop().await;
    }
}

#[tokio::test]
async fn restored_cluster_uses_fresh_generation_and_survives_failover_and_full_restart() {
    let root = TempDir::new().unwrap();

    // Build a real three-voter source cluster with durable SQL and SCRAM state.
    let source_bus = ChannelTransport::new_bus();
    let source_paths: Vec<_> = SOURCE
        .iter()
        .map(|id| root.path().join(format!("{id}.db")))
        .collect();
    let mut source_nodes = Vec::new();
    for (index, id) in SOURCE.iter().enumerate() {
        source_nodes.push(
            spawn_node(
                Arc::clone(&source_bus),
                id,
                &source_paths[index],
                Bootstrap::Voter(&SOURCE),
                60 + index as u64 * 40,
            )
            .await,
        );
    }

    let source_leader = leader_index(&source_nodes).await;
    let gateway = source_nodes[source_leader].gateway();
    gateway.create_table(schema()).await.unwrap();
    gateway.insert(&insert_plan(1, "before-disaster")).await.unwrap();
    gateway.initialize_identity(&[]).await.unwrap();
    gateway
        .create_user("alice", "phase5-recovery-secret", false)
        .await
        .unwrap();
    let source_identity = ReplicatedIdentityState::load(&source_nodes[source_leader].engine)
        .unwrap()
        .unwrap();
    let source_rows = source_nodes[source_leader]
        .engine
        .scan_table(table_id_for("items"), HlcTimestamp::MAX)
        .unwrap();
    assert_eq!(source_rows.len(), 1);
    for node in &source_nodes {
        wait_logical_state(node, 1, &source_identity).await;
    }
    drop(gateway);

    let source_backup_db = source_nodes[source_leader].path.clone();
    stop_all(&mut source_nodes).await;
    drop(source_nodes);

    // The operator artifact is created only after the source database is truly
    // offline, then independently verified before restore is attempted.
    let backup_path = root.path().join("phase5-cluster-recovery.nbbk");
    let source_manifest = create_offline_backup_at(&source_backup_db, &backup_path, 5_001)
        .expect("offline source backup must succeed");
    let backup = verify_backup_file(&backup_path).expect("backup must independently verify");
    assert_eq!(backup.manifest, source_manifest);
    let source_generation = backup.membership.generation;
    let source_history = ids_set(&SOURCE);
    assert_eq!(backup.membership.voters, source_history);

    // Restore only one designated fresh authority. Historical source IDs are
    // tombstoned and the recovery membership generation advances explicitly.
    let recovery_paths: Vec<_> = RECOVERY
        .iter()
        .map(|id| root.path().join(format!("{id}.db")))
        .collect();
    let report = restore_new_cluster(&backup_path, &recovery_paths[0], RECOVERY[0])
        .expect("designated recovery-node restore must succeed");
    assert_eq!(report.recovery_membership_generation, source_generation + 1);

    let recovery_bus = ChannelTransport::new_bus();
    let mut recovery_nodes = vec![
        spawn_node(
            Arc::clone(&recovery_bus),
            RECOVERY[0],
            &recovery_paths[0],
            Bootstrap::Voter(&[RECOVERY[0]]),
            60,
        )
        .await,
    ];
    assert_eq!(leader_index(&recovery_nodes).await, 0);
    wait_logical_state(&recovery_nodes[0], 1, &source_identity).await;
    assert_eq!(
        recovery_nodes[0]
            .engine
            .scan_table(table_id_for("items"), HlcTimestamp::MAX)
            .unwrap(),
        source_rows,
        "single restored authority must expose the exact backed-up logical rows"
    );
    wait_membership(&recovery_nodes[0], |state| {
        state.membership.voters == ids_set(&[RECOVERY[0]])
            && state.membership.removed.is_superset(&source_history)
            && state.membership.generation >= source_generation + 1
    })
    .await;

    // Admit two empty disks through the existing learner + snapshot/suffix
    // protocol. No copied consensus directory is used to manufacture voters.
    recovery_nodes.push(
        spawn_node(
            Arc::clone(&recovery_bus),
            RECOVERY[1],
            &recovery_paths[1],
            Bootstrap::Learner(&[RECOVERY[0]]),
            350,
        )
        .await,
    );
    add_and_promote(&recovery_nodes, 1).await;
    for node in &recovery_nodes {
        wait_logical_state(node, 1, &source_identity).await;
    }
    let first_learner_store =
        RocksDbRaftPersistenceStore::new(Arc::clone(&recovery_nodes[1].engine));
    let (first_learner_persistent, _) = first_learner_store
        .load()
        .unwrap()
        .expect("promoted learner must persist recovered Raft state");
    assert!(
        first_learner_persistent.snapshot_index >= report.boundary_index,
        "empty recovery learner must bootstrap through the restored snapshot boundary"
    );
    let two_voters = ids_set(&RECOVERY[..2]);
    for node in &recovery_nodes {
        wait_membership(node, |state| {
            !state.membership.is_joint()
                && state.membership.voters == two_voters
                && state.membership.removed.is_superset(&source_history)
        })
        .await;
    }

    recovery_nodes.push(
        spawn_node(
            Arc::clone(&recovery_bus),
            RECOVERY[2],
            &recovery_paths[2],
            Bootstrap::Learner(&RECOVERY[..2]),
            450,
        )
        .await,
    );
    add_and_promote(&recovery_nodes, 2).await;
    let three_voters = ids_set(&RECOVERY);
    for node in &recovery_nodes {
        wait_membership(node, |state| {
            !state.membership.is_joint()
                && state.membership.voters == three_voters
                && state.membership.removed.is_superset(&source_history)
                && state.membership.generation > source_generation + 1
        })
        .await;
        wait_logical_state(node, 1, &source_identity).await;
    }

    // A historical source identity is permanently fenced by the restore
    // tombstones; re-admission requires an entirely new incarnation ID.
    let leader = leader_index(&recovery_nodes).await;
    let reused = submit(
        &recovery_nodes[leader].client_tx,
        encode_membership_change(&MembershipChange::AddLearner(SOURCE[0].to_string())),
    )
    .await
    .expect_err("historical source identity must not be reusable");
    assert!(
        reused.contains("was removed and cannot be reused"),
        "unexpected historical-id rejection: {reused}"
    );

    // Lose the current leader, re-elect from the remaining fresh generation,
    // and prove a new replicated write can commit without the failed process.
    let failed_leader = leader_index(&recovery_nodes).await;
    recovery_nodes[failed_leader].stop().await;
    let new_leader = leader_index(&recovery_nodes).await;
    assert_ne!(new_leader, failed_leader);
    recovery_nodes[new_leader]
        .gateway()
        .insert(&insert_plan(2, "after-disaster-recovery"))
        .await
        .unwrap();
    for (index, node) in recovery_nodes.iter().enumerate() {
        if index != failed_leader {
            wait_logical_state(node, 2, &source_identity).await;
        }
    }

    // Full cluster restart from three independent disks. Deliberately pass an
    // empty bootstrap peer list: persisted recovered membership must remain the
    // authority, the stale former leader must catch up, and all state converges.
    stop_all(&mut recovery_nodes).await;
    drop(recovery_nodes);

    let restart_bus = ChannelTransport::new_bus();
    let mut restarted = Vec::new();
    for (index, id) in RECOVERY.iter().enumerate() {
        restarted.push(
            spawn_node(
                Arc::clone(&restart_bus),
                id,
                &recovery_paths[index],
                Bootstrap::Voter(&[]),
                60 + index as u64 * 40,
            )
            .await,
        );
    }

    let restart_leader = leader_index(&restarted).await;
    assert!(restart_leader < restarted.len());
    for node in &restarted {
        wait_membership(node, |state| {
            !state.membership.is_joint()
                && state.membership.voters == three_voters
                && state.membership.removed.is_superset(&source_history)
                && state.membership.generation > source_generation + 1
        })
        .await;
        wait_logical_state(node, 2, &source_identity).await;
    }

    let canonical_rows = restarted[0]
        .engine
        .scan_table(table_id_for("items"), HlcTimestamp::MAX)
        .unwrap();
    assert_eq!(canonical_rows.len(), 2);
    for node in &restarted[1..] {
        assert_eq!(
            node.engine
                .scan_table(table_id_for("items"), HlcTimestamp::MAX)
                .unwrap(),
            canonical_rows,
            "full restart must converge to byte-identical visible row state"
        );
        assert_eq!(
            ReplicatedIdentityState::load(&node.engine).unwrap(),
            Some(source_identity.clone()),
            "full restart must preserve exact replicated identity state"
        );
    }

    stop_all(&mut restarted).await;
}
