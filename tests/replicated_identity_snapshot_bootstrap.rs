// SPDX-License-Identifier: Apache-2.0
//! Phase-4 identity bootstrap proof over the existing Raft snapshot machinery.
//!
//! A fixed member is destroyed after an identity-bearing snapshot, an identity
//! credential rotation commits while that member is absent, and the empty
//! replacement must recover the snapshot plus retained suffix before serving.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use neuralbase::catalog::InMemoryCatalog;
use neuralbase::consensus::{
    encode_compact_log, ChannelBus, ChannelTransport, ClientCommand, CommittedEntry,
    FailClosedPersistenceStore, RaftNode, RaftPersistenceStore, RaftRole, RaftShared,
    RaftTaskHandle, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::HlcClock;
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::{ReplicatedGatewayError, ReplicatedSqlGateway};
use neuralbase::replicated_identity_store::ReplicatedIdentityState;
use neuralbase::replicated_snapshot_hooks::ReplicatedSqlSnapshotHooks;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

const IDS: [&str; 3] = [
    "identity-snapshot-a",
    "identity-snapshot-b",
    "identity-snapshot-c",
];
const REPLACEMENT_ELECTION_TIMEOUT_MS: u64 = 5_000;

struct SnapshotNode {
    id: String,
    dir: TempDir,
    engine: Arc<StorageEngine>,
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
        drop(self.clock);
        drop(self.readiness);
        dir
    }
}

async fn spawn_node(
    bus: ChannelBus,
    id: &str,
    election_timeout_ms: u64,
    dir: TempDir,
) -> SnapshotNode {
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
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
            "expected replacement never became leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_ready(node: &SnapshotNode) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while !node.readiness.load(Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "replacement never became serving-ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_identity(nodes: &[SnapshotNode], expected: &ReplicatedIdentityState) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let converged = nodes.iter().all(|node| {
            ReplicatedIdentityState::load(&node.engine)
                .unwrap()
                .as_ref()
                == Some(expected)
        });
        if converged {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "identity did not converge"
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

#[tokio::test]
async fn empty_member_recovers_identity_from_snapshot_plus_suffix_before_serving() {
    let bus = ChannelTransport::new_bus();
    let mut nodes = Vec::new();
    for (ordinal, id) in IDS.iter().enumerate() {
        nodes.push(
            spawn_node(
                Arc::clone(&bus),
                id,
                1_000 + ordinal as u64 * 1_500,
                TempDir::new().unwrap(),
            )
            .await,
        );
    }

    let leader_id = wait_for_leader(&nodes).await;
    let leader_pos = nodes.iter().position(|node| node.id == leader_id).unwrap();
    let gateway = nodes[leader_pos].gateway();
    gateway.initialize_identity(&[]).await.unwrap();
    let create = gateway
        .create_user("snapshot-user", "before-snapshot", false)
        .await
        .unwrap();
    let before = ReplicatedIdentityState::load(&nodes[leader_pos].engine)
        .unwrap()
        .unwrap();
    wait_for_identity(&nodes, &before).await;

    let boundary = create.raft_index;
    assert_eq!(compact_at(&nodes[leader_pos], boundary).await, boundary);
    let persisted = RocksDbRaftPersistenceStore::new(Arc::clone(&nodes[leader_pos].engine))
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(persisted.0.snapshot_index, boundary);
    assert!(!persisted.1.is_empty());

    let victim_id = IDS.iter().find(|id| **id != leader_id).unwrap().to_string();
    let victim_pos = nodes.iter().position(|node| node.id == victim_id).unwrap();
    let victim = nodes.remove(victim_pos);
    let destroyed_dir = victim.shutdown_into_dir().await;
    drop(destroyed_dir);

    let leader_pos = nodes.iter().position(|node| node.id == leader_id).unwrap();
    let alter = nodes[leader_pos]
        .gateway()
        .alter_user("snapshot-user", "after-snapshot")
        .await
        .unwrap();
    assert!(alter.raft_index > boundary);
    let after = ReplicatedIdentityState::load(&nodes[leader_pos].engine)
        .unwrap()
        .unwrap();
    assert_ne!(after, before);
    wait_for_identity(&nodes, &after).await;

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
    wait_for_identity(&nodes, &after).await;

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
    nodes[replacement_pos]
        .gateway()
        .drop_user("snapshot-user", false)
        .await
        .unwrap();
    let dropped = ReplicatedIdentityState::load(&nodes[replacement_pos].engine)
        .unwrap()
        .unwrap();
    assert!(!dropped.contains_user("snapshot-user"));
    wait_for_identity(&nodes, &dropped).await;

    for node in &mut nodes {
        node.stop().await;
    }
}
