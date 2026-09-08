// SPDX-License-Identifier: Apache-2.0
//! Phase-4 replicated identity guarantees over the real Raft state machine.
//!
//! Every node owns an independent RocksDB directory and applies committed Raft
//! entries through its own state machine. These tests therefore prove identity
//! convergence through replication rather than shared memory.

use std::sync::Arc;
use std::time::Duration;

use neuralbase::catalog::InMemoryCatalog;
use neuralbase::consensus::{
    ChannelTransport, ClientCommand, CommittedEntry, FailClosedPersistenceStore, RaftNode,
    RaftPersistenceStore, RaftRole, RaftShared, RaftTaskHandle, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::HlcClock;
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::{ReplicatedGatewayError, ReplicatedSqlGateway};
use neuralbase::replicated_identity_store::ReplicatedIdentityState;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;
use tokio::sync::{mpsc, Mutex};

struct NodeHarness {
    id: String,
    _dir: TempDir,
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
}

impl NodeHarness {
    fn gateway(&self) -> ReplicatedSqlGateway {
        ReplicatedSqlGateway::new(
            self.client_tx.clone(),
            Arc::clone(&self.shared),
            Arc::clone(&self.engine),
            Arc::clone(&self.clock),
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

async fn spawn_three_node_cluster() -> Vec<NodeHarness> {
    let bus = ChannelTransport::new_bus();
    let ids = ["identity-a", "identity-b", "identity-c"];
    let mut nodes = Vec::new();

    for (ordinal, id) in ids.iter().enumerate() {
        let dir = TempDir::new().unwrap();
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

        let transport =
            Arc::new(ChannelTransport::register((*id).to_string(), Arc::clone(&bus)).await);
        let peers = ids
            .iter()
            .filter(|peer| *peer != id)
            .map(|peer| (*peer).to_string())
            .collect();
        let raw_store: Arc<dyn RaftPersistenceStore> =
            Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine)));
        let strict_store: Arc<dyn RaftPersistenceStore> =
            Arc::new(FailClosedPersistenceStore::new(raw_store));
        let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(APPLY_CHANNEL_CAPACITY);
        let sm = Arc::clone(&state_machine);
        let apply_task = tokio::spawn(async move {
            while let Some(committed) = apply_rx.recv().await {
                let result = sm
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

        let mut node = RaftNode::new((*id).to_string(), peers, transport)
            .with_persistence(strict_store)
            .with_confirmed_apply_tx(apply_tx);
        node.set_election_timeout_ms(50 + ordinal as u64 * 15);
        let (client_tx, shared, handle) = node.spawn();

        nodes.push(NodeHarness {
            id: (*id).to_string(),
            _dir: dir,
            engine,
            clock,
            client_tx,
            shared,
            handle: Some(handle),
            apply_task,
        });
    }

    nodes
}

async fn wait_for_leader(nodes: &[NodeHarness], excluded: Option<usize>) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if excluded == Some(index) || node.handle.is_none() {
                continue;
            }
            if node.shared.lock().await.role == RaftRole::Leader {
                return index;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_identity(
    nodes: &[NodeHarness],
    expected: &ReplicatedIdentityState,
    excluded: Option<usize>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        let mut converged = true;
        for (index, node) in nodes.iter().enumerate() {
            if excluded == Some(index) || node.handle.is_none() {
                continue;
            }
            converged &= ReplicatedIdentityState::load(&node.engine).unwrap().as_ref()
                == Some(expected);
        }
        if converged {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "replicated identity state did not converge"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn identity_converges_survives_leader_loss_and_rotates_on_new_leader() {
    let mut nodes = spawn_three_node_cluster().await;
    let leader = wait_for_leader(&nodes, None).await;
    let gateway = nodes[leader].gateway();

    gateway.initialize_identity(&[]).await.unwrap();
    let create = gateway.create_user("alice", "before-failover", false).await.unwrap();
    assert_eq!(create.command_tag, "CREATE USER");

    let before = ReplicatedIdentityState::load(&nodes[leader].engine)
        .unwrap()
        .unwrap();
    assert!(before.contains_user("alice"));
    wait_for_identity(&nodes, &before, None).await;

    let before_bytes = before.encode().unwrap();
    let old_leader_id = nodes[leader].id.clone();
    nodes[leader].stop().await;

    let new_leader = wait_for_leader(&nodes, Some(leader)).await;
    assert_ne!(nodes[new_leader].id, old_leader_id);
    wait_for_identity(&nodes, &before, Some(leader)).await;

    let alter = nodes[new_leader]
        .gateway()
        .alter_user("alice", "after-failover")
        .await
        .unwrap();
    assert_eq!(alter.command_tag, "ALTER USER");
    let after = ReplicatedIdentityState::load(&nodes[new_leader].engine)
        .unwrap()
        .unwrap();
    assert_ne!(after.encode().unwrap(), before_bytes);
    wait_for_identity(&nodes, &after, Some(leader)).await;

    nodes[new_leader]
        .gateway()
        .drop_user("alice", false)
        .await
        .unwrap();
    let dropped = ReplicatedIdentityState::load(&nodes[new_leader].engine)
        .unwrap()
        .unwrap();
    assert!(!dropped.contains_user("alice"));
    wait_for_identity(&nodes, &dropped, Some(leader)).await;

    for (index, node) in nodes.iter_mut().enumerate() {
        if index != leader {
            node.stop().await;
        }
    }
}

#[tokio::test]
async fn follower_rejects_identity_ddl_without_local_side_effect() {
    let mut nodes = spawn_three_node_cluster().await;
    let leader = wait_for_leader(&nodes, None).await;
    let follower = (0..nodes.len()).find(|index| *index != leader).unwrap();

    let error = nodes[follower]
        .gateway()
        .create_user("mallory", "must-not-apply", true)
        .await
        .unwrap_err();
    assert!(matches!(error, ReplicatedGatewayError::NotLeader { .. }));
    assert!(ReplicatedIdentityState::load(&nodes[follower].engine)
        .unwrap()
        .is_none());

    for node in &mut nodes {
        node.stop().await;
    }
}
