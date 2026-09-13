// SPDX-License-Identifier: Apache-2.0
//! Phase-4 identity evidence across the Phase-3 membership lifecycle.
//!
//! The identity-bearing prefix is compacted before a brand-new learner is
//! admitted, forcing the empty learner to acquire authoritative identity through
//! the existing snapshot/catch-up machinery. The learner is then promoted,
//! participates in a credential rotation, and is removed while the surviving
//! voter set retains the same identity state.

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use neuralbase::catalog::InMemoryCatalog;
use neuralbase::consensus::{
    encode_compact_log, encode_membership_change, ChannelBus, ChannelTransport, ClientCommand,
    CommittedEntry, FailClosedPersistenceStore, MembershipChange, RaftNode, RaftPersistenceStore,
    RaftRole, RaftShared, RaftTaskHandle, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::HlcClock;
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::replicated_identity_store::ReplicatedIdentityState;
use neuralbase::replicated_snapshot_hooks::ReplicatedSqlSnapshotHooks;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

const TIMEOUT: Duration = Duration::from_secs(8);
const INITIAL: [&str; 3] = [
    "identity-member-a",
    "identity-member-b",
    "identity-member-c",
];
const LEARNER: &str = "identity-member-d";

type Shared = Arc<Mutex<RaftShared>>;

struct MemberNode {
    id: String,
    _dir: TempDir,
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Shared,
    _readiness: Arc<AtomicBool>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
}

impl MemberNode {
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

fn ids_set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| (*id).to_string()).collect()
}

async fn spawn_member(bus: ChannelBus, id: &str, learner: bool) -> MemberNode {
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
    let snapshot_store = Arc::new(ReplicatedSqlSnapshotHooks::new(
        Arc::clone(&engine),
        catalog,
        Arc::clone(&clock),
    ));
    let transport = Arc::new(ChannelTransport::register(id.to_string(), bus).await);

    let mut node = if learner {
        RaftNode::new_learner(
            id.to_string(),
            INITIAL.iter().map(|seed| (*seed).to_string()).collect(),
            transport,
        )
        .expect("valid learner bootstrap view")
    } else {
        let peers = INITIAL
            .iter()
            .filter(|peer| **peer != id)
            .map(|peer| (*peer).to_string())
            .collect();
        RaftNode::new(id.to_string(), peers, transport)
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
    node.set_election_timeout_ms(if learner { 250 } else { 70 });
    let readiness = node.serving_readiness();
    let (client_tx, shared, handle) = node.spawn();

    MemberNode {
        id: id.to_string(),
        _dir: dir,
        engine,
        clock,
        client_tx,
        shared,
        _readiness: readiness,
        handle: Some(handle),
        apply_task,
    }
}

async fn leader_index(nodes: &[MemberNode], voter_count: usize) -> usize {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        for (index, node) in nodes.iter().take(voter_count).enumerate() {
            if node.handle.is_some() && node.shared.lock().await.role == RaftRole::Leader {
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

async fn compact_at(node: &MemberNode, index: u64) -> u64 {
    submit(&node.client_tx, encode_compact_log(index, &[]))
        .await
        .unwrap()
}

async fn wait_identity(node: &MemberNode, expected: &ReplicatedIdentityState) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if ReplicatedIdentityState::load(&node.engine)
            .unwrap()
            .as_ref()
            == Some(expected)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} did not acquire identity",
            node.id
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_membership<F>(node: &MemberNode, predicate: F)
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

#[tokio::test]
async fn learner_bootstrap_promotion_rotation_and_removal_preserve_identity() {
    membership_lifecycle(false).await;
}

#[tokio::test]
async fn phase7_guarded_learner_snapshot_bootstrap_preserves_identity() {
    membership_lifecycle(true).await;
}

async fn change_member(
    node: &MemberNode,
    change: MembershipChange,
    guarded: bool,
) -> Result<u64, String> {
    if !guarded {
        return submit(&node.client_tx, encode_membership_change(&change)).await;
    }
    use neuralbase::consensus::operator_control::{GuardedMembership, MembershipGuard};
    let handle = node.handle.as_ref().unwrap().operator_handle();
    let status = handle.observe_authoritative().await?;
    handle
        .change_membership(GuardedMembership {
            guard: MembershipGuard {
                leader: status.id,
                term: status.term,
                generation: status.committed.generation,
            },
            change,
        })
        .await
}

async fn membership_lifecycle(guarded: bool) {
    let bus = ChannelTransport::new_bus();
    let mut nodes = Vec::new();
    for id in INITIAL {
        nodes.push(spawn_member(Arc::clone(&bus), id, false).await);
    }

    let leader = leader_index(&nodes, 3).await;
    let gateway = nodes[leader].gateway();
    gateway.initialize_identity(&[]).await.unwrap();
    let create = gateway
        .create_user("alice", "before-learner", false)
        .await
        .unwrap();
    let before = ReplicatedIdentityState::load(&nodes[leader].engine)
        .unwrap()
        .unwrap();
    for node in &nodes {
        wait_identity(node, &before).await;
    }

    assert_eq!(
        compact_at(&nodes[leader], create.raft_index).await,
        create.raft_index
    );

    nodes.push(spawn_member(Arc::clone(&bus), LEARNER, true).await);
    let leader = leader_index(&nodes, 3).await;
    change_member(
        &nodes[leader],
        MembershipChange::AddLearner(LEARNER.to_string()),
        guarded,
    )
    .await
    .unwrap();

    wait_membership(&nodes[3], |state| {
        state.membership.is_learner(&LEARNER.to_string())
    })
    .await;
    wait_identity(&nodes[3], &before).await;

    let promotion_deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let leader = leader_index(&nodes, 3).await;
        match change_member(
            &nodes[leader],
            MembershipChange::PromoteLearner(LEARNER.to_string()),
            guarded,
        )
        .await
        {
            Ok(_) => break,
            Err(error) if error.contains("caught up") => {
                assert!(
                    tokio::time::Instant::now() < promotion_deadline,
                    "learner never became promotable: {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("unexpected promotion failure: {error}"),
        }
    }

    let four = ids_set(&[
        "identity-member-a",
        "identity-member-b",
        "identity-member-c",
        "identity-member-d",
    ]);
    for node in &nodes {
        wait_membership(node, |state| {
            !state.membership.is_joint() && state.membership.voters == four
        })
        .await;
    }

    let leader = leader_index(&nodes, 4).await;
    nodes[leader]
        .gateway()
        .alter_user("alice", "after-promotion")
        .await
        .unwrap();
    let after = ReplicatedIdentityState::load(&nodes[leader].engine)
        .unwrap()
        .unwrap();
    assert_ne!(after, before);
    for node in &nodes {
        wait_identity(node, &after).await;
    }

    let leader = leader_index(&nodes, 4).await;
    change_member(
        &nodes[leader],
        MembershipChange::RemoveNode(LEARNER.to_string()),
        guarded,
    )
    .await
    .unwrap();

    let three = ids_set(&[
        "identity-member-a",
        "identity-member-b",
        "identity-member-c",
    ]);
    for node in &nodes[..3] {
        wait_membership(node, |state| {
            !state.membership.is_joint()
                && state.membership.voters == three
                && state.membership.removed.contains(LEARNER)
        })
        .await;
        wait_identity(node, &after).await;
    }

    for node in &mut nodes {
        node.stop().await;
    }
}
