// SPDX-License-Identifier: Apache-2.0
//! Phase-6 consensus-barrier safety evidence.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use neuralbase::consensus::{
    encode_membership_change, ClientCommand, CommittedEntry, MembershipChange, NodeId, RaftMessage,
    RaftNode, RaftRole, RaftShared, RaftTaskHandle, Transport, APPLY_CHANNEL_CAPACITY,
};
use neuralbase::hlc::HlcClock;
use neuralbase::read_barrier::{prepare_read_with_timeout, ReadBarrierError};
use neuralbase::read_consistency::ReadConsistency;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

const IDS: [&str; 3] = ["p6-a", "p6-b", "p6-c"];
const WAIT: Duration = Duration::from_secs(5);

type Bus = Arc<Mutex<HashMap<NodeId, mpsc::Sender<(NodeId, RaftMessage)>>>>;
type Blocks = Arc<Mutex<HashSet<(NodeId, NodeId)>>>;

struct PartitionTransport {
    id: NodeId,
    bus: Bus,
    blocked: Blocks,
    rx: Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>,
}

impl PartitionTransport {
    async fn register(id: NodeId, bus: Bus, blocked: Blocks) -> Self {
        let (tx, rx) = mpsc::channel(4096);
        bus.lock().await.insert(id.clone(), tx);
        Self {
            id,
            bus,
            blocked,
            rx: Mutex::new(rx),
        }
    }
}

#[async_trait]
impl Transport for PartitionTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) {
        if self
            .blocked
            .lock()
            .await
            .contains(&(self.id.clone(), to.clone()))
        {
            return;
        }
        let tx = self.bus.lock().await.get(to).cloned();
        if let Some(tx) = tx {
            let _ = tx.send((self.id.clone(), msg)).await;
        }
    }

    async fn recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().await.recv().await
    }
}

struct TestNode {
    _db: TempDir,
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    handle: Option<RaftTaskHandle>,
    apply_task: tokio::task::JoinHandle<()>,
}

impl TestNode {
    fn gateway(&self) -> ReplicatedSqlGateway {
        ReplicatedSqlGateway::new_with_readiness(
            self.client_tx.clone(),
            Arc::clone(&self.shared),
            Arc::clone(&self.engine),
            Arc::clone(&self.clock),
            Arc::new(AtomicBool::new(true)),
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

async fn spawn_node(id: &str, bus: Bus, blocked: Blocks, election_ms: u64) -> TestNode {
    let db = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(db.path()).unwrap());
    let clock = Arc::new(HlcClock::new(500));
    let transport = Arc::new(PartitionTransport::register(id.to_string(), bus, blocked).await);
    let peers = IDS
        .iter()
        .copied()
        .filter(|peer| *peer != id)
        .map(str::to_string)
        .collect();

    let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(APPLY_CHANNEL_CAPACITY);
    let apply_task = tokio::spawn(async move {
        while let Some(committed) = apply_rx.recv().await {
            let _ = committed.completion.send(Ok(()));
        }
    });

    let mut raft =
        RaftNode::new(id.to_string(), peers, transport).with_confirmed_apply_tx(apply_tx);
    raft.set_election_timeout_ms(election_ms);
    let (client_tx, shared, handle) = raft.spawn();
    TestNode {
        _db: db,
        engine,
        clock,
        client_tx,
        shared,
        handle: Some(handle),
        apply_task,
    }
}

async fn cluster() -> (Vec<TestNode>, Blocks) {
    let bus = Arc::new(Mutex::new(HashMap::new()));
    let blocked = Arc::new(Mutex::new(HashSet::new()));
    let mut nodes = Vec::new();
    for (idx, id) in IDS.iter().enumerate() {
        nodes.push(
            spawn_node(
                id,
                Arc::clone(&bus),
                Arc::clone(&blocked),
                70 + idx as u64 * 140,
            )
            .await,
        );
    }
    (nodes, blocked)
}

async fn leader(nodes: &[TestNode], excluded: Option<usize>) -> usize {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        for (idx, node) in nodes.iter().enumerate() {
            if excluded == Some(idx) || node.handle.is_none() {
                continue;
            }
            if node.shared.lock().await.role == RaftRole::Leader {
                return idx;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader election timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn submit(tx: &mpsc::Sender<ClientCommand>, payload: &[u8]) -> u64 {
    let (reply, response) = oneshot::channel();
    tx.send(ClientCommand {
        payload: payload.to_vec(),
        reply,
    })
    .await
    .unwrap();
    tokio::time::timeout(WAIT, response)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn isolate(index: usize, blocked: &Blocks) {
    let isolated = IDS[index].to_string();
    let mut guard = blocked.lock().await;
    for other in IDS.iter().copied().filter(|id| *id != IDS[index]) {
        guard.insert((isolated.clone(), other.to_string()));
        guard.insert((other.to_string(), isolated.clone()));
    }
}

async fn stop_all(nodes: &mut [TestNode]) {
    for node in nodes {
        node.stop().await;
    }
}

#[tokio::test]
async fn linearizable_barrier_follows_acknowledged_write_and_is_locally_applied() {
    let (mut nodes, _) = cluster().await;
    let leader = leader(&nodes, None).await;

    let write_index = submit(&nodes[leader].client_tx, b"phase6-test-write").await;
    prepare_read_with_timeout(
        Some(&nodes[leader].gateway()),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .unwrap();

    let state = nodes[leader].shared.lock().await.clone();
    assert!(
        state.commit_index > write_index,
        "read barrier did not advance the committed frontier"
    );
    assert_eq!(
        state.last_applied, state.commit_index,
        "strong read returned before its barrier was applied"
    );
    assert!(state.last_applied > write_index);
    stop_all(&mut nodes).await;
}

#[tokio::test]
async fn follower_explicitly_rejects_strong_read_instead_of_serving_local_state() {
    let (mut nodes, _) = cluster().await;
    let leader = leader(&nodes, None).await;
    let follower = (0..nodes.len()).find(|idx| *idx != leader).unwrap();

    let error = prepare_read_with_timeout(
        Some(&nodes[follower].gateway()),
        ReadConsistency::Leader,
        Duration::from_millis(500),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            error,
            ReadBarrierError::Gateway(
                neuralbase::replicated_gateway::ReplicatedGatewayError::NotLeader { .. }
            )
        ),
        "unexpected follower strong-read error: {error}"
    );
    stop_all(&mut nodes).await;
}

#[tokio::test]
async fn isolated_former_leader_cannot_manufacture_authoritative_or_linearizable_read() {
    let (mut nodes, blocked) = cluster().await;
    let old_leader = leader(&nodes, None).await;
    prepare_read_with_timeout(
        Some(&nodes[old_leader].gateway()),
        ReadConsistency::Leader,
        WAIT,
    )
    .await
    .unwrap();

    isolate(old_leader, &blocked).await;
    let new_leader = leader(&nodes, Some(old_leader)).await;
    assert_ne!(old_leader, new_leader);

    for mode in [ReadConsistency::Leader, ReadConsistency::Linearizable] {
        let result = prepare_read_with_timeout(
            Some(&nodes[old_leader].gateway()),
            mode,
            Duration::from_millis(300),
        )
        .await;
        assert!(result.is_err(), "isolated former leader served {mode} read");
    }

    prepare_read_with_timeout(
        Some(&nodes[new_leader].gateway()),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .unwrap();
    let new_state = nodes[new_leader].shared.lock().await.clone();
    assert_eq!(new_state.last_applied, new_state.commit_index);
    stop_all(&mut nodes).await;
}

#[tokio::test]
async fn membership_changes_do_not_count_learners_and_strong_reads_survive_reconfiguration() {
    let (mut nodes, _) = cluster().await;
    let leader_idx = leader(&nodes, None).await;

    let learner_id = "p6-nonvoting-learner".to_string();
    let add = encode_membership_change(&MembershipChange::AddLearner(learner_id.clone()));
    submit(&nodes[leader_idx].client_tx, &add).await;
    assert!(nodes[leader_idx]
        .shared
        .lock()
        .await
        .membership
        .learners
        .contains(&learner_id));

    // The absent learner cannot contribute to quorum; the voter quorum alone
    // must establish the strong-read barrier.
    prepare_read_with_timeout(
        Some(&nodes[leader_idx].gateway()),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .unwrap();

    let remove_learner = encode_membership_change(&MembershipChange::RemoveNode(learner_id));
    submit(&nodes[leader_idx].client_tx, &remove_learner).await;

    let removed_voter = (0..IDS.len()).find(|idx| *idx != leader_idx).unwrap();
    let remove_voter = encode_membership_change(&MembershipChange::RemoveNode(
        IDS[removed_voter].to_string(),
    ));
    submit(&nodes[leader_idx].client_tx, &remove_voter).await;

    // The implementation auto-finalizes a committed joint transition. A read
    // submitted immediately after the joint change must either commit under
    // the joint quorum or after the final configuration; it may never use the
    // removed member as a fabricated quorum shortcut.
    prepare_read_with_timeout(
        Some(&nodes[leader_idx].gateway()),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let membership = nodes[leader_idx].shared.lock().await.membership.clone();
        if membership.joint.is_none() && membership.removed.contains(IDS[removed_voter]) {
            assert!(!membership.voters.contains(IDS[removed_voter]));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "membership did not finalize"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop_all(&mut nodes).await;
}

#[tokio::test]
async fn leader_transfer_reestablishes_authority_before_strong_reads_succeed() {
    let (mut nodes, _) = cluster().await;
    let old_leader = leader(&nodes, None).await;
    prepare_read_with_timeout(
        Some(&nodes[old_leader].gateway()),
        ReadConsistency::Leader,
        WAIT,
    )
    .await
    .unwrap();

    nodes[old_leader]
        .handle
        .as_ref()
        .unwrap()
        .request_leader_transfer()
        .await
        .expect("leader transfer");
    let new_leader = leader(&nodes, Some(old_leader)).await;
    assert_ne!(new_leader, old_leader);

    prepare_read_with_timeout(
        Some(&nodes[new_leader].gateway()),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .unwrap();
    let state = nodes[new_leader].shared.lock().await.clone();
    assert_eq!(state.last_applied, state.commit_index);
    stop_all(&mut nodes).await;
}
