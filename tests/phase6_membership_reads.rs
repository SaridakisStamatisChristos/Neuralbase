// SPDX-License-Identifier: Apache-2.0
//! Phase-6 strong-read evidence across the Phase-3 learner lifecycle.

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    encode_membership_change, ChannelBus, ChannelTransport, ClientCommand, MembershipChange,
    RaftNode, RaftRole, RaftShared, RaftTaskHandle,
};
use neuralbase::hlc::HlcClock;
use neuralbase::read_barrier::prepare_read_with_timeout;
use neuralbase::read_consistency::ReadConsistency;
use neuralbase::replicated_gateway::ReplicatedSqlGateway;
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, Mutex};

const WAIT: Duration = Duration::from_secs(5);
type Shared = Arc<Mutex<RaftShared>>;

struct Node {
    _dir: TempDir,
    gateway: ReplicatedSqlGateway,
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Shared,
    handle: Option<RaftTaskHandle>,
}

async fn spawn_voter(id: &str, voters: &[&str], bus: &ChannelBus) -> Node {
    let transport = Arc::new(ChannelTransport::register(id.to_string(), Arc::clone(bus)).await);
    let peers = voters
        .iter()
        .copied()
        .filter(|peer| *peer != id)
        .map(str::to_string)
        .collect();
    let mut raft = RaftNode::new(id.to_string(), peers, transport);
    raft.set_election_timeout_ms(60);
    spawn_with_storage(raft).await
}

async fn spawn_learner(id: &str, voters: &[&str], bus: &ChannelBus) -> Node {
    let transport = Arc::new(ChannelTransport::register(id.to_string(), Arc::clone(bus)).await);
    let seeds = voters.iter().map(|id| (*id).to_string()).collect();
    let mut raft = RaftNode::new_learner(id.to_string(), seeds, transport).unwrap();
    raft.set_election_timeout_ms(60);
    spawn_with_storage(raft).await
}

async fn spawn_with_storage(raft: RaftNode<ChannelTransport>) -> Node {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let clock = Arc::new(HlcClock::new(500));
    let (client_tx, shared, handle) = raft.spawn();
    let gateway = ReplicatedSqlGateway::new_with_readiness(
        client_tx.clone(),
        Arc::clone(&shared),
        engine,
        clock,
        Arc::new(AtomicBool::new(true)),
    );
    Node {
        _dir: dir,
        gateway,
        client_tx,
        shared,
        handle: Some(handle),
    }
}

async fn leader(nodes: &[Node]) -> usize {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        for (index, node) in nodes.iter().enumerate() {
            if node.shared.lock().await.role == RaftRole::Leader {
                return index;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "leader election timed out");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn submit(tx: &mpsc::Sender<ClientCommand>, payload: Vec<u8>) -> Result<u64, String> {
    let (reply, response) = oneshot::channel();
    tx.send(ClientCommand { payload, reply })
        .await
        .map_err(|_| "client channel closed".to_string())?;
    match tokio::time::timeout(WAIT, response).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("reply channel closed".to_string()),
        Err(_) => Err("command timed out".to_string()),
    }
}

async fn wait_membership<F>(shared: &Shared, predicate: F)
where
    F: Fn(&RaftShared) -> bool,
{
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if predicate(&*shared.lock().await) {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "membership did not converge");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn stop_all(nodes: &mut [Node]) {
    for node in nodes {
        if let Some(handle) = node.handle.take() {
            handle.shutdown().await;
        }
    }
}

#[tokio::test]
async fn strong_reads_survive_learner_catchup_promotion_and_finalized_four_voter_config() {
    let bus = ChannelTransport::new_bus();
    let voters = ["p6m-a", "p6m-b", "p6m-c"];
    let learner_id = "p6m-d";
    let mut nodes = Vec::new();
    for id in voters {
        nodes.push(spawn_voter(id, &voters, &bus).await);
    }
    nodes.push(spawn_learner(learner_id, &voters, &bus).await);

    let leader_index = leader(&nodes[..3]).await;
    submit(
        &nodes[leader_index].client_tx,
        encode_membership_change(&MembershipChange::AddLearner(learner_id.to_string())),
    )
    .await
    .expect("learner admission");

    wait_membership(&nodes[3].shared, |state| {
        state.membership.learners.contains(learner_id)
    })
    .await;

    // Reads remain available on the voter leader while the fourth process is
    // explicitly non-voting and catching up. The barrier itself is replicated
    // to the learner without allowing it to count toward quorum.
    prepare_read_with_timeout(
        Some(&nodes[leader_index].gateway),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let leader_index = leader(&nodes[..3]).await;
        match submit(
            &nodes[leader_index].client_tx,
            encode_membership_change(&MembershipChange::PromoteLearner(learner_id.to_string())),
        )
        .await
        {
            Ok(_) => break,
            Err(error) if error.contains("caught up") => {
                assert!(tokio::time::Instant::now() < deadline, "learner never caught up");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("unexpected promotion failure: {error}"),
        }
    }

    let expected: BTreeSet<String> = ["p6m-a", "p6m-b", "p6m-c", "p6m-d"]
        .into_iter()
        .map(str::to_string)
        .collect();
    for node in &nodes {
        wait_membership(&node.shared, |state| {
            state.membership.joint.is_none() && state.membership.voters == expected
        })
        .await;
    }

    let promoted_leader = leader(&nodes).await;
    prepare_read_with_timeout(
        Some(&nodes[promoted_leader].gateway),
        ReadConsistency::Linearizable,
        WAIT,
    )
    .await
    .unwrap();
    let state = nodes[promoted_leader].shared.lock().await.clone();
    assert_eq!(state.last_applied, state.commit_index);

    stop_all(&mut nodes).await;
}
