// SPDX-License-Identifier: Apache-2.0
//! Phase 3 dynamic-membership lifecycle evidence.
//!
//! These tests exercise the consensus surface as a running in-process cluster:
//! learner admission/catch-up/promotion, joint-consensus finalization, 3->4->3
//! contraction, finalized-config restart, and rejection of a removed node
//! restarted from stale pre-removal state.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    encode_membership_change, ChannelBus, ChannelTransport, ClientCommand, MemPersistenceStore,
    MembershipChange, RaftNode, RaftPersistenceStore, RaftRole, RaftShared, RaftTaskHandle,
};
use tokio::sync::{mpsc, oneshot, Mutex};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

type Shared = Arc<Mutex<RaftShared>>;

fn ids_set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| (*id).to_string()).collect()
}

async fn submit(
    tx: &mpsc::Sender<ClientCommand>,
    payload: Vec<u8>,
) -> Result<u64, String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(ClientCommand {
        payload,
        reply: reply_tx,
    })
    .await
    .map_err(|_| "raft client channel closed".to_string())?;

    match tokio::time::timeout(TEST_TIMEOUT, reply_rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("raft client reply channel dropped".to_string()),
        Err(_) => Err("raft client command timed out".to_string()),
    }
}

async fn leader_index(shareds: &[Shared]) -> usize {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        for (index, shared) in shareds.iter().enumerate() {
            if shared.lock().await.role == RaftRole::Leader {
                return index;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster did not elect a leader before timeout"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_membership<F>(shared: &Shared, predicate: F)
where
    F: Fn(&RaftShared) -> bool,
{
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        if predicate(&shared.lock().await) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "membership condition did not converge before timeout"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_applied(shareds: &[Shared], index: u64) {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        let mut complete = true;
        for shared in shareds {
            if shared.lock().await.last_applied < index {
                complete = false;
                break;
            }
        }
        if complete {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Raft index {index} did not apply on all expected nodes"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn spawn_voter(
    id: &str,
    voters: &[&str],
    bus: &ChannelBus,
    store: Option<Arc<MemPersistenceStore>>,
) -> (mpsc::Sender<ClientCommand>, Shared, RaftTaskHandle) {
    let peers = voters
        .iter()
        .copied()
        .filter(|peer| *peer != id)
        .map(str::to_string)
        .collect();
    let transport = Arc::new(ChannelTransport::register(id.to_string(), Arc::clone(bus)).await);
    let mut node = RaftNode::new(id.to_string(), peers, transport);
    if let Some(store) = store {
        let store: Arc<dyn RaftPersistenceStore> = store;
        node = node.with_persistence(store);
    }
    node.set_election_timeout_ms(60);
    node.spawn()
}

async fn spawn_learner(
    id: &str,
    seed_voters: &[&str],
    bus: &ChannelBus,
    store: Option<Arc<MemPersistenceStore>>,
) -> (mpsc::Sender<ClientCommand>, Shared, RaftTaskHandle) {
    let transport = Arc::new(ChannelTransport::register(id.to_string(), Arc::clone(bus)).await);
    let seeds = seed_voters.iter().map(|seed| (*seed).to_string()).collect();
    let mut node = RaftNode::new_learner(id.to_string(), seeds, transport)
        .expect("valid learner bootstrap view");
    if let Some(store) = store {
        let store: Arc<dyn RaftPersistenceStore> = store;
        node = node.with_persistence(store);
    }
    node.set_election_timeout_ms(60);
    node.spawn()
}

async fn add_and_promote_learner(
    learner_id: &str,
    client_txs: &[mpsc::Sender<ClientCommand>],
    shareds: &[Shared],
) {
    let leader = leader_index(shareds).await;
    submit(
        &client_txs[leader],
        encode_membership_change(&MembershipChange::AddLearner(learner_id.to_string())),
    )
    .await
    .expect("learner admission must commit under the current voter quorum");

    wait_membership(&shareds[shareds.len() - 1], |state| {
        state.membership.is_learner(&learner_id.to_string())
    })
    .await;

    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        let leader = leader_index(shareds).await;
        match submit(
            &client_txs[leader],
            encode_membership_change(&MembershipChange::PromoteLearner(
                learner_id.to_string(),
            )),
        )
        .await
        {
            Ok(_) => return,
            Err(error) if error.contains("caught up") => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "learner never became eligible for promotion: {error}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("unexpected promotion failure: {error}"),
        }
    }
}

#[tokio::test]
async fn phase3_three_to_four_to_three_lifecycle_preserves_writes() {
    let bus = ChannelTransport::new_bus();
    let initial = ["p3_life_a", "p3_life_b", "p3_life_c"];
    let learner = "p3_life_d";

    let mut client_txs = Vec::new();
    let mut shareds = Vec::new();
    let mut _handles = Vec::new();
    for id in initial {
        let (tx, shared, handle) = spawn_voter(id, &initial, &bus, None).await;
        client_txs.push(tx);
        shareds.push(shared);
        _handles.push(handle);
    }
    let (tx, shared, handle) = spawn_learner(learner, &initial, &bus, None).await;
    client_txs.push(tx);
    shareds.push(shared);
    _handles.push(handle);

    let leader = leader_index(&shareds[..3]).await;
    let before = submit(&client_txs[leader], b"phase3-before-expand".to_vec())
        .await
        .expect("pre-expansion write commits");
    wait_applied(&shareds[..3], before).await;

    add_and_promote_learner(learner, &client_txs, &shareds).await;

    let four = ids_set(&[
        "p3_life_a",
        "p3_life_b",
        "p3_life_c",
        "p3_life_d",
    ]);
    for shared in &shareds {
        wait_membership(shared, |state| {
            !state.membership.is_joint() && state.membership.voters == four
        })
        .await;
    }

    let leader = leader_index(&shareds).await;
    let expanded_write = submit(&client_txs[leader], b"phase3-after-expand".to_vec())
        .await
        .expect("write must commit with four voters");
    wait_applied(&shareds, expanded_write).await;

    assert_ne!(
        shareds[3].lock().await.role,
        RaftRole::Leader,
        "new learner should not unexpectedly replace the stable leader before contraction"
    );
    let leader = leader_index(&shareds).await;
    submit(
        &client_txs[leader],
        encode_membership_change(&MembershipChange::RemoveNode(learner.to_string())),
    )
    .await
    .expect("non-leader voter removal must pass through joint consensus");

    let final_three = ids_set(&["p3_life_a", "p3_life_b", "p3_life_c"]);
    for shared in &shareds[..3] {
        wait_membership(shared, |state| {
            !state.membership.is_joint()
                && state.membership.voters == final_three
                && state.membership.removed.contains(learner)
        })
        .await;
    }

    let leader = leader_index(&shareds[..3]).await;
    let contracted_write = submit(&client_txs[leader], b"phase3-after-contract".to_vec())
        .await
        .expect("write must commit after contraction to three voters");
    wait_applied(&shareds[..3], contracted_write).await;
}

#[tokio::test]
async fn phase3_restart_uses_finalized_membership_not_stale_bootstrap_peers() {
    let bus = ChannelTransport::new_bus();
    let initial = ["p3_restart_a", "p3_restart_b", "p3_restart_c"];
    let learner = "p3_restart_d";
    let stores: Vec<_> = (0..4)
        .map(|_| Arc::new(MemPersistenceStore::new()))
        .collect();

    let mut client_txs = Vec::new();
    let mut shareds = Vec::new();
    let mut handles = Vec::new();
    for (index, id) in initial.iter().enumerate() {
        let (tx, shared, handle) =
            spawn_voter(id, &initial, &bus, Some(Arc::clone(&stores[index]))).await;
        client_txs.push(tx);
        shareds.push(shared);
        handles.push(handle);
    }
    let (tx, shared, handle) =
        spawn_learner(learner, &initial, &bus, Some(Arc::clone(&stores[3]))).await;
    client_txs.push(tx);
    shareds.push(shared);
    handles.push(handle);

    add_and_promote_learner(learner, &client_txs, &shareds).await;
    let expected = ids_set(&[
        "p3_restart_a",
        "p3_restart_b",
        "p3_restart_c",
        "p3_restart_d",
    ]);
    for shared in &shareds {
        wait_membership(shared, |state| {
            !state.membership.is_joint() && state.membership.voters == expected
        })
        .await;
    }

    for handle in handles {
        handle.shutdown().await;
    }
    drop(client_txs);
    drop(shareds);

    // Deliberately restart every process with an incorrect one-node bootstrap
    // peer view. Persisted finalized membership must remain the quorum source of
    // truth and override this process-local seed information.
    let restart_bus = ChannelTransport::new_bus();
    let all_ids = [
        "p3_restart_a",
        "p3_restart_b",
        "p3_restart_c",
        "p3_restart_d",
    ];
    let mut restarted_txs = Vec::new();
    let mut restarted_shareds = Vec::new();
    let mut _restarted_handles = Vec::new();
    for (index, id) in all_ids.iter().enumerate() {
        let transport = Arc::new(
            ChannelTransport::register((*id).to_string(), Arc::clone(&restart_bus)).await,
        );
        let store: Arc<dyn RaftPersistenceStore> = Arc::clone(&stores[index]) as _;
        let mut node = RaftNode::new((*id).to_string(), vec![], transport).with_persistence(store);
        node.set_election_timeout_ms(60);
        let (tx, shared, handle) = node.spawn();
        restarted_txs.push(tx);
        restarted_shareds.push(shared);
        _restarted_handles.push(handle);
    }

    let leader = leader_index(&restarted_shareds).await;
    for shared in &restarted_shareds {
        wait_membership(shared, |state| {
            !state.membership.is_joint() && state.membership.voters == expected
        })
        .await;
    }

    let index = submit(
        &restarted_txs[leader],
        b"phase3-write-after-full-restart".to_vec(),
    )
    .await
    .expect("finalized membership must remain writable after full restart");
    wait_applied(&restarted_shareds, index).await;
}

#[tokio::test]
async fn phase3_removed_node_from_stale_disk_cannot_disrupt_current_quorum() {
    let bus = ChannelTransport::new_bus();
    let ids = ["p3_stale_a", "p3_stale_b", "p3_stale_c"];
    let stores: Vec<_> = (0..3)
        .map(|_| Arc::new(MemPersistenceStore::new()))
        .collect();

    let mut client_txs = Vec::new();
    let mut shareds = Vec::new();
    let mut handles: Vec<Option<RaftTaskHandle>> = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        let (tx, shared, handle) =
            spawn_voter(id, &ids, &bus, Some(Arc::clone(&stores[index]))).await;
        client_txs.push(tx);
        shareds.push(shared);
        handles.push(Some(handle));
    }

    let leader = leader_index(&shareds).await;
    let baseline = submit(&client_txs[leader], b"phase3-stale-baseline".to_vec())
        .await
        .expect("baseline write commits");
    wait_applied(&shareds, baseline).await;

    let target = (leader + 1) % 3;
    let target_id = ids[target];
    let stale_store = Arc::new(MemPersistenceStore::new());
    let (stale_state, stale_snapshot) = stores[target]
        .load()
        .expect("read target persistence")
        .expect("target has durable pre-removal state");
    stale_store
        .save(&stale_state, &stale_snapshot)
        .expect("copy stale pre-removal disk image");

    submit(
        &client_txs[leader],
        encode_membership_change(&MembershipChange::RemoveNode(target_id.to_string())),
    )
    .await
    .expect("follower removal must finalize");

    let survivors: Vec<_> = shareds
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != target)
        .map(|(_, shared)| Arc::clone(shared))
        .collect();
    let survivor_ids: Vec<&str> = ids
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != target)
        .map(|(_, id)| *id)
        .collect();
    let expected = ids_set(&survivor_ids);
    for shared in &survivors {
        wait_membership(shared, |state| {
            !state.membership.is_joint()
                && state.membership.voters == expected
                && state.membership.removed.contains(target_id)
        })
        .await;
    }

    handles[target]
        .take()
        .expect("removed process handle")
        .shutdown()
        .await;

    // Re-register the removed identity using its old disk image. It still
    // believes the pre-removal configuration, but current voters carry the
    // tombstone and must reject its candidacy before adopting its higher term.
    let stale_transport = Arc::new(
        ChannelTransport::register(target_id.to_string(), Arc::clone(&bus)).await,
    );
    let stale_peers = ids
        .iter()
        .copied()
        .filter(|id| *id != target_id)
        .map(str::to_string)
        .collect();
    let stale_store_dyn: Arc<dyn RaftPersistenceStore> = stale_store;
    let mut stale_node = RaftNode::new(target_id.to_string(), stale_peers, stale_transport)
        .with_persistence(stale_store_dyn);
    stale_node.set_election_timeout_ms(20);
    let (_stale_tx, stale_shared, _stale_handle) = stale_node.spawn();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_ne!(
        stale_shared.lock().await.role,
        RaftRole::Leader,
        "removed stale identity must never regain leadership"
    );

    let current_leader = leader_index(&survivors).await;
    let survivor_original_index = (0..3)
        .filter(|index| *index != target)
        .nth(current_leader)
        .expect("map survivor leader back to original index");
    let index = submit(
        &client_txs[survivor_original_index],
        b"phase3-write-after-stale-rejoin-attempt".to_vec(),
    )
    .await
    .expect("valid current quorum must stay writable despite stale removed node");
    wait_applied(&survivors, index).await;
}
