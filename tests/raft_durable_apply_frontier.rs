// SPDX-License-Identifier: Apache-2.0
//! Durable state-machine frontier recovery for a live-cluster follower restart.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    AppendEntriesArgs, ChannelTransport, ClusterMembership, MemPersistenceStore, PersistentState,
    RaftMessage, RaftNode, RaftPersistenceStore, RaftRole, Transport,
};
use tokio::sync::mpsc;

#[tokio::test]
async fn restarted_follower_recovers_applied_prefix_without_replaying_it() {
    let a = "frontier-a".to_string();
    let b = "frontier-b".to_string();
    let c = "frontier-c".to_string();
    let membership = ClusterMembership::bootstrap(a.clone(), vec![b.clone(), c.clone()]);

    let store = Arc::new(MemPersistenceStore::new());
    let mut state = PersistentState::new();
    state.current_term = 3;
    state.membership = Some(membership);
    assert_eq!(state.append(3, b"one".to_vec()), 1);
    assert_eq!(state.append(3, b"two".to_vec()), 2);
    store.save(&state, &[]).expect("seed retained Raft state");

    let bus = ChannelTransport::new_bus();
    let a_transport = Arc::new(ChannelTransport::register(a.clone(), Arc::clone(&bus)).await);
    let b_transport = Arc::new(ChannelTransport::register(b.clone(), Arc::clone(&bus)).await);
    let persistence: Arc<dyn RaftPersistenceStore> = store;
    let (apply_tx, mut apply_rx) = mpsc::channel(8);

    let mut node = RaftNode::new(a.clone(), vec![b.clone(), c], a_transport)
        .with_persistence(persistence)
        .with_durable_apply_frontier(2)
        .with_confirmed_apply_tx(apply_tx);
    node.set_election_timeout_ms(1_000);
    let readiness = node.serving_readiness();
    assert!(
        !readiness.load(Ordering::Acquire),
        "restart must remain fail-closed until a valid leader is observed"
    );

    let (_client_tx, shared, handle) = node.spawn();
    {
        let recovered = shared.lock().await;
        assert_eq!(recovered.commit_index, 2);
        assert_eq!(recovered.last_applied, 2);
        assert_eq!(recovered.role, RaftRole::Follower);
    }

    b_transport
        .send(
            &a,
            RaftMessage::AppendEntries(AppendEntriesArgs {
                term: 4,
                leader_id: b.clone(),
                prev_log_index: 2,
                prev_log_term: 3,
                entries: vec![],
                leader_commit: 2,
            }),
        )
        .await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while !readiness.load(Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "restarted follower never reopened readiness after leader heartbeat"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert!(
        tokio::time::timeout(Duration::from_millis(50), apply_rx.recv())
            .await
            .is_err(),
        "already-durable prefix was redundantly replayed"
    );
    let recovered = shared.lock().await;
    assert_eq!(recovered.role, RaftRole::Follower);
    assert_eq!(recovered.leader_id.as_deref(), Some(b.as_str()));
    assert_eq!(recovered.commit_index, 2);
    assert_eq!(recovered.last_applied, 2);
    drop(recovered);

    handle.shutdown().await;
}

#[tokio::test]
#[should_panic(expected = "beyond persisted Raft log end")]
async fn impossible_durable_frontier_fails_closed() {
    let id = "frontier-corrupt".to_string();
    let store = Arc::new(MemPersistenceStore::new());
    let mut state = PersistentState::new();
    state.membership = Some(ClusterMembership::bootstrap(
        id.clone(),
        Vec::<String>::new(),
    ));
    assert_eq!(state.append(1, b"only".to_vec()), 1);
    store.save(&state, &[]).expect("seed Raft state");

    let bus = ChannelTransport::new_bus();
    let transport = Arc::new(ChannelTransport::register(id.clone(), bus).await);
    let persistence: Arc<dyn RaftPersistenceStore> = store;
    let _ = RaftNode::new(id, vec![], transport)
        .with_persistence(persistence)
        .with_durable_apply_frontier(2);
}
