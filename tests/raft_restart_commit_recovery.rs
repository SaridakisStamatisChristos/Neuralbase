// SPDX-License-Identifier: Apache-2.0
//! Restart recovery regression for prior-term entries whose commit index was volatile.
//!
//! A whole-cluster restart reloads the durable log but reconstructs `commit_index`
//! from the snapshot boundary. A newly elected leader must establish a current-term
//! commit point before Raft may advance over prior-term entries. These regressions
//! reproduce the condition that previously made the Phase-4 process identity test
//! intermittent and prove persisted processes stay non-serving until recovery has
//! reached a safe durable apply frontier.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    ChannelTransport, ClusterMembership, MemPersistenceStore, PersistentState, RaftNode,
    RaftPersistenceStore, RaftRole,
};
use tokio::sync::mpsc;

#[tokio::test]
async fn elected_leader_commits_prior_term_tail_after_whole_cluster_restart() {
    let ids = ["restart-a", "restart-b", "restart-c"];
    let membership = ClusterMembership::bootstrap(
        ids[0].to_string(),
        vec![ids[1].to_string(), ids[2].to_string()],
    );
    let prior_payload = b"quorum-durable-prior-term-entry".to_vec();

    // Model three disks after a crash. The prior-term entry exists durably on
    // every voter, while volatile commit/apply indexes are intentionally absent.
    let mut stores = Vec::new();
    for _ in &ids {
        let store = Arc::new(MemPersistenceStore::new());
        let mut state = PersistentState::new();
        state.current_term = 1;
        state.membership = Some(membership.clone());
        assert_eq!(state.append(1, prior_payload.clone()), 1);
        store.save(&state, &[]).expect("seed durable Raft state");
        stores.push(store);
    }

    let bus = ChannelTransport::new_bus();
    let mut transports = Vec::new();
    for id in &ids {
        transports.push(Arc::new(
            ChannelTransport::register((*id).to_string(), Arc::clone(&bus)).await,
        ));
    }

    let mut shareds = Vec::new();
    let mut readiness = Vec::new();
    let mut apply_rxs = Vec::new();
    let mut handles = Vec::new();
    for (ordinal, id) in ids.iter().enumerate() {
        let peers = ids
            .iter()
            .filter(|candidate| candidate != &id)
            .map(|candidate| (*candidate).to_string())
            .collect();
        let persistence: Arc<dyn RaftPersistenceStore> = stores[ordinal].clone();
        let (apply_tx, apply_rx) = mpsc::channel(8);
        let mut node = RaftNode::new((*id).to_string(), peers, Arc::clone(&transports[ordinal]))
            .with_persistence(persistence)
            .with_apply_tx(apply_tx);
        let node_readiness = node.serving_readiness();
        assert!(
            !node_readiness.load(Ordering::Acquire),
            "persisted node {} must start fail-closed before recovery",
            ids[ordinal]
        );
        // Strongly stagger elections so the regression is deterministic rather
        // than depending on scheduler timing in CI.
        node.set_election_timeout_ms(40 + ordinal as u64 * 180);
        let (_client_tx, shared, handle) = node.spawn();
        shareds.push(shared);
        readiness.push(node_readiness);
        apply_rxs.push(apply_rx);
        handles.push(handle);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let mut leader_count = 0usize;
        let mut fully_replayed = true;
        for shared in &shareds {
            let state = shared.lock().await;
            if state.role == RaftRole::Leader {
                leader_count += 1;
            }
            if state.commit_index < 2 || state.last_applied < 2 {
                fully_replayed = false;
            }
        }
        if leader_count == 1 && fully_replayed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "restart cluster never committed/applied the prior-term tail via a current-term barrier"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    for (ordinal, ready) in readiness.iter().enumerate() {
        assert!(
            ready.load(Ordering::Acquire),
            "node {} remained non-serving after recovery apply completed",
            ids[ordinal]
        );
    }

    for (ordinal, rx) in apply_rxs.iter_mut().enumerate() {
        let first = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("prior-term entry apply must be bounded")
            .expect("apply channel must remain open");
        let second = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("recovery barrier apply must be bounded")
            .expect("apply channel must remain open");

        assert_eq!(first.index, 1, "node {} replay order", ids[ordinal]);
        assert_eq!(
            first.command, prior_payload,
            "node {} prior payload",
            ids[ordinal]
        );
        assert_eq!(second.index, 2, "node {} barrier index", ids[ordinal]);
        assert!(
            second.command.is_empty(),
            "node {} recovery barrier must be a state-machine no-op",
            ids[ordinal]
        );
        assert!(
            second.term > first.term,
            "node {} barrier must belong to the newly elected term",
            ids[ordinal]
        );
    }

    for handle in handles {
        handle.shutdown().await;
    }
}

#[tokio::test]
async fn persisted_leader_stays_unready_until_recovery_barrier_is_confirmed_applied() {
    let id = "restart-readiness";
    let store = Arc::new(MemPersistenceStore::new());
    let mut state = PersistentState::new();
    state.current_term = 5;
    state.membership = Some(ClusterMembership::bootstrap(
        id.to_string(),
        Vec::<String>::new(),
    ));
    store.save(&state, &[]).expect("seed durable Raft state");

    let bus = ChannelTransport::new_bus();
    let transport = Arc::new(ChannelTransport::register(id.to_string(), bus).await);
    let persistence: Arc<dyn RaftPersistenceStore> = store;
    let (apply_tx, mut apply_rx) = mpsc::channel(4);
    let mut node = RaftNode::new(id.to_string(), Vec::new(), transport)
        .with_persistence(persistence)
        .with_confirmed_apply_tx(apply_tx);
    node.set_election_timeout_ms(20);
    let readiness = node.serving_readiness();
    assert!(
        !readiness.load(Ordering::Acquire),
        "persisted single-node leader must start non-serving"
    );

    let (_client_tx, shared, handle) = node.spawn();
    let committed = tokio::time::timeout(Duration::from_secs(1), apply_rx.recv())
        .await
        .expect("recovery barrier must reach confirmed apply")
        .expect("confirmed apply channel must remain open");
    assert_eq!(committed.entry.index, 1);
    assert!(committed.entry.command.is_empty());
    assert!(committed.entry.term > 5);
    assert!(
        !readiness.load(Ordering::Acquire),
        "leader became serving before recovery barrier apply completed"
    );

    committed
        .completion
        .send(Ok(()))
        .expect("Raft must still await recovery barrier completion");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while !readiness.load(Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "leader never became ready after recovery barrier apply"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let recovered = shared.lock().await;
    assert_eq!(recovered.role, RaftRole::Leader);
    assert_eq!(recovered.commit_index, 1);
    assert_eq!(recovered.last_applied, 1);
    drop(recovered);

    handle.shutdown().await;
}
