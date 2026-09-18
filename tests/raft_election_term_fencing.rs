// SPDX-License-Identifier: Apache-2.0
// Regression coverage for election/reply term fencing and leadership transfer.

use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    AppendEntriesArgs, AppendEntriesReply, ChannelTransport, MemPersistenceStore, RaftMessage,
    RaftNode, RaftPersistenceStore, RaftRole, RaftShared, RequestVoteReply, Transport,
};

async fn wait_for_role(
    shared: &Arc<tokio::sync::Mutex<RaftShared>>,
    expected: RaftRole,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if shared.lock().await.role == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Raft role {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn wait_for_term_at_least(
    store: &MemPersistenceStore,
    minimum: u64,
    timeout: Duration,
) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some((state, _)) = store.load().expect("load persisted Raft state") {
            if state.current_term >= minimum {
                return state.current_term;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for Raft term >= {minimum}"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

#[tokio::test]
async fn stale_granted_vote_reply_cannot_elect_candidate_in_newer_term() {
    let bus = ChannelTransport::new_bus();
    let target_id = "candidate".to_string();
    let voter_id = "voter".to_string();
    let other_id = "other".to_string();

    let target_transport =
        Arc::new(ChannelTransport::register(target_id.clone(), Arc::clone(&bus)).await);
    let voter = ChannelTransport::register(voter_id.clone(), Arc::clone(&bus)).await;
    let store = Arc::new(MemPersistenceStore::new());
    let store_dyn: Arc<dyn RaftPersistenceStore> = store.clone();
    let mut target = RaftNode::new(
        target_id.clone(),
        vec![voter_id.clone(), other_id],
        target_transport,
    )
    .with_persistence(store_dyn);
    target.set_election_timeout_ms(80);
    let (_cmd_tx, shared, handle) = target.spawn();

    wait_for_role(&shared, RaftRole::Candidate, Duration::from_secs(2)).await;
    let first_term = wait_for_term_at_least(&store, 1, Duration::from_secs(1)).await;
    let newer_term = wait_for_term_at_least(&store, first_term + 1, Duration::from_secs(3)).await;
    assert!(newer_term > first_term);
    wait_for_role(&shared, RaftRole::Candidate, Duration::from_millis(100)).await;

    voter
        .send(
            &target_id,
            RaftMessage::RequestVoteReply(RequestVoteReply {
                term: first_term,
                vote_granted: true,
            }),
        )
        .await;

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_ne!(
        shared.lock().await.role,
        RaftRole::Leader,
        "stale granted vote reply elected a candidate in a newer term"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn same_term_append_entries_demotes_competing_leader() {
    let bus = ChannelTransport::new_bus();
    let target_id = "target".to_string();
    let voter_id = "voter".to_string();
    let competing_id = "competing".to_string();

    let target_transport =
        Arc::new(ChannelTransport::register(target_id.clone(), Arc::clone(&bus)).await);
    let voter = ChannelTransport::register(voter_id.clone(), Arc::clone(&bus)).await;
    let competing = ChannelTransport::register(competing_id.clone(), Arc::clone(&bus)).await;
    let store = Arc::new(MemPersistenceStore::new());
    let store_dyn: Arc<dyn RaftPersistenceStore> = store.clone();
    let mut target = RaftNode::new(
        target_id.clone(),
        vec![voter_id.clone(), competing_id.clone()],
        target_transport,
    )
    .with_persistence(store_dyn);
    target.set_election_timeout_ms(500);
    let (_cmd_tx, shared, handle) = target.spawn();

    wait_for_role(&shared, RaftRole::Candidate, Duration::from_secs(2)).await;
    let term = wait_for_term_at_least(&store, 1, Duration::from_secs(1)).await;
    voter
        .send(
            &target_id,
            RaftMessage::RequestVoteReply(RequestVoteReply {
                term,
                vote_granted: true,
            }),
        )
        .await;
    wait_for_role(&shared, RaftRole::Leader, Duration::from_millis(250)).await;

    competing
        .send(
            &target_id,
            RaftMessage::AppendEntries(AppendEntriesArgs {
                term,
                leader_id: competing_id.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        )
        .await;

    wait_for_role(&shared, RaftRole::Follower, Duration::from_millis(250)).await;
    assert_eq!(
        shared.lock().await.leader_id.as_ref(),
        Some(&competing_id),
        "same-term leader demotion did not publish the legitimate leader"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn stale_append_entries_reply_cannot_advance_leader_match_state() {
    let bus = ChannelTransport::new_bus();
    let target_id = "leader".to_string();
    let voter_id = "voter".to_string();
    let other_id = "other".to_string();

    let target_transport =
        Arc::new(ChannelTransport::register(target_id.clone(), Arc::clone(&bus)).await);
    let voter = ChannelTransport::register(voter_id.clone(), Arc::clone(&bus)).await;
    let _other = ChannelTransport::register(other_id.clone(), Arc::clone(&bus)).await;
    let store = Arc::new(MemPersistenceStore::new());
    let store_dyn: Arc<dyn RaftPersistenceStore> = store.clone();
    let mut target = RaftNode::new(
        target_id.clone(),
        vec![voter_id.clone(), other_id],
        target_transport,
    )
    .with_persistence(store_dyn);
    target.set_election_timeout_ms(500);
    let (_cmd_tx, shared, handle) = target.spawn();

    wait_for_role(&shared, RaftRole::Candidate, Duration::from_secs(2)).await;
    let term = wait_for_term_at_least(&store, 1, Duration::from_secs(1)).await;
    voter
        .send(
            &target_id,
            RaftMessage::RequestVoteReply(RequestVoteReply {
                term,
                vote_granted: true,
            }),
        )
        .await;
    wait_for_role(&shared, RaftRole::Leader, Duration::from_millis(250)).await;

    voter
        .send(
            &target_id,
            RaftMessage::AppendEntriesReply(AppendEntriesReply {
                term: term.saturating_sub(1),
                success: true,
                match_index: 999,
            }),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    let status = handle
        .operator_handle()
        .status()
        .await
        .expect("operator status after stale reply");
    assert_eq!(
        status.matched.get(&voter_id).copied().unwrap_or(0),
        0,
        "stale replication acknowledgement mutated current-term leader progress"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn timeout_now_from_unrecognized_voter_does_not_start_election() {
    let bus = ChannelTransport::new_bus();
    let target_id = "target".to_string();
    let voter_id = "voter".to_string();
    let other_id = "other".to_string();

    let target_transport =
        Arc::new(ChannelTransport::register(target_id.clone(), Arc::clone(&bus)).await);
    let voter = ChannelTransport::register(voter_id, Arc::clone(&bus)).await;
    let mut target = RaftNode::new(
        target_id.clone(),
        vec!["voter".into(), other_id],
        target_transport,
    );
    target.set_election_timeout_ms(1_000);
    let (_cmd_tx, shared, handle) = target.spawn();

    voter
        .send(&target_id, RaftMessage::TimeoutNow { term: 0 })
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        shared.lock().await.role,
        RaftRole::Follower,
        "TimeoutNow from a voter that is not the recognized leader triggered an election"
    );

    handle.shutdown().await;
}
