// SPDX-License-Identifier: Apache-2.0
// Regression coverage for Raft's at-most-one-vote-per-term invariant.

use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    AppendEntriesArgs, ChannelTransport, MemPersistenceStore, RaftMessage, RaftNode,
    RaftPersistenceStore, RaftRole, RaftShared, RequestVoteArgs, Transport,
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

#[tokio::test]
async fn same_term_append_entries_preserves_persisted_vote_and_rejects_second_candidate() {
    let bus = ChannelTransport::new_bus();
    let target_id = "target".to_string();
    let leader_id = "leader".to_string();
    let other_id = "other".to_string();

    // Keep both peers absent while target starts its first election. Its outbound
    // RequestVote RPCs are therefore dropped and it remains a candidate after
    // durably voting for itself.
    let target_transport = Arc::new(
        ChannelTransport::register(target_id.clone(), Arc::clone(&bus)).await,
    );
    let store = Arc::new(MemPersistenceStore::new());
    let store_dyn: Arc<dyn RaftPersistenceStore> = store.clone();
    let mut target = RaftNode::new(
        target_id.clone(),
        vec![leader_id.clone(), other_id.clone()],
        target_transport,
    )
    .with_persistence(store_dyn);
    target.set_election_timeout_ms(500);
    let (_cmd_tx, shared, handle) = target.spawn();

    wait_for_role(&shared, RaftRole::Candidate, Duration::from_secs(2)).await;
    let (candidate_state, _) = store
        .load()
        .expect("load candidate state")
        .expect("candidate election must persist Raft state");
    let term = candidate_state.current_term;
    assert_eq!(candidate_state.voted_for.as_deref(), Some(target_id.as_str()));

    // A legitimate leader in the same term makes the candidate step down. Raft
    // permits that role transition, but it MUST NOT erase the vote already cast
    // in this term.
    let leader = ChannelTransport::register(leader_id.clone(), Arc::clone(&bus)).await;
    leader
        .send(
            &target_id,
            RaftMessage::AppendEntries(AppendEntriesArgs {
                term,
                leader_id: leader_id.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        )
        .await;
    wait_for_role(&shared, RaftRole::Follower, Duration::from_millis(250)).await;

    let (demoted_state, _) = store
        .load()
        .expect("load demoted state")
        .expect("same-term demotion must remain durable");
    assert_eq!(demoted_state.current_term, term);
    assert_eq!(
        demoted_state.voted_for.as_deref(),
        Some(target_id.as_str()),
        "same-term demotion must preserve the vote already cast"
    );

    // A different candidate now asks for a vote in that same term. Granting it
    // would violate Raft's persistent at-most-one-vote-per-term invariant.
    let other = ChannelTransport::register(other_id.clone(), Arc::clone(&bus)).await;
    other
        .send(
            &target_id,
            RaftMessage::RequestVote(RequestVoteArgs {
                term,
                candidate_id: other_id,
                last_log_index: 0,
                last_log_term: 0,
            }),
        )
        .await;

    let (_from, reply) = tokio::time::timeout(Duration::from_secs(1), other.recv())
        .await
        .expect("timed out waiting for vote reply")
        .expect("target transport closed unexpectedly");
    match reply {
        RaftMessage::RequestVoteReply(reply) => {
            assert_eq!(reply.term, term);
            assert!(
                !reply.vote_granted,
                "voter granted a second vote in the same term"
            );
        }
        other => panic!("expected RequestVoteReply, got {other:?}"),
    }

    handle.shutdown().await;
}
