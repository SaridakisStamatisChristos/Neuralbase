// SPDX-License-Identifier: Apache-2.0
//! Independent acknowledgement tests for regular Raft client commands.

use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{ChannelTransport, ClientCommand, RaftNode, RaftRole, RaftShared};
use tokio::sync::{oneshot, Mutex};

async fn wait_for_leader(shared: &Arc<Mutex<RaftShared>>) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1_000);
    loop {
        if shared.lock().await.role == RaftRole::Leader {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "single-node Raft leader election timed out"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn regular_client_success_waits_for_confirmed_state_machine_apply() {
    let bus = ChannelTransport::new_bus();
    let transport = Arc::new(ChannelTransport::register("ack1".into(), bus).await);
    let mut node = RaftNode::new("ack1".into(), vec![], transport);
    node.set_election_timeout_ms(30);
    let (apply_tx, mut apply_rx) = tokio::sync::mpsc::channel(4);
    let node = node.with_confirmed_apply_tx(apply_tx);
    let (cmd_tx, shared, _handle) = node.spawn();
    wait_for_leader(&shared).await;

    let (reply_tx, mut reply_rx) = oneshot::channel::<Result<u64, String>>();
    cmd_tx
        .send(ClientCommand {
            payload: b"regular-command".to_vec(),
            reply: reply_tx,
        })
        .await
        .expect("Raft command channel open");

    let committed = tokio::time::timeout(Duration::from_millis(500), apply_rx.recv())
        .await
        .expect("quorum-committed entry reaches apply channel")
        .expect("apply channel open");
    assert_eq!(committed.entry.index, 1);

    assert!(
        matches!(
            reply_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "client must not receive success before durable apply acknowledgement"
    );

    committed
        .completion
        .send(Ok(()))
        .expect("Raft must still await apply completion");
    let result = tokio::time::timeout(Duration::from_millis(500), reply_rx)
        .await
        .expect("client reply after apply")
        .expect("reply channel open");
    assert_eq!(result, Ok(1));

    let state = shared.lock().await.clone();
    assert_eq!(state.commit_index, 1);
    assert_eq!(state.last_applied, 1);
}

#[tokio::test]
async fn state_machine_failure_never_returns_sql_success() {
    let bus = ChannelTransport::new_bus();
    let transport = Arc::new(ChannelTransport::register("ack2".into(), bus).await);
    let mut node = RaftNode::new("ack2".into(), vec![], transport);
    node.set_election_timeout_ms(30);
    let (apply_tx, mut apply_rx) = tokio::sync::mpsc::channel(4);
    let node = node.with_confirmed_apply_tx(apply_tx);
    let (cmd_tx, shared, _handle) = node.spawn();
    wait_for_leader(&shared).await;

    let (reply_tx, reply_rx) = oneshot::channel::<Result<u64, String>>();
    cmd_tx
        .send(ClientCommand {
            payload: b"must-not-succeed".to_vec(),
            reply: reply_tx,
        })
        .await
        .expect("Raft command channel open");

    let committed = tokio::time::timeout(Duration::from_millis(500), apply_rx.recv())
        .await
        .expect("committed entry reaches apply channel")
        .expect("apply channel open");
    committed
        .completion
        .send(Err("injected durable apply failure".to_string()))
        .expect("Raft awaits failure result");

    let result = tokio::time::timeout(Duration::from_millis(500), reply_rx)
        .await
        .expect("failure reply is bounded")
        .expect("reply channel open");
    let error = result.expect_err("apply failure must not become success");
    assert!(error.contains("injected durable apply failure"));

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(shared.lock().await.last_applied, 0);

    let (next_reply, _next_rx) = oneshot::channel();
    assert!(
        cmd_tx
            .send(ClientCommand {
                payload: b"after-fail-stop".to_vec(),
                reply: next_reply,
            })
            .await
            .is_err(),
        "Raft node must fail-stop when a committed state-machine apply fails"
    );
}
