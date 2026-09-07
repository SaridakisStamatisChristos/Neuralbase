// SPDX-License-Identifier: Apache-2.0
//! Consensus-level fail-closed persistence tests.
//!
//! These tests go beyond exercising the persistence adapter directly: they
//! prove a live Raft client cannot observe success when the stable-log write for
//! its command fails.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    ChannelTransport, ClientCommand, FailClosedPersistenceStore, PersistentState, RaftNode,
    RaftPersistenceStore, RaftRole,
};
use tokio::sync::oneshot;

struct FailAfterFirstSave {
    saves: AtomicUsize,
}

impl FailAfterFirstSave {
    fn new() -> Self {
        Self {
            saves: AtomicUsize::new(0),
        }
    }
}

impl RaftPersistenceStore for FailAfterFirstSave {
    fn save(&self, _state: &PersistentState, _snapshot_data: &[u8]) -> Result<(), String> {
        let previous = self.saves.fetch_add(1, Ordering::SeqCst);
        if previous >= 1 {
            Err("injected log save failure".to_string())
        } else {
            Ok(())
        }
    }

    fn load(&self) -> Result<Option<(PersistentState, Vec<u8>)>, String> {
        Ok(None)
    }
}

#[tokio::test]
async fn command_persistence_failure_cannot_return_success() {
    let bus = ChannelTransport::new_bus();
    let transport = Arc::new(
        ChannelTransport::register("persist_fail_node".to_string(), Arc::clone(&bus)).await,
    );

    // The first save is the node's self-vote/current-term election record. The
    // second save is the first client log append and is injected to fail.
    let raw: Arc<dyn RaftPersistenceStore> = Arc::new(FailAfterFirstSave::new());
    let strict: Arc<dyn RaftPersistenceStore> =
        Arc::new(FailClosedPersistenceStore::new(raw));
    let mut node = RaftNode::new("persist_fail_node".to_string(), vec![], transport)
        .with_persistence(strict);
    node.set_election_timeout_ms(20);
    let (cmd_tx, shared, _handle) = node.spawn();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if shared.lock().await.role == RaftRole::Leader {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "single node did not become leader before persistence test"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let (reply_tx, reply_rx) = oneshot::channel();
    cmd_tx
        .send(ClientCommand {
            payload: b"must-not-ack".to_vec(),
            reply: reply_tx,
        })
        .await
        .expect("command reaches live leader before injected save failure");

    let reply = tokio::time::timeout(Duration::from_secs(1), reply_rx)
        .await
        .expect("client reply channel must close promptly after fail-stop");
    assert!(
        reply.is_err(),
        "persistence failure must drop the reply channel, never produce SQL/Raft success"
    );

    // The event loop panicked/fail-stopped at the required stable-store write;
    // it must not continue accepting later commands.
    let (second_reply, _second_rx) = oneshot::channel();
    let second_send = cmd_tx
        .send(ClientCommand {
            payload: b"after-failure".to_vec(),
            reply: second_reply,
        })
        .await;
    assert!(
        second_send.is_err(),
        "Raft command channel must close after a required persistence failure"
    );
}
