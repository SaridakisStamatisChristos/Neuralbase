// SPDX-License-Identifier: Apache-2.0
//! Consensus-level fail-closed persistence tests.
//!
//! These tests exercise `RaftNode` with raw persistence stores. They prove the
//! consensus core itself refuses to start after a load failure and cannot let a
//! live client observe success after a required stable-log save fails.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    ChannelTransport, ClientCommand, PersistentState, RaftNode, RaftPersistenceStore, RaftRole,
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

struct FailLoad;

impl RaftPersistenceStore for FailLoad {
    fn save(&self, _state: &PersistentState, _snapshot_data: &[u8]) -> Result<(), String> {
        Ok(())
    }

    fn load(&self) -> Result<Option<(PersistentState, Vec<u8>)>, String> {
        Err("injected load failure".to_string())
    }
}

#[tokio::test]
#[should_panic(expected = "fatal Raft persistence load failure: injected load failure")]
async fn persistence_load_failure_prevents_node_startup() {
    let bus = ChannelTransport::new_bus();
    let transport = Arc::new(
        ChannelTransport::register("persist_load_fail".to_string(), Arc::clone(&bus)).await,
    );
    let store: Arc<dyn RaftPersistenceStore> = Arc::new(FailLoad);
    let _ = RaftNode::new("persist_load_fail".to_string(), vec![], transport)
        .with_persistence(store);
}

#[tokio::test]
async fn command_persistence_failure_cannot_return_success() {
    let bus = ChannelTransport::new_bus();
    let transport = Arc::new(
        ChannelTransport::register("persist_fail_node".to_string(), Arc::clone(&bus)).await,
    );

    // The first save is the node's self-vote/current-term election record. The
    // second save is the first client log append and is injected to fail. This
    // store is attached directly: no fail-closed adapter is involved.
    let store: Arc<dyn RaftPersistenceStore> = Arc::new(FailAfterFirstSave::new());
    let mut node = RaftNode::new("persist_fail_node".to_string(), vec![], transport)
        .with_persistence(store);
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
