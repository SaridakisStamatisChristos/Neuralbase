// SPDX-License-Identifier: Apache-2.0
//! Safety boundary between legacy opaque Raft snapshots and replicated SQL.
//!
//! Phase 2 has a real SQL-aware state-machine snapshot store. These tests protect
//! the remaining legacy boundary: confirmed SQL apply **without** that store must
//! still reject arbitrary opaque Raft snapshot bytes rather than advancing a Raft
//! boundary with no reconstructable SQL/catalog state.

use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    encode_compact_log, ChannelTransport, ClientCommand, CommittedEntry, MemPersistenceStore,
    PersistentState, RaftNode, RaftPersistenceStore, RaftRole,
};
use tokio::sync::{mpsc, oneshot};

#[tokio::test]
async fn replicated_sql_mode_rejects_legacy_compaction_command() {
    let bus = ChannelTransport::new_bus();
    let transport =
        Arc::new(ChannelTransport::register("snapshot_guard".to_string(), Arc::clone(&bus)).await);
    let (apply_tx, _apply_rx) = mpsc::channel::<CommittedEntry>(8);
    let mut node = RaftNode::new("snapshot_guard".to_string(), vec![], transport)
        .with_confirmed_apply_tx(apply_tx);
    node.set_election_timeout_ms(20);
    let (cmd_tx, shared, handle) = node.spawn();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while shared.lock().await.role != RaftRole::Leader {
        assert!(
            tokio::time::Instant::now() < deadline,
            "single node did not become leader before snapshot guard test"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let (reply_tx, reply_rx) = oneshot::channel();
    cmd_tx
        .send(ClientCommand {
            payload: encode_compact_log(1, b"opaque-legacy-snapshot"),
            reply: reply_tx,
        })
        .await
        .expect("compaction request reaches leader");
    let reply = reply_rx
        .await
        .expect("leader returns explicit compaction error");
    let error = reply.expect_err("replicated SQL mode must reject legacy compaction");
    assert!(
        error.contains("disabled in replicated SQL mode"),
        "unexpected compaction rejection: {error}"
    );

    handle.shutdown().await;
}

#[tokio::test]
#[should_panic(
    expected = "replicated SQL confirmed apply cannot start from a legacy Raft snapshot"
)]
async fn replicated_sql_mode_refuses_persisted_legacy_snapshot() {
    let store: Arc<dyn RaftPersistenceStore> = Arc::new(MemPersistenceStore::new());
    let mut state = PersistentState::new();
    state.current_term = 3;
    state.append(3, b"old-entry".to_vec());
    state.install_snapshot(1, 3);
    store
        .save(&state, b"opaque-legacy-snapshot")
        .expect("seed legacy snapshot");

    let bus = ChannelTransport::new_bus();
    let transport = Arc::new(
        ChannelTransport::register("snapshot_restart".to_string(), Arc::clone(&bus)).await,
    );
    let (apply_tx, _apply_rx) = mpsc::channel::<CommittedEntry>(8);

    let _ = RaftNode::new("snapshot_restart".to_string(), vec![], transport)
        .with_persistence(store)
        .with_confirmed_apply_tx(apply_tx);
}
