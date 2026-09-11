// SPDX-License-Identifier: Apache-2.0
//! Phase-6 recovery/bootstrap serving-readiness evidence.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{
    ChannelTransport, ClusterMembership, MemPersistenceStore, PersistentState, RaftNode,
    RaftPersistenceStore,
};
use neuralbase::hlc::HlcClock;
use neuralbase::read_barrier::{prepare_read_with_timeout, ReadBarrierError};
use neuralbase::read_consistency::ReadConsistency;
use neuralbase::replicated_gateway::{ReplicatedGatewayError, ReplicatedSqlGateway};
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;

#[tokio::test]
async fn recovering_node_allows_local_but_rejects_strong_reads_before_serving_ready() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let clock = Arc::new(HlcClock::new(500));
    let bus = ChannelTransport::new_bus();
    let id = "p6-recovering";
    let transport = Arc::new(ChannelTransport::register(id.to_string(), bus).await);

    // A fresh empty node is intentionally serving-ready before its first election.
    // To exercise the real restart/bootstrap safety boundary, seed durable Raft
    // state and construct the node through the persistence recovery path. Existing
    // restart regressions prove this path starts fail-closed until a current-term
    // recovery barrier reaches the durable apply frontier.
    let store = Arc::new(MemPersistenceStore::new());
    let mut persisted = PersistentState::new();
    persisted.current_term = 5;
    persisted.membership = Some(ClusterMembership::bootstrap(
        id.to_string(),
        Vec::<String>::new(),
    ));
    store
        .save(&persisted, &[])
        .expect("seed persisted recovery state");
    let persistence: Arc<dyn RaftPersistenceStore> = store;

    let mut node = RaftNode::new(id.to_string(), Vec::new(), transport)
        .with_persistence(persistence);
    node.set_election_timeout_ms(5_000);
    let readiness = node.serving_readiness();
    assert!(
        !readiness.load(Ordering::Acquire),
        "persisted node must start fail-closed before recovery"
    );

    let (client_tx, shared, handle) = node.spawn();
    assert!(
        !readiness.load(Ordering::Acquire),
        "persisted node became ready before recovery authority was established"
    );

    let gateway = ReplicatedSqlGateway::new_with_readiness(
        client_tx,
        shared,
        engine,
        clock,
        Arc::clone(&readiness),
    );

    // Local retains its explicit backward-compatible local semantics even while
    // the consensus authority gate is closed.
    prepare_read_with_timeout(
        Some(&gateway),
        ReadConsistency::Local,
        Duration::from_millis(100),
    )
    .await
    .unwrap();

    // Strong modes must fail closed at the recovery readiness gate; they must not
    // silently fall back to stale local state.
    for mode in [ReadConsistency::Leader, ReadConsistency::Linearizable] {
        let error = prepare_read_with_timeout(Some(&gateway), mode, Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                ReadBarrierError::Gateway(ReplicatedGatewayError::CatchingUp)
            ),
            "recovering node unexpectedly served {mode}: {error}"
        );
    }

    handle.shutdown().await;
}
