// SPDX-License-Identifier: Apache-2.0
//! Phase-6 recovery/bootstrap serving-readiness evidence.

use std::sync::Arc;
use std::time::Duration;

use neuralbase::consensus::{ChannelTransport, RaftNode};
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
    let transport = Arc::new(ChannelTransport::register("p6-recovering".to_string(), bus).await);

    // Keep the fresh node pre-election so it represents the same readiness
    // boundary used during restart/snapshot/bootstrap catch-up. A strong read
    // must fail before touching consensus; Local retains its explicit local
    // semantics.
    let mut node = RaftNode::new("p6-recovering".to_string(), Vec::new(), transport);
    node.set_election_timeout_ms(5_000);
    let readiness = node.serving_readiness();
    let (client_tx, shared, handle) = node.spawn();
    assert!(!readiness.load(std::sync::atomic::Ordering::Acquire));

    let gateway = ReplicatedSqlGateway::new_with_readiness(
        client_tx,
        shared,
        engine,
        clock,
        Arc::clone(&readiness),
    );

    prepare_read_with_timeout(
        Some(&gateway),
        ReadConsistency::Local,
        Duration::from_millis(100),
    )
    .await
    .unwrap();

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
