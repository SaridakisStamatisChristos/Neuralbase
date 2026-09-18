// SPDX-License-Identifier: Apache-2.0
//! Consensus barrier used by Phase-6 strong read modes.
//!
//! NeuralBase deliberately uses a replicated-log barrier for the first Phase-6
//! strong-read implementation instead of adding a separate ReadIndex RPC. The
//! existing Raft client-command acknowledgement has unusually strong semantics:
//! success is returned only after a current-term entry has reached the active
//! (including joint-consensus) voter quorum *and* the local replicated state
//! machine has confirmed durable apply. Therefore the acknowledged barrier
//! index is both a current-leader authority proof and an already-applied safe
//! read frontier.
//!
//! This is intentionally more expensive than Raft ReadIndex because each
//! non-overlapping `Leader` or `Linearizable` read appends one control entry.
//! Concurrent strong reads whose invocations overlap one in-flight barrier may
//! safely share that barrier when it completes after each read began. A future
//! optimization may replace this with ReadIndex without changing the public
//! `ReadConsistency` contract.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use metrics::counter;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::read_consistency::ReadConsistency;
use crate::replicated_gateway::{ReplicatedGatewayError, ReplicatedSqlGateway};

pub const DEFAULT_STRONG_READ_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum ReadBarrierError {
    #[error("{0} reads require clustered Raft mode")]
    ClusterRequired(ReadConsistency),
    #[error("strong-read authority barrier timed out")]
    Timeout,
    #[error(transparent)]
    Gateway(#[from] ReplicatedGatewayError),
}

#[derive(Default)]
struct StrongReadBarrierState {
    last_success: Option<(usize, Instant)>,
}

fn strong_read_barrier_state() -> &'static Mutex<StrongReadBarrierState> {
    static STATE: OnceLock<Mutex<StrongReadBarrierState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(StrongReadBarrierState::default()))
}

fn reusable_barrier(
    last_success: Option<(usize, Instant)>,
    gateway_id: usize,
    request_started: Instant,
) -> bool {
    matches!(
        last_success,
        Some((last_gateway, completed_at))
            if last_gateway == gateway_id && completed_at >= request_started
    )
}

fn record_timeout_rejection() {
    counter!("neuralbase_strong_read_rejections_total", "reason" => "timeout").increment(1);
}

/// Establish the consensus prerequisite for one SQL read.
///
/// `Local` never coordinates. `Leader` and `Linearizable` both use the same
/// current-term replicated barrier; the latter additionally *relies on* the
/// Raft client acknowledgement invariant that acknowledgement occurs only after
/// `last_applied` has advanced through the barrier index. No strong mode ever
/// silently falls back to a local read.
///
/// Overlapping strong reads are single-flighted. A waiter may reuse only a
/// barrier that completed after that waiter's invocation began, so a read that
/// starts after a prior barrier cannot reuse stale authority. This preserves the
/// existing linearization contract while preventing a burst of concurrent reads
/// from appending one redundant Raft control entry per client.
pub async fn prepare_read(
    gateway: Option<&ReplicatedSqlGateway>,
    mode: ReadConsistency,
) -> Result<(), ReadBarrierError> {
    prepare_read_with_timeout(gateway, mode, DEFAULT_STRONG_READ_TIMEOUT).await
}

pub async fn prepare_read_with_timeout(
    gateway: Option<&ReplicatedSqlGateway>,
    mode: ReadConsistency,
    timeout: Duration,
) -> Result<(), ReadBarrierError> {
    match mode {
        ReadConsistency::Local => {
            counter!("neuralbase_reads_total", "consistency" => "local").increment(1);
            Ok(())
        }
        ReadConsistency::Leader | ReadConsistency::Linearizable => {
            let gateway = gateway.ok_or(ReadBarrierError::ClusterRequired(mode))?;
            let label = mode.as_str();
            let request_started = Instant::now();
            let gateway_id = gateway as *const ReplicatedSqlGateway as usize;

            let mut state =
                match tokio::time::timeout(timeout, strong_read_barrier_state().lock()).await {
                    Ok(state) => state,
                    Err(_) => {
                        record_timeout_rejection();
                        return Err(ReadBarrierError::Timeout);
                    }
                };

            if reusable_barrier(state.last_success, gateway_id, request_started) {
                counter!("neuralbase_reads_total", "consistency" => label).increment(1);
                counter!("neuralbase_strong_read_barriers_total", "result" => "coalesced")
                    .increment(1);
                return Ok(());
            }

            let elapsed = request_started.elapsed();
            let Some(remaining) = timeout.checked_sub(elapsed) else {
                record_timeout_rejection();
                return Err(ReadBarrierError::Timeout);
            };

            let result = tokio::time::timeout(remaining, gateway.prepare_mutation()).await;
            match result {
                Ok(Ok(())) => {
                    state.last_success = Some((gateway_id, Instant::now()));
                    counter!("neuralbase_reads_total", "consistency" => label).increment(1);
                    counter!("neuralbase_strong_read_barriers_total", "result" => "submitted")
                        .increment(1);
                    // `prepare_mutation` submits a non-SQL Raft control entry.
                    // ClientCommand success is emitted only after confirmed
                    // state-machine apply, so a successful result proves the
                    // barrier's safe frontier is already locally applied.
                    Ok(())
                }
                Ok(Err(error)) => {
                    if matches!(error, ReplicatedGatewayError::NotLeader { .. }) {
                        counter!("neuralbase_strong_read_rejections_total", "reason" => "not_leader")
                            .increment(1);
                    }
                    Err(ReadBarrierError::Gateway(error))
                }
                Err(_) => {
                    record_timeout_rejection();
                    Err(ReadBarrierError::Timeout)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_mode_never_requires_a_cluster_gateway() {
        prepare_read(None, ReadConsistency::Local).await.unwrap();
    }

    #[tokio::test]
    async fn strong_modes_never_silently_downgrade_without_raft() {
        assert!(matches!(
            prepare_read(None, ReadConsistency::Leader).await,
            Err(ReadBarrierError::ClusterRequired(ReadConsistency::Leader))
        ));
        assert!(matches!(
            prepare_read(None, ReadConsistency::Linearizable).await,
            Err(ReadBarrierError::ClusterRequired(
                ReadConsistency::Linearizable
            ))
        ));
    }

    #[test]
    fn barrier_reuse_requires_same_gateway_and_completion_after_invocation() {
        let request_started = Instant::now();
        let before = request_started
            .checked_sub(Duration::from_millis(1))
            .expect("instant supports small subtraction");
        let after = request_started + Duration::from_millis(1);

        assert!(!reusable_barrier(None, 7, request_started));
        assert!(!reusable_barrier(Some((7, before)), 7, request_started));
        assert!(!reusable_barrier(Some((8, after)), 7, request_started));
        assert!(reusable_barrier(Some((7, after)), 7, request_started));
    }
}
