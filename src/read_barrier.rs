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
//! `Leader` or `Linearizable` read appends one control entry. The cost is
//! accepted in Phase 6 in exchange for reusing the already-proven commit/apply
//! boundary. A future optimization may replace this with ReadIndex without
//! changing the public `ReadConsistency` contract.

use std::time::Duration;

use metrics::counter;
use thiserror::Error;

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

/// Establish the consensus prerequisite for one SQL read.
///
/// `Local` never coordinates. `Leader` and `Linearizable` both use the same
/// current-term replicated barrier; the latter additionally *relies on* the
/// Raft client acknowledgement invariant that acknowledgement occurs only after
/// `last_applied` has advanced through the barrier index. No strong mode ever
/// silently falls back to a local read.
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
            let start = std::time::Instant::now();
            let result = tokio::time::timeout(timeout, gateway.prepare_mutation()).await;
            match result {
                Ok(Ok(())) => {
                    counter!("neuralbase_reads_total", "consistency" => label).increment(1);
                    // `prepare_mutation` submits a non-SQL Raft control entry.
                    // ClientCommand success is emitted only after confirmed
                    // state-machine apply, so a successful result proves the
                    // barrier's safe frontier is already locally applied.
                    let _elapsed = start.elapsed();
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
                    counter!("neuralbase_strong_read_rejections_total", "reason" => "timeout")
                        .increment(1);
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
}
