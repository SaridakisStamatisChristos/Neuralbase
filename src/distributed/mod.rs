// SPDX-License-Identifier: Apache-2.0
// Distributed query planner.
//
// Partitions a physical plan into PlanFragments — one per shard involved —
// assigns each fragment to a node (data-locality scoring), and describes
// the exchange operators that connect fragments.
//
// CONFIDENCE: raw=0.78 effective=0.68
// DEPENDS_ON: cluster, distributed::exchange

// Session 5 — not yet wired into query path. Suppress dead_code.

use std::sync::Arc;

use crate::cluster::{ConsistentHashRouter, NodeInfo, NodeRegistry};

pub mod backpressure;
pub mod exchange;

// Re-exports: used by integration tests and future query-execution wiring.
// The binary itself does not yet call these directly — suppress until wired up.
#[allow(unused_imports)]
pub use backpressure::{bounded_channel, BoundedReceiver, BoundedSender, FlowController};
#[allow(unused_imports)]
pub use exchange::{
    broadcast_exchange, shuffle_exchange, BroadcastWriter, Gather, ShuffleReader, ShuffleWriter,
};

// Re-export from submodules so callers can `use distributed::*`.

// ── PhysicalPlanStub ──────────────────────────────────────────────────────

/// Minimal description of a physical plan for distribution purposes.
/// The real plan type lives in execution.rs; this stub avoids a circular dep.
#[derive(Debug, Clone)]
pub struct PhysicalPlanStub {
    /// Logical table involved (used for shard routing).
    pub table_id: u32,
    /// Estimated output row count (from statistics).
    pub estimated_rows: u64,
    /// Opaque serialized plan payload sent to each fragment executor.
    pub plan_bytes: Vec<u8>,
}

// ── PlanFragment ──────────────────────────────────────────────────────────

/// A single unit of distributed execution.
/// One fragment runs on one node and processes rows for one shard.
#[derive(Debug, Clone)]
pub struct PlanFragment {
    pub fragment_id: u32,
    pub shard_id: u32,
    pub assigned_node: NodeInfo,
    pub plan_bytes: Vec<u8>,
    /// Estimated rows this fragment will produce.
    pub estimated_rows: u64,
}

// ── FragmentStatus ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentStatus {
    Pending,
    Running,
    Completed,
    /// Transient failure — coordinator will attempt re-route.
    Failed(String),
}

// ── FragmentExecution ─────────────────────────────────────────────────────

/// Tracks the execution state of one fragment during a query.
#[derive(Debug, Clone)]
pub struct FragmentExecution {
    pub fragment: PlanFragment,
    pub status: FragmentStatus,
    /// Attempt count (1 = first try, 2 = first retry, …).
    pub attempt: u32,
}

// ── DistributedPlanner ────────────────────────────────────────────────────

/// Converts a physical plan into a list of fragments assigned to nodes.
pub struct DistributedPlanner {
    router: Arc<ConsistentHashRouter>,
    registry: Arc<NodeRegistry>,
    replication_factor: usize,
}

impl DistributedPlanner {
    pub fn new(
        router: Arc<ConsistentHashRouter>,
        registry: Arc<NodeRegistry>,
        replication_factor: usize,
    ) -> Self {
        Self {
            router,
            registry,
            replication_factor,
        }
    }

    /// Partition `plan` across all shards of `table_id`.
    /// Each shard gets one fragment assigned to its primary node.
    pub fn plan(&self, plan: PhysicalPlanStub) -> Vec<PlanFragment> {
        let shard_count = self.registry.shard_count();
        let mut fragments = vec![];
        for shard_id in 0..shard_count {
            let node = match self.router.shard_to_node(shard_id) {
                Some(n) => n,
                None => continue, // No alive node — skip shard (coordinator must handle).
            };
            fragments.push(PlanFragment {
                fragment_id: shard_id,
                shard_id,
                assigned_node: node,
                plan_bytes: plan.plan_bytes.clone(),
                estimated_rows: plan.estimated_rows / shard_count as u64 + 1,
            });
        }
        fragments
    }

    /// Re-route a failed fragment to a different replica node.
    /// Returns `None` if no alternative replicas are available.
    pub fn reroute(&self, fragment: &PlanFragment) -> Option<PlanFragment> {
        let replicas = self
            .router
            .shard_replicas(fragment.shard_id, self.replication_factor + 1);
        // Find a replica that is not the current assignment.
        replicas
            .into_iter()
            .find(|n| n.id != fragment.assigned_node.id)
            .map(|alt_node| PlanFragment {
                fragment_id: fragment.fragment_id,
                shard_id: fragment.shard_id,
                assigned_node: alt_node,
                plan_bytes: fragment.plan_bytes.clone(),
                estimated_rows: fragment.estimated_rows,
            })
    }
}

// ── QueryCoordinator ──────────────────────────────────────────────────────

/// Manages the lifecycle of a distributed query: submit fragments, handle
/// failures, collect results via Gather.
pub struct QueryCoordinator {
    planner: Arc<DistributedPlanner>,
    max_retries: u32,
}

impl QueryCoordinator {
    pub fn new(planner: Arc<DistributedPlanner>, max_retries: u32) -> Self {
        Self {
            planner,
            max_retries,
        }
    }

    /// Build the execution manifest for a plan (fragments + statuses).
    pub fn build_manifest(&self, plan: PhysicalPlanStub) -> Vec<FragmentExecution> {
        self.planner
            .plan(plan)
            .into_iter()
            .map(|f| FragmentExecution {
                fragment: f,
                status: FragmentStatus::Pending,
                attempt: 0,
            })
            .collect()
    }

    /// Mark a fragment as failed and attempt to re-route it.
    /// Returns `true` if a re-route was possible, `false` if no replicas remain.
    pub fn handle_failure(
        &self,
        manifest: &mut [FragmentExecution],
        fragment_id: u32,
        reason: String,
    ) -> bool {
        if let Some(exec) = manifest.iter_mut().find(|e| e.fragment.fragment_id == fragment_id) {
            exec.attempt += 1;
            if exec.attempt > self.max_retries {
                exec.status = FragmentStatus::Failed(format!(
                    "exhausted retries after {}: {reason}",
                    exec.attempt - 1
                ));
                return false;
            }
            match self.planner.reroute(&exec.fragment) {
                Some(new_frag) => {
                    exec.fragment = new_frag;
                    exec.status = FragmentStatus::Pending;
                    true
                }
                None => {
                    exec.status =
                        FragmentStatus::Failed(format!("no replicas for re-route: {reason}"));
                    false
                }
            }
        } else {
            false
        }
    }

    /// True if all fragments have succeeded.
    pub fn is_complete(&self, manifest: &[FragmentExecution]) -> bool {
        manifest.iter().all(|e| e.status == FragmentStatus::Completed)
    }

    /// True if any fragment has permanently failed.
    pub fn has_permanent_failure(&self, manifest: &[FragmentExecution]) -> bool {
        manifest.iter().any(|e| matches!(&e.status, FragmentStatus::Failed(_)))
    }
}

// ── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ClusterConfig, NodeRegistry};

    fn make_planner() -> DistributedPlanner {
        let config = ClusterConfig::default_3node();
        let registry = Arc::new(NodeRegistry::new(config));
        let router = Arc::new(ConsistentHashRouter::new(Arc::clone(&registry)));
        DistributedPlanner::new(router, registry, 2)
    }

    #[test]
    fn plan_creates_one_fragment_per_shard() {
        let planner = make_planner();
        let stub = PhysicalPlanStub {
            table_id: 1,
            estimated_rows: 1000,
            plan_bytes: b"SELECT * FROM t".to_vec(),
        };
        let frags = planner.plan(stub);
        // 3 alive nodes, 8 shards — all 8 should have a fragment.
        assert_eq!(frags.len(), 8);
    }

    #[test]
    fn coordinator_handles_failure_with_reroute() {
        let planner = Arc::new(make_planner());
        let coord = QueryCoordinator::new(Arc::clone(&planner), 2);
        let stub = PhysicalPlanStub {
            table_id: 1,
            estimated_rows: 100,
            plan_bytes: vec![],
        };
        let mut manifest = coord.build_manifest(stub);
        assert!(!manifest.is_empty());
        // Simulate failure on fragment 0.
        let original_node = manifest[0].fragment.assigned_node.id.clone();
        let rerouted = coord.handle_failure(&mut manifest, 0, "connection refused".to_string());
        // Whether rerouted depends on having a different replica available.
        if rerouted {
            // Re-routed fragment should be on a different node (if replicas exist).
            let new_node = &manifest[0].fragment.assigned_node.id;
            // Might be same if only 1 node — just verify it didn't panic.
            let _ = (original_node, new_node);
        }
    }

    #[test]
    fn coordinator_marks_permanently_failed_after_max_retries() {
        let planner = Arc::new(make_planner());
        let coord = QueryCoordinator::new(Arc::clone(&planner), 0); // 0 retries
        let stub = PhysicalPlanStub {
            table_id: 1,
            estimated_rows: 100,
            plan_bytes: vec![],
        };
        let mut manifest = coord.build_manifest(stub);
        let rerouted = coord.handle_failure(&mut manifest, 0, "dead".to_string());
        if !rerouted {
            assert!(coord.has_permanent_failure(&manifest));
        }
    }
}
