//! Plan cost model: estimated join costs and algorithm selection.
//!
//! CONFIDENCE: raw=0.73 effective=0.67
//! DEPENDS_ON: join_graph
//! RISK: cost estimates assume uniform data distributions; skewed data may
//!       make actual query times diverge significantly from predictions.

use crate::join_graph::{JoinGraph, TableStats};
use std::collections::HashMap;

// ── Join algorithm classifier ─────────────────────────────────────────────────

/// Which join algorithm the cost model expects to be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinAlgo {
    /// Build a hash table on the smaller side; probe with the larger side.
    Hash,
    /// Both inputs are pre-sorted on the join key; merge in one pass.
    SortMerge,
}

/// Row threshold above which `SortMerge` may be preferred over `Hash`.
/// (In practice the executor always uses hash for now; this is purely for
/// cost-model disambiguation.)
const SORT_MERGE_THRESHOLD: u64 = 500_000;

// ── Cost model ────────────────────────────────────────────────────────────────

/// Simple operator cost model.
///
/// Unit = "row units" — an abstract monotonically increasing cost metric.
/// Actual wall-clock times will differ; the model is used only for relative
/// comparison between competing join orders.
#[derive(Debug, Clone, Default)]
pub struct CostModel {
    /// Weight applied to the build side of a hash join (memory allocation cost).
    pub hash_build_factor: f64,
    /// Weight applied to probing each row of the larger side.
    pub hash_probe_factor: f64,
    /// Per-row sort cost (O(n log n) approximation base).
    pub sort_factor: f64,
}

impl CostModel {
    pub fn new() -> Self {
        Self {
            hash_build_factor: 2.0,
            hash_probe_factor: 1.0,
            sort_factor: 3.0,
        }
    }

    /// Choose the join algorithm based on the size of the smaller input.
    pub fn choose_algorithm(left_rows: u64, right_rows: u64) -> JoinAlgo {
        let smaller = left_rows.min(right_rows);
        if smaller >= SORT_MERGE_THRESHOLD {
            JoinAlgo::SortMerge
        } else {
            JoinAlgo::Hash
        }
    }

    /// Estimated cost of a single binary join operator.
    ///
    /// For a hash join: cost ≈ build_factor × |smaller| + probe_factor × |larger|.
    /// For a sort-merge join: cost ≈ sort_factor × (|left| log₂|left| + |right| log₂|right|).
    pub fn join_cost(&self, left_rows: u64, right_rows: u64) -> f64 {
        let algo = Self::choose_algorithm(left_rows, right_rows);
        let (smaller, larger) = if left_rows <= right_rows {
            (left_rows as f64, right_rows as f64)
        } else {
            (right_rows as f64, left_rows as f64)
        };
        match algo {
            JoinAlgo::Hash => self.hash_build_factor * smaller + self.hash_probe_factor * larger,
            JoinAlgo::SortMerge => {
                let log_left = (left_rows as f64).log2().max(1.0);
                let log_right = (right_rows as f64).log2().max(1.0);
                self.sort_factor * (left_rows as f64 * log_left + right_rows as f64 * log_right)
            }
        }
    }

    /// Estimated output cardinality of an equality join.
    ///
    /// selectivity ∈ (0, 1] — fraction of cross-product rows that match.
    pub fn output_rows(left_rows: u64, right_rows: u64, selectivity: f64) -> u64 {
        let sel = selectivity.clamp(1e-9, 1.0);
        ((left_rows as f64) * (right_rows as f64) * sel)
            .round()
            .max(1.0) as u64
    }

    /// Total estimated cost of executing a left-deep join plan in the given
    /// `table_order`.
    ///
    /// The model accumulates the cost of each binary join step, using the
    /// current intermediate cardinality as the left-side row count.
    pub fn total_plan_cost(
        &self,
        table_order: &[String],
        graph: &JoinGraph,
        stats: &HashMap<String, TableStats>,
    ) -> f64 {
        if table_order.len() <= 1 {
            return 0.0;
        }

        let mut total_cost = 0.0;
        // Running cardinality of the growing left-deep intermediate result.
        let mut running_rows: u64 = stats
            .get(&table_order[0])
            .map(|s| s.row_count)
            .unwrap_or(1_000_000);

        for right_name in table_order.iter().skip(1) {
            let right_rows = stats
                .get(right_name.as_str())
                .map(|s| s.row_count)
                .unwrap_or(1_000_000);

            // Find a join edge between the running result and the next table.
            // Use a conservative default selectivity if no direct edge is found.
            let selectivity = find_edge_selectivity(
                &table_order[..table_order
                    .iter()
                    .position(|t| t == right_name)
                    .unwrap_or(0)],
                right_name,
                graph,
            );

            total_cost += self.join_cost(running_rows, right_rows);
            running_rows = Self::output_rows(running_rows, right_rows, selectivity);
        }

        total_cost
    }
}

/// Find the first join edge connecting the `left_tables` set to `right_table`
/// and return its selectivity estimate; defaults to 0.01 if no edge is found.
fn find_edge_selectivity(left_tables: &[String], right_table: &str, graph: &JoinGraph) -> f64 {
    for edge in &graph.edges {
        let left_in_set = left_tables.iter().any(|t| t == &edge.left_table);
        let right_matches = edge.right_table == right_table;
        if left_in_set && right_matches {
            return graph.join_selectivity(&edge.left_table, right_table, edge);
        }
        // Check the reverse direction as well (edges are bidirectional).
        let right_in_set = left_tables.iter().any(|t| t == &edge.right_table);
        let left_matches = edge.left_table == right_table;
        if right_in_set && left_matches {
            return graph.join_selectivity(&edge.right_table, right_table, edge);
        }
    }
    // No explicit edge found — assume a cross-join that is filtered downstream.
    0.01
}

// ── A/B comparison helper ─────────────────────────────────────────────────────

/// Compare two join orderings by estimated total cost.
///
/// Returns `true` if `rl_order` costs ≤ `naive_order` (RL wins or ties).
pub fn rl_beats_naive(
    cost_model: &CostModel,
    rl_order: &[String],
    naive_order: &[String],
    graph: &JoinGraph,
    stats: &HashMap<String, TableStats>,
) -> bool {
    let rl_cost = cost_model.total_plan_cost(rl_order, graph, stats);
    let naive_cost = cost_model.total_plan_cost(naive_order, graph, stats);
    rl_cost <= naive_cost
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join_graph::{JoinEdge, JoinGraph};

    fn simple_stats(pairs: &[(&str, u64)]) -> HashMap<String, TableStats> {
        pairs
            .iter()
            .map(|(name, rows)| (name.to_string(), TableStats::new(name, *rows)))
            .collect()
    }

    #[test]
    fn hash_join_prefers_smaller_build_side() {
        let cm = CostModel::new();
        // cost(1000 build, 10000 probe) < cost(10000 build, 1000 probe)?
        // Both should be equal because we always pick the smaller side.
        let c1 = cm.join_cost(1_000, 10_000);
        let c2 = cm.join_cost(10_000, 1_000);
        // Our model picks the smaller: both c1 and c2 use 1000 as build side.
        assert!(
            (c1 - c2).abs() < 1e-6,
            "cost should be symmetric: {c1} vs {c2}"
        );
    }

    #[test]
    fn total_plan_cost_smaller_first_is_cheaper_for_many_tables() {
        let stats = simple_stats(&[
            ("region", 5),
            ("nation", 25),
            ("supplier", 1_000),
            ("customer", 15_000),
            ("orders", 150_000),
            ("lineitem", 600_000),
        ]);

        let edges = vec![
            JoinEdge::new("nation", "n_regionkey", "region", "r_regionkey"),
            JoinEdge::new("supplier", "s_nationkey", "nation", "n_nationkey"),
            JoinEdge::new("customer", "c_nationkey", "nation", "n_nationkey"),
            JoinEdge::new("orders", "o_custkey", "customer", "c_custkey"),
            JoinEdge::new("lineitem", "l_orderkey", "orders", "o_orderkey"),
        ];
        let graph = JoinGraph::new(
            vec![
                "region".into(),
                "nation".into(),
                "supplier".into(),
                "customer".into(),
                "orders".into(),
                "lineitem".into(),
            ],
            edges,
            stats.clone(),
        );

        let cm = CostModel::new();
        // RL order: smallest first
        let rl_order = vec![
            "region".into(),
            "nation".into(),
            "supplier".into(),
            "customer".into(),
            "orders".into(),
            "lineitem".into(),
        ];
        // Naive: largest first
        let naive_order = vec![
            "lineitem".into(),
            "orders".into(),
            "customer".into(),
            "supplier".into(),
            "nation".into(),
            "region".into(),
        ];

        let rl_cost = cm.total_plan_cost(&rl_order, &graph, &stats);
        let naive_cost = cm.total_plan_cost(&naive_order, &graph, &stats);

        assert!(
            rl_cost <= naive_cost,
            "RL cost ({rl_cost}) should be ≤ naive ({naive_cost})"
        );
    }

    #[test]
    fn single_table_plan_costs_zero() {
        let stats = simple_stats(&[("lineitem", 600_000)]);
        let graph = JoinGraph::new(vec!["lineitem".into()], vec![], stats.clone());
        let cm = CostModel::new();
        assert_eq!(
            cm.total_plan_cost(&["lineitem".to_string()], &graph, &stats),
            0.0
        );
    }
}
