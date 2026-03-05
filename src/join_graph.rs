//! Join graph extraction, statistics structures, and TPC-H query graph definitions.
//!
//! CONFIDENCE: raw=0.76 effective=0.73
//! DEPENDS_ON: catalog
//! RISK: table stats are approximate; selectivity estimates assume independence.


use std::collections::{HashMap, HashSet};

// ── Column / Table statistics ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Estimated number of distinct values.
    pub ndv: u64,
    /// Fraction of rows where this column is NULL (0.0 – 1.0).
    pub null_fraction: f64,
    /// Minimum observed value (integers only; None for non-integer columns).
    pub min_i64: Option<i64>,
    /// Maximum observed value.
    pub max_i64: Option<i64>,
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self {
            ndv: 1,
            null_fraction: 0.0,
            min_i64: None,
            max_i64: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableStats {
    pub table_name: String,
    pub row_count: u64,
    pub columns: HashMap<String, ColumnStats>,
}

impl TableStats {
    pub fn new(table_name: &str, row_count: u64) -> Self {
        Self {
            table_name: table_name.to_string(),
            row_count,
            columns: HashMap::new(),
        }
    }
}

// ── Join graph ────────────────────────────────────────────────────────────────

/// A directed equality join predicate: `left_table.left_column = right_table.right_column`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinEdge {
    pub left_table: String,
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
}

impl JoinEdge {
    pub fn new(left_table: &str, left_col: &str, right_table: &str, right_col: &str) -> Self {
        Self {
            left_table: left_table.to_string(),
            left_column: left_col.to_string(),
            right_table: right_table.to_string(),
            right_column: right_col.to_string(),
        }
    }
}

/// Undirected join hypergraph: tables are vertices, JoinEdges are edges.
#[derive(Debug, Clone)]
pub struct JoinGraph {
    /// Table names in original FROM-clause order (defines the "naive" ordering).
    pub tables: Vec<String>,
    pub edges: Vec<JoinEdge>,
    pub stats: HashMap<String, TableStats>,
}

impl JoinGraph {
    pub fn new(
        tables: Vec<String>,
        edges: Vec<JoinEdge>,
        stats: HashMap<String, TableStats>,
    ) -> Self {
        Self {
            tables,
            edges,
            stats,
        }
    }

    /// Returns tables in their original (naive left-deep) order.
    pub fn naive_order(&self) -> Vec<String> {
        self.tables.clone()
    }

    /// Row count for a table; falls back to 1_000_000 if unknown.
    pub fn row_count(&self, table: &str) -> u64 {
        self.stats
            .get(table)
            .map(|s| s.row_count)
            .unwrap_or(1_000_000)
    }

    /// Returns `true` if the undirected join graph contains a cycle.
    ///
    /// A cycle means the optimizer could loop when trying to peel tables one
    /// by one into a linear join order.
    pub fn has_cycle(&self) -> bool {
        // Build undirected adjacency list.
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for edge in &self.edges {
            adj.entry(edge.left_table.as_str())
                .or_default()
                .push(edge.right_table.as_str());
            adj.entry(edge.right_table.as_str())
                .or_default()
                .push(edge.left_table.as_str());
        }
        let mut visited: HashSet<&str> = HashSet::new();
        for start in self.tables.iter().map(String::as_str) {
            if !visited.contains(start) && dfs_has_cycle(start, "", &adj, &mut visited) {
                return true;
            }
        }
        false
    }

    /// Returns `Err` when the graph has a cycle (circular join predicates).
    pub fn validate(&self) -> Result<(), String> {
        if self.has_cycle() {
            Err("circular join predicate detected in query graph".to_string())
        } else {
            Ok(())
        }
    }

    /// Selectivity estimate: equity join between two tables via a single key.
    /// Assumes uniformity + independence: 1 / max(NDV_left, NDV_right).
    pub fn join_selectivity(&self, left_table: &str, right_table: &str, edge: &JoinEdge) -> f64 {
        let ndv_left = self
            .stats
            .get(left_table)
            .and_then(|ts| ts.columns.get(&edge.left_column))
            .map(|cs| cs.ndv.max(1))
            .unwrap_or_else(|| self.row_count(left_table).max(1));
        let ndv_right = self
            .stats
            .get(right_table)
            .and_then(|ts| ts.columns.get(&edge.right_column))
            .map(|cs| cs.ndv.max(1))
            .unwrap_or_else(|| self.row_count(right_table).max(1));
        1.0 / (ndv_left.max(ndv_right) as f64)
    }

    // ── TPC-H built-in statistics (SF 0.1) ──────────────────────────────────

    /// Returns approximate row counts for TPC-H base tables at scale factor 0.1.
    pub fn tpch_stats() -> HashMap<String, TableStats> {
        [
            ("lineitem", 600_122u64),
            ("orders", 150_000),
            ("customer", 15_000),
            ("supplier", 1_000),
            ("part", 20_000),
            ("partsupp", 80_000),
            ("nation", 25),
            ("region", 5),
        ]
        .iter()
        .map(|(name, rows)| (name.to_string(), TableStats::new(name, *rows)))
        .collect()
    }
}

fn dfs_has_cycle<'a>(
    node: &'a str,
    parent: &'a str,
    adj: &HashMap<&'a str, Vec<&'a str>>,
    visited: &mut HashSet<&'a str>,
) -> bool {
    visited.insert(node);
    if let Some(neighbors) = adj.get(node) {
        for &nb in neighbors {
            if !visited.contains(nb) {
                if dfs_has_cycle(nb, node, adj, visited) {
                    return true;
                }
            } else if nb != parent {
                return true;
            }
        }
    }
    false
}

// ── TPC-H query graph catalogue ───────────────────────────────────────────────

/// Returns a `JoinGraph` for the given TPC-H query number (1–22).
/// Tables are listed in canonical FROM-clause order (naive ordering).
/// Returns `None` for unsupported query numbers.
#[allow(clippy::too_many_lines)]
pub fn tpch_join_graph(query_num: u32) -> Option<JoinGraph> {
    let stats = JoinGraph::tpch_stats();
    let g = match query_num {
        // ── Single-table queries ─────────────────────────────────────────────
        1 => JoinGraph::new(vec!["lineitem".into()], vec![], stats),
        6 => JoinGraph::new(vec!["lineitem".into()], vec![], stats),
        22 => JoinGraph::new(vec!["customer".into()], vec![], stats),

        // ── Two-table queries ────────────────────────────────────────────────
        4 => JoinGraph::new(
            vec!["orders".into(), "lineitem".into()],
            vec![JoinEdge::new(
                "orders",
                "o_orderkey",
                "lineitem",
                "l_orderkey",
            )],
            stats,
        ),
        12 => JoinGraph::new(
            vec!["orders".into(), "lineitem".into()],
            vec![JoinEdge::new(
                "orders",
                "o_orderkey",
                "lineitem",
                "l_orderkey",
            )],
            stats,
        ),
        13 => JoinGraph::new(
            vec!["customer".into(), "orders".into()],
            vec![JoinEdge::new(
                "customer",
                "c_custkey",
                "orders",
                "o_custkey",
            )],
            stats,
        ),
        14 => JoinGraph::new(
            vec!["lineitem".into(), "part".into()],
            vec![JoinEdge::new("lineitem", "l_partkey", "part", "p_partkey")],
            stats,
        ),
        15 => JoinGraph::new(
            vec!["lineitem".into(), "supplier".into()],
            vec![JoinEdge::new(
                "lineitem",
                "l_suppkey",
                "supplier",
                "s_suppkey",
            )],
            stats,
        ),
        16 => JoinGraph::new(
            vec!["partsupp".into(), "part".into()],
            vec![JoinEdge::new("partsupp", "ps_partkey", "part", "p_partkey")],
            stats,
        ),
        17 => JoinGraph::new(
            vec!["lineitem".into(), "part".into()],
            vec![JoinEdge::new("lineitem", "l_partkey", "part", "p_partkey")],
            stats,
        ),
        19 => JoinGraph::new(
            vec!["lineitem".into(), "part".into()],
            vec![JoinEdge::new("lineitem", "l_partkey", "part", "p_partkey")],
            stats,
        ),

        // ── Three-table queries ──────────────────────────────────────────────
        3 => JoinGraph::new(
            vec!["customer".into(), "orders".into(), "lineitem".into()],
            vec![
                JoinEdge::new("customer", "c_custkey", "orders", "o_custkey"),
                JoinEdge::new("orders", "o_orderkey", "lineitem", "l_orderkey"),
            ],
            stats,
        ),
        11 => JoinGraph::new(
            vec!["partsupp".into(), "supplier".into(), "nation".into()],
            vec![
                JoinEdge::new("partsupp", "ps_suppkey", "supplier", "s_suppkey"),
                JoinEdge::new("supplier", "s_nationkey", "nation", "n_nationkey"),
            ],
            stats,
        ),
        18 => JoinGraph::new(
            vec!["customer".into(), "orders".into(), "lineitem".into()],
            vec![
                JoinEdge::new("customer", "c_custkey", "orders", "o_custkey"),
                JoinEdge::new("orders", "o_orderkey", "lineitem", "l_orderkey"),
            ],
            stats,
        ),

        // ── Four-table queries ───────────────────────────────────────────────
        10 => JoinGraph::new(
            vec![
                "customer".into(),
                "orders".into(),
                "lineitem".into(),
                "nation".into(),
            ],
            vec![
                JoinEdge::new("customer", "c_custkey", "orders", "o_custkey"),
                JoinEdge::new("orders", "o_orderkey", "lineitem", "l_orderkey"),
                JoinEdge::new("customer", "c_nationkey", "nation", "n_nationkey"),
            ],
            stats,
        ),
        20 => JoinGraph::new(
            vec![
                "supplier".into(),
                "nation".into(),
                "partsupp".into(),
                "part".into(),
            ],
            vec![
                JoinEdge::new("supplier", "s_suppkey", "partsupp", "ps_suppkey"),
                JoinEdge::new("supplier", "s_nationkey", "nation", "n_nationkey"),
                JoinEdge::new("partsupp", "ps_partkey", "part", "p_partkey"),
            ],
            stats,
        ),
        21 => JoinGraph::new(
            vec![
                "supplier".into(),
                "lineitem".into(),
                "orders".into(),
                "nation".into(),
            ],
            vec![
                JoinEdge::new("supplier", "s_suppkey", "lineitem", "l_suppkey"),
                JoinEdge::new("lineitem", "l_orderkey", "orders", "o_orderkey"),
                JoinEdge::new("supplier", "s_nationkey", "nation", "n_nationkey"),
            ],
            stats,
        ),

        // ── Five-table queries ───────────────────────────────────────────────
        2 => JoinGraph::new(
            vec![
                "part".into(),
                "supplier".into(),
                "partsupp".into(),
                "nation".into(),
                "region".into(),
            ],
            vec![
                JoinEdge::new("part", "p_partkey", "partsupp", "ps_partkey"),
                JoinEdge::new("supplier", "s_suppkey", "partsupp", "ps_suppkey"),
                JoinEdge::new("supplier", "s_nationkey", "nation", "n_nationkey"),
                JoinEdge::new("nation", "n_regionkey", "region", "r_regionkey"),
            ],
            stats,
        ),

        // ── Six-table queries ────────────────────────────────────────────────
        5 => JoinGraph::new(
            vec![
                "customer".into(),
                "orders".into(),
                "lineitem".into(),
                "supplier".into(),
                "nation".into(),
                "region".into(),
            ],
            vec![
                JoinEdge::new("customer", "c_custkey", "orders", "o_custkey"),
                JoinEdge::new("orders", "o_orderkey", "lineitem", "l_orderkey"),
                JoinEdge::new("lineitem", "l_suppkey", "supplier", "s_suppkey"),
                JoinEdge::new("customer", "c_nationkey", "nation", "n_nationkey"),
                JoinEdge::new("nation", "n_regionkey", "region", "r_regionkey"),
            ],
            stats,
        ),
        7 => JoinGraph::new(
            vec![
                "supplier".into(),
                "lineitem".into(),
                "orders".into(),
                "customer".into(),
                "nation".into(),
                "region".into(),
            ],
            vec![
                JoinEdge::new("supplier", "s_suppkey", "lineitem", "l_suppkey"),
                JoinEdge::new("lineitem", "l_orderkey", "orders", "o_orderkey"),
                JoinEdge::new("orders", "o_custkey", "customer", "c_custkey"),
                JoinEdge::new("supplier", "s_nationkey", "nation", "n_nationkey"),
                JoinEdge::new("customer", "c_nationkey", "nation", "n_nationkey"),
            ],
            stats,
        ),
        9 => JoinGraph::new(
            vec![
                "part".into(),
                "supplier".into(),
                "lineitem".into(),
                "partsupp".into(),
                "orders".into(),
                "nation".into(),
            ],
            vec![
                JoinEdge::new("part", "p_partkey", "lineitem", "l_partkey"),
                JoinEdge::new("supplier", "s_suppkey", "lineitem", "l_suppkey"),
                JoinEdge::new("lineitem", "l_suppkey", "partsupp", "ps_suppkey"),
                JoinEdge::new("lineitem", "l_partkey", "partsupp", "ps_partkey"),
                JoinEdge::new("lineitem", "l_orderkey", "orders", "o_orderkey"),
                JoinEdge::new("supplier", "s_nationkey", "nation", "n_nationkey"),
            ],
            stats,
        ),

        // ── Eight-table query ────────────────────────────────────────────────
        8 => JoinGraph::new(
            vec![
                "part".into(),
                "supplier".into(),
                "lineitem".into(),
                "orders".into(),
                "customer".into(),
                "nation".into(),
                "region".into(),
                "partsupp".into(),
            ],
            vec![
                JoinEdge::new("part", "p_partkey", "lineitem", "l_partkey"),
                JoinEdge::new("supplier", "s_suppkey", "lineitem", "l_suppkey"),
                JoinEdge::new("lineitem", "l_orderkey", "orders", "o_orderkey"),
                JoinEdge::new("orders", "o_custkey", "customer", "c_custkey"),
                JoinEdge::new("customer", "c_nationkey", "nation", "n_nationkey"),
                JoinEdge::new("nation", "n_regionkey", "region", "r_regionkey"),
                JoinEdge::new("supplier", "s_nationkey", "nation", "n_nationkey"),
            ],
            stats,
        ),

        _ => return None,
    };
    Some(g)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpch_stats_cover_all_standard_tables() {
        let stats = JoinGraph::tpch_stats();
        for name in &[
            "lineitem", "orders", "customer", "supplier", "part", "partsupp", "nation", "region",
        ] {
            assert!(stats.contains_key(*name), "missing table: {name}");
        }
    }

    #[test]
    fn triangle_is_detected_as_cycle() {
        let g = JoinGraph::new(
            vec!["a".into(), "b".into(), "c".into()],
            vec![
                JoinEdge::new("a", "id", "b", "id"),
                JoinEdge::new("b", "id", "c", "id"),
                JoinEdge::new("c", "id", "a", "id"),
            ],
            HashMap::new(),
        );
        assert!(g.has_cycle());
        assert!(g.validate().is_err());
    }

    #[test]
    fn star_schema_has_no_cycle() {
        let fact = "orders";
        let dims = ["customer", "supplier", "part"];
        let edges: Vec<JoinEdge> = dims
            .iter()
            .map(|d| JoinEdge::new(fact, "key", d, "key"))
            .collect();
        let tables: Vec<String> = std::iter::once(fact.to_string())
            .chain(dims.iter().map(|s| s.to_string()))
            .collect();
        let g = JoinGraph::new(tables, edges, HashMap::new());
        assert!(!g.has_cycle());
        assert!(g.validate().is_ok());
    }
}
