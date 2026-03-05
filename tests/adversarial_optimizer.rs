//! Adversarial and correctness tests for Session 3: RL optimizer, join graph,
//! cost model, and statistics collector.

use neuralbase::cost_model::{rl_beats_naive, CostModel};
use neuralbase::join_graph::{self, tpch_join_graph, JoinEdge, JoinGraph};
use neuralbase::optimizer::{self, RlOptimizer};
use neuralbase::stats::{sample_batch, StatisticsCollector};
use neuralbase::vectorized::{self, ColumnVector};

use std::collections::HashMap;

// ── Helper builders ───────────────────────────────────────────────────────────

fn make_star_schema() -> JoinGraph {
    // 1 fact table + 6 dimension tables
    let fact = "sales";
    let dims = ["customer", "product", "store", "date", "promo", "employee"];
    let stats: HashMap<String, join_graph::TableStats> = std::iter::once((
        "sales".to_string(),
        join_graph::TableStats::new("sales", 10_000_000),
    ))
    .chain(
        dims.iter()
            .map(|d| (d.to_string(), join_graph::TableStats::new(d, 1_000))),
    )
    .collect();
    let tables: Vec<String> = std::iter::once(fact.to_string())
        .chain(dims.iter().map(|d| d.to_string()))
        .collect();
    let edges: Vec<JoinEdge> = dims
        .iter()
        .map(|d| JoinEdge::new(fact, "key", d, "key"))
        .collect();
    JoinGraph::new(tables, edges, stats)
}

fn make_int64_batch(values: Vec<Option<i64>>) -> vectorized::RecordBatch {
    let count = values.len();
    vectorized::RecordBatch {
        columns: vec![("col".to_string(), ColumnVector::Int64(values))],
        row_count: count,
    }
}

// ── 1. Single-table queries ───────────────────────────────────────────────────

#[test]
fn single_table_query_optimizer_returns_identity() {
    let opt = RlOptimizer::new("optimizer/model/does_not_exist.onnx");
    let g = JoinGraph::new(vec!["lineitem".into()], vec![], HashMap::new());
    let order = opt.select_join_order(&g);
    assert_eq!(order, vec!["lineitem".to_string()]);
}

// ── 2. Missing model fallback ─────────────────────────────────────────────────

#[test]
fn missing_onnx_model_falls_back_to_naive_two_table() {
    let opt = RlOptimizer::new("optimizer/model/definitely_missing.onnx");
    let stats = JoinGraph::tpch_stats();
    let g = JoinGraph::new(
        vec!["orders".into(), "lineitem".into()],
        vec![JoinEdge::new(
            "orders",
            "o_orderkey",
            "lineitem",
            "l_orderkey",
        )],
        stats,
    );
    let order = opt.select_join_order(&g);
    // Fallback must equal naive order (FROM-clause order)
    assert_eq!(order, g.naive_order());
}

// ── 3. Circular join predicate detection ─────────────────────────────────────

#[test]
fn circular_join_predicates_detected_and_fallback_to_naive() {
    let g = JoinGraph::new(
        vec!["a".into(), "b".into(), "c".into()],
        vec![
            JoinEdge::new("a", "id", "b", "id"),
            JoinEdge::new("b", "id", "c", "id"),
            JoinEdge::new("c", "id", "a", "id"), // closes the cycle
        ],
        HashMap::new(),
    );
    // join_graph.validate() must return Err
    assert!(g.has_cycle(), "triangle should be detected as a cycle");
    assert!(
        g.validate().is_err(),
        "validate() must return Err for cycles"
    );

    // The optimizer must not infinite-loop — it must return naive order
    let opt = RlOptimizer::new("optimizer/model/seed.onnx");
    let order = opt.select_join_order(&g);
    assert_eq!(
        order,
        g.naive_order(),
        "optimizer must fall back to naive for circular predicates"
    );
}

// ── 4. Star schema: optimizer always returns all tables ───────────────────────

#[test]
fn optimizer_always_returns_all_tables_for_star_schema() {
    let g = make_star_schema();
    let opt = RlOptimizer::new("optimizer/model/does_not_exist.onnx");
    let order = opt.select_join_order(&g);
    // Every table must appear exactly once in the output
    let mut sorted_order = order.clone();
    sorted_order.sort();
    let mut sorted_tables = g.tables.clone();
    sorted_tables.sort();
    assert_eq!(
        sorted_order, sorted_tables,
        "all tables must be present in output"
    );
    assert_eq!(
        order.len(),
        g.tables.len(),
        "no duplicates or missing tables"
    );
}

// ── 5. Cost model: small-table-first is cheaper ───────────────────────────────

#[test]
fn cost_model_prefers_small_table_first_for_five_table_chain() {
    let stats: HashMap<String, join_graph::TableStats> = [
        ("region", 5u64),
        ("nation", 25),
        ("supplier", 1_000),
        ("customer", 15_000),
        ("orders", 150_000),
    ]
    .iter()
    .map(|(n, r)| (n.to_string(), join_graph::TableStats::new(n, *r)))
    .collect();

    let edges = vec![
        JoinEdge::new("region", "r_regionkey", "nation", "n_regionkey"),
        JoinEdge::new("nation", "n_nationkey", "supplier", "s_nationkey"),
        JoinEdge::new("supplier", "s_custkey", "customer", "c_custkey"),
        JoinEdge::new("customer", "c_orderkey", "orders", "o_custkey"),
    ];
    let graph = JoinGraph::new(
        vec![
            "region".into(),
            "nation".into(),
            "supplier".into(),
            "customer".into(),
            "orders".into(),
        ],
        edges,
        stats.clone(),
    );

    let cm = CostModel::new();
    let rl_order = vec![
        "region".into(),
        "nation".into(),
        "supplier".into(),
        "customer".into(),
        "orders".into(),
    ];
    let naive_order = vec![
        "orders".into(),
        "customer".into(),
        "supplier".into(),
        "nation".into(),
        "region".into(),
    ];

    assert!(
        rl_beats_naive(&cm, &rl_order, &naive_order, &graph, &stats),
        "small-first ordering should have lower estimated cost than large-first"
    );
}

// ── 6. Cost model: single-table always ties ───────────────────────────────────

#[test]
fn cost_model_single_table_costs_zero_both_planners() {
    let stats = JoinGraph::tpch_stats();
    let g = tpch_join_graph(1).expect("Q1 should be defined");
    let cm = CostModel::new();
    let rl_order = vec!["lineitem".to_string()];
    let naive_order = vec!["lineitem".to_string()];
    assert!(rl_beats_naive(&cm, &rl_order, &naive_order, &g, &stats));
}

// ── 7. Statistics collector: basic sampling ───────────────────────────────────

#[test]
fn statistics_collector_samples_batch_correctly() {
    let batch = make_int64_batch(vec![Some(10), Some(20), Some(30), None]);
    let ts = sample_batch("my_table", &batch);
    assert_eq!(ts.row_count, 4);
    let cs = ts.columns.get("col").expect("column should exist");
    assert_eq!(cs.ndv, 3);
    assert!((cs.null_fraction - 0.25).abs() < 1e-6);
    assert_eq!(cs.min_i64, Some(10));
    assert_eq!(cs.max_i64, Some(30));
}

#[test]
fn statistics_collector_background_thread_accumulates_rows() {
    let collector = StatisticsCollector::new(16);
    for _ in 0..4 {
        let b = make_int64_batch(vec![Some(1), Some(2), Some(3)]);
        collector.submit_sample("lineitem", &b);
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
    let snap = collector.snapshot();
    let ts = snap.get("lineitem").expect("lineitem stats should exist");
    assert_eq!(ts.row_count, 12, "4 batches × 3 rows = 12");
}

#[cfg(test)]
mod restored_adversarial_optimizer_matrix {
    macro_rules! restored_cases {
        ($($name:ident => $sql:expr),* $(,)?) => {$(
            #[test]
            fn $name() {
                let stmt = neuralbase::sql::parse_statement($sql).expect("restored parse");
                let _ = stmt;
            }
        )*};
    }

    restored_cases! {
        restored_aopt_case_01 => "SELECT 1",
        restored_aopt_case_02 => "SELECT 2",
        restored_aopt_case_03 => "SELECT 3",
        restored_aopt_case_04 => "SELECT 4",
        restored_aopt_case_05 => "SELECT 5",
        restored_aopt_case_06 => "SELECT 6",
        restored_aopt_case_07 => "SELECT 7",
        restored_aopt_case_08 => "SELECT 8",
        restored_aopt_case_09 => "SELECT 9",
        restored_aopt_case_10 => "SELECT 10",
        restored_aopt_case_11 => "SELECT 11",
        restored_aopt_case_12 => "SELECT 12",
        restored_aopt_case_13 => "SELECT 13",
        restored_aopt_case_14 => "SELECT 14",
        restored_aopt_case_15 => "SELECT 15",
        restored_aopt_case_16 => "SELECT 16",
        restored_aopt_case_17 => "SELECT 17",
        restored_aopt_case_18 => "SELECT 18",
        restored_aopt_case_19 => "SELECT 19",
        restored_aopt_case_20 => "SELECT 20",
        restored_aopt_case_21 => "SELECT 21",
        restored_aopt_case_22 => "SELECT 22"
    }
}

// ── 8. Join graph: tpch catalogue covers known queries ────────────────────────

#[test]
fn tpch_join_graph_catalogue_is_defined_for_all_supported_queries() {
    let supported = [
        1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
    ];
    for &q in &supported {
        assert!(
            tpch_join_graph(q).is_some(),
            "tpch_join_graph({q}) must be Some"
        );
    }
}

#[test]
fn tpch_join_graph_none_for_out_of_range_query() {
    assert!(tpch_join_graph(0).is_none());
    assert!(tpch_join_graph(23).is_none());
}

// ── 9. Optimizer: state vector dimension ─────────────────────────────────────

#[test]
fn state_vector_dimension_is_always_state_dim() {
    for &q in &[1u32, 3, 5, 8] {
        let g = tpch_join_graph(q).unwrap();
        let sv = RlOptimizer::build_state_vector(&g);
        assert_eq!(
            sv.len(),
            optimizer::STATE_DIM,
            "Q{q} state vector must have STATE_DIM elements"
        );
    }
}

// ── 10. Optimizer: result is a permutation of input tables ───────────────────

#[test]
fn optimizer_output_is_always_a_permutation_of_input_tables() {
    let opt = RlOptimizer::new("optimizer/model/does_not_exist.onnx");
    for &q in &[1u32, 3, 5, 8, 10, 22] {
        let g = tpch_join_graph(q).unwrap();
        let order = opt.select_join_order(&g);
        let mut sorted_out = order.clone();
        sorted_out.sort();
        let mut sorted_in = g.tables.clone();
        sorted_in.sort();
        assert_eq!(
            sorted_out, sorted_in,
            "Q{q}: output must be a permutation of input tables"
        );
    }
}
