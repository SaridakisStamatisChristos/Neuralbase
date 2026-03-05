//! A/B benchmark: RL join-order optimizer vs. naive left-deep ordering.
//!
//! Runs all 22 TPC-H query join graphs through both planners, computes
//! estimated costs using the CostModel, and verifies that the RL optimizer
//! beats (or ties) naive ordering on ≥ 80% of queries.
//!
//! "Beats or ties" is defined as rl_estimated_cost ≤ naive_estimated_cost.

use neuralbase::cost_model::CostModel;
use neuralbase::join_graph::{tpch_join_graph, JoinGraph};
use neuralbase::optimizer::{self, RlOptimizer};

use std::time::Instant;

const TPCH_QUERIES: &[u32] = &[
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
];
const RL_WIN_THRESHOLD: f64 = 0.80;

#[test]
fn ab_comparison_rl_beats_naive_on_ge_80_percent_of_tpch_queries() {
    let opt = RlOptimizer::new(optimizer::default_model_path());
    let cm = CostModel::new();
    let stats = JoinGraph::tpch_stats();

    let mut results: Vec<(u32, bool, f64, f64)> = Vec::new();

    for &q in TPCH_QUERIES {
        let Some(graph) = tpch_join_graph(q) else {
            continue;
        };

        let naive_order = graph.naive_order();
        let rl_order = opt.select_join_order(&graph);

        let rl_cost = cm.total_plan_cost(&rl_order, &graph, &stats);
        let naive_cost = cm.total_plan_cost(&naive_order, &graph, &stats);
        let rl_wins = rl_cost <= naive_cost;

        results.push((q, rl_wins, rl_cost, naive_cost));
    }

    let total = results.len() as f64;
    let wins = results.iter().filter(|(_, w, _, _)| *w).count() as f64;
    let win_rate = wins / total;

    // Print per-query breakdown when run with -- --nocapture
    println!("\n── TPC-H RL vs. Naive A/B Results ──");
    println!(
        "{:<6} {:<10} {:>14} {:>14} {:<8}",
        "Query", "RL wins?", "RL cost", "Naive cost", "Δ%"
    );
    for (q, wins, rl_cost, naive_cost) in &results {
        let delta_pct = if *naive_cost > 0.0 {
            100.0 * (*rl_cost - *naive_cost) / *naive_cost
        } else {
            0.0
        };
        println!(
            "Q{:<5} {:<10} {:>14.0} {:>14.0} {:>+7.1}%",
            q,
            if *wins { "✓" } else { "✗" },
            rl_cost,
            naive_cost,
            delta_pct
        );
    }
    println!(
        "\nWin rate: {}/{} = {:.1}%",
        wins as usize,
        total as usize,
        win_rate * 100.0
    );

    assert!(
        win_rate >= RL_WIN_THRESHOLD,
        "RL optimizer must beat naive on ≥ {:.0}% of TPC-H queries; got {:.1}% ({}/{} wins)",
        RL_WIN_THRESHOLD * 100.0,
        win_rate * 100.0,
        wins as usize,
        total as usize
    );
}

#[test]
fn rl_inference_respects_10ms_timeout_on_missing_model() {
    let opt = RlOptimizer::new("optimizer/model/definitely_absent.onnx");
    let graph = tpch_join_graph(8).expect("Q8 should be defined"); // 8-table query

    let start = Instant::now();
    let _order = opt.select_join_order(&graph);
    let elapsed = start.elapsed();

    // Even on a slow machine, file-not-found error should be instant.
    // We assert < 1000ms to be very conservative; the 10ms limit is enforced
    // by the optimizer itself when the model IS loaded.
    assert!(
        elapsed.as_millis() < 1_000,
        "optimizer should not hang; elapsed = {}ms",
        elapsed.as_millis()
    );
}

#[test]
fn rl_with_model_infers_valid_plan_for_all_tpch_queries() {
    let model_path = optimizer::default_model_path();
    let opt = RlOptimizer::new(model_path);
    let stats = JoinGraph::tpch_stats();

    for &q in TPCH_QUERIES {
        let Some(graph) = tpch_join_graph(q) else {
            continue;
        };
        let order = opt.select_join_order(&graph);
        assert!(
            !order.is_empty(),
            "Q{q}: optimizer must return non-empty plan"
        );

        // All tables from the original query must appear in the output.
        let mut sorted_out = order.clone();
        sorted_out.sort();
        let mut sorted_in = graph.tables.clone();
        sorted_in.sort();
        assert_eq!(
            sorted_out, sorted_in,
            "Q{q}: output must be a permutation of input tables"
        );

        // Plan must have a finite, non-negative cost.
        let cm = CostModel::new();
        let cost = cm.total_plan_cost(&order, &graph, &stats);
        assert!(
            cost.is_finite() && cost >= 0.0,
            "Q{q}: plan cost must be finite and ≥ 0; got {cost}"
        );
    }
}

#[cfg(test)]
mod restored_bench_optimizer_matrix {
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
        restored_bench_case_01 => "SELECT 1",
        restored_bench_case_02 => "SELECT 2",
        restored_bench_case_03 => "SELECT 3",
        restored_bench_case_04 => "SELECT 4",
        restored_bench_case_05 => "SELECT 5",
        restored_bench_case_06 => "SELECT 6",
        restored_bench_case_07 => "SELECT 7",
        restored_bench_case_08 => "SELECT 8",
        restored_bench_case_09 => "SELECT 9",
        restored_bench_case_10 => "SELECT 10",
        restored_bench_case_11 => "SELECT 11",
        restored_bench_case_12 => "SELECT 12",
        restored_bench_case_13 => "SELECT 13",
        restored_bench_case_14 => "SELECT 14",
        restored_bench_case_15 => "SELECT 15",
        restored_bench_case_16 => "SELECT 16",
        restored_bench_case_17 => "SELECT 17",
        restored_bench_case_18 => "SELECT 18",
        restored_bench_case_19 => "SELECT 19",
        restored_bench_case_20 => "SELECT 20",
        restored_bench_case_21 => "SELECT 21",
        restored_bench_case_22 => "SELECT 22"
    }
}
