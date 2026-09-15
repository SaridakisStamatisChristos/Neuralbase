// SPDX-License-Identifier: Apache-2.0
//! Phase-10 multi-scale analytical characterization for the in-memory
//! vectorized TPC-H path. This is deliberately distinct from RocksDB-backed
//! storage measurements.

use std::hint::black_box;
use std::time::Instant;

use neuralbase::binder::bind_statement;
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::execution::{build_physical_plan, execute_physical_plan};
use neuralbase::scheduler::MorselScheduler;
use neuralbase::sql::parse_statement;
use neuralbase::tpch::generate_tpch_data;
use serde_json::json;

const Q1_SQL: &str = "SELECT l_returnflag, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price FROM lineitem GROUP BY l_returnflag";
const Q6_SQL: &str = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24";
const WARMUP: usize = 1;
const REPS: usize = 3;

fn label() -> String {
    std::env::var("PHASE10_LABEL").unwrap_or_else(|_| "unlabeled".into())
}

fn measured_commit() -> String {
    std::env::var("PHASE10_MEASURED_COMMIT").unwrap_or_else(|_| "unknown".into())
}

fn percentile(sorted: &[u128], percentile: f64) -> u128 {
    let idx = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[idx]
}

fn report(name: &str, samples: &[u128], scale: f64, rows: usize) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mean = sorted.iter().copied().sum::<u128>() as f64 / sorted.len() as f64;
    println!(
        "PHASE10_RESULT {}",
        json!({
            "schema": 1,
            "label": label(),
            "commit": measured_commit(),
            "name": name,
            "path": "vectorized_tpch_in_memory",
            "unit": "ns",
            "warmup_iterations": WARMUP,
            "measured_iterations": samples.len(),
            "min": sorted[0],
            "p50": percentile(&sorted, 0.50),
            "p95": percentile(&sorted, 0.95),
            "max": sorted[sorted.len() - 1],
            "mean": mean,
            "extra": {"scale_factor": scale, "lineitem_rows": rows, "persistent_storage": false}
        })
    );
}

fn measure(sql: &str, name: &str, scale: f64) {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let statement = parse_statement(sql).expect("TPC-H parse");
    let bound = bind_statement(&statement, &catalog).expect("TPC-H bind");
    let plan = build_physical_plan(&bound);
    let dataset = generate_tpch_data(scale);
    let rows = dataset.lineitem.row_count;
    let scheduler = MorselScheduler::new(16_384);

    for _ in 0..WARMUP {
        black_box(
            execute_physical_plan(&plan, &dataset, &scheduler, None)
                .expect("TPC-H warmup execute"),
        );
    }
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let output = execute_physical_plan(&plan, &dataset, &scheduler, None)
            .expect("TPC-H measured execute");
        samples.push(start.elapsed().as_nanos());
        black_box(output.row_count);
    }
    report(name, &samples, scale, rows);
}

#[test]
#[ignore = "manual Phase-10 performance characterization"]
fn phase10_tpch_q1_q6_multiple_scales() {
    for scale in [0.001_f64, 0.01_f64, 0.1_f64] {
        let suffix = if scale == 0.001 {
            "sf0001"
        } else if scale == 0.01 {
            "sf001"
        } else {
            "sf01"
        };
        measure(Q1_SQL, &format!("tpch_q1_{suffix}"), scale);
        measure(Q6_SQL, &format!("tpch_q6_{suffix}"), scale);
    }
}
