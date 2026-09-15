// SPDX-License-Identifier: Apache-2.0
//! Phase-10 component characterization for the live general-query route.
//!
//! `server_parts/query.rs` currently constructs an SF=0.1 TPC-H dataset and a
//! row-oriented `QueryCatalog` in the SelectQuery route. Measure those two costs
//! independently before changing the route so the first optimization is tied to
//! evidence rather than source inspection alone.

use std::hint::black_box;
use std::time::Instant;

use neuralbase::query_executor::QueryCatalog;
use neuralbase::tpch::generate_tpch_data;
use serde_json::json;

const WARMUP: usize = 1;
const REPS: usize = 5;

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

fn report(name: &str, path: &str, samples: &[u128], extra: serde_json::Value) {
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
            "path": path,
            "unit": "ns",
            "warmup_iterations": WARMUP,
            "measured_iterations": samples.len(),
            "min": sorted[0],
            "p50": percentile(&sorted, 0.50),
            "p95": percentile(&sorted, 0.95),
            "max": sorted[sorted.len() - 1],
            "mean": mean,
            "extra": extra
        })
    );
}

#[test]
#[ignore = "manual Phase-10 component characterization"]
fn phase10_server_tpch_dataset_construction() {
    for _ in 0..WARMUP {
        let dataset = generate_tpch_data(0.1);
        black_box(dataset.lineitem.row_count);
    }
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let dataset = generate_tpch_data(0.1);
        samples.push(start.elapsed().as_nanos());
        black_box(dataset.lineitem.row_count);
    }
    report(
        "server_tpch_sf01_dataset_construction",
        "generate_tpch_data(0.1)",
        &samples,
        json!({"scale_factor": 0.1, "live_route_calls_per_select": 1}),
    );
}

#[test]
#[ignore = "manual Phase-10 component characterization"]
fn phase10_server_tpch_query_catalog_materialization() {
    let dataset = generate_tpch_data(0.1);
    for _ in 0..WARMUP {
        let catalog = QueryCatalog::from_tpch(&dataset);
        black_box(catalog.tables.len());
    }
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let catalog = QueryCatalog::from_tpch(&dataset);
        samples.push(start.elapsed().as_nanos());
        black_box(catalog.tables.len());
    }
    report(
        "server_tpch_sf01_query_catalog_materialization",
        "QueryCatalog::from_tpch",
        &samples,
        json!({"scale_factor": 0.1, "dataset_generation_in_timing": false}),
    );
}
