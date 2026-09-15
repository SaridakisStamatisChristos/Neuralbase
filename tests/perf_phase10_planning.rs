// SPDX-License-Identifier: Apache-2.0
//! Phase-10 parse/bind/physical-planning characterization.
//!
//! This deliberately measures planning stages independently from execution so
//! an executor optimization is not selected merely because total query time is
//! high. No timing threshold is asserted.

use std::hint::black_box;
use std::time::Instant;

use neuralbase::binder::bind_nb_statement;
use neuralbase::catalog::{ColumnDef, InMemoryCatalog, MutableCatalog, TableSchema};
use neuralbase::execution::build_physical_plan;
use neuralbase::sql::parse_nb_statement;
use serde_json::json;

const WARMUP: usize = 8;
const REPS: usize = 200;
const SQL: &str = "SELECT id FROM phase10_profile";

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

fn report(name: &str, path: &str, samples: &[u128]) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mean = sorted.iter().copied().sum::<u128>() as f64 / sorted.len() as f64;
    let payload = json!({
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
        "extra": {"sql": SQL}
    });
    println!("PHASE10_RESULT {payload}");
}

fn catalog() -> InMemoryCatalog {
    let catalog = InMemoryCatalog::default();
    catalog.create_table(TableSchema {
        name: "phase10_profile".into(),
        columns: vec![ColumnDef {
            name: "id".into(),
            data_type: "BIGINT".into(),
        }],
    });
    catalog
}

#[test]
#[ignore = "manual Phase-10 performance characterization"]
fn phase10_parse_bind_plan_characterization() {
    let catalog = catalog();

    for _ in 0..WARMUP {
        black_box(parse_nb_statement(SQL).expect("parse warmup"));
    }
    let mut parse_samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let statement = parse_nb_statement(SQL).expect("parse measured");
        parse_samples.push(start.elapsed().as_nanos());
        black_box(statement);
    }
    report("server_parse", "parse_nb_statement", &parse_samples);

    let statement = parse_nb_statement(SQL).expect("parse once for bind benchmark");
    for _ in 0..WARMUP {
        black_box(bind_nb_statement(&statement, &catalog).expect("bind warmup"));
    }
    let mut bind_samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let plan = bind_nb_statement(&statement, &catalog).expect("bind measured");
        bind_samples.push(start.elapsed().as_nanos());
        black_box(plan);
    }
    report(
        "binder_simple_table_select",
        "bind_nb_statement",
        &bind_samples,
    );

    let plan = bind_nb_statement(&statement, &catalog).expect("bind once for planning benchmark");
    for _ in 0..WARMUP {
        black_box(build_physical_plan(&plan));
    }
    let mut plan_samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let physical = build_physical_plan(&plan);
        plan_samples.push(start.elapsed().as_nanos());
        black_box(physical);
    }
    report(
        "physical_plan_simple_table_select",
        "build_physical_plan",
        &plan_samples,
    );
}
