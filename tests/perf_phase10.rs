// SPDX-License-Identifier: Apache-2.0
//! Phase-10 reproducible query/storage characterization harness.
//!
//! These tests intentionally have no wall-clock pass/fail threshold. They print
//! machine-readable `PHASE10_RESULT` records for exact-commit before/after
//! comparison. Run in release mode with one test thread.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use neuralbase::catalog::{Catalog, ColumnDef, InMemoryCatalog, MutableCatalog, TableSchema};
use neuralbase::hlc::HlcClock;
use neuralbase::mvcc::TransactionManager;
use neuralbase::query_executor::{
    execute_select_query, query_result_to_batch, QueryCatalog, QueryResult, ScalarVal,
};
use neuralbase::sql::parse_statement;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::StorageExecutor;
use neuralbase::tpch::generate_tpch_data;
use neuralbase::vectorized::RecordBatch;
use serde_json::json;
use sqlparser::ast::Statement;
use tempfile::TempDir;

const WARMUP: usize = 2;
const REPS: usize = 9;

fn label() -> String {
    std::env::var("PHASE10_LABEL").unwrap_or_else(|_| "unlabeled".into())
}

fn measured_commit() -> String {
    std::env::var("PHASE10_MEASURED_COMMIT").unwrap_or_else(|_| "unknown".into())
}

fn percentile(sorted: &[u128], percentile: f64) -> u128 {
    assert!(!sorted.is_empty());
    let idx = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[idx]
}

fn report(name: &str, path: &str, unit: &str, samples: &[u128], extra: serde_json::Value) {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum: u128 = sorted.iter().copied().sum();
    let mean = sum as f64 / sorted.len() as f64;
    let payload = json!({
        "schema": 1,
        "label": label(),
        "commit": measured_commit(),
        "name": name,
        "path": path,
        "unit": unit,
        "warmup_iterations": WARMUP,
        "measured_iterations": samples.len(),
        "min": sorted[0],
        "p50": percentile(&sorted, 0.50),
        "p95": percentile(&sorted, 0.95),
        "max": sorted[sorted.len() - 1],
        "mean": mean,
        "extra": extra,
    });
    println!("PHASE10_RESULT {payload}");
}

fn parse_query(sql: &str) -> Box<sqlparser::ast::Query> {
    match parse_statement(sql).expect("phase10 query must parse") {
        Statement::Query(query) => query,
        other => panic!("expected query statement, got {other:?}"),
    }
}

fn clone_result(result: &QueryResult) -> QueryResult {
    QueryResult {
        columns: result.columns.clone(),
        rows: result.rows.clone(),
    }
}

fn measure_query(
    name: &str,
    sql: &str,
    catalog: &QueryCatalog,
    expected_non_empty: bool,
) -> QueryResult {
    let query = parse_query(sql);
    for _ in 0..WARMUP {
        let result = execute_select_query(&query, catalog).expect("phase10 warmup query");
        black_box(result.rows.len());
    }

    let mut samples = Vec::with_capacity(REPS);
    let mut last = None;
    for _ in 0..REPS {
        let start = Instant::now();
        let result = execute_select_query(&query, catalog).expect("phase10 measured query");
        let elapsed = start.elapsed().as_nanos();
        if expected_non_empty {
            assert!(
                !result.rows.is_empty(),
                "{name} unexpectedly returned no rows"
            );
        }
        black_box(result.rows.len());
        samples.push(elapsed);
        last = Some(result);
    }
    report(
        name,
        "row_executor_phase8_to_legacy_in_memory_catalog",
        "ns",
        &samples,
        json!({"sql": sql}),
    );
    last.expect("at least one measured query result")
}

#[test]
#[ignore = "manual Phase-10 performance characterization"]
fn phase10_row_executor_characterization() {
    // Deliberately modest: large enough to expose row-engine work while keeping
    // exact-base/head CI characterization bounded.
    let dataset = generate_tpch_data(0.001);
    let catalog = QueryCatalog::from_tpch(&dataset);

    let filter = measure_query(
        "row_filter_projection",
        "SELECT l_orderkey, l_quantity, l_discount FROM lineitem WHERE l_quantity < 12 AND l_discount >= 0.03 LIMIT 512",
        &catalog,
        true,
    );
    assert!(filter.columns.len() >= 3);

    let aggregate = measure_query(
        "row_aggregate_sort",
        "SELECT l_returnflag, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag ORDER BY l_returnflag",
        &catalog,
        true,
    );
    assert!(!aggregate.rows.is_empty());

    let join = measure_query(
        "row_equi_join",
        "SELECT o.o_orderkey, c.c_custkey FROM orders AS o INNER JOIN customer AS c ON o.o_custkey = c.c_custkey LIMIT 512",
        &catalog,
        true,
    );
    assert!(!join.rows.is_empty());

    // Characterize row->wire-batch materialization separately. The clone is
    // outside the timed interval so this isolates query_result_to_batch.
    for _ in 0..WARMUP {
        let batch = query_result_to_batch(clone_result(&filter));
        black_box(batch.row_count);
    }
    let mut materialize_samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let input = clone_result(&filter);
        let start = Instant::now();
        let batch: RecordBatch = query_result_to_batch(input);
        materialize_samples.push(start.elapsed().as_nanos());
        assert_eq!(batch.row_count, filter.rows.len());
        black_box(batch.row_count);
    }
    report(
        "row_result_materialization",
        "query_result_to_batch",
        "ns",
        &materialize_samples,
        json!({"rows": filter.rows.len(), "columns": filter.columns.len()}),
    );
}

fn bench_schema() -> TableSchema {
    TableSchema {
        name: "phase10_scan".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
            },
            ColumnDef {
                name: "value".to_string(),
                data_type: "BIGINT".to_string(),
            },
            ColumnDef {
                name: "payload".to_string(),
                data_type: "TEXT".to_string(),
            },
        ],
    }
}

#[test]
#[ignore = "manual Phase-10 performance characterization"]
fn phase10_persistent_rocksdb_scan_characterization() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Arc::new(StorageEngine::open(dir.path()).expect("open RocksDB"));
    let clock = Arc::new(HlcClock::new(500));
    let txn_mgr = Arc::new(TransactionManager::new(engine.clone(), clock));
    let catalog = Arc::new(InMemoryCatalog::default());
    catalog.create_table(bench_schema());
    let executor = StorageExecutor::new(engine, txn_mgr, Arc::clone(&catalog) as Arc<dyn Catalog>);

    const ROWS: usize = 1024;
    for i in 0..ROWS {
        let pk = executor.next_pk();
        let id = i.to_string();
        let value = (i * 17).to_string();
        let payload = format!("phase10-payload-{i:04}-abcdefghijklmnopqrstuvwxyz");
        executor
            .insert_row(
                "phase10_scan",
                &pk,
                &[("id", &id), ("value", &value), ("payload", &payload)],
            )
            .expect("seed persistent scan benchmark");
    }

    for _ in 0..WARMUP {
        let batch = executor.scan_table("phase10_scan").expect("warmup scan");
        assert_eq!(batch.row_count, ROWS);
        black_box(batch.row_count);
    }

    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let batch = executor.scan_table("phase10_scan").expect("measured scan");
        samples.push(start.elapsed().as_nanos());
        assert_eq!(batch.row_count, ROWS);
        black_box(batch.row_count);
    }
    report(
        "persistent_rocksdb_mvcc_scan_decode",
        "StorageExecutor::scan_table",
        "ns",
        &samples,
        json!({"rows": ROWS, "columns": 3, "setup_in_timing": false}),
    );
}

#[test]
#[ignore = "manual Phase-10 performance characterization"]
fn phase10_parser_characterization() {
    let sql = "SELECT l_orderkey, l_quantity, l_discount FROM lineitem WHERE l_quantity < 12 AND l_discount >= 0.03 LIMIT 512";
    for _ in 0..WARMUP {
        black_box(parse_statement(sql).expect("parse warmup"));
    }
    let mut samples = Vec::with_capacity(REPS * 50);
    for _ in 0..REPS * 50 {
        let start = Instant::now();
        let statement = parse_statement(sql).expect("parse measured");
        samples.push(start.elapsed().as_nanos());
        black_box(statement);
    }
    report(
        "parser_filter_projection",
        "sqlparser_parse_statement",
        "ns",
        &samples,
        json!({"sql": sql}),
    );
}

#[test]
fn phase10_result_clone_guard_preserves_values() {
    let source = QueryResult {
        columns: vec!["a".into(), "b".into()],
        rows: vec![vec![ScalarVal::Int(7), ScalarVal::Text("x".into())]],
    };
    let cloned = clone_result(&source);
    assert_eq!(source.columns, cloned.columns);
    assert_eq!(source.rows, cloned.rows);
}
