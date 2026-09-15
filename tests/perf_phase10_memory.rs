// SPDX-License-Identifier: Apache-2.0
//! Phase-10 bounded-memory characterization for row-oriented joins, sorts and
//! aggregates. Each test is intended to be invoked separately so Linux VmHWM
//! growth reflects one workload after dataset/catalog setup.

use std::fs;
use std::hint::black_box;

use neuralbase::query_executor::{execute_select_query, QueryCatalog};
use neuralbase::sql::parse_statement;
use neuralbase::tpch::generate_tpch_data;
use serde_json::json;
use sqlparser::ast::Statement;

fn label() -> String {
    std::env::var("PHASE10_LABEL").unwrap_or_else(|_| "unlabeled".into())
}

fn measured_commit() -> String {
    std::env::var("PHASE10_MEASURED_COMMIT").unwrap_or_else(|_| "unknown".into())
}

fn vm_hwm_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1024)
}

fn run(name: &str, sql: &str) {
    let dataset = generate_tpch_data(0.01);
    let catalog = QueryCatalog::from_tpch(&dataset);
    let query = match parse_statement(sql).expect("memory query parse") {
        Statement::Query(query) => query,
        other => panic!("expected query, got {other:?}"),
    };
    let before = vm_hwm_bytes();
    let result = execute_select_query(&query, &catalog).expect("memory workload execute");
    black_box(result.rows.len());
    let after = vm_hwm_bytes();
    let delta = before.zip(after).map(|(a, b)| b.saturating_sub(a));
    println!(
        "PHASE10_RESULT {}",
        json!({
            "schema": 1,
            "label": label(),
            "commit": measured_commit(),
            "name": name,
            "path": "row_executor_linux_vmhwm_delta",
            "unit": "bytes",
            "warmup_iterations": 0,
            "measured_iterations": 1,
            "min": delta,
            "p50": delta,
            "p95": delta,
            "max": delta,
            "mean": delta,
            "extra": {
                "scale_factor": 0.01,
                "linux_proc_vmhwm_before": before,
                "linux_proc_vmhwm_after": after,
                "result_rows": result.rows.len(),
                "sql": sql,
                "interpretation": "process high-water growth after dataset/catalog setup; run workload in a fresh test process"
            }
        })
    );
}

#[test]
#[ignore = "manual Phase-10 memory characterization"]
fn phase10_memory_join() {
    run(
        "memory_join",
        "SELECT o.o_orderkey, c.c_custkey FROM orders AS o INNER JOIN customer AS c ON o.o_custkey = c.c_custkey LIMIT 10000",
    );
}

#[test]
#[ignore = "manual Phase-10 memory characterization"]
fn phase10_memory_sort() {
    run(
        "memory_sort",
        "SELECT l_orderkey, l_extendedprice FROM lineitem ORDER BY l_extendedprice DESC LIMIT 10000",
    );
}

#[test]
#[ignore = "manual Phase-10 memory characterization"]
fn phase10_memory_aggregate() {
    run(
        "memory_aggregate",
        "SELECT l_returnflag, l_linestatus, COUNT(*) AS c, SUM(l_extendedprice) AS s FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
    );
}
