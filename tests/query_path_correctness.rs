// SPDX-License-Identifier: Apache-2.0
//! Correctness locks for the first Phase-10 query-path optimization.
//!
//! These tests describe existing behavior before the production fast path is
//! introduced: ordinary persistent tables are scanned through `TableScanner`,
//! while `lineitem` remains the built-in TPC-H fixture route. Missing persistent
//! tables keep the historical empty-batch behavior of the physical executor.

use std::sync::atomic::{AtomicUsize, Ordering};

use neuralbase::execution::{execute_physical_plan, PhysicalPlan, TableScanner};
use neuralbase::scheduler::MorselScheduler;
use neuralbase::tpch::generate_tpch_data;
use neuralbase::vectorized::{ColumnVector, ExecError, RecordBatch};

struct StubScanner {
    calls: AtomicUsize,
    mode: ScanMode,
}

enum ScanMode {
    Batch(RecordBatch),
    Missing,
}

impl StubScanner {
    fn batch(batch: RecordBatch) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            mode: ScanMode::Batch(batch),
        }
    }

    fn missing() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            mode: ScanMode::Missing,
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl TableScanner for StubScanner {
    fn scan_table(&self, table_name: &str) -> Result<RecordBatch, ExecError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.mode {
            ScanMode::Batch(batch) => Ok(batch.clone()),
            ScanMode::Missing => Err(ExecError::TableNotFound(table_name.to_string())),
        }
    }
}

fn user_batch() -> RecordBatch {
    RecordBatch::new(vec![
        (
            "id".to_string(),
            ColumnVector::Int64(vec![Some(1), Some(2)]),
        ),
        (
            "name".to_string(),
            ColumnVector::Utf8(neuralbase::vectorized::Utf8Column::from_options(vec![
                Some("alpha"),
                Some("beta"),
            ])),
        ),
    ])
    .expect("valid user batch")
}

#[test]
fn persistent_non_lineitem_scan_uses_storage_and_preserves_limit() {
    let scanner = StubScanner::batch(user_batch());
    let data = generate_tpch_data(0.0001);
    let scheduler = MorselScheduler::new(1024);
    let plan = PhysicalPlan::Scan {
        table: "phase10_user_items".to_string(),
        projection: vec!["id".to_string()],
        predicate: None,
        limit: Some(1),
    };

    let output = execute_physical_plan(&plan, &data, &scheduler, Some(&scanner))
        .expect("persistent user-table scan");

    assert_eq!(scanner.calls(), 1);
    assert_eq!(output.row_count, 1);
    // Existing non-lineitem semantics return the scanner batch shape rather
    // than applying the projection in the physical executor. Phase 10 must not
    // silently change that behavior while removing setup overhead.
    assert_eq!(output.columns.len(), 2);
}

#[test]
fn lineitem_scan_keeps_tpch_fixture_authority_over_storage_scanner() {
    let scanner = StubScanner::batch(user_batch());
    let data = generate_tpch_data(0.0001);
    let scheduler = MorselScheduler::new(1024);
    let plan = PhysicalPlan::Scan {
        table: "lineitem".to_string(),
        projection: vec!["l_orderkey".to_string()],
        predicate: None,
        limit: Some(1),
    };

    let output = execute_physical_plan(&plan, &data, &scheduler, Some(&scanner))
        .expect("TPC-H lineitem scan");

    assert_eq!(scanner.calls(), 0, "lineitem must remain TPC-H-backed");
    assert_eq!(output.row_count, 1);
    assert!(output.column("l_orderkey").is_some());
    assert!(output.column("id").is_none());
}

#[test]
fn missing_persistent_table_keeps_historical_empty_batch_behavior() {
    let scanner = StubScanner::missing();
    let data = generate_tpch_data(0.0001);
    let scheduler = MorselScheduler::new(1024);
    let plan = PhysicalPlan::Scan {
        table: "phase10_missing".to_string(),
        projection: Vec::new(),
        predicate: None,
        limit: None,
    };

    let output = execute_physical_plan(&plan, &data, &scheduler, Some(&scanner))
        .expect("historical missing-table behavior");

    assert_eq!(scanner.calls(), 1);
    assert_eq!(output.row_count, 0);
    assert!(output.columns.is_empty());
}
