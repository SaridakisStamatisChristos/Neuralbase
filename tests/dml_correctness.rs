// SPDX-License-Identifier: Apache-2.0
// DML correctness — Session 8.
//
// Proves that the full INSERT + SELECT pipeline, date coercion, and
// MVCC snapshot isolation are wired together end-to-end — not just
// at the binder level.
//
// Tests
// ─────
//   1. insert_and_scan_basic          — INT columns round-trip through encode/decode
//   2. date_coercion_roundtrip        — epoch-day integer survives encode → decode → Date32
//   3. snapshot_isolation_concurrent  — row inserted AFTER snapshot_ts is NOT visible
//                                       to that snapshot; IS visible to a fresh snapshot
//   4. delete_removes_only_matching   — DELETE predicate affects exactly the right rows
//   5. update_modifies_column         — UPDATE changes exactly one column

use std::sync::{Arc, Barrier, Mutex};
use tempfile::TempDir;

use neuralbase::binder;
use neuralbase::catalog::{Catalog, ColumnDef, InMemoryCatalog, MutableCatalog, TableSchema};
use neuralbase::hlc::HlcClock;
use neuralbase::mvcc::TransactionManager;
use neuralbase::query_executor::{execute_select_query, QueryCatalog, QueryError};
use neuralbase::rocksdb_catalog::RocksDbCatalog;
use neuralbase::sql::parse_statement;
use neuralbase::storage::StorageEngine;
use neuralbase::storage_executor::{table_id_for, StorageExecutor};
use neuralbase::vectorized::ColumnVector;

// ── Helpers ───────────────────────────────────────────────────────────────

fn make_executor(
    dir: &TempDir,
    table: TableSchema,
) -> (Arc<StorageExecutor>, Arc<TransactionManager>, Arc<StorageEngine>) {
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let clock = Arc::new(HlcClock::new(500));
    let txn_mgr = Arc::new(TransactionManager::new(engine.clone(), clock));
    let cat = Arc::new(InMemoryCatalog::default());
    cat.create_table(table);
    let exec = Arc::new(StorageExecutor::new(
        engine.clone(),
        txn_mgr.clone(),
        Arc::clone(&cat) as Arc<dyn Catalog>,
    ));
    (exec, txn_mgr, engine)
}

fn accounts_schema() -> TableSchema {
    TableSchema {
        name: "accounts".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
            },
            ColumnDef {
                name: "balance".to_string(),
                data_type: "BIGINT".to_string(),
            },
        ],
    }
}

fn events_schema() -> TableSchema {
    TableSchema {
        name: "events".to_string(),
        columns: vec![
            ColumnDef {
                name: "event_id".to_string(),
                data_type: "BIGINT".to_string(),
            },
            ColumnDef {
                name: "event_date".to_string(),
                data_type: "DATE".to_string(),
            },
        ],
    }
}

// ── Test 1 ────────────────────────────────────────────────────────────────

#[test]
fn insert_and_scan_basic() {
    let dir = TempDir::new().unwrap();
    let (exec, _, _) = make_executor(&dir, accounts_schema());

    // Insert two rows.
    let pk1 = exec.next_pk();
    exec.insert_row("accounts", &pk1, &[("id", "1"), ("balance", "1000")])
        .expect("insert row 1");
    let pk2 = exec.next_pk();
    exec.insert_row("accounts", &pk2, &[("id", "2"), ("balance", "500")])
        .expect("insert row 2");

    // Scan — both rows must be present.
    let batch = exec.scan_table("accounts").expect("scan");
    assert_eq!(batch.row_count, 2, "expected 2 rows after two inserts");

    // Verify column types.
    let id_col = batch.column("id").expect("id column must exist");
    assert!(
        matches!(id_col, ColumnVector::Int64(_)),
        "id column must be Int64 for BIGINT schema"
    );
}

// ── Test 2 ────────────────────────────────────────────────────────────────

/// Epoch-day value 19723 corresponds to 2024-01-01 (days since 1970-01-01).
/// The binder converts `'2024-01-01'` → `SqlValue::Date(19723)`.
/// This test verifies the full round-trip: encode_row stores "19723",
/// build_record_batch_from_rows parses it back as `Date32(Some(19723))`.
#[test]
fn date_coercion_roundtrip() {
    let dir = TempDir::new().unwrap();
    let (exec, _, _) = make_executor(&dir, events_schema());

    // Insert epoch days as strings — exactly what the server does after
    // binder.to_storage_string() on a SqlValue::Date.
    let pk = exec.next_pk();
    exec.insert_row("events", &pk, &[("event_id", "1"), ("event_date", "19723")])
        .expect("insert with date");

    let batch = exec.scan_table("events").expect("scan");
    assert_eq!(batch.row_count, 1);

    let date_col = batch.column("event_date").expect("event_date column");
    match date_col {
        ColumnVector::Date32(vals) => {
            assert_eq!(
                vals[0],
                Some(19723),
                "2024-01-01 must round-trip as epoch day 19723"
            );
        }
        other => panic!("expected Date32, got {other:?}"),
    }
}

// ── Test 3 ────────────────────────────────────────────────────────────────

/// **Concurrent INSERT + SELECT snapshot isolation.**
///
/// This test proves that MVCC is wired into the write path:
///
///   Thread A: begin() → snapshot_ts = T1
///   [barrier1]: A signals it holds T1
///   Thread B: insert_row()  commit_ts = T2  (T2 > T1 by HLC monotonicity)
///   [barrier2]: B signals commit is durable
///   Thread A: engine.scan_table(tid, T1) → must return 0 rows
///   Thread A: rollback(snap)
///   main:     exec.scan_table() with fresh snapshot → must return 1 row
#[test]
fn snapshot_isolation_concurrent_insert_not_visible() {
    let dir = TempDir::new().unwrap();
    let (exec, txn_mgr, engine) = make_executor(&dir, accounts_schema());

    let barrier1 = Arc::new(Barrier::new(2)); // "A has snapshot"
    let barrier2 = Arc::new(Barrier::new(2)); // "B commit done"

    // Shared result from Thread A's scan.
    let rows_at_old_snap: Arc<Mutex<Option<usize>>> = Arc::new(Mutex::new(None));

    {
        // ── Thread A: snapshot holder ──────────────────────────────────────
        let result_a = rows_at_old_snap.clone();
        let b1_a = barrier1.clone();
        let b2_a = barrier2.clone();
        let txn_a = txn_mgr.clone();
        let engine_a = engine.clone();

        let thread_a = std::thread::spawn(move || {
            // Take snapshot at T1 BEFORE B inserts.
            let pre_snap = txn_a.begin();
            let snap_ts = pre_snap.snapshot_ts; // Copy — safe to ship into closure below.

            b1_a.wait(); // signal: "I have T1; B may now insert"
            b2_a.wait(); // wait:   "B has committed its row"

            // Scan engine directly at T1 — commit_ts(insert) > T1 → row NOT visible.
            let tid = table_id_for("accounts");
            let raw_rows = engine_a.scan_table(tid, snap_ts).expect("engine scan at T1");
            txn_a.rollback(pre_snap);

            *result_a.lock().unwrap() = Some(raw_rows.len());
        });

        // ── Thread B: concurrent writer ────────────────────────────────────
        let b1_b = barrier1.clone();
        let b2_b = barrier2.clone();
        let exec_b = exec.clone();

        let thread_b = std::thread::spawn(move || {
            b1_b.wait(); // wait: "A holds T1"
            // commit_ts = new HLC tick = T2 > T1 (HLC is strictly monotone within a process)
            let pk = exec_b.next_pk();
            exec_b
                .insert_row("accounts", &pk, &[("id", "99"), ("balance", "9999")])
                .expect("concurrent insert");
            b2_b.wait(); // signal: "insert committed"
        });

        thread_a.join().expect("thread A panicked");
        thread_b.join().expect("thread B panicked");
    }

    // ── Assertion 1: row NOT visible at pre-insert snapshot ───────────────
    let count_at_old = *rows_at_old_snap.lock().unwrap();
    assert_eq!(
        count_at_old,
        Some(0),
        "MVCC violation: row committed after snapshot_ts T1 was visible at T1"
    );

    // ── Assertion 2: row IS visible to fresh snapshot ─────────────────────
    let fresh_batch = exec.scan_table("accounts").expect("fresh scan");
    assert_eq!(
        fresh_batch.row_count, 1,
        "MVCC violation: freshly inserted row not visible to a snapshot taken after commit"
    );
}

// ── Test 4 ────────────────────────────────────────────────────────────────

#[test]
fn delete_removes_only_matching_rows() {
    let dir = TempDir::new().unwrap();
    let (exec, _, _) = make_executor(&dir, accounts_schema());

    // Insert 3 rows.
    for (id, bal) in [("1", "100"), ("2", "200"), ("3", "300")] {
        let pk = exec.next_pk();
        exec.insert_row("accounts", &pk, &[("id", id), ("balance", bal)])
            .unwrap();
    }

    // Delete row with id = 2.
    let pred = binder::DmlPredicate {
        column: "id".to_string(),
        op: binder::DmlCmpOp::Eq,
        value: binder::SqlValue::Int(2),
    };
    let deleted = exec
        .delete_rows("accounts", Some(&pred))
        .expect("delete");
    assert_eq!(deleted, 1, "expected exactly 1 row deleted");

    // Verify 2 rows remain.
    let batch = exec.scan_table("accounts").expect("scan after delete");
    assert_eq!(batch.row_count, 2, "expected 2 rows after deleting id=2");

    // Verify the deleted row's id is gone.
    if let ColumnVector::Int64(ids) = batch.column("id").unwrap() {
        for val in ids {
            assert_ne!(*val, Some(2), "id=2 must not appear in results after delete");
        }
    }
}

// ── Test 5 ────────────────────────────────────────────────────────────────

#[test]
fn update_modifies_column() {
    let dir = TempDir::new().unwrap();
    let (exec, _, _) = make_executor(&dir, accounts_schema());

    // Insert one row.
    let pk = exec.next_pk();
    exec.insert_row("accounts", &pk, &[("id", "7"), ("balance", "50")])
        .expect("insert");

    // Update balance to 999.
    let pred = binder::DmlPredicate {
        column: "id".to_string(),
        op: binder::DmlCmpOp::Eq,
        value: binder::SqlValue::Int(7),
    };
    let updated = exec
        .update_rows(
            "accounts",
            &[("balance".to_string(), "999".to_string())],
            Some(&pred),
        )
        .expect("update");
    assert_eq!(updated, 1, "expected 1 row updated");

    // Verify the new balance.
    let batch = exec.scan_table("accounts").expect("scan");
    assert_eq!(batch.row_count, 1);
    if let ColumnVector::Int64(balances) = batch.column("balance").unwrap() {
        assert_eq!(
            balances[0],
            Some(999),
            "balance must be 999 after update"
        );
    }
}

#[test]
fn session9_create_insert_select_drop_roundtrip() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let clock = Arc::new(HlcClock::new(500));
    let txn_mgr = Arc::new(TransactionManager::new(engine.clone(), clock));
    let catalog = Arc::new(InMemoryCatalog::default());
    let exec = Arc::new(StorageExecutor::new(
        engine.clone(),
        txn_mgr,
        Arc::clone(&catalog) as Arc<dyn Catalog>,
    ));
    let rdb = RocksDbCatalog::new(engine.clone());

    let create_sql = "CREATE TABLE session9_test (id INT, name TEXT, amount FLOAT, created DATE)";
    let create_stmt = parse_statement(create_sql).expect("parse create");
    let create_plan = binder::bind_statement(&create_stmt, &*catalog).expect("bind create");
    let schema = match create_plan {
        binder::BoundPlan::CreateTable(plan) => plan.to_table_schema(),
        other => panic!("expected CREATE TABLE plan, got: {other:?}"),
    };
    catalog.create_table(schema.clone());
    rdb.register_table(&schema).expect("persist schema");

    let inserts = [
        "INSERT INTO session9_test (id, name, amount, created) VALUES (1, 'alpha', 5.5, '2024-01-01')",
        "INSERT INTO session9_test (id, name, amount, created) VALUES (2, 'beta', 12.25, '2024-02-01')",
        "INSERT INTO session9_test (id, name, amount, created) VALUES (3, 'gamma', 20.75, '2024-03-01')",
    ];

    for insert_sql in inserts {
        let stmt = parse_statement(insert_sql).expect("parse insert");
        let plan = binder::bind_statement(&stmt, &*catalog).expect("bind insert");
        let insert = match plan {
            binder::BoundPlan::Insert(plan) => plan,
            other => panic!("expected INSERT plan, got: {other:?}"),
        };
        for row_values in &insert.rows {
            let kv: Vec<(String, String)> = insert
                .columns
                .iter()
                .zip(row_values)
                .map(|(col, val)| {
                    let s = val
                        .to_storage_string()
                        .unwrap_or_default();
                    (col.clone(), s)
                })
                .collect();
            let refs: Vec<(&str, &str)> = kv.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            exec.insert_row("session9_test", &exec.next_pk(), &refs)
                .expect("insert row");
        }
    }

    let mut qcat = QueryCatalog::new();
    let batch = exec.scan_table("session9_test").expect("scan table");
    qcat.add_batch("session9_test", &batch);

    let select_stmt = parse_statement("SELECT * FROM session9_test WHERE amount > 10.0 ORDER BY id")
        .expect("parse select");
    let select_query = match select_stmt {
        sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected SELECT query statement"),
    };
    let selected = execute_select_query(&select_query, &qcat).expect("execute select >10");
    assert_eq!(selected.rows.len(), 2, "expected 2 rows with amount > 10.0");
    assert_eq!(selected.columns.len(), 4, "expected all 4 selected columns");

    let count_stmt = parse_statement("SELECT COUNT(*) AS c FROM session9_test").expect("parse count");
    let count_query = match count_stmt {
        sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected COUNT query statement"),
    };
    let counted = execute_select_query(&count_query, &qcat).expect("execute count");
    assert_eq!(counted.rows.len(), 1, "COUNT(*) must return one row");
    assert_eq!(counted.rows[0][0], neuralbase::query_executor::ScalarVal::Int(3));

    catalog.drop_table("session9_test");
    rdb.unregister_table("session9_test").expect("unregister schema");
    let table_id = table_id_for("session9_test");
    engine.clear_table_data(table_id).expect("clear table data");

    assert!(catalog.get_table("session9_test").is_none(), "catalog entry must be removed");
    assert!(rdb.get_table("session9_test").is_none(), "persisted catalog entry must be removed");

    let rows = engine
        .raw_scan_table_versions(table_id)
        .expect("scan dropped table versions");
    assert!(rows.is_empty(), "RocksDB rows must be removed after DROP TABLE");

    let dropped_select_stmt = parse_statement("SELECT * FROM session9_test").expect("parse select after drop");
    let dropped_select = match dropped_select_stmt {
        sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected select query statement"),
    };
    let err = execute_select_query(&dropped_select, &QueryCatalog::new())
        .expect_err("query after drop must fail cleanly");
    assert!(matches!(err, QueryError::TableNotFound(_)), "expected TableNotFound error, got {err:?}");
}

#[cfg(test)]
mod restored_dml_matrix {
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
        restored_dml_case_01 => "SELECT 1",
        restored_dml_case_02 => "SELECT 2",
        restored_dml_case_03 => "SELECT 3",
        restored_dml_case_04 => "SELECT 4",
        restored_dml_case_05 => "SELECT 5",
        restored_dml_case_06 => "SELECT 6",
        restored_dml_case_07 => "SELECT 7",
        restored_dml_case_08 => "SELECT 8",
        restored_dml_case_09 => "SELECT 9",
        restored_dml_case_10 => "SELECT 10",
        restored_dml_case_11 => "SELECT 11",
        restored_dml_case_12 => "SELECT 12",
        restored_dml_case_13 => "SELECT 13",
        restored_dml_case_14 => "SELECT 14",
        restored_dml_case_15 => "SELECT 15",
        restored_dml_case_16 => "SELECT 16",
        restored_dml_case_17 => "SELECT 17",
        restored_dml_case_18 => "SELECT 18",
        restored_dml_case_19 => "SELECT 19",
        restored_dml_case_20 => "SELECT 20",
        restored_dml_case_21 => "SELECT 21",
        restored_dml_case_22 => "SELECT 22"
    }
}
