// Phase-10 general-query routing helpers.
//
// The historical SelectQuery path materializes the full synthetic SF=0.1
// TPC-H catalog before every execution, even for queries that reference only
// persistent user tables. Preserve that path as the semantic fallback, but try
// a persistent-only catalog first. A missing table is the only condition that
// triggers fallback; all other executor errors remain authoritative.

const BUILTIN_TPCH_TABLES: [&str; 8] = [
    "lineitem",
    "orders",
    "customer",
    "nation",
    "region",
    "part",
    "supplier",
    "partsupp",
];

fn is_builtin_tpch_table(name: &str) -> bool {
    BUILTIN_TPCH_TABLES
        .iter()
        .any(|builtin| builtin.eq_ignore_ascii_case(name))
}

fn add_non_builtin_persistent_tables(
    qcat: &mut QueryCatalog,
    catalog: &InMemoryCatalog,
    storage: Option<&dyn TableScanner>,
) {
    let Some(scanner) = storage else {
        return;
    };

    for schema in catalog.all_tables() {
        if is_builtin_tpch_table(&schema.name) {
            continue;
        }
        if let Ok(batch) = scanner.scan_table(&schema.name) {
            qcat.add_batch(&schema.name, &batch);
        }
    }
}

fn execute_select_query_tpch_first(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    storage: Option<&dyn TableScanner>,
) -> Result<crate::query_executor::QueryResult, crate::query_executor::QueryError> {
    let dataset = generate_tpch_data(0.1);
    let mut qcat = QueryCatalog::from_tpch(&dataset);
    for schema in catalog.all_tables() {
        if !qcat.tables.contains_key(&schema.name.to_lowercase()) {
            if let Some(scanner) = storage {
                if let Ok(batch) = scanner.scan_table(&schema.name) {
                    qcat.add_batch(&schema.name, &batch);
                }
            }
        }
    }
    crate::query_executor::execute_select_query(query, &qcat)
}

fn execute_select_query_with_persistent_fast_path(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    storage: Option<&dyn TableScanner>,
) -> Result<crate::query_executor::QueryResult, crate::query_executor::QueryError> {
    let mut persistent = QueryCatalog::new();
    add_non_builtin_persistent_tables(&mut persistent, catalog, storage);

    match crate::query_executor::execute_select_query(query, &persistent) {
        Err(crate::query_executor::QueryError::TableNotFound(_)) => {
            execute_select_query_tpch_first(query, catalog, storage)
        }
        result => result,
    }
}

#[cfg(test)]
mod phase10_query_fast_path_tests {
    use super::*;
    use crate::catalog::{ColumnDef, TableSchema};
    use crate::query_executor::ScalarVal;
    use crate::vectorized::{ColumnVector, ExecError, RecordBatch, Utf8Column};
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestScanner {
        batches: HashMap<String, RecordBatch>,
        calls: Mutex<Vec<String>>,
    }

    impl TestScanner {
        fn with_batch(mut self, name: &str, batch: RecordBatch) -> Self {
            self.batches.insert(name.to_lowercase(), batch);
            self
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls lock poisoned").clone()
        }
    }

    impl TableScanner for TestScanner {
        fn scan_table(&self, table_name: &str) -> Result<RecordBatch, ExecError> {
            self.calls
                .lock()
                .expect("calls lock poisoned")
                .push(table_name.to_lowercase());
            self.batches
                .get(&table_name.to_lowercase())
                .cloned()
                .ok_or_else(|| ExecError::TableNotFound(table_name.to_string()))
        }
    }

    fn user_catalog() -> InMemoryCatalog {
        let catalog = InMemoryCatalog::with_tpch_all_tables();
        catalog.register_table(TableSchema {
            name: "phase10_users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                },
            ],
        });
        catalog
    }

    fn user_batch() -> RecordBatch {
        RecordBatch::new(vec![
            (
                "id".to_string(),
                ColumnVector::Int64(vec![Some(1), Some(2)]),
            ),
            (
                "name".to_string(),
                ColumnVector::Utf8(Utf8Column::from_owned_options(vec![
                    Some("alpha".to_string()),
                    Some("beta".to_string()),
                ])),
            ),
        ])
        .expect("valid user batch")
    }

    fn nation_collision_batch() -> RecordBatch {
        RecordBatch::new(vec![
            (
                "n_nationkey".to_string(),
                ColumnVector::Int64(vec![Some(1)]),
            ),
            (
                "n_name".to_string(),
                ColumnVector::Utf8(Utf8Column::from_owned_options(vec![Some(
                    "CUSTOM".to_string(),
                )])),
            ),
        ])
        .expect("valid nation collision batch")
    }

    fn query(sql: &str) -> Box<sqlparser::ast::Query> {
        match parse_statement(sql).expect("query parse") {
            Statement::Query(query) => query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    fn assert_same_result(
        fast: &crate::query_executor::QueryResult,
        reference: &crate::query_executor::QueryResult,
    ) {
        assert_eq!(fast.columns, reference.columns);
        assert_eq!(fast.rows, reference.rows);
    }

    #[test]
    fn persistent_only_general_query_avoids_builtin_scans_and_matches_reference() {
        let catalog = user_catalog();
        let sql = query(
            "SELECT a.id, a.name FROM phase10_users a \
             JOIN phase10_users b ON a.id = b.id WHERE a.id = 1",
        );
        let fast_scanner = TestScanner::default().with_batch("phase10_users", user_batch());
        let fast = execute_select_query_with_persistent_fast_path(
            &sql,
            &catalog,
            Some(&fast_scanner),
        )
        .expect("persistent fast path");
        assert_eq!(fast_scanner.calls(), vec!["phase10_users".to_string()]);

        let reference_scanner =
            TestScanner::default().with_batch("phase10_users", user_batch());
        let reference = execute_select_query_tpch_first(
            &sql,
            &catalog,
            Some(&reference_scanner),
        )
        .expect("historical reference path");
        assert_same_result(&fast, &reference);
    }

    #[test]
    fn tpch_query_falls_back_to_historical_catalog() {
        let catalog = user_catalog();
        let sql = query("SELECT n_name FROM nation WHERE n_nationkey = 1");
        let scanner = TestScanner::default().with_batch("phase10_users", user_batch());
        let fast = execute_select_query_with_persistent_fast_path(&sql, &catalog, Some(&scanner))
            .expect("TPC-H fallback");

        let reference_scanner =
            TestScanner::default().with_batch("phase10_users", user_batch());
        let reference = execute_select_query_tpch_first(
            &sql,
            &catalog,
            Some(&reference_scanner),
        )
        .expect("historical reference path");
        assert_same_result(&fast, &reference);
    }

    #[test]
    fn mixed_persistent_and_tpch_query_falls_back_and_matches_reference() {
        let catalog = user_catalog();
        let sql = query(
            "SELECT u.id, n.n_name FROM phase10_users u \
             JOIN nation n ON u.id = n.n_nationkey WHERE u.id = 1",
        );
        let scanner = TestScanner::default().with_batch("phase10_users", user_batch());
        let fast = execute_select_query_with_persistent_fast_path(&sql, &catalog, Some(&scanner))
            .expect("mixed fallback");

        let reference_scanner =
            TestScanner::default().with_batch("phase10_users", user_batch());
        let reference = execute_select_query_tpch_first(
            &sql,
            &catalog,
            Some(&reference_scanner),
        )
        .expect("historical reference path");
        assert_same_result(&fast, &reference);
    }

    #[test]
    fn persistent_builtin_name_does_not_override_historical_tpch_precedence() {
        let catalog = user_catalog();
        let sql = query("SELECT n_name FROM nation WHERE n_nationkey = 1");
        let scanner = TestScanner::default()
            .with_batch("phase10_users", user_batch())
            .with_batch("nation", nation_collision_batch());
        let fast = execute_select_query_with_persistent_fast_path(&sql, &catalog, Some(&scanner))
            .expect("collision-preserving fallback");

        let reference_scanner = TestScanner::default()
            .with_batch("phase10_users", user_batch())
            .with_batch("nation", nation_collision_batch());
        let reference = execute_select_query_tpch_first(
            &sql,
            &catalog,
            Some(&reference_scanner),
        )
        .expect("historical reference path");
        assert_same_result(&fast, &reference);
        assert!(fast.rows.iter().flatten().all(|value| {
            value != &ScalarVal::Text("CUSTOM".to_string())
        }));
    }
}
