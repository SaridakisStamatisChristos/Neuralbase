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

fn insert_relation_name(
    name: &sqlparser::ast::ObjectName,
    relations: &mut std::collections::BTreeSet<String>,
) {
    // NeuralBase's persistent catalog is currently unqualified; preserve the
    // historical lookup semantics by using the final object identifier.
    if let Some(identifier) = name.0.last() {
        relations.insert(identifier.value.to_lowercase());
    }
}

fn collect_table_factor_relations(
    factor: &sqlparser::ast::TableFactor,
    relations: &mut std::collections::BTreeSet<String>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => insert_relation_name(name, relations),
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            collect_query_relations(subquery, relations)
        }
        // The current general executor rejects other TableFactor variants, so
        // there is no supported physical persistent relation hidden in them.
        _ => {}
    }
}

fn collect_select_relations(
    select: &sqlparser::ast::Select,
    relations: &mut std::collections::BTreeSet<String>,
) {
    for table_with_joins in &select.from {
        collect_table_factor_relations(&table_with_joins.relation, relations);
        for join in &table_with_joins.joins {
            collect_table_factor_relations(&join.relation, relations);
        }
    }

    // Correlated/non-correlated subqueries may appear in expressions. The
    // general executor supports these forms, so include their physical tables
    // without enabling sqlparser's optional visitor dependency.
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                collect_expr_relations(expr, relations)
            }
            _ => {}
        }
    }
    if let Some(expr) = &select.selection {
        collect_expr_relations(expr, relations);
    }
    if let Some(expr) = &select.having {
        collect_expr_relations(expr, relations);
    }
    if let sqlparser::ast::GroupByExpr::Expressions(expressions) = &select.group_by {
        for expr in expressions {
            collect_expr_relations(expr, relations);
        }
    }
}

fn collect_expr_relations(
    expr: &sqlparser::ast::Expr,
    relations: &mut std::collections::BTreeSet<String>,
) {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};

    match expr {
        Expr::Subquery(query)
        | Expr::Exists { subquery: query, .. }
        | Expr::InSubquery { subquery: query, .. } => collect_query_relations(query, relations),
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_relations(left, relations);
            collect_expr_relations(right, relations);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => collect_expr_relations(expr, relations),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_relations(expr, relations);
            collect_expr_relations(low, relations);
            collect_expr_relations(high, relations);
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_relations(expr, relations);
            for item in list {
                collect_expr_relations(item, relations);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expr_relations(operand, relations);
            }
            for condition in conditions {
                collect_expr_relations(condition, relations);
            }
            for result in results {
                collect_expr_relations(result, relations);
            }
            if let Some(result) = else_result {
                collect_expr_relations(result, relations);
            }
        }
        Expr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(args) = &function.args {
                for arg in &args.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        } => collect_expr_relations(expr, relations),
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }
}

fn collect_set_expr_relations(
    set_expr: &sqlparser::ast::SetExpr,
    relations: &mut std::collections::BTreeSet<String>,
) {
    match set_expr {
        sqlparser::ast::SetExpr::Select(select) => collect_select_relations(select, relations),
        sqlparser::ast::SetExpr::Query(query) => collect_query_relations(query, relations),
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_relations(left, relations);
            collect_set_expr_relations(right, relations);
        }
        _ => {}
    }
}

fn collect_query_relations(
    query: &sqlparser::ast::Query,
    relations: &mut std::collections::BTreeSet<String>,
) {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_relations(&cte.query, relations);
        }
    }
    collect_set_expr_relations(&query.body, relations);
    for order_by in &query.order_by {
        collect_expr_relations(&order_by.expr, relations);
    }
    if let Some(limit) = &query.limit {
        collect_expr_relations(limit, relations);
    }
    if let Some(offset) = &query.offset {
        collect_expr_relations(&offset.value, relations);
    }
}

/// Return the physical relation names referenced by a query, normalized to
/// lower-case catalog keys. This is deliberately dependency-neutral: Phase 11
/// does not enable sqlparser's optional visitor feature, keeping Cargo.lock and
/// the production dependency graph unchanged.
fn referenced_relation_names(
    query: &sqlparser::ast::Query,
) -> std::collections::BTreeSet<String> {
    let mut relations = std::collections::BTreeSet::new();
    collect_query_relations(query, &mut relations);
    relations
}

fn add_referenced_non_builtin_persistent_tables(
    qcat: &mut QueryCatalog,
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    storage: Option<&dyn TableScanner>,
) {
    let Some(scanner) = storage else {
        return;
    };

    for table in referenced_relation_names(query) {
        if is_builtin_tpch_table(&table) || catalog.get_table(&table).is_none() {
            continue;
        }
        if let Ok(batch) = scanner.scan_table(&table) {
            qcat.add_batch(&table, &batch);
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

    // Mixed TPC-H/persistent queries still need persistent relations, but only
    // those actually named by this query.
    for table in referenced_relation_names(query) {
        if qcat.tables.contains_key(&table) || catalog.get_table(&table).is_none() {
            continue;
        }
        if let Some(scanner) = storage {
            if let Ok(batch) = scanner.scan_table(&table) {
                qcat.add_batch(&table, &batch);
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
    add_referenced_non_builtin_persistent_tables(&mut persistent, query, catalog, storage);

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
    fn unrelated_persistent_tables_are_not_scanned() {
        let catalog = user_catalog();
        for i in 0..64 {
            catalog.register_table(TableSchema {
                name: format!("noise_{i}"),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                }],
            });
        }

        let sql = query("SELECT id, name FROM phase10_users WHERE id = 1");
        let scanner = TestScanner::default().with_batch("phase10_users", user_batch());
        execute_select_query_with_persistent_fast_path(&sql, &catalog, Some(&scanner))
            .expect("persistent fast path");

        assert_eq!(scanner.calls(), vec!["phase10_users".to_string()]);
    }

    #[test]
    fn derived_subquery_relations_are_collected_without_catalog_sweep() {
        let catalog = user_catalog();
        let sql = query(
            "SELECT d.id, d.name FROM (SELECT id, name FROM phase10_users WHERE id = 1) d",
        );
        let scanner = TestScanner::default().with_batch("phase10_users", user_batch());
        execute_select_query_with_persistent_fast_path(&sql, &catalog, Some(&scanner))
            .expect("derived persistent fast path");
        assert_eq!(scanner.calls(), vec!["phase10_users".to_string()]);
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
            .expect("TPC-H fallback");

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
