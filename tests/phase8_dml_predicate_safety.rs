// SPDX-License-Identifier: Apache-2.0
// Phase 8: persistent DML predicate safety regression tests.

use neuralbase::binder::{bind_statement, BindError, BoundPlan};
use neuralbase::catalog::{ColumnDef, InMemoryCatalog, TableSchema};
use neuralbase::sql::parse_statement;

fn catalog() -> InMemoryCatalog {
    let cat = InMemoryCatalog::default();
    cat.register_table(TableSchema {
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
    });
    cat
}

#[test]
fn update_complex_where_fails_closed_instead_of_becoming_full_table_update() {
    let stmt = parse_statement(
        "UPDATE accounts SET balance = 0 WHERE id = 1 AND balance > 0",
    )
    .expect("parse UPDATE");

    assert!(matches!(
        bind_statement(&stmt, &catalog()),
        Err(BindError::Unsupported)
    ));
}

#[test]
fn delete_unknown_where_column_fails_closed_instead_of_becoming_full_table_delete() {
    let stmt = parse_statement("DELETE FROM accounts WHERE missing = 1").expect("parse DELETE");

    assert!(matches!(
        bind_statement(&stmt, &catalog()),
        Err(BindError::Unsupported)
    ));
}

#[test]
fn update_non_literal_rhs_fails_closed() {
    let stmt =
        parse_statement("UPDATE accounts SET balance = 0 WHERE id = balance").expect("parse UPDATE");

    assert!(matches!(
        bind_statement(&stmt, &catalog()),
        Err(BindError::Unsupported)
    ));
}

#[test]
fn simple_update_predicate_remains_supported() {
    let stmt =
        parse_statement("UPDATE accounts SET balance = 0 WHERE id = 1").expect("parse UPDATE");

    match bind_statement(&stmt, &catalog()).expect("bind simple UPDATE") {
        BoundPlan::Update(plan) => assert!(plan.predicate.is_some()),
        other => panic!("expected UPDATE plan, got {other:?}"),
    }
}

#[test]
fn explicit_unfiltered_delete_retains_full_table_semantics() {
    let stmt = parse_statement("DELETE FROM accounts").expect("parse DELETE");

    match bind_statement(&stmt, &catalog()).expect("bind unfiltered DELETE") {
        BoundPlan::Delete(plan) => assert!(plan.predicate.is_none()),
        other => panic!("expected DELETE plan, got {other:?}"),
    }
}
