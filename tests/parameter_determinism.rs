// SPDX-License-Identifier: Apache-2.0
// Phase 8: parameter decoding happens before the established DML/Raft path.

use neuralbase::binder::{bind_statement, BoundPlan, DmlCmpOp, SqlValue};
use neuralbase::catalog::{ColumnDef, InMemoryCatalog, TableSchema};
use neuralbase::extended_protocol::{materialize_bound_sql, BindMessage, INT8OID};
use neuralbase::sql::parse_statement;

fn catalog() -> InMemoryCatalog {
    let catalog = InMemoryCatalog::default();
    catalog.register_table(TableSchema {
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
    catalog
}

#[test]
fn typed_bound_update_becomes_concrete_deterministic_plan_before_gateway() {
    let bind = BindMessage {
        portal_name: "p".to_string(),
        statement_name: "s".to_string(),
        parameter_formats: vec![],
        parameters: vec![Some(b"125".to_vec()), Some(b"7".to_vec())],
        result_formats: vec![],
    };

    let sql = materialize_bound_sql(
        "UPDATE accounts SET balance = $1 WHERE id = $2",
        &[INT8OID, INT8OID],
        &bind,
    )
    .expect("materialize typed parameters");
    assert_eq!(sql, "UPDATE accounts SET balance = 125 WHERE id = 7");

    let statement = parse_statement(&sql).expect("parse materialized SQL");
    let BoundPlan::Update(plan) = bind_statement(&statement, &catalog()).expect("bind update")
    else {
        panic!("expected update plan");
    };

    assert_eq!(
        plan.assignments,
        vec![("balance".to_string(), SqlValue::Int(125))]
    );
    let predicate = plan.predicate.expect("concrete predicate");
    assert_eq!(predicate.column, "id");
    assert_eq!(predicate.op, DmlCmpOp::Eq);
    assert_eq!(predicate.value, SqlValue::Int(7));
}

#[test]
fn repeated_materialization_is_byte_identical() {
    let bind = BindMessage {
        portal_name: "p".to_string(),
        statement_name: "s".to_string(),
        parameter_formats: vec![1],
        parameters: vec![Some(42_i64.to_be_bytes().to_vec())],
        result_formats: vec![],
    };

    let first = materialize_bound_sql("DELETE FROM accounts WHERE id = $1", &[INT8OID], &bind)
        .expect("first materialization");
    let second = materialize_bound_sql("DELETE FROM accounts WHERE id = $1", &[INT8OID], &bind)
        .expect("second materialization");
    assert_eq!(first, second);
    assert_eq!(first, "DELETE FROM accounts WHERE id = 42");
}
