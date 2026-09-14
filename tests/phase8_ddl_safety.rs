// SPDX-License-Identifier: Apache-2.0
// Phase 8: DDL semantic safety regressions.

use neuralbase::binder::{bind_statement, BindError, BoundPlan};
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::sql::parse_statement;

fn assert_unsupported(sql: &str) {
    let statement = parse_statement(sql).unwrap_or_else(|error| panic!("parse {sql}: {error}"));
    assert!(
        matches!(
            bind_statement(&statement, &InMemoryCatalog::default()),
            Err(BindError::Unsupported)
        ),
        "DDL with unenforced semantics must fail closed: {sql}"
    );
}

#[test]
fn primary_key_is_rejected_until_enforcement_exists() {
    assert_unsupported("CREATE TABLE t (id INT PRIMARY KEY)");
}

#[test]
fn unique_is_rejected_until_enforcement_exists() {
    assert_unsupported("CREATE TABLE t (id INT UNIQUE)");
}

#[test]
fn default_is_rejected_until_default_semantics_exist() {
    assert_unsupported("CREATE TABLE t (id INT DEFAULT 7)");
}

#[test]
fn check_is_rejected_until_enforcement_exists() {
    assert_unsupported("CREATE TABLE t (id INT CHECK (id > 0))");
}

#[test]
fn table_level_constraint_is_rejected_until_enforcement_exists() {
    assert_unsupported("CREATE TABLE t (id INT, CONSTRAINT t_pk PRIMARY KEY (id))");
}

#[test]
fn plain_create_table_remains_supported() {
    let statement = parse_statement("CREATE TABLE t (id INT, name TEXT)").expect("parse");
    assert!(matches!(
        bind_statement(&statement, &InMemoryCatalog::default()),
        Ok(BoundPlan::CreateTable(_))
    ));
}
