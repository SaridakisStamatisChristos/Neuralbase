// SPDX-License-Identifier: Apache-2.0
// Phase 8: window syntax that the executor does not model must fail closed.

use neuralbase::binder::{bind_statement, BindError, BoundPlan};
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::sql::parse_statement;

#[test]
fn explicit_window_frame_is_rejected_instead_of_ignored() {
    let statement = parse_statement(
        "SELECT ROW_NUMBER() OVER (ORDER BY 1 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn",
    )
    .expect("parse explicit frame");

    assert!(matches!(
        bind_statement(&statement, &InMemoryCatalog::default()),
        Err(BindError::Unsupported)
    ));
}

#[test]
fn named_window_is_rejected_instead_of_losing_definition() {
    let statement = parse_statement(
        "SELECT ROW_NUMBER() OVER w AS rn FROM lineitem WINDOW w AS (ORDER BY l_orderkey)",
    )
    .expect("parse named window");

    assert!(matches!(
        bind_statement(&statement, &InMemoryCatalog::with_tpch_lineitem()),
        Err(BindError::Unsupported)
    ));
}

#[test]
fn implemented_basic_window_shape_remains_bindable() {
    let statement = parse_statement(
        "SELECT l_orderkey, ROW_NUMBER() OVER (ORDER BY l_orderkey) AS rn FROM lineitem",
    )
    .expect("parse supported window");

    assert!(matches!(
        bind_statement(&statement, &InMemoryCatalog::with_tpch_lineitem()),
        Ok(BoundPlan::SelectQuery(_))
    ));
}
