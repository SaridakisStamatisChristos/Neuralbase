// SPDX-License-Identifier: Apache-2.0
// Phase 8 safety wrapper around the historical binder implementation.
//
// The legacy binder intentionally remains the implementation source for bound
// plan/value types. This module adds fail-closed validation at SQL boundaries
// where the older binder could otherwise discard semantics before execution.

use crate::catalog::Catalog;
use crate::sql::NbStatement;
use sqlparser::ast::Statement;

pub use crate::binder_legacy::{
    coerce_expr_to_value, date_str_to_epoch_days, BindError, BoundPlan, CreateTablePlan,
    DeletePlan, DmlCmpOp, DmlPredicate, InsertPlan, SqlValue, UpdatePlan,
};

/// Bind a standard SQL statement while preserving the historical binder's
/// supported behavior. Phase 8 validates semantics that must never be silently
/// discarded before delegating to the established binder.
pub fn bind_statement(
    statement: &Statement,
    catalog: &dyn Catalog,
) -> Result<BoundPlan, BindError> {
    validate_statement_semantics(statement)?;
    let plan = crate::binder_legacy::bind_statement(statement, catalog)?;
    validate_persistent_dml_predicate(statement, &plan)?;
    Ok(plan)
}

/// Bind a NeuralBase-extended statement through the Phase-8 safety boundary.
pub fn bind_nb_statement(
    nb_stmt: &NbStatement,
    catalog: &dyn Catalog,
) -> Result<BoundPlan, BindError> {
    match nb_stmt {
        NbStatement::Sql(statement) => bind_statement(statement.as_ref(), catalog),
        _ => crate::binder_legacy::bind_nb_statement(nb_stmt, catalog),
    }
}

/// Reject parsed CREATE TABLE features whose semantics NeuralBase does not yet
/// enforce. Silently dropping a PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY or
/// DEFAULT declaration would create a durable schema different from the SQL the
/// client requested.
fn validate_statement_semantics(statement: &Statement) -> Result<(), BindError> {
    if let Statement::CreateTable {
        columns,
        constraints,
        ..
    } = statement
    {
        if columns.iter().any(|column| !column.options.is_empty()) || !constraints.is_empty() {
            return Err(BindError::Unsupported);
        }
    }
    Ok(())
}

/// The historical DML binder represents "no WHERE clause" and "WHERE clause
/// that it could not bind" as the same `None` predicate. The storage executor
/// correctly interprets `None` as a full-table mutation, so allowing an
/// unsupported WHERE expression to reach it would broaden the write.
///
/// Phase 8 therefore rejects exactly that ambiguous state. A statement without
/// a WHERE clause retains its existing full-table semantics; a WHERE clause is
/// accepted only when the legacy binder produced a concrete predicate.
fn validate_persistent_dml_predicate(
    statement: &Statement,
    plan: &BoundPlan,
) -> Result<(), BindError> {
    match (statement, plan) {
        (
            Statement::Update {
                selection: Some(_), ..
            },
            BoundPlan::Update(update),
        ) if update.predicate.is_none() => Err(BindError::Unsupported),
        (Statement::Delete(delete), BoundPlan::Delete(bound))
            if delete.selection.is_some() && bound.predicate.is_none() =>
        {
            Err(BindError::Unsupported)
        }
        _ => Ok(()),
    }
}
