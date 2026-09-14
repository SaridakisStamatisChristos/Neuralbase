// SPDX-License-Identifier: Apache-2.0
// Phase 8 safety wrapper around the historical binder implementation.
//
// The legacy binder intentionally remains the implementation source for bound
// plan/value types. This module adds fail-closed validation at SQL boundaries
// where the older binder could otherwise discard semantics before execution.

use crate::catalog::Catalog;
use crate::sql::NbStatement;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Query, SelectItem, SetExpr, Statement, WindowType,
};

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

    // The legacy single-table fast path reduces projection expressions to
    // column names/aliases. That is correct for its historical scalar subset,
    // but it would discard a validated window expression such as
    // ROW_NUMBER() OVER (...). Route window-bearing queries directly to the
    // general executor, which is the implementation that actually evaluates
    // the supported Phase-8 partition/order window subset.
    if let Statement::Query(query) = statement {
        if query_contains_window(query) {
            return Ok(BoundPlan::SelectQuery(query.clone()));
        }
    }

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

/// Reject parsed syntax whose semantics the established execution path would
/// otherwise silently discard.
fn validate_statement_semantics(statement: &Statement) -> Result<(), BindError> {
    match statement {
        Statement::CreateTable {
            columns,
            constraints,
            ..
        } => {
            if columns.iter().any(|column| !column.options.is_empty()) || !constraints.is_empty() {
                return Err(BindError::Unsupported);
            }
        }
        Statement::Query(query) => validate_query_semantics(query)?,
        Statement::Explain { statement, .. } => validate_statement_semantics(statement)?,
        _ => {}
    }
    Ok(())
}

/// Phase 8 supports the established ROW_NUMBER/RANK/LAG/LEAD partition/order
/// subset, but not named-window inheritance or explicit frame clauses. The
/// legacy executor ignores those fields; accepting them would therefore return
/// a result for semantics the engine did not execute.
fn validate_query_semantics(query: &Query) -> Result<(), BindError> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            validate_query_semantics(&cte.query)?;
        }
    }
    validate_set_expr(&query.body)
}

fn validate_set_expr(set_expr: &SetExpr) -> Result<(), BindError> {
    match set_expr {
        SetExpr::Select(select) => {
            if !select.named_window.is_empty() {
                return Err(BindError::Unsupported);
            }
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        validate_expr_windows(expr)?;
                    }
                    _ => {}
                }
            }
            if let Some(selection) = &select.selection {
                validate_expr_windows(selection)?;
            }
            if let Some(having) = &select.having {
                validate_expr_windows(having)?;
            }
        }
        SetExpr::Query(query) => validate_query_semantics(query)?,
        SetExpr::SetOperation { left, right, .. } => {
            validate_set_expr(left)?;
            validate_set_expr(right)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_expr_windows(expr: &Expr) -> Result<(), BindError> {
    match expr {
        Expr::Function(function) => {
            if let Some(over) = &function.over {
                match over {
                    WindowType::NamedWindow(_) => return Err(BindError::Unsupported),
                    WindowType::WindowSpec(spec)
                        if spec.window_name.is_some() || spec.window_frame.is_some() =>
                    {
                        return Err(BindError::Unsupported);
                    }
                    WindowType::WindowSpec(_) => {}
                }
            }
            if let sqlparser::ast::FunctionArguments::List(args) = &function.args {
                for arg in &args.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        } => validate_expr_windows(expr)?,
                        _ => {}
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr_windows(left)?;
            validate_expr_windows(right)?;
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => {
            validate_expr_windows(expr)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr_windows(expr)?;
            validate_expr_windows(low)?;
            validate_expr_windows(high)?;
        }
        Expr::InList { expr, list, .. } => {
            validate_expr_windows(expr)?;
            for item in list {
                validate_expr_windows(item)?;
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(operand) = operand {
                validate_expr_windows(operand)?;
            }
            for condition in conditions {
                validate_expr_windows(condition)?;
            }
            for result in results {
                validate_expr_windows(result)?;
            }
            if let Some(result) = else_result {
                validate_expr_windows(result)?;
            }
        }
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        }
        | Expr::InSubquery {
            subquery: query, ..
        } => validate_query_semantics(query)?,
        _ => {}
    }
    Ok(())
}

fn query_contains_window(query: &Query) -> bool {
    query
        .with
        .as_ref()
        .is_some_and(|with| with.cte_tables.iter().any(|cte| query_contains_window(&cte.query)))
        || set_expr_contains_window(&query.body)
}

fn set_expr_contains_window(set_expr: &SetExpr) -> bool {
    match set_expr {
        SetExpr::Select(select) => {
            select.projection.iter().any(|item| match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    expr_contains_window(expr)
                }
                _ => false,
            }) || select
                .selection
                .as_ref()
                .is_some_and(expr_contains_window)
                || select.having.as_ref().is_some_and(expr_contains_window)
        }
        SetExpr::Query(query) => query_contains_window(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_contains_window(left) || set_expr_contains_window(right)
        }
        _ => false,
    }
}

fn expr_contains_window(expr: &Expr) -> bool {
    match expr {
        Expr::Function(function) => {
            function.over.is_some()
                || match &function.args {
                    sqlparser::ast::FunctionArguments::List(args) => {
                        args.args.iter().any(|arg| match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                            | FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(expr),
                                ..
                            } => expr_contains_window(expr),
                            _ => false,
                        })
                    }
                    _ => false,
                }
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_window(left) || expr_contains_window(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => expr_contains_window(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_window(expr)
                || expr_contains_window(low)
                || expr_contains_window(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_window(expr) || list.iter().any(expr_contains_window)
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_ref().is_some_and(|expr| expr_contains_window(expr))
                || conditions.iter().any(expr_contains_window)
                || results.iter().any(expr_contains_window)
                || else_result
                    .as_ref()
                    .is_some_and(|expr| expr_contains_window(expr))
        }
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        }
        | Expr::InSubquery {
            subquery: query, ..
        } => query_contains_window(query),
        _ => false,
    }
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
