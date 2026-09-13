// SPDX-License-Identifier: Apache-2.0
// Phase 8 semantic front-end for the row-oriented query executor.
//
// The established executor remains authoritative for table scans, joins,
// aggregation and windows. This front-end owns the no-FROM scalar cases that
// need precise NULL / three-valued logic and cast behavior.

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SelectItem,
    SetExpr, UnaryOperator, Value,
};

pub use crate::query_executor_legacy::{
    query_result_to_batch, QueryCatalog, QueryError, QueryResult, ScalarVal,
};

pub fn execute_select_query(
    query: &Query,
    catalog: &QueryCatalog,
) -> Result<QueryResult, QueryError> {
    if let Some(result) = execute_phase8_scalar(query)? {
        return Ok(result);
    }
    crate::query_executor_legacy::execute_select_query(query, catalog)
}

fn execute_phase8_scalar(query: &Query) -> Result<Option<QueryResult>, QueryError> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if !select.from.is_empty()
        || select.distinct.is_some()
        || select.having.is_some()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
    {
        return Ok(None);
    }

    let projection_requires = select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => required(expr),
        _ => false,
    });
    let selection_requires = select.selection.as_ref().is_some_and(required);
    if !projection_requires && !selection_requires {
        return Ok(None);
    }

    let columns: Vec<String> = select.projection.iter().map(column_name).collect();
    if let Some(selection) = &select.selection {
        if !matches!(truth(&eval(selection)?)?, Some(true)) {
            return Ok(Some(QueryResult {
                columns,
                rows: Vec::new(),
            }));
        }
    }

    let mut row = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return Ok(None),
        };
        row.push(eval(expr)?);
    }
    Ok(Some(QueryResult {
        columns,
        rows: vec![row],
    }))
}

fn column_name(item: &SelectItem) -> String {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::UnnamedExpr(Expr::Function(function)) => {
            function.name.to_string().to_lowercase()
        }
        SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.clone(),
        _ => "col".to_string(),
    }
}

fn required(expr: &Expr) -> bool {
    match expr {
        Expr::Value(Value::Null)
        | Expr::Cast { .. }
        | Expr::TypedString { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::InList { .. }
        | Expr::Between { .. } => true,
        Expr::BinaryOp { left, op, right } => {
            matches!(op, BinaryOperator::And | BinaryOperator::Or)
                || required(left)
                || required(right)
        }
        Expr::UnaryOp { op, expr } => matches!(op, UnaryOperator::Not) || required(expr),
        Expr::Nested(inner) => required(inner),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_some_and(required)
                || conditions.iter().any(required)
                || results.iter().any(required)
                || else_result.as_deref().is_some_and(required)
        }
        Expr::Function(function) => matches!(
            function.name.to_string().to_ascii_uppercase().as_str(),
            "COALESCE" | "NULLIF"
        ),
        _ => false,
    }
}

fn eval(expr: &Expr) -> Result<ScalarVal, QueryError> {
    match expr {
        Expr::Value(value) => literal(value),
        Expr::Nested(inner) => eval(inner),
        Expr::IsNull(inner) => Ok(ScalarVal::Bool(matches!(eval(inner)?, ScalarVal::Null))),
        Expr::IsNotNull(inner) => Ok(ScalarVal::Bool(!matches!(eval(inner)?, ScalarVal::Null))),
        Expr::UnaryOp { op, expr } => unary(op, eval(expr)?),
        Expr::BinaryOp { left, op, right } => binary(left, op, right),
        Expr::InList {
            expr,
            list,
            negated,
        } => in_list(expr, list, *negated),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval(expr)?;
            let ge = compare(&value, &eval(low)?, &BinaryOperator::GtEq)?;
            let le = compare(&value, &eval(high)?, &BinaryOperator::LtEq)?;
            let value = sql_and(&ge, &le)?;
            if *negated {
                sql_not(&value)
            } else {
                Ok(value)
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => eval_case(
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
        ),
        Expr::Function(function) => function_value(function),
        Expr::Cast {
            expr, data_type, ..
        } => cast(eval(expr)?, &data_type.to_string()),
        Expr::TypedString { data_type, value } => {
            if data_type.to_string().eq_ignore_ascii_case("DATE") {
                crate::binder::date_str_to_epoch_days(value)
                    .map(ScalarVal::Date)
                    .ok_or_else(|| QueryError::TypeError(format!("invalid DATE literal: {value}")))
            } else {
                Err(QueryError::Unsupported(format!(
                    "typed scalar literal: {data_type}"
                )))
            }
        }
        _ => Err(QueryError::Unsupported(format!(
            "scalar expression: {expr}"
        ))),
    }
}

fn literal(value: &Value) -> Result<ScalarVal, QueryError> {
    match value {
        Value::Number(raw, _) => raw
            .parse::<i64>()
            .map(ScalarVal::Int)
            .or_else(|_| raw.parse::<f64>().map(ScalarVal::Float))
            .map_err(|_| QueryError::TypeError(format!("invalid number: {raw}"))),
        Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => {
            Ok(ScalarVal::Text(value.clone()))
        }
        Value::Boolean(value) => Ok(ScalarVal::Bool(*value)),
        Value::Null => Ok(ScalarVal::Null),
        Value::Placeholder(_) => Err(QueryError::Unsupported("unbound parameter".into())),
        _ => Err(QueryError::Unsupported(format!("scalar value: {value}"))),
    }
}

fn unary(op: &UnaryOperator, value: ScalarVal) -> Result<ScalarVal, QueryError> {
    match op {
        UnaryOperator::Not => sql_not(&value),
        UnaryOperator::Plus => match value {
            ScalarVal::Int(_) | ScalarVal::Float(_) | ScalarVal::Null => Ok(value),
            _ => Err(QueryError::TypeError(
                "unary plus requires numeric input".into(),
            )),
        },
        UnaryOperator::Minus => match value {
            ScalarVal::Int(value) => value
                .checked_neg()
                .map(ScalarVal::Int)
                .ok_or_else(|| QueryError::TypeError("integer overflow".into())),
            ScalarVal::Float(value) => Ok(ScalarVal::Float(-value)),
            ScalarVal::Null => Ok(ScalarVal::Null),
            _ => Err(QueryError::TypeError(
                "unary minus requires numeric input".into(),
            )),
        },
        _ => Err(QueryError::Unsupported(format!("unary operator: {op}"))),
    }
}

fn binary(left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<ScalarVal, QueryError> {
    let left = eval(left)?;
    if matches!(op, BinaryOperator::And) && matches!(truth(&left)?, Some(false)) {
        return Ok(ScalarVal::Bool(false));
    }
    if matches!(op, BinaryOperator::Or) && matches!(truth(&left)?, Some(true)) {
        return Ok(ScalarVal::Bool(true));
    }
    let right = eval(right)?;

    match op {
        BinaryOperator::And => sql_and(&left, &right),
        BinaryOperator::Or => sql_or(&left, &right),
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Gt
        | BinaryOperator::Lt
        | BinaryOperator::GtEq
        | BinaryOperator::LtEq => compare(&left, &right, op),
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::Modulo => arithmetic(&left, op, &right),
        _ => Err(QueryError::Unsupported(format!("binary operator: {op}"))),
    }
}

fn truth(value: &ScalarVal) -> Result<Option<bool>, QueryError> {
    match value {
        ScalarVal::Bool(value) => Ok(Some(*value)),
        ScalarVal::Null => Ok(None),
        _ => Err(QueryError::TypeError("boolean expression required".into())),
    }
}

fn sql_and(left: &ScalarVal, right: &ScalarVal) -> Result<ScalarVal, QueryError> {
    Ok(match (truth(left)?, truth(right)?) {
        (Some(false), _) | (_, Some(false)) => ScalarVal::Bool(false),
        (Some(true), Some(true)) => ScalarVal::Bool(true),
        _ => ScalarVal::Null,
    })
}

fn sql_or(left: &ScalarVal, right: &ScalarVal) -> Result<ScalarVal, QueryError> {
    Ok(match (truth(left)?, truth(right)?) {
        (Some(true), _) | (_, Some(true)) => ScalarVal::Bool(true),
        (Some(false), Some(false)) => ScalarVal::Bool(false),
        _ => ScalarVal::Null,
    })
}

fn sql_not(value: &ScalarVal) -> Result<ScalarVal, QueryError> {
    Ok(match truth(value)? {
        Some(value) => ScalarVal::Bool(!value),
        None => ScalarVal::Null,
    })
}

fn compare(
    left: &ScalarVal,
    right: &ScalarVal,
    op: &BinaryOperator,
) -> Result<ScalarVal, QueryError> {
    if matches!(left, ScalarVal::Null) || matches!(right, ScalarVal::Null) {
        return Ok(ScalarVal::Null);
    }
    let ordering = match (left, right) {
        (ScalarVal::Int(a), ScalarVal::Int(b)) => a.cmp(b),
        (ScalarVal::Int(a), ScalarVal::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| QueryError::TypeError("unordered float".into()))?,
        (ScalarVal::Float(a), ScalarVal::Int(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| QueryError::TypeError("unordered float".into()))?,
        (ScalarVal::Float(a), ScalarVal::Float(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| QueryError::TypeError("unordered float".into()))?,
        (ScalarVal::Text(a), ScalarVal::Text(b)) => a.cmp(b),
        (ScalarVal::Date(a), ScalarVal::Date(b)) => a.cmp(b),
        (ScalarVal::Bool(a), ScalarVal::Bool(b)) => a.cmp(b),
        _ => return Err(QueryError::TypeError("incompatible comparison".into())),
    };
    let result = match op {
        BinaryOperator::Eq => ordering.is_eq(),
        BinaryOperator::NotEq => !ordering.is_eq(),
        BinaryOperator::Gt => ordering.is_gt(),
        BinaryOperator::Lt => ordering.is_lt(),
        BinaryOperator::GtEq => !ordering.is_lt(),
        BinaryOperator::LtEq => !ordering.is_gt(),
        _ => unreachable!(),
    };
    Ok(ScalarVal::Bool(result))
}

fn arithmetic(
    left: &ScalarVal,
    op: &BinaryOperator,
    right: &ScalarVal,
) -> Result<ScalarVal, QueryError> {
    if matches!(left, ScalarVal::Null) || matches!(right, ScalarVal::Null) {
        return Ok(ScalarVal::Null);
    }
    if let (ScalarVal::Date(a), ScalarVal::Date(b)) = (left, right) {
        if matches!(op, BinaryOperator::Minus) {
            return Ok(ScalarVal::Int((*a as i64) - (*b as i64)));
        }
    }
    match (left, right) {
        (ScalarVal::Int(a), ScalarVal::Int(b)) => match op {
            BinaryOperator::Plus => a.checked_add(*b).map(ScalarVal::Int),
            BinaryOperator::Minus => a.checked_sub(*b).map(ScalarVal::Int),
            BinaryOperator::Multiply => a.checked_mul(*b).map(ScalarVal::Int),
            BinaryOperator::Divide if *b != 0 => Some(ScalarVal::Int(a / b)),
            BinaryOperator::Modulo if *b != 0 => Some(ScalarVal::Int(a % b)),
            BinaryOperator::Divide | BinaryOperator::Modulo => {
                return Err(QueryError::DivisionByZero)
            }
            _ => unreachable!(),
        }
        .ok_or_else(|| QueryError::TypeError("integer overflow".into())),
        _ => {
            let a = as_f64(left)?;
            let b = as_f64(right)?;
            if matches!(op, BinaryOperator::Divide | BinaryOperator::Modulo) && b == 0.0 {
                return Err(QueryError::DivisionByZero);
            }
            Ok(ScalarVal::Float(match op {
                BinaryOperator::Plus => a + b,
                BinaryOperator::Minus => a - b,
                BinaryOperator::Multiply => a * b,
                BinaryOperator::Divide => a / b,
                BinaryOperator::Modulo => a % b,
                _ => unreachable!(),
            }))
        }
    }
}

fn as_f64(value: &ScalarVal) -> Result<f64, QueryError> {
    match value {
        ScalarVal::Int(value) => Ok(*value as f64),
        ScalarVal::Float(value) => Ok(*value),
        _ => Err(QueryError::TypeError("numeric input required".into())),
    }
}

fn in_list(expr: &Expr, list: &[Expr], negated: bool) -> Result<ScalarVal, QueryError> {
    let value = eval(expr)?;
    if matches!(value, ScalarVal::Null) {
        return Ok(ScalarVal::Null);
    }
    let mut saw_null = false;
    for candidate in list {
        let candidate = eval(candidate)?;
        if matches!(candidate, ScalarVal::Null) {
            saw_null = true;
        } else if matches!(
            compare(&value, &candidate, &BinaryOperator::Eq)?,
            ScalarVal::Bool(true)
        ) {
            return Ok(ScalarVal::Bool(!negated));
        }
    }
    if saw_null {
        Ok(ScalarVal::Null)
    } else {
        Ok(ScalarVal::Bool(negated))
    }
}

fn eval_case(
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
) -> Result<ScalarVal, QueryError> {
    for (condition, result) in conditions.iter().zip(results.iter()) {
        let matched = if let Some(operand) = operand {
            matches!(
                compare(&eval(operand)?, &eval(condition)?, &BinaryOperator::Eq)?,
                ScalarVal::Bool(true)
            )
        } else {
            matches!(truth(&eval(condition)?)?, Some(true))
        };
        if matched {
            return eval(result);
        }
    }
    else_result
        .map(eval)
        .transpose()
        .map(|v| v.unwrap_or(ScalarVal::Null))
}

fn function_value(function: &sqlparser::ast::Function) -> Result<ScalarVal, QueryError> {
    let name = function.name.to_string().to_ascii_uppercase();
    let FunctionArguments::List(args) = &function.args else {
        return Err(QueryError::Unsupported(format!("function args: {name}")));
    };
    let exprs: Result<Vec<&Expr>, QueryError> = args
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
            _ => Err(QueryError::Unsupported(format!("function arg: {name}"))),
        })
        .collect();
    let exprs = exprs?;
    match name.as_str() {
        "COALESCE" => {
            for expr in exprs {
                let value = eval(expr)?;
                if !matches!(value, ScalarVal::Null) {
                    return Ok(value);
                }
            }
            Ok(ScalarVal::Null)
        }
        "NULLIF" if exprs.len() == 2 => {
            let left = eval(exprs[0])?;
            let right = eval(exprs[1])?;
            if matches!(left, ScalarVal::Null) || matches!(right, ScalarVal::Null) {
                Ok(left)
            } else if matches!(
                compare(&left, &right, &BinaryOperator::Eq)?,
                ScalarVal::Bool(true)
            ) {
                Ok(ScalarVal::Null)
            } else {
                Ok(left)
            }
        }
        _ => Err(QueryError::Unsupported(format!("scalar function: {name}"))),
    }
}

fn cast(value: ScalarVal, target: &str) -> Result<ScalarVal, QueryError> {
    if matches!(value, ScalarVal::Null) {
        return Ok(ScalarVal::Null);
    }
    let target = target.to_ascii_uppercase();
    if target.starts_with("INT") || target.starts_with("INTEGER") || target.starts_with("BIGINT") {
        let value = match value {
            ScalarVal::Int(value) => value,
            ScalarVal::Float(value) if value.is_finite() => value.round() as i64,
            ScalarVal::Text(value) => value
                .trim()
                .parse::<i64>()
                .map_err(|_| QueryError::TypeError("invalid integer input".into()))?,
            _ => return Err(QueryError::TypeError("invalid integer cast".into())),
        };
        if (target.starts_with("INT") || target.starts_with("INTEGER"))
            && !target.starts_with("INT8")
            && i32::try_from(value).is_err()
        {
            return Err(QueryError::TypeError("INTEGER overflow".into()));
        }
        return Ok(ScalarVal::Int(value));
    }
    if target.starts_with("FLOAT") || target.starts_with("DOUBLE") || target.starts_with("REAL") {
        return match value {
            ScalarVal::Int(value) => Ok(ScalarVal::Float(value as f64)),
            ScalarVal::Float(value) => Ok(ScalarVal::Float(value)),
            ScalarVal::Text(value) => value
                .trim()
                .parse::<f64>()
                .map(ScalarVal::Float)
                .map_err(|_| QueryError::TypeError("invalid float input".into())),
            _ => Err(QueryError::TypeError("invalid float cast".into())),
        };
    }
    if target == "BOOLEAN" || target == "BOOL" {
        return match value {
            ScalarVal::Bool(value) => Ok(ScalarVal::Bool(value)),
            ScalarVal::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
                "true" | "t" | "yes" | "y" | "on" | "1" => Ok(ScalarVal::Bool(true)),
                "false" | "f" | "no" | "n" | "off" | "0" => Ok(ScalarVal::Bool(false)),
                _ => Err(QueryError::TypeError("invalid boolean input".into())),
            },
            _ => Err(QueryError::TypeError("invalid boolean cast".into())),
        };
    }
    if target == "DATE" {
        return match value {
            ScalarVal::Date(value) => Ok(ScalarVal::Date(value)),
            ScalarVal::Text(value) => crate::binder::date_str_to_epoch_days(&value)
                .map(ScalarVal::Date)
                .ok_or_else(|| QueryError::TypeError("invalid DATE input".into())),
            _ => Err(QueryError::TypeError("invalid DATE cast".into())),
        };
    }
    Err(QueryError::Unsupported(format!("scalar cast: {target}")))
}
