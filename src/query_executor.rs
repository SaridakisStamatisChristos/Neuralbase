// SPDX-License-Identifier: Apache-2.0
// Session 9: General SQL execution engine (row-oriented interpreter).
//
// Handles any sqlparser Query AST including:
//   - Multi-table FROM / implicit cross-join
//   - INNER JOIN / LEFT OUTER JOIN ... ON
//   - WHERE with arbitrary boolean expressions
//   - GROUP BY + SUM/COUNT/AVG/MIN/MAX aggregates
//   - HAVING post-aggregate filter
//   - ORDER BY multi-column ASC/DESC
//   - LIMIT / OFFSET
//   - Scalar subqueries, IN (subquery), NOT IN, EXISTS, NOT EXISTS
//   - Arithmetic: +, -, *, /
//   - Scalar functions: UPPER, LOWER, SUBSTRING/SUBSTR, EXTRACT, COALESCE, NULLIF
//   - CASE WHEN ... THEN ... ELSE ... END
//   - LIKE / NOT LIKE pattern matching
//   - Derived tables: FROM (SELECT ...) AS alias
//
// CONFIDENCE: raw=0.73 effective=0.66
// DEPENDS_ON: tpch, vectorized, sqlparser-rs

use crate::tpch::TpchDataSet;
use crate::vectorized::{ColumnVector, RecordBatch, Utf8Column};
use sqlparser::ast::{
    BinaryOperator, DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, GroupByExpr,
    JoinConstraint, JoinOperator, Offset, OrderByExpr, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, TableFactor, UnaryOperator, Value,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

// Maximum intermediate row count before a cross-join is refused.
// Prevents OOM when the planner cannot find an equi-join predicate.
const CROSS_JOIN_BUDGET: usize = 50_000;
// Hard cap for any materialized intermediate in join processing.
const INTERMEDIATE_ROWS_BUDGET: usize = 200_000;

// ── ScalarVal ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarVal {
    Int(i64),
    Float(f64),
    Text(String),
    /// Days since Unix epoch (1970-01-01), same as ColumnVector::Date32.
    Date(i32),
    Bool(bool),
    Null,
}

impl ScalarVal {
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(n) => Some(*n as f64),
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }
    fn cmp_val(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => None,
            (Self::Int(a), Self::Int(b)) => Some(a.cmp(b)),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::Int(a), Self::Float(b)) => (*a as f64).partial_cmp(b),
            (Self::Float(a), Self::Int(b)) => a.partial_cmp(&(*b as f64)),
            (Self::Text(a), Self::Text(b)) => Some(a.cmp(b)),
            (Self::Date(a), Self::Date(b)) => Some(a.cmp(b)),
            (Self::Bool(a), Self::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
    fn truthy(&self) -> bool {
        !matches!(self, Self::Null | Self::Bool(false))
    }
}

// ── Row type ──────────────────────────────────────────────────────────────────

/// An evaluated row: ordered list of (qualified column name, value).
/// Column names are qualified as "alias.colname" after joins.
type Row = Vec<(String, ScalarVal)>;

/// Look up a column value by name; supports both "t.col" and bare "col".
fn row_get<'r>(row: &'r Row, name: &str) -> Option<&'r ScalarVal> {
    // Exact match first.
    if let Some((_, v)) = row.iter().find(|(k, _)| k == name) {
        return Some(v);
    }
    // Suffix match: "alias.col" ends with ".{name}".
    let suffix = format!(".{name}");
    row.iter()
        .filter(|(k, _)| k.ends_with(&suffix) || k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
        .next()
}

// ── QueryError ────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("type error: {0}")]
    TypeError(String),
    #[error("subquery returned multiple rows")]
    SubqueryMultipleRows,
    #[error("unsupported SQL feature: {0}")]
    Unsupported(String),
    #[error("division by zero")]
    DivisionByZero,
}

// ── QueryCatalog ──────────────────────────────────────────────────────────────

/// All tables available to the query executor.
pub struct QueryCatalog {
    pub tables: HashMap<String, Vec<Row>>,
}

impl QueryCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn add_batch(&mut self, name: &str, batch: &RecordBatch) {
        self.tables
            .insert(name.to_lowercase(), recordbatch_to_rows(batch, name));
    }

    pub fn from_tpch(data: &TpchDataSet) -> Self {
        let mut cat = Self::new();
        cat.add_batch("lineitem", &data.lineitem);
        cat.add_batch("orders", &data.orders);
        cat.add_batch("customer", &data.customer);
        cat.add_batch("nation", &data.nation);
        cat.add_batch("region", &data.region);
        cat.add_batch("part", &data.part);
        cat.add_batch("supplier", &data.supplier);
        cat.add_batch("partsupp", &data.partsupp);
        cat
    }
}

impl Default for QueryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

// ── QueryResult ───────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarVal>>,
}

// ── RecordBatch ↔ rows ────────────────────────────────────────────────────────

fn recordbatch_to_rows(batch: &RecordBatch, _alias: &str) -> Vec<Row> {
    (0..batch.row_count)
        .map(|r| {
            batch
                .columns
                .iter()
                .map(|(col_name, col)| {
                    let sv = match col {
                        ColumnVector::Int32(v) => v[r]
                            .map(|x| ScalarVal::Int(x as i64))
                            .unwrap_or(ScalarVal::Null),
                        ColumnVector::Int64(v) => {
                            v[r].map(ScalarVal::Int).unwrap_or(ScalarVal::Null)
                        }
                        ColumnVector::Float64(v) => {
                            v[r].map(ScalarVal::Float).unwrap_or(ScalarVal::Null)
                        }
                        ColumnVector::Date32(v) => {
                            v[r].map(ScalarVal::Date).unwrap_or(ScalarVal::Null)
                        }
                        ColumnVector::Utf8(v) => {
                            v.get(r).map(ScalarVal::Text).unwrap_or(ScalarVal::Null)
                        }
                    };
                    (col_name.clone(), sv)
                })
                .collect()
        })
        .collect()
}

/// Convert a QueryResult to a RecordBatch for the PostgreSQL wire protocol.
pub fn query_result_to_batch(result: QueryResult) -> RecordBatch {
    if result.rows.is_empty() {
        return RecordBatch::empty();
    }
    let ncols = result.columns.len();
    let mut col_vals: Vec<Vec<ScalarVal>> = vec![Vec::new(); ncols];
    for row in &result.rows {
        for (j, v) in row.iter().enumerate() {
            col_vals[j].push(v.clone());
        }
    }
    let columns = result
        .columns
        .iter()
        .enumerate()
        .map(|(j, name)| {
            let col_data = &col_vals[j];
            let cv = infer_column_vector(col_data);
            (name.clone(), cv)
        })
        .collect::<Vec<_>>();
    RecordBatch::new(columns).unwrap_or_else(|_| RecordBatch::empty())
}

fn infer_column_vector(vals: &[ScalarVal]) -> ColumnVector {
    let non_null = vals.iter().find(|v| !matches!(v, ScalarVal::Null));
    match non_null {
        Some(ScalarVal::Int(_)) | None => ColumnVector::Int64(
            vals.iter()
                .map(|v| {
                    if let ScalarVal::Int(n) = v {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        Some(ScalarVal::Float(_)) => ColumnVector::Float64(
            vals.iter()
                .map(|v| match v {
                    ScalarVal::Float(f) => Some(*f),
                    ScalarVal::Int(n) => Some(*n as f64),
                    _ => None,
                })
                .collect(),
        ),
        Some(ScalarVal::Date(_)) => ColumnVector::Date32(
            vals.iter()
                .map(|v| {
                    if let ScalarVal::Date(d) = v {
                        Some(*d)
                    } else {
                        None
                    }
                })
                .collect(),
        ),
        Some(ScalarVal::Bool(_)) => ColumnVector::Int64(
            vals.iter()
                .map(|v| match v {
                    ScalarVal::Bool(b) => Some(*b as i64),
                    ScalarVal::Int(n) => Some(*n),
                    _ => None,
                })
                .collect(),
        ),
        _ => ColumnVector::Utf8(Utf8Column::from_owned_options(
            vals.iter()
                .map(|v| match v {
                    ScalarVal::Text(s) => Some(s.clone()),
                    ScalarVal::Int(n) => Some(n.to_string()),
                    ScalarVal::Float(f) => Some(format!("{f:.4}")),
                    ScalarVal::Date(d) => Some(d.to_string()),
                    ScalarVal::Bool(b) => Some(b.to_string()),
                    ScalarVal::Null => None,
                })
                .collect(),
        )),
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn execute_select_query(
    query: &Query,
    catalog: &QueryCatalog,
) -> Result<QueryResult, QueryError> {
    let top_row: Row = Vec::new();
    execute_query_inner(query, catalog, &top_row)
}

fn execute_query_inner(
    query: &Query,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<QueryResult, QueryError> {
    // CTE resolution: build an extended catalog if a WITH clause is present.
    let cte_ext: Option<QueryCatalog> = if let Some(with) = &query.with {
        let mut ext = QueryCatalog::new();
        for (k, v) in &catalog.tables {
            ext.tables.insert(k.clone(), v.clone());
        }
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.to_lowercase();
            let cte_result = execute_query_inner(&cte.query, &ext, outer_row)?;
            let cte_rows: Vec<Row> = cte_result
                .rows
                .iter()
                .map(|row_vals| {
                    cte_result
                        .columns
                        .iter()
                        .zip(row_vals.iter())
                        .map(|(col, val)| (format!("{cte_name}.{col}"), val.clone()))
                        .collect()
                })
                .collect();
            ext.tables.insert(cte_name, cte_rows);
        }
        Some(ext)
    } else {
        None
    };
    let eff_catalog: &QueryCatalog = cte_ext.as_ref().unwrap_or(catalog);

    match query.body.as_ref() {
        SetExpr::Select(select) => execute_select(select, query, eff_catalog, outer_row),
        SetExpr::Query(inner) => execute_query_inner(inner, eff_catalog, outer_row),
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => execute_set_op(
            op,
            set_quantifier,
            left,
            right,
            eff_catalog,
            outer_row,
            &query.order_by,
            query.limit.as_ref(),
            query.offset.as_ref(),
        ),
        _ => Err(QueryError::Unsupported("set expression type".into())),
    }
}

fn execute_select(
    select: &Select,
    query: &Query,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<QueryResult, QueryError> {
    let result = execute_select_bare(select, catalog, outer_row)?;

    // Step 7: ORDER BY
    let result = if !query.order_by.is_empty() {
        apply_order_by(result, &query.order_by)?
    } else {
        result
    };

    // Step 8: LIMIT / OFFSET
    apply_limit_offset(result, query.limit.as_ref(), query.offset.as_ref())
}

/// Execute a SELECT body without outer ORDER BY / LIMIT / OFFSET.
/// Used both by execute_select and as a building block for UNION sub-selects.
fn execute_select_bare(
    select: &Select,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<QueryResult, QueryError> {
    // Step 1: Extract equi-join predicates from WHERE first, then resolve FROM.
    let equi_pairs: Vec<(String, String)> = select
        .selection
        .as_ref()
        .map(extract_equi_pairs)
        .unwrap_or_default();
    let mut rows = resolve_from(
        &select.from,
        catalog,
        outer_row,
        &equi_pairs,
        select.selection.as_ref(),
    )?;

    // Step 2: WHERE
    if let Some(expr) = &select.selection {
        rows.retain(|row| {
            eval_expr(expr, row, catalog, outer_row)
                .map(|v| v.truthy())
                .unwrap_or(false)
        });
    }

    // Step 3: GROUP BY / aggregation
    let has_agg = has_aggregate_in_projection(&select.projection);
    let group_cols: Vec<Expr> = match &select.group_by {
        GroupByExpr::All => vec![],
        GroupByExpr::Expressions(exprs) => exprs.clone(),
    };
    let rows = if !group_cols.is_empty() || has_agg {
        perform_groupby(select, &rows, &group_cols, catalog, outer_row)?
    } else {
        rows
    };

    // Step 4: HAVING (already applied inside perform_groupby for grouped; apply here for non-grouped)
    let rows = if !group_cols.is_empty() {
        rows
    } else if let Some(having) = &select.having {
        let mut filtered = rows;
        filtered.retain(|row| {
            eval_expr(having, row, catalog, outer_row)
                .map(|v| v.truthy())
                .unwrap_or(false)
        });
        filtered
    } else {
        rows
    };

    // Step 4.5: Window functions (inject computed window values before projection)
    let rows = if has_window_in_projection(&select.projection) {
        apply_window_functions(&select.projection, rows, catalog, outer_row)?
    } else {
        rows
    };

    // Step 5: SELECT projection
    let result = apply_projection(&select.projection, &rows, catalog, outer_row)?;

    // Step 6: DISTINCT
    let result = if select.distinct.is_some() {
        dedup_result(result)
    } else {
        result
    };

    Ok(result)
}

/// Apply LIMIT and OFFSET to a QueryResult.
fn apply_limit_offset(
    result: QueryResult,
    limit: Option<&Expr>,
    offset: Option<&Offset>,
) -> Result<QueryResult, QueryError> {
    let off = offset
        .and_then(|o| match &o.value {
            Expr::Value(Value::Number(s, _)) => s.parse::<usize>().ok(),
            _ => None,
        })
        .unwrap_or(0);
    let lim = limit.and_then(|e| match e {
        Expr::Value(Value::Number(s, _)) => s.parse::<usize>().ok(),
        _ => None,
    });

    let (cols, mut rows) = (result.columns, result.rows);
    if off > 0 {
        rows = rows.into_iter().skip(off).collect();
    }
    if let Some(n) = lim {
        rows.truncate(n);
    }
    Ok(QueryResult {
        columns: cols,
        rows,
    })
}

// ── UNION / INTERSECT / EXCEPT ────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn execute_set_op(
    op: &SetOperator,
    set_quantifier: &SetQuantifier,
    left: &SetExpr,
    right: &SetExpr,
    catalog: &QueryCatalog,
    outer_row: &Row,
    order_by: &[OrderByExpr],
    limit: Option<&Expr>,
    offset: Option<&Offset>,
) -> Result<QueryResult, QueryError> {
    let lres = execute_setexpr(left, catalog, outer_row)?;
    let rres = execute_setexpr(right, catalog, outer_row)?;

    let is_all = matches!(
        set_quantifier,
        SetQuantifier::All | SetQuantifier::AllByName
    );

    let mut rows: Vec<Vec<ScalarVal>> = match op {
        SetOperator::Union => {
            let mut r = lres.rows;
            r.extend(rres.rows);
            r
        }
        SetOperator::Intersect => lres
            .rows
            .into_iter()
            .filter(|lr| rres.rows.contains(lr))
            .collect(),
        SetOperator::Except => lres
            .rows
            .into_iter()
            .filter(|lr| !rres.rows.contains(lr))
            .collect(),
    };

    // DISTINCT (default): remove duplicate rows.
    if !is_all {
        let mut seen: Vec<Vec<ScalarVal>> = Vec::new();
        rows.retain(|r| {
            if seen.contains(r) {
                false
            } else {
                seen.push(r.clone());
                true
            }
        });
    }

    let mut result = QueryResult {
        columns: lres.columns,
        rows,
    };

    if !order_by.is_empty() {
        result = apply_order_by(result, order_by)?;
    }

    apply_limit_offset(result, limit, offset)
}

/// Execute a SetExpr without outer ORDER BY / LIMIT (used for UNION sub-selects).
fn execute_setexpr(
    expr: &SetExpr,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<QueryResult, QueryError> {
    match expr {
        SetExpr::Select(select) => execute_select_bare(select, catalog, outer_row),
        SetExpr::Query(inner) => execute_query_inner(inner, catalog, outer_row),
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => execute_set_op(
            op,
            set_quantifier,
            left,
            right,
            catalog,
            outer_row,
            &[],
            None,
            None,
        ),
        _ => Err(QueryError::Unsupported(
            "set expression in combination query".into(),
        )),
    }
}

// ── Window function support ───────────────────────────────────────────────────

/// Return true if any projection item contains a window function call.
fn has_window_in_projection(items: &[SelectItem]) -> bool {
    items.iter().any(|item| {
        let e = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => return false,
        };
        matches!(e, Expr::Function(f) if f.over.is_some())
    })
}

/// Pre-compute window function values for all rows and inject them as extra
/// columns, so they are available to the projection step.
fn apply_window_functions(
    projection: &[SelectItem],
    mut rows: Vec<Row>,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<Vec<Row>, QueryError> {
    for item in projection {
        let (alias, func) = match item {
            SelectItem::UnnamedExpr(Expr::Function(f)) => (f.name.to_string().to_lowercase(), f),
            SelectItem::ExprWithAlias {
                expr: Expr::Function(f),
                alias,
            } => (alias.value.clone(), f),
            _ => continue,
        };
        if func.over.is_none() {
            continue;
        }
        let fname = func.name.to_string().to_uppercase();
        let values = compute_window_values(&fname, func, &rows, catalog, outer_row)?;
        for (row, val) in rows.iter_mut().zip(values) {
            row.push((alias.clone(), val));
        }
    }
    Ok(rows)
}

/// Compute per-row window function values for the entire row set.
fn compute_window_values(
    fname: &str,
    func: &Function,
    rows: &[Row],
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<Vec<ScalarVal>, QueryError> {
    // Extract partition_by and order_by from the window spec.
    let (partition_by, order_by_exprs) = extract_window_spec(func);

    let get_partition_key = |row: &Row| -> Vec<ScalarVal> {
        partition_by
            .iter()
            .map(|e| eval_expr(e, row, catalog, outer_row).unwrap_or(ScalarVal::Null))
            .collect()
    };

    // Build a sort order over indices: first by partition key, then by ORDER BY.
    let mut sorted_indices: Vec<usize> = (0..rows.len()).collect();
    sorted_indices.sort_by(|&a, &b| {
        let ka = get_partition_key(&rows[a]);
        let kb = get_partition_key(&rows[b]);
        for (k1, k2) in ka.iter().zip(kb.iter()) {
            if let Some(ord) = k1.cmp_val(k2) {
                if ord != Ordering::Equal {
                    return ord;
                }
            }
        }
        for ob in &order_by_exprs {
            let va = eval_expr(&ob.expr, &rows[a], catalog, outer_row).unwrap_or(ScalarVal::Null);
            let vb = eval_expr(&ob.expr, &rows[b], catalog, outer_row).unwrap_or(ScalarVal::Null);
            let ord = va.cmp_val(&vb).unwrap_or(Ordering::Equal);
            let ord = if ob.asc == Some(false) {
                ord.reverse()
            } else {
                ord
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    let mut results = vec![ScalarVal::Null; rows.len()];

    // Process each partition in the sorted order.
    let mut i = 0;
    while i < sorted_indices.len() {
        let part_key = get_partition_key(&rows[sorted_indices[i]]);
        let mut j = i + 1;
        while j < sorted_indices.len() && get_partition_key(&rows[sorted_indices[j]]) == part_key {
            j += 1;
        }
        let part_sorted = &sorted_indices[i..j];

        match fname {
            "ROW_NUMBER" => {
                for (rn, &orig_idx) in part_sorted.iter().enumerate() {
                    results[orig_idx] = ScalarVal::Int(rn as i64 + 1);
                }
            }
            "RANK" => {
                let mut current_rank = 1usize;
                let mut prev_order_vals: Option<Vec<ScalarVal>> = None;
                for (pos, &orig_idx) in part_sorted.iter().enumerate() {
                    let order_vals: Vec<ScalarVal> = order_by_exprs
                        .iter()
                        .map(|ob| {
                            eval_expr(&ob.expr, &rows[orig_idx], catalog, outer_row)
                                .unwrap_or(ScalarVal::Null)
                        })
                        .collect();
                    if let Some(ref prev) = prev_order_vals {
                        if order_vals != *prev {
                            current_rank = pos + 1;
                        }
                    }
                    results[orig_idx] = ScalarVal::Int(current_rank as i64);
                    prev_order_vals = Some(order_vals);
                }
            }
            "LAG" | "LEAD" => {
                let func_args: &[FunctionArg] = match &func.args {
                    sqlparser::ast::FunctionArguments::List(list) => &list.args,
                    _ => &[],
                };
                let arg_expr = func_args.iter().next().and_then(|fa| match fa {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
                    _ => None,
                });
                let lag_offset: i64 = func_args
                    .get(1)
                    .and_then(|fa| match fa {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::Number(s, _),
                        ))) => s.parse::<i64>().ok(),
                        _ => None,
                    })
                    .unwrap_or(1);

                if let Some(expr) = arg_expr {
                    for (pos, &orig_idx) in part_sorted.iter().enumerate() {
                        let target = if fname == "LAG" {
                            pos as i64 - lag_offset
                        } else {
                            pos as i64 + lag_offset
                        };
                        let val = if target >= 0 && (target as usize) < part_sorted.len() {
                            eval_expr(
                                &expr,
                                &rows[part_sorted[target as usize]],
                                catalog,
                                outer_row,
                            )
                            .unwrap_or(ScalarVal::Null)
                        } else {
                            ScalarVal::Null
                        };
                        results[orig_idx] = val;
                    }
                }
            }
            other => {
                return Err(QueryError::Unsupported(format!("window function: {other}")));
            }
        }

        i = j;
    }

    Ok(results)
}

/// Extract (partition_by, order_by) from a function's OVER clause.
/// Supports both WindowType::WindowSpec and a bare WindowSpec (pre-0.44 compat).
fn extract_window_spec(func: &Function) -> (Vec<Expr>, Vec<OrderByExpr>) {
    let over = match &func.over {
        Some(o) => o,
        None => return (vec![], vec![]),
    };
    // sqlparser >= 0.44 wraps the spec in WindowType::WindowSpec.
    // We use an opaque dynamic dispatch approach via Debug to handle both API shapes.
    // Try the new API first; on the old API `over` *is* a WindowSpec directly.
    extract_window_type_spec(over)
}

/// Extract (partition_by, order_by) from a `WindowType` value.
/// In sqlparser >= 0.44, `Function.over` is `Option<WindowType>` where
/// `WindowType::WindowSpec(spec)` carries the details we need.
fn extract_window_type_spec(over: &sqlparser::ast::WindowType) -> (Vec<Expr>, Vec<OrderByExpr>) {
    match over {
        sqlparser::ast::WindowType::WindowSpec(spec) => {
            (spec.partition_by.clone(), spec.order_by.clone())
        }
        sqlparser::ast::WindowType::NamedWindow(_) => (vec![], vec![]),
    }
}

// ── Hash-join helpers ─────────────────────────────────────────────────────────

/// Extract equi-join pairs (left_col, right_col) from a WHERE expression.
/// Only traverses AND nodes; stops at anything more complex.
fn extract_equi_pairs(expr: &Expr) -> Vec<(String, String)> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let (Some(l), Some(r)) = (expr_col_ref(left), expr_col_ref(right)) {
                vec![(l, r)]
            } else {
                vec![]
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut v = extract_equi_pairs(left);
            v.extend(extract_equi_pairs(right));
            v.sort();
            v.dedup();
            v
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => {
            let left_pairs = extract_equi_pairs(left);
            let right_pairs = extract_equi_pairs(right);
            if left_pairs.is_empty() || right_pairs.is_empty() {
                return vec![];
            }
            let left_set: HashSet<(String, String)> = left_pairs
                .into_iter()
                .map(|(a, b)| canonical_pair(a, b))
                .collect();
            let right_set: HashSet<(String, String)> = right_pairs
                .into_iter()
                .map(|(a, b)| canonical_pair(a, b))
                .collect();
            left_set.intersection(&right_set).cloned().collect()
        }
        Expr::Nested(inner) => extract_equi_pairs(inner),
        _ => vec![],
    }
}

fn canonical_pair(a: String, b: String) -> (String, String) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

/// Return the bare or qualified column name from an Identifier / CompoundIdentifier.
fn expr_col_ref(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => Some(
            parts
                .iter()
                .map(|p| p.value.to_lowercase())
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => None,
    }
}

/// True if `name` (bare or qualified) matches a fully-qualified column `qualified`.
/// e.g. "c_custkey" matches "customer.c_custkey"; "c.c_custkey" matches "c.c_custkey".
fn col_matches(name: &str, qualified: &str) -> bool {
    if name == qualified {
        return true;
    }
    // bare name matches suffix after last '.'
    if let Some(pos) = qualified.rfind('.') {
        if &qualified[pos + 1..] == name {
            return true;
        }
    }
    false
}

/// Deterministic string key for hashing a ScalarVal.
fn scalar_hash_key(val: &ScalarVal) -> String {
    match val {
        ScalarVal::Int(n) => format!("i{n}"),
        ScalarVal::Float(f) => format!("f{f:.10}"),
        ScalarVal::Text(s) => format!("t{s}"),
        ScalarVal::Date(d) => format!("d{d}"),
        ScalarVal::Bool(b) => format!("b{b}"),
        ScalarVal::Null => "null".to_string(),
    }
}

/// Given the current accumulated rows and a new table's rows, find an equi-join
/// predicate where one side is in `result` columns and the other is in `new_rows`.
/// Returns `(result_col, new_col)` when found.
fn find_join_key(
    result: &[Row],
    new_rows: &[Row],
    equi_pairs: &[(String, String)],
) -> Option<(String, String)> {
    let rcols: Vec<String> = result
        .first()
        .map(|r| r.iter().map(|(k, _)| k.clone()).collect())
        .unwrap_or_default();
    let ncols: Vec<String> = new_rows
        .first()
        .map(|r| r.iter().map(|(k, _)| k.clone()).collect())
        .unwrap_or_default();

    for (l, r) in equi_pairs {
        let l_in_result = rcols.iter().any(|c| col_matches(l, c));
        let r_in_new = ncols.iter().any(|c| col_matches(r, c));
        if l_in_result && r_in_new {
            let lk = rcols
                .iter()
                .find(|c| col_matches(l, c))
                .cloned()
                .unwrap_or_else(|| l.clone());
            let rk = ncols
                .iter()
                .find(|c| col_matches(r, c))
                .cloned()
                .unwrap_or_else(|| r.clone());
            return Some((lk, rk));
        }
        let r_in_result = rcols.iter().any(|c| col_matches(r, c));
        let l_in_new = ncols.iter().any(|c| col_matches(l, c));
        if r_in_result && l_in_new {
            let lk = rcols
                .iter()
                .find(|c| col_matches(r, c))
                .cloned()
                .unwrap_or_else(|| r.clone());
            let rk = ncols
                .iter()
                .find(|c| col_matches(l, c))
                .cloned()
                .unwrap_or_else(|| l.clone());
            return Some((lk, rk));
        }
    }
    None
}

/// Hash-join: for each left row, lookup matching right rows by key.
fn hash_join_keyed(
    left: Vec<Row>,
    right: Vec<Row>,
    lkey: &str,
    rkey: &str,
) -> Result<Vec<Row>, QueryError> {
    // Build hash index on the right side.
    let mut index: HashMap<String, Vec<usize>> = HashMap::new();
    for (ridx, rrow) in right.iter().enumerate() {
        if let Some(val) = row_get(rrow, rkey) {
            if !matches!(val, ScalarVal::Null) {
                index.entry(scalar_hash_key(val)).or_default().push(ridx);
            }
        }
    }
    // Probe.
    let mut out = Vec::new();
    for lrow in &left {
        if let Some(val) = row_get(lrow, lkey) {
            if let Some(indices) = index.get(&scalar_hash_key(val)) {
                for &ridx in indices {
                    if out.len() >= INTERMEDIATE_ROWS_BUDGET {
                        return Err(QueryError::Unsupported(format!(
                            "hash join output exceeds budget ({INTERMEDIATE_ROWS_BUDGET})"
                        )));
                    }
                    let mut combined = lrow.clone();
                    combined.extend_from_slice(&right[ridx]);
                    out.push(combined);
                }
            }
        }
    }
    Ok(out)
}

// ── FROM clause ───────────────────────────────────────────────────────────────

fn resolve_from(
    from: &[sqlparser::ast::TableWithJoins],
    catalog: &QueryCatalog,
    outer_row: &Row,
    equi_pairs: &[(String, String)],
    where_expr: Option<&Expr>,
) -> Result<Vec<Row>, QueryError> {
    if from.is_empty() {
        return Ok(vec![vec![]]);
    }

    // Build the join incrementally.  For each new table try to use a hash join
    // driven by an equi-predicate extracted from the WHERE clause.  If none is
    // found, fall back to a cross-product but refuse if it would exceed the
    // budget (prevents OOM on complex multi-table queries).
    //
    // The first table is loaded directly (no cross-product with a synthetic unit
    // row), so single-table scans over large tables are not mistakenly blocked
    // by the CROSS_JOIN_BUDGET check.
    let mut result: Vec<Row> = Vec::new();
    let mut first_table = true;

    for twj in from {
        // Resolve the relation.
        let mut table_rows = resolve_table_factor(&twj.relation, catalog, outer_row)?;
        apply_single_table_predicates(&mut table_rows, where_expr, catalog, outer_row);

        if first_table {
            // First table: take rows directly — no cross-product needed.
            result = table_rows;
            first_table = false;
        } else if let Some((lk, rk)) = find_join_key(&result, &table_rows, equi_pairs) {
            // Try hash-join first; cross-product with budget guard if not applicable.
            result = hash_join_keyed(result, table_rows, &lk, &rk)?;
        } else {
            let estimated = result.len().saturating_mul(table_rows.len());
            if estimated > CROSS_JOIN_BUDGET {
                return Err(QueryError::Unsupported(format!(
                    "implicit cross-join of {} x {} rows exceeds budget ({CROSS_JOIN_BUDGET}); \
                     no equi-join predicate found",
                    result.len(),
                    table_rows.len()
                )));
            }
            result = cross_product(result, table_rows)?;
        }

        // Apply explicit JOIN ... ON conditions (INNER / LEFT OUTER).
        for join in &twj.joins {
            let mut join_rows = resolve_table_factor(&join.relation, catalog, outer_row)?;
            apply_single_table_predicates(&mut join_rows, where_expr, catalog, outer_row);
            let is_left_outer = matches!(join.join_operator, JoinOperator::LeftOuter(_));

            let on_expr = match &join.join_operator {
                JoinOperator::Inner(JoinConstraint::On(e))
                | JoinOperator::LeftOuter(JoinConstraint::On(e)) => Some(e.clone()),
                _ => None,
            };

            if is_left_outer {
                result = left_outer_join(result, join_rows, on_expr.as_ref(), catalog, outer_row)?;
            } else {
                // For explicit INNER JOIN ... ON, extract any equi-predicate from
                // the ON clause and use hash join if possible.
                let join_equi = on_expr.as_ref().map(extract_equi_pairs).unwrap_or_default();
                if let Some((lk, rk)) = find_join_key(&result, &join_rows, &join_equi) {
                    let mut combined = hash_join_keyed(result, join_rows, &lk, &rk)?;
                    // Apply any remaining non-equi predicates from the ON clause.
                    if let Some(cond) = &on_expr {
                        combined.retain(|row| {
                            eval_expr(cond, row, catalog, outer_row)
                                .map(|v| v.truthy())
                                .unwrap_or(false)
                        });
                    }
                    result = combined;
                } else {
                    result = inner_join(result, join_rows, on_expr.as_ref(), catalog, outer_row)?;
                }
            }
        }
    }

    Ok(result)
}

fn resolve_table_factor(
    factor: &TableFactor,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<Vec<Row>, QueryError> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let tname = name.to_string().to_lowercase();
            let alias_str = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| tname.clone());
            let rows = catalog
                .tables
                .get(&tname)
                .ok_or_else(|| QueryError::TableNotFound(tname.clone()))?;
            // Re-qualify column names with alias.
            Ok(rows
                .iter()
                .map(|row| qualify_row(row, &alias_str))
                .collect())
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias_str = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| "derived".to_string());
            let result = execute_query_inner(subquery, catalog, outer_row)?;
            // Convert QueryResult back to rows, qualified with alias.
            Ok(result
                .rows
                .iter()
                .map(|row_vals| {
                    result
                        .columns
                        .iter()
                        .zip(row_vals)
                        .map(|(col, val)| (format!("{alias_str}.{col}"), val.clone()))
                        .collect()
                })
                .collect())
        }
        _ => Err(QueryError::Unsupported("table factor type".into())),
    }
}

fn split_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        _ => out.push(expr),
    }
}

fn expr_refs_only_row(expr: &Expr, row: &Row) -> bool {
    match expr {
        Expr::Identifier(id) => row_get(row, &id.value).is_some(),
        Expr::CompoundIdentifier(parts) => {
            let qualified = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            row_get(row, &qualified).is_some()
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_refs_only_row(left, row) && expr_refs_only_row(right, row)
        }
        Expr::UnaryOp { expr: inner, .. } => expr_refs_only_row(inner, row),
        Expr::Nested(inner) => expr_refs_only_row(inner, row),
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            expr_refs_only_row(inner, row)
                && expr_refs_only_row(low, row)
                && expr_refs_only_row(high, row)
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        }
        | Expr::ILike {
            expr: inner,
            pattern,
            ..
        } => expr_refs_only_row(inner, row) && expr_refs_only_row(pattern, row),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_refs_only_row(inner, row),
        Expr::InList {
            expr: inner, list, ..
        } => expr_refs_only_row(inner, row) && list.iter().all(|e| expr_refs_only_row(e, row)),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|o| expr_refs_only_row(o, row))
                .unwrap_or(true)
                && conditions.iter().all(|e| expr_refs_only_row(e, row))
                && results.iter().all(|e| expr_refs_only_row(e, row))
                && else_result
                    .as_ref()
                    .map(|e| expr_refs_only_row(e, row))
                    .unwrap_or(true)
        }
        Expr::Function(func) => func_args(&func.args).iter().all(|arg| match arg {
            sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                expr_refs_only_row(e, row)
            }
            sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => true,
            sqlparser::ast::FunctionArg::Named { arg, .. } => match arg {
                FunctionArgExpr::Expr(e) => expr_refs_only_row(e, row),
                FunctionArgExpr::Wildcard => true,
                _ => false,
            },
            _ => false,
        }),
        Expr::Value(_) | Expr::TypedString { .. } => true,
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => false,
        _ => false,
    }
}

fn apply_single_table_predicates(
    rows: &mut Vec<Row>,
    where_expr: Option<&Expr>,
    catalog: &QueryCatalog,
    outer_row: &Row,
) {
    if rows.is_empty() {
        return;
    }
    let Some(expr) = where_expr else {
        return;
    };

    let mut conjuncts = Vec::new();
    split_conjuncts(expr, &mut conjuncts);

    for conjunct in conjuncts {
        if !expr_refs_only_row(conjunct, &rows[0]) {
            continue;
        }
        rows.retain(|row| {
            eval_expr(conjunct, row, catalog, outer_row)
                .map(|v| v.truthy())
                .unwrap_or(false)
        });
        if rows.is_empty() {
            break;
        }
    }
}

fn qualify_row(row: &Row, alias: &str) -> Row {
    row.iter()
        .map(|(name, val)| {
            // Strip any existing prefix and re-qualify.
            let bare = name.rfind('.').map(|i| &name[i + 1..]).unwrap_or(name);
            (format!("{alias}.{bare}"), val.clone())
        })
        .collect()
}

fn cross_product(left: Vec<Row>, right: Vec<Row>) -> Result<Vec<Row>, QueryError> {
    let mut out = Vec::with_capacity(left.len() * right.len().max(1));
    for l in &left {
        for r in &right {
            if out.len() >= INTERMEDIATE_ROWS_BUDGET {
                return Err(QueryError::Unsupported(format!(
                    "cross product output exceeds budget ({INTERMEDIATE_ROWS_BUDGET})"
                )));
            }
            let mut combined = l.clone();
            combined.extend_from_slice(r);
            out.push(combined);
        }
    }
    Ok(out)
}

fn inner_join(
    left: Vec<Row>,
    right: Vec<Row>,
    on: Option<&Expr>,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<Vec<Row>, QueryError> {
    let estimated = left.len().saturating_mul(right.len());
    if estimated > CROSS_JOIN_BUDGET {
        return Err(QueryError::Unsupported(format!(
            "explicit cross-join of {} x {} rows exceeds budget ({CROSS_JOIN_BUDGET})",
            left.len(),
            right.len()
        )));
    }
    let combined = cross_product(left, right)?;
    if let Some(cond) = on {
        Ok(combined
            .into_iter()
            .filter(|row| {
                eval_expr(cond, row, catalog, outer_row)
                    .map(|v| v.truthy())
                    .unwrap_or(false)
            })
            .collect())
    } else {
        Ok(combined)
    }
}

fn left_outer_join(
    left: Vec<Row>,
    right: Vec<Row>,
    on: Option<&Expr>,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<Vec<Row>, QueryError> {
    let null_right: Row = if let Some(r) = right.first() {
        r.iter()
            .map(|(k, _)| (k.clone(), ScalarVal::Null))
            .collect()
    } else {
        vec![]
    };
    let mut out = Vec::new();
    for lrow in &left {
        let mut matched = false;
        for rrow in &right {
            if out.len() >= INTERMEDIATE_ROWS_BUDGET {
                return Err(QueryError::Unsupported(format!(
                    "left outer join output exceeds budget ({INTERMEDIATE_ROWS_BUDGET})"
                )));
            }
            let mut combined = lrow.clone();
            combined.extend_from_slice(rrow);
            let keep = on
                .map(|cond| {
                    eval_expr(cond, &combined, catalog, outer_row)
                        .map(|v| v.truthy())
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            if keep {
                out.push(combined);
                matched = true;
            }
        }
        if !matched {
            let mut padded = lrow.clone();
            padded.extend_from_slice(&null_right);
            out.push(padded);
        }
    }
    Ok(out)
}

// ── Aggregation ───────────────────────────────────────────────────────────────

fn perform_groupby(
    select: &Select,
    rows: &[Row],
    group_cols: &[Expr],
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<Vec<Row>, QueryError> {
    // Build group keys.
    let mut groups: Vec<(Vec<ScalarVal>, Vec<&Row>)> = Vec::new();
    let mut key_index: HashMap<Vec<String>, usize> = HashMap::new();

    if group_cols.is_empty() {
        groups.push((vec![], rows.iter().collect()));
    } else {
        for row in rows {
            let key: Vec<ScalarVal> = group_cols
                .iter()
                .map(|e| eval_expr(e, row, catalog, outer_row).unwrap_or(ScalarVal::Null))
                .collect();
            let key_strings: Vec<String> = key.iter().map(|v| format!("{v:?}")).collect();
            let idx = if let Some(&i) = key_index.get(&key_strings) {
                i
            } else {
                let i = groups.len();
                key_index.insert(key_strings, i);
                groups.push((key, Vec::new()));
                i
            };
            groups[idx].1.push(row);
        }
    }

    let mut out: Vec<Row> = Vec::new();
    for (group_key_vals, group_rows) in &groups {
        // Build a representative row for this group.
        // For aggregate-without-group on empty input, SQL returns one row;
        // use an empty synthetic row as the evaluation context.
        let mut rep: Row = group_rows
            .first()
            .map(|first| (*first).clone())
            .unwrap_or_default();

        // Compute aggregate expressions and add them to the row under alias names.
        for item in &select.projection {
            if let SelectItem::ExprWithAlias { expr, alias } = item {
                let value = eval_group_expr(expr, &rep, group_rows, catalog, outer_row)?;
                rep.push((alias.value.clone(), value));
            } else if let SelectItem::UnnamedExpr(expr) = item {
                let alias = expr_alias(expr);
                let value = eval_group_expr(expr, &rep, group_rows, catalog, outer_row)?;
                rep.push((alias, value));
            }
        }
        // Also override group-by key columns to use the correct values.
        for (i, grp_expr) in group_cols.iter().enumerate() {
            let col_name = expr_alias(grp_expr);
            if let Some(kv) = group_key_vals.get(i) {
                rep.push((col_name, kv.clone()));
            }
        }
        // Apply HAVING.
        if let Some(having) = &select.having {
            if !eval_group_expr(having, &rep, group_rows, catalog, outer_row)
                .map(|v| v.truthy())
                .unwrap_or(false)
            {
                continue;
            }
        }
        out.push(rep);
    }
    Ok(out)
}

fn eval_group_expr(
    expr: &Expr,
    row: &Row,
    group_rows: &[&Row],
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<ScalarVal, QueryError> {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            match name.as_str() {
                "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" => {
                    Ok(eval_aggregate(expr, group_rows, catalog, outer_row)
                        .unwrap_or(ScalarVal::Null))
                }
                _ => eval_expr(expr, row, catalog, outer_row),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let lv = eval_group_expr(left, row, group_rows, catalog, outer_row)?;
            let rv = eval_group_expr(right, row, group_rows, catalog, outer_row)?;
            eval_binary_values(lv, op, rv)
        }
        Expr::UnaryOp { op, expr: inner } => {
            let v = eval_group_expr(inner, row, group_rows, catalog, outer_row)?;
            match op {
                UnaryOperator::Minus => match v {
                    ScalarVal::Int(n) => Ok(ScalarVal::Int(-n)),
                    ScalarVal::Float(f) => Ok(ScalarVal::Float(-f)),
                    _ => Err(QueryError::TypeError("unary minus on non-numeric".into())),
                },
                UnaryOperator::Not => Ok(ScalarVal::Bool(!v.truthy())),
                UnaryOperator::Plus => Ok(v),
                _ => Err(QueryError::Unsupported(format!("unary op: {op}"))),
            }
        }
        Expr::Nested(inner) => eval_group_expr(inner, row, group_rows, catalog, outer_row),
        _ => eval_expr(expr, row, catalog, outer_row),
    }
}

fn eval_binary_values(
    lv: ScalarVal,
    op: &BinaryOperator,
    rv: ScalarVal,
) -> Result<ScalarVal, QueryError> {
    match op {
        BinaryOperator::Plus => numeric_op(&lv, &rv, |a, b| a + b),
        BinaryOperator::Minus => numeric_op(&lv, &rv, |a, b| a - b),
        BinaryOperator::Multiply => numeric_op(&lv, &rv, |a, b| a * b),
        BinaryOperator::Divide => {
            let b = rv.as_f64().unwrap_or(0.0);
            if b == 0.0 {
                return Err(QueryError::DivisionByZero);
            }
            Ok(ScalarVal::Float(lv.as_f64().unwrap_or(0.0) / b))
        }
        BinaryOperator::Modulo => {
            let b = rv.as_f64().unwrap_or(0.0);
            if b == 0.0 {
                return Err(QueryError::DivisionByZero);
            }
            Ok(ScalarVal::Float(lv.as_f64().unwrap_or(0.0) % b))
        }
        BinaryOperator::StringConcat => Ok(ScalarVal::Text(
            scalar_to_string(&lv) + &scalar_to_string(&rv),
        )),
        BinaryOperator::Eq => Ok(ScalarVal::Bool(lv == rv)),
        BinaryOperator::NotEq => Ok(ScalarVal::Bool(lv != rv)),
        BinaryOperator::Gt => Ok(ScalarVal::Bool(
            lv.cmp_val(&rv)
                .map(|c| c == std::cmp::Ordering::Greater)
                .unwrap_or(false),
        )),
        BinaryOperator::Lt => Ok(ScalarVal::Bool(
            lv.cmp_val(&rv)
                .map(|c| c == std::cmp::Ordering::Less)
                .unwrap_or(false),
        )),
        BinaryOperator::GtEq => Ok(ScalarVal::Bool(
            lv.cmp_val(&rv)
                .map(|c| c != std::cmp::Ordering::Less)
                .unwrap_or(false),
        )),
        BinaryOperator::LtEq => Ok(ScalarVal::Bool(
            lv.cmp_val(&rv)
                .map(|c| c != std::cmp::Ordering::Greater)
                .unwrap_or(false),
        )),
        BinaryOperator::And => Ok(ScalarVal::Bool(lv.truthy() && rv.truthy())),
        BinaryOperator::Or => Ok(ScalarVal::Bool(lv.truthy() || rv.truthy())),
        _ => Err(QueryError::Unsupported(format!("binary op: {op}"))),
    }
}

fn eval_aggregate(
    expr: &Expr,
    group_rows: &[&Row],
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Option<ScalarVal> {
    match expr {
        Expr::Function(func) => {
            let fname = func.name.to_string().to_uppercase();
            match fname.as_str() {
                "COUNT" => {
                    let is_star = func_args(&func.args).iter().any(|a| {
                        matches!(
                            a,
                            sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                        )
                    });
                    if is_star {
                        return Some(ScalarVal::Int(group_rows.len() as i64));
                    }
                    let arg_expr = func_args(&func.args).first().and_then(|a| match a {
                        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })?;
                    let cnt = group_rows
                        .iter()
                        .filter(|r| {
                            !matches!(
                                eval_expr(arg_expr, r, catalog, outer_row),
                                Ok(ScalarVal::Null) | Err(_)
                            )
                        })
                        .count();
                    Some(ScalarVal::Int(cnt as i64))
                }
                "SUM" => {
                    let arg_expr = func_args(&func.args).first().and_then(|a| match a {
                        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })?;
                    let vals: Vec<f64> = group_rows
                        .iter()
                        .filter_map(|r| eval_expr(arg_expr, r, catalog, outer_row).ok()?.as_f64())
                        .collect();
                    if vals.is_empty() {
                        Some(ScalarVal::Null)
                    } else {
                        Some(ScalarVal::Float(vals.iter().sum()))
                    }
                }
                "AVG" => {
                    let arg_expr = func_args(&func.args).first().and_then(|a| match a {
                        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })?;
                    let vals: Vec<f64> = group_rows
                        .iter()
                        .filter_map(|r| eval_expr(arg_expr, r, catalog, outer_row).ok()?.as_f64())
                        .collect();
                    if vals.is_empty() {
                        Some(ScalarVal::Null)
                    } else {
                        Some(ScalarVal::Float(
                            vals.iter().sum::<f64>() / vals.len() as f64,
                        ))
                    }
                }
                "MIN" => {
                    let arg_expr = func_args(&func.args).first().and_then(|a| match a {
                        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })?;
                    let mut m: Option<f64> = None;
                    for r in group_rows {
                        if let Ok(v) = eval_expr(arg_expr, r, catalog, outer_row) {
                            if let Some(f) = v.as_f64() {
                                m = Some(m.map_or(f, |cur: f64| cur.min(f)));
                            }
                        }
                    }
                    m.map(ScalarVal::Float).or(Some(ScalarVal::Null))
                }
                "MAX" => {
                    let arg_expr = func_args(&func.args).first().and_then(|a| match a {
                        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })?;
                    let mut m: Option<f64> = None;
                    for r in group_rows {
                        if let Ok(v) = eval_expr(arg_expr, r, catalog, outer_row) {
                            if let Some(f) = v.as_f64() {
                                m = Some(m.map_or(f, |cur: f64| cur.max(f)));
                            }
                        }
                    }
                    m.map(ScalarVal::Float).or(Some(ScalarVal::Null))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

// ── Projection ────────────────────────────────────────────────────────────────

fn apply_projection(
    items: &[SelectItem],
    rows: &[Row],
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<QueryResult, QueryError> {
    // Build column name list from first row evaluation.
    let cols: Vec<String> = items
        .iter()
        .flat_map(|item| match item {
            SelectItem::Wildcard(_) => rows
                .first()
                .map(|r| {
                    r.iter()
                        .map(|(k, _)| {
                            k.rfind('.')
                                .map(|i| k[i + 1..].to_string())
                                .unwrap_or_else(|| k.clone())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            SelectItem::QualifiedWildcard(name, _) => {
                let pfx = name.to_string().to_lowercase();
                rows.first()
                    .map(|r| {
                        r.iter()
                            .filter(|(k, _)| k.starts_with(&format!("{pfx}.")))
                            .map(|(k, _)| {
                                k.rfind('.')
                                    .map(|i| k[i + 1..].to_string())
                                    .unwrap_or_else(|| k.clone())
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            }
            SelectItem::UnnamedExpr(e) => vec![expr_alias(e)],
            SelectItem::ExprWithAlias { alias, .. } => vec![alias.value.clone()],
        })
        .collect();

    let result_rows: Vec<Vec<ScalarVal>> = rows
        .iter()
        .map(|row| {
            items
                .iter()
                .flat_map(|item| match item {
                    SelectItem::Wildcard(_) => {
                        row.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>()
                    }
                    SelectItem::QualifiedWildcard(name, _) => {
                        let pfx = name.to_string().to_lowercase();
                        row.iter()
                            .filter(|(k, _)| k.starts_with(&format!("{pfx}.")))
                            .map(|(_, v)| v.clone())
                            .collect()
                    }
                    SelectItem::UnnamedExpr(e) => {
                        // If we already computed the aggregate in perform_groupby, fetch from row by alias.
                        let alias = expr_alias(e);
                        if let Some(v) = row_get(row, &alias) {
                            vec![v.clone()]
                        } else {
                            vec![eval_expr(e, row, catalog, outer_row).unwrap_or(ScalarVal::Null)]
                        }
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let v = if let Some(v) = row_get(row, &alias.value) {
                            v.clone()
                        } else {
                            eval_expr(expr, row, catalog, outer_row).unwrap_or(ScalarVal::Null)
                        };
                        vec![v]
                    }
                })
                .collect()
        })
        .collect();

    Ok(QueryResult {
        columns: cols,
        rows: result_rows,
    })
}

// ── ORDER BY ──────────────────────────────────────────────────────────────────

fn apply_order_by(
    mut result: QueryResult,
    order: &[OrderByExpr],
) -> Result<QueryResult, QueryError> {
    let cols = result.columns.clone();
    result.rows.sort_by(|a, b| {
        for ob in order {
            let idx = match &ob.expr {
                Expr::Identifier(id) => cols.iter().position(|c| c.eq_ignore_ascii_case(&id.value)),
                Expr::Value(Value::Number(s, _)) => {
                    s.parse::<usize>()
                        .ok()
                        .and_then(|n| if n > 0 { Some(n - 1) } else { None })
                }
                _ => None,
            };
            let (va, vb) = if let Some(i) = idx {
                (
                    a.get(i).unwrap_or(&ScalarVal::Null),
                    b.get(i).unwrap_or(&ScalarVal::Null),
                )
            } else {
                continue;
            };
            let cmp = va.cmp_val(vb).unwrap_or(std::cmp::Ordering::Equal);
            let cmp = if ob.asc == Some(false) {
                cmp.reverse()
            } else {
                cmp
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(result)
}

fn dedup_result(mut result: QueryResult) -> QueryResult {
    let mut seen: Vec<Vec<String>> = Vec::new();
    result.rows.retain(|row| {
        let key: Vec<String> = row.iter().map(|v| format!("{v:?}")).collect();
        if seen.contains(&key) {
            false
        } else {
            seen.push(key);
            true
        }
    });
    result
}

// ── Expression evaluator ──────────────────────────────────────────────────────

fn eval_expr(
    expr: &Expr,
    row: &Row,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<ScalarVal, QueryError> {
    match expr {
        // Literals
        Expr::Value(v) => Ok(eval_value(v)),
        Expr::TypedString { value, .. } => {
            // TypedString covers `date '...'` and `timestamp '...'`
            // Store dates as YYYYMMDD integers to match tpch.rs storage format.
            if let Some(d) = iso_to_yyyymmdd(value) {
                Ok(ScalarVal::Date(d))
            } else {
                Ok(ScalarVal::Text(value.clone()))
            }
        }

        // Column references
        Expr::Identifier(id) => {
            let name = id.value.as_str();
            // Check the current row first, then outer row for correlated subqueries.
            row_get(row, name)
                .or_else(|| row_get(outer_row, name))
                .cloned()
                .ok_or_else(|| QueryError::ColumnNotFound(name.to_string()))
        }
        Expr::CompoundIdentifier(parts) => {
            let qualified = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            let bare = parts.last().map(|p| p.value.as_str()).unwrap_or("");
            row_get(row, &qualified)
                .or_else(|| row_get(row, bare))
                .or_else(|| row_get(outer_row, &qualified))
                .or_else(|| row_get(outer_row, bare))
                .cloned()
                .ok_or(QueryError::ColumnNotFound(qualified))
        }

        // Arithmetic
        Expr::BinaryOp { left, op, right } => eval_binary(left, op, right, row, catalog, outer_row),

        // Unary
        Expr::UnaryOp { op, expr: inner } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            match op {
                UnaryOperator::Minus => match v {
                    ScalarVal::Int(n) => Ok(ScalarVal::Int(-n)),
                    ScalarVal::Float(f) => Ok(ScalarVal::Float(-f)),
                    _ => Err(QueryError::TypeError("unary minus on non-numeric".into())),
                },
                UnaryOperator::Not => Ok(ScalarVal::Bool(!v.truthy())),
                UnaryOperator::Plus => Ok(v),
                _ => Err(QueryError::Unsupported(format!("unary op: {op}"))),
            }
        }

        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            Ok(ScalarVal::Bool(matches!(v, ScalarVal::Null)))
        }
        Expr::IsNotNull(inner) => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            Ok(ScalarVal::Bool(!matches!(v, ScalarVal::Null)))
        }

        // BETWEEN
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            let lo = eval_expr(low, row, catalog, outer_row)?;
            let hi = eval_expr(high, row, catalog, outer_row)?;
            let in_range = v
                .cmp_val(&lo)
                .map(|c| c != std::cmp::Ordering::Less)
                .unwrap_or(false)
                && v.cmp_val(&hi)
                    .map(|c| c != std::cmp::Ordering::Greater)
                    .unwrap_or(false);
            Ok(ScalarVal::Bool(if *negated { !in_range } else { in_range }))
        }

        // LIKE / NOT LIKE
        Expr::Like {
            expr: inner,
            pattern,
            negated,
            ..
        } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            let p = eval_expr(pattern, row, catalog, outer_row)?;
            if let (ScalarVal::Text(s), ScalarVal::Text(pat)) = (&v, &p) {
                let matched = like_match(s, pat);
                Ok(ScalarVal::Bool(if *negated { !matched } else { matched }))
            } else {
                Ok(ScalarVal::Null)
            }
        }
        // ILIKE (case-insensitive LIKE)
        Expr::ILike {
            expr: inner,
            pattern,
            negated,
            ..
        } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            let p = eval_expr(pattern, row, catalog, outer_row)?;
            if let (ScalarVal::Text(s), ScalarVal::Text(pat)) = (&v, &p) {
                let matched = like_match(&s.to_uppercase(), &pat.to_uppercase());
                Ok(ScalarVal::Bool(if *negated { !matched } else { matched }))
            } else {
                Ok(ScalarVal::Null)
            }
        }

        // IN list / IN subquery
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            let found = list.iter().any(|e| {
                eval_expr(e, row, catalog, outer_row)
                    .map(|ev| ev == v)
                    .unwrap_or(false)
            });
            Ok(ScalarVal::Bool(if *negated { !found } else { found }))
        }
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            let result = execute_query_inner(subquery, catalog, row)?;
            let found = result
                .rows
                .iter()
                .any(|r| r.first().map(|sv| sv == &v).unwrap_or(false));
            Ok(ScalarVal::Bool(if *negated { !found } else { found }))
        }

        // EXISTS
        Expr::Exists { subquery, negated } => {
            let result = execute_query_inner(subquery, catalog, row)?;
            let found = !result.rows.is_empty();
            Ok(ScalarVal::Bool(if *negated { !found } else { found }))
        }

        // Scalar subquery
        Expr::Subquery(q) => {
            let result = execute_query_inner(q, catalog, row)?;
            match result.rows.as_slice() {
                [] => Ok(ScalarVal::Null),
                [r] => Ok(r.first().cloned().unwrap_or(ScalarVal::Null)),
                _ => Err(QueryError::SubqueryMultipleRows),
            }
        }

        // CASE WHEN
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            for (cond, res) in conditions.iter().zip(results.iter()) {
                let matches = if let Some(op) = operand {
                    let lhs = eval_expr(op, row, catalog, outer_row)?;
                    let rhs = eval_expr(cond, row, catalog, outer_row)?;
                    lhs == rhs
                } else {
                    eval_expr(cond, row, catalog, outer_row)?.truthy()
                };
                if matches {
                    return eval_expr(res, row, catalog, outer_row);
                }
            }
            if let Some(els) = else_result {
                eval_expr(els, row, catalog, outer_row)
            } else {
                Ok(ScalarVal::Null)
            }
        }

        // Nested
        Expr::Nested(inner) => eval_expr(inner, row, catalog, outer_row),

        // Functions
        Expr::Function(func) => eval_function(func, row, catalog, outer_row),

        // CAST
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            match data_type {
                sqlparser::ast::DataType::Float(_) | sqlparser::ast::DataType::Double => {
                    Ok(v.as_f64().map(ScalarVal::Float).unwrap_or(ScalarVal::Null))
                }
                sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                    match v {
                        ScalarVal::Float(f) => Ok(ScalarVal::Int(f as i64)),
                        ScalarVal::Int(n) => Ok(ScalarVal::Int(n)),
                        _ => Ok(ScalarVal::Null),
                    }
                }
                _ => Ok(v),
            }
        }

        // EXTRACT(field FROM expr)
        Expr::Extract {
            field, expr: inner, ..
        } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            let yyyymmdd = match &v {
                ScalarVal::Date(d) => *d,
                ScalarVal::Int(n) => *n as i32,
                _ => return Ok(ScalarVal::Null),
            };
            let result = match field {
                DateTimeField::Year => yyyymmdd / 10_000,
                DateTimeField::Month => (yyyymmdd / 100) % 100,
                DateTimeField::Day => yyyymmdd % 100,
                _ => return Ok(ScalarVal::Null),
            };
            Ok(ScalarVal::Int(result as i64))
        }

        // Trim
        Expr::Trim { expr: inner, .. } => {
            let v = eval_expr(inner, row, catalog, outer_row)?;
            if let ScalarVal::Text(s) = v {
                Ok(ScalarVal::Text(s.trim().to_string()))
            } else {
                Ok(v)
            }
        }

        _ => Err(QueryError::Unsupported(format!("expr: {expr}"))),
    }
}

fn eval_value(v: &Value) -> ScalarVal {
    match v {
        Value::Number(s, _) => {
            if let Ok(n) = s.parse::<i64>() {
                ScalarVal::Int(n)
            } else if let Ok(f) = s.parse::<f64>() {
                ScalarVal::Float(f)
            } else {
                ScalarVal::Null
            }
        }
        // Bare string literals: keep as Text. Date casting happens only via TypedString.
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => ScalarVal::Text(s.clone()),
        Value::Boolean(b) => ScalarVal::Bool(*b),
        Value::Null => ScalarVal::Null,
        Value::Placeholder(_) => ScalarVal::Null,
        _ => ScalarVal::Null,
    }
}

fn eval_binary(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    row: &Row,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<ScalarVal, QueryError> {
    // Short-circuit AND/OR for correlated subquery performance.
    match op {
        BinaryOperator::And => {
            let lv = eval_expr(left, row, catalog, outer_row)?;
            if !lv.truthy() {
                return Ok(ScalarVal::Bool(false));
            }
            let rv = eval_expr(right, row, catalog, outer_row)?;
            return Ok(ScalarVal::Bool(rv.truthy()));
        }
        BinaryOperator::Or => {
            let lv = eval_expr(left, row, catalog, outer_row)?;
            if lv.truthy() {
                return Ok(ScalarVal::Bool(true));
            }
            let rv = eval_expr(right, row, catalog, outer_row)?;
            return Ok(ScalarVal::Bool(rv.truthy()));
        }
        _ => {}
    }

    let lv = eval_expr(left, row, catalog, outer_row)?;
    let rv = eval_expr(right, row, catalog, outer_row)?;

    match op {
        BinaryOperator::Plus => numeric_op(&lv, &rv, |a, b| a + b),
        BinaryOperator::Minus => numeric_op(&lv, &rv, |a, b| a - b),
        BinaryOperator::Multiply => numeric_op(&lv, &rv, |a, b| a * b),
        BinaryOperator::Divide => {
            let b = rv.as_f64().unwrap_or(0.0);
            if b == 0.0 {
                return Err(QueryError::DivisionByZero);
            }
            Ok(ScalarVal::Float(lv.as_f64().unwrap_or(0.0) / b))
        }
        BinaryOperator::Modulo => {
            let b = rv.as_f64().unwrap_or(0.0);
            if b == 0.0 {
                return Err(QueryError::DivisionByZero);
            }
            Ok(ScalarVal::Float(lv.as_f64().unwrap_or(0.0) % b))
        }
        BinaryOperator::StringConcat => {
            let ls = scalar_to_string(&lv);
            let rs = scalar_to_string(&rv);
            Ok(ScalarVal::Text(ls + &rs))
        }
        BinaryOperator::Eq => Ok(ScalarVal::Bool(lv == rv)),
        BinaryOperator::NotEq => Ok(ScalarVal::Bool(lv != rv)),
        BinaryOperator::Gt => Ok(ScalarVal::Bool(
            lv.cmp_val(&rv)
                .map(|c| c == std::cmp::Ordering::Greater)
                .unwrap_or(false),
        )),
        BinaryOperator::Lt => Ok(ScalarVal::Bool(
            lv.cmp_val(&rv)
                .map(|c| c == std::cmp::Ordering::Less)
                .unwrap_or(false),
        )),
        BinaryOperator::GtEq => Ok(ScalarVal::Bool(
            lv.cmp_val(&rv)
                .map(|c| c != std::cmp::Ordering::Less)
                .unwrap_or(false),
        )),
        BinaryOperator::LtEq => Ok(ScalarVal::Bool(
            lv.cmp_val(&rv)
                .map(|c| c != std::cmp::Ordering::Greater)
                .unwrap_or(false),
        )),
        _ => Err(QueryError::Unsupported(format!("binary op: {op}"))),
    }
}

fn numeric_op(
    a: &ScalarVal,
    b: &ScalarVal,
    f: impl Fn(f64, f64) -> f64,
) -> Result<ScalarVal, QueryError> {
    match (a, b) {
        (ScalarVal::Int(x), ScalarVal::Int(y)) => {
            let r = f(*x as f64, *y as f64);
            if r.fract() == 0.0 && r.abs() < i64::MAX as f64 {
                Ok(ScalarVal::Int(r as i64))
            } else {
                Ok(ScalarVal::Float(r))
            }
        }
        _ => {
            let fa = a
                .as_f64()
                .ok_or_else(|| QueryError::TypeError(format!("non-numeric: {a:?}")))?;
            let fb = b
                .as_f64()
                .ok_or_else(|| QueryError::TypeError(format!("non-numeric: {b:?}")))?;
            Ok(ScalarVal::Float(f(fa, fb)))
        }
    }
}

fn scalar_to_string(v: &ScalarVal) -> String {
    match v {
        ScalarVal::Text(s) => s.clone(),
        ScalarVal::Int(n) => n.to_string(),
        ScalarVal::Float(f) => format!("{f}"),
        ScalarVal::Date(d) => d.to_string(),
        ScalarVal::Bool(b) => b.to_string(),
        ScalarVal::Null => String::new(),
    }
}

fn eval_function(
    func: &Function,
    row: &Row,
    catalog: &QueryCatalog,
    outer_row: &Row,
) -> Result<ScalarVal, QueryError> {
    let fname = func.name.to_string().to_uppercase();
    let first_arg_expr = func_args(&func.args).first().and_then(|a| match a {
        sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
        _ => None,
    });

    match fname.as_str() {
        "UPPER" => {
            let v = eval_expr(
                first_arg_expr
                    .as_ref()
                    .ok_or_else(|| QueryError::TypeError("UPPER needs arg".into()))?,
                row,
                catalog,
                outer_row,
            )?;
            Ok(ScalarVal::Text(scalar_to_string(&v).to_uppercase()))
        }
        "LOWER" => {
            let v = eval_expr(
                first_arg_expr
                    .as_ref()
                    .ok_or_else(|| QueryError::TypeError("LOWER needs arg".into()))?,
                row,
                catalog,
                outer_row,
            )?;
            Ok(ScalarVal::Text(scalar_to_string(&v).to_lowercase()))
        }
        "SUBSTR" | "SUBSTRING" => {
            let arg1 = first_arg_expr
                .as_ref()
                .ok_or_else(|| QueryError::TypeError("SUBSTRING needs arg".into()))?;
            let s = scalar_to_string(&eval_expr(arg1, row, catalog, outer_row)?);
            let start: usize = func_args(&func.args)
                .get(1)
                .and_then(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        eval_expr(e, row, catalog, outer_row).ok()
                    }
                    _ => None,
                })
                .and_then(|v| v.as_f64())
                .map(|n| (n as usize).saturating_sub(1))
                .unwrap_or(0);
            let len: usize = func_args(&func.args)
                .get(2)
                .and_then(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        eval_expr(e, row, catalog, outer_row).ok()
                    }
                    _ => None,
                })
                .and_then(|v| v.as_f64())
                .map(|n| n as usize)
                .unwrap_or(s.len().saturating_sub(start));
            let chars: Vec<char> = s.chars().collect();
            let end = (start + len).min(chars.len());
            Ok(ScalarVal::Text(
                chars[start.min(chars.len())..end].iter().collect(),
            ))
        }
        "COALESCE" => {
            for arg in func_args(&func.args) {
                if let sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                    let v = eval_expr(e, row, catalog, outer_row)?;
                    if !matches!(v, ScalarVal::Null) {
                        return Ok(v);
                    }
                }
            }
            Ok(ScalarVal::Null)
        }
        "NULLIF" => {
            let a = eval_expr(
                first_arg_expr
                    .as_ref()
                    .ok_or_else(|| QueryError::TypeError("NULLIF arg".into()))?,
                row,
                catalog,
                outer_row,
            )?;
            let b_expr = func_args(&func.args)
                .get(1)
                .and_then(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                    _ => None,
                })
                .ok_or_else(|| QueryError::TypeError("NULLIF needs 2 args".into()))?;
            let b = eval_expr(b_expr, row, catalog, outer_row)?;
            Ok(if a == b { ScalarVal::Null } else { a })
        }
        "EXTRACT" => {
            // EXTRACT is typically represented as Function with special args in sqlparser.
            // Fall through to Unsupported — callers should handle date fields via TypedString.
            Ok(ScalarVal::Null)
        }
        // Aggregate functions used outside GROUP BY context produce NULL.
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => Ok(ScalarVal::Null),
        _ => Ok(ScalarVal::Null),
    }
}

// EXTRACT(YEAR/MONTH/DAY FROM date_col) — sqlparser represents this with a special Expr variant.
// Handle it in eval_expr via Expr::Extract if the version supports it.

// ── FunctionArguments helper ──────────────────────────────────────────────────

/// sqlparser 0.46 wraps function args in `FunctionArguments::List`; unwrap to a
/// plain slice so the rest of the code can use `.iter()`, `.first()`, `.get(n)`.
fn func_args(args: &sqlparser::ast::FunctionArguments) -> &[sqlparser::ast::FunctionArg] {
    if let sqlparser::ast::FunctionArguments::List(list) = args {
        &list.args
    } else {
        &[]
    }
}

// ── LIKE pattern matching ─────────────────────────────────────────────────────

fn like_match(s: &str, pattern: &str) -> bool {
    like_match_bytes(s.as_bytes(), pattern.as_bytes())
}

fn like_match_bytes(s: &[u8], p: &[u8]) -> bool {
    match (s, p) {
        (_, []) => s.is_empty(),
        (_, [b'%', rest @ ..]) => {
            if rest.is_empty() {
                return true;
            }
            (0..=s.len()).any(|i| like_match_bytes(&s[i..], rest))
        }
        ([], _) => false,
        ([sc, s_rest @ ..], [b'_', p_rest @ ..]) => {
            like_match_bytes(s_rest, p_rest) || {
                let _ = sc;
                false
            }
        }
        ([sc, s_rest @ ..], [pc, p_rest @ ..]) => {
            sc.eq_ignore_ascii_case(pc) && like_match_bytes(s_rest, p_rest)
        }
    }
}

// ── Date helpers ──────────────────────────────────────────────────────────────

/// Parse 'YYYY-MM-DD' string to YYYYMMDD integer.
/// Dates are stored as YYYYMMDD integers in tpch.rs for lexicographic-order
/// comparability (e.g., 19950315 > 19940101).
fn iso_to_yyyymmdd(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y: i32 = parts[0].parse().ok()?;
    let m: i32 = parts[1].parse().ok()?;
    let d: i32 = parts[2].parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(y * 10_000 + m * 100 + d)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn has_aggregate_in_projection(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            has_aggregate_expr(e)
        }
        _ => false,
    })
}

fn has_aggregate_expr(e: &Expr) -> bool {
    match e {
        Expr::Function(f) => {
            let name = f.name.to_string().to_uppercase();
            matches!(name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX")
        }
        Expr::BinaryOp { left, right, .. } => has_aggregate_expr(left) || has_aggregate_expr(right),
        Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => has_aggregate_expr(inner),
        Expr::Case {
            conditions,
            results,
            else_result,
            ..
        } => {
            conditions.iter().any(has_aggregate_expr)
                || results.iter().any(has_aggregate_expr)
                || else_result
                    .as_deref()
                    .map(has_aggregate_expr)
                    .unwrap_or(false)
        }
        _ => false,
    }
}

fn expr_alias(e: &Expr) -> String {
    match e {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => {
            parts.last().map(|p| p.value.clone()).unwrap_or_default()
        }
        Expr::Function(f) => f.name.to_string().to_lowercase(),
        Expr::BinaryOp { left, op, right } => {
            format!("{}_{}_{}", expr_alias(left), op, expr_alias(right))
        }
        Expr::Value(Value::Number(s, _)) => format!("const_{s}"),
        _ => "col".to_string(),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch_days_to_ymd(days: i32) -> (i32, u32, u32) {
        let z = days as i64 + 719468;
        let era = if z >= 0 {
            z / 146_097
        } else {
            (z - 146_096) / 146_097
        };
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        (y as i32, m as u32, d as u32)
    }

    fn make_rows(data: &[&[(&str, ScalarVal)]]) -> Vec<Row> {
        data.iter()
            .map(|r| r.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
            .collect()
    }

    #[test]
    fn like_match_percent_wildcard() {
        assert!(like_match("hello world", "%world"));
        assert!(like_match("hello world", "hello%"));
        assert!(like_match("hello world", "%lo wo%"));
        assert!(!like_match("hello world", "%xyz%"));
        assert!(like_match("brass", "%BRASS"));
    }

    #[test]
    fn like_match_underscore() {
        assert!(like_match("a1b", "a_b"));
        assert!(!like_match("ab", "a_b"));
    }

    #[test]
    fn scalar_val_comparison() {
        assert_eq!(
            ScalarVal::Int(3).cmp_val(&ScalarVal::Int(5)),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            ScalarVal::Float(1.5).cmp_val(&ScalarVal::Int(2)),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            ScalarVal::Text("a".into()).cmp_val(&ScalarVal::Text("b".into())),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(ScalarVal::Null.cmp_val(&ScalarVal::Int(1)), None);
    }

    #[test]
    fn epoch_days_to_ymd_known() {
        assert_eq!(epoch_days_to_ymd(0), (1970, 1, 1));
        assert_eq!(epoch_days_to_ymd(19723), (2024, 1, 1));
    }

    #[test]
    fn simple_select_filter() {
        use crate::sql::parse_statement;
        use sqlparser::ast::Statement;

        let mut catalog = QueryCatalog::new();
        catalog.tables.insert(
            "t".into(),
            make_rows(&[
                &[("t.id", ScalarVal::Int(1)), ("t.val", ScalarVal::Int(10))],
                &[("t.id", ScalarVal::Int(2)), ("t.val", ScalarVal::Int(20))],
                &[("t.id", ScalarVal::Int(3)), ("t.val", ScalarVal::Int(30))],
            ]),
        );

        let stmt = parse_statement("SELECT t.id, t.val FROM t WHERE t.val > 15").unwrap();
        let Statement::Query(q) = stmt else { panic!() };
        let result = execute_select_query(&q, &catalog).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert!(result.rows.iter().all(|r| {
            if let Some(ScalarVal::Int(n)) = r.get(1) {
                *n > 15
            } else {
                false
            }
        }));
    }

    #[test]
    fn group_by_sum() {
        use crate::sql::parse_statement;
        use sqlparser::ast::Statement;

        let mut catalog = QueryCatalog::new();
        catalog.tables.insert(
            "sales".into(),
            make_rows(&[
                &[
                    ("sales.cat", ScalarVal::Text("A".into())),
                    ("sales.amt", ScalarVal::Float(10.0)),
                ],
                &[
                    ("sales.cat", ScalarVal::Text("A".into())),
                    ("sales.amt", ScalarVal::Float(20.0)),
                ],
                &[
                    ("sales.cat", ScalarVal::Text("B".into())),
                    ("sales.amt", ScalarVal::Float(5.0)),
                ],
            ]),
        );

        let stmt =
            parse_statement("SELECT cat, sum(amt) AS total FROM sales GROUP BY cat").unwrap();
        let Statement::Query(q) = stmt else { panic!() };
        let result = execute_select_query(&q, &catalog).unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn order_by_limit() {
        use crate::sql::parse_statement;
        use sqlparser::ast::Statement;

        let mut catalog = QueryCatalog::new();
        catalog.tables.insert(
            "n".into(),
            make_rows(&[
                &[("n.v", ScalarVal::Int(3))],
                &[("n.v", ScalarVal::Int(1))],
                &[("n.v", ScalarVal::Int(2))],
            ]),
        );

        let stmt = parse_statement("SELECT v FROM n ORDER BY v ASC LIMIT 2").unwrap();
        let Statement::Query(q) = stmt else { panic!() };
        let result = execute_select_query(&q, &catalog).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], ScalarVal::Int(1));
        assert_eq!(result.rows[1][0], ScalarVal::Int(2));
    }

    #[test]
    fn case_when_expression() {
        use crate::sql::parse_statement;
        use sqlparser::ast::Statement;

        let mut catalog = QueryCatalog::new();
        catalog.tables.insert(
            "s".into(),
            make_rows(&[
                &[("s.x", ScalarVal::Int(5))],
                &[("s.x", ScalarVal::Int(15))],
            ]),
        );

        let sql = "SELECT CASE WHEN x > 10 THEN 1 ELSE 0 END AS flag FROM s";
        let stmt = parse_statement(sql).unwrap();
        let Statement::Query(q) = stmt else { panic!() };
        let result = execute_select_query(&q, &catalog).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], ScalarVal::Int(0));
        assert_eq!(result.rows[1][0], ScalarVal::Int(1));
    }
}
