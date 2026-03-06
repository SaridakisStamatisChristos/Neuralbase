// SPDX-License-Identifier: Apache-2.0
// Binder: resolves SQL AST nodes against the catalog and produces typed plans.
//
// Session 8: extended with INSERT, UPDATE, DELETE, CREATE TABLE.
// Session 11: extended with CreateUser, AlterUser, DropUser.
// Type coercion: SQL literals → SqlValue (int, float, text, date, null).
// Date coercion: 'YYYY-MM-DD' strings are converted to days-since-epoch (i32).
//
// CONFIDENCE: raw=0.76 effective=0.67
// DEPENDS_ON: catalog, sqlparser

use crate::catalog::{Catalog, ColumnDef, TableSchema};
#[cfg(test)]
use crate::catalog::InMemoryCatalog;
use crate::sql::NbStatement;
use sqlparser::ast::{
    Assignment, BinaryOperator, DataType, Expr, SelectItem, SetExpr,
    Statement, TableFactor, Value,
};
use thiserror::Error;

// ── SqlValue ─────────────────────────────────────────────────────────────────

/// A typed literal value produced by the binder after coercion.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Int(i64),
    Float(f64),
    Text(String),
    /// Days since the Unix epoch (1970-01-01) as i32.
    Date(i32),
    Null,
}

impl SqlValue {
    /// Render this value as a string for storage in the binary row codec
    /// (which stores all values as UTF-8 strings keyed by column name).
    pub fn to_storage_string(&self) -> Option<String> {
        match self {
            Self::Int(v)   => Some(v.to_string()),
            Self::Float(v) => Some(v.to_string()),
            Self::Text(s)  => Some(s.clone()),
            Self::Date(d)  => Some(d.to_string()),
            Self::Null     => None,
        }
    }
}

// ── DmlPredicate ─────────────────────────────────────────────────────────────

/// A simple binary comparison predicate used in WHERE clauses of DML
/// operations and SELECT statement predicate push-down.
#[derive(Debug, Clone, PartialEq)]
pub struct DmlPredicate {
    pub column: String,
    pub op: DmlCmpOp,
    pub value: SqlValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlCmpOp {
    Eq,
    Neq,
    Gt,
    Lt,
    GtEq,
    LtEq,
}

impl DmlPredicate {
    /// Evaluate this predicate against a row encoded as (column → string-value).
    /// Returns `false` for NULL column values.
    pub fn matches(&self, row: &std::collections::BTreeMap<String, String>) -> bool {
        let val_str = match row.get(&self.column) {
            Some(s) => s,
            None => return false,
        };
        match &self.value {
            SqlValue::Int(v) => {
                val_str
                    .parse::<i64>()
                    .map(|x| apply_op_i64(x, *v, self.op))
                    .unwrap_or(false)
            }
            SqlValue::Float(v) => {
                val_str
                    .parse::<f64>()
                    .map(|x| apply_op_f64(x, *v, self.op))
                    .unwrap_or(false)
            }
            SqlValue::Text(s) => match self.op {
                DmlCmpOp::Eq  => val_str == s,
                DmlCmpOp::Neq => val_str != s,
                _             => false,
            },
            SqlValue::Date(d) => {
                val_str
                    .parse::<i32>()
                    .map(|x| apply_op_i64(x as i64, *d as i64, self.op))
                    .unwrap_or(false)
            }
            SqlValue::Null => false,
        }
    }
}

fn apply_op_i64(x: i64, criterion: i64, op: DmlCmpOp) -> bool {
    match op {
        DmlCmpOp::Eq   => x == criterion,
        DmlCmpOp::Neq  => x != criterion,
        DmlCmpOp::Gt   => x > criterion,
        DmlCmpOp::Lt   => x < criterion,
        DmlCmpOp::GtEq => x >= criterion,
        DmlCmpOp::LtEq => x <= criterion,
    }
}

fn apply_op_f64(x: f64, criterion: f64, op: DmlCmpOp) -> bool {
    match op {
        DmlCmpOp::Eq   => (x - criterion).abs() < f64::EPSILON,
        DmlCmpOp::Neq  => (x - criterion).abs() >= f64::EPSILON,
        DmlCmpOp::Gt   => x > criterion,
        DmlCmpOp::Lt   => x < criterion,
        DmlCmpOp::GtEq => x >= criterion,
        DmlCmpOp::LtEq => x <= criterion,
    }
}

// ── Plan types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct InsertPlan {
    pub table: TableSchema,
    /// Column names aligned to `rows` entries (positional if no explicit list).
    pub columns: Vec<String>,
    /// Each inner Vec is one row of SqlValues aligned to `columns`.
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdatePlan {
    pub table: TableSchema,
    /// (column_name, new_value) pairs.
    pub assignments: Vec<(String, SqlValue)>,
    pub predicate: Option<DmlPredicate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeletePlan {
    pub table: TableSchema,
    pub predicate: Option<DmlPredicate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTablePlan {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
}

impl CreateTablePlan {
    pub fn to_table_schema(&self) -> TableSchema {
        TableSchema { name: self.name.clone(), columns: self.columns.clone() }
    }
}

// ── BoundPlan ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum BoundPlan {
    SelectConstI64(i64),
    SelectFromTable {
        table: TableSchema,
        projection: Vec<String>,
        /// Typed, parsed WHERE predicate (None → full scan).
        where_clause: Option<DmlPredicate>,
        limit: Option<u64>,
        query_fingerprint: String,
    },
    /// General SQL SELECT: JOIN / subquery / GROUP BY / multi-table.
    /// Executed by the row-oriented query_executor.
    SelectQuery(Box<sqlparser::ast::Query>),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    CreateTable(CreateTablePlan),
    DropTable { name: String },
    /// CREATE USER name WITH PASSWORD 'password'
    CreateUser { username: String, password: String },
    /// ALTER USER name WITH PASSWORD 'new_password'
    AlterUser { username: String, new_password: String },
    /// DROP USER [IF EXISTS] name
    DropUser { username: String, if_exists: bool },
    /// EXPLAIN [ANALYZE] SELECT ... — returns the query plan as text.
    Explain {
        query: Box<sqlparser::ast::Query>,
        analyze: bool,
    },
}

// ── BindError ─────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum BindError {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("unsupported SELECT shape")]
    UnsupportedSelect,
    #[error("column not found in table '{table}': {column}")]
    ColumnNotFound { table: String, column: String },
    #[error("column count mismatch: expected {expected}, got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },
    #[error("type coercion failed for column '{column}': {reason}")]
    TypeCoercionFailed { column: String, reason: String },
    #[error("unsupported SQL statement")]
    Unsupported,
}

// ── Main entry point ──────────────────────────────────────────────────────────

/// Bind a NeuralBase-extended statement (handles user management extensions).
pub fn bind_nb_statement(
    nb_stmt: &NbStatement,
    catalog: &dyn Catalog,
) -> Result<BoundPlan, BindError> {
    match nb_stmt {
        NbStatement::Sql(stmt) => bind_statement(stmt.as_ref(), catalog),
        NbStatement::CreateUser { username, password } => Ok(BoundPlan::CreateUser {
            username: username.clone(),
            password: password.clone(),
        }),
        NbStatement::AlterUser { username, new_password } => Ok(BoundPlan::AlterUser {
            username: username.clone(),
            new_password: new_password.clone(),
        }),
        NbStatement::DropUser { username, if_exists } => Ok(BoundPlan::DropUser {
            username: username.clone(),
            if_exists: *if_exists,
        }),
    }
}

pub fn bind_statement(
    statement: &Statement,
    catalog: &dyn Catalog,
) -> Result<BoundPlan, BindError> {
    match statement {
        Statement::Query(query) => bind_query(query, catalog),
        Statement::Insert(insert) => bind_insert(insert, catalog),
        Statement::Update { table, assignments, selection, .. } => {
            bind_update(table, assignments, selection.as_ref(), catalog)
        }
        Statement::Delete(delete) => bind_delete(delete, catalog),
        Statement::CreateTable { name, columns, if_not_exists, .. } => {
            bind_create_table(name, columns, *if_not_exists)
        }
        Statement::Drop { object_type, names, .. } => {
            use sqlparser::ast::ObjectType;
            if *object_type == ObjectType::Table {
                let name = names.first().map(|n| n.to_string()).unwrap_or_default();
                Ok(BoundPlan::DropTable { name })
            } else {
                Err(BindError::Unsupported)
            }
        }
        Statement::Explain { analyze, statement, .. } => {
            if let Statement::Query(q) = statement.as_ref() {
                Ok(BoundPlan::Explain {
                    query: q.clone(),
                    analyze: *analyze,
                })
            } else {
                Err(BindError::Unsupported)
            }
        }
        _ => Err(BindError::Unsupported),
    }
}

// ── SELECT binding ────────────────────────────────────────────────────────────

fn bind_query(
    query: &sqlparser::ast::Query,
    catalog: &dyn Catalog,
) -> Result<BoundPlan, BindError> {
    // Non-SELECT bodies (UNION, INTERSECT, VALUES, ...) go to the general executor.
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(BoundPlan::SelectQuery(Box::new(query.clone())));
    };

    if select.from.is_empty() {
        if select.projection.len() == 1 {
            if let SelectItem::UnnamedExpr(Expr::Value(Value::Number(s, _))) =
                &select.projection[0]
            {
                if let Ok(n) = s.parse::<i64>() {
                    return Ok(BoundPlan::SelectConstI64(n));
                }
            }
        }
        // Expressions without a FROM clause go to the general executor.
        return Ok(BoundPlan::SelectQuery(Box::new(query.clone())));
    }

    // Multiple FROM tables, JOINs, or derived-table sources — general executor.
    if select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || !matches!(select.from[0].relation, TableFactor::Table { .. })
    {
        return Ok(BoundPlan::SelectQuery(Box::new(query.clone())));
    }

    let relation = &select.from[0].relation;
    let TableFactor::Table { name, .. } = relation else {
        return Ok(BoundPlan::SelectQuery(Box::new(query.clone())));
    };

    let table_name = name.to_string();
    // If the table is not in the catalog, fall through to the general executor
    // which builds its own QueryCatalog from TpchDataSet.
    let Some(table) = catalog.get_table(&table_name) else {
        return Ok(BoundPlan::SelectQuery(Box::new(query.clone())));
    };

    let projection = if select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)))
    {
        table.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        select
            .projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Some(ident.value.clone()),
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                _ => None,
            })
            .collect()
    };

    let limit = query.limit.as_ref().and_then(|expr| match expr {
        Expr::Value(Value::Number(s, _)) => s.parse::<u64>().ok(),
        _ => None,
    });

    let where_clause = select
        .selection
        .as_ref()
        .and_then(|e| parse_simple_predicate(e, &table));

    Ok(BoundPlan::SelectFromTable {
        table,
        projection,
        where_clause,
        limit,
        query_fingerprint: query.to_string(),
    })
}

// ── INSERT binding ────────────────────────────────────────────────────────────

fn bind_insert(
    insert: &sqlparser::ast::Insert,
    catalog: &dyn Catalog,
) -> Result<BoundPlan, BindError> {
    let table_name = insert.table_name.to_string();
    let table = catalog
        .get_table(&table_name)
        .ok_or_else(|| BindError::TableNotFound(table_name.clone()))?;

    let columns: Vec<String> = if insert.columns.is_empty() {
        table.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        insert.columns.iter().map(|c| c.value.clone()).collect()
    };

    for col_name in &columns {
        if !table.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col_name)) {
            return Err(BindError::ColumnNotFound {
                table: table_name.clone(),
                column: col_name.clone(),
            });
        }
    }

    let source = insert.source.as_ref().ok_or(BindError::Unsupported)?;
    let SetExpr::Values(values_list) = source.body.as_ref() else {
        return Err(BindError::Unsupported);
    };

    let mut rows: Vec<Vec<SqlValue>> = Vec::with_capacity(values_list.rows.len());
    for row_exprs in &values_list.rows {
        if row_exprs.len() != columns.len() {
            return Err(BindError::ColumnCountMismatch {
                expected: columns.len(),
                actual: row_exprs.len(),
            });
        }
        let mut row_values = Vec::with_capacity(row_exprs.len());
        for (expr, col_name) in row_exprs.iter().zip(&columns) {
            let col_def = table
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| BindError::ColumnNotFound {
                    table: table_name.clone(),
                    column: col_name.clone(),
                })?;
            row_values.push(coerce_expr_to_value(expr, col_def)?);
        }
        rows.push(row_values);
    }

    Ok(BoundPlan::Insert(InsertPlan { table, columns, rows }))
}

// ── UPDATE binding ────────────────────────────────────────────────────────────

fn bind_update(
    table: &sqlparser::ast::TableWithJoins,
    assignments: &[Assignment],
    selection: Option<&Expr>,
    catalog: &dyn Catalog,
) -> Result<BoundPlan, BindError> {
    let TableFactor::Table { name, .. } = &table.relation else {
        return Err(BindError::UnsupportedSelect);
    };
    let table_name = name.to_string();
    let schema = catalog
        .get_table(&table_name)
        .ok_or_else(|| BindError::TableNotFound(table_name.clone()))?;

    let mut bound_assignments = Vec::with_capacity(assignments.len());
    for a in assignments {
        let col_name = a.id
            .last()
            .map(|i| i.value.as_str())
            .unwrap_or("")
            .to_string();
        let col_def = schema
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&col_name))
            .ok_or_else(|| BindError::ColumnNotFound {
                table: table_name.clone(),
                column: col_name.clone(),
            })?;
        bound_assignments.push((col_def.name.clone(), coerce_expr_to_value(&a.value, col_def)?));
    }

    let predicate = selection.and_then(|e| parse_simple_predicate(e, &schema));

    Ok(BoundPlan::Update(UpdatePlan {
        table: schema,
        assignments: bound_assignments,
        predicate,
    }))
}

// ── DELETE binding ────────────────────────────────────────────────────────────

fn bind_delete(
    delete: &sqlparser::ast::Delete,
    catalog: &dyn Catalog,
) -> Result<BoundPlan, BindError> {
    // sqlparser 0.46: standard `DELETE FROM t` populates `from: FromTable`.
    // `tables: Vec<ObjectName>` is only for MySQL multi-table DELETE syntax.
    let tables = match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(ts) => ts,
        sqlparser::ast::FromTable::WithoutKeyword(ts) => ts,
    };
    if tables.len() != 1 {
        return Err(BindError::Unsupported);
    }
    let TableFactor::Table { name, .. } = &tables[0].relation else {
        return Err(BindError::Unsupported);
    };
    let table_name = name.to_string().to_lowercase();
    let schema = catalog
        .get_table(&table_name)
        .ok_or_else(|| BindError::TableNotFound(table_name.clone()))?;

    let predicate = delete
        .selection
        .as_ref()
        .and_then(|e| parse_simple_predicate(e, &schema));

    Ok(BoundPlan::Delete(DeletePlan { table: schema, predicate }))
}

// ── CREATE TABLE binding ──────────────────────────────────────────────────────

fn bind_create_table(
    name: &sqlparser::ast::ObjectName,
    columns: &[sqlparser::ast::ColumnDef],
    if_not_exists: bool,
) -> Result<BoundPlan, BindError> {
    let name_str = name.to_string().to_lowercase();
    let bound_cols: Vec<ColumnDef> = columns
        .iter()
        .map(|col: &sqlparser::ast::ColumnDef| ColumnDef {
            name: col.name.value.clone(),
            data_type: sql_data_type_to_str(&col.data_type),
        })
        .collect();

    Ok(BoundPlan::CreateTable(CreateTablePlan {
        name: name_str,
        columns: bound_cols,
        if_not_exists,
    }))
}

fn sql_data_type_to_str(dt: &DataType) -> String {
    match dt {
        DataType::Int(_) | DataType::Integer(_) | DataType::Int4(_) => "INT".to_string(),
        DataType::BigInt(_) | DataType::Int8(_)                     => "BIGINT".to_string(),
        DataType::Float(_) | DataType::Double | DataType::Real   => "DOUBLE".to_string(),
        DataType::Decimal(_) | DataType::Numeric(_)                 => "DOUBLE".to_string(),
        DataType::Date                                               => "DATE".to_string(),
        DataType::Varchar(_) | DataType::Text | DataType::Char(_)   => "TEXT".to_string(),
        DataType::Boolean                                            => "INT".to_string(),
        other                                                        => other.to_string(),
    }
}

// ── Type coercion ─────────────────────────────────────────────────────────────

/// Coerce a SQL AST expression to a typed `SqlValue` using the target column's
/// declared data type.  Returns `BindError::TypeCoercionFailed` on mismatch.
pub fn coerce_expr_to_value(expr: &Expr, col: &ColumnDef) -> Result<SqlValue, BindError> {
    match expr {
        Expr::Value(v) => coerce_value(v, col),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            Expr::Value(v) => {
                let sv = coerce_value(v, col)?;
                match sv {
                    SqlValue::Int(n)   => Ok(SqlValue::Int(-n)),
                    SqlValue::Float(f) => Ok(SqlValue::Float(-f)),
                    _ => Err(BindError::TypeCoercionFailed {
                        column: col.name.clone(),
                        reason: "unary minus on non-numeric".to_string(),
                    }),
                }
            }
            _ => Err(BindError::TypeCoercionFailed {
                column: col.name.clone(),
                reason: "unsupported unary expression".to_string(),
            }),
        },
        _ => Err(BindError::TypeCoercionFailed {
            column: col.name.clone(),
            reason: format!("unsupported expression: {expr}"),
        }),
    }
}

fn coerce_value(v: &Value, col: &ColumnDef) -> Result<SqlValue, BindError> {
    match v {
        Value::Null => Ok(SqlValue::Null),

        Value::Number(s, _) => {
            let t = col.data_type.to_uppercase();
            if t.contains("INT") || t.contains("BOOL") {
                s.parse::<i64>().map(SqlValue::Int).map_err(|_| BindError::TypeCoercionFailed {
                    column: col.name.clone(),
                    reason: format!("cannot parse '{s}' as integer"),
                })
            } else if t.contains("DOUBLE") || t.contains("FLOAT")
                   || t.contains("REAL") || t.contains("DECIMAL") || t.contains("NUMERIC")
            {
                s.parse::<f64>().map(SqlValue::Float).map_err(|_| BindError::TypeCoercionFailed {
                    column: col.name.clone(),
                    reason: format!("cannot parse '{s}' as float"),
                })
            } else if t.contains("DATE") {
                s.parse::<i32>().map(SqlValue::Date).map_err(|_| BindError::TypeCoercionFailed {
                    column: col.name.clone(),
                    reason: format!("cannot parse '{s}' as date integer"),
                })
            } else {
                Ok(SqlValue::Text(s.clone()))
            }
        }

        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            let t = col.data_type.to_uppercase();
            if t.contains("DATE") {
                date_str_to_epoch_days(s).map(SqlValue::Date).ok_or_else(|| {
                    BindError::TypeCoercionFailed {
                        column: col.name.clone(),
                        reason: format!("cannot parse '{s}' as ISO date (YYYY-MM-DD)"),
                    }
                })
            } else {
                Ok(SqlValue::Text(s.clone()))
            }
        }

        Value::Boolean(b) => {
            let t = col.data_type.to_uppercase();
            if t.contains("INT") {
                Ok(SqlValue::Int(i64::from(*b)))
            } else {
                Ok(SqlValue::Text(b.to_string()))
            }
        }

        _ => Err(BindError::TypeCoercionFailed {
            column: col.name.clone(),
            reason: format!("unsupported literal type: {v}"),
        }),
    }
}

// ── Date coercion: 'YYYY-MM-DD' → days since 1970-01-01 ──────────────────────
//
// Uses Howard Hinnant's civil calendar algorithm.
// http://howardhinnant.github.io/date_algorithms.html

/// Convert a 'YYYY-MM-DD' string to days since the Unix epoch (1970-01-01).
/// Returns `None` on parse failure or invalid calendar values.
pub fn date_str_to_epoch_days(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 { return None; }
    let y: i32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let d: u32 = parts[2].parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) { return None; }
    Some(civil_to_epoch_days(y, m, d))
}

fn civil_to_epoch_days(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era: i32 = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = (y - era * 400) as u32;
    let moy = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * moy + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i32 - 719468
}

// ── WHERE predicate extraction ────────────────────────────────────────────────

/// Attempt to extract a simple `col op literal` predicate from a WHERE expr.
/// Returns `None` for complex expressions → caller performs a full scan.
fn parse_simple_predicate(expr: &Expr, table: &TableSchema) -> Option<DmlPredicate> {
    let Expr::BinaryOp { left, op, right } = expr else { return None; };
    let col_name = extract_identifier(left)?;
    let col_def = table.columns.iter()
        .find(|c| c.name.eq_ignore_ascii_case(&col_name))?;

    let dml_op = match op {
        BinaryOperator::Eq    => DmlCmpOp::Eq,
        BinaryOperator::NotEq => DmlCmpOp::Neq,
        BinaryOperator::Gt    => DmlCmpOp::Gt,
        BinaryOperator::Lt    => DmlCmpOp::Lt,
        BinaryOperator::GtEq  => DmlCmpOp::GtEq,
        BinaryOperator::LtEq  => DmlCmpOp::LtEq,
        _                     => return None,
    };

    let sql_value = coerce_expr_to_value(right, col_def).ok()?;
    Some(DmlPredicate { column: col_def.name.clone(), op: dml_op, value: sql_value })
}

fn extract_identifier(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
        _ => None,
    }
}

// ── Test helper (catalog builder) ─────────────────────────────────────────────

/// Build a minimal catalog for tests.
#[cfg(test)]
fn make_simple_catalog() -> InMemoryCatalog {
    let cat = InMemoryCatalog::default();
    cat.register_table(TableSchema {
        name: "t".to_string(),
        columns: vec![
            ColumnDef { name: "id".to_string(),   data_type: "INT".to_string() },
            ColumnDef { name: "name".to_string(), data_type: "TEXT".to_string() },
        ],
    });
    cat
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parse_statement;

    #[test]
    fn select_const_i64() {
        let stmt = parse_statement("SELECT 42").unwrap();
        let cat = InMemoryCatalog::with_tpch_lineitem();
        assert_eq!(bind_statement(&stmt, &cat).unwrap(), BoundPlan::SelectConstI64(42));
    }

    #[test]
    fn select_from_table_wildcard() {
        let stmt = parse_statement("SELECT * FROM t").unwrap();
        let cat = make_simple_catalog();
        match bind_statement(&stmt, &cat).unwrap() {
            BoundPlan::SelectFromTable { projection, table, .. } => {
                assert_eq!(table.name, "t");
                assert_eq!(projection, vec!["id", "name"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn select_with_where_predicate() {
        let stmt = parse_statement("SELECT * FROM t WHERE id = 1").unwrap();
        let cat = make_simple_catalog();
        match bind_statement(&stmt, &cat).unwrap() {
            BoundPlan::SelectFromTable { where_clause: Some(pred), .. } => {
                assert_eq!(pred.column, "id");
                assert_eq!(pred.op, DmlCmpOp::Eq);
                assert_eq!(pred.value, SqlValue::Int(1));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn insert_positional_values() {
        let stmt = parse_statement("INSERT INTO t VALUES (1, 'hello')").unwrap();
        let cat = make_simple_catalog();
        match bind_statement(&stmt, &cat).unwrap() {
            BoundPlan::Insert(p) => {
                assert_eq!(p.rows.len(), 1);
                assert_eq!(p.rows[0][0], SqlValue::Int(1));
                assert_eq!(p.rows[0][1], SqlValue::Text("hello".to_string()));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn insert_wrong_column_count_is_error() {
        let stmt = parse_statement("INSERT INTO t VALUES (1, 'hello', 'extra')").unwrap();
        let cat = make_simple_catalog();
        let err = bind_statement(&stmt, &cat).unwrap_err();
        assert!(matches!(err, BindError::ColumnCountMismatch { expected: 2, actual: 3 }));
    }

    #[test]
    fn update_set_predicate() {
        let stmt = parse_statement("UPDATE t SET name = 'world' WHERE id = 1").unwrap();
        let cat = make_simple_catalog();
        match bind_statement(&stmt, &cat).unwrap() {
            BoundPlan::Update(p) => {
                assert_eq!(p.assignments[0], ("name".to_string(), SqlValue::Text("world".to_string())));
                let pred = p.predicate.unwrap();
                assert_eq!(pred.value, SqlValue::Int(1));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn delete_with_predicate() {
        let stmt = parse_statement("DELETE FROM t WHERE id = 1").unwrap();
        let cat = make_simple_catalog();
        match bind_statement(&stmt, &cat).unwrap() {
            BoundPlan::Delete(p) => {
                assert_eq!(p.predicate.unwrap().value, SqlValue::Int(1));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn create_table_plan() {
        let stmt = parse_statement("CREATE TABLE orders (o_id INT, o_total DOUBLE)").unwrap();
        let cat = make_simple_catalog();
        match bind_statement(&stmt, &cat).unwrap() {
            BoundPlan::CreateTable(p) => {
                assert_eq!(p.name, "orders");
                assert_eq!(p.columns[0].data_type, "INT");
                assert_eq!(p.columns[1].data_type, "DOUBLE");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn date_coercion_iso_string() {
        // 2024-01-01 is 19723 days after 1970-01-01.
        assert_eq!(date_str_to_epoch_days("2024-01-01"), Some(19723));
    }

    #[test]
    fn date_coercion_epoch() {
        assert_eq!(date_str_to_epoch_days("1970-01-01"), Some(0));
    }

    #[test]
    fn date_coercion_invalid_returns_none() {
        assert!(date_str_to_epoch_days("not-a-date").is_none());
        assert!(date_str_to_epoch_days("2024-13-01").is_none());
    }

    #[test]
    fn date_column_coercion_in_insert() {
        let cat = InMemoryCatalog::default();
        cat.register_table(TableSchema {
            name: "events".to_string(),
            columns: vec![
                ColumnDef { name: "event_date".to_string(), data_type: "DATE".to_string() },
            ],
        });
        let stmt = parse_statement("INSERT INTO events VALUES ('2024-01-01')").unwrap();
        match bind_statement(&stmt, &cat).unwrap() {
            BoundPlan::Insert(p) => {
                assert_eq!(p.rows[0][0], SqlValue::Date(19723));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn dml_predicate_matches_int_row() {
        let mut row = std::collections::BTreeMap::new();
        row.insert("id".to_string(), "42".to_string());
        let pred = DmlPredicate { column: "id".to_string(), op: DmlCmpOp::Eq, value: SqlValue::Int(42) };
        assert!(pred.matches(&row));
        let pred_ne = DmlPredicate { value: SqlValue::Int(99), ..pred.clone() };
        assert!(!pred_ne.matches(&row));
    }

    #[test]
    fn insert_table_not_found_is_error() {
        let stmt = parse_statement("INSERT INTO nonexistent VALUES (1)").unwrap();
        let cat = make_simple_catalog();
        assert!(matches!(bind_statement(&stmt, &cat).unwrap_err(), BindError::TableNotFound(_)));
    }
}
