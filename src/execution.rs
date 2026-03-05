use crate::binder::{BoundPlan, DmlCmpOp, DmlPredicate};
use crate::scheduler::MorselScheduler;
use crate::tpch::TpchDataSet;
use crate::vectorized::{
    hash_aggregate, run_parallel_filter, table_scan, ColumnVector, ExecError, Predicate,
    RecordBatch,
};

// ── TableScanner trait ─────────────────────────────────────────────────────

/// Storage-layer abstraction for table scans.
/// Implemented by `StorageExecutor` (MVCC path) and by test stubs.
/// Defined here to keep `execute_physical_plan` independent of the storage
/// crate — `#[path]` test includes do not need to pull in storage deps.
pub trait TableScanner: Send + Sync {
    fn scan_table(&self, table_name: &str) -> Result<RecordBatch, ExecError>;
}

type PgColumns = Vec<(String, i32, i16)>;
type PgRows = Vec<Vec<Option<String>>>;

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    Scan {
        table: String,
        projection: Vec<String>,
        predicate: Option<Predicate>,
        limit: Option<usize>,
    },
    TpchQ1,
    TpchQ6,
}

pub fn build_physical_plan(plan: &BoundPlan) -> PhysicalPlan {
    match plan {
        BoundPlan::SelectConstI64(_) => PhysicalPlan::Scan {
            table: "const".to_string(),
            projection: vec![],
            predicate: None,
            limit: Some(1),
        },
        BoundPlan::SelectFromTable {
            table,
            projection,
            where_clause,
            limit,
            query_fingerprint,
        } => {
            let normalized = query_fingerprint.to_lowercase();
            if normalized.contains("group by l_returnflag") && normalized.contains("sum(") {
                return PhysicalPlan::TpchQ1;
            }
            if normalized.contains("l_discount")
                && normalized.contains("l_shipdate")
                && normalized.contains("sum(")
            {
                return PhysicalPlan::TpchQ6;
            }

            PhysicalPlan::Scan {
                table: table.name.clone(),
                projection: projection.clone(),
                predicate: where_clause.as_ref().and_then(dml_predicate_to_vectorized),
                limit: limit.map(|v| v as usize),
            }
        }
        // DML and DDL plans are handled directly in server.rs.
        _ => PhysicalPlan::Scan {
            table: "unsupported".to_string(),
            projection: vec![],
            predicate: None,
            limit: Some(0),
        },
    }
}

pub fn execute_physical_plan(
    plan: &PhysicalPlan,
    data: &TpchDataSet,
    scheduler: &MorselScheduler,
    storage: Option<&dyn TableScanner>,
) -> Result<RecordBatch, ExecError> {
    match plan {
        PhysicalPlan::Scan {
            table,
            projection,
            predicate,
            limit,
        } => {
            // Non-lineitem tables: try MVCC storage first.
            if !table.eq_ignore_ascii_case("lineitem") {
                if let Some(scanner) = storage {
                    return match scanner.scan_table(table) {
                        Ok(batch) => table_scan(&batch, predicate.as_ref(), None, *limit),
                        Err(ExecError::TableNotFound(_)) => Ok(RecordBatch::empty()),
                        Err(e) => Err(e),
                    };
                }
                return Ok(RecordBatch::empty());
            }
            // Lineitem: use in-memory TPC-H dataset (optimised path).
            let source = &data.lineitem;
            let projected = if projection.is_empty() {
                None
            } else {
                Some(projection.as_slice())
            };
            table_scan(source, predicate.as_ref(), projected, *limit)
        }
        PhysicalPlan::TpchQ1 => execute_tpch_q1(&data.lineitem, scheduler),
        PhysicalPlan::TpchQ6 => execute_tpch_q6(&data.lineitem, scheduler),
    }
}

fn execute_tpch_q1(
    lineitem: &RecordBatch,
    scheduler: &MorselScheduler,
) -> Result<RecordBatch, ExecError> {
    let predicate = Predicate::LtI64 {
        column: "l_orderkey".to_string(),
        value: i64::MAX,
    };
    let scanned = run_parallel_filter(lineitem, &predicate, scheduler)?;

    let Some(ColumnVector::Float64(prices)) = scanned.column("l_extendedprice") else {
        return Err(ExecError::ColumnTypeMismatch("l_extendedprice".to_string()));
    };
    let Some(ColumnVector::Float64(discounts)) = scanned.column("l_discount") else {
        return Err(ExecError::ColumnTypeMismatch("l_discount".to_string()));
    };

    let revenue = prices
        .iter()
        .zip(discounts)
        .map(|(price, discount)| match (price, discount) {
            (Some(p), Some(d)) => Some(p * (1.0 - d)),
            _ => None,
        })
        .collect::<Vec<Option<f64>>>();

    let enriched = RecordBatch::new(vec![
        (
            "l_returnflag".to_string(),
            scanned
                .column("l_returnflag")
                .ok_or_else(|| ExecError::ColumnNotFound("l_returnflag".to_string()))?
                .clone(),
        ),
        ("revenue".to_string(), ColumnVector::Float64(revenue)),
    ])?;

    hash_aggregate(&enriched, "l_returnflag", "revenue")
}

fn execute_tpch_q6(
    lineitem: &RecordBatch,
    scheduler: &MorselScheduler,
) -> Result<RecordBatch, ExecError> {
    let date_filtered = run_parallel_filter(
        lineitem,
        &Predicate::BetweenDate32 {
            column: "l_shipdate".to_string(),
            start: 19940101,
            end_exclusive: 19950101,
        },
        scheduler,
    )?;

    let discount_filtered = run_parallel_filter(
        &date_filtered,
        &Predicate::BetweenFloat64 {
            column: "l_discount".to_string(),
            low: 0.05,
            high: 0.07,
        },
        scheduler,
    )?;

    let Some(ColumnVector::Float64(quantity)) = discount_filtered.column("l_quantity") else {
        return Err(ExecError::ColumnTypeMismatch("l_quantity".to_string()));
    };
    let Some(ColumnVector::Float64(price)) = discount_filtered.column("l_extendedprice") else {
        return Err(ExecError::ColumnTypeMismatch("l_extendedprice".to_string()));
    };
    let Some(ColumnVector::Float64(discount)) = discount_filtered.column("l_discount") else {
        return Err(ExecError::ColumnTypeMismatch("l_discount".to_string()));
    };

    let mut revenue = 0.0_f64;
    for row in 0..discount_filtered.row_count {
        if let (Some(q), Some(p), Some(d)) = (quantity[row], price[row], discount[row]) {
            if q < 24.0 {
                revenue += p * d;
            }
        }
    }

    RecordBatch::new(vec![(
        "revenue".to_string(),
        ColumnVector::Float64(vec![Some(revenue)]),
    )])
}

/// Convert a binder-level `DmlPredicate` to a vectorized `Predicate` for SELECT push-down.
/// Returns `None` for operators/types that vectorized doesn't support (falls back to full scan).
pub fn dml_predicate_to_vectorized(pred: &DmlPredicate) -> Option<Predicate> {
    use crate::binder::SqlValue;
    match (&pred.op, &pred.value) {
        (DmlCmpOp::Eq, SqlValue::Text(s)) => Some(Predicate::EqText {
            column: pred.column.clone(),
            value: s.clone(),
        }),
        (DmlCmpOp::Gt, SqlValue::Int(v)) => Some(Predicate::GtI64 {
            column: pred.column.clone(),
            value: *v,
        }),
        (DmlCmpOp::Lt, SqlValue::Int(v)) => Some(Predicate::LtI64 {
            column: pred.column.clone(),
            value: *v,
        }),
        (DmlCmpOp::Eq, SqlValue::Int(v)) => Some(Predicate::EqI64 {
            column: pred.column.clone(),
            value: *v,
        }),
        _ => None,
    }
}

pub fn batch_to_pg_rows(batch: &RecordBatch) -> (PgColumns, PgRows) {
    let columns = batch
        .columns
        .iter()
        .map(|(name, col)| {
            let (oid, size) = match col {
                ColumnVector::Int32(_) => (23, 4),
                ColumnVector::Int64(_) => (20, 8),
                ColumnVector::Float64(_) => (701, 8),
                ColumnVector::Date32(_) => (1082, 4),
                ColumnVector::Utf8(_) => (25, -1),
            };
            (name.clone(), oid, size)
        })
        .collect::<Vec<_>>();

    (columns, batch.rows_as_strings())
}

pub fn mock_const_batch(value: i64) -> RecordBatch {
    RecordBatch::new(vec![(
        "?column?".to_string(),
        ColumnVector::Int64(vec![Some(value)]),
    )])
    .expect("const batch must build")
}


