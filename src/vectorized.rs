use crate::scheduler::MorselScheduler;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct Utf8Column {
    pub offsets: Vec<u32>,
    pub data: Vec<u8>,
    pub validity: Vec<bool>,
}

impl Utf8Column {
    pub fn from_options(values: Vec<Option<&str>>) -> Self {
        let mut offsets = Vec::with_capacity(values.len() + 1);
        let mut data = Vec::new();
        let mut validity = Vec::with_capacity(values.len());
        offsets.push(0);

        for value in values {
            match value {
                Some(text) => {
                    validity.push(true);
                    data.extend_from_slice(text.as_bytes());
                    offsets.push(data.len() as u32);
                }
                None => {
                    validity.push(false);
                    offsets.push(data.len() as u32);
                }
            }
        }

        Self {
            offsets,
            data,
            validity,
        }
    }

    pub fn from_owned_options(values: Vec<Option<String>>) -> Self {
        let refs = values
            .iter()
            .map(|v| v.as_deref())
            .collect::<Vec<Option<&str>>>();
        Self::from_options(refs)
    }

    pub fn get(&self, index: usize) -> Option<String> {
        if !self.validity.get(index).copied().unwrap_or(false) {
            return None;
        }
        let start = *self.offsets.get(index)? as usize;
        let end = *self.offsets.get(index + 1)? as usize;
        String::from_utf8(self.data[start..end].to_vec()).ok()
    }

    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn is_empty(&self) -> bool {
        self.validity.is_empty()
    }

}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnVector {
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Date32(Vec<Option<i32>>),
    Utf8(Utf8Column),
}

impl ColumnVector {
    pub fn len(&self) -> usize {
        match self {
            Self::Int32(values) => values.len(),
            Self::Int64(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Date32(values) => values.len(),
            Self::Utf8(values) => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn take(&self, indices: &[usize]) -> Self {
        match self {
            Self::Int32(values) => Self::Int32(indices.iter().map(|i| values[*i]).collect()),
            Self::Int64(values) => Self::Int64(indices.iter().map(|i| values[*i]).collect()),
            Self::Float64(values) => Self::Float64(indices.iter().map(|i| values[*i]).collect()),
            Self::Date32(values) => Self::Date32(indices.iter().map(|i| values[*i]).collect()),
            Self::Utf8(values) => {
                let taken = indices.iter().map(|i| values.get(*i)).collect::<Vec<_>>();
                Self::Utf8(Utf8Column::from_owned_options(taken))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    pub columns: Vec<(String, ColumnVector)>,
    pub row_count: usize,
}

impl RecordBatch {
    pub fn new(columns: Vec<(String, ColumnVector)>) -> Result<Self, ExecError> {
        let row_count = columns.first().map(|(_, col)| col.len()).unwrap_or(0);
        for (_, col) in &columns {
            if col.len() != row_count {
                return Err(ExecError::ColumnLengthMismatch);
            }
        }
        Ok(Self { columns, row_count })
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            row_count: 0,
        }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnVector> {
        self.columns
            .iter()
            .find(|(col_name, _)| col_name.eq_ignore_ascii_case(name))
            .map(|(_, col)| col)
    }

    pub fn select_columns(&self, projection: &[String]) -> Result<Self, ExecError> {
        let mut columns = Vec::new();
        for target in projection {
            let Some(col) = self.column(target) else {
                return Err(ExecError::ColumnNotFound(target.clone()));
            };
            columns.push((target.clone(), col.clone()));
        }
        Self::new(columns)
    }

    pub fn with_limit(&self, limit: usize) -> Result<Self, ExecError> {
        if limit >= self.row_count {
            return Ok(self.clone());
        }
        let indices = (0..limit).collect::<Vec<usize>>();
        let columns = self
            .columns
            .iter()
            .map(|(name, col)| (name.clone(), col.take(&indices)))
            .collect();
        Self::new(columns)
    }

    pub fn rows_as_strings(&self) -> Vec<Vec<Option<String>>> {
        let mut rows = Vec::with_capacity(self.row_count);
        for index in 0..self.row_count {
            let mut row = Vec::with_capacity(self.columns.len());
            for (_, col) in &self.columns {
                let value = match col {
                    ColumnVector::Int32(values) => values[index].map(|v| v.to_string()),
                    ColumnVector::Int64(values) => values[index].map(|v| v.to_string()),
                    ColumnVector::Float64(values) => values[index].map(|v| format!("{v:.4}")),
                    ColumnVector::Date32(values) => values[index].map(|v| v.to_string()),
                    ColumnVector::Utf8(values) => values.get(index),
                };
                row.push(value);
            }
            rows.push(row);
        }
        rows
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    EqI64 {
        column: String,
        value: i64,
    },
    GtI64 {
        column: String,
        value: i64,
    },
    LtI64 {
        column: String,
        value: i64,
    },
    BetweenDate32 {
        column: String,
        start: i32,
        end_exclusive: i32,
    },
    BetweenFloat64 {
        column: String,
        low: f64,
        high: f64,
    },
    /// Equality predicate for UTF-8 / Text columns.
    EqText {
        column: String,
        value: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Gt,
    Lt,
    Eq,
}

#[derive(Debug, Error, PartialEq)]
pub enum ExecError {
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("column type mismatch for {0}")]
    ColumnTypeMismatch(String),
    #[error("column length mismatch")]
    ColumnLengthMismatch,
    #[error("batch overflow: {0}")]
    BatchOverflow(usize),
    #[error("scheduler error: {0}")]
    Scheduler(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
}

pub fn table_scan(
    input: &RecordBatch,
    predicate: Option<&Predicate>,
    projection: Option<&[String]>,
    limit: Option<usize>,
) -> Result<RecordBatch, ExecError> {
    let filtered = if let Some(predicate) = predicate {
        filter(input, predicate)?
    } else {
        input.clone()
    };

    let projected = if let Some(projection) = projection {
        if projection.is_empty() {
            filtered
        } else {
            filtered.select_columns(projection)?
        }
    } else {
        filtered
    };

    if let Some(limit) = limit {
        projected.with_limit(limit)
    } else {
        Ok(projected)
    }
}

pub fn filter(input: &RecordBatch, predicate: &Predicate) -> Result<RecordBatch, ExecError> {
    if input.row_count > 10_000_000 {
        return Err(ExecError::BatchOverflow(input.row_count));
    }

    let mask = match predicate {
        Predicate::EqI64 { column, value } => filter_i64(input, column, *value, |v, c| v == c)?,
        Predicate::GtI64 { column, value } => filter_i64(input, column, *value, |v, c| v > c)?,
        Predicate::LtI64 { column, value } => filter_i64(input, column, *value, |v, c| v < c)?,
        Predicate::BetweenDate32 {
            column,
            start,
            end_exclusive,
        } => {
            let Some(ColumnVector::Date32(values)) = input.column(column) else {
                return Err(ExecError::ColumnTypeMismatch(column.clone()));
            };
            values
                .iter()
                .map(|v| {
                    v.map(|x| x >= *start && x < *end_exclusive)
                        .unwrap_or(false)
                })
                .collect::<Vec<bool>>()
        }
        Predicate::BetweenFloat64 { column, low, high } => {
            let Some(ColumnVector::Float64(values)) = input.column(column) else {
                return Err(ExecError::ColumnTypeMismatch(column.clone()));
            };
            values
                .iter()
                .map(|v| v.map(|x| x >= *low && x <= *high).unwrap_or(false))
                .collect::<Vec<bool>>()
        }
        Predicate::EqText { column, value } => {
            let Some(ColumnVector::Utf8(col)) = input.column(column) else {
                return Err(ExecError::ColumnTypeMismatch(column.clone()));
            };
            (0..input.row_count)
                .map(|i| col.get(i).as_deref() == Some(value.as_str()))
                .collect::<Vec<bool>>()
        }
    };

    let indices = mask
        .iter()
        .enumerate()
        .filter_map(|(index, keep)| keep.then_some(index))
        .collect::<Vec<usize>>();

    let columns = input
        .columns
        .iter()
        .map(|(name, col)| (name.clone(), col.take(&indices)))
        .collect::<Vec<_>>();

    RecordBatch::new(columns)
}

pub fn hash_aggregate(
    input: &RecordBatch,
    group_by: &str,
    value_col: &str,
) -> Result<RecordBatch, ExecError> {
    let Some(ColumnVector::Utf8(keys)) = input.column(group_by) else {
        return Err(ExecError::ColumnTypeMismatch(group_by.to_string()));
    };
    let Some(ColumnVector::Float64(values)) = input.column(value_col) else {
        return Err(ExecError::ColumnTypeMismatch(value_col.to_string()));
    };

    let mut groups: HashMap<String, (f64, usize, f64, f64)> = HashMap::new();
    for (row, value_opt) in values.iter().enumerate().take(input.row_count) {
        let Some(key) = keys.get(row) else {
            continue;
        };
        let Some(value) = *value_opt else {
            continue;
        };
        let entry = groups
            .entry(key)
            .or_insert((0.0, 0, f64::INFINITY, f64::NEG_INFINITY));
        entry.0 += value;
        entry.1 += 1;
        entry.2 = entry.2.min(value);
        entry.3 = entry.3.max(value);
    }

    let mut out_keys = Vec::new();
    let mut out_sum = Vec::new();
    let mut out_count = Vec::new();
    let mut out_avg = Vec::new();
    let mut out_min = Vec::new();
    let mut out_max = Vec::new();

    let mut ordered = groups.into_iter().collect::<Vec<_>>();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));

    for (key, (sum, count, min, max)) in ordered {
        out_keys.push(Some(key));
        out_sum.push(Some(sum));
        out_count.push(Some(count as i64));
        out_avg.push(Some(sum / count as f64));
        out_min.push(Some(min));
        out_max.push(Some(max));
    }

    RecordBatch::new(vec![
        (
            group_by.to_string(),
            ColumnVector::Utf8(Utf8Column::from_options(
                out_keys.iter().map(|v| v.as_deref()).collect(),
            )),
        ),
        ("sum".to_string(), ColumnVector::Float64(out_sum)),
        ("count".to_string(), ColumnVector::Int64(out_count)),
        ("avg".to_string(), ColumnVector::Float64(out_avg)),
        ("min".to_string(), ColumnVector::Float64(out_min)),
        ("max".to_string(), ColumnVector::Float64(out_max)),
    ])
}

pub fn sort_merge_join(
    left: &RecordBatch,
    right: &RecordBatch,
    left_key: &str,
    right_key: &str,
) -> Result<RecordBatch, ExecError> {
    let Some(ColumnVector::Int64(left_ids)) = left.column(left_key) else {
        return Err(ExecError::ColumnTypeMismatch(left_key.to_string()));
    };
    let Some(ColumnVector::Int64(right_ids)) = right.column(right_key) else {
        return Err(ExecError::ColumnTypeMismatch(right_key.to_string()));
    };

    let mut left_indices = left_ids
        .iter()
        .enumerate()
        .filter_map(|(idx, key)| key.map(|k| (k, idx)))
        .collect::<Vec<(i64, usize)>>();
    let mut right_indices = right_ids
        .iter()
        .enumerate()
        .filter_map(|(idx, key)| key.map(|k| (k, idx)))
        .collect::<Vec<(i64, usize)>>();

    left_indices.sort_by_key(|(key, _)| *key);
    right_indices.sort_by_key(|(key, _)| *key);

    let mut left_matches = Vec::new();
    let mut right_matches = Vec::new();

    let mut left_ptr = 0_usize;
    let mut right_ptr = 0_usize;

    while left_ptr < left_indices.len() && right_ptr < right_indices.len() {
        let left_key_value = left_indices[left_ptr].0;
        let right_key_value = right_indices[right_ptr].0;

        if left_key_value < right_key_value {
            left_ptr += 1;
            continue;
        }

        if left_key_value > right_key_value {
            right_ptr += 1;
            continue;
        }

        let mut left_end = left_ptr;
        while left_end < left_indices.len() && left_indices[left_end].0 == left_key_value {
            left_end += 1;
        }

        let mut right_end = right_ptr;
        while right_end < right_indices.len() && right_indices[right_end].0 == right_key_value {
            right_end += 1;
        }

        for (_, l_idx) in &left_indices[left_ptr..left_end] {
            for (_, r_idx) in &right_indices[right_ptr..right_end] {
                left_matches.push(*l_idx);
                right_matches.push(*r_idx);
            }
        }

        left_ptr = left_end;
        right_ptr = right_end;
    }

    let mut columns = Vec::new();
    for (name, col) in &left.columns {
        columns.push((format!("left.{name}"), col.take(&left_matches)));
    }
    for (name, col) in &right.columns {
        columns.push((format!("right.{name}"), col.take(&right_matches)));
    }

    RecordBatch::new(columns)
}

#[cfg(test)]
pub fn sort(input: &RecordBatch, by: &str) -> Result<RecordBatch, ExecError> {
    let mut indices = (0..input.row_count).collect::<Vec<usize>>();
    let Some(column) = input.column(by) else {
        return Err(ExecError::ColumnNotFound(by.to_string()));
    };

    indices.sort_by(|a, b| match column {
        ColumnVector::Int64(values) => values[*a].cmp(&values[*b]),
        ColumnVector::Int32(values) => values[*a].cmp(&values[*b]),
        ColumnVector::Float64(values) => values[*a]
            .partial_cmp(&values[*b])
            .unwrap_or(std::cmp::Ordering::Equal),
        ColumnVector::Date32(values) => values[*a].cmp(&values[*b]),
        ColumnVector::Utf8(values) => values.get(*a).cmp(&values.get(*b)),
    });

    let columns = input
        .columns
        .iter()
        .map(|(name, col)| (name.clone(), col.take(&indices)))
        .collect::<Vec<_>>();

    RecordBatch::new(columns)
}

#[cfg(test)]
pub fn sort_with_limit(
    input: &RecordBatch,
    by: &str,
    limit: usize,
) -> Result<RecordBatch, ExecError> {
    let sorted = sort(input, by)?;
    sorted.with_limit(limit)
}

pub fn run_parallel_filter(
    input: &RecordBatch,
    predicate: &Predicate,
    scheduler: &MorselScheduler,
) -> Result<RecordBatch, ExecError> {
    let chunks = scheduler
        .parallel_map_ranges(input.row_count, |range| {
            let indices = (range.start..range.end).collect::<Vec<usize>>();
            let mask = apply_predicate_to_indices(input, predicate, &indices);
            indices
                .iter()
                .zip(mask)
                .filter_map(|(idx, keep)| keep.then_some(*idx))
                .collect::<Vec<usize>>()
        })
        .map_err(ExecError::Scheduler)?;

    let columns = input
        .columns
        .iter()
        .map(|(name, col)| (name.clone(), col.take(&chunks)))
        .collect::<Vec<_>>();

    RecordBatch::new(columns)
}

fn apply_predicate_to_indices(
    input: &RecordBatch,
    predicate: &Predicate,
    indices: &[usize],
) -> Vec<bool> {
    match predicate {
        Predicate::EqI64 { column, value } => {
            apply_i64_indices(input, column, *value, indices, |v, c| v == c)
        }
        Predicate::GtI64 { column, value } => {
            apply_i64_indices(input, column, *value, indices, |v, c| v > c)
        }
        Predicate::LtI64 { column, value } => {
            apply_i64_indices(input, column, *value, indices, |v, c| v < c)
        }
        Predicate::BetweenDate32 { column, start, end_exclusive } => {
            let Some(ColumnVector::Date32(values)) = input.column(column) else {
                return vec![false; indices.len()];
            };
            indices
                .iter()
                .map(|idx| values[*idx].map(|v| v >= *start && v < *end_exclusive).unwrap_or(false))
                .collect()
        }
        Predicate::BetweenFloat64 { column, low, high } => {
            let Some(ColumnVector::Float64(values)) = input.column(column) else {
                return vec![false; indices.len()];
            };
            indices
                .iter()
                .map(|idx| values[*idx].map(|v| v >= *low && v <= *high).unwrap_or(false))
                .collect()
        }
        Predicate::EqText { column, value } => {
            let Some(ColumnVector::Utf8(col)) = input.column(column) else {
                return vec![false; indices.len()];
            };
            indices
                .iter()
                .map(|idx| col.get(*idx).as_deref() == Some(value.as_str()))
                .collect()
        }
    }
}

fn filter_i64<F>(
    input: &RecordBatch,
    column: &str,
    criterion: i64,
    cmp: F,
) -> Result<Vec<bool>, ExecError>
where
    F: Fn(i64, i64) -> bool + Copy,
{
    let op = if cmp(2, 1) {
        ComparisonOp::Gt
    } else if cmp(1, 2) {
        ComparisonOp::Lt
    } else {
        ComparisonOp::Eq
    };

    match input.column(column) {
        Some(ColumnVector::Int64(values)) => Ok(i64_mask_auto(values, criterion, op)),
        Some(ColumnVector::Int32(values)) => {
            // Upcast Int32 → i64 for comparison; avoids needing a separate EqI32 predicate.
            Ok(values
                .iter()
                .map(|v| v.map(|x| cmp(x as i64, criterion)).unwrap_or(false))
                .collect())
        }
        _ => Err(ExecError::ColumnTypeMismatch(column.to_string())),
    }
}

pub fn i64_mask_scalar(values: &[Option<i64>], criterion: i64, op: ComparisonOp) -> Vec<bool> {
    values
        .iter()
        .map(|v| {
            v.map(|x| match op {
                ComparisonOp::Gt => x > criterion,
                ComparisonOp::Lt => x < criterion,
                ComparisonOp::Eq => x == criterion,
            })
            .unwrap_or(false)
        })
        .collect()
}

pub fn i64_mask_auto(values: &[Option<i64>], criterion: i64, op: ComparisonOp) -> Vec<bool> {
    #[cfg(all(feature = "simd", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            return simd_i64_filter(values, criterion, op);
        }
    }

    i64_mask_scalar(values, criterion, op)
}

#[cfg(all(feature = "simd", target_arch = "x86_64"))]
fn simd_i64_filter(values: &[Option<i64>], criterion: i64, op: ComparisonOp) -> Vec<bool> {
    i64_mask_scalar(values, criterion, op)
}

fn apply_i64_indices<F>(
    input: &RecordBatch,
    column: &str,
    criterion: i64,
    indices: &[usize],
    cmp: F,
) -> Vec<bool>
where
    F: Fn(i64, i64) -> bool + Copy,
{
    match input.column(column) {
        Some(ColumnVector::Int64(values)) => indices
            .iter()
            .map(|idx| values[*idx].map(|v| cmp(v, criterion)).unwrap_or(false))
            .collect(),
        Some(ColumnVector::Int32(values)) => indices
            .iter()
            .map(|idx| values[*idx].map(|v| cmp(v as i64, criterion)).unwrap_or(false))
            .collect(),
        _ => vec![false; indices.len()],
    }
}
