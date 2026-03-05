//! Background statistics collector: samples RecordBatches and maintains
//! per-table column statistics in shared memory.
//!
//! CONFIDENCE: raw=0.74 effective=0.70
//! DEPENDS_ON: join_graph, vectorized
//! RISK: NDV estimation is exact only for the sampled window; large tables
//!       may undercount distinct values.


use crate::join_graph::{ColumnStats, TableStats};
use crate::vectorized::{ColumnVector, RecordBatch};
use std::collections::{HashMap, HashSet};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

// ── Internal message ──────────────────────────────────────────────────────────

struct SampleRequest {
    table_name: String,
    batch: RecordBatch,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Thread-safe, background-sampling column statistics collector.
///
/// Call `submit_sample` from any thread; a dedicated background thread
/// processes the batch asynchronously and updates the shared stats map.
/// Call `snapshot` to get a consistent point-in-time view.
#[derive(Debug, Clone)]
pub struct StatisticsCollector {
    stats: Arc<RwLock<HashMap<String, TableStats>>>,
    sender: mpsc::SyncSender<SampleRequest>,
}

impl StatisticsCollector {
    /// Create a collector and spawn the background processing thread.
    ///
    /// `queue_depth` is the maximum number of pending sample requests before
    /// `submit_sample` blocks.  A value of 64 is sufficient for most workloads.
    pub fn new(queue_depth: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<SampleRequest>(queue_depth);
        let stats: Arc<RwLock<HashMap<String, TableStats>>> = Arc::new(RwLock::new(HashMap::new()));
        let stats_clone = Arc::clone(&stats);

        thread::Builder::new()
            .name("stats-collector".to_string())
            .spawn(move || {
                for req in receiver {
                    let sampled = sample_batch(&req.table_name, &req.batch);
                    if let Ok(mut guard) = stats_clone.write() {
                        merge_stats(
                            guard
                                .entry(req.table_name)
                                .or_insert_with(|| TableStats::new("", 0)),
                            &sampled,
                        );
                    }
                }
            })
            .expect("stats-collector thread should spawn");

        Self { stats, sender }
    }

    /// Non-blocking submit of a batch for background sampling.
    ///
    /// Returns `false` if the queue is full (sample is dropped, not an error).
    pub fn submit_sample(&self, table_name: &str, batch: &RecordBatch) -> bool {
        let req = SampleRequest {
            table_name: table_name.to_string(),
            batch: batch.clone(),
        };
        self.sender.try_send(req).is_ok()
    }

    /// Returns a point-in-time snapshot of all accumulated statistics.
    pub fn snapshot(&self) -> HashMap<String, TableStats> {
        self.stats.read().map(|g| g.clone()).unwrap_or_default()
    }

    /// Returns stats for a specific table, or `None` if not yet sampled.
    pub fn table_stats(&self, table_name: &str) -> Option<TableStats> {
        self.stats
            .read()
            .ok()
            .and_then(|g| g.get(table_name).cloned())
    }
}

// ── Sampling logic ────────────────────────────────────────────────────────────

/// Compute statistics for a single batch.
///
/// For large batches only the first `MAX_SAMPLE_ROWS` rows are scanned
/// for NDV estimation; row_count always reflects the full batch size.
const MAX_SAMPLE_ROWS: usize = 8_192;

pub fn sample_batch(table_name: &str, batch: &RecordBatch) -> TableStats {
    let mut ts = TableStats::new(table_name, batch.row_count as u64);

    for (col_name, col_vec) in &batch.columns {
        let cs = sample_column_vector(col_vec, batch.row_count.min(MAX_SAMPLE_ROWS));
        ts.columns.insert(col_name.clone(), cs);
    }

    ts
}

fn sample_column_vector(col: &ColumnVector, limit: usize) -> ColumnStats {
    match col {
        ColumnVector::Int64(values) => {
            let sample: Vec<i64> = values.iter().take(limit).flatten().copied().collect();
            let null_count = values.iter().take(limit).filter(|v| v.is_none()).count();
            let total = values.len().min(limit);
            let null_fraction = if total == 0 {
                0.0
            } else {
                null_count as f64 / total as f64
            };
            let distinct: HashSet<i64> = sample.iter().copied().collect();
            let min_val = sample.iter().copied().min();
            let max_val = sample.iter().copied().max();
            ColumnStats {
                ndv: distinct.len() as u64,
                null_fraction,
                min_i64: min_val,
                max_i64: max_val,
            }
        }
        ColumnVector::Int32(values) => {
            let sample: Vec<i32> = values.iter().take(limit).flatten().copied().collect();
            let null_count = values.iter().take(limit).filter(|v| v.is_none()).count();
            let total = values.len().min(limit);
            let null_fraction = if total == 0 {
                0.0
            } else {
                null_count as f64 / total as f64
            };
            let distinct: HashSet<i32> = sample.iter().copied().collect();
            let min_val = sample.iter().copied().min().map(|v| v as i64);
            let max_val = sample.iter().copied().max().map(|v| v as i64);
            ColumnStats {
                ndv: distinct.len() as u64,
                null_fraction,
                min_i64: min_val,
                max_i64: max_val,
            }
        }
        ColumnVector::Float64(values) => {
            let null_count = values.iter().take(limit).filter(|v| v.is_none()).count();
            let total = values.len().min(limit);
            let null_fraction = if total == 0 {
                0.0
            } else {
                null_count as f64 / total as f64
            };
            // Approximate NDV for floats via integer-cast bucketing.
            let distinct: HashSet<i64> = values
                .iter()
                .take(limit)
                .flatten()
                .map(|&f| (f * 100.0) as i64)
                .collect();
            ColumnStats {
                ndv: distinct.len() as u64,
                null_fraction,
                min_i64: None,
                max_i64: None,
            }
        }
        ColumnVector::Date32(values) => {
            let null_count = values.iter().take(limit).filter(|v| v.is_none()).count();
            let total = values.len().min(limit);
            let null_fraction = if total == 0 {
                0.0
            } else {
                null_count as f64 / total as f64
            };
            let sample: Vec<i32> = values.iter().take(limit).flatten().copied().collect();
            let distinct: HashSet<i32> = sample.iter().copied().collect();
            ColumnStats {
                ndv: distinct.len() as u64,
                null_fraction,
                min_i64: sample.iter().copied().min().map(|v| v as i64),
                max_i64: sample.iter().copied().max().map(|v| v as i64),
            }
        }
        ColumnVector::Utf8(utf8_col) => {
            let total = limit;
            let mut null_count = 0usize;
            let mut seen: HashSet<String> = HashSet::new();
            for i in 0..total.min(utf8_col.len()) {
                match utf8_col.get(i) {
                    None => null_count += 1,
                    Some(s) => {
                        seen.insert(s.to_string());
                    }
                }
            }
            let null_fraction = if total == 0 {
                0.0
            } else {
                null_count as f64 / total as f64
            };
            ColumnStats {
                ndv: seen.len() as u64,
                null_fraction,
                min_i64: None,
                max_i64: None,
            }
        }
    }
}

/// Merge `new_sample` into an existing `TableStats`, accumulating row counts
/// and updating column statistics with running estimates.
fn merge_stats(existing: &mut TableStats, new_sample: &TableStats) {
    existing.row_count += new_sample.row_count;
    if existing.table_name.is_empty() {
        existing.table_name = new_sample.table_name.clone();
    }
    for (col_name, new_cs) in &new_sample.columns {
        let entry = existing.columns.entry(col_name.clone()).or_default();
        // Keep max NDV observed across all samples.
        entry.ndv = entry.ndv.max(new_cs.ndv);
        // Weighted-average null fraction (use uniform weighting for simplicity).
        entry.null_fraction = (entry.null_fraction + new_cs.null_fraction) / 2.0;
        entry.min_i64 = merge_min(entry.min_i64, new_cs.min_i64);
        entry.max_i64 = merge_max(entry.max_i64, new_cs.max_i64);
    }
}

fn merge_min(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn merge_max(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vectorized::ColumnVector;

    fn make_int64_batch(values: Vec<Option<i64>>) -> RecordBatch {
        let count = values.len();
        RecordBatch {
            columns: vec![("col".to_string(), ColumnVector::Int64(values))],
            row_count: count,
        }
    }

    #[test]
    fn sample_int64_column_stats_are_correct() {
        let batch = make_int64_batch(vec![Some(1), Some(2), Some(3), None, Some(2)]);
        let ts = sample_batch("t", &batch);
        let cs = ts.columns.get("col").expect("column should exist");
        assert_eq!(cs.ndv, 3, "distinct values: 1, 2, 3");
        assert!((cs.null_fraction - 0.2).abs() < 1e-6, "1 null in 5 rows");
        assert_eq!(cs.min_i64, Some(1));
        assert_eq!(cs.max_i64, Some(3));
    }

    #[test]
    fn sample_all_null_column() {
        let batch = make_int64_batch(vec![None, None, None]);
        let ts = sample_batch("t", &batch);
        let cs = ts.columns.get("col").expect("column should exist");
        assert_eq!(cs.ndv, 0);
        assert!((cs.null_fraction - 1.0).abs() < 1e-6);
    }

    #[test]
    fn collector_accumulates_samples_across_batches() {
        let collector = StatisticsCollector::new(16);
        let b1 = make_int64_batch(vec![Some(1), Some(2)]);
        let b2 = make_int64_batch(vec![Some(3), Some(4), Some(5)]);
        collector.submit_sample("orders", &b1);
        collector.submit_sample("orders", &b2);
        // Give the background thread time to process.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let stats = collector.snapshot();
        let ts = stats.get("orders").expect("orders stats should exist");
        // 2 + 3 = 5 rows total
        assert_eq!(ts.row_count, 5);
    }
}
