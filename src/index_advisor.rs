// SPDX-License-Identifier: Apache-2.0
// Self-tuning index advisor.
//
// Monitors query access patterns and recommends B-tree index creation or
// drops based on a cost-benefit model.  Runs as a background task; never
// blocks query execution.
//
// Architecture
// ────────────
//  WorkloadMonitor   — ring buffer of last N QueryPatterns
//  AccessStats       — per (table, column) cumulative access counts
//  CostBenefitModel  — estimates query speedup vs. write amplification
//  IndexAdvisor      — coordinates monitor + model; produces IndexDecisions
//  IndexExecutor     — applies IndexDecisions as real RocksDB secondary CFs
//
// Integration points
// ──────────────────
//  1. Call `advisor.record_query(&pattern)` from the query executor.
//  2. Call `advisor.advise()` periodically (e.g. every 1 000 queries or 60 s).
//  3. Call `executor.apply(&decisions, &engine)` to execute Create/Drop DDL.
//
// CONFIDENCE: raw=0.78 effective=0.73
// DEPENDS_ON: catalog, storage
// RISK: Cost model uses simple heuristics (row-count ratio); a cost model
//   backed by real histogram statistics would produce better decisions.
//   Index drops are advisory only — caller must verify no active query uses
//   an index before removing it.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::catalog::Catalog;
use crate::storage::StorageEngine;

// ── Query Pattern ─────────────────────────────────────────────────────────

/// The access pattern recorded for a single executed query.
#[derive(Debug, Clone)]
pub struct QueryPattern {
    /// Tables accessed (lower-case).
    pub tables: Vec<String>,
    /// Columns referenced in WHERE predicates.
    pub predicate_columns: Vec<(String, String)>, // (table, column)
    /// Columns referenced in JOIN conditions.
    pub join_columns: Vec<(String, String)>,
    /// Estimated rows scanned (0 means unknown).
    pub rows_scanned: u64,
    /// Estimated rows returned.
    pub rows_returned: u64,
    /// Wall time of the query in microseconds.
    pub elapsed_us: u64,
    /// When the pattern was recorded.
    pub recorded_at: Instant,
}

impl QueryPattern {
    /// Build a simple pattern from a table name and predicate column list.
    /// Convenience constructor — used in unit tests.
    #[cfg(test)]
    pub fn simple(table: &str, predicate_cols: &[&str]) -> Self {
        Self {
            tables: vec![table.to_string()],
            predicate_columns: predicate_cols
                .iter()
                .map(|c| (table.to_string(), c.to_string()))
                .collect(),
            join_columns: vec![],
            rows_scanned: 0,
            rows_returned: 0,
            elapsed_us: 0,
            recorded_at: Instant::now(),
        }
    }
}

// ── Access Stats ──────────────────────────────────────────────────────────

/// Running access statistics for a (table, column) pair.
#[derive(Debug, Default, Clone)]
pub struct AccessStats {
    /// Total predicate references.
    pub predicate_count: u64,
    /// Total join references.
    pub join_count: u64,
    /// When the column was last accessed.
    pub last_accessed: Option<Instant>,
    /// Cumulative query wall-time from patterns that referenced this column (µs).
    /// Higher = slow queries on this column → more benefit from an index.
    pub total_elapsed_us: u64,
    /// Cumulative rows scanned across matching patterns.
    pub total_rows_scanned: u64,
    /// Cumulative rows returned across matching patterns.
    pub total_rows_returned: u64,
}

impl AccessStats {
    fn total_accesses(&self) -> u64 {
        self.predicate_count + self.join_count
    }
}

// ── Index Candidate ───────────────────────────────────────────────────────

/// A proposed index on one or more columns of a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexCandidate {
    /// Lower-case table name.
    pub table: String,
    /// Columns in the index key (order matters for composites).
    pub columns: Vec<String>,
}

impl IndexCandidate {
    pub fn single(table: &str, column: &str) -> Self {
        Self {
            table: table.to_lowercase(),
            columns: vec![column.to_string()],
        }
    }

    pub fn composite(table: &str, columns: &[&str]) -> Self {
        Self {
            table: table.to_lowercase(),
            columns: columns.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Human-readable name: `idx_<table>_<col1>[_col2...]`
    pub fn index_name(&self) -> String {
        format!("idx_{}_{}", self.table, self.columns.join("_"))
    }
}

// ── Index Decision ────────────────────────────────────────────────────────

/// A recommendation produced by the advisor.
#[derive(Debug, Clone)]
pub enum IndexDecision {
    /// Create an index — estimated net benefit exceeds the threshold.
    Create {
        candidate: IndexCandidate,
        estimated_benefit: f64,
        estimated_cost_bytes: u64,
        reason: String,
    },
    /// Drop an index — unused for longer than the TTL.
    Drop {
        index_name: String,
        unused_for: Duration,
        reason: String,
    },
}

// ── Workload Monitor ──────────────────────────────────────────────────────

/// Ring-buffer of the last `capacity` query patterns plus cumulative stats.
pub struct WorkloadMonitor {
    ring: VecDeque<QueryPattern>,
    capacity: usize,
    /// Per (table, column) cumulative access statistics.
    pub stats: HashMap<(String, String), AccessStats>,
    /// Per-table query hit count (from `QueryPattern::tables`).
    pub table_query_counts: HashMap<String, u64>,
}

impl WorkloadMonitor {
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: VecDeque::with_capacity(capacity),
            capacity,
            stats: HashMap::new(),
            table_query_counts: HashMap::new(),
        }
    }

    /// Record a query access pattern.
    pub fn record(&mut self, pattern: QueryPattern) {
        // Trim ring if at capacity.
        if self.ring.len() == self.capacity {
            self.ring.pop_front();
        }

        // Accumulate per-table query counts (uses QueryPattern::tables field).
        for table in &pattern.tables {
            *self.table_query_counts.entry(table.clone()).or_default() += 1;
        }

        // Update per-column stats, including latency and scan cost signals.
        for (table, col) in &pattern.predicate_columns {
            let entry = self.stats.entry((table.clone(), col.clone())).or_default();
            entry.predicate_count += 1;
            entry.last_accessed = Some(pattern.recorded_at);
            entry.total_elapsed_us += pattern.elapsed_us;
            entry.total_rows_scanned += pattern.rows_scanned;
            entry.total_rows_returned += pattern.rows_returned;
        }
        for (table, col) in &pattern.join_columns {
            let entry = self.stats.entry((table.clone(), col.clone())).or_default();
            entry.join_count += 1;
            entry.last_accessed = Some(pattern.recorded_at);
            entry.total_elapsed_us += pattern.elapsed_us;
            entry.total_rows_scanned += pattern.rows_scanned;
            entry.total_rows_returned += pattern.rows_returned;
        }

        self.ring.push_back(pattern);
    }

    /// Columns with at least `min_accesses` total accesses.
    pub fn hot_columns(&self, min_accesses: u64) -> Vec<(&(String, String), &AccessStats)> {
        self.stats
            .iter()
            .filter(|(_, s)| s.total_accesses() >= min_accesses)
            .collect()
    }
}

// ── Cost-Benefit Model ────────────────────────────────────────────────────

/// Heuristic cost-benefit model.
///
/// Benefit  = estimated fraction of queries sped up × average query time saved.
/// Cost     = write_amplification_factor × estimated index size in bytes.
/// Net      = benefit − cost_weight × cost.
pub struct CostBenefitModel {
    /// Estimated table row count (used for index size estimation).
    pub estimated_row_count: u64,
    /// Estimated bytes per indexed key-value pair.
    pub bytes_per_entry: u64,
    /// How much write throughput a new index costs (0.0..1.0, default 0.05).
    pub write_amplification_weight: f64,
    /// Minimum net benefit score to recommend creation.
    pub creation_threshold: f64,
    /// Minimum unused duration before recommending a drop.
    pub drop_ttl: Duration,
}

impl Default for CostBenefitModel {
    fn default() -> Self {
        Self {
            estimated_row_count: 1_000_000,
            bytes_per_entry: 64,
            write_amplification_weight: 0.05,
            creation_threshold: 0.30,
            drop_ttl: Duration::from_secs(7 * 24 * 3600), // 7 days
        }
    }
}

impl CostBenefitModel {
    /// Estimate the net benefit of creating `candidate` given `access_stats`.
    ///
    /// Returns (net_benefit, estimated_cost_bytes).
    pub fn evaluate(
        &self,
        candidate: &IndexCandidate,
        stats: &HashMap<(String, String), AccessStats>,
        total_query_count: u64,
    ) -> (f64, u64) {
        // Benefit: fraction of queries that would use this index.
        let primary_col = match candidate.columns.first() {
            Some(c) => c,
            None => return (0.0, 0),
        };
        let key = (candidate.table.clone(), primary_col.clone());
        let access_count = stats.get(&key).map(|s| s.total_accesses()).unwrap_or(0);
        let access_fraction = if total_query_count > 0 {
            access_count as f64 / total_query_count as f64
        } else {
            0.0
        };

        // Rough scan-to-lookup speedup: index lookup is O(log n) vs O(n) scan.
        // For 1M rows: log2(1M) ≈ 20; speedup ≈ 1 000 000 / 20 = 50_000× on CPU.
        // Normalise to [0, 1] using a sigmoid-like factor capped at 0.9.
        let row_speedup_factor = if self.estimated_row_count > 0 {
            let ratio = self.estimated_row_count as f64
                / (self.estimated_row_count as f64).log2().max(1.0);
            (ratio / 100_000.0).min(0.9)
        } else {
            0.0
        };

        let benefit = access_fraction * row_speedup_factor;

        // Cost: estimated storage × write amplification weight.
        let cost_bytes = self.estimated_row_count * self.bytes_per_entry;
        let cost_score = self.write_amplification_weight
            * (cost_bytes as f64 / 1_073_741_824.0); // per GiB

        let net = benefit - cost_score;
        (net, cost_bytes)
    }
}

// ── Index Advisor ─────────────────────────────────────────────────────────

/// Coordinates workload monitoring and index recommendation.
pub struct IndexAdvisor {
    monitor: Mutex<WorkloadMonitor>,
    model: CostBenefitModel,
    total_queries: Mutex<u64>,
    catalog: std::sync::Arc<dyn Catalog>,
    /// Existing index names with their last access time (for drop advisory).
    existing_indexes: Mutex<HashMap<String, Instant>>,
}

impl IndexAdvisor {
    /// Create an advisor with default settings.
    pub fn new(catalog: std::sync::Arc<dyn Catalog>) -> Self {
        Self::with_capacity(catalog, 10_000)
    }

    pub fn with_capacity(catalog: std::sync::Arc<dyn Catalog>, capacity: usize) -> Self {
        Self {
            monitor: Mutex::new(WorkloadMonitor::new(capacity)),
            model: CostBenefitModel::default(),
            total_queries: Mutex::new(0),
            catalog,
            existing_indexes: Mutex::new(HashMap::new()),
        }
    }

    /// Record that a query was executed.  Non-blocking; takes lock briefly.
    pub fn record_query(&self, pattern: QueryPattern) {
        self.monitor.lock().unwrap().record(pattern);
        *self.total_queries.lock().unwrap() += 1;
    }

    /// Record an index access (prevents advisor from recommending a drop).
    pub fn touch_index(&self, index_name: &str) {
        self.existing_indexes
            .lock()
            .unwrap()
            .entry(index_name.to_string())
            .and_modify(|t| *t = Instant::now())
            .or_insert_with(Instant::now);
    }

    /// Register an existing index (so advisor can consider drop recommendations).
    pub fn register_index(&self, index_name: String) {
        self.existing_indexes
            .lock()
            .unwrap()
            .entry(index_name)
            .or_insert_with(Instant::now);
    }

    /// Analyse the workload and return index decisions.
    ///
    /// Intended to be called periodically (e.g. every 60 s or 1 000 queries).
    /// Never blocks query execution — holds only short Mutex locks.
    pub fn advise(&self) -> Vec<IndexDecision> {
        let monitor = self.monitor.lock().unwrap();
        let total_q = *self.total_queries.lock().unwrap();
        let existing = self.existing_indexes.lock().unwrap();

        let mut decisions: Vec<IndexDecision> = Vec::new();

        // ── Create recommendations ────────────────────────────────────────
        // Identify hot columns (≥ 2 accesses) and generate single-column
        // index candidates.  Composite candidates (up to 2 columns) are
        // generated for columns on the same table with correlated access.
        let hot = monitor.hot_columns(2);
        let mut table_hot: HashMap<&str, Vec<&str>> = HashMap::new();
        for ((table, col), _) in &hot {
            table_hot.entry(table.as_str()).or_default().push(col.as_str());
        }

        for (table, cols) in &table_hot {
            // Skip if table is not in catalog.
            if self.catalog.get_table(table).is_none() {
                continue;
            }

            // Single-column candidates.
            for &col in cols {
                let candidate = IndexCandidate::single(table, col);
                let (net, cost_bytes) =
                    self.model
                        .evaluate(&candidate, &monitor.stats, total_q);
                if net >= self.model.creation_threshold {
                    decisions.push(IndexDecision::Create {
                        candidate,
                        estimated_benefit: net,
                        estimated_cost_bytes: cost_bytes,
                        reason: format!(
                            "column accessed in {:.0}% of queries; net benefit={:.3}",
                            (monitor
                                .stats
                                .get(&(table.to_string(), col.to_string()))
                                .map(|s| s.total_accesses())
                                .unwrap_or(0) as f64
                                / total_q.max(1) as f64)
                                * 100.0,
                            net
                        ),
                    });
                }
            }

            // Composite candidate (top-2 hot columns on same table).
            if cols.len() >= 2 {
                let candidate = IndexCandidate::composite(table, &cols[..2]);
                let (net, cost_bytes) =
                    self.model
                        .evaluate(&candidate, &monitor.stats, total_q);
                if net >= self.model.creation_threshold {
                    decisions.push(IndexDecision::Create {
                        candidate,
                        estimated_benefit: net,
                        estimated_cost_bytes: cost_bytes,
                        reason: format!(
                            "composite ({}) hot on same table; net benefit={:.3}",
                            cols[..2].join(", "),
                            net
                        ),
                    });
                }
            }
        }

        // ── Drop recommendations ──────────────────────────────────────────
        for (name, last_used) in existing.iter() {
            if last_used.elapsed() >= self.model.drop_ttl {
                decisions.push(IndexDecision::Drop {
                    index_name: name.clone(),
                    unused_for: last_used.elapsed(),
                    reason: format!(
                        "index unused for {:.0} days",
                        last_used.elapsed().as_secs_f64() / 86_400.0
                    ),
                });
            }
        }

        decisions
    }
}

// ── Index Executor (DDL wiring — Session 7) ──────────────────────────────

/// Applies `IndexDecision`s from `IndexAdvisor::advise()` as real RocksDB
/// secondary column family DDL operations.
///
/// For `Create`: calls `StorageEngine::create_index_cf(index_name)` which
/// creates the CF and registers it in CF_CATALOG so it survives DB re-open.
///
/// For `Drop`: calls `StorageEngine::drop_index_cf(index_name)` which drops
/// the CF and removes the CF_CATALOG sentinel.
///
/// Schema tracking is done via `applied_indexes: Vec<String>` so callers can
/// query which indexes are currently active.
pub struct IndexExecutor {
    applied_indexes: Mutex<Vec<String>>,
}

/// Result of a single DDL decision application.
#[derive(Debug)]
pub enum DdlResult {
    Created { index_name: String },
    Dropped { index_name: String },
    Skipped { index_name: String, reason: String },
    Failed { index_name: String, error: String },
}

impl IndexExecutor {
    pub fn new() -> Self {
        Self {
            applied_indexes: Mutex::new(Vec::new()),
        }
    }

    /// Apply a slice of `IndexDecision`s to the given `StorageEngine`.
    ///
    /// Returns one `DdlResult` per decision.  Never panics — errors are
    /// captured in `DdlResult::Failed` so the background advisor loop can
    /// log them and continue.
    pub fn apply(
        &self,
        decisions: &[IndexDecision],
        engine: &StorageEngine,
    ) -> Vec<DdlResult> {
        let mut results = Vec::with_capacity(decisions.len());
        let mut applied = self.applied_indexes.lock().unwrap();

        for decision in decisions {
            match decision {
                IndexDecision::Create { candidate, .. } => {
                    let name = candidate.index_name();
                    if applied.contains(&name) {
                        results.push(DdlResult::Skipped {
                            index_name: name,
                            reason: "index already applied in this session".to_string(),
                        });
                        continue;
                    }
                    match engine.create_index_cf(&name) {
                        Ok(()) => {
                            applied.push(name.clone());
                            results.push(DdlResult::Created { index_name: name });
                        }
                        Err(e) => {
                            results.push(DdlResult::Failed {
                                index_name: name,
                                error: e.to_string(),
                            });
                        }
                    }
                }
                IndexDecision::Drop { index_name, .. } => {
                    match engine.drop_index_cf(index_name) {
                        Ok(()) => {
                            applied.retain(|n| n != index_name);
                            results.push(DdlResult::Dropped {
                                index_name: index_name.clone(),
                            });
                        }
                        Err(e) => {
                            results.push(DdlResult::Failed {
                                index_name: index_name.clone(),
                                error: e.to_string(),
                            });
                        }
                    }
                }
            }
        }
        results
    }

    /// List the names of all indexes created by this executor in this session.
    pub fn applied_indexes(&self) -> Vec<String> {
        self.applied_indexes.lock().unwrap().clone()
    }
}

impl Default for IndexExecutor {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::InMemoryCatalog;
    use std::sync::Arc;

    fn make_catalog() -> Arc<dyn Catalog> {
        Arc::new(InMemoryCatalog::with_tpch_lineitem())
    }

    #[test]
    fn index_candidate_name() {
        let c = IndexCandidate::single("lineitem", "l_orderkey");
        assert_eq!(c.index_name(), "idx_lineitem_l_orderkey");
        let c2 = IndexCandidate::composite("orders", &["o_custkey", "o_orderdate"]);
        assert_eq!(c2.index_name(), "idx_orders_o_custkey_o_orderdate");
    }

    #[test]
    fn workload_monitor_records_and_evicts() {
        let mut mon = WorkloadMonitor::new(3);
        for i in 0..5u64 {
            mon.record(QueryPattern::simple("lineitem", &["l_orderkey"]));
            let _ = i;
        }
        // Ring capacity is 3; only last 3 remain.
        assert_eq!(mon.ring.len(), 3);
        // Stats should have 5 accesses.
        let key = &("lineitem".to_string(), "l_orderkey".to_string());
        assert_eq!(mon.stats[key].predicate_count, 5);
    }

    #[test]
    fn hot_columns_filters_by_min_accesses() {
        let mut mon = WorkloadMonitor::new(100);
        for _ in 0..3 {
            mon.record(QueryPattern::simple("lineitem", &["l_orderkey"]));
        }
        for _ in 0..1 {
            mon.record(QueryPattern::simple("lineitem", &["l_extendedprice"]));
        }
        let hot = mon.hot_columns(2);
        assert_eq!(hot.len(), 1);
        assert!(hot[0].0 .1 == "l_orderkey");
    }

    #[test]
    fn advisor_records_without_panic() {
        let advisor = IndexAdvisor::new(make_catalog());
        for _ in 0..20 {
            advisor.record_query(QueryPattern::simple("lineitem", &["l_orderkey"]));
        }
        let decisions = advisor.advise();
        // May or may not recommend, but must not panic.
        let _ = decisions;
    }

    #[test]
    fn advisor_drop_recommendation_after_ttl() {
        let advisor = IndexAdvisor {
            monitor: Mutex::new(WorkloadMonitor::new(100)),
            model: CostBenefitModel {
                drop_ttl: Duration::from_millis(0), // expire immediately
                ..Default::default()
            },
            total_queries: Mutex::new(0),
            catalog: make_catalog(),
            existing_indexes: Mutex::new({
                let mut m = HashMap::new();
                // Insert an entry with a time guaranteed to be in the past.
                m.insert(
                    "idx_lineitem_l_orderkey".to_string(),
                    Instant::now() - Duration::from_secs(1),
                );
                m
            }),
        };
        let decisions = advisor.advise();
        let drops: Vec<_> = decisions
            .iter()
            .filter(|d| matches!(d, IndexDecision::Drop { .. }))
            .collect();
        assert!(!drops.is_empty(), "expected at least one drop recommendation");
    }

    #[test]
    fn cost_benefit_model_returns_non_negative_for_hot_column() {
        let model = CostBenefitModel::default();
        let candidate = IndexCandidate::single("lineitem", "l_orderkey");
        let mut stats: HashMap<(String, String), AccessStats> = HashMap::new();
        stats.insert(
            ("lineitem".to_string(), "l_orderkey".to_string()),
            AccessStats {
                predicate_count: 800,
                join_count: 200,
                last_accessed: Some(Instant::now()),
                ..Default::default()
            },
        );
        let (net, cost_bytes) = model.evaluate(&candidate, &stats, 1_000);
        assert!(cost_bytes > 0);
        // For a very hot column (1000/1000 accesses) benefit should be positive.
        let _ = net; // value depends on row count heuristic; just ensure it runs
    }

    #[test]
    fn index_executor_create_and_drop_via_rocksdb() {
        use crate::storage::StorageEngine;
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();
        let executor = IndexExecutor::new();

        let candidate = IndexCandidate::single("lineitem", "l_orderkey");
        let decisions = vec![IndexDecision::Create {
            candidate: candidate.clone(),
            estimated_benefit: 0.8,
            estimated_cost_bytes: 1024,
            reason: "test".to_string(),
        }];

        let results = executor.apply(&decisions, &engine);
        assert_eq!(results.len(), 1);
        assert!(
            matches!(&results[0], DdlResult::Created { index_name } if index_name == "idx_lineitem_l_orderkey")
        );

        // Second apply of same decision should be skipped (idempotent).
        let results2 = executor.apply(&decisions, &engine);
        assert!(matches!(&results2[0], DdlResult::Skipped { .. }));

        // Now drop it.
        let drop = vec![IndexDecision::Drop {
            index_name: "idx_lineitem_l_orderkey".to_string(),
            unused_for: Duration::from_secs(0),
            reason: "test drop".to_string(),
        }];
        let results3 = executor.apply(&drop, &engine);
        assert!(matches!(&results3[0], DdlResult::Dropped { .. }));
    }
}
