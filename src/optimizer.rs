//! RL join-order optimizer: loads the trained ONNX model and returns the
//! estimated best join order for a given join graph.
//!
//! State encoding (must match optimizer/training/train.py exactly):
//!   [0..64)  8×8 matrix (row-major, global TPC-H indices):
//!            off-diagonal A[i,j] = clamp(-log10(sel(i,j)) / LOG_NORM, 0, 1)
//!            diagonal     A[i,i] = (join_position + 1) / MAX_TABLES  (0 if not joined)
//!   [64..72) log10(rows) / LOG_NORM for present tables (0.0 for absent)
//!
//! Inference is step-by-step: the model is loaded once per query, then called
//! once per table to add, updating the diagonal after each pick.
//!
//! Falls back to naive left-deep ordering when:
//!   - The ONNX model file does not exist.
//!   - Total inference time exceeds `timeout_ms` milliseconds.
//!   - Any table in the query is not a known TPC-H table.
//!   - The join graph has a cycle.
//!   - Any tract error occurs.
//!
//! CONFIDENCE: raw=0.73 effective=0.68
//! DEPENDS_ON: join_graph, cost_model


use crate::join_graph::JoinGraph;
use std::sync::OnceLock;
use std::time::Instant;
use tract_onnx::prelude::*;

// ── Type alias ────────────────────────────────────────────────────────────────

type OnnxRunnable = SimplePlan<TypedFact, Box<dyn TypedOp>, Graph<TypedFact, Box<dyn TypedOp>>>;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Maximum number of tables represented in the state vector (= ACTION_DIM).
pub const MAX_TABLES: usize = 8;
/// Full state: 8×8 matrix + 8 cardinality features.
pub const STATE_DIM: usize = MAX_TABLES * MAX_TABLES + MAX_TABLES;
/// Number of actions (global TPC-H table slots).
pub const ACTION_DIM: usize = MAX_TABLES;
/// Default timeout covering model load + up to 8 inference steps.
pub const DEFAULT_TIMEOUT_MS: u64 = 500;
/// log₁₀ denominator used to normalise row counts and selectivity into [0, 1].
const LOG_NORM: f64 = 6.0;
/// Default join selectivity when no FK relationship is known.
const DEFAULT_SEL: f64 = 0.01;

// ── TPC-H global table registry ───────────────────────────────────────────────
//
// Order MUST match train.py TPCH_TABLE_LIST exactly:
//   lineitem=0, orders=1, customer=2, supplier=3,
//   part=4, partsupp=5, nation=6, region=7

const TPCH_TABLE_NAMES: [&str; MAX_TABLES] = [
    "lineitem", "orders", "customer", "supplier",
    "part", "partsupp", "nation", "region",
];

const TPCH_TABLE_ROWS: [u64; MAX_TABLES] = [
    600_122, 150_000, 15_000, 1_000, 20_000, 80_000, 25, 5,
];

/// FK selectivity pairs: (global_idx_a, global_idx_b, selectivity).
/// Values MUST match train.py SELECTIVITY dict exactly — bench formula:
///   sel = 1 / max(NDV_left, NDV_right), NDV defaults to row_count.
const TPCH_FK_SEL: &[(usize, usize, f64)] = &[
    (1, 0, 1.666_304_755e-6),  // orders(1)   ↔ lineitem(0):   1/600122
    (2, 1, 6.666_666_667e-6),  // customer(2) ↔ orders(1):     1/150000
    (6, 3, 1.0e-3),            // nation(6)   ↔ supplier(3):   1/1000
    (6, 2, 6.666_666_667e-5),  // nation(6)   ↔ customer(2):   1/15000
    (7, 6, 4.0e-2),            // region(7)   ↔ nation(6):     1/25
    (4, 5, 1.25e-5),           // part(4)     ↔ partsupp(5):   1/80000
    (3, 5, 1.25e-5),           // supplier(3) ↔ partsupp(5):   1/80000
    (0, 3, 1.666_304_755e-6),  // lineitem(0) ↔ supplier(3):   1/600122  [+]
    (4, 0, 1.666_304_755e-6),  // part(4)     ↔ lineitem(0):   1/600122  [+]
    (0, 5, 1.666_304_755e-6),  // lineitem(0) ↔ partsupp(5):   1/600122  [+]
];

// ── Precomputed selectivity feature matrix ────────────────────────────────────
//
// sel_features()[i][j] = clamp(-log10(sel(i,j)) / LOG_NORM, 0, 1)
// Diagonal = 0.0 (reserved for join-position feature).
// Computed once; shared across all queries.

static SEL_FEAT: OnceLock<[[f32; MAX_TABLES]; MAX_TABLES]> = OnceLock::new();

fn sel_features() -> &'static [[f32; MAX_TABLES]; MAX_TABLES] {
    SEL_FEAT.get_or_init(|| {
        let default_feat = (-(DEFAULT_SEL.log10()) / LOG_NORM).clamp(0.0, 1.0) as f32;
        let mut m = [[default_feat; MAX_TABLES]; MAX_TABLES];
        for i in 0..MAX_TABLES {
            m[i][i] = 0.0; // diagonal reserved for join-position feature
        }
        for &(a, b, sel) in TPCH_FK_SEL {
            let feat = (-(sel.log10()) / LOG_NORM).clamp(0.0, 1.0) as f32;
            m[a][b] = feat;
            m[b][a] = feat;
        }
        m
    })
}

/// Map a table name to its global TPC-H index, or `None` if unknown.
fn tpch_global_idx(table: &str) -> Option<usize> {
    TPCH_TABLE_NAMES.iter().position(|&t| t == table)
}

// ── Optimizer ─────────────────────────────────────────────────────────────────

/// ONNX-backed RL join-order optimizer with automatic naive fallback.
pub struct RlOptimizer {
    /// Path to the `.onnx` model file.
    pub model_path: String,
    /// Hard timeout for a single inference call (ms).
    pub timeout_ms: u64,
}

impl RlOptimizer {
    pub fn new(model_path: &str) -> Self {
        Self {
            model_path: model_path.to_string(),
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }

    pub fn with_timeout(model_path: &str, timeout_ms: u64) -> Self {
        Self {
            model_path: model_path.to_string(),
            timeout_ms,
        }
    }

    /// Returns the best join order for `graph`.
    ///
    /// Loads the ONNX model once, then iteratively picks the next table by
    /// querying the DQN with the current state.  Falls back to naive order
    /// on any error or timeout.
    pub fn select_join_order(&self, graph: &JoinGraph) -> Vec<String> {
        if graph.tables.len() <= 1 {
            return graph.naive_order();
        }
        if graph.validate().is_err() {
            return graph.naive_order();
        }

        // Map every table in the query to its global TPC-H index.
        // If any table is unknown (non-TPC-H query), fall back to naive.
        let global_idxs: Option<Vec<usize>> = graph
            .tables
            .iter()
            .map(|t| tpch_global_idx(t))
            .collect();
        let global_idxs = match global_idxs {
            Some(v) => v,
            None => {
                tracing::warn!(
                    model = %self.model_path,
                    "query contains non-TPC-H tables; falling back to heuristic"
                );
                return graph.naive_order();
            }
        };

        let start = Instant::now();

        // Load & optimise the ONNX model once per query — never inside the loop.
        let model = match self.load_model() {
            Some(m) => {
                tracing::info!(model = %self.model_path, "RL model loaded");
                m
            }
            None => {
                tracing::warn!(
                    model = %self.model_path,
                    "RL model not found, falling back to heuristic"
                );
                return graph.naive_order();
            }
        };

        // Build present mask ([MAX_TABLES] booleans by global index).
        let mut present = [false; MAX_TABLES];
        for &gi in &global_idxs {
            present[gi] = true;
        }

        // Step-by-step greedy decode: pick one table per DQN call.
        let n = global_idxs.len();
        let mut joined: Vec<usize> = Vec::with_capacity(n);

        loop {
            if joined.len() == n {
                break;
            }
            if start.elapsed().as_millis() as u64 > self.timeout_ms {
                tracing::warn!(
                    model = %self.model_path,
                    elapsed_ms = start.elapsed().as_millis(),
                    "RL inference timeout; falling back to heuristic"
                );
                return graph.naive_order();
            }

            let state = Self::build_rl_state(&present, &joined);
            let q_values = match Self::run_step_infer(&model, state) {
                Some(q) => q,
                None => {
                    tracing::warn!(
                        model = %self.model_path,
                        "RL inference error; falling back to heuristic"
                    );
                    return graph.naive_order();
                }
            };

            // Pick the valid table (present & not yet joined) with the highest Q-value.
            let action = (0..MAX_TABLES)
                .filter(|&i| present[i] && !joined.contains(&i))
                .max_by(|&a, &b| {
                    q_values[a]
                        .partial_cmp(&q_values[b])
                        .unwrap_or(std::cmp::Ordering::Equal)
                });

            match action {
                Some(t) => joined.push(t),
                None => break,
            }
        }

        // Map global indices back to table names.
        joined
            .iter()
            .map(|&i| TPCH_TABLE_NAMES[i].to_string())
            .collect()
    }

    // ── State encoding ───────────────────────────────────────────────────────

    /// Build the 72-dim RL state vector for a join graph with no tables joined yet.
    ///
    /// Encoding matches train.py `_state()` with an empty `joined` list:
    ///   off-diagonal A[i,j] = selectivity feature (constant for the query)
    ///   diagonal     A[i,i] = 0.0 (no joins yet)
    ///   tail [64..72)        = log-cardinality for present tables, 0 for absent
    pub fn build_state_vector(graph: &JoinGraph) -> Vec<f32> {
        let n = graph.tables.len().min(MAX_TABLES);
        let mut present = [false; MAX_TABLES];

        for table in graph.tables.iter().take(n) {
            if let Some(gi) = tpch_global_idx(table) {
                present[gi] = true;
            } else {
                // Non-TPC-H table: return a zero vector of the correct length.
                // The caller (select_join_order) handles the fallback.
                return vec![0.0f32; STATE_DIM];
            }
        }

        Self::build_rl_state(&present, &[])
    }

    /// Build a 72-dim state given which global tables are present and which
    /// have already been joined (in order).
    fn build_rl_state(present: &[bool; MAX_TABLES], joined: &[usize]) -> Vec<f32> {
        let feat = sel_features();
        let mut state = vec![0.0f32; STATE_DIM];

        // Off-diagonal: constant selectivity graph (same for all episodes).
        for i in 0..MAX_TABLES {
            for j in 0..MAX_TABLES {
                if i != j {
                    state[i * MAX_TABLES + j] = feat[i][j];
                }
            }
        }

        // Diagonal: join-position feature (pos+1) / MAX_TABLES.
        for (pos, &t) in joined.iter().enumerate() {
            state[t * MAX_TABLES + t] = (pos + 1) as f32 / MAX_TABLES as f32;
        }

        // Tail [64..72): log-cardinality for present tables, 0 for absent.
        for i in 0..MAX_TABLES {
            if present[i] {
                let rows = TPCH_TABLE_ROWS[i].max(1) as f64;
                state[MAX_TABLES * MAX_TABLES + i] =
                    (rows.log10() / LOG_NORM).clamp(0.0, 1.0) as f32;
            }
        }

        state
    }

    // ── Model loading ─────────────────────────────────────────────────────────

    /// Load, optimise, and compile the ONNX model.  Returns `None` on any error.
    fn load_model(&self) -> Option<OnnxRunnable> {
        if !std::path::Path::new(&self.model_path).exists() {
            return None;
        }
        tract_onnx::onnx()
            .model_for_path(&self.model_path)
            .ok()?
            .into_optimized()
            .ok()?
            .into_runnable()
            .ok()
    }

    // ── Single-step inference ─────────────────────────────────────────────────

    /// Run one forward pass through the loaded model.
    fn run_step_infer(model: &OnnxRunnable, state: Vec<f32>) -> Option<Vec<f32>> {
        let input_arr =
            tract_ndarray::Array2::from_shape_vec((1, STATE_DIM), state).ok()?;
        let input_tensor: Tensor = input_arr.into();
        let result = model.run(tvec![input_tensor.into()]).ok()?;
        let output_view = result[0].to_array_view::<f32>().ok()?;
        Some(output_view.iter().copied().collect())
    }
}

// ── Convenience function ──────────────────────────────────────────────────────

/// Returns the default seed model path, relative to the workspace root.
pub fn default_model_path() -> &'static str {
    "optimizer/model/neuralbase_optimizer.onnx"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join_graph::{JoinEdge, JoinGraph};
    use std::collections::HashMap;

    fn single_table_graph() -> JoinGraph {
        JoinGraph::new(vec!["lineitem".into()], vec![], HashMap::new())
    }

    fn two_table_graph() -> JoinGraph {
        let stats = JoinGraph::tpch_stats();
        JoinGraph::new(
            vec!["orders".into(), "lineitem".into()],
            vec![JoinEdge::new(
                "orders",
                "o_orderkey",
                "lineitem",
                "l_orderkey",
            )],
            stats,
        )
    }

    #[test]
    fn missing_model_falls_back_to_naive_order() {
        let opt = RlOptimizer::new("optimizer/model/does_not_exist.onnx");
        let g = two_table_graph();
        let order = opt.select_join_order(&g);
        assert_eq!(order, g.naive_order());
    }

    #[test]
    fn single_table_returns_identity() {
        let opt = RlOptimizer::new("optimizer/model/does_not_exist.onnx");
        let g = single_table_graph();
        let order = opt.select_join_order(&g);
        assert_eq!(order, vec!["lineitem".to_string()]);
    }

    #[test]
    fn circular_predicates_fall_back_to_naive() {
        let opt = RlOptimizer::new("optimizer/model/does_not_exist.onnx");
        let g = JoinGraph::new(
            vec!["a".into(), "b".into(), "c".into()],
            vec![
                JoinEdge::new("a", "id", "b", "id"),
                JoinEdge::new("b", "id", "c", "id"),
                JoinEdge::new("c", "id", "a", "id"),
            ],
            HashMap::new(),
        );
        // has_cycle → fallback → naive order preserved
        assert_eq!(opt.select_join_order(&g), g.naive_order());
    }

    #[test]
    fn state_vector_has_correct_dimension() {
        let g = two_table_graph();
        let sv = RlOptimizer::build_state_vector(&g);
        assert_eq!(sv.len(), STATE_DIM);
    }

    #[test]
    fn state_vector_encodes_selectivity_for_known_fk_pair() {
        // orders (global 1) ↔ lineitem (global 0), FK sel = 1/600122 ≈ 1.666e-6
        // (bench formula: 1/max(NDV_left, NDV_right) = 1/max(150000, 600122))
        // Expected feature = clamp(-log10(1/600122) / 6.0, 0, 1) ≈ 0.963
        let g = two_table_graph();
        let sv = RlOptimizer::build_state_vector(&g);
        let expected = (-(TPCH_FK_SEL[0].2.log10()) / LOG_NORM).clamp(0.0, 1.0) as f32;
        // A[lineitem=0, orders=1] = sv[0*8+1] = sv[1]
        assert!(
            (sv[1] - expected).abs() < 1e-5,
            "sv[1] = {} expected selectivity feature ≈ {}",
            sv[1],
            expected
        );
        // A[orders=1, lineitem=0] = sv[1*8+0] = sv[8]
        assert!(
            (sv[8] - expected).abs() < 1e-5,
            "sv[8] = {} expected selectivity feature ≈ {}",
            sv[8],
            expected
        );
        // Diagonal must be 0 (no joins yet)
        assert_eq!(sv[0], 0.0, "A[0,0] diagonal must be 0 before any join");
        assert_eq!(sv[9], 0.0, "A[1,1] diagonal must be 0 before any join");
        // Global cardinality features present for both tables
        assert!(sv[64] > 0.0, "lineitem (global 0) cardinality feature must be set");
        assert!(sv[65] > 0.0, "orders (global 1) cardinality feature must be set");
    }

    #[test]
    fn build_rl_state_updates_diagonal_for_joined_tables() {
        // After joining lineitem (global 0) first, A[0,0] = 1/8 = 0.125
        let present = {
            let mut p = [false; MAX_TABLES];
            p[0] = true; // lineitem
            p[1] = true; // orders
            p
        };
        let joined = vec![0usize]; // lineitem joined at position 0
        let state = RlOptimizer::build_rl_state(&present, &joined);
        let expected_pos = 1.0f32 / MAX_TABLES as f32;
        assert!(
            (state[0 * MAX_TABLES + 0] - expected_pos).abs() < 1e-6,
            "diagonal A[0,0] should be {} after joining at pos 0",
            expected_pos
        );
        // orders not yet joined → diagonal still 0
        assert_eq!(state[1 * MAX_TABLES + 1], 0.0);
    }
}
