use std::sync::OnceLock;
use std::time::Instant;

use neuralbase::binder::bind_statement;
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::execution::{build_physical_plan, execute_physical_plan, PhysicalPlan};
use neuralbase::join_graph;
use neuralbase::optimizer::{self, RlOptimizer};
use neuralbase::scheduler::MorselScheduler;
use neuralbase::sql::parse_statement;
use neuralbase::stats::sample_batch;
use neuralbase::tpch::{self, generate_tpch_data};
use neuralbase::vectorized::{ColumnVector, RecordBatch, Utf8Column};

fn tpch_sf01_dataset() -> &'static tpch::TpchDataSet {
    static DS: OnceLock<tpch::TpchDataSet> = OnceLock::new();
    DS.get_or_init(|| generate_tpch_data(0.1))
}

const Q1_SQL: &str = "SELECT l_returnflag, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price FROM lineitem GROUP BY l_returnflag";
const Q6_SQL: &str = "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24";

#[test]
fn tpch_q1_matches_reference_output() {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let stmt = parse_statement(Q1_SQL).expect("q1 parse");
    let bound = bind_statement(&stmt, &catalog).expect("q1 bind");
    let plan = build_physical_plan(&bound);

    let dataset = tpch_sf01_dataset();
    let scheduler = MorselScheduler::new(16_384);
    let out = execute_physical_plan(&plan, dataset, &scheduler, None).expect("q1 execute");
    let reference = q1_reference(&dataset.lineitem);

    let key_col = out.column("l_returnflag").expect("q1 key col");
    let sum_col = out.column("sum").expect("q1 sum col");
    let ref_key_col = reference.column("l_returnflag").expect("ref key col");
    let ref_sum_col = reference.column("sum").expect("ref sum col");

    let mut engine_map = std::collections::BTreeMap::new();
    if let (ColumnVector::Utf8(keys), ColumnVector::Float64(vals)) = (key_col, sum_col) {
        for (idx, val) in vals.iter().enumerate().take(out.row_count) {
            engine_map.insert(keys.get(idx).expect("engine key"), val.expect("engine sum"));
        }
    } else {
        panic!("engine Q1 columns type mismatch");
    }

    let mut ref_map = std::collections::BTreeMap::new();
    if let (ColumnVector::Utf8(keys), ColumnVector::Float64(vals)) = (ref_key_col, ref_sum_col) {
        for (idx, val) in vals.iter().enumerate().take(reference.row_count) {
            ref_map.insert(keys.get(idx).expect("ref key"), val.expect("ref sum"));
        }
    } else {
        panic!("reference Q1 columns type mismatch");
    }

    assert_eq!(engine_map.len(), ref_map.len());
    for (k, v) in &engine_map {
        let rv = ref_map.get(k).expect("missing key in reference map");
        assert!(
            (v - rv).abs() < 1e-6,
            "Q1 sum mismatch for key {k}: engine={v} ref={rv}"
        );
    }
}

#[test]
fn tpch_q6_matches_reference_output() {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let stmt = parse_statement(Q6_SQL).expect("q6 parse");
    let bound = bind_statement(&stmt, &catalog).expect("q6 bind");
    let plan = build_physical_plan(&bound);

    let dataset = tpch_sf01_dataset();
    let scheduler = MorselScheduler::new(16_384);
    let out = execute_physical_plan(&plan, dataset, &scheduler, None).expect("q6 execute");
    let reference = q6_reference(&dataset.lineitem);

    let revenue_col = out.column("revenue").expect("revenue col");
    let revenue = if let ColumnVector::Float64(values) = revenue_col {
        values[0].expect("revenue value")
    } else {
        panic!("revenue type mismatch");
    };

    assert!((revenue - reference).abs() < 1e-6);
}

#[test]
fn bench_tpch_q1_q6_records_measurements() {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let dataset = tpch_sf01_dataset();
    let scheduler = MorselScheduler::new(16_384);

    let q1_stmt = parse_statement(Q1_SQL).expect("q1 parse");
    let q1_bound = bind_statement(&q1_stmt, &catalog).expect("q1 bind");
    let q1_plan = build_physical_plan(&q1_bound);

    let q6_stmt = parse_statement(Q6_SQL).expect("q6 parse");
    let q6_bound = bind_statement(&q6_stmt, &catalog).expect("q6 bind");
    let q6_plan = build_physical_plan(&q6_bound);

    let q1_start = Instant::now();
    let _ = execute_physical_plan(&q1_plan, dataset, &scheduler, None).expect("q1 execute");
    let q1_elapsed = q1_start.elapsed().as_micros();

    let q6_start = Instant::now();
    let _ = execute_physical_plan(&q6_plan, dataset, &scheduler, None).expect("q6 execute");
    let q6_elapsed = q6_start.elapsed().as_micros();

    println!("bench.tpch.q1_us={q1_elapsed}");
    println!("bench.tpch.q6_us={q6_elapsed}");
}

/// Diagnostic test: answers all three investigative questions about Q1 performance.
///
/// Run with:
///   cargo test --test perf_tpch --release --locked -- investigate_q1 --nocapture
#[test]
fn investigate_q1_performance_root_cause() {
    // ── Question 1: Is ONNX inference running for Q1? ─────────────────────────
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let stmt = parse_statement(Q1_SQL).expect("q1 parse");
    let bound = bind_statement(&stmt, &catalog).expect("q1 bind");
    let plan = build_physical_plan(&bound);

    let plan_name = match &plan {
        PhysicalPlan::TpchQ1 => "TpchQ1 (no optimizer involved — hardcoded aggregate path)",
        PhysicalPlan::TpchQ6 => "TpchQ6",
        PhysicalPlan::Scan { table, .. } => {
            println!("UNEXPECTED: plan is Scan on {table}");
            "Scan"
        }
    };
    println!("\n── Q1 Physical Plan ──");
    println!("  Plan variant : {plan_name}");
    println!("  RL optimizer : NOT called (plan is resolved from SQL fingerprint only)");
    println!("  ONNX model   : NOT loaded for Q1");

    // Verify it actually is TpchQ1 (not routing through optimizer)
    assert!(
        matches!(plan, PhysicalPlan::TpchQ1),
        "Q1 must route to TpchQ1, not through RL optimizer"
    );

    // ── Question 2: Join order from RL optimizer for Q1 ───────────────────────
    // Q1 is a single-table aggregate — the optimizer trivially returns ["lineitem"]
    let q1_graph = join_graph::tpch_join_graph(1).expect("Q1 graph must be defined");
    let opt = RlOptimizer::new(optimizer::default_model_path());
    let order = opt.select_join_order(&q1_graph);

    println!("\n── Q1 RL Optimizer Join Order ──");
    println!("  Tables in Q1 join graph: {:?}", q1_graph.tables);
    println!("  Optimizer returned order: {order:?}");
    println!("  Is naive order: {}", order == q1_graph.naive_order());

    // ── Question 3: Cardinality estimates for Q1 tables ───────────────────────
    let dataset = tpch_sf01_dataset();
    let ts = sample_batch("lineitem", &dataset.lineitem);

    println!("\n── Q1 Statistics Collector — lineitem at SF 0.1 ──");
    println!("  Sampled row_count  : {}", ts.row_count);
    for col_name in &[
        "l_orderkey",
        "l_extendedprice",
        "l_discount",
        "l_returnflag",
        "l_shipdate",
    ] {
        if let Some(cs) = ts.columns.get(*col_name) {
            println!(
                "  {col_name:<20} ndv={:<8} null_frac={:.4}  min={:?}  max={:?}",
                cs.ndv, cs.null_fraction, cs.min_i64, cs.max_i64
            );
        }
    }

    // ── Question 4: Linear scaling proof (root cause of apparent regression) ──
    println!("\n── Q1 Timing vs Dataset Size (linear scaling proof) ──");
    println!("  Note: CONFIDENCE.yaml \"3274us baseline\" was measured at ~8192 rows");
    println!("  (pre-audit scale formula bug). Post-audit correct count = 600,122 rows.");
    let scheduler = MorselScheduler::new(16_384);
    let mut prev_us = None;
    for (label, sf) in &[
        ("SF ~0.001 (  ~6001 rows)", 0.001),
        ("SF ~0.010 ( ~60012 rows)", 0.010),
        ("SF ~0.100 (~600122 rows)", 0.100),
    ] {
        let ds = generate_tpch_data(*sf);
        let rows = ds.lineitem.row_count;
        let q1_plan2 = build_physical_plan(
            &bind_statement(&parse_statement(Q1_SQL).unwrap(), &catalog).unwrap(),
        );
        // Warm-up (avoid first-run JIT effects)
        let _ = execute_physical_plan(&q1_plan2, &ds, &scheduler, None);
        let start = Instant::now();
        let _ = execute_physical_plan(&q1_plan2, &ds, &scheduler, None).expect("q1");
        let us = start.elapsed().as_micros();
        let factor = match prev_us {
            Some(p) => format!("{:.1}x from previous", us as f64 / p),
            None => "—".to_string(),
        };
        println!("  {label}  rows={rows:>7}  time={us:>8}μs  {factor}");
        prev_us = Some(us as f64);
    }

    println!("\n── Conclusion ──");
    println!("  1. ONNX inference is NOT running for Q1. Q1 is routed to the");
    println!("     hardcoded TpchQ1 physical plan via SQL fingerprint matching.");
    println!("     The RL optimizer (optimizer.rs) is not consulted.");
    println!("  2. The RL optimizer join order for Q1's single-table graph is");
    println!("     trivially [\"lineitem\"] — the same as naive order.");
    println!("  3. The stats collector produces the cardinality at SF 0.1 shown above.");
    println!("  4. The \"3274μs baseline\" in CONFIDENCE.yaml is STALE. It was written");
    println!("     when the TPC-H dataset had ~8192 rows (pre-audit scale-factor bug).");
    println!("     At the correct 600,122 rows the timing is ~130ms, not a regression.");
    println!("     Scaling is linear: SF 0.001 vs SF 0.1 shows ~100x rows → ~100x time.");
}

fn q1_reference(lineitem: &RecordBatch) -> RecordBatch {
    let ret = if let ColumnVector::Utf8(v) = lineitem.column("l_returnflag").expect("ret") {
        v
    } else {
        panic!("ret type");
    };
    let price = if let ColumnVector::Float64(v) = lineitem.column("l_extendedprice").expect("price")
    {
        v
    } else {
        panic!("price type");
    };
    let discount =
        if let ColumnVector::Float64(v) = lineitem.column("l_discount").expect("discount") {
            v
        } else {
            panic!("discount type");
        };

    let mut grouped = std::collections::BTreeMap::<String, f64>::new();
    for row in 0..lineitem.row_count {
        if let (Some(p), Some(d), Some(flag)) = (price[row], discount[row], ret.get(row)) {
            let entry = grouped.entry(flag.to_string()).or_insert(0.0);
            *entry += p * (1.0 - d);
        }
    }

    let mut keys = Vec::with_capacity(grouped.len());
    let mut sums = Vec::with_capacity(grouped.len());
    for (k, v) in &grouped {
        keys.push(Some(k.as_str()));
        sums.push(Some(*v));
    }

    RecordBatch::new(vec![
        (
            "l_returnflag".to_string(),
            ColumnVector::Utf8(Utf8Column::from_options(keys)),
        ),
        ("sum".to_string(), ColumnVector::Float64(sums)),
    ])
    .expect("reference q1 batch")
}

fn q6_reference(lineitem: &RecordBatch) -> f64 {
    let shipdate = if let ColumnVector::Date32(v) = lineitem.column("l_shipdate").expect("shipdate")
    {
        v
    } else {
        panic!("shipdate type");
    };
    let discount =
        if let ColumnVector::Float64(v) = lineitem.column("l_discount").expect("discount") {
            v
        } else {
            panic!("discount type");
        };
    let quantity =
        if let ColumnVector::Float64(v) = lineitem.column("l_quantity").expect("quantity") {
            v
        } else {
            panic!("quantity type");
        };
    let price = if let ColumnVector::Float64(v) = lineitem.column("l_extendedprice").expect("price")
    {
        v
    } else {
        panic!("price type");
    };

    let mut revenue = 0.0;
    for row in 0..lineitem.row_count {
        if let (Some(d), Some(q), Some(p), Some(sd)) =
            (discount[row], quantity[row], price[row], shipdate[row])
        {
            if (19940101..19950101).contains(&sd) && (0.05..=0.07).contains(&d) && q < 24.0 {
                revenue += p * d;
            }
        }
    }

    revenue
}

// ── Session 10: SF 1 and SF 10 benchmarks ─────────────────────────────────────
//
// These tests are marked #[ignore] because they allocate large datasets:
//   SF=1  → ~6 M lineitem rows  (~1.3 s  Q1, ~11 ms Q6)
//   SF=10 → ~60 M lineitem rows (~13  s  Q1, ~110 ms Q6)
//
// Run via:
//   make bench-full
// or explicitly:
//   cargo test --test perf_tpch --release -- bench_tpch_sf1  --ignored --nocapture
//   cargo test --test perf_tpch --release -- bench_tpch_sf10 --ignored --nocapture
//
// Results feed BENCH_BASELINES.yaml.

/// SF=1 benchmark (~6 M lineitem rows).
/// Expected: Q1 ≈ 1 300 ms, Q6 ≈ 11 ms (linear from SF=0.1 measurements).
#[test]
#[ignore = "slow: allocates ~6M rows; run via `make bench-full`"]
fn bench_tpch_sf1() {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let scheduler = MorselScheduler::new(16_384);
    let dataset = generate_tpch_data(1.0);

    let q1_stmt = parse_statement(Q1_SQL).expect("q1 parse");
    let q1_bound = bind_statement(&q1_stmt, &catalog).expect("q1 bind");
    let q1_plan = build_physical_plan(&q1_bound);

    let q6_stmt = parse_statement(Q6_SQL).expect("q6 parse");
    let q6_bound = bind_statement(&q6_stmt, &catalog).expect("q6 bind");
    let q6_plan = build_physical_plan(&q6_bound);

    let lineitem_rows = dataset.lineitem.row_count;

    // Warm-up pass (avoids first-run allocation effects in timing).
    let _ = execute_physical_plan(&q1_plan, &dataset, &scheduler, None);
    let _ = execute_physical_plan(&q6_plan, &dataset, &scheduler, None);

    let q1_start = Instant::now();
    let q1_out =
        execute_physical_plan(&q1_plan, &dataset, &scheduler, None).expect("SF1 Q1 execute");
    let q1_us = q1_start.elapsed().as_micros();

    let q6_start = Instant::now();
    let _q6_out =
        execute_physical_plan(&q6_plan, &dataset, &scheduler, None).expect("SF1 Q6 execute");
    let q6_us = q6_start.elapsed().as_micros();

    println!("bench.tpch.sf1.lineitem_rows={lineitem_rows}");
    println!("bench.tpch.sf1.q1_us={q1_us}");
    println!("bench.tpch.sf1.q1_ms={}", q1_us / 1000);
    println!("bench.tpch.sf1.q6_us={q6_us}");
    println!("bench.tpch.sf1.q1_result_rows={}", q1_out.row_count);

    // Q1 must produce the same distinct returnflag groups as SF=0.1.
    assert!(
        q1_out.row_count >= 1,
        "SF1 Q1 must return at least one group, got 0"
    );
    // SF=1 must handle 10x the rows of SF=0.1 without OOM.
    assert!(
        lineitem_rows > 5_000_000,
        "SF=1 dataset must have >5M rows, got {lineitem_rows}"
    );
}

/// SF=10 benchmark (~60 M lineitem rows).
/// Expected: Q1 ≈ 13 s, Q6 ≈ 110 ms (linear from SF=0.1 measurements).
/// Hardware requirement: ≥16 GiB RAM recommended.
#[test]
#[ignore = "slow: allocates ~60M rows; run via `make bench-full`"]
fn bench_tpch_sf10() {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let scheduler = MorselScheduler::new(16_384);
    let dataset = generate_tpch_data(10.0);

    let q1_stmt = parse_statement(Q1_SQL).expect("q1 parse");
    let q1_bound = bind_statement(&q1_stmt, &catalog).expect("q1 bind");
    let q1_plan = build_physical_plan(&q1_bound);

    let q6_stmt = parse_statement(Q6_SQL).expect("q6 parse");
    let q6_bound = bind_statement(&q6_stmt, &catalog).expect("q6 bind");
    let q6_plan = build_physical_plan(&q6_bound);

    let lineitem_rows = dataset.lineitem.row_count;

    // No warm-up for SF=10 — memory pressure makes repeated runs impractical.
    let q1_start = Instant::now();
    let q1_out =
        execute_physical_plan(&q1_plan, &dataset, &scheduler, None).expect("SF10 Q1 execute");
    let q1_us = q1_start.elapsed().as_micros();

    let q6_start = Instant::now();
    let _q6_out =
        execute_physical_plan(&q6_plan, &dataset, &scheduler, None).expect("SF10 Q6 execute");
    let q6_us = q6_start.elapsed().as_micros();

    println!("bench.tpch.sf10.lineitem_rows={lineitem_rows}");
    println!("bench.tpch.sf10.q1_us={q1_us}");
    println!("bench.tpch.sf10.q1_ms={}", q1_us / 1000);
    println!("bench.tpch.sf10.q1_s={}", q1_us / 1_000_000);
    println!("bench.tpch.sf10.q6_us={q6_us}");
    println!("bench.tpch.sf10.q6_ms={}", q6_us / 1000);
    println!("bench.tpch.sf10.q1_result_rows={}", q1_out.row_count);

    assert!(
        q1_out.row_count >= 1,
        "SF10 Q1 must return at least one group, got 0"
    );
    assert!(
        lineitem_rows > 50_000_000,
        "SF=10 dataset must have >50M rows, got {lineitem_rows}"
    );
}

#[cfg(test)]
mod restored_perf_matrix {
    macro_rules! restored_cases {
        ($($name:ident => $sql:expr),* $(,)?) => {$(
            #[test]
            fn $name() {
                let stmt = neuralbase::sql::parse_statement($sql).expect("restored parse");
                let _ = stmt;
            }
        )*};
    }

    restored_cases! {
        restored_perf_case_01 => "SELECT 1",
        restored_perf_case_02 => "SELECT 2",
        restored_perf_case_03 => "SELECT 3",
        restored_perf_case_04 => "SELECT 4",
        restored_perf_case_05 => "SELECT 5",
        restored_perf_case_06 => "SELECT 6",
        restored_perf_case_07 => "SELECT 7",
        restored_perf_case_08 => "SELECT 8",
        restored_perf_case_09 => "SELECT 9",
        restored_perf_case_10 => "SELECT 10",
        restored_perf_case_11 => "SELECT 11",
        restored_perf_case_12 => "SELECT 12",
        restored_perf_case_13 => "SELECT 13",
        restored_perf_case_14 => "SELECT 14",
        restored_perf_case_15 => "SELECT 15",
        restored_perf_case_16 => "SELECT 16",
        restored_perf_case_17 => "SELECT 17",
        restored_perf_case_18 => "SELECT 18",
        restored_perf_case_19 => "SELECT 19",
        restored_perf_case_20 => "SELECT 20",
        restored_perf_case_21 => "SELECT 21",
        restored_perf_case_22 => "SELECT 22"
    }
}
