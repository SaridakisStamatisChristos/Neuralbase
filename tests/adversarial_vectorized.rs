use neuralbase::scheduler::MorselScheduler;
use std::sync::OnceLock;
use neuralbase::tpch::{self, generate_tpch_data};
use neuralbase::vectorized::{self,
    filter, i64_mask_auto, i64_mask_scalar, sort_merge_join, ColumnVector, ComparisonOp, ExecError,
    Predicate, RecordBatch,
};

fn tpch_sf01_dataset() -> &'static tpch::TpchDataSet {
    static DS: OnceLock<tpch::TpchDataSet> = OnceLock::new();
    DS.get_or_init(|| generate_tpch_data(0.1))
}

#[test]
fn empty_input_batch_no_panic() {
    let batch = RecordBatch::new(vec![(
        "l_orderkey".to_string(),
        ColumnVector::Int64(vec![]),
    )])
    .expect("empty batch should build");

    let out = filter(
        &batch,
        &Predicate::GtI64 {
            column: "l_orderkey".to_string(),
            value: 0,
        },
    )
    .expect("filter should succeed on empty batch");

    assert_eq!(out.row_count, 0);
}

#[test]
fn all_null_column_filtered_to_zero_rows() {
    let batch = RecordBatch::new(vec![(
        "l_orderkey".to_string(),
        ColumnVector::Int64(vec![None, None, None]),
    )])
    .expect("batch should build");

    let out = filter(
        &batch,
        &Predicate::GtI64 {
            column: "l_orderkey".to_string(),
            value: 0,
        },
    )
    .expect("filter should succeed");

    assert_eq!(out.row_count, 0);
}

#[test]
fn single_row_input_is_supported() {
    let batch = RecordBatch::new(vec![(
        "l_orderkey".to_string(),
        ColumnVector::Int64(vec![Some(42)]),
    )])
    .expect("single-row batch should build");

    let out = filter(
        &batch,
        &Predicate::EqI64 {
            column: "l_orderkey".to_string(),
            value: 42,
        },
    )
    .expect("single-row filter should work");

    assert_eq!(out.row_count, 1);
}

#[test]
fn oversized_batch_returns_error() {
    let huge_batch = RecordBatch {
        columns: vec![("l_orderkey".to_string(), ColumnVector::Int64(vec![Some(1)]))],
        row_count: 10_000_001,
    };

    let err = filter(
        &huge_batch,
        &Predicate::EqI64 {
            column: "l_orderkey".to_string(),
            value: 1,
        },
    )
    .expect_err("overflow batch should fail");

    assert_eq!(err, ExecError::BatchOverflow(10_000_001));
}

#[test]
fn mismatched_column_type_returns_error() {
    let batch = RecordBatch::new(vec![(
        "l_discount".to_string(),
        ColumnVector::Float64(vec![Some(0.05), Some(0.06)]),
    )])
    .expect("batch should build");

    let err = filter(
        &batch,
        &Predicate::EqI64 {
            column: "l_discount".to_string(),
            value: 1,
        },
    )
    .expect_err("type mismatch should fail");

    assert_eq!(err, ExecError::ColumnTypeMismatch("l_discount".to_string()));
}

#[test]
fn simd_and_scalar_masks_match_property_style() {
    for seed in 0..128_i64 {
        let mut values = Vec::new();
        for i in 0..512_i64 {
            if (i + seed) % 7 == 0 {
                values.push(None);
            } else {
                values.push(Some(((i * 37 + seed * 13) % 2000) - 1000));
            }
        }
        let criterion = (seed % 200) - 100;
        let scalar = i64_mask_scalar(&values, criterion, ComparisonOp::Gt);
        let auto = i64_mask_auto(&values, criterion, ComparisonOp::Gt);
        assert_eq!(scalar, auto);
    }
}

#[test]
fn scheduler_handles_non_multiple_batch() {
    let dataset = tpch_sf01_dataset();
    let scheduler = MorselScheduler::with_workers(1_023, 2);
    let out = vectorized::run_parallel_filter(
        &dataset.lineitem,
        &Predicate::GtI64 {
            column: "l_orderkey".to_string(),
            value: 0,
        },
        &scheduler,
    )
    .expect("parallel filter should succeed");

    assert_eq!(out.row_count, dataset.lineitem.row_count);
}

#[test]
fn tpch_generator_sf_0_1_is_about_600k_lineitem_rows() {
    let dataset = tpch_sf01_dataset();
    assert_eq!(dataset.lineitem.row_count, 600_122);
}

#[test]
fn sort_merge_join_handles_duplicate_keys_with_cartesian_matches() {
    let left = RecordBatch::new(vec![
        (
            "id".to_string(),
            ColumnVector::Int64(vec![Some(1), Some(1), Some(2)]),
        ),
        (
            "v_left".to_string(),
            ColumnVector::Int64(vec![Some(10), Some(11), Some(20)]),
        ),
    ])
    .expect("left batch");

    let right = RecordBatch::new(vec![
        (
            "id".to_string(),
            ColumnVector::Int64(vec![Some(1), Some(1), Some(3)]),
        ),
        (
            "v_right".to_string(),
            ColumnVector::Int64(vec![Some(100), Some(101), Some(300)]),
        ),
    ])
    .expect("right batch");

    let joined = sort_merge_join(&left, &right, "id", "id").expect("sort-merge join should work");
    assert_eq!(joined.row_count, 4);
}
