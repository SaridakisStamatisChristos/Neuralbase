// SPDX-License-Identifier: Apache-2.0
// Phase 8: reusable PostgreSQL 16 semantic differential matrix.

mod common;

use common::pg16_reference::{assert_matches_postgres, DifferentialCase};

#[test]
fn scalar_semantics_match_postgresql_16() {
    let cases = [
        DifferentialCase {
            name: "integer_literal",
            sql: "SELECT 1 AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "integer_arithmetic",
            sql: "SELECT 2 + 3 AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "boolean_literal",
            sql: "SELECT TRUE AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "text_literal",
            sql: "SELECT 'neuralbase' AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "text_equality_ascii",
            sql: "SELECT 'abc' = 'abc' AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "text_upper_ascii",
            sql: "SELECT UPPER('neuralbase') AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "is_null",
            sql: "SELECT NULL IS NULL AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "is_not_null",
            sql: "SELECT 7 IS NOT NULL AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "coalesce",
            sql: "SELECT COALESCE(NULL, 7) AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "nullif_equal",
            sql: "SELECT NULLIF(7, 7) AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "searched_case",
            sql: "SELECT CASE WHEN TRUE THEN 9 ELSE 3 END AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "integer_comparison",
            sql: "SELECT 2 < 3 AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "null_comparison_unknown",
            sql: "SELECT NULL = 1 AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "true_and_unknown",
            sql: "SELECT TRUE AND NULL AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "false_and_unknown",
            sql: "SELECT FALSE AND NULL AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "true_or_unknown",
            sql: "SELECT TRUE OR NULL AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "false_or_unknown",
            sql: "SELECT FALSE OR NULL AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "in_with_null_unknown",
            sql: "SELECT 2 IN (1, NULL) AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "not_in_with_null_unknown",
            sql: "SELECT 2 NOT IN (1, NULL) AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "in_match_beats_null",
            sql: "SELECT 1 IN (1, NULL) AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "explicit_integer_cast",
            sql: "SELECT CAST('42' AS INTEGER) AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "explicit_boolean_cast",
            sql: "SELECT CAST('true' AS BOOLEAN) AS v",
            ordered: true,
        },
        DifferentialCase {
            name: "date_difference_epoch_independent",
            sql: "SELECT DATE '2024-01-02' - DATE '1970-01-01' AS v",
            ordered: true,
        },
    ];

    for case in cases {
        assert_matches_postgres(case);
    }
}

#[test]
fn basic_window_semantics_match_postgresql_16() {
    let cases = [
        DifferentialCase {
            name: "row_number_single_row",
            sql: "SELECT ROW_NUMBER() OVER () AS rn",
            ordered: true,
        },
        DifferentialCase {
            name: "rank_single_row",
            sql: "SELECT RANK() OVER (ORDER BY 1) AS rnk",
            ordered: true,
        },
        DifferentialCase {
            name: "lag_single_row",
            sql: "SELECT LAG(7) OVER () AS previous_value",
            ordered: true,
        },
    ];

    for case in cases {
        assert_matches_postgres(case);
    }
}
