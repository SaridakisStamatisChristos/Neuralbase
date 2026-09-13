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
    ];

    for case in cases {
        assert_matches_postgres(case);
    }
}
