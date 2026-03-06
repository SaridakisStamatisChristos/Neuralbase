// SPDX-License-Identifier: Apache-2.0
// Fuzz target: SQL parser (sqlparser-rs + NeuralBase extensions).
//
// Feeds arbitrary byte slices to the parser entry points.
// Must not panic on any input — only well-formed SQL should succeed.
//
// Run (Linux/macOS only, requires nightly):
//   cargo +nightly fuzz run fuzz_sql_parser -- -max_len=4096 -runs=1000000

#![no_main]
use libfuzzer_sys::fuzz_target;
use neuralbase::sql::{parse_nb_statement, parse_statement};

fuzz_target!(|data: &[u8]| {
    // Only feed valid UTF-8 slices — the parser expects &str.
    if let Ok(sql) = std::str::from_utf8(data) {
        // Standard SQL path.
        let _ = parse_statement(sql);
        // NeuralBase extended statement path (CREATE USER, etc.).
        let _ = parse_nb_statement(sql);
    }
});
