// SPDX-License-Identifier: Apache-2.0
// Fuzz target: PostgreSQL wire protocol message parsing.
//
// Feeds arbitrary byte slices to protocol parsing entry points.
// Must not panic on malformed input.
//
// Run (Linux/macOS only, requires nightly):
//   cargo +nightly fuzz run fuzz_wire_protocol -- -max_len=65536 -runs=1000000

#![no_main]
use libfuzzer_sys::fuzz_target;
use neuralbase::protocol::{
    parse_message_length, parse_sasl_initial_response, parse_startup_body,
    parse_startup_username,
};

fuzz_target!(|data: &[u8]| {
    // Fuzz startup body parsing.
    let _ = parse_startup_body(data);

    // Fuzz startup username extraction.
    let _ = parse_startup_username(data);

    // Fuzz message length validation.
    if data.len() >= 4 {
        let len = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let _ = parse_message_length(len);
    }

    // Fuzz SASL initial response parsing.
    let _ = parse_sasl_initial_response(data);
});
