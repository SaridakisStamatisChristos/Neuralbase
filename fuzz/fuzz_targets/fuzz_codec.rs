// SPDX-License-Identifier: Apache-2.0
// Fuzz target: NB v2 binary row codec (encode + decode).
//
// Feeds arbitrary byte slices to the decoder.
// Must not panic or return unsound data on any input.
//
// Run (Linux/macOS only, requires nightly):
//   cargo +nightly fuzz run fuzz_codec -- -max_len=65536 -runs=1000000

#![no_main]
use libfuzzer_sys::fuzz_target;
use neuralbase::codec::decode_batch;

fuzz_target!(|data: &[u8]| {
    // The decoder must handle arbitrary bytes gracefully:
    // - Return None for invalid magic / truncated input.
    // - Never panic, index out of bounds, or invoke UB.
    let _ = decode_batch(data);
});
