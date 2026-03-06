# NeuralBase Fuzz Testing

## Requirements

- Rust nightly toolchain (`rustup install nightly`)
- `cargo-fuzz` (`cargo install cargo-fuzz`)
- Linux or macOS (libFuzzer is not available on Windows)

## Targets

| Target | Entry point | Max length |
|---|---|---|
| `fuzz_sql_parser` | `sql::parse_statement`, `sql::parse_nb_statement` | 4096 bytes |
| `fuzz_wire_protocol` | `protocol::parse_*` functions | 65536 bytes |
| `fuzz_codec` | `codec::decode_batch` | 65536 bytes |

## Running

```bash
# Run SQL parser fuzzer for 1M iterations
cargo +nightly fuzz run fuzz_sql_parser -- -max_len=4096 -runs=1000000

# Run wire protocol fuzzer for 1 hour
cargo +nightly fuzz run fuzz_wire_protocol -- -max_len=65536 -max_total_time=3600

# Run codec fuzzer for 1 hour
cargo +nightly fuzz run fuzz_codec -- -max_len=65536 -max_total_time=3600

# List all available fuzz targets
cargo +nightly fuzz list
```

## Crash artifacts

Crashes are saved to `fuzz/artifacts/<target>/`. Reproduce with:

```bash
cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<crash_file>
```

## Platform note

`cargo-fuzz` uses LLVM libFuzzer which is only supported on Linux and macOS.
On Windows, the fuzz harness code compiles but cannot be executed. Use WSL2
or a Linux CI runner for actual fuzz campaigns.
