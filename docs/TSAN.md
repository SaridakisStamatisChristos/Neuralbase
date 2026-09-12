# ThreadSanitizer / Data Race Detection

## Optional local Linux experiment

The checked-in CI does not run ThreadSanitizer. The following is an optional nightly experiment, not a verified release gate. Native RocksDB/C++ dependencies and Rust standard-library instrumentation need separate consideration; successful Rust instrumentation alone does not establish whole-process race freedom.

```bash
RUSTFLAGS="-Z sanitizer=thread" \
  cargo +nightly test --target x86_64-unknown-linux-gnu \
    --features tls --tests --locked -- --test-threads=1
```

Requirements:

- Rust nightly (`rustup install nightly`)
- Linux x86_64 (`x86_64-unknown-linux-gnu` target)
- ThreadSanitizer libraries (included with nightly toolchain)

## Windows (alternative)

ThreadSanitizer is not available on Windows MSVC targets.
Use the concurrency stress tests instead:

```powershell
cargo test --features tls --tests --locked -- --test-threads=4 concurrent
cargo test --features tls --tests --locked -- --test-threads=4 snapshot_isolation
```

These tests exercise the MVCC, GC, and Raft hot paths under parallel
contention to surface data races through observed behavior (panics,
assertion failures, incorrect results).

## Key race-prone modules

| Module | Concurrency mechanism | Test coverage |
|---|---|---|
| MVCC (`mvcc.rs`) | `Mutex<BTreeSet<u64>>` for active snapshots; serialized commits | `tests/mvcc_correctness.rs`, `tests/adversarial_mvcc.rs` |
| GC (`gc.rs`) | Snapshot-set mutex, relaxed stop flag, background thread | `adversarial_mvcc::gc_*` tests |
| Raft (`consensus/raft.rs`) | `mpsc` channels, `Arc<Mutex>` | `raft_correctness::*`, `adversarial_raft::*` |
| Server (`server.rs`) | `Semaphore`, `AtomicBool`, `Arc<Mutex>` | `per_user_connection_limit_rejects_excess` |
| HLC (`hlc.rs`) | `Mutex<HlcTimestamp>` | unit tests in `hlc.rs` |
