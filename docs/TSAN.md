# ThreadSanitizer / Data Race Detection

## Linux CI (recommended)

Run the full integration test suite under ThreadSanitizer:

```bash
RUSTFLAGS="-Z sanitizer=thread" \
  cargo +nightly test --target x86_64-unknown-linux-gnu \
    --features tls --tests -- --test-threads=1
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
| MVCC (`mvcc.rs`) | `RwLock<BTreeMap>` for active txns | `snapshot_isolation_concurrent_insert_not_visible` |
| GC (`gc.rs`) | Relaxed atomics, background thread | `adversarial_mvcc::gc_*` tests |
| Raft (`consensus/raft.rs`) | `mpsc` channels, `Arc<Mutex>` | `raft_correctness::*`, `adversarial_raft::*` |
| Server (`server.rs`) | `Semaphore`, `AtomicBool`, `Arc<Mutex>` | `per_user_connection_limit_rejects_excess` |
| HLC (`hlc.rs`) | `AtomicU64` with `Ordering::SeqCst` | unit tests in `hlc.rs` |
