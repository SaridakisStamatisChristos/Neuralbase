# TCP_NODELAY causal experiment

This branch does not commit a production transport change. It exists only to run a same-runner A/B experiment against the merged Phase-10 `main` state.

The workflow checks out the exact pull-request base SHA twice and uses the same probe harness for both measurements:

1. **baseline** — unchanged merged source;
2. **server_tcp_nodelay** — the same source tree with exactly one ephemeral edit after the SQL listener accepts a socket:

```rust
socket.set_nodelay(true)?;
```

The workflow verifies that the ephemeral production diff is exactly one added line in `src/server_parts/prelude.rs` before running the patched measurement.

The focused probe uses a real NeuralBase process, real PostgreSQL wire protocol, RocksDB storage, and the normal single-voter replicated mutation gateway. It records 50 measured iterations (after 5 warmups) for:

- `SET neuralbase_read_consistency = 'local'` as a near-zero-engine-work transport control;
- persistent local SELECT;
- persistent general-query self join;
- persistent leader SELECT;
- persistent linearizable SELECT;
- replicated INSERT.

No timing threshold is asserted. The artifact contains raw logs, machine-readable JSONL, the exact ephemeral patch, runner metadata, and a p50/p95 comparison table.

This experiment is intended to answer one narrow question: **does server-side TCP_NODELAY remove the approximately 40 ms PostgreSQL-wire completion floor without changing execution or acknowledgement ordering?**
