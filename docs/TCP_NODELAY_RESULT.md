# Server TCP_NODELAY endpoint evidence

## Scope

This document records the causal A/B experiment that motivated enabling `TCP_NODELAY` on accepted PostgreSQL-wire SQL sockets.

The experiment was run by draft PR #17 against merged Phase-10 `main` commit:

`28fa82541311dbbc40db0ae2ab924c2e6234b638`

The same runner checked out that exact base SHA twice and used the same probe harness for both measurements. The only production-source difference in the second measurement was:

```rust
socket.set_nodelay(true)?;
```

The workflow verified that the ephemeral production diff was exactly one added line in `src/server_parts/prelude.rs`.

Runner: Ubuntu 24.04, Linux 6.17 Azure, 4 vCPU Intel Xeon 6973P-C, approximately 16 GiB RAM, ext4, rustc 1.88.0.

Each metric used 5 warmups and 50 measured iterations through a real NeuralBase process and the PostgreSQL wire protocol.

## Results

| Endpoint operation | Baseline p50 | Server TCP_NODELAY p50 | p50 reduction | Baseline p95 | TCP_NODELAY p95 |
|---|---:|---:|---:|---:|---:|
| `SET neuralbase_read_consistency = 'local'` | 41.004 ms | 0.0338 ms | 99.92% | 41.236 ms | 0.0462 ms |
| persistent Local SELECT | 40.997 ms | 0.0864 ms | 99.79% | 41.238 ms | 0.1006 ms |
| persistent general self-join | 41.002 ms | 0.1109 ms | 99.73% | 41.274 ms | 0.1290 ms |
| persistent Leader SELECT | 40.993 ms | 0.1395 ms | 99.66% | 41.275 ms | 0.1555 ms |
| persistent Linearizable SELECT | 40.980 ms | 0.1761 ms | 99.57% | 41.300 ms | 0.1925 ms |
| replicated INSERT | 40.998 ms | 0.2912 ms | 99.29% | 41.310 ms | 0.3674 ms |

The general self-join NODELAY run had one 12.33 ms maximum outlier, while its p50 and p95 remained 0.111 ms and 0.129 ms respectively. No latency threshold is inferred from that isolated maximum.

## Interpretation

The near-zero-work `SET` control is the strongest discriminator: its p50 fell from approximately 41 ms to approximately 34 microseconds without changing query execution, Raft, storage, or acknowledgement ordering. The other endpoint paths lost the same approximately 40 ms floor.

This provides direct NeuralBase evidence that the observed floor was transport-induced rather than intrinsic query-engine or consensus latency for these single-voter measurements. It is consistent with the previously reproduced Nagle/delayed-ACK interaction around the separately written final PostgreSQL `ReadyForQuery` message.

The change does **not** establish multi-node distributed latency. Three-process durable-voter benchmarks remain necessary before making competitive distributed-database latency claims.

## Semantics

Enabling `TCP_NODELAY` changes TCP packetization behavior only. It does not alter:

- SQL execution order;
- PostgreSQL message order;
- Raft quorum requirements;
- confirmed local apply requirements;
- strong-read consistency semantics;
- backup, snapshot, or PITR behavior;
- durability acknowledgement boundaries.
