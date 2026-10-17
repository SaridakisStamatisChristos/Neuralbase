# Phase 10 performance profile

This document records the evidence used to select and validate Phase-10 optimization work. Performance changes are selected from reproducible measurements, not source inspection alone.

## Measurement identity

- Canonical Phase-10 base: `e8a4f1c7f5865c6e04f51f6e0658f0c2c81475db` (Phase-9 merge).
- Final measured Phase-10 code head: `7064a68bbb2d9d278d31fc6b9217095fa5cda7a9`.
- Final exact-base/head workflow: `phase10-bench` run `#23` / `34989277495`.
- Final artifact: `phase10-exact-base-head-16-7064a68bbb2d9d278d31fc6b9217095fa5cda7a9`, artifact id `10405291673`.
- Final code-head CI: `#435` / `34989277430`, success.
- Final benchmark runner: Linux 6.17 Azure, 4 vCPU AMD EPYC 9V74, approximately 16 GiB RAM, ext4, rustc 1.88.0.

The workflow preserves the same Phase-10 benchmark harness while checking out the exact canonical base and exact measured head. Hosted-runner measurements are not compared across different workflow runs when making optimization claims; the base/head values below come from the same run.

## Initial bottleneck profile

The initial exact-base profile showed that server query setup dominated the measured persistent-user query path:

- `QueryCatalog::from_tpch` at SF=0.1 was roughly 310 ms p50 in the initial profile.
- `generate_tpch_data(0.1)` was roughly 18 ms p50.
- persistent RocksDB/MVCC scan/decode of 1024 rows was below 1 ms p50.
- parser, binder, physical planning and successful strong-read barriers were microsecond/sub-microsecond scale relative to the server route.

That evidence selected one narrow optimization family: avoid constructing/materializing unrelated synthetic TPC-H fixtures when a query is provably satisfiable from persistent user tables while preserving the historical TPC-H-first fallback semantics.

## Final exact-base/head endpoint evidence

Run `#23` measured the real PostgreSQL-wire endpoint on the exact base and exact Phase-10 code head:

| Endpoint scenario | Base p50 | Head p50 | p50 delta | Base p95 | Head p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Local persistent single-table read | 61.95 ms | 40.99 ms | **-33.8%** | 62.07 ms | 41.04 ms |
| Leader persistent single-table read | 62.99 ms | 41.00 ms | **-34.9%** | 64.05 ms | 41.03 ms |
| Linearizable persistent single-table read | 64.02 ms | 41.00 ms | **-36.0%** | 64.98 ms | 41.04 ms |
| General persistent self-join (`SelectQuery`) | 740.61 ms | 40.99 ms | **-94.5%** | 1302.42 ms | 41.07 ms |
| Replicated INSERT | 40.98 ms | 41.00 ms | +0.05% | 41.06 ms | 41.03 ms |

The two production changes responsible for the read-path improvement are deliberately narrow:

1. simple physical persistent scans can bypass unrelated TPC-H fixture construction while retaining the historical `lineitem`/TPC-H path;
2. general user-only queries first execute against a persistent-only catalog, falling back to the historical TPC-H-first route only on `TableNotFound`.

The replicated write path was not optimized and is effectively unchanged in this run. Phase 10 therefore makes no write-throughput or write-latency improvement claim.

## Correctness-preserving fallback boundary

The persistent-first general-query route is constrained by tests that lock the prior semantics:

- a user-only persistent query can complete without scanning built-in TPC-H tables;
- pure TPC-H queries fall back to the historical catalog and match the reference result;
- mixed persistent + TPC-H queries fall back and match the reference result;
- a persistent table colliding with a built-in TPC-H name does not override the historical TPC-H precedence;
- only `QueryError::TableNotFound(_)` triggers fallback; other query errors remain authoritative and are not re-executed through a broader path.

The simple physical path likewise preserves the historical `lineitem` authority and missing-table behavior.

## Consensus/read-barrier evidence

Phase-10 measurements did not justify changing the Phase-6 strong-read contract. In the final exact-base/head run, successful in-process barriers remained in the tens-of-microseconds range while the real endpoint query route was tens to hundreds of milliseconds before optimization. The stale-former-leader failure scenario remains approximately 301 ms because it intentionally exercises a 300 ms fail-closed timeout.

No acknowledgement boundary, quorum rule, confirmed durable local apply requirement, read-consistency semantic, membership fence, snapshot ordering, backup publication rule or PITR recovery boundary was weakened.

## Memory characterization

Each memory workload runs in a fresh process and records Linux `VmHWM` growth after dataset/catalog setup. Final-head SF=0.01 observations in run `#23` were:

- join: 249,856 bytes;
- sort: 103,940,096 bytes;
- aggregate: 96,325,632 bytes.

The base observations in the same run were approximately 381 KiB join, 103.68 MiB sort and 96.26 MiB aggregate growth. The blocking sort/aggregate shapes therefore remain material resource-consumption areas. Phase 10 does **not** claim spill-to-disk support or generally bounded blocking operators beyond the executor's existing row/join safeguards.

## Lifecycle scenario caveat

The benchmark workflow also executes correctness-backed snapshot/compaction/restart, backup/verify/restore, learner catch-up/promotion and exact-index PITR scenarios. They all completed successfully on exact base and exact head. Their values are scenario wall time, not isolated primitive latency, and are not used to claim lifecycle performance improvements.

## Explicit non-selections

The measured profile did **not** justify changing the following in Phase 10:

- replacing the Phase-6 strong-read mechanism with ReadIndex or leases;
- weakening Raft acknowledgement or confirmed durable local apply semantics;
- introducing group commit/apply batching without an evidence-selected write bottleneck;
- snapshot compression/streaming changes without a measured primitive snapshot bottleneck;
- adding indexes merely to satisfy a performance checklist;
- integrating the ONNX/RL optimizer into the live planner without end-to-end plan-quality evidence;
- hosted-runner pass/fail latency thresholds before variance and hardware stability are characterized.

## Conclusion

Phase 10 found and removed a large, unnecessary TPC-H fixture/materialization cost from persistent user-query routes while preserving the historical query semantics and distributed safety boundaries. The largest measured win is the real endpoint general persistent self-join, from 740.61 ms p50 to 40.99 ms p50 in the final same-run exact-base/head comparison. Writes, consensus safety and recovery semantics were intentionally left unchanged where the measurements did not justify intervention.
