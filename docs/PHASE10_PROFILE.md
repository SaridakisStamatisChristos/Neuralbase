# Phase 10 performance profile

This document records the evidence used to select Phase-10 optimization work. Performance changes are not selected from source inspection alone.

## Measurement identity

- Canonical Phase-10 base: `e8a4f1c7f5865c6e04f51f6e0658f0c2c81475db`
- Profiling harness head: `1b67e5678cb36a0af3b97ecfd599cfdc48bcaa8e`
- Successful exact-base/head workflow: `phase10-bench` run `#9` / `34954349558`
- Artifact: `phase10-exact-base-head-16-1b67e5678cb36a0af3b97ecfd599cfdc48bcaa8e`, artifact id `10390382381`
- Runner: Ubuntu 24.04.5, Linux 6.17 Azure kernel, 4 vCPUs, Intel Xeon 6973P-C, approximately 16 GiB RAM, ext4, rustc 1.88.0

The harness source was preserved from the Phase-10 head and copied onto the exact base checkout, so the same benchmark code measured both commits. At this point the Phase-10 head contained benchmark/workflow/documentation changes only; it intentionally contained no production optimization. Base/head timing differences therefore characterize hosted-runner and execution-order noise rather than product improvement.

## Exact-base ranked observations

| Component / scenario | Base p50 | Base p95 | Interpretation |
| --- | ---: | ---: | --- |
| `QueryCatalog::from_tpch` at SF=0.1 | 310.10 ms | 537.13 ms | dominant isolated live-general-query setup cost |
| TPC-H Q1 SF=0.1 vectorized execution | 138.08 ms | 156.19 ms | analytical execution cost; in-memory path |
| real endpoint Leader user-table read | 70.08 ms | 74.00 ms | end-to-end PostgreSQL-wire persistent read |
| real endpoint Linearizable user-table read | 67.02 ms | 69.99 ms | end-to-end PostgreSQL-wire persistent read |
| real endpoint Local user-table read | 64.87 ms | 65.82 ms | end-to-end PostgreSQL-wire persistent read |
| real endpoint replicated insert | 41.00 ms | 41.09 ms | parse/bind/Raft/durable apply/wire |
| TPC-H Q6 SF=0.1 vectorized execution | 35.74 ms | 36.16 ms | analytical execution cost; in-memory path |
| `generate_tpch_data(0.1)` | 18.21 ms | 31.80 ms | unconditional dataset construction cost in current SELECT routes |
| row filter/projection | 11.51 ms | 11.68 ms | row executor at SF=0.001 |
| row aggregate/sort | 11.24 ms | 11.35 ms | row executor at SF=0.001 |
| persistent RocksDB/MVCC scan of 1024 rows | 0.887 ms | 0.906 ms | persistent scan/decode itself is not the dominant endpoint cost |
| row equi-join microcase | 0.040 ms | 0.065 ms | small test shape, not a general join-throughput claim |
| row result materialization | 0.010 ms | 0.014 ms | not a first-order bottleneck in measured shape |
| SQL parser (filter/projection statement) | 0.0088 ms | 0.0093 ms | negligible relative to endpoint execution |
| Leader read barrier | 0.0081 ms | 0.0093 ms | current replicated-control-entry mechanism is not first-order |
| Linearizable read barrier | 0.0078 ms | 0.0095 ms | current replicated-control-entry mechanism is not first-order |
| simple server parser | 0.0028 ms | 0.0030 ms | negligible |
| simple binder | 0.00051 ms | 0.00058 ms | negligible |
| simple physical planning | 0.00013 ms | 0.00013 ms | negligible |

The stale-former-leader strong-read failure measurement is approximately 301 ms because that test intentionally uses a 300 ms fail-closed timeout. It is a safety-boundary characterization, not successful-read latency.

## Memory characterization

Each memory workload runs in a fresh process and records Linux `VmHWM` growth after dataset/catalog setup. Exact-base observations at SF=0.01 were:

- join: 446,464 bytes;
- sort: 103,636,992 bytes;
- aggregate: 96,411,648 bytes.

The sort and aggregate shapes therefore remain material resource-consumption areas. Phase 10 must not introduce unbounded buffering or claim bounded spill behavior that does not exist.

## Lifecycle characterization caveat

The workflow also times existing correctness scenarios for snapshot/compaction/restart, backup/verify/restore, membership learner catch-up/promotion, and PITR. These values are deliberately labeled **scenario wall time**. The base checkout recompiles test binaries after the commit switch while the head often reuses build products, so direct base/head lifecycle timing deltas are contaminated by compilation/cache effects. They are useful for ensuring scenarios complete successfully under the benchmark workflow, but they are not valid primitive before/after performance evidence.

## Bottleneck selection

The evidence selects server query setup/execution before consensus-read optimization:

1. The general `BoundPlan::SelectQuery` route currently constructs an SF=0.1 TPC-H dataset and deep-materializes it into a row-oriented `QueryCatalog` before adding persistent user tables. The isolated catalog conversion is roughly **310 ms p50**, by far the largest measured server component.
2. The simple `BoundPlan::SelectFromTable` route also constructs SF=0.1 TPC-H data even for persistent user-table reads. Dataset construction alone is roughly **18 ms p50** and the real endpoint reads are roughly **65–70 ms p50**.
3. Persistent RocksDB/MVCC scan/decode of 1024 rows is below 1 ms p50 in its isolated shape.
4. Parser, binder, physical planning, result materialization, and current successful strong-read barriers are orders of magnitude smaller than the server route costs.

Therefore Phase 10 will first seek the smallest correctness-preserving way to avoid unnecessary TPC-H fixture construction/materialization for persistent user-only queries. Pure TPC-H behavior, mixed TPC-H/user queries, missing-table behavior, and any existing name-collision/shadowing behavior must remain unchanged. Correctness/reference tests must be added before the production optimization.

## Explicit non-selections

Based on this profile, Phase 10 does **not** currently justify:

- replacing the Phase-6 strong-read mechanism with ReadIndex or leases;
- Raft acknowledgement weakening, asynchronous confirmed-apply shortcuts, or durability changes;
- index creation merely to satisfy the phase checklist;
- optimizer/RL integration into the live route without query-quality evidence;
- snapshot compression or batching changes without a measured snapshot bottleneck;
- performance pass/fail thresholds on hosted-runner timings before variance is characterized.

Those items remain conditional, evidence-driven work rather than mandatory feature count.
