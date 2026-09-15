# Phase 10 status — optimizer, executor and distributed performance

Phase 10 starts from canonical `main` commit `e8a4f1c7f5865c6e04f51f6e0658f0c2c81475db` (Phase-9 merge). Post-merge CI run `#409` / `34943456742` completed successfully on that exact SHA.

## Correctness boundary

Phase 10 may improve performance but must not move any write acknowledgement before quorum commit plus confirmed durable local apply, weaken Phase-6 `Local`/`Leader`/`Linearizable` semantics, weaken membership fencing, or weaken snapshot/backup/PITR publication, restore, archive, compaction, identity, or stale-node guarantees.

## Benchmarkable path map

| Path | Live/server path? | Persistent storage? | Phase-10 interpretation |
| --- | --- | --- | --- |
| binder + physical executor (`binder`, `execution`) | used for bounded simple/known plans | dataset-dependent | measure separately from row executor |
| vectorized TPC-H (`tests/perf_tpch.rs`) | library execution path | no; in-memory `TpchDataSet` | analytical executor evidence only |
| Phase-8 row executor (`query_executor_phase8` -> legacy executor) | used by general-query routing | catalog materialization is in-memory | general SQL executor evidence |
| `StorageExecutor::scan_table` | used when persistent scanner is configured | yes; MVCC + RocksDB | persistent scan/decode evidence |
| `optimizer::RlOptimizer` | no live server integration | no | library/cost-model evidence only |
| strong-read barrier | yes for `Leader`/`Linearizable` | consensus/durable apply | benchmark success and fail-closed latency independently |
| exchange/distributed query modules | not an active end-to-end distributed SQL service | n/a | never report as live distributed-query throughput |

## Development state

- Slice 10.0 exact baseline audit: complete.
- Slice 10.1 benchmark contract: active.
- Slice 10.2 exact-head capture: pending benchmark harness execution.
- Slice 10.3 profiling: pending baseline capture.
- Optimization slices: blocked until measurement/profiling evidence selects them.

No acknowledgement, read-consistency, snapshot, backup, PITR, membership, identity, or persistence semantics have changed.
