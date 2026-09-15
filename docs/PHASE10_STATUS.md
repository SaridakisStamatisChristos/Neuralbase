# Phase 10 status — optimizer, executor and distributed performance

Phase 10 starts from canonical `main` commit `e8a4f1c7f5865c6e04f51f6e0658f0c2c81475db` (Phase-9 merge). Post-merge CI run `#409` / `34943456742` completed successfully on that exact SHA.

## Correctness boundary

Phase 10 may improve performance but must not move any write acknowledgement before quorum commit plus confirmed durable local apply, weaken Phase-6 `Local`/`Leader`/`Linearizable` semantics, weaken membership fencing, or weaken snapshot/backup/PITR publication, restore, archive, compaction, identity, or stale-node guarantees.

## Implemented optimization scope

Phase 10 deliberately selected only evidence-backed query-route optimizations:

1. Simple physical persistent scans bypass unrelated SF=0.1 TPC-H fixture construction while preserving the historical `lineitem`/TPC-H authority and missing-table behavior.
2. General persistent user queries attempt a persistent-only `QueryCatalog` first. Only `TableNotFound` falls back to the historical TPC-H-first route; mixed/TPC-H queries and built-in-name collisions preserve the prior semantics.

No Raft acknowledgement, read-barrier, membership, snapshot, backup, PITR, identity, persistence or durability semantics changed.

## Correctness evidence

The optimization boundary is locked by dedicated tests covering:

- persistent simple-scan routing and limit behavior;
- historical missing-table behavior;
- `lineitem` TPC-H authority;
- persistent-only general-query parity against the historical route;
- pure TPC-H fallback parity;
- mixed persistent/TPC-H fallback parity;
- built-in TPC-H name-collision precedence.

The pre-existing guarded identity/membership integration test was also hardened against legitimate Raft leadership turnover by retrying only explicit transient `not leader` and stale-guard errors within the existing timeout. Production code and correctness assertions were not weakened.

## Final measured code head

Final measured code head before documentation synchronization:

`7064a68bbb2d9d278d31fc6b9217095fa5cda7a9`

Validation on that exact SHA:

- CI `#435` / `34989277430`: **SUCCESS**.
- Phase-10 benchmark `#23` / `34989277495`: **SUCCESS**.
- Benchmark artifact: `phase10-exact-base-head-16-7064a68bbb2d9d278d31fc6b9217095fa5cda7a9` / artifact `10405291673`.

Final same-run endpoint p50 evidence versus canonical base:

- Local persistent read: 61.95 ms → 40.99 ms (~33.8% lower).
- Leader persistent read: 62.99 ms → 41.00 ms (~34.9% lower).
- Linearizable persistent read: 64.02 ms → 41.00 ms (~36.0% lower).
- General persistent self-join: 740.61 ms → 40.99 ms (~94.5% lower).
- Replicated INSERT: 40.98 ms → 41.00 ms (effectively unchanged; no improvement claim).

See `docs/PHASE10_PROFILE.md` for the detailed evidence and caveats.

## Resource posture

The executor retains its existing row/intermediate/join safeguards. Fresh-process SF=0.01 memory characterization on the final measured head records approximately 104 MB high-water growth for sort and 96 MB for aggregate. Phase 10 therefore does not claim general spill-to-disk or fully bounded blocking operators.

## Evidence-driven non-changes

Phase-10 measurements did not justify:

- ReadIndex/lease replacement for strong reads;
- Raft acknowledgement or durability shortcuts;
- group commit/apply batching;
- snapshot compression/streaming changes;
- opportunistic index creation;
- live ONNX/RL optimizer integration;
- hosted-runner latency pass/fail thresholds.

Those remain future work only when reproducible profiling selects them without weakening correctness.

## Closure checklist

- [x] Reproducible exact-base/head benchmark suite.
- [x] Bottlenecks profiled and documented.
- [x] Query optimizations preserve reference/historical correctness.
- [x] Distributed acknowledgement/read semantics preserved.
- [x] Resource behavior characterized without spill/boundedness overclaim.
- [x] Snapshot/backup/PITR correctness scenarios remain green.
- [x] Exact before/after commits and workflow evidence recorded.
- [x] No unsupported production/performance claims.
- [x] Performance profile and Phase-10 status synchronized.
- [x] Final measured code head full CI green.
- [x] Final measured code head exact-base/head benchmark green.
- [ ] Documentation-synchronization head full CI green.
- [ ] Documentation-synchronization head benchmark green.
- [ ] PR #16 marked ready and merged.
- [ ] Resulting `main` SHA recorded.
- [ ] Resulting `main` push CI green.

Phase 10 is **not closed** until the remaining merge/post-merge gates above are complete.
