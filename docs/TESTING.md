# Testing and evidence

NeuralBase separates evidence types so that a green test suite is not interpreted more broadly than it should be.

## Local commands

```bash
make test
make lint
make confidence
make adversarial
make tpch-correctness
make bench
```

## `make test`

Runs the core Rust integration/unit suite with TLS features enabled where required. It covers SQL, storage, MVCC, authentication, Raft, transport, replicated mutation/state-machine behavior, and the separate-process cluster harness.

The PostgreSQL TPC-H reference harness is deliberately separate because it starts an external PostgreSQL Docker container.

## Replicated SQL evidence

The replicated mutation path is not justified by unit tests alone.

Focused tests cover:

- deterministic command encoding/versioning/canonical ordering;
- leader-side concrete UPDATE/DELETE materialization;
- follower rejection without local mutation;
- quorum commit + confirmed apply before client success;
- apply failure returning no success;
- deterministic/idempotent RocksDB state-machine replay;
- durable HLC/apply-marker recovery;
- injected Raft persistence load/save failure;
- rejection of unsafe legacy snapshot/compaction state in replicated-SQL mode.

`tests/replicated_sql_process.rs` adds the process boundary. It starts three real `neuralbase` binaries with separate PostgreSQL/Raft/metrics ports and separate RocksDB directories. The test performs table CREATE/INSERT/UPDATE/DELETE, checks convergence, kills the elected leader, writes through a newly elected leader, restarts/catches up the killed node, fully restarts the cluster, and races a PostgreSQL mutation against leader kill.

The crash-race assertion is intentionally one-way: if the client observed success, the effect must remain recoverable from the surviving quorum. If the client receives an error or timeout after submission, the outcome is treated as uncertain rather than automatically retried.

## `make lint`

Runs rustfmt and Clippy with warnings denied. Lint-green is code-quality evidence, not functional correctness evidence.

## `make confidence`

Validates `CONFIDENCE.yaml` and its machine-readable claim boundaries.

The gate now permits `distributed_sql_replication: true` only with an explicit narrow scope: fixed membership, persistent table CREATE/DROP/INSERT/UPDATE/DELETE, follower-write rejection, no linearizable follower-read claim, no auth replication, no dynamic membership, no SQL snapshots, and no production-HA claim. `production_ready` remains required to be false.

## `make adversarial`

Exercises focused edge/failure suites across vectorized execution, optimizer behavior, MVCC, and Raft. Adversarial coverage is especially important for lifecycle races, bounded resources, consensus corner cases, and malformed inputs.

## TPC-H PostgreSQL reference suite

`make tpch-correctness` runs `tests/tpch_correctness.rs` with the opt-in `tpch-reference-tests` feature. The harness starts PostgreSQL 16 in Docker and compares NeuralBase output to reference output for the checked-in Q1-Q22 queries on a deterministic small dataset.

The CI job is separate, time-bounded, emits PostgreSQL diagnostics on failure, and always attempts cleanup.

This evidence supports the exact tested SQL/data combinations. It does not establish official TPC-H compliance or arbitrary-query PostgreSQL equivalence.

## Raft transport and persistence evidence

`tests/raft_tcp_transport.rs` verifies logical Raft IDs route over explicit loopback TCP addresses.

Raft lifecycle tests cover bounded apply-channel behavior and interruptible shutdown. Confirmed-apply tests require client success to wait for state-machine completion.

`tests/raft_persistence_fail_closed.rs` injects stable-storage failures directly into `RaftNode` and requires startup/mid-command fail-stop behavior rather than log-and-continue semantics.

## Snapshot safety evidence

`tests/replicated_sql_snapshot_guard.rs` protects an intentional current limitation: legacy opaque Raft compaction/snapshot state is unsafe for SQL recovery, so replicated-SQL mode rejects it. Passing this test is evidence of fail-closed behavior, **not** evidence that SQL-aware snapshot/bootstrap is implemented.

## Deployment-manifest gate

CI performs:

- Helm lint;
- default chart rendering;
- auth-enabled rendering;
- TLS-enabled rendering;
- explicit rejection of unsafe fixed-membership HPA configuration.

A successful render proves template consistency for the checked configuration. It does not prove live Kubernetes upgrade/failover, membership reconfiguration, or disaster recovery.

## Time bounds

Long-running CI commands are intentionally bounded. A deadlock should fail with diagnostics rather than consume a runner indefinitely. Timeouts are a diagnostic safety net, not a substitute for fixing deterministic hangs.

## Benchmarks

Benchmark results are meaningful only with exact commit, workload/scale, hardware/runtime, build profile/features, and warmup/repetition methodology. Do not generalize repository-specific results beyond their measured context.

## What green CI means

Green CI means the exact checked commit passed the repository's current executable gates.

For the replicated-table path, it supports the tested fixed-membership guarantees described above. It still does **not** mean:

- production readiness or general SQL HA;
- complete PostgreSQL compatibility;
- linearizable arbitrary-follower reads;
- replicated authentication state;
- SQL-aware snapshot/bootstrap or node replacement;
- coordinated dynamic membership;
- arbitrary partition/storage-corruption consistency;
- security certification;
- performance superiority outside measured workloads.
