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

Runs the core Rust integration/unit suite with TLS features enabled where required. It covers SQL, storage, MVCC, authentication, Raft, transport, replicated mutation/state-machine behavior, SQL snapshot codec/manager behavior, snapshot lifecycle, and the separate-process cluster harness.

The PostgreSQL TPC-H reference harness is deliberately separate because it starts an external PostgreSQL Docker container.

## Replicated SQL mutation evidence

Focused tests cover:

- deterministic command encoding/versioning/canonical ordering;
- leader-side concrete UPDATE/DELETE materialization;
- follower rejection without local mutation;
- quorum commit + confirmed apply before client success;
- apply failure returning no success;
- deterministic/idempotent RocksDB replay;
- durable HLC/apply-marker recovery;
- injected Raft persistence load/save failure;
- legacy opaque snapshot rejection when no SQL-aware state-machine snapshot store exists.

`tests/replicated_sql_process.rs` adds the OS-process boundary: three real `neuralbase` binaries with separate PostgreSQL/Raft/metrics ports and separate RocksDB directories exercise mutation convergence, leader loss/re-election, killed-node catch-up, full-cluster restart and a PostgreSQL mutation raced against leader kill.

The crash-race assertion is intentionally one-way: if the client observed success, the effect must remain recoverable from the surviving quorum. An error/timeout after submission remains outcome-uncertain.

## SQL snapshot codec and restore evidence

`src/replicated_snapshot.rs` unit tests cover deterministic/golden encoding, canonical table/row ordering, bad checksum, invalid magic, truncation, unsupported version, duplicate/noncanonical entries, duplicate primary keys, table-ID mismatch, impossible apply metadata and noncanonical wire order.

`src/replicated_snapshot_manager.rs` tests cover:

- export → fresh RocksDB restore → logical/byte equality;
- restart after restore;
- replay idempotence;
- corrupt snapshot leaving the target unchanged;
- injected restore-write failure with no partial publication;
- apply-index and commit-timestamp anti-regression;
- rejection of unreplicated future MVCC versions;
- fail-closed rejection of unsupported secondary-index state.

## Integrated snapshot lifecycle evidence

Phase 2 adds lifecycle tests beyond codec/storage units:

- interrupted InstallSnapshot recovery leaves a durable typed Installation stage; restart revalidates/restores/promotes the exact boundary and clears staging without duplicate SQL effects;
- `tests/replicated_sql_snapshot_cycles.rs` performs repeated SQL-aware snapshot/compaction cycles, adds a retained suffix, restarts from persisted active snapshot + suffix, verifies no duplicate MVCC versions, and continues writing;
- `tests/replicated_sql_snapshot_bootstrap.rs` starts three independent RocksDB/Raft state machines, compacts a leader, destroys one fixed member's entire database directory, writes a post-snapshot suffix while it is absent, restarts the same logical member ID with empty storage, requires snapshot + suffix reconstruction and readiness gating, transfers leadership to the reconstructed member, acknowledges another write there, kills it, verifies that acknowledged write survives on the remaining quorum, then restarts the reconstructed member and verifies exact convergence;
- `tests/replicated_sql_snapshot_process.rs` performs the corresponding replacement lifecycle with three real `neuralbase` child processes and TCP Raft: one stopped member is durably compacted, another member's full RocksDB directory is deleted, the same fixed logical node ID recovers through InstallSnapshot plus retained suffix, the reconstructed disk is restarted, and a later acknowledged mutation survives after the original snapshot-source leader is removed.

This is executable evidence for the checked fixed-member lifecycle. It is not evidence for arbitrary new-ID membership addition, joint consensus, automatic operator replacement, backup restore, PITR or Byzantine/storage-corruption tolerance.

## `make lint`

Runs rustfmt and Clippy with warnings denied. Lint-green is code-quality evidence, not functional correctness evidence.

## `make confidence`

Validates `CONFIDENCE.yaml` and its machine-readable claim boundaries.

The gate permits `distributed_sql_replication: true` only with a narrow scope: fixed membership, persistent table CREATE/DROP/INSERT/UPDATE/DELETE, follower-write rejection, SQL-aware snapshots and empty-storage recovery of an already-configured fixed member, no linearizable follower-read claim, no auth replication, no dynamic membership, and no production-HA claim. `production_ready` remains required to be false.

## `make adversarial`

Exercises focused edge/failure suites across vectorized execution, optimizer behavior, MVCC and Raft. Adversarial coverage is especially important for lifecycle races, bounded resources, consensus corner cases and malformed inputs.

## TPC-H PostgreSQL reference suite

`make tpch-correctness` runs `tests/tpch_correctness.rs` with the opt-in `tpch-reference-tests` feature. The harness starts PostgreSQL 16 in Docker and compares NeuralBase output to reference output for the checked-in Q1-Q22 queries on a deterministic small dataset.

This supports the exact tested SQL/data combinations; it does not establish official TPC-H compliance or arbitrary-query PostgreSQL equivalence.

## Raft transport and persistence evidence

`tests/raft_tcp_transport.rs` verifies logical Raft IDs route over explicit loopback TCP addresses.

`tests/raft_persistence_fail_closed.rs` injects stable-storage failures directly into `RaftNode` and requires fail-stop behavior. Snapshot persistence tests also cover staged Creation/Installation transitions and fatal staging/load/clear failures.

## Snapshot safety evidence

`tests/replicated_sql_snapshot_guard.rs` remains a legacy-boundary test: confirmed SQL apply **without** a SQL-aware snapshot store must still reject opaque legacy compaction/persisted snapshot state. Phase 2 does not reinterpret arbitrary opaque Raft bytes as SQL state.

The positive SQL-aware path is proven separately by the codec/manager, interrupted-install, repeated-cycle and empty-storage bootstrap tests described above.

## Deployment-manifest gate

CI performs Helm lint, default rendering, auth-enabled rendering, TLS-enabled rendering and explicit rejection of unsafe fixed-membership HPA configuration.

A successful render does not prove live Kubernetes upgrade/failover, dynamic membership or disaster recovery.

## Time bounds

Long-running CI commands are intentionally bounded. A deadlock should fail with diagnostics rather than consume a runner indefinitely. Timeouts are a safety net, not a substitute for fixing deterministic hangs.

## What green CI means

Green CI means the exact checked commit passed the repository's current executable gates.

For the distributed path, it supports the tested fixed-membership mutation and SQL-aware fixed-member snapshot lifecycle described above. It still does **not** mean:

- production readiness or general SQL HA;
- complete PostgreSQL compatibility;
- linearizable arbitrary-follower reads;
- replicated authentication state;
- coordinated dynamic membership or arbitrary new-node addition;
- automatic node replacement;
- backup/restore, PITR or disaster recovery;
- arbitrary partition/storage-corruption consistency;
- security certification;
- performance superiority outside measured workloads.
