# NeuralBase roadmap

NeuralBase is a pre-1.0 experimental SQL engine. Fixed-membership replicated persistent table mutations and the SQL-aware snapshot/recovery lifecycle for an existing fixed member are implemented and tested. The next distributed correctness boundary is coordinated membership, followed by identity, operational recovery and stronger read consistency.

This is an engineering roadmap, not a release-date commitment.

## Completed Phase 1 — fixed-membership replicated table mutations

- [x] Versioned deterministic mutation representation for persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE`.
- [x] Canonical DML ordering and deterministic concrete row/key effects.
- [x] Leader-only persistent table mutation routing; followers reject rather than mutate local RocksDB.
- [x] Current-term readiness barrier before mutation binding/materialization after election.
- [x] Leader-side `UPDATE`/`DELETE` predicate evaluation with concrete effects replicated to followers.
- [x] SQL success withheld until Raft quorum commit plus confirmed local state-machine apply.
- [x] Atomic SQL effect + durable apply marker and replay idempotence.
- [x] RocksDB-backed Raft stable storage and fail-stop required persistence handling.
- [x] Separate-process three-node convergence, leader loss/re-election, catch-up, full-cluster restart and acknowledged crash-race durability evidence.

## Completed Phase 2 — SQL-aware snapshot, bootstrap and fixed-member replacement

- [x] Versioned deterministic logical SQL snapshot with explicit magic/version, Raft boundary, SQL apply index, replicated HLC floor, catalog, table IDs, primary keys, exact row bytes, bounds and checksum.
- [x] Canonical table/row ordering and fail-closed corruption/version/duplicate/metadata validation.
- [x] Consistent export from one RocksDB snapshot.
- [x] Atomic durable restore of SQL data/catalog/apply marker with catalog/HLC publication only after success.
- [x] Restore anti-regression for durable SQL apply index and replicated commit timestamp.
- [x] Durable typed snapshot staging before irreversible lifecycle steps.
- [x] Safe Raft compaction only after SQL snapshot creation/validation/staging.
- [x] InstallSnapshot validation/staging and durable SQL restore before successful acknowledgement.
- [x] Crash recovery for interrupted follower snapshot installation.
- [x] Raft suffix retention only when snapshot-boundary index/term matches.
- [x] Repeated snapshot/compaction cycles, retained suffix, restart and continued writes.
- [x] Fresh fixed member starts non-serving while catching up.
- [x] Empty-storage bootstrap of the same already-configured logical member ID using snapshot + remaining Raft suffix.
- [x] Reconstructed member can become leader and acknowledge further writes.
- [x] Acknowledged post-recovery write survives reconstructed-leader failure and restart.
- [x] Legacy opaque snapshot state remains rejected where no SQL-aware snapshot store exists.

Phase 2 does **not** add dynamic membership, automatic replacement orchestration, backup/restore, replicated users, linearizable follower reads or production HA.

## P0 — coordinated membership changes

Fixed peer configuration remains intentional. Replica-count changes are not membership changes.

Acceptance criteria:

- add a learner/non-voting member;
- bootstrap/catch it up using the tested snapshot path;
- promote it safely;
- implement joint old/new configuration or an equivalently correct Raft configuration-change protocol;
- safely remove followers;
- transfer leadership before removing the current leader;
- persist membership configuration durably;
- handle crash/restart during joint configuration;
- reject duplicate node IDs/stale unsafe rejoin;
- test 3 → 4 → 3, failed bootstrap, minority partition and concurrent writes;
- only then reconsider HPA/operator scaling restrictions.

## P0 — replicated identity or explicit strongly consistent identity design

`CREATE USER`, `ALTER USER`, and `DROP USER` remain per-node.

Acceptance criteria for replication:

- deterministic versioned user/auth mutation commands;
- secret-handling semantics that avoid plaintext credentials in consensus logs;
- durable/replay-safe apply;
- cross-node authentication convergence tests;
- documented migration from existing per-node registries.

An external/independent identity design is acceptable only if its consistency and failure semantics are equally explicit.

## P0 — operational recovery

Build operator-facing recovery on top of the verified logical snapshot machinery:

- offline and online consistent backup;
- checksums and backup verification;
- restore into a single node and cluster bootstrap;
- explicit version-compatibility rules;
- interrupted/corrupt backup and restore tests;
- disaster-recovery runbook;
- point-in-time recovery and archived replicated-log/WAL-equivalent stream later.

Snapshot-based fixed-member catch-up is **not** a backup workflow by itself.

## P1 — read consistency modes

Current reads are local; arbitrary follower reads can lag committed state.

Candidate acceptance criteria:

- explicit local/stale mode;
- leader read with authority validation;
- Raft ReadIndex/quorum-barrier or otherwise justified linearizable mode;
- wait for local `last_applied >= read_index` before query execution;
- tests across immediate post-write reads, lag, leader loss, partitions and stale former leaders.

## P1 — SQL semantic depth

Expand SQL without weakening replicated-state safety: richer PostgreSQL type/cast semantics, window functions, DDL/catalog features, transaction protocol behavior, extended wire protocol, NULL/collation/date/time fidelity and differential tests.

## P2 — optimizer and execution performance

Only after lifecycle safety remains intact:

- Raft batching/pipelining and persistent peer connections;
- group commit/apply batching;
- snapshot streaming/compression;
- index access/predicate pushdown;
- cost model calibration, spills and memory accounting;
- reproducible write/read/failover/snapshot throughput/latency measurement.

Performance work must not weaken acknowledgement, snapshot or recovery semantics.

## P2 — production hardening

- authentication/authorization policy beyond the current registry;
- certificate lifecycle/rotation;
- backup encryption and secret management;
- broader network/storage chaos testing;
- upgrade/rollback compatibility;
- supply-chain/security automation with reviewed exceptions;
- production performance characterization.

## Explicit non-goals for the current stage

The project should not optimize for:

- claims of production SQL HA;
- automatic HPA-driven scaling;
- feature-count expansion at the expense of recovery semantics;
- official benchmark certification;
- broad PostgreSQL compatibility claims unsupported by executable evidence.

## Definition of a stronger pre-1.0 distributed milestone

A future milestone suitable for stronger HA/database claims should demonstrate at minimum:

1. the current deterministic/quorum-applied fixed-membership table-mutation guarantees;
2. the current SQL-aware snapshot/bootstrap and fixed-member recovery guarantees;
3. coordinated membership changes;
4. a defined identity/auth consistency model;
5. documented read-consistency modes;
6. tested backup/restore and disaster-recovery procedures;
7. deployment/security assumptions matching the tested topology.
