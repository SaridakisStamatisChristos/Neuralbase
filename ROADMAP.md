# NeuralBase roadmap

NeuralBase is a pre-1.0 experimental SQL engine. The fixed-membership replicated persistent table-mutation path is now implemented and process-tested; the roadmap therefore moves the P0 boundary from “connect SQL to Raft” to recovery, membership, identity, and read-consistency semantics required before stronger HA claims.

This is an engineering roadmap, not a release-date commitment.

## Completed Phase 1 — fixed-membership replicated table mutations

The following acceptance items are implemented and covered by executable tests:

- [x] Versioned deterministic mutation representation for persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE`.
- [x] Canonical DML ordering and deterministic concrete row/key effects.
- [x] Leader-only persistent table mutation routing; followers reject rather than mutate local RocksDB.
- [x] Current-term readiness barrier before mutation binding/materialization after election.
- [x] Leader-side `UPDATE`/`DELETE` predicate evaluation with concrete effects replicated to followers.
- [x] SQL success withheld until Raft quorum commit plus confirmed local state-machine apply.
- [x] Atomic SQL effect + durable apply marker and replay idempotence.
- [x] RocksDB-backed Raft stable storage.
- [x] Fail-stop handling of required Raft persistence load/save failures, including injected-failure coverage.
- [x] Separate-process three-node convergence, leader-loss/re-election, killed-node catch-up, full-cluster restart, and acknowledged crash-race durability evidence.
- [x] Fail-closed rejection of legacy opaque Raft snapshots/compaction in replicated-SQL mode.

These items justify a scoped fixed-membership table-replication claim. They do **not** justify production-readiness or general SQL-HA claims.

## P0 — SQL-aware snapshot, bootstrap, and node replacement

The current replicated-SQL mode disables legacy opaque Raft compaction because that snapshot format cannot reconstruct SQL/catalog state.

Acceptance criteria:

- define a versioned SQL-aware snapshot containing all replicated catalog/data state required to resume deterministic apply;
- atomically associate snapshot state with the corresponding Raft snapshot index/term;
- restore a fresh/replacement node from snapshot plus remaining log entries;
- prove replay/idempotence around snapshot boundaries;
- allow safe log compaction only after snapshot durability is confirmed;
- separate-process tests for snapshot creation, leader loss, replacement-node bootstrap, and convergence.

## P0 — replicated identity or explicit strongly consistent identity design

`CREATE USER`, `ALTER USER`, and `DROP USER` remain per-node today.

Acceptance criteria for replication:

- deterministic versioned user/auth mutation commands;
- secret-handling semantics that do not expose plaintext credentials in consensus logs;
- durable/replay-safe apply;
- cross-node authentication convergence tests;
- documented migration from existing per-node registries.

An alternative external/independent identity design is acceptable only if its consistency and failure semantics are equally explicit.

## P0 — coordinated membership changes

Fixed peer configuration remains intentional. Replica-count changes are not membership changes.

Acceptance criteria:

- add/remove member through a safe Raft membership protocol;
- new member catch-up before it is considered healthy/voting as appropriate;
- explicit handling of failed/incomplete membership transitions;
- deployment reconciliation that cannot bypass consensus membership;
- safe rollback/retry and operator diagnostics;
- process-level membership-change and restart tests.

## P1 — read consistency modes

Current reads are local; arbitrary follower reads can lag committed state.

Candidate acceptance criteria:

- document available consistency levels;
- add leader/read-index/lease-based path for linearizable reads where claimed;
- make follower/stale-read behavior explicit in the wire/API contract;
- test reads across commit propagation, failover, and partitions.

## P1 — operational recovery

Add documented and tested:

- backup/restore;
- snapshot inspection;
- node replacement;
- disaster recovery;
- upgrade/rollback compatibility;
- integrity verification and operator-facing failure diagnostics.

## P1 — SQL semantic depth

Expand SQL without weakening replicated-state safety. Candidate areas:

- stronger PostgreSQL type/cast compatibility;
- richer window-function coverage;
- broader DDL/catalog semantics;
- transaction syntax/isolation behavior;
- extended wire-protocol coverage;
- systematic NULL/collation/date/time compatibility suites.

Each addition should update `docs/SQL_SUPPORT.md` and include reference/adversarial evidence where appropriate.

## P2 — optimizer and execution performance

- cost model calibration across scale factors;
- spill-aware joins/aggregates;
- memory accounting per operator/query;
- parallel execution scheduling improvements;
- optimizer fallback/explainability when the ONNX policy is unavailable or low-confidence.

Performance work must retain a reproducible benchmark methodology.

## P2 — production hardening

- authentication/authorization policy beyond current credential registry;
- certificate lifecycle and rotation procedures;
- rate-limit/admission-control observability;
- backup encryption and secret-management integration;
- chaos testing across network partitions and storage faults;
- supply-chain/security automation with reviewed exceptions.

## Explicit non-goals for the current stage

The project should not optimize for:

- claims of production SQL HA;
- automatic HPA-driven cluster scaling;
- broad feature-count expansion at the expense of recovery semantics;
- official benchmark certification;
- broad PostgreSQL compatibility claims unsupported by executable evidence.

## Definition of a stronger pre-1.0 distributed milestone

A future milestone suitable for stronger HA/database claims should demonstrate at minimum:

1. the current deterministic/quorum-applied fixed-membership table-mutation guarantees;
2. SQL-aware snapshot/bootstrap and replacement-node recovery;
3. coordinated membership changes;
4. a defined identity/auth consistency model;
5. documented read-consistency semantics;
6. tested backup/restore and disaster-recovery procedures;
7. deployment/security assumptions matching the tested topology.
