# NeuralBase roadmap

NeuralBase is a pre-1.0 experimental SQL engine. The distributed correctness baseline now includes replicated persistent table mutations, SQL-aware snapshot/recovery, coordinated Raft membership changes, strongly consistent replicated SCRAM identity, the tested Phase-5 operator backup/restore/fresh-cluster recovery model, explicit Phase-6 read-consistency modes, and the tested opt-in Phase-7 managed membership reconciliation profile.

This is an engineering roadmap, not a release-date commitment.

## Completed Phase 1 — replicated persistent table mutations

- [x] Versioned deterministic commands for persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE`.
- [x] Canonical DML ordering and leader-side concrete `UPDATE`/`DELETE` materialization.
- [x] Followers reject persistent table mutations rather than mutating local RocksDB.
- [x] Current-term readiness barrier before leader materialization.
- [x] SQL success waits for Raft quorum commit plus confirmed durable local apply.
- [x] Atomic SQL effect + durable apply marker and replay-idempotent recovery.
- [x] RocksDB-backed Raft stable storage with fail-stop required-persistence handling.
- [x] Separate-process convergence, leader loss, catch-up, restart and acknowledged-write durability evidence.

## Completed Phase 2 — SQL-aware snapshot, compaction and recovery

- [x] Versioned deterministic logical SQL snapshot with explicit Raft boundary, apply index, HLC floor, catalog/table state, exact row bytes, bounds and checksum.
- [x] Consistent export from one RocksDB snapshot and fail-closed validation.
- [x] Atomic durable restore before volatile catalog/HLC publication.
- [x] Snapshot creation durably staged before Raft prefix truncation.
- [x] InstallSnapshot restores SQL state before successful acknowledgement.
- [x] Interrupted installation recovery and safe suffix-retention rules.
- [x] Empty-storage reconstruction of an already-configured member from snapshot + remaining Raft suffix.
- [x] Reconstructed-member leadership, acknowledged-write failover/restart, and repeated compaction-cycle evidence.

## Completed Phase 3 — coordinated Raft membership changes

- [x] Explicit learner/non-voting startup and admission.
- [x] Learner catch-up through the existing snapshot/log path before promotion.
- [x] Promotion through joint old/new voter configuration and finalization.
- [x] Safe non-leader voter removal through joint consensus.
- [x] Leadership-transfer constraint before removing the current leader.
- [x] Durable finalized membership that overrides stale process-local bootstrap peers after restart.
- [x] Removed-node tombstoning/stale-disk rejoin protection.
- [x] 3 → 4 → 3 lifecycle tests with writes before, during and after membership changes.

Phase 3 is a consensus capability, not an automatic Kubernetes scaling controller. The checked-in StatefulSet/Helm topology still requires deliberate membership operations; HPA-driven replica changes remain rejected.

## Completed Phase 4 — replicated strongly consistent identity

- [x] Versioned deterministic `NBRI` identity commands for initialize/create/alter/drop.
- [x] Leader-side password derivation before proposal; plaintext passwords are not representable in identity log commands.
- [x] Replicated identity accepts SCRAM verifier material only. PostgreSQL MD5 verifier material is rejected because it is reusable authentication material.
- [x] Identity mutation + durable replicated apply cursor are atomic in one RocksDB `WriteBatch`.
- [x] `CREATE USER`, `ALTER USER`, and `DROP USER` route through the Raft leader and wait for confirmed apply.
- [x] Cluster authentication reads the replicated RocksDB identity registry on every node.
- [x] Identity is included in the SQL-aware snapshot and restored atomically with the rest of replicated state.
- [x] Explicit legacy migration requires `NEURALBASE_IDENTITY_MIGRATION_SHA256` matching the exact selected strict-SCRAM `users.json`; malformed, duplicate, MD5, or digest-mismatched inputs fail closed.
- [x] Restart/replay, three-node convergence, leader-loss rotation/drop, snapshot bootstrap, learner promotion/removal, and real-process PostgreSQL authentication evidence.

Single-node mode deliberately keeps the historical local `users.json` behavior for backward compatibility. The replicated identity guarantee applies to configured clustered mode.

## Completed Phase 5 — operational backup / restore / disaster recovery

- [x] Explicit versioned NBBK logical backup envelope with bounded canonical metadata and integrity validation.
- [x] Offline consistent backup with RocksDB lock enforcement, restrictive staged publication and independent verification.
- [x] Leader-coordinated online consistent backup with exact committed/applied boundary and race/fail-closed evidence.
- [x] Independent verification with corruption/truncation/unsupported-version classification.
- [x] Fresh-target single-node restore preserving SQL/catalog/HLC/apply/replicated identity state.
- [x] Fresh recovery membership generation with historical source-ID tombstones.
- [x] Fresh-cluster rebuild through learner catch-up/promotion, new writes, leader loss and full restart.
- [x] Authenticated NBEC v1 encryption, strict key-file handling, wrong-key/tamper rejection and restrictive permissions.
- [x] Interrupted backup publication and stale restore-stage fail-closed evidence.
- [x] Compatibility/key semantics and operator disaster-recovery runbook.
- [x] Real OS-process backup/restore/auth/write/restart evidence.

Internal Raft snapshot catch-up remains distinct from operator backup. The standalone backup CLI is offline; online backup is currently an in-process coordinator API.

### Later recovery extension — PITR

Archived replicated-log/WAL-equivalent streaming and point-in-time recovery remain separate future work. Phase 5 does not infer PITR from retained Raft logs.

## Completed Phase 6 — explicit read consistency modes

- [x] Backward-compatible session `Local`/stale read mode with no consensus coordination.
- [x] Leader-authoritative mode with explicit current-leader/serving-readiness validation.
- [x] Linearizable leader-path mode implemented through a current-term Raft quorum barrier.
- [x] Strong reads proceed only after the existing client-command path confirms quorum commit and durable local apply through the barrier.
- [x] Strong reads fail closed on followers/recovering nodes and never silently downgrade to `Local`.
- [x] Stale former leader under partition cannot manufacture a successful strong read.
- [x] Tests across immediate post-write reads, concurrent reads/writes, leader loss/transfer, learners and promotion/finalization, recovery readiness, restart, snapshot/backup bootstrap, and real OS-process/TCP execution.
- [x] Strict session setting parser hardened against malformed Unicode boundaries.

The initial implementation deliberately uses one replicated control/log entry per `Leader` or `Linearizable` read. Arbitrary-follower linearizable routing, automatic follower-to-leader forwarding, and lower-overhead ReadIndex/lease optimization are not implemented and are not part of the Phase-6 claim.

## Completed Phase 7 — operator membership orchestration

- [x] Versioned deterministic desired-topology planning with monotonic revisions and immutable incarnation inventory.
- [x] Quorum/apply authority observations and leader/term/generation guards rechecked inside the serialized Raft loop.
- [x] Fresh non-voting learner creation, snapshot/log catch-up gating, joint-consensus promotion, leader transfer and finalized removal.
- [x] Durable managed storage identity, tombstone reuse rejection, retained storage, controller restart/reobservation and bounded retry behavior.
- [x] Independent Linux process adapter with authenticated SQL/SCRAM convergence across scale-out, leader restart, replacement and scale-in.
- [x] Opt-in Kubernetes adapter using one StatefulSet/PVC per incarnation, ownership/UID checks, resource-version guarded replica updates and retained PVCs.
- [x] Real disposable-kind lifecycle covering partial PVC-quota failure, configuration drift, 3→4 expansion, leader pod loss, fresh-identity replacement, 4→3 contraction and SQL/SCRAM convergence.
- [x] Exact PR-head CI #315 passed core tests, lint, confidence, adversarial, PostgreSQL reference, deployment-manifest and managed Kubernetes lifecycle gates on `fb7d099c6248f18b324d8817fb2878885dbe38a8` before claim synchronization.

The managed profile is explicit and opt-in. It does **not** adopt the static Helm/Compose topology, make raw replica-count changes safe, enable HPA, provide rolling-upgrade orchestration, or establish production HA/security readiness. Final milestone closure still requires the synchronized final PR head to pass CI and the merged `main` commit to pass post-merge CI.

See [the operator guide](docs/PHASE7_OPERATOR.md). Phase 8 has not started.

## P1 — SQL semantic depth

Expand SQL without weakening replicated-state safety. Current concrete gaps include SQL transaction blocks, Bind parameter substitution/result formats, constraint enforcement, exact numeric/boolean types, complete statement handling, and fail-closed unsupported DML predicates. Also improve window/frame semantics, DDL/catalog features, NULL/collation/date/time fidelity and differential tests. See [SQL support](docs/SQL_SUPPORT.md#client-and-sql-limitations).

## P2 — optimizer and execution performance

Only after lifecycle safety remains intact:

- Raft batching/pipelining and persistent peer connections;
- group commit/apply batching;
- snapshot streaming/compression;
- optional ReadIndex/lease optimization for the strong-read contract;
- integrate the library ONNX optimizer with the live SQL planner only with end-to-end correctness/performance evidence;
- index access/predicate pushdown;
- cost model calibration, spills and memory accounting;
- reproducible write/read/failover/snapshot throughput/latency measurement.

Performance work must not weaken acknowledgement, read-consistency, snapshot, membership, identity, recovery or managed-reconciliation semantics.

## P2 — production hardening

- authorization policy beyond the current user registry;
- credential freshness on follower authentication and an explicit existing-session revocation policy;
- certificate lifecycle/rotation;
- broader secret-management integration and backup key lifecycle automation;
- broader network/storage chaos testing;
- managed upgrade/rollback compatibility and reconciliation hardening;
- supply-chain/security automation with reviewed exceptions;
- production performance characterization.

## Explicit non-goals for the current stage

The project should not optimize for claims of production SQL HA, arbitrary-follower linearizable reads, automatic HPA-driven scaling, official benchmark certification, or broad PostgreSQL compatibility unsupported by executable evidence.

## Definition of a stronger pre-1.0 distributed milestone

A future milestone suitable for stronger HA/database claims should demonstrate at minimum:

1. the current deterministic/quorum-applied replicated table-mutation guarantees;
2. the current SQL-aware snapshot/recovery guarantees;
3. the current coordinated and managed membership lifecycle;
4. the current replicated SCRAM identity model;
5. the current explicit leader-path `Local`/`Leader`/`Linearizable` read-consistency contract;
6. tested backup/restore and disaster-recovery procedures;
7. deployment/security assumptions matching an operator-tested topology.