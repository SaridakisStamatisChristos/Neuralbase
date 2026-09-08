# Changelog

All notable user-visible changes to NeuralBase are recorded here.

NeuralBase is currently a **pre-1.0 experimental project**. The crate version is `0.1.0`; no development-phase label should be interpreted as a published stable release.

## [Unreleased]

### Replicated persistent table mutations

- Added versioned deterministic Raft commands for persistent table `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE`.
- Added leader-side concrete `UPDATE`/`DELETE` materialization, follower rejection, current-term readiness barriers, quorum-commit + confirmed-apply acknowledgement, atomic RocksDB apply markers, and fail-stop Raft persistence.
- Added multi-process convergence/failover/restart and acknowledged-write crash-race evidence.

### SQL-aware snapshots and recovery

- Added a versioned canonical logical SQL snapshot with Raft boundary metadata, durable apply/HLC state, catalog/table/row state, bounds and SHA-256 corruption detection.
- Snapshot creation is durably staged before prefix compaction; InstallSnapshot restores durable SQL state before success acknowledgement.
- Added interrupted-install recovery, safe suffix retention, repeated snapshot cycles, and empty-storage reconstruction of an existing logical member from snapshot + suffix.

### Coordinated Raft membership — Phase 3

- Added learner/non-voting startup and admission.
- Added learner catch-up gating before promotion.
- Added joint-consensus voter promotion/removal and finalized membership persistence.
- Added leadership-transfer constraints for leader removal and removed-node/stale-disk protection.
- Added 3 → 4 → 3 lifecycle, finalized-config restart, and stale removed-node regression tests.
- Automatic Kubernetes/HPA scaling remains disabled: the consensus protocol exists, but deployment reconciliation is not automated.

### Replicated identity — Phase 4

- Added versioned deterministic `NBRI` identity commands for initialize/create/alter/drop.
- Clustered `CREATE USER`, `ALTER USER`, and `DROP USER` now route through Raft and wait for quorum commit plus confirmed durable local apply.
- Password derivation happens on the leader before proposal. Replicated commands carry SCRAM verifier material only; plaintext passwords are not representable.
- PostgreSQL MD5 verifier material is rejected from the replicated identity path because it is reusable authentication material.
- Added an authoritative RocksDB identity registry whose mutation and replicated apply cursor are committed atomically.
- Cluster authentication now reads replicated identity state instead of a process-local `users.json` registry.
- Added identity to the SQL-aware snapshot so catch-up, compaction, empty-storage recovery and learner bootstrap preserve authentication state.
- Added strict legacy migration selected by `NEURALBASE_IDENTITY_MIGRATION_SHA256`; malformed, duplicate, MD5, and digest-mismatched registries fail closed.
- Added restart/replay, three-node failover/rotation/drop, membership lifecycle, snapshot-bootstrap and real-process PostgreSQL authentication tests.
- Single-node mode retains the historical local registry for backward compatibility.

### Documentation and deployment

- Synchronized architecture, distributed semantics, SQL support, threat model, testing, roadmap and confidence claims through Phases 3 and 4.
- Helm no longer copies a `users.json` file into each pod PVC as live identity state. An optional legacy source is mounted read-only and paired with an explicit SHA-256 migration authorization.
- Added CI rendering coverage for the identity-migration Helm path and rejection of incomplete migration configuration.

### Important remaining boundaries

- Reads remain local; arbitrary follower reads are not claimed linearizable.
- Membership operations are not automatically reconciled by the checked-in Kubernetes/Helm deployment; HPA remains rejected.
- Backup/restore, PITR and disaster-recovery workflows are not implemented by the internal snapshot catch-up path.
- Authorization remains intentionally limited compared with a production database security model.
- These changes do **not** make NeuralBase production-ready or justify a general production-HA claim.

### CI maintenance

- CI gates core tests, rustfmt/Clippy with warnings denied, confidence assertions, adversarial suites, PostgreSQL 16 TPC-H Q1-Q22 reference validation, Helm rendering, auth/identity-migration/TLS rendering and unsafe HPA rejection.

## [0.1.0] — development baseline

### SQL engine

- PostgreSQL wire endpoint, parser/binder, vectorized execution and general SQL execution.
- Multi-table joins, subqueries, grouping/aggregates, ordering/limits, CTE handling and selected window-function execution.
- Deterministic PostgreSQL 16 TPC-H Q1-Q22 reference suite at a small test scale.

### Storage and transactions

- Local MVCC/HLC/RocksDB storage.
- Local table DDL and `INSERT`/`UPDATE`/`DELETE` support.
- Persistent local user registry in standalone mode.

### Distributed subsystem

- Raft leader election/log replication, TCP/TLS transport and RocksDB-backed stable storage.

### Deployment

- Docker Compose development cluster.
- Kubernetes StatefulSet/headless-service deployment assets.
- Helm chart with auth/TLS render validation.

[Unreleased]: https://github.com/SaridakisStamatisChristos/Neuralbase/compare/main...HEAD
