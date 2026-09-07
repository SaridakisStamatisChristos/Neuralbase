# Changelog

All notable user-visible changes to NeuralBase are recorded here.

NeuralBase is currently a **pre-1.0 experimental project**. The crate version is `0.1.0`; no development-session label should be interpreted as a published stable release.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) where practical.

## [Unreleased]

### Fixed-membership replicated table mutations

- Added a versioned deterministic binary mutation format for persistent table `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE`.
- Added canonical DML key ordering and rejection of ambiguous/non-canonical command encodings.
- Added leader-side concrete `UPDATE`/`DELETE` materialization so followers apply exact row/key effects rather than re-evaluating predicates.
- Added a current-term readiness barrier before persistent mutation binding/materialization after election.
- Routed clustered persistent table mutations through the Raft leader; followers reject writes before proposal instead of mutating local RocksDB.
- Changed normal Raft client acknowledgement so success waits for quorum commit and confirmed state-machine apply.
- Added deterministic RocksDB state-machine apply with atomic SQL effect + durable replay marker and restart-safe HLC recovery.
- Added RocksDB-backed Raft stable storage and fail-stop handling of required persistence load/save failures.
- Added injected persistence/apply failure tests that require no false client success.
- Added a three-process PostgreSQL/Raft integration test with independent RocksDB directories covering convergence, leader loss/re-election, killed-node catch-up, full-cluster restart, and a write raced against leader kill.
- Replicated-SQL mode now rejects legacy opaque Raft compaction/snapshot state until a SQL-aware snapshot/bootstrap format exists.

### Important remaining boundaries

- User/auth `CREATE USER`, `ALTER USER`, and `DROP USER` remain per-node.
- Reads remain local; arbitrary follower reads are not claimed linearizable.
- Raft membership remains fixed from an operator/deployment perspective; HPA-driven scaling is still rejected.
- SQL-aware snapshot/bootstrap, replacement-node recovery, backup/restore, and disaster-recovery workflows are not implemented.
- These changes do **not** make NeuralBase production-ready or justify a general production-HA claim.

### Documentation and repository presentation

- Added dedicated architecture, SQL-support, distributed-semantics, deployment, and testing documentation.
- Added a prioritized `ROADMAP.md`, `CONTRIBUTING.md`, and `SECURITY.md`.
- Added issue and pull-request templates.
- Replaced stale session-oriented public documentation with evidence-scoped project documentation.
- Expanded `.env.example` to canonical `NEURALBASE_*` configuration.
- Added crate package metadata for repository/readme/description/categories.
- Removed committed one-off build, audit, and Clippy diagnostic logs from the working tree.

### CI maintenance

- Updated GitHub `checkout` and `cache` actions to Node 24-compatible v5 releases.
- CI gates core tests, rustfmt/Clippy with warnings denied, confidence assertions, adversarial suites, PostgreSQL 16 TPC-H Q1-Q22 reference validation, Helm rendering, auth/TLS chart rendering, and unsafe fixed-membership HPA rejection.

## [0.1.0] — development baseline

### SQL engine

- PostgreSQL wire endpoint, parser/binder, vectorized execution, and general SQL execution.
- Multi-table joins, subqueries, grouping/aggregates, ordering/limits, CTE handling, and selected window-function execution.
- Deterministic PostgreSQL 16 TPC-H Q1-Q22 reference suite at a small test scale.

### Storage and transactions

- Local MVCC/HLC/RocksDB storage.
- Local table DDL and `INSERT`/`UPDATE`/`DELETE` support.
- Persistent user registry for `CREATE USER`, `ALTER USER`, and `DROP USER`.
- User-registry rollback when persistence fails.

### Distributed subsystem

- Raft leader election and log replication subsystem.
- Real multi-process TCP transport and feature-gated TLS transport.
- Explicit logical-node-ID to peer-address mapping.
- Shutdown-safe apply-channel backpressure behavior.

### Deployment

- Docker Compose development cluster.
- Kubernetes StatefulSet/headless-service deployment assets.
- Helm chart with auth/TLS render validation.
- Automatic HPA disabled/rejected for fixed Raft membership.
- PodDisruptionBudget majority derived from replica count.

### Evidence and boundaries

- Core, lint, confidence, adversarial, TPC-H reference, and deployment-manifest CI gates.
- At this baseline, SQL mutations were local-only and automatic replicated SQL failover was not implemented.

[Unreleased]: https://github.com/SaridakisStamatisChristos/Neuralbase/compare/main...HEAD
