# Changelog

All notable user-visible changes to NeuralBase are recorded here.

NeuralBase is currently a **pre-1.0 experimental project**. The crate version is `0.1.0`; no historical development-session label should be interpreted as a published stable release.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) where practical.

## [Unreleased]

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
- Explicit documentation that SQL mutations are **not yet replicated through Raft** and automatic replicated SQL failover is not implemented.

[Unreleased]: https://github.com/SaridakisStamatisChristos/Neuralbase/compare/main...HEAD
