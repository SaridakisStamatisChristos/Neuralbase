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
- Added a three-process PostgreSQL/Raft integration test with independent RocksDB directories covering convergence, leader loss/re-election, killed-node catch-up, full-cluster restart and a write raced against leader kill.

### SQL-aware snapshot, compaction and fixed-member recovery

- Added a separate versioned logical SQL snapshot format with Raft boundary metadata, durable SQL apply index, replicated HLC floor, durable catalog/table state, table IDs, canonical primary keys, exact encoded row values, explicit bounds and SHA-256 corruption detection.
- Added fail-closed snapshot decoding for unsupported versions, corruption/truncation, duplicates/noncanonical ordering, table-ID mismatch and impossible metadata.
- Added consistent snapshot export from one RocksDB snapshot.
- Added fail-closed restore that atomically replaces durable SQL catalog/data/apply-marker state before publishing in-memory catalog/HLC state.
- Added restore anti-regression checks for the durable SQL apply index and replicated commit timestamp.
- Added typed durable snapshot staging for local Creation versus follower Installation transitions.
- SQL-aware compaction now validates and durably stages the snapshot before Raft prefix truncation can become durable.
- InstallSnapshot now validates/stages incoming SQL state, restores durable SQL state before Raft snapshot publication, and acknowledges success only after persistence succeeds.
- Added restart recovery for a crash after SQL restore but before active Raft snapshot publication.
- Fixed Raft snapshot suffix retention so a suffix is kept only when the local snapshot-boundary term matches the incoming term.
- Added exact snapshot-index correlation and explicit success/failure to InstallSnapshot replies.
- Added a fresh-member SQL serving gate: an empty fixed member remains non-serving until snapshot/suffix catch-up and a successful leader consistency exchange reach the commit point.
- Added repeated SQL snapshot/compaction/restart/suffix lifecycle tests.
- Added empty-storage recovery of the same already-configured fixed logical member ID, including post-snapshot suffix catch-up, replacement leadership, acknowledged post-recovery writes, leader loss, restart and exact convergence/no duplicate MVCC effects.
- Legacy opaque snapshots remain rejected in confirmed-SQL configurations that do not attach the SQL-aware snapshot store.

### Important remaining boundaries

- User/auth `CREATE USER`, `ALTER USER`, and `DROP USER` remain per-node.
- Reads remain local; arbitrary follower reads are not claimed linearizable.
- Raft membership remains fixed from an operator/deployment perspective; HPA-driven scaling is still rejected.
- Phase 2 recovery is limited to an already-configured fixed logical member ID; dynamic/new-ID membership and automatic node replacement are not implemented.
- Backup/restore, PITR and disaster-recovery workflows are not implemented by the cluster snapshot catch-up path.
- These changes do **not** make NeuralBase production-ready or justify a general production-HA claim.

### Documentation and repository presentation

- Added dedicated architecture, SQL-support, distributed-semantics, deployment and testing documentation.
- Added a prioritized `ROADMAP.md`, `CONTRIBUTING.md`, and `SECURITY.md`.
- Kept machine-readable confidence claims synchronized with executable distributed lifecycle evidence.
- Expanded `.env.example` to canonical `NEURALBASE_*` configuration.

### CI maintenance

- CI gates core tests, rustfmt/Clippy with warnings denied, confidence assertions, adversarial suites, PostgreSQL 16 TPC-H Q1-Q22 reference validation, Helm rendering, auth/TLS chart rendering and unsafe fixed-membership HPA rejection.

## [0.1.0] — development baseline

### SQL engine

- PostgreSQL wire endpoint, parser/binder, vectorized execution and general SQL execution.
- Multi-table joins, subqueries, grouping/aggregates, ordering/limits, CTE handling and selected window-function execution.
- Deterministic PostgreSQL 16 TPC-H Q1-Q22 reference suite at a small test scale.

### Storage and transactions

- Local MVCC/HLC/RocksDB storage.
- Local table DDL and `INSERT`/`UPDATE`/`DELETE` support.
- Persistent user registry for `CREATE USER`, `ALTER USER`, and `DROP USER`.

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

[Unreleased]: https://github.com/SaridakisStamatisChristos/Neuralbase/compare/main...HEAD
