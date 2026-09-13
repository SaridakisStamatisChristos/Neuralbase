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
- Automatic Kubernetes/HPA scaling remains disabled: the consensus protocol exists, while deployment changes must use deliberate membership operations or the separate Phase-7 managed profile.

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

### Operational backup / restore / disaster recovery — Phase 5

- Added explicit versioned NBBK logical backup artifacts rather than exposing raw internal Raft snapshot bytes.
- Added offline backup with source-lock enforcement, independent verification, restrictive atomic publication and strict compatibility/corruption handling.
- Added leader-coordinated online backup with one committed/applied logical boundary and race/fail-closed coverage.
- Added crash-safe fresh-target restore with SQL/catalog/HLC/apply/replicated-identity recovery, fresh membership generation and historical-node tombstones.
- Added fresh-generation cluster rebuild through learner catch-up/promotion, new writes, leader failover and full restart.
- Added authenticated NBEC v1 backup encryption, raw 32-byte key-file handling, wrong-key/tamper rejection and restrictive permissions.
- Added interruption evidence, real-process recovery evidence, compatibility/key semantics and an operator DR runbook.
- PITR, automatic DR and a standalone live-server online-backup CLI remain out of scope.

### Explicit read consistency — Phase 6

- Added session-scoped `Local`, `Leader`, and `Linearizable` read modes; new sessions default to backward-compatible `Local`.
- Added strict `SET neuralbase_read_consistency ...` / `SET neuralbase.read_consistency ...` parsing and malformed-input fail-closed behavior, including Unicode boundary hardening.
- Added a strong-read barrier that requires clustered mode, serving readiness and the current Raft leader.
- `Leader` and `Linearizable` currently use a current-term Raft control/log entry and proceed only after the existing client-command path confirms quorum commit plus durable local state-machine apply.
- Followers and recovering nodes reject strong reads explicitly; no strong mode silently downgrades to `Local`.
- Added stale-former-leader partition, follower rejection, leader-transfer, learner/promotion/finalization, recovery readiness, restart and Phase-5 restore-bootstrap coverage.
- Added real OS-process/TCP immediate linearizable read-after-write plus concurrent write/linearizable-read evidence.
- Arbitrary-follower linearizable routing, automatic follower-to-leader forwarding and ReadIndex/lease optimization remain unimplemented.

### Managed membership reconciliation — Phase 7

- Added a bounded deterministic desired-topology planner with monotonic revisions, immutable incarnation inventory, voter floors and one-action reconciliation.
- Added current-term quorum/apply authority observations plus leader/term/membership-generation guards checked inside the serialized Raft loop.
- Added managed fresh-learner startup using original genesis voters separately from current routing seeds, preserving safe replay after earlier membership changes.
- Added guarded learner admission, catch-up gating, joint-consensus promotion/removal, leadership transfer before leader removal, finalized tombstone retirement and retained storage.
- Added a Linux independent-process adapter with private management sockets, pidfd-based retirement checks, durable controller intent/restart recovery, authenticated SCRAM bootstrap and SQL/identity convergence tests.
- Added an opt-in Kubernetes adapter with one StatefulSet/PVC per incarnation, immutable ConfigMaps/Secrets, explicit context/namespace, ownership/UID validation, managed-field drift rejection and resource-version-guarded replica patches.
- Added disposable-kind lifecycle evidence for PVC-quota partial creation failure, configuration drift, 3→4 expansion, leader pod loss, fresh-identity replacement, 4→3 contraction, retained PVCs and SQL/SCRAM convergence.
- CI #315 passed the complete functional Phase-7 gate on `fb7d099c6248f18b324d8817fb2878885dbe38a8`; claim synchronization keeps `production_ready`, `production_ha` and `hpa_safe` false.
- Arbitrary Helm/StatefulSet replica scaling, HPA, rolling-upgrade orchestration, production security and production HA remain outside the Phase-7 claim.

### Documentation and deployment

- Audited runtime configuration and SQL/client limits against current implementation; added a complete environment/default/alias/TLS reference.
- Corrected two-binary startup and one-statement-per-request recovery examples.
- Documented standalone durability, backup source prerequisites, follower authentication freshness, ONNX/exchange integration limits, and actual metrics.
- Removed obsolete raw Kubernetes per-node credential-file seeding; migration remains explicit and digest-authorized through Helm.
- Updated package/chart descriptions and Compose comments to reflect replicated tables and identity.
- Corrected release Helm migration rendering to supply the required digest and aligned auth/TLS/negative cases with CI.
- Synchronized architecture, distributed semantics, SQL support, threat model, testing, roadmap, runbook and confidence claims through the implemented Phase-7 managed profile.
- Helm no longer copies a `users.json` file into each pod PVC as live identity state. An optional legacy source is mounted read-only and paired with an explicit SHA-256 migration authorization.
- Added CI rendering coverage for the identity-migration Helm path and rejection of incomplete migration configuration.

### Important remaining boundaries

- `Local` reads may be stale on followers; `Leader`/`Linearizable` require the current serving leader. Linearizable reads from arbitrary followers and automatic strong-read routing are not claimed.
- The opt-in Phase-7 managed profile sequences deployment membership safely within its tested scope; the checked-in static Kubernetes/Helm assets still do not make arbitrary replica-count/HPA changes safe.
- Manual Phase-5 backup/restore/fresh-cluster DR is implemented and tested; PITR and automatic disaster recovery remain unimplemented.
- Authorization remains intentionally limited compared with a production database security model.
- Managed rolling upgrades, hostile multi-tenant controller isolation, broad storage/network chaos certification and production performance characterization remain open.
- These changes do **not** make NeuralBase production-ready or justify a general production-HA claim.

### CI maintenance

- CI gates core tests, rustfmt/Clippy with warnings denied, confidence assertions, adversarial suites, PostgreSQL 16 TPC-H Q1-Q22 reference validation, Helm rendering, auth/identity-migration/TLS rendering, unsafe HPA rejection, and the disposable-kind Phase-7 managed Kubernetes lifecycle.

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