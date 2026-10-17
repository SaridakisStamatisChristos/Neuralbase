# NeuralBase roadmap

NeuralBase is a **pre-1.0 experimental SQL engine**. This document describes the current bounded capability surface and the next engineering work. It does not encode implementation order and should not be read as a release-readiness claim.

The authoritative executable evidence boundary is [`CONFIDENCE.yaml`](CONFIDENCE.yaml). SQL compatibility claims are additionally constrained by [`SQL_COMPATIBILITY.yaml`](SQL_COMPATIBILITY.yaml).

## Current implemented capability surface

### Replicated persistent table mutations

- [x] Replicate persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE` through deterministic Raft-backed state-machine commands.
- [x] Reject follower writes before proposal.
- [x] Materialize state-dependent `UPDATE`/`DELETE` effects on the serving leader before replication.
- [x] Acknowledge replicated mutations only after quorum commit plus confirmed durable local apply.
- [x] Preserve outcome uncertainty on timeout/disconnect after submission rather than claiming rollback.

### SQL-aware snapshot, compaction and recovery

- [x] Include SQL catalog/data/apply/HLC state in logical snapshots.
- [x] Durably stage validated snapshots before Raft prefix truncation.
- [x] Restore durable SQL state before acknowledging `InstallSnapshot`.
- [x] Reconstruct empty-storage fixed members from snapshot plus remaining log suffix.
- [x] Preserve apply/snapshot metadata ordering across restart and compaction cycles.

### Coordinated Raft membership

- [x] Support learner admission and catch-up.
- [x] Promote/remove through joint consensus and durable finalized membership.
- [x] Require leadership transfer before removing the current leader.
- [x] Persist removed incarnation identities and reject stale-disk rejoin.
- [x] Preserve membership across snapshot/restart and cluster lifecycle tests.

### Replicated SCRAM identity

- [x] Replicate `CREATE USER`, `ALTER USER`, and `DROP USER` as verifier-only identity commands.
- [x] Keep plaintext passwords out of replicated identity payloads.
- [x] Authenticate clustered nodes from authoritative replicated RocksDB identity state.
- [x] Support explicit digest-selected migration from a strict legacy SCRAM registry.
- [x] Reject PostgreSQL MD5 verifier material from replicated migration.
- [x] Preserve identity through snapshot bootstrap, membership change and restart.

### Backup, restore and fresh-cluster recovery

- [x] Versioned NBBK backup format with independent verification.
- [x] Offline create/verify/restore CLI.
- [x] Tested in-process online backup coordinator.
- [x] Authenticated NBEC backup encryption with out-of-band key files.
- [x] Crash-safe restore through staging and atomic publication.
- [x] Fresh-cluster recovery with a new generation and stale source-node fencing.
- [x] Recovery/process tests covering failover and full restart.

The online coordinator is not a standalone live-server CLI. Recovery is not in-place restore into an existing live cluster.

### Explicit read consistency

- [x] Session-scoped `Local`, `Leader`, and `Linearizable` modes.
- [x] `Local` remains the no-consensus default.
- [x] Strong modes require clustered mode and the current serving leader.
- [x] Strong reads use a current-term replicated barrier and wait for confirmed durable local apply.
- [x] Followers and recovering/non-serving nodes reject strong reads; no silent downgrade exists.
- [x] Partition, membership, recovery, restart and real-process read tests exercise the contract.

Automatic follower-to-leader routing, arbitrary-follower linearizable reads, ReadIndex and lease-read optimization remain open.

### Managed membership reconciliation

- [x] Versioned desired topology with explicit cluster incarnation and monotonic revision.
- [x] Deterministic one-action-at-a-time planner.
- [x] Quorum-authorized observation and term/generation-guarded execution.
- [x] Fresh learner creation, catch-up, promotion, leader transfer, finalized removal and retained storage.
- [x] Linux process adapter with independent processes/RocksDB directories.
- [x] Per-incarnation Kubernetes profile with UID/resource-version guards and retained PVCs.
- [x] Real-process and disposable-kind lifecycle evidence covering partial failure, drift, replacement and contraction.

See [`docs/MANAGED_MEMBERSHIP.md`](docs/MANAGED_MEMBERSHIP.md). Raw Helm/StatefulSet replica changes and HPA remain unsupported.

### SQL semantic and compatibility hardening

- [x] Executable [`SQL_COMPATIBILITY.yaml`](SQL_COMPATIBILITY.yaml) anti-overclaim profile.
- [x] PostgreSQL 16 row-for-row TPC-H Q1-Q22 checks on the deterministic reference dataset.
- [x] Reusable selected scalar/NULL/boolean/cast/date/text/window differential suite.
- [x] Fail-closed unsupported persistent DML predicates.
- [x] Fail-closed unsupported constraints/default declarations and window constructs.
- [x] Exactly-one-statement ordinary request boundary.
- [x] Explicitly unsupported SQL transaction blocks.
- [x] Bounded typed Parse/Bind/Execute/Describe/Close/Sync subset with real `postgres` client evidence.
- [x] Deterministic parameter materialization before replicated mutation proposal.

Full PostgreSQL compatibility is not claimed. Exact NUMERIC semantics, broader date/time/timestamp behavior, locale/collation fidelity, enforceable general constraints, arbitrary parameter OIDs, binary row output and complete extended-protocol recovery/metadata remain open.

### Exact committed-index archived recovery

- [x] Bind each archive stream to one verified NBBK/NBEC baseline and immutable timeline.
- [x] Record every required committed Raft position after the baseline.
- [x] Strict hash-linked archive format with gap/overlap/conflict detection.
- [x] Optional authenticated archive encryption with out-of-band keys.
- [x] Fence confirmed apply on required archive publication.
- [x] Replay exact committed-index targets into a fresh verified recovery generation.
- [x] Child timeline branching after earlier-point recovery.
- [x] Conservative bounded rollover/retirement.
- [x] Real-process SQL/identity exact-target recovery and branched restart evidence.

See [`docs/PITR.md`](docs/PITR.md) and [`docs/PITR_CORRECTNESS_MODEL.md`](docs/PITR_CORRECTNESS_MODEL.md). Timestamp targets and automatic DR are not implemented.

### Query-path performance

- [x] Profile the persistent user-query route with reproducible exact-base/head measurements.
- [x] Allow simple persistent scans to bypass unrelated synthetic TPC-H fixture construction while preserving built-in TPC-H authority.
- [x] Route general persistent-only queries through a narrow persistent catalog and fall back only on `TableNotFound`.
- [x] Lock mixed/built-in-name/fallback semantics with correctness tests.
- [x] Preserve write, consensus, read-barrier, snapshot, backup and PITR safety boundaries.

See [`docs/PERFORMANCE_PROFILE.md`](docs/PERFORMANCE_PROFILE.md). No general write-performance, production-capacity or hardware-independent throughput claim is made.

### Durable distributed performance and liveness

- [x] Incremental RocksDB Raft persistence rather than full-state rewrite on each update.
- [x] Pipelined replicated-write proposal locking without weakening acknowledgement semantics.
- [x] Persistent ordered per-peer TCP/TLS writers with `TCP_NODELAY`.
- [x] Process-incarnation fencing for peer transport.
- [x] Durable apply-frontier recovery.
- [x] Randomized retry/election hardening for repeated campaigns.
- [x] Same-host three-durable-voter characterization with explicit commit/topology provenance.

These measurements are characterization evidence, not a production capacity plan or multi-host SLA.

## Highest-value open work

### SQL correctness and compatibility

- [ ] Exact DECIMAL/NUMERIC precision and scale rather than DOUBLE mapping.
- [ ] Broader DATE/TIME/TIMESTAMP/timezone semantics with reference tests.
- [ ] Locale/collation semantics or explicit narrower deterministic alternatives.
- [ ] Enforceable constraint semantics where introduced.
- [ ] Fuller PostgreSQL result metadata and protocol error-state compatibility.
- [ ] SQL transaction blocks only after multi-statement and replicated-state semantics can be defined without weakening safety.

### Distributed reads and routing

- [ ] Evaluate ReadIndex/lease-based strong reads against the existing failure contract.
- [ ] Automatic current-leader routing for strong reads with explicit failure semantics.
- [ ] Arbitrary-follower linearizable reads only with a demonstrated correctness model and adversarial tests.

### Operations and resilience

- [ ] Upgrade orchestration and compatibility policy.
- [ ] Broader network/storage chaos coverage for managed reconciliation.
- [ ] Production-grade controller security/isolation before stronger operational claims.
- [ ] Timestamp-to-index recovery mapping only with an explicit durable time/index contract.
- [ ] Remote/object-store archive replication as a separate bounded capability if implemented.
- [ ] Measured recovery RPO/RTO only after an explicit deployment topology and workload are fixed.

### Security and authorization

- [ ] Broader SQL authorization/RBAC.
- [ ] Key-rotation and operational policy for backup/archive encryption.
- [ ] Additional storage-at-rest and upgrade threat-model work.

### Performance

- [ ] Continue measurement-driven profiling rather than checklist optimization.
- [ ] Address sort/aggregate memory growth only with reproducible workload evidence.
- [ ] Evaluate write-path batching/group commit only if measurements identify it as the dominant bottleneck and safety can be preserved.
- [ ] Integrate learned/ONNX optimization into the live planner only with end-to-end plan-quality evidence.

## Explicit non-goals / unsupported claims today

NeuralBase does not currently claim:

- production SQL HA;
- complete PostgreSQL compatibility;
- SQL multi-statement transactions or distributed transactions;
- arbitrary Helm/HPA replica scaling;
- automatic rolling-upgrade orchestration;
- arbitrary-follower linearizable reads;
- timestamp-target PITR;
- automatic disaster detection/failover/recovery;
- hardware-independent or multi-host performance guarantees.

Historical milestone/closure records are retained under [`docs/history/`](docs/history/) for provenance only and are not the current capability roadmap.
