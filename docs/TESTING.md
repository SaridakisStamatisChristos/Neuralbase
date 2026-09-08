# Testing and evidence

NeuralBase separates evidence types so a green suite is not interpreted beyond the exact behavior exercised.

## Local commands

```bash
make test
make lint
make confidence
make adversarial
make tpch-correctness
make bench
```

## Core replicated SQL/snapshot evidence

The core suite covers deterministic table command encoding, leader materialization, follower rejection, quorum+confirmed-apply acknowledgement, idempotent RocksDB replay, fail-closed Raft persistence, logical snapshot codec/restore, interrupted installation, repeated compaction cycles, empty-storage reconstruction and multi-process failover/restart.

## Phase 3 membership evidence

`tests/phase3_membership.rs` exercises:

- learner admission as a non-voter;
- catch-up before promotion;
- joint-consensus promotion and finalization;
- 3 → 4 → 3 voter lifecycle with writes preserved;
- durable finalized membership overriding stale startup peers after restart;
- removed-node/stale-disk protection.

This proves explicit consensus membership operations, not automatic Kubernetes reconciliation or HPA safety.

## Phase 4 identity evidence

Focused/unit tests cover deterministic versioned identity encoding, plaintext-secrecy boundaries, rejection of reusable MD5 material, canonical replicated state, atomic identity+apply-cursor writes, strict legacy migration, identity snapshot extension validation and replay behavior.

Integration suites add:

- `tests/phase4_identity.rs` — follower rejection, three-node convergence, leader loss, password rotation and drop;
- `tests/phase4_identity_recovery.rs` — restart persistence, idempotent replay and conflicting initialization failure;
- `tests/phase4_identity_snapshot_bootstrap.rs` — empty member recovery from identity-bearing snapshot + suffix, later leadership and user DDL;
- `tests/phase4_identity_membership.rs` — compacted identity state acquired by a new learner, promotion, four-voter credential rotation, removal and surviving-cluster convergence;
- `tests/phase4_identity_process.rs` — three real `neuralbase` processes, auth restart, cross-node authentication, password rotation, leader failure, successor user creation, killed-node rejoin and credential revocation.

The process test uses independent/nonexistent per-node user-file paths, so successful authentication after restart demonstrates replicated RocksDB authority rather than accidental shared-file state.

## Confidence gate

`tests/confidence_yaml.rs` protects the machine-readable boundary. It requires production readiness to remain false while asserting the tested Phase 3 membership and Phase 4 auth-replication capabilities, and it keeps automatic membership reconciliation/HPA and linearizable follower reads false.

## Lint, adversarial and PostgreSQL reference gates

`make lint` runs rustfmt and Clippy with warnings denied. `make adversarial` exercises focused edge/failure suites. `make tpch-correctness` compares checked-in Q1-Q22 output with PostgreSQL 16 on a deterministic small dataset.

## Deployment-manifest gate

CI performs Helm lint/default render, auth-required render, explicit identity-migration render, TLS render, rejection of incomplete migration configuration, and rejection of unsafe HPA configuration.

A successful render does not prove live Kubernetes membership orchestration, upgrade/failover, disaster recovery or production security.

## What green CI means

Green CI means the exact checked commit passed the repository's current executable gates. For the distributed path it supports replicated tables, SQL-aware snapshots, coordinated membership and replicated SCRAM identity under the tested scenarios.

It still does **not** mean production readiness, complete PostgreSQL compatibility, linearizable arbitrary-follower reads, automatic deployment membership reconciliation/HPA, backup/PITR/disaster recovery, security certification, or performance superiority outside measured workloads.
