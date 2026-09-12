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

## Phase 5 backup/restore/DR evidence

Phase-5 focused and integration suites cover the explicit NBBK codec/limits/checksums; offline source locking and atomic publication; independent verification classifications; fresh-target restore of SQL/catalog/HLC/apply/identity state; fresh recovery membership generation and stale-source tombstones; leader-coordinated online boundary capture under SQL/identity/membership/compaction/leadership races; authenticated NBEC encryption, wrong-key/tamper rejection and restrictive permissions; interrupted backup publication and representative stale restore stages; and full fresh-generation cluster rebuilding through learner catch-up/promotion, failover and restart.

`tests/phase5_recovery_process.rs` uses real `neuralbase` and `neuralbase-backup` OS processes/binaries for backup, restore, authentication, post-restore write and restart evidence. `tests/phase5_cluster_recovery.rs` exercises the wider restored-cluster lifecycle with independent RocksDB-backed Raft nodes.

Online backup tests exercise the in-process `OnlineBackupCoordinator`; this is not evidence for a standalone live-server backup CLI endpoint.

## Phase 6 read-consistency evidence

Phase 6 tests the explicit `Local`, `Leader`, and `Linearizable` session contract and the log-backed strong-read barrier.

- `src/read_consistency.rs` unit tests cover default/aliases, strict setting parsing, malformed targeted settings and malformed Unicode boundary input.
- `src/read_barrier.rs` unit tests prove `Local` needs no cluster gateway and strong modes do not silently downgrade when Raft is unavailable.
- `tests/phase6_read_barrier.rs` covers acknowledged-write → linearizable-read ordering, explicit follower rejection, an isolated former leader under partition, membership changes with learners excluded from quorum, finalized removal, and leadership transfer.
- `tests/phase6_membership_reads.rs` exercises a real learner admission/catch-up/promotion path and proves strong reads before and after the four-voter finalized configuration.
- `tests/phase6_recovery_readiness.rs` seeds persisted Raft recovery state and proves `Local` remains available while `Leader`/`Linearizable` fail closed with catching-up before serving readiness.
- `tests/phase6_read_consistency_process.rs` uses a real `neuralbase` OS process and PostgreSQL TCP connections to test session mode changes, immediate linearizable read-after-write, leader mode, independent-session Local default, restart, and a real Phase-5 backup/verify/restore bootstrap followed by a strong read.
- `tests/phase6_concurrent_process.rs` runs concurrent real-process writes and linearizable reads and verifies the observed row counts form a non-regressing committed prefix with the final strong read seeing every acknowledged write.

The implementation uses a current-term Raft control/log entry rather than a separate ReadIndex RPC. The safety evidence relies on the existing `ClientCommand` contract: success is returned only after the entry has quorum-committed under the active membership and the local replicated state machine confirms durable apply. This is correctness evidence for the leader path, not evidence for arbitrary-follower strong-read routing or a low-latency read optimization.

## Confidence gate

`tests/confidence_yaml.rs` protects the machine-readable boundary. It requires production readiness to remain false, asserts the tested membership/identity/backup boundaries, asserts the Phase-6 read modes and strong-read barrier contract, and keeps arbitrary-follower linearizable reads plus automatic membership reconciliation/HPA false.

## Lint, adversarial and PostgreSQL reference gates

`make lint` runs rustfmt and Clippy with warnings denied. `make adversarial` exercises focused edge/failure suites. `make tpch-correctness` compares checked-in Q1-Q22 output with PostgreSQL 16 on a deterministic small dataset.

## Deployment-manifest gate

CI performs Helm lint/default render, auth-required render, explicit identity-migration render, TLS render, rejection of incomplete migration configuration, and rejection of unsafe HPA configuration.

A successful render does not prove live Kubernetes membership orchestration, upgrade/failover, disaster recovery or production security.

## What green CI means

Green CI means the exact checked commit passed the repository's current executable gates. For the distributed path it supports replicated tables, SQL-aware snapshots, coordinated membership, replicated SCRAM identity, the tested Phase-5 recovery model, and the Phase-6 leader-path read-consistency contract under the scenarios above.

It still does **not** mean production readiness, complete PostgreSQL compatibility, linearizable arbitrary-follower reads, automatic strong-read routing, automatic deployment membership reconciliation/HPA, PITR or automatic DR, security certification, or performance superiority outside measured workloads.
