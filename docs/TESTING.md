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

Install the [native prerequisites](../CONTRIBUTING.md#development-prerequisites) and use the pinned Rust `1.88.0`. Gate definitions live in [`Makefile`](../Makefile):

| Command | Actual scope / prerequisite |
|---|---|
| `make test` | `cargo test --features tls --tests --locked`; includes library/binary unit tests and integration suites, including OS-process recovery/read tests |
| `make lint` | rustfmt plus `cargo clippy --all-targets --locked -- -D warnings`; Clippy uses default features, not an all-features matrix |
| `make confidence` | Machine-readable claim assertions in `tests/confidence_yaml.rs` |
| `make adversarial` | Vectorized suite with `simd`, plus optimizer/MVCC/Raft adversarial suites |
| `make tpch-correctness` | Separate `tls,tpch-reference-tests` gate; requires Docker for PostgreSQL 16, runs serially |
| `make bench` / `make bench-full` | Optional release-profile benchmarks; historical numbers are not refreshed by CI |
| `make cluster-test` | Starts Compose and runs Raft integration tests; the Rust suites create their own test nodes, so this is not a complete SQL test of the Compose deployment |

The normal core gate excludes the Docker-backed reference test by Cargo's `required-features`. CI runs that reference suite in a separate job. Fuzzing, ThreadSanitizer, `cargo deny`, benchmarks and live Kubernetes deployment are not part of the normal CI workflow. Some cleanup/certificate Makefile helpers use Windows `cmd` syntax; they are not portable Linux deployment instructions.

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

`auth.existingSecret` must be paired with an exact 64-hex `auth.migrationSha256`; auth-required startup from already initialized replicated state needs neither. TLS rendering configures SQL TLS only. Both CI and the release workflow exercise these render variants and an `autoscaling.enabled=true` failure case. The release validation job runs core/lint/confidence/adversarial gates; the PostgreSQL reference job belongs to normal CI, not the tag workflow.

A successful render does not prove live Kubernetes membership orchestration, upgrade/failover, disaster recovery or production security.

## Documentation validation

For documentation changes, verify relative links, environment names/defaults against their runtime readers, explicit `--bin` in server `cargo run` commands, and one SQL statement per client request. Repeated `psql -c` options share a connection and can validate session read modes; a single compound `SET ...; SELECT ...` cannot. Keep raw manifests and chart/release examples aligned with the same identity model.

Report which checks actually ran, including missing toolchains or Docker. A workflow that has started is not a passing gate, and a historical green commit does not validate a new documentation/configuration head.

## What green CI means

Green CI means the exact checked commit passed the repository's current executable gates. For the distributed path it supports replicated tables, SQL-aware snapshots, coordinated membership, replicated SCRAM identity, the tested Phase-5 recovery model, and the Phase-6 leader-path read-consistency contract under the scenarios above.

It still does **not** mean production readiness, complete PostgreSQL compatibility, linearizable arbitrary-follower reads, automatic strong-read routing, automatic deployment membership reconciliation/HPA, PITR or automatic DR, security certification, or performance superiority outside measured workloads.

## Phase-7 operator gates (validation in progress)

`phase7_planner`, `phase7_guarded_membership`, `phase7_managed_storage` and
`phase7_process` cover deterministic guarded planning, real Raft partitions and
joint boundaries, later learner genesis replay, immutable storage, independent
process lifecycle and replicated SQL/SCRAM convergence. The guarded variation in
`phase4_identity_membership` forces learner snapshot bootstrap after compaction.

The checks job additionally creates a disposable kind cluster and runs
`tests/phase7_kubernetes.py`: partial PVC-quota failure, object drift, 3→4 scaling,
leader restart/replacement, 4→3 scaling, retained PVCs and SQL/SCRAM convergence.
`tests/phase7_kubernetes_guards.py` checks object UID/version preconditions and
mid-command desired-revision rejection. Details and current closure status are in
[the Phase-7 guide](PHASE7_OPERATOR.md). Neither these tests nor static Helm
rendering establish HPA safety or production readiness.
