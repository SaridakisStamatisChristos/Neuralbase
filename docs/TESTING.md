# Testing and evidence

NeuralBase separates evidence types so a green suite is not interpreted beyond the exact behavior exercised.

## Local commands

```bash
make test
make lint
make confidence
make adversarial
make tpch-correctness
make sql-differential
make bench
```

Install the [native prerequisites](../CONTRIBUTING.md#development-prerequisites) and use the pinned Rust `1.88.0`. Gate definitions live in [`Makefile`](../Makefile):

| Command | Actual scope / prerequisite |
|---|---|
| `make test` | `cargo test --features tls --tests --locked`; includes library/binary unit tests and integration suites, including OS-process recovery/read/managed-membership and SQL/wire tests |
| `make lint` | rustfmt plus `cargo clippy --all-targets --locked -- -D warnings`; Clippy uses default features, not an all-features matrix |
| `make confidence` | Machine-readable claim assertions in `tests/confidence_yaml.rs` plus the executable `SQL_COMPATIBILITY.yaml` anti-overclaim profile |
| `make adversarial` | Vectorized suite with `simd`, plus optimizer/MVCC/Raft adversarial suites |
| `make tpch-correctness` | Separate `tls,tpch-reference-tests` gate; requires Docker for PostgreSQL 16 and runs checked-in TPC-H Q1-Q22 reference checks plus the semantic differential suite serially |
| `make sql-differential` | Reusable PostgreSQL-16 semantic differential suite only; requires Docker and `tls,tpch-reference-tests` |
| `make bench` / `make bench-full` | Optional release-profile benchmarks; historical numbers are not refreshed by normal CI |
| `make cluster-test` | Starts Compose and runs Raft integration tests; the Rust suites create their own test nodes, so this is not a complete SQL test of the Compose deployment |

The normal core gate excludes Docker-backed reference tests by Cargo's `required-features`. CI runs the PostgreSQL reference suite in a separate job. The checks job also creates a disposable kind cluster and executes the managed Kubernetes membership lifecycle. Fuzzing, ThreadSanitizer, `cargo deny` and full characterization benchmarks are not part of the normal CI workflow. Some cleanup/certificate Makefile helpers use Windows `cmd` syntax; they are not portable Linux deployment instructions.

## Replicated SQL and snapshot evidence

The core suite covers deterministic table command encoding, leader materialization, follower rejection, quorum+confirmed-apply acknowledgement, idempotent RocksDB replay, fail-closed Raft persistence, logical snapshot codec/restore, interrupted installation, repeated compaction cycles, empty-storage reconstruction and multi-process failover/restart.

## Membership lifecycle evidence

`tests/membership_lifecycle.rs` exercises learner admission as a non-voter, catch-up before promotion, joint-consensus promotion/finalization, a 3 → 4 → 3 voter lifecycle with writes preserved, durable finalized membership overriding stale startup peers after restart, and removed-node/stale-disk protection.

This proves explicit consensus membership operations. Arbitrary raw Kubernetes/Helm/HPA replica changes remain outside that contract; the managed membership profile sequences deployment changes through the membership protocol.

## Replicated identity evidence

Focused/unit tests cover deterministic versioned identity encoding, plaintext-secrecy boundaries, rejection of reusable MD5 material, canonical replicated state, atomic identity+apply-cursor writes, strict legacy migration, identity snapshot extension validation and replay behavior.

Integration suites include `tests/replicated_identity.rs`, `tests/replicated_identity_recovery.rs`, `tests/replicated_identity_snapshot_bootstrap.rs`, `tests/replicated_identity_membership.rs`, and the real-process `tests/replicated_identity_process.rs`. The process test uses independent/nonexistent per-node user-file paths, so successful authentication after restart demonstrates replicated RocksDB authority rather than accidental shared-file state.

## Backup, restore and fresh-cluster recovery evidence

Focused and integration suites cover the explicit NBBK codec/limits/checksums; offline source locking and atomic publication; independent verification classifications; fresh-target restore of SQL/catalog/HLC/apply/identity state; fresh recovery membership generation and stale-source tombstones; leader-coordinated online boundary capture under SQL/identity/membership/compaction/leadership races; authenticated NBEC encryption, wrong-key/tamper rejection and restrictive permissions; interrupted backup publication and representative stale restore stages; and full fresh-generation cluster rebuilding through learner catch-up/promotion, failover and restart.

`tests/recovery_process.rs` uses real `neuralbase` and `neuralbase-backup` OS processes/binaries for backup, restore, authentication, post-restore write and restart evidence. `tests/cluster_recovery.rs` exercises the wider restored-cluster lifecycle with independent RocksDB-backed Raft nodes. Online backup tests exercise the in-process `OnlineBackupCoordinator`; this is not evidence for a standalone live-server backup CLI endpoint.

## Read-consistency evidence

The read suites test the explicit `Local`, `Leader`, and `Linearizable` session contract and the log-backed strong-read barrier.

- `src/read_consistency.rs` unit tests cover default/aliases, strict setting parsing, malformed targeted settings and malformed Unicode boundary input.
- `src/read_barrier.rs` unit tests prove `Local` needs no cluster gateway and strong modes do not silently downgrade when Raft is unavailable.
- `tests/read_barrier.rs` covers acknowledged-write → linearizable-read ordering, explicit follower rejection, an isolated former leader under partition, membership changes with learners excluded from quorum, finalized removal, and leadership transfer.
- `tests/read_consistency_membership.rs` exercises a real learner admission/catch-up/promotion path and proves strong reads before and after a four-voter finalized configuration.
- `tests/read_consistency_recovery_readiness.rs` seeds persisted Raft recovery state and proves `Local` remains available while `Leader`/`Linearizable` fail closed with catching-up before serving readiness.
- `tests/read_consistency_process.rs` uses a real `neuralbase` OS process and PostgreSQL TCP connections to test session mode changes, immediate linearizable read-after-write, leader mode, independent-session Local default, restart, and a real backup/verify/restore bootstrap followed by a strong read.
- `tests/read_consistency_concurrent_process.rs` runs concurrent real-process writes and linearizable reads and verifies the observed row counts form a non-regressing committed prefix with the final strong read seeing every acknowledged write.

The implementation uses a current-term Raft control/log entry rather than a separate ReadIndex RPC. The safety evidence relies on the existing `ClientCommand` contract: success is returned only after the entry has quorum-committed under the active membership and the local replicated state machine confirms durable apply. This is correctness evidence for the leader path, not evidence for arbitrary-follower strong-read routing or a low-latency read optimization.

## Managed membership evidence

`managed_membership_planner`, `managed_membership_guards`, `managed_membership_storage` and `managed_membership_process` cover deterministic guarded planning, real Raft partitions and joint boundaries, later learner genesis replay, immutable storage, independent process lifecycle and replicated SQL/SCRAM convergence. The guarded variation in `replicated_identity_membership` forces learner snapshot bootstrap after compaction.

The checks job additionally creates a disposable kind cluster and runs `tests/managed_membership_kubernetes.py`: partial PVC-quota failure, object drift, 3→4 scaling, leader restart/replacement, 4→3 scaling, retained PVCs and SQL/SCRAM convergence. `tests/managed_membership_kubernetes_guards.py` checks object UID/version preconditions and mid-command desired-revision rejection. Neither the managed tests nor static Helm rendering establish HPA safety or production readiness.

## SQL semantic and wire evidence

The SQL suite uses an explicit capability inventory rather than treating parser acceptance as compatibility. [`SQL_COMPATIBILITY.yaml`](../SQL_COMPATIBILITY.yaml) is executable documentation guarded by `tests/sql_compatibility_profile.rs`.

The evidence set includes:

- `tests/sql_semantic_differential.rs` plus `tests/common/pg16_reference.rs` — PostgreSQL 16 differential checks for a selected scalar NULL/boolean/text/cast/date surface and basic window cases;
- `tests/sql_dml_predicate_safety.rs` — unsupported or unbindable persistent `UPDATE`/`DELETE` WHERE clauses fail closed instead of broadening to full-table mutation;
- `tests/sql_ddl_safety.rs` — unenforced CREATE TABLE defaults/constraints/options fail closed;
- `tests/window_semantics_safety.rs` — implemented basic window expressions retain their semantics while named windows and explicit frames fail closed;
- `tests/transaction_protocol_boundaries.rs` — exactly one statement per request, explicit unsupported `BEGIN`/`COMMIT`/`ROLLBACK`, no partial compound execution, and connection recovery after errors;
- `tests/extended_protocol.rs` — bounded typed Parse/Bind/Execute/Describe/Close/Sync behavior, parameter OIDs, NULLs and fail-closed statement/portal errors;
- `tests/postgres_client_compatibility.rs` — a real Rust `postgres` client prepares and reuses a typed statement against the live server;
- `tests/parameter_determinism.rs` — typed parameters materialize deterministically before the established parser/binder and replicated mutation path.

The supported parameter subset is intentionally narrow. OID 0 context-sensitive inference, arbitrary OIDs/arrays/composites, binary row output, complete result metadata and PostgreSQL's full extended-protocol failed-transaction/error-recovery state are not claimed. SQL transaction blocks remain unsupported and there is no distributed multi-statement transaction claim.

## Exact-index archived recovery / PITR evidence

PITR combines codec, storage, replay and real-process evidence instead of inferring recoverability from retained Raft logs. Unit/focused tests cover strict NBAR/NBPE decoding, unsupported format/state-machine versions, truncation/trailing ambiguity, gap/overlap/hash/timeline failures, canonical membership bytes, restart staging cleanup, corrupt finalized files, duplicate/conflicting publication, convergent same-record publication, encrypted wrong-key/tamper behavior, bounded stream enumeration, target availability, wrong baseline rejection before publication, fresh recovery generations, child-timeline old-future rejection and conservative retention retirement.

`tests/pitr_process.rs` uses real server/backup/PITR binaries. It captures a baseline, archives later committed mutations, selects an exact target, proves post-target table and password changes are absent after recovery, creates a distinct child timeline before resumed service, writes a new future, restarts, and verifies the recovered identity/data plus child future persist. The replay suite also exercises encrypted end-to-end archive recovery.

Timestamp-target recovery and automatic DR are intentionally outside this evidence.

## Query-path performance evidence

The `query-path-bench` workflow runs exact-base/head query and lifecycle characterization with the renamed `perf_query_path*` harnesses. The active product vocabulary is capability-based; legacy `PHASE10_LABEL`, `PHASE10_MEASURED_COMMIT` and `PHASE10_RESULT` tokens remain only as historical machine-readable benchmark protocol identifiers so archived exact-base/head results stay comparable. See [`PERFORMANCE_PROFILE.md`](PERFORMANCE_PROFILE.md).

## Three-voter durable-cluster characterization

`tests/perf_durable_cluster.rs` starts three production server processes with independent RocksDB directories and TCP Raft connections on one host. The ordinary test gate runs a small smoke test that deliberately probes before DDL exists, waits for all replicas, exercises all three read modes concurrently, and checks every acknowledged warmup/measured write on every voter. Regression tests also cover worker startup failure, narrow catch-up error classification, and incorrect/duplicate replicated rows.

Run the smoke/regression tests and then the optional release characterization:

```bash
cargo test --test perf_durable_cluster --release --locked -- --nocapture
PHASE11_RESULTS_DIR=/tmp/durable-cluster-results cargo test --test perf_durable_cluster --release --locked three_durable_voters_dataset_and_concurrency_characterization -- --ignored --nocapture --test-threads=1
```

The `PHASE11_*` names above are retained historical result-protocol identifiers for compatibility with archived benchmark data; they are not current product milestones.

The harness uses the locked `tokio-postgres` client directly so connection handshakes, queries, catch-up and concurrent cases have cancellable deadlines. Connection-driver tasks and worker tasks are aborted on cancellation, and owned server processes are stopped on exit, including partial startup failure. Timed operations are never retried; leadership turnover, ambiguous mutation outcomes, unexpected SQL errors and incorrect values fail the run. Only initial discovery retries known pre-submission leadership states.

All query results are checked. Every client executes five untimed warmups before the shared start signal; throughput timing begins before release and ends at the last worker completion. Reads use 100 sequential samples or 64 samples per concurrent client, and writes use 16 measured samples per concurrent client. These are bounded CI characterization samples, not a stable production p99 or saturation study. Both warmup and measured writes are verified on all voters.

Dataset sizes are 100/1,000/10,000/25,000 rows, seeded in 256-row batches. These read predicates currently use scan-and-filter execution, not an indexed point lookup. Dataset size and retained Raft history both change across scenarios; this suite alone cannot attribute a slowdown to either one. Strong reads continue to use the existing replicated-log barrier. No production consensus, storage, acknowledgement or read-consistency behavior is modified by the harness.

The dedicated `durable-cluster-bench` workflow checks formatting, runs smoke/regression tests, and then runs the ignored characterization. Machine-readable records report actual warmup and measured counts; a completion record and 27 distinct results are required. Failed/incomplete runs must not be treated as successful measurements. These results are for three voters on one runner, not three independent hosts.

## Confidence gate

`tests/confidence_yaml.rs` protects the machine-readable distributed boundary. `tests/sql_compatibility_profile.rs` separately locks the SQL anti-overclaim boundary. Production readiness, production HA, HPA safety, arbitrary-follower linearizable reads, automatic strong-read routing, timestamp-target PITR, automatic DR and full PostgreSQL compatibility remain false/not claimed; exact committed-index PITR is the bounded recovery claim.

## Lint, adversarial and PostgreSQL reference gates

`make lint` runs rustfmt and Clippy with warnings denied. `make adversarial` exercises focused edge/failure suites. `make tpch-correctness` compares checked-in Q1-Q22 output with PostgreSQL 16 on a deterministic small dataset and runs the selected semantic differential suite. `make sql-differential` runs just the reusable semantic differential suite.

## Deployment-manifest gate

CI performs Helm lint/default render, auth-required render, explicit identity-migration render, TLS render, rejection of incomplete migration configuration, and rejection of unsafe HPA configuration.

`auth.existingSecret` must be paired with an exact 64-hex `auth.migrationSha256`; auth-required startup from already initialized replicated state needs neither. TLS rendering configures SQL TLS only. A successful static manifest render does not prove the managed lifecycle by itself; the separate kind gate supplies that deployment evidence for the opt-in managed profile only and does not establish HPA safety, rolling upgrades or production security.

## Documentation validation

For documentation changes, verify relative links, environment names/defaults against their runtime readers, explicit `--bin` in server `cargo run` commands, one SQL statement per client request, and compatibility claims against `SQL_COMPATIBILITY.yaml`. Repeated `psql -c` options share a connection and can validate session read modes; a single compound `SET ...; SELECT ...` cannot. Keep raw manifests and chart/release examples aligned with the same identity model.

Report which checks actually ran, including missing toolchains or Docker. A workflow that has started is not a passing gate, and a historical green commit does not validate a new documentation/configuration head.

## What green CI means

Green CI means the exact checked commit passed the repository's current executable gates. For the distributed path it supports replicated tables, SQL-aware snapshots, coordinated membership, replicated SCRAM identity, tested backup/recovery, the leader-path read-consistency contract, the explicit process/Kubernetes reconciliation profile and exact committed-index archived recovery under the tested scenarios. For SQL compatibility it supports only the capability slices recorded in `SQL_COMPATIBILITY.yaml` and their named executable evidence.

It still does **not** mean production readiness, complete PostgreSQL compatibility, SQL transaction blocks/distributed transactions, linearizable arbitrary-follower reads, automatic strong-read routing, arbitrary Helm/HPA scaling, timestamp-target PITR or automatic DR, security certification, rolling-upgrade safety, or performance superiority outside measured workloads.
