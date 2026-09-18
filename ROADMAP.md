# NeuralBase roadmap

NeuralBase is a pre-1.0 experimental SQL engine. The distributed correctness baseline includes replicated persistent table mutations, SQL-aware snapshot/recovery, coordinated Raft membership changes, strongly consistent replicated SCRAM identity, the tested operator backup/restore/fresh-cluster recovery model, explicit read-consistency modes, managed membership reconciliation, the bounded SQL semantic/compatibility profile, and the completed Phase-10/11 evidence-driven performance work on persistent query routing, durable Raft persistence, write pipelining and peer transport.

This is an engineering roadmap, not a release-date commitment.

## Completed Phase 1 — replicated persistent table mutations

- [x] Versioned deterministic commands for persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE`.
- [x] Canonical DML ordering and leader-side concrete `UPDATE`/`DELETE` materialization.
- [x] Followers reject persistent table mutations rather than mutating local RocksDB.
- [x] Current-term readiness barrier before leader materialization.
- [x] SQL success waits for Raft quorum commit plus confirmed durable local apply.
- [x] Atomic SQL effect + durable apply marker and replay-idempotent recovery.
- [x] RocksDB-backed Raft stable storage with fail-stop required-persistence handling.
- [x] Separate-process convergence, leader loss, catch-up, restart and acknowledged-write durability evidence.

## Completed Phase 2 — SQL-aware snapshot, compaction and recovery

- [x] Versioned deterministic logical SQL snapshot with explicit Raft boundary, apply index, HLC floor, catalog/table state, exact row bytes, bounds and checksum.
- [x] Consistent export from one RocksDB snapshot and fail-closed validation.
- [x] Atomic durable restore before volatile catalog/HLC publication.
- [x] Snapshot creation durably staged before Raft prefix truncation.
- [x] InstallSnapshot restores SQL state before successful acknowledgement.
- [x] Interrupted installation recovery and safe suffix-retention rules.
- [x] Empty-storage reconstruction of an already-configured member from snapshot + remaining Raft suffix.
- [x] Reconstructed-member leadership, acknowledged-write failover/restart, and repeated compaction-cycle evidence.

## Completed Phase 3 — coordinated Raft membership changes

- [x] Explicit learner/non-voting startup and admission.
- [x] Learner catch-up through the existing snapshot/log path before promotion.
- [x] Promotion through joint old/new voter configuration and finalization.
- [x] Safe non-leader voter removal through joint consensus.
- [x] Leadership-transfer constraint before removing the current leader.
- [x] Durable finalized membership that overrides stale process-local bootstrap peers after restart.
- [x] Removed-node tombstoning/stale-disk rejoin protection.
- [x] 3 → 4 → 3 lifecycle tests with writes preserved.

Phase 3 is a consensus capability, not an automatic Kubernetes scaling controller. The checked-in StatefulSet/Helm topology still requires deliberate membership operations; HPA-driven replica changes remain rejected.

## Completed Phase 4 — replicated strongly consistent identity

- [x] Versioned deterministic `NBRI` identity commands for initialize/create/alter/drop.
- [x] Leader-side password derivation before proposal; plaintext passwords are not representable in identity log commands.
- [x] Replicated identity accepts SCRAM verifier material only. PostgreSQL MD5 verifier material is rejected because it is reusable authentication material.
- [x] Identity mutation + durable replicated apply cursor are atomic in one RocksDB `WriteBatch`.
- [x] `CREATE USER`, `ALTER USER`, and `DROP USER` route through the Raft leader and wait for confirmed apply.
- [x] Cluster authentication reads the replicated RocksDB identity registry on every node.
- [x] Identity is included in the SQL-aware snapshot and restored atomically with the rest of replicated state.
- [x] Explicit legacy migration requires `NEURALBASE_IDENTITY_MIGRATION_SHA256` matching the exact selected strict-SCRAM `users.json`; malformed, duplicate, MD5, or digest-mismatched inputs fail closed.
- [x] Restart/replay, three-node convergence, leader-loss rotation/drop, snapshot bootstrap, learner promotion/removal, and real-process PostgreSQL authentication evidence.

Single-node mode deliberately keeps the historical local `users.json` behavior for backward compatibility. The replicated identity guarantee applies to configured clustered mode.

## Completed Phase 5 — operational backup / restore / disaster recovery

- [x] Explicit versioned NBBK logical backup envelope with bounded canonical metadata and integrity validation.
- [x] Offline consistent backup with RocksDB lock enforcement, restrictive staged publication and independent verification.
- [x] Leader-coordinated online consistent backup with exact committed/applied boundary and race/fail-closed evidence.
- [x] Independent verification with corruption/truncation/unsupported-version classification.
- [x] Fresh-target single-node restore preserving SQL/catalog/HLC/apply/replicated identity state.
- [x] Fresh recovery membership generation with historical source-ID tombstones.
- [x] Fresh-cluster rebuild through learner catch-up/promotion, new writes, leader loss and full restart.
- [x] Authenticated NBEC v1 encryption, strict key-file handling, wrong-key/tamper rejection and restrictive permissions.
- [x] Interrupted backup publication and stale restore-stage fail-closed evidence.
- [x] Compatibility/key semantics and operator disaster-recovery runbook.
- [x] Real OS-process backup/restore/auth/write/restart evidence.

Internal Raft snapshot catch-up remains distinct from operator backup. The standalone backup CLI is offline; online backup is currently an in-process coordinator API.

## Completed Phase 9 — archived recovery stream and exact-index PITR

- [x] Versioned `NBAR` archive segments with explicit committed Raft index/term, timeline, previous-hash link, payload hash, checksum, state-machine compatibility marker and strict fail-closed decoding.
- [x] Canonical archived SQL, replicated identity, membership and known control records; unknown committed command families fail closed.
- [x] Opt-in synchronous runtime archive fence: durable logical apply is followed by durable archive publication before confirmed Raft apply completes.
- [x] Crash-safe staged segment publication, restart verification, gap/overlap/duplicate/conflict detection, bounded stream enumeration and compaction-frontier startup guards.
- [x] Authenticated `NBPE` ChaCha20-Poly1305 archive encryption with strict out-of-band raw key-file handling and wrong-key/tamper rejection.
- [x] Exact baseline/index/latest recovery into a fresh single-voter recovery generation with historical source-ID tombstones and independent staged verification before target publication.
- [x] Timeline branching after earlier-point recovery so old future segments cannot join the new history.
- [x] Conservative verified rollover/retirement that preserves the parent on ambiguity and requires a child baseline at the old durable frontier.
- [x] Real OS-process evidence for exact target recovery, replicated identity rollback, branched future writes and restart persistence, plus encrypted replay and archive fault tests.
- [x] Runtime archive frontier/append/failure/startup-rejection metrics and explicit stream segment limits.

Phase 9 deliberately targets exact committed Raft **indexes**, not wall-clock timestamps. Timestamp-to-index mapping, automatic disaster detection/recovery, remote archive replication/object storage, and production-HA claims remain out of scope.

## Completed Phase 6 — explicit read consistency modes

- [x] Backward-compatible session `Local`/stale read mode with no consensus coordination.
- [x] Leader-authoritative mode with explicit current-leader/serving-readiness validation.
- [x] Linearizable leader-path mode implemented through a current-term Raft quorum barrier.
- [x] Strong reads proceed only after the existing client-command path confirms quorum commit and durable local apply through the barrier.
- [x] Strong reads fail closed on followers/recovering nodes and never silently downgrade to `Local`.
- [x] Stale former leader under partition cannot manufacture a successful strong read.
- [x] Tests across immediate post-write reads, concurrent reads/writes, leader loss/transfer, learners and promotion/finalization, recovery readiness, restart, snapshot/backup bootstrap, and real OS-process/TCP execution.
- [x] Strict session setting parser hardened against malformed Unicode boundaries.

The initial implementation deliberately uses one replicated control/log entry per `Leader` or `Linearizable` read. Arbitrary-follower linearizable routing, automatic follower-to-leader forwarding, and lower-overhead ReadIndex/lease optimization are not implemented and are not part of the Phase-6 claim.

## Completed Phase 7 — operator membership orchestration

- [x] Versioned deterministic desired-topology planning with monotonic revisions and immutable incarnation inventory.
- [x] Quorum/apply authority observations and leader/term/generation guards rechecked inside the serialized Raft loop.
- [x] Fresh non-voting learner creation, snapshot/log catch-up gating, joint-consensus promotion, leader transfer and finalized removal.
- [x] Durable managed storage identity, tombstone reuse rejection, retained storage, controller restart/reobservation and bounded retry behavior.
- [x] Independent Linux process adapter with authenticated SQL/SCRAM convergence across scale-out, leader restart, replacement and scale-in.
- [x] Opt-in Kubernetes adapter using one StatefulSet/PVC per incarnation, ownership/UID checks, resource-version guarded replica updates and retained PVCs.
- [x] Real disposable-kind lifecycle covering partial PVC-quota failure, configuration drift, 3→4 expansion, leader pod loss, fresh-identity replacement, 4→3 contraction and SQL/SCRAM convergence.
- [x] Exact PR-head CI #315 passed core tests, lint, confidence, adversarial, PostgreSQL reference, deployment-manifest and managed Kubernetes lifecycle gates on `fb7d099c6248f18b324d8817fb2878885dbe38a8` before claim synchronization.

The managed profile is explicit and opt-in. It does **not** adopt the static Helm/Compose topology, make raw replica-count changes safe, enable HPA, provide rolling-upgrade orchestration, or establish production HA/security readiness.

See [the operator guide](docs/PHASE7_OPERATOR.md).

## Phase 8 — SQL semantic depth and compatibility profile

The Phase-8 implementation candidate passed the full code-only PR-head CI gate in **CI #366** on `5edcfc9b8a5df383a1302f7371bc89cda08f6564`. Milestone closure still requires the synchronized documentation/profile head to pass exact-head CI and the merged `main` commit to pass post-merge CI.

- [x] Authoritative executable [`SQL_COMPATIBILITY.yaml`](SQL_COMPATIBILITY.yaml) with explicit supported/reference-tested/divergent/partial/unsupported boundaries and anti-overclaim tests.
- [x] Reusable PostgreSQL 16 semantic differential harness alongside the existing TPC-H reference gate.
- [x] Selected scalar semantic depth for SQL three-valued NULL logic, `CASE`, `COALESCE`, `NULLIF`, `IN`/`NOT IN`, selected casts, DATE subtraction and text equality/casing reference cases.
- [x] Fail-closed DML predicate boundary so unsupported `UPDATE`/`DELETE` WHERE clauses cannot become unfiltered mutations.
- [x] Fail-closed CREATE TABLE options/constraints that are parsed but not enforceable.
- [x] Window semantic routing that preserves implemented window expressions; named windows and explicit frame clauses fail closed.
- [x] Exactly-one-statement request boundary; compound simple-query input fails before partial execution.
- [x] Explicit SQL transaction boundary: `BEGIN`, `COMMIT`, `ROLLBACK` remain unsupported; ordinary statements remain independent/autocommit-style requests.
- [x] Bounded typed PostgreSQL extended-protocol parameters with declared scalar OIDs, NULLs, text encodings and selected binary encodings.
- [x] `ParameterDescription`, statement/portal lifecycle handling and fail-closed missing statement/portal behavior.
- [x] Real Rust `postgres` client prepared-statement evidence against the live server.
- [x] Deterministic typed parameter materialization before the existing parser/binder and replicated mutation path.

Phase 8 does **not** claim full PostgreSQL compatibility, distributed SQL transactions, arbitrary OID/type inference, binary row output, complete RowDescription metadata, full extended-protocol failed-transaction/error state, exact PostgreSQL NUMERIC/DECIMAL semantics, general constraint enforcement, locale-aware collations, broad timestamp/timezone semantics, or general named/frame window support.

## Completed Phase 10 — evidence-driven query-path performance

- [x] Reproducible exact-base/head characterization for parser/binder/planner, storage/executor, endpoint, distributed, memory and lifecycle scenarios.
- [x] Persistent simple scans bypass unrelated synthetic TPC-H fixture construction while preserving the historical built-in-table authority.
- [x] General persistent-only queries use a narrow persistent catalog first and fall back to the historical TPC-H-first route only on `TableNotFound`.
- [x] Dedicated correctness guards cover persistent-only parity, mixed/TPC-H fallback, built-in-name collisions and fail-closed fallback boundaries.
- [x] Same-run endpoint evidence recorded in [`docs/PHASE10_PROFILE.md`](docs/PHASE10_PROFILE.md), including the general persistent self-join reduction from roughly 740.61 ms p50 to 40.99 ms p50 on the measured Phase-10 environment.
- [x] Phase-10 documentation synchronization, PR #16 merge, and post-merge `main` CI completed successfully.

Phase 10 deliberately did not weaken quorum/apply acknowledgement, read consistency, snapshot/recovery, backup/PITR, membership or identity semantics. Its hosted-runner measurements are evidence for the recorded commits/topology, not universal latency guarantees.

## Completed Phase 11 — durable distributed performance and Raft transport/liveness

- [x] Incremental RocksDB Raft persistence replaces whole-log rewrite behavior while retaining migration/read compatibility.
- [x] Replicated-write proposal locking is narrowed so quorum/apply waiting can pipeline outside the serialized proposal-critical section.
- [x] Persistent ordered per-peer TCP/TLS writer actors use `TCP_NODELAY`, reconnect/retry semantics and leadership-bound transport epoch resets.
- [x] Per-process transport incarnation fencing detects logical-node restarts, invalidates stale reverse sessions and rejects retired-incarnation traffic.
- [x] Durable state-machine apply frontier is restored into Raft restart state without unsafe replay or premature serving readiness.
- [x] Election/transfer term fencing and randomized bounded campaign-retry deadlines prevent stale-reply promotion and deterministic split-vote phase locking.
- [x] Persistent-only general-query materialization remains effectively independent of unrelated-table count in the measured noise scenarios.
- [x] Three-durable-voter same-host characterization completed 27/27 benchmark cases with zero measured failures on head `4fb3303534bb27c15394f2a7952207b857fad2dd`.
- [x] Replicated INSERT characterization reached approximately 1,436 ops/s at one client and 3,561 ops/s at eight clients in that exact CI topology.
- [x] Full core, lint, confidence, adversarial, PostgreSQL reference, deployment-manifest and real guarded-kind membership lifecycle gates passed in CI #503 before PR #20 merged as `dcf10d366e4ef5ee468f24e191a97be8b10acb1e`.

Phase 11 does not establish production capacity, multi-host latency, automatic strong-read routing, ReadIndex/lease semantics, group commit, snapshot streaming/compression or a production-HA claim.

## P1 — remaining SQL compatibility depth

Continue only with executable evidence and without weakening replicated-state safety:

- exact NUMERIC/DECIMAL precision, scale and overflow behavior;
- broader date/time/timestamp/timezone semantics;
- locale/collation catalog and ordering semantics;
- enforceable constraints/defaults and additional persistent DDL only when durable meaning can be preserved;
- broader window/frame semantics;
- fuller PostgreSQL result metadata, result-format negotiation and extended-protocol error recovery;
- broader typed parameter coverage and safe inference only where deterministic semantics can be established;
- SQL transaction blocks only after their local/distributed semantics can be implemented explicitly rather than simulated.

See [SQL support](docs/SQL_SUPPORT.md#client-and-sql-limitations).

## P2 — remaining optimizer, execution and distributed performance

Only after lifecycle safety remains intact:

- Raft batching/group commit beyond the current pipelined proposal path;
- apply batching where acknowledgement and durable-apply semantics remain explicit;
- snapshot streaming/compression;
- optional ReadIndex/lease optimization for the strong-read contract;
- integrate the library ONNX optimizer with the live SQL planner only with end-to-end correctness/performance evidence;
- index access/predicate pushdown;
- cost model calibration, spills and memory accounting;
- multi-host, longer-duration and hardware-controlled write/read/failover/snapshot characterization beyond the current same-host CI profile.

Persistent peer connections, write pipelining and reproducible same-host three-voter characterization are completed Phase-11 capabilities. Future performance work must not weaken acknowledgement, read-consistency, snapshot, membership, identity, recovery, managed-reconciliation or SQL fail-closed semantics.

## P2 — production hardening

- authorization policy beyond the current user registry;
- credential freshness on follower authentication and an explicit existing-session revocation policy;
- certificate lifecycle/rotation;
- broader secret-management integration and backup key lifecycle automation;
- broader network/storage chaos testing;
- managed upgrade/rollback compatibility and reconciliation hardening;
- supply-chain/security automation with reviewed exceptions;
- production performance characterization.

## Explicit non-goals for the current stage

The project should not optimize for claims of production SQL HA, arbitrary-follower linearizable reads, automatic HPA-driven scaling, official benchmark certification, broad PostgreSQL compatibility unsupported by executable evidence, or distributed SQL transaction semantics that are not implemented.

## Definition of a stronger pre-1.0 distributed milestone

A future milestone suitable for stronger HA/database claims should demonstrate at minimum:

1. the current deterministic/quorum-applied replicated table-mutation guarantees;
2. the current SQL-aware snapshot/recovery guarantees;
3. the current coordinated and managed membership lifecycle;
4. the current replicated SCRAM identity model;
5. the current explicit leader-path `Local`/`Leader`/`Linearizable` read-consistency contract;
6. tested backup/restore and disaster-recovery procedures;
7. the explicit SQL compatibility contract with executable differential/reference evidence;
8. deployment/security assumptions matching an operator-tested topology.