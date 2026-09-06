# NeuralBase roadmap

NeuralBase is currently a pre-1.0 experimental SQL engine with a real Raft subsystem but without replicated SQL state-machine semantics. The roadmap prioritizes semantic closure and failure correctness before feature expansion.

This is an engineering roadmap, not a release-date commitment.

## P0 — replicated SQL semantics

### 1. Deterministic mutation command model

Define a versioned command representation for all replicated state mutations.

Acceptance criteria:

- deterministic serialization;
- explicit schema/version compatibility;
- no dependence on local wall-clock/parser/planner nondeterminism during apply;
- covered commands for required table/catalog/user mutation classes.

### 2. Route mutating SQL through Raft

The SQL leader path must propose the deterministic command instead of directly treating local RocksDB mutation as authoritative.

Acceptance criteria:

- explicit follower behavior (redirect/reject/proxy policy);
- client success tied to documented commit/apply durability point;
- no success response for an uncommitted mutation;
- replay is idempotent.

### 3. Replicated apply state machine

Committed commands must update every member's logical database state consistently.

Acceptance criteria:

- deterministic table/catalog/auth apply;
- snapshot/recovery compatibility;
- convergence tests across separate processes;
- duplicate/replay safety.

## P0 — consensus durability

### 4. Fail-closed stable Raft persistence

Required consensus-state persistence failures must become explicit node-failure/availability events rather than best-effort warnings.

Acceptance criteria:

- durable term/vote/log semantics documented;
- injected storage-failure tests;
- no acknowledged consensus transition that depends on a failed required persistence operation.

### 5. Crash/restart proof

Acceptance criteria:

- kill/restart individual processes during writes;
- recover persisted Raft + SQL state;
- prove acknowledged mutations survive the documented failure model;
- validate state convergence after catch-up.

## P1 — failover and membership operations

### 6. SQL leader failover

Acceptance criteria:

- acknowledged SQL state remains visible after leader loss;
- new leader rejects stale/conflicting mutation paths;
- client-facing behavior is documented and tested.

### 7. Coordinated membership changes

Acceptance criteria:

- add/remove member through Raft membership protocol;
- new member catch-up before serving as healthy;
- safe operator rollback/retry semantics;
- deployment replica changes cannot bypass consensus membership.

### 8. Operational recovery

Add documented backup/restore, snapshot inspection, node replacement, and disaster-recovery workflows.

## P1 — SQL semantic depth

Expand SQL only after replicated mutation semantics are credible.

Candidate areas:

- stronger PostgreSQL type/cast compatibility;
- richer window-function coverage;
- broader DDL/catalog semantics;
- transaction syntax/isolation behavior;
- extended wire-protocol coverage;
- systematic NULL/collation/date/time compatibility suites.

Each addition should update `docs/SQL_SUPPORT.md` and include reference/adversarial evidence where appropriate.

## P2 — optimizer and execution performance

- cost model calibration across scale factors;
- spill-aware joins/aggregates;
- memory accounting per operator/query;
- parallel execution scheduling improvements;
- optimizer fallback/explainability when the ONNX policy is unavailable or low-confidence.

Performance work must retain a reproducible benchmark methodology.

## P2 — production hardening

- authentication/authorization policy beyond current credential registry;
- certificate lifecycle and rotation procedures;
- rate-limit/admission-control observability;
- backup encryption and secret management integration;
- upgrade/rollback compatibility matrix;
- chaos testing across network partitions and storage faults;
- supply-chain/security automation with reviewed exceptions.

## Explicit non-goals for the current stage

Until P0 is closed, the project should not optimize for:

- large feature-count expansion;
- automatic horizontal scaling;
- claims of production SQL HA;
- official benchmark certification;
- broad compatibility claims unsupported by executable evidence.

## Definition of a credible pre-1.0 milestone

A future milestone suitable for stronger distributed-database claims should demonstrate all of the following in CI or reproducible integration tests:

1. deterministic replicated SQL mutations;
2. quorum-based commit semantics;
3. process crash/restart durability;
4. leader failover preserving acknowledged SQL state;
5. coordinated membership changes;
6. documented recovery procedures;
7. security/deployment assumptions that match the tested topology.
