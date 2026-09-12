# SQL support

This document describes the SQL surface implemented by the current NeuralBase development branch. It is a capability map, not a claim of full PostgreSQL compatibility.

## Query support

NeuralBase implements `SELECT`, filtering/projection, inner/left joins, multi-table `FROM`, grouping/aggregates, `HAVING`, ordering, limits/offsets, scalar and `IN`/`EXISTS` subqueries, derived tables, non-recursive CTEs, selected set/window operations, `CASE`, selected scalar/date functions, `LIKE`, and arithmetic. Exact PostgreSQL semantic parity is claimed only where executable comparison tests exist.

## Persistent mutations

| Statement family | Standalone mode | Configured cluster |
|---|---|---|
| `CREATE TABLE` / `DROP TABLE` | Local durable mutation | Replicated through Raft |
| `INSERT` | Local MVCC/RocksDB mutation | Replicated through Raft |
| `UPDATE` | Local predicate evaluation | Leader materializes concrete row effects, then replicates |
| `DELETE` | Local predicate evaluation | Leader materializes concrete keys, then replicates |
| `CREATE USER` | Local `users.json` registry | **Replicated SCRAM identity through Raft** |
| `ALTER USER` | Local `users.json` registry | **Replicated SCRAM identity through Raft** |
| `DROP USER` | Local `users.json` registry | **Replicated identity through Raft** |

Clustered mode means `NEURALBASE_NODE_ID` is configured and durable RocksDB is available.

## Cluster mutation semantics

For replicated table/user mutations:

1. followers reject before proposal;
2. the leader establishes current-term apply readiness before state-dependent materialization;
3. table `UPDATE`/`DELETE` replicate concrete effects;
4. user passwords are converted to SCRAM verifier material before identity proposal;
5. SQL success waits for Raft quorum commit and confirmed durable local apply;
6. replay at an already-applied Raft index is idempotent.

A failure after submission to a leader is outcome-uncertain.

## Identity semantics

Cluster authentication reads authoritative replicated RocksDB identity. Plaintext passwords are not representable in the replicated identity command format. PostgreSQL MD5 verifier material is not accepted into replicated identity or migration.

Legacy `users.json` is only a clustered migration source, selected by exact `NEURALBASE_IDENTITY_MIGRATION_SHA256`; it is not the post-migration live authority.

## Snapshot/recovery boundary

The SQL-aware logical snapshot includes catalog/table rows, durable replicated apply/HLC metadata and replicated identity. Snapshot creation is staged before compaction; installation restores durable state before success ACK. Fixed-member reconstruction and learner catch-up can therefore recover both table and authentication state from snapshot + suffix.

## Membership boundary

The engine supports learner admission/catch-up, joint-consensus promotion/removal and durable finalized membership. This is not a SQL statement surface and is not automatically driven by the checked-in Kubernetes manifests.

## Read consistency

Phase 6 adds a session-scoped NeuralBase `SET` surface for read consistency:

```sql
SET neuralbase_read_consistency = local;
SET neuralbase_read_consistency = leader;
SET neuralbase_read_consistency = linearizable;
```

`SET neuralbase.read_consistency ...` is also accepted. The setting applies to the current connection and new sessions default to `Local`.

| Mode | Semantics |
|---|---|
| `Local` | Read locally applied node state with no consensus coordination. This is the backward-compatible default and may be stale on a follower. |
| `Leader` | Require clustered mode, serving readiness and the current Raft leader; establish a current-term replicated barrier before query execution. |
| `Linearizable` | Require the current leader and the same replicated barrier; proceed only after quorum commit plus confirmed durable local apply through the barrier. |

The current implementation uses one Raft control/log entry per `Leader` or `Linearizable` read. A follower does not proxy or downgrade a strong read: it returns an explicit not-leader error. A recovering node whose serving-readiness gate is closed returns catching-up. Standalone mode cannot satisfy the strong modes and reports them as unsupported.

The strong-read barrier runs before binder/catalog/query execution for read statements, so the read does not bind against catalog state older than the established frontier. Both simple-query and extended-protocol execution paths carry the session mode.

Arbitrary-follower linearizable reads, automatic follower-to-leader routing, ReadIndex and lease-read optimization are not currently implemented.

## TPC-H evidence

`tests/tpch_correctness.rs` executes checked-in Q1-Q22 against a deterministic small NeuralBase dataset and compares results with PostgreSQL 16. This is regression evidence for the exact tested forms, not official TPC-H certification or complete PostgreSQL compatibility.
