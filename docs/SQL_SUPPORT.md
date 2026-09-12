# SQL support

This document describes the SQL surface on the current `main` development line (crate `0.1.0`, including Phases 1–6). It is a capability map, not a claim of full PostgreSQL compatibility. Parsing a PostgreSQL-looking statement does not guarantee its clauses are implemented by the binder or executor.

## Query support

NeuralBase implements `SELECT`, filtering/projection, inner/left joins, multi-table `FROM`, grouping/aggregates, `HAVING`, ordering, limits/offsets, scalar and `IN`/`EXISTS` subqueries, derived tables, non-recursive CTEs, selected set/window operations, `CASE`, selected scalar/date functions, `LIKE`, and arithmetic. Exact PostgreSQL semantic parity is claimed only where executable comparison tests exist.

| Query surface | Current scope |
|---|---|
| Aggregates | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, grouping and `HAVING` |
| Set operations | `UNION`, `INTERSECT`, `EXCEPT`; do not infer parity for every quantifier/NULL combination |
| Windows | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD` with implemented partition/order handling; general frame semantics and named-window resolution are not implemented |
| `EXPLAIN SELECT` | Accepted, but emits the fixed text `PhysicalPlan: SeqScan -> Project`, not the actual full plan |
| `EXPLAIN ANALYZE SELECT` | Executes/times the query, but the current path suppresses its execution error; not a correctness check |
| Query budgets | Cross products limited to 50,000 rows; join intermediates to 200,000 rows; no general spill-to-disk implementation |

The server seeds the eight TPC-H table schemas and generates demo data for its query paths. General execution starts with a generated TPC-H catalog and adds non-conflicting persisted tables. Use distinct application table names; overwriting a built-in TPC-H name does not guarantee reads come from that persisted table. The ONNX `RlOptimizer` is available as a library/benchmark component and is not called by the live SQL planner.

## Client and SQL limitations

- **One statement per request.** The ordinary parser returns the first parsed statement; subsequent statements are not executed. The read-consistency `SET` parser rejects compound input outright. In `psql`, use separate interactive statements or repeated `-c` options on one invocation.
- **No SQL transaction blocks.** `BEGIN`, `COMMIT`, `ROLLBACK` and savepoints are not bound by the SQL server. MVCC/transaction-manager library tests do not establish multi-statement SQL transaction semantics.
- **Partial extended protocol.** Parse/Bind/Execute/Sync exist, but Bind does not substitute parameter values or implement result-format negotiation, and Describe returns NoData. Do not assume PostgreSQL driver/ORM compatibility from connection success or the advertised `server_version=16.0`.
- **Limited DDL.** Column names/types are recorded; declared primary-key, unique, foreign-key, check and default constraints are not enforced. SQL `ALTER TABLE`, explicit index DDL, views, grants and database/schema management are not implemented. The internal index advisor is a separate local mechanism.
- **Limited types.** Declared `DECIMAL`/`NUMERIC` map to `DOUBLE`, and `BOOLEAN` maps to `INT`; precision/scale and full PostgreSQL type semantics are not preserved.
- **Narrow DML binding.** `INSERT` accepts `VALUES`; `INSERT ... SELECT` is unsupported. `UPDATE`/`DELETE` use a simple column/literal predicate representation. Unsupported predicate forms can be dropped during binding, so do not use complex DML predicates without a specific regression test. Rich `SELECT` predicate support does not imply the same DML support.
- **Authentication is not authorization.** The server does not enforce per-user table privileges or an administrator-only user-DDL policy. User rotation/drop does not terminate already authenticated sessions.

Implementation references: [`sql.rs`](../src/sql.rs), [`binder.rs`](../src/binder.rs), [`query_executor.rs`](../src/query_executor.rs), [`server_parts/session.rs`](../src/server_parts/session.rs) and [`server_parts/query.rs`](../src/server_parts/query.rs).

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

Standalone durability assumes RocksDB opened successfully. Standalone DDL can acknowledge after a logged persistence error, and a multi-row standalone insert is applied row by row; these paths do not provide the clustered atomic commit/apply contract.

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

User DDL is quorum-committed and applied on the leader before success. Authentication on another node reads that node's locally applied replicated identity without a fresh quorum barrier. Immediate credential revocation/rotation visibility on every follower is therefore not guaranteed; session read consistency does not change the authentication handshake.

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

Both `=` and `TO` are accepted, with case-insensitive mode names and optional single quotes. `stale` aliases `local`; `leader-authoritative` and `leader_authoritative` alias `leader`. This is a session setting, not SQL `SET LOCAL` transaction syntax. Setting a strong mode can succeed before its prerequisites are checked; the next read establishes the barrier. A read here means a query or `EXPLAIN`; mutations retain their own replication rules.

| Mode | Semantics |
|---|---|
| `Local` | Read locally applied node state with no consensus coordination. This is the backward-compatible default and may be stale on a follower. |
| `Leader` | Require clustered mode, serving readiness and the current Raft leader; establish a current-term replicated barrier before query execution. |
| `Linearizable` | Require the current leader and the same replicated barrier; proceed only after quorum commit plus confirmed durable local apply through the barrier. |

The current implementation uses one Raft control/log entry per `Leader` or `Linearizable` read. A follower does not proxy or downgrade a strong read: it returns an explicit not-leader error. A recovering node whose serving-readiness gate is closed returns catching-up. Standalone mode cannot satisfy the strong modes and reports them as unsupported.

The strong-read barrier runs before binder/catalog/query execution for read statements, so the read does not bind against catalog state older than the established frontier. Both simple-query and extended-protocol execution paths carry the session mode.

Arbitrary-follower linearizable reads, automatic follower-to-leader routing, ReadIndex and lease-read optimization are not currently implemented.

| SQLSTATE | Read-consistency failure |
|---|---|
| `22023` | Invalid targeted `SET` syntax or mode |
| `0A000` | Strong read requested without clustered Raft mode |
| `25006` | Strong read sent to a follower; response may include a leader ID |
| `57P03` | Member not ready to serve |
| `57014` | Five-second strong-read barrier timeout |
| `58030` | Other consensus/apply failure |

Example on one connection to the current leader:

```bash
psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 5432 -U anon -d postgres \
  -c 'SET neuralbase_read_consistency = linearizable' \
  -c 'SELECT 1'
```

## TPC-H evidence

`tests/tpch_correctness.rs` executes checked-in Q1-Q22 against a deterministic small NeuralBase dataset and compares results with PostgreSQL 16. This is regression evidence for the exact tested forms, not official TPC-H certification or complete PostgreSQL compatibility.
