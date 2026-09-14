# SQL support

This document describes the SQL surface on the current development line (crate `0.1.0`, including the Phase 8 SQL semantic-depth work). It is a capability map, not a claim of full PostgreSQL compatibility. Parsing a PostgreSQL-looking statement does not guarantee its clauses are implemented by the binder or executor. The machine-readable contract is [`SQL_COMPATIBILITY.yaml`](../SQL_COMPATIBILITY.yaml).

## Query support

NeuralBase implements `SELECT`, filtering/projection, inner/left joins, multi-table `FROM`, grouping/aggregates, `HAVING`, ordering, limits/offsets, scalar and `IN`/`EXISTS` subqueries, derived tables, non-recursive CTEs, selected set/window operations, `CASE`, selected scalar/date functions, `LIKE`, and arithmetic. Exact PostgreSQL semantic parity is claimed only where executable comparison tests exist.

| Query surface | Current scope |
|---|---|
| Aggregates | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, grouping and `HAVING` |
| Set operations | `UNION`, `INTERSECT`, `EXCEPT`; do not infer parity for every quantifier/NULL combination |
| Windows | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD` with the implemented partition/order handling. Validated window queries route through the general query executor; explicit frames and named windows fail closed. |
| Scalar/NULL semantics | Selected no-`FROM` integer/boolean/text/DATE casts and expressions, including three-valued `NULL` logic, `IN`/`NOT IN`, `CASE`, `COALESCE`, and `NULLIF`, have PostgreSQL-16 differential coverage. This does not imply parity for every table/join/aggregate edge case. |
| `EXPLAIN SELECT` | Accepted, but emits the fixed text `PhysicalPlan: SeqScan -> Project`, not the actual full plan |
| `EXPLAIN ANALYZE SELECT` | Executes/times the query, but the current path suppresses its execution error; not a correctness check |
| Query budgets | Cross products limited to 50,000 rows; join intermediates to 200,000 rows; no general spill-to-disk implementation |

The server seeds the eight TPC-H table schemas and generates demo data for its query paths. General execution starts with a generated TPC-H catalog and adds non-conflicting persisted tables. Use distinct application table names; overwriting a built-in TPC-H name does not guarantee reads come from that persisted table. The ONNX `RlOptimizer` is available as a library/benchmark component and is not called by the live SQL planner.

## Client and SQL limitations

- **Exactly one statement per ordinary request.** Compound simple-query input such as `SELECT 1; SELECT 2` is rejected before either statement executes. NeuralBase does not implement PostgreSQL simple-query multi-statement behavior. In `psql`, use separate interactive statements or repeated `-c` options.
- **No SQL transaction blocks.** `BEGIN`, `COMMIT`, `ROLLBACK`, savepoints, and PostgreSQL failed-transaction session state are not implemented. Ordinary statements are independent/autocommit requests. Rejected transaction-control input does not poison the connection, but this is not a distributed-transaction contract. MVCC/transaction-manager library tests do not establish multi-statement SQL transaction semantics.
- **Partial extended protocol.** Parse/Bind/Execute/Describe/Close/Sync support a bounded subset. Parse-declared `BOOL`, `INT2`, `INT4`, `INT8`, `FLOAT4`, `FLOAT8`, `TEXT`, `VARCHAR`, `BPCHAR`, and `DATE` parameters can be materialized from text and selected PostgreSQL binary encodings; `NULL` parameters work; OID `0` inference fails closed. Statement Describe returns `ParameterDescription` followed by `NoData`; portal Describe returns `NoData`. Binary row results and full PostgreSQL result metadata/error-cycle semantics are not implemented. A binary result preference is tolerated only for commands such as the NeuralBase `SET` that emit no row data. A real Rust `postgres` client prepared-statement scenario is part of Phase-8 tests, but that does not imply arbitrary driver/ORM compatibility.
- **Limited DDL, fail closed for unsupported constraints.** Plain `CREATE TABLE`/`DROP TABLE` remain supported in their documented scope. `PRIMARY KEY`, `UNIQUE`, foreign-key/check/default declarations and table constraints are not enforced and are therefore rejected rather than silently discarded. SQL `ALTER TABLE`, explicit index DDL, views, grants and database/schema management are not implemented. The internal index advisor is a separate local mechanism.
- **Limited types.** Declared `DECIMAL`/`NUMERIC` map to `DOUBLE`, and durable `BOOLEAN` declarations use the historical integer representation; precision/scale and full PostgreSQL type semantics are not preserved. Timestamp/time-zone compatibility is not claimed.
- **Narrow DML binding, fail closed.** `INSERT` accepts `VALUES`; `INSERT ... SELECT` is unsupported. `UPDATE`/`DELETE` use a simple column/literal predicate representation. If a `WHERE` clause cannot be bound to that supported representation, the mutation is rejected rather than broadened to an unfiltered write. Rich `SELECT` predicate support does not imply the same DML support.
- **Authentication is not authorization.** The server does not enforce per-user table privileges or an administrator-only user-DDL policy. User rotation/drop does not terminate already authenticated sessions.

Implementation references: [`sql.rs`](../src/sql.rs), [`binder.rs`](../src/binder.rs), [`binder_phase8.rs`](../src/binder_phase8.rs), [`query_executor.rs`](../src/query_executor.rs), [`extended_protocol.rs`](../src/extended_protocol.rs), [`server_parts/session.rs`](../src/server_parts/session.rs) and [`server_parts/query.rs`](../src/server_parts/query.rs).

## Phase 8 executable semantic evidence

Phase 8 adds explicit evidence instead of promoting broad compatibility by implication:

- `tests/sql_compatibility_profile.rs` validates the machine-readable support/anti-overclaim contract in `SQL_COMPATIBILITY.yaml`.
- `tests/sql_semantic_differential.rs` compares the selected scalar/NULL/text/DATE/window matrix against PostgreSQL 16.
- `tests/phase8_dml_predicate_safety.rs` proves unsupported persistent DML predicates fail closed.
- `tests/phase8_ddl_safety.rs` proves unsupported constraint/default declarations fail closed.
- `tests/phase8_window_safety.rs` locks the supported basic window route and rejects named-window/explicit-frame semantics that are not implemented.
- `tests/phase8_transaction_protocol.rs` locks single-statement rejection, explicit transaction-block non-support, and connection recovery after errors.
- `tests/phase8_extended_protocol.rs` exercises the live bounded Parse/Bind/Execute/Describe lifecycle.
- `tests/phase8_postgres_client.rs` exercises the server with the real Rust `postgres` client and a typed prepared parameter.
- `tests/phase8_parameter_determinism.rs` verifies bound persistent-mutation parameters become concrete deterministic plans before the established mutation/Raft path.

These tests establish only their declared surface. They do not establish complete PostgreSQL compatibility.

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

Parameterized persistent mutations in the supported Phase-8 Bind subset are decoded and materialized into the existing concrete SQL/DML plan before a persistent proposal is formed. Followers do not independently reinterpret client parameter bytes.

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

The engine supports learner admission/catch-up, joint-consensus promotion/removal and durable finalized membership. Phase 7 also includes guarded managed reconciliation evidence. This remains a control-plane surface rather than SQL syntax, and the checked-in deployment intentionally does not claim automatic HPA-style database membership changes.

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

## TPC-H and PostgreSQL-reference evidence

`tests/tpch_correctness.rs` executes checked-in Q1-Q22 against a deterministic small NeuralBase dataset and compares results with PostgreSQL 16. `tests/sql_semantic_differential.rs` adds the selected Phase-8 scalar/NULL/text/DATE/window matrix. These are regression evidence for exact tested forms, not official TPC-H certification or complete PostgreSQL compatibility.
