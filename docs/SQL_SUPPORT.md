# SQL support

This document describes the SQL surface implemented by the current NeuralBase development branch. It is a capability map, not a claim of full PostgreSQL compatibility.

## Protocol versus SQL compatibility

NeuralBase speaks the PostgreSQL wire protocol sufficiently for ordinary client interaction, but it is its own SQL engine. PostgreSQL protocol compatibility does not imply PostgreSQL semantic, catalog, type-system, extension, or planner compatibility.

## Query support

| Area | Status | Notes |
|---|---|---|
| `SELECT` | Implemented | General and vectorized paths |
| `WHERE` | Implemented | Boolean/scalar expressions supported by evaluator |
| Projection / aliases | Implemented | General projection path |
| `INNER JOIN` | Implemented | General executor |
| `LEFT OUTER JOIN` | Implemented | General executor |
| Multi-table `FROM` | Implemented | Cross-join growth is bounded |
| `GROUP BY` | Implemented | Expression grouping supported by current AST path |
| `SUM`, `COUNT`, `AVG`, `MIN`, `MAX` | Implemented | General aggregate path |
| `HAVING` | Implemented | Post-aggregate filtering |
| `ORDER BY` | Implemented | Multi-column ordering |
| `LIMIT` / `OFFSET` | Implemented | General executor |
| Scalar subqueries | Implemented | Subject to executor semantics |
| `IN` / `NOT IN` subqueries | Implemented | General evaluator |
| `EXISTS` / `NOT EXISTS` | Implemented | General evaluator |
| Derived tables | Implemented | `FROM (SELECT ...) AS alias` |
| CTEs / `WITH` | Implemented | Non-recursive resolution in current executor |
| Set operations | Implemented in current executor | Coverage depends on AST form/quantifier |
| Window functions | Selected support | Not a claim of full PostgreSQL window semantics |
| `CASE` | Implemented | Scalar evaluator |
| `COALESCE`, `NULLIF` | Implemented | Scalar evaluator |
| `UPPER`, `LOWER`, substring forms | Implemented | Selected scalar functions |
| `EXTRACT` | Implemented | Selected date/time extraction |
| `LIKE` / `NOT LIKE` | Implemented | Pattern evaluator |
| Arithmetic expressions | Implemented | `+`, `-`, `*`, `/`; division-by-zero is an error |

## Persistent table definition and mutation

Persistent DDL/DML requires a configured storage path.

| Statement family | Single-node mode | Configured fixed-membership cluster |
|---|---|---|
| `CREATE TABLE` | Local durable RocksDB/catalog mutation | Replicated through Raft |
| `DROP TABLE` | Local durable RocksDB/catalog mutation | Replicated through Raft |
| `INSERT` | Local MVCC/RocksDB mutation | Replicated through Raft |
| `UPDATE` | Local predicate evaluation + MVCC mutation | Leader materializes concrete row effects, then replicates them |
| `DELETE` | Local predicate evaluation + MVCC mutation | Leader materializes concrete keys, then replicates them |
| `CREATE USER` | Per-node user registry | **Still per-node; not replicated** |
| `ALTER USER` | Per-node user registry | **Still per-node; not replicated** |
| `DROP USER` | Per-node user registry | **Still per-node; not replicated** |

Clustered mode means `NEURALBASE_NODE_ID` is configured. It also requires durable RocksDB storage; startup fails if the node ID is configured without `NEURALBASE_DB_PATH`/`DB_PATH`.

### Clustered table-mutation semantics

For persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE`:

1. followers reject the write before proposal rather than mutating local state;
2. the leader establishes a current-term apply-readiness barrier before binding/materializing the mutation;
3. `UPDATE`/`DELETE` predicates are evaluated only on the leader against confirmed-applied state;
4. the replicated payload contains deterministic concrete row/key effects;
5. SQL success waits for Raft quorum commit and confirmed durable local state-machine apply;
6. committed effects are applied deterministically and replay-idempotently on members.

Follower write rejection uses SQLSTATE `25006`. Other replicated-write failures use an internal/system error response. A failure after submission to a leader is outcome-uncertain; clients must not blindly replay non-idempotent SQL solely because they did not observe success.

### Read consistency

`SELECT` continues to read local node state. This phase does not implement a Raft read-index/lease protocol, so arbitrary follower reads are **not claimed linearizable** and may lag a newly committed write until the follower applies it.

## Snapshot/recovery boundary

Replicated table mutations survive the tested process kill/re-election/full-cluster restart path using persisted Raft and RocksDB state.

That is distinct from SQL-aware Raft snapshot/bootstrap support. Legacy opaque Raft compaction/snapshot state is rejected in replicated-SQL mode because it cannot reconstruct the SQL catalog/data state safely. Snapshot-based replacement-node bootstrap remains future work.

## Execution limits

The general executor has explicit intermediate-row budgets. These prevent accidental unbounded materialization when a query plan degenerates into a large cross join or similar expansion.

A query rejected by a safety budget should not be interpreted as unsupported SQL syntax; it can be a deliberate execution-resource refusal.

## NULL and type behavior

NeuralBase implements its own scalar value/evaluation layer. Treat its behavior as engine-specific unless a test explicitly proves PostgreSQL-equivalent behavior for a construct.

Do not assume complete PostgreSQL implicit casting, collation, numeric precision, timezone, interval, or three-valued-logic compatibility beyond covered cases.

## TPC-H evidence

`tests/tpch_correctness.rs` executes the checked-in Q1-Q22 SQL constants against a deterministic small NeuralBase dataset and compares results against PostgreSQL 16 reference execution.

This is strong regression evidence for the exact tested dataset/query forms. It is **not** equivalent to complete SQL compatibility, official TPC-H certification, production-scale TPC-H performance, or correctness for arbitrary scale factors/data distributions.

## Adding SQL support

A new SQL feature should include, where relevant:

1. parser/binder coverage;
2. execution tests for normal and NULL/error cases;
3. persistence tests for DDL/DML;
4. replicated-state-machine semantics when the mutation must be cluster-wide;
5. PostgreSQL reference comparison when semantic parity is intended;
6. adversarial/budget tests for potentially explosive operations;
7. an update to this matrix.
