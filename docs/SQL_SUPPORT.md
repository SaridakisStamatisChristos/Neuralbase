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
| Window functions | Selected support | Implemented path exists; not a claim of full PostgreSQL window semantics |
| `CASE` | Implemented | Scalar evaluator |
| `COALESCE`, `NULLIF` | Implemented | Scalar evaluator |
| `UPPER`, `LOWER`, substring forms | Implemented | Selected scalar functions |
| `EXTRACT` | Implemented | Selected date/time extraction |
| `LIKE` / `NOT LIKE` | Implemented | Pattern evaluator |
| Arithmetic expressions | Implemented | `+`, `-`, `*`, `/`; division-by-zero is an error |

## Data definition and mutation

Persistent DDL/DML requires a configured local storage path.

| Statement family | Status | Durability scope |
|---|---|---|
| `CREATE TABLE` | Implemented | Local node |
| `DROP TABLE` | Implemented | Local node |
| `INSERT` | Implemented | Local node |
| `UPDATE` | Implemented | Local node |
| `DELETE` | Implemented | Local node |
| `CREATE USER` | Implemented | Per-node user registry |
| `ALTER USER` | Implemented | Per-node user registry |
| `DROP USER` | Implemented | Per-node user registry |

The phrase **local node** matters: these mutations are not currently routed through Raft and are not synchronously replicated to peer RocksDB instances.

## Execution limits

The general executor has explicit intermediate-row budgets. These prevent accidental unbounded materialization when a query plan degenerates into a large cross join or similar expansion.

A query rejected by a safety budget should not be interpreted as unsupported SQL syntax; it can be a deliberate execution-resource refusal.

## NULL and type behavior

NeuralBase implements its own scalar value/evaluation layer. Its current behavior should be treated as engine-specific unless a test explicitly proves PostgreSQL-equivalent behavior for a construct.

Do not assume complete PostgreSQL implicit casting, collation, numeric precision, timezone, interval, or three-valued-logic compatibility beyond covered cases.

## TPC-H evidence

`tests/tpch_correctness.rs` executes the checked-in Q1-Q22 SQL constants against a deterministic small NeuralBase dataset and compares results against PostgreSQL 16 reference execution.

This is strong regression evidence for the exact tested dataset/query forms. It is **not** equivalent to:

- complete SQL compatibility;
- official TPC-H certification;
- production-scale TPC-H performance;
- correctness for arbitrary scale factors or data distributions.

Q1 and Q6 also have additional deterministic correctness coverage in the repository.

## Adding SQL support

A new SQL feature should include, where relevant:

1. parser/binder coverage;
2. execution tests for normal and NULL/error cases;
3. persistence tests for DDL/DML;
4. PostgreSQL reference comparison when semantic parity is intended;
5. adversarial/budget tests for potentially explosive operations;
6. an update to this matrix.
