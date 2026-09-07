# Confidence Report

**Updated:** 2026-09-07

NeuralBase is still pre-1.0 research/development software. The confidence boundary has moved: persistent table mutations now have a tested fixed-membership replicated path, but that path is deliberately narrower than a production-HA database claim.

## Strongest evidence

- PostgreSQL wire-protocol, parser/binder, DML, MVCC/HLC and RocksDB paths have executable integration/adversarial coverage.
- `tests/tpch_correctness.rs` compares the checked-in TPC-H Q1-Q22 queries row-for-row with PostgreSQL 16 on the deterministic small test dataset. This is correctness evidence for those cases, not official TPC-H certification or a production-scale performance result.
- Persistent table `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE` are represented as versioned deterministic replicated commands in clustered mode.
- `UPDATE`/`DELETE` predicates are evaluated on the leader after a current-term apply-readiness barrier; followers receive concrete row/key effects rather than re-planning the predicate.
- Followers reject persistent table writes before proposal instead of mutating local RocksDB state. A known leader ID is returned when available.
- Normal Raft client success is withheld until quorum commit and confirmed state-machine apply on the acknowledging node.
- Replicated SQL apply writes the SQL effect and durable replay marker atomically in RocksDB; replay is idempotent.
- Required Raft persistence load/save failures fail-stop the Raft node. Injected failure tests exercise the raw Raft core as well as the durable RocksDB store path.
- `tests/replicated_sql_process.rs` starts three real `neuralbase` OS processes with distinct SQL/Raft ports and distinct RocksDB directories. It exercises CREATE/INSERT/UPDATE/DELETE, leader loss, re-election, follower catch-up, full-cluster restart, and a write raced against leader kill.
- Replicated-SQL mode rejects legacy opaque Raft compaction/snapshot state until NeuralBase has a SQL-aware snapshot/restore format.
- Helm/default/auth/TLS rendering and unsafe fixed-membership HPA rejection remain CI-gated.

## Replicated SQL scope

`distributed_sql_replication: true` in `CONFIDENCE.yaml` means exactly this:

- configured clustered mode (`NEURALBASE_NODE_ID` plus durable RocksDB);
- fixed Raft membership;
- persistent table `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE`;
- follower writes are rejected rather than locally applied;
- leader acknowledgement occurs only after quorum commit and confirmed local state-machine apply;
- committed table mutations are deterministically applied and tested across separate processes, leader failover, catch-up, and restart.

It does **not** mean:

- `production_ready: true`;
- linearizable reads from arbitrary followers — reads are local and can lag committed state;
- replicated `CREATE USER`, `ALTER USER`, or `DROP USER` — authentication state remains per node;
- coordinated dynamic membership or safe HPA-driven scaling;
- SQL-aware Raft snapshots, bootstrap of a replacement node from snapshot, or operator-safe log compaction for replicated SQL;
- backup/restore or disaster-recovery workflows;
- complete PostgreSQL semantic compatibility;
- production SQL HA.

## Client failure semantics

An explicit follower rejection occurs before proposal and can be redirected/retried against the current leader.

A failure or timeout **after submission to a leader is outcome-uncertain**. The former leader may have replicated an entry to a quorum before the client observed the connection/error outcome. Non-idempotent mutations must therefore not be blindly replayed solely because the client did not receive success.

The process-level crash test enforces the one-way durability contract: whenever the client actually observes SQL success, that acknowledged effect must remain recoverable from the surviving quorum after leader loss.

## Remaining high-value correctness work

1. Define a SQL-aware snapshot/bootstrap format and safe node-replacement/log-compaction workflow.
2. Replicate authentication/user mutations, or define an equally explicit strongly consistent identity subsystem.
3. Add coordinated Raft membership changes and deployment reconciliation.
4. Add backup/restore and disaster-recovery procedures.
5. Add stronger read-consistency modes before describing arbitrary follower reads as current/linearizable.
6. Continue PostgreSQL compatibility and performance work only without weakening the above safety boundaries.

## Confidence interpretation

`CONFIDENCE.yaml` retains an internal regression score for its explicitly scoped engine and fixed-membership replicated-table path. It is not a statistical failure probability and it is not a production-readiness score. Executable evidence and the machine-readable limitations take precedence over the number.
