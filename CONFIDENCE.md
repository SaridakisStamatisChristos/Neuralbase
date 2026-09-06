# Confidence Report

**Updated:** 2026-09-06

This report is scoped deliberately. NeuralBase has two substantial, tested pieces of engineering: a local SQL/MVCC engine and a Raft consensus subsystem. They are **not yet one replicated SQL state machine**.

## Strongest evidence

- PostgreSQL wire-protocol, parser/binder, DML, MVCC/HLC and RocksDB paths have executable integration/adversarial coverage.
- `tests/tpch_correctness.rs` contains PostgreSQL 16 row-for-row reference comparisons for all Q1-Q22 on the deterministic small test dataset; Q1 and Q6 also have additional deterministic correctness checks.
- The Raft subsystem has election, log replication, snapshots/membership machinery and adversarial tests.
- Multi-process startup uses real TCP/TLS transport with logical Raft IDs separated from connectable peer addresses; loopback transport regression coverage verifies the mapping path.
- Raft shutdown remains interruptible when the bounded committed-entry apply channel is backpressured.
- Docker, raw Kubernetes and Helm configuration use explicit peer mappings and writable persistent user-registry paths; unsafe automatic HPA is rejected for fixed membership.
- User-registry persistence restores the previous durable in-memory state when a persistence attempt fails.

## Hard boundary

**SQL DDL/DML is not currently submitted through the Raft `ClientCommand`/committed-entry apply path.** A write to one node mutates that node's local RocksDB state. The existence of a healthy Raft quorum therefore does not make that SQL write durable on a majority of database nodes.

Accordingly:

- `production_ready: false`
- `distributed_sql_replication: false`
- automatic SQL failover is not claimed
- Kubernetes/Helm examples are fixed-membership development/research deployments, not an HA database service

These boundaries are machine-gated in `CONFIDENCE.yaml` and `tests/confidence_yaml.rs` so later documentation cannot silently drift back to an HA claim.

## Remaining high-value correctness work

1. Define a deterministic replicated SQL mutation command format.
2. Make Raft client acknowledgement occur only at the documented quorum-commit/local-apply durability point.
3. Apply every committed SQL mutation identically on each member, including deterministic INSERT keys and schema/user changes.
4. Replace best-effort Raft persistence with fail-closed stable-storage semantics.
5. Add separate-process crash/restart, partition, re-election and state-convergence tests.
6. Add coordinated Raft membership-change and operator recovery workflows.

## Confidence interpretation

`CONFIDENCE.yaml` retains an internal regression score for the **scoped** local engine + Raft subsystem. It is not a statistical failure probability and it is not a production-readiness score. Concrete executable evidence and explicit limitations take precedence over the number.
