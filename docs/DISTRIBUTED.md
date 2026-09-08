# Distributed semantics

NeuralBase has a tested fixed-membership replicated state-machine path for persistent table mutations and a SQL-aware snapshot/recovery path for safe Raft compaction plus reconstruction of an already-configured fixed member from empty local storage. This document defines those guarantees and the boundaries that remain outside them.

## Node identity and durable clustered startup

A Raft node has a **logical ID** and a **connectable address**. They are separate concepts.

Preferred configuration:

```bash
NEURALBASE_NODE_ID=node1
NEURALBASE_DB_PATH=/var/lib/neuralbase
NEURALBASE_RAFT_ADDR=0.0.0.0:7001
NEURALBASE_PEERS='node2=node2.internal:7001,node3=node3.internal:7001'
```

Clustered startup is fail-closed for persistent SQL prerequisites:

- `NEURALBASE_NODE_ID` requires `NEURALBASE_DB_PATH`/`DB_PATH`;
- persisted SQL catalog hydration errors abort clustered startup;
- required Raft stable-storage load/save/staged-snapshot failures fail-stop the Raft node.

Without `NEURALBASE_NODE_ID`, NeuralBase retains its single-node local DDL/DML behavior.

## Replicated mutation flow

For ordinary replicated SQL commands, append-only local logging is not success. The client reply is withheld until the command reaches quorum commit and the acknowledging node's state machine confirms durable apply.

Current replicated mutation classes:

- `CREATE TABLE`
- `DROP TABLE`
- `INSERT`
- `UPDATE`
- `DELETE`

DML carries a leader-selected commit timestamp and concrete primary-key/value effects. `UPDATE` and `DELETE` predicates are evaluated only by the leader against confirmed-applied state. Followers never reinterpret those predicates.

A newly elected leader commits a current-term readiness barrier before persistent mutation binding/materialization, ensuring its local SQL state machine has caught the committed prefix. Mutation materialization/proposal is serialized on the leader.

## Follower behavior and retries

Followers reject persistent table mutations before proposal. The PostgreSQL error identifies the known leader when available. This pre-submit rejection is safe to redirect/retry.

A timeout, disconnect, leadership-loss error, or other error after submission to a leader is outcome-uncertain. A non-idempotent mutation must not be blindly replayed solely because success was not observed.

Fresh fixed members reconstructed from empty storage have a separate serving-readiness gate. They can participate in Raft catch-up while SQL serving remains closed. Snapshot installation alone does not open the gate: the member must also complete a successful leader AppendEntries consistency exchange and apply through the advertised commit point.

## Deterministic apply and replay

Committed SQL commands are applied in Raft log order. The replicated SQL state machine:

- validates command version/shape and table identity;
- writes exact DML bytes at the leader-selected HLC timestamp;
- applies catalog/table changes deterministically;
- writes the SQL effect and durable SQL apply marker atomically in RocksDB;
- treats already-applied SQL Raft indices as replay/idempotence hits;
- restores the replicated HLC high-water mark after restart.

A state-machine apply error cannot result in SQL success.

## SQL-aware snapshot format

`src/replicated_snapshot.rs` defines a separate logical SQL snapshot format rather than reusing legacy opaque Raft bytes or raw RocksDB SST assumptions.

The versioned format contains:

- explicit magic and format version;
- Raft last-included index and term;
- latest durable replicated SQL apply index;
- latest replicated commit timestamp/HLC floor;
- durable table catalog/schema state;
- table IDs;
- canonical logical primary keys and exact encoded row values;
- bounded optional metadata extension;
- SHA-256 corruption digest.

Encoding uses canonical table and primary-key ordering, explicit lengths and explicit limits. Decoding rejects corruption, unsupported versions, duplicates/noncanonical ordering, table-ID mismatches, impossible apply metadata, truncation, oversized fields and trailing/ambiguous state.

The snapshot is logical and portable across RocksDB physical layouts. Secondary-index state is intentionally rejected until it has an explicit snapshot representation.

## Snapshot creation and safe compaction

SQL-aware compaction is limited to a Raft index that is both committed and locally applied.

The ordering is:

1. select the safe Raft `(index, term)` boundary;
2. obtain one consistent RocksDB snapshot;
3. export canonical catalog/table state plus durable SQL apply/HLC metadata;
4. validate the complete SQL snapshot artifact;
5. durably stage it as a **Creation** snapshot transition;
6. only then truncate/install the Raft snapshot boundary;
7. atomically publish active Raft state + active snapshot bytes and clear the staged candidate.

Raft prefix truncation therefore cannot become durable before a recoverable SQL snapshot artifact exists. If a crash leaves only a staged Creation candidate, startup discards it because the pre-compaction durable Raft log remains authoritative.

Repeated compaction cycles, retained post-snapshot suffixes, restart and continued mutation are covered by `tests/replicated_sql_snapshot_cycles.rs`.

## InstallSnapshot and restore semantics

A follower receiving a SQL-aware InstallSnapshot:

1. validates the snapshot checksum/format and embedded Raft boundary;
2. durably stages it as an **Installation** transition;
3. restores SQL catalog/data/apply-marker/HLC state;
4. only after SQL restore succeeds installs/persists the Raft snapshot boundary;
5. only after durable publication sends a successful InstallSnapshot reply.

The SQL restore replaces durable catalog/data/apply-marker state in one RocksDB `WriteBatch`; in-memory catalog/HLC publication happens only after that durable batch succeeds. Restore refuses apply-index or commit-timestamp regression and fails closed on malformed or unsupported state.

A crash after SQL restore but before active Raft publication leaves a durable Installation stage. Startup revalidates and idempotently restores that artifact, promotes the exact Raft boundary, publishes active state/snapshot, and clears staging. Restore failure leaves the node unavailable rather than serving partial state.

Raft snapshot installation also retains an existing local log suffix only when the local entry at `last_included_index` has the same term as the incoming snapshot boundary. Otherwise the potentially conflicting suffix is discarded.

## Fixed-member empty-storage bootstrap

The tested Phase 2 replacement scope is **recovery of a logical member that is already part of the configured fixed membership**.

A known member may restart with an empty RocksDB directory. It starts non-serving, receives the leader's SQL-aware snapshot, restores it, receives/applies the remaining Raft suffix, completes the leader consistency exchange, and only then becomes SQL-serving according to its Raft role.

`tests/replicated_sql_snapshot_bootstrap.rs` destroys one member's entire storage, writes a post-snapshot suffix while it is absent, restarts the same logical ID with a truly empty directory, requires snapshot + suffix convergence, transfers leadership to the reconstructed member, acknowledges another SQL write there, kills that leader, verifies the acknowledged write survives on the quorum, restarts the reconstructed member from its recovered disk, and verifies exact convergence/no duplicate MVCC effects.

This is not dynamic membership. It does not add an arbitrary new logical ID, promote a learner, change the voter set, or provide automatic operator-driven replacement.

## Stable Raft persistence

Raft term, vote, log, active snapshot-boundary metadata/bytes, and staged snapshot transitions are stored through RocksDB-backed persistence. Required persistence failures fail-stop in `RaftNode` itself; `FailClosedPersistenceStore` remains defense in depth.

## Read consistency boundary

Reads are not routed through a Raft ReadIndex/lease protocol. A follower serves local applied state and can lag the leader/commit frontier.

Therefore arbitrary follower reads are not claimed linearizable. Phase 2 serving readiness prevents a truly empty replacement from serving during reconstruction, but it does not convert normal follower reads into linearizable reads.

## Authentication boundary

`CREATE USER`, `ALTER USER`, and `DROP USER` remain per-node credential-registry mutations. They are not included in the replicated table state machine or SQL snapshot consistency claim.

## Fixed membership and deployment

Checked-in Compose/Kubernetes/Helm topologies remain fixed membership. HPA enablement is rejected because changing StatefulSet replica count is not a Raft membership transition.

Phase 2 does not relax this restriction. Coordinated membership changes are the next major distributed-safety phase.

## What is implemented versus still open

Implemented/tested for configured fixed-membership clusters:

- deterministic persistent table mutation commands;
- explicit follower write rejection;
- leader readiness barrier and concrete UPDATE/DELETE materialization;
- quorum commit + confirmed apply before SQL success;
- deterministic/idempotent RocksDB apply;
- durable/fail-closed Raft persistence;
- versioned deterministic logical SQL snapshots with corruption/bounds validation;
- stage-before-compaction and restore-before-InstallSnapshot-ACK ordering;
- crash recovery for interrupted snapshot installation;
- safe Raft suffix retention rules;
- repeated snapshot/compaction cycles and restart;
- same-ID fixed-member empty-storage snapshot + suffix reconstruction;
- reconstructed-member leadership, acknowledged-write failover and restart convergence;
- separate-process mutation convergence/failover/restart evidence from Phase 1.

Still open before stronger HA/production claims:

- coordinated dynamic membership and new-ID add/remove workflows;
- automatic node replacement/operator lifecycle;
- replicated auth/user state;
- stronger/linearizable read modes;
- backup/restore, point-in-time recovery and disaster recovery;
- broader partition/storage-fault/upgrade evidence and production performance characterization.

NeuralBase should therefore be described as an experimental SQL engine with tested **fixed-membership replicated table mutations and SQL-aware fixed-member snapshot recovery**, not as a production-ready HA database.
