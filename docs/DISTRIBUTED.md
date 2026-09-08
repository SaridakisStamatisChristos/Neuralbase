# Distributed semantics

NeuralBase has tested Raft paths for persistent table replication, SQL-aware snapshot/recovery, coordinated membership changes and strongly consistent clustered identity. This document defines those guarantees and the remaining boundaries.

## Clustered startup

A node has a logical Raft ID and a connectable address. Clustered mode requires `NEURALBASE_NODE_ID` and durable RocksDB storage. Required persistence, snapshot and replicated-state failures fail closed rather than silently degrading durability.

## Replicated mutation acknowledgement

For persistent table mutations and clustered user DDL, local append is not success. A leader reply is withheld until quorum commit and confirmed durable local state-machine apply.

Followers reject these mutations before proposal. Errors/timeouts after submission to a leader remain outcome-uncertain.

## Persistent table state

Replicated table classes are `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE`. DML carries leader-selected deterministic concrete effects. A current-term readiness barrier precedes state-dependent materialization after election.

## Replicated identity

Clustered user DDL (`CREATE USER`, `ALTER USER`, `DROP USER`) uses versioned `NBRI` commands. Plaintext passwords are converted to SCRAM verifier material before proposal and are not representable in the replicated command codec. PostgreSQL MD5 verifier material is excluded because it is reusable authentication material.

The authoritative cluster registry is RocksDB-backed `ReplicatedIdentityState`. Identity changes and the replicated apply cursor are committed in one RocksDB batch. Authentication on leaders and followers reads that replicated state.

Standalone mode intentionally keeps the legacy local registry behavior.

### Legacy migration

A legacy `users.json` can initialize clustered identity only when the registry is uninitialized and the operator supplies `NEURALBASE_IDENTITY_MIGRATION_SHA256` matching the exact chosen file. The migration parser is strict and accepts SCRAM records only. Malformed files, duplicate users, MD5 records and digest mismatch fail closed.

If no legacy file exists, an auth-disabled fresh cluster may initialize empty identity as part of the first `CREATE USER`. A fresh auth-required cluster must already have replicated identity or use explicit migration.

## SQL-aware snapshots

The logical snapshot contains Raft boundary metadata, durable replicated apply/HLC state, catalog/table rows and the identity extension. Encoding is canonical, bounded and checksummed.

Creation is validated and durably staged before Raft prefix truncation. Installation validates/stages/restores durable state before publishing the Raft boundary and acknowledging success. Interrupted follower installation resumes from durable typed staging after restart.

Because identity is part of the snapshot, compaction and new learner/fixed-member catch-up do not create a second credential authority.

## Empty-storage member recovery

An existing configured member may restart from an empty RocksDB directory. It remains SQL-non-serving while receiving snapshot + remaining suffix and opens serving only after leader-confirmed catch-up. Tests cover later leadership, acknowledged writes, failure, restart and exact convergence.

## Coordinated membership changes

Phase 3 adds explicit consensus membership operations:

- start/admit a learner that does not vote;
- catch the learner up through log replication and, when needed, the snapshot path;
- reject promotion until catch-up is sufficient;
- promote through joint old/new voter configuration and finalize;
- remove a non-leader voter through joint consensus;
- require leadership transfer before removing the current leader;
- persist finalized membership across restart;
- tombstone removed identities so stale pre-removal state cannot disrupt the current quorum.

The primary regression path exercises 3 → 4 → 3 while preserving writes. Phase 4 additionally proves replicated identity survives learner snapshot bootstrap, promotion, credential rotation and removal.

## Deployment boundary

The checked-in Compose/Kubernetes/Helm assets still describe a static process topology. The consensus layer can change membership, but no controller automatically sequences StatefulSet replica changes with learner admission/catch-up/promotion/removal. HPA therefore remains intentionally disabled/rejected.

## Read consistency

Reads are local. There is no Raft ReadIndex/lease protocol for arbitrary follower queries, so follower reads may lag committed state. Serving-readiness gates prevent a fresh empty member from serving partial reconstruction; they do not make normal follower reads linearizable.

## What green distributed evidence supports

- deterministic replicated table commands and concrete apply;
- follower rejection and quorum+confirmed-apply acknowledgement;
- durable/fail-closed Raft persistence;
- logical SQL+identity snapshots with safe compaction/install ordering;
- empty-storage snapshot+suffix reconstruction;
- learner admission/catch-up, joint-consensus promotion/removal and durable finalized membership;
- stale removed-node protection;
- replicated SCRAM identity, strict migration, failover rotation/drop and process-level authentication convergence.

Still open before stronger production claims: automatic operator membership reconciliation, linearizable/defined stronger reads, backup/PITR/disaster recovery, broader authorization/security, upgrade/storage-chaos evidence and production performance characterization.
