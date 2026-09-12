# Distributed semantics

NeuralBase has tested Raft paths for persistent table replication, SQL-aware snapshot/recovery, coordinated membership changes, strongly consistent clustered identity, and explicit Phase-6 read-consistency modes. This document defines those guarantees and the remaining boundaries.

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

Authentication reads each node's locally applied identity; it does not establish a new quorum barrier on every login. A lagging follower can therefore observe older credentials after a leader-acknowledged rotation/drop. Already authenticated sessions are not disconnected by user DDL. The strong-read session setting fences SQL reads, not authentication.

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

## Operator backup and fresh-cluster recovery

Phase 5 adds a distinct operator recovery lifecycle. NBBK v1 contains a bounded/checksummed logical SQL+identity snapshot, explicit compatibility/boundary metadata and committed membership recovery semantics. Offline creation requires the source RocksDB lock to be free. Online creation is leader-coordinated around a confirmed barrier and stable durable frontier; concurrent activity must order around the recorded boundary or make the attempt retry/fail closed.

Offline creation also requires persisted Raft state and committed membership; standalone-only storage is unsupported. A stopped follower's backup boundary may be behind the leader's latest acknowledged writes. The tool validates the captured boundary rather than claiming it is the globally newest recovery point.

NBEC v1 provides authenticated ChaCha20-Poly1305 encryption around the logical backup. Wrong-key/tampered artifacts fail authentication before restore target creation. Keys are out-of-band and are never stored inside the backup.

Restore is fresh-target-only. It creates one fresh single-voter recovery generation, tombstones historical source IDs, preserves the backed-up logical/Raft boundary, and publishes only after complete staged verification. Cluster recovery then adds fresh learners through the existing snapshot/log and joint-consensus membership lifecycle. It never copies one restored consensus disk to create several voters.

Interrupted restore remnants remain hidden non-authoritative stages and are never automatically resumed. A retry constructs a fresh stage. The operator procedure and compatibility/key rules are in `ops/RUNBOOK.md`.

## Deployment boundary

The checked-in Compose/Kubernetes/Helm assets still describe a static process topology. The consensus layer can change membership, but no controller automatically sequences StatefulSet replica changes with learner admission/catch-up/promotion/removal. HPA therefore remains intentionally disabled/rejected.

## Read consistency

Phase 6 exposes a per-session contract with three modes:

- `Local` — the backward-compatible default. The query reads locally applied state and performs no consensus coordination. A follower may therefore return state behind the latest committed cluster state.
- `Leader` — requires clustered mode, a serving-ready current leader and a successful current-term replicated barrier before query execution.
- `Linearizable` — requires the same current-leader barrier and relies on the Raft client-command invariant that success is emitted only after quorum commit and confirmed durable local apply. The SQL read executes only after that applied frontier is established.

The session surface is:

```sql
SET neuralbase_read_consistency = local;
SET neuralbase_read_consistency = leader;
SET neuralbase_read_consistency = linearizable;
```

Strong modes never silently fall back to `Local`. A follower returns an explicit not-leader error, and a reconstructing/recovering member whose serving-readiness gate is closed returns a catching-up error. An isolated former leader cannot complete a new quorum barrier and therefore cannot manufacture a successful authoritative/linearizable read.

The current Phase-6 implementation deliberately uses a Raft log/control entry as the barrier rather than a separate ReadIndex/lease RPC. This is more expensive—one consensus log entry per strong read—but it reuses the already-tested quorum-commit + confirmed-apply acknowledgement boundary. A future ReadIndex optimization may reduce that cost without changing the public consistency contract.

Arbitrary-follower linearizable reads and automatic follower-to-leader strong-read routing are **not** implemented. Clients requesting `Leader` or `Linearizable` must reach the current leader.

## What green distributed evidence supports

- deterministic replicated table commands and concrete apply;
- follower rejection and quorum+confirmed-apply acknowledgement;
- durable/fail-closed Raft persistence;
- logical SQL+identity snapshots with safe compaction/install ordering;
- empty-storage snapshot+suffix reconstruction;
- learner admission/catch-up, joint-consensus promotion/removal and durable finalized membership;
- stale removed-node protection;
- replicated SCRAM identity, strict migration, failover rotation/drop and process-level authentication convergence;
- versioned offline/online backup, independent verification, authenticated encrypted backup, fresh-target restore and fresh-generation cluster recovery;
- explicit `Local`/`Leader`/`Linearizable` read modes, including immediate read-after-write, stale-former-leader partition rejection, follower rejection, leadership transfer, learner/promotion membership transitions, recovery readiness, restart, restored-cluster bootstrap and real-process concurrent read/write evidence.

Still open before stronger production claims: automatic operator membership reconciliation, arbitrary-follower strong-read routing/optimization, PITR and automatic DR, broader authorization/security, upgrade/storage-chaos evidence and production performance characterization.
