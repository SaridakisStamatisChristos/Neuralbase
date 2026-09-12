# Confidence Report

**Updated:** 2026-09-12

NeuralBase remains pre-1.0 research/development software. The evidence boundary now includes replicated persistent table mutations, SQL-aware snapshot/recovery, coordinated Raft membership changes, strongly consistent replicated SCRAM identity, the documented Phase-5 operator backup/restore/fresh-cluster DR model, and explicit Phase-6 read-consistency modes on the leader path. It remains deliberately narrower than a production-HA database claim.

## Strongest evidence

- Persistent table `CREATE TABLE`, `DROP TABLE`, `INSERT`, `UPDATE`, and `DELETE` use versioned deterministic replicated commands in clustered mode.
- Leaders materialize state-dependent table effects after a current-term apply-readiness barrier; followers reject persistent writes before proposal.
- Replicated success waits for quorum commit and confirmed durable local state-machine apply.
- SQL snapshot creation is durably staged before Raft prefix truncation; follower installation restores durable state before success acknowledgement.
- Empty-storage member reconstruction, repeated compaction/restart, and real-process failover paths are tested.
- Phase 3 tests exercise learner admission/catch-up/promotion, joint-consensus 3 → 4 → 3 transitions, durable finalized membership and stale removed-node protection.
- Phase 4 routes clustered user DDL through Raft, derives SCRAM material before proposal, stores identity atomically with the replicated apply cursor, snapshots identity, and authenticates from replicated RocksDB state.
- Phase 4 tests cover three-node identity convergence, leader-loss password rotation/drop, crash/replay, snapshot reconstruction, learner promotion/removal, and real-process PostgreSQL authentication across restart/failover/rejoin.
- Strict legacy migration requires an exact `NEURALBASE_IDENTITY_MIGRATION_SHA256`; malformed, duplicate, MD5 or digest-mismatched input fails closed.
- Phase 5 adds versioned offline/online backup, independent verification, authenticated NBEC encryption, fresh-target restore, fresh-generation cluster rebuild, interruption evidence and a real-process recovery path.
- Phase 6 adds explicit `Local`, `Leader`, and `Linearizable` session modes. Strong modes require the current serving leader, use a current-term replicated barrier, and proceed only after quorum commit plus confirmed durable local apply through that barrier.
- Phase 6 tests cover follower rejection, stale former leader partitions, leader transfer, learner/promotion/finalization, recovery readiness, restart, Phase-5 restore bootstrap, concurrent real-process reads/writes and immediate linearizable read-after-write.
- PostgreSQL 16 TPC-H Q1-Q22 reference comparison remains part of CI at a deterministic small scale.

## Read-consistency scope

In configured clustered mode:

- every new session defaults to `Local` for backward compatibility;
- `Local` reads locally applied state without consensus coordination and may be stale on a follower;
- `Leader` requires the current serving leader and a successful current-term replicated barrier;
- `Linearizable` uses the same barrier and relies on the tested client-command invariant that success occurs only after quorum commit and confirmed durable local apply;
- followers reject strong reads explicitly rather than proxying or silently downgrading them;
- recovering/non-serving nodes fail strong reads with the catching-up boundary;
- an isolated former leader cannot complete the quorum barrier and cannot successfully serve a strong read;
- the current implementation spends one Raft control/log entry per strong read.

This scope does **not** include arbitrary-follower linearizable reads, automatic follower-to-leader read routing, ReadIndex, or lease-read optimization.

## Replicated identity scope

In configured clustered mode:

- `CREATE USER`, `ALTER USER`, and `DROP USER` are leader-routed replicated mutations;
- plaintext passwords are not encoded into the replicated identity command;
- the replicated credential representation is SCRAM verifier material;
- PostgreSQL MD5 verifier material is rejected from replication and legacy migration;
- identity mutation and the durable apply cursor share one RocksDB atomic batch;
- snapshots include identity so catch-up/recovery/membership transitions do not create a second authority;
- authentication on every node reads the replicated state.

Identity mutation ordering is consensus-backed, but login on a follower reads its locally applied registry without a fresh quorum barrier. Credential changes need not be visible instantly on lagging followers and do not revoke existing sessions. Standalone mode keeps the historical local registry for backward compatibility.

## Membership scope

The Raft layer supports explicit learner admission, learner catch-up, promotion through joint old/new voter configurations, coordinated removal, leadership-transfer constraints and durable finalized membership. This is consensus capability evidence.

It does **not** mean the checked-in Kubernetes/Helm manifests automatically reconcile arbitrary replica-count changes. HPA remains disabled because an operator/controller still must sequence deployment changes with the membership protocol.

## What the confidence claim still excludes

- `production_ready: true` or production SQL HA;
- linearizable reads from arbitrary followers or automatic strong-read routing;
- automatic Kubernetes membership reconciliation or safe HPA scaling;
- PITR or automatic disaster recovery beyond the documented manual fresh-cluster Phase-5 procedure;
- complete PostgreSQL semantic compatibility, SQL transaction blocks, bound parameters or constraint enforcement;
- live-server ONNX join-order planning or distributed query exchange;
- production-grade authorization/audit policy;
- broad upgrade/storage-chaos certification.

## Confidence interpretation

`CONFIDENCE.yaml` retains an internal regression score for an explicitly scoped system. It is not a statistical failure probability and is not a production-readiness score. Executable evidence and machine-readable limitations take precedence over the number.
