# NeuralBase threat model

**Updated:** 2026-09-13  
**Scope:** pre-1.0 standalone and clustered development/research deployments

This is a risk inventory, not a security certification.

## Sensitive assets

- SQL query text and application data in RocksDB;
- Raft log entries carrying deterministic table effects, strong-read control barriers and SCRAM verifier identity commands;
- logical snapshots/staged snapshot metadata containing table and identity state;
- Raft term/vote/log/membership metadata;
- standalone credential files and clustered legacy migration files;
- TLS private keys/CA material;
- NBBK/NBEC operator backups and their separately managed encryption keys;
- managed-controller desired topology, durable controller state and Kubernetes object ownership metadata;
- metrics and diagnostics.

## Trust boundaries

1. PostgreSQL client → SQL listener.
2. Raft node → Raft node; default TCP is trusted-network-only, optional TLS provides transport protection.
3. Process → local disk; RocksDB and migration files are trusted durable/operator inputs and are not transparently encrypted at rest.
4. Operator configuration → peer/bootstrap/migration/security configuration.
5. Phase-7 controller → local management socket or explicit Kubernetes context/namespace; the controller principal and its durable root are trusted for the managed profile.

## Authentication and identity risks

Clustered identity is replicated rather than per-node. `CREATE USER`, `ALTER USER`, and `DROP USER` route through Raft; authentication reads the replicated RocksDB registry on every node.

The identity command codec carries SCRAM verifier material and cannot encode plaintext passwords. This reduces consensus-log exposure compared with replicating raw passwords, but SCRAM verifier material remains sensitive and must be protected at rest and in backups/snapshots.

PostgreSQL MD5 password-hash material is intentionally rejected from clustered replication/migration because it is reusable authentication material. Standalone mode retains legacy local compatibility and therefore has a different risk boundary.

Authentication reads the selected node's locally applied identity without a fresh quorum barrier. A lagging follower can temporarily accept credentials that were rotated or dropped on the leader. Existing authenticated sessions are not disconnected by user DDL. The server does not enforce table privileges or restrict user DDL to an administrator role; authentication alone is not an authorization policy.

### Legacy migration risk

A legacy registry is imported only when the operator supplies `NEURALBASE_IDENTITY_MIGRATION_SHA256` matching the exact selected file. The parser rejects unknown/malformed state, duplicates and MD5 credentials. This prevents silent selection/merge of divergent per-node registries, but the chosen migration file and its digest decision remain trusted operator actions.

After migration, remove the migration source from deployment mounts where practical; it is no longer live authority.

## Replicated-log and snapshot confidentiality

Table commands, strong-read barrier entries, identity verifiers and logical snapshots can reveal sensitive operational/application state. Raft TLS protects transport only when enabled. NeuralBase does not claim encryption at rest for RocksDB, Raft logs or snapshots.

## Consensus and membership risks

Required Raft persistence failures fail-stop the node. Replicated success waits for quorum commit and confirmed local durable apply.

Learners do not count toward quorum. Promotion/removal use coordinated configuration changes; current-leader removal requires transfer. Finalized membership is persisted and removed-node tombstones prevent a stale disk from silently rejoining as a voter.

Phase 7 adds an opt-in managed reconciler that obtains a quorum/apply authority observation, plans one deterministic next action, and rechecks leader/term/membership generation inside the serialized Raft path before membership changes. The process adapter uses a private same-user management socket and pidfd-based retirement checks. The Kubernetes adapter uses an explicit context/namespace, ownership/UID checks, managed-field drift rejection and resource-version-guarded replica patches. Storage is retained after member retirement.

Residual risks still include operator misuse or compromise of the trusted controller principal/root, address/configuration mistakes, controller/Kubernetes API availability, lack of formal proof, lack of rolling-upgrade orchestration, and insufficient hostile-network/storage/upgrade chaos coverage. The managed profile does not make arbitrary StatefulSet/Helm/HPA replica changes safe and does not establish a hostile multi-tenant control plane.

## Snapshot/recovery risk

Snapshots are bounded/checksummed, staged before compaction and restored before InstallSnapshot success. They include identity, so a recovered member does not depend on an unrelated local credential mirror. Corrupt or regressive state fails closed.

Internal cluster snapshot catch-up is not itself an operator backup product. Phase 5 adds distinct NBBK/NBEC operator artifacts and a fresh-cluster restore lifecycle.

## Operator backup security

Operator backups contain application rows, catalog/state metadata and replicated SCRAM verifier material, so they remain sensitive even though plaintext user passwords are not represented in the replicated identity snapshot.

NBBK is integrity-protected but plaintext. NBEC v1 adds authenticated ChaCha20-Poly1305 confidentiality/integrity. The 32-byte decryption key is supplied out of band; it is not embedded in the artifact. The Unix key loader rejects group/other-readable key files, key debug output is redacted, and executable CLI evidence checks that raw key bytes are absent from normal command output. Wrong keys and authenticated-byte tampering fail before restore target creation.

NBEC v1 does not carry a key identifier. External key inventory/rotation is therefore an operator responsibility. Old keys must remain available while backups encrypted under them are retained. RocksDB databases and internal Raft logs remain outside this backup-container encryption boundary and are not transparently encrypted at rest.

## Read-consistency risk

The default `Local` mode intentionally performs no consensus coordination and may lag on a follower. Applications requiring the Phase-6 strong-read contract must select `Leader` or `Linearizable` and connect to the current serving Raft leader.

Strong reads fail closed rather than silently downgrading: followers return not-leader, recovering/non-serving nodes return catching-up, and an isolated former leader cannot successfully complete the quorum barrier. The current implementation uses a replicated control/log entry for every strong read, so heavy strong-read workloads can add consensus latency and log churn.

NeuralBase does not automatically route strong reads from followers to the leader and does not claim arbitrary-follower linearizable reads. A client that sends a security-sensitive linearizable read to an arbitrary node must treat explicit not-leader/catching-up failure as failure and retry only through an application/operator-controlled leader-discovery strategy.

## Transport/security posture

SQL TLS and Raft mTLS are configuration-dependent rather than secure-by-default production profiles. In a TLS-enabled build, configuring the SQL acceptor requires SSLRequest/TLS and rejects plaintext startup. Helm TLS settings configure SQL only; Raft mTLS needs its separate enable switch, CA and certificate-name configuration. See [CONFIGURATION.md](CONFIGURATION.md#tls). Certificate provisioning/rotation/revocation remain operator responsibilities. A compromised node with valid cluster credentials remains inside the Raft trust boundary.

The Phase-7 local-process adapter is a trusted-host development profile and uses plaintext Raft. The managed Kubernetes profile expects an isolated trusted namespace/network and an external controller principal with the documented RBAC permissions. Member pods do not mount Kubernetes API credentials, but compromise of the controller principal can still create/patch managed objects within its Role. This is not a production multi-tenant security boundary.

## Current production blockers

Stronger production claims require secure-by-default deployment profiles, certificate/secret lifecycle, richer authorization/auditability, broader network/storage/upgrade chaos testing, managed upgrade/rollback semantics, controller-principal hardening, and production resource/performance characterization. Arbitrary Helm/HPA scaling remains unsupported even though the explicit Phase-7 managed profile is lifecycle-tested. Arbitrary-follower strong-read routing and lower-overhead ReadIndex/lease optimization remain unimplemented. PITR and automatic DR remain unimplemented; Phase-5 backup encryption does not imply general database-at-rest encryption.

## Evidence references

- `src/read_consistency.rs` / `src/read_barrier.rs` — Phase-6 session contract and strong-read barrier.
- `tests/phase6_*` — partition, membership, recovery, restart/restore and real-process read evidence.
- `src/replicated_identity*.rs` — verifier-only identity, storage and strict migration.
- `src/replicated_state_machine.rs` — atomic replicated apply boundary.
- `src/replicated_snapshot*.rs` — logical snapshot lifecycle.
- `src/consensus/membership.rs` / `src/consensus/raft.rs` — membership and consensus behavior.
- `tests/phase3_membership.rs` — learner/joint-consensus lifecycle.
- `tests/phase4_identity*.rs` — identity failover/recovery/membership/process evidence.
- `tests/raft_persistence_fail_closed.rs` and replicated SQL snapshot/process suites — durability/failure evidence.
- `src/backup*.rs`, `src/offline_backup.rs`, `src/online_backup.rs`, `src/restore.rs` and `tests/phase5_*` — operator recovery/security evidence.
- `src/operator.rs`, `src/consensus/operator_control.rs`, `src/operator_admin.rs`, `ops/neuralbase_operator.py`, `ops/neuralbase_kubernetes.py` and `tests/phase7_*` — guarded managed membership/deployment evidence.
- `CONFIDENCE.md` / `CONFIDENCE.yaml` — machine-readable claim boundary.