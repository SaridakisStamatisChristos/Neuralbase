# NeuralBase

[![CI](https://github.com/SaridakisStamatisChristos/Neuralbase/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/SaridakisStamatisChristos/Neuralbase/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](Cargo.toml)

**NeuralBase is an experimental SQL engine in Rust** with a PostgreSQL-compatible wire endpoint, MVCC/RocksDB storage, vectorized/general execution, an ONNX join-order optimizer, and a multi-process Raft subsystem over TCP/TLS.

> [!IMPORTANT]
> NeuralBase is **pre-1.0 research/development software**. Configured clusters replicate persistent table mutations and SCRAM identity through Raft, support SQL-aware snapshot/recovery, and implement learner/joint-consensus membership changes. Successful replicated mutations wait for quorum commit plus confirmed durable local apply. This is still not a production-HA claim: follower reads are local and may lag, Kubernetes membership reconciliation is not automatic, operator backup/restore and fresh-cluster disaster recovery are implemented and tested to the documented Phase-5 scope; PITR, automatic disaster recovery, and broader authorization/security hardening remain open.

## Current highlights

- PostgreSQL wire endpoint with SCRAM authentication support.
- Parser/binder plus vectorized and row-oriented execution paths.
- MVCC, HLC timestamps, RocksDB persistence and bounded general execution.
- Deterministic replicated persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE`.
- Leader-side concrete `UPDATE`/`DELETE` materialization and follower write rejection.
- Raft acknowledgement only after quorum commit and confirmed durable state-machine apply.
- Versioned logical SQL snapshots with safe stage-before-compaction and restore-before-ACK ordering.
- Empty-storage member reconstruction from snapshot + remaining log suffix.
- Coordinated learner admission, catch-up, joint-consensus promotion/removal, durable finalized membership and stale-removed-node protection.
- Replicated `CREATE USER`, `ALTER USER`, and `DROP USER` with leader-side SCRAM derivation and no plaintext password in Raft identity commands.
- Cluster authentication from authoritative replicated RocksDB identity state.
- Strict digest-authorized migration from legacy SCRAM `users.json`; MD5 verifier material is rejected from replication.
- Real multi-process failover/restart coverage for SQL state and replicated authentication.
- Versioned NBBK offline/online backup, independent verification, crash-safe fresh-cluster restore, and authenticated NBEC backup encryption.
- Fresh-generation cluster recovery through one restored authority plus learner catch-up/promotion, failover and restart evidence.
- PostgreSQL 16 row-for-row TPC-H Q1-Q22 reference checks at a small deterministic scale.
- Docker Compose, Kubernetes StatefulSet and Helm development deployments.

## Quick start

### Single node

```bash
git clone https://github.com/SaridakisStamatisChristos/Neuralbase.git
cd Neuralbase
cargo build --release --locked
NEURALBASE_DB_PATH=/tmp/neuralbase-db cargo run --release --locked
```

Without `NEURALBASE_NODE_ID`, persistent table and user mutations retain the standalone local-storage behavior.

### Three-process topology

```bash
docker compose up --build -d --wait
```

The SQL endpoints are exposed on ports `5432`, `5433`, and `5434`. Each node owns independent RocksDB storage. Clustered startup requires durable RocksDB; configuring `NEURALBASE_NODE_ID` without `NEURALBASE_DB_PATH`/`DB_PATH` fails closed.

## Distributed write and identity semantics

Persistent table mutations and clustered user DDL are leader-routed. Followers reject before proposal. A newly elected leader establishes a current-term apply-readiness barrier before materializing state-dependent mutations.

A client-observed replicated success means the entry reached quorum commit and the local state machine confirmed durable apply. A timeout or disconnect after submission is still outcome-uncertain.

For user creation/rotation, the plaintext password exists only at the SQL/leader boundary needed to derive SCRAM material. The replicated identity command contains verifier material, not plaintext. Legacy PostgreSQL MD5 hashes are not accepted into replicated identity because possession of that material is sufficient for MD5 challenge responses.

## Snapshot and membership semantics

The logical snapshot contains SQL catalog/data/apply/HLC state and replicated identity state. It is validated and durably staged before log prefix truncation. Followers restore durable state before acknowledging InstallSnapshot.

The consensus layer supports learners and joint old/new voter configurations. A learner must catch up before promotion; removal is coordinated and the current leader must transfer leadership before being removed. Finalized membership is persisted and removed identities are protected against stale-disk rejoin.

The checked-in deployment does **not** automatically translate StatefulSet replica changes into these membership operations, so HPA remains intentionally disabled.

## Legacy identity migration

Clustered deployments upgrading from per-node `users.json` must explicitly select one strict SCRAM registry as authoritative:

```bash
sha256sum users.json
export NEURALBASE_USERS_FILE=/path/to/users.json
export NEURALBASE_IDENTITY_MIGRATION_SHA256=<exact-sha256>
```

The selected file is parsed strictly. Duplicate users, malformed fields, MD5 credentials, or a digest mismatch fail closed. Once migration is committed, every node authenticates from replicated RocksDB identity and the legacy file is no longer the live registry.

A fresh cluster with authentication disabled and no legacy file may initialize replicated identity with its first `CREATE USER`. A fresh cluster that starts with authentication required needs an already-replicated identity state or an explicitly authorized migration source.

## Capability snapshot

| Capability | Status |
|---|---|
| PostgreSQL wire endpoint | Implemented |
| Parser + binder | Implemented |
| MVCC + HLC + RocksDB | Implemented |
| Replicated persistent table DDL/DML | **Implemented and process-tested** |
| SQL-aware Raft snapshot + recovery | **Implemented and tested** |
| Learner/joint-consensus membership changes | **Implemented and lifecycle-tested** |
| Replicated SCRAM user/auth DDL | **Implemented and process-tested** |
| Legacy identity migration | **Explicit digest-selected SCRAM migration implemented** |
| Linearizable arbitrary-follower reads | **Not implemented** |
| Automatic Kubernetes membership reconciliation / HPA | **Not implemented** |
| Backup / restore / fresh-cluster DR | **Implemented and tested to Phase-5 scope** |
| Point-in-time recovery / automatic DR | **Not implemented** |
| Production SQL HA | **Not claimed** |

## Configuration

Important canonical variables include:

- `NEURALBASE_LISTEN_ADDR`
- `NEURALBASE_DB_PATH`
- `NEURALBASE_METRICS_PORT`
- `NEURALBASE_NODE_ID`
- `NEURALBASE_RAFT_ADDR`
- `NEURALBASE_PEERS`
- `NEURALBASE_RAFT_ELECTION_TIMEOUT_MS`
- `NEURALBASE_RAFT_TLS`
- `NEURALBASE_AUTH_REQUIRED`
- `NEURALBASE_USERS_FILE` — standalone registry path or clustered legacy-migration source path
- `NEURALBASE_IDENTITY_MIGRATION_SHA256` — exact digest authorizing clustered legacy identity import

## Verification

```bash
make test
make lint
make confidence
make adversarial
make tpch-correctness
```

Green CI is evidence for the exact checked commit and tested scopes, not a production-readiness or universal PostgreSQL-compatibility claim.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [SQL support](docs/SQL_SUPPORT.md)
- [Distributed semantics](docs/DISTRIBUTED.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Testing and evidence](docs/TESTING.md)
- [Threat model](docs/THREAT_MODEL.md)
- [Roadmap](ROADMAP.md)
- [Confidence model](CONFIDENCE.md)

## Project maturity

Phases 1–5 close replicated table mutations, SQL-aware snapshot/recovery, coordinated membership, replicated identity, and the documented operator backup/restore/fresh-cluster DR model under the repository's tested failure model. The next high-value correctness work is explicit stronger read-consistency semantics, followed by automatic membership orchestration and broader production hardening. PITR remains a later recovery extension.

## License

Apache-2.0. See [LICENSE](LICENSE).
