# Deployment

NeuralBase ships deployment assets for development, integration testing and architecture evaluation. They are **not** a production-HA database deployment.

## Single process

```bash
cargo build --release --locked
NEURALBASE_DB_PATH=/tmp/neuralbase-db cargo run --release --locked
```

Without `NEURALBASE_NODE_ID`, table and user DDL use the standalone local-storage/local-registry paths.

## Clustered startup requirements

Setting `NEURALBASE_NODE_ID` enables replicated state and requires durable RocksDB, persisted catalog hydration and Raft stable storage. A truly fresh/catching-up member keeps SQL serving gated until consensus catch-up reaches the leader-confirmed commit point.

## Environment reference

| Variable | Purpose |
|---|---|
| `NEURALBASE_LISTEN_ADDR` | SQL listener |
| `NEURALBASE_DB_PATH` | RocksDB directory; required in clustered mode |
| `NEURALBASE_METRICS_PORT` | Prometheus listener |
| `NEURALBASE_NODE_ID` | Logical Raft ID |
| `NEURALBASE_RAFT_ADDR` | Local Raft transport bind address |
| `NEURALBASE_PEERS` | Bootstrap logical-ID/address map |
| `NEURALBASE_RAFT_ELECTION_TIMEOUT_MS` | Base election timeout |
| `NEURALBASE_RAFT_TLS` | Select feature-gated Raft TLS |
| `NEURALBASE_AUTH_REQUIRED` | Require PostgreSQL authentication |
| `NEURALBASE_USERS_FILE` | Standalone registry path, or clustered **legacy migration source** path |
| `NEURALBASE_IDENTITY_MIGRATION_SHA256` | Exact SHA-256 authorizing the selected clustered legacy registry |

## Cluster identity bootstrap and migration

Clustered identity lives in replicated RocksDB state. Do not maintain separate live `users.json` files per pod.

For an upgrade with an existing legacy registry, select one strict SCRAM file as authoritative:

```bash
sha256sum users.json
NEURALBASE_USERS_FILE=/secure/migration/users.json \
NEURALBASE_IDENTITY_MIGRATION_SHA256=<exact-sha256> \
NEURALBASE_AUTH_REQUIRED=true \
neuralbase
```

The first connection that reaches the leader while migration is pending causes the selected registry to be initialized through Raft. Followers fail closed/redirect while that initialization is pending. Once committed, authentication reads replicated state and the migration source can be removed from the deployment.

Migration input must use the strict `users` array SCRAM shape shown by `users.json.example`. Unknown fields/comments, malformed base64, duplicate users, MD5 credentials and digest mismatches are rejected.

A fresh auth-disabled cluster with no migration file may connect and execute its first `CREATE USER`, which initializes replicated identity. A fresh cluster starting with `NEURALBASE_AUTH_REQUIRED=true` must already contain replicated identity (for example on existing PVCs) or provide an authorized migration source; otherwise authentication fails closed.

## Docker Compose

`docker compose up --build -d --wait` starts the checked-in three-process static topology. Each node owns independent RocksDB storage. Persistent table and identity mutations converge through Raft; volumes are not shared storage.

## Kubernetes and Helm

The chart deploys a static StatefulSet topology. The Raft implementation supports explicit learner/joint-consensus membership changes, but the chart does not automatically orchestrate those operations when replica count changes. Do not attach an HPA.

### Auth-required existing cluster

If PVCs already contain initialized replicated identity, authentication can simply be enabled:

```bash
helm upgrade neuralbase ./helm/neuralbase \
  --set config.authRequired=true
```

### Fresh/legacy migration through Helm

Create a Secret containing the exact strict SCRAM `users.json`, compute its digest from the same bytes, and supply both values:

```bash
sha256sum users.json
kubectl create secret generic neuralbase-users --from-file=users.json=users.json
helm upgrade --install neuralbase ./helm/neuralbase \
  --set config.authRequired=true \
  --set auth.existingSecret=neuralbase-users \
  --set auth.migrationSha256=<exact-sha256>
```

Helm mounts the migration file read-only. It is **not** copied to the data PVC. `auth.existingSecret` and `auth.migrationSha256` must be supplied together.

After migration has committed and the replicated registry is present on the cluster, remove the migration Secret/digest from subsequent Helm values while leaving `config.authRequired=true`.

## Backup and recovery operations

The external `neuralbase-backup` binary supports offline NBBK/NBEC creation, independent verification, and fresh-target recovery. Offline creation intentionally fails while another NeuralBase/RocksDB process owns the selected database. See `ops/RUNBOOK.md` for exact commands, encryption-key handling, interruption behavior, compatibility rules, and complete-cluster-loss recovery.

The leader-coordinated online backup implementation is currently an in-process `OnlineBackupCoordinator` API. It is not exposed as a standalone live-server CLI endpoint. Deployment automation must not claim otherwise.

Restored clusters begin from exactly one fresh recovery authority. Additional members must be fresh learners admitted/caught-up/promoted through the consensus membership API. Do not clone the restored PVC/directory into multiple voters.

## Membership and scaling

The consensus API supports adding a learner, catch-up, promotion, joint-consensus voter changes, removal and leadership transfer. The deployment assets do not yet reconcile these operations automatically with Kubernetes object changes. `replicaCount` therefore remains a deliberate static-topology setting and HPA is rejected.

## TLS

Build with `--features tls`. SQL TLS and Raft mTLS remain configuration-dependent; certificate lifecycle/rotation remains an operator responsibility.

## Operational readiness boundary

Current manifests demonstrate packaging for the tested replicated-table/snapshot/membership/identity engine. Phase-5 manual backup/restore/fresh-cluster DR is tested separately from deployment automation. Production readiness still requires automatic/operator-integrated membership reconciliation, stronger read-consistency modes, target-environment security review, broader fault/upgrade validation and production performance characterization; PITR and automatic DR remain unimplemented.
