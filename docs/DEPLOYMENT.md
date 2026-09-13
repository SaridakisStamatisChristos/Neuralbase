# Deployment

NeuralBase ships deployment assets for development, integration testing and architecture evaluation. They are **not** a production-HA database deployment.

## Single process

```bash
cargo build --release --locked --bins
NEURALBASE_LISTEN_ADDR=127.0.0.1:5432 \
NEURALBASE_DB_PATH=./data/neuralbase \
cargo run --release --locked --bin neuralbase
```

Without `NEURALBASE_NODE_ID`, table and user DDL use the standalone local-storage/local-registry paths.

The binary does not load `.env` files automatically. Standalone startup without a database path, or with a RocksDB open failure, uses in-memory/demo mode; persistent DML requires opened storage. See [build prerequisites](../CONTRIBUTING.md#development-prerequisites) and the [complete configuration reference](CONFIGURATION.md).

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

Defaults, legacy aliases, admission limits, TLS activation/precedence and fixed execution limits are specified in [CONFIGURATION.md](CONFIGURATION.md).

## Cluster identity bootstrap and migration

Clustered identity lives in replicated RocksDB state. Do not maintain separate live `users.json` files per pod.

For an upgrade with an existing legacy registry, select one strict SCRAM file as authoritative:

```bash
NEURALBASE_USERS_FILE=/secure/migration/users.json \
NEURALBASE_IDENTITY_MIGRATION_SHA256="$(sha256sum /secure/migration/users.json | cut -d ' ' -f1)" \
NEURALBASE_AUTH_REQUIRED=true \
./target/release/neuralbase
```

Use the actual file path and retain the member's existing exported `NEURALBASE_NODE_ID`, `NEURALBASE_DB_PATH`, Raft address and peer configuration when running this example; it does not define a new topology.

The first connection that reaches the leader while migration is pending causes the selected registry to be initialized through Raft. Followers reject with a leader hint while initialization is pending; they do not forward the connection. Once committed and applied on the serving members, authentication reads replicated state and the migration source can be removed from the deployment.

Migration input must use the strict `users` array SCRAM shape shown by `users.json.example`. Unknown fields/comments, malformed base64, duplicate users, MD5 credentials and digest mismatches are rejected.

A fresh auth-disabled cluster with no migration file may connect and execute its first `CREATE USER`, which initializes replicated identity. A fresh cluster starting with `NEURALBASE_AUTH_REQUIRED=true` must already contain replicated identity (for example on existing PVCs) or provide an authorized migration source; otherwise authentication fails closed.

## Docker Compose

`docker compose up --build -d --wait` starts the checked-in three-process static topology. Each node owns independent RocksDB storage. Persistent table and identity mutations converge through Raft; volumes are not shared storage.

| Service | Host ports |
|---|---|
| SQL nodes 1 / 2 / 3 | `5432` / `5433` / `5434` |
| Raft nodes 1 / 2 / 3 | `7001` / `7002` / `7003` |
| Node metrics | `9090` / `9091` / `9092` |
| Prometheus / Grafana | `9093` / `3000` |
| Jaeger UI / OTLP receivers | `16686` / `4317`, `4318` |

Port `8001` (host `8001`–`8003`) is reserved by existing assets for exchange experiments; `main.rs` does not start an exchange listener. Jaeger is an auxiliary development service; the server has no OTLP export pipeline. Grafana uses the development password `neuralbase`. Compose publishes ports on the host with authentication disabled; restrict access to the development environment. See [observability](../observability/README.md).

The Docker runtime image contains `/app/neuralbase` and `/app/neuralbase-operator` for guarded membership administration/readiness. It does not package `neuralbase-backup` or the optimizer model; build the backup tool separately for offline access to a stopped member's storage. `docker compose down` retains named volumes; adding `-v` deletes them.

## Kubernetes and Helm

The chart deploys a static StatefulSet topology. The Raft implementation supports explicit learner/joint-consensus membership changes, but the chart does not automatically orchestrate those operations when replica count changes. Do not attach an HPA.

The SQL Service selects all matching pods and is not leader-aware. Use an application-controlled connection to the current leader for writes and strong reads. Docker and Kubernetes probes only open the SQL TCP port; the server binds that listener before catch-up completes, so probe success does not establish SQL serving readiness, quorum availability or leadership.

Set `image.repository` / `image.tag` to an image you have built and made available to the cluster. The checked-in `0.1.0` tag is a packaging default, not evidence that it contains current `main`.

### Auth-required existing cluster

If PVCs already contain initialized replicated identity, authentication can simply be enabled:

```bash
helm upgrade neuralbase ./helm/neuralbase \
  --set config.authRequired=true
```

### Fresh/legacy migration through Helm

Create a Secret containing the exact strict SCRAM `users.json`, compute its digest from the same bytes, and supply both values:

```bash
migration_digest="$(sha256sum users.json | cut -d ' ' -f1)"
kubectl create secret generic neuralbase-users --from-file=users.json=users.json
helm upgrade --install neuralbase ./helm/neuralbase \
  --set config.authRequired=true \
  --set auth.existingSecret=neuralbase-users \
  --set-string auth.migrationSha256="$migration_digest"
```

Helm mounts the migration file read-only. It is **not** copied to the data PVC. `auth.existingSecret` and `auth.migrationSha256` must be supplied together.

After migration has committed and the replicated registry is present on the cluster, remove the migration Secret/digest from subsequent Helm values while leaving `config.authRequired=true`.

### Raw Kubernetes examples

`k8s/deployment.yaml` defines a three-node **StatefulSet**, despite its filename. The raw example does not mount a migration Secret or copy a credential registry onto each PVC. It retains `/data/neuralbase/users.json` solely as a legacy migration input: an existing file blocks implicit empty-user bootstrap until explicitly authorized, while fresh storage has no such file and can create its first user on the leader with auth disabled. The example Secrets are not usable credentials. Prefer Helm's paired Secret/digest configuration for migration; merely creating `neuralbase-users` does not mount or authorize it in the raw StatefulSet.

Existing PVCs from the old per-node-file example are not automatically migrated. Select and authorize the intended legacy SCRAM file explicitly; do not assume discarded mounts initialize replicated identity.

## Backup and recovery operations

The external `neuralbase-backup` binary supports offline NBBK/NBEC creation, independent verification, and fresh-target recovery. Its source must contain durable Raft state and committed membership; a standalone-only database is rejected. Offline creation intentionally fails while another NeuralBase/RocksDB process owns the selected database. See the [runbook](../ops/RUNBOOK.md) for exact commands, encryption-key handling, interruption behavior, compatibility rules, and complete-cluster-loss recovery.

The leader-coordinated online backup implementation is currently an in-process `OnlineBackupCoordinator` API. It is not exposed as a standalone live-server CLI endpoint. Deployment automation must not claim otherwise.

Restored clusters begin from exactly one fresh recovery authority. Additional members must be fresh learners admitted/caught-up/promoted through the consensus membership API. Do not clone the restored PVC/directory into multiple voters.

## Membership and scaling

The consensus API supports adding a learner, catch-up, promotion, joint-consensus voter changes, removal and leadership transfer. The static chart does not reconcile these operations when `replicaCount` changes, so that value remains a bootstrap topology setting and HPA is rejected.

The separate [Phase-7 managed profile](PHASE7_OPERATOR.md#managed-kubernetes-profile) connects explicit desired topology to one StatefulSet/PVC per incarnation. It checks committed membership, immutable object identity and Kubernetes resource versions before executing actions. Its CI gate includes a real kind lifecycle; Phase-7 validation remains in progress. Do not point this controller at an existing Helm deployment or change its replicas directly.

## TLS

Build with `--features tls`. SQL TLS and Raft mTLS remain configuration-dependent; certificate lifecycle/rotation remains an operator responsibility. Helm's TLS values configure SQL TLS only, when configured, the SQL listener rejects plaintext startup. See [TLS configuration](CONFIGURATION.md#tls) for activation, certificate-name checks, CA requirements and precedence.

## Operational readiness boundary

Current manifests demonstrate packaging for the tested replicated-table/snapshot/membership/identity engine. Phase-5 manual backup/restore/fresh-cluster DR is tested separately from deployment automation. Phase-6 strong read modes are implemented on the leader path. Production readiness still requires validated operation of the managed membership profile, target-environment security review, broader fault/upgrade validation and production performance characterization; PITR and automatic DR remain unimplemented.
