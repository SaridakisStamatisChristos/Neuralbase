# Deployment

NeuralBase ships deployment assets for development, integration testing, and architecture evaluation. Configured fixed-membership clusters replicate persistent table mutations through Raft, but the manifests are **not** a production-HA database deployment.

## Single process

```bash
cargo build --release --locked
NEURALBASE_DB_PATH=/tmp/neuralbase-db cargo run --release --locked
```

Defaults:

- SQL: `0.0.0.0:5432`
- metrics: port `9090`
- Raft: disabled unless `NEURALBASE_NODE_ID` is configured
- authentication: optional/configuration-dependent

Without `NEURALBASE_NODE_ID`, persistent table DDL/DML uses the local single-node storage path.

## Clustered startup requirements

Setting `NEURALBASE_NODE_ID` enables the replicated table-mutation runtime and changes the durability prerequisites:

- durable `NEURALBASE_DB_PATH`/`DB_PATH` is required;
- RocksDB open failure prevents a usable clustered runtime rather than silently creating an authoritative in-memory cluster;
- persisted catalog hydration failure aborts clustered startup;
- persisted Raft state is loaded from RocksDB;
- required Raft persistence load/save failure fail-stops the node.

## Environment reference

| Variable | Typical/default behavior | Purpose |
|---|---|---|
| `NEURALBASE_LISTEN_ADDR` | `0.0.0.0:5432` | SQL listener |
| `NEURALBASE_DB_PATH` | unset | RocksDB directory; required when `NEURALBASE_NODE_ID` is set |
| `NEURALBASE_METRICS_PORT` | `9090` | Prometheus listener |
| `NEURALBASE_NODE_ID` | unset | Logical Raft ID; enables clustered replicated table mutations |
| `NEURALBASE_RAFT_ADDR` | `0.0.0.0:7001` | Local Raft transport bind address |
| `NEURALBASE_PEERS` | empty | Fixed peer logical-ID/address map |
| `NEURALBASE_RAFT_ELECTION_TIMEOUT_MS` | `150` | Base election timeout |
| `NEURALBASE_RAFT_TLS` | false | Select feature-gated TLS Raft transport |
| `NEURALBASE_USERS_FILE` | `users.json` | Persistent **per-node** credential registry |
| `NEURALBASE_AUTH_REQUIRED` | false unless enabled | Require client authentication |
| `NEURALBASE_MAX_CONNECTIONS` | engine default | Global connection admission limit |
| `NEURALBASE_MAX_CONNECTIONS_PER_IP` | engine default | Per-IP connection admission limit |
| `NEURALBASE_MAX_CONNECTIONS_PER_USER` | optional/unlimited unless set | Per-user admission limit |

Use `.env.example` as a starting point. Legacy unprefixed aliases remain in selected code paths for compatibility.

## Docker Compose

```bash
docker compose up --build -d --wait
```

The Compose file starts a fixed three-node topology with explicit Raft peer mappings and per-node persistent volumes.

SQL ports:

- node1: `5432`
- node2: `5433`
- node3: `5434`

Each node owns an independent RocksDB database. Persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE` are replicated as deterministic Raft commands; the volumes are not shared storage.

A client connected to a follower receives a write rejection rather than local mutation. The client must direct the mutation to the elected leader.

## Read behavior

Reads are local to the connected process. This phase does not provide a linearizable follower-read protocol, so follower reads may lag until committed entries are learned/applied locally.

Do not place a generic load balancer in front of all SQL endpoints and assume arbitrary read-after-write or write routing semantics. An operator/client layer must understand leader-directed writes and the selected read-consistency policy.

## Kubernetes and Helm

The raw `k8s/` manifests and Helm chart use StatefulSet/fixed-membership assumptions. Stable pod identity maps naturally to logical Raft identity and persistent volumes keep each node's RocksDB state.

The chart validates:

```bash
helm lint helm/neuralbase
helm template neuralbase helm/neuralbase
```

Authentication and TLS render paths are also exercised in CI.

Automatic HPA enablement is rejected because changing `replicaCount` or StatefulSet size does not perform a coordinated Raft membership change. The PodDisruptionBudget is an availability aid that preserves a configured majority; it is not a membership controller.

## Authentication

Seed credentials may be supplied by the deployment Secret mechanism when authentication is enabled. Runtime `CREATE USER`, `ALTER USER`, and `DROP USER` write the live registry to `NEURALBASE_USERS_FILE`.

Authentication mutations remain per-node. A user created on one node does not automatically become cluster-wide simply because table data is replicated.

Do not place the writable live user-registry path directly on a read-only Secret mount.

## TLS

Build with:

```bash
cargo build --release --locked --features tls
```

SQL TLS uses the certificate/key settings documented by `src/tls.rs`. Node-to-node Raft TLS is selected with `NEURALBASE_RAFT_TLS=1` when the binary includes the `tls` feature.

See [THREAT_MODEL.md](THREAT_MODEL.md) before treating TLS/auth defaults as a production security profile.

## Persistence and recovery boundary

Each process owns local RocksDB and a local credential registry. Persistent table mutations and Raft stable state survive the tested process kill/re-election/full-cluster restart path.

However, replicated-SQL mode intentionally rejects legacy opaque Raft snapshot/compaction state. NeuralBase does not yet have a SQL-aware snapshot/bootstrap format for replacement nodes or safe log truncation.

Backup/restore, disaster recovery, and replacement-node procedures are therefore still release boundaries.

## Scaling

Increasing replicas is not an operator-safe membership-change workflow.

Before dynamic scaling is supported, NeuralBase needs coordinated consensus membership changes, state catch-up/bootstrap, address discovery/reconciliation, and explicit rollback/recovery behavior.

## Operational readiness boundary

Current manifests demonstrate packaging plus a tested fixed-membership replicated table-mutation topology. Production readiness additionally requires at minimum:

- SQL-aware snapshot/bootstrap and replacement-node recovery;
- coordinated membership operations;
- an explicit replicated/strongly consistent identity design;
- defined read-consistency modes and appropriate routing;
- backup/restore and disaster-recovery procedures;
- security review for the target environment;
- broader partition/storage-fault/upgrade validation.
