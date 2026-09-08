# Deployment

NeuralBase ships deployment assets for development, integration testing, and architecture evaluation. Configured fixed-membership clusters replicate persistent table mutations and support SQL-aware snapshot recovery of an already-configured fixed member, but the manifests are **not** a production-HA database deployment.

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

Setting `NEURALBASE_NODE_ID` enables the replicated-table runtime and requires:

- durable `NEURALBASE_DB_PATH`/`DB_PATH`;
- successful RocksDB open and persisted catalog hydration;
- persisted Raft state loaded from RocksDB;
- fail-stop handling of required Raft persistence and snapshot-staging failures.

A truly fresh fixed member starts with SQL serving gated closed until Raft snapshot/log catch-up is complete.

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

Use `.env.example` as a starting point. Legacy unprefixed aliases remain in selected code paths for compatibility.

## Docker Compose

```bash
docker compose up --build -d --wait
```

The Compose file starts a fixed three-node topology with explicit Raft peer mappings and per-node persistent volumes. SQL ports are `5432`, `5433`, and `5434`.

Each node owns independent RocksDB storage. Persistent table `CREATE`, `DROP`, `INSERT`, `UPDATE`, and `DELETE` are replicated as deterministic Raft commands; volumes are not shared storage. A client connected to a follower receives a write rejection rather than local mutation.

## Read behavior

Reads are local to the connected process. There is no linearizable follower-read protocol, so follower reads may lag until committed entries are applied locally.

The fresh-member readiness gate is narrower: a member reconstructing from empty storage does not enter the PostgreSQL serving loop until snapshot/suffix catch-up is confirmed. This does not make ordinary follower reads linearizable.

## Kubernetes and Helm

The raw `k8s/` manifests and Helm chart use StatefulSet/fixed-membership assumptions. Stable pod identity maps to logical Raft identity and persistent volumes keep per-node RocksDB state.

CI validates Helm lint, default render, authentication render, TLS render, and explicit rejection of unsafe HPA configuration.

Automatic HPA remains rejected because changing StatefulSet replica count does not perform a coordinated Raft membership change. Phase 2 does not relax this rule.

## Authentication

Runtime `CREATE USER`, `ALTER USER`, and `DROP USER` remain per-node. Credential state is not included in the replicated table/snapshot guarantee. Do not infer cluster-wide authentication consistency from table replication.

## TLS

Build with:

```bash
cargo build --release --locked --features tls
```

SQL TLS uses settings documented by `src/tls.rs`. Node-to-node Raft TLS is selected with `NEURALBASE_RAFT_TLS=1` when the binary includes the `tls` feature.

## Persistence and SQL-aware recovery

Each process owns local RocksDB and a local credential registry. Replicated SQL state now has a versioned logical snapshot format integrated with Raft compaction and InstallSnapshot.

For snapshot creation, NeuralBase validates and durably stages the SQL snapshot before making prefix truncation durable. For follower installation, it stages and restores durable SQL state before publishing the Raft snapshot boundary or acknowledging success. Interrupted installation is resumable from the staged artifact after restart.

A known fixed member that loses its entire RocksDB directory can be restarted with the **same configured logical member ID** and empty local storage. It remains non-serving while it receives the leader snapshot and remaining log suffix, then becomes serving-ready after catch-up. This exact path is regression-tested, including later leadership, acknowledged writes, failure and restart.

This is not a general operator replacement controller. It does not change membership, add a new logical ID, automatically recreate volumes/pods, restore authentication state, or provide backup/disaster-recovery semantics.

## Scaling

Increasing replicas is not an operator-safe membership-change workflow. Coordinated consensus membership changes, bootstrap/promotion semantics, address reconciliation and failure rollback are still required before scaling restrictions can change.

## Operational readiness boundary

Current manifests demonstrate packaging plus a tested fixed-membership replicated mutation and SQL snapshot-recovery topology. Production readiness additionally requires at minimum:

- coordinated membership operations and automatic lifecycle reconciliation;
- an explicit replicated/strongly consistent identity design;
- defined stronger read-consistency modes and routing;
- backup/restore, PITR and disaster-recovery procedures;
- security review for the target environment;
- broader partition/storage-fault/upgrade validation;
- production performance characterization.
