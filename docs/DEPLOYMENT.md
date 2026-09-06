# Deployment

NeuralBase ships deployment assets for development, integration testing, and architecture evaluation. They are not yet a production-HA database deployment because SQL storage is not replicated through Raft.

## Single process

Build and run:

```bash
cargo build --release --locked
NEURALBASE_DB_PATH=/tmp/neuralbase-db cargo run --release --locked
```

Defaults:

- SQL: `0.0.0.0:5432`
- metrics: port `9090`
- Raft: disabled unless `NEURALBASE_NODE_ID` is configured
- authentication: optional/configuration-dependent

## Environment reference

| Variable | Typical/default behavior | Purpose |
|---|---|---|
| `NEURALBASE_LISTEN_ADDR` | `0.0.0.0:5432` | SQL listener |
| `NEURALBASE_DB_PATH` | unset | RocksDB directory for persistent local SQL state |
| `NEURALBASE_METRICS_PORT` | `9090` | Prometheus listener |
| `NEURALBASE_NODE_ID` | unset | Logical Raft ID; setting it enables Raft startup |
| `NEURALBASE_RAFT_ADDR` | `0.0.0.0:7001` | Local Raft transport bind address |
| `NEURALBASE_PEERS` | empty | Peer logical-ID/address map |
| `NEURALBASE_RAFT_ELECTION_TIMEOUT_MS` | `150` | Base election timeout |
| `NEURALBASE_RAFT_TLS` | false | Select feature-gated TLS Raft transport |
| `NEURALBASE_USERS_FILE` | `users.json` | Persistent per-node credential registry |
| `NEURALBASE_AUTH_REQUIRED` | false unless enabled | Require client authentication |
| `NEURALBASE_MAX_CONNECTIONS` | engine default | Global connection admission limit |
| `NEURALBASE_MAX_CONNECTIONS_PER_IP` | engine default | Per-IP connection admission limit |
| `NEURALBASE_MAX_CONNECTIONS_PER_USER` | optional/unlimited unless set | Per-user admission limit |

Use `.env.example` as a safe starting template. Legacy unprefixed aliases remain in selected code paths for compatibility, but new configurations should use canonical names.

## Docker Compose

```bash
docker compose up --build -d --wait
```

The Compose file starts a fixed three-node development topology with explicit Raft peer mappings and per-node persistent volumes.

SQL ports:

- node1: `5432`
- node2: `5433`
- node3: `5434`

This topology validates real multi-process transport/election behavior. It does **not** mirror SQL rows between volumes.

## Kubernetes

The raw `k8s/` manifests use a StatefulSet and headless service. StatefulSet pod names provide stable logical node identities, while headless-service DNS provides connectable Raft addresses.

The live user registry resides on writable persistent storage. An optional Secret is treated as an initial seed, not mounted over the live registry read-only.

The raw topology intentionally does not include an HPA manifest.

## Helm

The chart lives at `helm/neuralbase/`.

Typical validation:

```bash
helm lint helm/neuralbase
helm template neuralbase helm/neuralbase
```

Authentication-enabled render:

```bash
helm template neuralbase helm/neuralbase \
  --set config.authRequired=true \
  --set auth.existingSecret=neuralbase-users
```

TLS-enabled render:

```bash
helm template neuralbase helm/neuralbase \
  --set tls.enabled=true \
  --set tls.existingSecret=neuralbase-tls
```

The chart rejects automatic HPA enablement because fixed Raft membership cannot safely infer membership changes from replica scaling.

The PodDisruptionBudget derives a majority requirement from `replicaCount`.

## Authentication

When authentication is enabled, seed credentials may be supplied through the deployment Secret mechanism. Runtime `CREATE USER`, `ALTER USER`, and `DROP USER` write the live registry to `NEURALBASE_USERS_FILE`.

Because auth changes are not yet Raft-replicated, operators must not assume a user created on one node automatically exists on another.

## TLS

Build with:

```bash
cargo build --release --locked --features tls
```

SQL TLS configuration supports the environment names documented by `src/tls.rs`, including `TLS_ENABLED`, certificate/key path aliases, and `NEURALBASE_TLS_*` certificate settings. Node-to-node Raft TLS is selected with `NEURALBASE_RAFT_TLS=1` when the binary includes the `tls` feature.

See [THREAT_MODEL.md](THREAT_MODEL.md) before treating TLS/auth defaults as a production security profile.

## Persistence

Each process owns its local RocksDB path and local credential registry. For containers, both must live on writable persistent storage if restart durability is required.

Do not place a writable live user-registry path directly on a read-only Secret mount.

## Scaling

Increasing replicas is not an operator-safe membership-change workflow today.

Before dynamic scaling is supported, NeuralBase needs coordinated consensus membership changes, address discovery/reconciliation, state catch-up, and explicit rollback/recovery behavior.

## Operational readiness boundary

The deployment manifests demonstrate packaging and topology correctness. Production readiness additionally requires at minimum:

- replicated SQL state-machine semantics;
- fail-closed consensus persistence;
- process crash/restart recovery tests;
- proven leader failover with acknowledged SQL state;
- coordinated membership operations;
- security review for the target environment;
- backup/restore and disaster-recovery procedures.
