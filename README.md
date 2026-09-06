# NeuralBase

NeuralBase is an experimental SQL engine written in Rust. It speaks the PostgreSQL wire protocol, executes analytical SQL through vectorized and row-oriented execution paths, stores persistent tables in MVCC/RocksDB, and includes a Raft consensus subsystem with real TCP/TLS multi-process transport.

The repository is intentionally explicit about one major boundary: **the Raft log is not yet the replicated SQL storage state machine**. SQL DDL/DML executed against a node currently mutates that node's local RocksDB state. The three-node deployment therefore exercises real Raft election/log transport and independent SQL nodes; it must not be presented as a replicated HA database until SQL writes are applied through Raft on every member.

> [!WARNING]
> Read **Distributed semantics** and the **Risk budget** before using the project outside development or research.

## Quick start

### Single node

```bash
git clone https://github.com/SaridakisStamatisChristos/Neuralbase.git
cd Neuralbase
cargo build --release --locked
cargo run --release --locked
```

The SQL listener defaults to `0.0.0.0:5432`.

```bash
psql -h 127.0.0.1 -p 5432 -U neuralbase -d neuralbase
```

Persistent SQL DDL/DML requires a RocksDB path:

```bash
NEURALBASE_DB_PATH=/tmp/neuralbase-db cargo run --release --locked
```

### Three-process development cluster

```bash
docker compose up --build -d --wait
```

SQL endpoints are exposed on `5432`, `5433`, and `5434`. Each node has its own persistent data volume. Raft peers communicate over real TCP using explicit logical-ID-to-address mappings such as `node2=node2:7001`.

This topology is useful for Raft transport/election testing. **Writes sent to node1 are not automatically mirrored into node2/node3 RocksDB.**

## Architecture

```text
PostgreSQL client
      |
      v
+---------------------+
| wire protocol/auth  |
+----------+----------+
           |
           v
+---------------------+
| parser + binder     |
+----------+----------+
           |
      +----+--------------------+
      |                         |
      v                         v
vectorized path          general SQL executor
      |                         |
      +------------+------------+
                   |
                   v
          StorageExecutor
          MVCC + HLC + RocksDB

Separate distributed subsystem:
  RaftNode <-> TCP/TLS Raft transport <-> RaftNode
  distributed planner / back-pressure primitives
```

## Capability status

| Capability | Status |
|---|---|
| PostgreSQL wire protocol v3 | Implemented |
| SQL parser + binder | Implemented |
| Vectorized scan/filter/project/aggregate path | Implemented |
| General multi-table JOIN/subquery execution path | Implemented, bounded by intermediate-result budgets |
| TPC-H Q1-Q22 deterministic reference tests | Row-for-row PostgreSQL 16 comparison tests present at small deterministic scale |
| Q1/Q6 larger deterministic correctness checks | Implemented |
| RL join-order optimizer (ONNX DQN) | Implemented; benchmark claims are repository-specific, not universal optimizer superiority |
| MVCC snapshot isolation + HLC | Implemented for local RocksDB storage |
| INSERT / UPDATE / DELETE | Implemented for local RocksDB storage |
| CREATE/DROP TABLE catalog persistence | Implemented for local RocksDB storage |
| CREATE/ALTER/DROP USER persistence | Implemented to `NEURALBASE_USERS_FILE` |
| Raft leader election/log replication | Implemented in the Raft subsystem |
| Multi-process Raft TCP transport | Implemented |
| Optional Raft mTLS transport | Feature-gated |
| SQL writes replicated through Raft | **Not implemented** |
| Automatic SQL failover / replicated HA | **Not implemented** |
| Prometheus metrics | Implemented |
| Kubernetes StatefulSet + Helm chart | Development/research deployment manifests |

### TPC-H evidence

`tests/tpch_correctness.rs` contains PostgreSQL-16 row-for-row reference comparisons for all 22 TPC-H query strings on a deterministic small dataset. Q1 and Q6 also have additional deterministic correctness coverage. The suite deliberately uses bounded data sizes; it is correctness evidence, not a claim of production-scale TPC-H performance.

Performance numbers should only be quoted from the reproducible benchmark files under `tests/perf/` and with their workload/hardware context intact.

## Distributed semantics

Setting `NEURALBASE_NODE_ID` enables the Raft node. Production-style peer configuration uses explicit logical IDs and connectable addresses:

```bash
NEURALBASE_NODE_ID=node1
NEURALBASE_RAFT_ADDR=0.0.0.0:7001
NEURALBASE_PEERS='node2=node2:7001,node3=node3:7001'
```

Legacy short names (`NODE_ID`, `RAFT_ADDR`, `PEERS`) remain accepted for compatibility. The documented `NEURALBASE_*` names take precedence.

Accepted peer forms:

```text
node2=node2.internal:7001,node3=node3.internal:7001   # preferred
node2:7001,node3:7001                                 # ID inferred from host
node2,node3                                           # default Raft port inferred
```

Malformed/duplicate peer IDs fail startup instead of silently creating an isolated logical cluster. In Kubernetes, StatefulSet pod names are logical Raft IDs and headless-service DNS names are transport addresses.

`NEURALBASE_RAFT_TLS=1` requires a binary built with `--features tls` and the node TLS certificate configuration described in `src/tls.rs` / `docs/THREAT_MODEL.md`.

### What Raft currently guarantees

The Raft subsystem has election, AppendEntries replication, snapshot/membership machinery, transport tests, and a committed-entry apply channel. It does **not** currently own the SQL DDL/DML commit path. Consequently, Raft quorum does not make a SQL mutation durable on a majority of database replicas.

Do not infer replicated database semantics from the presence of the Raft module alone.

## Configuration

Canonical environment variables:

| Variable | Default | Description |
|---|---|---|
| `NEURALBASE_LISTEN_ADDR` | `0.0.0.0:5432` | SQL listen address |
| `NEURALBASE_DB_PATH` | unset | RocksDB data directory; unset = in-memory query dataset / no persistent DML executor |
| `NEURALBASE_METRICS_PORT` | `9090` | Prometheus listener |
| `NEURALBASE_NODE_ID` | unset | Enables a Raft node when set |
| `NEURALBASE_RAFT_ADDR` | `0.0.0.0:7001` | Local Raft bind address |
| `NEURALBASE_PEERS` | empty | Comma-separated Raft peer mapping |
| `NEURALBASE_RAFT_ELECTION_TIMEOUT_MS` | `150` | Base election timeout |
| `NEURALBASE_RAFT_TLS` | `false` | Use feature-gated mTLS Raft transport |
| `NEURALBASE_USERS_FILE` | `users.json` | Persistent authentication registry |
| `NEURALBASE_AUTH_REQUIRED` | `false` unless configured | Require authentication |
| `NEURALBASE_MAX_CONNECTIONS` | project default | Global connection admission limit |
| `NEURALBASE_MAX_CONNECTIONS_PER_IP` | project default | Per-IP connection limit |
| `NEURALBASE_MAX_CONNECTIONS_PER_USER` | unlimited unless set | Per-user connection limit |

Legacy unprefixed listen/storage/Raft/metrics variables are accepted where documented in code, but new deployments should use `NEURALBASE_*`.

## Authentication durability

`CREATE USER`, `ALTER USER`, and `DROP USER` persist the credential registry to `NEURALBASE_USERS_FILE`. Kubernetes/Helm use an optional Secret only as an initial seed and keep the live registry on the writable data PVC; mounting the live registry read-only would make SQL user DDL fail.

Authentication state is still **per node** because SQL/admin mutations are not yet replicated through Raft.

## Kubernetes / Helm

The Helm chart provides a fixed-size StatefulSet, headless Raft discovery, persistent volumes, a PodDisruptionBudget, and optional TLS/auth seed Secrets. Automatic horizontal scaling is deliberately disabled: changing StatefulSet replica count behind a fixed Raft membership is unsafe without coordinated membership changes.

The raw `k8s/` example likewise uses a fixed three-member topology; there is intentionally no HPA manifest.

## Development and verification

```bash
make test
make lint
make confidence
make adversarial
make tpch-correctness
make bench
```

CI installs the native RocksDB/libclang/NASM build dependencies, runs the Rust correctness/lint/confidence/adversarial gates, and lints/renders the Helm chart. `tests/raft_tcp_transport.rs` specifically verifies that distinct logical Raft IDs route over explicit loopback TCP addresses in both directions.

## Risk budget

Do **not** rely on NeuralBase for:

- production OLTP workloads;
- replicated SQL durability or automatic database failover;
- unsupervised Raft membership changes;
- PII without additional authorization/encryption controls;
- performance claims outside the exact checked-in benchmark methodology;
- production security without reviewing `docs/THREAT_MODEL.md` and enabling/configuring TLS/auth appropriately.

Known high-value follow-up work is intentionally narrower than “add features”: wire SQL mutations into a deterministic replicated state machine, make Raft persistence fail-closed on stable-storage errors, prove restart/failover behavior across separate processes, and keep benchmark/documentation claims synchronized with executable evidence.

## License

Apache-2.0 — see `LICENSE`.
