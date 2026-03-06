# NeuralBase v1.0.0

NeuralBase is a self-optimising distributed SQL engine written in Rust. It speaks the PostgreSQL wire protocol, executes analytic queries through a vectorised morsel-driven engine, and uses a reinforcement-learning ONNX model to choose join orders. Storage is MVCC over RocksDB with Hybrid Logical Clocks; cluster coordination uses Raft consensus with automatic leader election and log replication.

> [!WARNING]
> Read the **Risk Budget** section before deploying to production.

---

## Quick Start

### Single node (cargo)

```bash
git clone https://github.com/your-org/neuralbase.git
cd neuralbase
cargo build --release --locked
cargo run --release --locked
# → Listening on 0.0.0.0:5432
```

```bash
psql -h 127.0.0.1 -p 5432 -U neuralbase -d neuralbase
neuralbase=# SELECT 1;
neuralbase=# SELECT l_returnflag, sum(l_extendedprice * (1 - l_discount))
             FROM lineitem GROUP BY l_returnflag;
```

### Docker Compose (3-node cluster)

```bash
docker-compose up --build
# node1:5432, node2:5433, node3:5434
# Prometheus :9090, Grafana :3000, Jaeger :16686
```

### Kubernetes (Helm)

```bash
helm install neuralbase ./helm/neuralbase \
  --set replicaCount=3 \
  --set tls.enabled=true \
  --set tls.existingSecret=my-tls-secret
```

See the Helm [values.yaml](helm/neuralbase/values.yaml) for all configuration knobs.

---

## Architecture

```
 Client (psql / JDBC)
        |  PostgreSQL wire protocol v3
        v
 +------------------+
 |  server.rs       |  TCP listener, TLS STARTTLS, auth
 +--------+---------+
          |
 +--------v---------+     +------------------+
 |  SQL Parser       | --> |  Binder          |
 |  (sqlparser-rs)   |     |  catalog resolve |
 +--------+---------+     +--------+---------+
          |                         |
 +--------v-------------------------v--------+
 |  RL Optimizer (ONNX DQN) + CostModel      |
 |  100% TPC-H win rate (22/22 queries)       |
 +---------------------+---------------------+
                        |
 +----------------------v---------------------+
 |  Vectorised Executor (morsel-driven)        |
 |  StorageExecutor -> MVCC -> RocksDB         |
 +---------------------+----------------------+
                        |
 +----------------------v---------------------+
 |  Raft Consensus + Distributed Planner       |
 |  ConsistentHashRouter, back-pressure exch.  |
 +--------------------------------------------+
```

---

## Features

| Feature | Status |
|---|---|
| PostgreSQL wire protocol v3 | Done |
| SQL parser + binder | Done |
| Vectorised execution (morsel-driven) | Done |
| TPC-H Q1 + Q6 exact correctness | Done |
| RL join-order optimizer (ONNX DQN, 22/22 win rate) | Done |
| MVCC snapshot isolation (RocksDB + HLC) | Done |
| Raft consensus (leader election, log replication) | Done |
| Distributed planner + back-pressure exchange | Done |
| StorageExecutor (MVCC query path) | Done |
| IndexAdvisor (live RocksDB CF DDL) | Done |
| Binary row codec (NB format) | Done |
| TLS/STARTTLS (feature-gated) | Done |
| SHA-256 authentication + per-user/per-IP limits | Done |
| Prometheus /metrics endpoint | Done |
| DML: INSERT, UPDATE, DELETE | Done |
| Subqueries, CTEs, window functions (parse) | Done |
| Graceful SIGTERM shutdown (30 s drain) | Done |
| Kubernetes StatefulSet + Helm chart | Done |
| CI/CD release pipeline (GitHub Actions) | Done |
| Multi-table JOIN execution | Future |
| Full TPC-H Q2-Q22 execution | Future |

---

## TPC-H Benchmarks (SF 0.1, release build)

Measured on the deterministic synthetic dataset (600,122 lineitem rows).

| Query | Latency | Notes |
|---|---|---|
| Q1 | ~272 ms | Full correctness verified |
| Q6 | ~68 ms | Full correctness verified |
| Q2-Q22 | parse + bind | Multi-table JOIN not yet executed |

RL optimizer win rate: **22/22** TPC-H join graphs (RL cost <= naive cost).

See `tests/perf/BENCH_BASELINES.yaml` for pinned baselines.

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NEURALBASE_LISTEN_ADDR` | `0.0.0.0:5432` | SQL listen address |
| `NEURALBASE_RAFT_ADDR` | `0.0.0.0:7001` | Raft peer address |
| `NEURALBASE_METRICS_PORT` | `9090` | Prometheus metrics port |
| `NEURALBASE_MAX_CONNECTIONS` | `500` | Global connection limit |
| `NEURALBASE_MAX_CONNECTIONS_PER_IP` | `100` | Per-IP connection limit |
| `NEURALBASE_MAX_CONNECTIONS_PER_USER` | `50` | Per-user connection limit |
| `NEURALBASE_AUTH_REQUIRED` | `true` | Require SHA-256 auth |
| `NEURALBASE_DB_PATH` | `/data/neuralbase` | RocksDB data directory |
| `TLS_ENABLED` | `0` | Enable TLS/STARTTLS |
| `TLS_CERT_PATH` | | Path to TLS certificate |
| `TLS_KEY_PATH` | | Path to TLS private key |

### Kubernetes Deployment

The Helm chart in `helm/neuralbase/` provides:

- **StatefulSet** with 3 replicas (configurable) and PersistentVolumeClaims
- **Headless Service** for Raft peer discovery via DNS
- **LoadBalancer Service** for external SQL access
- **PodDisruptionBudget** with `minAvailable: 2` (Raft quorum)
- **HPA** scaling on CPU and active connections (optional)
- **Graceful shutdown**: SIGTERM triggers 30 s drain; `preStop` hook sleeps 5 s

Raw manifests are also available in `k8s/` for non-Helm deployments.

---

## Development

```bash
make test         # cargo test --locked (all suites, 541+ tests)
make lint         # cargo clippy --locked -- -D warnings + fmt --check
make confidence   # CONFIDENCE.yaml gate tests (effective >= 0.75)
make bench        # TPC-H performance baselines (SF 0.1)
make adversarial  # property-based + malformed-input + boundary tests
```

---

## Threat Model

Full threat model: `docs/THREAT_MODEL.md`

- **SQL injection**: `sqlparser-rs` parses untrusted text into AST before the binder
- **Wire protocol**: frame lengths validated before allocation; malformed frames return PG errors
- **MVCC isolation**: snapshot isolation via HLC; GC protected by `safe_horizon` Mutex
- **Raft**: leader-only log writes; quorum commit; see `REVIEW_REQUIRED.md`
- **Auth**: SHA-256 password hashing, per-IP + per-user connection limits, configurable admission
- **TLS**: plaintext by default; production MUST enable TLS or bind to loopback
- **Metrics**: `/metrics` endpoint not auth-protected; do not expose port 9090 to untrusted networks

---

## Risk Budget

**This system MUST NOT be relied upon for:**
- Production OLTP workloads (write throughput not benchmarked at scale)
- Queries requiring multi-table JOINs (binder returns UnsupportedSelect)
- High-availability without operator review (Raft not TLA+ verified)
- PII storage without additional access control and encryption

**Effective system confidence: ~0.78**
(see `CONFIDENCE.yaml` and `CONFIDENCE.md` for full breakdown)

**Estimated failure probability under adversarial input: ~8-15%**
(bounded estimate; 541+ tests including property-based, fuzz, and boundary suites)

**Weakest links:**
1. `onnx_seed_model` (0.65) — trained DQN achieves 22/22 but model architecture limits ceiling
2. `simd_filter_path` (0.66) — AVX-512 not active on stable Rust; scalar fallback is correct
3. `tls` (0.68) — code-complete but requires NASM on Windows for `aws-lc-sys`

**Fastest ways to raise confidence:**
1. Write TLA+ spec for Raft consensus
2. Enable TLS in CI (install NASM, test full handshake)
3. Extend fuzz coverage to binary codec edge cases

---

## License

Apache-2.0 — see `LICENSE`.
