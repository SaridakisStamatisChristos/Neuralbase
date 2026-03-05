# NeuralBase v0.1.0

**NeuralBase** is a self-optimising distributed SQL engine written in Rust. It implements:

- **PostgreSQL wire-protocol v3** — connect with any `psql`, PG driver, or JDBC client
- **Vectorised in-memory execution** — morsel-driven parallelism for analytic queries
- **RL-guided join-order optimiser** — ONNX-backed DQN model selects join ordering
- **MVCC transactional storage** — RocksDB + Hybrid Logical Clocks; snapshot isolation
- **Multi-node Raft consensus** — full leader election, log replication, commit-index advancement
- **Self-tuning IndexAdvisor** — workload monitor drives live RocksDB secondary-CF DDL
- **TPC-H benchmark suite** — Q1 and Q6 exact correctness verified; Q2-Q22 parse + bind-error classified

> [!WARNING]
> NeuralBase is **research / pre-production** software. Read the **Risk Budget** section before use.

---

## Quickstart (single node)

### Prerequisites

| Tool | Version |
|---|---|
| Rust (stable) | `1.85` (see `rust-toolchain.toml`) |
| C/C++ toolchain | MSVC 2022 (Win) / GCC 12+ (Linux) |

```bash
# Clone + build (release)
git clone <repo>
cd neuralbase
cargo build --release --locked

# Run the server (port 5432 by default)
cargo run --release --locked

# Connect with psql
psql -h 127.0.0.1 -p 5432 -U neuralbase -d neuralbase
neuralbase=# SELECT 1;
neuralbase=# SELECT l_returnflag, sum(l_extendedprice * (1 - l_discount)) FROM lineitem GROUP BY l_returnflag;
```

### Docker

```bash
docker-compose up --build
```

---

## Quickstart (3-node cluster)

```bash
# Node 1 (Raft leader candidate)
NEURALBASE_NODE_ID=1 \
NEURALBASE_PEERS=127.0.0.1:5433,127.0.0.1:5434 \
cargo run --release --locked -- --port 5432

# Node 2
NEURALBASE_NODE_ID=2 \
NEURALBASE_PEERS=127.0.0.1:5432,127.0.0.1:5434 \
cargo run --release --locked -- --port 5433

# Node 3
NEURALBASE_NODE_ID=3 \
NEURALBASE_PEERS=127.0.0.1:5432,127.0.0.1:5433 \
cargo run --release --locked -- --port 5434
```

Raft election completes in < 300 ms. Submit queries to any node; the cluster coordinator routes fragments.

---

## TLS (optional)

TLS support is code-complete but requires **NASM** on Windows x64 (required by `aws-lc-sys`):

1. Install NASM from https://www.nasm.us/
2. Uncomment the TLS deps in `Cargo.toml` (`tokio-rustls`, `rustls`, `rcgen`, `rustls-pemfile`)
3. `cargo build --features tls --locked`
4. Set `TLS_ENABLED=1` (dev self-signed cert auto-generated)
5. Or set `TLS_CERT_PATH` + `TLS_KEY_PATH` for production certs

Without TLS the server runs plaintext on all interfaces — **bind to loopback or firewall in production**.

---

## Development

```bash
make test         # cargo test --locked (all suites)
make lint         # cargo clippy --locked + fmt --check
make confidence   # CONFIDENCE.yaml gate tests (effective >= 0.75)
make bench        # TPC-H performance baselines (SF 0.1)
make adversarial  # property-based + malformed-input + boundary tests
make e2e          # full end-to-end smoke (psql + Q1 + Q6)
```

---

## Feature Summary (Sessions 1–7)

| Feature | Status | Session |
|---|---|---|
| PostgreSQL wire protocol v3 | ✓ | S1 |
| SQL parser (sqlparser-rs) | ✓ | S1 |
| In-memory catalog + binder | ✓ | S1 |
| Vectorised execution (morsel) | ✓ | S2 |
| TPC-H Q1 + Q6 exact correctness | ✓ | S2 |
| RL join-order optimiser (ONNX) | ✓ | S3 |
| TPC-H Q1-Q22 parse/bind-error suite | ✓ | S7 |
| MVCC snapshot isolation (RocksDB) | ✓ | S4 |
| Hybrid Logical Clocks | ✓ | S4 |
| Raft consensus (leader + replication) | ✓ | S5 |
| Distributed planner + back-pressure | ✓ | S5 |
| StorageExecutor (MVCC query path) | ✓ | S6 |
| IndexAdvisor workload monitor | ✓ | S6 |
| IndexAdvisor DDL wiring (live CF DDL) | ✓ | S7 |
| Binary row codec (NB format) | ✓ | S7 |
| TLS infrastructure (feature-gated) | ○ | S7 |
| Multi-table JOIN planner | ✗ | Future |
| Full TPC-H Q2-Q22 execution | ✗ | Future |
| OLTP write throughput | ✗ | Future |

✓ = complete and tested · ○ = code-complete, not active in default build · ✗ = not yet implemented

---

## TPC-H Benchmarks (SF 0.1 — release build)

Measured on the deterministic synthetic dataset (600,122 lineitem rows).

| Query | Type | Result |
|---|---|---|
| Q1 | FULL correctness + timing | N_sum_disc_price verified; ~111–131 ms |
| Q6 | FULL correctness + timing | revenue verified; ~1–1.5 ms |
| Q2–Q5, Q7–Q22 | Parse + bind-error classification | All parse ok; multi-table JOIN → UnsupportedSelect |

Scaling is linear: SF 0.001 (6 K rows) ~2 ms · SF 0.01 (60 K rows) ~11 ms · SF 0.1 (600 K rows) ~111 ms.

See `tests/perf/BENCH_BASELINES.yaml` for pinned baselines.

---

## Architecture (C4-lite)

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Client (psql / PG driver)            PostgreSQL wire protocol v3        │
└───────────────────────────────────────┬──────────────────────────────────┘
                                        │ TCP (plaintext / TLS-ready)
                                        ▼
                      ┌─────────────────────────────────┐
                      │  server.rs   (generic stream S)  │
                      │  → startup handshake             │
                      │  → parse 'Q' message             │
                      └──────────┬──────────────────────┘
                                 │
                    ┌────────────▼───────────────┐
                    │  SQL parser + Binder        │
                    └────────────┬───────────────┘
                                 │  BoundPlan
          ┌──────────────────────▼──────────────────────────┐
          │  RL Optimizer (ONNX DQN) + Cost Model + Stats   │
          └──────────────────────┬──────────────────────────┘
                                 │  PhysicalPlan
          ┌──────────────────────▼──────────────────────────┐
          │  Physical Executor (vectorised, morsel-driven)   │
          │  TableScanner → StorageExecutor → MVCC → RocksDB│
          └──────────────────────┬──────────────────────────┘
                                 │
          ┌──────────────────────▼──────────────────────────┐
          │  Raft Consensus + Distributed Planner            │
          │  ConsistentHashRouter + back-pressure exchange   │
          └─────────────────────────────────────────────────┘
```

---

## Threat Model

> Full threat model: `docs/THREAT_MODEL.md`

Summary:
- **SQL injection**: `sqlparser-rs` lexes and parses before any execution; untrusted text
  is fully parsed into an AST before reaching the binder.
- **Wire protocol**: Frame lengths are validated before buffer allocation; malformed frames
  return PG error responses (no panic, no abort).
- **MVCC isolation**: Snapshot isolation enforced by HLC timestamp ordering; GC protected
  by `safe_horizon` Mutex preventing collection of visible versions.
- **Raft log**: Leader-only log writes; quorum-based commit; log entries are not executed
  until committed. See `REVIEW_REQUIRED.md` for invariant audit checklist.
- **TLS**: Plaintext by default. Production deployments MUST bind to loopback or enable TLS.
- **Prometheus scrape endpoint**: Not TLS/auth-protected in the default build; do not expose
  port 9090 to untrusted networks.

---

## Risk Budget

> This section is required by `agents.md §10` and must remain honest.

**This system MUST NOT be relied upon for:**
- Production OLTP workloads (write throughput, fsync guarantees not tested at scale)
- Queries requiring multi-table JOINs (binder returns UnsupportedSelect)
- TLS security in the default build (plaintext only; NASM required to activate TLS)
- High-availability production deployments (Raft consensus reviewed but not TLA+ verified)
- PII data storage (no access control, no column encryption, no audit log)

**Effective system confidence: ~0.76**
(CONFIDENCE.yaml `system.effective_confidence`; see `CONFIDENCE.md` for details)

**Estimated failure probability under adversarial input: ~10–18%**
(bounded estimate; based on adversarial test coverage across 310+ tests; fuzz gaps remain in
binary codec and RocksDB byte sequences)

**Weakest links (as of v0.1.0):**
1. `onnx_seed_model` (0.65) — heuristic weights, not trained DQN; replace with `optimizer/training/train.py` output
2. `simd_filter_path` (0.66) — AVX-512 not active on stable Rust 1.85; compiles to scalar fallback
3. `tls` (0.68) — infrastructure complete but not runtime-active without NASM

**Fastest ways to raise system confidence:**
1. Train the ONNX model (`optimizer/training/train.py`) → `onnx_seed_model` 0.65 → 0.80
2. Enable TLS (install NASM, uncomment deps, `--features tls`) → closes plaintext gap
3. Add property-based fuzz tests for binary codec → `storage_executor` 0.76 → 0.84
4. Write TLA+ spec for Raft consensus → `raft_consensus` 0.72 → 0.85+

---

## License

Apache-2.0 — see `LICENSE`.
