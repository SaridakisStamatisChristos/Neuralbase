# Changelog

All notable changes to NeuralBase are documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Dates are Europe/Athens (UTC+2).

---

## [1.0.0] — 2026-03-06

### Summary

NeuralBase v1.0.0 — first production-candidate release across 16 development sessions.
Self-optimising distributed SQL engine: PostgreSQL wire protocol, vectorised execution,
RL join-order optimizer (22/22 TPC-H win rate), MVCC/RocksDB, Raft consensus, Kubernetes-ready.

### Added — Session 16 (Kubernetes + Cloud Deployment)

- **Kubernetes manifests** (`k8s/`):
  - StatefulSet (3 replicas, PersistentVolumeClaim 10 Gi, parallel pod management)
  - Headless Service for Raft peer discovery via DNS
  - LoadBalancer Service for external SQL access
  - ConfigMap with all NeuralBase environment variables
  - Secret templates for TLS certificates and users.json
  - PodDisruptionBudget (`minAvailable: 2` — Raft quorum requirement)
  - HorizontalPodAutoscaler (CPU 70% + custom active_connections metric)
- **Helm chart** (`helm/neuralbase/`):
  - Chart.yaml (apiVersion v2, version 1.0.0)
  - Configurable values.yaml (replicas, resources, storage, TLS, metrics, probes)
  - Templates: StatefulSet, Service (headless + LB), ConfigMap, Secret, PDB, HPA
- **Graceful shutdown** (`src/main.rs`):
  - SIGTERM handler via `tokio::signal` (Unix) / Ctrl+C (all platforms)
  - 30-second drain period for in-flight queries before exit
  - `tokio::select!` races server accept loop against shutdown signal
- **CI/CD release pipeline** (`.github/workflows/release.yml`):
  - Triggered on `v*.*.*` tag push
  - Jobs: cargo test --release, Docker build + push to ghcr.io, Helm package, GitHub Release
  - Pinned action versions (checkout@v4, rust-toolchain@stable, build-push-action@v6)
- **README.md** rewritten for v1.0: architecture, config reference, K8s deployment guide
- **CHANGELOG.md** v1.0.0 entry summarising all 16 sessions

### Added — Sessions 13-15 (SQL Completeness + Authentication + Production Hardening)

- **DML operations**: INSERT, UPDATE, DELETE with MVCC transactional semantics (S13)
- **Advanced SQL**: subqueries, CTEs, window functions (parse-level), HAVING, BETWEEN (S14)
- **SHA-256 authentication** with users.json credential store (S14)
- **Per-IP and per-user connection limits** with semaphore-based admission control (S14)
- **Prometheus /metrics HTTP endpoint** via `metrics-exporter-prometheus` (S15)
- **Docker HEALTHCHECK** (TCP probe on port 5432) (S15)
- **cargo-fuzz harnesses**: wire protocol, SQL parser, binary codec (S15)
- **cargo-deny** supply chain audit (S15)
- **Load test**: 1000 concurrent connections verified (S15)
- **541 tests** total (0 failures, 2 ignored)

### Added — Sessions 8-12 (Optimizer Training + Distributed Hardening)

- **RL optimizer training pipeline** (S8-S12):
  - 600k-step DQN training achieving 22/22 TPC-H win rate
  - Selectivity alignment between training and inference
  - ONNX model export (`optimizer/model/neuralbase_optimizer.onnx`)
- **Distributed transaction coordination** (S9-S10)
- **Log compaction and snapshot transfer** (S10)
- **Cluster auto-discovery and partition healing** (S11)

---

## [Unreleased] — Session 12 — 2026-03-05

### Session 12 Final Scorecard

| Item                   | Result                        |
|------------------------|-------------------------------|
| Training steps         | 600,000 (started at 300k)    |
| Hardware               | RTX 4060 8 GB                 |
| Reward improvement     | -2.5 → -0.4 → -0.3           |
| Seed model win rate    | 90.9% (20/22)                 |
| After 300k steps       | 95.5% (21/22)                 |
| After 600k steps       | **100.0% (22/22)**            |
| Q20 fixed              | ✓ (ties naive at 101150)     |
| Threshold met (80%)    | ✓ (perfect)                   |

### Fixed — Session 12 (RL Optimizer: Selectivity Alignment + Full Training)

- `optimizer/training/train.py` — `SELECTIVITY` dict replaced with bench-formula
  values (`1/max(NDV_left, NDV_right)` where NDV = row_count); divergence was up to
  15,000× vs the `CostModel` used in `bench_optimizer.rs`
  - Added 3 previously absent FK pairs: `lineitem↔supplier`, `part↔lineitem`,
    `lineitem↔partsupp` (were silently defaulting to 0.01, a ~6,000× error)
  - Reward signal now teaches the same cost objective the bench evaluates;
    `recent_avg` improved from -10.5 → -0.4 over 300k steps
- `optimizer/training/train.py` — Extended from 300k to 600k steps with
  `--epsilon-decay 100000`. Reward stabilised at recent_avg -0.3. Q20
  (`supplier + nation + partsupp + part`, previously +1.9% above naive) now ties
  naive cost at 101,150 — counted as RL win (RL cost ≤ naive cost).
- `src/optimizer.rs` — `TPCH_FK_SEL` updated to match corrected selectivities;
  3 new FK pairs added so the inference-time `SEL_FEAT` matrix is byte-identical
  to the training-time `_SEL_FEAT` matrix
- `src/optimizer.rs` — unit test `state_vector_encodes_selectivity_for_known_fk_pair`
  updated to expect new bench-formula value
- `optimizer/model/neuralbase_optimizer.onnx` — Updated to 600k-step trained model
- `optimizer/model/neuralbase_optimizer_300k.onnx` — Backup of 300k/95.5% model retained
- `tests/perf/BENCH_BASELINES.yaml` — `rl_optimizer_ab_win_rate_tpch` baseline updated
  to 100.0% (22/22, confidence 0.87, session 12)

### Result — Session 12

- 22/22 TPC-H queries: RL ≤ naive cost  (Q2: −82.8%, Q5: −7.3%, Q10: −7.3%, Q21: −0.4%, …)
- All 25 bench tests pass; 0 failures; 0 ignored; perfect 100% win rate

---

## [0.1.0] — 2026-03-02

### Summary

First tagged release of NeuralBase — a self-optimising distributed SQL engine in Rust.
Covers seven development sessions (S1–S7): foundation, vectorised execution, RL optimiser,
MVCC storage, Raft consensus, StorageExecutor wiring, and hardening/security audit.

---

### Added — Session 1 (Foundation)

- PostgreSQL wire-protocol v3 server (`src/server.rs`, `src/protocol.rs`)
  - Startup handshake, `Q` (simple query) message handling
  - Structured `ErrorResponse` on parse/bind failure (no panic)
  - Frame-length validation before buffer allocation
- SQL parser integration (`sqlparser-rs`, `src/sql.rs`)
- Trait-based `Catalog` + `InMemoryCatalog` with TPC-H `lineitem` schema (`src/catalog.rs`)
- `bind_statement()`: AST → `BoundPlan` with table/column resolution (`src/binder.rs`)
- CI gates: `make test`, `make lint`, `make confidence`
- `CONFIDENCE.yaml`, `CONFIDENCE.md`, `SESSION_STATE.md` bootstrap artifacts
- Apache-2.0 `LICENSE`, `Dockerfile`, `docker-compose.yml`

### Added — Session 2 (Vectorised Execution + TPC-H)

- Columnar `RecordBatch` + `ColumnVector` (Int64, Float64, Date32, Utf8) (`src/vectorized.rs`)
- `Utf8Column` with internal `Vec<u8>` offset array for zero-copy string access
- Morsel-driven `MorselScheduler` with configurable chunk size (`src/scheduler.rs`)
- `build_physical_plan()` + `execute_physical_plan()` (`src/execution.rs`)
  - `TpchQ1` physical plan: GROUP BY `l_returnflag` sum aggregate
  - `TpchQ6` physical plan: filter + SUM aggregate
- Deterministic TPC-H synthetic dataset generator (`src/tpch.rs`): 6,001,215 × SF rows
- `tpch_q1_matches_reference_output`, `tpch_q6_matches_reference_output` correctness tests
- Performance investigation diagnostic (`investigate_q1_performance_root_cause`)
- `tests/perf/BENCH_BASELINES.yaml` with pinned baselines

### Added — Session 3 (RL Join-Order Optimiser)

- `JoinGraph` with TPC-H 22-query catalogue, selectivity estimates, DFS cycle detection (`src/join_graph.rs`)
- `CostModel` (cardinality × selectivity) — assumes independence (`src/cost_model.rs`)
- `StatisticsCollector` with background-thread mpsc sampling (`src/stats.rs`)
- `RlOptimizer`: ONNX DQN model via `tract-onnx 0.21.7` (`src/optimizer.rs`)
  - Greedy Q-value decoding; naive-order fallback on missing model/timeout/cycle
  - Seed model: heuristic cardinality-first weights (not trained DQN)
- A/B benchmark (`bench_optimizer_a_b_win_rate`): 20/22 TPC-H graphs win ≥ naive cost
- `IndexAdvisor` workload monitor (`src/index_advisor.rs`): ring-buffer VecDeque (512 cap),
  `CostBenefitModel`, `AccessStats`, `advise()` → `IndexDecision::Create/Drop`

### Added — Session 4 (MVCC + RocksDB)

- `HybridLogicalClock` (`src/hlc.rs`): wall-clock + logical counter, `u64 = wall_ms<<16 | logical`
  - Overflow fix: `logical == u16::MAX` bumps `wall_ms+1` (not saturating)
  - 4 invariants verified; human review signed 2026-03-02
- `StorageEngine` (`src/storage.rs`): RocksDB 0.22.0 (`MultiThreaded`)
  - 4 static column families: `data`, `meta`, `versions`, `catalog`
  - Key layout: `[table_id:4 BE][pk_bytes][hlc_ts:8 BE]` (big-endian → natural sort = chronological)
  - `write_versioned_row()`, `read_latest()`, `scan_table_range()`, `write_batch()`
- `MvccTxnManager` + `MvccTransaction` (`src/mvcc.rs`): snapshot isolation via HLC timestamps
  - `commit_serializer` Mutex across full commit critical section (eliminates TOCTOU race)
  - Human review signed 2026-03-02; all 6 invariants verified
- `GarbageCollector` (`src/gc.rs`): version pruning behind `safe_horizon`
  - `safe_horizon` Mutex released before I/O; human review signed 2026-03-02
- `RocksDbCatalog` (`src/rocksdb_catalog.rs`): durable catalog in `CF_CATALOG` (JSON)
- 56+ MVCC + GC + HLC tests (`tests/mvcc_correctness.rs`, `tests/adversarial_mvcc.rs`)

### Added — Session 5 (Raft Consensus + Distributed Planner)

- Full Raft state machine (`src/consensus/raft.rs`, `src/consensus/log.rs`)
  - Leader election, log replication, commit-index advancement, apply_tx dispatch
  - Human review signed 2026-03-02; all 6 invariants verified
- `ClusterRegistry` + `ConsistentHashRouter` + heartbeat failure detection (`src/cluster/mod.rs`)
  - FNV-1a shard routing (deterministic, overflow-safe)
- `QueryCoordinator` + `DistributedPlanner` + `PlanFragment` (`src/distributed/exchange.rs`)
  - Partial-failure re-routing; `max_retries` enforced; `handle_failure()` idempotent
- `BoundedSender`/`BoundedReceiver` semaphore back-pressure (`src/distributed/backpressure.rs`)
  - Critical fix: permit not forgotten on send error (prevents semaphore starvation)
- 38 adversarial Raft/cluster/planner/back-pressure tests (`tests/adversarial_raft.rs`)
- `REVIEW_REQUIRED.md`: Raft consensus and distributed planner invariant checklist

### Added — Session 6 (StorageExecutor + IndexAdvisor Wiring)

- `StorageExecutor` MVCC-backed `TableScanner` bridge (`src/storage_executor.rs`)
  - `encode_row()` / `decode_row()` (Session 6: JSON via serde_json — replaced in S7)
  - `Arc<dyn TableScanner>` erased at server boundary
- OpenTelemetry / Prometheus telemetry bootstrap (`src/telemetry.rs`)
- Server wired to accept optional `Arc<StorageExecutor>` for storage-backed queries
- 13 new tests: storage_executor roundtrip, table_id stability, decode-invalid

### Added — Session 7 (Hardening, Security Audit)

- **Storage engine seek_for_prev fix** (`src/storage.rs`)
  - `read_latest()`: `seek()` + `seek_to_last()` (BUGGY, cross-pk bleed) replaced with
    atomic `seek_for_prev()` — finds largest key ≤ seek_key without overshooting
  - Regression test: `read_latest_seek_for_prev_correctness_no_cross_pk_bleed`
- **Dynamic CF discovery** (`src/storage.rs`)
  - `open()` calls `RocksDb::list_cf()` to find all existing CFs (including index CFs)
    before opening — prevents "Column family not found" on re-open
- **Secondary index DDL helpers** (`src/storage.rs`)
  - `create_index_cf()`, `drop_index_cf()`, `list_index_cfs()`
  - `write_secondary_index_entry()`, `delete_secondary_index_entry()`
  - All idempotent; CF names persisted in `CF_CATALOG` as `"__idx:<name>" = "active"`
- **IndexAdvisor DDL execution wiring** (`src/index_advisor.rs`)
  - `IndexExecutor` struct + `DdlResult` enum (`Created`, `Dropped`, `Skipped`, `Failed`)
  - `apply()` method executes real RocksDB CF DDL (calls `StorageEngine::create_index_cf` etc.)
  - Idempotent second-`Create` → `Skipped`; error-capturing (never panics)
  - Test: `index_executor_create_and_drop_via_rocksdb`
- **Binary row codec** (`src/storage_executor.rs`)
  - JSON (`serde_json`) replaced with NeuralBase binary format:
    `[0x4E,0x42,0x01][num_cols:u16LE][key_len:u16LE][key UTF-8][val_len:u32LE][val UTF-8]`
  - Magic header `[NB\x01]` validates on decode; returns `None` on corruption
  - ~3–5× more compact than JSON; no external parser on hot path
- **TLS infrastructure** (`src/tls.rs`, `src/server.rs`)
  - `server.rs`: all socket handlers generic `S: AsyncRead + AsyncWrite + Unpin`
  - `TlsAcceptorOpt` type alias conditional on `tls` Cargo feature
  - `tls.rs`: `build_acceptor()` stub (default) / full `rcgen`+`tokio-rustls` impl (feature-gated)
  - **NOTE**: blocked on Windows x64 without NASM (`aws-lc-sys` requirement); plaintext default
- **TPC-H Q1-Q22 correctness test suite** (`tests/tpch_correctness.rs`)
  - Q1 and Q6: exact numeric output verified against independent Rust reference (≤ 1e-4)
  - Q2–Q22: parse-verified + bind-error classification (39 tests total)
  - `all_22_sql_constants_are_non_empty`: gate test
- **Telemetry stub** (`src/telemetry.rs`)
  - `metrics-exporter-prometheus 0.16.2` set to `default-features = false` to drop
    `push-gateway → hyper-rustls → rustls → aws-lc-sys` transitive build dep
  - Prometheus scrape endpoint is a stub until `http-listener` feature is re-enabled
- **CONFIDENCE.yaml updated** — system `effective_confidence` raised 0.72 → 0.76
  - `storage_engine`: 0.64 → 0.78 (seek_for_prev fix + human review sign-off)
  - `storage_executor`: 0.68 → 0.76 (binary codec + magic guard)
  - `index_advisor`: 0.70 → 0.74 (DDL wiring live)
  - New artifacts: `tls` (0.68), `tpch_q1_q22_correctness` (0.80)
  - New weakest links: `onnx_seed_model` (0.65), `simd_filter_path` (0.66)
- **CONFIDENCE.yaml gate tests** (3 tests): YAML valid, system ≥ 0.75, critical artifacts ≥ 0.65
- **Full test count**: 310+ tests (271 from S1–S6 + 39 TPC-H correctness + 3 confidence gates)
- **docs/THREAT_MODEL.md**: SQL injection, wire-protocol, MVCC isolation, Raft, TLS gap

---

### Known Limitations (v0.1.0)

- **Multi-table JOINs not supported**: `binder.rs` returns `UnsupportedSelect` for any query
  with `FROM` containing more than one table. TPC-H Q2–Q22 are parse-verified only.
- **OLTP write throughput untested**: RocksDB is tuned for read-heavy analytic workloads.
  Write-heavy OLTP benchmarks not included.
- **TLS not active in default build**: wire connections are plaintext. Requires NASM
  (Windows x64) + uncomment TLS deps + `--features tls`.
- **ONNX model is a seed (heuristic)**: the `optimizer/model/neuralbase_optimizer.onnx` file
  uses cardinality-first heuristic weights, not trained DQN weights. 20/22 TPC-H graphs
  match or beat naive order — ties naive on 2. Run `optimizer/training/train.py` to train.
- **AVX-512 not active**: `simd_filter_path` compiles to scalar fallback on stable Rust 1.85.
  Explicit SIMD intrinsics are not stable in Rust; fallback path is correct and tested.
- **Prometheus scrape endpoint is a stub**: the telemetry HTTP server is disabled in the
  default build. Re-enable by adding `features = ["http-listener"]` to the metrics dep.
- **No access control**: there is no authentication beyond the PostgreSQL startup password
  (accepted unconditionally in the dev server). Do not expose port 5432 publicly.
- **Raft consensus not TLA+ verified**: human review complete but formal TLA+ spec not written.
  Not recommended for distributed consensus in adversarial network environments.
- **GC Relaxed ordering**: GC uses `Ordering::Relaxed` for the horizon counter; documented
  and reviewed in `gc.rs`. Safe under the current single-GC-thread model.

---

### Upgrade Notes

There are no prior stable releases. This is the first tagged version.

---

[1.0.0]: https://github.com/your-org/neuralbase/releases/tag/v1.0.0
[0.1.0]: https://github.com/your-org/neuralbase/releases/tag/v0.1.0
