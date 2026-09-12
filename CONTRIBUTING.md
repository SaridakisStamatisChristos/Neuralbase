# Contributing to NeuralBase

NeuralBase welcomes focused contributions that improve correctness, evidence, clarity, or performance without overstating system guarantees.

## Development prerequisites

The crate declares Rust `1.88.0` as its minimum supported version, and `rust-toolchain.toml` pins that exact version for repository gates.

On Ubuntu 24.04, CI installs native dependencies equivalent to:

```bash
sudo apt-get update
sudo apt-get install -y \
  llvm-18-dev \
  libclang-18-dev \
  clang-18 \
  librocksdb-dev \
  nasm
export LIBCLANG_PATH=/usr/lib/llvm-18/lib
export ROCKSDB_INCLUDE_DIR=/usr/include
```

Docker is required for the PostgreSQL 16 TPC-H reference suite and for Compose-based integration work.

`.cargo/config.toml` supplies a Windows LLVM path and two build jobs. On Linux, export `LIBCLANG_PATH` as above so the real environment overrides that fallback; on other platforms set it to the installed libclang directory.

## Build

```bash
cargo build --locked
cargo build --locked --features tls
cargo run --locked --bin neuralbase
cargo run --locked --bin neuralbase-backup -- --help
```

The project has two binaries, so `cargo run` requires `--bin`. The server uses environment configuration; the backup binary has explicit create/verify/restore subcommands. Build C/C++ tooling and libclang are required by native dependencies; the Dockerfile provides a separate Debian build environment.

## Required gates

Before opening a pull request, run the gates relevant to your change:

```bash
make test
make lint
make confidence
make adversarial
```

`make lint` is intentionally strict: it runs `cargo fmt --check` and Clippy across all targets with warnings denied under the repository's pinned Rust 1.88.0 toolchain. Treat new warnings as CI failures rather than suppressing them without a documented reason.

For SQL semantic changes:

```bash
make tpch-correctness
```

For benchmark changes:

```bash
make bench
```

Do not commit generated build/lint logs.

## Contribution principles

### Preserve explicit distributed semantics

Persistent table and SCRAM identity mutations already use quorum commit plus confirmed durable local apply. Preserve those guarantees, the Phase-5 recovery model, and Phase-6 leader-path read barriers. Process tests support those scopes; they do not establish general production SQL HA, arbitrary-follower strong reads or automatic deployment membership reconciliation.

### Prefer correctness over feature count

A smaller patch with explicit semantics, failure behavior, diagnostics, and tests is preferred to a broad feature patch with weak validation.

### Keep APIs backward compatible when practical

Avoid breaking public scalar/configuration behavior solely to introduce a new internal path. When a breaking change is necessary, document migration behavior explicitly.

### Bound resource usage

Database operators that materialize intermediate state should have an intentional memory/row-count strategy. Avoid introducing unbounded queues or waits.

### Fail visibly

Malformed cluster configuration, persistence failures, and impossible state transitions should produce actionable errors rather than silent fallback where correctness is at risk.

## Pull request expectations

A strong PR includes:

- problem statement and semantic scope;
- implementation summary;
- failure/edge cases considered;
- tests added/updated;
- documentation updated when behavior changes;
- explicit statement of any remaining boundary.

Use the repository pull-request template as a checklist.

## SQL changes

Update `docs/SQL_SUPPORT.md` when adding or changing SQL behavior. If PostgreSQL parity is intended, add a deterministic reference comparison where feasible.

Cover NULL/error behavior and budget/resource behavior, not only happy paths.

## Raft/distributed changes

Update `docs/DISTRIBUTED.md` for changes to:

- transport identity/routing;
- commit/apply semantics;
- persistence guarantees;
- shutdown/backpressure;
- membership behavior;
- leader/follower client behavior.

Prefer separate-process tests for claims that depend on process isolation.

## Deployment changes

Helm changes should lint and render default, existing-identity auth, paired migration Secret/digest, and SQL TLS configurations, and reject incomplete migration and automatic HPA configuration. Coordinated membership already exists; HPA must remain disabled until a deployment reconciler safely invokes that protocol. Keep CI and release render examples synchronized. Raw Kubernetes examples must not reintroduce per-node credential-file authorities.

Update `docs/DEPLOYMENT.md` when configuration or topology changes.

## Documentation standard

Documentation should distinguish implementation, executable evidence, deployment assumptions, and future plans. Avoid language such as "production-ready", "fully PostgreSQL compatible", or "HA" unless the repository contains evidence for that exact claim.

Check [CONFIGURATION.md](docs/CONFIGURATION.md) against runtime readers when adding variables. Verify snippets use the correct binary and send one SQL statement per request. For documentation/deployment edits, inspect relative links and manifest rendering; do not report Rust gates as passed when the toolchain was unavailable. Historical ADRs and benchmark baselines should retain their original scope rather than acquire unmeasured current claims.

## Commit hygiene

Keep generated artifacts and one-off diagnostic output out of the working tree. Use descriptive commit messages and avoid mixing unrelated refactors with semantic changes.

## Security issues

Do not publish exploit details in a normal issue. Follow [SECURITY.md](SECURITY.md).
