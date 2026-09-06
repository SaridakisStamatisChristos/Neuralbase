# Testing and evidence

NeuralBase separates different kinds of evidence so that a green test suite is not interpreted more broadly than it should be.

## Local commands

```bash
make test
make lint
make confidence
make adversarial
make tpch-correctness
make bench
```

## `make test`

Runs the core Rust integration/unit test set with TLS features enabled where required by the repository gate.

This is the broad regression gate for SQL, storage, MVCC, authentication, Raft, transport, and supporting modules.

The TPC-H PostgreSQL reference harness is deliberately excluded from this generic target because it launches an external PostgreSQL Docker container.

## `make lint`

Runs formatting and Clippy with warnings denied.

A lint-green commit therefore satisfies both rustfmt and the repository's current Clippy warning policy. Lint success is code-quality evidence, not functional correctness evidence.

## `make confidence`

Runs assertions against the machine-readable confidence artifact.

The confidence gate protects project claims as well as code. In particular, it prevents the repository from silently claiming production readiness or replicated SQL semantics while those boundaries remain unimplemented.

## `make adversarial`

Exercises focused edge/failure suites across vectorized execution, optimizer behavior, MVCC, and Raft.

Adversarial tests are especially important for bounded resources, race-prone lifecycle behavior, consensus corner cases, and malformed inputs.

## TPC-H PostgreSQL reference suite

`make tpch-correctness` runs `tests/tpch_correctness.rs` with the opt-in `tpch-reference-tests` feature. The harness starts PostgreSQL 16 in Docker and compares NeuralBase output to reference output for the checked-in Q1-Q22 queries on a deterministic small dataset.

The CI job:

- is separate from core tests;
- runs serially/verbosely enough to identify a stuck query;
- has an explicit timeout;
- prints PostgreSQL container logs on failure;
- always attempts container cleanup.

This evidence supports the exact tested SQL/data combinations. It does not establish official TPC-H compliance or arbitrary-query PostgreSQL equivalence.

## Raft transport/lifecycle regression coverage

`tests/raft_tcp_transport.rs` verifies that distinct logical node IDs route over explicit loopback TCP addresses in both directions.

Raft adversarial coverage includes bounded apply-channel behavior. A regression test previously exposed a real lifecycle deadlock where shutdown could wait forever behind a full apply channel; the implementation now allows shutdown to interrupt that blocked apply send.

## Deployment-manifest gate

CI also performs:

- Helm lint;
- default chart rendering;
- auth-enabled rendering;
- TLS-enabled rendering;
- explicit rejection of the unsafe fixed-membership HPA manifest.

A successful render proves template consistency for the checked configuration. It does not prove a live Kubernetes cluster upgrade/failover path.

## Time bounds

Long-running CI commands are intentionally bounded. A test suite that deadlocks should fail with diagnostics rather than consume a runner indefinitely.

Timeouts are a diagnostic safety net, not a substitute for fixing deterministic hangs.

## Benchmarks

Benchmark results are meaningful only with:

- exact commit;
- workload and scale;
- hardware/runtime context;
- build profile/features;
- warmup/repetition methodology.

Do not turn a repository-specific benchmark result into a general claim that one optimizer or execution strategy is universally faster.

## What green CI means

Green CI means the checked commit passed the repository's current executable gates.

It does **not** mean:

- production readiness;
- complete PostgreSQL compatibility;
- distributed SQL durability;
- arbitrary crash consistency;
- security certification;
- performance superiority outside the measured workloads.

Those distinctions are part of NeuralBase's evidence model and should remain visible in documentation and reviews.
