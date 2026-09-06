# NeuralBase documentation

This directory contains the technical documentation for NeuralBase. The root `README.md` is the project entry point; these documents carry the details needed to evaluate, develop, deploy, or review the engine.

## Core documents

| Document | Audience | Scope |
|---|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Developers / reviewers | Major components, execution flow, state boundaries, invariants |
| [SQL_SUPPORT.md](SQL_SUPPORT.md) | Users / developers | SQL feature matrix, local DDL/DML semantics, execution limits |
| [DISTRIBUTED.md](DISTRIBUTED.md) | Distributed-systems reviewers | Raft transport, apply path, shutdown/backpressure, current replication boundary |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Operators / evaluators | Local, Compose, Kubernetes, Helm, TLS, auth, fixed membership |
| [TESTING.md](TESTING.md) | Contributors / reviewers | Test taxonomy, CI gates, TPC-H reference methodology, evidence limits |
| [THREAT_MODEL.md](THREAT_MODEL.md) | Security reviewers | Threats, trust boundaries, mitigations |
| [TSAN.md](TSAN.md) | Contributors | ThreadSanitizer workflow and caveats |

Repository-level policy and project files:

- [`../ROADMAP.md`](../ROADMAP.md) — prioritized engineering roadmap.
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — contributor setup and quality gates.
- [`../SECURITY.md`](../SECURITY.md) — vulnerability reporting policy.
- [`../CONFIDENCE.md`](../CONFIDENCE.md) / [`../CONFIDENCE.yaml`](../CONFIDENCE.yaml) — evidence-scoped confidence model.
- [`../CHANGELOG.md`](../CHANGELOG.md) — current development changelog.

## Documentation rule

A change that alters public behavior, configuration, SQL semantics, persistence guarantees, Raft semantics, deployment topology, or test evidence should update the relevant document in the same pull request.

Documentation must distinguish:

1. code that exists,
2. behavior verified by executable tests,
3. deployment behavior exercised in CI,
4. future design intent.

Those categories are intentionally not interchangeable.
