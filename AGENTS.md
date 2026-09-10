# AGENTS.md

Repository-specific guidance for coding agents and automated contributors.

## Project identity

NeuralBase is an experimental Rust SQL engine. Configured clusters replicate persistent table mutations and SCRAM identity through deterministic Raft-backed state machines, use SQL-aware snapshots, support learner/joint-consensus membership changes, and provide the tested Phase-5 NBBK/NBEC backup/restore/fresh-cluster recovery lifecycle.

Do not turn those scoped guarantees into a claim of general or production SQL HA. Reads are still local and may lag, deployment membership reconciliation and automatic node replacement are not implemented, PITR/automatic DR remain open, and the online backup coordinator is currently an in-process API rather than a standalone live-server CLI.

## Toolchain and gates

- Rust: `1.88.0`
- Core test gate: `make test`
- Formatting/Clippy: `make lint`
- Evidence assertions: `make confidence`
- Adversarial suites: `make adversarial`
- PostgreSQL reference suite: `make tpch-correctness`

Run the narrowest relevant tests during iteration and the complete affected gates before finishing.

## Engineering rules

1. Preserve existing scalar/public APIs unless a breaking change is necessary and documented.
2. Prefer explicit errors over silent fallback where persistence, consensus, identity, or correctness is involved.
3. Keep queues, waits, joins, and materialized intermediates intentionally bounded.
4. Do not hide deterministic hangs behind timeouts; timeouts are diagnostics/safety guards.
5. Keep local durability distinct from replicated durability in code comments and docs.
6. Snapshot creation must be durable before Raft prefix truncation; snapshot installation must restore durable SQL state before success acknowledgement.
7. Do not enable HPA until deployment reconciliation sequences replica changes through the existing coordinated membership protocol.
8. Update relevant docs with behavioral/configuration changes.
9. Do not commit build logs, Clippy output, temporary databases, secrets, or private keys.

## Documentation map

- Architecture: `docs/ARCHITECTURE.md`
- SQL surface: `docs/SQL_SUPPORT.md`
- Raft/distributed semantics: `docs/DISTRIBUTED.md`
- Deployment/configuration: `docs/DEPLOYMENT.md`
- Tests/evidence: `docs/TESTING.md`
- Roadmap: `ROADMAP.md`
- Security: `SECURITY.md`

## Definition of done

A substantive change is not done until its semantics, failure cases, tests, and documentation agree with each other. Distributed lifecycle work additionally requires exact-head CI and post-merge `main` CI before its milestone is considered complete.
