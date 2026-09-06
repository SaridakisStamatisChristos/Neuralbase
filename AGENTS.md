# AGENTS.md

Repository-specific guidance for coding agents and automated contributors.

## Project identity

NeuralBase is an experimental Rust SQL engine. It has a real Raft subsystem, but SQL mutations are **not yet** committed through a replicated Raft state machine.

Never convert the presence of Raft into a claim of replicated SQL HA without implementing and testing that semantic link.

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
6. Do not enable HPA for the fixed-membership Raft deployment until coordinated membership changes exist.
7. Update relevant docs with behavioral/configuration changes.
8. Do not commit build logs, Clippy output, temporary databases, secrets, or private keys.

## Documentation map

- Architecture: `docs/ARCHITECTURE.md`
- SQL surface: `docs/SQL_SUPPORT.md`
- Raft/distributed semantics: `docs/DISTRIBUTED.md`
- Deployment/configuration: `docs/DEPLOYMENT.md`
- Tests/evidence: `docs/TESTING.md`
- Roadmap: `ROADMAP.md`
- Security: `SECURITY.md`

## Definition of done

A substantive change is not done until its semantics, failure cases, tests, and documentation agree with each other.
