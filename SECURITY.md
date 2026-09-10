# Security policy

NeuralBase is experimental pre-1.0 software and is not currently presented as a production-hardened database. Security reports are still taken seriously.

## Reporting a vulnerability

Please do **not** disclose exploitable security details in a public issue or discussion.

Use GitHub's private vulnerability reporting / Security Advisory flow for this repository when available. If the private reporting UI is unavailable, open a minimal public issue requesting a private contact channel **without including exploit details, credentials, secrets, proof-of-concept payloads, or sensitive logs**.

A useful private report includes:

- affected commit/version;
- affected component;
- threat model / prerequisites;
- reproducible steps or proof of concept;
- expected versus observed behavior;
- impact assessment;
- suggested mitigation if known.

## Scope

Security-relevant areas include, but are not limited to:

- PostgreSQL wire/session parsing;
- authentication and credential persistence;
- TLS and certificate validation;
- Raft transport and peer identity;
- malformed SQL/input resource exhaustion;
- RocksDB/storage path handling;
- container/Kubernetes/Helm defaults;
- dependency/supply-chain vulnerabilities;
- operator backup confidentiality/integrity, recovery fencing and key handling.

## Supported versions

The project has not published a stable release series. Security fixes target the current `main` development line unless a future release policy states otherwise.

## Security boundaries

Current architecture has several explicit boundaries:

- Clustered SQL table state and SCRAM identity are replicated through Raft; standalone mode retains its local behavior. Operator backups therefore contain sensitive database and verifier material.
- Deployment manifests are development/research topology examples, not a production security baseline.
- TLS/authentication must be configured for the target environment.
- Availability controls and resource budgets reduce some denial-of-service risks but do not constitute a complete hostile-tenant isolation model.

Review [`docs/THREAT_MODEL.md`](docs/THREAT_MODEL.md) before deploying NeuralBase in any environment containing sensitive data or untrusted clients.

## Disclosure handling

The project aims to acknowledge valid reports, reproduce the issue, prepare a fix and regression test, and disclose details only after a reasonable remediation path exists. Timelines depend on severity and project availability.
