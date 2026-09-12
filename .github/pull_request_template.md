## Problem

Describe the concrete defect, limitation, or semantic gap.

## What changed

Summarize the implementation without hiding important behavioral boundaries.

## Semantics and failure cases

- What externally visible behavior changes?
- What happens on malformed input, persistence failure, timeout, shutdown, or partial failure?
- Does this affect local versus replicated durability semantics?

## Validation

- [ ] Relevant focused tests added/updated
- [ ] `make test`
- [ ] `make lint`
- [ ] `make confidence`
- [ ] `make adversarial` when relevant
- [ ] `make tpch-correctness` for SQL semantic changes when relevant
- [ ] Helm/default/existing-identity/migration/TLS rendering and negative migration/HPA cases checked for deployment changes

## Documentation

- [ ] README/docs updated for public behavior/configuration changes
- [ ] `docs/SQL_SUPPORT.md` updated for SQL surface changes
- [ ] `docs/DISTRIBUTED.md` updated for Raft/replication semantics
- [ ] `docs/DEPLOYMENT.md` updated for topology/config changes
- [ ] `docs/CONFIGURATION.md` matches runtime defaults and aliases
- [ ] `ROADMAP.md` updated if a release boundary changed

## Claim check

- [ ] Claims preserve the tested replicated table/identity, backup/recovery and leader-path read boundaries without implying production HA, arbitrary-follower strong reads or automatic membership reconciliation.
