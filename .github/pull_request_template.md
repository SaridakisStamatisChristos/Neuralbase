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
- [ ] Helm/default/auth/TLS rendering checked for deployment changes

## Documentation

- [ ] README/docs updated for public behavior/configuration changes
- [ ] `docs/SQL_SUPPORT.md` updated for SQL surface changes
- [ ] `docs/DISTRIBUTED.md` updated for Raft/replication semantics
- [ ] `docs/DEPLOYMENT.md` updated for topology/config changes
- [ ] `ROADMAP.md` updated if a release boundary changed

## Claim check

- [ ] This PR does not imply replicated SQL HA unless SQL mutations are actually quorum-committed/applied and supported by failover evidence.
