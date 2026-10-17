# Historical development evidence

This directory preserves implementation-era closure records that remain useful for provenance, reproducibility, or bounded evidence review but are **not** the current product documentation.

The active NeuralBase documentation describes capabilities by behavior rather than by the order in which they were implemented. Historical files here may therefore retain former phase labels, pull-request numbers, workflow run identifiers, baseline commits, and closure wording when those identifiers are necessary to understand the original evidence.

Current authoritative documents include:

- [`../MANAGED_MEMBERSHIP.md`](../MANAGED_MEMBERSHIP.md) for managed membership reconciliation;
- [`../PITR.md`](../PITR.md) and [`../PITR_CORRECTNESS_MODEL.md`](../PITR_CORRECTNESS_MODEL.md) for exact committed-index archived recovery;
- [`../PERFORMANCE_PROFILE.md`](../PERFORMANCE_PROFILE.md) for query-path performance evidence;
- [`../TESTING.md`](../TESTING.md) and the repository [`CONFIDENCE.yaml`](../../CONFIDENCE.yaml) for current executable evidence boundaries.

Do not use a historical closure file to infer a stronger current claim than the active confidence model and executable tests support.
