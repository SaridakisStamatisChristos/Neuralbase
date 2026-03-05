# Confidence Report (Session 2)

## Strongest guarantees
- Vectorized in-memory execution is active for scan/filter/project and TPC-H Q1/Q6 paths.
- SIMD feature path and scalar path both compile and pass equivalence-style tests.
- Morsel-based parallel filtering executes without deadlock in non-multiple batch sizes.
- TPC-H Q1/Q6 baselines are recorded in `tests/perf/BENCH_BASELINES.yaml`.

## Weakest guarantees
- Hash join and sort-merge join are implemented but have narrower verification depth than scan/aggregate paths.
- Storage remains in-memory only; no persistence or MVCC guarantees yet.
- AVX-512 intrinsic acceleration is not active on stable Rust 1.84, so the `simd` path currently uses scalar fallback.

## Uncertainty propagation
- `vectorized_join_paths` is currently the weakest link and bounds system effective confidence.
- Executor confidence is bounded by upstream planner, SIMD, and scheduler confidence values.
- System confidence is bounded by weakest-link propagation, not by mean score.

## Fastest confidence improvements
1. Expand adversarial and correctness verification depth for hash join and sort-merge join.
2. Add larger-scale benchmark runs and regression thresholds tied to hardware fingerprints.
3. Integrate persistent storage with the same vectorized operator contracts.

## Do not rely on
- Durability guarantees
- Distributed fault tolerance
- MVCC snapshot semantics
- Production-grade authentication/TLS
