# Performance benchmarks

NeuralBase keeps benchmark code and historical measurements for regression-oriented engineering. Benchmarks are **not** part of the normal CI correctness gate and should not be quoted without their exact workload and hardware context.

## Commands

```bash
make bench
make bench-full
```

`make bench` runs the checked-in release-mode TPC-H execution benchmarks and optimizer benchmark. `make bench-full` additionally enables the heavier ignored scale-factor runs.

## Recorded baseline scope

`BENCH_BASELINES.yaml` currently records historical measurements for:

- vectorized TPC-H Q1 at scale factor 0.1;
- vectorized TPC-H Q6 at scale factor 0.1;
- optimizer join-order cost comparisons over the repository's Q1-Q22 join graphs.

The optimizer percentage is a **repository cost-model comparison**, not a wall-clock claim that NeuralBase is faster than PostgreSQL, DuckDB, or another database.

The optimizer benchmark invokes `optimizer::RlOptimizer` directly using `optimizer/model/neuralbase_optimizer.onnx`. The model uses the eight TPC-H table identities and falls back to naive order for unknown tables, invalid graphs or inference failure/timeout. The server query planner does not invoke this optimizer, and the Docker runtime does not include the model file. Benchmark improvements therefore do not imply live SQL endpoint speedups.

## Reproducibility rule

A benchmark result is only meaningful with:

- exact commit/model artifact;
- command and Cargo profile/features;
- dataset/scale factor;
- hardware/runtime environment;
- repetition/warmup methodology.

Re-measure before using old development-laptop values as current performance claims.
