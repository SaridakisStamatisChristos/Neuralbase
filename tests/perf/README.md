# Performance Benchmarks

## Command

```bash
make bench
```

## Scope

- TPC-H Q1 baseline at scale factor 0.1
- TPC-H Q6 baseline at scale factor 0.1

## Notes

- Session 2 records deterministic local baselines for regression tracking.
- CI runs `make bench` and keeps benchmark artifacts in version control.
