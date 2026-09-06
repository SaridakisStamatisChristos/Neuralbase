# Observability

NeuralBase exposes a Prometheus-compatible metrics endpoint and emits runtime diagnostics to standard error.

## Metrics endpoint

`src/telemetry.rs` installs `metrics-exporter-prometheus` with an HTTP listener on:

```text
0.0.0.0:${NEURALBASE_METRICS_PORT}/metrics
```

The default metrics port is `9090` unless configuration overrides it.

The repository includes `ops/prometheus.yml` and the Compose topology includes Prometheus/Grafana-oriented development wiring.

## Failure behavior

Failure to install the Prometheus recorder is currently non-fatal; the SQL process continues and prints a warning. A common cause is a metrics-port collision when multiple local processes use the same port.

## Tracing status

The crate includes tracing dependencies, but `src/telemetry.rs` does not currently wire a complete distributed tracing/OpenTelemetry export pipeline. Do not infer end-to-end tracing from the dependency list alone.

## Operational note

Metrics are development/diagnostic evidence, not a complete production SLO/alerting package. Production use would still require documented dashboards, alerts, cardinality controls, retention, and incident procedures.
