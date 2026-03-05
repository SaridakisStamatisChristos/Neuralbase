// SPDX-License-Identifier: Apache-2.0
// Telemetry bootstrap — metrics + tracing initialisation.
//
// Exposes a Prometheus /metrics scrape endpoint on `metrics_port` when
// the metrics-exporter-prometheus crate is compiled with its http-listener
// feature (requires enabling the default features in Cargo.toml).
//
// Current build: default-features = false → scrape endpoint disabled.
// To enable a live scrape endpoint, add `features = ["http-listener"]` to
// the metrics-exporter-prometheus dep and uncomment the PrometheusBuilder
// block below.
//
// Tracing (tokio-tracing / OpenTelemetry) is not wired in this release;
// structured logs are emitted via eprintln! in the hot path.
//
// CONFIDENCE: raw=0.70 effective=0.68

/// Initialise metrics recorder.
///
/// When the `http-listener` feature is enabled this spawns a background
/// server on `0.0.0.0:<metrics_port>` exposing `/metrics`.
/// In the current build this is a no-op.
pub fn init(metrics_port: u16) {
    // Suppress unused-variable warning when feature is off.
    let _ = metrics_port;

    // Uncomment when http-listener feature is enabled:
    // use metrics_exporter_prometheus::PrometheusBuilder;
    // PrometheusBuilder::new()
    //     .with_http_listener(([0, 0, 0, 0], metrics_port))
    //     .install()
    //     .expect("failed to install Prometheus recorder");
    // eprintln!("[telemetry] Prometheus scrape: http://0.0.0.0:{metrics_port}/metrics");
}
