// SPDX-License-Identifier: Apache-2.0
//! Phase-10 real PostgreSQL-wire endpoint characterization.
//!
//! The library executor microbenchmarks deliberately identify individual paths,
//! but Phase 10 also needs evidence from the server route that users actually
//! exercise. This ignored test starts the real `neuralbase` binary as a durable
//! single-voter node and measures repeated user-table SELECTs under Local,
//! Leader and Linearizable consistency plus replicated INSERT latency.
//!
//! No timing threshold is asserted. Results are emitted as `PHASE10_RESULT`
//! JSON lines for exact-base/head comparison by the Phase-10 workflow.

use std::collections::HashSet;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};
use serde_json::json;
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(15);
const WARMUP: usize = 2;
const REPS: usize = 9;

struct NodeProcess {
    child: Child,
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn label() -> String {
    std::env::var("PHASE10_LABEL").unwrap_or_else(|_| "unlabeled".into())
}

fn measured_commit() -> String {
    std::env::var("PHASE10_MEASURED_COMMIT").unwrap_or_else(|_| "unknown".into())
}

fn reserve_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().expect("ephemeral address").port();
        drop(listener);
        if used.insert(port) {
            return port;
        }
    }
}

fn percentile(sorted: &[u128], percentile: f64) -> u128 {
    let idx = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[idx]
}

fn report(name: &str, samples: &[u128], extra: serde_json::Value) {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let mean = sorted.iter().copied().sum::<u128>() as f64 / sorted.len() as f64;
    let payload = json!({
        "schema": 1,
        "label": label(),
        "commit": measured_commit(),
        "name": name,
        "path": "postgres_wire_real_neuralbase_process",
        "unit": "ns",
        "warmup_iterations": WARMUP,
        "measured_iterations": samples.len(),
        "min": sorted[0],
        "p50": percentile(&sorted, 0.50),
        "p95": percentile(&sorted, 0.95),
        "max": sorted[sorted.len() - 1],
        "mean": mean,
        "extra": extra,
    });
    println!("PHASE10_RESULT {payload}");
}

fn connect(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!(
            "host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1"
        ),
        NoTls,
    )
}

fn start_node(root: &TempDir, sql_port: u16, raft_port: u16, metrics_port: u16) -> NodeProcess {
    let child = Command::new(env!("CARGO_BIN_EXE_neuralbase"))
        .env("NEURALBASE_NODE_ID", "phase10-endpoint")
        .env("NEURALBASE_LISTEN_ADDR", format!("127.0.0.1:{sql_port}"))
        .env("NEURALBASE_RAFT_ADDR", format!("127.0.0.1:{raft_port}"))
        .env("NEURALBASE_PEERS", "")
        .env("NEURALBASE_DB_PATH", root.path().join("db"))
        .env("NEURALBASE_USERS_FILE", root.path().join("users.json"))
        .env("NEURALBASE_METRICS_PORT", metrics_port.to_string())
        .env("NEURALBASE_RAFT_ELECTION_TIMEOUT_MS", "80")
        .env("NEURALBASE_AUTH_REQUIRED", "0")
        .env("NEURALBASE_RAFT_TLS", "0")
        .env_remove("NEURALBASE_PITR_ARCHIVE_DIR")
        .env_remove("NEURALBASE_PITR_KEY_FILE")
        .env_remove("NEURALBASE_PITR_MAX_SEGMENTS")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start neuralbase endpoint benchmark node");
    NodeProcess { child }
}

fn wait_ready(process: &mut NodeProcess, port: u16) -> Client {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(
            process.child.try_wait().expect("query child").is_none(),
            "phase10 endpoint node exited during startup"
        );
        if let Ok(client) = connect(port) {
            return client;
        }
        assert!(Instant::now() < deadline, "phase10 endpoint startup timed out");
        std::thread::sleep(Duration::from_millis(30));
    }
}

fn row_count(messages: &[SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|message| matches!(message, SimpleQueryMessage::Row(_)))
        .count()
}

fn measure_query(client: &mut Client, name: &str, consistency: &str) {
    let set = format!("SET neuralbase_read_consistency = '{consistency}'");
    client.simple_query(&set).expect("set read consistency");
    const SQL: &str =
        "SELECT id, name FROM phase10_endpoint_items WHERE id = 1 ORDER BY id LIMIT 1";
    for _ in 0..WARMUP {
        let messages = client.simple_query(SQL).expect("endpoint read warmup");
        assert_eq!(row_count(&messages), 1);
    }
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let messages = client.simple_query(SQL).expect("endpoint measured read");
        samples.push(start.elapsed().as_nanos());
        assert_eq!(row_count(&messages), 1);
    }
    report(
        name,
        &samples,
        json!({
            "consistency": consistency,
            "topology": "single_voter_real_process",
            "query_kind": "persistent_user_table_select",
            "includes_parse_bind_route_execute_wire": true
        }),
    );
}

#[test]
#[ignore = "manual Phase-10 performance characterization"]
fn phase10_real_endpoint_characterization() {
    let root = TempDir::new().expect("endpoint tempdir");
    let mut used = HashSet::new();
    let sql_port = reserve_port(&mut used);
    let raft_port = reserve_port(&mut used);
    let metrics_port = reserve_port(&mut used);
    let mut process = start_node(&root, sql_port, raft_port, metrics_port);
    let mut client = wait_ready(&mut process, sql_port);

    // Retry mutations only while the single voter is still becoming the serving
    // leader. Once a statement succeeds, all timed operations use the same
    // established connection and no ambiguous retry is performed.
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        match client.simple_query(
            "CREATE TABLE phase10_endpoint_items (id BIGINT, name TEXT)",
        ) {
            Ok(_) => break,
            Err(error)
                if error
                    .as_db_error()
                    .is_some_and(|db| matches!(db.code().code(), "25006" | "57P03")) =>
            {
                assert!(Instant::now() < deadline, "endpoint never became mutation-ready");
                std::thread::sleep(Duration::from_millis(30));
            }
            Err(error) => panic!("create benchmark table: {error}"),
        }
    }
    client
        .simple_query("INSERT INTO phase10_endpoint_items VALUES (1, 'baseline')")
        .expect("seed benchmark table");

    measure_query(&mut client, "endpoint_local_read", "local");
    measure_query(&mut client, "endpoint_leader_read", "leader");
    measure_query(
        &mut client,
        "endpoint_linearizable_read",
        "linearizable",
    );

    // Return to Local so the measured INSERT is not preceded by a read barrier;
    // mutation acknowledgement itself still retains quorum + confirmed durable
    // local apply semantics.
    client
        .simple_query("SET neuralbase_read_consistency = 'local'")
        .expect("restore local consistency");
    for i in 0..WARMUP {
        client
            .simple_query(&format!(
                "INSERT INTO phase10_endpoint_items VALUES ({}, 'warmup')",
                10_000 + i
            ))
            .expect("write warmup");
    }
    let mut write_samples = Vec::with_capacity(REPS);
    for i in 0..REPS {
        let sql = format!(
            "INSERT INTO phase10_endpoint_items VALUES ({}, 'measured')",
            20_000 + i
        );
        let start = Instant::now();
        client.simple_query(&sql).expect("measured endpoint insert");
        write_samples.push(start.elapsed().as_nanos());
    }
    report(
        "endpoint_replicated_insert",
        &write_samples,
        json!({
            "topology": "single_voter_real_process",
            "includes_parse_bind_raft_durable_apply_wire": true,
            "pitr_enabled": false
        }),
    );
}
