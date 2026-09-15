// SPDX-License-Identifier: Apache-2.0
//! Focused PostgreSQL-wire transport probe for the suspected Nagle/delayed-ACK stall.
//!
//! The companion workflow runs this identical harness twice on the same runner
//! from the same source commit: first unchanged, then with exactly one ephemeral
//! server-side `TcpStream::set_nodelay(true)` edit. No timing threshold is asserted.

use std::collections::HashSet;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};
use serde_json::json;
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(15);
const WARMUP: usize = 5;
const REPS: usize = 50;

struct NodeProcess {
    child: Child,
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn variant() -> String {
    std::env::var("TCP_NODELAY_VARIANT").unwrap_or_else(|_| "unknown".into())
}

fn measured_commit() -> String {
    std::env::var("TCP_NODELAY_BASE_SHA").unwrap_or_else(|_| "unknown".into())
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
        "variant": variant(),
        "commit": measured_commit(),
        "name": name,
        "path": "real_neuralbase_postgres_wire_same_runner_ab",
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
    println!("TCP_NODELAY_RESULT {payload}");
}

fn connect(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1"),
        NoTls,
    )
}

fn start_node(root: &TempDir, sql_port: u16, raft_port: u16, metrics_port: u16) -> NodeProcess {
    let child = Command::new(env!("CARGO_BIN_EXE_neuralbase"))
        .env("NEURALBASE_NODE_ID", "tcp-nodelay-probe")
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
        .expect("start NeuralBase probe node");
    NodeProcess { child }
}

fn wait_ready(process: &mut NodeProcess, port: u16) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(
            process.child.try_wait().expect("query child").is_none(),
            "probe node exited during startup"
        );
        if connect(port).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "probe startup timed out");
        std::thread::sleep(Duration::from_millis(30));
    }
}

fn is_leadership_not_ready(error: &postgres::Error) -> bool {
    error
        .as_db_error()
        .is_some_and(|db| matches!(db.code().code(), "25006" | "57P03"))
}

fn execute_setup_mutation_when_leader(port: u16, sql: &str, description: &str) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        let mut client = match connect(port) {
            Ok(client) => client,
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "endpoint did not accept setup connection while {description}: {error}"
                );
                std::thread::sleep(Duration::from_millis(30));
                continue;
            }
        };

        match client.simple_query(sql) {
            Ok(_) => return,
            Err(error) if is_leadership_not_ready(&error) => {
                assert!(
                    Instant::now() < deadline,
                    "endpoint never became mutation-ready while {description}: {error}"
                );
                std::thread::sleep(Duration::from_millis(30));
            }
            Err(error) => panic!(
                "{description} failed after submission; outcome is uncertain and must not be retried: {error}"
            ),
        }
    }
}

fn row_count(messages: &[SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|message| matches!(message, SimpleQueryMessage::Row(_)))
        .count()
}

fn measure_simple(client: &mut Client, name: &str, sql: &str, expected_rows: Option<usize>) {
    for _ in 0..WARMUP {
        let messages = client.simple_query(sql).expect("probe warmup");
        if let Some(expected) = expected_rows {
            assert_eq!(row_count(&messages), expected);
        }
    }

    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let start = Instant::now();
        let messages = client.simple_query(sql).expect("probe measurement");
        samples.push(start.elapsed().as_nanos());
        if let Some(expected) = expected_rows {
            assert_eq!(row_count(&messages), expected);
        }
    }

    report(
        name,
        &samples,
        json!({"topology": "single_voter_real_process"}),
    );
}

#[test]
#[ignore = "focused TCP_NODELAY transport characterization"]
fn tcp_nodelay_endpoint_probe() {
    let root = TempDir::new().expect("probe tempdir");
    let mut used = HashSet::new();
    let sql_port = reserve_port(&mut used);
    let raft_port = reserve_port(&mut used);
    let metrics_port = reserve_port(&mut used);
    let mut process = start_node(&root, sql_port, raft_port, metrics_port);
    wait_ready(&mut process, sql_port);

    execute_setup_mutation_when_leader(
        sql_port,
        "CREATE TABLE tcp_probe_items (id BIGINT, name TEXT)",
        "create probe table",
    );
    execute_setup_mutation_when_leader(
        sql_port,
        "INSERT INTO tcp_probe_items VALUES (1, 'baseline')",
        "seed probe table",
    );

    // The measured connection is created only after setup, so every timed sample
    // is steady-state protocol traffic rather than connection establishment.
    let mut client = connect(sql_port).expect("connect measurement client");

    measure_simple(
        &mut client,
        "set_local_roundtrip",
        "SET neuralbase_read_consistency = 'local'",
        Some(0),
    );

    measure_simple(
        &mut client,
        "persistent_local_select",
        "SELECT id, name FROM tcp_probe_items WHERE id = 1 ORDER BY id LIMIT 1",
        Some(1),
    );

    measure_simple(
        &mut client,
        "persistent_general_self_join",
        "SELECT a.id, a.name FROM tcp_probe_items a JOIN tcp_probe_items b ON a.id = b.id WHERE a.id = 1",
        Some(1),
    );

    client
        .simple_query("SET neuralbase_read_consistency = 'leader'")
        .expect("set leader consistency");
    measure_simple(
        &mut client,
        "persistent_leader_select",
        "SELECT id, name FROM tcp_probe_items WHERE id = 1 ORDER BY id LIMIT 1",
        Some(1),
    );

    client
        .simple_query("SET neuralbase_read_consistency = 'linearizable'")
        .expect("set linearizable consistency");
    measure_simple(
        &mut client,
        "persistent_linearizable_select",
        "SELECT id, name FROM tcp_probe_items WHERE id = 1 ORDER BY id LIMIT 1",
        Some(1),
    );

    client
        .simple_query("SET neuralbase_read_consistency = 'local'")
        .expect("restore local consistency");
    for i in 0..WARMUP {
        client
            .simple_query(&format!(
                "INSERT INTO tcp_probe_items VALUES ({}, 'warmup')",
                10_000 + i
            ))
            .expect("insert warmup");
    }

    let mut insert_samples = Vec::with_capacity(REPS);
    for i in 0..REPS {
        let sql = format!(
            "INSERT INTO tcp_probe_items VALUES ({}, 'measured')",
            20_000 + i
        );
        let start = Instant::now();
        client.simple_query(&sql).expect("measured insert");
        insert_samples.push(start.elapsed().as_nanos());
    }
    report(
        "replicated_insert",
        &insert_samples,
        json!({
            "topology": "single_voter_real_process",
            "durability": "replicated_gateway_confirmed_local_apply",
            "pitr_enabled": false
        }),
    );
}
