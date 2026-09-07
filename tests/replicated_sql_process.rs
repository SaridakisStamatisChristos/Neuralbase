// SPDX-License-Identifier: Apache-2.0
//! Process-boundary replicated SQL validation.
//!
//! This test intentionally does not use ChannelTransport or shared storage. It
//! starts three real `neuralbase` binaries with distinct PostgreSQL/Raft ports,
//! metrics ports, and RocksDB directories, then exercises table mutations over
//! the PostgreSQL wire protocol across leader loss and full-cluster restart.

use std::collections::{BTreeSet, HashSet};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(10);
const LEADER_TIMEOUT: Duration = Duration::from_secs(10);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(10);
const QUERY_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug)]
struct NodeSpec {
    id: String,
    sql_port: u16,
    raft_port: u16,
    metrics_port: u16,
    db_path: PathBuf,
    peers: String,
    election_timeout_ms: u64,
}

struct NodeProcess {
    spec: NodeSpec,
    child: Option<Child>,
}

impl NodeProcess {
    fn new(spec: NodeSpec) -> Self {
        Self { spec, child: None }
    }

    fn start(&mut self) {
        assert!(self.child.is_none(), "{} already running", self.spec.id);
        let child = Command::new(env!("CARGO_BIN_EXE_neuralbase"))
            .env("NEURALBASE_NODE_ID", &self.spec.id)
            .env(
                "NEURALBASE_LISTEN_ADDR",
                format!("127.0.0.1:{}", self.spec.sql_port),
            )
            .env(
                "NEURALBASE_RAFT_ADDR",
                format!("127.0.0.1:{}", self.spec.raft_port),
            )
            .env("NEURALBASE_PEERS", &self.spec.peers)
            .env("NEURALBASE_DB_PATH", &self.spec.db_path)
            .env(
                "NEURALBASE_METRICS_PORT",
                self.spec.metrics_port.to_string(),
            )
            .env(
                "NEURALBASE_RAFT_ELECTION_TIMEOUT_MS",
                self.spec.election_timeout_ms.to_string(),
            )
            .env("NEURALBASE_AUTH_REQUIRED", "0")
            .env("NEURALBASE_RAFT_TLS", "0")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|error| panic!("start {}: {error}", self.spec.id));
        self.child = Some(child);
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn is_running(&mut self) -> bool {
        match self.child.as_mut() {
            Some(child) => child.try_wait().expect("query child status").is_none(),
            None => false,
        }
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        self.kill();
    }
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

fn build_specs(root: &TempDir) -> Vec<NodeSpec> {
    let mut used = HashSet::new();
    let mut sql_ports = Vec::new();
    let mut raft_ports = Vec::new();
    let mut metrics_ports = Vec::new();
    for _ in 0..3 {
        sql_ports.push(reserve_port(&mut used));
        raft_ports.push(reserve_port(&mut used));
        metrics_ports.push(reserve_port(&mut used));
    }

    (0..3)
        .map(|i| {
            let id = format!("proc_node{}", i + 1);
            let peers = (0..3)
                .filter(|peer| *peer != i)
                .map(|peer| format!("proc_node{}=127.0.0.1:{}", peer + 1, raft_ports[peer]))
                .collect::<Vec<_>>()
                .join(",");
            NodeSpec {
                id,
                sql_port: sql_ports[i],
                raft_port: raft_ports[i],
                metrics_port: metrics_ports[i],
                db_path: root.path().join(format!("node{}", i + 1)),
                peers,
                election_timeout_ms: 140 + (i as u64 * 80),
            }
        })
        .collect()
}

fn connect(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1"),
        NoTls,
    )
}

fn wait_sql_ready(node: &mut NodeProcess) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited during startup", node.spec.id);
        if connect(node.spec.sql_port).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} SQL port did not become ready",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn is_follower_error(error: &postgres::Error) -> bool {
    error
        .as_db_error()
        .is_some_and(|db| db.code().code() == "25006")
}

enum MutationAttempt {
    Success,
    RetryablePreSubmit,
    Fatal(String),
    TimedOut,
}

fn mutation_attempt(port: u16, sql: &str) -> MutationAttempt {
    let sql = sql.to_string();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let _worker = thread::spawn(move || {
        let outcome = match connect(port) {
            // No query was submitted, so trying another node is safe.
            Err(_) => MutationAttempt::RetryablePreSubmit,
            Ok(mut client) => match client.simple_query(&sql) {
                Ok(_) => MutationAttempt::Success,
                // Explicit follower rejection occurs before proposal and is safe
                // to redirect to the known/current leader.
                Err(error) if is_follower_error(&error) => MutationAttempt::RetryablePreSubmit,
                // Any error after submitting to a leader is conservatively
                // outcome-uncertain. Never replay a non-idempotent INSERT here.
                Err(error) => MutationAttempt::Fatal(error.to_string()),
            },
        };
        let _ = result_tx.send(outcome);
    });

    result_rx
        .recv_timeout(QUERY_TIMEOUT)
        .unwrap_or(MutationAttempt::TimedOut)
}

fn mutate_on_leader(nodes: &mut [NodeProcess], sql: &str) -> usize {
    let deadline = Instant::now() + LEADER_TIMEOUT;
    loop {
        for (index, node) in nodes.iter_mut().enumerate() {
            if !node.is_running() {
                continue;
            }
            match mutation_attempt(node.spec.sql_port, sql) {
                MutationAttempt::Success => return index,
                MutationAttempt::RetryablePreSubmit => {}
                MutationAttempt::Fatal(error) => panic!(
                    "mutation on {} failed after submission; outcome is uncertain and MUST NOT be retried: {error}; SQL={sql}",
                    node.spec.id
                ),
                MutationAttempt::TimedOut => panic!(
                    "mutation on {} exceeded {:?}; outcome is ambiguous and MUST NOT be retried: {sql}",
                    node.spec.id, QUERY_TIMEOUT
                ),
            }
        }
        assert!(
            Instant::now() < deadline,
            "no Raft leader accepted mutation before deadline: {sql}"
        );
        thread::sleep(Duration::from_millis(40));
    }
}

fn read_rows_blocking(port: u16) -> Result<BTreeSet<(String, String)>, String> {
    let mut client = connect(port).map_err(|error| error.to_string())?;
    let messages = client
        .simple_query("SELECT id, name FROM replicated_items")
        .map_err(|error| error.to_string())?;
    let mut rows = BTreeSet::new();
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            rows.insert((
                row.get(0).unwrap_or_default().to_string(),
                row.get(1).unwrap_or_default().to_string(),
            ));
        }
    }
    Ok(rows)
}

fn read_rows(port: u16) -> Result<BTreeSet<(String, String)>, String> {
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let _worker = thread::spawn(move || {
        let _ = result_tx.send(read_rows_blocking(port));
    });
    result_rx
        .recv_timeout(QUERY_TIMEOUT)
        .map_err(|_| format!("read query exceeded {QUERY_TIMEOUT:?}"))?
}

fn wait_rows(node: &mut NodeProcess, expected: &BTreeSet<(String, String)>) {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        assert!(
            node.is_running(),
            "{} exited before convergence",
            node.spec.id
        );
        let last_read = match read_rows(node.spec.sql_port) {
            Ok(rows) if &rows == expected => return,
            Ok(rows) => format!("rows={rows:?}"),
            Err(error) => format!("error={error}"),
        };
        assert!(
            Instant::now() < deadline,
            "{} did not converge to rows {:?}; last read={}",
            node.spec.id,
            expected,
            last_read
        );
        thread::sleep(Duration::from_millis(40));
    }
}

fn wait_all_rows(nodes: &mut [NodeProcess], expected: &BTreeSet<(String, String)>) {
    for node in nodes.iter_mut().filter(|node| node.child.is_some()) {
        wait_rows(node, expected);
    }
}

#[test]
fn process_cluster_mutations_survive_failover_and_restart() {
    let root = TempDir::new().expect("cluster tempdir");
    let specs = build_specs(&root);
    let mut nodes: Vec<NodeProcess> = specs.into_iter().map(NodeProcess::new).collect();

    for node in &mut nodes {
        node.start();
    }
    for node in &mut nodes {
        wait_sql_ready(node);
    }

    mutate_on_leader(
        &mut nodes,
        "CREATE TABLE replicated_items (id BIGINT, name TEXT)",
    );
    mutate_on_leader(
        &mut nodes,
        "INSERT INTO replicated_items (id, name) VALUES (1, 'alpha'), (2, 'beta')",
    );
    mutate_on_leader(
        &mut nodes,
        "UPDATE replicated_items SET name = 'updated' WHERE id = 1",
    );
    let delete_leader = mutate_on_leader(&mut nodes, "DELETE FROM replicated_items WHERE id = 2");

    let expected_before_failover = BTreeSet::from([("1".to_string(), "updated".to_string())]);
    wait_all_rows(&mut nodes, &expected_before_failover);

    let killed = delete_leader;
    nodes[killed].kill();

    let new_leader = mutate_on_leader(
        &mut nodes,
        "UPDATE replicated_items SET name = 'after_failover' WHERE id = 1",
    );
    assert_ne!(new_leader, killed, "dead leader cannot acknowledge a write");

    let expected_after_failover = BTreeSet::from([("1".to_string(), "after_failover".to_string())]);
    for (index, node) in nodes.iter_mut().enumerate() {
        if index != killed {
            wait_rows(node, &expected_after_failover);
        }
    }

    nodes[killed].start();
    wait_sql_ready(&mut nodes[killed]);
    wait_rows(&mut nodes[killed], &expected_after_failover);

    for node in &mut nodes {
        node.kill();
    }
    for node in &mut nodes {
        node.start();
    }
    for node in &mut nodes {
        wait_sql_ready(node);
    }

    let crash_leader = mutate_on_leader(
        &mut nodes,
        "UPDATE replicated_items SET name = 'after_restart' WHERE id = 1",
    );
    let expected_after_restart = BTreeSet::from([("1".to_string(), "after_restart".to_string())]);
    wait_all_rows(&mut nodes, &expected_after_restart);

    // Race a real PostgreSQL write against SIGKILL of the current leader. A
    // failed client request has an intentionally uncertain outcome. The strong
    // contract is one-way: if the client observed success, that exact effect
    // must remain visible on the surviving quorum after leader death.
    let crash_port = nodes[crash_leader].spec.sql_port;
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let writer = thread::spawn(move || {
        let outcome = (|| -> Result<(), String> {
            let mut client = connect(crash_port).map_err(|error| error.to_string())?;
            ready_tx
                .send(())
                .map_err(|error| format!("signal crash-write readiness: {error}"))?;
            client
                .simple_query("UPDATE replicated_items SET name = 'crash_candidate' WHERE id = 1")
                .map(|_| ())
                .map_err(|error| error.to_string())
        })();
        let _ = result_tx.send(outcome);
    });

    ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("crash-write client did not become ready");
    thread::sleep(Duration::from_millis(2));
    nodes[crash_leader].kill();
    let crash_write = result_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("crash-write client did not unblock after leader kill");
    writer.join().expect("crash-write thread panicked");

    if crash_write.is_ok() {
        // A new-term, logically empty DELETE establishes a Raft commit/apply
        // barrier without changing the row being checked. This is necessary
        // because follower reads are local and may not have learned the dead
        // leader's final commit_index before the kill.
        mutate_on_leader(
            &mut nodes,
            "DELETE FROM replicated_items WHERE id = -999999",
        );
        let acknowledged = BTreeSet::from([("1".to_string(), "crash_candidate".to_string())]);
        for (index, node) in nodes.iter_mut().enumerate() {
            if index != crash_leader {
                wait_rows(node, &acknowledged);
            }
        }
    }

    mutate_on_leader(
        &mut nodes,
        "UPDATE replicated_items SET name = 'after_crash_recovery' WHERE id = 1",
    );
    let expected_after_crash =
        BTreeSet::from([("1".to_string(), "after_crash_recovery".to_string())]);
    for (index, node) in nodes.iter_mut().enumerate() {
        if index != crash_leader {
            wait_rows(node, &expected_after_crash);
        }
    }

    nodes[crash_leader].start();
    wait_sql_ready(&mut nodes[crash_leader]);
    wait_rows(&mut nodes[crash_leader], &expected_after_crash);
}
