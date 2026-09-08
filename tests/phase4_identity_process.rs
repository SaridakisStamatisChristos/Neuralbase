// SPDX-License-Identifier: Apache-2.0
//! Process-boundary Phase-4 identity validation.
//!
//! Three real NeuralBase binaries use distinct SQL/Raft ports and RocksDB paths.
//! The test bootstraps identity through SQL, restarts with authentication required,
//! verifies credentials on every node, rotates a password, kills that leader,
//! mutates identity on the successor, restarts the killed process, and checks
//! cluster-wide authentication convergence.

use std::collections::HashSet;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(12);
const LEADER_TIMEOUT: Duration = Duration::from_secs(12);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(12);
const QUERY_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Clone, Debug)]
struct NodeSpec {
    id: String,
    sql_port: u16,
    raft_port: u16,
    metrics_port: u16,
    db_path: PathBuf,
    users_file: PathBuf,
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

    fn start(&mut self, auth_required: bool) {
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
            .env("NEURALBASE_USERS_FILE", &self.spec.users_file)
            .env(
                "NEURALBASE_METRICS_PORT",
                self.spec.metrics_port.to_string(),
            )
            .env(
                "NEURALBASE_RAFT_ELECTION_TIMEOUT_MS",
                self.spec.election_timeout_ms.to_string(),
            )
            .env(
                "NEURALBASE_AUTH_REQUIRED",
                if auth_required { "1" } else { "0" },
            )
            .env("NEURALBASE_RAFT_TLS", "0")
            .env_remove("NEURALBASE_IDENTITY_MIGRATION_SHA256")
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
            let id = format!("identity_proc_node{}", i + 1);
            let peers = (0..3)
                .filter(|peer| *peer != i)
                .map(|peer| {
                    format!(
                        "identity_proc_node{}=127.0.0.1:{}",
                        peer + 1,
                        raft_ports[peer]
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            NodeSpec {
                id,
                sql_port: sql_ports[i],
                raft_port: raft_ports[i],
                metrics_port: metrics_ports[i],
                db_path: root.path().join(format!("node{}", i + 1)),
                users_file: root.path().join(format!("legacy-users-node{}.json", i + 1)),
                peers,
                election_timeout_ms: 150 + (i as u64 * 80),
            }
        })
        .collect()
}

fn connect_no_auth(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1"),
        NoTls,
    )
}

fn connect_auth(port: u16, user: &str, password: &str) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!(
            "host=127.0.0.1 port={port} user={user} password={password} dbname=postgres connect_timeout=1"
        ),
        NoTls,
    )
}

fn wait_no_auth_ready(node: &mut NodeProcess) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited during startup", node.spec.id);
        if connect_no_auth(node.spec.sql_port).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "{} SQL port did not become ready", node.spec.id);
        thread::sleep(Duration::from_millis(30));
    }
}

fn wait_auth_ready(node: &mut NodeProcess, user: &str, password: &str) {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited before auth convergence", node.spec.id);
        if connect_auth(node.spec.sql_port, user, password).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} did not accept replicated credential for {user}",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(40));
    }
}

fn wait_auth_rejected(node: &mut NodeProcess, user: &str, password: &str) {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited before auth rejection converged", node.spec.id);
        if connect_auth(node.spec.sql_port, user, password).is_err() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} continued accepting stale credential for {user}",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(40));
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

fn mutation_attempt(port: u16, auth: Option<(&str, &str)>, sql: &str) -> MutationAttempt {
    let sql = sql.to_string();
    let auth = auth.map(|(user, password)| (user.to_string(), password.to_string()));
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let _worker = thread::spawn(move || {
        let connection = match auth {
            Some((user, password)) => connect_auth(port, &user, &password),
            None => connect_no_auth(port),
        };
        let outcome = match connection {
            Err(_) => MutationAttempt::RetryablePreSubmit,
            Ok(mut client) => match client.simple_query(&sql) {
                Ok(_) => MutationAttempt::Success,
                Err(error) if is_follower_error(&error) => MutationAttempt::RetryablePreSubmit,
                Err(error) => MutationAttempt::Fatal(error.to_string()),
            },
        };
        let _ = result_tx.send(outcome);
    });

    result_rx
        .recv_timeout(QUERY_TIMEOUT)
        .unwrap_or(MutationAttempt::TimedOut)
}

fn mutate_on_leader(
    nodes: &mut [NodeProcess],
    auth: Option<(&str, &str)>,
    sql: &str,
) -> usize {
    let deadline = Instant::now() + LEADER_TIMEOUT;
    loop {
        for (index, node) in nodes.iter_mut().enumerate() {
            if !node.is_running() {
                continue;
            }
            match mutation_attempt(node.spec.sql_port, auth, sql) {
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
        assert!(Instant::now() < deadline, "no Raft leader accepted mutation: {sql}");
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn process_identity_converges_across_auth_restart_rotation_failover_and_rejoin() {
    let root = TempDir::new().expect("cluster tempdir");
    let specs = build_specs(&root);
    let mut nodes: Vec<NodeProcess> = specs.into_iter().map(NodeProcess::new).collect();

    for node in &mut nodes {
        node.start(false);
    }
    for node in &mut nodes {
        wait_no_auth_ready(node);
    }

    mutate_on_leader(
        &mut nodes,
        None,
        "CREATE USER alice WITH PASSWORD 'secret-one'",
    );

    // Re-open the whole cluster with authentication required. Each process must
    // authenticate from replicated RocksDB state; the configured legacy paths do
    // not exist and cannot act as local credential mirrors.
    for node in &mut nodes {
        node.kill();
    }
    for node in &mut nodes {
        node.start(true);
    }
    for node in &mut nodes {
        wait_auth_ready(node, "alice", "secret-one");
    }

    let rotation_leader = mutate_on_leader(
        &mut nodes,
        Some(("alice", "secret-one")),
        "ALTER USER alice WITH PASSWORD 'secret-two'",
    );
    for node in &mut nodes {
        wait_auth_ready(node, "alice", "secret-two");
        wait_auth_rejected(node, "alice", "secret-one");
    }

    // Kill the leader that acknowledged the rotation. The surviving quorum must
    // retain the new credential and accept further identity DDL on its successor.
    nodes[rotation_leader].kill();
    mutate_on_leader(
        &mut nodes,
        Some(("alice", "secret-two")),
        "CREATE USER bob WITH PASSWORD 'bob-secret'",
    );
    for (index, node) in nodes.iter_mut().enumerate() {
        if index != rotation_leader {
            wait_auth_ready(node, "bob", "bob-secret");
        }
    }

    // Rejoin the killed process from its existing disk. Retained-log catch-up (or
    // snapshot install if compaction races this test) must supply Bob's identity.
    nodes[rotation_leader].start(true);
    wait_auth_ready(&mut nodes[rotation_leader], "bob", "bob-secret");
    for node in &mut nodes {
        wait_auth_ready(node, "alice", "secret-two");
        wait_auth_ready(node, "bob", "bob-secret");
    }

    mutate_on_leader(
        &mut nodes,
        Some(("bob", "bob-secret")),
        "DROP USER alice",
    );
    for node in &mut nodes {
        wait_auth_rejected(node, "alice", "secret-two");
        wait_auth_ready(node, "bob", "bob-secret");
    }
}
