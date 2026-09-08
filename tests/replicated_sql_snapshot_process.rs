// SPDX-License-Identifier: Apache-2.0
//! Separate-process Phase 2 SQL snapshot/bootstrap proof.
//!
//! This test uses three real `neuralbase` child processes and TCP Raft. It
//! compacts one stopped member through the same SQL snapshot manager and staged
//! Raft persistence contract used by live compaction, deletes a different fixed
//! member's entire RocksDB directory, then proves that the same logical node ID
//! returns via InstallSnapshot + retained suffix before serving SQL.

use std::collections::{BTreeSet, HashSet};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use neuralbase::consensus::{RaftPersistenceStore, StagedSnapshot, StagedSnapshotKind};
use neuralbase::hlc::HlcClock;
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_snapshot_manager::ReplicatedSqlSnapshotManager;
use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;
use neuralbase::rocksdb_catalog::RocksDbCatalog;
use neuralbase::storage::StorageEngine;
use postgres::{Client, NoTls, SimpleQueryMessage};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(12);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(12);
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
            let id = format!("snapshot_proc_node{}", i + 1);
            let peers = (0..3)
                .filter(|peer| *peer != i)
                .map(|peer| {
                    format!(
                        "snapshot_proc_node{}=127.0.0.1:{}",
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
                peers,
                election_timeout_ms: 160 + i as u64 * 120,
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

fn ping(port: u16) -> bool {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let _worker = thread::spawn(move || {
        let ok = connect(port)
            .and_then(|mut client| client.simple_query("SELECT 1"))
            .is_ok();
        let _ = tx.send(ok);
    });
    rx.recv_timeout(QUERY_TIMEOUT).unwrap_or(false)
}

fn wait_sql_query_ready(node: &mut NodeProcess) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited during startup", node.spec.id);
        if ping(node.spec.sql_port) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} SQL service did not become query-ready",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(30));
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

fn mutate_on_leader(nodes: &mut [NodeProcess], sql: &str) -> usize {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        for (index, node) in nodes.iter_mut().enumerate() {
            if !node.is_running() {
                continue;
            }
            match mutation_attempt(node.spec.sql_port, sql) {
                MutationAttempt::Success => return index,
                MutationAttempt::RetryablePreSubmit => {}
                MutationAttempt::Fatal(error) => panic!(
                    "mutation on {} failed after submission; outcome uncertain: {error}; SQL={sql}",
                    node.spec.id
                ),
                MutationAttempt::TimedOut => panic!(
                    "mutation on {} exceeded {QUERY_TIMEOUT:?}; outcome uncertain: {sql}",
                    node.spec.id
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
        .simple_query("SELECT id, name FROM snapshot_process_items")
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
        let last = match read_rows(node.spec.sql_port) {
            Ok(rows) if &rows == expected => return,
            Ok(rows) => format!("rows={rows:?}"),
            Err(error) => format!("error={error}"),
        };
        assert!(
            Instant::now() < deadline,
            "{} did not converge to {expected:?}; last={last}",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(40));
    }
}

fn compact_stopped_member(path: &Path) -> u64 {
    let engine = Arc::new(StorageEngine::open(path).expect("open stopped leader storage"));
    let catalog = Arc::new(
        RocksDbCatalog::new(Arc::clone(&engine))
            .load_all()
            .expect("hydrate stopped leader catalog"),
    );
    let clock = Arc::new(HlcClock::new(500));
    let state_machine = ReplicatedSqlStateMachine::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    )
    .expect("load durable SQL apply state");
    let durable = state_machine
        .durable_state()
        .expect("read durable SQL apply state");
    assert!(durable.last_applied_index > 0);

    let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
    let (mut persistent, _) = store
        .load()
        .expect("load Raft persistence")
        .expect("stopped member must have persisted Raft state");
    let boundary = durable.last_applied_index;
    assert!(boundary <= persistent.last_log_index());
    let term = persistent.term_at(boundary);
    assert!(term > 0, "snapshot boundary term must be available");

    let manager = ReplicatedSqlSnapshotManager::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    );
    let bytes = manager
        .export(boundary, term)
        .expect("export stopped-member SQL snapshot");
    let data = Arc::new(bytes);
    store
        .stage_snapshot(&StagedSnapshot {
            kind: StagedSnapshotKind::Creation,
            last_included_index: boundary,
            last_included_term: term,
            data: Arc::clone(&data),
        })
        .expect("stage stopped-member SQL snapshot");
    persistent.install_snapshot(boundary, term);
    store
        .save(&persistent, data.as_slice())
        .expect("publish stopped-member SQL snapshot");
    boundary
}

fn inspect_stopped_member(path: &Path) -> (u64, u64, usize) {
    let engine = Arc::new(StorageEngine::open(path).expect("open stopped replacement storage"));
    let store = RocksDbRaftPersistenceStore::new(engine);
    let (persistent, snapshot) = store
        .load()
        .expect("load replacement Raft persistence")
        .expect("replacement must have persisted Raft state");
    (
        persistent.snapshot_index,
        persistent.last_log_index(),
        snapshot.len(),
    )
}

#[test]
fn empty_disk_fixed_member_recovers_from_snapshot_over_tcp_and_survives_restart_failover() {
    let root = TempDir::new().expect("cluster tempdir");
    let specs = build_specs(&root);
    let mut nodes: Vec<NodeProcess> = specs.into_iter().map(NodeProcess::new).collect();

    for node in &mut nodes {
        node.start();
    }
    for node in &mut nodes {
        wait_sql_query_ready(node);
    }

    mutate_on_leader(
        &mut nodes,
        "CREATE TABLE snapshot_process_items (id BIGINT, name TEXT)",
    );
    for id in 1..=11 {
        mutate_on_leader(
            &mut nodes,
            &format!("INSERT INTO snapshot_process_items (id, name) VALUES ({id}, 'v{id}')"),
        );
    }
    let leader = mutate_on_leader(
        &mut nodes,
        "INSERT INTO snapshot_process_items (id, name) VALUES (12, 'v12')",
    );
    let mut expected: BTreeSet<(String, String)> = (1..=12)
        .map(|id| (id.to_string(), format!("v{id}")))
        .collect();
    for node in &mut nodes {
        wait_rows(node, &expected);
    }

    for node in &mut nodes {
        node.kill();
    }

    let boundary = compact_stopped_member(&nodes[leader].spec.db_path);
    let victim = (0..nodes.len())
        .find(|index| *index != leader)
        .expect("replacement victim");
    let survivor = (0..nodes.len())
        .find(|index| *index != leader && *index != victim)
        .expect("remaining survivor");

    std::fs::remove_dir_all(&nodes[victim].spec.db_path)
        .expect("delete replacement member RocksDB directory");

    // Make the compacted member deterministically win the reconstruction-term
    // election while preserving fixed membership.
    nodes[leader].spec.election_timeout_ms = 80;
    nodes[survivor].spec.election_timeout_ms = 1_200;
    nodes[victim].spec.election_timeout_ms = 1_800;

    nodes[leader].start();
    nodes[survivor].start();
    wait_sql_query_ready(&mut nodes[leader]);
    wait_sql_query_ready(&mut nodes[survivor]);

    let accepted = mutate_on_leader(
        &mut nodes,
        "UPDATE snapshot_process_items SET name = 'after_snapshot' WHERE id = 1",
    );
    assert_eq!(
        accepted, leader,
        "the member with the compacted prefix must lead replacement bootstrap"
    );
    expected.remove(&("1".to_string(), "v1".to_string()));
    expected.insert(("1".to_string(), "after_snapshot".to_string()));
    wait_rows(&mut nodes[leader], &expected);
    wait_rows(&mut nodes[survivor], &expected);

    // Same fixed node ID, completely empty local storage. Its SQL server remains
    // non-serving until InstallSnapshot and the post-snapshot suffix are applied.
    nodes[victim].start();
    wait_rows(&mut nodes[victim], &expected);

    nodes[victim].kill();
    let (installed_boundary, replacement_last_log, active_snapshot_bytes) =
        inspect_stopped_member(&nodes[victim].spec.db_path);
    assert_eq!(installed_boundary, boundary);
    assert!(replacement_last_log > boundary);
    assert!(active_snapshot_bytes > 0);

    // Restart from the reconstructed disk and prove the active snapshot does not
    // regress the already-applied suffix.
    nodes[victim].start();
    wait_sql_query_ready(&mut nodes[victim]);
    wait_rows(&mut nodes[victim], &expected);

    // Finally remove the snapshot source leader. The reconstructed member and
    // original survivor must still form quorum and preserve a new acknowledged
    // mutation through failover.
    nodes[leader].kill();
    mutate_on_leader(
        &mut nodes,
        "UPDATE snapshot_process_items SET name = 'after_replacement_failover' WHERE id = 2",
    );
    expected.remove(&("2".to_string(), "v2".to_string()));
    expected.insert(("2".to_string(), "after_replacement_failover".to_string()));
    wait_rows(&mut nodes[survivor], &expected);
    wait_rows(&mut nodes[victim], &expected);

    for node in &mut nodes {
        node.kill();
    }
}
