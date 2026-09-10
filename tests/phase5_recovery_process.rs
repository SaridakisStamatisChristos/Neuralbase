// SPDX-License-Identifier: Apache-2.0
//! Real OS-process Phase-5 backup/restore/restart evidence.
//!
//! This test uses the actual `neuralbase` and `neuralbase-backup` binaries. It
//! creates SQL and replicated SCRAM state in one running process, stops it,
//! creates and independently verifies an operator backup through the CLI,
//! restores into a fresh node identity, boots that restored database with
//! authentication required, commits a new write, restarts the real server, and
//! finally verifies recovered Raft/identity state directly from the closed disk.

use std::collections::{BTreeSet, HashSet};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use neuralbase::consensus::RaftPersistenceStore;
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_identity_store::ReplicatedIdentityState;
use neuralbase::storage::StorageEngine;
use postgres::{Client, NoTls, SimpleQueryMessage};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(12);
const MUTATION_TIMEOUT: Duration = Duration::from_secs(12);
const QUERY_TIMEOUT: Duration = Duration::from_secs(4);
const SOURCE_ID: &str = "p5-process-source";
const RECOVERY_ID: &str = "p5-process-recovery";
const USER: &str = "alice";
const PASSWORD: &str = "phase5-process-secret";

#[derive(Clone, Debug)]
struct NodeSpec {
    id: String,
    sql_port: u16,
    raft_port: u16,
    metrics_port: u16,
    db_path: PathBuf,
    users_file: PathBuf,
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
            .env("NEURALBASE_PEERS", "")
            .env("NEURALBASE_DB_PATH", &self.spec.db_path)
            .env("NEURALBASE_USERS_FILE", &self.spec.users_file)
            .env(
                "NEURALBASE_METRICS_PORT",
                self.spec.metrics_port.to_string(),
            )
            .env("NEURALBASE_RAFT_ELECTION_TIMEOUT_MS", "80")
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

fn node_spec(root: &TempDir, used: &mut HashSet<u16>, id: &str, name: &str) -> NodeSpec {
    NodeSpec {
        id: id.to_string(),
        sql_port: reserve_port(used),
        raft_port: reserve_port(used),
        metrics_port: reserve_port(used),
        db_path: root.path().join(name),
        users_file: root.path().join(format!("{name}-legacy-users.json")),
    }
}

fn connect_no_auth(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1"),
        NoTls,
    )
}

fn connect_auth(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!(
            "host=127.0.0.1 port={port} user={USER} password={PASSWORD} \
             dbname=postgres connect_timeout=1"
        ),
        NoTls,
    )
}

fn wait_ready(node: &mut NodeProcess, auth_required: bool) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited during startup", node.spec.id);
        let ready = if auth_required {
            connect_auth(node.spec.sql_port).is_ok()
        } else {
            connect_no_auth(node.spec.sql_port).is_ok()
        };
        if ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} SQL/auth readiness did not converge",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(30));
    }
}

fn retryable_pre_submit(error: &postgres::Error) -> bool {
    error.as_db_error().is_some_and(|db| {
        matches!(db.code().code(), "25006" | "57P03")
    })
}

enum MutationAttempt {
    Success,
    RetryablePreSubmit,
    Fatal(String),
    TimedOut,
}

fn mutation_attempt(port: u16, auth_required: bool, sql: &str) -> MutationAttempt {
    let sql = sql.to_string();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let _worker = thread::spawn(move || {
        let connection = if auth_required {
            connect_auth(port)
        } else {
            connect_no_auth(port)
        };
        let outcome = match connection {
            Err(_) => MutationAttempt::RetryablePreSubmit,
            Ok(mut client) => match client.simple_query(&sql) {
                Ok(_) => MutationAttempt::Success,
                Err(error) if retryable_pre_submit(&error) => MutationAttempt::RetryablePreSubmit,
                Err(error) => MutationAttempt::Fatal(error.to_string()),
            },
        };
        let _ = result_tx.send(outcome);
    });

    result_rx
        .recv_timeout(QUERY_TIMEOUT)
        .unwrap_or(MutationAttempt::TimedOut)
}

fn mutate_when_ready(node: &mut NodeProcess, auth_required: bool, sql: &str) {
    let deadline = Instant::now() + MUTATION_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited before mutation", node.spec.id);
        match mutation_attempt(node.spec.sql_port, auth_required, sql) {
            MutationAttempt::Success => return,
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
        assert!(
            Instant::now() < deadline,
            "{} never became mutation-ready: {sql}",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(30));
    }
}

fn rows_blocking(port: u16) -> Result<BTreeSet<(String, String)>, String> {
    let mut client = connect_auth(port).map_err(|error| error.to_string())?;
    let messages = client
        .simple_query("SELECT id, name FROM recovery_items")
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

fn read_rows(port: u16) -> BTreeSet<(String, String)> {
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let _worker = thread::spawn(move || {
        let _ = result_tx.send(rows_blocking(port));
    });
    result_rx
        .recv_timeout(QUERY_TIMEOUT)
        .expect("recovery read query timed out")
        .expect("recovery read query failed")
}

fn run_cli(command: &mut Command) -> Output {
    let output = command.output().expect("run neuralbase-backup CLI");
    assert!(
        output.status.success(),
        "neuralbase-backup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn process_backup_restore_boot_auth_write_and_restart() {
    let root = TempDir::new().expect("recovery process tempdir");
    let mut used = HashSet::new();
    let source_spec = node_spec(&root, &mut used, SOURCE_ID, "source-db");
    let recovery_spec = node_spec(&root, &mut used, RECOVERY_ID, "recovery-db");
    let backup_path = root.path().join("process-recovery.nbbk");

    let mut source = NodeProcess::new(source_spec.clone());
    source.start(false);
    wait_ready(&mut source, false);
    mutate_when_ready(
        &mut source,
        false,
        "CREATE TABLE recovery_items (id BIGINT, name TEXT)",
    );
    mutate_when_ready(
        &mut source,
        false,
        "INSERT INTO recovery_items VALUES (1, 'before-backup')",
    );
    mutate_when_ready(
        &mut source,
        false,
        &format!("CREATE USER {USER} WITH PASSWORD '{PASSWORD}'"),
    );
    source.kill();

    run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-backup"))
            .arg("create")
            .arg("--db")
            .arg(&source_spec.db_path)
            .arg("--output")
            .arg(&backup_path),
    );
    run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-backup"))
            .arg("verify")
            .arg("--backup")
            .arg(&backup_path),
    );
    run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-backup"))
            .arg("restore")
            .arg("--backup")
            .arg(&backup_path)
            .arg("--target")
            .arg(&recovery_spec.db_path)
            .arg("--node-id")
            .arg(RECOVERY_ID),
    );

    let expected_before = BTreeSet::from([("1".to_string(), "before-backup".to_string())]);
    let mut recovery = NodeProcess::new(recovery_spec.clone());
    recovery.start(true);
    wait_ready(&mut recovery, true);
    assert_eq!(read_rows(recovery.spec.sql_port), expected_before);

    mutate_when_ready(
        &mut recovery,
        true,
        "INSERT INTO recovery_items VALUES (2, 'after-restore')",
    );
    let expected_after = BTreeSet::from([
        ("1".to_string(), "before-backup".to_string()),
        ("2".to_string(), "after-restore".to_string()),
    ]);
    assert_eq!(read_rows(recovery.spec.sql_port), expected_after);

    recovery.kill();
    recovery.start(true);
    wait_ready(&mut recovery, true);
    assert_eq!(read_rows(recovery.spec.sql_port), expected_after);
    recovery.kill();

    // Inspect only after the real process has released the RocksDB lock.
    let engine = Arc::new(StorageEngine::open(&recovery_spec.db_path).expect("open restored disk"));
    let raft_store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
    let (persistent, _) = raft_store
        .load()
        .expect("load recovered Raft state")
        .expect("recovered Raft state must exist");
    let membership = persistent
        .membership
        .expect("recovered membership must be persisted");
    assert_eq!(
        membership.voters,
        BTreeSet::from([RECOVERY_ID.to_string()])
    );
    assert!(membership.removed.contains(SOURCE_ID));
    assert!(!membership.voters.contains(SOURCE_ID));

    let identity = ReplicatedIdentityState::load(&engine)
        .expect("load recovered identity")
        .expect("recovered identity must exist");
    assert!(identity.contains_user(USER));
    assert!(!recovery_spec.users_file.exists());
}
