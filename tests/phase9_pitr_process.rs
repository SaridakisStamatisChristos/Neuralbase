// SPDX-License-Identifier: Apache-2.0
//! Real OS-process Phase-9 PITR evidence.
//!
//! This test exercises the actual `neuralbase`, `neuralbase-backup`, and
//! `neuralbase-pitr` binaries. It creates a verified Phase-5 baseline, restarts
//! the source with synchronous runtime archiving, captures an intermediate
//! recovery target before a password rotation and later write, recovers exactly
//! to that target, creates a child timeline before the recovered process starts,
//! verifies SQL/authentication state, accepts a new branched write, and restarts
//! the recovered process against the child archive.

use std::collections::{BTreeSet, HashSet};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use neuralbase::consensus::RaftPersistenceStore;
use neuralbase::pitr_archive::PitrArchiveWriter;
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::replicated_identity_store::ReplicatedIdentityState;
use neuralbase::storage::StorageEngine;
use postgres::{Client, NoTls, SimpleQueryMessage};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(15);
const MUTATION_TIMEOUT: Duration = Duration::from_secs(15);
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const SOURCE_ID: &str = "p9-pitr-source";
const RECOVERY_ID: &str = "p9-pitr-recovery";
const USER: &str = "alice";
const OLD_PASSWORD: &str = "phase9-before-target";
const NEW_PASSWORD: &str = "phase9-after-target";

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

    fn start(&mut self, auth_required: bool, archive: Option<&Path>) {
        assert!(self.child.is_none(), "{} already running", self.spec.id);
        let mut command = Command::new(env!("CARGO_BIN_EXE_neuralbase"));
        command
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
            .env_remove("NEURALBASE_PITR_KEY_FILE")
            .env_remove("NEURALBASE_PITR_MAX_SEGMENTS")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if let Some(archive) = archive {
            command.env("NEURALBASE_PITR_ARCHIVE_DIR", archive);
        } else {
            command.env_remove("NEURALBASE_PITR_ARCHIVE_DIR");
        }
        let child = command
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

fn connect_auth(port: u16, password: &str) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!(
            "host=127.0.0.1 port={port} user={USER} password={password} dbname=postgres connect_timeout=1"
        ),
        NoTls,
    )
}

fn wait_ready(node: &mut NodeProcess, auth_required: bool, password: &str) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited during startup", node.spec.id);
        let ready = if auth_required {
            connect_auth(node.spec.sql_port, password).is_ok()
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

fn wait_auth(node: &mut NodeProcess, password: &str) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited while waiting for auth", node.spec.id);
        if connect_auth(node.spec.sql_port, password).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} authentication did not converge",
            node.spec.id
        );
        thread::sleep(Duration::from_millis(30));
    }
}

fn retryable_pre_submit(error: &postgres::Error) -> bool {
    error
        .as_db_error()
        .is_some_and(|db| matches!(db.code().code(), "25006" | "57P03"))
}

enum MutationAttempt {
    Success,
    RetryablePreSubmit,
    Fatal(String),
    TimedOut,
}

fn mutation_attempt(
    port: u16,
    auth_required: bool,
    password: &str,
    sql: &str,
) -> MutationAttempt {
    let sql = sql.to_string();
    let password = password.to_string();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let _worker = thread::spawn(move || {
        let connection = if auth_required {
            connect_auth(port, &password)
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

fn mutate_when_ready(
    node: &mut NodeProcess,
    auth_required: bool,
    password: &str,
    sql: &str,
) {
    let deadline = Instant::now() + MUTATION_TIMEOUT;
    loop {
        assert!(node.is_running(), "{} exited before mutation", node.spec.id);
        match mutation_attempt(node.spec.sql_port, auth_required, password, sql) {
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

fn rows_blocking(port: u16, password: &str) -> Result<BTreeSet<(String, String)>, String> {
    let mut client = connect_auth(port, password).map_err(|error| error.to_string())?;
    let messages = client
        .simple_query("SELECT id, name FROM pitr_items")
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

fn read_rows(port: u16, password: &str) -> BTreeSet<(String, String)> {
    let password = password.to_string();
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);
    let _worker = thread::spawn(move || {
        let _ = result_tx.send(rows_blocking(port, &password));
    });
    result_rx
        .recv_timeout(QUERY_TIMEOUT)
        .expect("PITR read query timed out")
        .expect("PITR read query failed")
}

fn run_cli(command: &mut Command, name: &str) -> Output {
    let output = command.output().unwrap_or_else(|error| panic!("run {name}: {error}"));
    assert!(
        output.status.success(),
        "{name} failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn archive_frontier(archive: &Path) -> u64 {
    let output = run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-pitr"))
            .arg("status")
            .arg("--archive")
            .arg(archive),
        "neuralbase-pitr status",
    );
    let stdout = String::from_utf8(output.stdout).expect("PITR status is UTF-8");
    stdout
        .split_whitespace()
        .find_map(|field| field.strip_prefix("frontier="))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("missing frontier in PITR status: {stdout}"))
}

#[test]
fn process_pitr_exact_target_identity_branch_and_restart() {
    let root = TempDir::new().expect("PITR process tempdir");
    let mut used = HashSet::new();
    let source_spec = node_spec(&root, &mut used, SOURCE_ID, "source-db");
    let recovery_spec = node_spec(&root, &mut used, RECOVERY_ID, "recovery-db");
    let baseline_path = root.path().join("baseline.nbbk");
    let source_archive = root.path().join("source-archive");
    let branch_baseline = root.path().join("branch-baseline.nbbk");
    let branch_archive = root.path().join("branch-archive");

    let mut source = NodeProcess::new(source_spec.clone());
    source.start(false, None);
    wait_ready(&mut source, false, "");
    mutate_when_ready(
        &mut source,
        false,
        "",
        "CREATE TABLE pitr_items (id BIGINT, name TEXT)",
    );
    mutate_when_ready(
        &mut source,
        false,
        "",
        "INSERT INTO pitr_items VALUES (1, 'baseline')",
    );
    mutate_when_ready(
        &mut source,
        false,
        "",
        &format!("CREATE USER {USER} WITH PASSWORD '{OLD_PASSWORD}'"),
    );
    source.kill();

    run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-backup"))
            .arg("create")
            .arg("--db")
            .arg(&source_spec.db_path)
            .arg("--output")
            .arg(&baseline_path),
        "neuralbase-backup create",
    );
    run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-pitr"))
            .arg("init")
            .arg("--backup")
            .arg(&baseline_path)
            .arg("--archive")
            .arg(&source_archive),
        "neuralbase-pitr init",
    );

    source.start(true, Some(&source_archive));
    wait_ready(&mut source, true, OLD_PASSWORD);
    mutate_when_ready(
        &mut source,
        true,
        OLD_PASSWORD,
        "INSERT INTO pitr_items VALUES (2, 'at-target')",
    );
    let target = archive_frontier(&source_archive);
    mutate_when_ready(
        &mut source,
        true,
        OLD_PASSWORD,
        &format!("ALTER USER {USER} WITH PASSWORD '{NEW_PASSWORD}'"),
    );
    wait_auth(&mut source, NEW_PASSWORD);
    mutate_when_ready(
        &mut source,
        true,
        NEW_PASSWORD,
        "INSERT INTO pitr_items VALUES (3, 'after-target')",
    );
    let source_latest = archive_frontier(&source_archive);
    assert!(source_latest > target, "later source history must extend the archive");
    source.kill();

    run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-pitr"))
            .arg("verify")
            .arg("--archive")
            .arg(&source_archive),
        "neuralbase-pitr verify",
    );
    run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-pitr"))
            .arg("recover")
            .arg("--backup")
            .arg(&baseline_path)
            .arg("--archive")
            .arg(&source_archive)
            .arg("--target")
            .arg(target.to_string())
            .arg("--target-dir")
            .arg(&recovery_spec.db_path)
            .arg("--node-id")
            .arg(RECOVERY_ID),
        "neuralbase-pitr recover",
    );

    // Establish the branch before the recovered process can append its election
    // no-op or any user write. The child baseline is an offline backup of the
    // exact recovered boundary and has the fresh recovery membership generation.
    run_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-pitr"))
            .arg("branch")
            .arg("--parent-archive")
            .arg(&source_archive)
            .arg("--branch-target")
            .arg(target.to_string())
            .arg("--db")
            .arg(&recovery_spec.db_path)
            .arg("--baseline-out")
            .arg(&branch_baseline)
            .arg("--archive")
            .arg(&branch_archive),
        "neuralbase-pitr branch",
    );

    let expected_target = BTreeSet::from([
        ("1".to_string(), "baseline".to_string()),
        ("2".to_string(), "at-target".to_string()),
    ]);
    let mut recovery = NodeProcess::new(recovery_spec.clone());
    recovery.start(true, Some(&branch_archive));
    wait_ready(&mut recovery, true, OLD_PASSWORD);
    assert_eq!(read_rows(recovery.spec.sql_port, OLD_PASSWORD), expected_target);
    assert!(
        connect_auth(recovery.spec.sql_port, NEW_PASSWORD).is_err(),
        "password rotation after the selected target must be excluded"
    );

    mutate_when_ready(
        &mut recovery,
        true,
        OLD_PASSWORD,
        "INSERT INTO pitr_items VALUES (4, 'branched-future')",
    );
    let branch_frontier = archive_frontier(&branch_archive);
    assert!(branch_frontier > target, "new branch must archive its own future");
    let expected_branch = BTreeSet::from([
        ("1".to_string(), "baseline".to_string()),
        ("2".to_string(), "at-target".to_string()),
        ("4".to_string(), "branched-future".to_string()),
    ]);
    assert_eq!(read_rows(recovery.spec.sql_port, OLD_PASSWORD), expected_branch);

    recovery.kill();
    recovery.start(true, Some(&branch_archive));
    wait_ready(&mut recovery, true, OLD_PASSWORD);
    assert_eq!(read_rows(recovery.spec.sql_port, OLD_PASSWORD), expected_branch);
    recovery.kill();

    let source_writer = PitrArchiveWriter::open(&source_archive, None).expect("open source archive");
    let branch_writer = PitrArchiveWriter::open(&branch_archive, None).expect("open branch archive");
    assert_eq!(
        branch_writer.metadata().parent_timeline,
        Some(source_writer.metadata().timeline)
    );
    assert_eq!(branch_writer.metadata().branch_index, Some(target));
    assert_ne!(branch_writer.metadata().timeline, source_writer.metadata().timeline);

    // Inspect only after the real process has released RocksDB's lock.
    let engine = Arc::new(StorageEngine::open(&recovery_spec.db_path).expect("open PITR disk"));
    let raft_store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
    let (persistent, _) = raft_store
        .load()
        .expect("load recovered Raft state")
        .expect("recovered Raft state exists");
    let membership = persistent.membership.expect("recovered membership exists");
    assert_eq!(membership.voters, BTreeSet::from([RECOVERY_ID.to_string()]));
    assert!(membership.removed.contains(SOURCE_ID));
    assert!(!membership.voters.contains(SOURCE_ID));

    let identity = ReplicatedIdentityState::load(&engine)
        .expect("load recovered identity")
        .expect("recovered identity exists");
    assert!(identity.contains_user(USER));
    assert!(!recovery_spec.users_file.exists());
}
