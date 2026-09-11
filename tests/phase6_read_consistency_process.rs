// SPDX-License-Identifier: Apache-2.0
//! Real OS-process/TCP evidence for the Phase-6 SQL consistency contract.

use std::collections::HashSet;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Clone)]
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
    fn start(&mut self) {
        assert!(self.child.is_none());
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_neuralbase"))
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
                .env("NEURALBASE_METRICS_PORT", self.spec.metrics_port.to_string())
                .env("NEURALBASE_RAFT_ELECTION_TIMEOUT_MS", "80")
                .env("NEURALBASE_AUTH_REQUIRED", "0")
                .env("NEURALBASE_RAFT_TLS", "0")
                .env_remove("NEURALBASE_IDENTITY_MIGRATION_SHA256")
                .env("RUST_LOG", "warn")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .expect("start phase6 process node"),
        );
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn running(&mut self) -> bool {
        self.child
            .as_mut()
            .is_some_and(|child| child.try_wait().expect("child status").is_none())
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

fn spec(root: &TempDir, used: &mut HashSet<u16>, id: &str, db_name: &str) -> NodeSpec {
    NodeSpec {
        id: id.to_string(),
        sql_port: reserve_port(used),
        raft_port: reserve_port(used),
        metrics_port: reserve_port(used),
        db_path: root.path().join(db_name),
        users_file: root.path().join(format!("{db_name}-users.json")),
    }
}

fn connect(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1"),
        NoTls,
    )
}

fn wait_ready(node: &mut NodeProcess) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(node.running(), "phase6 process node exited during startup");
        if connect(node.spec.sql_port).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "phase6 process node did not become ready");
        thread::sleep(Duration::from_millis(30));
    }
}

fn read_value(client: &mut Client) -> String {
    let messages = client
        .simple_query("SELECT value FROM p6_items")
        .expect("strong SELECT");
    messages
        .into_iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .expect("row value")
}

fn run_backup_cli(command: &mut Command) -> Output {
    let output = command.output().expect("run neuralbase-backup");
    assert!(
        output.status.success(),
        "neuralbase-backup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn assert_linearizable_value(node: &mut NodeProcess, expected: &str) {
    wait_ready(node);
    let mut client = connect(node.spec.sql_port).expect("connect strong-read client");
    client
        .simple_query("SET neuralbase_read_consistency = linearizable")
        .expect("set linearizable");
    assert_eq!(read_value(&mut client), expected);
}

#[test]
fn process_session_modes_restart_and_phase5_restore_bootstrap() {
    let root = TempDir::new().expect("phase6 tempdir");
    let mut used = HashSet::new();
    let source_spec = spec(&root, &mut used, "p6-process-source", "source-db");
    let mut node = NodeProcess {
        spec: source_spec.clone(),
        child: None,
    };
    node.start();
    wait_ready(&mut node);

    let mut writer = connect(node.spec.sql_port).expect("connect writer");
    writer
        .simple_query("CREATE TABLE p6_items (value TEXT)")
        .expect("create table");
    writer
        .simple_query("INSERT INTO p6_items VALUES ('acknowledged')")
        .expect("acknowledged replicated write");

    // Immediate real-TCP read-after-write in the strongest mode.
    writer
        .simple_query("SET neuralbase_read_consistency = 'linearizable'")
        .expect("set linearizable");
    assert_eq!(read_value(&mut writer), "acknowledged");

    writer
        .simple_query("SET neuralbase.read_consistency TO leader")
        .expect("set leader authoritative");
    assert_eq!(read_value(&mut writer), "acknowledged");

    // A different session retains the backward-compatible Local default.
    let mut independent = connect(node.spec.sql_port).expect("connect independent session");
    assert_eq!(read_value(&mut independent), "acknowledged");
    drop(independent);
    drop(writer);

    node.kill();
    node.start();
    assert_linearizable_value(&mut node, "acknowledged");
    node.kill();

    // Phase-5 recovery establishes a fresh consensus generation. Phase 6 must
    // acquire authority in that generation rather than inheriting stale source
    // authority from the backup.
    let backup = root.path().join("phase6-restored.nbbk");
    run_backup_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-backup"))
            .arg("create")
            .arg("--db")
            .arg(&source_spec.db_path)
            .arg("--output")
            .arg(&backup),
    );
    run_backup_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-backup"))
            .arg("verify")
            .arg("--backup")
            .arg(&backup),
    );

    let recovery_spec = spec(&root, &mut used, "p6-process-recovery", "recovery-db");
    run_backup_cli(
        Command::new(env!("CARGO_BIN_EXE_neuralbase-backup"))
            .arg("restore")
            .arg("--backup")
            .arg(&backup)
            .arg("--target")
            .arg(&recovery_spec.db_path)
            .arg("--node-id")
            .arg(&recovery_spec.id),
    );

    let mut recovered = NodeProcess {
        spec: recovery_spec,
        child: None,
    };
    recovered.start();
    assert_linearizable_value(&mut recovered, "acknowledged");
    recovered.kill();
}
