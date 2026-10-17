// SPDX-License-Identifier: Apache-2.0
//! Real-process concurrent write/read serialization evidence for Phase 6.

use std::collections::HashSet;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};
use tempfile::TempDir;

const START_TIMEOUT: Duration = Duration::from_secs(12);
const WRITE_COUNT: usize = 24;
const READ_COUNT: usize = 36;

fn reserve_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        if used.insert(port) {
            return port;
        }
    }
}

fn connect(port: u16) -> Result<Client, postgres::Error> {
    Client::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1"),
        NoTls,
    )
}

fn row_count(client: &mut Client) -> usize {
    client
        .simple_query("SELECT value FROM p6_concurrent")
        .expect("linearizable SELECT")
        .into_iter()
        .filter(|message| matches!(message, SimpleQueryMessage::Row(_)))
        .count()
}

fn wait_ready(port: u16, child: &mut Child) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        assert!(child.try_wait().unwrap().is_none(), "phase6 process exited");
        if connect(port).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "phase6 process did not start");
        thread::sleep(Duration::from_millis(30));
    }
}

fn mutation_when_leader(port: u16, sql: &str) {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        let result = connect(port).and_then(|mut client| client.simple_query(sql).map(|_| ()));
        match result {
            Ok(()) => return,
            Err(error)
                if error
                    .as_db_error()
                    .is_some_and(|db| matches!(db.code().code(), "25006" | "57P03")) => {}
            Err(error) => panic!("mutation failed: {error}; SQL={sql}"),
        }
        assert!(
            Instant::now() < deadline,
            "node never became mutation leader"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn concurrent_inserts_and_linearizable_reads_form_monotonic_prefixes() {
    let root = TempDir::new().unwrap();
    let mut used = HashSet::new();
    let sql_port = reserve_port(&mut used);
    let raft_port = reserve_port(&mut used);
    let metrics_port = reserve_port(&mut used);
    let db = root.path().join("db");
    let users = root.path().join("users.json");

    let mut child = Command::new(env!("CARGO_BIN_EXE_neuralbase"))
        .env("NEURALBASE_NODE_ID", "p6-concurrent")
        .env("NEURALBASE_LISTEN_ADDR", format!("127.0.0.1:{sql_port}"))
        .env("NEURALBASE_RAFT_ADDR", format!("127.0.0.1:{raft_port}"))
        .env("NEURALBASE_PEERS", "")
        .env("NEURALBASE_DB_PATH", &db)
        .env("NEURALBASE_USERS_FILE", &users)
        .env("NEURALBASE_METRICS_PORT", metrics_port.to_string())
        .env("NEURALBASE_RAFT_ELECTION_TIMEOUT_MS", "80")
        .env("NEURALBASE_AUTH_REQUIRED", "0")
        .env("NEURALBASE_RAFT_TLS", "0")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();

    wait_ready(sql_port, &mut child);
    mutation_when_leader(sql_port, "CREATE TABLE p6_concurrent (value BIGINT)");

    let writer = thread::spawn(move || {
        let mut client = connect(sql_port).expect("writer connect");
        for value in 0..WRITE_COUNT {
            client
                .simple_query(&format!("INSERT INTO p6_concurrent VALUES ({value})"))
                .expect("acknowledged concurrent insert");
            thread::sleep(Duration::from_millis(2));
        }
    });

    let reader = thread::spawn(move || {
        let mut client = connect(sql_port).expect("reader connect");
        client
            .simple_query("SET neuralbase_read_consistency = linearizable")
            .expect("set linearizable");
        let mut observed = Vec::with_capacity(READ_COUNT);
        for _ in 0..READ_COUNT {
            observed.push(row_count(&mut client));
            thread::sleep(Duration::from_millis(1));
        }
        observed
    });

    writer.join().expect("writer thread");
    let observed = reader.join().expect("reader thread");

    for pair in observed.windows(2) {
        assert!(
            pair[0] <= pair[1],
            "linearizable reads regressed from {} committed rows to {}",
            pair[0],
            pair[1]
        );
    }
    assert!(observed.iter().all(|count| *count <= WRITE_COUNT));

    let mut final_reader = connect(sql_port).expect("final reader connect");
    final_reader
        .simple_query("SET neuralbase_read_consistency = linearizable")
        .expect("set final linearizable");
    assert_eq!(row_count(&mut final_reader), WRITE_COUNT);

    let _ = child.kill();
    let _ = child.wait();
}
