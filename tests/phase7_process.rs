// SPDX-License-Identifier: Apache-2.0
//! Real server + independent controller processes, TCP Raft and RocksDB.
#![cfg(target_os = "linux")]
use postgres::{Client, NoTls, SimpleQueryMessage};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct Deployment {
    root: TempDir,
    config: Value,
    path: PathBuf,
}
impl Deployment {
    fn new() -> Self {
        let root = tempfile::Builder::new().prefix("nb7-").tempdir().unwrap();
        let mut used = HashSet::new();
        let mut port = || loop {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let p = listener.local_addr().unwrap().port();
            if used.insert(p) {
                break p;
            }
        };
        let mut endpoints = serde_json::Map::new();
        let mut processes = serde_json::Map::new();
        for id in ["g1.a", "g1.b", "g1.c", "g1.d", "g1.e"] {
            endpoints.insert(id.into(), json!(format!("127.0.0.1:{}", port())));
            processes.insert(id.into(),json!({"sql_addr":format!("127.0.0.1:{}",port()),"metrics_addr":format!("127.0.0.1:{}",port())}));
        }
        let config = json!({"root":root.path().join("nodes"),"desired":{"version":1,"cluster":"g1","revision":1,"minimum_voters":3,"endpoints":endpoints,"voters":["g1.a","g1.b","g1.c"]},"processes":processes});
        let path = root.path().join("config.json");
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        Self { root, config, path }
    }
    fn state(&self) -> Value {
        serde_json::from_slice(&fs::read(self.root.path().join("nodes/state.json")).unwrap())
            .unwrap()
    }
    fn command(&self, verb: &str) -> Command {
        let mut c = Command::new("python3");
        c.arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/ops/neuralbase_operator.py"
        ))
        .arg(verb)
        .arg("--config")
        .arg(&self.path)
        .arg("--server")
        .arg(env!("CARGO_BIN_EXE_neuralbase"))
        .arg("--planner")
        .arg(env!("CARGO_BIN_EXE_neuralbase-operator"));
        c
    }
    fn invoke(&self, verb: &str) -> Value {
        let out = self.command(verb).output().unwrap();
        assert!(
            out.status.success() || out.status.code() == Some(2),
            "controller failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let line = out
            .stdout
            .split(|c| *c == b'\n')
            .filter(|s| !s.is_empty())
            .next_back()
            .expect("JSON controller output");
        serde_json::from_slice(line).unwrap()
    }
    fn converge(&self) -> Value {
        let deadline = Instant::now() + Duration::from_secs(100);
        loop {
            let result = self.invoke("reconcile");
            if result["plan"]["action"] == "Converged" {
                return result;
            }
            assert!(
                Instant::now() < deadline,
                "reconciliation did not converge: {result}"
            );
            thread::sleep(Duration::from_millis(100));
        }
    }
    fn desired(&mut self, voters: Vec<String>) {
        let rev = self.config["desired"]["revision"].as_u64().unwrap() + 1;
        self.config["desired"]["revision"] = json!(rev);
        self.config["desired"]["voters"] = json!(voters);
        fs::write(&self.path, serde_json::to_vec(&self.config).unwrap()).unwrap();
    }
    fn connect(
        &self,
        id: &str,
        user: &str,
        password: Option<&str>,
    ) -> Result<Client, postgres::Error> {
        let addr = self.config["processes"][id]["sql_addr"].as_str().unwrap();
        let port = addr.rsplit(':').next().unwrap();
        let mut config = postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(port.parse().unwrap())
            .user(user)
            .dbname("postgres")
            .connect_timeout(Duration::from_secs(1));
        if let Some(password) = password {
            config.password(password);
        }
        config.connect(NoTls)
    }
    fn sql(&self, id: &str, sql: &str) {
        self.connect(id, "postgres", None)
            .unwrap()
            .simple_query(sql)
            .unwrap();
    }
    fn verify(&self, ids: &[String], count: &str) {
        for id in ids {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let result = self
                    .connect(id, "p7_user", Some("phase-seven-password"))
                    .and_then(|mut c| c.simple_query("SELECT COUNT(*) FROM p7_items"));
                if let Ok(rows) = result {
                    if rows.iter().any(
                        |r| matches!(r,SimpleQueryMessage::Row(row) if row.get(0)==Some(count)),
                    ) {
                        break;
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "SQL/identity failed to converge on {id}"
                );
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    fn kill(&self, id: &str) {
        let pid = self.state()["pids"][id]["pid"].as_u64().unwrap();
        assert!(Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .unwrap()
            .success());
    }
}
impl Drop for Deployment {
    fn drop(&mut self) {
        if let Ok(data) = fs::read(self.root.path().join("nodes/state.json")) {
            if let Ok(state) = serde_json::from_slice::<Value>(&data) {
                if let Some(pids) = state["pids"].as_object() {
                    for record in pids.values() {
                        if let Some(pid) = record["pid"].as_u64() {
                            let _ = Command::new("kill")
                                .args(["-KILL", &pid.to_string()])
                                .stdout(Stdio::null())
                                .stderr(Stdio::null())
                                .status();
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn process_controller_scale_replace_restart_and_identity_convergence() {
    let mut d = Deployment::new();
    d.invoke("bootstrap");
    let initial = d.converge();
    let leader = initial["observed"]["leader"].as_str().unwrap();
    d.sql(leader, "CREATE TABLE p7_items (id INT, value TEXT)");
    d.sql(leader, "INSERT INTO p7_items VALUES (1, 'before')");
    d.sql(
        leader,
        "CREATE USER p7_user WITH PASSWORD 'phase-seven-password'",
    );
    let first = vec!["g1.a".to_string(), "g1.b".into(), "g1.c".into()];
    d.verify(&first, "1");
    d.desired(vec![
        "g1.a".into(),
        "g1.b".into(),
        "g1.c".into(),
        "g1.d".into(),
    ]);
    let before = fs::read(d.root.path().join("nodes/state.json")).unwrap();
    let plan = d.invoke("plan");
    assert_eq!(plan["plan"]["action"], json!({"CreateLearner":"g1.d"}));
    assert_eq!(
        before,
        fs::read(d.root.path().join("nodes/state.json")).unwrap(),
        "dry-run must not accept desired changes"
    );
    // Real deployment failure: occupied learner listener. Reconciliation may
    // create storage, but must not turn a failed process into a voter.
    let occupied =
        TcpListener::bind(d.config["desired"]["endpoints"]["g1.d"].as_str().unwrap()).unwrap();
    d.invoke("reconcile");
    thread::sleep(Duration::from_millis(200));
    let failed = d.invoke("plan");
    assert_ne!(failed["plan"]["action"], "Converged");
    drop(occupied);
    let expanded = d.converge();
    assert_eq!(
        expanded["observed"]["committed"]["voters"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    let all = vec!["g1.a".into(), "g1.b".into(), "g1.c".into(), "g1.d".into()];
    d.verify(&all, "1");
    // Lose and restart a voter with durable state. Every CLI invocation above
    // is also a separate controller process, exercising restart observations.
    let lost = expanded["observed"]["leader"].as_str().unwrap().to_string();
    d.kill(&lost);
    let recovered = d.converge();
    assert_eq!(
        recovered["observed"]["committed"]["voters"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    // Replace current leader with a fresh reserved identity. Planner must add
    // and promote replacement, transfer, remove, then retire the old process.
    let old = recovered["observed"]["leader"]
        .as_str()
        .unwrap()
        .to_string();
    let mut desired: Vec<String> = all.iter().filter(|id| **id != old).cloned().collect();
    desired.push("g1.e".into());
    d.desired(desired.clone());
    let replacement = d.converge();
    assert!(replacement["observed"]["committed"]["removed"]
        .as_array()
        .unwrap()
        .contains(&json!(old)));
    d.verify(&desired, "1");
    let leader = replacement["observed"]["leader"].as_str().unwrap();
    d.sql(leader, "INSERT INTO p7_items VALUES (2, 'after')");
    d.verify(&desired, "2");
    // 4 -> 3 follower contraction, with retained retired storage.
    let remove = desired
        .iter()
        .find(|id| id.as_str() != leader)
        .unwrap()
        .clone();
    desired.retain(|id| *id != remove);
    d.desired(desired.clone());
    let final_state = d.converge();
    assert_eq!(
        final_state["observed"]["committed"]["voters"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    d.verify(&desired, "2");
    assert!(d.root.path().join(format!("nodes/{remove}.db")).exists());
    assert!(d.state()["metrics"]["controller_starts"].as_u64().unwrap() > 10);
    let generation = final_state["observed"]["committed"]["generation"].clone();
    assert_eq!(
        d.converge()["observed"]["committed"]["generation"],
        generation,
        "duplicate reconciliation must not mutate membership"
    );
    desired.push(old);
    d.desired(desired);
    let rejected = d.invoke("plan");
    assert_ne!(rejected["plan"]["action"], "Converged");
}

#[test]
fn supervisor_process_identity_and_document_boundaries() {
    let result = Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/phase7_supervisor.py"
        ))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
