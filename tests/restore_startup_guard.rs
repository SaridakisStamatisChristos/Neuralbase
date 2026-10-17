// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::process::Command;

use neuralbase::restore::{ensure_clustered_startup_restore_safe, RestoreError};
use tempfile::TempDir;

#[test]
fn absent_target_with_restore_stage_fails_closed() {
    let root = TempDir::new().unwrap();
    let target = root.path().join("recovered-db");
    let staged = root.path().join(".recovered-db.restore-partial-99-1234-0");
    fs::create_dir(&staged).unwrap();
    fs::write(
        staged.join(".neuralbase-restore-state"),
        b"NBR5-RESTORE-1\nstate=building\n",
    )
    .unwrap();

    let error = ensure_clustered_startup_restore_safe(&target).unwrap_err();
    assert!(matches!(error, RestoreError::IncompleteRestoreStage(path) if path == staged));
    assert!(!target.exists());
}

#[test]
fn completed_target_is_authority_even_if_old_stage_remains() {
    let root = TempDir::new().unwrap();
    let target = root.path().join("recovered-db");
    let staged = root.path().join(".recovered-db.restore-partial-99-1234-0");
    fs::create_dir(&target).unwrap();
    fs::create_dir(&staged).unwrap();

    ensure_clustered_startup_restore_safe(&target).unwrap();
}

#[test]
fn real_clustered_process_refuses_to_create_empty_db_over_restore_remnant() {
    let root = TempDir::new().unwrap();
    let target = root.path().join("recovered-db");
    let staged = root.path().join(".recovered-db.restore-partial-99-1234-0");
    fs::create_dir(&staged).unwrap();
    fs::write(
        staged.join(".neuralbase-restore-state"),
        b"NBR5-RESTORE-1\nstate=building\n",
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_neuralbase"))
        .env("NEURALBASE_NODE_ID", "startup-guard-node")
        .env("NEURALBASE_DB_PATH", &target)
        .env("NEURALBASE_METRICS_PORT", "0")
        .env("NEURALBASE_LISTEN_ADDR", "127.0.0.1:0")
        .env("NEURALBASE_RAFT_ADDR", "127.0.0.1:0")
        .env("NEURALBASE_PEERS", "")
        .env("NEURALBASE_RAFT_TLS", "0")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("incomplete restore staging directory exists"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        !target.exists(),
        "startup must not create a fresh database over incomplete restore intent"
    );
}
