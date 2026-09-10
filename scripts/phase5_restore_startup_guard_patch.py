from pathlib import Path


def replace_exact(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    if new in text:
        return
    if old not in text:
        raise SystemExit(f"expected patch anchor not found in {path}")
    p.write_text(text.replace(old, new, 1))


replace_exact(
    "src/restore.rs",
    """    #[error(\"restore target appeared while restore was being built: {0}\")]\n    TargetAppeared(PathBuf),\n}""",
    """    #[error(\"restore target appeared while restore was being built: {0}\")]\n    TargetAppeared(PathBuf),\n    #[error(\"incomplete restore staging directory exists while target is absent: {0}\")]\n    IncompleteRestoreStage(PathBuf),\n}""",
)

replace_exact(
    "src/restore.rs",
    """/// Restore `backup_path` into a brand-new database directory at `target`.""",
    """/// Refuse clustered startup from an absent target while a matching restore stage exists.\n///\n/// A crash before atomic restore publication leaves only hidden sibling staging directories.\n/// Starting a clustered node at the absent final path must not silently create a fresh empty\n/// database and thereby discard the operator's recovery intent. Once the final target exists,\n/// it is authoritative and stale siblings do not block startup.\npub fn ensure_clustered_startup_restore_safe(target: &Path) -> Result<(), RestoreError> {\n    if target.exists() {\n        return Ok(());\n    }\n    let name = target\n        .file_name()\n        .ok_or(RestoreError::TargetNameMissing)?\n        .to_string_lossy();\n    let parent = target\n        .parent()\n        .filter(|path| !path.as_os_str().is_empty())\n        .unwrap_or_else(|| Path::new(\".\"));\n    let prefix = format!(\".{name}.restore-partial-\");\n    for entry in fs::read_dir(parent)? {\n        let entry = entry?;\n        if entry.file_type()?.is_dir()\n            && entry.file_name().to_string_lossy().starts_with(&prefix)\n        {\n            return Err(RestoreError::IncompleteRestoreStage(entry.path()));\n        }\n    }\n    Ok(())\n}\n\n/// Restore `backup_path` into a brand-new database directory at `target`.""",
)

replace_exact(
    "src/main.rs",
    """use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;\nuse neuralbase::rocksdb_catalog;""",
    """use neuralbase::replicated_state_machine::ReplicatedSqlStateMachine;\nuse neuralbase::restore::ensure_clustered_startup_restore_safe;\nuse neuralbase::rocksdb_catalog;""",
)

replace_exact(
    "src/main.rs",
    """    let storage_engine = if let Some(db_path) = env_with_legacy(\"NEURALBASE_DB_PATH\", \"DB_PATH\") {\n        match StorageEngine::open(Path::new(&db_path)) {""",
    """    let storage_engine = if let Some(db_path) = env_with_legacy(\"NEURALBASE_DB_PATH\", \"DB_PATH\") {\n        if clustered {\n            ensure_clustered_startup_restore_safe(Path::new(&db_path)).map_err(|error| {\n                io::Error::other(format!(\"clustered startup restore-safety check failed: {error}\"))\n            })?;\n        }\n        match StorageEngine::open(Path::new(&db_path)) {""",
)

Path("tests/phase5_restore_startup_guard.rs").write_text(r'''// SPDX-License-Identifier: Apache-2.0

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
    fs::write(staged.join(".neuralbase-restore-state"), b"NBR5-RESTORE-1\nstate=building\n")
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
    fs::write(staged.join(".neuralbase-restore-state"), b"NBR5-RESTORE-1\nstate=building\n")
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
''')
