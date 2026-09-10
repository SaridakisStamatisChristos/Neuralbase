// SPDX-License-Identifier: Apache-2.0
//! Crash-safe restore of one verified operator backup into a fresh recovery node.
//!
//! Restore never mutates an existing database directory. It reconstructs SQL,
//! identity, Raft snapshot state, and a deliberately new one-voter membership in
//! a hidden sibling directory, verifies that staged database independently, then
//! publishes the complete directory with one rename.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::backup::{BackupManifest, NeuralBaseBackup};
use crate::backup_encryption::{
    verify_encrypted_backup_file, BackupEncryptionError, BackupEncryptionKey,
};
use crate::catalog::InMemoryCatalog;
use crate::consensus::{
    encode_snapshot_payload, ClusterMembership, PersistentState, RaftPersistenceStore,
    MEMBERSHIP_FORMAT_VERSION,
};
use crate::hlc::HlcClock;
use crate::offline_backup::{verify_backup_file, OfflineBackupError};
use crate::raft_persistence::RocksDbRaftPersistenceStore;
use crate::replicated_snapshot_manager::{ReplicatedSqlSnapshotManager, SnapshotManagerError};
use crate::replicated_state_machine::{ReplicatedSqlApplyError, ReplicatedSqlStateMachine};
use crate::storage::{StorageEngine, StorageError};

const RESTORE_MARKER: &str = ".neuralbase-restore-state";
const RESTORE_MARKER_MAGIC: &str = "NBR5-RESTORE-1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    pub source_manifest: BackupManifest,
    pub recovery_node_id: String,
    pub recovery_membership_generation: u64,
    pub boundary_index: u64,
    pub boundary_term: u64,
}

#[derive(Debug, Error)]
pub enum RestoreError {
    #[error("backup validation failed before restore: {0}")]
    Backup(#[from] OfflineBackupError),
    #[error("encrypted backup validation failed before restore: {0}")]
    EncryptedBackup(#[from] BackupEncryptionError),
    #[error("restore target already exists: {0}")]
    TargetExists(PathBuf),
    #[error("restore target must have an existing parent directory")]
    TargetParentMissing,
    #[error("restore target must name a directory")]
    TargetNameMissing,
    #[error("recovery node id must be non-empty")]
    EmptyRecoveryNodeId,
    #[error("recovery node id {0:?} exists in source membership history; use a fresh node id")]
    ReusedRecoveryNodeId(String),
    #[error("cannot create a recovery membership because source generation overflowed")]
    MembershipGenerationOverflow,
    #[error("constructed recovery membership is invalid: {0}")]
    InvalidRecoveryMembership(String),
    #[error("restore storage failure: {0}")]
    Storage(#[from] StorageError),
    #[error("restore logical snapshot failure: {0}")]
    Snapshot(#[from] SnapshotManagerError),
    #[error("restore durable apply-state failure: {0}")]
    ApplyState(#[from] ReplicatedSqlApplyError),
    #[error("restore Raft persistence failure: {0}")]
    Raft(String),
    #[error("restore Raft snapshot envelope failure: {0}")]
    SnapshotEnvelope(String),
    #[error("restored staging database failed verification: {0}")]
    Verification(String),
    #[error("restore I/O failure: {0}")]
    Io(#[from] io::Error),
    #[error("could not allocate a unique restore staging directory")]
    StagingPathExhausted,
    #[error("restore target appeared while restore was being built: {0}")]
    TargetAppeared(PathBuf),
    #[error("incomplete restore staging directory exists while target is absent: {0}")]
    IncompleteRestoreStage(PathBuf),
}

/// Refuse clustered startup from an absent target while a matching restore stage exists.
///
/// A crash before atomic restore publication leaves only hidden sibling staging directories.
/// Starting a clustered node at the absent final path must not silently create a fresh empty
/// database and thereby discard the operator's recovery intent. Once the final target exists,
/// it is authoritative and stale siblings do not block startup.
pub fn ensure_clustered_startup_restore_safe(target: &Path) -> Result<(), RestoreError> {
    if target.exists() {
        return Ok(());
    }
    let name = target
        .file_name()
        .ok_or(RestoreError::TargetNameMissing)?
        .to_string_lossy();
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let prefix = format!(".{name}.restore-partial-");
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.file_name().to_string_lossy().starts_with(&prefix) {
            return Err(RestoreError::IncompleteRestoreStage(entry.path()));
        }
    }
    Ok(())
}

/// Restore `backup_path` into a brand-new database directory at `target`.
///
/// The source membership is historical evidence only. `recovery_node_id` must
/// be a fresh incarnation identity not present in the source's active or removed
/// sets. The restored database starts with a stable single-voter membership at
/// the backup Raft boundary; old source node IDs are tombstoned.
pub fn restore_new_cluster(
    backup_path: &Path,
    target: &Path,
    recovery_node_id: &str,
) -> Result<RestoreReport, RestoreError> {
    validate_target(target)?;
    let backup = verify_backup_file(backup_path)?;
    restore_verified_new_cluster(backup, target, recovery_node_id)
}

/// Restore an authenticated encrypted backup into a brand-new recovery cluster.
///
/// Decryption and complete backup validation finish before any restore target or
/// staging directory is created, so a wrong key or tampered artifact cannot
/// publish partial database state.
pub fn restore_encrypted_new_cluster(
    backup_path: &Path,
    key: &BackupEncryptionKey,
    target: &Path,
    recovery_node_id: &str,
) -> Result<RestoreReport, RestoreError> {
    validate_target(target)?;
    let backup = verify_encrypted_backup_file(backup_path, key)?;
    restore_verified_new_cluster(backup, target, recovery_node_id)
}

fn restore_verified_new_cluster(
    backup: NeuralBaseBackup,
    target: &Path,
    recovery_node_id: &str,
) -> Result<RestoreReport, RestoreError> {
    let recovery_membership = build_recovery_membership(&backup, recovery_node_id)?;
    let boundary = backup.manifest.metadata.last_included_index;
    let boundary_term = backup.manifest.metadata.last_included_term;
    let active_snapshot = encode_snapshot_payload(&recovery_membership, &backup.sql_snapshot)
        .map_err(RestoreError::SnapshotEnvelope)?;

    let staging = allocate_staging_path(target, backup.manifest.created_unix_ms)?;
    fs::create_dir(&staging)?;
    let result = restore_into_staging(
        &staging,
        &backup,
        &recovery_membership,
        &active_snapshot,
        recovery_node_id,
    );
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }

    if target.exists() {
        let _ = fs::remove_dir_all(&staging);
        return Err(RestoreError::TargetAppeared(target.to_path_buf()));
    }
    if let Err(error) = fs::rename(&staging, target) {
        let _ = fs::remove_dir_all(&staging);
        return Err(error.into());
    }
    sync_parent_dir(
        target
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?;

    Ok(RestoreReport {
        source_manifest: backup.manifest,
        recovery_node_id: recovery_node_id.to_string(),
        recovery_membership_generation: recovery_membership.generation,
        boundary_index: boundary,
        boundary_term,
    })
}

fn validate_target(target: &Path) -> Result<(), RestoreError> {
    if target.exists() {
        return Err(RestoreError::TargetExists(target.to_path_buf()));
    }
    if target.file_name().is_none() {
        return Err(RestoreError::TargetNameMissing);
    }
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(RestoreError::TargetParentMissing);
    }
    Ok(())
}

fn build_recovery_membership(
    backup: &NeuralBaseBackup,
    recovery_node_id: &str,
) -> Result<ClusterMembership, RestoreError> {
    if recovery_node_id.trim().is_empty() {
        return Err(RestoreError::EmptyRecoveryNodeId);
    }
    let recovery_node_id = recovery_node_id.to_string();
    let mut historical = backup.membership.replication_targets();
    historical.extend(backup.membership.removed.iter().cloned());
    if historical.contains(&recovery_node_id) {
        return Err(RestoreError::ReusedRecoveryNodeId(recovery_node_id));
    }

    let generation = backup
        .membership
        .generation
        .checked_add(1)
        .ok_or(RestoreError::MembershipGenerationOverflow)?;
    let membership = ClusterMembership {
        format_version: MEMBERSHIP_FORMAT_VERSION,
        generation,
        config_index: backup.manifest.metadata.last_included_index,
        voters: BTreeSet::from([recovery_node_id]),
        learners: BTreeSet::new(),
        joint: None,
        removed: historical,
    };
    membership
        .validate()
        .map_err(RestoreError::InvalidRecoveryMembership)?;
    Ok(membership)
}

fn allocate_staging_path(target: &Path, created_unix_ms: u64) -> Result<PathBuf, RestoreError> {
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .ok_or(RestoreError::TargetNameMissing)?
        .to_string_lossy();
    for attempt in 0..128u16 {
        let candidate = parent.join(format!(
            ".{name}.restore-partial-{}-{created_unix_ms}-{attempt}",
            std::process::id()
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(RestoreError::StagingPathExhausted)
}

fn restore_into_staging(
    staging: &Path,
    backup: &NeuralBaseBackup,
    recovery_membership: &ClusterMembership,
    active_snapshot: &[u8],
    recovery_node_id: &str,
) -> Result<(), RestoreError> {
    let marker = staging.join(RESTORE_MARKER);
    write_restore_marker(
        &marker,
        "building",
        backup,
        recovery_membership,
        recovery_node_id,
    )?;

    {
        let engine = Arc::new(StorageEngine::open(staging)?);
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let manager = ReplicatedSqlSnapshotManager::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        );
        let restored = manager.restore(&backup.sql_snapshot)?;
        if restored != backup.manifest.metadata {
            return Err(RestoreError::Verification(
                "logical snapshot restore returned different metadata".to_string(),
            ));
        }

        let mut persistent = PersistentState::new();
        persistent.current_term = backup.manifest.metadata.last_included_term;
        persistent.install_snapshot(
            backup.manifest.metadata.last_included_index,
            backup.manifest.metadata.last_included_term,
        );
        persistent.membership = Some(recovery_membership.clone());
        let raft_store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
        raft_store
            .save(&persistent, active_snapshot)
            .map_err(RestoreError::Raft)?;
        engine
            .db
            .flush_wal(true)
            .map_err(|error| RestoreError::Raft(format!("fsync restored RocksDB WAL: {error}")))?;
    }

    verify_staged_restore(staging, backup, recovery_membership, active_snapshot)?;
    write_restore_marker(
        &marker,
        "validated",
        backup,
        recovery_membership,
        recovery_node_id,
    )?;
    fs::remove_file(&marker)?;
    sync_parent_dir(staging)?;
    Ok(())
}

fn verify_staged_restore(
    staging: &Path,
    backup: &NeuralBaseBackup,
    recovery_membership: &ClusterMembership,
    expected_active_snapshot: &[u8],
) -> Result<(), RestoreError> {
    let engine = Arc::new(StorageEngine::open(staging)?);
    let raft_store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
    if raft_store
        .load_staged_snapshot()
        .map_err(RestoreError::Raft)?
        .is_some()
    {
        return Err(RestoreError::Verification(
            "restored target contains a staged Raft snapshot transition".to_string(),
        ));
    }
    let (persistent, active_snapshot) = raft_store
        .load()
        .map_err(RestoreError::Raft)?
        .ok_or_else(|| RestoreError::Verification("restored Raft state is missing".to_string()))?;
    let metadata = &backup.manifest.metadata;
    if persistent.snapshot_index != metadata.last_included_index
        || persistent.snapshot_term != metadata.last_included_term
        || persistent.current_term < metadata.last_included_term
        || persistent.last_log_index() != metadata.last_included_index
        || persistent.log.len() != 1
        || persistent.membership.as_ref() != Some(recovery_membership)
        || active_snapshot != expected_active_snapshot
    {
        return Err(RestoreError::Verification(
            "restored Raft boundary, membership, or active snapshot differs from recovery plan"
                .to_string(),
        ));
    }

    let catalog = Arc::new(InMemoryCatalog::default());
    let clock = Arc::new(HlcClock::new(500));
    let manager = ReplicatedSqlSnapshotManager::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    );
    let reexported = manager.export(metadata.last_included_index, metadata.last_included_term)?;
    if reexported != backup.sql_snapshot {
        return Err(RestoreError::Verification(
            "restored logical SQL/identity state does not re-export byte-identically".to_string(),
        ));
    }

    let state_machine = ReplicatedSqlStateMachine::new(engine, catalog, Arc::clone(&clock))?;
    let applied = state_machine.durable_state()?;
    if applied.last_applied_index != metadata.latest_sql_apply_index
        || applied.last_commit_ts != metadata.latest_commit_ts
    {
        return Err(RestoreError::Verification(
            "restored durable apply cursor differs from backup metadata".to_string(),
        ));
    }
    if metadata.latest_commit_ts != 0 && clock.tick().to_u64() <= metadata.latest_commit_ts {
        return Err(RestoreError::Verification(
            "restored HLC did not advance beyond the backed-up commit timestamp".to_string(),
        ));
    }
    Ok(())
}

fn write_restore_marker(
    path: &Path,
    state: &str,
    backup: &NeuralBaseBackup,
    membership: &ClusterMembership,
    recovery_node_id: &str,
) -> Result<(), RestoreError> {
    let content = format!(
        "{RESTORE_MARKER_MAGIC}\nstate={state}\nsource_created_unix_ms={}\nboundary_index={}\nboundary_term={}\nrecovery_node_id={}\nrecovery_generation={}\n",
        backup.manifest.created_unix_ms,
        backup.manifest.metadata.last_included_index,
        backup.manifest.metadata.last_included_term,
        recovery_node_id,
        membership.generation,
    );
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<(), RestoreError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> Result<(), RestoreError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, TableSchema};
    use crate::replicated_identity::{ReplicatedIdentityUser, ReplicatedScramCredential};
    use crate::replicated_identity_snapshot::ReplicatedIdentitySnapshotExtension;
    use crate::replicated_identity_store::ReplicatedIdentityState;
    use crate::replicated_snapshot::{
        ReplicatedSqlSnapshot, SnapshotMetadata, SnapshotRow, SnapshotTable,
    };
    use crate::storage_executor::table_id_for;
    use tempfile::TempDir;

    fn fixture_backup() -> NeuralBaseBackup {
        let schema = TableSchema {
            name: "items".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
            }],
        };
        let identity = ReplicatedIdentityState::new(vec![ReplicatedIdentityUser {
            username: "alice".to_string(),
            credential: ReplicatedScramCredential {
                salt: vec![7; 16],
                iterations: 4_096,
                stored_key: [8; 32],
                server_key: [9; 32],
            },
        }])
        .unwrap();
        let snapshot = ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: 7,
                last_included_term: 3,
                latest_sql_apply_index: 6,
                latest_commit_ts: 2_000_000_000_000u64 << 16,
            },
            tables: vec![SnapshotTable {
                table_id: table_id_for(&schema.name),
                schema,
                rows: vec![SnapshotRow {
                    primary_key: b"row-1".to_vec(),
                    value: b"encoded-row-value".to_vec(),
                }],
            }],
            metadata_extension: ReplicatedIdentitySnapshotExtension::Initialized(identity)
                .encode()
                .unwrap(),
        }
        .encode()
        .unwrap();
        let mut source_membership = ClusterMembership::bootstrap(
            "old-a".to_string(),
            ["old-b".to_string(), "old-c".to_string()],
        );
        source_membership.generation = 9;
        NeuralBaseBackup::new_offline(1234, source_membership, snapshot).unwrap()
    }

    fn write_backup(root: &TempDir, backup: &NeuralBaseBackup) -> PathBuf {
        let path = root.path().join("source.nbbk");
        fs::write(&path, backup.encode().unwrap()).unwrap();
        path
    }

    #[test]
    fn restore_preserves_logical_state_identity_boundary_and_hlc_floor() {
        let root = TempDir::new().unwrap();
        let backup = fixture_backup();
        let backup_path = write_backup(&root, &backup);
        let target = root.path().join("recovered-db");

        let report = restore_new_cluster(&backup_path, &target, "recovery-1").unwrap();
        assert_eq!(report.boundary_index, 7);
        assert_eq!(report.boundary_term, 3);
        assert_eq!(report.recovery_membership_generation, 10);
        assert!(target.join("CURRENT").is_file());
        assert!(!target.join(RESTORE_MARKER).exists());

        let engine = Arc::new(StorageEngine::open(&target).unwrap());
        let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
        let (mut persistent, _) = store.load().unwrap().unwrap();
        let membership = persistent.membership.as_ref().unwrap();
        assert_eq!(
            membership.voters,
            BTreeSet::from(["recovery-1".to_string()])
        );
        assert!(membership.removed.contains("old-a"));
        assert!(membership.removed.contains("old-b"));
        assert!(membership.removed.contains("old-c"));
        assert_eq!(persistent.current_term, 3);
        assert_eq!(persistent.append(4, b"next".to_vec()), 8);

        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let manager = ReplicatedSqlSnapshotManager::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&clock),
        );
        assert_eq!(manager.export(7, 3).unwrap(), backup.sql_snapshot);
        let state_machine =
            ReplicatedSqlStateMachine::new(engine, catalog, Arc::clone(&clock)).unwrap();
        let applied = state_machine.durable_state().unwrap();
        assert_eq!(applied.last_applied_index, 6);
        assert_eq!(applied.last_commit_ts, 2_000_000_000_000u64 << 16);
        assert!(clock.tick().to_u64() > applied.last_commit_ts);
    }

    #[test]
    fn restore_requires_a_fresh_recovery_node_identity() {
        let root = TempDir::new().unwrap();
        let backup = fixture_backup();
        let backup_path = write_backup(&root, &backup);
        let target = root.path().join("recovered-db");

        let error = restore_new_cluster(&backup_path, &target, "old-b").unwrap_err();
        assert!(matches!(error, RestoreError::ReusedRecoveryNodeId(_)));
        assert!(!target.exists());
    }

    #[test]
    fn existing_target_is_never_modified() {
        let root = TempDir::new().unwrap();
        let backup = fixture_backup();
        let backup_path = write_backup(&root, &backup);
        let target = root.path().join("recovered-db");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("keep"), b"unchanged").unwrap();

        let error = restore_new_cluster(&backup_path, &target, "recovery-1").unwrap_err();
        assert!(matches!(error, RestoreError::TargetExists(_)));
        assert_eq!(fs::read(target.join("keep")).unwrap(), b"unchanged");
    }

    #[test]
    fn corrupt_backup_never_publishes_a_target() {
        let root = TempDir::new().unwrap();
        let backup_path = root.path().join("broken.nbbk");
        fs::write(&backup_path, b"NBBK-broken").unwrap();
        let target = root.path().join("recovered-db");

        assert!(restore_new_cluster(&backup_path, &target, "recovery-1").is_err());
        assert!(!target.exists());
    }
}
