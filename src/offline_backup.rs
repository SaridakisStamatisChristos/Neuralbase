// SPDX-License-Identifier: Apache-2.0
//! Offline operator backup creation and independent verification.
//!
//! This path deliberately opens RocksDB itself. RocksDB's exclusive database
//! lock therefore makes "offline" an executable property: creation fails if a
//! NeuralBase process still owns the database. The backup path performs no
//! application-level writes to the source state.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::backup::{BackupCodecError, BackupManifest, NeuralBaseBackup, MAX_BACKUP_BYTES};
use crate::catalog::InMemoryCatalog;
use crate::consensus::{decode_snapshot_payload, RaftPersistenceStore};
use crate::hlc::HlcClock;
use crate::raft_persistence::RocksDbRaftPersistenceStore;
use crate::replicated_snapshot::ReplicatedSqlSnapshot;
use crate::replicated_snapshot_manager::{ReplicatedSqlSnapshotManager, SnapshotManagerError};
use crate::replicated_state_machine::{ReplicatedSqlApplyError, ReplicatedSqlStateMachine};
use crate::storage::{StorageEngine, StorageError};

#[derive(Debug, Error)]
pub enum OfflineBackupError {
    #[error("backup source does not exist: {0}")]
    SourceMissing(PathBuf),
    #[error("backup source is not a directory: {0}")]
    SourceNotDirectory(PathBuf),
    #[error("backup source does not look like an existing RocksDB database: {0}")]
    SourceNotRocksDb(PathBuf),
    #[error("backup destination must have an existing parent directory")]
    DestinationParentMissing,
    #[error("backup destination must name a file")]
    DestinationFileNameMissing,
    #[error("backup destination may not be inside the source database directory")]
    DestinationInsideSource,
    #[error("backup destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("open source storage for offline backup: {0}")]
    Storage(#[from] StorageError),
    #[error("read durable Raft state for offline backup: {0}")]
    Raft(String),
    #[error("offline backup requires an initialized durable Raft state")]
    MissingRaftState,
    #[error("offline backup requires committed Phase-3 membership in durable Raft state")]
    MissingMembership,
    #[error("refusing offline backup while a staged Raft snapshot transition is present")]
    StagedRaftSnapshot,
    #[error("source Raft snapshot boundary and active snapshot bytes are inconsistent")]
    InconsistentActiveRaftSnapshot,
    #[error("source active Raft snapshot is invalid: {0}")]
    InvalidActiveRaftSnapshot(String),
    #[error("read durable replicated apply state for offline backup: {0}")]
    ApplyState(#[from] ReplicatedSqlApplyError),
    #[error("committed membership index {membership_index} is newer than recoverable state boundary {boundary}")]
    MembershipBeyondRecoverableBoundary {
        membership_index: u64,
        boundary: u64,
    },
    #[error("recoverable state boundary {boundary} is beyond the durable Raft log/snapshot frontier {last_log_index}")]
    BoundaryBeyondRaftLog { boundary: u64, last_log_index: u64 },
    #[error("recoverable state boundary {0} has no durable Raft term")]
    MissingBoundaryTerm(u64),
    #[error("export logical state for offline backup: {0}")]
    Snapshot(#[from] SnapshotManagerError),
    #[error("encode or verify offline backup: {0}")]
    Codec(#[from] BackupCodecError),
    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,
    #[error("backup timestamp does not fit milliseconds since Unix epoch")]
    ClockOverflow,
    #[error("backup I/O failure: {0}")]
    Io(#[from] io::Error),
    #[error("backup file exceeds {MAX_BACKUP_BYTES} bytes")]
    FileTooLarge,
}

/// Create one offline backup and atomically publish it at `destination`.
///
/// The destination is never overwritten. Bytes are written to a restrictive
/// temporary file in the same directory, fsynced, independently decoded and
/// validated, then published with a no-overwrite hard link. The parent
/// directory is fsynced on Unix before success is reported.
pub fn create_offline_backup(
    db_path: &Path,
    destination: &Path,
) -> Result<BackupManifest, OfflineBackupError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OfflineBackupError::ClockBeforeEpoch)?;
    let created_unix_ms =
        u64::try_from(duration.as_millis()).map_err(|_| OfflineBackupError::ClockOverflow)?;
    create_offline_backup_at(db_path, destination, created_unix_ms)
}

/// Deterministic timestamp variant used by executable tests.
pub fn create_offline_backup_at(
    db_path: &Path,
    destination: &Path,
    created_unix_ms: u64,
) -> Result<BackupManifest, OfflineBackupError> {
    validate_paths(db_path, destination)?;
    if destination.exists() {
        return Err(OfflineBackupError::DestinationExists(
            destination.to_path_buf(),
        ));
    }

    // Opening the same RocksDB from another NeuralBase process fails its
    // exclusive lock. This is the enforcement boundary for "offline".
    let engine = Arc::new(StorageEngine::open(db_path)?);
    let raft_store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
    if raft_store
        .load_staged_snapshot()
        .map_err(OfflineBackupError::Raft)?
        .is_some()
    {
        return Err(OfflineBackupError::StagedRaftSnapshot);
    }
    let (persistent, active_snapshot) = raft_store
        .load()
        .map_err(OfflineBackupError::Raft)?
        .ok_or(OfflineBackupError::MissingRaftState)?;
    let membership = persistent
        .membership
        .clone()
        .ok_or(OfflineBackupError::MissingMembership)?;
    membership.validate().map_err(|error| {
        OfflineBackupError::Raft(format!("invalid committed membership: {error}"))
    })?;

    validate_active_snapshot(&persistent, &active_snapshot)?;

    let catalog = Arc::new(InMemoryCatalog::default());
    let clock = Arc::new(HlcClock::new(500));
    let state_machine = ReplicatedSqlStateMachine::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    )?;
    let applied = state_machine.durable_state()?;

    // An older compatible SQL snapshot may have an apply marker below its Raft
    // boundary when the omitted entries were non-SQL controls. The active
    // snapshot boundary is nevertheless already-proven applied Raft state.
    let boundary = applied.last_applied_index.max(persistent.snapshot_index);
    if membership.config_index > boundary {
        return Err(OfflineBackupError::MembershipBeyondRecoverableBoundary {
            membership_index: membership.config_index,
            boundary,
        });
    }
    let last_log_index = persistent.last_log_index();
    if boundary > last_log_index {
        return Err(OfflineBackupError::BoundaryBeyondRaftLog {
            boundary,
            last_log_index,
        });
    }
    let boundary_term = persistent.term_at(boundary);
    if boundary == 0 {
        if boundary_term != 0 {
            return Err(OfflineBackupError::MissingBoundaryTerm(boundary));
        }
    } else if boundary_term == 0 {
        return Err(OfflineBackupError::MissingBoundaryTerm(boundary));
    }

    let manager = ReplicatedSqlSnapshotManager::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    );
    let sql_snapshot = manager.export(boundary, boundary_term)?;
    let backup = NeuralBaseBackup::new_offline(created_unix_ms, membership, sql_snapshot)?;
    let encoded = backup.encode()?;

    publish_atomically(destination, &encoded, created_unix_ms)?;
    Ok(backup.manifest)
}

/// Verify a backup independently of any database target or restore operation.
pub fn verify_backup_file(path: &Path) -> Result<NeuralBaseBackup, OfflineBackupError> {
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    let max = u64::try_from(MAX_BACKUP_BYTES).unwrap_or(u64::MAX);
    if metadata.len() > max {
        return Err(OfflineBackupError::FileTooLarge);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BACKUP_BYTES {
        return Err(OfflineBackupError::FileTooLarge);
    }
    NeuralBaseBackup::decode(&bytes).map_err(Into::into)
}

fn validate_paths(db_path: &Path, destination: &Path) -> Result<(), OfflineBackupError> {
    if !db_path.exists() {
        return Err(OfflineBackupError::SourceMissing(db_path.to_path_buf()));
    }
    if !db_path.is_dir() {
        return Err(OfflineBackupError::SourceNotDirectory(
            db_path.to_path_buf(),
        ));
    }
    // `StorageEngine::open` has create-if-missing semantics. Require RocksDB's
    // existing-database marker so a typo can never create and "back up" a new DB.
    if !db_path.join("CURRENT").is_file() {
        return Err(OfflineBackupError::SourceNotRocksDb(db_path.to_path_buf()));
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(OfflineBackupError::DestinationParentMissing);
    }
    if destination.file_name().is_none() {
        return Err(OfflineBackupError::DestinationFileNameMissing);
    }

    let source = fs::canonicalize(db_path)?;
    let destination_parent = fs::canonicalize(parent)?;
    if destination_parent.starts_with(&source) {
        return Err(OfflineBackupError::DestinationInsideSource);
    }
    Ok(())
}

fn validate_active_snapshot(
    persistent: &crate::consensus::PersistentState,
    active_snapshot: &[u8],
) -> Result<(), OfflineBackupError> {
    let has_boundary = persistent.snapshot_index != 0;
    let has_bytes = !active_snapshot.is_empty();
    if has_boundary != has_bytes {
        return Err(OfflineBackupError::InconsistentActiveRaftSnapshot);
    }
    if !has_boundary {
        return Ok(());
    }

    let sql_bytes = match decode_snapshot_payload(active_snapshot)
        .map_err(OfflineBackupError::InvalidActiveRaftSnapshot)?
    {
        Some((snapshot_membership, sql)) => {
            if snapshot_membership.config_index > persistent.snapshot_index {
                return Err(OfflineBackupError::InvalidActiveRaftSnapshot(format!(
                    "snapshot membership config index {} exceeds boundary {}",
                    snapshot_membership.config_index, persistent.snapshot_index
                )));
            }
            if let Some(current_membership) = persistent.membership.as_ref() {
                if current_membership.config_index <= persistent.snapshot_index
                    && current_membership != &snapshot_membership
                {
                    return Err(OfflineBackupError::InvalidActiveRaftSnapshot(
                        "durable membership conflicts with membership embedded in active snapshot"
                            .to_string(),
                    ));
                }
            }
            sql
        }
        None => active_snapshot,
    };
    let decoded = ReplicatedSqlSnapshot::decode(sql_bytes)
        .map_err(|error| OfflineBackupError::InvalidActiveRaftSnapshot(error.to_string()))?;
    if decoded.metadata.last_included_index != persistent.snapshot_index
        || decoded.metadata.last_included_term != persistent.snapshot_term
    {
        return Err(OfflineBackupError::InvalidActiveRaftSnapshot(format!(
            "embedded boundary index={} term={} differs from durable index={} term={}",
            decoded.metadata.last_included_index,
            decoded.metadata.last_included_term,
            persistent.snapshot_index,
            persistent.snapshot_term
        )));
    }
    Ok(())
}

fn publish_atomically(
    destination: &Path,
    bytes: &[u8],
    created_unix_ms: u64,
) -> Result<(), OfflineBackupError> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = destination
        .file_name()
        .ok_or(OfflineBackupError::DestinationFileNameMissing)?
        .to_string_lossy();
    let staged = parent.join(format!(
        ".{file_name}.partial-{}-{created_unix_ms}",
        std::process::id()
    ));

    let result = (|| -> Result<(), OfflineBackupError> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&staged)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);

        // Verification is intentionally a fresh read/strict decode of staged
        // bytes, not trust in the in-memory object that produced them.
        verify_backup_file(&staged)?;

        match fs::hard_link(&staged, destination) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(OfflineBackupError::DestinationExists(
                    destination.to_path_buf(),
                ));
            }
            Err(error) => return Err(error.into()),
        }
        sync_parent_dir(parent)?;
        fs::remove_file(&staged)?;
        sync_parent_dir(parent)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&staged);
    }
    result
}

#[cfg(unix)]
fn sync_parent_dir(parent: &Path) -> Result<(), OfflineBackupError> {
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_parent: &Path) -> Result<(), OfflineBackupError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::{
        encode_snapshot_payload, ClusterMembership, PersistentState, StagedSnapshot,
        StagedSnapshotKind,
    };
    use crate::replicated_identity_snapshot::ReplicatedIdentitySnapshotExtension;
    use crate::replicated_snapshot::SnapshotMetadata;
    use tempfile::TempDir;

    fn initialize_source(root: &TempDir) -> PathBuf {
        let db_path = root.path().join("db");
        let engine = Arc::new(StorageEngine::open(&db_path).unwrap());
        let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
        let mut persistent = PersistentState::new();
        persistent.membership = Some(ClusterMembership::bootstrap(
            "n1".to_string(),
            Vec::<String>::new(),
        ));
        store.save(&persistent, b"").unwrap();
        drop(store);
        drop(engine);
        db_path
    }

    fn empty_sql_snapshot(boundary: u64, term: u64) -> Vec<u8> {
        ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: boundary,
                last_included_term: term,
                latest_sql_apply_index: 0,
                latest_commit_ts: 0,
            },
            tables: vec![],
            metadata_extension: ReplicatedIdentitySnapshotExtension::Uninitialized
                .encode()
                .unwrap(),
        }
        .encode()
        .unwrap()
    }

    #[test]
    fn offline_backup_is_published_only_after_strict_verification() {
        let source_root = TempDir::new().unwrap();
        let output_root = TempDir::new().unwrap();
        let db_path = initialize_source(&source_root);
        let destination = output_root.path().join("cluster.nbbk");

        let manifest = create_offline_backup_at(&db_path, &destination, 1234).unwrap();
        assert_eq!(manifest.created_unix_ms, 1234);
        let verified = verify_backup_file(&destination).unwrap();
        assert_eq!(verified.manifest, manifest);
        assert_eq!(verified.membership.voters.len(), 1);
        assert!(output_root.path().read_dir().unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("partial")));
    }

    #[test]
    fn existing_destination_is_never_overwritten() {
        let source_root = TempDir::new().unwrap();
        let output_root = TempDir::new().unwrap();
        let db_path = initialize_source(&source_root);
        let destination = output_root.path().join("cluster.nbbk");
        fs::write(&destination, b"keep-me").unwrap();

        let error = create_offline_backup_at(&db_path, &destination, 1234).unwrap_err();
        assert!(matches!(error, OfflineBackupError::DestinationExists(_)));
        assert_eq!(fs::read(&destination).unwrap(), b"keep-me");
    }

    #[test]
    fn staged_raft_transition_fails_closed() {
        let source_root = TempDir::new().unwrap();
        let output_root = TempDir::new().unwrap();
        let db_path = initialize_source(&source_root);
        let engine = Arc::new(StorageEngine::open(&db_path).unwrap());
        let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
        store
            .stage_snapshot(&StagedSnapshot {
                kind: StagedSnapshotKind::Creation,
                last_included_index: 0,
                last_included_term: 0,
                data: Arc::new(Vec::new()),
            })
            .unwrap();
        drop(store);
        drop(engine);

        let error =
            create_offline_backup_at(&db_path, &output_root.path().join("cluster.nbbk"), 1234)
                .unwrap_err();
        assert!(matches!(error, OfflineBackupError::StagedRaftSnapshot));
    }

    #[test]
    fn conflicting_active_snapshot_membership_fails_closed() {
        let source_root = TempDir::new().unwrap();
        let output_root = TempDir::new().unwrap();
        let db_path = initialize_source(&source_root);
        let engine = Arc::new(StorageEngine::open(&db_path).unwrap());
        let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));

        let current_membership =
            ClusterMembership::bootstrap("n1".to_string(), Vec::<String>::new());
        let snapshot_membership =
            ClusterMembership::bootstrap("other".to_string(), Vec::<String>::new());
        let mut persistent = PersistentState::new();
        persistent.install_snapshot(5, 2);
        persistent.membership = Some(current_membership);
        let sql_snapshot = empty_sql_snapshot(5, 2);
        let active_snapshot = encode_snapshot_payload(&snapshot_membership, &sql_snapshot).unwrap();
        store.save(&persistent, &active_snapshot).unwrap();
        drop(store);
        drop(engine);

        let error =
            create_offline_backup_at(&db_path, &output_root.path().join("cluster.nbbk"), 1234)
                .unwrap_err();
        assert!(matches!(
            error,
            OfflineBackupError::InvalidActiveRaftSnapshot(_)
        ));
    }

    #[test]
    fn malformed_or_incomplete_artifact_never_verifies() {
        let output_root = TempDir::new().unwrap();
        let path = output_root.path().join("broken.nbbk");
        fs::write(&path, b"NBBK\x01").unwrap();
        assert!(verify_backup_file(&path).is_err());
    }

    #[test]
    fn typo_source_is_not_created_as_an_empty_database() {
        let root = TempDir::new().unwrap();
        let missing = root.path().join("typo");
        let output = root.path().join("backup.nbbk");
        let error = create_offline_backup_at(&missing, &output, 1234).unwrap_err();
        assert!(matches!(error, OfflineBackupError::SourceMissing(_)));
        assert!(!missing.exists());
    }

    #[cfg(unix)]
    #[test]
    fn published_backup_permissions_are_restrictive() {
        use std::os::unix::fs::PermissionsExt;

        let source_root = TempDir::new().unwrap();
        let output_root = TempDir::new().unwrap();
        let db_path = initialize_source(&source_root);
        let destination = output_root.path().join("cluster.nbbk");
        create_offline_backup_at(&db_path, &destination, 1234).unwrap();
        let mode = fs::metadata(&destination).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
