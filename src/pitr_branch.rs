// SPDX-License-Identifier: Apache-2.0
//! Safe archive timeline initialization after recovery to an earlier point.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::backup::NeuralBaseBackup;
use crate::pitr::{ArchiveHash, ARCHIVE_TIMELINE_BYTES};
use crate::pitr_archive::{
    ArchiveStatus, ArchiveStreamMetadata, PitrArchiveError, PitrArchiveKey, PitrArchiveWriter,
};

const METADATA_FILE: &str = "stream.json";
const SEGMENTS_DIR: &str = "segments";
const STAGING_DIR: &str = ".staging";

/// Initialize a new archive namespace rooted at a recovered point on `parent`.
///
/// `backup` must be a fresh verified backup of the already-recovered cluster at
/// exactly `branch_index`. The new stream therefore has its own recoverable base
/// and timeline namespace; parent segments after the branch point can never join
/// the new chain because both timeline and baseline anchor differ.
pub fn initialize_branch_stream(
    root: &Path,
    parent: &PitrArchiveWriter,
    branch_index: u64,
    backup: &NeuralBaseBackup,
    baseline_artifact_bytes: &[u8],
    key: Option<PitrArchiveKey>,
) -> Result<ArchiveStatus, PitrBranchError> {
    parent.verify_target(branch_index)?;
    if backup.manifest.metadata.last_included_index != branch_index {
        return Err(PitrBranchError::BaselineIndexMismatch {
            expected: branch_index,
            actual: backup.manifest.metadata.last_included_index,
        });
    }
    let expected_term = if branch_index == parent.metadata().baseline_index {
        parent.metadata().baseline_term
    } else {
        parent.read_segment(branch_index)?.record.term
    };
    if backup.manifest.metadata.last_included_term != expected_term {
        return Err(PitrBranchError::BaselineTermMismatch {
            expected: expected_term,
            actual: backup.manifest.metadata.last_included_term,
        });
    }
    if root.exists() {
        if !root.is_dir() || fs::read_dir(root)?.next().transpose()?.is_some() {
            return Err(PitrBranchError::DestinationNotEmpty(root.to_path_buf()));
        }
    } else {
        fs::create_dir(root)?;
    }
    set_directory_permissions(root)?;
    fs::create_dir(root.join(SEGMENTS_DIR))?;
    fs::create_dir(root.join(STAGING_DIR))?;
    set_directory_permissions(&root.join(SEGMENTS_DIR))?;
    set_directory_permissions(&root.join(STAGING_DIR))?;

    let mut timeline = [0u8; ARCHIVE_TIMELINE_BYTES];
    OsRng
        .try_fill_bytes(&mut timeline)
        .map_err(|_| PitrBranchError::RandomFailure)?;
    if timeline == [0u8; ARCHIVE_TIMELINE_BYTES] {
        timeline[0] = 1;
    }
    if timeline == parent.metadata().timeline {
        timeline[0] ^= 0x80;
        if timeline == [0u8; ARCHIVE_TIMELINE_BYTES] {
            timeline[0] = 1;
        }
    }
    let baseline_hash: ArchiveHash = Sha256::digest(baseline_artifact_bytes).into();
    let mut metadata = ArchiveStreamMetadata::from_backup(
        backup,
        baseline_hash,
        timeline,
        key.is_some(),
        key.as_ref().map(PitrArchiveKey::key_id),
    )?;
    metadata.parent_timeline = Some(parent.metadata().timeline);
    metadata.branch_index = Some(branch_index);
    metadata.validate()?;
    publish_metadata(root, &metadata)?;

    let writer = PitrArchiveWriter::open(root, key)?;
    let status = writer.status();
    if status.metadata.parent_timeline != Some(parent.metadata().timeline)
        || status.metadata.branch_index != Some(branch_index)
        || status.frontier.index != branch_index
    {
        return Err(PitrBranchError::Verification(
            "published branch metadata did not reopen with expected lineage".into(),
        ));
    }
    Ok(status)
}

fn publish_metadata(root: &Path, metadata: &ArchiveStreamMetadata) -> Result<(), PitrBranchError> {
    let final_path = root.join(METADATA_FILE);
    if final_path.exists() {
        return Err(PitrBranchError::MetadataAlreadyExists(final_path));
    }
    let mut bytes = serde_json::to_vec(metadata)
        .map_err(|error| PitrBranchError::MetadataEncoding(error.to_string()))?;
    bytes.push(b'\n');
    let staged = root.join(format!(
        ".{METADATA_FILE}.branch-partial-{}",
        std::process::id()
    ));
    let result = (|| -> Result<(), PitrBranchError> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&staged)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        let check = fs::read(&staged)?;
        if check != bytes {
            return Err(PitrBranchError::Verification(
                "staged branch metadata changed before publication".into(),
            ));
        }
        fs::hard_link(&staged, &final_path)?;
        sync_dir(root)?;
        fs::remove_file(&staged)?;
        sync_dir(root)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staged);
    }
    result
}

fn sync_dir(path: &Path) -> Result<(), PitrBranchError> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn set_directory_permissions(path: &Path) -> Result<(), PitrBranchError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum PitrBranchError {
    #[error("parent archive failure: {0}")]
    Parent(#[from] PitrArchiveError),
    #[error("branch archive I/O failure: {0}")]
    Io(#[from] io::Error),
    #[error("branch archive destination is not empty: {0}")]
    DestinationNotEmpty(PathBuf),
    #[error("branch archive metadata already exists: {0}")]
    MetadataAlreadyExists(PathBuf),
    #[error("branch archive metadata serialization failed: {0}")]
    MetadataEncoding(String),
    #[error("secure random generation failed while creating branch timeline")]
    RandomFailure,
    #[error("branch backup boundary mismatch: expected {expected}, got {actual}")]
    BaselineIndexMismatch { expected: u64, actual: u64 },
    #[error("branch backup term mismatch: expected {expected}, got {actual}")]
    BaselineTermMismatch { expected: u64, actual: u64 },
    #[error("branch publication verification failed: {0}")]
    Verification(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::NeuralBaseBackup;
    use crate::consensus::{ClusterMembership, LogEntry};
    use crate::replicated_identity_snapshot::ReplicatedIdentitySnapshotExtension;
    use crate::replicated_snapshot::{ReplicatedSqlSnapshot, SnapshotMetadata};
    use tempfile::TempDir;

    fn backup(index: u64, term: u64, node: &str, generation: u64) -> NeuralBaseBackup {
        let snapshot = ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: index,
                last_included_term: term,
                latest_sql_apply_index: index,
                latest_commit_ts: 0,
            },
            tables: vec![],
            metadata_extension: ReplicatedIdentitySnapshotExtension::Uninitialized
                .encode()
                .unwrap(),
        }
        .encode()
        .unwrap();
        let mut membership = ClusterMembership::bootstrap(node.to_string(), Vec::<String>::new());
        membership.generation = generation;
        membership.config_index = index;
        NeuralBaseBackup::new_offline(1, membership, snapshot).unwrap()
    }

    #[test]
    fn branch_uses_new_timeline_and_records_parent_lineage() {
        let temp = TempDir::new().unwrap();
        let parent_root = temp.path().join("parent");
        let parent_backup = backup(5, 2, "old", 2);
        let parent_bytes = parent_backup.encode().unwrap();
        PitrArchiveWriter::initialize(&parent_root, &parent_backup, &parent_bytes, None).unwrap();
        let mut parent = PitrArchiveWriter::open(&parent_root, None).unwrap();
        parent
            .append_committed(&LogEntry {
                term: 3,
                index: 6,
                command: vec![],
            })
            .unwrap();
        parent
            .append_committed(&LogEntry {
                term: 3,
                index: 7,
                command: vec![],
            })
            .unwrap();

        let child_backup = backup(6, 3, "fresh", 4);
        let child_bytes = child_backup.encode().unwrap();
        let child_root = temp.path().join("child");
        let status =
            initialize_branch_stream(&child_root, &parent, 6, &child_backup, &child_bytes, None)
                .unwrap();
        assert_ne!(status.metadata.timeline, parent.metadata().timeline);
        assert_eq!(
            status.metadata.parent_timeline,
            Some(parent.metadata().timeline)
        );
        assert_eq!(status.metadata.branch_index, Some(6));
        assert_eq!(status.frontier.index, 6);

        let mut child = PitrArchiveWriter::open(&child_root, None).unwrap();
        child
            .append_committed(&LogEntry {
                term: 4,
                index: 7,
                command: vec![],
            })
            .unwrap();
        assert_eq!(child.status().frontier.index, 7);
    }

    #[test]
    fn branch_rejects_backup_not_captured_at_branch_point() {
        let temp = TempDir::new().unwrap();
        let parent_root = temp.path().join("parent");
        let parent_backup = backup(5, 2, "old", 2);
        let parent_bytes = parent_backup.encode().unwrap();
        PitrArchiveWriter::initialize(&parent_root, &parent_backup, &parent_bytes, None).unwrap();
        let parent = PitrArchiveWriter::open(&parent_root, None).unwrap();
        let bad = backup(4, 2, "fresh", 3);
        let bad_bytes = bad.encode().unwrap();
        assert!(matches!(
            initialize_branch_stream(
                &temp.path().join("child"),
                &parent,
                5,
                &bad,
                &bad_bytes,
                None
            ),
            Err(PitrBranchError::BaselineIndexMismatch { .. })
        ));
    }
}
