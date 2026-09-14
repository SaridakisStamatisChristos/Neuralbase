// SPDX-License-Identifier: Apache-2.0
//! Conservative Phase-9 archive retention.
//!
//! Archive v1 never trims the middle of a recovery chain. Retention is a
//! rollover operation: a replacement child timeline must already be fully
//! published and independently verified at the old stream's durable frontier.
//! Only then may the old stream be atomically moved out of service and deleted.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::pitr::TimelineId;
use crate::pitr_archive::{read_metadata, ArchiveStatus, PitrArchiveWriter};

const SEGMENTS_DIR: &str = "segments";
const STAGING_DIR: &str = ".staging";
const METADATA_FILE: &str = "stream.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionReport {
    pub retired_timeline: TimelineId,
    pub replacement_timeline: TimelineId,
    pub retired_frontier: u64,
    pub retired_segments: u64,
}

/// Retire an old archive stream only after a verified child stream replaces its
/// complete recovery prefix.
///
/// Callers must quiesce the source archive writer before invoking this function.
/// The implementation still defends against a late concurrent append: it first
/// atomically renames the old root, then rechecks the moved directory's immutable
/// metadata, exact segment sequence, and empty staging area. If anything changed,
/// the moved directory is preserved rather than deleted.
pub fn retire_replaced_stream(
    parent_root: &Path,
    parent: &PitrArchiveWriter,
    replacement: &PitrArchiveWriter,
) -> Result<RetentionReport, PitrRetentionError> {
    let parent_status = parent.status();
    let replacement_status = replacement.status();
    validate_replacement(&parent_status, &replacement_status)?;

    let parent_dir = parent_root
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = parent_root
        .file_name()
        .ok_or(PitrRetentionError::SourceNameMissing)?
        .to_string_lossy();
    let timeline_prefix = hex_prefix(&parent_status.metadata.timeline);
    let retired = parent_dir.join(format!(
        ".{name}.pitr-retired-{}-{timeline_prefix}",
        std::process::id()
    ));
    if retired.exists() {
        return Err(PitrRetentionError::RetiredPathExists(retired));
    }

    fs::rename(parent_root, &retired)?;
    sync_dir(parent_dir)?;

    // From this point onward failure is intentionally conservative: leave the
    // renamed source on disk so operator data is never destroyed on ambiguity.
    verify_moved_stream(&retired, &parent_status)?;
    fs::remove_dir_all(&retired)?;
    sync_dir(parent_dir)?;

    Ok(RetentionReport {
        retired_timeline: parent_status.metadata.timeline,
        replacement_timeline: replacement_status.metadata.timeline,
        retired_frontier: parent_status.frontier.index,
        retired_segments: parent_status.frontier.segments,
    })
}

fn validate_replacement(
    parent: &ArchiveStatus,
    replacement: &ArchiveStatus,
) -> Result<(), PitrRetentionError> {
    if replacement.metadata.parent_timeline != Some(parent.metadata.timeline) {
        return Err(PitrRetentionError::WrongParentTimeline);
    }
    if replacement.metadata.branch_index != Some(parent.frontier.index)
        || replacement.metadata.baseline_index != parent.frontier.index
    {
        return Err(PitrRetentionError::ReplacementBoundaryMismatch {
            expected: parent.frontier.index,
            branch: replacement.metadata.branch_index,
            baseline: replacement.metadata.baseline_index,
        });
    }
    if replacement.metadata.baseline_term != parent.frontier.term {
        return Err(PitrRetentionError::ReplacementTermMismatch {
            expected: parent.frontier.term,
            actual: replacement.metadata.baseline_term,
        });
    }
    if replacement.metadata.timeline == parent.metadata.timeline {
        return Err(PitrRetentionError::TimelineNotRotated);
    }
    Ok(())
}

fn verify_moved_stream(
    root: &Path,
    expected: &ArchiveStatus,
) -> Result<(), PitrRetentionError> {
    let metadata = read_metadata(root).map_err(|error| {
        PitrRetentionError::MovedVerification(format!("read moved metadata: {error}"))
    })?;
    if metadata != expected.metadata {
        return Err(PitrRetentionError::MovedVerification(
            "stream metadata changed during retirement".into(),
        ));
    }

    let staging = root.join(STAGING_DIR);
    if !staging.is_dir() {
        return Err(PitrRetentionError::MovedVerification(
            "staging directory is missing after retirement rename".into(),
        ));
    }
    if fs::read_dir(&staging)?.next().transpose()?.is_some() {
        return Err(PitrRetentionError::MovedVerification(
            "staging directory became non-empty during retirement".into(),
        ));
    }

    let segments = root.join(SEGMENTS_DIR);
    if !segments.is_dir() {
        return Err(PitrRetentionError::MovedVerification(
            "segments directory is missing after retirement rename".into(),
        ));
    }
    let suffix = if metadata.encrypted { ".nbpe" } else { ".nbar" };
    let mut indexes = Vec::new();
    for entry in fs::read_dir(&segments)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(PitrRetentionError::MovedVerification(format!(
                "unexpected non-file entry in retired segments: {}",
                entry.path().display()
            )));
        }
        let name = entry.file_name();
        let text = name.to_string_lossy();
        let Some(stem) = text.strip_suffix(suffix) else {
            return Err(PitrRetentionError::MovedVerification(format!(
                "unexpected file in retired segments: {}",
                entry.path().display()
            )));
        };
        if stem.len() != 20 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(PitrRetentionError::MovedVerification(format!(
                "invalid retired segment filename: {text}"
            )));
        }
        indexes.push(stem.parse::<u64>().map_err(|_| {
            PitrRetentionError::MovedVerification(format!(
                "invalid retired segment index: {text}"
            ))
        })?);
    }
    indexes.sort_unstable();
    if indexes.len() as u64 != expected.frontier.segments {
        return Err(PitrRetentionError::MovedVerification(format!(
            "retired segment count changed: expected {}, got {}",
            expected.frontier.segments,
            indexes.len()
        )));
    }
    for (offset, actual) in indexes.iter().enumerate() {
        let expected_index = expected
            .metadata
            .baseline_index
            .checked_add(offset as u64 + 1)
            .ok_or_else(|| {
                PitrRetentionError::MovedVerification("segment index overflow".into())
            })?;
        if *actual != expected_index {
            return Err(PitrRetentionError::MovedVerification(format!(
                "retired segment sequence changed: expected {expected_index}, got {actual}"
            )));
        }
    }
    if indexes.last().copied().unwrap_or(expected.metadata.baseline_index)
        != expected.frontier.index
    {
        return Err(PitrRetentionError::MovedVerification(format!(
            "retired frontier changed: expected {}, got {}",
            expected.frontier.index,
            indexes
                .last()
                .copied()
                .unwrap_or(expected.metadata.baseline_index)
        )));
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let text = name.to_string_lossy();
        if text != METADATA_FILE && text != SEGMENTS_DIR && text != STAGING_DIR {
            return Err(PitrRetentionError::MovedVerification(format!(
                "unexpected top-level retired archive entry: {}",
                entry.path().display()
            )));
        }
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<(), PitrRetentionError> {
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

fn hex_prefix(timeline: &TimelineId) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(8);
    for byte in timeline.iter().take(4) {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

#[derive(Debug, Error)]
pub enum PitrRetentionError {
    #[error("replacement stream does not name the retired stream as its parent timeline")]
    WrongParentTimeline,
    #[error(
        "replacement stream boundary mismatch: expected {expected}, branch={branch:?}, baseline={baseline}"
    )]
    ReplacementBoundaryMismatch {
        expected: u64,
        branch: Option<u64>,
        baseline: u64,
    },
    #[error("replacement stream term mismatch: expected {expected}, got {actual}")]
    ReplacementTermMismatch { expected: u64, actual: u64 },
    #[error("replacement stream did not rotate to a new timeline")]
    TimelineNotRotated,
    #[error("archive source path must name a directory")]
    SourceNameMissing,
    #[error("retirement staging path already exists: {0}")]
    RetiredPathExists(PathBuf),
    #[error("retired archive changed or could not be proven safe to delete: {0}")]
    MovedVerification(String),
    #[error("archive retention I/O failure: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::NeuralBaseBackup;
    use crate::consensus::{ClusterMembership, LogEntry};
    use crate::pitr_branch::initialize_branch_stream;
    use crate::pitr_archive::PitrArchiveWriter;
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
    fn retirement_requires_verified_replacement_at_exact_frontier() {
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

        let child_backup = backup(6, 3, "fresh", 4);
        let child_bytes = child_backup.encode().unwrap();
        let child_root = temp.path().join("child");
        initialize_branch_stream(
            &child_root,
            &parent,
            6,
            &child_backup,
            &child_bytes,
            None,
        )
        .unwrap();
        let child = PitrArchiveWriter::open(&child_root, None).unwrap();
        let report = retire_replaced_stream(&parent_root, &parent, &child).unwrap();
        assert_eq!(report.retired_frontier, 6);
        assert!(!parent_root.exists());
        assert!(child_root.exists());
    }

    #[test]
    fn retirement_rejects_replacement_behind_parent_frontier() {
        let temp = TempDir::new().unwrap();
        let parent_root = temp.path().join("parent");
        let parent_backup = backup(5, 2, "old", 2);
        let parent_bytes = parent_backup.encode().unwrap();
        PitrArchiveWriter::initialize(&parent_root, &parent_backup, &parent_bytes, None).unwrap();
        let mut parent = PitrArchiveWriter::open(&parent_root, None).unwrap();
        let child_backup = backup(5, 2, "fresh", 3);
        let child_bytes = child_backup.encode().unwrap();
        let child_root = temp.path().join("child");
        initialize_branch_stream(
            &child_root,
            &parent,
            5,
            &child_backup,
            &child_bytes,
            None,
        )
        .unwrap();
        let child = PitrArchiveWriter::open(&child_root, None).unwrap();
        parent
            .append_committed(&LogEntry {
                term: 3,
                index: 6,
                command: vec![],
            })
            .unwrap();
        assert!(matches!(
            retire_replaced_stream(&parent_root, &parent, &child),
            Err(PitrRetentionError::ReplacementBoundaryMismatch { .. })
        ));
        assert!(parent_root.exists());
    }
}
