// SPDX-License-Identifier: Apache-2.0
//! Deterministic Phase-9 recovery from a verified Phase-5 baseline plus archive.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::backup::NeuralBaseBackup;
use crate::catalog::InMemoryCatalog;
use crate::consensus::{
    encode_snapshot_payload, ClusterMembership, MembershipChange, PersistentState,
    RaftPersistenceStore, MEMBERSHIP_FORMAT_VERSION,
};
use crate::hlc::HlcClock;
use crate::pitr::{ArchiveCodecError, ArchiveHash, TimelineId};
use crate::pitr_archive::{ArchiveStreamMetadata, PitrArchiveError, PitrArchiveWriter};
use crate::raft_persistence::RocksDbRaftPersistenceStore;
use crate::replicated_snapshot_manager::{ReplicatedSqlSnapshotManager, SnapshotManagerError};
use crate::replicated_state_machine::{ReplicatedSqlApplyError, ReplicatedSqlStateMachine};
use crate::storage::{StorageEngine, StorageError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryTarget {
    Baseline,
    Index(u64),
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PitrRecoveryReport {
    pub source_timeline: TimelineId,
    pub source_baseline_index: u64,
    pub target_index: u64,
    pub target_term: u64,
    pub recovery_node_id: String,
    pub recovery_membership_generation: u64,
    pub replayed_records: u64,
}

pub fn recover_verified_new_cluster(
    backup: &NeuralBaseBackup,
    baseline_artifact_bytes: &[u8],
    archive: &PitrArchiveWriter,
    target: RecoveryTarget,
    destination: &Path,
    recovery_node_id: &str,
) -> Result<PitrRecoveryReport, PitrReplayError> {
    validate_target(destination)?;
    validate_baseline_binding(backup, baseline_artifact_bytes, archive.metadata())?;
    let status = archive.status();
    let target_index = match target {
        RecoveryTarget::Baseline => status.metadata.baseline_index,
        RecoveryTarget::Index(index) => index,
        RecoveryTarget::Latest => status.frontier.index,
    };
    if target_index < status.metadata.baseline_index || target_index > status.frontier.index {
        return Err(PitrReplayError::TargetUnavailable {
            baseline: status.metadata.baseline_index,
            frontier: status.frontier.index,
            target: target_index,
        });
    }

    let staging = allocate_staging_path(destination)?;
    fs::create_dir(&staging)?;
    let result = recover_into_staging(
        backup,
        archive,
        target_index,
        &staging,
        recovery_node_id,
    );
    let report = match result {
        Ok(report) => report,
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
    };

    if destination.exists() {
        let _ = fs::remove_dir_all(&staging);
        return Err(PitrReplayError::TargetAppeared(destination.to_path_buf()));
    }
    if let Err(error) = fs::rename(&staging, destination) {
        let _ = fs::remove_dir_all(&staging);
        return Err(error.into());
    }
    sync_dir(
        destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?;
    Ok(report)
}

fn recover_into_staging(
    backup: &NeuralBaseBackup,
    archive: &PitrArchiveWriter,
    target_index: u64,
    staging: &Path,
    recovery_node_id: &str,
) -> Result<PitrRecoveryReport, PitrReplayError> {
    let metadata = archive.metadata();
    let mut source_membership = backup.membership.clone();
    let mut target_term = backup.manifest.metadata.last_included_term;
    let mut replayed_records = 0u64;

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
        return Err(PitrReplayError::Verification(
            "baseline logical snapshot restored different metadata".into(),
        ));
    }
    let state_machine = ReplicatedSqlStateMachine::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    )?;

    let mut previous_hash = metadata.chain_spec().baseline_anchor()?;
    let mut expected_index = metadata
        .baseline_index
        .checked_add(1)
        .ok_or(PitrReplayError::IndexOverflow)?;
    while expected_index <= target_index {
        let segment = archive.read_segment(expected_index)?;
        if segment.timeline != metadata.timeline {
            return Err(PitrReplayError::Verification(format!(
                "segment {expected_index} belongs to another timeline"
            )));
        }
        if segment.record.index != expected_index {
            return Err(PitrReplayError::Verification(format!(
                "segment index mismatch: expected {expected_index}, got {}",
                segment.record.index
            )));
        }
        if segment.previous_hash != previous_hash {
            return Err(PitrReplayError::Verification(format!(
                "archive chain link mismatch at index {expected_index}"
            )));
        }

        state_machine.apply_log_entry(&segment.record.to_log_entry())?;
        if let Some(change) = segment.record.membership_change()? {
            source_membership = transition_membership(&source_membership, &change, expected_index)?;
        }
        target_term = segment.record.term;
        previous_hash = segment.hash()?;
        replayed_records = replayed_records
            .checked_add(1)
            .ok_or(PitrReplayError::IndexOverflow)?;
        expected_index = expected_index
            .checked_add(1)
            .ok_or(PitrReplayError::IndexOverflow)?;
    }

    let recovery_membership =
        build_recovery_membership(&source_membership, target_index, recovery_node_id)?;
    let logical_snapshot = manager.export(target_index, target_term)?;
    let active_snapshot = encode_snapshot_payload(&recovery_membership, &logical_snapshot)
        .map_err(PitrReplayError::SnapshotEnvelope)?;

    let mut persistent = PersistentState::new();
    persistent.current_term = target_term;
    persistent.install_snapshot(target_index, target_term);
    persistent.membership = Some(recovery_membership.clone());
    let raft_store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
    raft_store
        .save(&persistent, &active_snapshot)
        .map_err(PitrReplayError::Raft)?;
    engine
        .db
        .flush_wal(true)
        .map_err(|error| PitrReplayError::Raft(format!("fsync recovered RocksDB WAL: {error}")))?;

    drop(state_machine);
    drop(manager);
    drop(catalog);
    drop(clock);
    drop(raft_store);
    drop(engine);

    verify_staged_recovery(
        staging,
        target_index,
        target_term,
        &recovery_membership,
        &logical_snapshot,
        &active_snapshot,
    )?;

    Ok(PitrRecoveryReport {
        source_timeline: metadata.timeline,
        source_baseline_index: metadata.baseline_index,
        target_index,
        target_term,
        recovery_node_id: recovery_node_id.to_string(),
        recovery_membership_generation: recovery_membership.generation,
        replayed_records,
    })
}

fn verify_staged_recovery(
    staging: &Path,
    target_index: u64,
    target_term: u64,
    recovery_membership: &ClusterMembership,
    expected_logical_snapshot: &[u8],
    expected_active_snapshot: &[u8],
) -> Result<(), PitrReplayError> {
    let engine = Arc::new(StorageEngine::open(staging)?);
    let raft_store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
    if raft_store
        .load_staged_snapshot()
        .map_err(PitrReplayError::Raft)?
        .is_some()
    {
        return Err(PitrReplayError::Verification(
            "PITR target contains a staged Raft snapshot transition".into(),
        ));
    }
    let (persistent, active_snapshot) = raft_store
        .load()
        .map_err(PitrReplayError::Raft)?
        .ok_or_else(|| PitrReplayError::Verification("recovered Raft state is missing".into()))?;
    if persistent.snapshot_index != target_index
        || persistent.snapshot_term != target_term
        || persistent.current_term < target_term
        || persistent.last_log_index() != target_index
        || persistent.log.len() != 1
        || persistent.membership.as_ref() != Some(recovery_membership)
        || active_snapshot != expected_active_snapshot
    {
        return Err(PitrReplayError::Verification(
            "recovered Raft boundary/membership/snapshot differs from recovery plan".into(),
        ));
    }

    let catalog = Arc::new(InMemoryCatalog::default());
    let clock = Arc::new(HlcClock::new(500));
    let manager = ReplicatedSqlSnapshotManager::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&clock),
    );
    let reexported = manager.export(target_index, target_term)?;
    if reexported != expected_logical_snapshot {
        return Err(PitrReplayError::Verification(
            "recovered logical state does not re-export byte-identically".into(),
        ));
    }
    let state_machine = ReplicatedSqlStateMachine::new(engine, catalog, Arc::clone(&clock))?;
    let applied = state_machine.durable_state()?;
    if applied.last_applied_index != target_index {
        return Err(PitrReplayError::Verification(format!(
            "recovered apply index {} differs from target {target_index}",
            applied.last_applied_index
        )));
    }
    Ok(())
}

fn validate_baseline_binding(
    backup: &NeuralBaseBackup,
    baseline_artifact_bytes: &[u8],
    metadata: &ArchiveStreamMetadata,
) -> Result<(), PitrReplayError> {
    let artifact_hash: ArchiveHash = Sha256::digest(baseline_artifact_bytes).into();
    if artifact_hash != metadata.baseline_backup_sha256 {
        return Err(PitrReplayError::BaselineHashMismatch);
    }
    if backup.manifest.metadata.last_included_index != metadata.baseline_index
        || backup.manifest.metadata.last_included_term != metadata.baseline_term
        || backup.membership.generation != metadata.baseline_membership_generation
        || backup.membership.config_index != metadata.baseline_membership_config_index
        || backup.manifest.state_machine_compat_version != metadata.state_machine_compat_version
    {
        return Err(PitrReplayError::BaselineMetadataMismatch);
    }
    Ok(())
}

fn transition_membership(
    membership: &ClusterMembership,
    change: &MembershipChange,
    index: u64,
) -> Result<ClusterMembership, PitrReplayError> {
    let result = match change {
        MembershipChange::AddNode(id) | MembershipChange::AddLearner(id) => {
            membership.add_learner(id.clone(), index)
        }
        MembershipChange::PromoteLearner(id) => membership.begin_promotion(id, index),
        MembershipChange::RemoveNode(id) => membership.begin_removal(id, index),
        MembershipChange::FinalizeJoint => membership.finalize_joint(index),
    };
    result.map_err(|error| PitrReplayError::MembershipReplay { index, error })
}

fn build_recovery_membership(
    source: &ClusterMembership,
    target_index: u64,
    recovery_node_id: &str,
) -> Result<ClusterMembership, PitrReplayError> {
    if recovery_node_id.trim().is_empty() {
        return Err(PitrReplayError::EmptyRecoveryNodeId);
    }
    let recovery_node_id = recovery_node_id.to_string();
    let mut historical = source.replication_targets();
    historical.extend(source.removed.iter().cloned());
    if historical.contains(&recovery_node_id) {
        return Err(PitrReplayError::ReusedRecoveryNodeId(recovery_node_id));
    }
    let generation = source
        .generation
        .checked_add(1)
        .ok_or(PitrReplayError::MembershipGenerationOverflow)?;
    let membership = ClusterMembership {
        format_version: MEMBERSHIP_FORMAT_VERSION,
        generation,
        config_index: target_index,
        voters: BTreeSet::from([recovery_node_id]),
        learners: BTreeSet::new(),
        joint: None,
        removed: historical,
    };
    membership
        .validate()
        .map_err(PitrReplayError::InvalidRecoveryMembership)?;
    Ok(membership)
}

fn validate_target(target: &Path) -> Result<(), PitrReplayError> {
    if target.exists() {
        return Err(PitrReplayError::TargetExists(target.to_path_buf()));
    }
    if target.file_name().is_none() {
        return Err(PitrReplayError::TargetNameMissing);
    }
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(PitrReplayError::TargetParentMissing);
    }
    Ok(())
}

fn allocate_staging_path(target: &Path) -> Result<PathBuf, PitrReplayError> {
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .ok_or(PitrReplayError::TargetNameMissing)?
        .to_string_lossy();
    for attempt in 0..128u16 {
        // Keep the Phase-5 restore-partial prefix so the existing clustered
        // startup guard also fences an interrupted Phase-9 recovery.
        let candidate = parent.join(format!(
            ".{name}.restore-partial-pitr-{}-{attempt}",
            std::process::id()
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(PitrReplayError::StagingPathExhausted)
}

fn sync_dir(path: &Path) -> Result<(), PitrReplayError> {
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

#[derive(Debug, Error)]
pub enum PitrReplayError {
    #[error("archive failure during PITR replay: {0}")]
    Archive(#[from] PitrArchiveError),
    #[error("archive codec failure during PITR replay: {0}")]
    Codec(#[from] ArchiveCodecError),
    #[error("PITR storage failure: {0}")]
    Storage(#[from] StorageError),
    #[error("PITR snapshot failure: {0}")]
    Snapshot(#[from] SnapshotManagerError),
    #[error("PITR state-machine apply failure: {0}")]
    Apply(#[from] ReplicatedSqlApplyError),
    #[error("PITR I/O failure: {0}")]
    Io(#[from] io::Error),
    #[error("PITR target already exists: {0}")]
    TargetExists(PathBuf),
    #[error("PITR target parent directory does not exist")]
    TargetParentMissing,
    #[error("PITR target must name a directory")]
    TargetNameMissing,
    #[error("PITR target appeared while recovery was staged: {0}")]
    TargetAppeared(PathBuf),
    #[error("could not allocate a unique PITR staging directory")]
    StagingPathExhausted,
    #[error("PITR target {target} is unavailable; baseline={baseline}, frontier={frontier}")]
    TargetUnavailable { baseline: u64, frontier: u64, target: u64 },
    #[error("PITR baseline artifact SHA-256 does not match archive stream")]
    BaselineHashMismatch,
    #[error("PITR baseline manifest/membership does not match archive stream")]
    BaselineMetadataMismatch,
    #[error("PITR membership replay failed at index {index}: {error}")]
    MembershipReplay { index: u64, error: String },
    #[error("recovery node id must be non-empty")]
    EmptyRecoveryNodeId,
    #[error("recovery node id {0:?} exists in selected source membership history")]
    ReusedRecoveryNodeId(String),
    #[error("recovery membership generation overflow")]
    MembershipGenerationOverflow,
    #[error("constructed PITR recovery membership is invalid: {0}")]
    InvalidRecoveryMembership(String),
    #[error("PITR Raft persistence failure: {0}")]
    Raft(String),
    #[error("PITR Raft snapshot envelope failure: {0}")]
    SnapshotEnvelope(String),
    #[error("PITR staged target failed verification: {0}")]
    Verification(String),
    #[error("PITR recovery index overflow")]
    IndexOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::NeuralBaseBackup;
    use crate::consensus::{encode_membership_change, ClusterMembership};
    use crate::pitr_archive::PitrArchiveWriter;
    use crate::replicated_identity::{ReplicatedIdentityMutation, ReplicatedScramCredential};
    use crate::replicated_identity_snapshot::ReplicatedIdentitySnapshotExtension;
    use crate::replicated_snapshot::{ReplicatedSqlSnapshot, SnapshotMetadata};
    use crate::replicated_sql::{ReplicatedMutation, ReplicatedRowWrite};
    use crate::storage_executor::table_id_for;
    use tempfile::TempDir;

    fn baseline() -> NeuralBaseBackup {
        let snapshot = ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: 5,
                last_included_term: 2,
                latest_sql_apply_index: 5,
                latest_commit_ts: 10,
            },
            tables: vec![],
            metadata_extension: ReplicatedIdentitySnapshotExtension::Uninitialized
                .encode()
                .unwrap(),
        }
        .encode()
        .unwrap();
        NeuralBaseBackup::new_offline(
            1,
            ClusterMembership::bootstrap("old-a".into(), ["old-b".into(), "old-c".into()]),
            snapshot,
        )
        .unwrap()
    }

    fn sql(index: u64, key: u8) -> crate::consensus::LogEntry {
        crate::consensus::LogEntry {
            term: 3,
            index,
            command: ReplicatedMutation::InsertRows {
                table: "items".into(),
                table_id: table_id_for("items"),
                commit_ts: 100 + index,
                rows: vec![ReplicatedRowWrite {
                    primary_key: vec![key],
                    value: vec![key, 9],
                }],
            }
            .encode()
            .unwrap(),
        }
    }

    #[test]
    fn recovers_exact_intermediate_target_and_fresh_generation() {
        let temp = TempDir::new().unwrap();
        let archive_root = temp.path().join("archive");
        let backup = baseline();
        let bytes = backup.encode().unwrap();
        PitrArchiveWriter::initialize(&archive_root, &backup, &bytes, None).unwrap();
        let mut archive = PitrArchiveWriter::open(&archive_root, None).unwrap();
        archive
            .append_committed(&crate::consensus::LogEntry {
                term: 3,
                index: 6,
                command: ReplicatedMutation::CreateTable {
                    schema: crate::catalog::TableSchema {
                        name: "items".into(),
                        columns: vec![crate::catalog::ColumnDef {
                            name: "id".into(),
                            data_type: "BIGINT".into(),
                        }],
                    },
                }
                .encode()
                .unwrap(),
            })
            .unwrap();
        archive.append_committed(&sql(7, 1)).unwrap();
        archive.append_committed(&sql(8, 2)).unwrap();
        let target = temp.path().join("recovered");
        let report = recover_verified_new_cluster(
            &backup,
            &bytes,
            &archive,
            RecoveryTarget::Index(7),
            &target,
            "fresh-r1",
        )
        .unwrap();
        assert_eq!(report.target_index, 7);
        assert_eq!(report.replayed_records, 2);
        assert!(report.recovery_membership_generation > backup.membership.generation);

        let engine = Arc::new(StorageEngine::open(&target).unwrap());
        let sm = ReplicatedSqlStateMachine::new(
            Arc::clone(&engine),
            Arc::new(InMemoryCatalog::default()),
            Arc::new(HlcClock::new(500)),
        )
        .unwrap();
        assert_eq!(sm.durable_state().unwrap().last_applied_index, 7);
        let store = RocksDbRaftPersistenceStore::new(engine);
        let (persistent, _) = store.load().unwrap().unwrap();
        assert_eq!(persistent.snapshot_index, 7);
        assert_eq!(persistent.membership.unwrap().voters, BTreeSet::from(["fresh-r1".into()]));
    }

    #[test]
    fn identity_and_membership_history_follow_selected_target() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = baseline();
        let bytes = backup.encode().unwrap();
        PitrArchiveWriter::initialize(&root, &backup, &bytes, None).unwrap();
        let mut archive = PitrArchiveWriter::open(&root, None).unwrap();
        let credential = ReplicatedScramCredential {
            salt: vec![1; 16],
            iterations: 4096,
            stored_key: [2; 32],
            server_key: [3; 32],
        };
        archive
            .append_committed(&crate::consensus::LogEntry {
                term: 3,
                index: 6,
                command: ReplicatedIdentityMutation::Initialize { users: vec![] }
                    .encode()
                    .unwrap(),
            })
            .unwrap();
        archive
            .append_committed(&crate::consensus::LogEntry {
                term: 3,
                index: 7,
                command: ReplicatedIdentityMutation::CreateUser {
                    username: "alice".into(),
                    credential: credential.clone(),
                }
                .encode()
                .unwrap(),
            })
            .unwrap();
        archive
            .append_committed(&crate::consensus::LogEntry {
                term: 3,
                index: 8,
                command: encode_membership_change(&MembershipChange::AddLearner("old-d".into())),
            })
            .unwrap();
        archive
            .append_committed(&crate::consensus::LogEntry {
                term: 3,
                index: 9,
                command: ReplicatedIdentityMutation::AlterUser {
                    username: "alice".into(),
                    credential: ReplicatedScramCredential {
                        salt: vec![4; 16],
                        iterations: 4096,
                        stored_key: [5; 32],
                        server_key: [6; 32],
                    },
                }
                .encode()
                .unwrap(),
            })
            .unwrap();

        let target = temp.path().join("target");
        let report = recover_verified_new_cluster(
            &backup,
            &bytes,
            &archive,
            RecoveryTarget::Index(8),
            &target,
            "fresh-node",
        )
        .unwrap();
        assert_eq!(report.target_index, 8);
        let engine = Arc::new(StorageEngine::open(&target).unwrap());
        let store = RocksDbRaftPersistenceStore::new(engine);
        let (persistent, _) = store.load().unwrap().unwrap();
        let membership = persistent.membership.unwrap();
        assert!(membership.removed.contains("old-d"));
        assert!(!membership.voters.contains("old-d"));
    }

    #[test]
    fn unavailable_and_reused_identity_targets_fail_before_publication() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = baseline();
        let bytes = backup.encode().unwrap();
        PitrArchiveWriter::initialize(&root, &backup, &bytes, None).unwrap();
        let archive = PitrArchiveWriter::open(&root, None).unwrap();
        let target = temp.path().join("target");
        assert!(matches!(
            recover_verified_new_cluster(
                &backup,
                &bytes,
                &archive,
                RecoveryTarget::Index(6),
                &target,
                "fresh"
            ),
            Err(PitrReplayError::TargetUnavailable { .. })
        ));
        assert!(!target.exists());
        assert!(matches!(
            recover_verified_new_cluster(
                &backup,
                &bytes,
                &archive,
                RecoveryTarget::Baseline,
                &target,
                "old-a"
            ),
            Err(PitrReplayError::ReusedRecoveryNodeId(_))
        ));
        assert!(!target.exists());
    }
}