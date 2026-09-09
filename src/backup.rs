// SPDX-License-Identifier: Apache-2.0
//! Versioned operator-facing NeuralBase backup envelope.
//!
//! A Raft replication snapshot is an internal lifecycle artifact, not an
//! operator backup product. Phase 5 therefore gives backups their own strict
//! envelope while reusing the already-proven logical SQL snapshot and committed
//! membership representations.

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::consensus::{ClusterMembership, MEMBERSHIP_FORMAT_VERSION};
use crate::replicated_identity_snapshot::{
    IdentitySnapshotExtensionError, ReplicatedIdentitySnapshotExtension,
};
use crate::replicated_snapshot::{
    ReplicatedSqlSnapshot, SnapshotCodecError, SnapshotMetadata, MAX_REPLICATED_SNAPSHOT_BYTES,
    REPLICATED_SNAPSHOT_VERSION,
};

const MAGIC: &[u8; 4] = b"NBBK";
pub const BACKUP_FORMAT_VERSION: u8 = 1;
pub const BACKUP_STATE_MACHINE_COMPAT_VERSION: u16 = 1;
pub const MAX_BACKUP_MEMBERSHIP_BYTES: usize = 1024 * 1024;
const CHECKSUM_BYTES: usize = 32;
const HEADER_BYTES: usize = 148;
pub const MAX_BACKUP_BYTES: usize =
    HEADER_BYTES + MAX_BACKUP_MEMBERSHIP_BYTES + MAX_REPLICATED_SNAPSHOT_BYTES + CHECKSUM_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BackupKind {
    Offline = 1,
}

impl BackupKind {
    fn decode(value: u8) -> Result<Self, BackupCodecError> {
        match value {
            1 => Ok(Self::Offline),
            other => Err(BackupCodecError::UnsupportedBackupKind(other)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecoverySemantics {
    /// Restore logical SQL/identity state onto one designated bootstrap node,
    /// then establish a fresh consensus generation/topology deliberately.
    NewCluster = 1,
}

impl RecoverySemantics {
    fn decode(value: u8) -> Result<Self, BackupCodecError> {
        match value {
            1 => Ok(Self::NewCluster),
            other => Err(BackupCodecError::UnsupportedRecoverySemantics(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifest {
    pub created_unix_ms: u64,
    pub state_machine_compat_version: u16,
    pub snapshot_format_version: u8,
    pub membership_format_version: u8,
    pub kind: BackupKind,
    pub recovery_semantics: RecoverySemantics,
    pub metadata: SnapshotMetadata,
    pub membership_generation: u64,
    pub membership_config_index: u64,
    pub identity_included: bool,
    pub encrypted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeuralBaseBackup {
    pub manifest: BackupManifest,
    pub membership: ClusterMembership,
    /// Exact canonical `ReplicatedSqlSnapshot` bytes. Restore must consume these
    /// bytes rather than re-planning or re-encoding SQL mutations.
    pub sql_snapshot: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum BackupCodecError {
    #[error("NeuralBase backup exceeds {MAX_BACKUP_BYTES} bytes")]
    TooLarge,
    #[error("NeuralBase backup is truncated")]
    UnexpectedEof,
    #[error("invalid NeuralBase backup magic")]
    InvalidMagic,
    #[error("unsupported NeuralBase backup format version {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported NeuralBase backup flags {0:#04x}")]
    UnsupportedFlags(u8),
    #[error("unsupported NeuralBase backup kind {0}")]
    UnsupportedBackupKind(u8),
    #[error("unsupported NeuralBase recovery semantics {0}")]
    UnsupportedRecoverySemantics(u8),
    #[error("unsupported NeuralBase state-machine compatibility version {0}")]
    UnsupportedStateMachineVersion(u16),
    #[error(
        "backup declares SQL snapshot format version {0}, expected {REPLICATED_SNAPSHOT_VERSION}"
    )]
    SnapshotVersionMismatch(u8),
    #[error("backup declares membership format version {0}, expected {MEMBERSHIP_FORMAT_VERSION}")]
    MembershipVersionMismatch(u8),
    #[error("Phase-5 backup v1 requires replicated identity state to be included explicitly")]
    IdentityRequired,
    #[error("Phase-5 backup v1 does not yet support encryption")]
    EncryptionUnsupported,
    #[error("backup membership metadata exceeds {MAX_BACKUP_MEMBERSHIP_BYTES} bytes")]
    MembershipTooLarge,
    #[error("backup embedded SQL snapshot exceeds {MAX_REPLICATED_SNAPSHOT_BYTES} bytes")]
    SnapshotTooLarge,
    #[error("backup length mismatch: header declares {declared} bytes, got {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("backup membership SHA-256 mismatch")]
    MembershipHashMismatch,
    #[error("backup SQL snapshot SHA-256 mismatch")]
    SnapshotHashMismatch,
    #[error("backup envelope SHA-256 mismatch")]
    EnvelopeChecksumMismatch,
    #[error("backup membership serialization failure: {0}")]
    MembershipSerialization(String),
    #[error("backup membership is invalid: {0}")]
    InvalidMembership(String),
    #[error("backup membership is not canonically encoded")]
    NonCanonicalMembership,
    #[error("invalid embedded replicated SQL snapshot: {0}")]
    Snapshot(#[from] SnapshotCodecError),
    #[error("invalid embedded replicated identity state: {0}")]
    Identity(#[from] IdentitySnapshotExtensionError),
    #[error("backup manifest field {0} does not match the embedded state")]
    ManifestMismatch(&'static str),
    #[error(
        "membership config index {membership_config_index} exceeds backup boundary {last_included_index}"
    )]
    MembershipBeyondBoundary {
        membership_config_index: u64,
        last_included_index: u64,
    },
    #[error("backup boundary index/term pair is impossible")]
    InvalidBoundary,
}

impl NeuralBaseBackup {
    pub fn new_offline(
        created_unix_ms: u64,
        membership: ClusterMembership,
        sql_snapshot: Vec<u8>,
    ) -> Result<Self, BackupCodecError> {
        let decoded = ReplicatedSqlSnapshot::decode(&sql_snapshot)?;
        let manifest = BackupManifest {
            created_unix_ms,
            state_machine_compat_version: BACKUP_STATE_MACHINE_COMPAT_VERSION,
            snapshot_format_version: REPLICATED_SNAPSHOT_VERSION,
            membership_format_version: MEMBERSHIP_FORMAT_VERSION,
            kind: BackupKind::Offline,
            recovery_semantics: RecoverySemantics::NewCluster,
            metadata: decoded.metadata,
            membership_generation: membership.generation,
            membership_config_index: membership.config_index,
            identity_included: true,
            encrypted: false,
        };
        let backup = Self {
            manifest,
            membership,
            sql_snapshot,
        };
        backup.validate()?;
        Ok(backup)
    }

    pub fn encode(&self) -> Result<Vec<u8>, BackupCodecError> {
        self.validate()?;
        let membership_bytes = canonical_membership_bytes(&self.membership)?;
        if membership_bytes.len() > MAX_BACKUP_MEMBERSHIP_BYTES {
            return Err(BackupCodecError::MembershipTooLarge);
        }
        if self.sql_snapshot.len() > MAX_REPLICATED_SNAPSHOT_BYTES {
            return Err(BackupCodecError::SnapshotTooLarge);
        }

        let membership_len = u32::try_from(membership_bytes.len())
            .map_err(|_| BackupCodecError::MembershipTooLarge)?;
        let snapshot_len = u64::try_from(self.sql_snapshot.len())
            .map_err(|_| BackupCodecError::SnapshotTooLarge)?;
        let membership_hash = Sha256::digest(&membership_bytes);
        let snapshot_hash = Sha256::digest(&self.sql_snapshot);

        let mut out = Vec::with_capacity(
            HEADER_BYTES + membership_bytes.len() + self.sql_snapshot.len() + CHECKSUM_BYTES,
        );
        out.extend_from_slice(MAGIC);
        out.push(BACKUP_FORMAT_VERSION);
        out.push(0); // v1 flags: encryption/compression are not yet defined.
        out.push(self.manifest.kind as u8);
        out.push(self.manifest.recovery_semantics as u8);
        out.extend_from_slice(&self.manifest.state_machine_compat_version.to_be_bytes());
        out.push(self.manifest.snapshot_format_version);
        out.push(self.manifest.membership_format_version);
        out.push(u8::from(self.manifest.identity_included));
        out.extend_from_slice(&[0u8; 3]);
        out.extend_from_slice(&self.manifest.created_unix_ms.to_be_bytes());
        out.extend_from_slice(&self.manifest.metadata.last_included_index.to_be_bytes());
        out.extend_from_slice(&self.manifest.metadata.last_included_term.to_be_bytes());
        out.extend_from_slice(&self.manifest.metadata.latest_sql_apply_index.to_be_bytes());
        out.extend_from_slice(&self.manifest.metadata.latest_commit_ts.to_be_bytes());
        out.extend_from_slice(&self.manifest.membership_generation.to_be_bytes());
        out.extend_from_slice(&self.manifest.membership_config_index.to_be_bytes());
        out.extend_from_slice(&membership_len.to_be_bytes());
        out.extend_from_slice(&snapshot_len.to_be_bytes());
        out.extend_from_slice(&membership_hash);
        out.extend_from_slice(&snapshot_hash);
        debug_assert_eq!(out.len(), HEADER_BYTES);
        out.extend_from_slice(&membership_bytes);
        out.extend_from_slice(&self.sql_snapshot);

        if out.len() > MAX_BACKUP_BYTES.saturating_sub(CHECKSUM_BYTES) {
            return Err(BackupCodecError::TooLarge);
        }
        let checksum = Sha256::digest(&out);
        out.extend_from_slice(&checksum);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BackupCodecError> {
        if bytes.len() > MAX_BACKUP_BYTES {
            return Err(BackupCodecError::TooLarge);
        }
        if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES {
            return Err(BackupCodecError::UnexpectedEof);
        }

        let payload_len = bytes.len() - CHECKSUM_BYTES;
        let (payload, checksum) = bytes.split_at(payload_len);
        if Sha256::digest(payload).as_slice() != checksum {
            return Err(BackupCodecError::EnvelopeChecksumMismatch);
        }

        let mut reader = Reader::new(payload);
        if reader.take(4)? != MAGIC {
            return Err(BackupCodecError::InvalidMagic);
        }
        let version = reader.u8()?;
        if version != BACKUP_FORMAT_VERSION {
            return Err(BackupCodecError::UnsupportedVersion(version));
        }
        let flags = reader.u8()?;
        if flags != 0 {
            return Err(BackupCodecError::UnsupportedFlags(flags));
        }
        let kind = BackupKind::decode(reader.u8()?)?;
        let recovery_semantics = RecoverySemantics::decode(reader.u8()?)?;
        let state_machine_compat_version = reader.u16()?;
        if state_machine_compat_version != BACKUP_STATE_MACHINE_COMPAT_VERSION {
            return Err(BackupCodecError::UnsupportedStateMachineVersion(
                state_machine_compat_version,
            ));
        }
        let snapshot_format_version = reader.u8()?;
        if snapshot_format_version != REPLICATED_SNAPSHOT_VERSION {
            return Err(BackupCodecError::SnapshotVersionMismatch(
                snapshot_format_version,
            ));
        }
        let membership_format_version = reader.u8()?;
        if membership_format_version != MEMBERSHIP_FORMAT_VERSION {
            return Err(BackupCodecError::MembershipVersionMismatch(
                membership_format_version,
            ));
        }
        let identity_included = match reader.u8()? {
            1 => true,
            0 => false,
            _ => return Err(BackupCodecError::ManifestMismatch("identity marker")),
        };
        if reader.take(3)? != [0u8; 3] {
            return Err(BackupCodecError::UnsupportedFlags(0xff));
        }

        let created_unix_ms = reader.u64()?;
        let metadata = SnapshotMetadata {
            last_included_index: reader.u64()?,
            last_included_term: reader.u64()?,
            latest_sql_apply_index: reader.u64()?,
            latest_commit_ts: reader.u64()?,
        };
        let membership_generation = reader.u64()?;
        let membership_config_index = reader.u64()?;
        let membership_len = reader.u32()? as usize;
        if membership_len > MAX_BACKUP_MEMBERSHIP_BYTES {
            return Err(BackupCodecError::MembershipTooLarge);
        }
        let snapshot_len_u64 = reader.u64()?;
        let snapshot_len =
            usize::try_from(snapshot_len_u64).map_err(|_| BackupCodecError::SnapshotTooLarge)?;
        if snapshot_len > MAX_REPLICATED_SNAPSHOT_BYTES {
            return Err(BackupCodecError::SnapshotTooLarge);
        }
        let expected_membership_hash = reader.take(32)?;
        let expected_snapshot_hash = reader.take(32)?;
        debug_assert_eq!(reader.pos, HEADER_BYTES);

        let declared = HEADER_BYTES
            .checked_add(membership_len)
            .and_then(|value| value.checked_add(snapshot_len))
            .and_then(|value| value.checked_add(CHECKSUM_BYTES))
            .ok_or(BackupCodecError::TooLarge)?;
        if declared != bytes.len() {
            return Err(BackupCodecError::LengthMismatch {
                declared,
                actual: bytes.len(),
            });
        }

        let membership_bytes = reader.take(membership_len)?;
        let sql_snapshot = reader.take(snapshot_len)?.to_vec();
        if !reader.is_finished() {
            return Err(BackupCodecError::LengthMismatch {
                declared,
                actual: bytes.len(),
            });
        }
        if Sha256::digest(membership_bytes).as_slice() != expected_membership_hash {
            return Err(BackupCodecError::MembershipHashMismatch);
        }
        if Sha256::digest(&sql_snapshot).as_slice() != expected_snapshot_hash {
            return Err(BackupCodecError::SnapshotHashMismatch);
        }

        let membership: ClusterMembership = serde_json::from_slice(membership_bytes)
            .map_err(|error| BackupCodecError::MembershipSerialization(error.to_string()))?;
        membership
            .validate()
            .map_err(BackupCodecError::InvalidMembership)?;
        if canonical_membership_bytes(&membership)? != membership_bytes {
            return Err(BackupCodecError::NonCanonicalMembership);
        }

        let backup = Self {
            manifest: BackupManifest {
                created_unix_ms,
                state_machine_compat_version,
                snapshot_format_version,
                membership_format_version,
                kind,
                recovery_semantics,
                metadata,
                membership_generation,
                membership_config_index,
                identity_included,
                encrypted: false,
            },
            membership,
            sql_snapshot,
        };
        backup.validate()?;
        Ok(backup)
    }

    pub fn validate(&self) -> Result<(), BackupCodecError> {
        if self.manifest.state_machine_compat_version != BACKUP_STATE_MACHINE_COMPAT_VERSION {
            return Err(BackupCodecError::UnsupportedStateMachineVersion(
                self.manifest.state_machine_compat_version,
            ));
        }
        if self.manifest.snapshot_format_version != REPLICATED_SNAPSHOT_VERSION {
            return Err(BackupCodecError::SnapshotVersionMismatch(
                self.manifest.snapshot_format_version,
            ));
        }
        if self.manifest.membership_format_version != MEMBERSHIP_FORMAT_VERSION {
            return Err(BackupCodecError::MembershipVersionMismatch(
                self.manifest.membership_format_version,
            ));
        }
        if !self.manifest.identity_included {
            return Err(BackupCodecError::IdentityRequired);
        }
        if self.manifest.encrypted {
            return Err(BackupCodecError::EncryptionUnsupported);
        }
        if self.sql_snapshot.len() > MAX_REPLICATED_SNAPSHOT_BYTES {
            return Err(BackupCodecError::SnapshotTooLarge);
        }

        self.membership
            .validate()
            .map_err(BackupCodecError::InvalidMembership)?;
        if self.membership.format_version != self.manifest.membership_format_version {
            return Err(BackupCodecError::ManifestMismatch(
                "membership format version",
            ));
        }
        if self.membership.generation != self.manifest.membership_generation {
            return Err(BackupCodecError::ManifestMismatch("membership generation"));
        }
        if self.membership.config_index != self.manifest.membership_config_index {
            return Err(BackupCodecError::ManifestMismatch(
                "membership config index",
            ));
        }

        let snapshot = ReplicatedSqlSnapshot::decode(&self.sql_snapshot)?;
        if snapshot.metadata != self.manifest.metadata {
            return Err(BackupCodecError::ManifestMismatch("snapshot metadata"));
        }
        if snapshot.metadata.last_included_index == 0 {
            if snapshot.metadata.last_included_term != 0 {
                return Err(BackupCodecError::InvalidBoundary);
            }
        } else if snapshot.metadata.last_included_term == 0 {
            return Err(BackupCodecError::InvalidBoundary);
        }
        if self.membership.config_index > snapshot.metadata.last_included_index {
            return Err(BackupCodecError::MembershipBeyondBoundary {
                membership_config_index: self.membership.config_index,
                last_included_index: snapshot.metadata.last_included_index,
            });
        }
        if snapshot.metadata_extension.is_empty() {
            return Err(BackupCodecError::IdentityRequired);
        }
        ReplicatedIdentitySnapshotExtension::decode(&snapshot.metadata_extension)?;
        Ok(())
    }
}

fn canonical_membership_bytes(membership: &ClusterMembership) -> Result<Vec<u8>, BackupCodecError> {
    membership
        .validate()
        .map_err(BackupCodecError::InvalidMembership)?;
    serde_json::to_vec(membership)
        .map_err(|error| BackupCodecError::MembershipSerialization(error.to_string()))
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], BackupCodecError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(BackupCodecError::TooLarge)?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or(BackupCodecError::UnexpectedEof)?;
        self.pos = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, BackupCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, BackupCodecError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| BackupCodecError::UnexpectedEof)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, BackupCodecError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| BackupCodecError::UnexpectedEof)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, BackupCodecError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| BackupCodecError::UnexpectedEof)?,
        ))
    }

    fn is_finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicated_identity_snapshot::ReplicatedIdentitySnapshotExtension;

    fn sql_snapshot(boundary: u64) -> Vec<u8> {
        ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: boundary,
                last_included_term: if boundary == 0 { 0 } else { 3 },
                latest_sql_apply_index: boundary.saturating_sub(1),
                latest_commit_ts: if boundary > 1 { 42 } else { 0 },
            },
            tables: vec![],
            metadata_extension: ReplicatedIdentitySnapshotExtension::Uninitialized
                .encode()
                .unwrap(),
        }
        .encode()
        .unwrap()
    }

    fn membership() -> ClusterMembership {
        ClusterMembership::bootstrap("n1".to_string(), ["n2".to_string(), "n3".to_string()])
    }

    fn rewrite_checksum(bytes: &mut Vec<u8>) {
        let len = bytes.len();
        let checksum_at = len - CHECKSUM_BYTES;
        let checksum = Sha256::digest(&bytes[..checksum_at]);
        bytes[checksum_at..].copy_from_slice(&checksum);
    }

    #[test]
    fn backup_roundtrip_is_exact_and_deterministic() {
        let backup =
            NeuralBaseBackup::new_offline(1_725_000_000_123, membership(), sql_snapshot(7))
                .unwrap();
        let first = backup.encode().unwrap();
        let second = backup.encode().unwrap();
        assert_eq!(first, second);
        assert_eq!(NeuralBaseBackup::decode(&first).unwrap(), backup);
    }

    #[test]
    fn single_byte_corruption_is_rejected() {
        let backup = NeuralBaseBackup::new_offline(7, membership(), sql_snapshot(7)).unwrap();
        let mut encoded = backup.encode().unwrap();
        encoded[HEADER_BYTES + 1] ^= 0x80;
        assert!(matches!(
            NeuralBaseBackup::decode(&encoded),
            Err(BackupCodecError::EnvelopeChecksumMismatch)
        ));
    }

    #[test]
    fn truncation_is_rejected() {
        let backup = NeuralBaseBackup::new_offline(7, membership(), sql_snapshot(7)).unwrap();
        let mut encoded = backup.encode().unwrap();
        encoded.truncate(encoded.len() - 5);
        assert!(NeuralBaseBackup::decode(&encoded).is_err());
    }

    #[test]
    fn future_backup_version_is_rejected_after_integrity_validation() {
        let backup = NeuralBaseBackup::new_offline(7, membership(), sql_snapshot(7)).unwrap();
        let mut encoded = backup.encode().unwrap();
        encoded[4] = BACKUP_FORMAT_VERSION + 1;
        rewrite_checksum(&mut encoded);
        assert!(matches!(
            NeuralBaseBackup::decode(&encoded),
            Err(BackupCodecError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn ambiguous_trailing_bytes_are_rejected() {
        let backup = NeuralBaseBackup::new_offline(7, membership(), sql_snapshot(7)).unwrap();
        let encoded = backup.encode().unwrap();
        let split = encoded.len() - CHECKSUM_BYTES;
        let mut malformed = encoded[..split].to_vec();
        malformed.push(0);
        let checksum = Sha256::digest(&malformed);
        malformed.extend_from_slice(&checksum);
        assert!(matches!(
            NeuralBaseBackup::decode(&malformed),
            Err(BackupCodecError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn membership_newer_than_backup_boundary_is_rejected() {
        let membership = membership().add_learner("n4".to_string(), 9).unwrap();
        assert!(matches!(
            NeuralBaseBackup::new_offline(7, membership, sql_snapshot(7)),
            Err(BackupCodecError::MembershipBeyondBoundary { .. })
        ));
    }

    #[test]
    fn legacy_snapshot_without_identity_extension_is_not_a_phase5_backup() {
        let legacy = ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: 3,
                last_included_term: 1,
                latest_sql_apply_index: 0,
                latest_commit_ts: 0,
            },
            tables: vec![],
            metadata_extension: vec![],
        }
        .encode()
        .unwrap();
        assert!(matches!(
            NeuralBaseBackup::new_offline(7, membership(), legacy),
            Err(BackupCodecError::IdentityRequired)
        ));
    }
}
