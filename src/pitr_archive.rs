// SPDX-License-Identifier: Apache-2.0
//! Crash-safe durable storage for Phase-9 PITR archive streams.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::{rngs::OsRng, RngCore};
use rustls::crypto::cipher::{AeadKey, Iv};
use rustls::crypto::ring::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256;
use rustls::crypto::SharedSecret;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::backup::{NeuralBaseBackup, BACKUP_STATE_MACHINE_COMPAT_VERSION};
use crate::consensus::LogEntry;
use crate::pitr::{
    ArchiveChainSpec, ArchiveCodecError, ArchiveFrontier, ArchiveHash, ArchiveSegment,
    RecoveryRecord, TimelineId, ARCHIVE_FORMAT_VERSION, ARCHIVE_HASH_BYTES,
    ARCHIVE_STATE_MACHINE_COMPAT_VERSION, ARCHIVE_TIMELINE_BYTES, MAX_ARCHIVE_PAYLOAD_BYTES,
};

const METADATA_FILE: &str = "stream.json";
const SEGMENTS_DIR: &str = "segments";
const STAGING_DIR: &str = ".staging";
const STREAM_METADATA_VERSION: u8 = 1;
const MAX_METADATA_BYTES: usize = 64 * 1024;
const SEGMENT_SUFFIX: &str = ".nbar";
const ENCRYPTED_SEGMENT_SUFFIX: &str = ".nbpe";

const ENCRYPTED_MAGIC: &[u8; 4] = b"NBPE";
const ENCRYPTED_VERSION: u8 = 1;
const ENCRYPTED_ALGORITHM_CHACHA20_POLY1305: u8 = 1;
const PITR_KEY_BYTES: usize = 32;
const KEY_ID_BYTES: usize = 16;
const NONCE_BYTES: usize = 12;
const TAG_BYTES: usize = 16;
const ENCRYPTED_HEADER_BYTES: usize =
    4 + 1 + 1 + 2 + NONCE_BYTES + 8 + ARCHIVE_TIMELINE_BYTES + KEY_ID_BYTES;
const MAX_PLAINTEXT_SEGMENT_BYTES: usize =
    256 + MAX_ARCHIVE_PAYLOAD_BYTES + ARCHIVE_HASH_BYTES;
const MAX_ENCRYPTED_SEGMENT_BYTES: usize =
    ENCRYPTED_HEADER_BYTES + MAX_PLAINTEXT_SEGMENT_BYTES + TAG_BYTES;

pub type KeyId = [u8; KEY_ID_BYTES];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveStreamMetadata {
    pub format_version: u8,
    pub archive_format_version: u8,
    pub state_machine_compat_version: u16,
    pub timeline: TimelineId,
    pub parent_timeline: Option<TimelineId>,
    pub branch_index: Option<u64>,
    pub baseline_backup_sha256: ArchiveHash,
    pub baseline_index: u64,
    pub baseline_term: u64,
    pub baseline_membership_generation: u64,
    pub baseline_membership_config_index: u64,
    pub encrypted: bool,
    pub key_id: Option<KeyId>,
}

impl ArchiveStreamMetadata {
    pub fn from_backup(
        backup: &NeuralBaseBackup,
        baseline_backup_sha256: ArchiveHash,
        timeline: TimelineId,
        encrypted: bool,
        key_id: Option<KeyId>,
    ) -> Result<Self, PitrArchiveError> {
        let metadata = Self {
            format_version: STREAM_METADATA_VERSION,
            archive_format_version: ARCHIVE_FORMAT_VERSION,
            state_machine_compat_version: BACKUP_STATE_MACHINE_COMPAT_VERSION,
            timeline,
            parent_timeline: None,
            branch_index: None,
            baseline_backup_sha256,
            baseline_index: backup.manifest.metadata.last_included_index,
            baseline_term: backup.manifest.metadata.last_included_term,
            baseline_membership_generation: backup.membership.generation,
            baseline_membership_config_index: backup.membership.config_index,
            encrypted,
            key_id,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    pub fn branched(
        &self,
        new_timeline: TimelineId,
        branch_index: u64,
        baseline_backup_sha256: ArchiveHash,
        baseline_term: u64,
        membership_generation: u64,
        membership_config_index: u64,
        encrypted: bool,
        key_id: Option<KeyId>,
    ) -> Result<Self, PitrArchiveError> {
        if branch_index < self.baseline_index {
            return Err(PitrArchiveError::InvalidMetadata(
                "branch index precedes parent baseline".into(),
            ));
        }
        let metadata = Self {
            format_version: STREAM_METADATA_VERSION,
            archive_format_version: ARCHIVE_FORMAT_VERSION,
            state_machine_compat_version: ARCHIVE_STATE_MACHINE_COMPAT_VERSION,
            timeline: new_timeline,
            parent_timeline: Some(self.timeline),
            branch_index: Some(branch_index),
            baseline_backup_sha256,
            baseline_index: branch_index,
            baseline_term,
            baseline_membership_generation: membership_generation,
            baseline_membership_config_index: membership_config_index,
            encrypted,
            key_id,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    pub fn chain_spec(&self) -> ArchiveChainSpec {
        ArchiveChainSpec {
            timeline: self.timeline,
            baseline_index: self.baseline_index,
            baseline_term: self.baseline_term,
            baseline_backup_sha256: self.baseline_backup_sha256,
        }
    }

    pub fn validate(&self) -> Result<(), PitrArchiveError> {
        if self.format_version != STREAM_METADATA_VERSION {
            return Err(PitrArchiveError::UnsupportedMetadataVersion(
                self.format_version,
            ));
        }
        if self.archive_format_version != ARCHIVE_FORMAT_VERSION {
            return Err(PitrArchiveError::UnsupportedArchiveVersion(
                self.archive_format_version,
            ));
        }
        if self.state_machine_compat_version != ARCHIVE_STATE_MACHINE_COMPAT_VERSION {
            return Err(PitrArchiveError::UnsupportedStateMachineVersion(
                self.state_machine_compat_version,
            ));
        }
        self.chain_spec().validate()?;
        if self.baseline_membership_generation == 0 {
            return Err(PitrArchiveError::InvalidMetadata(
                "baseline membership generation is zero".into(),
            ));
        }
        if self.baseline_membership_config_index > self.baseline_index {
            return Err(PitrArchiveError::InvalidMetadata(
                "baseline membership config index exceeds baseline boundary".into(),
            ));
        }
        match (self.parent_timeline, self.branch_index) {
            (None, None) => {}
            (Some(parent), Some(index)) => {
                if parent == self.timeline {
                    return Err(PitrArchiveError::InvalidMetadata(
                        "branch timeline equals parent timeline".into(),
                    ));
                }
                if index != self.baseline_index {
                    return Err(PitrArchiveError::InvalidMetadata(
                        "branch index must equal branch baseline index".into(),
                    ));
                }
            }
            _ => {
                return Err(PitrArchiveError::InvalidMetadata(
                    "parent timeline and branch index must appear together".into(),
                ))
            }
        }
        match (self.encrypted, self.key_id) {
            (false, None) | (true, Some(_)) => Ok(()),
            (false, Some(_)) => Err(PitrArchiveError::InvalidMetadata(
                "plaintext stream must not declare a key id".into(),
            )),
            (true, None) => Err(PitrArchiveError::InvalidMetadata(
                "encrypted stream is missing key id".into(),
            )),
        }
    }
}

/// A 256-bit archive key loaded from an out-of-band file.
pub struct PitrArchiveKey {
    secret: SharedSecret,
}

impl PitrArchiveKey {
    pub fn from_bytes(bytes: [u8; PITR_KEY_BYTES]) -> Self {
        Self {
            secret: SharedSecret::from(Vec::from(bytes)),
        }
    }

    pub fn key_id(&self) -> KeyId {
        let digest = Sha256::digest(self.secret.secret_bytes());
        let mut id = [0u8; KEY_ID_BYTES];
        id.copy_from_slice(&digest[..KEY_ID_BYTES]);
        id
    }

    fn aead_key(&self) -> Result<AeadKey, PitrArchiveError> {
        let bytes: [u8; PITR_KEY_BYTES] = self
            .secret
            .secret_bytes()
            .try_into()
            .map_err(|_| PitrArchiveError::CryptoUnavailable)?;
        Ok(AeadKey::from(bytes))
    }
}

impl fmt::Debug for PitrArchiveKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PitrArchiveKey([REDACTED])")
    }
}

pub fn load_pitr_archive_key(path: &Path) -> Result<PitrArchiveKey, PitrArchiveError> {
    let before = fs::symlink_metadata(path).map_err(|source| PitrArchiveError::KeyIo {
        path: path.to_path_buf(),
        source,
    })?;
    if !before.file_type().is_file() {
        return Err(PitrArchiveError::KeyNotRegularFile(path.to_path_buf()));
    }
    let file = File::open(path).map_err(|source| PitrArchiveError::KeyIo {
        path: path.to_path_buf(),
        source,
    })?;
    let opened = file.metadata().map_err(|source| PitrArchiveError::KeyIo {
        path: path.to_path_buf(),
        source,
    })?;
    if !opened.file_type().is_file() {
        return Err(PitrArchiveError::KeyNotRegularFile(path.to_path_buf()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if before.dev() != opened.dev() || before.ino() != opened.ino() {
            return Err(PitrArchiveError::KeyChangedDuringOpen(path.to_path_buf()));
        }
        let mode = opened.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(PitrArchiveError::InsecureKeyPermissions {
                path: path.to_path_buf(),
                mode,
            });
        }
    }
    let mut bytes = Vec::with_capacity(PITR_KEY_BYTES + 1);
    file.take((PITR_KEY_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| PitrArchiveError::KeyIo {
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() != PITR_KEY_BYTES {
        return Err(PitrArchiveError::InvalidKeyLength {
            path: path.to_path_buf(),
            actual: bytes.len(),
        });
    }
    let array: [u8; PITR_KEY_BYTES] = bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| PitrArchiveError::InvalidKeyLength {
            path: path.to_path_buf(),
            actual: bytes.len(),
        })?;
    Ok(PitrArchiveKey::from_bytes(array))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveStatus {
    pub metadata: ArchiveStreamMetadata,
    pub frontier: ArchiveFrontier,
}

pub struct PitrArchiveWriter {
    root: PathBuf,
    metadata: ArchiveStreamMetadata,
    key: Option<PitrArchiveKey>,
    frontier: ArchiveFrontier,
}

impl PitrArchiveWriter {
    pub fn initialize(
        root: &Path,
        backup: &NeuralBaseBackup,
        baseline_artifact_bytes: &[u8],
        key: Option<PitrArchiveKey>,
    ) -> Result<ArchiveStatus, PitrArchiveError> {
        if root.exists() {
            if !root.is_dir() || fs::read_dir(root)?.next().transpose()?.is_some() {
                return Err(PitrArchiveError::DestinationNotEmpty(root.to_path_buf()));
            }
        } else {
            fs::create_dir(root)?;
        }
        set_directory_permissions(root)?;
        let segments = root.join(SEGMENTS_DIR);
        let staging = root.join(STAGING_DIR);
        fs::create_dir(&segments)?;
        fs::create_dir(&staging)?;
        set_directory_permissions(&segments)?;
        set_directory_permissions(&staging)?;

        let mut timeline = [0u8; ARCHIVE_TIMELINE_BYTES];
        OsRng
            .try_fill_bytes(&mut timeline)
            .map_err(|_| PitrArchiveError::RandomFailure)?;
        if timeline == [0u8; ARCHIVE_TIMELINE_BYTES] {
            timeline[0] = 1;
        }
        let baseline_hash: ArchiveHash = Sha256::digest(baseline_artifact_bytes).into();
        let metadata = ArchiveStreamMetadata::from_backup(
            backup,
            baseline_hash,
            timeline,
            key.is_some(),
            key.as_ref().map(PitrArchiveKey::key_id),
        )?;
        write_metadata_atomically(root, &metadata)?;
        sync_dir(root)?;
        let frontier = ArchiveFrontier {
            index: metadata.baseline_index,
            term: metadata.baseline_term,
            segment_hash: metadata.chain_spec().baseline_anchor()?,
            segments: 0,
        };
        Ok(ArchiveStatus { metadata, frontier })
    }

    pub fn open(root: &Path, key: Option<PitrArchiveKey>) -> Result<Self, PitrArchiveError> {
        let metadata = read_metadata(root)?;
        match (metadata.encrypted, key.as_ref()) {
            (true, None) => return Err(PitrArchiveError::KeyRequired),
            (false, Some(_)) => return Err(PitrArchiveError::UnexpectedKey),
            (true, Some(key)) if Some(key.key_id()) != metadata.key_id => {
                return Err(PitrArchiveError::WrongKeyId)
            }
            _ => {}
        }
        cleanup_staging(root)?;
        let mut writer = Self {
            root: root.to_path_buf(),
            metadata,
            key,
            frontier: ArchiveFrontier {
                index: 0,
                term: 0,
                segment_hash: [0; ARCHIVE_HASH_BYTES],
                segments: 0,
            },
        };
        writer.frontier = writer.verify_chain()?;
        Ok(writer)
    }

    pub fn status(&self) -> ArchiveStatus {
        ArchiveStatus {
            metadata: self.metadata.clone(),
            frontier: self.frontier.clone(),
        }
    }

    pub fn append_committed(&mut self, entry: &LogEntry) -> Result<u64, PitrArchiveError> {
        let record = RecoveryRecord::from_log_entry(entry)?;
        let expected = self
            .frontier
            .index
            .checked_add(1)
            .ok_or(PitrArchiveError::IndexOverflow)?;
        if record.index < expected {
            let existing = self.read_segment(record.index)?;
            if existing.record == record && existing.timeline == self.metadata.timeline {
                return Ok(record.index);
            }
            return Err(PitrArchiveError::ConflictingDuplicate(record.index));
        }
        if record.index > expected {
            return Err(PitrArchiveError::Gap {
                expected,
                actual: record.index,
            });
        }

        let segment = ArchiveSegment::new(
            self.metadata.timeline,
            now_unix_ms()?,
            self.frontier.segment_hash,
            record,
        )?;
        self.publish_segment(&segment)?;
        self.frontier = ArchiveFrontier {
            index: segment.record.index,
            term: segment.record.term,
            segment_hash: segment.hash()?,
            segments: self.frontier.segments + 1,
        };
        Ok(segment.record.index)
    }

    pub fn verify_chain(&self) -> Result<ArchiveFrontier, PitrArchiveError> {
        let mut expected = self
            .metadata
            .baseline_index
            .checked_add(1)
            .ok_or(PitrArchiveError::IndexOverflow)?;
        let mut previous = self.metadata.chain_spec().baseline_anchor()?;
        let mut frontier = ArchiveFrontier {
            index: self.metadata.baseline_index,
            term: self.metadata.baseline_term,
            segment_hash: previous,
            segments: 0,
        };
        for (index, path) in list_segment_files(&self.root, self.metadata.encrypted)? {
            if index < expected {
                return Err(PitrArchiveError::Overlap {
                    expected,
                    actual: index,
                });
            }
            if index > expected {
                return Err(PitrArchiveError::Gap {
                    expected,
                    actual: index,
                });
            }
            let segment = self.read_segment_path(&path)?;
            if segment.timeline != self.metadata.timeline {
                return Err(PitrArchiveError::WrongTimeline(index));
            }
            if segment.record.index != index {
                return Err(PitrArchiveError::FilenameIndexMismatch {
                    filename_index: index,
                    record_index: segment.record.index,
                });
            }
            if segment.previous_hash != previous {
                return Err(PitrArchiveError::PreviousHashMismatch(index));
            }
            previous = segment.hash()?;
            frontier = ArchiveFrontier {
                index,
                term: segment.record.term,
                segment_hash: previous,
                segments: frontier.segments + 1,
            };
            expected = expected
                .checked_add(1)
                .ok_or(PitrArchiveError::IndexOverflow)?;
        }
        Ok(frontier)
    }

    pub fn read_segment(&self, index: u64) -> Result<ArchiveSegment, PitrArchiveError> {
        if index <= self.metadata.baseline_index || index > self.frontier.index {
            return Err(PitrArchiveError::TargetUnavailable(index));
        }
        self.read_segment_path(&segment_path(
            &self.root,
            index,
            self.metadata.encrypted,
        ))
    }

    pub fn verify_target(&self, target: u64) -> Result<(), PitrArchiveError> {
        if target < self.metadata.baseline_index || target > self.frontier.index {
            return Err(PitrArchiveError::TargetUnavailable(target));
        }
        if target == self.metadata.baseline_index {
            return Ok(());
        }
        let segment = self.read_segment(target)?;
        if segment.record.index != target {
            return Err(PitrArchiveError::TargetUnavailable(target));
        }
        Ok(())
    }

    pub fn metadata(&self) -> &ArchiveStreamMetadata {
        &self.metadata
    }

    fn publish_segment(&self, segment: &ArchiveSegment) -> Result<(), PitrArchiveError> {
        let plaintext = segment.encode()?;
        let bytes = match (&self.key, self.metadata.encrypted) {
            (Some(key), true) => encrypt_segment(&plaintext, self.metadata.timeline, key)?,
            (None, false) => plaintext,
            _ => return Err(PitrArchiveError::InvalidKeyConfiguration),
        };
        let final_path = segment_path(
            &self.root,
            segment.record.index,
            self.metadata.encrypted,
        );
        if final_path.exists() {
            let existing = self.read_segment_path(&final_path)?;
            if existing == *segment {
                return Ok(());
            }
            return Err(PitrArchiveError::ConflictingDuplicate(
                segment.record.index,
            ));
        }
        let staging_path = self.root.join(STAGING_DIR).join(format!(
            ".{:020}.partial-{}-{}",
            segment.record.index,
            std::process::id(),
            segment.created_unix_ms
        ));
        let result = (|| -> Result<(), PitrArchiveError> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&staging_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);

            let staged = self.read_segment_path(&staging_path)?;
            if staged != *segment {
                return Err(PitrArchiveError::StagedVerificationMismatch);
            }
            match fs::hard_link(&staging_path, &final_path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    return Err(PitrArchiveError::ConflictingDuplicate(
                        segment.record.index,
                    ))
                }
                Err(error) => return Err(error.into()),
            }
            sync_dir(&self.root.join(SEGMENTS_DIR))?;
            fs::remove_file(&staging_path)?;
            sync_dir(&self.root.join(STAGING_DIR))?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&staging_path);
        }
        result
    }

    fn read_segment_path(&self, path: &Path) -> Result<ArchiveSegment, PitrArchiveError> {
        let max = if self.metadata.encrypted {
            MAX_ENCRYPTED_SEGMENT_BYTES
        } else {
            MAX_PLAINTEXT_SEGMENT_BYTES
        };
        let bytes = read_bounded_file(path, max)?;
        let plaintext = match (&self.key, self.metadata.encrypted) {
            (Some(key), true) => decrypt_segment(&bytes, self.metadata.timeline, key)?,
            (None, false) => bytes,
            _ => return Err(PitrArchiveError::InvalidKeyConfiguration),
        };
        ArchiveSegment::decode(&plaintext).map_err(Into::into)
    }
}

pub fn read_metadata(root: &Path) -> Result<ArchiveStreamMetadata, PitrArchiveError> {
    let bytes = read_bounded_file(&root.join(METADATA_FILE), MAX_METADATA_BYTES)?;
    let metadata: ArchiveStreamMetadata = serde_json::from_slice(&bytes)
        .map_err(|error| PitrArchiveError::MetadataEncoding(error.to_string()))?;
    metadata.validate()?;
    let canonical = canonical_metadata_bytes(&metadata)?;
    if canonical != bytes {
        return Err(PitrArchiveError::NonCanonicalMetadata);
    }
    Ok(metadata)
}

fn write_metadata_atomically(
    root: &Path,
    metadata: &ArchiveStreamMetadata,
) -> Result<(), PitrArchiveError> {
    metadata.validate()?;
    let bytes = canonical_metadata_bytes(metadata)?;
    let final_path = root.join(METADATA_FILE);
    if final_path.exists() {
        return Err(PitrArchiveError::MetadataAlreadyExists(final_path));
    }
    let staged = root.join(format!(".{METADATA_FILE}.partial-{}", std::process::id()));
    let result = (|| -> Result<(), PitrArchiveError> {
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
        let check = read_bounded_file(&staged, MAX_METADATA_BYTES)?;
        if check != bytes {
            return Err(PitrArchiveError::StagedVerificationMismatch);
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

fn canonical_metadata_bytes(metadata: &ArchiveStreamMetadata) -> Result<Vec<u8>, PitrArchiveError> {
    let mut bytes = serde_json::to_vec(metadata)
        .map_err(|error| PitrArchiveError::MetadataEncoding(error.to_string()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn list_segment_files(
    root: &Path,
    encrypted: bool,
) -> Result<Vec<(u64, PathBuf)>, PitrArchiveError> {
    let suffix = if encrypted {
        ENCRYPTED_SEGMENT_SUFFIX
    } else {
        SEGMENT_SUFFIX
    };
    let dir = root.join(SEGMENTS_DIR);
    if !dir.is_dir() {
        return Err(PitrArchiveError::MissingSegmentsDirectory(dir));
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(PitrArchiveError::UnexpectedArchiveEntry(entry.path()));
        }
        let name = entry.file_name();
        let text = name.to_string_lossy();
        let Some(stem) = text.strip_suffix(suffix) else {
            return Err(PitrArchiveError::UnexpectedArchiveEntry(entry.path()));
        };
        if stem.len() != 20 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(PitrArchiveError::UnexpectedArchiveEntry(entry.path()));
        }
        let index = stem
            .parse::<u64>()
            .map_err(|_| PitrArchiveError::UnexpectedArchiveEntry(entry.path()))?;
        out.push((index, entry.path()));
    }
    out.sort_by_key(|(index, _)| *index);
    Ok(out)
}

fn segment_path(root: &Path, index: u64, encrypted: bool) -> PathBuf {
    let suffix = if encrypted {
        ENCRYPTED_SEGMENT_SUFFIX
    } else {
        SEGMENT_SUFFIX
    };
    root.join(SEGMENTS_DIR)
        .join(format!("{index:020}{suffix}"))
}

fn cleanup_staging(root: &Path) -> Result<(), PitrArchiveError> {
    let dir = root.join(STAGING_DIR);
    if !dir.is_dir() {
        return Err(PitrArchiveError::MissingStagingDirectory(dir));
    }
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::remove_file(entry.path())?;
        } else {
            return Err(PitrArchiveError::UnexpectedArchiveEntry(entry.path()));
        }
    }
    sync_dir(&dir)?;
    Ok(())
}

fn encrypt_segment(
    plaintext: &[u8],
    timeline: TimelineId,
    key: &PitrArchiveKey,
) -> Result<Vec<u8>, PitrArchiveError> {
    if plaintext.len() > MAX_PLAINTEXT_SEGMENT_BYTES {
        return Err(PitrArchiveError::SegmentTooLarge);
    }
    let mut nonce = [0u8; NONCE_BYTES];
    OsRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| PitrArchiveError::RandomFailure)?;
    let plaintext_len =
        u64::try_from(plaintext.len()).map_err(|_| PitrArchiveError::SegmentTooLarge)?;
    let mut header = [0u8; ENCRYPTED_HEADER_BYTES];
    header[..4].copy_from_slice(ENCRYPTED_MAGIC);
    header[4] = ENCRYPTED_VERSION;
    header[5] = ENCRYPTED_ALGORITHM_CHACHA20_POLY1305;
    header[6..8].copy_from_slice(&0u16.to_be_bytes());
    header[8..20].copy_from_slice(&nonce);
    header[20..28].copy_from_slice(&plaintext_len.to_be_bytes());
    header[28..44].copy_from_slice(&timeline);
    header[44..60].copy_from_slice(&key.key_id());

    let mut payload = plaintext.to_vec();
    let packet = packet_key(key, nonce)?;
    let tag = packet
        .encrypt_in_place(0, &header, &mut payload)
        .map_err(|_| PitrArchiveError::EncryptionFailed)?;
    if tag.as_ref().len() != TAG_BYTES {
        return Err(PitrArchiveError::CryptoUnavailable);
    }
    let mut out = Vec::with_capacity(ENCRYPTED_HEADER_BYTES + payload.len() + TAG_BYTES);
    out.extend_from_slice(&header);
    out.extend_from_slice(&payload);
    out.extend_from_slice(tag.as_ref());
    Ok(out)
}

fn decrypt_segment(
    bytes: &[u8],
    timeline: TimelineId,
    key: &PitrArchiveKey,
) -> Result<Vec<u8>, PitrArchiveError> {
    if bytes.len() < ENCRYPTED_HEADER_BYTES + TAG_BYTES {
        return Err(PitrArchiveError::EncryptedSegmentTruncated);
    }
    if bytes.len() > MAX_ENCRYPTED_SEGMENT_BYTES {
        return Err(PitrArchiveError::SegmentTooLarge);
    }
    let header = &bytes[..ENCRYPTED_HEADER_BYTES];
    if &header[..4] != ENCRYPTED_MAGIC {
        return Err(PitrArchiveError::InvalidEncryptedMagic);
    }
    if header[4] != ENCRYPTED_VERSION {
        return Err(PitrArchiveError::UnsupportedEncryptedVersion(header[4]));
    }
    if header[5] != ENCRYPTED_ALGORITHM_CHACHA20_POLY1305 {
        return Err(PitrArchiveError::UnsupportedEncryptedAlgorithm(header[5]));
    }
    if header[6..8] != [0u8; 2] {
        return Err(PitrArchiveError::UnsupportedEncryptedFlags);
    }
    if header[28..44] != timeline {
        return Err(PitrArchiveError::EncryptedTimelineMismatch);
    }
    if header[44..60] != key.key_id() {
        return Err(PitrArchiveError::WrongKeyId);
    }
    let nonce: [u8; NONCE_BYTES] = header[8..20]
        .try_into()
        .map_err(|_| PitrArchiveError::EncryptedSegmentTruncated)?;
    let plaintext_len = usize::try_from(u64::from_be_bytes(
        header[20..28]
            .try_into()
            .map_err(|_| PitrArchiveError::EncryptedSegmentTruncated)?,
    ))
    .map_err(|_| PitrArchiveError::SegmentTooLarge)?;
    if plaintext_len > MAX_PLAINTEXT_SEGMENT_BYTES {
        return Err(PitrArchiveError::SegmentTooLarge);
    }
    let declared = ENCRYPTED_HEADER_BYTES
        .checked_add(plaintext_len)
        .and_then(|value| value.checked_add(TAG_BYTES))
        .ok_or(PitrArchiveError::SegmentTooLarge)?;
    if declared != bytes.len() {
        return Err(PitrArchiveError::EncryptedLengthMismatch {
            declared,
            actual: bytes.len(),
        });
    }
    let mut payload = bytes[ENCRYPTED_HEADER_BYTES..].to_vec();
    let packet = packet_key(key, nonce)?;
    let decrypted = packet
        .decrypt_in_place(0, header, &mut payload)
        .map_err(|_| PitrArchiveError::AuthenticationFailed)?
        .len();
    if decrypted != plaintext_len {
        return Err(PitrArchiveError::EncryptedLengthMismatch {
            declared: plaintext_len,
            actual: decrypted,
        });
    }
    payload.truncate(decrypted);
    Ok(payload)
}

fn packet_key(
    key: &PitrArchiveKey,
    nonce: [u8; NONCE_BYTES],
) -> Result<Box<dyn rustls::quic::PacketKey>, PitrArchiveError> {
    let tls13 = TLS13_CHACHA20_POLY1305_SHA256
        .tls13()
        .ok_or(PitrArchiveError::CryptoUnavailable)?;
    let algorithm = tls13.quic.ok_or(PitrArchiveError::CryptoUnavailable)?;
    if algorithm.aead_key_len() != PITR_KEY_BYTES {
        return Err(PitrArchiveError::CryptoUnavailable);
    }
    let packet = algorithm.packet_key(key.aead_key()?, Iv::from(nonce));
    if packet.tag_len() != TAG_BYTES {
        return Err(PitrArchiveError::CryptoUnavailable);
    }
    Ok(packet)
}

fn read_bounded_file(path: &Path, max: usize) -> Result<Vec<u8>, PitrArchiveError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(PitrArchiveError::InputNotRegularFile(path.to_path_buf()));
    }
    if metadata.len() > max as u64 {
        return Err(PitrArchiveError::SegmentTooLarge);
    }
    let file = File::open(path)?;
    let opened = file.metadata()?;
    if !opened.file_type().is_file() {
        return Err(PitrArchiveError::InputNotRegularFile(path.to_path_buf()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.dev() != opened.dev() || metadata.ino() != opened.ino() {
            return Err(PitrArchiveError::InputChangedDuringOpen(
                path.to_path_buf(),
            ));
        }
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(max as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(PitrArchiveError::SegmentTooLarge);
    }
    Ok(bytes)
}

fn now_unix_ms() -> Result<u64, PitrArchiveError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PitrArchiveError::ClockBeforeEpoch)?;
    u64::try_from(duration.as_millis()).map_err(|_| PitrArchiveError::ClockOverflow)
}

fn sync_dir(path: &Path) -> Result<(), PitrArchiveError> {
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

fn set_directory_permissions(path: &Path) -> Result<(), PitrArchiveError> {
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
pub enum PitrArchiveError {
    #[error("archive codec failure: {0}")]
    Codec(#[from] ArchiveCodecError),
    #[error("archive I/O failure: {0}")]
    Io(#[from] io::Error),
    #[error("archive destination is not empty: {0}")]
    DestinationNotEmpty(PathBuf),
    #[error("archive metadata already exists: {0}")]
    MetadataAlreadyExists(PathBuf),
    #[error("archive metadata serialization failed: {0}")]
    MetadataEncoding(String),
    #[error("archive metadata is not canonically encoded")]
    NonCanonicalMetadata,
    #[error("unsupported archive-stream metadata version {0}")]
    UnsupportedMetadataVersion(u8),
    #[error("unsupported archive format version in stream metadata {0}")]
    UnsupportedArchiveVersion(u8),
    #[error("unsupported state-machine compatibility version in stream metadata {0}")]
    UnsupportedStateMachineVersion(u16),
    #[error("invalid archive stream metadata: {0}")]
    InvalidMetadata(String),
    #[error("archive segments directory is missing: {0}")]
    MissingSegmentsDirectory(PathBuf),
    #[error("archive staging directory is missing: {0}")]
    MissingStagingDirectory(PathBuf),
    #[error("unexpected entry in archive directory: {0}")]
    UnexpectedArchiveEntry(PathBuf),
    #[error("archive chain gap: expected index {expected}, got {actual}")]
    Gap { expected: u64, actual: u64 },
    #[error("archive chain overlap: expected index {expected}, got {actual}")]
    Overlap { expected: u64, actual: u64 },
    #[error("archive segment {0} belongs to the wrong timeline")]
    WrongTimeline(u64),
    #[error("archive previous hash mismatch at index {0}")]
    PreviousHashMismatch(u64),
    #[error("archive filename index {filename_index} differs from record index {record_index}")]
    FilenameIndexMismatch {
        filename_index: u64,
        record_index: u64,
    },
    #[error("conflicting duplicate archive record at index {0}")]
    ConflictingDuplicate(u64),
    #[error("requested archive target {0} is unavailable")]
    TargetUnavailable(u64),
    #[error("archive recovery index overflow")]
    IndexOverflow,
    #[error("staged archive artifact failed verification")]
    StagedVerificationMismatch,
    #[error("PITR archive key is required for this encrypted stream")]
    KeyRequired,
    #[error("PITR archive key was supplied for a plaintext stream")]
    UnexpectedKey,
    #[error("PITR archive key does not match stream key id")]
    WrongKeyId,
    #[error("invalid PITR archive key configuration")]
    InvalidKeyConfiguration,
    #[error("PITR archive key is not a regular file: {0}")]
    KeyNotRegularFile(PathBuf),
    #[error("PITR archive key changed while being opened: {0}")]
    KeyChangedDuringOpen(PathBuf),
    #[error("PITR archive key permissions are too broad ({mode:#o}): {path}")]
    InsecureKeyPermissions { path: PathBuf, mode: u32 },
    #[error("PITR archive key must contain exactly {PITR_KEY_BYTES} raw bytes, got {actual}: {path}")]
    InvalidKeyLength { path: PathBuf, actual: usize },
    #[error("PITR archive key I/O failure for {path}: {source}")]
    KeyIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("archive input is not a regular file: {0}")]
    InputNotRegularFile(PathBuf),
    #[error("archive input changed while being opened: {0}")]
    InputChangedDuringOpen(PathBuf),
    #[error("archive segment exceeds bounded size")]
    SegmentTooLarge,
    #[error("system clock is before Unix epoch")]
    ClockBeforeEpoch,
    #[error("system clock does not fit milliseconds since Unix epoch")]
    ClockOverflow,
    #[error("secure random generation failed")]
    RandomFailure,
    #[error("required ChaCha20-Poly1305 crypto provider is unavailable")]
    CryptoUnavailable,
    #[error("PITR archive encryption failed")]
    EncryptionFailed,
    #[error("PITR archive authentication failed (wrong key or modified artifact)")]
    AuthenticationFailed,
    #[error("encrypted PITR segment is truncated")]
    EncryptedSegmentTruncated,
    #[error("invalid encrypted PITR segment magic")]
    InvalidEncryptedMagic,
    #[error("unsupported encrypted PITR segment version {0}")]
    UnsupportedEncryptedVersion(u8),
    #[error("unsupported encrypted PITR segment algorithm {0}")]
    UnsupportedEncryptedAlgorithm(u8),
    #[error("unsupported encrypted PITR segment flags")]
    UnsupportedEncryptedFlags,
    #[error("encrypted PITR segment timeline does not match stream")]
    EncryptedTimelineMismatch,
    #[error("encrypted PITR length mismatch: expected {declared}, got {actual}")]
    EncryptedLengthMismatch { declared: usize, actual: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::NeuralBaseBackup;
    use crate::consensus::ClusterMembership;
    use crate::replicated_identity_snapshot::ReplicatedIdentitySnapshotExtension;
    use crate::replicated_snapshot::{ReplicatedSqlSnapshot, SnapshotMetadata};
    use crate::replicated_sql::{ReplicatedMutation, ReplicatedRowWrite};
    use tempfile::TempDir;

    fn backup() -> NeuralBaseBackup {
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
            ClusterMembership::bootstrap("n1".into(), ["n2".into(), "n3".into()]),
            snapshot,
        )
        .unwrap()
    }

    fn entry(index: u64) -> LogEntry {
        LogEntry {
            term: 3,
            index,
            command: ReplicatedMutation::InsertRows {
                table: "pitr".into(),
                table_id: crate::storage_executor::table_id_for("pitr"),
                commit_ts: 100 + index,
                rows: vec![ReplicatedRowWrite {
                    primary_key: vec![index as u8],
                    value: vec![9],
                }],
            }
            .encode()
            .unwrap(),
        }
    }

    #[test]
    fn plaintext_publication_is_restart_safe_and_idempotent() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = backup();
        let baseline = backup.encode().unwrap();
        let initialized = PitrArchiveWriter::initialize(&root, &backup, &baseline, None).unwrap();
        assert_eq!(initialized.frontier.index, 5);
        let mut writer = PitrArchiveWriter::open(&root, None).unwrap();
        assert_eq!(writer.append_committed(&entry(6)).unwrap(), 6);
        assert_eq!(writer.append_committed(&entry(6)).unwrap(), 6);
        assert_eq!(writer.append_committed(&entry(7)).unwrap(), 7);
        drop(writer);
        let writer = PitrArchiveWriter::open(&root, None).unwrap();
        assert_eq!(writer.status().frontier.index, 7);
        assert_eq!(writer.status().frontier.segments, 2);
    }

    #[test]
    fn gap_and_conflicting_duplicate_fail_closed() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = backup();
        PitrArchiveWriter::initialize(&root, &backup, &backup.encode().unwrap(), None).unwrap();
        let mut writer = PitrArchiveWriter::open(&root, None).unwrap();
        assert!(matches!(
            writer.append_committed(&entry(7)),
            Err(PitrArchiveError::Gap {
                expected: 6,
                actual: 7
            })
        ));
        writer.append_committed(&entry(6)).unwrap();
        let mut changed = entry(6);
        changed.term = 4;
        assert!(matches!(
            writer.append_committed(&changed),
            Err(PitrArchiveError::ConflictingDuplicate(6))
        ));
    }

    #[test]
    fn encrypted_segments_require_key_and_reject_tampering() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = backup();
        let key = PitrArchiveKey::from_bytes([4u8; PITR_KEY_BYTES]);
        PitrArchiveWriter::initialize(&root, &backup, &backup.encode().unwrap(), Some(key)).unwrap();
        let mut writer = PitrArchiveWriter::open(
            &root,
            Some(PitrArchiveKey::from_bytes([4u8; PITR_KEY_BYTES])),
        )
        .unwrap();
        writer.append_committed(&entry(6)).unwrap();
        drop(writer);
        assert!(matches!(
            PitrArchiveWriter::open(&root, None),
            Err(PitrArchiveError::KeyRequired)
        ));
        assert!(matches!(
            PitrArchiveWriter::open(
                &root,
                Some(PitrArchiveKey::from_bytes([5u8; PITR_KEY_BYTES]))
            ),
            Err(PitrArchiveError::WrongKeyId)
        ));
        let path = segment_path(&root, 6, true);
        let mut bytes = fs::read(&path).unwrap();
        bytes[ENCRYPTED_HEADER_BYTES] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            PitrArchiveWriter::open(
                &root,
                Some(PitrArchiveKey::from_bytes([4u8; PITR_KEY_BYTES]))
            ),
            Err(PitrArchiveError::AuthenticationFailed)
        ));
    }

    #[test]
    fn metadata_rejects_branch_aliasing_parent_timeline() {
        let backup = backup();
        let timeline = [8u8; 16];
        let metadata = ArchiveStreamMetadata::from_backup(
            &backup,
            [3u8; 32],
            timeline,
            false,
            None,
        )
        .unwrap();
        let error = metadata
            .branched(timeline, 5, [3u8; 32], 2, 4, 5, false, None)
            .unwrap_err();
        assert!(matches!(error, PitrArchiveError::InvalidMetadata(_)));
    }
}
