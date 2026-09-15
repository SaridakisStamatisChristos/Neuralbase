// SPDX-License-Identifier: Apache-2.0
//! Versioned logical recovery records for Phase-9 point-in-time recovery.
//!
//! Archive v1 records every committed Raft position after a verified operator
//! backup boundary. SQL and identity payloads are the already-canonical NBRM
//! and NBRI command bytes. Membership payloads are the committed membership
//! command bytes. Known Raft/control positions are represented explicitly so
//! an absent index is always a detectable archive gap.

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::backup::BACKUP_STATE_MACHINE_COMPAT_VERSION;
use crate::consensus::{LogEntry, MembershipChange, MEMBERSHIP_CHANGE_TAG};
use crate::replicated_identity::is_replicated_identity_mutation;
use crate::replicated_sql::is_replicated_mutation;

const MAGIC: &[u8; 4] = b"NBAR";
const SQL_READINESS_BARRIER_V1: &[u8] = b"NBRB\x01";
pub const ARCHIVE_FORMAT_VERSION: u8 = 1;
pub const ARCHIVE_STATE_MACHINE_COMPAT_VERSION: u16 = BACKUP_STATE_MACHINE_COMPAT_VERSION;
pub const ARCHIVE_TIMELINE_BYTES: usize = 16;
pub const ARCHIVE_HASH_BYTES: usize = 32;
pub const MAX_ARCHIVE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const CHECKSUM_BYTES: usize = ARCHIVE_HASH_BYTES;
const HEADER_BYTES: usize = 4
    + 1
    + 1
    + 1
    + 1
    + 2
    + ARCHIVE_TIMELINE_BYTES
    + 8
    + 8
    + 8
    + 8
    + ARCHIVE_HASH_BYTES
    + ARCHIVE_HASH_BYTES
    + 4;
const KNOWN_FLAGS: u8 = 0;

pub type TimelineId = [u8; ARCHIVE_TIMELINE_BYTES];
pub type ArchiveHash = [u8; ARCHIVE_HASH_BYTES];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecoveryRecordKind {
    Sql = 1,
    Identity = 2,
    Membership = 3,
    Control = 4,
}

impl RecoveryRecordKind {
    fn decode(value: u8) -> Result<Self, ArchiveCodecError> {
        match value {
            1 => Ok(Self::Sql),
            2 => Ok(Self::Identity),
            3 => Ok(Self::Membership),
            4 => Ok(Self::Control),
            other => Err(ArchiveCodecError::UnsupportedRecordKind(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRecord {
    pub index: u64,
    pub term: u64,
    pub kind: RecoveryRecordKind,
    pub payload: Vec<u8>,
}

impl RecoveryRecord {
    pub fn from_log_entry(entry: &LogEntry) -> Result<Self, ArchiveCodecError> {
        if entry.index == 0 || entry.term == 0 {
            return Err(ArchiveCodecError::InvalidBoundary);
        }
        if entry.command.len() > MAX_ARCHIVE_PAYLOAD_BYTES {
            return Err(ArchiveCodecError::PayloadTooLarge);
        }
        let kind = if is_replicated_mutation(&entry.command) {
            // Strictly decode now. Archive publication must never preserve bytes
            // that merely share the NBRM prefix but are not a valid command.
            crate::replicated_sql::ReplicatedMutation::decode(&entry.command)
                .map_err(|error| ArchiveCodecError::InvalidSql(error.to_string()))?;
            RecoveryRecordKind::Sql
        } else if is_replicated_identity_mutation(&entry.command) {
            crate::replicated_identity::ReplicatedIdentityMutation::decode(&entry.command)
                .map_err(|error| ArchiveCodecError::InvalidIdentity(error.to_string()))?;
            RecoveryRecordKind::Identity
        } else if entry.command.starts_with(MEMBERSHIP_CHANGE_TAG) {
            decode_membership_payload(&entry.command)?;
            RecoveryRecordKind::Membership
        } else if entry.command.is_empty() || entry.command.as_slice() == SQL_READINESS_BARRIER_V1 {
            RecoveryRecordKind::Control
        } else {
            // Unknown non-empty entries are not assumed to be harmless. If a
            // future state-machine command is introduced, archive v1 must learn
            // its semantics explicitly before PITR-enabled nodes can acknowledge it.
            return Err(ArchiveCodecError::UnsupportedCommittedCommand);
        };
        Ok(Self {
            index: entry.index,
            term: entry.term,
            kind,
            payload: entry.command.clone(),
        })
    }

    pub fn to_log_entry(&self) -> LogEntry {
        LogEntry {
            term: self.term,
            index: self.index,
            command: self.payload.clone(),
        }
    }

    pub fn membership_change(&self) -> Result<Option<MembershipChange>, ArchiveCodecError> {
        if self.kind != RecoveryRecordKind::Membership {
            return Ok(None);
        }
        decode_membership_payload(&self.payload).map(Some)
    }

    fn validate(&self) -> Result<(), ArchiveCodecError> {
        if self.index == 0 || self.term == 0 {
            return Err(ArchiveCodecError::InvalidBoundary);
        }
        if self.payload.len() > MAX_ARCHIVE_PAYLOAD_BYTES {
            return Err(ArchiveCodecError::PayloadTooLarge);
        }
        match self.kind {
            RecoveryRecordKind::Sql => {
                if !is_replicated_mutation(&self.payload) {
                    return Err(ArchiveCodecError::RecordKindPayloadMismatch);
                }
                crate::replicated_sql::ReplicatedMutation::decode(&self.payload)
                    .map_err(|error| ArchiveCodecError::InvalidSql(error.to_string()))?;
            }
            RecoveryRecordKind::Identity => {
                if !is_replicated_identity_mutation(&self.payload) {
                    return Err(ArchiveCodecError::RecordKindPayloadMismatch);
                }
                crate::replicated_identity::ReplicatedIdentityMutation::decode(&self.payload)
                    .map_err(|error| ArchiveCodecError::InvalidIdentity(error.to_string()))?;
            }
            RecoveryRecordKind::Membership => {
                decode_membership_payload(&self.payload)?;
            }
            RecoveryRecordKind::Control => {
                if !self.payload.is_empty() && self.payload.as_slice() != SQL_READINESS_BARRIER_V1 {
                    return Err(ArchiveCodecError::RecordKindPayloadMismatch);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveSegment {
    pub timeline: TimelineId,
    pub state_machine_compat_version: u16,
    pub created_unix_ms: u64,
    pub previous_hash: ArchiveHash,
    pub record: RecoveryRecord,
}

impl ArchiveSegment {
    pub fn new(
        timeline: TimelineId,
        created_unix_ms: u64,
        previous_hash: ArchiveHash,
        record: RecoveryRecord,
    ) -> Result<Self, ArchiveCodecError> {
        let segment = Self {
            timeline,
            state_machine_compat_version: ARCHIVE_STATE_MACHINE_COMPAT_VERSION,
            created_unix_ms,
            previous_hash,
            record,
        };
        segment.validate()?;
        Ok(segment)
    }

    pub fn encode(&self) -> Result<Vec<u8>, ArchiveCodecError> {
        self.validate()?;
        let payload_len = u32::try_from(self.record.payload.len())
            .map_err(|_| ArchiveCodecError::PayloadTooLarge)?;
        let payload_hash: ArchiveHash = Sha256::digest(&self.record.payload).into();
        let mut out = Vec::with_capacity(HEADER_BYTES + self.record.payload.len() + CHECKSUM_BYTES);
        out.extend_from_slice(MAGIC);
        out.push(ARCHIVE_FORMAT_VERSION);
        out.push(KNOWN_FLAGS);
        out.push(self.record.kind as u8);
        out.push(0); // reserved
        out.extend_from_slice(&self.state_machine_compat_version.to_be_bytes());
        out.extend_from_slice(&self.timeline);
        out.extend_from_slice(&self.created_unix_ms.to_be_bytes());
        out.extend_from_slice(&self.record.index.to_be_bytes());
        out.extend_from_slice(&self.record.index.to_be_bytes()); // v1: one record per segment
        out.extend_from_slice(&self.record.term.to_be_bytes());
        out.extend_from_slice(&self.previous_hash);
        out.extend_from_slice(&payload_hash);
        out.extend_from_slice(&payload_len.to_be_bytes());
        debug_assert_eq!(out.len(), HEADER_BYTES);
        out.extend_from_slice(&self.record.payload);
        let checksum: ArchiveHash = Sha256::digest(&out).into();
        out.extend_from_slice(&checksum);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ArchiveCodecError> {
        let max = HEADER_BYTES
            .checked_add(MAX_ARCHIVE_PAYLOAD_BYTES)
            .and_then(|value| value.checked_add(CHECKSUM_BYTES))
            .ok_or(ArchiveCodecError::PayloadTooLarge)?;
        if bytes.len() > max {
            return Err(ArchiveCodecError::PayloadTooLarge);
        }
        if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES {
            return Err(ArchiveCodecError::UnexpectedEof);
        }
        let content_len = bytes.len() - CHECKSUM_BYTES;
        let (content, checksum) = bytes.split_at(content_len);
        let expected: ArchiveHash = Sha256::digest(content).into();
        if checksum != expected {
            return Err(ArchiveCodecError::ChecksumMismatch);
        }

        let mut reader = Reader::new(content);
        if reader.take(4)? != MAGIC {
            return Err(ArchiveCodecError::InvalidMagic);
        }
        let version = reader.u8()?;
        if version != ARCHIVE_FORMAT_VERSION {
            return Err(ArchiveCodecError::UnsupportedVersion(version));
        }
        let flags = reader.u8()?;
        if flags != KNOWN_FLAGS {
            return Err(ArchiveCodecError::UnsupportedFlags(flags));
        }
        let kind = RecoveryRecordKind::decode(reader.u8()?)?;
        if reader.u8()? != 0 {
            return Err(ArchiveCodecError::ReservedFieldNonZero);
        }
        let state_machine_compat_version = reader.u16()?;
        if state_machine_compat_version != ARCHIVE_STATE_MACHINE_COMPAT_VERSION {
            return Err(ArchiveCodecError::UnsupportedStateMachineVersion(
                state_machine_compat_version,
            ));
        }
        let timeline: TimelineId = reader
            .take(ARCHIVE_TIMELINE_BYTES)?
            .try_into()
            .map_err(|_| ArchiveCodecError::UnexpectedEof)?;
        let created_unix_ms = reader.u64()?;
        let first_index = reader.u64()?;
        let last_index = reader.u64()?;
        if first_index == 0 || first_index != last_index {
            return Err(ArchiveCodecError::InvalidBoundary);
        }
        let term = reader.u64()?;
        if term == 0 {
            return Err(ArchiveCodecError::InvalidBoundary);
        }
        let previous_hash: ArchiveHash = reader
            .take(ARCHIVE_HASH_BYTES)?
            .try_into()
            .map_err(|_| ArchiveCodecError::UnexpectedEof)?;
        let expected_payload_hash: ArchiveHash = reader
            .take(ARCHIVE_HASH_BYTES)?
            .try_into()
            .map_err(|_| ArchiveCodecError::UnexpectedEof)?;
        let payload_len = reader.u32()? as usize;
        if payload_len > MAX_ARCHIVE_PAYLOAD_BYTES {
            return Err(ArchiveCodecError::PayloadTooLarge);
        }
        let declared = HEADER_BYTES
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(CHECKSUM_BYTES))
            .ok_or(ArchiveCodecError::PayloadTooLarge)?;
        if declared != bytes.len() {
            return Err(ArchiveCodecError::LengthMismatch {
                declared,
                actual: bytes.len(),
            });
        }
        let payload = reader.take(payload_len)?.to_vec();
        if !reader.is_finished() {
            return Err(ArchiveCodecError::TrailingBytes);
        }
        let actual_payload_hash: ArchiveHash = Sha256::digest(&payload).into();
        if actual_payload_hash != expected_payload_hash {
            return Err(ArchiveCodecError::PayloadHashMismatch);
        }
        let segment = Self {
            timeline,
            state_machine_compat_version,
            created_unix_ms,
            previous_hash,
            record: RecoveryRecord {
                index: first_index,
                term,
                kind,
                payload,
            },
        };
        segment.validate()?;
        // Canonical re-encoding is part of acceptance. It catches any future
        // decoder relaxation that could otherwise create two accepted byte forms.
        if segment.encode()?.as_slice() != bytes {
            return Err(ArchiveCodecError::NonCanonicalEncoding);
        }
        Ok(segment)
    }

    pub fn hash(&self) -> Result<ArchiveHash, ArchiveCodecError> {
        Ok(Sha256::digest(self.encode()?).into())
    }

    fn validate(&self) -> Result<(), ArchiveCodecError> {
        if self.timeline == [0u8; ARCHIVE_TIMELINE_BYTES] {
            return Err(ArchiveCodecError::ZeroTimeline);
        }
        if self.state_machine_compat_version != ARCHIVE_STATE_MACHINE_COMPAT_VERSION {
            return Err(ArchiveCodecError::UnsupportedStateMachineVersion(
                self.state_machine_compat_version,
            ));
        }
        self.record.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveChainSpec {
    pub timeline: TimelineId,
    pub baseline_index: u64,
    pub baseline_term: u64,
    pub baseline_backup_sha256: ArchiveHash,
}

impl ArchiveChainSpec {
    pub fn validate(&self) -> Result<(), ArchiveCodecError> {
        if self.timeline == [0u8; ARCHIVE_TIMELINE_BYTES] {
            return Err(ArchiveCodecError::ZeroTimeline);
        }
        if self.baseline_index == 0 && self.baseline_term != 0 {
            return Err(ArchiveCodecError::InvalidBoundary);
        }
        if self.baseline_index != 0 && self.baseline_term == 0 {
            return Err(ArchiveCodecError::InvalidBoundary);
        }
        Ok(())
    }

    pub fn baseline_anchor(&self) -> Result<ArchiveHash, ArchiveCodecError> {
        self.validate()?;
        let mut hasher = Sha256::new();
        hasher.update(b"NeuralBase-PITR-v1-baseline-anchor");
        hasher.update(self.timeline);
        hasher.update(self.baseline_index.to_be_bytes());
        hasher.update(self.baseline_term.to_be_bytes());
        hasher.update(self.baseline_backup_sha256);
        Ok(hasher.finalize().into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveFrontier {
    pub index: u64,
    pub term: u64,
    pub segment_hash: ArchiveHash,
    pub segments: u64,
}

pub fn verify_archive_chain(
    spec: &ArchiveChainSpec,
    encoded_segments: &[Vec<u8>],
) -> Result<ArchiveFrontier, ArchiveCodecError> {
    spec.validate()?;
    let mut expected_index = spec
        .baseline_index
        .checked_add(1)
        .ok_or(ArchiveCodecError::IndexOverflow)?;
    let mut previous_hash = spec.baseline_anchor()?;
    let mut frontier = ArchiveFrontier {
        index: spec.baseline_index,
        term: spec.baseline_term,
        segment_hash: previous_hash,
        segments: 0,
    };

    for encoded in encoded_segments {
        let segment = ArchiveSegment::decode(encoded)?;
        if segment.timeline != spec.timeline {
            return Err(ArchiveCodecError::WrongTimeline);
        }
        if segment.record.index < expected_index {
            return Err(ArchiveCodecError::Overlap {
                expected: expected_index,
                actual: segment.record.index,
            });
        }
        if segment.record.index > expected_index {
            return Err(ArchiveCodecError::Gap {
                expected: expected_index,
                actual: segment.record.index,
            });
        }
        if segment.previous_hash != previous_hash {
            return Err(ArchiveCodecError::PreviousHashMismatch);
        }
        previous_hash = segment.hash()?;
        frontier = ArchiveFrontier {
            index: segment.record.index,
            term: segment.record.term,
            segment_hash: previous_hash,
            segments: frontier.segments + 1,
        };
        expected_index = expected_index
            .checked_add(1)
            .ok_or(ArchiveCodecError::IndexOverflow)?;
    }
    Ok(frontier)
}

pub fn decode_membership_payload(bytes: &[u8]) -> Result<MembershipChange, ArchiveCodecError> {
    if !bytes.starts_with(MEMBERSHIP_CHANGE_TAG) {
        return Err(ArchiveCodecError::RecordKindPayloadMismatch);
    }
    let change: MembershipChange = serde_json::from_slice(&bytes[MEMBERSHIP_CHANGE_TAG.len()..])
        .map_err(|error| ArchiveCodecError::InvalidMembership(error.to_string()))?;
    if crate::consensus::encode_membership_change(&change).as_slice() != bytes {
        return Err(ArchiveCodecError::NonCanonicalMembership);
    }
    Ok(change)
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ArchiveCodecError {
    #[error("archive segment is truncated")]
    UnexpectedEof,
    #[error("invalid NeuralBase archive magic")]
    InvalidMagic,
    #[error("unsupported archive format version {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported archive flags {0:#04x}")]
    UnsupportedFlags(u8),
    #[error("unsupported archive record kind {0}")]
    UnsupportedRecordKind(u8),
    #[error("archive reserved field is nonzero")]
    ReservedFieldNonZero,
    #[error("unsupported archive state-machine compatibility version {0}")]
    UnsupportedStateMachineVersion(u16),
    #[error("archive timeline must not be all zeroes")]
    ZeroTimeline,
    #[error("archive boundary index/term is impossible")]
    InvalidBoundary,
    #[error("archive payload exceeds {MAX_ARCHIVE_PAYLOAD_BYTES} bytes")]
    PayloadTooLarge,
    #[error("archive length mismatch: header declares {declared} bytes, got {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("archive payload SHA-256 mismatch")]
    PayloadHashMismatch,
    #[error("archive segment SHA-256 checksum mismatch")]
    ChecksumMismatch,
    #[error("archive segment contains trailing ambiguous bytes")]
    TrailingBytes,
    #[error("archive segment is not canonically encoded")]
    NonCanonicalEncoding,
    #[error("archive record kind does not match its payload")]
    RecordKindPayloadMismatch,
    #[error("invalid archived SQL mutation: {0}")]
    InvalidSql(String),
    #[error("invalid archived identity mutation: {0}")]
    InvalidIdentity(String),
    #[error("invalid archived membership mutation: {0}")]
    InvalidMembership(String),
    #[error("archived membership mutation is not canonically encoded")]
    NonCanonicalMembership,
    #[error("committed non-empty command has no Phase-9 recovery semantics")]
    UnsupportedCommittedCommand,
    #[error("archive chain belongs to a different timeline")]
    WrongTimeline,
    #[error("archive chain gap: expected index {expected}, got {actual}")]
    Gap { expected: u64, actual: u64 },
    #[error("archive chain overlap/out-of-order record: expected index {expected}, got {actual}")]
    Overlap { expected: u64, actual: u64 },
    #[error("archive previous-segment linkage hash mismatch")]
    PreviousHashMismatch,
    #[error("archive recovery index overflow")]
    IndexOverflow,
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], ArchiveCodecError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ArchiveCodecError::UnexpectedEof)?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(ArchiveCodecError::UnexpectedEof)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, ArchiveCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ArchiveCodecError> {
        let raw: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| ArchiveCodecError::UnexpectedEof)?;
        Ok(u16::from_be_bytes(raw))
    }

    fn u32(&mut self) -> Result<u32, ArchiveCodecError> {
        let raw: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ArchiveCodecError::UnexpectedEof)?;
        Ok(u32::from_be_bytes(raw))
    }

    fn u64(&mut self) -> Result<u64, ArchiveCodecError> {
        let raw: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| ArchiveCodecError::UnexpectedEof)?;
        Ok(u64::from_be_bytes(raw))
    }

    fn is_finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::encode_membership_change;
    use crate::replicated_identity::{ReplicatedIdentityMutation, ReplicatedScramCredential};
    use crate::replicated_sql::{ReplicatedMutation, ReplicatedRowWrite};

    fn timeline() -> TimelineId {
        [7u8; ARCHIVE_TIMELINE_BYTES]
    }

    fn backup_hash() -> ArchiveHash {
        [9u8; ARCHIVE_HASH_BYTES]
    }

    fn reseal_segment(bytes: &mut [u8]) {
        let content_len = bytes.len() - CHECKSUM_BYTES;
        let checksum: ArchiveHash = Sha256::digest(&bytes[..content_len]).into();
        bytes[content_len..].copy_from_slice(&checksum);
    }

    fn spec() -> ArchiveChainSpec {
        ArchiveChainSpec {
            timeline: timeline(),
            baseline_index: 10,
            baseline_term: 3,
            baseline_backup_sha256: backup_hash(),
        }
    }

    fn sql_entry(index: u64) -> LogEntry {
        LogEntry {
            term: 4,
            index,
            command: ReplicatedMutation::InsertRows {
                table: "t".into(),
                table_id: crate::storage_executor::table_id_for("t"),
                commit_ts: index * 100,
                rows: vec![ReplicatedRowWrite {
                    primary_key: vec![index as u8],
                    value: vec![1, 2, 3],
                }],
            }
            .encode()
            .unwrap(),
        }
    }

    #[test]
    fn segment_roundtrip_is_canonical() {
        let record = RecoveryRecord::from_log_entry(&sql_entry(11)).unwrap();
        let segment =
            ArchiveSegment::new(timeline(), 1234, spec().baseline_anchor().unwrap(), record)
                .unwrap();
        let bytes = segment.encode().unwrap();
        assert_eq!(ArchiveSegment::decode(&bytes).unwrap(), segment);
    }

    #[test]
    fn version_compatibility_and_extra_bytes_fail_with_valid_checksum() {
        let record = RecoveryRecord::from_log_entry(&sql_entry(11)).unwrap();
        let segment =
            ArchiveSegment::new(timeline(), 1234, spec().baseline_anchor().unwrap(), record)
                .unwrap();
        let bytes = segment.encode().unwrap();

        let mut wrong_version = bytes.clone();
        wrong_version[4] = ARCHIVE_FORMAT_VERSION + 1;
        reseal_segment(&mut wrong_version);
        assert_eq!(
            ArchiveSegment::decode(&wrong_version).unwrap_err(),
            ArchiveCodecError::UnsupportedVersion(ARCHIVE_FORMAT_VERSION + 1)
        );

        let mut wrong_state_machine = bytes.clone();
        let incompatible = ARCHIVE_STATE_MACHINE_COMPAT_VERSION + 1;
        wrong_state_machine[8..10].copy_from_slice(&incompatible.to_be_bytes());
        reseal_segment(&mut wrong_state_machine);
        assert_eq!(
            ArchiveSegment::decode(&wrong_state_machine).unwrap_err(),
            ArchiveCodecError::UnsupportedStateMachineVersion(incompatible)
        );

        let mut trailing = bytes;
        let checksum_offset = trailing.len() - CHECKSUM_BYTES;
        trailing.insert(checksum_offset, 0xA5);
        reseal_segment(&mut trailing);
        assert!(matches!(
            ArchiveSegment::decode(&trailing),
            Err(ArchiveCodecError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn membership_payload_must_be_canonical() {
        let canonical = encode_membership_change(&MembershipChange::AddLearner("n4".into()));
        let mut noncanonical = canonical;
        noncanonical.insert(MEMBERSHIP_CHANGE_TAG.len(), b' ');
        let error = RecoveryRecord::from_log_entry(&LogEntry {
            term: 4,
            index: 11,
            command: noncanonical,
        })
        .unwrap_err();
        assert_eq!(error, ArchiveCodecError::NonCanonicalMembership);
    }

    #[test]
    fn truncation_and_corruption_fail_closed() {
        let record = RecoveryRecord::from_log_entry(&sql_entry(11)).unwrap();
        let segment =
            ArchiveSegment::new(timeline(), 1234, spec().baseline_anchor().unwrap(), record)
                .unwrap();
        let bytes = segment.encode().unwrap();
        assert!(matches!(
            ArchiveSegment::decode(&bytes[..bytes.len() - 1]),
            Err(ArchiveCodecError::ChecksumMismatch | ArchiveCodecError::LengthMismatch { .. })
        ));
        let mut corrupt = bytes;
        let offset = HEADER_BYTES;
        corrupt[offset] ^= 1;
        assert_eq!(
            ArchiveSegment::decode(&corrupt).unwrap_err(),
            ArchiveCodecError::ChecksumMismatch
        );
    }

    #[test]
    fn chain_detects_gap_overlap_link_and_timeline_errors() {
        let s = spec();
        let first = ArchiveSegment::new(
            s.timeline,
            1,
            s.baseline_anchor().unwrap(),
            RecoveryRecord::from_log_entry(&sql_entry(11)).unwrap(),
        )
        .unwrap();
        let first_bytes = first.encode().unwrap();
        let second = ArchiveSegment::new(
            s.timeline,
            2,
            first.hash().unwrap(),
            RecoveryRecord::from_log_entry(&sql_entry(12)).unwrap(),
        )
        .unwrap();
        let second_bytes = second.encode().unwrap();
        let frontier =
            verify_archive_chain(&s, &[first_bytes.clone(), second_bytes.clone()]).unwrap();
        assert_eq!(frontier.index, 12);

        let gap = ArchiveSegment::new(
            s.timeline,
            3,
            first.hash().unwrap(),
            RecoveryRecord::from_log_entry(&sql_entry(13)).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            verify_archive_chain(&s, &[first_bytes.clone(), gap.encode().unwrap()]),
            Err(ArchiveCodecError::Gap {
                expected: 12,
                actual: 13
            })
        ));
        assert!(matches!(
            verify_archive_chain(&s, &[first_bytes.clone(), first_bytes.clone()]),
            Err(ArchiveCodecError::Overlap {
                expected: 12,
                actual: 11
            })
        ));

        let bad_link = ArchiveSegment::new(
            s.timeline,
            4,
            [8u8; 32],
            RecoveryRecord::from_log_entry(&sql_entry(12)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            verify_archive_chain(&s, &[first_bytes.clone(), bad_link.encode().unwrap()])
                .unwrap_err(),
            ArchiveCodecError::PreviousHashMismatch
        );

        let wrong_timeline = ArchiveSegment::new(
            [2u8; 16],
            5,
            first.hash().unwrap(),
            RecoveryRecord::from_log_entry(&sql_entry(12)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            verify_archive_chain(&s, &[first_bytes, wrong_timeline.encode().unwrap()]).unwrap_err(),
            ArchiveCodecError::WrongTimeline
        );
    }

    #[test]
    fn all_current_recoverable_command_families_are_classified() {
        let identity = ReplicatedIdentityMutation::CreateUser {
            username: "alice".into(),
            credential: ReplicatedScramCredential {
                salt: vec![1; 16],
                iterations: 4096,
                stored_key: [2; 32],
                server_key: [3; 32],
            },
        }
        .encode()
        .unwrap();
        let membership = encode_membership_change(&MembershipChange::AddLearner("n4".into()));
        let entries = [
            sql_entry(11),
            LogEntry {
                term: 4,
                index: 12,
                command: identity,
            },
            LogEntry {
                term: 4,
                index: 13,
                command: membership,
            },
            LogEntry {
                term: 4,
                index: 14,
                command: vec![],
            },
            LogEntry {
                term: 4,
                index: 15,
                command: SQL_READINESS_BARRIER_V1.to_vec(),
            },
        ];
        let kinds: Vec<_> = entries
            .iter()
            .map(|entry| RecoveryRecord::from_log_entry(entry).unwrap().kind)
            .collect();
        assert_eq!(
            kinds,
            vec![
                RecoveryRecordKind::Sql,
                RecoveryRecordKind::Identity,
                RecoveryRecordKind::Membership,
                RecoveryRecordKind::Control,
                RecoveryRecordKind::Control,
            ]
        );
    }

    #[test]
    fn known_readiness_barrier_is_control_but_near_match_fails_closed() {
        let record = RecoveryRecord::from_log_entry(&LogEntry {
            term: 1,
            index: 1,
            command: SQL_READINESS_BARRIER_V1.to_vec(),
        })
        .unwrap();
        assert_eq!(record.kind, RecoveryRecordKind::Control);
        let bytes = ArchiveSegment::new(timeline(), 1, [0u8; 32], record)
            .unwrap()
            .encode()
            .unwrap();
        assert_eq!(
            ArchiveSegment::decode(&bytes).unwrap().record.payload,
            SQL_READINESS_BARRIER_V1
        );

        let error = RecoveryRecord::from_log_entry(&LogEntry {
            term: 1,
            index: 2,
            command: b"NBRB\x02".to_vec(),
        })
        .unwrap_err();
        assert_eq!(error, ArchiveCodecError::UnsupportedCommittedCommand);
    }

    #[test]
    fn unknown_nonempty_command_is_rejected() {
        let error = RecoveryRecord::from_log_entry(&LogEntry {
            term: 1,
            index: 1,
            command: b"future-mutator".to_vec(),
        })
        .unwrap_err();
        assert_eq!(error, ArchiveCodecError::UnsupportedCommittedCommand);
    }
}
