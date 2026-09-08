// SPDX-License-Identifier: Apache-2.0
//! Versioned identity-state envelope carried inside replicated SQL snapshots.
//!
//! Phase-2 snapshots already provide one checksummed logical state-machine
//! artifact. Phase 4 uses the snapshot metadata-extension field as a nested,
//! explicitly versioned identity envelope rather than inventing a second copy
//! path. The envelope carries either an explicit uninitialized legacy state or
//! one canonical replicated identity registry.

use thiserror::Error;

use crate::replicated_identity_store::{IdentityStateError, ReplicatedIdentityState};

const MAGIC: &[u8; 4] = b"NBIX";
pub const IDENTITY_SNAPSHOT_EXTENSION_VERSION: u8 = 1;
const FLAG_UNINITIALIZED: u8 = 0;
const FLAG_INITIALIZED: u8 = 1;
const MAX_IDENTITY_STATE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatedIdentitySnapshotExtension {
    Uninitialized,
    Initialized(ReplicatedIdentityState),
}

#[derive(Debug, Error)]
pub enum IdentitySnapshotExtensionError {
    #[error("identity snapshot extension is truncated")]
    UnexpectedEof,
    #[error("invalid identity snapshot extension magic")]
    InvalidMagic,
    #[error("unsupported identity snapshot extension version {0}")]
    UnsupportedVersion(u8),
    #[error("invalid identity snapshot extension state flag {0}")]
    InvalidStateFlag(u8),
    #[error("identity snapshot state exceeds {MAX_IDENTITY_STATE_BYTES} bytes")]
    TooLarge,
    #[error("identity snapshot extension contains trailing bytes")]
    TrailingBytes,
    #[error("identity snapshot state is invalid: {0}")]
    InvalidState(#[from] IdentityStateError),
    #[error("identity snapshot state is not canonically encoded")]
    NonCanonicalState,
}

impl ReplicatedIdentitySnapshotExtension {
    pub fn encode(&self) -> Result<Vec<u8>, IdentitySnapshotExtensionError> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(IDENTITY_SNAPSHOT_EXTENSION_VERSION);
        match self {
            Self::Uninitialized => out.push(FLAG_UNINITIALIZED),
            Self::Initialized(state) => {
                out.push(FLAG_INITIALIZED);
                let encoded = state.encode()?;
                if encoded.len() > MAX_IDENTITY_STATE_BYTES {
                    return Err(IdentitySnapshotExtensionError::TooLarge);
                }
                let len = u32::try_from(encoded.len())
                    .map_err(|_| IdentitySnapshotExtensionError::TooLarge)?;
                out.extend_from_slice(&len.to_be_bytes());
                out.extend_from_slice(&encoded);
            }
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IdentitySnapshotExtensionError> {
        let mut reader = Reader::new(bytes);
        if reader.take(4)? != MAGIC {
            return Err(IdentitySnapshotExtensionError::InvalidMagic);
        }
        let version = reader.u8()?;
        if version != IDENTITY_SNAPSHOT_EXTENSION_VERSION {
            return Err(IdentitySnapshotExtensionError::UnsupportedVersion(version));
        }
        let extension = match reader.u8()? {
            FLAG_UNINITIALIZED => Self::Uninitialized,
            FLAG_INITIALIZED => {
                let len = reader.u32()? as usize;
                if len > MAX_IDENTITY_STATE_BYTES {
                    return Err(IdentitySnapshotExtensionError::TooLarge);
                }
                let raw = reader.take(len)?;
                let state = ReplicatedIdentityState::decode(raw)?;
                if state.encode()? != raw {
                    return Err(IdentitySnapshotExtensionError::NonCanonicalState);
                }
                Self::Initialized(state)
            }
            other => return Err(IdentitySnapshotExtensionError::InvalidStateFlag(other)),
        };
        if !reader.is_finished() {
            return Err(IdentitySnapshotExtensionError::TrailingBytes);
        }
        Ok(extension)
    }

    pub fn has_magic(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC)
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], IdentitySnapshotExtensionError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(IdentitySnapshotExtensionError::UnexpectedEof)?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(IdentitySnapshotExtensionError::UnexpectedEof)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, IdentitySnapshotExtensionError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, IdentitySnapshotExtensionError> {
        let raw: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| IdentitySnapshotExtensionError::UnexpectedEof)?;
        Ok(u32::from_be_bytes(raw))
    }

    fn is_finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicated_identity::{ReplicatedIdentityUser, ReplicatedScramCredential};

    fn state() -> ReplicatedIdentityState {
        ReplicatedIdentityState::new(vec![ReplicatedIdentityUser {
            username: "alice".to_string(),
            credential: ReplicatedScramCredential {
                salt: vec![7; 16],
                iterations: 4_096,
                stored_key: [8; 32],
                server_key: [9; 32],
            },
        }])
        .unwrap()
    }

    #[test]
    fn initialized_identity_snapshot_roundtrips_canonically() {
        let extension = ReplicatedIdentitySnapshotExtension::Initialized(state());
        let encoded = extension.encode().unwrap();
        assert!(ReplicatedIdentitySnapshotExtension::has_magic(&encoded));
        assert_eq!(
            ReplicatedIdentitySnapshotExtension::decode(&encoded).unwrap(),
            extension
        );
    }

    #[test]
    fn uninitialized_identity_snapshot_is_explicit() {
        let encoded = ReplicatedIdentitySnapshotExtension::Uninitialized
            .encode()
            .unwrap();
        assert_eq!(
            ReplicatedIdentitySnapshotExtension::decode(&encoded).unwrap(),
            ReplicatedIdentitySnapshotExtension::Uninitialized
        );
    }

    #[test]
    fn corrupt_identity_snapshot_extension_fails_closed() {
        let mut encoded = ReplicatedIdentitySnapshotExtension::Initialized(state())
            .encode()
            .unwrap();
        encoded.truncate(encoded.len() - 3);
        assert!(ReplicatedIdentitySnapshotExtension::decode(&encoded).is_err());
    }
}
