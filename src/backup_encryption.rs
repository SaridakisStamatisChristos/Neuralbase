// SPDX-License-Identifier: Apache-2.0
//! Authenticated encryption for operator-facing NeuralBase backups.
//!
//! NBEC v1 wraps the strict NBBK payload rather than replacing it. The outer
//! header is authenticated as AEAD additional data, while the inner NBBK is
//! explicitly marked as encrypted. Decryption therefore requires both a valid
//! ChaCha20-Poly1305 tag and the marked inner payload, preventing downgrade or
//! relabelling of plaintext backups.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::{rngs::OsRng, RngCore};
use rustls::crypto::cipher::{AeadKey, Iv};
use rustls::crypto::ring::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256;
use rustls::crypto::SharedSecret;
use thiserror::Error;

use crate::backup::{BackupCodecError, BackupManifest, NeuralBaseBackup, MAX_BACKUP_BYTES};
use crate::offline_backup::{capture_offline_backup_at, OfflineBackupError};

const MAGIC: &[u8; 4] = b"NBEC";
pub const ENCRYPTED_BACKUP_FORMAT_VERSION: u8 = 1;
pub const ENCRYPTED_BACKUP_ALGORITHM_CHACHA20_POLY1305: u8 = 1;
pub const BACKUP_ENCRYPTION_KEY_BYTES: usize = 32;
const NONCE_BYTES: usize = 12;
const TAG_BYTES: usize = 16;
const HEADER_BYTES: usize = 32;
pub const MAX_ENCRYPTED_BACKUP_BYTES: usize = HEADER_BYTES + MAX_BACKUP_BYTES + TAG_BYTES;

/// A 256-bit operator backup key.
///
/// The secret is held in rustls's zeroizing `SharedSecret` container and is
/// intentionally neither cloneable nor printable. Callers should prefer
/// [`load_backup_encryption_key`] so key bytes never appear in process argv.
pub struct BackupEncryptionKey {
    secret: SharedSecret,
}

impl BackupEncryptionKey {
    pub fn from_bytes(bytes: [u8; BACKUP_ENCRYPTION_KEY_BYTES]) -> Self {
        Self {
            secret: SharedSecret::from(Vec::from(bytes)),
        }
    }

    fn from_vec(bytes: Vec<u8>) -> Result<Self, usize> {
        let secret = SharedSecret::from(bytes);
        let actual = secret.secret_bytes().len();
        if actual != BACKUP_ENCRYPTION_KEY_BYTES {
            return Err(actual);
        }
        Ok(Self { secret })
    }

    fn aead_key(&self) -> Result<AeadKey, BackupEncryptionError> {
        let bytes: [u8; BACKUP_ENCRYPTION_KEY_BYTES] = self
            .secret
            .secret_bytes()
            .try_into()
            .map_err(|_| BackupEncryptionError::CryptoUnavailable)?;
        Ok(AeadKey::from(bytes))
    }
}

impl fmt::Debug for BackupEncryptionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BackupEncryptionKey([REDACTED])")
    }
}

#[derive(Debug, Error)]
pub enum BackupEncryptionError {
    #[error("backup encryption key is not a regular file: {0}")]
    KeyNotRegularFile(PathBuf),
    #[error("backup encryption key changed while being opened: {0}")]
    KeyChangedDuringOpen(PathBuf),
    #[error("backup encryption key permissions are too broad ({mode:#o}): {path}")]
    InsecureKeyPermissions { path: PathBuf, mode: u32 },
    #[error("backup encryption key must contain exactly {BACKUP_ENCRYPTION_KEY_BYTES} raw bytes, got {actual}: {path}")]
    InvalidKeyLength { path: PathBuf, actual: usize },
    #[error("backup encryption key I/O failure for {path}: {source}")]
    KeyIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("encrypted backup input is not a regular file: {0}")]
    InputNotRegularFile(PathBuf),
    #[error("encrypted backup input changed while being opened: {0}")]
    InputChangedDuringOpen(PathBuf),
    #[error("encrypted backup file I/O failure for {path}: {source}")]
    FileIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("encrypted backup destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("encrypted backup destination must have an existing parent directory")]
    DestinationParentMissing,
    #[error("encrypted backup destination must name a file")]
    DestinationFileNameMissing,
    #[error("captured offline backup failed: {0}")]
    Offline(#[from] OfflineBackupError),
    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,
    #[error("backup timestamp does not fit milliseconds since Unix epoch")]
    ClockOverflow,
    #[error("staged encrypted backup did not decode to the captured logical backup")]
    StagedVerificationMismatch,
    #[error("secure random nonce generation failed")]
    RandomFailure,
    #[error("required ChaCha20-Poly1305 backup crypto provider is unavailable")]
    CryptoUnavailable,
    #[error("backup encryption failed")]
    EncryptFailed,
    #[error("backup authentication failed (wrong key or modified artifact)")]
    AuthenticationFailed,
    #[error("encrypted NeuralBase backup exceeds {MAX_ENCRYPTED_BACKUP_BYTES} bytes")]
    TooLarge,
    #[error("encrypted NeuralBase backup is truncated")]
    UnexpectedEof,
    #[error("invalid encrypted NeuralBase backup magic")]
    InvalidMagic,
    #[error("unsupported encrypted NeuralBase backup format version {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported encrypted NeuralBase backup algorithm {0}")]
    UnsupportedAlgorithm(u8),
    #[error("unsupported encrypted NeuralBase backup flags {0:#06x}")]
    UnsupportedFlags(u16),
    #[error("encrypted NeuralBase backup reserved header bytes are nonzero")]
    ReservedFieldNonZero,
    #[error("encrypted backup length mismatch: header declares {declared} bytes, got {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("invalid authenticated NBBK payload: {0}")]
    Backup(#[from] BackupCodecError),
}

/// Load an exact raw 32-byte backup key from a regular file.
///
/// On Unix, group/other permission bits are rejected. The path is inspected
/// before and after open, and device/inode identity is compared to fail closed
/// if the path is substituted between those operations.
pub fn load_backup_encryption_key(
    path: &Path,
) -> Result<BackupEncryptionKey, BackupEncryptionError> {
    let before = fs::symlink_metadata(path).map_err(|source| key_io(path, source))?;
    if !before.file_type().is_file() {
        return Err(BackupEncryptionError::KeyNotRegularFile(path.to_path_buf()));
    }

    let file = File::open(path).map_err(|source| key_io(path, source))?;
    let opened = file.metadata().map_err(|source| key_io(path, source))?;
    if !opened.file_type().is_file() {
        return Err(BackupEncryptionError::KeyNotRegularFile(path.to_path_buf()));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        if before.dev() != opened.dev() || before.ino() != opened.ino() {
            return Err(BackupEncryptionError::KeyChangedDuringOpen(
                path.to_path_buf(),
            ));
        }
        let mode = opened.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(BackupEncryptionError::InsecureKeyPermissions {
                path: path.to_path_buf(),
                mode,
            });
        }
    }

    let mut bytes = Vec::with_capacity(BACKUP_ENCRYPTION_KEY_BYTES + 1);
    file.take((BACKUP_ENCRYPTION_KEY_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| key_io(path, source))?;
    BackupEncryptionKey::from_vec(bytes).map_err(|actual| BackupEncryptionError::InvalidKeyLength {
        path: path.to_path_buf(),
        actual,
    })
}

/// Create one encrypted offline backup without writing plaintext backup bytes to disk.
pub fn create_encrypted_offline_backup(
    db_path: &Path,
    destination: &Path,
    key: &BackupEncryptionKey,
) -> Result<BackupManifest, BackupEncryptionError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BackupEncryptionError::ClockBeforeEpoch)?;
    let created_unix_ms =
        u64::try_from(duration.as_millis()).map_err(|_| BackupEncryptionError::ClockOverflow)?;
    create_encrypted_offline_backup_at(db_path, destination, key, created_unix_ms)
}

/// Deterministic timestamp variant used by executable tests.
pub fn create_encrypted_offline_backup_at(
    db_path: &Path,
    destination: &Path,
    key: &BackupEncryptionKey,
    created_unix_ms: u64,
) -> Result<BackupManifest, BackupEncryptionError> {
    let backup = capture_offline_backup_at(db_path, destination, created_unix_ms)?;
    let encrypted = encrypt_backup(&backup, key)?;
    publish_encrypted_atomically(destination, &encrypted, &backup, key, created_unix_ms)
}

/// Independently authenticate, decrypt, and strictly validate an encrypted backup file.
pub fn verify_encrypted_backup_file(
    path: &Path,
    key: &BackupEncryptionKey,
) -> Result<NeuralBaseBackup, BackupEncryptionError> {
    let bytes = read_encrypted_backup_file(path)?;
    decrypt_backup(&bytes, key)
}

/// Encrypt one logical NBBK backup into an authenticated NBEC v1 artifact.
pub fn encrypt_backup(
    backup: &NeuralBaseBackup,
    key: &BackupEncryptionKey,
) -> Result<Vec<u8>, BackupEncryptionError> {
    let mut nonce = [0u8; NONCE_BYTES];
    let mut rng = OsRng;
    rng.try_fill_bytes(&mut nonce)
        .map_err(|_| BackupEncryptionError::RandomFailure)?;
    encrypt_backup_with_nonce(backup, key, nonce)
}

/// Authenticate, decrypt, and strictly decode one NBEC v1 artifact.
pub fn decrypt_backup(
    bytes: &[u8],
    key: &BackupEncryptionKey,
) -> Result<NeuralBaseBackup, BackupEncryptionError> {
    if bytes.len() > MAX_ENCRYPTED_BACKUP_BYTES {
        return Err(BackupEncryptionError::TooLarge);
    }
    if bytes.len() < HEADER_BYTES + TAG_BYTES {
        return Err(BackupEncryptionError::UnexpectedEof);
    }

    let header = &bytes[..HEADER_BYTES];
    if &header[..4] != MAGIC {
        return Err(BackupEncryptionError::InvalidMagic);
    }
    let version = header[4];
    if version != ENCRYPTED_BACKUP_FORMAT_VERSION {
        return Err(BackupEncryptionError::UnsupportedVersion(version));
    }
    let algorithm = header[5];
    if algorithm != ENCRYPTED_BACKUP_ALGORITHM_CHACHA20_POLY1305 {
        return Err(BackupEncryptionError::UnsupportedAlgorithm(algorithm));
    }
    let flags = u16::from_be_bytes([header[6], header[7]]);
    if flags != 0 {
        return Err(BackupEncryptionError::UnsupportedFlags(flags));
    }
    if header[28..32] != [0u8; 4] {
        return Err(BackupEncryptionError::ReservedFieldNonZero);
    }

    let nonce: [u8; NONCE_BYTES] = header[8..20]
        .try_into()
        .map_err(|_| BackupEncryptionError::UnexpectedEof)?;
    let plaintext_len_u64 = u64::from_be_bytes(
        header[20..28]
            .try_into()
            .map_err(|_| BackupEncryptionError::UnexpectedEof)?,
    );
    let plaintext_len =
        usize::try_from(plaintext_len_u64).map_err(|_| BackupEncryptionError::TooLarge)?;
    if plaintext_len > MAX_BACKUP_BYTES {
        return Err(BackupEncryptionError::TooLarge);
    }
    let declared = HEADER_BYTES
        .checked_add(plaintext_len)
        .and_then(|value| value.checked_add(TAG_BYTES))
        .ok_or(BackupEncryptionError::TooLarge)?;
    if declared != bytes.len() {
        return Err(BackupEncryptionError::LengthMismatch {
            declared,
            actual: bytes.len(),
        });
    }

    let mut payload = bytes[HEADER_BYTES..].to_vec();
    let packet_key = packet_key(key, nonce)?;
    let decrypted_len = packet_key
        .decrypt_in_place(0, header, &mut payload)
        .map_err(|_| BackupEncryptionError::AuthenticationFailed)?
        .len();
    if decrypted_len != plaintext_len {
        return Err(BackupEncryptionError::LengthMismatch {
            declared: plaintext_len,
            actual: decrypted_len,
        });
    }
    payload.truncate(decrypted_len);
    NeuralBaseBackup::decode_encrypted_payload(&payload).map_err(Into::into)
}

fn encrypt_backup_with_nonce(
    backup: &NeuralBaseBackup,
    key: &BackupEncryptionKey,
    nonce: [u8; NONCE_BYTES],
) -> Result<Vec<u8>, BackupEncryptionError> {
    let mut payload = backup.encode_encrypted_payload()?;
    if payload.len() > MAX_BACKUP_BYTES {
        return Err(BackupEncryptionError::TooLarge);
    }
    let plaintext_len =
        u64::try_from(payload.len()).map_err(|_| BackupEncryptionError::TooLarge)?;
    let header = build_header(nonce, plaintext_len);
    let packet_key = packet_key(key, nonce)?;
    let tag = packet_key
        .encrypt_in_place(0, &header, &mut payload)
        .map_err(|_| BackupEncryptionError::EncryptFailed)?;
    if tag.as_ref().len() != TAG_BYTES {
        return Err(BackupEncryptionError::CryptoUnavailable);
    }

    let total = HEADER_BYTES
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(TAG_BYTES))
        .ok_or(BackupEncryptionError::TooLarge)?;
    if total > MAX_ENCRYPTED_BACKUP_BYTES {
        return Err(BackupEncryptionError::TooLarge);
    }
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&header);
    out.extend_from_slice(&payload);
    out.extend_from_slice(tag.as_ref());
    Ok(out)
}

fn build_header(nonce: [u8; NONCE_BYTES], plaintext_len: u64) -> [u8; HEADER_BYTES] {
    let mut header = [0u8; HEADER_BYTES];
    header[..4].copy_from_slice(MAGIC);
    header[4] = ENCRYPTED_BACKUP_FORMAT_VERSION;
    header[5] = ENCRYPTED_BACKUP_ALGORITHM_CHACHA20_POLY1305;
    header[6..8].copy_from_slice(&0u16.to_be_bytes());
    header[8..20].copy_from_slice(&nonce);
    header[20..28].copy_from_slice(&plaintext_len.to_be_bytes());
    header
}

fn packet_key(
    key: &BackupEncryptionKey,
    nonce: [u8; NONCE_BYTES],
) -> Result<Box<dyn rustls::quic::PacketKey>, BackupEncryptionError> {
    let tls13 = TLS13_CHACHA20_POLY1305_SHA256
        .tls13()
        .ok_or(BackupEncryptionError::CryptoUnavailable)?;
    let algorithm = tls13.quic.ok_or(BackupEncryptionError::CryptoUnavailable)?;
    if algorithm.aead_key_len() != BACKUP_ENCRYPTION_KEY_BYTES {
        return Err(BackupEncryptionError::CryptoUnavailable);
    }

    // rustls's QUIC packet primitive is the provider-backed ChaCha20-Poly1305
    // AEAD. QUIC derives its nonce by XORing the packet number into the IV; a
    // packet number of zero therefore uses the supplied random 96-bit IV
    // unchanged, giving NBEC the standard AEAD(key, nonce, AAD, plaintext)
    // construction while keeping cryptographic implementation inside rustls.
    let packet = algorithm.packet_key(key.aead_key()?, Iv::from(nonce));
    if packet.tag_len() != TAG_BYTES {
        return Err(BackupEncryptionError::CryptoUnavailable);
    }
    Ok(packet)
}

fn key_io(path: &Path, source: io::Error) -> BackupEncryptionError {
    BackupEncryptionError::KeyIo {
        path: path.to_path_buf(),
        source,
    }
}

fn file_io(path: &Path, source: io::Error) -> BackupEncryptionError {
    BackupEncryptionError::FileIo {
        path: path.to_path_buf(),
        source,
    }
}

fn read_encrypted_backup_file(path: &Path) -> Result<Vec<u8>, BackupEncryptionError> {
    let before = fs::symlink_metadata(path).map_err(|source| file_io(path, source))?;
    if !before.file_type().is_file() {
        return Err(BackupEncryptionError::InputNotRegularFile(
            path.to_path_buf(),
        ));
    }
    if before.len() > MAX_ENCRYPTED_BACKUP_BYTES as u64 {
        return Err(BackupEncryptionError::TooLarge);
    }

    let file = File::open(path).map_err(|source| file_io(path, source))?;
    let opened = file.metadata().map_err(|source| file_io(path, source))?;
    if !opened.file_type().is_file() {
        return Err(BackupEncryptionError::InputNotRegularFile(
            path.to_path_buf(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.dev() != opened.dev() || before.ino() != opened.ino() {
            return Err(BackupEncryptionError::InputChangedDuringOpen(
                path.to_path_buf(),
            ));
        }
    }
    if opened.len() > MAX_ENCRYPTED_BACKUP_BYTES as u64 {
        return Err(BackupEncryptionError::TooLarge);
    }

    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_ENCRYPTED_BACKUP_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| file_io(path, source))?;
    if bytes.len() > MAX_ENCRYPTED_BACKUP_BYTES {
        return Err(BackupEncryptionError::TooLarge);
    }
    Ok(bytes)
}

fn publish_encrypted_atomically(
    destination: &Path,
    bytes: &[u8],
    backup: &NeuralBaseBackup,
    key: &BackupEncryptionKey,
    created_unix_ms: u64,
) -> Result<BackupManifest, BackupEncryptionError> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(BackupEncryptionError::DestinationParentMissing);
    }
    let file_name = destination
        .file_name()
        .ok_or(BackupEncryptionError::DestinationFileNameMissing)?
        .to_string_lossy();
    if destination.exists() {
        return Err(BackupEncryptionError::DestinationExists(
            destination.to_path_buf(),
        ));
    }
    let staged = parent.join(format!(
        ".{file_name}.partial-{}-{created_unix_ms}",
        std::process::id()
    ));

    let result = (|| -> Result<BackupManifest, BackupEncryptionError> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&staged)
            .map_err(|source| file_io(&staged, source))?;
        file.write_all(bytes)
            .map_err(|source| file_io(&staged, source))?;
        file.sync_all().map_err(|source| file_io(&staged, source))?;
        drop(file);

        let verified = verify_encrypted_backup_file(&staged, key)?;
        let mut expected_manifest = backup.manifest.clone();
        expected_manifest.encrypted = true;
        if verified.manifest != expected_manifest
            || verified.membership != backup.membership
            || verified.sql_snapshot != backup.sql_snapshot
        {
            return Err(BackupEncryptionError::StagedVerificationMismatch);
        }

        match fs::hard_link(&staged, destination) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(BackupEncryptionError::DestinationExists(
                    destination.to_path_buf(),
                ));
            }
            Err(error) => return Err(file_io(destination, error)),
        }
        sync_parent_dir(parent)?;
        fs::remove_file(&staged).map_err(|source| file_io(&staged, source))?;
        sync_parent_dir(parent)?;
        Ok(verified.manifest)
    })();

    if result.is_err() {
        let _ = fs::remove_file(&staged);
    }
    result
}

#[cfg(unix)]
fn sync_parent_dir(parent: &Path) -> Result<(), BackupEncryptionError> {
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|source| file_io(parent, source))
}

#[cfg(not(unix))]
fn sync_parent_dir(_parent: &Path) -> Result<(), BackupEncryptionError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::ClusterMembership;
    use crate::replicated_identity_snapshot::ReplicatedIdentitySnapshotExtension;
    use crate::replicated_snapshot::{ReplicatedSqlSnapshot, SnapshotMetadata};
    use tempfile::TempDir;

    fn backup() -> NeuralBaseBackup {
        let membership = ClusterMembership::bootstrap("n1".to_string(), Vec::<String>::new());
        let snapshot = ReplicatedSqlSnapshot {
            metadata: SnapshotMetadata {
                last_included_index: 7,
                last_included_term: 3,
                latest_sql_apply_index: 6,
                latest_commit_ts: 42,
            },
            tables: vec![],
            metadata_extension: ReplicatedIdentitySnapshotExtension::Uninitialized
                .encode()
                .unwrap(),
        }
        .encode()
        .unwrap();
        NeuralBaseBackup::new_offline(1_725_000_000_123, membership, snapshot).unwrap()
    }

    fn key(byte: u8) -> BackupEncryptionKey {
        BackupEncryptionKey::from_bytes([byte; BACKUP_ENCRYPTION_KEY_BYTES])
    }

    #[test]
    fn authenticated_container_roundtrips_and_cannot_be_republished_plain() {
        let original = backup();
        let encrypted = encrypt_backup_with_nonce(&original, &key(7), [9u8; NONCE_BYTES]).unwrap();
        assert_eq!(&encrypted[..4], MAGIC);

        let decrypted = decrypt_backup(&encrypted, &key(7)).unwrap();
        assert!(decrypted.manifest.encrypted);
        assert_eq!(decrypted.membership, original.membership);
        assert_eq!(decrypted.sql_snapshot, original.sql_snapshot);
        assert!(matches!(
            decrypted.encode(),
            Err(BackupCodecError::EncryptionUnsupported)
        ));
    }

    #[test]
    fn wrong_key_fails_authentication() {
        let encrypted = encrypt_backup_with_nonce(&backup(), &key(7), [9u8; NONCE_BYTES]).unwrap();
        assert!(matches!(
            decrypt_backup(&encrypted, &key(8)),
            Err(BackupEncryptionError::AuthenticationFailed)
        ));
    }

    #[test]
    fn ciphertext_tampering_fails_authentication() {
        let mut encrypted =
            encrypt_backup_with_nonce(&backup(), &key(7), [9u8; NONCE_BYTES]).unwrap();
        encrypted[HEADER_BYTES + 8] ^= 0x40;
        assert!(matches!(
            decrypt_backup(&encrypted, &key(7)),
            Err(BackupEncryptionError::AuthenticationFailed)
        ));
    }

    #[test]
    fn authenticated_header_tampering_fails_authentication() {
        let mut encrypted =
            encrypt_backup_with_nonce(&backup(), &key(7), [9u8; NONCE_BYTES]).unwrap();
        encrypted[8] ^= 0x01;
        assert!(matches!(
            decrypt_backup(&encrypted, &key(7)),
            Err(BackupEncryptionError::AuthenticationFailed)
        ));
    }

    #[test]
    fn unsupported_outer_fields_fail_closed() {
        let encrypted = encrypt_backup_with_nonce(&backup(), &key(7), [9u8; NONCE_BYTES]).unwrap();

        let mut future = encrypted.clone();
        future[4] = ENCRYPTED_BACKUP_FORMAT_VERSION + 1;
        assert!(matches!(
            decrypt_backup(&future, &key(7)),
            Err(BackupEncryptionError::UnsupportedVersion(_))
        ));

        let mut algorithm = encrypted.clone();
        algorithm[5] = ENCRYPTED_BACKUP_ALGORITHM_CHACHA20_POLY1305 + 1;
        assert!(matches!(
            decrypt_backup(&algorithm, &key(7)),
            Err(BackupEncryptionError::UnsupportedAlgorithm(_))
        ));

        let mut flags = encrypted;
        flags[7] = 1;
        assert!(matches!(
            decrypt_backup(&flags, &key(7)),
            Err(BackupEncryptionError::UnsupportedFlags(1))
        ));
    }

    #[test]
    fn plaintext_nbbk_is_not_an_encrypted_container() {
        let plain = backup().encode().unwrap();
        assert!(matches!(
            decrypt_backup(&plain, &key(7)),
            Err(BackupEncryptionError::InvalidMagic)
        ));
    }

    #[test]
    fn truncated_and_length_mismatched_containers_fail_closed() {
        let encrypted = encrypt_backup_with_nonce(&backup(), &key(7), [9u8; NONCE_BYTES]).unwrap();
        assert!(matches!(
            decrypt_backup(&encrypted[..20], &key(7)),
            Err(BackupEncryptionError::UnexpectedEof)
        ));

        let mut mismatched = encrypted;
        mismatched[27] ^= 1;
        assert!(matches!(
            decrypt_backup(&mismatched, &key(7)),
            Err(BackupEncryptionError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn key_debug_output_is_redacted() {
        assert_eq!(
            format!("{:?}", key(0x41)),
            "BackupEncryptionKey([REDACTED])"
        );
    }

    #[test]
    fn key_file_requires_exact_raw_length() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("backup.key");
        fs::write(&path, [7u8; BACKUP_ENCRYPTION_KEY_BYTES - 1]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert!(matches!(
            load_backup_encryption_key(&path),
            Err(BackupEncryptionError::InvalidKeyLength { actual: 31, .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn unix_key_file_permissions_must_exclude_group_and_other() {
        use std::os::unix::fs::PermissionsExt;

        let root = TempDir::new().unwrap();
        let path = root.path().join("backup.key");
        fs::write(&path, [7u8; BACKUP_ENCRYPTION_KEY_BYTES]).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        load_backup_encryption_key(&path).unwrap();

        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(matches!(
            load_backup_encryption_key(&path),
            Err(BackupEncryptionError::InsecureKeyPermissions { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn key_loader_rejects_symlinks() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let root = TempDir::new().unwrap();
        let target = root.path().join("target.key");
        let link = root.path().join("link.key");
        fs::write(&target, [7u8; BACKUP_ENCRYPTION_KEY_BYTES]).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&target, &link).unwrap();
        assert!(matches!(
            load_backup_encryption_key(&link),
            Err(BackupEncryptionError::KeyNotRegularFile(_))
        ));
    }
}
