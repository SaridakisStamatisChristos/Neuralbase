from pathlib import Path


def replace_exact(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    if new in text:
        return
    if old not in text:
        raise SystemExit(f"expected patch anchor not found in {path}")
    p.write_text(text.replace(old, new, 1))


replace_exact(
    "src/backup_encryption.rs",
    """    let backup = capture_offline_backup_at(db_path, destination, created_unix_ms)?;\n    let encrypted = encrypt_backup(&backup, key)?;\n    publish_encrypted_atomically(destination, &encrypted, &backup, key, created_unix_ms)\n}\n\n/// Independently authenticate, decrypt, and strictly validate an encrypted backup file.""",
    """    let backup = capture_offline_backup_at(db_path, destination, created_unix_ms)?;\n    publish_encrypted_backup(&backup, destination, key, created_unix_ms)\n}\n\n/// Encrypt, independently verify, and atomically publish one already-captured logical backup.\n///\n/// This crate-private publication primitive is shared by offline and online capture paths so\n/// both formats use exactly the same authenticated-container and durable-publication semantics.\npub(crate) fn publish_encrypted_backup(\n    backup: &NeuralBaseBackup,\n    destination: &Path,\n    key: &BackupEncryptionKey,\n    created_unix_ms: u64,\n) -> Result<BackupManifest, BackupEncryptionError> {\n    let encrypted = encrypt_backup(backup, key)?;\n    publish_encrypted_atomically(destination, &encrypted, backup, key, created_unix_ms)\n}\n\n/// Independently authenticate, decrypt, and strictly validate an encrypted backup file.""",
)

replace_exact(
    "src/online_backup.rs",
    """use crate::backup::{BackupCodecError, BackupKind, BackupManifest, NeuralBaseBackup};\nuse crate::catalog::InMemoryCatalog;""",
    """use crate::backup::{BackupCodecError, BackupKind, BackupManifest, NeuralBaseBackup};\nuse crate::backup_encryption::{\n    publish_encrypted_backup, BackupEncryptionError, BackupEncryptionKey,\n};\nuse crate::catalog::InMemoryCatalog;""",
)

replace_exact(
    "src/online_backup.rs",
    """    #[error(\"independent online backup verification failed: {0}\")]\n    Verification(OfflineBackupError),\n    #[error(\"system clock is before the Unix epoch\")]""",
    """    #[error(\"independent online backup verification failed: {0}\")]\n    Verification(OfflineBackupError),\n    #[error(\"encrypted online backup publication failed: {0}\")]\n    Encryption(#[from] BackupEncryptionError),\n    #[error(\"system clock is before the Unix epoch\")]""",
)

replace_exact(
    "src/online_backup.rs",
    """    pub async fn create_online_backup_at(\n        &self,\n        destination: &Path,\n        created_unix_ms: u64,\n    ) -> Result<BackupManifest, OnlineBackupError> {\n        validate_destination(destination)?;\n\n        for _ in 0..MAX_CAPTURE_ATTEMPTS {""",
    """    pub async fn create_online_backup_at(\n        &self,\n        destination: &Path,\n        created_unix_ms: u64,\n    ) -> Result<BackupManifest, OnlineBackupError> {\n        validate_destination(destination)?;\n        let backup = self.capture_online_backup_at(created_unix_ms).await?;\n        let encoded = backup.encode()?;\n        publish_atomically(destination, &encoded, created_unix_ms)?;\n        Ok(backup.manifest)\n    }\n\n    /// Create a leader-coordinated online backup directly as an authenticated NBEC artifact.\n    ///\n    /// Capture semantics are identical to plaintext online backup: one confirmed Raft barrier,\n    /// one stable state-machine boundary, and fail-closed retry on concurrent durable movement.\n    /// Plaintext NBBK bytes are never published when this method is selected.\n    pub async fn create_encrypted_online_backup(\n        &self,\n        destination: &Path,\n        key: &BackupEncryptionKey,\n    ) -> Result<BackupManifest, OnlineBackupError> {\n        let duration = SystemTime::now()\n            .duration_since(UNIX_EPOCH)\n            .map_err(|_| OnlineBackupError::ClockBeforeEpoch)?;\n        let created_unix_ms =\n            u64::try_from(duration.as_millis()).map_err(|_| OnlineBackupError::ClockOverflow)?;\n        self.create_encrypted_online_backup_at(destination, key, created_unix_ms)\n            .await\n    }\n\n    /// Deterministic timestamp variant used by executable tests.\n    pub async fn create_encrypted_online_backup_at(\n        &self,\n        destination: &Path,\n        key: &BackupEncryptionKey,\n        created_unix_ms: u64,\n    ) -> Result<BackupManifest, OnlineBackupError> {\n        validate_destination(destination)?;\n        let backup = self.capture_online_backup_at(created_unix_ms).await?;\n        publish_encrypted_backup(&backup, destination, key, created_unix_ms).map_err(Into::into)\n    }\n\n    async fn capture_online_backup_at(\n        &self,\n        created_unix_ms: u64,\n    ) -> Result<NeuralBaseBackup, OnlineBackupError> {\n        for _ in 0..MAX_CAPTURE_ATTEMPTS {""",
)

replace_exact(
    "src/online_backup.rs",
    """            let encoded = backup.encode()?;\n            publish_atomically(destination, &encoded, created_unix_ms)?;\n            return Ok(backup.manifest);""",
    """            return Ok(backup);""",
)

replace_exact(
    "src/online_backup.rs",
    """    use super::*;\n    use crate::consensus::{ChannelTransport, CommittedEntry, RaftNode, RaftPersistenceStore};""",
    """    use super::*;\n    use crate::backup_encryption::verify_encrypted_backup_file;\n    use crate::consensus::{ChannelTransport, CommittedEntry, RaftNode, RaftPersistenceStore};""",
)

replace_exact(
    "src/online_backup.rs",
    """        harness.handle.shutdown().await;\n        harness.apply_task.abort();\n    }\n\n    #[tokio::test]\n    async fn existing_destination_is_never_overwritten() {""",
    """        harness.handle.shutdown().await;\n        harness.apply_task.abort();\n    }\n\n    #[tokio::test]\n    async fn encrypted_online_backup_reuses_exact_boundary_and_authenticated_publication() {\n        let harness = single_node_harness().await;\n        let output = TempDir::new().unwrap();\n        let destination = output.path().join(\"online.nbec\");\n        let key = BackupEncryptionKey::from_bytes([0x41; 32]);\n\n        let manifest = harness\n            .coordinator\n            .create_encrypted_online_backup_at(&destination, &key, 2234)\n            .await\n            .unwrap();\n        assert_eq!(manifest.kind, BackupKind::Online);\n        assert!(manifest.encrypted);\n        assert!(manifest.metadata.last_included_index > 0);\n        assert_eq!(\n            manifest.metadata.latest_sql_apply_index,\n            manifest.metadata.last_included_index\n        );\n\n        let verified = verify_encrypted_backup_file(&destination, &key).unwrap();\n        assert_eq!(verified.manifest, manifest);\n        assert_eq!(verified.manifest.kind, BackupKind::Online);\n        assert!(verified.manifest.encrypted);\n\n        let wrong_key = BackupEncryptionKey::from_bytes([0x42; 32]);\n        assert!(matches!(\n            verify_encrypted_backup_file(&destination, &wrong_key),\n            Err(BackupEncryptionError::AuthenticationFailed)\n        ));\n        assert!(output.path().read_dir().unwrap().all(|entry| !entry\n            .unwrap()\n            .file_name()\n            .to_string_lossy()\n            .contains(\"partial\")));\n\n        #[cfg(unix)]\n        {\n            use std::os::unix::fs::PermissionsExt;\n            assert_eq!(fs::metadata(&destination).unwrap().permissions().mode() & 0o077, 0);\n        }\n\n        harness.handle.shutdown().await;\n        harness.apply_task.abort();\n    }\n\n    #[tokio::test]\n    async fn existing_destination_is_never_overwritten() {""",
)
