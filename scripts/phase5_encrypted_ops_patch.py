from pathlib import Path


def replace_exact(path: str, old: str, new: str) -> None:
    p = Path(path)
    s = p.read_text()
    if new in s:
        return
    if old not in s:
        raise SystemExit(f"expected patch anchor not found in {path}")
    p.write_text(s.replace(old, new, 1))


replace_exact(
    "src/offline_backup.rs",
    """pub fn create_offline_backup_at(\n    db_path: &Path,\n    destination: &Path,\n    created_unix_ms: u64,\n) -> Result<BackupManifest, OfflineBackupError> {\n    validate_paths(db_path, destination)?;""",
    """pub fn create_offline_backup_at(\n    db_path: &Path,\n    destination: &Path,\n    created_unix_ms: u64,\n) -> Result<BackupManifest, OfflineBackupError> {\n    let backup = capture_offline_backup_at(db_path, destination, created_unix_ms)?;\n    let encoded = backup.encode()?;\n    publish_atomically(destination, &encoded, created_unix_ms)?;\n    Ok(backup.manifest)\n}\n\n/// Capture one internally consistent offline logical backup without publishing it.\n///\n/// This crate-private boundary lets authenticated-encryption publication consume\n/// the same proven capture path without ever materializing a plaintext backup file.\npub(crate) fn capture_offline_backup_at(\n    db_path: &Path,\n    destination: &Path,\n    created_unix_ms: u64,\n) -> Result<NeuralBaseBackup, OfflineBackupError> {\n    validate_paths(db_path, destination)?;""",
)
replace_exact(
    "src/offline_backup.rs",
    """    let backup = NeuralBaseBackup::new_offline(created_unix_ms, membership, sql_snapshot)?;\n    let encoded = backup.encode()?;\n\n    publish_atomically(destination, &encoded, created_unix_ms)?;\n    Ok(backup.manifest)\n}""",
    """    NeuralBaseBackup::new_offline(created_unix_ms, membership, sql_snapshot)\n        .map_err(OfflineBackupError::from)\n}""",
)

replace_exact(
    "src/backup_encryption.rs",
    """use std::fs::{self, File};\nuse std::io::{self, Read};\nuse std::path::{Path, PathBuf};""",
    """use std::fs::{self, File, OpenOptions};\nuse std::io::{self, Read, Write};\nuse std::path::{Path, PathBuf};\nuse std::time::{SystemTime, UNIX_EPOCH};""",
)
replace_exact(
    "src/backup_encryption.rs",
    """use crate::backup::{BackupCodecError, NeuralBaseBackup, MAX_BACKUP_BYTES};""",
    """use crate::backup::{BackupCodecError, BackupManifest, NeuralBaseBackup, MAX_BACKUP_BYTES};\nuse crate::offline_backup::{capture_offline_backup_at, OfflineBackupError};""",
)
replace_exact(
    "src/backup_encryption.rs",
    """    #[error(\"secure random nonce generation failed\")]\n    RandomFailure,""",
    """    #[error(\"encrypted backup input is not a regular file: {0}\")]\n    InputNotRegularFile(PathBuf),\n    #[error(\"encrypted backup input changed while being opened: {0}\")]\n    InputChangedDuringOpen(PathBuf),\n    #[error(\"encrypted backup file I/O failure for {path}: {source}\")]\n    FileIo {\n        path: PathBuf,\n        #[source]\n        source: io::Error,\n    },\n    #[error(\"encrypted backup destination already exists: {0}\")]\n    DestinationExists(PathBuf),\n    #[error(\"encrypted backup destination must have an existing parent directory\")]\n    DestinationParentMissing,\n    #[error(\"encrypted backup destination must name a file\")]\n    DestinationFileNameMissing,\n    #[error(\"captured offline backup failed: {0}\")]\n    Offline(#[from] OfflineBackupError),\n    #[error(\"system clock is before the Unix epoch\")]\n    ClockBeforeEpoch,\n    #[error(\"backup timestamp does not fit milliseconds since Unix epoch\")]\n    ClockOverflow,\n    #[error(\"staged encrypted backup did not decode to the captured logical backup\")]\n    StagedVerificationMismatch,\n    #[error(\"secure random nonce generation failed\")]\n    RandomFailure,""",
)
replace_exact(
    "src/backup_encryption.rs",
    """/// Encrypt one logical NBBK backup into an authenticated NBEC v1 artifact.\npub fn encrypt_backup(""",
    """/// Create one encrypted offline backup without writing plaintext backup bytes to disk.\npub fn create_encrypted_offline_backup(\n    db_path: &Path,\n    destination: &Path,\n    key: &BackupEncryptionKey,\n) -> Result<BackupManifest, BackupEncryptionError> {\n    let duration = SystemTime::now()\n        .duration_since(UNIX_EPOCH)\n        .map_err(|_| BackupEncryptionError::ClockBeforeEpoch)?;\n    let created_unix_ms =\n        u64::try_from(duration.as_millis()).map_err(|_| BackupEncryptionError::ClockOverflow)?;\n    create_encrypted_offline_backup_at(db_path, destination, key, created_unix_ms)\n}\n\n/// Deterministic timestamp variant used by executable tests.\npub fn create_encrypted_offline_backup_at(\n    db_path: &Path,\n    destination: &Path,\n    key: &BackupEncryptionKey,\n    created_unix_ms: u64,\n) -> Result<BackupManifest, BackupEncryptionError> {\n    let backup = capture_offline_backup_at(db_path, destination, created_unix_ms)?;\n    let encrypted = encrypt_backup(&backup, key)?;\n    publish_encrypted_atomically(destination, &encrypted, &backup, key, created_unix_ms)\n}\n\n/// Independently authenticate, decrypt, and strictly validate an encrypted backup file.\npub fn verify_encrypted_backup_file(\n    path: &Path,\n    key: &BackupEncryptionKey,\n) -> Result<NeuralBaseBackup, BackupEncryptionError> {\n    let bytes = read_encrypted_backup_file(path)?;\n    decrypt_backup(&bytes, key)\n}\n\n/// Encrypt one logical NBBK backup into an authenticated NBEC v1 artifact.\npub fn encrypt_backup(""",
)
replace_exact(
    "src/backup_encryption.rs",
    """fn key_io(path: &Path, source: io::Error) -> BackupEncryptionError {\n    BackupEncryptionError::KeyIo {\n        path: path.to_path_buf(),\n        source,\n    }\n}\n\n#[cfg(test)]""",
    """fn key_io(path: &Path, source: io::Error) -> BackupEncryptionError {\n    BackupEncryptionError::KeyIo {\n        path: path.to_path_buf(),\n        source,\n    }\n}\n\nfn file_io(path: &Path, source: io::Error) -> BackupEncryptionError {\n    BackupEncryptionError::FileIo {\n        path: path.to_path_buf(),\n        source,\n    }\n}\n\nfn read_encrypted_backup_file(path: &Path) -> Result<Vec<u8>, BackupEncryptionError> {\n    let before = fs::symlink_metadata(path).map_err(|source| file_io(path, source))?;\n    if !before.file_type().is_file() {\n        return Err(BackupEncryptionError::InputNotRegularFile(path.to_path_buf()));\n    }\n    if before.len() > MAX_ENCRYPTED_BACKUP_BYTES as u64 {\n        return Err(BackupEncryptionError::TooLarge);\n    }\n\n    let file = File::open(path).map_err(|source| file_io(path, source))?;\n    let opened = file.metadata().map_err(|source| file_io(path, source))?;\n    if !opened.file_type().is_file() {\n        return Err(BackupEncryptionError::InputNotRegularFile(path.to_path_buf()));\n    }\n    #[cfg(unix)]\n    {\n        use std::os::unix::fs::MetadataExt;\n        if before.dev() != opened.dev() || before.ino() != opened.ino() {\n            return Err(BackupEncryptionError::InputChangedDuringOpen(path.to_path_buf()));\n        }\n    }\n    if opened.len() > MAX_ENCRYPTED_BACKUP_BYTES as u64 {\n        return Err(BackupEncryptionError::TooLarge);\n    }\n\n    let mut bytes = Vec::with_capacity(opened.len() as usize);\n    file.take(MAX_ENCRYPTED_BACKUP_BYTES as u64 + 1)\n        .read_to_end(&mut bytes)\n        .map_err(|source| file_io(path, source))?;\n    if bytes.len() > MAX_ENCRYPTED_BACKUP_BYTES {\n        return Err(BackupEncryptionError::TooLarge);\n    }\n    Ok(bytes)\n}\n\nfn publish_encrypted_atomically(\n    destination: &Path,\n    bytes: &[u8],\n    backup: &NeuralBaseBackup,\n    key: &BackupEncryptionKey,\n    created_unix_ms: u64,\n) -> Result<BackupManifest, BackupEncryptionError> {\n    let parent = destination\n        .parent()\n        .filter(|path| !path.as_os_str().is_empty())\n        .unwrap_or_else(|| Path::new(\".\"));\n    if !parent.is_dir() {\n        return Err(BackupEncryptionError::DestinationParentMissing);\n    }\n    let file_name = destination\n        .file_name()\n        .ok_or(BackupEncryptionError::DestinationFileNameMissing)?\n        .to_string_lossy();\n    if destination.exists() {\n        return Err(BackupEncryptionError::DestinationExists(destination.to_path_buf()));\n    }\n    let staged = parent.join(format!(\n        \".{file_name}.partial-{}-{created_unix_ms}\",\n        std::process::id()\n    ));\n\n    let result = (|| -> Result<BackupManifest, BackupEncryptionError> {\n        let mut options = OpenOptions::new();\n        options.write(true).create_new(true);\n        #[cfg(unix)]\n        {\n            use std::os::unix::fs::OpenOptionsExt;\n            options.mode(0o600);\n        }\n        let mut file = options\n            .open(&staged)\n            .map_err(|source| file_io(&staged, source))?;\n        file.write_all(bytes)\n            .map_err(|source| file_io(&staged, source))?;\n        file.sync_all()\n            .map_err(|source| file_io(&staged, source))?;\n        drop(file);\n\n        let verified = verify_encrypted_backup_file(&staged, key)?;\n        let mut expected_manifest = backup.manifest.clone();\n        expected_manifest.encrypted = true;\n        if verified.manifest != expected_manifest\n            || verified.membership != backup.membership\n            || verified.sql_snapshot != backup.sql_snapshot\n        {\n            return Err(BackupEncryptionError::StagedVerificationMismatch);\n        }\n\n        match fs::hard_link(&staged, destination) {\n            Ok(()) => {}\n            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {\n                return Err(BackupEncryptionError::DestinationExists(destination.to_path_buf()));\n            }\n            Err(error) => return Err(file_io(destination, error)),\n        }\n        sync_parent_dir(parent)?;\n        fs::remove_file(&staged).map_err(|source| file_io(&staged, source))?;\n        sync_parent_dir(parent)?;\n        Ok(verified.manifest)\n    })();\n\n    if result.is_err() {\n        let _ = fs::remove_file(&staged);\n    }\n    result\n}\n\n#[cfg(unix)]\nfn sync_parent_dir(parent: &Path) -> Result<(), BackupEncryptionError> {\n    File::open(parent)\n        .and_then(|file| file.sync_all())\n        .map_err(|source| file_io(parent, source))\n}\n\n#[cfg(not(unix))]\nfn sync_parent_dir(_parent: &Path) -> Result<(), BackupEncryptionError> {\n    Ok(())\n}\n\n#[cfg(test)]""",
)

replace_exact(
    "src/restore.rs",
    """use crate::backup::{BackupManifest, NeuralBaseBackup};""",
    """use crate::backup::{BackupManifest, NeuralBaseBackup};\nuse crate::backup_encryption::{\n    verify_encrypted_backup_file, BackupEncryptionError, BackupEncryptionKey,\n};""",
)
replace_exact(
    "src/restore.rs",
    """pub enum RestoreError {\n    #[error(\"backup validation failed before restore: {0}\")]\n    Backup(#[from] OfflineBackupError),""",
    """pub enum RestoreError {\n    #[error(\"backup validation failed before restore: {0}\")]\n    Backup(#[from] OfflineBackupError),\n    #[error(\"encrypted backup validation failed before restore: {0}\")]\n    EncryptedBackup(#[from] BackupEncryptionError),""",
)
replace_exact(
    "src/restore.rs",
    """pub fn restore_new_cluster(\n    backup_path: &Path,\n    target: &Path,\n    recovery_node_id: &str,\n) -> Result<RestoreReport, RestoreError> {\n    validate_target(target)?;\n    let backup = verify_backup_file(backup_path)?;\n    let recovery_membership = build_recovery_membership(&backup, recovery_node_id)?;""",
    """pub fn restore_new_cluster(\n    backup_path: &Path,\n    target: &Path,\n    recovery_node_id: &str,\n) -> Result<RestoreReport, RestoreError> {\n    validate_target(target)?;\n    let backup = verify_backup_file(backup_path)?;\n    restore_verified_new_cluster(backup, target, recovery_node_id)\n}\n\n/// Restore an authenticated encrypted backup into a brand-new recovery cluster.\n///\n/// Decryption and complete backup validation finish before any restore target or\n/// staging directory is created, so a wrong key or tampered artifact cannot\n/// publish partial database state.\npub fn restore_encrypted_new_cluster(\n    backup_path: &Path,\n    key: &BackupEncryptionKey,\n    target: &Path,\n    recovery_node_id: &str,\n) -> Result<RestoreReport, RestoreError> {\n    validate_target(target)?;\n    let backup = verify_encrypted_backup_file(backup_path, key)?;\n    restore_verified_new_cluster(backup, target, recovery_node_id)\n}\n\nfn restore_verified_new_cluster(\n    backup: NeuralBaseBackup,\n    target: &Path,\n    recovery_node_id: &str,\n) -> Result<RestoreReport, RestoreError> {\n    let recovery_membership = build_recovery_membership(&backup, recovery_node_id)?;""",
)

replace_exact(
    "src/bin/neuralbase-backup.rs",
    """use std::path::PathBuf;\nuse std::process::ExitCode;\n\nuse neuralbase::offline_backup::{create_offline_backup, verify_backup_file};\nuse neuralbase::restore::restore_new_cluster;""",
    """use std::path::{Path, PathBuf};\nuse std::process::ExitCode;\n\nuse neuralbase::backup_encryption::{\n    create_encrypted_offline_backup, load_backup_encryption_key, verify_encrypted_backup_file,\n};\nuse neuralbase::offline_backup::{create_offline_backup, verify_backup_file};\nuse neuralbase::restore::{restore_encrypted_new_cluster, restore_new_cluster};""",
)
replace_exact(
    "src/bin/neuralbase-backup.rs",
    """            let db = required_flag(&args[1..], \"--db\")?;\n            let output = required_flag(&args[1..], \"--output\")?;\n            reject_unknown_flags(&args[1..], &[\"--db\", \"--output\"])?;\n            let manifest = create_offline_backup(&PathBuf::from(db), &PathBuf::from(output))\n                .map_err(|error| error.to_string())?;""",
    """            let db = required_flag(&args[1..], \"--db\")?;\n            let output = required_flag(&args[1..], \"--output\")?;\n            let key_file = optional_flag(&args[1..], \"--key-file\")?;\n            reject_unknown_flags(&args[1..], &[\"--db\", \"--output\", \"--key-file\"])?;\n            let manifest = if let Some(key_file) = key_file {\n                let key = load_backup_encryption_key(Path::new(key_file))\n                    .map_err(|error| error.to_string())?;\n                create_encrypted_offline_backup(\n                    &PathBuf::from(db),\n                    &PathBuf::from(output),\n                    &key,\n                )\n                .map_err(|error| error.to_string())?\n            } else {\n                create_offline_backup(&PathBuf::from(db), &PathBuf::from(output))\n                    .map_err(|error| error.to_string())?\n            };""",
)
replace_exact(
    "src/bin/neuralbase-backup.rs",
    """            let backup = required_flag(&args[1..], \"--backup\")?;\n            reject_unknown_flags(&args[1..], &[\"--backup\"])?;\n            let verified =\n                verify_backup_file(&PathBuf::from(backup)).map_err(|error| error.to_string())?;""",
    """            let backup = required_flag(&args[1..], \"--backup\")?;\n            let key_file = optional_flag(&args[1..], \"--key-file\")?;\n            reject_unknown_flags(&args[1..], &[\"--backup\", \"--key-file\"])?;\n            let verified = if let Some(key_file) = key_file {\n                let key = load_backup_encryption_key(Path::new(key_file))\n                    .map_err(|error| error.to_string())?;\n                verify_encrypted_backup_file(&PathBuf::from(backup), &key)\n                    .map_err(|error| error.to_string())?\n            } else {\n                verify_backup_file(&PathBuf::from(backup)).map_err(|error| error.to_string())?\n            };""",
)
replace_exact(
    "src/bin/neuralbase-backup.rs",
    """            let backup = required_flag(&args[1..], \"--backup\")?;\n            let target = required_flag(&args[1..], \"--target\")?;\n            let node_id = required_flag(&args[1..], \"--node-id\")?;\n            reject_unknown_flags(&args[1..], &[\"--backup\", \"--target\", \"--node-id\"])?;\n            let report =\n                restore_new_cluster(&PathBuf::from(backup), &PathBuf::from(target), node_id)\n                    .map_err(|error| error.to_string())?;""",
    """            let backup = required_flag(&args[1..], \"--backup\")?;\n            let target = required_flag(&args[1..], \"--target\")?;\n            let node_id = required_flag(&args[1..], \"--node-id\")?;\n            let key_file = optional_flag(&args[1..], \"--key-file\")?;\n            reject_unknown_flags(\n                &args[1..],\n                &[\"--backup\", \"--target\", \"--node-id\", \"--key-file\"],\n            )?;\n            let report = if let Some(key_file) = key_file {\n                let key = load_backup_encryption_key(Path::new(key_file))\n                    .map_err(|error| error.to_string())?;\n                restore_encrypted_new_cluster(\n                    &PathBuf::from(backup),\n                    &key,\n                    &PathBuf::from(target),\n                    node_id,\n                )\n                .map_err(|error| error.to_string())?\n            } else {\n                restore_new_cluster(&PathBuf::from(backup), &PathBuf::from(target), node_id)\n                    .map_err(|error| error.to_string())?\n            };""",
)
replace_exact(
    "src/bin/neuralbase-backup.rs",
    """fn reject_unknown_flags(args: &[String], allowed: &[&str]) -> Result<(), String> {""",
    """fn optional_flag<'a>(args: &'a [String], flag: &str) -> Result<Option<&'a str>, String> {\n    let mut index = 0;\n    while index < args.len() {\n        if args[index] == flag {\n            let value = args\n                .get(index + 1)\n                .ok_or_else(|| format!(\"missing value for {flag}\"))?;\n            if value.starts_with(\"--\") {\n                return Err(format!(\"missing value for {flag}\"));\n            }\n            return Ok(Some(value));\n        }\n        index += 1;\n    }\n    Ok(None)\n}\n\nfn reject_unknown_flags(args: &[String], allowed: &[&str]) -> Result<(), String> {""",
)
replace_exact(
    "src/bin/neuralbase-backup.rs",
    """        \"  neuralbase-backup create --db <rocksdb-path> --output <backup.nbbk>\",\n        \"  neuralbase-backup verify --backup <backup.nbbk>\",\n        \"  neuralbase-backup restore --backup <backup.nbbk> --target <new-rocksdb-path> --node-id <fresh-node-id>\",\n        \"\",\n        \"create is offline-only: the source RocksDB must not be open by NeuralBase.\",\n        \"restore creates a fresh recovery cluster and refuses an existing target directory.\",""",
    """        \"  neuralbase-backup create --db <rocksdb-path> --output <backup> [--key-file <raw-32-byte-key>]\",\n        \"  neuralbase-backup verify --backup <backup> [--key-file <raw-32-byte-key>]\",\n        \"  neuralbase-backup restore --backup <backup> --target <new-rocksdb-path> --node-id <fresh-node-id> [--key-file <raw-32-byte-key>]\",\n        \"\",\n        \"create is offline-only: the source RocksDB must not be open by NeuralBase.\",\n        \"--key-file selects authenticated NBEC v1 encryption; key bytes are never accepted on argv.\",\n        \"without --key-file, create/verify/restore use the plaintext NBBK v1 format.\",\n        \"restore creates a fresh recovery cluster and refuses an existing target directory.\",""",
)
replace_exact(
    "src/bin/neuralbase-backup.rs",
    """    fn cli_rejects_unknown_arguments() {""",
    """    fn optional_key_file_requires_a_value() {\n        let args = vec![\"--key-file\".to_string()];\n        let error = optional_flag(&args, \"--key-file\").unwrap_err();\n        assert!(error.contains(\"missing value for --key-file\"));\n    }\n\n    #[test]\n    fn optional_key_file_returns_the_path_without_reading_key_material() {\n        let args = vec![\"--key-file\".to_string(), \"backup.key\".to_string()];\n        assert_eq!(optional_flag(&args, \"--key-file\").unwrap(), Some(\"backup.key\"));\n    }\n\n    #[test]\n    fn cli_rejects_unknown_arguments() {""",
)
