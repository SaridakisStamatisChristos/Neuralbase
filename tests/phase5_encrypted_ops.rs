// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::sync::Arc;

use neuralbase::backup_encryption::{
    create_encrypted_offline_backup_at, verify_encrypted_backup_file, BackupEncryptionError,
    BackupEncryptionKey, BACKUP_ENCRYPTION_KEY_BYTES,
};
use neuralbase::consensus::{ClusterMembership, PersistentState, RaftPersistenceStore};
use neuralbase::raft_persistence::RocksDbRaftPersistenceStore;
use neuralbase::restore::{restore_encrypted_new_cluster, RestoreError};
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;

fn key(byte: u8) -> BackupEncryptionKey {
    BackupEncryptionKey::from_bytes([byte; BACKUP_ENCRYPTION_KEY_BYTES])
}

fn initialize_source(root: &TempDir) -> std::path::PathBuf {
    let db_path = root.path().join("db");
    let engine = Arc::new(StorageEngine::open(&db_path).unwrap());
    let store = RocksDbRaftPersistenceStore::new(Arc::clone(&engine));
    let mut persistent = PersistentState::new();
    persistent.membership = Some(ClusterMembership::bootstrap(
        "source-a".to_string(),
        Vec::<String>::new(),
    ));
    store.save(&persistent, b"").unwrap();
    drop(store);
    drop(engine);
    db_path
}

#[test]
fn encrypted_offline_backup_verifies_and_restores_without_plaintext_artifact() {
    let source_root = TempDir::new().unwrap();
    let output_root = TempDir::new().unwrap();
    let db_path = initialize_source(&source_root);
    let backup_path = output_root.path().join("cluster.nbec");
    let encryption_key = key(0x41);

    let manifest = create_encrypted_offline_backup_at(
        &db_path,
        &backup_path,
        &encryption_key,
        1_725_000_000_123,
    )
    .unwrap();
    assert!(manifest.encrypted);
    let bytes = fs::read(&backup_path).unwrap();
    assert_eq!(&bytes[..4], b"NBEC");
    assert_ne!(&bytes[..4], b"NBBK");

    let verified = verify_encrypted_backup_file(&backup_path, &encryption_key).unwrap();
    assert!(verified.manifest.encrypted);
    assert_eq!(verified.manifest, manifest);

    let target = output_root.path().join("restored-db");
    let report = restore_encrypted_new_cluster(
        &backup_path,
        &encryption_key,
        &target,
        "recovery-a",
    )
    .unwrap();
    assert!(report.source_manifest.encrypted);
    assert!(target.join("CURRENT").is_file());
}

#[test]
fn wrong_key_fails_before_restore_target_is_created() {
    let source_root = TempDir::new().unwrap();
    let output_root = TempDir::new().unwrap();
    let db_path = initialize_source(&source_root);
    let backup_path = output_root.path().join("cluster.nbec");
    create_encrypted_offline_backup_at(&db_path, &backup_path, &key(7), 1234).unwrap();

    let target = output_root.path().join("wrong-key-target");
    let error = restore_encrypted_new_cluster(&backup_path, &key(8), &target, "recovery-a")
        .unwrap_err();
    assert!(matches!(
        error,
        RestoreError::EncryptedBackup(BackupEncryptionError::AuthenticationFailed)
    ));
    assert!(!target.exists());
}

#[test]
fn tampered_ciphertext_fails_before_restore_target_is_created() {
    let source_root = TempDir::new().unwrap();
    let output_root = TempDir::new().unwrap();
    let db_path = initialize_source(&source_root);
    let backup_path = output_root.path().join("cluster.nbec");
    let encryption_key = key(7);
    create_encrypted_offline_backup_at(&db_path, &backup_path, &encryption_key, 1234).unwrap();

    let mut bytes = fs::read(&backup_path).unwrap();
    let index = bytes.len() / 2;
    bytes[index] ^= 0x40;
    fs::write(&backup_path, bytes).unwrap();

    let target = output_root.path().join("tampered-target");
    let error = restore_encrypted_new_cluster(
        &backup_path,
        &encryption_key,
        &target,
        "recovery-a",
    )
    .unwrap_err();
    assert!(matches!(
        error,
        RestoreError::EncryptedBackup(BackupEncryptionError::AuthenticationFailed)
    ));
    assert!(!target.exists());
}

#[cfg(unix)]
#[test]
fn encrypted_backup_publication_is_restrictive() {
    use std::os::unix::fs::PermissionsExt;

    let source_root = TempDir::new().unwrap();
    let output_root = TempDir::new().unwrap();
    let db_path = initialize_source(&source_root);
    let backup_path = output_root.path().join("cluster.nbec");
    create_encrypted_offline_backup_at(&db_path, &backup_path, &key(7), 1234).unwrap();

    let mode = fs::metadata(&backup_path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
}
