// SPDX-License-Identifier: Apache-2.0
//! Explicit, fail-closed migration from the legacy per-node `users.json` registry.
//!
//! Migration is never automatic. The operator must provide
//! `NEURALBASE_IDENTITY_MIGRATION_SHA256` matching the exact legacy file chosen
//! as authoritative on the current leader. This turns an otherwise ambiguous
//! per-node choice into an explicit, auditable decision. Legacy PostgreSQL MD5
//! password hashes are rejected because they are reusable authentication
//! material and therefore cannot be placed in the Raft log.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::auth::{ScramKeys, StoredCredential, UserRecord};
use crate::replicated_identity::ReplicatedScramCredential;

pub const IDENTITY_MIGRATION_SHA256_ENV: &str = "NEURALBASE_IDENTITY_MIGRATION_SHA256";

#[derive(Debug, Clone)]
pub struct LegacyIdentityCandidate {
    pub path: PathBuf,
    pub sha256_hex: String,
    pub records: Vec<UserRecord>,
}

#[derive(Debug, Error)]
pub enum LegacyIdentityMigrationError {
    #[error("legacy identity registry does not exist: {0}")]
    Missing(PathBuf),
    #[error("failed to read legacy identity registry {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("legacy identity registry is malformed: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("legacy identity registry contains duplicate username {0}")]
    DuplicateUser(String),
    #[error("legacy identity registry contains invalid base64 for user {0}")]
    InvalidBase64(String),
    #[error("legacy identity registry contains invalid SCRAM key length for user {0}")]
    InvalidScramKeyLength(String),
    #[error("legacy identity registry contains invalid SCRAM verifier for user {user}: {reason}")]
    InvalidScramVerifier { user: String, reason: String },
    #[error("legacy MD5 credential for user {0} is reusable authentication material and cannot be migrated into Raft")]
    ReusableMd5Credential(String),
    #[error("explicit legacy identity migration requires {IDENTITY_MIGRATION_SHA256_ENV}=<sha256-of-authoritative-users.json>")]
    ExplicitAuthorizationRequired,
    #[error("legacy identity migration digest mismatch: expected {expected}, actual {actual}")]
    DigestMismatch { expected: String, actual: String },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyRegistryFile {
    users: Vec<LegacyUserRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "method", deny_unknown_fields)]
enum LegacyUserRecord {
    #[serde(rename = "scram-sha-256")]
    ScramSha256 {
        username: String,
        salt: String,
        iterations: u32,
        stored_key: String,
        server_key: String,
    },
    #[serde(rename = "md5")]
    Md5 { username: String, md5_hash: String },
}

pub fn load_legacy_identity_candidate(
    path: impl AsRef<Path>,
) -> Result<LegacyIdentityCandidate, LegacyIdentityMigrationError> {
    let path = path.as_ref().to_path_buf();
    if !path.exists() {
        return Err(LegacyIdentityMigrationError::Missing(path));
    }
    let bytes = std::fs::read(&path).map_err(|source| LegacyIdentityMigrationError::Read {
        path: path.clone(),
        source,
    })?;
    let doc: LegacyRegistryFile = serde_json::from_slice(&bytes)?;
    let sha256_hex = hex::encode(Sha256::digest(&bytes));
    let mut seen = BTreeSet::new();
    let mut records = Vec::with_capacity(doc.users.len());

    for entry in doc.users {
        let record = match entry {
            LegacyUserRecord::ScramSha256 {
                username,
                salt,
                iterations,
                stored_key,
                server_key,
            } => {
                if !seen.insert(username.clone()) {
                    return Err(LegacyIdentityMigrationError::DuplicateUser(username));
                }
                let salt_bytes = BASE64
                    .decode(&salt)
                    .map_err(|_| LegacyIdentityMigrationError::InvalidBase64(username.clone()))?;
                let stored_key_bytes = BASE64
                    .decode(&stored_key)
                    .map_err(|_| LegacyIdentityMigrationError::InvalidBase64(username.clone()))?;
                let server_key_bytes = BASE64
                    .decode(&server_key)
                    .map_err(|_| LegacyIdentityMigrationError::InvalidBase64(username.clone()))?;
                if stored_key_bytes.len() != 32 || server_key_bytes.len() != 32 {
                    return Err(LegacyIdentityMigrationError::InvalidScramKeyLength(username));
                }
                let mut stored_key_array = [0u8; 32];
                stored_key_array.copy_from_slice(&stored_key_bytes);
                let mut server_key_array = [0u8; 32];
                server_key_array.copy_from_slice(&server_key_bytes);
                let record = UserRecord {
                    username: username.clone(),
                    credential: StoredCredential::ScramSha256(ScramKeys {
                        salt_b64: BASE64.encode(&salt_bytes),
                        iterations,
                        stored_key: stored_key_array,
                        server_key: server_key_array,
                    }),
                };
                ReplicatedScramCredential::from_user_record(&record).map_err(|error| {
                    LegacyIdentityMigrationError::InvalidScramVerifier {
                        user: username,
                        reason: error.to_string(),
                    }
                })?;
                record
            }
            LegacyUserRecord::Md5 { username, md5_hash } => {
                let _ = md5_hash;
                return Err(LegacyIdentityMigrationError::ReusableMd5Credential(username));
            }
        };
        records.push(record);
    }

    records.sort_by(|a, b| a.username.cmp(&b.username));
    Ok(LegacyIdentityCandidate {
        path,
        sha256_hex,
        records,
    })
}

pub fn authorize_legacy_identity_candidate(
    candidate: LegacyIdentityCandidate,
) -> Result<LegacyIdentityCandidate, LegacyIdentityMigrationError> {
    let expected = std::env::var(IDENTITY_MIGRATION_SHA256_ENV)
        .map_err(|_| LegacyIdentityMigrationError::ExplicitAuthorizationRequired)?;
    if !expected.eq_ignore_ascii_case(&candidate.sha256_hex) {
        return Err(LegacyIdentityMigrationError::DigestMismatch {
            expected,
            actual: candidate.sha256_hex,
        });
    }
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::create_scram_user;
    use tempfile::TempDir;

    fn write_scram_registry(path: &Path, username: &str, password: &str) {
        let record = create_scram_user(username, password);
        let StoredCredential::ScramSha256(keys) = record.credential else {
            unreachable!();
        };
        let doc = serde_json::json!({
            "users": [{
                "method": "scram-sha-256",
                "username": username,
                "salt": keys.salt_b64,
                "iterations": keys.iterations,
                "stored_key": BASE64.encode(keys.stored_key),
                "server_key": BASE64.encode(keys.server_key),
            }]
        });
        std::fs::write(path, serde_json::to_vec_pretty(&doc).unwrap()).unwrap();
    }

    #[test]
    fn strict_scram_registry_loads_and_has_stable_digest() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        write_scram_registry(&path, "alice", "secret");
        let first = load_legacy_identity_candidate(&path).unwrap();
        let second = load_legacy_identity_candidate(&path).unwrap();
        assert_eq!(first.sha256_hex, second.sha256_hex);
        assert_eq!(first.records.len(), 1);
        assert_eq!(first.records[0].username, "alice");
    }

    #[test]
    fn malformed_registry_fails_closed() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        std::fs::write(&path, b"{ definitely-not-json").unwrap();
        assert!(matches!(
            load_legacy_identity_candidate(&path).unwrap_err(),
            LegacyIdentityMigrationError::Malformed(_)
        ));
    }

    #[test]
    fn duplicate_username_fails_closed() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        let record = create_scram_user("alice", "secret");
        let StoredCredential::ScramSha256(keys) = record.credential else {
            unreachable!();
        };
        let user = serde_json::json!({
            "method": "scram-sha-256",
            "username": "alice",
            "salt": keys.salt_b64,
            "iterations": keys.iterations,
            "stored_key": BASE64.encode(keys.stored_key),
            "server_key": BASE64.encode(keys.server_key),
        });
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({"users": [user.clone(), user]})).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            load_legacy_identity_candidate(&path).unwrap_err(),
            LegacyIdentityMigrationError::DuplicateUser(user) if user == "alice"
        ));
    }

    #[test]
    fn md5_registry_is_never_a_migration_candidate() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("users.json");
        std::fs::write(
            &path,
            br#"{"users":[{"method":"md5","username":"legacy","md5_hash":"0123456789abcdef0123456789abcdef"}]}"#,
        )
        .unwrap();
        assert!(matches!(
            load_legacy_identity_candidate(&path).unwrap_err(),
            LegacyIdentityMigrationError::ReusableMd5Credential(user) if user == "legacy"
        ));
    }
}
