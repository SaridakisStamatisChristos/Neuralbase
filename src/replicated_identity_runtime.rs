// SPDX-License-Identifier: Apache-2.0
//! Runtime authority and migration helpers for replicated cluster identity.
//!
//! Clustered servers authenticate from the durable replicated identity registry,
//! never from a process-local mirror. Legacy `users.json` import is deliberately
//! opt-in: the operator supplies the SHA-256 of the exact file selected as the
//! migration source. Only strict SCRAM registries are accepted by the migration
//! parser; MD5 verifier material is rejected before any Raft proposal is built.

use std::path::Path;
use std::sync::Arc;

use thiserror::Error;

use crate::auth::UserRecord;
use crate::replicated_gateway::{ReplicatedGatewayError, ReplicatedSqlGateway};
pub use crate::replicated_identity_migration::IDENTITY_MIGRATION_SHA256_ENV;
use crate::replicated_identity_migration::{
    authorize_legacy_identity_candidate, load_legacy_identity_candidate,
    LegacyIdentityMigrationError,
};
use crate::replicated_identity_store::{IdentityStateError, ReplicatedIdentityState};
use crate::storage::StorageEngine;

#[derive(Debug, Error)]
pub enum ReplicatedIdentityRuntimeError {
    #[error("replicated identity state failure: {0}")]
    State(#[from] IdentityStateError),
    #[error("legacy identity migration failure: {0}")]
    Migration(#[from] LegacyIdentityMigrationError),
    #[error("replicated identity migration proposal failed: {0}")]
    Gateway(#[from] ReplicatedGatewayError),
}

/// Read one authentication record from the authoritative replicated state.
/// `Ok(None)` means either the registry has not been initialized or the user is
/// absent; callers that require authentication should fail closed in both cases.
pub fn replicated_user_record(
    engine: &Arc<StorageEngine>,
    username: &str,
) -> Result<Option<UserRecord>, ReplicatedIdentityRuntimeError> {
    let Some(state) = ReplicatedIdentityState::load(engine)? else {
        return Ok(None);
    };
    state.user_record(username).map_err(Into::into)
}

pub fn replicated_identity_initialized(
    engine: &Arc<StorageEngine>,
) -> Result<bool, ReplicatedIdentityRuntimeError> {
    Ok(ReplicatedIdentityState::load(engine)?.is_some())
}

/// A fresh clustered database may bootstrap its first user without a migration
/// only when no legacy registry path exists at all. Merely empty, malformed, or
/// unreadable legacy files do not qualify: their presence forces explicit
/// operator selection instead of silently discarding possible credentials.
pub fn allow_empty_identity_bootstrap(users_file: &str) -> bool {
    !Path::new(users_file).exists()
}

/// If the operator configured `NEURALBASE_IDENTITY_MIGRATION_SHA256` and the
/// replicated registry is still uninitialized, strictly load and authorize the
/// selected legacy registry and submit one idempotent initialization command.
///
/// This is safe to call on every connection while migration is pending. Followers
/// return `NotLeader`; once a connection reaches the leader, the initialization
/// is quorum-committed and confirmed locally applied before this function returns.
pub async fn migrate_legacy_identity_if_configured(
    gateway: &ReplicatedSqlGateway,
    engine: &Arc<StorageEngine>,
    users_file: &str,
) -> Result<bool, ReplicatedIdentityRuntimeError> {
    if replicated_identity_initialized(engine)? {
        return Ok(false);
    }
    if std::env::var(IDENTITY_MIGRATION_SHA256_ENV).is_err() {
        return Ok(false);
    }

    let candidate =
        authorize_legacy_identity_candidate(load_legacy_identity_candidate(users_file)?)?;
    gateway.initialize_identity(&candidate.records).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicated_identity::{ReplicatedIdentityUser, ReplicatedScramCredential};
    use crate::replicated_identity_store::REPLICATED_IDENTITY_STATE_KEY;
    use crate::storage::CF_META;
    use tempfile::TempDir;

    #[test]
    fn fresh_bootstrap_requires_absent_legacy_file() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("missing-users.json");
        assert!(allow_empty_identity_bootstrap(missing.to_str().unwrap()));

        let present = dir.path().join("users.json");
        std::fs::write(&present, b"").unwrap();
        assert!(!allow_empty_identity_bootstrap(present.to_str().unwrap()));
    }

    #[test]
    fn authoritative_lookup_reads_replicated_state() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let state = ReplicatedIdentityState::new(vec![ReplicatedIdentityUser {
            username: "alice".to_string(),
            credential: ReplicatedScramCredential {
                salt: vec![1; 16],
                iterations: 4_096,
                stored_key: [2; 32],
                server_key: [3; 32],
            },
        }])
        .unwrap();
        let meta_cf = engine.db.cf_handle(CF_META).unwrap();
        engine
            .db
            .put_cf(
                &meta_cf,
                REPLICATED_IDENTITY_STATE_KEY,
                state.encode().unwrap(),
            )
            .unwrap();

        assert!(replicated_identity_initialized(&engine).unwrap());
        assert_eq!(
            replicated_user_record(&engine, "alice")
                .unwrap()
                .unwrap()
                .username,
            "alice"
        );
        assert!(replicated_user_record(&engine, "missing")
            .unwrap()
            .is_none());
    }
}
