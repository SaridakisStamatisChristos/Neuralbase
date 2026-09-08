// SPDX-License-Identifier: Apache-2.0
//! Durable authoritative state for replicated cluster identity.
//!
//! The complete logical registry is stored as one canonical `Initialize`
//! identity payload in RocksDB `CF_META`. Replicated identity apply replaces
//! that blob in the same WriteBatch that advances the existing replicated apply
//! cursor, so a crash cannot expose half-applied identity state.

use std::sync::Arc;

use thiserror::Error;

use crate::auth::UserRecord;
use crate::replicated_identity::{
    IdentityCodecError, ReplicatedIdentityMutation, ReplicatedIdentityUser,
    ReplicatedScramCredential,
};
use crate::storage::{StorageEngine, CF_META};

pub const REPLICATED_IDENTITY_STATE_KEY: &[u8] = b"raft/identity/state-v1";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicatedIdentityState {
    users: Vec<ReplicatedIdentityUser>,
}

#[derive(Debug, Error)]
pub enum IdentityStateError {
    #[error("invalid replicated identity state: {0}")]
    Codec(#[from] IdentityCodecError),
    #[error("rocksdb failure while reading replicated identity state: {0}")]
    Rocks(#[from] rocksdb::Error),
    #[error("replicated identity state contains a non-initialization payload")]
    InvalidStatePayload,
    #[error("identity user already exists: {0}")]
    UserAlreadyExists(String),
    #[error("identity user does not exist: {0}")]
    UserNotFound(String),
}

impl ReplicatedIdentityState {
    pub fn new(users: Vec<ReplicatedIdentityUser>) -> Result<Self, IdentityStateError> {
        let encoded = ReplicatedIdentityMutation::Initialize { users }.encode()?;
        Self::decode(&encoded)
    }

    pub fn from_user_records(records: &[UserRecord]) -> Result<Self, IdentityStateError> {
        let mut users = Vec::with_capacity(records.len());
        for record in records {
            users.push(ReplicatedIdentityUser {
                username: record.username.clone(),
                credential: ReplicatedScramCredential::from_user_record(record)?,
            });
        }
        Self::new(users)
    }

    pub fn load(engine: &Arc<StorageEngine>) -> Result<Option<Self>, IdentityStateError> {
        let meta_cf = engine
            .db
            .cf_handle(CF_META)
            .expect("CF_META must exist after StorageEngine::open");
        match engine.db.get_cf(&meta_cf, REPLICATED_IDENTITY_STATE_KEY)? {
            Some(bytes) => Self::decode(&bytes).map(Some),
            None => Ok(None),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, IdentityStateError> {
        ReplicatedIdentityMutation::Initialize {
            users: self.users.clone(),
        }
        .encode()
        .map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IdentityStateError> {
        match ReplicatedIdentityMutation::decode(bytes)? {
            ReplicatedIdentityMutation::Initialize { users } => Ok(Self { users }),
            _ => Err(IdentityStateError::InvalidStatePayload),
        }
    }

    pub fn users(&self) -> &[ReplicatedIdentityUser] {
        &self.users
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    pub fn contains_user(&self, username: &str) -> bool {
        self.find(username).is_ok()
    }

    pub fn user_record(&self, username: &str) -> Result<Option<UserRecord>, IdentityStateError> {
        let Ok(index) = self.find(username) else {
            return Ok(None);
        };
        self.users[index]
            .credential
            .to_user_record(&self.users[index].username)
            .map(Some)
            .map_err(Into::into)
    }

    pub fn create_user(&mut self, user: ReplicatedIdentityUser) -> Result<(), IdentityStateError> {
        match self.find(&user.username) {
            Ok(_) => Err(IdentityStateError::UserAlreadyExists(user.username)),
            Err(position) => {
                self.users.insert(position, user);
                self.validate_canonical()
            }
        }
    }

    pub fn alter_user(&mut self, user: ReplicatedIdentityUser) -> Result<(), IdentityStateError> {
        let index = self
            .find(&user.username)
            .map_err(|_| IdentityStateError::UserNotFound(user.username.clone()))?;
        self.users[index] = user;
        self.validate_canonical()
    }

    pub fn drop_user(
        &mut self,
        username: &str,
        if_exists: bool,
    ) -> Result<bool, IdentityStateError> {
        match self.find(username) {
            Ok(index) => {
                self.users.remove(index);
                Ok(true)
            }
            Err(_) if if_exists => Ok(false),
            Err(_) => Err(IdentityStateError::UserNotFound(username.to_string())),
        }
    }

    fn find(&self, username: &str) -> Result<usize, usize> {
        self.users
            .binary_search_by(|user| user.username.as_str().cmp(username))
    }

    fn validate_canonical(&self) -> Result<(), IdentityStateError> {
        let encoded = self.encode()?;
        let decoded = Self::decode(&encoded)?;
        if decoded == *self {
            Ok(())
        } else {
            Err(IdentityStateError::InvalidStatePayload)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicated_identity::ReplicatedScramCredential;
    use tempfile::TempDir;

    fn user(name: &str, seed: u8) -> ReplicatedIdentityUser {
        ReplicatedIdentityUser {
            username: name.to_string(),
            credential: ReplicatedScramCredential {
                salt: vec![seed; 16],
                iterations: 4_096,
                stored_key: [seed.wrapping_add(1); 32],
                server_key: [seed.wrapping_add(2); 32],
            },
        }
    }

    #[test]
    fn state_is_canonical_and_mutations_preserve_order() {
        let mut state = ReplicatedIdentityState::new(vec![user("bob", 2), user("alice", 1)])
            .expect("canonical identity state");
        assert_eq!(
            state
                .users()
                .iter()
                .map(|user| user.username.as_str())
                .collect::<Vec<_>>(),
            vec!["alice", "bob"]
        );

        state.create_user(user("carol", 3)).unwrap();
        assert!(state.contains_user("carol"));
        assert!(matches!(
            state.create_user(user("carol", 4)).unwrap_err(),
            IdentityStateError::UserAlreadyExists(_)
        ));
        state.alter_user(user("carol", 5)).unwrap();
        assert!(state.drop_user("carol", false).unwrap());
        assert!(!state.contains_user("carol"));
        assert!(!state.drop_user("missing", true).unwrap());
    }

    #[test]
    fn load_rejects_non_state_payload() {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let meta_cf = engine.db.cf_handle(CF_META).unwrap();
        let command = ReplicatedIdentityMutation::CreateUser {
            username: "alice".to_string(),
            credential: user("alice", 1).credential,
        }
        .encode()
        .unwrap();
        engine
            .db
            .put_cf(&meta_cf, REPLICATED_IDENTITY_STATE_KEY, command)
            .unwrap();

        assert!(matches!(
            ReplicatedIdentityState::load(&engine).unwrap_err(),
            IdentityStateError::InvalidStatePayload
        ));
    }
}
