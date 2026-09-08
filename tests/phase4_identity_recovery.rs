// SPDX-License-Identifier: Apache-2.0
//! Crash/restart and replay evidence for the Phase-4 identity state machine.

use std::sync::Arc;

use neuralbase::catalog::InMemoryCatalog;
use neuralbase::consensus::rpc::LogEntry;
use neuralbase::hlc::HlcClock;
use neuralbase::replicated_identity::{
    ReplicatedIdentityMutation, ReplicatedIdentityUser, ReplicatedScramCredential,
};
use neuralbase::replicated_identity_store::ReplicatedIdentityState;
use neuralbase::replicated_state_machine::{ReplicatedApplyOutcome, ReplicatedSqlStateMachine};
use neuralbase::storage::StorageEngine;
use tempfile::TempDir;

fn credential(seed: u8) -> ReplicatedScramCredential {
    ReplicatedScramCredential {
        salt: vec![seed; 16],
        iterations: 4_096,
        stored_key: [seed.wrapping_add(1); 32],
        server_key: [seed.wrapping_add(2); 32],
    }
}

fn entry(index: u64, mutation: ReplicatedIdentityMutation) -> LogEntry {
    LogEntry {
        term: 1,
        index,
        command: mutation.encode().unwrap(),
    }
}

#[test]
fn restart_preserves_identity_and_replay_before_apply_cursor_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let create_entry = entry(
        2,
        ReplicatedIdentityMutation::CreateUser {
            username: "alice".to_string(),
            credential: credential(7),
        },
    );

    {
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let sm = ReplicatedSqlStateMachine::new(Arc::clone(&engine), catalog, clock).unwrap();

        let init = entry(1, ReplicatedIdentityMutation::Initialize { users: vec![] });
        assert!(matches!(
            sm.apply_log_entry(&init).unwrap(),
            ReplicatedApplyOutcome::Applied { index: 1, .. }
        ));
        assert!(matches!(
            sm.apply_log_entry(&create_entry).unwrap(),
            ReplicatedApplyOutcome::Applied { index: 2, .. }
        ));
        assert!(ReplicatedIdentityState::load(&engine)
            .unwrap()
            .unwrap()
            .contains_user("alice"));
    }

    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let catalog = Arc::new(InMemoryCatalog::default());
    let clock = Arc::new(HlcClock::new(500));
    let recovered = ReplicatedSqlStateMachine::new(Arc::clone(&engine), catalog, clock).unwrap();

    let before = ReplicatedIdentityState::load(&engine).unwrap().unwrap();
    assert!(before.contains_user("alice"));
    assert_eq!(recovered.durable_state().unwrap().last_applied_index, 2);
    assert_eq!(
        recovered.apply_log_entry(&create_entry).unwrap(),
        ReplicatedApplyOutcome::AlreadyApplied { index: 2 }
    );
    assert_eq!(
        ReplicatedIdentityState::load(&engine).unwrap(),
        Some(before)
    );
}

#[test]
fn conflicting_reinitialization_fails_without_advancing_or_overwriting_identity() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let catalog = Arc::new(InMemoryCatalog::default());
    let clock = Arc::new(HlcClock::new(500));
    let sm = ReplicatedSqlStateMachine::new(Arc::clone(&engine), catalog, clock).unwrap();

    let original = ReplicatedIdentityMutation::Initialize {
        users: vec![ReplicatedIdentityUser {
            username: "alice".to_string(),
            credential: credential(1),
        }],
    };
    sm.apply_log_entry(&entry(1, original)).unwrap();
    let before = ReplicatedIdentityState::load(&engine).unwrap().unwrap();

    let conflicting = ReplicatedIdentityMutation::Initialize {
        users: vec![ReplicatedIdentityUser {
            username: "bob".to_string(),
            credential: credential(2),
        }],
    };
    assert!(sm.apply_log_entry(&entry(2, conflicting)).is_err());
    assert_eq!(sm.durable_state().unwrap().last_applied_index, 1);
    assert_eq!(
        ReplicatedIdentityState::load(&engine).unwrap(),
        Some(before)
    );
}
