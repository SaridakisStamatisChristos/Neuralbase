// SPDX-License-Identifier: Apache-2.0
//! Defense-in-depth fail-closed adapter for Raft stable storage.
//!
//! `RaftNode` itself treats required persistence errors as fatal consensus
//! failures and fail-stops rather than logging and continuing. This adapter
//! preserves that invariant at the store boundary too by converting underlying
//! save/load/snapshot-staging errors into explicit fatal-consensus panics before
//! they can be accidentally handled as recoverable by another caller.

use std::sync::Arc;

use super::log::{PersistentState, RaftPersistenceStore};

/// Strict adapter for consensus-critical persistence.
///
/// Required stable-storage operations never return an underlying storage error.
/// They panic with an explicit fatal-consensus message instead. `RaftNode`
/// independently fail-stops returned persistence errors; this adapter is defense
/// in depth and makes misuse through another caller fail closed as well.
pub struct FailClosedPersistenceStore {
    inner: Arc<dyn RaftPersistenceStore>,
}

impl FailClosedPersistenceStore {
    pub fn new(inner: Arc<dyn RaftPersistenceStore>) -> Self {
        Self { inner }
    }
}

impl RaftPersistenceStore for FailClosedPersistenceStore {
    fn save(&self, state: &PersistentState, snapshot_data: &[u8]) -> Result<(), String> {
        match self.inner.save(state, snapshot_data) {
            Ok(()) => Ok(()),
            Err(error) => panic!("fatal Raft persistence save failure: {error}"),
        }
    }

    fn stage_snapshot(&self, snapshot_data: &[u8]) -> Result<(), String> {
        match self.inner.stage_snapshot(snapshot_data) {
            Ok(()) => Ok(()),
            Err(error) => panic!("fatal Raft snapshot staging failure: {error}"),
        }
    }

    fn load(&self) -> Result<Option<(PersistentState, Vec<u8>)>, String> {
        match self.inner.load() {
            Ok(state) => Ok(state),
            Err(error) => panic!("fatal Raft persistence load failure: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailingStore {
        fail_save: AtomicBool,
        fail_load: AtomicBool,
        fail_stage: AtomicBool,
    }

    impl FailingStore {
        fn save_failure() -> Self {
            Self {
                fail_save: AtomicBool::new(true),
                fail_load: AtomicBool::new(false),
                fail_stage: AtomicBool::new(false),
            }
        }

        fn load_failure() -> Self {
            Self {
                fail_save: AtomicBool::new(false),
                fail_load: AtomicBool::new(true),
                fail_stage: AtomicBool::new(false),
            }
        }

        fn stage_failure() -> Self {
            Self {
                fail_save: AtomicBool::new(false),
                fail_load: AtomicBool::new(false),
                fail_stage: AtomicBool::new(true),
            }
        }
    }

    impl RaftPersistenceStore for FailingStore {
        fn save(&self, _state: &PersistentState, _snapshot_data: &[u8]) -> Result<(), String> {
            if self.fail_save.load(Ordering::SeqCst) {
                Err("injected save failure".to_string())
            } else {
                Ok(())
            }
        }

        fn stage_snapshot(&self, _snapshot_data: &[u8]) -> Result<(), String> {
            if self.fail_stage.load(Ordering::SeqCst) {
                Err("injected stage failure".to_string())
            } else {
                Ok(())
            }
        }

        fn load(&self) -> Result<Option<(PersistentState, Vec<u8>)>, String> {
            if self.fail_load.load(Ordering::SeqCst) {
                Err("injected load failure".to_string())
            } else {
                Ok(None)
            }
        }
    }

    #[test]
    #[should_panic(expected = "fatal Raft persistence save failure: injected save failure")]
    fn injected_save_failure_is_fail_closed() {
        let inner: Arc<dyn RaftPersistenceStore> = Arc::new(FailingStore::save_failure());
        let strict = FailClosedPersistenceStore::new(inner);
        strict.save(&PersistentState::new(), &[]).unwrap();
    }

    #[test]
    #[should_panic(expected = "fatal Raft snapshot staging failure: injected stage failure")]
    fn injected_snapshot_stage_failure_is_fail_closed() {
        let inner: Arc<dyn RaftPersistenceStore> = Arc::new(FailingStore::stage_failure());
        let strict = FailClosedPersistenceStore::new(inner);
        strict.stage_snapshot(b"candidate").unwrap();
    }

    #[test]
    #[should_panic(expected = "fatal Raft persistence load failure: injected load failure")]
    fn injected_load_failure_is_fail_closed() {
        let inner: Arc<dyn RaftPersistenceStore> = Arc::new(FailingStore::load_failure());
        let strict = FailClosedPersistenceStore::new(inner);
        let _ = strict.load();
    }
}
