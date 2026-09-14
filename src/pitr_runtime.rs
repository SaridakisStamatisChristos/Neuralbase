// SPDX-License-Identifier: Apache-2.0
//! Runtime integration for synchronous Phase-9 archival.
//!
//! This module deliberately sits after durable logical apply and before Raft's
//! confirmed-apply completion. When enabled, an entry is not confirmed applied
//! to Raft until its required archive segment is durably published.

use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::consensus::{LogEntry, RaftPersistenceStore};
use crate::pitr_archive::{load_pitr_archive_key, PitrArchiveWriter};
use crate::raft_persistence::RocksDbRaftPersistenceStore;
use crate::replicated_state_machine::ReplicatedSqlStateMachine;
use crate::storage::StorageEngine;

pub const PITR_ARCHIVE_DIR_ENV: &str = "NEURALBASE_PITR_ARCHIVE_DIR";
pub const PITR_KEY_FILE_ENV: &str = "NEURALBASE_PITR_KEY_FILE";

pub struct PitrRuntimeArchiver {
    writer: PitrArchiveWriter,
}

impl PitrRuntimeArchiver {
    pub fn open_from_env(
        engine: Arc<StorageEngine>,
        state_machine: &ReplicatedSqlStateMachine,
    ) -> io::Result<Option<Self>> {
        let archive_dir = std::env::var_os(PITR_ARCHIVE_DIR_ENV);
        let key_file = std::env::var_os(PITR_KEY_FILE_ENV);
        let Some(archive_dir) = archive_dir else {
            if key_file.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{PITR_KEY_FILE_ENV} requires {PITR_ARCHIVE_DIR_ENV}"),
                ));
            }
            return Ok(None);
        };

        let key = key_file
            .map(|path| load_pitr_archive_key(Path::new(&path)).map_err(io::Error::other))
            .transpose()?;
        let writer = PitrArchiveWriter::open(Path::new(&archive_dir), key)
            .map_err(|error| io::Error::other(format!("open PITR archive stream: {error}")))?;
        let status = writer.status();
        let durable = state_machine
            .durable_state()
            .map_err(|error| io::Error::other(format!("read durable apply state for PITR: {error}")))?;
        if status.metadata.baseline_index > durable.last_applied_index {
            return Err(io::Error::other(format!(
                "PITR baseline index {} is newer than durable state-machine apply index {}",
                status.metadata.baseline_index, durable.last_applied_index
            )));
        }
        if status.frontier.index > durable.last_applied_index {
            return Err(io::Error::other(format!(
                "PITR archive frontier {} is newer than durable state-machine apply index {}",
                status.frontier.index, durable.last_applied_index
            )));
        }

        let raft_store = RocksDbRaftPersistenceStore::new(engine);
        if let Some((persistent, _)) = raft_store
            .load()
            .map_err(|error| io::Error::other(format!("read Raft state for PITR: {error}")))?
        {
            if persistent.snapshot_index > status.frontier.index {
                return Err(io::Error::other(format!(
                    "PITR archive frontier {} is behind compacted Raft snapshot index {}; required recovery history is unavailable",
                    status.frontier.index, persistent.snapshot_index
                )));
            }
        }

        Ok(Some(Self { writer }))
    }

    /// Publish the archive record required for one already-durably-applied entry.
    ///
    /// Entries at or before the bound backup baseline are already represented by
    /// the baseline artifact and are intentionally not duplicated into the stream.
    pub fn archive_applied(&mut self, entry: &LogEntry) -> Result<(), String> {
        if entry.index <= self.writer.metadata().baseline_index {
            return Ok(());
        }
        self.writer
            .append_committed(entry)
            .map(|_| ())
            .map_err(|error| format!("PITR archive publication failed at index {}: {error}", entry.index))
    }

    pub fn durable_frontier(&self) -> u64 {
        self.writer.status().frontier.index
    }

    pub fn timeline(&self) -> [u8; 16] {
        self.writer.metadata().timeline
    }
}
