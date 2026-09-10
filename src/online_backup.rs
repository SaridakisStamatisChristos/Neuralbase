// SPDX-License-Identifier: Apache-2.0
//! Leader-coordinated online operator backup.
//!
//! Online backup never opens a second RocksDB process. A running Raft node
//! submits a non-SQL barrier and waits for confirmed state-machine apply, then
//! exports the logical SQL/identity state through one RocksDB snapshot. Raft
//! durable state is sampled before and after export; any concurrent log,
//! membership, term, or snapshot movement invalidates the attempt. Bounded
//! retries provide availability during light traffic while preserving a strict
//! fail-closed consistency contract under sustained concurrent activity.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{sleep, Duration, Instant};

use crate::backup::{BackupCodecError, BackupKind, BackupManifest, NeuralBaseBackup};
use crate::backup_encryption::{
    publish_encrypted_backup, BackupEncryptionError, BackupEncryptionKey,
};
use crate::catalog::InMemoryCatalog;
use crate::consensus::{
    ClientCommand, PersistentState, RaftPersistenceStore, RaftRole, RaftShared,
};
use crate::hlc::HlcClock;
use crate::offline_backup::{verify_backup_file, OfflineBackupError};
use crate::raft_persistence::RocksDbRaftPersistenceStore;
use crate::replicated_snapshot_manager::{ReplicatedSqlSnapshotManager, SnapshotManagerError};
use crate::storage::StorageEngine;

const ONLINE_BACKUP_BARRIER: &[u8] = b"NBBK-ONLINE\x01";
const MAX_CAPTURE_ATTEMPTS: usize = 8;
const SHARED_BOUNDARY_WAIT: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum OnlineBackupError {
    #[error("online backup destination must have an existing parent directory")]
    DestinationParentMissing,
    #[error("online backup destination must name a file")]
    DestinationFileNameMissing,
    #[error("online backup destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("cluster node is catching up replicated state")]
    CatchingUp,
    #[error("online backup requires the Raft leader; current leader is {leader:?}")]
    NotLeader { leader: Option<String> },
    #[error("Raft command channel closed during online backup")]
    CommandChannelClosed,
    #[error("Raft acknowledgement channel closed during online backup")]
    ReplyChannelClosed,
    #[error("Raft rejected online-backup barrier: {0}")]
    RaftRejected(String),
    #[error("read durable Raft state for online backup: {0}")]
    RaftPersistence(String),
    #[error("online backup requires initialized durable Raft state")]
    MissingRaftState,
    #[error("online backup requires committed dynamic membership")]
    MissingMembership,
    #[error("online backup refuses a joint membership transition")]
    JointMembership,
    #[error("online backup refuses a staged Raft snapshot transition")]
    StagedRaftSnapshot,
    #[error("invalid committed membership during online backup: {0}")]
    InvalidMembership(String),
    #[error("committed membership index {membership_index} exceeds backup boundary {boundary}")]
    MembershipBeyondBoundary {
        membership_index: u64,
        boundary: u64,
    },
    #[error("online backup boundary {0} has no durable Raft term")]
    MissingBoundaryTerm(u64),
    #[error("online backup could not obtain a stable Raft/state-machine capture after {MAX_CAPTURE_ATTEMPTS} attempts")]
    ConcurrentActivity,
    #[error("export logical state for online backup: {0}")]
    Snapshot(#[from] SnapshotManagerError),
    #[error("encode online backup: {0}")]
    Codec(#[from] BackupCodecError),
    #[error("independent online backup verification failed: {0}")]
    Verification(OfflineBackupError),
    #[error("encrypted online backup publication failed: {0}")]
    Encryption(#[from] BackupEncryptionError),
    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,
    #[error("backup timestamp does not fit milliseconds since Unix epoch")]
    ClockOverflow,
    #[error("online backup I/O failure: {0}")]
    Io(#[from] io::Error),
}

#[derive(Clone)]
pub struct OnlineBackupCoordinator {
    client_tx: mpsc::Sender<ClientCommand>,
    shared: Arc<Mutex<RaftShared>>,
    engine: Arc<StorageEngine>,
    clock: Arc<HlcClock>,
    serving_ready: Arc<AtomicBool>,
}

impl OnlineBackupCoordinator {
    pub fn new(
        client_tx: mpsc::Sender<ClientCommand>,
        shared: Arc<Mutex<RaftShared>>,
        engine: Arc<StorageEngine>,
        clock: Arc<HlcClock>,
        serving_ready: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client_tx,
            shared,
            engine,
            clock,
            serving_ready,
        }
    }

    pub async fn create_online_backup(
        &self,
        destination: &Path,
    ) -> Result<BackupManifest, OnlineBackupError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| OnlineBackupError::ClockBeforeEpoch)?;
        let created_unix_ms =
            u64::try_from(duration.as_millis()).map_err(|_| OnlineBackupError::ClockOverflow)?;
        self.create_online_backup_at(destination, created_unix_ms)
            .await
    }

    /// Deterministic timestamp variant used by executable tests.
    pub async fn create_online_backup_at(
        &self,
        destination: &Path,
        created_unix_ms: u64,
    ) -> Result<BackupManifest, OnlineBackupError> {
        validate_destination(destination)?;
        let backup = self.capture_online_backup_at(created_unix_ms).await?;
        let encoded = backup.encode()?;
        publish_atomically(destination, &encoded, created_unix_ms)?;
        Ok(backup.manifest)
    }

    /// Create a leader-coordinated online backup directly as an authenticated NBEC artifact.
    ///
    /// Capture semantics are identical to plaintext online backup: one confirmed Raft barrier,
    /// one stable state-machine boundary, and fail-closed retry on concurrent durable movement.
    /// Plaintext NBBK bytes are never published when this method is selected.
    pub async fn create_encrypted_online_backup(
        &self,
        destination: &Path,
        key: &BackupEncryptionKey,
    ) -> Result<BackupManifest, OnlineBackupError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| OnlineBackupError::ClockBeforeEpoch)?;
        let created_unix_ms =
            u64::try_from(duration.as_millis()).map_err(|_| OnlineBackupError::ClockOverflow)?;
        self.create_encrypted_online_backup_at(destination, key, created_unix_ms)
            .await
    }

    /// Deterministic timestamp variant used by executable tests.
    pub async fn create_encrypted_online_backup_at(
        &self,
        destination: &Path,
        key: &BackupEncryptionKey,
        created_unix_ms: u64,
    ) -> Result<BackupManifest, OnlineBackupError> {
        validate_destination(destination)?;
        let backup = self.capture_online_backup_at(created_unix_ms).await?;
        publish_encrypted_backup(&backup, destination, key, created_unix_ms).map_err(Into::into)
    }

    async fn capture_online_backup_at(
        &self,
        created_unix_ms: u64,
    ) -> Result<NeuralBaseBackup, OnlineBackupError> {
        for _ in 0..MAX_CAPTURE_ATTEMPTS {
            self.ensure_leader().await?;
            let boundary = self.submit_barrier().await?;
            self.await_shared_boundary(boundary).await?;

            let raft_store = RocksDbRaftPersistenceStore::new(Arc::clone(&self.engine));
            if raft_store
                .load_staged_snapshot()
                .map_err(OnlineBackupError::RaftPersistence)?
                .is_some()
            {
                return Err(OnlineBackupError::StagedRaftSnapshot);
            }
            let (before, active_before) = raft_store
                .load()
                .map_err(OnlineBackupError::RaftPersistence)?
                .ok_or(OnlineBackupError::MissingRaftState)?;
            let membership = before
                .membership
                .clone()
                .ok_or(OnlineBackupError::MissingMembership)?;
            membership
                .validate()
                .map_err(OnlineBackupError::InvalidMembership)?;
            if membership.is_joint() {
                return Err(OnlineBackupError::JointMembership);
            }
            if membership.config_index > boundary {
                return Err(OnlineBackupError::MembershipBeyondBoundary {
                    membership_index: membership.config_index,
                    boundary,
                });
            }

            // The barrier must still be the durable log frontier. If another
            // client/admin command has already appended, discard this attempt.
            if before.last_log_index() != boundary {
                continue;
            }
            let boundary_term = before.term_at(boundary);
            if boundary == 0 || boundary_term == 0 {
                return Err(OnlineBackupError::MissingBoundaryTerm(boundary));
            }

            let manager = ReplicatedSqlSnapshotManager::new(
                Arc::clone(&self.engine),
                Arc::new(InMemoryCatalog::default()),
                Arc::clone(&self.clock),
            );
            let sql_snapshot = match manager.export(boundary, boundary_term) {
                Ok(snapshot) => snapshot,
                Err(SnapshotManagerError::ApplyIndexBeyondRequestedSnapshot { .. }) => continue,
                Err(error) => return Err(error.into()),
            };

            if raft_store
                .load_staged_snapshot()
                .map_err(OnlineBackupError::RaftPersistence)?
                .is_some()
            {
                return Err(OnlineBackupError::StagedRaftSnapshot);
            }
            let (after, active_after) = raft_store
                .load()
                .map_err(OnlineBackupError::RaftPersistence)?
                .ok_or(OnlineBackupError::MissingRaftState)?;
            if !raft_fingerprint_unchanged(&before, &active_before, &after, &active_after) {
                continue;
            }

            self.ensure_leader().await?;
            let backup = NeuralBaseBackup::new_online(created_unix_ms, membership, sql_snapshot)?;
            if backup.manifest.metadata.latest_sql_apply_index != boundary {
                // A confirmed barrier must be the durable apply frontier inside
                // the same RocksDB snapshot. Anything else is a raced capture.
                continue;
            }
            return Ok(backup);
        }

        Err(OnlineBackupError::ConcurrentActivity)
    }

    async fn ensure_leader(&self) -> Result<(), OnlineBackupError> {
        if !self.serving_ready.load(Ordering::Acquire) {
            return Err(OnlineBackupError::CatchingUp);
        }
        let shared = self.shared.lock().await;
        if shared.role != RaftRole::Leader {
            return Err(OnlineBackupError::NotLeader {
                leader: shared.leader_id.clone(),
            });
        }
        Ok(())
    }

    async fn submit_barrier(&self) -> Result<u64, OnlineBackupError> {
        let (reply, response) = oneshot::channel();
        self.client_tx
            .send(ClientCommand {
                payload: ONLINE_BACKUP_BARRIER.to_vec(),
                reply,
            })
            .await
            .map_err(|_| OnlineBackupError::CommandChannelClosed)?;
        response
            .await
            .map_err(|_| OnlineBackupError::ReplyChannelClosed)?
            .map_err(OnlineBackupError::RaftRejected)
    }

    async fn await_shared_boundary(&self, boundary: u64) -> Result<(), OnlineBackupError> {
        let deadline = Instant::now() + SHARED_BOUNDARY_WAIT;
        loop {
            let shared = self.shared.lock().await;
            if shared.role != RaftRole::Leader {
                return Err(OnlineBackupError::NotLeader {
                    leader: shared.leader_id.clone(),
                });
            }
            if shared.commit_index >= boundary && shared.last_applied >= boundary {
                return Ok(());
            }
            drop(shared);
            if Instant::now() >= deadline {
                return Err(OnlineBackupError::ConcurrentActivity);
            }
            sleep(Duration::from_millis(1)).await;
        }
    }
}

fn raft_fingerprint_unchanged(
    before: &PersistentState,
    active_before: &[u8],
    after: &PersistentState,
    active_after: &[u8],
) -> bool {
    before.current_term == after.current_term
        && before.voted_for == after.voted_for
        && before.log == after.log
        && before.snapshot_index == after.snapshot_index
        && before.snapshot_term == after.snapshot_term
        && before.membership == after.membership
        && active_before == active_after
}

fn validate_destination(destination: &Path) -> Result<(), OnlineBackupError> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(OnlineBackupError::DestinationParentMissing);
    }
    if destination.file_name().is_none() {
        return Err(OnlineBackupError::DestinationFileNameMissing);
    }
    if destination.exists() {
        return Err(OnlineBackupError::DestinationExists(
            destination.to_path_buf(),
        ));
    }
    Ok(())
}

fn publish_atomically(
    destination: &Path,
    bytes: &[u8],
    created_unix_ms: u64,
) -> Result<(), OnlineBackupError> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = destination
        .file_name()
        .ok_or(OnlineBackupError::DestinationFileNameMissing)?
        .to_string_lossy();

    let mut staged_file = None;
    for suffix in 0..128u16 {
        let staged = parent.join(format!(
            ".{file_name}.online-partial-{}-{created_unix_ms}-{suffix}",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&staged) {
            Ok(file) => {
                staged_file = Some((file, staged));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    let (mut file, staged) = staged_file.ok_or_else(|| {
        OnlineBackupError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "unable to allocate online-backup staging file",
        ))
    })?;

    let result = (|| -> Result<(), OnlineBackupError> {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);

        let verified = verify_backup_file(&staged).map_err(OnlineBackupError::Verification)?;
        if verified.manifest.kind != BackupKind::Online {
            return Err(OnlineBackupError::Verification(
                OfflineBackupError::VerificationCorrupt(BackupCodecError::ManifestMismatch(
                    "backup kind",
                )),
            ));
        }

        match fs::hard_link(&staged, destination) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(OnlineBackupError::DestinationExists(
                    destination.to_path_buf(),
                ));
            }
            Err(error) => return Err(error.into()),
        }
        sync_parent_dir(parent)?;
        fs::remove_file(&staged)?;
        sync_parent_dir(parent)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&staged);
    }
    result
}

#[cfg(unix)]
fn sync_parent_dir(parent: &Path) -> Result<(), OnlineBackupError> {
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_parent: &Path) -> Result<(), OnlineBackupError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup_encryption::verify_encrypted_backup_file;
    use crate::consensus::{ChannelTransport, CommittedEntry, RaftNode, RaftPersistenceStore};
    use crate::replicated_state_machine::ReplicatedSqlStateMachine;
    use tempfile::TempDir;

    struct Harness {
        coordinator: OnlineBackupCoordinator,
        handle: crate::consensus::RaftTaskHandle,
        apply_task: tokio::task::JoinHandle<()>,
        _db: TempDir,
    }

    async fn single_node_harness() -> Harness {
        let db = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(db.path()).unwrap());
        let catalog = Arc::new(InMemoryCatalog::default());
        let clock = Arc::new(HlcClock::new(500));
        let state_machine = Arc::new(
            ReplicatedSqlStateMachine::new(
                Arc::clone(&engine),
                Arc::clone(&catalog),
                Arc::clone(&clock),
            )
            .unwrap(),
        );
        let raw_store: Arc<dyn RaftPersistenceStore> =
            Arc::new(RocksDbRaftPersistenceStore::new(Arc::clone(&engine)));

        let bus = ChannelTransport::new_bus();
        let transport = Arc::new(ChannelTransport::register("solo".to_string(), bus).await);
        let (apply_tx, mut apply_rx) = mpsc::channel::<CommittedEntry>(16);
        let apply_sm = Arc::clone(&state_machine);
        let apply_task = tokio::spawn(async move {
            while let Some(committed) = apply_rx.recv().await {
                let result = apply_sm
                    .apply_log_entry(&committed.entry)
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let failed = result.is_err();
                let _ = committed.completion.send(result);
                if failed {
                    break;
                }
            }
        });

        let mut node = RaftNode::new("solo".to_string(), Vec::new(), transport)
            .with_persistence(raw_store)
            .with_confirmed_apply_tx(apply_tx);
        node.set_election_timeout_ms(20);
        let readiness = node.serving_readiness();
        let (client_tx, shared, handle) = node.spawn();

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if shared.lock().await.role == RaftRole::Leader {
                break;
            }
            assert!(Instant::now() < deadline, "leader election timed out");
            sleep(Duration::from_millis(5)).await;
        }

        Harness {
            coordinator: OnlineBackupCoordinator::new(client_tx, shared, engine, clock, readiness),
            handle,
            apply_task,
            _db: db,
        }
    }

    #[tokio::test]
    async fn online_backup_is_exact_boundary_and_independently_verifiable() {
        let harness = single_node_harness().await;
        let output = TempDir::new().unwrap();
        let destination = output.path().join("online.nbbk");

        let manifest = harness
            .coordinator
            .create_online_backup_at(&destination, 1234)
            .await
            .unwrap();
        assert_eq!(manifest.kind, BackupKind::Online);
        assert!(manifest.metadata.last_included_index > 0);
        assert_eq!(
            manifest.metadata.latest_sql_apply_index,
            manifest.metadata.last_included_index
        );

        let verified = verify_backup_file(&destination).unwrap();
        assert_eq!(verified.manifest, manifest);
        assert_eq!(verified.membership.voters.len(), 1);
        assert_eq!(verified.manifest.kind, BackupKind::Online);
        assert!(output.path().read_dir().unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("partial")));

        harness.handle.shutdown().await;
        harness.apply_task.abort();
    }

    #[tokio::test]
    async fn encrypted_online_backup_reuses_exact_boundary_and_authenticated_publication() {
        let harness = single_node_harness().await;
        let output = TempDir::new().unwrap();
        let destination = output.path().join("online.nbec");
        let key = BackupEncryptionKey::from_bytes([0x41; 32]);

        let manifest = harness
            .coordinator
            .create_encrypted_online_backup_at(&destination, &key, 2234)
            .await
            .unwrap();
        assert_eq!(manifest.kind, BackupKind::Online);
        assert!(manifest.encrypted);
        assert!(manifest.metadata.last_included_index > 0);
        assert_eq!(
            manifest.metadata.latest_sql_apply_index,
            manifest.metadata.last_included_index
        );

        let verified = verify_encrypted_backup_file(&destination, &key).unwrap();
        assert_eq!(verified.manifest, manifest);
        assert_eq!(verified.manifest.kind, BackupKind::Online);
        assert!(verified.manifest.encrypted);

        let wrong_key = BackupEncryptionKey::from_bytes([0x42; 32]);
        assert!(matches!(
            verify_encrypted_backup_file(&destination, &wrong_key),
            Err(BackupEncryptionError::AuthenticationFailed)
        ));
        assert!(output.path().read_dir().unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("partial")));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&destination).unwrap().permissions().mode() & 0o077,
                0
            );
        }

        harness.handle.shutdown().await;
        harness.apply_task.abort();
    }

    #[tokio::test]
    async fn existing_destination_is_never_overwritten() {
        let harness = single_node_harness().await;
        let output = TempDir::new().unwrap();
        let destination = output.path().join("online.nbbk");
        fs::write(&destination, b"keep-me").unwrap();

        let error = harness
            .coordinator
            .create_online_backup_at(&destination, 1234)
            .await
            .unwrap_err();
        assert!(matches!(error, OnlineBackupError::DestinationExists(_)));
        assert_eq!(fs::read(&destination).unwrap(), b"keep-me");

        harness.handle.shutdown().await;
        harness.apply_task.abort();
    }

    #[tokio::test]
    async fn follower_fails_closed_without_publishing() {
        let db = TempDir::new().unwrap();
        let output = TempDir::new().unwrap();
        let destination = output.path().join("online.nbbk");
        let engine = Arc::new(StorageEngine::open(db.path()).unwrap());
        let clock = Arc::new(HlcClock::new(500));
        let bus = ChannelTransport::new_bus();
        let transport = Arc::new(ChannelTransport::register("follower".to_string(), bus).await);
        let node = RaftNode::new(
            "follower".to_string(),
            vec!["missing".to_string()],
            transport,
        );
        let readiness = node.serving_readiness();
        let (client_tx, shared, handle) = node.spawn();
        let coordinator = OnlineBackupCoordinator::new(client_tx, shared, engine, clock, readiness);

        let error = coordinator
            .create_online_backup_at(&destination, 1234)
            .await
            .unwrap_err();
        assert!(matches!(error, OnlineBackupError::NotLeader { .. }));
        assert!(!destination.exists());
        handle.shutdown().await;
    }

    #[test]
    fn raft_fingerprint_detects_any_durable_frontier_change() {
        let mut before = PersistentState::new();
        before.current_term = 2;
        before.membership = Some(crate::consensus::ClusterMembership::bootstrap(
            "n1".to_string(),
            Vec::<String>::new(),
        ));
        before.append(2, ONLINE_BACKUP_BARRIER.to_vec());

        let same = before.clone();
        assert!(raft_fingerprint_unchanged(&before, &[], &same, &[]));

        let mut changed = before.clone();
        changed.append(2, b"concurrent".to_vec());
        assert!(!raft_fingerprint_unchanged(&before, &[], &changed, &[]));
    }
}
