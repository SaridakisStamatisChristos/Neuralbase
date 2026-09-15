from pathlib import Path


def replace(path: str, old: str, new: str, count: int = 1) -> None:
    p = Path(path)
    text = p.read_text()
    if text.count(old) < count:
        raise SystemExit(f"anchor not found in {path}: {old[:100]!r}")
    p.write_text(text.replace(old, new, count))


def insert_before_final_brace(path: str, addition: str) -> None:
    p = Path(path)
    text = p.read_text()
    pos = text.rfind("}\n")
    if pos < 0:
        raise SystemExit(f"no final brace in {path}")
    p.write_text(text[:pos] + addition + text[pos:])


# Canonical membership decoding and checksum-valid format fault evidence.
replace(
    "src/pitr.rs",
    "serde_json::from_slice(&bytes[MEMBERSHIP_CHANGE_TAG.len()..])\n        .map_err(|error| ArchiveCodecError::InvalidMembership(error.to_string()))",
    "let change: MembershipChange = serde_json::from_slice(&bytes[MEMBERSHIP_CHANGE_TAG.len()..])\n        .map_err(|error| ArchiveCodecError::InvalidMembership(error.to_string()))?;\n    if crate::consensus::encode_membership_change(&change).as_slice() != bytes {\n        return Err(ArchiveCodecError::NonCanonicalMembership);\n    }\n    Ok(change)",
)
replace(
    "src/pitr.rs",
    '    #[error("invalid archived membership mutation: {0}")]\n    InvalidMembership(String),\n',
    '    #[error("invalid archived membership mutation: {0}")]\n    InvalidMembership(String),\n    #[error("archived membership mutation is not canonically encoded")]\n    NonCanonicalMembership,\n',
)
replace(
    "src/pitr.rs",
    "    fn backup_hash() -> ArchiveHash {\n        [9u8; ARCHIVE_HASH_BYTES]\n    }\n",
    "    fn backup_hash() -> ArchiveHash {\n        [9u8; ARCHIVE_HASH_BYTES]\n    }\n\n    fn reseal_segment(bytes: &mut [u8]) {\n        let content_len = bytes.len() - CHECKSUM_BYTES;\n        let checksum: ArchiveHash = Sha256::digest(&bytes[..content_len]).into();\n        bytes[content_len..].copy_from_slice(&checksum);\n    }\n",
)
replace(
    "src/pitr.rs",
    "    #[test]\n    fn truncation_and_corruption_fail_closed() {\n",
    '''    #[test]
    fn version_compatibility_and_extra_bytes_fail_with_valid_checksum() {
        let record = RecoveryRecord::from_log_entry(&sql_entry(11)).unwrap();
        let segment =
            ArchiveSegment::new(timeline(), 1234, spec().baseline_anchor().unwrap(), record)
                .unwrap();
        let bytes = segment.encode().unwrap();

        let mut wrong_version = bytes.clone();
        wrong_version[4] = ARCHIVE_FORMAT_VERSION + 1;
        reseal_segment(&mut wrong_version);
        assert_eq!(
            ArchiveSegment::decode(&wrong_version).unwrap_err(),
            ArchiveCodecError::UnsupportedVersion(ARCHIVE_FORMAT_VERSION + 1)
        );

        let mut wrong_state_machine = bytes.clone();
        let incompatible = ARCHIVE_STATE_MACHINE_COMPAT_VERSION + 1;
        wrong_state_machine[8..10].copy_from_slice(&incompatible.to_be_bytes());
        reseal_segment(&mut wrong_state_machine);
        assert_eq!(
            ArchiveSegment::decode(&wrong_state_machine).unwrap_err(),
            ArchiveCodecError::UnsupportedStateMachineVersion(incompatible)
        );

        let mut trailing = bytes;
        let checksum_offset = trailing.len() - CHECKSUM_BYTES;
        trailing.insert(checksum_offset, 0xA5);
        reseal_segment(&mut trailing);
        assert!(matches!(
            ArchiveSegment::decode(&trailing),
            Err(ArchiveCodecError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn membership_payload_must_be_canonical() {
        let canonical = encode_membership_change(&MembershipChange::AddLearner("n4".into()));
        let mut noncanonical = canonical;
        noncanonical.insert(MEMBERSHIP_CHANGE_TAG.len(), b' ');
        let error = RecoveryRecord::from_log_entry(&LogEntry {
            term: 4,
            index: 11,
            command: noncanonical,
        })
        .unwrap_err();
        assert_eq!(error, ArchiveCodecError::NonCanonicalMembership);
    }

    #[test]
    fn truncation_and_corruption_fail_closed() {
''',
)

# Archive writer: hard bounded scans and logical duplicate convergence.
replace(
    "src/pitr_archive.rs",
    "const MAX_METADATA_BYTES: usize = 64 * 1024;\n",
    "const MAX_METADATA_BYTES: usize = 64 * 1024;\npub const MAX_ARCHIVE_SEGMENT_FILES: usize = 1_000_000;\n",
)
replace(
    "src/pitr_archive.rs",
    "self.publish_segment(&segment)?;",
    "let published_hash = self.publish_segment(&segment)?;",
)
replace(
    "src/pitr_archive.rs",
    "segment_hash: segment.hash()?,",
    "segment_hash: published_hash,",
)

p = Path("src/pitr_archive.rs")
text = p.read_text()
start = text.find("    fn publish_segment(&self, segment: &ArchiveSegment) -> Result<(), PitrArchiveError> {")
end = text.find("    fn read_segment_path(&self, path: &Path) -> Result<ArchiveSegment, PitrArchiveError> {", start)
if start < 0 or end < 0:
    raise SystemExit("publish_segment block not found")
publish = '''    fn publish_segment(&self, segment: &ArchiveSegment) -> Result<ArchiveHash, PitrArchiveError> {
        let plaintext = segment.encode()?;
        let bytes = match (&self.key, self.metadata.encrypted) {
            (Some(key), true) => encrypt_segment(&plaintext, self.metadata.timeline, key)?,
            (None, false) => plaintext,
            _ => return Err(PitrArchiveError::InvalidKeyConfiguration),
        };
        let final_path = segment_path(&self.root, segment.record.index, self.metadata.encrypted);
        if final_path.exists() {
            let existing = self.read_segment_path(&final_path)?;
            if same_logical_segment(&existing, segment) {
                return Ok(existing.hash()?);
            }
            return Err(PitrArchiveError::ConflictingDuplicate(segment.record.index));
        }
        let staging_path = self.root.join(STAGING_DIR).join(format!(
            ".{:020}.partial-{}-{}",
            segment.record.index,
            std::process::id(),
            segment.created_unix_ms
        ));
        let result = (|| -> Result<ArchiveHash, PitrArchiveError> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&staging_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);

            let staged = self.read_segment_path(&staging_path)?;
            if staged != *segment {
                return Err(PitrArchiveError::StagedVerificationMismatch);
            }
            let published_hash = match fs::hard_link(&staging_path, &final_path) {
                Ok(()) => segment.hash()?,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let existing = self.read_segment_path(&final_path)?;
                    if same_logical_segment(&existing, segment) {
                        existing.hash()?
                    } else {
                        return Err(PitrArchiveError::ConflictingDuplicate(segment.record.index));
                    }
                }
                Err(error) => return Err(error.into()),
            };
            sync_dir(&self.root.join(SEGMENTS_DIR))?;
            fs::remove_file(&staging_path)?;
            sync_dir(&self.root.join(STAGING_DIR))?;
            Ok(published_hash)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&staging_path);
        }
        result
    }

'''
p.write_text(text[:start] + publish + text[end:])

replace(
    "src/pitr_archive.rs",
    "fn list_segment_files(\n    root: &Path,\n    encrypted: bool,\n) -> Result<Vec<(u64, PathBuf)>, PitrArchiveError> {\n",
    '''fn same_logical_segment(left: &ArchiveSegment, right: &ArchiveSegment) -> bool {
    left.timeline == right.timeline
        && left.state_machine_compat_version == right.state_machine_compat_version
        && left.previous_hash == right.previous_hash
        && left.record == right.record
}

fn list_segment_files(
    root: &Path,
    encrypted: bool,
) -> Result<Vec<(u64, PathBuf)>, PitrArchiveError> {
    list_segment_files_with_limit(root, encrypted, MAX_ARCHIVE_SEGMENT_FILES)
}

fn list_segment_files_with_limit(
    root: &Path,
    encrypted: bool,
    limit: usize,
) -> Result<Vec<(u64, PathBuf)>, PitrArchiveError> {
''',
)
replace(
    "src/pitr_archive.rs",
    "        out.push((index, entry.path()));",
    "        if out.len() >= limit {\n            return Err(PitrArchiveError::TooManySegments { limit });\n        }\n        out.push((index, entry.path()));",
)
replace(
    "src/pitr_archive.rs",
    '    #[error("unexpected entry in archive directory: {0}")]\n    UnexpectedArchiveEntry(PathBuf),\n',
    '    #[error("unexpected entry in archive directory: {0}")]\n    UnexpectedArchiveEntry(PathBuf),\n    #[error("archive contains more than the bounded limit of {limit} segment files")]\n    TooManySegments { limit: usize },\n',
)
insert_before_final_brace(
    "src/pitr_archive.rs",
    '''
    #[test]
    fn independent_writers_converge_on_identical_logical_segment() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = backup();
        let baseline = backup.encode().unwrap();
        PitrArchiveWriter::initialize(&root, &backup, &baseline, None).unwrap();
        let mut first = PitrArchiveWriter::open(&root, None).unwrap();
        let mut second = PitrArchiveWriter::open(&root, None).unwrap();
        first.append_committed(&entry(6)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        second.append_committed(&entry(6)).unwrap();
        assert_eq!(first.status().frontier.segment_hash, second.status().frontier.segment_hash);
        assert_eq!(second.status().frontier.index, 6);
    }

    #[test]
    fn restart_cleans_staging_and_rejects_corrupt_finalized_segment() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = backup();
        PitrArchiveWriter::initialize(&root, &backup, &backup.encode().unwrap(), None).unwrap();
        let staged = root.join(STAGING_DIR).join("leftover.partial");
        fs::write(&staged, b"interrupted publication").unwrap();
        let mut writer = PitrArchiveWriter::open(&root, None).unwrap();
        assert!(!staged.exists());
        writer.append_committed(&entry(6)).unwrap();
        drop(writer);

        let path = segment_path(&root, 6, false);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            PitrArchiveWriter::open(&root, None),
            Err(PitrArchiveError::Codec(ArchiveCodecError::ChecksumMismatch))
        ));
    }

    #[test]
    fn segment_directory_collection_is_hard_bounded() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = backup();
        PitrArchiveWriter::initialize(&root, &backup, &backup.encode().unwrap(), None).unwrap();
        fs::write(segment_path(&root, 6, false), b"one").unwrap();
        fs::write(segment_path(&root, 7, false), b"two").unwrap();
        assert!(matches!(
            list_segment_files_with_limit(&root, false, 1),
            Err(PitrArchiveError::TooManySegments { limit: 1 })
        ));
    }
''',
)

# Runtime observability around startup fences and publication.
replace(
    "src/pitr_runtime.rs",
    '        let writer = PitrArchiveWriter::open(Path::new(&archive_dir), key)\n            .map_err(|error| io::Error::other(format!("open PITR archive stream: {error}")))?;',
    '''        let writer = PitrArchiveWriter::open(Path::new(&archive_dir), key).map_err(|error| {
            metrics::counter!("neuralbase_pitr_startup_rejections_total", "reason" => "archive_open")
                .increment(1);
            io::Error::other(format!("open PITR archive stream: {error}"))
        })?;''',
)
replace(
    "src/pitr_runtime.rs",
    "        if status.frontier.segments > max_segments {\n            return Err(io::Error::other(format!(",
    '        if status.frontier.segments > max_segments {\n            metrics::counter!("neuralbase_pitr_startup_rejections_total", "reason" => "segment_limit").increment(1);\n            return Err(io::Error::other(format!(',
)
replace(
    "src/pitr_runtime.rs",
    "        if status.metadata.baseline_index > durable.last_applied_index {\n            return Err(io::Error::other(format!(",
    '        if status.metadata.baseline_index > durable.last_applied_index {\n            metrics::counter!("neuralbase_pitr_startup_rejections_total", "reason" => "baseline_ahead").increment(1);\n            return Err(io::Error::other(format!(',
)
replace(
    "src/pitr_runtime.rs",
    "        if status.frontier.index > durable.last_applied_index {\n            return Err(io::Error::other(format!(",
    '        if status.frontier.index > durable.last_applied_index {\n            metrics::counter!("neuralbase_pitr_startup_rejections_total", "reason" => "frontier_ahead").increment(1);\n            return Err(io::Error::other(format!(',
)
replace(
    "src/pitr_runtime.rs",
    "            if persistent.snapshot_index > status.frontier.index {\n                return Err(io::Error::other(format!(",
    '            if persistent.snapshot_index > status.frontier.index {\n                metrics::counter!("neuralbase_pitr_startup_rejections_total", "reason" => "compaction_ahead").increment(1);\n                return Err(io::Error::other(format!(',
)
replace(
    "src/pitr_runtime.rs",
    "        Ok(Some(Self {\n            writer,\n            max_segments,\n        }))",
    '''        metrics::gauge!("neuralbase_pitr_archive_frontier_index")
            .set(status.frontier.index as f64);
        metrics::gauge!("neuralbase_pitr_archive_segment_limit").set(max_segments as f64);
        Ok(Some(Self {
            writer,
            max_segments,
        }))''',
)
replace(
    "src/pitr_runtime.rs",
    "        if entry.index > status.frontier.index && status.frontier.segments >= self.max_segments {\n            return Err(format!(",
    '        if entry.index > status.frontier.index && status.frontier.segments >= self.max_segments {\n            metrics::counter!("neuralbase_pitr_archive_failures_total", "reason" => "segment_limit").increment(1);\n            return Err(format!(',
)
replace(
    "src/pitr_runtime.rs",
    '''        self.writer
            .append_committed(entry)
            .map(|_| ())
            .map_err(|error| {
                format!(
                    "PITR archive publication failed at index {}: {error}",
                    entry.index
                )
            })''',
    '''        match self.writer.append_committed(entry) {
            Ok(_) => {
                metrics::counter!("neuralbase_pitr_archive_appends_total").increment(1);
                metrics::gauge!("neuralbase_pitr_archive_frontier_index")
                    .set(self.writer.status().frontier.index as f64);
                Ok(())
            }
            Err(error) => {
                metrics::counter!("neuralbase_pitr_archive_failures_total", "reason" => "publication")
                    .increment(1);
                Err(format!(
                    "PITR archive publication failed at index {}: {error}",
                    entry.index
                ))
            }
        }''',
)

# Child timeline must reject a copied parent-future segment.
insert_before_final_brace(
    "src/pitr_branch.rs",
    '''
    #[test]
    fn branch_rejects_injected_parent_future_segment() {
        let temp = TempDir::new().unwrap();
        let parent_root = temp.path().join("parent");
        let parent_backup = backup(5, 2, "old", 2);
        let parent_bytes = parent_backup.encode().unwrap();
        PitrArchiveWriter::initialize(&parent_root, &parent_backup, &parent_bytes, None).unwrap();
        let mut parent = PitrArchiveWriter::open(&parent_root, None).unwrap();
        for index in [6, 7] {
            parent
                .append_committed(&LogEntry {
                    term: 3,
                    index,
                    command: vec![],
                })
                .unwrap();
        }

        let child_backup = backup(6, 3, "fresh", 4);
        let child_bytes = child_backup.encode().unwrap();
        let child_root = temp.path().join("child");
        initialize_branch_stream(&child_root, &parent, 6, &child_backup, &child_bytes, None)
            .unwrap();

        let name = format!("{:020}.nbar", 7);
        fs::copy(
            parent_root.join(SEGMENTS_DIR).join(&name),
            child_root.join(SEGMENTS_DIR).join(&name),
        )
        .unwrap();
        assert!(matches!(
            PitrArchiveWriter::open(&child_root, None),
            Err(PitrArchiveError::WrongTimeline(7))
        ));
    }
''',
)

# Encrypted replay and wrong-baseline evidence.
replace(
    "src/pitr_replay.rs",
    "    use crate::pitr_archive::PitrArchiveWriter;",
    "    use crate::pitr_archive::{PitrArchiveKey, PitrArchiveWriter};",
)
insert_before_final_brace(
    "src/pitr_replay.rs",
    '''
    #[test]
    fn encrypted_archive_replays_end_to_end() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = baseline();
        let bytes = backup.encode().unwrap();
        PitrArchiveWriter::initialize(
            &root,
            &backup,
            &bytes,
            Some(PitrArchiveKey::from_bytes([7u8; 32])),
        )
        .unwrap();
        let mut archive = PitrArchiveWriter::open(
            &root,
            Some(PitrArchiveKey::from_bytes([7u8; 32])),
        )
        .unwrap();
        archive
            .append_committed(&crate::consensus::LogEntry {
                term: 3,
                index: 6,
                command: vec![],
            })
            .unwrap();
        let target = temp.path().join("encrypted-recovered");
        let report = recover_verified_new_cluster(
            &backup,
            &bytes,
            &archive,
            RecoveryTarget::Latest,
            &target,
            "fresh-encrypted",
        )
        .unwrap();
        assert_eq!(report.target_index, 6);
        assert_eq!(report.replayed_records, 1);
        assert!(target.is_dir());
    }

    #[test]
    fn wrong_baseline_artifact_fails_before_publication() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("archive");
        let backup = baseline();
        let bytes = backup.encode().unwrap();
        PitrArchiveWriter::initialize(&root, &backup, &bytes, None).unwrap();
        let archive = PitrArchiveWriter::open(&root, None).unwrap();
        let target = temp.path().join("wrong-baseline-target");
        let mut wrong = bytes;
        wrong.push(0);
        assert!(matches!(
            recover_verified_new_cluster(
                &backup,
                &wrong,
                &archive,
                RecoveryTarget::Baseline,
                &target,
                "fresh-baseline-check"
            ),
            Err(PitrReplayError::BaselineHashMismatch)
        ));
        assert!(!target.exists());
    }
''',
)
