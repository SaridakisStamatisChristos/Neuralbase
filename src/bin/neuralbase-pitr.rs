// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use neuralbase::backup::MAX_BACKUP_BYTES;
use neuralbase::backup_encryption::{
    load_backup_encryption_key, verify_encrypted_backup_file, MAX_ENCRYPTED_BACKUP_BYTES,
};
use neuralbase::offline_backup::verify_backup_file;
use neuralbase::pitr_archive::{load_pitr_archive_key, PitrArchiveKey, PitrArchiveWriter};
use neuralbase::pitr_replay::{recover_verified_new_cluster, RecoveryTarget};

fn main() -> ExitCode {
    match run(std::env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("neuralbase-pitr: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    let Some(command) = args.first().map(String::as_str) else {
        return Err(usage());
    };
    match command {
        "init" => {
            let backup_path = required_flag(&args[1..], "--backup")?;
            let archive_path = required_flag(&args[1..], "--archive")?;
            let backup_key_file = optional_flag(&args[1..], "--backup-key-file")?;
            let archive_key_file = optional_flag(&args[1..], "--archive-key-file")?;
            reject_unknown_flags(
                &args[1..],
                &[
                    "--backup",
                    "--archive",
                    "--backup-key-file",
                    "--archive-key-file",
                ],
            )?;
            let backup_path = PathBuf::from(backup_path);
            let artifact = read_baseline_artifact(&backup_path)?;
            let backup = load_verified_backup(&backup_path, backup_key_file)?;
            let archive_key = load_archive_key(archive_key_file)?;
            let status = PitrArchiveWriter::initialize(
                &PathBuf::from(archive_path),
                &backup,
                &artifact,
                archive_key,
            )
            .map_err(|error| error.to_string())?;
            println!(
                "PITR archive initialized: timeline={} baseline_index={} baseline_term={} encrypted={} durable_frontier={}",
                hex(&status.metadata.timeline),
                status.metadata.baseline_index,
                status.metadata.baseline_term,
                status.metadata.encrypted,
                status.frontier.index
            );
            Ok(())
        }
        "status" | "verify" | "targets" => {
            let archive_path = required_flag(&args[1..], "--archive")?;
            let archive_key_file = optional_flag(&args[1..], "--archive-key-file")?;
            reject_unknown_flags(&args[1..], &["--archive", "--archive-key-file"])?;
            let writer = open_archive(Path::new(archive_path), archive_key_file)?;
            let status = writer.status();
            if command == "targets" {
                println!(
                    "recovery targets: baseline={} latest={} exact_index_range={}..={} timeline={}",
                    status.metadata.baseline_index,
                    status.frontier.index,
                    status.metadata.baseline_index,
                    status.frontier.index,
                    hex(&status.metadata.timeline),
                );
            } else {
                println!(
                    "PITR archive valid: timeline={} baseline_index={} frontier={} frontier_term={} segments={} encrypted={} parent_timeline={} branch_index={}",
                    hex(&status.metadata.timeline),
                    status.metadata.baseline_index,
                    status.frontier.index,
                    status.frontier.term,
                    status.frontier.segments,
                    status.metadata.encrypted,
                    status
                        .metadata
                        .parent_timeline
                        .as_ref()
                        .map(|id| hex(id))
                        .unwrap_or_else(|| "none".to_string()),
                    status
                        .metadata
                        .branch_index
                        .map(|index| index.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                );
            }
            Ok(())
        }
        "recover" => {
            let backup_path = required_flag(&args[1..], "--backup")?;
            let archive_path = required_flag(&args[1..], "--archive")?;
            let destination = required_flag(&args[1..], "--target-dir")?;
            let node_id = required_flag(&args[1..], "--node-id")?;
            let target = required_flag(&args[1..], "--target")?;
            let backup_key_file = optional_flag(&args[1..], "--backup-key-file")?;
            let archive_key_file = optional_flag(&args[1..], "--archive-key-file")?;
            reject_unknown_flags(
                &args[1..],
                &[
                    "--backup",
                    "--archive",
                    "--target-dir",
                    "--node-id",
                    "--target",
                    "--backup-key-file",
                    "--archive-key-file",
                ],
            )?;
            let recovery_target = parse_target(target)?;
            let backup_path = PathBuf::from(backup_path);
            let artifact = read_baseline_artifact(&backup_path)?;
            let backup = load_verified_backup(&backup_path, backup_key_file)?;
            let archive = open_archive(Path::new(archive_path), archive_key_file)?;
            let report = recover_verified_new_cluster(
                &backup,
                &artifact,
                &archive,
                recovery_target,
                &PathBuf::from(destination),
                node_id,
            )
            .map_err(|error| error.to_string())?;
            println!(
                "PITR recovery complete: source_timeline={} source_baseline={} target_index={} target_term={} replayed_records={} recovery_node_id={} recovery_membership_generation={}",
                hex(&report.source_timeline),
                report.source_baseline_index,
                report.target_index,
                report.target_term,
                report.replayed_records,
                report.recovery_node_id,
                report.recovery_membership_generation,
            );
            Ok(())
        }
        "diagnose" => {
            let archive_path = required_flag(&args[1..], "--archive")?;
            let archive_key_file = optional_flag(&args[1..], "--archive-key-file")?;
            reject_unknown_flags(&args[1..], &["--archive", "--archive-key-file"])?;
            match open_archive(Path::new(archive_path), archive_key_file) {
                Ok(writer) => {
                    let status = writer.status();
                    println!(
                        "archive diagnosis: healthy timeline={} baseline={} frontier={} segments={}",
                        hex(&status.metadata.timeline),
                        status.metadata.baseline_index,
                        status.frontier.index,
                        status.frontier.segments
                    );
                    Ok(())
                }
                Err(error) => Err(format!("archive diagnosis: invalid: {error}")),
            }
        }
        "help" | "--help" | "-h" => {
            println!("{}", usage());
            Ok(())
        }
        _ => Err(usage()),
    }
}

fn open_archive(
    path: &Path,
    archive_key_file: Option<&str>,
) -> Result<PitrArchiveWriter, String> {
    let key = load_archive_key(archive_key_file)?;
    PitrArchiveWriter::open(path, key).map_err(|error| error.to_string())
}

fn load_archive_key(path: Option<&str>) -> Result<Option<PitrArchiveKey>, String> {
    path.map(|path| load_pitr_archive_key(Path::new(path)).map_err(|error| error.to_string()))
        .transpose()
}

fn load_verified_backup(
    path: &Path,
    backup_key_file: Option<&str>,
) -> Result<neuralbase::backup::NeuralBaseBackup, String> {
    match backup_key_file {
        Some(key_path) => {
            let key = load_backup_encryption_key(Path::new(key_path))
                .map_err(|error| error.to_string())?;
            verify_encrypted_backup_file(path, &key).map_err(|error| error.to_string())
        }
        None => verify_backup_file(path).map_err(|error| error.to_string()),
    }
}

fn read_baseline_artifact(path: &Path) -> Result<Vec<u8>, String> {
    let max = MAX_BACKUP_BYTES.max(MAX_ENCRYPTED_BACKUP_BYTES);
    let metadata = std::fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.file_type().is_file() {
        return Err(format!("baseline backup is not a regular file: {}", path.display()));
    }
    if metadata.len() > max as u64 {
        return Err(format!("baseline backup exceeds bounded size: {}", path.display()));
    }
    let file = File::open(path).map_err(|error| error.to_string())?;
    let opened = file.metadata().map_err(|error| error.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.dev() != opened.dev() || metadata.ino() != opened.ino() {
            return Err(format!("baseline backup changed while opening: {}", path.display()));
        }
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(max as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > max {
        return Err(format!("baseline backup exceeds bounded size: {}", path.display()));
    }
    Ok(bytes)
}

fn parse_target(raw: &str) -> Result<RecoveryTarget, String> {
    match raw {
        "baseline" => Ok(RecoveryTarget::Baseline),
        "latest" => Ok(RecoveryTarget::Latest),
        _ => raw
            .parse::<u64>()
            .map(RecoveryTarget::Index)
            .map_err(|_| "--target must be baseline, latest, or an exact u64 recovery index".to_string()),
    }
}

fn required_flag<'a>(args: &'a [String], flag: &str) -> Result<&'a str, String> {
    let mut index = 0;
    while index < args.len() {
        if args[index] == flag {
            let value = args
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            if value.starts_with("--") {
                return Err(format!("missing value for {flag}"));
            }
            return Ok(value);
        }
        index += 1;
    }
    Err(format!("missing required {flag}\n{}", usage()))
}

fn optional_flag<'a>(args: &'a [String], flag: &str) -> Result<Option<&'a str>, String> {
    let mut index = 0;
    while index < args.len() {
        if args[index] == flag {
            let value = args
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            if value.starts_with("--") {
                return Err(format!("missing value for {flag}"));
            }
            return Ok(Some(value));
        }
        index += 1;
    }
    Ok(None)
}

fn reject_unknown_flags(args: &[String], allowed: &[&str]) -> Result<(), String> {
    if args.len() % 2 != 0 {
        return Err(usage());
    }
    let mut index = 0;
    while index < args.len() {
        let flag = args[index].as_str();
        if !allowed.contains(&flag) {
            return Err(format!("unknown argument {flag}\n{}", usage()));
        }
        if args[index + 1].starts_with("--") {
            return Err(format!("missing value for {flag}"));
        }
        index += 2;
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn usage() -> String {
    [
        "usage:",
        "  neuralbase-pitr init --backup <NBBK-or-NBEC> --archive <dir> [--backup-key-file <key>] [--archive-key-file <key>]",
        "  neuralbase-pitr status --archive <dir> [--archive-key-file <key>]",
        "  neuralbase-pitr verify --archive <dir> [--archive-key-file <key>]",
        "  neuralbase-pitr targets --archive <dir> [--archive-key-file <key>]",
        "  neuralbase-pitr recover --backup <NBBK-or-NBEC> --archive <dir> --target <baseline|latest|index> --target-dir <new-db> --node-id <fresh-id> [--backup-key-file <key>] [--archive-key-file <key>]",
        "  neuralbase-pitr diagnose --archive <dir> [--archive-key-file <key>]",
        "",
        "Runtime archival is enabled separately with NEURALBASE_PITR_ARCHIVE_DIR and optional NEURALBASE_PITR_KEY_FILE.",
        "Archive-enabled runtime uses a synchronous durability fence: confirmed Raft apply waits for finalized archive publication.",
        "Timestamp targets are intentionally unsupported in archive v1; use an exact recovery index.",
        "Keys are raw 32-byte out-of-band files and are never accepted directly on argv.",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_parser_is_explicit() {
        assert_eq!(parse_target("baseline").unwrap(), RecoveryTarget::Baseline);
        assert_eq!(parse_target("latest").unwrap(), RecoveryTarget::Latest);
        assert_eq!(parse_target("42").unwrap(), RecoveryTarget::Index(42));
        assert!(parse_target("2026-09-14T20:00:00Z").is_err());
    }

    #[test]
    fn init_requires_backup_and_archive() {
        let error = run(vec!["init".to_string()]).unwrap_err();
        assert!(error.contains("--backup"));
    }

    #[test]
    fn recover_requires_explicit_target() {
        let error = run(vec![
            "recover".into(),
            "--backup".into(),
            "x".into(),
            "--archive".into(),
            "a".into(),
            "--target-dir".into(),
            "db".into(),
            "--node-id".into(),
            "fresh".into(),
        ])
        .unwrap_err();
        assert!(error.contains("--target"));
    }
}