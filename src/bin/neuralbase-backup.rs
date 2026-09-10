// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use neuralbase::backup_encryption::{
    create_encrypted_offline_backup, load_backup_encryption_key, verify_encrypted_backup_file,
};
use neuralbase::offline_backup::{create_offline_backup, verify_backup_file};
use neuralbase::restore::{restore_encrypted_new_cluster, restore_new_cluster};

fn main() -> ExitCode {
    match run(std::env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("neuralbase-backup: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    let Some(command) = args.first().map(String::as_str) else {
        return Err(usage());
    };
    match command {
        "create" => {
            let db = required_flag(&args[1..], "--db")?;
            let output = required_flag(&args[1..], "--output")?;
            let key_file = optional_flag(&args[1..], "--key-file")?;
            reject_unknown_flags(&args[1..], &["--db", "--output", "--key-file"])?;
            let manifest = if let Some(key_file) = key_file {
                let key = load_backup_encryption_key(Path::new(key_file))
                    .map_err(|error| error.to_string())?;
                create_encrypted_offline_backup(&PathBuf::from(db), &PathBuf::from(output), &key)
                    .map_err(|error| error.to_string())?
            } else {
                create_offline_backup(&PathBuf::from(db), &PathBuf::from(output))
                    .map_err(|error| error.to_string())?
            };
            println!(
                "backup created: boundary_index={} boundary_term={} sql_apply_index={} membership_generation={} membership_config_index={} identity_included={} encrypted={}",
                manifest.metadata.last_included_index,
                manifest.metadata.last_included_term,
                manifest.metadata.latest_sql_apply_index,
                manifest.membership_generation,
                manifest.membership_config_index,
                manifest.identity_included,
                manifest.encrypted,
            );
            Ok(())
        }
        "verify" => {
            let backup = required_flag(&args[1..], "--backup")?;
            let key_file = optional_flag(&args[1..], "--key-file")?;
            reject_unknown_flags(&args[1..], &["--backup", "--key-file"])?;
            let verified = if let Some(key_file) = key_file {
                let key = load_backup_encryption_key(Path::new(key_file))
                    .map_err(|error| error.to_string())?;
                verify_encrypted_backup_file(&PathBuf::from(backup), &key)
                    .map_err(|error| error.to_string())?
            } else {
                verify_backup_file(&PathBuf::from(backup)).map_err(|error| error.to_string())?
            };
            println!(
                "backup valid: format={} boundary_index={} boundary_term={} sql_apply_index={} membership_generation={} membership_config_index={} recovery={:?} identity_included={} encrypted={}",
                verified.manifest.state_machine_compat_version,
                verified.manifest.metadata.last_included_index,
                verified.manifest.metadata.last_included_term,
                verified.manifest.metadata.latest_sql_apply_index,
                verified.manifest.membership_generation,
                verified.manifest.membership_config_index,
                verified.manifest.recovery_semantics,
                verified.manifest.identity_included,
                verified.manifest.encrypted,
            );
            Ok(())
        }
        "restore" => {
            let backup = required_flag(&args[1..], "--backup")?;
            let target = required_flag(&args[1..], "--target")?;
            let node_id = required_flag(&args[1..], "--node-id")?;
            let key_file = optional_flag(&args[1..], "--key-file")?;
            reject_unknown_flags(
                &args[1..],
                &["--backup", "--target", "--node-id", "--key-file"],
            )?;
            let report = if let Some(key_file) = key_file {
                let key = load_backup_encryption_key(Path::new(key_file))
                    .map_err(|error| error.to_string())?;
                restore_encrypted_new_cluster(
                    &PathBuf::from(backup),
                    &key,
                    &PathBuf::from(target),
                    node_id,
                )
                .map_err(|error| error.to_string())?
            } else {
                restore_new_cluster(&PathBuf::from(backup), &PathBuf::from(target), node_id)
                    .map_err(|error| error.to_string())?
            };
            println!(
                "restore complete: boundary_index={} boundary_term={} recovery_node_id={} recovery_membership_generation={} source_membership_generation={}",
                report.boundary_index,
                report.boundary_term,
                report.recovery_node_id,
                report.recovery_membership_generation,
                report.source_manifest.membership_generation,
            );
            Ok(())
        }
        "help" | "--help" | "-h" => {
            println!("{}", usage());
            Ok(())
        }
        _ => Err(usage()),
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

fn usage() -> String {
    [
        "usage:",
        "  neuralbase-backup create --db <rocksdb-path> --output <backup> [--key-file <raw-32-byte-key>]",
        "  neuralbase-backup verify --backup <backup> [--key-file <raw-32-byte-key>]",
        "  neuralbase-backup restore --backup <backup> --target <new-rocksdb-path> --node-id <fresh-node-id> [--key-file <raw-32-byte-key>]",
        "",
        "create is offline-only: the source RocksDB must not be open by NeuralBase.",
        "--key-file selects authenticated NBEC v1 encryption; key bytes are never accepted on argv.",
        "without --key-file, create/verify/restore use the plaintext NBBK v1 format.",
        "restore creates a fresh recovery cluster and refuses an existing target directory.",
        "the restore node id must not appear anywhere in the source membership history.",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_requires_explicit_paths() {
        let error = run(vec!["create".to_string()]).unwrap_err();
        assert!(error.contains("--db"));
        let error = run(vec!["verify".to_string()]).unwrap_err();
        assert!(error.contains("--backup"));
        let error = run(vec!["restore".to_string()]).unwrap_err();
        assert!(error.contains("--backup"));
    }

    #[test]
    fn cli_restore_requires_fresh_node_id_argument() {
        let error = run(vec![
            "restore".to_string(),
            "--backup".to_string(),
            "x.nbbk".to_string(),
            "--target".to_string(),
            "db".to_string(),
        ])
        .unwrap_err();
        assert!(error.contains("--node-id"));
    }

    #[test]
    fn optional_key_file_requires_a_value() {
        let args = vec!["--key-file".to_string()];
        let error = optional_flag(&args, "--key-file").unwrap_err();
        assert!(error.contains("missing value for --key-file"));
    }

    #[test]
    fn optional_key_file_returns_the_path_without_reading_key_material() {
        let args = vec!["--key-file".to_string(), "backup.key".to_string()];
        assert_eq!(
            optional_flag(&args, "--key-file").unwrap(),
            Some("backup.key")
        );
    }

    #[test]
    fn cli_rejects_unknown_arguments() {
        let error = run(vec![
            "verify".to_string(),
            "--backup".to_string(),
            "x".to_string(),
            "--repair".to_string(),
            "yes".to_string(),
        ])
        .unwrap_err();
        assert!(error.contains("unknown argument --repair"));
    }
}
