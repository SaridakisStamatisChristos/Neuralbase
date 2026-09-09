// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;
use std::process::ExitCode;

use neuralbase::offline_backup::{create_offline_backup, verify_backup_file};

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
            reject_unknown_flags(&args[1..], &["--db", "--output"])?;
            let manifest = create_offline_backup(&PathBuf::from(db), &PathBuf::from(output))
                .map_err(|error| error.to_string())?;
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
            reject_unknown_flags(&args[1..], &["--backup"])?;
            let verified = verify_backup_file(&PathBuf::from(backup))
                .map_err(|error| error.to_string())?;
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
        "  neuralbase-backup create --db <rocksdb-path> --output <backup.nbbk>",
        "  neuralbase-backup verify --backup <backup.nbbk>",
        "",
        "create is offline-only: the source RocksDB must not be open by NeuralBase.",
        "the destination is created with no-overwrite atomic publication semantics.",
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
