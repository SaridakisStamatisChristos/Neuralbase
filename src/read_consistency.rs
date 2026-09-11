// SPDX-License-Identifier: Apache-2.0
//! Explicit client-visible read-consistency contract.
//!
//! Phase 6 deliberately keeps the historical local read behavior as the
//! default. Stronger modes are opt-in and have distinct semantics:
//! - `Local`: read locally applied state without consensus coordination.
//! - `Leader`: require current leader authority, but do not advertise a
//!   linearizable apply frontier.
//! - `Linearizable`: require a consensus read frontier and wait until that
//!   frontier is locally applied before executing the SQL read.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version of the public/internal Phase-6 consistency contract.
pub const READ_CONSISTENCY_CONTRACT_VERSION: u8 = 1;

/// Per-session consistency requested for SQL reads.
///
/// `Local` is intentionally the default for backward compatibility with the
/// pre-Phase-6 server. Selecting a stronger mode must never silently degrade to
/// `Local`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
    #[default]
    Local,
    Leader,
    Linearizable,
}

impl ReadConsistency {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Leader => "leader",
            Self::Linearizable => "linearizable",
        }
    }

    pub const fn requires_consensus(self) -> bool {
        !matches!(self, Self::Local)
    }

    pub const fn requires_apply_frontier(self) -> bool {
        matches!(self, Self::Linearizable)
    }
}

impl fmt::Display for ReadConsistency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("unsupported read consistency mode '{value}'; expected local, leader, or linearizable")]
pub struct ParseReadConsistencyError {
    value: String,
}

impl FromStr for ReadConsistency {
    type Err = ParseReadConsistencyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "local" | "stale" => Ok(Self::Local),
            "leader" | "leader-authoritative" | "leader_authoritative" => Ok(Self::Leader),
            "linearizable" => Ok(Self::Linearizable),
            _ => Err(ParseReadConsistencyError {
                value: value.trim().to_string(),
            }),
        }
    }
}

/// Result of recognizing the NeuralBase per-session `SET` surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadConsistencySetting {
    /// SQL is not a NeuralBase read-consistency setting.
    NotSetting,
    /// A valid setting selecting the supplied mode.
    Set(ReadConsistency),
    /// The statement targeted this setting but was malformed or unsupported.
    Invalid(String),
}

/// Parse the Phase-6 session setting without handing it to the general SQL
/// parser. Accepted forms are intentionally small and deterministic:
///
/// ```text
/// SET neuralbase_read_consistency = 'linearizable'
/// SET neuralbase_read_consistency TO leader
/// SET neuralbase.read_consistency = local
/// ```
///
/// The parser rejects extra statements/trailing tokens instead of accidentally
/// accepting an ambiguous downgrade or compound command.
pub fn parse_read_consistency_setting(sql: &str) -> ReadConsistencySetting {
    let mut statement = sql.trim();
    if statement.is_empty() {
        return ReadConsistencySetting::NotSetting;
    }

    if let Some(without_semicolon) = statement.strip_suffix(';') {
        statement = without_semicolon.trim_end();
    }
    if statement.contains(';') {
        return if starts_with_setting_name(statement) {
            ReadConsistencySetting::Invalid(
                "read consistency SET must contain exactly one statement".to_string(),
            )
        } else {
            ReadConsistencySetting::NotSetting
        };
    }

    let Some(after_set) = strip_keyword(statement, "set") else {
        return ReadConsistencySetting::NotSetting;
    };
    let after_set = after_set.trim_start();

    let (after_name, targets_setting) = strip_setting_name(after_set);
    if !targets_setting {
        return ReadConsistencySetting::NotSetting;
    }
    let mut rest = after_name.trim_start();

    if let Some(after_equals) = rest.strip_prefix('=') {
        rest = after_equals.trim_start();
    } else if let Some(after_to) = strip_keyword(rest, "to") {
        rest = after_to.trim_start();
    } else {
        return ReadConsistencySetting::Invalid(
            "expected '=' or TO after neuralbase read-consistency setting".to_string(),
        );
    }

    if rest.is_empty() {
        return ReadConsistencySetting::Invalid("read consistency mode is missing".to_string());
    }

    let value = if let Some(quoted) = rest.strip_prefix('\'') {
        let Some(end) = quoted.find('\'') else {
            return ReadConsistencySetting::Invalid(
                "unterminated read consistency mode literal".to_string(),
            );
        };
        if !quoted[end + 1..].trim().is_empty() {
            return ReadConsistencySetting::Invalid(
                "unexpected trailing tokens after read consistency mode".to_string(),
            );
        }
        &quoted[..end]
    } else {
        if rest.split_whitespace().count() != 1 {
            return ReadConsistencySetting::Invalid(
                "unexpected trailing tokens after read consistency mode".to_string(),
            );
        }
        rest
    };

    match value.parse::<ReadConsistency>() {
        Ok(mode) => ReadConsistencySetting::Set(mode),
        Err(error) => ReadConsistencySetting::Invalid(error.to_string()),
    }
}

fn starts_with_setting_name(sql: &str) -> bool {
    let Some(after_set) = strip_keyword(sql.trim_start(), "set") else {
        return false;
    };
    strip_setting_name(after_set.trim_start()).1
}

fn strip_setting_name(input: &str) -> (&str, bool) {
    for name in ["neuralbase_read_consistency", "neuralbase.read_consistency"] {
        if input.len() >= name.len() && input[..name.len()].eq_ignore_ascii_case(name) {
            let rest = &input[name.len()..];
            if rest
                .chars()
                .next()
                .is_none_or(|c| c.is_whitespace() || c == '=')
            {
                return (rest, true);
            }
        }
    }
    (input, false)
}

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    if input.len() < keyword.len() || !input[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &input[keyword.len()..];
    if rest.chars().next().is_some_and(|c| !c.is_whitespace()) {
        return None;
    }
    Some(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_is_backward_compatible_default() {
        assert_eq!(ReadConsistency::default(), ReadConsistency::Local);
        assert!(!ReadConsistency::Local.requires_consensus());
        assert!(ReadConsistency::Leader.requires_consensus());
        assert!(ReadConsistency::Linearizable.requires_apply_frontier());
    }

    #[test]
    fn modes_parse_case_insensitively_with_documented_aliases() {
        assert_eq!("LOCAL".parse(), Ok(ReadConsistency::Local));
        assert_eq!("stale".parse(), Ok(ReadConsistency::Local));
        assert_eq!("leader-authoritative".parse(), Ok(ReadConsistency::Leader));
        assert_eq!("Linearizable".parse(), Ok(ReadConsistency::Linearizable));
        assert!("serializable".parse::<ReadConsistency>().is_err());
    }

    #[test]
    fn session_set_surface_is_explicit_and_strict() {
        assert_eq!(
            parse_read_consistency_setting(
                "SET neuralbase_read_consistency = 'linearizable';"
            ),
            ReadConsistencySetting::Set(ReadConsistency::Linearizable)
        );
        assert_eq!(
            parse_read_consistency_setting("set neuralbase.read_consistency TO leader"),
            ReadConsistencySetting::Set(ReadConsistency::Leader)
        );
        assert_eq!(
            parse_read_consistency_setting("SET neuralbase_read_consistency = stale"),
            ReadConsistencySetting::Set(ReadConsistency::Local)
        );
        assert_eq!(
            parse_read_consistency_setting("SET work_mem = '8MB'"),
            ReadConsistencySetting::NotSetting
        );
    }

    #[test]
    fn malformed_targeted_settings_fail_instead_of_downgrading() {
        for sql in [
            "SET neuralbase_read_consistency",
            "SET neuralbase_read_consistency =",
            "SET neuralbase_read_consistency = eventual",
            "SET neuralbase_read_consistency = local extra",
            "SET neuralbase_read_consistency = local; SELECT 1",
            "SET neuralbase_read_consistency = 'leader",
        ] {
            assert!(
                matches!(
                    parse_read_consistency_setting(sql),
                    ReadConsistencySetting::Invalid(_)
                ),
                "expected targeted setting to fail closed: {sql}"
            );
        }
    }
}
