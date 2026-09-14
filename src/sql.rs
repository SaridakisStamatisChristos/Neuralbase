use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlParseError {
    #[error("empty SQL input")]
    Empty,
    #[error("multiple SQL statements in one request are unsupported (got {0})")]
    MultipleStatements(usize),
    #[error("{0}")]
    Parser(String),
}

pub fn parse_statement(sql: &str) -> Result<Statement, SqlParseError> {
    if sql.trim().is_empty() {
        return Err(SqlParseError::Empty);
    }

    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql).map_err(|err| SqlParseError::Parser(err.to_string()))?;

    match statements.len() {
        0 => Err(SqlParseError::Empty),
        1 => Ok(statements.remove(0)),
        count => Err(SqlParseError::MultipleStatements(count)),
    }
}

// ── NeuralBase-extended statement type ────────────────────────────────────────

/// A statement parsed by NeuralBase's extended parser.
///
/// Covers standard SQL (via sqlparser-rs) plus NeuralBase-specific user
/// management commands that sqlparser does not handle natively.
#[derive(Debug, Clone, PartialEq)]
pub enum NbStatement {
    /// Standard SQL — forwarded to the sqlparser AST binder.
    Sql(Box<Statement>),
    /// CREATE USER name WITH PASSWORD 'password'
    CreateUser { username: String, password: String },
    /// ALTER USER name WITH PASSWORD 'new_password'
    AlterUser {
        username: String,
        new_password: String,
    },
    /// DROP USER [IF EXISTS] name
    DropUser { username: String, if_exists: bool },
}

/// Parse SQL, handling NeuralBase user-management extensions before delegating
/// to sqlparser for everything else.
pub fn parse_nb_statement(sql: &str) -> Result<NbStatement, SqlParseError> {
    let trimmed = sql.trim();
    let lower = trimmed.to_lowercase();

    if lower.starts_with("create user")
        || lower.starts_with("alter user")
        || lower.starts_with("drop user")
    {
        reject_compound_extension_sql(trimmed)?;
    }

    if lower.starts_with("create user") {
        return parse_create_user(trimmed);
    }
    if lower.starts_with("alter user") {
        return parse_alter_user(trimmed);
    }
    if lower.starts_with("drop user") {
        return parse_drop_user(trimmed);
    }

    parse_statement(sql).map(|s| NbStatement::Sql(Box::new(s)))
}

/// NeuralBase user-management syntax is parsed outside sqlparser-rs, so enforce
/// the same one-statement-per-request contract explicitly. Semicolons inside a
/// quoted password and one optional trailing terminator remain valid.
fn reject_compound_extension_sql(sql: &str) -> Result<(), SqlParseError> {
    let bytes = sql.as_bytes();
    let mut index = 0usize;
    let mut single_quote = false;

    while index < bytes.len() {
        match bytes[index] {
            b'\'' if single_quote => {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 2;
                    continue;
                }
                single_quote = false;
            }
            b'\'' => single_quote = true,
            b';' if !single_quote => {
                if !sql[index + 1..].trim().is_empty() {
                    return Err(SqlParseError::MultipleStatements(2));
                }
            }
            _ => {}
        }
        index += 1;
    }
    Ok(())
}

fn parse_create_user(sql: &str) -> Result<NbStatement, SqlParseError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    // tokens[0]="CREATE"  tokens[1]="USER"  tokens[2]=<name> ...
    if tokens.len() < 3 {
        return Err(SqlParseError::Parser(
            "CREATE USER requires a username".to_string(),
        ));
    }
    let username = tokens[2].to_string();
    let password = extract_password_literal(sql)?;
    Ok(NbStatement::CreateUser { username, password })
}

fn parse_alter_user(sql: &str) -> Result<NbStatement, SqlParseError> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 3 {
        return Err(SqlParseError::Parser(
            "ALTER USER requires a username".to_string(),
        ));
    }
    let username = tokens[2].to_string();
    let new_password = extract_password_literal(sql)?;
    Ok(NbStatement::AlterUser {
        username,
        new_password,
    })
}

fn parse_drop_user(sql: &str) -> Result<NbStatement, SqlParseError> {
    let lower = sql.to_lowercase();
    let if_exists = lower.contains("if exists");
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    let username = if if_exists {
        // DROP USER IF EXISTS <name>
        tokens.get(4).copied().unwrap_or("").to_string()
    } else {
        // DROP USER <name>
        tokens.get(2).copied().unwrap_or("").to_string()
    };
    if username.is_empty() {
        return Err(SqlParseError::Parser(
            "DROP USER requires a username".to_string(),
        ));
    }
    Ok(NbStatement::DropUser {
        username,
        if_exists,
    })
}

/// Extract the value of a `PASSWORD 'literal'` clause (case-insensitive).
fn extract_password_literal(sql: &str) -> Result<String, SqlParseError> {
    let lower = sql.to_lowercase();
    let pos = lower
        .find("password")
        .ok_or_else(|| SqlParseError::Parser("missing PASSWORD clause".to_string()))?;
    let after = sql[pos + "password".len()..].trim_start();
    // Skip optional "WITH" keyword before the literal.
    let after = if after.to_uppercase().starts_with("WITH") {
        after["with".len()..].trim_start()
    } else {
        after
    };
    if !after.starts_with('\'') {
        return Err(SqlParseError::Parser(
            "password must be a single-quoted string literal".to_string(),
        ));
    }
    let end = after[1..]
        .find('\'')
        .ok_or_else(|| SqlParseError::Parser("unterminated password literal".to_string()))?;
    Ok(after[1..1 + end].to_string())
}
