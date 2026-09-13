// SPDX-License-Identifier: Apache-2.0
// Shared PostgreSQL 16 semantic-reference harness for Phase 8 tests.

use neuralbase::query_executor::{execute_select_query, QueryCatalog, QueryError, ScalarVal};
use neuralbase::sql::parse_statement;
use postgres::{Client, NoTls, Row};
use sqlparser::ast::Statement;
use std::net::TcpStream;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const PG16_CONTAINER: &str = "neuralbase-pg16-ref";
const PG16_PORT: u16 = 15432;

#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalValue {
    Integer(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalResult {
    pub rows: Vec<Vec<CanonicalValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalError {
    pub sqlstate: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalOutcome {
    Rows(CanonicalResult),
    Error(CanonicalError),
}

#[derive(Debug, Clone, Copy)]
pub struct DifferentialCase {
    pub name: &'static str,
    pub sql: &'static str,
    /// `true` when SQL order is semantically significant. Unordered cases are
    /// normalized deterministically before comparison.
    pub ordered: bool,
}

fn pg_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn ensure_pg16_running() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let inspect = Command::new("docker")
            .args(["inspect", "-f", "{{.State.Running}}", PG16_CONTAINER])
            .output();

        let running = inspect
            .ok()
            .filter(|out| out.status.success())
            .is_some_and(|out| String::from_utf8_lossy(&out.stdout).trim() == "true");

        if !running {
            let _ = Command::new("docker")
                .args(["rm", "-f", PG16_CONTAINER])
                .output();
            let status = Command::new("docker")
                .args([
                    "run",
                    "-d",
                    "--rm",
                    "--name",
                    PG16_CONTAINER,
                    "-e",
                    "POSTGRES_PASSWORD=postgres",
                    "-e",
                    "POSTGRES_DB=tpch",
                    "-p",
                    "15432:5432",
                    "postgres:16-alpine",
                ])
                .status()
                .expect("docker command must execute");
            assert!(status.success(), "failed to start PostgreSQL 16 reference");
        }

        let deadline = Instant::now() + Duration::from_secs(30);
        while TcpStream::connect(("127.0.0.1", PG16_PORT)).is_err() {
            assert!(
                Instant::now() < deadline,
                "PostgreSQL 16 reference did not become ready within timeout"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    });
}

fn pg16_client() -> Client {
    ensure_pg16_running();
    let mut last_err = None;
    for _ in 0..30 {
        match Client::connect(
            "host=127.0.0.1 port=15432 user=postgres password=postgres dbname=tpch",
            NoTls,
        ) {
            Ok(client) => return client,
            Err(err) => {
                last_err = Some(err);
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
    panic!("failed to connect to PostgreSQL 16 reference: {last_err:?}");
}

fn pg_cell(row: &Row, index: usize) -> CanonicalValue {
    let ty = row.columns()[index].type_().name();
    match ty {
        "int2" => row
            .try_get::<_, Option<i16>>(index)
            .expect("decode int2")
            .map(|v| CanonicalValue::Integer(v as i64))
            .unwrap_or(CanonicalValue::Null),
        "int4" => row
            .try_get::<_, Option<i32>>(index)
            .expect("decode int4")
            .map(|v| CanonicalValue::Integer(v as i64))
            .unwrap_or(CanonicalValue::Null),
        "int8" => row
            .try_get::<_, Option<i64>>(index)
            .expect("decode int8")
            .map(CanonicalValue::Integer)
            .unwrap_or(CanonicalValue::Null),
        "float4" => row
            .try_get::<_, Option<f32>>(index)
            .expect("decode float4")
            .map(|v| CanonicalValue::Float(v as f64))
            .unwrap_or(CanonicalValue::Null),
        "float8" => row
            .try_get::<_, Option<f64>>(index)
            .expect("decode float8")
            .map(CanonicalValue::Float)
            .unwrap_or(CanonicalValue::Null),
        "bool" => row
            .try_get::<_, Option<bool>>(index)
            .expect("decode bool")
            .map(CanonicalValue::Bool)
            .unwrap_or(CanonicalValue::Null),
        "text" | "varchar" | "bpchar" | "name" => row
            .try_get::<_, Option<String>>(index)
            .expect("decode text")
            .map(CanonicalValue::Text)
            .unwrap_or(CanonicalValue::Null),
        other => panic!("reference harness does not yet decode PostgreSQL type '{other}'"),
    }
}

fn normalize(mut result: CanonicalResult, ordered: bool) -> CanonicalResult {
    if !ordered {
        result.rows.sort_by_key(|row| format!("{row:?}"));
    }
    result
}

pub fn postgres_outcome(case: DifferentialCase) -> CanonicalOutcome {
    let _guard = pg_lock().lock().expect("PostgreSQL reference lock poisoned");
    let mut client = pg16_client();
    match client.query(case.sql, &[]) {
        Ok(rows) => {
            let rows = rows
                .iter()
                .map(|row| {
                    (0..row.len())
                        .map(|index| pg_cell(row, index))
                        .collect::<Vec<_>>()
                })
                .collect();
            CanonicalOutcome::Rows(normalize(CanonicalResult { rows }, case.ordered))
        }
        Err(error) => CanonicalOutcome::Error(CanonicalError {
            sqlstate: error
                .code()
                .map(|code| code.code().to_string())
                .unwrap_or_else(|| "XXXXX".to_string()),
        }),
    }
}

fn neuralbase_error_sqlstate(error: &QueryError) -> &'static str {
    match error {
        QueryError::TableNotFound(_) => "42P01",
        QueryError::ColumnNotFound(_) => "42703",
        QueryError::TypeError(_) => "42804",
        QueryError::SubqueryMultipleRows => "21000",
        QueryError::Unsupported(_) => "0A000",
        QueryError::DivisionByZero => "22012",
    }
}

fn nb_value(value: &ScalarVal) -> CanonicalValue {
    match value {
        ScalarVal::Int(v) => CanonicalValue::Integer(*v),
        ScalarVal::Float(v) => CanonicalValue::Float(*v),
        ScalarVal::Text(v) => CanonicalValue::Text(v.clone()),
        ScalarVal::Bool(v) => CanonicalValue::Bool(*v),
        ScalarVal::Null => CanonicalValue::Null,
        ScalarVal::Date(v) => CanonicalValue::Integer(*v as i64),
    }
}

pub fn neuralbase_outcome(case: DifferentialCase) -> CanonicalOutcome {
    let statement = match parse_statement(case.sql) {
        Ok(statement) => statement,
        Err(_) => {
            return CanonicalOutcome::Error(CanonicalError {
                sqlstate: "42601".to_string(),
            })
        }
    };
    let Statement::Query(query) = statement else {
        return CanonicalOutcome::Error(CanonicalError {
            sqlstate: "0A000".to_string(),
        });
    };

    match execute_select_query(&query, &QueryCatalog::new()) {
        Ok(result) => CanonicalOutcome::Rows(normalize(
            CanonicalResult {
                rows: result
                    .rows
                    .iter()
                    .map(|row| row.iter().map(nb_value).collect())
                    .collect(),
            },
            case.ordered,
        )),
        Err(error) => CanonicalOutcome::Error(CanonicalError {
            sqlstate: neuralbase_error_sqlstate(&error).to_string(),
        }),
    }
}

pub fn assert_matches_postgres(case: DifferentialCase) {
    let pg = postgres_outcome(case);
    let nb = neuralbase_outcome(case);
    assert_eq!(
        nb, pg,
        "semantic divergence for case '{}' SQL: {}",
        case.name, case.sql
    );
}
