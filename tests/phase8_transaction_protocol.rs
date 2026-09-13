// SPDX-License-Identifier: Apache-2.0
// Phase 8: SQL transaction blocks remain unsupported, ordinary statements are
// independent/autocommit requests, and compound input may not partially run.

use neuralbase::binder::{bind_statement, BindError};
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::protocol::STARTUP_PROTOCOL_V3;
use neuralbase::sql::{parse_statement, SqlParseError};
use neuralbase::{server, storage, storage_executor};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};

async fn start_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());
    let task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<Arc<storage_executor::StorageExecutor>>,
            None,
            None::<Arc<storage::StorageEngine>>,
        )
        .await;
    });
    (addr, task)
}

async fn startup(stream: &mut TcpStream) {
    let mut body = Vec::new();
    body.extend_from_slice(&STARTUP_PROTOCOL_V3.to_be_bytes());
    body.extend_from_slice(b"user\0neuralbase\0database\0neuralbase\0\0");
    let mut packet = Vec::new();
    packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    packet.extend_from_slice(&body);
    stream.write_all(&packet).await.expect("startup write");
    let _ = read_until_ready(stream).await;
}

async fn read_message(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag).await?;
    let mut len = [0_u8; 4];
    stream.read_exact(&mut len).await?;
    let len = i32::from_be_bytes(len);
    if len < 4 || len > 1_048_576 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid backend message length",
        ));
    }
    let mut payload = vec![0_u8; (len - 4) as usize];
    stream.read_exact(&mut payload).await?;
    Ok((tag[0], payload))
}

async fn read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    timeout(Duration::from_secs(5), async {
        let mut messages = Vec::new();
        loop {
            let message = read_message(stream).await.expect("backend message");
            let ready = message.0 == b'Z';
            messages.push(message);
            if ready {
                return messages;
            }
        }
    })
    .await
    .expect("backend response timeout")
}

async fn simple_query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    let mut message = vec![b'Q'];
    message.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    message.extend_from_slice(&body);
    stream.write_all(&message).await.expect("query write");
    read_until_ready(stream).await
}

#[test]
fn parser_rejects_compound_requests_instead_of_taking_first_statement() {
    assert!(matches!(
        parse_statement("SELECT 1; SELECT 2"),
        Err(SqlParseError::MultipleStatements(2))
    ));
}

#[test]
fn transaction_control_statements_remain_explicitly_unsupported() {
    let catalog = InMemoryCatalog::default();
    for sql in ["BEGIN", "COMMIT", "ROLLBACK"] {
        let statement = parse_statement(sql).expect("transaction control parses");
        assert!(matches!(
            bind_statement(&statement, &catalog),
            Err(BindError::Unsupported)
        ));
    }
}

#[tokio::test]
async fn unsupported_transaction_control_does_not_poison_connection() {
    let (addr, server_task) = start_server().await;
    let mut client = TcpStream::connect(addr).await.expect("connect");
    startup(&mut client).await;

    let begin = simple_query(&mut client, "BEGIN").await;
    assert!(begin.iter().any(|(tag, _)| *tag == b'E'));
    assert!(!begin.iter().any(|(tag, _)| *tag == b'D'));

    let select = simple_query(&mut client, "SELECT 7").await;
    assert!(select.iter().any(|(tag, payload)| {
        *tag == b'D' && String::from_utf8_lossy(payload).contains('7')
    }));
    assert!(!select.iter().any(|(tag, _)| *tag == b'E'));

    server_task.abort();
}

#[tokio::test]
async fn compound_simple_query_fails_before_any_partial_result_and_session_recovers() {
    let (addr, server_task) = start_server().await;
    let mut client = TcpStream::connect(addr).await.expect("connect");
    startup(&mut client).await;

    let compound = simple_query(&mut client, "SELECT 1; SELECT 2").await;
    assert!(compound.iter().any(|(tag, payload)| {
        *tag == b'E' && String::from_utf8_lossy(payload).contains("42601")
    }));
    assert!(!compound.iter().any(|(tag, _)| *tag == b'D'));

    let select = simple_query(&mut client, "SELECT 9").await;
    assert!(select.iter().any(|(tag, payload)| {
        *tag == b'D' && String::from_utf8_lossy(payload).contains('9')
    }));
    assert!(!select.iter().any(|(tag, _)| *tag == b'E'));

    server_task.abort();
}
