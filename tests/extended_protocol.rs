// SPDX-License-Identifier: Apache-2.0
// Phase 8: live PostgreSQL extended-protocol contract tests.

use neuralbase::catalog::InMemoryCatalog;
use neuralbase::extended_protocol::INT4OID;
use neuralbase::protocol::STARTUP_PROTOCOL_V3;
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
    if !(4..=1_048_576).contains(&len) {
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

fn typed(tag: u8, body: Vec<u8>) -> Vec<u8> {
    let mut message = Vec::with_capacity(body.len() + 5);
    message.push(tag);
    message.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    message.extend_from_slice(&body);
    message
}

fn parse_message(name: &str, sql: &str, parameter_types: &[i32]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(sql.as_bytes());
    body.push(0);
    body.extend_from_slice(&(parameter_types.len() as i16).to_be_bytes());
    for oid in parameter_types {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    typed(b'P', body)
}

fn bind_text_message(portal: &str, statement: &str, values: &[Option<&str>]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(statement.as_bytes());
    body.push(0);
    body.extend_from_slice(&0_i16.to_be_bytes());
    body.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for value in values {
        match value {
            Some(value) => {
                body.extend_from_slice(&(value.len() as i32).to_be_bytes());
                body.extend_from_slice(value.as_bytes());
            }
            None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
    body.extend_from_slice(&0_i16.to_be_bytes());
    typed(b'B', body)
}

fn execute_message(portal: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(portal.as_bytes());
    body.push(0);
    body.extend_from_slice(&0_i32.to_be_bytes());
    typed(b'E', body)
}

fn describe_statement(name: &str) -> Vec<u8> {
    let mut body = vec![b'S'];
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    typed(b'D', body)
}

fn sync_message() -> Vec<u8> {
    typed(b'S', Vec::new())
}

#[tokio::test]
async fn typed_bind_parameter_reaches_live_execute_path() {
    let (addr, server_task) = start_server().await;
    let mut client = TcpStream::connect(addr).await.expect("connect");
    startup(&mut client).await;

    client
        .write_all(&parse_message("s1", "SELECT $1", &[INT4OID]))
        .await
        .expect("parse write");
    client
        .write_all(&bind_text_message("p1", "s1", &[Some("42")]))
        .await
        .expect("bind write");
    client
        .write_all(&execute_message("p1"))
        .await
        .expect("execute write");
    client.write_all(&sync_message()).await.expect("sync write");

    let messages = read_until_ready(&mut client).await;
    assert!(messages.iter().any(|(tag, _)| *tag == b'1'));
    assert!(messages.iter().any(|(tag, _)| *tag == b'2'));
    assert!(messages
        .iter()
        .any(|(tag, payload)| { *tag == b'D' && String::from_utf8_lossy(payload).contains("42") }));
    assert!(!messages.iter().any(|(tag, _)| *tag == b'E'));

    server_task.abort();
}

#[tokio::test]
async fn untyped_parameter_fails_closed_during_bind() {
    let (addr, server_task) = start_server().await;
    let mut client = TcpStream::connect(addr).await.expect("connect");
    startup(&mut client).await;

    client
        .write_all(&parse_message("s1", "SELECT $1", &[0]))
        .await
        .expect("parse write");
    client
        .write_all(&bind_text_message("p1", "s1", &[Some("42")]))
        .await
        .expect("bind write");

    let messages = read_until_ready(&mut client).await;
    assert!(messages.iter().any(|(tag, payload)| {
        *tag == b'E' && String::from_utf8_lossy(payload).contains("0A000")
    }));
    assert!(!messages.iter().any(|(tag, _)| *tag == b'2'));

    server_task.abort();
}

#[tokio::test]
async fn describe_statement_returns_parameter_description() {
    let (addr, server_task) = start_server().await;
    let mut client = TcpStream::connect(addr).await.expect("connect");
    startup(&mut client).await;

    client
        .write_all(&parse_message("s1", "SELECT $1", &[INT4OID]))
        .await
        .expect("parse write");
    client
        .write_all(&describe_statement("s1"))
        .await
        .expect("describe write");
    client.write_all(&sync_message()).await.expect("sync write");

    let messages = read_until_ready(&mut client).await;
    let parameter_description = messages
        .iter()
        .find(|(tag, _)| *tag == b't')
        .expect("ParameterDescription");
    assert!(parameter_description
        .1
        .windows(4)
        .any(|bytes| bytes == INT4OID.to_be_bytes()));
    assert!(messages.iter().any(|(tag, _)| *tag == b'n'));
    assert!(!messages.iter().any(|(tag, _)| *tag == b'E'));

    server_task.abort();
}

#[tokio::test]
async fn execute_missing_portal_fails_closed() {
    let (addr, server_task) = start_server().await;
    let mut client = TcpStream::connect(addr).await.expect("connect");
    startup(&mut client).await;

    client
        .write_all(&execute_message("missing"))
        .await
        .expect("execute write");

    let messages = read_until_ready(&mut client).await;
    assert!(messages.iter().any(|(tag, payload)| {
        *tag == b'E' && String::from_utf8_lossy(payload).contains("34000")
    }));

    server_task.abort();
}
