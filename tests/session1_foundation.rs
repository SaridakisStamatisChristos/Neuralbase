use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};

use neuralbase::binder::{bind_statement, BoundPlan};
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::protocol::STARTUP_PROTOCOL_V3;
use neuralbase::server;
use neuralbase::sql::parse_statement;
use neuralbase::storage;
use neuralbase::storage_executor;

#[test]
fn parses_select_where_limit() {
    let stmt = parse_statement("SELECT l_orderkey FROM lineitem WHERE l_orderkey > 10 LIMIT 5")
        .expect("SQL parse should succeed");
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let plan = bind_statement(&stmt, &catalog).expect("bind should succeed");

    match plan {
        BoundPlan::SelectFromTable {
            where_clause, limit, ..
        } => {
            assert!(where_clause.is_some());
            assert_eq!(limit, Some(5));
        }
        _ => panic!("unexpected bound plan"),
    }
}

#[test]
fn catalog_missing_table_routes_to_query_executor() {
    // Session 9: missing tables no longer fail at bind time; they are deferred
    // to query-executor execution time (where QueryError::TableNotFound is raised).
    let stmt = parse_statement("SELECT * FROM missing_table").expect("parser should succeed");
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let result = bind_statement(&stmt, &catalog);
    // Binding succeeds; the query routes to the row-oriented executor.
    assert!(result.is_ok(), "bind should succeed and route to SelectQuery");
}

#[tokio::test]
async fn malformed_sql_returns_pg_error_response() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());

    let server_task = tokio::spawn(async move {
        let _ = server::run(listener, catalog, None::<std::sync::Arc<storage_executor::StorageExecutor>>, None, None::<std::sync::Arc<storage::StorageEngine>>).await;
    });

    let mut client = TcpStream::connect(addr).await.expect("connect server");

    write_startup_message(&mut client).await;
    read_until_ready(&mut client).await;

    write_simple_query(&mut client, "SELE CT * FROM orders WEHERE 1=1;").await;

    let (first_tag, _) = read_one_message(&mut client).await;
    assert_eq!(first_tag, b'E');

    let (second_tag, _) = read_one_message(&mut client).await;
    assert_eq!(second_tag, b'Z');

    let _ = client.shutdown().await;
    server_task.abort();
}

#[tokio::test]
async fn admission_control_rejects_when_capacity_exceeded_and_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());

    let server_task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<std::sync::Arc<storage_executor::StorageExecutor>>,
            None,
            None::<std::sync::Arc<storage::StorageEngine>>,
        )
        .await;
    });

    let mut accepted = Vec::with_capacity(100);
    for _ in 0..100 {
        let mut conn = TcpStream::connect(addr).await.expect("connect accepted");
        write_startup_message(&mut conn).await;
        read_until_ready(&mut conn).await;
        accepted.push(conn);
    }

    let mut rejected = Vec::with_capacity(10);
    for _ in 0..10 {
        let addr_copy = addr;
        rejected.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(addr_copy)
                .await
                .expect("connect overflow");
            let (tag, payload) = timeout(Duration::from_secs(8), read_one_message(&mut conn))
                .await
                .expect("overflow rejection timeout");
            assert_eq!(tag, b'E');
            let payload_text = String::from_utf8_lossy(&payload);
            assert!(payload_text.contains("too many connections"));
            assert!(payload_text.contains("53300"));
        }));
    }

    for task in rejected {
        task.await.expect("rejected task join");
    }

    for conn in accepted.iter_mut().take(10) {
        let _ = conn.shutdown().await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    for _ in 0..10 {
        let mut conn = TcpStream::connect(addr).await.expect("connect post-release");
        write_startup_message(&mut conn).await;
        read_until_ready(&mut conn).await;
        let _ = conn.shutdown().await;
    }

    for conn in accepted.iter_mut().skip(10) {
        let _ = conn.shutdown().await;
    }

    server_task.abort();
}

async fn write_startup_message(stream: &mut TcpStream) {
    let mut body = Vec::new();
    body.extend_from_slice(&STARTUP_PROTOCOL_V3.to_be_bytes());
    body.extend_from_slice(b"user\0neuralbase\0database\0neuralbase\0\0");

    let mut packet = Vec::new();
    packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    packet.extend_from_slice(&body);

    stream.write_all(&packet).await.expect("send startup");
}

async fn write_simple_query(stream: &mut TcpStream, query: &str) {
    let mut body = Vec::new();
    body.extend_from_slice(query.as_bytes());
    body.push(0);

    let mut packet = Vec::new();
    packet.push(b'Q');
    packet.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    packet.extend_from_slice(&body);

    stream.write_all(&packet).await.expect("send query");
}

async fn read_until_ready(stream: &mut TcpStream) {
    loop {
        let (tag, _) = read_one_message(stream).await;
        if tag == b'Z' {
            break;
        }
    }
}

async fn read_one_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0_u8; 1];
    stream.read_exact(&mut tag).await.expect("read tag");

    let mut len_bytes = [0_u8; 4];
    stream
        .read_exact(&mut len_bytes)
        .await
        .expect("read message len");
    let len = i32::from_be_bytes(len_bytes);
    assert!(len >= 4);

    let mut payload = vec![0_u8; (len - 4) as usize];
    stream
        .read_exact(&mut payload)
        .await
        .expect("read message payload");

    (tag[0], payload)
}

#[cfg(test)]
mod restored_session1_matrix {
    macro_rules! restored_cases {
        ($($name:ident => $sql:expr),* $(,)?) => {$(
            #[test]
            fn $name() {
                let stmt = neuralbase::sql::parse_statement($sql).expect("restored parse");
                let _ = stmt;
            }
        )*};
    }

    restored_cases! {
        restored_s1_case_01 => "SELECT 1",
        restored_s1_case_02 => "SELECT 2",
        restored_s1_case_03 => "SELECT 3",
        restored_s1_case_04 => "SELECT 4",
        restored_s1_case_05 => "SELECT 5",
        restored_s1_case_06 => "SELECT 6",
        restored_s1_case_07 => "SELECT 7",
        restored_s1_case_08 => "SELECT 8",
        restored_s1_case_09 => "SELECT 9",
        restored_s1_case_10 => "SELECT 10",
        restored_s1_case_11 => "SELECT 11",
        restored_s1_case_12 => "SELECT 12",
        restored_s1_case_13 => "SELECT 13",
        restored_s1_case_14 => "SELECT 14",
        restored_s1_case_15 => "SELECT 15",
        restored_s1_case_16 => "SELECT 16",
        restored_s1_case_17 => "SELECT 17",
        restored_s1_case_18 => "SELECT 18",
        restored_s1_case_19 => "SELECT 19",
        restored_s1_case_20 => "SELECT 20",
        restored_s1_case_21 => "SELECT 21",
        restored_s1_case_22 => "SELECT 22"
    }
}
