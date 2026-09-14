// SPDX-License-Identifier: Apache-2.0
// Phase 8: process-level evidence with the real postgres client crate.

use neuralbase::catalog::InMemoryCatalog;
use neuralbase::{server, storage, storage_executor};
use postgres::types::Type;
use postgres::{Client, NoTls};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_client_reuses_typed_prepared_parameter_over_live_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let catalog = Arc::new(InMemoryCatalog::with_tpch_lineitem());
    let server_task = tokio::spawn(async move {
        let _ = server::run(
            listener,
            catalog,
            None::<Arc<storage_executor::StorageExecutor>>,
            None,
            None::<Arc<storage::StorageEngine>>,
        )
        .await;
    });

    let connection = format!(
        "host=127.0.0.1 port={} user=neuralbase dbname=neuralbase sslmode=disable",
        addr.port()
    );
    let outcome = tokio::task::spawn_blocking(move || -> Result<(), postgres::Error> {
        let mut client = Client::connect(&connection, NoTls)?;
        let statement =
            client.prepare_typed("SET neuralbase_read_consistency = $1", &[Type::TEXT])?;

        client.execute(&statement, &[&"local"])?;
        client.execute(&statement, &[&"stale"])?;

        let rows = client.simple_query("SELECT 1")?;
        assert!(rows.iter().any(|message| match message {
            postgres::SimpleQueryMessage::Row(row) => row.get(0) == Some("1"),
            _ => false,
        }));
        Ok(())
    })
    .await
    .expect("postgres client task");

    outcome.expect("real PostgreSQL client extended-protocol roundtrip");
    server_task.abort();
}
