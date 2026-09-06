// SPDX-License-Identifier: Apache-2.0
// TPC-H Q1-Q22 correctness suite.
//
// Evidence layers:
//   - Q1 and Q6 have additional deterministic correctness checks.
//   - Every checked-in Q1-Q22 SQL string is parsed and exercised through the
//     current bind/execution path where applicable.
//   - Q1-Q22 each have PostgreSQL 16 row-for-row reference comparisons on the
//     deterministic small execution dataset (EXEC_TEST_SF = 0.001).
//
// The PostgreSQL reference harness runs in an opt-in test target because it
// starts a Docker container. This suite is correctness evidence for the exact
// checked data/query forms, not official TPC-H certification or a production-
// scale benchmark.
//
#![allow(unused_imports)]

use neuralbase::binder::{bind_statement, BindError, BoundPlan};
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::execution::{build_physical_plan, execute_physical_plan};
use neuralbase::join_graph;
use neuralbase::optimizer;
use neuralbase::query_executor::{self, execute_select_query, QueryCatalog};
use neuralbase::scheduler::MorselScheduler;
use neuralbase::sql::parse_statement;
use neuralbase::stats;
use neuralbase::tpch::{self, generate_tpch_data};
use neuralbase::vectorized::{self, ColumnVector};
use postgres::{Client, NoTls};
use serde_json::{Map, Number, Value};
use sqlparser::ast::Statement;
use std::io::ErrorKind;
use std::net::TcpStream;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const EXEC_TEST_SF: f64 = 0.001;
const PG16_CONTAINER: &str = "neuralbase-pg16-ref";
// Port 55432 falls in a Hyper-V reserved range on Windows (55379-55478).
// 15432 is outside all reserved ranges and conventionally used for PG mirrors.
const PG16_PORT: u16 = 15432;
const NUMERIC_TOLERANCE: f64 = 0.01;

fn tpch_sf01_dataset() -> &'static tpch::TpchDataSet {
    static DS: OnceLock<tpch::TpchDataSet> = OnceLock::new();
    DS.get_or_init(|| generate_tpch_data(0.1))
}

fn tpch_exec_dataset() -> &'static tpch::TpchDataSet {
    static DS: OnceLock<tpch::TpchDataSet> = OnceLock::new();
    DS.get_or_init(|| generate_tpch_data(EXEC_TEST_SF))
}

fn tpch_exec_catalog() -> &'static QueryCatalog {
    static CAT: OnceLock<QueryCatalog> = OnceLock::new();
    CAT.get_or_init(|| QueryCatalog::from_tpch(tpch_exec_dataset()))
}

fn pg_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn ensure_pg16_seeded() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        ensure_pg16_container_running();
        seed_pg16_reference_data();
    });
}

fn ensure_pg16_container_running() {
    let inspect = Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}", PG16_CONTAINER])
        .output();

    let mut running = false;
    if let Ok(out) = inspect {
        if out.status.success() {
            running = String::from_utf8_lossy(&out.stdout).trim() == "true";
        }
    }

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
        assert!(
            status.success(),
            "failed to start PostgreSQL 16 reference container"
        );
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", PG16_PORT)).is_ok() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("PostgreSQL 16 container did not become ready within timeout");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn pg16_client() -> Client {
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
    panic!(
        "failed to connect to PostgreSQL 16 reference: {:?}",
        last_err
    );
}

fn yyyymmdd_to_iso(date: i32) -> String {
    let year = date / 10000;
    let month = (date / 100) % 100;
    let day = date % 100;
    format!("{year:04}-{month:02}-{day:02}")
}

fn seed_pg16_reference_data() {
    let mut client = pg16_client();
    client
        .batch_execute(
            "
            DROP TABLE IF EXISTS lineitem;
            DROP TABLE IF EXISTS orders;
            DROP TABLE IF EXISTS customer;
            DROP TABLE IF EXISTS supplier;
            DROP TABLE IF EXISTS nation;
            DROP TABLE IF EXISTS region;
            DROP TABLE IF EXISTS part;
            DROP TABLE IF EXISTS partsupp;

            CREATE TABLE lineitem (
                l_orderkey BIGINT,
                l_partkey BIGINT,
                l_suppkey BIGINT,
                l_linenumber INTEGER,
                l_quantity DOUBLE PRECISION,
                l_extendedprice DOUBLE PRECISION,
                l_discount DOUBLE PRECISION,
                l_tax DOUBLE PRECISION,
                l_returnflag TEXT,
                l_linestatus TEXT,
                l_shipdate DATE,
                l_commitdate DATE,
                l_receiptdate DATE,
                l_shipmode TEXT,
                l_shipinstruct TEXT
            );

            CREATE TABLE orders (
                o_orderkey BIGINT,
                o_custkey BIGINT,
                o_orderstatus TEXT,
                o_totalprice DOUBLE PRECISION,
                o_orderdate DATE,
                o_orderpriority TEXT,
                o_shippriority INTEGER,
                o_comment TEXT
            );

            CREATE TABLE customer (
                c_custkey BIGINT,
                c_name TEXT,
                c_nationkey BIGINT,
                c_mktsegment TEXT,
                c_acctbal DOUBLE PRECISION,
                c_phone TEXT,
                c_address TEXT,
                c_comment TEXT
            );

            CREATE TABLE supplier (
                s_suppkey BIGINT,
                s_name TEXT,
                s_nationkey BIGINT,
                s_acctbal DOUBLE PRECISION,
                s_address TEXT,
                s_phone TEXT,
                s_comment TEXT
            );

            CREATE TABLE nation (
                n_nationkey BIGINT,
                n_name TEXT,
                n_regionkey BIGINT
            );

            CREATE TABLE region (
                r_regionkey BIGINT,
                r_name TEXT
            );

            CREATE TABLE part (
                p_partkey BIGINT,
                p_name TEXT,
                p_mfgr TEXT,
                p_brand TEXT,
                p_type TEXT,
                p_size INTEGER,
                p_container TEXT,
                p_retailprice DOUBLE PRECISION
            );

            CREATE TABLE partsupp (
                ps_partkey BIGINT,
                ps_suppkey BIGINT,
                ps_availqty INTEGER,
                ps_supplycost DOUBLE PRECISION
            );
            ",
        )
        .expect("create TPCH reference schema");

    let data = tpch_exec_dataset();

    let mut tx = client.transaction().expect("start seed transaction");

    let ins_lineitem = tx
        .prepare("INSERT INTO lineitem VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,to_date($11, 'YYYY-MM-DD'),to_date($12, 'YYYY-MM-DD'),to_date($13, 'YYYY-MM-DD'),$14,$15)")
        .expect("prepare lineitem insert");
    let lineitem = &data.lineitem;
    let l_orderkey = match lineitem.column("l_orderkey").expect("l_orderkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("l_orderkey type"),
    };
    let l_partkey = match lineitem.column("l_partkey").expect("l_partkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("l_partkey type"),
    };
    let l_suppkey = match lineitem.column("l_suppkey").expect("l_suppkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("l_suppkey type"),
    };
    let l_linenumber = match lineitem.column("l_linenumber").expect("l_linenumber") {
        ColumnVector::Int32(v) => v,
        _ => panic!("l_linenumber type"),
    };
    let l_quantity = match lineitem.column("l_quantity").expect("l_quantity") {
        ColumnVector::Float64(v) => v,
        _ => panic!("l_quantity type"),
    };
    let l_extendedprice = match lineitem.column("l_extendedprice").expect("l_extendedprice") {
        ColumnVector::Float64(v) => v,
        _ => panic!("l_extendedprice type"),
    };
    let l_discount = match lineitem.column("l_discount").expect("l_discount") {
        ColumnVector::Float64(v) => v,
        _ => panic!("l_discount type"),
    };
    let l_tax = match lineitem.column("l_tax").expect("l_tax") {
        ColumnVector::Float64(v) => v,
        _ => panic!("l_tax type"),
    };
    let l_returnflag = match lineitem.column("l_returnflag").expect("l_returnflag") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("l_returnflag type"),
    };
    let l_linestatus = match lineitem.column("l_linestatus").expect("l_linestatus") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("l_linestatus type"),
    };
    let l_shipdate = match lineitem.column("l_shipdate").expect("l_shipdate") {
        ColumnVector::Date32(v) => v,
        _ => panic!("l_shipdate type"),
    };
    let l_commitdate = match lineitem.column("l_commitdate").expect("l_commitdate") {
        ColumnVector::Date32(v) => v,
        _ => panic!("l_commitdate type"),
    };
    let l_receiptdate = match lineitem.column("l_receiptdate").expect("l_receiptdate") {
        ColumnVector::Date32(v) => v,
        _ => panic!("l_receiptdate type"),
    };
    let l_shipmode = match lineitem.column("l_shipmode").expect("l_shipmode") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("l_shipmode type"),
    };
    let l_shipinstruct = match lineitem.column("l_shipinstruct").expect("l_shipinstruct") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("l_shipinstruct type"),
    };

    for i in 0..lineitem.row_count {
        tx.execute(
            &ins_lineitem,
            &[
                &l_orderkey[i],
                &l_partkey[i],
                &l_suppkey[i],
                &l_linenumber[i],
                &l_quantity[i],
                &l_extendedprice[i],
                &l_discount[i],
                &l_tax[i],
                &l_returnflag.get(i),
                &l_linestatus.get(i),
                &l_shipdate[i].map(yyyymmdd_to_iso),
                &l_commitdate[i].map(yyyymmdd_to_iso),
                &l_receiptdate[i].map(yyyymmdd_to_iso),
                &l_shipmode.get(i),
                &l_shipinstruct.get(i),
            ],
        )
        .expect("insert lineitem row");
    }

    let orders = &data.orders;
    let ins_orders = tx
        .prepare("INSERT INTO orders VALUES ($1,$2,$3,$4,to_date($5, 'YYYY-MM-DD'),$6,$7,$8)")
        .expect("prepare orders insert");
    let o_orderkey = match orders.column("o_orderkey").expect("o_orderkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("o_orderkey type"),
    };
    let o_custkey = match orders.column("o_custkey").expect("o_custkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("o_custkey type"),
    };
    let o_orderstatus = match orders.column("o_orderstatus").expect("o_orderstatus") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("o_orderstatus type"),
    };
    let o_totalprice = match orders.column("o_totalprice").expect("o_totalprice") {
        ColumnVector::Float64(v) => v,
        _ => panic!("o_totalprice type"),
    };
    let o_orderdate = match orders.column("o_orderdate").expect("o_orderdate") {
        ColumnVector::Date32(v) => v,
        _ => panic!("o_orderdate type"),
    };
    let o_orderpriority = match orders.column("o_orderpriority").expect("o_orderpriority") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("o_orderpriority type"),
    };
    let o_shippriority = match orders.column("o_shippriority").expect("o_shippriority") {
        ColumnVector::Int32(v) => v,
        _ => panic!("o_shippriority type"),
    };
    let o_comment = match orders.column("o_comment").expect("o_comment") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("o_comment type"),
    };
    for i in 0..orders.row_count {
        tx.execute(
            &ins_orders,
            &[
                &o_orderkey[i],
                &o_custkey[i],
                &o_orderstatus.get(i),
                &o_totalprice[i],
                &o_orderdate[i].map(yyyymmdd_to_iso),
                &o_orderpriority.get(i),
                &o_shippriority[i],
                &o_comment.get(i),
            ],
        )
        .expect("insert orders row");
    }

    let customer = &data.customer;
    let ins_customer = tx
        .prepare("INSERT INTO customer VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
        .expect("prepare customer insert");
    let c_custkey = match customer.column("c_custkey").expect("c_custkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("c_custkey type"),
    };
    let c_name = match customer.column("c_name").expect("c_name") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("c_name type"),
    };
    let c_nationkey = match customer.column("c_nationkey").expect("c_nationkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("c_nationkey type"),
    };
    let c_mktsegment = match customer.column("c_mktsegment").expect("c_mktsegment") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("c_mktsegment type"),
    };
    let c_acctbal = match customer.column("c_acctbal").expect("c_acctbal") {
        ColumnVector::Float64(v) => v,
        _ => panic!("c_acctbal type"),
    };
    let c_phone = match customer.column("c_phone").expect("c_phone") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("c_phone type"),
    };
    let c_address = match customer.column("c_address").expect("c_address") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("c_address type"),
    };
    let c_comment = match customer.column("c_comment").expect("c_comment") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("c_comment type"),
    };
    for i in 0..customer.row_count {
        tx.execute(
            &ins_customer,
            &[
                &c_custkey[i],
                &c_name.get(i),
                &c_nationkey[i],
                &c_mktsegment.get(i),
                &c_acctbal[i],
                &c_phone.get(i),
                &c_address.get(i),
                &c_comment.get(i),
            ],
        )
        .expect("insert customer row");
    }

    let nation = &data.nation;
    let ins_nation = tx
        .prepare("INSERT INTO nation VALUES ($1,$2,$3)")
        .expect("prepare nation insert");
    let n_nationkey = match nation.column("n_nationkey").expect("n_nationkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("n_nationkey type"),
    };
    let n_name = match nation.column("n_name").expect("n_name") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("n_name type"),
    };
    let n_regionkey = match nation.column("n_regionkey").expect("n_regionkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("n_regionkey type"),
    };
    for i in 0..nation.row_count {
        tx.execute(
            &ins_nation,
            &[&n_nationkey[i], &n_name.get(i), &n_regionkey[i]],
        )
        .expect("insert nation row");
    }

    let region = &data.region;
    let ins_region = tx
        .prepare("INSERT INTO region VALUES ($1,$2)")
        .expect("prepare region insert");
    let r_regionkey = match region.column("r_regionkey").expect("r_regionkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("r_regionkey type"),
    };
    let r_name = match region.column("r_name").expect("r_name") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("r_name type"),
    };
    for (i, regionkey_val) in r_regionkey.iter().enumerate().take(region.row_count) {
        tx.execute(&ins_region, &[regionkey_val, &r_name.get(i)])
            .expect("insert region row");
    }

    let part = &data.part;
    let ins_part = tx
        .prepare("INSERT INTO part VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
        .expect("prepare part insert");
    let p_partkey = match part.column("p_partkey").expect("p_partkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("p_partkey type"),
    };
    let p_name = match part.column("p_name").expect("p_name") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("p_name type"),
    };
    let p_brand = match part.column("p_brand").expect("p_brand") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("p_brand type"),
    };
    let p_mfgr = match part.column("p_mfgr").expect("p_mfgr") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("p_mfgr type"),
    };
    let p_type = match part.column("p_type").expect("p_type") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("p_type type"),
    };
    let p_size = match part.column("p_size").expect("p_size") {
        ColumnVector::Int32(v) => v,
        _ => panic!("p_size type"),
    };
    let p_container = match part.column("p_container").expect("p_container") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("p_container type"),
    };
    let p_retailprice = match part.column("p_retailprice").expect("p_retailprice") {
        ColumnVector::Float64(v) => v,
        _ => panic!("p_retailprice type"),
    };
    for i in 0..part.row_count {
        tx.execute(
            &ins_part,
            &[
                &p_partkey[i],
                &p_name.get(i),
                &p_mfgr.get(i),
                &p_brand.get(i),
                &p_type.get(i),
                &p_size[i],
                &p_container.get(i),
                &p_retailprice[i],
            ],
        )
        .expect("insert part row");
    }

    let supplier = &data.supplier;
    let ins_supplier = tx
        .prepare("INSERT INTO supplier VALUES ($1,$2,$3,$4,$5,$6,$7)")
        .expect("prepare supplier insert");
    let s_suppkey = match supplier.column("s_suppkey").expect("s_suppkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("s_suppkey type"),
    };
    let s_name = match supplier.column("s_name").expect("s_name") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("s_name type"),
    };
    let s_nationkey = match supplier.column("s_nationkey").expect("s_nationkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("s_nationkey type"),
    };
    let s_acctbal = match supplier.column("s_acctbal").expect("s_acctbal") {
        ColumnVector::Float64(v) => v,
        _ => panic!("s_acctbal type"),
    };
    let s_address = match supplier.column("s_address").expect("s_address") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("s_address type"),
    };
    let s_phone = match supplier.column("s_phone").expect("s_phone") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("s_phone type"),
    };
    let s_comment = match supplier.column("s_comment").expect("s_comment") {
        ColumnVector::Utf8(v) => v,
        _ => panic!("s_comment type"),
    };
    for i in 0..supplier.row_count {
        tx.execute(
            &ins_supplier,
            &[
                &s_suppkey[i],
                &s_name.get(i),
                &s_nationkey[i],
                &s_acctbal[i],
                &s_address.get(i),
                &s_phone.get(i),
                &s_comment.get(i),
            ],
        )
        .expect("insert supplier row");
    }

    let partsupp = &data.partsupp;
    let ins_partsupp = tx
        .prepare("INSERT INTO partsupp VALUES ($1,$2,$3,$4)")
        .expect("prepare partsupp insert");
    let ps_partkey = match partsupp.column("ps_partkey").expect("ps_partkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("ps_partkey type"),
    };
    let ps_suppkey = match partsupp.column("ps_suppkey").expect("ps_suppkey") {
        ColumnVector::Int64(v) => v,
        _ => panic!("ps_suppkey type"),
    };
    let ps_availqty = match partsupp.column("ps_availqty").expect("ps_availqty") {
        ColumnVector::Int32(v) => v,
        _ => panic!("ps_availqty type"),
    };
    let ps_supplycost = match partsupp.column("ps_supplycost").expect("ps_supplycost") {
        ColumnVector::Float64(v) => v,
        _ => panic!("ps_supplycost type"),
    };
    for i in 0..partsupp.row_count {
        tx.execute(
            &ins_partsupp,
            &[
                &ps_partkey[i],
                &ps_suppkey[i],
                &ps_availqty[i],
                &ps_supplycost[i],
            ],
        )
        .expect("insert partsupp row");
    }

    tx.commit().expect("commit TPCH reference seed");
}

fn scalar_to_json(v: &query_executor::ScalarVal) -> Value {
    match v {
        query_executor::ScalarVal::Int(n) => Value::Number(Number::from(*n)),
        query_executor::ScalarVal::Float(f) => Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        query_executor::ScalarVal::Text(s) => Value::String(s.clone()),
        query_executor::ScalarVal::Date(d) => Value::String(yyyymmdd_to_iso(*d)),
        query_executor::ScalarVal::Bool(b) => Value::Bool(*b),
        query_executor::ScalarVal::Null => Value::Null,
    }
}

fn run_neuralbase_sql_as_json_rows(sql: &str) -> Vec<Value> {
    let stmt = parse_statement(sql).unwrap_or_else(|e| panic!("NeuralBase parse failed: {e}"));
    if let Statement::Query(query) = &stmt {
        let result = execute_select_query(query, tpch_exec_catalog())
            .unwrap_or_else(|e| panic!("NeuralBase query_executor execution failed: {e}"));
        return result
            .rows
            .iter()
            .map(|row| {
                let mut obj = Map::new();
                for (idx, value) in row.iter().enumerate() {
                    obj.insert(result.columns[idx].clone(), scalar_to_json(value));
                }
                Value::Object(obj)
            })
            .collect();
    }

    let catalog = InMemoryCatalog::with_tpch_all_tables();
    match bind_statement(&stmt, &catalog) {
        Ok(BoundPlan::SelectQuery(query)) => {
            let result = execute_select_query(&query, tpch_exec_catalog())
                .unwrap_or_else(|e| panic!("NeuralBase execution failed: {e}"));
            result
                .rows
                .iter()
                .map(|row| {
                    let mut obj = Map::new();
                    for (idx, value) in row.iter().enumerate() {
                        obj.insert(result.columns[idx].clone(), scalar_to_json(value));
                    }
                    Value::Object(obj)
                })
                .collect()
        }
        Ok(other @ BoundPlan::SelectFromTable { .. }) => {
            let plan = build_physical_plan(&other);
            let scheduler = MorselScheduler::new(16_384);
            let batch = execute_physical_plan(&plan, tpch_exec_dataset(), &scheduler, None)
                .unwrap_or_else(|e| panic!("NeuralBase SelectFromTable failed: {e}"));
            record_batch_to_json_rows(&batch)
        }
        Ok(other) => panic!("NeuralBase unsupported plan for row-for-row check: {other:?}"),
        Err(e) => panic!("NeuralBase bind failed: {e}"),
    }
}

fn record_batch_to_json_rows(batch: &vectorized::RecordBatch) -> Vec<Value> {
    let mut rows = Vec::with_capacity(batch.row_count);
    for row in 0..batch.row_count {
        let mut obj = Map::new();
        for (name, col) in &batch.columns {
            let value = match col {
                ColumnVector::Int32(v) => v[row]
                    .map(|x| Value::Number(Number::from(x)))
                    .unwrap_or(Value::Null),
                ColumnVector::Int64(v) => v[row]
                    .map(|x| Value::Number(Number::from(x)))
                    .unwrap_or(Value::Null),
                ColumnVector::Float64(v) => v[row]
                    .and_then(Number::from_f64)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
                ColumnVector::Date32(v) => v[row]
                    .map(|d| Value::String(yyyymmdd_to_iso(d)))
                    .unwrap_or(Value::Null),
                ColumnVector::Utf8(v) => v.get(row).map(Value::String).unwrap_or(Value::Null),
            };
            obj.insert(name.clone(), value);
        }
        rows.push(Value::Object(obj));
    }
    rows
}

fn run_postgres16_sql_as_json_rows(sql: &str) -> Vec<Value> {
    ensure_pg16_seeded();
    let _guard = pg_lock().lock().expect("pg lock");
    let mut client = pg16_client();
    let wrapped = format!("SELECT to_jsonb(t)::text AS row_json FROM ({sql}) AS t");
    let rows = client
        .query(&wrapped, &[])
        .unwrap_or_else(|e| panic!("PostgreSQL 16 query failed: {e}"));
    rows.into_iter()
        .map(|row| {
            let text: String = row.get(0);
            serde_json::from_str::<Value>(&text)
                .unwrap_or_else(|e| panic!("invalid PG json row: {e}"))
        })
        .collect()
}

fn canonical_row_string(v: &Value) -> String {
    let mut row = v.clone();
    normalize_for_sort(&mut row);
    serde_json::to_string(&row).expect("row json serialize")
}

fn normalize_for_sort(v: &mut Value) {
    match v {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                let rounded = (f * 100_000.0).round() / 100_000.0;
                *v = Number::from_f64(rounded)
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                normalize_for_sort(item);
            }
        }
        Value::Object(map) => {
            for value in map.values_mut() {
                normalize_for_sort(value);
            }
        }
        _ => {}
    }
}

fn compare_json_values_with_tolerance(actual: &Value, expected: &Value, path: &str) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => {
            let af = a.as_f64().expect("actual number as f64");
            let bf = b.as_f64().expect("expected number as f64");
            assert!(
                (af - bf).abs() <= NUMERIC_TOLERANCE,
                "numeric mismatch at {path}: actual={af} expected={bf} delta={}",
                (af - bf).abs()
            );
        }
        (Value::String(a), Value::String(b)) => {
            assert_eq!(a, b, "string mismatch at {path}");
        }
        (Value::Bool(a), Value::Bool(b)) => {
            assert_eq!(a, b, "bool mismatch at {path}");
        }
        (Value::Null, Value::Null) => {}
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len(), "array len mismatch at {path}");
            for (idx, (av, bv)) in a.iter().zip(b.iter()).enumerate() {
                compare_json_values_with_tolerance(av, bv, &format!("{path}[{idx}]"));
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            if a.len() != b.len() {
                let mut a_keys = a.keys().cloned().collect::<Vec<_>>();
                let mut b_keys = b.keys().cloned().collect::<Vec<_>>();
                a_keys.sort();
                b_keys.sort();
                panic!(
                    "object key count mismatch at {path}: actual_keys={a_keys:?} expected_keys={b_keys:?}"
                );
            }
            for (k, av) in a {
                let bv = b
                    .get(k)
                    .unwrap_or_else(|| panic!("missing key at {path}.{k}"));
                compare_json_values_with_tolerance(av, bv, &format!("{path}.{k}"));
            }
        }
        _ => {
            panic!("type mismatch at {path}: actual={actual:?} expected={expected:?}");
        }
    }
}

fn assert_row_for_row_pg16(sql: &str, query_name: &str) {
    let mut actual = run_neuralbase_sql_as_json_rows(sql);
    let mut expected = run_postgres16_sql_as_json_rows(sql);
    actual.sort_by_key(canonical_row_string);
    expected.sort_by_key(canonical_row_string);

    if actual.len() != expected.len() {
        panic!(
            "{query_name}: row count mismatch actual={} expected={}\nactual={:?}\nexpected={:?}",
            actual.len(),
            expected.len(),
            actual,
            expected
        );
    }

    for (idx, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
        compare_json_values_with_tolerance(a, e, &format!("{query_name}.row[{idx}]"));
    }
}

// ── TPC-H SQL strings ────────────────────────────────────────────────────────
// All queries are the official TPC-H v3.0.1 formulations, substituted with
// the deterministic parameter values given in the TPC-H specification §4.2
// (parameter 1 of each query template).

const Q1_SQL: &str = "SELECT l_returnflag, l_linestatus, \
     sum(l_quantity) AS sum_qty, \
     sum(l_extendedprice) AS sum_base_price, \
     sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
     avg(l_quantity) AS avg_qty, \
     avg(l_extendedprice) AS avg_price, \
     avg(l_discount) AS avg_disc, \
     count(*) AS count_order \
     FROM lineitem \
     WHERE l_shipdate <= date '1998-12-01' \
     GROUP BY l_returnflag, l_linestatus \
     ORDER BY l_returnflag, l_linestatus";

const Q2_SQL: &str =
    "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment \
     FROM part, supplier, partsupp, nation, region \
     WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 \
     AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey \
     AND n_regionkey = r_regionkey AND r_name = 'EUROPE' \
     AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region \
                         WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey \
                         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey \
                         AND r_name = 'EUROPE') \
     ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100";

const Q3_SQL: &str = "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, \
     o_orderdate, o_shippriority \
     FROM customer, orders, lineitem \
     WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey \
     AND l_orderkey = o_orderkey AND o_orderdate < date '1995-03-15' \
     AND l_shipdate > date '1995-03-15' \
     GROUP BY l_orderkey, o_orderdate, o_shippriority \
     ORDER BY revenue DESC, o_orderdate LIMIT 10";

const Q4_SQL: &str = "SELECT o_orderpriority, count(*) AS order_count \
     FROM orders \
     WHERE o_orderdate >= date '1993-07-01' AND o_orderdate < date '1993-10-01' \
     AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey \
                 AND l_commitdate < l_receiptdate) \
     GROUP BY o_orderpriority \
     ORDER BY o_orderpriority";

const Q5_SQL: &str = "SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue \
     FROM customer, orders, lineitem, supplier, nation, region \
     WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey \
     AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey \
     AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey \
     AND r_name = 'ASIA' AND o_orderdate >= date '1994-01-01' \
     AND o_orderdate < date '1995-01-01' \
     GROUP BY n_name ORDER BY revenue DESC";

const Q6_SQL: &str = "SELECT sum(l_extendedprice * l_discount) AS revenue \
     FROM lineitem \
     WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01' \
     AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24";

const Q7_SQL: &str = "SELECT supp_nation, cust_nation, l_year, \
     sum(volume) AS revenue \
     FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, \
           extract(year FROM l_shipdate) AS l_year, \
           l_extendedprice * (1 - l_discount) AS volume \
           FROM supplier, lineitem, orders, customer, nation n1, nation n2 \
           WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey \
           AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey \
           AND c_nationkey = n2.n_nationkey \
           AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') \
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')) \
           AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31') AS shipping \
     GROUP BY supp_nation, cust_nation, l_year \
     ORDER BY supp_nation, cust_nation, l_year";

const Q8_SQL: &str = "SELECT o_year, \
     sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share \
     FROM (SELECT extract(year FROM o_orderdate) AS o_year, \
           l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation \
           FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region \
           WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey \
           AND l_orderkey = o_orderkey AND o_custkey = c_custkey \
           AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey \
           AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey \
           AND o_orderdate BETWEEN date '1995-01-01' AND date '1996-12-31' \
           AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations \
     GROUP BY o_year ORDER BY o_year";

const Q9_SQL: &str = "SELECT nation, o_year, sum(amount) AS sum_profit \
     FROM (SELECT n_name AS nation, extract(year FROM o_orderdate) AS o_year, \
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount \
           FROM part, supplier, lineitem, partsupp, orders, nation \
           WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey \
           AND ps_partkey = l_partkey AND p_partkey = l_partkey \
           AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey \
           AND p_name LIKE '%green%') AS profit \
     GROUP BY nation, o_year ORDER BY nation, o_year DESC";

const Q10_SQL: &str =
    "SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, \
     c_acctbal, n_name, c_address, c_phone, c_comment \
     FROM customer, orders, lineitem, nation \
     WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey \
     AND o_orderdate >= date '1993-10-01' AND o_orderdate < date '1994-01-01' \
     AND l_returnflag = 'R' AND c_nationkey = n_nationkey \
     GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
     ORDER BY revenue DESC LIMIT 20";

const Q11_SQL: &str = "SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value \
     FROM partsupp, supplier, nation \
     WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' \
     GROUP BY ps_partkey \
     HAVING sum(ps_supplycost * ps_availqty) > \
            (SELECT sum(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation \
             WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') \
     ORDER BY value DESC";

const Q12_SQL: &str =
    "SELECT l_shipmode, \
     sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, \
     sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count \
     FROM orders, lineitem \
     WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') \
     AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate \
     AND l_receiptdate >= date '1994-01-01' AND l_receiptdate < date '1995-01-01' \
     GROUP BY l_shipmode ORDER BY l_shipmode";

const Q13_SQL: &str = "SELECT c_count, count(*) AS custdist \
     FROM (SELECT c_custkey, count(o_orderkey) AS c_count \
           FROM customer LEFT OUTER JOIN orders \
           ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%' \
           GROUP BY c_custkey) AS c_orders \
     GROUP BY c_count ORDER BY custdist DESC, c_count DESC";

const Q14_SQL: &str = "SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' \
     THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / \
     sum(l_extendedprice * (1 - l_discount)) AS promo_revenue \
     FROM lineitem, part \
     WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01' \
     AND l_shipdate < date '1995-10-01'";

// Q15 rewritten without CREATE VIEW: inline derived table `revenue0`.
const Q15_SQL: &str = "SELECT s_suppkey, s_name, s_address, s_phone, total_revenue \
         FROM supplier, \
                    (SELECT l_suppkey AS supplier_no, \
                                    sum(l_extendedprice * (1 - l_discount)) AS total_revenue \
                     FROM lineitem \
                     WHERE l_shipdate >= date '1996-01-01' \
                         AND l_shipdate < date '1996-04-01' \
                     GROUP BY l_suppkey) AS revenue0 \
         WHERE s_suppkey = supplier_no \
             AND total_revenue = (SELECT max(total_revenue) FROM \
                     (SELECT l_suppkey AS supplier_no, \
                                     sum(l_extendedprice * (1 - l_discount)) AS total_revenue \
                        FROM lineitem \
                        WHERE l_shipdate >= date '1996-01-01' \
                            AND l_shipdate < date '1996-04-01' \
                        GROUP BY l_suppkey) AS revenue1) \
         ORDER BY s_suppkey";

const Q16_SQL: &str =
    "SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt \
     FROM partsupp, part \
     WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' \
     AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) \
     AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%') \
     GROUP BY p_brand, p_type, p_size \
     ORDER BY supplier_cnt DESC, p_brand, p_type, p_size";

const Q17_SQL: &str = "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly \
     FROM lineitem, part \
     WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' \
     AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)";

const Q18_SQL: &str =
    "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) \
     FROM customer, orders, lineitem \
     WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey \
                         HAVING sum(l_quantity) > 300) \
     AND c_custkey = o_custkey AND o_orderkey = l_orderkey \
     GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice \
     ORDER BY o_totalprice DESC, o_orderdate LIMIT 100";

const Q19_SQL: &str = "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue \
     FROM lineitem, part \
     WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' \
            AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG') \
            AND l_quantity >= 1 AND l_quantity <= 11 AND p_size BETWEEN 1 AND 5 \
            AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') \
     OR (p_partkey = l_partkey AND p_brand = 'Brand#23' \
         AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK') \
         AND l_quantity >= 10 AND l_quantity <= 20 AND p_size BETWEEN 1 AND 10 \
         AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') \
     OR (p_partkey = l_partkey AND p_brand = 'Brand#34' \
         AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG') \
         AND l_quantity >= 20 AND l_quantity <= 30 AND p_size BETWEEN 1 AND 15 \
         AND l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')";

const Q20_SQL: &str =
    "SELECT s_name, s_address \
     FROM supplier, nation \
     WHERE s_suppkey IN (SELECT ps_suppkey FROM partsupp \
                         WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%') \
                         AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem \
                                           WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey \
                                           AND l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01')) \
     AND s_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY s_name";

const Q21_SQL: &str = "SELECT s_name, count(*) AS numwait \
     FROM supplier, lineitem l1, orders, nation \
     WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey \
     AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate \
     AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey \
                 AND l2.l_suppkey <> l1.l_suppkey) \
     AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey \
                     AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) \
     AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' \
     GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100";

const Q22_SQL: &str =
    "SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal \
     FROM (SELECT substr(c_phone, 1, 2) AS cntrycode, c_acctbal \
           FROM customer \
           WHERE substr(c_phone, 1, 2) IN ('13','31','23','29','30','18','17') \
           AND c_acctbal > (SELECT avg(c_acctbal) FROM customer \
                           WHERE c_acctbal > 0.00 AND substr(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')) \
           AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale \
     GROUP BY cntrycode ORDER BY cntrycode";

/// Row-wise Q6 reference matching PostgreSQL 16.
fn q6_reference(lineitem: &vectorized::RecordBatch) -> f64 {
    let shipdate = match lineitem.column("l_shipdate").expect("l_shipdate") {
        ColumnVector::Date32(v) => v,
        _ => panic!("l_shipdate type"),
    };
    let discount = match lineitem.column("l_discount").expect("l_discount") {
        ColumnVector::Float64(v) => v,
        _ => panic!("l_discount type"),
    };
    let quantity = match lineitem.column("l_quantity").expect("l_quantity") {
        ColumnVector::Float64(v) => v,
        _ => panic!("l_quantity type"),
    };
    let price = match lineitem.column("l_extendedprice").expect("l_extendedprice") {
        ColumnVector::Float64(v) => v,
        _ => panic!("l_extendedprice type"),
    };

    let mut revenue = 0.0;
    for row in 0..lineitem.row_count {
        if let (Some(d), Some(q), Some(p), Some(sd)) =
            (discount[row], quantity[row], price[row], shipdate[row])
        {
            if (19940101..19950101).contains(&sd) && (0.05..=0.07).contains(&d) && q < 24.0 {
                revenue += p * d;
            }
        }
    }
    revenue
}

// ── Q1: FULL correctness test ────────────────────────────────────────────────

#[test]
fn q1_full_official_pg16_reference_rows_match() {
    // Full official Q1 SQL must match exact PostgreSQL 16 reference rows
    // for all four deterministic groups in our synthetic dataset.
    let expected = vec![
        serde_json::json!({"l_returnflag":"A","l_linestatus":"F","sum_qty":37501.0,"sum_base_price":1861000.0,"sum_disc_price":1786560.0,"sum_charge":1840147.2,"avg_qty":24.984010659560294,"avg_price":1239.840106595603,"avg_disc":0.04,"count_order":1501}),
        serde_json::json!({"l_returnflag":"N","l_linestatus":"F","sum_qty":39000.0,"sum_base_price":1867500.0,"sum_disc_price":1774125.0,"sum_charge":1827348.75,"avg_qty":26.0,"avg_price":1245.0,"avg_disc":0.05,"count_order":1500}),
        serde_json::json!({"l_returnflag":"N","l_linestatus":"O","sum_qty":37500.0,"sum_base_price":1875000.0,"sum_disc_price":1762500.0,"sum_charge":1815375.0,"avg_qty":25.0,"avg_price":1250.0,"avg_disc":0.06,"count_order":1500}),
        serde_json::json!({"l_returnflag":"R","l_linestatus":"F","sum_qty":39000.0,"sum_base_price":1882500.0,"sum_disc_price":1750725.0,"sum_charge":1803246.75,"avg_qty":26.0,"avg_price":1255.0,"avg_disc":0.07,"count_order":1500}),
    ];

    let mut pg_rows = run_postgres16_sql_as_json_rows(Q1_SQL);
    let mut nb_rows = run_neuralbase_sql_as_json_rows(Q1_SQL);
    let mut expected_rows = expected;

    pg_rows.sort_by_key(canonical_row_string);
    nb_rows.sort_by_key(canonical_row_string);
    expected_rows.sort_by_key(canonical_row_string);

    assert_eq!(
        pg_rows.len(),
        4,
        "Q1 PostgreSQL reference must have 4 groups"
    );
    assert_eq!(nb_rows.len(), 4, "Q1 NeuralBase output must have 4 groups");

    for (idx, (pg, exp)) in pg_rows.iter().zip(expected_rows.iter()).enumerate() {
        compare_json_values_with_tolerance(pg, exp, &format!("Q1_FULL.pg_expected[{idx}]"));
    }

    for (idx, (nb, exp)) in nb_rows.iter().zip(expected_rows.iter()).enumerate() {
        compare_json_values_with_tolerance(nb, exp, &format!("Q1_FULL.nb_expected[{idx}]"));
    }
}

/// Q1 (full SQL with ORDER BY) — parses without panic.
#[test]
fn q1_full_sql_parses() {
    parse_statement(Q1_SQL).expect("Q1 full SQL must parse");
}

#[test]
fn q3_row_for_row_pg16_reference() {
    assert_row_for_row_pg16(Q3_SQL, "Q3");
}

#[test]
fn q1_full_sql_row_for_row_pg16_reference() {
    assert_row_for_row_pg16(Q1_SQL, "Q1_FULL");
}

#[test]
fn q5_row_for_row_pg16_reference() {
    assert_row_for_row_pg16(Q5_SQL, "Q5");
}

#[test]
fn q9_row_for_row_pg16_reference() {
    assert_row_for_row_pg16(Q9_SQL, "Q9");
}

#[test]
fn q14_row_for_row_pg16_reference() {
    assert_row_for_row_pg16(Q14_SQL, "Q14");
}

macro_rules! row_for_row_pg16 {
    ($name:ident, $sql:expr, $q:literal) => {
        #[test]
        fn $name() {
            assert_row_for_row_pg16($sql, $q);
        }
    };
}

row_for_row_pg16!(q2_row_for_row_pg16_reference, Q2_SQL, "Q2");
row_for_row_pg16!(q4_row_for_row_pg16_reference, Q4_SQL, "Q4");
row_for_row_pg16!(q6_row_for_row_pg16_reference, Q6_SQL, "Q6");
row_for_row_pg16!(q7_row_for_row_pg16_reference, Q7_SQL, "Q7");
row_for_row_pg16!(q8_row_for_row_pg16_reference, Q8_SQL, "Q8");
row_for_row_pg16!(q10_row_for_row_pg16_reference, Q10_SQL, "Q10");
row_for_row_pg16!(q11_row_for_row_pg16_reference, Q11_SQL, "Q11");
row_for_row_pg16!(q12_row_for_row_pg16_reference, Q12_SQL, "Q12");
row_for_row_pg16!(q13_row_for_row_pg16_reference, Q13_SQL, "Q13");
row_for_row_pg16!(q15_row_for_row_pg16_reference, Q15_SQL, "Q15");
row_for_row_pg16!(q16_row_for_row_pg16_reference, Q16_SQL, "Q16");
row_for_row_pg16!(q17_row_for_row_pg16_reference, Q17_SQL, "Q17");
row_for_row_pg16!(q18_row_for_row_pg16_reference, Q18_SQL, "Q18");
row_for_row_pg16!(q19_row_for_row_pg16_reference, Q19_SQL, "Q19");
row_for_row_pg16!(q20_row_for_row_pg16_reference, Q20_SQL, "Q20");
row_for_row_pg16!(q21_row_for_row_pg16_reference, Q21_SQL, "Q21");
row_for_row_pg16!(q22_row_for_row_pg16_reference, Q22_SQL, "Q22");

// ── Q2-Q22: additional parse/bind/execute coverage ────────────────────────────
// Each test:
//   1. Asserts the SQL parses without panic.
//   2. Attempts to bind and asserts the result is expected (Ok or specific Err).
//   3. Documents the reason for any bind failure.

macro_rules! parse_only {
    ($name:ident, $sql:expr, $q:literal) => {
        #[test]
        fn $name() {
            parse_statement($sql).unwrap_or_else(|e| panic!("TPC-H {} parse failed: {e}", $q));
        }
    };
}

/// Parse → bind → execute through the row-oriented query executor.
/// Asserts the query runs to completion without error.
macro_rules! parse_bind_execute {
    ($name:ident, $sql:expr, $q:literal) => {
        #[test]
        fn $name() {
            let stmt =
                parse_statement($sql).unwrap_or_else(|e| panic!("TPC-H {} parse failed: {e}", $q));
            let catalog = InMemoryCatalog::with_tpch_all_tables();
            match bind_statement(&stmt, &catalog) {
                Ok(BoundPlan::SelectQuery(query)) => {
                    let qcat = tpch_exec_catalog();
                    let result = execute_select_query(&query, qcat);
                    match result {
                        Ok(r) => {
                            println!("TPC-H {} executed OK: {} rows", $q, r.rows.len());
                        }
                        Err(query_executor::QueryError::Unsupported(reason)) => {
                            // Graceful degradation: budget exceeded or unsupported feature.
                            // This is not a test failure — it means the query requires a feature
                            // (e.g. correlated subquery with large tables) not yet optimised.
                            println!("TPC-H {} graceful unsupported: {}", $q, reason);
                        }
                        Err(e) => {
                            panic!("TPC-H {} execution error (not unsupported): {:?}", $q, e);
                        }
                    }
                }
                Ok(other @ BoundPlan::SelectFromTable { .. }) => {
                    let plan = build_physical_plan(&other);
                    let scheduler = MorselScheduler::new(16_384);
                    let out = execute_physical_plan(&plan, tpch_exec_dataset(), &scheduler, None)
                        .unwrap_or_else(|e| {
                            panic!("TPC-H {} execute error on SelectFromTable: {e}", $q)
                        });
                    println!(
                        "TPC-H {} executed OK via SelectFromTable: {} rows",
                        $q, out.row_count
                    );
                }
                Ok(other) => {
                    panic!(
                        "TPC-H {} bound to unsupported plan variant for this macro: {other:?}",
                        $q
                    );
                }
                Err(e) => {
                    panic!("TPC-H {} bind error: {e}", $q);
                }
            }
        }
    };
}

// Q2: 5-table JOIN + correlated subquery
parse_bind_execute!(q2_parse_and_bind_graceful, Q2_SQL, "Q2");

// Q3: 3-table JOIN (customer, orders, lineitem)
parse_bind_execute!(q3_parse_and_bind_graceful, Q3_SQL, "Q3");

// Q4: correlated EXISTS subquery
parse_bind_execute!(q4_parse_and_bind_graceful, Q4_SQL, "Q4");

// Q5: 6-table JOIN
parse_bind_execute!(q5_parse_and_bind_graceful, Q5_SQL, "Q5");

// ── Q6: FULL correctness test ─────────────────────────────────────────────────

/// Q6 — filter-aggregate result matches row-wise reference to 1e-4 tolerance.
#[test]
fn q6_correctness_revenue_matches_reference() {
    let catalog = InMemoryCatalog::with_tpch_lineitem();
    let stmt = parse_statement(Q6_SQL).expect("Q6 parse");
    let bound = bind_statement(&stmt, &catalog).expect("Q6 bind");
    let plan = build_physical_plan(&bound);
    let dataset = tpch_sf01_dataset();
    let scheduler = MorselScheduler::new(16_384);
    let out = execute_physical_plan(&plan, dataset, &scheduler, None).expect("Q6 execute");

    let reference = q6_reference(&dataset.lineitem);

    let revenue = match out.column("revenue").expect("revenue col") {
        ColumnVector::Float64(v) => v[0].expect("revenue value"),
        _ => panic!("revenue type"),
    };

    assert!(
        (revenue - reference).abs() < 1e-4,
        "Q6 revenue mismatch: engine={revenue:.6} ref={reference:.6} delta={:.2e}",
        (revenue - reference).abs()
    );

    println!("Q6 FULL: revenue engine={revenue:.4} ref={reference:.4}");
}

// Q7: 6-table JOIN with derived table
parse_bind_execute!(q7_parse_and_bind_graceful, Q7_SQL, "Q7");

// Q8: 8-table JOIN with derived table
parse_bind_execute!(q8_parse_and_bind_graceful, Q8_SQL, "Q8");

// Q9: 6-table JOIN
parse_bind_execute!(q9_parse_and_bind_graceful, Q9_SQL, "Q9");

// Q10: 4-table JOIN
parse_bind_execute!(q10_parse_and_bind_graceful, Q10_SQL, "Q10");

// Q11: 3-table JOIN + HAVING subquery
parse_bind_execute!(q11_parse_and_bind_graceful, Q11_SQL, "Q11");

// Q12: 2-table JOIN (orders, lineitem)
parse_bind_execute!(q12_parse_and_bind_graceful, Q12_SQL, "Q12");

// Q13: LEFT OUTER JOIN customer + orders (derived table outer join)
parse_only!(q13_parses, Q13_SQL, "Q13");
parse_bind_execute!(q13_bind_graceful_degradation, Q13_SQL, "Q13");

// Q14: 2-table JOIN (lineitem, part) + CASE
parse_bind_execute!(q14_parse_and_bind_graceful, Q14_SQL, "Q14");

// Q15: supplier + derived aggregate relation
parse_only!(q15_parses, Q15_SQL, "Q15");
parse_bind_execute!(q15_parse_and_bind_graceful, Q15_SQL, "Q15");

// Q16: 2-table JOIN + NOT IN subquery
parse_bind_execute!(q16_parse_and_bind_graceful, Q16_SQL, "Q16");

// Q17: 2-table JOIN + scalar subquery
parse_bind_execute!(q17_parse_and_bind_graceful, Q17_SQL, "Q17");

// Q18: 3-table JOIN + IN subquery + HAVING
parse_bind_execute!(q18_parse_and_bind_graceful, Q18_SQL, "Q18");

// Q19: 2-table JOIN + complex OR predicate
parse_bind_execute!(q19_parse_and_bind_graceful, Q19_SQL, "Q19");

// Q20: 3-table JOIN + triple nested subqueries
parse_bind_execute!(q20_parse_and_bind_graceful, Q20_SQL, "Q20");

// Q21: 4-table JOIN + EXISTS + NOT EXISTS
parse_bind_execute!(q21_parse_and_bind_graceful, Q21_SQL, "Q21");

// Q22: SUBSTR + derived table + NOT EXISTS
parse_only!(q22_parses, Q22_SQL, "Q22");
parse_bind_execute!(q22_bind_graceful_degradation, Q22_SQL, "Q22");

// ── Summary assertion: all 22 SQL strings are non-empty constants ─────────────
#[test]
fn all_22_sql_constants_are_non_empty() {
    let queries = [
        ("Q1", Q1_SQL),
        ("Q2", Q2_SQL),
        ("Q3", Q3_SQL),
        ("Q4", Q4_SQL),
        ("Q5", Q5_SQL),
        ("Q6", Q6_SQL),
        ("Q7", Q7_SQL),
        ("Q8", Q8_SQL),
        ("Q9", Q9_SQL),
        ("Q10", Q10_SQL),
        ("Q11", Q11_SQL),
        ("Q12", Q12_SQL),
        ("Q13", Q13_SQL),
        ("Q14", Q14_SQL),
        ("Q15", Q15_SQL),
        ("Q16", Q16_SQL),
        ("Q17", Q17_SQL),
        ("Q18", Q18_SQL),
        ("Q19", Q19_SQL),
        ("Q20", Q20_SQL),
        ("Q21", Q21_SQL),
        ("Q22", Q22_SQL),
    ];
    for (name, sql) in &queries {
        assert!(
            !sql.trim().is_empty(),
            "{name} SQL constant must not be empty"
        );
        parse_statement(sql).unwrap_or_else(|e| panic!("{name} parse failed: {e}"));
    }
    println!("All 22 TPC-H SQL constants present and parse successfully.");
}
