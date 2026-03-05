// SPDX-License-Identifier: Apache-2.0
// TPC-H synthetic dataset — Session 9 extended to all 8 tables.
//
// Date format: YYYYMMDD integers (e.g. 19950315 for 1995-03-15).
// This matches the date literal conversion in query_executor.rs
// (TypedString `date '1995-03-15'` => ScalarVal::Date(19950315)).
//
// CONFIDENCE: raw=0.72 effective=0.65
// DEPENDS_ON: vectorized

use crate::vectorized::{ColumnVector, RecordBatch, Utf8Column};

// -- TpchDataSet ---------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TpchDataSet {
    pub lineitem: RecordBatch,
    pub orders:   RecordBatch,
    pub customer: RecordBatch,
    pub nation:   RecordBatch,
    pub region:   RecordBatch,
    pub part:     RecordBatch,
    pub supplier: RecordBatch,
    pub partsupp: RecordBatch,
}

pub fn generate_tpch_data(scale_factor: f64) -> TpchDataSet {
    let li_rows = lineitem_rows_for_scale(scale_factor);
    TpchDataSet {
        lineitem: build_lineitem(li_rows),
        orders:   build_orders(),
        customer: build_customer(),
        nation:   build_nation(),
        region:   build_region(),
        part:     build_part(),
        supplier: build_supplier(),
        partsupp: build_partsupp(),
    }
}

fn lineitem_rows_for_scale(sf: f64) -> usize {
    let sf = if sf.is_finite() { sf.max(0.0001) } else { 0.1 };
    (6_001_215.0_f64 * sf).round() as usize
}

// -- LINEITEM (15 columns) -----------------------------------------------------

// Shipdates covering 1992-1998 in YYYYMMDD format.
static SHIP_DATES: &[i32] = &[
    19920115, 19920301, 19920728, 19920915, 19921201,
    19930210, 19930703, 19931115, 19931201, 19930501,
    19940202, 19940315, 19940630, 19940901, 19941130,
    19950101, 19950316, 19950630, 19950901, 19951201,
    19960101, 19960315, 19960630, 19960901, 19961201,
    19970101, 19970601, 19971201,
    19980101, 19980830,
];

static COMMIT_DATES: &[i32] = &[
    19920110, 19920225, 19920720, 19920910, 19921120,
    19930205, 19930628, 19931110, 19931125, 19930425,
    19940130, 19940310, 19940625, 19940825, 19941120,
    19941228, 19950310, 19950620, 19950825, 19951120,
    19951228, 19960310, 19960620, 19960825, 19961120,
    19961228, 19970520, 19971120,
    19971228, 19980820,
];

static RECEIPT_DATES: &[i32] = &[
    19920125, 19920315, 19920810, 19920930, 19921215,
    19930225, 19930720, 19931130, 19931215, 19930515,
    19940215, 19940330, 19940715, 19940920, 19941215,
    19950120, 19950330, 19950715, 19950920, 19951215,
    19960120, 19960330, 19960715, 19960920, 19961215,
    19970120, 19970620, 19971215,
    19980320, 19980915,
];

static SHIPMODES: &[&str] = &["AIR", "MAIL", "SHIP", "TRUCK", "RAIL", "REG AIR", "FOB"];
static SHIPINSTRUCT: &[&str] = &[
    "DELIVER IN PERSON",
    "COLLECT COD",
    "NONE",
    "TAKE BACK RETURN",
];

fn build_lineitem(rows: usize) -> RecordBatch {
    let nd = SHIP_DATES.len();
    let mut l_orderkey      = Vec::with_capacity(rows);
    let mut l_partkey       = Vec::with_capacity(rows);
    let mut l_suppkey       = Vec::with_capacity(rows);
    let mut l_linenumber    = Vec::with_capacity(rows);
    let mut l_quantity      = Vec::with_capacity(rows);
    let mut l_extendedprice = Vec::with_capacity(rows);
    let mut l_discount      = Vec::with_capacity(rows);
    let mut l_tax           = Vec::with_capacity(rows);
    let mut l_returnflag    = Vec::with_capacity(rows);
    let mut l_linestatus    = Vec::with_capacity(rows);
    let mut l_shipdate      = Vec::with_capacity(rows);
    let mut l_commitdate    = Vec::with_capacity(rows);
    let mut l_receiptdate   = Vec::with_capacity(rows);
    let mut l_shipmode      = Vec::with_capacity(rows);
    let mut l_shipinstruct  = Vec::with_capacity(rows);

    for index in 0..rows {
        let i  = index as i64;
        let di = index % nd;

        l_orderkey     .push(Some((i / 3) + 1));
        l_partkey      .push(Some((i % 10) + 1));
        l_suppkey      .push(Some((i % 5) + 1));
        l_linenumber   .push(Some((i % 3 + 1) as i32));
        l_quantity     .push(Some(((i % 50) as f64) + 1.0));
        l_extendedprice.push(Some(1000.0 + ((i % 100) as f64) * 5.0));
        l_discount     .push(Some(0.04 + ((i % 4) as f64) * 0.01));
        l_tax          .push(Some(0.02 + ((i % 3) as f64) * 0.01));
        l_shipdate     .push(Some(SHIP_DATES[di]));
        l_commitdate   .push(Some(COMMIT_DATES[di]));
        l_receiptdate  .push(Some(RECEIPT_DATES[di]));
        match i % 4 {
            0 => {
                l_returnflag.push(Some("A"));
                l_linestatus.push(Some("F"));
            }
            1 => {
                l_returnflag.push(Some("N"));
                l_linestatus.push(Some("F"));
            }
            2 => {
                l_returnflag.push(Some("N"));
                l_linestatus.push(Some("O"));
            }
            _ => {
                l_returnflag.push(Some("R"));
                l_linestatus.push(Some("F"));
            }
        }
        l_shipmode     .push(Some(SHIPMODES[index % SHIPMODES.len()]));
        l_shipinstruct .push(Some(SHIPINSTRUCT[index % SHIPINSTRUCT.len()]));
    }

    RecordBatch::new(vec![
        ("l_orderkey".to_string(),      ColumnVector::Int64(l_orderkey)),
        ("l_partkey".to_string(),       ColumnVector::Int64(l_partkey)),
        ("l_suppkey".to_string(),       ColumnVector::Int64(l_suppkey)),
        ("l_linenumber".to_string(),    ColumnVector::Int32(l_linenumber)),
        ("l_quantity".to_string(),      ColumnVector::Float64(l_quantity)),
        ("l_extendedprice".to_string(), ColumnVector::Float64(l_extendedprice)),
        ("l_discount".to_string(),      ColumnVector::Float64(l_discount)),
        ("l_tax".to_string(),           ColumnVector::Float64(l_tax)),
        ("l_returnflag".to_string(),    ColumnVector::Utf8(Utf8Column::from_options(l_returnflag))),
        ("l_linestatus".to_string(),    ColumnVector::Utf8(Utf8Column::from_options(l_linestatus))),
        ("l_shipdate".to_string(),      ColumnVector::Date32(l_shipdate)),
        ("l_commitdate".to_string(),    ColumnVector::Date32(l_commitdate)),
        ("l_receiptdate".to_string(),   ColumnVector::Date32(l_receiptdate)),
        ("l_shipmode".to_string(),      ColumnVector::Utf8(Utf8Column::from_options(l_shipmode))),
        ("l_shipinstruct".to_string(),  ColumnVector::Utf8(Utf8Column::from_options(l_shipinstruct))),
    ])
    .expect("lineitem batch must build")
}

// -- ORDERS (20 rows) ----------------------------------------------------------

static ORDER_DATES: &[i32] = &[
    19930115, 19930705, 19930215, 19940101, 19940901,
    19940305, 19950101, 19950316, 19951101, 19960201,
    19920501, 19921201, 19930801, 19940601, 19941201,
    19950101, 19960101, 19960701, 19970101, 19971201,
];
static ORDER_STATUS: &[&str] = &[
    "F","F","F","F","F","F","O","O","O","O",
    "F","F","F","F","F","O","O","O","O","O",
];
static ORDER_PRIORITY: &[&str] = &[
    "1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW",
    "1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW",
    "1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW",
    "1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW",
];

fn build_orders() -> RecordBatch {
    let num = 20usize;
    let mut o_orderkey      = Vec::with_capacity(num);
    let mut o_custkey       = Vec::with_capacity(num);
    let mut o_orderstatus   = Vec::with_capacity(num);
    let mut o_totalprice    = Vec::with_capacity(num);
    let mut o_orderdate     = Vec::with_capacity(num);
    let mut o_orderpriority = Vec::with_capacity(num);
    let mut o_shippriority  = Vec::with_capacity(num);
    let mut o_comment       = Vec::with_capacity(num);

    for i in 0..num {
        o_orderkey     .push(Some((i as i64) + 1));
        o_custkey      .push(Some(((i % 10) as i64) + 1));
        o_orderstatus  .push(Some(ORDER_STATUS[i]));
        o_totalprice   .push(Some(5000.0 + (i as f64) * 250.0));
        o_orderdate    .push(Some(ORDER_DATES[i]));
        o_orderpriority.push(Some(ORDER_PRIORITY[i]));
        o_shippriority .push(Some(0i32));
        o_comment      .push(Some(format!("orders comment #{:02}", i + 1)));
    }

    RecordBatch::new(vec![
        ("o_orderkey".to_string(),      ColumnVector::Int64(o_orderkey)),
        ("o_custkey".to_string(),       ColumnVector::Int64(o_custkey)),
        ("o_orderstatus".to_string(),   ColumnVector::Utf8(Utf8Column::from_options(o_orderstatus))),
        ("o_totalprice".to_string(),    ColumnVector::Float64(o_totalprice)),
        ("o_orderdate".to_string(),     ColumnVector::Date32(o_orderdate)),
        ("o_orderpriority".to_string(), ColumnVector::Utf8(Utf8Column::from_options(o_orderpriority))),
        ("o_shippriority".to_string(),  ColumnVector::Int32(o_shippriority)),
        ("o_comment".to_string(),       ColumnVector::Utf8(Utf8Column::from_owned_options(o_comment))),
    ])
    .expect("orders batch must build")
}

// -- CUSTOMER (10 rows) --------------------------------------------------------

static MKTSEGS: &[&str] = &[
    "BUILDING","BUILDING","BUILDING","AUTOMOBILE","AUTOMOBILE",
    "HOUSEHOLD","HOUSEHOLD","MACHINERY","BUILDING","FURNITURE",
];
static PHONES: &[&str] = &[
    "13-111-111-1111","31-222-222-2222","23-333-333-3333",
    "29-444-444-4444","30-555-555-5555","18-666-666-6666",
    "17-777-777-7777","14-888-888-8888","33-999-999-9999",
    "25-000-000-0000",
];

fn build_customer() -> RecordBatch {
    let num = 10usize;
    let nation_cycle: &[i64] = &[6, 7, 1, 2, 3, 8, 9, 20, 6, 7];
    let mut c_custkey    = Vec::with_capacity(num);
    let mut c_name       = Vec::with_capacity(num);
    let mut c_nationkey  = Vec::with_capacity(num);
    let mut c_mktsegment = Vec::with_capacity(num);
    let mut c_acctbal    = Vec::with_capacity(num);
    let mut c_phone      = Vec::with_capacity(num);
    let mut c_address    = Vec::with_capacity(num);
    let mut c_comment    = Vec::with_capacity(num);

    for i in 0..num {
        c_custkey   .push(Some((i as i64) + 1));
        c_name      .push(Some(format!("Customer#{:08}", i + 1)));
        c_nationkey .push(Some(nation_cycle[i]));
        c_mktsegment.push(Some(MKTSEGS[i]));
        c_acctbal   .push(Some(100.0 + (i as f64) * 150.0));
        c_phone     .push(Some(PHONES[i].to_string()));
        c_address   .push(Some(format!("Customer Address #{:02}", i + 1)));
        c_comment   .push(Some(format!("customer comment #{:02}", i + 1)));
    }

    RecordBatch::new(vec![
        ("c_custkey".to_string(),    ColumnVector::Int64(c_custkey)),
        ("c_name".to_string(),       ColumnVector::Utf8(Utf8Column::from_owned_options(c_name))),
        ("c_nationkey".to_string(),  ColumnVector::Int64(c_nationkey)),
        ("c_mktsegment".to_string(), ColumnVector::Utf8(Utf8Column::from_options(c_mktsegment))),
        ("c_acctbal".to_string(),    ColumnVector::Float64(c_acctbal)),
        ("c_phone".to_string(),      ColumnVector::Utf8(Utf8Column::from_owned_options(c_phone))),
        ("c_address".to_string(),    ColumnVector::Utf8(Utf8Column::from_owned_options(c_address))),
        ("c_comment".to_string(),    ColumnVector::Utf8(Utf8Column::from_owned_options(c_comment))),
    ])
    .expect("customer batch must build")
}

// -- NATION (25 rows -- full TPC-H set) ----------------------------------------

static NATIONS: &[(i64, &str, i64)] = &[
    (0,  "ALGERIA",          0),
    (1,  "ARGENTINA",        1),
    (2,  "BRAZIL",           1),
    (3,  "CANADA",           1),
    (4,  "EGYPT",            4),
    (5,  "ETHIOPIA",         0),
    (6,  "FRANCE",           3),
    (7,  "GERMANY",          3),
    (8,  "INDIA",            2),
    (9,  "INDONESIA",        2),
    (10, "IRAN",             4),
    (11, "IRAQ",             4),
    (12, "JAPAN",            2),
    (13, "JORDAN",           4),
    (14, "KENYA",            0),
    (15, "MOROCCO",          0),
    (16, "MOZAMBIQUE",       0),
    (17, "PERU",             1),
    (18, "CHINA",            2),
    (19, "ROMANIA",          3),
    (20, "SAUDI ARABIA",     4),
    (21, "VIETNAM",          2),
    (22, "RUSSIA",           3),
    (23, "UNITED KINGDOM",   3),
    (24, "UNITED STATES",    1),
];

fn build_nation() -> RecordBatch {
    let mut n_nationkey = Vec::with_capacity(NATIONS.len());
    let mut n_name      = Vec::with_capacity(NATIONS.len());
    let mut n_regionkey = Vec::with_capacity(NATIONS.len());

    for &(key, name, rkey) in NATIONS {
        n_nationkey.push(Some(key));
        n_name     .push(Some(name));
        n_regionkey.push(Some(rkey));
    }

    RecordBatch::new(vec![
        ("n_nationkey".to_string(), ColumnVector::Int64(n_nationkey)),
        ("n_name".to_string(),      ColumnVector::Utf8(Utf8Column::from_options(n_name))),
        ("n_regionkey".to_string(), ColumnVector::Int64(n_regionkey)),
    ])
    .expect("nation batch must build")
}

// -- REGION (5 rows) -----------------------------------------------------------

static REGIONS: &[(i64, &str)] = &[
    (0, "AFRICA"),
    (1, "AMERICA"),
    (2, "ASIA"),
    (3, "EUROPE"),
    (4, "MIDDLE EAST"),
];

fn build_region() -> RecordBatch {
    let mut r_regionkey = Vec::with_capacity(REGIONS.len());
    let mut r_name      = Vec::with_capacity(REGIONS.len());

    for &(key, name) in REGIONS {
        r_regionkey.push(Some(key));
        r_name     .push(Some(name));
    }

    RecordBatch::new(vec![
        ("r_regionkey".to_string(), ColumnVector::Int64(r_regionkey)),
        ("r_name".to_string(),      ColumnVector::Utf8(Utf8Column::from_options(r_name))),
    ])
    .expect("region batch must build")
}

// -- PART (10 rows) ------------------------------------------------------------

// (key, name, brand, type, size, container, retailprice)
type PartRow = (i64, &'static str, &'static str, &'static str, &'static str, i32, &'static str, f64);
static PARTS: &[PartRow] = &[
    (1,  "goldenrod lavender spring peru", "MFGR#1", "Brand#13", "PROMO BURNISHED COPPER",   15, "JUMBO PKG",  901.0),
    (2,  "spring green frosted powder",    "MFGR#2", "Brand#23", "MEDIUM ANODIZED COPPER",   15, "MED BOX",   902.0),
    (3,  "almond antique salmon hot",      "MFGR#3", "Brand#45", "STANDARD POLISHED STEEL",  49, "SM CASE",   903.0),
    (4,  "cream spring sky wheat",         "MFGR#1", "Brand#12", "MEDIUM BURNISHED BRASS",    5, "WRAP CASE", 904.0),
    (5,  "forest lime green powder",       "MFGR#2", "Brand#23", "SMALL BURNISHED COPPER",   14, "SM BOX",    905.0),
    (6,  "bisque slate burnished steel",   "MFGR#4", "Brand#34", "LARGE PLATED BRASS",       23, "LG PKG",    906.0),
    (7,  "peru steel forest green linen",  "MFGR#1", "Brand#15", "ECONOMY BURNISHED NICKEL", 45, "WRAP BAG",  907.0),
    (8,  "yellow cornflower sienna wheat", "MFGR#5", "Brand#11", "STANDARD BRUSHED BRASS",    3, "JUMBO CAN", 908.0),
    (9,  "olive green moccasin lace",      "MFGR#4", "Brand#34", "MEDIUM POLISHED TIN",      19, "SM CAN",    909.0),
    (10, "slate pink black wheat chiffon", "MFGR#6", "Brand#55", "LARGE POLISHED COPPER",    36, "MED PKG",   910.0),
];

fn build_part() -> RecordBatch {
    let mut p_partkey     = Vec::with_capacity(PARTS.len());
    let mut p_name        = Vec::with_capacity(PARTS.len());
    let mut p_mfgr        = Vec::with_capacity(PARTS.len());
    let mut p_brand       = Vec::with_capacity(PARTS.len());
    let mut p_type        = Vec::with_capacity(PARTS.len());
    let mut p_size        = Vec::with_capacity(PARTS.len());
    let mut p_container   = Vec::with_capacity(PARTS.len());
    let mut p_retailprice = Vec::with_capacity(PARTS.len());

    for &(k, n, m, b, t, s, c, r) in PARTS {
        p_partkey    .push(Some(k));
        p_name       .push(Some(n));
        p_mfgr       .push(Some(m));
        p_brand      .push(Some(b));
        p_type       .push(Some(t));
        p_size       .push(Some(s));
        p_container  .push(Some(c));
        p_retailprice.push(Some(r));
    }

    RecordBatch::new(vec![
        ("p_partkey".to_string(),     ColumnVector::Int64(p_partkey)),
        ("p_name".to_string(),        ColumnVector::Utf8(Utf8Column::from_options(p_name))),
        ("p_mfgr".to_string(),        ColumnVector::Utf8(Utf8Column::from_options(p_mfgr))),
        ("p_brand".to_string(),       ColumnVector::Utf8(Utf8Column::from_options(p_brand))),
        ("p_type".to_string(),        ColumnVector::Utf8(Utf8Column::from_options(p_type))),
        ("p_size".to_string(),        ColumnVector::Int32(p_size)),
        ("p_container".to_string(),   ColumnVector::Utf8(Utf8Column::from_options(p_container))),
        ("p_retailprice".to_string(), ColumnVector::Float64(p_retailprice)),
    ])
    .expect("part batch must build")
}

// -- SUPPLIER (5 rows) ---------------------------------------------------------

type SupplierRow = (i64, &'static str, i64, f64, &'static str, &'static str, &'static str);
static SUPPLIERS: &[SupplierRow] = &[
    (1, "Supplier#000000001",  7, 100.75, "Address 1", "17-100-000-0001", "Supplier 1 comment"),
    (2, "Supplier#000000002",  6, 200.50, "Address 2", "18-200-000-0002", "Supplier 2 comment"),
    (3, "Supplier#000000003",  8, 150.25, "Address 3", "13-300-000-0003", "Supplier 3 comment"),
    (4, "Supplier#000000004",  9,  80.00, "Address 4", "31-400-000-0004", "Supplier 4 comment"),
    (5, "Supplier#000000005", 20, 120.30, "Address 5", "23-500-000-0005", "Supplier 5 comment"),
];

fn build_supplier() -> RecordBatch {
    let mut s_suppkey   = Vec::with_capacity(SUPPLIERS.len());
    let mut s_name      = Vec::with_capacity(SUPPLIERS.len());
    let mut s_nationkey = Vec::with_capacity(SUPPLIERS.len());
    let mut s_acctbal   = Vec::with_capacity(SUPPLIERS.len());
    let mut s_address   = Vec::with_capacity(SUPPLIERS.len());
    let mut s_phone     = Vec::with_capacity(SUPPLIERS.len());
    let mut s_comment   = Vec::with_capacity(SUPPLIERS.len());

    for &(k, n, nk, a, addr, phone, comment) in SUPPLIERS {
        s_suppkey  .push(Some(k));
        s_name     .push(Some(n));
        s_nationkey.push(Some(nk));
        s_acctbal  .push(Some(a));
        s_address  .push(Some(addr));
        s_phone    .push(Some(phone));
        s_comment  .push(Some(comment));
    }

    RecordBatch::new(vec![
        ("s_suppkey".to_string(),   ColumnVector::Int64(s_suppkey)),
        ("s_name".to_string(),      ColumnVector::Utf8(Utf8Column::from_options(s_name))),
        ("s_nationkey".to_string(), ColumnVector::Int64(s_nationkey)),
        ("s_acctbal".to_string(),   ColumnVector::Float64(s_acctbal)),
        ("s_address".to_string(),   ColumnVector::Utf8(Utf8Column::from_options(s_address))),
        ("s_phone".to_string(),     ColumnVector::Utf8(Utf8Column::from_options(s_phone))),
        ("s_comment".to_string(),   ColumnVector::Utf8(Utf8Column::from_options(s_comment))),
    ])
    .expect("supplier batch must build")
}

// -- PARTSUPP (50 rows -- 5 suppliers x 10 parts) ------------------------------

fn build_partsupp() -> RecordBatch {
    let num = 50usize;
    let mut ps_partkey    = Vec::with_capacity(num);
    let mut ps_suppkey    = Vec::with_capacity(num);
    let mut ps_availqty   = Vec::with_capacity(num);
    let mut ps_supplycost = Vec::with_capacity(num);

    for i in 0..num {
        let partkey = ((i / 5) as i64) + 1;
        let suppkey = ((i % 5) as i64) + 1;
        ps_partkey   .push(Some(partkey));
        ps_suppkey   .push(Some(suppkey));
        ps_availqty  .push(Some((100 + i as i32) * 10));
        ps_supplycost.push(Some(50.0 + (i as f64) * 3.5));
    }

    RecordBatch::new(vec![
        ("ps_partkey".to_string(),    ColumnVector::Int64(ps_partkey)),
        ("ps_suppkey".to_string(),    ColumnVector::Int64(ps_suppkey)),
        ("ps_availqty".to_string(),   ColumnVector::Int32(ps_availqty)),
        ("ps_supplycost".to_string(), ColumnVector::Float64(ps_supplycost)),
    ])
    .expect("partsupp batch must build")
}
