//! End-to-end `FlightSQL` test: the service executes SQL over a real on-disk
//! dataset and streams Arrow results back to a standard Flight SQL client,
//! exercising both the row-count path and the FTGS `GROUP BY` pushdown over
//! the wire.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow_flight::FlightInfo;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables, SqlInfo};
use futures::TryStreamExt;
use roaring::RoaringBitmap;
use tonic::transport::Channel;

use tutankhamun_server::aggregate_cache::AggregateCache;
use tutankhamun_server::bitmap_cache::BitmapCache;
use tutankhamun_server::cache::Validation;
use tutankhamun_server::flight_sql::{self, TutankhamunFlightSqlService};
use tutankhamun_server::memory::{MemoryBudget, SessionMemoryHandle};
use tutankhamun_server::metrics::Metrics;
use tutankhamun_server::shard::DiskShardWriter;
use tutankhamun_server::shutdown::ShutdownHandle;

/// A budget large enough that the §2.2 accounting never trips in the
/// functional tests (the per-session baseline plus shard mmap fit easily).
const AMPLE_BUDGET: u64 = 1 << 30; // 1 GiB

fn bitmap(docs: impl IntoIterator<Item = u32>) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    bm.extend(docs);
    bm
}

/// One shard `s0` under `{root}/ds1`: country us→{0,2}, de→{1,3};
/// fare [100, 200, 300, 400]. So count(*) = 4, sum(fare) per country: us=400,
/// de=600.
async fn write_dataset(root: &Path) {
    let dataset = root.join("ds1");
    let shard = dataset.join("s0");
    let mut w = DiskShardWriter::new(&shard, (0, 0)).expect("new shard");
    w.add_metric("fare", vec![100, 200, 300, 400])
        .expect("fare");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0, 2]));
    country.insert("de".to_string(), bitmap([1, 3]));
    w.add_string_field("country", country).expect("country");
    w.finalize().expect("finalize");
    // Publish the dataset catalog, as ingest would — the daemon resolves the
    // shard set from the manifest.
    tutankhamun_server::ingest::write_local_manifest(&dataset)
        .await
        .expect("write manifest");
}

async fn connect(addr: SocketAddr) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    FlightSqlServiceClient::new(channel)
}

/// A client with a live session: the handshake mints a token the client
/// auto-echoes as a bearer on subsequent calls.
async fn session_client(addr: SocketAddr) -> FlightSqlServiceClient<Channel> {
    let mut client = connect(addr).await;
    client.handshake("", "").await.expect("handshake");
    client
}

/// Spawn the service over a tempfile-backed dataset; returns (addr, shutdown,
/// server task, tempdir guards).
async fn start() -> (
    SocketAddr,
    ShutdownHandle,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let (addr, shutdown, server, storage, cache, _metrics) = start_with_budget(AMPLE_BUDGET).await;
    (addr, shutdown, server, storage, cache)
}

/// Like [`start`] but with an explicit daemon memory budget (§2.2), and also
/// returns the shared `Metrics` so tests can assert the gauges/counters. The
/// per-session cap is 100% of the budget, so the global limit is the binding
/// constraint in tests.
async fn start_with_budget(
    budget: u64,
) -> (
    SocketAddr,
    ShutdownHandle,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    tempfile::TempDir,
    tempfile::TempDir,
    Arc<Metrics>,
) {
    let storage = tempfile::tempdir().expect("storage tmp");
    let cache = tempfile::tempdir().expect("cache tmp");
    write_dataset(storage.path()).await;
    let storage_url = url::Url::from_directory_path(storage.path())
        .expect("absolute path")
        .to_string();

    let shutdown = ShutdownHandle::new();
    let listener = flight_sql::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind grpc");
    let addr = listener.local_addr().expect("local addr");
    let mem = Arc::new(MemoryBudget::new(budget));
    let bitmap_cache = Arc::new(BitmapCache::new(Arc::new(SessionMemoryHandle::new(
        Arc::clone(&mem),
        budget,
    ))));
    let aggregate_cache = Arc::new(AggregateCache::new(Arc::new(SessionMemoryHandle::new(
        Arc::clone(&mem),
        budget,
    ))));
    let metrics = Metrics::new(
        Arc::clone(&mem),
        Arc::clone(&bitmap_cache),
        Arc::clone(&aggregate_cache),
    );
    let svc = TutankhamunFlightSqlService::new(
        storage_url,
        cache.path().to_path_buf(),
        u64::MAX,
        Validation::Trust,
        mem,
        100,
        bitmap_cache,
        aggregate_cache,
        Arc::clone(&metrics),
    );
    let server = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { flight_sql::serve(listener, svc, shutdown).await }
    });
    (addr, shutdown, server, storage, cache, metrics)
}

/// Run `sql` through the ad-hoc statement path (`GetFlightInfo` → `DoGet`) and
/// collect the streamed batches.
async fn run_query(client: &mut FlightSqlServiceClient<Channel>, sql: &str) -> Vec<RecordBatch> {
    let info = client
        .execute(sql.to_string(), None)
        .await
        .expect("get_flight_info");
    do_get(client, info).await
}

fn int64(batch: &RecordBatch, col: usize) -> &Int64Array {
    batch
        .column(col)
        .as_any()
        .downcast_ref()
        .expect("int64 col")
}

/// `DoGet` the single endpoint of a metadata/query `FlightInfo` and collect.
async fn do_get(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
) -> Vec<RecordBatch> {
    let ticket = info.endpoint[0].ticket.clone().expect("endpoint ticket");
    let stream = client.do_get(ticket).await.expect("do_get");
    stream.try_collect().await.expect("collect batches")
}

/// Flatten a `Utf8` column across batches into owned strings.
fn string_col(batches: &[RecordBatch], col: usize) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let a: &StringArray = b.column(col).as_any().downcast_ref().expect("utf8 col");
        for i in 0..b.num_rows() {
            out.push(a.value(i).to_string());
        }
    }
    out
}

/// Flatten a `UInt32` column across batches.
fn u32_col(batches: &[RecordBatch], col: usize) -> Vec<u32> {
    let mut out = Vec::new();
    for b in batches {
        let a: &UInt32Array = b.column(col).as_any().downcast_ref().expect("u32 col");
        out.extend((0..b.num_rows()).map(|i| a.value(i)));
    }
    out
}

/// A `CommandGetTables` with no filters and the given `include_schema`.
fn tables_cmd(include_schema: bool) -> CommandGetTables {
    CommandGetTables {
        catalog: None,
        db_schema_filter_pattern: None,
        table_name_filter_pattern: None,
        table_types: vec![],
        include_schema,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_statement_roundtrip() {
    let (addr, shutdown, server, _storage, _cache) = start().await;
    let mut client = connect(addr).await;

    let mut stmt = client
        .prepare("SELECT count(*) AS n FROM ds1".to_string(), None)
        .await
        .expect("prepare");
    // No parameter binding yet → empty parameter schema; the dataset schema is
    // the query's output (`n`).
    assert_eq!(
        stmt.parameter_schema()
            .expect("param schema")
            .fields()
            .len(),
        0,
        "no bound parameters supported"
    );
    assert_eq!(
        stmt.dataset_schema()
            .expect("dataset schema")
            .field(0)
            .name(),
        "n"
    );

    let info = stmt.execute().await.expect("execute prepared");
    let batches = do_get(&mut client, info).await;
    assert_eq!(int64(&batches[0], 0).value(0), 4, "count(*) over ds1");

    // A prepared GROUP BY rides the same FTGS pushdown as the ad-hoc path.
    let mut grp = client
        .prepare(
            "SELECT country, sum(fare) AS s FROM ds1 GROUP BY country".to_string(),
            None,
        )
        .await
        .expect("prepare group-by");
    let info = grp.execute().await.expect("execute group-by");
    let batches = do_get(&mut client, info).await;
    let mut got = BTreeMap::new();
    for b in &batches {
        let countries: &StringArray = b.column(0).as_any().downcast_ref().expect("utf8 col");
        let sums = int64(b, 1);
        for i in 0..b.num_rows() {
            got.insert(countries.value(i).to_string(), sums.value(i));
        }
    }
    assert_eq!(got.get("us"), Some(&400));
    assert_eq!(got.get("de"), Some(&600));

    // Close releases each handle on the server.
    grp.close().await.expect("close group-by");
    stmt.close().await.expect("close count");

    drop(client);
    shutdown.trigger();
    server
        .await
        .expect("server task join")
        .expect("serve returned ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_sql_info_reports_capabilities() {
    let (addr, _shutdown, _server, _storage, _cache) = start().await;
    let mut client = connect(addr).await;

    // Empty request → the full capability set; the server name flag is present.
    let info = client.get_sql_info(vec![]).await.expect("get_sql_info");
    let codes = u32_col(&do_get(&mut client, info).await, 0);
    assert!(
        codes.contains(&(SqlInfo::FlightSqlServerName as u32)),
        "{codes:?}"
    );

    // A filtered request returns only the asked-for code — the path JDBC/ADBC
    // drivers exercise when probing individual capabilities.
    let info = client
        .get_sql_info(vec![SqlInfo::FlightSqlServerName])
        .await
        .expect("filtered get_sql_info");
    let codes = u32_col(&do_get(&mut client, info).await, 0);
    assert_eq!(codes, vec![SqlInfo::FlightSqlServerName as u32]);
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_metadata_lists_datasets_schemas_and_views() {
    let (addr, shutdown, server, _storage, _cache) = start().await;

    let mut client = connect(addr).await;

    // Catalogs / schemas: the single engine namespace.
    let info = client.get_catalogs().await.expect("get_catalogs");
    let catalogs = string_col(&do_get(&mut client, info).await, 0);
    assert!(catalogs.contains(&"datafusion".to_string()), "{catalogs:?}");

    let info = client
        .get_db_schemas(CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        })
        .await
        .expect("get_db_schemas");
    let schemas = string_col(&do_get(&mut client, info).await, 1);
    assert!(schemas.contains(&"public".to_string()), "{schemas:?}");

    // Tables: the dataset on disk shows up as a TABLE (table_name col 2, type col 3).
    let info = client
        .get_tables(tables_cmd(false))
        .await
        .expect("get_tables");
    let batches = do_get(&mut client, info).await;
    let names = string_col(&batches, 2);
    let types = string_col(&batches, 3);
    assert!(names.contains(&"ds1".to_string()), "{names:?}");
    let ds1_type = names
        .iter()
        .zip(&types)
        .find(|(n, _)| n.as_str() == "ds1")
        .map(|(_, t)| t.as_str());
    assert_eq!(ds1_type, Some("TABLE"));

    // include_schema=true adds the IPC-encoded per-table schema as a 5th column,
    // built from each dataset's own metadata.
    let info = client
        .get_tables(tables_cmd(true))
        .await
        .expect("get_tables schema");
    let batches = do_get(&mut client, info).await;
    assert_eq!(batches[0].num_columns(), 5, "table_schema column present");
    let names = string_col(&batches, 2);
    let ds1_row = names.iter().position(|n| n == "ds1").expect("ds1 row");
    let table_schema: &arrow::array::BinaryArray = batches[0]
        .column(4)
        .as_any()
        .downcast_ref()
        .expect("binary col");
    assert!(
        !table_schema.value(ds1_row).is_empty(),
        "ds1 carries a non-empty IPC schema"
    );

    // The LIKE filter actually filters.
    let mut hit = tables_cmd(false);
    hit.table_name_filter_pattern = Some("ds1".to_string());
    let info = client.get_tables(hit).await.expect("hit");
    let names = string_col(&do_get(&mut client, info).await, 2);
    assert!(names.contains(&"ds1".to_string()));

    let mut miss = tables_cmd(false);
    miss.table_name_filter_pattern = Some("nope".to_string());
    let info = client.get_tables(miss).await.expect("miss");
    let names = string_col(&do_get(&mut client, info).await, 2);
    assert!(
        !names.contains(&"ds1".to_string()),
        "filter excluded ds1: {names:?}"
    );

    // Table types.
    let info = client.get_table_types().await.expect("get_table_types");
    let kinds = string_col(&do_get(&mut client, info).await, 0);
    assert!(kinds.contains(&"TABLE".to_string()) && kinds.contains(&"VIEW".to_string()));

    // A session temp view appears as a VIEW for that session.
    let mut session = session_client(addr).await;
    session
        .execute_update(
            "CREATE VIEW v1 AS SELECT country, fare FROM ds1".to_string(),
            None,
        )
        .await
        .expect("create view");
    let info = session
        .get_tables(tables_cmd(false))
        .await
        .expect("session tables");
    let batches = do_get(&mut session, info).await;
    let names = string_col(&batches, 2);
    let types = string_col(&batches, 3);
    assert!(names.contains(&"ds1".to_string()) && names.contains(&"v1".to_string()));
    let v1_type = names
        .iter()
        .zip(&types)
        .find(|(n, _)| n.as_str() == "v1")
        .map(|(_, t)| t.as_str());
    assert_eq!(v1_type, Some("VIEW"));

    drop(client);
    drop(session);
    shutdown.trigger();
    server
        .await
        .expect("server task join")
        .expect("serve returned ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn flight_sql_count_and_group_by_roundtrip() {
    let (addr, shutdown, server, _storage, _cache) = start().await;

    let mut client = connect(addr).await;

    // count(*) — datasets under the storage root are addressable by name.
    let batches = run_query(&mut client, "SELECT count(*) AS n FROM ds1").await;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "count(*) is a single row");
    assert_eq!(int64(&batches[0], 0).value(0), 4);

    // GROUP BY country, sum(fare) — exercises the FTGS pushdown through Flight.
    let batches = run_query(
        &mut client,
        "SELECT country, sum(fare) AS s FROM ds1 GROUP BY country",
    )
    .await;
    let mut got = BTreeMap::new();
    for b in &batches {
        let countries: &StringArray = b.column(0).as_any().downcast_ref().expect("utf8 col");
        let sums = int64(b, 1);
        for i in 0..b.num_rows() {
            got.insert(countries.value(i).to_string(), sums.value(i));
        }
    }
    assert_eq!(got.get("us"), Some(&400), "us = 100 + 300");
    assert_eq!(got.get("de"), Some(&600), "de = 200 + 400");

    // A missing dataset is a clean planner error, not a hang or 500.
    let missing = client
        .execute("SELECT count(*) FROM does_not_exist".to_string(), None)
        .await;
    assert!(missing.is_err(), "unknown dataset should error");

    drop(client);
    shutdown.trigger();
    server
        .await
        .expect("server task join")
        .expect("serve returned ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn session_temp_view_persists_across_calls_and_is_isolated() {
    let (addr, shutdown, server, _storage, _cache) = start().await;

    // A session: create a view in one call, query it in a separate call.
    let mut client = session_client(addr).await;
    client
        .execute_update(
            "CREATE VIEW us_fare AS SELECT country, fare FROM ds1 WHERE country = 'us'".to_string(),
            None,
        )
        .await
        .expect("create view");
    let batches = run_query(&mut client, "SELECT count(*) AS n FROM us_fare").await;
    assert_eq!(int64(&batches[0], 0).value(0), 2, "us has docs 0 and 2");

    // Isolation: a tokenless client cannot see the session's temp view.
    let mut anon = connect(addr).await;
    assert!(
        anon.execute("SELECT * FROM us_fare".to_string(), None)
            .await
            .is_err(),
        "temp view must not leak outside its session"
    );

    // SessionLost: an unknown token is a clean error, not a silent new session.
    let mut bogus = connect(addr).await;
    bogus.set_token("not-a-real-token".to_string());
    assert!(
        bogus
            .execute("SELECT count(*) FROM ds1".to_string(), None)
            .await
            .is_err(),
        "unknown session token should error"
    );

    drop(client);
    shutdown.trigger();
    server
        .await
        .expect("server task join")
        .expect("serve returned ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn over_budget_query_fails_but_session_survives() {
    // Budget = exactly one session's baseline (SESSION_BASELINE_BYTES, 1 MiB):
    // the handshake is admitted, but there is zero headroom left to charge a
    // shard's forward column, so any scan must fail (§2.2).
    let (addr, shutdown, server, _storage, _cache, _metrics) = start_with_budget(1 << 20).await;
    let mut client = session_client(addr).await; // admitted: baseline fits

    let info = client
        .execute("SELECT count(*) AS n FROM ds1".to_string(), None)
        .await
        .expect("planning does not open shards");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let failed = match client.do_get(ticket).await {
        Err(_) => true,
        Ok(stream) => stream.try_collect::<Vec<RecordBatch>>().await.is_err(),
    };
    assert!(failed, "scan over the memory budget must fail");

    // The session is not killed (§2.2): a session-scoped, non-scanning call
    // still works. `execute_update` routes through `do_put_statement_update`,
    // which requires a live session (a reaped one would be SessionLost); a DDL
    // statement opens no shards, so it runs within the baseline-full budget.
    client
        .execute_update("CREATE VIEW alive AS SELECT 1".to_string(), None)
        .await
        .expect("session still alive after the over-budget query");

    drop(client);
    shutdown.trigger();
    server
        .await
        .expect("server task join")
        .expect("serve returned ok");
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_track_sessions_and_queries() {
    let (addr, shutdown, server, _storage, _cache, metrics) = start_with_budget(AMPLE_BUDGET).await;

    // A handshake opens a session → live gauge goes to 1.
    let mut client = session_client(addr).await;
    assert!(
        metrics.render().contains("tut_sessions_live 1"),
        "handshake should bump the live-session gauge"
    );

    // Running a SELECT through do_get records a query.
    let _ = run_query(&mut client, "SELECT count(*) AS n FROM ds1").await;
    let text = metrics.render();
    assert!(text.contains("tut_queries_total 1"), "{text}");
    assert!(text.contains("tut_query_errors_total 0"));

    drop(client);
    shutdown.trigger();
    server
        .await
        .expect("server task join")
        .expect("serve returned ok");
}
