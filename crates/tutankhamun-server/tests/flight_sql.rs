//! End-to-end `FlightSQL` test: the service executes SQL over a real on-disk
//! dataset and streams Arrow results back to a standard Flight SQL client,
//! exercising both the row-count path and the FTGS `GROUP BY` pushdown over
//! the wire.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use roaring::RoaringBitmap;
use tonic::transport::Channel;

use tutankhamun_server::flight_sql::{self, TutankhamunFlightSqlService};
use tutankhamun_server::shard::DiskShardWriter;
use tutankhamun_server::shutdown::ShutdownHandle;

fn bitmap(docs: impl IntoIterator<Item = u32>) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    bm.extend(docs);
    bm
}

/// One shard `s0` under `{root}/ds1`: country us→{0,2}, de→{1,3};
/// fare [100, 200, 300, 400]. So count(*) = 4, sum(fare) per country: us=400,
/// de=600.
fn write_dataset(root: &Path) {
    let shard = root.join("ds1").join("s0");
    let mut w = DiskShardWriter::new(&shard, (0, 0)).expect("new shard");
    w.add_metric("fare", vec![100, 200, 300, 400])
        .expect("fare");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0, 2]));
    country.insert("de".to_string(), bitmap([1, 3]));
    w.add_string_field("country", country).expect("country");
    w.finalize().expect("finalize");
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
    let storage = tempfile::tempdir().expect("storage tmp");
    let cache = tempfile::tempdir().expect("cache tmp");
    write_dataset(storage.path());
    let storage_url = url::Url::from_directory_path(storage.path())
        .expect("absolute path")
        .to_string();

    let shutdown = ShutdownHandle::new();
    let listener = flight_sql::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind grpc");
    let addr = listener.local_addr().expect("local addr");
    let svc = TutankhamunFlightSqlService::new(storage_url, cache.path().to_path_buf(), u64::MAX);
    let server = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { flight_sql::serve(listener, svc, shutdown).await }
    });
    (addr, shutdown, server, storage, cache)
}

/// Run `sql` through the ad-hoc statement path (`GetFlightInfo` → `DoGet`) and
/// collect the streamed batches.
async fn run_query(client: &mut FlightSqlServiceClient<Channel>, sql: &str) -> Vec<RecordBatch> {
    let info = client
        .execute(sql.to_string(), None)
        .await
        .expect("get_flight_info");
    let ticket = info.endpoint[0].ticket.clone().expect("endpoint ticket");
    let stream = client.do_get(ticket).await.expect("do_get");
    stream.try_collect().await.expect("collect batches")
}

fn int64(batch: &RecordBatch, col: usize) -> &Int64Array {
    batch
        .column(col)
        .as_any()
        .downcast_ref()
        .expect("int64 col")
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
