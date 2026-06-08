//! Arrow `FlightSQL` data-plane service.
//!
//! Registers an `arrow.flight.protocol.FlightService` on the gRPC port that
//! speaks `FlightSQL`, executing queries through the same in-process `DataFusion`
//! engine the `t9n sql` CLI uses ([`crate::sql::session_context`]). Datasets
//! under the daemon's storage root are addressable as tables by name:
//! `FROM <name>` resolves to `{storage_url}/<name>` via a lazy schema provider.
//!
//! Only the ad-hoc statement path is implemented (`GetFlightInfo` carrying a
//! `CommandStatementQuery`, then `DoGet` on the returned `TicketStatementQuery`).
//! Prepared statements, transactions, and the catalog-metadata RPCs are left at
//! their trait defaults; the native `SessionControl` session path (and an
//! FTGS-native `DoGet`) land in a later slice. Plain HTTP/2 — protected by
//! network policy until auth lands, like the ops port.

use std::any::Any;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::prelude::{DataFrame, SessionContext};
use futures::{Stream, TryStreamExt};
use prost::Message as _;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};

use crate::cache::Cache;
use crate::shutdown::ShutdownHandle;
use crate::sql::{self, TutankhamunTableProvider};
use crate::storage::StorageRegistry;

/// `FlightSQL` service over a daemon's storage root. Cheap to clone (tonic
/// clones the service per request): the state is a shared handle.
#[derive(Clone)]
pub struct TutankhamunFlightSqlService {
    inner: Arc<ServiceInner>,
}

#[derive(Debug)]
struct ServiceInner {
    storage_url: String,
    cache_dir: PathBuf,
    size_cap: u64,
    /// One [`Cache`] per dataset URL, created on first reference and reused
    /// across queries. A cache must be rooted at the same URL its dataset is
    /// discovered under — the cache fetches shard files by location relative to
    /// its own store — so a single root cache can't serve sub-prefix datasets.
    /// Keying by URL keeps each dataset's cache consistent across queries.
    caches: Mutex<HashMap<String, Arc<Cache>>>,
}

impl TutankhamunFlightSqlService {
    #[must_use]
    pub fn new(storage_url: String, cache_dir: PathBuf, size_cap: u64) -> Self {
        Self {
            inner: Arc::new(ServiceInner {
                storage_url,
                cache_dir,
                size_cap,
                caches: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Build a per-statement `DataFusion` context: the FTGS-pushdown
    /// [`sql::session_context`] with the lazy schema provider swapped in so any
    /// `FROM <name>` resolves to a dataset under the storage root. The optimizer
    /// rule and planner are stateless, so a fresh context per call is cheap; the
    /// expensive, stateful per-dataset caches are shared via [`ServiceInner`].
    fn query_context(&self) -> SessionContext {
        let ctx = sql::session_context();
        let provider = Arc::new(DatasetSchemaProvider {
            inner: Arc::clone(&self.inner),
        });
        // Replace the default catalog's "public" schema. A fresh context always
        // has the default `datafusion`/`public` catalog+schema.
        ctx.catalog("datafusion")
            .expect("default catalog present")
            .register_schema("public", provider)
            .expect("register default schema");
        ctx
    }

    /// Plan `query` and return the `DataFrame` plus its output schema. Shared by
    /// the two ad-hoc statement roundtrips so they derive the schema identically.
    async fn plan(&self, query: &str) -> Result<(DataFrame, SchemaRef), Status> {
        let df = self
            .query_context()
            .sql(query)
            .await
            .map_err(|e| plan_error(&e))?;
        let schema = df.schema().inner().clone();
        Ok((df, schema))
    }
}

#[tonic::async_trait]
impl FlightSqlService for TutankhamunFlightSqlService {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        // No auth in v1 (network-policy protected, like the ops port). Accept
        // the handshake with an empty token so clients that always handshake
        // before querying proceed.
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: bytes::Bytes::new(),
        };
        let stream = futures::stream::once(async move { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Plan (not execute) to learn the output schema. The ticket carries the
        // SQL itself, so the matching `do_get_statement` is self-contained — no
        // session state spans the two roundtrips.
        let (_df, schema) = self.plan(&query.query).await?;

        let ticket = TicketStatementQuery {
            statement_handle: query.query.into_bytes().into(),
        };
        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("encode schema: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let query = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("ticket handle not UTF-8 SQL: {e}")))?;
        let (df, schema) = self.plan(&query).await?;
        // `collect` drives the plan; the FTGS/scan execs bridge async→sync on
        // their own scoped threads (see `sql::scan`'s `block_on_scan`), so it is
        // safe to await here without parking a worker on a nested `block_on`.
        let batches = df.collect().await.map_err(|e| plan_error(&e))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Lazy schema provider: resolves any table name to a [`TutankhamunTableProvider`]
/// over `{storage_url}/<name>`, so datasets under the storage root are queryable
/// by their prefix name without pre-registration.
#[derive(Debug)]
struct DatasetSchemaProvider {
    inner: Arc<ServiceInner>,
}

impl ServiceInner {
    /// Get or create the [`Cache`] rooted at dataset `url`. Synchronous under
    /// the lock — [`Cache::open`] does a one-time dir scan, no `await`.
    fn cache_for(&self, url: &str) -> anyhow::Result<Arc<Cache>> {
        let mut caches = self.caches.lock().expect("cache map lock");
        if let Some(c) = caches.get(url) {
            return Ok(Arc::clone(c));
        }
        let registry = StorageRegistry::from_url(url)?;
        let cache = Arc::new(Cache::open(
            self.cache_dir.clone(),
            registry.store(),
            url.to_string(),
            self.size_cap,
        )?);
        caches.insert(url.to_string(), Arc::clone(&cache));
        Ok(cache)
    }
}

#[async_trait]
impl SchemaProvider for DatasetSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Datasets resolve on demand in `table`; enumerating would need a
        // storage LIST and nothing on the query path requires it.
        Vec::new()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let base = self.inner.storage_url.trim_end_matches('/');
        let url = format!("{base}/{name}");
        let cache = self
            .inner
            .cache_for(&url)
            .map_err(|e| DataFusionError::External(e.into()))?;
        // `try_new` derives the dataset's schema from its first shard, so this
        // touches storage on every reference. An absent/empty dataset is a clean
        // "table not found" (None), not an internal error.
        match TutankhamunTableProvider::try_new(url, cache).await {
            Ok(p) => Ok(Some(Arc::new(p) as Arc<dyn TableProvider>)),
            Err(e) if e.to_string().contains("no shards") => Ok(None),
            Err(e) => Err(DataFusionError::External(e.into())),
        }
    }

    fn table_exist(&self, _name: &str) -> bool {
        // We keep no registry; existence is determined lazily by `table`, which
        // is what DataFusion's query-resolution path consults — not this.
        false
    }
}

/// Bind the gRPC listener. Returns once accepting, so the caller can mark
/// readiness without racing the server (mirrors [`crate::ops_http::bind`]).
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bound = %listener.local_addr()?, "gRPC FlightSQL listening");
    Ok(listener)
}

/// Serve `FlightSQL` on `listener` until `shutdown` fires.
pub async fn serve(
    listener: TcpListener,
    svc: TutankhamunFlightSqlService,
    shutdown: ShutdownHandle,
) -> anyhow::Result<()> {
    let service = FlightServiceServer::new(svc);
    tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.shutdown_requested().await;
            info!("gRPC FlightSQL graceful shutdown signaled");
        })
        .await?;
    Ok(())
}

/// Map a `DataFusion` planning/execution error onto a gRPC status. The message
/// carries the detail (unknown table, parse error, scan failure); clients
/// surface it verbatim.
fn plan_error(e: &DataFusionError) -> Status {
    Status::internal(format!("query failed: {e}"))
}
