//! Arrow `FlightSQL` data-plane service.
//!
//! Registers an `arrow.flight.protocol.FlightService` on the gRPC port that
//! speaks `FlightSQL`, executing queries through the same in-process `DataFusion`
//! engine the `t9n sql` CLI uses ([`crate::sql::session_context`]). Datasets
//! under the daemon's storage root are addressable as tables by name:
//! `FROM <name>` resolves to `{storage_url}/<name>` via a lazy schema provider.
//!
//! **Sessions.** The handshake doubles as session-open: the server mints an
//! opaque token, the client echoes it as `authorization: Bearer <token>` on
//! every subsequent call, and the server resolves a persistent
//! [`SessionContext`] from it (§2.4). State that must survive across calls —
//! notably session-scoped temp views created via `CREATE VIEW` — lives in that
//! context. Calls without a token run against a fresh ephemeral context, exactly
//! as before. Idle (30 min) and aged (4 h) sessions are reaped.
//!
//! Prepared statements, transactions, and the catalog-metadata RPCs are left at
//! their trait defaults; the FTGS-native `DoGet` and the §2.8 narrowing cache
//! land later. Plain HTTP/2 — protected by network policy until auth lands, like
//! the ops port.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::prelude::{DataFrame, SessionContext};
use futures::{Stream, TryStreamExt};
use prost::Message as _;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;
use uuid::Uuid;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    CommandStatementQuery, CommandStatementUpdate, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};

use crate::cache::Cache;
use crate::shutdown::ShutdownHandle;
use crate::sql::{self, TutankhamunTableProvider};
use crate::storage::StorageRegistry;

/// Affinity header: the session id is published here on the handshake response
/// (for proxy routing) and accepted here as an alternative to the bearer token.
const SESSION_HEADER: &str = "x-tutankhamun-session-id";
// §2.4 defaults, hardcoded for v1; exposing them as config knobs is deferred
// (tracked in the roadmap, same call as the cache-cap default).
/// How often the reaper sweeps the session registry.
const REAP_INTERVAL: Duration = Duration::from_secs(60);
/// Idle timeout — a session untouched for this long is reaped (§2.4 default).
const IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Hard maximum age — a session older than this is reaped regardless of activity
/// (§2.4 default), bounding runaway sessions held by long-lived clients.
const MAX_AGE: Duration = Duration::from_secs(4 * 60 * 60);

/// `FlightSQL` service over a daemon's storage root. Cheap to clone (tonic
/// clones the service per request): the state is a shared handle.
#[derive(Clone)]
pub struct TutankhamunFlightSqlService {
    inner: Arc<ServiceInner>,
}

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
    /// Live sessions keyed by their opaque token. Holds the persistent context
    /// (and its session-scoped temp views); reaped on idle / max age.
    sessions: Mutex<HashMap<String, Arc<Session>>>,
}

// Manual `Debug`: the cache and session maps hold `Cache`/`SessionContext`
// values whose debug output is noise, and `SessionContext` is not guaranteed
// `Debug`. Print only the static configuration.
impl fmt::Debug for ServiceInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceInner")
            .field("storage_url", &self.storage_url)
            .field("cache_dir", &self.cache_dir)
            .field("size_cap", &self.size_cap)
            .finish_non_exhaustive()
    }
}

/// A live session: a persistent `DataFusion` context (holding any session-scoped
/// temp views) plus liveness timestamps for the reaper.
struct Session {
    ctx: SessionContext,
    created_at: Instant,
    last_access: Mutex<Instant>,
}

impl Session {
    fn new(ctx: SessionContext, now: Instant) -> Self {
        Self {
            ctx,
            created_at: now,
            last_access: Mutex::new(now),
        }
    }

    fn touch(&self, now: Instant) {
        *self.last_access.lock().expect("last_access lock") = now;
    }

    fn is_expired(&self, now: Instant, idle: Duration, max_age: Duration) -> bool {
        let last = *self.last_access.lock().expect("last_access lock");
        now.saturating_duration_since(last) > idle
            || now.saturating_duration_since(self.created_at) > max_age
    }
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

    /// Drop sessions past their idle timeout or maximum age. Returns the count
    /// reaped.
    fn reap_expired(&self, now: Instant) -> usize {
        let mut sessions = self.sessions.lock().expect("sessions lock");
        let before = sessions.len();
        sessions.retain(|_, s| !s.is_expired(now, IDLE_TIMEOUT, MAX_AGE));
        before - sessions.len()
    }
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
                sessions: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Build a `DataFusion` context: the FTGS-pushdown [`sql::session_context`]
    /// with a fresh lazy schema provider swapped in so any `FROM <name>` resolves
    /// to a dataset under the storage root and `CREATE VIEW` registers into a
    /// (per-context) session-scoped map. The optimizer rule and planner are
    /// stateless, so building one is cheap; the expensive, stateful per-dataset
    /// caches are shared via [`ServiceInner`].
    fn build_context(&self) -> SessionContext {
        let ctx = sql::session_context();
        let provider = Arc::new(DatasetSchemaProvider {
            inner: Arc::clone(&self.inner),
            registered: Mutex::new(HashMap::new()),
        });
        // Replace the default catalog's "public" schema. A fresh context always
        // has the default `datafusion`/`public` catalog+schema.
        ctx.catalog("datafusion")
            .expect("default catalog present")
            .register_schema("public", provider)
            .expect("register default schema");
        ctx
    }

    /// Mint an opaque session and return its token. The token is server-issued
    /// and unguessable (§2.4); the client echoes it but never parses it.
    fn open_session(&self) -> String {
        let token = Uuid::new_v4().to_string();
        let session = Arc::new(Session::new(self.build_context(), Instant::now()));
        self.inner
            .sessions
            .lock()
            .expect("sessions lock")
            .insert(token.clone(), session);
        token
    }

    /// Resolve the session a request belongs to. `None` → no token (run
    /// ephemerally). A token that is present but unknown (reaped / invalid) is a
    /// `SessionLost` condition (§2.4): the client must reopen.
    // `tonic::Status` is a large error type, but it's the gRPC error every
    // handler returns; boxing it here would just mismatch the trait signatures.
    #[allow(clippy::result_large_err)]
    fn resolve_session(&self, md: &MetadataMap) -> Result<Option<Arc<Session>>, Status> {
        let Some(token) = session_token(md) else {
            return Ok(None);
        };
        let session = self
            .inner
            .sessions
            .lock()
            .expect("sessions lock")
            .get(&token)
            .cloned();
        match session {
            Some(s) => {
                s.touch(Instant::now());
                Ok(Some(s))
            }
            None => Err(Status::not_found(
                "session not found or expired; reopen via handshake",
            )),
        }
    }

    /// The context to run a call against: the session's persistent one (cloned —
    /// clones share state, so temp views are visible), or a fresh ephemeral one.
    fn context_for(&self, session: Option<&Arc<Session>>) -> SessionContext {
        match session {
            Some(s) => s.ctx.clone(),
            None => self.build_context(),
        }
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
        // No auth in v1 (network-policy protected, like the ops port). The
        // handshake doubles as session-open: mint an opaque server-issued token
        // the client echoes as `authorization: Bearer <token>` afterwards (§2.4).
        let token = self.open_session();
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: bytes::Bytes::from(token.clone().into_bytes()),
        };
        let stream = futures::stream::once(async move { Ok(response) });
        let boxed: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(stream);
        let mut resp = Response::new(boxed);
        // Standard FlightSQL clients read the token from the `authorization`
        // response header and auto-echo it as a bearer on later calls.
        if let Ok(v) = format!("Bearer {token}").parse() {
            resp.metadata_mut().insert("authorization", v);
        }
        // Also publish it as the affinity header for proxy routing (§2.4).
        if let Ok(v) = token.parse() {
            resp.metadata_mut().insert(SESSION_HEADER, v);
        }
        Ok(resp)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Plan (not execute) to learn the output schema. The ticket carries the
        // SQL itself, so the matching `do_get_statement` is self-contained — it
        // re-resolves the same session from its own metadata.
        let session = self.resolve_session(request.metadata())?;
        let ctx = self.context_for(session.as_ref());
        let (_df, schema) = plan(&ctx, &query.query).await?;

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
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let query = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("ticket handle not UTF-8 SQL: {e}")))?;
        let session = self.resolve_session(request.metadata())?;
        let ctx = self.context_for(session.as_ref());
        let (df, schema) = plan(&ctx, &query).await?;
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

    async fn do_put_statement_update(
        &self,
        command: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        // DDL/DML (e.g. `CREATE VIEW`) only makes sense against a persistent
        // session — its effect must survive for later queries.
        let session = self.resolve_session(request.metadata())?.ok_or_else(|| {
            Status::failed_precondition("statement update requires a session; handshake first")
        })?;
        session
            .ctx
            .sql(&command.query)
            .await
            .map_err(|e| plan_error(&e))?
            .collect()
            .await
            .map_err(|e| plan_error(&e))?;
        // DataFusion does not report an affected-row count for these statements.
        Ok(0)
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Lazy + registerable schema provider. Resolves a table name first against the
/// session's registered objects (temp views/tables from `CREATE VIEW`), then
/// lazily to a [`TutankhamunTableProvider`] over `{storage_url}/<name>` — so
/// datasets under the storage root are queryable by name without
/// pre-registration. The `registered` map is per-context, hence session-scoped.
#[derive(Debug)]
struct DatasetSchemaProvider {
    inner: Arc<ServiceInner>,
    registered: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl DatasetSchemaProvider {
    fn registered_tables(&self) -> MutexGuard<'_, HashMap<String, Arc<dyn TableProvider>>> {
        self.registered.lock().expect("registered map lock")
    }
}

#[async_trait]
impl SchemaProvider for DatasetSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Registered objects only; datasets resolve on demand in `table` and
        // enumerating them would need a storage LIST nothing requires.
        self.registered_tables().keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        // Registered objects (temp views/tables) shadow datasets. Scope the lock
        // so its guard never crosses the `await` below.
        let registered = self.registered_tables().get(name).cloned();
        if let Some(t) = registered {
            return Ok(Some(t));
        }

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

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.registered_tables().insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.registered_tables().remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        // Registered objects are tracked; datasets are resolved lazily via
        // `table`, which is what query resolution consults — not this.
        self.registered_tables().contains_key(name)
    }
}

/// Extract the session token from request metadata: the `authorization: Bearer
/// <token>` header, or the affinity header as a proxy-mode fallback.
fn session_token(md: &MetadataMap) -> Option<String> {
    if let Some(v) = md.get("authorization")
        && let Ok(s) = v.to_str()
        && let Some(tok) = s.strip_prefix("Bearer ")
    {
        return Some(tok.to_string());
    }
    md.get(SESSION_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Plan `query` against `ctx` and return the `DataFrame` plus its output schema.
/// Shared by the two ad-hoc statement roundtrips so they derive it identically.
async fn plan(ctx: &SessionContext, query: &str) -> Result<(DataFrame, SchemaRef), Status> {
    let df = ctx.sql(query).await.map_err(|e| plan_error(&e))?;
    let schema = df.schema().inner().clone();
    Ok((df, schema))
}

/// Bind the gRPC listener. Returns once accepting, so the caller can mark
/// readiness without racing the server (mirrors [`crate::ops_http::bind`]).
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bound = %listener.local_addr()?, "gRPC FlightSQL listening");
    Ok(listener)
}

/// Serve `FlightSQL` on `listener` until `shutdown` fires. Spawns the session
/// reaper alongside the server, gated by the same handle.
pub async fn serve(
    listener: TcpListener,
    svc: TutankhamunFlightSqlService,
    shutdown: ShutdownHandle,
) -> anyhow::Result<()> {
    let reaper = tokio::spawn(run_reaper(svc.clone(), shutdown.clone()));
    let service = FlightServiceServer::new(svc);
    let result = tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.shutdown_requested().await;
            info!("gRPC FlightSQL graceful shutdown signaled");
        })
        .await;
    // Stop the reaper whether the server drained on shutdown or returned an
    // error: awaiting it would hang on the error path (it only exits on
    // shutdown), and it holds no state worth draining.
    reaper.abort();
    result?;
    Ok(())
}

/// Periodically reap idle / aged sessions until shutdown.
async fn run_reaper(svc: TutankhamunFlightSqlService, shutdown: ShutdownHandle) {
    let mut ticker = tokio::time::interval(REAP_INTERVAL);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let n = svc.inner.reap_expired(Instant::now());
                if n > 0 {
                    info!(reaped = n, "expired sessions reaped");
                }
            }
            () = shutdown.shutdown_requested() => break,
        }
    }
}

/// Map a `DataFusion` planning/execution error onto a gRPC status. The message
/// carries the detail (unknown table, parse error, scan failure); clients
/// surface it verbatim.
fn plan_error(e: &DataFusionError) -> Status {
    Status::internal(format!("query failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc() -> TutankhamunFlightSqlService {
        TutankhamunFlightSqlService::new(
            "memory:///".to_string(),
            std::env::temp_dir().join("t9n-session-test"),
            u64::MAX,
        )
    }

    #[test]
    fn is_expired_honours_idle_and_max_age() {
        let now = Instant::now();
        let s = Session::new(SessionContext::new(), now);
        assert!(!s.is_expired(now, IDLE_TIMEOUT, MAX_AGE));
        // Idle exceeded.
        assert!(s.is_expired(
            now + IDLE_TIMEOUT + Duration::from_secs(1),
            IDLE_TIMEOUT,
            MAX_AGE
        ));
        // Within idle but past max age.
        let old = now
            .checked_sub(MAX_AGE + Duration::from_secs(1))
            .expect("instant in range");
        let aged = Session::new(SessionContext::new(), old);
        aged.touch(now); // recently used, but created long ago
        assert!(aged.is_expired(now, IDLE_TIMEOUT, MAX_AGE));
    }

    #[test]
    fn reap_expired_removes_only_stale_sessions() {
        let svc = svc();
        let now = Instant::now();
        let fresh = Arc::new(Session::new(SessionContext::new(), now));
        let stale_at = now
            .checked_sub(IDLE_TIMEOUT + Duration::from_secs(60))
            .expect("instant in range");
        let stale = Arc::new(Session::new(SessionContext::new(), stale_at));
        {
            let mut map = svc.inner.sessions.lock().unwrap();
            map.insert("fresh".to_string(), fresh);
            map.insert("stale".to_string(), stale);
        }
        assert_eq!(svc.inner.reap_expired(now), 1);
        let map = svc.inner.sessions.lock().unwrap();
        assert!(map.contains_key("fresh"));
        assert!(!map.contains_key("stale"));
    }
}
