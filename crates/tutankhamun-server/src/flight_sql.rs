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
//! **Prepared statements** (create / get / `do_get` / update / close), the
//! **catalog-metadata RPCs** (catalogs / schemas / tables / table types), and
//! the **`GetSqlInfo`** capability probe (the connect-time RPC JDBC/ADBC/GUI
//! clients call) are implemented, and an explicit **`CloseSession`** custom
//! action frees a session immediately. Still deferred: prepared-statement
//! *parameter binding*, transactions / savepoints (a no-op until there is
//! mutable state to transact), the key/XDBC-info RPCs, the FTGS-native `DoGet`,
//! and the §2.8 narrowing cache. Plain HTTP/2 — protected by network policy
//! until auth lands, like the ops port.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::IpcWriteOptions;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use futures::{Stream, TryStreamExt};
use prost::Message as _;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status, Streaming};
use tracing::{Instrument as _, info, info_span};
use uuid::Uuid;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate,
    DoPutPreparedStatementResult, ProstMessageExt, SqlInfo, SqlSupportedCaseSensitivity,
    SqlSupportedTransaction, TicketStatementQuery,
};
use arrow_flight::{
    Action, ActionType, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, Result as FlightResult, SchemaAsIpc, Ticket,
};

use object_store::ObjectStore;

use crate::bitmap_cache::BitmapCache;
use crate::cache::Cache;
use crate::memory::{MemoryBudget, SessionMemoryHandle, SessionReservation};
use crate::metrics::Metrics;
use crate::shutdown::ShutdownHandle;
use crate::sql::{self, TutankhamunTableProvider};
use crate::status::{DatasetLoad, DatasetsReport, SessionsSummary, StatusSource, StructuralReport};
use crate::storage::StorageRegistry;

/// Affinity header: the session id is published here on the handshake response
/// (for proxy routing) and accepted here as an alternative to the bearer token.
const SESSION_HEADER: &str = "x-tutankhamun-session-id";
/// Custom `do_action` type for explicit session teardown. `FlightSQL` 56.2.1 has
/// no native `CloseSession` action, so it is advertised via `list_custom_actions`
/// and handled in `do_action_fallback`, freeing session state immediately rather
/// than waiting for the idle/max-age reaper.
const CLOSE_SESSION_ACTION: &str = "CloseSession";
// §2.4 defaults, hardcoded for v1; exposing them as config knobs is deferred
// (tracked in the roadmap, same call as the cache-cap default).
/// How often the reaper sweeps the session registry.
const REAP_INTERVAL: Duration = Duration::from_secs(60);
/// Idle timeout — a session untouched for this long is reaped (§2.4 default).
const IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Hard maximum age — a session older than this is reaped regardless of activity
/// (§2.4 default), bounding runaway sessions held by long-lived clients.
const MAX_AGE: Duration = Duration::from_secs(4 * 60 * 60);
/// Fixed memory charged per session on open — a rough floor for its persistent
/// `SessionContext` and cursor state, and the lever for admission control: if
/// this can't be reserved the daemon is at capacity (§2.2). Tunable.
const SESSION_BASELINE_BYTES: u64 = 1 << 20; // 1 MiB

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
    /// Prepared statements keyed by their opaque handle → the SQL text. The
    /// handle is an unguessable UUID; execution re-resolves the session from
    /// request metadata per call (like the ad-hoc statement path), so a prepared
    /// statement sees its session's temp views. Entries are dropped on
    /// `ClosePreparedStatement`. Parameter binding is not yet supported.
    prepared: Mutex<HashMap<String, String>>,
    /// Daemon-wide memory budget (§2.2). Every session's [`SessionMemoryHandle`]
    /// charges through it; query scans reserve the forward-column working set.
    budget: Arc<MemoryBudget>,
    /// Per-session cap as a percent of [`Self::budget`]'s limit.
    session_pct: u8,
    /// Daemon-shared doc-set bitmap cache (§2.8); threaded into each context as a
    /// `SessionConfig` extension so the scan can probe it.
    bitmap_cache: Arc<BitmapCache>,
    /// Daemon metrics (§3.4): session gauge + query latency/counters.
    metrics: Arc<Metrics>,
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
/// temp views) plus liveness timestamps for the reaper, and the baseline memory
/// reservation. The session's [`SessionMemoryHandle`] is kept alive by both this
/// reservation and the context's `SessionConfig` extension; all are released
/// when the session is reaped (its `Arc` drops).
struct Session {
    ctx: SessionContext,
    created_at: Instant,
    last_access: Mutex<Instant>,
    #[allow(dead_code)] // RAII: returns the baseline to the budget on reap
    baseline: SessionReservation,
}

impl Session {
    fn new(ctx: SessionContext, now: Instant, baseline: SessionReservation) -> Self {
        Self {
            ctx,
            created_at: now,
            last_access: Mutex::new(now),
            baseline,
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

    /// Dataset names under the storage root — the top-level directories. A
    /// single delimited LIST (one round-trip, no shard reads); unlike a full
    /// `discover`, it does not fetch every shard's `metadata.json` just to learn
    /// the names. A non-dataset top-level dir would list too, but resolving it
    /// as a table is then a clean "not found".
    async fn list_datasets(&self) -> anyhow::Result<Vec<String>> {
        let registry = StorageRegistry::from_url(&self.storage_url)?;
        let listing = registry.store().list_with_delimiter(None).await?;
        let mut names: Vec<String> = listing
            .common_prefixes
            .iter()
            .filter_map(|p| p.parts().last().map(|seg| seg.as_ref().to_string()))
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// Drop sessions past their idle timeout or maximum age. Returns the count
    /// reaped.
    fn reap_expired(&self, now: Instant) -> usize {
        let mut sessions = self.sessions.lock().expect("sessions lock");
        let before = sessions.len();
        sessions.retain(|_, s| !s.is_expired(now, IDLE_TIMEOUT, MAX_AGE));
        let reaped = before - sessions.len();
        drop(sessions);
        if reaped > 0 {
            self.metrics.sessions_reaped(reaped);
        }
        reaped
    }

    /// Summarise live sessions for `/status`: count, the oldest session's age,
    /// and total baseline bytes reserved (the admission floor, not live
    /// working-set — every session reserves exactly [`SESSION_BASELINE_BYTES`]).
    fn sessions_snapshot(&self) -> SessionsSummary {
        let sessions = self.sessions.lock().expect("sessions lock");
        let now = Instant::now();
        let count = sessions.len() as u64;
        let oldest_age_secs = sessions
            .values()
            .map(|s| now.saturating_duration_since(s.created_at).as_secs())
            .max()
            .unwrap_or(0);
        SessionsSummary {
            count,
            oldest_age_secs,
            reserved_bytes: count * SESSION_BASELINE_BYTES,
        }
    }

    /// Per-dataset resident cache footprint for `/status`. Each cache is keyed
    /// by its dataset URL (`{storage_url}/{name}`); strip the storage root to
    /// recover the dataset name. Datasets never queried have no cache and so
    /// don't appear — i.e. nothing loaded yet.
    fn loaded_snapshot(&self) -> Vec<DatasetLoad> {
        let base = self.storage_url.trim_end_matches('/');
        let caches = self.caches.lock().expect("cache map lock");
        let mut loaded: Vec<DatasetLoad> = caches
            .iter()
            .map(|(url, cache)| {
                let name = url
                    .strip_prefix(base)
                    .unwrap_or(url)
                    .trim_matches('/')
                    .to_string();
                let (shards, cached_bytes) = cache.resident();
                DatasetLoad {
                    name,
                    shards: shards as u64,
                    cached_bytes,
                }
            })
            .collect();
        loaded.sort_by(|a, b| a.name.cmp(&b.name));
        loaded
    }
}

#[async_trait]
impl StatusSource for TutankhamunFlightSqlService {
    async fn structural_snapshot(&self) -> StructuralReport {
        let sessions = self.inner.sessions_snapshot();
        let loaded = self.inner.loaded_snapshot();
        // A LIST failure must not fail the page — report datasets as unavailable.
        let datasets = match self.inner.list_datasets().await {
            Ok(names) => DatasetsReport { ok: true, names },
            Err(_) => DatasetsReport {
                ok: false,
                names: Vec::new(),
            },
        };
        StructuralReport {
            sessions,
            datasets,
            loaded,
        }
    }
}

impl TutankhamunFlightSqlService {
    #[must_use]
    pub fn new(
        storage_url: String,
        cache_dir: PathBuf,
        size_cap: u64,
        budget: Arc<MemoryBudget>,
        session_pct: u8,
        bitmap_cache: Arc<BitmapCache>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            inner: Arc::new(ServiceInner {
                storage_url,
                cache_dir,
                size_cap,
                caches: Mutex::new(HashMap::new()),
                sessions: Mutex::new(HashMap::new()),
                prepared: Mutex::new(HashMap::new()),
                budget,
                session_pct,
                bitmap_cache,
                metrics,
            }),
        }
    }

    /// Mint a fresh [`SessionMemoryHandle`] capped at `session_pct`% of the
    /// global budget.
    fn new_memory_handle(&self) -> Arc<SessionMemoryHandle> {
        let cap = self
            .inner
            .budget
            .limit()
            .saturating_mul(u64::from(self.inner.session_pct))
            / 100;
        Arc::new(SessionMemoryHandle::new(
            Arc::clone(&self.inner.budget),
            cap,
        ))
    }

    /// Build a `DataFusion` context: the FTGS-pushdown [`sql::session_context`]
    /// with a fresh lazy schema provider swapped in so any `FROM <name>` resolves
    /// to a dataset under the storage root and `CREATE VIEW` registers into a
    /// (per-context) session-scoped map. The optimizer rule and planner are
    /// stateless, so building one is cheap; the expensive, stateful per-dataset
    /// caches are shared via [`ServiceInner`].
    ///
    /// `mem` rides along as a `SessionConfig` extension so the (otherwise
    /// session-agnostic) query execs can charge their forward-column working set
    /// to it via [`TaskContext::session_config`] (§2.2).
    fn build_context(&self, mem: Arc<SessionMemoryHandle>) -> SessionContext {
        let config = SessionConfig::new()
            .with_extension(mem)
            .with_extension(Arc::clone(&self.inner.bitmap_cache));
        let ctx = sql::session_context_with(config);
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

    /// Plan and execute `query` against `ctx`, returning the output schema and
    /// collected batches. Shared by the statement and prepared-statement `do_get`
    /// paths. Wraps the work in a per-query span (§3.4) with `plan` / `execute`
    /// sub-spans, records the metrics histogram, and records `rows` / `error` on
    /// the query span. `prepared` distinguishes the two callers.
    #[allow(clippy::result_large_err)]
    async fn execute_query(
        &self,
        ctx: &SessionContext,
        query: &str,
        prepared: bool,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        let preview = sql_preview(query);
        let span = info_span!(
            "query",
            sql = %preview,
            prepared,
            rows = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        async {
            let (df, schema) = plan(ctx, query).instrument(info_span!("plan")).await?;
            let started = Instant::now();
            let result = df.collect().instrument(info_span!("execute")).await;
            let rows = match &result {
                Ok(batches) => batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64,
                Err(_) => 0,
            };
            self.inner
                .metrics
                .record_query(started.elapsed(), result.is_ok(), preview, rows);
            match result {
                Ok(batches) => {
                    tracing::Span::current().record("rows", rows);
                    Ok((schema, batches))
                }
                Err(e) => {
                    tracing::Span::current().record("error", "execute");
                    Err(plan_error(&e))
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Mint an opaque session and return its token. The token is server-issued
    /// and unguessable (§2.4); the client echoes it but never parses it.
    ///
    /// Admission control (§2.2): a fixed baseline is reserved against the new
    /// session's handle (and so the global budget). If it can't be satisfied the
    /// daemon is at capacity and the session is refused before any work begins.
    #[allow(clippy::result_large_err)]
    fn open_session(&self) -> Result<String, Status> {
        let mem = self.new_memory_handle();
        let baseline = mem.reserve(SESSION_BASELINE_BYTES).map_err(|e| {
            Status::resource_exhausted(format!("daemon at memory capacity, retry later: {e}"))
        })?;
        let token = Uuid::new_v4().to_string();
        // `mem` is retained by `ctx` (the extension) and `baseline`; the local
        // Arc drops here.
        let ctx = self.build_context(mem);
        let session = Arc::new(Session::new(ctx, Instant::now(), baseline));
        self.inner
            .sessions
            .lock()
            .expect("sessions lock")
            .insert(token.clone(), session);
        self.inner.metrics.session_opened();
        Ok(token)
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
            // Tokenless calls get a throwaway context with its own per-call
            // handle (no baseline): queries are still capped and released, just
            // not tied to a persistent session.
            None => self.build_context(self.new_memory_handle()),
        }
    }

    /// Resolve a prepared-statement handle to its SQL text. An unknown handle
    /// (never created, or already closed) is a clean `not_found`.
    #[allow(clippy::result_large_err)]
    fn prepared_sql(&self, handle: &bytes::Bytes) -> Result<String, Status> {
        let key = String::from_utf8(handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("prepared handle not UTF-8: {e}")))?;
        self.inner
            .prepared
            .lock()
            .expect("prepared lock")
            .get(&key)
            .cloned()
            .ok_or_else(|| Status::not_found("prepared statement not found or closed"))
    }
}

/// IPC-encapsulate an Arrow schema as the `Bytes` the `FlightSQL`
/// prepared-statement result carries for its dataset / parameter schemas.
#[allow(clippy::result_large_err)]
fn encode_schema(schema: &Schema) -> Result<bytes::Bytes, Status> {
    let message: IpcMessage = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
        .try_into()
        .map_err(|e| Status::internal(format!("encode schema: {e}")))?;
    Ok(message.0)
}

/// Stream a single metadata `RecordBatch` as a `DoGet` response, the same
/// encoder path the statement results use.
fn batch_stream(
    batch: RecordBatch,
) -> Response<<TutankhamunFlightSqlService as FlightService>::DoGetStream> {
    let schema = batch.schema();
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::iter(std::iter::once(Ok(batch))))
        .map_err(Status::from);
    Response::new(Box::pin(stream))
}

/// Fixed `GetSqlInfo` capability set, built once. Identifier-case flags follow
/// `DataFusion`'s dialect (unquoted folded to lowercase, `"`-quoted), and
/// `read_only` is false because the session path accepts `CREATE VIEW` DDL.
fn sql_info_data() -> &'static SqlInfoData {
    static DATA: OnceLock<SqlInfoData> = OnceLock::new();
    DATA.get_or_init(|| {
        let mut b = SqlInfoDataBuilder::new();
        b.append(SqlInfo::FlightSqlServerName, "Tutankhamun");
        b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        // FlightSqlServerArrowVersion is omitted: it's optional, and arrow
        // exposes no version constant to derive it from without drift.
        b.append(SqlInfo::FlightSqlServerReadOnly, false);
        b.append(SqlInfo::FlightSqlServerSql, true);
        b.append(SqlInfo::FlightSqlServerSubstrait, false);
        b.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::None as i32,
        );
        b.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
        b.append(
            SqlInfo::SqlIdentifierCase,
            SqlSupportedCaseSensitivity::SqlCaseSensitivityLowercase as i32,
        );
        b.append(
            SqlInfo::SqlQuotedIdentifierCase,
            SqlSupportedCaseSensitivity::SqlCaseSensitivityUnknown as i32,
        );
        b.build().expect("static SqlInfo set builds")
    })
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
        let token = self.open_session()?;
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
        // `execute_query` wraps this in a per-query span (§3.4). `collect` drives
        // the plan; the FTGS/scan execs bridge async→sync on their own scoped
        // threads (`block_on_scan`), so it is safe to await here without parking a
        // worker on a nested `block_on`.
        let (schema, batches) = self.execute_query(&ctx, &query, false).await?;

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

    // -- Prepared statements (no-parameter case; binding is deferred) --

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        // Plan against the call's session so the dataset schema reflects its
        // temp views; store the SQL under an opaque handle the client echoes.
        let session = self.resolve_session(request.metadata())?;
        let ctx = self.context_for(session.as_ref());
        let (_df, schema) = plan(&ctx, &query.query).await?;

        let handle = Uuid::new_v4().to_string();
        self.inner
            .prepared
            .lock()
            .expect("prepared lock")
            .insert(handle.clone(), query.query);

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: bytes::Bytes::from(handle.into_bytes()),
            dataset_schema: encode_schema(schema.as_ref())?,
            // No bound parameters are supported yet, so the parameter schema is
            // empty: a prepared statement is a fixed SQL string.
            parameter_schema: encode_schema(&Schema::empty())?,
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        if let Ok(key) = String::from_utf8(query.prepared_statement_handle.to_vec()) {
            self.inner
                .prepared
                .lock()
                .expect("prepared lock")
                .remove(&key);
        }
        Ok(())
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = self.prepared_sql(&query.prepared_statement_handle)?;
        let session = self.resolve_session(request.metadata())?;
        let ctx = self.context_for(session.as_ref());
        let (_df, schema) = plan(&ctx, &sql).await?;

        // The ticket carries the prepared command (the handle); the blanket
        // `do_get` routes it back to `do_get_prepared_statement`.
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        flight_info_for(schema.as_ref(), ticket, request)
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = self.prepared_sql(&query.prepared_statement_handle)?;
        let session = self.resolve_session(request.metadata())?;
        let ctx = self.context_for(session.as_ref());
        let (schema, batches) = self.execute_query(&ctx, &sql, true).await?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        // Parameter binding is deferred: we don't decode the bound parameter
        // batch. Validate the handle and echo it so the client runs the query
        // through `get_flight_info_prepared_statement` / `do_get`.
        let _ = self.prepared_sql(&query.prepared_statement_handle)?;
        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(query.prepared_statement_handle),
        })
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let sql = self.prepared_sql(&query.prepared_statement_handle)?;
        // An update (DDL/DML) only makes sense against a persistent session.
        let session = self.resolve_session(request.metadata())?.ok_or_else(|| {
            Status::failed_precondition("prepared update requires a session; handshake first")
        })?;
        session
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| plan_error(&e))?
            .collect()
            .await
            .map_err(|e| plan_error(&e))?;
        Ok(0)
    }

    // -- Capability probe (connect-time `GetSqlInfo`) --

    // The connect-time RPC most JDBC/ADBC/GUI clients call before anything else.
    // We serve a fixed flag set; the client filters to the codes it asked for.
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let schema = query.into_builder(sql_info_data()).schema();
        flight_info_for(schema.as_ref(), ticket, request)
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let batch = query
            .into_builder(sql_info_data())
            .build()
            .map_err(|e| metadata_error(&e))?;
        Ok(batch_stream(batch))
    }

    // -- Catalog metadata (catalogs / schemas / tables / table types) --

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let schema = query.into_builder().schema();
        flight_info_for(schema.as_ref(), ticket, request)
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append(CATALOG_NAME);
        let batch = builder.build().map_err(|e| metadata_error(&e))?;
        Ok(batch_stream(batch))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let schema = query.into_builder().schema();
        flight_info_for(schema.as_ref(), ticket, request)
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append(CATALOG_NAME, SCHEMA_NAME);
        let batch = builder.build().map_err(|e| metadata_error(&e))?;
        Ok(batch_stream(batch))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let schema = query.into_builder().schema();
        flight_info_for(schema.as_ref(), ticket, request)
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let session = self.resolve_session(request.metadata())?;
        let mut builder = query.into_builder();
        let include = builder.include_schema();
        let empty = Schema::empty();

        // Datasets under the storage root are TABLEs. The builder applies the
        // client's catalog / LIKE / table-type filters in `build`, so append
        // every candidate.
        for name in self
            .inner
            .list_datasets()
            .await
            .map_err(|e| Status::internal(format!("list datasets: {e}")))?
        {
            if include {
                let base = self.inner.storage_url.trim_end_matches('/');
                let url = format!("{base}/{name}");
                let cache = self
                    .inner
                    .cache_for(&url)
                    .map_err(|e| Status::internal(format!("cache: {e}")))?;
                match TutankhamunTableProvider::try_new(url, cache).await {
                    Ok(p) => builder
                        .append(
                            CATALOG_NAME,
                            SCHEMA_NAME,
                            &name,
                            "TABLE",
                            p.schema().as_ref(),
                        )
                        .map_err(|e| metadata_error(&e))?,
                    // Raced with eviction / an empty dir — just skip it.
                    Err(e) if e.to_string().contains("no shards") => {}
                    Err(e) => return Err(Status::internal(format!("dataset schema: {e}"))),
                }
            } else {
                builder
                    .append(CATALOG_NAME, SCHEMA_NAME, &name, "TABLE", &empty)
                    .map_err(|e| metadata_error(&e))?;
            }
        }

        // Session-scoped temp views (from CREATE VIEW) are VIEWs.
        if let Some(session) = &session
            && let Some(sp) = session
                .ctx
                .catalog(CATALOG_NAME)
                .and_then(|c| c.schema(SCHEMA_NAME))
        {
            for name in sp.table_names() {
                if include {
                    if let Some(t) = sp
                        .table(&name)
                        .await
                        .map_err(|e| Status::internal(format!("view schema: {e}")))?
                    {
                        builder
                            .append(
                                CATALOG_NAME,
                                SCHEMA_NAME,
                                &name,
                                "VIEW",
                                t.schema().as_ref(),
                            )
                            .map_err(|e| metadata_error(&e))?;
                    }
                } else {
                    builder
                        .append(CATALOG_NAME, SCHEMA_NAME, &name, "VIEW", &empty)
                        .map_err(|e| metadata_error(&e))?;
                }
            }
        }

        let batch = builder.build().map_err(|e| metadata_error(&e))?;
        Ok(batch_stream(batch))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        flight_info_for(table_types_schema().as_ref(), ticket, request)
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Ok(batch_stream(table_types_batch()?))
    }

    // -- Custom actions: explicit session teardown --

    async fn list_custom_actions(&self) -> Option<Vec<Result<ActionType, Status>>> {
        Some(vec![Ok(ActionType {
            r#type: CLOSE_SESSION_ACTION.to_string(),
            description: "Closes the session named by the bearer token, freeing its \
                          state immediately rather than waiting for the reaper."
                .to_string(),
        })])
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        if request.get_ref().r#type == CLOSE_SESSION_ACTION {
            // Idempotent: closing an already-reaped/unknown session is fine.
            // Later calls with the token return SessionLost, as the client expects.
            if let Some(token) = session_token(request.metadata()) {
                let removed = self
                    .inner
                    .sessions
                    .lock()
                    .expect("sessions lock")
                    .remove(&token);
                if removed.is_some() {
                    self.inner.metrics.session_closed();
                }
            }
            let stream = futures::stream::empty::<Result<FlightResult, Status>>();
            return Ok(Response::new(Box::pin(stream)));
        }
        Err(Status::invalid_argument(format!(
            "unsupported action type: {}",
            request.get_ref().r#type
        )))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// The single catalog / schema the engine resolves datasets under (the
/// `DatasetSchemaProvider` is registered as `public` under catalog `datafusion`).
/// Reporting these makes a qualified `"datafusion"."public"."<dataset>"` name
/// round-trip through query resolution.
const CATALOG_NAME: &str = "datafusion";
const SCHEMA_NAME: &str = "public";

/// `FlightSQL` `GetTableTypes` result schema: a single non-null `table_type` column.
fn table_types_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
}

/// The table types this server exposes: datasets are `TABLE`, temp views `VIEW`.
#[allow(clippy::result_large_err)]
fn table_types_batch() -> Result<RecordBatch, Status> {
    RecordBatch::try_new(
        table_types_schema(),
        vec![Arc::new(StringArray::from(vec!["TABLE", "VIEW"]))],
    )
    .map_err(|e| Status::internal(format!("build table types: {e}")))
}

/// Build a metadata `FlightInfo`: output `schema`, a single endpoint whose ticket
/// re-resolves the command, and the original descriptor.
#[allow(clippy::result_large_err)]
fn flight_info_for(
    schema: &Schema,
    ticket: Ticket,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let endpoint = FlightEndpoint::new().with_ticket(ticket);
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|e| Status::internal(format!("encode schema: {e}")))?
        .with_endpoint(endpoint)
        .with_descriptor(request.into_inner());
    Ok(Response::new(info))
}

/// Map an error from a metadata builder onto a gRPC status.
fn metadata_error(e: &arrow_flight::error::FlightError) -> Status {
    Status::internal(format!("build metadata batch: {e}"))
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

/// A bounded preview of the SQL text for a trace span attribute (standard
/// cardinality discipline — the span carries the query, not unbounded labels).
fn sql_preview(sql: &str) -> String {
    const MAX: usize = 200;
    sql.chars().take(MAX).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc() -> TutankhamunFlightSqlService {
        svc_with_budget(u64::MAX)
    }

    fn svc_with_budget(limit: u64) -> TutankhamunFlightSqlService {
        let budget = Arc::new(MemoryBudget::new(limit));
        let bitmap_cache = Arc::new(BitmapCache::new(Arc::new(SessionMemoryHandle::new(
            Arc::clone(&budget),
            limit,
        ))));
        let metrics = Metrics::new(Arc::clone(&budget), Arc::clone(&bitmap_cache));
        TutankhamunFlightSqlService::new(
            "memory:///".to_string(),
            std::env::temp_dir().join("t9n-session-test"),
            u64::MAX,
            budget,
            100, // per-session cap = 100% of global, so tests bind on the global
            bitmap_cache,
            metrics,
        )
    }

    /// A bare `Session` for the reaper tests, with a throwaway memory handle.
    fn test_session(now: Instant) -> Session {
        let mem = Arc::new(SessionMemoryHandle::new(
            Arc::new(MemoryBudget::new(u64::MAX)),
            u64::MAX,
        ));
        let baseline = mem.reserve(0).expect("baseline");
        Session::new(SessionContext::new(), now, baseline)
    }

    #[test]
    fn is_expired_honours_idle_and_max_age() {
        let now = Instant::now();
        let s = test_session(now);
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
        let aged = test_session(old);
        aged.touch(now); // recently used, but created long ago
        assert!(aged.is_expired(now, IDLE_TIMEOUT, MAX_AGE));
    }

    #[test]
    fn reap_expired_removes_only_stale_sessions() {
        let svc = svc();
        let now = Instant::now();
        let fresh = Arc::new(test_session(now));
        let stale_at = now
            .checked_sub(IDLE_TIMEOUT + Duration::from_secs(60))
            .expect("instant in range");
        let stale = Arc::new(test_session(stale_at));
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

    #[test]
    fn admission_rejects_when_budget_exhausted() {
        // Global budget holds exactly one session's baseline.
        let svc = svc_with_budget(SESSION_BASELINE_BYTES);
        let _first = svc.open_session().expect("first session admitted");
        let err = svc
            .open_session()
            .expect_err("second session rejected at capacity");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        // Reaping the first frees its baseline; a new session is admitted again.
        svc.inner.sessions.lock().unwrap().clear();
        svc.open_session()
            .expect("re-admitted after capacity frees");
    }

    #[test]
    fn sql_info_data_serves_requested_codes() {
        let data = sql_info_data();
        // No specific request → the full flag set.
        let all = data.record_batch(std::iter::empty()).expect("all infos");
        assert_eq!(all.num_rows(), 9, "every appended flag is present");
        // A single requested code → just that flag.
        let one = data
            .record_batch([SqlInfo::FlightSqlServerName as u32])
            .expect("single info");
        assert_eq!(one.num_rows(), 1);
        // Unknown codes filter to nothing rather than erroring.
        let none = data.record_batch([9_999]).expect("unknown info");
        assert_eq!(none.num_rows(), 0);
    }

    #[tokio::test]
    async fn close_session_action_frees_the_session() {
        let svc = svc();
        let token = svc.open_session().expect("open session");

        let mut md = MetadataMap::new();
        md.insert("authorization", format!("Bearer {token}").parse().unwrap());
        assert!(
            svc.resolve_session(&md).expect("resolve").is_some(),
            "token resolves before close"
        );

        let mut req = Request::new(Action {
            r#type: CLOSE_SESSION_ACTION.to_string(),
            body: bytes::Bytes::new(),
        });
        req.metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        FlightSqlService::do_action_fallback(&svc, req)
            .await
            .expect("close session action ok");

        // After teardown the token is SessionLost.
        assert!(
            svc.resolve_session(&md).is_err(),
            "closed token must not resolve"
        );
    }
}
