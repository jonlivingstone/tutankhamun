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
use tracing::{Instrument as _, info, info_span, warn};
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

use crate::aggregate_cache::AggregateCache;
use crate::bitmap_cache::BitmapCache;
use crate::cache::{Cache, Validation};
use crate::memory::{MemoryBudget, SessionMemoryHandle, SessionReservation};
use crate::metrics::Metrics;
use crate::shard_source::{ObjectStoreShardSource, ShardSource, ShardSummary};
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
/// How long a planned-but-unfetched statement ticket lives before the reaper
/// drops it. A client issues `DoGet` immediately after `GetFlightInfo`, so an
/// entry only lingers if the client abandons the query mid-flow; this bounds
/// that leak. Plenty generous for any real client round-trip.
const PLAN_TICKET_TTL: Duration = Duration::from_secs(5 * 60);

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
    /// Whether per-dataset caches re-hash resident shards on fetch (`--verify-shards`).
    validation: Validation,
    /// One [`Cache`] per dataset URL, created on first reference and reused
    /// across queries. A cache must be rooted at the same URL its dataset is
    /// discovered under — the cache fetches shard files by location relative to
    /// its own store — so a single root cache can't serve sub-prefix datasets.
    /// Keying by URL keeps each dataset's cache consistent across queries.
    caches: Mutex<HashMap<String, Arc<Cache>>>,
    /// Resolved shard set per dataset URL, discovered by walking the dataset's
    /// storage and cached for the daemon's life (see [`Self::resolve_summaries`]).
    /// Populated eagerly at startup by `warm_cache` and lazily on first query.
    /// Only non-empty resolutions are cached, so a bogus `FROM` name persists
    /// nothing; a changed dataset needs a restart to refresh.
    summaries: Mutex<HashMap<String, Arc<Vec<ShardSummary>>>>,
    /// Live sessions keyed by their opaque token. Holds the persistent context
    /// (and its session-scoped temp views); reaped on idle / max age.
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    /// Prepared statements keyed by their opaque handle → the SQL text. The
    /// handle is an unguessable UUID; execution re-resolves the session from
    /// request metadata per call (like the ad-hoc statement path), so a prepared
    /// statement sees its session's temp views. Entries are dropped on
    /// `ClosePreparedStatement`. Parameter binding is not yet supported.
    prepared: Mutex<HashMap<String, String>>,
    /// Ad-hoc statements planned by `GetFlightInfo` and awaiting their `DoGet`,
    /// keyed by a server-issued ticket UUID. `DoGet` removes and executes the
    /// stashed plan (single-use); the reaper drops any the client abandons (see
    /// [`PLAN_TICKET_TTL`]). This is what lets a query plan once instead of twice.
    plans: Mutex<HashMap<String, StashedPlan>>,
    /// Daemon-wide memory budget (§2.2). Every session's [`SessionMemoryHandle`]
    /// charges through it; query scans reserve the forward-column working set.
    budget: Arc<MemoryBudget>,
    /// Per-session cap as a percent of [`Self::budget`]'s limit.
    session_pct: u8,
    /// Daemon-shared doc-set bitmap cache (§2.8); threaded into each context as a
    /// `SessionConfig` extension so the scan can probe it.
    bitmap_cache: Arc<BitmapCache>,
    /// Daemon-shared per-shard aggregate cache; threaded into each context as a
    /// `SessionConfig` extension so the time-bucket aggregate path can probe it.
    aggregate_cache: Arc<AggregateCache>,
    /// Daemon metrics (§3.4): session gauge + query latency/counters.
    metrics: Arc<Metrics>,
    /// The object store rooted at [`Self::storage_url`], built once and reused
    /// (the storage root is fixed for the daemon's life). Backs `list_datasets`
    /// so it doesn't reopen a store on every catalog / `/status` call.
    root_store: OnceLock<Arc<dyn ObjectStore>>,
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

/// A query planned by `GetFlightInfo` and awaiting its `DoGet`. The Flight
/// two-phase flow plans for the output schema, then fetches; rather than stuff
/// the SQL in the ticket and re-plan on fetch, we stash the planned `DataFrame`
/// here under a server-issued ticket id and execute it directly on `DoGet` — one
/// plan, one dataset resolution per query. The `DataFrame` is self-contained (it
/// owns its `SessionState` snapshot and the resolved providers are baked into its
/// plan), so it needs nothing from the originating context. `preview` feeds the
/// execute-time query span without keeping the SQL text around (the schema is
/// recovered from the `DataFrame` at execute, so it isn't stored); `prepared`
/// labels that span — ad-hoc statements stash `false`, prepared ones `true`.
struct StashedPlan {
    df: DataFrame,
    prepared: bool,
    preview: String,
    created_at: Instant,
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
    /// Get or create the [`Cache`] rooted at dataset `url`, reusing `store` (the
    /// caller already built it to discover the shard set) rather than reopening one.
    /// Synchronous under the lock — [`Cache::open`] does a one-time dir scan, no
    /// `await`. Only called for datasets confirmed non-empty, so the `caches` map
    /// never grows an entry for a name that isn't a real dataset.
    fn cache_for(&self, url: &str, store: &Arc<dyn ObjectStore>) -> anyhow::Result<Arc<Cache>> {
        let mut caches = self.caches.lock().expect("cache map lock");
        if let Some(c) = caches.get(url) {
            return Ok(Arc::clone(c));
        }
        let cache = Arc::new(Cache::open(
            self.cache_dir.clone(),
            Arc::clone(store),
            url.to_string(),
            self.size_cap,
            self.validation,
        )?);
        caches.insert(url.to_string(), Arc::clone(&cache));
        Ok(cache)
    }

    /// The object store rooted at the storage root, built once and memoized. A
    /// racing first call may build twice; both are equivalent and only one is
    /// kept.
    fn root_store(&self) -> anyhow::Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.root_store.get() {
            return Ok(Arc::clone(store));
        }
        let store = StorageRegistry::from_url(&self.storage_url)?.store();
        let _ = self.root_store.set(Arc::clone(&store));
        Ok(store)
    }

    /// Resolve the dataset's shard set by walking its storage (the object-store
    /// listing — a dataset is its directory of shards), cached per dataset URL for
    /// the daemon's life. The storage layout is the source of truth; the cache
    /// makes discovery once-per-dataset rather than per-query. New/changed shards
    /// in an already-resolved dataset need a restart to refresh (the v1 trade —
    /// see `warm_cache` for the startup pass). An empty/absent dataset resolves
    /// empty and is not cached, so a bogus `FROM` name persists nothing.
    async fn resolve_summaries(
        &self,
        url: &str,
        store: Arc<dyn ObjectStore>,
    ) -> anyhow::Result<Arc<Vec<ShardSummary>>> {
        if let Some(summaries) = self
            .summaries
            .lock()
            .expect("summaries cache lock")
            .get(url)
        {
            return Ok(Arc::clone(summaries));
        }

        let summaries = Arc::new(ObjectStoreShardSource::new(store).discover().await?);
        if !summaries.is_empty() {
            self.summaries
                .lock()
                .expect("summaries cache lock")
                .insert(url.to_string(), Arc::clone(&summaries));
        }
        Ok(summaries)
    }

    /// Resolve the dataset at `url` to a registered table provider via the
    /// cached shard set (`cache_for` + `resolve_summaries` + `from_summaries`),
    /// or `None` if the dataset is empty / not yet populated. The single daemon
    /// seam for "dataset URL → provider"; callers map the `Option`/error to their
    /// own response (a query's "table not found", a listing's "skip").
    async fn table_provider(&self, url: &str) -> anyhow::Result<Option<TutankhamunTableProvider>> {
        // Discover with a lightweight store BEFORE opening a cache, so a
        // `FROM <name>` that resolves to nothing (a typo, an empty or absent
        // dataset) never creates a `Cache` (a dir scan + a `caches` entry). Only a
        // dataset that actually has shards gets one — the same store then backs it,
        // so it isn't built twice. This bounds the `caches`/`summaries` maps by
        // what exists, not by every table name a client references.
        let store = StorageRegistry::from_url(url)?.store();
        let summaries = self.resolve_summaries(url, Arc::clone(&store)).await?;
        if summaries.is_empty() {
            return Ok(None);
        }
        let cache = self.cache_for(url, &store)?;
        Ok(Some(TutankhamunTableProvider::from_summaries(
            url.to_string(),
            cache,
            summaries,
        )?))
    }

    /// Dataset names under the storage root — the top-level directories. A
    /// single delimited LIST (one round-trip, no shard reads); unlike a full
    /// `discover`, it does not fetch every shard's `metadata.json` just to learn
    /// the names. A non-dataset top-level dir would list too, but resolving it
    /// as a table is then a clean "not found".
    async fn list_datasets(&self) -> anyhow::Result<Vec<String>> {
        let listing = self.root_store()?.list_with_delimiter(None).await?;
        let mut names: Vec<String> = listing
            .common_prefixes
            .iter()
            .filter_map(|p| p.parts().last().map(|seg| seg.as_ref().to_string()))
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// Drop sessions past their idle timeout or maximum age, plus any stashed
    /// query plans whose `DoGet` never came (past [`PLAN_TICKET_TTL`]). Returns
    /// the session count reaped.
    fn reap_expired(&self, now: Instant) -> usize {
        // Abandoned GetFlightInfo plans: a normal DoGet removes its own entry, so
        // this only catches clients that planned and never fetched.
        self.plans
            .lock()
            .expect("plans lock")
            .retain(|_, p| now.saturating_duration_since(p.created_at) <= PLAN_TICKET_TTL);

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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_url: String,
        cache_dir: PathBuf,
        size_cap: u64,
        validation: Validation,
        budget: Arc<MemoryBudget>,
        session_pct: u8,
        bitmap_cache: Arc<BitmapCache>,
        aggregate_cache: Arc<AggregateCache>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            inner: Arc::new(ServiceInner {
                storage_url,
                cache_dir,
                size_cap,
                validation,
                caches: Mutex::new(HashMap::new()),
                summaries: Mutex::new(HashMap::new()),
                sessions: Mutex::new(HashMap::new()),
                prepared: Mutex::new(HashMap::new()),
                plans: Mutex::new(HashMap::new()),
                budget,
                session_pct,
                bitmap_cache,
                aggregate_cache,
                metrics,
                root_store: OnceLock::new(),
            }),
        }
    }

    /// Walk every dataset under the storage root and populate the per-dataset
    /// shard-set cache, logging each shard — the startup discovery pass. The
    /// storage layout is the source of truth; this turns the boot-time scan into
    /// a warm cache (rather than discarding it) so first queries skip discovery.
    /// Best-effort: a dataset that fails to list/discover is logged and skipped;
    /// lazy resolution in [`ServiceInner::resolve_summaries`] covers it on first
    /// query. Run as a background task so a large store doesn't gate readiness.
    pub async fn warm_cache(&self) {
        let datasets = match self.inner.list_datasets().await {
            Ok(d) => d,
            Err(e) => {
                warn!(error = ?e, "startup scan: list datasets failed; daemon continues");
                return;
            }
        };
        let base = self.inner.storage_url.trim_end_matches('/');
        let mut total_shards = 0usize;
        let mut total_docs = 0u64;
        for name in datasets {
            let url = format!("{base}/{name}");
            let store = match StorageRegistry::from_url(&url) {
                Ok(r) => r.store(),
                Err(e) => {
                    warn!(dataset = %name, error = ?e, "startup scan: open store failed");
                    continue;
                }
            };
            match self.inner.resolve_summaries(&url, store).await {
                Ok(summaries) => {
                    total_shards += summaries.len();
                    total_docs += log_shards(&summaries);
                }
                Err(e) => warn!(dataset = %name, error = ?e, "startup scan: discover failed"),
            }
        }
        info!(shards = total_shards, total_docs, "shard scan complete");
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
            .with_extension(Arc::clone(&self.inner.bitmap_cache))
            .with_extension(Arc::clone(&self.inner.aggregate_cache));
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

    /// Execute an already-planned `DataFrame` and return its schema + collected
    /// batches — the `DoGet` half of the two-phase flow, shared by the ad-hoc and
    /// prepared paths (the plan was built and resolved in `GetFlightInfo`). Wraps a
    /// per-query span (§3.4) with an `execute` sub-span (nothing to plan here),
    /// records the metrics histogram + `rows`/`error`, and labels the span with
    /// `prepared`. The schema is recovered from `df` before `collect` consumes it.
    #[allow(clippy::result_large_err)]
    async fn execute_df(
        &self,
        df: DataFrame,
        prepared: bool,
        preview: String,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), Status> {
        let span = info_span!(
            "query",
            sql = %preview,
            prepared,
            rows = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        async {
            let schema = df.schema().inner().clone();
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

    /// Plan-stash a `DataFrame` under a fresh server-issued id and return the id.
    /// Both `GetFlightInfo` handlers call this and then build their own ticket
    /// carrying the id; `do_get_stashed` consumes it. `prepared` labels the
    /// execute-time span; `sql` feeds the bounded preview.
    fn stash_plan(&self, df: DataFrame, prepared: bool, sql: &str) -> String {
        let id = Uuid::new_v4().to_string();
        self.inner.plans.lock().expect("plans lock").insert(
            id.clone(),
            StashedPlan {
                df,
                prepared,
                preview: sql_preview(sql),
                created_at: Instant::now(),
            },
        );
        id
    }

    /// Execute the single-use stashed plan named by `handle` and stream the
    /// result — the shared `DoGet` body for ad-hoc and prepared statements. Both
    /// carry a per-execution plan id in their (opaque) ticket handle; planning
    /// happened in `GetFlightInfo`, so this never re-plans or re-resolves. A
    /// consumed/expired id is a clean not-found (the client re-issues
    /// `GetFlightInfo`). `collect` drives the plan; the FTGS/scan execs bridge
    /// async→sync on their own scoped threads (`block_on_scan`), so awaiting here
    /// parks no worker on a nested `block_on`.
    #[allow(clippy::result_large_err)]
    async fn do_get_stashed(
        &self,
        handle: &[u8],
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let plan_id = String::from_utf8(handle.to_vec()).map_err(|e| {
            Status::invalid_argument(format!("ticket handle not a UTF-8 plan id: {e}"))
        })?;
        let stashed = self
            .inner
            .plans
            .lock()
            .expect("plans lock")
            .remove(&plan_id)
            .ok_or_else(|| {
                Status::not_found("plan not found or expired; re-issue GetFlightInfo")
            })?;
        let (schema, batches) = self
            .execute_df(stashed.df, stashed.prepared, stashed.preview)
            .await?;
        Ok(batches_stream(schema, batches))
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

/// Stream `batches` as a `DoGet` response under an explicit `schema` (the vec may
/// be empty, so the schema can't be derived from a batch). The shared encoder
/// path for query results and metadata.
fn batches_stream(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Response<<TutankhamunFlightSqlService as FlightService>::DoGetStream> {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::iter(batches.into_iter().map(Ok)))
        .map_err(Status::from);
    Response::new(Box::pin(stream))
}

/// Stream a single metadata `RecordBatch` as a `DoGet` response.
fn batch_stream(
    batch: RecordBatch,
) -> Response<<TutankhamunFlightSqlService as FlightService>::DoGetStream> {
    batches_stream(batch.schema(), vec![batch])
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
        // Plan once to learn the output schema, then stash the planned DataFrame
        // and hand back a ticket that names it — `do_get_statement` executes it
        // directly instead of re-planning the SQL. The stashed DataFrame is
        // self-contained (it owns its SessionState snapshot and the resolved
        // providers), so the local `ctx` can drop here.
        //
        // The ticket is now valid only on this daemon (the plan lives in memory
        // here). For single-node v1 that's correct — the endpoint has no
        // `location`, meaning "fetch from this same server" — and it's the same
        // affinity sessions already require. When multi-node HA lands, set
        // `FlightEndpoint.location` to this daemon's address so a proxy routes the
        // `DoGet` back to the daemon holding the plan.
        let session = self.resolve_session(request.metadata())?;
        let ctx = self.context_for(session.as_ref());
        let (df, schema) = plan(&ctx, &query.query)
            .instrument(info_span!("plan"))
            .await?;

        let ticket_id = self.stash_plan(df, false, &query.query);
        let ticket = TicketStatementQuery {
            statement_handle: ticket_id.into_bytes().into(),
        };
        flight_info_for(
            schema.as_ref(),
            Ticket::new(ticket.as_any().encode_to_vec()),
            request,
        )
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.do_get_stashed(&ticket.statement_handle).await
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
        // Plan this execution once and stash it, exactly like the ad-hoc path —
        // `do_get_prepared_statement` then runs the stashed plan instead of
        // re-planning. The long-lived `prepared` map (SQL by handle) is untouched;
        // it still feeds re-planning on the next execution and the DDL/`do_put`
        // paths. The ticket carries the per-execution plan id (not the prepared
        // handle): swap it into the echoed command so the blanket `do_get` routes
        // a `CommandPreparedStatementQuery` whose handle is the plan id.
        let sql = self.prepared_sql(&query.prepared_statement_handle)?;
        let session = self.resolve_session(request.metadata())?;
        let ctx = self.context_for(session.as_ref());
        let (df, schema) = plan(&ctx, &sql).instrument(info_span!("plan")).await?;

        let plan_id = self.stash_plan(df, true, &sql);
        let mut ticket_cmd = query;
        ticket_cmd.prepared_statement_handle = plan_id.into_bytes().into();
        let ticket = Ticket::new(ticket_cmd.as_any().encode_to_vec());
        flight_info_for(schema.as_ref(), ticket, request)
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // The handle is the per-execution plan id stashed by GetFlightInfo.
        self.do_get_stashed(&query.prepared_statement_handle).await
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
                // Raced with eviction / an empty dir → `None`, just skip it.
                let Some(p) = self
                    .inner
                    .table_provider(&url)
                    .await
                    .map_err(|e| Status::internal(format!("dataset schema: {e}")))?
                else {
                    continue;
                };
                builder
                    .append(
                        CATALOG_NAME,
                        SCHEMA_NAME,
                        &name,
                        "TABLE",
                        p.schema().as_ref(),
                    )
                    .map_err(|e| metadata_error(&e))?;
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
        // Resolve through the daemon's per-dataset discovery cache (warm hits skip
        // the walk); the provider holds the same `Arc` the scan reuses. An
        // empty/absent dataset is a clean "table not found" (None), not an
        // internal error.
        match self.inner.table_provider(&url).await {
            Ok(Some(p)) => Ok(Some(Arc::new(p) as Arc<dyn TableProvider>)),
            Ok(None) => Ok(None),
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
/// Shared by the ad-hoc and prepared `GetFlightInfo` paths so they derive it
/// identically.
async fn plan(ctx: &SessionContext, query: &str) -> Result<(DataFrame, SchemaRef), Status> {
    let df = ctx.sql(query).await.map_err(|e| plan_error(&e))?;
    let schema = df.schema().inner().clone();
    Ok((df, schema))
}

/// Log each shard (location, doc count, time range) and return the doc total —
/// the per-dataset slice of the startup discovery inventory `warm_cache` emits.
fn log_shards(summaries: &[ShardSummary]) -> u64 {
    let mut total_docs = 0;
    for s in summaries {
        info!(
            location = %s.location.as_ref(),
            num_docs = s.metadata.num_docs,
            time_range_start = s.metadata.time_range_start,
            time_range_end = s.metadata.time_range_end,
            "shard"
        );
        total_docs += s.metadata.num_docs;
    }
    total_docs
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
        let aggregate_cache = Arc::new(AggregateCache::new(Arc::new(SessionMemoryHandle::new(
            Arc::clone(&budget),
            limit,
        ))));
        let metrics = Metrics::new(
            Arc::clone(&budget),
            Arc::clone(&bitmap_cache),
            Arc::clone(&aggregate_cache),
        );
        TutankhamunFlightSqlService::new(
            "memory:///".to_string(),
            std::env::temp_dir().join("t9n-session-test"),
            u64::MAX,
            Validation::Trust,
            budget,
            100, // per-session cap = 100% of global, so tests bind on the global
            bitmap_cache,
            aggregate_cache,
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

    /// A `StashedPlan` over a trivial, storage-free query — enough to exercise the
    /// registry (insert / single-use removal / reaping) and `execute_df`.
    async fn test_stashed_plan(created_at: Instant) -> StashedPlan {
        let ctx = SessionContext::new();
        let df = ctx.sql("SELECT 1 AS x").await.expect("plan trivial query");
        StashedPlan {
            df,
            prepared: false,
            preview: "SELECT 1 AS x".to_string(),
            created_at,
        }
    }

    /// The per-dataset summary cache: an empty dataset dir resolves empty and is
    /// NOT cached; once a shard is written, a resolve discovers it by walking, and
    /// a second resolve reuses the cached `Arc` (no re-walk).
    #[tokio::test(flavor = "multi_thread")]
    async fn resolve_summaries_caches_and_skips_empty() {
        use crate::shard::DiskShardWriter;

        let root = tempfile::tempdir().expect("tmpdir");
        let storage_url = url::Url::from_directory_path(root.path())
            .expect("absolute")
            .to_string();
        let budget = Arc::new(MemoryBudget::new(u64::MAX));
        let mk_cache = || Arc::new(SessionMemoryHandle::new(Arc::clone(&budget), u64::MAX));
        let bitmap_cache = Arc::new(BitmapCache::new(mk_cache()));
        let aggregate_cache = Arc::new(AggregateCache::new(mk_cache()));
        let metrics = Metrics::new(
            Arc::clone(&budget),
            Arc::clone(&bitmap_cache),
            Arc::clone(&aggregate_cache),
        );
        let svc = TutankhamunFlightSqlService::new(
            storage_url.clone(),
            root.path().join(".cache"),
            u64::MAX,
            Validation::Trust,
            budget,
            100,
            bitmap_cache,
            aggregate_cache,
            metrics,
        );
        let ds_url = format!("{}/ds", storage_url.trim_end_matches('/'));
        let store = || {
            StorageRegistry::from_url(&ds_url)
                .expect("registry")
                .store()
        };

        // No shards yet: resolves empty and is NOT cached.
        let empty = svc
            .inner
            .resolve_summaries(&ds_url, store())
            .await
            .expect("resolve empty");
        assert!(empty.is_empty());
        assert!(
            !svc.inner.summaries.lock().unwrap().contains_key(&ds_url),
            "an empty dataset must not be cached, so it resolves again once it has shards",
        );

        // Write one shard, then resolve twice — discovery finds it by walking;
        // the second resolve reuses the cached Arc.
        let ds_dir = root.path().join("ds");
        let mut w = DiskShardWriter::new(&ds_dir.join("shard-0"), (0, 0)).expect("writer");
        w.add_metric("x", vec![1, 2, 3]).expect("add_metric");
        w.finalize().expect("finalize");

        let first = svc
            .inner
            .resolve_summaries(&ds_url, store())
            .await
            .expect("resolve");
        assert_eq!(first.len(), 1, "the written shard is discovered by walking");
        let second = svc
            .inner
            .resolve_summaries(&ds_url, store())
            .await
            .expect("resolve");
        assert!(
            Arc::ptr_eq(&first, &second),
            "the rerun reuses the cached shard set (no re-walk)",
        );
    }

    /// `do_get_stashed` executes the stashed plan WITHOUT re-resolving the dataset:
    /// after clearing the summaries cache, a `DoGet` still runs and leaves the cache
    /// empty (the scan reads shards via the `Cache`, never the summaries map). This
    /// is the single-plan "no re-resolve" guard (ad-hoc and prepared share this
    /// path).
    #[tokio::test]
    async fn do_get_stashed_does_not_re_resolve() {
        let svc = svc();
        let plan = test_stashed_plan(Instant::now()).await;
        svc.inner
            .plans
            .lock()
            .unwrap()
            .insert("tkt".to_string(), plan);
        // Whatever a prior resolve may have cached, start from empty.
        svc.inner.summaries.lock().unwrap().clear();

        let resp = svc.do_get_stashed(b"tkt").await;
        assert!(resp.is_ok(), "stashed plan executes");
        assert!(
            svc.inner.summaries.lock().unwrap().is_empty(),
            "DoGet ran the stash without resolving any dataset",
        );
    }

    /// Resolving a name that isn't a real dataset (no shards) returns `None`
    /// and creates no `Cache` entry — so a client naming bogus/absent tables
    /// can't grow the `caches` map.
    #[tokio::test(flavor = "multi_thread")]
    async fn table_provider_skips_cache_for_absent_dataset() {
        let svc = svc();
        let url = "memory:///no_such_dataset";
        let provider = svc.inner.table_provider(url).await.expect("resolve");
        assert!(provider.is_none(), "absent dataset resolves to no provider");
        assert!(
            !svc.inner.caches.lock().unwrap().contains_key(url),
            "no Cache is opened for a name that isn't a dataset",
        );
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

    /// The reaper drops a planned-but-unfetched ticket past its TTL and keeps a
    /// fresh one (an abandoned `GetFlightInfo` whose `DoGet` never came).
    #[tokio::test]
    async fn stashed_plan_reaped_by_ttl() {
        let svc = svc();
        let now = Instant::now();
        let stale_at = now
            .checked_sub(PLAN_TICKET_TTL + Duration::from_secs(1))
            .expect("instant in range");
        let fresh = test_stashed_plan(now).await;
        let stale = test_stashed_plan(stale_at).await;
        {
            let mut map = svc.inner.plans.lock().unwrap();
            map.insert("fresh".to_string(), fresh);
            map.insert("stale".to_string(), stale);
        }
        svc.inner.reap_expired(now);
        let map = svc.inner.plans.lock().unwrap();
        assert!(map.contains_key("fresh"));
        assert!(!map.contains_key("stale"));
    }

    /// `DoGet` on a ticket with no stashed plan (expired / re-fetched / bogus) is
    /// a clean not-found, telling the client to re-issue `GetFlightInfo`.
    #[tokio::test]
    async fn do_get_unknown_ticket_is_not_found() {
        let svc = svc();
        let ticket = TicketStatementQuery {
            statement_handle: Uuid::new_v4().to_string().into_bytes().into(),
        };
        // The Ok variant (a boxed stream) isn't `Debug`, so match rather than
        // `expect_err`.
        let err =
            FlightSqlService::do_get_statement(&svc, ticket, Request::new(Ticket::new(vec![])))
                .await
                .err()
                .expect("unknown ticket errors");
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    /// A stashed plan is consumed by its `DoGet`: the entry is gone afterwards, so
    /// a replay can't re-run a query the client already fetched.
    #[tokio::test]
    async fn stash_is_single_use() {
        let svc = svc();
        let plan = test_stashed_plan(Instant::now()).await;
        svc.inner
            .plans
            .lock()
            .unwrap()
            .insert("tkt".to_string(), plan);

        let ticket = TicketStatementQuery {
            statement_handle: "tkt".to_string().into_bytes().into(),
        };
        let resp =
            FlightSqlService::do_get_statement(&svc, ticket, Request::new(Ticket::new(vec![])))
                .await;
        assert!(resp.is_ok(), "stashed plan executes");
        assert!(
            !svc.inner.plans.lock().unwrap().contains_key("tkt"),
            "the plan is removed on DoGet (single-use)",
        );
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
