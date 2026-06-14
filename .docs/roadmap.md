# Tutankhamun v1 — implementation checklist

Trackable list of features to implement for v1. Check items off as
they ship. Each item points at the design section in
[`tutankhamun.md`](tutankhamun.md) for context.

Not prioritized — order of implementation is an engineering
decision. Items within an area are roughly in dependency order.

For v2 / v3 features and explicit non-goals, see the design doc
directly.

---

## Project bootstrap

- [x] Cargo workspace structure (`tutankhamun-server`,
      `tutankhamun-client`, shared crates)
- [x] Layered config: CLI (`clap`) + env + file + defaults
      (`figment` or similar) — §1.3
- [x] Graceful shutdown — SIGTERM → drain → deregister → exit, with
      configurable timeout — §1.3
- [x] Ops HTTP server (default port 8080) with `/healthz`,
      `/readyz` — §1.3
- [x] `tokio` runtime configuration in `main()` (worker thread
      count, etc.) — §2.3
- [x] `rayon` thread pool configuration as a `OnceLock` — §2.3
- [x] Async-to-Rayon dispatch helper (oneshot channel + Tokio
      future) — §2.3

## Storage backends

- [x] `object_store` integration with all backends enabled (S3,
      S3-compatible, GCS, Azure Blob, local filesystem, HTTP,
      in-memory) — §1.4
- [x] Credential chain wiring (env, instance metadata, IRSA,
      Workload Identity, SSO) — no hardcoded credentials — §1.4
- [x] Local hot-storage cache — directory creation, XDG defaults
      via `directories` crate — §1.5
- [x] Cache size enforcement (`--cache-size` accepts `100GB` /
      `50%` / etc.) with 10 GB default — §1.5
- [ ] Cache 5 %-free-space floor with `--cache-min-free-pct`
      override — §1.5
- [x] LRU eviction — §1.5
- [ ] `--pin-datasets` always-keep flag — §1.5
- [x] Persistent cache across restarts (scan cache dir, register
      existing files) — §1.5
- [x] Content-hash validation on shard load — opt-in bit-rot guard
      (`--verify-shards`); trust-by-default with atomic installs — §1.5
- [ ] Configurable hot-set pre-warm (`--prewarm`) — §1.5

## Engine — storage format (shards)

- [x] Shard directory layout (`metadata.json`, `metrics.arrow`,
      `postings/<field>.fst`, `postings/<field>.posting`) — §2.1
- [x] `metadata.json` schema (Arrow schema, numDocs, time range,
      format version, content hashes) — §2.1
- [x] Forward column writer — uncompressed single-batch Arrow IPC
      via `arrow-rs` — §2.1
- [x] Forward column reader — mmap via `memmap2` + zero-copy
      `&[i64]` cast via `bytemuck` — §2.1
- [x] Inverted index writer — `roaring` bitmaps per term + `fst`
      term dictionary — §2.1
- [x] Inverted index reader — FST range scan + Roaring bitmap
      iteration — §2.1
- [ ] Optional Parquet export of forward columns — §2.1

## Engine — abstractions

- [x] `Shard` trait — `forward_column()`, `inverted_index()`,
      `time_range()`, `num_docs()`, `schema()` — §2.1, §3.3 v1 disciplines
- [x] `DiskShard` implementation of `Shard` — §2.1, §3.3 v1
- [x] `ShardSource` trait — server-side abstraction over where
      shards come from — §3.3 v1 disciplines
- [x] Object-storage `ShardSource` implementation — §3.3 v1
- [ ] `ShardLocator` trait — client-side daemon discovery — §1.3
- [ ] K8s DNS `ShardLocator` implementation — §1.3
- [ ] Static-file `ShardLocator` implementation — §1.3
- [ ] Shard manager — composes shards from multiple sources;
      handles registration / eviction — §3.3 v1
- [x] Time-range query pruning (skip shards outside requested
      time range) — §3.3 v1 disciplines

## Engine — memory model

- [x] `MemoryBudget` global struct with `AtomicU64` charge counter
      — §2.2 (`memory::MemoryBudget`: a lock-free CAS-loop `reserve`; an
      `Arc` owned by the daemon — built in `serve`, held in the FlightSQL
      `ServiceInner` — rather than a process global, so it's testable)
- [x] `MemoryReservation` RAII guard (drop returns bytes) — §2.2
- [x] `SessionMemoryHandle` — per-session sub-budget with cap — §2.2
      (charges both the session counter and the global; `SessionReservation`
      releases both on drop)
- [x] Hard claim-or-fail allocation API (`reserve(n) -> Result<...,
      BudgetExceeded>`) — §2.2 (`BudgetExceeded` carries the scope —
      session vs global — requested, and available)
- [x] Admission control in `OpenSession` handler — §2.2 (the handshake
      reserves a fixed `SESSION_BASELINE_BYTES` baseline through the new
      session's handle; if it can't be satisfied the daemon is at capacity
      and the handshake returns `resource_exhausted`)
- [x] Per-session cap (default 20 % of global, configurable) — §2.2
      (`--max-session-memory-pct` / `TUT_MAX_SESSION_MEMORY_PCT`; global
      cap is `--memory-limit` / `TUT_MEMORY_LIMIT`, an absolute size,
      default 4GB — percent-of-RAM deferred)
- [x] mmap accounting via forward-column file sizes on shard open
      — §2.2 (the per-session handle threads to the otherwise
      session-agnostic execs as a `DataFusion` `SessionConfig` extension;
      `fetch_selected_shards` charges each opened shard's `metrics.arrow`
      byte size and holds the reservation alongside the shard, so an
      over-budget scan fails — per-query, session survives — and the
      charge releases when the shard drops. The `t9n sql` CLI sets no
      extension and charges nothing)

## Engine — group lookup

- [x] `GroupLookup` enum with backing variants — §2.5
- [x] `ConstantGroupLookup` — §2.5
- [x] `BitSetGroupLookup` — §2.5
- [x] `ByteGroupLookup` — §2.5
- [x] `U16GroupLookup` — §2.5
- [x] `U32GroupLookup` — §2.5
- [x] In-place upgrade between backings when cardinality crosses
      thresholds — §2.5
- [x] `next_group_callback(doc_ids, &mut BitTree)` dispatch — §2.5
- [ ] Memory cost reporting to `SessionMemoryHandle` — §2.5
      (`GroupLookup::memory_used()` exposed; §2.2's `SessionMemoryHandle`
      has now landed, so this is unblocked — thread the handle into the
      FTGS rayon per-shard loop and reserve the group-lookup / stat
      buffers, the natural next slice)

## Engine — FTGS

- [x] Four-level cursor (`next_field` / `next_term` / `next_group`
      / `group_stats`) — §2.6
- [x] Critical loop: doc-ID batch → group lookup callback →
      stat accumulation into `term_grp_stats[stat][group]` — §2.6
      (extensible `Stat` seam: scalar sum/count/min/max; mergeable
      sketches land as new variants)
- [x] Ordering-invariant enforcement (terms sorted, groups
      ascending, fields in declaration order) — §2.6
- [x] Per-shard FTGS execution (single-threaded per shard, run
      via Rayon) — §2.6, §2.3 (`ftgs_scan_merge` fans out over the
      bounded pool via `runtime::run_cpu` + `par_iter`; sequential
      when the pool isn't initialised)
- [x] Shard-fan-out merge (within a daemon) — §2.6
      (`merge_ftgs`: k-way merge of per-shard rows, combining stats
      on `(field, term, group)` via `StatSpec::combine`)
- [x] `GSVector`-equivalent two-level bitmap for merge — §2.6
      (row-per-`(field,term,group)` granularity + `BitTree` make a
      separate merge-time group bitmap unnecessary)
- [x] Arrow record-batch output (1024 / 4096 row default
      batching) — §2.6
      (both SQL exec nodes — `FtgsAggExec` and `TutankhamunExec` — chunk
      their output to the session's configured `batch_size` via a zero-copy
      `chunk_batch` over `RecordBatch::slice`, honoring DataFusion's own knob
      rather than a fixed cap; the native/Flight path will set its own default
      when built)
- [ ] Same merge code reused at client layer (cross-daemon) — §2.6
      (`merge_ftgs` is already the shared primitive; cross-daemon
      wiring still to build)
- [ ] Per-shard partial-aggregate cache (lazy partial cube) — §2.6
      (shards are immutable, so a per-`(shard content hash, dimension-set,
      column, stat)` partial is permanently valid and composes across
      shards via `StatSpec::combine` — a cuboid that never needs
      maintenance. Cache at the `FtgsAggExec` per-shard boundary so repeat
      aggregates skip the column re-scan and only re-merge. Scope:
      - **Unfiltered only.** A filtered partial depends on the doc-set,
        unbounded by predicate — already covered by the §2.8 bitmap cache.
      - **Apex + low-cardinality GROUP BY.** Cache the no-group apex
        cuboid and grouped cuboids whose group-lookup backing is
        `Constant`/`BitSet`/`Byte`/`Char` (≤ ~65 K groups); fall back to
        live scan when it spills to `Int`. The backing tier the FTGS run
        already chose *is* the cardinality gate — no extra estimation.
        High-card keys make a cuboid ≈ the raw column (no work saved) and
        are typically one-off (no reuse); low-card cuboids are small, hot,
        and roll up (`GROUP BY (a,b)` answers `GROUP BY a`).
      - **Byte-accounted LRU, not entry-counted.** A scalar/`Avg` cell is
        8–16 B; a sketch cell (`Hll`/`TDigest`/`Theta`/`TopK`) is ~KB, so
        a grouped sketch cuboid is KB × group count. Cache sketches too —
        they save the *most* recompute (a full hashing pass) and merge
        exactly (register-wise max adds no error) — but charge them by
        bytes so the `used_bytes` + LRU cap evicts the heavy sketch
        cuboids first, mirroring the shard and bitmap caches.)

## Engine — sessions

- [~] Session struct — §2.4 (`flight_sql::Session`: a persistent
      `SessionContext` + liveness timestamps. Per §2.8 the group lookup /
      stat stack / dynamic metrics are SQL state inside the context, not a
      typed struct; the memory handle waits on §2.2)
- [x] `OpenSession` handler — token issuance — §2.4 (the FlightSQL
      handshake doubles as session-open: mints an opaque server-issued token
      the client echoes as a bearer. Admission check waits on §2.2;
      time-range shard selection happens per-query in the scan)
- [x] `CloseSession` handler — explicit teardown — §2.4 (`FlightSQL` 56.2.1
      has no native CloseSession action, so it's a custom `do_action`
      advertised via `list_custom_actions` and handled in `do_action_fallback`:
      removes the session named by the bearer token, freeing its state at once
      rather than waiting for the idle/max-age reaper)
- [x] Idle timeout reaper (default 30 min) — §2.4 (const; configurable
      knob deferred)
- [x] Hard maximum age reaper (default 4 h) — §2.4 (const; configurable
      knob deferred)
- [x] `SessionLost` error on daemon-crashed-mid-session — §2.4
      (an unknown/expired token returns `not_found`; the client reopens)
- [x] Opaque token format (don't leak internals) — §2.4 (UUID v4,
      server-issued; client echoes, never parses)
- [~] Stat stack — §2.6 (per §2.8 expressed as SQL computed columns;
      native `PushStat`/`PopStat` deferred unless a workflow needs them)
- [~] Dynamic metric allocation + update — §2.6 (per §2.8 a computed
      column / `CREATE VIEW`; native typed op deferred)
- [~] Regroup operations (filter, bucket, query-based, regex,
      random, intersect) — §2.6 (per §2.8 expressed as SQL `WHERE` /
      subqueries / temp views; native typed regroup deferred)

## Wire / protocol

- [x] `tonic` gRPC server setup over HTTP/2 — §1.2
      (`flight_sql::serve` runs a `tonic::Server` on `grpc_addr`, bound before
      readiness and drained via the shared `ShutdownHandle`, mirroring the ops
      HTTP task)
- [ ] `tutankhamun.v1.SessionControl` protobuf definitions
      (`OpenSession`, `CloseSession`, `Regroup`, `PushStat`,
      `PopStat`, `MetricRegroup`, `GetStatus`, etc.) — §1.2
- [ ] `SessionControl` service implementation — §1.2
- [x] `arrow.flight.protocol.FlightService` registration — §1.2
      (registered via `arrow-flight`'s `FlightServiceServer` wrapping the
      `FlightSqlService` impl)
- [~] `DoGet(Ticket)` for FTGS result streaming as Arrow record
      batches — §1.2 (statement `DoGet` works — `do_get_statement` streams SQL
      results via `FlightDataEncoderBuilder`; the FTGS-native ticket and the
      `SessionControl` streaming path land with the session slice)
- [x] DataFusion embedded as a dependency — §3.2
- [x] Tutankhamun `TableProvider` implementation (Tier 1: projection
      + equality/range filter pushdown; `t9n sql` CLI verb) — §3.2
- [x] Time-range shard pruning in the SQL scan (prune shards by a
      predicate on the time column before fetch) — §3.2
- [x] Aggregation / GROUP BY pushdown from DataFusion → Tutankhamun
      FTGS scan (Tier 2/3) — §3.2
      (single- *and* multi-column `Int`/`String` GROUP BY *and* global
      no-GROUP-BY aggregates + COUNT(*)/SUM/MIN/MAX/AVG/approx_count_distinct
      pushed via an optimizer rule → `FtgsAggExec`, including the `String` NULL
      group; multi-column regroups the prefix columns into a combo group id
      (shared across shards) and scans the last column; the global path reuses
      `aggregate_docs` over the filtered set → one row with SQL empty-input
      semantics (count/approx → 0, sum/min/max/avg → NULL); `approx_distinct`
      works on `String` args too, hashing the inverted-index terms (no forward
      column) so the sketch still merges across shards; unsupported shapes fall
      back to DataFusion)
- [~] FlightSQL service implementation — §3.2
      (ad-hoc statement path: `get_flight_info_statement` plans for the output
      schema and `do_get_statement` executes through the in-process DataFusion
      engine [`sql::session_context`]; `do_handshake` opens a session and
      `do_put_statement_update` runs DDL/DML — `CREATE VIEW` etc. — against the
      session context; datasets under the storage root are addressable as tables
      by name via a lazy, registerable `SchemaProvider`. Prepared statements
      (create/get/`do_get`/update/close, no-parameter case) and the
      catalog-metadata RPCs (catalogs/schemas/tables/table-types, datasets as
      `TABLE` + session temp views as `VIEW`, reported under `datafusion`/`public`)
      now work; an explicit `CloseSession` custom action frees a session. Still
      remaining: prepared-statement *parameter binding* and the key/XDBC-info
      RPCs. Transactions are intentionally left unimplemented —
      a pure no-op until there is mutable state to transact, so honest
      `unimplemented` beats a fake commit/rollback)
- [x] `GetSqlInfo` capability RPC (`get_flight_info_sql_info` /
      `do_get_sql_info`) — §3.2. The connect-time capability probe JDBC/ADBC/GUI
      clients (DBeaver, DataGrip) call during connection setup; previously returned
      `unimplemented`, which could block them from connecting. Now serves a fixed
      `SqlInfo` flag set (server name `Tutankhamun` + crate version, read-only =
      false since `CREATE VIEW` DDL is accepted, SQL not Substrait, no
      transactions, `"`-quoted lowercase-folded identifiers per DataFusion),
      filtered to the codes the client requests. Browsing (catalog RPCs) and
      querying (statement `do_get`) already worked; this closes the JDBC/native
      connect path. **ODBC is out of scope server-side**: there is no first-party
      Arrow Flight SQL ODBC driver — reaching us over ODBC needs a third-party /
      ADBC-ODBC bridge, a client-side driver concern, not a t9n RPC)
- [x] Session-aware SQL execution — §3.2/§2.8 (sessions persist a
      `SessionContext` across calls, so session-scoped temp views/tables survive
      — the §2.8 name layer. The realization layer now lands too: a daemon-shared
      `bitmap_cache::BitmapCache` keys per-shard matched-doc Roaring bitmaps by
      `(shard identity, normalized clause set, visibility)` and serves exact hits
      (cross-session reuse of identical filters) plus monotone narrowing — a new
      filter that adds conjuncts reuses the cached bitmap and intersects only the
      delta (`result = matched_doc_set(cached) ∩ matched_doc_set(delta)`) instead
      of rescanning. Reached at the `fetch_selected_shards` chokepoint (both the
      row scan and the FTGS aggregate) via a `SessionConfig` extension, the same
      mechanism as the §2.2 memory handle; the `t9n sql` CLI sets no extension and
      caches nothing. Bounded by a `SessionMemoryHandle` sub-budget
      (`--bitmap-cache-pct`, default 25%, 0 disables) that charges the §2.2 global
      budget, with LRU eviction. Reserved: a `Visibility` key slot for v2 auth.
      Deferred: range-tightening narrowing and FTGS sub-result caching)
- [x] Session-affinity metadata header
      (`x-tutankhamun-session-id`) published in gRPC responses —
      §2.4 (set on the handshake response; also accepted as an input
      fallback to the bearer token for proxy mode)

## Routing

- [ ] Client library tracks `session_token → daemon_address` map
      (default mode) — §2.4
- [ ] Client library proxy mode (opt-in via config) — sends all
      session traffic to a proxy address, includes session ID
      header — §2.4

## Ingest

- [x] Batch ingest pipeline — Rust port of TSV converter — §3.3 v1
- [x] Output Tutankhamun-format shards (Arrow IPC + Roaring +
      FST) — §3.3 v1
- [x] Upload to object storage via `object_store` — §3.3 v1
- [x] Native Parquet ingest — `t9n ingest file.parquet` reads the typed
      Arrow schema directly (format inferred from extension or `--format`).
      Numeric columns become scaled `i64`: integers as-is, `Decimal128(p,s)`
      as the mantissa (scale from the schema), floats as `round(v × 10^scale)`
      (`--scale`, default 3); the per-field scale is recorded in `metadata.json`.
      Deferred: query-time decimal *presentation* (aggregates return the scaled
      integer), schema-inferred column mapping, Decimal256, CSV float-via-scale
- [ ] Daemon writable local state directory (configured via
      `--state-dir`) — for cache in v1; for WAL in v2 — §3.3 v1
- [ ] `flamdex-to-tutankhamun` migration tool — read old Imhotep
      Flamdex shards, write in new format — §2.1

## Query language — native Python client

- [ ] `tutankhamun` PyPI package skeleton — §3.2
- [ ] Connection / session classes (`tk.connect(...).session(...)`)
      — §3.2
- [ ] Fluent API (`.filter()`, `.group_by()`, `.select()`,
      `.fetch()`) — §3.2
- [ ] Lazy execution (composes SQL fragments until `.fetch()`) —
      §3.2
- [ ] Session token management — §3.2
- [ ] Dynamic metric definition (`session.define(name, expr)`) —
      §3.2
- [ ] Result conversion: `to_arrow()`, `to_pandas()`,
      `to_polars()`, `to_duckdb()` — §3.2
- [ ] Context manager support (`with session: ...`) — §3.2
- [ ] Decision: build on `ibis` or roll our own — §3.2

## Approximate aggregations

- [x] Pick crate strategy — `hyperloglogplus` + `tdigest` + custom
      theta (pure Rust; no C++ toolchain) — §3.1
- [~] `approx_count_distinct(field, [precision])` (HLL) — §3.1
      (pushed through the SQL `GROUP BY`/global paths via the `StatValue`
      seam, over `Int`/`Metric` forward columns or `String` index terms;
      optional `precision` arg still to add)
- [~] `approx_percentile(field, p, [compression])` (t-digest) —
      §3.1 (SQL `approx_percentile_cont(col, p [, centroids])` pushed
      through the `GROUP BY`/global paths via the `StatValue` seam over
      `Int`/`Metric` columns; each group buffers values and builds the
      digest at the merge edge; returns the column's `Int64` type like
      DataFusion. `Float`-column percentiles await float metric storage)
- [~] `approx_top_k(field, k, [capacity])` — §3.1 (the `k` most
      frequent values per group, returned as
      `List<Struct<value, count>>`. No `DataFusion` built-in, so a custom
      `approx_top_k` UDAF is registered — it makes the function available
      in SQL and is the row-scan fallback; the FTGS pushdown reuses the
      inverted index / forward column to count exactly per shard, keeps
      the top-`capacity`, and merges across shards via the `StatValue`
      seam — no Count-Min sketch needed. `String`/`Int`/`Metric` columns)
- [x] `theta(field, [nominal_entries])` returning Arrow `Binary`
      — §3.1 (a custom KMV theta sketch; the `theta` UDAF builds it as a
      `Binary` column — FTGS pushdown over `String`/`Int`/`Metric` columns,
      merged by union via the `StatValue` seam, with the UDAF as fallback)
- [x] `theta_intersect(a, b)` — §3.1 (a scalar UDF over two `Binary`
      sketch columns → the estimated overlap `Int64`; cohort intersection
      HLL can't do)
- [~] Sketch merge in the FTGS merge path (sketches are
      mergeable by construction) — §3.1 (HLL registers union via
      the `StatValue`/`combine_stats` seam; t-digest/theta extend it)
- [~] Sketches as Arrow record-batch columns (int64 for scalars,
      `Binary` for raw thetas) — §3.1 (HLL estimate emitted as a
      `UInt64` column; `Binary` arrives with theta)

## Observability

- [x] Prometheus `/metrics` endpoint on the ops port — §3.4
      (`metrics::Metrics`, a daemon-shared holder rendered as hand-rolled
      Prometheus text on the axum ops server beside `/healthz`/`/readyz`;
      no client-crate dependency)
- [~] Standard metrics (request rates, latency histograms, pool
      queue depths, memory pool, session counts, shard cache
      hit/miss, mmap'd bytes, error counts) — §3.4
      (shipped: `tut_memory_{limit,used}_bytes`, `tut_sessions_live`,
      `tut_bitmap_cache_{hits,narrows,misses}_total` + `_used_bytes`,
      `tut_build_info`, and query performance —
      `tut_query_duration_seconds` histogram, `tut_queries_total`,
      `tut_query_errors_total` (recorded around `df.collect()` in the
      statement/prepared `do_get` paths). Gauges are pulled live from the
      §2.2 budget / §2.8 cache; the session gauge + query counters are
      bumped by the flight service. Deferred: per-RPC request counts for
      the cheap metadata/DDL RPCs, shard-cache hit/miss + mmap'd bytes,
      and pool queue depths (rayon exposes none; tokio needs
      `tokio_unstable`))
- [x] OpenTelemetry tracing setup (OTLP exporter, configurable
      endpoint) — §3.4 (`--otlp-endpoint` / `TUT_OTLP_ENDPOINT`; unset disables
      export. OTLP/HTTP exporter via the `hyper-client` (reusing the in-tree
      hyper, not reqwest); `serve`'s layered subscriber — `EnvFilter` + fmt/json
      + the otel layer — is built inside the runtime in `run_serve` and held by a
      `TelemetryGuard` that flushes/shuts the batch exporter down on graceful
      exit. One-shot subcommands keep the simple fmt/json `init_tracing`)
- [~] Trace per query with span attributes — §3.4 (a `query` span per
      `do_get_statement`/`do_get_prepared_statement` with `sql` (bounded preview),
      `prepared`, `rows`, and `error`. Deferred attributes that need other
      subsystems: `dataset`/`time_range` (plan introspection), `memory_claimed`,
      and `claimed user` (the unwired §1.6 identity field))
- [~] Sub-spans for parse / plan / scan-per-shard / FTGS /
      merge / serialize — §3.4 (`plan` and `execute` sub-spans at the handler
      boundary. The deep per-shard/FTGS/merge spans run inside `block_on_scan`'s
      spawned thread, across which the parent span doesn't propagate without
      explicit capture/re-entry — deferred)
- [~] Structured JSON logging to stdout, tagged with trace ID —
      §3.4 (`TUT_LOG_JSON=1` emits JSON with the current span's name/fields via
      `with_current_span`; a literal `trace_id` field on every log line — a small
      custom layer reading the otel span context — is deferred)

## Web UI

- [x] `/status` page on the ops port — §3.5 (JSON-first: `/status.json`
      serialises live state, `/status` serves an embedded static HTML page
      [`include_str!`] that fetches it and renders client-side with vanilla JS —
      zero new deps. Structural state [sessions, datasets] flows from the
      `FlightSQL` service via a `status::StatusSource` trait so ops never sees its
      internals; numeric state comes from the shared `Metrics`. `/favicon.svg`
      serves the embedded `t9n.svg` mark)
- [x] Daemon health + build version display — §3.5 (version, uptime, readiness)
- [x] Loaded shards table (dataset, time range, size on disk,
      size mmap'd) — §3.5 (a per-dataset *resident* footprint on `/status` —
      dataset name, shards loaded, and cached bytes (≈ mmap'd, since a loaded
      shard is mmapped whole) — via `Cache::resident()` over the daemon's
      per-dataset cache map. Reports what's actually loaded locally, not a
      per-hit walk of all discoverable shards. Deferred: per-shard time range
      and a full discoverable-shard inventory — both need the discovery walk we
      avoid on the hot `/status` path)
- [x] Active sessions table (count, oldest age, total memory) —
      §3.5 (count, oldest-session age, and total *baseline* reserved bytes — the
      admission floor; per-session live working-set is not metered)
- [x] Memory pool usage breakdown — §3.5 (limit / used / available, off the §2.2
      budget, plus the §2.8 bitmap-cache resident bytes)
- [x] Recent queries ring buffer (last ~50) — §3.5 (bounded ring of the last 32
      in `Metrics`, fed by `record_query` with the SQL preview + rows + latency;
      status-only, not in the Prometheus exposition)
- [x] Pointers to `/metrics` and OTLP endpoints — §3.5 (the page footer links
      `/metrics` and `/status.json`; OTLP export is a push to a configured
      collector, not a local URL to link)

## Auth — v1 disciplines (so v2 plugs in cleanly)

Not auth itself — those are v2. These are the disciplines that
v1 must respect so v2 is a localized change.

- [ ] `username` field on requests is `Option<String>` with
      "claimed identity, not verified" comment; never hardcoded
      — §1.6
- [ ] gRPC interceptor structure ready for an auth interceptor
      to be added in v2 — §1.6
- [ ] `Identity` type as a placeholder used by session lifecycle
      / admission control (v1's Identity = claimed username; v2
      makes it verified) — §1.6
