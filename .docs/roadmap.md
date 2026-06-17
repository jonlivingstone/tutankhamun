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
- [x] Forward column reader — mmap zero-copy `&[i64]` — §2.1
      (`DiskShard::open` mmaps `metrics.arrow` and decodes its single batch via
      Arrow's `FileDecoder` over a `Buffer::from_custom_allocation` wrapping the
      mapping — `forward_column` returns a view straight into the file, no heap
      copy, no `bytemuck` needed (arrow's `ScalarBuffer<i64>` derefs to `&[i64]`).
      An in-crate test asserts the slice's address lies inside the mmap. Measured
      on 62 nyc_taxi shards: `open` dropped from ~700ms warm / 11.5s cold to
      ~21ms warm / ~95ms cold, and the ~3.7 GB heap moved to reclaimable page
      cache. The indexes were already mmap'd; now the forward columns are too.)
- [x] Inverted index writer — `roaring` bitmaps per term + `fst`
      term dictionary — §2.1
- [x] Inverted index reader — FST range scan + Roaring bitmap
      iteration — §2.1
- [ ] Optional Parquet export of forward columns — §2.1
- [ ] Nullable columns — §2.1
      (today `ingest` rejects any null in a declared column —
      `extract_numeric`/`extract_time` `bail!` on `is_null` — because the
      forward column is a dense `i64` buffer read zero-copy as `&[i64]`,
      with no place for "absent". Add null support: carry the Arrow
      validity bitmap alongside the values buffer (Arrow IPC already
      produces it; we currently discard it), mmap it, and consult it in
      the FTGS loop with SQL NULL semantics (sum/avg skip nulls,
      `count(col)` skips, `count(*)` counts). The values buffer stays a
      dense `&[i64]`, so the zero-copy read is preserved — the bitmap is
      a side input, not a layout change. Unblocks ingesting nullable
      source columns (e.g. nyc-taxi `passenger_count`) and per-shard
      schema evolution: a dataset's newer shards can carry a column that
      older shards project as null. Prereq for live add-column (§3.3).)

## Engine — abstractions

- [x] `Shard` trait — `forward_column()`, `inverted_index()`,
      `time_range()`, `num_docs()`, `schema()` — §2.1, §3.3 v1 disciplines
- [x] `DiskShard` implementation of `Shard` — §2.1, §3.3 v1
- [x] `ShardSource` trait — server-side abstraction over where
      shards come from — §3.3 v1 disciplines
- [~] Object-storage `ShardSource` implementation — §3.3 v1
      (`ObjectStoreShardSource` works, but discovery is currently run
      **per query** — a full `store.list()` + a GET of every shard's
      `metadata.json` (shard_source.rs:81), called fresh from
      `provider.rs`/`scan.rs` on every query. Not acceptable against
      object storage (LIST + N round-trips per query); must move behind a
      cached, version-checked catalog — see the per-dataset manifest item
      under Ingest. Marked incomplete until discovery is no longer
      per-query.)
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
- [x] Bounded working-set reservation (span-independent) — §2.2 (the
      per-session handle threads to the otherwise session-agnostic execs as
      a `DataFusion` `SessionConfig` extension. The scan
      (`sql::scan::for_each_shard_batch`) processes shards in
      bounded-concurrency batches (`runtime::cpu_width()` at a time),
      dropping each batch's forward-column residency before the next, and
      reserves one span-independent envelope up front — `batch width ×
      largest shard's num_docs × per-doc estimate` — held for the whole
      query, so an admitted query has room to finish and a full-history
      aggregate runs in bounded memory. The `t9n sql` CLI sets no extension
      and charges nothing.
      *Supersedes the original "charge each shard's `metrics.arrow` file
      size" model, which summed the whole span and held it at once —
      tripping the cap on wide-span queries and byte-charging evictable
      `mmap` pages as if heap.*)
- [ ] Project-aware / true-heap working-set accounting — §2.2 (the v1
      envelope is a conservative `num_docs`-derived proxy; refine to charge
      the realized heap — gathered output, group/stat buffers — and only the
      projected columns, so the estimate tracks actual allocation more
      tightly. Bounding the row-scan *output* `Vec<RecordBatch>` for an
      unbounded `SELECT *` (output backpressure) is the related follow-up.)

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
- [ ] `__time` canonical alias for the time field — §3.2
      (today the time field is exposed only under its ingested name, so
      SQL must hard-code e.g. `tpep_pickup_datetime`. Expose it
      *additionally* under a fixed `__time` name — the Druid convention —
      so `WHERE __time >= TIMESTAMP '...'` works on any dataset regardless
      of the original column name, for every FlightSQL client, not just
      the Python one. Add the alias in `arrow_schema_from_metadata`, and
      teach the filter pushdown + time-range pruning to treat `__time` as
      the time field — otherwise a predicate on the alias would lose shard
      pruning and fall back to a full scan. Makes a name-independent
      client `time_range=` (§3.2 Python client) trivial to layer on top.)
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
- [x] Time-bucket GROUP BY pushdown (`date_trunc`) — §3.2
      (today `try_build` (group_by.rs) only pushes down `GROUP BY` of bare
      `Expr::Column`s, so `GROUP BY date_trunc('month', <time>)` — a
      `ScalarFunction` — falls back to a row scan: materialise every matching
      row's time column + scale to ns, then DataFusion runs `date_trunc`
      per row and hash-aggregates. For a year over weekly shards that's ~37M
      rows for a 12-row answer. Recognise `date_trunc(unit, <time field>)` /
      `date_bin(...)` on the dataset's time column as a **derived integer group
      key** = the bucket-start epoch, computed per doc straight off the `i64`
      time column in the FTGS scan (fixed units — second…week, and `date_bin`
      — are integer truncation; month/year need chrono calendar math). Then
      time-window aggregation runs natively, with low-cardinality output
      (12 months / 52 weeks). This is the primary time-series query shape
      (cf. Druid `__time` + granularity, ClickHouse `toStartOfMonth`).
      Composes with the `__time` alias (above) and feeds the per-shard
      partial-aggregate cube (Engine — FTGS) — a repeated time-window
      aggregate then becomes near-free; without this pushdown the cube never
      applies, since the query never reaches FTGS.
      **Implemented:** `GroupKey`/`BucketUnit` + recognition of
      `date_trunc('<unit>', <time field>)` for second…year (integer truncation
      for second…day, chrono calendar math for week/month/quarter/year),
      including mixed with categoricals; a shard-granular single-bucket fast
      path (whole shard in one bucket → one group, no per-doc truncation); and
      it feeds the per-shard aggregate cache. `date_bin` NOT recognised yet.
      Synonyms (`__month` etc.) tracked separately below.)
- [ ] Canonical time-bucket synonyms (`__year`/`__quarter`/`__month`/`__week`/
      `__day`/`__hour`/`__minute`/`__second`) — §3.2
      (today the time-bucket fast path fires only for the exact shape
      `date_trunc('<unit>', <time field>)`; any near-miss a user writes
      — `to_char(t,'YYYY-MM')`, `extract(year ...), extract(month ...)`,
      `cast(date_trunc(...) as date)`, a timezone shift, an aliased wrap —
      silently falls back to a full row scan with no cache and no signal. A
      zero-arg synonym over the dataset's designated time field gives one
      blessed, foot-gun-proof handle that *is* the fast path by construction.
      Two surfaces:
      - **GROUP BY**: `GROUP BY __month` → the `date_trunc('month', <time
        field>)` bucket key (the existing pushdown).
      - **WHERE**: `__month = '2024-01'` → the half-open range
        `__time >= '2024-01-01' AND __time < '2024-02-01'` (and `>=`/`<=`/`IN`
        forms), i.e. the pushdown-able predicate shape — so a drill-down can't
        accidentally be written in a way that loses pushdown. Needs the
        half-open exact-range filter (Tech debt / Layer 1a) to push down.
      Implement as an AST rewrite keyed on the dataset's `metadata.time_field`,
      done after parse / before planning (NOT virtual columns — they'd leak
      into `SELECT *` and not help WHERE; NOT a pre-parse string regex — the
      time field isn't known until the FROM is parsed). Shares the time-field
      resolution with the `__time` alias above; `date_trunc` recognition stays
      as best-effort compatibility, `__month` becomes the recommended form.
      Out of scope for v1: cyclic/EXTRACT-style buckets (`__dow` day-of-week,
      `__moy` month-of-year, `__hod` hour-of-day) — a different family that
      partitions rather than truncates, so it doesn't roll up or range-select
      like the nested truncation grains; revisit separately if needed.)
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
- [ ] Per-dataset manifest (catalog snapshot) — §3.3
      (a small versioned object per dataset: the authoritative shard set
      + schema + version, written atomically. Today `ShardSource::discover()`
      runs a full `store.list()` **plus a GET of every shard's
      `metadata.json` on every query** (shard_source.rs:81) — a LIST + N
      round-trips per query, costly against S3/GCS. The manifest replaces
      that with one manifest read + a cheap version/etag check, and a query
      resolves a single consistent snapshot. Supersedes per-query
      `discover()`; the manifest version is also the cache-invalidation
      signal, so no manual `reload` op is needed.)
- [ ] Atomic re-ingest / replace in place — §3.3
      (rebuild a dataset's shards and swap the manifest atomically, so a
      query never sees a mixed/partial state mid-rewrite. Without the
      manifest this is unsafe live — readers observe old- and new-schema
      shards at once. Append of a brand-new shard is already safe live
      today via the `metadata.json` commit-marker; this covers the
      *replace* case. Needs the nullable-columns item (storage format,
      above) only when the new schema differs per shard; a uniform
      whole-dataset rewrite does not.)
- [ ] Live add / remove shards to a dataset — §3.3
      (control-plane op to add or drop individual shards from a dataset's
      manifest without a full re-ingest — append a fresh day/month, retire
      an old one — committed atomically via the manifest version. Extends
      the deferred "shard manager — registration / eviction" item above.)

## Query language — native Python client

- [x] `tutankhamun` PyPI package skeleton — §3.2 (`clients/python/`,
      src-layout, hatchling; deps: pyarrow, optional pandas/polars)
- [x] Connection / session classes (`tk.connect(...).session(...)`)
      — §3.2 (`connect()` → `Connection`; `session(dataset=…)` /
      `session_from_sql(…)` open over raw `pyarrow.flight`)
- [x] Fluent API (`.filter()`, `.group_by()`, `.select()`,
      `.fetch()`) — §3.2 (immutable `Query` composer → `pyarrow.Table`)
- [x] Lazy execution (composes SQL fragments until `.fetch()`) —
      §3.2
- [x] Session token management — §3.2 (handshake mints a bearer the
      client echoes on every RPC; transparent reopen+replay on
      `SessionLost`; `close()`/`with` frees it server-side)
- [x] Dynamic metric definition (`session.define(name, expr)`) —
      §3.2 (folded into the base relation as a computed column, not a
      textual macro — user aliases/identifiers untouched)
- [x] Result conversion: `to_arrow()`, `to_pandas()`,
      `to_polars()` — §3.2 (results are `pyarrow.Table`; `.to_pandas()`
      native, `tk.to_polars()` zero-copy. `to_duckdb` deferred)
- [x] Context manager support (`with session: ...`) — §3.2
- [x] Decision: build on `ibis` or roll our own — §3.2 (rolled our own
      thin SQL-fragment composer; the API passes SQL strings, so ibis's
      typed-expression model would conflict. Optional ibis backend later)
- [ ] `time_range=` on `session()` (absolute + relative windows) — §3.2
      (resolves to a `WHERE __time >= … AND __time < …` predicate, so it
      needs the `__time` alias above — or, until then, the discovered
      timestamp column. Accept both **absolute** bounds (ISO strings /
      `date` / `datetime`) and **relative** bounds resolved against `now()`
      at call time — e.g. `time_range=("3w", "1w")` = "3 weeks ago to 1
      week ago", giving IQL's `FROM <table> 3w 1w` ergonomics without
      extending SQL. Relative grammar follows the established
      observability convention (Splunk `earliest/latest`, Grafana
      `now-3w`, ES date-math, Flux `range(start:-3w)`); a fluent
      `.last("7d")` shorthand is optional sugar on top. The engine itself
      already supports the semantics via standard `now() - INTERVAL`
      arithmetic — verified — so this is purely client ergonomics.)
- [ ] Python package CI (lint + unit tests; none exists yet) — §3.4

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

## Tech debt

Known gaps and consolidations deferred from shipped work. Not features —
cleanups/fixes we don't want to lose track of.

1. **Repeated queries re-open shards (parse footer + build array views +
   mmap indexes) every time.** The `Cache` caches shard *files* on disk, not
   the parsed `DiskShard`, so `DiskShard::open` runs fresh per query. Since
   forward columns are now mmap'd zero-copy (§2.1), open is cheap (~21ms warm
   for 62 nyc_taxi shards), so this is low priority — but two refinements
   remain if shard counts grow large: (a) a **resident-shard LRU** keyed by
   `shard_id` (now ~free in memory since residency is page-cache-backed), and
   (b) **probe the aggregate cache before open** — `shard_id` is derivable from
   the discovery `ShardSummary` without opening, so a full cache hit could skip
   open entirely (~21ms → ~8ms discover-only). Both largely obviated by the
   mmap win; revisit only at scale.
2. **`BitmapCache` and `AggregateCache` duplicate LRU machinery.** Two
   daemon-shared caches with near-identical `Mutex<state>` + clock-LRU +
   `reserve_with_eviction`/`evict_oldest` + byte accounting + counters,
   differing only in key/value type and the bitmap cache's monotone-narrowing.
   A shared `LruCache<K, V>` (budget + eviction + counters) with the two as
   thin shims would remove the duplication. A bug in eviction must be fixed in
   both today.
3. **Filter normalization duplicated.** `aggregate_cache.rs` sorts+dedups its
   filter clauses inline; `bitmap_cache.rs::normalize` does the same. Move the
   canonicalization next to `PushedFilter` (`sql/pushdown.rs`) and have both
   caches call it, so the set-semantics policy lives in one place. (Folds into
   §2 if that consolidation happens.)
4. **Cache budget is per-cache, not a shared pool.** The bitmap and aggregate
   caches each get their own `cache_pct`%-of-`mem_limit` sub-budget, so total
   cache memory can reach `2 × cache_pct`% (default 25% → up to 50%), bounded by
   the global budget. Separate slices are intentional (a shared cap would let
   one cache starve the other's eviction — see `build_budget_and_caches`), but
   if cache pressure on sessions becomes a problem, revisit: a single shared
   `cache_pct` pool with combined eviction (needs §2's shared `LruCache`) or a
   distinct `--aggregate-cache-pct` flag.
