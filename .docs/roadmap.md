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
- [x] Content-hash validation on shard load — §1.5
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

- [ ] `MemoryBudget` global struct with `AtomicU64` charge counter
      — §2.2
- [ ] `MemoryReservation` RAII guard (drop returns bytes) — §2.2
- [ ] `SessionMemoryHandle` — per-session sub-budget with cap — §2.2
- [ ] Hard claim-or-fail allocation API (`reserve(n) -> Result<...,
      BudgetExceeded>`) — §2.2
- [ ] Admission control in `OpenSession` handler — §2.2
- [ ] Per-session cap (default 20 % of global, configurable) — §2.2
- [ ] mmap accounting via forward-column file sizes on shard open
      — §2.2

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
      (`GroupLookup::memory_used()` exposed; wiring waits on §2.2's
      `SessionMemoryHandle`)

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
- [ ] Arrow record-batch output (1024 / 4096 row default
      batching) — §2.6
- [ ] Same merge code reused at client layer (cross-daemon) — §2.6
      (`merge_ftgs` is already the shared primitive; cross-daemon
      wiring still to build)

## Engine — sessions

- [ ] Session struct (group lookup, stat stack, dynamic metrics,
      shard handles, memory handle) — §2.4
- [ ] `OpenSession` handler — admission check, shard set
      selection by time range, token issuance — §2.4
- [ ] `CloseSession` handler — explicit teardown — §2.4
- [ ] Idle timeout reaper (default 30 min, configurable) — §2.4
- [ ] Hard maximum age reaper (default 4 h, configurable) — §2.4
- [ ] `SessionLost` error on daemon-crashed-mid-session — §2.4
- [ ] Opaque token format (don't leak internals) — §2.4
- [ ] Stat stack — `PushStat`, `PopStat`, `GetNumStats` — §2.6
- [ ] Dynamic metric allocation + update — §2.6
- [ ] Regroup operations (filter, bucket, query-based, regex,
      random, intersect, etc. — full Imhotep parity) — §2.6

## Wire / protocol

- [ ] `tonic` gRPC server setup over HTTP/2 — §1.2
- [ ] `tutankhamun.v1.SessionControl` protobuf definitions
      (`OpenSession`, `CloseSession`, `Regroup`, `PushStat`,
      `PopStat`, `MetricRegroup`, `GetStatus`, etc.) — §1.2
- [ ] `SessionControl` service implementation — §1.2
- [ ] `arrow.flight.protocol.FlightService` registration — §1.2
- [ ] `DoGet(Ticket)` for FTGS result streaming as Arrow record
      batches — §1.2
- [x] DataFusion embedded as a dependency — §3.2
- [x] Tutankhamun `TableProvider` implementation (Tier 1: projection
      + equality/range filter pushdown; `t9n sql` CLI verb) — §3.2
- [x] Time-range shard pruning in the SQL scan (prune shards by a
      predicate on the time column before fetch) — §3.2
- [~] Aggregation / GROUP BY pushdown from DataFusion → Tutankhamun
      FTGS scan (Tier 2/3) — §3.2
      (single-column `Int`/`String` GROUP BY *and* global no-GROUP-BY
      aggregates + COUNT(*)/SUM/MIN/MAX/approx_count_distinct pushed via an
      optimizer rule → `FtgsAggExec`, including the `String` NULL group; the
      global path reuses `aggregate_docs` over the filtered set → one row with
      SQL empty-input semantics (count/approx → 0, sum/min/max → NULL);
      unsupported shapes fall back to DataFusion. Remaining: `approx_distinct`
      on a `String` arg (hash index terms, no forward column); multi-column
      GROUP BY via regroups; AVG)
- [ ] FlightSQL service implementation — §3.2
- [ ] Session-aware SQL execution — DataFusion planner reuses
      session state when new query's filter refines previous — §3.2
- [ ] Session-affinity metadata header
      (`x-tutankhamun-session-id`) published in gRPC responses —
      §2.4

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
      (pushed through the SQL `GROUP BY` path via the `StatValue`
      seam; optional `precision` arg still to add)
- [ ] `approx_percentile(field, p, [compression])` (t-digest) —
      §3.1
- [ ] `approx_top_k(field, k, [capacity])` (Count-Min + heavy
      hitters) — §3.1
- [ ] `theta(field, [nominal_entries])` returning Arrow `Binary`
      — §3.1
- [ ] `theta_intersect(a, b)` — §3.1
- [~] Sketch merge in the FTGS merge path (sketches are
      mergeable by construction) — §3.1 (HLL registers union via
      the `StatValue`/`combine_stats` seam; t-digest/theta extend it)
- [~] Sketches as Arrow record-batch columns (int64 for scalars,
      `Binary` for raw thetas) — §3.1 (HLL estimate emitted as a
      `UInt64` column; `Binary` arrives with theta)

## Observability

- [ ] Prometheus `/metrics` endpoint on the ops port — §3.4
- [ ] Standard metrics (request rates, latency histograms, pool
      queue depths, memory pool, session counts, shard cache
      hit/miss, mmap'd bytes, error counts) — §3.4
- [ ] OpenTelemetry tracing setup (OTLP exporter, configurable
      endpoint) — §3.4
- [ ] Trace per query with span attributes (claimed user,
      dataset, time range, memory claimed, rows scanned /
      returned, error type) — §3.4
- [ ] Sub-spans for parse / plan / scan-per-shard / FTGS /
      merge / serialize — §3.4
- [ ] Structured JSON logging to stdout, tagged with trace ID —
      §3.4

## Web UI

- [ ] `/status` page on the ops port — §3.5
- [ ] Daemon health + build version display — §3.5
- [ ] Loaded shards table (dataset, time range, size on disk,
      size mmap'd) — §3.5
- [ ] Active sessions table (count, oldest age, total memory) —
      §3.5
- [ ] Memory pool usage breakdown — §3.5
- [ ] Recent queries ring buffer (last ~50) — §3.5
- [ ] Pointers to `/metrics` and OTLP endpoints — §3.5

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
