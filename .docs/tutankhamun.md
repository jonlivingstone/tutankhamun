# Tutankhamun

Tutankhamun is a Rust analytics engine for **interactive, session-based
exploration of time-stamped event data** — the filter-it, group-it,
ask-the-next-question workflow — served with a predictable low-latency
tail and results that land directly in the Arrow toolchain (Polars,
DuckDB, Pandas, Jupyter).

This document is the design log: each decision is recorded with its
rationale and the alternatives considered. Tutankhamun is the successor
to Indeed's Imhotep engine — for that lineage and what carries over, see
[Appendix A: Imhotep heritage](#appendix-a-imhotep-heritage).

## 1. Positioning — why Tutankhamun

Tutankhamun is built for interactive exploration — a workflow where each
query builds on the last — rather than for one-shot queries answered in
isolation. That focus explains most of the design decisions below.

### 1.1 What it's for

Interactive, session-based exploration of time-stamped event data:
dashboards, cohort analysis, and ad-hoc investigation of a metric that
moved. The target workload is a sequence of related queries — filter,
inspect, filter again, group differently, add a metric — issued in one
line of investigation. Tutankhamun is built to make that sequence fast.

### 1.2 What makes it distinctive

Each feature addresses a specific, common limitation in existing engines.

- **Stateful exploration sessions.** Interactive exploration issues many
  related queries in sequence — narrow a filter, change a grouping, add
  a metric. In a query-per-request engine, each step re-scans from
  scratch. A Tutankhamun session holds the working state — the current
  grouping, the active filters, the running metrics — in memory on the
  server, so each step is an incremental update rather than a full
  re-scan.

- **Deterministic sampling for cohorts.** Cohort analysis tracks the
  same set of users across queries and across days. Explicit cohort
  membership normally has to be stored, and random sampling returns a
  different sample on each run. Tutankhamun decides sample membership by
  hashing the user ID, so the same users are included consistently
  across queries and days, with no stored cohort table.

- **Predictable tail latency.** The engine is Rust, with no garbage
  collector and no JIT warmup, avoiding the pause and warmup variance of
  JVM engines. p99 latency stays stable under load.

- **Arrow-native results.** Results stream as Arrow over Arrow Flight.
  The bytes on the wire are already the in-memory columnar layout, so
  Arrow clients — Polars, DuckDB, Pandas, Jupyter — read them with no
  deserialization step, and standard FlightSQL clients and BI tools
  connect without a custom driver.

- **Single-binary deployment.** The daemon is one stateless binary
  (`t9n`). All durable state lives in object storage; local disk is a
  rebuildable cache. There is no ZooKeeper and no metadata database. A
  node can be replaced with no data loss, and the same binary runs on
  K8s, Nomad, ECS, VMs, and bare metal.

- **Approximate aggregates.** Exact distinct-counts and percentiles over
  high-cardinality fields are memory-intensive, and cohort-overlap
  questions are hard to express. Tutankhamun includes mergeable
  approximate aggregates: `approx_distinct` (HyperLogLog),
  `approx_percentile_cont` (t-digest), `approx_top_k`, and `theta`
  sketches, with `theta_intersect` for cohort overlap.

## 2. Querying and exploration

The query surface and the interactive workflow the engine exposes: the
query language, how results reach client tools, and how an exploration
session reuses work across successive queries.

### 2.1 Results in the Arrow ecosystem

Results are Arrow record batches delivered over Arrow Flight. The bytes on
the wire are already the in-memory columnar layout, so a client reads them
into a DataFrame with no deserialization step:

- Arrow-native tools — Polars, Pandas, DuckDB, Jupyter — consume results
  zero-copy.
- The surface is FlightSQL, the standard Arrow way to ship SQL and
  results, so BI tools and JDBC/ODBC consumers connect through stock Arrow
  ADBC drivers (Tableau, Superset, Hex, `pd.read_sql`, …) with no custom
  driver.

The same Arrow schema carries from disk to wire to client frame; no
re-serialization happens along the way.

### 2.2 Interactive exploration

Exploration is a sequence of related queries against a pinned dataset and
time range — filter, inspect, filter again, group differently, add a
metric. A **session** holds that working state on the server, so each step
reuses the previous one instead of rescanning. A native Python client
library exposes the workflow as a fluent, lazy API:

```python
import tutankhamun as tk

session = tk.connect("grpc://tutankhamun:50051").session(
    dataset="logs",
    time_range=("2024-01-01", "2024-01-02"),
)

us = session.filter("country = 'US'")              # cheap refinement
by_device = us.group_by("device").select(
    "count(*), sum(revenue)"
).fetch()                                          # fast; reuses state

mobile_us = us.filter("device = 'mobile'")         # narrows further
by_city = mobile_us.group_by("city").select("count(*)").fetch()

session.define("ltv", "clicks * cpc + purchase_amount")   # derived metric
by_country_ltv = us.group_by("country").select("sum(ltv)").fetch()

session.close()                                    # or use as a context manager
```

The library returns a `pyarrow.Table` by default, with zero-copy adapters
for Polars and Pandas.

Three properties keep the sequence cheap:

- **Refinements reuse cached work.** A narrowing filter (`old AND extra`)
  intersects a cached result instead of rescanning, so drilling deeper
  does not cost more than the first step (§C.2).
- **Derived metrics are defined in place.** `session.define("ltv", …)`
  adds a computed column usable in later queries, without touching stored
  data.
- **Common work is shared.** A filter many analysts start from (say
  `country = 'US'`) is computed once and reused across sessions, and a
  cohort definition can be saved and referenced by name (§C.2).

Stock SQL clients (§2.1) issue independent queries without a session. They
do not get the cheap-refinement property, but get everything else —
predictable latency, joins, approximate aggregations, Arrow results.

Sessions are ephemeral: they time out when idle and are lost if their
daemon stops, at which point the client reopens (§6.3).

### 2.3 SQL queries and joins

The query language is SQL, parsed and planned by an embedded Apache
DataFusion, so the analytical surface is standard — the usual predicates,
projections, aggregations, CTEs, and window functions — with no
engine-specific dialect. Filters, projections, group-bys, and the
supported aggregations push down into Tutankhamun's scan (§4.1); anything
else executes in DataFusion.

The aggregate set includes the approximate functions — `approx_distinct`,
`approx_percentile_cont`, `approx_top_k`, and `theta` (§4.3) — alongside
the exact aggregates.

Joins span datasets and external files. A query can join a Tutankhamun
dataset with another dataset, or with a Parquet file read directly from
object storage:

```sql
SELECT t.country, sum(t.revenue)
FROM logs t
JOIN read_parquet('s3://attrs/2024-01-01.parquet') a USING (user_id)
WHERE a.tier = 'premium'
GROUP BY t.country
```

DataFusion plans the join, pushes the filter and projection into the
Tutankhamun scan, and executes the join in its own vectorized engine.

## 3. Storage

How data is laid out at rest and served into the query path: the on-disk shard format, the object-storage backends that hold shards durably, and the local cache that keeps them hot.

### 3.1 Storage format — inverted index + Arrow IPC forward columns

A shard is a directory holding three kinds of file: a metadata sidecar,
the forward (per-doc metric) columns, and the inverted index over the
indexed fields.

**Shard layout:**

```
shard-2024-01-01-12/
  metadata.json                ← Arrow schema + shard stats (numDocs,
                                  fields, time range, format version)
  metrics.arrow                ← uncompressed Arrow IPC, one record
                                  batch, one column per int field /
                                  metric. mmap-friendly, zero-copy
                                  reads as &[i64].
  postings/
    country.fst                ← FST term dictionary (sorted distinct
                                  terms, compact, fast prefix lookup
                                  and range scan).
    country.posting            ← Roaring bitmap per term (the inverted
                                  index: term → sorted doc-ID set).
    device.fst
    device.posting
    ...                        ← one .fst + .posting per indexed field.
```

**`metadata.json`** — the per-shard sidecar. It records the Arrow schema,
`numDocs`, the indexed and forward fields, the time range, the format
version, and per-file content hashes. A reader opens it first; it is the
source of truth for what the shard contains.

**`metrics.arrow`** — the forward columns: one `int64` column per metric
or int field, stored as **uncompressed single-batch Arrow IPC**. Forward
access in the FTGS loop is `metric[doc_id]` — a random index into a
contiguous `int64` array — so the file is memory-mapped and its values
buffer cast directly to `&[i64]`, with the Arrow footer parsed once at
open and never in the hot loop. The single-batch, uncompressed invariant
is load-bearing: small batches or compression break the zero-copy
`&[i64]` access. (Cold-tier shards may opt into Arrow IPC's LZ4/ZSTD
compression, trading that access for storage savings.) Because the
columns are plain Arrow, any Arrow-native tool — DuckDB, Polars, Pandas —
can open `metrics.arrow` directly.

**`postings/<field>.fst` + `postings/<field>.posting`** — the inverted
index, one pair per indexed field. The `.fst` is an FST term dictionary:
the sorted distinct terms, compact, supporting fast prefix lookup and
range scan, mapping each term to an offset into the `.posting` file. The
`.posting` file holds one Roaring bitmap per term — the term's sorted
doc-ID set. Term iteration walks the FST; per-term doc-ID retrieval
decodes the corresponding Roaring bitmap.

**Dataset schema (`schema.json`).** One file at the dataset root — beside
the shards, not inside them — records the dataset's column schema (field
name, kind, scale, nullability): the single schema every shard is read
under. It records column *types*, never which shards exist, so it does not
interfere with shard discovery (a dataset is still just its directory of
shards). It is authoritative when present and inferred from the shards
when absent, so a missing file degrades to inference rather than a dead
dataset. Nullability is a dataset-level property — a column is non-null
until a NULL is ingested, then recorded nullable permanently — while each
shard's `metadata.json` still records that shard's actual null-bearing
fact; existing shards are never rewritten when the dataset schema evolves.

### 3.2 Object storage — all backends transparent

Shards live in object storage, reached through a single storage
abstraction so every backend shares one code path; the engine never talks
to a specific cloud SDK directly. The operator selects a backend per
dataset via configuration.

**Backends supported:**

| Backend | Use case |
|---|---|
| AWS S3 | Native AWS deployments; credential chain handles IRSA / instance profile / env vars / SSO |
| S3-compatible (MinIO, Ceph, R2, B2, Wasabi, Garage, SeaweedFS) | On-prem object stores or alternate clouds — same code path, different endpoint URL |
| Google Cloud Storage | Native GCP deployments; Workload Identity for auth |
| Azure Blob Storage | Native Azure deployments; Managed Identity for auth |
| Local filesystem | Single-machine / air-gapped / dev-loop deployments — no MinIO required |
| HTTP (read-only) | Niche; pulling shards from a CDN-style source |
| In-memory | Tests only |

Credentials come from each cloud's standard provider chain (env vars →
instance metadata → IRSA / Workload Identity → SSO → static config); no
credentials are ever hardcoded.

### 3.3 Local hot storage — persistent cache with safe defaults

A persistent local cache holds shards downloaded from object storage. It
uses LRU eviction, survives daemon restarts, and runs with zero
configuration out of the box, with explicit overrides for production
sizing.

**Cache directory:**

- `--cache-dir <path>` sets the location. The default follows XDG
  conventions (no root required, runs in user mode out of the box):
  - Linux: `$XDG_CACHE_HOME/tutankhamun/cache` (typically
    `~/.cache/tutankhamun/cache`)
  - macOS: `~/Library/Caches/tutankhamun/cache`
  - Windows: `%LOCALAPPDATA%\tutankhamun\cache`
- System deployments (systemd unit, K8s container) pass
  `--cache-dir=/var/lib/tutankhamun/cache` explicitly; the daemon does not
  auto-switch paths based on root detection.

**Cache size:**

- `--cache-size <size>` accepts an absolute value (`100GB`, `500MB`) or a
  percentage (`50%` of the cache filesystem's total capacity).
- Default: `10 GB`. Production sizing is an explicit operator decision
  (e.g. `--cache-size 800GB` or `--cache-size 80%` on a 1 TB NVMe).

**Hard floor (always enforced, regardless of `--cache-size`):**

- Cache filesystem free space must stay **≥ 5 %** of total capacity.
- If LRU eviction cannot bring free space back above the floor, the daemon
  refuses new shard downloads and surfaces a clear error rather than
  letting the disk fill.
- Configurable via `--cache-min-free-pct` (default 5).

**Effective cap = `min(--cache-size, available-down-to-5%-floor)`** — both
checks apply; whichever triggers eviction first wins.

**Other behaviour:**

- LRU eviction, with `--pin-datasets <list>` to mark always-keep shards
  (skipped by LRU).
- Persistent across restarts: on startup the daemon scans the cache dir,
  registers existing files, and validates each shard's content hash before
  use.
- Lazy download: shards fetch on first query, with configurable hot-set
  pre-warm via `--prewarm "<dataset>:<time-range>"`.
- Content-hash validation on every shard load (one extra read, ~milliseconds).

**Storage hierarchy:**

```
Object storage  (canonical, durable, slow first-byte)
   │ download on first access
   ▼
Local NVMe cache  (this layer — Tutankhamun-managed, LRU, persistent)
   │ mmap
   ▼
OS page cache  (DRAM-speed, OS-managed, automatic)
   │
   ▼
FTGS hot loop
```

## 4. Engine

The query-execution core — what runs when a query lands: the single aggregation primitive, the group representation that keeps it memory-bounded, and the approximate aggregates.

### 4.1 FTGS implementation

The FTGS algorithm and its invariants are preserved from Imhotep; the
implementation is modernized.

**The algorithm:**

- The four-level cursor (`next_field` → `next_term` → `next_group` →
  `group_stats`).
- Strict ordering invariants (terms sorted within a field; groups
  ascending within a term; fields enumerated in declaration order).
- The split-then-merge decomposition for parallelism — per-shard splits,
  then N-way sorted merge using a compact two-level bitmap (`GSVector`
  equivalent).
- The "critical loop": walk doc-ID batches from a term's postings,
  dispatch to `GroupLookup::next_group_callback`, and accumulate into
  `term_grp_stats[stat][group]`.

**The implementation:**

- **Streaming over Arrow.** FTGS results are produced as Arrow record
  batches (typically 1024 or 4096 rows) that feed directly into the Arrow
  Flight stream (§2.1) with no re-serialization.
- **Postings via Roaring + FST.** Term iteration uses the FST's range
  scan; per-term doc-ID retrieval is a Roaring bitmap iteration. Both are
  SIMD-friendly and significantly faster than Imhotep's varint-delta
  postings for typical cardinalities.
- **Forward column access via mmap'd Arrow IPC.** Per-doc metric lookups
  read directly from `bytemuck::cast_slice::<u8, i64>(arrow_buffer)` — one
  memory read, no decompression, no deserialization.
- **Native SIMD without JNI.** Vector intrinsics live in the same crate;
  no FFI overhead.
- **No bespoke FTGS wire format.** Imhotep's hand-rolled prefix-compressed
  binary FTGS stream is retired; Arrow IPC is the standard.

The merge code runs at two layers, unchanged in structure from Imhotep:
within a daemon (across that daemon's shards) and within the client
library (across daemons). "Distribution is recursion" — the same merge
function is reused at both levels.

### 4.2 Group-lookup specialization

A `GroupLookup` trait with backing implementations specialized by current
group cardinality keeps many concurrent sessions in bounded memory:

| Backing | Cardinality | Bytes / doc |
|---|---|---|
| `ConstantGroupLookup` | 1 | 0 (single value) |
| `BitSetGroupLookup` | 2 | 1 bit |
| `ByteGroupLookup` | ≤ 256 | 1 byte |
| `U16GroupLookup` | ≤ 65 536 | 2 bytes |
| `U32GroupLookup` | up to 2³² | 4 bytes |

The engine upgrades the representation in place when a regroup pushes
cardinality past the current type's limit. Dispatch is hidden behind a
`next_group_callback(doc_ids, &mut BitTree)` strategy call, used by the
FTGS inner loop (§4.1).

In the Rust port:

- **Memory accounting.** Each backing reports its byte cost to the
  session's `MemoryReservation` (§5.1); RAII drop semantics make it exact.
- **Type safety.** A `GroupLookup` enum lets the compiler verify backing
  swaps preserve invariants.
- **No native code path.** The inner loop uses `std::simd` / `std::arch`
  intrinsics directly, without an FFI boundary.

### 4.3 Approximate aggregations

HLL, t-digest, and theta sketches are available as stat-stack primitives,
using the `approx_` prefix convention shared by other SQL engines
(BigQuery, Snowflake, Spark, Druid, Trino, DuckDB). Computation is
query-time.

**The API:**

| Function | Returns | Required args | Optional args | Backed by |
|---|---|---|---|---|
| `approx_distinct(field)` | int64 | field | — | HyperLogLog |
| `approx_percentile_cont(field, p)` | int64 | field, p (0.0–1.0) | centroids | t-digest |
| `approx_top_k(field, k)` | array | field, k | capacity | Count-Min + heavy hitters |
| `theta(field)` | binary (sketch) | field | nominal_entries | Theta sketch |
| `theta_intersect(a, b)` | int64 (estimated size) | two thetas | — | Theta sketch |

The first two reuse DataFusion's standard spellings (`approx_distinct`,
`approx_percentile_cont`) rather than bespoke names, so analyst muscle
memory and BI-tool SQL generators work unchanged; `approx_top_k`, `theta`,
and `theta_intersect` are Tutankhamun-registered UDAFs/UDF.
`approx_distinct` accepts `Metric`/`Int` forward columns and `String`
inverted-index terms; `approx_top_k` and `theta` accept any of the three.

Exact aggregates are also available for the small-data case where
exactness matters and memory blow-up is not a concern: `COUNT(DISTINCT
field)` (exact distinct) and `median(field)` run via DataFusion execution
rather than the FTGS push-down path. The `approx_*` / `theta` forms are
the recommended ones for any non-trivial cardinality.

## 5. Resource model

How execution stays bounded and parallel so many concurrent sessions share one daemon predictably: the memory budget and the two-pool concurrency model.

### 5.1 Memory model and accounting

**Decision:** Pragmatic and current — hard enforcement, rough mmap accounting,
per-session caps, admission control at the front door, no spill-to-disk.

**The three sub-decisions:**

**Measurement.** Budget a query's **bounded concurrent working set**, not
the volume of data it scans. The scan processes a query's shards in
bounded-concurrency batches (`runtime::cpu_width()` shards at a time;
`sql::scan::for_each_shard_batch`), dropping each batch — releasing its
forward-column residency — before opening the next. A single
span-*independent* envelope is reserved up front (`batch width × the
largest selected shard's `num_docs` × a per-doc working-set estimate`)
and held for the whole query, so an admitted query is guaranteed room to
finish and querying all of history uses no more memory than one batch.
This is intentionally cheap to compute — sizes come from shard metadata,
no `/proc/self/smaps` polling, no per-page tracking.

That summed the **whole span** of forward-column files and held them all
at once — a full-history aggregate over a year of weekly shards reserved
~3.6 GB and tripped the cap, even though only a bounded set is ever
resident. It also conflated two concerns: `mmap`'d forward-column pages
are demand-faulted and kernel-evictable, so they aren't the
non-reclaimable heap that OOMs the daemon — their residency is bounded by
the batch concurrency (a count), while the byte budget guards the heap
working set (group lookups, accumulators, gathered output). This still
closes Imhotep's "8 GB configured, 60 GB resident" gap
([Imhotep `ARCHITECTURE.md` section 6.3](../../imhotep/docs/modernization/ARCHITECTURE.md)),
now without rejecting legitimate wide-span queries.

**Limit-exceeded behaviour.** Hard enforcement, not advisory.

- **Allocations** call into a central `MemoryReservation` API. If the
  reservation would exceed the budget, the allocation fails with a
  `BudgetExceeded` error rather than proceeding.
- **In-query OOM** surfaces as a structured per-operation error
  (`session.regroup(...) → Err(BudgetExceeded)`). The session is
  **not** killed — the client can adjust (drop fields, narrow time
  range, pop stats) and retry within the same session.
- **At session-open time**, admission control checks "does the
  daemon have enough free budget for this session's initial shard
  set?" If not, the `OpenSession` RPC is rejected with a clear
  "daemon at capacity, retry later" error before any expensive work
  begins.

**Fairness.** Per-session cap as a fraction of the global budget
(default 20 %, configurable). A single session cannot monopolise the
daemon. No per-tenant quotas today; multi-tenancy is deferred until
the workload demands it.

**Failure-mode coverage:**

| Failure mode | How it is prevented |
|---|---|
| OS OOM-killer kills the daemon | mmap counted, hard global cap; daemon stays under its configured limit |
| One greedy query starves others | per-session cap (default 20 % of budget) |
| Thrashing under load | admission control rejects new sessions before total RAM is exhausted |

**What's deferred** (none are one-way; all extend cleanly from the
current design):

- **Per-tenant / per-user quotas.** Add when multi-tenant deployments
  emerge.
- **Adaptive degradation.** Reject lower-priority work first when
  under pressure. Requires a priority signal on requests.
- **Spill-to-disk** for very large group / merge buffers. Significant
  subsystem — write a spillable hash table and integrate with the
  local NVMe cache. Defer until a workload requires it.
- **Accurate page-cache tracking** via `/proc/self/smaps` polling.
  Defer until the rough mmap estimate proves insufficient in
  production.

**Implementation notes:**

- A central `MemoryBudget` struct holds the global cap and current
  charge. All allocations route through it via a
  `MemoryReservation` RAII guard — drop semantics return the bytes,
  so accounting cannot leak.
- Sessions hold a `SessionMemoryHandle` (a sub-budget capped at the
  per-session limit) that bounds their reservations.
- Admission control is a check against the global counter inside the
  `OpenSession` handler, before any shard is mmap'd.
- The crate baseline already covers this (`arc-swap`, `parking_lot`,
  `tokio` for the async surface). No new dependencies required.

### 5.2 Concurrency and threading

**Decision:** **Tokio for I/O, Rayon for CPU-bound compute.** Two
bounded pools, each tuned to its workload. This directly addresses
Imhotep's "five unbounded cached thread pools" anti-pattern
(see Imhotep [`ARCHITECTURE.md` section 7.1](../../imhotep/docs/modernization/ARCHITECTURE.md))
without the engineering cost of a thread-per-core architecture.

**Pool design:**

- **Tokio multi-threaded runtime** — handles all async / I/O work:
  gRPC connection acceptance, Flight streaming, control-plane RPCs,
  session orchestration, shard fan-out (one task per shard within a
  session), object-store fetches. Sized to `~2× CPU cores`. Connection
  and per-session task counts can scale to many thousands at a few
  MB of memory total — async tasks are kilobytes, not megabytes.
- **Rayon work-stealing pool** — handles all CPU-bound inner loops:
  FTGS execution, regroup application, merge, per-doc metric
  evaluation. Sized to exactly the number of physical cores.
- **Dispatch pattern**: an async Tokio task receives a request,
  validates it, then dispatches the CPU work to Rayon via a
  oneshot channel and `.await`s the result. The Tokio thread is
  freed for other I/O while the Rayon worker runs the actual
  computation.

**Why not the alternatives:**

- *Pure Tokio with `spawn_blocking` for CPU* — `spawn_blocking` is
  designed for *occasional* blocking work inside async code, not for
  a primary compute path. Each blocking task gets its own OS thread,
  and the pool can grow large (default cap 512), reintroducing the
  "many threads for few cores" problem Imhotep had.
- *Thread-per-core* (Glommio / monoio) — best raw p99 latency, used
  by ScyllaDB / Redpanda. Genuinely better latency story, but
  significantly harder programming model (shared-nothing, hash-based
  sharding, no cross-core shared state), needs Linux + io_uring (no
  macOS dev story), and the latency win is microseconds — invisible
  next to the milliseconds spent in the FTGS loop itself. Credible
  later upgrade if profiling justifies it; not necessary now.
- *Pure Tokio work-stealing, no separate compute pool* — explicit
  anti-pattern documented by Tokio itself. CPU-bound loops on the
  same runtime as I/O block I/O tasks on the same thread, causing p99
  latency spikes under any compute load.

**Failure-mode coverage:**

| Failure mode | How it is handled |
|---|---|
| Thread explosion under load (Imhotep-style) | Both pools are bounded; admission control rejects new sessions before queues saturate |
| CPU work blocking I/O | Separated pools; async tasks dispatch CPU work and yield |
| Cache thrash from work stealing on hot data | Rayon's work-stealing localises per-shard work; per-shard data fits in L3 cache for typical shard sizes |
| Tail latency under burst load | Bounded pools + admission control + per-session memory cap (§5.1) combine to keep p99 predictable |

**Implementation notes:**

- The Tokio runtime is configured in `main()` with
  `Runtime::Builder::new_multi_thread()` and an explicit
  `worker_threads` setting; the Rayon pool is configured via
  `rayon::ThreadPoolBuilder` and held as a `OnceLock` for the engine's
  lifetime.
- Async-to-CPU dispatch uses a thin helper that spawns onto Rayon and
  returns a Tokio-compatible future, so the call site reads as a
  normal `.await`.
- This is the same architecture used in production by Polars,
  DataFusion, InfluxDB 3, and Materialize — well-trodden ground.

**Future option:** if profiling ever shows scheduler overhead is the
bottleneck (unlikely for analytical workloads), porting the CPU path
to a thread-per-core architecture is a *bounded* refactor — the FTGS
inner loop is shard-local and does not need cross-core shared state,
so the per-shard work units are already in the right shape.

## 6. Platform & deployment

The runtime and the deployment discipline that keep the binary portable across orchestrators, plus how a session reaches the daemon that owns it.

### 6.1 Runtime — Rust

**Decision:** Rust (latest stable, `edition = "2024"` or current).

**Alternatives considered:** Java 21 + virtual threads, Go, hybrid
Rust-engine-plus-JVM-control-plane, C++.

**Why Rust:**

- **Memory safety without GC.** Tutankhamun is a long-running
  multi-tenant daemon holding shared mutable state across many threads.
  The borrow checker and `Send`/`Sync` traits encode at compile time
  the invariants that Imhotep currently mitigates with logging and
  architectural discipline (e.g. `ImhotepLocalSession`'s "not even
  close to remotely thread safe" comment, the manual ref-counting in
  `MetricCacheImpl`). Use-after-free bugs in a multi-tenant analytics
  daemon are not just crashes — they are potential cross-session data
  leaks.
- **Direct mapping of existing patterns.** `SharedReference<T>` and
  `ReloadableSharedReference<T>` become `Arc<T>` and
  `arc_swap::ArcSwap<T>`. Reference-counted metric sharing is the
  language's native idiom.
- **Predictable latency.** No JIT warmup, no GC pauses. For an
  interactive analytics engine where p99 matters, this is a
  qualitative improvement.
- **Ecosystem alignment.** The current generation of new analytics
  infrastructure (DataFusion, Polars, InfluxDB 3, Materialize,
  Quickwit, Databend, GreptimeDB) has converged on Rust. Apache
  Arrow-rs and the `object_store` crate are first-class. SIMD,
  zero-copy parsing, mmap, async I/O, and gRPC all have mature,
  boring choices.
- **Single static binary.** No runtime to tune, small container
  images, fast deploys.

**Why not the alternatives:**

- *Java 21 + virtual threads* — would let us port code faster, but
  keeps the GC, the JVM tuning surface, and the cooperative-only
  memory accounting story. Doesn't address the core safety bugs.
- *Go* — operationally simple but the FTGS inner loop benchmarks
  20–40 % behind modern JVM and substantially behind Rust; the data
  race story is also weaker than Rust's.
- *Hybrid Rust+JVM* — doubles the build complexity and on-call
  surface; saves rewriting the control plane but inherits the JVM's
  operational drawbacks for it.
- *C++* — the only serious peer to Rust for this workload, with a
  deeper existing talent pool of database engineers, but the
  memory-safety, data-race-safety, and tooling stories are all worse
  for a new long-running multi-tenant daemon. New infrastructure in
  2026 picks Rust roughly 3:1 over C++.

**Concrete crate baseline** (subject to revision per layer):

| Concern | Crate |
|---|---|
| Async runtime | `tokio` |
| Memory-mapped I/O | `memmap2` |
| SIMD | `std::simd`, `std::arch` |
| Concurrency primitives | `crossbeam`, `parking_lot`, `arc-swap` |
| Compression | `zstd`, `lz4_flex`, `snap` |
| Serialization | `prost`, `serde` |
| Build/test/bench | `cargo`, `criterion`, `insta` |

### 6.2 Deployment design discipline

**Decision:** Confirm a deployment-agnostic binary design discipline.
The specific first-launch artifact (Helm chart vs. Terraform module vs.
just a binary) is deferred — most of this layer is reversible if the
binary is designed correctly from the start.

The binary must be designed so that K8s, VMs, Nomad, ECS, and bare
metal all remain possible targets. Specifically:

- **Stateless binary.** All persistent state lives in object storage;
  local storage is a rebuildable cache. The binary survives process
  death and node replacement with no data loss.
- **Layered configuration.** CLI flags > environment variables >
  config file > built-in defaults. Implemented with `clap` + `figment`
  (or equivalent).
- **Standard credential provider chains.** No hardcoded credentials.
  AWS SDK v2 chain (env → instance metadata → IRSA → SSO) and its
  GCP / Azure equivalents.
- **Graceful shutdown.** SIGTERM → stop accepting new sessions →
  drain in-flight queries → deregister from discovery → exit, with a
  configurable timeout.
- **Separate operations HTTP port** (default 8080), distinct from
  the gRPC port. Serves `/healthz`, `/readyz`, `/metrics`
  (§7.2), and `/status` (§7.3). Plain HTTP — suitable for K8s
  probes, AWS ALB, GCP LB, Consul health checks, and operator
  curl-debugging.
- **Pluggable service discovery.** A `ShardLocator` trait — *for
  client-side discovery of which daemons exist* — with default
  implementations for K8s DNS, a static config file, and
  (optionally) Consul / etcd. Distinct from `ShardSource` (§8),
  which is the server-side abstraction over *where shards come
  from* (object storage vs in-memory writer).
- **Daemons never call each other on the data plane.** Inherited
  from Imhotep and preserved deliberately. The deferred multi-writer
  ingest phase (§8) coordinates via shared object storage and an external
  coordinator (e.g. K8s leader election), not via direct
  daemon-to-daemon RPC — preserving the property.
- **L7-routable RPC.** Session affinity is required (a session is
  stateful on the daemon that opened it). The transport choice
  (gRPC + Arrow Flight, §C.1) is L7-routable everywhere modern.

### 6.3 Session routing and lifecycle

A session is in-memory state on the daemon that opened it, so every call
after `OpenSession` must reach that same daemon, and its lifetime is
bounded three ways:

- **Explicit close** by the client.
- **Idle timeout** — reaped after inactivity (default 30 minutes,
  configurable).
- **Maximum age** — a hard cap regardless of activity (default 4 hours,
  configurable), so a forgotten session cannot linger.

There is no cross-daemon replication or recovery: if the daemon holding a
session stops, the next call returns a `SessionLost` error and the client
reopens against another daemon.

Session tokens are opaque bytes — clients do not parse them — which keeps
the routing mechanism a separable concern. Two routing modes are
supported.

**Client-tracks-daemon (default).** `OpenSession` returns a
`(session_token, daemon_address)` pair. The client records the mapping and
sends every subsequent call for that session directly to `daemon_address`,
bypassing the load balancer, which then only ever sees the initial
`OpenSession`. The server returns its address from local config, so this
path needs no service-discovery dependency.

**L7 proxy with session affinity (opt-in).** Behind a service-mesh sidecar
(Envoy, Linkerd) or an affinity-capable L7 proxy, the daemon publishes the
session ID in the metadata of every response and the client echoes it on
every call (header `x-tutankhamun-session-id`). The proxy routes
consistently — by consistent hashing on the session ID, or via its own
session-to-daemon map. The client is configured to send all requests to
the proxy address; the protocol is otherwise identical.

**Failure modes:**

| Failure mode | How it is handled |
|---|---|
| Daemon crashes mid-session | `SessionLost` error; client reopens against a different daemon |
| Client crashes / disconnects | Idle timeout reaps the session within 30 min |
| Runaway session (forgotten by a long-lived client) | Maximum age (default 4 h) forces termination |
| Load balancer cannot route subsequent calls | N/A by default — the client bypasses the LB after `OpenSession` |
| Daemon drained for shutdown | Operator stops `OpenSession` accepts and waits for active sessions to close or time out; no in-flight migration |

Rolling restarts drop the sessions pinned to a restarting daemon; clients
reopen. Session migration across daemons (a registry-backed scheme that
survives restarts) is a possible future extension — the opaque-token rule
keeps the client-facing format unchanged if it is ever added.

### 6.4 Further stack layers — concrete deployment artifacts deferred

Build / packaging / Helm chart / Terraform module / OCI image / CI
are operational deliverables, not architectural decisions. They
flow from the design discipline in §6.2 and can be built in any
order once the binary is shippable.

## 7. Security & operations

Operating the daemon: authentication posture, telemetry, and the status surface.

### 7.1 Auth — deferred; lab / trusted-network mode for now

**Decision:** The current design ships **without authentication or
authorization**. Deployments are constrained to **trusted networks only** —
corporate VPN, K8s internal network policy, AWS VPC private
subnets, single-tenant clusters. Network isolation is the security
boundary.

This is the standard pattern for new analytics infrastructure
(early Cassandra, Elasticsearch, ClickHouse, Redis all started this
way) and has a clean upgrade path.

**What "lab / trusted-network mode" means concretely:**

- The `username` field on requests is a **claimed label, not a
  verified identity**. Logged for diagnostics; not enforced.
- No multi-tenant deployments — cannot enforce per-tenant memory
  caps, per-user quotas, or RBAC without trustworthy identity.
- No public-internet exposure — the daemon's gRPC port is never
  reachable from untrusted networks.
- No cost attribution rollups (see §7.2 — explicitly deferred for
  the same reason).
- Operations port (status page, Prometheus) is also unauthenticated;
  protected by network policy.

**Roadmap for when auth becomes load-bearing:**

- **mTLS** for service-to-service authentication (gRPC has
  first-class support).
- **OIDC / JWT** for user identity, validated at the gRPC
  interceptor layer.
- **RBAC** for per-user / per-tenant authorization (dataset-level
  grants, query quotas, session limits).

**Small disciplines from day one** (so auth plugs in cleanly later):

- The `username` field in requests is a `Option<String>` with a
  comment explicitly noting "claimed identity, not verified." Do
  not hardcode it as `"anonymous"` or similar.
- gRPC interceptors are structured so that adding an
  authentication interceptor later is a single registration
  change, not a refactor.
- Session lifecycle and admission control are designed around an
  `Identity` type (even if today's `Identity` is just "whoever claimed
  this username"). Adding auth makes the `Identity` come from a verified
  source; everything downstream is unchanged.

**Triggers for accelerating auth:** first multi-tenant deployment;
first public-internet exposure; first compliance / audit
requirement.

### 7.2 Observability — minimal to start

**Decision:** Ship **standard infrastructure telemetry only** for now.
**No dedicated query history table.** **No cost attribution rollups.**
Both are deferred until real workflows demand them.

**The current design ships:**

- **Prometheus `/metrics` endpoint** — request rates, latency
  histograms, per-pool queue depths, memory pool usage, session
  counts, shard cache hit/miss, mmap'd bytes, error counts. Standard
  cardinality discipline (no per-user / per-query labels).
- **OpenTelemetry traces** — every query produces a trace with rich
  span attributes (client-claimed user, dataset, time range, memory
  claimed, rows scanned, rows returned, error type if any). Spans
  break down the phases (parse, plan, scan-per-shard, FTGS, merge,
  serialize).
- **Structured JSON logs to stdout** — one event per significant
  occurrence; tagged with trace ID for correlation with traces.

That covers ~90 % of operator diagnostic needs. Effort: ~1–2 weeks.

**Why not a dedicated query history table yet:**

Standard OpenTelemetry traces *are* a query history — every query is
a trace, with all the same information that would otherwise be in a
`_system.queries` row. Tracing UIs (Tempo, Jaeger, Honeycomb,
Datadog APM, etc.) already let operators filter slow traces, group
by attributes, see trends over time. A dedicated table adds
SQL-over-history as a convenience but does not add information.

What a dedicated table would *uniquely* enable — joining query
history with event data in SQL — is a real but rare workflow.
Deferred until a concrete use case justifies the build.

**Why not cost attribution yet:**

Cost attribution depends on **trustworthy user identity**, which
requires the auth model (deferred per §7.1). Until then, the
`username` field is whatever the client claims. Attribution data
without trustworthy identity is misleading.

Cost attribution also is most valuable for **multi-tenant SaaS
deployments** where internal chargeback matters. For team-scoped
deployments (each team runs its own daemon set) it is overkill.

Adding cost attribution later is cheap once the trace data exists —
mostly aggregation views over already-collected spans plus an
identity-trust upgrade.

**What's added cheaply later if demanded:**

- Query history as a queryable `_system.queries` dataset
  (subscribe to completed-query trace stream → write into a
  Tutankhamun dataset). ~1–2 weeks.
- Cost attribution rollups over trace data (once auth identity is
  trustworthy). ~1 week.

### 7.3 Web UI — minimal status page

**Decision:** Ship a **minimal read-only status page** on the daemon
for now. No query execution surface, no dashboards, no auth surface.

**What it shows** (plain HTML at e.g. `/status` on the daemon's HTTP
port — co-served with `/healthz`, `/readyz`, `/metrics`):

- Daemon health and build version.
- Loaded shards (dataset, time range, size on disk, size mmap'd).
- Active sessions (count, oldest session age, total session memory).
- Memory pool usage (claimed / available / per-session breakdown).
- Recent queries — last ~50 from an in-memory ring buffer
  (non-persistent; for "what is the daemon doing *right now*",
  not history).
- Pointers to the Prometheus and OpenTelemetry endpoints for
  deeper diagnostics.

**Why this scope and not more:**

- **First-impression UX.** A new operator deploying Tutankhamun can
  point a browser at the daemon and see "yes, it's alive, here's
  what it's doing" without first standing up Grafana / Tempo. This
  is a real adoption-barrier reduction for ~1 week of work.
- **No query execution surface.** Tutankhamun is an engine, not a
  product. The query UI audience already has excellent options
  natively speaking Arrow Flight / FlightSQL — Superset, Hex,
  Mode, Tableau, Jupyter notebooks. Building a competitor to those
  is months of work with unclear payoff and would shift Tutankhamun
  from being an engine to being a product.
- **No auth surface needed.** Read-only status; if the network can
  reach the page, the operator is authorized to see it (standard
  operator-network discipline). Auth on the operations port is a
  separate question from auth on the data plane (gRPC) and can be
  added with reverse-proxy patterns when needed.

**Effort:** ~1 week. Uses a minimal HTML template; data is already
collected by the metrics / session manager subsystems.

**A full query console** (Druid Console / Hue / Superset equivalent)
is **explicitly not in scope.** If demand emerges, it is a
separable downstream project — engine ships the Flight / FlightSQL
surface that makes building one straightforward.

## 8. Evolution — incremental ingestion

**Decision:** Tutankhamun supports incremental ingestion. Build it
**in three phases**, shipping value at each, deferring complexity
until the demand justifies the cost. The current design is **batch
ingest only** but commits to a small set of design disciplines that
keep the streaming and multi-writer phases cheap to add.

### 8.1 The phases

| Phase | Capability | Lag | Effort | Notes |
|---|---|---|---|---|
| **Batch** (current) | Batch only — shards built offline, uploaded to object storage, read by daemons. | Hours / days. | Already needed. | Preserves Imhotep's model. |
| **Streaming** (deferred) | Single-writer streaming per dataset — WAL + in-memory shard + background flush. | Sub-second. | ~3–4 months focused work. | Covers ~all analytical use cases that need fresh data. |
| **Multi-writer** (deferred, later) | Multi-writer per dataset — partition ownership, cross-writer compaction, repartitioning on failure. | Same (sub-second). | Additional ~2–3 months. | For datasets where per-dataset write throughput exceeds a single daemon's capacity. |

The query engine is **unchanged** between phases. Only the ingest
path evolves.

### 8.2 Batch → streaming design disciplines (commit now)

These cost ~nothing today and save ~1.5 months when streaming lands:

1. **`Shard` as a trait, not a concrete struct.** The current design
   has one implementation (`DiskShard` — mmap'd Arrow IPC + Roaring +
   FST). Streaming adds `MemoryShard` to the same trait. The query
   engine sees `&[Arc<dyn Shard>]` and does not care which type each
   shard is.
2. **`ShardSource` as a trait.** Today's source is "object storage
   listing." Streaming adds an "in-memory writer" source. The shard
   manager composes from multiple sources transparently.
3. **Daemon has a writable local state directory** (configured via
   flag). Today it is the unpacked-shard cache. Streaming puts the WAL
   there. The current design commits to the discipline; streaming adds
   the file.
4. **Time-range is first-class shard metadata.** Time-range pruning is
   needed anyway. Make it an explicit `Shard::time_range()` method,
   not a property derived from the shard filename. Streaming's
   `MemoryShard` reports `[earliest_event, now]` and the same
   pruning code works.
5. **Session API does not leak shard types.** Clients open sessions
   against datasets and time ranges; they never see "shard kind."
   This means a session can transparently combine disk and in-memory
   shards.

These are recorded as **load-bearing engine invariants** in §4 and
must not regress.

### 8.3 Streaming → multi-writer design disciplines (commit when shipping streaming)

Three further disciplines in the streaming phase keep multi-writer
cheap:

1. **Don't assume single writer per dataset in the data model.** The
   streaming writer is *the* writer; multi-writer has one writer per
   partition. The shard manager and metadata should already model
   "this dataset has shards from possibly many writers."
2. **Shard naming includes a writer identity.** Streaming uses a
   constant (`writer-0`); multi-writer uses real writer IDs. Same
   naming scheme.
3. **Compaction logic handles the general case** of multiple writers
   producing shards over overlapping time ranges. The streaming case
   is degenerate (one writer); multi-writer uses the same code.

### 8.4 Streaming architecture sketch (for planning, not committing)

When streaming is built, the shape is:

- **WAL** — append-only file per dataset in the daemon's local state
  directory. Records every accepted event before it becomes
  queryable. ~1–2 weeks.
- **`MemoryShard`** — mutable in-memory implementation of the `Shard`
  trait. Different internal structures from `DiskShard`
  (`HashMap` term dictionary, growable `Vec<i64>` columns) because
  FSTs and Arrow IPC buffers are immutable-by-design. Converted to
  the disk format on flush. ~3–5 weeks.
- **Concurrency** — `arc-swap` snapshot pattern. Writer accumulates
  into a pending buffer; periodically (every ~100 ms) atomically
  swap a built snapshot for readers. Bounded read staleness, no
  lock contention. ~1–2 weeks.
- **Flush mechanism** — when `MemoryShard` reaches a size threshold
  or a time threshold, seal it, create a new one for incoming
  writes, serialize the sealed one to Arrow IPC + Roaring + FST,
  upload to object storage, register as a normal disk shard, drop
  the in-memory copy. WAL entries are truncated on successful flush.
  ~1–2 weeks (correctness of hand-off is the tricky bit).
- **DoPut endpoint** — Flight `DoPut` accepts Arrow record batches
  from clients. Wired into the writer alongside any pull-based
  connectors (Kafka / Kinesis / Pub-Sub) which are themselves
  modular.
- **Compaction job** — hourly batch process merges small recent
  shards into larger ones. Simple for a single writer; more
  sophisticated under multi-writer to handle overlap.

The streaming phase commits to:
- **At-least-once delivery** (clients dedupe by event ID). Exactly-
  once is a separate, much harder problem; not needed for analytics.
- **Fixed schema per dataset.** Schema evolution is a separate
  future feature; streaming datasets declare a schema at creation and
  it is stable.
- **Single writer per dataset.** Throughput cap = one daemon's
  ingest capacity. For practical analytics workloads (thousands to
  low millions of events/second per dataset) this is plenty.

### 8.5 What is not built in the streaming phase (deferred to multi-writer or later)

- Multi-writer / horizontal write scale.
- Exactly-once semantics.
- Schema evolution within a dataset.
- Sophisticated cross-writer compaction.

### 8.6 What never gets built (positioning)

- An equivalent of Druid's full streaming subsystem with all its
  bells and whistles (lookups joined into streams, ingest-time
  rollup, complex partitioning by multiple dimensions, etc.).
  Tutankhamun's streaming surface is "fast enough, simple to run" —
  not "every Druid feature."

## Appendix A: Imhotep heritage

Tutankhamun is the successor to [Imhotep](../../imhotep), Indeed's
archived large-scale interactive analytics engine (retired 2021). It
preserves Imhotep's core query model and rebuilds the stack on modern
foundations. The query surface is SQL over Arrow Flight, not Imhotep's
IQL — IQL is not preserved, as no Imhotep-migration users exist.

For background on the predecessor system:

- [`FEATURES.md`](../../imhotep/docs/modernization/FEATURES.md) — what the engine does
- [`ARCHITECTURE.md`](../../imhotep/docs/modernization/ARCHITECTURE.md) — how it is built
- [`PIPELINE.md`](../../imhotep/docs/modernization/PIPELINE.md) — end-to-end dataflow
- [`IQL.md`](../../imhotep/docs/modernization/IQL.md) — the query language

### A.1 What carries over

The behavioural core is preserved; only the encoding and implementation
change.

- **FTGS as the single aggregation primitive** — one sorted streaming
  `(field, term, group, stats)` iterator that every query reduces to,
  with one merger reused at the daemon and client layers (§4.1).
- **The stateful session model** — a session mutating an in-memory group
  lookup and stat stack across many cheap operations (§2.2). In Imhotep
  this was largely *latent*: IQL opened and closed a session per query,
  so most IQL users never experienced it as a feature — only custom
  Indeed dashboards on the raw protocol did. Tutankhamun makes it
  first-class through a native client library (§2.2).
- **Group-lookup specialization by cardinality** — `Constant`,
  `BitSet`, `Byte`, `Char`, `Int` backings swapped automatically as
  group count changes, which keeps memory bounded across many concurrent
  sessions (§4.2).
- **Deterministic hash-based sampling** — reproducible cohorts across
  queries and days.
- **Immutable shards as the storage substrate** (§3.1).

### A.2 What modernization changes

The rewrite is not just maintenance; the new stack opens capabilities
Imhotep could not have.

- **Predictable latency** — Rust, no GC, no JIT warmup, versus the JVM's
  pauses and warmup.
- **Honest memory accounting** — including mmap'd memory, which Imhotep's
  `ImhotepMemoryPool` did not track (§5.1).
- **Native Arrow Flight surface** — zero-copy results into the Arrow
  ecosystem, versus Imhotep's bespoke wire format (§2.1, §C.1).
- **Modern operational surface** — OpenTelemetry traces, Prometheus
  metrics, structured logging, none of which Imhotep had (§7.2).
- **Single static binary** — no JVM to tune, small images, fast deploys.

### A.3 Migration and format notes

Old Imhotep Flamdex shards are not directly readable; a one-time
migration tool ports them to the new format. The port is straightforward
because the logical model — sorted terms, per-term postings, per-doc
metric columns — is preserved exactly (§3.1). IQL is not carried over; the
query surface is SQL (§2.3).

## Appendix B: Decision status & open questions

The decisions above are settled at the architectural level; what remains is implementation plus a small set of deliberately deferred items.

- **Deferred by design:** streaming and multi-writer ingestion (§8), authentication (§7.1), pre-computed sketches and broader sketch types (§4.3), and spill-to-disk and per-tenant quotas (§5.1).
- **Open questions** surface as work proceeds; record each as a new decision with its rationale and the alternatives considered, in the style of the sections above.

## Appendix C: Protocol and session internals

Implementation detail behind §2 — the transport that carries queries and
results, and the caching that makes session refinement cheap.

### C.1 Wire transport

The transport is gRPC over HTTP/2. Query traffic uses Arrow Flight
(specifically FlightSQL): statements go out and result record batches come
back as Arrow IPC, which is why results are zero-copy end to end (§2.1).
Flight is itself a gRPC service, so it shares one HTTP/2 connection, one
TLS setup, one interceptor chain, and one tracing path with the rest of
the server — a single transport with more than one service definition, not
multiple protocol surfaces.

Session context rides in gRPC metadata: a session token attached to a
request routes its SQL through session-aware planning (§C.2). A separate
typed control protocol for session mutation is not built; session and
group state are expressed as SQL over cached views instead (§C.2).

### C.2 Session state as cached views

Session and group state are modelled as incrementally-maintained
materialized views over the immutable shard set, exposed through
session-aware FlightSQL. The regroup vocabulary maps onto ordinary SQL
over a session-scoped, group-lookup-backed relation; a separate typed
control protocol is a deferred option, justified only by a concrete
workflow SQL cannot serve cheaply.

**The reframing.** The per-doc group lookup *is a relation*,
`groups(doc_id, group_id)`. Every regroup derives a new such relation;
"operate on group 3" is `WHERE group_id = 3` — addressable, because it is
a column. The typed ops map onto SQL:

| Regroup op | SQL form |
|---|---|
| Regroup (replace) | `CREATE OR REPLACE TEMP VIEW _g AS SELECT doc_id, <expr> AS group_id …` |
| Regroup (refine group 3) | derive a new `_g` from the old, narrowing `group_id = 3` |
| "operate on group 3" | `… WHERE group_id = 3` |
| PushStat | a computed column (`… , clicks*cpc AS revenue`) |
| top-N-by-X regroup | a subquery (`WHERE term IN (SELECT … ORDER BY … LIMIT n)`) — predicate-definable, computed server-side |
| push an *external* set | the one genuinely opaque case: upload via Flight `DoPut` into a session-private relation, then semi-join |

**Realization, not re-evaluation.** A classical view re-evaluates per
reference; here the realization is **cached and reused**. The cached
artifact is the doc-set as a Roaring bitmap (§3.1 already stores postings
this way). Refinement on a narrowing predicate (`old AND extra`) is a
bitmap **intersection**, not a rescan — Lucene's cached-`DocIdSet` model.
The cache is a tree of nested predicates: a new query reuses its nearest
cached ancestor and applies the delta. The cheap path is **monotone
narrowing**; widening or a non-subset jump falls back to the nearest
ancestor or a fresh scan. Exploration is overwhelmingly narrowing — that
is the structural reason this works.

**Why it is safe and stale-proof.** A session pins a shard set and shards
are immutable (§3.1), so a realized result is a pure function of
`(normalized sub-plan, shard set)` — **content-addressable, never stale
within a session, and shareable across sessions.**

**Name scope ≠ realization scope.** A session-private *name* can point at
a daemon-shared *realization*. Three scopes:

| Scope | Holds | Lifetime |
|---|---|---|
| Session | the mutable cursor (current grouping, stat stack) and views over session-private (`DoPut`-pinned) data | reaped with the session |
| Daemon (shared cache) | content-addressed realizations of deterministic sub-plans over immutable shards (bitmaps, FTGS sub-results) | LRU; rebuildable; lost on restart |
| Cluster (catalog) | persisted *named* view definitions (shared cohorts), in object storage | survives restarts; realized per-daemon into the shared cache on use |

This mirrors the shard-file cache (§3.3), already daemon-scoped and
shared; the derived-result cache is the same idea one level up.
`CREATE TEMP VIEW` is a session-scoped name, `CREATE VIEW` persists the
definition to the catalog, and "promotion" just publishes the name — the
bytes were already shared. What *forces* session scope is narrow: the
mutable cursor, and any view over session-private uploaded data.
Everything deterministic over base data is shareable, and should be — 50
analysts who all start with `country = 'US'` compute that bitmap once.

**Caveats reserved now (not yet built):**

- **Memory budget.** A daemon-wide shared cache competes with session
  working sets; it needs the `MemoryBudget` / `SessionMemoryHandle`
  accounting of §5.1. Until that lands the shared cache stays small/off.
- **Authorization boundary.** Keying the shared cache on
  `(plan, shard set)` is correct only while every session sees the same
  data. The key must reserve a visibility/tenant dimension so deferred
  auth (§7.1) cannot leak rows across users via a shared bitmap — trivial
  today (claimed identity, no enforcement), but the slot must exist now.

**Engine deliverable.** Transparency reduces to one component: a
session/daemon **cache keyed by normalized logical sub-plan → Roaring
bitmap**, plus a planner rule that probes the cache and recognizes
monotone narrowing (intersect, not rescan). The group-lookup backings
(§4.2) are the in-memory representation of a session-scoped `_g`, and the
FTGS scan (§4.1) already consumes a `&GroupLookup` positionally — so the
join against `groups(doc_id, …)` is an array lookup, not a hash join. No
second wire surface.

**What stays open.** Whether a typed control protocol is ever needed is
decided by real usage of the one ergonomically-awkward case — pushing a
large *external* set (`DoPut` + semi-join). Build it only if that proves
load-bearing; it would still ride the shared engine and session layer, not
a parallel implementation.
