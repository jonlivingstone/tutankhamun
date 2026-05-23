# Tutankhamun

A modernized successor to [Imhotep](../../imhotep) — Indeed's archived
large-scale interactive analytics engine. Tutankhamun preserves Imhotep's
core query model (FTGS aggregation, regroup primitives, stat stack,
session lifecycle) but rebuilds the stack on modern foundations and
extends the system with a small set of new capabilities. The query
language surface is SQL via Arrow Flight, not Imhotep's IQL — IQL is
not preserved (no Imhotep-migration users exist).

This document is the design log: each decision is recorded with its
rationale and the alternatives considered.

For background on what carries over, see the Imhotep documentation set:

- [`FEATURES.md`](../../imhotep/docs/modernization/FEATURES.md) — what the engine does
- [`ARCHITECTURE.md`](../../imhotep/docs/modernization/ARCHITECTURE.md) — how it is built
- [`PIPELINE.md`](../../imhotep/docs/modernization/PIPELINE.md) — end-to-end dataflow
- [`IQL.md`](../../imhotep/docs/modernization/IQL.md) — the query language

---

## 0. Positioning

Tutankhamun is not a Druid clone. Apache Druid is the closest mainstream
analogue (same era, same workload, parallel architecture), but Imhotep
made deliberately different design choices that Tutankhamun preserves and
sharpens.

### 0.1 What Tutankhamun is for

Teams that want **interactive, session-based exploratory analytics** on
time-stamped event data — the workflow Druid does not quite support.
Specifically:

- Predictable low-latency dashboards with a predictable tail.
- Deterministic sampling for cohort analysis as a first-class feature.
- Operational simplicity (one binary, no ZooKeeper, no metadata DB).
- Native zero-copy result delivery into the Arrow ecosystem
  (Polars / Pandas / DuckDB / Jupyter notebooks).
- Batch ingest in v1; sub-second incremental ingest from v2 onward
  (progressive plan — see §3.3).

### 0.2 Distinctive design features inherited from Imhotep

These are the things Tutankhamun must preserve to remain itself:

1. **Stateful sessions** (latent in Imhotep — explicit in Tutankhamun).
   A session mutates an in-memory group lookup and stat stack across
   many cheap operations, rather than starting fresh on every query.
   This is the foundation of interactive exploration: each user click
   is one cheap mutation, not a fresh scan. **Note (honesty):** in
   Imhotep this capability was largely *latent* — IQL opens and
   closes a session per query, so most IQL users never experienced
   the session model as a user-facing feature. Custom Indeed
   dashboards that talked to the raw Imhotep protocol did exploit it.
   Tutankhamun makes the session model first-class by surfacing it
   through a **native client library** (§3.2), not through a new
   query language.
2. **FTGS as the single aggregation primitive.** One sorted streaming
   `(field, term, group, stats)` iterator; every query reduces to it;
   one merger that runs identically at the daemon and client layers.
3. **Group-lookup specialization by cardinality.** `Constant`,
   `BitSet`, `Byte`, `Char`, `Int` backings, swapped automatically as
   group count changes. This is what keeps memory bounded across many
   concurrent sessions.
4. **Deterministic hash-based sampling.** Reproducible cohorts across
   queries and days — a feature Druid does not have natively.
5. **Immutable shards as the storage substrate.** Every shard, once
   sealed and written to object storage, is immutable. v1 ships
   batch-only ingest (no compaction needed). v2 adds in-memory
   `MemoryShard`s for streaming, which are sealed and flushed to
   immutable disk shards — preserving the "all on-disk state is
   immutable" property that simplifies the query path. Compaction
   appears in v2 (small recent shards → larger ones) but operates on
   the same immutable substrate. See §3.3.

### 0.3 What Tutankhamun does *not* try to compete with

- Full SQL standard compliance (Druid SQL is more complete; we
  cover the analytical subset via DataFusion).
- Built-in approximate algorithms breadth (HLL, t-digest, theta —
  v1; broader DataSketches surface later if needed).
- Cloud-managed offerings, vendor ecosystem, community Slack mass.

Incremental ingestion *is* a supported capability — see §3.3 for the
lag tier and architecture choice.

### 0.4 What modernization unlocks beyond Imhotep

The modernization is not just maintenance. The new stack opens
capabilities Imhotep could not have:

- **Sub-millisecond p99 latency.** Rust + no GC + no JIT warmup.
- **Single static binary.** No JVM tuning, small container images,
  fast deploys — versus Druid's 5–7 process types.
- **Honest memory accounting.** Including mmap'd memory, which
  Imhotep's `ImhotepMemoryPool` does not track.
- **Native Arrow Flight client surface.** Zero-copy results into the
  Arrow ecosystem. Stock FlightSQL clients (`pyarrow.flight`,
  ADBC drivers, BI tools) work out of the box with no custom
  driver; the native client library (§3.2) adds the session-aware
  workflow API for analysts who want it. Primary competitive
  advantage, not an afterthought.
- **Modern operational surface in v1.** OpenTelemetry traces,
  Prometheus metrics, structured logging — none of which Imhotep
  had.
- **Deployment-agnostic, orchestrator-friendly.** Designed for K8s
  / Nomad / ECS / VMs / bare metal via the §1.3 design discipline
  (health endpoints, graceful shutdown, layered config, standard
  credential chains).
- **mTLS, OIDC, RBAC in v2.** Auth is deferred (§1.6) — v1 ships
  trusted-network-only.

Engine-level modernizations and net-new features are deferred to
sections after the stack is settled.

---

## 1. Stack

Decisions made layer by layer. Each subsection records the choice, the
alternatives, and the reasoning at the time of the decision.

### 1.1 Runtime — Rust

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

### 1.2 Wire protocol — gRPC for control, Arrow Flight for streaming

**Decision:** gRPC over HTTP/2 as the transport. Two services registered
on the same server:

- `tutankhamun.v1.SessionControl` — custom Protobuf service for
  session lifecycle and state mutation (`OpenSession`, `CloseSession`,
  `Regroup`, `PushStat`, `PopStat`, `MetricRegroup`, etc.).
- `arrow.flight.protocol.FlightService` — the standard Apache Arrow
  Flight service for streaming result data. FTGS streams and bulk
  scans are delivered as Arrow record batches via `DoGet(Ticket)`.

**Alternatives considered:** pure gRPC with custom bytes-payload
streaming; Arrow Flight only (forcing control through Flight `Action`s);
custom binary over QUIC.

**Why this combination:**

- **Arrow Flight gives the entire Arrow client ecosystem for free.**
  Python (`pyarrow.flight`), Polars, DuckDB, Pandas, R, JavaScript,
  Julia, JDBC-equivalent via ADBC all speak Arrow Flight natively in
  2026. A Jupyter notebook can pull Tutankhamun query results into a
  DataFrame in one call with **zero deserialization** — the bytes off
  the wire are the columnar in-memory layout. This is positioned as a
  primary competitive advantage of Tutankhamun: native, frictionless,
  zero-copy integration with the modern analyst's toolchain. Druid
  exposes FlightSQL but it is bolted on; Tutankhamun makes it the
  primary surface from day one.
- **Custom control service preserves typed, structured operations.**
  Session lifecycle and state mutation operations are small,
  structured messages (`PushStat { expression: String }`,
  `Regroup { rules: Vec<RegroupRule> }`). Expressing them as Flight
  `Action`s (opaque `(type, body)` blobs) loses typing, codegen,
  schema evolution, and IDE support. A purpose-built gRPC service is
  genuinely clearer.
- **Single transport, single auth, single observability.** Flight is
  itself a standard gRPC service, so registering two services on one
  `tonic::Server` shares the HTTP/2 connection, the mTLS setup, the
  interceptors, the load balancer config, and the tracing hooks.
  "Two protocol surfaces" is a misleading framing — it is one
  transport with two service definitions.

**What this commits us to:**

- FTGS results must be expressible as Arrow `RecordBatch`es. The shape
  `(field, term, group, stat_1, stat_2, ...)` maps trivially onto an
  Arrow schema; this is not a constraint that bites.
- The legacy Imhotep bespoke prefix-compressed FTGS wire format is
  retired. Arrow IPC is already very tight; no measurable throughput
  loss is expected, and the interoperability win is large.
- `tonic` and `arrow-flight` crates are added to the dependency
  baseline.

### 1.3 Deployment design discipline

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
  (§3.4), and `/status` (§3.5). Plain HTTP — suitable for K8s
  probes, AWS ALB, GCP LB, Consul health checks, and operator
  curl-debugging.
- **Pluggable service discovery.** A `ShardLocator` trait — *for
  client-side discovery of which daemons exist* — with default
  implementations for K8s DNS, a static config file, and
  (optionally) Consul / etcd. Distinct from `ShardSource` (§3.3),
  which is the server-side abstraction over *where shards come
  from* (object storage vs in-memory writer).
- **Daemons never call each other on the data plane.** Inherited
  from Imhotep and preserved deliberately. v3 multi-writer ingest
  (§3.3) coordinates via shared object storage and an external
  coordinator (e.g. K8s leader election), not via direct
  daemon-to-daemon RPC — preserving the property.
- **L7-routable RPC.** Session affinity is required (a session is
  stateful on the daemon that opened it). The transport choice
  (gRPC + Arrow Flight, §1.2) is L7-routable everywhere modern.

### 1.4 Object storage — `object_store` crate, all backends transparent

**Decision:** Use the **`object_store` crate** as the storage
abstraction. All its backends are supported transparently; the
operator picks one per dataset via configuration.

**Backends supported (all free, via `object_store`):**

| Backend | Use case |
|---|---|
| AWS S3 | Native AWS deployments; credential chain handles IRSA / instance profile / env vars / SSO |
| S3-compatible (MinIO, Ceph, R2, B2, Wasabi, Garage, SeaweedFS) | On-prem object stores or alternate clouds — same code path, different endpoint URL |
| Google Cloud Storage | Native GCP deployments; Workload Identity for auth |
| Azure Blob Storage | Native Azure deployments; Managed Identity for auth |
| Local filesystem | Single-machine / air-gapped / dev-loop deployments — no MinIO required |
| HTTP (read-only) | Niche; pulling shards from a CDN-style source |
| In-memory | Tests only |

**What this commits us to:**

- The engine never reads or writes a specific cloud SDK directly.
  Everything goes through the `object_store` trait.
- Credentials come from the cloud SDK's standard credential
  provider chain (env vars → instance metadata → IRSA / Workload
  Identity → SSO → static config). **No hardcoded credentials.**
- New backends supported by `object_store` upstream become available
  to Tutankhamun automatically when the dep is updated.

**Effort:** essentially zero — `object_store` is the right
abstraction; using it is the work.

### 1.5 Local hot storage — persistent cache with safe defaults

**Decision:** Persistent local cache on a configurable directory.
LRU eviction. Survives daemon restarts. **Safe defaults that work
with zero configuration**; explicit overrides for production sizing.

**Cache directory:**

- `--cache-dir <path>` controls location. **Default follows XDG
  conventions** (no root required, runs in user mode out of the
  box):
  - Linux: `$XDG_CACHE_HOME/tutankhamun/cache` (typically
    `~/.cache/tutankhamun/cache`)
  - macOS: `~/Library/Caches/tutankhamun/cache`
  - Windows: `%LOCALAPPDATA%\tutankhamun\cache`
- For system deployments (systemd unit, K8s container), the
  install convention is to pass `--cache-dir=/var/lib/tutankhamun/cache`
  explicitly. The daemon does **not** auto-switch paths based on
  root detection.
- Rust crate `directories` (or `dirs`) provides the XDG / OS
  conventions.

**Cache size:**

- `--cache-size <size>` accepts either absolute (`100GB`, `500MB`)
  or percentage (`50%` of cache filesystem total capacity) values.
- **Default: `10 GB`** — small enough to "just run" on a laptop
  without risk, large enough to be useful for trying things out.
- Production sizing is an **explicit operator decision** (e.g.
  `--cache-size 800GB` or `--cache-size 80%` on a 1 TB NVMe).

**Hard floor (always enforced, regardless of `--cache-size`):**

- Cache filesystem free space must stay **≥ 5 %** of total
  capacity.
- If LRU eviction cannot bring free space back above the floor,
  the daemon **refuses new shard downloads** and surfaces a clear
  error rather than letting the disk fill.
- Configurable via `--cache-min-free-pct` (default 5).

**Effective cap = `min(--cache-size, available-down-to-5%-floor)`.**
Both checks apply; whichever triggers eviction first wins.

**Other defaults:**

- **LRU eviction**, with `--pin-datasets <list>` to mark always-keep
  shards (skipped by LRU).
- **Persistent across restarts** — on startup, scan cache dir and
  register existing files. Validates content hash from shard
  metadata before use.
- **Lazy pre-warm** — shards download on first query. Configurable
  hot-set pre-warm via `--prewarm "<dataset>:<time-range>"`.
- **Content-hash validation** — every shard load verifies the
  stored checksum (one extra read per shard load, ~milliseconds).

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

### 1.6 Auth — deferred to v2; lab / trusted-network mode in v1

**Decision:** v1 ships **without authentication or authorization**.
Deployments are constrained to **trusted networks only** —
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
- No cost attribution rollups (see §3.4 — explicitly deferred for
  the same reason).
- Operations port (status page, Prometheus) is also unauthenticated;
  protected by network policy.

**Roadmap to v2 (when auth becomes load-bearing):**

- **mTLS** for service-to-service authentication (gRPC has
  first-class support).
- **OIDC / JWT** for user identity, validated at the gRPC
  interceptor layer.
- **RBAC** for per-user / per-tenant authorization (dataset-level
  grants, query quotas, session limits).

**Small disciplines from day one** (so v2 plugs in cleanly):

- The `username` field in requests is a `Option<String>` with a
  comment explicitly noting "claimed identity, not verified." Do
  not hardcode it as `"anonymous"` or similar.
- gRPC interceptors are structured so that adding an
  authentication interceptor in v2 is a single registration
  change, not a refactor.
- Session lifecycle and admission control are designed around an
  `Identity` type (even if v1's `Identity` is just "whoever claimed
  this username"). v2 makes the `Identity` come from a verified
  source; everything downstream is unchanged.

**Triggers for accelerating auth:** first multi-tenant deployment;
first public-internet exposure; first compliance / audit
requirement.

### 1.7 Further stack layers — concrete deployment artifacts deferred

Build / packaging / Helm chart / Terraform module / OCI image / CI
are operational deliverables, not architectural decisions. They
flow from the design discipline in §1.3 and can be built in any
order once the binary is shippable.

---

## 2. Engine

The engine is what Tutankhamun actually *is*. Stack decisions enable
it; the engine determines its identity. This section captures decisions
about storage format, FTGS implementation, group-lookup specialization,
session model, memory accounting, and concurrency — preserving
Imhotep's behavioural model while modernizing the encoding,
implementation, and operational surface.

### 2.1 Storage format — inverted index + Arrow IPC forward columns

**Decision:** Bespoke inverted index files (Roaring bitmaps + FST term
dictionaries) for term postings, combined with **uncompressed
single-batch Arrow IPC files** for forward (per-doc metric) columns.

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

**Engineering invariant for forward columns:** **one record batch per
Arrow IPC file, uncompressed for hot data.** This is the only way to
guarantee mmap'd zero-copy `&[i64]` access in the FTGS inner loop. If
this invariant is violated (small batches, default compression), the
hot path silently degrades. Cold-tier shards may opt into Arrow IPC's
LZ4 / ZSTD compression switch, accepting the access penalty in exchange
for storage savings.

**Why this combination:**

- **FTGS forward access is `metric[doc_id]` — random into a contiguous
  `int64` array.** Arrow IPC with the invariant above gives this
  exactly: mmap the file, parse the footer once, cast the values
  buffer to `&[i64]` via `bytemuck`. Inner-loop performance is
  identical to a fully bespoke layout — the Arrow metadata is paid
  once at file open, never in the hot loop.
- **Term iteration and per-term doc-ID retrieval are FTGS-specific
  inverted-index operations** that Parquet and Arrow handle poorly.
  Roaring bitmaps (industry-standard compressed bitmap format used by
  Lucene, Druid, Elasticsearch, ClickHouse) and FSTs (Lucene's term
  dictionary, used by Tantivy) are the right tools and have mature
  Rust crates.
- **Forward columns are externally readable.** DuckDB, Polars, Pandas,
  Spark can all open `metrics.arrow` directly. This is a primary
  competitive advantage and complements the Arrow Flight wire
  protocol (§1.2) — the same Arrow schema flows from disk to wire to
  client DataFrame with zero re-serialization.
- **No bespoke debug tooling needed.** `arrow-cli`, `duckdb`, and
  every Arrow-native tool can dump a forward column. Custom
  inverted-index files have well-specified formats (Roaring + FST)
  and the corresponding tooling exists in the Rust crates.

**Alternatives considered:**

1. *Faithful Flamdex port* — preserve format bit-for-bit. Free
   backward compatibility, but inherits 2014-era encoding choices,
   ongoing maintenance of a proprietary format, no external
   readability.
2. *Fully bespoke modernized format* — Roaring + FST for inverted
   index, custom layout for forward columns. Slightly faster
   file-open (negligible), no interop. No real performance advantage
   in the FTGS hot path versus the chosen design.
3. *Pure Parquet with bolt-on inverted-index files* — maximum
   standardization, but Parquet's row-group orientation and page
   compression are a poor fit for the FTGS hot path. Decompression
   on access kills the mmap'd zero-copy property that Imhotep depends
   on.

**Prior art:** This is essentially the **Lucene / Tantivy model** —
inverted postings as bespoke files, doc values as columnar files —
with Arrow IPC as the doc-values format (the standardization Lucene
predates). **Hudi, Iceberg, and Delta Lake** layer similar
auxiliary-index structures on top of columnar storage.

**Rust crate baseline:**

| Concern | Crate |
|---|---|
| Forward columns (Arrow IPC read/write, mmap) | `arrow-rs`, `bytemuck` |
| Inverted-index postings (Roaring bitmaps) | `roaring` |
| Term dictionaries (FSTs) | `fst` |
| Reference implementation to study | `tantivy` (Rust search engine using this model) |
| Optional Parquet export for archival / interop | `parquet` (in `arrow-rs`) |

**Backward compatibility:** Old Imhotep Flamdex shards are not
directly readable. A one-time migration tool
(`flamdex-to-tutankhamun`) ports the data — straightforward because
the logical model (sorted terms + per-term postings + per-doc metric
columns) is preserved exactly. The migration tool can also run as part
of the ingest pipeline rewrite, since the new ingest will produce the
new format natively.

**Parquet export path:** Any shard's forward columns can be exported
to Parquet for downstream warehouse loading via a single arrow-rs call
(Arrow IPC → Parquet). This means Tutankhamun data is reachable from
the data-warehouse world without making Parquet the native storage
format.

### 2.2 Memory model and accounting

**Decision:** Pragmatic v1 — hard enforcement, rough mmap accounting,
per-session caps, admission control at the front door, no spill-to-disk.

**The three sub-decisions:**

**Measurement.** Count engine allocations (group lookups, merge
buffers, posting decode buffers, session-owned state) **plus** the
mmap'd shard working set. When a shard is opened for a session, the
forward-column file sizes (`metrics.arrow` byte size) are charged to
that session's budget as a rough estimate of resident pressure. This
is intentionally cheap to compute — no `/proc/self/smaps` polling, no
per-page tracking — and closes Imhotep's known gap that operators
report as "daemon configured for 8 GB uses 60 GB resident"
([Imhotep `ARCHITECTURE.md` §6.3](../../imhotep/docs/modernization/ARCHITECTURE.md)).

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
daemon. No per-tenant quotas in v1; multi-tenancy is deferred until
the workload demands it.

**Failure-mode coverage:**

| Failure mode | How v1 prevents it |
|---|---|
| OS OOM-killer kills the daemon | mmap counted, hard global cap; daemon stays under its configured limit |
| One greedy query starves others | per-session cap (default 20 % of budget) |
| Thrashing under load | admission control rejects new sessions before total RAM is exhausted |

**What's deferred to later versions** (none are one-way; all extend
cleanly from v1):

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

### 2.3 Concurrency and threading

**Decision:** **Tokio for I/O, Rayon for CPU-bound compute.** Two
bounded pools, each tuned to its workload. This directly addresses
Imhotep's "five unbounded cached thread pools" anti-pattern
(see Imhotep [`ARCHITECTURE.md` §7.1](../../imhotep/docs/modernization/ARCHITECTURE.md))
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
  later upgrade if profiling justifies it; not necessary for v1.
- *Pure Tokio work-stealing, no separate compute pool* — explicit
  anti-pattern documented by Tokio itself. CPU-bound loops on the
  same runtime as I/O block I/O tasks on the same thread, causing p99
  latency spikes under any compute load.

**Failure-mode coverage:**

| Failure mode | How v1 handles it |
|---|---|
| Thread explosion under load (Imhotep-style) | Both pools are bounded; admission control rejects new sessions before queues saturate |
| CPU work blocking I/O | Separated pools; async tasks dispatch CPU work and yield |
| Cache thrash from work stealing on hot data | Rayon's work-stealing localises per-shard work; per-shard data fits in L3 cache for typical shard sizes |
| Tail latency under burst load | Bounded pools + admission control + per-session memory cap (§2.2) combine to keep p99 predictable |

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

### 2.4 Session model and routing

**Decision:** Preserve Imhotep's session model unchanged in
behaviour; modernize the implementation. Session tokens are **opaque
bytes**. Default routing is **client-tracks-daemon** (option 1).
Deployment behind an **L7 proxy with consistent-hash routing** on the
session ID is a supported operational pattern (option 3). Encoded
daemon addresses (option 2) and a server-side session registry
(option 4) are both rejected for v1.

**The session model itself (preserved from Imhotep):**

- A session is **stateful in-memory on the daemon that opened it**.
  No persistence, no replication, no cross-daemon migration in v1.
- A session holds: a group lookup (per-doc → group ID array), a stat
  stack, dynamic metrics, a memory reservation handle (§2.2), and a
  set of mmap'd shard handles.
- The session is the unit of cheap mutation:
  filter → look at results → filter more → group differently →
  push a new stat → look again is one session with six operations,
  each reusing the previous state. This is the workflow Druid does
  not natively support (see §0).
- **Lifecycle**: explicit `CloseSession` from the client, or **idle
  timeout** (default 30 minutes, configurable), or **hard maximum
  age** (new in Tutankhamun, default 4 hours, configurable) to
  prevent runaway sessions.
- **Daemon death = session lost.** The next client call returns a
  clear `SessionLost` error; the client reopens. No partial-state
  recovery, no fail-over. This is consistent with the design
  discipline (no inter-daemon coupling) and with Imhotep's behaviour.

**Routing — the decision in detail:**

Session tokens returned by `OpenSession` are **opaque bytes**. The
client must not parse them. This single constraint preserves
flexibility — it rules out option 2 (encoded addresses) by design
and leaves the routing implementation as a separable concern.

**Default (option 1): client-tracks-daemon.**

- `OpenSession` returns `(session_token, daemon_address)`.
- The client library records `session_token → daemon_address` in an
  in-memory map.
- All subsequent operations on that session bypass the load balancer
  and connect directly to `daemon_address`.
- The LB only ever sees the initial `OpenSession` call.
- Implementation: lives entirely in the client library
  (`tutankhamun-client`). The server returns its address from local
  config (no service discovery dependency for this path).

**Supported deployment (option 3): L7 proxy with session affinity.**

- Operators may deploy Tutankhamun behind a service-mesh sidecar
  (Envoy, Linkerd) or a session-affinity-capable L7 proxy.
- The proxy reads the session ID from a known gRPC metadata header
  (`x-tutankhamun-session-id`) and routes consistently — either by
  consistent hashing on the session ID, or via a stateful
  session-to-daemon map maintained by the proxy.
- **Server-side requirement:** the daemon publishes the session ID in
  the metadata of every gRPC response (already true for streaming
  responses; cheap to add for unary). Clients echo the session ID on
  every subsequent call. No other server-side change is needed.
- **Client-side requirement:** when configured for proxy mode, the
  client library sends *all* requests for a session to the proxy's
  address (not the daemon's), and includes the session ID header on
  every request. The daemon-address returned in `OpenSession` is
  ignored.
- This mode is **opt-in via client configuration**; the protocol is
  identical.

**Why option 2 (encoded daemon address) is rejected:**

- **Security:** the token leaks internal cluster topology to every
  client. In multi-tenant deployments this is a real defense-in-depth
  regression versus opaque tokens.
- **Networking:** the daemon's address as seen from inside the
  cluster is frequently different from its address as seen from
  outside (NAT, private VPC, port-forwarding, hybrid cloud,
  dev-vs-prod hostname differences). Hard-coding the address in a
  token guarantees the token is wrong from some viewpoint.
- **Operational rigidity:** every daemon rename, network
  reconfiguration, or migration invalidates outstanding tokens. There
  is no way to reissue tokens without sessions being torn down.

**Why option 4 (server-side registry) is deferred:**

- The primary motivation for a registry is **session migration**
  (rolling restarts without dropping sessions). This is a real
  feature but a substantial subsystem — persistent session state, a
  registry service to operate, a migration protocol between daemons,
  cross-daemon memory accounting. Imhotep never had it; Tutankhamun
  can add it later if the operational pain justifies it.
- Today, daemon restarts drop sessions; clients reopen. The 30-minute
  idle timeout means a rolling restart with reasonable drain time
  affects a small minority of active sessions.

**Failure-mode coverage:**

| Failure mode | How v1 handles it |
|---|---|
| Daemon crashes mid-session | `SessionLost` error; client reopens against a different daemon |
| Client crashes / disconnects | Idle timeout reaps the session within 30 min |
| Runaway session (forgotten by a long-lived client) | Hard maximum age (default 4 h) forces termination |
| Load balancer cannot route subsequent calls | N/A — by default the client bypasses the LB after `OpenSession` |
| Session pinned to a daemon being drained for shutdown | Operator drains by stopping `OpenSession` accepts and waiting for active sessions to close or time out; no in-flight migration |

**Future option: session migration (registry-backed).** If
deployments emerge where rolling restarts without session loss become
operationally important, the registry pattern (option 4) is the
natural extension. The opaque-token decision keeps the door open —
adding a registry does not change the client-facing wire format.

### 2.5 Group-lookup specialization

**Decision: preserve Imhotep's design in Rust.** A `GroupLookup`
trait with backing implementations specialized by current group
cardinality:

| Backing | Cardinality | Bytes / doc |
|---|---|---|
| `ConstantGroupLookup` | 1 | 0 (single value) |
| `BitSetGroupLookup` | 2 | 1 bit |
| `ByteGroupLookup` | ≤ 256 | 1 byte |
| `U16GroupLookup` | ≤ 65 536 | 2 bytes |
| `U32GroupLookup` | up to 2³² | 4 bytes |

The engine upgrades the representation in place when a regroup pushes
cardinality past the current type's limit. Dispatch is hidden behind
a `next_group_callback(doc_ids, &mut BitTree)` strategy call, used by
the FTGS inner loop (§2.6).

**Why preserve:** this is the largest single reason Imhotep keeps
many concurrent sessions in bounded memory on a daemon. Specializing
by cardinality is the right answer; there is no better-known design
for this access pattern.

**What changes in the Rust port:**

- **Memory accounting.** Each backing reports its byte cost to the
  session's `MemoryReservation` (§2.2). Imhotep's accounting here was
  approximate; Rust's RAII makes it exact via drop semantics.
- **Type safety.** A `GroupLookup` enum (instead of Java's
  interface + instanceof checks) lets the compiler verify backing
  swaps preserve invariants.
- **No native code path needed.** Imhotep had optional JNI for the
  inner loop; in Rust, the same code can use `std::simd` or
  `std::arch` intrinsics directly without an FFI boundary.

### 2.6 FTGS implementation

**Decision: preserve the FTGS algorithm and its invariants;
modernize the implementation.**

**Preserved:**

- The four-level cursor (`next_field` → `next_term` → `next_group` →
  `group_stats`).
- Strict ordering invariants (terms sorted within a field; groups
  ascending within a term; fields enumerated in declaration order).
- The split-then-merge decomposition for parallelism — per-shard
  splits, then N-way sorted merge using a compact two-level bitmap
  (`GSVector` equivalent).
- The "critical loop" structure: walk doc-ID batches from a term's
  postings, dispatch to `GroupLookup::next_group_callback`,
  accumulate into `term_grp_stats[stat][group]`.

**Modernized:**

- **Streaming over Arrow.** FTGS results are produced as Arrow record
  batches (one batch per N output rows, where N is sized for
  reasonable batching — typical 1024 or 4096 rows). The batches feed
  directly into the Arrow Flight stream (§1.2) with no
  re-serialization.
- **Postings via Roaring + FST.** Term iteration uses the FST's
  range scan; per-term doc-ID retrieval is a Roaring bitmap
  iteration. Both are SIMD-friendly and significantly faster than
  Imhotep's varint-delta postings for typical cardinalities.
- **Forward column access via mmap'd Arrow IPC.** Per-doc metric
  lookups (the hot inner loop) read directly from
  `bytemuck::cast_slice::<u8, i64>(arrow_buffer)` — one memory read,
  no decompression, no deserialization.
- **Native SIMD without JNI.** Vector intrinsics live in the same
  crate; no FFI overhead.
- **No bespoke FTGS wire format.** Imhotep's hand-rolled
  prefix-compressed binary FTGS stream is retired; Arrow IPC is
  already very tight and is the standard.

**The merge code runs at two layers, unchanged in structure from
Imhotep:** within a daemon (across that daemon's shards) and within
the client library (across daemons). "Distribution is recursion" —
the same merge function is reused at both levels.

### 2.7 Approximate aggregations

Decided in §3.1 (net-new capability over Imhotep — HLL + t-digest +
theta sketches, `approx_` prefix, query-time only in v1).

---

## 3. New features beyond Imhotep parity

Decisions captured in order. Open candidates remain at the end of this
section.

### 3.1 Approximate aggregations

**Decision:** Add **HLL, t-digest, and theta sketches** as net-new
stat-stack primitives in v1. Use the **`approx_` prefix** convention
matching every other major SQL engine (BigQuery, Snowflake, Spark,
Druid, Trino, DuckDB). Query-time computation only in v1;
pre-computed sketch columns at ingest are deferred.

**Background — what Imhotep had:**

Confirmed by grepping the IQL source: zero hits for `theta`, `sketch`,
`hll`, `hyperloglog`, `datasketch`. Imhotep had only two functions of
this kind, both **exact**:

- `distinct(field)` — exact distinct using a hash set; memory blows up
  on high-cardinality fields.
- `percentile(field, p)` — exact percentile; same scaling problem.

Both were implemented as special end-of-pipeline groupings, not as
mergeable stat-stack citizens. Both fall over on large data.

Adding HLL / t-digest / theta is **net-new capability**, not
modernization of an existing feature.

**The v1 API:**

| Function | Returns | Required args | Optional args | Backed by |
|---|---|---|---|---|
| `approx_count_distinct(field)` | int64 | field | precision | HyperLogLog |
| `approx_percentile(field, p)` | int64 | field, p (0–100) | compression | t-digest |
| `approx_top_k(field, k)` | array | field, k | capacity | Count-Min + heavy hitters |
| `theta(field)` | binary (sketch) | field | nominal_entries | Theta sketch |
| `theta_intersect(a, b)` | int64 (estimated size) | two thetas | — | Theta sketch |

Exact `distinct(field)` and `percentile(field, p)` are also available
for the small-data case where exactness matters and memory blow-up is
not a concern. The `approx_*` functions are the recommended forms for
any non-trivial cardinality.

**Why `approx_` and not `f` for fuzzy:**

- Industry-unanimous convention across Druid, BigQuery, Snowflake,
  Spark, Trino, DuckDB. Analyst muscle memory matches.
- Self-documenting; `f` would require lookup ("does `f` mean float?
  filtered? function? fuzzy?").
- IQL already has `floatscale(field)` — an `f`-prefixed function
  unrelated to approximation. Two different meanings of `f` would
  confuse readers.
- "Fuzzy" usually denotes other concepts (fuzzy logic, fuzzy string
  matching). "Approximate" is the literature term.
- Tutankhamun's positioning (Arrow / DataFusion / DuckDB ecosystem
  fit) means BI tools and SQL-aware tooling expect `approx_*`.

**Why theta is in v1 (not deferred):**

Theta is the sketch type that unlocks **cohort intersection** —
"how many users are in both cohort A and cohort B" — which is
otherwise very expensive and is a common question in analytics
workflows. It is also the sketch most naturally exposed as a
returnable binary value (composable across sessions), which fits
Tutankhamun's interactive session model. Cheap to include alongside
HLL since both come from the same upstream library.

**Why query-time only in v1:**

- **Pre-computed sketches at ingest** (Druid's approach) deliver
  "billion-row distinct count in 100 ms" by storing the HLL as an
  extra column at build time. Significantly faster for very large
  shards, but requires:
  - A schema concept ("which fields have which sketches").
  - Ingest-pipeline complexity to compute sketches at build time.
  - Storage-format extension (extra columns alongside `metrics.arrow`).
- The query-time path is fast enough for the typical interactive
  workload and slots cleanly into the existing FTGS pipeline with no
  format changes.
- Pre-computed sketches can be added later **without changing the
  wire protocol** — they're an internal optimization that the engine
  picks transparently when the column exists.

**Implementation notes:**

- Sketches live in a new `sketches` module. Rust crate baseline adds
  `datasketches` (Rust port of the Apache DataSketches Java library)
  or, if maturity is insufficient, `hyperloglogplus` + `tdigest` for
  HLL / t-digest with a custom theta implementation.
- Sketches are first-class stat-stack values:
  `approx_count_distinct(user_id) / count()` yields the unique-rate
  per group; `approx_percentile(latency, 99)` is a per-group p99.
- Merge across shards happens in the standard FTGS merge path —
  sketches are mergeable by construction (this is the whole point).
- Sketches stream out as Arrow record-batch columns of the
  appropriate type (int64 for cardinality / percentile estimates,
  `Binary` for raw thetas). Clients receive them as Arrow,
  zero-copy, and can either consume the scalars directly or further
  merge raw thetas client-side using PyArrow / DuckDB Arrow
  extensions.

### 3.2 Query surface — SQL via DataFusion, fluent client library; no IQL

**Decision:** **SQL is the wire query language**, served by an
embedded **Apache DataFusion** behind a **FlightSQL** service.
**A native client library** (Python first, then JS / Rust) exposes a
**fluent, session-aware API** as the first-class user surface for
interactive workflows. **IQL is not implemented** — Imhotep is
sufficiently dead that no migration users are expected, so a
compatibility layer would be pure overhead.

This collapses what looked like a SQL-vs-IQL choice into three
independent concerns, each living at the right layer:

| Concern | Lives at | Implementation |
|---|---|---|
| Query language (predicates, projections, aggregations) | Wire | SQL (DataFusion) |
| Session state (group lookup, stat stack, dynamic metrics) | Server | Tutankhamun engine (preserved from Imhotep) |
| Workflow API (build, refine, drill in incrementally) | Client | Native library (Python first) — fluent, lazy, session-bound |

**The realization that drove this decision:**

Session features in Imhotep were largely *latent* (§0.2). IQL itself
opens-and-closes a session per query — most IQL users never
experienced the session model directly. The session capability was
exploited by custom internal Indeed dashboards that talked to the
raw Imhotep protocol.

This means Tutankhamun does not need a *language* (IQL or a SQL
extension) to expose sessions. It needs a **client-side workflow
API** that holds session state in the library and translates user
operations into either session-aware SQL queries or direct gRPC
control calls. This is what every modern DataFrame library already
does (Polars LazyFrame, Spark DataFrame, ibis, DuckDB Python
relational API) — the pattern is well-trodden.

**What the user-facing API looks like (Python):**

```python
import tutankhamun as tk

session = tk.connect("grpc://tutankhamun:50051").session(
    dataset="logs",
    time_range=("2024-01-01", "2024-01-02"),
)
# Equivalently:
# session = tk.connect(...).session_from_sql(
#     "SELECT * FROM logs WHERE date >= '2024-01-01' AND date < '2024-01-02'"
# )

us = session.filter("country = 'US'")              # cheap regroup
by_device = us.group_by("device").select(
    "count(*), sum(revenue)"
).fetch()                                          # FTGS — fast, reuses state

mobile_us = us.filter("device = 'mobile'")         # further cheap regroup
by_city = mobile_us.group_by("city").select(
    "count(*)"
).fetch()                                          # FTGS — reuses state

session.define("ltv",
    "clicks * cpc + purchase_amount"
)                                                  # dynamic metric
by_country_ltv = us.group_by("country").select(
    "sum(ltv)"
).fetch()

session.close()                                    # or use as context manager
```

The library returns `pyarrow.Table` by default, with adapters for
Polars / Pandas. Zero-copy throughout — Arrow from disk to wire to
Python.

**Architecture, layer by layer:**

- **Server**:
  - **DataFusion** parses and plans SQL.
  - Tutankhamun is registered as a `TableProvider` — DataFusion
    pushes filter / group-by / aggregation down into the FTGS
    engine; joins / windows / CTEs / etc. execute in DataFusion.
  - **Sessions are first-class server state.** Each gRPC connection
    can carry a session token in metadata; SQL queries issued in a
    session context route through *session-aware planning* — the
    server reuses the existing group lookup if the new query's
    filter is a refinement, otherwise rebuilds.
  - **FlightSQL** is exposed as the SQL transport (the canonical
    Arrow-ecosystem way to ship SQL + results).

- **Client library** (Python first):
  - Holds a session token; exposes the fluent API above.
  - Each method composes a SQL fragment and tracks accumulated state
    (filter clauses, group-by, projections, defined metrics).
  - `.fetch()` issues the composed SQL via FlightSQL with the
    session token attached; receives Arrow record batches; returns a
    Table.
  - Adapters: `to_pandas()`, `to_polars()`, `to_duckdb()` are all
    zero-copy via Arrow.
  - Implementation note: **strongly consider building on `ibis`**
    rather than from scratch. Ibis already provides the
    fluent-deferred-SQL pattern with a DataFusion backend; adding a
    Tutankhamun backend that knows about sessions is a smaller
    project than rolling our own.

- **UI layer** (out of scope for the engine itself):
  - A Honeycomb-style interactive UI is enabled by the client library
    (each user click → library call → cheap session mutation +
    fast `fetch()`) but is a separable downstream project. Not
    something Tutankhamun the engine ships by default.

- **Stock SQL clients** (Tableau, Superset, Hex, JDBC consumers via
  ADBC, `pd.read_sql`, etc.):
  - Issue independent SQL queries via FlightSQL. No session
    awareness; each query is a fresh scan (or benefits from
    transparent server-side query caching, if added later).
  - They do not get the cheap-incremental property, but they get
    everything else: sub-ms p99, joins, approximate aggregations,
    Arrow zero-copy results.

**IQL is not implemented:**

The earlier framing considered preserving IQL as a compatibility
layer for existing Imhotep users. With no migration audience to
serve (Imhotep was archived in 2021 and has long since faded), an
IQL parser / translator / service in Tutankhamun would be pure
overhead — engineering cost to port, ongoing maintenance burden,
two execution paths to test, larger daemon surface — for zero
realized value. **IQL is therefore not in scope for Tutankhamun**,
v1 or otherwise.

Users coming from Imhotep IQL queries will need to translate them
to SQL when adopting Tutankhamun. The mapping is mostly
mechanical (see the [Imhotep IQL docs](../../imhotep/docs/modernization/IQL.md)
for the operations IQL provides; almost all map directly to SQL
equivalents), but it is a one-time migration cost the (effectively
nonexistent) audience would bear.

**Why not the alternatives:**

- *Hand-rolled SQL-to-IQL transpiler* — reinvents what DataFusion
  already does; cannot natively support joins; FlightSQL would need
  a custom implementation. Strictly worse than embedding DataFusion.
- *IQL only* — abandons the BI-tool / notebook audience entirely;
  contradicts the "Jupyter killer combo" positioning; means
  Tutankhamun is "Imhotep modernized" without reaching the wider
  analytics ecosystem.
- *IQL alongside SQL as a compat layer* — was the original plan;
  dropped once it was clear there are no migration users to serve.
  Listed here so the rejection is recorded; would be reopened only
  if a real Imhotep-using audience appears.
- *Custom SQL extensions for session mutation*
  (`SESSION DEFINE METRIC ltv = ...`) — invents non-standard SQL that
  stock BI tools cannot use, and reinvents in language syntax what
  the client library exposes more ergonomically as method calls.

**Server-side joins come for free.**

Embedding DataFusion delivers server-side joins as a side effect.
This subsumes the previously open candidate of "Server-side joins
via embedded DataFusion / DuckDB" — DataFusion is the answer to both.
A SQL query that joins a Tutankhamun dataset with a Parquet file is
trivially expressible:

```sql
SELECT t.country, sum(t.revenue) 
FROM logs t 
JOIN read_parquet('s3://attrs/2024-01-01.parquet') a USING (user_id)
WHERE a.tier = 'premium'
GROUP BY t.country
```

DataFusion plans the join; pushes the filter / projection into
Tutankhamun's scan; executes the join in its own vectorized engine.

**What this commits us to:**

- `datafusion` and `arrow-flight` crates added to the baseline.
- Server work to teach DataFusion's planner about Tutankhamun's
  session state (the "session-aware SQL execution" piece). Bounded
  but non-trivial engineering project.
- A **first-class Python client library** as a deliverable alongside
  the server. Probably a `tutankhamun` PyPI package; possibly built
  on ibis.
- The server's binary grows by ~5–10 MB from DataFusion.

### 3.3 Incremental ingestion — progressive plan

**Decision:** Tutankhamun supports incremental ingestion. Build it
**in three stages**, shipping value at each, deferring complexity
until the demand justifies the cost. v1 ships **batch ingest only**
but commits to a small set of design disciplines that keep v2 and v3
cheap to add.

#### The stages

| Stage | Capability | Lag | Effort | Notes |
|---|---|---|---|---|
| **v1** | Batch only — shards built offline, uploaded to object storage, read by daemons. | Hours / days. | Already needed. | Preserves Imhotep's model. |
| **v2** | Single-writer streaming per dataset — WAL + in-memory shard + background flush. | Sub-second. | ~3–4 months focused work. | Covers ~all analytical use cases that need fresh data. |
| **v3** | Multi-writer per dataset — partition ownership, cross-writer compaction, repartitioning on failure. | Same (sub-second). | Additional ~2–3 months. | For datasets where per-dataset write throughput exceeds a single daemon's capacity. |

The query engine is **unchanged** between stages. Only the ingest
path evolves.

#### v1 → v2 design disciplines (commit now)

These cost ~nothing in v1 and save ~1.5 months in v2:

1. **`Shard` as a trait, not a concrete struct.** v1 has one
   implementation (`DiskShard` — mmap'd Arrow IPC + Roaring + FST).
   v2 adds `MemoryShard` to the same trait. The query engine sees
   `&[Arc<dyn Shard>]` and does not care which type each shard is.
2. **`ShardSource` as a trait.** v1's source is "object storage
   listing." v2 adds an "in-memory writer" source. The shard manager
   composes from multiple sources transparently.
3. **Daemon has a writable local state directory** (configured via
   flag). v1 uses it as the unpacked-shard cache. v2 puts the WAL
   there. v1 commits to the discipline; v2 adds the file.
4. **Time-range is first-class shard metadata.** v1 needs time-range
   pruning anyway. Make it an explicit `Shard::time_range()` method,
   not a property derived from the shard filename. v2's
   `MemoryShard` reports `[earliest_event, now]` and the same
   pruning code works.
5. **Session API does not leak shard types.** Clients open sessions
   against datasets and time ranges; they never see "shard kind."
   This means a session can transparently combine disk and in-memory
   shards.

These are recorded as **load-bearing engine invariants** in §2 and
must not regress.

#### v2 → v3 design disciplines (commit when shipping v2)

Three further disciplines in v2 keep v3 cheap:

1. **Don't assume single writer per dataset in the data model.** v2's
   writer is *the* writer; v3 has one writer per partition. The shard
   manager and metadata should already model "this dataset has shards
   from possibly many writers."
2. **Shard naming includes a writer identity.** v2 uses a constant
   (`writer-0`); v3 uses real writer IDs. Same naming scheme.
3. **Compaction logic handles the general case** of multiple writers
   producing shards over overlapping time ranges. v2's case is
   degenerate (one writer); v3 uses the same code.

#### v2 architecture sketch (for planning, not committing)

When v2 is built, the shape is:

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
  shards into larger ones. Simple in v2; more sophisticated in v3
  to handle multi-writer overlap.

v2 commits to:
- **At-least-once delivery** (clients dedupe by event ID). Exactly-
  once is a separate, much harder problem; not needed for analytics.
- **Fixed schema per dataset.** Schema evolution is a separate
  future feature; v2 datasets declare a schema at creation and it is
  stable.
- **Single writer per dataset.** Throughput cap = one daemon's
  ingest capacity. For practical analytics workloads (thousands to
  low millions of events/second per dataset) this is plenty.

#### What is not built in v2 (deferred to v3 or later)

- Multi-writer / horizontal write scale.
- Exactly-once semantics.
- Schema evolution within a dataset.
- Sophisticated cross-writer compaction.

#### What never gets built (positioning)

- An equivalent of Druid's full streaming subsystem with all its
  bells and whistles (lookups joined into streams, ingest-time
  rollup, complex partitioning by multiple dimensions, etc.).
  Tutankhamun's streaming surface is "fast enough, simple to run" —
  not "every Druid feature."

### 3.4 Observability — minimal v1

**Decision:** Ship **standard infrastructure telemetry only** in v1.
**No dedicated query history table.** **No cost attribution rollups.**
Both are deferred to v2-or-later if real workflows demand them.

**v1 ships:**

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

**Why not a dedicated query history table in v1:**

Standard OpenTelemetry traces *are* a query history — every query is
a trace, with all the same information that would otherwise be in a
`_system.queries` row. Tracing UIs (Tempo, Jaeger, Honeycomb,
Datadog APM, etc.) already let operators filter slow traces, group
by attributes, see trends over time. A dedicated table adds
SQL-over-history as a convenience but does not add information.

What a dedicated table would *uniquely* enable — joining query
history with event data in SQL — is a real but rare workflow.
Deferred until a concrete use case justifies the build.

**Why not cost attribution in v1:**

Cost attribution depends on **trustworthy user identity**, which
requires the auth model (deferred to v2 per §1.6). Until then, the
`username` field is whatever the client claims. Attribution data
without trustworthy identity is misleading.

Cost attribution also is most valuable for **multi-tenant SaaS
deployments** where internal chargeback matters. For team-scoped
deployments (each team runs its own daemon set) it is overkill.

Adding cost attribution later is cheap once the trace data exists —
mostly aggregation views over already-collected spans plus an
identity-trust upgrade.

**What's added cheaply in v2 if demanded:**

- Query history as a queryable `_system.queries` dataset
  (subscribe to completed-query trace stream → write into a
  Tutankhamun dataset). ~1–2 weeks.
- Cost attribution rollups over trace data (once auth identity is
  trustworthy). ~1 week.

### 3.5 Web UI — minimal status page

**Decision:** Ship a **minimal read-only status page** on the daemon
in v1. No query execution surface, no dashboards, no auth surface.

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

---

## Section 3 is closed at the architectural level

All identified new-feature candidates have been decided:

- §3.1 Approximate aggregations — HLL + t-digest + theta with
  `approx_` prefix, query-time only in v1.
- §3.2 Query surface — DataFusion + FlightSQL + native Python client
  library; **no IQL** (no migration audience); server-side joins
  delivered as a side effect.
- §3.3 Incremental ingestion — progressive v1 (batch) → v2
  (single-writer streaming) → v3 (multi-writer) plan, with v1
  design disciplines committed now.
- §3.4 Observability — standard Prometheus / OTel / structured logs;
  no dedicated query history table, no cost attribution rollups in
  v1.
- §3.5 Web UI — minimal read-only status page; no query console.

Further new features would be additions beyond what has been
discussed; surface them as they emerge.
