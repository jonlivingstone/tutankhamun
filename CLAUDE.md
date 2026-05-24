# Claude Code instructions — Tutankhamun

Project-specific guidance for Claude Code sessions in this repo. Self-contained;
read this and the `.docs/` set before any non-trivial change.

## Project in one paragraph

Tutankhamun is a Rust analytics engine — successor to Indeed's archived Imhotep.
The daemon binary is `t9n`; the Python package and Rust workspace are
`tutankhamun`. Environment variables use the `TUT_` prefix. The design intent
lives in `.docs/tutankhamun.md` (decisions + rationale) and the v1 build plan
lives in `.docs/roadmap.md` (a trackable checklist).

## Naming

| Concern | Name |
|---|---|
| Project / repo / Python package / import | `tutankhamun` |
| CLI binary | `t9n` |
| Env var prefix | `TUT_` (all env vars listed in `crates/tutankhamun-server/src/config.rs::env_vars`) |
| Thread name prefixes | `tk-tokio` (tokio workers), `tk-rayon-<n>` (rayon workers) |

## Strict rules

### Git

- **Never run `git commit`, `git push`, or any history-changing git operation
  without an explicit per-occasion instruction from the user.** "Finish X" is
  not permission to commit. Every commit needs its own "commit this" from the
  user.
- Never include `Co-Authored-By` lines or other AI attribution in commit
  messages or PR descriptions.
- When the user does ask for a commit, follow the protocol in the global
  system prompt (stage specific files; HEREDOC message; verify with
  `git status` after).

### Design discipline (load-bearing, see `tutankhamun.md` §1.3)

The binary must remain **deployment-agnostic** — runnable on K8s, VMs,
Nomad, ECS, bare metal. Do not regress any of these:

- **Stateless binary.** All persistent state in object storage; local
  storage is a rebuildable cache.
- **Layered config.** CLI > env > file > defaults. Plumbed through
  `clap` + `figment` in `config.rs`. New flags go through the same path;
  add the env var name to the `env_vars` module.
- **Standard credential provider chains.** No hardcoded credentials.
- **Graceful shutdown.** SIGTERM → drain → exit, gated by `ShutdownHandle`.
  Any long-running task that the daemon owns must hold a clone of the
  handle and yield on `.shutdown_requested()`.
- **Health endpoints on a separate ops HTTP port** (default 8080), distinct
  from the gRPC port. `/healthz` (liveness), `/readyz` (readiness),
  `/metrics` and `/status` land later.
- **Daemons never call each other on the data plane.** Inherited from
  Imhotep and preserved.

### Engine invariants (load-bearing, see `tutankhamun.md` §3.3)

These exist so that v2 streaming / v3 multi-writer drop in cheaply:

- `Shard` is a trait, not a concrete struct.
- `ShardSource` is a trait (where shards come from is pluggable).
- Time-range is first-class shard metadata (`Shard::time_range()`).
- Session API does not leak shard types — clients see datasets and time
  ranges, never "shard kind."
- Sessions are routed by an opaque `(session_token, daemon_address)` pair
  (default), or by a session-ID gRPC metadata header (proxy mode).

### Concurrency

- **Tokio for I/O, Rayon for CPU-bound compute.** Two bounded pools.
  CPU work dispatches to Rayon via `runtime::spawn_cpu`.
- Do not put CPU-bound loops on Tokio's main runtime.
- Both pools are bounded. Never reintroduce unbounded cached executors
  (Imhotep had five of these; it was a known anti-pattern).

### Storage format

- Forward columns: **uncompressed single-batch Arrow IPC**. mmap-friendly
  zero-copy reads as `&[i64]` via `bytemuck`. **Never write multi-batch
  forward-column files for hot data — the hot path silently degrades.**
- Inverted index: Roaring bitmaps (`roaring`) + FST term dictionaries
  (`fst`).
- `metadata.json` per shard carries Arrow schema, numDocs, time range,
  format version, content hashes.

## Useful commands

```sh
cargo build
cargo clippy --workspace --all-targets     # pedantic profile is on
cargo fmt
cargo fmt --check                          # CI-equivalent
cargo test --workspace
cargo run --bin t9n -- serve --help
```

Smoke-test the daemon:

```sh
cargo run --bin t9n -- serve --ops-addr 127.0.0.1:18080 &
curl -s http://127.0.0.1:18080/healthz
curl -s http://127.0.0.1:18080/readyz
kill -TERM %1
```

## Working style

- **Problem before options.** For architecture decisions with multiple
  options, explain *what is being decided and why it matters* before
  presenting the menu. The user has asked for this framing explicitly;
  jumping straight to a 4-option menu without context wastes their time.
- **Verify before claiming done.** `cargo build`, `cargo clippy`,
  `cargo fmt --check`, and (where applicable) a smoke run must pass
  before declaring a checkbox item complete. Treat compile success as
  evidence; treat "I wrote it" as not-yet-evidence.
- **Tick `.docs/roadmap.md` as items ship.** Items have design-doc
  cross-references; check them before implementing to make sure you're
  building what was decided.
- **Don't add features, refactor, or introduce abstractions beyond what
  the task requires.** Three similar lines is better than a premature
  abstraction. Same applies to comments — only WHY, never WHAT or
  narration.

## Pointers

- [`.docs/tutankhamun.md`](.docs/tutankhamun.md) — design log
- [`.docs/roadmap.md`](.docs/roadmap.md) — v1 implementation checklist
- [`README.md`](README.md) — user-facing quick start
- [`../imhotep/docs/modernization/`](../imhotep/docs/modernization/) —
  reference docs for the predecessor system, useful for understanding what
  Tutankhamun is preserving vs. modernizing
