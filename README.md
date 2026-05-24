# Tutankhamun

A Rust analytics engine — successor to Indeed's archived
[Imhotep](https://github.com/indeedeng/imhotep).

| Concern | Name |
|---|---|
| Project / repo / Python package / import | `tutankhamun` |
| CLI binary | `t9n` (i18n-style: `t` + 9 letters + `n`) |
| Environment variable prefix | `TUT_` |

## Status

Early bootstrap. `t9n serve` starts the daemon — Tokio runtime, Rayon pool,
operations HTTP server with `/healthz` and `/readyz`. No data plane yet.
Following the v1 checklist in [`.docs/roadmap.md`](.docs/roadmap.md).

## Quick start

### Install Rust

If you don't have Rust:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
```

The installer creates `~/.cargo/`. Add it to your shell once:

```sh
echo '. "$HOME/.cargo/env"' >> ~/.zshrc
source ~/.zshrc
```

### Build and run

```sh
cargo build
cargo run --bin t9n -- --help
cargo run --bin t9n -- serve --help
cargo run --bin t9n -- serve
```

`t9n serve` listens on `0.0.0.0:8080` (operations HTTP) by default. Override
with `--ops-addr` / `--grpc-addr` flags or the matching `TUT_*` environment
variables; see `t9n serve --help`.

### Smoke test

```sh
cargo run --bin t9n -- serve --ops-addr 127.0.0.1:8080 &
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
kill %1   # SIGTERM triggers graceful drain
```

## Project layout

```
.
├── Cargo.toml                          workspace root
├── rust-toolchain.toml                 pinned to stable
├── crates/
│   ├── tutankhamun-server/             daemon binary (t9n)
│   │   └── src/
│   │       ├── main.rs                 entry point + subcommand dispatch
│   │       ├── config.rs               layered config (CLI > env > file > defaults)
│   │       ├── runtime.rs              Rayon pool + async-to-Rayon dispatch
│   │       ├── shutdown.rs             cooperative drain coordination
│   │       └── ops_http.rs             /healthz, /readyz, /status (later)
│   └── tutankhamun-client/             placeholder client library
└── .docs/
    ├── tutankhamun.md                  design log
    └── roadmap.md                      v1 implementation checklist
```

## Development

Useful commands:

```sh
cargo build                    # check it still compiles
cargo clippy --workspace --all-targets   # lints (pedantic profile enabled)
cargo fmt                      # format
cargo test --workspace         # tests
```

### VS Code

`.vscode/launch.json` ships two CodeLLDB debug profiles:

- **Debug t9n serve** — builds `t9n`, launches it under LLDB with
  `serve --ops-addr 127.0.0.1:8080` and `RUST_LOG=debug`.
- **Debug unit tests (tutankhamun-server)** — runs the server-crate test
  binary under the debugger.

`.vscode/extensions.json` recommends [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)
and [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb).

## Design

- [`.docs/tutankhamun.md`](.docs/tutankhamun.md) — design log: decisions,
  rationale, alternatives considered.
- [`.docs/roadmap.md`](.docs/roadmap.md) — v1 implementation checklist.

## License

[Apache-2.0](LICENSE).
