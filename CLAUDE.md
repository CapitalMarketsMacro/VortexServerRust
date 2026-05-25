# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

**VortexServer** — an Axum-based WebSocket server that ingests
real-time data from NATS / Solace / WebSocket transports and serves
it to browser clients via `<perspective-viewer>` over WebSocket. Built
on [Perspective](https://perspective.finos.org/) (a C++ analytics
engine with Rust bindings). The Perspective engine lives in `Vortex/`
as a dependency; the main application is at the repo root.

## Repo Layout

```
src/                          — VortexServer application
  main.rs                     — Entry point: load config, build registry,
                                spawn ingress + Axum WS server, wait for shutdown
  config.rs                   — JSON + env config (figment), per-transport blocks
  logging.rs                  — tracing-subscriber: pretty / json, optional file rotation
  supervisor.rs               — Panic-catching task runner with exp backoff
  tables.rs                   — TableSlot (one Perspective Server per table)
  transform.rs                — RowTransform: stringify nested cols + composite PK
  ingress/
    mod.rs                    — Fan out per-table consumers; transport dispatcher
    nats.rs                   — NATS Core + JetStream consumers
    solace.rs                 — Solace direct-topic ingress (libsolclient FFI bridge)
    websocket.rs              — Upstream WebSocket client consumer

Cargo.toml                    — Workspace root + vortex-server package
config.example.json           — End-to-end example: one table per transport
rust-toolchain.toml           — Pinned nightly-2026-01-01
docker-compose.yml            — Local Solace broker for testing
.gitignore                    — Excludes target/, simulators' .venv/, node_modules/

scripts/                      — Cross-platform operational scripts
  solace.sh / solace.ps1      — Manage docker Solace broker (start/stop/status/logs)
  sim-<transport>-<lang>.{sh,ps1}  — 12 simulator launchers (see simulators/)

simulators/                   — Synthetic data publishers (Python + Node)
  README.md                   — Full simulator docs
  nats/{python,nodejs}/       — NATS Core + JetStream publisher
  solace/{python,nodejs}/     — Solace REST messaging publisher
  websocket/{python,nodejs}/  — WebSocket server (vortex connects to it)

Vortex/                       — Perspective engine dependency (C++ + Rust bindings)
  crates/
    perspective/              — Facade crate (re-exports client/server, Axum WS handler)
    perspective-client/       — Protobuf protocol, Arrow types, Client/Session/Table/View
    perspective-server/       — C++ engine bridge (FFI, build.rs + CMake + Conan)
  examples/axum-server/       — Standalone example with simulated Treasury bond data
  build.sh / build.bat        — Full C++ + Rust build scripts (rarely needed)
```

## vortex-server Application Architecture

vortex-server is the main app at the repo root (package `vortex-server`,
binary `vortex-server`). It's layered like this:

```
main.rs
  ├─ AppConfig::load(path)          → src/config.rs
  ├─ logging::init(...)              → src/logging.rs
  ├─ TableRegistry::build(...)       → src/tables.rs
  │    one TableSlot per table = one Perspective Server + Client + lazy Table
  ├─ ingress::spawn_consumers(...)   → src/ingress/mod.rs
  │    dispatch by TableSource → nats::start / solace::start / websocket::start
  │    each one calls supervisor::supervise(...) for panic-restart
  ├─ build_router(registry, ws_path) → axum::Router with one WS route per table
  └─ axum::serve(...).with_graceful_shutdown(...)
```

**Per-table isolation is the load-bearing pattern.** Every table gets:
- its own `perspective::server::Server` (one C++ ProtoServer, one FFI mutex)
- its own ingress task (NATS sub / Solace session / WS client)
- its own [`supervise`] wrapper that catches panics and respawns with backoff

Result: high-rate updates, parse errors, or even C++ engine corruption
on one table cannot affect any other.

**Ingress submodules** all follow the same shape: `connect()` (or
context init) → `start()` that spawns a supervised task → `run_table()`
that subscribes / connects → `wait_for_first_message()` to seed the
table's schema → long-running consumer loop with shutdown selection.
Mirror this shape when adding a new transport.

**Solace specifics worth knowing about:**
- One libsolclient session per table — libsolclient's callback runs on
  its own C thread, bridged to a bounded tokio mpsc channel
- libsolclient handles steady-state reconnect (`reapply_subscriptions(true)`
  + `reconnect_retries(-1)`); we only wrap the *initial* connect with our
  own retry. A fatal `SessionEvent::DownError` returns an error so
  `supervise` rebuilds from scratch.
- Payloads from Solace's REST gateway (and any C client using
  `solClient_msg_setBinaryAttachmentString`) come wrapped in an SDT-string
  envelope. `extract_payload()` calls `solClient_msg_getBinaryAttachmentString`
  via `solace-rs-sys` to unwrap, with raw-binary fallback. Both code
  paths are exercised in production-style configs.

## Perspective Crate Architecture (Vortex/)

Three crates, layered bottom-up:

```
perspective-client   (protocol layer)
        ↑
perspective-server   (C++ FFI bridge)
        ↑
perspective          (facade + Axum WebSocket server)
```

**`perspective-client`** — Protocol definitions (protobuf via prost), Arrow types, and the `Client`/`Session`/`Table`/`View` abstractions. The `Session` trait is generic over error type, enabling pluggable transports. Source lives in `src/rust/`. Proto file: `perspective.proto`.

**`perspective-server`** — Bridges Rust to the C++ engine via 9 FFI functions in `src/ffi.rs` (`psp_new_server`, `psp_new_session`, `psp_handle_request`, `psp_poll`, etc.). All FFI types are `#[repr(C, packed)]` and manually impl `Send + Sync`. The `Server` type manages sessions via `HashMap<u32, SessionCallback>`. `build.rs` orchestrates Conan → CMake → linking.

**`perspective`** — Facade that re-exports `perspective_client` as `client` and `perspective_server` as `server`. Adds the Axum WebSocket handler (`src/axum.rs`) which runs a full-duplex select loop between socket recv and an mpsc channel.

## Build Toolchain

| Tool | Version | Why |
|---|---|---|
| Rust nightly-2026-01-01 | exact, pinned | `rust-toolchain.toml`; `rustfmt.toml` uses unstable features (import grouping, comment wrapping) |
| Conan 2.x | latest | C++ dependency manager for Vortex/ |
| CMake 3.20+ | latest | C++ build |
| C++17 compiler | latest | Xcode (macOS), GCC/Clang (Linux), MSVC 2022 (Windows) |

### C++ Build Chain (`Vortex/crates/perspective-server/build.rs`)

Conan install → CMake configure → compile → link. Key details:
- Conan profiles in `conan/profiles/` (`windows-x64-static`, `linux-x64-static`, `macos-{x64,arm64}-static`). On first build, `build.rs` auto-detects the right profile from `target_os`/`target_arch`.
- Dependencies in `conanfile.py`: Arrow 22, protobuf 6.33, boost 1.86, re2, abseil, rapidjson, etc.
- Protoc discovery order: Conan output → `PROTOC` env var → bundled `protobuf-src` → system PATH
- Windows-specific links from the C++ side: `ole32, shell32, advapi32, bcrypt, ws2_32, crypt32, userenv`

### Corporate networks: Conan TLS trust & pre-built binaries

In a corporate environment, **prefer downloading pre-built Conan binaries over
compiling from source** — source archives are often blocked or slow, and Arrow
is by far the biggest source build. Two things gate this:

**1. Conan must trust the corporate TLS root.** On a TLS-intercepting network
(e.g. Norton Web/Mail Shield, Zscaler), `conan install` fails against
`center2.conan.io` with `CERTIFICATE_VERIFY_FAILED ... unable to get local
issuer certificate`, so it can download *nothing* and falls back to the local
`~/.conan2` cache (compiling anything not already cached). Conan uses
python-requests, so point it at a CA bundle that includes the corporate root —
e.g. export the Windows Trusted Root store to a PEM, then:

```powershell
setx CONAN_CACERT_PATH C:\path\corp-roots.pem
# or add to ~/.conan2/global.conf:  core.net.http:cacert_path=C:/path/corp-roots.pem
# (REQUESTS_CA_BUNDLE also works)
```

A normal *incremental* `cargo build` is unaffected (deps are cached); this only
bites a clean/fresh build (e.g. `cargo clippy` on a fresh checkout).

**2. Arrow currently builds from source by design.** `conanfile.py`'s
`configure()` sets `arrow.parquet=False` + `arrow.with_thrift=False`. ConanCenter's
Arrow recipe **defaults** are `parquet=True` / `with_thrift=True`, so our options
produce a package_id ConanCenter never pre-built → `--build=missing` compiles
Arrow from source. This is a **deliberate corporate-env workaround**: it lets
Arrow build *without* pulling thrift from `archive.apache.org` (a commonly-blocked
source URL). To switch to a pre-built Arrow, first fix (1), then match the recipe
defaults and verify a binary exists with `conan install ... --build=never` (NOT
`=missing`) — only flip the override off if that download succeeds, otherwise a
default-options source fallback would hit the blocked thrift download.

### Solace (`solace-rs` / `solace-rs-sys` build.rs)

- On first build, downloads a pinned `libsolclient` tarball (v7.26.1.8) for the active platform from `github.com/asimsedhain/solace-rs/releases`. ~30 MB. Override with `SOLCLIENT_TARBALL_URL=...` or `SOLCLIENT_LIB_PATH=/path/to/lib` to use a local copy.
- **Offline / enterprise builds:** the Linux x86_64 libs are vendored in `vendor/solclient/` and wired up via `.cargo/config.toml` (`SOLCLIENT_LIB_PATH`, repo-root-relative), so `cargo build` never hits the network for them. See `vendor/solclient/README.md`. This applies to Linux x86_64 only — on macOS / Windows / musl, unset the var (`SOLCLIENT_LIB_PATH= cargo build`) so the correct platform tarball downloads, or vendor that platform's libs the same way.
- macOS additionally links `dylib=gssapi_krb5` (system Kerberos).
- Linux/macOS use static libs `solclient`, `solclientssl`, `ssl`, `crypto`.
- **Windows is not supported by `solace-rs-sys` 1.1** — its `build.rs` does `panic!("Windows currently not supported")` (despite defining a Windows tarball name it never uses). We therefore **target-gate the `solace-rs` / `solace-rs-sys` deps out of Windows** in the root `Cargo.toml` and gate the Solace ingress behind the `solace` feature (see below). On Windows, Solace-sourced tables are skipped at runtime with a clear error log; NATS + WebSocket work normally. A real Windows port would mean vendoring/patching `solace-rs-sys` to consume the existing `solclient_Win_vs2015_*.tar.gz` and adding the MSVC system libs — not done.

## Build Commands

```bash
# Standard incremental build (handles Conan + C++ on first run automatically)
cargo build

# Release build (optimized for size: LTO, strip)
cargo build --release

# Run vortex-server
cargo run -p vortex-server                              # config.json (default)
cargo run -p vortex-server -- --config /path/to.json

# Run the bundled example (simulated Treasury bond data)
cargo run -p perspective-axum-example

# Tests + lint
cargo test
cargo test -p perspective -- concurrent_test
cargo fmt
cargo clippy
```

The `Vortex/build.sh` / `Vortex/build.bat` wrappers exist for full
C++-first builds but are rarely needed — `cargo build` orchestrates
Conan + CMake on first invocation via build.rs.

## Local Testing Infrastructure

Two operational layers sit at the repo root.

**Brokers (Docker)** — `docker-compose.yml` defines two independent services:

- `solace` (solace/solace-pubsub-standard) managed by `scripts/solace.{sh,ps1}`. Implementation gotchas: `nofile` hard limit must stay at `1048576` (Solace POST violation 022 otherwise crashloops the broker) and the healthcheck uses `/SEMP/v2/monitor/msgVpns/default` (the more obvious `/__about/api` returns HTTP 400 unconditionally).
- `nats` (nats:alpine, JetStream enabled with persistent storage) managed by `scripts/nats.{sh,ps1}`. Uses alpine specifically because the default `nats:latest` is a scratch image with the binary only — alpine ships busybox-wget so the `/healthz` healthcheck works. Both scripts share the same surface: `start | stop [--wipe] | status | logs | restart`.

**Simulators** — `simulators/{nats,solace,websocket}/{python,nodejs}/` with paired-implementation publishers (or a WS server for the websocket case). Each is run via `scripts/sim-<transport>-<lang>.{sh,ps1}` which lazily creates `.venv/` (Python) or `node_modules/` (Node). Schemas match `config.example.json` so rows are immediately viewable in Perspective. See `simulators/README.md` for the full table.

When debugging an ingress problem, the workflow is: bring up the matching broker → run vortex-server with the affected table → run the matching simulator → watch `vortex-server` logs for `seeded` / `apply` activity. The NATS JetStream simulators auto-create the `ORDERS` stream on first publish so they bootstrap a brand-new broker without manual setup.

## Feature Flags

| Crate | Flag | Effect |
|---|---|---|
| `vortex-server` | `solace` | **Default-on.** Compiles in Solace ingress (`solace-rs` + `solace-rs-sys`). The deps are target-gated to non-Windows, and `build.rs` only emits the `solace_enabled` cfg when this feature is on AND the target isn't Windows — so on Windows the feature resolves to a no-op and Solace code is compiled out. Pass `--no-default-features` (Linux/macOS) to build without Solace. |
| `perspective` | `axum-ws` | Enables Axum WebSocket server + Tokio |
| `perspective` | `external-cpp` | Use externally-built C++ artifacts |
| `perspective-server` | `disable-cpp` | Skip C++ entirely (headless mode) |
| `perspective-server` | `external-cpp` | Skip C++ build, use pre-built libs |
| `perspective-server` | `bundled-protoc` | Build protoc from source |
| `perspective-client` | `sendable` | Removes `Send` restriction (WASM compat) |
| `perspective-client` | `generate-proto` | On-demand protobuf codegen |

## Key Patterns

- **Async runtime**: Tokio. `perspective-*` crates use `async_lock::RwLock` (not `tokio::sync`) so they work outside a Tokio context.
- **Error types**: `ServerError = Box<dyn Error + Send + Sync>`, `ServerResult<T> = Result<T, ServerError>`. Client errors via `thiserror`. App-level errors via `anyhow` for ergonomics.
- **Response batching**: C++ returns `ResponseBatch` containing multiple responses per call. `poll()` must be called after updates to flush.
- **Memory safety**: All FFI objects implement `Drop` calling `psp_free`. Dropping without `close()` logs an error via `tracing::error`.
- **Lazy init**: `OnceLock` for `Client` and `Session` initialization in `LocalClient`/`LocalSession`. `TableSlot` also lazy-creates its table from the first ingress message (so schema is inferred from real data).
- **Session IDs**: `u32` client_id assigned per FFI session, tracked in server's session map.
- **Supervisor convention**: a closure returning `()` to `supervise()` means "I stopped on purpose, don't restart." A panic means "transient failure, restart with backoff." Errors logged inside the closure and then returning `()` will *not* restart — match that convention to stop a misbehaving table cleanly, or panic to force a fresh attempt.

## Build Profiles

Dev builds use `opt-level = "s"` and `panic = abort`. Release uses `opt-level = "z"`, LTO, single codegen unit, and symbol stripping — optimized aggressively for size.

## Platform Verification Matrix

What's been validated end-to-end vs. what's pending:

| Concern | macOS ARM64 | macOS x86_64 | Linux x86_64 | Windows x86_64 |
|---|---|---|---|---|
| `cargo build` (incl. C++) | ✅ verified | inherited from arm64 | inherited (CI) | ✅ verified (Solace gated out) |
| Solace ingress end-to-end | ✅ verified (500-msg burst) | — | — | ⊘ N/A (`solace-rs-sys` won't build on Windows; compiled out) |
| NATS Core ingress end-to-end | ✅ verified | — | — | ✅ verified (table seeded) |
| NATS JetStream ingress end-to-end | ✅ verified | — | — | ✅ verified (stream-not-ready retry, then seeded) |
| WebSocket ingress end-to-end | ✅ verified | — | — | ✅ verified (table seeded) |
| Per-table WS serve (route upgrade) | ✅ verified | — | — | ✅ verified |
| `scripts/solace.{sh,ps1}` (broker mgmt) | ✅ verified (bash) | — | — | ✅ `.ps1` validated (status/dispatch; broker bring-up N/A) |
| `scripts/nats.{sh,ps1}` (broker mgmt) | ✅ verified (bash) | — | — | ✅ `.ps1` verified (start/status/stop --wipe) |
| `scripts/sim-nats-*.{sh,ps1}` | ✅ verified (Python + Node, Core + JetStream) | — | — | ✅ `.ps1` verified (Python + Node, Core + JetStream) |
| `scripts/sim-ws-*.{sh,ps1}` | ✅ verified (Python + Node) | — | — | ✅ `.ps1` verified (Python + Node) |
| `scripts/sim-solace-*.{sh,ps1}` | ✅ verified (Python + Node) | — | — | ⊘ broker N/A on Windows (ingress compiled out) |

Notes from the Windows bring-up (2026-05): Solace is compiled out (see the
Solace build section); a missing JetStream stream at startup now retries
with backoff instead of permanently killing the table; docker volume names
are pinned (`vortex-nats-storage` / `vortex-solace-storage`) so they don't
inherit the checkout-dir-derived Compose project prefix; the Python sim
launchers use `python -m pip` (the `pip.exe` shim can't self-upgrade on
Windows); and the **Node** sims need `npm install` to trust the OS cert
store on TLS-intercepting networks — see `simulators/README.md`
("`UNABLE_TO_VERIFY_LEAF_SIGNATURE`").

When testing on Windows, the high-confidence path is:
1. `cargo build -p vortex-server` — builds the C++ engine + NATS/WS; Solace is target-gated out (verifies the `solace_enabled` cfg gating compiles cleanly with the deps absent)
2. `.\scripts\nats.ps1 start` — verifies Docker Desktop + the NATS broker management script
3. `.\scripts\sim-nats-py.ps1 --count=10` and `.\scripts\sim-nats-js.ps1 --count=10` (then re-run with `--mode=jetstream`) — verifies the four NATS sim paths against the bundled broker
4. `.\scripts\sim-ws-py.ps1` (and `sim-ws-js.ps1`) in one window — verifies the WS server, which vortex-server connects to
5. Run `cargo run -p vortex-server -- --config config.example.json` and watch for `table seeded` on RatesMarketData (NATS core), Orders (JetStream), MarketTicks (WebSocket); Executions (Solace) logs a skip
