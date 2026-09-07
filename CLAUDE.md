# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

**VortexServer** — an Axum-based WebSocket server that ingests
real-time data from NATS / Solace / WebSocket transports and serves
it to browser clients via `<perspective-viewer>` over WebSocket. Built
on [Perspective](https://perspective.finos.org/) (a C++ analytics
engine with Rust bindings). The Perspective engine lives in `Vortex/`
as a dependency; the main application is at the repo root.

The vendored engine is **Perspective v5.3.1** (`perspective`,
`perspective-client`, `perspective-server` crates, taken from the crates.io
tarballs) plus a small, documented set of local patches — see
"Perspective fork: local patches & upgrade recipe" below before touching
anything under `Vortex/crates/`.

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
tests/ws_roundtrip.rs         — Perspective Client ⇄ Axum WS route round-trip test
examples/ws_probe.rs          — Probe a RUNNING server: hosted tables, row count,
                                schema of one table over its WS endpoint

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
      conanfile.py / conan.lock / conan/profiles/  — the C++ dependency graph
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

**`perspective-server`** — Bridges Rust to the C++ engine via 10 FFI functions in `src/ffi.rs` (`psp_new_server`, `psp_new_session`, `psp_handle_request`, `psp_poll`, etc.). All FFI types are `#[repr(C, packed)]` and manually impl `Send + Sync`. The `Server` type manages sessions via `HashMap<u32, SessionCallback>`. `build.rs` orchestrates Conan → CMake → linking.

**`perspective`** — Facade that re-exports `perspective_client` as `client` and `perspective_server` as `server`. Adds the Axum WebSocket handler (`src/axum.rs`) which runs a full-duplex select loop between socket recv and an mpsc channel.

## Perspective fork: local patches & upgrade recipe

`Vortex/crates/*` is upstream Perspective **v5.3.1** from the crates.io
tarballs (`https://static.crates.io/crates/<name>/<name>-<ver>.crate`), not
a git subtree. Upstream builds its C++ deps from source with CMake
ExternalProject; this repo replaces that with Conan pre-built binaries. The
complete list of local modifications (everything else is byte-for-byte
upstream) is:

**`perspective-server`**
- `Cargo.toml` — `default = []` (upstream defaults to the `python` feature,
  which turns on `PSP_PYTHON_BUILD` + `PSP_PARALLEL_FOR`), no
  `wasm-exceptions`, new `bundled-protoc` feature, `protobuf-src` made
  optional, `which` build-dep, path dep on `perspective-client`, Conan files
  in `include`.
- `build.rs` — whole-file replacement: two-stage `conan install`, protoc
  discovery (Conan → `PROTOC` → bundled → PATH; every candidate must report
  the protobuf major version pinned in `conanfile.py`), links every static
  archive from the Conan lib dirs, Windows system libs.
- `cpp/perspective/CMakeLists.txt` — `PSP_USE_CONAN` branch (toolchain
  detection, vcpkg integration disabled, dynamic CRT under Conan on Windows,
  `find_package(... CONFIG)` block, `PSP_CONAN_DEFINITIONS`, Conan link
  targets). The ExternalProject branch is kept but its templates are gone.
- `cpp/protos/CMakeLists.txt` — Conan branch (protobuf/abseil include + lib
  dirs from the CMakeDeps data files).
- `cmake/modules/FindProtoc.cmake` — default protoc 33.5, `PSP_PROTOC_PATH`
  searched with `NO_DEFAULT_PATH` (a version-matched Conan protoc beats a
  stray one on PATH), macOS arm64 download name.
- Removed: `cmake/*.txt.in`, `cmake/re2/`, `cpp/perspective/env.js`
  (ExternalProject / WASM only).
- C++ source (3 files): `arrow_csv.cpp` + `view.cpp` gate the Arrow CSV
  reader/writer behind `PSP_ENABLE_CSV` (see "CSV support" under Key
  Patterns); inside that gate `arrow_csv.cpp` also includes
  `<arrow/buffer.h>` and builds the `BufferReader` from
  `arrow::Buffer::FromString(...)` because Arrow 22 dropped the
  `string_view` constructor upstream (Arrow 18) relies on;
  `computed_function.cpp` uses `std::string(result)` instead of
  `re2::StringPiece::ToString()` (re2 ≥ 2023 aliases `StringPiece` to
  `absl::string_view`).
- Rust (`src/ffi.rs`, `src/local_session.rs`, `src/server.rs`) — every call
  that takes the C++ server pointer (`ffi::Server::{new_session,
  handle_request, poll, close_session}`) is serialized through an
  `async_lock::Mutex` (see Key Patterns), so those four methods are `async`
  and their callers `.await`.

**`perspective-client`** — unmodified except `Cargo.toml` drops
`package.json` from `include` (file deleted). `src/rust/proto.rs` is the
pre-generated one from the tarball, so no protoc is needed on the Rust side.

**`perspective`** — unmodified except path deps in `Cargo.toml` and
`tests/concurrent_test.rs` (JSON rows instead of CSV; `TableInitOptions`
built via `Default`, upstream's struct literal is stale).

**Conan** — `conanfile.py`, `conan.lock`, `conan/profiles/*` are entirely
local. Dependency versions differ from upstream's ExternalProject pins on
purpose (Arrow 22 vs 18, re2 2025 vs 2021, ...) because those are what
ConanCenter ships pre-built. When upstream bumps a pin (check
`cmake/*.txt.in` in the new tarball), bump the Conan requirement — v5.3.1
needed `exprtk/0.0.3` for `vector_access_runtime_check`.

**Upgrade recipe** (what was done for 4.3.0 / client 4.4.0 → 5.3.1):
1. Download + extract the three tarballs for the target version into a
   scratch dir. Also keep the *current* upstream version's tarballs.
2. `perspective-client`, `perspective`: delete and copy in wholesale, then
   re-apply the one-line `Cargo.toml` tweaks above (use `Cargo.toml.orig`
   from the tarball, it keeps the comments).
3. `perspective-server`: copy in `cpp/` and `cmake/` wholesale, delete the
   files listed under "Removed", copy back the local `FindProtoc.cmake` and
   `cpp/protos/CMakeLists.txt`, then 3-way merge `cpp/perspective/CMakeLists.txt`
   (`git merge-file`, or a scratch git repo with base/local/upstream
   branches — only the Conan hunks conflict). Keep `src/`, `build.rs`,
   `Cargo.toml` (re-apply the feature edits onto the new `Cargo.toml.orig`),
   and the Conan files.
4. Re-apply the three C++ source patches (grep for `PSP_ENABLE_CSV` and
   `ToString()`), then `cargo build -p vortex-server` and fix what the
   compiler reports. Expect only dependency-API drift: Arrow, re2, exprtk.
   Note `PSP_ENABLE_CSV` is never defined, so a botched CSV hunk is compiled
   out silently — diff `arrow_csv.cpp` / `view.cpp` against the tarball.
5. If a Conan requirement changes, update the lockfile *incrementally* (see
   "Updating the lockfile") and verify every profile still resolves
   pre-built.
6. `cargo test -p perspective --features axum-ws` and
   `cargo test -p vortex-server --test ws_roundtrip`, then the end-to-end
   checklist in "Platform Verification Matrix".

## Build Toolchain

| Tool | Version | Why |
|---|---|---|
| Rust nightly-2026-01-01 | exact, pinned | `rust-toolchain.toml`; `rustfmt.toml` uses unstable features (import grouping, comment wrapping). Upstream 5.3.1 pins nightly-2026-06-01 but its crates build fine on this older nightly, so the enterprise toolchain pin is unchanged |
| Conan 2.x | latest | C++ dependency manager for Vortex/ |
| CMake 3.20+ | latest | C++ build |
| C++17 compiler | latest | Xcode (macOS), GCC/Clang (Linux), MSVC 2022 (Windows) |

### C++ Build Chain (`Vortex/crates/perspective-server/build.rs`)

Conan install → CMake configure → compile → link. Key details:
- Conan profiles in `conan/profiles/` (`windows-x64-static`, `linux-x64-static`, `macos-{x64,arm64}-static`). On first build, `build.rs` auto-detects the right profile from `target_os`/`target_arch`.
- Dependencies in `conanfile.py`: Arrow 22, protobuf 6.33, boost 1.90, re2, abseil, rapidjson, date, tsl-*, exprtk. **All resolve to pre-built ConanCenter binaries — nothing compiles from source** (see below).
- Protoc discovery order: Conan output → `PROTOC` env var → bundled `protobuf-src` → system PATH
- Windows-specific links from the C++ side: `ole32, shell32, advapi32, bcrypt, ws2_32, crypt32, userenv`

### C++ dependencies: pre-built only (Conan + lockfile)

Every C++ dependency downloads as a **pre-built binary** from ConanCenter —
the build never compiles Arrow (or anything else) from source. Two pieces make
this reliable and reproducible:

**1. `conanfile.py` uses Arrow's ConanCenter default options.** ConanCenter only
publishes a pre-built `arrow/22.0.0` binary for its *default* option set
(`parquet=True`, `with_thrift=True`); thrift itself is also pre-built, so enabling
it never reaches `archive.apache.org`. `boost` is pinned to **1.90.0** to match the
boost version Arrow's prebuilt was linked against — a different boost major.minor
changes Arrow's `package_id` and loses the prebuilt match. (Perspective uses boost
header-only, so the version isn't otherwise constrained.) There is **no
`configure()` override** — adding one (e.g. `parquet=False`) would force a source
build, which is exactly what we removed.

**2. `conan.lock` pins the exact, drift-proof graph.** `Vortex/crates/perspective-server/conan.lock`
freezes every recipe revision + package_id for a graph that is 100% pre-built on the
supported profiles. `build.rs` / `build.sh` / `build.bat` pass `--lockfile`, so a
*newer* ConanCenter recipe revision (e.g. a fresh `xsimd` rev published after Arrow's
binary was built) can never silently flip a dependency back to a source build. CI's
"Verify C++ deps are pre-built" step runs `conan install … --build=never` to enforce
this — it fails loudly on any mismatch instead of compiling Arrow for 30+ minutes.

**3. `build.rs` runs `conan install` in two stages.** Stage 1 is hermetic:
`--no-remote --build=never`, i.e. resolve the entire graph from `~/.conan2`
with zero network I/O and never compile anything. That is the steady state
on every developer machine after the first build and on CI with a restored
Conan cache, and it keeps builds working when ConanCenter is unreachable
(TLS-intercepting proxies, air-gapped runners). It also sidesteps a Conan
quirk: with remotes enabled Conan insists on *checking* the remote for the
binaries of skippable `tool_requires` (boost's `b2`, `cmake`, `nasm`, ...)
even though the pre-built boost never needs them, so a remote outage
breaks an otherwise fully cached build. Stage 2 (only if stage 1 fails)
enables remotes but stays `--build=never`: it downloads the pre-built
binaries the lockfile pins and fails loudly (`Missing binary: arrow/...`)
on a toolchain with no published binaries, instead of silently compiling
Arrow/boost/protobuf/openssl/thrift for 30+ minutes. Set
`PSP_CONAN_BUILD_MISSING=1` to opt in to `--build=missing` on such a
toolchain. `PSP_CONAN_NO_REMOTE=1` forbids stage 2 altogether (strict
offline builds; it also forbids FindProtoc.cmake's GitHub download of a
protoc zip, which is only attempted when the Conan protobuf package's
protoc does not run). `build.rs` deletes `conan_output/` before every
install so generator files of dependencies that left the graph are never
linked, and re-runs on changes to `conanfile.py`, `conan.lock`,
`conan/profiles`, `cmake/`, `cpp/`. Populate a cache for offline use from
the repo root with:

```bash
conan install Vortex/crates/perspective-server \
  --profile:host Vortex/crates/perspective-server/conan/profiles/<profile> \
  --lockfile Vortex/crates/perspective-server/conan.lock --build=never
```

**Pre-built binaries are toolchain-specific.** The published binaries exist for
**Linux gcc 13 (libstdc++11)** and **Windows msvc 194 (VS 2022)** — `cppstd=gnu17`/`17`,
`Release`, static — for every dependency in the lock. **macOS apple-clang 17**
coverage is partial: as of 2026-09 ConanCenter has no apple-clang 17 binaries for
protobuf, abseil, re2 and libbacktrace, so a Mac build needs
`PSP_CONAN_BUILD_MISSING=1` and compiles those four from source. A package_id is
keyed on the compiler *major* version, so building with e.g. gcc 11 loses the
match; `build.rs` then fails with `Missing binary` unless `PSP_CONAN_BUILD_MISSING=1`
opts in to a (slow) source build. CI therefore pins gcc 13 on Linux (ubuntu-24.04 +
explicit `gcc-13`) and relies on msvc 194 from the windows-2022 image, and runs the
actual `cargo build` with `PSP_CONAN_NO_REMOTE=1` after the Verify step warmed
the cache. Note the lockfile pins recipe revisions, not package revisions —
Conan 2 lockfiles never do — so the compiler pin is what keeps the package_ids
stable.

**Updating the lockfile.** Prefer an *incremental* update over regenerating
from scratch: a full regeneration re-resolves every recipe to its newest
ConanCenter revision, which is exactly the drift the lockfile exists to
prevent. `conan lock create --lockfile conan.lock` is always partial:
entries already in the lockfile are kept as-is and only requirements missing
from it are resolved and added (`--lockfile-partial` is accepted but is a
no-op for `lock create`). Run it once per target profile (each adds that
platform's tool_requires), then delete the superseded entry by hand (the
union lockfile keeps old versions until you do):

```bash
cd Vortex/crates/perspective-server
for p in linux-x64-static windows-x64-static macos-arm64-static macos-x64-static; do
  conan lock create . --profile:all conan/profiles/$p \
    --lockfile conan.lock --lockfile-out conan.lock
done
# remove the line for the old version (e.g. "exprtk/0.0.2#...") from conan.lock
# verify nothing would build from source for the enterprise targets:
conan graph info . --profile:all conan/profiles/linux-x64-static   --lockfile conan.lock --format=json
conan graph info . --profile:all conan/profiles/windows-x64-static --lockfile conan.lock --format=json
```

Every `graph.nodes[*].binary` must be `Cache`, `Download` or `Skip` — never
`Build` or `Missing`. Note the repo profiles start with `include(default)`,
and a bare `default` resolves to `~/.conan2/profiles/default` (the *host*
machine's compiler), so evaluating the Linux profile from a Windows box
needs a flat scratch profile with the Linux settings (gcc 13, libstdc++11,
gnu17) written out in full. A from-scratch regeneration is only for a
deliberate "refresh everything": delete `conan.lock`, run the *first*
profile with only `--lockfile-out` (an explicit `--lockfile` pointing at a
missing file is a hard error), then accumulate the rest:

```bash
cd Vortex/crates/perspective-server
rm -f conan.lock
conan lock create . --profile:all conan/profiles/linux-x64-static --lockfile-out conan.lock
for p in windows-x64-static macos-arm64-static macos-x64-static; do
  conan lock create . --profile:all conan/profiles/$p --lockfile conan.lock --lockfile-out conan.lock
done
```

**Corporate TLS note.** On a TLS-intercepting network (Zscaler, Norton), `conan
install` fails against `center2.conan.io` with `CERTIFICATE_VERIFY_FAILED` and can
download *nothing*. Conan uses python-requests with its own CA bundle (it does
not consult the Windows/macOS trust store, unlike `curl`/`git`), so point it at a
bundle that includes the interception root: `core.net.http:cacert_path=/path/bundle.pem`
in `~/.conan2/global.conf`, or per command `conan <cmd> ... -cc core.net.http:cacert_path=/path/bundle.pem`.
Conan 2 does **not** honor the Conan 1 `CONAN_CACERT_PATH` env var. The bundle
must contain the *currently served* root — Norton rotates its "Web/Mail Shield
Root" CA, and a bundle built for the old one fails with the same error. Dump the
chain the machine actually sees (PowerShell: open an `SslStream` to
`center2.conan.io:443` with a validation callback that records
`$chain.ChainElements`) and append those certificates to `certifi`'s
`cacert.pem`. Thanks to the two-stage install this only matters for a cold
cache or a lockfile change; a warm cache builds with no network at all.

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
cargo test -p perspective --features axum-ws          # engine: two clients, on_update over a LocalSession
cargo test -p vortex-server --test ws_roundtrip       # Perspective Client over a real Axum WebSocket

# Probe a running server the way <perspective-viewer> would (no browser needed):
cargo run --example ws_probe -- ws://127.0.0.1:4000/ws/Orders Orders --watch 5
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

When debugging an ingress problem, the workflow is: bring up the matching broker → run vortex-server with the affected table → run the matching simulator → watch `vortex-server` logs for `seeded` / `apply` activity, then `cargo run --example ws_probe -- ws://127.0.0.1:4000/ws/<Table> <Table> --watch 5` to read the live row count and schema over the real WebSocket path. If port 4222 is taken by another project's NATS container, run a throwaway broker on other ports (`docker run -d --name vortex-nats-test -p 14222:4222 -p 18222:8222 nats:alpine -js -m 8222`) and point `transports.nats.url` and the simulators' `--url` at it; `scripts/nats.ps1` hard-codes 4222. The NATS JetStream simulators auto-create the `ORDERS` stream on first publish so they bootstrap a brand-new broker without manual setup.

## Feature Flags

| Crate | Flag | Effect |
|---|---|---|
| `vortex-server` | `solace` | **Default-on.** Compiles in Solace ingress (`solace-rs` + `solace-rs-sys`). The deps are target-gated to non-Windows, and `build.rs` only emits the `solace_enabled` cfg when this feature is on AND the target isn't Windows — so on Windows the feature resolves to a no-op and Solace code is compiled out. Pass `--no-default-features` (Linux/macOS) to build without Solace. |
| `perspective` | `axum-ws` | Enables Axum WebSocket server + Tokio |
| `perspective` | `external-cpp` | Use externally-built C++ artifacts |
| `perspective-server` | `disable-cpp` | Skip C++ entirely (headless mode) |
| `perspective-server` | `external-cpp` | Skip C++ build, use pre-built libs |
| `perspective-server` | `bundled-protoc` | Off by default. Builds protoc from source via crates.io `protobuf-src` 2.1.1 instead of using the Conan-provided one (upstream patches `protobuf-src` to a GitHub fork for Windows/MSVC support; that patch is deliberately absent so cold builds never clone from GitHub — on Windows use the Conan protoc). Upstream's `python` (default there) and `wasm-exceptions` features were removed from this fork. |
| `perspective-client` | `sendable` | Removes `Send` restriction (WASM compat) |
| `perspective-client` | `generate-proto` | On-demand protobuf codegen |

## Key Patterns

- **Async runtime**: Tokio. `perspective-*` crates use `async_lock::RwLock` (not `tokio::sync`) so they work outside a Tokio context.
- **Error types**: `ServerError = Box<dyn Error + Send + Sync>`, `ServerResult<T> = Result<T, ServerError>`. Client errors via `thiserror`. App-level errors via `anyhow` for ergonomics.
- **Response batching**: C++ returns `ResponseBatch` containing multiple responses per call. `poll()` must be called after updates to flush.
- **Memory safety**: All FFI objects implement `Drop` calling `psp_free`. Dropping without `close()` logs an error via `tracing::error`.
- **Lazy init**: `OnceLock` for `Client` and `Session` initialization in `LocalClient`/`LocalSession`. `TableSlot` also lazy-creates its table from the first ingress message (so schema is inferred from real data).
- **Session IDs**: `u32` client_id assigned per FFI session, tracked in server's session map.
- **`TableInitOptions` grows fields** (5.x added `page_to_disk`, `list_flatten`): always build it with `TableInitOptions::default()` + `set_name()` / field assignment, never a struct literal.
- **CSV support is compiled out.** The ConanCenter pre-built Arrow has `with_csv=False`, so `arrow_csv.cpp` / `view.cpp` gate the CSV reader/writer behind `PSP_ENABLE_CSV` (never defined). `UpdateData::Csv` and `View::to_csv` come back as engine errors; use `JsonRows` / `JsonColumns` / `Ndjson` / `Arrow`. Turning CSV on (`arrow/*:with_csv=True` in `conanfile.py`) changes Arrow's package_id and forces a from-source Arrow build — don't.
- **Nested JSON columns**: every table is created with `list_flatten = stringify` (config `tables[].list_flatten`, default `stringify`), so an array in a column not listed in `stringify_columns` is stored as its JSON text — the pre-5.x behaviour, no row multiplication. Perspective 5.x's own default (`zip`) would silently expand such rows; `zip` / `cartesian` are opt-in per table. Nested *objects* still require `stringify_columns` (the engine rejects them).
- **Static tables cannot have an `index`**: a source-less table is seeded from `[]` (empty schema), so the index column cannot exist; `create_static_tables` rejects the config with a clear error, like it does for `composite_index`.
- **Supervisor convention**: a closure returning `()` to `supervise()` means "I stopped on purpose, don't restart." A panic means "transient failure, restart with backoff." Errors logged inside the closure and then returning `()` will *not* restart — match that convention to stop a misbehaving table cleanly, or panic to force a fresh attempt.

## Build Profiles

Dev builds use `opt-level = "s"` and `panic = abort`. Release uses `opt-level = "z"`, LTO, single codegen unit, and symbol stripping — optimized aggressively for size.

## Platform Verification Matrix

What's been validated end-to-end vs. what's pending:

| Concern | macOS ARM64 | macOS x86_64 | Linux x86_64 | Windows x86_64 |
|---|---|---|---|---|
| `cargo build` (incl. C++) | ✅ verified | inherited from arm64 | inherited (CI) | ✅ verified (Solace gated out) |
| Perspective **5.3.1** engine build (Conan, all pre-built, hermetic stage) | — | — | lock evaluated from a Windows host (`graph info`): all pre-built, boost reports `Invalid` there (host-evaluation artifact; CI Verify step is the ground truth) | ✅ verified (2026-09) |
| `tests/ws_roundtrip.rs` + `perspective` concurrent test on 5.3.1 | — | — | — | ✅ verified (2026-09) |
| Solace ingress end-to-end | ✅ verified (500-msg burst) | — | — | ⊘ N/A (`solace-rs-sys` won't build on Windows; compiled out) |
| NATS Core ingress end-to-end | ✅ verified | — | — | ✅ verified (table seeded; re-verified on 5.3.1, 2026-09: Python + Node sims, `ws_probe` reads 15 composite-key rows) |
| NATS JetStream ingress end-to-end | ✅ verified | — | — | ✅ verified (stream-not-ready retry, then seeded; re-verified on 5.3.1, 2026-09: 25 indexed rows) |
| WebSocket ingress end-to-end | ✅ verified | — | — | ✅ verified (table seeded; re-verified on 5.3.1, 2026-09: `ws_probe --watch` sees rows growing at the sim rate) |
| Per-table WS serve (route upgrade) | ✅ verified | — | — | ✅ verified (5.3.1: `ws_probe` opens each table over `/ws/<Table>`) |
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
