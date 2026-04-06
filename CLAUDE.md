# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

**VortexServer** — an Axum-based WebSocket server application built on top of [Perspective](https://perspective.finos.org/), a C++ analytics engine with Rust bindings. The Perspective engine lives in `Vortex/` as a dependency; the main application is at the repo root.

## Repo Layout

```
src/main.rs              — VortexServer application (axum WebSocket server)
Cargo.toml               — Workspace root + vortex-server package
Vortex/                  — Perspective engine (C++ + Rust bindings)
  crates/
    perspective/         — Facade crate (re-exports client/server, Axum WS handler)
    perspective-client/  — Protocol definitions (protobuf), Arrow types, Client/Session/Table/View
    perspective-server/  — C++ engine bridge (FFI, build.rs + CMake + Conan)
  examples/axum-server/  — Original example server
  build.sh / build.bat   — Full C++ + Rust build scripts
```

## Build Commands

```bash
# Full C++ + Rust build (first run ~15-20 min due to Conan; cached after)
cd Vortex && ./build.sh      # Linux/macOS
cd Vortex && build.bat       # Windows

# Rust-only rebuild (after C++ is already built)
cargo build

# Run VortexServer
cargo run -p vortex-server

# Run the original example
cargo run -p perspective-axum-example

# Run tests
cargo test

# Run a single test
cargo test -p perspective -- concurrent_test

# Format / Lint
cargo fmt
cargo clippy
```

## Rust Toolchain

Nightly (`nightly-2026-01-01`) — required. Set in `rust-toolchain.toml`. The `rustfmt.toml` uses unstable features (import grouping, comment wrapping).

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

## C++ Build Chain

`Vortex/crates/perspective-server/build.rs` drives: Conan install → CMake configure → compile → link. Key details:

- Conan profiles in `conan/profiles/` (windows-x64-static, linux-x64-static, macos-{x64,arm64}-static)
- Dependencies defined in `conanfile.py`: Arrow 22, protobuf 6.33, boost 1.86, re2, abseil, rapidjson, etc.
- Protoc discovery: Conan output → `PROTOC` env var → bundled `protobuf-src` → system PATH
- Windows links: ole32, shell32, advapi32, bcrypt, ws2_32, crypt32, userenv

## Feature Flags

| Crate | Flag | Effect |
|---|---|---|
| `perspective` | `axum-ws` | Enables Axum WebSocket server + Tokio |
| `perspective` | `external-cpp` | Use externally-built C++ artifacts |
| `perspective-server` | `disable-cpp` | Skip C++ entirely (headless mode) |
| `perspective-server` | `external-cpp` | Skip C++ build, use pre-built libs |
| `perspective-server` | `bundled-protoc` | Build protoc from source |
| `perspective-client` | `sendable` | Removes `Send` restriction (WASM compat) |
| `perspective-client` | `generate-proto` | On-demand protobuf codegen |

## Key Patterns

- **Async runtime**: Tokio. Uses `async_lock::RwLock` (not `tokio::sync`) so crates work outside Tokio context.
- **Error types**: `ServerError = Box<dyn Error + Send + Sync>`, `ServerResult<T> = Result<T, ServerError>`. Client errors via `thiserror`.
- **Response batching**: C++ returns `ResponseBatch` containing multiple responses per call. `poll()` must be called after updates to flush.
- **Memory safety**: All FFI objects implement `Drop` calling `psp_free`. Dropping without `close()` logs an error via `tracing::error`.
- **Lazy init**: `OnceLock` for `Client` and `Session` initialization in `LocalClient`/`LocalSession`.
- **Session IDs**: `u32` client_id assigned per FFI session, tracked in server's session map.

## Build Profiles

Dev builds use `opt-level = "s"` and `panic = abort`. Release uses `opt-level = "z"`, LTO, single codegen unit, and symbol stripping — optimized aggressively for size.
