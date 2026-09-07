# Perspective Native

Self-contained C++ engine + Rust bindings for [Perspective](https://perspective.finos.org/), using Conan for C++ dependency management.

## Prerequisites

- **Rust** (nightly) — installed via `rust-toolchain.toml`
- **CMake** 3.20+ (the Conan branch uses `cmake_path`)
- **C++ compiler** (MSVC on Windows, GCC/Clang on Linux/macOS)
- **Conan** 2.x — `pip install conan`

## Build

```bash
# Linux/macOS
./build.sh

# Windows
build.bat
```

The script will:
1. Check prerequisites and install Conan if needed
2. Download pre-built C++ dependencies via Conan (pinned by `crates/perspective-server/conan.lock`)
3. Build the C++ engine and Rust crates

First build takes ~5-10 min on the supported toolchains (gcc 13 / MSVC 2022; apple-clang 17 partially), where Conan downloads pre-built binaries rather than compiling them. On any other toolchain the build fails with `Missing binary` unless `PSP_CONAN_BUILD_MISSING=1` opts in to compiling the C++ deps from source (much slower). Subsequent builds reuse the cache and need no network at all.

## Usage

See `examples/axum-server/` for a working WebSocket server example.

```rust
use perspective::server::Server;
use perspective::client::{TableInitOptions, UpdateData};

let server = Server::new(None);
let client = server.new_local_client();
// CSV is compiled out of this build (the pre-built Arrow has no CSV module);
// use JSON rows / JSON columns / NDJSON / Arrow IPC instead.
let rows = r#"[{"name":"Alpha","value":100},{"name":"Beta","value":200}]"#.to_string();
let mut opts = TableInitOptions::default();
opts.set_name("my_table");
client.table(UpdateData::JsonRows(rows).into(), opts).await?;
```

## Project Structure

```
crates/
  perspective/           — Facade crate (axum-ws server)
  perspective-client/    — Protocol definitions + Arrow types
  perspective-server/    — C++ engine bridge (build.rs + CMake)
    cpp/perspective/     — C++ source code
    conanfile.py         — Conan dependency recipe
    conan/profiles/      — Platform-specific Conan profiles
examples/
  axum-server/           — WebSocket server example
```
