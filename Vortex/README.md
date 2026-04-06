# Perspective Native

Self-contained C++ engine + Rust bindings for [Perspective](https://perspective.finos.org/), using Conan for C++ dependency management.

## Prerequisites

- **Rust** (nightly) — installed via `rust-toolchain.toml`
- **CMake** 3.18+
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
2. Download and build C++ dependencies via Conan
3. Build the C++ engine and Rust crates

First build takes ~15-20 min (Conan compiles dependencies). Subsequent builds reuse the cache.

## Usage

See `examples/axum-server/` for a working WebSocket server example.

```rust
use perspective::server::Server;
use perspective::client::{TableInitOptions, UpdateData};

let server = Server::new(None);
let client = server.new_local_client();
let csv = "name,value\nAlpha,100\nBeta,200".to_string();
let mut opts = TableInitOptions::default();
opts.set_name("my_table");
client.table(UpdateData::Csv(csv).into(), opts).await?;
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
