#!/usr/bin/env bash
# ============================================================
#  Perspective Native — Build Script
#
#  Usage:
#    ./build.sh              Build everything
#    ./build.sh --clean      Full clean before building
# ============================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
CONAN_DIR="$ROOT/crates/perspective-server"
FULL_CLEAN=0

for arg in "$@"; do
    case "$arg" in
        --clean|-c) FULL_CLEAN=1 ;;
    esac
done

echo
echo "========================================"
echo "  Perspective Native Build"
echo "========================================"
echo

# --- Clean ---
if [ "$FULL_CLEAN" -eq 1 ]; then
    echo "--- Cleaning ---"
    rm -rf "$ROOT/target" "$CONAN_DIR/conan_output"
    echo "  Done"
    echo
fi

# --- Prerequisites ---
echo "--- Checking prerequisites ---"
echo

command -v rustc &>/dev/null || { echo "[ERROR] rustc not found. Install from https://rustup.rs"; exit 1; }
echo "  [OK] $(rustc --version)"

command -v cmake &>/dev/null || { echo "[ERROR] cmake not found."; exit 1; }
echo "  [OK] $(cmake --version | head -1)"

if command -v g++ &>/dev/null; then
    echo "  [OK] $(g++ --version | head -1)"
elif command -v clang++ &>/dev/null; then
    echo "  [OK] $(clang++ --version | head -1)"
else
    echo "[ERROR] No C++ compiler found."
    exit 1
fi

# --- Conan ---
if ! command -v conan &>/dev/null; then
    echo "  [INFO] Installing Conan..."
    if command -v pipx &>/dev/null; then
        pipx install conan
    elif command -v pip3 &>/dev/null; then
        pip3 install --user conan
    elif command -v pip &>/dev/null; then
        pip install --user conan
    else
        echo "[ERROR] Cannot install Conan. Install manually: pip install conan"
        exit 1
    fi
    export PATH="$HOME/.local/bin:$PATH"
fi
echo "  [OK] $(conan --version)"

# Ensure Conan default profile exists
conan profile show &>/dev/null 2>&1 || conan profile detect

# --- Detect platform profile ---
OS="$(uname -s)"
ARCH="$(uname -m)"
case "$OS" in
    Linux*)  PLATFORM="linux" ;;
    Darwin*) PLATFORM="macos" ;;
    MINGW*|MSYS*|CYGWIN*) PLATFORM="windows" ;;
    *)       echo "[ERROR] Unsupported OS: $OS"; exit 1 ;;
esac
case "$ARCH" in
    x86_64|amd64) ARCH_TAG="x64" ;;
    aarch64|arm64) ARCH_TAG="arm64" ;;
    *)             echo "[ERROR] Unsupported arch: $ARCH"; exit 1 ;;
esac
PROFILE_NAME="${PLATFORM}-${ARCH_TAG}-static"
PROFILE_FILE="$CONAN_DIR/conan/profiles/$PROFILE_NAME"

echo
echo "--- Installing C++ dependencies (Conan) ---"
echo "  Profile: $PROFILE_NAME"
echo

CONAN_ARGS=(install "$CONAN_DIR" --output-folder "$CONAN_DIR/conan_output" --build=missing)
[ -f "$PROFILE_FILE" ] && CONAN_ARGS+=(--profile:host "$PROFILE_FILE")
VENDOR_SOURCES="$CONAN_DIR/vendor/conan-sources"
if [ -d "$VENDOR_SOURCES" ]; then
    CONAN_HOME=$(conan config home)
    GLOBAL_CONF="$CONAN_HOME/global.conf"
    if ! grep -q "core.sources:download_cache" "$GLOBAL_CONF" 2>/dev/null; then
        echo "core.sources:download_cache=$VENDOR_SOURCES" >> "$GLOBAL_CONF"
    fi
fi
conan "${CONAN_ARGS[@]}"

echo
echo "--- Generating protobuf bindings ---"
echo

mkdir -p crates/perspective-client/docs
[ -f crates/perspective-client/docs/expression_gen.md ] || touch crates/perspective-client/docs/expression_gen.md

if [ ! -f crates/perspective-client/src/rust/proto.rs ]; then
    cargo build -p perspective-client --features generate-proto,protobuf-src,omit_metadata
fi

echo
echo "--- Building ---"
echo

cargo build --release -p perspective --features axum-ws

echo
echo "--- Deploying to dist/ ---"
echo

DIST="$ROOT/dist"
rm -rf "$DIST"
mkdir -p "$DIST/cpp_cache/build" "$DIST/example/src"

# Cache pre-built C++ artifacts
for d in "$ROOT/target/release/build/perspective-server-"*/out/build; do
    [ -d "$d" ] || continue

    # psp + protos static libs
    find "$d" -maxdepth 2 \( -name "libpsp.a" -o -name "psp.lib" \) -exec cp {} "$DIST/cpp_cache/build/" \;
    if [ -d "$d/protos-build" ]; then
        mkdir -p "$DIST/cpp_cache/build/protos-build"
        find "$d/protos-build" -maxdepth 2 \( -name "libprotos.a" -o -name "protos.lib" \) -exec cp {} "$DIST/cpp_cache/build/protos-build/" \;
    fi

    # Release subdirectory (MSVC)
    if [ -d "$d/Release" ]; then
        mkdir -p "$DIST/cpp_cache/build/Release"
        cp "$d/Release"/*.lib "$DIST/cpp_cache/build/Release/" 2>/dev/null || true
    fi
    if [ -d "$d/protos-build/Release" ]; then
        mkdir -p "$DIST/cpp_cache/build/protos-build/Release"
        cp "$d/protos-build/Release"/*.lib "$DIST/cpp_cache/build/protos-build/Release/" 2>/dev/null || true
    fi

    break
done

# Copy crate sources for path deps
cp -r "$ROOT/crates" "$DIST/crates"
cp "$ROOT/Cargo.toml" "$DIST/Cargo.toml"

# Create env script
cat > "$DIST/env.sh" << 'ENVEOF'
#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PSP_CPP_BUILD_DIR="$SCRIPT_DIR/cpp_cache"
echo "[OK] PSP_CPP_BUILD_DIR=$PSP_CPP_BUILD_DIR"
ENVEOF
chmod +x "$DIST/env.sh"

cat > "$DIST/env.bat" << 'ENVEOF'
@echo off
set "PSP_CPP_BUILD_DIR=%~dp0cpp_cache"
echo [OK] PSP_CPP_BUILD_DIR=%PSP_CPP_BUILD_DIR%
ENVEOF

# Create example
cat > "$DIST/example/Cargo.toml" << 'EXEOF'
[package]
name = "my-perspective-app"
version = "0.1.0"
edition = "2024"

[dependencies]
perspective = { path = "../crates/perspective", features = ["axum-ws"] }
axum = { version = ">=0.8,<0.9", features = ["ws"] }
tokio = { version = "1", features = ["full"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[workspace]
members = []

[patch.crates-io]
perspective = { path = "../crates/perspective" }
perspective-client = { path = "../crates/perspective-client" }
perspective-server = { path = "../crates/perspective-server" }
EXEOF

cp "$ROOT/examples/axum-server/src/main.rs" "$DIST/example/src/main.rs"

echo
echo "========================================"
echo "  Build succeeded!"
echo "========================================"
echo
echo "  dist/                - Deployable package"
echo "  dist/cpp_cache/      - Pre-built C++ artifacts"
echo "  dist/crates/         - Rust source crates"
echo "  dist/example/        - Example project"
echo
echo "  To use in your project:"
echo "    1. source dist/env.sh  (or run dist\\env.bat)"
echo "    2. cd dist/example && cargo build --release"
echo
