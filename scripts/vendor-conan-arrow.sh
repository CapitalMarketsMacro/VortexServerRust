#!/usr/bin/env bash
# Regenerate the vendored Conan Arrow binary for the current platform.
#
# Builds (or reuses) the Arrow binary package for this platform's pinned Conan
# profile and saves it to vendor/conan-cache/<platform>/arrow.tgz, which the
# C++ build restores at build time to skip downloading/compiling Arrow.
# Run this on a machine of the target platform — e.g. on Linux/CI to populate
# linux-x64. The output is tracked via Git LFS; commit it after running.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_DIR="$ROOT/Vortex/crates/perspective-server"
cd "$SERVER_DIR"

case "$(uname -s)" in
  Linux)  PROFILE=linux-x64-static; PLAT=linux-x64 ;;
  Darwin)
    if [ "$(uname -m)" = arm64 ]; then PROFILE=macos-arm64-static; PLAT=macos-arm64;
    else PROFILE=macos-x64-static; PLAT=macos-x64; fi ;;
  *) echo "Unsupported platform $(uname -s); use the .ps1 variant on Windows." >&2; exit 1 ;;
esac

PROFILE_PATH="$SERVER_DIR/conan/profiles/$PROFILE"
VENDOR_DIR="$SERVER_DIR/vendor/conan-cache/$PLAT"
mkdir -p "$VENDOR_DIR"
TMP="$(mktemp -d)"

echo "==> Ensuring Arrow is built/cached for $PROFILE ..."
conan install . -pr:h "$PROFILE_PATH" --build=missing -of "$TMP"

echo "==> Resolving Arrow package_id ..."
PKGID="$(conan graph info . -pr:h "$PROFILE_PATH" --format=json | python3 -c '
import json, sys
g = json.load(sys.stdin)
for n in g["graph"]["nodes"].values():
    if n.get("name") == "arrow":
        print(n["package_id"]); break
')"
[ -n "$PKGID" ] || { echo "arrow package_id not found in conan graph" >&2; exit 1; }
echo "    arrow package_id = $PKGID"

OUT="$VENDOR_DIR/arrow.tgz"
echo "==> Saving to $OUT ..."
conan cache save "arrow/22.0.0:$PKGID" --file "$OUT"
echo "Done. Commit $OUT (tracked via Git LFS)."
