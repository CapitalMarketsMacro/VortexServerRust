# Vendored Conan binary packages

Pre-built Conan **binary** packages, committed so the C++ build reuses them
instead of downloading or compiling from source — important on enterprise
laptops where source downloads (especially Arrow) are blocked or slow.

```
conan-cache/
  windows-x64/arrow.tgz      # `conan cache save` output for Arrow (MSVC 194)
  linux-x64/arrow.tgz        # per-package Arrow binary cache (Linux)
  linux-x64-static.tgz       # full linux-x64-static recipes + binaries snapshot
```

## How it's used

`perspective-server/build.rs` restores vendored Conan caches before
`conan install`:

- `restore_vendored_conan_binaries()` restores `vendor/conan-cache/<platform>/*.tgz`
  (e.g. `linux-x64/arrow.tgz`, `windows-x64/arrow.tgz`).
- On Linux x86_64, `maybe_restore_conan_cache()` also restores
  `vendor/conan-cache/linux-x64-static.tgz`, a full profile snapshot for
  offline builds.

Restore is **best-effort**: if a tarball is missing, is an unfetched Git LFS
pointer, fails to restore, or package IDs do not match, the build falls back to
vendored sources (`../conan-sources/`) or normal Conan downloads.

## package_id must match

A vendored binary is only reused when the resolved `package_id` matches exactly:
`os + arch + compiler + compiler.version + cppstd + runtime + every dependency's
package_id`.

`linux-x64-static.tgz` is keyed to:

| Setting | Value |
|---|---|
| `os` | Linux |
| `arch` | x86_64 |
| `compiler` | gcc |
| `compiler.version` | 15 |
| `compiler.libcxx` | libstdc++11 |
| `compiler.cppstd` | gnu17 |
| `build_type` | Release |

If the host toolchain differs (especially gcc major version), Conan may not
reuse vendored binaries and can fall back to build/download paths.

## Regenerating / adding a platform

Run on a machine of the target platform; commit the result (tracked via Git
LFS):

```bash
# Linux / macOS
./scripts/vendor-conan-arrow.sh
```
```powershell
# Windows
.\scripts/vendor-conan-arrow.ps1
```

For the **full Linux x86_64 static profile snapshot**:

```bash
cd Vortex/crates/perspective-server
conan install . --profile:host conan/profiles/linux-x64-static --build=missing
conan graph info . --profile:host conan/profiles/linux-x64-static --build=missing --format=json > /tmp/graph.json
conan list --graph=/tmp/graph.json --format=json > /tmp/pkglist.json
conan cache save --list=/tmp/pkglist.json --file=vendor/conan-cache/linux-x64-static.tgz
```

## Git LFS

These `.tgz` files are tracked via Git LFS (see root `.gitattributes`). Run
`git lfs install` and `git lfs pull` after cloning. Without git-lfs, files are
tiny pointer stubs and restore is skipped.
