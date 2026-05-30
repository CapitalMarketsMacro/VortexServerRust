# Vendored Conan binary packages

Pre-built Conan **binary** packages, committed so the C++ build reuses them
instead of downloading or compiling from source — important on enterprise
laptops where source downloads (especially Arrow) are blocked or slow.

```
conan-cache/
  windows-x64/arrow.tgz   # `conan cache save` output for Arrow (MSVC 194)
  linux-x64/arrow.tgz     # populate on a Linux machine / CI (see below)
```

## How it's used

`perspective-server/build.rs` calls `restore_vendored_conan_binaries()` before
`conan install`. For the current platform it runs `conan cache restore` on each
`*.tgz` here, seeding the local Conan cache. `conan install --build=missing`
then finds the binary already cached and skips the download/compile.

It's **best-effort**: if a tarball is missing, is an unfetched Git LFS pointer,
or its `package_id` doesn't match (e.g. a different `compiler.version`), the
restore is skipped and the build falls back to the vendored Arrow **source**
(`../conan-sources/`, also committed) or a normal download.

## package_id must match

A vendored binary is only reused when the resolved `package_id` matches exactly:
`os + arch + compiler + compiler.version + cppstd + runtime + every dependency's
package_id`. That's why the Conan profiles pin `compiler.version` (Windows → MSVC
`194`, matching the dep versions ConanCenter ships pre-built). Change a pinned
compiler version → regenerate the tarball.

## Regenerating / adding a platform

Run on a machine of the target platform; commit the result (tracked via Git LFS):

```bash
# Linux / macOS
./scripts/vendor-conan-arrow.sh
```
```powershell
# Windows
.\scripts\vendor-conan-arrow.ps1
```

These build (or reuse) Arrow for the platform's pinned profile, resolve its
`package_id`, and `conan cache save` it to the right subfolder here.

## Git LFS

These `.tgz` files are tracked via Git LFS (see root `.gitattributes`). Clone
with `git lfs install` already configured, or run `git lfs pull` after cloning.
Without git-lfs the files are tiny pointer stubs and the build cleanly falls
back to source/download.
