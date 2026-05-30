# Vendored Conan binary cache — `linux-x64-static`

`linux-x64-static.tgz` is a snapshot of the **prebuilt C++ dependency
binaries** (Arrow, Boost, protobuf, OpenSSL, …) that the
`perspective-server` C++ build links against, exported with
`conan cache save`. It lets a fresh **air-gapped Linux x86_64** machine build
with **zero conancenter / network access**.

This complements the two other offline pieces:

| Vendored artifact | Covers |
|---|---|
| `../conan-sources/` (Arrow source `.tar.gz`) | Conan *source* downloads (only used when a package builds from source) |
| **`linux-x64-static.tgz` (this dir)** | Conan *recipes + prebuilt binaries* — the bulk of what `conan install` needs |
| `vendor/solclient/` (separate branch) | the Solace `libsolclient` C library |

> The source download-cache alone is **not** enough for an offline build:
> `conan install` still resolves recipes and binary packages from the
> conancenter remote. This snapshot supplies both, so nothing is fetched.

## Storage: Git LFS

The tarball is large (~288 MB) and tracked via **Git LFS** (see
`/.gitattributes`). After cloning on a machine that will build offline:

```bash
git lfs install
git lfs pull        # materialize linux-x64-static.tgz (else it's a ~130-byte pointer)
```

The build will detect an un-fetched pointer (file < 4 KB) and skip the restore
with a warning rather than feeding a pointer to Conan.

## How it's consumed (automatic)

Both `cargo build` (via `perspective-server/build.rs`) and the standalone
`Vortex/build.sh` restore this snapshot **before** running `conan install`,
gated to Linux x86_64. The restore is skipped when:

- the packages are already in the local Conan cache (so it never re-extracts
  ~288 MB on an incremental rebuild), or
- the tarball is an un-fetched Git LFS pointer.

On failure the build logs a warning and continues — `conan install` then falls
back to the network exactly as it would without the snapshot.

## Toolchain it's keyed to

A Conan binary package only matches when the **consuming toolchain matches the
one that built it**. This snapshot was produced with:

| Setting | Value |
|---|---|
| `os` | Linux |
| `arch` | x86_64 |
| `compiler` | gcc |
| `compiler.version` | 15 |
| `compiler.libcxx` | libstdc++11 |
| `compiler.cppstd` | gnu17 |
| `build_type` | Release |

On a host with a **different gcc major version** (or libcxx), the package IDs
won't match, Conan will try to rebuild, and (without network) the build fails.
In that case, regenerate the snapshot on a representative machine (below).

## Regenerating / refreshing the snapshot

From a machine with network access and the **target toolchain**:

```bash
cd Vortex/crates/perspective-server

# 1. Populate the local cache for the profile (builds/downloads everything).
conan install . \
  --profile:host conan/profiles/linux-x64-static \
  --build=missing

# 2. Capture exactly that profile's graph as a package list.
conan graph info . \
  --profile:host conan/profiles/linux-x64-static \
  --build=missing --format=json > /tmp/graph.json
conan list --graph=/tmp/graph.json --format=json > /tmp/pkglist.json

# 3. Export recipes + binaries into the vendored tarball.
conan cache save --list=/tmp/pkglist.json \
  --file=vendor/conan-cache/linux-x64-static.tgz

# 4. Commit (Git LFS picks it up via .gitattributes).
git add vendor/conan-cache/linux-x64-static.tgz
git commit -m "chore: refresh vendored linux-x64-static Conan cache"
```

To verify a snapshot restores cleanly offline before committing:

```bash
export CONAN_HOME=$(mktemp -d)
conan profile detect
conan cache restore vendor/conan-cache/linux-x64-static.tgz
conan install . --profile:host conan/profiles/linux-x64-static \
  --build=never --no-remote      # fails loudly if any binary is missing
```
