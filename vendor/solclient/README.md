# Vendored libsolclient (Linux x86_64)

`solace-rs-sys`'s `build.rs` normally downloads a ~12 MB `libsolclient`
tarball from GitHub on every fresh machine:

```
https://github.com/asimsedhain/solace-rs/releases/download/0.0.0.0/solclient_Linux26-x86_64_opt_7.26.1.8.tar.gz
```

Enterprise / air-gapped machines have no egress to `github.com`, so the
download fails and the build dies. To avoid that, the tarball is vendored
here and the build is pointed at it via `SOLCLIENT_LIB_PATH`, which makes
`build.rs` skip the download entirely and link the static archives directly.

## Contents

| Path | What |
|---|---|
| `solclient_Linux26-x86_64_opt_7.26.1.8.tar.gz` | The upstream tarball, byte-for-byte (full-fidelity archive). |
| `solclient-7.26.1.8/lib/` | Extracted static libs + license files. Pruned to what static linking needs. |

`solclient-7.26.1.8/lib/` keeps only the static archives `build.rs` links —
`libsolclient.a` (→ `libsolclient.a.7.26.1.8`), `libsolclientssl.a`,
`libssl.a`, `libcrypto.a` — plus `licenses.txt` / `README.openssl`. The
shared `.so*` libs, debug `_d` variants, and `include/` headers were dropped
(not used by the static-only link); recover them from the tarball if needed.

Note: `libsolclient.a` is a symlink to `libsolclient.a.7.26.1.8`; both are
committed. (Linux/macOS preserve symlinks; this set is Linux-only anyway.)

## How it's wired

`../../.cargo/config.toml` sets, repo-root-relative:

```toml
[env]
SOLCLIENT_LIB_PATH = { value = "vendor/solclient/solclient-7.26.1.8/lib", relative = true }
```

No env setup is needed — `cargo build` picks this up automatically. Because
`[env]` does not override an already-set variable, a developer can still
point at a different copy by exporting `SOLCLIENT_LIB_PATH` themselves.

## Platform note

These libs are **Linux x86_64 (glibc)** only. On macOS / Windows / musl the
vendored libs won't link. There, either:

- unset the var so `build.rs` downloads the right tarball:
  `SOLCLIENT_LIB_PATH= cargo build`, or
- vendor that platform's tarball the same way (see filenames in
  `solace-rs-sys`'s `build.rs`, e.g. `solclient_Darwin-universal2_opt_*`,
  `solclient_Linux_musl-x86_64_opt_*`).

## Refreshing (new libsolclient version)

1. Download the matching tarball from the release URL above.
2. `tar xzf <tarball>` here.
3. Optionally prune `lib/` to the four `.a` files + their real targets.
4. Update the version in `../../.cargo/config.toml` and this README.
