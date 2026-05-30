// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the Apache License 2.0.                                               ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cmake::Config;

/// Find the protoc binary. Checks in order:
/// 1. Conan output directory (version-matched protoc)
/// 2. PROTOC env var
/// 3. protobuf-src crate (if bundled-protoc feature enabled)
/// 4. System PATH
fn find_protoc() -> PathBuf {
    if let Some(p) = find_protoc_from_conan() {
        println!("cargo:warning=Using protoc from Conan: {}", p.display());
        return p;
    }

    if let Ok(protoc) = std::env::var("PROTOC") {
        let p = PathBuf::from(&protoc);
        if p.exists() {
            println!("cargo:warning=Using PROTOC from environment: {protoc}");
            return p;
        }
    }

    #[cfg(feature = "bundled-protoc")]
    {
        let p = protobuf_src::protoc();
        println!("cargo:warning=Using bundled protoc: {}", p.display());
        return p;
    }

    #[allow(unreachable_code)]
    {
        if let Ok(p) = which::which("protoc") {
            println!("cargo:warning=Using system protoc: {}", p.display());
            return p;
        }
        panic!(
            "protoc not found. Either:\n\
             - Set PROTOC env var to the protoc binary path\n\
             - Install protoc (e.g. via Conan, chocolatey, or apt)\n\
             - Enable the 'bundled-protoc' feature to build from source"
        );
    }
}

/// Check if a protoc binary actually works (not just exists).
/// On older Linux systems, pre-built protoc may require newer glibc.
fn protoc_works(path: &Path) -> bool {
    Command::new(path)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Find protoc optionally — returns None if not found or not working.
/// CMake's FindProtoc.cmake will download protoc if we don't provide it.
fn find_protoc_optional() -> Option<PathBuf> {
    if let Some(p) = find_protoc_from_conan() {
        if protoc_works(&p) {
            println!("cargo:warning=Using protoc from Conan: {}", p.display());
            return Some(p);
        }
        println!("cargo:warning=Conan protoc found but doesn't run (glibc mismatch?)");
    }

    if let Ok(protoc) = std::env::var("PROTOC") {
        let p = PathBuf::from(&protoc);
        if p.exists() && protoc_works(&p) {
            println!("cargo:warning=Using PROTOC from environment: {protoc}");
            return Some(p);
        }
    }

    #[cfg(feature = "bundled-protoc")]
    {
        let p = protobuf_src::protoc();
        println!("cargo:warning=Using bundled protoc: {}", p.display());
        return Some(p);
    }

    #[allow(unreachable_code)]
    {
        if let Ok(p) = which::which("protoc") {
            if protoc_works(&p) {
                println!("cargo:warning=Using system protoc: {}", p.display());
                return Some(p);
            }
        }
        println!("cargo:warning=No working protoc found — CMake will download it");
        None
    }
}

/// Search the Conan output directory for the protoc binary.
fn find_protoc_from_conan() -> Option<PathBuf> {
    let base = Path::new("conan_output");
    let candidates = [
        base.join("build").join("generators"),
        base.join("build").join("Release").join("generators"),
        base.join("build").join("release").join("generators"),
        base.to_path_buf(),
    ];
    let conan_output = match candidates.iter().find(|d| d.is_dir()) {
        Some(d) => d.clone(),
        None => return None,
    };

    let protoc_name = if cfg!(windows) { "protoc.exe" } else { "protoc" };

    // Parse conanbuildenv scripts for PATH additions
    if let Ok(entries) = fs::read_dir(&conan_output) {
        for entry in entries.flatten() {
            let path = entry.path();
            let fname = path.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
            let is_buildenv = fname.starts_with("conanbuildenv")
                && (fname.ends_with(".bat") || fname.ends_with(".sh") || fname.ends_with(".ps1"));
            if !is_buildenv {
                continue;
            }
            if let Ok(content) = fs::read_to_string(&path) {
                for line in content.lines() {
                    let paths = if line.contains("PATH=") || line.contains("PATH \"") {
                        line.split(&[';', ':', '"', '\''][..])
                            .filter(|p| Path::new(p).is_absolute())
                            .collect::<Vec<_>>()
                    } else {
                        continue;
                    };
                    for dir in paths {
                        let protoc = Path::new(dir).join(protoc_name);
                        if protoc.exists() {
                            return Some(protoc);
                        }
                    }
                }
            }
        }
    }

    // Parse CMakeDeps .cmake files for protobuf package paths
    if let Ok(entries) = fs::read_dir(&conan_output) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.extension().map_or(false, |e| e == "cmake") {
                continue;
            }
            let fname = path.file_name().map(|f| f.to_string_lossy().to_lowercase()).unwrap_or_default();
            if !fname.contains("protobuf") {
                continue;
            }
            if let Ok(content) = fs::read_to_string(&path) {
                for line in content.lines() {
                    if line.contains("PACKAGE_FOLDER") || line.contains("_ROOT_") {
                        for part in line.split('"') {
                            let candidate = Path::new(part);
                            if candidate.is_absolute() && candidate.is_dir() {
                                let protoc = candidate.join("bin").join(protoc_name);
                                if protoc.exists() {
                                    return Some(protoc);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    None
}

fn main() -> Result<(), std::io::Error> {
    if std::env::var("DOCS_RS").is_ok() {
        return Ok(());
    }

    if std::option_env!("PSP_DISABLE_CPP").is_none()
        && std::env::var("CARGO_FEATURE_DISABLE_CPP").is_err()
        && let Some(artifact_dir) = cmake_build()?
    {
        cmake_link_deps(&artifact_dir)?;
    }

    Ok(())
}

/// Returns the Conan profile name for the current target platform.
fn conan_profile() -> &'static str {
    if cfg!(target_os = "windows") {
        "windows-x64-static"
    } else if cfg!(target_os = "linux") {
        "linux-x64-static"
    } else if cfg!(target_os = "macos") {
        if cfg!(target_arch = "aarch64") {
            "macos-arm64-static"
        } else {
            "macos-x64-static"
        }
    } else {
        panic!("Unsupported target OS for Conan profile selection");
    }
}

/// Restore the vendored Conan binary-cache snapshot for offline Linux builds.
///
/// No-op unless the snapshot exists, is materialized (not an unfetched Git LFS
/// pointer), and the packages aren't already in the local cache. Any failure is
/// logged and swallowed — the subsequent `conan install` then falls back to the
/// network exactly as it would without a snapshot.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn maybe_restore_conan_cache(manifest_dir: &Path) {
    let snapshot = manifest_dir
        .join("vendor")
        .join("conan-cache")
        .join("linux-x64-static.tgz");
    if !snapshot.is_file() {
        return;
    }

    // A materialized snapshot is hundreds of MB; an un-smudged Git LFS pointer
    // is ~130 bytes. Never feed a pointer to `conan cache restore`.
    let len = fs::metadata(&snapshot).map(|m| m.len()).unwrap_or(0);
    if len < 4096 {
        println!(
            "cargo:warning=vendored Conan cache at {} looks like an unmaterialized \
             Git LFS pointer ({len} bytes) — run `git lfs pull`. Skipping restore.",
            snapshot.display()
        );
        return;
    }

    // Already populated? Avoid re-extracting hundreds of MB on every build.
    if conan_binary_cached("arrow/22.0.0") {
        return;
    }

    println!(
        "cargo:warning=Restoring vendored Conan binary cache (offline) from {} ...",
        snapshot.display()
    );
    match Command::new("conan")
        .arg("cache")
        .arg("restore")
        .arg(&snapshot)
        .status()
    {
        Ok(s) if s.success() => println!("cargo:warning=Conan cache restore succeeded"),
        Ok(s) => println!(
            "cargo:warning=conan cache restore exited with {:?}; continuing \
             (conan install will fall back to the network)",
            s.code()
        ),
        Err(e) => println!("cargo:warning=failed to run `conan cache restore`: {e}; continuing"),
    }
}

/// True if the local Conan cache already holds at least one *binary* package
/// for `reference` (e.g. `arrow/22.0.0`). Uses a text heuristic over
/// `conan list --format=json` to avoid pulling a JSON crate into build.rs:
/// a populated cache shows `"packages": { "<id>": ... }`, while an empty or
/// missing entry shows `"packages": {}` or an `"error"` field.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn conan_binary_cached(reference: &str) -> bool {
    let Ok(output) = Command::new("conan")
        .args(["list", &format!("{reference}:*"), "--format=json"])
        .output()
    else {
        return false;
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.contains("\"packages\":")
        && !stdout.contains("\"packages\": {}")
        && !stdout.contains("\"error\"")
}

/// Run `conan install` and return the path to the Conan output directory.
/// Panics if Conan is not available — Conan is required for this build.
fn conan_install(manifest_dir: &Path) -> PathBuf {
    let conanfile = manifest_dir.join("conanfile.py");
    assert!(
        conanfile.exists(),
        "conanfile.py not found at {}",
        conanfile.display()
    );

    assert!(
        which::which("conan").is_ok(),
        "Conan is required but not found in PATH. Install with: pip install conan"
    );

    // Ensure a default Conan profile exists (all host profiles inherit from it).
    // `--exist-ok` is a no-op if the profile already exists.
    let detect_status = Command::new("conan")
        .args(["profile", "detect", "--exist-ok"])
        .status()
        .expect("Failed to run conan profile detect");
    assert!(
        detect_status.success(),
        "conan profile detect failed with exit code {:?}",
        detect_status.code()
    );

    let profile = conan_profile();
    let profiles_dir = manifest_dir.join("conan").join("profiles");
    let profile_path = profiles_dir.join(profile);

    let conan_output_dir = manifest_dir.join("conan_output");
    fs::create_dir_all(&conan_output_dir).ok();

    // Restore any vendored Conan binary packages for this platform into the
    // local cache before install, so heavy deps (e.g. Arrow) come from a
    // committed tarball instead of being downloaded or compiled from source.
    // Best-effort: a package_id mismatch (e.g. a different compiler.version)
    // or absent files just leave the cache untouched, and `--build=missing`
    // falls back to the vendored source archive / a download.
    restore_vendored_conan_binaries(manifest_dir);

    println!("cargo:warning=Running conan install with profile {profile} ...");

    let mut cmd = Command::new("conan");
    cmd.arg("install")
        .arg(manifest_dir)
        .arg("--output-folder")
        .arg(&conan_output_dir)
        .arg("--build=missing");

    // Use vendored source archives (e.g. Arrow) to avoid downloading
    // from URLs that may be blocked by corporate firewalls.
    let vendor_sources = manifest_dir.join("vendor").join("conan-sources");
    if vendor_sources.is_dir() {
        // Get Conan home directory and write download cache config to global.conf
        if let Ok(output) = Command::new("conan").arg("config").arg("home").output() {
            let conan_home = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let global_conf = PathBuf::from(&conan_home).join("global.conf");
            let existing = fs::read_to_string(&global_conf).unwrap_or_default();
            if !existing.contains("core.sources:download_cache") {
                // Use forward slashes to avoid UNC path issues on Windows
                let vendor_path = vendor_sources.display().to_string().replace('\\', "/");
                let conf_line = format!("core.sources:download_cache={vendor_path}");
                let new_content = if existing.is_empty() {
                    conf_line
                } else {
                    format!("{}\n{}", existing.trim(), conf_line)
                };
                let _ = fs::write(&global_conf, new_content);
            }
        }
    }

    // ── Offline / enterprise: restore a vendored Conan *binary* cache ─────────
    // The source download_cache above only avoids re-downloading source
    // tarballs; Conan recipes and prebuilt binaries still come from
    // conancenter. On an air-gapped Linux x86_64 box, restore a committed
    // snapshot of the full linux-x64-static binary cache so the `conan install`
    // below finds every package already built — zero network access required.
    // See vendor/conan-cache/README.md for how the snapshot is produced and the
    // exact toolchain it's keyed to.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    maybe_restore_conan_cache(manifest_dir);

    if profile_path.exists() {
        cmd.arg("--profile:host").arg(&profile_path);
    } else {
        println!(
            "cargo:warning=Conan profile {} not found, using default profile",
            profile_path.display()
        );
    }

    let status = cmd
        .status()
        .expect("Failed to run conan — is it installed?");

    assert!(
        status.success(),
        "Conan install failed with exit code {:?}",
        status.code()
    );

    println!("cargo:warning=Conan install succeeded");
    conan_output_dir
}

/// Restore vendored Conan binary packages (committed under
/// `vendor/conan-cache/<platform>/*.tgz`) into the local Conan cache so the
/// build reuses them instead of downloading/compiling. Best-effort: missing
/// files, unfetched git-lfs pointers, or a failed restore all leave the cache
/// as-is so the normal `conan install --build=missing` path takes over.
fn restore_vendored_conan_binaries(manifest_dir: &Path) {
    let platform = if cfg!(target_os = "windows") {
        "windows-x64"
    } else if cfg!(target_os = "linux") {
        "linux-x64"
    } else if cfg!(target_os = "macos") {
        if cfg!(target_arch = "aarch64") {
            "macos-arm64"
        } else {
            "macos-x64"
        }
    } else {
        return;
    };

    let dir = manifest_dir.join("vendor").join("conan-cache").join(platform);
    let entries = match fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return, // no vendored binaries for this platform
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.extension().map_or(false, |e| e == "tgz") {
            continue;
        }

        // A git-lfs pointer is a few hundred bytes; the real package is tens of
        // MB. Restoring a pointer would fail, so skip and let the build fall
        // back to the vendored source / a download.
        if fs::metadata(&path).map(|m| m.len() < 4096).unwrap_or(true) {
            println!(
                "cargo:warning=Vendored Conan package {} looks like a git-lfs pointer \
                 (run `git lfs pull`); skipping restore",
                path.display()
            );
            continue;
        }

        println!("cargo:warning=Restoring vendored Conan package {}", path.display());
        match Command::new("conan").arg("cache").arg("restore").arg(&path).status() {
            Ok(s) if s.success() => {}
            Ok(s) => println!(
                "cargo:warning=conan cache restore exited {:?} for {}; continuing without it",
                s.code(),
                path.display()
            ),
            Err(e) => println!(
                "cargo:warning=could not run conan cache restore ({e}); continuing without it"
            ),
        }
    }
}

fn cmake_build() -> Result<Option<PathBuf>, std::io::Error> {
    let mut dst = Config::new("cpp/perspective");
    if let Some(cpp_build_dir) = std::option_env!("PSP_CPP_BUILD_DIR") {
        std::fs::create_dir_all(cpp_build_dir)?;
        dst.out_dir(cpp_build_dir);
    }

    // Run Conan install before finding protoc
    let manifest_dir = std::fs::canonicalize(".")
        .expect("Failed to canonicalize current directory");
    let conan_output = conan_install(&manifest_dir);

    let profile = std::env::var("PROFILE").unwrap();
    dst.always_configure(true);
    dst.define("CMAKE_BUILD_TYPE", profile.as_str());

    // Force Release config on MSVC to match Conan's CMakeDeps
    if cfg!(windows) {
        dst.profile("Release");
    }

    dst.define("ARROW_BUILD_EXAMPLES", "OFF");
    dst.define("RAPIDJSON_BUILD_EXAMPLES", "OFF");
    dst.define("ARROW_CXX_FLAGS_DEBUG", "-Wno-error");

    // Find protoc if available — if not, CMake's FindProtoc.cmake
    // will download it automatically from GitHub.
    if let Some(protoc_path) = find_protoc_optional() {
        dst.define(
            "PSP_PROTOC_PATH",
            protoc_path
                .parent()
                .expect("protoc path returned root path or empty string"),
        );
    }
    dst.define("CMAKE_COLOR_DIAGNOSTICS", "ON");
    dst.define(
        "PSP_PROTO_PATH",
        std::env::var("DEP_PERSPECTIVE_CLIENT_PROTO_PATH").unwrap(),
    );
    dst.env(
        "DEP_PERSPECTIVE_CLIENT_PROTO_PATH",
        std::env::var("DEP_PERSPECTIVE_CLIENT_PROTO_PATH").unwrap(),
    );

    // Prevent vcpkg from interfering
    dst.env("VCPKG_ROOT", "");
    dst.define("VCPKG_MANIFEST_MODE", "OFF");

    // Set up Conan toolchain — search multiple possible locations
    // (Conan puts generators in different subdirs depending on platform/version)
    let search_dirs = [
        conan_output.join("build").join("generators"),
        conan_output.join("build").join("Release").join("generators"),
        conan_output.join("build").join("release").join("generators"),
        conan_output.clone(),
    ];

    let toolchain_file = search_dirs
        .iter()
        .map(|d| d.join("conan_toolchain.cmake"))
        .find(|f| f.exists())
        .unwrap_or_else(|| {
            panic!(
                "conan_toolchain.cmake not found in any of: {:?}",
                search_dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>()
            )
        });

    println!(
        "cargo:warning=Using Conan toolchain at {}",
        toolchain_file.display()
    );

    if cfg!(windows) {
        dst.generator_toolset("v143");
        // Dynamic CRT (/MD) to match conancenter pre-built binaries.
        dst.static_crt(false);
    }

    dst.define("CMAKE_TOOLCHAIN_FILE", &toolchain_file);
    let prefix_path = toolchain_file.parent().unwrap();
    dst.define("CMAKE_PREFIX_PATH", prefix_path);

    // macOS cross-compilation
    if cfg!(target_os = "macos") {
        if let Ok(arch) = std::env::var("PSP_ARCH") {
            let toolchain = match arch.as_str() {
                "x86_64" => "./cmake/toolchains/darwin-x86_64.cmake",
                "aarch64" => "./cmake/toolchains/darwin-arm64.cmake",
                _ => panic!("Unknown PSP_ARCH value: {arch}"),
            };
            // Conan toolchain already set — this is handled by Conan profile
            let _ = toolchain;
        }
    }

    dst.define("PSP_WASM_BUILD", "0");
    dst.define("PSP_WASM_EXCEPTIONS", "0");

    if std::env::var("CARGO_FEATURE_EXTERNAL_CPP").is_err() {
        dst.env("PSP_DISABLE_CLANGD", "1");
    }

    if !cfg!(windows) {
        dst.build_arg(format!("-j{}", num_cpus::get()));
    }

    if let Ok(cmake_args) = std::env::var("CMAKE_ARGS") {
        println!("cargo:warning=Setting CMAKE_ARGS from environment {cmake_args:?}");
        for arg in shlex::Shlex::new(&cmake_args) {
            dst.configure_arg(arg);
        }
    }

    dst.build_target("psp");

    println!("cargo:warning=Building cmake {profile}");
    if !std::env::var("PSP_BUILD_VERBOSE").unwrap_or_default().is_empty() {
        dst.very_verbose(true);
    }

    let artifact_dir = dst.build();
    Ok(Some(artifact_dir))
}

fn cmake_link_deps(cmake_build_dir: &Path) -> Result<(), std::io::Error> {
    let build_dir = cmake_build_dir.join("build");
    let mut linked = std::collections::HashSet::new();

    // Link psp from its build dir
    link_archives_flat(&build_dir, &mut linked)?;

    // Link protos from its build dir
    let protos_dir = build_dir.join("protos-build");
    link_archives_flat(&protos_dir, &mut linked)?;

    // Link Conan-installed libraries
    let manifest_dir = std::fs::canonicalize(".")?;
    let base = manifest_dir.join("conan_output");
    let candidates = [
        base.join("build").join("generators"),
        base.join("build").join("Release").join("generators"),
        base.join("build").join("release").join("generators"),
        base.clone(),
    ];
    let conan_cmake_dir = candidates.iter().find(|d| d.is_dir()).cloned().unwrap_or(base);
    if conan_cmake_dir.exists() {
        link_conan_libraries(&conan_cmake_dir, &mut linked)?;
    }

    // Windows system libraries
    if cfg!(windows) {
        for lib in &["ole32", "shell32", "advapi32", "bcrypt", "ws2_32", "crypt32", "userenv"] {
            println!("cargo:rustc-link-lib=dylib={lib}");
        }
    }

    println!("cargo:rerun-if-changed=cpp/perspective");
    println!("cargo:rerun-if-changed=conanfile.py");
    println!("cargo:rerun-if-changed=vendor/conan-cache");
    Ok(())
}

/// Parse Conan-generated .cmake data files to find library directories and
/// link all static archives found there.
fn link_conan_libraries(
    conan_output: &Path,
    linked: &mut std::collections::HashSet<String>,
) -> Result<(), std::io::Error> {
    let mut package_folders: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut lib_dirs: Vec<PathBuf> = Vec::new();

    // First pass: collect all PACKAGE_FOLDER values
    for entry in fs::read_dir(conan_output)? {
        let path = entry?.path();
        if !path.extension().map_or(false, |e| e == "cmake") {
            continue;
        }
        if let Ok(content) = fs::read_to_string(&path) {
            for line in content.lines() {
                if line.contains("PACKAGE_FOLDER") && line.contains("set(") {
                    if let Some((var_name, value)) = parse_cmake_set(line) {
                        package_folders.insert(var_name, value);
                    }
                }
            }
        }
    }

    // Second pass: resolve LIB_DIRS using package folders
    for entry in fs::read_dir(conan_output)? {
        let path = entry?.path();
        if !path.extension().map_or(false, |e| e == "cmake") {
            continue;
        }
        if let Ok(content) = fs::read_to_string(&path) {
            for line in content.lines() {
                if !line.contains("_LIB_DIRS") || !line.contains("set(") {
                    continue;
                }
                if let Some((_var_name, value)) = parse_cmake_set(line) {
                    let resolved = resolve_cmake_vars(&value, &package_folders);
                    let candidate = Path::new(&resolved);
                    if candidate.is_absolute() && candidate.is_dir() {
                        lib_dirs.push(candidate.to_path_buf());
                    }
                }
            }
        }
    }

    lib_dirs.sort();
    lib_dirs.dedup();

    for dir in &lib_dirs {
        println!("cargo:warning=Linking Conan libs from: {}", dir.display());
        link_archives_flat(dir, linked)?;
    }

    Ok(())
}

fn parse_cmake_set(line: &str) -> Option<(String, String)> {
    let line = line.trim();
    let inner = line.strip_prefix("set(")?.strip_suffix(')')?;
    let space_pos = inner.find(|c: char| c == ' ' || c == '\t')?;
    let var_name = inner[..space_pos].to_string();
    let value_part = inner[space_pos..].trim();
    let value = value_part.trim_matches('"').to_string();
    Some((var_name, value))
}

fn resolve_cmake_vars(
    input: &str,
    vars: &std::collections::HashMap<String, String>,
) -> String {
    let mut result = input.to_string();
    for _ in 0..10 {
        let mut changed = false;
        if let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let var_name = &result[start + 2..start + end];
                if let Some(value) = vars.get(var_name) {
                    result = format!("{}{}{}", &result[..start], value, &result[start + end + 1..]);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    result
}

fn link_archives_flat(dir: &Path, linked: &mut std::collections::HashSet<String>) -> Result<(), std::io::Error> {
    if !dir.is_dir() {
        return Ok(());
    }

    let dirs_to_scan: Vec<PathBuf> = if cfg!(windows) {
        let mut dirs = vec![dir.to_path_buf()];
        for sub in &["MinSizeRel", "Release", "RelWithDebInfo"] {
            let p = dir.join(sub);
            if p.is_dir() {
                dirs.push(p);
            }
        }
        dirs
    } else {
        vec![dir.to_path_buf()]
    };

    for scan_dir in &dirs_to_scan {
        println!("cargo:rustc-link-search=native={}", scan_dir.display());
        for entry in fs::read_dir(scan_dir)? {
            let path = entry?.path();
            if path.is_dir() {
                continue;
            }
            if let Some(name) = archive_lib_name(&path) {
                if linked.insert(name.clone()) {
                    println!("cargo:rustc-link-lib=static={name}");
                }
            }
        }
    }
    Ok(())
}

fn archive_lib_name(path: &Path) -> Option<String> {
    let ext = path.extension()?.to_string_lossy();
    let stem = path.file_stem()?.to_string_lossy();

    let is_archive = (cfg!(windows) && ext == "lib" && stem != "perspective")
        || (!cfg!(windows) && ext == "a");

    if !is_archive {
        return None;
    }

    let name = if cfg!(windows) {
        stem.to_string()
    } else {
        stem.strip_prefix("lib").unwrap_or(&stem).to_string()
    };
    Some(name)
}
