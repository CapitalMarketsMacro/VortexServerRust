// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the Apache License 2.0.                                               ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cmake::Config;

/// The protobuf major version Conan provides, read from `conanfile.py`
/// (`protobuf/6.33.5` -> "33"). `protoc --version` reports the same number
/// (`libprotoc 33.5`), so this is what every protoc candidate is compared
/// against: generated `perspective.pb.h` hard-fails to compile against a
/// libprotobuf of a different version, so a stray PATH protoc must never win.
fn expected_protoc_major() -> Option<String> {
    let conanfile = fs::read_to_string("conanfile.py").ok()?;
    let re_start = conanfile.find("\"protobuf/")?;
    let rest = &conanfile[re_start + "\"protobuf/".len()..];
    // "6.33.5\")" -> skip the leading "6." -> "33"
    let rest = rest.strip_prefix("6.")?;
    let major: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if major.is_empty() { None } else { Some(major) }
}

/// Check that a protoc binary runs (pre-built protoc may need a newer glibc
/// on old Linux) AND reports the protobuf major version Conan provides.
fn protoc_works(path: &Path) -> bool {
    let Ok(out) = Command::new(path).arg("--version").output() else {
        return false;
    };
    if !out.status.success() {
        return false;
    }
    let version = String::from_utf8_lossy(&out.stdout);
    let reported = version
        .trim()
        .strip_prefix("libprotoc ")
        .map(|v| v.chars().take_while(|c| c.is_ascii_digit()).collect::<String>())
        .unwrap_or_default();
    match expected_protoc_major() {
        Some(expected) if reported != expected => {
            println!(
                "cargo:warning=Ignoring protoc {} ({}): Conan protobuf is {}.x",
                path.display(),
                version.trim(),
                expected
            );
            false
        }
        _ => true,
    }
}

/// `PSP_CONAN_NO_REMOTE=1` — strict offline build: no Conan remote, no protoc
/// download; anything missing from the local caches is a hard error.
fn strict_offline() -> bool {
    println!("cargo:rerun-if-env-changed=PSP_CONAN_NO_REMOTE");
    matches!(
        std::env::var("PSP_CONAN_NO_REMOTE").as_deref(),
        Ok("1") | Ok("true")
    )
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
        if strict_offline() {
            println!("cargo:warning=No usable protoc found and PSP_CONAN_NO_REMOTE is set");
        } else {
            println!("cargo:warning=No usable protoc found — CMake will download it");
        }
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
    // Conan's CMakeDeps never removes generator files for packages that left
    // the graph; start clean so the *.cmake scan in `link_conan_libraries`
    // and the `*-data.cmake` GLOB in CMakeLists.txt only ever see the
    // current graph. `conan install` writes generators only after the graph
    // resolved, so a failed stage leaves nothing behind.
    let _ = fs::remove_dir_all(&conan_output_dir);
    fs::create_dir_all(&conan_output_dir).ok();

    // Pin the exact, all-pre-built dependency graph (recipe revisions) so a
    // newer ConanCenter recipe revision can never silently change what gets
    // resolved. See CLAUDE.md, "Updating the lockfile".
    let lockfile = manifest_dir.join("conan.lock");

    // Two-stage install (see CLAUDE.md, "C++ dependencies: pre-built only"):
    //
    // 1. Hermetic: `--no-remote --build=never`. Resolves the whole graph from
    //    the local Conan cache with zero network I/O and never compiles
    //    anything. This is the steady state on every developer machine after
    //    the first build and on CI runners with a restored ~/.conan2, and it
    //    keeps builds working when ConanCenter is unreachable (e.g. behind a
    //    TLS-intercepting corporate proxy). Fails fast if any binary is
    //    missing — including tool_requires such as boost's `b2`, which Conan
    //    otherwise insists on *checking* against the remote even though the
    //    prebuilt boost never needs it.
    // 2. Fallback: remotes enabled, still `--build=never`: downloads the
    //    ConanCenter pre-built binaries the lockfile pins and fails loudly
    //    ("Missing binary") on a toolchain with no published binaries instead
    //    of silently source-building Arrow/boost/protobuf/openssl/thrift for
    //    30+ minutes. `PSP_CONAN_BUILD_MISSING=1` opts in to `--build=missing`
    //    for such toolchains.
    //
    // `PSP_CONAN_NO_REMOTE=1` forbids stage 2 entirely (strict offline builds).
    let strict = strict_offline();
    println!("cargo:rerun-if-env-changed=PSP_CONAN_BUILD_MISSING");
    let build_missing = matches!(
        std::env::var("PSP_CONAN_BUILD_MISSING").as_deref(),
        Ok("1") | Ok("true")
    );

    let run = |stage: &str, extra: &[&str]| -> std::process::ExitStatus {
        println!("cargo:warning=Running conan install ({stage}) with profile {profile} ...");
        let mut cmd = Command::new("conan");
        cmd.arg("install")
            .arg(manifest_dir)
            .arg("--output-folder")
            .arg(&conan_output_dir)
            .args(extra);

        if lockfile.exists() {
            cmd.arg("--lockfile").arg(&lockfile);
        } else {
            println!(
                "cargo:warning=conan.lock not found at {} — dependency revisions are NOT pinned",
                lockfile.display()
            );
        }

        if profile_path.exists() {
            cmd.arg("--profile:host").arg(&profile_path);
        } else {
            println!(
                "cargo:warning=Conan profile {} not found, using default profile",
                profile_path.display()
            );
        }

        cmd.status()
            .expect("Failed to run conan — is it installed?")
    };

    if run("cache-only", &["--no-remote", "--build=never"]).success() {
        println!("cargo:warning=Conan install succeeded from local cache (no network)");
        return conan_output_dir;
    }

    assert!(
        !strict,
        "Conan install failed in cache-only mode and PSP_CONAN_NO_REMOTE=1 is set. Populate \
         the Conan cache first: `conan install {} --profile:host {} --lockfile {} \
         --build=never`, or unset PSP_CONAN_NO_REMOTE",
        manifest_dir.display(),
        profile_path.display(),
        lockfile.display()
    );

    let build_flag = if build_missing { "--build=missing" } else { "--build=never" };
    println!(
        "cargo:warning=Local Conan cache incomplete; retrying with remotes enabled ({build_flag})"
    );
    let status = run("remote", &[build_flag]);
    assert!(
        status.success(),
        "Conan install failed (remote stage) with exit code {:?}; see Conan's output above. \
         `Missing binary` means the host compiler does not match the pre-built set (Linux \
         gcc 13 / Windows msvc 194 / macOS apple-clang 17): realign the toolchain or update \
         conan.lock, or set PSP_CONAN_BUILD_MISSING=1 to opt in to compiling the C++ deps \
         from source. `CERTIFICATE_VERIFY_FAILED` behind a TLS-intercepting proxy: set \
         `core.net.http:cacert_path` in ~/.conan2/global.conf (CLAUDE.md, 'Corporate TLS note')",
        status.code()
    );

    println!("cargo:warning=Conan install succeeded");
    conan_output_dir
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

    // Find protoc — normally the version-matched one from the Conan protobuf
    // package. If nothing usable is found, CMake's FindProtoc.cmake downloads
    // a release zip from GitHub, unless PSP_CONAN_NO_REMOTE=1, in which case
    // that download is forbidden and configure fails with a clear message.
    if let Some(protoc_path) = find_protoc_optional() {
        dst.define(
            "PSP_PROTOC_PATH",
            protoc_path
                .parent()
                .expect("protoc path returned root path or empty string"),
        );
    }
    if strict_offline() {
        dst.define("PSP_PROTOC_NO_DOWNLOAD", "ON");
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

    // Parallelism: cmake-rs already passes `--parallel $NUM_JOBS` (or hands
    // the build cargo's jobserver on Unix Makefiles), so nothing to add here.

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

    // Everything that feeds `conan install` or the CMake configure/build.
    for path in [
        "cpp/perspective",
        "cpp/protos",
        "cmake",
        "conanfile.py",
        "conan.lock",
        "conan/profiles",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
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
