@echo off
setlocal enabledelayedexpansion

echo.
echo ========================================
echo   Perspective Native Build
echo ========================================
echo.

set "ROOT=%~dp0"
set "CONAN_DIR=%ROOT%crates\perspective-server"

:: --- Clean ---
if "%~1"=="--clean" (
    echo --- Cleaning ---
    if exist "%ROOT%target" rmdir /s /q "%ROOT%target"
    if exist "%CONAN_DIR%\conan_output" rmdir /s /q "%CONAN_DIR%\conan_output"
    echo   Done
    echo.
)

:: --- Prerequisites ---
echo --- Checking prerequisites ---
echo.

where rustc >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] rustc not found. Install from https://rustup.rs
    exit /b 1
)
for /f "tokens=*" %%i in ('rustc --version') do echo   [OK] %%i

where cmake >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] cmake not found.
    exit /b 1
)
for /f "tokens=*" %%i in ('cmake --version') do (
    echo   [OK] %%i
    goto :cmake_ok
)
:cmake_ok

:: --- Conan ---
where conan >nul 2>&1
if %errorlevel% neq 0 (
    echo   [INFO] Installing Conan...
    pip install conan
    if %errorlevel% neq 0 (
        echo [ERROR] Cannot install Conan.
        exit /b 1
    )
)
for /f "tokens=*" %%i in ('conan --version') do echo   [OK] %%i

conan profile show >nul 2>&1
if %errorlevel% neq 0 conan profile detect

:: --- Conan install ---
echo.
echo --- Installing C++ dependencies (Conan) ---
echo.

set "PROFILE_FILE=%CONAN_DIR%\conan\profiles\windows-x64-static"
set "CONAN_OUTPUT=%CONAN_DIR%\conan_output"
if not exist "%CONAN_OUTPUT%" mkdir "%CONAN_OUTPUT%"

set "VENDOR_SOURCES=%CONAN_DIR%\vendor\conan-sources"
if exist "%VENDOR_SOURCES%" (
    for /f "tokens=*" %%h in ('conan config home') do set "CONAN_HOME=%%h"
    findstr /c:"core.sources:download_cache" "!CONAN_HOME!\global.conf" >nul 2>&1
    if !errorlevel! neq 0 echo core.sources:download_cache=%VENDOR_SOURCES%>> "!CONAN_HOME!\global.conf"
)

if exist "%PROFILE_FILE%" (
    conan install "%CONAN_DIR%" --output-folder "%CONAN_OUTPUT%" --build=missing --profile:host "%PROFILE_FILE%"
) else (
    conan install "%CONAN_DIR%" --output-folder "%CONAN_OUTPUT%" --build=missing
)

if %errorlevel% neq 0 (
    echo [ERROR] Conan install failed.
    exit /b 1
)

:: --- Proto bindings ---
echo.
echo --- Generating protobuf bindings ---
echo.

if not exist "crates\perspective-client\docs" mkdir "crates\perspective-client\docs"
if not exist "crates\perspective-client\docs\expression_gen.md" (
    echo. > "crates\perspective-client\docs\expression_gen.md"
)

if not exist "crates\perspective-client\src\rust\proto.rs" (
    cargo build -p perspective-client --features generate-proto,protobuf-src,omit_metadata
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to generate proto bindings.
        exit /b 1
    )
)

:: --- Build ---
echo.
echo --- Building ---
echo.

cargo build --release -p perspective --features axum-ws
if %errorlevel% neq 0 (
    echo [ERROR] Build failed.
    exit /b 1
)

:: --- Deploy to dist ---
echo.
echo --- Deploying to dist ---
echo.

set "DIST=%ROOT%dist"
if exist "%DIST%" rmdir /s /q "%DIST%"
mkdir "%DIST%\cpp_cache\build" "%DIST%\example\src"

:: Cache pre-built C++ artifacts
for /d %%d in ("%ROOT%target\release\build\perspective-server-*") do (
    if exist "%%d\out\build" (
        for /r "%%d\out\build" %%f in (psp.lib) do (
            copy "%%f" "%DIST%\cpp_cache\build\" >nul 2>nul
        )
        if exist "%%d\out\build\protos-build" (
            mkdir "%DIST%\cpp_cache\build\protos-build" 2>nul
            for /r "%%d\out\build\protos-build" %%f in (protos.lib) do (
                copy "%%f" "%DIST%\cpp_cache\build\protos-build\" >nul 2>nul
            )
        )
        if exist "%%d\out\build\Release" (
            mkdir "%DIST%\cpp_cache\build\Release" 2>nul
            copy "%%d\out\build\Release\*.lib" "%DIST%\cpp_cache\build\Release\" >nul 2>nul
        )
        if exist "%%d\out\build\protos-build\Release" (
            mkdir "%DIST%\cpp_cache\build\protos-build\Release" 2>nul
            copy "%%d\out\build\protos-build\Release\*.lib" "%DIST%\cpp_cache\build\protos-build\Release\" >nul 2>nul
        )
    )
)

:: Copy crate sources
xcopy /s /e /q /i "%ROOT%crates" "%DIST%\crates" >nul

:: Copy workspace Cargo.toml
copy "%ROOT%Cargo.toml" "%DIST%\Cargo.toml" >nul

:: Create env.bat
> "%DIST%\env.bat" (
    echo @echo off
    echo set "PSP_CPP_BUILD_DIR=%%~dp0cpp_cache"
    echo echo [OK] PSP_CPP_BUILD_DIR=%%PSP_CPP_BUILD_DIR%%
)

:: Create example Cargo.toml
> "%DIST%\example\Cargo.toml" (
    echo [package]
    echo name = "my-perspective-app"
    echo version = "0.1.0"
    echo edition = "2024"
    echo.
    echo [dependencies]
    echo perspective = { path = "../crates/perspective", features = ["axum-ws"] }
    echo axum = { version = "^0.8", features = ["ws"] }
    echo tokio = { version = "1", features = ["full"] }
    echo tracing = "0.1"
    echo tracing-subscriber = { version = "0.3", features = ["env-filter"] }
    echo.
    echo [workspace]
    echo members = []
    echo.
    echo [patch.crates-io]
    echo perspective = { path = "../crates/perspective" }
    echo perspective-client = { path = "../crates/perspective-client" }
    echo perspective-server = { path = "../crates/perspective-server" }
)

:: Copy example main.rs
copy "%ROOT%examples\axum-server\src\main.rs" "%DIST%\example\src\main.rs" >nul

echo.
echo ========================================
echo   Build succeeded!
echo ========================================
echo.
echo   dist\              - Deployable package
echo   dist\cpp_cache\    - Pre-built C++ artifacts
echo   dist\crates\       - Rust source crates
echo   dist\example\      - Example project
echo.
echo   To use in your project:
echo     1. Run dist\env.bat
echo     2. cd dist\example ^&^& cargo build --release
echo.

endlocal
