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

echo.
echo ========================================
echo   Build succeeded!
echo ========================================
echo.

endlocal
