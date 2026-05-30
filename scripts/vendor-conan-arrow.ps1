<#
.SYNOPSIS  Regenerate the vendored Conan Arrow binary for the current platform.
.DESCRIPTION
  Builds (or reuses) the Arrow binary package for this platform's pinned Conan
  profile and saves it to vendor/conan-cache/<platform>/arrow.tgz, which the
  C++ build restores at build time to skip downloading/compiling Arrow.
  Run this on a machine of the target platform — e.g. on Windows to refresh
  windows-x64, or invoke the .sh variant on Linux/CI to populate linux-x64.
  The output is tracked via Git LFS; commit it after running.
.EXAMPLE  .\scripts\vendor-conan-arrow.ps1
#>
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ServerDir = Join-Path $Root 'Vortex\crates\perspective-server'
Set-Location $ServerDir

if ($env:OS -eq 'Windows_NT') {
  $ProfileName = 'windows-x64-static'; $Plat = 'windows-x64'
} else {
  throw "Run the .sh variant on Linux/macOS; this .ps1 targets Windows."
}

$ProfilePath = Join-Path $ServerDir "conan\profiles\$ProfileName"
$VendorDir = Join-Path $ServerDir "vendor\conan-cache\$Plat"
New-Item -ItemType Directory -Force $VendorDir | Out-Null
$Tmp = Join-Path ([System.IO.Path]::GetTempPath()) "vendor-arrow-$Plat"

Write-Host "==> Ensuring Arrow is built/cached for $ProfileName ..." -ForegroundColor Cyan
conan install . -pr:h $ProfilePath --build=missing -of $Tmp
if ($LASTEXITCODE -ne 0) { throw "conan install failed" }

Write-Host "==> Resolving Arrow package_id ..." -ForegroundColor Cyan
$graph = conan graph info . -pr:h $ProfilePath --format=json | ConvertFrom-Json
$node = $graph.graph.nodes.PSObject.Properties.Value |
  Where-Object { $_.name -eq 'arrow' } | Select-Object -First 1
if (-not $node) { throw "arrow node not found in conan graph" }
$PkgId = $node.package_id
Write-Host "    arrow package_id = $PkgId"

$Out = Join-Path $VendorDir 'arrow.tgz'
Write-Host "==> Saving to $Out ..." -ForegroundColor Cyan
conan cache save "arrow/22.0.0:$PkgId" --file $Out
if ($LASTEXITCODE -ne 0) { throw "conan cache save failed" }
Write-Host "Done. Commit $Out (tracked via Git LFS)." -ForegroundColor Green
