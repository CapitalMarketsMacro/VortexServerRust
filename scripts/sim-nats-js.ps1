<#
.SYNOPSIS  Run the NATS simulator (Node.js) on Windows.
.DESCRIPTION First run installs deps via npm. Forwards all remaining args.
.EXAMPLE     .\scripts\sim-nats-js.ps1
.EXAMPLE     .\scripts\sim-nats-js.ps1 --mode=jetstream
#>
$ErrorActionPreference = 'Stop'
$SimDir = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..')).Path 'simulators\nats\nodejs'
Set-Location $SimDir
if (-not (Test-Path node_modules)) {
  Write-Host "==> First run: installing npm deps in $SimDir" -ForegroundColor Cyan
  npm install --silent --no-audit --no-fund
}
& node publisher.js @args
exit $LASTEXITCODE
