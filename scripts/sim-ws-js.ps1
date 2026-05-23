<#
.SYNOPSIS  Run the WebSocket simulator server (Node.js) on Windows.
.DESCRIPTION First run installs `ws` via npm.
.EXAMPLE     .\scripts\sim-ws-js.ps1
.EXAMPLE     .\scripts\sim-ws-js.ps1 --port=9999 --rate-ms=100
#>
$ErrorActionPreference = 'Stop'
$SimDir = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..')).Path 'simulators\websocket\nodejs'
Set-Location $SimDir
if (-not (Test-Path node_modules)) {
  Write-Host "==> First run: installing npm deps in $SimDir" -ForegroundColor Cyan
  npm install --silent --no-audit --no-fund
}
& node server.js @args
exit $LASTEXITCODE
