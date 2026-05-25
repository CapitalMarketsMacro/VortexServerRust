<#
.SYNOPSIS  Run the WebSocket simulator server (Python) on Windows.
.DESCRIPTION Starts a WS server on ws://localhost:8765 by default;
             vortex-server's WS ingress connects to it as a client.
.EXAMPLE     .\scripts\sim-ws-py.ps1
.EXAMPLE     .\scripts\sim-ws-py.ps1 --port=9999 --rate-ms=100
#>
$ErrorActionPreference = 'Stop'
$SimDir = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..')).Path 'simulators\websocket\python'
Set-Location $SimDir
if (-not (Test-Path .venv)) {
  Write-Host "==> First run: creating venv and installing deps in $SimDir" -ForegroundColor Cyan
  python -m venv .venv
  # `python -m pip` (not the pip.exe shim) — on Windows pip can't replace its
  # own running executable, so `pip install --upgrade pip` errors noisily.
  & .venv\Scripts\python -m pip install --quiet --upgrade pip
  & .venv\Scripts\python -m pip install --quiet -r requirements.txt
}
& .venv\Scripts\python server.py @args
exit $LASTEXITCODE
