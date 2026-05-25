<#
.SYNOPSIS  Run the Solace simulator (Python) on Windows.
.DESCRIPTION Publishes JSON rows via REST messaging — needs the broker
             (.\scripts\solace.ps1 start) to be up.
.EXAMPLE     .\scripts\sim-solace-py.ps1
.EXAMPLE     .\scripts\sim-solace-py.ps1 --topic=executions/burst --rate-ms=50
#>
$ErrorActionPreference = 'Stop'
$SimDir = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..')).Path 'simulators\solace\python'
Set-Location $SimDir
if (-not (Test-Path .venv)) {
  Write-Host "==> First run: creating venv and installing deps in $SimDir" -ForegroundColor Cyan
  python -m venv .venv
  # `python -m pip` (not the pip.exe shim) — on Windows pip can't replace its
  # own running executable, so `pip install --upgrade pip` errors noisily.
  & .venv\Scripts\python -m pip install --quiet --upgrade pip
  & .venv\Scripts\python -m pip install --quiet -r requirements.txt
}
& .venv\Scripts\python publisher.py @args
exit $LASTEXITCODE
