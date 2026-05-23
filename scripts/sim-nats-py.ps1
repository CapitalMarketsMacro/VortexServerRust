<#
.SYNOPSIS  Run the NATS simulator (Python) on Windows.
.DESCRIPTION First run creates a venv and installs nats-py. Forwards all
             remaining args to publisher.py.
.EXAMPLE     .\scripts\sim-nats-py.ps1
.EXAMPLE     .\scripts\sim-nats-py.ps1 --mode=jetstream
#>
$ErrorActionPreference = 'Stop'
$SimDir = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..')).Path 'simulators\nats\python'
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
