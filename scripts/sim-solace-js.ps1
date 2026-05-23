<#
.SYNOPSIS  Run the Solace simulator (Node.js) on Windows.
.DESCRIPTION Uses built-in fetch — no npm deps.
.EXAMPLE     .\scripts\sim-solace-js.ps1
.EXAMPLE     .\scripts\sim-solace-js.ps1 --topic=executions/burst --rate-ms=50
#>
$ErrorActionPreference = 'Stop'
$SimDir = Join-Path (Resolve-Path (Join-Path $PSScriptRoot '..')).Path 'simulators\solace\nodejs'
Set-Location $SimDir
& node publisher.js @args
exit $LASTEXITCODE
