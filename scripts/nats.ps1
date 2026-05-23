<#
.SYNOPSIS
    Manage the local NATS broker (Windows).

.DESCRIPTION
    Wraps `docker compose` to start / stop / inspect the NATS service
    defined in docker-compose.yml at the repo root. Requires Docker
    Desktop (which ships Compose v2) to be installed and running.

.EXAMPLE
    .\scripts\nats.ps1 start
    .\scripts\nats.ps1 stop
    .\scripts\nats.ps1 stop -Wipe
    .\scripts\nats.ps1 status
    .\scripts\nats.ps1 logs
    .\scripts\nats.ps1 restart
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('start', 'stop', 'status', 'logs', 'restart', 'help')]
    [string]$Command = 'help',

    [switch]$Wipe
)

$ErrorActionPreference = 'Stop'

$Container             = 'vortex-nats'
$HealthyTimeoutSeconds = 60
$ClientPort            = 4222
$MonitorPort           = 8222
$VolumeName            = 'vortexserverrust_nats-storage'

# ----- helpers ---------------------------------------------------------------

function Die($Message) {
    Write-Host "ERROR: $Message" -ForegroundColor Red
    exit 1
}

function Require-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Die "Docker is not installed or not on PATH. See README.md for install steps."
    }
    try {
        docker info *> $null
        if ($LASTEXITCODE -ne 0) { throw }
    } catch {
        Die "Docker daemon isn't running. Start Docker Desktop from the Start menu."
    }
    try {
        docker compose version *> $null
        if ($LASTEXITCODE -ne 0) { throw }
    } catch {
        Die "Docker Compose v2 is required ('docker compose ...'). Install Docker Desktop."
    }
}

function Get-RepoRoot {
    try {
        $root = (& git rev-parse --show-toplevel 2>$null).Trim()
        if ($LASTEXITCODE -eq 0 -and $root) { return $root }
    } catch {}
    return (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}

function Inspect($format) {
    try {
        return (docker inspect $Container --format $format 2>$null).Trim()
    } catch {
        return $null
    }
}

# ----- commands --------------------------------------------------------------

function Cmd-Start {
    Require-Docker
    Set-Location (Get-RepoRoot)

    Write-Host "==> Bringing up NATS broker..." -ForegroundColor Cyan
    docker compose up -d nats
    if ($LASTEXITCODE -ne 0) { Die "docker compose up failed." }

    Write-Host "==> Waiting for healthcheck (up to $HealthyTimeoutSeconds seconds)..." -ForegroundColor Cyan
    $deadline = (Get-Date).AddSeconds($HealthyTimeoutSeconds)
    while ($true) {
        $state  = Inspect '{{.State.Status}}'
        $health = Inspect '{{.State.Health.Status}}'
        if (-not $state)  { $state  = 'missing' }
        if (-not $health) { $health = 'unknown' }

        switch ("$state`:$health") {
            'running:healthy' {
                Write-Host "Broker is healthy." -ForegroundColor Green
                Cmd-Status
                return
            }
            'running:starting' {
                Write-Host -NoNewline '.'
                Start-Sleep -Seconds 1
            }
            'running:unhealthy' {
                Write-Host
                Die "Broker reported unhealthy. Check '.\scripts\nats.ps1 logs'."
            }
            default {
                Write-Host
                Die "Unexpected container state ($state / $health). Check 'docker compose logs nats'."
            }
        }

        if ((Get-Date) -gt $deadline) {
            Write-Host
            Die "Timed out waiting for broker to become healthy after $HealthyTimeoutSeconds seconds. Check '.\scripts\nats.ps1 logs'."
        }
    }
}

function Cmd-Stop {
    Require-Docker
    Set-Location (Get-RepoRoot)

    if ($Wipe) {
        Write-Host "==> Stopping broker and wiping persisted JetStream data..." -ForegroundColor Cyan
        docker compose rm -sf nats
        docker volume rm $VolumeName *> $null
    } else {
        Write-Host "==> Stopping broker (JetStream data preserved in volume)..." -ForegroundColor Cyan
        docker compose stop nats
    }
    Write-Host "Done." -ForegroundColor Green
}

function Cmd-Status {
    Require-Docker
    Set-Location (Get-RepoRoot)

    $state = Inspect '{{.State.Status}}'
    if (-not $state) {
        Write-Host "Container '$Container' does not exist. Run: .\scripts\nats.ps1 start" -ForegroundColor Yellow
        return
    }

    $health   = Inspect '{{.State.Health.Status}}'
    if (-not $health) { $health = 'unknown' }
    $started  = Inspect '{{.State.StartedAt}}'
    $restarts = Inspect '{{.RestartCount}}'

    Write-Host "Container: $Container"
    Write-Host "State:     $state"
    Write-Host "Health:    $health"
    Write-Host "Started:   $started"
    Write-Host "Restarts:  $restarts"

    if ($state -eq 'running' -and $health -eq 'healthy') {
        Write-Host
        Write-Host "Endpoints:"
        Write-Host "  Clients     nats://localhost:$ClientPort    (vortex-server + simulators connect here)"
        Write-Host "  Monitoring  http://localhost:$MonitorPort   (varz / jsz / healthz)"

        try {
            $jsz = Invoke-RestMethod -Uri "http://localhost:$MonitorPort/jsz" -ErrorAction Stop
            Write-Host
            Write-Host "JetStream:"
            Write-Host "  streams=$($jsz.streams)  consumers=$($jsz.consumers)  messages=$($jsz.messages)"
        } catch {
            # Silent — summary is just a nicety.
        }
    }
}

function Cmd-Logs {
    Require-Docker
    Set-Location (Get-RepoRoot)
    docker compose logs -f --tail 50 nats
}

function Cmd-Restart {
    Cmd-Stop
    Cmd-Start
}

function Cmd-Help {
    Get-Help $PSCommandPath -Detailed
}

# ----- dispatcher ------------------------------------------------------------

switch ($Command) {
    'start'   { Cmd-Start }
    'stop'    { Cmd-Stop }
    'status'  { Cmd-Status }
    'logs'    { Cmd-Logs }
    'restart' { Cmd-Restart }
    'help'    { Cmd-Help }
}
