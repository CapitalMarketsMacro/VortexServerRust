<#
.SYNOPSIS
    Manage the local Solace PubSub+ broker (Windows).

.DESCRIPTION
    Wraps `docker compose` to start / stop / inspect the broker defined
    in docker-compose.yml at the repo root. Requires Docker Desktop
    (which ships Compose v2) to be installed and running.

.EXAMPLE
    .\scripts\solace.ps1 start
    .\scripts\solace.ps1 stop
    .\scripts\solace.ps1 stop -Wipe
    .\scripts\solace.ps1 status
    .\scripts\solace.ps1 logs
    .\scripts\solace.ps1 restart
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('start', 'stop', 'status', 'logs', 'restart', 'help')]
    [string]$Command = 'help',

    [switch]$Wipe
)

$ErrorActionPreference = 'Stop'

$Container             = 'vortex-solace'
$HealthyTimeoutSeconds = 180
$AdminUser             = 'admin'
$AdminPass             = 'admin'
$SmfPort               = 55554
$SmfTlsPort            = 55443
$SempPort              = 8080
$RestPort              = 9000

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
    # Fall back to the parent of the scripts directory.
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

    Write-Host "==> Bringing up Solace broker..." -ForegroundColor Cyan
    docker compose up -d solace
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
                Start-Sleep -Seconds 2
            }
            'running:unhealthy' {
                Write-Host
                Die "Broker reported unhealthy. Check '.\scripts\solace.ps1 logs'."
            }
            default {
                Write-Host
                Die "Unexpected container state ($state / $health). Check 'docker compose logs solace'."
            }
        }

        if ((Get-Date) -gt $deadline) {
            Write-Host
            Die "Timed out waiting for broker to become healthy after $HealthyTimeoutSeconds seconds. Check '.\scripts\solace.ps1 logs'."
        }
    }
}

function Cmd-Stop {
    Require-Docker
    Set-Location (Get-RepoRoot)

    if ($Wipe) {
        Write-Host "==> Stopping broker and wiping persisted state..." -ForegroundColor Cyan
        docker compose down -v
    } else {
        Write-Host "==> Stopping broker (state preserved in volume)..." -ForegroundColor Cyan
        docker compose stop solace
    }
    Write-Host "Done." -ForegroundColor Green
}

function Cmd-Status {
    Require-Docker
    Set-Location (Get-RepoRoot)

    $state = Inspect '{{.State.Status}}'
    if (-not $state) {
        Write-Host "Container '$Container' does not exist. Run: .\scripts\solace.ps1 start" -ForegroundColor Yellow
        return
    }

    $health   = Inspect '{{.State.Health.Status}}'
    if (-not $health) { $health = 'unknown' }
    $started  = Inspect '{{.State.StartedAt}}'
    $restarts = Inspect '{{.RestartCount}}'

    Write-Host "Container:  $Container"
    Write-Host "State:      $state"
    Write-Host "Health:     $health"
    Write-Host "Started:    $started"
    Write-Host "Restarts:   $restarts"

    if ($state -eq 'running' -and $health -eq 'healthy') {
        Write-Host
        Write-Host "Endpoints:"
        Write-Host "  SMF        tcp://localhost:$SmfPort      (vortex-server connects here)"
        Write-Host "  SMF/TLS    tcps://localhost:$SmfTlsPort"
        Write-Host "  REST       http://localhost:$RestPort        (POST to /TOPIC/<topic>)"
        Write-Host "  Manager    http://localhost:$SempPort        (UI: $AdminUser/$AdminPass)"

        # Best-effort broker version lookup via SEMP.
        try {
            $cred = New-Object System.Management.Automation.PSCredential(
                $AdminUser, (ConvertTo-SecureString $AdminPass -AsPlainText -Force))
            $resp = Invoke-RestMethod -Uri "http://localhost:$SempPort/SEMP/v2/config/about/api" `
                -Authentication Basic -Credential $cred `
                -AllowUnencryptedAuthentication -ErrorAction Stop
            if ($resp.data.sempVersion) {
                Write-Host "  sempVersion=$($resp.data.sempVersion)"
            }
        } catch {
            # Silent — version is just a nicety.
        }
    }
}

function Cmd-Logs {
    Require-Docker
    Set-Location (Get-RepoRoot)
    docker compose logs -f --tail 50 solace
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
