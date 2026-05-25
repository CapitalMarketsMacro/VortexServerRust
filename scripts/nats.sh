#!/usr/bin/env bash
# Manage the local NATS broker (macOS / Linux).
#
# Usage:
#   ./scripts/nats.sh start              Start the broker, wait for healthy
#   ./scripts/nats.sh stop               Stop the broker, keep state
#   ./scripts/nats.sh stop --wipe        Stop AND wipe persisted JetStream data
#   ./scripts/nats.sh status             Show container state, health, streams
#   ./scripts/nats.sh logs               Tail broker logs (Ctrl+C to exit)
#   ./scripts/nats.sh restart            stop + start
#
# Reads docker-compose.yml at the repo root. Requires Docker Engine or
# Docker Desktop with `docker compose` (v2) available on PATH.

set -euo pipefail

CONTAINER=vortex-nats
HEALTHY_TIMEOUT_SECONDS=60
CLIENT_PORT=4222
MONITOR_PORT=8222

# ----- helpers ---------------------------------------------------------------

color() { printf '\033[%sm%s\033[0m' "$1" "$2"; }
red()    { color '0;31' "$*"; }
green()  { color '0;32' "$*"; }
yellow() { color '0;33' "$*"; }
bold()   { color '1'    "$*"; }

die() { echo "$(red ERROR:) $*" >&2; exit 1; }

require_docker() {
  command -v docker >/dev/null 2>&1 \
    || die "Docker is not installed or not on PATH. See README.md for install steps."
  docker info >/dev/null 2>&1 \
    || die "Docker daemon isn't running. Start Docker Desktop (or 'systemctl start docker' on Linux)."
  docker compose version >/dev/null 2>&1 \
    || die "Docker Compose v2 is required ('docker compose ...'). Install Docker Desktop or the docker-compose-plugin package."
}

repo_root() {
  if git rev-parse --show-toplevel >/dev/null 2>&1; then
    git rev-parse --show-toplevel
  else
    cd "$(dirname "$0")/.." && pwd
  fi
}

# ----- commands --------------------------------------------------------------

cmd_start() {
  require_docker
  cd "$(repo_root)"

  echo "$(bold "==>") Bringing up NATS broker..."
  docker compose up -d nats

  echo "$(bold "==>") Waiting for healthcheck (up to ${HEALTHY_TIMEOUT_SECONDS}s)..."
  local deadline=$(( $(date +%s) + HEALTHY_TIMEOUT_SECONDS ))
  while :; do
    local state health
    state=$(docker inspect "$CONTAINER" --format '{{.State.Status}}' 2>/dev/null || echo missing)
    health=$(docker inspect "$CONTAINER" --format '{{.State.Health.Status}}' 2>/dev/null || echo unknown)

    case "$state:$health" in
      running:healthy)
        echo "$(green "Broker is healthy.")"
        break
        ;;
      running:starting)
        printf '.' ; sleep 1
        ;;
      running:unhealthy)
        echo
        die "Broker reported unhealthy. Check 'scripts/nats.sh logs'."
        ;;
      *:*)
        echo
        die "Unexpected container state ($state / $health). Check 'docker compose logs nats'."
        ;;
    esac

    if [[ $(date +%s) -gt $deadline ]]; then
      echo
      die "Timed out waiting for broker to become healthy after ${HEALTHY_TIMEOUT_SECONDS}s. Check 'scripts/nats.sh logs'."
    fi
  done

  cmd_status
}

cmd_stop() {
  require_docker
  cd "$(repo_root)"

  local wipe=false
  for arg in "$@"; do
    case "$arg" in
      --wipe|-w) wipe=true ;;
      *) die "Unknown flag for stop: $arg" ;;
    esac
  done

  if $wipe; then
    echo "$(bold "==>") Stopping broker and wiping persisted JetStream data..."
    # Per-service down isn't supported, so stop+rm the container and
    # delete just the nats volume by name.
    docker compose rm -sf nats
    docker volume rm vortex-nats-storage >/dev/null 2>&1 || true
  else
    echo "$(bold "==>") Stopping broker (JetStream data preserved in volume)..."
    docker compose stop nats
  fi
  echo "$(green Done.)"
}

cmd_status() {
  require_docker
  cd "$(repo_root)"

  if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
    echo "$(yellow Container '$CONTAINER' does not exist. Run: ./scripts/nats.sh start)"
    return 0
  fi

  local state health started restarts
  state=$(docker inspect "$CONTAINER" --format '{{.State.Status}}')
  health=$(docker inspect "$CONTAINER" --format '{{.State.Health.Status}}' 2>/dev/null || echo unknown)
  started=$(docker inspect "$CONTAINER" --format '{{.State.StartedAt}}')
  restarts=$(docker inspect "$CONTAINER" --format '{{.RestartCount}}')

  echo "$(bold "Container:") $CONTAINER"
  echo "$(bold "State:")     $state"
  echo "$(bold "Health:")    $health"
  echo "$(bold "Started:")   $started"
  echo "$(bold "Restarts:")  $restarts"

  if [[ "$state" == "running" && "$health" == "healthy" ]]; then
    echo
    echo "$(bold Endpoints:)"
    echo "  Clients     nats://localhost:${CLIENT_PORT}    (vortex-server + simulators connect here)"
    echo "  Monitoring  http://localhost:${MONITOR_PORT}   (varz / jsz / healthz)"

    # Best-effort JetStream summary. NATS pretty-prints /jsz with
    # whitespace after the colon, so the regex allows optional spaces.
    local jsz
    jsz=$(curl -sf "http://localhost:${MONITOR_PORT}/jsz" 2>/dev/null || true)
    if [[ -n "$jsz" ]]; then
      local streams consumers msgs
      streams=$(  echo "$jsz" | grep -oE '"streams"[[:space:]]*:[[:space:]]*[0-9]+'   | head -1 | grep -oE '[0-9]+$')
      consumers=$(echo "$jsz" | grep -oE '"consumers"[[:space:]]*:[[:space:]]*[0-9]+' | head -1 | grep -oE '[0-9]+$')
      msgs=$(     echo "$jsz" | grep -oE '"messages"[[:space:]]*:[[:space:]]*[0-9]+'  | head -1 | grep -oE '[0-9]+$')
      echo
      echo "$(bold JetStream:)"
      echo "  streams=${streams:-0}  consumers=${consumers:-0}  messages=${msgs:-0}"
    fi
  fi
}

cmd_logs() {
  require_docker
  cd "$(repo_root)"
  exec docker compose logs -f --tail 50 nats
}

cmd_restart() {
  cmd_stop
  cmd_start
}

cmd_help() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

# ----- dispatcher ------------------------------------------------------------

cmd="${1:-help}"
shift || true
case "$cmd" in
  start)            cmd_start "$@" ;;
  stop)             cmd_stop  "$@" ;;
  status|state)     cmd_status ;;
  logs|tail)        cmd_logs ;;
  restart)          cmd_restart ;;
  help|-h|--help)   cmd_help ;;
  *) die "Unknown command: $cmd (try: start | stop | status | logs | restart | help)" ;;
esac
