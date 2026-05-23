#!/usr/bin/env bash
# Manage the local Solace PubSub+ broker (macOS / Linux).
#
# Usage:
#   ./scripts/solace.sh start              Start the broker, wait for healthy
#   ./scripts/solace.sh stop               Stop the broker, keep state
#   ./scripts/solace.sh stop --wipe        Stop AND wipe persisted state
#   ./scripts/solace.sh status             Show container state, health, ports
#   ./scripts/solace.sh logs               Tail broker logs (Ctrl+C to exit)
#   ./scripts/solace.sh restart            stop + start
#
# Reads docker-compose.yml at the repo root. Requires Docker Engine or
# Docker Desktop with `docker compose` (v2) available on PATH.

set -euo pipefail

CONTAINER=vortex-solace
HEALTHY_TIMEOUT_SECONDS=180
ADMIN_USER=admin
ADMIN_PASS=admin
SMF_PORT=55554
SMF_TLS_PORT=55443
SEMP_PORT=8080
REST_PORT=9000

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
    # Fall back to the parent of this script's directory.
    cd "$(dirname "$0")/.." && pwd
  fi
}

# ----- commands --------------------------------------------------------------

cmd_start() {
  require_docker
  cd "$(repo_root)"

  echo "$(bold "==>") Bringing up Solace broker..."
  docker compose up -d solace

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
        printf '.' ; sleep 2
        ;;
      running:unhealthy)
        echo
        die "Broker reported unhealthy. Check 'scripts/solace.sh logs'."
        ;;
      *:*)
        echo
        die "Unexpected container state ($state / $health). Check 'docker compose logs solace'."
        ;;
    esac

    if [[ $(date +%s) -gt $deadline ]]; then
      echo
      die "Timed out waiting for broker to become healthy after ${HEALTHY_TIMEOUT_SECONDS}s. Check 'scripts/solace.sh logs'."
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
    echo "$(bold "==>") Stopping broker and wiping persisted state..."
    docker compose down -v
  else
    echo "$(bold "==>") Stopping broker (state preserved in volume)..."
    docker compose stop solace
  fi
  echo "$(green Done.)"
}

cmd_status() {
  require_docker
  cd "$(repo_root)"

  if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
    echo "$(yellow Container '$CONTAINER' does not exist. Run: ./scripts/solace.sh start)"
    return 0
  fi

  local state health started restarts
  state=$(docker inspect "$CONTAINER" --format '{{.State.Status}}')
  health=$(docker inspect "$CONTAINER" --format '{{.State.Health.Status}}' 2>/dev/null || echo unknown)
  started=$(docker inspect "$CONTAINER" --format '{{.State.StartedAt}}')
  restarts=$(docker inspect "$CONTAINER" --format '{{.RestartCount}}')

  echo "$(bold Container:)  $CONTAINER"
  echo "$(bold State:)      $state"
  echo "$(bold Health:)     $health"
  echo "$(bold Started:)    $started"
  echo "$(bold Restarts:)   $restarts"

  if [[ "$state" == "running" && "$health" == "healthy" ]]; then
    echo
    echo "$(bold Endpoints:)"
    echo "  SMF        tcp://localhost:${SMF_PORT}      (vortex-server connects here)"
    echo "  SMF/TLS    tcps://localhost:${SMF_TLS_PORT}"
    echo "  REST       http://localhost:${REST_PORT}        (POST to /TOPIC/<topic>)"
    echo "  Manager    http://localhost:${SEMP_PORT}        (UI: ${ADMIN_USER}/${ADMIN_PASS})"

    # Best-effort broker version lookup via SEMP.
    local version
    version=$(curl -sfu "${ADMIN_USER}:${ADMIN_PASS}" \
      "http://localhost:${SEMP_PORT}/SEMP/v2/config/about/api" 2>/dev/null \
      | grep -o '"sempVersion":"[^"]*"' \
      | head -1 || true)
    if [[ -n "$version" ]]; then
      echo "  $version"
    fi
  fi
}

cmd_logs() {
  require_docker
  cd "$(repo_root)"
  exec docker compose logs -f --tail 50 solace
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
