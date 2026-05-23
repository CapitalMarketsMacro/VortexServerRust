#!/usr/bin/env bash
# Run the WebSocket simulator server (Python). vortex-server's WS ingress
# is a *client*, so this script starts a server on ws://localhost:8765 by
# default; point vortex's WS-sourced table at it.
#
# Example:
#   ./scripts/sim-ws-py.sh
#   ./scripts/sim-ws-py.sh --port=9999 --rate-ms=100
set -euo pipefail
SIM_DIR="$(cd "$(dirname "$0")/.." && pwd)/simulators/websocket/python"
cd "$SIM_DIR"
if [[ ! -d .venv ]]; then
  echo "==> First run: creating venv and installing deps in $SIM_DIR" >&2
  python3 -m venv .venv
  .venv/bin/pip install --quiet --upgrade pip
  .venv/bin/pip install --quiet -r requirements.txt
fi
exec .venv/bin/python server.py "$@"
