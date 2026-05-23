#!/usr/bin/env bash
# Run the WebSocket simulator server (Node.js). First run installs `ws`.
#
# Example:
#   ./scripts/sim-ws-js.sh
#   ./scripts/sim-ws-js.sh --port=9999 --rate-ms=100
set -euo pipefail
SIM_DIR="$(cd "$(dirname "$0")/.." && pwd)/simulators/websocket/nodejs"
cd "$SIM_DIR"
if [[ ! -d node_modules ]]; then
  echo "==> First run: installing npm deps in $SIM_DIR" >&2
  npm install --silent --no-audit --no-fund
fi
exec node server.js "$@"
