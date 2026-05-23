#!/usr/bin/env bash
# Run the NATS simulator (Node.js). First run installs deps via npm.
#
# Example:
#   ./scripts/sim-nats-js.sh
#   ./scripts/sim-nats-js.sh --mode=jetstream
set -euo pipefail
SIM_DIR="$(cd "$(dirname "$0")/.." && pwd)/simulators/nats/nodejs"
cd "$SIM_DIR"
if [[ ! -d node_modules ]]; then
  echo "==> First run: installing npm deps in $SIM_DIR" >&2
  npm install --silent --no-audit --no-fund
fi
exec node publisher.js "$@"
