#!/usr/bin/env bash
# Run the Solace simulator (Node.js). Uses built-in fetch — no npm deps,
# but the script still creates an empty node_modules for parity.
#
# Example:
#   ./scripts/sim-solace-js.sh
#   ./scripts/sim-solace-js.sh --topic=executions/burst --rate-ms=50
set -euo pipefail
SIM_DIR="$(cd "$(dirname "$0")/.." && pwd)/simulators/solace/nodejs"
cd "$SIM_DIR"
exec node publisher.js "$@"
