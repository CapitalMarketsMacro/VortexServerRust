#!/usr/bin/env bash
# Run the Solace simulator (Python). Publishes JSON rows via REST
# messaging — needs the broker (./scripts/solace.sh start) to be up.
#
# Example:
#   ./scripts/sim-solace-py.sh
#   ./scripts/sim-solace-py.sh --topic=executions/burst --rate-ms=50
set -euo pipefail
SIM_DIR="$(cd "$(dirname "$0")/.." && pwd)/simulators/solace/python"
cd "$SIM_DIR"
if [[ ! -d .venv ]]; then
  echo "==> First run: creating venv and installing deps in $SIM_DIR" >&2
  python3 -m venv .venv
  .venv/bin/pip install --quiet --upgrade pip
  .venv/bin/pip install --quiet -r requirements.txt
fi
exec .venv/bin/python publisher.py "$@"
