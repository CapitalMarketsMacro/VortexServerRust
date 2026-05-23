#!/usr/bin/env bash
# Run the NATS simulator (Python). First run creates a venv and installs
# nats-py; subsequent runs reuse it. Forwards all args to publisher.py.
#
# Example:
#   ./scripts/sim-nats-py.sh                                 # rates Core
#   ./scripts/sim-nats-py.sh --mode=jetstream                # orders JetStream
#   ./scripts/sim-nats-py.sh --rate-ms=100 --count=500
set -euo pipefail
SIM_DIR="$(cd "$(dirname "$0")/.." && pwd)/simulators/nats/python"
cd "$SIM_DIR"
if [[ ! -d .venv ]]; then
  echo "==> First run: creating venv and installing deps in $SIM_DIR" >&2
  python3 -m venv .venv
  .venv/bin/pip install --quiet --upgrade pip
  .venv/bin/pip install --quiet -r requirements.txt
fi
exec .venv/bin/python publisher.py "$@"
