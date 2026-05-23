# Data Simulators

Local publishers and a WebSocket server that produce synthetic market
data for the four tables in `config.example.json`. Every transport that
vortex-server's ingress speaks has a simulator here, in both Python and
Node.js — pick whichever runtime you already have installed.

| Transport | Vortex table (`config.example.json`) | Simulator publishes / serves | Default endpoint |
|---|---|---|---|
| NATS Core | `RatesMarketData` (subject `rates.marketData`) | JSON rows with nested Bid/Ask ladders | `nats://localhost:4222` |
| NATS JetStream | `Orders` (stream `ORDERS`, subject `orders.>`) | JSON order rows on `orders.<symbol>` | `nats://localhost:4222` |
| Solace | `Executions` (topic `executions/>`) | JSON execution rows via Solace REST | `http://localhost:9000` |
| WebSocket | `MarketTicks` (endpoint `ws://localhost:8765/ticks`) | WS server streaming JSON ticks to clients | `ws://localhost:8765/ticks` |

## Layout

```
simulators/
├── nats/{python,nodejs}/        # NATS Core + JetStream publisher
├── solace/{python,nodejs}/      # Solace REST publisher
└── websocket/{python,nodejs}/   # WebSocket server (vortex connects to it)
```

The implementations are intentionally pairs of small, self-contained
scripts (~150 LOC each) — no shared framework, easy to read, easy to
fork into a custom data shape.

## Prerequisites

| Runtime | Why | Used by | Minimum |
|---|---|---|---|
| Python 3 | NATS, Solace, WebSocket simulators (Python) | All `python/` sims | 3.10+ |
| Node.js | NATS, Solace, WebSocket simulators (Node) | All `nodejs/` sims | 18 (for built-in `fetch`) |
| Docker  | Solace + NATS brokers used by the matching sims | Solace + NATS paths | Docker Desktop or Engine |

The NATS and Solace brokers needed by the corresponding simulators are
both bundled in the repo's `docker-compose.yml` and managed by the
`scripts/nats.{sh,ps1}` and `scripts/solace.{sh,ps1}` helpers — see the
top-level README for setup.

Dependencies are installed automatically on first run of each script
into a per-simulator `.venv/` (Python) or `node_modules/` (Node). Both
are gitignored, never committed.

## Quick start (per simulator)

The `scripts/sim-<transport>-<lang>.{sh,ps1}` wrappers handle
dependency bootstrap, find the simulator directory regardless of where
you invoke them from, and forward all arguments through to the entry
point.

### macOS / Linux

```bash
# NATS Core (publishes rates.marketData)
./scripts/sim-nats-py.sh                      # Python
./scripts/sim-nats-js.sh                      # Node

# NATS JetStream (publishes orders.<symbol> on stream ORDERS)
./scripts/sim-nats-py.sh --mode=jetstream
./scripts/sim-nats-js.sh --mode=jetstream

# Solace (publishes executions/test via REST messaging)
./scripts/sim-solace-py.sh
./scripts/sim-solace-js.sh

# WebSocket server (vortex-server connects to ws://localhost:8765/ticks)
./scripts/sim-ws-py.sh
./scripts/sim-ws-js.sh
```

### Windows (PowerShell)

```powershell
.\scripts\sim-nats-py.ps1
.\scripts\sim-nats-js.ps1 --mode=jetstream
.\scripts\sim-solace-py.ps1
.\scripts\sim-solace-js.ps1
.\scripts\sim-ws-py.ps1
.\scripts\sim-ws-js.ps1
```

## Common flags

The same flags work on every simulator (Python and Node) — the Node
versions read them as `--flag=value`:

| Flag | Default | Effect |
|---|---|---|
| `--rate-ms=N` | `200` | Inter-message delay (default ≈ 5 msg/s) |
| `--count=N` | `0` | Stop after N messages (`0` = run forever) |
| `--seed=N` | _none_ | Deterministic payload PRNG seed |

Transport-specific extras:

- **NATS**: `--url=nats://host:port`, `--mode=core|jetstream`,
  `--subject=...`, `--stream=...`, `--subject-prefix=...`
- **Solace**: `--url=http://host:port`, `--topic=...`,
  `--username=...`, `--password=...`
- **WebSocket**: `--host=...`, `--port=N`, `--path=...` (display-only)

## Payload schemas (matching `config.example.json`)

These match what each `tables[].index` / `composite_index` /
`stringify_columns` expects, so the rows are immediately usable by a
Perspective viewer once vortex-server is up.

**NATS Core → `RatesMarketData`** (composite index `MarketId`+`Id`,
nested arrays stringified by vortex):
```json
{
  "MarketId": "LON", "Id": "EURUSD",
  "Bid": [1.08500, 1.08490, 1.08480],
  "Ask": [1.08510, 1.08520, 1.08530],
  "Timestamp": "2026-05-23T14:50:00.000000Z"
}
```

**NATS JetStream → `Orders`** (index `OrderId`):
```json
{
  "OrderId": "ORD-00000001", "Symbol": "AAPL", "Side": "BUY",
  "Qty": 100, "Price": 187.45, "Status": "NEW",
  "Timestamp": "2026-05-23T14:50:00.000000Z"
}
```

**Solace → `Executions`** (index `ExecId`):
```json
{
  "ExecId": "EX-00000001", "OrderId": "ORD-00000001",
  "Symbol": "AAPL", "Qty": 50, "Price": 187.50,
  "Timestamp": "2026-05-23T14:50:00.000000Z"
}
```

**WebSocket → `MarketTicks`** (index `tickId`):
```json
{
  "tickId": "TICK-00000001", "symbol": "MSFT",
  "price": 412.32, "bid": 412.30, "ask": 412.34,
  "ts": "2026-05-23T14:50:00.000Z"
}
```

## End-to-end demo

```bash
# 1. Brokers (both bundled via docker-compose)
./scripts/solace.sh start            # ~60s to healthy
./scripts/nats.sh start              # ~5s

# 2. vortex-server (in another terminal)
cargo run -p vortex-server -- --config config.example.json

# 3. Simulators (one per terminal, or background them)
./scripts/sim-nats-py.sh                   # rates.marketData
./scripts/sim-nats-py.sh --mode=jetstream  # orders.*
./scripts/sim-solace-py.sh                 # executions/*
./scripts/sim-ws-py.sh                     # MarketTicks (point vortex at ws://localhost:8765/ticks)
```

The bundled `config.example.json` points the WebSocket-sourced
`MarketTicks` table at `wss://feed.example.com/ticks` for illustration —
flip it to `ws://localhost:8765/ticks` to consume from the bundled WS
simulator.

## What's intentionally minimal

These are local-testing aids. They prioritise readability over
features: no metrics, no config files, no plugin system. If you want a
custom symbol universe or a different payload shape, fork the entry
point — they're each one self-contained file.
