"""WebSocket data simulator for vortex-server (Python).

vortex-server's WebSocket *ingress* is a client (it connects out to a
remote feed and consumes pushed messages), so this script is a small WS
*server* that streams simulated MarketTicks JSON rows to every connected
client at a configurable rate.

Schema matches the `MarketTicks` table in config.example.json
(index = tickId). Point vortex-server at `ws://localhost:8765/ticks` to
consume.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import signal
import sys
from datetime import datetime, timezone

import websockets
from websockets.asyncio.server import serve


SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA",
           "AMD", "INTC", "NFLX"]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class FeedState:
    """Process-global state so reconnecting clients see continuous tick
    IDs and a continuous price walk, like a real market feed would."""

    def __init__(self) -> None:
        self.seq = 0
        self.prices: dict[str, float] = {}

    def next_tick(self) -> dict:
        self.seq += 1
        sym = random.choice(SYMBOLS)
        mid = self.prices.setdefault(sym, random.uniform(50.0, 500.0))
        mid = max(0.01, mid + random.uniform(-0.10, 0.10))
        self.prices[sym] = mid
        spread = max(0.01, mid * 0.0002)
        return {
            "tickId": f"TICK-{self.seq:08d}",
            "symbol": sym,
            "price":  round(mid, 2),
            "bid":    round(mid - spread / 2, 2),
            "ask":    round(mid + spread / 2, 2),
            "ts":     now_iso(),
        }


async def handle_client(ws, state: FeedState, rate_ms: int) -> None:
    peer = getattr(ws, "remote_address", "?")
    path = getattr(ws, "request", None)
    path_str = path.path if path is not None else "/"
    print(f"[ws] client connected from {peer} path={path_str}", flush=True)
    sent = 0
    try:
        while True:
            row = state.next_tick()
            await ws.send(json.dumps(row))
            sent += 1
            if sent % 100 == 0:
                print(f"[ws] -> {peer} sent={sent}", flush=True)
            await asyncio.sleep(rate_ms / 1000.0)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        print(f"[ws] client {peer} disconnected after {sent} messages", flush=True)


async def main_async(args: argparse.Namespace) -> None:
    state = FeedState()

    async def handler(ws):
        await handle_client(ws, state, args.rate_ms)

    stop = asyncio.Event()

    def trigger_stop():
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, trigger_stop)
        except NotImplementedError:
            # Windows proactor loop.
            pass

    async with serve(handler, args.host, args.port):
        print(f"[ws] listening on ws://{args.host}:{args.port}{args.path} "
              f"(rate ~{1000 / args.rate_ms:.1f} msg/s/client)", flush=True)
        await stop.wait()
        print("[ws] shutdown signalled, closing server", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host",    default="0.0.0.0",
                   help="Bind address (default 0.0.0.0)")
    p.add_argument("--port",    type=int, default=8765,
                   help="Bind port (default 8765)")
    p.add_argument("--path",    default="/ticks",
                   help="Display-only WS path (server accepts any path)")
    p.add_argument("--rate-ms", type=int, default=200,
                   help="Per-client inter-message delay (default 200ms ≈ 5 msg/s)")
    p.add_argument("--seed",    type=int, default=None,
                   help="Optional RNG seed for reproducible price walks")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    try:
        asyncio.run(main_async(args))
        return 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
