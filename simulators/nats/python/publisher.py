"""NATS data simulator for vortex-server.

Publishes either:
  - NATS Core messages to `rates.marketData`  (vortex table: RatesMarketData)
  - JetStream messages to `orders.*`          (vortex table: Orders)

The schema in each mode matches the corresponding entry in
`config.example.json` so the published rows are immediately usable by the
Perspective viewer once vortex-server is running.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import signal
import string
import sys
import time
from datetime import datetime, timezone

import nats
from nats.js.api import StreamConfig
from nats.js.errors import BadRequestError


# A handful of fake symbols so the data viewed in Perspective looks
# realistic and has interesting grouping behaviour.
RATES_SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF"]
RATES_MARKETS = ["LON", "NYC", "TKO"]
ORDER_SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA"]
ORDER_SIDES   = ["BUY", "SELL"]
ORDER_STATES  = ["NEW", "PARTIAL", "FILLED", "CANCELLED"]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_rate(prices: dict[str, float]) -> dict:
    """Build a RatesMarketData row with nested Bid/Ask price ladders."""
    market = random.choice(RATES_MARKETS)
    sym = random.choice(RATES_SYMBOLS)
    mid = prices.setdefault(sym, random.uniform(0.8, 200.0))
    # Random-walk the mid price.
    mid = max(0.0001, mid + random.uniform(-mid * 0.0005, mid * 0.0005))
    prices[sym] = mid
    spread = mid * 0.0001
    bid = round(mid - spread / 2, 5)
    ask = round(mid + spread / 2, 5)
    # Three-level ladder, slightly worse on each level.
    bids = [round(bid - i * spread, 5) for i in range(3)]
    asks = [round(ask + i * spread, 5) for i in range(3)]
    return {
        "MarketId": market,
        "Id": sym,
        "Bid": bids,
        "Ask": asks,
        "Timestamp": now_iso(),
    }


def make_order(seq: int, prices: dict[str, float]) -> dict:
    sym = random.choice(ORDER_SYMBOLS)
    mid = prices.setdefault(sym, random.uniform(50.0, 500.0))
    mid = max(0.01, mid + random.uniform(-0.25, 0.25))
    prices[sym] = mid
    return {
        "OrderId": f"ORD-{seq:08d}",
        "Symbol": sym,
        "Side": random.choice(ORDER_SIDES),
        "Qty": random.randint(1, 1000),
        "Price": round(mid, 2),
        "Status": random.choice(ORDER_STATES),
        "Timestamp": now_iso(),
    }


async def publish_core(args: argparse.Namespace) -> None:
    """Plain NATS Core publish loop. Subject must match vortex-server's
    `transports.nats` + `tables[*].source.subject` for the target table."""
    nc = await nats.connect(args.url, name="vortex-sim-nats-core")
    print(f"[nats-core] connected to {args.url}, "
          f"publishing to '{args.subject}' at ~{1000 / args.rate_ms:.1f} msg/s",
          flush=True)
    prices: dict[str, float] = {}
    sent = 0
    start = time.monotonic()
    try:
        while args.count == 0 or sent < args.count:
            row = make_rate(prices)
            await nc.publish(args.subject, json.dumps(row).encode())
            sent += 1
            if sent % 50 == 0:
                rate = sent / max(time.monotonic() - start, 1e-6)
                print(f"[nats-core] sent={sent} actual_rate={rate:.1f}/s", flush=True)
            await asyncio.sleep(args.rate_ms / 1000.0)
    finally:
        await nc.flush()
        await nc.close()
        print(f"[nats-core] disconnected after {sent} messages", flush=True)


async def publish_jetstream(args: argparse.Namespace) -> None:
    """JetStream publish loop. Ensures the target stream exists (idempotent)
    so this script can bootstrap a brand-new NATS server without manual setup."""
    nc = await nats.connect(args.url, name="vortex-sim-nats-jetstream")
    js = nc.jetstream()
    try:
        await js.add_stream(StreamConfig(name=args.stream, subjects=[f"{args.subject_prefix}.>"]))
        print(f"[nats-js] created stream '{args.stream}' "
              f"(subjects: {args.subject_prefix}.>)", flush=True)
    except BadRequestError:
        # Stream already exists with compatible config — nothing to do.
        print(f"[nats-js] stream '{args.stream}' already exists", flush=True)

    print(f"[nats-js] connected to {args.url}, "
          f"publishing under '{args.subject_prefix}.<symbol>' at "
          f"~{1000 / args.rate_ms:.1f} msg/s", flush=True)

    prices: dict[str, float] = {}
    sent = 0
    start = time.monotonic()
    try:
        while args.count == 0 or sent < args.count:
            row = make_order(sent + 1, prices)
            subject = f"{args.subject_prefix}.{row['Symbol'].lower()}"
            await js.publish(subject, json.dumps(row).encode())
            sent += 1
            if sent % 50 == 0:
                rate = sent / max(time.monotonic() - start, 1e-6)
                print(f"[nats-js] sent={sent} actual_rate={rate:.1f}/s", flush=True)
            await asyncio.sleep(args.rate_ms / 1000.0)
    finally:
        await nc.flush()
        await nc.close()
        print(f"[nats-js] disconnected after {sent} messages", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url",      default="nats://localhost:4222",
                   help="NATS connection URL")
    p.add_argument("--mode",     choices=("core", "jetstream"), default="core",
                   help="Publish via Core pub/sub or via JetStream")
    p.add_argument("--subject",  default="rates.marketData",
                   help="Core mode: subject to publish to")
    p.add_argument("--stream",   default="ORDERS",
                   help="JetStream mode: stream name (created if missing)")
    p.add_argument("--subject-prefix", default="orders",
                   help="JetStream mode: subject prefix (full subject = <prefix>.<symbol>)")
    p.add_argument("--rate-ms",  type=int, default=200,
                   help="Inter-message delay (default 200ms ≈ 5 msg/s)")
    p.add_argument("--count",    type=int, default=0,
                   help="Stop after this many messages (0 = forever)")
    p.add_argument("--seed",     type=int, default=None,
                   help="Optional RNG seed for reproducible payloads")
    return p.parse_args()


def install_sigint_handler(loop: asyncio.AbstractEventLoop) -> None:
    """Convert SIGINT/SIGTERM to a graceful cancel of the current task."""
    def cancel():
        for task in asyncio.all_tasks(loop):
            task.cancel()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, cancel)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler in proactor loop.
            pass


def main() -> int:
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    install_sigint_handler(loop)

    coro = publish_jetstream(args) if args.mode == "jetstream" else publish_core(args)
    try:
        loop.run_until_complete(coro)
        return 0
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n[interrupted]", flush=True)
        return 130
    finally:
        loop.close()


if __name__ == "__main__":
    sys.exit(main())
