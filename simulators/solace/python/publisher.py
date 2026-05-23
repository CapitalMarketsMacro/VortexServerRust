"""Solace REST messaging simulator for vortex-server.

Publishes JSON rows to a Solace topic via the REST messaging endpoint
(default port 9000 on the bundled docker-compose broker). The vortex
Solace ingress sees the payload arrive as an SDT-string-wrapped message
on SMF and unwraps it automatically.

Schema matches the `Executions` table in config.example.json (index = ExecId).

Using REST instead of native SMF keeps this script dependency-light
(`requests` only) — no libsolclient install needed.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

import requests


SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA"]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def make_execution(seq: int, order_seq: int, prices: dict[str, float]) -> dict:
    sym = random.choice(SYMBOLS)
    mid = prices.setdefault(sym, random.uniform(50.0, 500.0))
    mid = max(0.01, mid + random.uniform(-0.25, 0.25))
    prices[sym] = mid
    return {
        "ExecId":    f"EX-{seq:08d}",
        "OrderId":   f"ORD-{order_seq:08d}",
        "Symbol":    sym,
        "Qty":       random.randint(1, 500),
        "Price":     round(mid, 2),
        "Timestamp": now_iso(),
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url",      default="http://localhost:9000",
                   help="Solace REST messaging base URL")
    p.add_argument("--topic",    default="executions/test",
                   help="Topic to publish to (vortex subscribes to executions/>)")
    p.add_argument("--username", default="default",
                   help="Solace client username")
    p.add_argument("--password", default="",
                   help="Solace client password")
    p.add_argument("--rate-ms",  type=int, default=200,
                   help="Inter-message delay (default 200ms ≈ 5 msg/s)")
    p.add_argument("--count",    type=int, default=0,
                   help="Stop after this many messages (0 = forever)")
    p.add_argument("--seed",     type=int, default=None,
                   help="Optional RNG seed for reproducible payloads")
    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    base = args.url.rstrip("/")
    endpoint = f"{base}/TOPIC/{args.topic.lstrip('/')}"

    stop = False
    def handle_sig(*_):
        nonlocal stop
        stop = True
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle_sig)

    # A pooled session keeps the underlying TCP connection hot so we
    # aren't reopening it on every publish.
    sess = requests.Session()
    sess.auth = (args.username, args.password)
    sess.headers.update({"Content-Type": "application/json"})

    print(f"[solace] publishing to {endpoint} at ~{1000 / args.rate_ms:.1f} msg/s "
          f"as user '{args.username}'", flush=True)

    prices: dict[str, float] = {}
    sent = 0
    failed = 0
    start = time.monotonic()

    try:
        while not stop and (args.count == 0 or sent < args.count):
            row = make_execution(seq=sent + 1, order_seq=(sent // 3) + 1, prices=prices)
            try:
                r = sess.post(endpoint, data=json.dumps(row), timeout=5)
                if r.status_code // 100 != 2:
                    failed += 1
                    if failed <= 3 or failed % 100 == 0:
                        print(f"[solace] publish failed: HTTP {r.status_code} {r.text[:120]}",
                              flush=True)
            except requests.RequestException as exc:
                failed += 1
                if failed <= 3 or failed % 100 == 0:
                    print(f"[solace] publish failed: {exc}", flush=True)
            sent += 1
            if sent % 50 == 0:
                rate = sent / max(time.monotonic() - start, 1e-6)
                print(f"[solace] sent={sent} failed={failed} actual_rate={rate:.1f}/s",
                      flush=True)
            time.sleep(args.rate_ms / 1000.0)
    finally:
        sess.close()
        print(f"[solace] disconnected after {sent} messages "
              f"({failed} failed)", flush=True)
    return 0 if not stop else 130


if __name__ == "__main__":
    sys.exit(main())
