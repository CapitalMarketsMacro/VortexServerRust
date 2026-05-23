#!/usr/bin/env node
/**
 * WebSocket data simulator for vortex-server (Node.js).
 *
 * vortex-server's WebSocket ingress is a *client* — it connects out and
 * consumes pushed messages. So this script is a small WS *server* that
 * streams simulated MarketTicks JSON rows to every connected client at a
 * configurable rate.
 *
 * Schema matches the `MarketTicks` table in config.example.json
 * (index = tickId). Point vortex-server at ws://localhost:8765/ticks.
 */

import { WebSocketServer } from "ws";

function parseArgs(argv) {
  const opts = {
    host: "0.0.0.0",
    port: 8765,
    path: "/ticks",
    rateMs: 200,
    seed: null,
  };
  for (const arg of argv.slice(2)) {
    const [k, vRaw] = arg.split("=", 2);
    const v = vRaw ?? "";
    switch (k) {
      case "--host":    opts.host = v; break;
      case "--port":    opts.port = Number(v); break;
      case "--path":    opts.path = v; break;
      case "--rate-ms": opts.rateMs = Number(v); break;
      case "--seed":    opts.seed = Number(v); break;
      case "--help":
      case "-h":
        console.log("Usage: node server.js [--host=...] [--port=N] [--path=/ticks] " +
                    "[--rate-ms=N] [--seed=N]");
        process.exit(0);
      default:
        console.error(`unknown argument: ${arg}`);
        process.exit(2);
    }
  }
  return opts;
}

function makeRng(seed) {
  if (seed == null) return Math.random;
  let s = seed | 0;
  return function () {
    s = (s + 0x6D2B79F5) | 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA",
                 "AMD", "INTC", "NFLX"];

class FeedState {
  constructor(rng) {
    this.rng    = rng;
    this.seq    = 0;
    this.prices = new Map();
  }
  nextTick() {
    this.seq++;
    const sym = SYMBOLS[Math.floor(this.rng() * SYMBOLS.length)];
    let mid = this.prices.get(sym) ?? (50 + this.rng() * 450);
    mid = Math.max(0.01, mid + (this.rng() - 0.5) * 0.2);
    this.prices.set(sym, mid);
    const spread = Math.max(0.01, mid * 0.0002);
    return {
      tickId: `TICK-${String(this.seq).padStart(8, "0")}`,
      symbol: sym,
      price:  Number(mid.toFixed(2)),
      bid:    Number((mid - spread / 2).toFixed(2)),
      ask:    Number((mid + spread / 2).toFixed(2)),
      ts:     new Date().toISOString(),
    };
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const opts  = parseArgs(process.argv);
  const rng   = makeRng(opts.seed);
  const state = new FeedState(rng);

  const wss = new WebSocketServer({ host: opts.host, port: opts.port });
  console.log(`[ws] listening on ws://${opts.host}:${opts.port}${opts.path} ` +
              `(rate ~${(1000 / opts.rateMs).toFixed(1)} msg/s/client)`);

  wss.on("connection", async (ws, req) => {
    const peer = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
    console.log(`[ws] client connected from ${peer} path=${req.url}`);
    let sent = 0;
    let alive = true;
    ws.on("close",  () => { alive = false; });
    ws.on("error",  () => { alive = false; });
    while (alive && ws.readyState === ws.OPEN) {
      try {
        ws.send(JSON.stringify(state.nextTick()));
        sent++;
        if (sent % 100 === 0) {
          console.log(`[ws] -> ${peer} sent=${sent}`);
        }
      } catch {
        break;
      }
      await sleep(opts.rateMs);
    }
    console.log(`[ws] client ${peer} disconnected after ${sent} messages`);
  });

  // Graceful shutdown so docker / IDE stop-signals close client sockets cleanly.
  const closeAndExit = () => {
    console.log("[ws] shutdown signalled, closing server");
    for (const client of wss.clients) {
      try { client.close(1001, "server shutdown"); } catch {}
    }
    wss.close(() => process.exit(130));
    setTimeout(() => process.exit(130), 1000).unref();
  };
  process.on("SIGINT",  closeAndExit);
  process.on("SIGTERM", closeAndExit);
})();
