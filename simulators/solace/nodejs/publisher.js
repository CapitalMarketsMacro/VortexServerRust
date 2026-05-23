#!/usr/bin/env node
/**
 * Solace REST messaging simulator for vortex-server (Node.js).
 *
 * Uses Node 18+'s built-in `fetch` so there are no npm dependencies.
 * Mirrors simulators/solace/python/publisher.py — same topic, same
 * payloads, same CLI flags — so users can pick whichever runtime is
 * already installed.
 */

function parseArgs(argv) {
  const opts = {
    url: "http://localhost:9000",
    topic: "executions/test",
    username: "default",
    password: "",
    rateMs: 200,
    count: 0,
    seed: null,
  };
  for (const arg of argv.slice(2)) {
    const [k, vRaw] = arg.split("=", 2);
    const v = vRaw ?? "";
    switch (k) {
      case "--url":      opts.url = v; break;
      case "--topic":    opts.topic = v; break;
      case "--username": opts.username = v; break;
      case "--password": opts.password = v; break;
      case "--rate-ms":  opts.rateMs = Number(v); break;
      case "--count":    opts.count = Number(v); break;
      case "--seed":     opts.seed = Number(v); break;
      case "--help":
      case "-h":
        console.log("Usage: node publisher.js [--url=...] [--topic=...] " +
                    "[--username=...] [--password=...] [--rate-ms=N] [--count=N] [--seed=N]");
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

const SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA"];
const nowIso  = () => new Date().toISOString();
const pick    = (rng, xs) => xs[Math.floor(rng() * xs.length)];

function makeExecution(rng, seq, orderSeq, prices) {
  const sym = pick(rng, SYMBOLS);
  let mid = prices.get(sym) ?? (50 + rng() * 450);
  mid = Math.max(0.01, mid + (rng() - 0.5) * 0.5);
  prices.set(sym, mid);
  return {
    ExecId:    `EX-${String(seq).padStart(8, "0")}`,
    OrderId:   `ORD-${String(orderSeq).padStart(8, "0")}`,
    Symbol:    sym,
    Qty:       1 + Math.floor(rng() * 500),
    Price:     Number(mid.toFixed(2)),
    Timestamp: nowIso(),
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const opts = parseArgs(process.argv);
  const rng  = makeRng(opts.seed);

  const base     = opts.url.replace(/\/+$/, "");
  const endpoint = `${base}/TOPIC/${opts.topic.replace(/^\/+/, "")}`;
  const authHdr  = "Basic " + Buffer.from(`${opts.username}:${opts.password}`).toString("base64");

  console.log(`[solace] publishing to ${endpoint} at ~${(1000 / opts.rateMs).toFixed(1)} msg/s ` +
              `as user '${opts.username}'`);

  let stop = false;
  for (const sig of ["SIGINT", "SIGTERM"]) {
    process.on(sig, () => { stop = true; });
  }

  const prices = new Map();
  let sent = 0;
  let failed = 0;
  const start = process.hrtime.bigint();

  while (!stop && (opts.count === 0 || sent < opts.count)) {
    const row = makeExecution(rng, sent + 1, Math.floor(sent / 3) + 1, prices);
    try {
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": authHdr },
        body: JSON.stringify(row),
      });
      if (!res.ok) {
        failed++;
        if (failed <= 3 || failed % 100 === 0) {
          const text = await res.text().catch(() => "");
          console.error(`[solace] publish failed: HTTP ${res.status} ${text.slice(0, 120)}`);
        }
      }
    } catch (err) {
      failed++;
      if (failed <= 3 || failed % 100 === 0) {
        console.error(`[solace] publish failed: ${err.message ?? err}`);
      }
    }
    sent++;
    if (sent % 50 === 0) {
      const elapsed = Number(process.hrtime.bigint() - start) / 1e9;
      console.log(`[solace] sent=${sent} failed=${failed} ` +
                  `actual_rate=${(sent / elapsed).toFixed(1)}/s`);
    }
    await sleep(opts.rateMs);
  }
  console.log(`[solace] disconnected after ${sent} messages (${failed} failed)`);
  process.exit(stop ? 130 : 0);
})();
