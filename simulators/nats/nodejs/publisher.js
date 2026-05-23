#!/usr/bin/env node
/**
 * NATS data simulator for vortex-server (Node.js).
 *
 * Mirrors simulators/nats/python/publisher.py — same subjects, same
 * payloads, same CLI flags — so users can pick whichever runtime is
 * already installed.
 *
 *   node publisher.js                                 # Core, rates.marketData
 *   node publisher.js --mode=jetstream                # JetStream, orders.*
 *   node publisher.js --rate-ms=100 --count=500       # ~10/s, stop after 500
 */

import { connect, StringCodec } from "nats";

// ----- CLI -------------------------------------------------------------------

function parseArgs(argv) {
  const opts = {
    url: "nats://localhost:4222",
    mode: "core",
    subject: "rates.marketData",
    stream: "ORDERS",
    subjectPrefix: "orders",
    rateMs: 200,
    count: 0,
    seed: null,
  };
  for (const arg of argv.slice(2)) {
    const [k, vRaw] = arg.split("=", 2);
    const v = vRaw ?? "";
    switch (k) {
      case "--url":             opts.url = v; break;
      case "--mode":            opts.mode = v; break;
      case "--subject":         opts.subject = v; break;
      case "--stream":          opts.stream = v; break;
      case "--subject-prefix":  opts.subjectPrefix = v; break;
      case "--rate-ms":         opts.rateMs = Number(v); break;
      case "--count":           opts.count = Number(v); break;
      case "--seed":            opts.seed = Number(v); break;
      case "--help":
      case "-h":
        console.log("Usage: node publisher.js [--url=...] [--mode=core|jetstream] " +
                    "[--subject=...] [--stream=...] [--subject-prefix=...] " +
                    "[--rate-ms=N] [--count=N] [--seed=N]");
        process.exit(0);
      default:
        console.error(`unknown argument: ${arg}`);
        process.exit(2);
    }
  }
  if (!["core", "jetstream"].includes(opts.mode)) {
    console.error(`--mode must be 'core' or 'jetstream'`);
    process.exit(2);
  }
  return opts;
}

// ----- deterministic PRNG (matches the Python --seed flag semantics) ---------

function makeRng(seed) {
  if (seed == null) return Math.random;
  // Mulberry32: tiny, well-mixed, fine for synthetic feeds.
  let s = seed | 0;
  return function () {
    s = (s + 0x6D2B79F5) | 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ----- payload makers --------------------------------------------------------

const RATES_SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF"];
const RATES_MARKETS = ["LON", "NYC", "TKO"];
const ORDER_SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA"];
const ORDER_SIDES   = ["BUY", "SELL"];
const ORDER_STATES  = ["NEW", "PARTIAL", "FILLED", "CANCELLED"];

const nowIso = () => new Date().toISOString();
const pick   = (rng, xs) => xs[Math.floor(rng() * xs.length)];
const round5 = (n) => Number(n.toFixed(5));
const round2 = (n) => Number(n.toFixed(2));

function makeRate(rng, prices) {
  const market = pick(rng, RATES_MARKETS);
  const sym    = pick(rng, RATES_SYMBOLS);
  let mid = prices.get(sym) ?? (0.8 + rng() * 199.2);
  mid = Math.max(0.0001, mid + (rng() - 0.5) * mid * 0.001);
  prices.set(sym, mid);
  const spread = mid * 0.0001;
  const bid = round5(mid - spread / 2);
  const ask = round5(mid + spread / 2);
  return {
    MarketId: market,
    Id: sym,
    Bid: [bid, round5(bid - spread), round5(bid - spread * 2)],
    Ask: [ask, round5(ask + spread), round5(ask + spread * 2)],
    Timestamp: nowIso(),
  };
}

function makeOrder(rng, seq, prices) {
  const sym = pick(rng, ORDER_SYMBOLS);
  let mid = prices.get(sym) ?? (50 + rng() * 450);
  mid = Math.max(0.01, mid + (rng() - 0.5) * 0.5);
  prices.set(sym, mid);
  return {
    OrderId:   `ORD-${String(seq).padStart(8, "0")}`,
    Symbol:    sym,
    Side:      pick(rng, ORDER_SIDES),
    Qty:       1 + Math.floor(rng() * 1000),
    Price:     round2(mid),
    Status:    pick(rng, ORDER_STATES),
    Timestamp: nowIso(),
  };
}

// ----- publish loops ---------------------------------------------------------

async function publishCore(opts, rng) {
  const nc = await connect({ servers: opts.url, name: "vortex-sim-nats-core" });
  const sc = StringCodec();
  console.log(`[nats-core] connected to ${opts.url}, publishing to '${opts.subject}' ` +
              `at ~${(1000 / opts.rateMs).toFixed(1)} msg/s`);

  const prices = new Map();
  let sent = 0;
  const start = process.hrtime.bigint();
  installShutdown(async () => { await nc.flush(); await nc.close(); });

  while (opts.count === 0 || sent < opts.count) {
    nc.publish(opts.subject, sc.encode(JSON.stringify(makeRate(rng, prices))));
    sent++;
    if (sent % 50 === 0) {
      const elapsed = Number(process.hrtime.bigint() - start) / 1e9;
      console.log(`[nats-core] sent=${sent} actual_rate=${(sent / elapsed).toFixed(1)}/s`);
    }
    await sleep(opts.rateMs);
  }
  await nc.flush();
  await nc.close();
  console.log(`[nats-core] disconnected after ${sent} messages`);
}

async function publishJetstream(opts, rng) {
  const nc  = await connect({ servers: opts.url, name: "vortex-sim-nats-jetstream" });
  const jsm = await nc.jetstreamManager();
  const js  = nc.jetstream();
  const sc  = StringCodec();

  try {
    await jsm.streams.add({ name: opts.stream, subjects: [`${opts.subjectPrefix}.>`] });
    console.log(`[nats-js] created stream '${opts.stream}' (subjects: ${opts.subjectPrefix}.>)`);
  } catch (err) {
    // Stream already exists with compatible config — nothing to do.
    console.log(`[nats-js] stream '${opts.stream}' already exists`);
  }

  console.log(`[nats-js] connected to ${opts.url}, publishing under ` +
              `'${opts.subjectPrefix}.<symbol>' at ~${(1000 / opts.rateMs).toFixed(1)} msg/s`);

  const prices = new Map();
  let sent = 0;
  const start = process.hrtime.bigint();
  installShutdown(async () => { await nc.flush(); await nc.close(); });

  while (opts.count === 0 || sent < opts.count) {
    const row = makeOrder(rng, sent + 1, prices);
    const subject = `${opts.subjectPrefix}.${row.Symbol.toLowerCase()}`;
    await js.publish(subject, sc.encode(JSON.stringify(row)));
    sent++;
    if (sent % 50 === 0) {
      const elapsed = Number(process.hrtime.bigint() - start) / 1e9;
      console.log(`[nats-js] sent=${sent} actual_rate=${(sent / elapsed).toFixed(1)}/s`);
    }
    await sleep(opts.rateMs);
  }
  await nc.flush();
  await nc.close();
  console.log(`[nats-js] disconnected after ${sent} messages`);
}

// ----- plumbing --------------------------------------------------------------

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function installShutdown(closer) {
  let already = false;
  const handler = async () => {
    if (already) return;
    already = true;
    try { await closer(); } catch {}
    process.exit(130);
  };
  process.on("SIGINT",  handler);
  process.on("SIGTERM", handler);
}

(async () => {
  const opts = parseArgs(process.argv);
  const rng  = makeRng(opts.seed);
  try {
    if (opts.mode === "jetstream") await publishJetstream(opts, rng);
    else                            await publishCore(opts, rng);
  } catch (err) {
    console.error(`[fatal] ${err.message ?? err}`);
    process.exit(1);
  }
})();
