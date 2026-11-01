// RFC-0081 S0, the kill question: RFC-0077 S7's "verify it locally" run for one wallet in Node, with
// its pools nest opened as `mirror+rpc`: graph-allocations-nest's published dataset for the tables the
// pool filters name, then those filters asked of the RPC from where the dataset ends to `until`. The
// wallet nest stays `rpc`. Lodestar's own comparison (`src/lib/verify/run.ts`) drives both nests and
// compares every figure with `/api/portfolio` at one block.
//
// Every GET is logged with its bytes, and the run fails if one names the wallet.
// Run with Lodestar's tsx, which reads its TypeScript:
//   <lodestar>/node_modules/.bin/tsx hybrid.mjs <plain.wasm> <lodestar dir> <allocations nest dir>
//     <wallet> [--mirror url] [--rpc url] [--out report.json]
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fetchRpc, load } from "../browser/host.js";
import { fsStorage } from "./fs-storage.mjs";
import { nestFiles } from "../../mirror.mjs";

const [wasm, lodestar, allocationsDir, wallet, ...flags] = process.argv.slice(2);
const flag = (name, dflt) => {
  const at = flags.indexOf(name);
  return at < 0 ? dflt : flags[at + 1];
};
if (!wasm || !lodestar || !allocationsDir || !/^0x[0-9a-fA-F]{40}$/.test(wallet ?? "")) {
  console.error("usage: hybrid.mjs <plain.wasm> <lodestar dir> <allocations nest dir> <wallet> [--mirror url] [--rpc url] [--out file]");
  process.exit(2);
}
const mirrorBase = flag("--mirror", "https://nestsss.nestsss-worker.workers.dev");
const rpcUrl = flag("--rpc", "https://arb1.arbitrum.io/rpc");
const { verify } = await import(path.join(lodestar, "src/lib/verify/run.ts"));

// Every GET this process makes, the host's mirror reads among them, with the bytes each one carried.
const gets = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, init) => {
  const res = await realFetch(url, init);
  if (!init?.method || init.method === "GET") {
    gets.push({ url: String(url), status: res.status, bytes: Number(res.headers.get("content-length") ?? 0) });
  }
  return res;
};

// arb1.arbitrum.io answers ten headers to a batch and refuses fifty; as Lodestar's worker splits them.
const HEADER_BATCH = 10;
function rpcFor(url, count) {
  const post = fetchRpc({ main: url });
  return async (r) => {
    count.n++;
    let batch;
    try {
      batch = JSON.parse(r.body);
    } catch {
      return post(r);
    }
    if (!Array.isArray(batch) || batch.length <= HEADER_BATCH) return post(r);
    const items = [];
    for (let i = 0; i < batch.length; i += HEADER_BATCH) {
      let a;
      for (let k = 0; ; k++) {
        a = await post({ ...r, body: JSON.stringify(batch.slice(i, i + HEADER_BATCH)) });
        if (a.status !== 429 || k >= 6) break;
        await new Promise((ok) => setTimeout(ok, 500 * 2 ** k));
      }
      if (a.error || a.status !== 200) return a;
      const v = JSON.parse(a.body);
      if (!Array.isArray(v)) return a;
      items.push(...v);
    }
    return { status: 200, status_text: "OK", retry_after: null, body: JSON.stringify(items) };
  };
}

// Each pool table the filters name, and its indexer column.
const POOL_COLUMNS = {
  staking__tokens_delegated: "serviceProvider",
  staking__tokens_undelegated: "serviceProvider",
  staking__delegated_tokens_withdrawn: "serviceProvider",
  staking__thaw_request_created: "serviceProvider",
  staking__delegation_fee_cut_set: "serviceProvider",
  staking__provision_created: "serviceProvider",
  staking__tokens_to_delegation_pool_added: "serviceProvider",
  rewards__horizon_rewards_assigned: "indexer",
  rewards__rewards_assigned: "indexer",
  staking_legacy__stake_delegated: "indexer",
  staking_legacy__stake_delegated_locked: "indexer",
  staking_legacy__delegation_parameters_updated: "indexer",
  staking_legacy__allocation_created: "indexer",
  staking_legacy__rebate_claimed: "indexer",
  staking_legacy__rebate_collected: "indexer",
};

const bytes = readFileSync(wasm);
const root = mkdtempSync(path.join(tmpdir(), "nest-hybrid-"));
const allocations = nestFiles(allocationsDir);
const nests = new Map();
const opened = [];

const engine = {
  async open(n) {
    const pools = n.nid.startsWith("profile-pools-");
    const t0 = performance.now();
    const count = { n: 0 };
    const getsBefore = gets.length;
    const e = await load(bytes, {
      storage: fsStorage(path.join(root, n.nid)),
      rpc: rpcFor(rpcUrl, count),
      log: (l, s) => l < 2 && console.error(`guest[${l}]: ${s}`),
    });
    e.open({
      nid: n.nid,
      rpc: ["main"],
      storage: "host",
      fresh: true,
      // The pools nest is graph-allocations-nest, as published; the wallet nest is S7's own.
      files: pools ? allocations : n.files,
      ...(pools ? { mirror: { base: mirrorBase } } : {}),
      filters: n.filters,
      window: n.window,
      until: n.until,
      memory_mib: 1024,
    });
    for (let failures = 0; ; failures++) {
      try {
        await e.run();
        break;
      } catch (err) {
        if (err?.fault !== "follow_failed" || failures >= 5) throw err;
        console.error(`nuthatch: ${err.message}; carrying on from the store`);
        await new Promise((ok) => setTimeout(ok, 2000 * 2 ** failures));
      }
    }
    const ready = JSON.parse(e.request("GET", "/ready").body);
    const report = e.report();
    const mine = gets.slice(getsBefore).filter((g) => g.url.startsWith(mirrorBase));
    const o = {
      nest: pools ? "pools (mirror+rpc)" : "wallet (rpc)",
      ms: Math.round(performance.now() - t0),
      rpc_requests: count.n,
      mirror_gets: mine.length,
      mirror_bytes: mine.reduce((s, g) => s + g.bytes, 0),
      hydrated: report.mirror ?? null,
      block: ready.last_block,
      until: n.until,
      filters: n.filters.length,
      mem_mib: +(e.memoryBytes() / 2 ** 20).toFixed(1),
    };
    console.error(JSON.stringify(o));
    opened.push(o);
    nests.set(n.nid, e);
    return { block: ready.last_block, requests: count.n };
  },
  async sql(nid, q) {
    const a = nests.get(nid).request("GET", `/sql?q=${encodeURIComponent(q)}`);
    if (a.status !== 200) throw new Error(`sql ${a.status}: ${a.body.slice(0, 3000)}`);
    const env = JSON.parse(a.body);
    if (env.truncated || env.degraded || env.tip_unavailable) throw new Error("the engine answered from part of the nest");
    return env.rows;
  },
};

const lodestarFiles = nestFiles(path.join(lodestar, "public/verify-locally/nest"));
const t0 = performance.now();
const r = await verify({
  wallet,
  engine,
  engineId: "rfc-0081-s0",
  files: lodestarFiles,
  fetchPortfolio: async (type) =>
    (await fetch(`https://api.lodestar-dashboard.com/api/portfolio?address=${wallet}&type=${type}`)).json(),
  onProgress: (p) => console.error("progress", JSON.stringify(p)),
});
const ms = Math.round(performance.now() - t0);

// The privacy rule: no wallet-derived value in any mirror GET. Checked over every GET, mirror or not,
// except the server's own portfolio read, which names the wallet by design.
const w = wallet.toLowerCase().replace(/^0x/, "");
const mirrorGets = gets.filter((g) => g.url.startsWith(mirrorBase));
const leaks = gets.filter((g) => !g.url.startsWith("https://api.lodestar-dashboard.com/") && g.url.toLowerCase().includes(w));
const bad = r.figures.filter((f) => f.agreement !== "agree");

// `--oracle <manifest.json>`: per pool table, how many sealed segments hold a row of the wallet's
// indexers at all, which is what a perfect per-segment index could have admitted.
let oracle;
if (flag("--oracle") && r.pools.length) try {
  const manifest = JSON.parse(readFileSync(flag("--oracle"), "utf8"));
  const poolsNid = [...nests.keys()].find((k) => k.startsWith("profile-pools-"));
  const ids = r.pools.map((p) => `'${p.toLowerCase()}'`).join(", ");
  oracle = {};
  for (const [table, col] of Object.entries(POOL_COLUMNS)) {
    const segs = manifest.tables[table] ?? [];
    if (!segs.length) continue;
    // Counted in the engine, a hundred segments a statement, so no answer meets the row cap.
    let holding = 0;
    for (let i = 0; i < segs.length; i += 100) {
      const ranges = segs.slice(i, i + 100).map((s, k) => `(${i + k}, ${s.from_block}, ${s.to_block})`).join(", ");
      const [row] = await engine.sql(
        poolsNid,
        `SELECT COUNT(DISTINCT s.i) AS n FROM (VALUES ${ranges}) AS s(i, f, t) JOIN ${table} x \
ON x.block_number BETWEEN s.f AND s.t WHERE LOWER(x."${col}") IN (${ids})`,
      );
      holding += Number(row.n);
    }
    oracle[table] = { segments: segs.length, holding };
  }
} catch (e) {
  oracle = { error: String(e?.message ?? e).slice(0, 500) };
}
const out = {
  wallet,
  block: r.block,
  pools: r.pools,
  ms,
  rpc_requests: r.requests,
  nests: opened,
  mirror_gets: mirrorGets.length,
  mirror_bytes: mirrorGets.reduce((s, g) => s + g.bytes, 0),
  gets_naming_wallet: leaks.length,
  figures: r.figures.length,
  agree: r.figures.length - bad.length,
  differ: bad,
  peak_rss_mib: Math.round(process.resourceUsage().maxRSS / 1024),
  oracle,
};
console.log(JSON.stringify(out, null, 1));
if (flag("--out")) writeFileSync(flag("--out"), JSON.stringify({ ...out, gets, figures: r.figures }, null, 1));
process.exit(bad.length === 0 && leaks.length === 0 && r.figures.length > 0 ? 0 : 1);
