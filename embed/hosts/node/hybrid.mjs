// RFC-0081 S1's gate: RFC-0077 S7's "verify it locally" for each wallet given, in Node, with the pools
// read through `attach` instead of written into the statement. The wallet nest is `rpc`, its filters
// private. The pools nest is graph-allocations-nest at the commit its publisher sealed from, opened
// `mirror+rpc`: the tables its public filters' events decode into, whole, from the published dataset,
// then those filters from the RPC to the server's block. Its two answers reach the wallet's
// statements as Arrow IPC, and every figure is compared with `/api/portfolio` at that block by
// Lodestar's own comparison.
//
// Every GET is logged with its bytes; the run fails on one that names the wallet or an indexer.
// Run with Lodestar's tsx, which reads its TypeScript:
//   <lodestar>/node_modules/.bin/tsx hybrid.mjs <plain.wasm> <lodestar dir> <allocations nest dir>
//     <wallet>... [--mirror url] [--rpc url] [--out report.json]
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fetchRpc, load } from "../browser/host.js";
import { fsStorage } from "./fs-storage.mjs";
import { nestFiles } from "../../mirror.mjs";

const args = process.argv.slice(2);
const flag = (name, dflt) => {
  const at = args.indexOf(name);
  return at < 0 ? dflt : args.splice(at, 2)[1];
};
const mirrorBase = flag("--mirror", "https://nestsss.nestsss-worker.workers.dev");
const rpcUrl = flag("--rpc", "https://arb1.arbitrum.io/rpc");
const outFile = flag("--out");
const [wasm, lodestar, allocationsDir, ...wallets] = args;
if (!wasm || !lodestar || !allocationsDir || !wallets.length || !wallets.every((w) => /^0x[0-9a-fA-F]{40}$/.test(w))) {
  console.error("usage: hybrid.mjs <plain.wasm> <lodestar dir> <allocations nest dir> <wallet>... [--mirror url] [--rpc url] [--out file]");
  process.exit(2);
}
const lib = (f) => import(path.join(lodestar, "src/lib/verify", f));
const { serverAtOneBlock, nestFiles: lodestarNest } = await lib("run.ts");
const { LOG_WINDOW, poolFilters, walletFilters } = await lib("filters.ts");
const { ALWAYS_NOT_COMPUTED, compareCurator, compareDelegator, poolsToFetch } = await lib("compare.ts");
const sql = await lib("sql.ts");

// graph-allocations-nest at 7795a33, the commit its publisher sealed the public dataset from. Its
// identity names that dataset, so other files are refused by the mirror's absence of them.
const DATASET = /^d6fd43c8[0-9a-f]{52}6e8b$/;

// S7's empty relations, as `literalRows` writes them, which the comparison reads from the pools instead.
const EMPTY_POOLS = sql.literalRows([], [
  ["id", "address"], ["delegated_tokens", "HUGEINT"], ["delegator_shares", "HUGEINT"],
  ["delegated_thawing_tokens", "HUGEINT"], ["indexing_reward_cut", "HUGEINT"], ["query_fee_cut", "HUGEINT"],
]);
const EMPTY_THAWS = sql.literalRows([], [["delegator", "address"], ["indexer", "address"], ["thawing_until", "BIGINT"]]);
function attached(statement) {
  let out = statement;
  for (const [from, to] of [[EMPTY_POOLS, "SELECT * FROM pools.indexers"], [EMPTY_THAWS, "SELECT * FROM pools.thaws"]]) {
    const n = out.split(from).length - 1;
    if (n > 1) throw new Error("a relation appears twice");
    out = out.replace(from, to);
  }
  if (/VALUES|WHERE false/.test(out)) {
    throw new Error("a typed literal is left in the statement");
  }
  return out;
}
// The pools nest's figures in the types it computes them in, not cast to text for the page.
const typedPools = (pools) => sql.poolsSql(pools).replace(/CAST\((\w+) AS VARCHAR\) AS \1/g, "$1");

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

const bytes = readFileSync(wasm);
const root = mkdtempSync(path.join(tmpdir(), "nest-hybrid-"));
const allocations = nestFiles(allocationsDir);
const nestDir = path.join(lodestar, "public/verify-locally/nest");
const lodestarFiles = nestFiles(nestDir);

async function open(nid, config) {
  const t0 = performance.now();
  const count = { n: 0 };
  const before = gets.length;
  const e = await load(bytes, {
    storage: fsStorage(path.join(root, nid)),
    rpc: rpcFor(rpcUrl, count),
    log: (l, s) => l < 2 && console.error(`guest[${l}]: ${s}`),
  });
  e.open({ nid, rpc: ["main"], storage: "host", fresh: true, window: LOG_WINDOW, memory_mib: 1024, ...config });
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
  const mine = gets.slice(before).filter((g) => g.url.startsWith(mirrorBase));
  e.measured = {
    source: config.source,
    ms: Math.round(performance.now() - t0),
    rpc_requests: count.n,
    mirror_gets: mine.length,
    mirror_bytes: mine.reduce((s, g) => s + g.bytes, 0),
    hydrated: e.report().mirror ?? null,
    block: JSON.parse(e.request("GET", "/ready").body).last_block,
    filters: config.filters.length,
    mem_mib: +(e.memoryBytes() / 2 ** 20).toFixed(1),
  };
  console.error(JSON.stringify({ nid, ...e.measured }));
  return e;
}

const rows = (a, q) => {
  if (a.status !== 200) throw new Error(`sql ${a.status}: ${a.body.slice(0, 2000)}\n${q.slice(0, 300)}`);
  const env = JSON.parse(a.body);
  if (env.truncated || env.degraded || env.tip_unavailable) throw new Error("the engine answered from part of the nest");
  return env.rows;
};
const get = (e, q) => rows(e.request("GET", `/sql?q=${encodeURIComponent(q)}`), q);
const arrow = (e, q) => {
  const a = e.request("POST", "/sql", JSON.stringify({ q, format: "arrow" }));
  if (a.status !== 200 || !a.body_base64) throw new Error(`arrow ${a.status}: ${a.body?.slice(0, 2000)}`);
  return { arrow: a.body_base64, answer: a.headers["x-nuthatch-answer"] };
};

const ZERO = "0x0000000000000000000000000000000000000000";
const results = [];
let failed = false;
for (const wallet of wallets) {
  const t0 = performance.now();
  const before = gets.length;
  const server = await serverAtOneBlock(async (type) =>
    (await fetch(`https://api.lodestar-dashboard.com/api/portfolio?address=${wallet}&type=${type}`)).json(),
  );
  const block = server.block;
  const tag = wallet.slice(2, 10).toLowerCase();
  const walletNest = await open(`wallet-${tag}`, {
    source: "rpc",
    files: lodestarNest(lodestarFiles, "wallet.toml"),
    filters: walletFilters(wallet),
    until: block,
    attach: [{ name: "pools", tables: ["indexers", "thaws"] }],
  });
  // Which pools: S7's own statement, whose two relations are still empty here.
  const pools = poolsToFetch(get(walletNest, sql.delegatorStakesSql(wallet, [], [])));
  // A wallet in no pool still reads the pools' tables; an indexer no event names selects no row.
  const poolsNest = await open(`pools-${tag}`, {
    source: "mirror+rpc",
    files: allocations,
    mirror: { base: mirrorBase },
    filters: poolFilters(pools.length ? pools : [ZERO]).map((f) => ({ ...f, privacy: "public" })),
    until: block,
  });
  const dataset = poolsNest.measured.hydrated?.dataset ?? "";
  const given = {
    pools: { indexers: arrow(poolsNest, typedPools(pools.length ? pools : [ZERO])), thaws: arrow(poolsNest, sql.thawsSql(wallet)) },
  };
  const post = (q) => rows(walletNest.request("POST", "/sql", JSON.stringify({ q, attach: given })), q);
  const statements = [
    attached(sql.delegatorSql(wallet, [])),
    attached(sql.delegatorStakesSql(wallet, [], [])),
    sql.curatorSql(wallet),
    sql.curatorSignalsSql(wallet),
  ];
  const [delegator, stakes, curator, signals] = statements.map(post);
  const row = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v : null);
  const list = (v) => (Array.isArray(v) ? v : []);
  const d = compareDelegator({
    server: { delegator: row(server.delegator.data.delegator), stakes: list(server.delegator.data.stakes) },
    tab: { delegator: delegator[0] ?? null, stakes },
    pools,
  });
  const c = compareCurator({
    server: { curator: row(server.curator.data.curator), signals: list(server.curator.data.signals) },
    tab: { curator: curator[0] ?? null, signals },
  });
  const figures = [...d.figures, ...c];
  const differ = figures.filter((f) => f.agreement !== "agree");
  const mine = gets.slice(before);
  const named = [wallet, ...pools].map((v) => v.toLowerCase().slice(2));
  const leaks = mine.filter(
    (g) => !g.url.startsWith("https://api.lodestar-dashboard.com/") && named.some((v) => g.url.toLowerCase().includes(v)),
  );
  const r = {
    wallet,
    block,
    pools,
    dataset,
    figures: figures.length,
    differ,
    not_computed: ALWAYS_NOT_COMPUTED.length + d.notComputed.length,
    s: +((performance.now() - t0) / 1000).toFixed(1),
    wallet_nest: walletNest.measured,
    pools_nest: poolsNest.measured,
    gets_naming_wallet_or_indexer: leaks.length,
    statement_bytes: statements.map((s) => s.length),
    peak_rss_mib: Math.round(process.resourceUsage().maxRSS / 1024),
  };
  results.push(r);
  console.log(
    `${wallet}: block ${block}, ${figures.length} figures, ${differ.length} differ, ${pools.length} pools, ` +
      `${r.s} s, mirror ${(poolsNest.measured.mirror_bytes / 2 ** 20).toFixed(1)} MiB, ${leaks.length} GETs naming it`,
  );
  for (const f of differ) console.log(`  ${f.scope} ${f.label}: server ${f.server}, tab ${f.tab}`);
  if (differ.length || !figures.length || leaks.length || !DATASET.test(dataset)) failed = true;
}
if (outFile) writeFileSync(outFile, JSON.stringify(results, null, 1));
process.exit(failed ? 1 : 0);
