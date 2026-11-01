// RFC-0081 S3's gate: hosted answers checked by the package's `verify`, with no Lodestar code in the
// path. For each wallet, the four portfolio statements of `fixtures/portfolio.mjs` are sent to a
// hosted nest's `/sql`, then checked by opening the nest the answers name, by NID, at their block,
// narrowed by the wallet's filters. Then, at the last wallet's block, two more: a network-wide
// statement, which must come back outside the filters, and one hosted answer with one value altered
// by a proxy in front of the host, which must come back differing on that column and value.
//
//   node verify-gate.mjs <nuthatch-engine.js> <module.wasm> <nest dir> --hosted <url> <wallet>...
//     [--mirror url] [--rpc url] [--storage dir] [--out report.json]
//
// `--hosted` is a node's base URL, answering `POST /sql` with its envelope. Exits non-zero on a
// statement that is neither equal nor named, or on either of the two checks failing.
import { createServer } from "node:http";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { NETWORK_WIDE, UNFILTERED, delegatorStakesSql, filtersFor, poolsOf, statements } from "./fixtures/portfolio.mjs";

const args = process.argv.slice(2);
const flag = (name, dflt) => {
  const at = args.indexOf(name);
  return at < 0 ? dflt : args.splice(at, 2)[1];
};
const hosted = flag("--hosted");
const mirror = flag("--mirror", "https://nestsss.nestsss-worker.workers.dev");
const rpc = flag("--rpc", "https://arb1.arbitrum.io/rpc");
const storage = flag("--storage", mkdtempSync(path.join(tmpdir(), "nest-verify-")));
const outFile = flag("--out");
const [bundle, wasm, nestDir, ...wallets] = args;
if (!bundle || !wasm || !nestDir || !hosted || !wallets.length) {
  console.error("usage: verify-gate.mjs <nuthatch-engine.js> <module.wasm> <nest dir> --hosted <url> <wallet>... [--mirror url] [--rpc url] [--storage dir] [--out file]");
  process.exit(2);
}
const { Engine, VERSION } = await import(path.resolve(bundle));

// Every GET, so the run can show none named a wallet or an indexer.
const gets = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, init) => {
  const res = await realFetch(url, init);
  if (!init?.method || init.method === "GET") gets.push(String(url));
  return res;
};

const ask = async (base, q) => {
  const r = await realFetch(new URL("sql", base.endsWith("/") ? base : `${base}/`), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ q }),
  });
  const body = await r.text();
  if (r.status !== 200) throw new Error(`the host answered ${r.status}: ${body.slice(0, 300)}`);
  return JSON.parse(body);
};

/// The four answers at one block: a following host moves on between requests, so ask again until
/// all four name the same block.
async function atOneBlock(base, named) {
  for (let i = 0; ; i++) {
    const answers = {};
    for (const [name, q] of Object.entries(named)) answers[name] = await ask(base, q);
    const blocks = new Set(Object.values(answers).map((a) => a.provenance?.as_of));
    if (blocks.size === 1) return answers;
    if (i >= 10) throw new Error(`the host's answers did not settle on one block: ${[...blocks].join(", ")}`);
  }
}

// The test proxy: forwards `POST /sql` to the host and, for the one statement it is told to, alters
// the first row's value of one column before answering.
const tamper = { statement: null, column: null, value: null };
const proxy = createServer(async (req, res) => {
  let body = "";
  for await (const chunk of req) body += chunk;
  const upstream = await realFetch(new URL("sql", hosted.endsWith("/") ? hosted : `${hosted}/`), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
  let text = await upstream.text();
  if (upstream.status === 200 && JSON.parse(body).q === tamper.statement) {
    const a = JSON.parse(text);
    a.rows[0][tamper.column] = tamper.value;
    text = JSON.stringify(a);
  }
  res.writeHead(upstream.status, { "content-type": "application/json" });
  res.end(text);
});
await new Promise((ok) => proxy.listen(0, "127.0.0.1", ok));
const proxied = `http://127.0.0.1:${proxy.address().port}/`;

const verifier = Engine.verifier({
  module: wasm,
  nest: nestDir,
  mirror: { base: mirror },
  rpc,
  storage,
  window: 10_000_000,
  log: (l, s) => l < 2 && console.error(`guest[${l}]: ${s}`),
});

const failures = [];
const report = { package: VERSION, hosted, mirror, rpc, wallets: [] };
let last = null;
for (const wallet of wallets) {
  const started = performance.now();
  const before = gets.length;
  // Which pools the stakes statement computes: the positions the host lists, as S7's page finds them.
  const first = await ask(hosted, delegatorStakesSql(wallet, []));
  const pools = poolsOf(first.rows);
  const named = statements(wallet, pools);
  const answers = await atOneBlock(hosted, named);
  const filters = filtersFor(wallet, pools);
  const items = Object.entries(named).map(([name, statement]) => ({ name, statement, answer: answers[name] }));
  const result = await verifier.verify(items, null, filters);
  const columns = result.statements.flatMap((s) => s.columns ?? []);
  const line = {
    wallet, block: result.as_of, nid: result.nid, source: result.source, pools: pools.length,
    statements: Object.fromEntries(result.statements.map((s) => [s.name, s.verdict])),
    columns: columns.length,
    equal: columns.filter((c) => c.verdict === "equal").length,
    named: columns.filter((c) => c.verdict === "not computable").map((c) => c.column),
    rows: result.statements.reduce((n, s) => n + (s.rows?.here ?? 0), 0),
    mirror_bytes: result.mirror?.reused ? 0 : result.mirror?.bytes,
    rpc_requests: result.requests,
    gets: gets.length - before,
    s: Math.round((performance.now() - started) / 100) / 10,
  };
  console.log(JSON.stringify(line));
  report.wallets.push({ ...line, result });
  for (const s of result.statements) {
    if (s.verdict !== "equal") failures.push(`${wallet} ${s.name}: ${s.verdict} ${JSON.stringify(s.reason ?? s.columns?.filter((c) => c.verdict === "differs"))}`);
  }
  last = { wallet, filters, named, as_of: result.as_of };
}

// A network-wide statement, through the last wallet's filters, at whatever block the host is at now.
const net = await ask(hosted, NETWORK_WIDE);
{
  const r = await verifier.verify(net, NETWORK_WIDE, last.filters);
  const s = r.statements[0];
  console.log(JSON.stringify({ check: "network-wide", block: r.as_of, verdict: s.verdict, reason: s.reason }));
  report.network_wide = r;
  if (s.verdict !== "outside the filters") failures.push(`the network-wide statement came back ${s.verdict}`);
  // A table no filter names, which the narrowed nest holds none of.
  const epochs = await ask(hosted, UNFILTERED);
  const e = (await verifier.verify(epochs, UNFILTERED, last.filters)).statements[0];
  console.log(JSON.stringify({ check: "unfiltered table", verdict: e.verdict, reason: e.reason }));
  report.unfiltered = e;
  if (e.verdict !== "outside the filters" || !/no filter covers/.test(e.reason)) {
    failures.push(`a statement over a table no filter names came back ${e.verdict}`);
  }
}

// One value altered by the proxy: the curator totals' first figure, or the stakes' for a delegator.
const target = (await ask(hosted, last.named.curator)).rows.length ? "curator" : "delegator";
const column = target === "curator" ? "total_signalled_tokens" : "total_staked_tokens";
tamper.statement = last.named[target];
tamper.column = column;
const honest = await ask(hosted, tamper.statement);
tamper.value = `${honest.rows[0][column]}1`;
let altered;
for (let i = 0; i < 10; i++) {
  altered = await ask(proxied, tamper.statement);
  if (altered.provenance.as_of === honest.provenance.as_of || i === 9) break;
}
const t = await verifier.verify(altered, tamper.statement, last.filters);
const ts = t.statements[0];
const hit = ts.columns?.find((c) => c.column === column);
console.log(JSON.stringify({ check: "altered", statement: target, column, verdict: ts.verdict, values: hit?.values }));
report.altered = t;
const others = (ts.columns ?? []).filter((c) => c.column !== column && c.verdict !== "equal");
if (ts.verdict !== "differs" || hit?.verdict !== "differs" || hit.values[0].server !== tamper.value || others.length) {
  failures.push(`the altered answer came back ${ts.verdict}, ${JSON.stringify(hit)}`);
}

// An answer naming another nest: refused by name, before anything is asked of the mirror or the RPC.
{
  verifier.close();
  const other = structuredClone(honest);
  other.provenance.nid = "0".repeat(64);
  const before = gets.length;
  let refused = null;
  try {
    await verifier.verify(other, tamper.statement, last.filters);
  } catch (e) {
    refused = e.message;
  }
  console.log(JSON.stringify({ check: "another nid", refused, gets: gets.length - before }));
  if (!/not the nest that answered/.test(refused ?? "") || gets.length !== before) {
    failures.push(`an answer from another nest was not refused before any GET: ${refused}`);
  }
}

verifier.close();
proxy.close();
const named = gets.filter((u) => wallets.some((w) => u.toLowerCase().includes(w.slice(2).toLowerCase())));
if (named.length) failures.push(`${named.length} GETs named a wallet`);
report.gets = gets.length;
if (outFile) writeFileSync(outFile, JSON.stringify(report, null, 1));
if (failures.length) {
  for (const f of failures) console.error(`FAIL ${f}`);
  process.exit(1);
}
console.log(`gate: ${wallets.length} wallets' four statements equal or named, by NID; a network-wide statement outside the filters; an altered value named (${column})`);
