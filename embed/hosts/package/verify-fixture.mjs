// RFC-0081 S3's `verify` on a fixture, in Node, in CI: RFC-0077 S4's entity nest and its published
// directory, followed on RFC-0075 S3's recording. The "hosted" nest is the same nest opened whole from
// the mirror through the package, its answers named by the NID a node gives the same directory.
//
// - A statement about one receiver, checked through that receiver's filter: every column equal.
// - The same answer with one value altered: differs, on that column, with both values.
// - A statement over every transfer: outside the filters, not differing.
// - An answer naming another NID: refused by name before any GET.
//
// Usage: node verify-fixture.mjs <nuthatch-engine.js> <module.wasm> <nest-dir> <mirror-dir> <rpc url>
import { createServer } from "node:http";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { serveDirectory } from "../../mirror.mjs";

const [bundle, wasm, nestDir, mirrorDir, rpcUrl] = process.argv.slice(2);
if (!bundle || !wasm || !nestDir || !mirrorDir || !rpcUrl) {
  console.error("usage: verify-fixture.mjs <nuthatch-engine.js> <module.wasm> <nest-dir> <mirror-dir> <rpc url>");
  process.exit(2);
}
const { Engine } = await import(path.resolve(bundle));
const A = "0x37c9ef5d0801abdc25d9bbed096a080fa3c54e4c";
const served = await serveDirectory(path.resolve(mirrorDir));
const storage = mkdtempSync(path.join(tmpdir(), "nest-verify-fixture-"));

// The recording answers topic0 filters only; a narrowed nest's topics are dropped on the way to it, as
// an endpoint that ignores them would, and the module keeps only the logs its filters asked for.
const rpc = createServer(async (req, res) => {
  let raw = "";
  for await (const c of req) raw += c;
  const body = JSON.parse(raw);
  for (const call of Array.isArray(body) ? body : [body]) {
    if (call.method === "eth_getLogs") for (const f of call.params) f.topics = f.topics?.slice(0, 1);
  }
  const up = await fetch(rpcUrl, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  res.writeHead(up.status, { "content-type": "application/json" });
  res.end(await up.text());
});
await new Promise((ok) => rpc.listen(0, "127.0.0.1", ok));
const rpcProxy = `http://127.0.0.1:${rpc.address().port}/`;

const gets = [];
const realFetch = globalThis.fetch;
globalThis.fetch = (url, init) => {
  if (!init?.method || init.method === "GET") gets.push(String(url));
  return realFetch(url, init);
};

let failed = false;
const check = (ok, what) => {
  console.log(`${ok ? "pass" : "FAIL"}: ${what}`);
  if (!ok) failed = true;
};

const host = await Engine.open({ module: wasm, nest: nestDir, mirror: { base: served.base }, rpc: rpcProxy, storage });
const followed = await host.follow();
const nid = followed.identity.nid;
// The module names an answer by its store; a node names it by the nest's NID, which is what `verify` opens.
const hosted = async (q) => {
  const a = await host.sql(q);
  return { ...a, provenance: { ...a.provenance, nid } };
};

const ONE = `SELECT COUNT(*) AS n, CAST(SUM(CAST(value AS HUGEINT)) AS VARCHAR) AS total, MAX(block_number) AS last \
FROM tab__transfer WHERE LOWER("to") = '${A}'`;
const ALL = "SELECT COUNT(*) AS n, CAST(SUM(CAST(value AS HUGEINT)) AS VARCHAR) AS total FROM tab__transfer";
const filters = [{ contract: "tab", event: "Transfer", args: { to: [A] }, privacy: "public" }];
const one = await hosted(ONE);
const all = await hosted(ALL);
host.close();

const v = Engine.verifier({ module: wasm, nest: nestDir, mirror: { base: served.base }, rpc: rpcProxy, storage });
const r = await v.verify([{ name: "one", answer: one, statement: ONE }, { name: "all", answer: all, statement: ALL }], null, filters);
const [s1, s2] = r.statements;
check(
  r.nid === nid && r.source === "mirror+rpc" && s1.verdict === "equal" && s1.columns.every((c) => c.verdict === "equal") && Number(one.rows[0].n) > 0,
  `a statement within the filters, at block ${r.as_of}: ${s1.verdict}, ${s1.columns.map((c) => `${c.column} ${c.verdict}`).join(", ")} (${one.rows[0].n} transfers)`,
);
check(s2.verdict === "outside the filters", `a statement over every transfer: ${s2.verdict}, ${s2.reason}`);

const altered = structuredClone(one);
altered.rows[0].total = `${one.rows[0].total}7`;
const t = (await v.verify(altered, ONE, filters)).statements[0];
const hit = t.columns.find((c) => c.column === "total");
check(
  t.verdict === "differs" && hit.verdict === "differs" && hit.values[0].server === altered.rows[0].total &&
    hit.values[0].here === one.rows[0].total && t.columns.filter((c) => c.verdict !== "equal").length === 1,
  `one value altered: ${t.verdict}, ${JSON.stringify(hit)}`,
);

v.close();
const other = { ...one, provenance: { ...one.provenance, nid: "f".repeat(64) } };
const before = gets.length;
let refused = "";
try {
  await v.verify(other, ONE, filters);
} catch (e) {
  refused = e.message;
}
check(/not the nest that answered/.test(refused) && gets.length === before, `an answer from another nest, refused before any GET: ${refused}`);
v.close();

served.server.close();
rpc.close();
process.exit(failed ? 1 : 0);
