// RFC-0075 S6's budget gate, in Node against the wasm build. At a phone's 600 MiB and a desktop's
// 2 GiB: entities declaring more state than the cursor's share are refused at admission, a statement
// past one statement's share is refused by name while the cursor follows the recorded contract, and
// the cursor still seals and serves exactly what the node does (S3's `expected.json`). A ceiling the
// module already holds more than stops the cursor at its first window.
// Usage: node run-budget.mjs <pkg-dir> <rpc-url> <tab-parity-dir>
//   (fixture-rpc recorded --file <tab-parity-dir>/chain.json.gz serving <rpc-url>)
import { createRequire } from "node:module";
import { readFileSync, readdirSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import path from "node:path";

const require = createRequire(import.meta.url);
const [pkg, rpc, dir] = process.argv.slice(2);
const mod = require(path.join(path.resolve(pkg), "dbsp_wasm_spike.js"));
const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
const expected = JSON.parse(readFileSync(path.join(dir, "expected.json")));
const start = BigInt(Number(BigInt(chain.headers[0][0])) + 1);

const nestDir = path.join(dir, "s4");
const files = { "entities.toml": readFileSync(path.join(nestDir, "entities.toml"), "utf8") };
for (const sub of ["entities", "views"]) {
  for (const f of readdirSync(path.join(nestDir, sub)).sort()) {
    files[`${sub}/${f}`] = readFileSync(path.join(nestDir, sub, f), "utf8");
  }
}
// 150,000 rows is ~466 MiB of declared state: inside a desktop's 1,536 MiB cursor share, past a
// phone's 450 MiB.
const heavy = { ...files, "entities.toml": files["entities.toml"].replace(/max_rows = \d+/, "max_rows = 150000") };
// 8 million distinct 100-byte group keys, past 512 MiB of hash table, as `lodestar_indexer_ledger`'s
// 963 MiB is past both shares.
const LODESTAR_CLASS = "SELECT k, count(*) AS n FROM (SELECT repeat('k', 100) || CAST(range AS VARCHAR) AS k FROM range(8000000)) GROUP BY k";

const canon = (v) => Array.isArray(v) ? v.map(canon)
  : v && typeof v === "object" ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])])) : v;
const failures = [];
const check = (ok, what) => { if (!ok) failures.push(what); };
const sql = (q) => `/sql?q=${encodeURIComponent(q)}`;

for (const [device, mib] of [["phone", 600], ["desktop", 2048]]) {
  const nest = await mod.LiveNest.open(`tab-budget-${device}`, rpc, chain.contract, start);
  nest.setMemoryCeiling(mib);
  let admitted = true, refusal = "";
  try { nest.derive(JSON.stringify(heavy)); } catch (e) { admitted = false; refusal = e.message; }
  check(admitted === (device === "desktop"), `${device}: 150,000 declared rows admitted=${admitted} ${refusal}`);
  check(admitted || refusal.startsWith("over the tab's memory budget"), `${device}: refusal not named: ${refusal}`);
  nest.derive(JSON.stringify(files));

  const t0 = performance.now();
  let refusedAt = 0, followedAt = 0;
  const following = nest.follow().then((r) => { followedAt = performance.now() - t0; return r; });
  const heavyAnswer = nest.request("GET", sql(LODESTAR_CLASS), new Uint8Array())
    .then((a) => { refusedAt = performance.now() - t0; return a; });
  const [got, a] = [JSON.parse(await following), await heavyAnswer];
  check(a.status === 400 && a.text.includes("Out of Memory Error"), `${device}: statement not refused: ${a.status} ${a.text.slice(0, 300)}`);
  const compared = Object.fromEntries(Object.keys(expected).map((k) => [k, got[k]]));
  check(JSON.stringify(canon(compared)) === JSON.stringify(canon(expected)), `${device}: the cursor's segments or balances differ from the node's`);
  const after = await nest.request("GET", sql("SELECT count(*) AS n FROM tab__transfer"), new Uint8Array());
  check(after.status === 200 && JSON.parse(after.text).rows[0].n === 2259, `${device}: a small statement after the refusal: ${after.text.slice(0, 200)}`);
  const memory = mod.wasm_memory_bytes();
  check(memory < mib * 2 ** 20, `${device}: ${memory} bytes of linear memory against ${mib} MiB`);
  console.log(JSON.stringify({
    device, ceiling_mib: mib, heavy_entities_admitted: admitted,
    statement_refused_ms: Math.round(refusedAt), followed_ms: Math.round(followedAt),
    segments: Object.values(got.segments).flat().length,
    wasm_memory_mib: +(memory / 2 ** 20).toFixed(1),
  }));
  nest.free();
}

const walled = await mod.LiveNest.open("tab-budget-wall", rpc, chain.contract, start);
walled.setMemoryCeiling(1);
let wall = "";
try { await walled.follow(); } catch (e) { wall = e.message; }
check(wall.startsWith("over the tab's memory budget: the module holds"), `a 1 MiB ceiling did not stop the cursor: ${wall || "it followed"}`);

if (failures.length) {
  for (const f of failures) console.error(`FAIL ${f}`);
  process.exit(1);
}
console.log("gate: admission, the statement's refusal and the wall held at 600 MiB and 2 GiB, with the cursor's answers the node's");
