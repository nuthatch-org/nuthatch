// RFC-0075 S4's gate, the tab's half: the wasm build follows the recorded young contract with the
// nest's two entities and three views, takes the fixture's scripted reorg, and must answer
// `/derived/{entity}` and every view as the node does, before and after.
// Usage: node run-entities.mjs <pkg-dir> <rpc-url> <tab-parity-dir>
//   (fixture-rpc recorded --fork --file <tab-parity-dir>/chain.json.gz serving <rpc-url>)
import { createRequire } from "node:module";
import { readFileSync, readdirSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import path from "node:path";

const require = createRequire(import.meta.url);
const [pkg, rpc, dir] = process.argv.slice(2);
const mod = require(path.join(path.resolve(pkg), "dbsp_wasm_spike.js"));
const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
const expected = JSON.parse(readFileSync(path.join(dir, "s4-expected.json")));
const start = Number(BigInt(chain.headers[0][0])) + 1;

const nestDir = path.join(dir, "s4");
const files = { "entities.toml": readFileSync(path.join(nestDir, "entities.toml"), "utf8") };
for (const sub of ["entities", "views"]) {
  for (const f of readdirSync(path.join(nestDir, sub)).sort()) {
    files[`${sub}/${f}`] = readFileSync(path.join(nestDir, sub, f), "utf8");
  }
}

const canon = (v) => Array.isArray(v) ? v.map(canon)
  : v && typeof v === "object" ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])])) : v;
const check = (phase, got) => {
  const want = expected[phase];
  for (const k of Object.keys(want)) {
    if (JSON.stringify(canon(got[k])) !== JSON.stringify(canon(want[k]))) {
      const g = JSON.stringify(got[k]), w = JSON.stringify(want[k]);
      console.error(`MISMATCH ${phase}.${k}\n got: ${g.slice(0, 1500)}\nwant: ${w.slice(0, 1500)}`);
      process.exit(1);
    }
  }
};

const t0 = performance.now();
const nest = await mod.LiveNest.open("tab-entities", rpc, chain.contract, BigInt(start));
nest.derive(JSON.stringify(files));
await nest.follow();
const before = JSON.parse(await nest.answer());
const t1 = performance.now();
check("before", before);
const forked = await (await fetch(`${rpc}control/fork`, { method: "POST" })).json();
await nest.follow();
const after = JSON.parse(await nest.answer());
const t2 = performance.now();
check("after", after);

const rows = (r) => Object.fromEntries([
  ...Object.entries(r.derived).map(([k, v]) => [k, v.rows]),
  ...Object.entries(r.views).map(([k, v]) => [k, v.length]),
]);
console.log(JSON.stringify({
  before_ms: Math.round(t1 - t0), after_ms: Math.round(t2 - t1),
  wasm_memory_mib: +(mod.wasm_memory_bytes() / 2 ** 20).toFixed(1),
  fork: forked, before: rows(before), after: rows(after),
}));
console.log("gate: every entity and view row-identical to the node's, before and after the reorg");
