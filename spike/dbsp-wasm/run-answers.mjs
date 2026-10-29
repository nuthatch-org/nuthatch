// RFC-0075 S5's gate, the tab's half in Node: the wasm build follows the recorded young contract with
// S4's entities and views, then answers each request in `s5/requests.json` through
// `LiveNest.request`, and every status, content type and body must equal the node's in
// `s5-expected.json`, byte for byte, masked as `tests/e2e_tab_answers.rs` masks.
// Usage: node run-answers.mjs <pkg-dir> <rpc-url> <tab-parity-dir>
//   (fixture-rpc recorded --fork --file <tab-parity-dir>/chain.json.gz serving <rpc-url>)
import { createRequire } from "node:module";
import { readFileSync, readdirSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import path from "node:path";
import { differences } from "./mask.mjs";

const require = createRequire(import.meta.url);
const [pkg, rpc, dir] = process.argv.slice(2);
const mod = require(path.join(path.resolve(pkg), "dbsp_wasm_spike.js"));
const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
const requests = JSON.parse(readFileSync(path.join(dir, "s5", "requests.json")));
const expected = JSON.parse(readFileSync(path.join(dir, "s5-expected.json")));
const start = Number(BigInt(chain.headers[0][0])) + 1;

const nestDir = path.join(dir, "s4");
const files = { "entities.toml": readFileSync(path.join(nestDir, "entities.toml"), "utf8") };
for (const sub of ["entities", "views"]) {
  for (const f of readdirSync(path.join(nestDir, sub)).sort()) {
    files[`${sub}/${f}`] = readFileSync(path.join(nestDir, sub, f), "utf8");
  }
}

const t0 = performance.now();
const nest = await mod.LiveNest.open("tab-answers", rpc, chain.contract, BigInt(start));
nest.derive(JSON.stringify(files));
nest.setPollInterval(1n);
await nest.follow();
const t1 = performance.now();

const got = [];
const timings = [];
for (const req of requests) {
  const body = req.body === undefined ? new Uint8Array() : new TextEncoder().encode(req.body);
  const a0 = performance.now();
  const a = await nest.request(req.method, req.path, body);
  timings.push(performance.now() - a0);
  got.push({
    method: req.method, path: req.path, status: a.status, content_type: a.contentType, body: a.text,
  });
}
timings.sort((a, b) => a - b);
console.log(JSON.stringify({
  follow_ms: Math.round(t1 - t0),
  requests: requests.length,
  answer_ms_p50: +timings[Math.floor(timings.length / 2)].toFixed(1),
  answer_ms_max: +timings[timings.length - 1].toFixed(1),
  wasm_memory_mib: +(mod.wasm_memory_bytes() / 2 ** 20).toFixed(1),
}));
const failures = differences(got, expected);
if (failures.length) {
  for (const f of failures) console.error(`MISMATCH ${f}`);
  console.error(`${failures.length} of ${requests.length} answers differ from the node's`);
  process.exit(1);
}
console.log(`gate: all ${requests.length} answers byte-identical to the node's`);
