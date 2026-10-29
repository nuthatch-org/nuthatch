// RFC-0075 S3's gate, the tab's half: the wasm build follows the recorded young contract over HTTP,
// through nuthatch-core's RPC client on `fetch`, and must seal and serve what the node does.
// Usage: node run-live.mjs <pkg-dir> <rpc-url> <tab-parity-dir>
//   (fixture-rpc recorded --file <tab-parity-dir>/chain.json.gz serving <rpc-url>)
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import path from "node:path";

const require = createRequire(import.meta.url);
const [pkg, rpc, dir] = process.argv.slice(2);
const mod = require(path.join(path.resolve(pkg), "dbsp_wasm_spike.js"));
const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
const expected = JSON.parse(readFileSync(path.join(dir, "expected.json")));
const start = Number(BigInt(chain.headers[0][0])) + 1;

let sent = 0, received = 0, posts = 0;
const fetch0 = globalThis.fetch;
globalThis.fetch = async (url, init) => {
  posts += 1;
  sent += String(init?.body ?? "").length;
  const r = await fetch0(url, init);
  received += Number(r.headers.get("content-length") ?? 0);
  return r;
};

const t0 = performance.now();
const nest = new mod.LiveNest(rpc, chain.contract, BigInt(start));
const got = JSON.parse(await nest.follow());
const ms = performance.now() - t0;
const mem = mod.wasm_memory_bytes();

const compared = Object.fromEntries(Object.keys(expected).map((k) => [k, got[k]]));
const segs = Object.values(got.segments).flat();
console.log(JSON.stringify({
  wall_ms: Math.round(ms),
  wasm_memory_mib: +(mem / 2 ** 20).toFixed(1),
  rpc_posts: posts, rpc_requests: got.rpc_requests,
  bytes_sent: sent, bytes_received: received,
  segments: segs.length, segment_bytes: got.segment_bytes, hot_rows: got.hot_rows,
  holders: got.balances.holders,
}));
const canon = (v) => Array.isArray(v) ? v.map(canon)
  : v && typeof v === "object" ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])])) : v;
const mismatch = JSON.stringify(canon(compared)) !== JSON.stringify(canon(expected));
const misnamed = segs.filter((s) => s.hash !== s.bytes_hash);
if (mismatch || misnamed.length) {
  console.error(`MISMATCH\n got: ${JSON.stringify(compared)}\nwant: ${JSON.stringify(expected)}`);
  console.error(`first sealed rows: ${JSON.stringify(got.first_rows)}`);
  process.exit(1);
}
console.log(`gate: ${segs.length} segment hashes and ${got.balances.count} balances equal the node's`);
