// RFC-0077 S0's gate in Node: the plain module, instantiated with no wasm-bindgen glue, follows
// RFC-0075 S3's recording through a host that performs its requests, and must seal and report what
// the node pinned. Then RFC-0075 S5's requests, answered by `nh_request`, digested for comparison
// with the wasmtime host.
// Usage: node run-node.mjs <module.wasm> <rpc-url> <tab-parity-dir>
import { readFileSync } from "node:fs";
import { createHash, randomFillSync } from "node:crypto";
import { gunzipSync } from "node:zlib";
import path from "node:path";

const [wasm, rpc, dir] = process.argv.slice(2);
const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
const expected = JSON.parse(readFileSync(path.join(dir, "expected.json")));
const requests = JSON.parse(readFileSync(path.join(dir, "s5", "requests.json")));
const start = Number(BigInt(chain.headers[0][0])) + 1;

let mem;
const bytes = (ptr, len) => new Uint8Array(mem.buffer, ptr, len);
const text = new TextDecoder();
const imports = {
  nuthatch: {
    now_ms: () => Date.now(),
    random_fill: (ptr, len) => randomFillSync(bytes(ptr, len)),
    log: (level, ptr, len) => console.error(`guest[${level}]: ${text.decode(bytes(ptr, len))}`),
  },
};

const t0 = performance.now();
const { instance } = await WebAssembly.instantiate(readFileSync(wasm), imports);
const ms_instantiate = performance.now() - t0;
const g = instance.exports;
mem = g.memory;

function put(data) {
  const b = typeof data === "string" ? new TextEncoder().encode(data) : data;
  const ptr = g.nh_alloc(b.length);
  bytes(ptr, b.length).set(b);
  return [ptr, b.length];
}

function out(n) {
  const len = Math.abs(n);
  const ptr = g.nh_alloc(len);
  g.nh_output(ptr);
  const s = text.decode(bytes(ptr, len).slice());
  g.nh_free(ptr, len);
  if (n < 0) throw new Error(s);
  return s;
}

const call = (name, ...args) => JSON.parse(out(g[name](...args)));

console.error(JSON.stringify(call("nh_abi")));
call("nh_open", ...put(JSON.stringify({ nid: "tab-parity", contract: chain.contract, start, rpc: ["main"] })));

const endpoints = { main: rpc };
let posts = 0, steps = 0, waits = 0;
async function perform(r) {
  posts += 1;
  try {
    const res = await fetch(endpoints[r.endpoint], {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: r.body,
      signal: AbortSignal.timeout(r.deadline_ms),
    });
    return {
      status: res.status,
      status_text: res.statusText,
      retry_after: res.headers.get("retry-after"),
      body: await res.text(),
    };
  } catch (e) {
    return { error: String(e.cause ?? e.message), timed_out: e.name === "TimeoutError" };
  }
}

const t1 = performance.now();
for (;;) {
  steps += 1;
  const s = call("nh_step");
  if (s.idle) break;
  if (s.requests) {
    const answers = await Promise.all(s.requests.map(perform));
    call("nh_resume", ...put(JSON.stringify(answers)));
  } else if (s.wait_ms !== undefined) {
    waits += 1;
    await new Promise((r) => setTimeout(r, s.wait_ms));
  }
}
const ms_follow = performance.now() - t1;
const got = call("nh_report");

const digest = createHash("sha256");
let answered = 0;
for (const req of requests) {
  if (req.path.startsWith("/ready")) continue;
  const a = call("nh_request", ...put(req.method), ...put(req.path), ...put(req.body ?? ""));
  digest.update(`${req.method} ${req.path}\n${a.status}\n${a.content_type}\n${a.body}\n`);
  answered += 1;
}

const segs = Object.values(got.segments).flat();
console.log(JSON.stringify({
  host: "node",
  ms_instantiate: Math.round(ms_instantiate),
  ms_follow: Math.round(ms_follow),
  wasm_memory_mib: +(g.nh_memory_bytes() / 2 ** 20).toFixed(1),
  peak_rss_mib: +(process.resourceUsage().maxRSS / 1024).toFixed(1),
  steps, waits, rpc_posts: posts, rpc_requests: got.rpc_requests,
  segments: segs.length, hot_rows: got.hot_rows, holders: got.balances.holders,
  answers: answered, answers_sha256: digest.digest("hex"),
}));

const compared = Object.fromEntries(Object.keys(expected).map((k) => [k, got[k]]));
const canon = (v) => Array.isArray(v) ? v.map(canon)
  : v && typeof v === "object" ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])])) : v;
const misnamed = segs.filter((s) => s.hash !== s.bytes_hash);
if (JSON.stringify(canon(compared)) !== JSON.stringify(canon(expected)) || misnamed.length) {
  console.error(`MISMATCH\n got: ${JSON.stringify(compared)}\nwant: ${JSON.stringify(expected)}`);
  process.exit(1);
}
console.log(`gate: ${segs.length} segment hashes and ${got.balances.count} balances equal the node's`);
