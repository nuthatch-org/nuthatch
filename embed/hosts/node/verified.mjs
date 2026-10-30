// RFC-0077 S5's gate for the verified head: RFC-0065 S5's cases, re-run against the module through the
// Node host. fixture-rpc serves RFC-0075 S3's recording with whole headers up to block 26,241,700
// (`--headers headers.json.gz`); the host plays the light client, and tampers with what the RPC
// answers as each case says. With the honest head the module seals at or below it, and what it seals
// is the node's; with anything forged or off the verified chain, sealing stops and says why.
// Usage: node verified.mjs <module.wasm> <rpc-url> <tab-parity-dir>
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import path from "node:path";
import { fetchRpc, load } from "../browser/host.js";

const [wasm, rpcUrl, dir] = process.argv.slice(2);
if (!dir) {
  console.error("usage: verified.mjs <module.wasm> <rpc-url> <tab-parity-dir>");
  process.exit(2);
}
const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
const headers = JSON.parse(gunzipSync(readFileSync(path.join(dir, "headers.json.gz"))));
const expected = JSON.parse(readFileSync(path.join(dir, "expected.json")));
const start = Number(BigInt(chain.headers[0][0])) + 1;
const num = (h) => Number(BigInt(h));
const top = { number: num(headers.at(-1).number), hash: headers.at(-1).hash };
const logBlocks = new Set(chain.logs.map((l) => num(l.blockNumber)));
// A block whose header is tampered with carries no row, so the cursor itself never needs its timestamp.
let quiet = 26_240_500;
while (logBlocks.has(quiet)) quiet++;
const offChain = [...logBlocks].sort((a, b) => a - b).find((b) => b >= 26_240_100);
const bytes = readFileSync(wasm);

/// The RPC as `tamper` rewrites each reply: `(method, params, result) => result`.
function rpc(tamper) {
  const base = fetchRpc({ main: rpcUrl });
  if (!tamper) return base;
  return async (r) => {
    const a = await base(r);
    if (a.error || a.status !== 200) return a;
    const req = JSON.parse(r.body);
    const reply = JSON.parse(a.body);
    const one = (q, p) => (p && "result" in p ? { ...p, result: tamper(q.method, q.params, p.result) } : p);
    const out = Array.isArray(req)
      ? reply.map((p) => one(req.find((q) => q.id === p.id), p))
      : one(req, reply);
    return { ...a, body: JSON.stringify(out) };
  };
}

async function run(name, { verified, tamper }) {
  const engine = await load(bytes, {
    storage: null,
    rpc: rpc(tamper),
    log: (level, s) => level < 2 && console.error(`guest[${level}]: ${s}`),
    verified,
  });
  engine.open({ nid: `verified-${name}`, contract: chain.contract, start, rpc: ["main"], verified: true });
  const t = performance.now();
  const stats = await engine.run();
  const r = engine.report();
  return { r, stats, ms: Math.round(performance.now() - t) };
}

const balances = (r) => JSON.stringify(r.balances) === JSON.stringify(expected.balances);
const nothingSealed = (r) => r.sealed_through === null && Object.keys(r.segments).length === 0;
const honest = async () => top;
const cases = [];
let failed = 0;
async function gate(name, opts, check) {
  const { r, stats, ms } = await run(name, opts);
  const why = check(r, stats);
  const ok = why === true;
  if (!ok) failed++;
  cases.push({ case: name, ok, ms, sealed_through: r.sealed_through, last_block: r.last_block,
    unverified: stats.unverified ?? null, headers: r.verified?.headers });
  console.log(`${ok ? "ok" : "FAIL"} - ${name}${ok ? "" : `: ${why}`}`);
}
const expect = (cond, why) => (cond ? true : why);
const all = (...checks) => checks.find((c) => c !== true) ?? true;

await gate("headers that hash down from the verified block seal at or below it, as the node does", { verified: honest }, (r, s) => {
  const segs = Object.values(r.segments).flat();
  const nodes = Object.values(expected.segments).flat().filter((x) => x.to_block <= top.number);
  return all(
    expect(!s.unverified, `sealing stopped: ${s.unverified}`),
    expect(r.sealed_through !== null && r.sealed_through <= top.number, `sealed through ${r.sealed_through}`),
    expect(JSON.stringify(segs.map((x) => x.hash)) === JSON.stringify(nodes.map((x) => x.hash)) && segs.length > 0,
      `segments ${segs.map((x) => x.hash)} against the node's ${nodes.map((x) => x.hash)}`),
    expect(r.verified.headers === top.number - start + 1, `${r.verified.headers} headers hashed`),
    expect(r.last_block === expected.last_block, `followed to ${r.last_block}`),
    expect(balances(r), "balances differ from the node's"),
  );
});

await gate("a finalized block the light client cannot give is not sealed below", {
  verified: async () => { throw new Error("out of sync"); },
}, (r, s) => all(
  expect(/no verified finalized block: out of sync/.test(s.unverified), `unverified: ${s.unverified}`),
  expect(nothingSealed(r), `sealed through ${r.sealed_through}`),
  expect(r.last_block === expected.last_block && balances(r), "the cursor did not carry on"),
));

await gate("a finalized hash the RPC's chain does not reach is refused", {
  verified: async () => ({ ...top, hash: `0x${"77".repeat(32)}` }),
}, (r, s) => all(
  expect(/the light client verified 0x7777/.test(s.unverified), `unverified: ${s.unverified}`),
  expect(nothingSealed(r), `sealed through ${r.sealed_through}`),
));

await gate(`a forged header (block ${quiet}'s receipts root) stops sealing`, {
  verified: honest,
  tamper: (m, p, res) => (m === "eth_getBlockByNumber" && res && num(res.number) === quiet
    ? { ...res, receiptsRoot: `0x${"11".repeat(32)}` } : res),
}, (r, s) => all(
  expect(new RegExp(`block ${quiet} hashes to 0x[0-9a-f]{64}, not the ${headers.find((h) => num(h.number) === quiet).hash}`).test(s.unverified),
    `unverified: ${s.unverified}`),
  expect(nothingSealed(r), `sealed through ${r.sealed_through}`),
  expect(balances(r), "balances differ from the node's"),
));

await gate(`a missing header (block ${quiet}) stops sealing`, {
  verified: honest,
  tamper: (m, p, res) => (m === "eth_getBlockByNumber" && num(p[0]) === quiet ? null : res),
}, (r, s) => all(
  expect(/header\(s\) missing/.test(s.unverified), `unverified: ${s.unverified}`),
  expect(nothingSealed(r), `sealed through ${r.sealed_through}`),
));

await gate(`a log off the verified chain (block ${offChain}) holds the seal below it`, {
  verified: honest,
  tamper: (m, p, res) => (m === "eth_getLogs" && Array.isArray(res)
    ? res.map((l) => (num(l.blockNumber) === offChain ? { ...l, blockHash: `0x${"99".repeat(32)}` } : l)) : res),
}, (r, s) => {
  const segs = Object.values(r.segments).flat();
  return all(
    expect(s.unverified === `the RPC's logs for block ${offChain} are not on the verified chain`, `unverified: ${s.unverified}`),
    expect(r.sealed_through === null || r.sealed_through < offChain, `sealed through ${r.sealed_through}`),
    expect(segs.every((x) => x.to_block < offChain), "a segment holds the block off the chain"),
  );
});

console.log(JSON.stringify({ top, quiet, off_chain: offChain, cases }));
if (failed) {
  console.error(`RESULT: FAIL (${failed} of ${cases.length} cases)`);
  process.exit(1);
}
console.log(`RESULT: PASS (${cases.length} cases against the module)`);
