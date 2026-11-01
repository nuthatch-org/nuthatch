// RFC-0081 S4's gate: completeness for a narrowed nest under RFC-0077 S5's verified head, on RFC-0075
// S3's recording. The nest is S4's (`s4-nest`) narrowed to every Transfer, so it holds what the node
// does. Three fixture-rpc instances serve the recording with whole headers and the receipts
// `record-receipts.sh` took: one honest, one leaving block <drop>'s one log out of every `eth_getLogs`
// answer, and one doing that and serving the same block's receipts without it.
//
// - Honest, `complete`: what a run without `complete` holds, every admitted block's receipts hashed.
// - The log dropped, no `complete`: one row short, which is what the gate guards against.
// - The log dropped, `complete`: the log found from the block's receipts, and every row the same.
// - The receipts forged as well: sealing stops by name at the block.
// - A range past `max_headers`: refused with the figure, at open before any request when `until`
//   bounds it, and at the follow before any RPC request when it does not.
//
// Without `--until` the nest follows to the tip and each sealed segment must be the node's
// `c983470c…479aeae`; that needs receipts for every block the bloom admits up to the verified head,
// 966 of them. With `--until N` it indexes to block N, which CI runs on the few
// receipts that range needs, and seals the empty blocks before its first log.
//
// Usage: node complete.mjs <module.wasm> <tab-parity-dir> <honest-url> <dropped-url> <forged-url>
//          <drop-block> [--until N]
import { readFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import path from "node:path";
import { fetchRpc, GuestFault, load } from "../browser/host.js";
import { nestFiles } from "../../mirror.mjs";

const args = process.argv.slice(2);
const at = args.indexOf("--until");
const until = at < 0 ? undefined : Number(args.splice(at, 2)[1]);
const [wasm, dir, honestUrl, droppedUrl, forgedUrl, dropArg] = args;
if (!dropArg) {
  console.error("usage: complete.mjs <module.wasm> <tab-parity-dir> <honest-url> <dropped-url> <forged-url> <drop-block> [--until N]");
  process.exit(2);
}
const drop = Number(dropArg);
const num = (h) => Number(BigInt(h));
const headers = JSON.parse(gunzipSync(readFileSync(path.join(dir, "headers.json.gz"))));
const expected = JSON.parse(readFileSync(path.join(dir, "expected.json")));
const top = { number: num(headers.at(-1).number), hash: headers.at(-1).hash };
const files = nestFiles(path.join(dir, "s4-nest"));
const start = 26_239_807;
const filters = [{ contract: "tab", event: "Transfer" }];
const bytes = readFileSync(wasm);
const node = Object.values(expected.segments).flat().filter((x) => x.to_block <= top.number).map((x) => x.hash);
const ROWS = "SELECT * FROM tab__transfer ORDER BY block_number, log_index";

async function run(name, url, config) {
  let rpcRequests = 0;
  let asks = 0;
  const post = fetchRpc({ main: url });
  const engine = await load(bytes, {
    storage: null,
    rpc: (r) => {
      rpcRequests++;
      return post(r);
    },
    log: (level, s) => level < 2 && console.error(`guest[${level}]: ${s}`),
    verified: async () => {
      asks++;
      return top;
    },
  });
  const counts = () => ({ rpc: rpcRequests, verified_finalized: asks });
  const t = performance.now();
  try {
    engine.open({ nid: `complete-${name}`, files, rpc: ["main"], verified: true, filters,
      ...(until === undefined ? {} : { until }), ...config });
    const stats = await engine.run();
    const r = engine.report();
    const a = engine.request("GET", `/sql?q=${encodeURIComponent(ROWS)}`);
    if (a.status !== 200) throw new Error(`${ROWS}: ${a.status} ${a.body}`);
    return { r, stats, rows: JSON.parse(a.body).rows, ms: Math.round(performance.now() - t), counts: counts() };
  } catch (e) {
    if (e instanceof GuestFault) return { fault: e, counts: counts() };
    throw e;
  }
}

const cases = [];
let failed = 0;
async function gate(name, url, config, check) {
  const out = await run(name, url, config);
  const why = check(out);
  const ok = why === true;
  if (!ok) failed++;
  const r = out.r;
  cases.push({ case: name, ok, ms: out.ms, fault: out.fault?.message, requests: out.counts, rows: out.rows?.length,
    sealed_through: r?.sealed_through, segments: r && Object.values(r.segments).flat().map((x) => x.hash.slice(0, 8)),
    unverified: out.stats?.unverified ?? null, complete: r?.verified?.complete, headers: r?.verified?.headers });
  console.log(`${ok ? "ok" : "FAIL"} - ${name}${ok ? "" : `: ${why}`}`);
  return out;
}
const expect = (cond, why) => (cond ? true : why);
const all = (...checks) => checks.find((c) => c !== true) ?? true;
const segs = (r) => Object.values(r.segments).flat().map((x) => x.hash);
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const atDrop = (rows) => rows.filter((x) => Number(x.block_number) === drop).length;

const plain = await gate("verified, without complete: the rows every other case is held to", honestUrl, {},
  ({ rows, stats, fault }) => all(
    expect(!fault, `faulted: ${fault?.message}`),
    expect(!stats.unverified, `sealing stopped: ${stats.unverified}`),
    expect(atDrop(rows) === 1, `${atDrop(rows)} rows at block ${drop}`),
  ));
const base = plain.rows ?? [];
// Followed to the tip, what seals must be the node's; bounded by `until`, what the run without it sealed.
const sealed = ({ r }) => until !== undefined
  ? expect(r.sealed_through === plain.r.sealed_through, `sealed through ${r.sealed_through}, not ${plain.r.sealed_through}`)
  : all(
    expect(same(segs(r), node) && node.length > 0, `segments ${segs(r)} against the node's ${node}`),
    expect(r.sealed_through !== null && r.sealed_through <= top.number, `sealed through ${r.sealed_through}`),
    expect(same(r.balances, expected.balances), "balances differ from the node's"),
  );
const holds = (o) => all(
  expect(!o.fault, `faulted: ${o.fault?.message}`),
  expect(!o.stats.unverified, `sealing stopped: ${o.stats.unverified}`),
  expect(same(o.rows, base), `${o.rows.length} rows, not the ${base.length} without complete`),
  sealed(o),
);
const through = until ?? top.number;

await gate("honest, complete: every admitted block's receipts hash to its root, and nothing changes", honestUrl,
  { complete: true }, (o) => all(
    holds(o),
    expect(o.r.verified.complete.found === 0, `${o.r.verified.complete.found} logs found that the RPC gave already`),
    expect(o.r.verified.complete.receipts > 0, "no bloom admitted a block the RPC gave no log for"),
    expect(o.r.verified.complete.highest_checked >= through, `checked through ${o.r.verified.complete.highest_checked}`),
  ));

await gate(`block ${drop}'s log dropped, without complete: a row short`, droppedUrl, {}, ({ r, rows, fault }) => all(
  expect(!fault, `faulted: ${fault?.message}`),
  expect(rows.length === base.length - 1 && atDrop(rows) === 0, `${rows.length} rows, ${atDrop(rows)} at block ${drop}`),
  expect(until !== undefined || (segs(r).length === 1 && segs(r)[0] !== node[0]), `segments ${segs(r)}`),
));

await gate(`block ${drop}'s log dropped, complete: found from its receipts, and nothing differs`, droppedUrl,
  { complete: true }, (o) => all(
    holds(o),
    expect(o.r.verified.complete.found === 1, `${o.r.verified.complete.found} logs found from receipts`),
  ));

const root = headers.find((h) => num(h.number) === drop).receiptsRoot;
await gate(`block ${drop}'s log dropped and its receipts forged: sealing stops by name`, forgedUrl,
  { complete: true }, ({ r, stats, fault }) => all(
    expect(!fault, `faulted: ${fault?.message}`),
    expect(new RegExp(`^block ${drop}'s receipts hash to 0x[0-9a-f]{64}, not the receiptsRoot ${root} its verified header gives$`).test(stats.unverified ?? ""),
      `unverified: ${stats.unverified}`),
    expect(r.sealed_through === null || r.sealed_through < drop, `sealed through ${r.sealed_through}`),
    expect(Object.values(r.segments).flat().every((x) => x.to_block < drop), "a segment holds the forged block"),
  ));

const need = top.number - start + 1;
await gate(`until ${top.number} past a ceiling of 1,000 headers: refused at open, before any request`, honestUrl,
  { complete: true, until: top.number, max_headers: 1000 }, ({ fault, counts }) => all(
    expect(fault?.fault === "bad_config", `fault ${fault?.message}`),
    expect(fault?.message.includes(`at least ${need} headers, blocks ${start}..=${top.number}, past the host's ceiling of 1000`), `message ${fault?.message}`),
    expect(counts.rpc === 0 && counts.verified_finalized === 0, `requests ${JSON.stringify(counts)}`),
  ));

await gate("a verified head past a ceiling of 1,000 headers: the follow refused before any RPC request", honestUrl,
  { complete: true, max_headers: 1000, until: undefined }, ({ fault, counts }) => all(
    expect(fault?.fault === "follow_failed", `fault ${fault?.message}`),
    expect(fault?.message.includes(`would hash and test ${need} headers, blocks ${start}..=${top.number}, past the host's ceiling of 1000`), `message ${fault?.message}`),
    expect(counts.rpc === 0, `requests ${JSON.stringify(counts)}`),
  ));

console.log(JSON.stringify({ top, drop, until: until ?? null, cases }));
if (failed) {
  console.error(`RESULT: FAIL (${failed} of ${cases.length} cases)`);
  process.exit(1);
}
console.log(`RESULT: PASS (${cases.length} cases against the module)`);
