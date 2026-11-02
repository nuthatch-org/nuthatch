// RFC-0081 S1's gates on a fixture, in Node: RFC-0077 S4's entity nest and its published directory,
// followed on RFC-0075 S3's recording.
//
// - A `mirror+rpc` nest, narrowed by public filters, answers the filtered table exactly as an `rpc`
//   nest narrowed by the same filters does: the filters are a predicate over what was hydrated.
// - The privacy rule, by the host's own requests: every GET is logged, none names a filter's value,
//   and two opens whose filters differ only in their values ask the mirror for the same URLs.
// - A private filter on a `mirror` or `mirror+rpc` nest is refused by name before any GET.
// - `attach`: the `rpc` nest's Arrow answers bound into the hybrid nest's statement equal the hybrid
//   nest's own, a HUGEINT sum included; an answer from another block is refused by name.
//
// Usage: node hybrid-gates.mjs <module.wasm> <nest-dir> <mirror-dir> <rpc url>
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fetchRpc, load } from "../browser/host.js";
import { fsStorage } from "./fs-storage.mjs";
import { nestFiles, serveDirectory } from "../../mirror.mjs";

const [wasm, nestDir, mirrorDir, rpcUrl] = process.argv.slice(2);
if (!wasm || !nestDir || !mirrorDir || !rpcUrl) {
  console.error("usage: hybrid-gates.mjs <module.wasm> <nest-dir> <mirror-dir> <rpc url>");
  process.exit(2);
}
// Two receivers with transfers in the sealed history the mirror holds.
const A = "0x37c9ef5d0801abdc25d9bbed096a080fa3c54e4c";
const B = "0xcee939daf4981e0eea8ffe9e41bde3d28ca77964";
const TABLE = "tab__transfer";

const bytes = readFileSync(wasm);
const files = nestFiles(nestDir);
const served = await serveDirectory(path.resolve(mirrorDir));
const root = mkdtempSync(path.join(tmpdir(), "nest-hybrid-gates-"));

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

// The recording answers topic0 filters only, so the narrowed topics are dropped on the way to it, as an
// endpoint that ignores them would: the module keeps only the logs its filters asked for.
const post = fetchRpc({ main: rpcUrl });
const recorded = (r) => {
  const body = JSON.parse(r.body);
  for (const call of Array.isArray(body) ? body : [body]) {
    if (call.method === "eth_getLogs") for (const f of call.params) f.topics = f.topics?.slice(0, 1);
  }
  return post({ ...r, body: JSON.stringify(body) });
};

let opened = 0;
async function open(config) {
  const e = await load(bytes, {
    storage: fsStorage(path.join(root, `n${opened++}`)),
    rpc: recorded,
    log: (l, s) => l < 2 && console.error(`guest[${l}]: ${s}`),
  });
  const before = gets.length;
  e.open({ nid: `gate-${opened}`, rpc: ["main"], storage: "host", fresh: true, files, ...config });
  await e.run();
  e.gets = gets.slice(before);
  return e;
}

const filter = (to, privacy) => [
  { contract: "tab", event: "Transfer", args: { to: [to] }, ...(privacy ? { privacy } : {}) },
];
const sql = (e, q) => {
  const a = e.request("GET", `/sql?q=${encodeURIComponent(q)}`);
  if (a.status !== 200) throw new Error(`${q}: ${a.status} ${a.body}`);
  return JSON.parse(a.body);
};
const ROWS = `SELECT * FROM ${TABLE} ORDER BY block_number, log_index`;

// The filter as a predicate: hybrid against rpc, both narrowed by `to = A`.
const hybridA = await open({ source: "mirror+rpc", mirror: { base: served.base }, filters: filter(A, "public") });
const rpcA = await open({ source: "rpc", filters: filter(A) });
const h = sql(hybridA, ROWS);
const r = sql(rpcA, ROWS);
check(
  h.rows.length > 0 && JSON.stringify(h.rows) === JSON.stringify(r.rows),
  `mirror+rpc and rpc narrowed to one receiver answer the same ${h.rows.length} rows at block ${h.provenance.as_of}`,
);
check(
  h.provenance.as_of === r.provenance.as_of && hybridA.gets.length > 0 && rpcA.gets.length === 0,
  `both at block ${h.provenance.as_of}; ${hybridA.gets.length} GETs for the hybrid nest, none for the rpc nest`,
);

// The privacy rule, read off the host's own requests.
const hybridB = await open({ source: "mirror+rpc", mirror: { base: served.base }, filters: filter(B, "public") });
const named = [...hybridA.gets, ...hybridB.gets].filter((u) =>
  [A, B].some((v) => u.toLowerCase().includes(v.slice(2))),
);
check(named.length === 0, `no GET names a filter's value (${hybridA.gets.length + hybridB.gets.length} GETs)`);
const sorted = (us) => JSON.stringify([...us].sort());
check(
  sorted(hybridA.gets) === sorted(hybridB.gets),
  "two opens whose filters differ only in their values ask the mirror for the same URLs",
);

// A private filter decides nothing a mirror is asked: refused by name, before any GET.
for (const source of ["mirror", "mirror+rpc"]) {
  const before = gets.length;
  let fault;
  try {
    await open({ source, mirror: { base: served.base }, filters: filter(A) });
  } catch (e) {
    fault = e;
  }
  check(
    fault?.fault === "bad_config" && /filter on `tab.Transfer` is private/.test(fault.message) && gets.length === before,
    `a private filter on a \`${source}\` nest is refused by name before any GET: ${fault?.message}`,
  );
}

// `attach`: the rpc nest's answers, as Arrow, in the hybrid nest's statement.
const arrow = (e, q) => {
  const a = e.request("GET", `/sql?q=${encodeURIComponent(q)}&format=arrow`);
  if (a.status !== 200 || !a.body_base64) throw new Error(`${q}: ${a.status} ${a.body}`);
  return { arrow: a.body_base64, answer: a.headers["x-nuthatch-answer"] };
};
const TOTAL = `SELECT COUNT(*) AS n, SUM(CAST(value AS HUGEINT)) AS total FROM ${TABLE}`;
const attached = await open({
  source: "mirror+rpc",
  mirror: { base: served.base },
  filters: filter(A, "public"),
  attach: [{ name: "other", tables: ["moved", "total"] }],
});
const ask = (e, q, attach) => e.request("POST", "/sql", JSON.stringify({ q, attach }));
const given = { other: { moved: arrow(rpcA, `SELECT * FROM ${TABLE}`), total: arrow(rpcA, TOTAL) } };
const both = ask(
  attached,
  `SELECT (SELECT COUNT(*) FROM other.moved) AS n, (SELECT total + 1 FROM other.total) AS total_plus_one, \
(SELECT typeof(total) FROM other.total) AS type`,
  given,
);
const own = sql(attached, `SELECT n, total + 1 AS total_plus_one FROM (${TOTAL})`).rows[0];
const theirs = both.status === 200 ? JSON.parse(both.body).rows[0] : null;
check(
  theirs && String(theirs.n) === String(own.n) && String(theirs.total_plus_one) === String(own.total_plus_one),
  `another nest's answers attached as Arrow: ${JSON.stringify(theirs)} against its own ${JSON.stringify(own)}`,
);
const bare = ask(attached, "SELECT COUNT(*) FROM moved", given);
check(bare.status === 400 && /name it `other.moved`/.test(bare.body), `a bare attached name is refused: ${bare.body}`);

const earlier = await open({ source: "rpc", filters: filter(A), until: h.provenance.as_of - 50 });
const stale = ask(attached, "SELECT COUNT(*) AS n FROM other.moved", {
  other: { moved: arrow(earlier, `SELECT * FROM ${TABLE}`) },
});
check(
  stale.status === 400 && /`other.moved` is the other nest's answer as of block \d+, and this nest answers as of block \d+/.test(stale.body),
  `an attachment from another block is refused by name: ${stale.body}`,
);

served.server.close();
process.exit(failed ? 1 : 0);
