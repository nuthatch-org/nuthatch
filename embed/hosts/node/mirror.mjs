// RFC-0077 S4: a nest opened from a published mirror in the browser host (`../browser/host.js`), in
// Node over files. The module hydrates the nest's sealed history through `get` requests, rebuilds its
// views and entities, optionally follows the chain, then answers `../../mirror.mjs`'s requests.
// Prints one JSON line of what it measured; with `--expect`, compares every answer with a report
// `run-mirror-node.mjs` wrote from a node seeded from the same mirror, and exits non-zero on a
// difference. `--refused` instead expects `nh_open` or the first step to refuse the nest, and passes
// only if it did so before asking the mirror for anything.
// Usage: node mirror.mjs <module.wasm> <nest-dir> <mirror: url or directory> [--rpc <url>]
//          [--memory-mib n] [--follow] [--postcard <cuts.json>] [--expect <report.json>]
//          [--out <report.json>] [--refused] [--max-rows n] [--postcard made|altered --dataset <id>]
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";
import { fetchRpc, load } from "../browser/host.js";
import { fsStorage } from "./fs-storage.mjs";
import { answersOf, differences, digest, nestFiles, postcardOf, serveDirectory } from "../../mirror.mjs";

const [wasm, nestDir, mirrorArg, ...flags] = process.argv.slice(2);
const flag = (name) => {
  const at = flags.indexOf(name);
  return at < 0 ? undefined : flags[at + 1];
};
if (!wasm || !nestDir || !mirrorArg) {
  console.error("usage: mirror.mjs <module.wasm> <nest-dir> <mirror> [--rpc <url>] [--memory-mib n] [--follow] [--postcard <file>] [--expect <file>] [--out <file>] [--refused]");
  process.exit(2);
}
const files = nestFiles(nestDir);
// The same nest declaring more entity state: entities.toml is not data, so the mirror still serves it.
if (flag("--max-rows")) files["entities.toml"] = files["entities.toml"].replace(/max_rows\s*=\s*\d+/g, `max_rows = ${flag("--max-rows")}`);
let base = mirrorArg;
let served;
if (!/^https?:\/\//.test(mirrorArg)) {
  served = await serveDirectory(path.resolve(mirrorArg));
  base = served.base;
}

// Every GET the module asks for, so a refusal can be shown to have come first.
const gets = [];
const realFetch = globalThis.fetch;
globalThis.fetch = (url, init) => {
  if (!init?.method || init.method === "GET") gets.push(String(url));
  return realFetch(url, init);
};

const bytes = readFileSync(wasm);
const root = mkdtempSync(path.join(tmpdir(), "nest-mirror-"));
const t0 = performance.now();
const engine = await load(bytes, {
  storage: fsStorage(root),
  rpc: fetchRpc({ main: flag("--rpc") ?? "http://127.0.0.1:1/" }),
  log: (level, s) => console.error(`guest[${level}]: ${s}`),
});
// `--postcard <file>`, or `made` for one made from the mirror's catalogue as it stands, or `altered` for
// that one with another digest, which must be refused before a segment is fetched.
const pin = flag("--postcard");
const postcard = pin === "made" || pin === "altered"
  ? await postcardOf(base, flag("--dataset"))
  : pin ? JSON.parse(readFileSync(pin, "utf8")) : undefined;
if (pin === "altered") postcard.digest = postcard.digest.replace(/^./, (c) => (c === "A" ? "B" : "A"));
const config = {
  nid: "mirror-gate",
  rpc: ["main"],
  storage: "host",
  fresh: true,
  files,
  mirror: { base, ...(postcard ? { postcard } : {}) },
  ...(flag("--memory-mib") ? { memory_mib: Number(flag("--memory-mib")) } : {}),
  ...(postcard ? {} : { follow: flags.includes("--follow") }),
};

const finish = (code) => {
  served?.server.close();
  process.exit(code);
};

if (flags.includes("--refused")) {
  let fault;
  try {
    engine.open(config);
    await engine.run();
  } catch (e) {
    fault = e;
  }
  const line = { refused: Boolean(fault), fault: fault?.fault, message: fault?.message, gets: gets.length };
  console.log(JSON.stringify(line));
  // A pin is checked against the catalogue, so a refused pin may read the snapshot, the catalogue and
  // schema.json; never a segment.
  const fetched = gets.filter((u) => u.endsWith(".parquet") || u.endsWith(".tail")).length;
  if (!fault || (pin ? fetched > 0 : gets.length > 0)) {
    console.error(fault ? `refused, but only after ${gets.length} GET(s) to the mirror` : "the nest was not refused");
    finish(1);
  }
  console.error("refused before anything was downloaded");
  finish(0);
}

engine.open(config);
const t1 = performance.now();
const stats = await engine.run();
const ms_open = performance.now() - t1;
const linear_open = engine.memoryBytes();
const report = engine.report();
const t2 = performance.now();
const answers = await answersOf(files, async (target) => engine.request("GET", target));
const ms_queries = performance.now() - t2;
const line = {
  module_sha256: createHash("sha256").update(bytes).digest("hex"),
  mirror: report.mirror,
  last_block: report.last_block,
  sealed_through: report.sealed_through,
  segments: Object.values(report.segments).reduce((n, s) => n + s.length, 0),
  steps: stats.steps,
  requests: stats.requests,
  gets: gets.length,
  ms_instantiate: Math.round(t1 - t0),
  ms_open: Math.round(ms_open),
  ms_queries: Math.round(ms_queries),
  answers: Object.keys(answers).length,
  answers_sha256: digest(answers),
  linear_memory_open_mib: Math.round(linear_open / 2 ** 20),
  linear_memory_mib: Math.round(engine.memoryBytes() / 2 ** 20),
  peak_rss_mib: Math.round(process.resourceUsage().maxRSS / 1024),
};
console.log(JSON.stringify(line));
if (flag("--out")) writeFileSync(flag("--out"), JSON.stringify({ head: line.last_block, answers }, null, 1));
if (flag("--expect")) {
  const want = JSON.parse(readFileSync(flag("--expect"), "utf8"));
  if (want.head !== line.last_block) {
    console.error(`the node's head is ${want.head} and the module's ${line.last_block}`);
    finish(1);
  }
  const diff = differences(want.answers, answers);
  if (diff.length) {
    for (const k of diff) {
      console.error(`differs: ${k}\n  node:   ${JSON.stringify(want.answers[k])?.slice(0, 600)}\n  module: ${JSON.stringify(answers[k])?.slice(0, 600)}`);
    }
    finish(1);
  }
  console.error(`all ${Object.keys(answers).length} answers equal the node's (digest ${digest(want.answers)})`);
}
finish(0);
