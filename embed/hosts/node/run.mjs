// RFC-0077 S2: the browser host (`../browser/host.js`) in Node, over files, driving the plain module
// through `abi: 1`. Two gates, by the chain fixture-rpc serves:
//   s3  RFC-0075 S3's recording: the segment hashes and 1,000 balances equal `expected.json`.
//   s5  the same with `--fork` and S4's entities and views: all 33 of RFC-0075 S5's answers equal
//       `s5-expected.json`, masked as `tests/e2e_tab_answers.rs` masks.
// `--restart` is RFC-0077 S3's: a child process follows from an empty store until a segment is
// sealed and is killed with requests outstanding, then this one reopens the same files and finishes.
// Prints one JSON line of what it measured, then the gate's verdict; exits non-zero on a mismatch.
// Usage: node run.mjs <module.wasm> <rpc-url> <tab-parity-dir> <s3|s5> [--storage bytes|host]
//          [--restart]
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { gunzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import path from "node:path";
import { fetchRpc, load, onceSealed } from "../browser/host.js";
import { fsStorage } from "./fs-storage.mjs";
import { answersDigest, differences, segmentsDigest } from "../../mask.mjs";

const [wasm, rpc, dir, gate, ...flags] = process.argv.slice(2);
const flag = (name) => {
  const at = flags.indexOf(name);
  return at < 0 ? undefined : flags[at + 1];
};
const storage = flag("--storage") ?? "host";
const restart = flags.includes("--restart");
// The child's half of `--restart`: the directory to follow into, then die.
const dying = flag("--die-mid-follow");
if (!["s3", "s5"].includes(gate) || !["bytes", "host"].includes(storage) || (restart && storage !== "host")) {
  console.error("usage: run.mjs <module.wasm> <rpc-url> <tab-parity-dir> <s3|s5> [--storage bytes|host] [--restart]");
  process.exit(2);
}
const chain = JSON.parse(gunzipSync(readFileSync(path.join(dir, "chain.json.gz"))));
const start = Number(BigInt(chain.headers[0][0])) + 1;

const files = {};
if (gate === "s5") {
  const nest = path.join(dir, "s4");
  files["entities.toml"] = readFileSync(path.join(nest, "entities.toml"), "utf8");
  for (const sub of ["entities", "views"]) {
    for (const f of readdirSync(path.join(nest, sub)).sort()) {
      files[`${sub}/${f}`] = readFileSync(path.join(nest, sub, f), "utf8");
    }
  }
}

const root = dying ?? mkdtempSync(path.join(tmpdir(), "nest-node-host-"));
let killed;
if (restart) {
  const script = new URL(import.meta.url).pathname;
  const child = spawnSync(process.execPath, [script, wasm, rpc, dir, gate, "--die-mid-follow", root], {
    stdio: ["ignore", "pipe", "inherit"], encoding: "utf8",
  });
  if (child.signal !== "SIGKILL") {
    console.error(`the follow to kill ended ${child.signal ?? `with exit ${child.status}`}, not by SIGKILL`);
    process.exit(1);
  }
  killed = JSON.parse(child.stdout.trim().split("\n").pop());
}

const bytes = readFileSync(wasm);
const t0 = performance.now();
const engine = await load(bytes, {
  storage: fsStorage(root),
  rpc: fetchRpc({ main: rpc }),
  log: (level, s) => console.error(`guest[${level}]: ${s}`),
});
const ms_instantiate = performance.now() - t0;
console.error(JSON.stringify(engine.abi()));
engine.open({
  nid: gate === "s5" ? "tab-answers" : "tab-parity", contract: chain.contract, start, rpc: ["main"],
  storage, ...(storage === "host" ? { fresh: !restart } : {}),
  ...(gate === "s5" ? { poll_interval_secs: 1, files } : {}),
});

const t1 = performance.now();
if (dying) {
  const s = await engine.run(onceSealed(engine));
  if (!s.stopped) {
    console.error("the follow reached the tip before anything sealed");
    process.exit(1);
  }
  const r = engine.report();
  // Written synchronously: a pipe's stdout may still be buffered when the signal lands.
  writeSync(1, `${JSON.stringify({ last_block: r.last_block, sealed_through: r.sealed_through, steps: s.steps })}\n`);
  process.kill(process.pid, "SIGKILL");
}
const stats = await engine.run();
const ms_follow = performance.now() - t1;
const got = engine.report();
const segs = Object.values(got.segments).flat();
const out = {
  host: "node", gate, storage, ...(restart ? { restart: true, killed_at: killed } : {}),
  module_sha256: createHash("sha256").update(bytes).digest("hex"),
  ms_instantiate: Math.round(ms_instantiate), ms_follow: Math.round(ms_follow),
  wasm_memory_mib: +(engine.memoryBytes() / 2 ** 20).toFixed(1),
  peak_rss_mib: +(process.resourceUsage().maxRSS / 1024).toFixed(1),
  ...stats, rpc_requests: got.rpc_requests, segments: segs.length, hot_rows: got.hot_rows,
  segments_sha256: segmentsDigest(got.segments),
};
if (storage === "bytes") out.export_bytes = engine.export().length;

const failures = [];
if (gate === "s3") {
  const expected = JSON.parse(readFileSync(path.join(dir, "expected.json")));
  const canon = (v) => Array.isArray(v) ? v.map(canon)
    : v && typeof v === "object" ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canon(v[k])])) : v;
  const compared = Object.fromEntries(Object.keys(expected).map((k) => [k, got[k]]));
  if (JSON.stringify(canon(compared)) !== JSON.stringify(canon(expected))) {
    failures.push(`report\n got: ${JSON.stringify(compared)}\nwant: ${JSON.stringify(expected)}`);
  }
  const misnamed = segs.filter((s) => s.hash !== s.bytes_hash);
  if (misnamed.length) failures.push(`${misnamed.length} segments named for other bytes`);
} else {
  const requests = JSON.parse(readFileSync(path.join(dir, "s5", "requests.json")));
  const expected = JSON.parse(readFileSync(path.join(dir, "s5-expected.json")));
  const answers = requests.map((req) => {
    const a = engine.request(req.method, req.path, req.body ?? "");
    return { method: req.method, path: req.path, status: a.status, content_type: a.content_type, body: a.body };
  });
  out.answers = answers.length;
  out.answers_sha256 = answersDigest(answers);
  out.pin_sha256 = answersDigest(expected, { pinned: true });
  failures.push(...differences(answers, expected));
}
console.log(JSON.stringify(out));
rmSync(root, { recursive: true, force: true });
if (failures.length) {
  for (const f of failures) console.error(`MISMATCH ${f}`);
  process.exit(1);
}
const after = restart ? ` after a kill at block ${killed.last_block}, sealed through ${killed.sealed_through}` : "";
console.log(gate === "s3"
  ? `gate: ${segs.length} segment hashes and ${got.balances.count} balances equal the node's${after}`
  : `gate: all ${out.answers} answers equal the node's pin (${out.answers_sha256})${after}`);
