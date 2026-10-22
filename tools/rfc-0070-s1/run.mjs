// The wasm side of gate 2: the same replay as the native driver, compiled to wasm32-unknown-unknown and
// run in Node over the nest directory. Usage: node run.mjs <pkg-dir> <nest> <spec.toml> <out> [chunk-rows]
import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";

const [pkg, nest, specPath, out, chunkRows = "200000"] = process.argv.slice(2);
const require = createRequire(import.meta.url);
const { replay, wasmMemory } = require(path.resolve(pkg, "rfc_0070_s1.js"));

const fds = new Map();
const fd = (key) => {
  if (!fds.has(key)) fds.set(key, fs.openSync(path.join(nest, key), "r"));
  return fds.get(key);
};
const size = async (key) => fs.fstatSync(fd(key)).size;
const read = async (key, offset, length) => {
  const buf = Buffer.alloc(length);
  fs.readSync(fd(key), buf, 0, length, offset);
  return new Uint8Array(buf.buffer, buf.byteOffset, length);
};

// The binary's input walk (`blob::collect_files`), as the native driver's `is_input`.
const derived = new Set(["nuthatch.redb", "segments", ".git", ".DS_Store"]);
const inputs = {};
const walk = (rel) => {
  for (const e of fs.readdirSync(path.join(nest, rel), { withFileTypes: true })) {
    const p = rel ? `${rel}/${e.name}` : e.name;
    if (derived.has(e.name) || (!rel && e.name === "maintained")) continue;
    if (e.isDirectory()) walk(p);
    else if (e.isFile()) inputs[p] = fs.readFileSync(path.join(nest, p)).toString("hex");
  }
};
walk("");

const spec = fs.readFileSync(specPath, "utf8");
const manifest = fs.readFileSync(path.join(nest, "segments/manifest.json"), "utf8");
const schema = fs.readFileSync(path.join(nest, "schema.json"), "utf8");
const t = performance.now();
const got = JSON.parse(await replay(size, read, spec, manifest, schema, JSON.stringify(inputs), Number(chunkRows)));
const wall = (performance.now() - t) / 1e3;
fs.mkdirSync(out, { recursive: true });
fs.writeFileSync(path.join(out, "frames.jsonl"), got.jsonl);
fs.writeFileSync(path.join(out, "manifest.json"), got.manifest);
delete got.jsonl;
delete got.manifest;
got.wall_secs = wall;
got.wasm_memory_mib = wasmMemory().buffer.byteLength / 2 ** 20;
got.node_max_rss_mib = process.resourceUsage().maxRSS / 1024;
console.log(JSON.stringify(got));
