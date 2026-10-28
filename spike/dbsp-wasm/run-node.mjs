// RFC-0075 S0: step a dbsp circuit in Node from the wasm32-unknown-unknown build.
// Usage: node --stack-trace-limit=60 run-node.mjs [pkg-dir]   (after wasm-bindgen --target nodejs)
import { createRequire } from "node:module";
import path from "node:path";

const require = createRequire(import.meta.url);
const dir = path.resolve(process.argv[2] ?? "pkg");
const mod = require(path.join(dir, "dbsp_wasm_spike.js"));
const t0 = performance.now();
let out;
try {
  out = mod.spike_run();
} catch (e) {
  const frames = String(e.stack ?? e)
    .split("\n")
    .map((l) => l.replace(/\(wasm:\/\/[^)]*\)/, "").replace(/dbsp_wasm_spike\.wasm\./, ""))
    .filter((l) => !/__rustc|panicking|backtrace|rust_begin_unwind|panic_fmt/.test(l));
  console.error(frames.slice(0, 40).join("\n"));
  process.exit(2);
}
const ms = (performance.now() - t0).toFixed(1);
console.log(out);
const mem = mod.__wasm?.memory?.buffer?.byteLength;
console.log(`ok in ${ms} ms${mem ? `; wasm memory ${(mem / 2 ** 20).toFixed(0)} MiB` : ""}`);
const want = "[[(2, 1), (4, 1), (6, 1)], [(4, -1), (8, 1)], [(10, 1), (12, 1), (14, 1), (16, 1)]] [((0, 18), 1), ((1, 16), 1)]";
if (out !== want) {
  console.error(`MISMATCH\n got: ${out}\nwant: ${want}`);
  process.exit(1);
}

// The stress: many transactions through a spine, so merging and backpressure run on this thread.
const [steps, rows] = [Number(process.argv[3] ?? 2000), Number(process.argv[4] ?? 100)];
const t1 = performance.now();
const s = mod.spike_stress(steps, rows);
const stressMs = (performance.now() - t1).toFixed(0);
const mem2 = mod.__wasm?.memory?.buffer?.byteLength;
console.log(`stress ${steps}x${rows}: ${s} in ${stressMs} ms${mem2 ? `; wasm memory ${(mem2 / 2 ** 20).toFixed(0)} MiB` : ""}`);
