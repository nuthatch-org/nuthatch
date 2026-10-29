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

// RFC-0075 S1: nuthatch-core's balance view under the inline driver, the shape a tab runs.
const views = mod.spike_views();
const wantViews = '[("0xa", 70), ("0xb", 30), ("0xmint", -100)]';
console.log(`views ${views}`);
if (views !== wantViews) {
  console.error(`MISMATCH\n got: ${views}\nwant: ${wantViews}`);
  process.exit(1);
}

// RFC-0075 S1: one window through core's fetch_window and process_window, the native test's pin.
const win = mod.spike_window();
const wantWin =
  "committed=2 rows_sha256=8b6b04dba6c9e66507c3a23852d42c97a102850215b6e22b2b3c36e19eb3f491 " +
  'balances=[("0x1111111111111111111111111111111111111111", 60), ' +
  '("0x2222222222222222222222222222222222222222", 40), ' +
  '("0x0000000000000000000000000000000000000000", -100)] ' +
  'last=Some("12") sealed_through=12 hot=0 segments=[' +
  '"tok__transfer 10..=10 rows=1 1418460664977e9b6456532bbbb32543ece957f189c8a1d45b65317ba1705ca1", ' +
  '"tok__transfer 12..=12 rows=1 170d4dc640f41dddf3718712c011bbdff3cd2286eed32f44df4180a4856e4346"]';
console.log(`window ${win}`);
if (win !== wantWin) {
  console.error(`MISMATCH\n got: ${win}\nwant: ${wantWin}`);
  process.exit(1);
}

// The stress: many transactions through a spine, so merging and backpressure run on this thread.
const [steps, rows] = [Number(process.argv[3] ?? 2000), Number(process.argv[4] ?? 100)];
const t1 = performance.now();
const s = mod.spike_stress(steps, rows);
const stressMs = (performance.now() - t1).toFixed(0);
const mem2 = mod.__wasm?.memory?.buffer?.byteLength;
console.log(`stress ${steps}x${rows}: ${s} in ${stressMs} ms${mem2 ? `; wasm memory ${(mem2 / 2 ** 20).toFixed(0)} MiB` : ""}`);

// S0 remainder (#2476): the binary's own entity circuit, join and min/max included, with memory
// sampled every hundred windows. Linear memory never shrinks, so the last sample is the peak.
const [ewin, erows] = [Number(process.argv[5] ?? 1000), Number(process.argv[6] ?? 100)];
const mib = (b) => (b / 2 ** 20).toFixed(1);
const before = mod.wasm_memory_bytes();
let peak = before;
const samples = [];
const t2 = performance.now();
const e = mod.spike_entity(ewin, erows, (w) => {
  const m = mod.wasm_memory_bytes();
  peak = Math.max(peak, m);
  if (w % 250 === 0) samples.push(`${w}:${mib(m)}`);
});
const entityMs = (performance.now() - t2).toFixed(0);
const after = mod.wasm_memory_bytes();
console.log(`entity ${ewin}x${erows}: ${e}`);
console.log(`entity time ${entityMs} ms; wasm memory before ${mib(before)} MiB, after ${mib(after)} MiB, peak ${mib(Math.max(peak, after))} MiB; samples ${samples.join(" ")}`);

// RFC-0075 S2: the reorg property over the redb hot store on a byte buffer. tests/native.rs pins the
// same transcript digest, so a wasm32 run that converged differently fails here.
const { createHash } = await import("node:crypto");
const t3 = performance.now();
const reorg = mod.spike_reorg(12);
const reorgMs = (performance.now() - t3).toFixed(0);
console.log(reorg);
const reorgSha = createHash("sha256").update(reorg).digest("hex");
const wantReorg = "4f3b8bec690c201973ce15e64987659f67dd912547767241f31121111e96592a";
console.log(`reorg ${reorg.split("\n").length} cases in ${reorgMs} ms; transcript sha256 ${reorgSha}; wasm memory ${mib(mod.wasm_memory_bytes())} MiB`);
if (reorgSha !== wantReorg) {
  console.error(`MISMATCH\n got: ${reorgSha}\nwant: ${wantReorg}`);
  process.exit(1);
}
