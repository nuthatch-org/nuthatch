// The replay off the page's thread: nuthatch-timelapse and Burrmill in wasm, over a mirrored nest's
// final segments, each fetched whole and checked against the SHA-256 its name carries.
import init, { replay, wasmMemory } from "./pkg/rfc_0070_s1.js";

const ready = init();
// Segments fetched ahead of the engine, per table. The engine binds a chunk's segments one at a time,
// so without this every segment costs a full round trip in series.
const AHEAD = 12;
// A segment can straddle two chunks; one behind is kept for the second binding.
const BEHIND = 2;

const hex = (buf) => [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");

function mirrorStore(base, manifest) {
  const byKey = new Map();
  const perTable = new Map();
  for (const [table, segs] of Object.entries(manifest.tables)) {
    const list = segs.filter((s) => !s.provisional).sort((a, b) => a.from_block - b.from_block);
    perTable.set(table, list.map((s) => `segments/${s.file}`));
    list.forEach((s, idx) => byKey.set(`segments/${s.file}`, { table, idx, hash: s.hash, url: `${base}/${table}/${s.hash}.parquet` }));
  }
  const held = new Map();
  const stats = { fetched: 0, bytes: 0, held_bytes: 0, peak_held_bytes: 0 };
  async function fetchVerified(e) {
    const r = await fetch(e.url);
    if (!r.ok) throw new Error(`${e.table}/${e.hash}: HTTP ${r.status}`);
    const bytes = new Uint8Array(await r.arrayBuffer());
    const got = hex(await crypto.subtle.digest("SHA-256", bytes));
    if (got !== e.hash) throw new Error(`${e.table}/${e.hash}: content is ${got.slice(0, 12)}…, not the address it is stored under`);
    stats.fetched++;
    stats.bytes += bytes.length;
    return bytes;
  }
  function get(key) {
    if (!held.has(key)) {
      const e = byKey.get(key);
      if (!e) return Promise.reject(new Error(`${key} is not a final segment in the mirror's catalogue`));
      const p = fetchVerified(e).then((b) => {
        if (held.get(key) === p) { stats.held_bytes += b.length; stats.peak_held_bytes = Math.max(stats.peak_held_bytes, stats.held_bytes); }
        return b;
      });
      held.set(key, p);
    }
    return held.get(key);
  }
  function drop(key) {
    const p = held.get(key);
    held.delete(key);
    p?.then((b) => { stats.held_bytes -= b.length; }, () => {});
  }
  function around(key) {
    const { table, idx } = byKey.get(key);
    const list = perTable.get(table);
    for (let j = idx + 1; j <= idx + AHEAD && j < list.length; j++) get(list[j]).catch(() => {});
    for (const k of [...held.keys()]) {
      const e = byKey.get(k);
      if (e.table === table && e.idx < idx - BEHIND) drop(k);
    }
  }
  return {
    stats,
    size: async (key) => { const b = await get(key); around(key); return b.length; },
    read: async (key, offset, length) => (await get(key)).subarray(offset, offset + length),
  };
}

onmessage = async ({ data: { base, spec, chunkRows } }) => {
  const t0 = performance.now();
  try {
    await ready;
    const [manifestText, schemaText] = await Promise.all(
      ["manifest.json", "schema.json"].map((f) =>
        fetch(`${base}/${f}`, { cache: "no-cache" }).then((r) => { if (!r.ok) throw new Error(`${f}: HTTP ${r.status}`); return r.text(); })),
    );
    const store = mirrorStore(base, JSON.parse(manifestText));
    const progress = (done, total) => postMessage({ progress: { done, total, ...store.stats, memory: wasmMemory().buffer.byteLength } });
    const out = JSON.parse(await replay(store.size, store.read, spec, manifestText, schemaText, "{}", chunkRows, progress));
    postMessage({ done: out, stats: { ...store.stats, memory: wasmMemory().buffer.byteLength, wall_secs: (performance.now() - t0) / 1e3 } });
  } catch (e) {
    postMessage({ error: String(e?.message ?? e) });
  }
};
