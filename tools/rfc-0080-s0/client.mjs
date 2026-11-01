// RFC-0080 S0: a Node client holding a live copy of every shape, from one follower's stream.
//
//   node client.mjs <stream-base> <dataset> <shapes.json> <first> <count> <ready-file> <done-file> <out.json>
//
// Opens `count` streams from shape `first`, keeps each copy as a count per stored row (a `del` is
// -1, a `put` +1, the initial rows 1 each), writes `ready-file` once every stream has its initial
// answer, and on seeing `done-file` writes every copy to `out.json`: for each shape its rows, the
// watermark its hot window began above, the generation it reached and how often it refetched.

import http from "node:http";
import fs from "node:fs";

const [base, dataset, shapesPath, firstArg, countArg, readyFile, doneFile, outPath] =
  process.argv.slice(2);
const first = Number(firstArg);
const count = Number(countArg);
const shapes = JSON.parse(fs.readFileSync(shapesPath, "utf8"));
const agent = new http.Agent({ keepAlive: false, maxSockets: Infinity });

const copies = new Map();
let initial = 0;
let frames = 0;

function open(id) {
  const c = copies.get(id) ?? { rows: new Map(), g: 0, sealed_through: null, refetched: -1 };
  c.refetched += 1;
  c.rows = new Map();
  copies.set(id, c);
  let started = false;
  const req = http.get(`${base}/shape/${dataset}/${id}`, { agent }, (res) => {
    if (res.statusCode !== 200) {
      console.error(`shape ${id}: ${res.statusCode}`);
      setTimeout(() => open(id), 500);
      res.resume();
      return;
    }
    res.setEncoding("utf8");
    let buf = "";
    res.on("data", (chunk) => {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        const v = JSON.parse(line);
        if (!started) {
          started = true;
          c.g = v.g;
          c.sealed_through = v.sealed_through;
          for (const r of v.rows) c.rows.set(r, 1);
          if (c.refetched === 0) initial += 1;
          continue;
        }
        if (v.refetch) {
          res.destroy();
          open(id);
          return;
        }
        if (v.g < c.g) throw new Error(`shape ${id}: generation ${v.g} after ${c.g}`);
        c.g = v.g;
        frames += 1;
        const row = v.put ?? v.del;
        const n = (c.rows.get(row) ?? 0) + (v.put !== undefined ? 1 : -1);
        if (n === 0) c.rows.delete(row);
        else c.rows.set(row, n);
      }
    });
  });
  req.on("error", (e) => {
    console.error(`shape ${id}: ${e.message}`);
    setTimeout(() => open(id), 500);
  });
}

for (let id = first; id < first + count; id++) open(id);

const started = Date.now();
const tick = setInterval(() => {
  if (initial === count && !fs.existsSync(readyFile)) {
    fs.writeFileSync(readyFile, String(Date.now() - started));
    console.error(`client: ${count} streams attached in ${Date.now() - started} ms`);
  }
  if (fs.existsSync(doneFile)) {
    clearInterval(tick);
    const out = {};
    for (const [id, c] of copies) {
      out[id] = {
        shape: shapes[id],
        g: c.g,
        sealed_through: c.sealed_through,
        refetched: c.refetched,
        rows: [...c.rows.entries()],
      };
    }
    fs.writeFileSync(outPath, JSON.stringify({ frames, copies: out }));
    console.error(`client: ${frames} frames applied; copies written`);
    process.exit(0);
  }
}, 200);
