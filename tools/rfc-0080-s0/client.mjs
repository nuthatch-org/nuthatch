// RFC-0080 S0: a Node client holding a live copy of every shape, from one follower's stream.
//
//   node client.mjs <stream-base> <dataset> <shapes.json> <first> <count> <ready-file> <done-file> <out.json>
//
// Opens `count` streams from shape `first`, at most RAMP waiting for their initial answer at once,
// since ten thousand connects at one instant overflow the listen backlog. Each copy is a count per
// stored row (a `del` is -1, a `put` +1, the initial rows 1 each). Writes `ready-file` once every
// stream has its initial answer, and on seeing `done-file` writes every copy to `out.json`: for each
// shape its rows, the watermark its hot window began above, the generation it reached, how often the
// follower closed it for refetch and how often a connect had to be retried.

import http from "node:http";
import fs from "node:fs";

const [base, dataset, shapesPath, firstArg, countArg, readyFile, doneFile, outPath] =
  process.argv.slice(2);
const first = Number(firstArg);
const count = Number(countArg);
const RAMP = Number(process.env.RAMP ?? 64);
const shapes = JSON.parse(fs.readFileSync(shapesPath, "utf8"));
const agent = new http.Agent({ keepAlive: false, maxSockets: Infinity });

const copies = new Map();
const pending = [];
let opening = 0;
let initial = 0;
let frames = 0;

function next() {
  while (opening < RAMP && pending.length) open(pending.shift());
}

function retry(id) {
  copies.get(id).retries += 1;
  opening -= 1;
  setTimeout(() => {
    pending.push(id);
    next();
  }, 200);
}

function open(id) {
  opening += 1;
  const c = copies.get(id);
  const req = http.get(`${base}/shape/${dataset}/${id}`, { agent }, (res) => {
    if (res.statusCode !== 200) {
      res.resume();
      retry(id);
      return;
    }
    res.setEncoding("utf8");
    let buf = "";
    let started = false;
    res.on("data", (chunk) => {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        const v = JSON.parse(line);
        if (!started) {
          started = true;
          c.rows = new Map(v.rows.map((r) => [r, 1]));
          c.g = v.g;
          c.sealed_through = v.sealed_through;
          if (!c.attached) initial += 1;
          c.attached = true;
          opening -= 1;
          next();
          continue;
        }
        if (v.refetch) {
          c.refetched += 1;
          res.destroy();
          pending.push(id);
          next();
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
    if (!c.attached) retry(id);
    else console.error(`shape ${id}: ${e.message}`);
  });
}

for (let id = first; id < first + count; id++) {
  copies.set(id, { rows: new Map(), g: 0, sealed_through: null, refetched: 0, retries: 0, attached: false });
  pending.push(id);
}
const started = Date.now();
next();

const tick = setInterval(() => {
  if (initial === count && !fs.existsSync(readyFile)) {
    fs.writeFileSync(readyFile, String(Date.now() - started));
    console.error(`client: ${count} streams attached in ${Date.now() - started} ms`);
  }
  if (fs.existsSync(doneFile)) {
    clearInterval(tick);
    const out = {};
    let retries = 0;
    for (const [id, c] of copies) {
      retries += c.retries;
      out[id] = {
        shape: shapes[id],
        g: c.g,
        sealed_through: c.sealed_through,
        refetched: c.refetched,
        rows: [...c.rows.entries()],
      };
    }
    fs.writeFileSync(outPath, JSON.stringify({ frames, retries, copies: out }));
    console.error(`client: ${frames} frames applied, ${retries} connects retried; copies written`);
    process.exit(0);
  }
}, 200);
