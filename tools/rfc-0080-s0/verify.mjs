// RFC-0080 S0: check every client copy against the writer, and summarise the follower's costs.
//
//   node verify.mjs <out-dir> <writer-route-base> <g0>
//
// A copy is right when it holds each row once and its rows are exactly the writer's rows of its
// table, above the watermark its hot window began at, that its shape's terms admit. The terms are
// evaluated here again, independently of the follower's index. Costs count deltas after generation
// g0, when every client was attached.

import fs from "node:fs";
import readline from "node:readline";

const [out, writer, g0Arg] = process.argv.slice(2);
const g0 = Number(g0Arg);
const stats = JSON.parse(fs.readFileSync(`${out}/stats.json`, "utf8"));
const report = {};

const text = (v) => (typeof v === "string" ? v : JSON.stringify(v));
const key = (r) => `${r.block_number}:${r.log_index}`;

// copies.json is one header line, then one copy a line, read twice so no pass holds them all.
async function* copiesOf(path) {
  const lines = readline.createInterface({ input: fs.createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) if (line) yield JSON.parse(line);
}

if (fs.existsSync(`${out}/copies.json`)) {
  const path = `${out}/copies.json`;
  let header = null;
  let floor = Infinity;
  const tables = new Set();
  for await (const c of copiesOf(path)) {
    if (!header) {
      header = c;
      continue;
    }
    floor = Math.min(floor, c.sealed_through ?? 0);
    tables.add(c.shape.table);
  }
  const byTable = {};
  for (const t of tables) {
    const q = `SELECT * FROM ${t} WHERE block_number > ${floor}`;
    const res = await fetch(`${writer}/sql?q=${encodeURIComponent(q)}`);
    const body = await res.json();
    if (!res.ok) throw new Error(`writer /sql: ${JSON.stringify(body)}`);
    if (body.rows.length >= 50000) throw new Error(`${t}: the writer's answer hit the row cap`);
    byTable[t] = body.rows;
  }
  const checked = Object.values(byTable).reduce((a, r) => a + r.length, 0);
  let clients = 0;
  let wrong = 0;
  let held = 0;
  let refetched = 0;
  let stalled = 0;
  let maxG = 0;
  const firstWrong = [];
  let first = true;
  for await (const c of copiesOf(path)) {
    if (first) {
      first = false;
      continue;
    }
    if (c.stalled) {
      stalled += 1;
      continue;
    }
    clients += 1;
    refetched += c.refetched;
    maxG = Math.max(maxG, c.g);
    const want = new Set(
      byTable[c.shape.table]
        .filter((r) => r.block_number > (c.sealed_through ?? 0))
        .filter((r) =>
          Object.entries(c.shape.where).every(([col, vals]) => vals.includes(text(r[col]))),
        )
        .map(key),
    );
    const got = new Set();
    let once = true;
    for (const [row, n] of c.rows) {
      if (n !== 1) once = false;
      got.add(key(JSON.parse(row)));
    }
    held += got.size;
    const same = once && got.size === want.size && [...got].every((k) => want.has(k));
    if (!same) {
      wrong += 1;
      if (firstWrong.length < 5)
        firstWrong.push({ id: c.id, once, got: got.size, want: want.size, sealed: c.sealed_through });
    }
  }
  report.copies = {
    clients,
    stalled,
    wrong,
    first_wrong: firstWrong,
    rows_held: held,
    writer_rows_checked: checked,
    frames_applied: header.frames,
    connect_retries: header.retries,
    attached_all_in_ms: Number(fs.readFileSync(`${out}/ready`, "utf8")),
    refetched,
    max_generation: maxG,
  };
}

const tip = stats.filter((c) => c.generation > g0 && c.rows > 0);
const pct = (xs, p) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};
const sum = (f) => tip.reduce((a, c) => a + f(c), 0);
report.costs = {
  deltas: tip.length,
  rows: sum((c) => c.rows),
  frames: sum((c) => c.frames),
  filter_us_p50: pct(tip.map((c) => c.filter_ns), 50) / 1000,
  filter_us_p99: pct(tip.map((c) => c.filter_ns), 99) / 1000,
  apply_us_p50: pct(tip.map((c) => c.apply_ns), 50) / 1000,
  apply_us_p99: pct(tip.map((c) => c.apply_ns), 99) / 1000,
  store_apply_us_p50: pct(tip.map((c) => c.store_apply_ns), 50) / 1000,
  lock_wait_us_max: Math.max(...tip.map((c) => c.lock_wait_ns)) / 1000,
  filter_over_apply_total: sum((c) => c.filter_ns) / sum((c) => c.apply_ns),
  filter_over_apply_p50: pct(tip.map((c) => c.filter_ns / c.apply_ns), 50),
  filter_over_apply_p99: pct(tip.map((c) => c.filter_ns / c.apply_ns), 99),
  filter_over_store_apply_total: sum((c) => c.filter_ns) / sum((c) => c.store_apply_ns),
};
const rss = fs
  .readFileSync(`${out}/rss.tsv`, "utf8")
  .trim()
  .split("\n")
  .map((l) => Number(l.trim().split(/\s+/)[1]) / 1024)
  .filter((x) => x > 0);
report.follower_rss_mib = { median: pct(rss, 50), max: Math.max(...rss) };
if (fs.existsSync(`${out}/delivery.json`)) {
  const d = JSON.parse(fs.readFileSync(`${out}/delivery.json`, "utf8"));
  report.delivery = {
    rounds: d.length,
    streams_woken: d.reduce((a, c) => a + c.streams, 0),
    wake_us_p50: d.length ? pct(d.map((c) => c.wake_ns), 50) / 1000 : 0,
    wake_us_p99: d.length ? pct(d.map((c) => c.wake_ns), 99) / 1000 : 0,
    lag_us_p50: d.length ? pct(d.map((c) => c.lag_ns), 50) / 1000 : 0,
    lag_us_p99: d.length ? pct(d.map((c) => c.lag_ns), 99) / 1000 : 0,
  };
}
if (fs.existsSync(`${out}/datasets.json`)) {
  report.follower = JSON.parse(fs.readFileSync(`${out}/datasets.json`, "utf8"))[0];
}
if (fs.existsSync(`${out}/load.txt`)) report.load = fs.readFileSync(`${out}/load.txt`, "utf8").trim().split("\n");
console.log(JSON.stringify(report, null, 2));
