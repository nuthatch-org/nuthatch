// RFC-0080 second S0: one line per run of ab.sh, from each run's report.json.
//
//   node table.mjs <ab-out-dir>

import fs from "node:fs";

const dir = process.argv[2];
const pct = (x) => (x * 100).toFixed(1) + "%";
const us = (x) => Math.round(x).toLocaleString("en-GB") + " µs";
const lines = [
  "| Run | Filter | Frames a delta | Filter p50 | Apply p50 | Filter / apply, all | p50 | p99 | Copies wrong | RSS median | Delivery lag p50 / p99 | Closed |",
  "|---|---|---|---|---|---|---|---|---|---|---|---|",
];
for (const name of fs.readdirSync(dir).sort()) {
  const f = `${dir}/${name}/report.json`;
  if (!fs.existsSync(f)) continue;
  const r = JSON.parse(fs.readFileSync(f, "utf8"));
  const c = r.costs;
  const copies = r.copies
    ? `${r.copies.wrong} of ${r.copies.clients}${r.copies.stalled ? ` (+${r.copies.stalled} stalled)` : ""}`
    : "n/a";
  const d = r.delivery?.rounds ? `${us(r.delivery.lag_us_p50)} / ${us(r.delivery.lag_us_p99)}` : "-";
  lines.push(
    `| ${name} | ${name.endsWith("old") ? "json, inline" : "columns, task"} | ${Math.round(c.frames / c.deltas)} | ${us(c.filter_us_p50)} | ${us(c.apply_us_p50)} | **${pct(c.filter_over_apply_total)}** | ${pct(c.filter_over_apply_p50)} | ${pct(c.filter_over_apply_p99)} | ${copies} | ${Math.round(r.follower_rss_mib.median)} MiB | ${d} | ${r.follower?.closed ?? "-"} |`,
  );
  if (r.load) lines.push(`|  | ${r.load.map((l) => l.replace(/.*load averages?:\s*/, "")).join(" → ")} | | | | | | | | | | |`);
}
console.log(lines.join("\n"));
