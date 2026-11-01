// RFC-0081 S0: each pool table segment's Parquet footer from a published dataset, by HTTP range.
// Usage: node footers.mjs <manifest.json> <out dir> [dataset base url]
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
const [manifest, out] = process.argv.slice(2);
const base = process.argv[4] ?? "https://nestsss.nestsss-worker.workers.dev/d6fd43c87dd9593872b5c0f414b7291b230d3da7b2fcdf3ba2ad49e5ec496e8b";
const tables = ["staking__tokens_delegated","staking__tokens_undelegated","staking__delegated_tokens_withdrawn","staking__thaw_request_created","staking__delegation_fee_cut_set","staking__provision_created","staking__tokens_to_delegation_pool_added","staking__delegation_slashed","rewards__horizon_rewards_assigned","rewards__rewards_assigned","staking_legacy__stake_delegated","staking_legacy__stake_delegated_locked","staking_legacy__delegation_parameters_updated","staking_legacy__allocation_created","staking_legacy__rebate_claimed","staking_legacy__rebate_collected"];
const m = JSON.parse(readFileSync(manifest, "utf8"));
mkdirSync(out, { recursive: true });
let bytes = 0, reqs = 0;
const jobs = [];
for (const t of tables) for (const s of m.tables[t] ?? []) jobs.push([t, s]);
const t0 = Date.now();
async function one([t, s]) {
  const u = `${base}/${t}/${s.hash}.parquet`;
  let r = await fetch(u, { headers: { Range: "bytes=-65536" } }); reqs++;
  let buf = Buffer.from(await r.arrayBuffer()); bytes += buf.length;
  const total = Number(r.headers.get("content-range")?.split("/")[1]);
  const flen = buf.readUInt32LE(buf.length - 8);
  if (flen + 8 > buf.length) {
    r = await fetch(u, { headers: { Range: `bytes=-${flen + 8}` } }); reqs++;
    buf = Buffer.from(await r.arrayBuffer()); bytes += buf.length;
  }
  writeFileSync(`${out}/${t}~${s.hash}~${total}~${s.from_block}~${s.to_block}~${s.rows}.footer`, buf.subarray(buf.length - 8 - flen, buf.length - 8));
}
const q = [...jobs];
await Promise.all(Array.from({ length: 16 }, async () => { while (q.length) await one(q.shift()); }));
console.log(JSON.stringify({ segments: jobs.length, requests: reqs, bytes, ms: Date.now() - t0 }));
