#!/usr/bin/env node
// Replay every Transfer of one or more fungible LP tokens from their first block with Nuthatch,
// rebuild balances, and check  replay(non-zero holders) + balanceOf(0x0) == totalSupply().
//
//   node lp-nest-scan.mjs --rpc <url> --dir <nest dir> [--chain mainnet] [--window 5000]
//        [--concurrency 4] [--nuthatch nuthatch] [--min-pct 0.1] <0xLP[:startBlock]>...
//
// Needs: Node >= 20, the `nuthatch` binary, an archive-capable RPC with eth_getLogs.
// Resumable: re-running in the same --dir continues the backfill, it never starts over.
import { spawn } from 'node:child_process';
import { mkdirSync, writeFileSync, existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { parseArgs } from 'node:util';

const { values: o, positionals: tokens } = parseArgs({
  allowPositionals: true,
  options: {
    rpc: { type: 'string' }, dir: { type: 'string' }, chain: { type: 'string', default: 'mainnet' },
    'chain-id': { type: 'string', default: '1' },
    window: { type: 'string', default: '5000' }, concurrency: { type: 'string', default: '4' },
    nuthatch: { type: 'string', default: 'nuthatch' }, attach: { type: 'boolean', default: false }, port: { type: 'string', default: '8288' },
    'min-pct': { type: 'string', default: '0.1' }, name: { type: 'string', default: 'lp-scan' },
  },
});
if (!o.rpc || !o.dir || tokens.length === 0) {
  console.error('usage: lp-nest-scan.mjs --rpc <url> --dir <dir> <0xLP[:startBlock]>...'); process.exit(2);
}
const base = `http://127.0.0.1:${o.port}`;
const lps = tokens.map((t, i) => {
  const [address, start = '0'] = t.split(':');
  if (!/^0x[0-9a-fA-F]{40}$/.test(address)) throw new Error(`bad address ${t}`);
  return { address: address.toLowerCase(), start: Number(start), alias: `lp${i}` };
});

// 1. the nest: one ERC-20 Transfer ABI, one [[contracts]] entry per LP token.
mkdirSync(join(o.dir, 'abis'), { recursive: true });
writeFileSync(join(o.dir, 'abis', 'erc20_transfer.json'), JSON.stringify([{
  type: 'event', name: 'Transfer', anonymous: false, inputs: [
    { name: 'from', type: 'address', indexed: true }, { name: 'to', type: 'address', indexed: true },
    { name: 'value', type: 'uint256', indexed: false }] }], null, 1));
let toml = `[nest]\nname = "${o.name}"\nchain = "${o.chain}"\nchain_id = ${o['chain-id']}\n` +
  `rpc_urls = ["${o.rpc}"]\nschema_version = 2\nblock_timestamps = false\n`;
for (const l of lps) toml += `\n[[contracts]]\nalias = "${l.alias}"\naddress = "${l.address}"\n` +
  `start_block = ${l.start}\nabi = "abis/erc20_transfer.json"\n`;
if (!existsSync(join(o.dir, 'nuthatch.toml'))) writeFileSync(join(o.dir, 'nuthatch.toml'), toml);
// an existing nest keeps its own aliases (they name the tables)
for (const m of readFileSync(join(o.dir, 'nuthatch.toml'), 'utf8').matchAll(/alias = "([^"]+)"\naddress = "(0x[0-9a-f]{40})"/g)) {
  const l = lps.find((x) => x.address === m[2]); if (l) l.alias = m[1];
}

// 2. backfill from each token's first block, straight to sealed Parquet, then stop at the tip.
//    --attach: the nest is already running on --port (skip starting it).
const t0 = Date.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const get = async (p) => (await fetch(base + p)).json();
const rpc = async (method, params) => (await (await fetch(o.rpc, { method: 'POST',
  headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })).json()).result;
const child = o.attach ? null : spawn(o.nuthatch, ['dev', '--seal-direct', '--concurrency', o.concurrency,
  '--window', o.window, '--no-admin', '--listen', `127.0.0.1:${o.port}`], { cwd: o.dir, stdio: ['ignore', 'inherit', 'inherit'] });
for (;;) {
  await sleep(5000);
  try {
    const root = await get('/');
    const tip = parseInt(await rpc('eth_blockNumber', []), 16);
    if (root.last_block && tip - Number(root.last_block) < 100) break;
  } catch { /* still starting */ }
}
const secs = ((Date.now() - t0) / 1000).toFixed(0);

// 3. replay balances and compare with the chain.
const sql = async (q) => (await (await fetch(`${base}/sql?q=${encodeURIComponent(q)}`)).json());
const pad = (a) => a.slice(2).padStart(64, '0');
const ZERO = '0x0000000000000000000000000000000000000000';
const out = [];
for (const l of lps) {
  const t = `${l.alias}__transfer`;
  const rows = (await sql(`SELECT addr, CAST(sum(d) AS VARCHAR) AS bal FROM (
      SELECT "to" AS addr, value_dec AS d FROM ${t} UNION ALL SELECT "from", -value_dec FROM ${t})
    GROUP BY addr HAVING sum(d) <> 0 ORDER BY sum(d) DESC`)).rows ?? [];
  const bal = new Map(rows.map((r) => [String(r.addr ?? r[0]).toLowerCase(), BigInt(r.bal ?? r[1])]));
  const nz = [...bal].filter(([a]) => a !== ZERO);
  const replay = nz.reduce((s, [, v]) => s + v, 0n);
  const word = (h) => BigInt(h.slice(0, 66)); // some gateways pad eth_call results past 32 bytes
  const supply = word(await rpc('eth_call', [{ to: l.address, data: '0x18160ddd' }, 'latest']));
  const b0 = word(await rpc('eth_call', [{ to: l.address, data: '0x70a08231' + pad(ZERO) }, 'latest']));
  const holders = nz.sort((a, b) => (b[1] > a[1] ? 1 : -1)).filter(([, v]) => Number((v * 100000n) / (supply || 1n)) / 1000 >= Number(o['min-pct']));
  out.push({ lp: l.address, holders_total: nz.length, replay_supply: replay.toString(), onchain_total_supply: supply.toString(),
    balanceOf_zero: b0.toString(), check_ok: replay + b0 === supply,
    holders_over_min_pct: holders.map(([a, v]) => ({ address: a, balance: v.toString(), pct: Number((v * 1000000n) / (supply || 1n)) / 10000 })) });
}
console.log(JSON.stringify({ backfill_seconds: Number(secs), tokens: out }, null, 2));
child?.kill('SIGTERM');
