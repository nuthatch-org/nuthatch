# nuthatch troubleshooting

Symptom → what to look at (`/metrics`, Prometheus) → remedy. All `/metrics` series are on the running
`dev` at `http://127.0.0.1:8288/metrics`.

## Backfill seems stuck / hung

- **Start at `/ready`.** A nest that is not ready names why: `quarantine` (a terminal fault, such as
  an entity whose circuit stopped), `seal_fault` (sealing failing, hot still complete), `entities`,
  `ipfs_gave_up_documents`, and a runtime's root lists each unready nest with its `reasons`. The
  log line beside it names an RPC refusal or rate limit. `nuthatch watch` shows the same readings live.
- **Is it actually slow, or just quiet?** At tip the default poll interval is five minutes, so
  `seconds_since_poll` up to `poll_interval_secs` is normal. During backfill, watch
  `nuthatch_rows_decoded_total` / `nuthatch_last_block` climb; on a TTY `dev` shows events/sec and ETA.
- **`--seal-direct` on one endpoint runs one window at a time** whatever `--concurrency` says:
  `seal_direct_concurrency.capped_by` reads `single_rpc_endpoint`. Add `rpc_urls` to get the
  concurrency you asked for.
- A *sparse* contract over millions of blocks looks slow because each window is near-empty - widen it:
  `--window 50000`. That is also the widest range it will ask for, so keep it within the provider's
  `getLogs` range cap.
- An endpoint in the pool answering 429 is rested for its `Retry-After` (or the usual cooldown) and the
  rest of the pool serves the range; only when every endpoint is rate-limiting does the backfill slow
  down, and the log says so.

## "block N alone exceeds the provider's getLogs result cap"

A single block's logs are too large for the provider to return, and it can't be split further. Remedy:
use a provider with a higher/no result cap. This fails loudly rather than looping forever.

## Tip lag (falling behind the chain head)

- Check `nuthatch_tip_height` vs `nuthatch_last_block` - the gap is `nuthatch_tip_lag_blocks`. `nuthatch_sealed_through` trails
  further (past finality, by design).
- Causes: slow/rate-limited RPC (add endpoints or your own node), or a dense contract on a fast chain.
  The adaptive getLogs window self-tunes. Rule out the poll interval first (a gap that closes every
  five minutes is the default cadence, `--poll-interval` to change it), then RPC throughput.

## Reorgs

- Reorgs only ever touch the **hot store** - sealed segments are immutable. You should never see sealed
  data change. `nuthatch_reorgs_total` counts detections; the hot store rolls back and IVM views retract
  automatically, converging to canonical state.
- If you think you need to rewrite sealed Parquet to "fix" a reorg, the approach is wrong - the hot
  store already handles it.

## `/sql` returns 503 or times out

- **503 "server busy":** the analytical gate is saturated (default 2 concurrent). It's node
  self-protection - retry, don't raise the cap.
- **30 s timeout:** the query is too heavy. Add a `WHERE`/`LIMIT`, aggregate with `GROUP BY`, or run
  `explain` first to validate cheaply. Not a reason to remove the guard.
- **Binder/parse errors** come back with a fix hint (unknown table → nearest real table; `from`/`to` →
  double-quote; `sum(value)` → use `value_dec`). Follow the hint; it's derived from the real schema.

## RAM near the 2 GB budget

- The budget is per cursor (one chain's tip-following plus serving) and CI-enforced. Nests on one
  cursor share it (`max_rss_mb`, default 2048, per cursor); a mount projected to exceed it is refused.
  `GET /nests` reports `projected_rss_mb` and each mount's `estimated_rss_mb`; `nuthatch_rss_bytes`
  is the measured figure.
- Burrmill answers from one memory pool per nest (`NUTHATCH_BURRMILL_MEMORY_LIMIT`, default 512 MB) on
  2 threads; the 2 concurrent statements share that pool and spill or refuse rather than double it.
  If you're tight, lower concurrency rather than the pool.

## "semantic.toml drift" warnings at startup

`semantic.toml` describes a table/column the decode registry doesn't have (a stale edit, or the ABI
changed). Fix the file, or regenerate the derived parts - the footguns are always recomputed; only the
authored descriptions are yours to maintain.

## ABI won't resolve at `init`

nuthatch tries Sourcify then Etherscan-class APIs (`--explorer <url>` adds a Blockscout instance in
between). If they miss (unverified contract), pass `--abi <file>` to `init` or `add`, or point `--rpc`
at a node for a proxy's implementation lookup (EIP-1967 proxies resolve the implementation ABI
automatically).
