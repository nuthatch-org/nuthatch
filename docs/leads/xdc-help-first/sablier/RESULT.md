# sablier-xdc result

Verdict: FEASIBLE. Nuthatch indexes all Sablier XDC deployments (Lockup, Flow, Airdrops) from the public
RPC https://rpc.xinfin.network, no key, no HyperSync.

Scope delivered: all three families as event tables (14 contracts + 10 factory child templates, 202 tables).
NOT delivered: the Stream/Action/Campaign entities from `schema/`. It is an event source, not a drop-in for the
Envio entity API.

## Measured (this ThinkPad, public XDC RPC, via a counting proxy)
- Range: block 85,225,620 (first deployment) to tip 109,539,609 = 24.31M blocks.
- Rows: 535 decoded events in 69 non-empty tables (202 tables declared).
- Wall time: first backfill began 18:52:25Z; one restart at 18:56 (resumed from the sealed block, nothing refetched);
  "caught up to tip" at 19:04:58Z. About 12.5 min of run time in total, from an empty directory.
- RPC calls, from the committed `evidence/sablier-rpc-count.json` (proxy session through 2026-11-20 11:18Z, so it
  includes hours of following the tip): 1,512 total (676 eth_getLogs, 653 eth_getBlockByNumber, 181 eth_blockNumber,
  2 eth_chainId), 1 error. Counted by `../evidence-shared/rpcproxy.py`. An earlier reading of 724 calls was taken at
  catch-up (about 19:05Z) and was not kept, so it is not quoted as a measurement.
- Ground truth: independent `eth_getLogs` over Lockup v2.0 (`evidence/count_logs.py`, output `evidence/count-lockup-v2.json`, 244 getLogs calls): 417 logs. Nest rows
  for lockup_v2_0 up to the same block: 417. Equal.
- getLogs limits: no block-range error from rpc.xinfin.network at the window sizes used (50,000 then adaptive 728).
- Reorg: one shallow reorg seen while following (19:45Z, rolled back to 109,540,078, a few blocks), handled.
- Pain quoted: HyperSync ~1.65M blocks behind on XDC (enviodev/hyperindex#1829). Here: 0 blocks behind, lag 0 at tip.

## Gaps / bugs hit
- Factory nests run backfill sequentially ("--concurrency ignored until the step-5 filter flip"). Still 12 min here.
- "155 declared tables have no data ... tip could not be read" notice appears on resume. Cosmetic.
- Could not run prettier or the repo's `just` recipes in the clone: dependencies are not installed (no network
  signups). README is hand-wrapped to 120 columns. The patch touches no JS in `src/`, so tsc is not in scope.
