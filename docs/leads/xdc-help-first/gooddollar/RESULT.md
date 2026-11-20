# gooddollar-xdc result

Verdict: FEASIBLE for XDC UBIScheme, reconciliation matches. Celo not attempted (XDC only; Celo "if cheap" was
not measured).

## Measured (this ThinkPad, public RPC https://rpc.xinfin.network, counting proxy)
- Nest: `evidence/nest-nuthatch.toml` (UBIScheme proxy 0x2286...0faf, from block 95,249,624 per deployment_blocks.md).
- Range: 95,249,624 to tip 109,541,666 (14.3M blocks). Rows: 3,882,012 decoded events, of which 3,881,233 are
  UBIClaimed from 78,682 distinct claimers. These counts cover the whole nest, including blocks sealed by an earlier
  run before the log below starts.
- Wall time from empty dir to "caught up to tip": 18:56:43Z to 21:18:59Z = 2h22m, including two restarts and a
  concurrency-16 experiment (no speedup: bound by header calls). The final sealing leg alone, as the nest logged it: 3,773,310 events over blocks
  96,466,712 to 109,539,472 in 2h17m at 457 ev/s (3,773,310 / 8,220 s = 459, consistent). That leg is not the whole
  history: the remaining 108,702 of the 3,882,012 events (3,882,012 - 3,773,310) were sealed by earlier runs or arrived
  after block 109,539,472 while following; the split was not measured.
- RPC calls, from the committed `evidence/xdc-rpc-count.json` (proxy session through 2026-11-20 11:18Z, so it includes
  following the tip after catch-up): 2,452,265 total: 2,892 eth_getLogs, 2,449,211 eth_getBlockByNumber, 158
  eth_blockNumber, 4 eth_chainId, 42 errors (retried). An earlier reading taken at the end of the backfill gave
  about 2,449,210 total; that snapshot was overwritten and is not kept, so use the committed file. The time goes on one eth_getBlockByNumber per block that holds a claim, because RawLogs
  `block_timestamp` is NOT NULL. The log fetch itself is cheap.
- Disk: 359 MB of sealed Parquet segments.
- Daily-ledger reconciliation (`evidence/reconcile_nest.py`, `evidence/reconcile-full.json`): 375 protocol days (15 to 389),
  375 match on BOTH claim count (getClaimerCount) and amount (getClaimAmount), eth_call pinned at block
  109,541,686. 0 mismatches.
- Byte check: `check-against-rpc.ts` on blocks 96,300,000-96,320,000: 5,955 logs on chain, 5,955 in the nest, 0
  differences (topics, log_data, block_hash); also 95,849,000-95,900,000 (4 vs 4).
- Loader dry run (full history, `load-rawlogs.ts --dry-run`): 3,881,502 RawLogs rows (sealed rows only), 6 min,
  all (chain_id, tx_hash, log_index) keys unique. Typechecks under tsc --strict.
- Pain quoted: Envio XDC indexer stuck, HyperSync dropped XDC. Here: lag 0 at tip, no HyperSync.

## Not done / caveats
- The BigQuery path (`stageAndMerge`) was NOT run: no BigQuery access here. Only the `--dry-run` NDJSON path ran.
  The loader reuses the repo's own `stageAndMerge`, so the lead should run it once against a dev dataset.
- `tx_index` is written as 0 and `removed` is NULL (nest does not keep them); documented in README.
- dbt/pipeline-v5 tests were not run (not touched).

## Bugs found
- My loader bug, fixed in this run: dense ranges exceeded the nest's max_rows at the default 50k-block span and
  the loader aborted. It now splits the range in half until it fits (commit 3efb1d4).
- Nuthatch: `--concurrency` above 4 gives no speedup on a single public endpoint when headers dominate.
