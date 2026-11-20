# PR draft (do not post; lead reviews)

Title: feat(onchain-analytics): optional Nuthatch nest and RawLogs loader for the XDC UBIScheme

Closes #40

Body:

The XDC indexer on Envio got stuck, and then HyperSync dropped XDC support. XDC UBIScheme logs have had no source
since.

This adds `projects/onchain-analytics/nuthatch/`: an optional way to fill `RawLogs` for XDC from a plain JSON-RPC
endpoint, using [Nuthatch](https://github.com/nuthatch-org/nuthatch), a single binary. It has a nest config for the
UBIScheme proxy and a loader that turns the nest's rows into `RawLogs` rows and sends them through the existing
`stageAndMerge` path, so reruns are idempotent on `(chain_id, tx_hash, log_index)`.

It is opt-in. It is not wired into `pipeline-v5`, adds no dependency, and does not change HyperSync for the other
chains. Delete the folder and nothing else changes.

Checked against your own ledger: for every protocol day (15 to 389, 375 days) the nest's claim count and amount
equal `getClaimerCount(day)` and `getClaimAmount(day)`, read with `eth_call` at one pinned block. 375 of 375 match.
`check-against-rpc.ts` also rebuilds topics, data and block hash and compares them with `eth_getLogs`: 5,955 logs on a
dense 20k-block range, 0 differences.

Numbers from a run on a laptop against the public `https://rpc.xinfin.network`, no API key:

- 14.3M blocks from the first deployment block to the tip, 3.88M events (3.88M claims from 78.7k claimers).
- From an empty directory to caught up with the tip: 2h22m. It then keeps following.
- About 2.45M RPC calls (2,452,265 in the committed counter file, which also includes following the tip after catch-up), 2,892 of them `eth_getLogs`. The rest are one `eth_getBlockByNumber` per block with a claim,
  needed because `block_timestamp` is NOT NULL in `RawLogs`.
- 359 MB of Parquet on disk. Loader dry run over the full history: 3.88M rows in 6 minutes.

Not tested: the loader's BigQuery write (`stageAndMerge`) was not run, because the machine that prepared this has no
BigQuery access. Only `--dry-run` was run. Please try it on a dev dataset first. Also, `tx_index` is written as 0 and
`removed` as NULL, as the README says.

Try it:

```bash
cd projects/onchain-analytics/nuthatch/nest
nuthatch dev --dir . --listen 127.0.0.1:8090 --seal-direct --window 50000
cd ../../pipeline-v5
npx tsx ../nuthatch/load-rawlogs.ts --nest http://127.0.0.1:8090 --chain-id 50 --dry-run /tmp/rawlogs.ndjson
python3 -I ../nuthatch/reconcile_nest.py http://127.0.0.1:8090 https://rpc.xinfin.network 0x22867567e2d80f2049200e25c6f31cb6ec2f0faf
```

<!-- Include the next line only if the hosted nest is live when this is posted; otherwise delete it. -->
If you only want to query the data, a hosted copy is at NEST_URL (https://nuthatch-indexer.com/nests/gooddollar-xdc).
