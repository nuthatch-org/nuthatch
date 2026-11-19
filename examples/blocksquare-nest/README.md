# blocksquare-nest

A [nuthatch](https://github.com/nuthatch-org/nuthatch) nest for the **Blocksquare mainnet contracts**:
the factories, staking pools, governance pool, token, registries, revenue distribution and the
BST/ETH pair, decoded into SQL tables on your own machine, with a few views on top.

One binary, one config file, no Postgres, no Docker, no API key.

This is a scaffold to compare against the Envio indexer in
[blocksquare/blocksquare-indexer](https://github.com/blocksquare/blocksquare-indexer) (issue #45). It is
not a replacement for it. Read "What it does not do" before you compare anything.

## Where the addresses come from

Every address, the chain and the start block come from the indexer repo at commit `1849c90`:
`src/config/mainnet.ts` (mainnet addresses), `templates/template.mainnet.config.yaml` (which contracts
and events), `config.yaml` (the BST/ETH pool and price feed registry on chain 1) and `abis/`. Nothing
was taken from anywhere else.

## What it indexes

**Chain:** `mainnet` (id 1). **20 contracts and 2 factory templates, 118 tables.** Every event in
each ABI is a table, not only the events the Envio config subscribes to.

| alias | contract | address |
|---|---|---|
| `blocksquare_token` | BlocksquareToken (BST) | `0x509a38b7a1cc0dcd83aa9d06214663d9ec7c7f4a` |
| `certified_partners` | CertifiedPartners | `0x8dbb99cc3721f5c9cc7c9e92db260813cf78cdd3` |
| `data_storage_proxy` | DataStorageProxy | `0x8c2a858fe7b2bf155247c7f528c6ca7b186197b5` |
| `governance_pool` | GovernancePool | `0x6f1e92fb8a685aaa0710bad194d7b1aa839f7f8a` |
| `liquidity_staking_pool` | LiquidityStakingPool (BST:ETH) | `0x1802f66868d0649687a7a6bc9b8a4292e148daec` |
| `marketplace_pool_factory` | MarketplacePoolFactory | `0x17887106a14f38bf10512565bdbb5bd7ac12f001` |
| `property_factory` | PropertyFactory | `0x1ae91a263a690bf2129cf0b3acac92bbb67e6685` |
| `property_registry` | PropertyRegistry | `0x05325c1ab1440df7214db38f676f95999729267b` |
| `property_revenue_distribution` | PropertyRevenueDistribution | `0xbcb1757cdadec25b6cadc1a83953a70eda2da8cb` |
| `property_token_offering` | PropertyTokenOffering | `0x25862c4fb4ce9d6ff9b463488e0ec656fa08de78` |
| `users` | Users | `0x13344d0cb96b17df81c4171ce47e14ff6c1975f7` |
| `bst_eth_pool` | Uniswap V2 BST/ETH pair | `0x0e85fb1be698e777f2185350b4a52e5ee8df51a6` |
| `staking_community`, `_issuer`, `_rwa`, `_landhive` | PropertyStakingPool x4 | `0x13299657…5451`, `0x57ba8864…c73e`, `0x6f5ecf8a…4b07`, `0xfc7cd245…debf` |
| `valuation_community`, `_issuer`, `_rwa`, `_landhive` | OceanpointValuation x4 | the `valuationAddress` of each pool in `mainnet.ts` |

Two factory rules find contracts nobody lists by hand: `MarketplacePoolFactory.MarketplacePoolCreated`
adds each `MarketplacePool` (template `marketplace_pool`) and `PropertyFactory.NewPropToken` adds each
`PropertyToken` (template `property_token`). Children share tables, told apart by the `address` column.

## Verified

Measured on 2026-11-18 with nuthatch 5.23.0 on a Linux x86_64 box, against the two keyless public
endpoints in `nuthatch.toml` (Tenderly's gateway and mevblocker). One run, not a benchmark.

- `nuthatch dev --seal-direct --concurrency 4 --window 2560` from block 0: **255,258 events over blocks
  0 to 26,443,816 in 41 m 22 s**, then caught up to the tip and followed. The first half of the chain holds
  few events, so events a second is not a meaningful rate. The log has 175 `429` retry lines from
  Tenderly, which the run absorbed.
- Disk afterwards: **373 MB of sealed segments** (43,277 parquet files) and 0.6 MB of hot store.
- Peak memory was not measured.
- Counts in the sealed data: 107,462 BST transfers, 41,708 BST/ETH swaps, 3,722 property token transfers,
  2,108 governance pool deposits, 72 `Invested` events on the offering contract, 25 certified partners,
  **188 property tokens and 15 marketplace pools** discovered through the factories.
- Staking events, deposit and withdraw: community 346 and 324, issuer 141 and 84, RWA 78 and 67,
  landhive 9 and 6.
- **Not compared against Blocksquare's data.** I found no public GraphQL endpoint for the Envio
  deployment in the indexer repo, so none of these counts has been checked against it. They are what
  this nest holds, nothing more.

## What it does not do

- **No GraphQL.** Consumers read SQL (`nuthatch sql`, the HTTP `/sql` route, or MCP).
- **No handlers, so no handler math.** The entities in `schema.graphql` that the handlers compute are
  **not reproduced**: APY and reward calculation (`APYCalculation.ts`, the staking pool helpers),
  property and token valuation in currency (`OceanpointTokenValuation.ts`), USD pricing from the price
  feeds, limit order trade aggregation, referrals, and the `User`, `Wallet` and certified-partner
  aggregates. Their raw inputs are in the event tables, but these views do not compute them, and I did not
  check any figure against the Envio output.
- **Contracts in the indexer's config left out:** `ZeroEx` and `OneInchPostInteraction` (limit-order
  trades feed handler math, and ZeroEx is the shared 0x exchange proxy), `PriceFeedRegistry` and
  `PriceDataFeed` (only used for pricing), and Sepolia entirely. They can be added with `nuthatch add`.
- **Not on mainnet in their config, so absent here:** `UniswapV4PoolManager`, `UniswapV4PositionManager`
  and `UniswapV4Staking` (their mainnet addresses are empty strings in `mainnet.ts`, with a TODO for the
  deployment block), and `PropertyTokenOfferingV2` (`null` for mainnet).
- **Start blocks are 0.** Their mainnet config uses `start_block: 0`. Deployment blocks are not in the
  config or docs, and I did not derive them, so every contract starts at 0 and that is why the backfill
  scans 26 million blocks. The first BST transfer is at block 5,053,569. Setting a real `start_block` per
  contract would make the first sync much shorter.
- **Amounts are raw units.** BST has 18 decimals; divide by 1e18.
- **The views are mine, not a port.** They show event counts and net transfers, not Blocksquare's definitions.

## Run it

```sh
# This copy lives in nuthatch/examples/blocksquare-nest until a standalone repo exists.
cd examples/blocksquare-nest
nuthatch dev --dir .
nuthatch sql --dir . "SELECT * FROM property_tokens ORDER BY transfers DESC LIMIT 10"
```

`nuthatch dev --dir . --backfill 50000` indexes about the last week only, if you want to look first.
The full history needs a free endpoint that serves old logs; expect rate-limit retries.

## Views

- `property_tokens`: one row per property token the factory announced, with its transfer count and
  capital stack change count.
- `marketplace_pools`: one row per marketplace pool with its provider name, url, token, and deposit
  and withdrawal counts.
- `staking_events`: deposits and withdrawals across the four property staking pools, one row per event.
- `bst_balances`: BST balance per address, summed from `Transfer` events (raw units).

## Cost

No figures. I did not run the Envio setup and know nothing of what Blocksquare pays, so there is no
comparison here. What this run needed: one process, 373 MB of disk, free public RPCs.

## License

MIT OR Apache-2.0, as nuthatch.
