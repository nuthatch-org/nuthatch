---
name: nuthatch
description: Query this self-hosted nuthatch nest on mainnet - decoded events, balances, and read-only SQL. Use when asked about on-chain activity for these contracts.
---

# Querying the nuthatch nest

Contracts indexed on mainnet:
- `blocksquare_token` = 0x509a38b7a1cc0dcd83aa9d06214663d9ec7c7f4a
- `certified_partners` = 0x8dbb99cc3721f5c9cc7c9e92db260813cf78cdd3
- `data_storage_proxy` = 0x8c2a858fe7b2bf155247c7f528c6ca7b186197b5
- `governance_pool` = 0x6f1e92fb8a685aaa0710bad194d7b1aa839f7f8a
- `liquidity_staking_pool` = 0x1802f66868d0649687a7a6bc9b8a4292e148daec
- `marketplace_pool_factory` = 0x17887106a14f38bf10512565bdbb5bd7ac12f001
- `property_factory` = 0x1ae91a263a690bf2129cf0b3acac92bbb67e6685
- `property_registry` = 0x05325c1ab1440df7214db38f676f95999729267b
- `property_revenue_distribution` = 0xbcb1757cdadec25b6cadc1a83953a70eda2da8cb
- `property_token_offering` = 0x25862c4fb4ce9d6ff9b463488e0ec656fa08de78
- `users` = 0x13344d0cb96b17df81c4171ce47e14ff6c1975f7
- `bst_eth_pool` = 0x0e85fb1be698e777f2185350b4a52e5ee8df51a6
- `staking_community` = 0x13299657e662894b933bb3ee73f7f8da94b55451
- `staking_issuer` = 0x57ba886442d248c2e7a3a5826f2b183a22ecc73e
- `staking_rwa` = 0x6f5ecf8a8dadaac3c8440a080c9271f845f34b07
- `staking_landhive` = 0xfc7cd245913691fb2d305a0fc60a1dadd1ecdebf
- `valuation_community` = 0xaeea40bb8393174459c4016bce2625076fe4deff
- `valuation_issuer` = 0x14db5b377d13ec9f9e7747f42d71634b5ef5a335
- `valuation_rwa` = 0x05f5b75d80291910a54b65aea9b45f549cdd0acc
- `valuation_landhive` = 0x3c18b35e8e919224ea4099acc0d280eda76a71c0

Data is local - never call an external API for it.

## Preferred: MCP
If a `nuthatch` MCP server is configured, use its tools. Call `schema` first to learn the
data model, then `sql` / `entity` / `balance` / `top_balances` (read `complete_history` in their results: when false or null the figures are `net_flow_since_indexed_from`, not balances; true means indexed from the configured `start_block` with no `--backfill`, the deployment if `start_block` is the deployment block, which `init` sets).

## Fallback: HTTP (a `nuthatch dev` must be running)
- Recent rows:  `curl localhost:8288/entities?limit=20`
- Read-only SQL: `curl -G localhost:8288/sql --data-urlencode 'q=SELECT count(*) FROM blocksquare_token__approval'`
- Balances: `GET /balances`, read with the status at `GET /`: the figures are net flow since its `start_block`, balances only when it says `history_from_deployment: true` (false or null means net flow). True means indexed from the configured `start_block` with no `--backfill`: the deployment only if `start_block` is the deployment block, which `init` sets

`sql` reads hot and sealed rows together, so it covers the live tip. Each answer's
`provenance.sealed_through` is the block through which this nest has sealed history; newer rows
may still be hot, and it is not the chain's finality head.
