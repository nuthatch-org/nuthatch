---
name: nuthatch
description: Query this self-hosted nuthatch nest on mainnet - decoded events, balances, and read-only SQL. Use when asked about on-chain activity for these contracts.
---

# Querying the nuthatch nest

Contracts indexed on mainnet:
- `uniswap_v2pair` = 0x9d593420cc4936b8a8435cdfa0d6bbde9bdff9a4

Data is local - never call an external API for it.

## Preferred: MCP
If a `nuthatch` MCP server is configured, use its tools. Call `schema` first to learn the
data model, then `sql` / `entity` / `balance` / `top_balances` (read `complete_history` in their results: when false or null the figures are `net_flow_since_indexed_from`, not balances; true means indexed from the configured `start_block` with no `--backfill`, the deployment if `start_block` is the deployment block, which `init` sets).

## Fallback: HTTP (a `nuthatch dev` must be running)
- Recent rows:  `curl localhost:8288/entities?limit=20`
- Read-only SQL: `curl -G localhost:8288/sql --data-urlencode 'q=SELECT count(*) FROM uniswap_v2pair__approval'`
- Balances: `GET /balances`, read with the status at `GET /`: the figures are net flow since its `start_block`, balances only when it says `history_from_deployment: true` (false or null means net flow). True means indexed from the configured `start_block` with no `--backfill`: the deployment only if `start_block` is the deployment block, which `init` sets

`sql` reads hot and sealed rows together, so it covers the live tip. Each answer's
`provenance.sealed_through` is the block through which this nest has sealed history; newer rows
may still be hot, and it is not the chain's finality head.
