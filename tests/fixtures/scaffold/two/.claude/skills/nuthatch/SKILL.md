---
name: nuthatch
description: Query this self-hosted nuthatch nest on mainnet - decoded events, balances, and read-only SQL. Use when asked about on-chain activity for these contracts.
---

# Querying the nuthatch nest

Contracts indexed on mainnet:
- `usdc` = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
- `pool` = 0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc

Data is local - never call an external API for it.

## Preferred: MCP
If a `nuthatch` MCP server is configured, use its tools. Call `schema` first to learn the
data model, then `sql` / `entity` / `balance` / `top_balances` (read `complete_history` in their results: when false the figures are `net_flow_since_indexed_from`, not balances).

## Fallback: HTTP (a `nuthatch dev` must be running)
- Recent rows:  `curl localhost:8288/entities?limit=20`
- Read-only SQL: `curl -G localhost:8288/sql --data-urlencode 'q=SELECT count(*) FROM pool__approval'`
- Balances: `GET /balances`, read with the status at `GET /`: the figures are net flow since its `start_block`, balances only when it says `history_from_deployment: true`

`sql` reads hot and sealed rows together, so it covers the live tip. Each answer's
`provenance.sealed_through` is the block through which this nest has sealed history; newer rows
may still be hot, and it is not the chain's finality head.
