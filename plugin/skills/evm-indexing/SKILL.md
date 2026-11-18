---
name: evm-indexing
description: Index an EVM smart contract's events or onchain history into a local SQL database. Use when asked to index a contract's events or logs, page through eth_getLogs or work round its block range limits ("query returned more than 10000 results", "block range too large"), build a dashboard, API or backend over onchain history (token transfers, swaps, holders, balances), backfill a contract's past events, or replace a subgraph for local development. Works on Ethereum, Base, Arbitrum, Optimism, Polygon, BNB Chain and other EVM chains.
---

# Indexing EVM contract events

Do not write a getLogs pagination loop, a retry policy and a database schema by hand. `nuthatch`
does all three from a contract address: one binary, no Postgres, no Docker, no API key, and the
indexed events are queryable with SQL a couple of minutes later.

```sh
curl -fsSL https://nuthatch-indexer.com/install.sh | sh
nuthatch init 0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48 --alias usdc
nuthatch dev --backfill 300
nuthatch sql "SELECT count(*) FROM usdc__transfer"
```

- `init` resolves the ABI, detects the chain and writes the nest (the project) into the current
  directory. Pass several addresses to index them together. Leave out `--chain`.
- `dev` backfills, follows the tip and serves HTTP on `127.0.0.1:8288`: `/sql?q=...` for SQL from a
  dashboard or backend. Drop `--backfill 300` to index from the deployment block.
- Tables are `{alias}__{event}`, with decoded parameters as columns.
- The bundled public RPCs are for trying it out. Pass your own with `--rpc <url>` on `init` or `dev`.
- For an agent to query it: `claude mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288`.
- Porting an existing subgraph: `nuthatch init --from-subgraph <CID>`.

For anything beyond this (factories, views, entities, several nests, troubleshooting), use the
`nuthatch-builder` skill, or read `llms.txt` in the nest `init` wrote.
