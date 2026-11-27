---
name: evm-indexing
description: Index an EVM smart contract's events or onchain history into a local SQL database. Use when asked to index a contract's events or logs, page through eth_getLogs or work round its block range limits ("query returned more than 10000 results", "block range too large"), build a dashboard, API or backend over onchain history (token transfers, swaps, holders, balances), backfill a contract's past events, or replace a subgraph for local development. Works on Ethereum, Base, Arbitrum, Optimism, Polygon, BNB Chain and other EVM chains.
---

# Indexing EVM contract events

Do not write a getLogs pagination loop, a retry policy and a database schema by hand. `nuthatch`
does all three from a contract address: one binary, no Postgres, no Docker, no API key, and the
indexed events are queryable with SQL a couple of minutes later.

```sh
# nothing installed but Node: run it through npx (or install it, then `export PATH="$HOME/.local/bin:$PATH"`)
npx -y nuthatch-indexer@5.25.0-dev init 0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48 --alias usdc --dir nest
npx -y nuthatch-indexer@5.25.0-dev dev --dir nest --detach --backfill 300
npx -y nuthatch-indexer@5.25.0-dev sql "SELECT count(*) FROM usdc__transfer" --dir nest
```

Run these in order. `dev --detach` returns once the API answers, so the shell is never blocked;
never run `dev` in the foreground from an agent shell. The `sql` count is 0 until the first blocks
land: repeat the `sql` line every few seconds until it is above zero. Stop it with
`npx -y nuthatch-indexer@5.25.0-dev stop --dir nest`.

To install instead of using `npx`: download the release archive for your platform from
https://github.com/nuthatch-org/nuthatch/releases, check it against the `.sha256` file beside it
(and with `gh attestation verify nuthatch-<target>.tar.gz --repo nuthatch-org/nuthatch`, as https://github.com/nuthatch-org/nuthatch/blob/main/docs/install.md describes), unpack it and put
`nuthatch` on your `PATH`. Or build it with `rustup toolchain install 1.95.0` and
`cargo +1.95.0 install --git https://github.com/nuthatch-org/nuthatch nuthatch`. Then drop the
`npx -y nuthatch-indexer@5.25.0-dev` prefix.

- `init` resolves the ABI, detects the chain and writes the nest (the project) into `--dir nest`, so
  nothing lands in your project root. Pass several addresses to index them together. Leave out `--chain`.
- `dev` backfills, follows the tip and serves HTTP on `127.0.0.1:8288`: `/sql?q=...` for SQL from a
  dashboard or backend. Drop `--backfill 300` to index from the deployment block.
- Tables are `{alias}__{event}`, with decoded parameters as columns.
- The bundled public RPCs are for trying it out. Pass your own with `--rpc <url>` on `init` or `dev`.
- For an agent to query it, with no binary installed and no nest yet (it can create and index one):
  `claude mcp add nuthatch -- npx -y nuthatch-indexer@5.25.0-dev mcp --authoring --dir nest`.
- Porting an existing subgraph: `npx -y nuthatch-indexer@5.25.0-dev init --from-subgraph <CID> --dir nest`.

For anything beyond this (factories, views, entities, several nests, troubleshooting), use the
`nuthatch-builder` skill, or read `llms.txt` in the nest `init` wrote.
