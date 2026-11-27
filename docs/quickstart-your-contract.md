# Your contract's history as SQL in 10 minutes

Point nuthatch at one contract and get its event history as SQL tables on your own machine, with an
MCP server your coding agent can query. No Postgres, no subgraph, no handlers to write, and no
`eth_getLogs` windows to hand-roll. Checked against nuthatch 5.24.2. The blocks below were run
from an empty home directory against Base USDC and the Uniswap V3 USDC/WETH pool, and the timings were measured on 5.24.0.

## 1. Install

```sh
curl -fsSL https://nuthatch-indexer.com/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

or, with Node and no install, put `npx -y nuthatch-indexer` in front of every `nuthatch` below:

```sh
npx -y nuthatch-indexer --version
```

Measured: about 5 s for the installer, about 9 s for the first `npx` call.

## 2. Init with your address and chain

Use an empty directory, because `init` writes `nuthatch.toml`, `abis/` and the rest into the current one.
`--alias` is the table prefix, so choose a short one: without it the prefix is the ABI's contract name,
which for a proxy like USDC is `fiat_token_v2_2`.

```sh
mkdir usdc-nest && cd usdc-nest
nuthatch init 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 --chain base --alias usdc
```

`init` checks proxies. For USDC it prints a warning like this (block numbers are the real ones for your
run):

```text
⚠ usdc: the proxy starts at block <proxy_start>, but its current implementation was deployed at block <impl_start>.
  Events before block <impl_start> may use earlier implementation ABIs and will not be decoded by this scaffold.
```

The fix is a second `[[contracts]]` entry in `nuthatch.toml` at the same address, with the earlier ABI
saved under `abis/` and an `events` allowlist of the events the current ABI lacks:

```toml
[[contracts]]
alias = "usdc_v1"
address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
start_block = <proxy_start>
abi = "abis/usdc_v1.json"
events = ["<EventName>"]   # only events the current ABI lacks
```

Leave `--chain` out and `init` probes the known chains for the address. The ABI comes from Sourcify,
then Blockscout, then Etherscan if `ETHERSCAN_API_KEY` is set. A contract nobody has verified needs
`--abi path/to/abi.json`. Another contract to try, on mainnet:

```sh
nuthatch init 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640 --chain mainnet --alias pool
```

## 3. Choose an RPC

`init` gives the nest free public endpoints. They work for trying it out and they are shared and
rate-limited. `nuthatch doctor` measures what an endpoint really allows, so run it before a long backfill:

```sh
nuthatch doctor --rpc https://your-endpoint.example --address 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913
```

It prints the widest `eth_getLogs` block range the endpoint accepts for your contract, and the
`--window` it recommends. In plain words: `eth_getLogs` asks for a contract's logs between two blocks,
and providers refuse ranges that are too wide or that return too many logs. A **10-block limit** (the
Alchemy free plan's "up to a 10 block range" error) means a day of Ethereum, 7,200 blocks, is
720 or more requests instead of one. Wider is faster; a result cap of 10,000 logs makes a busy contract
need narrower windows whatever the range limit says.

Measured on 25 November 2026 with `nuthatch doctor --address`. These are measurements of one account
on one day, not vendor claims, and plans change:

| Endpoint | Chain | Widest window measured | Note |
|---|---|---|---|
| bundled Tenderly / mevblocker | mainnet | 320 / 160 blocks | rate-limited |
| bundled Tenderly / Nodies | Base | 640 blocks, batches of 5 | Nodies answered 403 to batches; the first Base USDC row took about 55 s |
| Alchemy (the account used here) | mainnet | 163,840 blocks | not the free plan's 10-block limit |
| Alchemy (the account used here) | Base | 2,560 blocks | for Base USDC; first row in 14 s |

Run `doctor` on your own key rather than reading this table: your plan decides the number. What we did
not measure: Alchemy's free plan itself (the account used here is not on it), GraphOps, and any other
provider.

Use your endpoint for the recent window or for the whole history:

```sh
export NUTHATCH_RPC=https://your-endpoint.example
nuthatch dev --backfill 300
nuthatch dev
```

From 5.24.2 both `init` and `dev` read `NUTHATCH_RPC`, so the URL stays out of argv and ordinary
process-list output (`ps`). Typing `export NUTHATCH_RPC=...` at the prompt still goes into shell
history, like any other command. `--rpc https://your-endpoint.example` does the same job as an
explicit flag, but it also puts the URL on the command line.

The URL can live in `nuthatch.toml` as `rpc_urls` instead. With a 10-block limit, nuthatch learns the
limit from the error and keeps going. Against a proxy that rejected every range over 10 blocks (a simulation of that error, not the plan itself), a day of Ethereum took well over ten minutes, so start with `--backfill`.

## 4. Run it

```sh
nuthatch dev --backfill 300
```

`--backfill 300` starts 300 blocks behind the tip (about an hour on Ethereum, ten minutes on Base), so
there are rows in seconds on a good endpoint. Without it `dev` backfills from the deployment block.
`dev` logs `caught up to tip` when it has the recent window and then follows the chain.

## 5. Three SQL queries

In a second terminal, in the same directory. Tables are `{alias}__{event}`. List them with `.tables`
in the REPL (`nuthatch sql`), which is also where `.schema usdc__transfer` shows the columns.
`from` is a SQL reserved word, so double-quote it. Token amounts are raw integers in a text column:
`value_dec` is the same number as a decimal you can sum (USDC has 6 decimals).

```sh
nuthatch sql 'SELECT count(*) AS transfers, min(block_number) AS first_block, max(block_number) AS last_block FROM usdc__transfer'
```

```sh
nuthatch sql 'SELECT "from", "to", value_dec / 1000000.0 AS usdc, block_number FROM usdc__transfer ORDER BY value_dec DESC LIMIT 5'
```

```sh
nuthatch sql 'SELECT "to" AS address, count(*) AS received, sum(value_dec) / 1000000.0 AS usdc FROM usdc__transfer GROUP BY "to" ORDER BY received DESC LIMIT 5'
```

Another contract's events have other names: the pool above has `pool__swap`, with `amount0`, `amount1` and
`sqrtPriceX96`.

## 6. Give your agent the data over MCP

`nuthatch dev` must be running. Claude Code:

```sh
claude mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288
claude mcp list
```

`claude mcp list` should report the `nuthatch` server as Connected. Without an install, use
`claude mcp add nuthatch -- npx -y nuthatch-indexer mcp --url http://127.0.0.1:8288`.

Cursor reads `.cursor/mcp.json` in the nest directory (Windsurf, Gemini CLI and Claude Code's
`.mcp.json` share the shape):

```json
{
  "mcpServers": {
    "nuthatch": { "command": "nuthatch", "args": ["mcp", "--url", "http://127.0.0.1:8288"] }
  }
}
```

Then ask: "how many USDC transfers are in the nest and who received the most?" The agent gets
schema discovery and read-only SQL. `init` also wrote `llms.txt`, `AGENTS.md` and a Claude Code
skill into the directory. Other clients: [AGENTS.md](../AGENTS.md#mcp).

## If it fails

- **`eth_getLogs` errors in the log** (a provider is refusing your windows): the page for each error says
  what it means and the flag to use:
  [`query returned more than 10000 results`](https://nuthatch-indexer.com/docs/errors/query-returned-more-than-10000-results/),
  [`Log response size exceeded`](https://nuthatch-indexer.com/docs/errors/log-response-size-exceeded/),
  [`exceed maximum block range`](https://nuthatch-indexer.com/docs/errors/exceed-maximum-block-range/),
  [`query timeout exceeded`](https://nuthatch-indexer.com/docs/errors/query-timeout-exceeded/).
- **`every RPC endpoint is rate-limiting us`**: the free bundled endpoints are shared. Set `NUTHATCH_RPC` (or pass `--rpc`).
- **`nuthatch sql` says it cannot find a store or anything answering**: `dev` is not running, or
  `--dir` is not the nest directory.
- **`no table`**: your `--alias` is the prefix; `.tables` lists the names.
- **Port 8288 is taken**: `nuthatch dev --listen 127.0.0.1:8299`, and `--url http://127.0.0.1:8299` on
  `sql` and `mcp`.
- **`no contract code` or no verified ABI**: check `--chain`; for an unverified contract pass `--abi`.
- Stuck anyway: [Discord](https://discord.gg/p9Xc45txWa), or [exit codes](cli-errors.md).
