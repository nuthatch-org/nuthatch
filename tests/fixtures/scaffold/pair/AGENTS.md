<!-- Written by nuthatch from nuthatch.toml; `nuthatch schema` rewrites it. -->
# AGENTS.md

This directory is a nuthatch nest: a local SQL index of these contracts on mainnet.
Query it here; never call a third-party API for this data.

- `uniswap_v2pair` = 0x9d593420cc4936b8a8435cdfa0d6bbde9bdff9a4

## Tables
- `uniswap_v2pair__approval` - Approval(address,address,uint256) (block_number, block_hash, block_timestamp, tx_hash, log_index, address, _seq, owner, spender, value)
- `uniswap_v2pair__burn` - Burn(address,uint256,uint256,address) (block_number, block_hash, block_timestamp, tx_hash, log_index, address, _seq, sender, amount0, amount1, to)
- `uniswap_v2pair__mint` - Mint(address,uint256,uint256) (block_number, block_hash, block_timestamp, tx_hash, log_index, address, _seq, sender, amount0, amount1)
- `uniswap_v2pair__swap` - Swap(address,uint256,uint256,uint256,uint256,address) (block_number, block_hash, block_timestamp, tx_hash, log_index, address, _seq, sender, amount0In, amount1In, amount0Out, amount1Out, to)
- `uniswap_v2pair__sync` - Sync(uint112,uint112) (block_number, block_hash, block_timestamp, tx_hash, log_index, address, _seq, reserve0, reserve1)
- `uniswap_v2pair__transfer` - Transfer(address,address,uint256) (block_number, block_hash, block_timestamp, tx_hash, log_index, address, _seq, from, to, value)

## Run it headless

`nuthatch dev` runs until it is stopped. `--detach` starts it in the background, with its log
outside the nest (whose identity counts every file in it), and `--wait-rows` returns only once
`/ready` reports a row is indexed, so the `sql` below sees data:

```sh
nuthatch dev --backfill 300 --detach --wait-rows
```

It exits 0 only when this `dev` is serving with rows. Exit 10 (`port_in_use`) means another
process holds the port and nothing was started: do not query then, you would read the other
nest or an empty one. Exit 11 (`rows_timeout`) leaves it running; the log it prints says why.
`nuthatch stop` stops it. `--backfill 300` starts 300 blocks behind the tip; without it `dev`
reads from each contract's start block. A nest that has already indexed ignores it.

## Query

```sh
nuthatch sql "SELECT count(*) FROM uniswap_v2pair__approval" --json
```

`llms.txt` has the HTTP routes and the SQL type rules: wide integers are text with a `_dec`
companion, and `"from"` and `"to"` must be double-quoted.

## MCP

`nuthatch mcp --print-config --client <name>` prints the config file for `claude`, `cursor`,
`codex`, `vscode`, `windsurf` or `gemini`, and says on stderr where it goes. The server bridges
to the running `nuthatch dev`; call its `schema` tool first.
