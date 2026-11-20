# Agent prompt verification: arc-usdc

Nest: `https://nuthatch-indexer.com/nests/arc-usdc` (nuthatch 5.23.0), run 2026-11-19.
Prompts are the five in `showcase/configs/arc-usdc.json`.

## Status: NOT verified with `claude -p`

The brief asked for each prompt to be run with `claude -p` (haiku) against the nest's MCP. That could not be
done: the build session's `claude` CLI answered `Not logged in · Please run /login` for every call (no
credentials were available there, and none were looked for). **So no model has answered these prompts yet.**

What was done instead, and what it does and does not show:

- The MCP endpoint was exercised directly over JSON-RPC (`initialize`, `tools/list`, `tools/call`), with no
  key, through the public proxy. The server lists `status`, `schema`, `tables`, `table`, `sql`, `at`,
  `explain`, `entity`, `diff`, `balance`, `top_balances`.
- For each prompt, the tool call a competent agent would make was run, and it returned the data needed to answer.
  That shows each prompt is answerable from the nest's tools, not that haiku phrases the query correctly.
- Prompt 3 was reworded after this: the `status` tool has no chain tip, so "how far behind the tip" is not
  answerable over MCP (it is on the page, from `/ready`). The new wording asks for the newest block and timestamp.

To finish the check, on a logged-in machine:

```sh
claude mcp add --transport http arc-usdc https://nuthatch-indexer.com/nests/arc-usdc/mcp
claude -p --model haiku "Who were the 10 largest USDC senders in the last 24 hours on this nest? Show amounts in USDC (6 decimals)."
```

and paste the answers below.

## Tool results per prompt

| # | Prompt | Tool call | Result |
|---|---|---|---|
| 1 | 10 largest USDC senders, last 24h | `sql`: sum `value_dec` by `"from"`, last 86400 s | 10 rows. Top: the zero address (mints) 3,129,035.79 USDC, then `0xbb33...3240` 662,052.23 USDC over 11,174 transfers, `0x15b5...0a80` 152,505.73 |
| 2 | Transfers per hour, last 12h, busiest hour | `sql`: `date_trunc('hour', ...)` count | 13 hourly rows (the first and last partial). Busiest: 07:00 UTC with 21,420 |
| 3 | Latest block and newest transfer time | `sql`: `max(block_number)`, `max(block_timestamp)` | Returns the latest indexed block (73,062,400 at the time); `status` also gives `last_block` and `sealed_through` |
| 4 | Largest USDC transfer ever | `sql`: `ORDER BY value_dec DESC LIMIT 1` | Block 72,688,874, 2026-11-17 13:43:11 UTC, 100,000,000 USDC from the zero address (a mint) to `0x4e42...b37bf` |
| 5 | Every table with row count | `tables`, then `sql` counts | 11 tables; `usdc__transfer` has 1,394,910 rows |

Raw output of the calls is in the build log, not committed. These figures change as the nest follows the tip.

## The page's own queries

`showcase/check-examples.sh https://nuthatch-indexer.com/nests/arc-usdc` ran all five example queries with no
error: 20, 10, 25, 2 and 10 rows.
