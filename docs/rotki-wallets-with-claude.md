# Use your rotki wallets with Claude through a nuthatch nest

An address-history nest (RFC-0063) watches accounts rather than contracts and serves their history
Etherscan-shaped at `/api`, from your own RPC, on your own machine. `nuthatch mcp` bridges a running
one to Claude, so you can ask about your own wallets and get answers that cite the blocks they rest
on and say what the nest does not hold.

rotki can read the same nest as its indexer, so the history Claude reads is the history rotki reads.
That support is in an open pull request, [rotki/rotki#13686](https://github.com/rotki/rotki/pull/13686),
and in the unofficial fork [nuthatch-org/rotki-nuthatch](https://github.com/nuthatch-org/rotki-nuthatch)
until it is merged.

## 1. Run a nest over your addresses

```toml
# ~/wallets/nuthatch.toml
[nest]
name = "wallets"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://your-ethereum-rpc"]

[address_history]
addresses = ["0xYourFirstAddress", "0xYourSecondAddress"]
# start_block = 15000000   # optional: history before it is never fetched
```

```sh
nuthatch dev --dir ~/wallets
```

On Ethereum the nest finds normal and internal transactions with `trace_filter`, so the RPC must serve
it; give a separate endpoint with `--trace-rpc` if yours does not. Other chains, and a directory serving
several chains at one URL, are in [examples/paywalled-chains](../examples/paywalled-chains/). The nest
backfills each address in the background; `curl -s 127.0.0.1:8288/ready` shows how far.

More addresses can be added while it runs, from the same machine:

```sh
curl -X POST -H 'content-type: application/json' \
  -d '{"address":"0xYourThirdAddress"}' http://127.0.0.1:8288/api/watch
```

## 2. Connect Claude

```sh
claude mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288
```

On an address-history nest the server offers four tools: `coverage` (the watched addresses, the
blocks held for each and what is missing, with why), `wallet_history` (one address over a block or
time window, decimals applied), `wallet_status` (the nest's `/ready`) and `diff` (what happened to the
watched addresses in a range, in fixed sentences). It also offers two prompts, `wallet-history` and
`coverage-gaps`. The event-table tools, such as `sql`, are not offered, because this kind of nest has
no SQL tables. Address-history nests do not serve `/mcp` over HTTP, so use the stdio bridge above.

## 3. Ask

- "Which of my addresses does the nest cover, and which blocks is it still missing?"
  Claude calls `coverage`. Each missing range is `pending` (watched and backfilling), `not_watched`
  or `never` (this nest will not hold it, for example a contract's transactions where they cannot be
  proved), counted from `start_block` to the finalized head.
- "What did 0xYourFirstAddress send and receive in March 2026?"
  Claude calls `wallet_history` with `from_time` and `to_time`. The nest places the times on blocks
  itself. Native amounts come back as `value_native`, token amounts as `value_formatted` with the
  token's symbol.
- "Which of my transactions in 2025 does the nest not cover?"
  Claude calls `coverage` and reads the missing ranges that fall in 2025, or calls `wallet_history`
  over the year and reports each kind the nest refused, with its reason.

## What the answers carry, and what they do not

Each `wallet_history` answer carries the nest's `generation` per kind, its finalized head, tip and
`served_through`, and a `not_seen` list. A kind whose window is not wholly covered is refused whole,
never shortened. Token names, symbols and decimals are the token contract's answers at the latest
block, read when the answer is built. An incoming transaction that moves no value is not listed. The
nest holds no prices, fiat values or balances; for those, use rotki.

The RPC provider sees the watched addresses in the requests the nest makes. The history itself stays
on your machine until you ask: `nuthatch mcp` passes an answer to the client that launched it, and
from there it goes to whichever model that client uses.
