# Backfilling Ethereum history from Xatu's public Parquet

An opt-in history source for Ethereum mainnet. Instead of asking an RPC for `eth_getLogs` and for a
block header per log-bearing block, `nuthatch dev` reads old logs and block timestamps from the
public Parquet files [ethPandaOps](https://ethpandaops.io) publishes for their Xatu dataset, and
uses the RPC for the rest.

```text
nuthatch dev --history-source xatu
```

It is **off unless you name it**. Without the flag nothing about nuthatch changes: no request is made
to the dataset, and no code on the default path knows it exists.

## What it is

- A wrapper around the normal RPC source, not a second indexer. Decode, sealing and serving see the
  same logs and timestamps they would have had from the node; only where the bytes came from differs.
- A flag (or `NUTHATCH_HISTORY_SOURCE=xatu`), not a nest setting. Where history is read from does not
  change what a nest *is*, so a nest backfilled from the dataset has the same NID as one backfilled
  from RPC, and the two can share segments.
- Public, anonymous HTTPS GETs of static files. No account, no key, no identifier sent. If the
  dataset is unreachable, you lose nothing: the RPC serves everything, as it does today.

## When to use it

Use it when you backfill a **dense** contract's history on mainnet, or when your RPC provider does
not put `blockTimestamp` on logs (so every log-bearing block costs a header call). Both are where
most of an RPC bill goes. The measured comparison is at the end of this page.

Skip it for a sparse contract on a provider that is cheap for you: the RPC bill is already small,
and the dataset's files are large (tens of MB per 1,000 blocks) however few logs you want.
If you do not need timestamps at all, `block_timestamps = "null"` removes the header calls on every
chain with no dataset at all.

## Which chains

**Ethereum mainnet only** (chain id 1). On any other chain the flag is refused at start with a
message saying so, rather than ignored. The dataset starts after the Merge (block 15,537,394); a
nest that starts earlier is served from the RPC below that block, with no error.

## Lag

The dataset trails the chain head by **9 to 13 hours** (measured 2026-11-22: head 26,469,823, newest
file starting at 26,466,000). nuthatch only seals past finality, so the dataset is always behind the
seal line and the tail always comes from the RPC. You do not configure the handoff:

- A request is split at 1,000-block file boundaries. Files the dataset has are read from it; blocks
  beyond its newest file, and any file that failed, go to the RPC in contiguous runs.
- Parts are disjoint and taken in ascending order, so there is no gap and no duplicate.
- A missing file is remembered for ten minutes, so the tail does not ask the dataset on every poll.

## How it stays honest

A file is used only after three checks, and any failure falls back to the RPC for that 1,000-block
range with a `WARN xatu:` log line saying why:

1. The block file has exactly one row per block and an unbroken parent-hash chain. Files before about
   block 26.4M carry no parent hashes (the column is missing or empty), so there the RPC's hash is
   checked at the file's first block too: two RPC calls per 1,000 blocks, and the hashes of the
   blocks between are the dataset's word.
2. The logs file has the columns this reader needs, in the format it knows (hex text; anything else is
   refused, never guessed at).
3. The RPC's hash for the file's last block equals the dataset's: **one RPC call per 1,000 blocks**
   (two on the older files above).

What it cannot check: that every log of a block is *present*. The dataset carries no receipts root.
The equality below is measured on the sample it was measured on; if you need proof for a range you
care about, compare a few `eth_getLogs` answers against it, or leave the flag off.

Memory is bounded: the compressed bytes of four files (up to about 200 MB) and the decoded rows of
the one filter last asked of each.

Fixtures in CI (`tests/fixtures/xatu`) are real mainnet files for blocks 26,462,624 to 26,462,635
beside the recorded `eth_getLogs` and header answers for the same blocks; the tests assert the logs
and timestamps are identical field for field, and cover the handoff, a corrupt file, a hash
mismatch and a reorg notice.

## Options

| | |
|---|---|
| `--history-source xatu` | turn it on (mainnet only) |
| `--history-url <url>` | read the files from a mirror with the same layout |

Applies to single-nest `nuthatch dev`. A `mounts.toml` runtime and address-history nests do not use
it yet.

## Measured

`USDC` (`0xa0b8…eb48`, about 90 logs a block), backfilled the way the indexer's windows do it
(adaptive window, timestamps for log-bearing blocks, one checkpoint per window) once through
Alchemy alone and once through the dataset with Alchemy behind it, on 2026-11-22. The two runs
produced the same bytes: a digest over every log and its timestamp is equal in both rows.

| Blocks | Source | Wall time | HTTP requests to the RPC | Logs |
|---|---|---|---|---|
| 5,000 (26,300,000 on) | RPC only | 56.1 s | 488 | 483,027 |
| | Xatu + RPC | 26.1 s | 5 | 483,027 |
| 50,000 (26,200,000 on) | RPC only | 441.1 s | 4,176 | 4,161,008 |
| | Xatu + RPC | 267.2 s | 83 | 4,161,008 |

The 50,000-block row downloaded 1,899 MB (50 files); the 5,000-block row 176 MB. The remaining RPC
requests are the hash checks above. How to read it:

- Alchemy puts `blockTimestamp` on logs, so this run never paid for headers. On a provider that does
  not, the RPC-only column also carries up to one header lookup per log-bearing block (here, every
  block), and the saving is larger. That case was not run.
- It is one stream, windows in order, in a debug build, with the source in Rust and nothing else
  running. The indexer fans windows out concurrently, which speeds the RPC-only column more than this
  one. Treat the wall times as "no slower, usually faster", and the request counts as the result.
- Re-run it with `cargo test --lib xatu_bench -- --ignored --nocapture`; the variables are in the
  test's comment. It needs `ALCHEMY_API_KEY` and is not part of CI.
