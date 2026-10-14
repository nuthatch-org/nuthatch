# RFC-0064: The deposit sweep - one pass per block for a large, growing address set

- Status: **Accepted by Chief, 2026-10-14.** "I want us to deliver this but for EVM."
- Author: Pete (cargopete)
- Date: 2026-10-14
- Depends on: RFC-0063 (address history: its store, coverage, finality and `/api`), RFC-0010 Part B
  and RFC-0008 C5 (webhooks and their durable outbox).
- Tracking: #2044.
- Nature: new binary capability, decided by Chief and recorded here, per RFC-0044 §8 and CLAUDE.md.

## 1. The user and what he asked for

A payment processor (stake-machine, Discord, 2026-10-14) needs a deposit indexer: every incoming
transfer to its custody addresses, **hundreds of thousands of addresses**, with the list growing at
runtime (append-only is enough). He asked about non-EVM networks "eventually"; that stays out of
scope, and he was told so.

He found fystack/multichain-indexer, which does this across 14 chains with a bloom filter, NATS
JetStream and Redis. Nuthatch's case is EVM only: one binary, nothing beside it, answers in the
Etherscan shapes and SQL nuthatch already serves, and rows checked against Etherscan.

## 2. Why RFC-0063's discovery does not fit

Address mode discovers each address on its own: `trace_filter` both ways and `eth_getLogs` with the
address in a topic, window by window, per address. For one ordinary wallet over 1.19 million mainnet
blocks that was 20 minutes and 7,324 RPC calls (measured 2026-10-14). The cost grows with the number
of addresses; at 100,000 it is not a design.

## 3. The design: read each block once, match the whole set

A **sweep** reads every block from a start block onwards, once, and matches it against the watched
set in memory:

| What | From | Measured on mainnet, 2026-10-14, per block |
|---|---|---|
| Normal transactions, both directions | `eth_getBlockByNumber` with transactions | 0.2 s, 260-290 txs, ~400 KB |
| Token transfers (ERC-20, 721, 1155) | `eth_getLogs` by `Transfer`, `TransferSingle`, `TransferBatch` topic, no address | 0.2 s, 320-400 logs; 10 blocks in one call 0.38 s |
| Internal transfers (optional) | `trace_block` | 0.56 s, ~2,000 frames, ~2 MB |
| Receipts | only for matched transactions | |

**The cost is per block, not per address.** Following the mainnet tip costs about 1 s of work per
12 s block whether the set holds 10 addresses or 1,000,000. The set is an exact `HashSet` of 20-byte
addresses, 2 MB at 100,000, not a bloom filter: there is nothing to gain from false positives at that
size.

**Backfill is the expensive direction**: about 3 calls a block, so a million blocks is roughly 35
hours at 8 in parallel. The default is therefore to sweep from the head at first start. History for
an individual address, when wanted, comes from RFC-0063's per-address discovery, which already
exists and is the cheaper path for a few addresses.

Matched rows go into RFC-0063's existing tables, so `txlist`, `txlistinternal`, `tokentx`,
`tokennfttx` and `token1155tx` answer for any swept address exactly as they do now, and
`NUTHATCH_INCOMPLETE` still answers a range the sweep has not covered.

## 4. A growing set

Addresses are appended at runtime through `/api/watch`, extended to take a list (thousands per call)
and unchanged in being loopback-only. An address joins the sweep at the next block the sweep reads,
and the store records that block as its **joined_at**. Its sweep coverage is `[joined_at, swept
head]`; nothing before `joined_at` is claimed, so a query below it answers incomplete, never empty.

**Coverage is held once for the sweep, not per address.** Per-address coverage rows, rewritten every
poll for 100,000 addresses, would turn a cheap pass into a large write. The sweep keeps one span and
each address keeps its `joined_at`; an address's coverage is the union of its own RFC-0063 coverage
and the sweep span from `joined_at`.

## 5. One feed for the whole set

Polling 100,000 addresses one by one is what this replaces, so the sweep adds one read and one push
across the set:

- **`module=account&action=nuthatchDeposits`**: every incoming row to any watched address over a block
  range, all kinds together, paged in block order like the other actions, with the same
  `NUTHATCH_INCOMPLETE` refusal above the swept head.
- **Webhooks**: matched deposits delivered through the existing durable outbox (RFC-0008 C5),
  at-least-once, with a retraction when a reorg takes a delivered row back. A processor credits on
  delivery and reverses on retraction, or waits for the confirmation depth it chooses.

## 6. Reorgs and finality

Unchanged from RFC-0063. Rows above the finalized head are anchored by block hash and taken back
when the chain moves under them; the sweep's span is cut back to the fork point and re-read. The
webhook retraction is the outside world's view of the same event.

## 7. Configuration

```toml
[address_history.sweep]
from_block = "head"   # or a block number; "head" means wherever the chain is at first start
internal = true       # trace each block for internal transfers; needs a trace-capable RPC
```

Absent the table, a nest behaves exactly as today. `addresses` may stay empty; the set comes from
`/api/watch`.

## 8. Against the non-negotiables

- **Single binary, nothing beside it.** No NATS, no Redis: the store is the nest's redb, delivery is
  the existing outbox.
- **≤2 GB per cursor.** The set is 2 MB at 100,000 and 20 MB at a million. A block's data is a few
  MB and is dropped once matched. Gated in S4.
- **Determinism.** Matching is exact set membership over chain data; no model anywhere in the path.
- **No phone-home.** The RPC sees what it already sees. Nobody else learns an address.
- **Single-cursor law.** One sweep per chain, on that chain's cursor.

## 9. Slices and their gates

- **S0, the spike, which can stop the programme.** A standalone sweep following the mainnet tip for
  an hour against a set of 100,000 addresses, a few of them real and active. Gate: every row for the
  real addresses equals Etherscan's for the same blocks; peak RSS and calls per block recorded.
- **S1, the sweep in the binary.** Normal transactions and token transfers, the set from config and
  `/api/watch` with bulk append, sweep coverage and `joined_at`. Gate: Etherscan parity for pinned
  addresses over a pinned block range; a reorg property test over the sweep span.
- **S2, internal transfers.** `trace_block` per block behind `internal = true`. Gate: `txlistinternal`
  parity on pinned cases.
- **S3, the feed.** `nuthatchDeposits` and webhook delivery with retraction. Gate: a reorg in a test
  chain retracts exactly the rows it took back, once each.
- **S4, scale.** 100,000 and 1,000,000 addresses: RSS under the cursor budget, tip lag measured,
  append latency measured. Gate: the budget.
- **S5, more chains.** Base, BSC, Polygon, Arbitrum, each with its own trace method (RFC-0063 §7's
  table) and a measured per-block cost against its block time. Base produces a block every 2 s, so
  the per-block budget there is a sixth of mainnet's.

## 10. What this does not do

Non-EVM chains. Balances (a processor reads them from its own node). Address derivation or key
management. A hosted deposit service: a processor runs the stock binary itself.

## 11. Open questions

1. Whether `nuthatchDeposits` should also carry outgoing rows (sweeps to a hot wallet), or stay
   incoming-only as the name says.
2. The confirmation depth webhooks should wait for by default: none (deliver at the tip, retract on
   reorg) or a configured depth.
3. Whether S0 should run on Base as well as mainnet, given its 2 s blocks.
