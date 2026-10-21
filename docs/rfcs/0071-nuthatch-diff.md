# RFC-0071: The diff - what happened between block X and block Y, as a story

- Status: **Proposed 2026-10-21, awaiting Chief.** One of five "fun" RFCs Chief asked for on
  2026-10-21 (0068 to 0072), planned in parallel and to be built one after another.
- Author: Pete (cargopete)
- Date: 2026-10-21
- Depends on: RFC-0016 (the semantic layer, `semantic.toml`), RFC-0008 (labels and flags), RFC-0061
  (256-bit values stay decimal text), RFC-0023 tier 2 (`metadata.json`, token decimals), RFC-0063
  (address history) for §7's third column only.
- Blocks: nothing.
- Nature: new query surface in the default binary, if accepted as recommended in §9. That is the case
  RFC-0044 §8 says must be decided by Chief and recorded in `CLAUDE.md`; this RFC does not edit it.

## 1. What it is for

Given a nest and a block range, or one transaction, a new `diff` subcommand writes a short account
of what changed for the contracts the nest indexes: who moved what, whose balances moved most, who
held the token for the first time, which approvals were granted, and whether anything changed hands
at the control plane: ownership, admin, roles, pauses, upgrades. Rows become sentences by fixed
templates. No model writes a word of it.

Two readers want it. An operator who looks at a nest once a day wants "what happened since I last
looked" without writing six queries. And someone looking at an exploit wants the flow of funds in
the attack block laid out, exactly, with the amounts the chain recorded. The second is the more
demanding case, and most of this RFC is about being honest about what a nest can see of it.

## 2. What exists, read 2026-10-21 at `origin/main`

- **Decoded tables.** Every event table `{alias}__{event}` carries the implicit columns
  `block_number`, `block_hash`, `block_timestamp` (unless the nest is timestamp-free), `tx_hash`,
  `log_index`, `address` and `_seq` (`decode/src/registry.rs:368`), then the event's parameters.
  Every table has a `topic0` and a canonical signature, which is what a template can key on without
  trusting a name.
- **`semantic.toml` carries prose, not kinds.** `Semantic` (`scaffold/src/semantic.rs:15`) holds a
  description per nest, table, column and view, plus derived footguns (`reserved_words`, `big_ints`,
  `overflows_dec`, `bools`). Nothing in it says "this table is a transfer" or "this event is an
  ownership change". The only shape detector in the tree is `TableSchema::is_transfer_shaped`
  (`decode/src/registry.rs:337`): a `__transfer` table whose parameters are `address, address, uint`.
  `semantic.toml` is outside the data identity (`identity/src/lib.rs:277`), so anything added to it
  later moves the NID but never forces a re-index.
- **Decimals live in `metadata.json`**, an optional per-nest cache of `decimals()`, `symbol()` and
  `name()` keyed by address (`src/metadata.rs`), filled by the metadata fetch subcommand. A nest that
  never ran it has no decimals at all.
- **Built-in views are current state.** `balances`, `exposure` and `velocity` are DBSP views of the
  state at the head, in i128 base units. None of them answers "what was the balance at block X", so a
  diff cannot be a subtraction of two balance snapshots. It is a fold over the transfer rows in the
  range.
- **Labels** are content-addressed snapshots of `{address, label}` (`src/labels.rs`), queryable as
  `labels` from `/sql`. **Threshold flags** are per-transfer annotations keyed `{block}-{log}-thr`
  (`src/flags.rs`).
- **Call rows.** With `[extract] top_level_calls = true`, a transaction sent directly to an indexed
  contract becomes a row keyed by 4-byte selector, with `tx_from` (`src/calldata.rs:341`). The rows
  carry no ETH value. `[extract] traces` and `state` are refused at startup: no source is wired until a
  colocated node exists (`src/indexer.rs:3008`).
- **Address history** (RFC-0063) keeps `txlist`, `txlistinternal` and `tokentx` rows per watched
  address in redb tables (`ah_txlist` and so on, `src/address_history.rs:292`), served
  Etherscan-shaped at `/api`. Internal transactions come from `trace_filter`. These rows are not
  Burrmill tables and are not reachable from `/sql`.
- **MCP.** Seven generic tools plus shape-gated ones (`src/mcp.rs:326`); `balance` and
  `top_balances` are advertised only where a transfer-shaped decoder exists. A `diff` tool fits the
  same pattern.

## 3. Inputs and outputs

**Inputs**, exactly one of:

- a block range, `--from X --to Y`, inclusive;
- one block, `--block N`, which is the range N to N;
- one transaction, `--tx 0x…`. The diff is the rows whose `tx_hash` matches, in the block that
  contains it. Finding that block is the only step that may use the nest's RPC
  (`eth_getTransactionReceipt`), because a scan of every table for one hash is unbounded. `--block N`
  alongside `--tx` skips the call. The RPC tells the diff where to look and contributes nothing to
  what it says;
- `--since-last`: from the block after the previous diff's `to`, up to the sealed boundary. The
  cursor is kept by the client in the operator's state directory, keyed by the nest's data identity,
  and **never in the nest directory**: every file there counts towards the NID, hidden ones included
  (`identity/src/lib.rs`), so a cursor file would change the nest's identity each time it was used.

**Surfaces.** The subcommand reads a nest directory directly, or a running instance through `--url`,
in the same way `nuthatch sql` does. The running instance gains `GET /diff` with the same parameters,
and the MCP server gains a `diff` tool that calls it. All three return the same document; the CLI and
the MCP tool render it, the route returns it.

**Formats.** `--format text` (default, for a terminal), `markdown` (for an issue or an incident
channel), `json` (the document itself, which the other two are rendered from), and for §8's forensic
graph, `dot` and `mermaid`.

A text diff, for illustration:

```
Blocks 21,000,000 to 21,000,099 on mainnet (chain 1). All final: sealed through 21,004,212.
Nest pair, data identity 3f9a0c…e1, 1,214 rows across 4 tables.

Control
  21,000,042  OwnershipTransferred: owner of pair changed from 0x1111…  to 0x2222…
Largest movements, USDC (6 decimals)
  21,000,017  0x3333… sent 1,250,000.000001 USDC to 0x4444… [exchange]
  …
New holders, USDC: 37
Approvals: 12, of which 2 unlimited (2^256-1)
  21,000,090  0x5555… approved 0x6666… to spend any amount of USDC
Other events
  Sync(uint112,uint112) x 312
  Swap(address,uint256,uint256,uint256,uint256,address) x 296
Not seen by this nest: native ETH, internal calls, tokens it does not index.
```

Addresses are printed in full in every format; they are abbreviated above for the page width only.
The last line is not decoration: it is the per-kind statement of §7, printed every time, because an
empty or short diff must never read as "nothing happened".

## 4. How rows become sentences

**A fixed catalogue, keyed on topic0 and parameter shape.** The binary ships templates for event
signatures whose meaning is a standard rather than a convention: ERC-20 `Transfer` and `Approval`,
ERC-721 `Transfer` (told apart from ERC-20 by the indexed `tokenId`), `ApprovalForAll`, ERC-1155
`TransferSingle` and `TransferBatch`, `OwnershipTransferred`, `Paused` and `Unpaused`, ERC-1967
`Upgraded`, `AdminChanged` and `BeaconUpgraded`, and AccessControl's `RoleGranted`, `RoleRevoked` and
`RoleAdminChanged`. Keying on topic0 and types, not table names, means a contract that names its
event `Transfer` but emits something else gets the fallback, not a wrong sentence. Mint and burn are
transfers from or to the zero address and read as such.

**An author's template, later (S4).** Most real nests are dominated by events no standard names:
Horizon's staking events, a pair's `Sync`. S0 measures how much (§11). The proposed answer is an
optional `story` line per table in `semantic.toml`, for example
`story = "{indexer} staked {tokens:GRT} with {serviceProvider}"`, checked by the existing drift check
so a template naming a column the table lacks is caught at `dev` time. It sits outside the data
identity, so adding one never re-indexes.

**The fallback** is the signature and a count: `Sync(uint112,uint112) x 312`, with the first and
last block. It is never dropped: every row in the range is accounted for in some line or count, and
the JSON carries a per-table row count that must sum to the range's total.

**Ranking** is a fixed order of sections, then a fixed order within each:

1. **Control plane**: every ownership, admin, role, pause and upgrade event, all of them, in chain
   order. These are never ranked out; a diff that hid an `Upgraded` under a cap would fail its main
   job.
2. **Largest movements, per token**: the top K transfers by amount, and the top K addresses by net
   flow, computed per token. There are no prices in the core, so a diff never compares amounts of two
   tokens and never orders tokens by value. Tokens are ordered by row count, then address.
3. **First-ever**: addresses whose first transfer-in of that token is in the range (new holders), and
   tables whose first row is in the range (the first `Upgraded` this contract ever emitted).
4. **Approvals**, with unlimited approvals (2^256-1) called out by name.
5. **Flags and labels**: threshold flags raised in the range, and every labelled address that
   appears, with its label.
6. **Everything else**, as fallback counts.

Ties break on `(block_number, log_index)`, then address bytes. K defaults to 10 and is a flag.

**Exact numbers.** Amounts are read as the stored decimal text (RFC-0061) and scaled by the token's
`decimals` from `metadata.json` by moving the decimal point, never through a float. Trailing zeros of
the fraction are trimmed and nothing else is: 1,250,000.000001 USDC is printed as that. Where
`decimals` is unknown the amount is printed in base units with "(decimals unknown)", and the footer
says how to fetch them; a diff never guesses 18. The JSON carries the base-unit string and the
decimals separately so a reader can redo the arithmetic.

## 5. Determinism

The same nest, the same range and the same binary give byte-identical output. The output names
everything it depends on in a provenance footer: the data identity, the range and its endpoint block
hashes, `sealed_through` at the time of the diff, the binary version (which fixes the template
catalogue), and the hashes of `metadata.json`, the labels snapshots and `semantic.toml`. It contains
no wall-clock time. A change in any of those is a different diff, and the footer says which.

**Unfinalised ranges are allowed and labelled.** A range that reaches above `sealed_through` is read
from hot ∪ sealed like any `/sql` query, and the header says "provisional: blocks N to M are not final
and may be reorganised", per block span, and the JSON carries `final: false`. After a reorg the same
request gives a different, correct answer for the new canonical chain; the endpoint hashes in the
footer show that the input moved. `--since-last` advances its cursor only to the sealed boundary, so
an operator's daily diff never skips a block a reorg rewrote. A `--final-only` flag refuses a range
that is not entirely sealed, for anyone who wants that as a guarantee rather than a label.

Nothing here feeds stored state. A diff is presentation over rows the nest already serves; it stores
nothing (the `--since-last` cursor is client-side), so non-negotiable 4 is not engaged.

## 6. Cost and bounds

A diff is a handful of Burrmill aggregations over hot ∪ sealed, each a `GROUP BY` over the rows in
range: counts per table, sums per (token, address), top K per token. Memory is bounded by the number
of distinct (token, address) pairs in range, not by rows, and every query runs under the existing
`/sql` resource limits (the 512 MB and two-thread analytical budget RFC-0048 lists), inside the
cursor's 2 GB.

- **The bound is rows, not blocks.** A block range says nothing about volume: 100 blocks of a busy
  pair can outweigh a month of a governance contract. The diff counts the rows in range first, which
  is cheap from segment statistics, and refuses above a cap (proposed 2,000,000 rows, measured in S1)
  with the count and a narrower range to try. It never truncates silently.
- **New holders is the expensive section.** It asks whether each address touched in range was ever
  touched before X, a semi-join against the whole history of that transfer table. It is computed for
  the touched set only and under its own budget; if that is exceeded, the section is omitted with a
  line saying so and why, not left out silently.
- **Coverage.** A range below the nest's start block or above its indexed head is refused with the
  covered interval, rather than answered as empty.

## 7. What each kind of nest can see

| | Event nest (default) | Event nest with `top_level_calls` | Address-history nest (RFC-0063) |
|---|---|---|---|
| Token transfers of indexed contracts (and factory children) | Yes, exact | Yes, exact | Only those touching a watched address, any token |
| Tokens the nest does not index | No | No | Yes, where a watched address is a party |
| Who sent the transaction (`tx.from`) | No | Only for transactions sent directly to an indexed contract | Yes, for watched addresses' transactions |
| Functions called on indexed contracts | No, only what they emitted | Top-level calls only, decoded by selector | No |
| Native ETH moved by a transaction | No | No, call rows carry no value | Yes, for watched addresses |
| Internal calls and internal ETH transfers | No | No | Yes, from `trace_filter`, for watched addresses |
| Reverted calls, storage changes | No | No | No |

So for an ordinary event nest, the flow of funds is token transfers of indexed tokens and nothing
else. An exploit that drains ETH through an attacker contract, or moves a token the nest does not
index, is invisible to it, and the diff says so in its last line. The address-history column is
narrower than it looks: it sees everything around a watched address and nothing between two
addresses neither of which is watched, and it can only be added to an exploit after the fact by
watching the victim or attacker address and backfilling. Its rows live in redb rather than Burrmill,
so reading them is S5's own work. Call traces and state diffs wait for the colocated node (slice 6),
and the diff gains nothing from them until then.

## 8. Forensic mode

`--flow` turns a tx or block diff into a graph: one node per address, one edge per (from, to, token)
with the summed exact amount and the transfer count, edges ordered by the `(block, log_index)` of
their first transfer. Nodes carry their labels and, for indexed contracts, their nest alias. The zero
address is drawn as "minted" and "burned". Text output is the numbered edge list; `dot` and
`mermaid` render the same graph with node ids derived from the address, so two runs give the same
file. Flows larger than K edges keep every edge in JSON and the top K per token in the drawing, and
the drawing says how many it left out.

What forensic mode will not do: infer intent ("attacker", "victim"), net a cycle into a single edge,
price anything, or fill a gap with an RPC call. A flash loan appears as the two transfers it was.

## 9. Default binary or a feature

**Recommendation: the default binary.** It is read-only presentation over rows the nest already
serves: no new stored state, no new config, no on-disk change, no background work, no network call
other than the optional receipt lookup in §3, which goes to the operator's own RPC. The forensic case
argues for it most: an operator in the middle of an incident should not need to rebuild with a
feature switched on to find out where the funds went.

RFC-0046 §1's deletion test, applied honestly: deleting the diff from the tree costs a self-hoster a
convenience and nothing they depend on, since every number in it can be rebuilt by hand from `/sql`.
That is the test passing in the sense that matters here, because the test exists to catch something
that gates or withholds function, and this withholds nothing. What a default-binary feature does
change for someone who never uses it, and the record should say so: one more subcommand in `--help`,
one more MCP tool in every agent's tool list (a few hundred tokens of its context), one more route,
and some binary size for the templates, measured in S1. If Chief prefers the RFC-0059 posture, a
`diff` cargo feature off by default costs nothing in design; the slices are the same.

## 10. Non-goals

- No LLM, anywhere. The sentences are templates; an agent that wants prose can read the JSON.
- No prices or USD values. Nothing in the core has them, and a diff will not fetch them.
- No historical balances. The diff reports flows in range, not "balance at block X", which no view
  holds.
- No traces through `debug_*` RPC to fill §7's gaps. That was declined for `[extract]` and is declined
  here for the same reason.
- No intent labelling, risk scoring or alerting. Alerts are `src/alerts.rs`'s business.
- No diff across nests or chains. One diff, one nest, one cursor.

## 11. Slices and their gates

Each gate is written so it can fail.

- **S0, the spike, which can stop this.** Two measurements, no binary change. (a) Build a nest over
  the tokens of a public exploit and compare its flow, from hand-written SQL, against a published
  post-mortem. Candidate: Euler Finance, mainnet, 2023-03-13, whose loss moved through DAI, USDC,
  WBTC and stETH and is documented by Euler and by several auditors; the transaction hashes and
  figures are to be taken from the post-mortem, not from memory. **Gate:** every leg of the
  post-mortem's flow involving an indexed token appears with the exact amount, and every leg the
  nest misses is one §7 predicts it misses. A leg missed that §7 says is visible stops the forensic
  half until §7 is corrected. (b) Over one week of five real nests (Lodestar, defi-pulse, two
  stopgap nests, one ERC-20), count the share of rows §4's fixed catalogue covers. **Gate:** at least
  half the rows on at least three of the five. Below that, the story is mostly fallback counts, S4
  moves ahead of S1, and the RFC goes back to Chief with the numbers.
- **S1, range diff.** Range and single block, text and JSON, the fixed catalogue, ranking, exact
  numbers, provenance footer, finality labelling, the row cap. **Gates:** golden fixtures give
  byte-identical output on two runs and on macOS and Linux; a reorg property test (random depths
  against the hot store) gives, after each reorg, the same diff as one computed from scratch on the
  canonical chain; on a copy of the largest Lodestar nest, the largest range under the cap stays
  within the cursor's 2 GB with the RSS reported; a range over the cap is refused, not truncated.
  Mutation check: a template that rounds an amount must turn a golden red.
- **S2, the other inputs and surfaces.** `--tx`, `--since-last`, Markdown, `GET /diff` and the MCP
  tool. **Gates:** the MCP tool's JSON, the route's and the CLI's are byte-identical for one request;
  `--since-last` run daily across a reorg in the hot range neither skips nor repeats a block; `--tx`
  with no RPC reachable and no `--block` fails with a message naming `--block`.
- **S3, forensic mode.** `--flow`, text, DOT and Mermaid. **Gate:** S0's exploit, run through the
  binary, reproduces every in-scope post-mortem leg with exact amounts, and the DOT file is
  byte-identical across two runs.
- **S4, author templates.** `story` in `semantic.toml`. **Gates:** adding a `story` leaves the data
  identity unchanged; a `story` naming a missing column is a drift warning at `dev`; S0(b)'s coverage
  re-measured on Lodestar with templates written for it.
- **S5, address-history source.** Read `txlist`, `txlistinternal` and `tokentx` for watched
  addresses into the diff and the flow graph. **Gate:** for one watched address over one range, the
  diff's ETH and internal-transfer lines match Etherscan's `txlistinternal` for the same range,
  exactly.

## 12. Risks

- **A fluent sentence is believed.** A diff that reads well and misses half the flow is worse than a
  table. The §7 line on every output and the per-table row counts that must sum to the range total
  are the guard; S0(a) is the test of whether they are enough.
- **Fallback-dominated output.** If most real nests are custom events, the diff is a count table
  with a header. S0(b) measures it before anything is built.
- **New holders is a full-history query.** Its cost grows with the nest's age. If S1's measurement is
  poor, the section becomes opt-in.
- **`metadata.json` is optional and often absent.** Base units are exact but unreadable; the footer's
  pointer to the fetch is the only mitigation.
- **The token table in §7 is read from code, not run.** S0(a) is where it meets a real exploit.

## 13. Open questions for Chief

1. Default binary, as §9 recommends, or an off-by-default `diff` feature?
2. Is S0(a)'s exploit the right one? Euler is well documented but large; a smaller exploit with a
   single token would make a cleaner first gate and a weaker demonstration.
3. Should `--tx` be allowed its one RPC call, or must the caller always supply `--block`?
4. The row cap: a fixed number, or derived from the cursor's budget as RFC-0048 does for scans?
5. S4's `story` templates: worth a field in `semantic.toml`, or should authors who want sentences
   write a view and let the fallback name it?
