# RFC-0071: The diff - what happened between block X and block Y, as a story

- Status: **Accepted (2026-10-21, Chief).** One of five "fun" RFCs Chief asked for on
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

## 14. S0, measured 2026-10-21

**Verdict: continue, with changes.** S0(a) passes: every post-mortem leg involving an indexed token is
in the nest with the chain's exact amount, and every miss is one §7 predicts. S0(b) fails: the fixed
catalogue covers at least half the week's rows on one nest of five, not three. By §11's own rule S4
moves ahead of S1, and these numbers go back to Chief. S0(a) also found one defect that §7 does not
predict, below, which S1 must fix before it prints a net-flow line.

Everything ran on nuthatch 5.8.2 built from `origin/main` at `cdd224e5`, through `nuthatch sql`, so
the rows are the ones S1 will read. The harness is `tools/rfc-0071-s0/`: `euler-nest/` (the nest),
`euler-flow.sh` and `coverage.sh`.

### S0(a): the Euler Finance exploit, 2023-03-13

**Nest.** Eighteen contracts: DAI, USDC, WBTC, WETH, stETH and wstETH, and the Euler eToken and dToken
of each of the six markets attacked (eDAI, dDAI, eWETH, dWETH, eWBTC, dWBTC, ewstETH, dwstETH, eUSDC,
dUSDC, estETH, dstETH). Hand-written event ABIs: ERC-20 `Transfer` and `Approval`, WETH9's `Deposit`
and `Withdrawal`, stETH's `TransferShares` and `Submitted`. Not indexed: Euler's own contract, Aave's
pool and aTokens, the Balancer vault, the Curve stETH pool and the Uniswap pool. Backfilled with
`nuthatch bench backfill --keep` over blocks 16,817,996 to 16,818,085, the six attack transactions'
blocks and nothing else: 6,910 rows, 5 `eth_getLogs` calls, 1.7 s.

**Deviation.** No history before 16,817,996, so S0 says nothing about §4's new-holders section, which
needs each token's whole history. That is S1's measurement.

**Sources.** CertiK, [Euler Finance Incident Analysis](https://www.certik.com/resources/blog/4iSrYY6HoaYxk1aKyjFb5v-euler-finance-incident-analysis),
which walks the DAI transaction step by step and gives each market's take; Beosin,
[Analysis of the Euler Finance's $197M exploit](https://beosin.com/resources/analysis-of-the-euler-finance%E2%80%99s-197m-exploit),
which walks the same transaction with its own figures and lists all six hashes; BlockSec,
[the root cause of Euler's $200M](https://blocksec.com/blog/defi-exploit-analysis-root-cause-euler-s-200m),
for the DAI flow in round numbers; EigenPhi,
[$200 million heist unmasked](https://eigenphi.substack.com/p/200-million-heist-unmasked-unraveling),
for the token set of each transaction.

**Check one: the rows against the chain.** The six receipts carry 142 `Transfer` logs. 139 are at
indexed addresses, and the nest holds exactly 139 rows for those transactions, with the same
`(tx, log_index, from, to, value)` for every one, compared as strings. Changing one digit of one value
makes the comparison fail, so it is not passing on two empty sides. The other 3 are Aave treasury
mints of aDAI, aWETH and aUSDC: a token the nest does not index, as §7 says.

**Check two: the post-mortems' legs.** The DAI transaction, `0xc310a0af…b111d` at block 16,817,996,
is the one CertiK and Beosin both narrate:

| Leg, as the post-mortems give it | Post-mortem figure | In the nest (log index) | Exact amount found |
|---|---|---|---|
| Flash loan of DAI from Aave | 30M | 0, aDAI to 0xebc2…bf99 | 30,000,000 DAI |
| Into the borrowing contract 0x583c… | 30M | 2 | 30,000,000 DAI |
| Deposit into Euler | 20M | 5, to 0x2718…25d3 | 20,000,000 DAI |
| eDAI for the deposit | 19,568,124 (Beosin) | 7, minted | 19,568,124.39877498114146706 eDAI |
| `mint()`: eDAI and dDAI | 195,681,243 and 200M (Beosin; CertiK 195.6M) | 11 and 14, minted | 195,681,243.987749811414670603 eDAI, 200,000,000 dDAI |
| `repay()` | 10M DAI, 10M dDAI | 17 and 19 | 10,000,000 DAI to Euler; 10,000,000 dDAI burned |
| `mint()` again | as before | 23 and 25 | as before, exactly |
| `donateToReserves()` | 100M eDAI | 29, to the zero address | 100,000,000 eDAI |
| Liquidation, debt moved | 259,319,058 (CertiK) | 34, 36 and 38 | 254,234,370.990601202559999999 dDAI from violator to liquidator (burned, then minted) and 5,084,687.419812024051199999 dDAI minted; the two sum to CertiK's figure |
| Liquidation, collateral moved | about 310,930,612 eTokens (CertiK) | 42 | 310,930,612.374274603970808264 eDAI to 0xa0b3…f12b |
| Withdrawal | 38.9M | 46 | 38,904,507.348306697267428294 DAI |
| Flash loan repaid | "repaid" | 54 | 30,027,000 DAI to aDAI |
| Profit | 8,877,507 DAI (CertiK), ~8.9M (Beosin) | net flow of 0xebc2…bf99 | 8,877,507.348306697267428294 DAI |

The post-mortems give each of the other five transactions as one figure per market. Each is found as
the attack contract's net flow in that transaction:

| Market, transaction, block | CertiK | Found, exactly |
|---|---|---|
| WETH, `0x47ac35…4088f`, 16,818,024 | 8,080 WETH | 8,080.498391868396965958 WETH (28,994.303891868396965958 in, 20,913.8055 back to Aave) |
| WBTC, `0x71a908…38617`, 16,818,057 | 846.4 WBTC | 846.43597139 WBTC (3,849.13597139 in, 3,002.7 back), then swapped at the Uniswap pool for 11,559.126335382286632783 WETH |
| wstETH, `0x62bd3d…8c4`, 16,818,062 | 73,821 stETH | 66,271.498175683316786271 wstETH net over two Balancer loans of 99,000, unwrapped to 73,821.459345383044466413 stETH and sent to the Curve pool |
| USDC, `0x465a67…1d9`, 16,818,065 | 34,224,863 USDC | 34,224,863.41846 USDC (244,413,863.41846 in, 210,189,000 back) |
| stETH, `0x309783…f311`, 16,818,085 | not given | 3,878.698857548022052968 stETH, sent to the Curve pool |

CertiK's five figures are its five markets. The stETH market's take appears in EigenPhi's token sets
and in no figure in either narrated post-mortem.

**Misses, and whether §7 predicted each.**

- The ETH the Curve swaps paid for both stETH legs. Native ETH: §7 says no, for every event nest.
- Aave's treasury mints of aTokens. A token not indexed: §7 says no.
- CertiK's liability value (248,751,315), the 20% discount, and Euler's DAI balance before the deposit
  (8,904,507). These are contract state, not events: §7's "storage changes" row and §10's "no
  historical balances". The last can be read off the rows (38,904,507.35 withdrawn less the 30M
  deposited), but the diff will not do that arithmetic.

No leg §7 calls visible was missed, so by §11 the forensic half continues.

**What S0(a) found that §7 does not predict.** WETH leaves the transfer graph without a `Transfer`.
The WETH market's 8,080.498391868396965958 WETH was unwrapped in a separate transaction,
`0x68eb306c…4691` at block 16,818,032, and the 11,559.126335382286632783 WETH from the WBTC swap was
unwrapped inside `0x71a908…`. WETH9 records each as `Withdrawal(address,uint256)` and emits no
`Transfer`. The rows are in the nest, but §4's catalogue has no template for them, so §8's graph and
the net-flow section would show the attack contract still holding 19,639.62 WETH it had already turned
into ETH. That is a wrong number, not a missing one, which is the failure §12's first risk names. Two
changes for S1, before any net-flow line is printed:

1. Add WETH9's `Deposit(address indexed,uint256)` and `Withdrawal(address indexed,uint256)` to the
   catalogue as mint and burn, folded into net flow like a transfer from or to the zero address.
2. Say in §4 and on the §7 line that net flow is the sum of the transfers seen, not a balance change:
   a token can change a balance through other events (WETH9) or through none (stETH's rebase). stETH
   shows the small version here: 73,821.459345383044466413 stETH arrived and
   73,821.459345383044466412 left, so net flow claims 1 wei that share arithmetic may not have left.

Three smaller findings for S1. Templates must bind parameters by position and type, not by name:
WETH9 calls its `Transfer` parameters `src`, `dst` and `wad`. Mint and burn read literally: the
donation is "burned 100,000,000 eDAI", and a liquidation's debt transfer is a burn and a mint of dDAI.
That is what the chain recorded, not what a post-mortem calls it; §8 already declines to infer intent,
and the sentence should say "to the zero address" rather than imply supply was destroyed. And a flash
loan is visible only as the transfer from the lender's address (aDAI, the Balancer vault), which reads
as a loan only with a label on that address.

### S0(b): template coverage

`coverage.sh` takes a nest's `/tables` (from `nuthatch serve` on a copy, or the running instance),
classifies every table by topic0 and its parameters' indexed pattern against §4's catalogue, and counts
rows per table with one `nuthatch sql` query each, over the week ending at the newest row the nest
holds. Tables of kind `call` and `block` count as rows, in the fallback. Views are not rows.

| Nest | Copy | Week ending | Rows | Catalogue | Share |
|---|---|---|---|---|---|
| Lodestar allocations (`alloc-nest`, Arbitrum) | ThinkPad release-gate copy, frozen 2026-10-12 | 2026-10-12 | 56,449 | 0 | 0.0% |
| defi-pulse (mainnet) | live on the ThinkPad, nuthatch 5.7.1 | 2026-10-21 | 53,299 | 0 | 0.0% |
| BetSwirl BNB (stopgap) | ThinkPad, stopped 2026-10-07 | 2026-05-11 | 15 | 0 | 0.0% |
| openloom BSC (stopgap) | ThinkPad, stopped 2026-10-08 mid-backfill | 2026-05-19 | 3 | 0 | 0.0% |
| GRT supply (mainnet ERC-20) | ThinkPad | block 26,142,091 (last 50,400 blocks; the nest has no timestamps) | 8,751 | 8,751 | 100.0% |

**One of five at or above half. The gate fails.**

The two stopgap weeks are thin because both nests stopped. Over their whole history the shares are
0.0% of 403,093 rows (BetSwirl) and 4.3% of 659,873 (openloom, all of it ERC-721 `Transfer` from its
card and medal contracts), and Lodestar's whole history is 0.0% of 6,003,316. The verdict does not
depend on which week was taken.

What the rows are: Horizon's allocation, reward, stake and provision events (Lodestar); USDC's `Mint`
and `Burn`, Lido's `Submitted` and withdrawal-queue events and a Chainlink feed (defi-pulse); bets and
rolls (BetSwirl); `Mint` and `TakeReward` from factory children (openloom). None is a standard. The
catalogue covers the nests that index a token's `Transfer`, and most nests people actually built do
not, because a token's balances were not the question they were asking.

Two further observations for S1 and S4:

- **The control-plane section was empty on every nest.** Not one `OwnershipTransferred`, `Paused`,
  `Upgraded` or role event in any table, because a nest holds only the events its author declared
  (GRT supply has `events = ["Transfer"]`). The section §4 ranks first can only report what the nest
  indexes. If it matters, `init` would have to include those events by default, which changes the data
  identity of every new nest: a decision in its own right, not an S1 detail.
- **A small extension does not rescue the gate.** Adding WETH9's `Deposit` and `Withdrawal` and the
  common non-standard supply events (USDC's `Mint` and `Burn`, USDT's `Issue` and `Redeem`) would take
  defi-pulse to 94% of its week, since 50,318 of its 53,299 rows are USDC mints and burns, and would
  move none of the other three: two of five. Lodestar needs S4's authored `story` lines, which is what
  §11 says follows from this gate failing.

### Cost

Alchemy: 6 `eth_getTransactionReceipt` (for check one's reference, not anything the diff would read)
and 5 `eth_getLogs`. Symbols and decimals came from a public RPC. Under 1,000 compute units in all,
well under one cent. Coverage read local copies and one running nest and made no RPC calls.

### What changes

- S4 (author templates) moves ahead of S1, per §11. Its gate already re-measures Lodestar with
  templates written for it.
- S1's catalogue gains WETH9's `Deposit` and `Withdrawal` as mint and burn, and §4 and the §7 line say
  that net flow is transfers seen, not balance change.
- S1 binds template parameters by position and type, never by column name.
- Whether `init` should index control-plane events by default is a question for Chief before S1:
  without it, the first section of every diff measured here is empty.

## 15. Chief's decision on S0, 2026-10-21

Chief agreed to continue in a new order, which supersedes §11's from S1 on:

1. **The flow first.** The forensic `--flow` for a transaction, block or range, in text, DOT and
   Mermaid, with §14's corrections: WETH9's `Deposit` and `Withdrawal` read as mint and burn,
   parameters bound by position and type, transfers to and from the zero address said as such, and
   net flow described as transfers seen, never as a balance. **Gate:** S0's Euler nest re-run gives
   every leg in §14 to the unit, and no longer shows the 19,639.62 WETH the attacker had unwrapped;
   byte-identical goldens on macOS and Linux; RSS on a Lodestar copy inside the budget.
2. **Author templates and a better fallback, together.** `story` lines in `semantic.toml` (outside
   the data identity), and a fallback that groups each event by its parameters rather than printing
   "Signature x N". **Gate:** Lodestar with templates written for it reaches half its rows; every row
   is still counted somewhere.
3. **The range story** (old S1) and the other inputs and surfaces (old S2) on top of both.
4. **The address-history source** (old S5), unchanged.

`init` does not add control-plane events to new nests by default: it would change every new nest's
data identity for a section few read. RFC-0072's S1 waits for step 2, since weather reuses these
templates.

## 16. The flow, built 2026-10-22

§15 step 1 is built as `nuthatch diff --flow` in the default binary (`src/diff.rs`): a transaction
(`--tx`, with `--block` or one `eth_getTransactionReceipt`), a block, or an inclusive range, read
through the same local-or-`--url` backend as `nuthatch sql`, rendered as text, JSON, DOT or Mermaid.
Every format is drawn from the JSON document. `--flow` is required, so the range story can become the
default later without changing what an existing command prints.

**What reads as flow.** An ERC-20 `Transfer` (topic0, two indexed addresses, an unindexed `uint256`),
and `Deposit` and `Withdrawal` emitted by the chain's listed WETH9-style wrapper, as transfers from and
to the zero address (decision 3 below). Parameters are bound
by position, type and indexedness; a column's name is only used to quote it. Every other table,
ERC-721 and ERC-1155 transfers included, is counted in range under "other events", so the per-table
counts sum to the range total. Text says "(zero address)" and never "minted" or "burned". Net flow is
headed "of the transfers seen, per token (not a balance)", and every output ends with the §7 line,
extended to name balance changes no event records.

### Gate

| Gate | Result |
|---|---|
| S0's Euler nest, every §14 leg to the unit | Pass. The DAI transaction's 19 transfers in log order carry each figure of §14's first table exactly, profit 8,877,507.348306697267428294 DAI included; the five other markets' figures (8,080.498391868396965958 WETH, 846.43597139 WBTC, 66,271.498175683316786271 wstETH and 73,821.459345383044466413 stETH, 34,224,863.41846 USDC, 3,878.698857548022052968 stETH) each appear as the legs §14 names |
| No longer shows the 19,639.62 WETH unwrapped | Pass. Over blocks 16,818,024 to 16,818,057 the attack contract `0x036c…5f1c` received and sent 61,448.430227250683598741 WETH: net 0. The two WETH9 withdrawals are one edge to the zero address, 19,639.624727250683598741 WETH |
| Byte-identical goldens | Pass on macOS (aarch64). Eleven goldens in `tests/fixtures/diff/euler/golden/` (seven transactions in text, the DAI one also in JSON, DOT and Mermaid, and the WETH range), asserted by `tests/diff_flow.rs` in the ordinary `it` suite, so CI runs them on Linux. The fixture is S0's nest with the 29 segments a `--seal-direct` backfill sealed (612 KB) and a store holding only the three watermarks `dev` would; the version string is the one substitution. The same range read from a hot (unsealed) copy gives the same lines |
| RSS inside 2 GB, widest range under the cap | Pass. On the GRT supply nest Lodestar reads (Arbitrum, 6,695,742 transfers, a ThinkPad copy), blocks 234,915,449 to 376,570,031 hold 1,999,995 transfer rows: text peaked at 399 MiB in 11.7 s, JSON (119,056 edges, 67 MB) at 463 MiB. Lodestar's allocations nest (release-gate copy, frozen 2026-10-12) has no transfer table; its whole history, 6,003,316 rows, is counted as other events in 1.6 s at 95 MiB |
| Over the cap: refused | Pass. 2,000,172 rows refused in 0.29 s, naming the count, the 2,000,000 cap and a narrower span. Nothing is printed to stdout |
| Outside coverage: refused | Pass. A range below `start_block` or above the indexed head is refused with the covered interval |

**Mutation checks**, each applied to `src/diff.rs`, shown in the diff before the run, restored after:
reading WETH9's `Deposit`/`Withdrawal` as other events (mainnet WETH dropped from the wrapper list)
turns the attacker's WETH net to 19639624727250683598741 and two golden tests red; reading every
emitter's rows in a `Deposit` table as flow fails the shared-alias test (a vault's deposit drawn as
an edge); dropping the "this chain lists a wrapper" condition fails the Arbitrum test; binding the sender by the name `from` fails the
positional test (the goldens stay green, since every Euler token but WETH names it `from`, and WETH
falls back to position); "(burned)" for "(zero address)" fails the wording test and two golden tests;
a cap check of `cap * 2` fails the cap test; trimming fractions to six places fails the scaling test
and three golden tests.

### Decisions the earlier sections did not settle

- **A transaction lists every transfer.** §8's edges sum legs, so the DAI transaction's three eDAI
  mints are one edge. For `--tx` only, the text and JSON also list each transfer in log order, which
  is where §14's legs are read. Blocks and ranges get edges only.
- **WETH9 is a property of a contract, not of a signature.** A `Deposit(address indexed, uint256)`
  or `Withdrawal` row is flow only when the address that emitted it, the row's own `address` column,
  is the listed wrapper for the nest's chain. Every other row of the same table, a factory child or a
  vault sharing the alias, is counted under other events, so a table can appear on both sides with
  counts that still sum to the total. Neither the alias nor "same contract also emits `Transfer`" is
  enough: on Optimism two Curve gauges (crvUSDT-gauge `0x393f…2668`, crvUSDC-gauge `0xf9cb…beef`)
  emit this exact `Deposit` beside a `Transfer` from the zero address in the same transaction, which
  would count each deposit twice. The list, each checked on chain on 2026-10-22 by `symbol()` and
  recent logs (`Deposit` present, and no `Transfer` from the zero address at the contract in the
  sampled deposit's transaction or, for WBNB, in the 20-block window):

  | Chain | Wrapper | `symbol()` | Evidence |
  |---|---|---|---|
  | mainnet (1) | `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2` | WETH | 452 `Deposit` in 40 blocks; tx `0xc3270265…1902` has none |
  | optimism (10) | `0x4200000000000000000000000000000000000006` | WETH | 7 in 40 blocks; tx `0x63f985bc…b766` has none |
  | base (8453) | `0x4200000000000000000000000000000000000006` | WETH | 314 in 40 blocks; tx `0xdeda5343…b557` has none |
  | bsc (56) | `0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c` | WBNB | 52 in 20 blocks, 0 `Transfer` from zero |
  | polygon (137) | `0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270` | WPOL | 5 in 40 blocks; tx `0x792d70f0…64e9` has none |
  | gnosis (100) | `0xe91d153e0b41518a2ce8dd3d7944fa863463a97d` | WXDAI | 1 in 40 blocks; tx `0x21c4fd3c…d7ac` has none |
  | monad (143) | `0x3bd359c1119da7da1d913d1c4d2b7c461115433a` | WMON | 2 in 40 blocks; tx `0xdc55fb13…a401` has none |

  Not listed: Arbitrum One's WETH (`0x82af…bab1`) emitted no `Deposit` in 2,000 blocks and 99
  `Transfer`s from the zero address, so it wraps by `Transfer` and is already flow. Robinhood Chain
  had no `Deposit` from any contract in 40 blocks and no wrapper could be checked. A later slice may
  let an author declare further wrappers in `semantic.toml`; nothing for that is built.
- **The row cap counts transfer rows only**, at the proposed 2,000,000: the measurement above leaves
  it with room. Other tables are counted, not read, and are not capped.
- **`--top` defaults to 25** per token for edges and net flow in text, DOT and Mermaid; JSON keeps all.
- **A partial read is refused.** A degraded table or an unreadable hot tip stops the flow rather than
  drawing it with a gap.
- `GET /` gains `start_block`, so `--url` can refuse a range below it.

### Not verified

- Linux byte-identity rests on CI running `tests/diff_flow.rs`; it was not run on a Linux box by hand.
- `--url` was checked once against `nuthatch serve` on a copy of the Euler nest (same flow as the
  local read); it has no automated test.
- The receipt lookup for `--tx` without `--block` is tested only for its failure message.
- Labels on nodes are read from the nest's `labels/` snapshots; no nest measured here has any.

### For the next slice

Step 2's grouped fallback should reuse this document's per-table counts, which already account for
every row. The flow's "other events" line on Lodestar is 73 tables long; it is the first thing the
grouped fallback should replace.
