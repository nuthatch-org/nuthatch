# RFC-0063: Address history - an Etherscan-compatible local source for rotki

- Status: **Accepted by Chief, 2026-10-09.** Locked by Jenny and Codex in two rounds the same day.
- Author: Pete (cargopete)
- Date: 2026-10-09
- Reopens: #272 (watched positional filters), #277 (traces, from a remote RPC), #308 (bounded blocks
  and transactions). #276 (a colocated node) stays closed unless measured coverage or cost demands it.
- Nature: new binary capability. Per RFC-0044 §8 and CLAUDE.md it is decided by Chief and recorded,
  or it does not happen. Chief: "this is our first potential serious user... if we have to extend
  nuthatch we will."

## 1. The user and what he asked for

rotki is a local-first portfolio and accounting app. On 2026-10-09 its founder answered
rotki/rotki#13335: "Absolutely would add a local indexer as a source." What rotki gets from Etherscan
and Blockscout today is the set of transactions where a user's address appears as a normal
transaction, a token transfer or an internal transaction; timestamp to block number; and on mainnet,
staking withdrawals, MEV and block production.

The free versions of those sources closed in 2025-2026: Etherscan's free tier dropped chains on
2025-11-22, Blockscout keyed every call from 2026-07-01, and Routescan serves Ethereum only. A user
without a paid key now gets empty history on Base, Optimism and Gnosis.

## 2. How rotki consumes an indexer (read at rotki 2783d11)

One abstraction, `EtherscanLikeApi` (`rotkehlchen/externalapis/etherscan_like.py`), with indexers
tried in a user-set order (`chain/evm/node_inquirer.py`, `_get_indexers_in_order`). **A failure falls
through to the next indexer; an empty success does not.** So an indexer that cannot answer must fail,
never return `[]`.

| Action | What rotki reads |
|---|---|
| `account/txlist` | `hash, blockNumber, timeStamp, from, to, value, input, gas, gasPrice, gasUsed, nonce`, optional type, authorisations, and on OP-stack chains the L1 fee as `L1FeesPaid`, a decimal (a missing one is an unknown fee, not zero). Saved as full records. |
| `account/txlistinternal` | parent `hash, blockNumber, timeStamp, from, to, value, traceId, gas, gasUsed, callType`; delegatecalls skipped; also queried by `txhash`. `traceId` is parsed as an integer. |
| `account/tokentx` | `hash, timeStamp, blockNumber` only. Hashes are hydrated over RPC. |
| `block/getblocknobytime` | `timestamp, closest=before|after`, a decimal block number. |
| `account/txsBeaconWithdrawal` | `blockNumber, withdrawalIndex, validatorIndex, timestamp, amount` (gwei). |
| `account/getminedblocks` | `blockNumber, timeStamp, blockReward` (wei). |

Account lists page at 1,000 (Etherscan) or 10,000 (Blockscout) rows and restart at the last block,
inclusively. **A single block holding a full page does not progress.** That trap is rotki's
(`etherscan_like.py:562`) and the adapter in §6 overrides it.

## 3. Architecture

**A local rotki-mode nest per user.** Config names the watched addresses, the chains, the history
bounds and the RPCs. Per-user history never leaves the user's machine, except as the address filters
the upstream RPC necessarily sees. One cursor per chain, as always.

**Hosted global nests for what is address-free.** Timestamp to block number reveals nothing about a
user, so one headers nest per chain on our boxes answers it for every rotki user, and appears in the
Hosted section of nuthatch-indexer.com/nests. Per-address history is never hosted: a hosted service
would have to index every transfer and transaction on a chain, and would see every user's wallet.

**Polling.** A rotki-mode nest polls every **5 minutes** by default (Chief, 2026-10-09), so an idle
nest costs a handful of calls an hour. The poll backfills to the finalized head; rotki's sync ends
15 seconds behind now, so the blocks above it are covered when a request asks for them (§12).

## 4. Discovery

- **Token transfers.** `eth_getLogs` with no emitter and a positional address filter: ERC-20 and
  ERC-721 `Transfer` with the address in topic 1, then topic 2; ERC-1155 `TransferSingle` and
  `TransferBatch` with the address in topic 2, then topic 3. Union, deduplicated by (tx, log index).
  Today's `LogFilter` carries emitter and topic0 only, and the union fetch can drop the emitter
  restriction; positional predicates must survive every fetch path.
- **Normal and internal transactions.** `trace_filter` with `fromAddress`, then separately
  `toAddress` (a combined filter can mean intersection), unioned. Root frames are the normal
  transactions; non-root frames are the internal ones, listed by Etherscan's rules as measured
  against its answers in slice 4: value-carrying calls, creations at any value (typed `create` or
  `create2`), and selfdestructs (`self-destruct`, from the dying contract to its beneficiary);
  delegatecalls and zero-value calls are not listed.
  **Reverted frames are listed, not excluded.** This section first said reverted effects would be
  excluded; Etherscan does otherwise, so we match it. A frame that reverted is listed with
  `isError` "1" and `errCode` "execution reverted"; a frame that succeeded inside a reverted
  ancestor is listed with `isError` "1" and an empty `errCode`. rotki does not read `isError` on
  internal rows, so it books a reverted internal transfer as a real one, from Etherscan or from us
  alike.
  `traceId` is Etherscan's: "0" followed by "_1" for each level of depth in the address form
  (`[0, 2]` is "0_1_1"), which is not unique within a transaction, and absent in the `txhash` form.
  rotki parses it with Python's `int()`, which accepts the underscores. Our own stable key is
  `nuthatchTraceIndex`, the frame's pre-order position in the whole trace, the same whether the
  frame is reached by address or by `txhash`.
  An empty answer from either direction is checked on its own, never excused by rows from the
  other (#2013).
- **Nonce search is a cross-check, not a source.** `eth_getTransactionCount` is monotone, so a binary
  search finds every block where an EOA's nonce moved. It checks outgoing completeness cheaply. It
  is not sufficient alone: EIP-7702 authorisations move the nonce without a sent transaction, and
  ERC-4337 operations are sent by a bundler. Balance search is unsound and is not used.
- **Where `trace_filter` is unavailable**, completeness needs every block traced (`trace_block`),
  shared across watched addresses. That is the fallback, priced per chain in §7.
- **Hydration.** Every discovered hash is fetched with its receipt and block timestamp.

Coverage is tracked per (chain, address, action, block interval). Adding an address backfills it.
A reorg invalidates the affected tail. Unsupported, incomplete and genuinely empty are three
different outcomes.

## 5. The HTTP surface

`GET /api` on the nest, Etherscan-shaped: `chainid, module, action`, and for account lists
`address, startblock, endblock, sort, page, offset`, inclusive bounds, ascending by default, 1,000
rows a page. `txlistinternal` also takes `txhash`; off a loopback bind only a transaction the nest
holds for a watched account is traced for it. Actions: the six in §2, plus `account/balance`
(**not built**: it answers `NUTHATCH_UNSUPPORTED`), the `proxy` RPC passthroughs rotki uses
(`eth_blockNumber`, `eth_getBlockByNumber`,
`eth_getTransactionByHash`, `eth_getTransactionReceipt`, `eth_getCode`, `eth_call`) and
`logs/getLogs`. `contract/getabi`, `contract/getcontractcreation` and `block/getblockreward` answer
unsupported until built.

Success is `{"status":"1","message":"OK","result":...}`; a covered empty list is `[]`. Anything the
nest cannot answer completely is `{"status":"0","message":"NOTOK","result":"NUTHATCH_UNSUPPORTED: ..."}`
or `NUTHATCH_INCOMPLETE: ...`, which the rotki adapter turns into an error, so rotki falls back.
Quantities are decimal strings, as Etherscan's are; `to` is empty for a creation; failed and
zero-value transactions are kept.

## 6. The rotki side

A `Nuthatch` indexer in rotki, configurable by URL, registered in the explorer priority list and in
the separate withdrawals path. It pages with fixed bounds and an incrementing `page` over a pinned
snapshot, and fails on a repeated page rather than looping. We write it and offer it as a PR to
rotki once every slice below passes.

## 7. RPC and cost

Probed 2026-10-09 at a busy block on each chain:

| Chain | Source | `trace_filter` | `trace_block` | `debug_traceBlockByNumber` |
|---|---|---|---|---|
| Ethereum | GraphOps | yes | yes | no |
| Base | GraphOps, Alchemy | yes | yes | Alchemy only |
| Gnosis | Alchemy | yes | yes | yes |
| BSC | Alchemy | yes | yes | yes |
| Optimism | Alchemy | yes after Bedrock; **an empty success before it** | as `trace_filter` | yes |
| Polygon | Alchemy | no, "not supported after the Erigon-to-Bor migration" | yes | yes |
| Arbitrum | Alchemy | no | no | yes after Nitro; "missing trie node" before it |
| Scroll | Alchemy | no, worded as "not enabled for this app" | no | yes |

Filtered traces are cheap: two directional queries over a million blocks at 10,000-block windows are
about 200 requests plus pagination and hydration. Polygon, Arbitrum and Scroll have no filter, so
every block in a covered range is traced once and shared across watched addresses: the expensive path.

**Optimism before Bedrock answers `trace_filter` with an empty success** while
`debug_traceBlockByNumber` returns calls for the same block. Trusting it would serve empty history as
complete. That range, and Arbitrum before Nitro, answer `NUTHATCH_UNSUPPORTED` so rotki falls back.
Every figure here is replaced by a measured one before its chain ships.

Measured 2026-10-10 on Alchemy. Gnosis serves `trace_filter` over 1,000 blocks in about a second, though
a 50,000-block window can time out or answer 503, and a timeout now reads as one so the span narrows.
Base states a 5,000-block limit. Optimism states 5,000 and then, after about 15 seconds, 100: one
address over 50,000 Optimism blocks is 1,000 sequential calls and took 28 minutes, so an Optimism
backfill is hours per million blocks and the 5-minute poll keeps up with the tip easily.

Partitions off Ethereum carry headers alone and serve timestamp to block. On the OP stack the
header's `withdrawalsRoot` is the message passer's storage root from Isthmus on and the block lists
no withdrawals, so on chains 10 and 8453 a partition holds no withdrawal list and none is checked.

## 8. MEV, withdrawals and block production

Withdrawals come from block bodies (EIP-4895). Fee-recipient blocks come from headers; rewards from
receipts' priority fees. **MEV comes from the MEV-Boost relays' public data API**
(`/relay/v1/data/bidtraces/proposer_payload_delivered?block_number=`, keyless), asked of the six that
answered on 2026-10-10: Flashbots, ultrasound, bloXroute regulated, Agnostic, Aestus and Titan
(bloXroute max-profit, Eden and Manifold no longer do). It is off unless `[address_history]` sets
`mev_relays = true`, since the relays see the nest's IP and the block numbers it asks about, which
name the validator. The rows come from the chain: every block a watched address was fee recipient
of (slice 5) is a row. A builder's payment, a value transfer to the address from the block's miner or
in the block's last transaction, directly or as an internal transfer from a payout contract (cold
test M1, 2026-10-11), becomes a row only on a relay's word. MEV is a label on a row, never a claim
about the row set: deliveries are matched to the canonical block by hash, a claim naming the address
is preferred, relays that disagree make the row a `conflict` with no value, and no delivery at the
queried relays is `unrecorded`, never zero. A relay saves its delivery record after the block is
published and only logs a failure to (mev-boost-relay `service.go`), so neither finality nor an
empty answer proves there was none: such blocks, and builder payments without a row, are asked again
daily for 30 days, and a delivery found then upgrades or inserts the row and moves the generation.
Relay answers are cached only at or below the finalized head and dropped above a reorg; a relay that
cannot be asked fails the window rather than reading as no delivery. The rows
are served as `module=account&action=nuthatchProducedBlocks`: block, timestamp, fee recipient and its
block reward, the relays' recipient, value, builder and proposer keys, and the builder's payment
transaction and value. rotki's adapter reads them ahead of beaconcha.in when the nest is in
Ethereum's indexer order, with beaconcha.in still asked after it for what the nest cannot
attribute, and turns them into the same block and MEV events beaconcha.in's data does; a builder's
transfer to a block's own fee recipient counts as MEV, the block reward never twice.

**Block partitions.** Reading withdrawals and fee recipients from bodies costs one call per block
per nest: about 269,000 calls for the slice-5 gate's 265,000 blocks, and roughly 9 million for the
post-Shanghai chain. Since that scan is the same for every address, it is done once, by
`nuthatch partitions` on our boxes, and published; a nest that opts in downloads it.

- **What is published.** For each span of 10,000 blocks, one Parquet file holding every block's
  full execution header and complete, ordered withdrawal list, each RLP-encoded exactly as the
  chain hashes it, plus a manifest listing the files. Only finalized spans are built. The file is
  deterministic: the same range builds to the same bytes, so two builds can be compared byte for
  byte, and an interrupted build resumes from its manifest.
- **Who uses it.** Only a nest whose `[address_history.mirror]` names the mirror's URL, the chain,
  the blocks to take from it and a cache budget in MB. Nothing is fetched before that, not even a
  catalogue. The default is still the direct path of slice 5, which needs no mirror, and a
  filesystem path in `url` imports partitions from local files.
- **What the nest checks.** Every header is hashed from its own bytes; numbers run contiguously
  across the whole span; each header's parent hash is the previous block's hash; and the last
  block's hash must equal the one the nest's own RPC returns at that height, at or below the
  finality the nest already serves through. That anchors the whole span to the user's RPC rather
  than to us. Each block's withdrawals root is then recomputed from its full list, as a Merkle
  Patricia trie keyed by position within the block (EIP-4895), so a missing, altered, reordered or
  invented withdrawal fails. Only then does anything become coverage. A missing partition, a short
  one, or one that fails any check records nothing, and its blocks answer `NUTHATCH_INCOMPLETE`:
  the body scan does not stand in for it. The manifest's claims never become coverage, because the
  nest never reads the manifest.
- **Rewards.** `blockReward` needs every fee, which headers cannot give. For the few blocks a
  watched address produced, the nest fetches the block's transactions, receipts and ommers from its
  own RPC by the verified block hash, recomputes the transactions root, receipts root and ommers
  hash against the header, and only then sums each transaction's gas used (from cumulative gas)
  times its effective tip, plus the fork's issuance and the ommer inclusion reward before the Merge.
- **What leaves the machine.** The mirror sees the nest's IP and the block ranges it downloads,
  never an address: every partition is fetched whole, whatever it holds.
- **What it costs.** Measured: 461 bytes a block at 14 million, 690 at 20 million (Cancun headers
  and sixteen withdrawals), zstd-compressed. By those figures the whole chain would be roughly
  12 to 16 GB, a sensible optional archive and not a compulsory download, which is why
  partitions are fetched by range into a bounded, disposable cache: a nest keeps what it extracted
  for its addresses and deletes the files, and an address added later may download a partition
  again.
- **Where it runs.** The producer is ours and is listed on the website under Hosted. Wallet queries
  are never routed to it: each nest answers its own.

## 9. Slices and their gates

Every gate is parity against Etherscan for a pinned wallet and range, exact hash sets and the fields
rotki reads, run on both boxes. The reference answers were pinned on 2026-10-09 with an Etherscan V2
free key, whose answers cap at 1,000 rows, so the pinning splits ranges at that cap.

1. **Coverage, `/api` and pagination.** 1,001 records in one block page through with exact
   identities; an uncovered range answers `NUTHATCH_INCOMPLETE`; a reorg invalidates the tail.
2. **Block partitions, headers and `getblocknobytime`** (§8). Publication is deterministic: one
   range built twice gives byte-identical files. Verification refuses, as incomplete, a partition
   with a withdrawal dropped, a field altered, withdrawals reordered within a block, the range
   truncated, a partition omitted, or a parent link broken; an interrupted download is recovered
   by fetching the partition again. Bytes per block and laptop ingest time are measured and
   reported. `getblocknobytime` answers only from verified headers, and sampled before and after
   answers equal Etherscan's, pre-London blocks among them. Slice 5's parity gates pass again
   through downloaded partitions, with no per-user body scan for withdrawals and bodies hydrated
   only for blocks a watched address produced; `getminedblocks` honours rotki's `startblock` and
   `endblock`, which rotki does send (`eth2.py:822`), so no genesis coverage is needed; bounded
   and unbounded requests, partition boundaries and pagination are covered. The producer's RSS is
   measured while it builds.
3. **Token transfers and normal transactions.** vitalik.eth (`0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045`),
   Ethereum blocks 18,000,000 to 20,000,000. The pinned files are multisets: the 1,000-row cap
   overlaps at each split, so `txlist` has 2,929 rows for 2,318 transactions, and the token files
   are larger than their hash sets for the same reason and because one transaction can carry
   several logs. The gate compares hash sets, not those row counts: `txlist` (2,318) and `tokentx`
   (3,334) equal Etherscan's, with `txlist` field parity; `tokennfttx` (106) and `token1155tx` (53)
   likewise. `docs/releases/v5.1.0.md` records the run. A non-empty wallet and range is pinned per
   further chain.
4. **Internal transactions.** The same wallet and range (`txlistinternal`, 15 rows), plus pinned
   fixtures for an internal-only receipt, a failed ancestor, a creation, a selfdestruct and an EIP-7702
   authorisation. Field parity and identical `traceId`s by address and by `txhash`.
5. **Withdrawals and block production.** `0x7a25bd5f286fb722e7578c62e86a675ff0a00b15`, Ethereum
   17,034,870 to 17,300,000: 3,131 withdrawals (the pinned file holds 4,333 rows, the overlap of its
   split pinning), exact indices and every field, read from block bodies. Pinned proposer fixtures
   for `getminedblocks`, before and after the Merge and with an uncle, and reward parity to the wei.
   (Lido's withdrawal vault was the first choice, and the address this RFC first gave for it was
   mistyped: `0xB9D7934878B5FB9610B3Fe8A5e441e8faF7E293f` fails its own EIP-55 checksum, and Etherscan
   rightly has nothing for it. The vault is `0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f`, and Etherscan
   does list its withdrawals, withdrawal index 1,041,981 in block 17,100,000 among them.) Two things
   stay open after this slice. `getminedblocks` takes no range, so it answers `NUTHATCH_INCOMPLETE`
   unless the address is covered from genesis; and withdrawals cost one block body per block, about
   269,000 calls for this gate's range. Slice 2 addresses both with hosted global partitions.
6. **The rotki adapter.** rotki's own test suite passes with nuthatch first in the order, and a
   fresh rotki profile for a pinned wallet produces the same history events as with Etherscan.
7. **MEV.** Pinned relay and non-relay blocks for a known proposer: amounts equal beaconcha.in's for
   the fields rotki reads, and rotki's final events count no income twice.

Chains go Ethereum first (every slice), then Base, then the rest in the order rotki's users need.

## 10. What this does not do

No hosted per-address history. No completeness claimed for a chain or era until its gates pass;
until then it answers `NUTHATCH_UNSUPPORTED` and rotki falls back.

## 11. Lightweight by requirement

A rotki-mode nest runs on a laptop beside a desktop app (Chief, 2026-10-09). Each item is a gate.
Figures marked (target) are confirmed by the first measurement, then frozen as budgets.

1. **Memory.** Idle RSS (target) under 100 MB per chain, under 300 MB while backfilling. No DBSP,
   Burrmill or Parquet sealing in this mode unless a measurement shows a need.
2. **Disk.** Only rows touching watched addresses, plus coverage, under compact binary keys; with a
   mirror, also one timestamp per verified block for `getblocknobytime`, and a partition cache
   bounded by the `cache_mb` the operator chose.
3. **RPC.** A 5-minute poll; the widest windows the provider accepts; each hash hydrated once; the
   idle call rate reported on `/metrics`.
4. **Speed.** A 1,000-row `/api` page in (target) under 10 ms from an index keyed (address, block,
   position), never a scan. Backfill resumes from coverage after a crash.
5. **Addresses change at runtime.** Adding a watched address starts its backfill without a restart.
6. **Start.** Under 1 s from a covered store to serving; nothing to run but the binary.
7. **Keys.** A trace-capable RPC is the only required credential, and the nest says which chain
   needs one and why.
8. **Private by default.** No head count prompt; localhost only; no outbound call but the RPCs and,
   once `[address_history.mirror]` names one, the partition mirror, which sees the nest's IP and
   the block ranges it downloads, never an address; and once `mev_relays = true`, the MEV-Boost
   relays, which see the IP and the block numbers of the address's produced and relay-paid blocks
   (§8). Both are off until named.
9. **Offline.** Covered history serves with the RPC down; gaps answer `NUTHATCH_INCOMPLETE`.

## 12. The tip, reorgs and rotki's sync end (2026-10-11)

Two cold tests of 5.1.1 (Codex, and a Claude agent with no build history) found that the nest and
rotki's sync did not meet. rotki ends a sync at now minus 15 seconds (`CHAIN_TOP_LAG`, set by rotki's
maintainers), resolves that time with `getblocknobytime`, and asks every account action up to the
block it got. 5.1.1 answered `getblocknobytime` only from verified partitions, so a nest without them
failed every nest-only sync; and it shortened an `endblock` above its head to the head and answered
`status 1`, so rotki recorded history it never received (17 of Etherscan's 65 rows, cold test B1).
The plan below went through six rounds of review against the code before both reviewers agreed.

- **Exact or refused.** An explicit `endblock` is answered whole or refused, never shortened.
  `getblocknobytime` answers from verified headers, else by searching the nest's own RPC, with the
  answer proven against its neighbour by parent hash, and above the finalized head linked by parent
  hashes to the chain head the search began from; a timestamp newer than the chain head, or a
  chain whose blocks can share a second, is refused.
- **The tip on demand.** The cursor backfills to the finalized head. A request above what is covered
  and at or below the chain head is handed to the cursor, which covers that address and the actions
  the request needs (produced blocks need mined blocks and transactions first), up to the block
  asked for, holes included, and answers within a deadline (8 s by default, under rotki's 30 s read timeout).
  The cursor stays the only writer: requests wait on a bounded queue and are served between its
  windows, so none waits behind another address's backfill. A read reaching above the finalized head
  goes through the cursor as well, which checks for a reorg before it answers. Produced blocks are
  the exception: they are served through the finalized head only, because a relay's answer about a
  block is used only once the block is final, and their coverage commits in one transaction with the
  re-ask entries for blocks without evidence.
- **Reorgs in the mutable store.** Anything read above the finalized head is read against the hash
  of its window's last block, checked before and after the fetch and tracked with the record. Each
  poll, and each request above the finalized head, compares the newest tracked hash with the RPC's;
  on a mismatch it walks down to the newest that still matches and takes back everything above it:
  rows, coverage, cached transactions, timestamps, traces and relay answers. A fetch is also checked
  against the newest tracked hash under it, so a window read on a fork cannot hide the stale block
  beneath its own matching anchor; every block hash taken from the trace endpoint must be the main
  endpoint's; the trace endpoint must report the main endpoint's hash at the window's last block;
  every comparison is made twice; and a fetch that fails any of it keeps nothing. A fetch that began before a reorg keeps nothing above the finalized head. Tracked
  hashes are persisted, so a restart finds a reorg it slept through, even one the chain has since
  finalized past: what was tracked was mutable when it was read. Only a block under the finalized
  head the store committed is a conflict, which stops the cursor. A property test over random reorg
  depths gates it.
- **The bar.** rotki's ordinary Etherscan sync can import unfinalized history and does not
  automatically reconcile already imported history after a reorg. The nest integration retains this
  shared limitation for this handover; the nest itself converges.
- **Also from the cold tests.** `logs/getLogs` is asked in spans the provider accepts, at most 64
  calls a request, and answers incomplete beyond that (M3); a failing span retry logs at warn and a
  failed reconcile counts as a failed poll (silent stall). On M5 (a mirrored nest idling at 240 MB
  RSS): the store's page cache is now 32 MiB rather than redb's 1 GiB default, but on macOS that was
  not what held the memory. After a 26-partition ingest the physical footprint idles at 68 MB while
  RSS reads 143 MB, the difference being freed allocator regions macOS keeps resident until it needs
  them; a restart on the same store idles at 32 MB.

**Second round (2026-10-11).** Two more cold tests of the 5.2.0 candidate said not ready, and these
followed:

- **Memory per request (B1).** One `getLogs` over 400,000 blocks of USDC transfers took the physical
  footprint to 1.2 GB: spans were as wide as the provider allowed and every log was held before the
  page was cut. Spans now start at 16 blocks and double only while the answer is sparse, an answer
  past 4 MiB is refused as it streams in and its span narrowed, the read stops once it holds the
  page asked for (`page` and `offset` are honoured), and two run at a time, a third answering
  incomplete.
- **Finality (M1).** A reorg reaching under the finalized head went unseen, because only hashes above
  it were tracked. The store now keeps the hashes of the last 32 finalized heads it committed, and
  every poll, the first after a restart included, compares the newest and one older one in turn with
  the RPC. A mismatch is a reorg under finality or an RPC on another chain under the same id; either
  way the cursor stops, `/ready` gives the reason and every answer is refused, one in flight
  included; the conflict is kept in the store, so only removing the store clears it. Every fetch is
  held against the newest finalized hash too, so a switch after a request's own check is caught. A new finalized head's hash is read on both sides of the checks, so a fork switch during
  the poll cannot become the record, and is committed with the head in one transaction. The reorg property
  test now also runs through the cursor's own window against a node whose traces follow the chain,
  with reorgs that cross finality, and fails if the run covered nothing.
- **Produced blocks first (M2).** Each pass covers produced blocks and the relay re-asks before the
  backfill and the body scan, so a long backlog cannot starve them.
- **One body scan for every address (M3).** A block's miner, hash, timestamp and withdrawals are the
  same whoever asks, so the scan keeps that summary for the newest 100,000 blocks, reorgs taking it
  back with everything else. An address watched later reads the summaries and re-reads only the
  bodies of blocks it mined.
- **Smaller.** `paymentTx` and `paymentValue` report every leg of a split payment; unwatching an
  address no longer discards other addresses' windows in flight; `/ready` shows a failing RPC at
  once; `rpc_requests_total` is current; a refusal always names its reason; `addresses = []` is
  valid; a non-loopback bind warns.

## 13. Chains where `trace_filter` is not the source (2026-10-15)

§7's filtered traces do not hold off Ethereum. reth caps `trace_filter` at 100 blocks and replays each
block it covers: Base took 47 s per 100 blocks on GraphOps (2026-10-14), so one account's history there
is about a million calls; Polygon and Arbitrum do not serve it at all. On chains 10, 56, 100, 137, 8453
and 42161 the nest therefore finds an account's normal and internal transactions by **the balance
residual**:

- **Outgoing** transactions are the blocks where the account's nonce rises, found by bisection.
- **Incoming value**: an externally owned account's balance falls only through what it sends, so over
  any range `balance(end) - balance(before) + own spending - own known income` is exactly what others
  sent it, and zero proves nothing came. A positive residual is bisected to single blocks, and only
  those are traced, with `debug_traceBlockByNumber`.
- **The books must balance.** Each traced block's credits, from call frames and from what the chain
  mints outside them (the OP stack's deposit `mint`, Gnosis's bridge `AffirmationCompleted`), must
  equal the balance's rise to the wei, or the window is not recorded.
- **Accounts it cannot prove** are a contract or an account with EIP-7702 delegated code, whose balance
  another account's code can spend. They are found at the first block with code, or the block whose
  nonce rose for an authorisation, and every account action answers `NUTHATCH_UNSUPPORTED` from there,
  so rotki asks another source. A reorg below that block removes the mark.
- **What it does not list**: an incoming transaction that moves no value, a zero-value call or a
  failed transfer to the account. Etherscan lists them; they change nothing in rotki's books.
- **A contract that comes and goes.** `CREATE` cannot target an address with a nonce or code
  (EIP-684), so an account that ever sent cannot become one, and a contract present at a block
  boundary has a nonce, which the bisection sees rise with no transaction behind it. What remains is a
  never-used address a contract is created at and destroyed again inside one window. When its code
  spent more than came in, the balance falls with no transaction and the account is unprovable from
  that range. When it spent exactly what came in, the residual is zero and both are missed: a known
  limit, on an address no key has ever sent from.

The fee rule, `gasUsed × effectiveGasPrice + l1Fee + operator fee`, was checked on senders a block's
trace credits nothing, sampled across each chain's history: Base 211, Optimism 166, Polygon 98,
Arbitrum since Nitro 64, BSC 106, Gnosis 56, none off by a wei after Gnosis's merge. Credits made
outside an ordinary transaction were checked on the receiving side: Polygon state-sync deposits (8 of
8) and Arbitrum deposits from L1 (5 of 5) are in the trace and balance; Gnosis's bridge mints have no
frame at all and equalled their event's value in 4 of 4, which is why that event is read.

Gated through the binary, 2026-10-15:

| Chain | Account | Reference | Outgoing | Incoming value | Fields |
|---|---|---|---|---|---|
| Optimism | small EOA | Blockscout | 5 / 5 | 4 / 4 | 50, identical |
| Optimism | busy EOA, 1,411 sent | Blockscout | 1,411 / 1,411 | 20 / 20 | 14,110, identical |
| Optimism | 7702-delegated | Blockscout, to delegation | 231 / 231 | 3 / 3 | 2,310, identical; unsupported from block 155,556,863 |
| Base | busy EOA, 905 sent | the S0 spike (same method) | 905 / 905 | 11 / 11 | |
| Arbitrum | EOA, 594 sent | Blockscout | 594 / 594 | 10 / 10 | 5,940, identical; one zero-value incoming row, the stated gap |
| Gnosis | EOA, 238 sent | Blockscout | 238 / 238 | 2 / 2 | 2,380, identical |
| Gnosis | small EOA | Blockscout | 16 / 16 | 1 / 1 | 160, identical |
| Gnosis | a bridge-mint recipient | Blockscout | 3 / 3 | 0 / 0 | 30, identical; the mint's window balanced only with its event read |
| BNB Chain | EOA, 200,000 blocks | none free; its nonce | 165, nonce moved 165 | 48, from 48 traced blocks | books balanced |
| Polygon | EOA, 200,000 blocks | none free; its nonce | 13, nonce moved 13 | none in range | |

Three rows have no independent reference, because Base, Polygon and BNB Chain need a paid plan at
Blockscout and Etherscan or are not served there. Base was checked against the S0 spike, the same
method implemented a second time; BNB Chain and Polygon are a smoke run whose only checks are the
nonce and the books. Their proof is Optimism's gate on the same method and the books, which balanced on
every block traced. A backfill window on these chains has a 16,384-block floor, because its cost is
round trips per bisection level and per transaction, not per block; the Base account above took
1 h 52 min and about 18,200 calls, 8,047 of them GraphOps `eth_getLogs`.

## 14. One endpoint for several chains (2026-10-15)

rotki takes one nest address, sends `chainid` with every call, and drops an indexer for a chain whose
answer says that `chainid` is not served. So `nuthatch dev --dir` on a directory of address-history
nests, one subdirectory per chain, serves them all at one `/api`, dispatched on `chainid`. Each chain
keeps its own cursor, store and RPCs, so the single-cursor law holds per chain and one chain's stall or
reorg touches no other. Watching reaches every chain; readiness and metrics are per chain.

Checked with Gnosis and Base in one directory: every Gnosis answer through the shared endpoint was the
solo nest's, byte for byte apart from the per-store `generation`; Base matched the S0 spike; an unknown
`chainid` was refused in rotki's wording.

rotki's half is separate. Its `sync_watched` registers accounts with a nest on Ethereum only, so until it
learns `/ready`'s `chain_ids` a non-Ethereum nest lists its accounts in its own `addresses`.
