# RFC-0081: The engine in use - fast cold opens, one statement over two nests, answers a client can check

- Status: **Accepted, 2026-10-31 (Chief). S0 measured 2026-11-01 (§11): narrow.** Row-group reads are not
  selective; a hybrid open of whole tables passes every gate but the 64 MiB one. S1 accepted on those
  narrowed terms 2026-11-01 (Chief): whole segments of the named tables, 200 MiB from the mirror, cold
  under 300 s. **S1 measured 2026-11-01 (§12).** S2 begun on Chief's word the same day; **S2 measured
  2026-11-01 (§13).** S3 begun on Chief's word the same day; **S3 measured 2026-11-01 (§14)**, against a
  stand-in host, since Lodestar's public route cannot answer the gate. No later slice is begun until
  Chief says.
- Author: Pete (cargopete)
- Date: 2026-10-31
- Depends on: RFC-0077 (the embeddable engine, S0 to S5 and S7 built, shipped as
  `nuthatch-events.wasm` in 5.15.0), RFC-0052 (the mirror), RFC-0068 (a published nest read by hash),
  RFC-0065 S5 (the verified head), RFC-0079 (Arrow results, for S1 only).
- Blocks: nothing. RFC-0080's client half builds on the same host and shares the size budget (§5).
- Nature: changes to the engine crate and its hosts, opt-in and off by default as RFC-0077 §1a requires.
  The binary is unchanged in every slice; S3 relies on `/sql`'s `provenance`, which it already returns.

## 1. Where this comes from

GraphOps, on 2026-10-31, after the blog post on the engine: "nuthatch is pulling away from the
competition in terms of what makes it unique", and "excited to bring it to the platform".

RFC-0077 proved the property. S7 then put it in front of a real page, Lodestar's `/profile`, and every
figure for seven wallets came out equal to the server's at the server's block. That is the evidence
the engine works. The same run is also the best list of what it is like to use, because S7 was the
first time anyone built on the engine rather than on a spike, and the engine's author still had to
work round it in five places. This RFC takes those five, and the one the platform raises, and asks
for each to be fixed in the engine rather than in the next page that embeds it.

The test for every slice: **the next embedder writes less code than S7's page did, and gets an answer
it can trust further.** Nothing here makes the engine the headline. The binary stays the pitch
(RFC-0077 §1a, §11).

## 2. What S7 left, measured

| | What S7 found | Where |
|---|---|---|
| Cold open | 73 s to 1,995 s on arb1's public RPC; 11,566 requests for the wallet in a pool of 74 million shares. Almost all of it is the pools nest, whose data is public and identical for every visitor | RFC-0077 §18, gate table |
| Two nests, one answer | The wallet nest and the pools nest are two instances. Pool rows and thaw requests reach the wallet's SQL as typed literal rows written into the statement by the page, quoted and cast because a bare wide integer "would be read as a float". The stakes query hit `/sql`'s 16,384-byte cap once | §18, `src/lib/verify/sql.ts` in Lodestar |
| The host | Lodestar vendors the module, `host.js` and `opfs.js` by script and writes its own worker, header batching for arb1 and `follow_failed` handling | §18, `scripts/vendor-nuthatch-engine.sh` |
| Completeness | "An RPC that leaves a log out is not caught." A narrowed nest makes this sharper: it asks for few logs, so an omission is a large share of the answer | §18, RFC-0065 §6 |
| Arbitrum's head | Nothing verifies a block. S5's verified head is Helios on mainnet's beacon chain; Arbitrum's finality is an L1 assertion | §18 |
| Reach | Safari, Firefox and a phone never run. The wasmtime host answers no `get` and no `verified_finalized`, so it opens no mirror and seals nothing verified | §17, §16 |
| Size | 5,267,248 bytes brotli against the 5,500,000 gate: 232,752 bytes of headroom, before RFC-0080's `sync` client or anything here | §17 |

And from the platform: a hosted nest's `/sql` answer already carries `provenance.nid` and
`provenance.as_of` (`core/src/answer.rs`, `sql_envelope`). That is everything a client needs to name
the dataset and the block an answer came from. S7 rebuilt that check by hand for one page. Nothing yet
lets any client check any hosted answer that way.

## 3. Proposals

### 3.1 A hybrid open: what is public from the mirror, what is yours from your RPC

S7's slow part is not the wallet's data. It is the delegation pools of the wallet's indexers, which
are the same rows for every visitor and are already sealed in `graph-allocations-nest`'s published
segments. The tab fetched them from arb1 one window and one throttle at a time.

`nh_open` gains, per nest, a `source`: `rpc` (today's), `mirror` (S4's hydrate, today's) or
`mirror+rpc`: sealed history from a published dataset pinned by digest (RFC-0068 §3), then the
narrowed tail from the RPC from that dataset's `sealed_through` to `until`. S4 refused `mirror` with
`filters` (§18); this lifts the refusal for the case where the mirror dataset's data identity matches
the nest's and the filter applies to what was hydrated as a query predicate, not a fetch.

**The privacy rule, which is the reason this is safe to offer to rotki-shaped users.** The module never
puts a value from a private filter into a `get` request. A filter is marked `private` (the default for
any value the host says came from the user) or `public`. A `mirror` or `mirror+rpc` nest may be
narrowed only by public filters; a private one forces `rpc`. So a mirror learns which published nest a
visitor opened, which is what a page load already tells the site, and nothing about the wallet.

The size question this cannot avoid: S4 hydrates a dataset whole, and RFC-0068 §5 puts Lodestar's
published segments at 512 MB, past the 600 MiB ceiling once admitted. S4's pack already reads a segment
by range at query time. Hybrid open reads only the segments of the pool tables, and inside them only
the row groups whose statistics admit the public filter's values, by HTTP range. Whether that is small
is the kill question (S0).

### 3.2 One statement over two nests

The two-instance shape is right: one instance runs one nest, one cursor (RFC-0077 §8), and the pools
nest has its own `until` and its own failure. What is wrong is that the page is the join.

`nh_open` gains `attach`: a list of `{name, tables}` naming another instance's tables, which the host
supplies at query time as Arrow IPC, the answer RFC-0079 adds to `/sql` (`format=arrow`), read through
`nh_request` on the other instance. The module binds each attached table under the attachment's name
(`pools.delegation_pools`) for that statement only, from bytes the host passes; it holds nothing
between statements and never follows the other nest. The attached instance's `provenance.as_of` must
equal this one's, or the statement is refused by name, which is the check S7's page did by re-reading
`/api/portfolio` until both sides agreed.

RFC-0079 is a hard dependency of this part. `/sql`'s JSON rows carry column names and no types, and
typing them again is the float trap S7 hit; Arrow IPC carries the types the other instance bound.

### 3.3 The host package

`nuthatch-events.wasm` ships with its README and nothing else (5.15.0's assets). Every embedder writes
the worker, the step loop, OPFS, the throttle and the reopen. S7's page spent about 450 lines on it (`verify-worker.js`,
`browser.ts`, `run.ts` and the vendor script), and 195 more comparing answers, before doing anything of
its own.

Ship a host beside the module: `nuthatch-engine.js`, one ES module for browsers and Node, built from
`embed/hosts/browser/` and `embed/hosts/node/` with their behaviour unchanged.

```js
const nest = await Engine.open({ module, nest: files, rpc: url, storage: "opfs", until })
await nest.follow()                   // steps until idle, answers requests, rests on wait_ms
const r = await nest.sql("SELECT ...")
nest.close()
```

It owns the worker, storage, retries across a transport failure, a header-batch size per endpoint (arb1's
tens, today Lodestar's code), and the `follow_failed` carry. It does not own RPC URLs, keys or a light
client: those are the page's, passed in. Released as an asset with a sha256, like the module. npm is a
second version line and waits on Chief (§9).

### 3.4 Check a hosted answer

The platform's case. A client holding a hosted nest's `/sql` answer has its `nid`, its `as_of` and the
statement it sent. `nest.verify(answer, statement, filters)` in the host package opens that nest by NID
(the files from the mirror's catalogue, as S4 resolves them), `until` `as_of`, narrowed by the caller's
filters, through hybrid open where it can, runs the same statement and compares column by column as
strings, as S7 does. The result is per column: equal, differs (both values shown), or not computable
here (named, never shown).

Two limits that are the design and not a gap:

- **A narrowed nest answers only what its filters cover.** `verify` cannot tell on its own whether a
  statement reads outside them; a `SUM` over all delegators run on one wallet's rows differs for an
  honest reason. The caller declares the filters, and the result says which ones it ran under. A
  network-wide statement is the shared-view case and is not verifiable in a tab at all; that is what
  running the binary is for (RFC-0077 §1a).
- **Equality with a server is not truth.** It says the server and the visitor's RPC agree. It becomes
  more than that only where §3.5 and §3.6 apply.

Nothing changes in the binary or the platform: `provenance` is already in every `/sql` answer. A hosted
operator who wants to say "check this yourself" links to a page that calls `verify`.

### 3.5 Completeness for a narrowed nest

An RPC that omits a log is caught today by nothing. For a narrowed nest under a verified head there is a
check that costs headers, not receipts: every header's `logsBloom`. For each block in range, test the
bloom for each filter's address and topics. A block whose bloom admits a filter and for which the RPC
returned no log for it is fetched in full (`eth_getBlockReceipts`), the receipts hashed to the header's
`receiptsRoot`, and its logs taken from them. A forged receipt list fails the root; an omitted log is
then found or proven absent. A bloom false positive costs one receipts fetch and changes nothing.

It needs every header in the range, hashed and linked to the verified head, which S5 already does from
the last seal. That is affordable for mainnet and a young contract (S5 hashed 1,894 headers in under a
second) and not for Arbitrum's 48 windows of 10,000,000 blocks. So `complete: true` is opt-in, and the
module adds up the headers it would need before starting and refuses past a ceiling the host sets, by
name and with the figure, rather than running for an hour.

### 3.6 Arbitrum's head

Direction, with its own spike, last in order. An Arbitrum One block is final when an assertion naming
its hash is confirmed in the Rollup contract on L1. With S5's Helios-verified L1 state root, an
`eth_getProof` for the Rollup contract's latest confirmed assertion, checked by Merkle-Patricia proof in
the module, gives a verified L2 block hash, and S5's header linking runs down from it unchanged. These
are hash and Merkle checks, not zk, so the out-of-scope list holds.

The honest cost: confirmation follows the challenge period, so the verified Arbitrum head is about a week
behind the chain. Everything newer is what the RPC said, and the page must keep saying so. Whether a
week-old verified head is worth building for the platform's users is Chief's call (§9).

### 3.7 Reach

The parts of RFC-0077 that wait on a machine rather than a design:

- **Safari and iPhone.** `safaridriver --enable` needs an administrator on this Mac (§17). Once enabled,
  the CI-equivalent run is S5's 13 shapes and S3's kill-and-reopen in Safari, then the hybrid open on a
  phone over its own network, with peak memory read from Web Inspector.
- **Firefox**, by the same script under geckodriver.
- **The wasmtime host** gains `get` (so it opens a mirror and a hybrid nest) and `verified_finalized`
  from Helios's Rust crate (MIT), as a test dependency of `embed/tests/wasmtime-host` only, as wasmtime
  is today.

The address build and rotki stay where RFC-0077 left them: S6 waits for Lefteris to ask.

## 4. Slices and their gates

Each ends runnable, each gate can fail, and an S0 that fails stops the RFC. Every slice also holds the
two gates of §5. Sizes are days of focused work, estimated.

- **S0, the kill question: is a hybrid open small?** A spike in `embed/` and a Node script, no host
  package yet. Open S7's slowest wallet, 0xd7d7…d076, with its pools nest as `mirror+rpc` from
  `graph-allocations-nest`'s published dataset (published to a local mirror first if the public one is
  not at a matching identity, as S4 did), reading only the row groups the indexers' filter admits.
  **Gate**: the same 53 figures equal to `/api/portfolio` at one block; bytes fetched from the mirror
  recorded and under 64 MiB; cold under 300 s against arb1's public RPC, against S7's 1,995 s; every `get`
  URL the host performed logged, and none containing the wallet's address. **It stops the RFC's first
  half** if the pool tables' row groups are not selective (the published segments are ordered by block,
  not by indexer, so this is a real question): then hybrid open is whole-segment reads only, and §3.1
  narrows to datasets small enough to hydrate. About 3 days.
- **S1, hybrid open and `attach` in the engine.** §3.1 and §3.2 built, with `private` and `public`
  filters. **Gate**: Lodestar's S7 SQL with every typed literal removed, the pools read through
  `attach`, 0 figures differing across S7's seven addresses; a statement over an attachment at another
  `as_of` refused by name; a private filter on a `mirror` nest refused by name; a mutation of the
  privacy rule (a private value allowed into a `get`) red in a test that inspects the host's requests.
  S5's 13 shapes and S3's restart unchanged. About 5 days, after RFC-0079's IPC answer.
- **S2, the host package.** §3.3. **Gate**: Lodestar's `/profile` switch on the package with its
  vendored worker and batching deleted, the same 0 differing; a `docs/` example of under 40 lines that
  opens defi-pulse-nest from the public mirror and answers one `/sql` equal to a node; the package's
  Node half runs S5's 32 answers to `22966cee…363febc5`. About 4 days.
- **S3, check a hosted answer.** §3.4 in the package. **Gate**: for S7's seven addresses, the four
  portfolio statements sent to the hosted `graph-allocations-nest`, then `verify`'d by NID with no
  Lodestar code in the path: every column equal or named; a hosted answer with one value altered by a
  test proxy reported as differing, that column and value named; a network-wide statement reported as
  outside the filters, not as a mismatch. About 3 days.
- **S4, completeness.** §3.5. **Gate**: on RFC-0075 S3's recording under S5's verified head, fixture-rpc
  set to drop one log from one `eth_getLogs` answer: the log found from receipts and the segment hash
  `c983470c…479aeae` unchanged; the same with a forged receipts list: sealing stops by name; a range whose
  header count passes the host's ceiling refused before any request, with the figure. Mutation-checked
  on the bloom test and the root check. About 4 days.
- **S5, reach.** §3.7. **Gate**: S5's 13 shapes and S3's kill-and-reopen pass in Safari and Firefox; a
  hybrid open on an iPhone with its peak memory recorded; the wasmtime host opens defi-pulse-nest from
  a local mirror and seals under a Helios-verified head. Safari needs Chief's administrator step. About
  3 days.
- **S6, Arbitrum's verified head.** Only if Chief says it is worth a week-old head (§3.6). Its own S0
  first: one confirmed assertion's L2 hash proven against a Helios-verified L1 state root, in Node.
  **Gate**: S7's slowest delegator sealed under a verified Arbitrum head, a forged proof refused by
  name. About 5 days after its S0.

## 5. Cross-cutting gates

- **The binary is unchanged** in every slice, held as RFC-0077 holds it: its `Cargo.lock`, `cargo tree`,
  every `--help` two levels deep, the files `init` writes, and its whole suite. Nothing here needs a
  change to it; a slice that finds it does stops and says so.
- **Size.** The event build stays under 5,500,000 bytes brotli, with 232,752 bytes of headroom today and
  RFC-0080's `sync` client due to spend some of it. S1's binding code and S4's bloom and receipts check
  are expected to be small; §3.6's trie verifier is the one that might not be. Before any slice crosses
  the gate it stops and puts the figures to Chief: trim (RFC-0075 §17's symbol attribution says where
  the bytes are), split a feature out, or raise the ceiling on purpose. The ceiling is never raised by a
  slice that needs it.

## 6. Against the non-negotiables

1. **Single static binary.** Unchanged in every slice (§5). The engine and its host package are opt-in
   release assets beside it; RFC-0046 §1's deletion test holds.
2. **Footprint.** The module's own 2 GiB and 600 MiB ceilings bind every open, hybrid included; S0
   records bytes fetched and S5 a phone's peak.
3. **No phone-home.** The module still reaches nothing; every request is the host's, to an endpoint the
   page chose. §3.1's privacy rule goes further than the non-negotiable asks: a wallet's values never
   reach a mirror at all.
4. **Determinism.** The same decode, sealing and Burrmill; every gate is equality with the node or the
   server. `verify` compares answers and stores nothing.
5. **Licence.** Helios is MIT and test-only. A Merkle-Patricia verifier for S6 is chosen from the alloy
   crates (MIT OR Apache-2.0) or written; nothing copyleft.

Out of scope: nothing hosted is built (S3 reads answers an operator already serves), no token, no zk or
TEE (S4 and S6 are hash and Merkle checks).

## 7. Risks

1. **The published segments are ordered by block.** If a pool's rows are spread through every row group,
   range reads save nothing, which is why it is S0. A per-dataset sort by an indexed column would fix it
   and would change sealed bytes, which this RFC does not propose.
2. **`verify` will be read as more than it is.** Equality with the visitor's RPC is not proof, and the
   result must say which filters, which head and which RPC it ran under, every time.
3. **Arbitrum's public RPC is the bottleneck in every measurement.** A keyed endpoint would make every
   figure look better; gates stay on the public one, so the numbers are the floor a visitor gets.
4. **A host package is a promise,** like `abi: 1`. It is versioned with the module and pinned by the
   same interface test.

## 8. Non-goals

Several nests behind one cursor in one module (`attach` binds bytes, it follows nothing). Publishing what
an embedded nest seals. A hosted verifier or any service of ours. Reordering sealed segments. The address
build and rotki (RFC-0077 S6 waits). RFC-0080's shapes, which are that RFC's.

## 9. Open questions for Chief

1. **npm.** The host package as a release asset only, or also on npm, which is a second version line?
2. **Arbitrum's head (S6).** A verified head about a week behind the chain: worth building for the
   platform, or stop after S5?
3. **Lodestar.** S1 to S3's gates run on the `/profile` branch (`pete/verify-locally`), which is unpushed
   until you say. They can run locally as S7's did; deploying it is separate.
4. **Safari.** S5 needs `safaridriver --enable` run once by an administrator on the Mac.

## 10. RFC-0044 §8

The host package (§3.3) and `verify` (§3.4) are new supported surface outside the binary, of the kind
RFC-0077 §10 recorded a line for. If accepted, this wants its own line in `CLAUDE.md`.

## 11. S0 measured outcome, 2026-11-01

**Verdict: narrow.** The kill condition is met: the pool tables' row groups are not selective, so a
hybrid open reads whole segments, as §4 says it then must. Read that way, a hybrid open of the tables
the filters name, whole, passes every gate except the 64 MiB one: the same 53 figures at one block, in
109 s from cold against S7's 1,995 s, and no GET naming the wallet. It fetches 179 MiB, every time
and for every wallet. §3.1 narrows to that shape; whether 179 MiB is acceptable for a cold open is
Chief's call, since the 64 MiB figure cannot be met without changing sealed bytes (§7.1, §8).

**The dataset.** graph-allocations-nest's published dataset on the public mirror's Worker is
`d6fd43c8…6e8b`, written by 5.12.0, sealed through block 520,131,075, its seed complete through
520,131,917. A clean copy of the nest at `7795a33` computes that identity; its `main` (`36bd3de`)
computes `545c3113…0006`, which differs only in files the pool tables do not read (the `nuthatch.toml`
and ABIs are byte-identical). Nothing was published, locally or to R2: the pools nest *is*
graph-allocations-nest at `7795a33`, narrowed by Lodestar's `poolFilters`, rather than Lodestar's own
`pools.toml`, whose identity no mirror holds.

**Is a hybrid open small? No.** The fifteen pool tables the filters name (a sixteenth,
`staking__delegation_slashed`, has no rows) are 1,995 segments, 185,382,371 bytes. Measured by
`embed/rowgroups/` from each segment's footer, read by HTTP range:

| | Segments | Row groups | Admitted by min/max | Bytes | Admitted bytes | Segments holding a row of the three indexers |
|---|---|---|---|---|---|---|
| Fifteen pool tables | 1,995 | 1,995 | 1,949 (97.7%) | 185,382,371 | 172,389,148 | 1,891 (94.8%) |

- **One row group per segment.** A segment is about a thousand rows; the writer's row-group size is far
  above that. Row-group pruning is segment pruning.
- **Min/max admits almost all of them.** The indexer column holds random hex, so any segment with a few
  dozen indexers spans nearly the whole range. Three indexers' values fall inside 1,949 of 1,995.
- **No bloom filter on the indexer columns.** `zstd-bloom-v1` blooms `address`, `from`, `to`, `owner`,
  `spender`, `topic0` and hashes; `indexer` and `serviceProvider` are none of them.
- **A perfect index would not save it either.** The last column is the oracle: the segments that hold a
  row of the wallet's three indexers at all, counted in the engine after the run. 1,891 of 1,995. Three
  active indexers appear in almost every block range, because the segments are ordered by block, as §7.1
  feared. Only reordering sealed data by indexer would make reads selective, and this RFC does not
  propose that.

Reading the footers alone took 1,995 range requests and 29 s.

**What was run instead, as §4 prescribes for this outcome: whole segments of the named tables.** A
spike in the plain module (`embed/plain/src/lib.rs`, `embed/src/mirror.rs`, `embed/src/narrow.rs`):
`mirror` and `filters` together, refused until now, hydrate only the tables the filters name, each
segment whole and checked against its content address as S4 does, then follow the filters from the
RPC to `until`. The identity is computed from the nest's files as given; `[[calls]]` is cut from the
`nuthatch.toml` the module parses, as no call is made and no call table hydrated, and the dataset's
views are not bound, since they read the whole network. The driver is `embed/hosts/node/hybrid.mjs`,
which runs Lodestar's own `verify` (`src/lib/verify/run.ts`) with the wallet nest as `rpc` and the
pools nest as `mirror+rpc`, logs every GET with its bytes, and fails on any GET that names the wallet.

Apple M5 Pro, Node 26.11.1, the browser host over files, fresh storage, the public Worker and
`https://arb1.arbitrum.io/rpc`, wallet 0xd7d7…d076:

| Gate | Result |
|---|---|
| The same 53 figures equal to `/api/portfolio` at one block | **pass**: 53 of 53 at block 520,229,965, pools `0x089f…4774`, `0xe9e2…cf59`, `0xf92f…a6d4` |
| Bytes from the mirror recorded, under 64 MiB | **fail**: 188,006,752 bytes (179.3 MiB) in 1,997 GETs |
| Cold under 300 s against arb1's public RPC | **pass**: 109.0 s and 699 RPC requests, against S7's 1,995 s and 11,566 |
| Every GET logged, none containing the wallet's address | **pass**: 1,999 GETs, none naming it outside the two `/api/portfolio` reads that do by design |
| Kill condition: row groups selective | **met**: 1,949 of 1,995 admitted |

| Nest | Source | Time | RPC requests | Mirror | Linear memory |
|---|---|---|---|---|---|
| Wallet | `rpc`, 10 filters | 61.1 s | 643 | none | 18.9 MiB |
| Pools | `mirror+rpc`, 16 filters | 45.4 s | 56, blocks 520,131,918 to 520,229,965 | 1,997 GETs: `manifest.json`, `_seed/seed.json`, 1,995 segments; 188,006,752 bytes | 26.4 MiB |

Process peak RSS 610 MiB. An earlier run gave the same shape (wallet 60.5 s and 642 requests, pools
47.2 s and 56, the same bytes) and stopped in the driver's own oracle query before printing, so its
comparison is not counted. The pool tables have no seed tails: the 98,048 blocks after the mirror's
`complete_through` came from the RPC in 56 requests.

**Two findings that change §3.1 beyond its size.**

- **Row-group reads would have broken the privacy rule.** The indexers are public, but *which* indexers
  a visitor's open selects is derived from the wallet: it is the wallet's set of pools. A fetch pattern
  over row groups chosen by those values tells the mirror that set. Whole-table reads tell it only the
  dataset and the tables, which the nest's filter shape fixes and no value changes. The privacy rule
  needs a second clause: no wallet-derived value in a `get`, **and no `get` chosen by one**.
- **A range read cannot be checked.** A segment's content address covers the whole file, so part of one
  can be trusted only as far as the mirror is. Whole segments keep S4's check, segment by segment.

**What it does not show.** Only Node: no browser, no OPFS, no phone, where 179 MiB through 1,997 GETs
and 610 MiB of process RSS are the questions. One wallet; the bytes are the same for any wallet, since
the tables are whole, but the RPC half grows with the pools. The Worker answers a range with
`Content-Range: bytes NaN-NaN/<size>`; the size is right and a suffix range works, but a reader that
trusts the start would not. It no longer matters to this design.

**The binary.** Unchanged: the change is under `embed/` and `docs/` only, and no file the binary
builds from is touched, so its `Cargo.lock`, `cargo tree` and `--help` are those of `main`.
`embed/rowgroups/` is its own workspace with one dependency, `parquet` 59.3.0 (MIT OR Apache-2.0),
built by nothing in CI.

**If Chief takes the narrowed §3.1, S1 needs:**

1. The 64 MiB gate replaced by a figure Chief sets for a whole-table open: 179 MiB here, cached by
   the Worker and, in a tab, kept in the pack, so a second visit fetches only new segments.
2. `source: "mirror+rpc"` as a key of its own rather than `mirror` beside `filters`, with the tables to
   hydrate named by the filters' events, and `private` and `public` filters as §3.1 has them.
3. A nest with `[[calls]]` accepted when hydrated, its calls never run, in the module's parsing rather
   than by cutting text; and a decision on the dataset's views.
4. The pools nest pinned to the dataset the publisher holds: Lodestar would ship graph-allocations-nest
   at the publisher's commit (or a postcard of it), since its own `pools.toml` names no dataset.
5. The browser measurement this spike did not make: Brave on OPFS and a phone, with peak memory.

## 12. S1 measured, 2026-11-01

**Verdict: the narrowed gate passes.** Chief accepted S1 on S0's terms the same day: whole segments of
the tables the public filters name, 200 MiB from the mirror in place of 64, cold under 300 s.

**What was built**, all in `embed/`, the interface still `abi: 1` (only optional keys, an optional answer
field and a request body key were added, as its versioning rules allow):

- `nh_open`'s **`source`**: `rpc`, `mirror` or `mirror+rpc`; absent, what it was before. `mirror+rpc`
  needs `mirror` and `filters`, and refuses a postcard, which pins a nest that does not follow.
- **`privacy`** on each filter, `private` by default. A private filter on a `mirror` or `mirror+rpc` nest
  is refused by name before anything is fetched. The hydrated tables are computed from the filters'
  events alone (`narrow::tables`, which reads no value), so no `get` names a value or is chosen by one.
- **The filters as a predicate.** §3.1 says the filter "applies to what was hydrated as a query
  predicate, not a fetch"; S0 did not do that, so its pool tables held the network's whole history up to
  the mirror's end and the filters' rows after it. S1 reads each hydrated table through the filters
  (`LOWER(col) IN (...)` on address and hash arguments, `block_number >= from`), so a `mirror+rpc` nest
  answers as an `rpc` nest under the same filters would. An argument whose stored form a predicate cannot
  compare (an indexed integer) is refused by name with `mirror+rpc`.
- **`[[calls]]`** accepted with `mirror+rpc` by parsing `nuthatch.toml` as TOML and dropping the table,
  in place of S0's line cutting; the identity is still computed from the files as given. **The dataset's
  views and entities are not bound** in a `mirror+rpc` nest, since each reads every row; a statement
  naming one of the views is refused by name. `[[calls]]` stays refused with `mirror`, whose call tables
  would stop at the mirror's end while its events carried on.
- **`attach`** (§3.2): `nh_open` declares `{name, tables}`; a `POST /sql` supplies each table as the
  other instance's Arrow IPC answer, base64, with its `x-nuthatch-answer` envelope. Bound under its own
  name for that statement, each column cast back to the type the answer carried (a `HUGEINT` stays a
  `DECIMAL(38,0)`). Refused by name: an answer whose `provenance.as_of` is not this nest's, a truncated
  answer, a bare name for an attached table, a table not supplied, and a name colliding with the nest's.
- **`/sql` answers Arrow** in the module, as RFC-0079 S1 made the node do: `format=arrow` in the query or
  body; `nh_request` answers `body_base64` and `headers["x-nuthatch-answer"]` for it. The browser worker
  and Service Worker pass both through.
- Burrmill moved from `ea8cf40` to `fc2dd16`, the binary's own pin, for the public `result_schema` an
  empty Arrow answer opens with.

Burrmill exposes no way to bind Arrow batches, and this slice changes no other repository, so an
attachment's cells cross into it as text and are cast back. Every type an attachment carries reads back
exactly that way; a timestamp, a list or a binary column is refused by name rather than approximated.

**The pools nest** is graph-allocations-nest at `7795a33`, the commit its publisher sealed from, whose
identity names the public dataset `d6fd43c8…6e8b`; the driver fails a run whose hydrated dataset is not
that one. Lodestar shipping those files in place of its `pools.toml` is S2's page change.

**The gate, `embed/hosts/node/hybrid.mjs`**, run with Lodestar's tsx: per wallet, the server's portfolio
at one block; the wallet nest `rpc` with its private filters and `attach: pools`; the pools found by
S7's own stakes statement (whose two relations are still the empty ones here, holding no value); the
pools nest `mirror+rpc` with S7's pool filters marked public (a wallet in no pool names the zero address,
which selects no row: the tables are read either way); the pools' two answers (`lodestar_indexers`'
figures in their own types, and the wallet's thaw requests) taken as Arrow and attached; then S7's four
statements with both literal relations replaced by `pools.indexers` and `pools.thaws`. The driver refuses
a statement still holding `VALUES` or an empty relation. Apple M5 Pro, Node 26.11.1, files, fresh
storage, the public Worker, `https://arb1.arbitrum.io/rpc`, one process for all seven:

| Wallet | Block | Figures | Differ | Pools | Cold | Wallet nest (s, RPC) | Pools nest (s, RPC) | Mirror |
|---|---|---|---|---|---|---|---|---|
| 0xcdb9…b2c3 | 520,262,313 | 81 | 0 | 2 | 109.9 s | 64.1, 674 | 40.0, 40 | 179.4 MiB |
| 0x3a35…bb3c | 520,262,557 | 43 | 0 | 1 | 129.8 s | 82.1, 764 | 42.9, 22 | 179.4 MiB |
| 0xe3af…2ac2 | 520,263,307 | 32 | 0 | 1 | 151.1 s | 108.6, 925 | 38.7, 22 | 179.4 MiB |
| 0xd7d7…d076 | 520,263,796 | 53 | 0 | 3 | 112.9 s | 61.6, 643 | 45.6, 56 | 179.4 MiB |
| 0x96a3…65b2 | 520,264,288 | 50 | 0 | 0 | 118.8 s | 74.5, 669 | 41.9, 22 | 179.4 MiB |
| 0x6d64…24bc | 520,264,803 | 50 | 0 | 0 | 115.1 s | 69.4, 678 | 43.4, 22 | 179.4 MiB |
| 0xacbd…0db5 | 520,265,243 | 100 | 0 | 0 | 151.2 s | 103.5, 805 | 45.2, 22 | 179.4 MiB |

409 figures, 0 differing. The mirror half is the same for every wallet: 2,002 GETs, 188,066,801 bytes
(179.4 MiB, under 200), the dataset since grown to 2,000 segments complete through 520,219,931. No GET
named the wallet or any of its indexers. The pools nest's linear memory was 26.1 MiB; the process's peak
RSS reached 738 MiB over all seven. The stakes statement is 5,726 bytes, where S7's hit the 16,384-byte
cap once with its literal rows; an earlier run of 0xd7d7 alone gave the same 53 figures in 111.7 s.

**The fixture gates, `embed/hosts/node/hybrid-gates.mjs`**, on RFC-0077 S4's nest and mirror followed
on RFC-0075 S3's recording, in CI's `wasm-plain` leg:

| Gate | Result |
|---|---|
| `mirror+rpc` under public filters equals `rpc` under the same filters | **pass**: the same 147 rows at block 26,245,870 |
| No GET names a filter's value; two opens differing only in value fetch the same URLs | **pass** |
| A private filter on a `mirror` and on a `mirror+rpc` nest, refused by name before any GET | **pass**, both |
| Another nest's Arrow answers attached, a `HUGEINT` sum included | **pass**: 147 and 7,350,000,000,000,000,001, `DECIMAL(38,0)` |
| An attachment from another block, refused by name | **pass**: "`other.moved` is the other nest's answer as of block 26245820, and this nest answers as of block 26245870" |

**Mutations**, each built and run against that script, then reverted: a `get` chosen by a filter's
value (one extra fetch when the value ends in `c`) turns the same-URLs check red and nothing else; a
filter's value in a `get`'s query string turns both privacy checks red; the private-filter refusal
removed turns both refusal checks red. The first form tried, dropping the segments outright, failed the
open instead and was discarded as red for the wrong reason.

**Unchanged.** S5's 13 shapes (`shapes.mjs`), S3's restart and S5's 32 answers in Node, fresh and killed
(`run.mjs`), S4's mirror gates and postcards, and S5's six verified cases all pass on the S1 module. The
event build is 5,321,484 bytes brotli against the 5,500,000 gate, 41,626 more than S0's 5,279,858:
178,516 bytes of headroom. The binary is unchanged: nothing outside `embed/`, `docs/` and CI's
`wasm-plain` step is touched, so its `Cargo.lock`, `cargo tree` and `--help` are `main`'s.

**What it does not show.** Node only: no browser, OPFS or phone (S5's). One cold open per wallet: the
pack is not yet reused between visits, so a second visit fetches the 179 MiB again. `/balances` and the
built-in views of a `mirror+rpc` nest fold the hydrated history whole, not through the filters; read the
named tables through `/sql`. The JavaScript tab build (`/play`) answers Arrow too but does not expose the
envelope; the plain module is the engine this RFC builds on.

## 13. S2 measured, 2026-11-01

**Verdict: the gate passes.** Chief said go to S2 the same day.

**What was built.**

- **`nuthatch-engine.js`**, `embed/hosts/package/`: `engine.js` and `build.mjs`, which joins it with
  `host.js`, `opfs.js` and `fs-storage.mjs` into one ES module, their code copied unchanged, and names
  each source's sha256 in its header. 29,298 bytes (9,895 gzip), no dependency, no top-level await: a
  `node:` import becomes `process.getBuiltinModule`, which a browser and a bundler never reach. The
  API is §3.3's: `Engine.open({module, nest, rpc, storage, ...})`, `follow()`, `sql(q, {attach})`,
  `arrow(q)`, `request()`, `close()`, with every other key `nh_open`'s, so `source`, `filters` with
  `privacy`, `mirror` and `attach` pass through. In a page each nest runs in the package's own worker,
  one per page, started from the package's URL; in a worker or in Node, in place. It owns the store
  (OPFS, memory, or a directory in Node), names it by module, files and what the store holds when the
  caller does not, retries a POST that got no reply, splits a batch past its endpoint's cap (10 for
  arb1, Lodestar's figure) and paces the parts through a throttle, and carries `follow_failed` as
  S7's worker did. It refuses a module that is not `abi: 1`. `embed/README.md` documents it.
- **The pack reused between visits**, S1's first gap. The module already reopened a hydrated store
  without asking the mirror; nothing checked it. Now hydration records the dataset in the store, and a
  reopen requires the files to name that dataset and every packed segment to hash to its content
  address, or fails `open_failed` ending "open it fresh", which the package does once. What is new
  comes from the RPC, from the store's last block, as for any reopened nest. The report's `mirror`
  gains `reused`. No export changes; `abi` stays 1.
- **Release.** The engine job builds the package at the released version, runs both package gates on
  the module that ships, and attaches `nuthatch-engine.js` and its `.sha256` beside the module, attested
  and checked before publishing like it. No npm (§9). Its `workflow_dispatch` dry run on this branch
  (run 41307875395) passed and kept both as an artifact: the package's sha256 there,
  `9cab0a9f…7793ae25`, is the one this Mac builds, so the bundle is the same bytes on both.

**The gate.**

| Gate | Result |
|---|---|
| Lodestar's `/profile` on the package, its vendored worker and batching deleted, S7's seven addresses | **pass**: 409 figures, 0 differing, headless Brave on OPFS (below) |
| A `docs/` example under 40 lines: defi-pulse-nest from the public mirror, one `/sql` equal to a node | **pass**: `docs/examples/engine-defi-pulse.mjs`, 29 lines. 1,573 segments through block 26,319,435 in 34.7 s; 145,574 USDC mints and 31,712,233,655,307,092 minted to block 24,500,000, the same from the 5.16.0 binary seeded from the same mirror (`nuthatch seed`, `nuthatch sql`) |
| The package's Node half runs S5's 32 answers to `22966cee…363febc5` | **pass**, `hosts/package/run.mjs s5`: opened fresh (1,433 RPC requests, 4.2 s) and again from its kept store (2 requests, 0.1 s), both `22966cee…363febc5` |
| Binary unchanged; size gate held | **pass**: nothing outside `embed/`, `docs/` and the two workflows is touched. 5,322,991 bytes brotli, 219 fewer than `main`'s module and 177,009 under the gate |

**Lodestar** (`pete/engine-package`): `verify-worker.js`, the vendored `host.js` and `opfs.js`, the
header batching and `pools.toml` are gone. The page imports the package from
`public/verify-locally/engine/`, runs the wallet nest `rpc` with private filters and `attach: pools`,
and, when the wallet is in a pool, the pools nest as graph-allocations-nest at `7795a33` opened
`mirror+rpc` with public filters, its two answers attached as Arrow. `literalRows` is gone; a wallet in
no pool opens no pools nest and its statements read two empty relations. The lines that host the
engine fell from 233 (the worker and `browser.ts`) to 132 (`engine.ts` and `browser.ts`); with
`run.ts` and the vendor script the four files went from 450 lines to 386, the vendor script now also
writing the pools nest. Apple M5 Pro, `next dev`, headless Brave, fresh profile per address, the public
Worker and `https://arb1.arbitrum.io/rpc`:

| Wallet | Block | Figures | Differ | Pools | RPC requests | Cold |
|---|---|---|---|---|---|---|
| 0xcdb9…b2c3 | 520,276,680 | 81 | 0 | 2 | 708 | 104 s |
| 0x3a35…bb3c | 520,276,896 | 43 | 0 | 1 | 777 | 122 s |
| 0xe3af…2ac2 | 520,277,318 | 32 | 0 | 1 | 943 | 166 s |
| 0xd7d7…d076 | 520,277,938 | 53 | 0 | 3 | 691 | 107 s |
| 0x96a3…65b2 | 520,278,374 | 50 | 0 | 0 | 659 | 73 s |
| 0x6d64…24bc | 520,278,574 | 50 | 0 | 0 | 670 | 70 s |
| 0xacbd…0db5 | 520,278,782 | 100 | 0 | 0 | 798 | 107 s |

Lodestar's lint (0 errors), typecheck and 1,314 tests pass. Its `scripts/verify-locally-node.mjs` runs
the page's own `verify` on the package over files, which is how the Node figures below were taken.

**The second visit, 0xd7d7…d076.**

| | Mirror GETs | Mirror bytes | RPC requests | Time | Figures |
|---|---|---|---|---|---|
| Brave on OPFS, first visit | 2,002 | 188,066,801 | 690 | 107 s | 53, 0 differ |
| Brave on OPFS, second visit, same profile | 0 | 0 | 4 | 6 s | 53, 0 differ |
| Node over files, first visit | 2,002 | 188,066,801 | 700 | 115.6 s | 53, 0 differ |
| Node, second visit | 0 | 0 | 4 | 4.6 s | 53, 0 differ |
| Node, a later visit, 4,203 blocks on | 0 | 0 | 72 | 8.1 s | 53, 0 differ |

The two second visits were at the server's same block, which its cache held. The pools nest's reopen,
the 2,000 packed segments (185,432,974 bytes) hashed against their addresses included, took 1.7 s in
Node. **Mutation**: the hash comparison in `Cellar::check_packed` disabled, `run.mjs reuse` goes red
on its altered pack ("an altered pack was opened, not fetched again", the rebuild failing on the bytes
the check let through), and passes again restored.

**What does not match the RFC, or is left.**

- **Reuse is per store, and a store is per pool set.** A different wallet, or the same wallet in a new
  pool, is a new store and fetches the 179 MiB again. The RPC tail is filtered by the pools, so a
  store cannot be shared across pool sets; sharing the pack alone between stores is not built.
- **The page ships the pools nest's data files**, 1,744,203 bytes of JSON (720,000 gzip), because the
  identity hashes every data file, and graph-allocations-nest's include `verification/` id lists and a
  check script that no indexing reads.
- **Stores are not cleaned up.** A new module names new stores; the old ones stay in OPFS.
- `embed/hosts/node/hybrid.mjs`, S1's driver, reads Lodestar's `literalRows` and two-config
  `nestFiles`, which the Lodestar branch removes; it runs against Lodestar's `main` as it stood for S1.
- Firefox, Safari and a phone are S5's; no browser but Brave ran the package.

## 14. S3 measured, 2026-11-01

**Verdict: the gate passes, against a stand-in for the hosted nest.** Chief said go to S3 the same day.
Lodestar's public SQL route cannot answer the gate as written, for two reasons recorded below, so the
hosted nest in every figure here is the 5.16.0 release binary running graph-allocations-nest, seeded
from the public mirror and following arb1 to the tip, answering `POST /sql` as any operator's node
does. That is a departure, and the first thing to read.

**Lodestar's route, tried first.** `https://api.lodestar-dashboard.com/api/sql/query` with dataset
`staking` is kittiwake's free-form tier in front of the allocations nest. It returns the nest's rows and
`provenance` with `nid` and `as_of`, re-wrapped as `{data: {rows}, provenance}`, which `verify` reads.

- **It refuses two of the four statements.** The delegator and stakes statements fold each position
  with `list_reduce(..., lambda acc, e: ...)`; kittiwake screens every statement with sqlparser's generic
  dialect, which cannot parse a lambda, and answers 400 ("Expected: ), found: acc"). The curator and
  signals statements are answered.
- **Its NID is the NID of no published files.** It answers as `e82e8cf6…93c1`. The files that name the
  public dataset, graph-allocations-nest at `7795a33`, compute `c24c60af…3092` (hidden files included,
  as a node counts them); `36bd3de`, `680cedb`, `c313d08` and `1730f70` compute four others, and the
  dataset's own `publish.json` names a fifth, `cb768e49…7ec6`. Production's directory differs from every
  commit tried, in files that do not touch the data. `verify` refuses the curator answer by name, before any request:

  > these files compute NID c24c60af… (data identity d6fd43c8…6e8b), and the answer names e82e8cf6…:
  > they are not the nest that answered

  That refusal is the design working, and it is why no figure here is from Lodestar's host. Its registry
  hash, `0x916324e2…0449`, is the stand-in's, and both answered 2,707 delegations to the same query.

**What was built**, under `embed/` only, `abi` still 1 (an optional `nh_open` key, a request body key
and optional answer fields):

- **`Engine.verifier(options).verify(answer, statement, filters)`** in the host package, `answer` also a
  list of `{name, answer, statement}` at one block. It reads `provenance.nid` and `as_of`, refuses a
  truncated answer or one naming no block, and opens the nest by NID: the caller's files must compute it
  (`expect_nid` below), the dataset under the mirror is the one they name, as S4 resolves it.
  `mirror+rpc` when every filter is public, `rpc` otherwise; `until` the answer's block, and a nest that
  reaches another block is refused rather than compared. Each statement is run there and compared column
  by column as text, rows in one order whatever order either side gave. A column is equal, differs (every
  differing row's two values, up to 20), or not computable (named, never shown: a column only one side
  has, or a statement this nest refused). The result names the NID, block, source, filters, mirror
  dataset and end, the RPC's host, and that the head is the RPC's.
- **`expect_nid`** on `nh_open`: the NID the files must compute, refused by name before the host is asked
  for anything. `nh_report` carries `identity`, `{nid, dataset}`, and `follow()` returns it.
- **`narrowed`** in a narrowed nest's `/sql` provenance (the tables its filters name, the tables the
  statement read, those no filter covers) and **`"whole": true`** in a `/sql` body, which reads a
  `mirror+rpc` nest's hydrated tables without the filters' predicate. Both are the module's own fields;
  `sql_envelope` in core is untouched.
- The package's `nest` directory is now read as a node reads it for a NID: hidden files count, `.git`,
  `.DS_Store`, `.admin-token`, `nuthatch.redb` and `segments` do not. Before, it skipped every hidden
  file, so a nest with `.claude/` computed a NID no node gives. A default store name changes with it.
- A `follow_failed` ending "open it fresh" (a kept store past `until`) is opened fresh once, as an
  `open_failed` one was.

**The rule for "outside the filters".** A statement is outside them when either holds, and then nothing
is compared:

1. **It names a table no filter covers.** The nest holds no row of it. Read from the module's own parser
   (`narrowed.unfiltered`); it parsed all four portfolio statements, lambdas included.
2. **Its answer depends on rows the filters do not select.** On a `mirror+rpc` nest each hydrated table
   holds the published dataset's every row, and the filters are a predicate over it (§12). The statement
   is run twice, through the filters and `whole`; if the answers differ, it reads outside them.

Neither needs the server's cooperation, so a lying server cannot turn a mismatch into "outside" by what
it answers. The limits: rule 2 sees the network only through the mirror's end, so a row outside the
filters that arrived after it is not seen; and on an `rpc` nest only rule 1 applies, since nothing
outside the filters was ever fetched. The caller still declares the filters, and the result says which.

**The gate.** `embed/hosts/package/verify-gate.mjs`, with the four statements copied from Lodestar's
`src/lib/verify/sql.ts` into `embed/hosts/package/fixtures/portfolio.mjs`. Where Lodestar wrote pools
and thaw requests in as literal rows, these read the nest's own tables, limited to the wallet and its
pools (found from the host's own stakes answer, as S7's page finds them), so one statement goes to the
host and runs in the narrowed nest unchanged. The filters are Lodestar's wallet and pool filters, marked
public: the wallet's address is already in the statement sent to the host, and a `mirror+rpc` GET is
chosen by events, never values (§12). Apple M5 Pro, Node 26.11.1, files, a fresh store per wallet, the
public Worker and `https://arb1.arbitrum.io/rpc`; the host at `c24c60af…3092`:

| Wallet | Block | Pools | Statements | Columns equal | Rows | RPC requests | Cold |
|---|---|---|---|---|---|---|---|
| 0xcdb9…b2c3 | 520,303,534 | 2 | 4 equal | 38 of 38 | 7 | 50 | 57.6 s |
| 0x3a35…bb3c | 520,303,534 | 1 | 4 equal | 38 of 38 | 4 | 34 | 55.6 s |
| 0xe3af…2ac2 | 520,303,534 | 1 | 4 equal | 38 of 38 | 3 | 32 | 57.2 s |
| 0xd7d7…d076 | 520,304,615 | 3 | 4 equal | 38 of 38 | 4 | 66 | 54.6 s |
| 0x96a3…e5b2 | 520,304,615 | 0 | 4 equal | 38 of 38 | 10 | 32 | 49.1 s |
| 0x6d64…24bc | 520,304,615 | 0 | 4 equal | 38 of 38 | 10 | 32 | 47.4 s |
| 0xacbd…0db5 | 520,304,615 | 0 | 4 equal | 38 of 38 | 20 | 32 | 51.4 s |

28 statements, 266 columns over 58 rows, every one equal, none named. Each open hydrated 2,236 segments,
204,949,749 bytes (195.5 MiB, under S1's 200), more than S1's 179.4 MiB because the wallet's curation and
GNS tables now come from the mirror too; 2,238 GETs, none naming a wallet. The RPC tail ran from the
mirror's end, 520,219,931, about 84,000 blocks.

| Check | Result |
|---|---|
| A network-wide statement (every delegation's tokens) | **outside the filters**: "its answer changes when the rows outside the filters (the published dataset's, through block 520219931) are read" |
| A table no filter names (`epochs__epoch_run`) | **outside the filters**: "it reads epochs__epoch_run, which no filter covers" |
| One value altered by a proxy in front of the host | **differs** on that column alone: `total_signalled_tokens`, host `23491697265158386461000001`, here `2349169726515838646100000`; for 0xd7d7 alone, `total_staked_tokens`, `2735450000000000000000001` against `273545000000000000000000` |
| An answer naming another NID | **refused** by name, 0 GETs |

**In CI**, `hosts/package/verify-fixture.mjs` in the `wasm-plain` leg and the release's engine job, on
RFC-0077 S4's nest and mirror followed on RFC-0075 S3's recording: one receiver's statement equal on
every column, 147 transfers at block 26,245,870; every transfer's sum outside the filters; one value
altered, differing on that column with both values; another NID refused before any GET. The "host" is
the same nest opened whole through the package, its answers named by the NID the module computes for the
files, which the stand-in above shows a node gives too.

**Mutations**, each run against that script and reverted: the `whole` probe dropped from `verify`, and
separately `whole` ignored in the module, each turns the network-wide check red ("differs") and nothing
else; the value comparison disabled turns the altered check red; the `expect_nid` refusal removed turns
the other-NID check red.

**Unchanged.** S5's 32 answers through the package fresh and kept, the pack reuse, and S1's hybrid gates
pass on the S3 module. The event build is 5,326,406 bytes brotli against the 5,500,000 gate, 1,590 more
than `main`'s module built here: 173,594 bytes of headroom. The package is 38,397 bytes (12,632 gzip).
The binary is unchanged: nothing outside `embed/`, `docs/` and the two workflows is touched.

**What does not match the RFC, or is left.**

- **No Lodestar host in the figures.** Its route cannot parse half the statements and its NID names
  files nobody has. For a hosted operator to say "check this yourself", `verify` needs the operator's
  exact files published beside the dataset, or the files whose NID the node reports to be the ones the
  repository holds. Which, and whether the mirror should carry a nest's files, is a question for Chief;
  nothing here changes the binary or the publisher.
- **"Opens by NID" means the caller brings the files.** The mirror is keyed by data identity and its
  `publish.json` names one NID per dataset, so a NID cannot be looked up there; the files are checked
  against it instead, and the dataset they name is hydrated.
- **Rule 2's horizon** is the mirror's end, and on an `rpc` nest only rule 1 applies, as above.
- **Rows are compared as a set,** in one canonical order, so a host that answers the right rows in the
  wrong `ORDER BY` order is not caught; the values are.
- Node only. The verifier runs in a page through the package's worker unchanged, but no browser ran it.
- `embed/hosts/node/hybrid.mjs`, S1's driver, is deleted: Lodestar #389 removed the `literalRows` and
  two-config `nestFiles` it imported. Its job is covered by `hybrid-gates.mjs` (the hybrid open, the
  privacy rule and `attach`, in CI), the package's `run.mjs`, Lodestar's own `verify-locally-node.mjs` on
  the package (§13), and now `verify-gate.mjs` for the seven wallets live.
