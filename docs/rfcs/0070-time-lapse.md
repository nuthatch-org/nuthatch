# RFC-0070: Time-lapse - a contract's sealed history replayed at speed

- Status: **Parked 2026-10-22 (Chief): stopped after S2.** Chief scratched it the day S2 was built;
  the engine room (RFC-0069) is the only programme running. Do not start S3 unasked. Accepted
  2026-10-21. S0 measured 2026-10-22 (§13); Chief watched its rendering the same day and said keep
  building. S1 built 2026-10-22 (§14): `nuthatch-timelapse` gives S0's three
  frame hashes natively and as wasm in Node, and its validator refuses each forbidden construct by name.
  S2 built 2026-10-22 (§15): the player page replays four specs over the public mirror in a tab, each to
  the native hash, and its single-file export plays every frame offline with zero requests.
- Author: Pete (cargopete)
- Date: 2026-10-21
- Depends on: RFC-0047 (the sealed directory as a contract), RFC-0061 (256-bit values stay decimal
  text), RFC-0065 (the nest in a tab: Burrmill and mirrored segments in a browser worker).
- Related, being written in parallel: RFC-0068 (nest postcards, a self-contained `.html` holding
  segments and Burrmill-wasm) and RFC-0069 (the engine room, a live terminal dashboard). This RFC
  depends on neither; §6 says how it would use each.
- Blocks: nothing.
- Nature: a new pure crate, a player page beside the RFC-0065 playground, and later an optional
  producer in the binary behind an off-by-default feature (§9).

## 1. What it is for

Chief asked for this one as a fun feature, and it should be judged as one: a thing people watch and
pass on. A time-lapse takes a nest's sealed history and plays it back fast. The top holders of a
token reshuffle, an indexer climbs the delegation table, a pool's reserves swing through a depeg.
"Watch the Graph's top indexers by delegated stake through 2024 in 40 seconds."

What makes it more than a chart is the thing nuthatch already has. The source is sealed,
content-addressed segments, so a replay is a pure function of the nest's data and a short spec. The
same nest and the same spec give the same frames, byte for byte, on any machine and in a browser
tab. A shared recording therefore carries its own check: anyone holding the nest, or the mirror's
copy of it, can recompute the frames and compare one hash.

## 2. What exists, read 2026-10-21 at `origin/main`

- **Segments are already in replay order.** `sort_rows_for_seal` (`seal/src/lib.rs:132`) sorts every
  segment by `(block_number, log_index)`, and each catalogue entry carries `from_block`, `to_block`
  and `rows` (`seal/src/cut.rs:19`). A block-ordered pass can be planned from `manifest.json` alone,
  without opening a file, and a chunk of known row count picked before it is read.
- **Readers can pin what they read.** `seal::read_lease` (`src/seal.rs:438`) keeps a segment on disk
  while a reader holds it, which matters because a provisional segment is replaced and removed at the
  table's next seal (`seal/src/cut.rs:30`).
- **Burrmill has what a per-chunk query needs.** Its dialect parity suite (`burrmill`
  `crates/burrmill-bench/src/dialect_parity.rs`) covers window functions, `QUALIFY`, `arg_max` and
  `last_value ... IGNORE NULLS`, and RFC-0065 runs it in a browser worker over mirrored segments.
- **The arithmetic is already a dependency.** Amounts are canonical decimal text (RFC-0061).
  `alloy-primitives`' signed 256-bit integer is already used by `nuthatch-decode`, which builds for
  both wasm targets.
- **The built-in IVM is the wrong engine for this.** `balances`, `exposure` and `velocity`
  (`src/exposure.rs`, `src/velocity.rs`) and RFC-0041 entities are DBSP circuits that keep the
  *current* answer, with durability and reorg retraction around them. A replay needs every
  intermediate answer, never retracts, and has to run in a tab, where DBSP does not build. Their
  declarations are still the right *vocabulary* (a key and a summed quantity), and §4 borrows it.
- **RFC-0059's folds are adjacent but different.** A checkpointed fold carries an order-dependent
  view to the head for serving; it is behind the off-by-default `folds` feature and parked. A
  time-lapse carries a much simpler state and throws it away when the replay ends.

## 3. Prior art

Bar-chart races (Flourish, and countless matplotlib scripts) show the form works and people share
it. Dune and similar dashboards draw balances over time, but from a query re-run per bucket over a
hosted warehouse, and nothing about the result can be checked by a third party. asciinema shows a
terminal recording can be a small text file rather than a video. None of them ties the picture to
content-addressed source data; that is the part only nuthatch can offer.

## 4. The design

### 4.1 A frame

A **frame** is the state of one keyed quantity at one block: the top N keys by value, the number of
keys whose value is non-zero, and the total. Frame `i` describes the state after every event with
`block_number <= from_block + (i + 1) * step - 1`. It carries the closing block number and that
block's timestamp, so a player can print a date without an RPC.

Two fold kinds cover the examples Chief gave:

- **`sum`**: balances, positions, delegated stake, holder counts. Each event contributes signed
  deltas to keys; the state is the running sum per key.
- **`last`**: pool reserves, prices from `Sync`-style events, any gauge an event reports absolutely.
  The state per key is the value from the latest `(block_number, log_index)`.

Ranking is by value descending, then key ascending as bytes, so a tie never depends on hash-map
order. A key whose `sum` returns to zero leaves the state entirely, which is what bounds memory by
*live* holders rather than every address ever seen (§5).

### 4.2 Declaring one

A small TOML file, outside the nest directory, so it touches neither the NID nor the data identity:

```toml
title = "Top delegators by tokens delegated"
nest = "graph-allocations-nest-next"      # resolved to a NID when the replay starts
from_block = 60_665_219
to_block = 408_795_910
frames = 1200                             # or step_blocks; one is derived from the other
fps = 30                                  # playback only; does not enter the hash
top = 10

[quantity]
fold = "sum"
sql = """
SELECT block_number, log_index, delegator AS key, tokens AS delta
FROM staking_legacy__stake_delegated
"""
```

For a token's holders the query is a `UNION ALL` of the credit to `"to"` and the debit from
`"from"`, the debit marked by a `sign` column, because a decimal-text amount cannot be negated in SQL
without leaving 256 bits.

The query is **row-local**, and loading refuses, by name, anything else: no aggregate, window,
`DISTINCT`, join, `ORDER BY` or `LIMIT`, only decoded tables as sources, and exactly the columns
`block_number`, `log_index`, `key`, `delta` (or `value` for `last`) and an optional `sign`. The rule
is what makes chunked execution exact: a row-local query over a block range returns the same rows
whether it runs over one chunk or the whole history. The validator follows the shape of
`src/entities.rs`, which already refuses what RFC-0041 cannot maintain.

Why not a SQL query with a block parameter, evaluated once per frame: §5 measures it. Why not reuse an
RFC-0041 entity by name: attractive, and left as an open question (§12), because an entity's SQL is an
aggregate that would have to be lowered back into the row-local map, and that lowering is new code to
get wrong.

### 4.3 One pass

1. Resolve the nest, take a read lease, and take the catalogue's final segments for the tables the
   query reads, up to `to_block`. **Provisional segments and the hot tail are never read**: the first
   are replaced at the next seal, the second can still reorg. A range above `sealed_through` is
   refused, with the block it would need.
2. Plan chunks from the catalogue: consecutive block ranges, cut on frame boundaries, each holding at
   most a fixed number of rows across all the query's tables (RFC-0059 S0 learned to bound windows by
   event volume, not by blocks).
3. For each chunk in block order, Burrmill evaluates the query over that range and returns rows
   ordered by `(block_number, log_index)`; the fold applies them, parsing each amount into a signed
   256-bit integer and failing loudly on anything else.
4. At every frame boundary the fold emits a frame from an ordered index over the live keys.

The pass starts at the nest's first sealed block, not at `from_block`, because the state on
1 January 2024 is the sum of everything before it. Frames are only emitted inside the range.

### 4.4 The recording

A recording is two things, both canonical JSON:

- **`frames.jsonl`**: one line per frame, keys in a fixed order, values as decimal text.
- **`manifest.json`**: the NID and data identity, the spec's hash, the fold's version string, the
  catalogue entries read (table, hash, block range), `from_block`, `to_block`, the frame count and
  `frames_sha256`.

Verification is recomputation: load the spec, replay over any copy of the nest whose data identity
matches, and compare `frames_sha256`. The segment list is provenance rather than the check, because a
copy re-sealed under a newer writer profile holds the same rows under different file hashes. Playback
choices (fps, colours, easing between frames) live in the player and never enter the hash.

## 5. Cost

S0 measured these on real segments; where §13 differs, §13 stands.

Measured 2026-10-21 on this MacBook (Apple Silicon), on a local copy of
`graph-allocations-nest-next`: 4,811,656 rows in 1,970 segments, 647 MB on disk, which is about
134 bytes per sealed row. The test table is `staking_legacy__stake_delegated`: 346,288 events, 139
indexers, 309,549 delegators. The fold was a 70-line std-only Rust program over the events exported
in seal order; it is a stand-in for §4.3 step 3, not the design's code.

| What | Keys | Frames | Time | Peak RSS |
|---|---|---|---|---|
| Per-frame grid in SQL (DuckDB, 2 threads): dense key x frame, running sum, `row_number` | 139 indexers | 1,200 | 0.05 s | 0.28 GB (whole run) |
| The same SQL | 309,549 delegators | 1,200 | did not finish in 3 min; killed at 13.8 GB | - |
| Streaming fold, top 10 | 309,549 delegators | 1,200 | 0.12 to 0.13 s | 146 MB |
| Streaming fold, top 10 | 309,549 delegators | 12,000 | 0.12 s | - |

Three things follow. The grid approach is fine for a handful of keys and unusable for holders,
because it materialises keys times frames (371 million cells here) while only 324,425 of them ever
change. The fold costs about 0.35 µs per event and almost nothing per frame: ten times the frames
cost no measurable time. And it is deterministic in the plain sense: two runs gave the same frame
hash. Reading the rows is the larger cost: DuckDB scanned one text column of all 4.8 million rows in
0.39 s, so a four-column read is estimated at a few million rows a second on two threads.

**Estimate, not measured: USDC on Ethereum through 2024.** Assume 50 to 100 million `Transfer` events
in the year and 2 to 4 million live holders; both are order-of-magnitude figures to be replaced by a
count in S0. At 134 bytes a row that is roughly 7 to 13 GB of segments for the year, and several
times that for the history before it, which the pass must also read (§4.3). At an estimated 3 to 5
million rows a second the year's read is 10 to 35 s, plus the earlier history. State: the measured
fold held two `String` copies of each 42-character key and used about 470 bytes per key including
its input; interned 20-byte keys, a 256-bit value and one ordered-index entry come to an estimated
100 bytes, so 200 to 400 MB for USDC's live holders. That fits the 2 GB budget only because zero
balances leave the state; the measured form at 4 million holders would be about 1.9 GB. The
producer therefore carries a hard state cap (1 GiB by default), stops with a named error when it is
crossed, and never swaps.

A recording is small: 1,200 frames of ten entries at about 90 bytes each is about 1.1 MB of JSON
before compression. That is the property the rest of the design leans on.

## 6. Rendering, and which ships first

The producer and the renderers are separate. A renderer reads `frames.jsonl` and nothing else, so
there is one frame format and as many players as are worth having, rather than a third renderer that
knows about segments.

- **The tab first.** A player page beside the RFC-0065 playground. For a mirrored nest small enough
  to load, the tab runs the producer itself, in wasm, over the segments RFC-0065 already fetches and
  hash-checks; for a large one it plays a recording made natively. **Export** is a single `.html`: the
  player, its CSS and the frames inlined, no network request of any kind, about 1 to 2 MB. If RFC-0068
  lands, a postcard can carry a time-lapse as one more panel, and there the replay can be recomputed
  in place, because the postcard already holds the segments and Burrmill.
- **The terminal second.** If RFC-0069 is accepted, a frames file becomes one more thing the engine
  room can play, with its instruments, rather than a second TUI in this RFC. **Export** is an
  asciinema v2 cast, which is newline-delimited JSON written directly: no crate, and nothing from
  asciinema itself, which is GPL-3.0, is linked or vendored.
- **GIF and video are not built here.** A GIF needs a font rasteriser and a bundled font; the
  candidate fonts are OFL-1.1, which `deny.toml` does not allow. The tab can record its own canvas
  with the browser's `MediaRecorder`, and a cast converts with `agg` (Apache-2.0) outside the binary.

The tab ships first because sharing is the point and a static page is the most shareable thing
nuthatch can make; because it needs no binary change (§9); and because RFC-0065 has already done the
hard part of running Burrmill over verified segments in a browser. The terminal player is cheap once
the engine room exists, and premature before it does.

Licences: `alloy-primitives` (MIT OR Apache-2.0) and `sha2` (MIT OR Apache-2.0) are already in the
tree. A terminal player in this RFC would bring `ratatui` and `crossterm` (both MIT); deferring the
terminal to RFC-0069 avoids adding either here.

## 7. Non-goals

- Arbitrary SQL per frame. Joins, windows and aggregates over history are what §5 shows does not scale
  with keys; the row-local rule is the design, not a first-version limit.
- The hot tail or the live head. A time-lapse is of sealed history; following the head is the engine
  room's job.
- USD values, prices derived from mapping logic, or anything RFC-0053 calls the ordered,
  mapping-derived family. A `last` fold over a `Sync` event's reserves is in; a computed price is not.
- Two chains in one replay, or two nests merged into one frame stream.
- Writing anything into the nest. The replay opens segments read-only and leaves no state behind.
- Video encoding, GIF writing or font rendering in the binary.
- Hosting recordings for people. A recording is a file the operator keeps or posts where they like.

## 8. Against the non-negotiables

- **Single static binary.** S0 to S2 change nothing the binary ships. S3 adds a producer behind an
  off-by-default feature, with no new dependency (Burrmill, `alloy-primitives`, `sha2` are present).
- **2 GB per cursor.** The replay is not a cursor and holds none, but it runs on the same boxes, so it
  carries the 1 GiB state cap of §5 and S3's gate records peak RSS on the largest nest available.
- **No phone-home.** The producer reads local files. The exported page loads nothing: no CDN, no font
  service, no analytics. S2's gate checks this mechanically.
- **Determinism.** Frames derive only from final sealed segments through a row-local query and an
  integer fold, ordered by `(block_number, log_index)` with byte-order tie-breaks. No LLM is anywhere
  near it, and nothing it computes feeds stored state.
- **Licence.** Every dependency named here is MIT, Apache-2.0 or both; asciinema's GPL code is not
  used, only its documented file format.
- **RFC-0044 §8.** S0 to S2 need no record: a harness, a crate the binary does not call, and a page
  outside the binary. S3 needs Chief's acceptance, as RFC-0059 and RFC-0060 did for their features.
  Promotion to the default binary would need the §8 line in `CLAUDE.md`.

**RFC-0046 §1's deletion test, applied honestly.** It was written for payment, and read literally it
is passed trivially here: delete the time-lapse and a self-hoster loses the time-lapse and nothing
else, because it stores nothing, changes no config `init` writes, adds no field to any on-disk format
and gates nothing. That makes the test necessary but not decisive; it says the feature is safe to
have, not where it should live. Placement is decided by the cost to everyone who does not use it, and
that is what §9 weighs. The stronger form RFC-0059 and RFC-0060 use, that a default build's CLI help,
`init` output, on-disk layout and behaviour are unchanged, is S3's gate, and it can fail.

## 9. Default binary or off by default

**Recommendation: off by default for S3, a `timelapse` cargo feature, with promotion to the default
binary left to Chief once recordings exist that people watch.** The case for default is real: no new
dependency, no stored state, a few hundred lines. The case against is that it is a new subcommand in
every operator's help, for a feature nobody has yet asked for, and that the tab covers the demo
without it. A feature costs nothing to promote later; a default subcommand is awkward to take away.

## 10. Slices and their gates

- **S0, the measurement that can stop it.** A harness in `tools/`, not shipped, runs the design's fold
  over a real nest's segments through Burrmill. Gates: at 20 sampled frames, the fold's top N and
  totals are byte-identical to Burrmill's `SELECT key, SUM(...) WHERE block_number <= N` over the same
  segments; two runs on the MacBook and one on the ThinkPad give the same `frames_sha256`; peak RSS and
  time are recorded for the largest nest available, and the real USDC event and holder counts replace
  §5's estimates if a USDC nest is built. **It stops if** the fold and the SQL disagree for a reason
  that is not a defect in the fold, if a representative nest's live-key state exceeds 1 GiB, or if
  Chief watches the first rendering and does not want a second.
- **S1, `nuthatch-timelapse`.** The spec, its validator, chunk planning, the fold, the frame encoding
  and the manifest, as a pure crate added to the wasm `cargo check` that covers `nuthatch-decode` and
  `nuthatch-seal`. Gates: the validator refuses each forbidden construct of §4.2 by name, one test per
  construct, each mutation-checked; native and wasm builds give the same `frames_sha256` on S0's nest.
- **S2, the player and the export.** The page beside the playground: replay a mirrored nest in the tab
  and play it; load a recording; export one `.html`. Gates: the exported file, opened in a headless
  browser with networking disabled, plays every frame and makes zero requests; its embedded frames
  hash equals the native harness's for the same spec; the in-tab replay's peak memory is recorded per
  nest, and a nest it cannot fit is a finding, not a pass.
- **S3, the producer in the binary**, behind `timelapse`, off by default. Writes `frames.jsonl` and
  `manifest.json` from a local nest, holding a read lease, and a cast file. Gates: a default build's
  CLI help, `init` output, on-disk layout and behaviour are unchanged (a test that diffs them, and
  fails when the feature leaks); the binary's `frames_sha256` equals the tab's; peak RSS on Lodestar
  recorded and under the state cap; a range above `sealed_through` and a provisional segment are both
  refused.
- **S4, the terminal player**, only if RFC-0069 is accepted: frames played through the engine room.
  Gate: a cast recorded twice from the same recording is byte-identical.

## 11. Risks

- **The estimates in §5 are wrong by more than a factor.** USDC's event and holder counts are not
  measured here. S0 measures the largest nest we have, and the state cap turns a wrong estimate into a
  named error rather than a swapping box.
- **It is not fun.** A replay of a dull table is a dull film. S0's last stop condition is there for
  that, and the demo nest is chosen for its story, not its size.
- **Ties and churn.** A top 10 that changes every frame is noise. The player can ease between frames,
  but the frames themselves stay exact; a smoothed ranking would be a second, unhashed truth.
- **Mirrored nests large enough to be interesting may be too large for a tab.** The design already
  separates producer from player; a large nest is recorded natively and played in the tab.
- **Catalogue churn during a replay.** Holding a read lease covers a provisional segment being replaced
  under the reader; S3 tests it.

## 12. Open questions for Chief

1. Is the tab the right first renderer, with no binary change until S3?
2. Off by default for the producer, or straight into the default binary given it adds no dependency?
3. Which nest is the demo? The delegation table measured in §5 has a story (who leads, and when); a
   USDC nest for a year is a build of roughly 10 GB that does not exist yet.
4. May a nest ship time-lapse specs, say in a `timelapses/` directory kept outside its identity as
   `maintained.toml` is, or do specs stay entirely outside the nest?
5. Should an RFC-0041 entity be accepted by name as the quantity (§4.2), at the cost of lowering its
   aggregate back into a row-local map?
6. Is a browser-recorded WebM an acceptable stand-in for GIF?
7. Should recordings sit beside a nest on the public mirror, as static files like its segments?

## 13. S0, measured 2026-10-22

The harness is `tools/rfc-0070-s0/`, its own crate outside the workspace and the binary. It reads
`segments/manifest.json` and `schema.json`, takes the final segments of the tables the spec's query
names, binds each chunk's segments in Burrmill (`register_facts`, the windowed binding the main crate
uses), runs the row-local query ordered by `(block_number, log_index, key)`, and folds the rows into a
signed 256-bit sum per key with an ordered index over the live keys. Chunks are cut on frame closes
and bounded by catalogue row counts (200,000 by default). It then binds every final segment once and
asks Burrmill, at 20 evenly spaced frames, for `SELECT key, SUM(...) ... WHERE block_number <= N
GROUP BY key HAVING SUM(...) <> 0` ordered by value then key, and for the count and total of the same
grouping, and compares them as text with the frame. It builds the `sum` fold only; `last` is S1's.

Three specs, in `tools/rfc-0070-s0/specs/`, on copies of two nests:

- **The demo**: indexers by net GRT delegated (`StakeDelegated` tokens in, `StakeDelegatedLocked`
  tokens out) on `graph-allocations-nest-next` as sealed on Helsinki on 2026-10-12, blocks
  60,586,400 to 385,547,599, 1,200 frames of 270,801 blocks, 499,514 rows.
- **Delegators**: the same quantity keyed by delegator, for state size.
- **GRT holders**: `grt-supply-nest`, every Arbitrum GRT `Transfer` as a credit to `to` and a debit
  from `from`, the zero address left out, blocks 42,449,227 to 512,623,626, 11,937,686 rows. This is
  the nearest thing to §5's USDC case that exists, and the largest replay by rows; the allocations
  nest (510 MB, 4,801 segments) is the largest by size.

| Gate | Result |
|---|---|
| 20 frames byte-identical to Burrmill's `GROUP BY` | **Pass**, all three specs: 60 of 60 frames, top 10, live count and total identical as text |
| Same `frames_sha256` twice on the MacBook | **Pass**: demo `fce5af90...a4174`, delegators `43bbe523...5f101`, holders `26235ec1...ba906b`, each in two runs |
| Same `frames_sha256` on the ThinkPad | **Pass**: the same three hashes, built and run on x86_64 Linux from the segments copied across |
| Chunking does not change the frames | **Pass**: the demo cut into 3 chunks and into 1,200, the holders into 35 and into 427, give the same hash |
| Peak RSS and time on the largest nest | Recorded below; nothing near the 1 GiB stop |
| Chief watches the first rendering | **Pass**, 2026-10-22: he watched the demo's `.html` and said keep building |

| Replay (MacBook, fold only) | Rows | Live keys at peak | Time | Peak RSS |
|---|---|---|---|---|
| Demo, 136 indexers | 499,514 | 136 | 0.32 to 0.40 s | 134 MB |
| Delegators | 499,514 | 307,788 | 0.49 s | 244 MB |
| GRT holders | 11,937,686 | 119,270 | 7.5 to 7.6 s | 268 MB |
| GRT holders, 20,000-row chunks | 11,937,686 | 119,270 | 8.5 s | 180 MB |

With the 20-frame check in the same process the holders replay peaked at 732 MB and took 20.6 s, the
check being 20 full `GROUP BY`s over 2,944 segments. The ThinkPad took 0.61 s, 0.90 s and 13.6 s for
the three. The holders replay split as 2.5 s encoding rows to JSON, 4.0 s folding them, and the rest
Burrmill reading and sorting.

What this replaces in §5, and what it adds:

1. **Throughput is lower than §5 estimated.** End to end the holders replay ran at 1.57 million rows a
   second, against §5's 3 to 5 million. A third of the time is the JSON detour the harness takes
   because Burrmill's Arrow is not ours to name; S1 should fold Arrow batches directly. At the
   measured rate a year of USDC at 100 to 200 million rows (two per transfer) is 64 to 127 s, not
   10 to 35 s. USDC itself is still not measured: no USDC nest was built.
2. **The per-key state is larger than §5's interned estimate.** 307,652 more live keys cost 110 MB of
   RSS, about 360 bytes a key with 42-character text keys, an `Rc` shared by the map and the index,
   and a 256-bit value. At that size the 1 GiB cap is about 3 million live keys, which is USDC's
   range. S1 should intern 20-byte keys when the key is an address, as §5 assumed.
3. **A key leaves the state only when its quantity is conserved.** GRT holders reach zero exactly and
   leave: 119,270 live at the peak. Net delegation per delegator almost never does, because
   undelegated tokens include rewards and exclude the tax, so 307,788 delegator keys stayed live. §4.1's
   bound by live keys holds for balances, not for every sum.
4. **Never reading a provisional segment can cut a sparse table's end off by months.** The legacy
   staking tables stopped at block 408,818,200, and their last 725 and 271 rows sit in provisional
   segments in this copy, sealed by 5.0.1 before the chain-time age rule of #2084. The demo therefore
   ends at block 385,547,920 (3 October 2025) rather than at the Horizon switchover. A copy re-sealed
   under the age rule would finalise them; until then a replay stops at the last final segment, and
   the refusal names the block.
5. **A frame's timestamp is the newest folded event's, not its closing block's.** A nest does not hold
   every block's timestamp, and `grt-supply-nest` holds none (`block_timestamps = false`), so its
   frames carry `null`. §4.1's "that block's timestamp" needs a source the design does not yet have;
   S1 should name the field for what it is and accept an optional `block_timestamp` column in the
   query, as the harness does.
6. **NID and data identity are not in S0's manifest.** Computing them needs the nest walk the main
   crate does; S0 records the `registry_hash` from `schema.json`, the spec's hash and every segment
   read. S3 has the walk.
7. **A recording is the size §5 said**: 994 KB of `frames.jsonl` for the demo, and the exported page
   with the player is 1.07 MB.

Not verified in S0: the `last` fold; the row-local validator (S1); a read lease, since the harness
reads copies nothing else writes; the in-tab replay; and USDC.

§12, as S0 took them, with Chief's answer to the first: the tab is the first renderer and the binary
is unchanged (1, Chief). The producer stays off by default (2, §9's leaning). The demo is the
delegation table (3). Specs stay outside the nest (4, §4.2's leaning). An RFC-0041 entity is not
accepted by name (5, §4.2's leaning). Questions 6 and 7 were not exercised.

## 14. S1, built 2026-10-22

`nuthatch-timelapse` (`timelapse/`) is the spec, the row-local validator, the chunk plan, both folds,
the frame encoding and the manifest, with no filesystem and no engine. The caller runs
`Plan::chunk_sql` over each chunk's final segments and hands Burrmill's Arrow batches to a `Replay`;
the crate folds them directly, so the same code folds natively and in a tab. It is not a dependency of
the binary. CI runs its fmt, clippy and tests, adds it to the wasm `cargo check` beside `decode` and
`seal` on both targets, and runs `cargo deny` over its tree, which the binary's never reaches.

The gates were run through `tools/rfc-0070-s1/`, a driver outside the binary and outside CI: the same
replay as a native binary and as `wasm32-unknown-unknown` through wasm-bindgen in Node 26, both over
Burrmill `8ab3048` (the rev `burrmill-wasm` builds for the tab), on fresh copies of S0's two nests.

| Gate | Result |
|---|---|
| The validator refuses each forbidden construct by name | **Pass**: 21 refusal tests, one per construct. 20 mutations of the validator, one removing each refusal (the join's turns both join tests red), and 4 of the plan and the replay; each applied to the committed tree, shown applied, red at its test's assertion, restored from `HEAD` to a clean tree |
| Native and wasm give the same `frames_sha256` on S0's nests | **Pass**: both give S0's `fce5af90...a4174`, `43bbe523...5f101` and `26235ec1...ba906b`; the frame encoding is unchanged. A `last` spec (`tools/rfc-0070-s1/specs/indexer-reward-cut.toml`) gives `ea7ec80e...883cf21` on both |
| Frames still equal Burrmill's own answer | **Pass**: 20 sampled frames per spec against `GROUP BY` for `sum` and `row_number() ... = 1` for `last`, 80 of 80 identical |
| Chunk boundaries never change the frames | **Pass**: a property test over random segment tilings, chunk bounds and batch sizes, for both folds; on real segments the demo at 3 and 1,200 chunks and the holders at 35 and 427, native and wasm, give one hash each |
| Peak RSS and time against S0 | Recorded below |

| Replay, fold only (MacBook) | Rows | S0 | S1 native | S1 wasm in Node |
|---|---|---|---|---|
| Demo, 136 indexers | 499,514 | 0.32 to 0.40 s, 134 MB | 0.15 to 0.16 s, 132 to 134 MB | 0.48 s, 81 MiB of wasm memory |
| Delegators, 307,788 live | 499,514 | 0.49 s, 244 MB | 0.32 to 0.34 s, 231 to 232 MB | 0.68 s, 149 MiB |
| GRT holders, 119,270 live | 11,937,686 | 7.5 to 7.6 s, 268 MB | 4.2 s, 294 to 299 MB | 7.4 s, 138 MiB |
| GRT holders, 20,000-row chunks | 11,937,686 | 8.5 s, 180 MB | 4.9 s, 194 MB | same hash |

Folding Arrow directly took the holders replay from 1.57 to 2.8 million rows a second; the fold itself,
decimal parsing included, is 3.0 s of the 4.2. The holders' peak rose by about 27 MB, and that is
Burrmill's newer rev, not the fold: the same driver on S0's Burrmill `0777d76` peaked at 267 and 278 MB.

**Bytes per live key.** Read off RSS as S0 read it (delegators less demo, over the 307,652 more live
keys), S1 holds about 320 bytes a key against S0's 360. Counted by the allocator
(`tools/rfc-0070-s1/src/bin/state-bytes.rs`), the state is 183 bytes a key at 307,788 keys and 165 at
3 million, against 222 and 206 for S0's form. Interning the address saves about 40 bytes; §5's 100 was
not reached because the value and the key are each held twice, once in the hash map and once in the
ordered index, and both structures run part-empty. The 1 GiB cap is therefore about 3.3 million live
keys by RSS, about 6.5 million by the allocator's count.

Decisions the RFC did not settle:

1. **No engine in the crate.** It takes Arrow 59 record batches, the Arrow Burrmill now uses, so §13's
   JSON detour is gone. Integer and decimal columns and all three string layouts are read; any other
   type is refused, naming the column.
2. **Address keys.** A key spelled `0x` and 40 lower-case hex digits is held as 20 bytes; any other key
   stays text. Both order and print exactly as their text, so the hash cannot tell.
3. **The validator's reach.** Beyond §4.2's list it refuses `GROUP BY`, `HAVING`, `QUALIFY`, `WINDOW`,
   `WITH`, subqueries, `UNION` without `ALL`, `INTERSECT` and `EXCEPT`, `TOP` and `FETCH`, derived
   tables, table functions and `VALUES`, `*`, unnamed expressions, and any function outside a
   twelve-name scalar allowlist. A source must be a table in `schema.json`, so an authored view is
   refused. The arms of a `UNION ALL` must name their columns in one order, because the engine pairs
   them by position. An optional `block_timestamp` column is accepted, as §13 finding 5 asked.
4. **`last`.** A key given two values in one log is refused, since nothing orders them. A value of zero
   leaves the state, and the total is the sum of the current values.
5. **The spec** takes `frames` or `step_blocks`, exactly one. Blocks past the last whole frame are not
   played, as in S0. The manifest gains `first_block`, where the pass began.
6. **NID and data identity are in the manifest**, computed by `nuthatch-identity` from the nest's
   authored inputs, found by the binary's own walk, with the registry hash `schema.json` records rather
   than one regenerated from the config and ABIs, which needs the main crate. On `grt-supply-nest` both
   equal what `nuthatch nest nid` (5.8.0) prints. On the allocations copy they do not, and the
   difference is exactly that hash: 5.8.0 regenerates `9696fb0f...` where the 5.0.1 that sealed the
   copy recorded `916324e2...`, and the binary's own bundle manifest with that one value swapped hashes
   to S1's NID. The manifest therefore names the identity of the decode that sealed the segments read.
   A tab holding only a mirror's manifest and segments has no authored inputs and writes both as null;
   the registry hash and the segment list stay.
7. **The provisional refusal is the enforcing surface.** The plan also filters provisional segments
   out, but once the range check passes no provisional segment starts at or below the last frame, so
   the filter cannot fire; the refusal is what the mutation checks.

Not verified in S1: a run on the ThinkPad; a headless browser rather than Node; the in-tab replay and
its memory (S2); a read lease (S3); USDC. The driver is not in CI, as S0's harness is not.

## 15. S2, built 2026-10-22

The page is `tools/rfc-0070-s2/web/`: `timelapse.html` and its script, a worker that runs the replay,
`player.html`, the template every export is filled from, and four specs pinned to mirrored datasets. It
replays a spec over the public mirror in the tab, loads a recording (`frames.jsonl` and `manifest.json`,
refused unless the frames hash to the manifest), plays it, and exports one `.html`. The preview on the
page is the export itself in a frame, so what plays there is the file that downloads. `export.mjs` fills
the same template from a native recording. `gate.mjs` runs the gates in headless Chrome over the
DevTools protocol.

The tab's engine is S1's driver built for the web by `build.sh` (34.5 MB, 5.4 MB at brotli, the size of
the playground's module), in a worker, reading segments from the mirror's Worker, each checked against
the SHA-256 its key names before Burrmill sees it. Nothing in the binary changed.

| Gate | Result |
|---|---|
| The export, opened with networking off, plays every frame and makes zero requests | **Pass** for all four in-tab exports: Chrome's headless shell 1243 with every host unresolvable and the page offline, played at 120 fps; every frame index was drawn (1,200, 1,200, 600 and 1,200) and the protocol saw no request but the file itself. Three mutants of the demo's export each fail it: an added image (one request), one altered address (the page's hash differs), a player that skips one frame (1,199 drawn) |
| Its embedded frames hash equals the native harness's for the same spec | **Pass**, four of four. The native driver over a local copy of the same mirrored segments, and the tab over the mirror, give `fce5af90...a4174`, `43bbe523...5f101`, `ea7ec80e...883cf21` and `26235ec1...ba906b`, S0's and S1's hashes. A native recording loaded through the page's file input exports to the same hash; one with a single altered field is refused, naming both hashes |
| The in-tab replay's peak memory, per nest | Recorded below. Every spec fitted; no nest was found that the tab could not hold |

| In the tab (MacBook, headless Chrome) | Rows | Fetched and checked | Time | wasm memory | Largest process | Whole browser |
|---|---|---|---|---|---|---|
| Demo, 136 indexers | 499,514 | 410 segments, 56.5 MiB | 9.7 s | 76.3 MiB | 520 MiB | 719 MiB |
| Delegators, 307,788 live | 499,514 | 410 segments, 56.5 MiB | 9.5 s | 144.6 MiB | 586 MiB | 786 MiB |
| Indexer reward cut (`last`) | 1,001 | 1 segment, 0.1 MiB | 0.8 s | 18.7 MiB | 303 MiB | 503 MiB |
| GRT holders, 119,270 live | 11,937,686 | 2,945 segments, 351.2 MiB | 66.2 s | 136.9 MiB | 641 MiB | 845 MiB |

Peak RSS was sampled by `ps` every 250 ms over Chrome and every process under it; the same page idle
measured 102 MiB in its largest process and 288 MiB in all. The reward-cut row is the engine's fixed
cost, about 200 MiB over idle with 18.7 MiB of wasm memory, which is the compiled module. The fold's
own state is the wasm memory, and it tracks S1's Node figures (81, 149 and 138 MiB). Segment bytes held
by the page at any one time peaked at 5.6 MiB, because the worker drops a table's segments two behind
the one the engine is reading. The holders' 66 s is the network: natively, over the same segments on
disk, the replay took 3.7 s.

Decisions the RFC did not settle:

1. **Where it lives.** Here, beside S0's and S1's tools, because S2 was to change only this tree. The
   directory is self-contained and meant to be copied as it is into `burrmill-wasm`'s `web/play/`,
   where `/play/:path+` already proxies, so the site needs no Astro copy and no change. The tab runs a
   second wasm module beside the playground's, since `burrmill-wasm` does not carry `nuthatch-timelapse`.
2. **The mirror's copy and the Helsinki copies agree.** The mirrored allocations nest has every legacy
   staking segment final to block 408,818,200, where S0's copy had its last rows provisional (§13 finding
   4), yet over S0's ranges it gives S0's hashes. A spec in the page is pinned to a dataset, so a
   replay reads the same final segments whenever it runs.
3. **No identity from a mirror.** The tab, and the native driver over a directory without
   `nuthatch.toml`, write the NID and data identity as null, as §14 decision 6 said a tab would. The
   registry hash and the segment list stay.
4. **The frames go in as one JSON string**, with `<` escaped, so the player hashes the bytes of
   `frames.jsonl` and nothing a template could rewrite. The manifest goes in whole, segment list
   included, as provenance.
5. **How a recording reads is the player's.** Decimals, a unit and what the keys are counted as are
   embedded beside the frames and never hashed. A recording of one of the page's own specs takes that
   spec's; anything else starts as plain integers.
6. **Two accents, not a hue per key.** S0's rendering gave each key its own hue; the house palette has
   two, so a key gets one of the two at one of four strengths. Rows still move, so a key can be followed.
   The export uses the system's fonts, since a font file would be a request.
7. **Nothing is cached between replays.** No origin-private store as the playground keeps; a second
   replay fetches again, and only the browser's HTTP cache may spare it.

Not verified in S2: any browser but Chromium (no Firefox, no Safari, no phone); the page behind the live
site's `/play` proxy, since nothing was deployed; a nest the tab cannot fit, since the largest
mirrored replay available fitted; the ThinkPad; USDC; and §12 question 6, a recorded WebM.
