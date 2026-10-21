# RFC-0070: Time-lapse - a contract's sealed history replayed at speed

- Status: **Proposed (2026-10-21), awaiting Chief.**
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
