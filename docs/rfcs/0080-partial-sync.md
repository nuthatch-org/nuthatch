# RFC-0080: Partial sync - live local copies of part of a nest, and federation with the rest

- Status: **Accepted (2026-10-30, Chief): "ok letsgoooo with the wave of rfcs".** Built in the order 0078, 0079, 0080, each slice on its own gate; an S0 that fails stops its RFC. Chief, the same day, on GraphOps's
  five suggestions: "lets do ALL that, 1-5 !!!!! but HA first". Third in build order, after RFC-0078
  and RFC-0079.
- Author: Pete (cargopete)
- Date: 2026-10-30
- Depends on: RFC-0079 (Arrow results, the columnar wire), RFC-0077 (the embeddable engine and its
  host interface), RFC-0076 FR17 (remote followers, which serve this), RFC-0073 (the feed, strict
  isolation, bearer tokens), RFC-0065 (the nest in a tab).
- Nature: new capability. The server half is in the default binary, opt-in per route; the client half
  is in the engine crate of RFC-0077, opt-in and off by default, as every engine feature is.

## 1. Abstract

GraphOps, 2026-10-30, pointing at Electric SQL: "allow a nuthatch wasm client to sync *partial views*
of data in realtime", with "a subscription that can seamlessly adjust how much data gets synced", and
DataFusion "to federate across local partial views and remote full views, to do infrequent joins onto
remote nuthatch, while keeping most data local". The goal in their words: "huge nests on the backend
and clients just get the data they need, filtered for the local user".

Electric's unit is a **shape**: one table and a `WHERE` clause. A client asks for a shape, receives a
snapshot of the rows that match, then every change to that set, and keeps a local copy it queries
itself. This RFC defines shapes over a nest's tables, served by followers so the writer pays nothing
for them; a client in the engine that holds shapes in its own store and queries them with its own
Burrmill; resizing a shape without a gap; and a remote table provider so one SQL statement can read
local shapes and the full remote nest together.

## 2. What already exists, and what does not

Most of the machinery is in the tree, which is why the request is reasonable:

- **Snapshot then deltas** is how the feed already works: a `snapshot` per dataset, then `delta`s
  keyed by `(epoch, generation)` (`src/feed.rs:39`, `:81`), in Arrow IPC since feed v2.
- **Reorgs as retractions.** A rollback already arrives as rows removed (`Changes.removed`,
  `src/feed.rs:850`), which is the part a general sync engine finds hardest and a chain indexer has
  for nothing.
- **Followers on other hosts** (FR17) hold a dataset's whole hot state in memory and every sealed file
  its catalogue names, verified.
- **SQL in a tab.** The engine (RFC-0077) runs Burrmill over its own store, and the tab already
  fetches content-addressed segments from a mirror and checks them by hash.
- **Remote SQL with provenance and historical reads**: `/sql` answers `as_of`, and can answer as of a
  block (`historical_block`, `src/serve.rs`).

What is missing:

1. **Filtering.** The feed sends a whole dataset to a trusted follower. Nothing sends a subset, and
   nothing evaluates a predicate against a delta, rollbacks included.
2. **An untrusted subscriber.** The feed is behind the admin token. A shape endpoint is public read
   surface and needs the limits RFC-0073 gave `/sql`.
3. **Resizing** without refetching and without a gap.
4. **A client store for shapes** and a **remote table** in the client's planner.

## 3. Goals

1. A client subscribes to a shape and holds exactly the rows of that table that match it, as of a
   generation it can name, and stays current within the follower's own visibility lag plus one
   round trip.
2. After a reorg, every client's copy equals the canonical rows that match, with no client action.
3. A shape can be widened or narrowed; the copy is correct throughout and nothing is fetched twice.
4. The writer's cost does not depend on how many clients there are. Shapes are served by followers.
5. One SQL statement in the client may read local shapes and remote tables, and its answer states
   the provenance of each side.
6. A client sees only what the operator exposed, and an operator's gateway can pin a shape to a
   value it chooses ("filtered for the local user") without nuthatch knowing who the user is.

## 4. Non-goals

- Writes from the client. Electric syncs writes through Postgres; a nest's rows come only from the
  chain. A shape is read-only.
- Arbitrary SQL as a shape. A shape's predicate is a small language (§5.1), because the follower
  evaluates it on every delta for every subscriber.
- Shapes over authored views, `/derived` entities or joins. Over a nest's stored tables only in v1;
  maintained entities (RFC-0041) are a later slice if the measurement allows (open question 2).
- Identity. nuthatch still sees a string; who a user is stays the gateway's job (§5.6).

## 5. Design

### 5.1 A shape

```json
{ "route": "uniswap-v3", "table": "swaps",
  "where": { "pool": ["0x88e6...", "0x8ad5..."], "block": { "gte": 21000000 } },
  "columns": ["block", "tx", "pool", "amount0", "amount1"] }
```

`where` is a conjunction. Each term is a column with a set of values (`IN`) or a range on an ordered
column. Columns must be declared shapeable by the operator (§5.6). No functions, no `OR` across
columns, no joins. That is enough for "this user's addresses", "these pools", "the last week", and it
compiles to two things the follower can do cheaply: a hash lookup on the value terms and a comparison
on the range.

A shape's identity is the canonical form of that object, hashed. Two clients asking for the same shape
share its sealed answers (§5.3) and its index entry (§5.4).

### 5.2 The protocol

`GET /<route>/shape?s=<shape>` on a follower, Electric's shape of protocol and a deliberately plain
one: HTTP, so a CDN and a gateway can sit in front of it.

1. **Initial**: the follower answers with an Arrow IPC stream (RFC-0079 §5.1) of the matching rows,
   and an offset `(epoch, generation)` in its schema metadata.
2. **Live**: `GET ...&offset=<e,g>&live=1` holds open and streams frames as deltas arrive: puts, and
   retractions by key, each frame with its offset. A client that reconnects with its last offset
   resumes; one whose offset the follower no longer has (an epoch change, a resnapshot) receives
   `409 must-refetch` and starts again from 1, as Electric does.

### 5.3 Sealed and hot

A shape's rows live in two places, and are served differently.

**Sealed rows never change**, so the follower answers them from Burrmill with the shape as a query over
the sealed segments, and the answer is addressed by `(shape hash, catalogue hash)`. It is cacheable
forever, by the follower and by any CDN, and a thousand clients asking for the same pool's history cost
one query. A shape whose terms prune by Parquet statistics (a block range, usually) may instead be
answered with the segment hashes and row-group ranges, for the client to fetch from the mirror itself
and verify; most address-shaped predicates will not prune, since addresses scatter across row groups,
so the query is the default and the pruned path an optimisation S2 measures.

**Hot rows change**, and are the live stream. The initial answer is the sealed part plus the follower's
hot rows that match, at one generation.

### 5.4 Filtering deltas on a follower

The follower already applies every delta to its copy. For each table with live shapes it keeps an
index from each value term's `(column, value)` to the shapes that name it. Applying a delta, it reads
each put row's shapeable columns once, looks them up, checks the remaining terms of the shapes found,
and appends the row to each matching subscriber's queue. A retraction (rollback, or a put replacing a
row) goes to every subscriber that holds the old row, which the same lookup on the old value finds.

The cost is per row, not per row per shape: one hash probe per shapeable column. Hot rows are stored
as JSON text today (`entities` values), so a row's columns are parsed once per delta, not once per
subscriber. [VERIFY: the parse cost per row against FR17's apply cost, in S0. If it dominates, the
shapeable columns are extracted once at apply time.]

Each subscriber's queue is bounded, as a follower's own is: a client that does not read is closed and
must refetch. The follower never waits on a client.

### 5.5 Resizing

A client changes its shape by asking for the new one with its current offset and the old shape's hash.
The follower answers with the rows in *new but not old*, as of that offset, then continues the live
stream for the new shape from the same offset. Rows in *old but not new* the client drops itself: it
holds the predicate and can evaluate it. Because the extra rows and the stream share one offset, there
is no window in which a change is missed or applied twice.

For the sealed part the difference is a query (`new AND NOT old`), cacheable as in §5.3.

### 5.6 What an operator exposes, and the gateway's pin

```toml
# mounts.toml, per mount
[mounts.sync]
tables = { swaps = ["pool", "sender", "block"], transfers = ["from", "to", "block"] }
max_shapes_per_connection = 16
max_values_per_term = 1000
pin = { header = "x-nuthatch-pin", column = "owner" }   # optional
```

Off unless written. Only listed tables and columns are shapeable; RFC-0073's strict isolation applies
to the sealed-part queries as to any `/sql`. The bearer token of FR12 applies.

`pin` is how "filtered for the local user" works without nuthatch knowing users. When set, the
follower ANDs `column IN (<header value>)` into every shape on that mount and refuses a request
without the header. The gateway authenticates the user and sets the header; nuthatch sees a string, as
it sees a tenant label, and the out-of-scope line on identity is unmoved. An operator who sets `pin`
must strip the header from client requests at the gateway, and the docs say so first.

### 5.7 The client

In the engine crate, behind a `sync` feature. A shape is held as a table in the engine's own store
(RFC-0077 §3.3, `host` or `bytes` mode), under the shape's hash, with its offset. The engine's
existing host interface carries it: `nh_step` yields `{"kind": "get", ...}` for the initial and
resize answers, and a new request kind, `stream`, for the live connection, whose frames the host feeds
back with `nh_resume`. The guest still performs no I/O. A reload resumes from the stored offset.

The engine's Burrmill registers each shape as a table, so the page queries it with `nh_request
/sql` exactly as it queries a nest it indexes itself. No second query API.

### 5.8 Federation

The client's Burrmill also registers **remote tables**: a DataFusion `TableProvider` whose scan sends a
statement to the remote nest's `/sql` in Arrow (RFC-0079), with the projection and the filters
DataFusion pushes down. A query joining a local shape to a remote table runs the local side locally and
sends the remote side as one pushed-down scan. When the remote side is small (a lookup of names, a
price at a block), this is the "infrequent join onto remote nuthatch".

**Consistency across the two.** The local copy is at an offset that maps to a block; the remote scan is
sent with `historical_block` set to that block, so both sides describe one moment of the chain, and the
answer's provenance carries both `as_of`s, which are equal. A remote that cannot answer that block (it
has pruned it, or has not reached it) refuses, and the client says so rather than mixing two moments.

[VERIFY: whether Burrmill exposes enough of DataFusion's planner to register a custom
`TableProvider` from the engine, given its arrow is pinned separately (RFC-0079 §2).]

## 6. Against the non-negotiables

Determinism: a shape is a projection of rows the core already derived; nothing new feeds stored state.
The ≤2 GiB budget binds the writer, which this does not touch; a follower serving shapes has its own
`--max-rss-mb`, and subscriber queues are charged to it as feed connections are. No phone-home: the
client talks only to the endpoint its page names. The default binary without `[mounts.sync]` is the
binary today, and the engine without `sync` is the engine today, which is RFC-0046 §1's test.

## 7. Order of work

| Slice | What | Gate | Size |
|---|---|---|---|
| S0 | Spike: one table, value terms only, live stream from a follower, a Node client applying it. 10,000 shapes on one follower at tip load on the Uniswap fixture. Can stop the RFC. | Per-delta filter cost under 10% of the follower's apply cost; every client's copy equals the canonical filtered rows after a randomised reorg suite | one week |
| S1 | §5.2 protocol, §5.4 in full with ranges, §5.6 config and limits, the 409 refetch | The reorg property suite per shape; a stalled client never delays the follower | two weeks |
| S2 | §5.3 sealed answers and their cache, the pruned-fetch path measured | Sealed answers byte-equal to the `/sql` of the same predicate | one week |
| S3 | §5.5 resizing | A randomised resize sequence under live load: the copy equals the canonical rows after every step | one week |
| S4 | §5.7 the client in the engine, the `stream` request kind, the browser host | A shape survives a reload; a tab demo on `/play` | two weeks |
| S5 | §5.8 federation | A federated join equals the same join run wholly on the remote, at the same block | two weeks |
| S6 | `pin`, the docs, an independent review of the public surface | The review, as FR8's was | one week |

## 8. Risks

- **Fan-out on a hot follower.** A popular token's transfers matching thousands of shapes is a write
  amplification the writer never had. Bounded per follower by its memory and by adding followers; S0
  measures where it bends.
- **The pin is only as good as the gateway.** A gateway that forwards a client's own header gives every
  client every user's data. It is configuration nuthatch cannot check, so the docs and the default
  refusal (no header, no answer) carry it.
- **Two consistency domains in one answer.** §5.8's same-block rule removes most of it; a remote that
  cannot answer that block refuses rather than approximating.
- **Scope.** This is seven slices on top of two RFCs not yet built. S0 is placed to stop it cheaply.

## 9. Alternatives

- **Run Electric.** It syncs Postgres; a nest's hot store is redb and its history is Parquet. Scaled
  mode's Postgres would work for the hot part only, and gives up reorg retractions as first-class.
- **Clients poll `/sql`.** Works today, and is what a demo would do without this. It refetches
  everything every time and cannot keep a copy current.
- **Ship whole datasets to the browser** (RFC-0077 S4, a mirrored nest in a tab). Right for a small
  nest; the request is precisely for nests too large for that.

## 10. Open questions

1. GraphOps's first demo, which decides S0's fixture and table.
2. Whether shapes over maintained entities (RFC-0041) are wanted in v1. Their rows change on every
   block that touches a group, which is a different cost from append-mostly tables.
3. Whether `pin` should allow several columns, or one is enough for "the local user".
4. Whether a writer with no followers should be allowed to serve shapes itself, for a small operator.
   Proposed: no, a local follower costs a process and keeps goal 4 true by construction.

## 11. S0 measured (2026-11-01): the gate is met

**Built, nothing served.** `src/shape.rs`: a shape (a table and value terms, hashed by its canonical
JSON), an index from each `(column, value)` to the shapes naming it, and a `Table` that applies puts,
replacements, retractions and rollbacks to each subscriber's copy. It is a spike: no endpoint, no queue,
no sealed part, and none of the S0 row's live stream or Node client. Those are S1's.

**The correctness half of the gate passes.**
`overwrites_and_rollbacks_keep_every_copy_equal_to_the_filtered_canonical_rows` runs 3,000 randomised
puts, overwrites, retractions and multi-step rollbacks against 12 shapes and checks every copy against
the filtered canonical rows after every step.

**The cost half passes.** `a_shape_filter_costs_under_a_tenth_of_the_follower_apply` (ignored; run it
in release with `--ignored --nocapture`) sends 500 blocks of 200 rows as v2 bodies through what a
follower does today, `decode_delta` then `MemStore::apply`, and times a filter over the rows each
delta added, with 10,000 single-term shapes over 2,000 pools, so every row is in five shapes. Best of
five, 100,000 rows, release, on one laptop:

| | time | against decode + apply |
|---|---|---|
| decode | 60 ms | |
| apply | 19 ms | |
| filter, each row parsed to JSON | 86 ms | 1.10 |
| filter, indexed columns read off the row's text | 6.2 ms | 0.078 |

The gate is under 0.10, so it is met, at 62 ns a row. Three earlier measurements are worth keeping:

1. Against a bare in-memory insert as the baseline the filter cost 5.3 times the apply. That baseline
   left out the frame decode and meant nothing, so the test now runs the real path.
2. With the real baseline, parsing each row cost 1.1 to 1.5, so §5.4's `[VERIFY]` is answered: the
   parse dominates, and the shapeable columns must be read without it. Reading them off the row's
   text did that and measured 0.12, a near miss.
3. The rest was not the search for the column, as first guessed. Each of the five candidates was
   being re-checked against its other terms, which for a shape with one term is a walk of a map that
   finds nothing. Keeping single-term shapes apart from the rest (`Candidates::solo`, copied in as they
   are) took 0.12 to 0.078.

**A probe in the decoder was built and dropped.** The v2 frame carries `pool` as an Arrow column
before each row is put back together as text, so the filter was also tried there (a `RowProbe` hook in
`decode_delta`, reading the column with no search). It measured 0.156 before the fix above and 0.080
after: no better than reading the text, at the price of a hook in the follower's decoder. It is not in
the tree. S1 filters `Changes.added` and `Changes.removed`, which leaves the wire format alone.

**What this does not show.**
- Fan-out is real work beyond the filter. This puts a row in five shapes and counts them; appending the
  row to five subscribers' queues is not measured. The figure holds at five shapes a row and says
  nothing about a token in five hundred.
- Only single-term shapes are in the benchmark. A shape with several terms takes the slower
  check, which the equivalence test covers for correctness and nothing times.
- One machine, one row shape (eight fields, about 300 bytes).
- Ranges (§5.1) are not built; they are S1.

S0's gate is met on both halves and S1 is next.

## 12. S1 in progress (2026-11-01): the follower's half of the filter

**Built.**

- `src/shape_feed.rs`: `ShapeHub`, the subscribers to shapes. A follower's applied delta becomes at most
  one `Frame` per subscriber, `(epoch, generation)` and a list of puts and retractions by key. Subscribers
  to the same shape share one entry and one index slot. Each queue is bounded and fed with `try_send`,
  so the follower never waits; one that is full is closed, which its receiver sees as the end of the
  stream after what fitted, and is the signal to fetch again. Entries nobody listens to are dropped from
  the index when more than half are dead.
- `Changes::log` (`src/feed.rs`), the ordered record of each stored row a put or a rollback changed. It
  is off until `MemStore::track_changes` is called, so a follower with no shape subscribers pays
  nothing, and a seal's prune is not in it, because a pruned row is still in the nest's history.
- Ranges (§5.1) on an unsigned-integer column, `gte` and `lte`, in the shape's JSON as the RFC shows it.
  A shape with no range hashes to the bytes it did before, so no identity moved. A range with any other
  key (`gt`, say) is refused. A shape with a range and no value term cannot be found by a lookup, so
  each such shape is checked against every row.

**How a key is settled.** A delta may touch a key several times. The hub takes the last touch as where
the row ends up, and every row the key had on the way as a copy that may hold it: a subscriber whose
shape selects the final row gets a put, and one whose shape selected an earlier row and not the final
one gets a retraction. A retraction of a key a copy does not hold changes nothing, so over-telling is
safe where under-telling is not.

**Tested.**

- `every_copy_equals_the_filtered_canonical_rows_after_overwrites_and_reorgs`: 1,500 randomised steps
  of new blocks, overwrites and reorgs (a rollback and a different chain in one delta), against a real
  `MemStore` applying real deltas, with twelve subscribers that join at different points, three kinds
  of shape (value terms; a window of blocks with a pool; an open-ended range), and some sharing a shape.
  After every step each copy, built from its first answer and the frames since, equals the canonical
  rows its shape selects. With retractions switched off the suite fails, so it does test them.
- `a_stalled_subscriber_is_closed_and_delays_no_one`: a queue of two, ten deltas; the other subscriber,
  on the same shape, gets all ten.
- Ranges agree between the parsed row and its text, over rows in range, out of it, absent, negative,
  fractional, hex, boolean and null.

**Not built, and so S1 is not done.**

1. ~~Nothing calls the hub.~~ Built, below.
2. The protocol of §5.2: `GET /<route>/shape`, the initial Arrow answer with its offset, the live stream,
   and the `409 must-refetch`.
3. ~~`[mounts.sync]` (§5.6)~~ Built, below.
4. ~~Measurement~~ Done for the hub at five shapes a row; see below.

**The hub is wired in.** Each dataset's copy on a follower (`Data` in `src/reader.rs`) holds a hub.
`apply_delta` applies the delta and feeds the hub under the hub's lock, and
`Follower::subscribe_shape(dataset, shape, queue)` takes the copy's `(epoch, generation)` and subscribes
under the same lock, so no delta falls between a subscriber's starting point and its frames and none is
told twice. The first subscription turns `track_changes` on for that copy; a follower nobody subscribes
to is as it was. A fresh snapshot builds a new copy and a new hub, which ends every subscriber of the old
one. `a_shape_subscriber_follows_the_deltas_after_its_offset` applies two deltas through
`Follower::apply_delta`, one the shape selects and one it does not, and then a fresh snapshot.

**`[mounts.sync]` is read and checked (§5.6), and nothing serves it yet.** `src/mount_sync.rs`: the
tables a shape may name with the columns it may constrain, `max_shapes_per_connection` (16) and
`max_values_per_term` (1000), each held to a ceiling so a typo cannot lift it past what a follower holds.
`MountSync::admit(held, shape)` is what the endpoint will call, and refuses with the part at fault: an
unlisted table or column (a range names a column as a term does), an empty term, a term over the value
bound, a connection over its shape bound. It is read per `[[mounts]]` record, checked when the table is
validated, and written back. Three choices: a table listed with no columns may be named with no terms,
which is the whole table; an empty value list is refused rather than selecting nothing; and `pin` is
not accepted, so a `mounts.toml` that asks for one fails to load instead of being ignored, until S6
builds it. Nothing documents it for operators yet, since there is nothing for them to turn on.

Still to do: the endpoint (2) and the measurement (4) above. The offset a subscriber
gets is the copy's, not yet the nest's block for it, which the endpoint's initial answer needs.

**The hub measured (`the_hub_costs_what_it_costs`, ignored, release).** Twenty thousand rows as 100 v2
bodies, through `decode_delta`, `MemStore::apply` and the hub, with 10,000 single-term shapes over 2,000
pools, so every row is in five shapes and goes to five subscribers. Best of five:

| | time | against decode + apply |
|---|---|---|
| decode | 12.2 ms | |
| apply | 3.2 ms | |
| apply while tracking changes | 3.5 ms | +0.017 |
| the hub | 19.6 ms | 1.28 |

Tracking is cheap. The hub is not, and the S0 gate did not promise it would be: the filter found the
shapes at 0.078 of the follower's work, and delivering to them costs about 1 µs a row at five
subscribers a row, 200 ns a delivery, which is more than the follower's decode and apply together. It
scales with deliveries, as §8 said it would, so a popular table matched by thousands of shapes is the
case to size against, and this figure is for five.

**Rows held once.** The guess was that every delivery copied the row's text twice, once building the
subscriber's ops and once cloning the frame for each sender on the shape. A `ShapeOp` now holds its key
and row as shared text and a `Frame` its ops as a shared slice, so a row is copied once however many
subscribers it goes to. Measured again, the hub took 11.2 ms against 19.6 before: 0.72 of decode and
apply, from 1.28, at 112 ns a delivery. Tracking is +0.012. That is a smaller cost, not a small one, and
it is not profiled: what is left is likely the per-subscriber frame (a list built, then copied into a
shared slice), the map that gathers them, and the channel send. The figure is still for five
subscribers a row.

## 13. Reading the endpoint's ground before building it (2026-11-01)

Two things in the tree decide the endpoint's shape, and §5.2 and §5.6 as written get both wrong.

1. **`/shape` is taken.** Every nest already answers `GET /shape` (RFC-0025): `{"transfers": ..,
   "compliance": ..}`, the capability shape that `nuthatch mcp` reads to decide which tools to offer
   (`src/serve.rs`, `src/mcp.rs`). §5.2's `GET /<route>/shape?s=<shape>` would be the same path with a
   different meaning, and the word is the RFC's own, so the clash is in the RFC and not in the code.
   **Proposed: `GET /<route>/sync`**, with the shape as `s` and the offset and `live` as in §5.2. It
   matches `[mounts.sync]`, and nothing answers it today. §5.2 and §5.6 are not rewritten until Chief
   agrees the name.
2. **A follower does not know a mount's `[mounts.sync]`.** It learns a route from the writer's `mount`
   line, which becomes `Spec` in `src/reader.rs`: dataset, nid, SQL surface, tenant, share, limits,
   chain, isolation, data identity. Config the follower is to enforce has to ride that line the way
   `limits` does, so `admit` can run on the follower that serves the shape (§3 goal 4 puts shapes on
   followers, not the writer). That is an addition to the feed's `mount` line. A follower that does not
   know the field ignores it and so refuses every shape, which is the safe default, and a writer that
   predates it sends none. It needs the same care as `limits` got: a test that an old follower still
   reads the line, and a bump to whatever the rolling-upgrade gate checks.

Neither is built. The order is: the field on the `mount` line and `Spec`, then the route in the
follower's layer (the follower already intercepts a route's requests before its router sees them, in
`reader.rs`'s `service`), then the initial answer. The initial answer is the part with the most left to
read: it joins the follower's hot rows with the sealed segments its catalogue names at one offset, and
`Follower::subscribe_shape`'s offset is the copy's generation, which still has to be made a block.

**The route name is agreed (Chief, 2026-11-01): `GET /<route>/sync`.** §5.2 and §5.6 above still say
`/shape` and are read as `/sync`.

**Step 1, a follower learns `[mounts.sync]`, is built.** A mount's `sync` rides the feed's `mount` line
as an optional field, skipped when absent: `overlay_mount_record` puts it on the `AppState` a mount is
served from (`AppState::sync`, so boot, mount, resume and move all carry it), `Served::of` reads it
there, the writer's `mount` line sends it, and the follower keeps it on the route's `Spec`. A changed
`sync` changes the `Spec`, so the route is rebuilt as a changed limit would. No feed version bump: the
field is additive with a default, `Line` ignores a key it does not know, and a follower that predates it
reads no `sync` and so would refuse every shape, which is the safe side. Three tests:
`a_mount_line_carries_sync_and_reads_without_it` (absent, present, and a later writer's extra key),
`a_mounts_sync_reaches_its_state_and_the_feed_line` and
`a_follower_takes_a_routes_sync_from_the_mount_line` (set, changed, cleared). Not tested: a real old
binary against a new writer, which the line test stands in for.

Next: the route in the follower's layer, answering `admit` and `subscribe_shape`, then the initial answer.
