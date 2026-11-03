# RFC-0080: Partial sync - live local copies of part of a nest, and federation with the rest

- Status: **Accepted (2026-10-30, Chief): "ok letsgoooo with the wave of rfcs".** Built in the order 0078, 0079, 0080, each slice on its own gate; an S0 that fails stops its RFC. Chief, the same day, on GraphOps's
  five suggestions: "lets do ALL that, 1-5 !!!!! but HA first". Third in build order, after RFC-0078
  and RFC-0079. **S0 measured 2026-11-01 (§11): the gate is not met, 11.4% of the follower's apply
  on the fixture and 30.6% with every row in a shaped table, so by that rule it stops.** Copies were
  exact through the reorg suite and 10,000 live streams. **Reopened by Chief the same day; the second
  S0 (§12) narrows it: the gate is met for address-shaped shapes over the busy table (uniform 6.1% and
  5.1% on two boxes), not under fan-out or with every row shaped (hot:100 12.9%, all-tables 12.8 to
  14.6%).** What remains is per-frame work on the apply. **S1 kept as built, 2026-11-03 (Chief:
  "whatever you think is best", on #2620):** it reached main with #2596 before the call was recorded.
  It stays in the default binary, off unless a mount writes `[mounts.sync]`, and is not for a runtime
  shared between tenants until #2600 and #2612 are fixed. §13's "the gate is met" rests on the
  denominator #2585 disputes; that question stays open there.
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

## 11. S0 measured (2026-11-01)

On a MacBook (Apple M5 Pro, 18 cores, 48 GiB), release build off `main` at `4b2804b8`, with other
sessions' builds on the same machine (load average 5 to 41 across the runs). Nothing here has run
on a CI runner.

**Verdict: stop.** The gate is not met. The filter cost 11.4% of the follower's apply on the fixture
the slice names, and 30.6% when every row lands in a shaped table. The status line's rule is that a
failed S0 stops its RFC. Correctness and memory both held, and the overage comes from two costs the
measurement can name, so a second S0 with those two changed is the case for reopening it. That is
Chief's call.

### What was built

The spike's code is not on `main`: S0 stopped the RFC, and a stopped spike does not belong in the
default binary's follower path. It is kept on the branch `pete/rfc-0080-s0` (#2570, closed unmerged)
for a second S0, and the paths below are on that branch.

`src/shape_s0.rs`, read only when `NUTHATCH_SHAPES_S0` names a shapes file on a follower
(`nuthatch serve --follow`). Unset, nothing is loaded, no listener is bound, and `apply_delta`
takes no lock; what it adds is four clock reads. The CLI is untouched, so every `--help` is
unchanged.

- **The filter** (§5.4, value terms only). Each shape is indexed under the values of its first term.
  For each row in a delta's `Changes`, removed then added, the row is read once: a row that opens
  with an unescaped table name no shape names is refused before a parser starts, and otherwise the
  fields no shape names are skipped as they are scanned. Its columns are probed, and the shapes found
  are checked against their remaining terms. Each matching row is encoded once and its frame pushed
  to every matching shape's queue.
- **The stream** (§5.2's shape, not its protocol). On `NUTHATCH_SHAPES_S0_LISTEN`,
  `GET /shape/<nid>/<id>` answers NDJSON: one line with the shape's hot rows, the generation and the
  watermark, then `{"g", "put"|"del": <stored row>}` frames. A reader 4,096 frames behind is closed
  with `{"refetch": true}`. No offsets, no resume, no ranges.
- **The hook.** The hub is locked across `MemStore::apply` and the filter, so an attach reads the copy
  at exactly the generation its next frame follows. Each delta records its filter time, the
  `MemStore::apply` time and the follower's whole apply as `feed_metrics` times it, with the filter
  and any wait for the hub taken out.
- **The client**, `tools/rfc-0080-s0/client.mjs`: Node, one stream a shape, a count per stored row (a
  `del` is -1), so a delta's removals and puts apply in either order. `run.sh` drives a run and
  `verify.mjs` checks every copy against the writer's own `/sql`, with the terms evaluated again in
  JavaScript, independently of the index.

### The fixture

The point-read gate's chain: `fixture-rpc multinest`, one contract with the ten-event Uniswap V4
`PoolManager` ABI at 200 logs a block, served locally. A writer backfills 200 blocks, then follows
300 more one block a poll, so each delta at tip is one block of 200 rows across ten tables. The
follower attaches after the backfill; costs count only deltas after every client has its initial
answer. Ten thousand shapes, all attached and read live by one Node process.

Shape sets (`shapes.mjs`, seeded):

- **uniform**: over `pool_manager__swap`, one to four of the fixture's 4,096 senders each; one in
  five adds a second term on `id`.
- **skew**: as uniform, but half the shapes add one of eight hot senders. The fixture draws those
  senders as rarely as any other, so it is a tail.
- **hot:k**: as uniform, but k shapes are the whole pool's swaps, so every swap row matches k shapes.
  This is the popular-token case of §8. Run over 150 blocks, not 300.
- **all-tables**: spread over all ten tables, each naming `tx_hash`es of its table. Every row is in
  a shaped table and is read in full.

### Filter against apply

| Run | Frames a delta | Filter p50 | Apply p50 | Filter / apply, all deltas | p50 | p99 | Copies wrong |
|---|---|---|---|---|---|---|---|
| Hook on, no shapes | 0 | 8 µs | 1,030 µs | 0.7% | 0.8% | 1.5% | n/a |
| uniform | 102 | 179 µs | 1,595 µs | **11.4%** | 11.7% | 16.9% | 0 of 10,000 |
| skew | 105 | 108 µs | 1,087 µs | 11.3% | 10.9% | 27.3% | 0 of 10,000 |
| hot:100 | 2,099 | 156 µs | 1,032 µs | 13.2% | 14.7% | 22.0% | 0 of 10,000 |
| hot:1000 | 20,091 | 859 µs | 6,987 µs | 14.3% | 17.0% | 29.1% | 0 of 10,000 |
| all-tables | 83 | 487 µs | 1,574 µs | **30.6%** | 31.5% | 40.9% | 0 of 10,000 |

Apply is the denominator the gate names: the follower's whole apply of the delta, its copy, its
derived state and its publish. Against `MemStore::apply` alone (105 to 157 µs p50) the filter costs
more than the apply in every loaded run: 1.1 times in uniform, 3.1 times in all-tables. Apply moved
between runs with the machine's load (1,030 to 1,595 µs p50 for the same shape of delta), and in hot:1000 the
delivery of two million frames on the same runtime slowed it to 7 ms, which flatters that ratio.

**Where the filter's time goes.** `shape_s0::tests::s0_route_cost` (ignored; `S0_SHAPES` and
`S0_ROWS`) times `route` alone over the same shapes and real stored rows, 20 swap rows and 180 others
a delta, every shape attached and no task waiting:

| Shapes | Frames a delta | `route` p50 |
|---|---|---|
| uniform | 108 | 36 µs |
| skew | 92 | 40 µs |
| all-tables | 230 | 231 µs |

Two costs, then. **Waking the readers** is most of the loaded figure: uniform's 179 µs is 36 µs of
filtering, and the rest is mostly a `notify_one` per stream touched, each unparking a task, on a
loaded machine. It scales with the
number of distinct readers a delta touches, not with frames: hot:100 pushed twenty times uniform's
frames for less time. **Reading a row** costs about 1.1 µs for one of these rows of about 600 bytes
in a shaped table, so a nest whose shapes cover its busy table pays about a fifth of apply before a
single frame.
That answers §5.4's [VERIFY]: the parse dominates as soon as the shaped table is the busy one.

Two versions came before these figures, and both were worse. One parsed each row into a map of
every field: 29% with 100 shapes. Another woke every stream on every delta through one shared
`Notify`: 2.0 ms a delta at 10,000 shapes, with the apply slowed to 11 ms by the stampede.

### Memory

The follower's RSS, median while following, against the 2,048 MiB per-cursor budget:

| | RSS |
|---|---|
| Hook on, no shapes | 74 MiB |
| 10,000 shapes indexed, before any client | 67 to 70 MiB, against 51 MiB without shapes: about 17 MiB of index |
| uniform, skew and all-tables, 10,000 live streams | 418 to 423 MiB |
| hot:100 | 464 MiB |
| hot:1000 | 498 MiB |

About 35 KiB a live stream, most of it the connection rather than the shape. 10,000 streams fit in a
fifth of the budget; about 50,000 would take the rest.

### Copies

Every copy in every loaded run equalled the writer's canonical rows: 0 of 10,000 wrong in each, up to
5,042,992 rows held across the copies (hot:1000), each exactly once, with no refetch.

The first uniform run found 162 copies each holding one row too many: hot rows at block 19,798 under a
`sealed_through` of 19,899. `/sql` withholds a hot row at or under the watermark once its table has a
segment, and the follower's copy still carries such rows. The initial answer now applies the same
rule. Any server half must, and S1's protocol must say so.

**The randomised reorg suite**, `shape_s0::tests::every_client_copy_is_its_canonical_rows_through_random_reorgs`:
a proptest of 256 cases, each up to 40 steps of windows, reorgs of depth 1 to 5 clamped at the
watermark, seals that prune hot, and clients detaching and reattaching. It drives a real redb `Store`
through its feed into a `MemStore`, routes each delta's `Changes`, and compares all seven shapes'
copies against the writer's hot and sealed rows after every step. It passes. Two mutations turn it
red: routing puts and not retractions, and skipping the check of a shape's remaining terms. The fixture
chain never reorgs, so the loaded runs test fan-out and delivery, and the suite tests retraction.

### What S0 did not do

Retraction by key: frames carry the stored row, and the client keys its copy by it. Offsets, resume
and the 409. Ranges. Sealed answers: the initial answer is hot rows only, and a copy is checked above
the watermark it attached at. Config, limits, the bearer token and `pin`. The client in the engine:
Node stands in for it.

Three costs of the spike's own plumbing are not in the gated figure but would bind S1. An attach scans
the whole hot copy under the hub's lock: 3.4 ms each, so 10,000 attached in 33 to 54 s, with deltas
held behind them. Ten thousand connects at once overflowed macOS's listen backlog of 128: up to 6,411
connects were retried at a ramp of 64. And the frames, written to 10,000 sockets on the follower's
runtime, slow its apply.

### What reopening would take

A second S0 against this gate on these shape sets, with two changes:

1. **Probe the delta's columns, not its rows.** Feed v2 already sends a delta's puts as one Arrow Utf8
   column per field (`src/feed_wire.rs`), and the follower rebuilds each row from them. Probing the
   shapeable columns there costs a hash lookup per row, not a parse. Retractions still need the
   removed rows read, and those are rare.
2. **Wake readers off the apply path.** The apply appends to queues and signals one delivery task,
   which wakes the streams. The wake-ups are then delivery's cost, budgeted and measured as delivery,
   not hidden.

Then, before S1's protocol: initial answers taken from a pinned version outside the lock, retraction
by key, the watermark rule above, several shapes on one connection (§5.6's
`max_shapes_per_connection`) so 35 KiB is not paid per shape, a connect rate limit beside the shape
limits, and `[mounts.sync]` in place of the variable.

## 12. Second S0 measured (2026-11-01)

Chief reopened S0 the same day: "lets revisit the partial sync, cant be that bad?". The second S0
made exactly the two changes §11 named, against the same gate, fixture and shape sets.

**Verdict: narrow.** The gate is met for the shapes the slice names, address-shaped terms over the
busy table: uniform at 6.1% of apply on the Mac and 5.1% on the ThinkPad, against 12.7% and 11.0%
for the first S0's filter in the same sessions, and skew at 6.2% and 5.2%. It is not met when every
row is in a shaped table (all-tables, 14.6% and 12.8%) or under fan-out (hot:100 12.9% on both,
hot:1000 24.1% and 65.8%). Reading rows is no longer the cost: what remains is per-frame work still
on the apply path, encoding each matched row's frame and pushing it to every matching queue, and
this S0 was not allowed to move it. Copies were exact in every run, the reorg suite passes on the
column path, memory is about a quarter of the budget, and a stalled client does not slow the
follower. Whether S1 proceeds on the narrowed claim, or a third S0 moves the per-frame work first,
is Chief's call.

### What changed

The code is on `pete/rfc-0080-s0b` (#2576, a draft, not for merging), built on the first spike.

1. **The filter probes the delta's columns.** A feed v2 follower keeps a decoded delta's Arrow
   batches on the `Delta`, with each row's place among the puts, when shapes are on.
   `Index::matching_columns` passes over a table no shape names a batch at a time, and in a shaped
   table probes each probe column's values, as the JSON text the column carries, in one hash lookup
   a row, checking a shape's other terms against the same row's columns. Escaped values are
   unescaped first. Retractions, rows a batch sent whole and batches sent as rows are read as JSON,
   as before. The probe is used only when every put changed a row, so `Changes::added` lines up
   with the puts; otherwise the delta is read as JSON. A differential test checks the probe finds
   exactly the `(put, shape)` pairs the JSON read does.
2. **Readers are woken by a delivery task.** Each shape's queue has its own lock and wake-up. The
   apply pushes a frame and, the first time a delta touches a stream, owes it one wake-up; after the
   delta it hands the owed ids to a delivery task and signals once. Delivery wakes the streams. The
   apply never waits on delivery or on a reader, and a reader no longer takes the hub's lock.

Both filters stay in the binary, selected by `NUTHATCH_SHAPES_S0_FILTER=json|columns` and
`NUTHATCH_SHAPES_S0_DELIVERY=inline|task`, so the A/B runs one binary. Unset, nothing is loaded and
no column is kept, as in §11. Full CI, with the RAM, point-read and tip-lag gates, is green on the
branch.

### The boxes

Two, each running the old and the new filter interleaved, shape set by shape set, in one session
(`tools/rfc-0080-s0/ab.sh`), the same release build on both:

- **Mac**: Apple M5 Pro, 18 cores, 48 GiB. Load average 2 to 4 for most runs, 9.5 at the start of
  the first and 14.8 at the start of one stall control, with other sessions' builds on the box.
- **ThinkPad**: Intel i9-13980HX, 32 threads, 62 GiB, Debian 13. Not idle: its own services hold a
  steady 3 to 4 cores. The runs went at `nice 10` and the load average stayed 2.5 to 8.6.

### Filter against apply

Filter as a share of the follower's apply, summed over every delta after the clients attached, with
the per-delta p50 and p99. §11's figure is the first S0's own run.

| Run | §11 | Old, Mac | New, Mac | Old, ThinkPad | New, ThinkPad |
|---|---|---|---|---|---|
| Hook on, no shapes | 0.7% | 0.7% (0.8, 1.1) | 0.3% (0.2, 0.7) | 0.7% (0.7, 1.0) | 0.2% (0.2, 0.4) |
| uniform | 11.4% | 12.7% (12.8, 22.2) | **6.1%** (6.1, 9.8) | 11.0% (11.0, 17.2) | **5.1%** (5.1, 7.9) |
| skew | 11.3% | 12.8% (12.1, 42.2) | **6.2%** (5.8, 14.8) | 11.4% (10.6, 35.8) | **5.2%** (4.9, 15.6) |
| hot:100 | 13.2% | 30.4% (33.1, 61.1) | 12.9% (12.5, 17.4) | 20.2% (20.1, 59.6) | 12.9% (12.9, 17.0) |
| hot:1000 | 14.3% | 60.4% (77.2, 179.8) | 24.1% (30.0, 42.7) | 93.1% (102.1, 135.4) | 65.8% (64.1, 91.2) |
| all-tables | 30.6% | 38.1% (39.2, 57.7) | 14.6% (15.4, 22.0) | 28.6% (29.1, 47.2) | 12.8% (12.8, 19.5) |

Frames a delta: 102 uniform, 104 to 106 skew, 2,100 hot:100, 20,091 hot:1000, 83 all-tables, the
same for both filters. Apply p50 was 0.8 to 1.5 ms in the 10,000-shape runs, except hot:1000 with the
new filter: 2.6 ms on the Mac and 2.0 ms on the ThinkPad, against 1.7 and 1.5 ms with the old. The
lowest, 0.8 ms, was the new all-tables run on the Mac, which raises its ratio.
Skew's p99 is over the gate on both boxes; its sum and p50 are not.

The old filter's hot rows are worse than §11's. §11 said its 7 ms apply in hot:1000 flattered the
ratio, and on these quieter runs it was flattered: the same filter at a 1.7 ms apply is 60%, not
14%. So the first S0's hot figures understated the problem, and the second S0's improvement there is
against the higher number.

**Where the new filter's time goes.** `s0_route_cost` (ignored) now times both filters on the same
decoded deltas, then the column probe alone and the encoding of its matched rows' frames alone, on
the Mac:

| Shapes | Frames a delta | Old route p50 | New route p50 | Probe alone | Encoding frames |
|---|---|---|---|---|---|
| uniform | 101 | 37 µs | 15 µs | 5 µs | 11 µs |
| all-tables | 246 | 224 µs | 108 µs | 29 µs | 92 µs |

The probe is about 0.15 µs a row in a shaped table, against §11's 1.1 µs to parse one, so reading
the delta is solved. Encoding a frame, the stored row escaped into a JSON string, is about 0.4 to 0.5 µs a
matched row, and pushing a frame to a queue about 40 ns: hot:1000's 20,091 pushes are most of its
818 µs. Both are fan-out, and both still run on the apply.

### Delivery, and a stalled client

Delivery's lag, from the apply handing over a delta's streams to the last of them woken: p50 56 to
218 µs and p99 138 to 552 µs on the Mac, p50 119 to 214 µs and p99 224 to 393 µs on the ThinkPad,
highest in hot:1000. It runs beside the apply and is not counted in the gated figure.

A stalled client was measured as a pair over 400 blocks: hot:100 with every client reading, then the
same with 50 of the 100 whole-pool streams reading their initial answer and never reading again.

| | Filter / apply | Apply p50 | Apply p99 | Follower at the end | Stalled streams closed |
|---|---|---|---|---|---|
| Mac, control | 11.6% | 1,390 µs | 4,339 µs | generation 452, the writer's tip | n/a |
| Mac, 50 stalled | 12.4% | 662 µs | 3,988 µs | generation 452, the writer's tip | 17 |
| ThinkPad, control | 12.9% | 1,446 µs | 1,721 µs | generation 452, the writer's tip | n/a |
| ThinkPad, 50 stalled | 13.3% | 1,460 µs | 2,005 µs | generation 452, the writer's tip | 0 |

The follower kept to the writer's tip with stalled clients, at the same apply cost; the other 9,950
copies were exact. A stalled stream's frames first fill its socket buffers: on the Mac 17 of the 50
passed the 4,096-frame bound and were closed for refetch, and on the ThinkPad, whose localhost
buffers are larger, none reached it in 400 blocks. `a_stalled_reader_never_delays_routing` checks the
bound itself: routing past it closes the reader and owes it one wake-up, and nothing waits.

### Copies and the reorg suite

Every copy in every run equalled the writer's canonical rows, under §11's watermark rule: 0 wrong of
10,000 in each of the 20 A/B runs and the two stall controls, and 0 of 9,950 in each of the four
runs with stalled clients. The randomised reorg suite now
runs twice, as in §11 and again with deltas sent through feed v2's encoder and decoder, probed by
columns and woken by delivery, with blocks of up to 13 rows so a table reaches a batch of columns
(`every_client_copy_is_its_canonical_rows_through_random_reorgs_by_columns`, 256 cases). Both pass.
Two mutations turn the column suite red: skipping the check of a shape's other terms, and routing no
retractions; the first also fails the differential test.

### Memory

The follower's RSS median while following was 430 to 523 MiB on the Mac and 532 to 547 MiB on the
ThinkPad with 10,000 live streams, against the 2,048 MiB per-cursor budget, as in §11. Keeping a
delta's Arrow batches until it is applied is not charged to the dataset's budget in the spike; S1
must charge it.

### What S1 would need

If Chief takes the narrowed claim, S1 adds to §11's list:

1. **Fan-out off the apply.** The apply records each delta's matched `(row, shape)` pairs once, by
   reference to the decoded delta, and delivery encodes each row's frame once and appends it to
   the matching queues. The apply's cost is then per matched row, not per frame, which is the
   change hot:k and all-tables need. A third S0 can measure it against this gate before S1, and is
   the cheaper way to know.
2. **The kept batches charged** to the dataset's budget beside the decoded rows.
3. **A bound a stalled client reaches**: by bytes as well as frames, since socket buffers hold
   thousands of frames before the queue sees any.

## 13. S0 on this branch (2026-11-01): the gate is met

§11 and §12 record the spike on `pete/rfc-0080-s0` and `pete/rfc-0080-s0b`, which is not on `main`.
This section and those after it are the filter and hub built on this branch, in `src/shape.rs` and
`src/shape_feed.rs`, measured by a different benchmark (the filter alone, against decode and apply
over `MemStore`, fed v2 bodies), so its figures are not comparable with §11's and §12's.

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

## 14. S1 in progress (2026-11-01): the follower's half of the filter

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

## 15. Reading the endpoint's ground before building it (2026-11-01)

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

## 16. The route, first cut (2026-11-01): `GET /<route>/sync`

**Built.** `Follower::serve_sync`, answered in the follower's own layer for a route ahead of the nest's
router. JSON lines, `application/x-ndjson`, `Cache-Control: no-store`:

```
{"offset":[epoch,generation],"scope":"hot"}
{"ops":[{"put":{"key":"..","row":{..}}}, ..]}        # the copy's rows the shape selects, 500 a line
{"up_to_date":true}
{"offset":[e,g],"ops":[{"put":..},{"retract":{"key":".."}}]}   # with live=1, one per delta, until the stream ends
```

The request is `?s=<shape JSON>`, with `live=1` for the stream. It refuses with a JSON `{"error","why"}`:
404 `sync_off` where the mount has no `[mounts.sync]`; 400 for a shape that does not parse or a missing
`s`; 403 `refused` with `MountSync::admit`'s reason; 503 `catching_up` before the copy's snapshot; and
409 `must_refetch` for any `offset`, which is always true, since a follower keeps no history to resume
from. A live stream ends when its queue (256 frames) fills or the copy is replaced by a fresh snapshot,
and the client asks again. The shape is subscribed before the rows are read, so a delta between the two
is in both: a put read twice, or a retraction of a row never held, either of which a copy takes in its
stride, and replaying a key's operations from any state at or after the offset reaches the same end.

`sync_answers_a_shape_from_the_copy_and_streams_what_changes_it` drives it through the route's own
layer: the refusals, an answer without `live` (header, only the selected rows, end), a live stream that
gets a put for a new row, a retraction for a row that left the shape and nothing for a row outside it,
and ends when a fresh snapshot replaces the copy.

**What this is not, and what is wrong with it as it stands.**

1. ~~**Hot rows only.**~~ Superseded by §17: the first answer reads sealed rows too, and `scope` is
   `full`. As first cut, `scope` said `hot`. A nest with sealed history gets an answer missing every sealed
   row that matches, which is §5.3's whole sealed half. A client must not treat this as the shape's
   rows until that is built.
2. ~~No limits on the answer.~~ The first answer now runs under `/sql`'s guards, read from the route's
   own `AppState`: a permit on the SQL gate while the copy is scanned (`503` busy, `429` for a tenant at
   its share, as `/sql` refuses), the route's `max_rows` and `max_result_bytes` (`413
   shape_too_large`, the scan stopping its collecting past either), its timeout (`504`), and room in the
   response-bytes budget, taken before the answer is sent and kept until its last line has been.
   Tested: the row cap and the busy gate. Not tested: the timeout, and a response-bytes budget that is
   full. Not covered: the live stream, whose lines are not counted against the bytes budget, and how many
   streams a follower holds open, which is bounded by nothing but memory and the 256-frame queue each.
3. **No token check.** §5.6 says FR12's bearer token applies. This route does not check one, and I have
   not checked what a follower's other read routes do about it.
4. **`max_shapes_per_connection` is a stub.** A request holds one shape, so `admit` is called with none
   held. The limit means something once a connection can hold several, which needs a protocol §5.2 does
   not have (one request per shape).
5. **JSON lines, not Arrow** (§5.2 asks for Arrow IPC for the first answer), and no resize (§5.5).
6. **Offset is the copy's generation**, not a block, as before.

Item 1 decides whether this can be turned on for anyone; the live streams' own limits and the rest are S1's remaining protocol.

## 17. The sealed half: what reading `/sql` showed (2026-11-01)

§16 item 1 is the sealed rows. §5.3 says to answer them as "the shape as a query over the sealed
segments", and S2's gate is "sealed answers byte-equal to the `/sql` of the same predicate". Reading how
`/sql` is built shows that is not one decision but three, and the first answer cannot be finished
without them.

1. **Two representations of a row.** A hot row is stored JSON, and `/sql` ingests it as such under the
   table's registry schema (`charged_hot_rows`, then `query_hot_cold`). A sealed row is Parquet, typed.
   Both reach a client of `/sql` as the schema's columns, which can include columns no stored row has
   (the RFC-0061 `value_dec` and `value_overflow` pair is derived in SQL). The live frames of §16 carry
   the *stored* row. A client that takes its first answer from the sealed query and its updates from the
   frames would hold rows of two shapes under one key. Either the frames are converted to the SQL
   projection, or the sealed rows are written back out as stored JSON, which is not byte-exact from
   Parquet. The first is the only one that can meet S2's gate, and costs a conversion on the live path.
2. **Keys.** A put and a retraction are by key (`Store::entity_key(block, log)` for a log table), and
   the sealed query's rows carry none. A table that is not log-shaped may have no key a client can
   reconstruct from its columns. The answer has to carry a key per row, so the query selects whatever
   the key is made of, and the rule from a row to its key becomes part of the protocol.
3. **Where the two meet.** The first answer would be `/sql`'s own, sealed and hot together, read after
   the subscription as §16's convergence argument allows (a row sealed in between is in the sealed side
   and its hot copy was never retracted, since a prune is not a retraction). That means running the
   shape as a statement through `run_sql_query_at`, with its own memo and guards, rather than scanning
   the copy, which is the better reuse and also what makes the §16 guards redundant for the first answer.

Nothing here is built. The choice in (1) decides the rest, and is Chief's: **the shape's wire row is the
`/sql` projection, with a key column**, or it is the stored row and sealed history is out of a shape's
reach (a shape then covers the hot window only, which is Electric-like for a live tail and not for
"huge nests"). I would take the first.

## 18. Option A, built (2026-11-01): a shape's rows are `/sql`'s, keyed

Chief took A in §17: the wire row is the `/sql` projection, and each op carries a key.

**What changed.**

- **The first answer is `/sql`'s own.** `Shape::select_sql` writes the shape as a statement (terms as
  `CAST(col AS VARCHAR) IN (..)`, ranges as numeric bounds, `ORDER BY block_number, log_index`, every
  name and value quoted). `serve::sync_query` runs it through `run_sql_query_at`, so it has the gate,
  caps, timeout, memo and response-bytes budget of `/sql` and reads the sealed segments with the hot
  copy as `/sql` does, and `serve::within_pin` gives it the pinned copy a follower's catalogue is only
  read from. §16's own scan, caps and timeout are gone, replaced by those. The header line says
  `"scope":"full"` and carries the answer's `provenance`. A statement cut at the row cap is `413
  shape_too_large`. A route whose mount is `sql = "allowlist"` still answers shapes: `[mounts.sync]` is
  the control for that, not the SQL surface.
- **A key per row** is `Store::entity_key(block_number, log_index)` of the row (`shape::row_key`). A row
  that has no such pair is `500 no_key` in the first answer, and ends a live stream.
- **Live rows are shown as `/sql` shows them.** A frame's stored rows go through `serve::project_rows`,
  the same engine over those rows alone, before they are sent, so a put has the columns of a row from the
  first answer. Retractions go by the store's key.

**Tested** (`sync_answers_a_shape_as_sql_does_and_streams_what_changes_it`, over an ERC-20 `Transfer`
nest): the first answer's rows and keys; the row cap, and the busy gate, now through `/sql`'s code; a
live put whose row **equals what `/sql` answers for that row**, which is S2's gate for the hot half; a
retraction; the route's layer; the end of the stream on a fresh snapshot. Unit tests for `row_key` and
`select_sql`, including quotes in names and values.

**Not tested, and where to look first.**

1. ~~Sealed rows.~~ Tested since: `sync_answers_sealed_rows_with_the_hot_ones` seals blocks 1 and 2
   with `seal_range`, gives the copy the catalogue in its snapshot as a follower's would be, and a shape
   by term answers the sealed rows and the hot one in chain order, a shape by a block range answers from
   sealed rows alone (so a range on a numeric column is tested against Parquet, item 4's good case),
   and a live stream gets the frame for the next delta. Not yet tested: a seal that happens while a
   stream is open (the hot rows moving to a segment under a subscriber).
2. ~~The key rule against every writer.~~ Read, and checked. The cursor stores every decoded row under
   `entity_key(row.block_number, row.log_index)` (`core/src/cursor.rs`), and `DecodedRow::to_json` writes
   both fields into the row, for logs and for rows that descend from no log (blocks, call results, which
   take the reserved `log_index` band), so the rule holds by construction;
   `the_key_of_a_decoded_rows_json_is_the_key_the_cursor_stores_it_under` checks it across that band. A
   row the store keys some other way would break it, so the live path also compares the keys of the rows
   it shows with the keys the store holds them under and ends the stream if they differ (tested with a
   row put under a key its own block and log do not give). Not checked: IPFS document rows, whose key is
   `IPFS_ROW_LOG_INDEX_BASE + slot` and whose row `log_index` I did not read, and the first answer's
   hot rows, which have no store key to compare with.
3. **Terms by two rules.** The first answer filters in the engine on `CAST(col AS VARCHAR)`; the live
   path filters stored JSON by `Shape::matches`. For addresses and integers they agree. Booleans,
   decimals and anything the engine renders differently from the stored text are untested, and a
   disagreement would put a row in one and not the other.
4. **Ranges on non-numeric columns** are compared as numbers by the engine; untested.
5. ~~Cost of the live conversion~~ Measured (`showing_a_frames_rows_costs_what_it_costs`, ignored,
   release): the engine over a batch of stored rows takes about 2.1 ms a call whatever the batch (2.5 ms
   for one row, 2.1 ms for five, 2.2 ms for fifty, 3.0 ms for five hundred), so a fixed cost of the
   session and about 1.7 µs a row beyond it. The first build paid that for every live connection and
   every frame, which at 10,000 subscribers is twenty seconds of engine a delta. It now pays it **once a
   delta per subscribed table, however many subscribers**: `Follower::show_changes` runs the delta's
   final rows through the engine and keys them, checks the keys against the store's, and hands the hub a
   map from key to shown row, so a frame holds shared text and a stream only encodes it. Two costs
   remain: it runs on the apply path, under the hub's lock, as a blocking call of 2 to 3 ms (not on a
   blocking thread, since the apply is already serial per dataset), and it runs only where a subscriber
   exists. If it cannot run (no route of the dataset has a state) or the keys do not hold, the hub is
   dropped and every stream ends, so nobody is sent rows in a form their first answer did not use.
6. Still open from §16: no token check, the live stream's own limits, the one-shape stub of
   `max_shapes_per_connection`, JSON lines and not Arrow, and an offset that is the copy's generation.
