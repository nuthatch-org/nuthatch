# RFC-0076: Scaling the shared runtime - past one host and one writer

- Status: **Accepted (2026-10-29, Chief): "yes. letsgooo".** FR17 to FR19 in the order of §4, S0 first; FR20 as direction only, per §3.4's recommendation, with no build and no amendment of the single-cursor law until that build is authorised. Answers GraphOps's "Follow-up requests: scaling the shared runtime
  past one host and one writer" (FR17 to FR20), written against `546b94a8` (v5.13.0).
- Author: Pete (cargopete)
- Date: 2026-10-29
- Depends on: RFC-0073 (the shared runtime: followers, tenant shares, strict isolation, fault
  containment), RFC-0021 (one cursor per chain), RFC-0022 (the lease and fence), RFC-0027 (joining
  mounts), RFC-0073 FR16 (the segment mirror).
- Blocks: GraphOps's public launch (FR17, FR18, FR19). FR20 blocks nothing; GraphOps asks for a path.
- Nature: FR17 to FR19 are new capability in the default binary for an operator who runs the shared
  runtime, and keep every law as written. **FR20 does not.** It needs Chief to amend the
  single-cursor law and RFC-0073 §5, and this RFC asks for that decision rather than assuming it (§3.4).

## 1. What this is for

RFC-0073 made one runtime a fair place for many organisations' nests. It is still one machine and one
process: followers must share the writer's disk, every nest on a chain moves at the pace of the
slowest, a chain seats at most as many tenants as it has permits, and one process does a chain's
whole ingest. GraphOps's launch puts hundreds of organisations on mainnet and the large L2s, so each
of these is a ceiling on every customer at once.

## 2. What exists, read 2026-10-29 at `546b94a8`

**The cursor** is `runtime_index_loop` (`src/indexer.rs:2355`), one per chain. One union `getLogs` per
window (`union_filter`, `:1159`; fetch at `:2648`), then `fan_out_window` (`:2112`) runs each live
nest's `process_window` **in turn**, awaited one after another. The window starts at `min(nexts)`
over live nests (`:2521`) and its size is one `AdaptiveWindow` shared by the cursor and capped for
the most demanding nest (`:2396`). There is no prefetch of window N+1 and no concurrency between nests.
Progress is already per nest (`nexts`, `:2379`, each nest's own `last_block` and checkpoints); the
coupling is that the cursor fetches from the minimum and waits for everyone.

- A re-admitted nest rejoins with its position unchanged, pulls `global_next` back, and its
  siblings skip the windows they have via the `nexts[i] > to` guard (`readmit_due`, `:1720`). The
  range is fetched again for all of them.
- A `joining` mount already catches up on its own: `PendingJoin::catch_up` (`src/runtime.rs:5017`)
  runs `NestIngest::catch_up` (`src/indexer.rs:7317`), a solo loop with its own chunker and fetches,
  then hands the nest to the cursor with `CursorCommand::Mount`.
- Reorgs are detected once per chain against the most caught-up nest's checkpoints
  (`detect_reorg`, `:2526`) and rolled back per nest (`fan_out_rollback`, `:2041`).
- **No window deadline exists.** RFC-0073 FR6 said one would; `docs/admin-api.md` "Faults" says
  "not yet", because abandoning a window part-way can let its commit land after its derived state
  was dropped. That is a promise of RFC-0073's that 5.13.0 did not keep, and FR18 is where it is kept.
- No metric records a window's duration; the only duration histogram is per RPC request.

**Followers** (`src/reader.rs`, `src/feed.rs`, `src/feed_http.rs`) take one NDJSON stream per writer:
`hello`, `mount` lines, then per dataset a `snapshot` and `delta`s keyed by `(epoch, generation)`,
rows as JSON strings, the catalogue's bytes on every delta that moves `sealed_through`. The writer
never waits for a follower: a full queue closes the connection and the follower resnapshots. From the
writer's directory a follower reads more than segments: sealed Parquet, `mounts.toml` (for
`max_rss_mb`), `.admin-token` when no token is in the environment, each nest's `nuthatch.toml` and
labels (`reader.rs:1186`), and offchain snapshots under `<dataset>/offchain`. Retention is
`POST /_admin/feed/applied` plus `segments/.retained.json`. No metric reports visibility lag or a
follower's cost; `lag_generations` on `/ready` is the only figure.

**The mirror** (`src/publish.rs`) uploads segments, `schema.json`, the catalogue (compare-and-swap),
`publish.json` and the seed snapshot, in that order, **after** the local catalogue has moved.
Seeding checks every file against its sha256 (`src/seed.rs:386`).

**The SQL gate** is one per chain cursor (`src/indexer.rs:2837`), two permits by default, at most
`SQL_MAX_CONCURRENCY_CEILING = 16` (`src/serve.rs:54`), read once from the environment. Every tenant
with a mount on the gate is seated with a reservation of one permit (`GateLedger::may_take`,
`src/fair_share.rs:127`): past as many tenants as permits, each may hold one and must wait for any to
free. Strict isolation builds no pools at boot; it checks `min(permits, datasets) x 512 MB` plus the
ingest reservation against the wall (`src/analytics_budget.rs:150-166`), so three permits at 512 MB
already break 2 GiB, and its session cache holds 16 `(dataset, tenant)` sessions (`src/analytics.rs:141`).

**Found while reading:** `set_max_rss_mb` (`src/runtime.rs:5945`) checks projected use and the
entity cap but does not re-run `validate_strict_budget`, so lowering the ceiling live on a strict
runtime with raised permits leaves a pool split boot would refuse. Fixed in S1 (§3.1).

## 3. The design, request by request

### 3.1 FR19. Many more tenants per chain

**Reservations follow activity, not mounts.** A tenant is *active* on a gate while it has a query
queued or running there, and only active tenants are seated. The fair share in `GateLedger::fair` is
computed over the active set by weight, as today. An idle tenant with a thousand mounts holds nothing.

**A reserve for the newly active.** A running permit is never taken back, so a tenant that becomes
active when every permit is held by borrowers can only wait for one to finish. The gate keeps
`reserve = max(1, permits / 8)` permits that may be taken only by a tenant holding none. Borrowing past
a fair share stops at the reserve. A tenant under its share is then admitted within the 250 ms wait
whenever a reserve permit is free, which holds whenever fewer than `reserve` tenants become active in
the same 250 ms; that is the bound the benchmark measures and the document states, not a stronger one.

**The permit count is runtime configuration.** `[runtime] sql_permits` in `mounts.toml`, settable by
`PUT /_admin/runtime` beside `max_rss_mb`, persisted first, as `max_rss_mb` is. The compile-time 16
goes. The ceiling is memory: `permits x per_statement_mb + ingest reservation + headroom <= max_rss_mb`,
the same arithmetic `analytics_budget` does today, rechecked on every change to either figure (which
closes the gap in §2). At the default 2048 MB nothing changes: two permits.

**Engine memory per running statement, not per permit.** Strict mode charges one pool per permit
because it builds a pool per `(dataset, tenant)`. Instead every statement on a gate draws its memory
limit from one cursor analytics budget, released when it ends, so 1,000 tenants cost the gate nothing
until they query and 200 querying tenants cost what 200 running statements cost, bounded by permits.
Per-tenant sessions stay for isolation; the session cache becomes an LRU sized from the budget, and
the cost of a cold session is measured before the size is chosen, since 1,000 tenants will miss it.

**Reporting.** `GET /_admin/tenants` adds per tenant and gate `active`, `waits_total`,
`wait_seconds` (a histogram summary) and `rejections_total`; `nuthatch_sql_admission_wait_seconds
{tenant}` is a new histogram, and `reason="tenant_share"` stays distinct from `busy`.

**Gate:** `docs/bench/rfc-0076-tenants.md`: 1,000 tenants mounted on one chain, 200 querying at
once in a closed loop, admission wait p50 and p99 per tenant under its share, against today's
reservation-per-mount as the baseline. GraphOps has offered load profiles; this is the obvious place.

### 3.2 FR18. Nests progress independently on a shared chain

The cursor stays one per chain: one fetch position, one finality view, one reorg authority, one
failure boundary for extraction. What changes is that it stops waiting for its nests.

**Stages.**

1. **Extraction** fetches each range once with the union filter, verifies headers, and appends
   windows to a **window buffer**: ordered, bounded, each window carrying its logs, headers and the
   block hashes extraction saw. It runs ahead of its nests up to the buffer's limit and overlaps the
   fetch of window N+1 with nests processing window N.
2. **Per-nest ingest** is one task per nest, the single writer of that nest's dataset, reading the
   buffer from its own watermark and running `process_window` exactly as today. Up to
   `ingest_concurrency` nests run at once (default: available cores, at most 4 at the default
   ceiling); each dataset still has exactly one writer and every window commits in one hot-store
   transaction, so what a nest stores is what it stores today.
3. **Detachment.** A nest whose watermark falls behind the buffer's oldest window detaches to the
   catch-up path `joining` mounts already use, fetching for itself, and re-attaches when it reaches
   the buffer. A re-admitted nest does the same. Neither moves extraction's position, so no sibling
   fetches anything twice.

**The buffer is charged to the cursor.** A new admission term, `window_buffer_mb` (default 64 MiB at
the 2048 MB ceiling, scaling with it as the entity cap does), holds windows in memory; past it the
buffer spills to `<runtime>/cursors/<chain>/buffer/`, which is scratch, never data, and deleted at
boot. Fetched logs are not priced today at all (§2), so this term is new honesty, not a new cost.

**Reorgs.** Detection moves from the most caught-up nest's store to extraction, which keeps the
recent header chain itself. On a reorg it drops buffered windows above the common ancestor and puts
a **reorg marker** with the ancestor in the buffer. A nest reaching the marker rolls back from its
own watermark as `rollback_reorg` does today: a nest at or below the ancestor does nothing, a nest
whose sealed watermark is above it is quarantined terminally, as today. Every nest agrees on the
depth because one component decided it. Each nest's per-window stale-tail check (`process_window`,
`:7807`) stays as the backstop.

**Factory nests are the hard part.** Today a factory nest discovers a child inside the window and the
union filter widens at the next window. With extraction running ahead, windows already in the buffer
were fetched without the child. A nest that discovers a child therefore refetches, for itself, the
child's logs over the buffered range it has not yet processed, address-filtered, before it continues;
extraction widens its filter from the next fetch. The over-cap and topic0-only paths
(`FACTORY_FLIP_THRESHOLD`) stay as they are. This is the slice's first test, because it is the place
a decoupled design silently loses rows.

**Failure.** A failing `getLogs` stalls extraction; nests keep committing the windows already
buffered and report `waiting_on_extraction`. A slow nest delays only itself, until it detaches.
The bound on siblings is stated per window size in `docs/admin-api.md`: a sibling's added tip lag is
at most the time to hand one window across the buffer, measured and gated, not a figure argued here.

**The window deadline (RFC-0073 FR6's unkept promise).** Splitting `process_window` into an
abandonable part (decode, fetch, entity bind, view deltas) and a commit that is not is the
prerequisite the docs name. With nests decoupled the deadline becomes a backstop: `[mounts.limits]
window_deadline_ms`, abandoning before the commit only, retracting view deltas as a failed commit
does today, and quarantining retryably after three consecutive overruns.

**Metrics.** `nuthatch_nest_window_seconds` (histogram), `nuthatch_nest_behind_extraction_blocks`,
`nuthatch_nest_extraction_wait_seconds_total`, `nuthatch_cursor_buffer_bytes` and
`nuthatch_cursor_buffer_spilled_bytes`.

**Gates.** Tip lag's after-seen p50 stays at or under 30 ms with the buffer hop in it; the
single-nest RSS gate holds; reorg property tests run with randomised nest speeds and detachment, and
must converge every nest to the canonical chain; a test with one nest slowed tenfold shows its
siblings' tip lag inside the documented bound; a factory test discovers a child in a buffered window
and compares row for row with today's cursor.

### 3.3 FR17. Low-latency followers over the network

**Everything a follower reads from disk comes from the writer or the mirror instead.**

| Read today | Remote source |
|---|---|
| Sealed segments | The `[runtime.publish]` mirror, each file checked against its catalogue hash as seeding does |
| Nest config and labels | The `mount` line carries the bundle hash; the follower fetches the bundle from the registry (RFC-0073 FR15) and checks it |
| Offchain snapshots | Published to the mirror beside the segments, content-addressed |
| `mounts.toml` (`max_rss_mb`) | The follower's own `--max-rss-mb`, defaulting to the per-cursor default |
| `.admin-token` | `NUTHATCH_ADMIN_TOKEN` only |

**A follower never names a segment it cannot read.** A sealing delta prunes hot rows and moves the
catalogue together, so a follower cannot apply half of it. On a delta whose catalogue names files it
lacks, the follower fetches and verifies them first, then applies the delta whole; deltas behind it
queue within the dataset's budget and a queue past it resnapshots, as a full queue does now. The
writer is unchanged and never waits: the mirror already uploads after the local catalogue moves, and
the follower's wait for the mirror is where that shows. Seal visibility is therefore reported apart
from tip visibility.

**Wire encoding v2.** Negotiated on `hello` (`feed: 2`), v1 NDJSON kept for inspection and for old
followers through one release. A delta becomes a length-prefixed frame: a small header (dataset,
epoch, generation, catalogue hash) and its puts as an Arrow IPC record batch per table, drops and
meta as compact fixed records. Arrow 59 with `ipc` is already a dependency. Flow control stays TCP's
and the writer's bounded queues; a stalled remote follower is dropped exactly as a local one is.

**Visibility lag on one clock.** The follower acknowledges each applied generation on the existing
`applied` channel within 10 ms; the writer records commit-to-ack minus half the measured round trip,
per dataset, as `nuthatch_feed_visibility_seconds{dataset, follower}`. It never compares two hosts'
clocks. Beside it: `nuthatch_feed_delta_bytes`, `nuthatch_feed_encode_seconds`,
`nuthatch_follower_apply_seconds` and per-connection queue bytes, which `FeedMemory` already tracks
and nothing exports.

**Retention through the feed.** The `applied` reports already carry what the ledger needs; for remote
followers the writer applies the same `releasable` rule to **mirror** deletions instead of to
`.retained.json`, so a file a remote follower still names stays in the mirror.

**The figure.** GraphOps proposes p99 at or under 100 ms within a region at tip-following load. That
is plausible for tip deltas, which are small, and not for sealing deltas, which wait on an upload and
a download. The RFC proposes gating tip deltas on the agreed figure and reporting seal visibility
without a gate until there is a measurement.

**Gates.** The rolling-upgrade gate gains a leg with followers in separate network namespaces, no
shared path and a MinIO mirror; GraphOps's second-region environment answers the latency criterion,
which CI cannot. Answers and provenance (`as_of`, `sealed_through`, `nid`) compared with a local
follower for every route, as `e2e_follower` does.

### 3.4 FR20. More than one writer per chain: a design, and a decision for Chief

**This is the request that crosses a law.** `CLAUDE.md` says a cursor is "single-chain,
single-writer, one observable failure boundary"; RFC-0073 FR5 declined "several cursors per chain"
and §5 lists it as a non-goal, and Chief accepted both on 2026-10-27. GraphOps acknowledge this and
argue the properties survive. Some do. One does not, and that should be decided with it in view.

**The design FR18 makes possible.** FR18's extraction stage becomes a separable component, the
chain's **cursor** in the sense the law means: it alone fetches, verifies headers, decides finality
and reorg depth, and holds the window buffer, now readable over the network. A **writer** is a process
holding a set of datasets, each under a per-dataset lease and fence in its hot store, consuming the
buffer exactly as FR18's per-nest tasks do in-process. The RFC-0022 lease and fence need no change of
mechanism: they guard a store, and today the store is keyed by chain (`src/worker.rs:53`) because the
cursor was the unit of ownership; keying it by dataset is placement, not a new protocol. A dataset
moves between writers by releasing its lease after a sealed boundary and seeding from the mirror, the
paths that exist today. Followers (FR17) already speak per dataset, so one route namespace over many
writers is a roster of which writer holds which dataset, served by the admin API of the extraction
component or by any writer.

**What holds.** One finality view and one reorg depth per chain, because one component decides them.
One writer per dataset, enforced by the store's fence, not by agreement. Extraction once per chain.

**What does not.** One failure boundary per chain becomes two kinds: the extraction component, whose
failure stalls every writer on the chain, and each writer. Between them is a network link carrying
every log the chain produces for every nest, which the single process never had. The factory
refetch of §3.2 crosses that link or goes to the RPC from each writer. And it is a distributed
system to operate: a component nuthatch has not had to run, upgrade or monitor before.

**Recommendation.** Accept §3.4 as the direction, so FR18's buffer is built with the seam where
FR20 needs it (a buffer with an ordered, versioned interface, not a channel of in-process structs),
and **build none of it** until FR18 and FR19 have shipped and GraphOps can show a chain whose load
still exceeds one machine afterwards. The split today, a second runtime with its own cursor, costs
duplicate RPC and coordination outside nuthatch, and is correct. If Chief accepts the direction, the
single-cursor law needs amending to read "one cursor per chain, one writer per dataset", and RFC-0073
§5 annotated, at the time the build is authorised rather than now.

## 4. Order of work

Each slice ends runnable, keeps the 2 GiB per-cursor CI budget for a default single-nest runtime and
tip lag's after-seen gate.

| Slice | Requests | Size |
|---|---|---|
| S0 | FR18 spike: the buffer and per-nest tasks on one chain with a factory nest, after-seen p50 and row-for-row parity against today's cursor. Can stop FR18. | one week |
| S1 | FR19: active-set reservations, the reserve, `sql_permits` live, the strict recheck, per-statement engine memory, the tenant bench | one to two weeks |
| S2 | FR18: stages, detachment, reorg markers, the window split and deadline, metrics | two to three weeks |
| S3 | FR17: remote sources, sealing-delta fetch, feed v2, visibility and cost metrics, the network-namespace gate | two to three weeks |
| - | FR20 | design only, §3.4, Chief's decision |

S0 goes first because it is the slice that can falsify the others: if the buffer hop costs the
after-seen gate, or factory parity cannot be held, FR18 is redesigned before anything is built on it.

## 5. Non-goals

As GraphOps list them: authentication, accounts, billing, identity-bound quotas, a hosted control
plane, GraphOps-specific code, Kubernetes manifests, nuthatch serving a registry. And for now, several
writer processes per chain (§3.4).

## 6. Risks

- **FR18 touches the hottest loop in the tree**, and everything after it depends on its parity. S0
  exists for this reason, and the reorg property suite gains randomised nest speeds.
- **Factory discovery under decoupling** is where rows go missing quietly. It is tested by parity
  against today's cursor, not by asserting nothing went wrong.
- **FR19's reserve is a bound, not a promise**: a burst of more newly active tenants than the reserve
  inside 250 ms waits. The document says so with the measured figure.
- **FR17 makes the mirror load-bearing for queries.** A slow mirror shows as seal visibility lag on
  remote followers and never as a short answer, because a delta naming a missing file is not applied.

## 7. Open questions

1. FR17: the gated figure for tip visibility, to be agreed with GraphOps on measured load.
2. FR18: whether the default `ingest_concurrency` should be lower at the 2048 MB ceiling, once S0
   has measured what concurrent windows cost in memory.
3. FR19: the reserve's size, once the bench has GraphOps's activity profile.
4. FR20: Chief's decision on the direction and, separately and later, on the amendment.

## 10. S3 built (2026-10-29, #2490)

FR17 as §3.3 describes it, with three departures, each for a reason found while building.

**What was built.** `nuthatch serve --follow <writer> --mirror <mirror> --registry <registry>
--cache <dir> [--max-rss-mb N]` runs a follower that shares no path with the writer. `--cache` is its
own directory, laid out as a runtime's: each mount's nest is installed at `data/<nid>` from the
registry, each segment a catalogue names lands at `segments/<hash>.parquet` once its bytes hash to
that name, each dataset's offchain snapshots under its `offchain/`. The token is
`NUTHATCH_ADMIN_TOKEN` alone. The local follower is unchanged; `--runtime` still selects it.

A delta or snapshot whose catalogue names a segment the follower does not hold is held, with every
line behind it for that dataset, charged to the dataset's budget, while a fetch task retries with
backoff from 100 ms to 2 s; past the budget the follower resnapshots. The held lines are applied in
order once the files are in. `/<route>/ready` reports `follower.mirror`: what is waited for, how
long, and the last error. The publisher now also uploads each provisional segment to
`_provisional/<hash>.seg` and each offchain snapshot, then its catalogue, to `_offchain/`. It never
deleted anything from a mirror before; it now deletes a provisional segment a fold replaced, and only
that, by `seal`'s `releasable` rule: every connected follower of the dataset has applied the commit
that stopped naming it and the grace has passed, or the hour is up. Final segments stay append-only.
All of this is behind `followers = true` in `[runtime.publish]` or `[mounts.publish]`, off by
default: offchain snapshots may be data an operator never meant to publish, a public mirror's
credentials may not allow deletes, and with it off a pass puts and deletes exactly what 5.13.0 did.
On, the publisher also writes `_followers.json`, and a follower whose seal waits on a prefix without
it says on `/ready` that the mirror does not publish for followers. The follower's `--cache` deletes a
segment once no copy it holds, retiring copies and lines waiting on the mirror included, has named it
for the same 5-minute grace.

Feed v2 is negotiated by `?feed=2` and answered with its own content type, so a follower facing a
writer that predates it reads v1, and a writer facing an old follower sends v1. Visibility,
encode, delta-size, apply, seal-wait, mirror and per-connection queue figures are exported as
`docs/admin-api.md` lists them.

**Departures.**

1. *The registry fetch is by NID, not by bundle hash.* The bundle hash includes `generator_version`,
   so the hash a 5.14 writer computes for its dataset directory is not the hash of the bundle a 5.13
   `nest publish` put in the registry. The `mount` line already carries the NID, which is a content
   address over the same manifest less the generator version, and `pull_by_nid` refuses a bundle
   that does not compute to it before `nest load`'s checks run. What the `mount` line gained instead
   is `data_identity`, the prefix the writer's publisher mirrors the dataset under, which the
   follower cannot work out from the bundle: an offchain pull moves it.
2. *Feed v2 compresses each batch itself.* Arrow's IPC compression decompresses into whatever size
   the sender declares, which broke the follower's guarantee that nothing it reads is allocated past
   its budget: a compressible delta well under its wire limit decompressed past it. Each batch is
   now an uncompressed IPC stream LZ4-compressed whole behind its size, and the follower checks that
   size, then the rows the columns rebuild to, against what the dataset has left before allocating
   either. A table with fewer than 8 puts sends them as they are: the schema cost more than columns
   saved.
3. *The visibility series is labelled by dataset and kind, not follower.* Connections come and go;
   a follower label would grow without bound.

**Figures.** v1 against v2 on deltas a real store commits for windows of decoded ERC-20 transfers,
release build, Apple M5 Pro, one thread (`feed_wire::tests::measure_v1_against_v2`):

| Delta | v1 bytes | v2 bytes | v2/v1 | v1 encode | v2 encode | v1 decode | v2 decode | apply |
|---|---|---|---|---|---|---|---|---|
| one row | 717 | 533 | 0.74 | 0.5 µs | 2.3 µs | 1.1 µs | 0.2 µs | 0.5 µs |
| a tip block, 40 transfers | 21,144 | 3,647 | 0.17 | 10.5 µs | 76.4 µs | 19.8 µs | 38.5 µs | 3.9 µs |
| 50 blocks, 1,000 transfers | 526,275 | 74,433 | 0.14 | 0.25 ms | 1.7 ms | 0.49 ms | 0.85 ms | 0.11 ms |
| 1,000 blocks, 20,000 transfers | 10,591,557 | 1,682,962 | 0.16 | 5.4 ms | 34.5 ms | 9.9 ms | 17.8 ms | 2.8 ms |

v2 is a sixth of v1's bytes on any window worth the name, and costs about seven times v1's encode
and twice its decode. Encoding is once per delta however many followers it goes to, where v1
serialises once per connection, so the writer's cost evens out at about seven followers; a delta of
more than 512 ops is encoded off the async workers. Apply is the same either way.

Visibility lag as the writer records it, 200 tip blocks and 20 seals, one follower beside the writer
and one on "another host" in the same process over loopback with a filesystem mirror, debug build
(`e2e_remote_follower::measure_visibility`): tip deltas 398 observations, mean 1.4 ms, p50 under
2.5 ms, all but one under 5 ms, the slowest under 25 ms. Seals: the follower beside the writer, all
20 under 1 ms; the remote one, all 20 between 100 and 250 ms, every one of them spent waiting for the
publisher, which the test runs after the seal commits, so each first fetch missed and waited out the
100 ms backoff (`nuthatch_follower_seal_wait_seconds` mean 120 ms). That is the shape §3.3 predicted:
tip visibility is the feed's, seal visibility is the mirror's.

**Gates.** `tests/e2e_remote_follower.rs`: a remote follower answers as a local one on `/sql`,
`/tables`, `/entities`, `/schema` and `/ready`, `as_of`, `sealed_through` and `nid` included, through
the snapshot, tip deltas, a seal, a fold and a seal published only where a final segment goes while
the delta still calls it provisional, and over v1 too; the same against versitygw for both mirror and
registry (the `minio-publish` job); a slow mirror, a mirror missing the file and one serving the wrong
bytes each leave it answering the generation before the seal, 200 and whole. `publish.rs` tests the
mirror deletion against a follower that has and has not moved on, and offchain snapshots end to end
with a tampered one refused, and that with `followers` off a seal and a fold leave a `memory://`
mirror with no key a 5.13.0 pass would not have put and nothing deleted.
`a_mirror_not_published_for_followers_says_so` and
`a_folded_segment_leaves_the_follower_cache_after_the_grace` cover the follower side.
`scripts/rolling-upgrade.sh` gains a leg with a separate process, its
own directory, the registry and a filesystem mirror; its chain never seals, so it gates the CLI, the
processes and the registry, and the e2e gates segments.

Mutation checks, each red: applying a sealing delta before its files are in (a `503` where the
held generation should answer, `a held seal is never a refusal`); skipping the hash check (a short
answer, 4 rows of 6 with `degraded: true`, and a tampered offchain snapshot taken); splitting a row
whose text the columns cannot rebuild (`a_v2_delta_decodes_to_exactly_the_delta_encoded`);
publishing follower files with `followers` off (`_followers.json`, `_offchain/` listed on the off
mirror); sweeping a cached segment a copy names (a `503`, `a sealed segment this copy names is gone
from disk`); dropping the marker check (`/ready` says only that the mirror lacks the file).

**Not done.** Network namespaces: the gate's remote follower shares the host's loopback, and
GraphOps's second-region environment answers the latency criterion, which CI cannot. An offchain
pull moving a dataset's data identity, and so its mirror prefix, predates this slice; a remote
follower follows the new prefix within 30 s, once the publisher has refilled it.
