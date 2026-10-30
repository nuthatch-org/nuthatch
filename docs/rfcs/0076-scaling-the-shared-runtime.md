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

## 8. S0 measured

Measured 2026-10-29 on `pete/2487-rfc-0076-s0-window-buffer` (#2487), off `546b94a8`, on a MacBook
(Apple silicon, other nuthatch sessions running on it). Nothing here has run on a CI runner.

### What was built

Behind `NUTHATCH_CURSOR_BUFFER=<windows>` (`NUTHATCH_INGEST_CONCURRENCY` optional), read in
`spawn_cursor`. Unset, every runtime runs `runtime_index_loop` exactly as before; no test sets it.

- **Extraction** (`buffered_extraction`): one task per chain that polls the tip, fetches each range once
  with the union filter, the narrowed `base ∪ children` filter and its in-window discovery fixpoint
  included, and appends windows to an in-memory buffer bounded by window count. Each window records
  the addresses it asked for. It runs the same one `detect_reorg` per window the shared cursor does.
- **Per-nest ingest** (`buffered_nest_task`): one tokio task per nest, owning its `NestIngest`, the
  only writer of its dataset, reading the buffer from its own watermark and calling the unchanged
  `process_window`. A semaphore bounds how many commit at once (default `min(cores, 4)`).
- **Factory refetch** (`buffered_window_logs`): before a nest processes a buffered window, any child
  it knows that the window's fetch did not ask for is fetched for that nest alone, address-filtered,
  over the window's range from the nest's own watermark, with the discovery fixpoint, and merged.
  Extraction reads each nest's children after every committed window, so its next fetch is wider.
- **Minimal detachment.** A full buffer waits for the slowest nest unless some nest has drained it,
  in which case the oldest window is dropped. A nest behind the oldest window fetches for itself with
  its own `fetch_window` and rejoins the buffer when its watermark reaches it.

### What was not built

Reorg markers: a reorg extraction detects ends the buffered cursor with an error. Lifecycle commands:
the channel is held and never read, so a live mount, unmount or rename waits until the driver times
out. Quarantine re-admission: a nest's fault is terminal on this cursor. The over-cap single-block
recovery (COR-5), the `topic0_refused` fallback, a byte bound on the buffer and any spill to disk,
the window deadline, and every metric in §3.2. Headers are not carried in the buffer: each nest's
`process_window` still asks for its own timestamps and checkpoint hash, as it does today.

### Parity

`indexer::tests::the_buffered_cursor_stores_what_the_shared_cursor_stores`: two static nests and a
factory nest on one 60-block chain, windows capped at 3, children announced at blocks 10 and 25
(the second emitting once before its announcement in that block) and a never-adopted stranger. The
shared cursor's run is the reference; every table of every nest, hot and sealed, is compared row by
row in full JSON. Two buffered runs, the factory nest slowed 30 ms a window in both:

- a 64-window buffer, so every window is fetched before the factory nest reaches it and the refetch
  path carries both children (asserted: refetches > 0, no own fetches);
- a 2-window buffer, so the fast nests drain it and the factory nest detaches (asserted: own fetches > 0).

Both are identical to the reference. **Mutation:** dropping the refetched logs instead of merging them
turns it red on the refetch run, for the reason it exists:
`nest 2, buffer of 64 windows (refetch path): table child__ping: 39 rows wanted, 4 stored; missing [...block 14...]`.

### One nest slowed tenfold

`indexer::tests::s0_one_nest_slowed_tenfold` (ignored; a measurement). Three static nests, 150 blocks,
windows of 3. The slowed nest's unslowed window time was 4.4 ms; 39 ms was added, about ten times.
Milliseconds for the two siblings to reach the tip, three runs each:

| Cursor | Nothing slowed | One nest slowed |
|---|---|---|
| Shared (today) | 661 to 729 | 2,621 to 2,750 |
| Buffered, 8 windows | 565 to 671 | 436 to 545 |

Today a slow nest is every sibling's pace. Buffered, the siblings do not notice it and the slow nest
takes what its own delay costs (2,311 to 2,350 ms). The price is that the slow nest, once detached,
fetches its windows a second time.

### Tip lag, after-seen p50

`tip-lag.sh`'s fixture and method with the nest mounted in a runtime directory, so the cursor is the
one measured, 100 samples a run. The poller is perl rather than bash 5, which this machine lacks.

| Run | 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| Shared cursor | 24 ms | 29 ms | 27 ms | 26 ms |
| Buffered | 24 ms | 29 ms | 32 ms | 23 ms |

No difference the noise does not cover. Both sit above CI's 17 to 19 ms on this laptop, so the 30 ms
gate itself was not tested here; the buffer hop is not visible in it.

### RSS, one nest

`multinest-footprint.sh` with `NESTS=1`, median RSS while following the live tip, three runs:
shared cursor 84, 80 and 84 MB; buffered 92, 93 and 93 MB. **About 10 MB more**, not explained yet:
an eight-window buffer holding the backfill's widest windows is the first suspect, and a byte bound
is what §3.2's `window_buffer_mb` already proposes, but it was not profiled. Far inside the 2 GiB
budget, and still a regression S2 must account for before any footprint gate sees it.

### Verdict

**Continue.** The two things that could stop FR18 did not: factory parity holds row for row through
both the refetch and the detached path, with a mutation to prove the test can see a lost child, and
the buffer hop adds nothing measurable to after-seen tip lag. A slow nest stops costing its siblings.
S2 carries what S0 stubbed: reorg markers, lifecycle at window boundaries, re-admission, the
buffer's byte bound (and the 10 MB above), and the after-seen gate measured on a CI runner.

## 9. S2 built

Built 2026-10-29 on `pete/2489-rfc-0076-s2-independent-nests` (#2489), off S0's branch, and measured on
the same MacBook as §8, other nuthatch sessions running on it. Nothing here has run on a CI runner.

### What was built

The S0 spike is rewritten as `src/indexer/buffered.rs`, a child of the indexer, and is **the default**
(below). `NUTHATCH_CURSOR_BUFFER=0` runs the shared cursor for one release.

- **Extraction** keeps the window ends it fetched with the hash it read for each, and decides a reorg
  from them, falling back to the most caught-up nest's checkpoints when it has none or none is
  canonical. It checks before a fetch, as the shared cursor does, **and after one**: the previous
  window's end is read again once the new window's logs and header are in, and a window that does not
  extend the chain extraction holds is not buffered. The property test found that second check
  missing (below). On a reorg it drops every buffered window above the ancestor and issues a
  **marker**; each nest rolls back from its own position with the unchanged `rollback_reorg` at its
  next window boundary, terminally if the fork is below its sealed watermark, as today. A nest that has
  missed markers older than the 64 kept, or rejoins from a quarantine, checks its own checkpoints
  first, as `check_joined_nests` does.
- **Headers in the buffer.** Each window carries the hash and timestamp extraction read for its end,
  and a nest's checkpoint for that block is answered from it; every other hash a window is checked
  against still comes from the source, so the per-window backstops (#2146) are unchanged.
- **Lifecycle.** Mount, unmount and rename are applied as they arrive. A mounted nest behind the
  buffer catches up by itself; an unmount is acknowledged once its task has ended and nothing on the
  cursor holds its store (extraction reads a nest's store through a `Weak`, under a lock the
  acknowledgement waits on). A retryable fault backs off as today and the re-admitted nest **fetches
  its missed range for itself**, never moving extraction back; a panic is that nest's terminal fault,
  contained per nest as before. The over-cap block (COR-5) becomes a window each nest asks for by its
  own filter, a factory nest then by `base ∪ children`, so only a nest still over the cap is faulted;
  the `topic0_refused` fallback is extraction's, as it was the cursor's.
- **The bound.** `window_buffer_mb` is charged to every cursor's admission (64 MiB at 2048, scaling
  with the ceiling, following `PUT /_admin/runtime` live). Past it windows spill to
  `<runtime>/cursors/<chain>/buffer/`, four times the bound, emptied when the cursor starts. Extraction
  holds windows for the nests behind the fastest and **at most two at or ahead of the fastest**: a
  single nest's cursor holds the window it is processing and the next.
  A nest that owns every log in a window is lent the window rather than given a copy.
- **The window split and the deadline.** `process_window` is now `prepare_window`, which decodes,
  reads, checks and binds and has no side effect a retry would see, and `commit_window`, which feeds
  the views, commits, and does everything after. `[mounts.limits] window_deadline_ms` (on the mount
  that indexes the dataset, live by `PATCH`) times out the first only; three overruns in a row are a
  retryable quarantine. It applies on both cursors.
- **Metrics**: `nuthatch_nest_window_seconds`, `nuthatch_nest_window_overruns_total`,
  `nuthatch_nest_behind_extraction_blocks`, `nuthatch_nest_extraction_wait_seconds_total`,
  `nuthatch_cursor_buffer_bytes`, `nuthatch_cursor_buffer_spilled_bytes`.

Not built: factory refetch across the network for FR20, a per-nest `ingest_concurrency` above the
process setting, and §7's question 2, which still wants a measurement of concurrent windows'
memory on a multi-nest cursor.

### The default

The buffered cursor is the default because the three conditions set for it hold with it on: the whole
`it` suite (622 tests, 0 failed), the reorg property suite (16 cases) and the parity tests. Every test
target, the lib suite included, was run with the switch in both positions: every target passed but one in both runs, 2,978 tests each, and the one was the same in both: the
builder's config reference did not yet name `window_deadline_ms`, now documented. The suite's
runtimes really ran buffered: a probe that made the buffered cursor fail at once turned
`e2e_runtime_parity` red with the switch on.

### Parity

`the_buffered_cursor_stores_what_the_shared_cursor_stores` (S0's, unchanged) passes on the rewrite,
through the refetch path (64 windows) and the detached path (2 windows). New,
`the_buffered_cursor_converges_through_a_reorg_as_the_shared_cursor_does`: the reorg fixture below
under the shared cursor and three buffered configurations, each compared row for row with the shared
cursor's run over the replacement chain alone, and asserting the buffered runs went through a marker.

### Reorgs

The fixture: three nests, two static and a factory; the chain is replaced from block `fork` while
the tip moves, a child announced only on the replaced chain must vanish and one announced only on
the replacement must appear, and every replaced block differs in hash, timestamp and value.
`every_nest_on_the_buffered_cursor_converges_after_a_reorg` is the property: fork 8 to 30, replaced 1
to 8 blocks later, each nest slowed 0 to 19 ms a window, 1 to 6 buffered windows (small enough to
detach the slow nests), windows of 1 to 4 blocks. 16 cases, 59 s.

Its first run failed, on its own random draw: `fork 20, replaced at tip 22, delays [14, 12, 13] ms, 3
windows of 2: no nest-wide tip: [Some(40), Some(20), Some(40)]`. Extraction had checked its newest
window end before a fetch, the chain was replaced between that check and the fetch, and the new
window's header was read from the replacement, so the chain extraction held was mixed and looked
canonical at its end. Nest `b`, with no rows in 19..=20, had committed extraction's old checkpoint for
block 20 and refused every window after it on its parent check, waiting for a marker that never
came. The shared cursor closes this race at the nest, by reading hashes after the logs; extraction
now does the same (§9 "What was built"). The case is pinned in the parity test.

### One nest slowed tenfold

`s2_one_nest_slowed_tenfold_per_window_size` (ignored; a measurement). Three nests following a tip
that publishes a window's worth of blocks every 40 ms; the third slowed to ten times its siblings'
unslowed p50 tip lag; the siblings' lag is from a block's publication to its commit. p50, ms:

| Window | Shared, none / one slowed | Buffered, 4 permits | Buffered, 1 permit |
|---|---|---|---|
| 1 block (slowed 73 ms) | 10.0 / 541 | 9.1 / 8.0 | 10.4 / 458 |
| 4 blocks (slowed 80 ms) | 9.9 / 670 | 8.8 / 9.5 | 16.2 / 647 |
| 16 blocks (slowed 94 ms) | 10.8 / 802 | 9.4 / 8.5 | 13.5 / 712 |

**The bound, per window size: with an ingest permit free, the slowed nest adds nothing to its
siblings' tip lag at any of the three sizes** (the differences are inside run-to-run noise, p95 under
16 ms throughout). With every permit held by slower nests a sibling queues behind them at their pace,
as on the shared cursor, which is why `NUTHATCH_INGEST_CONCURRENCY` should stay above the number of
nests expected to be slow; at the default (cores, at most 4) a single slow nest never takes the last
permit. `a_nest_slowed_tenfold_does_not_set_its_siblings_pace_on_the_buffered_cursor` gates it: a
nest slowed 150 ms a window, siblings' p50 under 75 ms buffered and at least 75 ms shared.
`a_nest_over_its_window_deadline_is_quarantined_and_its_co_tenants_index_on` covers the deadline on
both cursors.

### Tip lag, after-seen p50

`tip-lag.sh`'s fixture and method with the nest mounted in a runtime directory, 100 samples a run,
the two cursors alternated, the S2b binary (whose log confirms which cursor each run started).

| Run | 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| Shared cursor | 27 ms | 33 ms | 34 ms | 30 ms |
| Buffered | 29 ms | 33 ms | 32 ms | 28 ms |

p99 52 to 67 ms either way. No difference the noise does not cover, as in S0; this laptop reads
above CI's 17 to 19 ms on both, so the 30 ms gate itself is still CI's to judge.

### RSS, one nest, and S0's 10 MB

`multinest-footprint.sh` with `NESTS=1`, median RSS while following the live tip, three runs each,
release binaries built with symbols kept for the profiler. "S2" is this branch before the window was
lent to the nest, "S2b" after (`c0012c74`).

| Binary | Shared cursor | Buffered, default | Buffered, 8 windows | Buffered, 1 window |
|---|---|---|---|---|
| S0 (`cf02f9ab`) | 85, 86, 86 | - | 95, 95, 93 | 85, 83, 84 |
| S2 | 83, 87, 83 | 89, 89, 88 | - | - |
| S2b | 79, 79, 79 | 84, 81, 81 | - | 84, 79, 81 |

**S0's 10 MB was not data at the tip.** A `vmmap -summary` and `heap` snapshot of each process
four minutes in, at the tip:

| Process | RSS | Physical footprint (peak) | Malloc live | Malloc resident | Malloc dirty |
|---|---|---|---|---|---|
| S0, shared | 85 MB | 30.0 (49.5) MB | 14.7 MB | 43.0 MB | 23.5 MB |
| S0, buffered(8) | 95 MB | 30.1 (58.7) MB | 15.3 MB | 52.3 MB | 23.5 MB |
| S2b, shared | 81 MB | 30.3 (43.9) MB | 14.7 MB | 39.2 MB | 23.7 MB |
| S2b, buffered | 84 MB | 30.3 (46.4) MB | 14.6 MB | 41.7 MB | 23.6 MB |

At the tip S0's buffered process held 0.6 MB more live memory than its shared one; its peak footprint
was 9.2 MB higher, which is the RSS gap. S0 never dropped a window its nests had passed: it held
eight, and the eight it held while catching up from the backfill to the tip were the widest of the
run. Freed, those pages stay resident in macOS's malloc zone, clean and outside the footprint, and
`ps` counts them. One window removes the gap (83 to 85 MB). S2 drops a window once every live nest
has passed it, holds at most two at or ahead of the fastest nest, and lends a nest the window instead
of copying it when the nest owns every log in it, as a single nest does. What remains is 2 to 5 MB
by the harness, with equal live memory and equal footprint by the profile: a 2.5 MB higher peak,
retained the same way. A one-window buffer did not separate from the default inside run-to-run
noise, so it is not attributed further. On Linux the release binary uses jemalloc with a one-second
dirty decay, which returns such pages; the footprint gates run there and have not been run.

### Mutations

Each against the committed tree, reverted after, the red assertion as printed:

- **Nests ignore reorg markers** (the rollback replaced by keeping the position): the parity test,
  `Buffered(64) did not converge: no nest-wide tip: [Some(40), Some(29), Some(40)]`. On the other two
  buffered cases of that run the per-window backstop rolled the nests back without the marker, which
  is why the fixture includes a nest with no rows over the fork.
- **The deadline wraps the commit** (one timeout over prepare and commit together):
  `a window whose commit landed (1 rows stored) reported Ok(None) after 1 overruns`.
- **The byte bound ignored** (a window always kept in memory): `spill true: the buffer held 194924
  bytes in memory against a bound of 11948 (largest window 9940)`.

### Not verified

- Nothing has run on a CI runner: the after-seen gate at 30 ms, the dense multi-nest footprint gate
  (`REGRESSION_MB` 602) and the RSS figures above are this laptop's. The footprint gate measures 20
  nests on one cursor and should be re-banded with `rss-band.yml` before the default ships.
- §3.2's factory test ("discovers a child in a buffered window and compares row for row with today's
  cursor") is S0's parity test, kept; it has not been run against a real factory corpus.
- §7 question 2, `ingest_concurrency` at the 2048 MB ceiling, is unanswered: what concurrent windows
  cost in memory was not measured on a multi-nest cursor.
- The spill path is exercised by a test with a 12 KB bound, not by a chain whose windows are tens of
  megabytes.

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
for the same 5-minute grace. A cached file is hashed against its name on first use after each connection and when fetched;
one that does not match is removed, fetched again and listed on `/ready` (`follower.cache_refused`).
A file rewritten after its check is read as it is until the follower's next connection.

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

**What a delta costs a follower, from the socket to the applied copy.** `A` is what the dataset has
left when the delta's dataset name is read; the slack is `SNAPSHOT_SLACK`, 64 KiB, plus the frame
header and a dataset name of at most 4 KiB. Every row below is alive at the instant named, and
their sum at any instant is at most `A` plus the slack.

| Allocation | Alive | Charged by | Released |
|---|---|---|---|
| frame header, dataset name | while the frame is read | the slack | next frame |
| payload (v2) | read to the end of decoding | refused unread past `A` + slack, then taken first by `decode_delta` | dropped, and given back, when decoding returns |
| catalogue, op keys and values, raw rows (v2) | from decoding until applied | taken before each copy is made | with the delta, after it is applied |
| one `Op` per op (v2) | from decoding until applied | its size taken as it is pushed | with the delta |
| decompressed batch and its Arrow buffers (v2) | while one batch is decoded | twice its decompressed size, taken before decompressing | given back once both are dropped |
| rows rebuilt from columns (v2) | from decoding until applied | worked out from column lengths and taken before any is built | moved into the ops; with the delta |
| the decoded delta in the handler (v1 and v2) | until applied | `Delta::bytes()` added to the copy's peak, checked against `A` | after the apply |
| the copy's growth while applying (v1 and v2) | from the apply on | `MemStore::peak_with`, worst point of the apply | it is the copy |
| a line held for the mirror | until its files are in | `Pending::bytes`, with the copy, against `A`; moved into the queue, not copied | applied or dropped with the connection |
| v1 line text | while parsed | refused at `A` + slack on the wire, read without holding the line whole; a decoded value is never longer than its text | as parsed |

The parser works out a delta's `A` only once every line before it is applied, so one capped delta is
in flight at a time. Under v2 the payload and the rows copied out of it are alive together, so a
large delta a v1 follower would refuse by its peak is refused by a v2 follower as it is decoded; both
refuse before the copy grows. Each charge has a mutation that turns its test red:
`the_payload_is_charged_beside_what_is_copied_from_it`, `a_catalogue_is_charged_as_it_is_copied`,
`ops_are_charged_as_they_are_decoded`, `rebuilt_rows_are_charged_before_they_are_built`,
`a_batch_is_held_to_its_peak_not_to_each_part_alone`, and the 1.2 MiB v1 case of
`a_delta_over_the_budget_is_refused_before_it_is_applied` for the handler.
`a_payload_near_the_limit_that_decompresses_to_it_is_refused` drives the worst case: a payload near
`A` whose batch decompresses to near `A` again.

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

Two corrections after measuring 5.14.0 over a 43 ms link (2026-10-30). Its follower sent one report
at a time, so with two mounts committing on one block the second dataset's acknowledgement queued
behind the first's round trip (tip p50 73 ms against 20 ms, #2527); reports now go concurrently.
And it took half the *previous* report's round trip, which on a jittery link pinned 131 of 755 tip
observations under 0.5 ms against a 4 ms ping floor (#2526); each report is now numbered, and the
writer waits for that report's own round trip, carried by a later one, before observing.

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
