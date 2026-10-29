# RFC-0075: The whole nest in a tab - ingest, hot store, reorgs, entities and the API in a browser

- Status: **Accepted 2026-10-28 (Chief): "i wanna do it".** S0's kill question was measured the same
  day and did not kill it (§3); S0 passed its gate the same day (§12). S1 to S6 are authorised in
  order, each on its own gate (§9).
- Author: Pete (cargopete)
- Date: 2026-10-28
- Depends on: RFC-0065 (the nest in a tab: decode, sealing and Burrmill already in a browser worker),
  RFC-0066 (the nest builder and its in-tab backfill), RFC-0074 (redb on `wasm32-unknown-unknown`
  over a byte buffer; on branch `pete/rotki-embedded-s0`, parked), RFC-0041 (authored incremental
  entities), RFC-0021 (one cursor per chain).
- Blocks: nothing.
- Nature: a separate artefact plus a refactor of the binary's crates that changes none of its
  behaviour. §8 says why it needs no RFC-0044 §8 record.

## 1. Where this comes from

Chris at GraphOps said on 2026-10-27 that one of nuthatch's interesting properties is that it is pure
Rust and could be embedded as a wasm blob. RFC-0074 tested that for one consumer, rotki's address
history, and it held. This RFC asks the general question: can the whole nest run in a browser, with no
server process anywhere?

RFC-0065 got most of the way. A worker at `/play` follows a chain from an RPC, decodes, seals Parquet
segments byte-identical to the binary's and answers SQL through Burrmill over hot ∪ sealed. What it
left out, by its own §3, is everything stateful between decode and seal: the redb hot store, the reorg
path, the DBSP circuits (built-in `balances`, `exposure`, `velocity` and `entities.toml`) and the HTTP
surface. The tab today keeps its unsealed tail as a list of rows and has no rollback, so it seals only
what two RPCs agree is final.

This RFC closes that gap. The result is a tab that is a nest: the same cursor, the same store, the same
circuits and the same answers as `nuthatch dev`, with the browser in place of the operating system.

## 2. What builds for wasm today

| Piece | State | Evidence |
|---|---|---|
| `nuthatch-decode`, `nuthatch-seal`, `nuthatch-scaffold`, `nuthatch-identity` | build for wasm32, CI-checked | RFC-0065, RFC-0066 |
| Burrmill | builds for wasm32; burrmill-wasm is 35.3 MB raw, 5.6 MB brotli | burrmill-wasm |
| redb 2.6.3 | builds for `wasm32-unknown-unknown` behind a custom `StorageBackend`; WASI does not (a nightly feature under `target_os = "wasi"`) | RFC-0074 S0, measured |
| dbsp 0.320.0 | **does not build as published**; builds and runs with a 459-line patch | §3, measured 2026-10-28 |
| `src/rpc.rs` | reqwest and tokio; the decision logic is transport-free | §5 |
| `src/indexer.rs` | 27,826 lines, 340 `tokio::` mentions, 52 clock or sleep calls | §6 |
| `src/serve.rs` | 15k lines of axum handlers over `AppState` | §7 |

## 3. DBSP on wasm32: the kill question

This was the unknown that could stop the programme, so it went first.

**As published, dbsp does not build.** `feldera-types` depends on actix-web unconditionally, which
brings mio, socket2 and zstd; dbsp links mimalloc's C build, `nix`, `core_affinity`, asks tokio for
`rt-multi-thread` (a `compile_error!` on wasm) and rkyv's `size_64` (only implemented for 64-bit
pointers); two literals overflow a 32-bit `usize`; the multihost exchange names `TcpStream`. At run time
it would fail further on: `Runtime::run` spawns a thread per worker, a multi-thread tokio runtime for
merges and an RSS monitor thread; the client and worker handshake parks on a `Condvar`; and
`Instant::now()` panics on this target.

**A patch makes it build, run and answer correctly.** 459 lines added and 89 removed across six
feldera crates, every change a target cfg, so the native build compiles the published code. Two parts
are design rather than plumbing:

1. **Workers are closures on the calling thread.** On wasm32 `Runtime::run` builds the circuit inline
   and registers a `serve_one` closure; `broadcast_command` sends as before and then drives every
   worker until its queue is empty, so the reply is waiting when the client looks for it.
2. **Merges are synchronous.** The merger runs `merge_step` until there is no work, and
   `backpressure_wait` drains first, so the `Notify` it would await is never needed.

Measured in Node 26, release build at `opt-level = "s"`: a map plus an integrated aggregate answers
correctly in 13 ms; 20,000 transactions of 100 rows, every fourth one retracting 100 earlier rows, go
through a spine in 4.3 s, and the native build of the same patched crate gives the identical relation.
The dbsp module with two small circuits is 3.57 MB raw and 679 KB brotli, about an eighth more than
the tab already downloads. Licences are MIT OR Apache-2.0 throughout.

nuthatch already runs every circuit with one worker (`init_circuit(1, ..)` in `views.rs`,
`exposure.rs`, `velocity.rs`, `entity_circuit.rs`), so single-threaded dbsp loses only the overlap
between merging and folding.

The spike is in `spike/dbsp-wasm/`; `vendor.sh` rebuilds the patched crates from the registry and the
patch file, which is the full diff.

~~**Not yet reached**: an entity-shaped circuit (a join over `entity_row.rs`'s `Row`),
`sharded_accumulator`'s backpressure waiter (`operator/dynamic/sharded_accumulator.rs:288`), which
awaits a `Notify` only a merger task signals, `complete_merges` and `retain_keys`. The rest of S0
covers them.~~ Reached, or shown to be off the one-worker path, in §12.

## 4. The design

**One dedicated worker runs the whole nest**: ingest, hot store, folds, sealing and queries. The page
talks to it over `postMessage`. It is single-threaded by necessity, not preference: Rust's wasm threads
need `-Zbuild-std` on nightly, the project pins 1.95 stable, and `SharedArrayBuffer` needs
cross-origin isolation, which would break every cross-origin RPC and the R2 mirror unless each sends
CORP headers. Burrmill in the tab is single-threaded already. The cost is that a long query holds up
the cursor and the other way round; the tab already runs statements one at a time.

**The hot store is redb over OPFS.** An `OpfsBackend` implements redb's five synchronous
`StorageBackend` methods over one `FileSystemSyncAccessHandle` (read and write at an offset, truncate,
flush, size), which matches exactly and is available only in a worker. RFC-0074's `Vec<u8>` backend is
the fallback where sync handles are missing. Both are backends behind a trait, as `CLAUDE.md` asks;
`Store::from_db` is the only constructor that changes. A nest's data is one OPFS directory per NID:
`nuthatch.redb`, `segments/<hash>.parquet` (where `/play` keeps segments now) and `manifest.json`.

The browser enforces single-writer for us: a second tab's `createSyncAccessHandle` on the same file
throws `NoModificationAllowedError`, which is a firmer lock than the flock the binary relies on.
`wireapp/redb-opfs` does this bridge but is GPL-3.0, so it is read for ideas only; ours is about 60
lines.

**Reorgs are the binary's.** `rollback_reorg` (`src/indexer.rs:7589`) is synchronous and touches only
the store and the views: read the meta keys, retract the entities in range through the circuits, roll
back factory children, commit one transaction. It ports unchanged once the views are driven inline. A
reorg below `sealed_through` stays a terminal fault, the same rule the tab follows for sealing now.
The tab gains what it lacks today: it can follow the tip, roll back what a fork took away, and seal
past finality as the binary does.

**The views are driven inline.** `BalanceView::start` and `EntityView::start_bound` spawn a thread and
a channel to keep circuits off the tokio pool. A `ViewDriver` trait with two implementations, the
threaded one the binary uses today and an inline one that steps the circuit on `apply`, is chosen by
the runtime and not by cfg. `flush()` is a no-op inline.

## 5. RPC and async

The tab fetches in JavaScript today and hands JSON to wasm. RFC-0074 went the other way, with Rust
owning the loop and a synchronous host import, which a browser cannot offer without `Atomics.wait`.
This RFC takes a third shape: **Rust owns the loop and awaits `fetch`**, polled by the browser's event
loop through wasm-bindgen-futures, which burrmill-wasm already uses.

`rpc.rs` splits into decisions and transport. The decision logic is free functions and per-endpoint
atomics with no I/O: `select_rpcs`, `classify_status`, `classify_rpc_error`, `looks_like_cap`,
`suggested_range`, `parse_retry_hint` and the pool's health, rests and heads. It moves unchanged. The
transport becomes `trait Transport { async fn post(url, body) -> (u16, Bytes) }` with reqwest in the
binary and `fetch` in the tab, and `RpcClient<T: Transport>` over it; timeouts go behind the same
trait.

`Source` is `#[async_trait]` on `Send + Sync`, and a `JsFuture` is not `Send`. The answer is
`send_wrapper`, as `burrmill-wasm/src/js_store.rs` already does, which is sound because the tab has one
thread (§4) and only because of that. Making the trait `?Send` under a cfg would fork business logic.

## 6. The crate boundary

The refactor is a crate, not cfg forks. **`nuthatch-core`** takes `NestIngest`'s window path
(`fetch_window`, then `process_window`: decode, `balances`, `exposure`, `velocity`, entities, commit,
seal) and `rollback_reorg`, over four traits:

- `Source`, which exists.
- `HotStore`, which exists; its two `spawn_blocking` commit wrappers become the synchronous calls,
  with the binary keeping its blocking-pool version. *(S1, as built: the core trait keeps
  `commit_window_blocking` async, and each store decides how to run it. The node keeps the blocking
  pool, so its commit fsync stays off the async workers (audit F-C3); a tab's store does the work
  inside the future. The trait has no cfg and the node's behaviour is unchanged.)*
- `SegmentSink`, for `seal_range`'s writes: bytes under `segments/<hash>`, then the manifest. The pure
  half is `nuthatch-seal` already.
- `Clock`, for the 52 clock and sleep sites, most of which are in the solo loop and pacing rather than
  the window path. A missed `Instant::now()` is a panic at run time on wasm, not a compile error, so
  each crate takes its time through one shim module and CI forbids `std::time::Instant` elsewhere.

Already pure and moving with it: `chains`, `chunker`, `factory`, `labels`, `alerts`, `freshness`,
`registry`, the `entity_*` modules, `views`, `exposure`, `velocity`, `source`. Staying in the binary
only: mounts, graft, migrate, publish, folds, graph, `watch`, the admin cockpit, the address-history
cursor and the process-global metrics, for which the tab gets a no-op `NestMetrics`.

rkyv needs `size_32` on wasm32, which changes dbsp's internal archive layout. Nothing in nuthatch
hashes or persists rkyv bytes and checkpoints are unused, so it is invisible; a native checkpoint
could never be loaded in a tab, which nobody wants.

## 7. The query surface

What `serve.rs` serves is axum handlers over `AppState`. The refactor is to pull each route body into
an axum-free `answer(request) -> response` over the nest state, as `sql_answer` already is. The binary
keeps its router and calls the same functions. The tab links the functions and offers them two ways:

1. **A JS API from the worker** (`nest.sql(q)`, `nest.entity(id)`, `nest.derived(name, key)`),
   returning the same JSON text the routes return. This is what `/play` consumes.
2. **A Service Worker** that answers the HTTP shapes for a path prefix on the page's origin and
   relays to the nest worker. Every existing client then works unchanged inside the page: the
   scaffolded skill's `fetch` calls, `llms.txt`'s examples, a dashboard. It is the HTTP API inside the
   page, not a server; nothing outside the browser can reach it.

MCP over stdio has no meaning in a tab. MCP over HTTP through the Service Worker is the same `answer()`
in another envelope, and is left for a user who asks.

## 8. Against the non-negotiables

1. **Single static binary.** Unchanged. The binary is built from the same crates and S1's gate is
   that it behaves identically; the tab is a separate artefact, as RFC-0065's is.
2. **Footprint.** A tab nest sits inside the per-cursor 2 GiB budget and under wasm32's 4 GiB space;
   the engine already refuses statements over 2 GiB, and 600 MiB on phones. S6 enforces both with a
   cursor running. Memory64 is not used: Safari does not ship it.
3. **No phone-home.** The tab talks to the RPCs the visitor chose and the mirror it reads, as `/play`
   does now. Nothing is sent about anyone.
4. **Determinism.** The same decode, the same circuits and the same sealer. The gates are byte-identical
   segments, row-identical entity state and the reorg property test converging under wasm (§9).
5. **Licence.** dbsp, the feldera crates, `web-time` and `send_wrapper` are MIT OR Apache-2.0.
   `redb-opfs` is GPL and is not used.

No RFC-0044 §8 record is needed: nothing new enters the default binary, which S1's gate holds to its
current behaviour, and the tab is a separate artefact as RFC-0065 is.

## 9. Slices and their gates

Each ends runnable, and each gate can fail. Sizes are days of focused work.

- **S0, dbsp on wasm32.** The kill question is answered (§3). What remains: lift
  `entity_circuit.rs`'s `lower()` plan, with its join over `Row`, into the spike and step 1,000
  windows. **Gate**: the relation equals the native build's, with no panic and no deadlock, wall time
  and peak wasm memory recorded. A path that blocks names the next patch. Also: open the upstream
  conversation with feldera (actix-web optional, the zip and object_store trims, the inline runtime
  behind a `single-thread` feature). About 1 day.
- **S1, `nuthatch-core`.** The window path and `rollback_reorg` behind `Source`, `HotStore`,
  `SegmentSink` and `Clock`, with the `ViewDriver` trait; the binary uses the crate through its
  threaded driver; the patched dbsp carried as a `[patch]` with a CI job that builds it for wasm32 and
  runs the step test. **Gate**: native sealing of a pinned range gives the same segment hashes before
  and after; `tests/e2e_reorg.rs` and `tests/e2e_entity_reorg.rs` are green;
  `cargo check --target wasm32-unknown-unknown -p nuthatch-core` runs in CI. 5 to 8 days, and the
  largest slice.
- **S2, the hot store in the tab.** `OpfsBackend` and the `Vec<u8>` backend behind `Store::from_db`;
  `nuthatch-core` driven from the `/play` worker over a `TapeSource` played from a recorded chain.
  **Gate**: the reorg property test runs under Node against the wasm build and converges for every
  depth band; a second tab opening the same store is refused. About 3 days.
- **S3, the live cursor.** `Source` over `fetch` through the `Transport` trait, following a young
  contract from its deploy block as RFC-0066 S2 does, now through `nuthatch-core`: hot store, the three
  built-in views, sealing from the store and pruning sealed rows. **Gate**: for the same contract and
  range, every segment hash equals the node's and `/balances` is row-identical; peak memory and bytes
  recorded in Chrome and Safari. About 4 days.
- **S4, entities and views.** `entities.toml` circuits built from the scaffolded nest and folded
  inline; `views/*.sql` answered over hot ∪ sealed through the ported `bind_relation`. **Gate**:
  `/derived/{entity}` and every authored view row-identical to native at the same watermark, including
  after a scripted reorg. About 4 days.
- **S5, the query surface.** Route bodies as `answer()`; the JS API, then the Service Worker.
  **Gate**: the scaffolded skill's `fetch` examples and `llms.txt` run unchanged against the page;
  `/sql`, `/entity`, `/derived`, `/balances`, `/explain` and `/ready` byte-identical to native for a
  fixed nest. About 3 days.
- **S6, size and memory.** `opt-level`, `wasm-opt`, and what dbsp drags in that a tab never calls
  (clap, the profiler, LIR); the 2 GiB and 600 MiB budgets enforced with a cursor and a query running
  together. **Gate**: module size published beside burrmill-wasm's 5.6 MB brotli; the Lodestar-class
  refusal still holds with a cursor running. About 2 days.

## 10. Risks

1. **The dbsp patch is ours to carry.** feldera releases about weekly (0.349.0 against our 0.320.0
   pin on 2026-10-28). The CI job in S1 catches a bump that breaks it; an upstream PR shrinks it. The
   inline runtime is the part that can rot.
2. **A dbsp path S0 did not reach.** The sharded accumulator's waiter is the likeliest; S0's second
   half exists to find it.
3. **Clocks.** A missed `Instant::now()` panics at run time. The shim-plus-lint rule in §6 is the
   guard.
4. **Quota and eviction.** Safari evicts an origin unused for seven days unless
   `navigator.storage.persist()` is granted; Chromium allows up to 60 % of disk, Firefox the lesser of
   10 % and 10 GiB. A tab nest is a cache of something re-derivable, and the page says so.
5. **Phones.** A backfill's unsealed tail and a heavy view share one 600 MiB space. S3 measures it.
6. **Public RPCs cap history.** A contract older than about eight days needs a full-history RPC, as
   RFC-0065 §5 and RFC-0066 §12 found. Unchanged here.

## 11. Non-goals

Threads (nightly only). Memory64 (not in Safari). Publishing what a tab seals (RFC-0065 §7). Anything
hosted or any account. Folds and the `graph` feature. Address history, which has RFC-0074's module.
MCP over stdio.

## 12. S0 measured outcome, 2026-10-28

The remainder of S0 (#2476): the binary's own entity circuit on wasm32.

**What was lifted.** `src/entity_circuit.rs`, `entity_plan.rs`, `entity_expr.rs` and `entity_row.rs`
are compiled into the spike by `#[path]`, unchanged, as RFC-0074 compiled the address-history
files: the plan stepped is the one a nest lowers (`map_index` → `join_index` → `aggregate_linear`,
`aggregate(Min)`, `aggregate(Max)` and the `outer_join_default` graft, over the rkyv-archived
`Row`), not a copy of it. The harness (`spike/dbsp-wasm/src/entity.rs`) declares
`transfers ⋈ labels on to = address`, grouped by `(to, tier)`, with `count`, `sum`, `avg`, `min` and
`max` over the value, a left filter dropping zero values and a right filter dropping tier 0. It steps
1,000 windows of 100 transfers; every fourth window retracts 50 earlier transfers, every fiftieth
retracts a label and re-inserts it with a new tier, so both inputs see weight `-1` and a tier change
moves a whole group. One in sixteen values is NULL. The oracle is `Plan::evaluate`, the batch fold the
binary's own tests hold the circuit to (RFC-0041 §8), checked every 250 windows and at the end.

**Gate: passed.** In Node 26, release build (`opt-level = "s"`, thin LTO):

| | wasm32, Node | native aarch64 |
|---|---|---|
| windows / rows | 1,000 × 100 | the same |
| facts | 75,000 inserted, 12,500 retracted; labels 531 inserted, 19 retracted | the same |
| groups | 504 | 504 |
| relation digest | `d4b83ac16862b79d` | `d4b83ac16862b79d` |
| oracle agreed at every checkpoint | yes | yes |
| wall time | 1,996 ms (debug build: 13,069 ms) | 1,034 ms (release), so wasm is 1.9x |
| wasm linear memory | 14.3 MiB before, 53.9 MiB after, which is the peak (linear memory never shrinks); 38.6 MiB at window 500 | |
| dbsp's own profile of the circuit's state | 36,970,128 bytes | 38,174,568 bytes |
| module after wasm-bindgen | 5,849,145 bytes raw, 944,210 brotli (3.57 MB / 679 KB before the entity circuit) | |

The digest is SipHash over the relation rendered as text, so a 32-bit and a 64-bit build hash the
same bytes. The profile's state sizes differ by 3 % because rkyv archives sizes as 32-bit on wasm32
(§3); the relation is the same.

No panic and no deadlock; nothing in the dbsp patch changed for this half of S0, so
`rfc-0075-dbsp-wasm32.patch` is as §3 left it.

**The three paths named in §3.** None is on the path a nest runs:

- `sharded_accumulator`'s backpressure waiter (`operator/dynamic/sharded_accumulator.rs:288`) is
  reached only through `ShardedAccumulator`, and `dyn_shard_accumulate` takes `dyn_accumulate`
  instead when `Runtime::num_workers() == 1` (`sharded_accumulator.rs:60-63`). Every circuit the
  binary builds is `init_circuit(1, ..)`. The one-worker accumulator inserts into a `Spine`, whose
  `backpressure_wait` the patch drains synchronously, and that path ran here.
- `complete_merges` has no caller in dbsp outside its tests.
- `retain_keys` is reached only through `integrate_trace_retain_keys`, which none of the binary's
  circuits calls (`entity_circuit.rs`, `views.rs`, `exposure.rs`, `velocity.rs`).

A multi-worker layout under the single-thread runtime would reach the first; it should be refused at
`Runtime::run` rather than supported, and the upstream draft says so.

**A finding about the binary, not the tab.** `Plan::evaluate`'s accumulator (`entity_plan.rs`,
`Acc`) keeps one `sum`, one `min` and one `max` per group, so a plan with two `Sum`s folds both into
one field, while the circuit keeps a slot pair per aggregate (`LinAcc`). The harness's first plan had
`sum(value), sum(tier)`, and the circuit was right where the oracle was wrong. `Plan::evaluate` is
test-only (`grep -rn "\.evaluate(" src` finds only `entity_circuit.rs`'s and `entity_plan.rs`'s
tests), and no test declares two aggregates of one kind, so nothing a nest serves is affected. It
wants its own issue as a test-oracle defect; it is not fixed here.

**Upstream.** The feldera issue and the four PRs are drafted in `spike/dbsp-wasm/UPSTREAM.md` and
not posted.

## 13. S2 outcome, 2026-10-29

S2 (#2478): the hot store in the tab.

**What moved.** The binary's redb store is `nuthatch_core::redb_store::Store<F>`, where `F` is the
feed's tap (RFC-0073 FR1). The node's `Store` is `Store<feed::Tap>`. The tab's is `Store<NoFeed>`,
which numbers commits and records nothing. The feed's snapshot reads stay in the binary because
their receivers are tokio's. `Store::from_db` is public and takes a `Database` over any backend, as
§4 says. `detect_reorg` moved into `cursor` beside `rollback_reorg`. A commit runs on tokio's
blocking pool when a runtime is current, so the node is unchanged, and inline when none is, as in a
tab or a bare test.

**The backends.** `backend::Bytes` is RFC-0074's `Vec<u8>` backend, which was not in the tree, so
it was written again here. `opfs::OpfsBackend` implements redb's five methods over one
`FileSystemSyncAccessHandle`. It was written from redb's trait and the File System Standard, and
`redb-opfs` was not read. A `NoModificationAllowedError` from `createSyncAccessHandle` becomes
`OpenError::Locked`. A worker without sync handles or OPFS gets `OpenError::Unavailable`, and the
`/play` entry point (`PlayNest`) falls back to `Bytes` on that error alone.

**Gate: passed.**

- *Reorg property, Node against the wasm build.* `tests/e2e_reorg.rs`'s property and its
  `reorg_depth` strategy, unchanged, run through core's `detect_reorg`, `rollback_reorg`,
  `fetch_window` and `process_window`. The store is the real redb store on `Bytes`, the source a
  `TapeSource`, the windows four blocks wide. Proptest's deterministic ChaCha runner draws 12 depths
  and each band's edges are added, which makes 18 cases: shallow 1-2, mid 3-12, and deep 13-23,
  including depth 23, a fork below every checkpoint. Every case converges. The tip row carries the
  replacement hash, the cursor resumes after the checkpoint the node's would, and the hot rows and
  balances equal a clean run over the post-reorg chain. The transcript's sha256 is
  `4f3b8bec…6e96592a` in Node 26 and natively, so the two builds converged identically. Node takes
  227 ms and native 248 ms. Linear memory is 58.9 MiB at the end of `run-node.mjs`.
- *A second tab is refused.* Under headless Chrome for Testing 156, in a dedicated worker (as
  `tests/opfs.rs` declares), there are three tests. A store written, closed and reopened on OPFS
  keeps its meta, rows and checkpoint. A second `OpfsBackend::open` of a held store is
  `OpenError::Locked`. A second worker's `createSyncAccessHandle` on the held file is refused with
  `NoModificationAllowedError` and succeeds once the handle drops. CI runs the three tests in
  `wasm-core` on Chrome for Testing.
- *Both mutations fail.* Dropping the balance retraction in `rollback_reorg` fails the property at
  the first case. Mapping the DOM error to anything but `Locked` fails the lock test.

**What it does not show.** The second worker stands in for a second tab. The lock belongs to the
file and not to the context, so this is the same refusal, but no test opens two tabs. Nothing seals
in the property: a fork below `sealed_through` is a terminal fault by design, and the deep band
reaches block 1. Sealing from the store is S3. The module is 10.9 MB raw and 1.78 MB brotli after
wasm-bindgen, against S0's 5.8 MB and 944 KB, with redb, proptest and the S2 harness added. S6 is
where that gets cut. Wiring `PlayNest` into the `/play` page is a change to the frontend repo, and
it has not been made.

## 14. S3 outcome, 2026-10-29

S3 (#2479): the live cursor.

**What moved.** The RPC client is `nuthatch_core::rpc::RpcClient<T: Transport>`, and the node's is
`RpcClient<Http>` over reqwest. The decision logic moved as it was: endpoint order, health, rests,
retries, batching, splitting and classification. What changed in the move is the bytes (`post` and a
`Reply`), the request deadline (`Transport::build`), the waits (`time::sleep` and `timeout`, which are
tokio's natively and `setTimeout` in a tab), the Prometheus counters (hooks the node implements) and
visibility. `src/rpc.rs` keeps its two test modules byte for byte: 90 tests, plus chunker's 21, pass
as they did. The tab's transport is `nuthatch_core::fetch::Fetch`, which holds every JS value in a
`SendWrapper`. Its deadline is `AbortSignal.timeout`, and it covers the body as reqwest's does.

**The gate's chain.** It is a recording, so CI needs no network. A young mainnet ERC-20,
`0x0c26fe5e579c3b007ba16d921ba8204a47c05f1a`, was recorded from its deploy block 26,239,807 to
26,245,870 from two keyless public RPCs (`tests/fixtures/tab-parity/record.sh`). That is 6,065 headers
(hash and timestamp) and 2,259 Transfers, 430 KB gzipped. `fixture-rpc recorded` serves it over HTTP,
and refuses any block outside it. The node runs `nuthatch dev` on an `init`ed nest
(`tests/e2e_tab_parity.rs`). The tab runs the wasm build: `TabNest` with mainnet's finality and seal
span, the three views inline, and a sink with the node's table floor. Both are held to one pinned
`expected.json`.

**Gate: passed for Chrome, not run for Safari.**

| | node (`nuthatch dev`) | tab, Node 26, `Bytes` store | tab, Chrome for Testing 156, OPFS store |
|---|---|---|---|
| segment `tab__transfer` 26,239,813..=26,241,612, 2,259 rows | `c983470c…479aeae` | the same | the same |
| sealed through / last block | 26,245,806 / 26,245,870 | the same | the same |
| `/balances` (1,360 holders, the 1,000 served) | pinned | row-identical | row-identical |
| hot rows after the seal | | 0 | 0 |
| wall time | | 945 ms | 940 ms |
| wasm linear memory at the end, the peak | | 30.6 MiB | 27.8 MiB |
| RPC requests, bytes received / sent | | 1,433, 1,673,693 / 163,741 | the same |
| segment bytes held | | 88,012 | 88,012 |

**The gate failed first, for a real reason.** The spike's lockfile had parquet 59.3.2 and the
binary's had 59.3.0. parquet writes its version into each file's `created_by`, so the rows were equal
and the bytes were not. Pinning the spike's arrow and parquet to 59.3.0 made them equal. A tab must
therefore ship the node's parquet, to the patch version. This gate catches it, and nothing else does.

**Safari** was not measured. `safaridriver --enable` asks for an administrator's password, and
without it the WebDriver session is refused. Chief, to run it once enabled: start
`fixture-rpc recorded --port 18947 --file tests/fixtures/tab-parity/chain.json.gz`, then from
`spike/dbsp-wasm` run `SAFARIDRIVER=/usr/bin/safaridriver TAB_PARITY_RPC=http://127.0.0.1:18947/
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner cargo test --release --target
wasm32-unknown-unknown --test live -- --nocapture`. `vendor.sh` must have run first, and the wasm32
C toolchain must be set as CI sets it.

**What it does not show.** The contract's Transfers all fall within its first 1,800 blocks, so there
is one segment, and it is final. A table's provisional segment, folded back through `segment_rows`,
is not exercised by this chain. The recorded headers carry no transactions. Node has no OPFS, so
there the store is `Bytes`; in Chrome it is OPFS, opened as `PlayNest` opens it. No run followed a
live public RPC; the recording was taken from public RPCs, and that is all.

## 15. S4 outcome, 2026-10-29

S4 (#2480): entities and views.

**What moved.** `entities.toml`'s declaration is `src/entity_decl.rs` (`EntityDecl`, `parse`, and
`sql_path`, the rule `read_sql` applies), and `uses_sample` and `sql_tokens` moved from
`entities.rs` into `entity_lower.rs`, their one other caller. Nothing else in the binary changed. The
spike compiles `entity_decl.rs`, `entity_lower.rs` and `entity_bind.rs` by `#[path]`, unchanged, as
S0 compiled the circuit, so an entity is declared, lowered and bound by the binary's own code, then
folded on `nuthatch-core`'s `EntityLane` with `Driver::Inline`. The tab has no offchain tables
(§11), so `entity_bind.rs`'s offchain imports resolve to uninhabited types, and an `offchain__` source
gets the binder's own "no offchain table" refusal. A warm store's entities are unavailable, with the
node's reason.

**The query side.** The binary's pinned Burrmill rev (`ea8cf40`) already carries the wasm32 tail:
`attach_store`, `register_store_facts`, which reads footers by range from an object store, and
`sql_async`. `burrmill-wasm` itself, the JS package, is not used; it is not in that rev. `query.rs`
ports `analytics::define_views_bound`'s table loop: schema ∪ manifest ∪ hot, segments at or below
`sealed_through`, hot rows past it where cold covers the table. It also ports `bind_relation` onto
that tail, with the cellar's segments in an in-memory store. Views come from each `CREATE VIEW` in
`views/*.sql`. There is a real difference in execution. The node binds segment files, and Burrmill
substitutes its owned fold for their aggregates. A store-backed table declines that substitution,
so the tab's aggregates are DataFusion's plans. The gate holds the answers of the two paths equal.

**The gate's chain.** It is S3's recording, with a scripted reorg added to `fixture-rpc recorded
--fork`. Before the fork, the last ten blocks carry 17 synthetic mints, and `POST /control/fork`
replaces the last six and adds two, with different recipients and amounts. The nest
(`tests/fixtures/tab-parity/s4/`) declares `received` (by `to`: count, sum, min and max block) and
`sent` (by `from`, filtered), plus three views: one over the facts by thousand-block bucket, one over
an entity, and a join of the two entities. The node runs `nuthatch dev` (`tests/e2e_tab_entities.rs`).
The tab runs the wasm build in Node (`run-entities.mjs`). Each reads `/derived/{entity}?limit=1000`
and `SELECT * FROM <view>` at the tip, then again after the fork once the new tip has been applied.
Rows are compared whole, ordered by their key-sorted text, and both are held to
`s4-expected.json`.

**Gate: passed in Node, not run in Chrome or Safari.**

| | before the fork (tip 26,245,870) | after (26,245,872) |
|---|---|---|
| `received` / `sent` rows | 722 / 645 | 727 / 645 |
| `per_kblock` | 3 rows: 703, 1,556 and 17 transfers | 3 rows |
| `busy_receivers` / `two_way` | 242 / 2 | 240 / 2 |
| sealed through | 26,245,806 | 26,245,808 |
| tab wall time | 793 ms from deploy block to answers | 32 ms |
| wasm linear memory at the end | | 39.3 MiB |

The views read both halves. `per_kblock`'s last bucket is the 17 hot mints, and its first two are the
sealed segment. The module is 67.3 MB raw (48.3 MB of code and 14.1 MB of names) and 8.39 MB brotli,
against S2's 10.9 MB and 1.78 MB. That is DataFusion, and S6 is where it gets cut.

**Both mutations fail.** Skipping `retract_entities` in core's `rollback_reorg` fails `after.derived`.
The rolled-back mints stay counted, and a recipient below the fork is counted twice, since the
rollback resumes below the fork. Binding no sealed segments in `query.rs` fails `before.views`.

**A finding in the spike.** With Burrmill in its graph, the spike resolves `serde_json`'s and
`parquet`'s features exactly as the binary does (`cargo tree -e features`, identical lists;
`arbitrary_precision` arrives with Burrmill). Burrmill did not move any segment hash. The two
synthetic segments that `tests/native.rs` and `run-node.mjs` pin moved in S3, when the spike's
parquet went from 59.3.2 to the node's 59.3.0 and the version string written into each file changed
with it. S3 carries the re-pin.

**What it does not show.** Neither S4 run was in Chrome or Safari. The view port skips what the node
does for a degraded or missing segment, a historical window, the `wanted`-table pruning,
`load_relation`'s range checks, and the rewrite of dates to ISO text. No view here returns a date. A
view's body is sqlparser's rendering of the query, not the file's text as `view_body` slices it.
Entity state is unbounded in the tab (`allowance_bytes` is `u64::MAX`, never sampled), which is S6's
budget.
