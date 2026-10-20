# Benchmarks (RFC-0004)

Two harnesses: **backfill** (the write path - this page's main subject) and **query** (the read path,
[below](#query-benchmarks-the-read-path)).

**House rule:** every performance number nuthatch publishes traces to a committed
`docs/bench/*.json` - with **commit**, **provider**, and **hardware** (and date when the harness
recorded one). No hand-typed numbers, including flattering ones. `tests/bench_citations.rs` fails
the build when a citation on this page has no such file, or when the file is missing those fields.
This page documents the harness and the pinned workloads; the baseline matrix is filled in from
real runs (an archive node is needed for the historical ranges).

## The harness

```sh
nuthatch bench backfill --dir <nest> --from <block> --to <block> [--runs 3] [--rpc <url>] [--out report.json]
```

Runs the real **fetch → decode → store** path over a pinned block range and reports the **median**
across runs of:

- **events/sec** - total decoded events ÷ wall-clock (excluding init). The headline.
- **wall-clock (s)**, **peak RSS (MB)**, **RPC requests** (including failover retries).

It writes to a throwaway store per run (never the nest's own DB), so runs are independent and
repeatable. `--rpc` overrides the nest's endpoints - point it at your own node for a T2 run.

Nothing here optimises anything. It exists so the seal-direct / adaptive-chunker / pipeline work
(later RFC-0004 slices) is each gated on a measured before/after, not a wish.

## Backfill throughput is tracked, not gated

CLAUDE.md sets a backfill floor of 10K events/s, aiming for 30K. **No CI check fails on events/sec.**
The `backfill throughput · tip lag` job runs `.github/workflows/backfill-throughput.sh` on every PR: three
batches of 15 over the chain `fixture-rpc footprint` serves (blocks 1 to 20,000, 80,000 rows, seal-direct,
concurrency 4). It reports the median to the step summary and an artifact, and fails only if a run
did not decode all 80,000 events. #1723 measured the figure before choosing that shape.

The runner's figure is stable. Five batches on `ubuntu-latest` (4 cores, 16 GB) gave medians from
23,735 to 24,770 ev/s, a 4.2% spread, median 24,446 ([report](bench/backfill-throughput-runner.json)).

It cannot see the regressions a floor would be there to catch:

| change | effect on the median | report |
| --- | --- | --- |
| concurrency 1 instead of 4, same runner | **+19%** (29,185 ev/s) | [runner c1](bench/backfill-throughput-runner-c1.json) |
| `decode_window` decodes every log 4 times, dev box | -3.1% (98,391 to 95,338) | [base a](bench/backfill-sensitivity-base-a.json), [mutant a](bench/backfill-sensitivity-decode4-a.json) |
| the same mutation, second pair | -1.4% (96,959 to 95,611) | [base b](bench/backfill-sensitivity-base-b.json), [mutant b](bench/backfill-sensitivity-decode4-b.json) |

A floor tight enough to see a 4x decode cost would sit inside the runner's own batch spread. A
floor loose enough to survive that spread passes the decode mutation, and losing the pipeline
reads as an improvement. When this was measured the fixture was a single-threaded script sharing
four cores with the binary, so the figure is mostly the fixture and the fetch, not nuthatch. The point-read gate states the
rule (ci.yml): a ceiling that passes the known break is decoration. A throughput gate needs a
scenario where nuthatch's own work dominates the wall clock. Until there is one, the floor is a
design target measured by hand with `bench backfill`, not a gate.

## Tip lag is gated on its after-seen median

The `backfill throughput · tip lag` job also runs `.github/workflows/tip-lag.sh` on every PR. `fixture-rpc footprint --moving` serves
the footprint chain with a tip the script advances one block at a time, and `dev --poll-interval 1s`
follows it. For 100 blocks the script records two figures: **lag**, from the block appearing to its
four rows answering through `/sql`, and **after seen**, from nuthatch's first `eth_blockNumber`
reporting the block to the same answer. After seen is the fetch, decode and commit without the wait
for the next poll. Both go to the step summary and the artifact
([runner report](bench/tip-lag-runner.json)). The job fails when the after-seen p50 exceeds 30 ms, or
when a block's rows are not queryable within 30 s.

#1884 measured the noise and then put a sleep before the hot-store commit, behind an environment
variable in a throwaway build, to see what each figure could detect. Three batches of 100 per row,
`ubuntu-latest` (4 cores) and the ThinkPad (32 cores). The 4.7.0 row includes the throwaway build
run with a 0 ms sleep:

| build | after-seen p50, runner | after-seen p50, ThinkPad | lag p50, runner |
| --- | --- | --- | --- |
| 4.7.0 | 17, 18, 18, 19 ms | 19, 20, 21, 19, 25, 23 ms | 497-604 ms |
| +20 ms before the commit | 38, 38, 39 ms | 42, 43, 43 ms | 509-600 ms |
| +50 ms before the commit | 66, 67, 68 ms | 71, 74, 71 ms | 561-615 ms |

Of those cells, the runner's 4.7.0 after-seen p50 of 17 ms is the committed artifact; the other
batches and the throwaway builds are recorded on #1884 and have no `docs/bench` file.

The after-seen median sees a 20 ms delay with no overlap on either machine, so it carries the
ceiling, set at 30 ms between the runner's 19 and 38. The lag median moves about 100 ms between runs
on where the block lands in the poll interval, more than a 50 ms delay moves it, so it is tracked
and not gated. Both p99s are tracked: the runner's after-seen p99 ran from 32 to 229 ms at a fixed
build, which is preemption rather than nuthatch.

## Workloads (pinned, public, reproducible)

| ID | Nest | Range | Character |
|----|------|-------|-----------|
| W1 | USDC (mainnet) | 100,000 blocks ending 21,400,000 | dense single-contract (~1.3M events) |
| W2 | [horizon-nest](https://github.com/cargopete/horizon-nest) (Arbitrum) | full history from deployment | sparse multi-contract, L2 cadence |
| W3 | USDC + WETH + Uniswap V3 factory (mainnet) | 50,000 blocks | mixed density, multi-table fan-out |


## What a backfill costs against a metered endpoint

Throughput is not the only number an operator cares about. "Be your own indexer" is partly an argument
about bills. Header CU is **20** for `eth_getBlockByNumber` - Alchemy's published schedule, confirmed
by a counting proxy in front of a paid key (#765). `eth_getLogs` is 60 CU, `eth_blockNumber` is 10 CU.
`$0.45` per million CU for the first 300M/month. Same rates as [`operators.md`](operators.md).

**The previously published ~$1,192 full-history Uniswap V3 extrapolation is withdrawn.** It was
derived from an estimated ~3.6M CU over 674k blocks, which cannot be right at 20 CU/header: a BSC
row on the same table claimed ~3.0M CU total for ~180k event-bearing blocks, and `180,000 × 20 =
3.60M` CU for headers *alone*. No committed method breakdown supports either the 3.6M input or the
$1,192. #765's live `uniswap-v3` Arbitrum catch-up (61,709 headers / 171,509 blocks) is a *different*
workload class - post-factory-flip, topic0-only, inflated by headers of logs later discarded - and
must not be silently substituted.

Price a backfill from a counted run (`nuthatch_rpc_methods_total` × the schedule above), not from
this page. Steady-state **tip-following** on Arbitrum is computed in operators.md at **~$134/month**
at those same rates, of which ~$93 is header fetches.

### The finding: our own default is the cost, not the indexing

**Headers dominate.** #765 measured 99.5% of CU on `eth_getBlockByNumber` for a factory catch-up:
61,709 headers vs 110 `eth_getLogs` vs 24 `eth_call`. The cause is `block_timestamps = true` (the
default): one header fetch per block that still has a kept row, plus retries when a provider returns
a partial batch (`block_timestamps: N/M block(s) missing - refusing a partial map`).

A topic0-only factory fetch used to stamp **every** topic0 match on the chain, including other
protocols that share the event shape, then discard those rows. That nest kept 1,627 event-bearing
blocks and bought ~200,000 headers. As of pragmatic-peregrine the stamp follows local filtering:
only blocks that produced a kept row (plus `[[calls]]` sample blocks) are fetched.

`block_timestamps = false` (RFC-0029 §6b) removes the header term entirely, at the cost of the
column. A nest that never asks "when" should not be paying for it.

**A full-history backfill on a busy factory gets priced and agreed before it is started, not after.**
Two guards worth having on the provider side rather than in a habit: a **spend limit** (the "Set
limit" control beside each usage chart) turns an accident into a refusal, and a **custom throughput
override** turns a concurrency spike into throttling rather than billing.

### The other lever: concurrency, not volume

The only limit actually hit that day was **throughput, not spend**: 11,717 CU/s against a 10,000 cap,
caused by two backfills running at once rather than by either one being large. A provider-side
throughput cap turns that into throttling instead of billing, and costs nothing to set.

## Sourcing tiers

- **T1** - public RPC defaults (round-robin), as a new user experiences it.
- **T2** - your own node (localhost `eth_getLogs`), via `--rpc`.
- **T3** - a caching proxy (e.g. erpc) in front of T1.

W1/W3 historical ranges and W2's full history require an **archive** node (public sequencer/
non-archive endpoints don't serve old `eth_getLogs`). Public-RPC (T1) numbers are noisy - take the
median of three runs, date them, and read them as "what a new user should expect," not as the
product's capability.

## Seal-direct (`--seal-direct`)

For ranges already past finality - nearly all of a backfill - rows can go straight to sealed Parquet,
bypassing the hot store: decode → buffered rows → content-addressed segments, no redb write, no
read-back, no prune. The bounded buffer caps RSS by construction. `seal_range` is the one shared
writer, so a given range yields **byte-identical** segments whether sealed directly or via the hot
store (asserted by `seal::seal_direct_matches_seal_via_hot_store`).

Measured before/after. At the time of this run, `--seal-direct` changed two things at once, not one:
the storage path, and the fetch-windowing strategy (`window_adaptive: args.seal_direct` - no flag
separated them, so this table could not isolate the storage path alone). On this range both arms did
34 requests, so the windowing difference never actually diverged the two runs here, and the
sequential comparison below is unaffected. **`--window-adaptive` (#744) now decouples the two** -
hot-fixed, hot-adaptive, seal-fixed and seal-adaptive are all reachable independently - but the runs
below predate the flag and have not been re-measured with it; treat this table's windowing caveat as
historical, not as a live gap. Artifacts:
[`docs/bench/722-hot.json`](bench/722-hot.json), [`docs/bench/722-seal-direct.json`](bench/722-seal-direct.json).
`722-seal-direct.json` records `window_adaptive: true`, so now that `--seal-direct` alone is the
fixed-window arm, reproducing that artifact takes `--seal-direct --window-adaptive`, not
`--seal-direct` by itself.

| Path | Range | Events | Wall-clock | events/sec | RPC requests |
|---|---|---|---|---|---|
| hot store (decode → redb) | USDC, blocks 25,809,368 to 25,809,487 (public RPC) | 11,758 | 4.11 s | **2,860** | 34 |
| seal-direct (decode → Parquet) | same | 11,758 | 4.46 s | **2,634** | 34 |

**No speedup measured today - if anything ~8% slower (0.92×).** That contradicts both this page's own
prior entry (5.2×, measured hours earlier the same day, same commit lineage) and the architectural
reasoning below it: a buffered bulk Parquet write shouldn't lose to a redb B-tree point-insert plus
fsync per row. This is not published as a correction to 5.2× - it's a discrepancy neither number
explains. Reproduced twice, back-to-back (a 5-run and a separate 3-run session, both landing in the
same ~2,600-2,900 ev/s band for *both* paths): not a single noisy sample. Leading suspect is that this
ran on a heavily shared dev box with two other agents' `cargo test`/`cargo build` active during the
measurement window (load 4.8-6.4 on 32 cores) - redb's many small synchronous writes are exactly the
operation most exposed to shared disk contention, more than seal-direct's few large buffered ones.
**No longer unconfirmed - and the leading suspect above was wrong.** The controlled re-run happened on
2026-08-23 against the RFC-0039 replay rig (#767), which removes the network entirely. It settles
#744, and not in the direction anyone expected:

| the same workload, same tape, same box | wall clock | events/sec |
|---|---:|---:|
| live, against `eth-pokt.nodies.app` | 20.7-21.3 s | **553-568** |
| replayed from disk, network removed | **0.15 s** | **77,640** (median of 8) |

**The network was 99.3% of the live wall clock.** Every seal-direct-versus-hot-store ratio this page
has ever published was therefore reading a *0.7% signal through 99.3% noise* - which is the whole
explanation for a figure that read 8.7x in July, 5.2x one morning and 0.92x the same afternoon. The
storage-path delta was never measurable through a public endpoint at all. It was not a shared box, or
not mainly; it was that the quantity being compared was two orders of magnitude smaller than the
thing being measured.

The replay arm's own spread is **1.029x** across 8 runs, against the 3.8x measured live - and the
event count is *identical* on every run, which is the mechanism working rather than a secondary
observation. It does **not** yet meet #767's own ±2% acceptance bar: worst deviation from median is
2.03%, and the residue is box jitter, which the rig explicitly does not claim to fix. Stated as a
miss rather than rounded down to a pass.

**What this means for the multipliers on this page: they are not comparable to a replayed number and
never were.** A live figure measures a provider; a replayed figure measures decode plus store. Both
are honest; they answer different questions, and a ratio built from one cannot be restated in terms
of the other.

**The storage-path question, answered on the rig.** The 2026-08-23 replay aborted the seal-direct
arm on a recorded 429, which is the error-preservation mechanism working and also means the
comparison did not run. A second tape of the same 120-block Transfer-only range, recorded against
an endpoint whose timestamp batches all succeed, is
[`docs/bench/tapes/usdc-120-fixed-clean`](bench/tapes/usdc-120-fixed-clean). Both arms replay it,
fixed window, `--keep` on disk, five-run median, commit `9b3bb6f`, 18 cores / 48 GB RAM. The
429-bearing tape stays; it is the #784 reproduction. Artifacts:
[`docs/bench/744-hot-replay.json`](bench/744-hot-replay.json),
[`docs/bench/744-seal-direct-replay.json`](bench/744-seal-direct-replay.json).

| Path | Events | Wall-clock | events/sec | vs hot store | RPC requests |
|---|---|---|---|---|---|
| hot store (replay) | 11,758 | 0.16 s | **74,978** | 1× | 12 |
| seal-direct (replay) | 11,758 | 0.06 s | **185,628** | **2.48×** | 12 |

Same event count on every run of both arms. Same 12 source calls. The only variable is where the
rows are written. Seal-direct is 2.48× the hot store once the network is not in the measurement.
That is smaller than 8.7× or 5.2×, larger than 0.92×, and is the first of those figures that was
measuring the storage path.

Reproduce it:

```sh
nuthatch bench backfill --dir docs/bench/nests/usdc-120 \
  --from 25809368 --to 25809487 --runs 5 \
  --replay docs/bench/tapes/usdc-120-fixed-clean --keep /var/tmp/nuthatch-744-hot
nuthatch bench backfill --dir docs/bench/nests/usdc-120 \
  --from 25809368 --to 25809487 --runs 5 --seal-direct \
  --replay docs/bench/tapes/usdc-120-fixed-clean --keep /var/tmp/nuthatch-744-seal
```

**12,933 was not wrong - it answers a different question.** This bench nest declares `Transfer`
only, so 11,758 is the right count for *this* table; 12,933, reported by this PR's own earlier
commit and separately by Iris's independent replication, is the count of every log at the contract
address, which is what an `init`-scaffolded nest declares (every event in the ABI) over the same
range. Verified directly against `eth_getLogs` with no nuthatch binary involved, blocks
25,809,368 to 25,809,487 in four 30-block chunks summed, on two independent providers (`eth.drpc.org`,
`eth.api.onfinality.io/public`) that agree exactly: every log at `0xa0b8…eB48` = 12,933;
`topics[0] = Transfer` = 11,758; `Approval` = 1,046; everything else = 129.
11,758 + 1,046 + 129 = 12,933 exactly, with nothing left over. Two nests, two workloads, two correct
counts - a reader who runs this same range through `init` instead of this Transfer-only bench
config should expect 12,933, not 11,758. This closes item 1 of #744: there is no
retry-without-dedup bug to hunt.

**Provenance:** measured 2026-08-22 at commit `6145386`, 5 runs (median reported), provider
`eth-pokt.nodies.app` (`BenchReport.provider` - the first of the nest's RPC pool; public endpoints are
noisy and interchangeable within a run), 32 cores / 62 GB RAM. **Caveats:** public-endpoint throughput
varies several-fold run to run - Iris's own hot-store check on 2026-08-22 saw a 3.8× spread inside one
arm (172 ev/s on run 1 against 658-649 ev/s on runs 2-3) - so absolute ev/s is "what this provider gave
today," not a target. Both arms here declare no `[[calls]]`, so this is a bare-event workload and a
nest with `[[calls]]` will not reach these figures - `BenchReport.calls_declared` is the field that
would prove that from the artifact rather than the prose, but these two were measured at `6145386`,
before #742 added it, so they predate `calls_declared: 0`. (#725 - every seal-direct path hardcoding
an empty calls slice regardless of what the nest declared - is closed; `bench` now refuses a
declared-`[[calls]]` nest run without `--state-rpc` outright, the `state_rpc` guard in `src/bench.rs`. #743 closed the
same hole in the *hot* arm, which is this table's denominator: until then it took no `calls`
parameter at all, so a calls nest would have paid tier-3 cost in the numerator and not in the
denominator, and the ratio would have flattered seal-direct by exactly the work the hot arm skipped.
Both arms resolve calls through the same `resolve_calls_for_window` now, so a calls-nest comparison
is like for like. No figure on this page moves: both arms below declare none.) The earlier
8.7× figure (2026-07-16, `f1a57de`) was measured against a harness that called `put_entity` per row -
one redb write transaction and one fsync per row -
rather than the `commit_window` path the indexer uses. #224 (`0cd291e`, 2026-07-30) fixed the harness;
8.7× was an upper bound against that strawman, not a measurement of the current code. Run it yourself:

```sh
# --window-adaptive on the seal-direct arm because that is what the artifact above recorded
# (`window_adaptive: true`); since #744 decoupled the two flags, `--seal-direct` on its own is the
# fixed-window arm and reproduces a different run.
nuthatch bench backfill --dir <nest> --from A --to B                                    # hot store (baseline)
nuthatch bench backfill --dir <nest> --from A --to B --seal-direct --window-adaptive     # seal-direct
```

## Pipeline (`--concurrency K`, seal-direct only)

Once the storage path is cheap, wall-clock is dominated by sequential `getLogs` round-trip latency.
The pipeline fetches `K` windows concurrently (`futures::stream::buffered`) but consumes results
**in block order**, so the sealed segments are byte-identical to the sequential path (asserted by
`indexer::pipelined_backfill_matches_sequential`). Bounded in-flight windows cap RSS.

Stacked measured result (USDC, same 120 blocks, public RPC). Provenance: 2026-08-22, commit
`6145386`, 5 runs (median), provider `eth-pokt.nodies.app`, 32 cores / 62 GB RAM. Artifact:
[`docs/bench/722-pipeline-8.json`](bench/722-pipeline-8.json).

| Path | events/sec | vs hot store | RPC requests |
|---|---|---|---|
| hot store (decode → redb) | 2,860 | 1× | 34 |
| seal-direct | 2,634 | 0.92× | 34 |
| seal-direct + 8-way pipeline | **8,315** | **~2.9×** | 24 |

```sh
nuthatch bench backfill --dir <nest> --from A --to B --seal-direct --concurrency 8
```

The pipeline itself still shows a real speedup - concurrent fetch genuinely overlaps public-endpoint
round-trip latency, ~3.2× over single-threaded seal-direct here, bounded by this provider - though part
of the ~2.9× vs. hot store is fewer, wider requests (24 against the hot arm's 34) rather than the
overlapped latency alone; against your own node (`--rpc`, `--concurrency 16`) it goes further. What
collapsed is the *storage-path* half
of the old ~22.5× (hot store vs. pipeline): see the seal-direct-vs-hot-store discrepancy above - see
also #744, now measured on the rig at 2.48× rather than collapsed to 0.92×. Today's live hot-store
baseline came in far faster than the 289 ev/s this page previously
quoted, and that drags every live "vs hot store" ratio in this table down with it. RSS rose to ~68 MB with
8 windows in flight - bounded by `K` and well within the 256 MB budget.

**Caveats:** public-endpoint throughput varies several-fold run to run (see the note above) - the
*ratios* here are the claim, not the absolute ev/s. All three arms declare no `[[calls]]` - a
bare-event workload; a nest with `[[calls]]` will not reach these multipliers. #725 (every
seal-direct path hardcoding an empty calls slice regardless of what the nest declared) is closed;
`bench` now refuses to run a declared-`[[calls]]` nest without `--state-rpc` instead. #743 closed it
on the hot arm too, so the "vs hot store" column is a comparison between two arms doing the same
tier-3 work rather than one doing it and one skipping it.

## Baseline matrix (pre-optimization)

_Pending - the full W1-W3 × T1-T3 matrix is populated from archive-node runs (needed for the
historical ranges) and committed as `bench-report.json` artifacts._

## Query benchmarks (the read path)

The backfill harness measures the write path only, which left entity point-read latency and the `/sql`
scan cost free to regress silently. `nuthatch bench query` is the guard for both. It runs **offline
against an already-indexed nest** - stop `dev` first, since the bench opens the store directly.

```sh
nuthatch bench query --dir <nest> [--sql "<query>"] [--reads N] [--iters N] [--out report.json]
```

Reports:

- **entity point-read p50/p99** - `--reads N` keys in chain order from the oldest, read once to warm
  the cache and then timed. It is a **prefix, not an even spread**: pass `--reads` at or above the hot
  row count (as the CI gate does) and it covers every key, but a smaller `--reads` over a large hot
  store times a contiguous run of the B-tree and will flatter itself.
- **`/sql` query latency p50/p99 and peak RSS** over the hot ∪ cold union. The default query is a
  `SELECT count(*)` on the largest hot table: deliberately the full-tip-materialising scan, because
  that is the **#1 RAM risk on a deep-finality L2** and the number most worth watching.

Same house rule as the backfill matrix: numbers come from a committed `bench-report.json` with date,
provider, hardware and commit, or they are not quoted. Run it before and after any change to the
serving or storage path - a cached query session, a bounded hot scan, or a compact row format
would all show up here first. A `query` report carries `hardware` and `commit` but no `provider`,
because it runs entirely offline against a store on disk: no endpoint is involved, and the machine is
what makes two of these numbers comparable at all.

### The point-read gate (issue #283)

Reporting a number is not tracking it. CLAUDE.md says benchmark regressions fail the build, so
`bench query` also takes limits, and exits non-zero when one is breached:

```sh
nuthatch bench query --dir <nest> --reads 200200 \
  --min-reads 12800 --max-point-read-p50-us 8
```

Pass none of them and the bench only reports, which is what an operator poking at their own nest
wants. Pass any of them and it is a gate.

That is the command CI runs, and it deliberately sets no p99 ceiling - see below for why the tail is
tracked rather than gated. `--max-point-read-p99-us` still exists for an operator who wants it.

**Point-reads see the unsealed tip, not the backfill.** `get_entity` is a hot-store read, and rows
past finality are sealed to Parquet and pruned out of redb - so of the fixture's 200,200 indexed
rows only 12,800 (64 blocks x 200 logs) are still readable this way, which is what `--min-reads` is
set against. The first version of this gate, on the earlier 8,004-row fixture, asserted
`--min-reads 8004`, reasoned from the backfill size, and therefore failed every run.

**Always pass `--min-reads` alongside a ceiling.** A nest with nothing indexed samples no keys and
reports `p50 = 0µs`, and zero is under every ceiling anyone would ever write - so a gate without a
floor is greenest exactly when it has measured nothing. The `footprint` check learned the same lesson
and asserts its row count before it compares a peak.

CI runs this as the **`point-read latency`** job (`.github/workflows/point-read.sh`), on the dense
fixture the `per-cursor RAM budget` job uses (#424): `fixture-rpc multinest` serves the chain, one contract
with the 10-event Uniswap V4 ABI at 200 logs a block, the nest is written inline, and every run
indexes exactly 200,200 rows across ten tables. No secret and no third party, so a fork's pull
request can satisfy it, and a change in p99 is a change in nuthatch rather than in somebody's rate
limiter. The report uploads as an artifact on every run, including a failing one.

### Where the ceilings come from

They were chosen by **breaking the read path on purpose and measuring what happened**, not by leaving
generous headroom - and both halves were measured on `ubuntu-latest`, the runner that enforces the
gate, rather than on a dev box. Replacing the B-tree seek in `Store::get_entity` with a linear scan -
the exact regression the gate claims to catch - gives, over the 256-row hot store:

| | p50 | p99 | p99.9 |
|---|---|---|---|
| baseline, 3 CI runs | 0.59 - 0.82µs | 0.70 - 1.00µs | 0.77 - 3.96µs |
| linear scan in place of the seek | 18.15µs | 34.45µs | 49.78µs |

That table is the 256-hot-row fixture the gate first ran on. #424 re-pointed it at the dense
fixture and measured the same mutation there on `ubuntu-latest`: the committed baseline
`docs/bench/point-read.json` reads p50 0.94µs, p99 1.5µs over 12,800 hot rows, and the linear scan
landed 87x above the 8µs ceiling where the 256-row fixture had put it at 1.8x (the figures are in
the job's own comment in `ci.yml`, not in a committed `docs/bench` file).

The first version of this gate used 200µs/2,000µs. The scan sits comfortably under both, so that gate
reported `OK: within the point-read ceilings` **with a full scan in the read path** - a number, not a
gate.

A second version used 15µs, measured the same way but on a 32-core/62 GB dev box (baseline 0.66-1.59µs,
scan 24.2µs), on the assumption that a slower CI runner would scale baseline and regression together.
Measuring both on the runner showed that it does not: the runner's baseline is *at or below* the dev
box's, while its mutation is *faster* (18.15µs against 24.2µs), leaving 15µs only a 1.21x margin below
a real regression rather than the 1.61x claimed.

- **p50, ceiling 8µs, is the gate that discriminates.** 9.8x above the worst observed baseline and 2.3x
  below a full scan - better on both sides at once than 15µs was on either. p50 is the robust
  statistic here: its observed spread is 1.4x where p99.9's is 5.1x.
- **p99 and p99.9 are tracked and gated on by nothing.** This is the design call, not an oversight:
  gating loosely and tracking precisely are two different jobs, and one ceiling cannot do both. A p99
  tight enough to catch a regression flakes on a shared VM; one loose enough to survive catches
  nothing. This gate briefly carried a 150µs p99 backstop on the second horn of that, and the table
  above is what retired it - the scan mutation only moved p99 to 34.45µs, so a 150µs ceiling did not
  fire on the one regression the gate exists to catch. A ceiling that passes the known break is not a
  weak gate, it is decoration that reads as coverage, and on a *required* check that is worse than no
  check because it stops anyone looking.

  The tail is still the number worth having - it is simply read with our eyes across releases rather
  than compared to a threshold. p99 over 256 samples is the 4th-worst read and preemption-dominated
  here; p99.9 swung 0.77-3.96µs at fixed commits. Neither is a property of the read path on this
  hardware, which is precisely why neither can fail a build. Both are in the uploaded report and the
  step summary.

  `MAX_P99_US` is still there for an operator on a quiet machine who wants the tail enforced
  deliberately; it just has no default and CI does not set it.

What this gate does **not** catch: a regression landing under 8µs - a partial scan, or a scan when the
hot store is much smaller than 256 rows - cold-start latency (it measures warm, see `warm_cache` in the
report), point-reads against the Postgres backend, and anything about the sealed-segment SQL path. It is a
floor on gross regressions, not a microbenchmark.

### The baseline is the runner's own artifact (issue #385)

`docs/bench/point-read.json` is the `point-read latency` job's uploaded report, taken verbatim from a
green run (commit `03d296b`, p50 0.94µs, p99 1.5µs, 12,800 hot rows across ten tables) and committed
with nothing edited but a trailing newline. Its `hardware` field reads
`4 cores, 16 GB RAM`, which is the machine the table above was measured on and the machine the 8µs
ceiling is enforced on. **The committed baseline and the enforcing surface are now the same box.**

They were not, for as long as this gate has existed. The file recorded `32 cores, 62 GB RAM` and
p50 1.24µs from the dev box, which is 1.5-2.1x the runner's own baseline - so anyone re-deriving the
ceiling from the only committed report there was would have derived it from the wrong machine, on a
gate that had already been bitten by exactly that: 15µs came from the dev box and left 1.21x of real
margin below a regression rather than the 1.61x it claimed. The FAIL message in `bench query` had to
spend four lines telling readers *not* to use the one file named "baseline", which is a workaround
with a comment on it rather than a fix.

- **The dev-box record is kept**, as `docs/bench/point-read-devbox.json`. The 32-core numbers are the
  reference point for the footprint work and worth not losing; they are simply not what this gate is
  measured against, and the filename now says so.
- **`BASELINE` in `ci.yml` stops it drifting back.** The job compares the `hardware` its fresh report
  records against the `hardware` the committed baseline records, and fails on a mismatch. A committed
  number cannot go stale loudly - it can only be read and believed - so the check is the only thing
  that would notice.
- **Provenance is checked, values are not.** A "measured p50 within Nx of the baseline" rule is a
  second and much tighter ceiling in disguise: the runner's p50 has been seen from 0.58µs to 0.82µs
  at fixed commits, and its p99.9 from 0.77µs to 23.02µs, so any factor loose enough not to flake is
  looser than the 8µs gate already is. Which machine produced a file is the one thing a committed
  baseline can be definitively wrong about, so that is what is enforced.
- **Refreshing it:** download `point-read-report` from a green `point-read latency` run on `main` and
  commit it as `docs/bench/point-read.json`. `bench query` writes a trailing newline (#385), so the
  committed file is byte-identical to the artifact and a hand-edit is never needed. Do not touch the
  `hardware` field; if the runner spec has genuinely changed, the ceiling wants re-measuring, not the
  baseline re-labelling.


## The per-cursor RAM budget (≤2 GB)

CLAUDE.md non-negotiable 2: **≤2 GB RAM per active-chain cursor**, shared across whatever nests sit on
that cursor. RFC-0021 makes it per-cursor rather than per-runtime. Density is RAM-bounded, not free,
and this is the measurement that says whether we hold it.

It is measured on the adversarial case, because a pass on a quiet contract tells us nothing: a large
ABI, a high event rate, many nests on **one** cursor, and **at tip** rather than mid-backfill.

```sh
bash .github/workflows/multinest-footprint.sh          # measure + enforce
scripts/multinest-rss-spread.sh 7                      # the noise band, before moving a ceiling
```

Both commands are bare on purpose: the harness's defaults **are** the scenario CI enforces (20 nests,
1000 backfill blocks, tip-follow to 20200), and the spread tool inherits them, so what you measure
here is what the gate measures. Export a knob and you are measuring something else - which matters
most in the one case you would want to, re-baselining a ceiling, because a smaller scenario yields a
smaller band and a ceiling below the healthy enforced figure (#395).

### Two ceilings, and why they are not one

| | ceiling | what a breach means |
|---|---|---|
| `MAX_RSS_MB` | 2048 MB | the **budget** was broken. A product promise, not a tuning parameter. |
| `REGRESSION_MB` | 602 MB | this scenario got materially more expensive. |

Both halves of that 602 are measured **on the runner that enforces it**, which is the only place the
margin is real (the derivation sits beside the value in `ci.yml`):

| | |
|---|---|
| runner band (#1156) | 387 to 481 MB over seven sequential runs, median 429 (`rss-band.yml` run 33956127005, 2026-09-05); ceiling 481 x 1.25 + 1 |
| previous (#1067) | 372 MB from a single run, ceiling 466 MB, which main then crossed at random |
| before batching | 131 MB, ceiling 180 MB |

#1067 is why it first moved. The scenario is 12,010 rows per nest, below `SEAL_DIRECT_BATCH` (20,000),
so the tip path now holds the whole 240,200 rows in the hot store instead of sealing each finality
advance. The 2 GB budget still has 1.4 GB of margin. Re-run `rss-band.yml` on the runner before
moving it again; `scripts/multinest-rss-spread.sh` is the same arithmetic by hand.

The budget alone cannot be a regression gate. The scenario measured ~145 MB against 2048 when the
ceiling was first set, and a few hundred since, so a change could cost several times the memory and
still pass. A gate that cannot fail is not coverage, so the
regression ceiling is set from the observed run-to-run spread and is the one that actually catches
drift. They are nested and must not be reconciled into a single number.

The same logic separates this from the `footprint (RAM budget)` CI job, which is **not** redundant
with it. That one is a tight tripwire on a deliberately small scenario (one nest, one event type,
8004 rows, 256 MB); its sensitivity comes from being small. This one is the product budget on the
scenario most likely to break it. Neither subsumes the other.

### What the measurement found

| | |
|---|---|
| nests on one cursor | 20 |
| ABI | Uniswap V4 `PoolManager` - 10 events, `Initialize`/`Swap` 8 inputs each |
| event rate | 200 logs/block across the cursor |
| blocks | 1000 backfill, then 200 of live tip-following |
| rows | 240,200 |
| **peak RSS** | **143 MB**, against the 2048 MB budget |
| at-tip RSS | 143 MB |

The 143 above is the **top of the measured band** - the worst of the 15-run spread the regression
ceiling was derived from (111, 132, 133 x7, 134, 140, 141, 141, 143), because a ceiling is set from
the top of a band and not from its middle. The committed artifact
`docs/bench/multinest-footprint.json` is a **single run** of the same scenario, so it reads lower:
139 peak, 134 at-tip. Both are correct and they are not meant to match; if you have compared one
against the other and found them disagreeing, that is the reason.

**History is close to free in RAM, and that is the sealing design working rather than luck.** At a 4x
event rate (608,800 rows) peak went only 143 → 198 MB, and the at-tip figure (173 MB) came in *below*
the peak - so the peak is a backfill burst, not a steady-state cost. RSS tracks the near-tip window
and the per-window fan-out, not the size of history, because past finality the rows are sealed to
Parquet and leave the heap.

**The runtime's own projection was ~13x too pessimistic at the time.** `estimate_nest_rss_mb`
projected 1920 MB for this exact cursor (`120 + 20 × 90`) against a measured 143 MB, a flat per-nest
constant that could not see ABI size, table count, or event rate, and since a cursor whose
*projected* RSS exceeds `max_rss_mb` is refused before it starts, admission control capped a cursor
at ~22 nests on a model an order of magnitude out. It has since been fitted to this artifact:
`NEST_BASE_RSS_MB` is 5 MB per nest on a 120 MB base, a 5x margin over the ~1 MB per nest observed
(`src/runtime.rs`); the view terms are still the conservative guesses.

### The fixture

`fixture-rpc multinest` serves the chain locally, the nests are written inline, and topic0s are derived by
hashing the signatures in the same ABI file the harness writes. No secret, no third party, and a fork
PR can satisfy the check - the lesson of issue #260, which this inherits rather than relearns.

The tip **moves**: `eth_blockNumber` advances a fixed step per call once the backfill has drained,
so the cursor genuinely spends the run's second half on the live path. It is deterministic in its
*endpoint* rather than its schedule - a faster machine arrives sooner, but every run serves exactly
the same logs, so peak RSS stays comparable between runs.

**The check refuses to pass unless it can show it did the work**: every nest reached the final tip,
every nest cleared a row floor, and **all ten tables of every nest hold rows**. That last one also
proves each topic0 landed - a wrong topic0 does not error, it yields an empty table, which is exactly
how a footprint check passes having measured nothing.
