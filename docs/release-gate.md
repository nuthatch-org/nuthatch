# The release gate

A release candidate is run against a copy of the Lodestar allocations nest, with the queries Lodestar
and kittiwake actually send it, under the budget that nest runs on in production. A refusal, an
error, an out-of-memory or a time regression past a stated bound turns the candidate red. Red stops
the roll to production; it does not block the tag or the release (Chief, 2026-10-03). Issue #1749.

It exists because of 4.1.1. On 2026-10-03 it reached the Lodestar box and its allocations nest
refused the dashboard's join-heavy views within minutes (`HashJoinInput` out of memory under a 2 GB
budget). Every CI gate had passed: the footprint jobs index synthetic data and run no authored view.

## The pieces

| File | What it is |
|---|---|
| `nuthatch-gate/alloc-queries.tsv` in kittiwake | The query set: one statement per line, each with its consumer and call site. Private; see below. |
| `scripts/release-gate.sh` | Serves a nest copy with one binary, runs the set, gives the verdict. |
| `scripts/gate/common.sh` | Production's budget, how a set is read and the canonical answer form. |
| `scripts/gate/baseline-4.2.0.tsv` | The proof run's baseline: 4.2.0 on the 2026-10-03 copy, on a MacBook. |
| `scripts/release-gate-run.sh` | The ThinkPad's job: fetch the candidate, gate it, post the status. |

## The query set

Lodestar sends nothing to the nest itself: its dashboard reads kittiwake's API, and kittiwake is the
only thing that opens a socket to a nest (`crates/nest`). So the set is every statement kittiwake
sends to the allocations nest (`NestId::Alloc`), labelled `lodestar` where it serves a dashboard
route and `kittiwake` where it is one of kittiwake's own jobs (the directory refresh, the ingest
crons, the live feed, RAV collection, QoS scoring). 58 are Lodestar's, 15 kittiwake's, and 2 are
asked by both.

The set carries kittiwake's statements, and kittiwake is private while this repo is public, so it
lives in the kittiwake repo, under `nuthatch-gate/`, beside the generator that compiles it from
kittiwake's own source and a CI check that fails when it goes stale. Nothing here holds a copy:
`release-gate.sh` takes the set as an argument, and `release-gate-run.sh` refuses to start unless
`GATE_SET` names it. `nuthatch-gate/README.md` there says how to regenerate it.

Parameters are representative literals pinned to one day, so the set is the same file however often
it runs. Each statement appears once: the nest memoises answers by statement text, so a duplicate
would time the memo.

## Running the gate

    scripts/release-gate.sh [--baseline F] [--write-baseline F] [--passes N] [--concurrency N] <binary> <nest-copy> <set>

The copy needs its sealed segments **and** a copy of its `nuthatch.redb`: without the redb it serves
no sealed history. The script starts `nuthatch serve` on the copy under production's environment,
which is `PROD_ENV` in `scripts/gate/common.sh`, copied from the allocations nest's unit on the
Lodestar box (port 8107):

    NUTHATCH_SQL_MAX_CONCURRENCY=2  NUTHATCH_ANALYTICS_MEMORY_LIMIT=256MB  NUTHATCH_ENGINE=burrmill
    NUTHATCH_BURRMILL_MEMORY_LIMIT=2GB  NUTHATCH_ANALYTICS_THREADS=8  NUTHATCH_MAX_RSS=6GB

When that unit's environment changes, change `PROD_ENV` with it, or the gate tests a budget nobody
runs. `--env FILE` replaces `PROD_ENV` with another nest's, `NUTHATCH_*=VALUE` lines, and serves the
copy under that file's settings alone: a `NUTHATCH_*` variable the caller carries is dropped. Each pass starts a fresh server, because of the memo; a query's status is its worst pass and
its time the median of its passes. For each query it records status, rows, time and the digest of
its answer from the first pass that answered; an answer that moves between passes is noted.

`--concurrency N` (default 1; `release-gate-run.sh` passes 2, as 8107 serves two statements at once)
sends the set N statements at a time in its own order, waiting for a whole group before the next, so
every pass pairs the same statements (#1773). The set lists each kittiwake call site's statements
together, so a pair is mostly two statements one route sends at once. Each statement keeps its own
time and its own answer, compared as at concurrency 1, and `schedule-pass-N.tsv` in the output
directory records each statement's group, start and end. When the binary exports the
analytics pool and jemalloc gauges (#1778), the run also reports what they read at the RSS peak and
the largest single pool reservation.

It fails (exit 1) on:

- any error or refusal from `/sql`, an out-of-memory, or a degraded answer;
- the server dying under a query;
- the serving process's peak RSS over the 2 GiB per-cursor budget (`GATE_MAX_RSS_MB`, 2048): on
  Linux the kernel's high-water mark, `VmHWM`, read as each server is stopped, elsewhere the
  half-second samples;
- with `--baseline`, a query slower than **2x its baseline and more than 1000 ms slower**, or the
  set's p99 slower than **1.5x the baseline's and more than 1000 ms slower**. Both halves must hold,
  so a 40 ms query taking 90 ms is noise. The bounds are `GATE_QUERY_FACTOR`, `GATE_QUERY_SLACK_MS`,
  `GATE_P99_FACTOR` and `GATE_P99_SLACK_MS`;
- with `--baseline`, a statement both runs answered whose answer differs: `answer differs`, with the
  first differing row of each side printed under it (#1772).

Exit 2 is a broken rig (no redb, a port, a binary that will not start), not a verdict on the binary.

### A loaded box is a setup fault (#1898)

On 2026-10-05 the 4.8.0 gate ran with a load average of 26, a build on every core, and called the
noise a regression: production's own binary went over budget in the same run. Timings and RSS taken
on a box something else is saturating say nothing about the binary, so the gate checks the load:

- **It records it.** The first lines of the output give the 1-minute load average
  (`/proc/loadavg`, or `sysctl -n vm.loadavg` on macOS) and the core count; the run samples it
  every 5 s into `<out>/load.tsv` and reports the peak and the share of samples over the limit.
  A baseline's header carries the same line.
- **It waits before it starts.** Until the load per core is under `GATE_MAX_LOAD_PER_CORE`
  (default 0.3) it sleeps, polling every `GATE_LOAD_POLL_SECS` (15), for up to `GATE_MAX_LOAD_WAIT`
  seconds (1800). If the box is still loaded it exits 2 naming the load, serves nothing and posts no
  regression.
- **It refuses a verdict from a run that was loaded.** If more than `GATE_LOAD_MAX_SHARE` (0.25) of
  the samples taken during the run were over `GATE_RUN_MAX_LOAD_PER_CORE` (0.6), the result is
  `RESULT: SETUP FAULT`, exit 2, whatever the timings or RSS say, and no `--write-baseline` file is
  written. `release-gate-run.sh` posts `error`, not `failure`, for any exit 2.

The defaults are calibrated to the ThinkPad: 32 cores, idling at a load of about 3.8 (0.12 per
core) with the QoS nest running. The 4.8.0 incident was a load of 26, 0.81 per core. The start
limit of 0.3 per core (about 9.6) sits well above idle and well under the incident. The run's own
limit is higher, 0.6 (about 19), because the gate's server is part of the load it reads: its eight
analytics threads add roughly 0.25 to 0.4 per core, so a quiet run reads 0.37 to 0.52 and stays
under 0.6, while the incident's 0.81 does not. The run limit is the start limit plus the gate's own
share, set by hand; on a box with a different core count, scale both. Read a few runs' `load.tsv`
before trusting them elsewhere. The 1-minute average lags, so a burst that starts late in a run may
only show in the next one.

If the load cannot be read at all (no `/proc/loadavg`, no `sysctl`), the gate says so and measures
anyway. `GATE_LOADAVG_FILE` points the reader at a file whose first field is the load, and
`GATE_NCPU` fixes the core count; the tests use both.

### Comparing answers

Each answer is kept canonical in `<out>/answers/<id>.rows`, one row per line as JSON with its keys
sorted, and the baseline records its sha256 and where its rows are (`# answers:`), so a candidate run
can show the row that differs. The canonical form decides what counts as the same answer:

- Every value compares exactly as served, floats included: a float one bit off, or an integer that
  comes back as a float, is a different answer. Floats were rounded to 12 significant digits until
  #1883, when two fresh 4.7.0 servers answered every production set alike to the last bit
  ([the record](https://github.com/nuthatch-org/nuthatch/blob/ede03ca7/docs/reproducibility-2026-10-05.md)). A baseline from a binary before 4.6.0, which summed
  DOUBLE in arrival order, can differ from a later one in the last digit.
  The rows go through jq, and only jq 1.7 or later keeps each number's digits and form: an older one parses
  every number to a double, so 9007199254740993 would match 9007199254740992 and 1.0 would match 1.
  The gate refuses an older jq as a setup fault (exit 2). The ThinkPad, the Mac and CI's
  ubuntu-latest all have 1.7.
- Rows are compared in order when the statement has a top-level `ORDER BY`, and sorted first when it
  has none, since without one the order is the engine's choice. A window's or a subquery's `ORDER
  BY` sits inside parentheses and does not count. When it cannot tell (a comment, an unclosed quote,
  unbalanced parentheses) it sorts and says so; the baseline's `compared` column records which.
- A statement whose answer moves without the binary changing is tagged in the set, `# volatile:
  <id> <why>` (written by `collect/main.rs`), and held to its row count only. So is a truncated
  answer without a top-level `ORDER BY`, which is an arbitrary subset of the rows.

The tags come from gating production twice on one copy a minute apart and reading what moved. On
2026-10-03 nothing in the set moved with the clock (its literals are pinned, and no view reads
`now()`); four statements moved anyway. Two were a DOUBLE cast to text, now rounded as above. Two,
`payments.accounts` and `delegation_events`, page with `ORDER BY ... LIMIT` on a key with ties, so
which tied rows fill the page is the engine's choice, and a second run returned a different page.
They are tagged volatile; a tiebreaker in kittiwake's statements would let them be compared
exactly. Re-run the double gate after refreshing the set.

Answers only mean something against a baseline measured on the same copy, which is what the runner
does. A baseline without the digest column (the committed `baseline-4.2.0.tsv`) is read as before,
its answers reported as not compared. A run writing a baseline and the run reading it need separate
`--out` directories, since each starts its `answers/` afresh.

`tests/release_gate_script.rs` runs the script against a nest sealed from the fixture chain: a
refused query fails and is named, an answered set passes and writes a baseline, a regression fails
past the bound and passes inside it, and a candidate answering differently from its baseline (a view
edited between the runs) fails with the first differing row, while rows reordered under no top-level `ORDER
BY` (none at all, or one in a subquery), a float 1e-13 off (as a number or as text) and a volatile statement's new
answer pass.

## What it does not catch

The comparison is with the release production runs, so a wrong answer both releases share passes
it. On the allocations nest, daily parity against the Network Subgraph (`deploy/parity`) is what
sees those. A per-nest DuckDB reference stage (#1796) also did, and was removed on 2026-10-04.

## Where it runs: the ThinkPad

The copy is a whole nest directory, segments and redb, and the gate wants a quiet box for its
timings, so it does not fit a GitHub runner. It runs on the ThinkPad, which already runs the QoS nest; the gate is scheduled away from
that nest's busy periods. Every gate run against a copy, the timer's or one by hand with
`release-gate.sh`, takes the same lock beside it (`alloc-nest.gate-lock`; flock on Linux, a mkdir
lock elsewhere) and waits for any other: two `serve`s cannot open one redb.

**The copy** lives at `~/release-gate/alloc-nest` and is refreshed from the Lodestar box in Helsinki
before each run, through the `GATE_REFRESH` hook, `deploy/release-gate/refresh-from-helsinki.sh`
(#1774). It has two halves:

- On Helsinki, `deploy/release-gate/helsinki/gate-export.sh` is the forced command of the
  ThinkPad's key (`~/.ssh/nuthatch-gate`) in root's `authorized_keys`, accepted from the ThinkPad's
  tailnet address only. It answers `snapshot` and a read-only rsync of its stage
  (`/var/lib/nuthatch-gate/stage`), and refuses anything else.
- `snapshot` stages the nest directory: sealed segments hardlinked (they are immutable), the
  config and views copied, and a copy of `nuthatch.redb`. The redb is open in the unit and redb has
  no online backup, but a store nothing is writing is a byte image of its last commit, because every
  commit is fsynced. The nest writes only during an ingest cycle, so the copy is checked rather than
  timed: the redb and `segments/manifest.json` are hashed before and after, the copy is hashed, and
  `/ready`'s `last_block` is read on both sides. Any difference means a commit or a seal landed
  during the copy, and the copy is taken again, up to ten times. It ends with a `PROVENANCE` file
  carrying the version, `last_block`, `sealed_through` and the sha256s of the redb and manifest.
  Nothing is stopped.
- The ThinkPad pulls the stage with rsync into `alloc-nest.incoming`, hardlinking unchanged
  segments from the current copy. It swaps the new copy in only when the pulled `PROVENANCE` is the
  snapshot it asked for, the redb and the manifest match their staged sha256s, and
  `sealed_through` has not gone backwards. The previous copy is kept as `alloc-nest.prev`. Any failure leaves the copy as it was
  and fails the refresh, which `release-gate-run.sh` posts as `error`. It takes the copy's gate
  lock, so a refresh run by hand waits for a gate that is serving the copy.

A redb that passes those checks and that `serve` still refuses to open stops the gate at startup:
`release-gate.sh` exits 2 when `serve` exits before answering `/health`, and the run posts `error`,
not a verdict. The provenance line of each run (`as_of`, `sealed_through`) shows how fresh the copy
was.

**The trigger** is `scripts/release-gate-run.sh`:

- `--poll`, from a systemd timer every 15 minutes: gates the newest published release or release
  candidate that is newer than production, by the version in its tag, and whose commit carries no
  `release-gate/alloc-nest` status. A release at or below production is never gated, and with
  nothing newer it exits quietly. A `-rc` tag runs the release
  workflow like any `v*` tag and is published as a prerelease with its binaries, so tagging a
  candidate is enough to have it gated within the quarter hour.
- `<tag>` by hand, to gate or re-gate one release.
- `--binary PATH --sha SHA` for a local build, which is how a burrmill rev bump is gated before
  anything is tagged. Build it with `cargo build --profile dist --locked`, the fat-LTO profile the
  release ships, so the gate measures what an operator downloads.

For each run it downloads and checksums the candidate's Linux binary
(`nuthatch-x86_64-unknown-linux-gnu.tar.gz`) and the production release's, measures production
first and writes that as the baseline, then gates the candidate against it. The baseline is
therefore the same box, the same copy and the same day, so neither a refreshed copy nor a
different machine reads as a regression. The committed `baseline-4.2.0.tsv` is the proof run's
record, not what the ThinkPad compares against.

**Production** is what the allocations nest runs, which is not always the latest release: on
2026-10-04 it ran 4.2.1, rolled back from 4.3.0 (#1790), and v4.3.1 was gated against v4.3.0 and
failed for fixing 4.3.0's wrong answers (#1804). So the runner reads it from the copy's
`PROVENANCE`, whose `version=` the snapshot took from the nest's own `/ready`: `version=4.2.1`
means production is `v4.2.1`. It is read after `GATE_REFRESH`, so it is the version that served the
copy being measured. `--production <tag>` overrides it. Only a copy with no `PROVENANCE` falls back
to the latest full release that is not the candidate, and the run says so in its output and in the
status (`as the latest release, no PROVENANCE`). A `PROVENANCE` whose version is not a release
version stops the run (exit 2) rather than guessing, with an `error` status on the candidate naming
the version; a `--poll` that meets it before choosing a candidate has no commit to post on, and
exits 2 saying so.

`--poll` judges "newer than production" against the same production, read from the copy as it
stands before the refresh; after the refresh it is read again, and a candidate no longer newer than
it (production was rolled to it meanwhile) is left ungated: the run says so and exits 0, posting
nothing.

Production failing its own gate is still the baseline, since it is what production does: over
the RSS budget, or refusing statements of its own. Its peak and failures are named in the
candidate's status and output. The candidate's verdict stays its own: it fails on its own
refusals, its regressions against production's times, answers that differ from production's and
its own peak, and a statement production fails can neither regress nor differ. Only a production run that could not be measured (exit 2) posts `error`.

**The result** reaches the release as a commit status on the candidate's commit, context
`release-gate/alloc-nest`, posted with `gh api repos/nuthatch-org/nuthatch/statuses/<sha>`:
`pending` when it starts, then `success`, `failure` (with the failed queries named) or `error`
(the gate could not run). The full output stays in `~/release-gate/runs/` on the ThinkPad.

It never blocks the tag: the tag exists before the gate runs, a status on a tagged commit gates
nothing on GitHub, and `release-gate/alloc-nest` must not be added to the required contexts. What
it stops is the roll: read the status before `deploy-nest.sh roll`, and a red one means the
candidate does not go to production.

A run that has posted any final status is not repeated by `--poll`; re-gate by hand with the tag.

## Every production nest (#1794)

On 2026-10-03 the QoS nest refused its daily views all day on 4.2.1 and 4.3.0 (#1781), and the
gate, which ran the allocations nest alone, never saw it. So the runner gates every nest named in a
config file, `GATE_NESTS`, which the ThinkPad holds at `~/release-gate/nests.conf`
(`deploy/release-gate/nests.conf.example` is the shape). Each line is one nest:

    <name> <copy> <query set> <env file> <refresh: helsinki | local | none>

| Nest | Unit | Copy refreshed |
|---|---|---|
| `alloc-nest` | graph-allocations-nest-next, Helsinki | from Helsinki |
| `qos-nest` | qos-reo-nest, the ThinkPad | locally, from `/opt/nuthatch/qos-reo-nest` |
| `gns-nest` | graph-gns-nest-next, Helsinki | from Helsinki |
| `dips-nest` | nuthatch-dips, Helsinki | from Helsinki |
| `data-services-nest` | data-services-nest, Helsinki | from Helsinki |
| `staking-archive-nest` | graph-staking-legacy-readonly (serve-only), Helsinki | from Helsinki |

**Since 2026-10-16 the copies are frozen.** The Helsinki nests stopped with the Lodestar withdrawal on
2026-10-13 and the QoS nest is gone, so every refresh failed and every release from 5.4.0 to 5.6.3
carried `error` on all six contexts. Each line in the ThinkPad's `nests.conf` now has refresh `none`,
gating against the copies taken on 2026-10-12, and each copy's `PROVENANCE` is kept as
`PROVENANCE.helsinki-20261012`, so the baseline is the latest full release that is not the candidate
rather than the 5.0.1 production last ran. A poll skips a release that already has a status, `error`
included, so a release that errored is gated by naming it: `GATE_NESTS=~/release-gate/nests.conf
scripts/release-gate-run.sh v<x.y.z>`. With no production nest left, the roll's gate check has
nothing to vouch for; the gate is what a release is measured against.

The runner reads the whole file first, so a bad line gates nothing, then runs itself once per nest
with that nest's copy, set, environment and refresh. Each nest is gated exactly as the allocations
nest is above, under its own copy lock, against the production version its own copy's
`PROVENANCE` records, and posts its own status, `release-gate/<name>`; a `--poll` gates, per nest,
the newest release newer than that nest's production with no status of that nest's context. The
run's exit is the worst of the nests'. The output carries each nest's lines prefixed `[<name>]`, and
a nest with nothing to gate stays quiet.

**The query sets** are kittiwake's statements for each nest's `NestId`, so they live in the private
kittiwake repo with the allocations nest's, as `nuthatch-gate/<set>-queries.tsv`, generated by its
`nuthatch-gate/collect/collect-queries.sh` and checked stale in its CI. Nothing in this repo holds
them. The QoS set asks one closed day, as production does, never a count over the whole history.

**The environment** of each nest is its unit's, not this script's guess: the env file holds the
`NUTHATCH_*` budget settings (concurrency, memory limits, engine, threads, the RSS cap, the memo and
cache sizes) read from the running process's own environment, `/proc/<pid>/environ`, so drop-ins and
environment files count. Tokens, RPC URLs and paths into the unit's directory stay on the box. The
file is passed as `release-gate.sh --env`, and its `NUTHATCH_SQL_MAX_CONCURRENCY`, when set, is the
concurrency the set is sent at. Production's RSS budget, 2 GiB per cursor, applies to every nest.

**The refresh** of a Helsinki nest is `refresh-from-helsinki.sh <name>`: it asks the export for
`snapshot <name>` and pulls `/var/lib/nuthatch-gate/nests/<name>/`. The export answers only names
its configuration allowlists, one `NEST=<name> <dir> <url>` line each in
`/etc/nuthatch/gate-export.env`, and a pull only of the bare stage or an allowlisted nest's; anything
else is refused, as before. A bare `snapshot` still means the allocations nest. The QoS nest is on
the ThinkPad, so `refresh-from-helsinki.sh --local qos-nest` runs the same export here, as root with
`sudo -n` (the unit's directory is its own), against `~/release-gate/export-local.env`, and copies
the stage with a local rsync that hands the copy to the gate's user. The consistency check and the
swap are the same, and no ssh is involved.

**Installing it** is one command from Chief's Mac, after this is on `main` and kittiwake's sets are
on its `main`:

    deploy/release-gate/install-nests-from-mac.sh

It installs the export on Helsinki and rewrites its allowlist from the five running units, snapshots
each once, checks from the ThinkPad that the key is refused anything off the allowlist, then on the
ThinkPad pulls `~/nuthatch-ops` and `~/kittiwake`, writes `nests.conf`, the six env files and the
local export config, refreshes every copy and installs the unit, which sets `GATE_NESTS`.

The timer is `deploy/release-gate/release-gate.{service,timer}`, a systemd user unit running `--poll` every 15 minutes from `~/nuthatch-ops`, a worktree of the repo on `main`; the unit file carries the install lines. The unit sets `GATE_NESTS` to `~/release-gate/nests.conf`, so `install-nests-from-mac.sh` copies it over only after writing the config and refreshing every copy. The config names the sets under `~/kittiwake/nuthatch-gate/`, so the ThinkPad needs a clone of the private kittiwake repo at `~/kittiwake` (its `gh` is authenticated as cargopete, which can read it); pull it when a set changes. Without `GATE_NESTS` the runner gates one nest, from `GATE_SET` (required) and `GATE_REFRESH`.
