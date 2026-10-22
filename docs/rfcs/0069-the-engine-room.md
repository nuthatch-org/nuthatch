# RFC-0069: The engine room - a running nest watched from a terminal

- Status: **Accepted (2026-10-21, Chief).** His acceptance lifts, for this RFC, his 2026-10-03 preference for the browser cockpit over a TUI. S0 measured 2026-10-22 (§15): the feature adds 0.30 MiB on macOS and 0.34 MiB on Linux, `cargo deny` stays green, and one watcher leaves tip lag and RSS inside their unwatched spread. Chief watched the clip on 2026-10-22 and said build it, with §14 answered as the RFC recommends. S1 built 2026-10-22 (§16): the line mode, every number equal to the endpoint's in the same read on a solo nest and a two-chain runtime. S2 built 2026-10-22 (§17): the engine room on a terminal, its snapshots from recorded bodies, and one watcher still inside the unwatched spread.
- Author: Pete (cargopete)
- Date: 2026-10-21
- Depends on: the endpoints a nest already serves (`/ready`, `/metrics`, `/`, `/nests`, the admin
  routes), RFC-0010 Part A (the admin surface and its guard), RFC-0021 and RFC-0063 §14 (one cursor
  per chain), and the `nuthatch-tui-client` repository, which this would absorb (§2).
- Blocks: nothing.
- Nature: a client subcommand in the default binary, behind a named cargo feature that is on by
  default (§7), plus two small read-only additions to the server (§5). New capability in the default
  binary, so it needs Chief's line under RFC-0044 §8 before S1 starts.

## 1. What it is for

An operator who runs a nest wants to see it working: blocks arriving at the tip, the seal following
finality, memory against the 2 GiB a cursor is allowed, the RPC endpoints answering or failing over,
a reorg when one lands, and queries going past with their latency. Today that view is spread across
`/ready` (JSON), `/metrics` (Prometheus text) and the admin cockpit (a browser tab). On a box reached
over ssh none of the three is where the operator is.

`nuthatch watch` would be one screen in the terminal, read-only, pointed at any nest the operator can
reach. It is also the first thing about nuthatch that is worth recording: a README clip of a cursor
following mainnet and sealing behind itself shows the product working, where a static API transcript
shows only that it answers.

## 2. What exists, read 2026-10-21 at `origin/main` (`cdd224e5`) and nuthatch 5.8.2

- **A terminal client already exists.** `nuthatch-org/nuthatch-tui-client` (v0.2.0, 2026-09-24,
  24 commits, 4,549 lines, `MIT OR Apache-2.0`, ratatui 0.29) is a separate binary that reads
  `/ready`, `/metrics`, `/`, `/tables`, `/nest`, `/queries` and `/sql`. It already settles several
  questions this RFC would otherwise have to: it polls at the nest's own `poll_interval_secs` (held
  between 2 and 30 s), reads a 503 body rather than discarding it, shows `STALE` when `/ready` stops
  answering, honours `NO_COLOR`, opens an ssh forward itself, and steps through a runtime's nests from
  `/nests`. Issues #812 and #844 were filed from it, and they are why `/metrics` carries per-endpoint
  RPC health, process CPU and storage bytes. Its look is generic, and it is a second thing to
  install and keep in step with the binary's metric names.
- **The admin cockpit is the browser equivalent.** `src/admin.html` (eight tabs, read-only) and
  `GET /_admin/events`, a server-sent stream of `GET /`'s body every 2 s. On 2026-10-03 Chief asked
  for that cockpit "rather than more TUI". This RFC is asked for on different grounds, as a showpiece
  (§10), and it must not become a second cockpit (§9).
- **What the endpoints carry.** `/ready`: tip, `last_block`, `lag_blocks`, `sealed_through`,
  `seal_lag_blocks`, freshness mode and `poll_interval_secs`, `seconds_since_poll`, the seal-direct
  pass's position, quarantine state. `/metrics`: about 120 series on a solo nest, among them
  `nuthatch_rss_bytes`, `nuthatch_reorgs_total`, `nuthatch_rows_sealed_total`,
  `nuthatch_sealed_segments_bytes`, `nuthatch_hot_store_bytes`, per-endpoint
  `nuthatch_rpc_endpoint_{requests,failures,retries}_total` and a duration histogram,
  `nuthatch_http_requests_total`, `nuthatch_sql_queries_total`, `nuthatch_sql_rejections_total` by
  reason, the analytics pool's reserved and peak bytes, and in a runtime the same positions per nest
  under a `nest` label. `/nests` at a runtime root lists each mount with its chain and live health.
- **What they do not carry.** No request is timed anywhere: `count_request` (`src/serve.rs:1273`)
  increments a counter and nothing else, so "queries going by with their latency" has no source. A
  reorg is a counter with no depth and no time. RSS is the process's, never a cursor's.
- **Guards.** `/`, `/ready`, `/metrics` and `/nests` are unauthenticated read routes. The `/_admin*`
  routes exist only when admin is enabled (`indexer::admin_enabled`: localhost bind, or
  `NUTHATCH_ADMIN_TOKEN` set), and `token_ok` then demands the token on every bind; with no token
  configured, the open surface answers only a loopback `Host` and `Origin` (#2136).
- **The default cadence is slow.** `dev` polls a caught-up cursor every 300 s unless
  `--poll-interval` says otherwise (`src/freshness.rs:25`). Out of the box, the tip arrives in
  five-minute steps, not block by block.

## 3. The attach model

A client of the HTTP endpoints, in a separate process. It reads what `/ready`, `/metrics`, `/`,
`/nests` and, with a token, `/_admin/*` already serve, and nothing else. The alternatives:

- **In-process (`dev --tui`).** Rejected. It cannot watch a box from a laptop, it shares a terminal
  with the log, and a panic in a drawing routine would sit in the indexer's process. The indexer's
  footprint and its UI should not share a fate.
- **A second telemetry path** (a socket or stream made for the TUI). Rejected. Whatever the TUI needs
  that the endpoints lack is a gap in what an operator can see by any means, and belongs on the
  endpoints, where Prometheus and the cockpit see it too (§5).

The target is `--url`, default `http://127.0.0.1:8288`, as `nuthatch sql` takes it. Over Tailscale it
is the tailnet address; to a loopback-bound box, the ssh forward the existing client already opens
(`--ssh host`, `BatchMode`, reopened with backoff). The admin token comes only from the client's own
`NUTHATCH_ADMIN_TOKEN` and is sent as `Authorization: Bearer`, never as a flag or a query string, so it
stays out of shell history and of a proxy's access log. Without a token, every panel that reads an
unauthenticated route works and the request feed says `NO TOKEN` (§4). The client asks nothing of the
server's guard and changes none of it.

## 4. The panels

The machine is an engine room, following the `steampunk-ui` skill's terminal annex: Foundry tokens in
truecolour, the computed 256-colour cube indices otherwise, no background set in 256-colour mode, never
the 16 named ANSI colours, and no glyph that does not stand for a live value. A plain word wins
wherever the diegetic one is less exact, so `TIP`, `REORG` and `RPC` keep their names.

| Panel | Shows | Reads | Every |
|---|---|---|---|
| **THE TIP** | tip, indexed, lag in blocks and seconds, time to next poll from `poll_interval_secs` and `seconds_since_poll`; a strip of recent arrivals, one mark per poll that moved `last_block` | `/ready` | 1 s |
| **THE PRESS** | `sealed_through`, seal lag, rows and bytes sealed; a mark per advance of `sealed_through`; the seal-direct pass's fetched/completed/target when one runs | `/ready`, `/metrics` | 1 s, 2 s |
| **BOILER** | process RSS as a needle against the budget line, 2 GiB times the cursors in the process; analytics pool reserved and peak; hot and sealed bytes | `/metrics` | 2 s |
| **FEED LINES** | one row per RPC endpoint label: requests, failures and retries per interval, mean latency from the histogram; the endpoint whose count moved last interval is lit; a retry delta reads `FAILOVER` | `/metrics` | 2 s |
| **REORG** | count since start, and when §5's gauges exist, depth and age of the last one | `/metrics` | 2 s |
| **DISPATCH** | recent requests: route, name, status, milliseconds; requests per second and p50 over the window | `/_admin/requests` (§5), with token | 1 s |
| **NAMEPLATE** | nest name, chain, version, freshness mode, NID prefix; `LIVE`, `BACKFILL`, `ATTENTION`, `QUARANTINED`, `STALE` | `/ready`, `/` | 1 s |

Rules every panel keeps:

- **Absent is `NO SIGNAL`, never zero.** `/ready` already returns null for a cursorless lag and for a
  seal lag before anything has sealed; the panel must show the null as a null. A mutation test per
  panel: render with the field absent and assert the zero glyph does not appear.
- **No motion without a cause.** A mark appears when a counter moved between two reads. Nothing turns,
  pulses or animates on a timer, so a still screen is a still machine.
- **No `/sql` by default.** The existing client sends a `count(*)` and a newest-rows query to `/sql`
  on every refresh, which is the watcher spending the nest's query budget on itself. The event feed
  moves behind a key, runs at the nest's poll interval, and is off when `/queries` says SQL is closed.
- **The budget line is labelled for what it measures.** RSS is a process figure. In a multichain
  runtime the needle is the process against the sum of its cursors' budgets, and the panel says so;
  it never divides RSS between cursors, because nothing measures that split.

Refresh rates are the defaults above, floored at 1 s and stretched to the poll interval for the panels
whose source only moves on a poll. `--interval` overrides them.

## 5. The two gaps, and the smallest addition for each

**G1, a request feed.** A ring of the last 256 completed requests in each nest's serve state, written
by the existing `count_request` middleware: sequence number, unix milliseconds, route template (`/sql`,
`/q/{name}`, `/derived/{entity}`), the name the template captures, status, milliseconds and response
bytes. No SQL text, no query string, no client address. Served at `GET /_admin/requests?since=<seq>`,
behind the admin guard like `/_admin/storage`, because even timings of other callers are the
operator's data. One mutex push per request and a fixed allocation of about 40 KiB (estimate); the
point-read p50 bench, which already gates CI, is the check that it costs nothing measurable.

**G2, reorg depth and time.** Two gauges per nest, `nuthatch_nest_last_reorg_depth_blocks` and
`nuthatch_nest_last_reorg_unixtime`, set where `inc_reorgs` is already called. Absent until a reorg,
not zero.

Neither touches the data path. Both are useful to Prometheus and the cockpit without the TUI, which is
the test of whether they belong on the server. Seal events, RPC failover and tip arrival need no
addition: each is a delta between two reads of a series that exists.

## 6. The cost to the nest of being watched

Measured 2026-10-21 on an M-series MacBook, nuthatch 5.8.2 release binary, a USDC nest following
mainnet with `--poll-interval 12s`, 300 sequential requests per route, nest CPU from `ps` before and
after:

| Route | Response | Latency p50 | Nest CPU per request |
|---|---|---|---|
| `/ready` | 759 B | 0.7 ms | 0.20 ms |
| `/` | 773 B | 0.6 ms | 0.13 ms |
| `/metrics` | 18.4 KB, 120 series | 4.5 ms | 1.5 ms |

`/metrics` is the expensive one on macOS because `rss_bytes` forks `ps` on every render; Linux reads
`/proc/self/status` instead and is expected to be cheaper, which S0 measures on the ThinkPad. At the
defaults of §4 (`/ready` and `/` at 1 Hz, `/metrics` at 0.5 Hz) that is about 1.1 ms of CPU per
second, a little over 0.1% of one core, and no retained memory: polling holds no connection state on
the server between requests.

The existing client shows why the `/sql` rule in §4 matters. The same nest, alternating 60 s windows,
`nuthatch-tui-client` 0.2.0 at `--interval 2s` against no watcher: unwatched 60 ms and 30 ms of nest
CPU, watched 500 ms and 360 ms, with 149 requests in each watched window (about five per refresh, two
of them `/sql`). That is under 1% of one core, so it is not a problem on this nest, but it is ten times
the idle figure. Subtracting the per-route costs above (about 55 ms a minute for `/ready`, `/` and
`/metrics` at that rate) leaves most of it to the two queries, whose cost grows with the table, so a
nest the size of Lodestar would pay more per refresh. `ps` reads CPU in 10 ms steps, so the unwatched
figures are coarse.

The gate is the nest's own benchmarks rather than a CPU figure: tip lag's after-seen p50 and RSS with
one watcher attached must sit inside the run-to-run spread of the same bench unwatched (S0, S2).

## 7. Crate, weight and where it lives

ratatui 0.30.2 with crossterm 0.29.0, as the existing client uses one minor earlier. Every crate either
adds is MIT, Apache-2.0 or both, all on `deny.toml`'s allow list. Measured with `cargo tree` on a
scratch crate (`default-features = false`, features `crossterm` and `layout-cache`), compared by name
against nuthatch's `Cargo.lock`:

- **20 crates new to the tree**: ratatui, ratatui-core, ratatui-widgets, ratatui-crossterm, crossterm,
  kasuari (the layout solver), compact_str, castaway, unicode-truncate, line-clipping, lru, strum,
  strum_macros, instability, document-features, litrs, indoc, signal-hook, signal-hook-mio and
  critical-section. Plus a second `darling` (0.24 beside 0.23) at build time only, via `instability`.
- **Binary size**: a stripped, thin-LTO scratch binary drawing a block, a gauge, a sparkline and a table
  is 734,736 bytes against 339,360 for an empty `main`, so about 0.39 MiB (macOS arm64). The 5.8.2
  macOS binary is 95.6 MB, so the estimate is about 0.4%, and a full set of panels perhaps twice that.
  S0 measures the real figure on both release targets.
- **Build time**: the scratch crate built in 14.6 s at `-j 2` from clean. Small beside nuthatch's own
  build, by estimate; S0 records the real difference.

Three places it could live:

1. **Stay in `nuthatch-tui-client`.** No cost to the binary. But the client parses series names the
   binary may rename, and nothing ties their versions together; a README clip of a second download is
   not "one binary, one command".
2. **A subcommand in the default binary.** Client and server in one crate, so a test can assert that
   every series the client reads is one `METRICS.render()` emits, and a rename breaks the build, not the
   screen. About 0.4% of the binary.
3. **A second binary in the release tarball.** The cost of both and the benefit of neither.

**Recommendation: 2, behind a cargo feature `watch` that is on by default**, the shape `object-store`
already has: named so that `--no-default-features` removes it, and on so that the binary in the
README is the binary that ships. The code moves from `nuthatch-tui-client`, which we own and which is
`MIT OR Apache-2.0`, so porting it is allowed; that repository is then archived with a pointer.

**RFC-0046 §1's deletion test, applied honestly.** Delete `watch` from the tree and a self-hoster loses
a viewer and nothing else: no data, no route they depend on, no behaviour of `dev`. It passes, but it
is the wrong test for the default-versus-feature question, because it passes for any read-only client.
The question that decides it is whether 20 crates and about 0.4% are worth the subcommand being
present for everyone who installs with `curl | sh`. I think they are, for the reason in option 2. If
Chief prefers off by default, the only loss is that the README clip must say which build it shows.

## 8. One view per cursor

A cursor is a chain (RFC-0021). Pointed at a runtime root, the client reads `/nests`, groups mounts by
chain, and draws one engine room per cursor: THE TIP, THE PRESS and REORG from that chain's nests'
`nest`-labelled series and their own `/<name>/ready`, never from the process-global gauges, which in a
multichain runtime report whichever cursor polled last (the mainnet nest once reported Arbitrum's tip;
`src/serve.rs` records it). BOILER and FEED LINES are process-wide and drawn once. Wide terminals show
cursors side by side; narrow ones one at a time, `n` and `N` to step. Nests sharing a cursor show
together under it, because they share its tip and its budget. An RFC-0063 §14 directory of
address-history nests is the same case: one chain per subdirectory, one cursor each.

## 9. Degrading

- **Narrow.** At 100 by 31 every panel shows (the floor the existing client documents); below 80 columns
  the strips go and readouts stack; below 60 only the nameplate and the numbers.
- **No colour.** `NO_COLOR` removes every colour escape; state is carried by words (`STALE`,
  `FAILOVER`) and reverse video, never by colour alone.
- **Piped, or `TERM=dumb`.** No screen. One line per interval to stdout, the same numbers: chain, tip,
  lag, sealed through, RSS against budget, RPC endpoints up, requests per second, `NO SIGNAL` where
  absent. It is a log line, not a health check; `/ready` remains the thing to gate on.

## 10. Against the non-negotiables

- **Single binary.** Yes; it is a subcommand.
- **2 GiB per cursor.** The client runs in its own process. The server additions are a fixed ring and
  two gauges per nest; §6 and S0 measure the watched cost.
- **No phone-home.** It talks to the `--url` it is given and to nothing else: no update check, no font
  or asset fetch, no default remote. ssh is invoked only with `--ssh`.
- **Determinism.** It reads; nothing it does reaches stored state. G1 and G2 are serving-layer reads.
- **Licence.** As §7.
- **RFC-0044 §8.** A new subcommand in the default binary, and two admin-surface additions. Chief's line
  in `CLAUDE.md` is needed before S1.

## 11. Non-goals

- Grafana. No history beyond what is on screen, no stored series, no queries over time, no alerting;
  `/metrics` into Prometheus remains the answer for those.
- Control. No pause, unmount, reseal or config edit. Read-only, as the cockpit is.
- A second cockpit. Panels that need tables, SQL or config stay in `/_admin/`.
- A fleet view across processes. One `--url` at a time.

## 12. Slices and their gates

- **S0, cost and look.** (a) Build the release binary with the feature on both release targets;
  gate: growth at most 1 MiB and `cargo deny check` green. (b) Run the tip-lag bench and an RSS
  sample with one watcher at the §4 defaults and without, on the ThinkPad; gate: both within the
  bench's own unwatched spread. (c) Render the panels from captured endpoint bodies and record a clip
  for Chief. **Stops it** if (a) or (b) fails, or if Chief looks at the clip and says no.
- **S1, the line mode.** The piped output of §9, using only reqwest and tokio already in the tree, so it
  ships nothing new. Gate: against a solo `dev` and a two-chain runtime, every number on the line equals
  the endpoint's value in the same read; with each field removed in turn, the line says `NO SIGNAL`.
- **S2, one cursor's engine room.** ratatui, the panels of §4 less DISPATCH, ported from
  `nuthatch-tui-client` and restyled. Gates: snapshot tests on ratatui's `TestBackend` at 120x40, 80x24
  and 60x20 from recorded bodies; `NO_COLOR` output holds no colour escape; no colour literal outside the
  theme; every series the client reads is emitted by `METRICS.render()`, checked in a test; S0(b)
  repeated with the real client.
- **S3, G1 and G2.** Gates: point-read p50 and tip-lag CI benches unchanged; a test that the ring holds
  no query text; `/_admin/requests` gated exactly as `admin_events_gated_like_the_admin_page` asserts
  for the event stream; reorg gauges absent before a reorg and correct after the reorg property test's
  fixtures.
- **S4, one view per cursor.** Gate: on a two-chain runtime, each cursor's tip is its own chain's, from
  a test that fails if the process-global gauge is read instead.
- **S5, the recording.** A `vhs` or asciinema tape in the tree, recorded from a real nest, with the
  `--poll-interval` it used in the caption. Gate: replaying the tape against a fresh nest produces the
  same screen layout; no figure in the clip is synthetic.

## 13. Risks

- **It drifts from the cockpit.** Two read-only views of one nest can disagree. Both read the same
  routes, and S2's series test keeps the client honest; the cockpit has its own.
- **The clip oversells the default.** Out of the box the tip moves every five minutes. The clip will be
  recorded at a short poll interval and must say so, and THE TIP shows the countdown to the next poll so
  a quiet default reads as quiet, not dead.
- **macOS `/metrics` forks `ps`.** At 0.5 Hz that is one fork every 2 s for as long as someone watches.
  Small, but a reason not to raise the default rate.
- **Terminal variety.** crossterm covers the targets nuthatch ships (Linux and macOS); Windows is not a
  release target.

## 14. Open questions for Chief

1. Default-on feature, as recommended, or off by default with the clip labelled as a feature build?
2. The name: `nuthatch watch`, or `nuthatch top`, which says what it is to anyone who has used `htop`?
3. Archive `nuthatch-tui-client` once S2 lands, or keep it as the client for older binaries?
4. Should `/_admin/requests` carry the first characters of a `/sql` statement? This RFC says no, and
   shows the route only.
5. Is the 2026-10-03 preference for the cockpit over a TUI still the rule for operator work, with this
   as the exception made for the showpiece, or does it lift?

## 15. S0, measured 2026-10-22

The prototype is `tools/rfc-0069-s0/`: `watch.rs`, the subcommand as one module, and `wire.patch`,
which compiles it into the binary behind a `watch` feature on by default (`ratatui` 0.30.2 with
`crossterm` and `layout-cache`, a `Watch` arm in `cli.rs` and `main.rs`). Neither is merged; the
measurements below are of the base commit `0a082a3d` with and without that patch. It reads `/ready`
and `/` every second, `/metrics` every two, `/nests` every thirty, and `/_admin/requests` only with a
token, and a recording made with `--record` or `capture.sh` replays through the same code with
`--replay`.

`nuthatch-tui-client` is `github.com/nuthatch-org/nuthatch-tui-client` (a clone sits at
`~/Projects/nuthatch-tui-client`): `src/api.rs`, `app.rs`, `ui.rs`, `worker.rs`, `tunnel.rs`,
`picker.rs`, `config.rs` and `format.rs`, with 1,694 of its 4,549 lines in `tests.rs`. S0 did not
port it. It uses blocking reqwest on a worker thread and ratatui 0.29; the prototype is async on the
binary's own tokio and reqwest. S2 ports its ssh forward (`tunnel.rs`), its `/nests` stepping
(`picker.rs`) and its tests' recorded bodies, and leaves its `/sql` feed behind, as §4 says.

| Gate | Result |
|---|---|
| (a) Binary growth, macOS arm64, `--profile dist` | **Pass**: 96,947,552 to 97,261,984 bytes, +314,432 (0.30 MiB, 0.32%) |
| (a) Binary growth, Linux x86_64, `--profile dist` | **Pass**: 118,089,520 to 118,448,656 bytes, +359,136 (0.34 MiB, 0.30%), built on the ThinkPad |
| (a) `cargo deny check` with the feature on | **Pass**: advisories, bans, licences and sources ok. Three new duplicate warnings, `darling`, `darling_core` and `darling_macro` 0.24 beside 0.23, build-time only |
| (b) Tip-lag after-seen p50, one watcher | **Pass**: 23 and 26 ms watched, inside 19 to 29 ms unwatched |
| (b) Nest RSS, one watcher | **Pass**: median 74,372 and 74,544 KiB watched, inside 73,852 to 74,596 KiB unwatched; peaks 76,472 and 78,100 KiB inside 75,808 to 78,488 |
| (c) Panels from captured bodies, and a clip | **Done**: 300 s of a live mainnet nest, replayed. **Open**: Chief watching it |

The feature adds 23 crates on both targets, by name and version against the same commit without it:
§7's 20 and `darling` 0.24's three. Every one is MIT, Apache-2.0 or both.

**(b), on the ThinkPad.** `watched-tip-lag.sh` runs `.github/workflows/tip-lag.sh` unchanged (100
samples, 5 warm-up, the footprint fixture, `--poll-interval 1s`) with the `dist` binary above, samples
the nest's `VmRSS` from `/proc` every second, and with `WATCH=1` attaches one `nuthatch watch` in a
120 by 40 pty at the defaults, recording its reads. Runs alternated, unwatched first:

| Run | Watched | Watcher reads | After-seen p50 | After-seen p99 | Lag p50 | RSS median | RSS peak |
|---|---|---|---|---|---|---|---|
| u1 | no | - | 24 ms | 48 ms | 538 ms | 73,852 KiB | 75,808 KiB |
| w1 | yes | 277 | 23 ms | 46 ms | 456 ms | 74,372 KiB | 76,472 KiB |
| u2 | no | - | 19 ms | 32 ms | 431 ms | 74,360 KiB | 76,396 KiB |
| u3 | no | - | 29 ms | 49 ms | 535 ms | 74,596 KiB | 77,752 KiB |
| w2 | yes | 279 | 26 ms | 43 ms | 566 ms | 74,544 KiB | 78,100 KiB |
| u4 | no | - | 29 ms | 75 ms | 479 ms | 74,388 KiB | 78,488 KiB |

About 2.5 reads a second over each watched run, which is the defaults. w2's lag p50 sits 28 ms above
the unwatched range; lag moves with where a block lands in the poll interval, which is why it is
tracked and not gated (`docs/benchmarks.md`), and the gate here is after-seen and RSS.

§6 expected `/metrics` to be cheaper on Linux, and it is. On the ThinkPad, 300 sequential requests per
route against an idle fixture nest, CPU from `/proc/<pid>/stat` in 10 ms ticks: `/ready` 750 B, p50
0.49 ms, 0.33 ms of nest CPU a request; `/` 780 B, 0.48 ms, 0.33 ms; `/metrics` 17.7 KB, 0.78 ms, 0.63
ms, against 1.5 ms on macOS. At the defaults that is about 1 ms of CPU a second, a tenth of a percent of
one core. The tick makes each figure good to about 0.03 ms.

**(c), the look.** `capture.sh` recorded 760 reads over 300 s from a copy of `defi-pulse-nest` run by
the 5.9.0 release binary on this MacBook: `dev --backfill 4000 --seal-direct --concurrency 2
--poll-interval 12s` against `ethereum-rpc.publicnode.com` and `eth.drpc.org`, from launch. The
clip is `engine-room.tape` replaying that file at 5x with the `dist` build above, 48 s, GIF and MP4.
Every figure on it is a body the nest served; the replay changes only the pace, and the footer says
so with the recording's start time.

What drawing real bodies turned up:

1. **`/` does not name the nest.** Its `name` is `nuthatch`. The nameplate takes the nest's name from
   the `nest` label on `/metrics`, so no further route is read.
2. **Lag in seconds has no source.** `/ready` carries no tip timestamp, so §4's "lag in seconds" cannot
   be drawn honestly. THE TIP shows the age of the last poll and the countdown to the next instead.
3. **`nuthatch_rows_sealed_total` stays 0 across a seal-direct pass**, while the same nest reports
   465.7 KiB of sealed segments; only the tip-seal path counts its rows. THE PRESS shows the 0 the nest
   serves. Filed as #2339.
4. **At a 12 s poll, FEED LINES is idle in most 2 s intervals**, so a per-interval mean latency reads
   `idle` more often than a number. S2 should keep the last interval that asked anything, with its age.
5. **The seal-direct pass on 4,000 blocks took about 2 s**, so the clip opens with it done and shows one
   stamp in THE PRESS. Mainnet seals tip history every 1,800 blocks, about six hours, so a five-minute
   recording shows the seal lag growing, not a second stamp. S5's recording wants a longer backfill.
6. `/nests` answers 404 on a solo nest, and the budget line reads one cursor, as §8 expects.

Not verified in S0: the line mode (S1); narrow terminals, `NO_COLOR` and 256-colour mode on a real
terminal (the code paths exist, only truecolour was looked at); the ssh forward; a multichain runtime
(§8, S4); the token path, since `/_admin/requests` does not exist until S3; the watched cost on macOS,
where `/metrics` forks `ps`; and a clean build-time difference, because the four `dist` builds shared
machines with each other.

§14, as S0 took them, from the RFC's own recommendations: the feature is on by default (1, and the
record in `CLAUDE.md` says so); the subcommand is `nuthatch watch` (2); `nuthatch-tui-client` is
archived with a pointer once S2 lands (3); `/_admin/requests` carries the route only, never SQL text
(4); and the 2026-10-03 preference is lifted for this RFC only, with the cockpit still the browser
surface for operator work (5).

## 16. S1, the line mode, built 2026-10-22

Chief watched S0's clip on 2026-10-22 and said build it, answering §14 as recommended: on by default,
`nuthatch watch`, `nuthatch-tui-client` archived once S2 lands, route only in `/_admin/requests`, and
the cockpit preference lifted for this RFC only. Engine room is the only programme running.

`src/watch/` holds the subcommand behind the `watch` feature, on by default and adding no crate in
this slice: `prom.rs` parses the exposition, `state.rs` folds each read into one `App` (S2 draws from
the same state), and `line.rs` writes the line. Each pass reads `/ready`, `/`, `/metrics` and `/nests`
together, then each mount's `/<name>/ready` when `/nests` lists any, and prints one logfmt line per
nest:

```
nest=defi-pulse chain=mainnet tip=26249645 lag=0 sealed=26249579 rss=191430656 budget=2147483648 rpc_up=2/2 http_rps=2.0
```

`--record` writes every read with its pass number, so a line can be checked against the bodies it came
from. `tools/rfc-0069/line-gate.sh` runs the line against a live nest with `--record`, recomputes every
field from the recorded bodies in jq, which shares no code with the client's parser, and compares.

| Gate | Result |
|---|---|
| Solo `dev` (`defi-pulse`, mainnet, `--poll-interval 12s`, two public RPCs), 60 s | **Pass**: 30 passes, 30 lines, 270 fields, 0 mismatches (2 `NO SIGNAL`, the first pass's two rates) |
| Two-chain runtime (`defi-pulse` on mainnet, USDC on Base, public RPCs), 60 s | **Pass**: 30 passes, 60 lines, 540 fields, 0 mismatches (4 `NO SIGNAL`, as above per line) |
| Each field's source removed in turn | **Pass**: `a_missing_source_reads_no_signal` drops each of the nine sources (the `nest` label, `/`'s chain, `tip`, `lag_blocks`, `sealed_through`, RSS, the failure counters, the HTTP counter, and `/nests` for the budget) and the field reads `"NO SIGNAL"` |

The gate script can fail: a binary that reports the previous read's RSS (M5 below) gives 3 mismatches
in 6 passes against the same solo nest.

Decisions the RFC left open:

1. **logfmt, raw integers.** Bytes and blocks print as the endpoint served them, so a line can be
   compared exactly and parsed by anything; `"NO SIGNAL"` is quoted so the line stays logfmt.
2. **One line per nest.** In a runtime, each mount's line comes from its own `/<name>/ready`, with its
   chain from `/nests`; RSS, budget, RPC and HTTP are process-wide and repeat on each line.
3. **`rpc_up` is endpoints whose failure counter did not move since the previous pass, of all listed.**
   Counters cannot say "up"; a failure in the interval is the nearest honest reading. The first pass
   has no previous one and reads `NO SIGNAL`, as does `http_rps`.
4. **`sealed` is `NO SIGNAL` while `sealed_through` is 0**, which `/ready` serves beside a null seal lag
   before anything has sealed.
5. **The budget counts distinct chains in `/nests`**, not mounts: nests on one chain share a cursor.
   A runtime with no mounts has a budget of 0.
6. **The interval is the nest's own poll interval held between 2 and 30 s** (the shortest mount's in a
   runtime), as `nuthatch-tui-client` did, unless `--interval` says otherwise.
7. **`/` is never the nest's name** (S0, finding 1): the solo name is the `nest` label on
   `nuthatch_nest_last_block`.
8. In this slice the line is written whether or not stdout is a terminal; S2 draws the screen on a
   terminal and keeps the line for a pipe and `TERM=dumb`.

Mutations, each applied to the committed tree, shown applied, run, and restored from `HEAD` to a clean
tree:

| Mutation | Result |
|---|---|
| M1 `sealed` prints `sealed_through` 0 as 0 | red: `nothing_sealed_yet_is_no_signal_not_zero`, `a_runtime_writes_one_line_per_mount_from_its_own_ready` |
| M2 `rpc_up` counts cumulative failures, not the interval's | red: `every_number_is_the_endpoints`, `a_missing_source_reads_no_signal` |
| M3 a runtime's lines read the root `/ready` | red: `a_runtime_writes_one_line_per_mount_from_its_own_ready` |
| M4 the budget counts mounts, not chains | red: `a_runtime_writes_one_line_per_mount_from_its_own_ready` |
| M5 RSS from the previous `/metrics` read | red: `every_number_is_the_endpoints`, `a_missing_source_reads_no_signal`; and the live gate, 3 mismatches |
| M6 an absent number printed as 0 | red: three tests |

The first fixtures could not tell M2 or M4 from the code: one failing endpoint was also the only one
with failures, and the runtime had one mount per chain. A third endpoint with old failures and a third
mount on mainnet make both visible.

Not verified in S1: `--no-default-features` beyond the help-group test and a lib build; the line on
Linux (the client is plain reqwest and tokio, as `nuthatch sql` is); a nest behind a token, since the
line reads no admin route.

## 17. S2, one cursor's engine room, built 2026-10-22

On a terminal `nuthatch watch` now draws the engine room; piped or under `TERM=dumb` it writes S1's
line. The `watch` feature gains `ratatui` 0.30.2 (`crossterm`, `layout-cache`), the crates S0 measured.
New in `src/watch/`: `theme.rs` (the Foundry palette, the only place a colour is named), `screen.rs`
(the panels), `series.rs` (every `/metrics` name the client reads), `tunnel.rs` and `picker.rs`
(ported from `nuthatch-tui-client`), and `tests.rs`. THE TIP, THE PRESS, BOILER, FEED LINES and REORG
are drawn; DISPATCH waits for S3's route. Pointed at a runtime's root, the screen draws one mount at a
time, `n` and `N` to step, from that mount's `/<name>/ready` and `nest`-labelled series; S4 groups them
by cursor. `--record` and `--replay` go through the same `App::apply` as a live read.

The recorded bodies are `tests/fixtures/watch/`: 45 s of the `defi-pulse` copy from S1's gate and 45 s
of the two-chain runtime, each read at 1 s by S1's line mode with every sixth `/metrics` read kept, no
body edited. `nuthatch-tui-client`'s recorded bodies are 3.10.0's and lack the `nest` label this client reads its
per-nest series by, so its tests were ported (the ssh forward, the picker, the absent-is-not-zero
cases) and its bodies were replaced by these.

| Gate | Result |
|---|---|
| `TestBackend` snapshots at 120x40, 80x24 and 60x20 from recorded bodies | **Pass**: seven snapshots (solo at the three sizes and 50x16, runtime at the three and its second mount), and a test that reads the tip, indexed block, seal and nest name out of the recording and finds each on the screen, so a snapshot cannot quietly agree with a wrong drawing |
| `NO_COLOR` output holds no colour escape | **Pass**: both recordings at all three sizes through `CrosstermBackend`, every SGR sequence parsed: no 30-37, 38, 40-47, 48, 58, 90-97 or 100-107. The same screen in truecolour does write them, so the check can see one. The state word stays reversed without colour |
| No colour literal outside the theme | **Pass**: every file of `src/watch/` but `theme.rs` is free of `Color::`, `Rgb(`, `Indexed(` and escape literals, and a second test fails if a file is added to the module without being listed for the check. A third asserts no cell sets a background and 256-colour mode never uses one of the 16 named colours |
| Every series the client reads is emitted by `METRICS.render()` | **Pass**: `every_series_the_client_reads_is_emitted` renders a fresh registry with one nest and one RPC call and finds all 16 names in `series::ALL`; `the_watcher_names_series_only_here` fails if a series name appears as a literal anywhere else in the client |
| S0(b) repeated with the real client, ThinkPad | **Pass**: after-seen p50 18, 19 and 20 ms watched, inside 18 to 20 ms unwatched; RSS median 74,312, 74,188 and 74,524 KiB watched, inside 73,480 to 74,676 KiB unwatched; peaks inside too |

S0(b), repeated as S0 ran it (`tools/rfc-0069-s0/watched-tip-lag.sh`, 100 samples, the footprint
fixture, `--poll-interval 1s`, the watcher in a 120 by 40 pty at its defaults), with the S2 tree's
`dist` build, alternating:

| Run | Watched | Watcher reads | After-seen p50 | After-seen p99 | Lag p50 | RSS median | RSS peak |
|---|---|---|---|---|---|---|---|
| u1 | no | - | 19 ms | 30 ms | 460 ms | 74,676 KiB | 78,624 KiB |
| w1 | yes | 287 | 18 ms | 32 ms | 517 ms | 74,312 KiB | 76,948 KiB |
| u2 | no | - | 20 ms | 32 ms | 541 ms | 73,688 KiB | 76,196 KiB |
| w2 | yes | 277 | 19 ms | 31 ms | 534 ms | 74,188 KiB | 76,272 KiB |
| u3 | no | - | 20 ms | 30 ms | 508 ms | 73,480 KiB | 75,620 KiB |
| w3 | yes | 274 | 20 ms | 31 ms | 453 ms | 74,524 KiB | 78,388 KiB |
| u4 | no | - | 18 ms | 33 ms | 534 ms | 74,076 KiB | 76,268 KiB |

The watcher read `/ready` and `/` every second and `/metrics` every two, about 2.5 reads a second, as
in S0. The Linux `dist` binary of this tree is 118,581,200 bytes, 491,680 (0.47 MiB) above S0's
118,089,520 for the same base without the feature; S0's prototype added 359,136.

Decisions the RFC left open:

1. **Lag in seconds is not drawn** (S0, finding 2): THE TIP shows the poll interval, the age of the
   last poll and the countdown to the next, counted from the instant `/ready` was read.
2. **RPC mean latency keeps the last interval that asked anything** (S0, finding 4), with its age
   beside it, `57 ms 6s`; `-` until an interval has asked.
3. **`/ready` is read every second regardless of the nest's poll interval**, not stretched to it as §4
   suggested: the countdown and `STALE` need a fresh read, and S0(b) measured the cost at 1 Hz. `STALE`
   is no answer from `/ready`, or none for three read periods (at least 5 s).
4. **The nameplate's name is the `nest` label** (S0, finding 1), and in a runtime the mount's name.
5. **`watch.toml`, not `nests.toml`.** Named targets live in `~/.config/nuthatch/watch.toml`, same
   tables as the old client's file less `decimals`, which fed the `/sql` panel and is refused by name.
   The list is offered only on a terminal, with no `--url`, `--ssh` or `--nest`.
6. **Below 80 by 24 the panels stack** into one borderless column, strips and hints gone; below 60
   columns RPC becomes one `up` line and the pool row goes. Inside a panel narrower than 50 columns the
   long forms shorten and FEED LINES drops its RETRY column (a retry still lights `FAILOVER`).
7. **The client reads the `nest`-labelled series only**, with no fallback to the process-global ones,
   so the series test covers everything it reads; a nest too old to label them reads `NO SIGNAL`.
8. **`/sql` is never read.** The old client's feed is not ported.

Mutations, each applied to the committed tree, shown applied, run, and restored to a clean tree:

| Mutation | Result |
|---|---|
| `NO_COLOR` mode still paints the 256-colour index | red: `no_color_writes_no_colour_escape` |
| One ink's index set to 8, a named ANSI colour | red: `colours_come_from_the_palette` |
| RPC mean shows `idle` instead of the last interval's figure | red: the solo and runtime snapshots, `rpc_mean_keeps_the_last_interval_that_asked_anything` |
| `STALE` never fires | red: `a_nest_that_stops_answering_is_stale` |
| A series named as a literal in `line.rs` | red: `the_watcher_names_series_only_here` |
| A series renamed in `series.rs` | red: `every_series_the_client_reads_is_emitted`, both snapshot tests |
| The nameplate named from `/` | red: the solo snapshots, `the_screen_shows_the_recordings_own_figures` |
| The countdown frozen at the read | red: `the_poll_countdown_runs_between_reads` (the snapshots alone could not see it, so the test was added) |
| Full layout from 81 columns | red: both snapshot tests |
| A runtime mount drawn from the root `/ready` | red: `a_runtime_draws_its_selected_mount` |
| A background set beside every foreground | red: `colours_come_from_the_palette` |

`nuthatch-tui-client` is ready to be archived with a pointer to `nuthatch watch` once this lands;
it has not been touched.

Not verified in S2: the screen on a real terminal other than through `script` and the snapshots (the
pty run on the two-chain runtime drew its tips and `mount 1 of 2`); 256-colour mode by eye; the ssh
forward against a real host (the tests use a stand-in `ssh` that refuses a key or listens with `nc`);
the picker on a terminal; macOS's watched cost, where `/metrics` forks `ps`.
