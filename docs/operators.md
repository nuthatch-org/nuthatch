# Running nuthatch as an operator

Everything needed to run nuthatch on your own infrastructure, or on someone else's behalf: how to
deploy it, what it demands of you, how it fails, what to scrape, what to back up, and where it is
still honestly unfinished.

**Two audiences, one document.** If you are putting nuthatch on a box, start at
[Deploy recipes](#deploy-recipes). If you are a platform team deciding whether to adopt it, read
[The division of labour](#the-division-of-labour), [Known gaps](#known-gaps), and the
[Go-live checklist](#go-live-checklist) first.

**Companions:** [`prod-readiness.md`](prod-readiness.md) is the *release* gate - what must be true
before a build ships. This document is the *run* guide - what must be true in your environment.
[Reading sealed segments without nuthatch](reading-segments.md) is the contract for pointing
DuckDB, DataFusion, or another engine at the same Parquet directory. What is deferred and why
lives in the [issue queue](https://github.com/nuthatch-org/nuthatch/issues)
(the `parked` label means *decided against for now*, not *forgotten*); [`backlog.md`](backlog.md)
explains how to read it and the [RFC index](rfcs/README.md) says what each RFC is.

Written against **2.0.0** (2026-08-06) and re-read against **4.10.1** on 2026-10-05, by running
what it describes: every command, flag, route, metric name and default below was checked against
that binary or its source, and the figures that could not be re-sourced were removed. A figure that
names its date is a measurement from that day, not a promise about today. Read
[Known gaps](#known-gaps) before exposing `/sql`.

---

## The division of labour

nuthatch is built to be **fronted**, not exposed raw. The dividing line, stated once and kept:

> **The node owns resource safety. The gateway owns access policy.**

**nuthatch provides:** deterministic indexing, isolated per-nest storage, a bounded query surface,
per-nest health and metrics, and signals rich enough to alert, capacity-plan, and bill against.

**Your platform provides:** identity, authentication, authorisation, per-caller rate limits, quotas,
metering, billing, and TLS. There are no accounts and no tenancy inside the binary, and there will not
be: `CLAUDE.md` puts hosted-SaaS multi-tenancy out of scope permanently.

**What it never does:** phone home unasked. No telemetry by default, no mandatory API tokens, no gated
data service. Every outbound connection is one you configured - your RPC endpoints, your webhook
sinks, and (only when you run them by hand) the ABI resolvers and sanctions-list fetchers - with one
exception you have to say yes to: the head count below.

### The head count (RFC-0054)

After the first successful `nuthatch init` on a machine with a terminal, nuthatch asks once whether
it may count you, and shows the exact object it would send. The default is no, and the answer is
stored in `~/.config/nuthatch/count.toml` (or under `$XDG_CONFIG_HOME`), so it is never asked
again. With a yes, each later `init` sends one object - nuthatch version, OS, architecture, a
built-in chain id (anything else is sent as 0) and which kind of `init` it was - with no identifier
and no addresses, under a two-second ceiling, and nothing about a failure is ever printed. Nothing
is asked or sent when `CI` or `NUTHATCH_NO_COUNT` is set. `nuthatch count` shows the state,
`nuthatch count payload` prints the objects, and `nuthatch count off` stops it.

---

## Deploy recipes

`nuthatch dev` **is** the serve command - it backfills, follows the tip, and serves the API in one
process. "I tried it locally" to "it's running on my box" is just running that under a supervisor.

### systemd

```ini
# /etc/systemd/system/nuthatch.service
[Unit]
Description=nuthatch indexer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=nuthatch
WorkingDirectory=/var/lib/nuthatch/mynest        # the nest directory (holds nuthatch.toml)
ExecStart=/usr/local/bin/nuthatch dev --listen 127.0.0.1:8288 --seal-direct --concurrency 8
Restart=on-failure
RestartSec=5
# Remote admin is OFF as written: the bind above is localhost, so the admin UI needs no token.
# To enable it you must change the bind AND choose a token: generate one, never paste a literal:
#   Environment=NUTHATCH_ADMIN_TOKEN=<openssl rand -hex 32>
# Keep it inside the footprint budget; the box needs headroom for analytical queries.
MemoryMax=2G

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload && sudo systemctl enable --now nuthatch
journalctl -u nuthatch -f          # a clean progress line during backfill, then quiet tip-following
```

#### Rolling a unit onto a new version

`scripts/deploy-nest.sh install <binary> <version>` puts the binary at
`/usr/local/bin/nuthatch-<version>`; `scripts/deploy-nest.sh roll <unit> <version> --smoke <file>`
points the unit at it, restarts, and waits until `/ready` reports that version. It then runs every
statement in the smoke file against `/sql`. Nuthatch's production units keep their smoke files,
one per unit and one statement per line, in the private kittiwake repo under `nuthatch-gate/smoke/`,
because they carry its statements; `deploy/roll-helsinki-from-mac.sh` reads them from `SMOKE_DIR`
(default `~/Projects/kittiwake/nuthatch-gate/smoke`) and copies each unit's file to the box.

Before it touches a unit, `deploy/roll-helsinki-from-mac.sh` reads the release tag's commit statuses
and refuses unless every rolled unit's `release-gate/<nest>` status is `success`. It names each red,
pending or missing status with its description and link. `--override '<reason>'` rolls anyway and
prints the reason at the top of the roll's output.

When a statement fails, the roll puts the unit back on its previous binary and runs the failing
statements there. If the previous binary answers any of them, the new one is worse: the unit stays
reverted. If the previous binary refuses all of them too, the failure is pre-existing, not a
regression: the roll says so, naming each statement, and moves the unit back onto the new version.

| Exit | Meaning | Unit left on |
|---|---|---|
| 0 | rolled, and every smoke statement answered | the new version |
| 1 | any failure, including a smoke failure the previous binary does not share | the previous version, after a smoke failure |
| 2 | usage | untouched |
| 3 | smoke failures the previous binary shares | the new version |

Exit 3 is non-zero on purpose, so a loop over several units stops on it.

### Docker

A container image is published per release:

```sh
docker run -d --name nuthatch --restart unless-stopped \
  -v "$PWD/mynest:/nest" -p 127.0.0.1:8288:8288 \
  ghcr.io/nuthatch-org/nuthatch:5.6.3
```

> **No admin token, deliberately.** The image's `CMD` binds `0.0.0.0:8288` inside the container, so
> the bind is not localhost and `NUTHATCH_ADMIN_TOKEN` decides whether the admin routes exist at all.
> Without one they are **not mounted**, the right default for a command people copy-paste. Turning
> remote admin on is a deliberate second step: see [Enabling remote admin](#enabling-remote-admin).

The image **ships the same binary attached to the GitHub Release** rather than a separate from-source
build, so the two cannot drift. It runs as an unprivileged user (uid 10001), carries only
`ca-certificates` beyond the binary, and mounts the nest directory at `/nest` - the only writable state.

The default command binds `0.0.0.0:8288` *inside* the container; publish it to `127.0.0.1` on the host
as above and put a reverse proxy (TLS + auth) in front, the same posture as bare metal. `docker stop`
sends SIGTERM, which drains and checkpoints cleanly.

`linux/amd64` only for now. The release attaches an aarch64 Linux binary, but no multi-arch image is
built from it yet; on arm64, run the binary directly.
Pin the version tag rather than `:latest` for anything you care about.

> **The writer pool indexes as of 0.9.3.** Until then it did not: a worker registered, took a lease
> and reported, and ran no ingestion loop, so no rows appeared (issue #250). If you are on 0.9.2 or
> earlier, scaled mode does not write - upgrade, or use embedded mode.

**Scaled mode needs the `-scaled` image**, not this one. The default image is the embedded build and
carries no database driver. `nuthatch worker` and `nuthatch control` are still *listed* in its `--help`
- the CLI surface is shared - but running either gives you a refusal naming the feature flag rather
than a mysterious failure:

```
Error: the writer-worker role needs a build with `--features postgres-store`. The default
binary is the embedded one and carries no database driver (CLAUDE.md non-negotiable 1).
```

That is deliberate: a subcommand that vanishes from `--help` depending on how the binary was built is
harder to diagnose than one that explains itself. Use the scaled artifact and it works:

```sh
docker run --rm ghcr.io/nuthatch-org/nuthatch:5.6.3-scaled worker --help
```

Two images rather than one because non-negotiable 1 says the primary artifact runs with zero external
services - a binary carrying a Postgres driver is a different promise even when it behaves identically
unused. `-scaled` is deliberately **not** tagged `latest`: the default image stays the one you get by
not thinking about it. The release also attaches `nuthatch-scaled-x86_64-unknown-linux-gnu.tar.gz` for
anyone deploying without containers.

The release also attaches `nuthatch-graph-<target>.tar.gz` for Linux and macOS, built with
`--features graph`. It is the binary for serving a nest's Graph-dialect GraphQL, as a
[subgraph stopgap](subgraph-stopgap.md) does; the default binary has no `/graphql` route.

> **Do not use `:0.7.0`.** It is published but cannot start (`GLIBC_2.38 not found`) - the release job
> pushed before it smoke-tested, so the failing test failed the job without unpublishing the image.
> `0.7.1` is the first working tag, and the job now tests before it pushes. `:latest` was broken for the
> same reason and is fixed by `0.7.1`.

**glibc floor.** The Linux binary is dynamically linked and needs **glibc 2.35** (measured on the
4.1.0 artifact; 2.34 up to 4.0.2), so it runs on Ubuntu 22.04+, Debian 12+ and anything newer, and
not on RHEL 9 or Amazon Linux 2023, which ship 2.34. It is built on a pinned runner rather than
`ubuntu-latest` precisely so that floor does not drift upward unnoticed - which it had, to 2.39, until
the container image's smoke test caught it. The aarch64 binary is built on `ubuntu-22.04-arm` for the
same reason and has the same 2.35 floor (`hypot` and `hypotf`, measured on a 4.11.0 build, #1961).
Only the default and graph builds ship for aarch64; the scaled build is x86_64 only.

To build it yourself instead:

```sh
cargo build --profile dist --locked
cp target/dist/nuthatch . && docker build -t nuthatch .
```

---

## Deployment model

One static binary. No Postgres, no Docker daemon, no IPFS, no sidecar. State is a directory.

| Topology | Command | Shape |
|---|---|---|
| **Nest** (one indexer) | `nuthatch dev --dir <nest>` | one chain, one cursor, one API at `/` |
| **Many nests** | `nuthatch dev --dir <dir>` (with a `mounts.toml`) | N nests across one **or more** chains, one isolated cursor per chain, each mount's full API under `/<alias>/` |

A **cursor** is the unit that matters. It is always single-chain, single-writer, and one observable
failure boundary. A runtime hosting nests on Ethereum and Arbitrum runs two cursors in one process, each
with its own tip, finality view, reorg handling, and RSS budget. Two chains are never multiplexed
behind one cursor; the runtime refuses to.

Multichain is a **capability, not a mandate** - one chain per runtime stays valid and is the simplest
default.

### Scaled mode: a fleet across machines (RFC-0022)

Everything above is **embedded mode**, and it is still the primary deliverable: one binary, no
external services, the thing `curl | sh` gets you. Scaled mode is a different deployment for a
different problem - one operator running many nests across many machines - and it is **opt-in at
build time** (`--features postgres-store`). The published binary does not carry a database driver.

Reach for it when a single box can no longer hold your cursors inside its RAM budget, or when serving
load and ingestion load want to scale independently. Not before: several nests on one machine is simpler,
and simplicity is the point of the embedded path.

| Role | Command | Owns |
|---|---|---|
| **Control plane** | `nuthatch control --db <postgres>` | *desired state* - what should run |
| **Writer** | `nuthatch worker --control-db … --hot-store … --chains …` | cursors it holds a **lease** on; ingests, decodes, seals |
| **Query-FE** | `nuthatch serve --dir <nest> --hot-store <postgres>` | nothing - serves from shared state |

```sh
docker compose -f docker-compose.scaled.yml --profile fleet up \
  --scale writer=2 --scale fe=3
```

**The three ideas worth understanding before you run it:**

**1. The control plane states intent; it commands nothing.** `POST /nests` records that a nest should
run and returns 200. That does **not** mean it is running - it means the fleet has been told to. A
writer picks it up on its next tick. There is deliberately no "start this nest on worker w3" endpoint,
because that would be the one call able to put a cursor somewhere the scheduler did not choose and the
lease did not arbitrate.

**`worker` is not `dev`.** `dev` indexes a nest directory it owns outright and knows nothing about a
control plane; running two of those against one store is the double-writer bug rather than a pool.
`worker` reconciles - heartbeat, take a lease per assigned cursor, index only what it holds. A worker
declares the chains it *can* host (`--chains`); the scheduler decides which it *should*, and the lease
decides which it *does*. Its id defaults to the hostname, and it **refuses to start** if it cannot
determine one, because two workers sharing an id look like a single worker to the registry.

**2. Ownership is a lease, and the store enforces it.** A cursor is held by exactly one writer at a
time. If a writer stalls - long GC, paused container, a host that goes away - its lease expires and
another writer takes over. When the original wakes up, its writes are **refused by the store**, not
merely discouraged: every write carries a fence, and a stale fence is rejected inside the same
transaction as the write. On its next reconcile tick the stale writer learns the lease is gone, stops
indexing that cursor and keeps any others it still holds. This is why `--scale writer=N` is safe.

**3. The control plane and the lease are independent, on purpose.** A control-plane outage stops
*rescheduling*, not *ingestion* - writers keep their leases and keep working. It follows that the two
can legitimately disagree: a writer whose heartbeat has lapsed but whose lease is live keeps its
cursor, and the scheduler's wish to rehome it is refused. That is correct, not a bug. A plan is not
permission.

**Answering "why is my nest not running?"** - `GET /plan` runs the same placement logic the writers
run and reports what could not be placed *and why*:

```json
{"assign":[{"chain":"mainnet","worker":"writer-1"}],
 "unplaceable":[{"chain":"base","rss_mb":2400,"reason":"toolargeforanyworker",
   "detail":"this cursor alone exceeds the largest worker's budget - adding workers will not help"}]}
```

The two unplaceable reasons demand different actions: `noroomrightnow` is fixed by adding a worker,
`toolargeforanyworker` never is. Adding capacity for the second would be money spent on nothing.

**Versions are pinned fleet-wide.** Every FE node resolves an endpoint through the control plane
(`PUT /nests/<name>/pin`), not through the registry's movable `latest`. If each node read `latest`
itself, then during an upgrade one node would serve the new schema while another served the old, and
the same endpoint would answer differently depending on where the load balancer sent the request. A
declared-but-unpinned endpoint is explicitly **not servable** - an FE refuses rather than guesses.

**Workers pull the nests they are assigned.** A worker indexes what is under `--nest-root` and pulls
anything else from `--registry` (a directory, or `s3://bucket/prefix`). Without this a worker could
only run nests you had already copied onto that exact machine - and since the scheduler decides *which*
box holds a cursor, that meant declaring a nest centrally and then discovering the assigned worker had
nothing to run. Local always wins: a nest you placed on a box is a deliberate act, often a hand-edited
view, and is never silently replaced by the registry's copy.

**Pin the bundle, not just the version.** With a `bundle_hash` pinned, a worker fetches **by content
address** and never consults the registry's index, so re-tagging `1.0.0` in the registry cannot change
what any worker runs. Unpinned, your fleet is exactly as trustworthy as your registry. Pulled bundles
are cached at `<--nest-cache>/<name>/<hash>`, which is why re-pinning actually re-pulls instead of
quietly reusing what is already on disk - and why that cache is safe to delete at any time.

Keep `--nest-cache` off the `--nest-root` mount. The writer-node compose mounts nests read-only, on
purpose: nests are input, pulled bundles are runtime state, and mixing them is how a re-pinned fleet
would end up running the old bundle with nothing reporting an error.

**Secrets never enter a bundle.** Private RPC URLs and API keys live in the control plane keyed by
nest (`PUT /nests/<name>/secrets`), and a writer receives only the secrets of the nests it is
assigned. The interface is **write-only**: you can list which keys exist, never read a value back.
Rotating a secret changes no bundle hash, so it does not invalidate segment reuse or force a
re-index.

**What scaled mode does not do**, and will not: per-tenant billing, metering, quotas, or authz between
mutually-untrusting paying customers. That is a gateway's job in front of nuthatch, and deliberately
out of scope - see RFC-0022 §6.

```
runtime-dir/
  mounts.toml             # runtime state: chains + mount records (tenant, alias, nid) + budget
  mount-jobs.json         # the admin API's job table (3.13.0): every mount, move and resume job
  segments/               # SHARED sealed Parquet, content-addressed - two nests that decode the
                          #   same contract hold ONE copy here, not two (RFC-0033 §11a)
  data/
    <nid>/                # a dataset, keyed by NEST IDENTITY rather than by a name you chose
      nuthatch.toml       # contracts, events, factories, webhooks, alerts
      semantic.toml       # what the data means (drives MCP + SQL hints)
      queries.toml        # optional: the author's sanctioned query surface (RFC-0034)
      maintained.toml     # optional: views answered from a stored copy (RFC-0062)
      schema.json         # generated: registry hash + table list
      abis/               # vendored ABIs (no runtime resolution)
      views/              # authored SQL views
      checks/             # invariant/parity checks for `nuthatch check`
      nuthatch.redb       # hot store (mutable, reorg-affected)
      segments/
        manifest.json     # this dataset's segment catalogue + sealed watermark
      maintained/         # copies of maintained views, derived and safe to delete (RFC-0062)
```

The nest directory is the *entire* state. Move it, copy it, snapshot it.

**Serving several nests.** `GET /nests` is the roster. Every nest's full API lives under its prefix:
`/<name>/tables`, `/<name>/sql`, `/<name>/_admin/`. `/sql` stays per-nest scoped - a query sees one
nest's data. Stores are per-nest; only the cursor is shared. Static and factory nests co-exist, may
mount at different heights, and each backfills its own history; the cursor only couples them at tip.

Nests live in their own repositories rather than in-tree; see the
[nest catalogue](nest-catalogue.md) for what ships and what is planned.

> **Scaled mode exists** (RFC-0022): a Postgres hot store, a writer pool taking one lease per cursor,
> a query-FE tier, and a control plane holding desired state - see *Scaled mode* below. DataFusion
> federation (RFC-0013) was a separate question, never built, and closed on 2026-09-08.
>
> It is newer and less exercised than embedded mode, which runs in production. If one process per box
> is enough for you, that remains the recommended shape.

---

## Capacity and sizing

**The budget is per active-chain cursor: 2 GB RAM.** A runtime's total is the sum of its cursors. This
is a CI-enforced ceiling, not an aspiration, and `dev` **refuses to start** a cursor whose
projected footprint exceeds `max_rss_mb` (default 2048). It also refuses to start unless the
analytical split itself fits that ceiling (RFC-0047 C4):

```
(pools × analytics.memory_limit) + ingestion_reservation + runtime_headroom ≤ 2 GiB
```

`pools` is the smaller of `sql_permits` and the number of nest datasets on the cursor, since a
dataset's sessions share one pool however many permits it has (#1825). `sql_permits` is
cursor-wide (the same gate every nest on the cursor shares), default 2. The
analytics knobs are **runtime**, not nest identity: they live in the environment, never in
`nuthatch.toml`, so two operators with different RAM still share a nest content address.

| Key | Env | Default | What it is |
|---|---|---|---|
| `analytics.memory_limit` | `NUTHATCH_ANALYTICS_MEMORY_LIMIT` | 512MB | memory for a nest's analytics statements together: every session opened over one nest's dataset draws on one pool of this size, so a second concurrent statement competes for it rather than doubling it (#1792), and each dataset in a runtime has a pool of its own (two mounts of one dataset share it). A statement that needs more is refused, not slowed: hash joins and final aggregates do not spill |
| `analytics.threads` | `NUTHATCH_ANALYTICS_THREADS` | 2 (ceiling 16) | worker threads per session. Above 16 is refused; not a term in the RAM equation |
| `analytics.temp_directory` | `NUTHATCH_ANALYTICS_TEMP_DIRECTORY` | Linux: `$XDG_CACHE_HOME/nuthatch` or `~/.cache/nuthatch`; elsewhere the process temp dir | parent of per-session spill dirs (`nuthatch-spill-{pid}-{seq}`; do not point two processes at one directory). Not `/tmp` on Linux by default, because that is often a tmpfs, where spill is RAM outside the per-cursor budget; do not point this at one |
| `analytics.max_temp_size` | `NUTHATCH_ANALYTICS_MAX_TEMP_SIZE` | 2GB | spill per analytics session. A `/sql` query that spills past it is stopped and answered `507`. On disk it does not buy room in the equation above; on a tmpfs it is RAM |
| `ingestion_reservation` | `NUTHATCH_INGESTION_RESERVATION` | derived: 1024MB | the ingest path: runtime, decode, DBSP, redb. The derived 1024 is the remainder of today's 2 GiB split after 2 × 512 MB of analytics, **not** a measurement, and it also covers `runtime_headroom` below. Raising it is always allowed. Lowering it is allowed only together with a measured `runtime_headroom`, and never below 256MB; alone it is refused, because the 1024 also stands for what statements hold outside the pool (#1241, #1899) |
| the engine's limit | `NUTHATCH_BURRMILL_MEMORY_LIMIT` | `analytics.memory_limit` | takes the place of `analytics.memory_limit` where set, shared the same way, and is what each pool counts at in the split. Kept from the releases that carried two engines |
| the wall | `NUTHATCH_MAX_RSS` | 2048MB | the ceiling the split is held to, for a process given more than 2 GiB. **Raise-only**, and a statement by the operator, not a measurement: the footprint job measures the default and nothing above it |
| `runtime_headroom` | `NUTHATCH_RUNTIME_HEADROOM` | 0 (inside the derived reservation) | what the process holds outside the engine pool while it answers: the runtime, the unsealed rows each statement reads from the hot store, the segment lists it binds, and pages the allocator has not yet returned. Set it from a measurement, and with it the reservation may be measured too |

Sizes accept `512`, `512MB`, `1GB`, `2GiB`. `NUTHATCH_SQL_MAX_CONCURRENCY` remains the permit
count, still capped at 16, and is **not** an unconstrained config key. `analytics.threads` shares
that ceiling. Raising permits on a one-nest cursor costs no memory in the split, since its
statements share one pool; two datasets at 1024 MB each plus the derived ingest floor is 3072 MB,
and is refused at startup.

`ingestion_reservation` alone may be raised and not lowered, which is what makes the inequality worth
having. The consequence is the property to hold onto: **no configuration this gate accepts gives
analytics more RAM than the shipped default that the footprint CI job measures**, unless the operator
has raised the wall with `NUTHATCH_MAX_RSS`, or measured both terms below, and so said what the process holds. The gate is
arithmetic over the walls, not an enforcement of ingest RSS; ingest, DBSP, redb and result
materialisation are still bounded by the `max_rss_mb` wall and the footprint job, not by this sum.

**Measuring the two terms** (#1899, closed 2026-10-05 with the figures below, taken on 4.10.0), on a
copy of the nest, under production's settings, on the box that runs it:

- `NUTHATCH_INGESTION_RESERVATION`: the VmHWM of `nuthatch dev` on the copy with no queries, over a
  catch-up and a few polls. The QoS nest: 372 MiB.
- `NUTHATCH_RUNTIME_HEADROOM`: from four release-gate runs, the largest peak RSS less the pool held
  at that peak. The gate reads the pool every 0.5 s, so its reading can come from a moment other
  than the high-water mark; read `nuthatch_rss_bytes` and `nuthatch_analytics_pool_reserved_bytes`
  from `/metrics` together a few times a second and take the pool at the largest RSS. The QoS nest,
  at 4 threads: up to 931 MiB over twelve runs, so 960 MB. Before burrmill 1a1e326 it was up to
  1,200 MiB, and the pool had to be 1.25 GiB rather than 704 MB for every statement to answer.

Round each up. The headroom grows with `analytics.threads` and with how much a statement scans, so
measure again when either changes. A pool sized from these may still be too small for a statement
to run: the gate says so as a refusal, and that is the trade to take to whoever owns the budget.

**Ingestion liveness outranks query completion.** A query that cannot run in its budget fails naming
these keys. It never degrades block processing.

The projection model (deliberately rough):

| Component | MB |
|---|---|
| runtime base, paid once per process | 120 |
| each nest: hot store + decode registry + balance view | 5 (fitted at ~1 MB observed, with a 5x margin) |
| each additional IVM view (exposure, velocity) or factory child registry | 40 |
| each authored incremental entity: its circuit and thread | 8, plus 3,200 bytes per declared row |

`GET /nests` reports the projection as `projected_rss_mb` and each mount's share as
`estimated_rss_mb` (`0` for a second mount of a dataset already charged once). Provision against
the measurement (`nuthatch_rss_bytes`, also reported as `rss_bytes` on `GET /nests`) and treat the
projection as a guard rail rather than a sizing tool. A runtime holding one WETH dataset under two
mounts, forty blocks of history, measured 48 MB resident against a 125 MB projection on 4.10.1
(macOS, 2026-10-05); the same nest alone, at tip with 300 blocks behind it, measured 216 MB.

**What actually drives memory:**

- **Deep-finality chains.** The hot store holds everything between the sealed watermark and the tip.
- **`/sql` on a hot table.** The current query path materialises the whole tip per query. The single
  largest RAM risk on a busy nest (see [Known gaps](#known-gaps)).
- **Factory nests.** A template with many discovered children carries a larger child registry.

**Measure before committing.** `nuthatch bench backfill` reports events/sec, wall clock and peak RSS
over a pinned range; `nuthatch bench query` reports entity point-read p50/p99 plus the `/sql` hot-scan
cost and RSS. Run both against a representative nest on your hardware and your RPC before sizing a
fleet.

**Disk.** Sealed Parquet is zstd-compressed (level 3, the `zstd-bloom-v1` writer profile; segments
sealed before that profile existed are Snappy and still read) and content-addressed. Growth is
proportional to decoded events, not chain history: a nest tracking a few events on a few contracts
stays small.

---

### The analytical memo

A `/sql` answer is a function of its inputs: the statement and its row cap, the sealed watermark, the
hot store's write generation, each maintained entity's watermark, and the content of `nuthatch.toml`,
`views/*.sql` and `labels/*.json`. Since 3.6.0 the nest remembers answers under a hash of exactly
those, so a statement repeated while none of them has moved is answered from memory - before the
permit gate, since it costs no engine work - and reports `"cached": true`. One commit, one seal, one
edited view changes the key and the next request computes. It is not a TTL and cannot serve a stale
row: same key, same inputs, same rows. What it does is turn a dashboard's fifth request for the same
fold from seconds into microseconds, on a cursor that commits a handful of times an hour.

Measured on the Lodestar allocations nest, whose dashboard's 47 distinct statements took 89 s serially
after #1183: the four heaviest are whole-history folds at 5 to 16 s each, and every one of them repeats
within the panel's refresh. A degraded answer (a table reduced by an unreadable segment, a tip that
would not scan) is never remembered, and an answer larger than a quarter of the ceiling is declined
rather than evicting everything else. `NUTHATCH_SQL_MEMO_BYTES` sets the ceiling; `0` disables it.
The memo lives inside the same RSS budget as everything else - 64 MiB by default, which is why the
default is what it is and not larger.

### Maintained views

The memo helps a statement that repeats. A nest whose authored view is expensive whatever the
statement (a join-heavy view over the whole history, paged or filtered differently by each caller)
can instead declare that view **maintained** in `maintained.toml` (RFC-0062):

```toml
[[view]]
name = "bet"
```

The nest then answers that view from a Parquet copy of its own request-time evaluation, stored at
`maintained/<view>/<identity>.parquet` beside `segments/`. The identity is a hash of everything the
evaluation reads: the binary (its version and a digest of the executable) and the engine, the content
of `nuthatch.toml` and `views/*.sql` and the other authored inputs, the schema of the tables the view
reaches, their sealed segments and their hot rows. A copy is served only to a request whose inputs
hash to the same identity, so it cannot be stale and never differs from the view: a commit, a seal or
an edit that touches what the view reads, or an upgrade, simply finds no copy. That request answers
from the definition, at today's cost, and the copy for its inputs is built behind it. A block that
carries no rows for the view's tables changes nothing, and the copy stays current.

What it costs:

- **One build per identity**: one evaluation of the view, the work of one cold request reaching it.
  Builds run one at a time per cursor, each holding one of the cursor's `/sql` permits and the
  dataset's analytics pool while it runs, so a build is admitted exactly as a query is. A build waits
  behind requests for a permit rather than ahead of them.
- **Disk**: a copy is roughly the size of the view's rows. Each view keeps the copy just built and the
  four most recently built before it, so a reorg back to a recent state answers at once; `recent = N`
  at the top of `maintained.toml` changes the four. Copies are derived: deleting `maintained/` loses
  nothing but the time to rebuild them, and they are not published to a mirror.
- **Nothing for a nest that declares nothing.** No file, no directory, no metric, no `/ready` field.

`maintained.toml` is an authored input, so it is in the NID, and it is outside the data identity, so a
nest that declares a view still seeds from a mirror published without it. A view that cannot be
maintained refuses the nest at startup, by name, and fails `nuthatch check`: one that is not in
`views/*.sql`, one whose definition calls a volatile function such as `now()`, and in this release one
that reads an incremental entity, an offchain snapshot view, `labels` or a `{template}__children`
view, whose rows the identity does not cover yet. A historical read (`as_of`), a declared query and
`/explain` always answer from the definition.

`/ready` carries a `maintained` list, one entry per view with its copies on disk, their bytes, the
requests it answered from a copy (`hits`) and from the definition (`fallbacks`), its builds, build
failures, the last build's duration and the last error. `/metrics` has the same per nest and view as
`nuthatch_maintained_*`. A view whose fallbacks keep rising while its builds do not is one whose
inputs change faster than it can be built; it is answered exactly as before, just not faster.

## What a nest costs at tip

Capacity above is about RAM. This section is about a different bill: **RPC requests**, which is what
your provider actually charges for, and which does not stop after backfill - a nest following tip
keeps paying it for as long as it runs. "Be your own indexer" does not mean this cost disappears; it
means you are the one who sees it.

**Measured on our own reference deployment**
([#750](https://github.com/nuthatch-org/nuthatch/issues/750), audited 2026-08-22): four nests, one
week, **~11.8M RPC requests** against **~100 HTTP requests served** (the sum of the table below) -
roughly **118,000 RPC requests per HTTP request answered**. Only `graph-staking-nest`'s figure was
checked against the auditor's own `/metrics` probes and corrected down, from a raw 40 to **~39
external requests over 7.2 days** (about five a day) against its unchanged 3,954,332 RPC requests;
the other three nests' counts are as directly measured and may still include some of that same probe
traffic. So ~100, and the ~118,000:1 built on it, are if anything an understatement of the true RPC
cost per real request, not an overstatement.

| Nest | Role | RPC requests | HTTP requests served | Window |
|---|---|---:|---:|---|
| `graph-staking-nest` | Lodestar delegation feed | 3,954,332 | ~39 (corrected; excludes audit probes) | 7.2 days |
| `graph-gns-nest` | Lodestar developer-activity | 3,952,456 | 28 | 7.2 days |
| `horizon-nest` | Lodestar Oracle; paid Alchemy state RPC | 1,110,323 | 1 | 7.2 days |
| `doudouchain-v2-nest` | labelled *temporary*, 3 entities, 98 MB on disk; **stopped 2026-08-22** | 2,806,035 | 32 | 5 days |

The fourth row, explicitly labelled *temporary*, was stopped on the board's instruction once the
audit surfaced it: 2.8M RPC requests over five days to hold three entities. What remains running is
**~9M RPC requests a week** across the first three nests, and the audit's own conclusion about that
remainder is that none of it is waste - it is load established, not assumed, to be necessary (next
section).

### Why: `block_timestamps` is a header round trip, every block

The mechanism is the one already described under [Configuration
surface](#configuration-surface): a timestamp lives in the block header, not in the log
`eth_getLogs` returns, so serving `block_timestamp` costs one extra `eth_getBlockByNumber` per
distinct block (RFC-0029 §4). That cost does not end when backfill does - a nest at tip pays it again
on every new block, indefinitely.

**Since 3.11, only on a node that does not say.** Current execution clients put `blockTimestamp` on
every log (execution-apis, 2025), and nuthatch now takes the timestamp from there and fetches a header
only for a block whose logs lack it. Checked 2026-09-24 on Alchemy's Ethereum, Sepolia, Base, Arbitrum
and Arc endpoints and on the public Sepolia, Base and Arc ones: all carry it, and it matches the
header. The same 20,000-block Sepolia backfill paid **4,444** `eth_getBlockByNumber` on 3.9.0 and
**34** after, about 89,600 CU against 1,300, and sealed byte-identical segments. The headers left are
the poll loop's own, described below.

Arbitrum produces about 345,600 blocks a day. `graph-staking-nest` averaged **~549,000 requests a
day** over the audit window - the right order of magnitude for a header fetch per block plus its log
polling on top.

Both Lodestar panels turned out to need the column, confirmed by reading the app's own SQL rather than
assumed: `graph-staking-nest` filters on `block_timestamp` for a "last seven days" delegation view,
and `graph-gns-nest` uses it as the entity's `createdAt` for a weekly publication trend. So the column
stays on for both - not because it was left at its default, but because each nest's actual consumer
asked for it. That is a decision made against your own consumers, not a recommendation this page is
making either way; see the `block_timestamps` guidance under [Configuration
surface](#configuration-surface) for how to decide it for a new nest.

### What that costs against a priced endpoint

None of the request volume above was billed: three of the four nests run against
`arb1.arbitrum.io`, the same class of free public endpoint the [README](../README.md) already flags
as "rate-limited and shared" and "not fine for real work." `horizon-nest` is the one exception, on
paid Alchemy - 1.1M requests to serve a single HTTP request over the audit window, with no invoice
figure quoted for it in #750.

Pricing the steady-state load against a metered endpoint (e.g. Alchemy), for a cursor polling every ~2 s,
the default before 4.15.2. Since 4.15.2 a cursor polls every 5 minutes unless told otherwise, and the
polling terms below shrink by a factor of about 150; the next section has the measurement.

- **Block headers** (`eth_getBlockByNumber`): 20 CU. On Arbitrum (~4 blocks/s), 345,600 blocks/day = ~6.91M CU/day.
- **Tip polling** (`eth_blockNumber`): 10 CU. Polling every ~2 s = 43,200 calls/day = ~0.43M CU/day.
- **Log polling** (`eth_getLogs`): 60 CU. Polling every ~2 s = 43,200 calls/day = ~2.59M CU/day.

```
Header-only baseline:
cost/month ≈ blocks/day × CU(eth_getBlockByNumber) × days/month × $/CU
           ≈ 345,600     × 20                       × 30         × $0.00000045
           ≈ $93/month

Total tip-following composition (headers + polling):
CU/day     ≈ 6.91M (headers) + 0.43M (tip) + 2.59M (logs) ≈ 9.93M CU/day
cost/month ≈ 9.93M CU/day × 30 days × $0.00000045/CU ≈ ~$134/month
```

`eth_getBlockByNumber` at 20 CU, `eth_getLogs` at 60 CU, `eth_blockNumber` at 10 CU, and $0.45 per million CU for the first 300M CU/month, are Alchemy's
own published pay-as-you-go rates, checked 2026-08-22. Sources:
[compute unit costs](https://www.alchemy.com/docs/reference/compute-unit-costs),
[pricing](https://www.alchemy.com/pricing).

`nuthatch_rpc_methods_total` exposes method-labelled counters (e.g. `nuthatch_rpc_methods_total{method="eth_getBlockByNumber"}`), allowing operators to multiply each method by their provider's compute-unit schedule to compute exact invoices. `nuthatch_rpc_requests_total` tracks total outbound HTTP requests / batch envelopes.

A nest sitting at tip on Arbitrum costs on the order of **~$134/month** against a paid provider (with ~$93 of that being the header fetches). That figure is this computation, not a measurement - the reference deployment itself paid nothing for it, because it runs against a free endpoint.

**Measured against a paid endpoint, 2026-09-06** ([#1173](https://github.com/nuthatch-org/nuthatch/issues/1173)):
the allocations nest on the Lodestar box, at tip on Alchemy, over 45 seconds - 268 `eth_getBlockByNumber`,
61 `eth_getLogs` and 84 `eth_blockNumber` a minute, **~9,900 CU a minute, ~430M a month, roughly $185**.
The computation above had the shape right and the attribution wrong. Headers are bought only for blocks
that produced a kept row (#765), and of that day's 345,600 Arbitrum blocks **95** did. The header per
block is the **poll loop's own**: a reorg check on every two-second poll, then a checkpoint hash and a
`finalized` probe on every committed window. The rows were a rounding error; the cadence was the bill.

### Turning the dial: `--poll-interval` and `--finality-only`

That is why the dial is on the cadence ([RFC-0040](rfcs/0040-the-freshness-dial.md) §3, knobs 1 and 2).
Both are `nuthatch dev` flags, never `nuthatch.toml` fields: how often you ask is not what the nest is,
and two nests differing only in cadence hold identical rows under one content address.

```sh
# The default since 4.15.2: same rows, up to five minutes later, for roughly a hundredth of the
# requests of a two-second cursor.
nuthatch dev --dir .

# Follow the tip closely, and pay for every poll.
nuthatch dev --dir . --poll-interval 2s

# Never index past the chain's finality boundary: nothing you hold can reorg.
nuthatch dev --dir . --poll-interval 5m --finality-only
```

- **`--poll-interval <DURATION>`** (`5m`, `1h`, or bare seconds) is how long a caught-up cursor waits
  before asking for the tip again. Unset, it is **5 minutes** (since 4.15.2; before that, the chain's
  block time, never under 2 s), so a new row can take up to five minutes to appear. Pass a shorter
  interval for a nest whose readers need the tip closely. Polling faster than blocks arrive finds
  nothing new and is still billed. Every poll costs a tip call and, when a window commits,
  a reorg check, a checkpoint and a `finalized` probe, whether or not a block carried an event. At five
  minutes the allocations nest above drops from ~9,900 CU a minute to the order of **100** - a few
  dollars a month - and holds exactly the same rows. Match it to your consumers: a dashboard on a
  five-minute cron cannot see the difference between a two-second cursor and a five-minute one.
- **`--finality-only`** caps the cursor at the chain's finality boundary instead of the tip. Nothing it
  indexes can be reorged, so the reorg check stops once the hot store holds no unfinalised row, and
  the hot store only ever carries rows waiting to seal. On Arbitrum the `finalized` tag runs roughly
  fifteen to twenty minutes behind the tip; that lag is the price, and it is deliberate.

**Nothing about this is silent.** `/ready` carries a `freshness` object - `{"mode": "tip" | "finality",
"poll_interval_secs": N, "poll_interval_source": "block_time" | "flag"}` - and `lag_blocks` is then
the distance you chose. `block_time` means the interval was defaulted rather than flagged. Its stall thresholds scale with the interval (at least three intervals), so a quiet
five-minute cursor is not reported as a dead one, and a dead pool is still reported inside a
quarter of an hour.

**What does not change.** The rows, the decode, the sealing, and a segment's content address, which is
a function of its block range and rows alone. A slower cursor returns the same data later; it never
returns a substitute. Backfill is unaffected - the dial only applies once the cursor has caught up.

---

## Configuration surface

**Files:** `mounts.toml` (chains, mounted nests, budget), `nuthatch.toml` per nest (contracts, events,
factories, flags, webhooks, alerts), `semantic.toml` per nest (descriptions for the AI and
SQL surfaces). Full key reference:
[`config-reference.md`](../skills/nuthatch-builder/config-reference.md).

**Environment:**

| Variable | Purpose |
|---|---|
| `NUTHATCH_ADMIN_TOKEN` | required for the admin UI when bound off-localhost; presented as `?token=` or `Authorization: Bearer` |
| `NUTHATCH_SQL_MEMO_BYTES` | ceiling on the analytical memo's remembered rows, in serialized bytes (default 64 MiB; `0` turns it off). See *The analytical memo* under capacity and sizing. |
| `NUTHATCH_CALL_BODY_CONCURRENCY` | block-body batches (20 blocks each) fetched at once for `top_level_calls` (default 4, ceiling 10). Memory stays one 200-block chunk of bodies whatever the value. On `--seal-direct` it multiplies with `--concurrency`, since each window fetches its own bodies. |

**Runtime flags that matter operationally** (`dev`; `bench backfill` shares `--rpc`, `--seal-direct`
and `--concurrency`):

| Flag | Use |
|---|---|
| `--listen` | bind address. Defaults to `127.0.0.1:8288` |
| `--rpc` | override configured endpoints without editing config. Repeatable. Single-chain runtimes only (ambiguous once a runtime spans chains) |
| `--rpc-fallback` | endpoint(s) asked only while every other endpoint is failing. Repeatable. Put a paid key here behind free endpoints and it bills only for what they could not answer. Otherwise endpoints share the load round-robin, so a paid key in `--rpc` takes its share of every call. Single-chain runtimes only |
| `--seal-direct` | backfill finalised history straight to Parquet, bypassing the hot store. Prerequisite for `--concurrency`; the storage path alone is not a speedup. Current figures: [benchmarks.md](benchmarks.md) |
| `--concurrency` | concurrent window fetches during seal-direct backfill. 8-16 against your own node; low on rate-limited public RPC |
| `--window` | the widest `eth_getLogs` block range nuthatch asks for, and where its adaptive window starts. Without it the window starts at the chain default and grows over empty ranges (to 100,000 blocks for a log-only nest); with it no request is wider than this, though it still shrinks when a provider refuses a range. Set it to your provider's range cap, or high (50k) for a *sparse* contract |
| `--backfill N` | index only the last N blocks (recent-history mode) |
| `--no-admin` | remove the admin UI routes entirely. Use it when you front your own dashboard |
| `--fail-fast` | exit on first fault instead of quarantining. For CI and operators who prefer fail-stop |

**`block_timestamps` - decide it at `init`, because you cannot change it later.**

Every table carries an implicit `block_timestamp`, and fetching it is the single most expensive thing
a backfill does: timestamps live in the block header, which `eth_getLogs` does not return, so they
cost a separate `eth_getBlockByNumber` per distinct block. On the workloads we measured that is
roughly **85% of backfill wall clock** (RFC-0029 §4). `nuthatch init --no-timestamps` drops the column
and stops paying for it. Since 3.11 that cost is gone on any node that returns `blockTimestamp` on
its logs, which every endpoint we checked does (see *What a nest costs at tip*), so check yours before
giving up the column. One public endpoint, `arb1.arbitrum.io`, returns the field as `0x0` on every
log; since 4.10.1 a window holding a block whose timestamp is missing or zero is refused and retried,
against another endpoint where the pool has one, rather than stored (#1911). Such a node costs
retries, never wrong rows.

Read the next paragraph before you reach for it.

This is **not a tuning flag**. Turning it off removes a column from every table, which is a breaking
change for anyone querying it, and it changes the bytes of every sealed segment - segments are
content-addressed, so a nest that switches **cannot reuse its own history and must re-index from
scratch**. For an existing nest, "faster backfill" and "re-index everything once" largely cancel out.
The win is real, but it is a **new-nest** win.

Because of that, the runtime enforces it rather than trusting the file. A nest that has already
indexed refuses to start if the declaration disagrees with its stored data, and a timestamp-free nest
is stamped `schema_version = 2` so an older nuthatch refuses it outright instead of quietly indexing
timestamps into a store built without them. Changing your mind means a *new* nest, served alongside
the old one until its consumers move - the ordinary breaking-update path (RFC-0020 slice 3).

A nest that indexes timestamps stays `schema_version = 1` and is readable by 0.8.x, so nothing about
this affects you unless you opt in.

**When it is clearly right:** you know every consumer, none of them ask a time-series question, and
you are standing up a new nest over a long history. **When it is clearly wrong:** anyone might want
"per day" or "per hour" later. Blocks give you ordering; only timestamps give you time. If you are
unsure, keep them - the default is on for a reason.

**Secrets.** Private RPC URLs and webhook HMAC secrets live in the nest's `nuthatch.toml`. The rule
(RFC-0019 §4) is that secrets never go into a published bundle - so keep the directory `0700` and
owned by the service user, and never publish a bundle built from a config carrying a credential.

In **scaled** mode, per-nest secret injection at mount time is built (RFC-0022 §5): secrets live in the
control plane and a worker receives only those of the nests it is actually assigned. The interface is
write-only - you can list which keys exist and never read a value back - and rotating one changes no
bundle hash, so it neither invalidates segment reuse nor forces a re-index.

---

## Running an unlisted EVM chain

Ethereum mainnet, Arbitrum One, Base, BSC, Polygon, Gnosis, Optimism, Monad and Robinhood Chain are **built in**:
keyless public endpoints, a tuned
`eth_getLogs` window, chain-appropriate finality, and bytecode probing so `init` can detect which of
them a contract lives on.

**Any other EVM chain also works** - it just has to be configured by hand. Teams have run nuthatch on
World Chain (`480`) and Base Sepolia (`84532`) this way. The split is worth knowing exactly:

| Command | Unlisted chain? |
|---|---|
| `dev`, `sql`, `bench` | **yes** - chain-agnostic, falls back to defaults |
| `init`, `add` | **yes, with `--rpc`** - `--chain <any-name> --rpc <url>` reads the chain id from the endpoint; ABI resolution is still Sourcify/Etherscan-gated, so pass `--abi` for an unverified contract |

So the working recipe is `nuthatch init 0x… --chain <name> --rpc <url> --abi ./abi.json`, or write
`nuthatch.toml` yourself, vendor the ABI, and run.

```toml
[nest]
name = "my-nest"
chain = "world-chain"        # any label you like - it is not looked up for an unlisted chain
chain_id = 480               # MUST match what your endpoints report; verified at startup
rpc_urls = ["https://your-endpoint.example"]
schema_version = 1

[[contracts]]
alias = "router"
address = "0x…"
start_block = 1234567        # no bytecode probing here, so supply it yourself
abi = "abis/router.json"     # vendor the ABI by hand; Sourcify/Etherscan lookup is chain-gated
events = ["Swapped"]         # optional allowlist
```

Then `nuthatch dev --dir .` as usual. Everything downstream - decode, sealing, `/sql`, views, MCP,
runtimes, bundles - is chain-agnostic and behaves exactly as it does on a built-in chain.

**Two caveats that are easy to miss:**

1. **You inherit default finality and window.** An unlisted chain gets `Depth(64)` finality and a
   **20-block** `eth_getLogs` window, because nuthatch has no per-chain policy for it. Depth-64 is the
   Ethereum-L1-shaped assumption; if your chain finalises differently - a fast L2, or one with deeper
   reorgs - that default is a guess, and the conservative direction is *deeper*. The 20-block window is
   deliberately small: it grows over empty ranges, but a long backfill starts slowly. Start it wider
   with `--window` (a sparse contract can often take 50000), which is also the widest range nuthatch
   will then ask for, so keep it within your provider's range cap.
2. **`chain_id` is enforced.** Every endpoint in `rpc_urls` is checked against it at startup and a
   mismatch is refused, because indexing against the wrong chain corrupts state silently. Get the id
   right and the pool stays honest; get it wrong and nuthatch tells you immediately rather than three
   days into a backfill.

If you are running an unlisted chain in anger, say so - a chain with real usage is a candidate for the
built-in registry, which is where the tuned window and finality policy come from.

### Monad: a depth of eight, and why it is not the tag

Monad (chain id `143`) ships with `Depth(8)`: the seal boundary is eight blocks behind tip, about
2.4 s. The `finalized` tag sits one block behind tip and is irreversible without a hard fork, so
this is not a reorg margin - it is an **execution** margin. Monad executes a block up to three blocks
after consensus finalises it, and a probe that reads logs for a block finalised but not yet executed
would get an empty list, indistinguishable from an empty block. Every probe on 2026-09-03 found
receipts complete at `latest`, but eight samples are not an invariant, so eight blocks buys the margin
instead. Do not raise the depth by analogy with an L2 - anything past `finalized` is final - and do not lower
it to the tag by analogy either. The soak RFC-0051's addendum asked for ran on 2026-09-04: 24 hours,
three endpoints, 648,532 reads of `finalized` and its receipts, zero short answers, and the boundary
still stays at eight, because the tail refetch covers two blocks of a three-block execution deferral
and a day of clean reads is evidence about three endpoints rather than a runtime guarantee (RFC-0051
addendum item 16).

Three things to know before a Monad backfill, all measured on 2026-09-03:

- **The shipped window is 100 blocks**, the documented cap on `rpc.monad.xyz`. Monad blocks are
  dense - the busiest contract that day carried 77 logs per block - so on a busy address it is the
  result cap you hit, and `nuthatch doctor --address <it>` recommends 40 across the pool. Alchemy's
  `rpc1.monad.xyz` serves 640 address-filtered on its own.
- **No shipped endpoint keeps historic state.** All three serve logs and blocks from block 1, so a
  from-genesis backfill of events works. A pinned `[[calls]]` at an old block (RFC-0023) fails with
  `Block requested not found` and needs an archive endpoint: `init ... --chain monad --rpc <url>` or
  `dev --rpc <url>`, either of which makes your endpoint the whole pool (the built-in name is still
  used for the seal depth and window; only the chain id is never looked up over `--rpc`).
  `rpc-mainnet.monadinfra.com` keeps state but refuses JSON-RPC batches, so it is not listed.
- **`init` cannot detect a deployment block.** That probe is `eth_getCode` at old heights, which is a
  state read, so on the shipped endpoints it reports `deployment block undetected` and the backfill
  starts from a tip offset. Set `start_block` in `nuthatch.toml`, or pass `--backfill <blocks>`, for
  the history you actually want; at 300 ms a block a day is 288,000 blocks.
- **Alert on tip lag in seconds, not blocks.** At 300 ms a block, twenty blocks behind is six seconds.

### Robinhood Chain: Arbitrum's policy, one endpoint, and a rate limit per second

Robinhood Chain (chain id `4663`) is an Arbitrum Nitro L2 and ships with Arbitrum One's policy:
`FinalizedTag`, sealing at the node's `finalized` tag, which ran about twenty minutes (12,000
blocks) behind tip on 2026-09-04. The `safe` tag is served too and runs about seven minutes closer;
nuthatch has no `safe` policy, so it does not use it, and the difference is latency rather than
safety. The fallback depth, which only runs on an endpoint that stops serving the tag, is 15,000
blocks: this chain does ten blocks a second, so a depth copied from a slower chain would sit inside
the unfinalised window.

Three things to know, measured on 2026-09-04:

- **One shipped endpoint, `rpc.mainnet.chain.robinhood.com`, and it throttles per second.** It
  serves a 640-block address-filtered window on the busiest token and a batch of 5, and it answers
  the archive probe with HTTP 429. Hit it hard and every call is 429 for a while: a `doctor` run
  straight after a sampling pass read a window of 40. The shipped window is 320. For a backfill in
  anger bring `--rpc` at a keyed provider; the two other keyless hosts that answer the chain id
  cannot back a nest (one lacks `eth_blockNumber`, the other wants an archive token).
- **Timestamps are what the throttle bites on.** The endpoint's batch limit is 5, so a 320-block
  window's `block_timestamps` fetch is 64 batch calls, and the per-second limit answers most of
  them 429: a keyless `dev` with timestamps on spent two minutes retrying its first window and
  indexed nothing. The same nest scaffolded with `init --no-timestamps` backfilled 2,000 blocks
  (23,953 USDG transfers) in 16 s with not one 429. So the zero-setup demo on this chain is
  `--no-timestamps`; timestamps want a keyed endpoint, whose batch limit is not 5 - the same
  nest with timestamps on, `dev --rpc <Alchemy>`, did 2,000 blocks in 14 s at 2,286 events/s. One
  thing to know about those timestamps: at ten blocks a second, about ten blocks share each
  one-second `block_timestamp`, so it orders blocks only to the second.
- **The tag policy was exercised, not assumed.** The keyless nest above, left following, sealed
  its first two cuts at the `finalized` tag inside 25 minutes (40,014 rows, `nuthatch_reorgs_total`
  0), the delay being the tag's own lag past the nest's start block plus the 20,000-row batch.
- **Keyless `init` works where Sourcify does.** USDG and WETH, the two busiest contracts, are
  verified through their proxies. The Stock Tokens (`MSTR`, `AMC`, `SPY`, ...) are 283-byte proxies
  that are not, and the explorer's Blockscout API sits behind a Cloudflare challenge, so it is not
  wired as a keyless source. A Stock Token is an ERC-20: `--abi` with the standard ERC-20 ABI
  indexes its transfers, and that is the ABI nobody needs to fetch.
- **The chain is busy.** About 148 logs a block from three thousand emitters, and a window of 320
  blocks on a busy token sits under the endpoint's 10,000-log result cap with room; on a busier one
  the chunker narrows from the refusal, which the endpoint states in words.

### Arbitrum's L1 block: `[extract] l1_blocks`

Every Arbitrum Nitro header (Arbitrum One, Robinhood Chain) carries `l1BlockNumber`, the Ethereum
block the L2 block was sequenced against. A contract that counts in L1 blocks, the Graph's
`EpochManager` being the case that asked for this (#1839), needs it to place an event, and the log
does not carry it.

```toml
[extract]
l1_blocks = true
```

This adds one table, `l1_blocks`: a row per block that carries a row of this nest, with
`block_number`, `block_hash`, `block_timestamp` (when the nest has timestamps) and `l1_block_number`.
A view joins it on `block_number` and `block_hash`:

```sql
SELECT e.*, l.l1_block_number
FROM staking__allocation_created e
JOIN l1_blocks l USING (block_number, block_hash);
```

It costs one header per block that carries a row, the header the timestamp comes from when the logs
do not carry one, and nothing for the blocks in between. `[extract] blocks` keeps the same column on
every block, at a header for every block in the window, and the two are refused together: if a nest
already has `blocks`, read `blocks.l1_block_number`. Off by default, and off changes nothing: no
table, no header, the same segments. It is refused on a registered chain that does not report the
field, and on an unlisted chain a header without it refuses the window rather than storing 0. Like
`blocks`, turning it on for a nest that has already indexed is refused at startup, because the
history it would cover has no rows; index a new nest instead.

---

## The service surface

Per-nest routes. In a runtime they are prefixed: `/<name>/sql`, `/<name>/tables`, and so on.

| Route | Purpose |
|---|---|
| `GET /` | summary: nest identity, heights, table count |
| `GET /health` | liveness. `200 "ok"` while the process serves |
| `GET /ready` | readiness. Per-nest: `503` if quarantined, the source stops answering, the cursor stops advancing (`wedged`), or the seal stops advancing (`tip_seal_stalled`, 3.6.1) |
| `GET /metrics` | Prometheus text exposition |
| `GET /tables`, `GET /table/{name}` | schema and recent rows, merged hot and cold; `"degraded": true` and the table in `degraded_tables`, as on `/sql`, when the sealed rows could not be read |
| `GET /schema` | the full data model |
| `GET /sql?q=…` | read-only analytical SQL over hot and sealed data; the answer carries `cached`, `degraded`, `tip_unavailable` and a `provenance` block (`as_of`, `sealed_through`, `nid`, `registry_hash`) |
| `GET /explain` | binds caller-supplied SQL without running it (`valid`, `note`); guarded like `/sql` |
| `GET /queries`, `GET /q/{name}` | the mount's `sql` mode (`open`, `allowlist`, `deny`) and its declared queries (RFC-0034), and one of them by name |
| `GET /derived`, `GET /derived/{entity}`, `GET /derived/{entity}/{key}` | authored incremental entities (RFC-0041): the list with its head, one relation, one row |
| `GET /ipfs/gave-up?limit=100` | the `[[ipfs]]` documents the nest stopped trying to fetch, in block order |
| `GET /entities`, `GET /entity/{id}` | entity point-reads, transparently across the hot/cold seam |
| `GET /balances`, `GET /balance/{address}` | the derived IVM balance view |
| `GET /exposure/{address}`, `GET /flags` | compliance surfaces (RFC-0008) |
| `GET /nest` | nest identity and registry hash |
| `GET /shape` | capability probe: what this nest can answer (drives adaptive MCP) |
| `GET /_admin/`, `/_admin/events`, `/_admin/config`, `/_admin/storage` | admin UI (eight tabs since 4.2.1), its event stream, the authored files and limits as JSON with every URL cut to scheme and host and every secret withheld, and the sealed catalogue per table (`?table=` for one table's segments). Token-gated off-localhost; removable with `--no-admin` |

**Runtime root routes:** `GET /nests` (roster with live per-nest health), `GET /ready` (runtime-wide),
`GET /health`, and `GET /metrics` (the whole runtime's exposition, served before anything is mounted;
each mount's `/metrics` serves the same).

**Runtime admin routes** (3.13.0), token-gated off-localhost like the admin UI and removed by
`--no-admin`. The full request and response shapes are in [admin-api.md](admin-api.md).

| Route | Purpose |
|---|---|
| `POST /_admin/nests` | mount `{name, nid}` as a job (`202`); `?wait=true` answers when done, `?dry_run=true` prices it |
| `GET /_admin/mounts`, `GET /_admin/mounts/{name}` | every mount job, or one: `accepted`, `fetching`, `joining`, `live`, `failed` |
| `POST /_admin/suspend/{name}`, `POST /_admin/resume/{name}` | pause a mount behind a `503`, and catch it up again |
| `POST /_admin/move/{name}` | move a name to another `nid` without a gap |
| `DELETE /_admin/nests/{name}` | unmount; `?reclaim=true` also frees the dataset once nothing mounts it |
| `DELETE /_admin/datasets/{nid}` | reclaim a dataset unmounted earlier |

---

### Control-plane endpoints (scaled mode only)

| Route | Purpose |
|---|---|
| `GET /nests` · `POST /nests` · `DELETE /nests/{name}` | declare/inspect/remove desired state |
| `GET /nests/{name}/resolve` · `PUT /nests/{name}/pin` | what an endpoint serves, and pinning it |
| `GET /nests/{name}/secrets` · `PUT` · `DELETE .../{key}` | key **names** only on read; write-only values |
| `GET /workers` | live workers and their budgets |
| `GET /plan` | placement, including what cannot be placed and why |
| `GET /health` | unauthenticated, for load balancers |

Bound off-localhost the control plane **refuses to start** without `NUTHATCH_CONTROL_TOKEN`. This is a
refusal rather than a warning because the endpoint decides what an entire fleet runs.

## Security posture

**Bind localhost and front it.** The API defaults to `127.0.0.1:8288`. `/sql` is guarded but **not
authenticated** - the guards below bound *how much*, never *who*. Off-localhost binds log a loud
warning at startup. Put TLS and authentication in front, always.

**A public nest without an allowlist is an open query engine.** Said plainly because it is the single
decision that matters here: `sql = "open"` is the default and it is the right default for a local
`nuthatch dev`, where exploration is the point - but on an endpoint strangers can reach, it means
anyone may run arbitrary analytical SQL over your disk, bounded only by the guards below. If that is
not what you want, set `sql = "allowlist"` on the mount and declare the queries it answers, or
`sql = "deny"` to close SQL entirely while the typed routes keep serving. See
[Bounding what a mount will answer](https://nuthatch-indexer.com/docs/operate/security/#bounding-what-a-mount-will-answer).

**Query guards** - node self-protection against a single runaway query or a burst, not per-caller
quotas (that needs identity a single-tenant node does not have). They bound *how much* one query
costs; they say nothing about *which* queries a nest is willing to answer, which is the allowlist's
job:

| Guard | Default | What it bounds |
|---|---|---|
| statement timeout | 30 s | a runaway (e.g. cartesian) query is interrupted mid-flight |
| max result rows | 50,000 | the Rust-side result buffer, outside the engine's own memory limit |
| max concurrent queries | 2 | the real DoS multiplier: a semaphore. A request over the limit waits up to **250 ms** for a permit and only then returns `503`, so a short burst smooths instead of bouncing (#1319); the wait is charged against the statement timeout, so queuing cannot extend a request's total deadline. The permit count still decides how many queries run at once. `NUTHATCH_SQL_MAX_CONCURRENCY` still overrides it, ceiling 16, and is not an unconstrained config key |
| max queued queries | 256 per nest | the bound that makes the wait above safe: it caps how *many* requests may be parked waiting, where the 250 ms caps how *long* each one waits. Without it a burst parks arrival-rate × 250 ms requests before any time out. Past the cap a request is refused immediately, as every over-limit request was before the wait existed. A parked request holds only its query string (≤ 16 KiB), so the worst case is roughly 4 MB per nest |
| engine memory / threads | 512 MB / 2 (threads ceiling 16) | `analytics.memory_limit` / `analytics.threads`. Memory times the pools (the smaller of the permit count and the cursor's datasets) is refused at startup if it plus `ingestion_reservation` exceeds 2 GiB. Threads share the permit ceiling of 16 and are refused above it |
| max query length | 16 KiB | rejects absurd query strings before the planner |
| max unsealed rows scanned | 2,000,000 | the tip is materialised per query; past this the query is refused with `503` rather than served partially |
| declared-query scan bound | 512 MiB | named queries (`/q/{name}`) only: source bytes the plan may read, each Parquet scan charged the widest table it can reach, plus the hot and maintained rows copied for it. Checked on the connection that runs the statement, before it is evaluated. Over the cap, or a plan that can rescan (nested-loop or delim join, recursive CTE), answers `422` |

`/sql` is **read-only and single-statement**: a query must open with `SELECT` or `WITH`, filesystem
and network table functions are refused, and `;`-stacking a second statement is rejected outright.
Rejections surface as a `4xx` or `503` and count in the backward-compatible aggregate
`nuthatch_sql_rejections_total`, and in `nuthatch_sql_rejections_total{reason=…}`. The fixed
`reason` values distinguish `busy`, `too_large`, `invalid`, `bounded`, declared-query
`admission`, `timeout`, and `out_of_memory` (the engine ran out of its memory budget, still a `400`
to the caller); they are the useful series for separating saturation from a caller error. A declared
query refused by its scan bound answers `422` and also counts in
`nuthatch_named_scan_refusals_total`; the bounds of those admitted are the
`nuthatch_named_scan_bytes` histogram.

**`/explain` is guarded identically.** It plans caller-supplied SQL without returning rows, but
planning still materialises the tip, so it carries the same scan cost as the query it is describing
- and therefore the same ceilings, including the unsealed-row one. An over-budget tip answers `503`
with the reason rather than reporting on sealed data alone, which would bind against a narrower
schema than a following `/sql` would see. Anywhere you bound `/sql`, bound `/explain` with it: a
guard on one and not the other leaves the cheaper-looking route as the expensive one.

**Admin surface.** Off-localhost the admin UI requires `NUTHATCH_ADMIN_TOKEN` on every request; token
comparison is constant-time. `--no-admin` removes the routes entirely rather than merely gating them.

**`/metrics` and `/health` are unauthenticated by design.** Scope them to your internal network at the
gateway if you do not want them public.

**Browser front ends: `--cors` (#1318).** By default a nest sends no `Access-Control-Allow-Origin`,
so a page served from another origin cannot call it at all, and the only answer was a reverse proxy
or routing through your own server. Both are correct and both are a wall for anyone who just wants a
page to read a nest. `dev` and `serve` take `--cors <ORIGIN>`, repeatable:

```bash
nuthatch dev --cors https://app.example.com --cors http://localhost:3000
nuthatch serve --cors '*'          # any origin; fine for a public read-only nest
```

It sets allow-origin, allow-methods `GET,POST,OPTIONS` and allow-headers, and answers preflight.
`POST` is there for the GraphQL routes (`/graphql`, `/subgraphs/id/{id}`, `/subgraphs/name/…`,
present only in a `--features graph` build), which take their query in a body - a `GET`-only list would leave a browser GraphQL client
preflighting `POST`, being refused, and never sending the query. No `/_admin` path is ever
given the headers, so the flag cannot open a runtime's lifecycle routes to another origin (#1642). Values
must be exact origins **with a scheme and no trailing slash** - `app.example.com` and
`https://app.example.com/` are both refused at startup, because an `Origin` header never looks like
either and the browser-side failure is a generic CORS error that tells an operator nothing. A single
`*` may not be combined with named origins.

**What it does not do.** It grants reach, not resources: every guard in the table above still
applies, and the flag adds no authentication. A nest on a public bind with `--cors '*'` is a public
read-only API, which may be exactly what you want - decide that deliberately. Omit the flag and
nothing about the nest changes.

**Run it unprivileged.** A dedicated service user, `0700` on the nest directory, `MemoryMax` set to
the cursor budget. The binary needs outbound network to your RPC endpoints and webhook sinks, nothing
else.

**Supply chain.** Nest bundles are content-addressed; `nest load` verifies the manifest format, every
file's hash, and that the decode registry regenerated from the inputs matches the manifest. A nest
that does not reproduce its own decode registry is refused. Compliance packs are ed25519-signed
(`nuthatch pack keygen|build|verify`). Licensed `MIT OR Apache-2.0`; `cargo-deny` runs in CI.

**Per-caller rate limiting is the gateway's job, not nuthatch's.** The node cannot rate-limit by caller
because nothing it serves carries a caller identity: the data routes (`/entities`, `/sql`, `/explain`
and the rest) have no accounts and no API keys. `NUTHATCH_ADMIN_TOKEN` above is not a counter-example
- it is one shared operator credential gating `/_admin`, not a per-caller identity you could meter,
and every caller who has it is the same caller as far as the node can tell. The query guards above
bound the cost of a single request and the total concurrent analytical load; they say nothing about
how many requests a given caller may make. The concurrency semaphore releases each permit when its
query finishes. An in-process request-per-second counter with no identity would be worse than
nothing: one caller can exhaust the whole window and block every other caller until it resets,
converting an accidental poller into a service-wide outage.

For operators who need per-caller rate limiting, the right place is the reverse proxy in front:

```caddyfile
# Caddy with mholt/caddy-ratelimit (compile with xcaddy; not in the stock binary)
your-domain.example {
    reverse_proxy localhost:8288

    rate_limit {
        zone api {
            key     {remote_host}
            events  60
            window  1m
        }
    }
}
```

```nginx
# nginx (ngx_http_limit_req_module, standard in most packages)
http {
    limit_req_zone $binary_remote_addr zone=api:10m rate=60r/m;

    server {
        location / {
            limit_req zone=api burst=20 nodelay;
            proxy_pass http://127.0.0.1:8288;
        }
    }
}
```

Neither is a complete security configuration - TLS, authentication, and an allowlist belong in front
too. The rate limit is one layer of a stack, not the stack. See [The division of labour](#the-division-of-labour).

### Enabling remote admin

The recipes above ship with remote admin off. Not caution for its own sake: the admin surface mounts
and unmounts nests, so a reachable one behind a guessable token is full control of what the runtime
serves.

If you need it, generate the token. Never copy a literal out of documentation, including this page:

```sh
openssl rand -hex 32
```

Set it as `NUTHATCH_ADMIN_TOKEN`, keep the published port on `127.0.0.1`, reach it through a reverse
proxy with TLS, and treat the value as a credential: not in shell history, not in a committed compose
file, and not in a world-readable unit file (`chmod 600`, or `EnvironmentFile=` pointing at a
root-owned file).

Two things worth knowing first:

- **Setting a token also removes a refusal.** `nuthatch` declines to serve the admin routes
  off-localhost *unless* a token is set. So setting one is not purely "adding auth", it also lifts
  the guard that was protecting you. Correct behaviour, but it means the token is the only thing
  between the network and `POST /_admin/nests`.
- **Anything on the same Docker network reaches it** regardless of `-p 127.0.0.1:...`, because the
  publish flag governs the host, not the container network.

---

## Observability

### Metrics

Global series (whole process). Chain-height globals are present only for a solo runtime: there is no
honest way to aggregate mainnet and Arbitrum block numbers. In a multi-nest runtime use the labelled
per-nest series below.

| Series | Meaning |
|---|---|
| `nuthatch_tip_height`, `nuthatch_last_block`, `nuthatch_tip_lag_blocks` | is it keeping up |
| `nuthatch_sealed_through` | cold-layer watermark |
| `nuthatch_rows_decoded_total`, `nuthatch_rows_sealed_total`, `nuthatch_reorgs_total`, `nuthatch_checkpoints_missed_total` (windows committed with no reorg checkpoint; reorg detection is blind while it climbs), `nuthatch_ipfs_unreadable_total`, `nuthatch_ipfs_resolved_total`, `nuthatch_ipfs_given_up_total`, `nuthatch_ipfs_unverified_total`, `nuthatch_ipfs_oversize_total`, `nuthatch_ipfs_rows_refused_total`, `nuthatch_ipfs_retries_total` | ingestion |
| `nuthatch_http_requests_total`, `nuthatch_sql_queries_total`, `nuthatch_sql_rejections_total`, `nuthatch_sql_rejections_total{reason=…}` | serving; the unlabelled rejection total is the aggregate, and the fixed `reason` label classifies its refusals |
| `nuthatch_sql_memo_hits_total`, `nuthatch_sql_memo_misses_total`, `nuthatch_sql_memo_bytes` | the analytical memo (#1186): how many `/sql` answers were remembered rather than computed, and what it holds |
| `nuthatch_rpc_requests_total` | outbound HTTP POSTs (one per request or batch envelope, including failover retries) |
| `nuthatch_rpc_endpoint_requests_total{endpoint}`, `nuthatch_rpc_endpoint_failures_total{endpoint}`, `nuthatch_rpc_endpoint_retries_total{endpoint}`, `nuthatch_rpc_request_duration_seconds` | the same traffic per endpoint, host only, and its latency histogram; what the admin page's RPC tab reads |
| `nuthatch_rpc_methods_total{method=…}` | individual JSON-RPC method invocations; a batch of 200 `eth_getBlockByNumber` is 200 here and 1 on `nuthatch_rpc_requests_total`. Multiply by a provider's per-method CU schedule to estimate a bill |
| `nuthatch_start_time_seconds` | unix time the runtime built its first cursor; the counters are since then, so this is what turns `nuthatch_rpc_methods_total` into a monthly figure. Absent on a serve-only process |
| `nuthatch_rss_bytes` | process memory: the number to provision against |
| `nuthatch_analytics_pool_reserved_bytes`, `nuthatch_analytics_pool_peak_bytes`, `nuthatch_analytics_engines` | what `/sql` statements hold against the analytics memory pools (#1778). A statement that runs while the cached engine is busy opens another, and every engine opened over one nest's dataset draws on one pool bounded by `NUTHATCH_BURRMILL_MEMORY_LIMIT` less the eighth kept for the footer cache (#1792). Reserved is what those pools hold now, each counted once; peak is the most any one engine's statements have held since start; engines counts the cached and in-flight ones |
| `nuthatch_jemalloc_allocated_bytes`, `nuthatch_jemalloc_active_bytes`, `nuthatch_jemalloc_resident_bytes`, `nuthatch_jemalloc_retained_bytes` | the allocator's view, Linux glibc builds only, refreshed at each scrape. Allocated is what the program holds; resident less allocated is what jemalloc keeps in RAM for reuse; RSS less resident is memory jemalloc does not manage (C libraries calling glibc malloc, thread stacks, the binary, mapped files). Retained is mapped but already returned to the OS, so not in RSS. The binary sets jemalloc to `thp:never,narenas:8,dirty_decay_ms:1000,background_thread:true`: on a kernel with transparent huge pages set to `always`, pages jemalloc purged otherwise stayed resident inside their huge pages and showed up as RSS above resident, about 500 MiB on the allocations nest (#1773). `_RJEM_MALLOC_CONF` overrides it |
| `nuthatch_entity_state_bytes{nest,entity}` | bytes an authored entity's DBSP circuit holds in memory, from DBSP's own profile (#1834). It covers the circuit's traces, not the served copy of the relation, which `nuthatch_entity_rows` counts. The profile sizes every trace, so it is taken on the circuit's thread after the entity changes, at most once per `NUTHATCH_ENTITY_STATE_SAMPLE_SECS` (default 60, `0` turns it off), and an entity not yet sampled has no line. Measured on the ThinkPad against the allocations nest: 0.6 ms for 266 MB of state (309,549 groups), 3.3 ms for 1.37 GB (510,692 groups with a `MAX`) |
| `nuthatch_last_poll_unixtime` | liveness of the ingest loop itself |
| `nuthatch_fetch_window_blocks` | the block span of the cursor's latest `eth_getLogs` window. A backfill whose window sat at ~10 blocks for three hours after a rate-limited hour (#1170) shows here long before it shows in its ETA; the controller now widens again after four clean windows at the lowered ceiling |
| `nuthatch_seal_direct_fetched` vs `nuthatch_seal_direct_completed` | the seal-direct pass's fetch position against its durable watermark (#1169). A restart resumes from `completed`; the gap is the work it redoes, which on a sparse range can be tens of millions of blocks |
| `nuthatch_alert_outbox_depth` | webhook/alert delivery backlog |
| `nuthatch_audit_ranges_total`, `nuthatch_audit_mismatches_total`, `nuthatch_audit_errors_total` | the sealed-history audit, only with `--audit-rpc` (below): ranges compared, rows that differed, samples that could not finish |

Per-nest series, labelled `{nest="…"}` with the nest's route in a runtime (its alias, or `tenant/alias`)
and its name in a single-nest `dev` (#1415) - the ones that make co-tenancy operable:
`nuthatch_nest_hot_store_bytes` and `nuthatch_nest_sealed_segments_bytes` (the segments this nest's
manifest names; a segment two datasets share is counted under both, so for disk used read the
unlabelled `nuthatch_sealed_segments_bytes`, which counts each store once),
`nuthatch_nest_analytics_pool_reserved_bytes`, `nuthatch_nest_analytics_pool_peak_bytes` and
`nuthatch_nest_analytics_engines` (the global pool series for the engines opened over this nest's
dataset; two mounts of one dataset show the same engines),
`nuthatch_nest_tip_height`, `nuthatch_nest_last_block`, `nuthatch_nest_tip_lag_blocks`,
`nuthatch_nest_sealed_through`, `nuthatch_nest_rows_decoded_total`,
`nuthatch_nest_rows_sealed_total`, `nuthatch_nest_reorgs_total`, `nuthatch_nest_checkpoints_missed_total`,
`nuthatch_nest_ipfs_unreadable_total` (rows an `[[ipfs]]` declaration read that named no usable CID),
`nuthatch_nest_ipfs_pending` (documents named and neither stored nor given up on; sealing holds below
the lowest), `nuthatch_nest_ipfs_resolved_total`, `nuthatch_nest_ipfs_given_up_total`,
`nuthatch_nest_ipfs_unverified_total` (documents fetched that nothing proved against their CID, so no
row), `nuthatch_nest_ipfs_oversize_total` (documents refused past the byte, block or depth cap),
`nuthatch_nest_ipfs_rows_refused_total` (proven documents whose content did not fit their declared typed
rows, so neither the document nor any row was written),
`nuthatch_nest_ipfs_retries_total` (failed document fetches tried again, each also a warn line naming
the CID, block, attempt, error and the wait before the next; a window paused on a gateway shows here),
`nuthatch_nest_seal_direct_fetched`,
`nuthatch_nest_fetch_window_blocks`, `nuthatch_nest_health` (1 indexing / 0 quarantined; an unmounted or
suspended mount has no series),
`nuthatch_nest_quarantine_total`, and `nuthatch_cursor_live{chain}`.

A nest that mirrors itself (RFC-0052, `--publish-target`) also carries, per nest and only then:
`nuthatch_publish_sealed_through` (what the published catalogue covers),
`nuthatch_publish_lag_blocks` (blocks spanned by the final local segments the mirror does not hold, summed over tables; a quiet table's provisional tail is not owed, so a fully published nest reads 0), `nuthatch_publish_pending_segments`,
`nuthatch_publish_bytes_total`, `nuthatch_publish_errors_total` and `nuthatch_publish_dead_letter`.

### What to alert on

| Alert | Condition | Why |
|---|---|---|
| **Nest quarantined** | `nuthatch_nest_health == 0` | something is broken and a human should look |
| **Cursor dead** | `nuthatch_cursor_live == 0` | a whole chain stopped advancing |
| **Tip lag growing** | `nuthatch_tip_lag_blocks` trending up over 15m in solo mode, or `nuthatch_nest_tip_lag_blocks` per nest | RPC trouble or an overloaded box |
| **Ingest stalled** | `time() - nuthatch_last_poll_unixtime > 300` | the loop is wedged even if the process is alive |
| **Outbox backing up** | `nuthatch_alert_outbox_depth` rising | a webhook sink is down or slow |
| **Memory near budget** | `nuthatch_rss_bytes` over ~75% of the cursor ceiling | usually the `/sql` hot-scan |
| **Query rejections spiking** | `rate(nuthatch_sql_rejections_total{reason="busy"}[5m])` | the node is at its analytical concurrency or queue bound; inspect the aggregate too, but do not confuse caller mistakes with saturation |
| **Queries out of memory** | `increase(nuthatch_sql_rejections_total{reason="out_of_memory"}[2m]) > 0` | a query did not fit the engine's memory budget; a release that regressed memory shows here first, as 4.1.1 would have |
| **Quarantine flapping** | `increase(nuthatch_nest_quarantine_total[1h]) > 3` | a retryable fault that never settles |
| **Mirror behind** | `nuthatch_publish_lag_blocks` growing across several intervals, or `nuthatch_publish_dead_letter == 1` | the bucket is unreachable or refusing an object, and consumers of the mirror see stale history |
| **Sealed history disagrees** | `increase(nuthatch_audit_mismatches_total[1d]) > 0` | a second endpoint served different logs for a sealed range; one of the two providers is wrong (below) |
| **Audit not running** | `increase(nuthatch_audit_ranges_total[6h]) == 0` for 2h (the series exists only where `--audit-rpc` is set; six hours assumes the default 24 samples a day) | the audit endpoint is unreachable or on the wrong chain; `nuthatch_audit_errors_total` and the log say which |

### Auditing sealed history against a second endpoint

Nothing on the ingest path can see an `eth_getLogs` answer that leaves a log out of the middle of a
range (#1670): the range seals as if whole. No provider has been seen to do it, and the cheap per-block
checks cannot catch it without stalling or false alarms, so the detector is a sample. The audit asks
a **different** endpoint for a sealed range, decodes the answer with the nest's own registry, and
compares it row by row with the sealed segments for those blocks. A difference is reported and
nothing else: sealed segments are immutable, and which side is wrong is for you to decide.

```sh
# One-off: four sampled ranges, exit 1 on any difference.
nuthatch audit sealed --dir . --rpc https://another-provider.example/KEY --samples 4 --seed 7
# Re-check one reported range.
nuthatch audit sealed --dir . --rpc https://another-provider.example/KEY --from 21000000 --to 21000999
# In the background of a running nest: 24 ranges a day, reported on /metrics and in the log.
nuthatch dev --dir . --audit-rpc https://another-provider.example/KEY
```

- **Sampling.** Each sample is `--span` (`--audit-span`) blocks, 1,000 by default, between the
  first block a compared table covers (or the contracts' `start_block`) and the nest's
  `sealed_through`, the one boundary the indexer cuts every table at. A stretch past the last
  event segment was sealed empty, and is sampled like any other. `sanction_hit`, which the
  screening command removed on 2026-10-08 sealed over an operator-named range, does not move the
  boundary. The background audit
  reads `sealed_through` from the running nest; `nuthatch audit sealed` reads it from the hot
  store when the nest is stopped, and otherwise uses the furthest sealed segment, which can only
  fall short of it. Even-numbered samples land in a segment picked in proportion to its rows, where an omission
  has something to omit; odd-numbered samples are uniform over the sealed blocks, so a range the first
  endpoint called empty is still asked about. The same seed over the same segments picks the same
  ranges. `--seed` defaults to the current time and is printed; the background audit logs its seed
  and each range.
- **What is compared.** The rows of the nest's contract event tables, every column except
  `block_timestamp`, which comes from a header the audit does not fetch. Factory children's tables,
  `[extract] blocks` rows and `[[calls]]` rows are not compared.
- **Reading a mismatch.** The warning names the range, the endpoint (host only, never the key) and up
  to ten rows of each kind: *endpoint only* is what an omitting indexing endpoint leaves behind,
  *sealed only* is a row the audit endpoint did not serve, and *differing* is one log decoded two ways.
  Ask a third endpoint about the same range with `--from`/`--to` before deciding. To repair a range
  that really is short, re-index the nest from an endpoint that serves it; nothing patches a segment.
- **Which endpoint.** An endpoint in the nest's `rpc_urls` is refused, and for `--audit-rpc` so are
  `dev`'s `--rpc` and `--rpc-fallback`, since an endpoint can only agree with itself. One on the
  same host draws a warning, since one provider may serve both from one backend. The chain id is
  checked before the first comparison.
- **RPC cost.** One `eth_getLogs` per sample, plus the splits a provider forces by refusing the width,
  and one `eth_chainId` when the audit starts. No headers and no tip polls. At the default rate that
  is 24 `eth_getLogs` a day per nest. The background audit's calls are counted in
  `nuthatch_rpc_requests_total` with the nest's own.
- **Memory.** One sample holds at most 20,000 rows on each side, served and sealed, in whole blocks:
  a denser range is compared on both sides up to the last block that fit, and the report says so.
  A single block over the limit is kept and compared whole, as the seal path keeps it, and the
  report says that too. An endpoint that answers a dense range with nothing therefore costs the
  same as one that answers it in full. Sealed segments are read one at a time and filtered a
  record batch at a time, so a small sample inside a large segment holds its own rows and one batch,
  never the segment. For rows the size of an ERC-20 `Transfer` (about 400 bytes of row JSON) that is
  tens of megabytes while a sample runs, nothing between samples, and it does not grow with the nest.
- **Scope.** `--audit-rpc` runs on a single-nest `dev`; a runtime directory refuses it, and
  `nuthatch audit sealed` is run per nest instead. It is off unless set, and talks only to the
  endpoint you name.

### Health versus readiness

Get this right in your supervisor and your load balancer:

- **`/health`** is liveness. `200` while the process serves. Restart on failure.
- **`/ready`** is readiness. Runtime root: `200` only when **every** cursor and nest is indexing; `503`
  with a body naming what is quarantined (`quarantined`) **and which nests are stalled, with the terms
  that took each one unready** (`stalled`). Per-nest `/<name>/ready` answers only for that nest.
  Every readiness response also carries `version`, the release version compiled into the running
  binary, so a probe can identify the process it just judged.

  A nest reaches the `stalled` list on exactly the terms its own `/<name>/ready` uses - one function
  computes both, so the two surfaces cannot disagree. Before 3.7.0 the root consulted the quarantine
  set alone, and a runtime answered `{"quarantined":[],"ready":true}` while a nest inside it had not
  sealed in two days (#1204). **A 503 here is advice, not a gate:** every healthy nest carries on
  serving reads to whoever asks for it directly, so wiring a supervisor to this endpoint tells it to
  fetch a human rather than to restart the process. If yours restarts on a failed readiness check,
  point it at `/health` instead.

**A cursor at tip is not the same claim as a seal that is keeping up**, and since 3.6.1 `/ready`
answers both. `lag_blocks` describes *following*; `seal_lag_blocks` is how far the sealed watermark
trails what the cursor has indexed, and `tip_seal_stalled` is the verdict on it. Before 3.6.1 the only
seal fields on this endpoint described the bulk `--seal-direct` backfill, so a nest that had quietly
stopped sealing at tip answered `ready: true` with `lag_blocks: 0` indefinitely - measured at 739,192
blocks behind on a live nest (#1199). `seal_lag_blocks` is `null` for a `serve`-only role, and `null`
before anything has sealed at all, so read `sealed_through: 0` beside it as "nothing sealed yet"
rather than as a caught-up seal.

**A seal-direct pass reports the concurrency it runs at.** `seal_direct_concurrency` is
`{"requested", "effective", "capped_by"}`, and `null` for a nest not started with `--seal-direct`. With a
single RPC endpoint the pass runs one window at a time whatever `--concurrency` says, to avoid a stall
of the whole runtime, and `capped_by` then reads `single_rpc_endpoint` (#1399). Add a second endpoint
to get the concurrency you asked for. A nest mounted into a running runtime is held to the same cap,
counted on the endpoints of its chain's cursor, which is the source it fetches through.

**One IPFS document holds a seal-direct window for at most five minutes by default.** A window seals
only once every document it names is fetched or given up on. A failed fetch is tried again with a
timeout of 30, then 60, then 120 seconds, each retry a warn line and a count in
`nuthatch_nest_ipfs_retries_total`. Under `--seal-direct` a document not fetched within
`--ipfs-window-deadline` (default `300s`) is given up on, logged and counted as before. It is then
absent from the sealed segment, and nothing fetches it again yet (#1410 keeps that repair open). Every
document either path gives up on is recorded under the store's `ipfs_gave_up:` meta keys with its CID,
block, declaration, last error and time: `/ready` counts them as `ipfs_gave_up_documents`, and
`/ipfs/gave-up?limit=100` lists them in block order. That is the trade the flag sets:
raise it, or pass `0` to give up only once all ten attempts have failed, if you would rather a window
wait than seal without a document. `/ready` reports the setting as
`seal_direct_ipfs_window_deadline_secs`, `0` meaning none. A second `--ipfs` gateway is the remedy for a gateway
that stalls on some documents: on 2026-09-15 The Graph's gateway stopped two QoS documents after 256
to 320 KiB, and Pinata served them whole in 4.6 and 6.1 s.

**What to expect from it.** A healthy nest cuts a segment at least once per the chain's seal span,
about six hours of chain time, so `seal_lag_blocks` should oscillate rather than climb. It trending up
without bound over a day is the shape of the #1199 defect and worth a look even on an older binary,
where the field does not exist and the comparison has to be `last_block` against `sealed_through` by
hand.

Readiness is **advice to a supervisor, not a traffic gate**. A runtime with one quarantined nest reports
`503` at the root while its healthy nests keep serving correct data on their own prefixes. Wire root
readiness to a load balancer and one sick tenant evicts every healthy tenant from rotation. **Route on
per-nest `/<name>/ready`; page on the root.**

### Logs

Structured `tracing` to stdout. Quarantine and re-admission are `warn!` with nest, chain, fault class
and the full error chain. Off-localhost binds warn at startup. Backfill prints a progress line; tip
following is quiet.

---

## The failure model

**The unit that fails alone:**

| Fault | Blast radius |
|---|---|
| One nest's decode, store, seal, view or webhook error | that nest is quarantined; siblings on the same cursor keep indexing |
| One nest's finality violation (reorg below its sealed watermark) | that nest; terminal |
| A cursor's unrecoverable error | that chain's cursor and its nests; other chains' cursors keep running |
| RPC endpoint failure | none. Round-robin failover; the dead endpoint is cooled down |
| Transient RPC errors, tip fetch failures | none. Retried with escalating stall warnings |

**Retry policy.** Retryable faults back off exponentially from 5s, doubling, capped at 5 minutes,
unbounded attempts - so restarting a wedged RPC provider recovers within minutes without anyone
typing anything. Terminal faults do not retry and stay quarantined until restart, with the reason on
`/nests`.

**Re-admission is safe by construction.** A re-admitted nest rejoins behind its siblings, pulls the
cursor's window position back, and siblings skip windows they already committed. No nest re-processes
or skips a window. The cost is a re-fetch of the intervening range, which is why the backoff cap is
minutes rather than seconds.

**The process exits in exactly three cases:** the server stops (bind failure or shutdown signal);
every cursor has been quarantined (nothing will advance again, so dying honestly under a supervisor
beats serving frozen data); or `--fail-fast` is set and anything faults. Otherwise it stays up and
degrades.

**Reorgs** only ever touch the hot store, and only that of the affected chain's cursor. Sealed
segments are written strictly past finality and are immutable, so the columnar layer never rewinds. A
reorg deeper than the sealed watermark is a terminal fault by design: finality was violated, and
silently rewriting sealed history would be worse than stopping.

A reorg of either kind is found by a reorg check, not the instant the chain changes, and until then
the nest answers from what it last indexed, discarded blocks included. There is no fixed bound on that
window. A check runs when a poll sees the tip move; a fork that leaves the tip height unchanged is
re-checked at most every 12 seconds while the cursor is idle; a failing RPC delays it further; and under
`--finality-only` no check runs while the cursor sits inside its finality ceiling. Measured once against
a forked chain at a one-second poll interval, the rows of the abandoned branch were served for about
half a second. For an ordinary reorg this is the near-tip provisionality every indexer has, and sealed
history is untouched. For one below the seal the sealed rows are themselves on the abandoned branch, so
they are served until the check runs and then the nest halts; the only protection is a finality depth
the chain actually honours.

**Restart safety.** SIGTERM and SIGINT drain in-flight requests and exit **0**. Progress is
checkpointed and rows are keyed by `(block, log_index)`, so a restart resumes without gaps or
duplicates.

---

## Runbook

| Symptom | First checks | Action |
|---|---|---|
| Root `/ready` is 503 | `GET /nests` - which nest, what `quarantine.reason`, what `class` | retryable: watch for auto re-admission. terminal: fix the cause, restart |
| Tip lag climbing | `nuthatch_rpc_requests_total` rate, provider status | add or replace endpoints in `rpc_urls`; lower `--concurrency` if rate limited |
| A nest stuck at a block | its `quarantine.reason` on `/nests`; logs | a `getLogs` provider cap on a busy template usually wants a smaller `--window` |
| RSS approaching the ceiling | which nest, via `nuthatch_nest_*`; recent `/sql` traffic | the hot-scan is the usual cause. Restrict `/sql` at the gateway, or move the nest to its own cursor |
| Backfill crawling | `--window` versus contract sparsity; `--concurrency` | a sparse contract wants a *large* window; a dense one wants concurrency against your own node |
| Webhook backlog | `nuthatch_alert_outbox_depth`, sink availability | a drain posts eight deliveries at once with a 10 s timeout each, so one slow sink delays its own alerts, not the others'; past 10,000 undelivered alerts the oldest are shed, loudly |
| Chain-id mismatch at startup | the startup error names the endpoint | an endpoint in the pool is on the wrong network. Every endpoint is verified at boot, on purpose |
| Suspect data | `nuthatch check --dir <nest>` | runs the nest's committed invariant and parity checks against recorded fixtures |

---

## Data lifecycle

**Backup.** The nest directory is the whole state. Sealed segments are content-addressed and
immutable, so they are safe to copy while the process runs. The hot store (`nuthatch.redb`) is a live
redb file: snapshot it at the filesystem level, or stop the process for a consistent copy. The
hot store is not optional: every resume marker lives in it, so a nest directory restored without it
backfills from its start block again, and with a store older than its segments it re-reads the
difference. Back the two up together.

**Reading sealed data without this binary.** The sealed directory is plain Parquet plus a catalogue.
[Reading Nuthatch segments without Nuthatch](reading-segments.md) is the contract: layout, catalogue
schema, ordering, and the 256-bit encoding (canonical decimal text in `Utf8`; `_dec` / `_overflow`
are view columns the SQL engine adds, not Parquet). Point another engine at the same files. Do not glob
`segments/*.parquet`, and do not expect a narrowed numeric type in the file.

**Mirroring sealed data to a bucket (RFC-0052).** `nuthatch publish sync --target <dir or
s3://bucket/prefix>` copies every non-provisional segment and the catalogue, less its provisional
entries, under `<target>/<data identity>/`, then writes `publish.json`; a second run uploads only
what changed. A provisional segment is a table's tail of under 1,000 rows, held back because the
table's next seal rewrites it. A table that has stopped receiving rows, and every table of an
archived nest, keeps that tail forever, so the mirror never has those rows. When you know the chain
is done with the nest, stop it and run `nuthatch publish finalise --dir <nest>`, which lists the
tails, then again with `--yes`, which marks them final; the next `publish sync` uploads them.
Nothing is rewritten: the files and their hashes are unchanged. It refuses while anything holds the
nest's `nuthatch.redb`. If rows arrive after all, they start a new segment rather than folding into
the finalised one.
`dev` and `serve` do it continuously with `--publish-target`: a pass at start, one after every seal,
and one every `--publish-interval` (60 s) otherwise, with `--publish-parallelism` uploads in flight
(2). The publisher runs on its own thread, so a slow or unreachable bucket delays the mirror and never
a seal. Credentials are the usual `AWS_*` environment variables, never `nuthatch.toml`. The policy
is `s3:PutObject`, `s3:GetObject` and `s3:AbortMultipartUpload` on the prefix (HEAD is authorised
by `GetObject`) and `s3:ListBucket` on the bucket for that prefix, without which a missing object
answers 403 rather than 404. Segments upload in 8 MiB parts, so without `AbortMultipartUpload` a
failed upload leaves its parts billable until a lifecycle rule removes them. Do not grant `s3:DeleteObject`: a credential that cannot delete cannot take a published
segment away, so the mirror is append-only by policy rather than by promise. It can still overwrite
a key, which is what `doctor --publish` below catches, and bucket versioning makes that recoverable.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::my-bucket/nuthatch/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-bucket",
      "Condition": { "StringLike": { "s3:prefix": "nuthatch/*" } }
    }
  ]
}
```

Five consecutive failures on the same object set `nuthatch_publish_dead_letter` and slow retries
tenfold until one succeeds. A runtime directory publishes per mount instead, from `[mounts.publish]`
in `mounts.toml`, and refuses `--publish-target`. `nuthatch publish status --target <target>` prints
the target, local and remote `sealed_through`, and the segments and bytes still to upload, and writes
nothing. `nuthatch doctor --publish <target>` checks every object against the local segment, so an
object replaced by hand fails it even at the same size: a filesystem mirror is hashed, and a bucket's
ETags are compared only with `--publish-etag-md5` (`--etag-md5` on `publish verify`), which is right
when the store's ETag is the MD5 of the object, as on AWS S3 without SSE-KMS or SSE-C and on MinIO.
Without it a bucket fails closed as not content-checked, and on any other store
`nuthatch publish verify --deep`, which downloads and re-hashes, is the check. `/_admin/` shows each
publishing nest's target, lag and last success.

**Seeding a nest from a mirror.** `nuthatch seed --from <mirror> --dir <nest>` fills a nest that has
not indexed with a mirror's history, so it never backfills over RPC. `<mirror>` is what `publish sync`
was given as `--target`: a path, `s3://bucket/prefix`, or the `https://` address of a public bucket.
The nest directory must be the one that was published, unedited, because the dataset is found by its
data identity. A file one side holds and the other lacks moves that identity, and so does a
`schema.json` written by a different release, so publish from a clean checkout and commit
`schema.json`. Every file is checked against its hash, a second run resumes a failed one, and the
next `dev` follows the chain from the block after the mirror ends.
Seeding needs more than the reader's mirror holds: the provisional tails, and the block the
publisher's history begins at. Both are in a seed snapshot under `<dataset>/_seed/`, written by a
running nest's `--publish-target` once it has caught up, and by `publish sync` on a stopped nest.
`publish sync` beside a running nest cannot read its store and leaves the snapshot alone. A mirror
whose history begins after the nest's declared start block is refused.
The hashes show every file is the one the mirror's catalogue names. They do not show who wrote that
catalogue or that it is true to the chain, so seed from an operator you would trust to run the nest
for you. A seed that is interrupted leaves the nest marked, and `dev` refuses it until `seed` is run
again.

**Restore.** Put the directory back and start. Progress resumes from the checkpoint.

**Copying a stopped nest to another machine.** Copy the whole directory and start it there. Startup
checks the copy against what its store was indexed under and refuses a store whose data no longer
matches `nuthatch.toml` (#1420): a changed decode (an event, column or `[[ipfs]]` declaration), a
changed `block_timestamps`, or a change to what the data covers, which is the chain, each contract's
address and start block, the factories and the extraction settings. Refused, the store is left
untouched: restore the setting, or remove `nuthatch.redb` and `segments/` to re-index. RPC endpoints
and the nest's name are deliberately not checked, because they cannot change a stored row, so point
the copy at its own endpoints freely. They still move the NID and the data identity, which hash
`nuthatch.toml` as written; `/` and `/nest` show both for a running nest.

**Segment identity across versions.** A segment's hash covers the Parquet file bytes, which include
the `created_by` string stamped by the arrow-rs build. Same binary means identical bytes and identical
hashes, so re-running a backfill or running the same release on two boxes produces byte-identical,
de-duplicating segments. **Across nuthatch versions built on different arrow-rs releases, segment
identity may differ** even when every decoded row is identical. Do not use a segment hash as a
cross-version equality proof; compare decoded rows for that.

**Consistency: entity reads versus derived IVM views** (balances, exposure, velocity - the three
built-in DBSP relations, not authored `views/*.sql`). The entity store and those views
advance independently, so **a read taken during a reorg window can see transient skew between them**:
`/balances` (a view) and `/sql` (over stored rows) may briefly disagree about the same block. Both
converge within a tick; neither is wrong in isolation. This matters if you **join across the two
surfaces**, because the halves may have been taken either side of a rollback. If that skew would be
visible to your users:

- prefer a **single surface** per answer (each is individually consistent), or
- pin the read at or below `sealed_through` (reported on `/ready` and in `/sql` responses' `provenance`
  block) - sealed data is past finality and never moves.

Tip-following data is inherently provisional; this is the same caveat any indexer carries at the tip,
stated explicitly rather than left to be discovered.

**Binary upgrades.** Proven in production across 0.3.0 → 0.6.0 → 0.6.2 → 1.0.0 on a box serving public
traffic throughout: each was a binary swap and a restart, with no data migration and no flag changes.
Every 4.x release since has been the same swap and restart, and so is 5.0.0 for a nest without a
`[screening]` table, which 5.0.0 refuses until it is deleted. Each release's notes carry a
**Compatibility and upgrade** section that says so, or names the migration or re-index if one is
needed.

---

## Nest lifecycle operations

The deploy unit is a **content-addressed bundle**: config, ABIs, views, labels and skills, plus a
manifest pinning the expected decode-registry hash.

```sh
nuthatch nest bundle <dir>                        # produce a .bundle, prints its content address
nuthatch nest publish <bundle> --registry <ref>   # publish as name@version, advance latest
nuthatch nest load <ref> --registry <ref>         # pull and install, hash-verified
nuthatch nest load <bundle|url|dir> --expect <h>  # or install directly, asserting the hash
nuthatch nest nid --dir <dir>                     # the NID a runtime stores the nest under
nuthatch nest load <nid> --registry <ref>         # pull by NID, refused unless the bundle computes to it
```

The registry is **decoupled** from the binary: a filesystem path or S3-compatible object storage, with
private-nest authentication. nuthatch pulls; it never becomes the registry, and resolution stays
local-first. (Live S3 verification against a real bucket is still pending an operator run.)

A bundle carries `nuthatch.toml` verbatim, so `nest bundle` refuses one holding a webhook `secret`, or an RPC,
webhook or alert URL that looks keyed (userinfo, a key-named query parameter, or a long token in the
path). That test is a heuristic and cannot recognise every key, so keep credentials out of the file you
bundle: pass RPC endpoints at run time with `--rpc`. `--allow-secrets` bundles anyway, for a bundle that
is never meant to leave the box.

**Upgrading a nest** (RFC-0020 named the goal, "without the resync tax"; what 4.10.1 does is
narrower than that RFC, and is stated here as it is):

```sh
# There is no upgrade command. A pre-2.0 directory is migrated once; the runtime classifies the
# change and refuses a breaking one until you accept it.
nuthatch migrate --dir <runtime> --dry-run    # prints the plan, including any BREAKING changes by name
nuthatch migrate --dir <runtime>              # applies it; add --allow-breaking to accept one
```

- **A new version is a new NID**, because the NID is a content address: any edit to the nest's
  inputs, a view or a description included, yields a different dataset, and that dataset is indexed
  from its start block. What it shares with the old one is disk, not work: a sealed segment is
  content-addressed, so two datasets that decode the same contract over the same range hold one
  copy under the runtime's `segments/`. Per-derivation reuse across an NID change (RFC-0033, #357)
  is not built; `dev` computes the reuse keys at startup, warns about a view that could never be
  reused, and acts on none of it.
- **On a runtime the swap is `POST /_admin/move/<name> {"nid": "<new nid>"}`** (3.13.0): the new
  NID is mounted beside the old, caught up, then takes over the name's routes in one step, so a
  reader sees the old answers, then the new, and never an error between. The old dataset is left
  for `nuthatch prune` or `DELETE /_admin/datasets/<nid>`. For a change consumers would observe as
  removed, renamed or retyped, mount the new NID under a second alias instead and move consumers
  on their own clock; nothing in the binary serves two versions of one name.
- **`nuthatch migrate` classifies**, and only for a pre-2.0 directory: its plan names each
  `BREAKING` change per alias, and the run stops before anything moves unless `--allow-breaking`
  accepts it. A 4.x directory has nothing for it to do.

**Hosting nests on a runtime** (RFC-0027, 3.13.0). A runtime runs its own nests' lifecycle: a caller
hands it a NID and it does the rest, with no restart and no co-tenant interrupted.

- Declare the chains in `mounts.toml` and start with `--registry`. The runtime may start with nothing
  mounted; the first mount onto a chain starts that chain's cursor.
- `POST /_admin/nests {name, nid}` fetches a NID the runtime does not hold, verifies it, installs it
  at `data/<nid>/` and indexes it, reporting progress as a job. A refused mount leaves nothing behind.
- `?dry_run=true` first, to see the backfill, the per-block RPC work and the memory against the
  cursor's ceiling, and whether it would be refused.
- Suspend and resume a mount, move a name to a new NID without a gap, and unmount with
  `?reclaim=true` to free the disk once nothing else mounts the dataset.
- Meter per nest from `/metrics`: `nuthatch_nest_hot_store_bytes` and
  `nuthatch_nest_sealed_segments_bytes`.

Tenants are opaque labels the runtime refcounts; sign-in, plans, billing and per-tenant authorisation
belong to the gateway in front of it. The walkthrough is [admin-api.md](admin-api.md).

**Labels and flags** (RFC-0008):

```sh
nuthatch labels import addrs.csv --dir <nest>   # labeled set, feeds the exposure view
nuthatch audit report --from --to --json        # summarise threshold flags
nuthatch pack build --key <keypair>             # signed manifest of the registry, flags and alerts
```

---

## AI: point a coding agent at your data

The MCP server is compiled in. Wire a client to a running nest in one step - `nuthatch mcp
--print-config` prints exactly what to paste:

```sh
nuthatch dev &                       # the index the agent will query
nuthatch mcp --print-config          # copy-paste config for Claude Code / any MCP client
claude mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288
```

`nuthatch mcp` is a thin, fully-offline stdio bridge to the local `dev` HTTP API - it never contends
with the single writer and nothing phones home. The client launches it; you ask for your contract's
data in plain English and it writes the SQL. Tool advertisement is adaptive (RFC-0025): a nest only
advertises the tools it can actually answer.

---

## Stability contract

Nuthatch follows **semantic versioning**, and **5.x is the stable line**. This section is the
commitment, not a description of habits: what a release on the 5.x line may do to you, what waits for
6.0, and what is deliberately outside the promise.

4.x held this promise from 4.0.0 to 4.15.2. 5.0.0 broke it twice, both for the removed WASM layer: a
`[screening]` table no longer loads, and the `screen_status` MCP tool is gone ([notes](releases/v5.0.0.md)).

### The promise, for 5.x

From 5.0.0 to the last 5.x release:

1. **Config keeps working.** A `nuthatch.toml`, `mounts.toml` or `entities.toml` that works on a 5.x
   release works on every later 5.x release, with the same meaning.
2. **Data directories upgrade drop-in.** A later 5.x opens a data directory written by an earlier 5.x
   in place: replace the binary and restart. No re-index, no re-seal, no migration command.
3. **The HTTP, SQL and MCP surfaces do not break.** No route, response field, generated table or
   column, or MCP tool is removed, renamed or retyped.

**Upgrade only.** The promise runs forwards. A later 5.x may update the on-disk format as it opens a
directory, and an earlier 5.x is not promised to read the result. To keep a way back, copy the data
directory before upgrading.

### How releases are cut on 5.x

- **A released 5.x only ever gets patch releases** (5.0.1, 5.0.2, ...), and a patch only fixes.
- **Features wait for the next monthly minor** (5.1, 5.2, ...). A minor may add; it may not break
  anything in the table below.
- **Correctness and security fixes ship immediately**, as a patch, without waiting for the month.
- **A correctness fix is not a break**, even where an answer changes: a wrong answer made right is
  the fix. Nuthatch never re-decodes stored history, so where rows were stored wrong the notes name
  the affected range to index again (3.13.2's Arbitrum timestamps are the model). Where a config was
  accepted but silently produced wrong data, the fix may refuse it at startup, and the refusal says
  what to change. A security fix with no compatible form may break a surface in a patch. In every one
  of these cases the release notes say so, name the fix, and explain why.

### What a minor may change, and what waits for 6.0

| Surface | A 5.x minor may | Reserved for 6.0 |
|---|---|---|
| `nuthatch.toml`, `mounts.toml`, `entities.toml` keys | add a key; deprecate one with a startup warning | remove or rename a key, or change its type or meaning |
| nest `schema_version` | bump it when the upgrade is in place and automatic | a bump that requires a re-index |
| On-disk layout (redb tables, segment layout, `manifest.json`, `schema.json`, `data/<nid>/`) | change it when the upgrade is in place and automatic | a layout needing a re-seal, a re-index or a migration command |
| HTTP routes and response shapes | add a route; add a field | remove a route, remove a field, or change a field's type or units |
| SQL: generated tables and columns (`{alias}__{event}`, the fixed columns, the event's params), authored views and entities by name | add a table or a column | remove or rename a table or column, or change a column's type |
| MCP tools | add a tool | remove or rename a tool, remove an argument, or make an optional argument required |
| CLI flags | add a flag; deprecate one with a warning | remove or rename a flag on `init`, `dev`, `sql`, `add` or `check` |
| Metric names and labels | add a metric or a label | remove or rename a `nuthatch_*` series, change its unit, or change what a label means |

**MCP tools are advertised per nest** (RFC-0025): a nest lists the tools it can answer, so discover
them with `tools/list` rather than hardcoding the list. The promise is that a tool a nest advertises
on a 5.x release is still advertised, under the same name and with compatible arguments, for that
nest on every later 5.x. Tool descriptions and the layout of result text may change; the data a
result carries does not.

**Deprecation window.** A deprecation is announced in at least one minor before removal, removal
comes **no sooner than 90 days** after that release, and removal itself waits for 6.0. The warning
names the replacement. That is the floor rather than the target: anything an operator wires into a
unit file, a scrape config or a dashboard gets longer.

### Not covered by 5.x, deliberately

Stated because a platform team will ask, and because a vague promise is worse than a narrow one.

- **Downgrades**, above. Upgrade only.
- **Off-by-default cargo features: `graph`, `folds` and `counter`.**
  None is in the published binaries or images, and all are experimental: their config keys, routes,
  on-disk state and behaviour may change or go in any 5.x release. A build that enables one is not
  covered by any line above for what that feature adds. (`graph` is RFC-0053's partial read surface
  and RFC-0060's parked endpoint; `folds` is RFC-0059, parked; `counter` is x402.)
- **The SQL dialect itself.** The tables and columns are nuthatch's and are covered; the functions and
  planner behind `/sql` are the engine's: Burrmill, on DataFusion, since 4.1, and DuckDB before it. An
  engine change that alters a function's behaviour ships in a minor, and its notes say so. The resource guards (timeout, row cap, spill cap) may tighten in a
  patch when that is the security fix.
- **Segment identity across `arrow-rs` releases.** Byte-identical segments are a correctness
  boundary, not an API, and the boundary belongs to a dependency we do not control: a segment's hash
  covers the Parquet bytes including arrow-rs's `created_by` stamp, so identical decoded rows can
  hash differently under a different build. What *is* promised is that sealed segments stay readable
  and are never rewritten; compare decoded rows, not hashes, across versions.
- **`semantic.toml`'s derived half.** Its `[table.*.footguns]` are regenerated from the ABI; your
  authored descriptions are preserved, and that half is covered.
- **Rejecting what was never read.** Since 3.13.2, `nuthatch check` fails on a `nuthatch.toml` key
  nuthatch does not read. A config using a key added in 4.2 is not promised to pass `check` on 4.1.
- **The admin UI's internals.** `/_admin` is a human surface, not an API. Its HTML and internal
  endpoints may change in any release; the JSON admin API (`docs/admin-api.md`) is covered by the
  HTTP row.
- **The `postgres-store` build's internal schema** while scaled mode is young. Scaled mode's external
  surfaces (its HTTP API, its CLI, its config) are covered like everything else.

### How the promise is checked

Partly by tests, partly by release practice, and the difference is stated here rather than left to
be found. What a test enforces in the required CI job:

- **Data directories:** `tests/upgrade_golden.rs` (#1597) copies a runtime directory written by
  v3.13.2 and never regenerated (`tests/fixtures/upgrade/`), opens it with the current build and
  checks exact values: both mount records, the NID recomputed from the stored nest, the sealed
  segment's hash, the hot store's rows and watermarks, `/sql` over sealed and hot rows, `/entity`
  point reads and an authored entity's value. It then indexes two more blocks and seals, and checks
  those too. Renaming a redb table or changing the NID derivation turns it red. The tree holds that
  one data directory and a `config-4.0` beside it; no later release has added a directory of its
  own, and none is ever rewritten.
- **Config:** the same file freezes a `nuthatch.toml`, `mounts.toml` and `entities.toml` as a 4.0
  user writes them, and checks what each key means, not only that it parses. Four more committed
  `nuthatch.toml` files must parse with no unknown key (`only_keys_nothing_reads_are_unknown`).
- **Data identity:** the decoder's registry hash for ERC-20 and tuple ABIs is pinned to the value
  3.6.1 printed (`decode/src/registry.rs`).
- **HTTP and MCP:** the core routes must exist and answer, and the generic MCP tools must be
  advertised by name.

What no test enforces yet: the fixture is one ERC-20 nest, so factories, calls, `[[ipfs]]` and
offchain data have no frozen store behind them; and there is no golden of HTTP response shapes
beyond the fields the upgrade test reads, nor of MCP tool arguments. Until there is, each release's
notes carry a checked compatibility section (what `src/cli.rs`, `src/metrics.rs`, `src/blob.rs` and
the registry did between the two tags), and a release that cannot state it is not tagged.

### History

This table covered `1.x`, then `2.x`, and was retargeted at each major **as part of cutting the
release**. At 3.0 it was not: through the whole 3.x line it still said `2.x`, the same gap #312
recorded at 1.0. The 3.x minors also did what the table reserves for a major more than once, each as
a correctness fix and each named in its notes: a config an earlier 3.x accepted was refused (3.1.0,
3.9.0), a nest had to be re-indexed (3.7.0, 3.8.0), and `POST /_admin/nests` moved from `200` to `202`
(3.13.0). 4.0 retargets the table, adds the release cadence above, and makes the correctness-fix
exception explicit rather than habitual.

---

## Upgrading to 2.0

Written **after** migrating a real deployment, not before. The numbers below are from the two-nest
Lodestar box, not from a fixture - the distinction matters, because a fixture and that box have already
disagreed about this exact behaviour once, and the box was right.

### What it costs

```text
BEFORE   882 MB   504 parquet files
$ nuthatch migrate --dir /opt/verify20
Shared 504 segment(s) into segments/ (504 duplicate copies reclaimed).
real 0m0.259s
AFTER    880 MB   252 parquet files      per-dataset leftovers: 0
```

**A quarter of a second, and nothing re-indexed.** A separate 428 MB single-nest run took 0.144s. The
migration moves files and rewrites a config; it never touches the chain.

Note the disk total barely moved, and that is not a failure: the segments were ~4 MB of that 882 MB,
and the bulk is two 393 MB redb hot stores which are per-dataset by design - mutable, reorg-affected,
and not shareable. **The file count is the measure of the collapse, not `du`.** Nests with a lot of
sealed history will see the byte saving too; these two simply had little.

Rehearse it anyway: `nuthatch migrate --dir <copy> --dry-run` prints the whole plan and changes
nothing.

### What breaks, and where it is in the table above

| Break | Table row |
|---|---|
| `roost.toml` → `mounts.toml`, `[roost]` → `[runtime]` | config keys - *rename, reserved for a major* |
| `nuthatch roost dev` → `nuthatch dev` (one command; the directory decides what runs) | CLI flags/commands |
| `nest diff` and `nest upgrade` removed - the runtime now classifies at the moment identity changes | CLI |
| On-disk layout: `nests/<name>/` → `data/<nid>/`, plus a shared content-addressed `segments/` | on-disk layout - *`nuthatch migrate` performs it* |
| `GET /nests` roster field `roost` → `runtime` | HTTP - *field rename* |
| The Starlark config front-end is gone (retired 2026-07-21, unreachable since) | config |

Every row is a break the contract reserves for a major. Nothing here was a surprise to the table,
which is the point of having written it first.

Additive in the same release, so nothing to do: `provenance` now carries `nid`, naming *which dataset*
answered rather than only how it decoded - which matters once early cutoff lets a result legitimately
come from data a different identity produced.

### A single nest has nothing to migrate

If the service runs `nuthatch dev --dir <nest>` against a directory holding a `nuthatch.toml`, the
upgrade is a **binary swap**: stop, swap, start. The layout change is to *runtime* directories, and a
solo nest does not have one. `nuthatch migrate` is for a directory containing a **`roost.toml`**; no
such file, nothing to run.

Verified on the Lodestar box: two solo nests upgraded 1.0.2 → 2.0.0 by binary swap, every table's row
count identical before and after (422 and 3,491 rows), both back at tip within seconds, no migration
invoked. Before the swap, 2.0.0 was pointed read-only at a copy of one nest's on-disk data and counted
byte-identical rows at a common block ceiling - so the compatibility was measured on real data, not
assumed from the version number.

### The order, for a runtime directory

1. `nuthatch migrate --dir <copy> --dry-run` against a copy. Read the plan.
2. Stop the service. The migration wants no writer.
3. Swap the binary, `nuthatch migrate --dir <runtime>`, start.

A breaking schema change is **named and refused** with nothing moved; add `--allow-breaking` once
consumers are ready, or mount the new version under a different alias and migrate them on their own
clock. The data is safe either way - this is about queries, not bytes.

---

## Known gaps

Stated plainly, because finding them yourself in production would be worse.

**Before exposing `/sql`:**

- **0.6.1 and earlier accept `;`-stacked SQL statements**, which combined with `COPY … TO` / `ATTACH`
  is an arbitrary file-write primitive, bounded by the service user's permissions. **Fixed in 0.6.2**
  (released 2026-07-28). If you are on 0.6.1 or earlier, upgrade before exposing `/sql` to anything -
  authentication in front does not help, because the caller is already past it.

**Operational gaps:**

- **`/sql` materialises the whole tip per query**, now **bounded**: past 2,000,000 unsealed rows the
  query is refused with `503` and the response names `sealed_through` so a caller can narrow to sealed
  data. It refuses rather than truncating, because a partial tip would silently change the answer to an
  aggregate. Generous enough to be invisible on a normal chain; it exists so a deep-finality tip turns
  into a clear error instead of an OOM that takes co-tenants with it.
- **Container images are published** to `ghcr.io/nuthatch-org/nuthatch` - `:<version>` for embedded,
  `:<version>-scaled` for the scaled build. The recipe above still works if you would rather build one.
- **Secrets live in on-disk config *in embedded mode*.** Private RPC URLs and webhook HMAC secrets sit
  in the nest's `nuthatch.toml`, so the mitigation there is filesystem permissions: `0700`, owned by
  the service user. In **scaled** mode they live in the control plane and are injected per nest, per
  worker, scoped to the cursors that worker actually holds (RFC-0022 §5, built) - write-only, so you
  can list which keys exist and never read a value back.
- **Scaled mode is built but young.** The control plane, the writer pool, cursor leases with a
  store-enforced fence, the query-FE tier and the registry pull all exist and are verified across real
  machines (runbook level 5). It has not run a production workload for anyone, and until 0.9.3 the
  writer pool did not index at all (#250) - a defect a suite of ten passing checks did not catch,
  because every one of them tested the control plane rather than the data. Weigh it accordingly.
  DataFusion federation (RFC-0013) was never built and was closed on 2026-09-08; both modes use the
  embedded engine, Burrmill.
- **No ExEx or trace/state extraction.** Colocated-reth ingestion (RFC-0003) and firehose-class
  extraction (RFC-0014) are gated on a synced node - an infrastructure decision, not a coding one.

**Correctness boundaries, documented and accepted:**

- Balances exceeding `i128` base units are omitted from derived views rather than truncated. No real
  token approaches this.
- One malformed log fails its whole `getLogs` window and retries against another endpoint rather than
  being skipped. Deliberate: silently dropping an on-chain event would be a correctness bug.
- An endpoint that leaves a log out of the middle of a `getLogs` answer is not detected at ingest;
  the sealed-history audit above finds it after the fact, by sampling.
- Segment identity may differ across nuthatch versions built on different arrow-rs releases.
- Entity reads and derived views are eventually consistent within the reorg window.

---

## Proving it works

This guide tells you how to *run* nuthatch. [`verification.md`](verification.md) tells you how to
**prove it works** - an acceptance runbook where every step has a command, an expected result, what it
proves, and what a failure means.

Worth walking before go-live, and worth handing to a second operator: a claim someone else confirmed is
worth more than one we assert. It also states which levels **we** have verified and which we have not -
scaled mode's compose stack and any multi-machine run are the honest gaps, and the ones where outside
verification is most valuable.

## Go-live checklist

Before pointing real traffic at a nuthatch deployment:

- [ ] Running a release that includes the `/sql` statement-stacking fix (post-0.6.1).
- [ ] Bound to localhost or an internal interface, with TLS and authentication in front.
- [ ] Admin surface accounted for: bound to localhost, or `--no-admin`, or a generated
      `NUTHATCH_ADMIN_TOKEN` behind TLS.
- [ ] Running as an unprivileged user; nest directory `0700`; `MemoryMax` set to the cursor budget.
- [ ] Prometheus scraping `/metrics`; alerts wired for quarantine, cursor death, tip lag, ingest
      stall and memory.
- [ ] Load balancer routes on **per-nest** `/<name>/ready`; paging on the runtime-root `/ready`.
- [ ] `nuthatch bench backfill` and `nuthatch bench query` run on your hardware and RPC; sizing
      derived from the measurements, not the projection.
- [ ] Every `rpc_urls` pool has at least two endpoints on the correct chain (verified at startup).
- [ ] Backup covering the nest directory, tested by restoring into a clean box.
- [ ] `nuthatch check` passing for each nest, with parity fixtures committed.
- [ ] A restart drill performed: SIGTERM, restart, confirm no gaps or duplicates.
- [ ] A binary upgrade rehearsed on a copy of the data directory: the 4.x promise runs forwards only,
      so the copy is the way back (see [Stability contract](#stability-contract)). A pre-2.0
      directory is rehearsed with `nuthatch migrate --dry-run` on the copy first.
- [ ] Nest onboarding process agreed. *(Adding a nest no longer restarts the runtime - `POST
      /_admin/nests` mounts one live, since 0.7.0. This line said otherwise for two releases.)*
- [ ] **The acceptance runbook walked**, at the levels matching your deployment - see
      [`verification.md`](verification.md). It is falsifiable step by step, which this checklist is
      deliberately not: a checklist records a decision, a runbook produces evidence.
