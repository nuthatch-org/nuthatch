# RFC-0073: The shared runtime - one runtime, many organisations' nests

- Status: **Accepted (2026-10-27, Chief): "lets do ALL of it".** All fifteen requests in GraphOps's
  "Feature requests: hosting every customer's nests in one shared runtime" (written against
  `efbcb3f`, v5.12.0). The out-of-scope list in `CLAUDE.md` is amended the same day for FR2, FR4's
  cost half and FR8.
- Author: Pete (cargopete)
- Date: 2026-10-27
- Depends on: RFC-0012 and RFC-0032 (the multi-nest runtime and mounts), RFC-0027 (the live roost:
  admission and the cursor ceiling), RFC-0021 (one cursor per chain), RFC-0034 (`queries.toml`),
  RFC-0049 §2 (decode drops), RFC-0019 (the bundle registry), RFC-0022 (scaled mode).
- Blocks: GraphOps's public launch of hosted nests.
- Nature: new capability in the default binary for an operator who hosts many organisations' nests
  in one runtime. Every item is operator configuration or operability. Nothing makes nuthatch know who
  a caller is: authentication, keys, accounts, billing and identity stay in the operator's gateway.

## 1. What this is for

GraphOps will run the stock binary and image as a hosted data service. Customers build nests with
`nuthatch init`, submit `nest bundle` output, GraphOps verifies it with `nest load`, publishes it to a
private S3 registry and mounts it by NID on one runtime as `<organisation>/<slug>`. A GraphOps proxy
does TLS, keys, routing, rate limits, metering and billing.

The runtime already hosts many nests with no restart and no co-tenant interrupted. What it does not do
is behave well when the tenants are unrelated organisations paying for a service: one tenant's burst
takes every other tenant's query permits, guards are process-wide constants, metrics cannot tell
mounts apart, a panic in one nest takes its whole chain down, and every restart is everyone's outage.

## 2. What exists, read 2026-10-27 at `origin/main` (`42e095e4`)

**Query admission.** One `tokio::Semaphore` per chain cursor, shared by every nest on it
(`serve.rs:90`, reused at `runtime.rs:4443-4448`): 2 permits by default, 16 at most, set only by
`NUTHATCH_SQL_MAX_CONCURRENCY` (`serve.rs:36,54,133-157`), a 250 ms admission wait inside the 30 s
timeout (`serve.rs:175,4772`), 256 queued per nest (`serve.rs:191`). Response bytes in flight are one
process-wide 256 MiB semaphore (`serve.rs:107,113-121`). Nothing at the gate knows a tenant or mount;
a request learns its mount only from its route (`serve.rs:634`, `runtime.rs:301-307`).

**Guards.** Statement timeout 30 s, 50,000 rows, 64 MiB result, 2,000,000 hot rows / 64 MiB, 512 MiB
named-query scan, 16 KiB query text: compile-time constants copied into `AppState`
(`serve.rs:162-256`, `indexer.rs:3554-3557`, `runtime.rs:7226-7229`). Analytics memory and threads are
env only (`analytics_budget.rs:1-33`). The one per-mount guard is the `sql` surface
(`allowlist.rs:38-48`, `runtime.rs:169-175`). `POST /_admin/nests` takes only `{name, nid}`
(`runtime.rs:2390-2402`), and `GET /_admin/mounts/<name>` returns the mount job, not the record.

**Metrics.** `nuthatch_http_requests_total`, `nuthatch_sql_queries_total` and
`nuthatch_sql_rejections_total{reason}` are process-global (`metrics.rs:651-653,931,1246-1268`). Per-nest
series exist for indexing and the analytics pool (`metrics.rs:1436+`, `:1468-1484`). There is no SQL
duration histogram. Burrmill creates DataFusion's plan metrics and discards them
(`burrmill df/catalog.rs:197-199`).

**Hot tip.** Every `/sql` call reads the whole hot tip into the session
(`serve.rs:4791-4813`, `analytics.rs:2998-3018`) unless the memo hits (`serve.rs:4693-4715`).

**Memory.** `[runtime] max_rss_mb`, default 2048, read once at boot (`runtime.rs:364-368,472,1970`).
Admission charges each mount an estimate (`runtime.rs:624-654`) and refuses `507` over the ceiling or
over 32 entity circuits per cursor (`runtime.rs:569-592,4029-4130`). At boot an over-budget cursor
stops the whole runtime (`runtime.rs:2024-2063`).

**Faults.** Per-nest quarantine covers window errors, `prepare`, reorg rollback, dying IVM threads,
an entity over `max_rows`, finality violations, unfixable over-cap `getLogs` and refused state calls
(`indexer.rs:1561-1686,2025-2418,7405-7512`), with co-tenant tests (`indexer.rs:13635,15811`,
`runtime.rs:6995`, `tests/e2e_cursor_death_isolation.rs`). Two gaps: **a panic in one nest's ingest
kills the cursor**, quarantining every nest on that chain (`runtime.rs:2320-2338`), and **one nest's
`build_nest` error aborts the whole boot** (`indexer.rs:2759`, `runtime.rs:2087`). A `/sql` panic is
already confined to its request (`serve.rs:4784,4901`).

**Restarts.** The listener binds after every cursor is built (`runtime.rs:2181,2241`). Restart-to-ready
is measured for one nest only (68-74 ms, `docs/bench/rfc-0042-992-restart-to-ready.md`). There is no
handoff between binaries: redb takes an exclusive lock at `Database::open` (`store.rs:605-610,725-727`).

**Chains.** Declared at boot as `{chain, chain_id, rpc_urls}` (`runtime.rs:309-339`); finality and
window come from the built-in registry (`chains.rs:545-546`). There is no `/_admin/chains`, and an
undeclared chain is `409` (`runtime.rs:4037-4043`). RPC URLs are a fixed `Vec` (`rpc.rs:860-863`).

**Read replicas.** Scaled mode's `serve` is per nest over Postgres (`indexer.rs:3628-3740`); the
multi-nest runtime has no `--hot-store`. Nothing lets a second process read an embedded data dir.

**SQL isolation.** Read-only enforcement is layered (`analytics.rs:1335-1395,1739,1822,1979,2275,2318`,
burrmill `df/mod.rs:441-443`). Table functions are an allow-list of three (`analytics.rs:2018`);
scalar functions are a deny-list. Confinement is structural: a session is opened per `data/<nid>`
and starts empty (`analytics.rs:45,1402`, `engine_burrmill.rs:3`). **Two mounts of one NID share that
session, memory pool and memo.** `fuzz/` covers decode only. Two past escapes: `read_csv` via a quoted
name (v0.9.3) and stacked `COPY … TO` (v0.6.2).

**Bundles and registry.** `nest bundle`, `nest load`, `nest publish` print text only
(`blob.rs:163-230,352,630-728`, `distribution.rs:316`); none has `--json`. `nest load` warns about
webhook, alert and RPC egress but not IPFS or offchain sources (`blob.rs:561-610`). Registry
credentials are the `AWS_*` env vars (`distribution.rs:440-487`); a mount job's failure reason is free
text (`mount_jobs.rs:42-48`, `runtime.rs:3798-3805`). CI's S3 job tests the segment mirror, not bundle
fetch by NID (`tests/e2e_minio_publish.rs`).

**Decode drops.** Three sites log at debug and count nothing (`indexer.rs:5280,6080,9345`); a factory
child's decode errors are dropped with no log at all (`indexer.rs:9342`).

**MCP bridge.** `mcp --url` sends no headers (`mcp.rs:607-635`); URLs in errors are already redacted
(`mcp.rs:643-653`).

**Security policy.** "Only the latest release receives security fixes" (`SECURITY.md:30-37`), while
`docs/releases/README.md:13-24` describes `release/<maj>.<min>` patch branches.

## 3. The design, request by request

Each item keeps GraphOps's acceptance criteria. Where the design narrows or extends one, it says so.

### FR1. Query capacity beyond one process

A **read-only `serve` role over the runtime's data directory**: `nuthatch serve --runtime <dir>
--follow <writer-url>`. It opens sealed segments directly (they are immutable files), never opens redb,
and takes the hot tip from a **hot-state feed** the writer exposes on its admin listener: a stream of
committed hot-window deltas per nest, keyed by `write_generation`, from which the reader keeps its own
in-memory `HotStore`. Mount changes arrive on the same feed. A reader reports per-nest readiness and its
lag behind the writer, and answers with the same `as_of`, `sealed_through` and `nid`. Adding a reader
needs no re-index and no writer restart. The feed is an operator-only endpoint behind the admin token.

### FR2. Fair share between tenant labels

Weighted fair queueing in front of each gate, keyed by **tenant label**, the same opaque string the
runtime already refcounts. Each tenant gets a share of permits and of response bytes in flight,
weights default to equal, and an idle tenant's share is lent. A tenant at its own share is refused with
`reason="tenant_share"`, distinguishable from process saturation (`reason="busy"`). Weights and caps are
set by `PUT /_admin/tenants/<tenant>` and persisted in `mounts.toml`. The acceptance bound is theirs:
a saturating tenant delays another tenant's admitted query by no more than the 250 ms admission wait.
Nuthatch still does not know who a tenant is.

### FR3. Per-mount resource limits

`POST /_admin/nests` and a new `PATCH /_admin/mounts/<name>` accept optional `limits`: statement
timeout, max rows, max result bytes, hot rows and bytes scanned, named-query scan bound, analytics
memory, max queued and the `sql` surface. Each is clamped to the process value, persisted in the mount
record (outside the NID), applied without interrupting the mount, and returned by
`GET /_admin/mounts/<name>` along with the rest of the record. The guard constants become process
defaults settable by flag and env, still clamped to today's ceilings.

### FR4. Per-nest metrics, and a cost report

**Metrics (launch):** a `nest` label on the HTTP, SQL and rejection counters, a per-nest SQL duration
histogram, and the existing per-nest analytics pool series. Cardinality is bounded by the mount count.

**Cost (later):** every `/sql`, `/q/{name}`, GraphQL and typed-route response carries a
`Nuthatch-Cost` header: sealed bytes scanned, hot rows scanned, CPU time, peak engine memory, queue
wait and result bytes. Sealed bytes scanned must be identical for the same query over the same sealed
data, which is tested. This needs Burrmill to return DataFusion's plan metrics instead of discarding
them. It is a report, not a price: nuthatch does not meter or bill.

### FR5. Memory budgets that grow

`PUT /_admin/runtime` raises (or lowers, refusing below current use) `max_rss_mb` live, persisted in
`mounts.toml`. The entity-circuit cap becomes a function of the ceiling (32 per 2 GiB, at least 32).
Boot stops refusing the whole runtime for an over-budget cursor: it boots the mounts that fit, in
`mounts.toml` order, and marks the rest refused with the reason. The per-query hot-tip copy is charged
to the requesting tenant's share (FR2) and bounded by the mount's limits (FR3); replacing the copy
with a scan of the hot store is a later optimisation, not a launch item. GraphOps's alternative, several
cursors per chain, is not taken: one cursor per chain stays a law, and a second runtime is the way to
split a chain's load.

### FR6. Fault containment between mounts

Per-nest ingest runs under `catch_unwind`; a panic quarantines that nest as a terminal fault with the
panic message as its reason, and the cursor carries on. A nest whose `build_nest` fails at boot is
booted quarantined rather than aborting the runtime. A slow-but-healthy nest gets a per-nest window
deadline so it cannot hold its co-tenants' window. The guarantee is documented in `docs/admin-api.md`
per fault class (decode, entity over `max_rows`, maintained-view failure, query panic, extraction RPC
error, ingest panic, build failure), each with a test in which a co-tenant keeps indexing and serving.

### FR7. Restarts and upgrades without an outage

With FR1, readers keep answering through a writer restart from sealed data and their last hot state,
and report the writer's absence as lag rather than failure. The writer's restart time is bounded and
measured for 1, 10, 100 and 500 mounts, published in `docs/bench/`. The release gate gains a rolling
upgrade test: previous release writer and readers, replace readers, replace writer, with a query
answered at every step.

### FR8. Query isolation for mutually untrusted tenants

The shared runtime is the configuration where this guarantee holds: `[runtime] isolation = "strict"`.
It sets:

- **A written threat model** (`docs/security/shared-runtime.md`).
- **An allow-list of scalar functions** (today a deny-list), documented, with a test that fails when
  Burrmill or DataFusion adds a function the list does not name.
- **No session sharing across tenants**: two tenants mounting one NID get separate sessions, memory
  pools and memo entries, keyed by tenant as well as dataset.
- **Confinement tests that attack it**: a query on mount A tries every known path to B's files,
  segments, hot rows and catalogue (quoted function names, replacement scans, `information_schema`,
  view text, file URLs, stacked statements) and must fail.
- **A SQL fuzz target** in CI asserting read-only enforcement and confinement.
- **An independent review** GraphOps commissions before launch; findings come to us first.

The guarantee is not claimed in docs, release notes or to GraphOps until that review has run.

### FR9. Add a chain to a running runtime

`POST /_admin/chains` declares a chain (`chain`, `chain_id`, `rpc_urls`, optional finality depth and
window for a chain the registry does not know), dormant until mounted, as boot-declared chains are.
`PUT /_admin/chains/<chain>/rpc` replaces the RPC pool live; in-flight requests finish on the old pool.
Both persist in `mounts.toml`.

### FR10. Machine-readable bundle inspection

`nuthatch nest inspect <bundle|dir> --json`, no runtime needed, with a versioned schema
(`"schema": "nuthatch.nest-inspect/1"`): NID, data identity, chain, contracts and events, factories and
templates, entities, feature-gated declarations, extraction that needs archive or trace RPC (`[[calls]]`,
`[extract]` traces and state, address history), and every egress (RPC URLs, webhooks, alerts, IPFS
gateways and declarations, offchain sources). `nest load` gains the same egress list, closing its IPFS
and offchain gap.

### FR11. `POST /sql`

`POST /<name>/sql` with `{"q": "...", "max_rows": n}`, identical to the `GET` form in guards and
response. The same for `/explain`. `/q/{name}` gains a JSON body form for its arguments.

### FR12. A supported release line

Policy, decided by Chief, recorded in `SECURITY.md`. The proposal: security fixes are backported to the
previous minor release for 30 days after a new minor ships, on the `release/<maj>.<min>` branches the
release docs already describe; hosting operators who ask get private advance notice of a security
release, under a disclosure agreement.

### FR13. Credentials in the MCP bridge

`nuthatch mcp --url <u> --token-env <VAR>` (and `NUTHATCH_MCP_TOKEN`) sends `Authorization: Bearer`
on every request. The token is never printed by `--print-config` or the guide, and a non-2xx body is
passed through with any echo of the token redacted.

### FR14. Decode gaps as metrics and status

`nuthatch_nest_logs_undecoded_total{nest, contract, topic0}` at all four drop sites, including the
factory child that is dropped silently today, and an `undecoded` block in `/<name>/ready` and `/nest`:
the total and the five largest contract/topic pairs. The `Err` arm logs at `warn`, rate-limited per
window. This is RFC-0049 §2's proposal, built.

### FR15. Verified S3 registry fetch

A CI job against versitygw with a private bucket: `nest publish`, then a runtime mount by NID through
the admin API, then a denied credential and a missing NID. Credentials come from object_store's standard
AWS chain (env, web identity, container and instance metadata), never `mounts.toml` or the command
line. A failed mount job gains a structured `code`: `not_found`, `access_denied`, `verification_failed`
or `other`, alongside the reason text.

## 4. Order of work

Each slice ends runnable and keeps the ≤2 GB per-cursor CI budget for a default single-nest runtime.

| Slice | Requests | Size |
|---|---|---|
| S1 | FR11, FR13, FR14, FR10, FR4 metrics, FR15 | days each |
| S2 | FR6, FR3 | one to two weeks |
| S3 | FR2, FR5, FR9 | two to three weeks |
| S4 | FR8 (gated on the external review) | two to three weeks, then the review |
| S5 | FR4 cost (needs a Burrmill change) | one week |
| S6 | FR1, FR7 | the largest: weeks |
| - | FR12 | Chief's decision |

## 5. Non-goals

Authentication, API keys, accounts, identity, billing, metering as pricing, a hosted control plane,
any GraphOps-specific code path, Kubernetes manifests. Several cursors per chain. Nuthatch running the
service itself: the 2026-09-29 retirement stands.

## 6. Risks

- **FR8 is a promise.** A read-only escape in Burrmill or DataFusion becomes cross-customer exposure on
  a shared runtime. Mitigated by the allow-list, per-tenant sessions, fuzzing and the review gate, and by
  scoping the guarantee to `isolation = "strict"`.
- **FR1's hot-state feed** is a second consumer of the hot store's commit stream; it must not slow the
  writer's tip path. Tip lag's after-seen p50 stays a gated benchmark.
- **FR2 adds a queue in front of every query**; its uncontended cost is measured and gated.
- **The amendment** widens what nuthatch does about tenants from "refcounts a string" to "shares
  capacity by a string". It still knows nothing about who the string is.

## 7. Open questions

1. FR12: the backport window and whether to offer private advance notice.
2. FR1: whether readers may also run on another host from a segment mirror plus the feed, or only
   beside the writer.
3. FR2: whether shares apply per chain gate (today's structure) or across the process.
