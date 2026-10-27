# The runtime admin API

A runtime (`nuthatch dev --dir <dir>` over a directory holding a `mounts.toml`) manages its own nests.
A caller hands it a nest identity (NID) and the runtime fetches it, verifies it, stores it, indexes it
and serves it. This page is the whole surface for a platform or a script driving that lifecycle
(#1552). Nothing here knows who the caller is: sign-in, plans, billing and per-tenant authorisation
belong to whatever sits in front of the runtime.

## Starting a runtime to be driven

```toml
# mounts.toml
[runtime]
name = "fleet-1"

[[chains]]
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://…"]
```

```sh
NUTHATCH_ADMIN_TOKEN=… nuthatch dev --dir fleet-1 --listen 0.0.0.0:8288 --registry s3://bucket/nests
```

- **Chains are declared here, at boot.** A runtime may start with nothing mounted; the first mount onto
  a chain starts that chain's cursor and dials its RPC only then. Chains cannot be added over the API.
- **`--registry`** is where a mount fetches a NID the runtime does not hold: a filesystem path or
  `s3://bucket/prefix`. Publish to it with `nuthatch nest publish <bundle> --registry <path>`; a NID is
  what `nuthatch nest nid --dir <dir>` prints. Without `--registry`, only NIDs already under `data/` mount.
- **Every request must present the admin token, localhost included**, as `Authorization: Bearer
  <token>` or `?token=<token>`; without it the answer is `401`, whatever the `Host` (#2132, #2354).
  The token is `NUTHATCH_ADMIN_TOKEN` when set. Without it, a localhost runtime makes one on first
  start and keeps it in `<dir>/.admin-token`, owner-only, where `<dir>` is the runtime's `--dir`; the
  startup log prints an admin URL carrying it, and `TOKEN=$(cat <dir>/.admin-token)` is what a script
  reads. An off-localhost runtime without the variable serves no admin routes at all. `--no-admin`
  removes them everywhere.
- **Request bodies are JSON** and need `Content-Type: application/json`; without it a `POST` is
  answered `415`. That holds for suspend and resume too, though they read no body, so
  a cross-site form cannot reach them (#1642).

## The lifecycle

| Do | Call | Answers |
|---|---|---|
| Mount a NID under a name | `POST /_admin/nests` `{"name": "usdc", "nid": "<nid>"}` | `202` and the job |
| Mount it filled from its mirror | the same, with `"seed": true` and optionally `"publish": {"target": "…"}` | `202` and the job |
| Read a mount's progress | `GET /_admin/mounts/<name>` | the job, and its mount record under `record` |
| List every mount | `GET /_admin/mounts` | `{"mounts": [job, …]}` |
| Price a mount first | `POST /_admin/nests?dry_run=true` with the same body | `200` and a report |
| Change its SQL surface or limits | `PATCH /_admin/mounts/<name>` `{"limits": {"max_rows": 1000}}` | `200` and the record |
| Pause a mount | `POST /_admin/suspend/<name>` | `200` |
| Resume it | `POST /_admin/resume/<name>` | `202` and the job |
| Move a name to a new NID | `POST /_admin/move/<name>` `{"nid": "<new nid>"}` | `202` and the job |
| Unmount | `DELETE /_admin/nests/<name>` | `200`, with `"was_mounted": false` when there was nothing to unmount (a retry is not a failure); `400` for a name no mount could have |
| Unmount and free the disk | `DELETE /_admin/nests/<name>?reclaim=true` | `200` and the reclaim, for a live or suspended mount; `409`, with nothing unmounted, for a mount with no recorded NID |
| Free a dataset unmounted earlier | `DELETE /_admin/datasets/<nid>` | `200`, `409` kept, `404` absent |
| Read the memory ceiling | `GET /_admin/runtime` | `200` and the ceiling, with what each cursor holds |
| Raise or lower it | `PUT /_admin/runtime` `{"max_rss_mb": 1536}` | `200` and the same; `400` above 2048; `409` below what a cursor holds |

`<name>` is the route the nest serves at, `/<name>/…`: `usdc` for the runtime's default tenant, and
`acme/usdc` for any other tenant. Each mount's route depends on its own tenant alone, so it never
changes on a restart, whatever else is mounted. A name is one or two parts, `alias` or `tenant/alias`,
each of letters, digits, `_` and `-` and at most 64 characters; an alias may not end in `__moving`,
which a move reserves, and the default tenant is never spelled out (`usdc`, not `default/usdc`, nor
`acme/usdc` when `[runtime] default_tenant = "acme"`). Neither part may be `health`, `nests`, `metrics`,
`ready` or `_admin`, which are the runtime's own routes; boot refuses those too. Any other name is
refused with `400` before anything is recorded. Nor may a tenant share its name with a default-tenant
alias, since `acme` and `acme/sql` would both serve `/acme/sql`: that depends on what is mounted, so it
is refused when the mount is planned, before it joins, as a refusal below. A NID is read in either case
and kept in lower case, the form `nuthatch nest nid` prints; `mounts.toml` must spell it so.
`/<name>/ready` answers for the nest itself, and `GET /nests` lists the roster.

### Mounts are jobs

A mount can take minutes when it fetches, so it answers `202` at once with a job:

```json
{"name": "usdc", "nid": "9f2c…", "phase": "accepted", "since_unixtime": 1790700000, "generation": 1}
```

`generation` is the claim this job holds on the name, from a counter the runtime never reuses, so a
job that was forgotten cannot mount over a later one. `phase` moves through `accepted`, `fetching`
(only when the registry is needed), `seeding` (only for `"seed": true`), `joining` (catching up beside the cursor before it joins), and
ends at `live` or `failed`, with a `reason` and a `code` on a failure. A suspended mount reads `suspended`. Poll
`GET /_admin/mounts/<name>`; reading it never waits on a mount in progress. Unfinished jobs survive a
restart and resume; failed ones stay readable until the name is mounted again or unmounted.

**`live` means indexing and serving, not caught up.** A mount joining a chain that already has a
cursor catches up beside it before it goes live. The first mount onto a chain starts that chain's
cursor and backfills inside it, so it is `live` while history is still arriving. Read
`/<name>/ready` for the distance: `lag_blocks` is how far behind the tip it is, and `ready` stays
`true` while it catches up, since readiness means serving and advancing.

A second `POST` of the same name and NID is idempotent: `202` with the running job, or `200` once it is
live. The same name with another NID is `409`, whether that name is live, mounting or suspended;
changing a live mount's nest is a move.

`?wait=true` answers only when the mount has finished, with the synchronous statuses below, and a repeat
of a live mount is `200` there too. Use it from a script; a platform should poll.

`code` is for a caller to branch on, where `reason` is for a person to read:

| `code` | Why |
|---|---|
| `not_found` | the registry holds no bundle under that NID, or the mirror holds no dataset for it or lacks a file its catalogue names |
| `access_denied` | the registry refused the credential, or has none for a private bucket (an S3 `401` or `403`) |
| `verification_failed` | the registry served bytes that are not that nest: the bundle computes to another NID, a file's hash or the content address does not match, or the decode registry does not reproduce; or a mirror served a segment that does not hash to its address, or a seed snapshot whose tails do not match their digest |
| `other` | anything else, including every refusal below |

### Seeding a mount from its mirror

`POST /_admin/nests {"name": "usdc", "nid": "<nid>", "seed": true}` fills the dataset from a mirror
before it indexes, which is how a lost runtime is rebuilt without re-indexing every nest over RPC
(RFC-0073 FR16). The mirror is the request's `publish`, else the mount record's `[mounts.publish]`,
else `[runtime.publish]` in `mounts.toml`; with none of them the job fails. `publish` takes the
`[mounts.publish]` keys as JSON, `{"target": "s3://bucket/prefix", "interval": "60s", "parallelism":
2}`, and once the mount is live it is written to the mount's record and the mount publishes there.
Credentials come from the runtime's `AWS_*` environment, never the request.

The seed is `nuthatch seed --from <mirror>` run on the dataset: the same refusals, and every file
checked against its content address. While it runs the job reads `seeding`, with progress:

```json
{"name": "usdc", "phase": "seeding", "seed": true,
 "seed_progress": {"segments": 42, "done": 17, "fetched": 17, "fetched_bytes": 803225344}, …}
```

`done` counts files on disk and checked, and `fetched` those this attempt downloaded. Once every file
is in, the mount joins and follows the chain from the block after the mirror ends.

A refused or interrupted seed ends the job `failed` with its `code`, and keeps the dataset and every
file it checked, so a second `POST` of the same body resumes it and fetches only what is missing. A
restart resumes an unfinished seed by itself. The seed is skipped, and the mount carries on from its
own data, when the dataset has already indexed or another mount already serves it. `"seed": true` with
`?wait=true` is `400`: a seed is read on its job.

### Refusals

| Status | Why |
|---|---|
| `400` | a malformed NID, a name the runtime would not accept, or a tenant that shares a mount's name (see above), an invalid `publish`, or `seed` with `wait` |
| `404` | the runtime does not hold the NID and was started without `--registry` |
| `409` | the name is taken, the chain is not declared, the chain's cursor has died (restart the runtime), the nest declares authored entities and the runtime runs `--seal-direct`, or another mount of that name or dataset is still catching up |
| `507` | the mount would breach the chain cursor's RAM ceiling, the reason carrying projected and ceiling MB, or its authored entities would put the cursor over its cap (32 at 2048 MB, proportionally fewer below) |

On the job route a refusal ends the job `failed` with the same reason. With `--registry`, a NID the
registry does not hold is accepted like any mount and ends the job `failed`, its reason naming the NID;
with `?wait=true` it answers `500` with the same reason. A mount the runtime fetched and
then refused is removed again: a refusal leaves nothing on disk, except after a seed was asked for,
which keeps what it fetched so the next attempt resumes it.

### Per-mount limits

A mount may lower the `/sql` guards for itself (RFC-0073 FR3). `POST /_admin/nests` takes optional
`sql`, `queries` and `limits` beside `name` and `nid`, and `PATCH /_admin/mounts/<name>` changes them
on a live mount:

```json
{"name": "acme/usdc", "nid": "9f2c…", "sql": "open",
 "limits": {"statement_timeout_ms": 5000, "max_rows": 1000, "analytics_memory_bytes": 134217728}}
```

| Field | Process value it may not exceed |
|---|---|
| `statement_timeout_ms` | `NUTHATCH_SQL_TIMEOUT_MS`, 30 s at most |
| `max_rows` | `NUTHATCH_SQL_MAX_ROWS`, 50,000 at most |
| `max_result_bytes` | `NUTHATCH_SQL_MAX_RESULT_BYTES`, 64 MiB at most |
| `max_hot_rows` | `NUTHATCH_SQL_MAX_HOT_ROWS`, 2,000,000 at most |
| `max_hot_bytes` | `NUTHATCH_SQL_MAX_HOT_BYTES`, 64 MiB at most |
| `max_named_scan_bytes` | `NUTHATCH_SQL_MAX_NAMED_SCAN_BYTES`, 512 MiB at most |
| `analytics_memory_bytes` | the analytics pool, `analytics.memory_limit` |
| `max_queued` | `NUTHATCH_SQL_MAX_QUEUED`, 256 at most |

A limit only ever lowers: a value above the process's, or zero, is refused with `400` naming the field
and the ceiling, never clamped. An absent field is the process value. The limits are recorded in the
mount's `[mounts.limits]` in `mounts.toml`, outside the NID: setting them re-indexes nothing and moves
no data, and two mounts of one NID may carry different ones.

`PATCH` takes any of `sql`, `queries` and `limits`. Each one given replaces the record's whole value, so
`{"limits": {}}` returns a mount to the process values. Nothing is remounted or restarted: the next
request runs under the new limits, and a request already in flight finishes under the ones it started
with. A mount still being mounted or moved answers `409` with its job; a name with no mount record
`404`. On `POST`, giving `sql` or `limits` for a name that is already live is `409` unless they match
its record: change a live mount with `PATCH`.

**Analytics memory on a shared dataset.** Two mounts of one NID share the dataset's analytics pool,
sized by `analytics.memory_limit`. A mount's `analytics_memory_bytes` is checked against that shared
pool while the mount's statement runs, every 5 ms, and the statement is stopped with `507` once the
pool holds more. It bounds what the pool may hold while this mount is querying, so a co-tenant's
statements count against it too: a tight cap on one mount can refuse its query because another is
busy. It cannot raise the pool, and a mount with no cap still runs to the pool's own limit.

The other guards are this mount's alone. Its timeout answers `504`, its unsealed-row and byte caps
`503`, and its row and byte caps truncate the answer with `truncated: true`. A mount that lowers
`max_result_bytes` neither reads nor fills the dataset's memo, whose key does not carry the byte cap.

### Dry run

`POST /_admin/nests?dry_run=true` runs the same admission checks a mount does and mounts nothing. If the
NID has to be fetched it is fetched and verified, and stays installed.

```json
{
  "name": "usdc", "nid": "9f2c…", "fetched": true, "shares": null,
  "chain": "mainnet", "start_block": 6082465, "tip": 21000000, "blocks_to_backfill": 14917535,
  "has_data": false, "per_block_rpc": ["[extract] blocks"],
  "incoming_mb": 180, "projected_mb": 420, "ceiling_mb": 2048,
  "refusal": null, "refusal_status": null
}
```

`refusal_status` is the status a real mount would answer; a mount already live on that NID reads
`"mounted": true` with no refusal, as its repeat answers `200`. `tip` is known only for a chain whose
cursor is running; a dry run dials nothing. `per_block_rpc` lists extraction that costs calls per block
beyond the shared `eth_getLogs`, which is what makes a nest expensive to index. `shares` names a mount
that already indexes this dataset, in which case the mount costs nothing further.

### The memory ceiling

`max_rss_mb` is each chain cursor's RAM ceiling, shared by every mount on that chain. `PUT
/_admin/runtime` changes it with no restart and writes it to `mounts.toml` before it takes effect, so
the next boot holds to it; the next mount is admitted against it at once.

```json
{"max_rss_mb": 1536, "max_rss_mb_limit": 2048, "max_entities_per_cursor": 24,
 "cursors": [{"chain": "mainnet", "projected_mb": 420, "entities": 3}]}
```

- **2048 is the most it accepts.** The ≤2 GB budget per cursor is the ceiling, not a default: a
  chain that needs more nests than fit is split into a second runtime, not given a second cursor.
- **A value below what a cursor holds is `409`**, naming the chain. What a cursor holds is the
  admission estimate of its live and joining mounts, `projected_mb` above, not its measured RSS, so a
  lowered ceiling always admits what is already running. It never unmounts anything.
- **The entity cap follows it**: 32 authored entities per cursor at 2048 MB, proportionally fewer
  below, rounded down. A value whose cap is under a cursor's `entities` is `409` too.
- Boot prices each store at its size then, so a ceiling set to exactly `projected_mb` can refuse the
  last mount at the next start once stores have grown. Leave headroom.

### Suspend and resume

A suspended mount leaves its cursor, releases its store and answers `503` with `"suspended": true` in
place of its routes. Its data and record stay, and it stays suspended across a restart
(`mounts.toml` lists it under `suspended`). Resume catches it up from where it stopped. A quarantined
mount is released by suspending it, then resuming it; resume answers `404` for a mount that is not
suspended.

### Moving a name

`POST /_admin/move/<name>` mounts the new NID beside the old one, catches it up, then switches the
name's routes in one step. Its job carries `"is_move": true`. A reader polling `/<name>/` sees the
old nest, then the new, and never an error between. The old nest is then taken off its cursor. A
move keeps its chain.

### Reclaiming disk

Unmounting keeps the dataset, so a remount is free. `?reclaim=true` on the unmount, or
`DELETE /_admin/datasets/<nid>` later, removes it once no mount names it; a dataset another mount still
uses, or a mount still in progress is fetching or joining, is kept and the answer says by whom:

```json
{"outcome": "kept", "nid": "9f2c…", "mounted_by": ["acme/usdc"]}
```

An unmount that fails part-way answers `500` with the `nid`, so the dataset can still be freed by
`DELETE /_admin/datasets/<nid>`.

Inside a running runtime this removes the dataset and the segments only it references. Segments a live
fold left behind stay for an offline `nuthatch prune`.

## Tenant shares

Each chain's cursor has one SQL gate, two permits by default, shared by every mount on that chain, and
the process has one budget of response bytes in flight. Both are divided by tenant label (RFC-0073
FR2). Nuthatch still knows nothing about who a tenant is; the label is the first part of a mount's name.

| Do | Call | Answers |
|---|---|---|
| Set a tenant's share | `PUT /_admin/tenants/<tenant>` `{"weight": 2, "max_permits": 1, "max_response_mib": 32}` | `200`, `{"tenant", "share"}` |
| Restore the default | `PUT /_admin/tenants/<tenant>` `{}` | `200` |
| Read every tenant's share and use | `GET /_admin/tenants` | `{"tenants": [{"tenant", "share", "mounts", "permits_held", "response_kib_in_flight", "hot_kib_in_flight"}, …]}` |

A `PUT` replaces the tenant's whole share, every field optional. It is written to `mounts.toml` under
`[tenants.<tenant>]` first and then applied to every running mount, with no restart; a share the file
could not take is refused and changes nothing. A tenant may be given a share before it mounts anything.
`weight` is 1 to 1000, default 1; `max_permits` caps the permits it holds on any one gate;
`max_response_mib` caps its response bytes in flight across the process. A bad value is `400`, an
unknown field `422`.

**How the gate is divided.** Every tenant with a mount on a gate holds a reservation of one permit. A
tenant may borrow more only while every other tenant's reservation stays free, so a tenant below its
reservation is admitted within the 250 ms admission wait however busy another tenant is: it waits
behind its own queries, never another's. When several tenants wait, a freed permit goes to the one
holding the fewest permits per unit of weight. Half the response-bytes budget is reserved the same way,
split by weight between the tenants mounted; the other half is borrowed first come.

**Hot-tip copies.** A query over the unsealed tip copies those rows for as long as it runs, a memory
apart from its response. Each gate has 128 MiB of them in flight (RFC-0073 FR5), divided between its
tenants exactly as response bytes are, and `max_response_mib` does not cap it. The copy is measured
before a row is parsed and charged to the tenant until the query ends, however it ends, so a tenant
past its share is refused before it copies anything. `hot_kib_in_flight` is what each tenant holds.

**The trade.** A permit already running is never taken back, so a reservation is held whether or not
its tenant is busy. A tenant can use every permit only when it is the only tenant on the gate. With as
many tenants as permits each holds at most one, and with more tenants than permits a tenant without a
permit waits for any reservation to free. Raising `NUTHATCH_SQL_MAX_CONCURRENCY` buys room, within the
per-cursor memory budget its ceiling protects.

**Refusals.** A tenant at its own share, at its cap, or waiting with every reservation taken, is
answered `429` with `"reason": "tenant_share"` and counted as `nuthatch_sql_rejections_total
{reason="tenant_share"}`. A full gate while the tenant is under its share is still `503`, `server busy`,
and `reason="busy"`. A runtime with one tenant behaves as it did before shares existed: it may hold
every permit and is only ever told `busy`. A lone nest outside a runtime has no tenant at all.

A maintained-view build takes a permit as a tenant mounting its dataset, so it never takes another
tenant's reservation.

## Faults

A chain's cursor carries every nest on that chain, and a fault in one nest takes out that nest alone.
`GET /nests` gives each mount a `health` of `indexing`, `quarantined` or `suspended`, and a quarantined
one a `quarantine` object: `kind` (`nest`, or `cursor` when the chain's cursor died), `class`
(`retryable`, backing off from 5 s to 300 s, or `terminal`, until a restart), `reason`, `attempts` and
`next_retry_unixtime`. The root `/ready` answers `503` while any mount is quarantined or stalled and
names each; `/<name>/ready` answers for that mount alone. `--fail-fast` turns every fault below into
the end of the runtime.

| Fault | The faulted mount | Its co-tenants | `/nests`, `/<name>/ready` |
|-------|-------------------|----------------|---------------------------|
| Decode error | The log is skipped, logged at debug. Not a fault. | Unaffected | `indexing`, `200` |
| Entity over `max_rows` | Terminal quarantine; the entity stops. | Keep indexing | `quarantined`, terminal, the entity named; `503` |
| Maintained-view build failure | Not quarantined. The view answers from its definition at request time; `nuthatch_maintained_build_failures_total` counts it. | Unaffected | `indexing`, `200` |
| Query panic | That request answers `500`. Its permit on the cursor's SQL gate is returned. | Keep serving on the same gate | Unchanged |
| Extraction RPC error | A failed shared `getLogs` is retried for the whole cursor, so a dead pool stalls every nest on the chain. A missing header or timestamp refuses that nest's window, retried without advancing. A block over the provider's cap that an address-filtered refetch cannot fit is a terminal quarantine of the factory nests that forced the wide fetch. | Other chains unaffected; on the same chain they stall with it, as extraction is shared | Stalled: `indexing`, `503` naming `poll_stalled`. Over-cap: `quarantined`, terminal. |
| A dead IVM circuit, or a reorg below the sealed watermark | Terminal quarantine. | Keep indexing | `quarantined`, terminal; `503` |
| Any other window error, such as a failed store write | Retryable quarantine with backoff, then re-admitted behind the cursor. | Keep indexing | `quarantined`, retryable; `503` |
| Ingest panic | Terminal quarantine, `reason` `this nest's ingest panicked: <message>`. Covers its window, its `prepare` and its reorg rollback. | Keep indexing; the cursor carries on | `quarantined`, terminal; `503` |
| Build failure at boot | Booted quarantined, `reason` `this nest failed to build: <error>`. Every route answers `503` naming it; its record stays in `mounts.toml`. Unmount drops it; a restart builds it again. | Boot and index | `quarantined`, terminal; `503` |
| Over the ceiling at boot | Booted quarantined, `reason` `refused at boot: <the refusal a mount would answer>`, naming projected and ceiling MB or the entity cap. Mounts are admitted in `mounts.toml` order; one that does not fit takes no room from those after it. Its record stays; unmount drops it, and a restart with room boots it. | Boot and index | `quarantined`, terminal; `503` |

A boot at which no nest builds or fits still fails, naming each.

**What a panicked nest serves.** A window's rows commit in one hot-store transaction, so a panic
leaves the store at a window boundary and `/sql` answers the last committed window, frozen, as for
any quarantined nest. In-memory state the panic interrupted is not trusted again: the nest indexes
nothing until a restart rebuilds it from the store.

**A slow nest holds its co-tenants.** The cursor runs each nest's window in turn, so a nest whose
windows are slow but succeed slows every nest on its chain. There is no per-nest window deadline
yet: abandoning a window part-way can let its commit land after its derived state was dropped, so a
deadline needs the window split into a part that can be abandoned and a commit that cannot.

## Bundles

A caller that never runs the binary checks, publishes and collects bundles here (RFC-0073 FR10). A
bundle is the file `nuthatch nest bundle` writes, sent as the request body with `Content-Type:
application/octet-stream`; any other content type is answered `415`.

| Do | Call | Answers |
|---|---|---|
| Check a bundle and price its mount | `POST /_admin/bundles?dry_run=true` | `200` and the report below |
| Publish it to `--registry` | `POST /_admin/bundles?as=<name>@<version>` | `200`, `{"nid", "blob_hash", "name", "version"}` |
| Read what the registry holds for a NID | `GET /_admin/bundles/<nid>` | `200` with `"held": true` and the inspection, or `404` with `"held": false` |
| Delete what nothing reaches | `POST /_admin/bundles/prune` `{"keep": ["<nid>", …]}` | `200` and what was deleted; `?dry_run=true` lists only |

**Every bundle is checked as `nuthatch nest load` checks it**: its format, every file's hash, the decode
registry reproduced from its inputs, and its files computing to the NID its manifest claims. One that
fails is answered `422` with the reason `nest load` gives, and nothing is written. The check unpacks the
bundle into a temporary directory that is removed before the answer; neither `data/`, `mounts.toml`
nor the registry is touched by a dry run.

```json
{
  "nid": "9f2c…", "data_identity": "41ab…", "blob_hash": "c07e…",
  "inspection": {"schema": "nuthatch.nest-inspect/1", "nid": "9f2c…", …},
  "admission": {"name": "usdc", "nid": "9f2c…", "fetched": false, "shares": null, "chain": "mainnet",
                "per_block_rpc": [], "projected_mb": 420, "ceiling_mb": 2048, "refusal": null, …}
}
```

`inspection` is the document `nuthatch nest inspect <bundle> --json` prints for the same bundle.
`admission` is what `POST /_admin/nests?dry_run=true` answers for its NID, under the bundle's nest name
or `?name=<name>`; it never fetches, since the bundle is in hand.

**Publish** needs `?as=`; `name` alone publishes as version `h<first 12 of the hash>`, as `nest publish`
does. Publishing the same bundle again changes nothing and answers the same. A runtime started without
`--registry` answers `409`, and a registry that refuses this runtime's write (a read-only directory, or
S3 credentials without write) answers `403`, naming the registry. A `POST /_admin/bundles` with neither
`?dry_run=true` nor `?as=` is `400`.

**The size limit** is 64 MiB a bundle, past which the answer is `413`; `--max-bundle-mb` changes it. A
bundle is a nest's config, ABIs and views, so a real one is far smaller.

**Prune** deletes every blob, NID entry and `name@version` ref, `latest` included, that is reachable
neither from a NID in `keep` nor from any mount of this runtime: live, suspended, joining, with a job,
or recorded in `mounts.toml`. A blob is reachable from a NID when that NID's entry names it or its own
manifest computes to it. An invalid NID in `keep` is `400`. The answer lists `deleted_refs`,
`deleted_nids`, `deleted_blobs`, `kept_blobs` and `skipped_blobs`, each skip with why, and `reachable`,
the NIDs the prune kept. A `latest` whose version was pruned is deleted, not moved to an older one.

Prune never leaves a ref naming a missing blob. It deletes refs first and blobs after, re-reading each
ref before deleting it and listing every ref again before each blob; a publish writes its blob before
its refs, and checks its blob is still there after them. A blob younger than 15 minutes is skipped,
because it may be one whose publisher has not written its refs yet; a publisher stalled longer than
that between its blob and its refs finds its blob gone and writes it again. Within one runtime, publishes and
prunes take turns. Against a second runtime or `nest publish` sharing the registry, one window remains:
a publish of a blob that already exists, landing entirely between this prune's last check of that blob
and its delete, a few milliseconds, leaves its refs naming nothing. Publishing again repairs it. A ref
re-pointed in the same instant may be deleted rather than kept; it never dangles.

Publish and prune answer synchronously. Neither dials an RPC, a bundle is bounded by the size limit,
and both are safe to repeat, so a caller whose request timed out sends it again.

## What to meter

The runtime serves `/metrics` at its root, before anything is mounted, with per-nest series labelled `{nest="<name>"}`, among them
`nuthatch_nest_hot_store_bytes`, `nuthatch_nest_sealed_segments_bytes`, `nuthatch_nest_last_block` and
`nuthatch_nest_tip_lag_blocks`. A segment two datasets share counts under both, so for disk used read
the unlabelled `nuthatch_sealed_segments_bytes`. See [operators.md](operators.md#observability).
