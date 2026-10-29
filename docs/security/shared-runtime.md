# Shared runtime: threat model

RFC-0073 FR8. Written 2026-10-27 against the branch that adds `[runtime] isolation = "strict"`.

**Nothing on this page is a guarantee yet.** The isolation it describes is not claimed in the docs,
in release notes or to GraphOps until an independent review, commissioned by GraphOps, has run
against it and its findings have come to us first (privately, as [SECURITY.md](../../SECURITY.md)
asks). Until then this is a description of what the code does and what it is meant to stop, with the
tests that hold it, written so a reviewer can attack it.

## The setting

```toml
[runtime]
name = "shared"
isolation = "strict"     # default "shared"
```

`strict` changes four things, and nothing else:

1. **Sessions, pools and memo entries are per tenant.** Two tenants mounting one dataset (one NID)
   get separate cached SQL sessions, separate engine memory pools and separate memo entries. Under
   `shared` they share all three, which is fine when one operator owns every mount.
2. **A function allow-list.** A statement, and every authored view it reaches, may call only the
   functions in `ALLOWED_FUNCTIONS` in [`src/isolation.rs`](../../src/isolation.rs), plus the
   DuckDB spellings Burrmill rewrites into them (`DIALECT_FUNCTIONS`). Today that is every function
   the engine registers except three, which describe the server rather than compute on a value:
   `version`, `arrow_metadata` and `arrow_field`. A statement the guard cannot parse for the
   functions it calls is refused rather than passed.
3. **Authored views may only be views.** A `views/*.sql` statement that is not `CREATE VIEW` is not
   run. `CREATE TABLE … AS` materialises its rows outside the engine's memory pool.
4. **The boot budget charges a pool per permit.** See [Memory](#memory).

The allow-list is closed against upstream drift both ways. `the_function_lists_name_every_registered_function`
fails the build when Burrmill or DataFusion registers a function the lists do not name (or stops
registering one they do), so a new function is reviewed before it ships. A strict session also
refuses to open, at runtime, over an engine whose registry holds a name neither list carries.

## Who the adversary is

**A tenant.** It controls:

- every statement sent to its own mount: `/sql`, `/explain`, `/table/{name}`, `/entity/{id}`,
  `/diff`, and `/q/{name}` where the operator declared queries;
- the nest it mounts, which is everything a bundle carries: `nuthatch.toml`, ABIs, `views/*.sql`,
  `entities.toml`, `maintained.toml`, `labels/*.json`, `offchain/manifest.json`.

It does not control the host, the binary, the environment, `mounts.toml`, the admin token or any
other tenant's bundle. A caller who can reach `/acme/…` is `acme` as far as nuthatch knows:
authentication and per-tenant authorisation are the gateway's job, as they always were.

**What it wants:** another tenant's rows (sealed segments, hot tip, maintained copies, offchain
snapshots, labels), another tenant's nest configuration (RPC keys and webhook secrets live in
`nuthatch.toml`), any other file the process can read, a write anywhere, or another tenant's
service.

## What is defended, and what holds it

| Path to B | Control | Pinned by |
|---|---|---|
| A file-reading function, by any spelling: `read_parquet`, `"read_parquet"`, `read"_"parquet`, `main.read_parquet`, comment-split | Text deny-list over a comment- and quote-stripped statement; an AST allow-list of three table functions (`generate_series`, `range`, `unnest`); Burrmill's planner, which has no file-reading table function at all | `confinement::a_mount_cannot_reach_another_datasets_files_rows_or_catalogue` |
| A path in table position: `FROM '/…'`, `JOIN '/…'`, `FROM "/…"`, `'file://…'` | The replacement-scan guard; a base table must be named `[A-Za-z0-9_]+`; Burrmill resolves a name only among the session's own relations | same |
| A stacked or non-query statement: `;` then `COPY … TO`, `ATTACH`, `WITH … COPY`, `CREATE EXTERNAL TABLE`, `INSERT`, `SET`, `SHOW`, `EXPLAIN` | Leading-keyword gate, `WITH`-prefixed DML refusal, stacking refusal, Burrmill's single-query parse | same |
| Another dataset's tables, by qualifier: `bbbb.t__e`, `"/…/data/bbbb".t__e`, `datafusion.public.t__e` | A session is opened per dataset and starts empty; a qualifier resolves inside it, so each of these reads A's own `t__e` | same |
| The catalogue: `information_schema`, view text, B's view names | Burrmill builds `information_schema.tables` and `.columns` from the session alone; `views`, `df_settings`, `schemata`, `pg_catalog` do not exist; view text is not exposed | same |
| The parts a registration is built from: `t__e__raw`, `__hot`, `__union` | Refused by name in Burrmill's walk | same |
| A scalar function that reaches outside the query | `input_file_name` and `file_row_index` are not registered; under `strict`, the allow-list | `isolation::tests::*`, `confinement::strict_refuses_off_list_functions_in_statements_and_the_views_they_reach` |
| B's hot rows through a reused session | Sessions are cached per dataset, and per tenant under `strict` | `confinement::a_mount_cannot_reach…` (B's session holds B's hot rows first) |
| An authored path that leaves the nest: `offchain/manifest.json` naming `/…` or `../…` | Fixed in 5.12.1, see below | `confinement::an_offchain_catalogue_cannot_name_a_file_outside_its_nest` |
| Another tenant of the same dataset, through its session, pool or memo | `strict` partitions all three by tenant | `confinement::two_tenants_of_one_dataset_get_separate_sessions_and_pools`, `serve::tests::a_strict_mount_queries_in_its_tenants_session_on_every_sql_route`, `sqlmemo::tests::every_input_changes_the_key`, `e2e_runtime_lifecycle::a_strict_runtimes_live_mount_queries_in_its_tenants_partition` |
| A bundle entry that is a link, a sparse file or a device, or a blob file reached through a link | Extraction admits only regular files and directories; the integrity check refuses a link anywhere on a file's path. See the findings below | `blob::tests::a_bundle_holds_only_regular_files_and_directories` |
| A manifest path with a backslash, a second separator on Windows | Refused at the integrity check | `blob::tests::a_manifest_path_with_a_backslash_is_refused` |
| A statement or view that builds a value larger than its pool | Burrmill refuses a value past 16 MiB and charges outputs from 256 KiB to the pool | `confinement::a_statement_cannot_allocate_far_past_its_pool` |
| A bundle file named like derived state in another case (`Segments/manifest.json`) | Refused at the integrity check, case-insensitively | `blob::tests::a_bundle_cannot_carry_derived_state_in_another_case` |
| An authored view naming a path, a file function, a registration's parts, the catalogue or B's view | Burrmill's planner, the same one a statement meets: view bodies do not pass `/sql`'s text guards | `confinement::an_authored_view_reaches_no_more_than_a_statement` |
| A failed statement's error explanation | Served from what defining the views recorded; no engine is opened outside the request | `confinement::a_served_error_is_explained_without_opening_an_engine` |
| Any write, any read of B's secret, by statements or authored views no one listed | The SQL fuzz target, below | `fuzz/sql`, CI job `fuzz-sql` |

Most of these are older than `strict` and hold under `shared` too; the confinement tests run every
attack in both modes.

## A finding fixed in 5.12.1

**An offchain catalogue could bind any Parquet file the process could read.** `offchain/manifest.json`
is an authored input: it ships in a bundle and is part of the nest's identity. Each snapshot entry
names a `file`, and the session joined it to `offchain/segments/` without checking it. An absolute
path replaces the base when joined, so a bundle whose manifest named
`/srv/runtime/segments/<hash>.parquet` served another dataset's sealed segment as
`offchain__<table>`, and `SELECT * FROM offchain__<table>` returned its rows. A `../` path did the
same. It also answered, by whether the view appeared, whether any path existed. It affected every
mode, `shared` included, and every nest loaded from a bundle someone else wrote.

The fix shipped in 5.12.1 (#2450, #2453): `offchain::load` refuses a snapshot file that is not a plain
name under `offchain/segments/` or that is a link, and the `offchain/` directories may not be links
either, so neither `/sql` nor an entity reading an offchain table can follow one.
Reading another dataset's segment this way needed its path, and so its content hash; any other
Parquet file on the host needed only its path. That bounds it, it does not excuse it.

## Findings from the FR8 pre-review

An internal adversarial pass on 2026-10-28, before the independent review, found these.

**A bundle could copy any file the process can read into the tenant's nest.** Fixed in 5.12.2
(#2463), advisory
[GHSA-mqw2-cc6q-mpw6](https://github.com/nuthatch-org/nuthatch/security/advisories/GHSA-mqw2-cc6q-mpw6).
Bundle extraction used `tar`'s `unpack`, which creates symbolic links with any target; its guard
checks only where the link itself is placed. The integrity check then read each manifest file
through the link. A first bundle with a wrong hash was refused with `blob file … is corrupt:
manifest …, actual <sha256>`, which is the sha256 of any host file, and a missing-file error told
whether a path existed. A second bundle carrying that sha256 passed, and install copied the
target's bytes into the nest as a regular file. A sealed segment's sha256 is its file name, so
another dataset's segment needed no first step: linked into `offchain/segments/` it served that
dataset's rows through `offchain__…`. The same link to `/dev/zero` made the check read until the
process died, and a GNU sparse entry unpacked far more than the bundle held. It was reached by any
install: a mount from the registry, `POST /_admin/bundles` (dry run included) and `nest load`.
Extraction now admits only regular files and directories, the integrity check refuses a link on
any file's path, and a manifest path holding a backslash is refused.

**On a case-insensitive filesystem a bundle could write its dataset's sealed catalogue.** Fixed in
5.12.2, same advisory. `Segments/manifest.json` is not the excluded name `segments`, so it counted
in the identity and installed; macOS's default filesystem then served it as
`segments/manifest.json`, binding any segment in the shared store by its hash. Linux filesystems
are case-sensitive and were not affected. A manifest path naming derived state is now refused,
case-insensitively.

**Explaining a failed query opened an engine outside the request.** Fixed in 5.13.0. When an error
named an authored view, `/sql`, `/explain`, `/q/…`, `/diff` and GraphQL replayed every authored
statement in a fresh engine to find out why: on the async thread, after the SQL permit was
released, with its own memory budget, without the tenant's isolation, and running `CREATE TABLE …
AS`, which strict refuses. An error text that merely quoted `Table with name <view>` asked for it.
It reached nothing of another tenant's, but it was unbounded work outside every limit on this page.
A served error is now explained from what defining the views recorded.

**A scalar function's result was not charged to the engine pool.** Bounded by burrmill#122, pinned
in #2469. One value was as large as its arguments asked for: `repeat('a', 1500000000)` grew peak
RSS by 5.7 GB under a 512 MB pool, `lpad('a', 1000000000)` by 3.8 GB, `array_repeat(1, 100000000)`
by 1.6 GB and `range(100000000)` by 0.8 GB, and nested `replace` grows exponentially with no size
argument at all. Burrmill now holds three bounds. No value that a function, `||` or a literal
repeated per row builds may pass 16 MiB; where an argument chooses the size it is refused from the
arguments before anything is allocated, elsewhere once built, so nesting cannot compound. Every
output of 256 KiB or more is charged to the statement's pool while its bytes live, as are `range`
and `generate_series` in a FROM clause. What remains is the floor: an output under 256 KiB is not
charged, so a statement can hold that much per call above its pool, beside the one value under
16 MiB a function may build before its result is checked. Burrmill's own test allows a statement
24 MiB past its pool for this, and `confinement::a_statement_cannot_allocate_far_past_its_pool`
holds each statement above, and a recursive CTE doubling a string, to the pool plus that slack.

**An authored view's first bytes could panic its definition.** Fixed in 5.13.0, found by
`fuzz/sql`. A multibyte character across byte six panicked the `CREATE` check on every query that
defined the view. It was confined to the request, and to the nest that shipped the file.

### Plausible, not tested

- Strict's allow-list reads `Expr::Function` only. A function reached through method-call or another
  special syntax, if Burrmill's parser accepts one, would not be seen. Only `version`,
  `arrow_metadata` and `arrow_field` are withheld, so little is behind it.
- Process state that grows without a request-side bound: the engine registry in
  `engine_burrmill` gains an entry per session opened and is pruned only when `/metrics` is
  scraped, and `SESSION_OPENS` is never evicted. The held-relation, view-failure and peak maps are
  bounded by mount churn, which the operator drives.
- A hand-laid `data/<nid>/maintained` that is a link is followed when undeclared copies are retired.
  A bundle cannot produce one; an operator can.

## What is not defended

Said plainly, because a shared runtime is a shared machine.

- **Timing.** Tenants share CPU, Burrmill's worker threads, the page cache and the disk. Which of a
  tenant's queries are fast or slow can be inferred from another's. The session cache holds 16
  sessions for the whole process, so one tenant's queries evict another's and the eviction shows
  as a cold start.
- **Availability between tenants.** The SQL permits (2 by default) are per cursor and shared by
  every mount on it; so are the 256 queued requests per nest, the per-dataset memo gate and the
  process-wide 256 MiB of response bytes. A tenant can hold all of them. Fair share between tenants
  is RFC-0073 FR2, not this. A hot tip is materialised per query outside the engine pool (up to
  2,000,000 rows or 64 MiB), as it always was.
- **Data two tenants share by construction.** Two tenants mounting one NID read the same dataset;
  the NID is a content address. `strict` separates their sessions, not their data.
- **The shared segment store.** Sealed segments of every dataset live once, by content hash, in
  `segments/`. A dataset only ever reads the hashes its own catalogue lists, and a seeded catalogue's
  files are verified against their hashes, so reading another's segment needs its bytes already.
  Two findings above broke that premise, each needing only a hash; both are fixed in 5.12.2.
- **Layout disclosure.** An error can name the tenant's own segment paths, which show the runtime's
  directory. It names nothing of another tenant's that the statement did not spell out.
- **Everything outside SQL.** A nest's webhooks, offchain pulls, IPFS gateways and RPC endpoints are
  egress the tenant chose (`nest load` lists them). An HTTPS endpoint on the operator's network is
  reachable from a tenant's offchain pull. Ingest faults are FR6.
- **The engine itself.** A panic in a query is confined to its request; the fuzz target found one on
  its first run (Burrmill `listcomp.rs:231` sliced non-ASCII text after `[` mid-character), fixed
  in burrmill 242d73e. A memory-safety bug in Burrmill, DataFusion or Arrow is not confined, and
  nothing here would contain one.
- **`isolation = "shared"`.** Points 1 to 4 above do not apply. It remains right for one operator's
  own mounts.

## Memory

A strict runtime does not raise the worst case, but it does spend more of it, and the boot check now
charges for that.

- **Engine pools.** Under `shared` a dataset's sessions share one pool, so a cursor's pools are at
  most its datasets and at most its permits, and the boot check charges the smaller. Under `strict`
  each session has its own, and a session runs one statement at a time, so every running statement
  draws its limit and the check charges one pool per permit (`analytics_budget::validate_strict_budget`,
  RFC-0076). The same check runs on every live change of `sql_permits` or `max_rss_mb`. With the
  shipped split (2 permits × 512 MB, 1,024 MB reserved) nothing changes; a configuration that relied
  on a single dataset to run two permits at 768 MB each is refused, because two statements of that
  dataset's tenants are 2 × 768 MB.
  A pool is a limit, not an allocation: it reserves only while a statement runs, and only as many
  statements run at once as there are permits, so a thousand tenants still hold at most permits ×
  pool size, and nothing while they are idle. A pool lives only as long as its session, so tenant
  labels that come and go do not accumulate
  (`confinement::a_hundred_tenants_leave_no_pool_reserved_and_no_pool_without_a_session`).
  Since burrmill#122 the values a statement builds are charged too, from 256 KiB per output, and no
  one value passes 16 MiB, so a statement holds at most its pool plus that slack (Burrmill tests 24
  MiB). Before it, one `repeat` call could grow the process by gigabytes; see the findings above.
- **Cached sessions.** Each holds its view catalogue and up to an eighth of the engine limit in
  Parquet footers (64 MB at the default 512 MB). The cache is 16 sessions for the process while each
  cursor's split leaves nothing over, as at the default, whatever the mode: there `strict` fills the
  same slots with more distinct keys, which costs misses rather than memory. Where an operator has
  given a cursor more than its split charges, the cache keeps one more session for each 65 MB left
  over, the most one may hold, up to 256 (RFC-0076). A cold session costs 0.9 ms over a cached one in
  release on the bench fixture, and an idle one 0.46 MiB and about six threads
  (`docs/bench/rfc-0076-tenants.md`).
- **Memo.** Entries are keyed by tenant, so two tenants asking the same question store it twice.
  The memo's process ceiling (64 MiB, `NUTHATCH_SQL_MEMO_BYTES`) is unchanged.

Admission charges a second mount of a dataset nothing, under either mode: what strict adds is bounded
by the permits and the two process-wide caps above, none of which a mount moves.

## The fuzz target

`fuzz/sql` is its own cargo-fuzz crate (it links the engine; the decode targets in `fuzz/` must not).
`sql_confinement` lays out two datasets as a runtime does, gives B a secret drawn at start-up in its
sealed rows, hot rows, config and view text, and runs fuzzed statements on A, shared and strict. It
fails on an answer or error carrying B's secret that the statement did not spell out, and on any
file under the runtime directory created, removed or changed. Statements are assembled from fragments
of every attack above, appended to statements that already answer, so the fuzzer recombines escapes
rather than discovering `SELECT`. An input may also carry A's `views/zz.sql`, from the same
fragments, since a bundle is the tenant's too. A panic is an outcome rather than a finding, as `/sql` confines one
to its request. Planting the secret in A's own rows makes it fail within seconds, which is the check
that it can see a leak at all.

```sh
cargo fuzz run --fuzz-dir fuzz/sql -s none -O sql_confinement
```

It builds on the pinned stable toolchain with no sanitizer, because the nightly that address
sanitising needs does not compile dbsp. CI runs it as `fuzz-sql`, bounded like `fuzz-smoke`: a
tripwire, not a campaign.

## Reporting

A way through any row of the table above is a vulnerability: report it privately as
[SECURITY.md](../../SECURITY.md) describes, never in a public issue.
