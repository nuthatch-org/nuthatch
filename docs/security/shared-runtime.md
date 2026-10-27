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
| Any write, any read of B's secret, by statements no one listed | The SQL fuzz target, below | `fuzz/sql`, CI job `fuzz-sql` |

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
- **Layout disclosure.** An error can name the tenant's own segment paths, which show the runtime's
  directory. It names nothing of another tenant's that the statement did not spell out.
- **Everything outside SQL.** A nest's webhooks, offchain pulls, IPFS gateways and RPC endpoints are
  egress the tenant chose (`nest load` lists them). An HTTPS endpoint on the operator's network is
  reachable from a tenant's offchain pull. Ingest faults are FR6.
- **The engine itself.** A panic in a query is confined to its request; the fuzz target found one on
  its first run (Burrmill `listcomp.rs:231` slices non-ASCII text after `[` mid-character), which is
  Burrmill's to fix. A memory-safety bug in Burrmill, DataFusion or Arrow is not confined, and
  nothing here would contain one.
- **`isolation = "shared"`.** Points 1 to 4 above do not apply. It remains right for one operator's
  own mounts.

## Memory

A strict runtime does not raise the worst case, but it does spend more of it, and the boot check now
charges for that.

- **Engine pools.** Under `shared` a dataset's sessions share one pool, so a cursor's pools are at
  most its datasets and at most its permits, and the boot check charges the smaller. Under `strict`
  each tenant of a dataset has its own, so the check charges one pool per permit
  (`analytics_budget::validate_strict_budget`). With the shipped split (2 permits × 512 MB, 1,024 MB
  reserved) nothing changes; a configuration that relied on a single dataset to run two permits at
  768 MB each is refused at boot, because two tenants of that dataset are 2 × 768 MB.
- **Cached sessions.** Each holds its view catalogue and up to an eighth of the engine limit in
  Parquet footers (64 MB at the default 512 MB). The cache is 16 sessions for the process whatever
  the mode, so `strict` adds no slots: it fills them with more distinct keys, which costs misses and
  rebuild latency rather than memory. The worst case, 16 × 64 MB, is the same as today.
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
rather than discovering `SELECT`. A panic is an outcome rather than a finding, as `/sql` confines one
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
