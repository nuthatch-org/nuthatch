# Architecture, the transform layer and entity derivation (full text)

Moved verbatim from `CLAUDE.md` on 2026-11-19 to keep the always-loaded standing brief short. Nothing was reworded or dropped. `CLAUDE.md` links here; the non-negotiables and the rules summarised there still bind.

## Architecture (two modes, one codebase)

**Embedded mode (default):** single process. Ingestion (RPC extraction with aggressive
batching, Cryo-style; optional reth ExEx when colocated with a node) → deterministic decode →
hot tip store (redb) for entity point-reads → sealed content-addressed Parquet segments past
finality → Burrmill (DataFusion) reading the segments **read-only** for analytical SQL, since 4.1
(DuckDB until then). Only the ingestion thread writes segments; queries only read them. Never
design around concurrent segment writers.

**Scaled mode (docker-compose):** same crates, Postgres replaces redb for the hot store,
the same Burrmill engine serves hot + cold behind one SQL surface. Feature-flag the storage backend
behind a trait; no `#[cfg]` forks of business logic.

**Multi-nest tenancy (in the runtime):** one runtime hosts **N nests**, across **one or more
chains**, running **one isolated cursor per distinct chain** (RFC-0021) - each cursor with its
own finality view and reorg boundary. A single nest is simply N=1; there is no separate mode to
opt into and no container to declare.

**A nest's data is keyed by its content address (NID); a mount is keyed by (tenant, NID).** A
*tenant* is an **opaque string** that labels a mount and never touches the data layer - it exists
so one operator can host nests on behalf of several parties. Two tenants mounting the same nest
share one dataset: it is **never indexed twice**, and deleting one mount decrements a reference
rather than destroying data someone else is using. Because the NID is a true content address, any
edit yields a different nest, so divergence forks its own data automatically and cannot
contaminate a shared one.

Strict per-nest **and per-cursor** isolation of storage, reorg, and blast radius: one nest's bad
view or runaway factory, or one chain's stall or reorg, must not harm another. The single-cursor
law holds **per chain**: a cursor is always single-chain, single-writer, one observable failure
boundary - never multiplex two chains behind one cursor. Multichain in one runtime is a
**capability, not a mandate**; one chain per runtime stays valid and is the default. A second
chain means a second cursor - in the same runtime or on another worker (the distributed pool,
RFC-0022) - but never a second chain behind one cursor. See RFC-0012, RFC-0021.

> **Status (2026-08-13): shipped, verified against v2.2.0.** The roost is retired: there is no
> `roost` subcommand, and `nuthatch dev --dir <dir>` runs one nest or many depending on what the
> directory holds - a `nuthatch.toml` or a `mounts.toml` (RFC-0032). Data lives at `data/<nid>/`,
> a mount record carries `tenant`, `alias` and `nid`, two mounts may share one nid, and `nuthatch
> prune` is what reclaims a dataset nothing mounts any more - unmounting one of two mounts leaves
> the data alone. `nuthatch migrate` moves a pre-2.0 directory across. Tenants may
> now be described as shipped, with the caveat the paragraph above already states: an opaque label
> nuthatch refcounts and knows nothing else about. `docs/rfcs/0032` is the 2.0 shape; `0012` +
> `0021` + `0027` are how it got here.

**Reorg strategy:** reorgs only ever touch the mutable hot store - and only that of the
affected chain's cursor, isolated from other cursors in the same runtime. Segments are sealed to
Parquet strictly past finality, so the columnar layer is append-only and immutable. If a
change requires mutating sealed segments, the design is wrong - go back.

**Entity derivation.**
- Built-in IVM (shipped): `balances`, `exposure`, and `velocity`, maintained by DBSP. Reorgs
  are retractions; backfills are batch runs of the same circuit.
- Authored SQL (shipped, RFC-0018 §1): `views/*.sql` are named queries evaluated at request
  time over hot ∪ sealed. Not incremental.
- Authored incremental entities: [RFC-0041](docs/rfcs/0041-authored-incremental-entities.md),
  **shipped 2026-08-28** in 3.0.0-alpha, off the back of GraphOps feedback that a view recomputed on
  every query gives the caller a name but no query-performance benefit. It was built under the 2026
  feature freeze as the first of five carve-outs, and the ordered sequence in §9 is complete (#818,
  #820, #821, #822). The freeze itself ended on 2026-09-08; see the build-order status below.
  An entity is declared in `entities.toml`, maintained by DBSP as blocks arrive, served from
  `/derived` and by name from `/sql`. Slice 3's criteria were measured against a copy of the real
  Lodestar nest: the panel it replaces went p50 2.15 s to 87.7 ms, and one block's update is flat at
  ~285 µs against 309,548 groups. RFC-0033's durable grafting (#357) is **not** in v1 - per-entity
  reuse across an NID change is still a whole-nest local rebuild.
- Imperative (escape hatch): **removed 2026-10-08 by Chief.** There is no WASM handler layer;
  derivation is the built-in IVM, `views/*.sql` and `entities.toml`.

## The transform layer: removed 2026-10-08

Chief removed the WASM transform layer outright on 2026-10-08 (#1999): `transform`, `screen` and
`effectful`, `wit/`, `components/`, live `[screening]` and the wasmtime dependencies. Do not rebuild
it or write WASM components; reopening it is Chief's call. A nest that still declares `[screening]`
is refused at load, by name. One rule from it outlives it: optional integrations warn-and-skip when
unconfigured.
