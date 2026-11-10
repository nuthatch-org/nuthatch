# RFC-0045: Offchain data - what Dune actually does, and what nuthatch has already answered twice

- Status: **Implemented: stages 1 and 2.** The file drop (#1215) shipped 2026-09-09; the scheduled
  pull, `nuthatch offchain pull` (#1216, #1448), shipped 2026-09-23. Stage 3 (§8) is deferred and
  unfiled; not reopened on 2026-11-10 (Chief). Tracker #1206 is closed.
- Author: Jenny
- Date: 2026-08-29
- Origin: a research report on offchain ingestion (Dune, Goldsky, The Graph, Ponder, Allium,
  Flipside, DataFusion, x402) delivered to the board on 2026-08-29, read against our own tree rather
  than accepted at face value. §9 records where the report and the tree disagree.
- Depends on: RFC-0013 §3 (sealed content-addressed segments, the storage this lands in), RFC-0037
  (IPFS content resolution - the offchain side table we already shipped), RFC-0008 (the compliance
  pack, whose list snapshots are the other one), RFC-0018 §1 (authored SQL over hot ∪ sealed),
  RFC-0034 (the bounded query surface this must not widen), RFC-0041 (whether an offchain table can
  be a DBSP input), RFC-0042 (which engine federates it, and the role inventory this belongs in).
- Blocks: nothing. It did not unblock RFC-0042, and RFC-0042 has since closed: **KEEP DuckDB**
  (§14, 2026-08-30). Offchain federation, if it ever ships, is a DuckDB role. The DataFusion
  `TableProvider` path waits on a §14 reopen.

## §0 - Why write this down at all

Every indexer eventually meets the same request: *join this chain data against a price, a label, a
CSV somebody's analyst maintains.* Dune answers it with uploads and a curated `prices` schema, The
Graph with file data sources, Goldsky with offchain sources and HTTP transforms, Ponder with a
`fetch()` in the handler. The request is real and the peers all serve it.

The reason to write it down now, under a freeze, is that the question has a wrong answer that looks
attractive: bolt on a general "external data" mechanism, and discover afterwards that it has taken
the determinism guarantee with it. Nuthatch's defining property is that a nest can be re-derived from
the chain. Offchain data cannot be. That is not a detail to solve later; it is the whole design
constraint, and it is the thing every peer has either fudged or admitted to.

> **Offchain data can be made auditable. It cannot be made re-derivable. Any design that blurs those
> two is the wrong design, and the boundary belongs in the storage layout, not in the documentation.**

## §1 - What is already true

The report treats this as a greenfield capability. It is not. Nuthatch has shipped the offchain
pattern twice, and both times chose the same shape.

**Sanctions list snapshots (RFC-0008, `src/lists.rs`).** A list snapshot is a set of addresses
written as `lists/<sha256>.json`, with a provenance index at `lists.manifest.json` recording which
lists were fetched, when, and from where. The module's own doc comment states the rule this RFC would
generalise:

> Fetching is **host-side and out-of-band** - never in the data path, never a phone-home during
> indexing.

And the audit trail it produces is a triple: `(list-snapshot hash, block range, component hash)`. The
operator owns the list's provenance; nuthatch owns making the exact set it used reproducible. That is
precisely the "content-hash plus source plus fetch time plus module version" the report recommends,
already built, already shipped, already load-bearing in `src/screen.rs` and `src/audit.rs`.

**IPFS content resolution (RFC-0037, shipped v2.6.0).** A verified, content-addressed side table:
resolved documents spill into sealed segments, the multihash is recomputed over the bytes a gateway
returns, and the CID that was already a column becomes a join key. RFC-0037 §2 is explicitly about
why IPFS is *not* an HTTP enricher, and the answer is content-addressing: the same bytes come back or
the fetch fails.

So the house answer to offchain data already exists in two instances. **The proposal in this RFC
should be to generalise those, not to import Dune's.** A third bespoke provenance format would be the
failure mode, not the feature.

## §2 - What Dune actually does

Worth recording accurately, because the two layers are constantly conflated and only one of them is a
product surface we would be copying.

**Layer one: user-supplied data, two surfaces.**

| | CSV upload (UI/API) | Table API (`/v1/uploads/*`) |
|---|---|---|
| Schema | inferred | declared explicitly |
| Size | 200 MB cap | no documented cap |
| Update | full replace | incremental append (`insert`), `clear`, `delete` |
| Namespace | `dune.<team>.<dataset>` (legacy: `dune_upload.<table>`) | operator-chosen namespace |
| Cost | free | 10 credits per table creation |
| Visibility | public by default; private needs Enterprise | same |

The legacy `/v1/table/*` endpoints are deprecated with removal dated 2026-03-01; request and response
bodies are unchanged. Column names may not begin with a digit or special character, and timestamps
are the fragile case (convert to ISO before upload). Combining several uploads is done with the
"query a query" feature and a `UNION ALL`.

The design lesson is one sentence: **CSV upload is quick, inflexible and replace-only; the
programmatic API is schema-explicit, incremental and append-only.** Both are only ways to get an
external table in front of the same SQL engine.

**Layer two: curated datasets, pipelined out of band.** `prices` is the exemplar and the most-joined
offchain table in the ecosystem. It is a hybrid: Coinpaprika for roughly 2,000 major tokens defined
in token lists, DEX-derived prices from `dex.trades` for the long tail. The overview page claims
around 900,000 tokens across 70-plus chains at roughly 30-minute refresh, while `prices.day`
specifically updates once daily at 00:00 UTC with the previous day's close. Outlier filtering
(VWMP/MAD, a $10k volume threshold), volume-weighted averaging, and forward-fill caps of 30d/7d/2d by
granularity. Every row carries a `source` column, `'coinpaprika'` or `'dex.trades'`.

The ingestion mechanism is the interesting part and it is stated plainly in the spellbook prices
README: the community edits a dbt model listing which tokens to price, and *"this spell then tells a
background pipeline to go obtain pricing data."* The fetched output is treated as a **source** in
Spellbook, because a background service ingests it. Backfills can take days.

Community datasets (Farcaster via Neynar, Snapshot, Reservoir, Flashbots) are third parties streaming
into Dune, distinct from dbt-modeled spells. Lens ingestion was discontinued in December 2025;
historical data only.

**The stack, secondhand and flagged as such in §10:** DuneSQL is a Trino fork with custom plugins
(native UINT256/INT256, Spark views), reached via PostgreSQL then Spark/Databricks then Trino.
Storage is Parquet under Delta Lake on S3. Transformation was dbt from 2020 and is migrating to
SQLMesh, with Dune's CTO quoted saying internal token-metadata APIs are now pulled directly into the
lake by Python-based SQLMesh models. Partitioning is by `block_time`/`block_date`/`block_number` with
no traditional indexes; min/max column statistics drive file and row-group skipping.

**The architectural takeaway, and it is the load-bearing one:** onchain and offchain tables in Dune
meet **only in the SQL query**. There is no offchain-aware join engine. The engine sees tables. That
is exactly DuckDB's federation model, and it means the query half of this RFC costs approximately
nothing.

## §3 - What the peers do

**The Graph** is the richest source of lessons, all of them cautionary. File data sources fetch from
IPFS or Arweave via templates spawned at runtime. They are isolated: they cannot read or write
chain-based entities, only file-specific ones. Per Graph Node's implementation docs they *"currently
can only exist as dynamic data sources, instantiated from templates. They cannot be configured as
static data sources in the manifest,"* and *"some parts of the implementation assume offchain data
sources are 'one shot' - only a single trigger is handled per data source instance."*

And the determinism question is openly unsolved:

> Entities from offchain data sources do not currently influence the PoI. Causality region IDs are
> not deterministic.

with stated impact that offchain data cannot be verified through PoI, may affect dispute resolution,
and limits trustless verification guarantees. Subgraphs otherwise forbid arbitrary external calls in
handlers precisely to preserve determinism; offchain data sources are the sanctioned escape hatch,
and the escape is real.

**Goldsky Mirror** is a serverless `sources → transforms → sinks` pipeline in YAML. Sources include
subgraph entities, datasets, and offchain sources (Kafka, other databases), unified into one schema
with onchain data. Two transform types: SQL, and **external HTTP handler transforms** (their own
example is fetching token prices from Coingecko). Sinks include S3 with native Parquet and
`partition_columns`. Reorgs are handled with changelog/stream semantics.

**Ponder** does offchain fetches inline in TypeScript handlers. `context.client` caches RPC responses
in the database so repeat calls are deterministic-ish; developers also `fetch()` arbitrary APIs and
write with `onConflictDoUpdate`. No determinism guarantee is offered. It is an app-backend framework
and honest about it.

**Allium and Flipside** are warehouse-native: ingest to Snowflake or BigQuery, and offchain joins
happen in the customer's warehouse, not the indexer. Worth noting dYdX's split, which maps onto ours:
**Ender** for onchain ingestion into Postgres, **Vulcan** for offchain ingestion into Redis. Different
stores for different mutability profiles is the same instinct as our redb/Parquet split.

## §4 - Where the report's mapping is right

Three of its conclusions hold up against the tree and are worth adopting as stated.

**Offchain tables are cold-only.** They have no tip and no reorg semantics, so they have no business
in redb. A dropped file or a fetched batch becomes a sealed Parquet segment and nothing else. This
keeps the mutable-tip machinery exclusively about chain data and makes offchain reproducibility a
question of segment retention rather than reorg replay. Correct, and it is what RFC-0037 already does
with resolved documents.

**A separate namespace.** Offchain tables live under their own prefix, mirroring Dune's separation of
`dune.*` from onchain schemas. This is not cosmetic: the namespace is where the "outside the
reindex-from-chain guarantee" boundary becomes visible to a reader of a query, rather than a claim in
a document nobody opens.

**Append-only is what makes a source a DBSP input.** A feed that appends timestamped segments is a
well-behaved RFC-0041 input; a table replaced wholesale is a full-input change and forces
recomputation. If the connector path is ever built it should be append-oriented for exactly this
reason, and the file-drop path should be honest about being a snapshot source.

## §5 - Where it is wrong, and the corrections matter

**`httpfs` is not available, and must not be.** The report offers "query remote Parquet without local
ingestion" as a lightweight escape hatch. Our DuckDB is `features = ["bundled", "parquet", "json"]`;
httpfs is absent. That absence is not an oversight to correct, it is a control. `src/analytics.rs`
opens with `enable_external_access=false`, rejects file access and replacement scans, and carries a
parser-derived AST allowlist whose own comment names this class of risk:

> extension-gated readers. Inert today only because those extensions are not in the bundled build -
> i.e. safe by build configuration rather than by policy. Bundling one, or DuckDB promoting one to
> core, would turn each into a live file read with no change on our side.

Enabling httpfs would make every `/sql` query a potential outbound fetch, which is an SSRF surface on
a public endpoint, and it would widen the bounded surface RFC-0034 exists to bound. **Any offchain
design that requires httpfs is rejected on those grounds alone.** Fetching happens host-side and out
of band, as `lists.rs` already does it, and the query engine only ever sees local sealed segments.

**Nuthatch has no x402 integration.** The report's Stage 3 rests on "Nuthatch already has x402
integration and an MCP server." The MCP server is real. x402 appears exactly once in this repository,
in RFC-0011, and it refers to **Lodestar's** pay-per-query feature in a Next.js app that is not this
product. There is no x402 code in the tree. Paid feeds are therefore not an enhancement to a connector
path, they are a second unbuilt capability stacked on the first, and Stage 3 should be read
accordingly.

**Fetching cannot go through the WASM transform runtime as described.** The report suggests using the
WASM runtime as the fetch-and-transform layer, citing Goldsky's HTTP handler transforms. Our purity
rule forbids exactly that shape. A component granted zero capabilities is deterministic by
construction and may feed canonical entities; an effectful component that can reach the network
produces **annotations only, never canonical entities**, and that is enforced by the absence of the
capability rather than by convention. Beyond the rule, the code is not there: `src/effectful.rs` has
no production caller at all, its `Grants` struct carries an `http_hosts` field its own comment admits
is declared ahead of the linker work, and outbound HTTP is deferred to C5 which is unbuilt.

So the fetch and the transform are two jobs, not one: a host-side out-of-band fetcher in the shape of
`lists.rs`, and then, if any reshaping is wanted, a **pure** zero-capability component over the
fetched bytes. That split is the liminal design working as intended, and it is better than Goldsky's
because it keeps the deterministic half deterministic.

**Dune's upload surface is a solution to a problem we do not have.** Uploads exist because Dune is a
shared multi-tenant warehouse where a user has no other way to put a file where the engine can see it.
A nuthatch operator has a filesystem and owns the process. The file-drop path is therefore much
smaller than Dune's: seal this file into a namespace with provenance recorded. No 200 MB cap, no
credits, no public-by-default, no upload endpoint. Copying the API shape would be copying the
constraint that produced it.

## §6 - The determinism boundary, stated once

This is the part that has to be right, and the peers give us a clean spectrum to place ourselves on.
The Graph content-addresses its inputs and still excludes offchain entities from PoI. Dune does not
attempt determinism and instead stamps every price row with its `source`. Ponder offers nothing.

Nuthatch's position should be the one it already takes for sanctions lists, generalised:

**Two guarantees, named separately and never conflated.**

1. **Re-derivable from chain.** Onchain tables. Delete the store, re-run, get the same bytes. This is
   the product's spine and offchain data must not be allowed anywhere near it.
2. **Reproducible by snapshot.** Offchain tables. Not re-derivable, because the source may be gone or
   changed. But *auditable*: every segment records the content hash of the raw fetched bytes, the
   source URI, the fetch timestamp, and the version of whatever fetched or transformed it. Given the
   snapshot, the result is re-checkable by anyone.

The second is exactly the `(list-snapshot hash, block range, component hash)` triple that screening
already produces, and `lists.manifest.json` is already the file that holds it. Generalising that
manifest to cover any offchain segment is a smaller job than inventing a provenance format, and it has
the advantage of having survived contact with an audit.

The boundary must be structural: separate namespace, separate manifest, and a documented statement
that offchain tables sit outside the reindex-from-chain guarantee. A nest that contains offchain
segments should be able to say so without anyone reading its config.

## §7 - Sequencing against RFC-0041 and RFC-0042

RFC-0041 shipped 2026-08-28, so the DBSP input question has an answer available rather than a
dependency: an append-only offchain feed is a valid input, a wholesale-replaced snapshot is not, and
§4 records that.

**Amended 2026-09-02.** RFC-0042 closed on 2026-08-30: **KEEP DuckDB**, parked to 2027-09-01 with
four reopen triggers. The engine question this section was waiting on has an answer. Offchain
tables, if they ever ship, federate as local sealed Parquet through DuckDB. The DataFusion
`TableProvider` / `ListingTable` path recorded below is what a §14 reopen would use; it is not
the path now.

The prospective fifth DuckDB role (federating an external namespace, alongside parser, incremental
reference, restart seed, and entity serving) is therefore DuckDB's to keep. The 2026-08-29
operational ask, to note it in #935's inventory while that inventory was open, is spent with the
decision. It does not become a slice of work.

The source report was delivered again on 2026-09-02. It is the same document this RFC was read
against on 2026-08-29, not a new RFC. Numbering it a second time would split the argument. The
corrections in §5 still hold: httpfs is refused, fetching is not a WASM capability, x402 is not
in this tree (the counter design is now RFC-0046, still unbuilt).

For the record, the Rust-native path remains well trodden if RFC-0042 reopens. DataFusion's
extension point is the `TableProvider` trait with `SchemaProvider`/`CatalogProvider`;
`ListingTable` covers the file-drop case across Parquet, CSV, JSON and Avro with Hive
partitioning and metadata caching, `StreamingTable` covers unbounded inputs, and
`CREATE EXTERNAL TABLE ... STORED AS PARQUET LOCATION` registers external data from SQL. The
`datafusion-table-providers` crate ships working providers for Postgres, MySQL, SQLite, DuckDB,
Flight SQL and ODBC. That reduces reopen risk. It is not permission to start.

## §8 - The proposal, if the freeze ever lifts for it

Staged, smallest first, each stage standing alone.

**Stage 1 - file drop.** A local CSV, Parquet or JSON file is validated, schema inferred or asserted,
converted to a sealed Parquet segment in an offchain namespace, and recorded in a generalised
provenance manifest with content hash, source path, ingest timestamp and tool version. Queried through
existing federation. No new always-on machinery, no endpoint, no daemon. **Acceptance:** an operator
drops a file and joins it against chain data in one query, with provenance visible in the result, and
the nest still reindexes from chain with the offchain namespace absent.

**Stage 2 - one pull connector, for prices.** A scheduled host-side fetch appending timestamped
segments, with the fetch out of band and behind the cursor, never inline in the data path. Prices
first because it is the most-joined offchain table in the ecosystem. Append-only so it is a valid
RFC-0041 input. If any reshaping is needed it goes through a pure zero-capability component, not an
effectful one. **Acceptance:** a price segment refreshes on schedule, feeds an incremental entity
correctly against the DuckDB oracle, and a network outage degrades to stale-and-labelled rather than
wrong. Note RFC-0037's outstanding limit as the precedent to avoid repeating: its resolution still
runs inline under a 64-fetch budget rather than out of band, and that is the remaining work there.

**Stage 3 - deferred, and larger than it looks.** Push/webhook ingestion, and paid feeds. The latter
is RFC-0046 (accepted in principle, unbuilt) stacked on stages 1 and 2, not an enhancement to a
connector that exists. Neither should be considered until stages 1 and 2 have proved the namespace
and provenance model.

**Thresholds that would change the plan.** If RFC-0042 reopens under a §14 trigger and DuckDB goes,
stage 1 ships as a `ListingTable`/`TableProvider` from the first commit rather than being ported
later. If offchain datasets routinely exceed a single node, adopt a Hive-partitioned layout before
adding connectors. If verifiability ever becomes a product requirement, escalate provenance from
journaled to content-addressed and signed, following The Graph's PoI argument rather than Dune's
stamp-the-source.

## §9 - What this RFC does not do

It does not start work. It is not a third carve-out and does not ask to become one; per CLAUDE.md a
carve-out is a decision Chief records in that file, and an approved RFC is not one until it appears
there. RFC-0042 is closed; this RFC does not reopen it. It proposes no change to the query surface,
and specifically proposes **not** enabling httpfs, now or later.

The 2026-08-29 operational output (note the fifth DuckDB role in #935) is spent. There is no new
operational output on 2026-09-02.

## §10 - Provenance of the claims in this document

Held to the same standard the document asks of the data.

**Verified against this tree** (2026-08-29, at `5b5636a`): the absence of x402 (one match, RFC-0011,
referring to Lodestar); the DuckDB feature set and absence of httpfs; the analytics allowlist and its
extension-gated-reader comment; `lists.rs`'s out-of-band fetch rule and `lists.manifest.json`;
RFC-0037's shipped status and its remaining inline-fetch limit; `effectful.rs` having no production
caller and its unwired `http_hosts`; RFC-0042's carve-out state as recorded in CLAUDE.md.

**From the source report, primary-sourced but not independently checked by me:** Dune's upload limits,
namespaces, endpoint migration and deprecation date; the prices methodology, filters and refresh
figures; Graph Node's offchain-data-source constraints and PoI quotations; Goldsky's source/transform/
sink surface; DataFusion's provider traits and the contrib crate.

**Secondhand, treat as directional:** Dune's internal stack. The Trino fork and S3 scale numbers come
from a July 2023 Trino Fest talk; the Delta Lake on S3 storage claim from an undated architecture doc;
the dbt to SQLMesh migration and the Python-models-pull-APIs detail from a vendor case study quoting
Dune's CTO rather than a Dune engineering post.

**Unverified and flagged as such:** Farcaster/Neynar sync cadence, where community sources say both
"every 12 hours" and "once a day" and no primary Dune statement was found; Snapshot's ingestion
tooling. The old CSV write-API documentation is a beta artifact and should not be treated as current.

**Quoted carefully because the number does not mean what it looks like:** x402 adoption. The July 2026
Visa-Artemis report gives roughly $15 million in adjusted volume across 109.6 million transactions
since launch in May 2025, and Chainalysis cautions that this measures protocol traffic rather than
proven agentic commerce, much of the late-2025 surge coming from PING meme coin activity rather than
agents buying services. Not evidence of demand for paid data feeds.
