# Launch copy - home turf (The Graph forum + #indexers)

Draft. This is the **Phase 1** post - it goes out *before* Show HN (RFC-0007), to the people who
already run this kind of infrastructure and will give the sharpest feedback. Framed as observability
for the ecosystem, not a pitch. Paulie credited and given a heads-up before it posts.

**Partnership disclosure timing is the operator's call** (RFC-0007 Phase 1): if their platform
announcement lands first, keep the one-sentence hosted-option mention below; if not, delete it and let
the post stand alone.

> **Historical draft, not current copy (noted 2026-10-23).** Written for 2.x and 3.x, never posted,
> and wrong in several places now. SQL runs on Burrmill (DataFusion), not DuckDB, since 4.1. The WASM
> transform layer and its WIT interfaces were removed on 2026-10-08, and the ExEx stub with them; ExEx
> is designed, not built. Authored incremental entities (RFC-0041) shipped in 3.0.0-alpha. The
> 58 MB figure is a dated 2.x measurement. Rewrite it
> against the current README before any use.

---

## Title

> Nuthatch: a single-binary, self-hosted indexer for local observability over your own data

## Body

Sharing something I've been building that's squarely aimed at people in this channel: **nuthatch**, a
self-hosted blockchain indexer that's one Rust binary and one command - no Postgres, no Docker, no
external services. You point it at a contract, it decodes the ABI into queryable tables, and you have a
live SQL + MCP API in under two minutes.

Why post it here first: the thing indexers and data-service operators keep needing is *cheap local
observability* - a way to answer "what did this contract actually do" without standing up a whole
stack or renting a metered API. Nuthatch runs a live 3-contract nest in **~58 MB of RAM** (CI-enforced
≤256 MB, budget is 2 GB), follows the tip reorg-safely, and seals content-addressed Parquet past
finality that DuckDB queries read-only. Balances, exposure and velocity are incremental (DBSP): a
reorg is a retraction, not a recompute. Nest-authored SQL views run at query time.

It ships a **Horizon nest** as the worked example - the Graph Horizon contracts on Arbitrum One,
decoded, with parity checks (`nuthatch check parity`) against the canonical source. That's the demo I'd
most like this audience to kick the tyres on: does the decoded state match what you'd expect, and where
does it drift?

Honest about scope: nine EVM chains built in and any other with `--rpc`; events, declared `eth_call`s
(pinned or one per event row) and verified IPFS documents; traces and state diffs need a node; RPC
polling today (in-process reth ExEx is designed and stubbed). `MIT OR Apache-2.0`, a self-funded public good maintained by one person - not a startup, no token, no phone-home,
and a public "what we'll never build" list.

_(Optional, only if the operator has announced:) An operator in this ecosystem is preparing a hosted
option for teams who'd rather not self-host - same binary, their gateway in front._

Repo, install, and the Horizon nest: https://github.com/nuthatch-org/nuthatch

What I'm after here specifically: **where does the decoded Horizon state drift from what you'd expect,
and what would stop you running this next to your indexer?** Not looking for stars - looking for the
failure you'd hit that I haven't.

---

## r/rust angle (separate day, separate framing)

Lead with the engineering, not the domain. Hooks that play in r/rust:

- **DBSP retractions**: the three built-in circuits (balances, exposure, velocity) treat a reorg as
  a retraction in the same circuit that runs the backfill as a batch. Feldera crates, MIT/Apache.
  Arbitrary authored views are not this; they are query-time SQL (RFC-0041, frozen).
- **Batched Arrow over WIT**: the transform host passes Arrow IPC buffers across the component
  boundary, never one event per call - because a per-event WASM call can't survive a ≥10K ev/s floor.
- **DuckDB single-writer by design**: only the ingestion thread writes; queries attach read-only. The
  determinism proof is the hook - the seal-direct fast path produces byte-identical Parquet to the slow
  path, and there's a test that asserts it. r/rust likes a determinism proof more than a benchmark.
- The honest footprint number and the CI job that enforces it.

Title candidate: *"Nuthatch: one Rust binary that indexes a contract into SQL - reorgs as DBSP
retractions on balances, byte-identical fast/slow paths"*.
