# Nuthatch - CLAUDE.md

Nuthatch is a self-hosted-first, AI-native blockchain indexer. One Rust binary, one command,
live indexed API in under two minutes. No mandatory third-party data dependency, ever.
Tagline: "be your own indexer."

This file is the standing brief. Read it before any task. When a task conflicts with the
non-negotiables below, stop and flag it instead of proceeding.

## Non-negotiables

1. **Single static binary** is the primary deliverable. Embedded mode must run with zero
   external services: no Postgres, no Docker, no IPFS. `curl | sh` → `nuthatch init 0xAddr
   --chain mainnet` → `nuthatch dev` → live API. Target: <2 minutes to first indexed query.
2. **Footprint budget: ≤2 GB RAM per active-chain cursor** - one chain's tip-following +
   serving in embedded mode, whether that cursor hosts one nest or several. A single-chain
   runtime is one cursor (≤2 GB); a multichain runtime's total is Σ cursors (RFC-0021). The budget
   is per-cursor and shared across the nests on that cursor - density is RAM-bounded, not free.
   Treat this as a CI-enforced budget (per cursor), not an aspiration. If a design decision
   threatens it, surface the tradeoff before implementing.
   *(Amended 2026-10-28 by Chief: the 2 GB figure is the budget nuthatch's own design must fit per cursor at the default ceiling, gated in CI. An operator may give a cursor a higher `max_rss_mb` by choice, live or in `mounts.toml`; nothing in nuthatch requires it, and the default stays 2048.)*
3. **No phone-home.** The binary makes no network call about the operator that the operator did
   not explicitly turn on. No telemetry by default and none required; no mandatory API tokens; no
   gated data services. The one permitted exception is the opt-in head count of RFC-0054, which is
   off until a person answers yes, carries no identifier, and must pass RFC-0046 §1's deletion test
   at every release: remove it from the tree and a self-hoster loses nothing. *(Amended 2026-10-08
   by Chief, from "No telemetry, no mandatory API tokens, no gated data services.")* AI
   features use local models (Ollama) or BYO API key, and degrade gracefully offline.
4. **Determinism in the core.** ABI decoding, reorg handling, entity derivation, and anything
   feeding stored state must be deterministic and re-executable. LLMs generate code and tests;
   LLM output never sits in the runtime data path.
5. **`MIT OR Apache-2.0`** for the core - the maximally permissive option, and the Rust-ecosystem
   norm. Anyone may use, modify, embed or resell it, including in closed products. *(Relicensed from
   AGPL-3.0 on 2026-07-28. This was a deliberate trade: copyleft was the only thing preventing a
   hosted competitor from closing and reselling nuthatch, and that protection was given up in
   exchange for maximal adoption and zero friction for embedders.)*
   **The dependency rule is now stricter, not looser:** we can no longer consume GPL/AGPL code at
   all, so do not vendor or port from copyleft projects we don't own (notably SQD's worker-rs) -
   read for ideas only. Safe dependencies: reth (MIT/Apache), Cryo (permissive), Feldera/DBSP
   (MIT OR Apache-2.0), DataFusion/Arrow/DuckDB (Apache-2.0). Do NOT add Materialize (BSL) or any
   Envio/HyperSync dependency. `deny.toml` enforces this in CI.

## Architecture (summary; full text in [docs/agent-brief/architecture.md](docs/agent-brief/architecture.md))

- **Embedded mode (default):** single process. RPC ingestion (aggressive batching; optional reth ExEx
  when colocated) → deterministic decode → hot tip store (redb) → sealed content-addressed Parquet
  segments past finality → Burrmill (DataFusion) reads segments **read-only** for analytical SQL
  (DuckDB before 4.1). Only the ingestion thread writes segments. Never design around concurrent
  segment writers.
- **Scaled mode (docker-compose):** same crates, Postgres replaces redb for the hot store. Feature-flag
  the storage backend behind a trait; no `#[cfg]` forks of business logic.
- **Multi-nest tenancy:** one runtime hosts N nests across one or more chains, with **one isolated
  cursor per distinct chain** (RFC-0021). A cursor is always single-chain, single-writer, one failure
  boundary: never multiplex two chains behind one cursor. Multichain is a capability, not a mandate.
  A nest's data is keyed by its NID (content address), a mount by (tenant, NID). A tenant is an opaque
  string that never touches the data layer; two mounts of one NID share one dataset (never indexed
  twice). Strict per-nest and per-cursor isolation of storage, reorg and blast radius. See RFC-0012,
  RFC-0021, RFC-0032.
- **Reorg strategy:** reorgs only touch the mutable hot store of the affected chain's cursor. Segments
  are sealed strictly past finality and are append-only and immutable. If a change requires mutating
  sealed segments, the design is wrong: go back.
- **Entity derivation.** Built-in IVM (shipped): `balances`, `exposure` and `velocity`, maintained by
  DBSP (reorgs are retractions). Authored SQL (shipped, RFC-0018 §1): `views/*.sql` are named queries
  evaluated at request time over hot ∪ sealed. Not incremental. Authored incremental entities:
  [RFC-0041](docs/rfcs/0041-authored-incremental-entities.md), **shipped 2026-08-28** in 3.0.0-alpha,
  declared in `entities.toml`, maintained by DBSP, served from `/derived` and by name from `/sql`.
  Imperative handlers (WASM transform layer) were **removed 2026-10-08 by Chief** (#1999): do not
  rebuild it or write WASM components; reopening is Chief's call. A nest that declares `[screening]`
  is refused at load. Optional integrations warn-and-skip when unconfigured.

## Correctness rules (full text in [docs/agent-brief/correctness.md](docs/agent-brief/correctness.md))

- Decode is deterministic Rust, topic0-keyed, contract-ABI priority with generic fallback. ABI
  acquisition: Sourcify first, then Etherscan-class APIs; cache ABIs locally. Never retroactively
  re-decode stored history when ABIs improve; version decodings.
- Golden/deterministic-simulation tests for every handler and view; AI-generated tests are welcome
  but must be deterministic and reviewed like any code. Property tests for reorg handling.
- Benchmarks are CI artifacts: single-nest RSS, dense multi-nest RSS, entity point-read p50 and tip
  lag's after-seen p50 fail the build on regression. Backfill events/sec and total tip lag are
  tracked, not gated. Details and the issue numbers are in the full text.
- Running the whole suite on macOS needs rustup's pinned toolchain first on `PATH`, `ulimit -n 10240`,
  and `ps` available (#2606). Details in the full text.

## AI-native surface (built-in, sovereignty-respecting)

- MCP server compiled into the binary: schema discovery, SQL execution, entity lookup,
  streaming subscribe. Works fully offline against the local instance.
- `nuthatch init 0xAddr` scaffolds schema, ABIs, views, llms.txt and a skill from the ABI.
- Ship `llms.txt`, docs-as-MCP, and a `.claude/skills/` directory in scaffolded projects so
  coding agents get real syntax instead of hallucinating.
- Local-first AI: Ollama support and BYO-key. Any AI feature must have a documented
  no-network fallback or be clearly marked unavailable offline.

## Build order and Chief's decisions (full text in [docs/agent-brief/build-order-status.md](docs/agent-brief/build-order-status.md))

The 2026 feature freeze is over (lifted 2026-09-08). **The status log of accepted and parked RFCs, with
dates and conditions, is in the full text and is the record RFC-0044 §8 asks for: read it before
touching anything an RFC, a feature flag or a "parked" label might govern.** Rules from it that bind
every change:

- **RFC-0046 §1 deletion test is a build gate:** delete every payment feature from the tree and a
  self-hoster loses nothing; enable one and the binary still runs, unpriced, for anyone who did not.
  A price that cannot be turned off, a settlement path the binary requires to start, or a key we hold
  violates non-negotiable 3. #1217 fails if it ever does.
- **Never describe the Graph compatibility surface (RFC-0053) as a drop-in replacement, anywhere.** It is a
  partial read surface: exact for a subgraph's event-shaped fields, a named refusal for the rest. It
  never runs AssemblyScript and is a compiler over stored state, not a second indexer.
- **RFC-0059 (folds) and RFC-0060 (Network Subgraph endpoint) stay outside the default binary, behind
  off-by-default cargo features** (`folds`, `graph`). A default build's CLI help, `init` config, on-disk
  layout and behaviour must be what they would be had neither RFC been written. Both are parked.
- New binary capability is decided by Chief and recorded in that status log (RFC-0044 §8's "recorded in CLAUDE.md" now means this log); an RFC acceptance is
  scoped to its evidence (S0 spikes can stop the rest).

Slices (each ends runnable). Do not start slice N+1 while slice N has failing tests or an unmet budget:

1. Skeleton: single binary, config, `init` (ABI fetch → generated project), RPC ingestion,
   decode, redb hot store, HTTP serving of entity point-reads. One chain (Ethereum). This
   slice alone must hit the <2-minute demo.
2. Parquet sealing past finality + DuckDB (Burrmill since 4.1) read-only analytical SQL + reorg property tests.
3. DBSP declarative views (the IVM core) replacing hand-rolled entity updates.
4. ~~Transform runtime ported from liminal with batched Arrow WIT interfaces.~~ Shipped, then removed 2026-10-08 by Chief.
5. MCP server + scaffolded skills + llms.txt.
6. ExEx ingestion mode (colocated reth), then scaled mode (Postgres/DataFusion).

Do not start slice N+1 while slice N has failing tests or an unmet budget.

## Tasks (full table in [docs/agent-brief/tasks.md](docs/agent-brief/tasks.md))

Build, test and lint commands are [yatr](https://github.com/cargopete/yatr) tasks defined in `yatr.toml`.
Run them through yatr, not the tools directly (unchanged inputs are restored from cache).

- `yatr run --agent <task>...` runs tasks, one line per task; failures add the command, exit code and
  log tail (full log in `.yatr/logs/<task>.log`).
- **Before reporting work as done, run `yatr run --agent ci` and fix what it reports.**
- To add or change a task, edit `yatr.toml`, then run `yatr check` and
  `yatr agents --write docs/agent-brief/tasks.md`. The table there is generated; never hand-edit it.
  `yatr explain`, `yatr affected` and `--json` forms are listed in that file.

## Out of scope: do not build, do not suggest (full text in [docs/agent-brief/out-of-scope.md](docs/agent-brief/out-of-scope.md))

The full list has amendments and exact wording; read it before proposing any new feature. Headlines:

- Hosted service, billing, metering and **hosted-SaaS multi-tenancy** (per-tenant authz/quotas,
  isolation between mutually-untrusting paying customers). In scope: multi-nest tenancy in the runtime
  (a tenant is an opaque label plus refcounting, no identity, authn, quotas or metering) and
  distributed **self-hosted** scaled mode (RFC-0022). The line is what nuthatch does about tenants,
  not who they are.
- **Nuthatch does not run a hosted nest service** (platform retired 2026-09-29). Amendments (2026-10-09, 2026-10-21 by
  Chief) say we may offer to host nests for people and teams, and the offer is public on the site, as the
  stock binary run as an operator would (ThinkPad or VPSes): no billing, no accounts, nothing in this tree
  built for it. The shared runtime (RFC-0073) put per-tenant-label shares and caps, query isolation between
  mounts and a per-request cost report in scope; authentication, API keys, accounts, billing and identity
  stay out and stay the gateway's job.
- Token, staking, decentralized network features.
- Non-EVM chains before EVM is airtight. A named user asking for a named chain is a decision for
  Chief, not a standing authorisation.
- TEE attestation, zk proofs.
- Kubernetes manifests, Helm charts, or any deployment story beyond binary + compose.

## Agent worker rules (cloud and headless sessions)

- One task, one branch `agent/<id>`, one PR. Never push to `main` and never merge.
- The brief you were given is the scope. Don't touch anything listed as out of scope.
- Before opening or updating the PR, run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and the tests for the crates you touched. CI is the merge gate.
- Never weaken, skip or delete a test to get green.
- PR body: summary, risks, and exactly how you verified it.
- Stop and say so in the PR (don't push on) if the change needs a public API or schema change, `unsafe`, a new dependency, or more than about 400 changed lines.
- `.claude/hooks/wake.sh` notifies the orchestrator when you stop. Leave it alone.
