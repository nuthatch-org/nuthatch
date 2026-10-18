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

## Correctness rules

- Decode: deterministic Rust, topic0-keyed, contract-ABI priority with generic fallback.
  ABI acquisition: Sourcify first, then Etherscan-class APIs. Cache ABIs locally.
- Never retroactively re-decode stored history when ABIs improve; version decodings.
- Golden/deterministic-simulation tests for every handler and view (Matchstick lineage):
  fixed block fixtures in, exact entity state out. AI-generated tests are welcome, but they
  must be deterministic and reviewed like any code.
- Property tests for reorg handling: random reorg depths against the hot store must always
  converge to the canonical chain state.
- Benchmarks are CI artifacts. Single-nest RSS, dense multi-nest RSS, entity point-read p50 and
  tip lag's after-seen p50 (fetch, decode and commit at the tip: 30 ms against 17-19 ms measured)
  fail the build on regression. Backfill events/sec is measured on every PR and tracked, not gated:
  the CI fixture cannot see a 4x decode cost (docs/benchmarks.md, #1723). Total tip lag, dominated
  by the poll interval, is tracked, not gated (#1884).

## AI-native surface (built-in, sovereignty-respecting)

- MCP server compiled into the binary: schema discovery, SQL execution, entity lookup,
  streaming subscribe. Works fully offline against the local instance.
- `nuthatch init 0xAddr` scaffolds schema, ABIs, views, llms.txt and a skill from the ABI.
- Ship `llms.txt`, docs-as-MCP, and a `.claude/skills/` directory in scaffolded projects so
  coding agents get real syntax instead of hallucinating.
- Local-first AI: Ollama support and BYO-key. Any AI feature must have a documented
  no-network fallback or be clearly marked unavailable offline.

## Build order (vertical slices; each ends runnable)

> **Status 2026-09-08: the 2026 feature freeze is over. Chief lifted it, in full, on 2026-09-08.**
> Slices 1-5 are shipped. The freeze ran from 2026-08-20 and did what it was written to do: every
> defect worth finding that quarter came from running the product rather than extending it. It ends
> because there is now a body of design worth building, not because the discipline failed.
>
> **The carve-out mechanism is retired with it.** All five carve-outs were taken and all five are
> spent: RFC-0041 (authored incremental entities, shipped 3.0.0-alpha), RFC-0042 (the no-DuckDB
> investigation, closed KEEP DuckDB at §14, superseded when Burrmill replaced DuckDB in 4.1),
> RFC-0051 (Monad), RFC-0050 (Robinhood Chain) and
> RFC-0040 (the freshness dial, shipped 3.5.0). Their reasoning lives in those RFCs and in the
> release notes; it is not repeated here. Nothing needs a carve-out any more, because nothing is
> frozen that a decision has not separately deferred.
>
> **What the unfreeze is for.** One programme, chosen the day the freeze lifted: **RFC-0044 through
> RFC-0048, in full.** The subgraph port skill, offchain data, x402 at the counter, the lakehouse
> commitments, and pricing query access. They are one argument in five documents - the first RFCs
> about what nuthatch guarantees to people who are not running it - and each proposes specifying
> behaviour that already happens rather than inventing a mechanism. Every one of them leads with the
> non-negotiable it appears to break and shows why it does not; **those arguments are load-bearing
> and none of them is now waived.** RFC-0046 §1's test in particular is a build gate, not a
> paragraph: *delete every payment feature from the tree and a self-hoster loses nothing.*
>
> **RFC-0053 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0053](docs/rfcs/0053-graph-subgraph-compatibility.md) on 2026-09-09, a Graph-dialect-to-SQL
> compatibility surface so a caller adopts a nest by changing a GraphQL URL. **Rescoped 2026-09-12:
> that last clause did not survive contact with a measurement. What was built is a partial read
> surface - exact for a subgraph's event-shaped fields, a named refusal for the rest - and an
> analytics client cannot adopt it unmodified, because GraphQL refuses a whole query for one
> unanswerable field. Do not describe this as a drop-in replacement anywhere; RFC-0053's Measured
> outcome has the figures and the rescoped deliverables.** It is **new binary
> capability**, which is exactly the case RFC-0044 §8 says must be decided here or not happen, and it
> **overrides RFC-0044 §11's "not a subgraph compatibility layer" non-goal**, annotated in that RFC.
> It sits outside the programme's original five and is now the sixth document in it.
>
> Three things the acceptance does not do, because they were the grounds for §11 in the first place.
> It does not run AssemblyScript, in the indexing path or anywhere else; RFC-0038 §8 still holds.
> It does not promise byte-for-byte parity for the ordered, mapping-derived family - `derivedETH`,
> `volumeUSD`, `totalValueLockedUSD` and their relatives are a **compiler over stored state, not a
> second indexer**, and RFC-0038 §6a's finding is unchanged. And it does not reopen arbitrary SQL or
> RFC-0034's resource limits merely because a request arrived as GraphQL. The compatibility surface
> is a read surface; nothing about it touches non-negotiable 4, because no LLM and no query shape
> feeds stored state.
>
> **The authorisation is scoped to the evidence.** RFC-0053 S0 (#1264, the migration validator) is in
> the `resolute-robin` sprint. S1 to S4 (#1265 to #1268) are filed and wait on what S0 measures,
> because S0 is the only slice that can falsify the others. Accepting the direction is not accepting
> the build-out sight unseen.
>
> **RFC-0058 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief unparked #1324 on
> 2026-09-26 and asked for [RFC-0058](docs/rfcs/0058-cross-nest-sql.md) to be built: a cross view, a
> declared, read-only SQL route over two or more mounts of one tenant in one runtime. It is new query
> surface in the default binary. It stores nothing, feeds no circuit and never merges two chains' block
> streams, so the single-cursor law holds. Its capacity comes out of each member cursor's budget at
> startup and is never borrowed at request time. S0 reported the same day: continue.
> **Parked the same day, after S0**: its one motivating user, the Arcaidia builder, runs on the hosted
> platform, which runs one container per nest, so a cross view could not reach him without a platform
> change, and nobody else asked for it. S1 to S5 wait for a user who does.
>
> **RFC-0059 and RFC-0060 are accepted, and this line is the record RFC-0044 §8 asks for.** Chief
> accepted [RFC-0059](docs/rfcs/0059-checkpointed-folds.md) (checkpointed folds) and
> [RFC-0060](docs/rfcs/0060-the-network-subgraph-endpoint.md) (the Network Subgraph endpoint) on
> 2026-09-21, on one condition that binds every slice: **all of it stays outside the default binary,
> behind cargo features that are off by default** (`folds`, and `graph`, which enables it). No
> regular nuthatch user sees any graph-facing surface unless they opt in. The test is RFC-0046 §1's
> deletion test: a default build's CLI help, the config `init` writes, its on-disk layout and its
> behaviour are what they would be had neither RFC been written. RFC-0060 is the named consumer
> RFC-0053's unpark trigger asks for. It unparks 0053 for its 21 client operations only, and the
> "not a drop-in replacement" rule above still stands. Order of work: the ThinkPad-only facade code
> goes under version control first (done as `pete/network-facade-rescue`, 2026-09-21), then
> RFC-0059's S0 spike, which can stop the programme, and only then the runtime.
>
> **Parked 2026-09-26 by Chief: RFC-0059 and RFC-0060, in full.** RFC-0059 S0 to S3 are built and
> stay in the tree behind the off-by-default features (S3's head snapshots in #1517). Nothing further
> is started: not S4 serving, not RFC-0060's fold ports. It stopped at #1514, where a keyed
> `allocations` fold proved to carry all 861,541 allocations ever made, about 14,600 of them active,
> against the 300 MiB head gate. Even finished, it would replace the gateway for indexer-agent,
> indexer-service-rs and tap-agent only, and the "not a drop-in replacement" rule stands. Reopening
> is Chief's call on whether that is worth the remaining sprints. The trackers are #1441 and #1442.
>
> **RFC-0062 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0062](docs/rfcs/0062-maintained-views.md) (maintained views) on 2026-10-07 after its S0 spike put
> every BetSwirl SDK shape under 1 s cold with byte-identical answers. It is new capability in the
> default binary, opt-in per nest through `maintained.toml`, which stays outside the data identity. A
> maintained copy is the request-time view evaluated by the same engine and reused only when every
> input hashes the same, so it adds no second implementation and nothing to the data path that
> determinism forbids. Each slice is gated on byte-identical answers and the Lodestar release gate.
>
> **RFC-0054 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0054](docs/rfcs/0054-the-head-count.md) (the head count) on 2026-10-08 and amended
> non-negotiable 3 above with its §1 wording. It is the first network call the binary makes *about*
> an operator rather than *for* one, which is why it needed the amendment and not a reading: off until
> a person types `y` after a successful `init`, two events, no identifier, tallies not records, the
> tally public. §11 is settled with the RFC's own leanings: a single-purpose `count.toml`, the
> receiver on the Helsinki host where we own the ingress log, `chain` kept under the registry-only
> rule, and the number published early with its caveat in the same sentence.
>
> **RFC-0063 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0063](docs/rfcs/0063-address-history.md) (address history) on 2026-10-09 for rotki, whose
> founder said yes to a local indexer source the same day (rotki/rotki#13335). Chief's words: build
> everything rotki needs, extending nuthatch as far as that takes. It reopens #272, #277 and #308 from
> frozen-for-2027, now scoped as RFC-0063 slices. A rotki-mode nest is its own cursor beside the
> event pipeline, per user and local, Etherscan-shaped at `/api`. It polls every 5 minutes and is
> gated to stay lightweight. Only address-free data such as timestamp to block is hosted. Seven slices,
> each gated on Etherscan parity, MEV from the relays included.
>
> **RFC-0065 is accepted through S6, and this line is the record RFC-0044 §8 asks for.** Chief
> accepted [RFC-0065](docs/rfcs/0065-the-nest-in-a-tab.md) (the nest in a tab) on 2026-10-17. It
> runs nuthatch's core somewhere new: decode, sealing and Burrmill compiled to WebAssembly in a
> browser worker, following a chain from an RPC. S0 and S1 are done (#2083): defi-pulse-nest is the
> demo, and the playground is live at nuthatch-indexer.com/play. S2 to S5 are authorised in order,
> each on its own gate. The binary is unchanged by it; the tab reuses the crates the binary already
> uses, so a tab and a node seal the same bytes. The tab seals nothing until two independent RPCs
> agree on a finalized block hash, and every segment it seals must hash to the one on the mirror.
> Until S5's light client verifies the head, the page says plainly that it trusts its RPCs.
> **S6 accepted 2026-10-18**, Chief: "yes very good i want this": a young contract's backfill in the
> tab, gated on segment hashes equal to native sealing, as the engine for RFC-0066's nest builder.
>
> **What is still deferred, and stays deferred.** Lifting the freeze is not a blanket reopening.
> `docs/frozen-for-2027.md` stands unchanged, with its own rule: reopen one item at a time, naming
> the new demand or evidence and an acceptance criterion that can fail. Chief separately deferred
> RFC-0003, RFC-0023, RFC-0031, RFC-0033, RFC-0034 and RFC-0036 again on 2026-09-08, and closed
> RFC-0013 and RFC-0021 the same day. Slice 6 below (ExEx, scaled mode) is not started. RFC-0042 is
> moot: Burrmill replaced DuckDB in 4.1, so its trigger on RFC-0033 slice 4 (#357) no longer applies.
>
> **The out-of-scope list below is unchanged and still binds.** No hosted service, no token, no
> non-EVM before EVM is airtight, no TEE or zk, no Kubernetes. The freeze ending widens what may be
> built; it does not widen what the product is.
>
> **On the apparent conflict between that list and RFC-0046 / RFC-0048**, raised in review of the PR
> that made this change and settled here rather than in an RFC a reader has to go and find. The list
> forbids **us** running a hosted service and **us** billing and metering customers - the
> become-a-data-service-company path - and per-tenant billing and authz remain the gateway's job. It
> does not forbid an operator charging at their own endpoint. **That is the same line the tenancy
> paragraph above already draws:** what nuthatch does about tenants, not who they are. The Nuthatch
> Data Service has answered `402 TAP-Receipt header required` since long before this programme and
> nobody read it as a violation, because the paywall is a deployment an operator chose rather than a
> property of the binary. x402 is a second payment method for the same choice, and the §8 governance
> question was put to Chief on 2026-08-30 and answered: cross the line deliberately.
>
> **What keeps that distinction honest is a test, not an assurance**, and it gates the whole payment
> half: *delete every payment feature from the tree and a self-hoster loses nothing; enable one and
> the binary still runs, unpriced, for anyone who did not.* A price that cannot be turned off, a
> settlement path the binary requires to start, or a key we hold has crossed from operator choice
> into gated product and violates §3. #1217 exists to fail if it ever does.

1. Skeleton: single binary, config, `init` (ABI fetch → generated project), RPC ingestion,
   decode, redb hot store, HTTP serving of entity point-reads. One chain (Ethereum). This
   slice alone must hit the <2-minute demo.
2. Parquet sealing past finality + DuckDB (Burrmill since 4.1) read-only analytical SQL + reorg property tests.
3. DBSP declarative views (the IVM core) replacing hand-rolled entity updates.
4. ~~Transform runtime ported from liminal with batched Arrow WIT interfaces.~~ Shipped, then removed 2026-10-08 by Chief.
5. MCP server + scaffolded skills + llms.txt.
6. ExEx ingestion mode (colocated reth), then scaled mode (Postgres/DataFusion).

Do not start slice N+1 while slice N has failing tests or an unmet budget.

## Out of scope - do not build, do not suggest

- In this tree: hosted service, billing, metering, **hosted-SaaS multi-tenancy** (per-tenant
  authz/quotas, isolation between mutually-untrusting paying customers - that's the
  become-a-data-service-company path, and the gateway's job regardless). Note: **multi-nest
  tenancy in the runtime** (a tenant is an opaque ownership label plus refcounting - no identity,
  no authn, no quotas, no metering) and *distributed **self-hosted** scaled mode* (one
  operator's writer pool + query-FE tier + control-plane, RFC-0022) are both **in scope** - see Architecture. **The line is what
  nuthatch does about tenants, not who they are** (amended 2026-08-04): it sees a string, refcounts
  it, and knows nothing else, so an operator's tenants may well be paying customers and nuthatch
  has no concept of it. Per-tenant billing and authz stay out and are the gateway's job. The
  earlier wording drew the line at "cooperating tenants an operator picked - not paying strangers",
  which asked a question nobody could answer from the code.
  **Amended 2026-09-29: Nuthatch does not run a hosted nest service.** The 2026-09-23 override
  that approved one is withdrawn, and the platform built under it (`nuthatch-org/nuthatch-hosted`)
  is retired. Hosted nests will be offered by GraphOps on its own platform, running the stock
  binary like any other operator, which is why the website's hosted page says "coming soon".
  Nothing in this tree is built for it: a hosted need that would require a change here is a normal
  nuthatch feature on its own merits, or it is not built.
  **Amended 2026-10-06: Chief lifted that rule for the subgraph stopgap.** Nuthatch serves stopgap
  nests for BNB and Polygon subgraphs the network does not serve, on the ThinkPad or new VPSes, until an
  indexer picks each one up ([docs/subgraph-stopgap.md](docs/subgraph-stopgap.md)). It is the stock
  binary run as an operator would, not a platform: no billing, no accounts, nothing in this tree built
  for it. The hosted-platform retirement above otherwise stands.
  **Amended 2026-10-09 by Chief: we may offer to host nests for people and teams**, not only stopgap
  subgraphs. The same terms bind: the stock binary run as an operator would, on the ThinkPad or our
  VPSes, no billing, no accounts, nothing in this tree built for it. It is an offer made to a named
  user, not a platform or a public service; a hosted need that requires a change here is a normal
  nuthatch feature on its own merits, or it is not built. That includes a direct offer to a named
  Graph user whose subgraph is hurt by the Studio sunset or a chain's deprecation; the 2026-10-08
  stopgap hold still rules out promotion and any approach through the Foundation.
- Token, staking, decentralized network features (a possible future Graph Horizon data
  service is explicitly deferred).
- Non-EVM chains before EVM is airtight.
- TEE attestation, zk proofs (verifiability = deterministic re-execution of the core
  + content-addressed segments; nothing heavier).
- Kubernetes manifests, Helm charts, or any deployment story beyond binary + compose.
