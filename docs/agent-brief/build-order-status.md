# Build order, status log and Chief's decisions (full text)

RFC-0044 §8 says a new binary capability is decided and "recorded in CLAUDE.md". Since 2026-11-19 the log below is that record: CLAUDE.md links it, and RFC-0044 §8 carries a note saying so.

Moved verbatim from `CLAUDE.md` on 2026-11-19 to keep the always-loaded standing brief short. Nothing was reworded or dropped; only relative links were re-pointed (`../../`) because the file moved two directories down. `CLAUDE.md` links here; the non-negotiables and the rules summarised there still bind.

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
> [RFC-0053](../../docs/rfcs/0053-graph-subgraph-compatibility.md) on 2026-09-09, a Graph-dialect-to-SQL
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
> 2026-09-26 and asked for [RFC-0058](../../docs/rfcs/0058-cross-nest-sql.md) to be built: a cross view, a
> declared, read-only SQL route over two or more mounts of one tenant in one runtime. It is new query
> surface in the default binary. It stores nothing, feeds no circuit and never merges two chains' block
> streams, so the single-cursor law holds. Its capacity comes out of each member cursor's budget at
> startup and is never borrowed at request time. S0 reported the same day: continue.
> **Parked the same day, after S0**: its one motivating user, the Arcaidia builder, runs on the hosted
> platform, which runs one container per nest, so a cross view could not reach him without a platform
> change, and nobody else asked for it. S1 to S5 wait for a user who does.
>
> **RFC-0059 and RFC-0060 are accepted, and this line is the record RFC-0044 §8 asks for.** Chief
> accepted [RFC-0059](../../docs/rfcs/0059-checkpointed-folds.md) (checkpointed folds) and
> [RFC-0060](../../docs/rfcs/0060-the-network-subgraph-endpoint.md) (the Network Subgraph endpoint) on
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
> [RFC-0062](../../docs/rfcs/0062-maintained-views.md) (maintained views) on 2026-10-07 after its S0 spike put
> every BetSwirl SDK shape under 1 s cold with byte-identical answers. It is new capability in the
> default binary, opt-in per nest through `maintained.toml`, which stays outside the data identity. A
> maintained copy is the request-time view evaluated by the same engine and reused only when every
> input hashes the same, so it adds no second implementation and nothing to the data path that
> determinism forbids. Each slice is gated on byte-identical answers and the Lodestar release gate.
>
> **RFC-0054 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0054](../../docs/rfcs/0054-the-head-count.md) (the head count) on 2026-10-08 and amended
> non-negotiable 3 above with its §1 wording. It is the first network call the binary makes *about*
> an operator rather than *for* one, which is why it needed the amendment and not a reading: off until
> a person types `y` after a successful `init`, two events, no identifier, tallies not records, the
> tally public. §11 is settled with the RFC's own leanings: a single-purpose `count.toml`, the
> receiver on the Helsinki host where we own the ingress log, `chain` kept under the registry-only
> rule, and the number published early with its caveat in the same sentence.
> **Amended 2026-11-12 by Chief** (RFC-0054 §12): still opt-in. A yes now also covers a weekly
> `alive` from a running nest with a bucketed query count, and an `init` with no terminal prints
> one line saying how to be counted. On-by-default was built and withdrawn the same day.
>
> **RFC-0063 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0063](../../docs/rfcs/0063-address-history.md) (address history) on 2026-10-09 for rotki, whose
> founder said yes to a local indexer source the same day (rotki/rotki#13335). Chief's words: build
> everything rotki needs, extending nuthatch as far as that takes. It reopens #272, #277 and #308 from
> frozen-for-2027, now scoped as RFC-0063 slices. A rotki-mode nest is its own cursor beside the
> event pipeline, per user and local, Etherscan-shaped at `/api`. It polls every 5 minutes and is
> gated to stay lightweight. Only address-free data such as timestamp to block is hosted. Seven slices,
> each gated on Etherscan parity, MEV from the relays included.
>
> **RFC-0065 is accepted through S6, and this line is the record RFC-0044 §8 asks for.** Chief
> accepted [RFC-0065](../../docs/rfcs/0065-the-nest-in-a-tab.md) (the nest in a tab) on 2026-10-17. It
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
> **RFC-0066 is accepted through S2, and this line is the record RFC-0044 §8 asks for.** Chief
> accepted [RFC-0066](../../docs/rfcs/0066-the-nest-builder.md) (the nest builder) on 2026-10-18 ("letsgooooo",
> after S0 landed in #2097 and #2098). It is a static page beside the playground, not a binary change:
> a visitor pastes a contract address, the tab scaffolds the nest with `init`'s own code compiled to
> wasm (`nuthatch-scaffold`, `nuthatch-identity`), shows its NID, and indexes it on RFC-0065 S6's
> backfill. S1 (the page and a zip out) and S2 (Index it) are authorised in order, each on its own
> gate: the zip run with `nuthatch dev --dir` reports the NID the page showed, and every segment the
> tab seals equals the one a node seals from that zip. Nothing of ours is hosted for it; S3 (GitHub)
> waits, and S4 (a drafted view) will not be built: no users yet (Chief, 2026-10-18).
>
> **RFC-0068 to RFC-0072 are accepted, and this line is the record RFC-0044 §8 asks for.** Chief asked
> for "something fun" on 2026-10-21 and accepted all five the same day, each on its own RFC's
> recommendations, to be built one after another in this order:
> [0071](../../docs/rfcs/0071-nuthatch-diff.md) (the diff, default binary, read-only, templated sentences,
> no model), [0070](../../docs/rfcs/0070-time-lapse.md) (time-lapse, the tab first, the binary producer
> behind an off-by-default `timelapse` feature), [0072](../../docs/rfcs/0072-chain-weather.md) (chain
> weather, default binary, opt-in per nest by `weather.toml`, model prose off until `[prose]` is
> written, and model text is never stored or served by the nest, only sent at delivery),
> [0068](../../docs/rfcs/0068-nest-postcards.md) (postcards, reshaped by Chief the same day into a link: a `/play` URL
> whose fragment pins a published nest's segments by hash, read from any static host; no binary
> change), and
> [0069](../../docs/rfcs/0069-the-engine-room.md) (the engine room, a watch subcommand behind a `watch`
> feature on by default, which lifts his 2026-10-03 preference for the cockpit over a TUI for this
> one). Each slice keeps its own gate; an S0 that fails stops its RFC.
>
> **Where they stand (2026-10-23).** 0071 shipped in 5.9.0. 0068's postcards are live in `/play`.
> 0069 is built in full, S1 to S5 (#2343 to #2347): `nuthatch watch` is in the default binary.
> **0070 and 0072 are parked by Chief (2026-10-22, "scratch off weather and time lapse")**: time-lapse
> stopped after S2, live at `/play/timelapse/` with no binary producer; chain weather stopped after
> S0, with nothing in the binary. Do not resume either unasked.
>
> **RFC-0073 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0073](../../docs/rfcs/0073-the-shared-runtime.md) (the shared runtime) on 2026-10-27, all sixteen of
> GraphOps's requests: "lets do ALL of it". It is new capability in the default binary for an operator
> who hosts many organisations' nests in one runtime, and the out-of-scope list below is amended the
> same day for the three items that crossed it. The order is the RFC's: the small operability items
> first, then per-mount limits and fault containment, then fair share and runtime memory, then the
> hardened SQL surface, with read replicas and upgrades without an outage as the last and largest
> slices. FR8's isolation guarantee ships only after an independent review.
>
> **RFC-0077 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0077](../../docs/rfcs/0077-the-embeddable-engine.md) (the embeddable engine) on 2026-10-30, from
> Chris's "it's pure rust and can(?) be embedded as a wasm blob". Chief's rule binds every slice:
> "nuthatch is one indexer software with many ways to run it, wasm only if needed (as a feature) and
> the main path, the delightful core, stays as is." The binary stays the main pitch. The engine is
> an opt-in separate crate, off by default; the default binary gains no wasmtime and no wasm
> toolchain, and RFC-0046 §1's deletion test applies. The wasm32 constraint binds only the modules
> the two share, and where it would cost the native path, the binary keeps its own implementation.
> S0 to S5 are authorised in order, each on its own gate, then S7, a "verify it locally" switch on Lodestar's
> `/profile`; S6 (rotki) waits for Lefteris to ask. A
> blog post on what it is and how it works follows completion.
>
> **RFC-0081 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0081](../../docs/rfcs/0081-the-engine-in-use.md) (the engine in use) on 2026-10-31: what S7 had to
> work round, fixed in the engine, plus `verify` for a hosted `/sql` answer. Each slice begins only when
> Chief says. S0 to S2 are built (S0 narrowed the hybrid open to whole pool tables, 179 MiB once); S3 and
> S4 are authorised; S5 waits (Safari skipped for now); **S6, Arbitrum's verified head, is declined**
> (Chief, 2026-11-01: "a week old thing is not worth it"), so the engine trusts its RPC on Arbitrum. RFC-0077 §1a binds every slice, the binary is unchanged
> in each, and no slice raises the module's size ceiling on its own.
>
> **RFC-0076 is accepted, and this line is the record RFC-0044 §8 asks for.** Chief accepted
> [RFC-0076](../../docs/rfcs/0076-scaling-the-shared-runtime.md) (scaling the shared runtime) on 2026-10-29,
> GraphOps's follow-up requests: "yes. letsgooo". FR17 to FR19 are new capability in the default binary:
> followers on other hosts from the mirror, nests on one chain progressing from their own watermarks
> behind one extraction stage, and tenant reservations held only while a tenant queries. The cursor
> stays one per chain. S0, a spike of FR18's window buffer, goes first and can stop FR18. **FR20, several
> writer processes per chain, is accepted as direction only**: nothing of it is built, and the
> single-cursor law above is not amended, until FR18 and FR19 have shipped and a chain is shown to
> exceed one machine after them.
>
> **RFC-0078 to RFC-0080 are accepted, and this line is the record RFC-0044 §8 asks for.** Chief
> accepted [RFC-0078](../../docs/rfcs/0078-writer-failover.md) (writer failover),
> [RFC-0079](../../docs/rfcs/0079-arrow-results.md) (Arrow results) and
> [RFC-0080](../../docs/rfcs/0080-partial-sync.md) (partial sync) on 2026-10-30, after GraphOps's "this is
> fundamentally not HA" on FR20 and their Electric SQL suggestion: "ok letsgoooo with the wave of rfcs".
> Built in that order, HA first. RFC-0078 keeps the single-cursor law as written: one writer per chain
> at any moment, a standby taking over through a lease in the operator's mirror, opt-in, embedded mode
> unchanged. RFC-0079's IPC answer is default-binary and adds no dependency; Flight SQL is behind an
> off-by-default `flight` feature. RFC-0080's server half is opt-in per mount and served by followers;
> its client half is an off-by-default `sync` feature of the engine crate. The gateway's `pin` header
> keeps identity out of nuthatch. Each S0 can stop its RFC. **RFC-0080's third S0 failed (2026-11-03,
> §20, #2585)**: the shipped `/sync` cost 461% of a follower's apply on the gate's fixture, against a
> 10% gate, and no shape class passes. Chief chose to take the server half out of the default binary:
> it is built only with the off-by-default `sync-server` feature, a default build has no `/sync` route
> and refuses `[mounts.sync]` by name, and the code stays. A redesign (rows shown without the engine,
> §12's column probe) waits for a user who asks for it.
>
> **`block_timestamps = "lazy"` is accepted, and this line is the record RFC-0044 §8 asks for.** Chief
> accepted #3065 (PR #3078) on 2026-11-21: an opt-in `[nest] block_timestamps = "lazy"`, set by
> `nuthatch init --lazy-timestamps`, so a chain whose `eth_getLogs` carries no `blockTimestamp` (XDC,
> #3061) backfills without one `eth_getBlockByNumber` per log block. It is new binary capability: a
> config value, a CLI flag and a `schema_version = 2` nest. **Every lazy row's `block_timestamp` is
> null, even where the node stamped the log.** Chief chose that over keeping each log's own stamp,
> which made which rows were null depend on which endpoint served `eth_getLogs`, against
> non-negotiable 4, and over amending the non-negotiable, which stands as written. Nothing is lost by
> it: the eager default already takes a log's stamp and buys no header when the node sends one, so
> lazy is only for chains whose endpoints do not. An indexed nest is refused if it switches to or from
> lazy, and eager and timestamp-free nests keep their sealed bytes and content addresses.

>
> **What is still deferred, and stays deferred.** Lifting the freeze is not a blanket reopening.
> `docs/frozen-for-2027.md` stands unchanged, with its own rule: reopen one item at a time, naming
> the new demand or evidence and an acceptance criterion that can fail. Chief separately deferred
> RFC-0003, RFC-0023, RFC-0031, RFC-0033, RFC-0034 and RFC-0036 again on 2026-09-08, and closed
> RFC-0013 and RFC-0021 the same day. Of slice 6 below, ExEx is not started; scaled mode is built (the `postgres-store` feature, RFC-0022) and every release publishes it as the separate `scaled` artifact. RFC-0042 is
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
