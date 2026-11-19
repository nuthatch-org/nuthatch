# RFC-0044: The subgraph port skill - one subgraph, one nest

- Status: **Implemented in part.** S1 (#1210), S2 (#1211) and S5 (#1214) shipped 2026-09-09. S3 (#1212)
  and S4 (#1213) were closed not planned on 2026-09-12, parked with RFC-0053. Tracker #1205 is
  closed.
- Author: Pete (cargopete)
- Date: 2026-08-26
- Provenance: the GraphOps 1:1 with Chris Wessels. His words, from the transcript summary:
  *"A future AI skill that migrates an existing subgraph and its GraphQL queries into a nest and
  SQL was suggested as a way to make trial adoption much easier."*
- Depends on: **0038** (the input importer this sits on top of, shipped v2.6.0), **0041** (where the
  ported entities land, proposed), **0017** (the builder skill: the house form for a skill RFC, and
  the drift rules that keep one honest), 0018 §1, 0016 §2.
- Blocks: the only cheap acquisition path we have. Every prospective user already runs a subgraph.
- Nature: an **adoption** RFC. It designs a skill and a report, not runtime behaviour. Deliberately
  small in the 0017 sense: the expensive knowledge already exists, having been earned three times by
  hand on horizon, livepeer and lodestar.

## 1. The claim this RFC makes

> **A team that runs a subgraph can hand its deployment CID to an agent and get back a working nest,
> plus an honest account of every field that will not match.**

Note which half is load-bearing. The scaffold is the easy half and mostly already ships. The account
of what will not match is the deliverable, because the audience is someone who already has a working
system and is deciding whether to keep it.

## 2. What already ships, and exactly where it stops

`nuthatch init --from-subgraph <CID>` (RFC-0038 §6c, shipped v2.6.0, 2026-08-19) reads a published
manifest off IPFS with every body hashed back to its own CID before it is believed, vendors the ABIs
the manifest pins, writes the contracts, chain and start blocks, and infers factory rules where the
parameter names the template - reporting by name, with candidates, where it cannot. On Uniswap V3
it needed no help.

A subgraph, however, is four artefacts, and the binary imports one of them:

| Artefact | What it decides | Who imports it today |
|---|---|---|
| `subgraph.yaml` + `networks.yaml` | contracts, chain, start blocks, ABIs, events, templates | **the binary**, well |
| `schema.graphql` | the entity shapes a caller depends on | **nobody**. `subgraph_import.rs` never opens it |
| `src/mappings/*.ts` | how each entity derives, and every `eth_call` it makes | **a human**, by hand |
| the caller's GraphQL | the queries an application already has running | **nobody** |

The importer covers the **inputs**. Nothing covers the **outputs**. That is the whole gap, and it is
why §6c of RFC-0038 could report "the importer needed no help" and then hand-write the `[[calls]]`
stanzas underneath: somebody read `fetchTokenSymbol` in the AssemblyScript and typed the TOML.

**Correcting the record:** the working note from the livepeer port (2026-07-22) says `eth_call`
enrichment is structurally unreachable in a nest. RFC-0038 §3 closed that on 2026-08-19 - a call may
now be parameterised by the row that triggered it, pinned at that row's block, with `CallKey`
unchanged. Anything written against the older boundary, this RFC included until this paragraph, is
describing a limit that no longer exists.

## 3. Why this is a skill and not more binary

The three unimported artefacts are not parsing problems. They are judgement problems.

A mapping handler is imperative AssemblyScript. Deciding whether a given entity field is a pure
function of the event log, a read of contract state, or a read of the subgraph's own prior output
requires reading code and reasoning about it. RFC-0038 §6a is precisely this exercise done by hand
on `pricing.ts`, and its conclusion - that `getEthPriceInUSD` is exactly expressible while
`findEthPerToken` is order-dependent by construction - is not something a parser arrives at.

That is a good task for a model with the repo's own rules in front of it, and a bad thing to encode
as a static analyser over a language we do not otherwise touch. It is also the RFC-0017 distinction
applied one level up: the builder skill teaches an agent to drive nuthatch, this teaches an agent to
**translate** something into nuthatch.

## 4. Scope: one subgraph, one nest, and nothing cleverer

A manifest already is exactly one dataset, one chain (per `networks.yaml` entry), one schema. It maps
onto one nest without any interpretation. Deliberately excluded, because each invents a problem we do
not have:

- **No merging several subgraphs into a nest.** Cross-nest reuse already exists (RFC-0033 grafting)
  and is a better answer to that want.
- **No fan-out of one subgraph across nests.** A multi-network `networks.yaml` yields one nest per
  network, which is one cursor per chain, which is the law (RFC-0021).
- **No round trip.** Nothing generates a subgraph from a nest.

## 5. What the skill emits

### 5a. The port report, first, before anything runs

The first artefact is a report, written before the nest is scaffolded and readable by someone who has
not decided to port yet. Every entity field in `schema.graphql` is classified into exactly one of
four classes, each traceable to a named line of mapping source:

| Class | Meaning | Ported as |
|---|---|---|
| **Exact** | a pure function of decoded events | a view or entity; byte-identical |
| **Call-derived** | reads contract state at the row's block | a `[[calls]]` declaration (RFC-0038 §3) |
| **Fixed point** | reads back its own or another entity's prior output | a convergent value: defensible, and **different** |
| **Unreachable** | needs internal calls, `@fulltext`, or time travel | not ported; named, with the reason |

The fourth column of RFC-0038 §6a's cost table is where this classification comes from; this RFC's
contribution is making it a mechanical output rather than an essay somebody writes once per port.

**A report that is wrong about its own gaps is worse than no report**, for the same reason a skill
that lies about a flag name is worse than no skill (RFC-0017). §10 makes that the acceptance test
rather than a hope.

### 5b. Then the artefacts

`nuthatch.toml`, `abis/`, `views/` or `entities/` (§6), `[[calls]]` stanzas derived from the mapping's
actual contract reads, `checks/`, and a README carrying the port report verbatim. The layout is the
first-party nest convention already in use: commit the config, ABIs, checks, schema, semantics and
views; gitignore the store, the segments and the logs; public RPCs only in the committed TOML.

### 5c. Known traps the skill must carry

Earned, not theorised, and each one cost real hours:

- The **proxy trap** applies to `nuthatch init 0xAddr` and *not* to `--from-subgraph`: the manifest's
  pinned ABIs are the implementation ABIs, which is the entire reason the importer prefers them. The
  skill must say which path it is on, because the advice inverts.
- `[[factories]] watch =` takes a **contract alias or template name, never an address**, whatever
  `config-reference.md` shows.
- One proxy may need **several ABIs** across its history: Horizon renamed every staking event, and a
  nest carrying only the current ABI loses 366 million blocks silently.
- The snake_caser explodes acronyms: `ServiceURIUpdate` becomes `service_u_r_i_update`. Cosmetic,
  and it will be the first thing anyone notices.
- **Verify against the chain, not the gateway.** The decentralised gateway needs auth and may be
  refusing the deployment anyway; `cast call` on the canonical getters is ground truth the subgraph
  itself reads. On-chain sentinels (`deactivationRound = 2^256-1`) map to a view's `null`.

## 6. Where the entities land, and why that does not block this

A subgraph's entities are incrementally maintained. Ours, today, are `views/*.sql`, recreated in
ephemeral DuckDB per request - which is exactly the gap Chris raised in the same conversation and
exactly what RFC-0041 exists to close. Ported today, a nest is correct and reads its entities slower
than the subgraph it replaced. That is a memorable first impression of the wrong kind for precisely
the trial user this RFC is courting.

It is not, however, a reason to wait. Everything difficult here - manifest handling, the mapping
read, the classification, the call derivation, the query rewrite - is indifferent to how entities are
materialised. Only the emit target changes, from `views/*.sql` to RFC-0041's `entities/*.sql` once
#820 defines the authoring surface and #822 serves them. That is one section of one skill file.

So: **build against views now, with the emit target isolated in one place**, and the report states
plainly which of the two a given field landed in. Waiting buys nothing and costs every trial user
between now and #822.

## 7. The GraphQL half comes second

Chris asked for the queries too, and that is the harder half: the caller has working GraphQL against
a subgraph schema, and a nest serves SQL and entity point-reads. Somebody's application code changes
either way.

It is deferred to a later slice on purpose, because it is the only part of this RFC with an unsolved
design question inside it (§12.2) and it must not hold up the part that does not. A port that gets
the data right and hands over a query-translation table is worth shipping; a port that waits for a
GraphQL compatibility answer ships nothing.

That half is now [RFC-0053](0053-graph-subgraph-compatibility.md), accepted 2026-09-09. It overrides
§11's compatibility-layer non-goal; this section's sequencing argument is unaffected, because the
whole point of it was that the two halves ship independently.

## 8. The freeze

The 2026 freeze permits bug fixes, security, performance, maintenance, **marketing** and making the
delightful core best in class. It has exactly two recorded carve-outs and this is not one of them.

The split is clean, and Chief decides it rather than this document:

- **Slices 1-3 change no nuthatch behaviour.** They are skill files and a report generator, in the
  repo alongside `skills/nuthatch-builder/`. No runtime code, nothing in the data path, no new
  configuration surface. That is adoption tooling, which reads as permitted.
- **Slice 4 is different.** Teaching `--from-subgraph` to read `schema.graphql` would be new binary
  capability and needs its own explicit decision, recorded in CLAUDE.md, or it does not happen.
  *(Note 2026-11-19: CLAUDE.md now links its decision log, which lives in
  [docs/agent-brief/build-order-status.md](../agent-brief/build-order-status.md). "Recorded in CLAUDE.md" means that log.)*

Nothing here is a carve-out until it appears in that list.

## 9. Slices

1. **S1 - the classifier and the report.** Read `schema.graphql` and the mappings, classify every
   field into §5a's four classes with a source citation each, emit the report. No scaffolding. Run it
   against the three subgraphs we have already ported by hand, where the answer is known.
2. **S2 - the emit.** `[[calls]]` derivation from the mapping's real contract reads, views, checks,
   README. Sits on top of `--from-subgraph` rather than reimplementing it.
3. **S3 - the acceptance port.** §10, on a subgraph somebody actually runs.
4. **S4 - the query table.** GraphQL operation to SQL, per §7. Gated on §12.2.
5. **S5 - the RFC-0041 swap.** Emit `entities/*.sql` once #820 and #822 land. One file.

## 10. Acceptance

Two gates, both mechanical.

**The port runs.** A subgraph neither of us picked for convenience is ported by an agent holding only
this skill and a shell, reaches tip, and answers a canned question correctly.

**The report is right about itself.** Diff the ported nest against the gateway, field by field.
Every field the report called *exact* matches byte-for-byte, and every field that diverges was named
in advance as *fixed point*, *call-derived* or *unreachable*. **A single unpredicted divergence fails
the slice**, because an unpredicted divergence is the exact experience this RFC exists to prevent.
RFC-0038 §6b establishes that a gateway diff is runnable; this reuses the method and changes what is
being judged.

## 11. Non-goals

- **Not running AssemblyScript.** The skill *reads* mappings to classify them. Nothing executes them,
  now or later; RFC-0038 §8 rules that out and this does not reopen it.
- **Not claiming byte-identical entity parity.** RFC-0038 §6a settled that it is unavailable
  declaratively for the order-dependent family. This RFC's job is to say so before the user finds out.
- **Not a subgraph compatibility layer.** No GraphQL endpoint that impersonates a subgraph, no
  `graph-node` protocol surface. A translation table, not a shim.
  **Overridden 2026-09-09, on Chief's decision, by
  [RFC-0053](0053-graph-subgraph-compatibility.md)**, which proposes exactly the surface this bullet
  rules out. The non-goal was right for *this* RFC and §7 is why: a port skill that waited on a
  compatibility answer would have shipped nothing. What changed is that the question now has a
  document of its own, so declining it here no longer declines it everywhere. Nothing else in §11
  moves, and RFC-0053 independently keeps AssemblyScript out of the indexing path and refuses to
  claim byte-for-byte parity for mapping-derived values.
  **Rescoped 2026-09-12 after it was built and measured:** what RFC-0053 delivers is a *partial read
  surface* - exact for a subgraph's event-shaped fields, a named refusal for the rest - and not the
  impersonating endpoint this bullet rules out. On the pinned Uniswap V4 target it answers 70 of 184
  fields the port report classifies exact, and because GraphQL refuses a whole query for one
  unanswerable field, an analytics client cannot adopt it unmodified. This bullet's *instinct* was
  therefore better than the override credited it for; what the override bought is a real and narrower
  thing. See RFC-0053's Measured outcome.
- **Not automatic.** The skill produces a nest and a report for a human to accept. It does not deploy.

## 12. Open questions

1. **Where does the skill live?** `skills/nuthatch-subgraph-port/` in the nuthatch repo beside the
   builder skill, or in `claude-skills` with the rest of the private toolbox? The builder skill's
   precedent says the repo, and the repo makes it installable by a stranger, which is the point.
2. **How faithful should the query table be?** A per-operation SQL equivalent is honest and manual.
   A generated GraphQL surface over the nest is seductive and starts building a compatibility layer
   §11 rules out. Leaning hard on the first.
3. **Does the classifier need the mappings at all for the simple cases?** A schema plus a manifest
   may settle most fields without reading a line of AssemblyScript. Worth measuring on the three
   known ports before committing to the harder path.
4. **Multi-network manifests.** One nest per network is the obvious reading, but the report is then
   per-network too, and start blocks differ by an order of magnitude. Does the skill emit N reports
   or one with N columns?
