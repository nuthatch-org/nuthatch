# Subgraph stopgap: nests for subgraphs the network does not serve

**Opened 2026-10-06. Status: S0 measured and its gate failed, S2 done, BetSwirl's handback package written and its nest retired; the offer stands on demand.** This file is the central tracker. Issues carry the
detail and are labelled [`subgraph-stopgap`](https://github.com/nuthatch-org/nuthatch/labels/subgraph-stopgap).

## Why

On 2026-10-08 Studio traffic for BNB Smart Chain and Polygon starts moving onto The Graph network.
A deployment that no indexer picks up stops answering, and its developer finds out when their app
breaks.

We offered the Graph Foundation a stopgap in #subgraph-development on 2026-10-04: build a nest from
the subgraph's manifest so its event data keeps answering, and point the developer back at the
network once an indexer serves it. They replied on 2026-10-05 that they would follow up in DMs.
Chief decided on 2026-10-06 to build it now rather than wait for that conversation.

## What the offer is, and is not

A nest reproduces what a subgraph derives **from decoded events**: ids, addresses, block numbers,
timestamps, event parameters, `@derivedFrom` relations. Every field answers exactly or is refused by
name. Sums and counts over events can answer exactly where a view reproduces them, as BetSwirl's token
counters (`betCount`, `totalWagered`) do; that is decided field by field. Prices and other ordered,
mapping-derived values are not reproduced. GraphQL fails a whole query for one refused field, so an analytics dashboard will
usually not work unmodified, while bots, audit trails and event readers usually will.

This is **not a drop-in replacement** and must not be described as one anywhere.
[What the compatibility surface is](graph-compatibility-what-it-is.md) is the reference for the
boundary.

Two deliverables, in order of how certain their value is:

1. **The orphan list.** Which BNB and Polygon deployments have demand on chain and no indexer
   serving them. Useful to the Foundation whatever the stopgap turns out to cover.
2. **The stopgap nest.** One command from a deployment CID to a serving nest, with an honest
   per-field coverage report.

## What already exists

| piece | where | since |
| --- | --- | --- |
| manifest importer, `init --from-subgraph <CID>` | `src/subgraph_import.rs`, RFC-0038 | v2.6.0 |
| port skill and per-field report | `skills/nuthatch-subgraph-port`, `port-emit`, RFC-0044 | 2026-09-09 |
| Graph-dialect GraphQL read surface | `src/graph_query.rs`, RFC-0053 (parked, shipped parts supported) | 2026-09-12 |
| BNB and Polygon chain presets | `src/chains.rs` | |
| deployment and allocation state | `graph-allocations-nest` | |
| precedent: an unserved deployment ported | [doudouchain-v2-nest](https://github.com/nuthatch-org/doudouchain-v2-nest) | |

No slice below needs new capability in the default binary. RFC-0053 and RFC-0044 stay parked: this
programme fixes what the BNB and Polygon sample shows broken in what already shipped, and starts no
further slices of either.

## Slices

| slice | issue | what | fails if | status |
| --- | --- | --- | --- | --- |
| S0 | [#1940](https://github.com/nuthatch-org/nuthatch/issues/1940) | `init --from-subgraph` and `port-emit` on 15-20 published `bsc` and `matic` deployments, mixed shapes; record init success, fields answered, backfill cost | fewer than half the sample yield a serving nest that answers at least a third of its report-exact fields, or BetSwirl BNB Chain does not; the offer then narrows to S2 alone | measured 2026-10-06: **gate failed**, 0 of 21 from a CID alone ([report](subgraph-stopgap-s0.md)) |
| S1 | [#1941](https://github.com/nuthatch-org/nuthatch/issues/1941) | fix what S0 broke; CID to serving GraphQL surface end to end on both chains, golden tests from the sample | a sample deployment that should port does not | S0 failed; no decision on S1 is recorded here |
| S2 | [#1942](https://github.com/nuthatch-org/nuthatch/issues/1942) | resolve each manifest's `network:` from IPFS; view of `bsc`/`matic` deployments with signal or an indexing agreement and zero active allocations; alert on new entries | the list disagrees with the network subgraph's allocation counts at a pinned block | done: Lodestar's `/subgraphs/migration` is the list, verified exact at a pinned block; verifier and alert in #1945 |
| S3 | [#1943](https://github.com/nuthatch-org/nuthatch/issues/1943) | per-subgraph handback package: coverage report, how to run, how to point back at the network | someone who did not build it cannot stand one up from the package alone | BetSwirl BNB Chain's package written and cold-tested twice ([stopgap/betswirl-bnb.md](stopgap/betswirl-bnb.md)) |

S0 and S2 are independent and run in parallel. S0 is the slice that can stop the stopgap half.

## What the list says (2026-10-06)

Lodestar's directory: 114 bsc and 161 matic deployments carry signal and have no indexer. Only 3 earned
query fees in the last 30 days (Thena BSC V3 Fusion 483 GRT, BetSwirl BNB Chain 104 GRT, boost-polygon
~1 GRT), and 7 hold signal at or above REO's 500 GRT floor. None was created in the last 30 days, so
the Studio migration has not reached the list yet. How many Studio subgraphs move, and whether all are
published and curated, is the question for the Foundation.

Of the three, only **BetSwirl BNB Chain** (`Qmd5oqyojVx5wWSFuWfKz3YVLPHdE3KU5458Qqq3SVeGEB`) is genuinely
stranded: it is BetSwirl's only BNB deployment, and its Polygon twin is served by three indexers. Thena
and boost are old versions whose newer deployments are served (Thena BSC V3 Fusion `QmPBTDjp...` by
seven indexers, Boost Indexer Production by fourteen). BetSwirl BNB Chain is the first stopgap nest.

The handback package for it, tested by two cold runs from the page alone, is
[`stopgap/betswirl-bnb.md`](stopgap/betswirl-bnb.md).

**When it is served, a post goes up on [learn-thegraph.com/dispatches](https://learn-thegraph.com/dispatches/)**
covering the list, the numbers above, what the nest answers and what it refuses (Chief, 2026-10-06).

## Known limits

- **S2 sees only what is on chain.** A Studio deployment never published on chain is invisible to
  it. Only the Foundation can see those.
- **BNB backfill needs a paid archive endpoint.** The only working public BNB endpoint refuses
  history a million blocks back (`src/chains.rs`), and Polygon's archive coverage is narrow. S0
  measures the cost per subgraph before anything is promised.
- **Mapping logic is not run.** No AssemblyScript, anywhere, per RFC-0038 §8. Pricing and other
  ordered, mapping-derived fields are refused permanently, not pending.

## Decided

- **The stopgap runs on demand (Chief, 2026-10-07).** A nest is stood up when someone whose Studio
  subgraph lost service asks for one, not ahead of demand. The orphan alert keeps watching the chains Studio
  drops so we know who is at risk; the offer in the forum post and the dispatch is how they reach us. A hosted
  nest that goes unused is retired with a dated note, as BetSwirl was.
- **Who runs a stopgap nest when the developer cannot: we do.** Chief lifted the 2026-09-29
  no-hosting rule for this programme on 2026-10-06 (CLAUDE.md, out-of-scope list). Nests are served
  from the ThinkPad or new VPSes, stock binary, no billing, until an indexer allocates. The
  [fallback forum post](nuthatch-subgraph-fallback-forum-post.md)'s hosted-endpoint offer is consistent
  with this again, but scope it to unserved subgraphs before posting.

## Log

- **2026-10-06** Tracker opened, S0 to S3 filed as #1940 to #1943.
- **2026-10-06** Chief lifted the no-hosting rule for the stopgap: we serve the nests ourselves.
- **2026-10-06** S2 done (#1945). S0 gate raised from "any answered field" to a third of fields on half the sample, on review.
- **2026-10-06** S0 measured: the gate failed under both wordings ([report](subgraph-stopgap-s0.md)).
- **2026-10-06** Orphan alert live on the ThinkPad (user timer, 15 min, its own webhook). BetSwirl BNB Chain chosen as the first stopgap nest.
- **2026-10-07** BetSwirl BNB Chain retired by Chief: no users in its first day (about 198 requests in five hours, about 180 of them our own cache warm-up), no bet since March. Service stopped and disabled on the ThinkPad, data and mirror kept; the hosted URL answers 410 once `.deploy/helsinki-caddy-retire.sh` runs on Helsinki. The orphan alert keeps running.
