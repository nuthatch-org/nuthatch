# Check a subgraph locally before publishing

*Asked in The Graph's Discord on 2026-11-11, after Studio stopped test-indexing more chains: "Could
Nuthatch possibly be your own studio prior to deploying?"*

**Partly.** A nest will tell you, from your own RPC and in under a minute, whether the manifest, ABIs,
addresses, start blocks and events of a subgraph are right. It will not tell you whether your mappings
are right, because it never runs them. Studio's test indexing did, and that was most of its value.

Everything below was measured on 2026-11-11 with the published 5.19.0 binary and graph-cli 0.98.1, on
the scaffold `graph init --from-contract` writes for one mainnet ERC-721
(`0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D`, four events).

## What it checks

### 1. The manifest, the ABIs and the events

```
nuthatch init --from-subgraph ./subgraph.yaml --dir nest
nuthatch dev --dir nest --backfill 200000
```

`init` reads the local manifest and the ABI files it names, keeps each data source's address, start
block and event list, and writes one SQL table per event. Nothing is fetched and nothing is published.
`dev` then indexes those events from the RPC in `nest/nuthatch.toml`. `--backfill` bounds the run to
the last N blocks, so the check does not wait on the contract's whole history.

On the scaffold above, 200,000 blocks were at tip 33 s after `dev` started, on the keyless endpoints
`init` writes. The three tables with events in that range held 1,578, 1,645 and 634 rows, the same
counts a raw `eth_getLogs` over the same range returned per topic.

```
curl -s localhost:8288/sql -H 'content-type: application/json' \
  -d '{"q": "select * from bayc__transfer order by block_number desc limit 5"}'
```

What a failure here means is specific. A manifest that does not parse, an ABI path that does not
resolve or an event signature the ABI does not hold stops `init`. A wrong address or a start block
after the events gives a table with no rows.

### 2. What each entity field is made of

```
nuthatch port-report --dir .
```

Run in the subgraph directory. It reads `schema.graphql` and the AssemblyScript mappings and prints,
for every entity field, the mapping line that assigns it and one of four classes: exact (a pure
function of decoded events), call-derived (reads contract state), fixed point (reads back stored
entities) or unreachable. It writes nothing and takes a few milliseconds.

It reads the mappings. It does not run them. With `from` deliberately assigned from
`event.params.to`, the report still classed the field exact and printed
`assigned from event.params.to` on its row: a reader can see the mistake there, and nothing flags it.

### 3. The entities, by their subgraph names, over SQL

```
nuthatch port-emit --dir . --out nest
```

This writes one view per entity into `nest/views/`, carrying the exact fields under the names the
schema gives them, so `select * from transfer` returns `from`, `to`, `tokenId`, `blockNumber`,
`blockTimestamp` and `transactionHash`. A field it cannot render is named in a comment at the top of
the view with the reason.

`port-report` and `port-emit` are authoring tools and are not listed in `nuthatch --help`.

## What it does not check

- **The mappings never run.** [RFC-0038 §8](rfcs/0038-subgraph-parity.md) rules out executing
  AssemblyScript, in the indexing path or anywhere else. A handler that throws, divides by zero, loads
  an entity that is not there or computes the wrong number is invisible here.
- **There is no indexing-error surface.** Nothing reports a failed handler, because no handler ran. In
  the `nuthatch-graph` build `_meta { hasIndexingErrors }` is always `false`.
- **Call-derived and fixed-point fields are classified, not computed.** The report tells you which
  fields they are. Their values are not checked.
- **It is not a GraphQL test endpoint.** The `nuthatch-graph` build serves `/subgraphs/id/…`, but on
  this scaffold 5.19.0 answered no entity query at all: `graph init` gives every entity the id
  `event.transaction.hash.concatI32(event.logIndex.toI32())`, no decoded column corresponds to it, and
  `port-emit` reported `0 of 27 fields` answered (#2843). From 5.21 the `nuthatch-graph` build's
  `port-emit` renders that id from `tx_hash` and `log_index` as the hex graph-node serves, and answers
  all 27; the default build's still reports 0. On hand-written subgraphs the pinned ports answered 38% (Uniswap V4) and
  44% (Carbon) of exact fields, and a query naming one refused field fails whole. [What the compatibility surface is, and is not](graph-compatibility-what-it-is.md) has
  the detail.

So a clean run here means the subgraph is pointed at the right contract, from the right block, with
ABIs that decode what the contract emits. It does not mean the subgraph will index without error.
For the handlers themselves, [Matchstick](https://thegraph.com/docs/en/subgraphs/developing/creating/unit-testing-framework/)
unit tests or a local graph-node are still the tools.
