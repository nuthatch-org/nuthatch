# The Graph compatibility surface: what it is, and what it is not

*Read this before pointing a client at a nest. It takes two minutes and will tell you whether this is
any use to you.*

## What it is

A nest can serve a **GraphQL endpoint shaped like the subgraph you ported it from**, when nuthatch is
built with `--features graph`. The published binary is a default build and answers `/graphql` 404; the
routes are registered only behind that feature, off by default since 2026-09-21 (#1440). In that build
your client's schema introspection works, its queries parse, its filters and ordering and pagination
behave the way graph-node's do, and the values come back in graph-node's own wire types.

It is for the case where **a subgraph has stopped and nobody is going to fix it**. You point a client at
a nest instead and get back the part of your data that a nest can reproduce from decoded chain events,
exactly.

**The one guarantee that matters:** every field either answers *exactly*, or is *refused by name with a
reason*. There is no third behaviour. A nest will not hand you a plausible substitute - not a default, not
an unfiltered set where you asked for a filter, not an empty list where the schema promises a non-null
one. If it cannot reproduce a value, it says so and the query fails.

## What it is not

**It is not a drop-in replacement.** The name was wrong and has been retired. Most clients cannot adopt
it unmodified, for a reason worth understanding before you try.

**GraphQL refuses a whole query for one unanswerable field.** There is no partial-answer mode. If your
query names five fields and a nest cannot answer one of them, you get an error, not the other four. So
"a nest answers 38% of fields" does **not** mean "38% of your queries work". It can mean none of them do.

**The fields it cannot answer are, in general, the interesting ones.** The split is not random. It falls
along a line:

| answers | does not answer |
| --- | --- |
| ids, timestamps, block numbers, log indices | prices and anything derived from one |
| addresses, token ids, transaction hashes | priced totals - volume in USD, fees, TVL |
| values taken straight from an event's parameters | values from ordered, stateful mapping code |
| sums and counts over events, where a view reproduces them | |
| `@derivedFrom` relations between the above | day and hour aggregate entities |

On Uniswap V4 that means `Swap.id`, `pool`, `sender`, `timestamp` and `logIndex` answer, while
`Swap.amount0`, `amountUSD`, `Token.symbol`, `Pool.volumeUSD` and `Pool.totalValueLockedUSD` do not, and
`PoolDayData` has no table at all.

**Pricing will never be exact**, and that is a design decision rather than unfinished work. Values like
`derivedETH` and `totalValueLockedUSD` are the output of ordered, stateful mapping code. Reproducing them
byte-for-byte means running the original mappings, which is a second indexer.
[RFC-0038 §6a](rfcs/0038-subgraph-parity.md) refuses that, deliberately. A nest is a compiler over
stored state.

## Is it any use to you?

**Yes, if** you want the event record: every Swap on a pool since some block, with sender, transaction
hash and log index. Bots, reconciliation, audit trails, backfills, anything reading what happened rather
than what it was worth. You get exact answers over the full history, hot storage and sealed Parquet
alike.

**Probably not, if** you are backing a dashboard. Most numbers on one are prices or priced totals, and
one refused field fails the whole query. Event-derived counts and sums are decided field by field:
BetSwirl's SDK documents, token counters included, answer in full
([stopgap/betswirl-bnb.md](stopgap/betswirl-bnb.md)).

**Find out**, rather than guessing. Scaffold the nest with `nuthatch init --from-subgraph <CID>`, then
run `nuthatch port-emit --dir <subgraph source> --out <nest>`, which prints a coverage line and writes
it into the nest's `README.md`. `--dir` needs the subgraph's `schema.graphql` and AssemblyScript
mappings: a deployment CID holds only compiled WASM, so get the source from the subgraph's repository
([how the Uniswap V4 port pinned it](port-uniswap-v4-mainnet.md)). Aimed at a CID's files alone, every
field but `@derivedFrom` reads as unanswered. For the pinned Uniswap V4 port, measured 2026-09-12:

```
coverage: 70 of 184 fields the report calls exact are answered (38%): 54 in views,
          2 maintained incrementally, 14 derived by reverse lookup, 114 not answered at all
```

Every unanswered field is named with its reason, in the `-- NOT IN THIS VIEW` comments of the relevant
`views/*.sql` and on stdout when `port-emit` ran. **Check your own queries' fields against that list
before you migrate anything.** The number at the top is not the number that matters; whether *your*
fields are in it is.

The outcome depends far more on the shape of your subgraph than on how much of this we have built. A
subgraph whose entities mirror events ports well - the second pinned target, Carbon, answered 44% with
no priced fields at all when measured on 2026-09-12. A subgraph built for analytics does not.

## What is accepted and refused, exactly

[The accepted dialect](graph-query-dialect-accepted.md) lists every query shape that compiles and every
one that is refused, with the reason for each refusal. Nothing there is approximated: a shape that is not
listed as accepted is refused by name.

## Where the design record lives

[RFC-0053](rfcs/0053-graph-subgraph-compatibility.md) - in particular its **Measured outcome** section,
which carries the figures, what was delivered against what was intended, and which parts of the ceiling
are permanent.
