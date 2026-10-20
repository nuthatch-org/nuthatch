# Lodestar migration inventory

**Issue #1074.** The denominator for "how far through the Lodestar migration are we".

Produced 2026-09-01 against `~/Projects/lodestar` at `ee78088`, and `~/Projects/graph-allocations-nest`.

## What is settled here, and what is not

**Settled, by reading the code:** which files reach a gateway, *which* subgraph each one reaches,
what entities they ask for, what views exist, and what the nest declares. Every count below is from
a command that can be re-run, and the commands are quoted.

**Not settled here:** whether a given view actually *answers* a given route's query. That is a
per-field claim and it is #1076's job, at a pinned block. Nothing in this file should be read as
parity evidence. Where a row says a view exists, it means a file with that name exists in
`graph-allocations-nest/views/`, which is a weaker statement than it looks.

## 1. There are six subgraphs, not one

This is the finding that changes the shape of the problem. `src/lib/subgraph.ts` presents one module
and fronts **six distinct gateway subgraphs**, each with its own remit answer:

| client fn | subgraph | what it is | consumers |
| --- | --- | --- | ---: |
| `subgraphQuery` | `DZz4…qmp` | **Graph Network on Arbitrum One.** The migration target. | **37** |
| `ensQuery` | `5XqP…GtH` | ENS names on Ethereum **mainnet**, reverse resolution | 5 |
| `delegationEventsQuery` | `4LLz…UWds` | Paolo Diomede's discrete delegation/undelegation events | 3 |
| `qosOracleQuery` | `CnfJ…kbN3` | Ellipfra's fork of the Gateway QoS Oracle | 2 |
| `horizonPerfQuery` | `eD1T…8Y4h` | community Horizon indexer-performance timeseries | 1 |
| `dispatchRegistryQuery` | `6qhp…iJz1` | Dispatch JSON-RPC indexer registry | **0** |

Two things follow immediately.

**`dispatchRegistryQuery` has no consumers.** It is exported, it builds a gateway URL from
`GRAPH_API_KEY`, and nothing in `src` or `scripts` calls it. It is dead client code inflating every
gateway-reference count anyone has quoted.

**"Migrate off the subgraph" was never one job.** ENS is a different chain. The QoS oracle is a
different data category. The delegation-events subgraph is a third party's. Each needs its own
answer, and #1078's framing of a single `src/lib/subgraph.ts` seam quietly assumed one.

## 2. The real size

```sh
rg -l "subgraphQuery|ensQuery|horizonPerfQuery|delegationEventsQuery|qosOracleQuery|GRAPH_API_KEY" \
  src scripts --glob '*.ts' --glob '*.tsx' | grep -v __tests__ | grep -v 'lib/subgraph.ts'
```

**56 non-test files** touch a gateway. Fourteen of them never import the shared client and reach
`GRAPH_API_KEY` directly, which is why every sweep anchored on the client has undercounted.

For comparison: #1078's surface table names **7**. #1086 adds **8**. #1079 quotes "37 files touching
the gateway" and calls that figure alarming and misleading. The alarming-and-misleading figure was
itself low by 19.

**Already on nuthatch: 11 files.** `api/sql/{query,named,catalog,receipt}`, `api/delegation-events`,
`api/delegation-flows`, `api/developer-activity`, `api/dips`, `cron/check-dips`,
`lib/notifications/dips.ts`, and `lib/nuthatch.ts` itself.

**No file uses both.** The intersection of the gateway set and the nuthatch set is empty. There is no
dual-source surface anywhere in Lodestar, which is the mechanical reason #1080 matters: every switch
performed so far has been a replacement, not a fallback.

## 3. Group C: the migration

**Scope, stated explicitly, because the first two drafts left it ambiguous.** Group C is *on-chain
state Lodestar reads, whichever subgraph currently serves it* - not "surfaces of the Graph Network
subgraph". nuthatch indexes chains, not subgraphs, so which subgraph a fact arrives through today is
an implementation detail of the thing being replaced.

That admits two sets a Graph-Network-only reading would have excluded: the **delegation-events**
subgraph, a third party's, indexing the same Arbitrum staking contracts `graph-allocations-nest`
already declares; and the **QoS oracle**, which §5 establishes is on chain by construction.

Excluded, with reasons rather than by omission: **group A**, where the gateway itself is the subject;
**group B's off-chain half**, where the meaning behind a hash lives on IPFS; and **ENS**, which is
Ethereum mainnet and therefore a different chain, a different cursor, and a different nest.

This is the numerator and the denominator both.

| # | surface | entities read | on-chain | nest status | blocker |
| ---: | --- | --- | --- | --- | --- |
| 1 | `api/network-stats` | `graphNetwork`, `block` | yes | `80-lodestar-network.sql` | nest undeployed (#1075) |
| 2 | `api/grt-flow` | `graphNetwork` | yes | `80-lodestar-network.sql` | nest undeployed |
| 3 | `lib/ingest/network-snapshot.ts` | `graphNetwork` | yes | `80-lodestar-network.sql` | nest undeployed |
| 4 | `api/epochs` | `epoches` | yes | `50-lodestar-epochs.sql` | nest undeployed |
| 5 | `api/token-metrics` | `epoches` | yes | `50-lodestar-epochs.sql` | nest undeployed |
| 6 | `lib/ingest/epochs.ts` | `epoches` | yes | `50-lodestar-epochs.sql` | nest undeployed |
| 7 | `lib/ingest/allocations.ts` | `allocations` | yes | `40-lodestar-allocations.sql` | nest undeployed |
| 8 | `api/poi` | `allocations`, `indexer` | yes | `40-lodestar-allocations.sql` | nest undeployed |
| 9 | `api/cron/tap-provision` | `allocations` | yes | `40-lodestar-allocations.sql` | nest undeployed |
| 10 | `lib/ingest/disputes.ts` | `disputes`, `fisherman` | yes | `60-lodestar-disputes.sql` | nest undeployed |
| 11 | `lib/ingest/rav.ts` | `paymentsEscrowTransactions` | yes | `70-lodestar-escrow.sql` | nest undeployed |
| 12 | `api/payments` | `paymentsEscrow*`, `graphTallyTokensCollecteds` | yes | `70-lodestar-escrow.sql` | nest undeployed |
| 13 | `api/indexers` | `indexers`, `account` | yes | **no view** | view + nest |
| 14 | `api/indexer-stake-history/[address]` | staking history | yes | **no view** | view + nest |
| 15 | `api/rewards-history` | `indexer`, `stakes`, `delegator` | yes | **no view** | view + nest (#1082) |
| 16 | `api/curators` | `curators` | yes | **no view** | view + nest |
| 17 | `lib/ingest/delegations.ts` | `delegationEvents` (3rd-party sg) | yes | **no view** | view + nest (#1082) |
| 18 | `api/cron/ingest-horizon-activity` | `delegationEvents`, `provisions` | yes | **no view** | view + nest (#1084) |
| 19 | `api/provisions` | `provisions` + ENS | **mixed** | **no view** | split first |
| 20 | `api/indexer/[address]` | indexer + delegators + metadata | **mixed** | partial | split first |
| 21 | `api/portfolio` | delegator/stakes/signals + ENS | **mixed** | partial | split first |
| 22 | `api/apr-provenance/[address]` | `indexer` + ENS | **mixed** | partial | split first |
| 23 | `api/feed` | network activity | yes | **no view** | unclassified, needs a read |
| 24 | `api/cron/check-conversions` | direct `GRAPH_API_KEY` | ? | **no view** | unclassified, needs a read |
| 25 | `lib/protocols/fetcher.ts` | direct `GRAPH_API_KEY` | ? | **no view** | unclassified, needs a read |
| 26 | `lib/refresh.ts` | 11 entities across 3 subgraphs | **mixed** | partial | split first |
| 27 | `lib/ingest/qos.ts` | `allocationDailyDataPoints`, `queryDailyDataPoints` | yes (oracle) | **no view** | decode shape undecided (#1083) |
| 28 | `api/indexer-qos/[address]` | QoS oracle daily grain | yes (oracle) | **no view** | decode shape undecided (#1083) |
| 29 | `app/indexers/[address]/opengraph-image.tsx` | `account`, `indexer` | yes | **no view** | view + nest |

**Twenty-nine surfaces, one per row**, partitioned on the nest-status column: **12** with a view file
of the right shape; **12** needing a view written, of which three nobody has read yet and two are the
QoS pair blocked on the shape question rather than on effort; and **5** mixed, unclassifiable until
the on-chain half is split from the ENS/IPFS half.

**One row, one surface, deliberately.** An earlier draft carried `scripts/backfill.ts` and
`backfill-rav.ts` as a single row naming two files, which made "rows" and "surfaces" different words
for different numbers and left the appendix checker unable to validate the total it was checking.
They are excluded now, for the reason already applied to `scripts/backfill-qos.ts` in §5: a backfill
script is a **runner** for an ingestion module already counted here, not a data dependency of its
own. Migrate `lib/ingest/rav.ts` and `backfill-rav.ts` follows it; counting both would double-count
one dependency and flatter the denominator.

**The blocker column names the *next* blocker, not the only one**, and "a view exists" is the first
of three steps rather than a state of near-readiness. Each of the 12 still needs: the nest deployed
(#1075), its fields proven against the subgraph it replaces at a pinned block (#1076), and the route
or module switched to read it (#1078). **Twelve** of these twenty-nine are request-time handlers
calling `subgraphQuery` with no database module between the user and the gateway (§6a), so **deploying
the nest migrates none of them on its own** - it only makes the next step possible. (The 21 in §6a is
a count over the 27 API routes, not over this group: nine of the 21 are group A or group B, and group
C also contains ingestion modules that are not routes at all.)

So the honest figure today, against group C, is **0 of 29 migrated**, with 12 having cleared the
first of three steps. Not a percentage anyone should publish yet, because the five mixed rows can
move the denominator in either direction once split, and "has a view" is the weakest of the three
things a migrated surface needs.

### `api/subgraph` is not in this group, and is not a surface at all

It reads as the obvious seam - it is pinned to the Graph Network subgraph and returns `graphNetwork`
aggregates - and the first draft of this file listed it as group C on that basis. Reading it settles
it differently:

```ts
export async function POST(request: NextRequest) {
  // In production, block direct subgraph proxy - all queries must go through cached GET endpoints
  if (process.env.NODE_ENV === 'production') {
    return NextResponse.json({ error: 'Direct subgraph queries are disabled...' }, { status: 403 });
  }
```

**It returns 403 in production on every request.** It is development-only tooling, it carries no
production gateway dependency, and migrating it would migrate nothing. It is excluded from the
denominator rather than counted as remaining work.

Two things about it are worth recording even so, because they are the sprint's own theme wearing a
different hat. The file is 588 lines, of which roughly 320 are **hand-written mock data** - invented
GraphOps and Stake.fish indexers with fabricated stake figures. And at line 299 a real GraphQL error
from the gateway does not fail: it logs and **falls through to that mock data**, so in development a
broken query renders as a healthy network. Neither reaches production. Both are the pattern this
project keeps finding in its own tests, and they sit in the one route a developer is most likely to
trust while checking whether a migration worked.

## 4. Group B: chain-derived hash, off-chain meaning

On chain you get an IPFS hash. The name, the schema and the manifest behind it do not exist on chain,
and non-negotiable 1 forbids IPFS at runtime, deliberately.

`api/subgraph-names`, `api/subgraph-search`, `api/subgraph-versions/[hash]`,
`api/subgraph-deployments`, `api/subgraph-history/[hash]`, `api/subgraph-curation/[hash]`,
`api/subgraph-fees-30d`, `lib/disassembly/signal.ts`, `lib/disassembly/source-hint.ts`,
`app/subgraphs/[hash]/opengraph-image.tsx`, `api/ens`.

**Eleven surfaces, and most are mixed rather than out of remit.** `subgraph-curation` and
`subgraph-fees-30d` read `curatorSignals` and `indexerAllocations`, which are on chain and in the
nest's declared `curation` contract; only the display name beside them is not. #1079 lists group B as
two files. It is eleven, and the interesting half of each is migratable.

## 5. Group A: the gateway is the subject

Correct as they are. A subgraph playground with no subgraph is a deleted feature.

`api/subgraph-playground/[hash]`, `api/x402/query`, `api/studio/query/[id]`, `api/bounty-query/[id]`,
`api/gateway/[key]`, `lib/gateway-probe.ts`, `components/SubgraphGraphiQL.tsx`,
`api/indexer-status/[address]`, `api/indexing-status/[hash]`, `cron/check-subgraph-health`,
`api/indexer-trends`.

**Eleven surfaces**, four more than #1079's seven, and it is a decision rather than an oversight in
two of them: `indexer-status` and `indexing-status` query indexers' own `/status` endpoints, which is
serving telemetry, not chain state.

### The QoS question, which #1083 gets backwards - and which is why QoS is in group C, not here

This is the one classification that moved between drafts, so the reasoning is recorded rather than
just the answer.

#1083 argues `qos.ts` is out of remit because quality-of-service "never existed on chain". Read, it
does not query the gateway's telemetry API at all. It queries **a subgraph of the Gateway QoS
Oracle** (`allocationDailyDataPoints`, `queryDailyDataPoints`), and an oracle subgraph exists because
an oracle **posts to chain**.

So the data is on chain by construction. The real question is not remit but **shape**: whether the
oracle's payload arrives as decodable events or as opaque calldata, because nuthatch decodes
topic0-keyed events and nothing else. That wants checking against the oracle contract rather than
reasoning about.

**Which is why `lib/ingest/qos.ts` and `api/indexer-qos/[address]` are counted in group C**, with a
blocker of "decode shape undecided", and not parked here as out of remit. A surface whose data is on
chain belongs in the denominator even when we do not yet know how to reach it; putting it in group A
would make the goal look closer than it is, which is precisely what §3's scope statement exists to
prevent. `scripts/backfill-qos.ts` is a runner for `lib/ingest/qos.ts` rather than a surface of its
own, so it is not counted separately.

## 6. The crons, which are the live dependency

`vercel.json` schedules **19 crons**. Eleven are gateway-backed:

| cron | every | source |
| --- | --- | --- |
| `ingest-horizon-activity` | **2 min** | Graph Network + delegation events |
| `refresh` | 5 min | three subgraphs |
| `snapshot-network` | 5 min | Graph Network |
| `tap-provision` | 5 min | Graph Network |
| `ingest-epochs` | 10 min | Graph Network |
| `ingest-delegations` | 15 min | delegation events |
| `check-subgraph-health` | 15 min | Graph Network (group A) |
| `ingest-allocations` | 1 h | Graph Network |
| `ingest-rav` | 1 h | Graph Network |
| `ingest-disputes` | 6 h | Graph Network |
| `ingest-qos` | 6 h | QoS oracle |

**Something needs settling here.** #1081 records that `scripts/cron-runner.ts` runs on the droplet
under system cron, and `vercel.json` schedules the same ingestion as Vercel crons. Either both run,
or one is vestigial. Nobody has written down which, and it matters for #1081: you cannot hand over an
ingestion path you cannot name.

## 6a. The seam is not only the ingestion layer

[`docs/audits/2026-09-plan.md`](https://github.com/nuthatch-org/nuthatch/blob/ede03ca7/docs/audits/2026-09-plan.md) §6 states the architecture as:

```
subgraph APIs  ->  scripts/cron-runner.ts (droplet, system cron)  ->  Postgres  ->  Next.js UI
```

and concludes *"the UI mostly reads Postgres. So the migration seam is the ingestion layer, not the
API routes - a conclusion reached only after first writing the issue the other way round."*

**The first framing was the right one.** Counted:

```sh
rg -l "\bsubgraphQuery\b" src/app/api --glob '*.ts' | grep -v __tests__   # 27 routes
```

**21 of those 27 routes reference no database module at all** - no `@/lib/db` and no
`@/lib/studio/db`, the only two database import paths in `src/app/api`. The two lists partition the
27 exactly:

*No database module (21).* `cron/ingest-horizon-activity`, `curators`, `epochs`, `grt-flow`,
`indexer-stake-history/[address]`, `indexer-status/[address]`, `indexer/[address]`, `indexers`,
`indexing-status/[hash]`, `network-stats`, `payments`, `poi`, `portfolio`, `provisions`,
`subgraph-curation/[hash]`, `subgraph-deployments`, `subgraph-fees-30d`, `subgraph-history/[hash]`,
`subgraph-names`, `subgraph-search`, `subgraph-versions/[hash]`.

*Has a database module (6).* `apr-provenance/[address]`, `bounty-query/[id]`,
`cron/check-subgraph-health`, `cron/tap-provision`, `rewards-history`, `token-metrics`.

Spot-checked three of the 21 by hand: `network-stats:51`, `curators:44` and `epochs:19` each `await
subgraphQuery(...)` in the request handler with nothing between the user and the gateway.

**Split by group, because the 21 is not all migration work.** Two are group A and correctly stay
(`indexer-status`, `indexing-status`, which query indexers' own `/status` endpoints). Seven are the
group B `subgraph-*` metadata routes, whose on-chain half is migratable and whose display names are
not. **Twelve are group C network state**: `cron/ingest-horizon-activity`, `curators`, `epochs`,
`grt-flow`, `indexer-stake-history`, `indexer/[address]`, `indexers`, `network-stats`, `payments`,
`poi`, `portfolio`, `provisions`.

The consequence is concrete and it is the sprint's central question: **migrating the ingestion layer
alone leaves 21 request-time gateway dependencies standing, 12 of them network state.** A completion
claim resting on the nine `src/lib/ingest/*` modules would be a claim about the cron path only. That
is the failure #1086 describes, and #1086's eight is an undercount against these twelve.

**What those routes actually do without the key**, checked rather than assumed, because "it would
break" is not a finding and the answer turned out to matter:

```sh
for f in $(cat /tmp/nodb.txt); do grep -A3 "if (!hasSubgraphAccess())" "$f" | head -4; done
```

All 21 carry a `hasSubgraphAccess()` guard. **Twenty return `503 {"error": "No API key
configured"}`** - a visible, honest failure.

**One does not.** `api/subgraph-names`:

```ts
if (!hasSubgraphAccess()) return NextResponse.json({ data: {} });
```

**`200`, with an empty body.** It is the route that resolves deployment IPFS hashes to display names,
so with no key every subgraph name on the site silently becomes blank and nothing anywhere reports a
fault. That is this project's most-recorded defect - an absence rendering as health - sitting in
production code on the one route whose failure is least visible, because a missing name looks like a
subgraph that has no name.

Worth its own issue rather than a line here. Filed as #1097; the same sweep then found #1103
(`api/ens` 200 `{ ensName: null }`) and #1104 (`api/token-metrics` 200 `{ data: [] }`).

This is offered as the §6 deliverable the audit plan asks for - "find a migration-relevant thing with
no issue" - and it wants filing rather than leaving here.

### Decision (#1092): nest-direct

Taken 2026-09-02. The 12 group-C request-time routes migrate onto a nest, not onto Postgres first.

Postgres-cache-first would be a new cache layer those routes do not have, filled from the gateway,
and only later from a nest. After that first step, removing `GRAPH_API_KEY` would not 503 the pages:
it would serve whatever Postgres last wrote, with no freshness. #1080 forbids that. Completing the
nine `src/lib/ingest/*` modules does not touch these routes at all, which is the failure the issue
describes.

The five surfaces already on a nest are nest-direct and fail visibly without a configured origin.
That is the pattern, and #1080 already wrote the serving rules in front of it (`/ready` before
answering, freshness on the response, no Graph fallback). No further switch until those three are
implemented.

**#1078's done-state is therefore:** every group-C surface, including these 12 request-time routes,
served by a nest, with that readiness gate in front. Not "the ingest modules switched". The 6 routes
that already read Postgres keep that path; their crons are the ingest job. Group A stays on the
gateway. Group B's display names stay.

The issue title said 22 of 27. Re-measured on 2026-09-02 against `~/Projects/lodestar` at `fa18fc4`,
same commands as the appendix: **21 of 27** have no database module, and they partition the 27 with
the 6 that do. The body listed those 21 and then six database-touching routes as "five".
`token-metrics` is the interesting extra: it has a database module and still falls through to the
gateway on a miss (#1104).

## 7. What this changes about the open issues

- **#1074 is not board-only and needs no GraphOps conversation.** The repository is on this laptop.
  This file is the deliverable; what remains is reading the five mixed and three unclassified rows.
- **#1078's table is 7 rows of a 29-row group.** Its `src/lib/subgraph.ts` seam is real but is one of
  six clients, so a nuthatch-backed implementation behind it migrates the Graph Network surfaces and
  touches none of ENS, QoS or delegation events. **Done is not the ingest modules.** Per the §6a
  decision, done is every group-C surface served by a nest, including the 12 request-time routes,
  nest-direct, behind the #1080 readiness gate. The 6 Postgres-reading routes keep that path and
  migrate with their crons.
- **#1079 should cover 11 group-A surfaces and 11 group-B, not 7 and 2**, and should say for each
  mixed row which half stays.
- **#1082 is bigger than `delegations`.** **Twelve** group-C surfaces have no view at all, not one,
  and a further **five** are the mixed rows whose on-chain half has no view either - so seventeen of
  the twenty-nine are short of one, against twelve that have a view file of the right shape. #1082
  names `delegations` and the escrow asymmetry; those are two of the twelve.
- **#1083's premise needs replacing** with the shape question in §5.
- **#1086's eight routes are correct and incomplete**; the direct-`GRAPH_API_KEY` set adds fourteen
  more files it did not see, and §6a raises its 8 request-time routes to 12 group-C ones (21 in all).
- **The audit plan's own module table undercounts by construction.** It lists nine `src/lib/ingest/*`
  modules and their gateway-reference counts. Those nine are real, and they are 9 of 56.
- **#1080 has already been answered once, by accident.** `lib/nuthatch.ts` records that the panels
  migrated in 4.26.0 "need a configured Nuthatch origin and fail visibly without one, with no
  alternate Graph source". Fail-visibly is therefore the *existing* policy for migrated surfaces, not
  an open choice, and `NUTHATCH_DIPS`'s "no fallback exists" is that policy rather than an exception.
  #1080's real question is whether it survives contact with group C's 29.

## 8. The denominator, stated

**"100% of Lodestar's on-chain network state, served by nuthatch nests"** = group C, 29 surfaces
today, minus whatever the five mixed rows shed on splitting.

Explicitly **inside** it, provisionally: the **two QoS surfaces**, on the §5 reasoning that an oracle
subgraph exists because an oracle posts to chain. Their blocker is a decode-shape question, not a
remit one, and until that is answered against the oracle contract they are counted as remaining work.
If the payload turns out not to be event-shaped they leave the denominator and it becomes 27, so the
figure is stated with that caveat rather than by quietly picking whichever answer is smaller. The
same rule applies as everywhere else here: a surface whose data is on chain stays in the denominator
even when we do not yet know how to reach it.

Explicitly outside it, and finished by being correct: group A's 11, group B's off-chain half, and
ENS - Ethereum mainnet, and therefore a different chain, cursor and nest.

Explicitly **not** the goal: zero `GRAPH_API_KEY` in the repository. Per #638, the goal is that the
key is not load-bearing for Lodestar's own dashboard. Fifty-six files touch a gateway and roughly
twenty of them always will.

## Appendix: how to re-derive every count

Run from `~/Projects/lodestar` unless stated. Every headline figure in this document came from one of
these, re-run and confirmed on 2026-09-01 at `ee78088`.

**Two traps, both of which produced a confident zero while writing this.** `rg` on this machine is a
shell function from Claude Code's snapshot rather than a binary, so it does not exist inside a spawned
`bash script.sh`; and zsh does not word-split, so passing `"src scripts"` as one variable searches a
path of that name and finds nothing. A probe that errors prints `0` and reads exactly like a real
zero. `dispatchRegistryQuery: 0` below is a **real** zero, and the way to know that is that the same
command returns 56, 37, 5, 3, 2 and 1 for its siblings.

```sh
# The helper every count below uses. Paths literal, never via one variable.
n() { rg -l "$1" src scripts --glob '*.ts' --glob '*.tsx' \
        | grep -v __tests__ | grep -v 'lib/subgraph.ts' | sort -u | wc -l; }

n 'subgraphQuery|ensQuery|horizonPerfQuery|delegationEventsQuery|qosOracleQuery|GRAPH_API_KEY'  # 56
n '\bsubgraphQuery\b'                                                                          # 37
n '\bensQuery\b'                                                                               #  5
n '\bdelegationEventsQuery\b'                                                                  #  3
n '\bqosOracleQuery\b'                                                                         #  2
n '\bhorizonPerfQuery\b'                                                                       #  1
n '\bdispatchRegistryQuery\b'                                                                  #  0
```

```sh
# The 14 that reach GRAPH_API_KEY without importing the shared client - why client-anchored
# sweeps undercount. Note the THREE import styles: an earlier version of this command knew only
# `@/lib/subgraph` and would have miscounted the eight files using a relative path.
rg -l 'GRAPH_API_KEY' src scripts --glob '*.ts' --glob '*.tsx' \
  | grep -v __tests__ | grep -v 'lib/subgraph.ts' | sort > /tmp/gk.txt      # 17 reference the key
while read f; do
  grep -qE "from ['\"](@/lib/subgraph|\.\./subgraph|\./subgraph)['\"]" "$f" || echo "$f"
done < /tmp/gk.txt                                                          # 14 do so directly

# The import styles actually present, so the alternation above can be checked rather than trusted:
rg -o --no-filename "from ['\"][^'\"]*subgraph['\"]" src scripts --glob '*.ts' --glob '*.tsx' \
  | sort | uniq -c        # 40 @/lib/subgraph, 7 ../subgraph, 1 ./subgraph
```

An earlier draft derived this set with a `comm` of the with-key and without-key symbol sweeps. That
computes "references `GRAPH_API_KEY` and none of the five query symbols", which is a different claim:
a file could do both and be wrongly excluded. The membership happened to be identical - the same 14
files - but the command did not compute the sentence attached to it, which is the difference between
a reproducible count and a lucky one.

```sh
# The 27 API routes, and the 21/6 request-time partition of §6a.
rg -l '\bsubgraphQuery\b' src/app/api --glob '*.ts' | grep -v __tests__ | sort > /tmp/sq.txt   # 27
: > /tmp/db.txt; : > /tmp/nodb.txt      # truncate first: the loop appends, so a second run
                                       # without this doubles both files and they stop partitioning
while read f; do
  if grep -qE "from '@/lib/(studio/)?db'" "$f"; then echo "$f" >> /tmp/db.txt
  else echo "$f" >> /tmp/nodb.txt; fi
done < /tmp/sq.txt        # nodb 21, db 6
wc -l < /tmp/db.txt; wc -l < /tmp/nodb.txt   # 6 and 21; assert they sum to 27 before using either

# `@/lib/db` and `@/lib/studio/db` are the only two database import paths under src/app/api:
rg -o --no-filename "from '[^']*(db|database|postgres|pg)[^']*'" src/app/api --glob '*.ts' | sort -u
```

```sh
# Already served by a nest (11), and the crons (19).
rg -l 'nuthatchSql|nuthatchEnabled|hasNuthatch' src --glob '*.ts' --glob '*.tsx' | grep -v __tests__
jq '.crons | length' vercel.json

# api/subgraph's production guard, and its size.
grep -n "NODE_ENV === 'production'" src/app/api/subgraph/route.ts && wc -l src/app/api/subgraph/route.ts
```

**The group C partition (12 / 12 / 5 over 29 rows) is derived from this file's own table**, so it
is checkable against the document rather than the repo. Run from the nuthatch repo:

```sh
awk -F'|' '
  /## 3\. Group C/ { s = 1 }
  /\*\*Twenty-nine surfaces/ { s = 0 }
  !s || !/^\| / { next }
  { k = $2; gsub(/^[ \t]+|[ \t]+$/, "", k) }
  k !~ /^[0-9]+$/ { next }
  {
    rows++; nest = $6; onchain = $5
    gsub(/^[ \t]+|[ \t]+$/, "", nest); gsub(/^[ \t]+|[ \t]+$/, "", onchain)
    if ($3 ~ /,/) multi = multi "\n" $0
    if (index(nest, ".sql`")) covered++
    else if (nest == "**no view**" && onchain == "**mixed**") mixed++
    else if (nest == "**no view**") noview++
    else if (nest == "partial") mixed++
    else unclassified++
  }
  END {
    printf "covered %d, needs a view %d, mixed %d, unclassified %d, sum %d, rows %d\n",
      covered, noview, mixed, unclassified, covered + noview + mixed + unclassified, rows
    # One row is one surface: a row naming two files makes the total unverifiable.
    if (multi != "") { print "row names more than one surface:" multi; exit 1 }
    if (covered + noview + mixed != rows || rows != 29 || unclassified) exit 1
  }' docs/lodestar-migration-inventory.md
```

### And the prose against the table

The partition script checks the table against itself. That is not where this document kept going
wrong: **seven review rounds found seven stale figures, every one of them a sentence that had not been
updated when the table moved.** So the check that matters asserts the prose:

```sh
f=docs/lodestar-migration-inventory.md
read -r n cov noview mixed ok < <(awk -F'|' '
  /## 3\. Group C/ { s = 1 }
  /\*\*Twenty-nine surfaces/ { s = 0 }
  !s || !/^\| / { next }
  { k = $2; gsub(/^[ \t]+|[ \t]+$/, "", k) }
  k !~ /^[0-9]+$/ { next }
  {
    n++; nest = $6; onchain = $5
    gsub(/^[ \t]+|[ \t]+$/, "", nest); gsub(/^[ \t]+|[ \t]+$/, "", onchain)
    if (k + 0 != n || $3 ~ /,/) bad = 1
    if (index(nest, ".sql`")) cov++
    else if (nest == "**no view**" && onchain == "**mixed**") mixed++
    else if (nest == "**no view**") noview++
    else if (nest == "partial") mixed++
  }
  END { print n + 0, cov + 0, noview + 0, mixed + 0, bad ? "no" : "yes" }' "$f")
[ "$n $cov $noview $mixed" = "29 12 12 5" ] || { echo "table is $n rows, $cov / $noview / $mixed"; exit 1; }
[ "$ok" = yes ] || { echo "row numbers are not 1..n, or a row names two surfaces"; exit 1; }

# Every sentence that quotes one of those numbers, by the number it must quote.
missing=0
for m in "group C, $n surfaces" "0 of $n migrated" "7 rows of a $n-row group" \
  "group C's $n" "**$cov** with a view file" "**$noview** needing a view written" \
  "**$mixed** mixed" "$cov having cleared the" "$cov / $noview / $mixed over $n rows"; do
  grep -qF -- "$m" "$f" || { echo "prose disagrees with the table: $m"; missing=1; }
done
[ "$missing" = 0 ] && echo "$n rows = $cov covered + $noview no view + $mixed mixed; prose agrees"
```

Run it before quoting any figure from this file elsewhere. A number here that no longer matches the
table is the single most likely defect in the document, on seven rounds of evidence.
