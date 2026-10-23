# Stopgap handback: BetSwirl BNB Chain

> **Retired 2026-10-07.** The hosted endpoint is stopped: in the day it ran it served no one but us (BetSwirl's
> last bet on BNB Chain was on 20 March 2026, and nobody got in touch). Everything below still works for
> anyone who wants to run it: the nest, the mirror and the release downloads are unchanged. Kept as
> written otherwise.

The package for one stopgap nest: what it answers, how to run it yourself, and how to go back to The
Graph network. It is written for someone who did not build the nest. The programme is in
[docs/subgraph-stopgap.md](https://github.com/nuthatch-org/nuthatch/blob/main/docs/subgraph-stopgap.md).

| | |
| --- | --- |
| Subgraph deployment | `Qmd5oqyojVx5wWSFuWfKz3YVLPHdE3KU5458Qqq3SVeGEB`, network `bsc` |
| Unserved on the network since | 2026-09-17 11:31 UTC (last allocation closed) |
| Nest | [nuthatch-org/betswirl-bnb-nest](https://github.com/nuthatch-org/betswirl-bnb-nest) |
| Hosted endpoint | `https://betswirl-bnb.89.167.109.4.sslip.io/subgraphs/id/Qmd5oqyojVx5wWSFuWfKz3YVLPHdE3KU5458Qqq3SVeGEB` |
| Mirror | `https://pub-bc282d5016f242a783a6c28cbcd4401a.r2.dev`, listed at [nuthatch-indexer.com/mirror](https://nuthatch-indexer.com/mirror) |
| Binary | the `nuthatch-graph` release download, 4.11.0 or later |

Where this page and the nest's README disagree on how to run it, this page is current: the README
predates the 4.11.0 release and will be corrected with the nest's next re-index.

## What it answers

Every field of the four GraphQL documents BetSwirl's client sends (`@betswirl/sdk-core` 0.1.27) answers
exactly: 30 leaf fields on a bet, 17 on a token. The documents are in the nest's
[`queries/`](https://github.com/nuthatch-org/betswirl-bnb-nest/tree/main/queries): `bet.graphql`
(variables `{"id": "<bet id>"}`), `bets.graphql` (`{"first": 20}`, plus `skip`, `where`, `orderBy`,
`orderDirection`), `token.graphql` (`{"id": "<token address>"}`) and `tokens.graphql` (`{"first": 10}`).
Anything else is refused by name, never answered with a substitute. The gas token answers as
`ETH`, `ETH`, 18 decimals: that is what BetSwirl's bank contract returns on BNB Chain, and so what the
subgraph stored. The nest README has the
field-by-field table and the mapping handler each rule was read from. It is not a whole subgraph: 7 of
its 51 entity types have a view.

**Check your own queries before relying on it.** If your app sends a field outside those documents,
send that query to the endpoint: it either answers or names the field it refuses.

**Speed.** A `bet` or `bets` query that is not in the answer cache takes 8 to 17 s on the full
history, more on a busy machine, measured on 2026-10-06 (the comparison query below is that kind too); `token` and `tokens` take about 1.5 s; a
repeated query takes milliseconds. A query that runs past 29 s returns HTTP 200 with an `errors` body
naming the time budget. Both are being worked on in
[nuthatch#1951](https://github.com/nuthatch-org/nuthatch/issues/1951).

## Run it yourself

**What you need:**
- an x86_64 Linux machine, an Apple Silicon Mac, or an arm64 Linux machine on 4.12.0 or later
  (`nuthatch-graph-aarch64-unknown-linux-gnu.tar.gz`; this package was cold-tested on 4.11.0 only).
  On 2026-10-06 an x86_64 binary under emulation was too slow for `bet` and `bets` to finish inside
  the 29 s budget;
- about 2 GB of RAM;
- `curl`, `git`, `jq`, `sha256sum` (Linux) or `shasum` (Mac), and optionally `gh` 2.49 or later to verify
  provenance. Distribution packages of `gh` can be older (Ubuntu 24.04 ships 2.45): install it from
  [cli.github.com](https://cli.github.com);
- an archive BNB Chain RPC endpoint. The public ones do not serve this much history.

```sh
VERSION=v4.11.0                                   # the version this was tested on; 4.11.0 is the floor
FILE=nuthatch-graph-x86_64-unknown-linux-gnu.tar.gz   # Apple Silicon: nuthatch-graph-aarch64-apple-darwin.tar.gz
export BNB_ARCHIVE_RPC=   # required: your archive endpoint
: "${BNB_ARCHIVE_RPC:?set BNB_ARCHIVE_RPC first}"

# 1. the graph build
curl -fLO "https://github.com/nuthatch-org/nuthatch/releases/download/$VERSION/$FILE"
curl -fLO "https://github.com/nuthatch-org/nuthatch/releases/download/$VERSION/$FILE.sha256"
sha256sum -c "$FILE.sha256"                        # Mac: shasum -a 256 -c. Checks the download, not who built it
gh attestation verify "$FILE" --repo nuthatch-org/nuthatch && echo verified   # who built it (gh needs login)
tar xzf "$FILE"

# 2. the nest, filled from the mirror instead of a 109-million-block backfill
git clone https://github.com/nuthatch-org/betswirl-bnb-nest
./nuthatch seed --dir betswirl-bnb-nest --from https://pub-bc282d5016f242a783a6c28cbcd4401a.r2.dev
# seed ends by suggesting a bare `nuthatch dev`; use step 3 instead, which this nest needs.

# 3. follow the chain and serve. --state-rpc is the same endpoint: the nest reads the bank's
#    getTokens() at each AddToken block for token names and decimals. It runs in the foreground:
#    leave it running and use a second terminal for the rest.
NUTHATCH_BURRMILL_MEMORY_LIMIT=1024MB \
  ./nuthatch dev --dir betswirl-bnb-nest --rpc "$BNB_ARCHIVE_RPC" --state-rpc "$BNB_ARCHIVE_RPC"
```

Seeding took 6 s and reaching the chain head about 20 s more, in a clean container on 2026-10-06.
GraphQL is then at `http://127.0.0.1:8288/subgraphs/id/Qmd5oqyojVx5wWSFuWfKz3YVLPHdE3KU5458Qqq3SVeGEB`;
add `--listen 0.0.0.0:8288` to reach it from outside the machine or a container. The memory setting
is needed for the single-`bet` query, which runs out of memory at the default 512 MB (#1951); it stays
inside the 2 GB budget.

`seed` refuses a nest directory that differs from the published one, so seed a clean clone and edit
afterwards. Without the mirror, `dev` alone backfills from the deployment block: about 16 minutes on
a 32-core machine against a paid endpoint.

**Check your copy** once it has caught up: the nest's `tests/run.sh` compares it with an independent
reference built from raw chain logs (see Limits). The hosted comparison below is historical: that
endpoint was retired on 2026-10-07 and the `hosted.json` line no longer answers.

```sh
curl -s 127.0.0.1:8288/ready | jq '.ready, .lag_blocks'   # wait for true and 0
Q='{"query":"{ bets(first: 100, orderBy: betTimestamp, orderDirection: asc) { id betAmount payout payoutMultiplier houseEdge } }"}'
curl -s -H 'content-type: application/json' -d "$Q" http://127.0.0.1:8288/subgraphs/id/Qmd5oqyojVx5wWSFuWfKz3YVLPHdE3KU5458Qqq3SVeGEB | jq -S . > mine.json
curl -s -H 'content-type: application/json' -d "$Q" https://betswirl-bnb.89.167.109.4.sslip.io/subgraphs/id/Qmd5oqyojVx5wWSFuWfKz3YVLPHdE3KU5458Qqq3SVeGEB | jq -S . > hosted.json
cmp mine.json hosted.json && echo same
```

That query reads settled rows, so it should match exactly. Default-ordered pages and the token
counters (`betCount`, `totalWagered`) can differ while either side is a few blocks behind; compare
those when both report `lag_blocks` 0.
## Going back to the network

The stopgap ends when an indexer allocates to the deployment again. Watch for it in either place:

- [lodestar-dashboard.com/subgraphs/migration](https://www.lodestar-dashboard.com/subgraphs/migration):
  the deployment leaves the list when it has an indexer.
- The network subgraph: `allocations(where: {subgraphDeployment: "0xdb11ebed14197843a9f306efd6b271acd9993598cecb5bdc044cd25c36d710c0", status: Active}) { indexer { id } }`.

Then point your client back at the gateway URL it used before
(`https://gateway.thegraph.com/api/<key>/deployments/id/Qmd5oqyojVx5wWSFuWfKz3YVLPHdE3KU5458Qqq3SVeGEB`),
send the comparison query to both, and switch once they agree.

## Limits

A nest you run yourself carries no proof of indexing, no allocation and no dispute path. (The hosted
endpoint, retired 2026-10-07, was one machine on a home connection with no uptime promise.) The answers are checked against an independent
reference built from raw chain logs (32,257 bets, zero differences, `tests/run.sh` in the nest), not
against a graph-node running the subgraph, because none serves it.
