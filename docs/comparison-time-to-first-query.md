# Time to first query: Nuthatch, Ponder, Envio HyperIndex and a subgraph

Measured on 17 November 2026 for [#2979](https://github.com/nuthatch-org/nuthatch/issues/2979). One
contract, one block range, one keyless RPC, each tool from an empty directory along its own documented
quickstart. The Nuthatch side reruns with
[`scripts/compare-ttfq-nuthatch.sh`](../scripts/compare-ttfq-nuthatch.sh); the files typed for the
others are in [`comparison-time-to-first-query/`](comparison-time-to-first-query/), with the trimmed
logs and the timing harness that ran them (`harness/run-ponder.sh`, `run-envio.sh`, `run-graph.sh`).

## Method

- **Contract and range.** WETH on Ethereum mainnet, `0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2`, blocks
  26,400,000 to 26,400,999 inclusive: a thousand blocks, about five days old, long past finality.
- **Ground truth.** `eth_getLogs` for the address over the range, in ten 100-block calls, gives
  **49,354 logs**: 33,329 Transfer, 7,585 Withdrawal, 5,810 Deposit and 2,630 Approval, across 997 blocks.
- **RPC.** `https://eth.drpc.org`, keyless, for every tool. Of the public endpoints tried, publicnode
  now wants a token for logs this old, 1rpc caps `eth_getLogs` at 50 blocks, flashbots has pruned them,
  and llamarpc and merkle were down or rate-limiting; mevblocker also served the range and was kept as
  the second opinion. drpc's free tier throttles with HTTP 429, and three of the four tools met it; the
  counts are given below, because they explain most of the spread.
- **Time to first query** is from starting the run command (`nuthatch dev`, `ponder dev`, `envio dev`,
  `docker compose up`) to the first query that returns at least one indexed WETH event, polled every
  0.2 s (0.5 s for graph-node). **Range indexed** is from the same start to the moment the tool itself
  says it holds the whole range: the `as_of` block on Nuthatch's `/sql` answer, Ponder's `/ready`,
  Envio's `_meta.isReady`, the subgraph's `_meta.block.number`. Wall clock, from `date +%s.%N`.
- **Counted events.** After each run, each tool's own query surface is asked how many rows of each event
  it holds in the range (for graph-node, its Postgres tables), and the total is checked against the
  49,354.
- **No tuning.** Each tool runs at its defaults. The only edits are what its quickstart needs to name this
  contract, this range and this RPC, and they are listed below.
- **Machine.** Apple M5 Pro, 48 GB, macOS 26.5.1. Node 26.11.1, npm 11.20.0, pnpm 12.6.0, yarn 1.22.22.
  Docker 29.5.2 in Colima 0.10.3 (vz, Rosetta on, 4 CPUs, 6 GiB), Docker Compose 5.5.1.

## Results

| | Nuthatch 5.22.0 | Ponder 0.18.9 | Envio HyperIndex 3.15.0 | graph-node 0.45.0 |
|---|---|---|---|---|
| Needs | the binary | Node 22.12+, npm | Node 22.15+, pnpm, Docker (Postgres 18.3, Hasura 2.43.0), an Envio API token for its default data source | Docker (graph-node, IPFS, Postgres), Node for graph-cli |
| Code or config typed | none: two flags on `init` | 4 TypeScript files, about 110 lines: ABI, config, schema, handlers | 5 lines of `config.yaml`: range and RPC; schema and handlers generated | 1 line of `subgraph.yaml` and 1 of the compose file; schema and mappings generated |
| Install and scaffold | 4.0 s download and check, 2.4 s `init` | 16.6 s `create-ponder` | 2.9 s `envio init` | 16.3 s graph-cli, 19.6 s `graph init`, 1.6 s build |
| Time to first query | **1.0 s** (0.96, 0.97, 1.20) | **2.9 s** (2.78, 3.02) | **26 s** (25.8, 26.9) | **189 s** (once) |
| Range indexed | **16.0 s** (15.2, 16.0, 38.9) | **216 s** (228.8, 203.5) | **216 s** (186.4, 245.7) | **567 s** (once) |
| RPC refusals, per run | 0, 0, 6 HTTP 429s | 117, 95 HTTP 429s | 136, 203 failed ranges, nearly all 429 | none logged |
| Events in range | 49,354 | 49,354 | 49,354 | 49,354 |

Figures in bold are the median of three runs for Nuthatch and the mean of two for Ponder and Envio;
graph-node ran once. Every run of every tool held exactly the 49,354 logs, split exactly as the ground
truth splits them. graph-node's 189 s and 567 s are from `docker compose up`; from `graph deploy`, 15 s
later, they are 174 s and 552 s.

### What the numbers say, fairly

- **Ponder is quick to a first answer too.** Its first row was queryable in under 3 s, as close to
  Nuthatch as makes no difference to a person. Its install is one command and it needs no Docker and no
  database for development (PGlite). What it asks for is code: an ABI file, a config, a schema and a
  handler per event, which Nuthatch, Envio and graph-cli all generate from the ABI.
- **Ponder's backfill looks paced by the RPC rather than by Ponder.** It asks for each block with all
  its transactions (`eth_getBlockByNumber` with `true`) beside the logs, about a thousand requests over
  this range, and drpc's free tier answered a hundred or so with 429; its own handlers took 0.002 ms an
  event. Nuthatch also fetches a header per block, for `block_timestamp`, but without the transactions.
  On a paid endpoint, or one that does not throttle, this column would look different, and that was not
  measured here.
- **Envio's default path is not keyless.** `envio init` refuses to run non-interactively without an API
  token (an empty `--api-token ""` gets past it), and `envio dev` on the scaffold as written stops with
  "An Envio API token is required for using HyperSync as a data-source." HyperSync is what Envio is fast
  at, and this measurement does not test it, because it needs an account. With `rpc: for: sync` it
  indexes from drpc, and drpc refused its opening ranges (409 blocks down to 104) with "ranges over
  10000 blocks are not supported on free plan", so Envio halved and retried for about 20 s before the
  first rows landed. That error text does not match the ranges asked for; it is drpc's, not Envio's.
- **Envio and graph-cli need no code typed either.** Both read the ABI and write the schema and a
  handler per event, so all that was edited was the range and the RPC. The difference from Nuthatch is
  that the generated handlers are code in the project, there to be changed.
- **The subgraph's first answer waits for its first write batch.** graph-node scanned from the first
  block immediately and wrote nothing visible until its first write batch committed, 14,075 entities
  ending at block 26,400,304, which is when the first query was answered. It then indexed about 1.8
  blocks a second. Its generated schema also stores the block timestamp and transaction hash for each
  event, which Envio's does not.
- **Nuthatch's third run** met six 429s and took 38.9 s rather than 16. The other two met none.

### graph-node on Apple Silicon

The official compose file pulls `graphprotocol/graph-node`, which for 0.45.0 is published for amd64 only,
so on this Mac it runs under Rosetta. Run that way, graph-node was killed for running out of memory
(`OOMKilled: true`, exit 137) within ten seconds of starting to process blocks, five times out of five:
three times in the 6 GiB VM, once in a 16 GiB VM where it climbed from 374 MiB to 12 GiB in about twelve
seconds, and once more there against a second RPC (`rpc.mevblocker.io`) to rule drpc out. The same
version built unmodified from source for arm64 (`ghcr.io/nuthatch-org/graph-node:v0.45.0`, revision
`5973cd9`) ran the same subgraph in 123 MiB in the 6 GiB VM, and that is the run in the table. On an
amd64 Linux host the official image would run natively, which was not tried here; this is a limit of
the platform, stated so that nobody on a Mac is surprised by it.

## Commands typed

### Nuthatch

```sh
curl -fsSLO https://github.com/nuthatch-org/nuthatch/releases/download/v5.22.0/nuthatch-aarch64-apple-darwin.tar.gz
curl -fsSLO https://github.com/nuthatch-org/nuthatch/releases/download/v5.22.0/nuthatch-aarch64-apple-darwin.tar.gz.sha256
shasum -a 256 -c nuthatch-aarch64-apple-darwin.tar.gz.sha256
tar xzf nuthatch-aarch64-apple-darwin.tar.gz
./nuthatch init 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 --chain mainnet \
  --rpc https://eth.drpc.org --start-block 26400000
./nuthatch dev --listen 127.0.0.1:8301
curl -G 127.0.0.1:8301/sql --data-urlencode \
  "q=select count(*) from weth9__transfer where block_number between 26400000 and 26400999"
```

The checksum was `a4f9d304e21b372fd167f5ac4cca2309e3b4db442c2d6ec1facbc2ef8a28f9a7`, matching the
release's `.sha256`. Nuthatch has no end block for an event nest, so it carries on past 26,400,999 to the
tip; the range is timed to the answer whose `as_of` passes the end, and the counts are filtered by
`block_number`. `/sql` answers from a snapshot that can trail `/ready`'s `last_block` by a window, which
is why the script judges completion from the answer itself.

### Ponder

```sh
npx create-ponder@0.18.9 weth --template empty --npm --skip-git
cd weth
rm abis/ExampleContractAbi.ts
# write abis/WethAbi.ts, ponder.config.ts, ponder.schema.ts, src/index.ts (in ponder/ beside this doc)
PONDER_RPC_URL_1=https://eth.drpc.org npm run dev
curl -H 'content-type: application/json' localhost:42069/graphql \
  --data '{"query":"{ transfers(where: {blockNumber_gte: \"26400000\", blockNumber_lte: \"26400999\"}) { totalCount } }"}'
```

`.ponder/` was removed before each run so that neither started from Ponder's RPC cache.

### Envio HyperIndex

```sh
npx envio@3.15.0 init contract-import local -a weth-abi.json --contract-name WETH -n weth -d weth \
  -b 1 -c 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 -r https://eth.drpc.org -s 26400000 \
  --single-contract --all-events -l typescript --package-manager pnpm --api-token ""
cd weth
# config.yaml: start_block 0 -> 26400000, add end_block and the rpc block (diff in envio/ beside this doc)
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock   # Colima only
pnpm dev
curl -H 'x-hasura-admin-secret: testing' localhost:8080/v1/graphql \
  --data '{"query":"{ WETH_Transfer_aggregate { aggregate { count } } }"}'
npx envio stop
```

`weth-abi.json`, beside this doc, is the four events of the Sourcify ABI that Nuthatch's `init` resolved;
Ponder's `abis/WethAbi.ts` and the subgraph's `--abi` are the same four. `contract-import explorer`, the
path that fetches the ABI itself, needs the token outright; `local` with
the same events-only ABI the other tools used does not, once given an empty one. The `-s` and `-r` flags
were dropped from the generated config, because chain 1 has HyperSync, so they were added back by hand.
The first `envio dev` pulled Postgres and Hasura (about 40 s, not counted in the table).

### graph-node

```sh
npm i @graphprotocol/graph-cli@0.98.1
npx graph init weth weth --protocol ethereum --from-contract 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 \
  --network mainnet --contract-name WETH --index-events --start-block 26400000 --abi weth-abi.json --skip-git
# subgraph.yaml: add endBlock: 26400999 under startBlock
curl -fsSLO https://raw.githubusercontent.com/graphprotocol/graph-node/v0.45.0/docker/docker-compose.yml
# docker-compose.yml: ethereum: 'mainnet:https://eth.drpc.org'; on Apple Silicon, the arm64 image (above)
docker compose up -d
cd weth && npx graph codegen && npx graph build
npx graph create --node http://localhost:8020/ weth
npx graph deploy --node http://localhost:8020/ --ipfs http://localhost:5001 weth --version-label v0.0.1
curl localhost:8000/subgraphs/name/weth -H 'content-type: application/json' \
  --data '{"query":"{ _meta { block { number } } transfers(first: 1) { id } }"}'
docker compose down -v
```

The counts were read from Postgres directly (`select count(*) from sgd1.transfer where block_number
between ...`), since GraphQL pages at 1,000 entities and has no count. Images were already in the local
cache for graph-node and IPFS; Postgres `latest` (18.7) was pulled in 18.7 s, not counted.

## Limits

- **One machine, one evening, one RPC.** Two or three runs per tool, not a distribution. drpc's free tier
  throttled every tool at some point, and how often decided most of the spread; the ranking of the
  backfill times on a paid or private endpoint is not something this shows.
- **Envio was not measured as Envio is meant to be run.** HyperSync needs an account, and the brief was
  to sign up for nothing. Its RPC mode is a fallback, and its time here says nothing about HyperSync.
- **graph-node ran from an arm64 build** of the official source, not the official image, which cannot run
  this subgraph on Apple Silicon (above). It was measured once.
- **A thousand blocks is short.** Nuthatch's figure is a few dozen `eth_getLogs` windows and the
  block-header fetches for `block_timestamp`; a year of WETH is a different measurement, and so is a
  contract whose interesting state needs code rather than its events.
- **The tools do not do the same amount of work.** Beside the logs, Ponder fetches every block's
  transactions, graph-node each block's receipts, Nuthatch each block's header, and Envio what its RPC
  source asks for; Ponder, Envio and graph-node then run handler code per event, where Nuthatch decodes
  from the ABI. That is each tool's design rather than a fault in the comparison, but the backfill
  column measures it.
- **Human time is not counted.** Writing Ponder's 110 lines, or editing the YAML for the others, took a
  person minutes the table does not show.

## Raw logs

In [`comparison-time-to-first-query/logs/`](comparison-time-to-first-query/logs/), trimmed to the lines that
carry information: Ponder's terminal UI redraws and Envio's repeated stack traces are cut, and the
graph-node log keeps its warnings, errors and write batches. The filters are at the top of each file.
