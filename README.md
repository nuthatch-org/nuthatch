# nuthatch

> **Turn an EVM contract's history into a local SQL database.** One Rust binary, no Postgres, no
> subgraph to write, and an MCP server built in.

[![ci](https://github.com/nuthatch-org/nuthatch/actions/workflows/ci.yml/badge.svg)](https://github.com/nuthatch-org/nuthatch/actions/workflows/ci.yml)
· Website: [www.nuthatch-indexer.com](https://www.nuthatch-indexer.com)

```sh
curl -fsSL https://nuthatch-indexer.com/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
nuthatch init 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 --alias weth
nuthatch dev --backfill 300
```

Then, in a second terminal:

```sh
nuthatch sql "SELECT count(*) FROM weth__transfer"
```

The installer covers macOS Apple Silicon and Linux x86_64 and aarch64, and the `export` adds its
directory, which a fresh macOS shell lacks. `init` indexes WETH and detects its chain. The block
carries no `#` comments because zsh, the macOS default, does not treat a pasted `#` as one: it
passes the rest of the line on as arguments.

The Linux binary needs glibc 2.35 or newer to run from 4.1.0, which is also what it is built on;
4.0.x ran on 2.34. No Intel Mac binary is published: there, and on other platforms, build from
source with Rust 1.95.0 ([docs/install.md](docs/install.md)).
`init` creates a **nest**: a directory holding the contract's ABI, its config and, once `dev` runs,
its indexed data. `--backfill 300` starts 300 blocks behind the tip, about an hour of mainnet, so there
are rows to query within seconds on the bundled public endpoints. Without it, `dev` backfills from the
contract's deployment block: for WETH that is 21 million blocks, a long backfill on free public endpoints
and a job for your own RPC (`--rpc`).

Those five lines, run as written on the 4.10.1 release on 2026-10-05, took 13 s from the `curl` to a
non-zero count in a clean macOS shell on an M5 Pro, and 21 s in a fresh `ubuntu:24.04` container
([docs/readme-check-2026-10-05.md](https://github.com/nuthatch-org/nuthatch/blob/ede03ca7/docs/readme-check-2026-10-05.md), which records every command in
this file).

| | Needs a subgraph | Needs handler code | Data comes from | What you run | Query with |
|---|---|---|---|---|---|
| **The Graph** | yes | yes (AssemblyScript) | indexers on the network | nothing, or graph-node + Postgres + IPFS | GraphQL |
| **Ponder** | no | yes (TypeScript) | your RPC endpoint | Node.js, plus Postgres in production | SQL, GraphQL |
| **nuthatch** | no | no: tables come from the ABI | your RPC endpoint | one binary | SQL, HTTP, MCP |

Like Ponder, nuthatch reads the chain over JSON-RPC, so it needs an endpoint, and a provider may
charge for one. The public endpoints it bundles are for trying it out, not for keeping it running.
It complements The Graph rather than replacing it: a subgraph serves an application from a network
of indexers, while a nest puts a contract's history in a database on your own machine.

**Why.** Getting at a contract's history usually means writing a subgraph or handler code, running a
database, or renting someone else's copy. nuthatch generates the tables from the ABI, runs as one
process with nothing else to install, and keeps the data on your machine: at most 2 GB of RAM per
chain, no telemetry unless you opt in, no account. The built-in MCP server lets Claude or any MCP client query it.

---

## What you get

- **No authoring.** `init 0xAddr` resolves the ABI (Sourcify, then Etherscan), generates the schema and
  decoders, and scaffolds the project. You write nothing.
- **No infra.** A single Rust binary. Embedded mode needs no Postgres, no Docker, no IPFS.
- **It's just SQL.** Your contract's events become per-event tables you query with real analytical SQL -
  the live tip *and* sealed history, one surface.
- **It's yours, and it's small.** ≤2 GB RAM for single-chain tip-following, CI-enforced. No telemetry by
  default, no mandatory API token, ever: the one opt-in is a head count at `init` (`nuthatch count`),
  and [its totals are public](https://www.nuthatch-indexer.com/count). Most people say no or are never asked, so
  they are floors.

---

## Who runs it

- **[Lodestar](https://www.lodestar-dashboard.com)**, an analytics dashboard for The Graph Protocol on
  Arbitrum One, serves live panels from self-hosted nests instead of The Graph gateway.
- **[GraphOps](https://graphops.xyz)**, an indexer and core developer on The Graph, is a design
  partner. Its feedback is what led to maintained entities ([RFC-0041](docs/rfcs/0041-authored-incremental-entities.md)).

An earlier example, now finished: **[Arcaidia](https://arcaidia.io)**, a speed layer over Circle's
CCTP built at ETHOnline 2026, read its indexed state from two nests on Ethereum Sepolia and Arc
Testnet: its solver discovered intents there, its settlement agent tracked CCTP there, and its web
console rendered from them. The nests were stopped on 2026-09-29.

More at [nuthatch-indexer.com/stories](https://www.nuthatch-indexer.com/stories).

Nuthatch does not run a hosted nest service.

---

## Install

```sh
curl -fsSL https://nuthatch-indexer.com/install.sh | sh
```

That downloads the prebuilt binary for your platform from the latest release, verifies its SHA-256,
and installs it to `~/.local/bin` (override with `NUTHATCH_INSTALL_DIR`). **No compiler is
involved.** A stock macOS shell does not have `~/.local/bin` on its `PATH`, which is why the quickstart
above exports it; add the same line to `~/.zshrc` (or `~/.bashrc`) to keep it for new terminals. Prebuilt binaries cover macOS Apple Silicon and Linux x86_64 and aarch64 (aarch64 from the release after 4.11.0) and are attached to every
release with their checksums, if you would rather fetch one by hand. **No Intel Mac binary is
published**; the installer says so and points at the source build below.

**The Linux binary is dynamically linked and needs one thing**, measured off the published
artifact with `objdump -T` rather than inferred:

- **glibc 2.35 or newer** - the measured ABI floor, and also what the release is *built* on. Up to
  4.0.2 the binary referenced no symbol newer than `GLIBC_2.34`, so 2.34 was what you needed to run
  it and 2.35 only what we compiled it on ([#978](https://github.com/nuthatch-org/nuthatch/issues/978));
  4.1.0 references `hypot` at `GLIBC_2.35`, where libm re-versioned it, so the two numbers now agree.

It links `libc`, `libm` and `libgcc` and no C++ runtime. Releases before 4.1 embedded DuckDB and
also needed libstdc++ from GCC 11.

Debian 12 and Ubuntu 22.04 clear it. RHEL 9 and Amazon Linux 2023 ship glibc 2.34 and ran 4.0.x; from
4.1.0 they need the source build.

**Verify who built it.** Every release binary carries a build provenance attestation, which a
checksum cannot give you:

```sh
gh attestation verify nuthatch-x86_64-unknown-linux-gnu.tar.gz --repo nuthatch-org/nuthatch
```

**From source**, which is the only route on a platform we do not publish a binary for, Intel Macs
included (the Intel build has not been verified on Intel hardware):

```sh
rustup toolchain install 1.95.0
cargo +1.95.0 install --git https://github.com/nuthatch-org/nuthatch nuthatch
```

The `+1.95.0` is required: `cargo install --git` ignores the repo's toolchain pin, and a newer
default toolchain fails to compile a dependency.

**Container images** are published per release to `ghcr.io/nuthatch-org/nuthatch` - `:<version>` for
embedded, `:<version>-scaled` for the scaled build. The image ships the *same binary attached to the
release*, so the two cannot drift.

[docs/install.md](docs/install.md) has the detail behind each of these: how the ABI floor is measured,
what the attestation proves and what `--repo` is for, and why the toolchain pin exists.

**Chains.** Ethereum, Arbitrum One, Base, BSC, Polygon, Gnosis, Optimism, Monad and Robinhood Chain are *built in*, with
measured public endpoints and tuned finality settings - **omit `--chain` and nuthatch probes each for
your contract's bytecode and picks the one it lives on.** Point at your own node with `--rpc`.

**Any other EVM chain works too** - World Chain, Base Sepolia, your own devnet. Name the chain and
say where it lives:

```sh
nuthatch init 0xADDR --chain world-chain --rpc https://your-endpoint.example
```

The chain id is read **from the endpoint itself**, so there is nothing to look up or type wrong. On
one of the nine built-in names `--rpc` is not consulted for the chain id, but it **is** the pool: your
endpoints replace the bundled public ones outright. Omit `--rpc` on an unregistered name and the
refusal tells you the remedy.

See [running an unlisted EVM chain](docs/operators.md#running-an-unlisted-evm-chain) for the
finality caveat, which is the part worth reading: a chain whose `finalized` tag runs close to the tip
needs a depth-based policy instead, or you seal immutable Parquet that could never be corrected.

### Bring your own RPC endpoint

**nuthatch assumes a paid RPC endpoint, or your own node, for anything you intend to keep running.**
That is the golden path.

Worth knowing, since we are being precise about it: most figures currently in
[`docs/benchmarks.md`](docs/benchmarks.md) were measured against *public* endpoints, and that is a
known weakness of those numbers rather than a recommendation - a benchmark taken through a
rate-limited endpoint measures the endpoint. On one workload the network was **99.3% of backfill wall
clock** (the same tape live against a public endpoint and replayed from disk, 2026-08-23, in
`docs/benchmarks.md`), which is why the replay rig (RFC-0039) exists and why those figures carry that
caveat on the page itself.

The free public endpoints bundled per chain exist for one job: so `init` → `dev` works with **zero
setup**. Treat them as testing and initial validation. Why they are not fine for real work, said here
rather than discovered at 3am:

- **They are rate-limited and shared**, and throughput varies by the hour.
- **They fail intermittently, and not always loudly.** nuthatch fails over across the pool and
  retries, but a window that every endpoint refuses stalls until one recovers; `/ready` reports
  `stalled` when that happens.
- **Deep backfills will crawl or stop**, and many free endpoints prune old blocks, so a backfill from
  a 2020 deploy block can fail partway.

**Check an endpoint before you trust a backfill to it.** `nuthatch doctor` probes one and reports the
largest `getLogs` window it will actually serve, its batch limit, and whether it has archive history -
measured, not taken from the provider's documentation:

```sh
nuthatch doctor --rpc https://your-endpoint.example --address 0xADDR
```

**Use your own endpoint for anything you care about** - your own node, or a paid provider:

```sh
nuthatch init 0xADDR --chain arbitrum-one --rpc https://your-endpoint.example/arbitrum
nuthatch dev --rpc https://your-endpoint.example/arbitrum   # or set rpc_urls in nuthatch.toml
```

`--rpc` is repeatable, and nuthatch round-robins across the pool with per-endpoint health tracking, so
listing two or three endpoints gets you failover as well as throughput. Every endpoint in a pool must be
on the **same chain** - nuthatch verifies this at startup and refuses to run against a mixed pool, since
indexing against the wrong chain corrupts state silently.

---

## Querying your data - the whole point

Every declared event becomes a table named `{alias}__{event}` (e.g. `usdc__transfer`), carrying the
event's fields plus `block_number`, `block_hash`, `block_timestamp`, `tx_hash`, `log_index`,
`address` and a `_seq` ordinal.

> `block_timestamp` costs a block-header round trip per block: on OBIB case 1 it took the backfill
> from 74.8 s to 1,689 s, 22.6x ([RFC-0029 §4c](docs/rfcs/0029-the-fastest-indexer.md), 2026-07-31). A
> nest that will never ask a time-series question can drop the column with `init --no-timestamps` and
> skip that entirely. It is an **init-time** choice: changing it later is a breaking schema change and
> a full re-index, so it is worth a moment's thought and is deliberately not a flag you can flip.
> [Details](docs/operators.md#configuration-surface).

```sh
# one-shot from the terminal (prints an aligned table; --json to pipe to jq)
nuthatch sql 'SELECT "from" AS sender, count(*) AS n FROM usdc__transfer GROUP BY 1 ORDER BY n DESC LIMIT 5'

# or over HTTP, against a running `nuthatch dev`
curl 'localhost:8288/sql?q=SELECT%20count(*)%20FROM%20usdc__transfer'
```

- **`nuthatch sql`** queries the local store when `dev` is stopped, and transparently falls back to the
  running instance's API when `dev` holds it - the same command works either way.
- **A degraded nest says so.** If a sealed segment is unreadable, nuthatch serves the rest of the table
  rather than failing your query - but it will not let that pass silently. `/sql` returns `degraded`
  and `degraded_tables` naming the affected tables, `nuthatch sql` prints a warning line, and the MCP
  server carries the same notice. The caveat is a fact about the *nest*, not about the row count you
  happened to get, so it appears whether or not this particular query touched the gap.
- **A failed query tells you how to fix it.** An engine error is classified against the nest's own
  schema and a hint is appended after the engine's raw message: an unknown table names the closest
  real one, a view that failed to *build* says so rather than "does not exist", and a Solidity `bool`
  column, stored as exact text `'true'`/`'false'`, explains why it blows up inside `COALESCE`, `CASE`
  and `bool_and`. Same treatment on `/sql`, the MCP `sql` tool and `nuthatch sql`.
- **Hot + cold in one surface.** Queries span the live unsealed tip (redb) *and* sealed history
  (Parquet), transparently - you never think about the boundary.
- **Big-int friendly.** `uint256` values are exact text; amounts that fit in 38 digits also get a
  `{col}_dec` DECIMAL view. `SUM(value_dec)` is those values: a full-width word is NULL and is not a
  term. `WHERE NOT {col}_overflow` writes the same sum out. Ids, nonces and hashes stay on the raw
  column.
- **AI-native.** A Model Context Protocol server is compiled in (`nuthatch mcp`) - point Claude (or any
  MCP client) at your indexer and ask your contract's data in plain English, fully offline.

---

## How fast is it

We ran **someone else's** benchmark rather than writing our own: Sentio's
[OBIB](https://github.com/sentioxyz/open-blockchain-indexer-benchmark).

**Case 1** indexes `Transfer` from LBTC across 22.2M Ethereum blocks.

| | |
|---|---|
| wall clock | **74.8 s** (median of 3) |
| events | **294,278** (matches Sentio's own README exactly) |
| RPC requests | **321** |
| peak RSS | **320 MB** |
| measured | commit `8e94f6c`, 2026-07-30, Alchemy, 11-core laptop: [`docs/bench/obib-case1.json`](docs/bench/obib-case1.json) |

**Case 2** is case 1's contract with per-account balances, and OBIB's implementations get them with one
`balanceOf()` per account. **We make none.** For a plain ERC-20 the balance *is* the transfer history,
so we index the token's whole life instead and derive it - trading 2.5M extra blocks of cheap `getLogs`
for zero `eth_call` round trips.

| | |
|---|---|
| wall clock | **49.2 s** (median of 3) |
| accounts | **7,634** - OBIB's published figure, exactly |
| `eth_call` round trips | **0** |
| RPC requests | **136** |
| peak RSS | **325 MB** |
| measured | commit `5a81a37`, 2026-08-07, Alchemy, 11-core laptop: [`docs/bench/obib-case2.json`](docs/bench/obib-case2.json) |
| on 4.10.1 | **73.4 s** (median of 3), 174 requests, 403 MB, the release binary against Tenderly's keyless gateway, which throttled it, 2026-10-05: [`docs/bench/obib-case2-4.10.1-tenderly-2026-10-05.json`](docs/bench/obib-case2-4.10.1-tenderly-2026-10-05.json) and [the note beside it](docs/bench/obib-case2-4.10.1-tenderly-2026-10-05.md) |

Reference times for the same case, from OBIB's README: Sentio 7.78 min, Envio 8.54 min, Subsquid
46.85 min.

**Two caveats, stated rather than buried.** First, this is deliberately not like-for-like on *range*:
OBIB windows to 100,001 blocks, we index 2,611,334. On OBIB's own range we take **9.3 s** - but that
run cannot produce the case's output at all, because absolute balances need history from before the
window, which is precisely why the benchmark makes the RPC calls. Second, "derived" is proven rather
than asserted: at the pinned end block, 39 sampled accounts - the ten largest, ten smallest non-zero,
ten zero-balance and ten by address order - **all matched `balanceOf()`**, including every zero-balance
account, which is the case an off-by-one in the ledger would betray. The count is 7,634 and not 7,635
because `0x0` is the mint/burn counterparty rather than a holder.

**Case 6** is the factory-template case: the Uniswap V2 factory over blocks 19,000,000-19,010,000,
discovering pairs from `PairCreated` and indexing `Swap` on every child it finds. No per-child config,
no redeploy, one rule.

| | |
|---|---|
| wall clock | **4.67 s** (median of 5; runs 4.5-5.4 s) |
| events | **35,271** = **35,039** swaps, matching OBIB's expected count exactly, plus the 232 `PairCreated` rows |
| children discovered | **232** |
| RPC requests | **14** |
| peak RSS | **229 MB** (median; 205-251 MB across runs) |
| measured | nuthatch **4.7.0**, the published `aarch64-apple-darwin` release binary, against Tenderly's keyless public gateway (`mainnet.gateway.tenderly.co`), on an 18-core Apple M5 Pro with 48 GB, 2026-10-05 |
| artifact | [`docs/bench/obib-case6-4.7.0-tenderly-2026-10-05.json`](docs/bench/obib-case6-4.7.0-tenderly-2026-10-05.json), with the binary's sha256, the command and every run in [the note beside it](docs/bench/obib-case6-4.7.0-tenderly-2026-10-05.md) |

The figure this table used to show, **49.5 s** with 16 requests and 247 MB, is **withdrawn: 1.0.1 on
a closed Alchemy account**. Nobody, us included, can rerun it. Its report stays in the tree as
[`docs/bench/obib-case6.json`](docs/bench/obib-case6.json) for the record, not as a claim.

For scale, OBIB's own published figures for case 6 differ between its two tables: its results table
gives Envio HyperIndex **1.92 min**, Subsquid 5.34 min and Sentio 14.36 min, while its case-6 page
reports Envio at **30 s** from an earlier round. We quote both, and neither is a like-for-like
ranking: those runs were on other machines, other days and other endpoints, and Envio and Subsquid
serve this from their own pre-indexed networks, where nuthatch runs against plain JSON-RPC.

Cases 1 and 2 were measured in July and August 2026 against the same Alchemy account case 6's old
figure is withdrawn for, so nobody can rerun those two as they were; they stay because their
artifacts carry commit, provider and hardware, and case 2 has been rerun on the current release
without an account, in the row above. The artifacts are
[`docs/bench/obib-case1.json`](docs/bench/obib-case1.json),
[`docs/bench/obib-case2.json`](docs/bench/obib-case2.json),
[`docs/bench/obib-case2-4.10.1-tenderly-2026-10-05.json`](docs/bench/obib-case2-4.10.1-tenderly-2026-10-05.json) and
[`docs/bench/obib-case6-4.7.0-tenderly-2026-10-05.json`](docs/bench/obib-case6-4.7.0-tenderly-2026-10-05.json);
`nuthatch bench backfill` re-runs any of them.
The case-2 nest is committed at [`obib-case2/`](obib-case2/) - keyless, so the endpoint arrives via
`--rpc`, and verified to rebuild from a clean checkout.
The case-6 nest is published at [`nuthatch-org/obib-case6`](https://github.com/nuthatch-org/obib-case6)
so the run can be reproduced rather than believed, and is submitted upstream as
[sentioxyz/open-blockchain-indexer-benchmark#3](https://github.com/sentioxyz/open-blockchain-indexer-benchmark/pull/3).
Case 6 needs no account at all: the release binary, that nest and the public gateway are the whole
setup.

**Wall clock on a shared endpoint is the provider's number as much as ours.** On the withdrawn Alchemy
setup we checked whether provider caching flattered the case-6 figure by re-running an adjacent,
never-fetched range of the same size: **48.2 s** against 49.5 s
([`obib-case6-cold-control.json`](docs/bench/obib-case6-cold-control.json), 2026-08-04), so caching
was not the explanation. The event count was the same in every run, and so was the request count for
a given version: **16** then, **14** on 4.7.0 in all five runs. Those are the honest measure of range
control.

Two things the case 1 number is worth knowing about:

- **It did not finish at all before v0.9.0.** Alchemy returns its oversized-range refusal as HTTP
  **400**, which our status classifier did not enumerate - so a window that needed splitting was
  retried unchanged, forever. Running an outside benchmark found a defect that our own testing had not.
- **Most of the original wall clock was buying `block_timestamp`** - one serial round trip per block,
  for a column that workload never stores: 1,689 s with timestamps against 74.8 s without, on the
  same range and endpoint ([RFC-0029 §4c](docs/rfcs/0029-the-fastest-indexer.md), 2026-07-31).
  Timestamps are now demand-driven and the log window adapts to what an endpoint will actually serve.

Case 6 found a defect too, in the harness rather than the indexer: `bench backfill` fetched a fixed
address list, so a **factory nest was measured without its children** - 232 events in 2.6 s against an
expected 35,039, reported as a success ([#310](https://github.com/nuthatch-org/nuthatch/issues/310)).
Running an outside benchmark has now found two things our own testing did not.

**Analytical queries** run on [Burrmill](https://github.com/nuthatch-org/burrmill), our engine on
DataFusion, over sealed Parquet. Until 4.1 they ran on DuckDB, and the change was not made for speed:
on our largest nest, through `/sql`, Burrmill took about **2.5 times DuckDB's time** per statement
with eight times the memory allowed to each session, measured 2026-10-01 for the
[4.1.0 release notes](docs/releases/v4.1.0.md). What it buys is one language in the binary and exact
arithmetic that refuses rather than wraps. The reasons and the log of the switch are in
[Replacing DuckDB, after all](https://nuthatch-indexer.com/blog/replacing-duckdb-after-all).

---

## How it works (the 30-second version)

```
RPC ingestion  →  deterministic decode  →  redb hot store (the unsealed tip)
                                                            │
                                        past finality  →  content-addressed Parquet segments
                                                            │
                                        Burrmill reads segments read-only   →  SQL (hot ∪ sealed)
```

- **One cursor per chain.** It follows the tip over `eth_getLogs`, decodes each window into the hot
  store, and seals a segment once its blocks are past the chain's finality (a depth of 64 on mainnet;
  a tag or a depth per chain). Reorgs only ever touch the hot store; a sealed segment is never
  rewritten.
- **Deterministic core.** Decode, reorg handling and entity derivation are deterministic and
  re-executable: same inputs, same content-addressed output. No LLM sits in the data path.
- **Single writer, read-only queries.** One ingestion thread writes; `/sql` attaches read-only.
  Analytical SQL has run on [Burrmill](https://github.com/nuthatch-org/burrmill), our engine on
  DataFusion, since 4.1.0; DuckDB was the engine before that and is no longer in the binary.
- **Derived tables are circuits.** The three built-in relations and any entity declared in
  `entities.toml` are maintained by DBSP as blocks arrive; a reorg is a retraction. On Arbitrum,
  `[extract] l1_blocks = true` adds a table of each block's L1 block number (4.6.0).
- **History travels.** `publish` mirrors a nest's sealed segments to a bucket and `seed` starts a nest
  from one instead of backfilling (4.9.0); a public mirror of four Graph Protocol nests is at
  [nuthatch-indexer.com/mirror](https://nuthatch-indexer.com/mirror).
- **A release is gated before it reaches production.** The candidate serves a copy of a production
  nest under its production budget, answers the statements that nest actually receives, and every
  answer is compared with production's ([docs/release-gate.md](docs/release-gate.md)). It exists
  because 4.1.1 passed CI and refused the dashboard's views within minutes of deployment.

---

## Point an AI at it

nuthatch has a Model Context Protocol server compiled in, so a coding agent can query your contract's
data in plain English - offline, no phone-home. Wiring it is one step:

```sh
nuthatch dev &                  # the index the agent will query
nuthatch mcp --print-config     # prints a copy-paste config for Claude Code / any MCP client
```

Or add it to Claude Code directly:

```sh
claude mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288
```

Then just ask: *"what are the top USDC holders?"* - the agent writes the SQL and runs it against your
nest. (Making that correct on the first try is the [semantic-layer work](docs/rfcs/0016-governed-semantic-layer-and-agent-grade-mcp.md).)

**Teach your agent to *build* nests too.** Install the builder skill and an agent can drive nuthatch
itself - `init`, config, factories, compliance, multi-nest runtimes, troubleshooting - before you even have a nest:

```sh
cp -r skills/nuthatch-builder ~/.claude/skills/   # or your repo's .claude/skills/
```

Its CLI/config references are generated from the binary and CI-checked for drift, so the skill never
lies about a flag ([RFC-0017](docs/rfcs/0017-builder-skill.md)).

---

## Everything else it can do

The core is "your contract → SQL." Beyond that, nuthatch has a full feature set for teams and operators
who need more - none of it in the way of the happy path:

- **Many contracts, one nest.** Declare several contracts in `nuthatch.toml`; index them together.
- **Contract state, pinned** (RFC-0023). A `[[calls]]` block reads a contract at a fixed block and
  stores the result as a table, optionally with calldata built from the row that triggered it - the
  `contract.balanceOf(event.params.user)` a subgraph would write. Pinning the block is what keeps it
  deterministic: the answer is fixed, so two operators re-running the same nest get the same bytes,
  and the result is content-addressed on `(chain_id, block, contract, calldata)`. Needs `--state-rpc`
  pointed at an archive node.
- **IPFS documents, verified** (RFC-0037). An `[[ipfs]]` block turns a column of content addresses
  into a table of resolved documents. Every body is re-hashed and checked against the CID it claims
  to be, so a gateway serving the wrong bytes yields no row rather than a plausible one. The CID is
  taken from whatever shape the contract stored - a bare CID, an `ipfs://` URI, a gateway URL, or a
  raw 32-byte digest - and **the host is discarded**, because that string came from a log and
  honouring it would let whoever emitted the event choose what your indexer connects to.
- **Factory / dynamic contracts** (RFC-0009). Watch a factory (e.g. a pool factory); children are
  discovered at runtime and indexed into shared `{template}__*` tables - no redeploy per child.
- **Derivation, three speeds.** Decoded events are tables, sealed as they index. Three built-in
  relations - balances, exposure, velocity - are incremental DBSP circuits; a reorg is a
  retraction. Nest-authored `views/*.sql` are named SQL evaluated at query time over hot ∪ sealed,
  not IVM. And a nest can declare its own **authored incremental entities** in `entities.toml`
  ([RFC-0041](docs/rfcs/0041-authored-incremental-entities.md)): a `SELECT` that DBSP maintains as
  blocks arrive, served from `/derived` and queryable by name from `/sql`, with reorgs handled as
  retractions like the built-ins. On a copy of the Lodestar nest that took the `indexer_rewards` panel
  from a p50 of 2.15 s to 87.7 ms ([`docs/bench/3.0.0-alpha-live.md`](docs/bench/3.0.0-alpha-live.md)).
- **Labels and flags** (RFC-0008). Address labels, threshold & velocity flags and
  counterparty-exposure views.
- **Alerts & webhooks** (RFC-0010). HMAC-signed egress with a durable at-least-once outbox; a slow
  endpoint never blocks indexing.
- **Built-in admin UI.** A self-contained page at `/_admin/` - status, tables, view/nest inspector.
  Localhost-open; off-localhost it requires a token per request.
- **Many nests, one runtime, one or more chains** (RFC-0012, RFC-0021). Host many nests in one
  process; nests on the same chain share a single cursor and one `getLogs` per window, and a
  runtime can span **multiple chains** with **one isolated cursor per
  chain** - a Base nest and an Arbitrum nest in one runtime. Per-nest isolation, and a footprint budget
  **per active-chain cursor** (≤2 GB). A capability, not a mandate: one chain per runtime stays the simple
  default.
- **Mount and unmount nests without a restart** (RFC-0027). `POST /_admin/nests` mounts one and
  `DELETE /_admin/nests/<name>` unmounts one, live, so a change to the nest set no longer stops every
  co-tenant. A mount is admitted only if it fits the cursor's RAM budget (refused, never warned),
  catches up *before* it joins so it never drags co-tenants back through history, and only then gets
  routes; an unmount is a drain, not a route removal. The set persists in `mounts.toml`, so a restart
  converges on what you last asked for. Suspend, resume, move a name to a new NID without a gap,
  dry-run a mount for its projected footprint, and fetch a NID from a registry at mount time: all in
  [nest lifecycle operations](docs/operators.md#nest-lifecycle-operations).
- **Scaled mode - a fleet across machines** (RFC-0022). When one box can no longer hold your cursors,
  the *same crates* run as three roles: a **control plane** holding what should run, a **writer pool**
  (`nuthatch worker`) whose members take cursor **leases**, and a **query-FE tier** (`nuthatch serve`)
  that serves from shared state and owns nothing. A role flag, never a fork, and opt-in at build time
  (`--features postgres-store`), so the published binary carries no database driver. Ownership is
  enforced by the store: every write carries a fence, and a stalled worker that wakes up finds its
  writes refused. Workers pull the nests they are assigned from a registry by content address, so
  re-tagging a version cannot change what a fleet runs. This is the **self-hosted distributed** path
  for one operator's cooperating nests; per-tenant billing and authz stay out of scope.
- **Nest bundles + registry - bundle one, publish it, load it anywhere.** `nuthatch nest bundle` packs
  a nest's authored inputs into one portable, content-addressed `.bundle`; `nest load <bundle-or-url>`
  verifies and installs it, regenerating the decode registry and asserting it matches, so anyone runs
  your *exact* nest. A **registry** (RFC-0019) is a filesystem path or any S3-compatible bucket
  (`AWS_*` env, `AWS_ENDPOINT` for non-AWS): `nest publish <bundle> --registry … --as name@version`,
  then `nest load name@version --registry …`. The registry is never mandatory; a bundle and
  `load <file|dir>` need none.
- **Mirror a nest to a bucket** ([RFC-0052](docs/rfcs/0052-the-mirrored-nest.md)). `nuthatch publish
  sync --target s3://bucket/prefix` copies a nest's sealed Parquet segments, its catalogue and a
  provenance envelope to any S3-compatible bucket or a directory, and `dev --publish-target` keeps
  the mirror current as segments seal, uploading in streamed parts so ingestion does not wait on the
  bucket. The mirror is keyed by the nest's data identity, not its NID: an edit that moves the NID but not
  the data identity, which is the cosmetic case "Safe upgrades" below describes, keeps publishing to
  the same dataset, and any edit that changes what is decoded forks a new one.
  `publish status` says what is still to upload, `publish verify` checks every object against the
  local segment (`--deep` re-downloads and re-hashes), and `doctor --publish` puts the mirror in a
  health check. A table's last few hundred rows wait to fold into its next segment and are not
  mirrored until they do; on a nest whose chain has gone quiet, `publish finalise` lets them go.
  Reading it needs no nuthatch: DuckDB, Trino or anything that reads Parquet, as
  [Reading a published nest](docs/reading-published-nest.md) describes.
- **Start a nest from a mirror** (4.9.0). `nuthatch seed --from <mirror> --dir <nest>` fills a nest
  that has not indexed from a path, an `s3://` prefix or a public bucket's `https://` address,
  checking every file against the mirror's catalogue, and the next `dev` follows the chain from the
  block after the mirror ends. The hashes show each file is the one the catalogue names, not that
  the catalogue is true to the chain: seed from an operator you would trust to run the nest. Four
  Graph Protocol nests are published at [nuthatch-indexer.com/mirror](https://nuthatch-indexer.com/mirror).
- **Safe upgrades - no resync tax** (RFC-0020, RFC-0033). Updating a nest is not a subgraph-style
  genesis resync, and in 2.0 it needs no command to remember. The **runtime** classifies the update
  when a nest's identity changes: *compatible* (additive only) is applied, *breaking* (a
  consumer-observable change - a dropped column, a removed table) is **named and refused** until you
  say `--allow-breaking`. Grafting does the rest: a **cosmetic** edit - a comment, a renamed view, a
  doc change - moves the nest's identity and **adopts the existing dataset**, so nothing re-indexes.
  Segments are content-addressed and shared across the runtime, so **two nests that decode the same
  contract hold one copy**, not two. What a subgraph pays a full resync for, nuthatch answers with a
  hash comparison.
- **Derive-first - the `eth_call` you don't need** (RFC-0023). The Graph Foundation's figure is that
  more than 70% of subgraphs call `eth_call`, much of it for reads that are *derivable* from the
  events they already index - they fetch only because they have no way to derive. Nuthatch does:
  `nuthatch recipe add total_supply` drops in a SQL view
  that computes an ERC-20's `totalSupply()` as Σ minted − Σ burned from Transfer events - deterministic,
  free, no archive node. That view runs at query time; it is not a DBSP circuit. It derives what a
  subgraph pays an archive node to fetch. For the handful of
  reads that *aren't* derivable but never change - `decimals`/`symbol`/`name` - `nuthatch metadata fetch`
  calls once and caches forever.
- **Ingestion that survives real providers** (RFC-0028). An oversized `eth_getLogs` is split and
  retried, taking the provider's own suggested range when it offers one; a failure we *cannot* classify
  is split once anyway, so an endpoint whose phrasing we have never seen still works rather than
  stalling. Rate limits, transport blips and credential rejections are told apart - a rejected API key
  is cooled down loudly instead of retried forever. And sealed segments now flush on a boundary derived
  from the **data**, not from wherever a fetch window happened to stop, so two operators indexing the
  same range produce byte-identical segments regardless of their RPC tuning.
- **Metrics.** Prometheus `/metrics` - tip lag, rows decoded/sealed, reorgs, query counts, RSS.

---

## Running it in production

nuthatch is built to be **fronted**, not exposed raw - gateways, auth, and metering are the operator's
layer; nuthatch ships the *guards* (query timeout, row cap, result-byte cap, concurrency limit, a
filesystem-access denylist on `/sql`) and *signals* (`/metrics`) that make fronting it safe. It binds `127.0.0.1` by
default; `--listen` elsewhere and put a gateway in front. See [`docs/operators.md`](docs/operators.md).

- **Footprint:** ≤2 GB RAM per active-chain cursor, one binary, graceful SIGTERM shutdown with
  checkpointed resume. The startup check counts the engine's memory pool, the ingestion reservation
  and a headroom against `NUTHATCH_MAX_RSS` and refuses a configuration that does not fit, naming
  the largest value that would; since 4.10.0 an operator can set the headroom and the reservation
  from measurements (`NUTHATCH_RUNTIME_HEADROOM`, `NUTHATCH_INGESTION_RESERVATION`;
  [capacity and sizing](docs/operators.md#capacity-and-sizing)).
- **Durability:** content-addressed segments are safe to copy while running; back up the nest directory.
- **Before you roll a release:** the [release gate](docs/release-gate.md) runs the candidate against
  a copy of a production nest with the statements it serves and compares every answer with
  production's. The same discipline applies to your own nest: compare answers, not just status.
- **`dev` is the serve command** - it backfills, follows the tip, and serves in one process.
  Copy-paste **systemd** and **Docker** recipes are in [`docs/operators.md`](docs/operators.md#deploy-recipes).
- **Outgrown one machine?** [Scaled mode](docs/operators.md#scaled-mode-a-fleet-across-machines-rfc-0022)
  spreads cursors across a writer pool with an independently-scaled serving tier. Reach for it when a
  single box cannot hold your cursors inside its RAM budget - not before, because several nests on one
  machine is simpler and that simplicity is the point of the embedded path.

**[`docs/operators.md`](docs/operators.md) is the full operating guide**, and worth reading before you
run this for real rather than after. **[`docs/verification.md`](docs/verification.md)** is its
counterpart: an acceptance runbook that *proves* a deployment works, step by falsifiable step, and says
plainly which levels we have verified ourselves and which we have not.

**Still deciding whether to trust it at all?** [`docs/kicking-the-tyres.md`](docs/kicking-the-tyres.md)
is a guide to *falsifying* nuthatch rather than confirming it: the cold walk, correctness against a
public subgraph, what it costs to keep running, a red-team pass on `/sql`, and where we have already
been wrong. We would rather you found the next one than a user did.

The guide covers the questions people actually hit:

| If you're wondering | Go to |
|---|---|
| how do I tune backfill against my RPC's limits? | [configuration surface](docs/operators.md#configuration-surface) - `--window`, `--concurrency`, `--seal-direct` |
| what do I scrape, and what should page me? | [observability](docs/operators.md#observability) - metrics, alerts, health vs readiness |
| what happens when something breaks? | [the failure model](docs/operators.md#the-failure-model) and the [runbook](docs/operators.md#runbook) |
| how do I back this up? | [data lifecycle](docs/operators.md#data-lifecycle) |
| how do I run an unlisted chain? | [running an unlisted EVM chain](docs/operators.md#running-an-unlisted-evm-chain) |
| what isn't finished yet? | [known gaps](docs/operators.md#known-gaps) - stated plainly |

---

## What a stable release means here

A major version is a promise about **stability**, not a claim of completeness.

- **5.x is the stable line.** Within 5.x, `nuthatch.toml`, `mounts.toml` and `entities.toml` keep
  working; a data directory upgrades drop-in, with no re-index; and the HTTP, SQL and MCP surfaces do
  not break. Upgrade only: a downgrade is not promised. Off-by-default cargo features are
  experimental and not covered. The full terms are the
  [stability contract](docs/operators.md#stability-contract).
- **Minors add, patches fix.** A released 5.x only gets patch releases; features wait for the next
  minor, and correctness and security fixes ship at once as patches. The cadence is whatever the
  work needs: 4.1.0 to 4.10.1 shipped between 2026-10-02 and 2026-10-05, each with notes under
  [`docs/releases/`](docs/releases/).
- **Upgrades are a binary swap.** No data migration, no conversion step. Proven on a production box
  across 0.3.0 → 0.6.0 → 0.7.2 and at each major since, and in CI: every build opens a frozen
  v3.13.2 data directory and reads it back exactly (`tests/upgrade_golden.rs`).
- **MSRV 1.95**, measured rather than asserted: it is what CI, `rust-toolchain.toml` and the release
  build all use. A version nobody tests is not a promise.
- **Embedded mode is the production path.** `dev` runs in production today, hosting one nest or many.
  Scaled mode is built and verified across real machines, but younger: until 0.9.3 its writer pool
  did not index at all. If one process per box is enough, that is still the shape to reach for.

**What is deliberately not here:** a hosted service, a token, telemetry on by default, non-EVM chains before EVM is
airtight, or any deployment story beyond binary + compose. Those are not backlog items; they are out
of scope.

## Security

nuthatch binds `127.0.0.1` by default and is built to be **fronted**. Before you expose `/sql` to
anyone you do not trust, read [`SECURITY.md`](SECURITY.md) - and be on a current release:

- **v0.9.3** fixes an **arbitrary file read** on `/sql`. DuckDB accepts a *quoted* function name and
  the guard only matched an unquoted one, so `SELECT * FROM "read_csv"('/etc/passwd')` executed. Every
  earlier release is affected.
- **v0.6.2** fixes an **arbitrary file write** on `/sql` via `;`-stacked `COPY … TO`.

Both have published advisories on the repo's Security tab. The full pre-1.0 adversary pass, including
the findings we closed as *not ours to fix* and why, is in
[`docs/security-audit-2026-07-31.md`](docs/security-audit-2026-07-31.md).

---

## Project

- **Design** lives in [RFCs](docs/rfcs/) (0001-0061, statuses in the
  [index](docs/rfcs/README.md)); the north star and the CLI/UX direction are
  [RFC-0015](docs/rfcs/0015-the-delightful-core.md). Deferred/leftover work is in
  [`docs/backlog.md`](docs/backlog.md); what each release changed is in [`docs/releases/`](docs/releases/).
- **Governance:** a self-funded public good, maintained by one person; everything is open source. No
  hosted service, no token, no phone-home. See [`GOVERNANCE.md`](GOVERNANCE.md) and the standing
  design brief [`CLAUDE.md`](CLAUDE.md).
- **Out of scope:** a hosted/metered service, non-EVM chains before EVM is airtight, or any deployment
  story beyond binary + compose.

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this
work by you shall be dual licensed as above, without any additional terms or conditions.

---

<p align="center"><i>be your own indexer.</i></p>
