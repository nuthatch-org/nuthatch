# nuthatch workflows

Copy-paste-runnable recipes. Flags are authoritative in [cli-reference.md](cli-reference.md); config
keys in [config-reference.md](config-reference.md).

## Index one contract (the happy path)

```sh
nuthatch init 0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48 --alias usdc   # chain auto-detected
nuthatch dev --backfill 300    # the last 300 blocks; omit to backfill from deployment
nuthatch sql "SELECT count(*) FROM usdc__transfer"
```

Without `--alias` the prefix is the contract's name from the ABI (`fiat_token_v2_2__transfer` for
USDC). If auto-detect can't find the contract (or you want a specific chain), pass `--chain` with a
registry name: `mainnet`, `arbitrum-one`, `base`, `bsc`, `polygon`, `gnosis`, `optimism`, `monad`,
`robinhood`. `init` ends by printing the `nuthatch dev` line to run next and, for a long history, the
same line with `--backfill N` for the recent window only. To index only recent history instead of
from deployment: `nuthatch dev --backfill 100000` (last 100k blocks). Point at your own node with
`--rpc https://…` to dodge public-RPC limits.

## Index several contracts together

Pass multiple addresses to `init`, or grow an existing nest with `add` (no re-init):

```sh
nuthatch init 0xUSDC 0xWETH --alias usdc,weth
# …later…
nuthatch add 0xDAI --alias dai        # resolves the ABI, appends to nuthatch.toml, regenerates artifacts
```

`add` never re-detects the chain (a nest is one chain) and refuses an address already present. It
changes the nest's decode registry, so the next `dev` on an existing store **refuses to start** rather
than resume: the stored rows were decoded under the old registry. Re-index to adopt it. Moving
`nuthatch.redb` and `segments/` aside works; on a nest that is serving, build a copy to tip on a spare
port, compare it, then swap the directories, which keeps the outage to a restart.

## Fast backfill of a long history

For a from-deployment backfill, seal finalized history straight to Parquet (skip the hot store) and
overlap RPC latency:

```sh
nuthatch dev --seal-direct --concurrency 8
```

Keep `--concurrency` at 1 against a single public endpoint (high concurrency to one host can stall the
runtime); use 8-16 only when you have multiple `rpc_urls` or your own node. For a *sparse* contract over
a long range, widen the getLogs window: `--window 50000`. `--window N` is where the adaptive window
starts and the widest range it will ever ask for; it still shrinks when a provider refuses a range.
Without it the window starts at the chain default and grows over empty ranges.

## Factories / dynamic contracts

When a factory deploys children (e.g. a pool factory), declare a template + factory in `nuthatch.toml`
(see config-reference). Children are discovered at runtime and indexed into shared `{template}__*`
tables, distinguished by the `address` column. Each template gets a `{template}__children` view:

```sh
nuthatch sql "SELECT address, discovered_block, parent_address FROM \"pool__children\""
```

No redeploy per child; the discovery is deterministic and survives restarts (rebuilt from stored
factory events).

## Publish and consume a nest (the deploy unit)

A nest is a self-contained, content-addressed deploy unit - ABIs vendored, config + semantics pinned.

```sh
nuthatch nest bundle .                     # → a <name>-<hash>.bundle file; prints the content-address
nuthatch nest load ./my-nest.bundle        # verifies every file hash + re-derives the decode registry
nuthatch nest inspect ./my-nest.bundle --json  # what it is and every endpoint it reaches, offline
# load also accepts an http(s):// URL to a .bundle, so a nest is shareable from any static host:
nuthatch nest load https://host/my-nest.bundle --expect <hash>
```

`load` fails loudly if the inputs don't reproduce the manifest's registry hash - same inputs + same
generator → same decode, verifiably. `semantic.toml` travels in the bundle and is hash-checked too.

You can also `init --from <git-url|dir>` to start from a published nest instead of an address.

## Run many nests in one process (RFC-0032)

One runtime hosts several nests behind one API, **one isolated cursor per chain**: nests on the same
chain share that cursor and one getLogs per window - N nests for roughly one nest's RPC cost - and a
second chain gets a second cursor. Create a `mounts.toml` (see config-reference) and:

```sh
nuthatch dev --dir .            # serves /nests + each nest: default tenant /<alias>/…, any other /<tenant>/<alias>/…
```

Each nest is isolated (own store, own reorg blast radius) but shares its cursor's finality view. A
per-cursor RAM budget (`max_rss_mb`, default 2048) refuses a mount projected to blow it; `GET /nests`
reports the projection per runtime and per mount.

## What happened, and where the tokens went (RFC-0071)

```sh
nuthatch metadata fetch                                       # once: decimals and symbols
nuthatch diff --from 16818024 --to 16818085                   # the range story
nuthatch diff --since-last --format markdown                  # everything sealed since the last run
nuthatch diff --flow --tx 0xHASH --block 16817996             # one transaction, every transfer in log order
nuthatch diff --flow --from 16818024 --to 16818057 --format dot > flow.dot
```

Without `--flow`, `diff` tells the range in a fixed order: every control-plane event (ownership,
pause, upgrade, `AdminChanged`, roles) in chain order and never capped; per token the `--top`
largest transfers and net flows; new holders (no transfer in of the token before the range, among
the rows this nest holds); ERC-20 approvals with 2^256-1 named as unlimited, then ERC-721
approvals; threshold flags and every labelled address seen; then every other table, told or
grouped as below. Each standard event is matched by topic0 and parameter shape, never by name.
New holders read the token's history before the range, and above 20,000,000 such rows the section
is omitted with that count rather than computed slowly. `--since-last` runs from the block after the
previous one to the sealed boundary and keeps its cursor in `$XDG_STATE_HOME/nuthatch/diff/` (or
`~/.local/state`), never in the nest directory, whose files count towards the NID. A range above the
sealed boundary is labelled provisional (`"final": false`); `--final-only` refuses it instead.
`text` and `markdown` are rendered from the `json` document, which is what `GET /diff` and the MCP
`diff` tool return byte for byte.

The flow reads ERC-20 `Transfer` rows, matched by event signature and parameter position, never by
name, and `Deposit`/`Withdrawal` as transfers from and to the zero address only when emitted by the
chain's WETH9-style wrapper (WETH, WBNB, WPOL, WXDAI, WMON). Amounts are exact,
scaled by `metadata.json` (base units, marked "decimals unknown", without it). Net flow is the sum of
the transfers seen, not a balance: native ETH, internal calls and tokens the nest does not index are
invisible, and every output says so. `--format json` carries every edge; text, `dot` and `mermaid`
show the `--top` largest per token. A range with more than 2,000,000 transfer rows, or one outside
what the nest has indexed, is refused rather than drawn in part, and so is one whose read passes
500,000 distinct (token, from, to) edges: memory follows edges, not rows. Without `--block`, `--tx`
makes one `eth_getTransactionReceipt` call to find its block.

On an address-history nest (RFC-0063) the same commands read the watched addresses' `txlist`,
`txlistinternal` and `tokentx` rows instead of event tables. A transaction's value and an internal
transfer are movements of the chain's own currency (ETH, BNB, xDAI, POL, MON), never summed with an
ERC-20; a row two watched addresses share is told once; a reverted row is listed and moves nothing.
The story adds the native rows in chain order, the first `--top` of them, and `/api` serves every
row. It sees everything around a watched address and nothing between two it does not watch, and says
so. A range any watched address has not been read over is refused with what is covered, rows above
the finalized head are provisional, and `nuthatch dev` serves the same document at `GET /diff`.

Every other table in range is listed below the flow, most rows first, and the per-table counts sum
to the range's total. A table with a `story` line in `semantic.toml` is told one sentence per row
(the first `--top` in chain order); one without is grouped: its rows, block span, emitting contracts,
and for each indexed parameter the distinct count and the three values on the most rows. Writing a
story line is the way to turn that list into sentences; see config-reference.md.

## Export rows as Arrow (RFC-0079)

For a large export, or when the next step is a dataframe, ask `/sql` for Arrow instead of JSON:

```sh
curl 'localhost:8288/sql?q=SELECT%20*%20FROM%20usdc__transfer' \
  -H 'Accept: application/vnd.apache.arrow.stream' -D head.txt -o transfers.arrows
```

`format=arrow` in the query string (or the `POST` body) does the same where a header cannot be set;
`/q/{name}` takes the `Accept` header only, since its query string is the query's parameters. The
body is one Arrow IPC stream, which pyarrow (`pa.ipc.open_stream`), Polars, DuckDB and Arrow JS read
as is. The envelope (`count`, `truncated`, `degraded`, `provenance`) is JSON in the
`x-nuthatch-answer` header, so read it there: there is no `rows` key to find. The rows are the JSON
answer's, same order, same caps; check `truncated` before treating an export as complete, and page
with `WHERE block_number > …` rather than raising `max_rows`. 256-bit values are still decimal text
in Arrow, so the `_dec` rules still hold. A refusal is JSON either way. For a point read or a small
panel stay on JSON: the schema makes the Arrow answer larger.

Arrow Flight SQL exists only in the flight build, where `dev` gains a flag for its listen address:
the `nuthatch-flight-<target>.tar.gz` release artefact from 5.17.0, the `<version>-flight` image, or
a source build with the `flight` cargo feature. The default binary has neither the feature nor the flag, and
`cli-reference.md` documents the default build, so point someone at the flight build before
suggesting Flight. It answers statements and the catalogue calls (catalogs, schemas, tables, table
types), so a client can list a route's tables, and names a runtime's mount with the
`x-nuthatch-route` header. ADBC's Flight SQL driver is the client it is tested with.

## Wire an AI client to a running nest

```sh
nuthatch dev &                        # the index the agent queries
nuthatch mcp --print-config           # copy-paste MCP client config (Claude Code / any client)
# or, Claude Code directly:
claude mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288
# a remote nest behind a gateway: name the variable holding its bearer token
claude mcp add nuthatch -- nuthatch mcp --url https://nest.example.com --token-env NEST_TOKEN
```

The agent then calls the `schema` tool (per-nest, with footguns + coverage), writes SQL, and gets
compact results with a provenance stamp. Fully offline; nothing phones home.

## Run it in production

`dev` is the serve command. Put it under systemd or Docker and a reverse proxy - copy-paste recipes are
in [`docs/operators.md`](https://github.com/nuthatch-org/nuthatch/blob/main/docs/operators.md).
Off-localhost, set `NUTHATCH_ADMIN_TOKEN` and bind behind a gateway (the `/sql` guards bound *how much*,
never *who* - auth is the operator's layer). On localhost without it, `dev` makes an admin token in
`<dir>/.admin-token` (owner-only) on first start and logs the `/_admin/?token=…` URL; every admin
request needs it, and `nuthatch watch --dir <dir>` reads it from there.
