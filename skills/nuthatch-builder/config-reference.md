# nuthatch config reference

The configuration surfaces a nest or a multi-nest runtime uses. Keys mirror the serde structs in
`src/config.rs`, `src/semantic.rs`, `src/runtime.rs` (mount config moved there when RFC-0032
retired the roost), `src/allowlist.rs` (the query ceiling, RFC-0034 phase 2), `src/calls.rs`
(`[[calls]]`), `src/ipfs.rs` (`[[ipfs]]`), and `src/entities.rs` (`[[entities]]`). CI checks this
**both ways**: the build fails if this file names a key those structs don't have, and equally if a
struct grows a key this file never mentions: a shipped key an agent cannot see is one it will never
use.

## `nuthatch.toml` - a nest

Written by `init`; edit by hand freely.

```toml
[nest]
name = "usdc"                 # nest name (also the default mount alias)
chain = "mainnet"             # a registry name: mainnet | arbitrum-one | base | bsc | polygon |
                              #   gnosis | optimism | monad | robinhood
chain_id = 1
rpc_urls = ["https://…"]      # tried in order, then round-robin failover
schema_version = 1            # config/data layout version (managed by nuthatch)
block_timestamps = true       # init-time only; false drops block_timestamp from every table

[[contracts]]                 # one or more
alias = "usdc"                # table prefix → usdc__transfer, usdc__approval, …
                              #   never `offchain` or `offchain_…`: its tables would land in
                              #   `offchain__`, where offchain snapshots are served. Refused at load
address = "0xA0b8…eB48"
start_block = 6082465         # optional; deployment block (init detects it)
abi = "abis/usdc.json"        # vendored ABI path
events = ["Transfer"]         # optional allowlist; omit to decode every event

[flags]                       # optional (RFC-0008 C3); amounts are base units as strings
threshold = "1000000000000"       # single-transfer flag ≥ this
velocity_amount = "5000000000000" # windowed-volume flag ≥ this
velocity_window = 7200            # window in BLOCKS, not seconds (≈24h at 12s blocks); default 7200

[[templates]]                 # optional (RFC-0009 factories)
name = "pool"                 # shared table prefix for discovered children
                              #   same rule as an alias: not inside `offchain__`
abi = "abis/pool.json"
filter = "topic0"             # optional backfill-STRATEGY override - HOW the range is fetched:
                              #   force topic0-only fetch for many-children templates
events = ["Swap"]             # optional event allowlist - WHAT is decoded from it.
                              #   omit/empty = every event the ABI defines. A name the ABI
                              #   lacks is refused at load, not silently empty at query time.

[[factories]]                 # optional (RFC-0009)
watch = "factory"             # ALIAS of the watched [[contracts]] (or a template, for nesting)
event = "PoolCreated"         #   - NOT an address; an overloaded event takes its full signature
child_param = "pool"          # the event param holding the new child's address
template = "pool"             # which [[templates]] the child uses
start = 12369621             # optional: ignore discoveries before this block

[[webhooks]]                  # optional (RFC-0010 Part B)
name = "large-transfers"
table = "usdc__transfer"
where = "value_dec > 1e6"     # NOTE: the toml key is `where`
url = "https://…"
batch_max = 100               # optional
finality = "sealed"           # optional: "sealed" (default) | "tip"
since = "registration"        # optional: "genesis" | "registration" | a block number
secret = "…"                  # optional HMAC signing secret

[[alerts]]                    # optional (RFC-0008 C5)
kinds = ["threshold_flag", "entity_fault"]
                              # what to deliver. `threshold_flag` is a compliance annotation (RFC-0008
                              # C5) and must match the emitted kind. `entity_fault` is
                              # operational: an authored incremental entity's circuit has stopped,
                              # which is terminal and quarantines the nest. Nothing validates this
                              # list, so a typo is a sink that silently never fires.
url = "https://…"
format = "raw"                # optional, default "raw". "discord" for a Discord webhook, which will
                              # not accept the raw payload: it answers 400 "Cannot send an empty
                              # message" and the delivery worker retries a non-success forever, so a
                              # raw sink there is an outbox that never drains. "discord" sends
                              # {"content": "…"} instead. An unrecognised value is a load error, not
                              # a silent fallback.

[[calls]]                     # optional (RFC-0023 tier 3) - a pinned `eth_call`, stored as a table.
                              # Needs `--state-rpc <archive endpoint>`; the endpoint is NEVER a config
                              # key, because it usually carries an API key and this file is pinned
                              # into the nest's content address.
                              #
                              # TWO FORMS, and exactly one applies.
                              #
                              # 1. SAMPLED - one fixed calldata, every N blocks. For an oracle read or
                              #    an ungoverned parameter.
name = "eth_usd"              # becomes the result table
contract = "0x5f4eC3Df…"      # the contract to call
calldata = "0xfeaf968c"       # hex, selector included; fixed arguments only
every = 1000                  # sample every N blocks (default 1000). Anchored on absolute block
                              # numbers, so a resumed backfill samples the same blocks.
start = 12369621              # optional: first block to sample from

[[calls]]                     # 2. ROW-DRIVEN (RFC-0038 §3) - one call per row of a table, with the
                              #    row's own values as arguments. This is what a subgraph mapping does
                              #    (`c.balanceOf(event.params.to)`), and it fires at the row's block.
name = "recipient_balance"
on = "usdc__transfer"         # the source table; mutually exclusive with `every`
contract = "0xa0b86991…"      # a fixed address, OR:
contract_column = "{pool}"    #   a column of `on` holding one (a factory's new child)
signature = "balanceOf(address)"   # the ABI signature; calldata is encoded from it
args = ["{to}"]               # `{column}` takes the row's value; anything else is a literal.
                              # An *indexed* dynamic param (string/bytes) is REFUSED: the log holds
                              # keccak(value), not the value, so it cannot be passed on.

[[ipfs]]                      # optional (RFC-0037) - resolve the documents a column's CIDs name.
                              # Needs `--ipfs <gateway-or-your-own-node>`; also never a config key,
                              # because a gateway is an access path and must not enter the content
                              # address. Every document is verified against its CID before storage.
                              # Documents resolve behind the cursor, retried with backoff, and one that
                              # fails 10 times is given up on (`nuthatch_nest_ipfs_given_up_total`):
                              # NO row rather than a wrong one, and the range seals once it is decided.
                              # Under `--seal-direct` one is also given up on after
                              # `--ipfs-window-deadline` (default 5m, `0` for none), and is then absent
                              # from the sealed segment. Each give-up is counted on `/ready` and listed
                              # at `/ipfs/gave-up`. Every retry is a warn line and counts in
                              # `nuthatch_nest_ipfs_retries_total`.
name = "token_metadata"       # becomes the result table
on = "nft__uri_set"           # the table whose rows carry the CID
cid_column = "uri"            # which column. A bare CID, `ipfs://…`, or a full gateway URL all work -
                              # only the content address is kept, never the host.
[[ipfs]]                      # the CID inside JSON rather than the whole column (RFC-0037 slice 5)
name = "qos_payload"
on = "data_edge__call_submit_qo_s_payload"   # a top-level call table works as well as an event table
cid_column = "_payload"       # a string, or `bytes` that are UTF-8, holding JSON
cid_json_path = "hash"        # ONE top-level key naming the CID. An object names one document, an
                              # array one per element.
json_match = { topic = "gateway_indexer_attempt_qos_5_minutes_prod_v3" }
                              # optional, needs `cid_json_path`: resolve only the objects whose string
                              # fields equal these, before any fetch. Both keys enter the nest identity.
                              # A row naming no usable CID writes nothing and counts in
                              # `nuthatch_nest_ipfs_unreadable_total`. A document is stored only once
                              # proven: re-encoded in Kubo's default layout, or reassembled from a CAR
                              # whose every block hashes to its CID. Otherwise NO row, counted in
                              # `nuthatch_nest_ipfs_unverified_total`, or `_oversize_total` past
                              # 16 MiB, 4,096 blocks or 16 levels.
[ipfs.rows]                   # optional (RFC-0037 slice 8): explode each proven JSON document into
                              # typed rows, so views read columns instead of parsing megabytes of JSON.
table = "qos_indexer_attempt" # the typed table. Every row also carries `cid`, `document_log_index`,
                              # `source_log_index` and `element`.
max_rows = 4096               # most elements one document may have; also the key room it is allotted.
                              # A block holds 124,000 typed rows in all. A document that does not fit
                              # is refused before any fetch.
keep_content = false          # default true; false stores the document row with empty `content`
columns = [                   # one per top-level key: name = the key, type = string | number | u64 |
                              # i64 | bool | address. `number` keeps the decimal text (cast it in a
                              # view); an absent string or number is "", any other absent type refuses.
  { name = "indexer_wallet", type = "address" },
  { name = "query_count", type = "number" },
]
                              # A document whose elements do not fit is refused WHOLE: no document row,
                              # no typed rows, given up on and counted in
                              # `nuthatch_nest_ipfs_rows_refused_total`. Enters the identity only when
                              # declared.

[extract]                     # optional
top_level_calls = true        # decode transactions sent directly to this nest's contracts - what a
                              # subgraph's `callHandlers` fire on (RFC-0038 §5). Ordinary RPC; no node
                              # needed. `traces` (internal calls) and `state` still are, and are
                              # refused at startup until one exists.
l1_blocks = true              # Arbitrum-family chains only (arbitrum-one, robinhood): an `l1_blocks`
                              # table, one row per block that carries a row of this nest, with
                              # `l1_block_number` from its header. Join on (block_number, block_hash).
                              # One header per such block, none for the rest. Refused beside
                              # `blocks = true`, whose `blocks` table already has the column for
                              # every block. Off: no table, no header fetched.
```

### `[address_history]` - an account's history for rotki (RFC-0063)

A nest with this table and no `[[contracts]]` watches accounts instead of contracts, and serves their
history Etherscan-shaped at `GET /api`, so rotki can use it as an indexer. It runs on its own light
path: no SQL engine, no sealing, one cursor polling the chain head.

```toml
[address_history]
addresses = ["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"]  # the accounts to watch; seeds the stored set
start_block = 0               # optional: history before it is never fetched, and reads incomplete
end_block = 20000000          # optional: stop here
poll_interval = "5m"          # optional, default 5m; `--poll-interval` overrides it
mev_relays = false            # optional, default off: ask the MEV-Boost relays, see below
request_deadline = "8s"       # optional, default 8s: the most a request waits, queue and background work included
validators = [2259472]        # optional, Ethereum only: validators whose proposed blocks to prove, see below
beacon_url = "http://127.0.0.1:5052"  # optional: a beacon API for blocks the mirror has not published
```

The cursor backfills each address up to the finalized head (the RPC's `finalized` tag, else the
registry's depth under the head), spending at most a poll interval a pass on address windows before
block bodies and produced blocks take their turn. An address whose window fails is retried at half the
span, down to the floor, then after 1, 2, 4 and up to 32 polls, while the others carry on. A request whose `endblock` lies above what is covered, and at or
below the chain head, is covered on demand by the cursor, up to the block asked for, within
`request_deadline`; otherwise it answers `NUTHATCH_INCOMPLETE`. Withdrawals and mined blocks need only
the block bodies covered, whatever trace discovery has reached, and their refusal names how far the
bodies are. A read reaching above the finalized
head goes through the cursor too, which checks the chain for a reorg before it answers. An explicit
`endblock` is answered whole or refused, never shortened to what is covered; one past the chain head
is refused. Without `endblock` the answer runs to the end of the covered run the start is in, and a
page past the first needs an `endblock`, so every page reads the same blocks. Blocks above the finalized
head are read against their block hash: each poll checks the newest such hash, and a reorg takes back
everything read above the fork, rows, coverage and caches, until it is read again. A conflict at or
under the finalized head stops the cursor and says so; history under it is never rewritten.

On Optimism, Base, Arbitrum, Polygon, BNB Chain and Gnosis the nest finds an account's normal and
internal transactions without `trace_filter`, which on these chains replays every block it covers or is
not served. Outgoing transactions are the blocks where the account's nonce rises; incoming value is
found where its balance rises by more than its own transactions explain, and only those blocks are
traced, with `debug_traceBlockByNumber`, which the trace RPC must serve. Each traced block's credits
must equal the balance's rise to the wei, or the window is not recorded. This proves the history of an
externally owned account: for a contract, or an account with EIP-7702 delegated code, every account
action answers `NUTHATCH_UNSUPPORTED` from the first block it had code, so the caller asks another
source. An incoming transaction that moves no value, a zero-value call or a failed transfer to the
account, is not listed, though Etherscan lists it; token transfers are found from logs as everywhere.

One `nuthatch dev` can serve several chains at one address: point `--dir` at a directory holding no
`nuthatch.toml` of its own, only one subdirectory per chain, each an address-history nest. Each runs its
own cursor and store as it would alone; `/api` answers as the nest `chainid` names, and a `chainid` no
nest serves answers `NUTHATCH_UNSUPPORTED`. `POST /api/watch` and `/api/unwatch` reach every chain (a
`validator` only Ethereum's); one chain refusing does not stop the others, and the answer, failed if any
chain refused, lists each chain's result, so a repeat converges them. `GET /api/watch` lists the union and, under `chains`, each chain's own list, `/ready` lists `chain_ids` and each
chain's state, and `/metrics` labels each sample with `chain_id`. Each chain reads its RPCs from
`NUTHATCH_RPC_<chain id>` and `NUTHATCH_TRACE_RPC_<chain id>`, else from its nest's `rpc_urls`;
`--rpc`, `--trace-rpc` and `--rpc-fallback` are refused there, because each names one chain's.

`getblocknobytime` answers from verified headers where they bracket the timestamp, and otherwise by
searching the nest's own RPC, proving the answer against its neighbour block by parent hash and,
above the finalized head, linking it by parent hashes to the head the search began from. It refuses a
timestamp newer than the chain head, and refuses on chains whose blocks can share a second
(Arbitrum, BNB Chain).

For a large, growing set of accounts, a deposit watcher's custody addresses say, the nest can read
each block once for the whole set instead of discovering each account on its own (RFC-0064). Its cost
is per block, not per account:

```toml
[address_history.sweep]
from_block = 21000000         # optional: where the sweep starts; omitted, the finalized head at first start
internal = true               # optional, default off: trace each block for internal transfers; needs trace_block
confirmations = 12            # optional: sweep to this many blocks under the head, not the finalized head
webhook = "https://processor.example/deposits"  # optional: POST the deposits feed here, at least once
```

A sweeping nest covers normal transactions and ERC-20, ERC-721 and ERC-1155 transfers for every watched
account from the block it joined. It never reads back: a read below that block, or over blocks already
swept without what it asks for, answers `NUTHATCH_INCOMPLETE` saying it is never covered, and only a
read past the swept head says to ask again. There is no token allowlist: every transfer naming a
watched account is served and is a deposit, so filter by the token contract. Accounts join at runtime in bulk, on a loopback bind only:
`POST /api/watch` with `{"addresses": ["0x…", …]}` adds them in one transaction, all or none, and answers
`{"added": n, "watched": total}`. With `internal = true` each block is traced once and internal
transfers, such as value a smart wallet or contract sends from inside a call, are served too, but
only over blocks swept with tracing on; without it a `txlistinternal` range answers `NUTHATCH_UNSUPPORTED`. The
sweep follows the finalized head, or with
`confirmations` that many blocks under the chain head; set `poll_interval` near the block time to see
deposits soon after they land.

Every incoming row that moved value is a deposit in one feed across the whole set; a failed
transaction, a frame inside a reverted call, a zero transfer and a `callcode` frame are served by the
account actions as Etherscan serves them, but are not deposits. The feed is read with
`GET /api?module=account&action=nuthatchDeposits&after=<seq>&offset=<n>`: entries in sequence order,
each `{"seq", "event": "deposit", "address", "action", "blockNumber", "row"}`, and `sweptThrough`, the
last block the feed is complete to. A reorg above finality adds `{"event": "retraction", "retracts":
<seq>, …}` for each deposit it took back, once, and the replacement arrives as a new deposit. With
`webhook` set the feed is POSTed as `{"deposits": [...]}`, oldest first; a batch counts as delivered
only after a 2xx, so a failed delivery is sent again, never skipped, and a consumer drops repeats by
`seq`.

On Ethereum mainnet, withdrawals, produced blocks and `getblocknobytime` can come from verified block
partitions (RFC-0063 §8) instead of one RPC call per block. It is off unless this table names a
mirror, and the mirror then sees the nest's IP and the block ranges it downloads, never an address:

```toml
[address_history.mirror]
url = "https://mirror.example/partitions"  # or a local directory of partitions, an import
chain_id = 1                  # must be the nest's chain
from_block = 17030000         # the blocks taken from the mirror; the RPC scan never reads them
# to_block = 17309999         # optional; omitted, as far as the mirror has published, checked every
                              # poll, the blocks above it read from the RPC until they are published
cache_mb = 512                # disk the downloaded partitions may hold; at least 64
```

`from_block` bounds what address history reads from the partitions; validator proofs read the
proposer catalogue from its first span (block 23,551,570) whatever it says.

`mev_relays = true` in `[address_history]` asks the MEV-Boost relays' public data API about each
block a watched address produced, or was paid for by a relay-built block's builder, and serves the
result as `module=account&action=nuthatchProducedBlocks`: fee recipient, block reward, the relays'
recipient and value, the builder's payment transaction, and `mev` of `relay`, `conflict` or
`unrecorded`, through the finalized head only, since a relay's answer about a block is used only
once the block is final. The rows come from the chain: every block the address received the fees of
is one. A
builder's payment, whether a transaction from the builder or an internal transfer in the block's last
transaction (a payout contract), becomes a row only on a relay's word. `unrecorded` means no delivery
at the queried relays when last asked, never zero MEV: a relay saves its record after the block is
published and can fail to, so such blocks are asked again daily for 30 days, and a delivery found
then upgrades the row or inserts the missing one. `builderPubkey` and `proposerPubkey` come from one
relay's claim: the deliveries for the canonical block hash, in the fixed relay order, the first whose
fee recipient is the watched address, else the first; `conflict` compares recipient and value only.

`validators` (or `POST /api/watch` with `{"validator": N}`, on a loopback bind) names validator indices
whose proposed blocks the nest proves, served as `module=account&action=nuthatchValidatorBlocks&validator=N`:
every block the validator proposed in the range, wherever its fees went, complete or refused, with
`supportedFrom` and `verifiedThrough`. The proposer of block N is proven by the beacon header the next
execution block commits to (EIP-4788), read from the proposer catalogue the mirror publishes and checked
against the chain like the partitions; `beacon_url` lets the nest build the catalogue itself above what
the mirror has published, and that beacon sees the machine's IP and the roots asked for. Ethereum
mainnet only, from the first block the catalogue covers; earlier ranges are refused.
It is off by default; the relays see this machine's IP and those block numbers.

On Ethereum mainnet a nest can also prove which blocks a validator proposed, whoever received their
fees: `validators = [1278077]` in `[address_history]`, or `POST /api/watch` with `{"validator":
1278077}`, and `module=account&action=nuthatchValidatorBlocks&validator=N&startblock=&endblock=`
answers every block validator N proposed in the range, or `NUTHATCH_INCOMPLETE`. Each row carries
`blockNumber`, `blockHash`, `timeStamp`, `slot`, `proposerIndex`, `feeRecipient`, `blockReward` with
`rewardStatus` (`verified`, or `unavailable` and retried, never zero) and the `mev` fields as above;
the answer carries `supportedFrom` and `verifiedThrough`. The proof is the execution chain's own: block
N+1's `parent_beacon_block_root` (EIP-4788) commits to the beacon header that produced block N, whose
`proposer_index` names the proposer, and every row of a span is checked before any is used. The
catalogue covers slot 12,777,208 (block 23,551,570) onward; a range starting earlier is refused. Spans
come from the mirror's `proposers/` files beside its partitions; for blocks the mirror has not
published, `beacon_url = "http://localhost:5052"` builds them here (or `NUTHATCH_BEACON_URL` in the environment, which wins and keeps a keyed URL out of `nuthatch.toml`) (the beacon sees this machine's IP
and the roots asked for), and without it they stay incomplete. Like the partitions, the proof is
relative to the chain the nest's own RPC calls finalized. `nuthatch partitions --proposers
--beacon-url URL` builds the catalogue for a mirror from its block partitions.

Each partition is verified before use: every header hashed, linked to its parent and anchored to the
block the nest's own RPC calls finalized, every withdrawal list hashed to its root. One that is
missing or fails stays `NUTHATCH_INCOMPLETE`. `nuthatch partitions` builds them.

The table stays out of the nest's identity and its bundle: the accounts are private and change at
runtime through `POST /api/watch` and `POST /api/unwatch` with `{"address": "0x…"}`, accepted only on
a loopback bind; any other bind logs a warning at start. `addresses = []` is valid, for a nest that
serves timestamp lookups and accounts added later. A range the nest has not fetched answers
`NUTHATCH_INCOMPLETE`, never an empty list. `/ready` answers 503 until the first finalized head is
read, and still carries `chain_id`; `served_through` gives, per action family the nest serves, the block
up to which every account the family reads for is answered without waiting, `null` before any is:
`transactions` (the five discovered actions, every watched account), `bodies` (withdrawals and mined
blocks, Ethereum) and `produced` (with `mev_relays`), both over the accounts asked for them (every account while validators are watched), since a plain
wallet is not body-scanned until it asks. An account above its unprovable marker is refused for
good rather than waited on, and so does not hold `transactions` back. A sweeping nest reports the end
of its swept span as `transactions`: it never reads back, so below that nothing is still to come. A
client that stops there is answered without waiting. It reports `stalled: true` while the last poll failed or no window advances, lists each address
whose windows keep failing under `stalled_backfill` (failures, span, `retry_in_polls`, the last error),
and 503 with
the reason once a finalized block the nest committed changes hash (a reorg under finality, or an RPC
on another chain with the same id), after which nothing is answered. The conflict is kept in the
store across restarts; removing the address-history store is how a nest reads its history again.

The rows are Etherscan's account records with a narrower field set on two row types: token rows
(`tokentx`, `tokennfttx`, `token1155tx`) omit `tokenName`, `tokenSymbol`, `tokenDecimal`, `gasUsed`
and `functionName`, and internal rows carry an empty `input`. rotki's readers of those row types use
none of them. On OP-stack chains (Optimism, Base) a `txlist` row carries `L1FeesPaid`, the receipt's
L1 data fee in wei, as Etherscan's does; a deposit from L1 has none. `txlistinternal` with `txhash`
traces a transaction the nest has not traced yet, but off a loopback bind only one it holds for a
watched account: like the proxy, the operator's trace RPC and disk are lent to anyone only on
loopback. `logs/getLogs` honours `page` and `offset` (at most 1,000), asks the RPC in spans that
start at 16 blocks and grow while they come back sparse, narrows any answer past 4 MiB, stops once
it holds the page, and runs two
requests at a time; a range that needs more than 64 calls answers `NUTHATCH_INCOMPLETE`, and a first
page whose logs all fall inside one block answers `NUTHATCH_UNSUPPORTED`, since paging from that block
would return the same page forever.

What leaves the machine: the RPC provider sees the watched addresses in its trace and log filters
(history stays local; the provider's view does not); the mirror, if named, sees the IP and block
ranges; the relays, if turned on, see the IP and block numbers.

## `entities.toml` - authored incremental relations (RFC-0041)

An entity is a maintained keyed relation. Its declaration is authored input, included in the nest
identity, and its SQL lives beside it under `entities/`. The declaration is deliberately separate
from an ordinary view: it names the point-read key and the maximum live input admitted to the
incremental runtime.

```toml
[[entities]]
name = "delegated_balance"          # stable entity name, and the SQL file stem
sql = "entities/delegated_balance.sql" # exactly one file under entities/
key = ["indexer", "delegator"]     # one or more output columns, in point-read order
max_rows = 1000                      # the most rows the relation may hold; crossing it faults
                                     #   the entity, and admission charges 3,200 bytes per row
```

`name` must match the SQL filename, `key` must name output columns, and `max_rows` must be greater
than zero. An empty manifest is refused rather than quietly meaning that the runtime has no work to
do. The relation is maintained as blocks arrive and served at `/derived/{name}`, by key at
`/derived/{name}/{key}`, and by name from `/sql`; [entities.md](entities.md) has the admitted SQL,
the cost and the fault semantics.

## `maintained.toml` - views answered from a stored copy (RFC-0062)

Names authored views (from `views/*.sql`) that the nest answers from a Parquet copy of their own
evaluation instead of evaluating them on every request. Nothing about the view changes: same SQL,
same answer to the byte. Only how often it is computed does.

```toml
recent = 4          # optional: older copies kept per view beside the current one (default 4)

[[view]]
name = "bet"        # an authored view; the name `CREATE VIEW` gives it

[[view]]
name = "token"
```

A copy is keyed on everything the view reads, so it is used only while none of that has moved: a
block with rows for the view's tables, a seal of them, an edited view or a new binary all mean the next
request answers from the definition as before, and the copy for the new state is built behind it.
Refused at startup, by name, and by `nuthatch check`: a name that is not an authored view, a view
whose definition (or any view it reads) calls a volatile function such as `now()` or `random()`, and
one that reads an incremental entity, an `offchain__*` view, `labels` or a `{template}__children` view.

In the NID, out of the data identity, like `queries.toml`: declaring a view does not stop a nest
seeding from a mirror. Copies live in `maintained/` beside `segments/`, are derived, are never
published, and are safe to delete. [views.md](views.md) says when to declare one.

## `semantic.toml` - what the data *means* (RFC-0016)

Generated by `init`; edit descriptions freely. The `[table.*.footguns]` are **derived** from the ABI
and regenerated - leave them alone.

```toml
schema_version = 1

[nest]
description = "USDC transfers on Ethereum mainnet."

[table.usdc__transfer]
description = "An ERC-20 token transfer."
grain = "one row per Transfer event"

[table.usdc__transfer.columns]
from = "The sender address."
value = "Amount, base units (6 decimals). Use value_dec for arithmetic."

[table.usdc__transfer.footguns]   # DERIVED - do not edit
reserved_words = ["from", "to"]   # double-quote these in SQL
big_ints = ["value"]              # SUM(value_dec) is the values that fit; WHERE NOT
                                  # value_overflow is that same sum
overflows_dec = []                # the subset of big_ints too wide for DECIMAL(38,0).
                                  # _dec is NULL past 38 digits, so a sum of _dec omits
                                  # those rows. WHERE NOT col_overflow is that same sum.
                                  # CAST(col AS DOUBLE) for a price that never fits.
bools = []                        # Solidity `bool` columns, stored as exact text 'true'/'false'
                                  # rather than a SQL boolean. A direct comparison (col = true) or
                                  # a boolean operator (AND/NOT) casts implicitly and works; a
                                  # function needing one type across its arguments (COALESCE, CASE,
                                  # bool_and/bool_or, UNION) does not, and fails to build with
                                  # "an explicit cast is required". Write col = 'true', or
                                  # CAST(col AS BOOLEAN).

[view.top_recipients]             # optional (RFC-0018): what an authored view means
description = "The 100 addresses receiving the most USDC, by total value."
```

`[view.*]` is rendered into `/schema` and the MCP exactly like a table, so an agent sees the
derivation by name and what it means rather than rediscovering it from SQL.

## `mounts.toml` - many nests, one runtime (RFC-0012; multichain RFC-0021; renamed in 2.0)

**The roost is retired.** There is one command - `nuthatch dev --dir <dir>` - and what it runs is a
property of the directory: a `nuthatch.toml` runs that one nest, a `mounts.toml` runs every nest it
mounts. `roost dev` no longer exists, and a directory still holding a pre-2.0 `roost.toml` is refused
with a pointer to `nuthatch migrate` (which moves data and never re-indexes).

**This file is runtime state, not authored config.** `migrate` writes it and the runtime keeps it in
step.


One chain or several, the shape is the same: chains are listed under `[[chains]]` (a top-level array
beside `[runtime]`), one isolated cursor per chain, and each nest declares its own `chain` in its
`nuthatch.toml`. Nests on the same chain share that chain's cursor:

```toml
[runtime]                     # was [roost] before 2.0
name = "my-runtime"
max_rss_mb = 2048             # optional per-CURSOR RAM ceiling (default 2048); the runtime's
                              # total budget is Σ cursors
default_tenant = "acme"       # optional (default "default"); the tenant a mount without `tenant`
                              # belongs to, served with no tenant segment in the route. Opaque -
                              # nuthatch refcounts a tenant, nothing more.
suspended = ["usdc"]          # runtime state: mounts suspended over the admin API. Kept on disk,
                              # neither indexed nor served (503) until `POST /_admin/resume/<name>`.
nests = []                    # pre-2.0: the nests resolved at nests/<name>/. Still read, but only
                              # for an alias no [[mounts]] record names; `migrate` moves them across.

[[chains]]
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://…"]

[[chains]]                    # a second chain is a second cursor (RFC-0021); a chain with nothing
chain = "base"                # mounted on it yet starts its cursor on the first mount
chain_id = 8453
rpc_urls = ["https://…"]

[[mounts]]                    # what is mounted, by identity
alias = "usdc"
nid = "9f2c…"
```

The pre-2.0 top-level form (`chain`/`chain_id`/`rpc_urls` directly under `[runtime]`) is **refused
at startup**: `runtime 'my-runtime' uses the pre-2.0 top-level chain form. 2.0 declares chains under
[[chains]] - one form, one meaning. nuthatch migrate rewrites it for you.` The single-cursor law
holds per chain - never multiplex two chains behind one cursor.

### `[[mounts]]` - where a nest's data actually lives (RFC-0032)

**Runtime state, not authored config. Do not hand-write it.** `nuthatch migrate` writes it, and the
runtime keeps it in step.

```toml
[[mounts]]
alias = "usdc"                  # the name in `nests` and the route prefix `/usdc/…`
nid = "9f2c…"                   # 64 hex chars: the nest's identity, and its dataset key
```

Before 2.0 a runtime resolved each nest at `nests/<name>/`. Now the nest is read from `data/<nid>/` - addressed by *what the nest is* rather than by what an operator
called it. Both resolve, so a partly-migrated directory is a supported state.

Run `nuthatch migrate --dir <dir> --dry-run` to see the plan before applying it. The migration
moves data and never re-indexes; if two aliases turn out to be the same nest, it says so and merges
them onto one dataset.

**Two aliases may name the same `nid`, and that is the point.** They share one dataset: one store,
one place in the cursor, one backfill, two routes.

```toml
[[mounts]]
alias = "acme-usdc"
nid = "9f2c…"

[[mounts]]
alias = "globex-usdc"
nid = "9f2c…"        # same nest, same data, indexed once
```

`/acme-usdc/…` and `/globex-usdc/…` both serve it. `GET /nests` reports each mount's `nid` and its
`shared_with` list, so a shared dataset is distinguishable from two separate backfills, and the
alias's `estimated_rss_mb` is `0` because the footprint was charged once to the dataset.

Sharing is by *identity*, so it survives nothing and forks on anything: edit either nest's inputs and
the NID changes, the mounts stop matching, and the edited one gets its own dataset automatically.

### `sql` and `[[mounts.queries]]` - bounding a public endpoint (RFC-0034)

`/sql` accepts arbitrary SQL. That is the product locally and a liability publicly: the node guards
(concurrency 2, 30 s, 50,000 rows, 2,000,000 hot rows, 16 KB of query text) bound one query and say
nothing about *which* queries a nest will answer.

```toml
[[mounts]]
alias = "usdc"
nid = "9f2c…"
sql = "allowlist"               # "open" (default) | "deny" | "allowlist"

[[mounts.queries]]
name = "recent_to"
sql = "SELECT \"from\", value FROM usdc__transfer WHERE \"to\" = {who} LIMIT {n}"
params = { who = "address", n = "int" }
```

A named query reads decoded tables, authored views and entities by name. The built-in `balances`
circuit is not a SQL relation (it is served at `/balances`); `nuthatch recipe add balances` writes a
`{alias}_balances` view if you want one in SQL.

| `sql` | Behaviour |
|---|---|
| `open` (default) | Arbitrary `/sql` and `/explain`, guards only. Unchanged from every prior release. |
| `deny` | `/sql` and `/explain` return `403`. The typed routes (`/tables`, `/entity/{id}`, `/balances`, …) still serve. |
| `allowlist` | Only the declared queries answer, via `GET /q/{name}`. Free-form returns `403` naming the allowed set. |

**The caller sends a name and arguments, never SQL.** Parameter types are `int` and `address` only -
both have a total validating parse into a form with no escaping hazard. `text` is deliberately absent:
free text needs escaping, and "we escaped it carefully" is how that class of bug ships.

`GET /queries` lists the surface with each query's parameters, and works on a bounded nest without
being refused - so a caller can discover what to ask before being told no.

Two configuration mistakes are **refused at load** rather than serving something surprising: declaring
queries while leaving `sql = "open"` (the control would silently not apply), and `sql = "allowlist"`
with no queries (the nest would answer nothing).

A declared query is still subject to every node guard. Being on the allowlist means the operator is
willing to answer it, not that it is cheap.

**This is mount config, not manifest**, so changing it leaves the NID untouched and re-indexes
nothing - and two tenants sharing one dataset can expose different surfaces over it.

### `[mounts.publish]` - mirroring a mount's dataset (RFC-0052 S2)

A mount can mirror its dataset's sealed segments to a directory or an S3-compatible prefix as they
seal, for Trino, Snowflake, BigQuery, Databricks or DuckDB to read as external tables:

```toml
[[mounts]]
alias = "usdc"
nid = "9f2c…"

[mounts.publish]
target = "s3://my-bucket/nuthatch"   # or a directory; credentials come from the usual AWS_* env
interval = "60s"                     # optional: reconcile at least this often (a seal wakes it sooner)
parallelism = 2                      # optional: uploads in flight, 1 to 16
```

Files land under `<target>/<data identity>/`, one directory per table, then the catalogue, then
`publish.json`; only non-provisional segments are published, and nothing unsealed ever leaves the
box. An empty target, an unparseable or zero `interval`, or a `parallelism` outside 1 to 16 is
**refused at load**. A mount added to a running runtime starts its mirror if its record carries this
table; unmounting stops it. A solo nest uses `--publish-target` on `dev` or `serve` instead, and a
runtime directory refuses that flag.

**This is mount config, not manifest**, so adding or changing it moves neither the NID nor the data.
Two mounts of the same dataset that name the same target publish one copy.

### `[mounts.counter]` - charging at your own endpoint (RFC-0046 S2)

**Absent from an ordinary build.** The counter is behind the off-by-default `counter` Cargo feature,
so a binary built without it has no `[counter]` key, no price, and no payment code at all. That is
the point rather than an accident: delete every payment feature from the tree and a self-hoster
loses nothing (#1217 is the test that fails if it ever stops being true).

With the feature on, an operator may put a price on one of their own mounts:

```toml
[[mounts]]
tenant = "default"
alias = "usdc"
nid = "..."
sql = "allowlist"

[mounts.counter]
price = "1000"                                          # USDC base units, as a string
recipient = "0x1111111111111111111111111111111111111111"
network = "testnet"                                     # or "mainnet"
```

| Key | Meaning |
|---|---|
| `price` | Cost per named-query request, in **USDC base units**, quoted as a string so TOML cannot round a 256-bit amount before the verifier reads it. Must be greater than zero; a zero price is refused at load rather than serving free. |
| `recipient` | The address the authorisation pays. Refused at load if it is not an address. |
| `network` | `mainnet` or `testnet`, selecting the EIP-3009 domain the signature is checked against. |

A priced mount answers an unpaid request with `402` and a challenge naming the exact terms; a request
carrying a valid `Payment-Signature` is served. The counter **verifies and records, and settles
nothing** - `nuthatch settle` drains the recorded authorisations, deliberately outside the request
path so no facilitator sits between a question and its answer.

Only named queries (`sql = "allowlist"`) can be priced. The authorisation log is
`authorisations.jsonl` beside the nest, and a nonce is spent per **payer**, so two buyers choosing
the same nonce do not collide. An authorisation is recorded only once an answer exists to release,
so a refused or failed query spends nothing.

**The `402` carries a quote (RFC-0048).** The body's `quote` names the flat `ceiling`, the byte
`bound` the statement planned to (Parquet scans, cold and hot source bytes, the tables charged) and
the `snapshot` it was taken against: `catalogue_hash` (sha256 of `segments/manifest.json`),
`sealed_through`, `hot_rows` and `hot_bytes`. Its `id` also rides in the challenge as
`accepts[0].extra.nuthatchQuote`. A client that sends the id back in a `nuthatch-quote` header is held
to that snapshot: if the catalogue or `sealed_through` has moved, the answer is `409` with a fresh
quote and nothing is recorded. Without the header the request is served against the current snapshot
at the same flat price. A statement that cannot be bounded, or whose bound exceeds the admission cap,
is refused with `422` rather than quoted.

**Settling.** `nuthatch settle --dir <nest> --exec <cmd>` hands each pending authorisation to `<cmd>`
on stdin: exit 0 settled, 2 failed, anything else deferred. `--batch N` hands up to N at a time, one
JSON line each, and reads one outcome line per row back
(`{"network","payer","nonce","outcome":"settled"|"failed","detail"}`); a row the command does not
report stays pending whatever its exit code. Either way the command must be idempotent by network,
payer and nonce, because a crash can land between settling on-chain and journalling it here. A payer
whose authorisation failed is refused from then on. Run it on whatever schedule suits the operator;
`--dry-run` lists the queue and writes nothing.

### `queries.toml` - the author's ceiling (RFC-0034 phase 2)

A nest **author** can declare the maximum surface they sanction. A mount may then narrow within it,
never widen it - so a published nest is self-describing about what it answers, which is a property a
registry needs and an operator cannot supply for someone else's nest.

```toml
# queries.toml, beside nuthatch.toml
[[queries]]
name = "recent_to"
sql = "SELECT \"from\", value FROM usdc__transfer WHERE \"to\" = {who} LIMIT {n}"
params = { who = "address", n = "int" }
```

Two ways to widen, both refused **at startup**:

- exposing a name the ceiling does not contain
- keeping a sanctioned name and changing its statement or parameters (widening by redefinition is
  still widening, and a name-only check would miss it)

No `queries.toml` → no ceiling, and a mount exposes whatever its operator chooses. An **empty**
`queries.toml` is different: it sanctions nothing.

**Why its own file rather than a section in `nuthatch.toml`.** The ceiling is an authored input, so it
belongs in the NID. But `nuthatch.toml` is part of the *data identity*, so a ceiling living there would
move the data identity on every security tweak and force a full re-index - the exact cost phase 2 was
sequenced behind early cutoff to avoid. `queries.toml` is in the NID and out of the data identity,
exactly like `views/`.

### Tenants (RFC-0032)

A mount belongs to a **tenant**: an opaque string nuthatch refcounts and knows nothing else about. No
authz, no quotas, no metering - identity is the gateway's job. Omit it and the mount belongs to
`[runtime] default_tenant`, which is `default` unless set.

```toml
[[mounts]]
tenant = "acme"
alias = "usdc"
nid = "9f2c…"

[[mounts]]
tenant = "globex"
alias = "usdc"        # the same alias is fine - it is unique WITHIN a tenant
nid = "9f2c…"         # same nest, one dataset, one backfill
```

**Routes carry the tenant unless it is the default one.** A mount of the default tenant (`default`,
or whatever `[runtime] default_tenant` names) serves at `/usdc/…`; any other tenant's at
`/acme/usdc/…`. Each mount's route depends on its own tenant alone (3.13.1), so adding or removing
another tenant never moves it, before or after a restart. A runtime whose mounts all belong to the
default tenant never shows the word in a URL. With `default_tenant = "acme"`, a mount that omits
`tenant` is an `acme` mount served at `/usdc/…`, and one that says `tenant = "default"` is an ordinary
other tenant at `/default/usdc/…` (#1933). `GET /nests` reports each mount's `tenant` and
`base_path`.

Both `tenant` and `alias` are path segments, so both are restricted to letters, digits, `_` and `-`.
Opaque means nuthatch does not interpret it, not that it may contain `..`.
