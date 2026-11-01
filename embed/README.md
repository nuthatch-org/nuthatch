# The embeddable engine

nuthatch is one indexer with many ways to run it. The binary is the main one, and nothing here
changes it. This directory holds the other: nuthatch's core as a `wasm32-unknown-unknown` module that
a host drives through a small interface, for data that belongs to one user on one device (a wallet,
rotki, a dapp's frontend, `/play`). When many users share one view, run the binary instead.

The engine is opt-in and off by default. The binary does not build anything in this directory, and
wasmtime appears in no lockfile but the test host's. [RFC-0077](../docs/rfcs/0077-the-embeddable-engine.md)
is the design; this file is the interface it promises.

| Path | What it is |
|---|---|
| `src/` | `nuthatch-embed`: the event nest of RFC-0075, and its wasm-bindgen build for `/play` |
| `plain/` | `nuthatch-embed-plain`: the same nest as a plain core module, the one this interface describes |
| `hosts/browser/` | the reference browser host: `host.js`, OPFS, a worker, the Service Worker |
| `hosts/node/` | the browser host run in Node over files, as CI runs it |
| `hosts/package/` | the host package, `nuthatch-engine.js`, built from both (RFC-0081 S2) |
| `tests/wasmtime-host/` | the reference non-browser host on wasmtime, and the pinned-interface check |

Build the module with `plain/build.sh <out.wasm>`. It needs the `wasm32-unknown-unknown` target,
`wasm-opt`, and a clang that targets wasm32 (zstd's C, through `nuthatch-seal`).

## The interface, `abi: 1`

### Conventions

- **A core module.** One linear memory, exported as `memory`, at most 4 GiB. No WASI, no component
  model, no wasm-bindgen: any runtime that instantiates a core module can host it.
- **Pointers and lengths are `i32`, read unsigned.** A JavaScript host writes `ptr >>> 0`: past 2 GiB
  a pointer arrives negative. Store offsets and file lengths are `i64`.
- **Values cross as UTF-8 JSON**, except `nh_export`'s output and `nh_request`'s three arguments,
  which are raw bytes.
- **Every import must be supplied.** A core module has no optional imports. `log` may do nothing.
- **One instance runs one nest**, on one thread. Exports are never called from inside an import, and
  never two at once.

### Memory ownership

The guest owns its memory. The host borrows it in two directions:

- **In.** The host calls `nh_alloc(len)`, writes `len` bytes at the pointer it returns, and passes
  `(ptr, len)` to an export. The export takes ownership and frees the buffer, whatever it returns.
  The host never touches that pointer again.
- **Out.** An export returns `n`. `|n|` bytes wait in the guest's output buffer: the host calls
  `nh_alloc(|n|)`, `nh_output(ptr)`, reads `|n|` bytes, and calls `nh_free(ptr, |n|)`. The next export
  that produces output replaces the buffer. `n >= 0` is a result; `n < 0` is a fault.
- `nh_alloc(0)` is valid and `nh_free` of it is a no-op.

### Exports

| Export | Signature | |
|---|---|---|
| `nh_abi` | `() -> i32` | the version and what the build can do: `{"abi": 1, "capabilities": ["events"], "storage": ["bytes", "host"], "engine": "<nuthatch version>"}` |
| `nh_open` | `(ptr, len) -> i32` | open the nest from a config (below); answers `{"storage": "bytes" \| "host"}` |
| `nh_step` | `() -> i32` | advance until blocked: `{"idle": true}`, `{"requests": [...]}` or `{"wait_ms": n}` |
| `nh_resume` | `(ptr, len) -> i32` | the answers to the last `requests`, a JSON array; answers `{}` |
| `nh_request` | `(method_ptr, method_len, target_ptr, target_len, body_ptr, body_len) -> i32` | one HTTP request as the node's router answers it: `{"status", "content_type", "body"}` |
| `nh_export` | `() -> i32` | storage `bytes`: the whole store, raw bytes |
| `nh_alloc` | `(len) -> ptr` | guest memory for the host to write into |
| `nh_free` | `(ptr, len)` | give it back |
| `nh_output` | `(ptr)` | copy the last output to `ptr` |
| `nh_report` | `() -> i32` | a probe for the gates: what RFC-0075 S3's gate compares |
| `nh_memory_bytes` | `() -> f64` | a probe: linear memory in bytes, the high-water mark |

The probes' shapes are pinned like the rest; what they report is for the gates and not promised.

The module also exports what its linker leaves visible: `__getrandom_custom` and
`__getrandom_v03_custom` (the randomness hooks, which call `random_fill`), zstd's C shims
`rust_zstd_wasm_shim_calloc`, `rust_zstd_wasm_shim_free`, `rust_zstd_wasm_shim_malloc`,
`rust_zstd_wasm_shim_memcmp`, `rust_zstd_wasm_shim_memcpy`, `rust_zstd_wasm_shim_memmove`,
`rust_zstd_wasm_shim_memset` and `rust_zstd_wasm_shim_qsort`, and the globals `__data_end` and
`__heap_base`. A host must not call them. They are
pinned with the rest, because a host can see them.

### Imports, module `nuthatch`

| Import | Signature | |
|---|---|---|
| `now_ms` | `() -> f64` | wall clock in ms since the epoch: `/ready`, entity sampling, the RPC client's rests |
| `random_fill` | `(ptr, len)` | fill guest memory with random bytes; seeds hash maps only, and no answer depends on it |
| `log` | `(level, ptr, len)` | one UTF-8 line; level 0 is a panic, 1 a host-storage failure, 2 information (a hydration's summary) |
| `store_read` | `(handle, offset: i64, ptr, len) -> i32` | read up to `len` bytes at `offset`; bytes read, 0 at the end |
| `store_write` | `(handle, offset: i64, ptr, len) -> i32` | write `len` bytes at `offset`, growing the file; bytes written |
| `store_len` | `(handle) -> i64` | the file's length |
| `store_set_len` | `(handle, len: i64) -> i32` | truncate or extend; 0 |
| `store_sync` | `(handle) -> i32` | make written bytes durable; 0 |
| `store_close` | `(handle)` | release the file, and its lock |

The `store_*` imports act on a file the host opened at the guest's request (storage `host`, below).
Each returns a negative number for a failure and never throws or traps; the host logs the reason
itself. They are synchronous because every host has a synchronous file once it is open:
`FileSystemSyncAccessHandle` in a browser worker, `fs.*Sync` in Node, a file in Rust or Python.

### Faults

A negative length carries `{"fault": name, "message": text}`:

| Fault | |
|---|---|
| `bad_config` | `nh_open`'s config did not parse, named an unknown key, or asked for something the mode cannot do |
| `bad_call` | out of order: a second `nh_open`, answers that do not match the requests, JSON that does not parse |
| `not_open` | no nest yet, or storage `host` still opening: step first |
| `open_failed` | the store or the nest could not be opened |
| `locked` | storage `host`: another holder has the store (`open` answered `"locked": true`) |
| `storage` | storage `host`: an `open`, `remove` or `list` failed |
| `unanswered` | `nh_step` while requests are outstanding: `nh_resume` first |
| `stalled` | the engine waits on nothing a host can answer, or a query never finished |
| `follow_failed` | following the chain failed; the next `nh_step` starts again from the store |
| `unsupported` | asked of the wrong mode, as `nh_export` in storage `host` |
| `report_failed` | a probe failed |

A fault leaves the instance usable. A **trap** does not: a panic logs at level 0 and traps, and the
host drops the instance.

### `nh_open`

```json
{
  "nid": "my-nest",
  "rpc": ["main"],
  "storage": "host",
  "fresh": true,
  "memory_mib": 2048,
  "poll_interval_secs": 12,
  "files": {"nuthatch.toml": "...", "abis/token.json": "...", "schema.json": "...",
            "entities.toml": "...", "entities/received.sql": "...", "views/20-s4.sql": "..."},
  "mirror": {"base": "https://..."},
  "follow": true,
  "verified": true,
  "complete": true,
  "max_headers": 10000,
  "filters": [{"contract": "staking", "event": "TokensDelegated", "from": 408825706,
               "args": {"delegator": ["0x..."]}}],
  "window": 10000000,
  "until": 519817135,
  "source": "rpc",
  "attach": [{"name": "pools", "tables": ["indexers"]}]
}
```

`rpc` is required. It names endpoints; the host maps each name to a URL, so no URL or key enters the
module. `storage` is `bytes` (the default) or `host`. `fresh` (storage `host` only) removes the
nest's files before opening. `memory_mib` is the ceiling RFC-0075 §17 holds the module to, 2,048
unless set; a phone's is 600. `poll_interval_secs` is what `/ready` reports as the node's
`--poll-interval`. Unknown keys are refused, so a host never believes a setting took.

**The nest** is `files`, a nest directory's files by path, as text (RFC-0077 S4): `nuthatch.toml`,
the ABIs it names, `schema.json`, `labels/*.json`, `entities.toml` and its SQL, `views/*.sql`. The
module reads `nuthatch.toml` through the binary's own parsing and indexes it on the chain registry
the binary ships: its contracts and their events, templates and factories, labels, and flags. What
only a node runs is refused by name rather than ignored: `[[webhooks]]`, `[[alerts]]`, `[[calls]]` (accepted with source `mirror+rpc`, below),
`[[ipfs]]`, `[address_history]`, `[extract]`, a key nothing reads, and a config schema past 2.
Without a mirror, a contract must declare its `start_block`. A host that leaves `nuthatch.toml` out
and gives `contract` and `start` instead gets RFC-0075's young contract: one ERC-20's `Transfer` on
mainnet from `start`.

**`mirror`** (storage `host` only) hydrates the nest's sealed history from a published directory
before it follows the chain, as `nuthatch seed` fills a node's (RFC-0052 §8). The module computes the
nest's data identity from `files` as a node does from its directory, which names the dataset under
`base`; reads the seed snapshot and the catalogue it completes; checks them as `nuthatch seed` does;
and asks for every segment and seed tail, each appended to the pack and read back against its content
address before the catalogue names it. Every file the host gives that a node would hash into the
identity must be there, `schema.json` included. A hydration that does not finish leaves the store
marked, and the next open hydrates again from nothing. A store that finished is reopened from its pack
with nothing fetched (RFC-0081 S2): the dataset its files name must be the one it was hydrated from,
and every segment in the pack must still hash to its content address, or the open fails
(`open_failed`, ending "open it fresh"). The chain past it comes from the RPC, as for any reopened
nest. The report's `mirror` says `"reused": true` for such an open. `mirror.postcard` (`{"cuts": {table: block},
"digest": "..."}`) pins the catalogue as an RFC-0068 postcard does: cut at each table's block, it must
hash with the mirror's `schema.json` to `digest`, no seed tail is read, and the nest is what the
publisher had sealed through the highest cut. A pinned nest does not follow.

Before it asks the mirror for anything, the module adds up what the nest declares against the
ceiling, as RFC-0075 §17 charges it: each entity 8 MiB plus 3,200 bytes per `max_rows`, against the
three quarters of `memory_mib` a statement leaves. A nest that cannot fit is refused (`open_failed`)
with its figures, and nothing is downloaded. The segments themselves go to the host's storage and
are read by range, so they are not charged.

**`follow`** is whether `nh_step` follows the chain past what the store holds; true unless the
mirror is pinned. False, `nh_step` answers `{"idle": true}` once the store is open.

**`verified`** (RFC-0077 S5) holds every seal to a finalized block the host verified, from a source
the RPC cannot forge: Helios's beacon light client in a browser (`verifiedFinalized` in
burrmill-wasm's `web/play/verified.js`), or whatever a non-browser host brings. Once a follow, the
module asks for it with a `verified_finalized` request; then it fetches every header from the last
seal up to that block from the RPC, hashes each one itself, links them by parent hash down from the
verified hash, and seals only at or below it, and only rows whose block hash is on that chain. If
the host has no verified block, the chain does not reach it, a header is forged or missing, or a
row names a block hash off the verified chain, sealing stops there and the next `{"idle": true}`
says why in `unverified`, by block; the cursor carries on, and its rows stay in the hot store. Rows
above the verified block are still what the RPC said, and an RPC that leaves a log out is caught only
with `complete` (RFC-0065 §6). Without `verified`, the module seals on the chain's own policy, as a node does.

**`complete`** (RFC-0081 S4, with `verified` and `filters`) catches an RPC that leaves a log out. For
each block under the verified head, the module tests the verified header's `logsBloom` for each
filter's address and topics; where the bloom admits a filter and the RPC returned no log of it, it
asks for the block's receipts (`eth_getBlockReceipts`, an `rpc` request like any other), hashes them
into the receipt trie, and takes the filter's logs from them only if the root is the header's
`receiptsRoot`. A bloom false positive costs that one fetch and changes nothing; a receipts list that
does not hash to the root holds the seal below its block, and `unverified` names it. A block is sealed
only once it is checked against the header on the verified chain, so what an earlier follow indexed
above the head it then had, or before a reopen, is indexed again once the head passes it. A log's
address, topics, data, index and block are what the root covers; its transaction hash is the RPC's,
as it is for every log. The cost is the receipts: on mainnet a block's bloom has about three quarters
of its bits set, so a filter on one contract's address admits about half the blocks it never logged
in, each a receipts list of about a megabyte. **`max_headers`** (10,000 unless set) is the most
headers one follow may hash and test: a fresh store whose `until` puts more past its start is refused
at `nh_open` (`bad_config`) before any request, and a follow whose verified head is more past what is
hashed fails (`follow_failed`) before any RPC request, each with the figure.

**`filters`** (RFC-0077 S7) narrow the nest to the logs a host names, such as one user's own.
Each names a contract by its alias, an event by its name (or its signature where the ABI overloads
the name), optionally the first block to ask from (never before the contract's `start_block`), and
for any of the event's indexed arguments the values it may take, addresses or 32-byte words. The
cursor then asks for those logs only: one `eth_getLogs` per filter and value, since an endpoint that
serves ten million blocks to a filter with one value per topic serves far fewer to one with a list
(arb1.arbitrum.io: 10,000,000 against 100,000). An event no filter names is not fetched, and its
table stays empty: a narrowed nest is a selection, not the contract's history, so `filters` are
refused with source `mirror`, as is a factory. Source `mirror+rpc` is how a narrowed nest reads a
mirror (below). A filter naming an event, argument or value the nest cannot mean is refused by name
(`bad_config`). Each log the endpoint returns is checked against the
filter that asked for it, so an endpoint that ignores a topic costs bandwidth, not correctness; a
range it will not answer whole is halved, and a throttle is asked again after a rest.

A filter's `privacy` is `private` (the default) or `public`. A private filter's values came from the
user, and only ever narrow a nest read from the RPC: source `mirror` or `mirror+rpc` with a private
filter is refused by name before anything is asked of the mirror.

**`source`** (RFC-0081 S1) is where the nest's history comes from: `rpc`, `mirror`, or
`mirror+rpc`. Absent, it is `mirror` when `mirror` is given and `rpc` otherwise, as before. With
`mirror+rpc`, `mirror` and `filters` are both required and every filter must be public. The module
hydrates, from the dataset the files name, the tables the filters' events decode into, each segment
whole and checked against its content address, then asks the RPC for the filters' logs from where the
dataset ends to `until`. Which tables and segments are fetched follows from the filters' events alone,
so no `get` carries a filter's value or is chosen by one: the mirror learns which dataset and which
tables, never whose. Every statement over a hydrated table reads it through the filters as a
predicate, so it holds the rows an `rpc` nest under the same filters would, whichever side brought
them; a value a predicate cannot compare to a stored row (an indexed argument that is not an address
or a 32-byte hash) is refused by name. The dataset's views and entities read every row and are not
bound: a statement naming one of its views is refused by name. `[[calls]]` is accepted, never run,
and its tables are not hydrated; the identity is still computed from the files as given. A postcard
pins a nest that does not follow, so it is refused with `mirror+rpc`. `/balances` and the built-in
views fold what the store holds, the hydrated history whole; read the named tables through `/sql`.

**`attach`** (RFC-0081 §3.2) is a list of `{"name", "tables"}`: other instances' answers a statement
may bind. A `POST /sql` supplies them beside `q` as
`"attach": {"<name>": {"<table>": {"arrow": "<base64>", "answer": "<x-nuthatch-answer>"}}}`: the other
instance's `/sql` answer with `format=arrow`, and its envelope, as text or as an object. The statement
names each as `<name>.<table>`; it is bound with the types the answer carried, for that statement only,
and the module keeps nothing between statements and follows nothing. A table the statement names and
the request does not supply, a bare name of an attached table, an answer that was truncated, and an
answer whose `provenance.as_of` is not this nest's are each refused by name. An attached table may
not share a name with the nest's tables, views or entities, or with another attachment's.

**`window`** is the blocks per `eth_getLogs`, in place of the chain's registered window. A narrowed
nest wants it as wide as its endpoint allows.

**`until`** is the last block the cursor indexes. The nest then answers as of that block, which is
how a host compares its answers with another source's at the same block. A store already past
`until` faults the follow rather than answer from later blocks.

A follow pass ends at the head it saw when it began; the host steps again to follow further. On a
chain that makes several blocks a second a pass would otherwise never be idle.

**Reopening.** Storage `host` without `fresh` opens the store the host kept, and the nest carries on
from its last block, as a node restarted on its data directory does. Before it answers anything it
rebuilds what is derived and not stored, from the sealed segments and the hot rows, with no RPC:
`balances`, `exposure` and `velocity`, and every entity in `files`. That costs a pass over the nest's
history at every open. A store whose rebuild fails does not open (`open_failed`, saying why), so a
reopened nest never answers from part of its history. One that sealed before its segments were kept
in storage (an engine older than RFC-0077 S3) is refused the same way, and is opened `fresh`.

### Stepping

```
nh_open(config)
loop:
  s = nh_step()
  idle      -> at the tip; step again whenever the host wants to follow it
               (`unverified`, with `verified`: why sealing stopped)
  requests  -> perform each; nh_resume([answer, ...]) in the same order; step again
  wait_ms   -> wait that long; step again
nh_request(method, target, body)   any time after the store is open
```

- Every request carries an `id` and a `kind`. Answers go back **positionally**, one per request, all
  in one `nh_resume`, before the next `nh_step`. The host may perform them concurrently.
- `nh_step` returns `requests` only when it has new ones. A host that steps again without resuming
  gets `unanswered`.
- `wait_ms` is the engine resting: the RPC client honouring `Retry-After`, a backoff. The host must
  step again once it has passed, and after every `nh_resume`; the engine makes no progress otherwise.
  It wakes its own futures when an answer arrives (RFC-0077 §12's executor trap), so a host never
  polls anything but `nh_step`.
- `nh_request` never yields: everything a query reads is in storage. It routes `/ready`, `/sql`,
  `/explain`, `/entity/{id}`, `/entities`, `/balances`, `/balance/{address}` and `/derived/*` as the
  node does, from the same `nuthatch_core::answer` functions; any other path is a 404 with no body.
  `content_type` is empty where the node sends none. `/sql` with `format=arrow` (in the query or a
  `POST` body) answers an Arrow IPC stream, as the node does (RFC-0079): its bytes come back as
  `body_base64` in place of `body`, and its envelope in `headers["x-nuthatch-answer"]`, the node's
  header. A host that wants HTTP maps it one to one, as
  `hosts/browser/sw.js` does; a host that wants a function calls it with a target.

### Requests and answers

| `kind` | Request | Answer | Failure |
|---|---|---|---|
| `rpc` | `{"endpoint", "body", "deadline_ms"}` | `{"status", "body", "status_text"?, "retry_after"?}` | `{"error", "timed_out"?}` |
| `open` | `{"name"}` | `{"handle": n}` | `{"error", "locked": bool}` |
| `remove` | `{"name"}` | `{}` (also when there was nothing to remove) | `{"error"}` |
| `list` | `{"prefix"}` | `{"names": [...]}`, every file under the nest whose name starts with `prefix` | `{"error"}` |
| `get` | `{"url", "max_bytes", "append"?}` | `{"status", "body"}`; with `append`, `{"status", "offset", "length"}` | `{"error"}` |
| `verified_finalized` | `{}` | `{"number", "hash"}`, the finalized block the host's light client verified | `{"error"}`: none now |

An `rpc` request is one JSON-RPC POST. The host returns every reply whole, whatever its status and
whether or not it is JSON: the RPC client classifies a provider's refusals by their words. `error`
means no reply came; `timed_out` says it was the deadline. Retries across a transport failure are the
host's; retries the RPC client decides on, a cap or a range split, arrive as new requests. A host
answers a `kind` it does not know with `{"error"}`. A new kind is only ever asked for behind an
`nh_open` option the host has to set.

A `get` is asked for only behind `mirror`. It is one GET of `url`, a file under the mirror's base.
Any status but 200 is answered as `{"status"}` alone, and the guest reads a 404 as an absence. A body
past `max_bytes` is an `error`, never cut short. Without `append` the body comes back as text; with
it, the host writes the body at the end of the open file `append` names (the pack, below) and answers
where it went, so a segment never crosses the interface as JSON. A host may perform several at once:
each lands whole, and the guest checks every one against its content address.

**RPC bodies are JSON strings, in both directions.** `body` is the request's JSON text in a string,
and so is the reply's. Passing bytes would mean base64, a third larger, or a second buffer and a
framing per answer, which every host would have to implement. A string is what every host's HTTP
client already returns (`Response.text()`, `ureq`'s `read_to_string`, Python's `.text`), and the
reply must pass whole even when it is a provider's HTML error page. Measured on RFC-0075 S3's
recording, the escaping and the answer wrappers cost 12.7%: 1,901,530 bytes resumed for 1,687,032
bytes of bodies over 1,433 RPC posts, and a 3.9 s follow is not spent there. JSON-RPC is UTF-8, so a
host decodes a body as UTF-8, lossily.

### Storage

Chosen at `nh_open`:

- **`bytes`.** The store lives in linear memory. `nh_export` returns it whole, consistent between
  write transactions; every save copies all of it. Nothing touches the host's files.
- **`host`.** The store is the host's file. The guest asks for it by name through `nh_step`
  (`open`, and `list` and `remove` for `fresh`), and reads and writes it through the synchronous
  `store_*` imports. Opening, listing and removing are requests, not imports, because a browser can
  do them only asynchronously: `getFileHandle`, `createSyncAccessHandle`, `removeEntry` and
  `entries` all return promises. Only an open sync access handle is synchronous.

Names are relative, `/`-separated, with no empty, `.` or `..` part; a host refuses any other. The
host keeps one directory per nest, as the node keeps `data/<nid>/`: the browser host uses an OPFS
directory named for the `nid`, the Node and wasmtime hosts a directory they are given. The guest
opens `nuthatch.redb`, and with `mirror` also `mirror.pack`. Sealed segments and their catalogue live inside the store, in tables beside
the hot rows, not as `segments/<hash>.parquet` files as on a node: a seal happens inside a window's
commit, which is synchronous, and a browser can open a new file only asynchronously, so a file per
segment could only be written after the seal had pruned the rows it holds. In the store they commit
before the prune, as the node's `manifest.json` does. The segment bytes are the node's, hash for
hash. In storage `bytes` they are part of what `nh_export` returns. History hydrated from a mirror is the
exception: those segments are appended to `mirror.pack` before the cursor runs, and the store records
each one's offset and length, so a query reads them by range. The pack only grows; a seed tail a later
seal folds in leaves its bytes behind.

The single-writer lock is the host's. In a browser a second `createSyncAccessHandle` on the same
file throws `NoModificationAllowedError`, which the browser host answers as `"locked": true`, and
the guest faults `locked`. A host with no lock of its own must not open one nest twice.

### What a host needs

- **Compile once, then cache.** The event module is about 33 MB. wasmtime compiles it in 2 s on
  18 cores, 18 s on CI's 4 and 42 s on one, and the compile peaks at 1.3 GiB on Linux and 1.8 GiB on
  macOS whatever the compiler or thread count (RFC-0077 §12). A precompiled module loads in under
  10 ms and the whole run stays under 200 MiB. A host that cannot spend that peak once, or must
  compile at every start inside phone-sized memory or a 128 MB isolate, cannot run the event build.
  `tests/wasmtime-host` caches by the module's sha256 and wasmtime's compatibility hash; a browser
  caches compiled modules itself.
- **Memory.** Linear memory never shrinks: a statement's high-water mark stays until the instance
  is dropped. The module holds itself to `memory_mib`. A reopened or mirrored nest's views are
  rebuilt from one pre-summed row per key that Burrmill folds from the sealed segments, as a node's
  are, not by replaying every sealed transfer: GRT's 2.96 million mainnet transfers open in 233 MiB
  that way, and in 1,519 MiB by replay (RFC-0077 §16).
- **Durability** in storage `host` is `store_sync`'s. Over files, every commit syncs: on the M5 Pro
  a follow of S3's recording takes 3.5 s over files and 0.7 s in `bytes`.
- **Trust.** The module reaches nothing. Every request is one the host performs, to an endpoint the
  host chose. Rows above finality are what the RPC said. Without `verified`, finality is the RPC's
  word too; with it, what is sealed hashes down to a block the host's light client verified.

### Versioning

`abi` is bumped by any change to an export's or import's shape: a name added, removed or retyped.
`tests/wasmtime-host/src/abi.rs` holds each version's shapes, and `wasmtime-host abi <module.wasm>`
fails a module that differs from the version it reports; a published version's table is frozen by
its hash. Within a version, the JSON may gain an optional `nh_open` key, an optional answer field or
a request kind behind an option, and nothing a host of that version would read differently.

## The host package

`nuthatch-engine.js` (RFC-0081 S2) is the host a page or a Node program uses instead of writing its
own: one ES module, released beside `nuthatch-events.wasm` with its own sha256 and versioned with it.
`hosts/package/build.mjs` makes it from `hosts/package/engine.js` and the reference hosts below, whose
code it carries unchanged. It refuses a module that does not speak `abi: 1`.

```js
import { Engine } from "./nuthatch-engine.js";
const nest = await Engine.open({ module: "nuthatch-events.wasm", nest: files, rpc: url, storage: "opfs", until });
await nest.follow();                       // steps until idle, answering every request
const env = await nest.sql("SELECT ...");  // /sql's envelope; `{attach}` binds other nests' answers
const answer = await nest.arrow("SELECT ..."); // an Arrow answer, as another nest's `attach` takes it
nest.close();
```

| Option | |
|---|---|
| `module` | the `.wasm` (a URL, a path in Node, bytes, or a compiled `WebAssembly.Module`); a `.gz` name is inflated |
| `nest` | the nest's files by path; in Node, also a directory |
| `rpc` | a URL, or `{name: url}`: the module sees only the names |
| `storage` | `"opfs"` (a page's default; memory where OPFS is missing), `"memory"` (Node's default), or in Node a directory, each nest kept under `<dir>/<nid>` |
| `nid` | the store's name; by default one per module, files and what the store holds (filters, source, mirror), so a second open reuses the first and a store is never reopened under other files or filters |
| `fresh` | open the store empty |
| `headerBatch` | the longest JSON-RPC batch an endpoint takes, a number or `{name: n}`; `arb1.arbitrum.io` is 10 unless said |
| `verified` | the page's light client, `() => ({number, hash})`; sets `verified` |
| `log`, `onProgress` | `(level, line)`, and `({block, tip, requests})` at most twice a second while following |
| `worker`, `workerUrl` | in a page, nests run in the package's own worker (one per page, ended when its last nest closes), started from the package's URL; `worker: false` runs them in the calling context |

Every other key is `nh_open`'s, passed as given: `filters`, `source`, `mirror`, `attach`, `until`,
`window`, `follow`, `memory_mib`, `poll_interval_secs`, `contract`, `start`.

What it owns: the worker, the store, retries across a transport failure (a POST that got no reply is
sent again, up to three times; a deadline that passed is the module's to judge), splitting a batch
longer than its endpoint takes and pacing the parts through a throttle, and the `follow_failed`
carry (up to five times, resting 2 s doubling). A kept store the module will not reopen, a pack that
fails its check or files that name another dataset, is opened fresh once. What it does not own: RPC
URLs, keys and a light client, which are the caller's. It reaches nothing the caller did not name.

`follow()` answers `{nid, storage, block, tip, requests, ms, mirror}`; `request(method, target, body)`
is `nh_request`, for any route. `docs/examples/engine-defi-pulse.mjs` opens defi-pulse-nest from the
public mirror in 29 lines. Its gates, in CI's `wasm-plain` leg and the release's engine job:

```sh
node hosts/package/build.mjs /tmp/nuthatch-engine.js
node hosts/package/run.mjs /tmp/nuthatch-engine.js /tmp/plain.wasm s5 http://127.0.0.1:8954/ ../tests/fixtures/tab-parity
node hosts/package/run.mjs /tmp/nuthatch-engine.js /tmp/plain.wasm reuse $d/s4-nest $d/s4-mirror
```

The first is S5's 32 answers through the package, opened fresh and then from its kept store; the
second a mirrored nest closed and reopened with no GET and the same answers, then reopened over a pack
with one byte altered, which must be fetched again.

## The reference hosts and the gate

| Host | Where | Storage | Runs as |
|---|---|---|---|
| browser | `hosts/browser/`: `host.js` the interface, `opfs.js`, `nest-worker.js`, `page.js`, `sw.js` | OPFS | `run-sw.mjs` in headless Chrome |
| Node | `hosts/node/run.mjs`: the same `host.js`, over `fs-storage.mjs` | files | `node` |
| wasmtime | `tests/wasmtime-host/` | files | `cargo build --release`, then `wasmtime-host` |

The gate: the same `.wasm` file, unmodified, answers RFC-0075 S5's 32 requests in all three hosts
equal to the node's pin, `tests/fixtures/tab-parity/s5-expected.json`. Each host prints the module's
sha256 and one digest over its answers, masked as `tests/e2e_tab_answers.rs` masks them, beside the
same digest over the pin; all of them must agree. CI's `wasm-plain` leg runs it on every PR that
touches the engine. Locally:

```sh
plain/build.sh /tmp/plain.wasm
fixture-rpc recorded --fork --port 8954 --file ../tests/fixtures/tab-parity/chain.json.gz &
node hosts/node/run.mjs /tmp/plain.wasm http://127.0.0.1:8954/ ../tests/fixtures/tab-parity s5
tests/wasmtime-host/target/release/wasmtime-host run /tmp/plain.wasm http://127.0.0.1:8954/ ../tests/fixtures/tab-parity s5
tests/wasmtime-host/target/release/wasmtime-host abi /tmp/plain.wasm
node run-sw.mjs <chrome> /tmp/plain.wasm http://127.0.0.1:8954/ ../tests/fixtures/tab-parity
```

`fixture-rpc` is `tools/fixture-rpc`. Without `--fork` and with `s3` in place of `s5`, the Node and
wasmtime hosts run RFC-0075 S3's gate instead: the segment hash and the 1,000 balances.

`--restart` (all three hosts, storage `host`) is RFC-0077 S3's gate: the host follows from an empty
store until a segment is sealed, is killed there with requests outstanding (SIGKILL: a child process
for Node and wasmtime, the whole browser for Chrome), then reopens the same files and finishes. The
answers must still equal the pin, and the segments those of a run that was never killed;
`hosts/agree.mjs` holds every host to both.

RFC-0077 S4's gate is a nest opened from a mirror. `run-mirror-node.mjs` seeds a copy of a nest
directory from a mirror with `nuthatch seed`, serves it, or with `--rpc` follows it to the tip, and
writes what it answers to `mirror-answers.mjs`'s requests: every authored view, each table's count and
range, each entity, the balances. `hosts/node/mirror.mjs` (Node) and `run-sw-mirror.mjs` (Chrome)
open the same directory and mirror in the module and must answer the same. CI holds the module to
the node's pins for RFC-0075 S4's entity nest, `tests/fixtures/tab-parity/s4-mirror-{seed,tip}.json`,
which `tests/e2e_tab_mirror.rs` holds the node to:

```sh
d=../tests/fixtures/tab-parity
node hosts/node/mirror.mjs /tmp/plain.wasm $d/s4-nest $d/s4-mirror --expect $d/s4-mirror-seed.json
node hosts/node/mirror.mjs /tmp/plain.wasm $d/s4-nest $d/s4-mirror --memory-mib 600 --max-rows 150000 --refused
node run-mirror-node.mjs ../target/release/nuthatch <nest-dir> <mirror-url> --out /tmp/node.json
node hosts/node/mirror.mjs /tmp/plain.wasm <nest-dir> <mirror-url> --expect /tmp/node.json
```

RFC-0077 S5's gates. `hosts/node/verified.mjs` re-runs RFC-0065 S5's cases against the module, over
RFC-0075 S3's recording with whole headers up to block 26,241,700 (`headers.json.gz`, which
`record-headers.sh` took from mainnet): the honest verified block seals the node's segment at or below
it, and an absent verified block, one the chain does not reach, a forged or missing header and a log
off the verified chain each stop sealing by name. `hosts/node/shapes.mjs` follows every nest shape the
binary's tests run, which `tests/e2e_embed_shapes.rs` writes out of their code under
`tests/fixtures/embed-shapes/` (the scaffold fixtures, a factory nest, labels and flags, the reorg
suites), era by era, and must answer as that test pins the node. CI holds the event build to 5.5 MB
brotli:

```sh
d=../tests/fixtures/tab-parity rpc=../tools/fixture-rpc/target/release/fixture-rpc
$rpc recorded --port 8955 --file $d/chain.json.gz --headers $d/headers.json.gz &
node hosts/node/verified.mjs /tmp/plain.wasm http://127.0.0.1:8955/ $d
node hosts/node/shapes.mjs /tmp/plain.wasm $rpc ../tests/fixtures/embed-shapes
node size.mjs /tmp/plain.wasm --max-brotli 5500000
```

RFC-0081 S4's gate. `hosts/node/complete.mjs` opens S4's nest narrowed to every Transfer with
`verified` and `complete`, against three fixture-rpcs serving the recording with whole headers and the
receipts `record-receipts.sh` took from mainnet: honest, one leaving block 26,239,830's log out of every
`eth_getLogs` answer, and one also forging that block's receipts. The log must be found from the
receipts with every row the same, the forged list must stop sealing by name, and a range past
`max_headers` must be refused with the figure. CI runs it to block 26,239,840 on the nine blocks of
receipts that range needs (`receipts.json.gz`). Without `--until` it follows to the tip and must seal the
node's segment, which needs every bloom-admitted block's receipts, 968 blocks and 56 MB gzipped, recorded
with `record-receipts.sh` and kept out of the repository:

```sh
r4="--file $d/chain.json.gz --headers $d/headers.json.gz --receipts $d/receipts.json.gz"
$rpc recorded --port 8956 $r4 &
$rpc recorded --port 8957 $r4 --drop-log 26239830:315 &
$rpc recorded --port 8958 $r4 --drop-log 26239830:315 --forge-receipts 26239830 &
node hosts/node/complete.mjs /tmp/plain.wasm $d http://127.0.0.1:8956/ http://127.0.0.1:8957/ \
  http://127.0.0.1:8958/ 26239830 --until 26239840
```
