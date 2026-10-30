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
| `log` | `(level, ptr, len)` | one UTF-8 line; level 0 is a panic, 1 a host-storage failure |
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
  "contract": "0x...",
  "start": 26239000,
  "rpc": ["main"],
  "storage": "host",
  "fresh": true,
  "memory_mib": 2048,
  "poll_interval_secs": 12,
  "files": {"entities.toml": "...", "entities/received.sql": "...", "views/20-s4.sql": "..."}
}
```

`contract`, `start` and `rpc` are required. `rpc` names endpoints; the host maps each name to a URL,
so no URL or key enters the module. `storage` is `bytes` (the default) or `host`. `fresh` (storage
`host` only) removes the nest's files before opening. `memory_mib` is the ceiling RFC-0075 §17 holds
the module to, 2,048 unless set; a phone's is 600. `poll_interval_secs` is what `/ready` reports as
the node's `--poll-interval`. `files` are a nest's authored files, `entities.toml`, `entities/*.sql`
and `views/*.sql`, by path. Unknown keys are refused, so a host never believes a setting took.

Until RFC-0077 S4 the nest is one ERC-20's `Transfer` on mainnet from `start`, as RFC-0075's tab is;
S4 adds `nuthatch.toml`. A warm reopen is S3's: until then a host opens `fresh`.

### Stepping

```
nh_open(config)
loop:
  s = nh_step()
  idle      -> at the tip; step again whenever the host wants to follow it
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
  `content_type` is empty where the node sends none. A host that wants HTTP maps it one to one, as
  `hosts/browser/sw.js` does; a host that wants a function calls it with a target.

### Requests and answers

| `kind` | Request | Answer | Failure |
|---|---|---|---|
| `rpc` | `{"endpoint", "body", "deadline_ms"}` | `{"status", "body", "status_text"?, "retry_after"?}` | `{"error", "timed_out"?}` |
| `open` | `{"name"}` | `{"handle": n}` | `{"error", "locked": bool}` |
| `remove` | `{"name"}` | `{}` (also when there was nothing to remove) | `{"error"}` |
| `list` | `{"prefix"}` | `{"names": [...]}`, every file under the nest whose name starts with `prefix` | `{"error"}` |

An `rpc` request is one JSON-RPC POST. The host returns every reply whole, whatever its status and
whether or not it is JSON: the RPC client classifies a provider's refusals by their words. `error`
means no reply came; `timed_out` says it was the deadline. Retries across a transport failure are the
host's; retries the RPC client decides on, a cap or a range split, arrive as new requests. A host
answers a `kind` it does not know with `{"error"}`. A new kind is only ever asked for behind an
`nh_open` option the host has to set.

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
directory named for the `nid`, the Node and wasmtime hosts a directory they are given. Today the
guest opens one file, `nuthatch.redb`; sealed segments stay in memory until RFC-0077 S3 moves them
to `segments/<hash>.parquet` beside it.

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
  is dropped. The module holds itself to `memory_mib`.
- **Durability** in storage `host` is `store_sync`'s. Over files, every commit syncs: on the M5 Pro
  a follow of S3's recording takes 3.5 s over files and 0.7 s in `bytes`.
- **Trust.** The module reaches nothing. Every request is one the host performs, to an endpoint the
  host chose. Rows above finality are what the RPC said.

### Versioning

`abi` is bumped by any change to an export's or import's shape: a name added, removed or retyped.
`tests/wasmtime-host/src/abi.rs` holds each version's shapes, and `wasmtime-host abi <module.wasm>`
fails a module that differs from the version it reports; a published version's table is frozen by
its hash. Within a version, the JSON may gain an optional `nh_open` key, an optional answer field or
a request kind behind an option, and nothing a host of that version would read differently.

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
