# RFC-0074: nuthatch embedded in rotki - address history as a WebAssembly module

- Status: **S0 passed its gate, 2026-10-28.** S1, a draft rotki branch, is built and waits for Chief's
  review before any PR is opened (§9).
- Author: Pete (cargopete)
- Date: 2026-10-28
- Depends on: RFC-0063 (address history, the code this compiles), RFC-0065 (nuthatch's core as a
  wasm guest).
- Blocks: nothing.
- Nature: a separate artefact, not a change to the binary. `embed/` is its own crate, excluded from the
  published package as `decode/` and `seal/` are; the binary's sources are compiled into it unchanged.

## 1. Where this comes from

Chris at GraphOps pointed out on 2026-10-27 that nuthatch is pure Rust and could ship inside rotki "as
a wasm blob": rotki keeps its RPC dependency and gains an indexer without a second process. Chief asked
for it on 2026-10-28. rotki today reaches a nest over HTTP (the adapter in rotki/rotki#13342), which
asks the user to install and run `nuthatch dev` beside rotki. Embedded, the same answers come from a
module rotki loads itself.

The question S0 answers is whether the address history that passed RFC-0063's slice-3 gate natively
still passes it as a guest that opens nothing: no file, no socket, no clock of its own.

## 2. The host boundary

The module is built for `wasm32-unknown-unknown`. It imports two functions and nothing else, not even
WASI:

| Import (`nuthatch`) | What the host does |
|---|---|
| `rpc_batch(ptr, len) -> len` | reads a JSON array of `{endpoint, method, params}`, sends each as one JSON-RPC POST (concurrently if it likes), and returns the length of a JSON array of `{body}` or `{error}`, one per request, in order |
| `rpc_take(ptr)` | copies that answer into guest memory |

`endpoint` is `main` (logs, transactions, receipts, nonces, headers) or `trace` (`trace_filter`,
`trace_block`, `trace_transaction`), the binary's `--rpc` and `--trace-rpc`. The host returns the HTTP
body whole, JSON-RPC errors included, because discovery narrows a ranged call on the provider's own
words (`limited to 100 blocks`). Transport retries are the host's.

| Export | |
|---|---|
| `nh_open(config, state)` | opens the store from bytes an earlier `nh_export` gave, or empty |
| `nh_sync({addresses?, from, to?, max_windows})` | the cursor's pass: finalized head, reorg walk, then windows up to `to` |
| `nh_query(params)` | one Etherscan-shaped request, answered as `/api` answers it, covering on demand |
| `nh_watch({add, remove})` | the watched set |
| `nh_export()` | the whole store, for the host to keep |

Each returns the length of its output, negative for an error message, and `nh_output(ptr)` copies it
out. `nh_alloc` and `nh_free` give the host guest memory to write inputs into.

**Calls are batched the way native discovery batches them.** Discovery is async and keeps eight spans
in flight per provider. In the guest each RPC future queues its request and returns pending; when no
future can advance, a small executor hands the whole queue to `rpc_batch` and wakes each caller with
its answer. The binary's concurrency therefore reaches the network unchanged, with no thread or runtime
in the guest: the S0 run made 8,185 calls in 748 rounds.

**The store is redb over a byte buffer.** redb takes a `StorageBackend`; the module's is a `Vec<u8>`,
so `nh_export` is that vector and `nh_open` puts one back. The host keeps it where it keeps its own
data. rotki's draft keeps it zlib-compressed in its encrypted user database, which matters because the
store is a list of the user's addresses and their history.

**No clock.** Nothing on the S0 path reads one, which is as well: `std::time::Instant::now()` panics on
this target. The one reader in the included code, `DiffSnapshot::until`, is not reached. When MEV
re-asks are added (§7), the host passes `now` as an argument rather than the guest asking.

**Why not WASI.** `wasm32-wasip1` was the first choice, for a clock and stderr. redb 2.6.3 does not
build there on stable: `lib.rs` has `#![cfg_attr(target_os = "wasi", feature(wasi_ext))]`. On
`wasm32-unknown-unknown` redb compiles its fallback file backend, which the module never uses.

## 3. What the module is made of

- **Compiled unchanged, by `#[path]`:** `src/address_history.rs`, `src/address_discovery.rs`,
  `src/address_residual.rs`. These are the binary's own files, not copies; the store, coverage,
  discovery, hydration, internal-transaction rules and `respond` are the ones the slice-3 gate passed.
- **Stand-ins for what they reach in the rest of the binary** (`embed/src/lib.rs`, about 140 lines): a
  `Store` that is only a redb handle, an uninhabited `RpcClient`, a constant, `parse_duration`,
  `QueryBudgetExceeded`, the `VerifiedBlock` type and `block_from_rpc_value`.
- **Ported from `src/address_mode.rs`**, because that file is the HTTP server and cannot be included:
  the window driver (finalized head held twice, main and trace on one chain before and after a fetch,
  windows above finality read against an anchor, `check_anchor`, `check_tracked`'s reorg walk) in
  `embed/src/driver.rs`, and the `getblocknobytime` RPC search with its neighbour and ancestry proofs
  in `embed/src/blocktime.rs`. About 400 lines that now exist twice; §8's S2 removes the second copy.
- **Not used:** the native cursor's `Pace`, which sizes windows to an 8 s request deadline. The guest
  takes fixed 50,000-block windows, the binary's own ceiling, and bounds on-demand cover by a number of
  windows (`cover_windows`, default 4) instead of seconds.

Nothing in `src/` changed. `Cargo.toml` gains `/embed` in `exclude`.

## 4. Size and build

`embed/build-wasm.sh` builds it: 1,359,423 bytes, 389,753 gzipped. The build remaps every local path
out of the module, and two builds from different checkout paths gave the same bytes (sha256
`8f4b97d4…9624` at `30719d92`). wasmtime precompiles it to a 3.3 MB `.cwasm` for the host's CPU.

## 5. The gate

vitalik.eth over Ethereum blocks 18,000,000 to 20,000,000, the range RFC-0063's slice 3 used. Main RPC
GraphOps, traces Alchemy, as for the native nest. Etherscan's answers re-pinned the same day with the
free key, split at its 1,000-row cap, and compared as hash sets, since the split pinning overlaps rows.

| | Embedded module | Etherscan |
|---|---|---|
| `txlist` | 2,318 hashes, every field rotki reads equal on all 2,318 | 2,318 |
| `txlistinternal` | 15 rows, equal on every field rotki reads and the failure fields | 15 |
| `tokentx` | 3,334 hashes | 3,334 |
| `tokennfttx` | 106 hashes | 106 |
| `token1155tx` | 53 hashes | 53 |
| `getblocknobytime` | 20 of 20 pinned answers (`tests/fixtures/rfc0063/blocktimes.jsonl`) | |

No hash missing, none extra. The counts are slice 3's, so the native nest's answers for the range are
the same sets; the native run below compared its own answers with the same files and passed the same way.

Also checked: a range past what is covered answers `NUTHATCH_INCOMPLETE`, never `[]`; the exported
8.5 MB store, opened in a second instance, answered every list with no RPC call; and above the
finalized head, a 60,000-block range ending five blocks under the chain head was covered on demand by
one query in 36 s and equalled Etherscan's (10 of 10 transactions, 41 of 41 token transfers).

**Measured against the native nest** on the same range and RPCs, one after the other on the Mac:

| | Embedded, Python host | Native 5.13.0, `request_deadline = "120s"` |
|---|---|---|
| Backfill wall time | 775 s, 41 windows | 646 s, 48 windows |
| Main RPC calls | 8,001 | 8,504 |
| Trace RPC calls | 184 | 375 |
| Peak memory | 177 MB, the whole Python process | 73 MB, the nest, sampled every 2 s |
| Store | 8.5 MB, 1.46 MB compressed | 8.5 MB |

The module is 20% slower and makes fewer calls, not more: its fixed windows skip the per-window checks
the native pace repeats while its windows are still small, and it samples empty trace windows less
often for the same reason (5 `trace_block` calls against 40). The time goes to serial windows behind
one Python host, not to wasm: reading every answer back took 0.10 s in both.

The native nest was run with a 120 s request deadline because at its default of 8 s its pace keeps
windows to a few thousand blocks: the first attempt covered 200,000 blocks in 20 minutes and would have
taken about three hours here, past the gate script's two-hour limit. The longer deadline lets it reach
the same 50,000-block windows the module uses. That is a finding about the binary, not the module:
the pace protects requests waiting on the cursor, and a fresh backfill with nothing waiting does not
need it.

Of the 177 MB, 35 MB is Python with httpx and wasmtime imported, and about 95 MB is Cranelift compiling
the module. Loaded precompiled instead, the process holds 45 MB before opening the store, 86 MB with
the 8.5 MB store open and 95 MB after answering every list; the guest's own memory is 27 MB of that.

## 6. What failed or is missing

- **wasip1** does not build (§2), so the module has no stderr: errors come back through the exports.
- **Withdrawals, produced blocks, MEV and the partitions mirror** are not in the module. They answer
  `NUTHATCH_INCOMPLETE` or `NUTHATCH_UNSUPPORTED`, which rotki falls through on. The mirror needs an
  HTTP fetch the boundary does not have; it would be a third import, or partitions handed in by the
  host.
- **The proxy passthroughs, balances and `logs/getLogs`** are not served; rotki's own RPC answers those.
- **Chains other than Ethereum** compile in (the balance residual is in `address_residual.rs`) but were
  not run.
- **No CI builds the module.** Because it compiles `src/address_*.rs` by path, a change there that does
  not build for wasm32 breaks it silently.
- **Copies.** `nh_open` copies the state twice and `nh_export` copies the whole store, so a host that
  saves after every change moves megabytes each time. rotki's draft saves only after a call that reached
  the RPC.
- **wasmtime in rotki** is an 8 MB wheel and a 25 MB shared library on disk.

## 7. Against the non-negotiables

- **Single static binary.** Untouched; the binary builds and ships as before.
- **Footprint.** The guest is inside the per-cursor budget with room: 27 MB of wasm memory for this
  wallet and range.
- **No phone-home.** The module can reach nothing. Every call it makes is one the host chose to send, to
  the RPC the host chose.
- **Determinism.** The same source, and the gate's parity, which is the same parity slice 3 met.
- **Licence.** The module is `MIT OR Apache-2.0`, shipped inside AGPL rotki, which is a permitted use.
  The S0 harness is Python and lives outside the nuthatch tree, in the session scratchpad.
- **RFC-0044 §8.** S0 changes nothing the binary does, so it needs no record. Offering the module to
  rotki is a decision about where nuthatch runs, and it is Chief's.

## 8. Slices

- **S0, the spike. Passed.** §5.
- **S1, the rotki side.** On rotki branch `pete/nuthatch-embedded`, on top of #13342: a `nuthatch_embedded`
  setting, a requests transport adapter that answers the Nuthatch indexer's `/api`, `/ready` and
  `/api/watch` calls from the module, the user's own Ethereum node as its RPC (rotki's public nodes serve
  no traces), and the store kept in the encrypted user database. Tests replay a recorded node against
  Etherscan's hashes. Gate: rotki's tests and linters pass; not opened as a PR until Chief has read it.
- **S2, one copy of the driver.** Move the window driver and the `getblocknobytime` search out of
  `address_mode.rs` into a module the binary and `embed/` both include, and add the wasm32 build of
  `embed/` to CI. Gate: the binary's address-history tests unchanged and green, the module's gate
  re-run.
- **S3, shipping.** rotki builds colibri from source in `package.py`; the module could be built there
  from a pinned nuthatch revision the same way, or published as a wheel whose hash `uv.lock` pins. The
  S1 draft vendors the module with its sha256 for now. rotki's maintainers choose.
- **S4, withdrawals and produced blocks**, through partitions the host fetches and hands in.

## 9. Open questions

1. Whether rotki wants the module at all, or would rather link a native library into colibri, which is
   already Rust in their tree. The wasm boundary keeps nuthatch's code from touching the disk or the
   network on its own, which suits a privacy-first app; a native crate would be smaller and faster.
2. Which RPC. The draft uses the user's owned Ethereum node and refuses without one, so rotki falls
   back to its other indexers. A trace-capable node is the one credential address history needs.
