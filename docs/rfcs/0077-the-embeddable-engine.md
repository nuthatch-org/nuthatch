# RFC-0077: The embeddable engine - nuthatch's core as an opt-in wasm module, beside the binary

- Status: **Accepted, 2026-10-30.** S0 to S5 and S7 (the Lodestar demo) authorised in order; S6 waits for rotki
  to ask. S0 passed 2026-10-30 (§12). S1 built in part, 2026-10-30 (§13). S2 built 2026-10-30, its
  address-build half deferred (§14). S3 built 2026-10-30, its Chrome leg CI's (§15). S4 built 2026-10-30,
  with the cold folds in the module (§16). S5 built 2026-10-30, Safari waiting on Chief and the address
  build's size gate deferred (§17).
- Author: Pete (cargopete)
- Date: 2026-10-30
- Depends on: RFC-0075 (the whole nest in a tab, S0 to S6 merged), RFC-0074 (address history as a
  wasm module inside rotki; S0 passed 2026-10-28 and parked, on branch `pete/rotki-embedded-s0`,
  issue #2474), RFC-0065 (the nest in a tab, and its light client), RFC-0068 (postcards: a published
  nest read by hash), RFC-0052 (the mirror and `seed`), RFC-0041 (authored entities).
- Blocks: nothing.
- Nature: a separate artefact and a refactor of the binary's crates that changes none of its
  behaviour. It makes a promise the project has not made before, a supported interface outside the
  binary, so acceptance wants a line in `CLAUDE.md` (§10).

## 1. Where this comes from

Chris at GraphOps, on 2026-10-27: "one of nuthatch's super interesting properties is that it's pure
rust and can(?) be embedded as a wasm blob". He asked it about rotki: "that keeps rotki's rpc
dependency but introduces embedded indexing".

Two days later the question mark has two answers, and they do not share a line of glue:

1. **RFC-0075** put the event nest in a browser: ingest, the redb hot store, reorgs, the built-in views,
   `entities.toml`, `views/*.sql`, `/sql` and the node's HTTP answers, byte-identical to `nuthatch
   dev`. It is `spike/dbsp-wasm/`, live at nuthatch-indexer.com/play/live/ since 2026-10-30. Its host
   is JavaScript, reached through wasm-bindgen: Rust awaits `fetch` and opens OPFS itself.
2. **RFC-0074 S0** put address history inside rotki's Python, through wasmtime. Its host is anything
   that can answer two imports, `rpc_batch` and `rpc_take`; the store is redb over a byte buffer the
   host keeps. vitalik.eth over blocks 18,000,000 to 20,000,000 answered every list equal to
   Etherscan. It is `embed/` on its branch, never merged.

This RFC turns the "(?)" into a supported property, and a secondary one: the binary stays what
nuthatch is and what it leads with. Beside it, opt-in and off by default, sits **one embeddable
engine**, a `wasm32-unknown-unknown` module with a small, versioned, documented host interface, for
the cases where the data belongs to one user on one device. A browser tab, a Node or Electron process,
rotki's Python through wasmtime and any other host drive the same module the same way. The tab and the
rotki embedding stop being two spikes and become two hosts.

## 1a. The principle: one indexer, many ways to run it

Chief's line, which every slice below is held to: **nuthatch is one indexer with many ways to run it.
The native binary is the main path, the delightful core, and it stays as it is. The embeddable engine
is opt-in**, a separate crate (`embed/`) built only when wanted.

What that means in practice:

- **A default build is unchanged** in behaviour, CLI, config, on-disk layout, dependencies and
  performance. Building it needs no wasm toolchain, and nothing here adds a runtime cost to it. RFC-0046
  §1's deletion test applies: delete `embed/` and every embed-only feature from the tree, and a binary
  user loses nothing.
- **The wasm32 constraint binds only what the embed shares with the binary**: no threads, no direct file
  or network access, no C dependency without a wasm build. It applies to the shared modules behind the
  seams that already exist (`Source` and `Transport`, the store's backend, the clock shim in
  `nuthatch_core::time`). It does not bind the binary's own code: tokio's multi-threaded runtime, mounts,
  the admin cockpit, followers and the rest stay native-only. CI builds the shared modules for wasm32 on
  the PRs that touch them, and on no others.
- **Where a move into core would make the native path slower or more awkward, the native path wins.**
  The binary keeps a native implementation behind the trait, as it kept its blocking-pool commit and its
  threaded view driver in RFC-0075 S1, rather than bending to fit wasm.
- **One engine, two ways to run it.** Embed it when the data belongs to one user on one device: a
  wallet, rotki, a dapp's frontend. Run the binary when many users share one view: dashboards, APIs,
  anything past the memory budget. The two give byte-identical answers, so an app can move from one to
  the other without its answers changing.
- **The binary stays the pitch.** In Chief's words: "the binary stays the main pitch, the delightful
  core, all the things nuthatch is today; the embeddable engine is off by default but is important for
  some use cases." The README, the site and `llms.txt` keep leading with the binary. The engine is
  presented as a secondary capability for named cases (personal data on one device: rotki, wallets,
  dapp frontends, `/play`), never as the headline and never as "nuthatch is now wasm" (§11).

## 2. What exists, read 2026-10-30

| | Event nest (RFC-0075) | Address history (RFC-0074 S0) |
|---|---|---|
| Where | `spike/dbsp-wasm/` on main | `embed/` on `pete/rotki-embedded-s0` |
| Module, raw | 34,491,597 bytes (the one `/play/live` serves) | 1,359,423 bytes |
| Compressed | 5,245,414 brotli (q11, measured today on the deployed file) | 389,753 gzip |
| Imports | 84, every one from wasm-bindgen's JS glue | 2: `rpc_batch`, `rpc_take` |
| RPC | Rust awaits `fetch` (`nuthatch_core::fetch::Fetch`) | host-performed, batched |
| Storage | OPFS sync handle, opened by Rust (`nuthatch_core::opfs`) | byte buffer, exported to the host |
| Clock | `performance.now` and `Date` through JS | none read |
| Exports | wasm-bindgen classes `LiveNest`, `PlayNest` | `nh_open`, `nh_sync`, `nh_query`, `nh_watch`, `nh_export` |
| Binary's code, included by `#[path]` | `src/entity_bind.rs`, `entity_decl.rs`, `entity_lower.rs`, `sql_errors.rs` | `src/address_history.rs`, `address_discovery.rs`, `address_residual.rs` |
| Binary's code, ported | `analytics::define_views_bound`, `engine_burrmill`'s `bind_relation`, `read_sealed`'s probe (`query.rs`, `http.rs`) | the window driver and the `getblocknobytime` search from `address_mode.rs`, about 400 lines (`driver.rs`, `blocktime.rs`) |
| Built in CI | yes, `wasm-core` | no |

The 84 imports, by what they reach: `fetch`'s `Response` (`status`, `headers`, `arrayBuffer`, `then`),
OPFS (`getDirectory`, `getFileHandle`, `createSyncAccessHandle`, `read`, `write`, `truncate`, `flush`,
`getSize`, `close`), randomness (`crypto.getRandomValues`, and Node's `require` and `randomFillSync` for
getrandom's Node path), the clock (`performance.now`, `Date.getTime`), `queueMicrotask`, and the
exported classes' constructors. None of it is the engine; all of it is one host's way of answering five
questions. That is the gap this RFC closes.

## 3. The design

### 3.1 The guest asks; the host acts

The module performs no I/O. It runs until it needs something only a host can give, then returns
saying what. This is the one change to RFC-0075's shape, which had Rust await `fetch` (§5 there). The
reason is the hosts: a browser cannot answer a synchronous import with a network round trip without
`Atomics.wait` and cross-origin isolation, which RFC-0075 §4 rejected, and Python, Node and wasmtime
are happiest calling in and getting an answer. A guest that yields serves both.

RFC-0074 already holds half of this. Its futures queue their requests and return pending; when none
can advance, a small executor hands the whole queue to the host. The difference is that the hand-off
becomes a return from `nh_step` instead of a call to a blocking import, so the same module works for a
host that must await.

### 3.2 The interface, version 1

Every value crosses as JSON at a pointer and a length, as RFC-0074 does. Each export returns the length
of its output, negative for an error message; `nh_output(ptr)` copies it out; `nh_alloc` and `nh_free`
give the host guest memory.

| Export | |
|---|---|
| `nh_abi()` | the interface version and what this build can do: `{"abi": 1, "capabilities": ["events", "address"]}` |
| `nh_open(config)` | a nest's files (`nuthatch.toml`, `abis/`, `entities.toml`, `views/*.sql`) or an address-history config; the storage mode; the memory ceiling |
| `nh_step()` | advance until blocked. Returns `{"idle": true}`, `{"requests": [...]}`, `{"wait_ms": n}` or a named fault |
| `nh_resume(answers)` | answers to the last `requests`, one per request, in order |
| `nh_request(method, target, body)` | one HTTP request as the node's router would answer it: status, content type, body bytes |
| `nh_export()` | the whole store, in byte-buffer mode |

A request is `{"kind": "rpc", "endpoint": "main", "body": ..., "deadline_ms": n}` (or `"trace"`, the
binary's trace RPC) or `{"kind": "get", "url": ..., "max_bytes": n}`, the second for a mirror (§4,
S4). `nh_request` never yields: everything a query reads is already in storage. The host returns each body
whole, JSON-RPC errors included, because the RPC client classifies a provider's refusals by their
words (`classify_rpc_error`, `looks_like_cap`, RFC-0075 §5). Retries across a transport failure are the
host's; retries the RPC client decides on (a cap, a range split) are the guest's and arrive as new
requests.

| Import (`nuthatch`) | |
|---|---|
| `now_ms() -> f64` | wall clock, for `/ready`, entity state sampling and the RPC client's rests |
| `random_fill(ptr, len)` | randomness for the engine's hash maps; nothing stored depends on it |
| `store_open`, `store_read`, `store_write`, `store_len`, `store_set_len`, `store_sync`, `store_remove`, `store_list` | storage mode `host` only (§3.3) |
| `log(level, ptr, len)` | optional; a host that does not provide it gets no log |

All imports are synchronous. Storage is the reason that works: redb's `StorageBackend` is five
synchronous calls, and every host has a synchronous file: `FileSystemSyncAccessHandle` in a browser
worker, `fs.*Sync` in Node, a file in Python or Rust.

**Queries out are the node's routes.** `nh_request` routes `/sql`, `/explain`, `/entity/{id}`,
`/entities`, `/balances`, `/balance/{address}`, `/derived/*` and `/ready` through
`nuthatch_core::answer`, as RFC-0075 S5 does now, and the address build adds `/api`, `/api/watch` and
`/ready` as RFC-0063 serves them. A host that wants HTTP maps it one to one: the tab's Service Worker
does already, and RFC-0074 S1's rotki adapter answered `/api`, `/ready` and `/api/watch` from the
module. A host that wants a function calls `nh_request` with a target. There is no second query API to
keep equal to the first.

### 3.3 Storage

Two modes, chosen at `nh_open`:

- **`bytes`.** The module holds the store in linear memory and `nh_export` gives it to the host, as
  RFC-0074 does and as rotki's draft kept it, compressed in its encrypted user database. Simple, and
  every save copies the whole store.
- **`host`.** The module asks the host for named files through the storage imports: `nuthatch.redb`,
  `segments/<hash>.parquet` and `manifest.json`, the node's own layout under `data/<nid>/`. The browser
  host implements them over OPFS, which moves `nuthatch_core::opfs` out of the module and into the
  host's JS, a few dozen lines. The single-writer lock stays the browser's: a second
  `createSyncAccessHandle` throws `NoModificationAllowedError` (RFC-0075 S2), and the host reports it
  as `locked`.

Segments go through storage too. Today they do not: the tab's `Cellar` (`spike/dbsp-wasm/src/cellar.rs`)
holds every sealed segment in a `HashMap` in linear memory, which is one reason a reload loses its
history (§5) and why a mirrored nest could not fit (§6).

### 3.4 One crate, two builds

**Decision: feature-selected builds of one crate, not one module with everything.** The crate is
`nuthatch-embed` with features `events` and `address`, and the published artefacts are
`nuthatch-events.wasm` and `nuthatch-address.wasm`, each answering `nh_abi` with its capabilities and
refusing a route it was not built with by name.

The argument is size, and where it comes from. RFC-0075 §17 attributed the event build: DataFusion,
arrow, sqlparser and parquet are about half the code directly and most of the generic half besides;
dbsp is 1.3 MB of 36.3 MB; nuthatch's own crates 0.3 MB. Address history needs none of the engine. The
linker drops what nothing reaches, and a router reaches every route it names, so one module with both
cannot shed the engine for an embedder who never sends SQL. rotki would download about 5.25 MB of brotli
to use code that is 390 KB gzipped today. A combined build is one `cargo` invocation away for a host
that wants both, and it is not published until one asks.

### 3.5 Not wasm-bindgen, not the component model

**Decision: a plain core-module ABI.** It runs unchanged in a browser (`WebAssembly.instantiate`), Node,
wasmtime from Rust or Python, and any other runtime, with no generator on either side. wasm-bindgen's
glue is browser JS and binds the module to it, which is the 84 imports above. The component model
(WIT) needs a component-aware host: wasmtime has one; a browser needs the component translated to JS
first; other runtimes vary. A core module can be wrapped as a component later without changing the
guest, so nothing is lost by starting plain.

**Against the 2026-10-08 history.** Chief removed the WASM transform layer that day (#1999): the
`transform`, `screen` and `effectful` handlers, `wit/`, `components/` and the wasmtime dependencies.
That was nuthatch as the **host**, running guest code in its data path. This is the opposite: nuthatch
as the **guest**, inside someone else's process. The binary gains no wasmtime, no WIT and no component;
nothing here runs foreign code inside nuthatch, and nothing reopens the transform layer. wasmtime
appears only as a dev-dependency of `embed/`'s tests (§4, S2), which the binary does not build.

## 4. Slices and their gates

Each ends runnable, and each gate can fail. Sizes are days of focused work, estimated.

- **S0, the host-neutral event build.** The unknown that could narrow this RFC: can every wasm-bindgen
  import be removed from the event build without forking business logic? getrandom has a custom
  backend; `web-time` gives way to `now_ms`; OPFS and `fetch` leave for the host. What is not known is
  whether something in DataFusion's tree reaches JS another way. Build the spike's S3 cursor with the
  §3.2 imports and a stub host, list its imports, and run S3's recorded chain under Node with no
  wasm-bindgen glue and under wasmtime. **Gate**: the module imports only `nuthatch.*`; the segment hash
  `c983470c…479aeae` and the 1,000 balances equal RFC-0075 S3's pinned `expected.json` in both hosts.
  Recorded: wasmtime's compile time and memory for a 34 MB module, and a precompiled load. RFC-0074
  found Cranelift took about 95 MB to compile a 1.36 MB module; if a 34 MB one cannot be compiled or
  precompiled within a host's reasonable budget, the event build is browser-and-Node only, and §1's
  promise narrows to say so. Binary: untouched, since S0 changes only the spike. About 2 days.
- **S1, one codebase.** The binary's modules the spikes include by path move into crates, so nothing
  compiles `src/` by `#[path]`:
  - `entity_bind`, `entity_decl`, `entity_lower` and `sql_errors` into `nuthatch-core`. `entity_bind`
    reads the binary's `entity_offchain` (snapshot files), so core takes a small trait for offchain
    tables, which the binary implements over its files and an embedding answers with none, as
    `spike/dbsp-wasm/src/offchain_none.rs` does by stand-in today.
  - The table binding the tab ports (`define_views_bound`, `bind_relation`,
    `reject_unknown_table_refs`, `expand_through_views`, `read_sealed`'s probe; `src/analytics.rs` and
    `src/engine_burrmill.rs`) into core over a segment reader, so the node binds files and an embedding
    binds storage, through one function. The route bodies are there already: RFC-0075 S5 put them in
    `nuthatch_core::answer` (729 lines).
  - `address_history`, `address_discovery`, `address_residual`, the window driver and the
    `getblocknobytime` search into a crate the binary and `embed/` both use, which is RFC-0074 S2's
    plan, removing the 400 duplicated lines.
  - `spike/dbsp-wasm/` becomes `embed/` with the two features; its `web/` files become
    `embed/hosts/browser/`.
  - The dbsp patch (`rfc-0075-dbsp-wasm32.patch`, 1,335 lines; 459 added and 89 removed in source)
    moves from `vendor.sh`'s download-and-patch to a pinned git rev of a fork, and PRs 1 and 2 of
    `spike/dbsp-wasm/UPSTREAM.md` (actix-web optional; the zip and object_store trims) are offered to
    Feldera. PRs 3 and 4 are offered once Chief agrees. The binary stays on published dbsp.

  **A premise that proved false.** There is no cargo workspace to promote the tab into. `core/`,
  `decode/`, `seal/`, `identity/` and the rest are sibling path crates with their own `Cargo.lock`, and
  the spike has its own `[workspace]` and lockfile. That is how RFC-0075 S3's gate first failed: the
  spike resolved parquet 59.3.2 and the binary 59.3.0, and the version string parquet writes into every
  file moved the segment hash. `embed/` stays a sibling crate, because a root workspace would apply the
  dbsp patch to the binary. Instead, S1 adds a check that every crate `embed/` shares with the binary
  resolves to the binary's version.

  **Gate**: no `#[path]` into `src/` anywhere and no `spike/` directory; the binary unchanged (§1a): its
  tests, benches and budgets, its CLI help, the config `init` writes, its on-disk layout, and its
  dependency tree (`cargo tree` before and after, nothing added), with native seal hashes for a pinned
  range equal before and after; every RFC-0075 gate (S0's digest, S2's reorg transcript, S3's segment hash, S4's
  entities and views, S5's 32 answers, S6's budgets) green against `embed/`'s event build; RFC-0074
  S0's parity re-run against the address build with the same Etherscan files; both builds made for
  wasm32 in CI on every PR that touches core or `embed/`. 6 to 8 days, the largest slice.
- **S2, the host interface.** §3.2 specified in `embed/README.md` with a version, and implemented. Two
  reference hosts in the tree: the browser host (JS, OPFS, the Service Worker), and a non-browser host
  written in Rust on wasmtime, as a test harness under `embed/tests`. The second is Rust because there
  is no Python in nuthatch-org's repositories (Chief, 2026-09-05), and wasmtime is what rotki's Python
  binding wraps. The browser host also runs under Node over `fs`, which is how CI exercises it without a
  browser. **Gate**: the same `.wasm` file, unmodified, gives byte-identical answers in the browser host
  (Chrome), the Node host and the wasmtime host for RFC-0075 S5's request set, and equal to the node's
  pinned `s5-expected.json`; the address build gives RFC-0074 S0's lists in the wasmtime host. A change
  to an export's shape without a version bump fails a pinned-interface test. Binary: unchanged, and
  wasmtime appears in no lockfile but `embed/`'s. About 4 days.
- **S3, survives a restart.** A reopened store comes back with three things missing, not one:
  1. **Sealed history.** The `Cellar` is in memory (§3.3), so the segments are gone while the store
     says `sealed_through` and has pruned those rows.
  2. **The built-in views.** `TabNest::open_sealing` (`spike/dbsp-wasm/src/tab.rs`) starts
     `balances`, `exposure` and `velocity` empty whatever the store holds, so `/balances` after a reload
     covers only what was indexed since.
  3. **Entities.** `derived.rs` marks them unavailable on a warm store, with the reason text the node's
     `EntityView::start_bound` gives.

  `/play/live` opens a fresh store on every visit because of the third (`live.js` in burrmill-wasm:
  "Each load opens a fresh store, so the entities start cold and are available"). The node does not
  leave them unavailable:
  on a warm start it seeds them from sealed segments, one at a time, plus the hot tail
  (`seed_entities`, `src/indexer.rs:979`, #865), rebuilds the three views by a cold fold in Burrmill plus
  a hot replay (`rebuild_views`, `src/indexer.rs:8516`), and refolds factory children
  (`rebuild_children`). The reason text still says "RFC-0041 §5.3's warm-restart seed is not
  implemented", which is stale, and so is the matching comment above
  `refuse_seal_direct_with_entities`. S3 moves the three rebuilds into core over the segment reader and
  runs them in the module on open. **Gate**: follow S3's recording and S4's fork with entities, kill the
  host mid-follow, reopen from the same storage, finish; `/balances`, `/derived/*`, every view and the
  segment hashes equal the node's at the same head, in the browser host on OPFS and the wasmtime host
  on files. Binary: the rebuilds move behind the segment reader with the node's file-backed reader as
  its native implementation; `tests/e2e_warm_restart.rs`, `tests/e2e_entity_reorg.rs` and
  `tests/bench_restart_mounts.rs` unchanged and green, and restart no slower than before on the same
  bench. `/play/live` then stops sweeping its stores. About 4 days.
- **S4, any nest.** Today the tab is one ERC-20's `Transfer` on mainnet: the registry is
  `tab.rs`'s `erc20()`, the chain policy three constants in `live.rs` ("Mainnet's policy, as
  `src/chains.rs` gives the node"), no factory and empty labels. S4 takes `nuthatch.toml` and its ABIs
  through the binary's own parsing (moved to core; `src/config.rs` reaches address history, calldata,
  IPFS and webhooks, which the move separates), the chain registry from `src/chains.rs`, factories and
  labels. Then it hydrates sealed history from a published mirror as `nuthatch seed` does
  (`src/seed.rs`, RFC-0052 §8): catalogue and `_seed/` through `get` requests, every file checked
  against its content address, optionally pinned by a postcard's digest (RFC-0068 §3), segments written
  to host storage and read by range at query time, as `/play` reads them. Then it seeds the views and
  entities (S3) and follows the tip. Before any download it adds up what the nest needs (catalogue
  sizes, entity admission at 8 MiB plus 3,200 bytes per declared row as RFC-0075 §17 charges) against
  the ceiling, and refuses by name when it cannot fit. **Gate**: defi-pulse-nest from the public mirror, and
  RFC-0075 S4's entity nest published to a local mirror, opened in the browser host, answer equal to a
  node seeded from the same mirror at the same head, at the 2 GiB ceiling; a nest that cannot fit at 600 MiB is refused
  before download with its figures. For scale, RFC-0068 §5 has DeFi Pulse's published segments at 128 MB
  and Lodestar's at 512 MB; neither has been opened this way. Binary: config parsing and the chain
  registry move without changing a byte the binary accepts or writes; its config tests,
  `tests/e2e_seed_from_mirror.rs` and the CLI's help unchanged. Where a config section only the
  binary uses (mounts, webhooks, admin) would drag native-only code into core, it stays in the binary
  and core sees the nest's part alone. About 5 days.
- **S5, trust, parity, reach.**
  - *The head.* `/play`'s light client is Helios in the page (`web/play/verified.js` in burrmill-wasm,
    RFC-0065 S5): it verifies a finalized block through the beacon sync committee, from one consensus
    endpoint configured for mainnet only. The interface takes it as an optional host input,
    `verified_finalized {number, hash}`; the module then seals only at or below it and checks that every
    header from the last seal hashes up to it, as RFC-0065 S5 does in wasm. Helios stays in the browser
    host, so the module gains nothing from it and a non-browser host may bring its own. The page's
    "trusts its RPC" line can then narrow, not go: rows above finality are still what the RPC said, and
    completeness is unverified (RFC-0065 §6).
  - *Parity.* Every nest shape the binary's tests run, through the Node host: the scaffold fixtures
    (`tests/fixtures/scaffold/pair`, `two`), a factory nest (`tests/e2e_runtime_lifecycle.rs`,
    `tests/e2e_follower.rs`), labels, and the reorg suites. Most of those chains are built in code
    (`tests/common/tape.rs`), not fixture files, so the first job is a recorder that writes them out for
    the module.
  - *Safari and iPhone.* Not run once yet; `safaridriver` needs an administrator (RFC-0075 §14).
  - *Size budgets.* CI fails the event build past 5.5 MB brotli and the address build past 1.5 MB raw.
  - *MCP.* Keep deferred. `nuthatch mcp` is a stdio bridge to the HTTP API (`src/mcp.rs`). In a
    browser no MCP client outside the page can reach a Service Worker, so nobody is served; elsewhere a
    host that wants MCP can bridge `nh_request` the way the binary bridges HTTP.

  **Gate**: with a verified head, a forged header or a log off the verified chain stops sealing by name
  (RFC-0065 S5's cases, re-run against the module); every listed shape equal to the node's; the size
  gates in CI. Binary: unchanged; S5 adds nothing to it. About 5 days, Safari excepted.
- **S6, rotki on the interface. Only if Lefteris asks.** RFC-0074 is parked (#2474); rotki#13342, the
  HTTP adapter its S1 built on, was closed on scope. If rotki asks for embedded indexing, the address
  build replaces `embed/`'s S0 module and rotki's host answers `nh_step`'s requests where it answered
  `rpc_batch`. No rotki PR is opened otherwise. **Gate**: RFC-0074 §5's parity in rotki's host, and
  rotki's tests and linters. Binary: unchanged.
- **S7, the demo: Lodestar's `/profile`, verified locally.** Chief, 2026-10-30. Not started: it is built after S5, in the Lodestar repository, and nothing here implements it. Lodestar's network-wide
  pages are the shared-view case and stay on the hosted `graph-allocations-nest`. Its wallet-connected
  `/profile` is one user's data: delegations (staked, thawing, realised and unrealised rewards),
  curation signals and 90 days of rewards history, loaded today from `/api/portfolio` and the rewards
  route. A "verify it locally" switch there runs the engine in the tab against the visitor's RPC on
  Arbitrum One. It fetches the visitor's own staking and curation logs by indexed topic, plus the
  delegation pools of the indexers they are delegated to, which is what turns shares into GRT. The page
  shows the server's figures and the tab's side by side, each marked where they agree. Needs S4
  (Arbitrum, the Horizon proxies' ABIs). **Gate**: for a set of real delegator and curator addresses,
  every figure the tab computes equals `/api/portfolio`'s at the same block, and any that cannot be
  computed is named, not shown. The change is to the Lodestar repository, made with Chief's go.
  Binary: unchanged. It is the blog post's worked example.

## 5. Honest limits

- **It still needs an RPC.** The engine indexes what a host's RPC returns. Public RPCs serve about eight
  days of logs (RFC-0075 §10), so an older contract needs a full-history RPC or a mirror (S4).
- **Single-threaded.** A long statement holds up the cursor and the other way round (RFC-0075 §4).
- **Memory.** wasm32's 4 GiB address space; the budgets of RFC-0075 §17, 2 GiB and 600 MiB; linear memory
  never shrinks, so a statement's high-water mark stays until the instance is dropped.
- **"Pure Rust", with two caveats.** dbsp builds for wasm32 only with our patch, until Feldera takes it.
  zstd-sys, through `nuthatch-seal`, compiles C for the target with clang, as CI's wasm steps set up.
- **A host's storage can be evicted.** Safari evicts an unused origin after seven days unless
  persistence is granted (RFC-0075 §10). An embedded nest is re-derivable and the browser host says so.

## 6. Against the non-negotiables

1. **Single static binary.** Unchanged and primary, the main path and the main pitch (§1a). Every
   slice's gate holds a default build's behaviour, CLI, config, on-disk layout, dependencies and
   performance where they are; building it needs no wasm toolchain; it gains no wasmtime and no WIT. The
   module is opt-in, a separate crate built only when wanted. RFC-0046 §1's deletion test holds: delete
   `embed/` and the binary user loses nothing. The wasm32 constraint binds only the modules the two
   share, behind existing traits; where it would cost the native path, the binary keeps a native
   implementation behind the trait.
2. **Footprint.** The module enforces the per-cursor budget itself (RFC-0075 §17) whatever the host.
3. **No phone-home.** The module can reach nothing. Every request it makes is one the host performs, to
   the RPC, mirror or consensus endpoint the host chose. Nothing is sent about anyone.
4. **Determinism.** The same decode, circuits, sealer and routes as the node; every gate is equality with
   the node. Randomness from the host seeds hash maps only, and no answer depends on it, which the
   gates show by running three hosts.
5. **Licence.** The crate is `MIT OR Apache-2.0`, embeddable in closed or copyleft hosts alike (rotki is
   AGPL, a permitted use). dbsp and the feldera crates are MIT OR Apache-2.0; wasmtime, a test-only
   dependency, is Apache-2.0 with the LLVM exception.

## 7. Risks

1. **S0 narrows the promise.** If the event build cannot drop JS or a 34 MB module is too costly to
   compile in wasmtime, "any host" becomes "a browser, Node, and the address build anywhere". S0 says
   which, first.
2. **Size grows as more moves to core.** Config parsing, chains and the binding code are the binary's,
   and some of it pulls dependencies a tab never needed. The S5 ceilings catch it; RFC-0075 §17's
   symbol attribution says where it went.
3. **CI time.** `wasm-core` took 18 to 39 minutes as one job, and #2528 splits it into three parallel
   legs. A second build, a wasmtime host and the parity set add to that; S1 and S2 measure it against
   #2528's numbers, not today's.
4. **The dbsp patch rots.** Feldera releases about weekly (0.349.0 against our 0.320.0 on 2026-10-28).
   The fork's CI build catches a bump that breaks it; upstream PRs shrink it.
5. **An interface is a promise.** Once a host outside the tree depends on `abi: 1`, a change costs a
   version. S2 pins the shapes, and nothing is published as stable until S2's gate passes.
6. **Safari** is untested at every slice so far.

## 8. Non-goals

Threads (nightly only). Memory64 (not in Safari). The binary embedding a wasm runtime. Publishing what
an embedded nest seals (RFC-0065 §7). Anything hosted, any account. The component model in v1 (§3.5).
Folds and the `graph` feature. Several nests sharing one cursor inside one module: one instance runs one
nest, and a host runs more instances.

## 9. Open questions for Chief

1. **Server-side hosts.** The interface does not rule out a server: the module imports nothing WASI, so
   wasmtime, Node or an edge runtime can run it. This RFC builds and gates none, and does not propose
   one. The native binary is the primary deliverable (non-negotiable 1) and does that job more cheaply;
   RFC-0065 §7 found 128 MB isolates rule out the engine on Cloudflare Workers, though the address build
   would fit. Should any server-side host be supported, or stay possible and unsupported?
2. **Publishing the modules.** Where an embedder gets them: a GitHub release asset beside the binary,
   npm, or both. Each is a version line to keep.
3. **The upstream PRs.** `UPSTREAM.md`'s issue and four PRs are drafted and not posted. Post PRs 1 and 2
   now, and 3 and 4 with them or after?
4. **`/play/live`'s ceiling.** Whether the page should open mirrored nests (S4) by default or keep the
   young-contract cursor as its first screen.

## 10. RFC-0044 §8

RFC-0075 needed no record: the tab was a separate artefact and the binary unchanged. This keeps both
properties, but it also publishes an interface and calls the module supported, which is a decision about
where nuthatch runs and what it promises outside the binary. RFC-0065 §8 put the same kind of step
("a new way of running nuthatch's core") to Chief for a line in `CLAUDE.md`, and RFC-0074 §7 said the
same of offering the module to rotki. If accepted, this wants that line.

## 11. Docs, site, and after it lands

- **The README, the site and `llms.txt` keep leading with the binary**: `curl | sh`, `nuthatch init`,
  `nuthatch dev`, the two-minute path, as today. Nothing in their first screen changes.
- **The engine gets its own page**, linked from the docs and not from the hero: `embed/README.md` for
  the interface (S2), and a page on the site that opens with the use cases it is for (one user's data on
  one device: rotki, wallets, dapp frontends, `/play`) and says plainly when to run the binary instead.
- **No copy calls nuthatch "wasm"** or describes the engine as a new direction. It is the same indexer,
  run another way, for a narrower set of cases.
- **A thorough blog post** for nuthatch-indexer.com, explaining what the embeddable engine is and how it
  works, follows once the slices are done, in the same register: a capability beside the binary, with
  S7's Lodestar page as its worked example. It is not written in this RFC's PR.

## 12. S0 measured outcome, 2026-10-30

**What was built.** `spike/dbsp-wasm/plain/` is RFC-0075 S3's live cursor as a plain core module. It
compiles the spike's library with nothing changed in decode, the circuits, the sealer, the store or the
routes. It exports `nh_abi`, `nh_open`, `nh_step`, `nh_resume` and `nh_request` as §3.2 has them, with
`nh_alloc`, `nh_free` and `nh_output`. It adds two S0 probes that are not part of `abi: 1`: `nh_report`,
which is what `LiveNest::follow` reports, and `nh_memory_bytes`. It imports `nuthatch.now_ms`,
`nuthatch.random_fill` and `nuthatch.log`, and nothing else. Storage is `bytes` only, because the
`store_*` imports are S2's. Two hosts drive the module. `plain/run-node.mjs` is Node with no
wasm-bindgen glue: `WebAssembly.instantiate`, `fetch` and `crypto.randomFillSync`. `plain/host/` is
wasmtime 48.0.8 from Rust with ureq, a separate crate with its own lockfile. CI runs both in a fourth
leg, `wasm-plain`.

**What removing JavaScript took.**

- *Core and the spike.* `nuthatch-core` gains a default-on `js` feature. It gates `fetch`, `opfs` and
  the `setTimeout` sleep, and its wasm32-only dependencies become optional behind it. With it off, a
  pending `time::sleep` registers its deadline and waker, and the embedding's executor turns the
  earliest one into `{"wait_ms": n}`. The spike gains the same feature for its bindings, `LiveNest`
  and `PlayNest` included. Its young-contract open and report moved into `young.rs`, which `live.rs`
  and the plain module share.
- *Four leaf crates, patched.* burrmill's wasm32 section (`crates/burrmill/Cargo.toml` at the pinned
  `ea8cf40`) turns on `uuid/js`, getrandom 0.3's `wasm_js`, getrandom 0.2's `js` and chrono's
  `wasmbind` unconditionally. Cargo features are additive, so nothing downstream can turn them off.
  `plain/vendor.sh` vendors four crates and applies `rfc-0077-plain.patch` (376 lines), under the
  plain crate's own `[patch.crates-io]`:
  - web-time 1.1.0's clock becomes `nuthatch.now_ms`.
  - chrono 0.4.45's `wasmbind` code is compiled out, and `Utc::now` reads web-time.
  - uuid 1.29.0's `js` means `rng-getrandom`.
  - getrandom 0.2.17's `js` means `custom`.

  getrandom 0.3 and 0.4 take `--cfg getrandom_backend="custom"`, and one symbol,
  `__getrandom_v03_custom`, serves both. Once those four were patched, nothing in DataFusion's tree
  reached JavaScript by any other route. wasm-bindgen is still compiled, but no reachable code calls
  it.
- *A trap in the executor.* A no-op waker stalls the cursor at the first batched header fetch
  (request 1,421 of the recording). The RPC client's `FuturesUnordered` polls only children that were
  woken. Answers and sleeps now wake the waker they were polled with.

**Gate: passed, both halves.**

- *Imports.* The `tab`-profile module after `wasm-opt -O1` imports `nuthatch.log`,
  `nuthatch.random_fill` and `nuthatch.now_ms`, three in all. The event build, rebuilt from the same
  tree, imports 84, every one from `./dbsp_wasm_spike_bg.js`. `plain/imports.mjs` fails on any other
  module.
- *Parity.* Under Node and under wasmtime, precompiled and compiled at load, the module follows S3's
  recording to the pinned `expected.json`: the segment hash `c983470c…479aeae`, `sealed_through`,
  `last_block` and the 1,000 balances. Beyond the gate, RFC-0075 S5's request set minus `/ready`
  (31 requests, through `nh_request` and DataFusion) gives byte-identical answers in both hosts. The
  sha256 over them is `68fd12bc…c5d776a8`.
- *The wait path.* The recording never refuses, so the gate run makes no waits. In a one-off run, the
  Node host answered the fourth request with `429` and `Retry-After: 1`. The client rested, `nh_step`
  returned one `wait_ms`, the follow took 1,501 ms instead of about 480, and every answer was the same.

**Numbers.** Apple M5 Pro, 18 cores, macOS. Node 26.11.1, wasmtime 48.0.8, one run each. The
second column is CI's `wasm-plain` leg on a 4-core x86_64 `ubuntu-latest` runner (run 41058267885).

| | M5 Pro, macOS | CI, Linux |
|---|---|---|
| plain module, raw | 32,667,941 bytes, 5,007,337 brotli (event build today: 34,505,176 and 5,248,083) | 32,681,299 bytes |
| Node: instantiate; follow (1,433 RPC posts, 1,434 steps, no waits); peak RSS | 32 ms; 480 ms; 339 MiB | 151 ms; 1,536 ms; 397 MiB |
| wasmtime, Cranelift, compile on every core | 1,702 ms (18 threads), peak RSS 1,772 MiB | 18,193 ms (4 threads), peak RSS 1,275 MiB |
| wasmtime, Cranelift, compile on one thread | 12,085 ms, peak RSS 1,591 MiB | 42,345 ms, peak RSS 1,272 MiB |
| wasmtime, Winch, every core / one | 808 ms / 2,956 ms, peak RSS 1,724 / 1,712 MiB | not run |
| precompiled `.cwasm` | 97,303,216 bytes (Cranelift), 183,224,168 (Winch) | 102,611,896 bytes (Cranelift) |
| load the precompiled module | 9 ms; peak 119 MiB through the follow and 31 queries | 7 ms; peak 124 MiB |
| follow in wasmtime, precompiled | 327 ms (Cranelift), 611 ms (Winch) | 902 ms |
| linear memory at the end | 33.6 MiB in both hosts | 33.6 MiB in both hosts |

**The budget question.** A 34 MB-class module compiles in wasmtime. The compile peaks at 1.3 GiB on
Linux and 1.8 GiB on macOS, whichever compiler runs and however many threads it has. That makes the
cost the module's state, not codegen parallelism. It takes 2 s on 18 cores, 18 s on CI's 4 and 42 s
on one thread. RFC-0074's address build cost about 95 MB for 1.36 MB, so the cost scales roughly with
size. A precompiled module loads in under 10 ms, and the whole run, follow and queries included,
stays under 130 MiB.

**Does S1 proceed as written: yes. The promise narrows by one sentence.** The event build runs in a
non-browser host. That host pays up to 1.8 GiB once, to compile, and should cache the result. From
then on it loads in under 130 MiB, inside the per-cursor budget. The one-time peak is under the 2 GiB
ceiling and well over the 600 MiB one. A host that must compile at every start inside phone-sized
memory, or a 128 MB isolate, cannot run the event build; it gets the address build. §1's "any host"
should say so when S2 writes the interface down.

What changes for later slices:

- **S1.** The plain build needs its own RUSTFLAGS. Cargo merges `rustflags` across config files, and
  the spike's `.cargo/config.toml` selects getrandom's `wasm_js`. With both cfgs set, getrandom 0.3
  does not compile. `plain/build.sh` replaces the flags rather than adding to them, and `embed/`'s two
  builds will need the same. The four leaf patches belong with the dbsp patch's move to a pinned fork.
  A burrmill change that put its wasm32 dependencies behind a default-on `js` feature would retire
  the uuid and getrandom 0.2 patches. Whether chrono's `wasmbind` has another enabler is still to be
  checked.
- **S2.** The wasmtime reference host should cache its compiled module (`Module::serialize`, or
  wasmtime's cache), and `embed/README.md` should state the compile peak. S0 carries RPC bodies as
  JSON strings inside JSON. S2 decides whether `abi: 1` keeps that or passes bytes. Any host's executor
  must deliver wakes.

**What it does not show.** The plain module has not run in a browser; the browser host is S2. The
`host` storage mode and its imports are unbuilt. Nothing was measured on a phone-class device or
through rotki's Python wasmtime binding, whose wasmtime version will differ. Peak RSS is the
process's `ru_maxrss`, so it includes wasmtime's own. wasmtime 48 is the newest release whose MSRV is
the pinned 1.95; 50 needs 1.97.

**The binary.** Unchanged. Its `cargo tree` with features is line-for-line the same as before, except
that `nuthatch-core` now shows `default,js`, a feature that gates only wasm32 dependencies. No
lockfile changed; the two new crates carry their own, and wasmtime appears only in `plain/host/Cargo.lock`.

## 13. S1 measured outcome, 2026-10-30

**What moved.**

- `entity_bind`, `entity_decl`, `entity_lower` and `sql_errors` are in `nuthatch-core`. The binary
  re-exports them at their old paths, so nothing that names them changed. Core gains direct edges
  to `sqlparser`, `toml`, `alloy-primitives` and `nuthatch-scaffold`, all already in the binary's
  tree with the same features. Tests that need the binary moved beside it: the two offchain-snapshot
  cases to `src/entity_offchain.rs`, and the nine Burrmill-raised hint cases to
  `src/engine_burrmill.rs`. None was dropped.
- Offchain tables are two traits in core, `OffchainTables` and `OffchainSnapshot`. The binary
  implements them over its snapshot files; an embedding binds through `NoOffchain`, which replaces
  `offchain_none.rs`.
- `nuthatch_core::bind` holds what the node and the tab both bind `/sql` with: an entity relation's
  rows and casting view (`bind_relation`), the column types (`hold_relation_types`), `read_sealed`'s
  probe and row read, and the walk through views (`expand_through_views`, `referenced_tables`). Both
  call it.
- `spike/dbsp-wasm/` is `embed/`, crate `nuthatch-embed`; `plain/` is `nuthatch-embed-plain`; `web/` is
  `embed/hosts/browser/`. There is no `spike/` directory and no `#[path]` from `embed/` or `core/`
  into `src/`.
- `tests/embed_versions.rs`, in the binary's suite, fails when a crate in `embed/Cargo.lock` or
  `embed/plain/Cargo.lock` resolves to a version the binary's lockfile does not hold on the same
  semver-compatible line. Only the wasm-bindgen family may differ: it is pinned to the CLI the
  wasm-tools action installs, and the binary never builds it.

**What it found.** The check failed on its first run with 144 drifted crates in each engine lockfile,
alloy 1.7.3 against 1.6.0, serde_json 1.0.154 against 1.0.150 and zstd-safe 7.3.0 against 7.2.4 among
them. Both lockfiles are now aligned. The plain build had patched uuid 1.29.0 and the binary has
1.23.5; the uuid part of `rfc-0077-plain.patch` applies to 1.23.5 unchanged, so the plain build now
patches that.

**The forks.** Chief approved forks under nuthatch-org the same day, so the patches left `vendor.sh`.
Each fork has a branch at the exact rev the crate was published from (the sha1 in its
`.cargo_vcs_info.json`) carrying two commits: the crates.io manifests, which resolve workspace
inheritance, and then the patch. Every source file the patches touch was first compared with the git
file at that rev, and all matched, so `embed/` builds the same sources `vendor.sh` produced.

| Fork | Branch | Base | Head |
|---|---|---|---|
| nuthatch-org/feldera | `nuthatch/wasm32-0.320.0` | `9a66270b` | `48131669` (24 files, +499 -89) |
| nuthatch-org/size-of | `nuthatch/wasm32-0.1.7` | `e6deefbd` | `1181ec2f` |
| nuthatch-org/web-time | `nuthatch/plain-1.1.0` | `2d200ded` | `d5700e7e` |
| nuthatch-org/chrono | `nuthatch/plain-0.4.45` | `17033825` | `8a1f8f62` |
| nuthatch-org/uuid | `nuthatch/plain-1.23.5` | `5dc6b3d1` | `66da8597` |
| nuthatch-org/getrandom | `nuthatch/plain-0.2.17` | `b625985d` | `48857ee9` |

`embed/` and `plain/` take them by `rev` under `[patch.crates-io]`. `vendor.sh`, `plain/vendor.sh` and
the two patch files are gone, and so are CI's vendor steps. The manifests commit is what makes this
pinning rather than porting: the patch's manifest hunks were written against crates.io's flattened
manifests, and feldera's own use workspace inheritance, which cannot turn a workspace dependency's
default features off per crate. An upstream PR therefore needs those hunks rewritten against
feldera's workspace.

**What did not move, and why.**

- *`define_views_bound` as one function over a segment reader.* Only the pure parts moved. The node's
  version decides what happens to a segment that is missing, corrupt, quarantined, gone on a follower
  or overtaken by a new catalogue, against files on disk. Putting that behind a reader trait is a design
  choice about the native path, which §1a says wins, so it is left for the slice that needs it (S3's
  rebuilds read through the same reader).
- *Address history.* On main nothing is duplicated: `embed/` with the address build exists only on
  `pete/rotki-embedded-s0`. The modules are about 12,400 lines (`address_history`,
  `address_discovery`, `address_residual`) that reach `address_mode` (10,541 lines), the binary's
  `Store`, `rpc::RpcClient`, `config`, `freshness` and `analytics::QueryBudgetExceeded`. The window
  driver is tokio inside `address_mode`. Separating them needs RFC-0074 S2's open decisions: the store
  seam, and how the driver yields to a host. Not mechanical, so not done.
- *The `events` and `address` features.* With no address build on main, an `address` feature would be
  empty. They land with the address crate.
- *The upstream offers.* UPSTREAM.md's issue and PRs are not posted; that needs Chief's word.

**The binary.** Unchanged:

- *Dependencies.* `cargo tree -e features` (default, `--all-features`) and `cargo tree -e normal,build
  --target all` list the same crate, version and feature nodes before and after: 1,917, 1,998 and 614.
  The lockfile gains four dependency edges on `nuthatch-core`, and no package.
- *CLI and config.* Every subcommand's `--help`, two levels deep, is byte-identical. The files
  `nuthatch init --abi` writes are identical.
- *Tests.* The whole suite on the M5 Pro: 2,877 passed, none failed, 18 ignored. That
  includes `e2e_tab_parity`, `e2e_tab_entities` and `e2e_tab_answers`, the node's half of the pinned
  segment hash and answers. Benches and budgets are CI's.

**The engine.** Run locally on the forks, every gate CI runs outside Chrome passes. `embed/`'s native
test passes, 14 tests and 1 ignored. The tab module, 34,495,764 bytes after wasm-opt, gives S3's
segment hash and 1,000 balances, S4's entities and views before and after the reorg, all 32 of S5's
answers byte-identical, and S6's budgets. The plain module still imports three functions, all
`nuthatch.*`, and gives the segment hash and balances in Node and in wasmtime. Its 31 answers hash to
`68fd12bc…c5d776a8` in both, the sha256 S0 recorded.

**For burrmill-wasm.** `scripts/build-play.sh` defaults `NEST_TAB` to `../nuthatch/spike/dbsp-wasm`
and copies `dbsp_wasm_spike.js` and `dbsp_wasm_spike_bg.wasm`; `web/play/live/live.js` imports
`./pkg/dbsp_wasm_spike.js`. They become `../nuthatch/embed` and `nuthatch_embed*`. Until that change,
`/play/live` builds with `NEST_TAB` set and the old names fail loudly at the copy.

**Not covered by S1's gate as written.** RFC-0074 S0's parity re-run against an address build: there
is no address build on main to run it against. The wasm32 Chrome leg (S2's OPFS and reorg transcript)
runs in CI only.

**Also seen.** `tools/rfc-0076-tenants/src/main.rs` still includes `src/fair_share.rs` by `#[path]`.
It is RFC-0076's bench, not the engine, and out of this slice; the gate's "anywhere" catches it.

## 14. S2 measured outcome, 2026-10-30

**What was built.**

- *The interface, written down.* `embed/README.md` specifies `abi: 1`: every export and import with
  its signature, the memory ownership rules, the step, resume and request protocol, the `nh_open`
  config, eleven named faults, the two storage modes, what a host needs, and how versions move.
- *The guest.* `embed/plain/` implements it. `nh_open` takes the nest's authored files, the memory
  ceiling, the poll interval and the storage mode. Storage `host` is redb over a file the host holds
  open (`plain/src/host_file.rs`). `nh_export` gives the store in storage `bytes`. Every error is
  `{"fault", "message"}`.
- *The browser host* is `embed/hosts/browser/host.js`, about 170 lines, with no wasm-bindgen. It runs
  the module through `abi: 1` with OPFS (`opfs.js`) in the worker that answers the Service Worker,
  and in Node over files (`hosts/node/`). `nuthatch_core::opfs` is no longer reached by this module:
  the host's JavaScript holds the files.
- *The wasmtime host* moved from `plain/host/` to `embed/tests/wasmtime-host/`. It caches the compiled
  module by the module's sha256 and wasmtime's compatibility hash, implements storage `host` over
  files, and holds the pinned interface.
- *The pinned interface.* `tests/wasmtime-host/src/abi.rs` holds `abi: 1`'s 33 shapes as wasmtime
  types them; `wasmtime-host abi <module>` fails a module whose shapes differ from the version it
  reports, and the version's table is frozen by its sha256 (`59363eb2…f9a11071`). A unit test fails if
  the README stops naming any of them.
- *CI.* `wasm-plain` builds the module once and runs every host on that file: the pinned-interface
  check, S3's gate in Node and wasmtime, S5's in Node and wasmtime in both storage modes and in Chrome
  on OPFS, then `hosts/agree.mjs`, which fails unless every run loaded the same file and every S5
  answer set hashes to the pin's digest. RFC-0075 S5's Service Worker run moved there from `wasm-tab`,
  because the browser host now drives the plain module; the wasm-bindgen module's 32 answers are still
  gated in Node by `run-answers.mjs`, and `wasm-tab` no longer builds the `web` binding nothing in this
  tree used.

**Where the interface departs from §3.2.**

- *Opening, removing and listing files are requests, not imports.* §3.2 had eight synchronous
  `store_*` imports and said every host has a synchronous file. Every host has a synchronous file
  once it is open, but a browser cannot open one synchronously: `getFileHandle`,
  `createSyncAccessHandle`, `removeEntry` and `entries` all return promises. So `open`, `remove` and
  `list` arrive through `nh_step` beside `rpc`, and the imports are the six that act on an open handle:
  `store_read`, `store_write`, `store_len`, `store_set_len`, `store_sync` and `store_close`, the last
  new, because closing is what releases OPFS's lock. In storage `host` the store therefore opens on the
  first `nh_step`, not in `nh_open`.
- *`log` is not optional.* A core module has no optional imports; a host that wants no log supplies
  one that does nothing.
- *`nh_abi` says more*: `{"abi": 1, "capabilities": ["events"], "storage": ["bytes", "host"],
  "engine": "<version>"}`.
- *The linker's leftovers are part of the shape.* The module also exports getrandom's two hooks, zstd's
  eight C shims, `__data_end` and `__heap_base`. A host can see them, so they are pinned and the
  README names them as not for calling. S0's two probes, `nh_report` and `nh_memory_bytes`, are pinned
  too; what they report is not promised.
- *Until S4 the config is the young contract's* (`contract`, `start`), as RFC-0075's tab is; S4 adds
  `nuthatch.toml`.

**The RPC-body question: strings.** `abi: 1` keeps each RPC body as a JSON string inside the JSON, in
both directions. A reply must pass whole even when it is a provider's HTML error page, and a string is
what every host's HTTP client already returns. Bytes would mean base64, a third larger, or a second
buffer and a framing per answer that every host implements. Measured on S3's recording: 1,901,530
bytes resumed for 1,687,032 bytes of bodies over 1,433 RPC posts, 12.7% for the escaping and the
answer wrappers, inside a follow that takes 0.5 to 3.8 s.

**Gate.**

| | Result |
|---|---|
| The same `.wasm`, unmodified, in Chrome, Node and wasmtime, S5's 32 requests equal to `s5-expected.json` | **pass.** CI (run 41083776606, `wasm-plain`): seven runs of one module, `1a5086f8…ba7187d2`, Chrome on OPFS, Node and wasmtime each over files and in `bytes`; every S5 answer set's digest `22966cee…363febc5`, the pin's. Locally on the M5 Pro, Node and wasmtime gave the same digest for `524e0009…d23b9ec3` |
| The address build gives RFC-0074 S0's lists in the wasmtime host | **deferred**: there is no address build on main (§13); extracting one needs RFC-0074 S2's store seam and driver-yield decisions, which are not made |
| A shape change without a version bump fails a pinned-interface test | **pass**: `nh_export(i32)` built into a real module fails `wasmtime-host abi` by name; the table's own unit tests cover a changed, added or removed shape and an unpublished version |
| Binary unchanged; wasmtime in no lockfile but `embed/`'s | **pass**: nothing outside `embed/`, the workflow, this RFC and one comment in `tests/embed_versions.rs` changed; wasmtime is only in `embed/tests/wasmtime-host/Cargo.lock` |

The digest is one sha256 over each answer's method, path, status, content type and body, masked as
`tests/e2e_tab_answers.rs` masks `/ready`; `embed/mask.mjs` and the wasmtime host compute it the same
way, and the pin's own digest is printed beside it. S3's segment hash and 1,000 balances also hold in
Node and wasmtime over files.

**Numbers.** Apple M5 Pro, 18 cores, macOS, Node 26.11.1, wasmtime 48.0.8, one run each, the `tab`
module of 33,947,090 bytes.

| | Node, browser host over files | wasmtime |
|---|---|---|
| Load | instantiate 31 ms | compile 1,668 ms, peak RSS 1,896 MiB; from the cache 63 ms, 130 MiB after |
| Follow of S5's chain, storage `host` (files) | 3,785 ms | 3,359 ms |
| Follow, storage `bytes` | 689 ms | 500 ms |
| Peak RSS through follow and queries | 283 to 303 MiB | 152 to 161 MiB |
| Linear memory at the end | 30.2 MiB (S3), 34.3 MiB (S5, files), 37.3 MiB (S5, bytes) | the same |

On CI's 4-core `ubuntu-latest` runner, the same run's module (34,010,529 bytes): wasmtime compiled it
in 17,719 ms at a peak of 1,369 MiB, then loaded it from the cache in 30 ms (63 MiB after, 154 to
164 MiB peak through follow and queries). S5's follow took 2,318 ms over files and 2,082 ms in `bytes`
in Node, 1,513 and 1,343 ms in wasmtime, and 3,586 ms in Chrome on OPFS, the page's whole open.

Storage `host` over files costs about 3 s on the Mac, because redb syncs every commit and the hosts make that
a real `fdatasync`; `bytes` syncs nothing. `nh_export` gives 2,641,920 bytes after S5's follow.

**What it does not show.** The Chrome leg has not run on this Mac: Chrome for Testing would not spawn
here (`spawn` error -88), as before, so the browser host's evidence is CI's alone. Safari
is still untested. The Node and wasmtime hosts take no lock on the store, so opening one nest twice
from two processes is the host's to prevent, as the README says. Warm reopen is S3's: every gate run
opens `fresh`.

**The binary.** Unchanged. The diff outside `embed/` is `.github/workflows/ci.yml`, this RFC and a
comment in `tests/embed_versions.rs`; no `Cargo.toml` or `Cargo.lock` of the binary changed.

## 15. S3 measured outcome, 2026-10-30

**What was built.**

- *The rebuilds, in core.* `nuthatch_core::restart` holds what a warm start rebuilds from stored
  history with no RPC: `rebuild_views` (balances, exposure, velocity; #294's one hot scan),
  `seed_entities` (RFC-0041 §5.3, #865) and `rebuild_children` (#373). They read sealed segments
  through `SealedHistory`: the catalogue, one segment's rows through `DecodedRow::from_stored`, one
  segment's columns. An entity is seeded through `SeedEntity`, which the node's `EntityView` and the
  tab's `TabEntity` both implement. Error texts, the segment order, the `sealed_through` bound and
  the per-segment chunking are the node's, moved, not rewritten.
- *The native path wins.* The node's implementation is `SegmentFiles` over `data/<nid>/`, in
  `src/indexer.rs`: segments through `seal::read_segment_decoded`, columns through the existing
  `visit_sealed_columns`, and the views' cold folds through `ColdFolds`, which is Burrmill's
  pre-summed `net_balances`, `oversized_transfers`, `cold_exposure` and `cold_velocity`, as before. An
  implementation without `ColdFolds` replays each sealed transfer through the live loop's deltas
  instead, which equals the view grown from genesis. The embed takes that path. The node's three
  functions keep their names and signatures as thin calls into core, so `build_nest`,
  `FollowerDerived::build`, the follower and their tests are untouched.
  `the_restart_rebuild_walks_the_hot_store_once` reads `core/src/restart.rs` now, where the scan moved.
- *The cellar persists.* Over a store, `Cellar` keeps each segment's bytes and the catalogue in two
  tables of the store's own file, `cellar_segments` and `cellar_manifest`. A seal's new segments, its
  catalogue and the removal of what the catalogue no longer names commit in one transaction, before
  the seal prunes hot. Nothing is held in linear memory between seals any more: a segment is read
  from the store when a query binds it, and storage `bytes` carries the segments in `nh_export`.
- *The module, on open.* `young::open` builds the store, the cellar over it, and the views rebuilt
  from both when the store has indexed before; `TabNest::derive` seeds the entities the same way. A
  rebuild or seed that fails fails `nh_open` (`open_failed`, with its reason) rather than serve part
  of a history: a reopened embedded nest has no `unavailable` state of its own. A store that sealed
  before its segments were kept (any engine before S3) has lost them and is refused by name; no gate builds such a store.
- *Hosts.* `--restart` on all three reference hosts (README, "The reference hosts and the gate"):
  follow from an empty store until a segment is sealed, die by SIGKILL with requests outstanding (a
  child process for Node and wasmtime; for Chrome, the browser process, then a second launch on the
  same profile and origin), reopen the same storage with `fresh: false`, finish. `hosts/agree.mjs`
  now also requires an S5 restart run from every host, and one set of segments per gate across every
  run, killed or not. Each run prints `segments_sha256`, one digest over its segments computed the
  same way in `mask.mjs` and the wasmtime host.

**Where this departs from §3.3: segments are not `segments/<hash>.parquet`.** §3.3 had the module
write the node's layout, a file per segment. It cannot, without bending one side. A seal runs inside
the window's commit, synchronously, through `SegmentSink`; a browser opens a file only through a
promise (S2's reason `open` became a request). A file per segment could only be written behind the
seal, after the prune had removed its rows from hot, which is the crash window #2261 closed on the
node. Making the seal path asynchronous would change the node's, which §1a rules out. Inside the
store, the segments commit before the prune, as the node's `manifest.json` does, through the same
`store_sync`. The bytes are the node's: the gate compares their hashes. **`abi` stays 1**: no export
or import changed, and no request kind was added. Hydrating a mirror (S4) happens before the cursor
runs, when asynchronous opens are fine, so S4 may still write `segments/` files for mirrored history.

**`rebuild_children` is in core and unused by the module.** The tab has no factories until S4, so it
is exercised only by the node's tests (`rebuild_children_*`, unchanged and green).

**Gate.**

| | Result |
|---|---|
| S3's recording, killed mid-follow and reopened from the same storage: segment hashes and the 1,000 balances equal the node's | **pass**, Node and wasmtime over files. Killed at block 26,241,666 with the first segment sealed through it and 380 steps taken; the reopen finished in 1,058 steps. Segment `c983470c…479aeae`, and `expected.json` whole |
| S4's fork with entities (S5's chain), killed and reopened: `/balances`, `/derived/*`, every view, against the node's pin | **pass**, Node and wasmtime over files: all 32 of S5's answers digest to `22966cee…363febc5`, the pin's, the same kill point. Chrome on OPFS: CI's (below) |
| The segment hashes equal an uninterrupted run's | **pass**: every run, killed or not, in `bytes` or over files, in both hosts, gives segments digest `68db3e3e…2d853cbf37` |
| The same in the browser host on OPFS | **CI's** `wasm-plain` leg: `run-sw.mjs --restart`. Chrome for Testing does not launch on this Mac (§14) |
| Binary: `e2e_warm_restart`, `e2e_entity_reorg`, `bench_restart_mounts` unchanged and green | **pass**: no file changed; 4 and 29 tests pass, the bench runs (below) |
| Binary: restart no slower on the same bench | **pass**: `bench_restart_mounts`, release, on the M5 Pro, two runs of each build (main at `5b7cadfe`, then this branch). Serving and resumed at 500 mounts, before: 7,007 / 8,703 ms and 5,838 / 7,757 ms; after: 6,782 / 8,408 ms and 5,962 / 7,344 ms. At 100 mounts 1,711 and 1,549 ms before, 1,550 and 1,546 ms after. The follower's derived state over 500 sealed segments of 500,000 rows, built through the moved `seed_entities` and `rebuild_views`: 986 and 916 ms before, 838 and 827 ms after. The run-to-run spread is wider than any difference |

Both halves were mutation-checked on the plain module. With the views' rebuild skipped on open, S3's
restart run fails on its report and S5's on `/balances` and `/balance/…`, while the uninterrupted S3
run still passes. With the entities' seed skipped, S5's restart run fails on `/ready`, the two views
that read `received`, and every `/derived` route; the uninterrupted run still passes.

**Numbers.** Apple M5 Pro, macOS, Node 26.11.1, wasmtime 48.0.8, the `tab` module of 34,017,235
bytes (`86d9dca7…7035c7`), one run each.

| | Node, files | wasmtime, files |
|---|---|---|
| S3, uninterrupted follow | 3,620 ms, 1,436 steps | 3,484 ms |
| S3, reopen after the kill, to the tip | 2,762 ms, 1,058 steps | 2,443 ms |
| S5, uninterrupted follow | 3,805 ms | 3,494 ms |
| S5, reopen after the kill, to the tip | 3,042 ms | 2,684 ms |
| Linear memory at the end: uninterrupted, reopened | 34.3 MiB, 23.9 MiB (S5) | the same |

A reopen of S5's finished store, rebuilding the views and both entities from one sealed segment of
2,259 rows and 17 hot rows, then checking the tip: 116 ms in Node, three RPC posts, with `/balances`
and `/derived/received` byte-identical before and after.

The rebuild replays sealed history row by row, so its cost grows with the nest. That is fine for a
young contract and not for RFC-0068's 128 MB DeFi Pulse segments: S4, which opens mirrored nests,
should give the module a `ColdFolds` in Burrmill as the node has, and measure it.

**#2534.** The unavailable reason (`EntityView::start_bound`, and the tab's copy) said RFC-0041
§5.3's warm-restart seed was not implemented. It is, and the reason now says what it means when it
shows: the seed did not complete, the log says why, and the next restart seeds again. The comment
above `refuse_seal_direct_with_entities` and its message are corrected the same way. The refusal
stays: the seed runs only when a nest starts warm, so a seal-direct run would serve its entities
empty until the next restart. Moving the seed found a defect it did not fix, filed as #2539: an
entity whose seed fails after `seed_begin` is left available and partly fed, not unavailable.

**For burrmill-wasm's `/play/live`.** It opens a fresh store on every visit because entities started
unavailable on a warm one. That reason is gone: `LiveNest::open` rebuilds the views and `derive`
seeds the entities from the cellar, which now lives in the OPFS store. To stop sweeping, `live.js`
needs a stable `nid` per contract and start block in place of a fresh one per load, no sweep of old
stores, `derive` called with the same files as before, and its comment ("Each load opens a fresh
store") dropped. A store a pre-S3 build sealed is refused at open, so the page should sweep that one
store on that error and open it fresh.

**What it does not show.** Chrome has not run this on the Mac; Safari not at all. The kill lands
between steps, with RPC requests outstanding; a kill inside a host storage call is redb's
crash-safety, which this does not test separately. Node and wasmtime hosts still take no lock.

**The binary.** Unchanged in behaviour: the three rebuilds run the same code through the trait, the
views' cold folds are the same Burrmill queries, and only the unavailable reason and the seal-direct
refusal's last sentence read differently (#2534). `cargo tree` and `Cargo.lock` unchanged. Every
subcommand's `--help`, two levels deep (60 files), and the files `init --abi` writes are
byte-identical before and after. The whole suite: 2,877 passed, none failed, 18 ignored.

## 16. S4 measured outcome, 2026-10-30

**What was built.**

- *The nest's part of `nuthatch.toml`, in core.* `nuthatch_core::nest_config` holds `[nest]`, the
  contracts, templates, factories, flags and `[extract]` as the binary's `Config` declares them, the
  schema-version gates, the path-segment check, the decode registry they build and the factory rules
  they declare. The binary's `Config` takes those types from core and keeps webhooks, alerts, calls,
  IPFS and address history, which only a node runs. `NestConfig::parse` is how the module reads a
  `nuthatch.toml`: the same types and gates, and a refusal by name of what only a node runs, of a key
  nothing reads and of `[extract]`.
- *The chain registry, in core.* `nuthatch_core::chains` holds every registered chain and its policy,
  Arbitrum One's included. What reaches an RPC or parses a URL (`resolve`, the `arb1` caveat and
  header cap) stays in the binary's `chains.rs`, which re-exports the rest.
- *The seed's checks, in core.* `nuthatch_core::mirror` holds the seed snapshot's envelope, the keys a
  published directory uses, and the checks `nuthatch seed` makes before it trusts a catalogue
  (`check_snapshot`, `local_catalogue`, `canonical`), with RFC-0068 §3's postcard cut and digest beside
  them. `src/seed.rs` and `src/publish.rs` call them where they defined them.
- *The views' cold folds, in core.* The four Burrmill queries a node's `ColdFolds` runs, and the reading
  of their rows, are in `nuthatch_core::restart`; `src/analytics.rs` calls them.
- *Any nest in the module.* `nh_open` takes a nest's files with its `nuthatch.toml` (`NestSpec`), on the
  chain's registered policy, with its factories (children rebuilt on a warm open, as a node rebuilds
  them), labels and flags. RFC-0075's young contract stays, as `contract` and `start` with no
  `nuthatch.toml`.
- *Mirror hydration.* `nh_open`'s `mirror` makes the module compute the nest's data identity from its
  files, read the dataset's seed snapshot and catalogue, check them as `nuthatch seed` does, and ask
  the host for every segment and seed tail. Optionally a postcard pins the catalogue. Then the views,
  children and entities are rebuilt from the hydrated history, and the cursor follows the tip from
  `complete_through + 1` unless `follow` is false.
- *The cold folds in the module.* `embed/src/folds.rs` answers `ColdFolds` with the node's queries, run
  by Burrmill over the cellar's segments, read by range. A warm or mirrored nest's views are rebuilt
  from one row per key, as on a node.
- *Hosts.* The browser host answers `get` (`host.js`, so in Node and in a browser alike); the nest
  worker opens any `nh_open` config; `hosts/browser/mirror.html` and `run-sw-mirror.mjs` are the
  browser gate, `hosts/node/mirror.mjs` the Node one, `run-mirror-node.mjs` the node's half.

**`abi` stays 1.** No export or import changed shape: `wasmtime-host abi` finds the 33 pinned shapes
equal. What was added is what the README's versioning rules allow within a version: optional `nh_open`
keys (`mirror`, `follow`; `contract` and `start` optional once `files` holds a `nuthatch.toml`), the
`get` request kind, asked for only behind `mirror`, an answer field (`nh_report`'s `mirror`, a probe)
and log level 2.

**Where this departs from the RFC.**

- *The segments go to one pack, not `segments/<hash>.parquet`.* §15 left S4 free to write a file per
  segment for mirrored history. A host file is a sync access handle in a browser, and DeFi Pulse has
  1,529 segments: 1,529 `open` requests at every warm open, and as many handles held. The module
  instead has the host append each segment to `mirror.pack`, one file, and records each one's offset
  and length in the store, in a table beside S3's. A query reads a segment by range from the pack
  (`cellar::Segments`, an `ObjectStore` over the cellar), so Burrmill reads footers and row groups,
  not whole files. The bytes are the node's: each is read back and hashed before the catalogue names
  it.
- *The budget charges declared entity state, not catalogue sizes.* The catalogue carries no byte
  sizes, so knowing them before a download is a HEAD per segment (1,529 for DeFi Pulse, 2,297 for GRT
  on mainnet), and the segments do not sit in linear memory anyway: they are on the host's storage,
  read by range. DeFi Pulse's 138 MB of segments open at 17 MiB of linear memory. What a mirrored
  nest declares that does live in memory is its entities, so admission is RFC-0075 §17's charge
  against the ceiling, made before the first `get`. A mirror needs storage `host`: in storage `bytes`
  the segments would be in memory, and that is refused by name.
- *The cold folds were needed, and are built.* S3's rebuild replays every sealed transfer and holds
  every delta until it applies them. On GRT's mainnet transfers that took the module to 1,519 MiB of
  linear memory, over what a 600 MiB ceiling leaves and close to the 2 GiB wall.

**Gate.** The node's half is `run-mirror-node.mjs` on a debug build of this branch: a copy of the nest
seeded with `nuthatch seed` and served with `nuthatch serve`, or with `--rpc` followed by `nuthatch
dev`. Both halves answer `embed/mirror-answers.mjs`'s requests: every authored view whole and by
count, each table's count, block range and `log_index` sum, each entity's first thousand rows, the
balances and the hot rows. Rows are compared as multisets, with the NID and the node's `cached` flag
left out. Two DeFi Pulse views cannot be compared row by row between any two engines and are compared
by shape: `stablecoin_supply_events` is capped at 50,000 rows, so which rows fill the cap is the
engine's order (the tables under it are compared whole); `largest_supply_moves_7d` ends `ORDER BY
abs(usd) DESC LIMIT 25` with 18 rows tied at 50,000,000.06 for 9 places, so it is asked again with
every column appended to the order, and that answer is compared whole.

| | Result |
|---|---|
| defi-pulse-nest from the public mirror, browser host, 2 GiB, equal to a node seeded from the same mirror at the same head | **pass.** Dataset `0a643352…b2e48f`, catalogue `8ba65fb0…5822e0` unchanged before and after the runs, head 26,306,829, all 33 answers equal (digest `73bfaf00…700799`) in the browser host in Node and in Brave (Chromium) on OPFS. Chrome for Testing still does not launch on this Mac; Brave does, so this is a real Chromium run. CI's Chrome leg runs the local-mirror gate |
| RFC-0075 S4's entity nest published to a local mirror, browser host, equal to the node at the same head | **pass**, Node and Brave on OPFS: at the seed's head 26,245,806 (digest `26882c7a…b8a69a`), and followed by both to the recording's tip 26,245,870 (`d943c23d…2911e8`), entities and views included. CI runs both in Node and in Chrome against the node's pins, which `tests/e2e_tab_mirror.rs` holds the node to |
| A nest that cannot fit at 600 MiB is refused before download, with its figures | **pass**, Node and Brave: the same nest declaring 150,000 rows per entity is refused (`open_failed`, "~932 MiB against the 450 MiB its 600 MiB ceiling leaves") with no GET made. At 2 GiB it opens and equals the node |
| A postcard pins the catalogue | **pass**: one made from the mirror's catalogue opens through the cut; an altered digest is refused before any segment is fetched |
| Binary: config parsing and the chain registry move without changing a byte accepted or written | **pass**, below |

**Numbers.** Apple M5 Pro, macOS, Node 26.11.1, the `tab` module after `wasm-opt`: 34,336,230 bytes,
5,259,438 brotli (S3's: 34,017,235). S5's ceiling of 5.5 MB brotli holds. One run each, over the public
mirror's Worker URL except where said:

| | DeFi Pulse | GRT supply, mainnet (a local copy of its published dataset) |
|---|---|---|
| Segments hydrated | 1,529, 138,128,890 bytes | 2,297, 286,028,654 bytes |
| Open, Node host | 46.4 s (40.7 s at 600 MiB); 0.86 s from a local copy | 5.5 s; 26.7 s by replay |
| Open, Brave on OPFS | 44.5 s | not run |
| Linear memory after open | 17 MiB | 242 MiB (232 at 600 MiB); 1,519 MiB by replay |
| Linear memory after the gate's queries | 171 MiB, the high-water of a 50,000-row answer | 242 MiB |
| Process peak RSS, Node | 616 MiB | 564 MiB |
| The gate's queries | 33 in 7.6 s | 9 in 2.6 s, equal to the node at 2 GiB and at 600 MiB |

Every statement binds every segment's footer, read by range from the pack: 1,529 per `/sql` on DeFi
Pulse, about 0.2 s each. An engine kept between statements would remove that; it is left for when a
page needs it.

**What it does not show.** Chrome for Testing and Safari: the browser evidence here is Brave, and CI's
Chrome leg runs the local mirror only, since a public download does not belong in CI. The wasmtime host
answers no `get`, so it opens no mirror. No Arbitrum One nest was opened from a mirror: the published
`grt-supply-arbitrum-one` comes from a commit the local checkout has moved past, so its data identity
no longer matches, and Arbitrum is shown by its registry entry and a parse test only; S7 will exercise
it. A host's RPC URLs never enter the module, so the `arb1` header-batch cap is the host's to apply. A
warm reopen does not check that the files given hash to the identity the store was hydrated under, as
the node's registry guard does. Labels reach the views but not `/sql`'s `labels` table, which the
module does not bind. A transient fetch failure once failed a public run in Brave; the browser host now
retries a `get`'s transport failure three times, as the README makes retries the host's.

**The binary.** Unchanged in behaviour. The moved code is the same code: `Config` reads
`nuthatch.toml` through the same types, and `registry::from_nest`, `seed` and the cold folds call core
with the same messages. `Cargo.lock` gains one edge, `nuthatch-core` to `serde_ignored`, already in the
tree at 0.1.14, and no package. Every subcommand's `--help`, two levels deep (60 files), and the files
`init --abi` writes for mainnet and for Arbitrum One are byte-identical to main's. The whole suite:
2,878 passed, none failed, 18 ignored; the one new test is `e2e_tab_mirror`. `tests/skill_refs.rs` now
names `core/src/nest_config.rs` as a config source: without it the config-reference gate would have
stopped scanning `[nest]`'s and `[[contracts]]`'s keys, silently, once the types left `src/config.rs`.

## 17. S5 measured outcome, 2026-10-30

**What was built.**

- *The verified head.* `nh_open` takes `"verified": true`. Once a follow, the module asks the host for
  its verified finalized block with a `verified_finalized` request, answered `{number, hash}`; then it
  fetches every header from the last seal up to that block through the RPC client, hashes each one
  (`embed/src/verified.rs`: the header's RLP and keccak, no new dependency but a direct edge to
  `alloy-primitives`), links them by parent hash down from the verified hash and to the last seal's
  hash, and holds every seal to that block and to rows whose block hash is on that chain. The hold is
  `ExtraRows::seal_through`, the seam core's window already has for exactly this. A refusal stops
  sealing, not the cursor: the next `{"idle": true}` carries `unverified` with the reason, and the
  rows stay hot. Messages are RFC-0065 S5's `chain.rs`'s. The browser host takes a `verified`
  callback; Helios stays in the page that supplies it (burrmill-wasm's `verifiedFinalized`).
- *Labels and children in `/sql`.* §16 found the module bound no `labels` table. The rows, and the
  `{template}__children` views a factory nest gets, are now built in `nuthatch_core::bind`
  (`label_rows`, `children_views`), moved from `engine_burrmill.rs` and `analytics.rs` with the same
  strings, and both the node and the module bind them.
- *The recorder.* `tests/e2e_embed_shapes.rs`, in the binary's suite, builds each shape's chain with
  `tests/common/tape.rs`'s helpers, era by era, and writes it out as a fixture-rpc recording beside
  the nest and its requests under `tests/fixtures/embed-shapes/`. fixture-rpc's `recorded` reads
  several eras (`{chain_id, eras: [{tip, finalized, headers, logs}]}`), moving on at `POST
  /control/fork`, and `--headers` serves whole headers where their hash is the recording's. The test
  runs the real binary (`nuthatch dev`) over each recording, settles it after each era (the era's
  newest row read back with its hash, then the cursor and the seal unmoved over two polls) and pins
  its answers. Without `NUTHATCH_PIN_EMBED_SHAPES` the recordings, nests and requests must be what the
  code builds, so a pin cannot drift from the chain that made it. `embed/hosts/node/shapes.mjs` is the
  module's half over the same recordings.
- *Size gates.* `size.mjs --max-brotli 5500000` fails the plain module in `wasm-plain` and the
  wasm-bindgen module `/play` loads in `wasm-tab`.

**`abi` stays 1.** No export or import changed: `wasmtime-host abi` finds the 33 pinned shapes equal.
`verified` is an optional `nh_open` key, `verified_finalized` a request kind asked for only behind it,
and `unverified` an optional answer field, which the README's versioning rules allow within a
version. §4 called it a host input; a request is how a host input reaches a module that yields, and a
new export would have cost a version for a value the module needs once a follow.

**Found by the shapes, and fixed.** The module's tip loop rolled back a reorg without dropping the
RPC client's timestamp cache above the fork, as the node does (`forget_cached_above`, RFC-0029 §6d).
Every reorg shape whose replacement blocks carry new timestamps answered the old ones; RFC-0075 S4's
fork could not show it, because its replacement blocks keep their timestamps. `TabNest::follow` now
forgets them where the node does.

**Gate.**

| | Result |
|---|---|
| With a verified head, a forged header or a log off the verified chain stops sealing by name | **pass**, `hosts/node/verified.mjs`, six cases on RFC-0075 S3's recording with whole headers to block 26,241,700 (1,894 headers, `headers.json.gz`, 1.1 MB, recorded from publicnode by `record-headers.sh`). The honest block seals through 26,241,700 and its segment is the node's `c983470c…479aeae`, 1,894 headers hashed, the 1,000 balances the node's. No light client: nothing sealed, "the host gave no verified finalized block: out of sync". A hash the chain does not reach: nothing sealed, "block 26241700 is 0xfd12…; the light client verified 0x7777…". Block 26,240,500's receipts root altered: nothing sealed, "block 26240500 hashes to 0xfe06…, not the 0x7bc4… its RPC gave". The same header withheld: nothing sealed, the RPC client's "block_headers: 1/1894 header(s) missing", which does not name the block. Block 26,240,101's logs moved to another block hash: sealed through 26,239,812, below it, "the RPC's logs for block 26240101 are not on the verified chain". The cursor reaches the tip in every case |
| Every listed shape equal to the node's | **pass**: 13 shapes, 25 eras, 175 answers, every one equal to the node's pin. The scaffold `pair` (2,000 mainnet blocks, a segment cut by the 1,800-block span) and `two`; `factory`, a parent whose `Transfer` recipients are children, reorged to drop one child and find another, `child__children` included; `labels`, `tests/e2e_follower.rs`'s derived nest with flags, a labelled address and an entity, sealed through 2, the `labels` table and a join on it included; `reorg-above-seal`, three eras, sealed through 8 and reorged above it; and `tests/e2e_reorg.rs`'s property at depths 1, 2, 3, 7, 12, 13, 18 and 23, both ends and the middle of each band |
| The size gates in CI | **pass** for the event build: `wasm-plain` and `wasm-tab` fail past 5,500,000 bytes brotli. The address build's 1.5 MB raw gate is **deferred**: there is no address build on main (§13, §14) |
| Binary unchanged; S5 adds nothing to it | **pass**, below |

The gates were mutation-checked on the module. Without the header check, the unreachable-hash, forged
header and off-chain cases go red; without the row check, the off-chain case does; without the labels
table, `labels` does; without the children views, `factory` does; without `forget_cached_above`, the
reorg depths do. Each mutant was a different file by sha256 before it ran.

**Numbers.** Apple M5 Pro, macOS, Node 26.11.1, the `tab` module after `wasm-opt`: 34,377,919 bytes,
8,038,925 gzip, 5,267,248 brotli (S4's: 34,336,230 and 5,259,438), 232,752 bytes under the ceiling.

| | |
|---|---|
| The honest verified follow of S3's recording, 1,894 headers hashed, in `bytes` | 936 ms |
| The missing-header case, the RPC client's eight retry rounds | 7.4 s |
| All 13 shapes through the module, one process | 3.2 s, peak RSS 577 MiB |
| The node's half, the real binary over the same 13 recordings, debug build | 186 s, mostly settling |
| RFC-0075 S3, S5 and S5 restarted, re-run on this module | unchanged: segment and balances, digest `22966cee…363febc5` |

**Where this departs from the slice as written.**

- *Factories.* `tests/e2e_runtime_lifecycle.rs` and `tests/e2e_follower.rs` declare no factory. The
  factory shape takes `tests/e2e_nest_inspect.rs`'s declaration, a `Transfer`'s `to` as the child.
- *`two`'s start blocks.* USDC's deploy block is four million blocks before the pool's, which no
  recording carries; at mainnet's 20-block window it would be 196,000 `getLogs` a run. The shape is
  the scaffold's nest with both contracts starting at the pool's block, and says so.
- *What the shapes do not run.* The tests' own windows and spans (`spawn_nest`'s overrides): both
  halves run each chain's registered policy, so they cut the same segments. The follower's offchain
  entity: an embedding binds no offchain tables (§13). `reorg_below_finality_halts`: a node and a
  module both stop, and comparing two halts needs a shape of its own.
- *The wasmtime host* answers no `verified_finalized`; a non-browser host brings its own light client.

**Not done, and why.**

- *Safari and iPhone.* Waiting on Chief: `safaridriver --enable` needs an administrator, and no run
  was attempted without it.
- *The address build's size gate.* Deferred with the address build (RFC-0074 S2's decisions).
- *MCP.* Deferred, as §4 says.

**The binary.** Unchanged in behaviour. Two files changed, `src/analytics.rs` and
`src/engine_burrmill.rs`, each calling `nuthatch_core::bind` for what it built in place, with the same
strings. No `Cargo.toml` or `Cargo.lock` of the binary changed. Every subcommand's `--help`, two levels
deep (60 pages), and the files `init --abi` writes for mainnet and Arbitrum One are byte-identical
between this branch and the same tree with main's two files. The whole suite: 2,879 passed, none failed,
18 ignored; the one new test is `e2e_embed_shapes`. A first run, beside a module build, lost
`deploy_nest_script::a_failure_only_the_new_binary_has_reverts_after_asking_the_previous_one` to a
killed script (exit `None`); it passed alone and in the whole suite again.
