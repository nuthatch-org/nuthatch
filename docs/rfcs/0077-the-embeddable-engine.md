# RFC-0077: The embeddable engine - nuthatch's core as an opt-in wasm module, beside the binary

- Status: **Accepted, 2026-10-30.** S0 to S5 and S7 (the Lodestar demo) authorised in order; S6 waits for rotki to ask.
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
- **S7, the demo: Lodestar's `/profile`, verified locally.** Chief, 2026-10-30. Lodestar's network-wide
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
