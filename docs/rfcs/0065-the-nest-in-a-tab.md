# RFC-0065: The nest in a tab - a published nest queried, then kept current, in a browser

- Status: **Accepted 2026-10-17, through S6.** S0 to S5 shipped by 2026-10-18 (S5 in
  burrmill-wasm#2); S6, added 2026-10-18, is not started. Measured outcome in §12. Chief, on the draft: "I like it. Save the plan as an RFC."
- Author: Pete (cargopete)
- Date: 2026-10-16
- Depends on: RFC-0052 (the mirrored nest: `manifest.json`, `schema.json`, content-addressed
  segments on R2), RFC-0047 (the sealed directory as a contract), Burrmill's `pete/wasm` branch and
  the `burrmill-wasm` repository.
- Blocks: nothing.
- Nature: a separate artefact, not a change to the binary. §8 says why it needs no RFC-0044 §8
  record until S2, and what that record would be.

## 1. Where this comes from

`burrmill-wasm` compiles Burrmill to `wasm32-unknown-unknown`. A static page fetches a published
nest's segments over HTTP, checks each against its content address, and answers the nest's authored
views in the tab. On the Lodestar nest (4,745 segments, 6.0 M rows, 500 MB) all 23 views were
row-identical to native Burrmill. Against DuckDB-Wasm 1.33.1 in Node, over the 22 views both
answered: 43.6 s cold and 32.0 s warm, against 158.8 s and 76.1 s. `lodestar_indexer_ledger` had no
answer from DuckDB after an hour and took burrmill-wasm 4.9 s (`burrmill-wasm/README.md`,
`bench/*.jsonl`).

That is a fast reader, not a new kind of thing: DuckDB-Wasm, Observable Framework and Evidence all
put an engine over static Parquet in a page. The question this RFC answers is what nuthatch can do
in a tab that they cannot. A research pass on 2026-10-16 looked for prior art (§2) and checked
which of nuthatch builds for wasm (§3). Its answer, adopted here: **a nest that is both serverless
and current.** History from the mirror, verified by hash; the blocks since the last seal fetched
and decoded in the tab by nuthatch's own decoder; one SQL surface over both.

The Lodestar nest no longer runs. The demo nest is chosen in S0 (§9).

## 2. Prior art

Searched 2026-10-16 for in-browser, OPFS/IndexedDB, Cloudflare Workers, WASI/Spin and
light-client-backed Ethereum indexers. Nothing found does ingest, decode, store and query in wasm.
That is a statement about one search, and this RFC does not claim "first".

| Project | What it is | What it lacks against this RFC |
|---|---|---|
| etherfold (wighawag, npm 0.11.0, 2026-09-30) | TypeScript indexer designed to run in-browser over EIP-1193, IndexedDB store, reorg detection | No SQL in the browser, no columnar store, no wasm, no published history to start from |
| LogEx (tdenisenko) | Rust, native; joins CL+EL p2p, rebuilds each block's receipt trie against `receiptsRoot` | Native only, no release |
| DuckDB-Wasm, Observable, Evidence, datafusion-wasm | Engines over static Parquet in a page | No ingest; no content-address check; the §1 numbers |
| graph-node, Substreams, Subsquid, Envio, Ponder | Run user handlers in wasm or JS | The indexer itself is a server |
| Helios (`@a16z/helios` 0.12.0) | Ethereum light client in wasm | A verifier, not an indexer; see §6 |

## 3. What builds for wasm today

Checked 2026-10-16 with `cargo check --locked`, both `wasm32-unknown-unknown` and `wasm32-wasip2`:

- `nuthatch-decode` (`decode/`): clean, unchanged. Its split for #581 is what made this possible.
- A scratch crate of `nuthatch-decode` + arrow/parquet 59.3.1 `ArrowWriter` (snap, zstd) + sha2 +
  alloy-consensus `calculate_receipt_root`: clean. Decode, seal bytes, content hash and
  receipts-root re-derivation all compile.
- `src/seal.rs`: the pure core (`sort_rows_for_seal`, `rows_to_batch`, `write_parquet`, the hash and
  the manifest entry) is about 120 lines. The rest is filesystem, `tokio::sync::watch` and coupling
  to `registry`, `runtime`, `store`, `prune` and `crash`. `src/lib.rs` has no `cfg(target_arch)`
  gating, so the core needs a crate boundary, as decode got one.
- Not portable and not wanted in a tab: `src/rpc.rs` (reqwest + tokio, tangled with axum test
  servers), redb, the reorg path in `src/indexer.rs`, entities (dbsp), axum/hyper.

Trap: on this machine Homebrew's `rustc` precedes rustup's on `PATH`, and a wasm check then fails
with "can't find crate core".

## 4. The design

Three layers in one page, each usable alone.

**The reader (exists).** Burrmill in a Web Worker binds the nest from the mirror as nuthatch binds
it: only the tables a statement reaches, through the views it names (#896). Each segment is fetched
whole, checked against the SHA-256 its key names, and kept in the origin-private file system, so a
second visit fetches nothing that has not changed.

**The tail.** The mirror's manifest says which blocks are sealed. The worker fetches logs from the
next block to the head from a browser-reachable RPC, decodes them with `nuthatch-decode` compiled to
wasm against the nest's own contracts and ABIs, and holds the rows in memory as an unsealed table.
A view is answered over sealed segments and the tail together, so it reads to the head.

**Sealing in the tab.** Below finality (about 13 minutes on mainnet) tail rows are sealed with the
extracted seal core into Parquet in OPFS, under their content address. Sealing only below finality
means the tab needs no rollback: a reorg can only touch unsealed rows, which are refetched. Because
the core is deterministic, a range the tab seals must hash to exactly what native nuthatch seals for
it. That equality is the test that the tab is running nuthatch and not an imitation of it.

**Where finality comes from.** "Below finality" is only as good as the source that says a block is
final, and a public RPC's word is not authenticated. Before S5 the tab seals a range only when two
independent RPCs both report its end at or below their `finalized` block, with the same hash for
every block in the range and its parent chaining to the last sealed block; a disagreement, a missing
`finalized` tag, or a hash that differs withholds the seal and the rows stay in the tail. Segments
sealed this way are marked in OPFS and on the page as sealed against unauthenticated finality. From
S5, Helios's beacon light client verifies the finalized checkpoint through the sync committee, so the
boundary comes from consensus rather than from an RPC, and the mark is dropped. Either way a tab
segment whose hash later disagrees with the mirror's for the same range is discarded and refetched.

## 5. Browser constraints, measured 2026-10-16

| RPC (mainnet) | CORS | `eth_getLogs` |
|---|---|---|
| rpc.flashbots.net | `*` | 20,000-block range answered; some old ranges answer 4444 "pruned history" |
| ethereum-rpc.publicnode.com | `*` | 9,000 blocks near head; old blocks want a token |
| cloudflare-eth.com | `*` | 800-block cap |
| eth.drpc.org, 1rpc.io | yes | 100 and 50 blocks |

The tail is therefore short by design: from the mirror's last seal to the head, which for a nest
that seals continuously is minutes to hours of blocks, well inside a 9,000-block call. History never
comes from a public RPC.

Storage: Chromium allows up to 60% of disk per origin, Firefox min(10% of disk, 10 GiB) best-effort,
Safari evicts an origin unused for 7 days unless `navigator.storage.persist()` is granted. Memory:
4 GiB wasm32 in Chrome and Firefox; Memory64 is not in Safari. `lodestar_indexer_ledger` already
peaked at 963 MiB. Chrome discards inactive tabs and throttles hidden timers, so the tail is
resumable from the manifest and OPFS, never from in-memory state alone.

## 6. Verifying the head

Helios, in RPC mode, takes logs from the untrusted RPC, fetches `eth_getBlockReceipts` for each
block in the result, recomputes the receipts root against a beacon-verified header, and checks each
log against its receipt. Two limits, both stated rather than worked around:

- **Completeness is not verified.** Nothing in Helios uses the bloom, and EIP-7745 calls the mainnet
  bloom practically useless; an RPC that omits a log passes. A block's logs are complete only if the
  tab checks all of that block's receipts, about 0.9 MB a block, which is affordable for a tail of
  hours and not for following the head all day. *(Measured 2026-10-18, §12: 1.17 MB a block on
  DeFi Pulse, and 881 of a seal span's 1,800 blocks carry rows, so even inclusion for one span is
  about 1 GB. Neither is affordable as a default.)*
- **Header provenance reaches back about 27 hours** (EIP-2935's 8,191 hashes). Older blocks have no
  path in Helios, and EIP-4444 expiry is live.

So S5 claims exactly this: every row in the tail is shown to be in the chain, and, where the tab
checked all receipts, that no row of that block is missing. Sealed history stays trusted by its hash
and by whoever sealed it, which is no change from today.

## 7. Non-goals

- **Not the WASM transform layer.** Chief removed it on 2026-10-08 (#1999). This RFC runs
  nuthatch's own decoder and engine in a browser; it adds no handler layer, no components and no
  wasmtime, and nothing here touches the binary's derivation path.
- ~~Full history ingest in a tab. History comes from a mirror.~~ S6 backfills a young contract in the
  tab (Chief, 2026-10-18), for RFC-0066's nest builder. A mature nest's history still comes from a mirror.
- Cloudflare Workers or other edge runtimes. 128 MB per isolate rules out Burrmill; a decode-and-seal
  Worker would compile but the native binary does that job more cheaply.
- Publishing what a tab seals. A tab-sealed segment is a local cache that happens to be
  byte-identical to the published one.
- Entities, folds, maintained views in the tab. Authored SQL views only, at first.
- A verified completeness claim for sealed history.

## 8. Against the non-negotiables

- **Single static binary.** Untouched. The page is a separate artefact; the binary gains nothing
  until S2, which only moves code across a crate boundary.
- **≤2 GB.** The tab is not the CI budget, but S1 records peak wasm memory for every view, and a
  view over 2 GiB is a finding, not a pass.
- **Determinism.** S4's gate is hash equality with native sealing; it is the strongest form this
  rule can take.
- **No phone-home.** The page talks to the mirror and the RPC the visitor chose. Nothing else.
- **RFC-0044 §8.** S0 to S1 need no record: they change nothing in the tree. S2 is a refactor with a
  byte-identical gate. S3 onwards is a new way of running nuthatch's core and needs Chief's line in
  `CLAUDE.md` before it starts.

## 9. Slices and their gates

- **S0, the demo nest, which can stop the programme.** Pick a published nest that still seals, on a
  chain with a CORS-open RPC from §5, with authored views worth looking at. Add a GET-only CORS rule
  on its R2 prefix. Gate: every authored view answers in Chrome from the mirror, row-identical to
  native Burrmill. If no live nest has views that answer in a tab, stop.
- **S1, the zero-server page.** The `burrmill-wasm` playground pointed at the demo nest, served
  statically. Gates: a second visit makes zero segment requests; cold and warm times for every view
  in Chrome, over R2, beside DuckDB-Wasm on the same page, published with the losses.
- **S2, `nuthatch-seal`.** The pure seal core moved into its own crate, and `cargo check` for both
  wasm targets on `nuthatch-decode` and `nuthatch-seal` added to CI. Gate: native sealing of a
  pinned range produces the same segment hashes before and after the move.
- **S3, the live tail, decode only.** The worker fetches `sealed_through..head`, decodes it in wasm,
  and answers views over sealed and tail rows. Gate: once native nuthatch seals a range the tab
  decoded, the tab's rows for that range equal the sealed rows, as multisets of nuthatch-encoded rows.
- **S4, sealing in the tab.** Below finality, to OPFS, resumed after a discard. Gates: for every range
  the tab sealed, its segment hash equals the hash on the mirror; against a scripted RPC pair, a range
  one RPC reports finalized and the other does not, or reports with a different block hash, or that is
  reorged before the second RPC finalizes it, is never sealed and stays in the tail.
- **S5, the verified head.** Helios in the same worker. Gates: a `finalized` block the light client
  cannot verify is not sealed below; a log altered in the RPC response is refused; for blocks marked complete, a log removed from the response is detected; peak memory with
  Helios and Burrmill together recorded. The page says which blocks are complete and which are only
  included.
- **S6, a backfill in the tab.** A nest with no published history, indexed from its deploy block in the
  tab: the live tail's fetch over a longer range, sealed by the same sealer. History older than
  EIP-2935's 8,191 blocks is sealed on two RPCs agreeing, and the page says so. Gate: for a real
  contract deployed in the last week, every segment the tab seals equals the one native `nuthatch
  dev` seals for the same range, with the time and bytes it took recorded. A contract whose history
  public RPCs will not serve is a finding, not a pass.

## 10. Risks

- The demo nest's views may be heavier than Lodestar's, or lighter and uninteresting. S0 is there to
  find out.
- Public RPC caps change without notice; drpc's error message already misreports its own limit. The
  page lets the visitor supply an RPC URL.
- Tab lifetime. A discarded tab loses its tail; S4 makes the loss a refetch, not a gap.
- ~~Helios and Burrmill together in one wasm32 address space are unmeasured.~~ Measured, §12.
- Safari: no Memory64, aggressive eviction. Supported, but the numbers are published per browser.

## 11. Open questions

1. Which nest. Candidates are whatever is on the mirror and still sealing on 2026-10-16; S0 decides.
2. Whether `burrmill-wasm` moves under nuthatch-org, and whether Burrmill's `pete/wasm` changes are
   merged upstream behind a feature or kept on a branch.
3. ~~Whether S5 checks all receipts for every tail block by default, at about 0.9 MB a block, or only
   on request.~~ On request, 20 blocks at a time (Chief, 2026-10-18); §12 has the figures.
4. ~~Whether the page should accept any mirror URL, making it a client for every published nest
   rather than a demo of one.~~ No: a curated list of our nests, the picker `nests.json` already
   feeds, with DeFi Pulse the one that follows the head (Chief, 2026-10-18).

## 12. Measured outcome, 2026-10-18

**S4.** The tab's sealer, resumed from the mirror's seed at block 26,213,187, sealed 26,213,188 to
26,214,987 from publicnode, and all 8 segments hash to the ones DeFi Pulse published on 5.7.1, with no
native segment missed. Shipped to nuthatch-indexer.com/play in burrmill-wasm#1.

**S5 does less with Helios than §6 planned, and checks more itself.** Helios's own verified
`eth_getLogs` proves each block through `eth_getProof` against older state, which drpc serves,
publicnode only for the last few blocks and flashbots not at all ("exceeds maximum proof window"). So the tab takes one thing from
Helios, the finalized block hash, which the consensus endpoint can withhold but not forge. Every
header from the last seal up to it is hashed in wasm (alloy-consensus) and must link to it by parent
hash, and every sealed log must name a block hash on that chain. The bloom does not help: 1,337 of
1,550 recent blocks match DeFi Pulse's filter.

- **Logs are agreed, not proven, by default.** Both RPCs must still return the same logs before a
  seal. Proving inclusion for one span would mean about 1 GB of receipts.
- **Complete on request.** The page checks the 20 newest finalized tail blocks with rows: their
  receipts must hash to the verified header, and the block's matching logs must equal the RPC's.
  In Chrome, 20 of 20 came back complete from 14.3 MB of receipts in 1.7 s.
- **Gates.** 12 scripted cases in `bench/verified-test.mjs`: an unverifiable finalized block, a
  forged or missing header, a log off the chain, and a log altered, added or left out against real
  receipts. Mutating either check turns a case red. Live, from the same seed: 5,260 headers hash
  down to Helios's block 26,218,447, and the 8 segments still equal the mirror's.
- **In a browser.** Chrome, resumed from the same seed against a local copy of the mirror frozen at
  it, sealed 21 segments through block 26,218,587 below Helios's block 26,218,606. Pointed at the live
  mirror once the nest had sealed that far, the tab found all 21 published with the same hashes.
  Shipped in burrmill-wasm#2.
- **Memory.** One sample each in Chrome, by `measureUserAgentSpecificMemory`: 261 MiB for the tab
  with Helios, against 250 without it. Helios's own wasm memory is 2 to 3 MiB.
- **Cost.** Public RPCs serve about 26 headers a second, so a seal span's headers take about 70 s.
  They are fetched only when a seal would follow, shared across both RPCs, and the tail shows first.
