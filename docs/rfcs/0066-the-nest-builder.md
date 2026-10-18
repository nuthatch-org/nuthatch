# RFC-0066: The nest builder - a nest defined, then indexed, in one browser tab

- Status: **Accepted 2026-10-18, through S2.** S0 to S2 built (§12). Chief: "a team defines a nest in some sort of 'nest builder' ui page
  ... and then when they click 'index it' the nest starts indexing right there in the browser."
- Author: Pete (cargopete)
- Date: 2026-10-18
- Depends on: RFC-0065 (the nest in a tab), and its S6 in particular, the backfill in the tab, which
  is the engine "index it" runs.
- Blocks: nothing.
- Nature: a separate artefact, a static page in `burrmill-wasm`, plus one refactor in this tree (§4):
  `init`'s pure parts and the nest's identity move into crates that build for wasm, as `nuthatch-seal`
  and `nuthatch-decode` did for RFC-0065 S2.

## 1. What it is for

A hackathon team has a contract deployed this week and wants a live, queryable index of it before
the judging. Today that is `curl | sh`, `nuthatch init`, `nuthatch dev` and somewhere to run it. The
builder is the same three steps on one page, with nothing to install and nothing for us to host:
paste an address, look at the tables and the starter view the page generates, edit the views, press
**Index it**. The tab backfills from the deploy block, follows the head, and answers SQL over both,
with the same decoder and sealer a node uses (RFC-0065).

It is a demo of the product's own claim, "be your own indexer", made literal.

## 2. What exists, read 2026-10-18 at `origin/main`

- **`init` is mostly pure, but not separable today.** `project::init` (`src/project.rs:13`) mixes three
  kinds of work in the same functions. RPC calls: the chain id, proxy slots
  (`resolve_implementation`, `:1274`), the deploy block (`:1741`) and a log sample (`check_abi_fits`,
  `:1353`). The ABI fetch: Sourcify v2, then Blockscout, then Etherscan v2 (`src/abi.rs:27`). And
  `std::fs::write` for each file. What decides the files' contents is pure: the alias, the `Config`
  literal (`:130-160`), `registry.schema()`, `semantic::generate`, the starter view, llms.txt and
  SKILL.md. All of it sits in the main crate, which depends on tokio, reqwest, redb and `std::fs`
  (`Cargo.toml:103-104`) and does not build for wasm.
- **The identity is in the main crate too.** The NID (`Manifest::nid`, `src/blob.rs:102`) and the
  data identity (`Manifest::data_identity`, `:352`) hash the nest directory's files, canonicalised by
  `build_manifest` (`:235`). Only `DecodeRegistry::hash` (`decode/src/registry.rs:1064`) is already in
  a wasm-checked crate.
- **The MCP server cannot do this.** It is a bridge to a running `nuthatch dev`, and its tools
  (`src/mcp.rs:322`) query: status, schema, tables, sql, explain, entity. Nothing in it scaffolds or
  validates a nest. The builder needs the code under `init`, not the MCP.
- **The browser can reach what `init` reaches.** Sourcify's v2 API answers any origin
  (`access-control-allow-origin: *`, checked 2026-10-18). The RPCs RFC-0065 §5 lists answer
  `eth_getStorageAt`, `eth_call` and `eth_getLogs` from a page. Etherscan needs a key, so it is the
  visitor's own or it is skipped.
- **Validation at load is separable in principle.** `Config::load` (`src/config.rs:565-630`) only
  parses and checks TOML. View validation needs the engine, which the tab already has.

## 3. The design

The page does the I/O; Rust decides the bytes. A step that writes a file natively returns its
contents in wasm, and the page keeps the nest directory as a map of path to bytes, in OPFS.

1. **Address and chain.** The page reads the chain id from the RPC, resolves a proxy from its slots,
   and fetches the ABI from Sourcify (or from an Etherscan-class API with the visitor's key, or from a
   pasted file). The deploy block comes from Sourcify's `deployment.blockNumber` where it has one:
   `init`'s own search reads old state that public RPCs do not serve, and on 2026-10-18 it found
   nothing through publicnode for a pair deployed the day before. Each answer goes into the same parsers `init` uses.
2. **Scaffold.** `nuthatch-scaffold` (§4) turns (ABI, address, chain, deploy block, options) into the
   files `nuthatch init` writes: `nuthatch.toml`, `abis/`, `schema.json`, `views/`, `semantic.toml`,
   llms.txt and the skill. The page shows the tables and the starter view.
3. **Edit.** Views are SQL in a text box, checked by the tab's Burrmill as they are typed. More
   contracts can be added, each going through 1 and 2. The NID is shown as it changes.
4. **Index it.** RFC-0065 S6: backfill from the deploy block, seal, follow the head. The views answer
   as rows arrive.
5. **Keep it.** Download the directory as a zip that `nuthatch dev --dir` runs unchanged, or commit it
   to a GitHub repository (§5). Either way a node indexing the same directory computes the same NID
   and seals the same segments, which is what lets a team move from the tab to a real deployment
   without starting again.

## 4. The refactor in this tree

Two crates, both added to the wasm `cargo check` that already covers `nuthatch-decode` and
`nuthatch-seal`:

- **`nuthatch-scaffold`**: the pure content of `init`, as functions from inputs to file contents.
  `project::init` keeps its I/O and calls them, so the binary's `init` writes what it wrote before.
- **`nuthatch-identity`**: `build_manifest`'s canonicalisation, the NID and the data identity, over a
  map of path to bytes rather than a directory walk. `blob.rs` calls it with the files it read.

The gate for both is RFC-0065 S2's: native output byte-identical before and after the move.

## 5. GitHub

"Connect with GitHub" in the usual sense is an OAuth web flow. Its last step exchanges a code for a
token using the app's client secret, which has to sit on a server. That is a hosted component, and
the out-of-scope list in `CLAUDE.md` rules out running one. Three ways that stay inside the line:

- **A zip** that the team commits themselves. No GitHub at all, and it always works.
- **A fine-grained token the visitor pastes**, scoped to one repository, kept in that browser and
  sent only to `api.github.com`, which answers browsers (CORS) for its REST API. The page creates the
  commit through the git data API. Nothing of ours sees the token.
- **The device flow**, which needs no secret. Unverified: whether its endpoints on `github.com` answer
  a page's cross-origin request. If they do not, it needs a proxy, and it falls with the web flow.

The proposal is the first two. The web flow is not built.

**Settled 2026-10-18, Chief: "we dont need a full connect iwth github".** The zip is how a nest leaves the
tab; the team commits it themselves. S3 is not built unless someone asks for it.

## 6. AI

A model can draft a view from a sentence, and that is useful here. Non-negotiable 3 allows a local
model or the visitor's own key, never one of ours, and the builder must be complete without it. Its
output is authored SQL that the visitor reads and saves. Like any view, it is code, so nothing a
model writes reaches stored state (non-negotiable 4).

## 7. Non-goals

- A hosted builder with accounts, saved projects on our side, or a backend of any kind.
- Publishing a tab-built nest to the public mirror from the tab. A mirror write needs credentials we
  hold; a team that wants a mirror runs a node with `--publish-target`, as every other nest does.
- Factories, entities, calls and IPFS in the first version: `init`'s single-contract path first.
- Chains whose public RPCs do not answer a page (RFC-0065 §5).

## 8. Against the non-negotiables

- **Single static binary.** The binary gains no feature; §4 moves code across crate boundaries, gated
  on identical output.
- **≤2 GB.** The tab is not the CI budget. S2 records the tab's peak memory for its backfill.
- **No phone-home.** The page talks to the RPC and the ABI source the visitor chose, and to GitHub
  only if they opt in. Nothing else.
- **Determinism.** A tab-built nest must hash to the NID `nuthatch init` gives for the same inputs,
  and seal the same segments.
- **RFC-0044 §8.** S0 is a refactor with a byte-identical gate. S1 onwards is a new way to create a
  nest, and needs Chief's line in `CLAUDE.md` before it starts.

## 9. Slices and their gates

- **S0, `init` and the identity in wasm.** The two crates of §4. Gates: for USDC, a proxy and a
  contract with no Sourcify match, `nuthatch init` writes byte-identical files before and after the
  move, and a wasm test build gives the same files and the same NID from the same inputs.
- **S1, the builder page.** Address to scaffolded nest, editable views, the NID shown, a zip out.
  Gate: the zip, run with `nuthatch dev --dir`, reports the NID the page showed.
- **S2, Index it.** RFC-0065 S6 driven from the page. Gate: for a contract deployed in the last
  week, every segment the tab seals equals the one `nuthatch dev` seals from the downloaded zip, with
  time, bytes and peak memory recorded.
- **S3, to GitHub.** The token path of §5. Gate: the commit's tree is the zip's, file for file. Not
  built: the zip is enough (§5).
- **S4, a drafted view.** BYO key or a local model; off by default.

## 10. Risks

- `init` has grown its I/O into its pure parts over 66 RFCs; S0 may find more coupling than §2 saw.
- A young contract is the only kind a public RPC will backfill quickly. A team indexing USDC in a tab
  will wait, and the page must say so before it starts, not after.
- The tab's storage is per browser and Safari evicts it after 7 days. The zip is the durable copy.

## 11. Open questions

1. Whether the builder lives at `/play` beside the published nests, or on its own page.
2. Whether S3's token path is worth building before a team asks for it, given the zip. **Answered:
   no** (§5).

## 12. Measured outcome

Recorded 2026-10-18, against nuthatch 5.7.1 and burrmill-wasm at `c7ce789`.

- **S0** (#2097, #2098, #2100). `nuthatch init` on USDC, a UniswapV2 pair, CryptoPunks and a
  two-contract nest with a local ABI and no timestamps wrote byte-identical files before and after the
  move. The wasm module, run under Node, gives the pair the NID `nuthatch dev` reports for it.
- **S1** (burrmill-wasm #4), live at nuthatch-indexer.com/play/build.html. `bench/builder-gate.mjs`
  resolves live, zips, unzips and compares: USDC (a proxy), the pair (its deploy block from Sourcify)
  and a two-contract nest each match `init` byte for byte, and `dev` reports the page's NID. In Chrome,
  the zip the page downloaded with an edited view ran under `dev`, which reported the NID the page
  showed, and the view answered.
- **S2** (burrmill-wasm #5). `bench/index-gate.mjs` runs `dev` on a builder nest to the finalized head,
  then seals the tab to the same block: for the pair deployed on 2026-10-17, 6 of 6 segments hash to the
  node's, 154 logs in 3.8 s at 133 MiB peak RSS. On the live page it indexed in 3.9 s and answered a
  count in 250 ms.
- **Full segments.** The same gate on groklet (`0x787873c9…8adc`, deployed at block 26,211,195), with
  GraphOps as the first RPC and Alchemy as the second: 13 of 13 segments hash to the node's, all of them
  full (4,199 transfers and 2,401 approvals among them), 6,604 logs in 13.7 s at 232 MiB peak RSS.
- **Public RPC history.** Probed with USDC logs 5,000, 20,000 and 60,000 blocks back: publicnode answers
  only the first without a personal token, and flashbots answers the older two with nothing. The page now
  defaults to mevblocker and drpc, which answer all three (burrmill-wasm #6), and the groklet gate passes
  on those two alone: 13 of 13, in 29.7 s. A contract older than about eight days needs a full-history
  RPC in either box.
- **Base and Arbitrum One** (burrmill-wasm #9). The page carries each chain's seal span from the registry
  (10,800 and 86,400 blocks). Gated against `nuthatch dev`: a four-day-old Base pair, 34 of 34 full
  segments, 93,904 logs in 385 s at 361 MiB peak; GMX on Arbitrum from 100,000 blocks back, 2 of 2 full.
  Base's keyless RPCs are thin: the one second that passed a browser's CORS preflight allows 50 blocks a
  log query. nodies answers the preflight with no CORS header, so browsers cannot use it.
- **Rows before their span seals** (burrmill-wasm #10). On Arbitrum nothing seals until 86,400 blocks
  complete, and the page first answered 0 for 569 indexed logs. It now serves the agreed rows the
  backfill has not sealed, and the head loop fetches only blocks past them.
- **A memory budget** (burrmill-wasm #8). The heaviest Lodestar view peaks near 2 GiB, which kills a
  phone tab, and watching memory from the page cannot see it: the engine computes for seconds without
  yielding. The engine now holds a budget and refuses a statement over it: 600 MiB on phones, and elsewhere
  2 GiB, nuthatch's budget per active-chain cursor, which a tab following a chain is (burrmill-wasm
  #12). Refused at 600 MiB, the module stood at 836 MiB and the tab went on answering; under 2 GiB the
  heaviest Lodestar view completes with the module at 1,956 MiB.
- **Views are planned as they are typed**, against the declared tables with no data, so the engine's own
  error shows before anything is indexed.
