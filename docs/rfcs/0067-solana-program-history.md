# RFC-0067: Solana program history - one program's decoded, finalized history as sealed Parquet

**Status:** **Accepted 2026-10-21 (Chief)**, on one condition that binds every slice: all of it ships
behind the `solana` cargo feature, **off by default**, and a regular nuthatch user never sees it
unless they build for it (§9). It is the first non-EVM chain, decided under CLAUDE.md's 2026-10-21
amendment ("Non evm chains could be supported upon user request"), which makes each such chain
Chief's call. No named user has asked yet, so the demand test in §10 has to find one before S2.
Decoding is IDL-only; there are no WASM decoders (§6). S0 can stop the programme.

**Date:** 2026-10-21

**Author:** Pete (cargopete)

**Depends on:** RFC-0021 (one cursor per chain), RFC-0046 §1 (the deletion test), Burrmill (4.1).

**Blocks:** nothing.

## 1. Abstract

Give a Solana nest a program ID and an IDL; it backfills that program's transactions, follows the
tip, seals only finalized history into slot-range Parquet segments aligned to epochs, and serves it
through `/sql` and MCP like any other nest. The unit is a program, never the chain. Account-state
history for that program comes later, forward from the day the nest starts.

## 2. Motivation

The research brief Chief shared on 2026-10-21 ("Nuthatch on Solana: Landscape, Build Plan, and
Go-to-Market") surveyed the market. Its
claims are the brief's, not measurements of ours; S0 checks the ones the design rests on.

- Hosted providers now cover whole-chain history well: Triton's Superbank, Helius and Triton's
  `getTransactionsForAddress`, Alchemy's Account Archive for account state since July 2025.
- What is poorly served is a small team wanting **its own program's** decoded instruction, event and
  account history as files it owns, on a laptop or a small VPS. Carbon is a library to assemble,
  Jetstreamer a backfill engine, solarchive.org undecoded and chain-wide.
- That is nuthatch's existing shape: one binary, sealed content-addressed Parquet, SQL and MCP over it.

The brief's own warning stands at the top of this RFC: the EVM product shipped polished releases
without users. Every slice after S1 is gated on evidence of demand, not on the previous slice working.

## 3. Goals

1. A Solana nest is declared by its scope: program IDs, optional extra accounts, a start slot.
2. Sealed history is finalized-only and provably complete: every slot in a segment's range is a
   block or a slot proved skipped on finalized evidence (§5).
3. Decoding is deterministic from an IDL in effect at that slot; raw rows are always kept, so a
   better IDL re-decodes without re-fetching.
4. A default build is byte-for-byte unaffected (§9).

## 4. Non-goals

- **The whole chain.** At roughly 500 GB per recent epoch (the brief's figure), "everything" is a
  data-centre project. A nest without a program scope is refused.
- **WASM decoders, or any user code in the decode path.** The transform layer was removed on
  2026-10-08. A program without a usable IDL is served raw.
- **Compressed state** (Bubblegum cNFTs, ZK-compressed accounts). Out of v1.
- **Code from AGPL projects.** Old Faithful's tooling and Superbank are AGPL: read for ideas only,
  as with SQD's worker-rs. Jetstreamer is MIT/Apache and may be depended on, behind its own feature.
- **Historical account state before the nest started.** Neither RPC nor Old Faithful holds it.
- **A hosted service, a public corpus run by us, or a price.** The out-of-scope list is unchanged.
- **Sub-second tip latency.** Finalized-only is the contract.

## 5. Model

**Cursor.** One cursor per Solana cluster, single-writer, its own failure boundary. The single-cursor
law holds: a Solana cursor never shares a block stream with an EVM one. A runtime may host both only
as separate cursors, each inside its own 2 GB budget.

**Time axis.** Slot, not block height. Skipped slots are stored explicitly (`blocks.skipped`), so a
segment can prove it is complete. `block_height` and `block_time` are attributes.

**What proves a skip.** A slot is recorded as skipped only on finalized evidence: it is at or below
the finalized slot, `getBlocks` at `finalized` does not list it, and the parent links agree, meaning
the next produced block's `parentSlot` is the previous produced slot, jumping over it. A block an RPC
cannot return (not available, missing from long-term storage) is *unavailable*, not skipped, and a
segment holding an unavailable slot does not seal until it is fetched. S0 records which RPC errors
mean which, on each provider used.

**Finality.** Rows enter the hot store (redb) at confirmed or finalized, and only finalized slots are
sealed. Alpenglow replaced TowerBFT on mainnet on 2026-10-21; Anza states finality fell to about
150 ms. That figure is not ours: S0 measures the finalized lag on whatever RPC we use. Nothing about
the seal depends on it being small, only on it being finalized.

**Reorgs.** A confirmed slot that does not finalize is retracted from hot. Sealed segments are never
touched, exactly as on EVM.

**Segments.** Slot ranges aligned to epochs (432,000 slots): 7,200-slot segments, sixty to an epoch,
with a coarser 43,200 option for sparse nests. A manifest records the range, the skipped-slot
bitmap, row counts, schema version, the IDL hash per program and the source.

**Identity.** The scope (cluster, program IDs, extra accounts, start slot) and each IDL's hash are
part of the NID, as an ABI is today; any change is a different nest.

**Tables** (all carry `slot` and `block_time`, sorted by `slot, tx_index`):

| Table | Holds |
|---|---|
| `blocks` | slot, parent slot, height, blockhash, time, skipped |
| `transactions` | signature, fee, compute units, error, signer, version, log truncation flag |
| `instructions` | `(slot, tx_index, ix_index, inner_index)` with `inner_index = -1` for outer; program, accounts, data, decoded name |
| `logs` | per line, with program and depth |
| `token_balance_deltas` | pre and post token balances, mint, owner, Token or Token-2022 |
| `account_writes` | (S4) pubkey, owner, lamports, data, data hash, `closed` |
| `ix_<program>_<name>`, `ev_<program>_<name>`, `acct_<program>_<type>` | typed columns from the IDL |

Account keys are always resolved in full before decoding: static keys plus the addresses loaded from
address lookup tables, with `maxSupportedTransactionVersion: 0`.

## 6. Decoding

- Anchor IDLs (0.30+, with legacy IDLs converted) and Codama IDLs are parsed into a type tree and
  Borsh is decoded straight into Arrow builders. No codegen, no compile step for the user.
- Discriminators: Anchor's 8-byte sighash and 0.31's custom discriminators.
- Events are read from Anchor's self-CPI event instructions where present, which are authoritative;
  `Program data:` log lines are a fallback, flagged when logs were truncated.
- **IDLs are versioned by slot.** Programs upgrade, and a stale IDL decodes the wrong bytes without an
  error. The nest records each program's deployment slots and decodes with the IDL in effect; an IDL
  the operator supplies names the slot range it covers.
- A decode-failure rate is kept per segment. Above a threshold, the decoded tables of that segment
  are not sealed; the raw tables still are.
- Decoding is pure: the same bytes and the same IDL give the same rows. No model is in this path.

## 7. Ingestion

| Source | Role | Slice |
|---|---|---|
| JSON-RPC (`getSignaturesForAddress` + `getTransaction`, or `getBlock` for dense programs) | backfill and gap fill | S0 |
| Yellowstone gRPC, finalized, filtered to the program | tip | S1 |
| Old Faithful epoch CARs | backfill of dense programs | S3, behind a second feature |

Dependencies stay light: the component Solana crates (`solana-pubkey`, `solana-signature`,
`solana-message`, the SPL interface crates), not `solana-sdk` or `solana-client`, and our own small
JSON-RPC client on `reqwest`. Yellowstone's client needs `protoc` at build time, which is acceptable
only because the feature is off by default. Jetstreamer pulls in RocksDB and a Clang 16 build; if S3
goes ahead it sits behind `solana-old-faithful`, or we write a minimal CAR reader of our own.

## 8. Serving

The same surfaces as an EVM nest: `/sql` over hot and sealed through Burrmill, authored
`views/*.sql`, MCP's schema, SQL and entity tools. An as-of-slot account lookup arrives with S4 as a
view over `account_writes`, not a new engine. No Solana JSON-RPC facade is in scope.

## 9. Packaging, and the deletion test

Everything lives behind `solana`, off by default. The test, RFC-0046 §1's: a default build's CLI help,
the config `init` writes, its on-disk layout and its behaviour are what they would be had this RFC
not been written. A default build given a Solana nest refuses it by name ("this nest needs a build
with `--features solana`"), as a default build refuses `folds/` today. The default build's dependency
tree gains nothing; `cargo tree` with and without the feature is checked in CI.

## 10. Slices and gates

- **S0, the spike, which can stop the programme.** Behind the feature: one well-known Anchor program
  backfilled over RPC for a bounded slot range (about one epoch), raw tables sealed into epoch-aligned
  segments, queried through Burrmill. Gates: every slot in range accounted for as a block or a
  skip proved on finalized evidence (§5), with an unavailable block refusing the seal; the
  program's transactions over the range match an independent count (a second RPC, or solarchive.org's daily Parquet); RSS inside 2 GB; the finalized lag after Alpenglow and
  the RPC calls and wall time recorded. A default build's `cargo tree` and help are unchanged.
- **S1, the tip.** Yellowstone at finalized, reconnect from a slot, gaps filled over RPC, a reorg of
  confirmed slots retracted from hot, the same property tests as EVM's reorg suite.
- **The demand gate, between S1 and S2.** Chief runs the brief's two-week test: about twenty direct
  messages and one public post, both under his name. S2 starts only with at least three teams willing
  to try it on their own program and at least one asking for account history. Otherwise the
  programme parks at S1 and the record says why.
- **S2, decoding.** Runtime Anchor and Codama decoding, IDL fetch from chain, IDLs versioned by slot,
  the per-segment failure gate. Gated on equality with an independent decoder's output (Carbon's
  decoder crates as a test oracle, not a dependency) for the S0 program.
- **S3, Old Faithful.** Behind `solana-old-faithful`. Gated on segment hashes equal to an RPC-sourced
  seal of the same range.
- **S4, account history.** Account writes filtered by owner from Yellowstone, seeded from one
  `getProgramAccounts` snapshot, `closed` honoured, as-of-slot views. Gated on agreement with a live
  `getAccountInfo` at sampled slots.

## 11. Risks

- **Nobody wants it.** The cheapest risk to test and the likeliest. The demand gate is there for it.
- **Build weight.** Yellowstone, protoc and RocksDB threaten the single-binary story; the feature
  boundary is what contains them, and the default build is checked.
- **IDL drift.** Decoders break silently on program upgrades. Slot-versioned IDLs and keeping raw rows
  are the mitigation; the failure-rate gate makes it loud.
- **Bandwidth.** A full epoch is roughly 500 GB; Old Faithful backfill of a busy program is a server's
  job. Documented as such, never claimed for a laptop.
- **Someone ships it first.** Anza's "Horizon" (account updates as a dataset, unreleased as of the
  brief) would become an input source, not a competitor to fight.

## 12. Open questions

1. Which program S0 uses. Leaning: one with a public Anchor IDL and moderate volume, so an epoch fits
   comfortably over RPC.
2. Which RPC S0 runs on: a free tier is enough for one epoch of a moderate program. No paid plan is
   bought without Chief.
3. Whether `logs` earns its own table or stays a column on `transactions`.
