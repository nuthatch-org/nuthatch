# RFC-0068: Nest postcards - a nest's sealed history and its SQL engine in one HTML file

- Status: **Proposed (2026-10-21), awaiting Chief.** Nothing is accepted and nothing is built.
- Author: Pete (cargopete)
- Date: 2026-10-21
- Depends on: RFC-0065 (the nest in a tab: Burrmill compiled to wasm, hash-checked segments),
  RFC-0052 (the mirrored nest: `manifest.json` as the catalogue, `publish.json`, content-addressed
  segments), RFC-0047 (the sealed directory as a contract).
- Blocks: nothing.
- Nature: a file format and two producers of it. One producer is a button on the `/play` page, a
  separate artefact; the other is a CLI subcommand, proposed behind an off-by-default cargo feature
  (§9). The RFC-0044 §8 record each needs is in §10.

## 1. What it is for

A postcard is one self-contained `.html` file. It holds a nest's sealed Parquet segments, the
catalogue that names them, the nest's authored views, and Burrmill compiled to wasm. Whoever receives
it opens it in a browser and runs SQL over the nest, offline, with nothing installed and nothing
hosted by anyone. "Every Uniswap v4 hook deployment, as of block N", sent as an attachment.

The point is the product's own claim, "be your own indexer", carried one step further: the recipient
does not need an indexer, a server, a mirror, or us. The sender needed only their own nest.

The engine is the expensive part of the file, not the data, and that decides most of this RFC (§3).

## 2. What exists, read 2026-10-21

- **The engine in a tab.** `burrmill-wasm` (private, `nuthatch-org`) answers a mirrored nest's
  authored views in a browser worker, row-identical to native Burrmill (RFC-0065 §1). It fetches
  each segment over HTTP and checks it against the SHA-256 its key names. It holds a memory budget:
  600 MiB on phones, 2 GiB elsewhere (RFC-0066 §12).
- **The catalogue and its hash.** A published nest's `manifest.json` lists every sealed segment per
  table with `hash`, `from_block`, `to_block`, `rows` and `writer_profile`. `publish.json` carries
  `catalogue_sha256` (the SHA-256 of that catalogue, `src/publish.rs:737`), `data_identity`, `nid`,
  `chain_id` and `sealed_through`. Both are what a postcard needs to say what it is.
- **The trust sentence already written.** `src/seed.rs` says of the mirror: the hashes prove each
  file is the one the catalogue names, and prove nothing about who wrote the catalogue or whether it
  is true to the chain. A postcard inherits that sentence unchanged (§6).
- **Nothing produces a single file today.** `pack` builds the compliance manifest (RFC-0008 C6),
  `publish` mirrors a directory to object storage, `seed` reads one back. None of them bundles an
  engine.

## 3. Size, measured 2026-10-21

All figures from files on the MacBook, with `wc -c`, `gzip -9`, `brotli -q 11` and `xz -9`. Nothing
was built for this RFC.

**The engine.** The module live on `/play` (`.play-deploy/pkg/burrmill_wasm_bg.wasm`, built
2026-10-21, `opt-level = "s"`, `wasm-opt -Os`) carries Burrmill, Parquet, decode, seal, scaffold,
identity and alloy-consensus:

| Form | Bytes | As base64 in HTML |
|---|---|---|
| raw | 35,303,889 | 47.1 MB |
| gzip -9 | 9,028,436 | 12.0 MB |
| brotli -q 11 | 5,656,750 | 7.5 MB |
| xz -9 | 5,446,688 | 7.3 MB |

The module of 2026-10-17 was 54,134,307 bytes raw and 7,076,774 at brotli, so this number moves.
A postcard needs Burrmill, Parquet and SHA-256 only. A reader-only build without decode, seal,
scaffold, identity and alloy is smaller by an amount nobody has measured; `twiggy` cannot attribute
the stripped module (60,521 items, none named). **Estimate:** 4 to 5 MB at brotli. S1 measures it.

A single file cannot use HTTP's `Content-Encoding`, so the page decompresses the engine itself.
`DecompressionStream('gzip')` is in Chrome, Firefox (113) and Safari (16.4), and costs no code.
Brotli costs a JavaScript decoder in the file (google/brotli's, MIT; size **estimated** under 200 KB,
unmeasured) and saves 4.5 MB of base64. S0 measures both and picks one.

**The data.** Segments are already zstd inside Parquet, so the cost is base64's 4/3. Local copies of
four mirrored nests (`burrmill-wasm/data/`):

| Nest | Segments | Rows | Parquet bytes | As base64 | gzip -6 saves |
|---|---|---|---|---|---|
| dips (Graph DIPs contracts) | 24 | 38 | 108,824 | 0.15 MB | - |
| graph-gns | 16 | 73,176 | 18,622,636 | 24.8 MB | 23% |
| defi-pulse | 1,181 | 1,532,888 | 128,179,943 | 170.9 MB | 4% |
| allocations (Lodestar) | 4,745 | 5,990,740 | 511,783,531 | 682.4 MB | - |

That is about 84 bytes a row on DeFi Pulse and 254 on graph-gns, with a floor of a few KB a segment:
20 of dips' 24 segments are under 6 KB. DeFi Pulse's catalogue is 498,191 bytes, 114,880 at gzip.

**Whole postcards**, at the brotli engine (7.5 MB) plus the page (estimated 0.2 MB):

- dips: about 7.9 MB. At the gzip engine, about 12.4 MB.
- graph-gns: about 32.5 MB, or about 27 MB with the data compressed again.
- defi-pulse: about 179 MB.

**The ceiling.** An attachment travels as MIME base64, another 4/3. Gmail's published limit is 25 MB
a message and Outlook.com's 20 MB (provider documentation, not checked here), and many company
gateways sit at 10 MB. **Proposed default ceiling: 15 MB of file**, about 20 MB on the wire. At the
brotli engine that leaves about 7 MB of base64, or about 5 MB of Parquet: some 60,000 rows shaped
like DeFi Pulse, or 20,000 like graph-gns. At the gzip engine it leaves about 2 MB of Parquet. Small,
narrow nests fit; a busy nest's whole history does not, and should not pretend to.

**Above the ceiling the producer refuses**, and prints each table's size and segment count so the
sender can narrow the selection (§4). It does not sample, because a sampled table answers `count(*)`
and `sum` wrongly without saying so. It does not write a link, because a link needs something hosted;
the mirror and `/play` already serve that case. A sender who means to move the file by USB or a file
share raises the ceiling explicitly, up to a hard limit S0 sets from what a browser will open (§11).

## 4. What a postcard holds

One HTML document, no external references of any kind:

1. **The page**: HTML, CSS and the JavaScript glue, inline.
2. **The engine**: the wasm module, compressed and base64'd, in a non-executing
   `<script type="application/octet-stream">` element, with its SHA-256.
3. **The catalogue**: the nest's full `manifest.json`, `publish.json` where the nest is published,
   and `schema.json`, verbatim.
4. **The views**: `views/*.sql`, verbatim.
5. **The segments**: each chosen segment in its own non-executing element, keyed by its content
   address, base64 of the exact bytes the nest sealed, compressed again only where that saves 5% or
   more (graph-gns above, not DeFi Pulse).

**Selection is by whole table and whole segment, never by row.** A sender picks tables and,
optionally, a block range, which is widened to segment boundaries. Every embedded segment is
therefore byte-identical to one the nest sealed and the mirror holds, under the same name. The full
catalogue always travels, so its hash still equals the published `catalogue_sha256`, and the page
can show which catalogue entries are present and which were left out. Omission is visible, not
silent. A row filter would need new Parquet, which is a new file no node ever sealed, and the
integrity story of §6 would end there. The nest is the filter: a narrow nest makes a small postcard.

Only sealed segments travel. Rows in the hot store, and a published nest's provisional tails
(RFC-0052 `_seed/`), are left out: they can still change.

**The file is deterministic.** The same engine, catalogue, views, selection and page version give the
same bytes, with no timestamp of the producer's own. The producer prints the file's SHA-256, so a
sender can state it beside the attachment, and the two producers of §5 can be gated on writing the
same file (§9, S3).

## 5. Who makes one

- **The page.** A "Postcard" control on `/play`, over a curated nest it already reads, and in the
  nest builder over a nest the tab has sealed (RFC-0066). The page holds the engine and fetches the
  chosen segments from the mirror, checking each hash as it does today. It shows the size before
  building, refuses above the ceiling, and hands the file to the visitor as a download. Nothing about
  it touches the binary.
- **The CLI.** A `postcard` subcommand over a local nest directory's sealed segments, for an operator
  whose nest is not on the mirror. Proposed shape, not yet a command: nuthatch postcard --dir DIR
  --tables T1,T2 --out FILE. It needs the engine bytes at build time (§9).

## 6. Integrity: what the page checks and what that proves

On open, before any query, the page:

1. hashes the decompressed engine and compares it with the hash in the file;
2. hashes the embedded catalogue and shows it beside `catalogue_sha256`, the data identity, the NID
   and the chain;
3. hashes every embedded segment and compares it with the content address the catalogue names for
   it. A segment that fails is not registered, and the page names it and its table. A table with a
   failed segment answers nothing rather than answering short.

**What that proves.** The file is internally consistent and was not damaged in transit, and every
segment in it is byte-for-byte the segment that some nest sealed under that address. Because the
addresses are the mirror's, a recipient who compares the catalogue hash shown on the page with the
nest's `publish.json` on the mirror, or with a node running the same nest, learns that the postcard
holds exactly what that nest published. That comparison is out of band, by eye or by a second tool:
the page makes no network call to do it (§8).

**What it does not prove.** Who made the file: the hashes and the data sit in the same file, so a
forger who edits one segment can edit the catalogue and its hash to match, and the page will pass.
That the data is true to the chain, which is the nest's job and its sealer's, as for every mirror
(`src/seed.rs`). That the engine is the one we released, unless the recipient compares its hash with
a published one. Signing would answer the first; Chief said "no signing for now" on 2026-10-05 for the
mirror, and this RFC does not reopen it (§12).

The page says this in two sentences at the top, not in a help panel.

## 7. What a recipient can do

Kept small on purpose:

- **Run SQL** over the embedded tables in a text box, with the engine's own errors.
- **Run the nest's authored views**, one button each, in their file order.
- **Three canned queries**, generated from the catalogue: per table, the row count, the first and
  last block, and the ten newest rows.
- **Export the current result as CSV**, through a download the page builds in memory.
- **Read the schema**: tables, columns and types from `schema.json`.

Not in v1: charts, entities, maintained views, folds, joins across two postcards, saving queries
back into the file. The engine's memory budget from RFC-0066 applies unchanged.

## 8. Freshness and the network

**A postcard is a snapshot.** Its header says: sealed through block N on chain C, published at T where
the nest was published, and that no row after block N is in the file. On a chain with a long seal
span, `sealed_through` trails the head by up to that span (86,400 blocks on Arbitrum One, RFC-0066
§12), and the header says that too.

**No live tail in v1.** RFC-0065's tail would make a postcard current, but only by calling an RPC,
and the promise that makes a postcard safe to open from email is that it calls nothing. A recipient
who wants the head is pointed, in plain text and not as a fetch, at `/play` or at running the nest.
Reopening this is §12, question 4.

**No network, enforced.** The page carries a Content-Security-Policy meta tag:
`default-src 'none'; script-src 'sha256-…'; style-src 'unsafe-inline'; worker-src blob:;
img-src data:; connect-src 'none'; form-action 'none'; base-uri 'none'`, with `'wasm-unsafe-eval'`
added to `script-src` so the module can instantiate, and the inline scripts admitted by their hashes,
which the producer computes. `connect-src 'none'` is the guarantee: no fetch, XHR, WebSocket or beacon
leaves the page. Decompression and decoding work on in-memory bytes and need none of them. The page
counts `securitypolicyviolation` events and shows the count; S0 gates on it being zero.

## 9. Default binary or feature

The page producer is not a binary question: it is a static page in `burrmill-wasm`.

For the CLI producer, **RFC-0046 §1's deletion test does not decide it**, and saying otherwise would be
dishonest. Delete the subcommand and a self-hoster loses nothing they had before, and nothing else in
the binary would depend on it. Any optional, purely additive command passes that test wherever it
lives. What decides it is what the command costs everyone who never uses it:

- **Binary size.** The 5.8.2 macOS release tarball is 37,877,867 bytes. Embedding today's engine adds
  5.66 MB at brotli (about 15%) or 9.03 MB at gzip (about 24%), on every `curl | sh`, for a feature
  most operators will not run. A reader-only engine shrinks that; it does not remove it.
- **The release pipeline.** The engine is a wasm32 build from a private repository, needing Homebrew
  LLVM's clang for zstd-sys. Putting it in every release build ties the binary's release to that
  toolchain and to a repository the public cannot rebuild from.

**Recommendation: off by default, behind a `postcard` cargo feature**, as `folds` and `graph` are, with
a separate release asset if a user asks for one. A default build's CLI help, `init` output, on-disk
layout and behaviour are what they would be had this RFC not been written, and S3 gates on exactly
that. Promotion to the default binary is a later decision for Chief, on a user's request and a
measured size.

## 10. Against the non-negotiables

- **Single static binary.** Unchanged by default. The feature build stays one binary; the engine is
  bytes inside it.
- **≤2 GB.** Not the CI budget, but the recipient's tab runs under the same 2 GiB engine budget, and
  S0 records peak memory for every view.
- **No phone-home.** The postcard calls nothing (§8). The producers call only what they call today:
  the page the mirror, the CLI nothing.
- **Determinism.** A postcard is a read surface over sealed bytes; nothing in it feeds stored state.
  The file itself is deterministic (§4).
- **MIT OR Apache-2.0.** The engine and every crate in it are already under it. The brotli decoder,
  if chosen, is MIT. `burrmill-wasm` being private is a distribution question, not a licence one
  (§12, question 2).
- **RFC-0044 §8.** S0 and S1 change nothing in this tree. S2 is a new surface in a separate artefact
  and, like RFC-0065 S3 and RFC-0066 S1, needs Chief's line in `CLAUDE.md` before it starts. S3 adds
  a feature to the binary and needs its own line, naming the feature as off by default.

## 11. Slices and their gates

- **S0, a postcard by hand, which can stop the programme.** No producer: a script assembles two files
  from today's engine, dips whole and a DeFi Pulse selection near the ceiling, once with a gzip
  engine and once with brotli. Gates: opened from `file://` with the CSP of §8, in Chrome, Firefox and
  Safari on the MacBook, every authored view answers row-identical to native Burrmill (multisets of
  the nuthatch encoding, as RFC-0065 measures), with zero CSP violations and zero network requests in
  the browser's own log. Recorded: file size, time to first answer, peak memory, per browser, and the
  largest file each browser opens. Each file is also mailed to Gmail, Outlook.com and one other
  provider, downloaded at the far end, and hashed. **Stop** if Chrome cannot run the engine from
  `file://`, or if two of the three providers strip or quarantine the attachment: then a postcard
  is not a postcard, and whether it is still worth building as "a file you share" is Chief's call.
- **S1, a reader-only engine.** A second build target in `burrmill-wasm` with Burrmill, Parquet and
  SHA-256 only. Gates: every S0 view still identical; the size recorded; two clean builds of the same
  commit give the same bytes, since a postcard prints the engine's hash and a recipient can only check
  a hash that can be reproduced. If the build is not reproducible, that is a finding to fix before S3.
- **S2, the page producer.** The `/play` control of §5. Gates: for a pinned nest and selection, every
  embedded segment's hash equals the mirror catalogue's entry, and the catalogue's hash equals
  `publish.json`'s; a test that flips one byte of one embedded segment sees the page refuse that
  segment, name it, and answer nothing from its table; a selection over the ceiling is refused with
  the per-table sizes. Mutating either check turns its test red.
- **S3, the CLI producer, behind `postcard`.** Gates: for the same nest, selection and engine, the CLI
  writes the same bytes the page does; a default build's `--help`, `init` output and release binary
  are unchanged from the commit before, which is the deletion test written as a gate; the feature
  build's size delta is recorded.

Not slices: a live tail, signing, a shared-postcard gallery.

## 12. Non-goals

- **Hosting.** Nothing in this RFC is served by us; the file is the whole delivery.
- **Rows that can change.** No hot store, no provisional tails, no unsealed rows.
- **Row-level selection or sampling** (§3, §4).
- **A live tail** in v1 (§8).
- **Entities, maintained views, folds.** Authored SQL views only, as in RFC-0065 §7.
- **Writing back.** A postcard never becomes a nest; the zip from RFC-0066 is how a nest travels.
- **The WASM transform layer.** Removed on 2026-10-08 (#1999). This runs the engine, not handlers.

## 13. Risks

- **It looks like HTML smuggling.** A large base64 blob that inline JavaScript decodes is how a common
  phishing technique delivers payloads, and mail gateways score HTML attachments on it. Some
  recipients will never see a postcard. S0 measures this before anything else is built.
- **`file://` is each browser's own.** Workers from `blob:` URLs, `wasm-unsafe-eval` under a meta CSP
  and OPFS behave differently on an opaque origin, and change between releases. OPFS is not assumed:
  a postcard decompresses on every open. If a worker is refused, the engine runs on the main thread
  and the page says it may pause.
- **Phones.** Mail apps preview HTML attachments without running script. A phone recipient must save
  the file and open it in a browser, under the 600 MiB budget.
- **The engine dominates the size**, so the whole design leans on S1. If a reader-only engine stays
  near 5.7 MB at brotli, the useful data budget stays near 5 MB of Parquet.
- **A frozen engine.** A postcard answers with the engine it was made with, forever. That is what
  makes it reproducible, and it also keeps that engine's bugs. The page shows the engine's version.
- **A snapshot read as current.** Mitigated by the header of §8, not removed.

## 14. Alternatives

- **The engine fetched at open time from a CDN.** A tiny file, but a network call on open, a third
  party in the trust path, and a postcard that stops working when the URL does. Rejected.
- **A zip of Parquet plus a link to `/play`.** Smaller, but needs our page to stay up and a browser
  that grants it file access. It is the mirror case with extra steps.
- **DuckDB-Wasm instead of Burrmill.** 35.6 to 41.3 MB raw for its three builds, slower on nuthatch's
  views (RFC-0065 §1), and not the engine a node answers with.
- **The CLI fetches the engine from the release page** when run, keeping the default binary small.
  It is a network call the operator asked for, which non-negotiable 3 allows, but the command then fails offline.
  Worth having as a fallback if S3 is never promoted.

## 15. Open questions for Chief

1. **The ceiling.** 15 MB by default, refused above it with the sizes shown, raisable to a hard limit
   S0 sets. Is refusing the right behaviour, or should the producer offer to drop whole tables until
   it fits?
2. **`burrmill-wasm` stays private?** Every postcard carries its build. A recipient can check the
   engine's hash only against something published, and can rebuild it only from a public source.
   RFC-0065 §11 asked the same question for a different reason.
3. **Which producer first.** The page (S2) needs no binary change and reaches visitors; the CLI (S3)
   reaches operators whose nests are not on the mirror. The order above is page first.
4. **A live tail later?** It would need `connect-src` opened to one RPC, behind a button the recipient
   presses, and the page would stop being a file that calls nothing. Lean: never in a postcard; that
   is what `/play` is.
5. **Signing.** Out, per 2026-10-05. A postcard leaves every channel we control, and its hashes
   cannot say who made it (§6), which is a stronger case for signing than the mirror's. Still out?
