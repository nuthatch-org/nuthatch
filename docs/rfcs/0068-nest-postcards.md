# RFC-0068: Nest postcards - a link that opens a published nest, pinned and checked, in a tab

- Status: **Accepted (2026-10-21, Chief).** Chief reshaped it the same day from a self-contained
  `.html` file to a link ("lets make postcards just a url or something not email"); this is that
  shape. **S0 measured 2026-10-22 (§14): the cut held on every pair, and Pages, raw GitHub and
  jsDelivr each served the tab row-identical to native Burrmill in Chrome and Safari. R2 was not
  tested, and the week of daily snapshots was not run.** §13: Chief said yes to any host named in a
  link (2026-10-22); Q2 and Q3 take this RFC's leanings until he says otherwise.
- Author: Pete (cargopete)
- Date: 2026-10-21
- Depends on: RFC-0065 (the nest in a tab: `/play`, Burrmill in wasm, hash-checked segments),
  RFC-0052 (the mirrored nest: `manifest.json` as the catalogue, append-only, content-addressed
  segments under `<data_identity>/`), RFC-0047 (the sealed directory as a contract).
- Blocks: nothing.
- Nature: a change to the `/play` page in `burrmill-wasm`, a separate artefact. **No change to the
  binary**: `nuthatch publish sync` already writes everything a postcard reads (§6).

## 1. What it is for

A postcard is a URL. It opens nuthatch-indexer.com/play on one published nest, pinned to the segments
that nest had sealed when the link was made, with a query or view ready to run. The tab fetches the
catalogue and segments from wherever the nest is published, checks every byte against the hashes the
link carries, and answers in the browser. "Every Uniswap v4 hook deployment, as of block N", pasted
into a chat.

The point is "be your own indexer" carried to the reader: the sender runs a nest and puts its sealed
directory on any static host they control; the recipient needs a browser. No server runs a query, and
the query itself never reaches one, because it travels in the URL fragment (§4).

## 2. What `/play` does today, read 2026-10-21 at `burrmill-wasm` `a42752f`

- **The worker already opens an arbitrary base.** `open({ base, views, decoder })` in
  `web/play/worker.js` fetches `${base}/manifest.json`, `schema.json` and `_seed/seed.json`, and reads
  each segment from `${base}/<table>/<hash>.parquet`, checking its SHA-256 against the address and
  caching it in OPFS by hash. Nothing in it is specific to our mirror.
- **The page only offers our nests.** `play.js` reads `nests.json` (base
  `https://nestsss.nestsss-worker.workers.dev`, a curated list) and chooses by `?nest=<name>`. That is
  RFC-0065 §11's answer 4 (Chief, 2026-10-18: a curated list, not any mirror URL). A postcard to a
  sender's own host reopens that answer for links only; §11 asks Chief to confirm it.
- **A share link exists, in the wrong place.** Every run writes `?nest=<name>&q=<sql>` into the
  address with `history.replaceState`. That is the query string, so the SQL goes to the host serving
  `/play` on every reload.
- **The catalogue is not pinned.** The manifest is fetched with `cache: "no-cache"` and used whole.
  A nest that seals more between sending and opening answers over more rows than the sender saw, and
  a host that drops or adds an entry goes unnoticed. `schema.json` is not checked at all.
- **Views come from `nests.json`**, copied from each nest's repository at a commit. The mirror does not
  publish `views/` (RFC-0052 publishes the catalogue, segments, `schema.json`, `publish.json` and
  `_seed/`), so a nest on someone else's host arrives with no views.
- **The tab follows the head by default**: it loads the seed's provisional tails and fetches a live
  tail from an RPC. A snapshot link wants neither.

So the reader exists. What is missing is a pinned catalogue, a fragment, views that travel with the
link, a mode with no tail, and a way to name a host that is not ours.

## 3. Pinning a snapshot of a catalogue that keeps growing

The obvious pin, `catalogue_sha256` from `publish.json`, does not work: the mirror rewrites
`manifest.json` every time the nest seals, so the hash a sender copies stops matching within minutes.

What does hold is RFC-0052 §3.2: the published catalogue is append-only and an entry is never removed.
So a link pins **a cut**: for each table, the highest `to_block` the sender's catalogue held. The tab
takes the host's current catalogue, keeps the entries at or below each table's cut, and hashes them.

**Measured on DeFi Pulse, 2026-10-21.** The catalogue copied on 2026-10-17 held 1,181 entries; the
mirror's today holds 1,288. All 1,181 are in today's, and cutting today's at the old catalogue's
highest `to_block` (26,209,583) gives exactly those 1,181, no more, no fewer. Two findings from the same
check: the local `publish.json` beside that copy said `sealed_through` 26,207,783, below entries the
catalogue listed (the two may have been copied at different moments), so the cut comes from the
catalogue's own `to_block`s, never from `publish.json`; and segment boundaries differ between tables,
so one global block number could admit a lagging table's later segment. Hence one cut per table.

**The digest** the link carries is SHA-256 over `schema.json`'s bytes followed by the cut's entries as
sorted `table hash` lines. It binds the schema, every table, every segment and the absence of any
other. A host can withhold; it cannot substitute or add without the tab refusing.

## 4. The link

```
https://nuthatch-indexer.com/play/#v=1&m=<base>&d=<data_identity>&c=<cuts>&h=<digest>&q=<query>
```

- `m`, the base URL of the published directory; omitted for our mirror.
- `d`, the data identity, which is the directory name `publish sync` writes under. It names the nest;
  it is not checked, since the tab cannot recompute it from published files. `h` is the check.
- `c`, the per-table cuts in sorted table order; `h`, the digest of §3, base64url.
- `q`, the query, raw-deflated and base64url'd; optionally `w`, the views it needs, the same way.

**Everything after `#` stays in the browser.** Browsers do not send a fragment in a request, so neither
our host nor the sender's sees the query, the views or the pin. The sender's host sees the opener's IP
and a `Referer` of `https://nuthatch-indexer.com/` when segments are fetched, as for any link.

**Length, measured with Node's `deflateRawSync` at level 9:**

| Query | Raw | Percent-encoded | Deflate + base64url |
|---|---|---|---|
| a one-line `SELECT` (58 chars) | 58 | 86 | 75 |
| DeFi Pulse `10-eth-usd.sql` | 531 | 721 | 380 |
| DeFi Pulse `30-aave.sql` | 4,939 | 6,621 | 2,742 |
| all four DeFi Pulse views | 8,898 | 12,150 | 4,299 |

With the fixed fields (about 300 characters for ten tables, an estimate), a link with a short query is
under 400 characters and one carrying all four DeFi Pulse views about 4,600. Browsers accept URLs far
longer than that (Chromium documents 2 MB; not measured here). The binding limit is where links are
pasted: Discord's documented message limit is 2,000 characters, X shortens every link. **Fallback for a
long query:** above 1,500 characters the page offers a short link whose `q` is `sha256:<hex>` and a
`.sql` file for the sender to put at `<base>/queries/<hex>.sql`. The tab fetches it and refuses it
unless it hashes to the link's value. That works only on a host the sender controls, and the page says
so.

## 5. Hosts, and what they need

A host must serve the published directory as plain files over HTTPS with
`Access-Control-Allow-Origin` on GET responses. The tab sends no custom headers, so a CORS-simple GET
needs no preflight. Checked with `curl -D -` and an `Origin` header, 2026-10-21:

| Host | Allow-Origin | Notes |
|---|---|---|
| our Worker (`nestsss.nestsss-worker.workers.dev`) | `*` | preflight also answered: GET, HEAD, `range`; manifests `max-age=60` |
| our bucket's `r2.dev` URL | none | a browser cannot read it; a sender's own R2 bucket needs a CORS rule |
| GitHub Pages (`rust-lang.github.io`) | `*` | `max-age=600` |
| `raw.githubusercontent.com` | `*` | |
| jsDelivr | `*` | |
| `ipfs.io`, `dweb.link` | `*` | both answered **429** from this machine: rate-limited |

So GitHub Pages works as it stands, and is free. Two traps there: Pages runs Jekyll unless the site has
a `.nojekyll` file, and Jekyll drops paths starting with `_`, so `_seed/` would vanish (a postcard does
not read it, but a seeding node would); and GitHub documents a 1 GB site and 100 MB file limit
(unverified here), which DeFi Pulse's 128 MB fits and Lodestar's 512 MB fits too. IPFS gateways serve
CORS but rate-limit, so an IPFS-only postcard is supported and not recommended.

**A host that lags is not a host that lies.** A cached manifest older than the link's cut (Pages caches
ten minutes) holds fewer entries than the cut; the tab says the host has not caught up and offers a
retry, rather than calling the digest wrong.

## 6. How a sender makes one

1. **Publish.** For our curated nests this is done. For the sender's own,
   `nuthatch publish sync --dir . --target ./site` writes the directory to a folder (the filesystem
   target exists today, `src/publish.rs:588`), which they push to GitHub Pages with a `.nojekyll`; or
   `--target s3://bucket` to their own R2 or S3 bucket with a GET CORS rule.
2. **Open it in `/play`.** A field takes a published base URL; the tab reads `publish.json` and the
   catalogue and opens it, unpinned, as it opens our nests today.
3. **Copy the postcard.** A button computes the cut and digest from the catalogue the tab has open,
   folds in the current query and any views it uses, and copies the link.

**No new binary capability.** Step 1 is `publish sync` as shipped; steps 2 and 3 are the page. RFC-0046
§1's deletion test is trivially passed: delete postcards and a self-hoster's binary is byte-identical.
The RFC-0044 §8 record therefore covers the page only. A later convenience, `publish sync` printing
the link when told the host's public URL, would be a new flag and need its own line; it is not proposed.

**The nest builder** (RFC-0066) seals in the tab, into OPFS, and publishes nothing, so it cannot make a
postcard until its zip has been run by a node and published. Not in v1.

## 7. What the tab checks, and what that proves

On opening a postcard, before any query, the tab:

1. fetches `schema.json` and `manifest.json` from the base, cuts the catalogue (§3), and compares the
   digest with `h`; a mismatch stops it and says which side has more entries, when it can tell;
2. fetches each segment a statement reaches and checks it against its address, as today; a segment that
   fails is named and its table answers nothing rather than answering short;
3. runs with no seed tails and no live tail, and shows: chain, data identity, per-table cut, and "these
   rows are what the nest had sealed through block N; nothing later is included."

**What that proves.** The rows the tab answered from are exactly the set of segments the link names,
whatever host served them. The host is untrusted: it can refuse or lag, and the tab says so; it cannot
change an answer without the tab refusing. Because segment addresses are the ones every node and our
mirror use, the same link opened against a different host holding the same nest gives the same answer.

**What it does not prove.** Who made the link: anyone can compute a digest over any catalogue, and a
link edited in transit names a different set, so a postcard is as trustworthy as the channel it came
through. That the data is true to the chain, which is the nest's and its sealer's business, as
`src/seed.rs` says of the mirror. Signing stays out, per Chief on 2026-10-05.

## 8. What a recipient can do

What `/play` does now, within the cut: run SQL, run the link's views, chart, export CSV. Plus one
button, **Follow to the head**, which drops the pin and turns the tab into ordinary `/play` for that
nest, live tail and all, with the page saying the answers are no longer the postcard's.

## 9. Against the non-negotiables

- **Single static binary; ≤2 GB.** The binary is untouched. The tab keeps its 2 GiB engine budget and
  600 MiB on phones (RFC-0066 §12).
- **No phone-home.** Nothing new. The tab talks to the host the link names, and to an RPC only after
  Follow to the head.
- **Determinism.** A read surface over sealed, content-addressed bytes; nothing feeds stored state. The
  pin makes the answer reproducible from the link alone.
- **Out of scope list.** We host nothing new: links to our nests use the mirror we already run, and any
  other link uses the sender's host.

## 10. Slices and their gates

- **S0, the cut and the hosts, which can stop the programme.** (a) Snapshot every nest's published
  catalogue daily for seven days; for each day pair, cutting the later catalogue at the earlier one's
  per-table `to_block`s gives exactly the earlier entry set. DeFi Pulse passed one such pair (§3). Any
  nest whose published catalogue drops, rewrites or back-fills an entry under a cut stops the design: a
  link would then need a frozen copy, which is hosting. (b) A nest published with `publish sync` to a
  folder, pushed to GitHub Pages with `.nojekyll`, and to an R2 bucket with a CORS rule, opened by a
  hand-written link in Chrome, Firefox and Safari: every authored view row-identical to native Burrmill
  (multisets of the nuthatch encoding, as RFC-0065 measures). Stop if neither host serves the tab.
- **S1, the reader.** Fragment parsing, the cut and digest of §3, `schema.json` pinned, tails off,
  `q` and `w` from the fragment, and the existing share link moved from the query string into it. Gates,
  each by a scripted host: an altered segment is refused and named; an entry added under a cut, an entry
  dropped and an altered `schema.json` each fail the digest; a host behind the cut reads as behind; the
  browser's network log over a full session contains no request whose URL holds the query. Mutating
  each check turns its case red.
- **S2, the writer.** The base-URL field and Copy postcard. Gates: a link copied in one browser profile
  and opened in a fresh one gives the same answer digest; the long-query fallback round-trips and
  refuses a `.sql` file edited by one byte; link lengths for every curated nest's views recorded.

## 11. Non-goals

- A self-contained file. Dropped by Chief on 2026-10-21; see §12.
- A shortener, a link registry, or any server of ours that sees a postcard.
- Rows that can still change: no hot store, no provisional tails, unless the recipient follows the head.
- Signing, and the nest builder as a producer (§6).
- Entities, maintained views and folds: authored SQL views only, as RFC-0065 §7.

## 12. Alternatives

- **The self-contained `.html` file**, this RFC's first draft: engine and segments base64'd into one
  file. Measured, it was the engine that sank it: 5.66 MB at brotli (7.5 MB as base64) for today's
  `/play` module, so dips' 38 rows made a 7.9 MB file and DeFi Pulse 179 MB, against mail limits of
  20 to 25 MB, and gateways score such files as HTML smuggling.
- **Pinning `catalogue_sha256`.** Breaks on the nest's next seal (§3).
- **A frozen copy per postcard** on the sender's host, so the whole catalogue can be pinned. Works, and
  multiplies storage per link; the cut gets the same guarantee from the append-only rule for free.
- **Publishing `views/` with the directory.** Tidier for views, but it changes what `publish sync`
  writes and makes a binary change of a page feature; the link carries the views instead.

## 13. Open questions for Chief

1. **Any host, or our mirror only?** RFC-0065 §11 settled `/play` on a curated list. A postcard to a
   sender's own GitHub Pages means `/play` opens a base URL named in a link. The design assumes yes,
   for links and the base-URL field, with the picker staying curated.
2. **IPFS.** Supported by construction, rate-limited in practice (429 from both gateways tried). List it
   in the docs, or leave it unmentioned until someone asks?
3. **Follow to the head** on a postcard: in, as proposed, or should a postcard only ever show its
   snapshot?

## 14. S0, measured 2026-10-22

**(a) The cut holds on every pair found; the week of daily pairs was not run.** No snapshot job was
installed. Instead every older catalogue that already existed was used: the mirror's catalogues today,
the mirror copies in `burrmill-wasm/data/` (published 2026-10-13 and 2026-10-17), one taken from the
mirror on 2026-10-21, the frozen release-gate copies on the ThinkPad (2026-10-11 and 2026-10-12, each
checked against its `PROVENANCE` hash), and older local copies. A local catalogue counts as what a
`publish sync` at that moment writes, its provisional entries removed, and only when a definition-only
copy of its directory computes the mirror's data identity. Three that did not (`grt-supply-*` from
2026-10-07, `alloc-l1` from 2026-10-05) also pass and are left out of the count.

For each pair, the later catalogue is cut at the earlier one's per-table highest `to_block` and the
entries compared whole, every field but `provisional`:

| Nest | Snapshots | Pairs | Entries, first to last | Dropped, rewritten, back-filled |
|---|---|---|---|---|
| graph-allocations-nest | 10-11, 10-12, 10-13 (local), 10-13 (mirror), 10-22 | 10 | 4,727 to 5,157 | 0 |
| qos-reo-nest | 10-11, 10-12, 10-22 | 3 | 11,554 to 12,662 | 0 |
| defi-pulse-nest | 10-17, 10-21, 10-22 | 3 | 1,181 to 1,307 | 0 |
| graph-gns-nest | 10-11, 10-12, 10-13, 10-22 | 6 | 16 to 44 | 0 |
| dips-nest | 10-11, 10-12, 10-13, 10-22 | 6 | 24 to 28 | 0 |
| betswirl-bnb-nest | 10-06, 10-22 | 1 | 260 to 260 | 0 |
| data-services-nest | 10-11, 10-12, 10-13, 10-22 | 6 | 0 to 0 | vacuous |

35 pairs over 7 nests, 29 of them over a non-empty catalogue, all exact; no entry straddled a cut.
**Against the gate as written this falls short:** seven calendar days carry a snapshot (10-06, 10-11,
10-12, 10-13, 10-17, 10-21, 10-22), the longest run of consecutive days is three, and 11 of the
adjacent pairs are a day apart or less; the rest span 4.5 to 15.8 days. A long pair is the harder
test of append-only, not the easier one: the allocations pair from 10-13 to 10-22 spans a restart and
nuthatch 5.6.3 to 5.8.2. `schema.json` was byte-identical across all 33 pairs with both copies, the
5.0.1 to 5.8.2 ones included.

Three things S1 must carry from it:

- **A table with nothing under the cut.** 27 allocations tables and 2 GNS tables held no entries at
  the earlier snapshot and some later; 2 of the 27 were not listed at all. A link carries no cut for
  such a table, so the reader must admit nothing for any table `c` does not name. Reading a missing cut
  as unbounded would fail the digest on an honest host.
- **Append-only is a property of sealing, not of publishing.** `publish sync` writes the local
  catalogue whole, minus provisional entries (`published_catalogue` in `src/publish.rs`), and never
  merges with or checks against the remote one. Nothing found here drops an entry, but a nest
  re-indexed from scratch under the same identity with different seal boundaries would rewrite
  entries under every cut. The digest refuses that, which is right; the page should say the host no
  longer holds the pinned set, not that it lies.
- `publish sync` to a folder leaves `manifest.json.lock` beside the catalogue; it went to Pages too.
  Harmless, and worth leaving out of the directory a sender pushes.

**(b) Three hosts served the tab; R2 was not tested.** `graph-gns-nest` from its frozen 2026-10-12 gate
copy, PROVENANCE file removed so it computes the mirror's identity `494ce630…`: 16 segments, 73,176
rows, published with `publish sync --target <folder>` by 5.8.2 and pushed to a new public repository,
`nuthatch-org/postcard-s0-pages`, with `.nojekyll`. `/play` taking a base URL was prototyped in a local
copy of the live page (its `play.js` and `worker.js` byte-identical to production), run on
`localhost`, nothing deployed: `#v=1&m=<base>&d=<data identity>&w=<views>&q=<query>`, `w` and `q`
raw-deflated, no seed tails and no live tail. Each run emptied the tab's segment cache first, so all
13 segments the views reach came from the named host. Seven views, from the three view files at the
commit `/play` lists, compared as RFC-0065 measures, against `examples/native.rs` over the same folder:

| Host (`m`) | Allow-Origin, cache | Chrome 154 | Safari 26.5 | Firefox |
|---|---|---|---|---|
| `nuthatch-org.github.io/postcard-s0-pages` | `*`, `max-age=600` | 7 of 7 identical | 7 of 7 identical | not installed |
| `raw.githubusercontent.com/…/main` | `*`, `max-age=300` | 7 of 7 identical | 7 of 7 identical | not installed |
| `cdn.jsdelivr.net/gh/…@main` | `*`, `max-age=604800`, `s-maxage=43200` | 7 of 7 identical, third run | 7 of 7 identical | not installed |

The digests, rows in brackets: `subgraph_publications` (17,159) `db04f6d5…`, `developer_activity_weekly`
(187) `5d93158c…`, `subgraph_activity` (17,159) `40ba7288…`, `subgraph_versions` (30,282) `47974319…`,
`subgraph_current` (17,159) `2ce4449e…`, `deployment_subgraphs` (30,282) `9af478ca…`,
`account_default_names` (0) `e3b0c442…`. No request from the tab carried the query or the views.

- **jsDelivr** lost one segment fetch on each of the first two Chrome runs, a cold CDN under sixteen
  concurrent requests; the same file fetched alone answered. The worker caches a failed fetch's promise
  for its life, so one transient failure fails every later view that reaches the segment. S1 retries a
  failed fetch once and does not keep the rejection. jsDelivr also holds a branch ref for up to 12
  hours at its edge, so it is the host most likely to read as behind a fresh link.
- **Pages** took about 25 minutes to publish its first build, during a GitHub Actions incident.
  `.nojekyll` kept `_seed/`.
- **R2 not tested.** The mirror's key is scoped to `nestsss`: listing and creating buckets answer 403.
  A CORS rule on `nestsss` would change the live mirror, and serving a new bucket publicly needs the
  Cloudflare API rather than an S3 key. A sender's own bucket with a CORS rule is therefore unmeasured.
- Firefox is not installed on this machine. Safari was driven by opening the link; the page posted its
  digests back to the local server that served it.

**§13 as taken.** Q1, any host named in a link: yes, Chief, 2026-10-22, the picker staying curated.
Q2, IPFS: this RFC's leaning, supported by construction and not recommended. Q3, Follow to the head:
in, as §8 proposes.

The gate's stop condition is not met: three hosts serve the tab. S1 can start; the daily snapshots and
R2 are Chief's to ask for.
