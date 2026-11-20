# PR draft (do not post; lead reviews)

Title: docs: add an optional Nuthatch config for XDC

Body:

XDC was dropped from the Envio indexers in 7.3.0 because HyperSync fell about 1.65M blocks behind on it
(enviodev/hyperindex#1829). People who still need Sablier data on XDC have no indexer now.

This adds `nuthatch/xdc/`: a config for [Nuthatch](https://github.com/nuthatch-org/nuthatch), a single binary that
reads a plain JSON-RPC endpoint, for the XDC deployments of Lockup (v2.0, v3.0, v4.0), Flow (v1.1, v2.0, v3.0) and
Airdrops (v1.3, v2.0, v3.0 factories, with campaigns discovered as they are created). The ABIs are copied from the
`sablier` package already in `package.json`, so decoding matches.

It is opt-in. It is not in `just`, CI, `package.json` or the published package, and it adds no dependency. Delete the
folder and nothing else changes.

What it is not: it does not build the entities in `schema/`. It stores one table per decoded event and you query it
with SQL. It is not a replacement for the Envio or Graph entity API.

Numbers from a run on a laptop against the public `https://rpc.xinfin.network`, no API key:

- 24.3M blocks (85,225,620 to the tip), 535 events, caught up to the tip in about 12 minutes from an empty directory.
- 724 RPC calls in total (524 `eth_getLogs`).
- Lockup v2.0 on XDC: 417 logs by direct `eth_getLogs`, 417 rows in the nest.

Try it:

```bash
pnpm install
node nuthatch/xdc/sync-abis.mjs
cd nuthatch/xdc && nuthatch dev --dir . --listen 127.0.0.1:8090 --seal-direct
```

<!-- Include the next line only if the hosted nest is live when this is posted; otherwise delete it. -->
A hosted copy of this nest, if you only want to query it: NEST_URL (https://nuthatch-indexer.com/nests/sablier-xdc).

Not run: `just prettier-check` and `na biome lint` (dependencies not installed on the machine that prepared this). The
only changed files are a README, a TOML config, a small .mjs script and a .gitignore.
