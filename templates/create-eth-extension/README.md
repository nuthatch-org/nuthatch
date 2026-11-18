# create-eth nuthatch extension

This is a deliberately small Scaffold-ETH 2 extension skeleton: `extension/packages/nextjs` holds the
page overlay, `extension/nest` is the nest at the extension root, and the root
`package.json.args.mjs` adds an `index` script. `extension/scripts/index.mjs` runs
`npx -y nuthatch-indexer dev --dir nest --backfill 300 --detach` and polls `POST /sql` until rows
exist. The current CLI has **no `--wait-rows` flag**; `--detach` only waits for the first chain poll.

**Offline convention caveat:** this repository does not contain Scaffold-ETH 2 or its extension
schema, and the exact merge/export and overlay conventions could not be checked offline. The layout
is intentionally limited to the conventional `package.json.args.mjs` plus `extension/` files; check
it against the `create-eth` version you publish for before release.

Publish this directory as its own repository, **`nuthatch-org/create-eth-nuthatch`**, for:

```sh
npx create-eth -e nuthatch-org/create-eth-nuthatch
```

After creating a project, supply a Base RPC and start indexing, then start Next.js:

```sh
export NUTHATCH_RPC_8453=https://your-base-rpc.example
yarn index
yarn dev
```

The page reads the latest 20 transfers from the nest server-side. `NUTHATCH_URL` defaults to
`http://127.0.0.1:8288`. Stop the nest with `npx -y nuthatch-indexer stop --dir nest` from the
project root. Do not commit RPC provider keys.
