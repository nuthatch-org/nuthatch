# create-eth nuthatch extension

A [Scaffold-ETH 2 extension](https://github.com/scaffold-eth/create-eth) that ships a nest. It
follows create-eth's layout (everything lives under `extension/`, merged into the generated app):

- `extension/package.json` - merged into the root `package.json`; adds `yarn index` and `yarn index:stop`.
- `extension/scripts/index.mjs` - runs `npx -y nuthatch-indexer dev --dir nest --backfill 300 --detach`,
  then polls `POST /sql` until a USDC Transfer row exists (the released CLI has no `--wait-rows`).
- `extension/nest/` - the nest (Base USDC `Transfer`), plus an `AGENTS.md` for coding agents.
- `extension/packages/nextjs/app/transfers/page.tsx` - a new `/transfers` route reading the nest
  server-side. It is a new route, not an overlay of `app/page.tsx`: create-eth copies extension files
  with `clobber: false`, so an extension cannot replace a file the base template already has.

## Verified

Run against `create-eth` 2.0.23 in its `--dev` mode (which reads `externalExtensions/<name>/extension`
from the create-eth package, since non-dev `-e` only accepts GitHub repositories, not local paths):
`create-dapp-se2 app1 --dev -e nuthatch --skip-install --solidity-framework hardhat` produced
`scripts/index.mjs`, `nest/`, `packages/nextjs/app/transfers/page.tsx` and the `index` scripts in
`package.json`. The `yarn install` and `/transfers` render steps were not run; the same page logic
is proven end to end in `templates/viem-next`.

## Publishing

This directory must be published as **its own repository**, e.g. `nuthatch-org/create-eth-nuthatch`,
with `extension/` at the repo root, because create-eth fetches `owner/repo[:branch]` from GitHub. Then:

```sh
npx create-eth@latest -e nuthatch-org/create-eth-nuthatch
```

## Use

```sh
export NUTHATCH_RPC=https://your-base-rpc.example   # a public RPC that allows eth_getLogs
yarn index
yarn start
```

Open http://localhost:3000/transfers. `NUTHATCH_URL` defaults to `http://127.0.0.1:8288`. Never
commit RPC provider keys.
