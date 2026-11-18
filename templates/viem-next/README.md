# viem + Next.js + nuthatch

A minimal App Router/TypeScript page showing the latest 20 USDC transfers on Base. The page queries
the local nest's `POST /sql` endpoint server-side; viem supplies the Base client, chain metadata and
USDC amount formatting. There is no browser CORS setup and no `eth_getLogs` application query.

## Clean clone

Requires Node.js 20.9+ and a Base JSON-RPC endpoint that allows `eth_getLogs` (verified with `https://base-rpc.publicnode.com`; `https://mainnet.base.org` rate-limits it with HTTP 429 and never reaches rows). `npx` downloads the nuthatch release
binary on first use. No Rust toolchain, Postgres, or API token is required by nuthatch itself.

```sh
cd templates/viem-next
cp .env.example .env.local
npm install
export NUTHATCH_RPC=https://your-base-rpc.example
npm run index
npm run dev
```

Open http://localhost:3000. `NUTHATCH_URL` defaults to `http://127.0.0.1:8288`; set it in the shell
(or `.env.local`) if the nest is elsewhere. When using `.env.local` for the RPC, also export that
value for `npm run index`: the indexing script intentionally inherits the shell environment.

`npm run index` runs `npx -y nuthatch-indexer dev --dir nest --backfill 300 --detach`, then polls
`/sql` until a USDC Transfer row is available. **The nuthatch version in this repository has no
`--wait-rows` flag**; `--detach` itself waits only for the first chain poll, so the script implements
the row wait with the documented HTTP API. The initial backfill can take time on a rate-limited RPC.
Stop the detached nest with `npm run stop:index` from this directory.

To start the web app without restarting the nest, run `npm run dev` in another terminal. In
production, run `npm run build && npm start` with the nest and `NUTHATCH_URL` already available.
