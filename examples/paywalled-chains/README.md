# Etherscan-compatible account history on your own RPC

Etherscan's free tier stopped covering BNB Smart Chain, Base, OP Mainnet and Avalanche C-Chain in
November 2025. This directory is a nuthatch runtime that serves an account's history on **Base, BNB
Chain and Gnosis** at one Etherscan-shaped `/api`, from your own RPC, on your own machine. rotki and
anything else that speaks Etherscan's `module=account` interface can read it.

Each subdirectory is an address-history nest (RFC-0063): one chain, one cursor, its own store.

## Run it

```sh
mkdir -p ~/history
curl -fsSL https://codeload.github.com/nuthatch-org/nuthatch/tar.gz/main \
  | tar -xz -C ~/history --strip-components=3 nuthatch-main/examples/paywalled-chains
export NUTHATCH_RPC_8453=https://your-base-rpc
export NUTHATCH_RPC_56=https://your-bsc-rpc
nuthatch dev --dir ~/history
```

Then watch an account on every chain, and ask for its history:

```sh
curl -X POST -H 'content-type: application/json' \
  -d '{"address":"0xYourAddress"}' http://127.0.0.1:8288/api/watch
curl -s 127.0.0.1:8288/ready
curl 'http://127.0.0.1:8288/api?chainid=8453&module=account&action=txlist&address=0xYourAddress'
```

`chainid` picks the chain: 8453 Base, 56 BNB Chain, 100 Gnosis. `/ready` gives, per chain,
`served_through.transactions`: the block every watched account is answered through without waiting.
`/api/watch` is accepted on a loopback bind only. To drop a chain, delete its subdirectory.

## The RPC you need

On these chains nuthatch finds an account's transactions from its nonce and balance, then traces only
the blocks where value came in. So the endpoint must serve **archive state** and
**`debug_traceBlockByNumber`**. One URL can do both; a separate trace endpoint goes in
`NUTHATCH_TRACE_RPC_<chain id>`.

- **Gnosis** works keyless: `gnosis/nuthatch.toml` names `rpc.gnosischain.com` and `rpc.gnosis.gateway.fm`,
  which both served both on 2026-11-03.
- **Base and BNB Chain** need your own node or a provider plan. On 2026-11-03 the keyless endpoints
  nuthatch bundles for them (`base.gateway.tenderly.co`, `base-pokt.nodies.app`,
  `bsc-rpc.publicnode.com`) and `mainnet.base.org` each refused or rate-limited
  `debug_traceBlockByNumber`, and publicnode refused archive state.

A chain with no RPC from either its environment variable or its `rpc_urls` stops the runtime at
start, by name.

## The trap: history starts at `start_block`

With no `start_block`, each chain backfills a watched account **from block 0**. That is complete, and
slow on chains as long as Base and BNB Chain.

Setting `start_block` in a chain's `[address_history]` skips everything before it, for every
account. A request that starts before it is refused, not answered short:

```
{"status":"0","message":"NOTOK","result":"NUTHATCH_UNSUPPORTED: history starts at block 49020000; ask from there and use another source before it"}
```

A request without `startblock` asks from block 0, so with `start_block` set a client that omits it
gets this refusal and falls through to its next source, if it has one. Set it only below the
first block your accounts were active on that chain, or leave it out and pay the backfill once.

## Checked

2026-11-03, nuthatch 5.17.0, this directory with only `gnosis/`, `start_block = 49020000` and two EOAs
watched. Both were covered to the finalized head 1 min 41 s after start, through some public-endpoint
rate limiting. `txlist` for `0x99e2…f690` over blocks
49,020,000 to 49,087,320 returned 68 rows, all outgoing, and the account's nonce moved 68 (`0xe48` to
`0xe8c`) over the same range. `tokentx` returned 68 rows:

```
$ curl -s 'http://127.0.0.1:8391/api?chainid=100&module=account&action=tokentx&address=0x99e2af1564039f80dbc61fbb6a3c4627aea8f690&startblock=49020000&endblock=49087320'
{"status":"1","message":"OK","result":[{"blockNumber":"49020674","timeStamp":"1793365085",
 "hash":"0xdfc6944c48d34b3f726d137e9ec1fb9312a0c5f15164946d282198e0dae4639e",
 "contractAddress":"0x1d1498166ddceee616a6d99868e1e0677300056f",
 "from":"0x99e2af1564039f80dbc61fbb6a3c4627aea8f690","to":"0x622bebeaa1e283b59d5f93fcf8073704e0670bbd",
 "value":"78155277190000000000","transactionIndex":"4","logIndex":"17",...},...]}
```

The full directory, with the two variables pointed at keyless endpoints, started all three chains and
`/ready` listed 56, 100 and 8453. No Base or BNB Chain history was read: those endpoints cannot trace.

## Limits

- An account with contract code, or EIP-7702 delegated code, gets `NUTHATCH_UNSUPPORTED` for
  `txlist` and `txlistinternal` from the block it had code; its token transfers are still served.
- An incoming transaction that moves no value is not listed, though Etherscan lists it.
- `getblocknobytime` is refused on BNB Chain, whose blocks can share a second.
- Your RPC provider sees which addresses you watch. The history itself stays on your machine.

Every key and action: [config reference](../../skills/nuthatch-builder/config-reference.md), under
`[address_history]`.
