# Port report

Every entity field in `schema.graphql`, classified against the mappings. Nothing has been scaffolded. A field this report calls exact must match byte-for-byte; a field it names as fixed point, call-derived or unreachable will not reproduce, and the reason is on that row. An unpredicted divergence is a defect in this report.

Source: `four-classes`

Path: `--from-subgraph`. The proxy trap does not apply: the manifest pins implementation ABIs.

## Summary

| Class | Count |
|---|---:|
| exact | 15 |
| call-derived | 2 |
| fixed point | 2 |
| unreachable | 5 |

## exact

A pure function of decoded events. Port as a view or entity; byte-identical.

| Field | Citation | Why |
|---|---|---|
| `Bundle.id` | `src/mappings/core.ts:38` | assigned from `'1'` |
| `Pool.id` | `src/mappings/core.ts:24` | assigned from `event.params.pool.toHex()` |
| `Pool.liquidity` | `src/mappings/core.ts:30` | assigned from `ZERO_BI` |
| `Pool.sqrtPrice` | `src/mappings/core.ts:27` | assigned from `ZERO_BI` |
| `Pool.swaps` | `schema.graphql:24` | `@derivedFrom(field: "pool")` - reverse lookup, a SQL join |
| `Pool.token0` | `src/mappings/core.ts:25` | assigned from `token0.id` |
| `Pool.token0Price` | `src/mappings/core.ts:28` | assigned from `ZERO_BD` |
| `Pool.token1` | `src/mappings/core.ts:26` | assigned from `token1.id` |
| `Pool.token1Price` | `src/mappings/core.ts:29` | assigned from `ZERO_BD` |
| `Pool.totalValueLockedToken0` | `src/mappings/core.ts:31` | assigned from `ZERO_BD` |
| `Swap.amount0` | `src/mappings/core.ts:58` | assigned from `event.params.amount0` |
| `Swap.id` | `src/mappings/core.ts:56` | assigned from `event.transaction.hash.toHex() + '-' + event.logIndex.toString()` |
| `Swap.pool` | `src/mappings/core.ts:57` | assigned from `pool.id` |
| `Swap.timestamp` | `src/mappings/core.ts:59` | assigned from `event.block.timestamp` |
| `Token.id` | `src/mappings/core.ts:12` | assigned from `event.params.token0.toHex()` |

## call-derived

Reads contract state at the row's block. Port as `[[calls]]` (RFC-0038 §3). Needs `--state-rpc`.

| Field | Citation | Why |
|---|---|---|
| `Token.decimals` | `src/common/token.ts:18` | `handlePoolCreated` reads contract state; assigned from `fetchTokenDecimals(event.params.token0)`; assigned at `src/mappings/core.ts:14` |
| `Token.symbol` | `src/common/token.ts:5` | `handlePoolCreated` reads contract state; assigned from `fetchTokenSymbol(event.params.token0)`; assigned at `src/mappings/core.ts:13` |

## fixed point

Reads back stored entity output. A nest can converge; the number will be different. This field will not reproduce.

| Field | Citation | Why |
|---|---|---|
| `Bundle.ethPriceUSD` | `src/mappings/core.ts:40` | `handleSwap` reads stored entity output (`getEthPriceInUSD()`); a nest can converge, this will not reproduce |
| `Token.derivedETH` | `src/mappings/core.ts:53` | `handleSwap` reads stored entity output (`findEthPerToken(token0 as Token)`); a nest can converge, this will not reproduce |

## unreachable

Will not be ported. This field will not reproduce.

| Field | Citation | Why |
|---|---|---|
| `BlockStat.blockNumber` | `src/mappings/core.ts:65` | written from blockHandler `handleBlock`; nuthatch indexes logs |
| `BlockStat.id` | `src/mappings/core.ts:64` | written from blockHandler `handleBlock`; nuthatch indexes logs |
| `Token.name` | `schema.graphql:33` | no mapping writes this field |
| `Token.whitelistPools` | `schema.graphql:32` | no mapping writes this field |
| `_Schema_.tokenSearch` | `schema.graphql:3` | `@fulltext` search index `tokenSearch` |

## Traps

These are the ones the three hand ports paid for, and they apply on this path:

- The proxy trap applies to `nuthatch init 0xAddr` and **not** to `--from-subgraph`. This report is the latter.
- `[[factories]] watch` takes a contract alias or template name, never an address, whatever `config-reference.md` shows.
- One proxy may need several ABIs across its history. Horizon renamed every staking event; a nest carrying only the current ABI loses 366 million blocks silently.
- The snake_caser explodes acronyms: `ServiceURIUpdate` becomes `service_u_r_i_update`.
- Verify against the chain (`cast call` on the canonical getters), not the gateway. On-chain sentinels (`deactivationRound = 2^256-1`) map to a view's `null`.
