# Worked example: Uniswap V3 `pricing.ts` (RFC-0038 §6a)

This is the classification the report has to get right, because it is the one that decides whether
"port the top 25 subgraphs" is grind or is impossible. Eleven of those twenty-five are Uniswap.
Read `src/common/pricing.ts` in `Uniswap/v3-subgraph`, not from memory.

The committed fixture at [fixtures/four-classes/](fixtures/four-classes/) is this example reduced
to the four classes. CI diffs the report. Horizon staking is the same shape with fewer tricks:
delegation events are exact; a current-balance fold of those events is also exact; anything the
mappings never write is unreachable.

## `getEthPriceInUSD` is exact

It loads one pool (`STABLE_TOKEN_POOL`) and returns `token1Price` (or `token0Price`), which derives
from `sqrtPriceX96` - a field carried in `Initialize` and `Swap`. No stored recursion, no write
order.

RFC-0038 §6d measured it at block 496,213,467 on Arbitrum: nuthatch `ethPriceUSD` and the subgraph
`bundle.ethPriceUSD` were identical to where their decimal type stops.

**Class: exact.** Citation: the `return usdcPool.token1Price` line. Port as a view over the latest
swap on the reference pool.

## `findEthPerToken` is fixed point

It iterates `token.whitelistPools` and, for each, reads **`token1.derivedETH` - the previously
stored value of another token's price**, alongside `pool.totalValueLockedToken1`:

```ts
const ethLocked = pool.totalValueLockedToken1.times(token1.derivedETH)
if (ethLocked.gt(largestLiquidityETH) && ethLocked.gt(MINIMUM_NATIVE_LOCKED)) {
  priceSoFar = pool.token1Price.times(token1.derivedETH as BigDecimal)
}
```

A token's price is a function of *when the other token was last written*, not of the event log
alone. Two indexers replaying the same events in a different handler order can legitimately produce
different numbers.

**Class: fixed point.** Citation: the `token1.derivedETH` line inside the loop. A nest can solve
the mutual recursion to convergence; that number is defensible and **not the subgraph's**. The
report has to say `Token.derivedETH` will not reproduce, and why.

Anything downstream that multiplies by `derivedETH` (`getTrackedAmountUSD`, `volumeUSD`,
`totalValueLockedUSD` in the USD column) inherits the class.

## `fetchTokenSymbol` / `fetchTokenDecimals` are call-derived

The mapping binds the token contract and calls `try_symbol` / `try_decimals` at the pool-creation
block. RFC-0038 §3 is exactly this: a `[[calls]]` row pinned at the triggering row's block,
`contract_column = "{token0}"`, `signature = "symbol()"`.

**Class: call-derived.** Citation: the `ERC20.bind` / `try_symbol` line. Needs `--state-rpc`.
`Pool.token0Price` stays **exact**: decimals are a scale factor, not a stored recursion.

## Horizon, in the same vocabulary

The Graph Network subgraph's `TokensDelegated` / `TokensUndelegated` / `DelegatedTokensWithdrawn`
fields are exact (the graph-staking-nest views copy them). Current delegated balances are a fold of
those events, also exact, and the hand port left them out on purpose because the nest's job was the
activity feed, not because they were unreachable. `@fulltext` on the network schema, and any
blockHandler-only stats, are unreachable.

A field the tool classifies differently from that hand port is a defect in the tool or a finding
about the hand port. Investigate. Do not average.
