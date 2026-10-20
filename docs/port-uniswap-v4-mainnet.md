# Port: Uniswap V4, Ethereum mainnet

RFC-0044 slice 3, the acceptance port (#1212). Sprint `resolute-robin`.

This is the verification record. `nest/README.md` is the *generated* classifier output and is
overwritten by any re-run of `port-emit`; this file is what was measured against a live reference,
and where the two disagree this one is the evidence.

## Subject

| | |
|---|---|
| Deployment | `Qmda2K4NcKWXB2AqyGUZEU35DgxSqFFRhkCmrJ8oC9po7i` (subgraph `EzLH76FWsZUSBTfp2P6CV7cqN56e1rSt2uWtqtsUMEeb`) |
| Chain | Ethereum mainnet, start block 21,688,329 |
| Data sources | 3: `PoolManager`, `PositionManager`, `EulerSwapFactory` |
| Templates | **none** |
| Entities / fields | 19 / 231, of which 90 are `BigDecimal` |

Chosen because a user's queries stopped returning data, not because we picked it
(nuthatch-org/graph-support#32). The canonical deployment
`QmZsgJLiLQKpb8hxTmQ5LWyrFVvfWzVaL4WK8dfFBn7EeK` answers `indexing_error`; Ellipfra's redeployment
above carries the fixes and is both the thing ported and the diff reference, because diffing a port
of one manifest against a different manifest would fail the slice for the wrong reason.

### The source had to be pinned by hand

**A deployment CID resolves to compiled WASM, not AssemblyScript**, so `port-report --dir` cannot be
aimed at a deployment. The mappings came from `Uniswap/v4-subgraph`, pinned on evidence:

- `schema.graphql` byte-identical to upstream.
- All seven ABIs identical after `jq -cS` normalisation; they differ on disk only because graph-cli
  re-serialises JSON on deploy.
- All three addresses and `startBlock`s exact against `networks.json` `mainnet`.

**Not** pinned by the mapping bytes, which cannot be compared to source. That is the one gap in the
chain of custody and it is stated here rather than glossed.

## What runs

`init --from-subgraph` → 3 contracts, 8 event tables, 0 templates. `port-emit` → 19 views, a check,
a README. Backfilled 21,688,329 → tip in about 11 hours on one Alchemy key, 39.2M events. The
adaptive fetch window fell from 14,623 blocks to ~130 as pool density rose; a rate taken from the
opening range is worthless and none is quoted here.

## What was verified

Nest frozen by stopping `dev` and starting `serve`, which owns no cursor. Reference pruning floor is
25,942,028 and does not slide. **Pinned at block 25,945,634, which is the nest's frozen `as_of`.**

| entity | reference | nest | aligned | missing | extra | field divergences |
|---|---:|---:|---:|---:|---:|---:|
| Swap (500-block window) | 6,243 | 6,243 | 6,243 | 0 | 0 | **0** |
| Pool | 132,769 | 132,816 | 132,769 | 0 | 47 | **0** |
| Token (emitted view) | 45,322 | 12,822 | 12,799 | 32,523 | 23 | n/a |

**Seven fields reproduce byte for byte** across 151,811 aligned rows: `sender`, `sqrtPriceX96`,
`tick`, `logIndex`, `createdAtBlockNumber`, `createdAtTimestamp`, `hooks`. Every field the report
called `exact` that could be asked, was exact.

**The pin must equal the nest's `as_of`, and an earlier run of this table did not.** The emitted
`token` view is `SELECT id FROM (...) GROUP BY id` with no block column, so it answers as of whatever
the nest has reached regardless of the pin. A first run had the nest at 25,945,634 and the pin at
25,943,500, which reported **27** extra tokens where there are **23**: four of them were simply created
in those 2,134 blocks. `Pool` and `Swap` carry block predicates and were unaffected. The harness now
refuses an unequal pin rather than leaving the invariant to the procedure.

The table came from `scripts/port-diff-uniswap-v4.py --block 25945634 --window 500` against a nest
under `serve` frozen at that block. The script was removed on 2026-10-20 (#2294) and lives in git
history at `14187c7a`: the reference has since pruned past the pin (on 2026-10-20 it held data from
block 26,230,124 only), so the table can no longer be reproduced as it was. Querying the reference
needs **no credential**: `https://api.thegraph.com/subgraphs/id/<CID>` answers keyless. It reports
an auth failure, and a pruned-block refusal, as HTTP **200** with the error in the body, so a client
gating on status reads either as an empty result.

## What did not reproduce

The part that matters. Ordered by how much a migrating team would care.

1. **Every USD- and ETH-denominated figure.** All 90 `BigDecimal` fields. `Bundle.ethPriceUSD` and
   `Token.derivedETH` are order-dependent folds over prior entity state (`findNativePerToken` walks
   other tokens' `derivedETH`), and no view carries either. The report named 22 of the 90 as fixed
   point and called the other 68 exact; it is wrong about 68 of them (#1274).
2. **Every interval entity.** `UniswapDayData`, `PoolDayData`, `PoolHourData`, `TokenDayData`,
   `TokenHourData` serve nothing.
3. **`PoolManager`, `Bundle`, `Transaction`, `Tick` serve nothing.** Nine of nineteen views are
   `CREATE VIEW x AS SELECT 1 AS port_placeholder` (#1277).
4. **Token metadata.** `symbol`, `name`, `decimals`, `totalSupply` need `eth_call` and no `[[calls]]`
   was emitted. Consequently `Swap.amount0`/`amount1` are unobtainable too: the raw `int128` is in the
   table, but `convertTokenToDecimal` needs `decimals`.
5. **72% of `Token` rows.** The view projects `currency0` only, and answers 12,822 against the
   reference's 45,322 at the same pin. The nest *holds* 45,347 distinct `currency0 ∪ currency1` values,
   so this is the overlay losing rows rather than the indexer missing them (#1277).
6. **`Pool` is unusable as an entity.** The view is a `UNION ALL` of the Initialize and Swap tables
   with no key: 35,142,058 rows for 132,765 pools, and `Pool.id` is not in it at all (#1277).

### And one thing that did not reproduce in the user's favour

**The port gains 47 pools the subgraph silently drops**, and 23 tokens with them - the `currency0`
values of pools the reference never created. `handleInitialize` does a bare `return`
before `pool.save()` when `fetchTokenDecimals` is null (`poolManager.ts:81`, `:107`). I called
`decimals()` on every currency of all 47: **42 have one that will not answer**, and the addresses say
why - `0x833589fc…2913` is USDC on Base, `0x42000000…0006` is WETH on the OP Stack, and
`0xeeee…eeee` accounts for eleven. Pools opened against addresses holding no contract on mainnet.
Two of the 47 are unexplained and are left open on #1212.

This is the same mechanism as the outage that chose the subject: the canonical deployment aborts on
exactly such a pool, and the price of the fixed deployment surviving is that it holds fewer pools
than the chain does.

The classifier has no class for this. It is not a field that fails to reproduce, it is a row the
reference never writes, and `classes.md` has no vocabulary for "the mapping returns early and the
entity is never created".

## Verdict against §10

**"The port runs."** Passes.

**"The report is right about itself."** Fails, and the diff is not what decides it.

- **Coverage.** The emitted views answer **27 of 231** fields. The report called **205** `exact`. 178
  `exact` fields cannot be asked, so they cannot match byte for byte.
- **An unpredicted divergence.** The 47 pools. §10 fails a slice on one.
- **Where it could be checked, it was right.** Seven of seven. The faults are in which assignment the
  classifier reads (#1274) and in what the overlay renders (#1277), not in its notion of `exact`.

## Open

- #1274 classifier: initialiser and dead-handler citations.
- #1277 overlay: placeholder views, a check that cannot fail, the `token` row loss, `Pool.id`, the
  `pool` union.
- The 2 unexplained extra pools.
- A class for "the handler returns early and the entity is never written".
- Historical time travel was never compared: the reference prunes below 25,942,028. If that matters,
  `ephemeris-node` can index the same deployment unpruned with the real mappings, which would also
  settle whether the reference's impossible `PoolManager.totalValueLockedUSD` of
  5,551,102,598,968,110,768,763 is the mapping's arithmetic or its indexer's. Both `minus`/`plus`
  pairs in the mapping are balanced, so the obvious explanation is not the cause. Deferred by the
  board until a diff asked for it; it has not.
