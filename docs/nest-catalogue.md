# Prebuilt nest catalogue - which protocols to ship first

**What this is:** a prioritised, tiered plan for the prebuilt *nests* nuthatch should ship - the
packaged indexing definitions (ABIs + decoded event tables + declarative entity views) that let an
operator replace a rented Graph/Goldsky/Alchemy subgraph with a local, self-hosted equivalent. The
thesis of "be your own indexer" is only as strong as the catalogue of things people actually want
indexed on day one.

**What this is not: a status board.** This document ranks *candidates*. What is actually published
lives in the [nests index](https://github.com/nuthatch-org/nests), which is the single source of
truth for what an operator can run today. Read a tier here as "how hard we expected this to be",
never as "this has not shipped" - as of 2026-08-04 the index lists Graph Horizon, Uniswap V3,
Uniswap V4, Livepeer and POA as 🟢 available, and two of those are not ranked in this file at all.
Keeping a plan and a status board in one document is what let a shipped nest sit in Tier 3 for weeks.

**Scope:** EVM-only - Ethereum mainnet + major L2s (Arbitrum, Optimism, Base, Polygon), per
[CLAUDE.md](../CLAUDE.md): non-EVM stays out of scope until EVM is airtight, except that a named user
asking for a named chain is a decision for Chief. None is built.

**Provenance:** ranking and figures come from a fact-checked research pass (fan-out web search →
adversarial verification, 71/75 claims survived a 2-of-3-refute gate) over The Graph's own network
stats, Messari's *State of The Graph*, the subgraph registry, provider catalogues (Goldsky, Alchemy,
Chainstack), and per-protocol subgraph/contract docs. Dated **2026-07-19**. Sources listed at the
foot. Query-volume figures are cited as of the source date, not re-measured here.

---

## The demand signal (why this ordering)

Indexing demand is not evenly spread - it's brutally concentrated by protocol *category*, and within
each category by a handful of canonical protocols. The hard numbers:

| Category | Live subgraphs | Canonical protocols (the names that soak up the demand) |
|----------|---------------:|----------------------------------------------------------|
| **DEX** | **4,176** | Uniswap (v2/v3/v4), Sushiswap, Curve, Balancer, PancakeSwap |
| **Lending** | **1,424** | Aave, Compound (v2/v3), MakerDAO/Sky, Morpho, Spark, Venus |
| **Staking / LST** | **867** | Lido, Rocket Pool, EigenLayer, Frax-ether, Convex |
| **Bridge** | **771** | Native L2 bridges (OP, Arbitrum, Base), Across, Hop |
| **NFT Marketplace** | **436** | OpenSea (Seaport), Blur, Rarible |
| **Governance** | **416** | (per-protocol - Compound/Aave/Maker governor forks) |
| **Yield Aggregator** | **387** | Convex, Yearn, Beefy |
| **Perpetuals** | **266** | GMX, dYdX-class |
| **Name Service** | **223** | ENS, Space ID, Unstoppable Domains |
| **Options** | **179** | - |

*Source: Messari subgraph classification / State of The Graph. DeFi as a whole accounts for ~11,218
of the classified subgraphs; 6,000+ are live on the decentralized network, 14,700+ classified in the
registry.*

Two facts that shape strategy more than the raw table:

1. **The demand is real and growing.** The Graph Network served **991M queries in a trailing 30 days
   (11× year-on-year)** and **1.27T cumulative queries to 75,000+ projects** as of early 2026. This
   isn't a dead standard we're cloning - it's a live, paid market for exactly the data a nest emits.
2. **Forks/templates multiply reach.** Messari's standardized schemas are explicitly organised as
   `aave-forks`, `compound-forks`, `gmx-forks`, `balancer-forks` - **one schema serves many deployed
   instances of the same design.** For nuthatch this is the whole game: build the *template* nest once,
   ship it parameterised across every fork and every chain. A good nest is a family, not a singleton.

---

## What makes a GOOD vs POOR nest candidate

Before the tiers, the rubric that produced them. This is the reusable filter for any future candidate.

### Good nest candidate
- **High, proven indexing demand** - sits in a top category *and* has a canonical/official subgraph
  with real downstream consumers (frontends, dashboards, aggregators like DefiLlama/Dune/Messari).
- **Factory or template pattern** - the protocol deploys child contracts (Uniswap pools) or is
  forked widely. This is nuthatch's home turf: first-class factory/dynamic-contract discovery
  ([RFC-0009](rfcs/0009-factory-and-dynamic-contract-discovery.md)) means one nest tracks thousands of
  pools; fork-templating means one nest serves many protocols.
- **Event-rich but deterministically decodable** - the state that matters is carried in logs
  (`Swap`, `Transfer`, `Supply`), so a topic0-keyed decode reconstructs it. No trace/state-diff
  dependency.
- **Stable, long-lived, widely-deployed contracts** - the Uniswap V3 factory has run unchanged for
  ~5 years across every major chain. One nest × N chains is free reach.

### Poor nest candidate (or: build later, carefully)
- **Needs traces or state diffs** - anything whose canonical state isn't in events (internal calls,
  raw storage). nuthatch defers firehose-class extraction until the reth ExEx path lands
  ([RFC-0014](rfcs/0014-firehose-class-extraction-traces-and-state.md), backlog-gated on a node).
  Don't ship a nest that silently needs `debug_*`.
- **Churning upgradeable proxies** - `init` resolves an EIP-1967 (and legacy OpenZeppelin or beacon)
  proxy to its current implementation's ABI and warns that history before the last upgrade may not
  decode against it. A bespoke proxy, or events that only an earlier implementation emitted, still
  need declaring by hand.
- **Value lives in `eth_call` state, not logs** - e.g. a price that's only readable via a view
  function. That is a declared `[[calls]]` read pinned to a block, so it stays deterministic.
- **Heavy off-chain math presented as "the data"** - APR→APY, USD pricing from `sqrtPriceX96`. Not
  disqualifying (Aave and Uniswap both need it) but it's real view-layer work - grade the complexity
  honestly rather than pretending it's a Transfer log.

---

## Tier 0 - the beachhead: Graph Protocol infrastructure

**A different axis from the tiers below.** Tiers 1-3 rank on *aggregate* indexing demand across the
whole ecosystem. Tier 0 ranks on *acute* demand from the audience nuthatch can reach first: **Graph
Protocol indexers and operators** (the GraphOps / Lodestar orbit -
[RFC-0002](rfcs/0002-horizon-nest.md), [RFC-0011](rfcs/0011-graph-network-nest-lodestar-migration.md)).
Smaller headcount than "everyone who wants a Uniswap dashboard", but the pain is on the operational
critical path and - the crux - **indexers pay real GRT query fees to read this data off the
decentralized network today.** A self-hosted, deterministic, GRT-free equivalent is a "shut up and
take my money" pitch to exactly the people nuthatch is already talking to. This is the beachhead; the
DeFi tiers are the broad market that comes after.

Post-Horizon this got *easier*, not harder: Horizon upgraded staking **in place** (same proxy
address), so the protocol is now a **stable, defined contract set on Arbitrum One** - the textbook
"good candidate" shape.

### 0.1 The network subgraph - the crown jewel
- **Why:** every indexer's `indexer-agent` / `indexer-service` reads it continuously to associate
  deployment IDs with active allocations and drive allocation/routing decisions. It is the single
  most operationally-load-bearing subgraph in the protocol, and reading it on the decentralized
  network costs GRT. This is the nest that lands the first hundred users who actually pay attention.
- **Footprint (Arbitrum One):** a defined multi-contract set - nuthatch already **ships and has
  verified live** the core:
  - `HorizonStaking` `0x00669A4CF01450B64E8A2A20E9b1FCB71E61eF03` (the in-place-upgraded staking proxy)
  - `SubgraphService` `0xb2Bb92d0DE618878E438b55D5846cfecD9301105` (owns the allocation lifecycle now)
  - `StakingExtension` `0x3bE385576d7C282070Ad91BF94366de9f9ba3571` (legacy allocation/delegation history)
  - plus `RewardsManager`, `EpochManager`, `L2Curation`, `L2GNS`, and the Horizon payments layer
    (`GraphPayments`, `PaymentsEscrow`, `GraphTallyCollector`) and `DisputeManager` - pin these from
    the `graphprotocol/contracts` `addresses.json` rather than trusting a half-remembered hex.
- **Events:** provisions (`ProvisionCreated/Increased/Thawed/Slashed`, `ThawRequestCreated/Fulfilled`),
  delegation (`TokensDelegated/Undelegated`, `DelegationSlashed`), allocations (`AllocationCreated/
  Resized/Closed`), collection (`IndexingRewardsCollected`, `QueryFeesCollected`), curation
  (`Signalled`/`Burned`), GNS (`SubgraphPublished`, `SignalMinted/Burned`), rewards
  (`HorizonRewardsAssigned`), epochs (`EpochRun`), disputes.
- **Complexity:** 🟠 **Medium** - events are *sparse* (a few per minute; comfortably inside the RAM
  budget, per RFC-0002 acceptance) and decode cleanly, but the value is in the **derived economic
  views**: effective delegation cut, realized APR, per-epoch fee/reward aggregation. This is the
  strongest possible governed-semantic-layer target ([RFC-0016](rfcs/0016-governed-semantic-layer-and-agent-grade-mcp.md)).
- **Status:** shipped as `horizon-nest` (RFC-0002, live on Arbitrum) - staking, service, delegation.
  The remaining network-subgraph surface (Indexer Directory, Curation, Epochs; the Lodestar migration,
  RFC-0011) **extends `horizon-nest`** rather than forking a second nest - the earlier
  `graph-network-nest` was a byte-identical clone and has been retired.

### 0.2 EBO - the Epoch Block Oracle
- **Why:** posts the canonical per-epoch start block for every indexed chain, so all indexers close
  multichain allocations against a consistent reference and produce comparable POIs. Core plumbing -
  indexers can't operate multichain without it (`graphprotocol/block-oracle`, GIP-0038).
- **Footprint / mechanism:** data is posted **on-chain as calldata** to a **DataEdge** contract
  (GIP-0025); on Arbitrum an `EventfulDataEdge` variant **emits events** rather than relying on
  traces. The **Epoch Subgraph** decodes the calldata payloads.
- **Complexity:** 🟠 **Medium** - content work on the shipped calldata decoder: `[extract]
  top_level_calls` decodes transactions sent directly to the nest's contracts over ordinary RPC. What
  EBO adds is decoding the DataEdge payload itself, which is EBO-specific.

### 0.3 Gateway QoS / Rewards-Eligibility Oracle - watch the shape settle
- **Why:** indexer quality-of-service (latency, availability, blocks-behind) feeds the gateway's
  Indexer Selection Algorithm and, in Horizon, **gates indexing-reward eligibility** - so indexers
  care intensely about it.
- **Fit caveat (the honest one):** this is the least "pure" of the three. The canonical data
  *originates off-chain* (gateway telemetry), and the Horizon-era on-chain mechanism has shifted to
  the **Rewards Eligibility Oracle** (REO, GIP-0079) - a dedicated oracle contract `RewardsManager`
  checks at claim time - rather than a clean DataEdge feed. The legacy QoS-oracle subgraph decoded
  DataEdge calldata + IPFS; the on-chain surface here is still consolidating. nuthatch decoding the
  postings is perfectly deterministic (you decode a poster's claims, you are not the oracle) - just
  be clear-eyed that it's a different trust shape, and that this one is a *watch-and-follow* target,
  not a stable one yet. Matches RFC-0011's own open question.
- **Complexity:** 🟠 **Medium-High**, and the most likely to move under you.

**Tier-0 sequencing:** network subgraph first (largely shipped as `horizon-nest`; extend it with the
remaining surface), EBO second (on the shipped `top_level_calls` decoder), QoS/REO third (let the
on-chain shape settle).
Residual friction to be *deliberate* about, not accidental: positioning self-hosting-away-from-paid-
queries with GraphOps - it's Lodestar-aligned and complementary, but it's the CLAUDE.md/Horizon
tension, so frame it on purpose.

## Tier 1 - build first (the launch set)

The five that maximise *demand × fit × demo value*. This is the set that makes the "replace your
subgraph in two minutes" pitch land.

### 1. ERC-20 stablecoins - USDC / USDT / DAI *(category: stablecoin / token)*
- **Why:** universal, the single most-queried token surface, and the perfect **<2-minute demo** -
  the footprint CI gate already indexes USDC. Every wallet, dashboard, and accounting tool wants
  clean transfer/holder data.
- **Footprint:** fixed canonical contracts per chain; deployed on *every* chain. Not a factory.
- **Events:** `Transfer`, `Approval`. That's it.
- **Complexity:** 🟢 **Trivial** - this is the generic ERC-20 decode already in the core. Ship as a
  templated nest with holder-balance and supply views. Instant credibility, near-zero build cost.

### 2. Uniswap V3 - the flagship factory nest *(category: DEX)*
- **Why:** DEX is the #1 category by a mile (4,176 subgraphs); Uniswap is *the* canonical example of
  a complex DEX index, relied on by its own UI, DefiLlama, Dune, and countless analytics apps. This
  nest is the showcase for nuthatch's factory discovery.
- **Footprint:** **factory pattern.** `UniswapV3Factory` at
  `0x1F98431c8aD98523631AE4a59f267346ea31F984` (mainnet, ~5 yrs stable) emits `PoolCreated`, spawning
  per-pool child contracts. Plus `NonfungiblePositionManager`
  (`0xC36442b4a4522E871399CD717aBDD847Ab11FE88`). Deployed across mainnet + all major L2s (per-chain
  subgraphs).
- **Events:** factory `PoolCreated`; per-pool `Initialize`, `Swap`, `Mint`, `Burn`, `Flash`,
  `Collect`; NPM `IncreaseLiquidity`, `DecreaseLiquidity`, `Collect`, `Transfer`.
- **Complexity:** 🔴 **High** - stateful tick + liquidity accounting, `sqrtPriceX96` fixed-point price
  math, USD pricing via oracle references, TVL/fee-APR derived fields. This is the nest that *proves*
  declarative views (DBSP) earn their keep - and the one to get golden-tested hardest.

### 3. Aave V3 - the lending flagship *(category: lending)*
- **Why:** Lending is category #2 (1,424 subgraphs). Aave ships **official** subgraphs
  (`aave/protocol-subgraphs`) across **20+ chains** including all our targets; cross-protocol lending
  analytics ("compare USDC borrow rates everywhere") is a marquee downstream use case.
- **Footprint:** `Pool V3` at `0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2` (mainnet) + reserve
  configuration. Not a classic factory but a well-defined multi-contract set; **fork-templatable**
  (`aave-forks` covers Spark, etc.).
- **Events:** `Supply`, `Borrow`, `Repay`, `LiquidationCall`, `UsageAsCollateral`, `SwapBorrowRate`,
  `RedeemUnderlying` + reserve/user-position entities.
- **Complexity:** 🟠 **Medium** - events decode cleanly, but faithful rates need off-chain interest
  accrual + APR→APY math (the `aave-utilities` logic). A good exercise for the view layer; the raw
  event tables are useful even before the derived rates land.

### 4. ENS - the archetypal name service *(category: name service)*
- **Why:** repeatedly named (with Uniswap and NFTs) as *the* archetype driving community subgraph
  creation. Name→address resolution is a near-universal frontend dependency.
- **Footprint:** multi-contract - ENS **Registry** + **Base Registrar Controller** (registrant vs
  controller/manager distinction). Mainnet-centric.
- **Events:** registration events → `registrations` (with `expiryDate`, `domain`) and `domain`
  entities; resolver/record changes.
- **Complexity:** 🟠 **Medium** - no heavy math, but multi-contract joins and the
  registrant/controller model need care. High recognisability per unit effort.

### 5. Generic ERC-721 / ERC-1155 NFT nest *(category: NFT)*
- **Why:** NFT marketplaces (436) plus the enormous long tail of collections. A *generic* standards
  nest indexes any collection from its address - huge surface, one build.
- **Footprint:** any ERC-721/1155 contract; user supplies the address (the `init 0xAddr` flow).
- **Events:** ERC-721 `Transfer`, `Approval`, `ApprovalForAll`; ERC-1155 `TransferSingle`,
  `TransferBatch`, `URI`.
- **Complexity:** 🟢 **Low** - standard interfaces, generic decode. Ownership/holder and
  transfer-history views. Pairs naturally with the stablecoin nest as the "any token" pair.

---

## Tier 2 - high value, second wave

Strong demand, more build cost, or dependent on Tier-1 templates landing first.

- **Uniswap V2 + Sushiswap** *(DEX)* - simpler constant-product AMM (`Swap`/`Mint`/`Burn`/`Sync`,
  `PairCreated` factory). Sushi is a V2 fork → **the template-reuse proof**: one AMM-v2 nest, two
  protocols, many chains. 🟠 Medium (much simpler than V3).
- **Compound V2 + V3** *(lending)* - canonical alongside Aave; `compound-forks`/`compound-v3`
  standardized schemas exist. cToken/Comet event model. 🟠 Medium.
- **Lido** *(LST)* - the staking category leader (867 subgraphs); stETH/wstETH across 8+ chains,
  integrated into 10+ DeFi protocols; 18+ maintained Dune dashboards = heavy analytics demand.
  Multi-contract, event-rich (deposits, withdrawals, validator exits, oracle/APR reporting).
  🟠 Medium-High.
- **Curve** *(DEX/stableswap)* - canonical DEX; `curve-finance` standardized schema. StableSwap
  invariant math makes pricing non-trivial. 🔴 High.
- **Seaport / OpenSea** *(NFT marketplace)* - the #1 marketplace protocol; `OrderFulfilled`-class
  events. Complex order structs but pure log decode. 🟠 Medium.
- **Balancer** *(DEX)* - canonical; `balancer-forks` template; weighted/vault architecture.
  🔴 High.

---

## Tier 3 - specialist / nice-to-have

Real demand but narrower, more complex, or blocked on capabilities nuthatch hasn't shipped yet.

- **Uniswap V4** *(DEX)* - **shipped.** Published as
  [`uniswap-v4-ethereum`](https://github.com/nuthatch-org/uniswap-v4-ethereum) and 🟢 available in
  the [nests index](https://github.com/nuthatch-org/nests). The singleton `PoolManager` + hooks
  pattern turned out to *suit* nuthatch rather than block it: no factory, no per-pool contracts, ten
  event tables off one address. Left in this tier only as the record of a prediction that was wrong -
  it was rated 🔴 High difficulty and Tier 3 here, and it landed anyway.
- **EigenLayer** *(restaking)* - high narrative demand; `eigenlayer` standardized schema exists.
  Operator/staker delegation event model. 🟠 Medium-High.
- **GMX** *(perps)* - perpetuals category leader; `gmx-forks` template. Position/PnL accounting.
  🔴 High.
- **MakerDAO / Sky** *(CDP/stablecoin)* - canonical CDP; complex multi-contract (vat/ilk). 🔴 High.
- **Rocket Pool, Frax-ether, Convex** *(LST/yield)* - round out staking/yield; fork-templatable.
  🟠 Medium.
- **Blur** *(NFT marketplace)* - trader-focused marketplace, strong on mainnet. 🟠 Medium.
- **Morpho, Spark, Silo, Venus** *(lending)* - the lending long tail; largely `aave`/`compound`-fork
  templatable once those Tier-1/2 templates exist. 🟠 Medium.
- **Safe (Gnosis Safe) wallets** *(infra)* - factory-deployed proxies; useful for wallet analytics
  but value is partly in `eth_call` state. 🟠 Medium.
- **Native bridges (OP / Arbitrum / Base)** *(bridge)* - bridge is a big category (771) but deposits
  /withdrawals often lean on L1↔L2 message state; grade carefully against the traces limitation. 🟠.
- **Chainlink price feeds** *(oracle)* - `AnswerUpdated` aggregator events; useful as a **pricing
  input** to other nests (Uniswap/Aave USD views) more than a standalone product. 🟢 Low-Medium.

---

## Strategic notes for sequencing

- **Lead with the template families, not one-offs.** AMM-v2 (Uniswap V2 → Sushi), lending
  (Aave-forks, Compound-forks), and the generic token/NFT standards each turn one build into many
  shipped nests across forks and chains. Prioritise nests that *factor*.
- **Tier 1 is a deliberately mixed difficulty curve:** two 🟢 trivial wins (stablecoins, NFT
  standards) that demo instantly and exercise the generic decode already in core, framing two 🔴/🟠
  showcases (Uniswap V3, Aave) that prove factory discovery and declarative views. Ship the easy pair
  first for momentum; land the flagship pair for credibility.
- **Respect the capability fence.** Every Tier-1/2 nest here is pure-event-decodable - no nest should
  ship needing traces/state-diffs (deferred, RFC-0014), or history from a proxy implementation era
  `init` did not resolve, until that is declared. When a candidate needs them, that's a Tier-3-or-later signal, not a "make it work
  somehow."
- **This dovetails with the Horizon nest work.** RFC-0002 (`horizon-nest`) and the Lodestar migration
  (RFC-0011) already prove the "publish a real nest" path; this catalogue is the demand-ranked list of
  *which nests come next*. It ranks **content**, not engineering work: when a nest here is actually
  picked up, it becomes an issue like anything else, and the
  [issue queue](https://github.com/nuthatch-org/nuthatch/issues) stays the single source of truth for
  what is being built.

---

## Sources

Primary sources behind the figures and rankings (all consulted 2026-07-19):

- The Graph - *6,000+ Subgraphs Live on Network* (AAVE, Balancer, ENS, Lido, SushiSwap named)
- Messari - *State of The Graph* Q2/Q4 2025 (category subgraph counts, query volume, DeFi share)
- *subgraph-registry* - agent-friendly classification of 14,700+ / 15K+ subgraphs
- Messari **Standardized Subgraphs** - canonical per-category schemas (aave-forks, compound-forks,
  curve-finance, lido, eigenlayer, gmx-forks, balancer-forks, convex-finance, frax-ether-staking, …)
- Uniswap Developers - *Subgraphs Overview* (v2/v3/v4 official); *v3-new-chain-deployments*
- Uniswap/v3-subgraph (DeepWiki) - event mapping, factory→pool pattern, tick/price math
- Etherscan - *Uniswap V3: Factory* `0x1f98…f984`; *Aave: Pool V3* `0x8787…4fa4e2`
- `aave/protocol-subgraphs` - official Aave V2/V3 subgraphs, 20+ chains; `aave-utilities` (rate math)
- ENS Docs - *Subgraph* (registry + base registrar controller, registration entities)
- Lido - official Dune dashboard catalogue (18+ maintained dashboards)
- Provider catalogues - Goldsky (Mirror / subgraph-as-source), Alchemy dapp directory, Chainstack
  *Top hosted subgraph platforms 2026*; *Querying 90 DeFi Lending Protocols with one GraphQL query*
- enviodev/uniswap-v4-indexer - V4 singleton/hooks pattern reference
- DefiLlama - cross-protocol TVL/analytics (downstream-consumer evidence)

**Tier 0 (Graph Protocol infrastructure)** - added 2026-07-19:

- nuthatch [RFC-0002](rfcs/0002-horizon-nest.md) & [RFC-0011](rfcs/0011-graph-network-nest-lodestar-migration.md)
  - the shipped `horizon-nest`; source of the vendored, live-verified Arbitrum addresses used above.
  RFC-0011's remaining surface (Indexer Directory, Curation, Epochs) **extends `horizon-nest`** - it
  said "planned `graph-network-nest`" until 2026-08-15, contradicting the status block above, which
  has recorded since July that that nest was a byte-identical clone and was retired
- `graphprotocol/contracts` - `packages/horizon/addresses.json`, `packages/subgraph-service/addresses.json`
  (pin the payments-layer + dispute addresses here)
- `graphprotocol/graph-network-subgraph` (`master`) - the network subgraph manifest + Horizon event
  signatures; docs at thegraph.com/docs (contracts table, Graph Horizon overview, indexing overview)
- `graphprotocol/block-oracle` + forum GIP-0038 (Epoch Block Oracle) and GIP-0025 (DataEdge)
- GIP-0079 + `graphprotocol/rewards-eligibility-oracle` (REO); `edgeandnode/gateway` (off-chain QoS / ISA)

*Tiers 1-3 generated from a fact-checked deep-research pass (71 of 75 verified claims survived
adversarial review); figures reflect source dates (2025-early 2026) and are not independently
re-measured. Tier 0 cross-checks a focused web verification (July 2026, post-Horizon) against
nuthatch's own live-verified RFC-0002 addresses - the two core contracts (HorizonStaking,
SubgraphService) matched both sources; the newer payments-layer addresses should be pinned byte-for-
byte from `addresses.json` before use.*
