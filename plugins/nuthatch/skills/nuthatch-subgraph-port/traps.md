# Known traps (RFC-0044 §5c)

Earned on the horizon, livepeer and lodestar hand ports. Each one cost real hours. The skill must
carry them, because the advice inverts depending on which `init` path you are on.

## 1. The proxy trap does not apply to `--from-subgraph`

`nuthatch init 0xAddr` resolves the ABI from Sourcify / Etherscan. For a beacon proxy that returns
the *proxy* ABI, which declares none of the events the implementation emits. That is the proxy trap.

`--from-subgraph` is the other path. The manifest pins implementation ABIs by CID, which is the
entire reason the importer prefers them. **Say which path you are on.** This skill is always
`--from-subgraph`. Do not "helpfully" re-resolve the ABI from Sourcify on top of a manifest.

## 2. `[[factories]] watch` takes an alias or template name, never an address

```toml
[[factories]]
watch = "factory"        # ALIAS of the watched [[contracts]] (or a template, for nesting)
event = "PoolCreated"    #   - NOT an address
```

`config-reference.md` shows the same. An address in `watch` will not match.

## 3. One proxy may need several ABIs across its history

Horizon renamed every staking event. A nest carrying only the current ABI loses 366 million blocks
silently. When the mappings or the manifest name more than one ABI for the same address (legacy
`Staking` plus `HorizonStaking`, a renamed event list), keep them all. S2 wires that; S1's report
should already have noticed if a mapping still handles the old names.

## 4. The snake_caser explodes acronyms

`ServiceURIUpdate` becomes `service_u_r_i_update`. Cosmetic, and it will be the first thing anyone
notices. `nuthatch init --from-subgraph` uses this aliaser (`src/subgraph_import.rs::to_alias`).
Do not "fix" the names in the report; they are what the nest will actually emit.

## 5. Verify against the chain, not the gateway

The decentralised gateway needs auth and may be refusing the deployment anyway. `cast call` on the
canonical getters is ground truth the subgraph itself reads. On-chain sentinels
(`deactivationRound = 2^256-1`) map to a view's `null`.

A gateway diff (RFC-0038 §6b) is still the acceptance method for a finished port. It is not the
source of truth for a field this report already called fixed point: that field is *supposed* to
differ.
