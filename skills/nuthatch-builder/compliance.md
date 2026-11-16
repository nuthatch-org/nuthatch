# nuthatch labels and flags

Only relevant when the user asks for labels / flags / exposure / an audit record (RFC-0008). Amounts
throughout are i128 base units, returned as decimal strings. Flags are authoritative in
[cli-reference.md](cli-reference.md).

## The pieces, and when each applies

| Feature | What it does | Drive it with |
|---|---|---|
| **Labels** | Content-addressed sets of tagged addresses (the annotation substrate). | `nuthatch labels import <file>` / `labels list` |
| **Flags** | `threshold` (single transfer ≥ N) and `velocity` (windowed volume) flags. Configured in `nuthatch.toml` `[flags]`. | query the `flags` MCP tool / `/flags` |
| **Exposure** | An address's direct counterparty exposure to the labeled set. | the `exposure` MCP tool / `/exposure/{addr}` |
| **Pack** | Build/sign/verify a manifest of the registry hash, flag thresholds and alert sinks (ed25519). | `nuthatch pack keygen` → `pack build --key …` → `pack verify --pubkey …` |
| **Audit** | `report` summarises the threshold flags in a block range. | `nuthatch audit report --from … --to … --json` |
| **As of a time** | Where the nest stood at a time (`block`, `next_block`, `exact`, `final`), and any SQL answered over the facts as they stood at a block or time. | `/at?time=<unix>` or the `at` MCP tool; `at_block` / `at_time` on `/sql` |
| **Issuer recipes** | For a FiatToken-style stablecoin (Blacklisted, UnBlacklisted, Mint, Burn): `frozen` (blacklisted now, with balances), `outstanding` (supply less frozen and `treasury`-labelled balances, as an attestation counts it; needs a label set) and `mint_burn_daily` (minted and burned per UTC day beside the Mint and Burn events, with supply). Views, so `at_time` on `/sql` gives supply and freezes at a report date; the treasury label set is the one imported now, so import it as it stood then. | `nuthatch recipe add frozen` (and `outstanding`, `mint_burn_daily`) |
| **Evidence** | One answer as of a sealed block, with the nest's NID and registry hash, the rows and their hash, optionally signed with the pack key. Anyone with a nest of the same NID re-asks it and gets match or the field that differs. Refuses an unsealed block. | `nuthatch evidence make "<SELECT>" --at-time <unix> [--key k] --out b.json`, then `nuthatch evidence verify b.json --pubkey <key>` |

Live sanctions screening and the WASM screening component were removed on 2026-10-08. A
`nuthatch.toml` that still declares `[screening]` is refused at load; delete the table.

Do not present this output as legal/regulatory advice - it is a deterministic annotation layer, not
a compliance opinion.
