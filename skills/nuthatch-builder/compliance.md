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

Live sanctions screening and the WASM screening component were removed on 2026-10-08. A
`nuthatch.toml` that still declares `[screening]` is refused at load; delete the table.

Do not present this output as legal/regulatory advice - it is a deterministic annotation layer, not
a compliance opinion.
