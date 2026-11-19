# Emit (RFC-0044 S2)

Sits on top of `nuthatch init --from-subgraph`. Do not reimplement the importer. The nest already
has contracts, ABIs, factories, `nuthatch.toml`. This overlay adds what the mapping actually reads.

```sh
nuthatch init --from-subgraph <CID> --dir nest/
nuthatch port-report --dir path/to/subgraph > nest/README.md
nuthatch port-emit --dir path/to/subgraph --out nest/
```

`--dir` is the subgraph (schema + mappings). `--out` is the nest the importer wrote. The proxy trap
does **not** apply on this path: the manifest pinned implementation ABIs.

## What lands where

| Class | Emitted as |
|---|---|
| exact | `views/20-<entity>.sql` — a `CREATE VIEW` over the triggering event table |
| call-derived | `[[calls]]` in `nuthatch.toml`, parameterised by the triggering row (RFC-0038 §3) |
| fixed point | named in the README; not emitted. The number will be different. |
| unreachable | named in the README; not emitted |

The README is the S1 port report, verbatim. entities.toml is S5; emit `views/*.sql` now.

A `Template.create(event.params.x)` in an event handler becomes a `[[factories]]` rule for that
template; a creation it cannot pin to the handler's own event parameter is named, not guessed.

`--dir` must hold the mapping **source**. From a deployment CID alone (IPFS has compiled WASM)
nothing is classified, so `port-emit` refuses and prints no coverage figure.

## `[[calls]]` must trace

Every stanza comes from a specific `.try_*` / `.bind` / `ethereum.call` in the mapping. The
signature is that method (`try_symbol()` → `symbol()`), `on` is the handler's event table
(`Factory` + `PoolCreated` → `factory__pool_created`), `contract_column` is the bind argument
resolved onto the triggering row (`event.params.token0` → `{token0}`).

If that line is not a contract read, there is no stanza. Dropping the call from the mapping drops
the `[[calls]]`. Inventing a getter the mapping never made is the failure this slice exists to
prevent.

Needs `--state-rpc` to resolve at run time. That endpoint is never a config key.

## Traps, still

Read [traps.md](traps.md) before emitting. `[[factories]] watch` is an alias, never an address.
Do not re-resolve ABIs from Sourcify on top of a manifest.
