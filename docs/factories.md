# Factories & dynamic contract discovery (RFC-0009)

Most real DeFi protocols deploy contracts at runtime: a Uniswap V3 *factory* emits a `PoolCreated`
event, and the pool it announces is a new contract you also want to index. A static
`[[contracts]]` list can't express that. Nuthatch factories can: declare a **template** (an ABI) and
a **factory rule** (which event announces a child, and where the child address is), and Nuthatch
discovers and indexes every child automatically - retroactively during backfill, reorg-safely at the
tip.

## Configure

```toml
# The watched contract (the factory itself) - a normal contract.
[[contracts]]
alias = "factory"
address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"   # Uniswap V3 factory
abi = "abis/factory.json"

# The template: an ABI applied to every discovered child. All children of one template share tables
# (`pool__swap`, `pool__mint`, …), distinguished by the implicit `address` column.
[[templates]]
name = "pool"
abi = "abis/uniswap_v3_pool.json"
# filter = "topic0"   # optional: force the topic0-only backfill filter (see "Scale" below)

# The rule: when `factory`'s `PoolCreated` fires, index the child in the `pool` param under `pool`.
[[factories]]
watch = "factory"          # a contract alias, or another template (nested, up to depth 3)
event = "PoolCreated"      # an overloaded event needs its full signature: "PoolCreated(address,uint24)"
child_param = "pool"       # the event parameter holding the child contract address
template = "pool"
# start = 12369621         # optional: only honour discoveries at or after this block
```

`nuthatch dev` then indexes the factory *and* every pool it discovers, into the shared `pool__*`
tables. `nuthatch init --from <nest>` validates a published factory nest (references resolve, no
template/alias collision, depth ≤ 3).

## How it works

- **Tip:** a factory nest fetches logs **topic0-only** (no address filter), so a pool created *and*
  traded in the same block is already in hand. Each window is decoded in chain order, routing every
  log to a contract decoder or a discovered child's template decoder, and discovering new children
  inline - no extra RPC round-trip.
- **Backfill (`--seal-direct`):** a sequential two-pass per chunk - pass 1 fetches the current
  address filter (contracts + children so far) and updates the child registry from the factory
  events; pass 2 re-fetches the chunk for only the newly discovered children (children-only, so their
  *historical* activity is sealed even though it predates their discovery in the filter). Rows are
  sorted by `(block, log_index)` and sealed to deterministic, content-addressed segments.
- **Reorg:** a rolled-back factory event drops its children from the registry; the child's own rows
  roll back with their block. The discovered set is a pure fold over factory events ≤ B, so it
  reproduces exactly - a property test pins the convergence.
- **Provenance:** each sealed segment records the child registry's content hash (`registry_snapshot`)
  at seal time, so a segment carries exactly which discovered set produced it.

## Scale

A factory backfill's address filter grows with every child, and providers cap address-list size.
Above **~500 children** (or a per-template `filter = "topic0"` override) the backfill **flips** to a
topic0-only fetch with local registry-lookup filtering. The flip is byte-identical - it changes only
how logs are fetched, never what gets sealed.

## The children view

Every template gets an auto-generated **`{template}__children`** view over the sealed factory events -
the discovered children with their provenance, de-duplicated to the earliest discovery per address:

```sql
-- which pools, discovered when, by which factory
SELECT address, discovered_block, discovered_timestamp, parent_address
FROM "pool__children"
ORDER BY discovered_block DESC
LIMIT 20;

-- pools created "this week" without a join (discovered_timestamp is the block's unix time)
SELECT count(*) FROM "pool__children"
WHERE discovered_timestamp > epoch(now()) - 7*24*3600;
```

The child event tables (`pool__swap`, …) are ordinary tables - query them like any other, filtering
by `address` for a single pool.

## Not supported (by design)

- Per-child handler logic (children of one template share one decode + one set of tables).
- Nesting beyond depth 3.
- Bytecode / trace-based discovery (discovery is event-driven only).
