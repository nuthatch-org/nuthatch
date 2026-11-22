# The four classes (RFC-0044 §5a)

Every field of every `@entity` in `schema.graphql` goes in exactly one class. The citation is a
named line of mapping source (or of the schema, for `@derivedFrom` and `@fulltext`).

| Class | Meaning | Ported as |
|---|---|---|
| **exact** | a pure function of decoded events | a `views/*.sql` view (entities.toml is S5); byte-identical |
| **call-derived** | reads contract state at the row's block | a `[[calls]]` declaration (RFC-0038 §3) |
| **fixed point** | reads back its own or another entity's prior output | a convergent value: defensible, and **different** |
| **unreachable** | needs internal calls, `@fulltext`, or time travel | not ported; named, with the reason |

`nuthatch port-report --dir <subgraph>` emits this table. An agent classifying by hand follows the
same rules so a disagreement is a finding, not a style difference.

## How to classify a field

1. Find every assignment `entity.field = …` (and `new Entity(id)`, which writes `id`).
2. Read the right-hand side, including helpers it calls. Do not execute them.
3. Take the **worst** writer (unreachable > fixed point > call-derived > exact).

Signals, most specific first:

- Schema `@fulltext` → **unreachable**. Citation is the schema line. Graph-node search index, not a
  stored column.
- Written only from a `blockHandler` → **unreachable**. nuthatch indexes logs. Citation is the
  assignment. (Traces / internal calls are the same class, with that reason.)
- No mapping writes the field, and it is not `@derivedFrom` → **unreachable**, "no mapping writes
  this field".
- RHS calls `Contract.bind`, `ethereum.call`, or `.try_*` (the graph-ts eth_call pattern), or a
  helper that does → **call-derived**. Port as `[[calls]]` on the triggering table, pinned at the
  row's block. Needs `--state-rpc`.
- Helper walks other entities in a loop and reads stored computed fields (Uniswap
  `findEthPerToken` reading `token1.derivedETH`) → **fixed point**. A nest can solve the mutual
  recursion to convergence; that number is not the subgraph's. Say so.
- `@derivedFrom` → **exact**, citation on the schema. It is a reverse lookup, a SQL join.
- RHS is `event.params` / `event.block` / `event.transaction` / `event.address` / `event.logIndex`,
  or a fold of the field's own prior value with those (`total.plus(event.params.amount)`) → **exact**.
- Helper that loads one hardcoded entity and reads an exact field (`getEthPriceInUSD` reading
  `pool.token1Price`) → **exact**. RFC-0038 §6d measured this.

Call-derived used as a *scale factor* (token `decimals` inside `sqrtPriceX96ToTokenPrices`) does
**not** reclassify the price. The decimals field is call-derived; the price stays exact. Fixed point
does leak: anything that multiplies by `derivedETH` is fixed point.

## Citations

`file:line`, 1-indexed, the assignment (or the helper's contract-call / loop-load line when that is
what decides the class). Schema citations are `schema.graphql:N`.

A field with two writers keeps the citation of the worse class. `Token.derivedETH = ZERO_BD` on
create and `= findEthPerToken(token)` on swap is fixed point, cited at the `findEthPerToken` call.

## What the report must say

For every non-exact field: **this field will not reproduce, and here is why.** Exact fields promise
byte-identical. A single unpredicted divergence fails the port (RFC-0044 §10).
