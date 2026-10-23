# The Graph query dialect a nest accepts

RFC-0053 S2 (#1266). What `/graphql` lowers to Burrmill SQL, what it refuses, and why each refusal is
a refusal rather than an approximation. The route exists only in a nuthatch built with
`--features graph`: the default binary answers it 404, and the published `nuthatch-graph-*` downloads
are built with it (see the last section). Everything on this page describes that one build.

**The governing rule is RFC-0053 §Design: "It must never silently approximate a value and call the
endpoint drop-in."** Applied to a query, that means an argument this compiler does not implement is an
error in the Graph envelope, never a query quietly run without it. Two exceptions are stated where
they apply: `subgraphError` is accepted and ignored, and a fragment's type condition is not checked. A dropped `where` clause returns
*more* rows than the caller asked for, and a caller who cannot tell that from a correct answer is
worse off than one who got an error.

Every refusal below names the thing refused, so a caller can act on it.

## Accepted

### Roots

| shape | SQL |
|---|---|
| `pools { … }` | `SELECT … FROM "pool" ORDER BY "id" ASC LIMIT 100 OFFSET 0` |
| `pool(id: "0x…") { … }` | the same with `WHERE "id" = '0x…' LIMIT 1`, answered as an object |
| `pools(block: { number_gte: N }) { … }` | answered on the latest block when the nest's head has reached `N`, else graph-node's own refusal. Not time travel: *"the query will be executed on the latest block **only if** the subgraph has progressed to or past the minimum block number"* (`graph/src/schema/api.rs:1189`), which a nest satisfies exactly with no history stored |
| `_meta { block { number } … }` | not compiled at all: answered from the nest's own head. It takes the same `block: { number_gte: N }` precondition and refuses any other argument, rather than answering a named block with the head |

### Numeric ordering and comparison

A nest stores every big number as canonical text (`analytics.rs:2745`: columns are `UBIGINT`, everything
else is text), so `ORDER BY "value"` compares **strings**. Text order agrees with numeric order only while
every value has the same digit count, so `9000351` ranked above `60000353` and a `value_gt` dropped rows it
should have kept (#1325).

`orderBy` and the four ordering comparisons - `_gt`, `_gte`, `_lt`, `_lte` - on a `BigInt`, `BigDecimal` or
`Int8` field therefore compare a **key** rather than the column: a sign character, the integer part's digit
count zero-padded, then the digits with the point removed, with negatives carrying an inverted length and
the nines complement. It works under `DESC` because it is one key, works whether the
column is text or a real integer, and is exact for an integer part up to 99,999 digits - the width of the
length term. A `uint256` is 78.

`TRY_CAST(.. AS DECIMAL(38,0))` is the obvious fix and the wrong one: a `uint256` reaches 78 digits, so the
cast is NULL past 38 and a row does not sort oddly - it **disappears** from a filter it satisfies.

Equality, `_not`, `_in` and `_not_in` compare the column directly: canonical text compares equal exactly
when the numbers do. The key assumes canonical text - no leading zeros, nothing trailing the point - which
is what the decode registry and graph-node's `normalized()` both produce.

### Scalar wire types

graph-node maps every stored value to a GraphQL value in `graph/src/data/store/mod.rs:554`, and only one
of the numeric scalars stays a number. A nest's decoded columns are typed differently again - a `uint256`
is canonical text, a block number is `UBIGINT` - so the lane casts at the projection rather than
letting storage decide the wire shape.

| schema scalar | sent as | why |
| --- | --- | --- |
| `Int` | JSON number | `q::Value::Int`. The only numeric scalar that is not a string |
| `BigInt`, `BigDecimal`, `Int8` | JSON **string** | `q::Value::String`. A number here is unholdable above 2^53 and parses differently below it |
| `Bytes`, `String`, `ID` | JSON string | already text on both sides |
| `Boolean` | JSON boolean | a boolean on both sides |

The cast is in the `SELECT`, never in the view: `where` and `orderBy` compare the stored column, and a
view that cast `value` to text made `orderBy: value` lexicographic.

`Timestamp` is not in that table. graph-node sends microseconds since the epoch, which is a unit
conversion rather than a cast, and a nest's column is in whatever unit its view put there - so the lane
leaves it alone rather than converting a known unit into a wrong number.

### Relation traversal

A **to-one** reference is one `LEFT JOIN` on the id the parent row already holds:

```graphql
{ pools { id token0 { symbol decimals } } }
```

```sql
SELECT b."id", j1."symbol" AS "j1__symbol", j1."decimals" AS "j1__decimals"
FROM "pool" b LEFT JOIN "token" j1 ON j1."id" = b."token0"
ORDER BY b."id" ASC LIMIT 100 OFFSET 0
```

`LEFT` and not `INNER`: a reference whose target row is absent leaves the parent in the answer with a
null relation, which is what graph-node does. An inner join would drop the parent silently, and a
missing row is exactly the case a failed subgraph leaves behind.

Because the target's id is unique the join cannot multiply rows, so `first` still means what it says.

A **`@derivedFrom` list** is aggregated rather than joined, because a join would multiply the parent row
once per child and `first` would stop meaning anything. One correlated subquery per parent keeps the
parent's row count and the child's page size separate:

```graphql
{ pools { id swaps { id amount } } }
```

```sql
SELECT b."id",
  coalesce((SELECT to_json(list(t.s)) FROM (
     SELECT struct_pack("id" := c1."id", "amount" := c1."amount") AS s
     FROM "swap" c1 WHERE c1."pool" = b."id" ORDER BY c1."id" ASC LIMIT 100) t), '[]') AS "c1__swaps"
FROM "pool" b ORDER BY b."id" ASC LIMIT 100 OFFSET 0
```

The join key is the schema author's: `@derivedFrom(field: "pool")` says `Swap.pool` holds the parent id.

`to_json(list(struct_pack(…)))` in preference to returning a `LIST` of `STRUCT`, so the column arrives
as a plain JSON string and nothing depends on how the row serialiser handles a nested `STRUCT`. The
inner subquery exists because `ORDER BY` and `LIMIT` cannot sit inside the aggregate.

**`coalesce` is not decoration.** `list()` over zero rows is `NULL` in Burrmill, as it was in DuckDB
before 4.1 (re-measured through `/sql` on 4.10.1, 2026-10-05), so a parent with no children would
answer `null` for a field the generated schema types `[Swap!]!`.

One level, matching the depth `E_orderBy` advertises in the reference: graph-node emits
`token0__symbol` and no `token0__whitelistPools__id`.

Every column is qualified with a base alias whether or not the query joins, so a relation whose target
shares a column name - `id` always does - cannot turn a working query into an ambiguous one on some
other schema.

The two names come from two different derivations, both taken from the live graph-node reference:
`lower_first` for the singular, `plural` for the collection. `modifyLiquidity` pluralises to
`modifyLiquidities`, and getting that wrong means the client's query names do not exist here.

### Arguments

`first` (default 100, **refused** outside 0..=1000 rather than clamped, so a caller who asked for
1,500 rows is told rather than quietly given 1,000), `skip` (default 0), `orderBy`, `orderDirection`,
`where`, `id` on a singular root, and `subgraphError`, which is **accepted and ignored**: a nest runs
no mapping, so `allow` and `deny` cannot differ.

### `where` operators

Bare, `_not`, `_gt`, `_gte`, `_lt`, `_lte`, `_in`, `_not_in`, and the text set: `_contains`,
`_starts_with`, `_ends_with`, each with a `_not` form and each with a `_nocase` form, eight more.

**Which operators a field accepts is derived from the schema, not from a table here.** The operator has
to be one `graph_schema::filter_suffixes` declares for that field's type, and that function was derived
from the recorded reference. `Bytes` carries ten operators and `String` eighteen - no prefix forms and
no `_nocase` anywhere on `Bytes` - so `sender_starts_with` on a `Bytes` field is refused. Accepting it
would answer a query that a client's own validator, built from the schema we advertise, would reject.

Text operators lower to `LIKE`, `NOT LIKE`, `ILIKE` or `NOT ILIKE` with a declared escape character:

```
hooks_contains: "50%_x"   ->   b."hooks" LIKE '%50\%\_x%' ESCAPE '\'
```

**The escaping is the whole point.** `%` and `_` are wildcards in `LIKE` and *data* in a filter, so
`hooks_contains: "50%"` has to match a literal `50%` rather than everything beginning `50`. Backslash
is escaped first, or escaping the wildcards would introduce backslashes that then get re-escaped, and
the SQL literal's own quote-doubling is applied last so nothing above can undo it.

### Nested relation filters

`token0_: Token_filter` - a filter on the related entity. The generated schema advertises one for every
relation, so refusing it made our own schema validate a query the endpoint then rejected.

```graphql
{ pools(where: { liquidity_gt: "1", token0_: { symbol_contains: "ET" } }) { id } }
```

```sql
WHERE b."liquidity" > '1'
  AND coalesce(b."token0" IN (SELECT n0."id" FROM "token" n0
                              WHERE n0."symbol" LIKE '%ET%' ESCAPE '\'), false)
```

A membership subquery rather than a join, so the parent's row count is unchanged and `first` keeps
meaning what it says. It was a correlated `EXISTS` until #1458, where one combined with a selected
relation produced an invalid binding on DuckDB; membership is correct either way, and the `coalesce`
keeps a null reference or a null child id from answering `null` under another boolean. Conditions
inside are lowered against the **child** entity, with the child's own operator set, and an unknown
field there is named against the child.

**A nested filter across a `@derivedFrom` list - `swaps_` - is refused by name.** The reference
advertises those too, but asking the live reference endpoint for one returns **HTTP 504** on this
deployment: graph-node advertises an input it cannot itself answer here. So the semantics are not
something this slice has measured, "probably matches if any child matches" is a guess about which rows
come back, and the refusal says exactly that rather than pretending it is an unknown field.

An empty nested filter is refused for the same reason an empty `and`/`or` is.

### `and` and `or`

Both lower to a bracketed boolean tree, and conditions inside one filter object are `AND`ed, as `where`
itself is:

```
where: { liquidity_gt: "1", or: [{ id: "a" }, { hooks: "b" }] }
  ->   b."liquidity" > '1' AND ((b."id" = 'a') OR (b."hooks" = 'b'))
```

The outer brackets are load-bearing: spliced in unbracketed, `x AND a OR b` binds as `(x AND a) OR b`,
which answers a different question. They nest.

An **empty** `and`/`or`, and an empty filter object inside one, are refused. `_in []` could be reasoned
about from SQL - there is no empty `IN` list and the empty set matches nothing - but "no conditions" has
no such forced reading, and graph-node's behaviour here is not something this slice has measured.

### Fragments

Named fragments and fragment spreads, inline fragments, fragments defined after the operation that uses
them, and a spread inside a fragment. Spliced structurally, never by text substitution - this endpoint
already had one bug from deciding things by searching the raw query.

This is not a nicety: **the introspection document a generated client sends is built from fragments**
(`fragment FullType on __Type`, `...FullType`), so refusing them refused the one request that has to
work before any other can.

A spread with no definition is refused by name rather than treated as nothing, which would silently
drop every field it was carrying. A fragment that spreads itself is an error rather than a stack
overflow.

The type condition on `... on Type` is consumed and not checked, because every field is validated
against the entity anyway: a spread naming fields the entity has not got is already refused by name.

`_in []` lowers to `FALSE` and `_not_in []` to `TRUE`, because SQL has no empty `IN` list and the
empty set is a legitimate thing for a client to send. Suffixes are matched longest-first, so
`_not_in` is not read as `_not` followed by a field called `in`.

### Variables

A named operation with a variable definition list, `$name` in any argument position, values from the
request body's `variables`, and header defaults where the request omits one:

```graphql
query Pools($first: Int!, $min: BigInt = "5") {
  pools(first: $first, where: { liquidity_gt: $min }) { id }
}
```

This is the shape **every generated client sends**, so a compiler that refuses variables refuses in
practice every query a client produces, whatever it can do with a hand-written one.

The operation header is parsed rather than skipped to the first `{`. A variable default may itself be
an object - `$w: Pool_filter = { id: "0xaaa" }` - and skipping to the first brace lands inside it,
after which the whole operation reads as garbage.

## Refused, by name

Rows marked † are narrowed by [Network Subgraph client shapes](#network-subgraph-client-shapes-rfc-0060)
below; the refusal holds outside the shapes listed there.

| refused | why it is not approximated |
|---|---|
| `block: { number: N }`, `block: { hash: … }` † | needs a block-ranged entity store the nest has not got (#1267). Answering as of head while the caller named a past block is a wrong answer that looks right. `number_gte` is **not** in this row - see above |
| a `block` object carrying anything besides `number_gte` alone | `{ number_gte: N, number: M }` names two different requirements and only one of them is answerable |
| arguments on a traversed field (`swaps(first: 5)`) † | the relation would need its own `LIMIT`, and a dropped `first` there returns every related row. Refused **before** any traversal is lowered: the guard once sat after the aggregation branch, so this compiled with the argument silently gone |
| a **stored** list of entity ids (`Token.whitelistPools`, no `@derivedFrom`) | a different shape needing `unnest`, not the aggregation below, and using the wrong one would answer with the wrong join |
| a traversal more than one level deep † | refused by name rather than answered with an N+1 walk, which would answer slowly and with a different transaction view per row |
| an entity root with no selection set | not a legal GraphQL query, and answering `*` would invent a field list the caller never asked for |
| an operator the schema does not declare for that field's type | `sender_starts_with` on a `Bytes` field: our own generated schema says it does not exist |
| an empty `and`/`or`, or an empty filter object inside one | "no conditions" has no forced reading, and guessing one would be approximating a predicate |
| a fragment spread with no definition | treating it as nothing would silently drop every field it carried |
| a field an introspection type has not got (`__Type { nope }`) | graph-node's validation refuses it, and a `null` there reads as a real answer. Every field `introspection.graphql` declares answers, `queryType { fields }` included |
| a collection the schema declares and no view serves | `` `tokens` is not served by this nest: it has no `token` view ``, rather than the engine's "table does not exist" and a hint about some other table |
| `orderBy` that traverses a relation (`token0__symbol`) | needs the same join as a nested selection |
| `mutation`, `subscription` | a nest has no mappings, so it has nothing to mutate and nothing to stream |
| more than one operation and no `operationName` | there is no way to know which was meant. `Operation name required`, graph-node's own wording |
| an `operationName` no operation in the document carries | `Operation name not found `X``, likewise. An anonymous operation carries no name, so it is never what a name selects |
| directives on an operation | skipping one silently is the same class of mistake as a dropped filter |
| an unbound non-null `$name` (`$first: Int!`) | neither the request nor the header supplies a value, and graph-node refuses it too. A *nullable* `$name` the request leaves out is an argument or input field that was not given, as the GraphQL spec and graph-node read it: BetSwirl's SDK declares `$where: Token_filter` and sends no `where` |
| a `null` filter value, as a literal or through `variables` † | in a filter it could mean `IS NULL` or the absence of the condition, and those select different rows. It used to parse as the enum `null` and compile to `= 'null'`, matching rows whose value is that four-character string |
| a fractional number in `variables` | `BigInt` and `BigDecimal` travel as strings over GraphQL precisely because a float loses them, so a fractional JSON number is refused rather than rounded into a filter |

## Network Subgraph client shapes (RFC-0060)

The same graph build serves the Network Subgraph client dialect on top of everything above; there is
no further switch. A default build registers none of these routes and never reads
`graph/history.toml` (`serve.rs`: the policy is `None` outside the feature). RFC-0059 and RFC-0060
were parked on 2026-09-26 with their slices S0 to S3 left in the tree behind the feature, so what
follows describes that build as it stands. It narrows the four † rows of the refusal table:
`block: { number: N }` and `block: { hash: … }` are answered where `graph/history.toml` declares a
policy, a derived list takes
its own arguments, the two-level shapes below are accepted, and `null` is accepted for equality and
inequality.

An explicit `block: null` selects the head, on entity roots and on `_meta`, as on the Rust monitor's
first page. Subsequent hash-pinned pages require the historical mode below.

One to-one relation level is supported generally. A derived list directly below it is also supported,
with its own `first`, `skip`, `orderBy`, `orderDirection` and `where` applied in the correlated query.
This is the bounded two-level shape the Horizon escrow client uses:

```graphql
{ paymentsEscrowAccounts { payer { signers(first: 1000, where: { isAuthorized: true }) { id } } } }
```

Top-level derived lists accept the same filtering and pagination arguments. They may also select a
to-one relation with scalar children, as in `subgraphDeployments { indexerAllocations { indexer { id } } }`.
Two more shapes come from BetSwirl's published client. A to-one relation below a to-one relation,
`gameToken { token { symbol } }`, is a second `LEFT JOIN` with scalar children. A `@derivedFrom` field
typed as one entity, `weightedGameBet: WeightedGameBet @derivedFrom(field: "bet")`, fetches at most two
children: none answers `null`, one answers the object, and two answer graph-node's own
`Ambiguous result for derived field` error. All of these remain one SQL statement, not a
parent-by-parent request loop. Further traversal is not accepted.

A stored list of scalars, `rolled: [BigInt!]`, travels as `to_json` text and is answered as the array.

Null equality and inequality lower to `IS NULL` and `IS NOT NULL`, including the Rust monitor's
`closedAt_not: null`. Null is not the string `"null"`, and never removes a filter condition. `null`
with any other comparison stays refused.

### Historical reads

A nest whose entity views derive exclusively from complete block-stamped facts can declare
`graph/history.toml`:

```toml
version = 1
first_block = 42449585
max_head_age_seconds = 15
max_block_distance = 60
```

This enables entity number selectors throughout the declared indexed range and hash selectors at
retained canonical checkpoints. It filters raw facts before the authored SQL views evaluate; it does
not filter today's aggregated entities. Current maintained entities, labels and offchain snapshots are
not substituted into past-state queries. The query still uses the ordinary SQL admission, row, memory
and time bounds. Checkpoint hashes have a persisted reverse lookup, updated transactionally on writes
and rollbacks and rebuilt whenever this build opens a writable store, because a default build keeps
none and may have written checkpoints since.

All roots default to the same captured head. `_meta(block: ...)` describes its selected checkpoint.
Historical numeric metadata is accepted between retained checkpoints, as it is in graph-node: the
requested number is returned while `hash` and `timestamp` are null. Hash selectors still require an
actual retained canonical checkpoint. Entity queries by number do not require a header at that number
and make no RPC calls. Future blocks, blocks before `first_block`, stale heads and a store generation
changing during the operation are errors. Unknown hashes remain unavailable. The age limit and
optional distance limit are admission checks, not evidence that a deployment keeps up with the chain.
`GET /graph/status` reports the same freshness check.

The policy is an author assertion of complete input history, not a backfill or completeness proof.
It does not make a partial schema or incomplete entity derivation a Network Subgraph replacement.

## What S2 is done when

- A canonical client query against the reference schema returns rows that match the reference
  deployment, modulo the declared divergence list.
- A mutation that drops any single argument, operator, default or bound above makes a test fail.
  [`mut/m-s2-http.sh`](https://github.com/nuthatch-org/nuthatch/blob/ede03ca7/mut/m-s2-http.sh) was the set.

## Where the surface is reachable

`POST /graphql`, `POST /subgraphs/id/{id}`, `POST /subgraphs/name/{*name}`. The three shapes exist so
an existing client's URL can be swapped host-for-host without rewriting the path. They are
registered only in a build with `--features graph` (#1440); a default build answers them 404.

Queries run through `run_sql_query`, so they inherit the node's admission bounds and row cap rather
than opening a second unmetered way into Burrmill. An answer `/sql` would flag as truncated, degraded or
missing its tip is refused here, because a GraphQL response has nowhere to carry the flag.
