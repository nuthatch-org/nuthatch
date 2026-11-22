# Authored views - a nest's logic layer (RFC-0018 §1)

A nest can ship **authored SQL views** in `views/*.sql` - the derivations it exists to answer ("top
holders", "daily volume", "delegator exposure"). This is what makes a nest feel alive: its reasoning
is visible, diffable, and agent-legible instead of living in ad-hoc REPL queries that get lost.

## What a view is (and isn't)

- A view is a read-only `CREATE VIEW …` over the nest's tables. It's **recomputed per query** over the
  live tip ∪ sealed history (one surface) - never written back, never in the ingest/decode/seal path,
  and not materialised unless you declare it maintained (below). Adding a view can't corrupt data or
  slow indexing.
- It is **not** an incrementally-maintained entity. A view is just SQL that runs when you query it.
  The shipped IVM relations are the three built-in circuits (`balances`, `exposure`, `velocity`) and,
  since 3.0.0, whatever a nest declares in `entities.toml` - see [entities.md](entities.md). Choose a
  view unless the query is both asked often and expensive; a view is free until someone asks, an
  entity costs memory continuously.

## Adding one

Drop a file in `views/`. Files load in **sorted filename order** (`10-…`, `20-…`), so a `20-` view can
build on a `10-` one.

```sql
-- views/10-top-recipients.sql
CREATE VIEW top_recipients AS
  SELECT "to" AS addr, count(*) AS n
  FROM "usdc__transfer"
  GROUP BY "to"
  ORDER BY n DESC
  LIMIT 20;
```

Then **describe what it means** in `semantic.toml` so the MCP and `/schema` render it:

```toml
[view.top_recipients]
description = "The 20 addresses that received the most USDC transfers."
```

Query it by name - it's just another relation:

```sh
nuthatch sql "SELECT * FROM top_recipients"
```

## The footguns (the same four every agent trips on)

1. **Reserved-word columns.** `from` and `to` are SQL keywords - double-quote them: `SELECT "from"`.
   Call the MCP `schema` tool; it lists exactly which columns are reserved for this nest.
2. **Big-int columns are exact text.** A `uint256`/`int256` column (e.g. `value`) is stored as a
   text string. Never `SUM(value)`. For **amounts**, `SUM(value_dec)` is the values that fit:
   a full-width word is NULL and is not a term. `WHERE NOT value_overflow` is that same sum.
   **Ids, nonces and hashes stay on the raw column.** `_dec` is `DECIMAL(38,0)` and is NULL for a
   full-width uint256, so a nonce filter on `nonce_dec` returns no rows while the raw column
   is right. `schema` lists `_dec` columns and which of them overflow.
3. **Bool columns are exact text too.** A Solidity `bool` column (e.g. `enabled`) is stored as text
   `'true'`/`'false'`, not a SQL boolean. `enabled = true` and `AND`/`NOT` implicitly cast and work;
   `COALESCE`, `CASE`, `bool_and`/`bool_or` and `UNION` do not, and fail to build with "an explicit
   cast is required". Write `enabled = 'true'` or `CAST(enabled AS BOOLEAN)`. `schema` lists which
   columns are bools.
4. **The hot/cold seam.** A view sees the whole hot ∪ cold surface, so results are current - but it's
   recomputed each query, not a maintained snapshot. `schema` shows `sealed_through` vs the tip.

## When a view is too slow: `maintained.toml` (RFC-0062)

A view is recomputed on every request. When one is expensive (a page of a join-heavy view over the
whole history takes seconds) and its tables change less often than it is asked, declare it in
`maintained.toml` at the nest root:

```toml
[[view]]
name = "top_recipients"
```

The nest then answers it from a stored copy of its own evaluation, rebuilt whenever anything it reads
changes, and from the definition until the rebuild lands. Answers are identical either way. It
suits a view that is asked often and whose whole history evaluates in the analytics pool; it does not
suit one whose tables gain rows every block, which is never current and simply answers as before. A
view that calls `now()` or reads `labels`, an `offchain__*` view or an entity cannot be maintained and
refuses the nest at startup. [config-reference.md](config-reference.md) has the file;
[entities.md](entities.md) is the alternative for an aggregate that must cost only the new rows.

## Validation is loud (RFC-0018 §1)

A broken or **drifted** view - one that references a table or column the ABI no longer produces -
fails `nuthatch check` with a fuzzy-matched fix hint, and warns loudly at `dev` startup. It never
silently vanishes. A bad view is fault-isolated: it never disables the other views or the query
surface, but you'll always hear about it. So: write the view, run `nuthatch check`, fix what it tells
you.
