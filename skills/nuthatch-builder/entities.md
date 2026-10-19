# Authored incremental entities - a relation the indexer keeps, not one it recomputes (RFC-0041)

A nest can declare a relation in `entities.toml`. Nuthatch compiles the `SELECT` into a DBSP circuit
and **maintains it as blocks arrive**, so a query reads an answer that is already computed instead of
recomputing one. Shipped in 3.0.0-alpha.

Read [views.md](views.md) first if you have not: a view and an entity look almost identical in the
file and are completely different at runtime, and choosing wrongly is the common mistake.

## View or entity

| | authored view | authored entity |
|---|---|---|
| lives in | `views/*.sql` | `entities.toml` |
| when it runs | on every query | as each block arrives |
| costs | query time, every time | memory, continuously |
| SQL it accepts | anything the read-only surface allows | a small admitted subset, below |
| reorg | nothing to do - it reads current data | retraction, handled for you |

**Prefer a view.** It is free until someone asks, it accepts any SQL, and it cannot be wrong about
history. Reach for an entity when a specific query is both *asked often* and *expensive*, which in
practice means an aggregation over a table with a lot of sealed history.

Measured on 3.0.0 against a copy of the Lodestar nest (`docs/bench/3.0.0-alpha-live.md`), one panel
over 733 sealed segments went from **2.15 s to 88 ms** by becoming an entity. That is the shape worth
converting: the same aggregate, asked repeatedly, over history that keeps growing.

## Declaring one

```toml
[[entities]]
name = "indexer_rewards"
sql = "entities/indexer_rewards.sql"
key = ["indexer"]
max_rows = 100000
```

Put the query in `entities/indexer_rewards.sql`:

```sql
SELECT indexer, SUM(CAST(tokensRewards AS HUGEINT))
FROM service__indexing_rewards_collected
GROUP BY indexer
```

`sql` must name `entities/<name>.sql`, with the filename matching the entity name. Both `check`
and `dev` read that file. Inline SQL is refused; move existing inline queries into the named file.

- **`name`** is the relation's name. It is how you query it, and it **must not collide with a decoded
  table** - the nest refuses to start rather than shadow one.
- **`key`** must name at least one output column, and must be unique in the result. Validation runs
  the query once and checks; a non-unique key is refused at load, not at the first block.
- **`max_rows`** is the declared bound, and it is a **fault** rather than a warning. Crossing it stops
  the entity, which quarantines the nest. Size it for what the relation will hold in a year, not
  today, and see the budget note below.

## The SQL it accepts

An entity's `SELECT` is compiled, not merely run, so the subset is small and checked at load:

- **Aggregates: `sum`, `min`, `max`, `avg`, `count`, `count(*)`.** Nothing else. This is an
  allowlist, not a refusal list, and deliberately so - a SQL engine knows dozens of aggregate names and grows the
  set, so anything not proven incrementally maintainable is refused rather than silently admitted.
- One `GROUP BY`, projection, filtering, exact arithmetic, and an equijoin between two tables.
- Column names bind against the nest's ABI at load. An entity naming a column the contract does not
  have is refused when you start, not at the first block that would have used it.

If your query does not fit, it is a view. That is not a consolation prize - see the table above.

## What it costs

Charged against the per-cursor RAM budget at **3,200 bytes per declared `max_rows`**, plus about 8 MB
for the circuit and its thread. So `max_rows = 100000` reserves roughly 320 MB of the cursor's budget
whether or not the relation ever fills. A mount that would exceed the budget is refused at load.

Measured at real scale on 3.0.0-alpha (`docs/bench/3.0.0-alpha-live.md`) the true cost was 940 to
1,482 bytes per maintained row depending on how wide the key and aggregates are, so the charge is
deliberately conservative. `nuthatch_entity_state_bytes` on `/metrics` is what a running circuit holds.
A circuit whose sampled state outgrows its charge, such as a join whose output is far larger than
its input, stops as one crossing `max_rows` does.

## What happens on a restart

**The entity is rebuilt from the sealed corpus and the hot tail.** No RPC, no re-index: the facts are
already on disk. Measured on 3.0.0-alpha (`docs/bench/3.0.0-alpha-live.md`): a nest with 733 sealed
segments took **1.9 seconds**, one with 2,985 took 2.4 seconds. Expect a restart to get slower by
about that much, once, per entity.

This is also why editing an entity is cheap. `entities.toml` is excluded from the nest's *data*
identity, so an edit moves the package NID without invalidating the decoded facts - the entity
rebuilds locally and the chain is never re-fetched.

## What happens on a reorg

Nothing you have to write. The removed facts are fed back at weight `-1` and the aggregate retracts;
there is no rollback interface because there is nothing to roll back. Observed live on Ethereum
mainnet during the 3.0.0-alpha stress test (`docs/releases/3.0.0-alpha-stresstest.md`): a reorg rolled
back 32 rows and the relation still summed to the same total as every decoded transfer the nest held,
to the last digit.

## Reading an offchain table

An entity may read or join `offchain__<table>`, the snapshots `nuthatch offchain drop` and `pull` seal.

- **Its columns are typed from the snapshots: text, integers and booleans.** A float or decimal
  column is refused at load, so publish a scaled integer such as `price_e8`. So is a column typed
  one way in one snapshot and another way in the next.
- **A new snapshot is a delta.** The window after it is sealed feeds its rows at `+1`. Nothing
  already applied is recomputed.
- **A replaced or deleted snapshot is a rebuild** from the sealed corpus, the hot tail and every
  snapshot still present. It runs beside the live relation and is swapped in whole, so until it
  lands the entity answers from the old snapshots and says so.
- **Offchain rows never retract.** A reorg retracts the chain side only.
- **What an entity holds from its offchain tables is bounded by its own `max_rows`,** in rows and
  at 3,200 bytes a row, across all of them. Past that the entity faults, naming the table, and the
  rest of the nest carries on. Admission charges such an entity twice its `max_rows`, so declare a
  larger one to hold a larger table.
- **New snapshots are picked up at window boundaries.** A chain that stops producing blocks also
  stops the entity picking up snapshots.

`/ready`, `/derived` and `/sql` provenance name what was applied:
`"offchain": {"offchain__prices": {"snapshots": 12, "latest": "<hash>"}}`.

**Such an entity is reproducible by snapshot, not re-derivable from chain,** and every surface says
so with `"reproducibility": "snapshot"`, as does any `/sql` answer that reads `offchain__<table>`
directly. The label follows from what the entity reads, so no configuration removes it. Do not cite
one of these answers as chain-derived.

## Watching one

Seven series on `/metrics`, labelled by nest and entity:

| series | alert on |
|---|---|
| `nuthatch_entity_current` | 0 for long means it is not keeping up |
| `nuthatch_entity_seconds_since_progress` | the watermark has stopped moving |
| `nuthatch_entity_faulted` | the circuit stopped. Terminal, and the nest is quarantined |
| `nuthatch_entity_unavailable` | it holds no answer and is not being served |
| `nuthatch_entity_applied_through` | how far it has folded |
| `nuthatch_entity_rows` | how big it has become - watch it against `max_rows` |
| `nuthatch_entity_state_bytes` | what its circuit holds in memory, sampled at most once a minute (`NUTHATCH_ENTITY_STATE_SAMPLE_SECS`) |

A fault also pushes an `entity_fault` alert if a `[[alerts]]` sink watches that kind, so you find out
without polling. `faulted` and `unavailable` are separate because the response differs: one is dead,
the other is simply empty.

## Reading one

- `GET /derived/{entity}/{key}` - a keyed point read. A map lookup; it does not touch the SQL engine.
- `GET /derived/{entity}` - the first page, with provenance.
- `` `SELECT … FROM {entity}` `` on `/sql` - queryable by name, under the column names you wrote,
  joinable against decoded tables like any other relation.

Every response carries provenance saying the relation is incremental and how far it is applied, so a
caller can tell a current answer from a lagging one.

## Footguns

- **A relation is only cheap to *derive*, never cheap to *return*.** Selecting every maintained row
  still pays for those rows and is still bounded by the row cap. IVM does not repeal I/O.
- **A fault is terminal.** The entity does not restart itself, and the nest is quarantined behind it,
  because serving a frozen relation as though it were current is the failure mode this design exists
  to avoid.
- **Adding an entity to a nest with history costs a seed on the next restart**, not on the next block.
  Measure it before you add one to a nest whose restart time matters.
