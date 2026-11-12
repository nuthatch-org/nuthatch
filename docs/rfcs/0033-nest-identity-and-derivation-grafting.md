# RFC-0033: Nest identity and derivation grafting - editing a nest without re-indexing it

- Status: **Implemented** (slices 1-3, 5 and 6; slice 6 shipped in v2.0.0, 2026-08-06; slice 4 deferred)
- Author: Pete (cargopete)
- Date: 2026-08-04
- Depends on: RFC-0012 (the content-addressed nest bundle - the NID this RFC builds *below*),
  RFC-0018 §1 (authored SQL views in `views/` - the derivations this RFC hashes), RFC-0013 (DuckDB as
  the engine whose version enters the key), RFC-0019 (registry - where a re-hashed nest is published).
- Supersedes: RFC-0020's `nest diff` and `nest upgrade` (see §9). RFC-0020's *problem* - the N-1
  resync tax - stays solved; this RFC solves it generally instead of per-command.
- Related: RFC-0032 keys datasets by NID. This RFC adds a **finer key below it**. The two are
  independent and compose: the NID stays the package identity (mounts, refcounting, the versioning
  boundary), the derivation hash is the reuse identity.
- Nature: **full RFC**, and the one with real correctness risk. Every rejected shortcut in §4 and §7
  is a shipped bug in someone else's system.
- Origin: the 2026-08-04 architecture session (chris, GraphOps) plus the Fable deep-research pass that
  answered R-1. Decisions logged in the session's working notes (unpublished) §4 and §10.

## Abstract

Content addressing gives an exact answer to "is this the same nest?" and it is unforgiving: change one
character in one view and the NID changes, so the nest is a different nest and its data is a different
dataset. That is correct, and on its own it means **every edit is a full re-index** - the precise tax
RFC-0020 set out to kill.

Grafting is the escape. Below the package identity sits a per-derivation reuse key. When a nest is
edited, each derivation is re-keyed independently; those whose key is unchanged **reuse their existing
data**, and only the genuinely-changed subgraph recomputes. Edit one view in a nest of twenty and
nineteen graft.

Two things make this hard rather than obvious, and both are settled here:

1. **Text is never sufficient.** The key binds to *resolved source identity* - chain, contract, event
   signature, ABI hash, schema version - not to names in SQL. PostgreSQL learned this in
   `pg_stat_statements`, which hashes table OIDs rather than table names, and the reason is that a name
   can point at something new without the text changing.
2. **We cannot prove two different-looking queries equal, and we must not try.** Query equivalence is
   NP-complete under set semantics (Chandra-Merlin) and *undecidable* for unions of conjunctive queries
   under bag semantics, which is what SQL actually is. So the matcher is strictly syntactic and errs
   toward missing true matches. **Soundness over completeness is forced, not chosen.**

v1 is whole-derivation reuse only. Partial block-range reuse is deferred to v2 on purpose: it is
exactly where the shipped bugs live.

## 1. The two axes, stated once

| | **NID (package identity)** | **Derivation hash (reuse identity)** |
|---|---|---|
| Granularity | The whole nest bundle | One derivation |
| Computed over | Canonical manifest of all authored inputs | Canonical plan + resolved sources + inputs' hashes |
| What it answers | "Is this the same nest?" | "Can this data be reused?" |
| Used by | Mounts, refcounting, sharing, versioning (RFC-0032) | Grafting only |
| Changes when | Any authored byte changes | Only this derivation or its ancestry changes |

Whole-package identity **over-invalidates by construction**, which is why a second axis is needed
rather than a cleverer NID. Substreams, Dagster (`code_version` plus a recursive `data_version`) and
Unison all converge on per-derivation transitive hashing; we are not inventing a shape here.

Grafting is therefore **per-derivation and transitive** - a Merkle hash over the derivation DAG. A
derivation's hash includes its inputs' hashes, so a change propagates downstream and nowhere else.

## 2. The reuse key

```
reuse_key = H(
    cache_format_version
  ‖ normalized_plan                  -- canonical form of this derivation, see §3
  ‖ [ reuse_key(input) for input ]   -- transitive, in canonical order
  ‖ resolved_source_identity         -- see §2.1
  ‖ block_range_covered
  ‖ engine_id ‖ engine_version       -- see §2.2
  ‖ finality_state                   -- see §2.3
)
```

### 2.1 Resolved source identity, never names

`(chain_id, contract_address, event_signature, abi_hash, schema_version)`.

A view reading `usdc__transfer` must key on *what that table is*, not on the eight characters naming
it. Re-`init` a nest against a different contract under the same name and the text is byte-identical
while the data is unrelated. This is the correctness trap the research pass confirmed, and the
precedent is direct: `pg_stat_statements` hashes OIDs, not names. **Bind to identity, never to a name.**

### 2.2 Engine and version are correctness, not bookkeeping

**DuckDB changed CTE semantics at 1.4**: CTEs became materialized by default where they had been
inlined. PostgreSQL flipped the same switch the other way at 12. Same SQL, different engine version,
different evaluation, potentially different results.

A key without the engine version is unsound **across our own upgrades**, which is the failure mode
that would be discovered in production by an operator rather than in CI by us.

### 2.3 Finality state

Data above finality is provisional. A key covering an above-finality range must include the **block
hash**, so that a reorg invalidates it by construction rather than by our remembering to.

## 3. The equivalence class: narrow and syntactic

Only these normalisations are applied. Everything else is significant.

**Safe:**

- Whitespace and comments
- Alias α-renaming
- ~~Inner join written as a comma versus explicit `JOIN`~~ - **withdrawn, see below**

> **Amended after building slice 1 (2026-08-05).** The comma-versus-explicit normalisation is
> **not safe and is not implemented.** Against DuckDB's own AST the two forms are not a renaming: the
> comma form yields `ref_type: "CROSS"` with the predicate in `where_clause`, while the explicit form
> yields `ref_type: "REGULAR"` with the predicate in `condition`. Normalising them means *moving a
> predicate between clauses*, which is a semantic rewrite rather than a renaming - and proving that
> rewrite sound in general is the equivalence problem §2 says we are not attempting. It happens to
> hold for two inner joins; "happens to hold" is not the standard here. Cost of withdrawing it: an
> author who rewrites a comma join as an explicit one recomputes once. Cost of keeping it: a class of
> false match.
>
> Two other things the AST settled, both in our favour and neither needing code:
> - **Literal types are already distinguished.** `5/2` carries `INTEGER` where `5/2.0` carries
>   `DECIMAL(2,1)`, so §3's integer-division trap cannot be normalised away by accident.
> - **Whitespace and comments collapse for free.** They touch exactly one field, `query_location`, a
>   byte offset into the source. Dropping it *is* the normalisation - no tokenizer, and no risk of
>   stripping a `--` that lives inside a string literal.
>
> And an implementation note worth keeping: canonicalisation runs on **DuckDB's own parser**
> (`json_serialize_sql`), not a second one. A separate parser could disagree with the engine about
> what a statement means, and a disagreement in the permissive direction is a false match. Using the
> executing engine's parse makes that class of error unreachable rather than unlikely - and it
> satisfies §2.2 structurally, since a DuckDB release that changes how a statement parses changes the
> key by construction.

**Explicitly unsafe, preserved verbatim, with the reason each one is a trap:**

| Normalisation | Why it is refused |
|---|---|
| Projection order | Tuples are positional; reordering changes the result |
| Literal types | `5/2` = 2, `5/2.0` = 2.5 - integer division makes a literal's *type* semantic |
| `AND` reordering | SQL guarantees no short-circuit, so reordering can change *whether it throws* |
| `DISTINCT` | Bag ↔ set is exactly the boundary where the theory becomes undecidable |
| `ORDER BY` | Changes the result of anything downstream that depends on row order |
| CTE ↔ subquery | See §2.2 - the engine treats them differently, and changed how across versions |

No prover, no cost-based matching, no "these look equivalent". A missed match costs a recompute. A
false match ships wrong data silently, and is discovered by a user.

## 4. The hard refusal list

Never matched, never reused, at any granularity:

- **Volatile and nondeterministic functions** - `now()`, `random()`, and any UDF not on an explicit
  determinism whitelist. Every production cache surveyed refuses these, and Trino #22533 is what
  happens when you do not: a materialized view over `CURRENT_TIMESTAMP` served a frozen timestamp,
  because snapshot-based freshness has no concept of time-dependence.
- **Float aggregation where order matters** - IEEE-754 addition is not associative, so the same rows
  in a different order give a different sum.
- **Anything relying on implicit row order.**
- **Provisional above-finality data without a block hash in the key** (§2.3).
- **Holistic aggregates for partial reuse** (§7).

Refusal is loud: the derivation recomputes and the reason is reported, so an author can see *why* their
view never grafts instead of wondering why edits are slow.

> **Amended after building slice 3 (2026-08-05).** Two things this section needed that it did not say.
>
> **A bare volatile keyword is not a function node.** `SELECT current_timestamp` - standard SQL, no
> parentheses - parses as a `COLUMN_REF` in DuckDB's AST, not a `FUNCTION`. A denylist that only
> inspects function calls misses it completely, and the derivation then caches a frozen timestamp
> forever, which *is* Trino #22533 in one line of SQL. The check now also inspects **unqualified,
> single-element** column references. Qualified ones (`t.current_date`) stay untouched, since those
> are ordinary columns; a column genuinely named `current_date` gets over-refused, which is the safe
> direction.
>
> **Float aggregation cannot be decided from the parse**, so it is not in the static list. Whether
> `sum(x)` is float-associative depends on `x`'s *type*, which needs binding the view against the
> nest's schema rather than parsing it. Detecting it half-way would be worse than not at all -
> flagging every `sum()` makes almost every useful view never-graftable, and flagging only float
> *literals* would create confidence that a `DOUBLE` column had been checked when it had not. **§10's
> determinism gate is the backstop and is the stronger instrument**: it catches float
> non-associativity empirically, along with every other nondeterminism the list does not name. The
> right division of labour is a denylist for what we can name and a gate that actually runs the query
> twice - not an allowlist over DuckDB's several hundred scalar functions, which would be wrong on
> day one and wrong differently after every upgrade.

## 5. Early cutoff and backdating - the largest win

This is the mechanism, and it is the thing Substreams does **not** have.

After a re-hash, recompute the derivation over a **bounded probe range** and compare the *output*
digest against the existing data's. If unchanged, alias the new identity to the existing dataset and
**stop propagation downstream** - descendants never recompute, because their input did not actually
change even though its name did.

Precedent: Nix content-addressed derivations, and Salsa's red-green backdating.

The counter-example is instructive. Substreams is purely *input-addressed*, so a cosmetic key change
busts the global cache: their v1.9.1 `initialBlock` change "caused more trouble" and was reverted for
exactly this reason. Input addressing cannot tell "the recipe changed" from "the output changed".

Early cutoff is also **what makes RFC-0034 phase 2 work.** An allowlist edit flips the NID, every
derivation recomputes over the probe range, every output is identical, everything backdates, nothing
re-indexes. That was hand-waved when RFC-0034's sequencing was decided; this section is how it is
actually done.

## 6. Cycles

Derivations read decoded events and other derivations, never themselves, so the graph is a **DAG by
construction**.

A cycle is a **load-time refusal** with the cycle named - not a runtime problem, not a fixpoint, not a
design question. Decided rather than debated.

## 7. Partial reuse is v2, and here is why

**v1 is whole-derivation reuse only.** A derivation's stored data is reused when its full reuse key
matches. No partial block-range reuse, no compensation queries.

That covers the common case (edit one view, everything else grafts) and avoids an entire bug class.
When v2 comes, the rules are already fixed:

- **Block-range only, below finality only, decomposable aggregates only.** Reuse the overlap, compute a
  compensation query for the delta.
- `sum`, `count`, `min`, `max`, `avg` decompose. **`median`, percentiles and `COUNT(DISTINCT)` do
  not** - and CALCITE-1984 is a shipped bug that is precisely this: a rewrite turned count-distinct
  into count and silently changed the answer.
- Above the reorg threshold: refuse entirely, or key on block hash so a reorg invalidates by
  construction. graph-node's precedent is the same - grafts are disallowed within the reorg threshold.

Partial reuse is where watermark-boundary and holistic-aggregate bugs live. It waits until the key is
proven in production.

## 8. The key schema is a versioned, frozen contract

- Every cache path embeds a **`cache_format_version`**. Changing what goes into the key is a global
  invalidation, gated behind a version bump and a migration path - never a silent change of meaning.
- **Cached failures get their own TTL.** Substreams cached deterministic WASM errors *forever* until an
  expiry was added.
- Cache entries are defended with **hash and size** (Bazel's discipline), and an untrusted writer never
  produces an authoritative entry.

## 9. What happens to `nest diff` and `nest upgrade`

Both are subsumed by grafting and **removed in 2.0** (RFC-0035).

But they carried something grafting does not replace: the **compatible-versus-breaking
classification**. Grafting makes the *data* free; it says nothing about whether a consumer's queries
still work.

Resolution, consistent with the runtime-intelligence principle: **the runtime detects a breaking change
at mount time and refuses or warns.** Same information, no command to remember, no ceremony. An
operator mounting a nest whose schema drops a column the previous NID exposed is told at mount, not
after a dashboard breaks.

## 10. The determinism gate

Grafting is only as correct as the determinism of the derivations it reuses. So CI runs every
derivation **twice over the same finalized range and diffs the outputs**. Anything that fails is
refused for caching entirely.

This sits directly beside CLAUDE.md non-negotiable 4, and it is cheap: finalized ranges are exactly
where re-execution is supposed to be free.

## 11. Slices

| # | Slice | Ends with |
|---|---|---|
| 1 | **Hash the plan.** Canonical normalisation (§3) + `resolved_source_identity` (§2.1) + engine/version (§2.2). No reuse yet, just a stable key. | Two byte-different-but-α-equivalent views hash identically; a re-`init` against a different contract under the same table name hashes *differently*. Both as tests. |
| 2 | **The DAG.** Build the derivation graph from `views/`; transitive hashing; cycle refusal at load (§6). | A nest with a diamond dependency hashes correctly, and a hand-written cycle is refused by name. |
| 3 | **Refusals.** The hard list (§4) and the determinism gate (§10). | A view calling `now()` is reported as never-graftable, with the reason. CI fails a nondeterministic derivation. |
| 4 | ~~**Whole-derivation reuse.**~~ **Deferred - see below.** | ~~Edit one view in a nest of twenty; nineteen graft; the RPC request count is zero.~~ |
| 5 | **Early cutoff.** Adopt an existing dataset when the new identity produces identical data (§5). | A cosmetic edit - a comment, a view rename, a doc change - that moves the NID re-indexes *nothing*. |
| 6 | **Mount-time breaking-change detection** (§9), and removal of `nest diff` / `nest upgrade`. | Mounting a schema-incompatible successor warns or refuses with the incompatibility named. |

Slices 1-3 ship no user-visible behaviour and must not be skipped for that reason: they are the
correctness of everything after them.

> **Slice 4 deferred, slice 5 promoted (2026-08-05).** Checked before building, and slice 4's
> acceptance criterion **cannot fail**: it is already true and always has been.
>
> Authored views are **not materialised**. `define_nest_views` defines them per query on an ephemeral
> in-memory DuckDB over the sealed segments and the hot tip; nothing persists them. So editing a view
> has never cost an RPC request *for the view*, and "the RPC request count is zero" passes against a
> build containing no grafting at all. Slice 4's premise - "match the key, reuse the data" - has no
> data to match, because the derivation was never stored.
>
> **What a view edit actually costs** is the thing to fix. Views are pinned in the blob, so editing
> one moves the NID; under RFC-0032 the dataset lives at `data/<nid>/`, so the new identity resolves
> to an empty directory and **the chain data re-indexes**. The cost was never recomputing a view - it
> is re-fetching everything, because the dataset moved.
>
> That is early cutoff's job, so slice 5 comes next and slice 4 waits for RFC-0018 §3's hot
> incremental views, which is the first thing nuthatch will have that is a *stored* derivation.
> Slices 1-3 are not wasted: early cutoff needs exactly the DAG and the keys they built, to know which
> derivations moved and whether their outputs did.
>
> **Amendment (2026-08-24): RFC-0041 now specifies that derivation.** Its first implementation keeps
> entity state rebuildable rather than durable, so slice 4 still has nothing persistent to graft.
> RFC-0041 slice 4 activates this slice only after measured restart cost justifies immutable entity
> checkpoints: unchanged entity keys graft, changed nodes and descendants rebuild from adopted facts,
> and no definition edit refetches chain history.
>
> The general lesson, since this is the third acceptance criterion this RFC series has had to correct:
> **a criterion phrased as the absence of an effect passes trivially when the mechanism is missing.**
> Prefer one that fails loudly against today's build before the work starts.
>
> The same shape showed up four times in the *tests* while building slices 1-5, each time caught only
> by deliberately breaking the code and re-running:
>
> - "a real table name is never renamed" passed against a renamer that renamed everything - twice,
>   because the dangerous path needs *both* a declared alias and an unaliased qualifier before it is
>   reachable at all.
> - "an edit propagates downstream and nowhere else" passed against a build with transitivity removed
>   entirely, because the node it edits has no descendants. It asserts isolation, not propagation.
> - "a cosmetic edit adopts the existing dataset" passed against a build with the exclusion list
>   removed, because the edit was made before the copy and both sides ended up identical.
>
> None of these were subtle in hindsight and all four would have shipped. **Mutation-check any test
> whose failure mode is silent** - which, in a reuse cache, is all of them.

## 11a. Cross-nest segment reuse (amendment, 2026-08-05)

Chris's note the rest of this RFC has not delivered: *"it would work across all the nests in the
system ... if two entity hashes across any nest are the same, you can reuse the data across."*

### Why dataset-level identity cannot do it

RFC-0033 §5's data identity is computed over a nest's **authored inputs**, and `nuthatch.toml`
contains `[nest] name`. Two nests indexing the same contract under different names therefore have
different data identities, and never adopt each other's data. Dataset-level sharing works *within* a
nest's lineage - a cosmetic edit adopts its predecessor - and essentially never fires *across* nests.

That is not a bug in the data identity. It is the wrong **granularity**: the unit two different nests
have in common is not the dataset, it is the **table**.

### The mechanism already exists

Sealed segments are already content-addressed (RFC-0009): the file is `{table}-{hash}.parquet` where
the hash is over the Parquet bytes, seal is already idempotent on `(table, hash)`, and the manifest
already references each segment by `hash` **and** `file`. Two nests decoding the same contract with
the same binary produce **byte-identical segment files** - that is the determinism guarantee, and it
is already tested.

So cross-nest reuse is not a new mechanism. It is moving the segment store up one level:

```
before   <root>/data/<nid>/segments/{table}-{hash}.parquet     one copy per dataset
after    <root>/segments/{hash}.parquet                        one copy per distinct segment
         <root>/data/<nid>/segments/manifest.json              unchanged: references by hash
```

Precedent is the same as §5's: Nix store paths and OCI layers. A dataset stops *owning* bytes and
starts *referencing* them.

### What this is keyed by

The **segment content hash**, not a derivation key. Deliberately: the segment hash is already computed,
already proven byte-stable for identical inputs, and requires no matching logic at all - two datasets
either produce the same bytes or they do not. Introducing a second, cleverer key here would reopen the
equivalence problem §2 exists to avoid.

The table name stays out of the shared path. `{table}-{hash}` was per-dataset disambiguation; in a
shared store the hash alone is the identity, and two tables that genuinely produced identical bytes are
identical data.

### The dangerous part: `prune`

Today `prune` deletes a dataset directory wholesale. With a shared store that would delete **bytes
another dataset still references** - the one data-loss failure mode in this design, and the reason it
gets designed rather than patched.

Rules, mirroring RFC-0032 §5's refcount:

1. A segment's refcount is **derived**: the number of dataset manifests referencing its hash. Never
   stored - a stored count drifts, and here drift deletes data.
2. `prune` removes a dataset's *manifest and hot store* first, then collects segments no surviving
   manifest references. Manifest-first, so an interrupted prune leaves orphaned bytes (recoverable)
   rather than dangling references (not).
3. A segment with **no** referencing manifest is collectable, and like a dataset it is only collectable
   - an operator still has to ask.

### What it does not change

- **Sealed segments stay immutable.** Sharing never mutates; a second nest referencing a segment writes
  nothing. The RFC-0009 non-negotiable holds untouched.
- **The ≤2 GB per-cursor budget.** This is disk, not RAM.
- **Solo `nuthatch dev`.** A single nest with no runtime root has no shared store and keeps its
  segments where they are. Sharing is a property of a runtime hosting several nests, not of a nest.
- **Tenancy.** Two tenants may reference one segment for the same reason they may share a dataset: the
  bytes are provably identical, so there is nothing to leak that either did not already have.

*(Annotated 2026-11-12: a shared segment that fails its hash is now quarantined, for every dataset that names it; see [the 5.21 release notes](../releases/v5.21.0.md).)*

### Slices

| # | Slice | Ends with |
|---|---|---|
| A | The shared store: seal writes `<root>/segments/<hash>`, readers resolve through it, per-dataset manifests unchanged. | Two nests indexing the same contract hold **one** copy on disk, proven by counting files. |
| B | Segment refcounting in `prune`, manifest-first collection. | Pruning one of two nests that share segments leaves the other's data **readable**. This is the test that matters. |
| C | `migrate` relocates existing per-dataset segments into the store. | An existing deployment migrates without re-indexing and without duplicate bytes. |

Slice B is where the risk is. It should not be built after A ships - a shared store with a `prune` that
does not understand sharing is strictly worse than no sharing at all.

## 12. Risks

**A false match is silent and ships wrong data.** This is the only risk that matters, and the whole
design is shaped around it: syntactic matching only, bind to identity not names, engine version in the
key, an explicit refusal list, and a determinism gate in CI. If a rule here ever looks like unnecessary
conservatism, §3 and §4 name the shipped bug it prevents.

**The key schema is easy to extend and hard to un-extend.** Hence §8. Adding a field is a global
invalidation; treat it as a migration, not a patch.

**Grafting graduates from optimisation to prerequisite** once RFC-0034 phase 2 lands, because at that
point a routine security tweak flips the NID. That is a sequencing constraint on RFC-0034, and it is
recorded there too.

## 13. Status

Draft. R-1 was answered by research on 2026-08-04 and every design question it raised is decided
(working notes §10, unpublished). Nothing here is waiting on anyone.
