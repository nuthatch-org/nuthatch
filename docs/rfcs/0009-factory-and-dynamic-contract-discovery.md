# RFC-0009: Factory and dynamic contract discovery

- Status: Implemented (2026-07-18)
- Author: Pete (cargopete)
- Date: 2026-07-16 (v1: 2026-07-14)
- Depends on: RFC-0001 (Implemented - decode registry, nest toml), RFC-0004
  (Implemented - the pipelined backfill this design must now compose with, §3)
- Blocks: DeFi-workload credibility at public launch (see revised priority)
- Priority (revised): NOT a blocker for v0.1.0-rc.1 or the GraphOps pilot (the
  graph-network contracts are static - no factories needed there). STRONGLY
  recommended before the public launch phase (RFC-0007 Phase 2): "can it index
  Uniswap?" is the first question every launch audience asks, and the honest answer
  should be yes, not "accepted RFC." Decide at tag time; do not slip the rc for it.
- Revision note: v2 syncs dependencies to shipped code, specifies discovery under
  the *pipelined* backfill (filter-version rule, §3), adds the ExEx simplification
  (§3a), repoints stale cross-references (v1's RFC-0010/0011 are not in-tree; the
  wildcard RFC is "future"), and adds `discovered_timestamp` now that
  `block_timestamp` ships.

## Abstract - unchanged from v1

Factory patterns for nests: a contract's event announces a child contract, and
Nuthatch indexes that child with a declared template - automatically, retroactively
during backfill, reorg-safely at the tip. The dynamic-data-sources capability of
subgraphs, without which most real DeFi protocols are unindexable.

## Motivation - unchanged from v1

(Static `[[contracts]]` cannot express Uniswap-class runtime deployment; table
stakes, not differentiation; the first feature whose state must itself survive
reorgs and be reproducible.)

## Goals / Non-goals - unchanged from v1

(Goals: toml-declared factories/templates; correctness across backfill/tip/reorg;
determinism - discovered set is a pure function of chain history, registry hash
extended; scale via shared tables and a single filter. Non-goals: per-child logic;
nesting beyond depth 3; bytecode/trace-based discovery.)

## Design

### 1. Nest definition - unchanged from v1

(`[[templates]]` + `[[factories]]` with watch/event/child_param/template/start;
validation at init/load; table naming `{template}__{event_snake}` shared across all
children, distinguished by the implicit `address` column.)

One addition: registry entries also record `discovered_timestamp` (from the
`block_timestamp` implicit column, shipped 2026-07-15) so the `{template}__children`
view answers "pools created this week" without a join.

### 2. The child registry - unchanged from v1

(Hot-store-persisted `(template_id, child_address) → {discovered_block,
discovered_log_index, parent_address, depth}`; discovery is a consumer of decode,
not a special path; reorg rule - registry entries with `discovered_block > B`
removed on rollback, child rows already covered by block-range rollback, invariant
asserted in tests; determinism - registry state at B is a pure fold over factory
events ≤ B, `registry_snapshot` hash written into each sealed segment's manifest
entry.)

### 3. Ingestion in the three regimes - REVISED for the pipelined backfill

**Tip (live):** unchanged from v1 - on discovery in block B, next poll's filter
includes the child; same-block creation+activity handled by re-scanning block B's
already-fetched logs against the updated registry before advancing the cursor (logs
in hand, no extra RPC).

**Backfill, sequential (`backfill_direct`):** unchanged from v1 - two-pass per
chunk: pass 1 fetches with the current filter and updates the registry from factory
events; pass 2 fetches the same chunk for newly discovered children only (usually
empty), merged by `(block, log_index)`. A child discovered in chunk N needs re-fetch
only within chunk N.

**Which logs a child gets (#2150, #2153, 2026-10-19).** Every regime applies one rule: a child's
logs are decoded from its announcing block B on, including those earlier in B than the
announcement, and none from before B. The tip re-scan above is that rule; the sequential backfill
used to keep a child's logs anywhere in the fetched chunk, so what was sealed depended on chunk
boundaries. Discovery is tentative until a window commits: a refused window registers nothing.

**Backfill, pipelined (`backfill_direct_pipelined`, K windows in flight) - NEW.**
The v1 design predates the pipeline and is insufficient under it: window N+1..N+K−1
may already be *fetched* with a filter that predates a child discovered while
*consuming* window N - and, unlike earlier windows, those later windows CAN contain
the child's events. Rule:

- Every fetched window records the `filter_version` (a monotonic counter bumped on
  each registry change) it was fetched under.
- Consumption stays strictly in block order (the existing determinism guarantee).
  When consuming window N discovers children (version v → v+1), any already-fetched
  window > N with `filter_version < v+1` gets a **supplemental fetch** before its
  consumption: children-only addresses × template topic0s over that window's range,
  merged by `(block, log_index)`.
- Windows not yet fetched simply use the current filter.
- Cost bound: supplemental fetches ≤ (discovering windows) × (K−1), each tiny
  (children-only). For factory-sparse ranges this is ~zero; for factory-dense ranges
  (a launchpad backfill) the adaptive answer is the §4 topic0-flip, which makes
  filters version-independent and eliminates supplemental fetches entirely - the
  pipeline then composes with zero changes.
- The path-equivalence discipline extends: a new test asserts pipelined-with-
  factories produces byte-identical segments to sequential-with-factories over the
  same range (the RFC-0004 `pipelined_backfill_matches_sequential` pattern, factory
  edition).

**Reorg:** unchanged from v1; the existing proptest gains a factory dimension
(random creation events inside reorged ranges; registry + tables converge).

### 3a. ExEx mode - NEW (simplification, not complication)

Under `ExExSource`, logs arrive from receipts in-process - there is no getLogs
filter at all. Factories in ExEx mode reduce to pure local registry-lookup
filtering: no two-pass, no supplemental fetches, no provider limits. Same-block
handling is inherent (the whole block's logs are always in hand). This makes the
sovereignty mode the *simplest* factory implementation, not the hardest - worth one
sentence in the docs, and it means §3's complexity is confined to the RPC source.

### 4. Filter management at scale - unchanged from v1, cross-reference repointed

(Above ~500 children, flip from address-list to topic0-only with local
registry-lookup filtering; automatic, logged, per-template override.) v1 noted this
shares a code path with "RFC-0011 wildcard indexing" - that RFC is not in-tree;
read as "the future wildcard RFC," and keep the code path shaped for it regardless
(the flip IS wildcard mechanics scoped to a registry).

### 5. Nested factories - unchanged from v1

(Watch may name a template; depth ceiling 3; cycles impossible by construction.)

## Serving and views - unchanged from v1

(Template tables are ordinary tables; auto-generated `{template}__children` view -
now with `discovered_timestamp` per §1.)

## Implementation plan - v1 steps with two insertions

1. Toml schema + validation + registry + unit tests.
2. Tip regime incl. same-block re-scan; live test against Uniswap V3 factory.
3. Sequential backfill two-pass; overhead measured on a 50k-block Uniswap V3 range
   (target <15% vs static-list of the same final set).
3a. **NEW:** pipelined composition (filter_version + supplemental fetches) + the
   factory path-equivalence test (pipelined ≡ sequential, byte-identical).
4. Reorg proptest extension; registry snapshot in seal manifest.
5. Filter-strategy flip + threshold tuning (and confirm flip mode makes the pipeline
   supplemental-fetch-free, per §3).
5a. **NEW:** ExEx-mode factory test against the bridge harness (registry-lookup-only
   path) - cheap now that the Source trait ships; the full-node soak stays with
   RFC-0003.
6. Docs + stories-page update.

## Testing and acceptance - v1 items plus

- v1: Uniswap V3 nest from the factory alone; spot-parity for 5 pools at a pinned
  block; same-block fixture; reorg proptest; overhead published; ≥1,000-child flip
  exercised; registry snapshot hash identical across sources.
- NEW: factory path-equivalence (pipelined ≡ sequential); supplemental-fetch
  accounting visible in bench output (the honesty rule: publish the factory
  overhead number under the pipeline, not just sequentially).

## Risks - unchanged from v1, one addition

(Provider getLogs quirks → topic0-flip; registry growth → revisit at ~10⁶ children;
template ABI drift → vendored-ABI stance, `--abi-from` workaround.) Addition:
**pipeline complexity creep** - if §3's filter-version machinery grows past ~a
screenful, the fallback is honest and cheap: factories force `--concurrency 1`
below the flip threshold and full pipelining above it (flip mode needs no
versioning). Ship the simple correct thing first; the supplemental-fetch
optimization can follow measurement.

## Open questions

1. v1 Q1 stands (discovery stays watcher-scoped; unscoped-emitter indexing belongs
   to the future wildcard RFC).
2. v1 Q2 stands (`end` conditions deferred; children are forever until demand says
   otherwise).
3. NEW: does the GraphOps platform want factories exposed as a per-nest capability
   flag for hosted tenants (resource-estimation reasons - a factory nest's row count
   is unbounded by construction)? Their call, their layer; our side already prints
   the pre-backfill estimate. Raise on the call, decide nothing in core.
