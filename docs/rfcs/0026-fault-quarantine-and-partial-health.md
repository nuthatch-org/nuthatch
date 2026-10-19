# RFC-0026: Fault quarantine and partial health - a roost survives its sick nests

- Status: **Implemented** (2026-07-27) - all three slices shipped.
  **Slice 1** (nest fault boundary): the three `?` sites in §1's table now quarantine the nest instead
  of the cursor; `prepare` moved inside the boundary; `TerminalFault` classifies the two no-retry
  faults; a `Supervisor` owns the live/quarantined partition that drives the cursor's min/max/union
  (§3.1). **Slice 2** (cursor fault boundary): `select_all`-aborts-everything replaced by
  `supervise_cursors` - a dead cursor is retired and logged, siblings keep indexing, and the roost
  exits only when every cursor is gone. **Slice 3** (health surface): `health::RoostHealth` is the live
  handle behind `/nests` (per-request health merge), a new roost-root `/ready` (503 on any quarantine),
  a per-nest `/<name>/ready` that answers for *that* nest, `nuthatch_nest_health` /
  `nuthatch_nest_quarantine_total` / `nuthatch_cursor_live` metrics, and `--fail-fast`.
  **Issue #147 is closed**: one chain's finality violation no longer takes another chain's cursor down.
- Author: Pete (cargopete)
- Date: 2026-07-27
- Depends on: RFC-0012 (per-nest isolation of storage/reorg/blast-radius; the shared cursor),
  RFC-0021 (one isolated cursor per chain - the unit this RFC makes independently killable).
- Blocks: RFC-0021 §3+ (further multichain slices assume a cursor's death is survivable),
  RFC-0022 (the distributed scheduler cannot place or drain a cursor it has no health signal for).
- Nature: **mini-RFC** - a bounded design decision, not a new capability. Scope is deliberately four
  questions (§3-§6); everything else is implementation detail.
- Origin: [issue #147](https://github.com/nuthatch-org/nuthatch/issues/147), found in the full-repo
  audit (concurrency dimension). Sprint
  [amiable-axolotl](https://github.com/nuthatch-org/nuthatch/blob/ede03ca7/docs/sprint-amiable-axolotl.md), tier 1.

## Abstract

`CLAUDE.md` states, as a non-negotiable: *"one nest's bad view or runaway factory, or one chain's stall
or reorg, must not harm another."* The code does not honour it. One nest's error kills its whole cursor,
and one cursor's death aborts every sibling cursor and exits the process. A reorg-below-finality on Base
takes down an Arbitrum nest that was indexing perfectly.

This RFC replaces *fail-everything* with **quarantine**: the smallest unit that can fail alone is
removed from the working set, its siblings keep indexing and serving, and its state is visible as
unhealthy rather than absent. It defines the fault taxonomy, the re-admission policy, the health
surface, and the one case where the process is still allowed to exit.

## 1. What actually happens today

Three propagation paths, all `?`:

**Per-nest errors kill the cursor** (`indexer.rs`, `roost_index_loop`):

| Line | Call | Consequence |
|------|------|-------------|
| `:512` | `nest.prepare(…).await?` | one nest's backfill failure kills the cursor **before the tip loop starts** - no sibling ever indexes a block |
| `:549` | `nest.rollback_reorg(ancestor)?` | one nest's finality-violation `bail!` (`:1706`) kills the cursor mid-reorg, with siblings possibly already rolled back |
| `:594` | `nest.process_window(…).await?` | one nest's decode/store/seal/IVM/webhook error kills the cursor; also carries `ensure_views_healthy`'s dead-circuit `bail!` (`:1670-1678`) |

**Cursor death kills the roost** (`roost.rs:385-399`): `select_all(ingests)` returns on the *first*
cursor's completion - success or failure - and the code then `abort()`s **every** cursor and returns,
exiting the process. The comment calls this "fate-share the server with every cursor … the
single-failure-boundary rule, held per cursor." It is the opposite: the boundary is the whole roost.

Note what is *already* right and must stay: `source.tip()` failure retries forever with
`escalate_stall` (`:525`), `source.logs()` failure retries after a sleep (`:608`), and `detect_reorg`
errors degrade to `debug!` (`:555`, `:1651`). Transient RPC trouble is not a fault. The gap is
everything that reaches a `?`.

## 2. The principle

**Quarantine the smallest unit that can fail alone, and never let a quarantined unit look healthy.**

Two corollaries that decide most of the detail below:

- **Isolation already exists in the data plane.** RFC-0012 gives every nest its own store, segments,
  and views; RFC-0021 gives every chain its own cursor and finality view. A nest's failure cannot
  corrupt a sibling's bytes *today*. The bug is purely in the **control plane** - error propagation -
  which is why this is a restructuring rather than a redesign.
- **Frozen data is not wrong data, but it is not fresh data.** A quarantined nest's tables are a
  correct view of the chain as of its last committed block. Serving that is fine; serving it *without
  saying so* is what `CLAUDE.md` forbids ("never serve stale data as if healthy"). So: keep serving,
  mark loudly. See §5.

## 3. Question 1 - does a nest fault differ from a cursor fault?

**Yes, and the distinction is which unit cannot make progress.** Two axes, four cases:

| | **Retryable** (transient) | **Terminal** (needs an operator) |
|---|---|---|
| **Nest fault** | `process_window` store/RPC/webhook error; `prepare` backfill failure | `rollback_reorg` finality violation (`:1706`), or a rollback that fails after retracting views (#2165); `ensure_views_healthy` dead IVM circuit (`:1670-1678`) |
| **Cursor fault** | *(none - tip/logs already retry in-loop)* | single-block-over-cap (`:603`); wrong-chain endpoint pool (see #150) |

Three rulings fall out:

1. **A nest fault quarantines the nest, never the cursor.** Its siblings on that cursor keep indexing.
   This is the case the audit found and covers every `?` in §1's table.
2. **A cursor fault quarantines the cursor** - and therefore, transitively, every nest mounted on it.
   Nests on *other* cursors are untouched. There is no such thing today as a cursor fault that is not
   also terminal: the two transient cursor-level failures already retry in-loop and must keep doing so.
3. **Terminal faults are not retried.** A finality violation re-`bail!`s on the next attempt by
   construction; a dead IVM circuit thread cannot be revived in-process. Retrying either is a busy-loop
   that spams logs and hides the operator's actual job. Terminal means *quarantined until restart*.

**The unit of quarantine is therefore the nest, with the cursor as the coarser fallback.** This is
exactly RFC-0022's scheduling unit one level down, which is the point: the scheduler will need to drain
a cursor, and draining is quarantine with intent.

### 3.1 The shared-cursor subtlety (the part that is easy to get wrong)

`roost_index_loop` derives its cursor from the nests: `global_next` is the **min** of `nexts` (`:560`),
the reorg reference is the **max** (`:539`), and the fetch filter is the **union** of every nest's
addresses/topics (`:567`). A quarantined nest must be removed from **all three**, or the quarantine is
worse than the crash:

- left in the **min**, a stuck nest pins `global_next` at its dead cursor forever - the whole cursor
  stalls while *appearing* alive. This is the failure mode that would make a naive "just log and
  `continue`" fix actively harmful.
- left in the **union**, the cursor keeps paying `getLogs` bandwidth for a nest that consumes nothing.
- left in the **reorg fan-out**, `rollback_reorg` is called on a nest whose state no longer advances.

So quarantine is *removal from the working set*, not a skip-flag on an iteration. Implementation
follows: partition `nests`/`nexts` into live and quarantined, and derive min/max/union from the live
partition only. If the last live nest on a cursor is quarantined, the cursor is quarantined (there is
nothing left to advance).

## 4. Question 2 - retry and backoff for a quarantined unit

**Retryable faults:** exponential backoff, ×2 from 5s, capped at 5 minutes, unbounded attempts. The cap
matters more than the curve - an operator restarting a wedged RPC provider should see recovery within
minutes without anyone typing anything.

**Terminal faults:** no retry. Quarantined until process restart, with the reason on the health surface.

**Re-admission is safe by construction, and this is worth stating explicitly.** A re-admitted nest
rejoins with its `nexts[i]` *unchanged* - i.e. behind. That pulls `global_next` back down to it, and
the siblings that ran ahead skip the re-fetched windows via the existing `if nexts[i] > to { continue }`
guard (`:580`). No nest re-processes a window it already committed; no nest skips one. The cost is a
re-fetch of the intervening range for the whole cursor, which is why the backoff cap is minutes rather
than seconds: re-admission is correct but not free.

**But "unchanged" is safe only once the nest's own checkpoints are checked** (#2165). A nest out of
the working set misses every reorg fanned out meanwhile, and the shared detection reads only the most
caught-up sibling, which has already re-indexed past the fork. So a nest that joins the cursor, at
startup, on mount or on re-admission, first runs `detect_reorg` against its own store and rolls back
if it must, before it indexes forward. A check that cannot reach the source quarantines it again.

**Attempt accounting is per unit and resets on success** - one committed window clears a nest's
counter. A nest that fails every third window is degraded, not quarantined; that is a job for metrics
and an operator, not for the supervisor.

## 5. Question 3 - how partial health is expressed

Today's surface cannot express "partly working", for a structural reason: the `/nests` roster
(`roost.rs:360-380`) is a `serde_json::Value` built **once at startup** and cloned per request. It must
become a live handle - an `Arc<RwLock<…>>` or equivalent - that the cursors update on state change.
That is the single largest mechanical change in this RFC.

**`GET /nests`** - each entry gains:

```json
{ "name": "lodestar", "chain": "arbitrum-one",
  "health": "indexing" | "quarantined" | "degraded",
  "quarantine": {
    "kind": "nest" | "cursor",
    "class": "retryable" | "terminal",
    "reason": "reorg to block N is below the sealed watermark M …",
    "since_unixtime": 1753574400,
    "attempts": 3,
    "next_retry_unixtime": 1753574700
  },
  "last_block": 21500000
}
```

`quarantine` is absent when `health` is `indexing`. The reason string is the underlying `anyhow` chain,
because that is what an operator needs to act.

**`GET /ready`** - roost-level, and it does not exist yet: the roost router (`serve.rs:201`) mounts only
`/health` and `/nests`; `/ready` (`serve.rs:117`) is per-nest and reads *global* `METRICS`, so in a
multichain roost it answers for whichever cursor polled last. That is a latent wrong answer independent
of this RFC. Rulings:

- **`/health` stays liveness** - plain `200 "ok"` while the process serves. Unchanged.
- **`/ready` becomes per-cursor-aware and is mounted at the roost root.** `200` when **every** cursor is
  indexing; **`503` when any nest or cursor is quarantined**, with a body naming them.
- **Per-nest `/<name>/ready` answers for that nest** - `503` if that nest is quarantined, `200`
  otherwise, so a consumer polling one nest is not misled by a sibling's fault.

503-on-any-quarantine is the conservative choice: a supervisor should treat a partly-broken roost as
not-ready and page someone, while the healthy nests keep serving reads to consumers who ask for them
directly. Readiness is advice to a supervisor; it does not gate traffic.

**Metrics** - `nuthatch_nest_health{nest,chain}` (1 indexing / 0 quarantined),
`nuthatch_quarantine_total{nest,kind,class}`, `nuthatch_cursor_live{chain}`. Enough to alert on
"anything quarantined" and to graph flapping.

**Logs** - quarantine and re-admission are `warn!`, with nest, chain, class, and the full error chain.

## 6. Question 4 - does the process ever exit?

**Almost never. Three cases, and only three:**

1. **The server stops** (bind failure, shutdown signal) → exit, as today. Serving is the roost's reason
   to exist; without it the cursors are indexing into a void.
2. **Every cursor is quarantined** → exit non-zero, after logging each reason. Nothing will ever advance
   again, and a restart is the only path that can help. Staying up to serve frozen data behind a
   permanent `503` is worse than dying honestly under a supervisor that will restart us.
3. **`--fail-fast`** (new flag, off by default) → exit on the first quarantine of any kind. This
   preserves today's behaviour exactly, for CI, deterministic tests, and operators who genuinely want
   fail-stop. Cheap to keep, and it makes the parity tests easy to write.

Otherwise: a quarantined nest or cursor never exits the process. Notably, **a cursor completing
successfully no longer ends the roost** - today `select_all` treats any completion as the end. A cursor
returning `Ok(())` (an empty nest list, `:503`) should simply be retired from the set.

## 7. Non-goals

- **No automatic repair.** Quarantine stops the bleeding; it does not re-seal segments, rebuild a dead
  circuit, or lower a finality depth. Those are operator decisions with data consequences.
- **No cross-cursor coordination.** A quarantine on chain A never influences chain B. That coupling is
  the very thing being removed.
- **No change to the data plane.** No store, segment, view, or reorg semantics change. If a slice needs
  one, the design is wrong - go back.
- **No scheduler.** Draining, placement, and re-placement across machines are RFC-0022's. This RFC only
  guarantees the health signal that RFC-0022 will consume.

## 8. Slices

Each ends runnable, with tests.

**Slice 1 - the nest fault boundary.** Partition `roost_index_loop`'s working set into live/quarantined;
derive min/max/union from the live partition (§3.1); classify errors at the three `?` sites (§1) into
retryable/terminal; backoff + re-admission (§4). `prepare` moves inside the boundary so a backfill
failure quarantines one nest rather than stillbirthing the cursor.
*Test:* two nests on one cursor, nest A forced to fail in `process_window`; assert A is quarantined,
B keeps committing windows, and A's tables are unchanged from its last good block.

**Slice 2 - the cursor fault boundary.** Replace `select_all`-aborts-everything (`roost.rs:385`) with
per-cursor supervision; retire completed cursors; exit only per §6.
*Test:* two cursors, cursor A `bail!`s below finality; assert cursor B keeps indexing and the process
stays up. (This is the issue's headline scenario and the acceptance test for the whole RFC.)

**Slice 3 - the health surface.** Live roster handle, `/nests` health fields, roost-root `/ready`,
per-nest `/ready`, metrics, `--fail-fast`.
*Test:* quarantine a nest, assert `/nests` shows it with a reason, roost `/ready` is 503, the healthy
sibling's `/<name>/ready` is 200, and its data still reads.

## 9. Acceptance

- A nest fault leaves every sibling on its cursor indexing, byte-identical to a run where the faulty
  nest was never mounted.
- A cursor fault leaves every other cursor indexing, byte-identical to a solo run of that chain.
- No quarantined unit is ever reported healthy on `/nests`, `/ready`, or in metrics.
- A quarantined-then-recovered nest converges to the same tables as a nest that never failed
  (re-admission correctness, §4).
- `--fail-fast` reproduces today's behaviour exactly.
- Single-nest, single-chain `dev` is unchanged - no new failure semantics for the default path.

## 10. Open questions (implementation, not scope)

- Should a nest that flaps - quarantined and re-admitted N times in a window - escalate to terminal?
  Leaning yes with a generous N, but it needs a real flapping trace before picking one.
- Does `--fail-fast` belong on `dev` too, or only `roost`? A solo nest has no siblings, so its
  fail-stop behaviour is already what this flag describes.
- Where does the quarantine reason live across a restart - purely in-memory, or a breadcrumb in the
  nest's store so `/nests` can say "quarantined last run, reason X" after a supervisor bounce?
