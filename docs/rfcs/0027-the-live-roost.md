# RFC-0027: The live roost - mounting and unmounting nests without a restart

- Status: **Implemented** (2026-07-28) - all 7 slices
- Author: Pete (cargopete)
- Date: 2026-07-28
- Depends on: RFC-0012 (per-nest isolated stores, the shared cursor, nest-as-bundle - the deploy unit
  this RFC makes hot), RFC-0021 (one isolated cursor per chain - the unit a mount joins), RFC-0026
  (the health/quarantine substrate and `supervise_cursors` - this RFC extends both), RFC-0019
  (registry resolution, for mounting by `name@version`), RFC-0020 (segment reuse - the fast admission
  path).
- Blocks: RFC-0022 §3 (the distributed control plane cannot converge desired state onto a worker that
  can only change its nest set by restarting).
- Nature: **mini-RFC** - a bounded design decision over machinery that already exists. Scope is four
  questions (§3-§6); everything else is implementation detail.
- Origin: the operator conversation with GraphOps. Onboarding or removing a tenant's nest currently
  requires restarting the process that hosts every *other* tenant's nest.

## Abstract

A roost's nest set is frozen at boot. `roost::dev` reads `roost.toml`'s `nests` list once, groups the
nests by chain, spawns one cursor per group, and builds a static `axum` router. Adding or removing a
nest means editing TOML and restarting - which stops every co-tenant nest in the runtime.

For an operator running nests on behalf of others, that turns routine onboarding into a maintenance
window, and it makes the blast radius of a *configuration* change larger than the blast radius of a
*fault* (RFC-0026 made faults survivable; config changes are not).

This RFC makes the nest set mutable at runtime: **mount** installs and starts a nest into a live roost,
**unmount** drains and removes one, and neither touches a sibling. It settles admission (what may be
mounted, and against what budget), the catch-up handshake (how a new nest joins a cursor that is
already at tip), the control surface (API, CLI, and where desired state lives), and unmount semantics
(drain versus kill, and what happens to the data).

It is also RFC-0022 §3 with the distribution removed. That section already specifies API-driven nest
lifecycle with no process restarts, for the distributed plane; those semantics are entirely local, and
building them embedded first proves them without waiting for Postgres.

## 1. What actually happens today

**The nest set is read once.** `roost::dev` (`roost.rs:258`) loads every name in `roost.toml`,
`group_by_chain`s them, and hands each group to `indexer::spawn_roost`. Nothing re-reads the config.

**The router is immutable.** `serve::run_roost` (`serve.rs:230`) loops `app.nest("/{name}", …)` over the
states it was given and calls `bind_and_serve`. `axum::Router` is composed before it is served; there is
no insertion point afterwards.

**The roster is a startup snapshot.** `roost.rs` builds `roster_entries` once as a `serde_json::Value`.
RFC-0026 already had to work around this, merging *live* health into the *static* roster per request
(`serve::merge_roster_health`) precisely because a boot-time snapshot cannot express a runtime change.
That is the same structural problem this RFC finishes off.

**Admission control is startup-only.** The per-cursor RSS projection and the refusal above `max_rss_mb`
(`roost.rs`, per group) run once, before anything starts. There is no code path that asks "may this nest
join a *running* cursor?".

**The supervisor exits when the cursor set empties.** `supervise_cursors` (`roost.rs:433`) loops until
`ingests` is empty and then returns, ending the process. It cannot accept a cursor spawned later, and it
cannot distinguish "every cursor died" from "the operator removed the last nest".

Note what is already right and must be preserved: stores are per-nest and isolated, a nest fault
quarantines the nest, a cursor fault quarantines the cursor, and health is a live handle read per
request. This RFC adds a third state transition to a machine that already has two.

## 2. The principle

**The cursor owns its nest set; the API sends it commands.**

Every mutation is a message the cursor drains at a **window boundary** - never a mutation applied from
an HTTP handler. This is not fastidiousness. The cursor's loop computes `global_next` as a min over its
nests, detects reorgs once, and fans rollback out to every nest; mutating the set underneath that
mid-window would produce a rollback applied to a nest that was not present for the roll forward. A
window boundary is the only point at which the set is quiescent and the invariant "every live nest has
committed the same windows" holds.

Two corollaries:

- **Mount and unmount are asynchronous.** The API accepts a command and returns `202` with a handle; the
  roster reports the resulting state. An operator polls `/nests`, exactly as they would poll a
  reconciliation loop. Pretending a mount is synchronous would mean blocking an HTTP request on a window
  boundary and a backfill.
- **The budget is enforced at admission, not observed afterwards.** `CLAUDE.md`'s per-cursor ceiling
  stays a *refusal*, or it stops being a budget.

## 3. Question 1 - what may be mounted into a running roost?

Three admission classes.

**(a) Caught-up mount (the fast path).** A nest arriving with sealed segments that already cover history
to at least the cursor's finalised position - the shape `nest load` produces from a bundle, and exactly
what RFC-0020 slice 4's segment reuse constructs. It needs a short catch-up over the unsealed remainder
and joins within a window or two. This is the common operator case: redeploying a known nest, or
mounting one whose history another roost already produced.

**(b) Cold mount.** A nest starting at its contracts' deploy blocks with nothing sealed. Admitted, but it
must catch up before it joins the shared cursor (§4). A cold mount of a from-genesis nest is a backfill
with a mount at the end of it, and the API should read that way rather than pretending otherwise.

**(c) Refused.** Three refusals, all hard:

| Refusal | Why | Response |
|---|---|---|
| the cursor's projected RSS would exceed `max_rss_mb` | the per-cursor budget is a non-negotiable | `507` + the projection and the ceiling |
| the nest's chain is not declared by the roost | a cursor needs a verified endpoint set; adding a chain live is a non-goal (§7) | `409` |
| the name is already mounted | mounting over a live nest is an upgrade, and that is RFC-0020's job | `409`, pointing at `nest upgrade` |

Admission re-runs the existing `estimate_nest_rss_mb` against the *target cursor's* current membership.
The projection is deliberately rough (RFC-0012 §3), so it is a guard against the obviously-too-big, not
a precise allocator. Operators provision against the measured `rss_bytes` on the roster; the check exists
so a careless mount cannot silently blow a ceiling the brief calls non-negotiable.

**Verification is not optional.** A mount resolves through the same path as `nest load`: manifest format,
per-file hashes, and the decode registry regenerated from the inputs matching the manifest. A bundle that
fails verification is refused before anything is written into `nests/`. Hot mounting must not become the
one door into the runtime that skips the checks the cold door enforces.

## 4. Question 2 - how does a new nest join a cursor that is already at tip?

The cursor is at block N. The new nest's next unindexed block is M, and M is usually far below N.

**The tempting answer is wrong.** Splicing the nest straight into the shared set does work correctly:
RFC-0026 §4 established that a nest rejoining with its `next` behind pulls `global_next` down, and
siblings skip the re-fetched windows through the existing `if nexts[i] > to { continue }` guard. Nothing
double-processes and nothing is skipped. But the *cost* is that the whole cursor re-walks the range from
M to N. For a re-admitted nest that is minutes behind, that is the accepted price. For a nest mounted at
its deploy block, it drags every co-tenant back through years of history. Correct, and unusable.

**The ruling: a two-phase join.**

**Phase 1 - private catch-up.** The mounted nest is registered, routed, and visible on the roster with
`health: "catching-up"`, but it is *not* in the cursor's fan-out set. It runs its own backfill task over
the same `Source` (the shared RPC pool, so its load is accounted and its failover behaviour is the one
the operator configured), using the existing backfill path including `--seal-direct` semantics. Its
faults quarantine the nest and nothing else - the RFC-0026 boundary already covers it.

**Phase 2 - splice.** When the nest's `next` comes within `join_threshold` blocks of the cursor's
`global_next` (default: one window), it requests a join. The cursor drains the request at the next window
boundary and adds it to the live set with its `next` unchanged. The RFC-0026 re-admission math then does
exactly the right thing: `global_next` dips by at most the threshold, siblings skip the re-fetched
windows, and within a window or two everything is aligned. The nest flips to `health: "indexing"` and its
`/<name>/ready` starts answering `200`.

Two consequences worth stating:

- **A catching-up nest serves.** Its routes exist and its data is queryable from the first block it
  indexes. It reports its real `last_block` and it is not `ready`. Serving partial data honestly beats
  404ing a nest an operator can see on the roster, and consumers already have `last_block` and `/ready`
  to decide with.
- **Catch-up shares the RPC pool and must not starve the tip.** The private backfill takes its own
  concurrency, defaulted low (2) and separately configurable. A mount is a background job; the cursor's
  tip-following is the foreground one. Getting this backwards would mean an onboarding degrades every
  co-tenant's tip lag, which is the exact failure the whole RFC exists to prevent.

## 5. Question 3 - the control surface, and where desired state lives

**API** (under the existing per-roost admin surface, so `--no-admin` disables it wholesale):

| Route | Does |
|---|---|
| `POST /_admin/nests` | mount. Body: `{ "name": …, "nid": <content address, optional> }`. `200` + the roster entry, or a §3 refusal |
| `DELETE /_admin/nests/{name}` | unmount (§6). `?purge=true` to delete the data too. `202` |
| `GET /nests` | the roster, gaining `health: "catching-up"` and `mounted_at` |

> **Amendment (#550).** This row originally specified `{ "source", "expect" }` and `202`; the shipped
> handler (`src/runtime.rs`) takes `nid` and returns `200`, and the board ruled the code right on both
> counts rather than the document. `nid` is the content address a nest's data is already keyed by
> (`CLAUDE.md`), so it is the field consistent with the data layer, not a shortcut that happened to
> ship; `expect` is redundant once the identifier already *is* the hash it would assert. `nid` is
> optional only because a nest already on record from a prior mount resolves without it - omitting it
> against a fresh `mounts.toml` runtime, with no record to fall back on, is #517, not a supported
> no-arg mount. `202` was wrong on its face: the handler awaits the mount and only builds a response
> once it has returned and been persisted to `mounts.toml`, so the work is done before the caller is
> answered, which `202 Accepted` denies by definition. `source` is not dropped, only moved - see the
> open question below.

> **Amendment (#2144).** The table above and the #550 amendment are both superseded;
> [docs/admin-api.md](../admin-api.md) is the contract. Unmount takes `?reclaim=true`, not `?purge=true`,
> and an unknown `purge` key is ignored, so it unmounts and keeps the data. A mount answers `202` with a
> job (#1544) and `200` only with `?wait=true`. `nuthatch roost mount` and `unmount` below were retired
> with the roost in 2.0; the admin routes are the only lifecycle surface.

Authentication is the existing admin token: `NUTHATCH_ADMIN_TOKEN`, presented as `?token=` or
`Authorization: Bearer` (the header form lands with the audit-tail work). Off-localhost the token is
**required**, as it already is for the admin UI. There is no new auth concept here and there must not be
one: who may mount is the operator's gateway's decision, and `--no-admin` exists for operators who front
their own control plane and want the runtime to have no lifecycle surface at all.

**CLI:** `nuthatch roost mount <ref> --url <roost>` and `nuthatch roost unmount <name> --url <roost>`,
which are thin clients over the two routes. Against a *stopped* roost they fall back to editing
`roost.toml` and installing the bundle, so the same two verbs work whether or not the process is up.

**Desired state lives in `roost.toml`.** A successful mount appends the name (atomic write: temp file,
rename); a successful unmount removes it. A restart therefore converges to the set the operator last
asked for, which is the property that makes the API safe to use. This is the embedded stand-in for
RFC-0022 §3's control-plane DB, and it is deliberately the *same* file the static path reads: one
representation of desired state, two ways to edit it.

The conflict this creates must be named rather than discovered: **at runtime, nuthatch owns
`roost.toml`'s `nests` list.** An operator who manages that file with configuration management should
run `--no-admin` and restart to change the set. Fighting a config-management tool over a file is a
losing game, so the RFC declines to play it and makes the choice explicit instead.

## 6. Question 4 - what does unmount mean?

**Drain, not kill.** In order: stop admitting the nest to new windows, let the in-flight window commit,
flush the alert outbox, seal nothing new, close the redb store cleanly, remove the route, release the
budget. redb is single-writer; a half-closed store is the one way a lifecycle operation could corrupt
data that faults never do.

**Data is retained by default.** Unmount removes a nest from the *running set*; `nests/<name>/` stays on
disk with its `nuthatch.redb` and `segments/`. Re-mounting it later is then a class-(a) admission - the
fast path. Deletion requires `?purge=true`, and purge is refused while a drain is in flight. Destroying
indexed history is a thing an operator should have to ask for in words.

**In-flight requests complete; new ones 404.** The route is removed after the drain. An operator running
a gateway drains at their layer first; this RFC does not attempt connection draining on their behalf.

**Retiring the last nest on a cursor retires the cursor** - there is no reason to hold an RPC pool and a
tip loop for nobody. This requires an amendment to RFC-0026 §6, and it is the subtle bit:

> **RFC-0026 §6 case 2 amended.** "Every cursor is gone → exit non-zero" becomes "every cursor is gone
> **through quarantine** → exit non-zero". A cursor retired by an operator's unmount is an intended
> state, not a failure. A roost with zero mounted nests stays up, serving `/nests`, `/ready`, and its
> admin surface, waiting for a mount.

Without that distinction, unmounting your last nest kills the process you were about to mount the
replacement into, which would be a memorable way to learn the difference.

## 7. Non-goals

- **Scheduling and placement across machines.** RFC-0022 §2. This RFC gives one runtime a mutable nest
  set; it does not decide *which* runtime a nest belongs on.
- **Adding or changing a chain endpoint live.** Mounting a nest on an undeclared chain is refused (§3).
  Spawning a cursor at runtime is machinery this RFC needs for the *declared*-chain case; taking new RPC
  endpoints over an API is a separate trust decision and a later slice.
- **Changing a mounted nest in place.** That is `nest upgrade` (RFC-0020), which already handles
  compatible hot-swap and the breaking path. Mount over a live name is refused and points there.
- **Per-tenant authz or quotas on the lifecycle API.** One admin token, all or nothing. Identity is the
  gateway's job, per `CLAUDE.md` and the division of labour agreed with GraphOps.
- **Live editing of `max_rss_mb`.** The ceiling is read at boot. Raising it to fit a mount is a restart.

## 8. Slices

Each ends runnable, in dependency order:

1. **Dynamic dispatch.** Replace the static `app.nest("/{name}", …)` loop with a registry
   (`Arc<RwLock<HashMap<String, SharedNest>>>`) behind a single dispatcher, and make the roster read from
   the same live source instead of a startup snapshot. No lifecycle yet. Proven by a parity test: a
   statically-mounted roost serves byte-identical responses through the new dispatcher.
2. **The lifecycle channel and unmount.** The command channel drained at window boundaries, plus drain,
   route removal, budget release, cursor retirement, and the RFC-0026 §6 amendment. Unmount before mount
   because it is the simpler direction and it proves the channel.
3. **Mount.** Admission (§3, all three refusals), bundle verification, the two-phase join (§4), and the
   `catching-up` health state.
4. **The control surface.** The two routes, the two CLI verbs, `roost.toml` persistence, and the
   `--no-admin` interaction.

## 9. Acceptance

- **Parity.** A roost with nests A and B mounted statically, and one with A mounted statically and B
  mounted live, serve byte-identical tables for B over the same range.
- **Isolation during lifecycle.** Mounting and unmounting B does not interrupt A: A serves throughout,
  and its tip lag does not rise by more than one window across the operation.
- **Admission is a refusal.** A mount projected over `max_rss_mb` returns `507` and changes nothing -
  including `roost.toml`.
- **Catch-up is honest.** A cold-mounted nest reports `catching-up` and `503` on its `/ready` until it
  joins, and never reports a `last_block` it has not indexed.
- **Drain is clean.** Unmount, then reopen the nest's store directly: no corruption, and the last
  committed window is intact. Re-mounting resumes without gaps or duplicates.
- **Budget holds across a cycle.** Measured RSS after mount-then-unmount returns to within noise of the
  pre-mount measurement (the store is closed, not leaked).
- **Convergence.** Kill the process after a mount and restart it: the persisted set comes back up.
- **An empty roost survives.** Unmount the last nest; the process stays up and a subsequent mount works.

## 10. Open questions (implementation, not scope)

- `join_threshold`: one window is the proposed default. Whether it should scale with the cursor's window
  size or be a flat block count wants a measurement on a busy chain.
- Catch-up concurrency default (proposed: 2) and whether it should back off automatically when the
  cursor's tip lag rises - i.e. whether the background job should yield to the foreground one
  dynamically rather than by static configuration.
- Whether `POST /_admin/nests` should accept a registry `name@version` in slice 3 or only a local bundle,
  deferring RFC-0019 resolution (and its credentials) to a later slice. This is also where the `source`
  field this RFC originally specified in §2 belongs (#550): a source-based mount - fetch-and-materialise
  from a bundle path, URL, or registry ref - is a real feature with real questions attached, not least a
  network fetch on an admin surface that needs bounding against non-negotiable 3. It is deliberately
  deferred here rather than shipped as a field on the slice-2 mount, and should be designed alongside
  this question rather than bolted on ahead of it.
- Whether a catching-up nest should be exposed to webhooks and alerts, or hold delivery until it joins.
  Leaning hold: a nest replaying history would otherwise fire alerts for events that are years old.
