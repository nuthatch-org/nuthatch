# RFC-0034: The query allowlist - a bounded public surface without a resync

- Status: **Implemented** - phases 1 and 2 (2026-08-05)
- Author: Pete (cargopete)
- Date: 2026-08-04
- Depends on: RFC-0032 (the mount table - phase 1's allowlist is mount config and has nowhere to live
  without it), RFC-0016 (the semantic layer and `/sql` errors-as-prompts - the surface being bounded),
  RFC-0012 (the nest bundle, for phase 2's manifest ceiling).
- Sequencing constraint: **phase 2 must not land before RFC-0033 (grafting).** See §5 - this is the
  whole reason the RFC is in two phases.
- Nature: mini-RFC in mechanism, full RFC in sequencing. The security control is small; *where it
  lives* is the decision.
- Origin: chris (GraphOps), 2026-08-04 - a nest exposed publicly should carry an allowed list of
  queries. Decision O-10 in the session's working notes (unpublished) §10.

## Abstract

`/sql` accepts arbitrary SQL. That is the product for a local developer and a liability for a public
endpoint: it is an open analytical query engine over an operator's disk, and the guards that exist
(concurrency 2, a 30 s timeout, 50,000 result rows, 2,000,000 hot rows scanned, 16 KB of query text)
are **node self-protection, not a security boundary**. They bound the damage of one query. They say
nothing about which queries a nest is willing to answer at all.

The fix is an allowlist: a nest served in production answers a declared set of queries and refuses the
rest. The mechanism is uncontroversial. The decision worth an RFC is **where the allowlist lives**, and
the answer is: **in mount config now, in the manifest later** - because putting it in the manifest today
would flip the nest's content address on every security tweak, and with no grafting yet that is a real
re-index. We would be shipping the exact problem grafting exists to remove.

## 1. What exists today

`serve.rs` guards `/sql` with, in order: a 16 KB query-length cap (`SQL_MAX_QUERY_LEN`), a concurrency
semaphore of 2 (`SQL_MAX_CONCURRENCY`), a 30-second timeout (`SQL_TIMEOUT`), a 50,000-row result cap
(`SQL_MAX_ROWS`) and a 2,000,000-row bound on the hot-store scan (`SQL_MAX_HOT_ROWS`).

Every one of those is a per-query blast-radius limit. None of them limits the *set* of queries, so a
public endpoint is:

- an unbounded analytical surface over whatever the nest indexed,
- with a cost profile an attacker can explore for free by trying queries,
- and no way for an operator to say "this nest serves these five things".

Note also that `/explain` used to not carry the hot-row bound `/sql` does; #367 closed that gap
unconditionally, so both endpoints are bounded whether or not a mount runs an allowlist. What the
allowlist still closes is the *set* of queries `/explain` will plan at all, per the two paragraphs
above.

## 2. Phase 1 - the allowlist is mount config

**Ships now, with RFC-0032.**

A mount record (RFC-0032 §4) gains an optional query surface. Because it is *mount* config:

- it is not an authored input, so **the NID is untouched**,
- so **nothing re-indexes**,
- so **two tenants with different query surfaces keep the same NID and share one dataset** - which
  matters, because a security control that forked datasets would quietly undo RFC-0032's entire point.

Shape, kept deliberately small:

| Mode | Behaviour |
|---|---|
| absent (default) | Today's behaviour. Arbitrary `/sql`, guards only. The local-dev experience is unchanged. |
| `deny` | `/sql` and `/explain` are refused entirely. The typed routes (`/tables`, `/entity/{id}`, `/balances`, …) still serve. |
| an explicit list | Only the named queries answer. |

A listed query is a **named, parameterised statement** - a name, SQL, and typed parameters - not a
regex over query text and not a prefix match. Matching user-supplied SQL against patterns is the shape
of every SQL-filter bypass ever written; the client sends a *name and arguments*, never text.

Refusal is a 4xx naming the allowed set, in the errors-as-prompts style RFC-0016 established, so an
agent hitting a bounded nest is told what it *can* ask rather than left guessing.

## 3. Phase 2 - the manifest declares the author's ceiling

**Ships with or after RFC-0033.**

The manifest gains a declared maximum surface: what the nest's *author* sanctions being asked of it. It
is an authored input, so it is hashed with the bundle and it is part of the NID.

A mount may then **narrow within the ceiling, never widen it**. An author ships a nest saying "these
twelve queries are what this is for"; an operator exposes four of them.

*(#2135.)* `open` is wider than any ceiling, so a mount on the default `open` serves the ceiling
itself as its allowlist. The check runs wherever a mount takes its surface: at boot, and on a live
mount, resume and move.

Why this is worth having at all, given phase 1 works: it moves the surface from an operator decision to
an author guarantee. A published nest becomes self-describing about what it answers, which is the
property a registry needs and an operator cannot supply for someone else's nest.

## 4. The rejected option, recorded

**Carving the allowlist out of the NID** - keeping it in the manifest but excluding it from the hash -
was considered and rejected. It would have given phase 2's authoring model with phase 1's cost, today.

It is rejected because one exception to "any authored edit changes the identity" is how content
addressing rots. The next exception is then an argument rather than a rule, and the rule is only
load-bearing while it is absolute. RFC-0032's sharing and RFC-0033's grafting both rest on it.

The two-phase sequencing gets the same outcome without touching the invariant.

## 5. The sequencing is the decision

Once phase 2 lands, editing an allowlist flips the NID. Under RFC-0033 that is fine: every derivation
recomputes over its probe range, every output is identical, everything backdates, nothing re-indexes
(RFC-0033 §5). Without grafting, the same edit is a full re-index of a production nest - triggered by a
routine security tweak.

So the consequence to state plainly: **after phase 2, grafting is no longer an optimisation but a
prerequisite for ordinary operations.** Do not ship phase 2 into a runtime that cannot graft.

## 6. What this is not

- **Not authentication.** The allowlist bounds *what may be asked*, not *who may ask*. Identity stays
  the gateway's job (CLAUDE.md).
- **Not a rate limiter or a quota.** Per-tenant quotas are out of scope and remain so.
- **Not a replacement for the guards.** The guards stay exactly as they are. A named query can still be
  expensive, and the timeout and row caps are what stop it.
- **Not on by default.** A local `nuthatch dev` is an exploration tool and arbitrary SQL is the point.
  The allowlist is something an operator turns on when a nest faces the public.

## 7. Slices

| # | Slice | Ends with |
|---|---|---|
| 1 | Named parameterised queries + `deny` mode as mount config; refusal responses in the RFC-0016 style. | A mounted nest with a three-query surface answers those three and refuses a fourth with the allowed set named. `/explain` is bounded by the same list. |
| 2 | Docs and the production recipe: the operator guide states plainly that a public nest without an allowlist is an open query engine. | `docs/operators.md` and the website's production page say it in those words. |
| 3 | *(gated on RFC-0033)* Manifest ceiling; mount narrows within it; refusal on widening. | A mount attempting to expose a query the author did not sanction is refused at mount time. |

## 8. Status

Draft. Phase 1 is writable and buildable as soon as RFC-0032 slice 2 lands. Phase 3/§3 waits on
RFC-0033, by design and not by accident.
