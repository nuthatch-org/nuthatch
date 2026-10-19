# The backlog

**GitHub Issues are the source of truth for what is left to do.** This file is orientation: what the
tracks mean, which decisions are settled, and how to query the queue.

It deliberately does **not** list individual items any more. It used to, and by 2026-08-06 the list
had drifted badly enough to be actively misleading: it still called RFC-0023 tier 1 "building" when
tiers 1-2 had shipped, still said "roost" a fortnight after the concept was retired, and had no row at
all for RFCs 0032-0035 - the entire 2.0 arc. Every one of its items already existed as an issue, each
one better written than the entry it came from. A second list is a list that drifts.

| Question | Where it is answered |
|---|---|
| What is each RFC, and is it built? | [RFC index](rfcs/README.md) |
| **What is left to do?** | **[Open issues](https://github.com/nuthatch-org/nuthatch/issues)** |
| What is the current sprint or a deferred capability? | [Documentation guide](README.md) |
| What must be true before this runs unattended? | [prod-readiness.md](prod-readiness.md) |
| How do I prove a claim on my own hardware? | [verification.md](verification.md) |

## Reading the queue

Labels carry the meaning that this file's "tracks" used to. Every open issue carries **exactly one
priority or `parked`**. A bare issue with neither is a backlog bug rather than a style choice.
The 2026 capability-freeze items were closed into [frozen-for-2027.md](frozen-for-2027.md), so
`frozen` is not an open-queue state.

**How urgent is it?**

| Label | What it means |
|---|---|
| `p0` | Do now: a gate that cannot fail, a live defect, or a **published claim that is false** |
| `p1` | Next: a real correctness or coverage gap, with no immediate exposure |
| `p2` | Later: tidy-up, ergonomics, or a gap with a known workaround |
| `parked` | **Deferred by decision, not by oversight. Do not treat as a blocker please.** Carries no priority, on purpose - re-raising it as one is how a settled decision gets relitigated |

**What kind of thing is it?** More than one may apply.

| Label | What it means |
|---|---|
| `bug` | Something is wrong in shipped behaviour |
| `security` | Hardening or audit follow-up |
| `verification` | An unproven claim that needs a real run - **evidence, not code**. Closing one means a measurement exists, not that something was written |
| `performance` | Throughput, latency or footprint |
| `tech-debt` | Deferred cleanup with a recorded reason |
| `rfc` | Tracks a named RFC slice; the RFC holds the design, the issue holds the state |
| `documentation`, `enhancement`, `question` | As they read |

**Who can pick it up, and can it start?**

| Label | What it means |
|---|---|
| `board-only` | Needs credentials or machine access the firm must never hold. **Agents must not attempt these**, including "just the safe part" |
| `blocked` | Cannot proceed until another issue lands. The blocker is named in a comment - if it is not, the label is wrong |
| `help wanted` | Genuinely open to an outside contributor. Two of gallant-gecko's merged PRs came in this way |
| `good first issue` | Small, self-contained, and does not need the whole architecture in your head first |

**Sprint labels** (`brisk-bunting`, `temperate-tern`, and so on) mark membership of one sprint and
remain afterwards as provenance. The open sprint is `dependable-dotterel` (2026-10-19): the chain we
stored is the chain. It is the first of six that triage the 2026-10-19 cold audit (#2110); the rest
are `earnest-eider`, `faithful-fieldfare`, `grounded-goldcrest`, `hardy-hawfinch` and
`inquisitive-ibisbill`, in that order. A sprint label on a closed issue is historical; do not use it
to infer that work remains.

The queries that matter:

```sh
gh issue list --limit 100 --json number,title,labels          # everything open
gh issue list --label p0                                      # drop what you are doing
gh issue list --label p1                                      # the real queue
gh issue list --label parked                                  # deliberately not now
gh issue list --label verification                            # claims owing a measurement
```

Sorting by priority beats filtering by `parked`: an issue is actionable when it has a `p*` label.
`grep -v parked` used to be the recommendation here only because priorities did not exist yet.

The issues carrying `parked` on 2026-09-28 were closed that day. The decisions are in
[parked-closed.md](parked-closed.md). An open `parked` issue after that date is a new deferral, and
the label still means what the table above says.

## Standing decisions - do not re-raise these as blockers

These are settled. They are recorded here rather than in an issue because the answer is "no, and here
is why", which is not a task.

**A colocated reth node is deferred** (2026-07-29). It is the substrate RFC-0003 reads from and
RFC-0014 extracts from, and it stays the single unlock for that branch. The cost is provisioning plus
**days** of sync (full) or **TB and longer** (archive) - a hardware and ops job, not a coding session.
Deferring it is a decision. What it gates stays `parked`: ExEx wiring, trace/state extraction, an
honest tip-lag number, and RFC-0023 tier 3's pinned-block verification.

**The first DataFusion gate did not meet its bar** (2026-08-02). It measured **1.6-2.7x DuckDB's
latency** on `net_balances` over sealed segments, widening with segment size, at exact result parity.
That result remains evidence, not an architectural commandment: [RFC-0042](rfcs/0042-rust-native-without-duckdb.md)
closed KEEP DuckDB at its §14 and is parked, and the engine has since been Burrmill, on DataFusion,
from 4.1.0 ([release notes](releases/v4.1.0.md)); DuckDB is no longer in the binary.

**Turso is double-gated**, not rejected: a production-ready release, and a measured win over redb
that federation does not already provide. Until both, no.

The third gate - *a permissive, non-BSL licence* - is **dead, and was wrong when written**
(2026-07-17; corrected 2026-08-10). `tursodatabase/turso` and `tursodatabase/libsql` are both MIT,
checked against the GitHub API on 2026-08-10, so the licence never barred anything. Of the two that
remain, production-readiness is arguable rather than settled (production use is claimed at several
organisations, but it is pre-1.0 and some features are marked experimental), and the measured win has
**never been attempted**. #366 carried the measurement and was closed on 2026-08-10 with the decision
that the one-mental-model case for replacing redb does not survive inspection.

**Scaled mode is no longer infra-blocked.** RFC-0022 turned it into ordinary work - the `HotStore`
trait, a Postgres backend with a redb-parity suite, the query-FE role, and ownership fencing. The old
framing that "nothing in this track is verifiable on the dev laptop" was true when written and is now
only half true: scaled mode moved out of it, the node did not.

## The failure mode this file exists to prevent

Twice, entries here **outlived their fix** - proxy/EIP-1967 introspection and SSE push were both
listed as open long after they shipped, and the proxy entry was additionally misdiagnosed, describing
a gap that was already closed while the real one (a *bespoke* proxy that silently indexes zero rows)
went unrecorded until it cost a day on the Livepeer nest.

That is the argument for one list. An issue gets closed when the work lands, because closing it is how
you finish; a bullet in a document gets closed when someone remembers.

## History

Dated records, not live plans. Read them for *why*, never for *what is left*:

- [progress-log.md](https://github.com/nuthatch-org/nuthatch/blob/ede03ca7/docs/progress-log.md) - what happened, when
- `sprint-*.md` - completed sprint scopes as they stood, kept at
  [`ede03ca7`](https://github.com/nuthatch-org/nuthatch/tree/ede03ca7/docs)
- [high-level-roadmap-jul-aug-2026.md](https://github.com/nuthatch-org/nuthatch/blob/ede03ca7/docs/high-level-roadmap-jul-aug-2026.md) - the architecture session that produced RFCs 0032-0035
