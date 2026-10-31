# RFC-0078: Writer failover - a standby that takes over when the writer dies

- Status: **Accepted (2026-10-30, Chief): "ok letsgoooo with the wave of rfcs".** Built in the order 0078, 0079, 0080, each slice on its own gate; an S0 that fails stops its RFC.
- Author: Pete (cargopete)
- Date: 2026-10-30
- Depends on: RFC-0076 FR17 (remote followers from the mirror), RFC-0073 FR16 (the segment
  mirror and the hot-state feed), RFC-0052 (the mirrored nest and its conditional catalogue put),
  RFC-0022 (the lease and fence), RFC-0040 (sealing cadence).
- Answers: GraphOps on RFC-0076 FR20, 2026-10-30: "this is fundamentally not HA, which is why we
  requested it". Not a launch requirement by their own account; built next because the queue allows.
- Nature: new opt-in capability in the default binary. **Keeps the single-cursor law as written**:
  at any moment one process writes a chain's datasets. Embedded mode is unchanged and needs no
  external service; failover needs a mirror, which only an operator who wants failover configures.

## 1. Abstract

RFC-0076 §3.4 read FR20 as a capacity request and designed several writers per chain behind one
extraction component. That design does not survive the loss of a box: it says itself that the
extraction component's "failure stalls every writer on the chain". What GraphOps asked for is that
a chain keeps indexing when the machine writing it dies.

This RFC gives that without a second writer. A **standby** is a remote follower (FR17) that also
watches a **lease object** in the mirror. The writer renews the lease; when renewals stop for longer
than the lease's term, the standby takes the lease with a conditional put, which hands it a higher
**fence**, writes the hot state it already holds into a store of its own, and resumes the cursor from
its last applied block. Every publication to the mirror carries the fence and is refused below the
current one, so a writer that was only paused cannot publish after it wakes. A planned move of a
nest between writers, which GraphOps also mentioned, is the same path with a release instead of an
expiry.

## 2. Motivation

Today a chain on the shared runtime has one writer process. Followers on other hosts (FR17) keep
answering queries if it dies, but at a fixed `as_of`: nothing advances until an operator starts a new
writer, which then seeds from the mirror's sealed segments and `_seed/` tail and replays the unsealed
gap by RPC. That is a recovery measured in minutes to hours and needs a person.

Scaled mode (RFC-0022) already solved takeover for its own store: a lease and a monotonic fence in the
same Postgres row, expiry on the database's clock, every write guarded. The default binary has the
same machinery in redb (`core/src/redb_store.rs:149`, `guard_fence` at `:1705` on every write
transaction) but the lease lives inside the writer's own file, so no other host can see it, take it
or be refused by it. Failover needs the lease somewhere both hosts can see, and the mirror is the one
shared thing the default binary already writes.

## 3. Goals

1. A chain keeps advancing after its writer's host dies, with no operator action, within a target
   of **30 seconds** from the last renewal to the standby's first committed window (a target, not a
   result; §8 measures it).
2. **No split brain in anything externally visible.** A writer that loses the lease can neither
   publish to the mirror nor feed a follower afterwards. This is enforced by the mirror and by the
   followers, not by the old writer's good behaviour.
3. Answers after failover are row-for-row what an uninterrupted writer would have produced.
4. A planned handover (drain one writer, promote another) is the same mechanism, so a nest can move
   between hosts without an outage.
5. Embedded mode, the default, does not change and acquires no dependency.

## 4. Non-goals

- Several writers active on one chain at once. That is RFC-0076 §3.4, still direction only.
- Surviving the loss of the mirror. The mirror is the arbiter; if it is down, the writer keeps
  writing (§5.2) and no takeover can happen. An operator who needs the arbiter to be highly
  available buys an object store that is.
- Consensus among more than one standby beyond what the conditional put gives (§5.1 is already
  correct for N standbys; electing a preferred one is not attempted).
- Failover for a writer without a mirror, or for scaled mode, which has its own.

## 5. Design

### 5.1 The lease object

One object per dataset prefix, beside the catalogue: `<prefix>/<dataset>/lease.json`.

```json
{ "owner": "writer-a/4f2c", "fence": 7, "term_secs": 10, "renewal": 1834 }
```

`owner` is a random id the process draws at start. `fence` is monotonic and only ever increases on a
change of owner. `renewal` counts renewals by the current owner.

- **Renew**: the owner rewrites the object every `term_secs / 3` with `renewal + 1`, as a conditional
  put on the ETag it last wrote (`put_if`, `src/publish.rs:157` for the filesystem mirror; the
  `PutMode::Update` path for S3). A precondition failure means somebody else wrote it: the owner has
  lost the lease and stops (§5.3).
- **Acquire**: a standby takes the lease only by a conditional put on the exact ETag it observed,
  with `fence + 1` and its own `owner`. Two standbys racing for one lapsed lease both put on the same
  ETag; the store accepts exactly one. This is the existing catalogue CAS applied to a second key,
  and `publish.rs`'s two-publishers test (`:2743`) is the pattern for its test.
- **Release** (planned handover): the owner writes `"owner": null` at the same fence after its last
  sealed boundary. A standby may take a released lease at once without waiting out the term.

**Expiry is never a comparison of two clocks.** The standby reads the lease every second and notes,
on its own monotonic clock, how long the ETag has gone unchanged. It may acquire after
`term_secs + margin` without change, `margin` defaulting to `term_secs / 2`. This is the rule
RFC-0076 §3.3 used for visibility lag, and the reason RFC-0022 moved its lease next to the data: a
lease whose expiry depends on two hosts agreeing on the time fails silently. What it relies on is the
weaker assumption that two monotonic clocks run at nearly the same *rate*, which is the assumption
every lease makes.

S3 has offered `If-Match` on `PutObject` since November 2024; MinIO does too. A store without
conditional writes cannot hold a lease and the writer refuses to start with failover configured on
one, as `publish` already refuses to run its CAS there. [VERIFY: which stores `object_store`'s
`PutMode::Update` fails open on rather than erroring.]

### 5.2 The writer

With `[failover] lease = true` (or `--failover`), a writer at start acquires the lease. If it is held
and live it does not start as writer: it starts as a standby. Its local redb store is claimed with the
lease's fence, so `guard_fence` refuses every local write from the moment the in-process fence and the
mirror's disagree.

Before each window's commit the writer checks that its last successful renewal is younger than
`term_secs` on its own monotonic clock. If not, it **self-fences**: it stops committing, closes every
feed connection, and `/ready` reports `lost_lease`. Its HTTP routes keep answering at their last
`as_of`, labelled as such, because a follower that has stopped advancing does the same.

The writer's check is a courtesy that makes the common case clean; it is not what keeps a stale
writer out. That is §5.3.

If the mirror is unreachable the writer cannot renew. It self-fences after `term_secs`, exactly as if
it had lost the lease, because it cannot know it has not. This trades availability during a mirror
outage for never having two writers, which is the right way round.

### 5.3 The fence at every boundary that leaves the box

A paused writer (`SIGSTOP`, a long GC, a suspended VM) wakes believing it still holds the lease. Three
things it might do, and what refuses each:

| What the stale writer tries | Refused by |
|---|---|
| Publish a segment and move the mirror's catalogue | The catalogue put becomes conditional on the lease too: a publish first reads `lease.json`, refuses if `fence` is not its own, and carries `fence` in `publish.json`. The catalogue CAS then fails if the new writer has published in between. A stale writer that publishes nothing new loses nothing, since its sealed segments are the new writer's too. |
| Feed a follower | `hello` carries the writer's fence. A follower records the highest fence it has accepted per dataset and refuses a feed below it, and re-reads `lease.json` on every reconnect. |
| Answer a query directly | Not refused; it answers at its last `as_of` with that `as_of` on the answer, as §5.2 says. It cannot answer *ahead* of the new writer with different rows, because it commits nothing. |

[VERIFY: the publish path's CAS has a window between reading `lease.json` and the catalogue put. The
new writer has to have published at least once for the catalogue CAS to catch it. A stale writer
publishing in that window adds segments sealed from rows it committed before it lost the lease,
which are true rows; the risk to examine is a seal from rows the new writer later rolls back on a
reorg. S1 decides whether the publish needs the fence inside the catalogue object itself.]

### 5.4 The standby and promotion

A standby is an FR17 remote follower with failover on. It already holds, per dataset, the writer's
`entities`, `meta` and `blocks` tables in memory as of its last applied generation (`feed.rs:836`),
and has fetched and verified every sealed file its catalogue names. `mirrored_meta`
(`core/src/tap.rs:45`) leaves out only the lease, the fence, `owner` and `outbox_next_seq`, so
`last_block`, `sealed_through` and the block checkpoints the reorg check reads all arrive.

Promotion, once the lease is acquired:

1. Stop applying deltas. Any still in flight from the old writer are from a lower fence and are
   dropped.
2. Write the held tables into a fresh redb store under the data directory, install the catalogue,
   claim the store at the new fence. This is the state a `--seal-direct` backfill leaves, which
   `seed.rs` already produces from a mirror, so the store is warm-started the way RFC-0077 S3 rebuilds
   derived state after a restart.
3. Start the cursor at `last_block + 1`. The first window runs `detect_reorg` against the held
   checkpoints, so a reorg that happened while nobody was writing is rolled back as any other.
4. Open the feed to the other followers at the new fence, and start renewing.

The data a promotion has is what the follower had applied. The rows it lacks are those between its
last applied generation and the old writer's last commit, and the cursor re-derives them from the
chain, so goal 3 holds by the same determinism that makes a restart correct.

A standby with no hot state (a cold standby, or a follower that resnapshotted and has not caught up)
promotes from the mirror instead: `seed_dataset` (`src/seed.rs:71`) to the `_seed/` tail's
`complete_through`, then the cursor from there. Correct, slower by the unsealed gap, which RFC-0040's
sealing cadence bounds.

### 5.5 The outbox

The outbox is not on the feed, so a promoted writer has no record of webhooks the old one had
committed and not delivered. Deliveries committed in the last moments before a crash would be lost.
The proposal is to mirror the `outbox` table on the feed as a fourth table, so the promoted writer
redelivers whatever was pending. That makes delivery across a failover at-least-once rather than
at-most-once; receivers drop duplicates by the outbox `seq`.
[VERIFY: that `seq` is on the delivered payload and documented as the idempotency key.]

### 5.6 Planned handover

`POST /_admin/failover/handover` on the writer waits for the next sealed boundary, publishes, releases
the lease (§5.1), and stops committing. The standby acquires at once and promotes with nothing to
replay. This is the "a nest can be moved across writers" GraphOps mentioned, and it is the same code
as §5.4 with a shorter wait.

## 6. What does not change

The single-cursor law: one cursor per chain, one writer, one observable failure boundary, at any
moment. A takeover moves the boundary to another host; it never makes two. RFC-0073 §5 and RFC-0076
§5 stand. No phone-home: the lease lives in the operator's own mirror. The 2 GiB per-cursor budget
applies to the promoted writer as to any writer; the standby already pays it as a follower, and
promotion adds the redb write of the held state, which S1 measures.

## 7. Order of work

| Slice | What | Size |
|---|---|---|
| S0 | The lease object over `FsMirror` and MinIO: acquire, renew, release, the two-standby race, expiry by observed staleness. Self-fencing in the writer. No promotion yet: the standby only reports that it would take over. | one week |
| S1 | Fences at the boundaries of §5.3: `hello`, `publish.json`, the publish-time lease check. The §5.3 [VERIFY] answered. `SIGSTOP` on the writer, resume after takeover, assert it publishes and feeds nothing. | one week |
| S2 | Promotion from a warm follower (§5.4) and from the mirror. The outbox on the feed (§5.5). Parity: kill the writer at random points over a factory nest and a reorging devnet, compare every table row-for-row with an uninterrupted writer. | two weeks |
| S3 | Planned handover (§5.6), metrics (`nuthatch_lease_*`, time-to-promote), the docs, and the measured failover time against goal 1. | one week |

S0 can stop it: if `PutMode::Update` on GraphOps's store does not refuse a stale ETag, there is no
arbiter and the RFC needs a different one.

## 8. Testing

- The two-standby race and the stale-writer cases as deterministic tests over `FsMirror`, as RFC-0022
  did over Postgres.
- A failover gate in CI: writer, two standbys and MinIO in separate network namespaces (RFC-0076 S3's
  harness). Kill the writer with `SIGKILL`, `SIGSTOP` and a network cut; each run must promote exactly
  one standby, lose no committed row, publish nothing at a stale fence, and match an uninterrupted run.
- Time-to-promote recorded per run and reported, not gated, until GraphOps agree a figure.

## 9. Risks

- **Mirror latency sets the floor.** A lease renewal is a round trip to the object store. On a slow
  store the term must be long enough not to self-fence on a bad second, which lengthens failover.
  Ten seconds is a guess until S0 measures renewal latency on GraphOps's store.
- **A flapping network** can make a healthy writer self-fence and a standby promote. That is a
  correct outage of a few seconds and an unnecessary move, not a split brain. Metrics make it visible.
- **The standby costs a follower's memory**, which it does already; failover makes running one
  mandatory for an operator who wants it.
- **The stale-publish window** of §5.3 is the part of this design I am least sure of, which is why S1
  exists before anything is promoted.

## 10. Alternatives

- **RFC-0076 §3.4, several writers.** Solves capacity, not availability; its extraction component is
  the same single point of failure. Kept as direction for the capacity case.
- **The lease in Postgres, as scaled mode.** Correct, and already built, but makes Postgres a
  dependency of failover in a binary whose default needs nothing. An operator who has Postgres can run
  scaled mode today.
- **An external coordinator** (etcd, Consul, Kubernetes leases). Moves the arbiter outside nuthatch
  and outside its tests; a non-goal of RFC-0076 §5 for the same reason Kubernetes manifests are.
- **Raft between writer and standbys.** The right answer for a database; here the mirror is already a
  linearisable register per key, and a second consensus system would be one more thing to be wrong.

## 11. Open questions

1. The default `term_secs`, once S0 has renewal latency from GraphOps's object store.
2. Whether the publish needs the fence inside the catalogue object (§5.3 [VERIFY]), decided in S1.
3. Whether a standby should be allowed to promote for a subset of a runtime's datasets, or whether a
   runtime's leases move together. Per-dataset is simpler to reason about; per-runtime matches how
   operators think about a box. Proposed: per dataset, with handover taking all of a runtime's at once.
4. GraphOps's figure for time-to-promote.

## 12. S0 built (2026-10-30)

On `pete/rfc-0078-writer-failover`, off `ced48584`, on a MacBook. Not yet run on a CI runner.

**What was built.**

- `src/lease.rs`: the lease object of §5.1 at `<data_identity>/lease.json`, beside the catalogue.
  Take, renew and release are each a `Mirror::put_if` on the exact bytes last seen. `Watch` measures
  how long those bytes have gone unchanged on the standby's own clock, and `hold` renews every third
  of the term and fences the store off on a loss or a lapse.
- `put_if` reports a lost race as a typed `CasConflict`, so a holder can tell "another writer wrote
  it" (stop) from a failed request (retry until the term runs out). The refusal messages are as they were.
- `HotStore::fence_off`, implemented for redb. It moves the store's fence past the one every handle
  holds, so the next commit fails with `LostOwnership`, the RFC-0022 error, and no second stop
  path was added.
- `[mounts.publish.failover] term = "10s"`. At boot each failover dataset's lease is taken before its
  cursor is spawned (`take_writer_leases`). A lease still being renewed refuses the boot with the
  holder's id; one left by a dead writer is waited out and taken at the next fence. Once the stores
  exist, each is claimed and held (`hold_writer_leases`), and a lost lease quarantines the nest,
  terminal, with the reason on `/ready`.

**Measured.**

| Test | Against | Result |
|---|---|---|
| 12 unit tests in `lease::tests`: take, renew, the two-standby race, a stale take, a failed request is not a loss, self-fencing by the term, the standby's term-plus-margin rule, a holder that loses its mirror fences off before a standby may take | `FsMirror`, `memory://` | pass |
| `a_failover_mount_takes_the_lease_and_a_second_writer_is_refused_while_it_renews` | a real mount, `FsMirror` | pass |
| `fencing_off_refuses_every_handle_and_keeps_the_fence_monotonic` | redb | pass |
| `e2e_minio_lease`: two boots race, never two holders at one fence; an unrenewed lease taken over after 4.5 s | versitygw | pass, 5.3 s |
| `the_store_enforces_conditional_writes` (existing) | versitygw | pass: the server answers 412 to a stale `If-Match` |

**The kill question is answered for versitygw**, the store CI runs: it refuses a stale ETag
itself. It is **not** answered for GraphOps's store, which is the one that matters. The existing test's
own notes say Garage and Zenko CloudServer answer both violations with a 200, so neither can hold a
lease. `the_store_enforces_conditional_writes` is the check to run against their store before S1.

**Departures and gaps.**

1. *One process cannot test the cross-host race.* `ObjMirror::put_if` serialises calls within a
   process, so the e2e race is decided by the store's `If-None-Match` and never by `If-Match`
   between two in-flight writes. The 412 test above covers the store's half; two processes against
   one bucket is S1's harness.
2. *What the cursor does after `fence_off` is untested end to end.* Its next commit fails with
   `LostOwnership`, and a window error quarantines the nest. Whether readmission then retries in a
   loop, and whether that overwrites the terminal quarantine `hold_writer_leases` sets, has not been
   run. S1 stops the cursor cleanly instead of relying on the readmit path.
3. *The standby does not report.* `await_takeable` is built and tested, but nothing in
   `serve --follow` calls it yet. Done at the start of S1, since S1 needs the standby there anyway.
4. *Feeds are not closed on loss* (§5.2). S1, with the fence on `hello`.

## 13. S1 in progress (2026-10-31)

**Built.**

- *A lost store is a terminal fault.* `fault::is_terminal` now counts `LostOwnership` as well as
  `TerminalFault`. Before this a fenced-off nest was quarantined as retryable and readmitted on a
  backoff, failing the same way each time and overwriting the reason `hold_writer_leases` records
  (S0's gap 2). `a_fenced_off_store_quarantines_its_nest_terminally_and_spares_its_co_tenant`
  holds it: no retry, nothing claimed, the co-tenant rolls back as usual.
- *The fence on the feed.* Each `mount` line carries the writer's fence. It sits on `mount` and not on
  `hello` as §5.3 had it, because a fence is per dataset and a connection carries many. A follower keeps
  the highest fence it has accepted per dataset across reconnects and refuses a writer below it
  (`a_follower_refuses_a_writer_below_the_highest_fence_it_followed`). The stale writer's feed is
  not closed by the writer itself: it commits nothing after fencing off, and a follower that has
  seen the new writer will not take it back.
- *The fence at the mirror.* A failover publisher reads `lease.json` before it moves the catalogue
  and refuses, as a `CasConflict`, when the lease is at another fence
  (`a_writer_below_the_mirrors_lease_does_not_move_the_catalogue`). Segments may still upload, which
  is harmless because they are content-addressed and named by no catalogue.
- *Failover only where a lease was taken.* A failover mirror set up on a live mount or by
  `set_publish` is refused with "restart the runtime", rather than running without a lease.

**Still open in S1.**

1. The §5.3 [VERIFY]. A lease read and the catalogue put are two requests, so a writer whose lease
   lapses between them can still move the catalogue once, if the new writer has not published yet.
   What it moves is rows it committed while it held the lease, sealed past finality, which are the
   new writer's rows too; the case to rule out is a seal of rows the new writer later rolls back,
   and sealing is past finality, so that needs a finality violation. Not yet tested.
2. ~~Two processes against one bucket, with `SIGSTOP` on the holder.~~ Built, below.

**Built since.** `serve --follow --mirror ... --standby` watches each dataset's lease once a second and
reports on that dataset's `/ready` under `standby`: the holder, its fence, whether it could be taken,
and the fence a takeover would hold. It logs once when the lease lapses. It takes nothing; promotion
is S2 (`a_standby_reports_the_holder_and_then_the_lapse`). That closes S0's gap 3.

**The two-process gate.** `scripts/failover-gate.sh <binary>`, and the `failover` CI job beside
`rolling-upgrade`. Two `dev` runtimes over the fixture chain publish one dataset to one mirror with a
3-second term. Writer A takes fence 1 and indexes. Writer B, started while A renews, exits naming A.
A is frozen with `SIGSTOP`; B waits the lease out and takes fence 2. A is resumed; it finds the lease
taken, fences its store off, reports the loss on the nest's `/ready`, and its publisher stops. The
lease is still B's two terms later. PASS on a MacBook against a directory mirror with a debug build,
three runs; not yet run in CI.

Departures from §8. It runs on localhost, not in separate network namespaces: RFC-0076 S3's leg runs
the same way (`scripts/rolling-upgrade.sh`), and a namespace harness exists nowhere in the tree. And
the mirror is a directory by default; `FAILOVER_MIRROR=s3://...` runs it against an object store. The
run found one thing the unit tests had not: a publisher that lost its lease kept retrying and
dead-lettered itself. A lost lease is now `LostLease` at the mirror as well, and the publisher stops
on it.
