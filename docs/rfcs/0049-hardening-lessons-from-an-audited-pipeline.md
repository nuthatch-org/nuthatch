# RFC-0049: Hardening lessons from an audited ingestion pipeline - eight failure shapes, checked against ours

- Status: **Reference and analysis. No implementation, no decision.** Under the 2026 feature freeze
  this is a document to argue with, not work to start. It is **not** a carve-out and it does not
  start, reorder or unblock any slice. Its only operational output is §10's candidate issues, each of
  which must stand on its own under the freeze's bug-fix, security, performance and maintenance
  allowance. **§2 is built by RFC-0073 FR14 (#2416), 2026-10-27**: `nuthatch_nest_logs_undecoded_total`,
  the `undecoded` block in `/ready` and `/nest`, and one `warn` line per window.
- Author: Jenny
- Date: 2026-09-02
- Depends on: RFC-0026 (fault quarantine and partial health), RFC-0025 (adaptive tool
  advertisement), RFC-0022 (the Postgres backend and its parity obligation), RFC-0028 (adaptive log
  range control), RFC-0034 (the query allowlist), RFC-0008 (compliance: screening, flags, alerts),
  RFC-0001 (decode and the registry).
- Origin: field experience with production ingestion systems, read against our own code rather than
  adopted. No system is named and nothing below depends on which one it was: every claim about
  nuthatch is checked against this tree at `1d1c8db` and stands or falls on that alone.
- Blocks: nothing.

## §0 - Why write this down at all

Reading someone else's audit findings is cheap, and the temptation is to file them as reassurance:
*we don't do that.* Mostly we don't. But an audit of a mature ingestion system is not a list of that
system's mistakes so much as a list of **the shapes a mistake takes in this kind of software**, and
those shapes travel. A pipeline that fetches from an RPC provider, checks what came back, decodes it
and stores it has a fixed set of places to go quietly wrong, and the interesting question is not
whether we made the same mistake but whether we have anything that would *tell us* if we had.

So each section below states a failure **shape** in the abstract, then checks this tree against it
and returns a verdict: **solved**, **partly**, or **open**. Two are solved outright and three are
handled with a gap worth naming, which is worth recording once rather than rediscovering. The
remaining three are open, and they are open in the same direction; §9 says what that direction is.

> **The failures worth fearing are the ones that render as health.** A crash is a gift. Every shape
> below is a way for a system to keep running, keep reporting green, and hold less data than it
> should.

| § | Shape | Verdict here |
|---|---|---|
| §1 | The reference list is never itself checked | **Closed for the short-at-the-end case by #1144** (2026-09-04): every window refetches the previous window's last two blocks and the seal cut never enters that tail. The short-by-a-few-in-the-middle case stays where `verification.md` puts it |
| §2 | The decoder's drops are invisible | **Open** |
| §3 | Configured-but-empty means off, and the surface still says on | **Partly** - one case, and it is the compliance one |
| §4 | The check that cannot fire | **Partly** - the mechanism exists, its coverage does not reach the risky code |
| §5 | Two implementations, one invariant | **Partly** - a real parity suite, an unguarded edge |
| §6 | Retrying a deterministic failure | **Solved** |
| §7 | The alert that catches only the failures that stop the machine | **Open**, and it is §1 and §2's home |
| §8 | The config that reaches production is not the config in the repo | **Solved**, structurally |

## §1 - The reference list is never itself checked

**The shape.** Cross-validation compares the parts of a fetched unit against each other: receipts
against transactions, traces against transactions, counts against sets. Every such check needs one
list to measure the others by, and that list is assumed. In the audited system this was benign on
every chain but one, where a fetch strategy *rebuilt* the reference list from individual lookups and
dropped the ones that returned null. When an upstream blip hit a transaction and its receipt
together, both lists came back one entry short, cross-validation compared them with each other, and
passed. A perfectly consistent unit that was simply missing a transaction. Out of a long list of
findings it was the only silent one, and it was silent for exactly this reason: **the thing every
check was measured against was the thing nothing checked.**

**Us.** Our reference is the window's `eth_getLogs` response. Nothing independent says the provider
returned every log in that range. We defend the two neighbouring failures well - `src/chunker.rs`
detects a provider's result-count cap from its refusal text and splits, with a test set built from
real messages across four providers, and `src/rpc.rs` classifies a per-item error inside an HTTP 200
batch rather than reading it as a missing block. Both of those catch a provider that **says** no. A
provider that returns `[]`, or returns nine logs where ten exist, says nothing at all, and the window
seals clean.

**What would catch it, and what it costs.** The block header carries `logsBloom`, which is computed
by consensus over exactly the addresses and topics of that block's logs. It is a one-sided oracle:
false positives are possible, false negatives are not. So for a block where the bloom tests negative
for our address, harvesting zero logs is *proven* correct; where it tests positive and we harvested
zero, we have a suspicion. One suspicion is noise - that is what a false positive looks like. A run
of them over a sampled range is not: the false-positive rate of a 2048-bit bloom at Ethereum's load
is small, and independent per block, so *k* consecutive positives against zero harvested logs is
evidence roughly to the *k*th power. That makes this a **sampled audit, not a per-block gate**, which
is also what keeps it affordable.

**What it does not catch, stated before anyone builds it on the strength of the paragraph above.**
The bloom is a membership filter, not a count. It answers *was there a log matching this address and
topic in this block*, and nothing else. So it catches the **empty** case - we harvested nothing where
something existed - and it is blind to the partial one. Ten logs in the block and nine in our hands
leaves the bloom positive and our response non-empty, and this check passes. That is half of the
failure named at the top of this section, and it is the half a `getLogs` response is most likely to
produce by an off-by-one at a chunk boundary.

Two things follow. The empty case is still worth catching on its own terms: an entire window
silently empty is the catastrophic version, it is what a misfiled filter or a provider serving a
stale index produces, and today nothing at all would notice it. And **detecting a partial omission
needs a counting oracle, which the header does not give us** - `receiptsRoot` would, but verifying it
means fetching every receipt in the block, which is the expensive path this whole approach exists to
avoid. So the honest scope is "did we get nothing where there was something", and any proposal that
claims more than that is claiming more than the bloom can deliver.

The cost is the header, and for a nest with `extract.blocks` on (RFC-0036) we are already fetching
it. Shape: a log-completeness mode on `nuthatch audit` over a sealed range, in the same family as
that command's existing screening replay, which re-derives stored annotations rather than trusting
them. No flag is named here - the doc-command gate is right that a document should not describe an
invocation the binary does not accept. This is the same idea pointed at the layer below.

**Verdict: open, and only partly answerable.** The empty case has an affordable check and does not
need a second endpoint or a consensus client. The short-by-a-few case does not, and stays where
`verification.md`'s "what we do not claim" section already puts it.

**2026-09-04, #1144: the affordable check turned out not to be the bloom, and the mechanism that
landed is cheaper than a check.** Measured on mainnet, a sparse address tests positive in the
`logsBloom` of a quarter of the blocks it has nothing to do with (1,381 of 2,048 bits set), so a
bloom-gated refetch would retry forever on exactly the dense chains this section is about. What
landed instead: every fetched window starts `FETCH_TAIL_OVERLAP` (two) blocks before the cursor, the
seal buffer is keyed by `(block, log_index)` and deduplicates the merged tail, the cut is held back
until the tail is out of the refetch range, and the tip path drops rows the store already holds
before they reach a view. A window answered short at its end is completed by the next window; the
sealed rows and their content addresses are identical to a run against a provider that never
answered short, and both facts are tested by mutation. At tip this widens the one call the poll
already makes and adds none. A hole strictly inside a window, and a range short by a few rows within
a block, remain outside it, as the paragraph above says.

## §2 - The decoder's drops are invisible

**The shape.** A pipeline that filters as it maps discards two different things through one hole: the
input it correctly has no interest in, and the input it *should* have decoded and could not. Written
as one `filter_map`, the two are indistinguishable downstream and the second leaves no trace.

**Us.** Three sites, identical:

```rust
// src/indexer.rs:2964, :3515, :5849 - three sites, identical
.filter_map(|log| match registry.decode(log) {
    Ok(Some(r)) => Some(r),
    Ok(None) => None,                                   // no decoder for this topic0
    Err(e) => { tracing::debug!("decode skipped: {e:#}"); None }   // a decoder that failed
})
```

`Ok(None)` is ordinary and correct - a nest with `events = ["Transfer"]` on a chatty contract ignores
most of what it fetches by design. `Err` is not ordinary. It means a log whose topic0 we recognise
did not decode against the ABI we hold, which is what a proxy upgrade, a re-deployment behind the
same address, or a vendored ABI from the wrong verification looks like from in here. It is logged at
**`debug`**, which no operator runs, and counted nowhere. A nest in that state indexes fewer rows
every window, serves them without complaint, and is green on every alert in `operators.md`.

**What makes this sharper than it looks:** we already compute this exact signal, once, and throw it
away. `project.rs::check_abi_fits` samples logs at two windows during `init`, compares observed
topic0s against the ABI's, and reports `Mismatch`/`Partial`/`Fit` before a nest is ever written. That
is precisely the right check. It runs at the one moment the ABI is *least* likely to be wrong, and
never again - and a contract's ABI does not drift at `init`, it drifts three months later behind a
proxy. The run-time inputs are already in hand: `logs.len()` is computed at `src/indexer.rs:3508` for
the chunker's benefit, so the undecoded fraction is a subtraction we are not doing.

Two counters, per nest, and the `Err` arm at `warn` with a per-window rate rather than per log.
`nuthatch_nest_rows_decoded_total` exists; its denominator does not.

**Verdict: open.** The cheapest finding in this document and the one with the worst failure mode.

**Built 2026-10-27 by RFC-0073 FR14 (#2416)**, as one per-nest counter by contract and topic0 rather than
two: a log `Ok(None)` drops is ordinary and stays uncounted.

## §3 - Configured-but-empty means off, and the surface still says on

**The shape.** The audited system shipped several chains whose per-chain validation schema was an
empty file. Empty happens to throw at startup, so the current state fails closed - but a schema
containing `{}` is a *valid* schema with no constraints, which accepts every document silently, and
`{}` is exactly what someone types to make an empty file stop throwing. One character between a crash
and a guard that is on paper and absent in fact.

**Us, the half we got right.** `allowlist.rs:266`:

```rust
SqlAccess::Allowlist if self.queries.is_empty() => bail!(...)
```

An allowlist with nothing on it is refused rather than interpreted. That is the correct treatment of
this shape and it is already in the tree.

**Us, the half we did not.** `screen.rs:325`:

```rust
pub fn from_config(dir: &Path, lists: &[String]) -> Result<Option<Self>> {
    if lists.is_empty() { return Ok(None); }   // screening off, no log, no warning
```

A *named but missing* list snapshot errors loudly, with the hash in the message - good. A `[screening]`
table with no lists disables screening and says nothing, and the "screening enabled: N list(s), M
addresses" line only prints on the branch where it is on, so the signal is an absence. `audit.rs:25`
skips the replay on the same predicate, so `nuthatch audit` also passes.

Meanwhile `serve.rs::shape` computes the capability from something else entirely:

```rust
let compliance = s.threshold.is_some() || s.velocity_threshold.is_some()
    || s.dir.join(labels::LABELS_DIR).is_dir()
    || s.dir.join(lists::LISTS_DIR).is_dir();     // a directory on disk, not a configured list
```

So `nuthatch lists fetch` followed by forgetting to reference the hash in `nuthatch.toml` produces a
nest that screens nothing, advertises `screen_status` over MCP, and answers "no hits" for every
address asked. RFC-0025's whole argument for shape-gating is that a tool answering `{"count":0}`
reads to an agent as *the index is empty* rather than *this tool is not live here*. That argument is
strongest for the compliance tools and it is the compliance tools where the predicate diverges from
the runtime's.

One smaller edge in the same family: `mcp.rs::fetch_shape` defaults to advertising **everything**
when the `/shape` probe fails, deliberately, so a stale nest degrades to today's surface. That is the
right default for `transfers`, where a false negative is a nuisance. For `compliance` a false
positive is an unscreened nest answering a screening question.

**Verdict: partly.** The fix is not new capability: make `/shape`'s compliance predicate the one the
runtime actually gates on, and warn (or refuse, as the allowlist does) when `[screening]` is present
and empty.

## §4 - The check that cannot fire

**The shape.** A validator filtered candidate records on a field that exists in one node family's
output and not the other's, then ran on chains from the family that lacks it. The predicate evaluated
false for every record, the function returned "no inconsistency" unconditionally, and the per-chain
config went on reporting the check as enabled. Nobody noticed, because a check that never fires and a
check that always passes produce identical output for as long as nothing is wrong. The per-chain profiles made it worse:
the flag was left **on** across the whole family whose output lacks the field, and deliberately
turned **off** on several of the chains where it would have worked, for a documented and legitimate
reason. So the config was at its most confident exactly where the check was doing nothing.

**Us.** The general answer is already this project's, and better argued than most: `mutation-coverage.md`
opens with *a test that passes when the thing it tests is deleted is not a test*, and the nightly
`mutants.yml` enforces it against a baseline of known survivors. That is the correct instrument for
this shape - a guard that cannot fire is a mutant that survives.

The gap is coverage. Scope today is `src/chunker.rs`, `src/seal.rs`, `src/registry.rs`, about 300
mutants; `registry.rs` yields only three because #581 moved decode into its own crate. The doc names
the consequence itself: *"`cargo mutants -d decode` is 185 mutants and is the obvious next addition
once the nightly's real wall-clock is known from a few runs rather than from one laptop."* Decode is
where our provider-shape-dependent predicates live, and §2 says a decode failure currently has no
observable consequence at all - which is the exact condition under which a decorative test in that
crate would never be noticed.

**Verdict: partly.** The mechanism is right and shipped; extending it to `decode/` is the follow-up
the doc already anticipated, now with a second reason.

## §5 - Two implementations, one invariant

**The shape.** A configuration flag was read at exactly one construction site. Every per-chain
factory took a different route and silently received the default. Nothing was broken on the day,
because no chain that needed the flag had its own factory - the finding was not a bug but a
**latent trap**, and the reason to write it down was that the next reasonable refactor springs it
with no error anywhere.

**Us.** `HotStore` is a trait with two implementations, redb and Postgres, and `tests/pg_parity.rs`
is a genuinely good answer to it. Its module docs make the argument correctly - it drives both
backends through one sequence and compares *store against store*, not store against a literal,
because "my mental model of redb's ordering is exactly the thing most likely to be wrong". It refuses
to skip in CI (`NUTHATCH_REQUIRE_PG=1`) on the grounds that a parity suite which silently no-ops is
worse than none. All correct.

What is unguarded is its coverage. The trait declares 35 methods; six are named nowhere in the suite:

| Method | Why it matters |
|---|---|
| `rollback_to_and_set_meta` | the reorg path's atomic write. `rollback_to` **is** compared; the variant that also moves the meta key is not |
| `get_block_hash` | the reorg path's read - the hash comparison that decides a reorg happened |
| `commit_window_blocking` | the async commit the tip loop takes |
| `outbox_remove_batch_blocking` | at-least-once delivery bookkeeping |
| `get_entity`, `recent_by_table` | point read and per-table listing |

Twenty-nine of thirty-five is a good number. The problem is that nothing states it, so nobody learns
when it becomes twenty-nine of forty. This project has already built the right instrument for that
twice: `mutants-baseline.toml`, where a survivor not on the list fails the job and a stale entry gets
reported, and `skill_refs::authored_files_only_mention_real_metrics`, which derives its canonical set
from `Metrics::render()` so a documented name and an emitted name cannot drift. The same shape
applies here - a list of the methods the parity suite exercises, cross-checked against the trait, with
deliberate exemptions carrying a reason someone can disagree with.

**Verdict: partly.** Not a bug. A trap of the same species as the one described, and cheap to disarm.

## §6 - Retrying a deterministic failure

**The shape.** The audited system retried a failed unit seven times with backoff before parking it.
For a transient upstream failure that is right. For a schema mismatch - deterministic, identical on
every attempt - the retry budget buys nothing but delay, and the useful signal turns out not to be
the error counter but the *arrival* in the terminal queue, because nothing lands there by accident.

**Us: solved, and by design.** RFC-0026 splits the fault classes at the point of quarantine, and
`health.rs::QuarantineInfo` carries `class: "retryable" | "terminal"` with `next_retry_unixtime:
None` for the terminal case. A terminal fault does not spend a budget. `nuthatch_nest_quarantine_total`
is monotonic precisely so a nest that flaps in and out is visible on a graph while its current state
keeps reading healthy, which is the same insight as "the arrival is the signal, not the depth".

The one asymmetry is §2's: a decode failure never reaches this machinery at all, because it is not a
fault, it is a `None`. Whether it should be classifiable is a real design question and not one this
document answers - most `Ok(None)` genuinely is not a fault. The counter comes first; what to do when
it is nonzero comes after evidence.

## §7 - The alert that catches only the failures that stop the machine

**The shape.** The audited system exported a validation-error counter that nothing alerted on, and
carried per-chain alert rules that reached a chat channel and no pager. Both halves are the same
mistake: **a signal is not an alert, and an alert is not a route.**

**Us.** Better than that, and worth saying plainly. `operators.md` §"What to alert on" ships eight
alerts with conditions, `/metrics` carries the RFC-0026 health series appended to the render so an
operator can alert on "anything quarantined" without polling `/nests`, `/ready` distinguishes
liveness from readiness with an explicit note that readiness is advice to a supervisor rather than a
traffic gate, and the entity series were added specifically because a two-day live run had to poll
`/ready` and parse JSON to answer four questions no series could.

Now read the eight conditions in a row: nest quarantined, cursor dead, tip lag growing, ingest
stalled, outbox backing up, memory near budget, query rejections spiking, quarantine flapping.

**Every one of them fires when the machine stops. None of them fires when the machine keeps running
and indexes less than it should.** That is not a criticism of the table, which is complete for what
it covers; it is an observation that the entire alerting surface is liveness and resources, and that
this is exactly the blind spot §1 and §2 sit in. A nest whose ABI stopped matching its contract keeps
its tip lag at zero, polls on schedule, never quarantines, and holds a shrinking fraction of its
data.

**Verdict: open, and it is where §1 and §2 land.** The ninth row writes itself once §2's counter
exists: undecodable logs as a fraction of fetched, per nest, sustained.

## §8 - The config that reaches production is not the config in the repo

**The shape.** A validation schema in the audited system's repo reached the running process through
a generator and a deployment, so the process read a *rendered copy* rather than the file anyone
edits. Under incident pressure people edited the deployed copy directly - a documented, sanctioned
route - and back-ported to the repo afterwards, or forgot to.
The next deploy overwrites that edit either way, silently, so a fix made under pressure has a shelf
life nobody is told about. "What is this process enforcing" and "what does the repo say it
enforces" were two questions with two answers, and no endpoint that could settle it.

**Us: solved, structurally, and in two independent ways.** A nest's identity *is* the hash of its
content, so an edited nest is a different nest with its own dataset, and divergence forks rather than
contaminates. And the part that is deliberately **outside** the content address - mount config, where
RFC-0034 §2 puts the SQL access mode and the allowlist so that changing them does not re-index - is
reported by the node itself at `GET /queries`: the effective mode, whether free-form is allowed, and
every declared query with its parameters and path.

That endpoint is the whole of the answer, and it is worth naming why: the security-relevant config is
the config that is *not* content-addressed, for a good reason, and the compensating control is that
the node will tell you what it is actually enforcing rather than requiring you to trust a file. The
audited system had no such endpoint, which is the entire difference.

## §9 - The one thing all of this says

Five shapes are handled here, wholly or with a named gap, and three are not. The three are not a
coincidence. Everything this
project defends well is a failure that **stops** something: a cursor dies, a lease is lost, a query
is refused, a pod will not start, a segment will not seal. The instruments are excellent and the
arguments behind them are written down. What has no instrument is the failure that **continues**: a
provider that returns nine logs where ten exist, a decoder that stopped matching its contract, a
screening stage that is off while its tool is advertised. Each of those keeps the process healthy,
the cursor advancing, the lag at zero, and every one of the eight alerts green.

`CLAUDE.md` forbids serving stale data *as if healthy*, and RFC-0026 implements that faithfully for
the case where a unit is out of service. The shapes above are the other case: data that is not stale,
merely **incomplete**, served as if whole. The three candidate issues in §10 all point at the same
gap and are best read together.

## §10 - Candidate issues

Each stands on its own under the freeze; none is new capability. Ordered by cost-to-benefit, not by
section.

1. **Count what the decoder drops** (§2). Two per-nest counters - undecoded logs and decode errors -
   and lift the `Err` arm from `debug` to a per-window `warn`. `logs.len()` is already computed at
   `src/indexer.rs:3508`. Bug-fix and observability; the smallest change in this document and the
   largest failure it closes. Adds the ninth row to `operators.md`'s alert table (§7).
2. **Make the compliance capability read the runtime's own predicate** (§3). `serve.rs::shape`
   currently infers compliance from a directory on disk while `screen.rs` gates on the configured
   list, so a nest can advertise `screen_status` and screen nothing. Warn, or refuse as
   `allowlist.rs:266` already does, when `[screening]` is present and empty. Security-adjacent
   correctness.
3. **A coverage gate on the store parity suite** (§5). Twenty-nine of thirty-five `HotStore` methods
   are exercised; the six that are not include the reorg path's `get_block_hash` and
   `rollback_to_and_set_meta`. Same instrument as `mutants-baseline.toml`: a list with reasons, and a
   failure when the trait grows past it. Maintenance.
4. **Extend the mutation nightly to `decode/`** (§4). 185 mutants, already measured, already named in
   `mutation-coverage.md` as the obvious next addition. Now carries a second argument: §2 shows a
   decode failure has no observable consequence today, which is the condition under which a
   decorative test there survives indefinitely. Maintenance; sequence it after item 1.
5. **A sampled `logsBloom` audit over a sealed range** (§1). The only independent oracle available
   without a second endpoint or a consensus client, one-sided in the useful direction, and free for a
   nest already fetching headers under RFC-0036. **Scope it to the empty case and say so in the
   name**: the bloom is a membership filter and cannot count, so it catches "nothing where there was
   something" and is blind to "nine where there were ten". Shape it as a mode of `nuthatch audit`, alongside the
   screening replay, and state the false-positive caveat in the output rather than in a footnote.
   The largest of the five; propose it only if item 1 finds anything.

Not proposed, and listed so nobody proposes them as though they were free: per-block bloom
verification in the ingest path (the sampling is the affordable part), dual-endpoint read comparison
(a second provider is a cost and a configuration burden, and item 5 gets most of the property for
nothing), and any classification of decode failures into the RFC-0026 fault machinery before item 1
has produced evidence about how often they occur.
