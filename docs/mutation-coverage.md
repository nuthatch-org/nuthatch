# Mutation coverage on the delightful core

**A test that passes when the thing it tests is deleted is not a test.** This project has been caught
by that repeatedly - four tests and three RFC acceptance criteria once passed with the mechanism they
tested removed - and the standing answer was "mutation-check it", by hand, when someone remembered.

Nobody remembered reliably. On 2026-08-22 two paths were mutation-checked by hand and **both** had a
hole (#725, #745). Two for two is not luck; it is a measurement of how much of a suite is decorative.

## What runs, and when

`.github/workflows/mutants.yml`, weekly (Sundays 03:00 UTC) and on demand, as one matrix job per file shard
(two for `chunker.rs`, eight for `seal.rs`, one for `registry.rs`), each running `cargo mutants
--file <file> --shard <n/m> -- --lib -- --skip a_wide_batch_stays_under_the_cursor`. The skipped
test measures one wide query's RSS in a child process. On a loaded runner it sits a few tens of
megabytes over its 512 MiB ceiling and aborts the unmutated baseline before a mutant is scored
(#2030). A baseline entry does not apply to that, and the ordinary suite still runs the test.
**Not on pull requests**, and the reason is measured rather
than assumed. On an 18-core machine, when the job landed (2026-08-23):

| | |
|---|---|
| unmutated baseline | 114s build + 154s test (all targets) |
| the same, `-- --lib` | 122s build + **28s** test |
| per mutant | ~150s, because each one needs its own rebuild |
| `src/chunker.rs` alone | 26 mutants |
| the scoped set | ~300 mutants |
| at `-j 6` on a busy box | load hit **70**, individual builds stretched to **802s** |

A per-PR gate at those numbers is one people learn to skip, which is the disease this is meant to
cure rather than catch. #768's own risk section said so before any of it was measured; the
measurement agreed.

## What is in scope

`src/chunker.rs`, `src/seal.rs`, `src/registry.rs` - the paths that decide what gets fetched and what
reaches stored state, where a decorative test is most expensive. Not the whole crate: that was **4,503
mutants** against ~300 here when measured on 2026-08-23 (the 2026-10-05 nightly tested 244 across
its eleven shards), and a signal nobody reads is not a signal.

`src/registry.rs` yields only 3 mutants (still 3 on the 2026-10-05 nightly) because #581 moved decode
into its own crate. The decode path is not skipped, it simply lives elsewhere - `cargo mutants -d
decode` was 185 mutants on 2026-08-23 and is the obvious next addition; the sweep's wall-clock is
now known (two to three hours per 25-mutant shard at `-j 2` on the 2026-10-05 run), and it has not
been added.

## The baseline

`.github/mutants-baseline.toml` lists survivors we have seen, looked at, and chosen not to chase
today - each with a reason. A survivor **not** in that file fails the job, naming the mutation and the
file. The gate is therefore about *new* decorative tests, not a demand to fix everything at once.

Removing an entry is the good direction and needs no ceremony. Adding one needs a reason a reader can
disagree with. `scripts/mutants-check.py` also prints baseline entries that no longer survive, so a
stale exemption gets noticed - #769's allow-list check caught exactly that shape the day after it was
built, on the author of the sprint.

Matching is on `(file, mutation text)` rather than line number, because a line number moves when
somebody adds a comment above it, and a gate that fires on unrelated edits is a gate people route
around.

## It is deliberately not a required check yet

The job **can** go red - that is criterion 1, and the check is proven to fire in both directions
against a real survivor rather than an invented one. But the workflow is required on no branch.

That is the sequence #593 spent a whole sprint establishing for the fuzz job: land it, let it run
red-capable on `main`, *then* decide whether it becomes required. **Adding a context that cannot fail
is worse than adding no context**, and this repo has installed one of those before - the review
signature check was red and required on nothing, so it blocked nothing.

Deciding to make it required is a board call, and it should be made after a few weekly runs have
shown what it actually costs and how noisy it actually is.

## The first survivor

The first scoped run found one, in the window controller #672 took five attempts to get right:

```
src/chunker.rs:134: replace < with <= in AdaptiveWindow::served_by_splitting
```

`if w < self.max` and `if w <= self.max` differ only when `narrowest_served == self.max` exactly: the
original leaves `self.window` untouched, the mutant also clamps it - which matters when `window > max`,
reachable after `set_max` lowers the ceiling under a wider window. A real gap and a narrow one, and
precisely the kind of boundary that #672 kept getting wrong.

Recorded in the baseline rather than fixed on the day the gate landed, because a gate whose first act
is to demand unrelated work is a gate people learn to route around.
