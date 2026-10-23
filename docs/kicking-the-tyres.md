# Kicking the tyres

**A guide to finding out whether nuthatch is any good, written by the people who make it.**

This is not [`verification.md`](verification.md), which is a runbook for proving *your* deployment
works once you have decided to run one. This is for the step before that: you are evaluating, you are
sceptical, and you want to know where it breaks before you build on it.

So the posture here is adversarial. **You are not trying to confirm the claims. You are trying to
falsify them**, and this document tells you where we think you have the best chance, including the
places we have already been wrong.

If you find something, we would rather have it as an issue than not know:
<https://github.com/nuthatch-org/nuthatch/issues>. A finding that makes us look bad is worth more to
us than a clean run.

---

## Before you start: use a paid RPC endpoint

**This is the setup instruction, not a footnote.** nuthatch assumes a paid endpoint for serious work.
The public RPCs bundled with a scaffolded nest exist so that `init` → `dev` works with zero setup and
nothing else - they are rate-limited, shared, and will sometimes return an empty result rather than an
error.

Evaluate on a free tier and you will mostly be measuring the free tier. We have measured that
directly, on RFC-0039's recorded tape (RFC-0040, 2026-08-23, 2.7.x): on a rate-limited endpoint,
**12 calls the indexer made became 84 HTTP requests** - a 7x retry amplification - because a `429`
was retried up to four times across the endpoint pool. Being throttled made a nest send more, which
throttled it harder. Since 4.7.0 an endpoint that answers `429` rests and is routed around, with
`Retry-After` honoured, so the multiplier should be smaller now; it has not been re-measured, and
on a free tier you would be benchmarking whatever is left of that loop.

So: **the primary pass is against an endpoint you pay for.** If you want the free-tier numbers too,
run them as an explicit second arm and label them as such - it is a fair question, and it is not the
same question.

If a paid endpoint is not something you are willing to provide, that is worth knowing before you go
further: it is a real cost of running this, [what it costs](https://nuthatch-indexer.com/docs/operate/costs/)
puts a figure on it, and no amount of testing will make that go away.

---

## Rule zero: write down what you expect, before you run anything

Not ceremony. We have been caught by this repeatedly, and so will you.

Four identical 90-second runs of the same demo once measured **2, 15, 28 and 198 events**
([`docs/sprint-meticulous-magpie.md`](https://github.com/nuthatch-org/nuthatch/blob/ede03ca7/docs/sprint-meticulous-magpie.md), August 2026). A figure of `289 events/sec`, measured 2026-07-16,
outlived the harness that produced it by five weeks and ended up in a published document
(`docs/releases/v2.7.0.md`). A benchmark said seal-direct was 8.7x faster in July 2026, 5.2x one
morning and 0.92x that same afternoon (`docs/benchmarks.md`, settled 2026-08-23).

**Predict the number, then measure it.** Without a prior expectation you will accept whatever you get
and call it fine - and so did we.

---

## Phase 1 - The cold walk

The pitch is *an address to a live indexed API in under two minutes*. Test that claim as a stranger:
a fresh directory, only the published docs, no reading the source when something is unclear, and a
stopwatch.

```sh
nuthatch init 0xYourContract --chain mainnet
nuthatch dev --backfill 300        # without it, dev backfills from the deployment block
nuthatch sql "SELECT count(*) FROM <alias>__<event>"
```

**Pick a contract we did not pick.** Ours are WETH in the README and USDC in `verification.md`,
both dense, mainstream and with a well-behaved ABI. Yours should be the awkward one: a proxy, an
unverified implementation, a contract that deployed two years ago, one that emits nothing for months
at a stretch.

Write down every moment of friction as it happens, including the ones you would normally shrug past.
Those are the findings; the stopwatch is only the headline.

**What a bad answer looks like:** it takes far longer than two minutes and the output does not tell
you why. Slow with an explanation is a different product from slow in silence.

---

## Phase 2 - Correctness, against something that is not us

The strongest test available, and the only one where the answer is not a matter of opinion:
**reproduce a subgraph and compare the numbers.**

Point a nest at a contract that a public subgraph also indexes. Query the same quantity from both.
They either match or they do not.

- Exact matches are the claim.
- A divergence is interesting **whichever way it falls**. When we did this for a Graph Horizon nest
  (#649, August 2026, `docs/network-snapshot-counts.md`) we were wrong on three fields and the
  reference was wrong on one - its own `stakedIndexersCount` (88) disagreed with its own entity set
  (97) by nine.
- Check totals *and* row counts *and* boundaries. An aggregate can be right while the underlying set
  is wrong.

**What a bad answer looks like:** the numbers agree and neither of you can say why, or a divergence
gets explained rather than measured.

---

## Phase 3 - What it costs, not just how fast

Speed is the easy half. **Ask what a month of this costs**, because that is what decides whether you
keep it running.

```sh
curl -s localhost:8288/metrics | grep nuthatch_rpc_requests_total
```

Take that reading, wait an hour, take it again.

Known, measured, on our own reference deployment (issue #750, audited 2026-08-22, on 2.x; the
figures are in `docs/operators.md` under *What a nest costs at tip*):

| | |
|---|---|
| four nests, one week | **~11.8M RPC requests** |
| HTTP requests they served | **~100** |
| one Arbitrum nest, steady state | **~549,000 requests/day** |

Those 2.x figures were later traced to the poll loop's two-second cadence, not to the rows. Today a
caught-up cursor polls every five minutes by default (`--poll-interval`), and since 3.11 a timestamp
comes from the log wherever the node supplies `blockTimestamp`, with a header fetched only where it
does not. Attribute your own bill by method:

```sh
curl -s localhost:8288/metrics | grep nuthatch_rpc_methods_total
```

and multiply each method by your provider's schedule. `/ready`'s `freshness` object shows the poll
interval and whether the cursor follows the tip or finality.

**Watch for the multiplier if you run the free-tier arm.** The 7x described in *Before you start*
is the thing to look for: request count several times nominal, and rising as the endpoint throttles
harder. On the paid arm it should not appear at all, because the retry loop never starts - and if it
does appear, that is a more interesting finding than anything else on this page. See RFC-0040.

**What a bad answer looks like:** you cannot attribute the bill. Take the reading before you form an
opinion about the cost.

---

## Phase 4 - Break it on purpose

Nothing here is exotic. All of it has happened to us.

- **A bad provider minute.** Point it at an endpoint that rate-limits, or one that lies by returning
  an empty result instead of an error. Public endpoints do this. Does the nest stall loudly or
  quietly?
- **Kill it mid-backfill.** `kill -9` during a run, then restart. Does it resume, and is the data the
  same as an uninterrupted run?
- **A reorg.** Hard to arrange deliberately; if you catch one, check that the affected rows converge
  to the canonical chain rather than keeping both.
- **A config change under a running nest.** Add a contract, change an alias, restart. Then check that
  `schema.json` moved with it - and that if it did not, the nest *said so*.
- **Disk pressure.** Fill the volume during a seal and see what the next query reports. A damaged
  sealed segment should reduce its own table and say `degraded`, not fail the whole query.

**What a bad answer looks like:** anything that keeps reporting success. A run that exits 0 having
lost data is the worst outcome on this page, and we have shipped one - see the honesty section.

---

## Phase 5 - Red team

**This is for an instance you own.** Everything below is about checking your own exposure.

The `/sql` surface is the interesting one, and it has a history:

- **v0.6.2** fixed an **arbitrary file write** via `;`-stacked `COPY ... TO`.
- **v0.9.3** fixed an **arbitrary file read**: DuckDB, the engine until 4.0, accepted a quoted
  function name and the guard matched only the unquoted form, so
  `SELECT * FROM "read_csv"('/etc/passwd')` executed.

Both were ours. So:

- Try to read a file. Try it quoted, unquoted, and with whatever casing and whitespace you can think
  of. **We have been caught by exactly this twice.**
- Try to write one. Try stacked statements.
- Check what binds where. `/sql` binds `127.0.0.1` by default and the admin UI at `/_admin/`
  needs a token on every request, made in `.admin-token` when you set none - verify that on your box
  rather than believing this sentence.
- If you put it behind a proxy, check the proxy is not the only thing standing between the internet
  and an unauthenticated SQL endpoint.
- Webhook signatures: `X-Nuthatch-Signature` is HMAC. Verify it actually verifies.
- **Was an open finding:** [#289](https://github.com/nuthatch-org/nuthatch/issues/289), closed
  2026-08-24 - until 4.0, DuckDB's `allowed_directories` did nothing unless `enable_external_access`
  was false at startup, and that flag was then passed. Since 4.1.0 the engine is Burrmill, and the
  denylist in front of it is what refuses `read_csv` today, quoted or not. Press it.

**What a bad answer looks like:** a guard that matches on a string. If you can find one, it is
probably bypassable, and that is the shape both previous holes had.

---

## Things to watch out for, because we have been wrong about them

The most useful section, and the least comfortable to write.

- **A green tick that means nothing.** Our signature-gate check was red *and required on no branch*,
  so it blocked nothing. A fuzz job reported success regardless of outcome. Four tests and three RFC
  acceptance criteria once passed with the mechanism they tested **removed**. If a check matters to
  you, delete the thing it checks and confirm it fails.
- **Absent data reading as healthy.** A failed timestamp batch once wrote `block_timestamp = 0` and
  reported success. `count(block_timestamp)` counts a zero, so our own first check said "0 missing"
  and we believed it. When something looks clean, ask what a broken version would have looked like.
- **Public RPC endpoints are not a test environment.** Ours are bundled so that `init` → `dev` works
  with zero setup. They are rate-limited, shared, and will sometimes return an empty result rather
  than an error. Measuring nuthatch through one measures the endpoint: we found the **network was
  99.3% of backfill wall clock** (`docs/benchmarks.md`, measured 2026-08-23 on RFC-0039's replay rig).
- **Documentation rots faster than code.** `llms.txt` told coding agents to run `nuthatch roost` for
  five releases after that subcommand was removed. If a doc and the binary disagree, the binary is
  right - and please tell us.
- **Benchmarks measure the harness until proven otherwise.** Ours has been caught three times
  measuring something other than what it claimed. If a number matters to you, reproduce it.

---

## Reporting what you find

An issue with the command, the expected result and the actual one is worth more than a polished
report. If it is a security finding, say so in the title and we will treat it accordingly.

We would genuinely rather you found it than a user did.
