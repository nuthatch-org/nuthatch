# RFC-0054: The head count - an opt-in ping at `nuthatch init`, and the word in non-negotiable 3 it asks to change

**Status:** **Accepted 2026-10-08 (Chief).** Non-negotiable 3 is amended with §1's wording in
`CLAUDE.md`, and the acceptance is recorded in its build-order block. §11 is settled with this RFC's
leanings: (1) a single-purpose `count.toml`; (2) the receiver is the one-file binary on the Helsinki
host, and Vercel holds nothing; (3) `chain` is sent, under the registry-only rule; (4) the number is
published from the first week with §5.6's caveat. Slices: S1 the binary (`src/count.rs`, `count`
subcommand, `maybe_ask`, A1-A4, A6), S2 the receiver (`count/receiver/`, A5), S3 publishing: the site's `/count` page reads the receiver's
`GET /totals` live, and the README links to it. That replaces §5.6's weekly action and `docs/count/`
snapshots, because main is protected and a bot commit a week would be a PR a week; the receiver's
store already is the dated series. A7's test becomes the page showing a non-empty store. Previously **draft - design only, blocked on §1**
from 2026-09-11.

**Date:** 2026-09-11

**Author:** Pete (cargopete)

**Depends on:** RFC-0015 (`init` is the surface this rides), RFC-0046 §1 (the deletion test, applied
here as a build gate), RFC-0052 §0 (the "no network call the binary does not make today" standard
this RFC deliberately fails and says so), RFC-0007 (launch and validation - the number this
produces is the one that RFC never had).

**Blocks:** nothing. Enables an honest answer to "how many people use this", which today has none.

**Origin:** a question asked on 2026-09-11 - is there a way to see on GitHub how many people have used
nuthatch or created nests. There is not. Every public proxy (stars, clones, release-asset
downloads, crates.io downloads) measures interest or fetching, not running, and none of them can
count a nest. The only instrument that can is the binary, and the binary is forbidden from
reporting anything. This RFC is about whether that prohibition means what it says.

---

## Abstract

Nuthatch cannot count its own users. It can count stars (interest), clones and release downloads
(fetching), and crates.io downloads (mostly CI). None of these is a person who ran `nuthatch init`,
and none of them is a nest. The gap is structural: the only place a nest's creation is observable is
the machine it happens on, and non-negotiable 3 says the binary sends nothing about that machine
anywhere.

This RFC proposes the smallest counter that can exist under a plain reading of that rule's purpose:
**off by default, asked once, answered locally, two events, no identifiers, tallies not records,
and the tally published.** It then confronts the fact that non-negotiable 3 does not say "no
telemetry by default"; it says "no telemetry", and an opt-in ping is telemetry. §1 argues the rule
should be amended to say what it protects rather than what it forbids, and proposes the wording.
§10 option A is the design if it is not amended: publish the proxies, stop asking the question.

## 1. The non-negotiable this appears to violate, and why the answer is "it does"

`CLAUDE.md` §3: *"No phone-home. No telemetry, no mandatory API tokens, no gated data services. AI
features use local models (Ollama) or BYO API key, and degrade gracefully offline."*

RFC-0046 §1 showed that "no gated data services" binds the artefact, not the operator. RFC-0052 §0
went further and set a standard this tree now cites: *with no target configured the binary makes no
network call it does not make today.* Both arguments were reinterpretations that survived because
the text admitted them. This one does not. "No telemetry" is two words with no qualifier, and a
ping that reports a `nuthatch init` to a server we run is telemetry however it is switched on.
Reading "no telemetry" as "no telemetry unless asked nicely" would be exactly the drift `CLAUDE.md`
line 8 exists to stop - *when a task conflicts with the non-negotiables below, stop and flag it
instead of proceeding.* This RFC is the flag.

So the honest framing is not "why it does not violate the rule" but "what the rule is for, and
whether the wording serves it." Three things non-negotiable 3 protects, read against the tree:

1. **An operator's machine talks to nobody the operator did not choose.** `init` already makes
   network calls the operator did not configure by name - chain detection against public RPCs,
   ABI resolution against Sourcify and Etherscan, IPFS gateways for `--from-subgraph`
   (`src/project.rs:13-60`). Those are calls made *for* the operator, in service of the command they
   typed, and they carry the operator's inputs to third parties the operator can override. A count
   ping is a call made *about* the operator, to us. The distinction is the one that matters and the
   current wording does not draw it, because it did not need to: there was nothing in the second
   category.
2. **Nothing degrades offline.** A counter that could delay, fail or alter `init` would break this.
   §5.4 makes the ping fire-and-forget behind a two-second ceiling with no retry, and §7's first
   acceptance criterion is that `init` on a machine with no route to the counter host behaves
   byte-for-byte as it does today.
3. **Nobody is tracked.** This is the one the word "telemetry" is standing in for, and it is the one
   the design in §5 is built around: no install identifier, no nest identifier, no address, no path,
   no hostname, no per-ping record in the handler's store at all - only tallies, and the tallies
   public. §5.5 says what "no IP retained" does and does not prove once a request has crossed a
   network.

**The proposed amendment**, verbatim, replacing the first two sentences of non-negotiable 3:

> **No phone-home.** The binary makes no network call about the operator that the operator did not
> explicitly turn on. No telemetry by default and none required; no mandatory API tokens; no gated
> data services. The one permitted exception is the opt-in head count of RFC-0054, which is off
> until a person answers yes, carries no identifier, and must pass RFC-0046 §1's deletion test at
> every release: remove it from the tree and a self-hoster loses nothing.

That names the exception rather than leaving it to a reading, which is how RFC-0053's override of
RFC-0044 §11 was recorded and is the only shape a change to a non-negotiable should take. If Chief
prefers the two words as they stand, this RFC is closed rather than deferred, and §10 A applies.

## 2. Motivation

RFC-0007 shipped a launch kit and has been "launch ongoing" since. RFC-0006's grant applications
ask for usage and get proxies. The 2026-09-08 unfreeze picked a programme - RFC-0044 through 0048,
the first RFCs *about what nuthatch guarantees to people who are not running it* - on the strength
of GraphOps feedback, one Graphtronauts thread and a board report. Every one of those inputs is a
conversation. None is a count.

What the proxies say and do not say, as of this RFC (numbers from the one-shot stats script that
prompted it; re-run for current values):

| Signal | Measures | Can it count a nest? |
|--------|----------|----------------------|
| GitHub stars, forks, watchers | interest | no |
| GitHub traffic (14-day window, then discarded) | visits and clones | no |
| Release-asset `download_count` | binaries fetched, incl. every `install.sh` run and every CI job | no |
| crates.io downloads | mostly mirrors, docs.rs and CI; an upper bound on nothing useful | no |
| Homebrew tap | no analytics for taps at all | no |
| GHCR | no public pull count | no |
| Dependents ("Used by") | crates that depend on the library, not operators of the binary | no |

Two things follow. The floor of real use is unknown; it may be a dozen people or a few hundred and
the proxies cannot tell those apart. And the strategic decisions being taken now - which chains,
which surfaces, what to freeze for 2027 - are taken on the loud minority who talk to us. A count
does not fix that, but it bounds it.

The counter-argument, stated so it is not pretended away: a project that has said "no telemetry" in
its standing brief since day one has made a promise, and people who chose it partly for that promise
are entitled to find the promise kept. §5 is designed so that a person who never answers yes can
verify, with a packet capture, that nothing changed for them. That is not the same as the promise
being kept as written, which is why §1 asks for the wording to change rather than pretending it has
not.

## 3. Goals

1. A number that means "people who ran `init` and said yes" and a number that means "nests those
   people created", both floors, both published.
2. Off until a person types `y`. Not opt-out, not "opt-out with a notice", not on by default in any
   channel including the OCI image and the install script.
3. No identifier of any kind, client or server side. Two operators who answer yes are
   indistinguishable to us in every respect the payload carries.
4. `init` cannot be slowed, failed, or changed in output by the counter's presence, absence, success
   or failure.
5. Deletable: `git rm src/count.rs` plus the subcommand and the prompt, and every test that is not
   about the counter still passes.
6. The payload is shown to the person before they are asked, and `nuthatch count payload` shows it
   any time after.

## 4. Non-goals

- Crash reports, error reports, performance metrics, feature usage beyond the one event. Each is a
  different RFC with a different argument, and most of them should lose.
- Counting `dev`, `serve`, queries, or anything after `init`. A running nest is the operator's
  business; this counts a beginning, once.
- Counting installs. The install script and the packaging channels stay exactly as they are; a
  person who installs and never runs `init` is not counted and should not be.
- Any form of "improving the product with your data". The count improves nothing but our knowledge
  of one integer.
- A hosted anything. The receiver in §5.5 is a counter, not a service, and is deliberately too
  small to become one.

## 5. Design

### 5.1 Two events, no identifiers

| Event | Fires | Once per | Answers |
|-------|-------|----------|---------|
| `counted` | when a person answers **y** and the answer is stored | machine, per answer | "how many people" |
| `init` | when `nuthatch init` completes successfully and the stored answer is yes | nest created | "how many nests" |

There is no install id, no random token, no machine fingerprint. The cost of that is stated: a
person who reinstalls a machine and answers yes again is counted twice; a person who runs `init`
five times to get the aliases right creates five `init` events. So "people" is not a floor - it
is a floor with reinstall noise on top - and "nests" is a floor on attempts, not on nests that
lived. Both are still infinitely better than the current number, which does not exist, and neither
error grows with time in a way that changes a decision.

The design considered and rejected for that reason is a random 128-bit token generated at opt-in
and stored locally, which would give exact distinct-install counts and a delete-to-reset semantic.
It is rejected because the moment there is an identifier there is a record per identifier, and the
strongest thing this RFC can say - **we hold no record of any individual ping** - stops being true.
Go's telemetry made the same choice for the same reason and has lived with the same noise since
2023.

### 5.2 The payload

One JSON object shape, two values of `event`. `nuthatch count payload` prints both, populated
for this machine.

An `init` event is exactly this:

```json
{"v":1,"event":"init","version":"3.6.1","os":"linux","arch":"x86_64","chain":42161,"source":"addresses"}
```

A `counted` event is the same object with `"event":"counted"`. `chain` and `source` are present
on `counted` only when the ping is sent from `maybe_ask` after a successful `init`, and then they
are the chain and source of that init, the same values the `init` event that follows will carry.
They are **absent** (the keys are not in the JSON, not null) when the ping is sent from
`count on`, which has no nest. The client does not invent one.

A bare `count on` therefore sends:

```json
{"v":1,"event":"counted","version":"3.6.1","os":"linux","arch":"x86_64"}
```

The receiver increments `counted` total for that body. It does not increment per-chain or
per-source tallies from a `counted` event, whether or not those keys are present; those
breakdowns are `init`-only, per §5.5.

- `v`: payload schema version. A change to the field set is a bump, is an RFC amendment, and re-asks
  nobody - a person who said yes to v1 said yes to a stated set of fields, and a new field is a new
  question. Until they are re-asked, the client sends v1.
- `event`: `init` | `counted`. Not a free string.
- `version`: the nuthatch version, from `CARGO_PKG_VERSION`. Always present on both events.
- `os`, `arch`: `std::env::consts::{OS, ARCH}`. Coarse; a dozen values between them. Always present
  on both events.
- `chain`: on `init`, always present: the chain id **only if it is in the built-in registry**
  (`src/chains.rs`); anything else is sent as `0`. A custom chain id can be a fingerprint of one
  operator; a registry id cannot. On `counted`, present only in the `maybe_ask` path, with the
  same rule and the same value as the `init` that follows; absent on `count on`.
- `source`: on `init`, always present: `addresses` | `from` | `subgraph` - the three arms at the
  top of `project::init`. Which of them people actually use is the single most useful thing in the
  payload for RFC-0044's and RFC-0053's sequencing, and it identifies nobody. On `counted`, the
  same presence rule as `chain`.

Not present, and the RFC records why each was considered: the addresses (identify the nest, and
often the operator), the directory name (a path), the hostname, any timestamp finer than the
arrival day in UTC (the handler's store is day-grained and nothing finer), the RPC endpoints
(identify a provider account), the number of contracts (a fingerprint at the tails), and any extra
identifying header (reqwest's default User-Agent is a version string; the request sets it to the
literal `nuthatch-count/1`). The handler has no path that stores headers; that is the checkable
claim in §5.5, not a claim about ingress logs in front of the handler.

### 5.3 Asking, once, after the nest exists

The question is asked **after** a successful `init`, never before and never during, so a scaffold
is never held hostage to a prompt. It is asked only when all of the following hold:

1. stdin and stdout are terminals (`std::io::IsTerminal`, as `progress.rs` already checks);
2. no `CI` environment variable is set (the convention every CI vendor honours);
3. `NUTHATCH_NO_COUNT` is unset;
4. no answer has been stored for this machine;
5. `init` was not invoked with a flag that means "do not talk to me". As of 3.6.1 `InitArgs`
   carries no `--quiet` or `--yes` (verified 2026-09-11 against `src/cli.rs`); if one is added
   later it must gate this.
6. nothing is already waiting on stdin. A line typed or pasted ahead is the next command, not an
   answer to a question not yet printed, so it is left to the shell and the question waits for a
   later `init` (#2235).

The prompt, in full, printed once:

```
nuthatch has no idea how many people use it. Can it count you?

If you say yes it will send this, once now and once per future `init`, to
https://count.nuthatch-indexer.com and nothing else, ever:

  {"v":1,"event":"init","version":"3.6.1","os":"linux","arch":"x86_64","chain":42161,"source":"addresses"}

No identifier, no addresses. The handler stores no IP, header or body. The totals
are public at https://www.nuthatch-indexer.com/count. `nuthatch count off` reverses
this.

Count me? [y/N]
```

Default is **N**. Enter is no. Any answer, including no, is stored, so the question is asked exactly
once per machine and a no is never nagged. A person who wants to change their mind uses the
subcommand; the prompt never reappears.

The stored answer lives at `$XDG_CONFIG_HOME/nuthatch/count.toml`, falling back to
`$HOME/.config/nuthatch/count.toml`, following the `$XDG_CACHE_HOME` precedent in
`bench.rs:1461-1468`. This is the first user-level config file nuthatch writes; nothing else in
`src/` touches `XDG_CONFIG_HOME` or `~/.config` (verified 2026-09-11). That is a small new surface
and §11 asks whether it wants its own name. Contents:

```toml
# nuthatch count - see `nuthatch count --help`
counted = false
asked_at = "2026-09-11"
```

### 5.4 Sending

- HTTPS `POST`, JSON body from §5.2, through the existing `reqwest` client with `rustls` (no new
  dependency; the RPC path already carries it).
- One attempt. **Two-second ceiling**, enforced with `tokio::time::timeout`, after which `init`
  returns regardless. No retry: a retry is a second request, and a lost ping costs nobody anything.
- Every failure is `debug!` and nothing else. No warning, no stderr, no exit code change, no
  "could not reach count server" line. A person who answered yes on a machine with no route to the
  host will never learn that from `init`, and that is correct: the counter is not allowed to have a
  failure mode a person can see.
- The request is made only after `init` has written its last byte to the nest directory and its
  last line of normal output. If the process is killed in the two-second window the nest is whole.

### 5.5 The receiver

A counter, not a service: a single HTTPS endpoint whose entire behaviour is *increment some
tallies and return 204*. The committed handler keeps **no per-request record**. Concretely, per UTC
day the tally store holds one integer for each of: `counted` total; `init` total; `init` by
`version`; `init` by `os`/`arch`; `init` by `chain`; `init` by `source`. A `counted` event
increments `counted` total only. Per-chain and per-source keys on a `counted` body, if present,
are not tallied. Cardinality of the whole key space is a few hundred rows per day at most,
forever bounded by the registry size and the release count.

What this repository can prove, by reading the handler: it has no path that persists the request
IP, any header, or the request body after the tallies are incremented, and it takes no timestamp
finer than the day. That is the checkable claim. It is not a claim that no such metadata exists
anywhere the request passed through.

Two deployments, and they are not equivalent on that last point.

The path that has no platform-managed request log is a one-file binary on the Helsinki host,
beside the Lodestar nests. Ingress is ours. Logging is specified: no access log and no request log
of IP, headers, or bodies; retention of that metadata is none, because none is written. Process
logs are operational only (start, stop, panic, tally-store errors). This is the deployment A5's
logging half is written against.

A Vercel function remains a possible place to put the *tally store*, because the site already
lives there (`nuthatch-frontend`, which is where `install.sh` is served from) and a function in
front of a key-value counter is the smallest thing that increments. A Vercel function sits behind
provider-managed ingress and logging. The repository's handler cannot prove that the source IP,
headers, or request record never exist in those logs, and this RFC does not claim they do not. The
privacy and acceptance claims do not cover Vercel ingress logs.

The receiver's source is committed to this repository under `count/receiver/` so the handler
claim is checkable by reading forty lines, not by trusting us. §11 still carries the deployment
choice; it no longer pretends the two are the same for logging.

Is this the "hosted service" the out-of-scope list forbids? No, by the same reading `CLAUDE.md`
gives at line 219: the list forbids *us* running a data service and billing for it. A counter that
serves nobody, holds no data anyone could want, and would be unnoticed by every operator if it
vanished tomorrow is not that. It is closer to the website than to a service.

### 5.6 Publishing the tally

If we ask people to be counted, they see the count. Weekly, an action reads the receiver's tallies
and commits `docs/count/<year>-W<week>.json` and a rolling `docs/count/latest.json`; the website
renders `latest.json` at `/count`. The stats script from 2026-09-11 gains a section that reads it
alongside the proxies, so the one place the numbers are compared is the one that says which is
which.

The README gets one line under the progress log's numbers, in the house voice: *"N people have
said yes to being counted and created M nests. Most people say no or are never asked, so these are
floors."* Numbers over adjectives, and no adjective would survive the second sentence anyway.

### 5.7 The subcommand

```
nuthatch count            # status: on/off/never asked, config path, last payload shape
nuthatch count on         # store yes; sends one `counted` with chain and source absent
nuthatch count off        # store no; sends nothing, ever again
nuthatch count payload    # print both v1 objects this machine can send, populated
nuthatch count forget     # delete the config file; the question may be asked again
```

`on` from a non-TTY works (it is an explicit command, not a prompt), which is how an operator who
manages fleets by script can opt a machine in deliberately. It has no nest and does not look for
one on disk, so the `counted` payload it sends is §5.2's bare object: `chain` and `source` are
absent. There is no `--count` flag on `init` and there will not be: a flag on the hot command
invites the install script and every tutorial to add it, and the count should be a thing a person
did on purpose.

## 6. Implementation sketch

One module, `src/count.rs`, ~200 lines including the prompt text and the config file:

- `pub fn maybe_ask(kind: Source) -> ()` - called last in `project::init` on the success path only.
  Checks §5.3's five conditions, asks, stores, and on yes calls
  `send(Event::Counted { chain, source })` then `send(Event::Init { chain, source })`, both
  populated from the init that just succeeded (same `chain` and `source` on both). On a stored
  yes, calls `send(Event::Init { chain, source })` alone.
- `count on` calls `send(Event::Counted { chain: None, source: None })`. Those `None`s serialise
  as absent keys, not null, which is the bare object in §5.2.
- `async fn send(ev: Event)` - builds §5.2's payload, posts under `timeout(2s)`, `debug!`s any
  error, returns `()`. Never `Result`. Nothing upstream can react to it.
- `Answer::{load, store}` over the config path from §5.3.
- `cli.rs`: a `Count` subcommand with the five arms of §5.7.

The receiver, under `count/receiver/`, is the smallest worker that satisfies §5.5, with its own
README stating what it stores in the same table form as §5.5 so the two documents can be diffed.

Nothing touches `seal.rs`, `store.rs`, `indexer.rs` or any path that runs after `init`. The word
"count" does not appear in `dev`, `serve` or the runtime.

## 7. Testing and acceptance

Each criterion is written so that it can fail.

- **A1 - silence by default.** A packet capture of `nuthatch init 0x… --chain arbitrum` on a fresh
  machine, run to completion with stdin not a TTY, shows requests to RPC endpoints, Sourcify or
  Etherscan and **zero** requests to the count host. Same run with a TTY, answering `N` or pressing
  Enter: zero. Same run with `CI=1` or `NUTHATCH_NO_COUNT=1`: zero and no prompt.
- **A2 - unchanged offline.** With the count host blackholed in `/etc/hosts` and a stored yes,
  `init`'s stdout, stderr, exit code and the resulting nest directory are byte-identical to a run
  with a stored no. Wall clock differs by at most the two-second ceiling.
- **A3 - the payload is the payload.** A test serialises `Event::Init` for every registry chain and
  every `Source` and asserts the key set is exactly `{v, event, version, os, arch, chain, source}`,
  `event` is `"init"`, and a non-registry chain id serialises as `0`. The same test serialises
  `Event::Counted` twice: with `chain` and `source` unset, the key set is exactly
  `{v, event, version, os, arch}`, `event` is `"counted"`, and `chain`/`source` are absent not
  null; with the chain and source of a just-completed init, the key set is exactly
  `{v, event, version, os, arch, chain, source}`, `event` is `"counted"`, and `chain`/`source`
  match the `Init` that follows. This is the test that fails when someone adds a field without
  an RFC amendment.
- **A4 - the deletion test, mechanised.** A CI job on a branch that removes `src/count.rs`, the
  `Count` subcommand and the `maybe_ask` call must build and pass every test not tagged `count`.
  RFC-0046 §1's test is a paragraph; this one runs.
- **A5 - the committed handler stores none of the request.** Reading `count/receiver/` shows no
  path that writes the request IP, any header, or the request body after the increment. After
  1,000 synthetic pings from 1,000 distinct source IPs, the tally store contains no value that
  varies with the IP, and its size is a function of the distinct `(day, field)` keys only. A
  reviewer can dump it and see tallies. If the receiver is the Helsinki binary, it is configured
  with no access or request log of IP, headers or bodies; process logs are operational only. A5
  does not claim that a Vercel function's ingress logs are empty, and a Vercel deployment does
  not satisfy the no-platform-log half.
- **A6 - once.** Two consecutive `init`s on a TTY produce one prompt. `count forget` then `init`
  produces a second.
- **A7 - the number exists.** Four weeks after S2 ships, `docs/count/latest.json` exists, has been
  updated by the action at least three times, and the README line reads from it. If the number is
  small, the README says the small number.

## 8. The decision that is not engineering

§1 is Chief's, not mine to settle alone, and the arguments for a no are real:

- "No telemetry" has been in the standing brief since the skeleton. Some fraction of the people this
  RFC wants to count chose nuthatch *because* of it, and a carefully-worded exception is still an
  exception. The most-trusted projects in this space - cargo, rustup, nix, reth - send nothing and
  are not worse for it.
- The number will be small and noisy, and small noisy numbers get quoted. "N people have said yes"
  in a grant application is a worse sentence than no sentence, if N is eleven.
- Once a receiver exists, the next RFC that wants to add a field has a place to send it. §5.2's
  schema-version rule and A3 are the guard; guards erode.

And the arguments for a yes, which this RFC obviously holds:

- Every decision about what this project is for is currently taken on anecdote. A floor, even a
  bad one, is a fact.
- The design gives a no-sayer a stronger guarantee than they have today: today the rule is a
  sentence in a markdown file, and after this RFC it is A1, a packet capture that anyone can repeat.
- The precedent this follows is the one that survived scrutiny. Go's telemetry proposal was
  opt-out in February 2023, was changed to opt-in after the response, publishes every number it
  collects, and holds no identifier. That is the shape here, minus even the weekly upload.

The RFC recommends yes with the amendment in §1, and records that a no closes it cleanly.

## 9. Risks

- **A person answers yes without reading.** Mitigated by showing the payload before the question
  and by the default being no; not eliminated. The thing they consented to is, by design, the
  thing a careful reader would not mind - that is the whole reason §5.2 is so short.
- **The receiver goes down.** Nobody notices, by §5.4. The tally has a gap and `docs/count/`
  shows it. Not a risk to any operator.
- **The receiver is compromised.** An attacker gains tallies. There is nothing else in the
  handler's store to gain, by A5, and that is the property to protect at every change to the
  receiver. Compromise of a Vercel account is a different surface, which is why Helsinki is the
  path with no platform-managed request log.
- **Scope creep.** Named in §8. The concrete guard is that `v` is bumped by an RFC amendment, A3
  fails on any drift, and the prompt text is what people said yes to.
- **The number is used dishonestly.** Floors get reported as counts. The README line in §5.6
  carries its own caveat in the same sentence so it cannot be quoted without it.
- **The XDG config file is new surface.** A second feature will want to put something in it. §11
  asks the question now so it is a decision rather than a habit.

## 10. Alternatives considered

- **A. Do nothing to the binary; publish the proxies.** The stats script exists; commit its output
  weekly to `docs/count/` and stop asking the question. This is the design if §1 is refused, and it
  is not nothing: a public, dated series of stars and release downloads is more than most projects
  keep. It still cannot count a nest.
- **B. Opt-out with a first-run notice**, the .NET SDK / Homebrew / Astro shape. Rejected without
  much debate: it is the shape that has cost every project that chose it a thread on the orange
  site, it puts the burden on the person who cares most, and it is not what "opt-in" means.
- **C. A random install token** for exact distinct counts. Rejected in §5.1; the noise it removes
  is smaller than the guarantee it breaks.
- **D. Piggyback on an existing call.** `init` already fetches from Sourcify; a request to a URL
  we control for, say, the chain registry would be logged for free. Rejected because it is exactly
  the thing §1's first bullet distinguishes: a call made *for* the operator that quietly also
  reports *about* them. Worse than a ping, not better, and the reason RFC-0052 §0's standard was
  written the way it was.
- **E. Count at `install.sh`.** Measures installs, which is what release-asset downloads already
  approximate, and cannot be opted into before the binary exists to ask. Rejected.
- **F. Heartbeat from a running nest** (weekly `alive` event). Would give "nests that lived", the
  number §5.1 admits it lacks. Rejected for v1 because it moves the counter out of `init` and into
  the runtime, which is the boundary §4 and §6 hold; may be its own RFC if the `init` count proves
  to be worth having.

## 11. Open questions

1. Does the user-level config file want a broader name (`~/.config/nuthatch/nuthatch.toml` with a
   `[count]` table) so the next thing that needs a home does not create a second file - or is a
   single-purpose file the point, because a second thing needing a home should be its own decision?
   The RFC leans single-purpose and asks.
2. Where the receiver runs: a Vercel function beside the static site, or a one-file binary on the
   Helsinki box. These are no longer equivalent for logging. Vercel is a possible place for the
   tally store and has no process to keep alive; it does not satisfy A5's no-platform-log half,
   because ingress logs are the provider's. Helsinki has a process someone runs, and is the
   deployment whose logging we specify: no access or request log of IP, headers or bodies.
3. Should `chain` be sent at all, or is `source` alone enough? The RFC keeps it because "which
   chains people actually create nests on" is the question RFC-0030/0031/0050/0051 answered by
   guessing; §5.2's registry-only rule is what makes it safe.
4. Whether A7's four-week window is right, or whether the number should not be published until it
   has enough days behind it that a single week's noise does not dominate. Publishing early and
   saying so is the house habit; the RFC follows it.
