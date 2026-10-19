# Verifying a nuthatch deployment

A falsifiable acceptance runbook. Every step has a **command**, an **expected result** you can compare
against, what it **proves**, and what a failure means.

This is deliberately not [`operators.md`](operators.md), which tells you *how to run* nuthatch. This
tells you *how to prove it works*, for your own sign-off, or so a second operator can independently
confirm the claims this project makes rather than taking them on trust.

## Run it, don't read it

Most of this document is executable:

```sh
./scripts/verify.sh 0 1 2      # artifact, single nest, correctness
./scripts/verify.sh 5          # scaled mode (needs a fleet up)
./scripts/verify.sh all --strict
```

Each check maps to a numbered step below, asserts a concrete result, and prints what failed with its
output. **A skip is not a pass** - steps whose prerequisites are absent are counted separately, because
a green run that silently skipped the interesting half is worse than a red one. `--strict` turns any
skip into a failure. **CI does not pass it** - `the compose fleet comes up` runs `verify.sh 5` plain,
against `target/release/nuthatch` - so `--strict` is a thing you do by hand, and this line used to
claim otherwise.

The steps that genuinely need a human are marked as skips with the procedure attached rather than
faked: a restart drill, a deliberate reorg, breaking a nest to watch its co-tenants survive, and
comparing a row count against an independent source. That last one is the step separating *it ran* from
*it is correct*, and no script can do it for you.

CI runs the fleet half on every push to `main` and every pull request - `the compose fleet comes up`
stands the whole stack up, walks level 5, and asserts exactly one worker takes the lease.

## How to use it

Levels are independent and cumulative. **Run the levels that match what you deploy**: level 5 is
irrelevant if you run a single nest, and levels 1 to 4 still matter if you run a fleet.

Every step is written so a *failure is unambiguous*. If a step says "expect `ok`" and you get something
else, that is a finding worth reporting, not something to interpret. Wherever an expected value depends
on your chain or contract, the step says so.

**Please report results, including passes.** A level someone actually ran is worth more than a level we
assert. Open an issue with the level, the step, what you expected and what you got.

### What we have verified ourselves

Stated plainly so you know which steps are re-confirmation and which are genuinely new evidence.

**Each row says *which release* it was verified on, not just yes or no.** A bare "yes" ages silently:
it reads as current however long ago it was earned, and this is the one page in the repo whose entire
purpose is being checkable. Two rows had drifted that way by 2.4.0 and are now dated (#571). Level 0
was a third and read differently: "by hand, **per release**" is a claim about a *practice*, which ages
worse than a bare "yes" because it sounds like a standing guarantee while naming no release at all. It
now names one.

| Level | Verified by us | On what |
|---|---|---|
| 0 Artifact | yes, **by hand, on 2.4.0** | From the *published* `nuthatch-x86_64-unknown-linux-gnu.tar.gz` of **v2.4.0**, on **2026-08-14**: checksum `OK` against the release's own `.sha256`, `nuthatch --version` reports `2.4.0`, the worker role refuses by name, and the binary's highest required symbol is exactly `GLIBC_2.34`, read off `objdump -T` rather than inferred from the fact that it started. All four steps, `verify.sh 0` green. **Not automated** - `release.yml` publishes the checksums and nothing in CI verifies a published artifact afterwards, so this is a practice rather than a gate, and it is only as current as the last person who did it. **Repeated on 4.10.1, 2026-10-05, on an Apple Silicon Mac:** the published `aarch64-apple-darwin` tarball, `verify.sh 0` all four green beside its `.sha256` (the first arm64 run), and `gh attestation verify` exit 0. The same day's `x86_64-unknown-linux-gnu` tarball: checksum `OK`, attestation verified, and `objdump -T` on the unpacked binary gives `hypot`/`hypotf` at `GLIBC_2.35` as its highest requirement, linking `libc`, `libm` and `libgcc_s` only, read on the Mac without running it |
| 1 Single nest | CI every commit; **production through 2.4.0** (2026-08-15) | CI, plus the Lodestar production box upgraded **2.0.0 → 2.4.0** in place, a four-version jump. Level 0 first on that machine: the *published* Linux tarball, `sha256sum -c` → `OK`, binary reports 2.4.0, glibc 2.43 against the 2.35 floor. Then all three services (`nuthatch`, `nuthatch-gns`, **`horizon-nest`** - the third is not named `nuthatch*` and a name-based enumeration misses it) restarted onto the new binary, confirmed by `readlink /proc/<pid>/exe` rather than assumed. **Row counts identical at a fixed watermark** (block ≤ 494,743,936): staking 127 / 8 / 261 / 142, GNS 3,529, plus 19 horizon tables - no gaps, no duplicates. Resumed from checkpoint, `as_of` climbing 494,747,848 → 494,754,297 (80 blocks in 20 s). End-to-end proved by **data match, not a flag**: the dashboard's newest delegation event `0x138872fd…` for 77,100,063,717,931,471,486,199 tokens returns from the nest at block 494,718,328. **One gap, self-inflicted:** the before-snapshot was piped through `head -30`, which SIGPIPE'd the capture, so only 19 of horizon's 73 tables have a before-value. Those 19 match; the other 54 are unverified and cannot be retrospectively. The two nests Lodestar depends on are fully covered. **Steps 1.1 to 2.2 repeated on 4.10.1, 2026-10-05, macOS:** a WETH nest over the last 200 mainnet blocks through the bundled public endpoints; `weth__transfer` at a fixed watermark counted 6,169 before a SIGTERM and 6,169 after the restart, and `nuthatch check` passed |
| 2 Correctness | yes | CI (deterministic fixtures, property tests) |
| 3 Many nests | yes | live two-chain run, 8-nest density run, and a 2.0 two-alias/one-dataset run. **3.1, 3.4 and 3.6 repeated on 4.10.1, 2026-10-05:** a one-chain runtime, a second mount of the same dataset reaching `live` and unmounting while the first kept answering, and `cross-nest-adoption.sh` 10 of 10 |
| 4 Guards | yes | CI + a live `/sql` adversary check; **4.1 and 4.2 repeated against 4.10.1 on 2026-10-05**, quoted `"read_csv"` included. **4.4 is CI-only so far** - the flip refusal and the schema-version stamp are covered by tests; no one has yet run a timestamp-free nest over a long backfill and timed it, so we publish no speed figure for it |
| 5 Scaled mode | **across machines on 2.4.0** (2026-08-15), except skew | Two real machines on the **published 2.4.0 artifacts**: control plane, Postgres and an FE on one host, a second writer on another, reaching the store over an SSH tunnel. **5.1** both register - `{"count":2,...,"thinkpad-remote"}`. **5.2** declaring a nest starts it with no restart. **5.3** exactly one owner for the cursor. **5.4** killing the holding writer moved the lease to the remote machine and incremented `owner_fence` to 2, so the fence was exercised by a **real handover** rather than a simulated one (it reached 3 later, on a second genuine handover). **5.5** the dead writer aged out of the registry on the 30 s TTL. **5.11** the remote worker indexed **2.2 M blocks** into the other machine's Postgres - the check that could not be completed on 0.9.3, where a held cursor produced no indexing. **5.9** with the control plane stopped and Postgres deliberately left up, it indexed **3,000,000 blocks through the outage** (61,733,584 -> 64,733,584) and resumed on healing, never losing the lease; the script asserts control returns `000` and Postgres returns `1` *before* claiming a pass, so a cut that never happened cannot pass trivially. **Skew is NOT re-verified** and remains proven on v0.9.3 only: pushing a worker's clock needs root on a machine running other work. What was measured instead is the renewal cadence the skew claim rests on - `lease_expires_at` advancing 77 s over a 70 s window, on the store's clock. **One improvement found:** a worker holding a cursor with no nest present was *silent* on 0.9.3 (`nests=0`, cause unstated); on 2.4.0 it warns, naming both the cause and the remedy. |

Level 5 is where independent verification is worth the most, for exactly that reason.

### What we do not claim

A page whose purpose is being checkable owes the reader its limits as plainly as its results. Every row
above says which release it was verified on; this section says what none of them establish.

**We do not verify that the chain we indexed is the chain consensus agreed on.** Nuthatch follows an
execution-layer RPC endpoint and hash-links the headers it is served. That proves the blocks it holds
form an **internally consistent** chain, and it is what makes reorg convergence checkable. It does not
prove that chain is the network's canonical one, because settling that needs consensus-layer data -
attestations and finality from the beacon chain - which nuthatch does not read.

In practice the gap is narrow: a single honest endpoint closes it, and disagreement between two
endpoints surfaces as a reorg the hot store handles. But the failure it leaves open is real. **An
endpoint that lies consistently is indistinguishable, to us, from one that tells the truth** - the
headers hash-link, the reorg logic never fires, and every downstream check passes. Content-addressed
segments and deterministic re-execution do not help here either: they prove we processed what we were
given identically, not that what we were given was right.

The honest mitigations are operational rather than cryptographic: use an endpoint you trust, and where
it matters, run two nests on two providers and compare sealed segment hashes. RFC-0011 already does
exactly that between the GraphOps host and the Hetzner shadow, and cross-operator segment-hash equality
is the strongest independent check currently available to us.

Stated because Edge & Node stated it publicly about their own system (RFC-0043 §7.3) and it applies to
us in precisely the same way. A limitation that is true of the whole class is not a reason to stay
quiet about ours.

---

## Level 0: the artifact is what it claims

**0.1 Version matches the tag you downloaded**

```sh
nuthatch --version
```

Expect the version you fetched. *Proves* you are testing what you think you are. A mismatch usually
means a stale binary earlier in `PATH`: check `command -v nuthatch`.

**0.2 Checksum**

```sh
sha256sum -c nuthatch-x86_64-unknown-linux-gnu.tar.gz.sha256   # Linux
shasum -a 256 -c nuthatch-aarch64-apple-darwin.tar.gz.sha256    # macOS
```

Expect `nuthatch-…tar.gz: OK`. *Proves* transport integrity. Run it in the directory you unpacked
into, next to the tarball the checksum file names: `-c` reads that name out of the file rather than
taking it from you.

`verify.sh` covers this step too, and only when it can: it wants the tarball and its `.sha256` sitting
beside the binary, which is only true if that is how you got it. A binary you built yourself gets a
**skip** naming what was absent, not a quiet pass.

**0.3 It runs on your libc**

```sh
nuthatch --version && echo started-ok
```

Expect no `GLIBC_… not found`. The Linux binary needs **glibc 2.35** since 4.1.0 (2.34 before),
so Debian 12, Ubuntu 22.04 and newer are fine, and RHEL 9 and Amazon Linux 2023 are not. *If this
fails*, your distro is older than the floor; build from source or use the container image.

**0.4 Embedded mode carries no database driver**

```sh
nuthatch worker --control-db x --hot-store y --chains z
```

Expect a refusal naming `--features postgres-store`. *Proves* the default artifact is the embedded one
(non-negotiable 1). The subcommand is *listed* in `--help` on purpose: a command that vanishes
depending on build flags is harder to diagnose than one that explains itself.

---

## Level 1: a single nest, end to end

The under-two-minutes claim. Use any contract on any supported chain; a busy ERC-20 is easiest.

**Verified on Monad with 3.3.0, on both boxes (2026-09-03 macOS, 2026-09-04 Linux).** The published
`x86_64-unknown-linux-gnu` tarball on the Hetzner box, checksum `OK`, keyless: `init` on the
AgoraDollar proxy resolved the ABI through Sourcify in **1 s**, `dev --backfill 2000 --seal-direct`
sealed 321 events in **1 s** and caught the tip in under a second, then followed about seven blocks
(two seconds) behind with zero reorgs. On macOS the day before, 73 events, 1 s, 4 s to tip. The
Sourcify step only passes on 3.3.0 or later: 3.2.0 and before asked for a field selector Sourcify
had stopped accepting (#1138) and fell through to sources that need a key.

**Bring an RPC endpoint.** `--rpc <url>` *replaces* the pool - it is the whole set of endpoints the
run may contact, and nothing public is appended after it (an earlier version of this sentence said it
"adds", which `select_rpcs` has never done) - so pass one and the built-in public endpoint stops being
the bottleneck. On 2.4.0 with the mainnet default alone, this
exact walk 429s on `eth_getBlockByNumber` and indexes **nothing** - the cursor holds position by design
rather than sealing zeroed timestamps, so you get a live, queryable, empty API. The two-minute claim is
about *API live and answering with provenance*, which it comfortably makes (17 s, measured on the
published 2.4.0 tarball); it is not a claim that a public endpoint will feed you 2000 blocks of USDC in
that time. #578 was the related defect, closed 2026-08-15: `/ready` said `ready:true` throughout.

**1.1 Scaffold**

```sh
nuthatch init 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 --chain mainnet --alias usdc --dir /tmp/v-usdc
```

Expect a scaffolded nest, and a printed table count > 0 (17, for this contract). *Proves* ABI
resolution and code generation.

**`--alias usdc` is doing real work here, not decoration.** Without it the alias is the contract name
from the ABI (`fiat_token_v2_2__*` for this one; `c0__*` only when the ABI carries no usable name),
and step 1.3 below - which asks for `usdc__transfer` - fails with a catalog error. It did, for anyone
who ran this page verbatim before 2026-08-14.

**If it warns that sampled logs match none of the resolved ABI's events**, that is the check working:
you have hit a proxy whose implementation ABI the public resolvers did not return, or one whose
earlier implementation emitted different events (USDC warns about its pre-upgrade history). Re-run
with `--abi path/to/implementation.json`, or add the earlier ABI as a second `[[contracts]]` entry
bounded to its era. A nest that indexes nothing is the failure this warning exists to prevent.

**1.2 Index and serve**

```sh
nuthatch dev --dir /tmp/v-usdc --backfill 2000
```

Expect progress output, then a bound listener. *Proves* ingestion, decode, storage and serving in one
process.

**1.3 The API answers**

```sh
curl -s localhost:8288/health          # expect: ok
curl -s localhost:8288/tables | head   # expect: a non-zero count and your tables
curl -s --get localhost:8288/sql --data-urlencode 'q=select count(*) from "usdc__transfer"'
```

Expect a non-zero count. *Proves* the decoded data is queryable. **Compare it against an independent
source**: a block explorer, or `cast logs` over the same range. Matching counts is the whole point;
this is the step that distinguishes "it ran" from "it is correct".

**1.4 Provenance is attached**

Expect every `/sql` response to carry a `provenance` block with `as_of`, `registry_hash` and
`sealed_through`. *Proves* answers are attributable to a specific decode version and a specific chain
position. `as_of` ahead of `sealed_through` means it is tip-following rather than serving only sealed
history.

---

## Level 2: correctness under adverse conditions

The levels above prove it works. These prove it stays right when things go wrong.

**2.1 Restart with no gaps or duplicates**

```sh
curl -s --get localhost:8288/sql --data-urlencode 'q=select count(*) c from "usdc__transfer"'
# SIGTERM the process, wait for it to exit, start it again with the same command
# re-run the same count once it has caught up to the same block
```

Expect the count for a **fixed block range** to be identical. *Proves* checkpointed resume. A higher
count means duplicates; a lower one means a gap. Either is a serious finding.

**2.2 Invariants and parity**

```sh
nuthatch check --dir /tmp/v-usdc
```

Expect a pass. *Proves* the nest's committed fixtures still decode to the same entities, the
regression net for decode changes.

**2.3 A reorg converges** *(needs a chain that reorgs, or trust CI)*

Our property tests drive random reorg depths against the hot store and assert convergence to canonical
state. Reproducing that live requires catching a real reorg; if you want to verify it yourself, watch
for `reorg` in the logs and confirm the affected block range's row count matches the canonical chain
afterwards. *Proves* reorgs only ever touch the mutable hot store.

**"Canonical" here means what the configured endpoint served**, and the section below says why that is
not the same as what consensus agreed.

---

## Level 3: a runtime hosting many nests

**3.1 Co-tenancy**

```sh
nuthatch dev --dir /tmp/v-runtime   # a dir with a mounts.toml runs every nest it mounts
curl -s localhost:8288/nests
```

Expect every mounted nest listed with its chain, registry hash and footprint (`estimated_rss_mb`), and
each serving its full API under `/<name>/`. *Proves* co-tenancy with per-nest routing.

**3.2 Nests on one chain share a cursor**

Expect one `getLogs` per window regardless of how many nests share a chain, visible in RPC-provider
metering, or in the logs. *Proves* the cost claim: N nests on a chain for roughly one nest's RPC spend.

**3.3 Multichain isolation**

With nests on two chains, expect two cursors, each with its own tip and finality. *Proves* a cursor is
never multiplexed across chains. Stalling one chain's RPC must not stall the other's ingestion.

**3.4 Mount and unmount without a restart**

```sh
curl -XPOST   localhost:8288/_admin/nests -H 'Content-Type: application/json' \
              -d '{"name":"another","nid":"<nid>"}'                  # 202 and the job
curl          localhost:8288/_admin/mounts/another                   # until "phase": "live"
curl -XDELETE localhost:8288/_admin/nests/another
```

The `nid` is what `nuthatch nest nid --dir <nest>` prints; a body without one resolves only a name
the runtime already has on record, and a fresh name without it ends `failed`. Expect the mount to
reach `live` and the unmount to succeed **without co-tenants being interrupted**: check the other
nests' `/ready` and row counts across the operation. *Proves* the live runtime: a
configuration change no longer has a wider blast radius than a fault.

Expect a mount that would breach the cursor's RAM budget to end `failed`, its reason carrying the
projected and ceiling figures; with `?wait=true` the same refusal is a `507`. That refusal is the
feature; a budget that can be quietly exceeded is not a budget.

**3.5 Per-nest blast radius**

Break one nest deliberately; an invalid authored view is easiest. Expect that nest to be quarantined
and reported, and **every other nest to keep serving**. *Proves* isolation. A runtime-wide failure here
is the most serious finding in this document.

**3.6 Cross-nest dataset adoption**

```sh
bash scripts/cross-nest-adoption.sh   # NUTHATCH=/path/to/binary optional; PORT and RPC_PORT too
```

*Scripted acceptance run.* Two independently-authored nests (different alias, description, and
`views/`) share the same contract, ABI, and start block, so their `data_identity()` matches and
their NIDs differ. Nest A indexes and seals a small fixture history; nest B is then migrated in
against an endpoint switched to return **zero logs for the whole range**. Expect:

- `migrate` prints `ADOPT` for B, not a fresh backfill.
- B's NID differs from A's (two mounts of one nest would not prove this).
- B's row count equals A's after mounting, even though its RPC endpoint cannot serve the history.
- Zero `eth_getLogs` calls into the historic range after B was mounted.

*Proves* that `runtime::adoptable` is general: it matches any nest whose decode inputs agree, not
only edits of the same lineage. This is the case GraphOps described: "if two entity hashes across
any nest are the same, you can reuse the data across."

---

## Level 4: the guards

Each of these is a deliberate refusal. **A missing refusal is the finding**, not a convenience.

**4.1 `/sql` is read-only**

```sh
curl -s --get localhost:8288/sql --data-urlencode 'q=CREATE TABLE x(a int)'
curl -s --get localhost:8288/sql --data-urlencode "q=SELECT 1; COPY (SELECT 1) TO '/tmp/x'"
```

Expect both refused, the second because `;`-stacked statements are rejected. *Proves* the surface is
structurally read-only. A stacked `COPY TO` succeeding is an arbitrary file write.

**4.2 Filesystem access is refused**

```sh
curl -s --get localhost:8288/sql --data-urlencode "q=SELECT * FROM read_csv('/etc/passwd')"
```

Expect a refusal. *Proves* the file-access denylist.

**4.3 The hot-scan cap**

Past 2,000,000 unsealed rows expect `503` naming `sealed_through`, rather than the box running out of
memory. *Proves* the largest RAM risk is bounded. It **refuses rather than truncating**, because a
partial tip would silently change the answer to an aggregate.

**4.4 The timestamp declaration cannot be flipped**

```sh
nuthatch init 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 --dir ts-nest
nuthatch dev --dir ts-nest --backfill 500       # let it index, then stop
sed -i 's/block_timestamps = true/block_timestamps = false/' ts-nest/nuthatch.toml
nuthatch dev --dir ts-nest                      # refused: the declaration needs `schema_version = 2`
sed -i 's/schema_version = 1/schema_version = 2/' ts-nest/nuthatch.toml
nuthatch dev --dir ts-nest                      # refused: a breaking schema change
```

Expect both `dev` commands to **refuse to start**: the first names the `schema_version` stamp the
declaration needs, the second, with the stamp set, names it a breaking schema change against the
data already indexed. *Proves* a nest cannot end up holding two schemas: rows and segments written
before the edit carrying `block_timestamp` and everything after not, with nothing to say so until a
query hits the wrong half.

Then the other direction, which is the one an operator actually wants:

```sh
nuthatch init 0xA0b8…eB48 --alias usdc --dir fast-nest --no-timestamps
grep -E 'schema_version|block_timestamps' fast-nest/nuthatch.toml
nuthatch dev --dir fast-nest --backfill 5000 --seal-direct
nuthatch sql --dir fast-nest 'SELECT * FROM usdc__transfer LIMIT 1'
```

Expect `schema_version = 2`, no `block_timestamp` column in the result, and a **visibly faster**
backfill than the same range with timestamps on. The v2 stamp is what makes a 0.8.x binary refuse this
nest rather than index timestamps into it; check that too if you have an old binary to hand.

**4.5 Admin exposure**

Bind off-localhost without `NUTHATCH_ADMIN_TOKEN` and expect the admin routes to be unavailable; with a
token, expect requests lacking it to be refused. *Proves* the admin surface is not reachable
unauthenticated off-localhost.

---

## Level 5: scaled mode (a fleet)

**This is the level we most want independently verified.**

> **2026-07-31 - a genuinely distributed fleet ran, and the clock-skew invariant PASSED.**
>
> Control plane + store + FE tier on one box (`10.44.1.1`), **two workers on their own machines**
> reaching the store over a private network. Both registered -
> `{"count":2,"workers":[{"id":"59e57b46be66"},{"id":"928d24d4d604"}]}` - and the scheduler assigned
> the `arbitrum-one` cursor to a **named remote worker**, which took the lease with a fence.
>
> **skew: PASS.** The holding worker's clock was pushed **10 minutes** forward on its own machine. It
> neither lost the lease nor extended it by ten minutes: `expires_at` advanced **66 s**, the renewal
> cadence on the *database's* clock. RFC-0022's claim - expiry is evaluated by the store, so a wrong
> clock on a worker can neither win nor lose it a cursor - is now demonstrated across machines rather
> than argued.
>
> **partition: BLOCKED, and the blocker is itself a finding.** It needs `last_block` to advance, and
> the assigned worker never indexed: it logs `acquired cursors chains=["arbitrum-one"]` and then
> `runtime secrets injected for held cursors nests=0`. **A declared nest plus an assigned cursor did
> not produce indexing in the distributed topology.** The lease machinery demonstrably works across
> machines; the *ingestion* half remains unproven, and that gap should be chased before anyone claims
> the writer pool works end to end.
>
> Getting this far took six tooling fixes, every one found by running it rather than reading it: a 422
> network payload (so `multi` had never created a box), a missing capacity fallback, a 409 on a
> leftover network, error handling that hid all three behind `curl: (56)`, inconsistent SSH host-key
> policy against recycled IPs, and a `psql` helper that never `cd`'d to its compose file.
>
> **2026-08-02 - `partition` PASSES. The last cross-machine invariant is proven.**
>
> ```
> last_block=490339521; cutting writer off the control-plane API for 90s
>   (Postgres stays reachable - a control-plane outage, not a store outage)
> PASS: indexed through the partition (490339521 -> 490339898)
> PASS: resumed after healing        (490339898 -> 490340039)
> ```
>
> A worker lost the control plane for 90 seconds and indexed **377 blocks** through it, then resumed
> reconciling cleanly. That is RFC-0022's central operational claim - the control plane *schedules*,
> the writer *indexes*, and losing the former must not stop the latter - demonstrated rather than
> argued. Note the cut targets the control-plane **port** only: cutting the whole host would take the
> store with it and test nothing (the mistake the first version of this check made).
>
> **2026-08-02 - RESOLVED, and measured.** A distributed fleet on the **published v0.9.3 artifacts**
> indexed for real: control plane and store on one box, workers on their own, and the cursor schema
> went from lease-keys-only to carrying `start_block`, `block_timestamps` and a climbing `last_block`
> (490,332,652 → 490,338,764). Those keys are written by `build_nest`/`index_loop` - code a worker
> never reached before. **The writer pool writes.**
>
> Two things worth keeping from the run. A cursor handover mid-test incremented `owner_fence` to 2 and
> moved `lease_owner`, so the fence was exercised by a real handover rather than a simulated one. And
> the first attempt stalled on `HTTP 429` from a free public endpoint - the batch narrowing correctly
> *refused* to split there, since halving under a rate limit doubles requests in the wrong direction.
>
> **5.11 initially failed for want of a precondition it never established**: it asserts a held cursor
> indexes, and no nest was declared, so there was nothing to index. The lab now declares one first. A
> check that asserts an outcome without arranging its cause is the same shape as the bug it was written
> to catch.
>
> **2026-07-31 - the writer pool does not write (issue #250), and it re-reads everything below.**
>
> `worker::run` registers, takes leases, loads secrets and reports. It contains **no indexing code** -
> no `index_loop`, no `build_nest`, no backfill path. A worker acquires a cursor and does nothing with
> it, so `last_block` never moves.
>
> **The 10/10 was true and meant less than it read.** Those checks are real and they passed: they
> exercise registration, planning, unplaceable reporting, lease fencing, fleet-wide version pinning
> and write-only secrets. **Not one asserts that a row appears.** That is the shape of the failure -
> a suite that verifies the machinery *around* a thing rather than the thing - and a level-5 check
> that asserts indexed data is part of the fix, or it recurs.
>
> The embedded path (`dev`) is unaffected; this is scaled mode only.

>
> **A first-run 5.1b failure should be re-run before it is reported.** The harness runs its level-5
> checks ~23 s after `compose up`, before workers have heartbeated, so worker-registration can fail
> once and pass immediately after. That is a readiness-gating flaw in the harness, not a product
> fault - and it is why the distributed path now waits for registration rather than sleeping and
> hoping.
> **2026-07-30 - the two cross-machine cases were repaired before ever being run.** `partition` and
> `skew` printed their expectations for a human to read and asserted nothing, which hid two defects in
> the tests themselves: `partition` blocked the whole control-plane *host*, and since Postgres runs on
> that box in the `multi` shape it cut the writer off from its **hot store** too - making its own
> stated expectation ("the cursor STILL INDEXING") impossible to satisfy. Both now assert against the
> shared Postgres and exit non-zero on failure. **Neither had been run at the time of writing**;
> both were run on 2026-08-02 (#255), which is when the table above moved.

Our automated tests run against a live Postgres in CI and cover every invariant below, and the compose
fleet has since been brought up end to end on real machines: `fleet-lab.sh up multi`, three Hetzner
boxes on a private network, installing the **published v0.9.3** artifacts (#255), then two machines
on the published 2.4.0 artifacts (2026-08-15, the table above). That run has not been repeated on
3.x or 4.x, so for the release you are holding this level rests on CI's compose job plus a
cross-machine run two majors old. If you verify one level from this document, this is still the one
worth your time.

Needs the **scaled** artifact: `…:<version>-scaled` or `nuthatch-scaled-…tar.gz`. The default build
refuses these commands by name.

**5.0 Prerequisites, both of which have bitten us**

```sh
nuthatch init 0xYourContract --chain arbitrum-one --dir nest
sudo chown -R 10001:10001 nest
```

The fleet mounts `./nest`. Without it, FE nodes exit and **writers keep running** - they take work from
the control plane, not from disk - so the failure looks like an FE bug rather than a missing directory.
Without the `chown`, FE nodes exit with `Permission denied`: the image runs unprivileged as uid 10001,
and a root-owned bind mount is unwritable to it.

That second one **passes on Docker Desktop and fails on Linux**, because Desktop fakes mount
permissions. If you are testing on a Mac and it works, that is not evidence it works.

**5.1 The stack comes up**

```sh
docker compose -f docker-compose.scaled.yml --profile fleet up \
  --scale writer=2 --scale fe=3
curl -s localhost:8290/health     # expect: ok
curl -s -H "authorization: Bearer $NUTHATCH_CONTROL_TOKEN" localhost:8290/workers    # expect: 2 workers with their budgets
```

Every control-plane route except `/health` needs that bearer header (the token the fleet was
started with); the commands below omit it for brevity.

*Proves* the topology: control plane reachable, workers registering, FE nodes up. We have run this on a
single host, on 2026-08-02 across three machines on published v0.9.3 artifacts (#255), and on
2026-08-15 across two machines on 2.4.0. **It has not been run on 3.x or 4.x**, so a report against
the release you are holding still helps most.

**5.2 Declaring a nest starts it, without a restart**

```sh
curl -XPOST localhost:8290/nests \
  -H 'Content-Type: application/json' -d '{"name":"usdc","chain":"arbitrum-one","estimated_rss_mb":120}'
curl -s localhost:8290/plan
```

Expect the declare to return `200` with a note that it is *desired state*, and the plan to assign the
chain to one worker. Within a tick (5s) expect that worker's logs to show `acquired cursors`.

*Proves* dynamic lifecycle. Note `200` means **told**, not **running**: the fleet converges on its own
tick. Conflating those is the most likely source of confusion at this level.

**5.3 Exactly one owner**

With two workers both able to host the chain, expect **exactly one** to log `acquired cursors` for it.
*Proves* the single-owner invariant. Two workers claiming one cursor is the most serious possible
finding in this document: it is the failure the lease and fence exist to prevent.

**5.4 Lease handover on writer loss**

```sh
docker compose -f docker-compose.scaled.yml kill <the owning writer>
```

Expect the other worker to acquire the cursor within a lease TTL (30s by default), and indexing to
continue. *Proves* failover. Then bring the killed worker back and expect it **not** to resume writing
the cursor it lost: its fence is stale, and the store refuses it.

**5.5 A stale writer is refused, not trusted**

Pause the owning writer long enough for its lease to expire (`docker pause`, then wait past the TTL),
let the other take over, then unpause it. Expect the resumed worker to log a **lost-ownership** error
and stop writing that cursor, rather than writing alongside the new owner.

*Proves* the fence. This is the invariant everything else rests on, and the one a comment in a config
file cannot deliver.

**5.6 Serving scales independently**

Expect every FE node to answer identically for the same query, and adding or removing FE nodes to
change no cursor's ownership and no ingestion progress. *Proves* the plane split.

**5.7 Versions resolve identically fleet-wide**

```sh
curl -XPUT localhost:8290/nests/usdc/pin -H 'Content-Type: application/json' -d '{"version":"1.0.0","bundle_hash":"<64 hex>"}'
# then, against each FE node:
curl -s localhost:8290/nests/usdc/resolve
```

Expect the same version from every node. *Proves* the fix for the bug that only exists across machines:
if each node resolved `latest` itself, one would serve the new schema while another served the old, and
the same endpoint would answer differently depending on where the load balancer sent the request.

An unpinned endpoint must report `servable: false`: an FE refusing is correct, guessing is not.

**5.8 Secrets stay out of bundles**

```sh
curl -XPUT localhost:8290/nests/usdc/secrets -H 'Content-Type: application/json' -d '{"key":"rpc_url","value":"<canary>"}'
curl -s localhost:8290/nests/usdc/secrets     # expect: key names only, never values
```

Then `grep` your canary through the nest directory, any generated bundle, and the sealed segments.
Expect **no match anywhere**. *Proves* secret isolation. Also confirm rotating the secret does not
change the nest's bundle hash; if it does, every rotation invalidates segment reuse.

**5.9 A control-plane outage does not stop ingestion**

```sh
docker compose -f docker-compose.scaled.yml stop postgres   # or just the control plane
```

Expect workers to log failed reconcile ticks and **keep indexing** cursors whose leases have not
expired. *Proves* the deliberate independence: a control-plane outage must stop *rescheduling*, not
*ingestion*. Workers exiting here would turn a database blip into a fleet-wide outage.

**5.10 Under-scheduling is loud**

Declare a nest whose cursor exceeds every worker's budget. Expect `GET /plan` to report it
`unplaceable` with reason `toolargeforanyworker`, and the workers to warn every tick. *Proves* a fleet
that cannot run what it was asked never looks healthy. Note the distinction from `noroomrightnow`:
adding a worker fixes the latter and never the former.

**5.12 A worker runs a nest it has never seen** *(needs a registry; the lab automates it)*

```sh
./scripts/fleet-lab.sh pull
```

Publish the nest to a registry, pin it (`PUT /nests/<name>/pin`), then **delete it from the writer
box** and restart the writer with `--registry`. Expect the writer to log `pulling from the registry`
and `last_block` to advance in the shared store.

*Proves* the half of scaled mode that RFC-0022 named and did not build: an operator declares a nest
centrally, and the machine that ends up holding that cursor may have nothing on disk. `desired_nest`
recorded a `version` and a `bundle_hash` from the beginning; until v1.0.0 nothing on the worker side
read them, which is why a worker could be assigned a nest it had no way to locate (issue #250).

Two details are the point rather than incidental:

- **Deleting the nest first.** A run that leaves it in place passes whether or not the pull works,
  because operator-placed nests win over the registry.
- **Pinning.** With a `bundle_hash` the fetch is by content address and the registry's mutable index
  is never consulted, so re-tagging `1.0.0` in the registry cannot change what a worker runs.
  Unpinned, a worker is exactly as trustworthy as its registry.

Pulled nests are cached at `<--nest-cache>/<name>/<hash>`, content-addressed so that re-pinning
resolves to a different directory and re-pulls, rather than silently reusing the bundle it already
has. Deleting that cache costs one download.

---

## Reporting

Please report **passes as well as failures**: a level someone ran is worth more than a level we
assert, and the table at the top should get shorter over time.

Useful to include: the level and step, expected versus actual, `nuthatch --version`, chain and
contract, and whether embedded or scaled. For level 5, how many writers and FE nodes.

Known-unverified items are tracked in [`docs/prod-readiness.md`](prod-readiness.md) §11; anything you
confirm there can move from 🟡 to ✅ with your evidence attached.
