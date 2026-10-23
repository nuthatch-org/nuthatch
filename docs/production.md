# Running this unattended

**What this is:** the order to do things in, from a fresh box to something you stop watching.
**What it is not:** a reference. [`operators.md`](operators.md) is the reference and it is thorough;
this page exists because it is thorough, and a first-time operator needs a path through it rather
than a second copy of it. Every step below links out rather than restating, deliberately - a fact
written in two places is a fact that will disagree with itself in a month.

The test for "unattended" is not that it runs. It is that **nothing needs you until something is
actually wrong, and when something is wrong you find out from a page rather than from a user.**

---

## The path

### 1. Get it running at all

Install, `init` a nest, `dev` it, query it. [Quickstart in the README](../README.md). Do this on a
laptop first - the point is to know what healthy looks like before you automate around it.

### 2. Decide the deployment shape

One nest or several, one chain or several, single box or a fleet.
[Deployment model](operators.md#deployment-model). Most people are one box with one or more nests on
one chain, and should stay there: scaled mode exists for when a single cursor cannot keep up, not as
the default.

### 3. Close the network down before anything else

Bind to localhost or an internal interface, put TLS and auth in front, set `NUTHATCH_ADMIN_TOKEN` or
pass `--no-admin`, run as an unprivileged user with the nest directory `0700`.
[Security posture](operators.md#security-posture). Do this now rather than after it works, because
"after it works" is how a debug binding survives into production.

### 4. Size it from measurement, not from the projection

`nuthatch bench backfill` and `nuthatch bench query` on **your** hardware and **your** RPC.
[Capacity and sizing](operators.md#capacity-and-sizing). Set `MemoryMax` to the cursor budget so a
runaway is bounded by the unit file rather than by the OOM killer's judgement.

### 5. Give it two endpoints, and check they are the right chain

Every `rpc_urls` pool wants at least two entries. `nuthatch doctor --rpc <url>` before you trust one
with a backfill - it reports the real `eth_getLogs` width, batch ceiling and archive depth, each of
which otherwise shows up mid-backfill as something that merely looks like slowness.

### 6. Make it survive a restart, then prove it

A systemd unit or a compose file: [deploy recipes](operators.md#deploy-recipes). Then do the drill -
SIGTERM, restart, confirm no gaps and no duplicates. A restart policy you have not exercised is a
hypothesis.

### 7. Wire observability before you stop watching

Prometheus on `/metrics`, alerts for quarantine, cursor death, tip lag, ingest stall and memory:
[what to alert on](operators.md#what-to-alert-on). Load balancers route on per-nest `/<name>/ready`;
paging goes on the root `/ready`. The distinction matters:
[health versus readiness](operators.md#health-versus-readiness).

If you want deliveries out of the nest rather than scrapes into it, that is
[`examples/webhooks/`](../examples/webhooks/), including a receiver you can run.

### 8. Back it up, and restore it once

Back up the nest directory. Then **restore it into a clean box**, because a backup nobody has
restored is a belief. [Data lifecycle](operators.md#data-lifecycle).

### 9. Rehearse the upgrade you will eventually do at 2am

Swap the binary and restart against a non-production copy of the data directory, which is how a
5.x upgrade goes and the copy is the only way back; `nuthatch migrate --dry-run` is for a pre-2.0
directory only: [nest lifecycle operations](operators.md#nest-lifecycle-operations). Each release's notes carry a
**Compatibility and upgrade** section saying whether it is replace-the-binary-and-restart or needs a
migration or re-index; that section is the one thing to read before upgrading, and what it promises
is bounded by the [stability contract](operators.md#stability-contract).

### 10. Prove it, with someone else watching

[`verification.md`](verification.md) is falsifiable step by step - command, expected result, what it
proves, what a failure means. Walk it at the levels matching your deployment. Hand it to a second
operator if you have one: a claim someone else confirmed is worth more than one you assert.

Then run the [go-live checklist](operators.md#go-live-checklist) as the final gate. It records
decisions; the runbook produces evidence; you want both.

---

## What actually runs without you

| Runs itself | Needs a human, on a schedule | Should page you |
|---|---|---|
| Tip following, sealing, reorg handling | Upgrades (read in-place-safe vs reseal) | Cursor death, ingest stall |
| Quarantine of a failed nest or cursor | Backup restore drills | Nest quarantined |
| Webhook retry via the durable outbox | Re-running `bench` after a hardware or RPC change | Tip lag past your threshold |
| Adding a nest live via `POST /_admin/nests` | Reviewing `nuthatch check` parity fixtures | Memory near the cursor budget |
| | | Alert outbox depth rising |

The middle column is the one people skip. Nothing in it fails loudly, which is exactly why it needs a
calendar rather than a monitor.

## Two honest gaps

**Scaled mode is the least-verified path.** `verification.md` says which levels have been verified by
the maintainers and on which release. CI brings the compose stack up on every change; the last
multi-machine run was on 2.4.0, and it has not been repeated on a later major. If you are running scaled mode, you are
ahead of the verification, and outside evidence is worth sending back.

**The macOS binary has no CI tests.** `aarch64-apple-darwin` is compiled and published, and it has
been run by hand: [verification.md](verification.md) records a live index, restart and `check` on
4.10.1. No CI job runs on macOS, though; see [prod-readiness §8](prod-readiness.md#8-release-engineering). Fine for a
laptop; think twice before it is the thing you leave running.
