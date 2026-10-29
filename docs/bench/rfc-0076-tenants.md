# RFC-0076 S1: a thousand tenants on one chain's SQL gate

Measured 2026-10-29 on the MacBook (Apple M5 Pro, 18 cores, 48 GB, macOS 26.5.1, load 3.4 at the start of the run from other sessions), branch
`pete/2488-rfc-0076-s1-tenants` off `546b94a8` (v5.13.0). The gate measured is RFC-0076 §3.1's
admission ledger, `src/fair_share.rs`; the baseline is the same file at `546b94a8`. Reproduce with
`tools/rfc-0076-tenants/run.sh`.

## What is measured, and what is not

The harness compiles both ledgers into one binary beside a three-item shim (`TenantShare`, the
refusal metric's enum, the hot-copy budget) and drives each with the same workload on tokio's paused
clock. 1,000 tenants are mounted on the gate, one seat each, as 1,000 `tenant/alias` mounts on one
chain seat them. Every client asks for a permit the way `/sql` does (`try_admit`, then `wait_admit`
for up to 250 ms), holds it for a service time drawn from an exponential distribution with a 20 ms
mean, gives it back, and asks again after its think time. Each client is seeded, so a run is the same
every time. 120 virtual seconds per run.

This measures **admission alone**. A statement's service time is a sleep, not Burrmill: the engine's
own concurrency is RFC-0042's subject and #1006's, and nothing here changes it. The wall-clock
column is what it cost one thread to run the ledger through 120 s of that load, so it is a measure
of the ledger's own CPU and of nothing else.

Two profiles, each run at three permit counts:

- **closed**: the RFC's gate as written. 200 tenants query at once in a closed loop, one client each,
  no think time. Every one of them holds no permit when it asks, so every one is under its share.
- **mixed**: the case the reserve is for. 8 heavy tenants with 16 clients each and no think time,
  which borrow whatever the gate lets them, beside 192 light tenants with one client each and a 1 s
  mean think time, which become active one query at a time and are always under their share.

## Results

`wait` is from asking to admission or refusal, refusals included at 250 ms. `permits busy` is the
mean number of permits running statements. Light tenants are the tenants under their share that §3.1
asks about.

| profile | ledger | class | tenants x clients | wait p50 ms | wait p99 ms | refused | admitted/s | permits busy | wall s |
|---|---|---|---|---|---|---|---|---|---|
| closed-2 | 5.13.0 | uniform | 200 x 1 | 250.00 | 250.00 | 87.7% | 98 | 1.96 | 67.7 |
| closed-2 | S1 | uniform | 200 x 1 | 250.00 | 250.00 | 87.7% | 98 | 1.96 | 4.4 |
| closed-16 | 5.13.0 | uniform | 200 x 1 | 232.00 | 250.00 | 2.3% | 782 | 15.64 | 117.3 |
| closed-16 | S1 | uniform | 200 x 1 | 232.00 | 250.00 | 2.3% | 782 | 15.64 | 7.6 |
| mixed-2 | 5.13.0 | heavy | 8 x 16 | 250.00 | 250.00 | 87.0% | 66 | 1.31 | 33.4 |
| mixed-2 | 5.13.0 | light | 192 x 1 | 250.00 | 250.00 | 78.6% | 33 | 0.64 | 33.4 |
| mixed-2 | S1 | heavy | 8 x 16 | 250.00 | 250.00 | 87.0% | 66 | 1.31 | 2.6 |
| mixed-2 | S1 | light | 192 x 1 | 250.00 | 250.00 | 78.6% | 33 | 0.64 | 2.6 |
| mixed-16 | 5.13.0 | heavy | 8 x 16 | 232.00 | 250.00 | 27.1% | 392 | 7.82 | 34.1 |
| mixed-16 | 5.13.0 | light | 192 x 1 | 0.00 | 8.00 | 0.0% | 188 | 3.75 | 34.1 |
| mixed-16 | S1 | heavy | 8 x 16 | 213.00 | 250.00 | 7.0% | 521 | 10.40 | 3.6 |
| mixed-16 | S1 | light | 192 x 1 | 0.00 | 10.00 | 0.0% | 188 | 3.75 | 3.6 |
| mixed-64 | 5.13.0 | heavy | 8 x 16 | 232.00 | 250.00 | 27.1% | 392 | 7.82 | 34.5 |
| mixed-64 | 5.13.0 | light | 192 x 1 | 0.00 | 0.00 | 0.0% | 188 | 3.76 | 34.5 |
| mixed-64 | S1 | heavy | 8 x 16 | 29.00 | 52.00 | 0.0% | 2549 | 50.90 | 6.4 |
| mixed-64 | S1 | light | 192 x 1 | 0.00 | 0.00 | 0.0% | 188 | 3.76 | 6.4 |


## Reading it

**The closed loop is overload, and no reservation scheme changes overload.** 200 clients each wanting
a 20 ms statement against 16 permits is 200 permits of demand on 16, so a client waits about
(200 - 16) / 16 x 20 ms = 230 ms, which is what both ledgers measure (p50 232 ms), and at 2 permits
88% are refused. Both ledgers give the same answer to the millisecond because, with one client per
tenant, nobody ever holds a second permit: 5.13.0's one-per-mount reservation and S1's reserve both
reduce to "any free permit, to whoever holds none, oldest first". A tenant under its share gets a
permit within the wait exactly when the gate has capacity for it.

**The mixed profile is where they differ, and S1 does what §3.1 says.** Under 5.13.0, 1,000 mounted
tenants are more than any permit count, so every tenant may hold one permit and no more, whatever is
idle: the eight heavy tenants run 8 statements at 16 permits and at 64 alike, and a quarter of their
queries are refused while half the gate or more sits empty. Under S1 only the active tenants count,
so the heavy tenants borrow everything but the reserve: 10.4 permits busy at 16 (from 7.8) and 50.9
at 64 (from 7.8), with refusals down from 27% to 7% and to none. The light tenants, the ones under
their share, are admitted at once at p50 at 16 and 64 permits under both. At 16 their p99 is 10 ms against
5.13.0's 8 ms: the reserve is two permits, the light tenants average 3.75 permits of demand, and the
ones past the reserve wait for a borrower's statement to end. That is the bound §3.1 states, measured:
**a newly active tenant is admitted at once when fewer tenants arrive together than the reserve
holds, and otherwise waits for a running statement to finish**, here at most 10 ms at p99, because a
borrower's statement ends long before 250 ms. A burst of arrivals larger than the reserve with
statements that do not end inside the wait would be refused, and nothing in S1 prevents that.
`fair_share::tests::a_burst_larger_than_the_reserve_waits` states it as a test.

**At the default count, nothing changes.** Two permits and a reserve of one behave as 5.13.0's two
reservations did for two or more tenants: a tenant holding a permit never takes the second while
another mounted tenant holds none, and the mixed profile at 2 permits gives identical figures.

**The ledger's own cost.** 5.13.0's ledger counted seated tenants on every admission decision, and
ordered waiters by scanning every tenant, so with 1,000 mounted and 200 waiting each decision was
quadratic in the mounts. Running it through 120 s of the closed loop at 16 permits took 117.3 s of one core;
S1's tallies are kept as tenants change and ordering looks only at tenants with a waiter, and the
same run took 7.6 s. On a live gate, the old cost is CPU on the executor and time under the
ledger's lock.

## The state machine

A tenant's place on one gate, as `src/fair_share.rs` keeps it:

```
            mount (Seat::new)                        a query asks
  absent ───────────────────► idle ─────────────────────────────────┐
     ▲                         │ ▲                                    ▼
     │  last seat dropped,     │ │ last permit back,             waiting (queued)
     │  nothing held           │ │ nothing queued                     │ admitted: free > kept_from
     └─────────────────────────┘ │                                    ▼
                                 └────────────────────────────── holding (active)
                                                                     │ ▲ borrows one more only
                                                                     └─┘ while free > reserve,
                                                                         if any mounted tenant
                                                                         holds none
```

- **idle**: mounted, holding nothing, nothing queued. Counted in `idle_seated`, the tenants the
  reserve is kept for; contributes no weight to anyone's share.
- **waiting** and **holding** are *active*: counted in `active` and `active_weight`, which divide the
  gate by weight for the refusal reason and the queue's order.
- **Admission**: a tenant holding no permit may take any free one; one holding a permit must leave
  `reserve = max(1, permits / 8)` free while another mounted tenant holds none. A tenant at
  `max_permits` is refused whatever is free.
- **Order**: among waiters that may take, the fewest permits held per unit of weight first, then the
  oldest ticket.
- **Refusal reason**: `tenant_share` when capped, or when the tenant already holds a permit and either
  permits are free but reserved or it is at its fair share among the active tenants; `busy` when it
  holds none and the gate is full. A runtime with one tenant is only ever told `busy`.
- **A lowered `sql_permits`** takes free permits at once and the rest as permits come back, so held
  permits can exceed the new total until their statements end; none is preempted.

## Cold sessions

§3.1 asks for the cost of a cold session before the strict session cache is sized from the budget.
`analytics::confinement::measure_cold_session_cost` (ignored; `cargo test --release --lib
measure_cold_session -- --ignored --nocapture`) opens 200 strict tenants' sessions over one nest of
8 tables of 10 segments with a view each, then queries each again from the cache:

| | release | debug |
|---|---|---|
| first query, cold session | 3.43 ms | 14.23 ms |
| same query, cached session | 2.56 ms | 12.10 ms |
| resident per idle cached session | 0.46 MiB | 0.68 MiB |
| threads per cached session | 6.6 | 6.0 |

A cold session costs 0.9 ms here: cheap against a statement, so a thousand tenants missing a cache of
16 lose little latency. What a cached session costs is the bound, not the idle figure: up to an
eighth of the engine limit in Parquet footers (64 MB at 512 MB) and its own threads. So the cache is
sized at one more session per 65 MB the split leaves over, from a floor of 16 (the 5.13.0 size, and
what the default leaves) to 256, about 1,700 idle threads. The fixture's footers are small, so the
idle figure is a floor for a real nest, not an estimate of one.

## Not measured

- Real statements under the gate. The service time is a sleep; a slow statement holds its permit
  longer and moves every wait up with it, in both ledgers alike.
- GraphOps's activity profile. RFC-0076 §7.3 leaves the reserve's size open until it exists; the
  mixed profile is ours.
- More than one gate. Each chain's cursor has its own; nothing here crosses them.
