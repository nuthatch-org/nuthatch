# RFC-0073 FR7: a writer's restart to ready, by mount count

Measured 2026-10-27 on an Apple M5 Pro (18 cores, 48 GB), macOS, release build, in-process, medians
of 3 restarts. Harness: [`tests/bench_restart_mounts.rs`](../../tests/bench_restart_mounts.rs),
`#[ignore]`d and run explicitly:

```
cargo test --release --test bench_restart_mounts -- --ignored --nocapture
```

Each mount is its own nest and dataset on one cursor (one chain), indexed cold to block 5 from the
scripted tape, then stopped. Before each restart one block is added to the tape. Both clocks start
before `spawn_runtime`:

- **serving**: the composed runtime's `/ready` answers `200`. In `nuthatch dev` the listener binds
  only after every cursor is built, so this is when anything at all answers.
- **resumed**: every mount has indexed the block that arrived while it was down, so each cursor carried
  on from its checkpoint rather than from scratch.

## Result

| mounts | serving | resumed | process RSS after |
| ---: | ---: | ---: | ---: |
| 1 | 9 ms | 14 ms | 39 MB |
| 10 | 95 ms | 127 ms | 70 MB |
| 100 | 992 ms | 1,371 ms | 352 MB |
| 500 | 10,238 ms | 13,135 ms | 2,060 MB |

About 10 ms a mount up to 100, then 20.5 ms a mount at 500: the cost grows faster than the mount
count. Nothing here says why; it has not been profiled.

## What this does and does not say

**The upper bound FR7 asks for, for these nests on this machine**: a writer restarted with N small mounts
answers within N x 21 ms at N <= 500, and resumes every mount within N x 27 ms.

**It is not a bound for real nests.** Each mount here holds five blocks. The RFC-0042 restart
measurement (`rfc-0042-992-restart-to-ready.md`) found the constant dominating at this size and segment
count dominating at a realistic one; a mount's build also rebuilds its views from its sealed history,
which these nests barely have.

**500 small mounts do not fit one cursor's budget.** Admission charges each mount 5 MB plus its redb
file, and refuses a cursor projected over `max_rss_mb` (2,048) at boot, so `nuthatch dev` would not
start this runtime on one chain. The harness calls `spawn_runtime` directly and so skips admission;
the RSS column, the whole test process after three restarts, is consistent with that refusal rather
than a counter-example to it.

**During the restart, followers answer.** Readers following the writer (FR1) keep serving their last
state throughout, which `tests/e2e_follower.rs` and `scripts/rolling-upgrade.sh` check; this table is
how long they report the writer absent.
