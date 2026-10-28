# RFC-0073 FR1: a follower's snapshot and derived rebuild over a long sealed history

Measured 2026-10-28 on an Apple M5 Pro (18 cores, 48 GB), macOS, release build, in-process, one run
each. Harness: [`tests/bench_restart_mounts.rs`](../../tests/bench_restart_mounts.rs),
`follower_snapshot_and_derive_over_a_long_sealed_history`, `#[ignore]`d:

```
NUTHATCH_BENCH_BLOCKS=5000 cargo test --release --test bench_restart_mounts follower_snapshot -- --ignored --nocapture
```

One nest, 100 transfers a block among 200 accounts, a `received` entity summing each recipient's
transfers, sealed every 5 blocks so the history is many small segments. Two intervals:

- **snapshot**: the writer's store read into a snapshot, written as the feed line, read back and
  applied to a follower's in-memory copy. Little of the hot window remains once sealed.
- **derived**: `FollowerDerived::build` over that copy: the balance, exposure and velocity views and
  the entity seeded from every sealed segment plus the hot tail. This is what a follower pays on
  joining, after every writer restart, and on a rebuild after a torn feed or a `--seal-direct` seal.

| blocks | rows | segments | snapshot | derived |
| ---: | ---: | ---: | ---: | ---: |
| 500 | 50,000 | 50 | 7 ms | 100 ms |
| 5,000 | 500,000 | 500 | 23 ms | 861 ms |
| 20,000 | 2,000,000 | 2,000 | 42 ms | 3,794 ms |

The rebuild is roughly linear in sealed rows, about 1.9 µs a row, and dominates: the snapshot is
small because sealing has emptied hot. The 20,000-block run overlapped another build on the same
machine for part of its time, so it may read high.

## What this does not say

Real nests are wider (more tables, more entities, joins), and RFC-0042 found segment count dominating
at a realistic layout. On a nest like Lodestar's allocations nest, expect seconds to tens of seconds
per rebuild. During it the follower keeps serving what it had after a writer restart; after a torn
feed its derived routes answer `catching_up` until it finishes.
