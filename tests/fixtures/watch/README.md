# `nuthatch watch` fixtures (RFC-0069 S2)

Bodies a live nest served, recorded 2026-10-22 by `nuthatch watch --interval 1 --record` (5.9.0 plus
the S1 line mode) from a MacBook:

- `solo.jsonl`: a copy of `defi-pulse-nest` run as `dev --backfill 3000 --seal-direct --concurrency 2
  --poll-interval 12s` against `ethereum-rpc.publicnode.com` and `eth.drpc.org`.
- `runtime.jsonl`: a two-chain runtime, the same nest mounted as `pulse` on mainnet beside USDC's
  `Mint` and `Burn` on Base as `baseusdc`, `--backfill 2000 --poll-interval 12s`, public RPCs.
- `dispatch.jsonl` (S3): the same solo nest on the S3 build, recorded by the screen itself at its
  defaults while a script sent it `/sql`, `/tables`, `/entities` and a missing `/table/{name}`.

Each is the last 45 s of a 90 s recording keeping every sixth `/metrics` read (every third in `dispatch.jsonl`), as a
watcher reading it every 6 s would have; no body was edited. `snapshots/` is what `src/watch/tests.rs` draws
from them; `NUTHATCH_WATCH_SNAPSHOTS=update cargo test --lib watch::` rewrites it.
