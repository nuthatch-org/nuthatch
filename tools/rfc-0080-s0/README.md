# RFC-0080 S0's cost gate, against the shipped `/sync`

The harness behind RFC-0080 §20. It measures what a follower's `/sync` subscribers cost its apply, on
the fixture and against the denominator §11 and §12 used, so a redesign can be held to the same gate.

It needs a binary built with the server half:

```sh
cargo build --release --features sync-server
BIN=target/release/nuthatch tools/rfc-0080-s0/run.sh uniform 10000 300   # one run
BIN=target/release/nuthatch tools/rfc-0080-s0/batch.sh 3                  # a control, then each mix three times
```

- `run.sh <mix> [shapes] [deltas]`: `fixture-rpc multinest` (the point-read gate's chain), a writer
  runtime with `[mounts.sync]` on its one mount and its feed published, and a follower
  (`serve --runtime --follow`). `client.mjs` holds one live stream per shape. Once every stream has its
  first answer and the follower is at the writer's tip, the window of `deltas` deltas opens. It prints
  one `result` line: the mean `/sync` part of a delta (`nuthatch_follower_sync_seconds`), the rest of
  the apply (`nuthatch_follower_apply_seconds` less that), and the share, which the gate holds under 10%.
- `shapes.mjs <n> <mix>`: §12's shape sets (`uniform`, `hot:<k>`, `all-tables`), seeded, from the
  fixture's own derivations.
- `client.mjs`: Node, one `GET /<route>/sync?live=1` a shape, at most 64 connecting at once, a `503`
  asked again.

Node and bash only, as the rest of the tree. macOS needs `ulimit -n` above 10,000, which the script
raises.
