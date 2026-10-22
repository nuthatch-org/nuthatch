# The engine room, recorded (RFC-0069 S5)

![nuthatch watch on a defi-pulse nest following mainnet](engine-room.gif)

`nuthatch watch` on a fresh copy of `defi-pulse-nest`, recorded live on 2026-10-22 from 19:03:40 UTC
on an M-series MacBook with a release build. The nest ran `nuthatch dev --backfill 20000 --seal-direct
--concurrency 2 --poll-interval 12s` against `ethereum-rpc.publicnode.com` and `eth.drpc.org`; the
default poll interval is 300 s, so out of the box the tip moves in five-minute steps, not twelve-second
ones. `traffic.sh` sent it `/sql`, `/tables`, `/entities` and a missing `/table/{name}` a few times a
second for DISPATCH to show. Nothing is synthetic: the clip opens on the seal-direct pass (BACKFILL,
no tip read yet, the seal stamping forward), which took 59 s, and ends LIVE at the tip.

- `engine-room.gif`: the clip, 90 s at real speed, from `../engine-room.tape`.
- `engine-room.jsonl.gz`: every body the watcher read while it drew, `--record`'s format;
  `gunzip` it and `nuthatch watch --replay` draws it again.
- `engine-room.txt`: the text of each frame vhs kept, which `../layout-gate.sh` compares with a second
  recording against another fresh nest.
