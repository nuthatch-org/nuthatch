#!/usr/bin/env bash
# RFC-0080 §20's batch: a no-subscriber control, then each mix three times, interleaved so the
# machine's load falls on every mix alike. One `result` line per run.
#
#   BIN=target/release/nuthatch tools/rfc-0080-s0/batch.sh [runs]
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
RUNS=${1:-3}
bash "$HERE/run.sh" uniform 0 100 2>&1 | grep -E '^result|never|died|did not|values:'
for _ in $(seq 1 "$RUNS"); do
  for mix in uniform hot:100 all-tables; do
    bash "$HERE/run.sh" "$mix" 10000 300 2>&1 | grep -E '^result|never|died|did not|values:'
  done
done
echo BATCH-DONE
