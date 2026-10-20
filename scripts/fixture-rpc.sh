#!/usr/bin/env bash
# Prints the path of a built tools/fixture-rpc binary, building it first unless FIXTURE_RPC already
# names one (CI downloads it beside the nuthatch binary). Build output goes to stderr.
#
#   RPC_BIN="$(bash scripts/fixture-rpc.sh)"; "$RPC_BIN" footprint 8545 &
set -euo pipefail

if [ -n "${FIXTURE_RPC:-}" ]; then
  [ -x "$FIXTURE_RPC" ] || { echo "FAIL: FIXTURE_RPC=$FIXTURE_RPC is not executable" >&2; exit 1; }
  echo "$(cd "$(dirname "$FIXTURE_RPC")" && pwd)/$(basename "$FIXTURE_RPC")"
  exit 0
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cargo build --release --locked --quiet --manifest-path "$root/tools/fixture-rpc/Cargo.toml" \
  --target-dir "$root/target" >&2
echo "$root/target/release/fixture-rpc"
