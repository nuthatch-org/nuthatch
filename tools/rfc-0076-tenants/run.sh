#!/usr/bin/env bash
# RFC-0076 S1 bench: today's src/fair_share.rs against 5.13.0's, same workload, paused clock.
# Writes the table docs/bench/rfc-0076-tenants.md quotes.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$here/baseline"
git -C "$here" show 546b94a8:src/fair_share.rs > "$here/baseline/fair_share.rs"
cargo run --release --quiet --manifest-path "$here/Cargo.toml"
