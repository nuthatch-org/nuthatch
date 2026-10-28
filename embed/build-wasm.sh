#!/bin/bash
# Build the embedded module reproducibly: the same commit and toolchain give the same bytes on any
# machine, because no local path is left in it.
#
#   embed/build-wasm.sh [target-dir]
#
# Prints the module's path and sha256.
set -euo pipefail

EMBED=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$EMBED/.." && pwd)
TARGET=${1:-$EMBED/target}
CARGO_HOME=${CARGO_HOME:-$HOME/.cargo}
RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup}

export RUSTFLAGS="--remap-path-prefix=$CARGO_HOME=/cargo --remap-path-prefix=$RUSTUP_HOME=/rustup --remap-path-prefix=$REPO=/nuthatch"
cargo build --release --locked --target wasm32-unknown-unknown \
  --manifest-path "$EMBED/Cargo.toml" --target-dir "$TARGET"

WASM=$TARGET/wasm32-unknown-unknown/release/nuthatch_embed.wasm
if strings "$WASM" | grep -q -e "$HOME" -e "$REPO"; then
  echo "FAIL: $WASM still names a local path" >&2
  exit 1
fi
echo "$WASM"
shasum -a 256 "$WASM" | cut -d' ' -f1
