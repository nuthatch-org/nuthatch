#!/usr/bin/env bash
# RFC-0077 S0: the plain module, built as the tab's is (`tab` profile, wasm-opt -O1) but with no
# wasm-bindgen. RUSTFLAGS replaces `../.cargo/config.toml`'s rather than adding to them, because that
# file selects getrandom's `wasm_js` backend and this build selects `custom`.
# Usage: build.sh <out.wasm> [cargo profile, default tab]
set -euo pipefail
cd "$(dirname "$0")"
profile=${2:-tab}
[ -d vendor/web-time ] || ./vendor.sh
export RUSTFLAGS='--cfg getrandom_backend="custom" -C link-arg=-zstack-size=8388608 -C link-arg=--max-memory=4294967296'
cargo build --locked --profile "$profile" --target wasm32-unknown-unknown
wasm-opt -O1 --strip-debug --strip-producers --enable-bulk-memory --enable-nontrapping-float-to-int \
  --enable-sign-ext --enable-mutable-globals --enable-reference-types --enable-multivalue \
  "target/wasm32-unknown-unknown/$profile/dbsp_wasm_plain.wasm" -o "$1"
