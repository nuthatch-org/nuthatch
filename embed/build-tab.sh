#!/usr/bin/env bash
# RFC-0075 S6: the module a tab loads, as the gates run it. `tab` profile, names stripped, then
# wasm-opt -O1: -Oz and -Os cut the raw bytes further and grow the brotli ones, which a page pays.
# Usage: build-tab.sh <nodejs|web> <out-dir>
set -euo pipefail
cd "$(dirname "$0")"
cargo build --locked --profile tab --target wasm32-unknown-unknown
wasm-bindgen --target "$1" --remove-name-section --remove-producers-section --out-dir "$2" \
  target/wasm32-unknown-unknown/tab/nuthatch_embed.wasm
# rustc's default wasm32 features, which the module already uses.
wasm-opt -O1 --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext \
  --enable-mutable-globals --enable-reference-types --enable-multivalue \
  "$2/nuthatch_embed_bg.wasm" -o "$2/nuthatch_embed_bg.wasm"
node size.mjs "$2/nuthatch_embed_bg.wasm"
