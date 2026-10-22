#!/usr/bin/env bash
# Builds S1's driver for a tab into web/pkg: the engine the player page runs in a worker.
#   CLANG=/opt/homebrew/opt/llvm/bin  a clang with the WebAssembly backend (zstd-sys)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
clang_dir=${CLANG:-/opt/homebrew/opt/llvm/bin}
[ -x "$clang_dir/clang" ] || { echo "no clang at $clang_dir: Apple's has no wasm32 backend; brew install llvm" >&2; exit 1; }
export CC_wasm32_unknown_unknown=$clang_dir/clang AR_wasm32_unknown_unknown=$clang_dir/llvm-ar
cd "$here/../rfc-0070-s1"
# burrmill-wasm's release profile, given here so S1's native timings keep the profile they were taken with.
cargo build --release --lib --target wasm32-unknown-unknown \
  --config profile.release.lto=true --config profile.release.codegen-units=1 --config 'profile.release.opt-level="s"'
wasm=target/wasm32-unknown-unknown/release/rfc_0070_s1.wasm
wasm-bindgen "$wasm" --out-dir "$here/web/pkg" --target web
wasm-opt -Os --strip-debug --enable-bulk-memory --enable-nontrapping-float-to-int --enable-sign-ext \
  --enable-mutable-globals "$here/web/pkg/rfc_0070_s1_bg.wasm" -o "$here/web/pkg/rfc_0070_s1_bg.wasm"
echo "wasm: $(wc -c < "$here/web/pkg/rfc_0070_s1_bg.wasm") bytes"
