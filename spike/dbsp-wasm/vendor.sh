#!/usr/bin/env bash
# Rebuild ./vendor: download the crates Cargo.lock pins, then apply the RFC-0075 patch.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf vendor && mkdir vendor
for c in dbsp feldera-types feldera-storage feldera-buffer-cache feldera-samply feldera-ir feldera-macros feldera-size-of; do
  v=$(awk -v n="name = \"$c\"" '$0 == n { getline; gsub(/version = |"/, ""); print; exit }' Cargo.lock)
  curl -fsSL "https://static.crates.io/crates/$c/$c-$v.crate" | tar -xz -C vendor
  mv "vendor/$c-$v" "vendor/$c"
done
patch -s -p0 < rfc-0075-dbsp-wasm32.patch
