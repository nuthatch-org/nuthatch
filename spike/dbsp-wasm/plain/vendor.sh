#!/usr/bin/env bash
# Rebuild ./vendor: the four crates whose wasm32 build reaches JavaScript, patched to reach the host
# instead (RFC-0077 S0). web-time's clock becomes `nuthatch.now_ms`; chrono's `Utc::now` reads
# web-time; uuid's and getrandom 0.2's `js` features select the custom randomness backend.
# The spike's own ../vendor (dbsp) comes from ../vendor.sh.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf vendor && mkdir vendor
for spec in web-time:web-time:1.1.0 chrono:chrono:0.4.45 uuid:uuid:1.29.0 getrandom:getrandom02:0.2.17; do
  IFS=: read -r crate dir version <<< "$spec"
  curl -fsSL "https://static.crates.io/crates/$crate/$crate-$version.crate" | tar -xz -C vendor
  mv "vendor/$crate-$version" "vendor/$dir"
  rm -f "vendor/$dir/.cargo-ok" "vendor/$dir/.cargo_vcs_info.json" "vendor/$dir/Cargo.lock"
done
patch -s -p0 < rfc-0077-plain.patch
