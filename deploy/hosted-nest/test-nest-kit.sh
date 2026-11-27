#!/usr/bin/env bash
# Tests for new-nest.sh and remove-nest.sh against a fake NEST_ROOT (#3137). No host path, systemctl or network is touched.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
new=$here/new-nest.sh rm=$here/remove-nest.sh
ROOT=$(mktemp -d) && export NEST_ROOT=$ROOT
trap 'rm -rf "$ROOT"' EXIT
# new-nest.sh scrapes `ss`; where it is absent or shows no 81xx/84xx listener the port scan aborts the script. A stub keeps it hermetic.
mkdir -p "$ROOT/bin" && printf '#!/bin/sh\necho "LISTEN 0 0 127.0.0.1:8199 0.0.0.0:*"\n' > "$ROOT/bin/ss" && chmod +x "$ROOT/bin/ss"
export PATH=$ROOT/bin:$PATH
fail=0
ok()  { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }
check() { local what=$1; shift; if "$@"; then ok "$what"; else bad "$what"; fi; }

# ---- new-nest.sh: the git spec
pwn='https://example.invalid/x.git;touch${IFS}/tmp/pwned-by-spec;#'
for s in "$pwn" 'http://example.invalid/x.git' 'https://example.invalid/x.git rm -rf /' 'git@host:o/r.git$(id)' 'https://h/`id`' '-https://h/x'; do
  out=$("$new" demo base "$s" - 2>&1); rc=$?
  check "new-nest rejects spec: $s" test $rc -ne 0 -a -z "$(grep -F 'git clone' <<<"$out")"
done
out=$("$new" demo base 'https://github.com/o/r_1.git' - 2>&1)
check "new-nest clones a good https spec with --" grep -qF 'git clone -- https://github.com/o/r_1.git /var/lib/nuthatch-public/demo' <<<"$out"
out=$("$new" demo base 'git@github.com:o/r.git' - 2>&1)
check "new-nest accepts a git@ spec" grep -qF 'git clone -- git@github.com:o/r.git ' <<<"$out"

# ---- remove-nest.sh: shared units survive
sys=$ROOT/etc/systemd/system var=$ROOT/var/lib/nuthatch-public
mkdir -p "$sys" "$ROOT/etc/nuthatch-public" "$ROOT/etc/caddy" "$var/base-usdc"
for u in nuthatch-public-proxy nuthatch-public-base-usdc-proxy nuthatch-public-xdc-rpc-ua nuthatch-public-base-usdc-nest; do echo "[Unit]" > "$sys/$u.service"; done
echo "backfill" > "$var/base-usdc/data"
printf '# BEGIN nuthatch-public proxy\nsite {}\n# END nuthatch-public proxy\n' > "$ROOT/etc/caddy/Caddyfile"
for n in proxy base-usdc-proxy xdc-rpc-ua base-usdc-nest base-usdc; do
  "$rm" --apply --purge-data "$n" >/dev/null 2>&1; rc=$?
  check "remove-nest refuses $n" test $rc -ne 0
done
check "shared units still present" test -f "$sys/nuthatch-public-proxy.service" -a -f "$sys/nuthatch-public-base-usdc-proxy.service" -a -f "$sys/nuthatch-public-xdc-rpc-ua.service"
check "other nest's data still present" test -f "$var/base-usdc/data"

# a unit without the header and without a Caddy block is not ours, whatever its name
echo "[Unit]" > "$sys/nuthatch-public-stranger.service"; mkdir -p "$var/stranger"
"$rm" --apply --purge-data stranger >/dev/null 2>&1
check "remove-nest refuses a unit without the new-nest.sh header" test $? -ne 0 -a -f "$sys/nuthatch-public-stranger.service" -a -d "$var/stranger"
printf '# Made by deploy/hosted-nest/new-nest.sh\n' > "$sys/nuthatch-public-stranger.service"
"$rm" --apply --purge-data stranger >/dev/null 2>&1
check "remove-nest refuses a headed unit with no Caddy block" test $? -ne 0 -a -f "$sys/nuthatch-public-stranger.service" -a -d "$var/stranger"

# ---- a nest made by new-nest.sh is removed, and only it
"$new" --apply mine base 0x0000000000000000000000000000000000000001 1 >/dev/null 2>&1
mkdir -p "$var/mine"
"$rm" mine >/dev/null 2>&1
check "dry run removes nothing" test -f "$sys/nuthatch-public-mine.service" -a -d "$var/mine"
if sed --version >/dev/null 2>&1; then
  "$rm" --apply --purge-data mine >/dev/null 2>&1
  check "remove-nest removes its own nest" test ! -e "$sys/nuthatch-public-mine.service" -a ! -e "$var/mine" -a ! -e "$sys/nuthatch-public-mine-proxy.service"
  check "remove-nest left the others" test -f "$sys/nuthatch-public-proxy.service" -a -f "$var/base-usdc/data"
else
  echo "skip remove-nest --apply success path: BSD sed has no sed -i without a suffix (the script targets the Linux host)"
fi
exit $fail
