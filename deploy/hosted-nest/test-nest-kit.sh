#!/usr/bin/env bash
# Tests for new-nest.sh and remove-nest.sh against a fake root (#3137, #3205).
# No host path, systemctl or network is touched.
set -uo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
new=$here/new-nest.sh
rm=$here/remove-nest.sh
fail=0
ok()  { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }
check() { local what=$1; shift; if "$@"; then ok "$what"; else bad "$what"; fi; }

# ---- #3137: git spec and remove-nest only touches nests new-nest made
ROOT=$(mktemp -d) && export NEST_ROOT=$ROOT
# new-nest.sh scrapes `ss`; where it is absent or shows no 81xx/84xx listener the port scan aborts.
mkdir -p "$ROOT/bin" && printf '#!/bin/sh\necho "LISTEN 0 0 127.0.0.1:8199 0.0.0.0:*"\n' > "$ROOT/bin/ss" && chmod +x "$ROOT/bin/ss"
export PATH=$ROOT/bin:$PATH
cleanup_3137() { rm -rf "$ROOT"; }
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
sys=$ROOT/etc/systemd/system
var=$ROOT/var/lib/nuthatch-public
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
cleanup_3137
unset NEST_ROOT

# ---- #3205: flags / --env-file work under env -i (what sudo's reset leaves)
work=$(mktemp -d)
cleanup_3205() { rm -rf "$work"; }
check_eval() { if eval "$2"; then ok "$1"; else bad "$1"; fi; }
addr=0x$(printf 'a%.0s' $(seq 40))
unit=nuthatch-public-acme-vault.service
# Keep the ss stub on PATH so --apply does not abort.
r1=$work/r1
env -i PATH="$PATH" "$here/new-nest.sh" --apply --root "$r1" --host-ip 192.0.2.7 --local-host 100.64.0.9 \
  acme-vault base "$addr" 100 >"$work/o1" 2>&1 || { cat "$work/o1"; cleanup_3205; exit 1; }
u=$r1/etc/systemd/system/$unit
check_eval "flags: unit carries --local-host" "grep -q -- '--local-host 100.64.0.9' $u"
check_eval "flags: files written under --root" "[ -f $u ] && [ -f $r1/etc/nuthatch-public/acme-vault.env ]"
check_eval "flags: --host-ip used" "grep -rq '192.0.2.7' $r1/etc"
check_eval "flags: no default IP leaked" "! grep -rq '89.167.109.4' $r1/etc"

r2=$work/r2
printf '# test\nNEST_ROOT=%s\nHOST_IP=192.0.2.8\nNEST_LOCAL_HOST=100.64.0.10\n' "$r2" > "$work/nest.conf"
env -i PATH="$PATH" "$here/new-nest.sh" --apply --env-file "$work/nest.conf" acme-vault base "$addr" 100 >"$work/o2" 2>&1 \
  || { cat "$work/o2"; cleanup_3205; exit 1; }
check_eval "env-file: --local-host from file" "grep -q -- '--local-host 100.64.0.10' $r2/etc/systemd/system/$unit"
check_eval "env-file: --host-ip from file" "grep -rq '192.0.2.8' $r2/etc"

r3=$work/r3
env -i PATH="$PATH" "$here/new-nest.sh" --apply --env-file "$work/nest.conf" --root "$r3" acme-vault base "$addr" 100 >/dev/null 2>&1
check_eval "flag wins over file" "[ -f $r3/etc/systemd/system/$unit ] && grep -rq '192.0.2.8' $r3/etc"
printf 'BOGUS=1\n' > "$work/bad.conf"
check_eval "unknown env-file key refused" "! env -i PATH=$PATH $here/new-nest.sh --env-file $work/bad.conf acme-vault base $addr 100 >/dev/null 2>&1"

r4=$work/r4
env -i PATH="$PATH" "$here/new-nest.sh" --root "$r4" acme-vault base "$addr" 100 >/dev/null 2>&1
check_eval "dry run writes nothing" "[ ! -e $r4/etc ]"

env -i PATH="$PATH" "$here/remove-nest.sh" --apply --root "$r1" acme-vault >"$work/o5" 2>&1 || { cat "$work/o5"; cleanup_3205; exit 1; }
check_eval "remove --root deletes the unit" "[ ! -e $u ]"
cleanup_3205

[ $fail = 0 ] && echo "all ok"
exit $fail
