#!/usr/bin/env bash
# Swaps a production nest on Helsinki onto a re-index staged beside it, and back: the directory swap
# of deploy/swap-allocations-nest-l1.sh (#1882) with the units, directories, ports and the nest's own
# invariant read from a config file (deploy/swaps/<name>.conf).
#
#   deploy/swap-nest-dir.sh <conf> check               the prechecks alone; nothing stops
#   deploy/swap-nest-dir.sh <conf> swap
#   deploy/swap-nest-dir.sh <conf> rollback <stamp>    <stamp> is what `swap` printed
#
# No unit file is edited. `swap` stops both units, moves the production directory to
# <dir>.<OLD_SUFFIX>-<stamp> and the staged one into its place, and starts production on its own
# unit: every path, port, flag, drop-in and publish target stays as it was. Downtime is one stop and
# start of the production port.
#
# Everything the swap reads is checked before anything stops: both units active, both directories on
# one filesystem, the same binary, the staged nest within MAX_GAP blocks of production with no failed
# views, the INVARIANT answered as INVARIANT_WANT on the staged port and not on production's (so the
# two can be told apart afterwards, since a re-index of one definition keeps its NID), and the smoke.
# After the start it requires /ready, the staged NID, the invariant and the smoke on the production
# port; any failure puts the old directory back, starts it, and checks it came back. The box side
# runs as a transient systemd unit, so a dropped ssh cannot leave it half done.
#
# Config (sourced; see deploy/swaps/data-services-nest-fresh.conf):
#   PROD_UNIT STAGED_UNIT  the systemd units
#   PROD_DIR STAGED_DIR    their --dir, under one filesystem
#   PROD_PORT STAGED_PORT  their --listen ports on 127.0.0.1
#   OLD_SUFFIX             the old directory becomes $PROD_DIR.$OLD_SUFFIX-<stamp>
#   SMOKE                  the unit's smoke file, one statement per line (private kittiwake repo)
#   INVARIANT INVARIANT_WANT  one /sql statement whose one-column, one-row answer is INVARIANT_WANT
#                          on the re-index and something else on the directory it replaces
#   MAX_GAP                blocks the staged nest may be from production (default 2000)
#   SWAP_HOST SWAP_KEY     the box (default root@89.167.109.4, ~/.ssh/hetzner_drpc)
set -euo pipefail

conf=${1:-}
mode=${2:-}
[ -n "$conf" ] && [ -f "$conf" ] || { echo "usage: $0 <conf> check | swap | rollback <stamp>" >&2; exit 2; }
case "$mode" in
  swap|check) [ $# -eq 2 ] || { echo "usage: $0 <conf> $mode" >&2; exit 2; }
    stamp=$(date -u +%Y%m%dT%H%M%SZ) ;;
  rollback) [ $# -eq 3 ] && [[ $3 =~ ^[0-9]{8}T[0-9]{6}Z$ ]] || { echo "usage: $0 <conf> rollback <stamp>" >&2; exit 2; }
    stamp=$3 ;;
  *) echo "usage: $0 <conf> check | swap | rollback <stamp>" >&2; exit 2 ;;
esac
MAX_GAP=2000
# shellcheck disable=SC1090
. "$conf"
for v in PROD_UNIT STAGED_UNIT PROD_DIR STAGED_DIR PROD_PORT STAGED_PORT OLD_SUFFIX SMOKE INVARIANT INVARIANT_WANT; do
  [ -n "${!v:-}" ] || { echo "$conf does not set $v" >&2; exit 2; }
done
for v in PROD_UNIT STAGED_UNIT OLD_SUFFIX; do
  [[ ${!v} =~ ^[a-z0-9][a-z0-9-]*$ ]] || { echo "$v is not a plain name: ${!v}" >&2; exit 2; }
done
for v in PROD_DIR STAGED_DIR; do
  [[ ${!v} =~ ^/[A-Za-z0-9_./-]+$ ]] && [[ ${!v} != */ ]] || { echo "$v is not an absolute path: ${!v}" >&2; exit 2; }
done
for v in PROD_PORT STAGED_PORT MAX_GAP; do
  [[ ${!v} =~ ^[0-9]+$ ]] || { echo "$v is not a number: ${!v}" >&2; exit 2; }
done
[ "$PROD_UNIT" != "$STAGED_UNIT" ] && [ "$PROD_DIR" != "$STAGED_DIR" ] && [ "$PROD_PORT" != "$STAGED_PORT" ] \
  || { echo "production and staged must be different units, directories and ports" >&2; exit 2; }
smoke=${SMOKE/#\~\//$HOME/}
[ -f "$smoke" ] || { echo "no smoke file at $smoke: clone nuthatch-org/kittiwake or fix SMOKE" >&2; exit 2; }
host=${SWAP_HOST:-root@89.167.109.4}
key=${SWAP_KEY:-$HOME/.ssh/hetzner_drpc}
ssh_() { ssh -i "$key" -o BatchMode=yes -o ServerAliveInterval=15 "$host" "$@"; }
work=/root/$STAGED_UNIT-swap-$stamp
job=$STAGED_UNIT-$mode-$stamp

IFS= read -r -d '' remote <<'REMOTE' || true
set -euo pipefail
mode=$1 stamp=$2 work=$3
. "$work/conf.env"
PROD=$PROD_UNIT STAGED=$STAGED_UNIT P=$PROD_DIR L=$STAGED_DIR PP=$PROD_PORT LP=$STAGED_PORT
OLD=$P.$OLD_SUFFIX-$stamp
say() { echo "[$(date -u +%H:%M:%S)] $*"; }
die() { say "STOP: $*"; exit 1; }
js() { jq -er "$1"; }
command -v jq >/dev/null || die "jq is not on this box"
get() { curl -fsS -m "${2:-30}" "http://127.0.0.1:$1"; }
sql() { curl -sS -m 600 -G "http://127.0.0.1:$1/sql" --data-urlencode "q=$2"; }
nid() { get "$1/nest" | js .nid; }
last() { get "$1/ready" | js .last_block; }
views_ok() { [ "$(get "$1/ready" | js '.views_failed | arrays | length')" = 0 ]; }
wait_ready() {  # wait_ready PORT SECONDS
  local t=0
  until get "$1/ready" 5 >/dev/null 2>&1; do
    [ "$t" -lt "$2" ] || return 1
    sleep 5; t=$((t + 5))
  done
}
invariant() { sql "$1" "$INVARIANT" | js '.columns[0] as $c | .rows[0][$c]'; }
smoke() {  # smoke PORT: every statement in $work/smoke.sql answers without an error
  local n=0 s out
  while IFS= read -r s; do
    case "$s" in ''|--*) continue ;; esac
    out=$(sql "$1" "$s") || return 1
    printf '%s' "$out" | jq -e '.error | . == null or . == false or . == ""' >/dev/null || { say "smoke refused: ${s:0:120}"; return 1; }
    n=$((n + 1))
  done < "$work/smoke.sql"
  say "smoke: $n statements answered"
}
execstart() { systemctl show -p ExecStart --value "$1"; }
bin_of() { execstart "$1" | grep -o 'path=[^ ;]*' | head -1; }

if [ "$mode" = swap ] || [ "$mode" = check ]; then
  systemctl is-active -q "$PROD" || die "$PROD is not active"
  systemctl is-active -q "$STAGED" || die "$STAGED is not active"
  [ -d "$P" ] && [ -d "$L" ] || die "missing $P or $L"
  [ ! -e "$OLD" ] || die "$OLD already exists"
  [ "$(stat -c %d "$P")" = "$(stat -c %d "$(dirname "$P")")" ] && [ "$(stat -c %d "$L")" = "$(stat -c %d "$(dirname "$P")")" ] \
    || die "$P and $L are not on one filesystem, so mv would copy"
  execstart "$PROD" | grep -q -- "--dir $P --listen 127.0.0.1:$PP " || die "$PROD does not serve $P on $PP"
  execstart "$STAGED" | grep -q -- "--dir $L --listen 127.0.0.1:$LP " || die "$STAGED does not serve $L on $LP"
  [ "$(bin_of "$PROD")" = "$(bin_of "$STAGED")" ] || die "the units run different binaries: $(bin_of "$PROD") vs $(bin_of "$STAGED")"
  get "$PP/ready" >/dev/null || die "$PP is not ready"
  get "$LP/ready" >/dev/null || die "$LP is not ready"
  lp=$(last "$PP") ls=$(last "$LP")
  gap=$((ls - lp))
  [ "${gap#-}" -le "$MAX_GAP" ] || die "$LP is at $ls, more than $MAX_GAP blocks from $PP at $lp"
  old_nid=$(nid "$PP") new_nid=$(nid "$LP")
  old_inv=$(invariant "$PP") new_inv=$(invariant "$LP")
  [ "$new_inv" = "$INVARIANT_WANT" ] || die "$LP answers the invariant as $new_inv, not $INVARIANT_WANT"
  [ "$old_inv" != "$INVARIANT_WANT" ] || die "$PP already answers the invariant as $INVARIANT_WANT; nothing to swap, and nothing would tell the two apart"
  views_ok "$LP" || die "$LP reports failed views"
  smoke "$LP" || die "$LP failed the smoke"
  printf '%s\n' "$old_nid" > "$work/old-nid"; printf '%s\n' "$new_nid" > "$work/new-nid"
  printf '%s\n' "$old_inv" > "$work/old-invariant"
  say "prechecks passed: $PP $old_nid at $lp (invariant $old_inv), $LP $new_nid at $ls (invariant $new_inv)"
  if [ "$mode" = check ]; then say "CHECKED: nothing was stopped or moved"; exit 0; fi

  moved_old=0 moved_new=0
  restore() {
    trap - ERR; set +e
    say "restoring $P from $OLD"
    systemctl stop "$PROD" || true
    if [ "$moved_new" = 1 ]; then mv "$P" "$L"; fi
    if [ "$moved_old" = 1 ]; then mv "$OLD" "$P"; fi
    systemctl start "$PROD"
    wait_ready "$PP" 900 && [ "$(invariant "$PP")" = "$old_inv" ] \
      && say "restored: $PP serves the old directory again (invariant $old_inv); $L is intact, start $STAGED to resume it" \
      || say "RESTORE DID NOT COME BACK READY ON THE OLD DIRECTORY: look at journalctl -u $PROD now"
    exit 1
  }
  trap restore ERR
  systemctl stop "$STAGED"
  systemctl stop "$PROD"
  mv "$P" "$OLD"; moved_old=1
  mv "$L" "$P"; moved_new=1
  systemctl start "$PROD"
  say "started $PROD on the staged directory"
  wait_ready "$PP" 900 || { say "$PP not ready after 15 minutes"; false; }
  [ "$(nid "$PP")" = "$new_nid" ] || { say "$PP reports $(nid "$PP"), not $new_nid"; false; }
  [ "$(invariant "$PP")" = "$INVARIANT_WANT" ] || { say "$PP answers the invariant as $(invariant "$PP"), not $INVARIANT_WANT"; false; }
  views_ok "$PP" || { say "$PP reports failed views"; false; }
  smoke "$PP" || false
  trap - ERR
  say "SWAPPED: $PP serves $new_nid at block $(last "$PP"), invariant $INVARIANT_WANT. The old directory is $OLD."
  say "rollback: deploy/swap-nest-dir.sh <conf> rollback $stamp"
else
  [ -d "$OLD" ] || die "no $OLD to roll back to"
  [ -f "$work/old-nid" ] && [ -f "$work/new-nid" ] && [ -f "$work/old-invariant" ] || die "no $work/old-nid, new-nid and old-invariant from the swap"
  [ ! -e "$L" ] || die "$L exists; move it aside first"
  old_nid=$(cat "$work/old-nid")
  old_inv=$(cat "$work/old-invariant")
  # Only the state this swap left: anything else means the directories are not what they were.
  ! systemctl is-active -q "$STAGED" || die "$STAGED is active; it must stay stopped after the swap"
  get "$PP/ready" >/dev/null || die "$PP is not ready, so what it serves cannot be checked"
  [ "$(invariant "$PP")" = "$INVARIANT_WANT" ] || die "$PP answers the invariant as $(invariant "$PP"), not the swapped-in $INVARIANT_WANT; nothing moved"
  moved_new=0 moved_old=0
  # A failed move, a failed start or a stop of this unit between the moves: put the swapped-in
  # directory back where it was serving, so the rollback is abandoned rather than half done.
  unroll() {
    trap - ERR TERM INT HUP; set +e
    say "putting the swapped-in directory back at $P"
    systemctl stop "$PROD" || true
    # Each flag is set before its move, since a signal lands between the two: what is actually at
    # $P decides, so an interrupted move is undone and a failed one is not repeated.
    if [ "$moved_old" = 1 ] && [ -d "$P" ]; then mv "$P" "$OLD"; fi
    if [ "$moved_new" = 1 ] && [ ! -e "$P" ]; then mv "$L" "$P"; fi
    systemctl start "$PROD"
    wait_ready "$PP" 900 && [ "$(invariant "$PP")" = "$INVARIANT_WANT" ] \
      && say "ROLLBACK ABANDONED: $PP serves the swapped-in directory again (invariant $INVARIANT_WANT); $OLD is intact" \
      || say "ROLLBACK ABANDONED AND $PP DID NOT COME BACK ON THE SWAPPED-IN DIRECTORY: look at journalctl -u $PROD now"
    exit 1
  }
  trap unroll ERR TERM INT HUP
  systemctl stop "$PROD"
  moved_new=1; mv "$P" "$L"
  moved_old=1; mv "$OLD" "$P"
  systemctl start "$PROD"
  wait_ready "$PP" 900 || { say "$PP not ready after 15 minutes; journalctl -u $PROD"; false; }
  [ "$(nid "$PP")" = "$old_nid" ] || { say "$PP reports $(nid "$PP"), not $old_nid"; false; }
  [ "$(invariant "$PP")" = "$old_inv" ] || { say "$PP answers the invariant as $(invariant "$PP"), not the old $old_inv"; false; }
  smoke "$PP" || false
  trap - ERR TERM INT HUP
  say "ROLLED BACK: $PP serves $old_nid with invariant $old_inv. The staged directory is back at $L ($STAGED is stopped)."
fi
REMOTE

ssh_ "mkdir -p $work && cat > $work/smoke.sql" < "$smoke"
for v in PROD_UNIT STAGED_UNIT PROD_DIR STAGED_DIR PROD_PORT STAGED_PORT OLD_SUFFIX INVARIANT INVARIANT_WANT MAX_GAP; do
  printf '%s=%q\n' "$v" "${!v}"
done | ssh_ "cat > $work/conf.env"
printf '%s\n' "$remote" | ssh_ "cat > $work/$mode.sh"
ssh_ "systemd-run --unit=$job --collect --quiet bash $work/$mode.sh $mode $stamp $work"
echo "stamp $stamp; following $job on $host"
ssh_ "journalctl -u $job -f -n 50 -o cat & j=\$!; while systemctl is-active -q $job 2>/dev/null; do sleep 2; done; sleep 2; kill \$j"
result=$(ssh_ "journalctl -u $job -o cat | grep -cE '^\[..:..:..\] (SWAPPED|ROLLED BACK|CHECKED):'" || true)
[ "$result" = 1 ] || { echo "$mode did not complete; see journalctl -u $job on $host" >&2; exit 1; }
