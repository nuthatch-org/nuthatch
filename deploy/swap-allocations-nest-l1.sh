#!/usr/bin/env bash
# Swaps the allocations nest on Helsinki (graph-allocations-nest-next, 8107) onto the fresh index with
# `[extract] l1_blocks` staged beside it (graph-allocations-nest-l1, 8108), and back (#1882).
#
#   deploy/swap-allocations-nest-l1.sh check               the prechecks alone; nothing stops
#   deploy/swap-allocations-nest-l1.sh swap
#   deploy/swap-allocations-nest-l1.sh rollback <stamp>     <stamp> is what `swap` printed
#
# A directory swap: no unit file is edited. `swap` stops both units, moves the production directory to
# <dir>.pre-l1-<stamp> and the staged one into its place, and starts production. Every path, port,
# flag and drop-in stays as it was, so parity, the gate export and the roll keep working unchanged.
# Downtime is one stop and start of 8107.
#
# Everything the swap reads is checked before anything stops. After the start it requires /ready, the
# staged NID on /nest, epoch 1390's query fees at the subgraph's value, the newest epoch placed from
# l1_blocks, and every smoke statement answered; any failure puts the old directory back, starts it,
# and checks it came back. The box side runs as a transient systemd unit, so a dropped ssh cannot
# leave it half done.
#
# The staged unit ran its backfill without --rpc-fallback (arb1 in the pool caps header batches at ten,
# and publicnode answers 403). That does not carry over: the swap keeps production's own unit, so 8107
# serves with its own ExecStart, fallbacks and drop-ins.
#
# Order, for #1882: apply kittiwake's db/schema.sql (two nullable epoch columns) as the app role, run
# `check`, run `swap` (Chief schedules it), roll kittiwake (nuthatch-org/kittiwake#201), then
# Lodestar (nuthatch-org/lodestar#325). Each of the two consumers also reads a nest without the L1
# range, so a rollback here never strands them.
#
# Smoke statements: SMOKE (default ~/Projects/kittiwake/nuthatch-gate/smoke/graph-allocations-nest.sql).
set -euo pipefail

host=${SWAP_HOST:-root@89.167.109.4}
key=${SWAP_KEY:-$HOME/.ssh/hetzner_drpc}
smoke=${SMOKE:-$HOME/Projects/kittiwake/nuthatch-gate/smoke/graph-allocations-nest.sql}
mode=${1:-}
case "$mode" in
  swap|check) [ $# -eq 1 ] || { echo "usage: $0 $mode" >&2; exit 2; }
    stamp=$(date -u +%Y%m%dT%H%M%SZ) ;;
  rollback) [ $# -eq 2 ] && [[ $2 =~ ^[0-9]{8}T[0-9]{6}Z$ ]] || { echo "usage: $0 rollback <stamp>" >&2; exit 2; }
    stamp=$2 ;;
  *) echo "usage: $0 check | swap | rollback <stamp>" >&2; exit 2 ;;
esac
[ -f "$smoke" ] || { echo "no smoke file at $smoke: clone nuthatch-org/kittiwake or set SMOKE" >&2; exit 2; }
ssh_() { ssh -i "$key" -o BatchMode=yes -o ServerAliveInterval=15 "$host" "$@"; }
work=/root/alloc-l1-swap-$stamp
job=alloc-l1-$mode-$stamp

IFS= read -r -d '' remote <<'REMOTE' || true
set -euo pipefail
mode=$1 stamp=$2 work=$3
PROD=graph-allocations-nest-next STAGED=graph-allocations-nest-l1
P=/opt/nuthatch/graph-allocations-nest-next L=/opt/nuthatch/graph-allocations-nest-l1
OLD=$P.pre-l1-$stamp
WANT=2981030500877230556383   # the subgraph's queryFeesCollected for epoch 1390
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
fees1390() { sql "$1" "SELECT CAST(query_fees_collected AS VARCHAR) AS v FROM lodestar_epochs WHERE id = 1390" | js '.rows[0].v'; }
newest_source() { sql "$1" "SELECT boundary_source AS s FROM epoch_boundaries ORDER BY epoch DESC LIMIT 1" | js '.rows[0].s'; }
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
  systemctl is-active -q $PROD || die "$PROD is not active"
  systemctl is-active -q $STAGED || die "$STAGED is not active"
  [ -d $P ] && [ -d $L ] || die "missing $P or $L"
  [ ! -e $OLD ] || die "$OLD already exists"
  [ "$(stat -c %d $P)" = "$(stat -c %d /opt/nuthatch)" ] && [ "$(stat -c %d $L)" = "$(stat -c %d /opt/nuthatch)" ] \
    || die "$P and $L are not on one filesystem, so mv would copy"
  grep -qx 'l1_blocks = true' $L/nuthatch.toml || die "$L/nuthatch.toml does not enable l1_blocks"
  ! grep -qx 'l1_blocks = true' $P/nuthatch.toml || die "$P already enables l1_blocks; nothing to swap"
  execstart $PROD | grep -q -- "--dir $P --listen 127.0.0.1:8107 " || die "$PROD does not serve $P on 8107"
  execstart $STAGED | grep -q -- "--dir $L --listen 127.0.0.1:8108 " || die "$STAGED does not serve $L on 8108"
  [ "$(bin_of $PROD)" = "$(bin_of $STAGED)" ] || die "the units run different binaries: $(bin_of $PROD) vs $(bin_of $STAGED)"
  get 8107/ready >/dev/null || die "8107 is not ready"
  get 8108/ready >/dev/null || die "8108 is not ready"
  lp=$(last 8107) ls=$(last 8108)
  gap=$((ls - lp))
  [ "${gap#-}" -le 2000 ] || die "8108 is at $ls, more than 2000 blocks from 8107 at $lp"
  old_nid=$(nid 8107) new_nid=$(nid 8108)
  [ "$old_nid" != "$new_nid" ] || die "both nests report NID $old_nid"
  [ "$(fees1390 8108)" = "$WANT" ] || die "8108 does not read epoch 1390's query fees as $WANT"
  [ "$(newest_source 8108)" = l1 ] || die "8108's newest epoch is not placed from l1_blocks"
  views_ok 8108 || die "8108 reports failed views"
  smoke 8108 || die "8108 failed the smoke"
  printf '%s\n' "$old_nid" > "$work/old-nid"; printf '%s\n' "$new_nid" > "$work/new-nid"
  say "prechecks passed: 8107 $old_nid at $lp, 8108 $new_nid at $ls"
  if [ "$mode" = check ]; then say "CHECKED: nothing was stopped or moved"; exit 0; fi

  moved_old=0 moved_new=0
  restore() {
    trap - ERR; set +e
    say "restoring $P from $OLD"
    systemctl stop $PROD || true
    if [ "$moved_new" = 1 ]; then mv $P $L; fi
    if [ "$moved_old" = 1 ]; then mv $OLD $P; fi
    systemctl start $PROD
    wait_ready 8107 900 && [ "$(nid 8107)" = "$old_nid" ] \
      && say "restored: 8107 serves $old_nid again; $L is intact, start $STAGED to resume it" \
      || say "RESTORE DID NOT COME BACK READY ON $old_nid: look at journalctl -u $PROD now"
    exit 1
  }
  trap restore ERR
  systemctl stop $STAGED
  systemctl stop $PROD
  mv $P $OLD; moved_old=1
  mv $L $P; moved_new=1
  systemctl start $PROD
  say "started $PROD on the l1_blocks directory"
  wait_ready 8107 900 || { say "8107 not ready after 15 minutes"; false; }
  [ "$(nid 8107)" = "$new_nid" ] || { say "8107 reports $(nid 8107), not $new_nid"; false; }
  [ "$(fees1390 8107)" = "$WANT" ] || { say "8107 misreads epoch 1390's query fees"; false; }
  [ "$(newest_source 8107)" = l1 ] || { say "8107's newest epoch is not from l1_blocks"; false; }
  views_ok 8107 || { say "8107 reports failed views"; false; }
  smoke 8107 || false
  trap - ERR
  say "SWAPPED: 8107 serves $new_nid at block $(last 8107). The old directory is $OLD."
  say "rollback: deploy/swap-allocations-nest-l1.sh rollback $stamp"
else
  [ -d $OLD ] || die "no $OLD to roll back to"
  [ -f "$work/old-nid" ] && [ -f "$work/new-nid" ] || die "no $work/old-nid and new-nid from the swap"
  [ ! -e $L ] || die "$L exists; move it aside first"
  old_nid=$(cat "$work/old-nid")
  new_nid=$(cat "$work/new-nid")
  # Only the state this swap left: anything else means the directories are not what they were.
  ! systemctl is-active -q $STAGED || die "$STAGED is active; it must stay stopped after the swap"
  get 8107/ready >/dev/null || die "8107 is not ready, so what it serves cannot be checked"
  [ "$(nid 8107)" = "$new_nid" ] || die "8107 serves $(nid 8107), not the swapped-in $new_nid; nothing moved"
  systemctl stop $PROD
  mv $P $L
  mv $OLD $P
  systemctl start $PROD
  wait_ready 8107 900 || die "8107 not ready after 15 minutes; journalctl -u $PROD"
  [ "$(nid 8107)" = "$old_nid" ] || die "8107 reports $(nid 8107), not $old_nid"
  [ "$(fees1390 8107)" = "$WANT" ] || die "8107 misreads epoch 1390's query fees"
  smoke 8107 || die "8107 failed the smoke"
  say "ROLLED BACK: 8107 serves $old_nid. The l1_blocks directory is back at $L ($STAGED is stopped)."
fi
REMOTE

ssh_ "mkdir -p $work && cat > $work/smoke.sql" < "$smoke"
printf '%s\n' "$remote" | ssh_ "cat > $work/$mode.sh"
ssh_ "systemd-run --unit=$job --collect --quiet bash $work/$mode.sh $mode $stamp $work"
echo "stamp $stamp; following $job on $host"
ssh_ "journalctl -u $job -f -n 50 -o cat & j=\$!; while systemctl is-active -q $job 2>/dev/null; do sleep 2; done; sleep 2; kill \$j"
result=$(ssh_ "journalctl -u $job -o cat | grep -cE '^\[..:..:..\] (SWAPPED|ROLLED BACK|CHECKED):'" || true)
[ "$result" = 1 ] || { echo "$mode did not complete; see journalctl -u $job on $host" >&2; exit 1; }
