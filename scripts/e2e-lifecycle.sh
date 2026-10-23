#!/usr/bin/env bash
# End-to-end check of the runtime-owned nest lifecycle (#1552, 3.13.0) against a real runtime on a
# real chain. Every step asserts; the first failure stops the run and says what it saw.
#
#   BASE=http://host:port TOKEN=… NID1=… NID2=… RUNTIME_DIR=… RESTART='cmd' ./scripts/e2e-lifecycle.sh
#
# The runtime must start empty (chains declared, nothing mounted) with --registry holding both
# NIDs, which must be two versions of one nest on one chain. RESTART stops and starts it.
set -uo pipefail

# The token a runtime made for itself (#2354) when TOKEN is not given.
TOKEN="${TOKEN:-$(cat "${RUNTIME_DIR:-.}/.admin-token" 2>/dev/null)}"
: "${BASE:?}" "${TOKEN:?}" "${NID1:?}" "${NID2:?}" "${RUNTIME_DIR:?}" "${RESTART:?}"
MOUNT_TIMEOUT=${MOUNT_TIMEOUT:-1800}
H_AUTH="Authorization: Bearer $TOKEN"
H_JSON="Content-Type: application/json"

step=0
say() { printf '\n== %s\n' "$*"; }
fail() {
  printf 'FAIL [step %s]: %s\n' "$step" "$*" >&2
  exit 1
}
ok() { printf '   ok  %s\n' "$*"; }

# api METHOD PATH [BODY] -> sets STATUS and BODY
api() {
  local method=$1 path=$2 data=${3:-}
  local out
  if [ -n "$data" ]; then
    out=$(curl -s -w '\n%{http_code}' -X "$method" -H "$H_AUTH" -H "$H_JSON" -d "$data" "$BASE$path")
  else
    out=$(curl -s -w '\n%{http_code}' -X "$method" -H "$H_AUTH" "$BASE$path")
  fi
  STATUS=${out##*$'\n'}
  BODY=${out%$'\n'*}
}
expect() {
  [ "$STATUS" = "$1" ] || fail "$2: expected $1, got $STATUS: $BODY"
  ok "$2 ($STATUS)"
}
field() { printf '%s' "$BODY" | jq -r "$1"; }

# wait_phase NAME PHASE [TIMEOUT] -> fails on a phase that can no longer become PHASE
wait_phase() {
  local name=$1 want=$2 limit=${3:-$MOUNT_TIMEOUT} waited=0 phase=
  while [ "$waited" -lt "$limit" ]; do
    api GET "/_admin/mounts/$name"
    phase=$(field .phase)
    [ "$phase" = "$want" ] && { ok "$name reached $want after ${waited}s"; return 0; }
    if [ "$phase" = failed ] && [ "$want" != failed ]; then
      fail "$name failed while waiting for $want: $(field .reason)"
    fi
    sleep 5
    waited=$((waited + 5))
  done
  fail "$name did not reach $want in ${limit}s (last: $phase)"
}
wait_up() {
  for _ in $(seq 1 60); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/health")" = 200 ] && return 0
    sleep 1
  done
  fail "runtime did not come back"
}
served_nid() { curl -s "$BASE/$1/sql?q=SELECT%201" | jq -r '.provenance.nid // empty'; }
metric() { curl -s "$BASE/metrics" | awk -v m="$1" 'index($0, m) == 1 { print $NF; exit }'; }

say "$((step += 1)). an empty runtime, and the gate in front of the admin API"
api GET /nests
expect 200 "roster"
[ "$(field '.nests | length')" = 0 ] || fail "runtime is not empty: $BODY"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "$H_JSON" -d "{\"name\":\"x\",\"nid\":\"$NID1\"}" "$BASE/_admin/nests")" = 401 ] ||
  fail "a mount without the token was not refused"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/_admin/mounts")" = 401 ] ||
  fail "reading the jobs without the token was not refused"
ok "no token is 401"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "$H_AUTH" -d "{\"name\":\"x\",\"nid\":\"$NID1\"}" "$BASE/_admin/nests")" = 415 ] ||
  fail "a mount without a JSON content type was not 415"
ok "no content type is 415"
api POST /_admin/nests '{"name":"x","nid":"not-a-nid"}'
expect 400 "a malformed nid"

say "$((step += 1)). names boot would refuse are refused, and nothing is persisted"
long=$(printf 'a%.0s' $(seq 1 300))
for bad in '../escape' '' "$long" 'a/b/c' 'usdc__moving' 'x.y' 'default/usdc'; do
  api POST '/_admin/nests?wait=true' "{\"name\":\"$bad\",\"nid\":\"$NID1\"}"
  [ "$STATUS" = 400 ] || fail "name '${bad:0:20}' got $STATUS: $BODY"
done
ok "seven hostile names refused with 400"
eval "$RESTART"
wait_up
ok "the runtime still restarts"
api GET /nests
[ "$(field '.nests | length')" = 0 ] || fail "a refused name was mounted: $BODY"

say "$((step += 1)). a NID the registry does not hold fails, and leaves nothing"
missing=$(printf 'ab%.0s' $(seq 1 32))
api POST /_admin/nests "{\"name\":\"ghost\",\"nid\":\"$missing\"}"
expect 202 "mount of an unknown nid is accepted"
wait_phase ghost failed 60
api GET /_admin/mounts/ghost
case "$(field .reason)" in *"not found"*) ok "reason names it: $(field .reason)" ;; *) fail "reason: $BODY" ;; esac
[ ! -e "$RUNTIME_DIR/data/$missing" ] || fail "a failed fetch left data/$missing"
ok "nothing written"

say "$((step += 1)). dry run fetches, prices and mounts nothing"
api POST '/_admin/nests?dry_run=true' "{\"name\":\"acme/ds\",\"nid\":\"$NID1\"}"
expect 200 "dry run"
[ "$(field .fetched)" = true ] || fail "dry run did not fetch: $BODY"
[ "$(field .refusal)" = null ] || fail "dry run refused: $BODY"
[ "$(field .chain)" = arbitrum-one ] || fail "dry run chain: $BODY"
ok "fetched, chain $(field .chain), start $(field .start_block), projected $(field .projected_mb) of $(field .ceiling_mb) MB"
[ -f "$RUNTIME_DIR/data/$NID1/nuthatch.toml" ] || fail "the verified nest is not installed"
api GET /nests
[ "$(field '.nests | length')" = 0 ] || fail "dry run mounted something: $BODY"
ok "nothing mounted"

say "$((step += 1)). mount by NID is a job; a restart mid-join resumes it"
api POST /_admin/nests "{\"name\":\"acme/ds\",\"nid\":\"$NID1\"}"
expect 202 "mount accepted"
[ "$(field .phase)" = accepted ] || fail "phase: $BODY"
wait_phase acme/ds joining 120
eval "$RESTART"
wait_up
ok "restarted while joining"
wait_phase acme/ds live
api POST /_admin/nests "{\"name\":\"acme/ds\",\"nid\":\"$NID1\"}"
expect 200 "a repeated mount of a live name is idempotent"
api POST /_admin/nests "{\"name\":\"acme/ds\",\"nid\":\"$NID2\"}"
expect 409 "another nid under a live name"
[ "$(served_nid acme/ds)" = "$NID1" ] || fail "acme/ds does not answer from NID1"
ok "acme/ds serves NID1"

say "$((step += 1)). a second tenant shares the dataset"
api POST '/_admin/nests?dry_run=true' "{\"name\":\"globex/ds\",\"nid\":\"$NID1\"}"
expect 200 "dry run of a shared dataset"
[ "$(field .shares)" != null ] || fail "dry run did not see the share: $BODY"
ok "shares $(field .shares)"
api POST /_admin/nests "{\"name\":\"globex/ds\",\"nid\":\"$NID1\"}"
expect 202 "second tenant accepted"
wait_phase globex/ds live 120
[ "$(served_nid globex/ds)" = "$NID1" ] || fail "globex/ds does not answer from NID1"
ok "globex/ds serves the same dataset"

say "$((step += 1)). per-nest metrics"
hot=$(metric 'nuthatch_nest_hot_store_bytes{nest="acme/ds"}')
[ -n "$hot" ] && [ "$hot" -gt 0 ] || fail "no per-nest hot bytes for acme/ds"
ok "acme/ds hot store $hot bytes"
[ -n "$(metric 'nuthatch_nest_sealed_segments_bytes{nest="acme/ds"}')" ] || fail "no per-nest sealed series"
ok "per-nest sealed series present"

say "$((step += 1)). suspend, across a restart, and resume"
api POST /_admin/suspend/globex/ds '{}'
expect 200 "suspend"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/globex/ds/health")" = 503 ] || fail "suspended routes are not 503"
[ "$(curl -s "$BASE/globex/ds/health" | jq -r .suspended)" = true ] || fail "503 does not say suspended"
ok "globex/ds answers a named 503"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/acme/ds/health")" = 200 ] || fail "co-tenant stopped serving"
ok "acme/ds unaffected"
wait_phase globex/ds suspended 10
eval "$RESTART"
wait_up
wait_phase acme/ds live 300
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/globex/ds/health")" = 503 ] || fail "suspension did not survive a restart"
wait_phase globex/ds suspended 10
grep -q 'globex/ds' "$RUNTIME_DIR/mounts.toml" || fail "mounts.toml lost the suspended record"
ok "still suspended after restart"
api POST /_admin/resume/globex/ds '{}'
expect 202 "resume"
wait_phase globex/ds live 300
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/globex/ds/health")" = 200 ] || fail "resumed routes are not 200"
ok "globex/ds serving again"

say "$((step += 1)). move a name to a new NID while a reader polls it"
reads=$(mktemp)
(while [ ! -e "$reads.stop" ]; do
  code=$(curl -s -o "$reads.body" -w '%{http_code}' "$BASE/acme/ds/sql?q=SELECT%201")
  printf '%s %s\n' "$code" "$(jq -r '.provenance.nid // "-"' < "$reads.body" 2>/dev/null)" >> "$reads"
  sleep 0.2
done) &
reader=$!
sleep 2
api POST /_admin/move/acme/ds "{\"nid\":\"$NID2\"}"
expect 202 "move accepted"
wait_phase acme/ds live
sleep 3
touch "$reads.stop"
wait "$reader"
total=$(wc -l < "$reads")
bad=$(awk '$1 != 200' "$reads" | wc -l)
[ "$bad" -eq 0 ] || fail "$bad of $total reads during the move were not 200: $(awk '$1 != 200' "$reads" | head -3 | tr '\n' ' ')"
flips=$(awk -v a="$NID1" -v b="$NID2" 'BEGIN { s = a } $2 == b { s = b } $2 == a && s == b { n++ } END { print n + 0 }' "$reads")
[ "$flips" -eq 0 ] || fail "$flips reads went back to the old nid after the switch"
grep -q " $NID1\$" "$reads" && grep -q " $NID2\$" "$reads" || fail "the reader did not see both nids: $(sort -u -k2 "$reads" | head)"
ok "$total reads, all 200, old nid then new, never back"
rm -f "$reads" "$reads.stop" "$reads.body"
[ "$(served_nid globex/ds)" = "$NID1" ] || fail "the move disturbed the other tenant"
ok "globex/ds still on NID1"

say "$((step += 1)). a crash in the middle of a move"
api POST /_admin/move/globex/ds "{\"nid\":\"$NID2\"}"
expect 202 "move accepted"
eval "$RESTART"
wait_up
ok "the runtime boots after a crash mid-move"
! grep -q '__moving' "$RUNTIME_DIR/mounts.toml" || fail "mounts.toml holds a staging record"
wait_phase globex/ds live 600
[ "$(served_nid globex/ds)" = "$NID2" ] || fail "globex/ds is not on NID2 after the move"
ok "globex/ds finished its move to NID2"

say "$((step += 1)). unmount and reclaim"
api DELETE "/_admin/datasets/$NID1"
expect 200 "NID1, which nothing mounts after both moves, is reclaimed"
[ ! -e "$RUNTIME_DIR/data/$NID1" ] || fail "data/$NID1 is still on disk"
api DELETE '/_admin/nests/acme/ds?reclaim=true'
expect 200 "unmount acme/ds with reclaim"
[ "$(field .reclaim.outcome)" = kept ] || fail "NID2 was not kept for globex: $BODY"
ok "NID2 kept for $(field '.reclaim.mounted_by | join(",")')"
api DELETE '/_admin/nests/globex/ds?reclaim=true'
expect 200 "unmount globex/ds with reclaim"
[ "$(field .reclaim.outcome)" = reclaimed ] || fail "NID2 was not reclaimed: $BODY"
[ ! -e "$RUNTIME_DIR/data/$NID2" ] || fail "data/$NID2 is still on disk"
ok "NID2 reclaimed and gone"
api DELETE "/_admin/datasets/$NID2"
expect 404 "an absent dataset"

say "$((step += 1)). an emptied runtime stays up"
sleep 5
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/health")" = 200 ] || fail "the runtime exited when emptied"
api GET /nests
[ "$(field '.nests | length')" = 0 ] || fail "roster not empty: $BODY"
ok "empty and still serving"

printf '\nPASS: %s steps\n' "$step"
