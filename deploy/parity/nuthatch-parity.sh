#!/usr/bin/env bash
# nuthatch-parity - the daily Lodestar parity run on Helsinki (#1713, #1718), started by
# nuthatch-parity.timer. Runs scripts/lodestar-parity.sh pinned at sealed_through, then at the
# nest's head, keeps each run's output and exit status under PARITY_LOG_DIR, and posts one Discord
# line for any run that exits 1 or 4.
#
#   0  every mode ran clean
#   1  a disagreement, a failure to compare, or a precondition this wrapper could not meet; posted
#   2  known differences only (#1114, #1116); logged, not posted
#   3  the head mode could not bring the two sides to one head on any attempt; logged, not posted
#   4  the subgraph side did not answer, so nothing was compared (#1818); posted as NOT RUN
#
# Environment (all optional):
#   PARITY_SCRIPT          (default /usr/local/lib/nuthatch-parity/lodestar-parity.sh)
#   PARITY_ENV_FILE        GRAPH_API_KEY and optional NEST_URL, GRAPH_GATEWAY; mode 0600
#                          (default /etc/nuthatch/parity.env)
#   PARITY_WEBHOOK_FILE    (default /etc/nuthatch/parity_discord_webhook_url)
#   PARITY_LOG_DIR         (default /var/log/nuthatch/parity)
#   PARITY_RETAIN_DAYS     run logs older than this are deleted; runs.tsv is kept (default 90)
#   PARITY_MODES           (default "sealed head")
#   PARITY_HEAD_ATTEMPTS   head runs tried while the subgraph is short of the nest (default 3)
#   PARITY_HEAD_RETRY_SECS (default 600)
set -euo pipefail

script=${PARITY_SCRIPT:-/usr/local/lib/nuthatch-parity/lodestar-parity.sh}
env_file=${PARITY_ENV_FILE:-/etc/nuthatch/parity.env}
hook_file=${PARITY_WEBHOOK_FILE:-/etc/nuthatch/parity_discord_webhook_url}
log_dir=${PARITY_LOG_DIR:-/var/log/nuthatch/parity}
retain=${PARITY_RETAIN_DAYS:-90}
modes=${PARITY_MODES:-sealed head}
head_attempts=${PARITY_HEAD_ATTEMPTS:-3}
head_sleep=${PARITY_HEAD_RETRY_SECS:-600}

say() { echo "nuthatch-parity: $*" >&2; }
trap 'rc=$?; say "internal error at line $LINENO (exit $rc)"; exit 1' ERR

json_escape() {
  local s=$1
  s=${s//\\/\\\\}
  s=${s//\"/\\\"}
  s=${s//$'\t'/ }
  s=${s//$'\r'/ }
  s=${s//$'\n'/ }
  printf '%s' "$s" | tr -d '\000-\037'
}

page() {
  local hook msg
  msg=$(json_escape "${1:0:1800}")
  hook=$(head -n 1 "$hook_file" 2>/dev/null) || hook=""
  if [ -z "$hook" ]; then
    say "cannot read a webhook from $hook_file, so this was not posted: $1"
    return 1
  fi
  if ! curl -fsS -m 15 -H 'Content-Type: application/json' -d "{\"content\":\"$msg\"}" "$hook" >/dev/null; then
    say "could not post to Discord: $1"
    return 1
  fi
}

# A precondition this wrapper cannot meet is a parity run that did not happen, which must not be
# quieter than one that disagreed.
fail() {
  say "$*"
  page "PARITY NOT RUN on $(hostname): $*" || true
  exit 1
}

[ -n "${modes// /}" ] || fail "PARITY_MODES is empty, so no comparison would run"
for m in $modes; do
  case "$m" in sealed | head) ;; *) fail "unknown mode $m in PARITY_MODES" ;; esac
done
case "$retain" in '' | *[!0-9]*) fail "PARITY_RETAIN_DAYS must be a whole number of days" ;; esac
case "$head_attempts" in '' | *[!0-9]* | 0) fail "PARITY_HEAD_ATTEMPTS must be a positive integer" ;; esac
command -v jq >/dev/null || fail "jq is not on PATH; lodestar-parity.sh needs it"
command -v curl >/dev/null || fail "curl is not on PATH"
[ -x "$script" ] || fail "no executable parity script at $script"
[ -f "$env_file" ] || fail "no env file at $env_file (GRAPH_API_KEY=..., mode 0600)"
if [ -n "$(find "$env_file" -prune \( -perm -g+r -o -perm -o+r \) 2>/dev/null)" ]; then
  fail "$env_file is readable beyond its owner; it holds GRAPH_API_KEY, chmod 0600 it"
fi

# Read the three keys this run uses rather than sourcing a file root executes.
read_key() { sed -n "s/^$1=//p" "$env_file" | tail -n 1 | sed -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'\$/\1/"; }
GRAPH_API_KEY=$(read_key GRAPH_API_KEY)
[ -n "$GRAPH_API_KEY" ] || fail "GRAPH_API_KEY is empty in $env_file, so the subgraph side cannot be asked"
export GRAPH_API_KEY
v=$(read_key NEST_URL)
if [ -n "$v" ]; then export NEST_URL=$v; fi
v=$(read_key GRAPH_GATEWAY)
if [ -n "$v" ]; then export GRAPH_GATEWAY=$v; fi

mkdir -p "$log_dir" || fail "cannot create $log_dir"
[ -w "$log_dir" ] || fail "$log_dir is not writable"
find "$log_dir" -type f -name '*.log' -mtime +"$retain" -exec rm -f {} + \
  || say "could not prune logs older than $retain days in $log_dir"

runs=$log_dir/runs.tsv
[ -f "$runs" ] || printf 'started\tmode\texit\tpin\tversion\tattempt\tlog\n' >"$runs"

# The first line naming what disagreed; failing that, the last FAIL line, which every exit 1 prints.
failing_line() {
  local log=$1 line
  line=$(grep -E '(^| )DIFF$|UNPAIRED|MISSING|BOUNDARY|DO NOT CANCEL|unexplained|unmodelled|nobody has classified|reclassify|subgraph-only|nest-only' "$log" \
    | grep -v 'KNOWN-DIFF' | head -n 1 || true)
  [ -n "$line" ] || line=$(grep -E '^FAIL |disagree|failed' "$log" | tail -n 1 || true)
  [ -n "$line" ] || line=$(grep -v '^exit ' "$log" | tail -n 1 || true)
  printf '%s' "$line" | sed -e 's/^ *//'
}

# One run of one mode into its own log; leaves its status in last_rc, with any undefined exit as 1.
run_mode() {
  local mode=$1 attempt=$2 stamp log rc=0 pin version
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  log=$log_dir/$stamp-$mode.log
  [ "$attempt" -eq 1 ] || log=$log_dir/$stamp-$mode-$attempt.log
  PARITY_MODE=$mode "$script" >"$log" 2>&1 || rc=$?
  echo "exit $rc" >>"$log"
  pin=$(sed -n 's/^PIN .*block=\([0-9]*\).*/\1/p' "$log" | head -n 1)
  version=$(sed -n 's/^PIN .*version=\([^ ]*\).*/\1/p' "$log" | head -n 1)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$stamp" "$mode" "$rc" "${pin:--}" "${version:--}" \
    "$attempt" "$log" >>"$runs"
  say "$mode attempt $attempt: exit $rc, pin ${pin:-unknown}, version ${version:-unknown}, log $log"
  if [ "$rc" -eq 4 ]; then
    local why
    why=$(sed -n 's/^NOT RUN //p' "$log" | tail -n 1)
    page "PARITY NOT RUN ($mode): ${why:-the subgraph side did not answer} - pin ${pin:-unknown} on ${version:-unknown}, $log on $(hostname)" \
      || true
  elif [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ] && ! { [ "$mode" = head ] && [ "$rc" -eq 3 ]; }; then
    local what
    what=$(failing_line "$log")
    [ "$rc" -eq 1 ] || what="exit $rc, which lodestar-parity.sh does not define; ${what}"
    page "PARITY FAIL ($mode) pin ${pin:-unknown} on ${version:-unknown}: ${what} - $log on $(hostname)" \
      || true
    rc=1
  fi
  last_rc=$rc
}

worst=0
for mode in $modes; do
  attempt=1
  while :; do
    run_mode "$mode" "$attempt"
    if [ "$mode" = head ] && [ "$last_rc" -eq 3 ] && [ "$attempt" -lt "$head_attempts" ]; then
      attempt=$((attempt + 1))
      sleep "$head_sleep"
      continue
    fi
    break
  done
  if [ "$mode" = head ] && [ "$last_rc" -eq 3 ]; then
    say "head: the nest and the subgraph could not be brought to the same head in $attempt attempt(s)"
  fi
  # 1 outranks 4 outranks 3 outranks 2: a disagreement, a subgraph that would not answer, a head that
  # could not be reached, then known differences.
  case "$last_rc" in
    1) worst=1 ;;
    4) [ "$worst" -eq 1 ] || worst=4 ;;
    3) [ "$worst" -eq 1 ] || [ "$worst" -eq 4 ] || worst=3 ;;
    2) [ "$worst" -ne 0 ] || worst=2 ;;
  esac
done
exit "$worst"
