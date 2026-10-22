#!/usr/bin/env bash
#
# RFC-0069 S0(b): `.github/workflows/tip-lag.sh` unchanged, with the nest's RSS sampled from /proc
# every second and, when WATCH=1, one `nuthatch watch` at its defaults attached for the whole run.
# The watcher runs in a pty (`script`) at 120x40, so it draws as it would on a screen, and records
# every read so the run shows it was polling. Linux only.
#
# usage: watched-tip-lag.sh <repo> <nuthatch binary> <out dir> <label> <WATCH 0|1>
# Env: PORT (8390), RPC_PORT (8648), SAMPLES (100), FIXTURE_RPC (built if unset).
set -euo pipefail

REPO="$1"
BIN="$2"
OUT="$3"
LABEL="$4"
WATCH="$5"
PORT="${PORT:-8390}"
RPC_PORT="${RPC_PORT:-8648}"
mkdir -p "$OUT"
RSS="$OUT/$LABEL.rss.tsv"
READS="$OUT/$LABEL.watch.jsonl"
: > "$RSS"
rm -f "$READS"

nest_pid() { pgrep -f -- "--listen 127.0.0.1:$PORT" | head -1 || true; }

(
  pid=
  for _ in $(seq 1 1500); do
    pid="$(nest_pid)"
    [ -n "$pid" ] && break
    sleep 0.2
  done
  [ -n "$pid" ] || exit 0
  if [ "$WATCH" = 1 ]; then
    until curl -fsS -m 1 "127.0.0.1:$PORT/ready" > /dev/null 2>&1; do
      kill -0 "$pid" 2>/dev/null || exit 0
      sleep 0.2
    done
    tail -f /dev/null | TERM=xterm-256color COLORTERM=truecolor script -qfec \
      "stty cols 120 rows 40; exec $BIN watch --url http://127.0.0.1:$PORT --record $READS" \
      /dev/null > /dev/null 2>&1 &
  fi
  while kill -0 "$pid" 2>/dev/null; do
    kb="$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null || true)"
    hwm="$(awk '/^VmHWM:/ {print $2}' "/proc/$pid/status" 2>/dev/null || true)"
    [ -n "$kb" ] && printf '%s\t%s\t%s\n' "$(date +%s)" "$kb" "$hwm" >> "$RSS"
    sleep 1
  done
) &
SAMPLER=$!

status=0
BIN="$BIN" OUT="$OUT/$LABEL.tip-lag.json" PORT="$PORT" RPC_PORT="$RPC_PORT" \
  bash "$REPO/.github/workflows/tip-lag.sh" || status=$?
pkill -f -- "watch --url http://127.0.0.1:$PORT" 2>/dev/null || true
wait "$SAMPLER" 2>/dev/null || true

reads=0
[ -f "$READS" ] && reads="$(wc -l < "$READS" | tr -d ' ')"
jq -c --arg label "$LABEL" --argjson watch "$WATCH" --argjson reads "$reads" --rawfile rss "$RSS" '
  ($rss | split("\n") | map(select(length > 0) | split("\t") | map(tonumber))) as $s
  | ($s | map(.[1]) | sort) as $kb
  | {label: $label, watched: ($watch == 1), watcher_reads: $reads,
     after_seen_p50_ms: .after_seen_ms.p50, after_seen_p99_ms: .after_seen_ms.p99,
     lag_p50_ms: .lag_ms.p50, rss_samples: ($kb | length),
     rss_median_kib: $kb[(($kb | length) - 1) / 2 | floor], rss_max_kib: $kb[-1],
     hwm_kib: ($s | map(.[2]) | max)}
' "$OUT/$LABEL.tip-lag.json" | tee -a "$OUT/summary.jsonl"
exit "$status"
