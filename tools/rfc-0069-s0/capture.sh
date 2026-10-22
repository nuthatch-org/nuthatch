#!/usr/bin/env bash
#
# RFC-0069 S0: record a running nest's /ready, /, /metrics and /nests bodies, one JSON object per
# read, in the shape `nuthatch watch --replay` reads. At the watcher's own defaults: /ready and /
# every second, /metrics every two, /nests every thirty.
#
# usage: capture.sh <url> <seconds> <out.jsonl>
set -euo pipefail

URL="${1%/}"
SECS="$2"
OUT="$3"
BODY="$(mktemp)"
trap 'rm -f "$BODY"' EXIT

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time() * 1000'; }

read_route() {
  local t status
  t="$(now_ms)"
  status="$(curl -s -m 5 -o "$BODY" -w '%{http_code}' "$URL$1" || true)"
  [ -n "$status" ] || status=0
  jq -nc --argjson t "$t" --arg route "$1" --argjson status "$status" --rawfile body "$BODY" \
    '{t_ms: $t, route: $route, status: $status, body: $body}' >> "$OUT"
  : > "$BODY"
}

start="$(now_ms)"
for i in $(seq 0 $(( SECS - 1 ))); do
  read_route /ready
  read_route /
  if [ $(( i % 2 )) -eq 0 ]; then read_route /metrics; fi
  if [ $(( i % 30 )) -eq 0 ]; then read_route /nests; fi
  next=$(( start + (i + 1) * 1000 ))
  wait_ms=$(( next - $(now_ms) ))
  if [ "$wait_ms" -gt 0 ]; then sleep "$(printf '%d.%03d' $(( wait_ms / 1000 )) $(( wait_ms % 1000 )))"; fi
done
echo "captured $(wc -l < "$OUT" | tr -d ' ') reads over ${SECS} s into $OUT"
