#!/usr/bin/env bash
# Run nginx.conf for real and prove its limits (#3028, #3031). Needs nginx and python3; nothing else,
# no root, no network. Used before a deploy and after editing nginx.conf.
#
#   test-proxy.sh                        against a stub that answers 200 to everything
#   UPSTREAM=127.0.0.1:8120 test-proxy.sh   against a real nest already running there
#   CONF=nginx-base-usdc.conf test-proxy.sh  the same checks against the hosted-nest template
#
# With the stub, every 403, 404 and 413 can only have come from the proxy. With a real nest it also
# shows the nest's own answers pass through.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Not named NGINX: nginx reads that variable as a list of inherited sockets.
nginx_bin=${NGINX_BIN:-$(command -v nginx || true)}
[ -n "$nginx_bin" ] && [ -x "$nginx_bin" ] || { echo "no nginx; set NGINX_BIN=/path/to/nginx" >&2; exit 2; }

work=$(mktemp -d)
pids=()
cleanup() { for p in "${pids[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; rm -rf "$work"; }
trap cleanup EXIT

proxy_port=${PROXY_PORT:-18480}
stub_port=${STUB_PORT:-18120}
upstream=${UPSTREAM:-127.0.0.1:$stub_port}
conf=${CONF:-nginx.conf}
[ -f "$here/$conf" ] || { echo "no such config: $here/$conf" >&2; exit 2; }

if [ -z "${UPSTREAM:-}" ]; then
  python3 -I "$here/stub-upstream.py" "$stub_port" &
  pids+=($!)
fi

# The lines marked TEST-* (and the /run/nuthatch-public paths) are the only ones this rewrites.
mkdir -p "$work/run/tmp"
sed -e "s#/run/nuthatch-public[-a-z0-9]*#$work/run#g" \
    -e "/TEST-LOG/s#.*#    access_log off;#" \
    -e "/TEST-ERRLOG/s#.*#error_log $work/error.log warn;#" \
    -e "/TEST-UPSTREAM/s#server [0-9.:]*;#server $upstream;#" \
    -e "/TEST-LISTEN/s#listen [0-9.:]*;#listen 127.0.0.1:$proxy_port;#" \
    "$here/$conf" > "$work/nginx.conf"

"$nginx_bin" -t -c "$work/nginx.conf" -e "$work/error.log" >"$work/t.out" 2>&1 \
  || { cat "$work/t.out"; exit 1; }
"$nginx_bin" -c "$work/nginx.conf" -e "$work/error.log" &
pids+=($!)

ready=
for _ in $(seq 1 50); do
  [ "$(curl -s -o /dev/null -m 1 -w '%{http_code}' "http://127.0.0.1:$proxy_port/health")" = 200 ] && { ready=1; break; }
  sleep 0.2
done
[ -n "$ready" ] || { echo "the proxy never answered /health 200 (see $work/error.log)" >&2; cat "$work/error.log" >&2; exit 1; }

base="http://127.0.0.1:$proxy_port"
status=0
"$here/check-proxy.sh" "$base" || status=1

if [ -z "${UPSTREAM:-}" ]; then
  echo "== timeout backstop"
  start=$(date +%s)
  code=$(curl -s -o /dev/null -m 30 -w '%{http_code}' -G "$base/sql" --data-urlencode 'q=slow') || code=000
  took=$(( $(date +%s) - start ))
  if [ "$code" = 504 ] && [ "$took" -le 18 ]; then
    printf 'ok    %-46s %s after %ss\n' "a hung nest is cut off at ~15 s" "$code" "$took"
  else
    printf 'FAIL  %-46s %s after %ss (wanted 504 by 18 s)\n' "a hung nest is cut off at ~15 s" "$code" "$took"
    status=1
  fi
fi

# #3138: a tool calls its route in process, past nginx, so the env file's NUTHATCH_MCP_TOOLS may name
# only tools whose route this proxy serves. A tool with no route listed here is a failure by design:
# add its route to nginx.conf on purpose first.
echo "== mcp tools match the allowed routes"
case "$conf" in
  nginx-base-usdc.conf) envfile=base-usdc.env.example ;;
  *) envfile=arc-testnet.env.example ;;
esac
tools=$(sed -n 's/^NUTHATCH_MCP_TOOLS=//p' "$here/$envfile")
if [ -z "$tools" ]; then
  printf 'FAIL  %-46s %s sets no NUTHATCH_MCP_TOOLS\n' "public /mcp offers a bounded tool list" "$envfile"
  status=1
fi
for tool in ${tools//,/ }; do
  case "$tool" in
    status) path=/ ;;   schema) path=/schema ;;   tables) path=/tables ;;
    table) path=/table/t ;;   sql) path='/sql?q=select' ;;   explain) path='/explain?q=select' ;;
    *) printf 'FAIL  %-46s %s\n' "tool $tool" "no route the proxy serves"; status=1; continue ;;
  esac
  code=$(curl -s -o /dev/null -m 5 -w '%{http_code}' "$base$path") || code=000
  if [ "$code" = 404 ] || [ "$code" = 000 ]; then
    printf 'FAIL  %-46s %s answers %s\n' "tool $tool" "$path" "$code"; status=1
  else
    printf 'ok    %-46s %s answers %s\n' "tool $tool" "$path" "$code"
  fi
done
for route in /entity/x /diff /at /balance/x /balances; do
  code=$(curl -s -o /dev/null -m 5 -w '%{http_code}' "$base$route") || code=000
  [ "$code" = 404 ] || { printf 'FAIL  %-46s %s answers %s\n' "withheld route" "$route" "$code"; status=1; }
done

# Last, because it spends the limiter: a fresh 80-way flood from 127.0.0.1.
echo "== abuse limits"
sleep 62   # let the per-IP buckets above refill
"$here/check-proxy.sh" "$base" --burst | sed -n '/== flood/,$p' || status=1
[ "${PIPESTATUS[0]}" -eq 0 ] || status=1

exit "$status"
