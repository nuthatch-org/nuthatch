#!/usr/bin/env bash
# Prove what a public nest URL lets through and what it does not (#3028, #3031).
#
#   check-proxy.sh <base-url> [--burst]
#
# Black-box: only curl, so it runs the same against the proxy on a laptop (test-proxy.sh does) and
# against the live public URL after deploy. Exit 0 only if every check holds.
#
#   - the read routes and SQL and MCP answer;
#   - every write, admin and operator route is refused with 403 or 404 (`nuthatch dev --no-admin` has no
#     such routes; the proxy blocks them as well, so each lock is checked through the other's absence);
#   - a body over the limit is 413;
#   - with --burst, a flood from one address is answered 429 before it is served. That is the only check
#     that spends your own quota on the URL: run it from a machine you can afford to have limited for a
#     minute, never from the host's monitoring.
set -uo pipefail

base=${1:-}
burst=${2:-}
[ -n "$base" ] || { echo "usage: $0 <base-url> [--burst]" >&2; exit 2; }
base=${base%/}
fails=0

# expect <label> <allowed-code-regex> <curl args...>
expect() {
  local label=$1 want=$2; shift 2
  local got
  got=$(curl -s -o /dev/null -m 20 -w '%{http_code}' "$@" 2>/dev/null) || got=000
  if [[ $got =~ ^($want)$ ]]; then
    printf 'ok    %-46s %s\n' "$label" "$got"
  else
    printf 'FAIL  %-46s %s (wanted %s)\n' "$label" "$got" "$want"
    fails=$((fails + 1))
  fi
}

# body_has <label> <needle> <curl args...>
body_has() {
  local label=$1 needle=$2; shift 2
  local out
  out=$(curl -s -m 20 "$@" 2>/dev/null) || out=
  if [[ $out == *"$needle"* ]]; then
    printf 'ok    %-46s has %s\n' "$label" "$needle"
  else
    printf 'FAIL  %-46s lacks %s\n' "$label" "$needle"
    fails=$((fails + 1))
  fi
}

json=(-H 'content-type: application/json')

echo "== served"
expect "GET /"                       200 "$base/"
expect "GET /health"                 200 "$base/health"
expect "GET /ready"                  200 "$base/ready"
expect "GET /schema"                 200 "$base/schema"
expect "GET /tables"                 200 "$base/tables"
expect "GET /table/usdc__transfer"   200 "$base/table/usdc__transfer"
expect "GET /table/usdc__transfer?limit=5" 200 "$base/table/usdc__transfer?limit=5"
expect "GET /sql?q=SELECT 1"         200 -G "$base/sql" --data-urlencode 'q=SELECT 1 AS one'
expect "POST /sql"                   200 "${json[@]}" -d '{"q":"SELECT 1 AS one"}' "$base/sql"
body_has "POST /mcp tools/list"      '"tools"' "${json[@]}" -H 'accept: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' "$base/mcp"

echo "== refused: admin and operator surface"
# Against the host itself nginx answers these, so they are held to its exact answers. Behind the path URL on
# nuthatch-indexer.com (a /nests/ URL) Vercel answers first: a trailing-slash /_admin/ is a 404 and a double
# slash a 308 before either reaches the host. Both are refusals, so only that URL is let off the exact answer.
# That also means a run against the path URL does not exercise nginx's own refusal: run it against the host too.
admin_slash=403; dslash='403|404'
if [[ $base == */nests/* ]]; then admin_slash='403|404'; dslash='403|404|308'; fi
expect "GET /_admin/"                "$admin_slash" "$base/_admin/"
expect "GET /_admin"                 403 "$base/_admin"
expect "GET /_admin/config"          403 "$base/_admin/config"
expect "GET /_admin/events"          403 "$base/_admin/events"
expect "POST /_admin/nests"          403 -X POST "${json[@]}" -d '{"name":"x","nid":"y"}' "$base/_admin/nests"
expect "DELETE /_admin/nests/x"      403 -X DELETE "$base/_admin/nests/x"
expect "PUT /_admin/runtime"         403 -X PUT "${json[@]}" -d '{}' "$base/_admin/runtime"
expect "GET /metrics"                403 "$base/metrics"
expect "traversal /sql/../_admin/"   '403|404' --path-as-is "$base/sql/../_admin/config"
expect "encoded /%5fadmin/config"    '403|404' --path-as-is "$base/%5fadmin/config"
expect "double slash //_admin/"      "$dslash" --path-as-is "$base//_admin/config"

echo "== refused: routes not offered publicly"
for p in /nests /entities /entity/x /derived /derived/x /balances /balance/0x0 /exposure/0x0 /flags \
         /diff /at /queries /q/x /api /graphql /graph/status /sync /ipfs/gave-up /not-a-route; do
  expect "GET $p"                    '403|404' "$base$p"
done

echo "== refused: table route takes one plain table name"
expect "GET /table/"                 404 "$base/table/"
expect "GET /table/a/b"              404 "$base/table/a/b"
expect "GET /table/a.b"              404 "$base/table/a.b"
expect "GET /table/a%2Fb"            404 --path-as-is "$base/table/a%2Fb"
expect "GET /table/<encoded ctl>"    404 --path-as-is "$base/table/x%0d%0ay"

echo "== refused: methods"
for m in PUT DELETE PATCH; do
  expect "$m /sql"                   '403|405' -X "$m" "$base/sql"
done
expect "POST /"                      '403|405' -X POST "${json[@]}" -d '{}' "$base/"
expect "POST /tables"                '403|405' -X POST "${json[@]}" -d '{}' "$base/tables"
expect "GET /mcp"                    '403|405' "$base/mcp"
body_has "SQL write is refused"      'read-only' "${json[@]}" -d '{"q":"COPY (SELECT 1) TO '"'"'/tmp/x.csv'"'"'"}' "$base/sql"
expect "SQL write is a 4xx"          '4[0-9][0-9]' "${json[@]}" -d '{"q":"DELETE FROM usdc__transfer"}' "$base/sql"

echo "== limits"
big=$(head -c 100000 /dev/zero | tr '\0' 'a')
expect "100 KB body to /sql"         413 "${json[@]}" -d "{\"q\":\"SELECT '$big'\"}" "$base/sql"
expect "100 KB body to /mcp"         413 "${json[@]}" -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$big\"}" "$base/mcp"

if [ "$burst" = "--burst" ]; then
  echo "== flood from this address (spends its quota for about a minute)"
  codes=$(for _ in $(seq 1 80); do
    curl -s -o /dev/null -m 20 -w '%{http_code}\n' -G "$base/sql" --data-urlencode 'q=SELECT 1' &
  done; wait)
  n429=$(grep -c '^429$' <<<"$codes" || true)
  n200=$(grep -c '^200$' <<<"$codes" || true)
  if [ "$n429" -gt 0 ] && [ "$n200" -gt 0 ]; then
    printf 'ok    %-46s %s served, %s limited (429)\n' "80 parallel /sql from one address" "$n200" "$n429"
  else
    printf 'FAIL  %-46s %s served, %s limited (429)\n' "80 parallel /sql from one address" "$n200" "$n429"
    fails=$((fails + 1))
  fi
fi

echo
if [ "$fails" -eq 0 ]; then echo "all checks passed"; else echo "$fails check(s) FAILED"; exit 1; fi
