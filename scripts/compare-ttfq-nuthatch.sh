#!/usr/bin/env bash
# The Nuthatch side of docs/comparison-time-to-first-query.md (#2979), from an empty directory.
#
#   scripts/compare-ttfq-nuthatch.sh                       # 5.22.0, eth.drpc.org, port 8301
#   VERSION=5.22.0 RPC=https://eth.drpc.org PORT=8301 RUNS=3 scripts/compare-ttfq-nuthatch.sh
#
# Downloads the release binary, checks its .sha256, scaffolds WETH with `init`, runs `nuthatch dev`,
# and prints one line per run: seconds to the first /sql answer carrying a row, seconds until the
# cursor has passed the range's end, and the event count per table over the range. Needs curl, jq,
# shasum and a `date` that knows %N (macOS 13+, GNU).
set -euo pipefail

VERSION="${VERSION:-5.22.0}"
RPC="${RPC:-https://eth.drpc.org}"
PORT="${PORT:-8301}"
RUNS="${RUNS:-1}"
WETH=0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
FROM=26400000
TO=26400999
# eth_getLogs over the range, counted per topic0 on 2026-11-17 against the same RPC.
EXPECT_TOTAL=49354

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) target=aarch64-apple-darwin ;;
  Darwin-x86_64) target=x86_64-apple-darwin ;;
  Linux-x86_64) target=x86_64-unknown-linux-gnu ;;
  Linux-aarch64) target=aarch64-unknown-linux-gnu ;;
  *) echo "no prebuilt binary for $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

work="${WORK:-$(mktemp -d)}"
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.2f", b - a }'; }
sql() { curl -sf -G "http://127.0.0.1:$PORT/sql" --data-urlencode "q=$1"; }

if curl -s -o /dev/null "http://127.0.0.1:$PORT/ready"; then
  echo "port $PORT is already answering; pick another with PORT=" >&2; exit 1
fi

t=$(now)
mkdir -p "$work/bin"
tarball="nuthatch-$target.tar.gz"
base="https://github.com/nuthatch-org/nuthatch/releases/download/v$VERSION"
curl -fsSL -o "$work/bin/$tarball" "$base/$tarball"
curl -fsSL -o "$work/bin/$tarball.sha256" "$base/$tarball.sha256"
want=$(awk '{print $1}' "$work/bin/$tarball.sha256")
got=$(shasum -a 256 "$work/bin/$tarball" | awk '{print $1}')
[ "$want" = "$got" ] || { echo "sha256 mismatch: $got, release says $want" >&2; exit 1; }
tar -xzf "$work/bin/$tarball" -C "$work/bin"
bin="$work/bin/nuthatch"
echo "install: $(since "$t") s, $("$bin" --version), sha256 $got"

range="block_number between $FROM and $TO"
count_q="select 'transfer' t, count(*) n from weth9__transfer where $range
  union all select 'deposit', count(*) from weth9__deposit where $range
  union all select 'withdrawal', count(*) from weth9__withdrawal where $range
  union all select 'approval', count(*) from weth9__approval where $range"

for run in $(seq 1 "$RUNS"); do
  nest="$work/run$run"
  mkdir -p "$nest"
  t0=$(now)
  (cd "$nest" && "$bin" init "$WETH" --chain mainnet --rpc "$RPC" --start-block "$FROM" </dev/null) \
    > "$nest/init.log" 2>&1
  init_s=$(since "$t0")

  t1=$(now)
  (cd "$nest" && exec "$bin" dev --listen "127.0.0.1:$PORT") > "$nest/dev.log" 2>&1 &
  pid=$!
  trap 'kill $pid 2>/dev/null || true' EXIT

  # Done is judged from the answer itself: /sql reports the block it answered as of, which can trail
  # /ready's last_block by a window, so a count taken on /ready's word can come up short.
  first="" answer=""
  while :; do
    kill -0 $pid 2>/dev/null || { echo "nuthatch dev exited; see $nest/dev.log" >&2; exit 1; }
    answer=$(sql "$count_q" 2>/dev/null || true)
    if [ -n "$answer" ]; then
      n=$(jq '[.rows[].n] | add // 0' <<<"$answer")
      [ -z "$first" ] && [ "$n" -gt 0 ] && first=$(since "$t1")
      [ -n "$first" ] && [ "$(jq '.provenance.as_of // 0' <<<"$answer")" -ge "$TO" ] && break
    fi
    sleep 0.2
  done
  backfill=$(since "$t1")

  counts=$(jq -r '[.rows[] | "\(.t)=\(.n)"] | sort | join(" ")' <<<"$answer")
  total=$(jq '[.rows[].n] | add' <<<"$answer")
  kill $pid; wait $pid 2>/dev/null || true
  trap - EXIT

  verdict=ok
  [ "$total" = "$EXPECT_TOTAL" ] || verdict="MISMATCH, expected $EXPECT_TOTAL"
  echo "run $run: init ${init_s} s, first query ${first} s, range indexed ${backfill} s after dev started; $counts total=$total ($verdict)"
done
echo "logs: $work"
