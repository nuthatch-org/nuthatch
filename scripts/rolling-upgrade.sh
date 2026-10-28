#!/usr/bin/env bash
# rolling-upgrade.sh - RFC-0073 FR7's release-gate step: take a runtime from one release to the next
# with a query answered at every step.
#
#   scripts/rolling-upgrade.sh <previous-binary> <current-binary>
#   scripts/rolling-upgrade.sh --previous <tag> <current-binary>
#
# A runtime with one nest over the fixture chain (scripts/fixture-rpc.sh) runs the previous binary as
# its writer (`dev`) and two followers (`serve --runtime --follow`). Then, in order: follower one is
# replaced by the current binary, follower two likewise, and the writer last. A prober asks both
# followers the same count every 200 ms, as a load balancer would, and every probe must be answered
# correctly by at least one of them. After each step the gate also waits for the followers to report
# the writer connected and no generations behind.
#
# --previous TAG downloads that release's asset for GATE_TARGET (default x86_64-unknown-linux-gnu)
# with gh and checks it against its .sha256. A previous release that predates followers cannot run
# the first step; the gate says so and exits 2 (a setup fault), unless ROLLING_SAME=1, which runs the
# current binary in both roles to prove the procedure until a release that can follow exists.
#
# Exit 0 PASS; 1 FAIL (a probe nobody answered, or a step that never became ready); 2 setup fault.
# The run's logs stay in ROLLING_OUT (default: a temp dir, printed).
set -euo pipefail

CONTRACT=0x000000000000000000000000000000000000c0de
TRANSFERS=8
READY_SECS=${ROLLING_READY_SECS:-120}
target=${GATE_TARGET:-x86_64-unknown-linux-gnu}
repo=${GATE_REPO:-nuthatch-org/nuthatch}

die() { echo "rolling-upgrade: $*" >&2; exit 2; }
fail() { echo "rolling-upgrade: FAIL: $*" >&2; exit 1; }
say() { echo "rolling-upgrade: $*" >&2; }

previous_tag=""
if [ "${1:-}" = "--previous" ]; then
  [ $# -ge 2 ] || die "--previous needs a tag"
  previous_tag=$2
  shift 2
fi
if [ -n "$previous_tag" ]; then
  [ $# -eq 1 ] || die "usage: $0 --previous <tag> <current-binary>"
  current=$1
else
  [ $# -eq 2 ] || die "usage: $0 <previous-binary> <current-binary>"
  previous=$1
  current=$2
fi
[ -x "$current" ] || die "no binary at $current"

out=${ROLLING_OUT:-$(mktemp -d)}
mkdir -p "$out"
say "logs in $out"

pids=""
cleanup() {
  for p in $pids; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

if [ -n "$previous_tag" ]; then
  asset=nuthatch-$target.tar.gz
  mkdir -p "$out/previous"
  gh release download "$previous_tag" -R "$repo" -p "$asset" -p "$asset.sha256" -D "$out/previous" \
    || die "could not download $asset from $previous_tag"
  if command -v sha256sum >/dev/null; then
    (cd "$out/previous" && sha256sum -c "$asset.sha256" >/dev/null)
  else
    (cd "$out/previous" && shasum -a 256 -c "$asset.sha256" >/dev/null)
  fi || die "$asset from $previous_tag fails its checksum"
  tar -xzf "$out/previous/$asset" -C "$out/previous"
  previous=$(find "$out/previous" -type f -name nuthatch | head -n 1)
  [ -n "$previous" ] || die "no nuthatch binary inside $asset"
  chmod +x "$previous"
fi
[ -x "$previous" ] || die "no binary at $previous"

if ! "$previous" serve --help 2>/dev/null | grep -q -- '--follow'; then
  if [ "${ROLLING_SAME:-}" = 1 ]; then
    say "the previous binary cannot follow; ROLLING_SAME=1, so the current binary plays both roles"
    previous=$current
  else
    die "the previous binary ($("$previous" --version 2>/dev/null)) predates followers (RFC-0073 FR1); \
there is nothing to upgrade without an outage yet. ROLLING_SAME=1 runs the procedure on one binary"
  fi
fi

free_port() {
  local p
  while :; do
    p=$((20000 + RANDOM % 30000))
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
      echo "$p"
      return
    fi
  done
}

rpc_bin=$(bash "$(dirname "$0")/fixture-rpc.sh")
rpc_port=$(free_port)
"$rpc_bin" scripted --port "$rpc_port" --contract "$CONTRACT" --tip 9 --finalized 8 \
  >"$out/rpc.log" 2>&1 &
pids="$pids $!"

rt=$out/runtime
mkdir -p "$rt/nests"
cat >"$out/erc20.abi.json" <<'EOF'
[{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]
EOF
waited=0
until "$current" init "$CONTRACT" --chain arbitrum-one --rpc "http://127.0.0.1:$rpc_port/" \
  --abi "$out/erc20.abi.json" --dir "$rt/nests/gate" >"$out/init.log" 2>&1; do
  waited=$((waited + 1))
  [ "$waited" -lt 20 ] || die "init failed: $(tail -n 5 "$out/init.log")"
  rm -rf "$rt/nests/gate"
  sleep 0.5
done
cat >"$rt/roost.toml" <<EOF
[roost]
name = "rolling"
nests = ["gate"]
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = ["http://127.0.0.1:$rpc_port/"]
EOF
"$current" migrate --dir "$rt" >"$out/migrate.log" 2>&1 || die "migrate failed: $(tail -n 5 "$out/migrate.log")"

NUTHATCH_ADMIN_TOKEN=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')
export NUTHATCH_ADMIN_TOKEN

writer_port=$(free_port)
writer_url=http://127.0.0.1:$writer_port
start_writer() {
  "$1" dev --dir "$rt" --listen "127.0.0.1:$writer_port" --poll-interval 1s >>"$out/writer.log" 2>&1 &
  writer_pid=$!
  pids="$pids $writer_pid"
}

f_port1=$(free_port)
f_port2=$(free_port)
start_follower() { # <binary> <port> <log>
  "$1" serve --runtime "$rt" --follow "$writer_url" --listen "127.0.0.1:$2" >>"$out/$3.log" 2>&1 &
  echo $!
}

json_get() { curl -fsS -m 5 "$1" 2>/dev/null; }

count_of() { # <base> -> the count, or nothing
  json_get "$1/gate/sql?q=SELECT%20count(*)%20AS%20n%20FROM%20$table" \
    | sed -n 's/.*"rows":\[{"n":\([0-9]*\)}\].*/\1/p'
}

wait_for() { # <what> <command...>
  local what=$1 t=0
  shift
  until "$@"; do
    t=$((t + 1))
    [ "$t" -lt $((READY_SECS * 4)) ] || fail "$what was not reached in ${READY_SECS}s"
    sleep 0.25
  done
  say "ok: $what"
}

writer_answers() { [ "$(count_of "$writer_url")" = "$TRANSFERS" ]; }
follower_caught_up() { # <port>
  local r
  r=$(json_get "http://127.0.0.1:$1/gate/ready") || return 1
  printf '%s' "$r" | grep -q '"writer":"connected"' && printf '%s' "$r" | grep -q '"lag_generations":0'
}

say "previous: $("$previous" --version 2>/dev/null), current: $("$current" --version 2>/dev/null)"
start_writer "$previous"
writer_ready() { json_get "$writer_url/ready" >/dev/null; }
wait_for "the writer is ready" writer_ready
table=$(json_get "$writer_url/gate/tables" | sed -n 's/.*"tables":\[{"table":"\([a-z0-9_]*\)".*/\1/p')
[ -n "$table" ] || die "the writer lists no table"
wait_for "the writer answers $TRANSFERS transfers" writer_answers

f1=$(start_follower "$previous" "$f_port1" follower1)
pids="$pids $f1"
f2=$(start_follower "$previous" "$f_port2" follower2)
pids="$pids $f2"
wait_for "follower one has caught up" follower_caught_up "$f_port1"
wait_for "follower two has caught up" follower_caught_up "$f_port2"

probe_log=$out/probes.log
: >"$probe_log"
(
  # A follower being replaced answers nothing; that is what the other one is for.
  set +e
  while :; do
    a=$(count_of "http://127.0.0.1:$f_port1")
    b=$(count_of "http://127.0.0.1:$f_port2")
    if [ "$a" = "$TRANSFERS" ] || [ "$b" = "$TRANSFERS" ]; then
      echo "ok $(date +%s) $a $b" >>"$probe_log"
    else
      echo "FAIL $(date +%s) one=${a:-none} two=${b:-none}" >>"$probe_log"
    fi
    sleep 0.2
  done
) 2>>"$out/prober.err" &
prober=$!
pids="$pids $prober"

say "replacing follower one"
kill "$f1"
wait "$f1" 2>/dev/null || true
f1=$(start_follower "$current" "$f_port1" follower1)
pids="$pids $f1"
wait_for "the upgraded follower one has caught up" follower_caught_up "$f_port1"

say "replacing follower two"
kill "$f2"
wait "$f2" 2>/dev/null || true
f2=$(start_follower "$current" "$f_port2" follower2)
pids="$pids $f2"
wait_for "the upgraded follower two has caught up" follower_caught_up "$f_port2"

say "replacing the writer"
kill "$writer_pid"
wait "$writer_pid" 2>/dev/null || true
sleep 2
start_writer "$current"
wait_for "the upgraded writer answers $TRANSFERS transfers" writer_answers
wait_for "follower one follows the upgraded writer" follower_caught_up "$f_port1"
wait_for "follower two follows the upgraded writer" follower_caught_up "$f_port2"

kill "$prober"
wait "$prober" 2>/dev/null || true
probes=$(wc -l <"$probe_log" | tr -d ' ')
failed=$(grep -c '^FAIL' "$probe_log" || true)
[ "$probes" -gt 0 ] || fail "the prober recorded nothing"
if [ "$failed" -gt 0 ]; then
  grep '^FAIL' "$probe_log" | head -n 5 >&2
  fail "$failed of $probes probes went unanswered"
fi
say "PASS: $probes probes across three replacements, every one answered"
