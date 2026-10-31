#!/usr/bin/env bash
# failover-gate.sh - RFC-0078's two-process gate: two writers, one dataset, one lease in the mirror.
#
#   scripts/failover-gate.sh <nuthatch-binary>
#
# Writer A and writer B run the same runtime from separate directories, publishing to one mirror
# with failover on. In order:
#
#   1. A boots, takes the lease at fence 1 and indexes the fixture chain.
#   2. B boots while A renews and must refuse to start, naming A.
#   3. A is frozen with SIGSTOP. B boots again, waits the lease out, and takes it at fence 2.
#   4. A is resumed. It must find the lease taken, fence its store off and say so on /ready, and the
#      mirror's lease must still be B's.
#
# Exit 0 PASS; 1 FAIL; 2 setup fault. The run's logs stay in FAILOVER_OUT (default: a temp dir).
set -euo pipefail

CONTRACT=0x000000000000000000000000000000000000c0de
TRANSFERS=8
TERM_SECS=3
READY_SECS=${FAILOVER_READY_SECS:-120}

die() { echo "failover-gate: $*" >&2; exit 2; }
fail() { echo "failover-gate: FAIL: $*" >&2; exit 1; }
say() { echo "failover-gate: $*" >&2; }

[ $# -eq 1 ] || die "usage: $0 <nuthatch-binary>"
bin=$1
[ -x "$bin" ] || die "no binary at $bin"

out=${FAILOVER_OUT:-$(mktemp -d)}
mkdir -p "$out"
say "logs in $out"

pids=""
cleanup() {
  for p in $pids; do
    kill -CONT "$p" 2>/dev/null || true
    kill "$p" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

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

json_get() { curl -fsS -m 5 "$1" 2>/dev/null; }
body_of() { curl -sS -m 5 "$1" 2>/dev/null || true; }

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

rpc_bin=$(bash "$(dirname "$0")/fixture-rpc.sh")
rpc_port=$(free_port)
"$rpc_bin" scripted --port "$rpc_port" --contract "$CONTRACT" --tip 9 --finalized 8 \
  >"$out/rpc.log" 2>&1 &
pids="$pids $!"

rt=$out/a
mkdir -p "$rt/nests"
cat >"$out/erc20.abi.json" <<'EOF'
[{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]
EOF
waited=0
until "$bin" init "$CONTRACT" --chain arbitrum-one --rpc "http://127.0.0.1:$rpc_port/" \
  --abi "$out/erc20.abi.json" --dir "$rt/nests/gate" >"$out/init.log" 2>&1; do
  waited=$((waited + 1))
  [ "$waited" -lt 20 ] || die "init failed: $(tail -n 5 "$out/init.log")"
  rm -rf "$rt/nests/gate"
  sleep 0.5
done
cat >"$rt/roost.toml" <<EOF
[roost]
name = "failover"
nests = ["gate"]
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = ["http://127.0.0.1:$rpc_port/"]
EOF
"$bin" migrate --dir "$rt" >"$out/migrate.log" 2>&1 || die "migrate failed: $(tail -n 5 "$out/migrate.log")"
mirror=${FAILOVER_MIRROR:-$out/mirror}
cat >>"$rt/mounts.toml" <<EOF

[runtime.publish]
target = "$mirror"
interval = "1s"

[runtime.publish.failover]
term = "${TERM_SECS}s"
EOF
cp -R "$rt" "$out/b"

start_writer() { # <dir> <port> <log>
  "$bin" dev --dir "$1" --listen "127.0.0.1:$2" --poll-interval 1s >>"$out/$3.log" 2>&1 &
  echo $!
}

lease_file() { find "$mirror" -name lease.json 2>/dev/null | head -n 1; }
lease_field() { # <field>
  local f
  f=$(lease_file)
  [ -n "$f" ] || return 0
  grep -Eo "\"$1\":(\"[^\"]*\"|[0-9]+|null)" "$f" | head -n 1 | cut -d: -f2- | tr -d '"'
}

a_port=$(free_port)
a=$(start_writer "$rt" "$a_port" writer-a)
pids="$pids $a"
a_ready() { json_get "http://127.0.0.1:$a_port/ready" >/dev/null; }
wait_for "writer A is ready" a_ready
table=$(json_get "http://127.0.0.1:$a_port/gate/tables" | sed -n 's/.*"tables":\[{"table":"\([a-z0-9_]*\)".*/\1/p')
[ -n "$table" ] || die "writer A lists no table"
a_answers() {
  local n
  n=$(json_get "http://127.0.0.1:$a_port/gate/sql?q=SELECT%20count(*)%20AS%20n%20FROM%20$table" \
    | sed -n 's/.*"rows":\[{"n":\([0-9]*\)}\].*/\1/p')
  [ "$n" = "$TRANSFERS" ]
}
wait_for "writer A answers $TRANSFERS transfers" a_answers
[ "$(lease_field fence)" = 1 ] || fail "writer A should hold fence 1, the lease says $(cat "$(lease_file)")"
a_owner=$(lease_field owner)
say "ok: writer A holds the lease at fence 1 as $a_owner"

# 2. A second writer while A renews: refused, naming A.
b_port=$(free_port)
b=$(start_writer "$out/b" "$b_port" writer-b-refused)
t=0
while kill -0 "$b" 2>/dev/null; do
  t=$((t + 1))
  [ "$t" -lt 120 ] || { kill "$b"; fail "writer B kept running while A renewed the lease"; }
  sleep 0.25
done
grep -q "still renewing" "$out/writer-b-refused.log" \
  || fail "writer B stopped without the lease refusal: $(tail -n 5 "$out/writer-b-refused.log")"
grep -q "$a_owner" "$out/writer-b-refused.log" || fail "the refusal does not name writer A"
say "ok: writer B refused while A renews"

# 3. Freeze A; B waits the lease out and takes it.
kill -STOP "$a"
say "writer A frozen"
b=$(start_writer "$out/b" "$b_port" writer-b)
pids="$pids $b"
b_holds() { [ "$(lease_field fence)" = 2 ] && [ "$(lease_field owner)" != "$a_owner" ]; }
wait_for "writer B takes the lease at fence 2" b_holds
b_owner=$(lease_field owner)
b_ready() { json_get "http://127.0.0.1:$b_port/ready" >/dev/null; }
wait_for "writer B is ready" b_ready

# 4. Resume A: it must find the lease gone and stop.
kill -CONT "$a"
say "writer A resumed"
a_stopped() {
  grep -q "lost the writer lease" "$out/writer-a.log" \
    && body_of "http://127.0.0.1:$a_port/gate/ready" | grep -q "lost the writer lease"
}
wait_for "writer A reports the lost lease and stops" a_stopped
a_publisher_stopped() { grep -q "publishing gate stopped" "$out/writer-a.log"; }
wait_for "writer A's publisher stops" a_publisher_stopped
! grep -q "dead-lettered" "$out/writer-a.log" || fail "writer A's publisher retried a lost lease"
sleep $((TERM_SECS * 2))
[ "$(lease_field owner)" = "$b_owner" ] && [ "$(lease_field fence)" = 2 ] \
  || fail "the lease moved after A resumed: $(cat "$(lease_file)")"
say "PASS: one writer at every fence, and the paused one stopped on waking"
