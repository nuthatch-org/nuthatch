#!/usr/bin/env bash
# The README's first three commands against the real chain, timed (#1719). Invoked weekly by
# live-smoke.yml; runs the same by hand.
#
#   scripts/live-smoke.sh                       # download and verify the latest release, then smoke it
#   NUTHATCH_BIN=./nuthatch scripts/live-smoke.sh   # smoke a binary you already have
#
# Each attempt: `init` mainnet USDC on the shipped keyless endpoints, `dev --backfill 300` in the
# background, wait for /ready, then one /sql that must return a row from usdc__transfer - all inside
# SMOKE_LIMIT_SECS measured from the start of `init`. Then SIGTERM, and `dev` must exit 0.
# SMOKE_ATTEMPTS attempts with backoff between them; only when every one fails is the run red.
#
# Env: NUTHATCH_BIN, SMOKE_LIMIT_SECS (180), SMOKE_ATTEMPTS (3), SMOKE_LISTEN (127.0.0.1:18288),
#      SMOKE_REPO (nuthatch-org/nuthatch), SMOKE_TAG (latest published release). Needs curl, jq, gh.

set -uo pipefail

LIMIT="${SMOKE_LIMIT_SECS:-180}"
ATTEMPTS="${SMOKE_ATTEMPTS:-3}"
LISTEN="${SMOKE_LISTEN:-127.0.0.1:18288}"
REPO="${SMOKE_REPO:-nuthatch-org/nuthatch}"
API="http://$LISTEN"
USDC=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
SHUTDOWN_SECS=30

for v in "$LIMIT" "$ATTEMPTS"; do
  case "$v" in
    ""|*[!0-9]*|0) echo "::error::SMOKE_LIMIT_SECS and SMOKE_ATTEMPTS must be positive integers, got '$v'"; exit 2 ;;
  esac
done

tmp="${TMPDIR:-/tmp}"
WORK=$(mktemp -d "${tmp%/}/live-smoke.XXXXXX")
DEV_PID=""
INIT_PID=""

cleanup() {
  [ -n "$INIT_PID" ] && kill -KILL "$INIT_PID" 2>/dev/null
  [ -n "$DEV_PID" ] && kill -KILL "$DEV_PID" 2>/dev/null
  rm -rf "$WORK"
}
# bash 3.2 exits 0 from an EXIT trap unless the status is carried through by hand.
trap 'rc=$?; cleanup; exit $rc' EXIT
trap 'exit 130' INT TERM

err() { echo "::error::$*"; }

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi \
    | awk '{ print $1 }'
}

fetch_release() {
  local target tag tarball want got
  case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) target=x86_64-unknown-linux-gnu ;;
    Darwin/arm64) target=aarch64-apple-darwin ;;
    Darwin/x86_64) target=x86_64-apple-darwin ;;
    *) err "no published binary for $(uname -s)/$(uname -m); set NUTHATCH_BIN"; exit 1 ;;
  esac
  tag="${SMOKE_TAG:-$(gh release view --repo "$REPO" --json tagName --jq .tagName)}"
  if [ -z "$tag" ]; then
    err "could not resolve the latest release of $REPO"
    exit 1
  fi
  tarball="nuthatch-$target.tar.gz"
  echo "== release $tag, $tarball =="
  if ! gh release download "$tag" --repo "$REPO" --dir "$WORK/rel" \
    --pattern "$tarball" --pattern "$tarball.sha256"; then
    err "could not download $tarball from $REPO $tag"
    exit 1
  fi
  want=$(awk '{ print $1 }' "$WORK/rel/$tarball.sha256")
  got=$(sha256 "$WORK/rel/$tarball")
  if [ -z "$want" ] || [ "$want" != "$got" ]; then
    err "$tarball sha256 is $got, the release sidecar says '$want'"
    exit 1
  fi
  echo "sha256 ok: $got"
  if ! gh attestation verify "$WORK/rel/$tarball" --repo "$REPO" \
    --signer-workflow "$REPO/.github/workflows/release.yml" --source-ref "refs/tags/$tag"; then
    err "$tarball has no provenance attestation from $REPO's release workflow at $tag that verifies"
    exit 1
  fi
  echo "attestation ok: built by $REPO's release workflow at $tag"
  tar -xzf "$WORK/rel/$tarball" -C "$WORK/rel"
  NUTHATCH_BIN="$WORK/rel/nuthatch"
}

# One attempt. Returns 0 on a pass; on a failure prints why and returns 1, leaving nothing running.
attempt() {
  local n="$1" dir="$WORK/nest-$1" start elapsed code body="" count rc i
  mkdir -p "$dir"
  start=$(date +%s)
  over() { [ $(( $(date +%s) - start )) -ge "$LIMIT" ]; }
  remaining() { echo $(( LIMIT - ($(date +%s) - start) )); }
  fail() {
    err "attempt $n/$ATTEMPTS: $* (after $(( $(date +%s) - start ))s, limit ${LIMIT}s)"
    if [ -f "$dir/init.log" ]; then echo "--- init log (tail) ---"; tail -n 20 "$dir/init.log"; fi
    if [ -f "$dir/dev.log" ]; then echo "--- dev log (tail) ---"; tail -n 30 "$dir/dev.log"; fi
    [ -n "$INIT_PID" ] && kill -KILL "$INIT_PID" 2>/dev/null && wait "$INIT_PID" 2>/dev/null
    [ -n "$DEV_PID" ] && kill -KILL "$DEV_PID" 2>/dev/null && wait "$DEV_PID" 2>/dev/null
    INIT_PID=""
    DEV_PID=""
    return 1
  }

  echo "== attempt $n/$ATTEMPTS: init $USDC on mainnet =="
  "$NUTHATCH_BIN" init "$USDC" --chain mainnet --alias usdc --dir "$dir" >"$dir/init.log" 2>&1 &
  INIT_PID=$!
  while kill -0 "$INIT_PID" 2>/dev/null; do
    over && { fail "init did not finish inside the wall clock"; return 1; }
    sleep 1
  done
  wait "$INIT_PID"
  rc=$?
  INIT_PID=""
  [ "$rc" -eq 0 ] || { fail "init exited $rc"; return 1; }
  echo "init done at $(( $(date +%s) - start ))s"

  # Another process on the port would answer /ready and /sql for a nest that is not ours.
  if curl -s -o /dev/null --max-time 2 "$API/ready"; then
    fail "something is already listening on $LISTEN; set SMOKE_LISTEN"
    return 1
  fi
  (cd "$dir" && exec "$NUTHATCH_BIN" dev --backfill 300 --listen "$LISTEN") >"$dir/dev.log" 2>&1 &
  DEV_PID=$!

  code=""
  while [ "$code" != 200 ]; do
    kill -0 "$DEV_PID" 2>/dev/null || { fail "dev exited before /ready answered"; return 1; }
    over && { fail "/ready did not answer 200 inside the wall clock (last status '${code:-none}')"; return 1; }
    code=$(curl -s -o "$dir/ready.json" -w '%{http_code}' --max-time "$(remaining)" "$API/ready")
    [ "$code" = 200 ] || sleep 1
  done
  echo "/ready 200 at $(( $(date +%s) - start ))s"

  # /ready can precede the first decoded block, so ask until a row is there or the clock runs out.
  count=0
  while :; do
    kill -0 "$DEV_PID" 2>/dev/null || { fail "dev exited before /sql returned a row"; return 1; }
    over && { fail "/sql returned no row from usdc__transfer inside the wall clock (last body: ${body:-none})"; return 1; }
    body=$(curl -s --max-time "$(remaining)" \
      "$API/sql?q=SELECT%20block_number%20FROM%20usdc__transfer%20LIMIT%201")
    count=$(printf "%s" "$body" | jq -r ".count // 0" 2>/dev/null)
    [ "${count:-0}" -ge 1 ] && break
    sleep 1
  done
  elapsed=$(( $(date +%s) - start ))
  over && { fail "a row arrived, but after the wall clock"; return 1; }
  echo "/sql returned $count row(s) from usdc__transfer at ${elapsed}s"

  kill -TERM "$DEV_PID"
  i=0
  while kill -0 "$DEV_PID" 2>/dev/null; do
    if [ "$i" -ge "$SHUTDOWN_SECS" ]; then
      fail "dev did not exit within ${SHUTDOWN_SECS}s of SIGTERM"
      return 1
    fi
    sleep 1
    i=$((i + 1))
  done
  wait "$DEV_PID"
  rc=$?
  DEV_PID=""
  [ "$rc" -eq 0 ] || { fail "dev exited $rc on SIGTERM, not 0"; return 1; }
  echo "dev exited 0 on SIGTERM"
  echo "PASS: init to first row in ${elapsed}s (limit ${LIMIT}s), attempt $n/$ATTEMPTS"
  return 0
}

if [ -z "${NUTHATCH_BIN:-}" ]; then
  fetch_release
fi
"$NUTHATCH_BIN" --version || { err "$NUTHATCH_BIN does not run"; exit 1; }

for n in $(seq 1 "$ATTEMPTS"); do
  attempt "$n" && exit 0
  if [ "$n" -lt "$ATTEMPTS" ]; then
    backoff=$((n * 15))
    echo "attempt $n/$ATTEMPTS failed, retrying in ${backoff}s"
    sleep "$backoff"
  fi
done
err "live smoke failed $ATTEMPTS/$ATTEMPTS attempts: init, dev and a first row from mainnet did not fit in ${LIMIT}s"
exit 1
