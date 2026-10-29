# shellcheck shell=bash
# Sourced by release-gate.sh and release-gate-run.sh: one gate run at a time per nest copy. Two runs
# against one copy contend for its redb lock, and the second `serve` will not start.
#
# gate_lock <nest-copy-dir> takes the copy's lock, waiting for whoever holds it, and keeps it until
# the shell exits (call gate_unlock from the EXIT trap). It exports GATE_LOCK_HELD, so a child gate
# run on the same copy, as release-gate-run.sh starts, rides on its parent's lock instead of waiting
# on it for ever. flock where there is one; elsewhere (macOS) a mkdir lock that records its pid and
# is broken once that pid has gone, since nothing releases it for a killed holder.

gate_lock_path() {
  local parent
  parent=$(cd "$(dirname "$1")" && pwd -P) || return 1
  printf '%s/%s.gate-lock\n' "$parent" "$(basename "$1")"
}

gate_lock() {
  local path holder said=0
  path=$(gate_lock_path "$1") || { echo "gate lock: no directory above $1" >&2; return 1; }
  [ "${GATE_LOCK_HELD:-}" != "$path" ] || return 0
  if command -v flock >/dev/null && [ -z "${GATE_LOCK_PORTABLE:-}" ]; then
    exec 9>>"$path" || return 1
    if ! flock -n 9; then
      echo "gate lock: another gate run holds $path; waiting" >&2
      flock 9 || return 1
    fi
  else
    # The pid file is the claim, created by the shell with O_EXCL: uutils mkdir (Ubuntu 26.04) is
    # not exclusive, and two runs both "made" the directory.
    while :; do
      mkdir -p "$path.d" || return 1
      (set -C; echo "$$" >"$path.d/pid") 2>/dev/null && break
      [ -d "$path.d" ] || continue
      holder=$(cat "$path.d/pid" 2>/dev/null || true)
      if [ -n "$holder" ] && ! kill -0 "$holder" 2>/dev/null; then
        echo "gate lock: breaking $path.d, left by pid $holder, which has gone" >&2
        rm -f "$path.d/pid"
        continue
      fi
      [ "$said" -eq 1 ] || echo "gate lock: another gate run holds $path.d; waiting" >&2
      said=1
      sleep 1
    done
    GATE_LOCK_DIR=$path.d
  fi
  export GATE_LOCK_HELD=$path
}

gate_unlock() {
  if [ -n "${GATE_LOCK_DIR:-}" ] && [ "$(cat "$GATE_LOCK_DIR/pid" 2>/dev/null || true)" = "$$" ]; then
    rm -rf "$GATE_LOCK_DIR"
  fi
  GATE_LOCK_DIR=""
}
