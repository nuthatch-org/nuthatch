# shellcheck shell=bash
# Sourced by release-gate.sh and release-gate-run.sh: one gate run at a time per nest copy. Two runs
# against one copy contend for its redb lock, and the second `serve` will not start.
#
# gate_lock <nest-copy-dir> takes the copy's lock, waiting for whoever holds it, and keeps it until
# the shell exits (call gate_unlock from the EXIT trap). It exports GATE_LOCK_HELD, so a child gate
# run on the same copy, as release-gate-run.sh starts, rides on its parent's lock instead of waiting
# on it for ever. flock where there is one; elsewhere (macOS) a pid file created O_EXCL, which is
# broken once that pid has gone, since nothing releases it for a killed holder.

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
    # not exclusive, and two runs both "made" the directory. own.<pid> is written before the claim
    # and kept with it: it is the one right to break that claim, see gate_lock_break.
    while :; do
      mkdir -p "$path.d" || return 1
      echo "$$" 2>/dev/null >"$path.d/own.$$" || continue
      (set -C; echo "$$" >"$path.d/pid") 2>/dev/null && break
      [ -d "$path.d" ] || continue
      holder=$(cat "$path.d/pid" 2>/dev/null || true)
      if [ -n "$holder" ] && ! kill -0 "$holder" 2>/dev/null; then
        [ -z "${GATE_LOCK_BEFORE_BREAK:-}" ] || eval "$GATE_LOCK_BEFORE_BREAK"
        if gate_lock_break "$path.d" "$holder"; then
          echo "gate lock: breaking $path.d, left by pid $holder, which has gone" >&2
          continue
        fi
      fi
      [ "$said" -eq 1 ] || echo "gate lock: another gate run holds $path.d; waiting" >&2
      said=1
      sleep 1
    done
    GATE_LOCK_DIR=$path.d
  fi
  export GATE_LOCK_HELD=$path
}

# gate_lock_break <lock-dir> <dead-pid> removes the claim only if it is still that pid's. Two waiters
# that saw the same dead pid would otherwise both remove "it", the second removing the first's new
# claim. The right to break a claim is one file, own.<pid> and then brk.<pid>.<breaker>, and it only
# ever moves by rename, so one waiter holds it; a dead breaker's is taken over the same way. While
# it is held nothing else can remove that claim and no claim can replace it.
gate_lock_break() {
  local token=$1/brk.$2.$$ t broke=1
  if ! mv "$1/own.$2" "$token" 2>/dev/null; then
    for t in "$1/brk.$2".*; do
      [ -e "$t" ] && ! kill -0 "${t##*.}" 2>/dev/null && mv "$t" "$token" 2>/dev/null && break
    done
    [ -e "$token" ] || return 1
  fi
  if [ "$(cat "$1/pid" 2>/dev/null || true)" = "$2" ] && ! kill -0 "$2" 2>/dev/null; then
    rm -f "$1/pid" && broke=0
  fi
  rm -f "$token"
  return "$broke"
}

# Unlinks our own files by name and nothing else, so no claim is removed but by its owner or a
# breaker; an rm -rf would take whatever else is in the directory when it reads it.
gate_unlock() {
  if [ -n "${GATE_LOCK_DIR:-}" ] && [ "$(cat "$GATE_LOCK_DIR/pid" 2>/dev/null || true)" = "$$" ]; then
    rm -f "$GATE_LOCK_DIR/pid" "$GATE_LOCK_DIR/own.$$"
    rmdir "$GATE_LOCK_DIR" 2>/dev/null || true
  fi
  GATE_LOCK_DIR=""
}
