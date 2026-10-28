# shellcheck shell=bash
# Sourced by release-gate-run.sh and rolling-upgrade.sh: how two release tags compare.

is_num() { case "$1" in '' | *[!0-9]*) return 1 ;; esac; }

# semver_gt A B: tag A is a newer version than tag B. A leading v and any +build are ignored, a
# prerelease sorts before its release, and prerelease identifiers compare as numbers where both are.
# A tag that is not a version is never newer.
semver_gt() {
  local a=${1#v} b=${2#v} ac bc ap bp a1 a2 a3 b1 b2 b3 rest x y
  a=${a%%+*} b=${b%%+*}
  ac=${a%%-*} bc=${b%%-*}
  ap=${a#"$ac"} bp=${b#"$bc"}
  ap=${ap#-} bp=${bp#-}
  IFS=. read -r a1 a2 a3 rest <<<"$ac"
  IFS=. read -r b1 b2 b3 rest <<<"$bc"
  for x in "$a1" "$a2" "$a3" "$b1" "$b2" "$b3"; do is_num "${x:-0}" || return 1; done
  a1=${a1:-0} a2=${a2:-0} a3=${a3:-0} b1=${b1:-0} b2=${b2:-0} b3=${b3:-0}
  if [ "$a1" -ne "$b1" ]; then [ "$a1" -gt "$b1" ]; return; fi
  if [ "$a2" -ne "$b2" ]; then [ "$a2" -gt "$b2" ]; return; fi
  if [ "$a3" -ne "$b3" ]; then [ "$a3" -gt "$b3" ]; return; fi
  [ "$ap" != "$bp" ] || return 1
  [ -n "$ap" ] || return 0
  [ -n "$bp" ] || return 1
  while :; do
    x=${ap%%.*} y=${bp%%.*}
    if [ "$x" != "$y" ]; then
      if is_num "$x" && is_num "$y"; then [ "$x" -gt "$y" ]; return; fi
      if is_num "$x"; then return 1; fi
      if is_num "$y"; then return 0; fi
      [ "$x" \> "$y" ]
      return
    fi
    # Equal so far: the one with fewer identifiers is the older.
    [ "$ap" != "$x" ] || return 1
    [ "$bp" != "$y" ] || return 0
    ap=${ap#*.} bp=${bp#*.}
  done
}
