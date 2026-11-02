#!/usr/bin/env bash
# Rolls a published release onto the Helsinki units, run from a workstation that can
# `ssh root@<host>`. Uses the deploy scripts from the release's own tag, checksums the Linux release,
# installs it, and rolls each unit with its smoke file, the allocations nest first. A smoke failure
# stops here: a regression reverts that unit, a failure the previous release shares keeps it (exit 3).
#   deploy/roll-helsinki-from-mac.sh <version> [--override '<reason>'] [unit ...]
#   deploy/roll-helsinki-from-mac.sh 4.3.1                         every unit
#   deploy/roll-helsinki-from-mac.sh 4.3.1 graph-allocations-nest-next
# Before touching a unit, the tag's commit must carry success on release-gate/<nest> for every rolled
# unit (#1848); a red, pending or missing status refuses, and --override '<reason>' rolls anyway.
# The smoke files carry kittiwake's statements, so they live in the private kittiwake repo:
# SMOKE_DIR (default ~/Projects/kittiwake/nuthatch-gate/smoke) holds <unit without -next>.sql.
set -euo pipefail
v=${1:?usage: roll-helsinki-from-mac.sh <version> [--override '<reason>'] [unit ...]}; shift
host=${ROLL_HOST:-root@89.167.109.4}
repo=${ROLL_REPO:-nuthatch-org/nuthatch}
smoke_dir=${SMOKE_DIR:-$HOME/Projects/kittiwake/nuthatch-gate/smoke}
override="" units=()
while [ $# -gt 0 ]; do
  case "$1" in
    --override) [ -n "${2:-}" ] || { echo "--override needs a reason" >&2; exit 2; }
      override=$2; shift 2 ;;
    *) units+=("$1"); shift ;;
  esac
done
# Each unit in roll order, with the release-gate nest whose status vouches for it.
gates=(graph-allocations-nest-next=alloc-nest data-services-nest=data-services-nest
  graph-gns-nest-next=gns-nest nuthatch-dips=dips-nest graph-staking-legacy-readonly=staking-archive-nest)
gate_of() { local g; for g in "${gates[@]}"; do [ "${g%%=*}" != "$1" ] || echo "${g#*=}"; done; }
[[ $v =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || { echo "not a version: $v" >&2; exit 2; }
if [ ${#units[@]} -eq 0 ]; then for g in "${gates[@]}"; do units+=("${g%%=*}"); done; fi
for u in "${units[@]}"; do [[ $u =~ ^[a-z0-9][a-z0-9-]*$ ]] || { echo "not a unit name: $u" >&2; exit 2; }; done

# The combined status endpoint answers the newest status per context.
refusals=()
if sha=$(gh api "repos/$repo/commits/v$v" --jq .sha) &&
  statuses=$(gh api "repos/$repo/commits/$sha/status?per_page=100" \
    --jq '.statuses[] | [.context, .state, .description // "", .target_url // ""] | @tsv'); then
  for u in "${units[@]}"; do
    nest=$(gate_of "$u")
    if [ -z "$nest" ]; then refusals+=("$u: no release gate covers this unit"); continue; fi
    ctx=release-gate/$nest
    state=$(awk -F'\t' -v c="$ctx" '$1 == c { print $2; exit }' <<<"$statuses")
    detail=$(awk -F'\t' -v c="$ctx" '$1 == c { print "\"" $3 "\" " $4; exit }' <<<"$statuses")
    case "$state" in
      success) ;;
      "") refusals+=("$ctx ($u): no status on v$v ($sha)") ;;
      pending) refusals+=("$ctx ($u): still running, $detail") ;;
      *) refusals+=("$ctx ($u): $state, $detail") ;;
    esac
  done
else
  refusals+=("could not read the statuses of v$v from $repo")
fi
for r in ${refusals[@]+"${refusals[@]}"}; do echo "gate: $r" >&2; done
if [ ${#refusals[@]} -gt 0 ] && [ -z "$override" ]; then
  echo "refusing to roll v$v: its release gates did not pass; --override '<reason>' rolls anyway" >&2
  exit 1
fi
[ ${#refusals[@]} -gt 0 ] || echo "ok   gates: every rolled unit's release-gate status is success on v$v"
if [ -n "$override" ]; then
  printf '\n*** OVERRIDE: rolling v%s past its release gates ***\n*** reason: %s ***\n\n' "$v" "$override"
fi

[ -d "$smoke_dir" ] || { echo "no smoke directory at $smoke_dir: clone nuthatch-org/kittiwake or set SMOKE_DIR" >&2; exit 2; }
smokes=()
for u in "${units[@]}"; do
  if [ -f "$smoke_dir/${u%-next}.sql" ]; then smokes+=("${u%-next}.sql")
  else echo "note: no $smoke_dir/${u%-next}.sql, $u rolls without a smoke" >&2; fi
done

# The smoke files ride over the same connection on stdin, into a directory beside the clone.
COPYFILE_DISABLE=1 tar -C "$smoke_dir" -cf - -T /dev/null ${smokes[@]+"${smokes[@]}"} | ssh "$host" "V=$v; UNITS='${units[*]}'; "'set -e
d=/tmp/nuthatch-roll-$V; rm -rf "$d" "$d-smoke"; mkdir "$d-smoke"; tar -xf - -C "$d-smoke"
git clone -q --depth 1 --branch "v$V" https://github.com/nuthatch-org/nuthatch "$d"; cd "$d"
u=https://github.com/nuthatch-org/nuthatch/releases/download/v$V/nuthatch-x86_64-unknown-linux-gnu.tar.gz
curl -fsSL -o n.tgz "$u"; curl -fsSL -o n.sha "$u.sha256"
echo "$(cut -d" " -f1 n.sha)  n.tgz" | sha256sum -c --quiet
mkdir x; tar xzf n.tgz -C x; bin=$(find x -type f -name nuthatch | head -1)
scripts/deploy-nest.sh install "$bin" "$V"
for unit in $UNITS; do
  smoke=$d-smoke/${unit%-next}.sql
  if [ -f "$smoke" ]; then scripts/deploy-nest.sh roll "$unit" "$V" --smoke "$smoke"
  else scripts/deploy-nest.sh roll "$unit" "$V"; fi
done
scripts/deploy-nest.sh check'
