#!/usr/bin/env bash
# release-gate-run.sh - the ThinkPad's side of the release gate (#1749): fetch a release
# candidate's Linux binary, run scripts/release-gate.sh against a production nest's copy, and
# report the verdict on the candidate's commit as a status. See docs/release-gate.md.
#
#   scripts/release-gate-run.sh <tag>                     gate a published release or candidate
#   scripts/release-gate-run.sh --poll                    gate the newest ungated release that is
#                                                         newer than production; none, exit 0 quietly
#   scripts/release-gate-run.sh --binary PATH --sha SHA   gate a local build of commit SHA
#   options: --production <tag>  the version production runs, measured as the baseline (default:
#                                the version the copy's PROVENANCE records, read after the refresh;
#                                with no PROVENANCE, the latest full release that is not the candidate)
#            --no-status         run and print, post nothing
#
# The candidate is compared with the production version measured on the same box, the same copy
# and the same day, so neither a refreshed copy nor a different machine reads as a regression.
# Production over its own budget, or failing statements itself, is still the baseline: the
# candidate is judged on its own refusals, regressions, differing answers and peak.
# The status never blocks the tag: the tag exists before the gate runs, and the context is not a
# required check anywhere. A red status stops the roll (deploy-nest.sh), not the release.
#
# Every production nest (#1794): GATE_NESTS names a config file with one nest per line, and each is
# gated in turn by this script run for that nest alone, posting its own status,
# release-gate/<name>. The exit is the worst of theirs. See deploy/release-gate/nests.conf.example.
#   <name> <copy> <query set> <env file> <refresh: helsinki | local | none>
#
# The rolling upgrade (RFC-0073 FR7): after the nests, the candidate is run through
# scripts/rolling-upgrade.sh once, against the latest release older than it, and posts
# release-gate/rolling-upgrade, naming whether it ran across releases or fell back to one binary.
# A poll runs it only for the newest release, and only if that has no such status yet.
#
# Environment:
#   GATE_NESTS    the nests' config file; unset means the one nest the variables below describe
#   GATE_STATE    working directory: binaries and run logs             (default ~/release-gate)
#   GATE_NEST     the allocations nest copy, segments plus redb        (default $GATE_STATE/alloc-nest)
#   GATE_NAME     the nest's name, as in its status context            (default alloc-nest)
#   GATE_SET      the query set, required for one nest: kittiwake's nuthatch-gate/alloc-queries.tsv
#                 (private); with GATE_NESTS each nest's set comes from the config
#   GATE_ENV      production's environment for the nest, read from its unit (release-gate.sh --env);
#                 unset means release-gate.sh's PROD_ENV, the allocations nest's
#   GATE_REFRESH  a command run before the gate to refresh GATE_NEST, e.g. the rsync from Helsinki;
#                 unset means the copy is used as it stands
#   GATE_REFRESH_SCRIPT  what a config's helsinki and local refreshes run
#                        (default deploy/release-gate/refresh-from-helsinki.sh beside this)
#   GATE_REPO     (default nuthatch-org/nuthatch)
#   GATE_TARGET   release asset target         (default x86_64-unknown-linux-gnu)
#   GATE_PASSES   passes per binary            (default 3)
#   GATE_CONCURRENCY  statements in flight at once (default 2, as 8107's SQL_MAX_CONCURRENCY); a
#                     GATE_ENV that sets NUTHATCH_SQL_MAX_CONCURRENCY decides it instead
#   GATE_ROLLING  1 gates the nests then the rolling upgrade (default), 0 the nests only, only the
#                 rolling upgrade only. It needs a fixture-rpc: FIXTURE_RPC, or cargo to build one
set -euo pipefail

name=${GATE_NAME:-alloc-nest}
CONTEXT=release-gate/$name
here=$(cd "$(dirname "$0")" && pwd)
state=${GATE_STATE:-$HOME/release-gate}
nest=${GATE_NEST:-$state/alloc-nest}
set_file=${GATE_SET:-}
repo=${GATE_REPO:-nuthatch-org/nuthatch}
target=${GATE_TARGET:-x86_64-unknown-linux-gnu}
passes=${GATE_PASSES:-3}
concurrency=${GATE_CONCURRENCY:-2}

log() { echo "release-gate-run: $*" >&2; }
die() { log "$*"; exit 2; }
trap 'rc=$?; log "internal error at line $LINENO (exit $rc)"; exit 2' ERR

tag="" poll=0 local_bin="" sha="" production_flag="" post=1
args=("$@")
while [ $# -gt 0 ]; do
  case "$1" in
    --poll) poll=1; shift ;;
    --binary) [ $# -ge 2 ] || die "--binary needs a path"; local_bin=$2; shift 2 ;;
    --sha) [ $# -ge 2 ] || die "--sha needs a commit"; sha=$2; shift 2 ;;
    --production) [ $# -ge 2 ] || die "--production needs a tag"; production_flag=$2; shift 2 ;;
    --no-status) post=0; shift ;;
    -h|--help) sed -n '2,51p' "$0"; exit 0 ;;
    --*) die "unknown option $1" ;;
    *) [ -z "$tag" ] || die "one tag at a time"; tag=$1; shift ;;
  esac
done
modes=$(( poll + (${#tag} > 0 ? 1 : 0) + (${#local_bin} > 0 ? 1 : 0) ))
[ "$modes" -eq 1 ] || die "give exactly one of <tag>, --poll, --binary PATH --sha SHA"
[ -z "$local_bin" ] || [ -n "$sha" ] || die "--binary needs --sha: the status has to land on a commit"
command -v gh >/dev/null || die "gh is not on PATH"
rolling=${GATE_ROLLING:-1}
case "$rolling" in 0 | 1 | only) ;; *) die "GATE_ROLLING is 0, 1 or only, not $rolling" ;; esac

# shellcheck source=gate/semver.sh
. "$here/gate/semver.sh"

status_posted=0
post_status() {
  local st=$1 desc=$2 ctx=$CONTEXT
  desc=${desc:0:140}
  if [ "$post" -eq 0 ]; then
    log "status (not posted): $ctx $st - $desc"
    return 0
  fi
  gh api -X POST "repos/$repo/statuses/$sha" -f state="$st" -f context="$ctx" \
    -f description="$desc" >/dev/null
  # A pending status left behind by a dying run would read as "still running" for ever.
  [ "$st" = pending ] || [ "$ctx" != "$CONTEXT" ] || status_posted=1
  log "posted $ctx $st on $sha: $desc"
}

sha256_check() {
  local dir=$1 file=$2
  if command -v sha256sum >/dev/null; then
    (cd "$dir" && sha256sum -c "$file.sha256" >/dev/null)
  else
    (cd "$dir" && shasum -a 256 -c "$file.sha256" >/dev/null)
  fi
}

# Download and verify one release's binary; prints its path.
fetch() {
  local t=$1 dir=$state/bins/$1 asset=nuthatch-$target.tar.gz
  if [ ! -x "$dir/nuthatch" ]; then
    rm -rf "$dir"
    mkdir -p "$dir"
    gh release download "$t" -R "$repo" -p "$asset" -p "$asset.sha256" -D "$dir" >&2 \
      || die "could not download $asset from $t"
    sha256_check "$dir" "$asset" || die "$asset from $t fails its checksum"
    tar -xzf "$dir/$asset" -C "$dir"
    local found
    found=$(find "$dir" -type f -name nuthatch | head -n 1)
    [ -n "$found" ] || die "no nuthatch binary inside $asset from $t"
    [ "$found" = "$dir/nuthatch" ] || mv "$found" "$dir/nuthatch"
    chmod +x "$dir/nuthatch"
  fi
  printf '%s\n' "$dir/nuthatch"
}

commit_of() { gh api "repos/$repo/commits/$1" --jq .sha; }

# The newest status for the context decides. A pending one older than GATE_STALE_SECS is a run that
# died before posting its verdict, so the release is gated again rather than left pending forever.
has_status() {
  gh api "repos/$repo/commits/$1/statuses" --jq "[.[] | select(.context == \"$CONTEXT\")] | first
    | if . == null then \"none\"
      elif .state == \"pending\" and (now - (.updated_at | fromdate)) > ${GATE_STALE_SECS:-7200} then \"stale\"
      else .state end" | grep -qvE '^(none|stale)$'
}

# The rolling upgrade belongs to the candidate, not to a nest, so it runs once, after the nests.
if [ "$rolling" = 1 ]; then
  rc_nests=0 rc_rolling=0
  GATE_ROLLING=0 "$0" ${args[@]+"${args[@]}"} || rc_nests=$?
  GATE_ROLLING=only "$0" ${args[@]+"${args[@]}"} || rc_rolling=$?
  if [ "$rc_nests" -gt 1 ] || [ "$rc_rolling" -gt 1 ]; then exit 2; fi
  if [ "$rc_nests" -eq 1 ] || [ "$rc_rolling" -eq 1 ]; then exit 1; fi
  exit 0
fi

if [ "$rolling" = only ]; then
  CONTEXT=release-gate/rolling-upgrade
  mkdir -p "$state/bins" "$state/runs"
  trap 'rc=$?; if [ "$rc" -gt 1 ] && [ "$status_posted" -eq 0 ] && [ -n "$sha" ]; then
    post_status error "the rolling upgrade could not run (exit $rc); see $state/runs on the gate box" || true; fi' EXIT
  if [ "$poll" -eq 1 ]; then
    # Only the newest release: every older one predates this step, and walking back through them
    # is what --poll did to the nests on 2026-10-03.
    listed=$(gh release list -R "$repo" -L 20 --json tagName,isDraft \
      --jq '.[] | select(.isDraft | not) | .tagName') || die "could not list the releases of $repo"
    newest=$(head -n 1 <<<"$listed")
    [ -n "$newest" ] || exit 0
    c=$(commit_of "$newest")
    ! has_status "$c" || exit 0
    tag=$newest
    log "gating the rolling upgrade to $tag, the newest release, which has no $CONTEXT status"
  fi
  if [ -n "$tag" ]; then
    sha=$(commit_of "$tag")
    label=$tag
    cand_bin=$(fetch "$tag")
  else
    [ -x "$local_bin" ] || die "not an executable: $local_bin"
    label="local build of ${sha:0:12}"
    cand_bin=$local_bin
  fi
  run=$state/runs/$(date -u +%Y%m%dT%H%M%SZ)-rolling-upgrade-${tag:-${sha:0:12}}
  mkdir -p "$run"
  post_status pending "replacing followers, then the writer, from the previous release to $label"
  log "rolling upgrade to $label"
  rc=0
  ROLLING_OUT=$run/out GATE_REPO=$repo GATE_TARGET=$target \
    "${GATE_ROLLING_SCRIPT:-$here/rolling-upgrade.sh}" "$cand_bin" >"$run/rolling.txt" 2>&1 || rc=$?
  cat "$run/rolling.txt"
  mode=$(sed -n 's/^rolling-upgrade: MODE //p' "$run/rolling.txt" | head -n 1)
  why=$(grep -E '^rolling-upgrade: FAIL: ' "$run/rolling.txt" | tail -n 1 | sed 's/^rolling-upgrade: FAIL: //' || true)
  case "$rc" in
    0) post_status success "passed, ${mode:-mode not reported}" ;;
    1) post_status failure "${mode:-mode not reported}: ${why:-failed}" ;;
    *) post_status error "could not run (exit $rc)${mode:+, $mode}; see $run" ; exit 2 ;;
  esac
  exit "$rc"
fi

# A config path as written, ~ for $HOME, else relative to the config file's directory.
conf_path() {
  case "$1" in
    '~/'*) printf '%s\n' "$HOME/${1#'~/'}" ;;
    /*) printf '%s\n' "$1" ;;
    *) printf '%s\n' "$2/$1" ;;
  esac
}

if [ -n "${GATE_NESTS:-}" ]; then
  [ -f "$GATE_NESTS" ] || die "no nests config at $GATE_NESTS (GATE_NESTS)"
  conf_dir=$(cd "$(dirname "$GATE_NESTS")" && pwd)
  refresh_script=${GATE_REFRESH_SCRIPT:-$(cd "$here/.." && pwd)/deploy/release-gate/refresh-from-helsinki.sh}
  names=() copies=() sets=() envs=() refreshes=()
  lineno=0
  # Read whole before anything runs, so a bad line gates nothing rather than half the nests.
  while IFS= read -r line || [ -n "$line" ]; do
    lineno=$((lineno + 1))
    case "$line" in '' | '#'*) continue ;; esac
    read -r n c s e r extra <<<"$line"
    [ -n "$r" ] && [ -z "${extra:-}" ] \
      || die "$GATE_NESTS line $lineno: want <name> <copy> <set> <env> <refresh>, got: ${line:0:100}"
    printf '%s\n' "$n" | grep -Eq '^[a-z0-9][a-z0-9-]*$' || die "$GATE_NESTS line $lineno: not a nest name: $n"
    case "$r" in helsinki | local | none) ;; *) die "$GATE_NESTS line $lineno: refresh is helsinki, local or none, not $r" ;; esac
    for seen in ${names[@]+"${names[@]}"}; do
      [ "$seen" != "$n" ] || die "$GATE_NESTS line $lineno: $n is listed twice"
    done
    names+=("$n") copies+=("$(conf_path "$c" "$conf_dir")") sets+=("$(conf_path "$s" "$conf_dir")")
    envs+=("$(conf_path "$e" "$conf_dir")") refreshes+=("$r")
  done <"$GATE_NESTS"
  [ ${#names[@]} -gt 0 ] || die "no nests in $GATE_NESTS"

  worst=0
  for i in "${!names[@]}"; do
    n=${names[$i]}
    case "${refreshes[$i]}" in
      helsinki) refresh="'$refresh_script' '$n'" ;;
      local) refresh="'$refresh_script' --local '$n'" ;;
      none) refresh="" ;;
    esac
    nest_out=$(mktemp "${TMPDIR:-/tmp}/release-gate-run.XXXXXX")
    rc=0
    GATE_NESTS="" GATE_NAME=$n GATE_NEST=${copies[$i]} GATE_SET=${sets[$i]} GATE_ENV=${envs[$i]} \
      GATE_REFRESH=$refresh "$0" ${args[@]+"${args[@]}"} >"$nest_out" 2>&1 || rc=$?
    # A poll with nothing to gate stays quiet, as the single nest's does.
    if [ "$rc" -ne 0 ] || [ -s "$nest_out" ]; then
      sed "s/^/[$n] /" "$nest_out"
      case "$rc" in 0) v=passed ;; 1) v=failed ;; *) v="could not run (exit $rc)" ;; esac
      log "$n: $v"
    fi
    rm -f "$nest_out"
    if [ "$rc" -gt 1 ]; then worst=2; elif [ "$rc" -eq 1 ] && [ "$worst" -eq 0 ]; then worst=1; fi
  done
  exit "$worst"
fi

[ -n "$set_file" ] || die "GATE_SET is not set: point it at a kittiwake checkout's nuthatch-gate/alloc-queries.tsv"
[ -f "$set_file" ] || die "no query set at $set_file (GATE_SET)"
env_args=()
if [ -n "${GATE_ENV:-}" ]; then
  [ -f "$GATE_ENV" ] || die "no environment file at $GATE_ENV (GATE_ENV)"
  env_args=(--env "$GATE_ENV")
  c=$(sed -n 's/^NUTHATCH_SQL_MAX_CONCURRENCY=//p' "$GATE_ENV" | tail -n 1)
  [ -z "$c" ] || concurrency=$c
fi

mkdir -p "$state/bins" "$state/runs" "$(dirname "$nest")"
# One gate at a time on the copy, held across the refresh and both runs; release-gate.sh takes the
# same lock, so a run by hand waits for this one, and the children here ride on it.
# shellcheck source=gate/lock.sh
. "$here/gate/lock.sh"
cleanup() {
  rc=$?
  gate_unlock
  if [ "$rc" -ne 0 ] && [ "$rc" -ne 1 ] && [ "$status_posted" -eq 0 ] && [ -n "$sha" ]; then
    post_status error "the gate could not run (exit $rc); see $state/runs on the gate box" || true
  fi
}
trap cleanup EXIT
gate_lock "$nest" || die "could not take the lock on $nest"

full_releases="" listed_full=0
# Sets production and production_from for candidate $1: --production; else the version the copy's
# PROVENANCE records, which the refresh read from production's own /ready; else, with no
# PROVENANCE, the latest full release that is not the candidate, which is a guess (#1804).
# Called directly, never in $(...), so that its die exits the run.
resolve_production() {
  local v
  if [ -n "$production_flag" ]; then
    production=$production_flag production_from=flag
  elif [ -f "$nest/PROVENANCE" ]; then
    v=$(sed -n 's/^version=//p' "$nest/PROVENANCE" | tail -n 1 | tr -d '\r')
    v=${v#v}
    if ! printf '%s\n' "$v" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
      [ -n "$sha" ] || die "$nest/PROVENANCE records version '$v', which is not a release version, and no candidate is chosen yet, so no status is posted; pass --production"
      post_status error "the copy's PROVENANCE records version '${v:0:40}', not a release version; pass --production" || true
      die "$nest/PROVENANCE records version '$v', which is not a release version; pass --production"
    fi
    production=v$v production_from=provenance
  else
    if [ "$listed_full" -eq 0 ]; then
      full_releases=$(gh release list -R "$repo" -L 20 --exclude-pre-releases --json tagName,isDraft \
        --jq '.[] | select(.isDraft | not) | .tagName') || die "could not list the releases of $repo"
      listed_full=1
    fi
    production=$(printf '%s\n' "$full_releases" | grep -vxF "$1" | head -n 1 || true)
    production_from=latest
  fi
  [ -n "$production" ] || die "no full release to measure as production; pass --production"
}

if [ "$poll" -eq 1 ]; then
  # Newest first. A draft is skipped: its assets are still being built, and it is not yet a release.
  # A release at or below production is never a candidate: it was superseded, not missed. Production
  # here is the copy as it stands; it is read again after the refresh below.
  listed=$(gh release list -R "$repo" -L 20 --json tagName,isDraft \
    --jq '.[] | select(.isDraft | not) | .tagName') || die "could not list the releases of $repo"
  for t in $listed; do
    resolve_production "$t"
    semver_gt "$t" "$production" || continue
    c=$(commit_of "$t")
    if ! has_status "$c"; then tag=$t; break; fi
  done
  [ -n "$tag" ] || exit 0
  log "gating $tag, the newest release newer than production without a $CONTEXT status"
fi

if [ -n "$tag" ]; then
  sha=$(commit_of "$tag")
  label=$tag
else
  [ -x "$local_bin" ] || die "not an executable: $local_bin"
  label="local build of ${sha:0:12}"
fi

if [ -n "${GATE_REFRESH:-}" ]; then
  log "refreshing the copy: $GATE_REFRESH"
  sh -c "$GATE_REFRESH" >&2 || die "GATE_REFRESH failed"
fi

resolve_production "${tag:-}"
from_note=""
case "$production_from" in
  flag) log "production is $production, as --production gives it" ;;
  provenance) log "production is $production, the version $nest/PROVENANCE records" ;;
  latest)
    log "$nest has no PROVENANCE, so production is taken to be $production, the latest full release that is not the candidate; pass --production if it is not"
    from_note=" as the latest release, no PROVENANCE" ;;
esac
if [ "$poll" -eq 1 ] && ! semver_gt "$tag" "$production"; then
  log "after the refresh production is $production, so $tag is no longer newer than it; nothing to gate"
  exit 0
fi
[ "$production" != "$tag" ] || die "the candidate and production are the same release ($tag)"

post_status pending "running $label against the $name copy, baseline $production$from_note"

prod_bin=$(fetch "$production")
if [ -n "$tag" ]; then cand_bin=$(fetch "$tag"); else cand_bin=$local_bin; fi

run=$state/runs/$(date -u +%Y%m%dT%H%M%SZ)-$name-${tag:-${sha:0:12}}
mkdir -p "$run"

log "measuring production $production for the baseline"
rc=0
"$here/release-gate.sh" --passes "$passes" --concurrency "$concurrency" ${env_args[@]+"${env_args[@]}"} --out "$run/production" \
  --write-baseline "$run/baseline.tsv" "$prod_bin" "$nest" "$set_file" >"$run/production.txt" 2>&1 || rc=$?
# Production failing its own gate is still what production does, so it is still the baseline. Only
# a run that could not measure it (exit 2, no baseline written) leaves nothing to compare with.
if { [ "$rc" -ne 0 ] && [ "$rc" -ne 1 ]; } || [ ! -s "$run/baseline.tsv" ]; then
  cat "$run/production.txt" >&2
  why=""
  ! grep -qE '^release-gate: the box is loaded|^RESULT: SETUP FAULT - the box was loaded' "$run/production.txt" || why=", the box was loaded"
  post_status error "production $production could not be measured (exit $rc$why), so there is no baseline; see $run"
  exit 2
fi
prod_note=""
if [ "$rc" -eq 1 ]; then
  prod_failed=$(sed -n 's/^RESULT: FAIL - failed: //p' "$run/production.txt" | sed -e 's/, peak RSS$//' -e 's/^peak RSS$//')
  prod_peak=$(sed -n 's/^release-gate: peak RSS \([0-9]*\) MiB, budget \([0-9]*\) MiB: OVER$/\1 \2/p' "$run/production.txt")
  if [ -n "$prod_failed" ]; then
    n_failed=$(printf '%s\n' "$prod_failed" | awk -F', ' '{ print NF }')
    if [ "$n_failed" -eq 1 ]; then they="it cannot"; else they="they cannot"; fi
    log "production $production fails $prod_failed itself, so $they regress; the candidate still fails on any it refuses"
    prod_note="$prod_note, which fails $n_failed itself"
  fi
  if [ -n "$prod_peak" ]; then
    log "production $production peaked at ${prod_peak% *} MiB, over the ${prod_peak#* } MiB budget; the candidate is held to the budget on its own peak"
    if [ -n "$prod_failed" ]; then prod_note="$prod_note and"; else prod_note="$prod_note, which"; fi
    prod_note="$prod_note peaked at ${prod_peak% *} MiB, over budget"
  fi
fi

log "gating $label"
rc=0
"$here/release-gate.sh" --passes "$passes" --concurrency "$concurrency" ${env_args[@]+"${env_args[@]}"} --out "$run/candidate" \
  --baseline "$run/baseline.tsv" "$cand_bin" "$nest" "$set_file" >"$run/candidate.txt" 2>&1 || rc=$?
cat "$run/candidate.txt"

# The counts only: the p99 is in the run's output, and the status has 140 characters.
summary=$(grep -E '^release-gate: [0-9]+ of [0-9]+ answered' "$run/candidate.txt" | sed -e 's/^release-gate: //' -e 's/;.*//' || true)
result=$(grep -E '^RESULT: ' "$run/candidate.txt" | sed 's/^RESULT: //' || true)
# A loaded box is a setup fault (#1898): said in the status, so it is not read as a broken rig.
load_why=""
grep -qE '^release-gate: the box is loaded|^RESULT: SETUP FAULT - the box was loaded' "$run/candidate.txt" && load_why="box load" || true
case "$rc" in
  0) post_status success "${summary:-passed} (against $production$from_note$prod_note)" ;;
  1) post_status failure "${summary:-failed}; ${result#FAIL - } (against $production$from_note$prod_note)" ;;
  *) post_status error "the gate could not run (exit $rc${load_why:+: $load_why}); see $run" ; exit 2 ;;
esac
exit "$rc"
