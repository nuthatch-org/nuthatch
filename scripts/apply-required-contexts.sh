#!/usr/bin/env bash
# Apply `.github/required-checks.txt` to a protected branch's required status checks (#1094).
#
# The committed list is the source of truth; this is how it reaches GitHub. Before this existed the
# only way to change the enforced set was a hand-typed `gh api` call, which left the repository
# unable to say what it had asked for - the gap Jules named on #1094: "keep the settings change as
# an auditable deployment artifact".
#
# Deliberately a PATCH of `required_status_checks` alone, not the PUT that `protect-branch.sh` does.
# That script copies a whole protection object onto a *new* sprint branch, where there is nothing to
# preserve. Run against `main` it would also write `strict` and `enforce_admins`, silently changing
# settings the caller never mentioned. Narrow endpoint, narrow blast radius.
#
#   scripts/apply-required-contexts.sh                  # dry run against main, prints the diff
#   scripts/apply-required-contexts.sh --apply          # writes it
#   scripts/apply-required-contexts.sh --apply --branch sprint/foo
#
# Needs a token with admin write on the repo. `GITHUB_TOKEN` in Actions cannot do this (#909), the
# same reason `check-required-contexts.sh` needs `PROTECTION_READ_TOKEN` to read (#1095).
set -euo pipefail

branch=main
apply=0
while [ $# -gt 0 ]; do
  case "$1" in
    --apply)  apply=1; shift ;;
    --branch) branch="${2:?--branch needs a value}"; shift 2 ;;
    *) echo "usage: $0 [--apply] [--branch <name>]" >&2; exit 2 ;;
  esac
done

root="$(cd "$(dirname "$0")/.." && pwd)"
repo="${GITHUB_REPOSITORY:-nuthatch-org/nuthatch}"
token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"

if [ -z "$token" ]; then
  echo "FAIL: no GH_TOKEN/GITHUB_TOKEN. Reading and writing branch protection is admin-scoped;" >&2
  echo "      without a token this would compare and write nothing while looking like it worked." >&2
  exit 1
fi

want=$(grep -v '^#' "$root/.github/required-checks.txt" | grep -v '^$' | sed 's/^ *//;s/ *$//' | sort)
[ -n "$want" ] || { echo "FAIL: required-checks.txt lists no contexts" >&2; exit 1; }

# `strict` is read back and re-sent unchanged: this script's job is the context list, and quietly
# flipping the up-to-date-branch requirement while doing it is the class of surprise it exists to
# avoid.
current_json=$(GH_TOKEN="$token" gh api "repos/${repo}/branches/${branch}/protection/required_status_checks")
have=$(printf '%s' "$current_json" | jq -r '.contexts | sort | .[]')
strict=$(printf '%s' "$current_json" | jq -c .strict)

if [ "$want" = "$have" ]; then
  echo "no change: ${branch} already enforces the $(echo "$want" | wc -l | tr -d ' ') committed contexts"
  exit 0
fi

echo "--- enforced on ${branch} now"; echo "$have" | sed 's/^/  /'
echo "+++ committed in .github/required-checks.txt"; echo "$want" | sed 's/^/  /'
echo
echo "to add:";    comm -23 <(echo "$want") <(echo "$have") | sed 's/^/  + /'
echo "to remove:"; comm -13 <(echo "$want") <(echo "$have") | sed 's/^/  - /'

if [ "$apply" -eq 0 ]; then
  echo
  echo "dry run. Re-run with --apply to write this. Nothing was changed."
  exit 0
fi

body=$(printf '%s\n' "$want" | jq -Rn --argjson strict "$strict" '{strict: $strict, contexts: [inputs | gsub("^\\s+|\\s+$"; "") | select(length > 0)]}')
GH_TOKEN="$token" gh api --method PATCH \
  "repos/${repo}/branches/${branch}/protection/required_status_checks" \
  --input - <<<"$body" >/dev/null
echo "applied: ${branch} now enforces the committed list"
