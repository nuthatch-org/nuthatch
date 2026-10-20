#!/usr/bin/env bash
# Copy `main`'s required status checks onto a named branch (#715).
# Usage: scripts/protect-branch.sh sprint/foo
set -euo pipefail
branch="${1:?branch name}"
root="$(cd "$(dirname "$0")/.." && pwd)"
contexts=$(grep -v '^#' "$root/.github/required-checks.txt" | grep -v '^$' \
  | jq -cRn '[inputs | gsub("^\\s+|\\s+$"; "") | select(. != "")]')
gh api --method PUT "repos/${GITHUB_REPOSITORY:-nuthatch-org/nuthatch}/branches/${branch}/protection" \
  --input - <<EOF
{
  "required_status_checks": {
    "strict": true,
    "contexts": $contexts
  },
  "enforce_admins": true,
  "required_pull_request_reviews": null,
  "restrictions": null
}
EOF
echo "protection copied onto $branch"
