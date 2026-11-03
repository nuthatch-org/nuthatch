#!/usr/bin/env bash
# Compare `.github/required-checks.txt` to the live protection API on `main` (#715).
#
# ## Why this refuses to run blind (#845)
#
# It used to answer a missing token by printing a count and **exiting 0**. That is the same shape as
# the mutation checker in #841: the comparison is the entire point of the script, and its degraded
# mode was indistinguishable from success except by reading stdout. Wired into a workflow without a
# token deliberately granted, it would have produced a permanently green step that compared nothing.
#
# So: no token is a **failure**, and `--offline` is how a caller says it knows and wants the
# file-only check anyway. "I could not compare" must never render as "they match".
#
# The token needs read access to branch protection, and **`GITHUB_TOKEN` cannot be given it at all**
# (#909). `administration` is an App/PAT scope; it is not a key the workflow `permissions:` block
# accepts, and a file that names it is rejected at validation, so the workflow never starts. This
# comment used to say the opposite, and so did the FAIL text below - which meant the recovery path
# offered to a reader at the moment they needed it could not be taken. A failure message naming an
# impossible fix is worse than one that says nothing.
#
# The reachable path is a PAT or App token with repo admin read, held as the repository secret
# `PROTECTION_READ_TOKEN` and passed in as `GH_TOKEN`.
set -euo pipefail

# **Byte semantics, not the runner's locale.** Every context in `.github/required-checks.txt` contains
# `·` (U+00B7, the bytes C2 B7), and `grep`/`sort` change behaviour on non-ASCII input depending on
# `LC_*`: GNU grep can decline to match a line it considers an encoding error, and `sort` refuses an
# illegal byte sequence outright. A CI runner whose locale differs from a developer's would then read
# a healthy file as one missing a context - which is the shape of the #1119 failure, where `main`
# reported `required-checks.txt is missing 'Jules approval'` against a file that plainly contains it.
#
# Pinning to C makes the comparison byte-wise and identical everywhere. The contexts are compared for
# exact equality, so collation order is irrelevant to the result.
export LC_ALL=C

offline=0
for arg in "$@"; do
  case "$arg" in
    --offline) offline=1 ;;
    *) echo "usage: $0 [--offline]" >&2; exit 2 ;;
  esac
done

root="$(cd "$(dirname "$0")/.." && pwd)"
file="$root/.github/required-checks.txt"
# **An unreadable file must not be reported as a file with the wrong contents.** Any failure reading
# the list yields an empty `expected`, and the very next check then says
# `required-checks.txt is missing 'Jules approval'` - naming the file's *contents* as the fault when
# nothing was read at all. That message sent a CI failure on `main` to the wrong place (#1119).
if [ ! -r "$file" ]; then
  echo "cannot read $file, so the committed list was never compared" >&2
  exit 1
fi
# **Read the whole file first, and check that it was read.** The case neither the `-r` test nor the
# emptiness test below can see is a read that fails *after* emitting some lines: `expected` comes out
# non-empty and **incomplete**, and the drift check then reports contexts missing from a file that
# actually has them.
#
# A filter pipeline cannot answer "was the whole file read". Under `pipefail` the status is the
# rightmost failure, so `grep -v '^#'` failing with 2 is masked by the next `grep -v '^$'` exiting 1
# for empty input - which is how a directory in place of the file reported itself as "empty or all
# comments". One `cat`, one status, one question answered.
set +e
raw=$(cat -- "$file")
read_status=$?
set -e
if [ "$read_status" -ne 0 ]; then
  echo "reading $file failed with status $read_status - the list may be truncated, so nothing here is a comparison" >&2
  exit 1
fi

# **Filter in the shell, so there is no pipeline status to mask.** `... | sort || true` suppresses a
# `sort` that dies after emitting partial output just as thoroughly as it suppresses `grep` exiting 1
# for no output, and the two need opposite treatment: the first is a truncated list reported as a
# file missing contexts, the second is an ordinary empty file. Under `pipefail` they are not even
# distinguishable, because the status is the rightmost failure and a later stage exiting 1 on empty
# input hides an earlier stage exiting 2.
#
# No subprocess, no pipeline, nothing to swallow. `sort` is then the only command left with a status
# worth checking, and it is checked exactly.
contexts=()
while IFS= read -r line; do
  [ -n "$line" ] || continue
  case "$line" in '#'*) continue ;; esac
  contexts+=("$line")
done <<< "$raw"

if [ ${#contexts[@]} -eq 0 ]; then
  echo "read no contexts from $file - it is empty or all comments. Nothing was compared, so this is not a drift check" >&2
  exit 1
fi

set +e
expected=$(printf '%s\n' "${contexts[@]}" | sort)
sort_status=$?
set -e
if [ "$sort_status" -ne 0 ]; then
  echo "sorting the ${#contexts[@]} contexts from $file failed with status $sort_status - the list may be incomplete, so nothing here is a comparison" >&2
  exit 1
fi

# The `reviewed-by signature` gate was retired: a PR is admitted by CI and Jules, not by a line of
# text a party could type about itself. Asserted in the negative because re-adding the context
# without re-adding the workflow blocks every PR on a check that can never report.
if grep -qx 'reviewed-by signature' <<< "$expected"; then
  echo "required-checks.txt names the retired 'reviewed-by signature' context, whose workflow no longer exists" >&2
  exit 1
fi
# A here-string, not `echo | grep -q`: grep -q exits at its match, the echo's next write takes
# SIGPIPE, and pipefail turns a found context into a missing one (#2280).
if ! grep -qx 'Jules approval' <<< "$expected"; then
  echo "required-checks.txt is missing 'Jules approval'" >&2
  exit 1
fi

token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"

if [ "$offline" -eq 1 ]; then
  echo "offline: file lists $(echo "$expected" | wc -l | tr -d ' ') contexts including Jules approval."
  echo "offline: NOT compared against live protection - this is not a drift check."
  exit 0
fi

if [ -z "$token" ]; then
  cat >&2 <<'MSG'
FAIL: no GH_TOKEN/GITHUB_TOKEN, so live branch protection was not read.

This is a failure and not a pass. The whole job of this script is to compare the committed list
against what GitHub actually enforces; without a token it has compared nothing, and reporting
success would mean "the check that watches the checks" is itself scenery.

  - Locally:     GH_TOKEN=$(gh auth token) scripts/check-required-contexts.sh
  - In Actions:  set the repository secret PROTECTION_READ_TOKEN to a PAT (or App token) with
                 admin read on this repo, and pass it as GH_TOKEN. GITHUB_TOKEN cannot do this
                 job: reading branches/main/protection is an admin-scoped read, and there is no
                 `permissions:` key that grants it (#909 - naming `administration` there is not
                 merely useless, it invalidates the workflow file and stops it running at all).
  - Deliberately without a token: pass --offline, which checks the file alone and says so.
MSG
  exit 1
fi

live=$(curl -fsS -H "Authorization: Bearer $token" -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/${GITHUB_REPOSITORY:-nuthatch-org/nuthatch}/branches/main/protection" \
  | jq -r '.required_status_checks.contexts[]' | sort)

if [ -z "$live" ]; then
  echo "FAIL: the protection API returned no contexts. Either protection is off on main, or the" >&2
  echo "      token cannot read it. Both are worth knowing; neither is a pass." >&2
  exit 1
fi

if [ "$expected" != "$live" ]; then
  echo "required-checks.txt disagrees with live protection on main:" >&2
  echo "--- file ---" >&2
  echo "$expected" >&2
  echo "--- live ---" >&2
  echo "$live" >&2
  exit 1
fi
echo "required checks match live protection on main ($(echo "$live" | wc -l | tr -d ' ') contexts)"
