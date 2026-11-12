#!/usr/bin/env bash
# Find every place this repo states a nuthatch version, and flag any that is not the current one.
#
#   ./scripts/version-check.sh          # checks against Cargo.toml's version
#   ./scripts/version-check.sh 2.5.0    # checks against a version you name
#
# The sibling of nuthatch-frontend/scripts/version-check.sh, and written for the same reason. The
# website took three passes to get current on 2026-08-15; the same day, `docs/operators.md` was found
# pinning its two copy-paste `docker run` commands at `:2.0.0` - five releases stale, in the document
# an operator is most likely to follow verbatim. Nothing was broken by it, which is exactly why it
# survived five releases: a five-release-old image starts up perfectly well.
#
# A version is a claim, and claims want a checker rather than a good memory.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

WANT=${1:-}
[ -z "$WANT" ] && WANT=$(sed -n 's/^version = "\([^"]*\)".*/\1/p' Cargo.toml | head -1)
[ -z "$WANT" ] && { echo "could not read a version from Cargo.toml; pass one as an argument"; exit 2; }
echo "current version: $WANT"
echo

fail=0

# 1. The copy-paste commands. These are what someone actually runs, so a stale tag here is not a
#    documentation nit - it hands them an old binary that works, which is the hardest kind to notice.
echo "container tags in docs and compose defaults (copy-paste commands):"
while IFS= read -r line; do
  [ -z "$line" ] && continue
  case "$line" in
    # Globbed on both sides: these lines are whole shell commands, so the tag is rarely the last
    # thing on them (`... :2.5.0-scaled worker --help`). Anchoring at the end reported a correct tag
    # as stale, which is the failure mode that gets a checker switched off.
    *":$WANT"*|*':<version>'*|*':{version}'*) printf "  OK   %s\n" "$line" ;;
    *) printf "  STALE %s\n" "$line"; fail=$((fail + 1)) ;;
  esac
done < <(grep -rn "ghcr.io/nuthatch-org/nuthatch:" --include="*.md" --include="docker-compose*.yml" . 2>/dev/null | grep -v "^./target" | grep -v "/rfcs/")

# 2. Cargo.toml is the source of truth and must agree with itself.
have=$(sed -n 's/^version = "\([^"]*\)".*/\1/p' Cargo.toml | head -1)
echo
if [ "$have" = "$WANT" ]; then
  echo "Cargo.toml: OK ($have)"
else
  echo "Cargo.toml: STALE ($have, expected $WANT)"; fail=$((fail + 1))
fi

# The MCP Registry entry names a release and its image. The release flow touches Cargo.toml, so this
# is where a bump of server.json is made part of it; `mcp::tests::the_registry_entry_names_this_release`
# fails the release PR's CI for the same reason.
echo
reg_version=$(sed -n 's/^  "version": "\([^"]*\)".*/\1/p' server.json | head -1)
reg_image=$(sed -n 's/.*ghcr.io\/nuthatch-org\/nuthatch:\([^"]*\)".*/\1/p' server.json | head -1)
if [ "$reg_version" = "$WANT" ] && [ "$reg_image" = "$WANT" ]; then
  echo "server.json: OK ($reg_version)"
else
  echo "server.json: STALE (version $reg_version, image tag $reg_image, expected $WANT)"; fail=$((fail + 1))
fi

# SECURITY.md said "currently the 0.1.x line" through twelve 3.x releases (#1541). The section names
# no version at all now, so any number that appears in it is a regression.
echo
if sed -n '/^## Supported versions/,/^## /p' SECURITY.md | grep -qE '\b[0-9]+\.[0-9x]+(\.[0-9x]+)?\b'; then
  echo "SECURITY.md supported versions: STALE (names a version; say \"the latest release\" instead)"
  fail=$((fail + 1))
else
  echo "SECURITY.md supported versions: OK"
fi

# 3. Everything else, reported but never judged. `docs/rfcs/` and the progress log are dated writing
#    and *should* keep the version they were written about; so should a security note naming the
#    release that fixed something. Listing them without a verdict is deliberate - a checker that
#    cried wolf over history would be ignored within two releases.
echo
echo "other version mentions - review by eye, most are legitimate history:"
grep -rnoE "\b[0-9]+\.[0-9]+\.[0-9]+\b" --include="*.md" --include="*.toml" . 2>/dev/null \
  | grep -v "^./target" | grep -v "/rfcs/" \
  | grep -vE ":${WANT//./\\.}$" \
  | grep -viE "1\.95\.0|1\.85\.0|0\.0\.0|127\.0\.0|glibc|2\.3[0-9]\.[0-9]" \
  | sed 's/^/  /' | head -25

echo
if [ "$fail" -gt 0 ]; then
  echo "$fail stale reference(s) that a reader would act on - fix before tagging"
  exit 1
fi
echo "no stale references in anything a reader would copy-paste"
