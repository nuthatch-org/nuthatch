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
#
#    One verdict per image reference, not per line: a line naming two images must have both current.
#    A tag may carry a flavour suffix (`-scaled`, `-flight`) and may be a placeholder. A past
#    release's own notes (`docs/releases/vA.B.C.md`) name that release's tag, which is what they
#    should say, so there the tag is judged against A.B.C and not against the current version.
echo "container tags in docs and compose defaults (copy-paste commands):"
tags_seen=0
operators_seen=0
while IFS= read -r hit; do
  [ -z "$hit" ] && continue
  file=${hit%%:*}
  rest=${hit#*:}
  line=${rest%%:*}
  tag=${rest#*:}
  tag=${tag#ghcr.io/nuthatch-org/nuthatch:}
  case "$tag" in *'{'*) ;; *) tag=${tag%\}} ;; esac
  while case "$tag" in *.|*-) true ;; *) false ;; esac; do tag=${tag%?}; done
  want=$WANT
  case "$file" in
    ./docs/releases/v[0-9]*.md) want=$(basename "$file" .md); want=${want#v} ;;
  esac
  # A variable with a default (${TAG:-4.0.0}) is judged on its default; one without has no tag to judge.
  case "$tag" in
    *'$'*':-'*) tag=${tag#*:-}; tag=${tag%%\}*} ;;
  esac
  case "$tag" in
    "$want"|"$want"-*|'<version>'*|'{version}'*|*'$'*) verdict=OK ;;
    *) verdict=STALE; fail=$((fail + 1)) ;;
  esac
  tags_seen=$((tags_seen + 1))
  case "$file:$tag" in
    ./docs/operators.md:'<version>'*) ;;
    ./docs/operators.md:*) operators_seen=$((operators_seen + 1)) ;;
  esac
  printf "  %-5s %s:%s: %s\n" "$verdict" "$file" "$line" "$tag"
done < <(grep -rnoE "ghcr\.io/nuthatch-org/nuthatch:[A-Za-z0-9._<>{}:\$-]*" --include="*.md" --include="docker-compose*.yml" \
  --exclude-dir=target --exclude-dir=.git --exclude-dir=rfcs . 2>/dev/null)

# A check that finds nothing has stopped checking. operators.md is the document an operator follows
# verbatim, so it must keep naming at least one concrete image tag for this to judge.
if [ "$operators_seen" -eq 0 ]; then
  echo "  STALE docs/operators.md names no concrete image tag; the check below has nothing to read"
  fail=$((fail + 1))
fi
[ "$tags_seen" -eq 0 ] && { echo "  STALE no image tag found anywhere"; fail=$((fail + 1)); }

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

# SECURITY.md said "currently the 0.1.x line" through twelve 3.x releases (#1541). Each release now
# restates the policy for itself ("when 5.20.0 is released, 5.19 keeps ..."), so the section may name
# only the current minor and the one before it; any other line it names is a regression.
echo
core=${WANT%%-*}  # a pre-release version has the release's minor
current_minor=${core%.*}
major=${current_minor%.*}
minor=${current_minor#*.}
# The previous minor of X.Y is X.(Y-1). Of X.0 it is the last minor of the line before, which this
# script cannot know, so any (X-1).* is accepted there.
if [ "$minor" -gt 0 ]; then
  previous_minor="$major.$((minor - 1))"
  allowed="$current_minor $previous_minor"
else
  previous_minor="$((major - 1)).*"
  allowed="$current_minor"
fi
stale_minors=""
for named in $(sed -n '/^## Supported versions/,/^## /p' SECURITY.md \
  | grep -oE '[0-9]+\.([0-9]+|x)(\.[0-9x]+)?' | sed -E 's/^([0-9]+\.[0-9]+).*/\1/' | sort -u); do
  case " $allowed " in *" $named "*) continue ;; esac
  [ "$minor" -eq 0 ] && [ "${named%%.*}" = "$((major - 1))" ] && continue
  stale_minors="$stale_minors$named "
done
if [ -n "$stale_minors" ]; then
  echo "SECURITY.md supported versions: STALE (names ${stale_minors}; only $current_minor and $previous_minor may appear)"
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
