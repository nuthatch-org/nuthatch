# Release notes

`release.yml`'s `release-notes` job reads `docs/releases/<tag>.md` (e.g. `docs/releases/v2.6.0.md`)
and uses it as the GitHub release body. Write that file on the release branch, alongside the
`Cargo.toml` version bump, before tagging.

The same release PR bumps the default image in `docker-compose.scaled.yml` and
`docker-compose.writer-node.yml` to the new version: nothing rewrites them, and
`tests/compose_image_is_current.rs` fails CI until they match `Cargo.toml`.

The same PR bumps the two `docker run` image tags in `docs/operators.md` and, in `SECURITY.md`, the
supported-versions sentence ("When 5.20.0 is released, 5.19 keeps receiving security fixes ..."), which
names the new minor and the one before it (at X.0, any minor of the previous major, which the script cannot know). `scripts/version-check.sh` fails CI's `fmt · clippy · build
(default)` job until both match `Cargo.toml`.

If the notes file is missing when the tag is pushed, the `release-notes` job fails and nothing further
runs - no build, no empty release to edit afterward. There is no fallback to an empty body; a
missing notes file is a build failure, not a warning.

`server.json`, the MCP Registry entry, is bumped in the same PR: its `version` and its `ghcr.io/nuthatch-org/nuthatch:<version>` image tag. `scripts/version-check.sh` checks both, and a unit
test fails the release PR's CI if they lag. Once the tag's image is published, run the manual `mcp registry`
workflow to publish the entry.

No em dashes - spaced hyphen, matching the rest of the project's public-facing prose.

## Patch releases on 4.x

A 4.x patch only fixes, and `main` carries the next minor's features, so a patch is not cut from
`main`. It is cut from `release/<major>.<minor>` (`release/4.0`, created at the `v4.0.0` tag): the
fix lands on `main` first, is cherry-picked onto the release branch with the version bump and its
notes, and the tag goes on that branch's merge commit. The notes file is then copied to `main` so the
record is in one place. 4.0.1 was the first.

Then merge the release branch back into `main` with `git merge -s ours release/<major>.<minor>`, as
a merged PR (not a squash). The fixes are already on `main`, so this changes no file; it makes the
release PR's merge commit an ancestor of `main`, which `sprint-landed`'s Rule B requires 14 days
after the merge. `release/4.15` and `release/4.0` were not, and failed it (#2392).

## Pre-releases

A tag carrying a semver pre-release identifier - the `-` in `v3.0.0-alpha.1` - is published as a
GitHub pre-release. The workflow decides this from the tag itself, so nothing is ticked by hand
and nothing is remembered. A pre-release never takes the repository's "Latest release" badge,
which is the whole point: `curl | sh` and anyone arriving at the repo front page continue to be
offered the last stable version.

`action-gh-release`'s own `prerelease` input defaults to **false**, so this had to be stated
rather than left to the default; the `publish` job restates it on the undraft and prints
`prerelease=` alongside `draft=` so the tag run says out loud what it did.
