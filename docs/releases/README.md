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

`server.json`, the MCP Registry entry, is bumped in the same PR: its `version`, its `ghcr.io/nuthatch-org/nuthatch:<version>` image tag and its npm package's `version`, with `npm/package.json`. `scripts/version-check.sh` checks all four, and a unit
test fails the release PR's CI if they lag. Once the tag's image and npm package are published, run the manual `mcp registry`
workflow to publish the entry: the registry refuses an entry whose npm version does not exist yet.

The same PR reruns `scripts/build-directory-plugin.sh` and commits `plugin-directory/`: its `plugin.json` takes
the version from `Cargo.toml`, and CI's `scripts/build-directory-plugin.sh --check` fails until it is regenerated.

No em dashes - spaced hyphen, matching the rest of the project's public-facing prose.

## Patch releases on an older line

A patch only fixes, and `main` carries the next minor's features, so a patch to a minor that is no
longer the newest is not cut from `main`. It is cut from `release/<major>.<minor>` (`release/4.0`,
created at the `v4.0.0` tag): the fix lands on `main` first, is cherry-picked onto the release branch
with the version bump and its notes, and the tag goes on that branch's merge commit. The notes file is then copied to `main` so the
record is in one place. 4.0.1 was the first.

Then merge the release branch back into `main` with `git merge -s ours release/<major>.<minor>`, as
a merged PR (not a squash). The fixes are already on `main`, so this changes no file; it makes the
release PR's merge commit an ancestor of `main`, which `sprint-landed`'s Rule B requires 14 days
after the merge. `release/4.15` and `release/4.0` were not, and failed it (#2392).

`release.yml` needs nothing different for such a tag. It moves the image's `:latest` and the GitHub
release's Latest badge only when `scripts/release-latest.sh` says the tag is the highest stable
version, its npm job publishes a lower version under `line-<major>.<minor>` rather than `latest`, and
it verifies provenance against the tag either way. Do not
run the manual `mcp registry` workflow for it: the registry entry names the newest release.

## Long-term support lines

Proposed in #3098; the operator-facing promise is in [SECURITY.md](../../SECURITY.md#long-term-support).
Sized for one maintainer who tags every release, with AI workers preparing the branches.

**Cadence and window.** One LTS minor is designated in the first week of January and of July. It gets
security fixes for 9 months from its designation date, so two lines overlap for 3 months and there
are never more than two at once. Every other minor keeps the 30-day window.

**Which minor.** The newest minor that, on the designation day:

1. has been out for at least 14 days, so it has had the soak that a minor every few days otherwise
   never gets;
2. has passed the [release gate](../release-gate.md) and has no open issue reporting a regression
   or a security problem against it;
3. is not the `.0` of a new major, whose first weeks are when the stability contract is newest.

If the newest minor fails one of these, take the one before it, and so on. The designation is a
PR on `main` that adds the SECURITY.md row (line, designated date, end date, `Supported`); the date
is the day that PR merges.

**Designating a line**, in order:

1. Create the branch at the minor's tag if it does not exist yet, and give it `main`'s required checks:
   `git push origin v<X.Y.0>^{commit}:refs/heads/release/<X.Y>`, then
   `scripts/protect-branch.sh release/<X.Y>`. If a patch already exists on the line, the branch does
   too; use it.
2. Rehearse the upgrade an operator will make, from the previous LTS to this one:
   `scripts/rolling-upgrade.sh --previous v<previous LTS> <this line's binary>`. Put its MODE and
   result line in the designation PR. A failure blocks the designation, not the minor.
3. Merge the designation PR: the SECURITY.md row and nothing else. `scripts/version-check.sh` fails
   a row without a designated date and an end date.

**What is backported.** A fix for something [SECURITY.md](../../SECURITY.md) puts in scope, including
a dependency update for an advisory that reaches a shipped path. Nothing else: no features, no
ordinary bug fixes, no performance work. One exception: a change to CI or release tooling that puts
no new code in the binary (a retired runner image, an expired action pin) may land when the line
would otherwise be unreleasable.

**Cutting an LTS patch.** The process above, with these specifics:

1. The fix lands on `main` first. For a privately reported issue, every supported line's patch is
   prepared before disclosure and they are tagged together, because publishing one fix discloses it
   for the others. For an issue that is already public (an upstream advisory), the newest minor
   ships first and the LTS patch follows, normally within 7 days.
2. A branch named `backport/<X.Y>/<issue>` (an AI worker's is `agent/<id>`, as always) is cut from
   `release/<X.Y>`, and the fix is applied with `git cherry-pick -x <sha>` so the commit names its
   origin on `main`. The PR targets `release/<X.Y>` and carries the version bump, the notes and the
   version-file updates listed at the top of this page. The notes say `LTS, security fixes only`
   and carry the usual **Compatibility and upgrade** section, which for an LTS patch is always
   "replace the binary and restart".
3. If the cherry-pick does not apply cleanly, the rewrite is reviewed as new code. If it needs more
   than the fix (a format change, a new config key, an API change), stop: the advisory says the line
   does not get this fix and names the release to upgrade to. A rewrite that cannot be reviewed with
   confidence is worse than an honest "upgrade".
4. CI runs on the PR as on any other. `cargo deny` reads the live advisory database, so a new
   RUSTSEC entry turns an LTS branch red months after the cut. That is a signal, not noise: triage it
   as a backport candidate (update the crate, or an `ignore` with its reason, as on `main`).
5. The release gate is optional for a patch, as for any release; if run, pass
   `--production v<previous patch on the line>`, because the default baseline is the newest minor.
6. The maintainer tags the merge commit on `release/<X.Y>` and pushes the tag. Then copy the notes
   to `main` and merge the branch back with `git merge -s ours`, as above.

Several low-severity fixes that are already public may be batched into one patch.

**Ending a line.** On its end date the row's status becomes `Ended` (`scripts/version-check.sh` notes
a row past its date that does not say so). The branch, its tags and its releases stay: they are the
record, and an operator may still build from them. A line is not ended early.

**First line, and the rehearsal #3098 asks for.** The first designation is due in the first week of
January 2027. Before it is announced, the whole path is run once on its branch with a prerelease tag,
`v<X.Y.1>-rc.1`, carrying no code change: CI on the branch, `release.yml` from the branch, provenance
verified against that tag, and the Latest badge, `:latest` and npm's `latest` left where they were.
A prerelease is the right vehicle because `release.yml` never lets one take any of the three. It
does move npm's `next` dist-tag to the rehearsal; move it back with `npm dist-tag` if `next` was
pointing at a newer prerelease.

## Pre-releases

A tag carrying a semver pre-release identifier - the `-` in `v3.0.0-alpha.1` - is published as a
GitHub pre-release. The workflow decides this from the tag itself, so nothing is ticked by hand
and nothing is remembered. A pre-release never takes the repository's "Latest release" badge,
which is the whole point: `curl | sh` and anyone arriving at the repo front page continue to be
offered the last stable version.

`action-gh-release`'s own `prerelease` input defaults to **false**, so this had to be stated
rather than left to the default; the `publish` job restates it on the undraft and prints
`prerelease=` alongside `draft=` so the tag run says out loud what it did.
