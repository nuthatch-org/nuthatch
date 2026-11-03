//! #829 - every third-party Action runs with this repository's credentials, so it must be pinned to
//! an immutable commit SHA rather than a tag anyone can move.
//!
//! The pinning itself was a morning's work. **This file is the part that matters**, because #913's
//! finding is that a gate written once and never exercised stops watching: a single unpinned `uses:`
//! added in six months' time restores the whole hazard, silently, and nothing else in the repo would
//! notice. A tag is not a version - it is a pointer, and the person who can move it is not
//! necessarily the person who published it.
//!
//! Deliberately a **shape** check rather than an allowlist of known-good SHAs. An allowlist is the
//! `CONFIG_SOURCES` failure mode that `doc_command_check.rs` and `analytics.rs` both call out: it
//! needs hand-editing on every legitimate bump, so it either blocks Dependabot or gets relaxed until
//! it means nothing. "Forty hex characters" needs no maintenance and cannot drift.

use std::path::{Path, PathBuf};

fn workflows() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".github/workflows")
}

/// Every `uses:` in every workflow, as `(file, line number, reference)`.
fn every_uses() -> Vec<(String, usize, String)> {
    let mut out = Vec::new();
    let dir = workflows();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "yml" || x == "yaml"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no workflow files found under {} - this gate would pass vacuously, which is exactly the \
         failure #913 is about",
        dir.display()
    );
    for f in files {
        let name = f.file_name().unwrap().to_string_lossy().to_string();
        for (i, line) in std::fs::read_to_string(&f).unwrap().lines().enumerate() {
            // Both spellings. A step in a list is `- uses: …`; a bare `uses:` appears under a
            // job key. Matching only the second found 15 of 53 on the day this was written, and the
            // floor assertion below is the only reason that undercount was ever noticed.
            let t = line.trim().trim_start_matches("- ").trim_start();
            if let Some(rest) = t.strip_prefix("uses:") {
                out.push((name.clone(), i + 1, rest.trim().to_string()));
            }
        }
    }
    out
}

/// A local composite action (`./.github/actions/foo`) is this repository's own code and carries no
/// third-party trust. Everything else is somebody else's code running with our credentials.
///
/// # #973: `docker://` used to be excluded here, on a claim with nothing behind it
///
/// The old comment said *"a `docker://` reference is pinned by digest elsewhere"*. There was no
/// elsewhere. No workflow used one, so the exclusion protected nothing, was verified by nothing, and
/// the first `uses: docker://image:latest` anyone added would have run mutable third-party code with
/// workflow credentials and passed this gate green.
///
/// A container image reference is third-party trust in exactly the way an action reference is - more
/// so, since it is an entire root filesystem rather than a script - so it is no longer excluded. It
/// is pinned by its own rule below, because the syntax differs: an action pins with `@<40-hex sha>`,
/// an image pins with `@sha256:<64-hex digest>`.
fn is_third_party(reference: &str) -> bool {
    !reference.starts_with('.')
}

/// Is this a container image reference rather than an action reference?
fn is_docker(reference: &str) -> bool {
    reference.starts_with("docker://")
}

/// A `docker://` reference is immutable only when it names a digest: `docker://img@sha256:<64 hex>`.
///
/// A tag - `:latest`, `:v1`, or no tag at all - is a mutable pointer the publisher can repoint at
/// any time, which is the whole reason actions are pinned to a sha rather than a tag.
fn pinned_digest(reference: &str) -> Option<&str> {
    let after = reference
        .split('#')
        .next()?
        .trim()
        .rsplit_once("@sha256:")?
        .1;
    (after.len() == 64 && after.chars().all(|c| c.is_ascii_hexdigit())).then_some(after)
}

fn pinned_sha(reference: &str) -> Option<&str> {
    let after_at = reference.split('#').next()?.trim().rsplit_once('@')?.1;
    (after_at.len() == 40 && after_at.chars().all(|c| c.is_ascii_hexdigit())).then_some(after_at)
}

#[test]
fn every_third_party_action_is_pinned_to_a_commit_sha() {
    let all = every_uses();
    let third_party: Vec<_> = all
        .iter()
        .filter(|(_, _, r)| is_third_party(r))
        .cloned()
        .collect();

    // Without this the test passes on an empty set - the "mechanism cannot fail" shape from #913.
    assert!(
        third_party.len() >= 20,
        "expected the workflows to use a substantial number of third-party actions, found {}. \
         Either the scan stopped matching reality or the workflows changed shape; check the parser \
         before relaxing this number.",
        third_party.len()
    );

    let unpinned: Vec<String> = third_party
        .iter()
        .filter(|(_, _, r)| {
            // #973: two syntaxes, two pins. An action pins with `@<40-hex sha>`; a container image
            // pins with `@sha256:<64-hex digest>`. Neither is satisfied by a tag.
            if is_docker(r) {
                pinned_digest(r).is_none()
            } else {
                pinned_sha(r).is_none()
            }
        })
        .map(|(f, n, r)| format!("  {f}:{n}  {r}"))
        .collect();

    assert!(
        unpinned.is_empty(),
        "these actions are pinned to a mutable ref, so whoever can move that tag can run code with \
         this repository's release credentials (#829). Pin each to a full 40-character commit SHA \
         and keep the version as a trailing comment:\n\n\
         \x20   uses: <owner>/<action>@<40-char-commit-sha> # <version>\n\n\
         Resolve the SHA with:  gh api repos/<owner>/<repo>/commits/<tag> --jq .sha\n\n{}",
        unpinned.join("\n")
    );
}

/// A pin with no version comment is unreadable: nobody can tell `a5f673d0` from `7c8d7d13` at a
/// glance, so a reviewer cannot see that a bump went from v4 to v5. The comment is what makes the
/// pin auditable, and #829 asks for it explicitly.
#[test]
fn every_pin_records_the_version_it_pins() {
    let missing: Vec<String> = every_uses()
        .into_iter()
        .filter(|(_, _, r)| is_third_party(r) && !is_docker(r) && pinned_sha(r).is_some())
        .filter(|(_, _, r)| !r.contains('#'))
        .map(|(f, n, r)| format!("  {f}:{n}  {r}"))
        .collect();
    assert!(
        missing.is_empty(),
        "a bare SHA is not reviewable - add the version it pins as a trailing comment so a bump is \
         legible in a diff:\n{}",
        missing.join("\n")
    );
}

/// Dependabot is the other half of #829. Immutable pins never advance on their own, so without a
/// bot raising them an action sits at a vulnerable commit forever and the hardening has simply
/// traded one risk for another.
#[test]
fn dependabot_can_advance_the_pins() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join(".github/dependabot.yml");
    let s = std::fs::read_to_string(&p).unwrap_or_else(|e| {
        panic!(
            "{} is missing ({e}). Pinning without it leaves every action frozen at whatever commit \
             was current the day it was pinned.",
            p.display()
        )
    });
    assert!(
        s.contains("github-actions"),
        "dependabot.yml exists but does not watch the `github-actions` ecosystem, so the pins this \
         repo just took on will never be raised:\n{s}"
    );
}

/// **#928.** `dtolnay/rust-toolchain` is the one action here whose *ref* is the configuration: the
/// `1.95.0` branch bakes the version into its own script and declares no `toolchain` input, while the
/// `nightly` branch defaults to `nightly`. Two branches of one repository, two different compilers.
///
/// Pinning both to SHAs (#829) removed the only signal that says which is which, and Dependabot walked
/// straight into it: its first run proposed rewriting the **nightly** pin to the **1.95.0** SHA with
/// the comment still reading `# nightly`. The fuzz job would have installed stable, `cargo-fuzz`
/// needs nightly, and nothing in the diff looked wrong.
///
/// `dependabot.yml` now ignores that dependency, but an ignore rule is a request rather than a
/// mechanism - somebody can still make this edit by hand. This is the mechanism: two distinct
/// toolchains must keep two distinct SHAs, checked offline, no network.
#[test]
fn the_two_rust_toolchain_pins_stay_distinct() {
    let mut by_comment: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        Default::default();
    for (file, line, r) in every_uses() {
        let Some(rest) = r.strip_prefix("dtolnay/rust-toolchain@") else {
            continue;
        };
        let (sha, comment) = rest.split_once('#').unwrap_or_else(|| {
            panic!("{file}:{line}: a rust-toolchain pin with no version comment is unreadable: {r}")
        });
        by_comment
            .entry(comment.trim().to_string())
            .or_default()
            .insert(sha.trim().to_string());
    }
    assert!(
        by_comment.len() >= 2,
        "expected at least two distinct rust-toolchain lines (a pinned release and nightly); found \
         {by_comment:?}. If the fuzz job stopped using nightly, that is the bug this guards."
    );
    for (comment, shas) in &by_comment {
        assert_eq!(
            shas.len(),
            1,
            "`# {comment}` is pinned to more than one SHA: {shas:?}"
        );
    }
    let all: std::collections::BTreeSet<&String> = by_comment.values().flatten().collect();
    assert_eq!(
        all.len(),
        by_comment.len(),
        "two rust-toolchain comments share a SHA, so two different toolchains resolve to the same \
         action branch - one of them is not the compiler its comment claims (#928):\n{by_comment:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// #973 regression controls.
//
// The workflows use no `docker://` reference today, which is exactly why the old exclusion was
// invisible: it protected nothing, was checked by nothing, and would have admitted the first one
// anybody added. A gate for a case that does not occur yet can only be proven on synthetic input.
// ---------------------------------------------------------------------------------------------

/// The classifier and both pin rules, driven directly. Returns `true` if the reference would be
/// reported as unpinned by `every_third_party_action_is_pinned_to_a_commit_sha`.
fn would_be_reported(reference: &str) -> bool {
    if !is_third_party(reference) {
        return false;
    }
    if is_docker(reference) {
        pinned_digest(reference).is_none()
    } else {
        pinned_sha(reference).is_none()
    }
}

#[test]
fn a_mutable_docker_reference_is_refused() {
    for r in [
        "docker://alpine:latest",
        "docker://alpine",
        "docker://alpine:3.20",
        "docker://ghcr.io/owner/image:v1 # v1",
        // A 40-hex sha is an *action* pin and means nothing to a registry: images are addressed by
        // a 64-hex sha256 digest, so this must not be accepted by the action rule leaking across.
        "docker://alpine@0123456789012345678901234567890123456789",
        // Right prefix, wrong length.
        "docker://alpine@sha256:0123456789abcdef",
    ] {
        assert!(
            would_be_reported(r),
            "`{r}` is a mutable or wrongly-pinned image reference and must be refused. Before #973 \
             every one of these passed, because `docker://` was excluded from the gate on the claim \
             that it was \"pinned by digest elsewhere\" - and there was no elsewhere."
        );
    }
}

#[test]
fn a_digest_pinned_docker_reference_is_accepted() {
    let digest = "a".repeat(64);
    for r in [
        format!("docker://alpine@sha256:{digest}"),
        format!("docker://ghcr.io/owner/image@sha256:{digest} # 3.20"),
    ] {
        assert!(
            !would_be_reported(&r),
            "`{r}` names an immutable digest and must be accepted, or the gate is unsatisfiable and \
             will simply be switched off by whoever needs a container step"
        );
    }
}

#[test]
fn a_local_action_is_still_exempt_and_an_action_still_needs_its_sha() {
    assert!(
        !would_be_reported("./.github/actions/setup"),
        "a local composite action is this repository's own code"
    );
    assert!(
        would_be_reported("actions/checkout@v4"),
        "the original rule must still hold - a tag is not a pin"
    );
    assert!(
        !would_be_reported("actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683 # v4"),
        "a 40-hex commit sha is the action pin and must still be accepted"
    );
}

// #2621: the same hazard through the other doors. A browser installed `@stable`, a tool installed
// with no version, or an archive piped straight into `tar` runs whatever is current that day, with
// this repository's token. These scan `ci.yml` and the local composite actions it calls.

fn ci_and_local_actions() -> Vec<(String, Vec<String>)> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = vec![root.join(".github/workflows/ci.yml")];
    for e in std::fs::read_dir(root.join(".github/actions")).unwrap() {
        let p = e.unwrap().path().join("action.yml");
        if p.is_file() {
            files.push(p);
        }
    }
    files
        .into_iter()
        .map(|f| {
            let lines = std::fs::read_to_string(&f)
                .unwrap()
                .lines()
                .map(str::to_string)
                .collect();
            (f.display().to_string(), lines)
        })
        .collect()
}

fn is_comment(line: &str) -> bool {
    line.trim_start().starts_with('#')
}

#[test]
fn ci_runs_with_a_read_only_token_unless_a_job_asks() {
    let ci = std::fs::read_to_string(workflows().join("ci.yml")).unwrap();
    let lines: Vec<&str> = ci.lines().collect();
    let at = lines.iter().position(|l| *l == "permissions:").expect(
        "ci.yml has no top-level `permissions:`, so every job gets the default token scope",
    );
    assert_eq!(
        lines.get(at + 1).map(|l| l.trim()),
        Some("contents: read"),
        "the top-level block should grant `contents: read` and nothing else"
    );
}

#[test]
fn every_archive_ci_fetches_is_checked_against_a_digest() {
    let mut bad = Vec::new();
    for (f, lines) in ci_and_local_actions() {
        for (i, l) in lines.iter().enumerate() {
            if is_comment(l) || !l.contains("curl ") {
                continue;
            }
            let remote =
                l.contains("https://") || lines.get(i + 1).is_some_and(|n| n.contains("https://"));
            let to_file = l.contains(" -o ");
            if remote && !to_file {
                bad.push(format!(
                    "{f}:{}: fetched without saving it to check: {}",
                    i + 1,
                    l.trim()
                ));
            }
            if to_file
                && !lines[i + 1..(i + 4).min(lines.len())]
                    .iter()
                    .any(|n| n.contains("sha256sum -c"))
            {
                bad.push(format!(
                    "{f}:{}: saved with no sha256sum -c after it: {}",
                    i + 1,
                    l.trim()
                ));
            }
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[test]
fn no_ci_tool_or_browser_is_installed_at_whatever_is_current() {
    let mut bad = Vec::new();
    for (f, lines) in ci_and_local_actions() {
        for (i, l) in lines.iter().enumerate() {
            if is_comment(l) {
                continue;
            }
            if l.contains("@stable") || l.contains("@latest") {
                bad.push(format!("{f}:{}: floating channel: {}", i + 1, l.trim()));
            }
            if l.contains("cargo install ") && !l.contains("--version") {
                bad.push(format!(
                    "{f}:{}: cargo install with no --version: {}",
                    i + 1,
                    l.trim()
                ));
            }
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}
