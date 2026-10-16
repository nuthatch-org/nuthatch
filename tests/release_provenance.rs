//! #830 - a checksum downloaded from the same release as the binary it describes establishes nothing
//! about *who built the binary*.
//!
//! Anyone able to replace a tarball can replace its `.sha256` in the same breath, and a compromised
//! release credential or a retagged Action (#829) leaves the pair perfectly consistent. Build
//! provenance attestations answer the question the sidecar cannot - which repository, which commit,
//! which workflow run - and they are signed by GitHub's OIDC identity rather than by a key we would
//! have to hold and rotate.
//!
//! This file exists because #913's finding is that gates stop watching. Specifically, the failure mode
//! here is a **new artifact added to the release and never attested**: the workflow would go green, the
//! release would publish, and one of its binaries would silently carry no provenance at all. So the set
//! of attested subjects is not a hand-kept list - it is derived from the workflow's own `files:` blocks
//! and required to cover them, the same parser-derived discipline `src/analytics.rs` settled on after
//! four hand-kept denylists in a row came up short.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// The workflow **with full-line comments removed**.
///
/// Not a nicety. The first version of this file grepped the raw text, and two of its four gates then
/// survived their own mutation: deleting `id-token: write` left the *comment above it* saying
/// `id-token: write`, and gutting the verify step left the comment above that mentioning
/// `gh attestation verify`. Both gates went on passing while the thing they guard was gone - #913's
/// shape 3, produced by hand, inside #913's own sprint, by someone who had just written the issue.
///
/// A gate that documents itself well enough will match its own prose. Scan the code, not the essay.
fn release_yml() -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".github/workflows/release.yml");
    let raw =
        std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("cannot read {}: {e}", p.display()));
    let stripped: String = raw
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        stripped.len() < raw.len(),
        "no comments were stripped from release.yml - either the file lost its commentary or this \
         filter has stopped working, and the gates below would be matching prose again"
    );
    stripped
}

/// Tarball names referenced anywhere in the workflow, with `${{ … }}` expressions left intact - the
/// comparison only needs the two sets to be written the same way, not to be resolved.
fn tarballs_in(section: &str) -> BTreeSet<String> {
    section
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_prefix("- ").or(Some(l)))
        .map(str::trim)
        .filter(|l| l.ends_with(".tar.gz"))
        .map(|l| l.trim_start_matches("subject-path:").trim().to_string())
        .collect()
}

/// Everything the workflow *attaches* to the release, from the `files:` blocks.
fn attached() -> BTreeSet<String> {
    let s = release_yml();
    let mut out = BTreeSet::new();
    let mut in_files = false;
    for line in s.lines() {
        let t = line.trim();
        if t.starts_with("files:") {
            in_files = true;
            continue;
        }
        if in_files {
            if t.ends_with(".tar.gz") {
                out.insert(t.to_string());
            } else if !t.ends_with(".tar.gz.sha256") {
                in_files = false;
            }
        }
    }
    out
}

/// Everything the workflow *attests*, from `subject-path:`.
fn attested() -> BTreeSet<String> {
    let s = release_yml();
    let mut out = BTreeSet::new();
    for (i, line) in s.lines().enumerate() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("subject-path:") {
            let rest = rest.trim();
            if rest.is_empty() || rest == "|" {
                // A block scalar: take the indented lines that follow.
                out.extend(tarballs_in(
                    &s.lines().skip(i + 1).take(6).collect::<Vec<_>>().join("\n"),
                ));
            } else {
                out.insert(rest.to_string());
            }
        }
    }
    out
}

/// The permissions an attestation needs. `id-token: write` is what lets the job obtain the
/// short-lived OIDC certificate it signs with; without it the action fails at runtime, which would at
/// least be loud. `attestations: write` stores the result.
#[test]
fn the_release_workflow_may_mint_attestations() {
    let s = release_yml();
    for key in ["id-token: write", "attestations: write"] {
        assert!(
            s.contains(key),
            "release.yml does not grant `{key}`, so attest-build-provenance cannot run (#830)"
        );
    }
}

/// **The one that matters.** Every artifact the release publishes must carry provenance. A new target
/// or a new flavour added to `files:` and not to `subject-path:` would publish unattested, and nothing
/// else in the repo would notice.
#[test]
fn every_published_tarball_is_attested() {
    let attached = attached();
    let attested = attested();

    // One templated name since #2035 made each (target, variant) its own matrix job.
    assert!(
        !attached.is_empty(),
        "found only {} attached tarball(s) in release.yml - the parser has stopped matching the \
         workflow, so this gate would pass vacuously (#913 shape 1). Fix the scan, do not relax \
         this floor.\nattached: {attached:?}",
        attached.len()
    );

    let unattested: Vec<&String> = attached.difference(&attested).collect();
    assert!(
        unattested.is_empty(),
        "these artifacts are attached to the release but never attested, so they publish with no \
         provenance at all (#830):\n  {:?}\n\nattested: {:?}\n\nAdd an `actions/attest-build-provenance` \
         step naming each, on the job that BUILT it - attesting a file downloaded back from the \
         release would only certify that the download succeeded.",
        unattested, attested
    );
}

/// The attestation is only load-bearing if something refuses to publish without it. #830 asks for the
/// workflow to verify artifact identity *before* publishing, and order is the whole point: a check
/// that runs after the release is public has not prevented anything.
#[test]
fn publication_is_gated_on_verifying_provenance() {
    let s = release_yml();
    let publish = s
        .split_once("\n  publish:")
        .unwrap_or_else(|| panic!("release.yml has no `publish:` job any more"))
        .1;

    let verify_at = publish.find("gh attestation verify").unwrap_or_else(|| {
        panic!(
            "the publish job does not verify provenance before undrafting. Without it the \
             attestation is decorative: nothing refuses to publish a release whose provenance does \
             not check out (#830).\n{publish}"
        )
    });
    let undraft_at = publish
        .find("--draft=false")
        .expect("the publish job no longer undrafts - has the release flow changed shape?");

    assert!(
        verify_at < undraft_at,
        "provenance is verified AFTER the release goes public, which prevents nothing. The verify \
         step must precede the undraft."
    );
    assert!(
        publish.contains("gh attestation verify") && publish.contains("--repo"),
        "`gh attestation verify` without `--repo` accepts an attestation from any repository, which \
         is most of the property being established"
    );
}

/// A verification nobody is told about is not a verification. #830 asks for it in the install path.
#[test]
fn the_install_path_documents_verification() {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md");
    let s = std::fs::read_to_string(&p).expect("README.md");
    assert!(
        s.contains("gh attestation verify"),
        "README.md does not tell a user how to verify a downloaded binary's provenance (#830). The \
         command exists and the release is signed; a reader who is never shown it gains nothing."
    );
    assert!(
        s.contains("--repo nuthatch-org/nuthatch"),
        "the documented verify command omits `--repo`, which would accept an attestation from any \
         repository - teaching the reader a check that does not check"
    );
}

/// Not an assertion - a printout, so `cargo test -- --nocapture` shows what the parser above actually
/// sees. Written after a sibling gate in this sprint silently matched 15 of 53 lines and passed.
#[test]
fn show_what_the_parser_sees() {
    println!("attached: {:#?}", attached());
    println!("attested: {:#?}", attested());
}
