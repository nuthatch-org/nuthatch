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

/// The file names under every `key:` in the workflow, the inline value or a `|` block scalar's lines,
/// with `${{ … }}` expressions left intact - the comparison only needs the sets to be written the same
/// way, not to be resolved. A block ends at its first line holding a `:` outside an expression.
fn files_under(key: &str) -> Vec<String> {
    let s = release_yml();
    let lines: Vec<&str> = s.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let Some(rest) = line.trim().strip_prefix(key) else {
            continue;
        };
        let rest = rest.trim();
        if !rest.is_empty() && rest != "|" {
            out.push(rest.to_string());
            continue;
        }
        out.extend(
            lines[i + 1..]
                .iter()
                .map(|l| l.trim())
                .take_while(|l| !l.is_empty() && !l.replace("${{", "").contains(':'))
                .map(str::to_string),
        );
    }
    out
}

/// Not a release artifact: the module's README, attached so the release page explains the module.
const DOCUMENTATION: &str = "nuthatch-events-README.md";

/// Everything the workflow *attaches* to the release, from the `files:` blocks, less the checksum
/// sidecars and the README.
fn attached() -> BTreeSet<String> {
    files_under("files:")
        .into_iter()
        .filter(|f| !f.ends_with(".sha256") && f != DOCUMENTATION)
        .collect()
}

/// Everything the workflow *attests*, from `subject-path:`.
fn attested() -> BTreeSet<String> {
    files_under("subject-path:").into_iter().collect()
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

/// **The one that matters.** Every artifact the release publishes must carry provenance: tarballs,
/// the engine's module and host package, and their SBOMs (#3097). A new target, a new flavour or a
/// new kind of file added to `files:` and not to `subject-path:` would publish unattested, and nothing
/// else in the repo would notice.
#[test]
fn every_published_artifact_is_attested() {
    let attached = attached();
    let attested = attested();

    // One templated name since #2035 made each (target, variant) its own matrix job.
    assert!(
        attached.iter().any(|f| f.ends_with(".tar.gz"))
            && attached.contains("nuthatch-events.wasm")
            && attached.iter().any(|f| f.ends_with(".cdx.json")),
        "the attached set lacks a tarball, the module or an SBOM - the parser has stopped matching \
         the workflow, so this gate would pass vacuously (#913 shape 1). Fix the scan, do not relax \
         this floor.\nattached: {attached:?}"
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

/// #3097: every binary tarball and the engine's module ship a CycloneDX SBOM of the same name, and
/// so, by the gate above, under the same attestation. The SBOM is written by tools/sbom in the job
/// that built the artifact, before anything is attached, and `publish` refuses a release missing one
/// and verifies each like the artifact it describes.
#[test]
fn every_binary_and_the_module_ship_their_sbom() {
    let attached = attached();
    let wanted: Vec<String> = attached
        .iter()
        .filter_map(|f| {
            f.strip_suffix(".tar.gz")
                .or_else(|| f.strip_suffix(".wasm"))
                .map(|stem| format!("{stem}.cdx.json"))
        })
        .collect();
    assert!(
        wanted.len() >= 2,
        "no tarball or module found: {attached:?}"
    );
    let missing: Vec<&String> = wanted.iter().filter(|w| !attached.contains(*w)).collect();
    assert!(
        missing.is_empty(),
        "these SBOMs are not attached beside their artifact (#3097): {missing:?}"
    );

    let s = release_yml();
    for job in ["\n  build:", "\n  engine:"] {
        let body = s.split_once(job).expect("release.yml lost a job").1;
        let body = body.split("\n  container:").next().unwrap();
        let generate = body
            .find("--manifest-path tools/sbom/Cargo.toml")
            .unwrap_or_else(|| panic!("{job} does not run tools/sbom (#3097)"));
        let attach = body.find("softprops/action-gh-release").unwrap();
        assert!(
            generate < attach && body[generate..attach].contains("--deny deny.toml"),
            "{job} must write its SBOM before attaching, with deny.toml's licence check"
        );
    }

    let publish = s.split_once("\n  publish:").unwrap().1;
    let verify = publish.find("gh attestation verify").unwrap();
    assert!(
        publish[..verify].contains("--pattern '*.cdx.json'")
            && publish[..verify]
                .contains("files+=(nuthatch-events.wasm nuthatch-engine.js *.cdx.json)"),
        "publish does not download and verify the SBOMs before undrafting (#3097)"
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

/// The release workflow's `workflow_dispatch` is a dry run (RFC-0079 §16): it may build every binary,
/// and must never create, attach to, attest for or publish a release, or push an image. Every step
/// that would is gated on a tag push, by its own `if:` or its job's.
#[test]
fn a_dispatch_never_releases() {
    const RELEASING: [&str; 5] = [
        "softprops/action-gh-release",
        "attest-build-provenance",
        "docker push",
        "gh release edit",
        "npm publish",
    ];
    let s = release_yml();
    let jobs = s.split_once("\njobs:\n").expect("release.yml has jobs").1;
    let mut checked = 0;
    let mut ungated = Vec::new();
    let mut close = |job: &(String, bool), step: &mut Vec<&str>| {
        if RELEASING.iter().any(|r| step.iter().any(|l| l.contains(r))) {
            checked += 1;
            let own = step
                .iter()
                .any(|l| l.starts_with("        if:") && l.contains("github.event_name == 'push'"));
            if !own && !job.1 {
                ungated.push(format!("{} {}", job.0, step[0].trim()));
            }
        }
        step.clear();
    };
    let mut job = (String::new(), false);
    let mut step: Vec<&str> = Vec::new();
    for line in jobs.lines() {
        if line.starts_with("  ") && !line.starts_with("   ") && line.ends_with(':') {
            close(&job, &mut step);
            job = (line.trim().to_string(), false);
        } else if line.starts_with("    if:") {
            job.1 =
                line.contains("github.event_name == 'push'") && !line.contains("workflow_dispatch");
        } else if line.starts_with("      - ") {
            close(&job, &mut step);
            step.push(line);
        } else if !step.is_empty() {
            step.push(line);
        }
    }
    close(&job, &mut step);
    assert!(
        checked >= 6,
        "found only {checked} releasing steps; the scan has stopped matching release.yml"
    );
    assert!(
        ungated.is_empty(),
        "these steps would release from a workflow_dispatch dry run: {ungated:?}"
    );
}

/// #2735: an image is part of the release, so it goes public at the same gate as the binaries. No
/// job before `publish` may push one; `publish` pushes only after provenance verifies, attests each
/// pushed digest, verifies that attestation, and only then undrafts.
#[test]
fn images_go_public_only_past_the_publish_gate() {
    let s = release_yml();
    let (before, publish) = s
        .split_once("\n  publish:")
        .expect("release.yml has no `publish:` job any more");
    assert!(
        !before.contains("docker push"),
        "an image is pushed before the publish gate, so a failed engine or provenance check still \
         leaves it public (#2735)"
    );
    let at = |needle: &str| {
        publish
            .find(needle)
            .unwrap_or_else(|| panic!("the publish job has no `{needle}` (#2735)\n{publish}"))
    };
    let verify = at("gh attestation verify");
    let push = at("docker push");
    let attest = at("subject-digest:");
    let verify_image = at("gh attestation verify \"oci://");
    let move_latest = at("- name: move :latest");
    let undraft = at("--draft=false");
    assert!(
        verify < push
            && push < attest
            && attest < verify_image
            && verify_image < move_latest
            && move_latest < undraft,
        "the publish job must verify the binaries, push, attest the pushed digest, verify that, \
         move :latest, and only then undraft (#2735)"
    );
}

/// #2741: `--repo` alone accepts an attestation any workflow on any ref of the repository signed, so a
/// branch's workflow could attest a file and pass. Every verify, the workflow's and the documented
/// ones, names the release workflow and the tag it ran on.
#[test]
fn every_verify_names_the_release_workflow_and_its_tag() {
    const SIGNER: &str = "--signer-workflow nuthatch-org/nuthatch/.github/workflows/release.yml";
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let read = |p: &str| {
        std::fs::read_to_string(root.join(p)).unwrap_or_else(|e| panic!("cannot read {p}: {e}"))
    };
    let mut unbound = Vec::new();
    let mut seen = 0;
    for (name, text, signer) in [
        (
            "release.yml",
            release_yml(),
            "--signer-workflow \"$GITHUB_REPOSITORY/.github/workflows/release.yml\"",
        ),
        ("README.md", read("README.md"), SIGNER),
        ("docs/install.md", read("docs/install.md"), SIGNER),
        (
            "scripts/live-smoke.sh",
            read("scripts/live-smoke.sh"),
            "--signer-workflow \"$REPO/.github/workflows/release.yml\"",
        ),
    ] {
        let text = text.replace("\\\n", " ");
        for line in text
            .lines()
            .filter(|l| l.contains("gh attestation verify "))
        {
            seen += 1;
            if !(line.contains(signer) && line.contains("--source-ref")) {
                unbound.push(format!("{name}: {}", line.trim()));
            }
        }
    }
    assert!(
        seen >= 6,
        "found only {seen} verify commands; the scan has stopped matching"
    );
    assert!(
        unbound.is_empty(),
        "these verify commands accept an attestation from any workflow or ref of the repository \
         (#2741): {unbound:#?}"
    );
}

/// #2741: a `v*` tag runs the workflow file of the commit it names, so a tag on an unmerged commit
/// skips main's required checks. The first job refuses a commit that is on neither `main` nor the
/// tag's own `release/<major>.<minor>` branch (docs/releases/README.md), before any release exists.
#[test]
fn a_tag_off_main_and_its_release_branch_is_refused_first() {
    let s = release_yml();
    let notes = s
        .split_once("\n  release-notes:")
        .expect("release.yml has no `release-notes:` job")
        .1
        .split_once("\n  build:")
        .expect("release.yml has no `build:` job after release-notes")
        .0;
    let check = notes
        .find("git merge-base --is-ancestor \"$GITHUB_SHA\" origin/main")
        .unwrap_or_else(|| {
            panic!("release-notes does not check the tag is on main (#2741)\n{notes}")
        });
    assert!(
        notes.contains("git merge-base --is-ancestor \"$GITHUB_SHA\" \"origin/release/"),
        "release-notes does not accept a tag on its release branch, which is how a patch is cut"
    );
    assert!(
        notes.contains("fetch-depth: 0"),
        "the checkout is shallow, so `merge-base --is-ancestor` cannot see main's history"
    );
    let draft = notes
        .find("softprops/action-gh-release")
        .expect("release-notes no longer creates the draft");
    assert!(
        check < draft,
        "the reachability check runs after the draft release is created"
    );
}
