//! #3150: `scripts/npm-dist-tag.sh`, which `release.yml` asks which npm dist-tag a version publishes
//! under. A registry error other than E404 must fail, not read as "latest is unset".

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

/// Runs the script with a stand-in `npm` that prints `stdout`, writes `stderr` and exits `code`.
fn dist_tag(version: &str, stdout: &str, stderr: &str, code: i32) -> (bool, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let npm = dir.path().join("npm");
    std::fs::write(
        &npm,
        format!("#!/bin/sh\nprintf '%s' '{stdout}'\nprintf '%s' '{stderr}' >&2\nexit {code}\n"),
    )
    .unwrap();
    std::fs::set_permissions(&npm, std::fs::Permissions::from_mode(0o755)).unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/npm-dist-tag.sh");
    let out = Command::new("bash")
        .arg(script)
        .arg(version)
        .env("NPM", &npm)
        .output()
        .expect("bash must be on PATH");
    (
        out.status.success(),
        String::from_utf8(out.stdout).unwrap().trim().to_string(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

#[test]
fn a_registry_error_other_than_404_fails() {
    for stderr in [
        "npm error code E503\nnpm error 503 Service Unavailable",
        "npm error code ENOTFOUND\nnpm error network request failed",
        "",
    ] {
        let (ok, out, _) = dist_tag("5.23.1", "", stderr, 1);
        assert!(!ok, "stderr {stderr:?} must fail");
        assert_eq!(out, "", "no tag may be printed on failure");
    }
}

#[test]
fn an_unpublished_package_takes_latest() {
    let (ok, out, _) = dist_tag(
        "5.23.1",
        "",
        "npm error code E404\nnpm error 404 Not Found",
        1,
    );
    assert!(ok);
    assert_eq!(out, "latest");
}

#[test]
fn a_package_with_no_latest_tag_takes_latest() {
    let (ok, out, _) = dist_tag("5.23.1", "", "", 0);
    assert!(ok);
    assert_eq!(out, "latest");
}

#[test]
fn the_newest_release_takes_latest() {
    assert_eq!(dist_tag("5.23.0", "5.22.4\n", "", 0).1, "latest");
    assert_eq!(dist_tag("5.10.0", "5.9.9\n", "", 0).1, "latest");
}

#[test]
fn a_patch_on_an_older_line_takes_its_line_tag() {
    let (ok, out, _) = dist_tag("5.22.5", "5.23.0\n", "", 0);
    assert!(ok);
    assert_eq!(out, "line-5.22");
}

#[test]
fn a_prerelease_takes_next_without_asking_the_registry() {
    // Exit 1 with a 503: the registry is never consulted, so the failure cannot matter.
    let (ok, out, _) = dist_tag("6.0.0-rc.1", "", "npm error code E503", 1);
    assert!(ok);
    assert_eq!(out, "next");
}
