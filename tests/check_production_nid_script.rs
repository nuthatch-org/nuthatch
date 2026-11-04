//! `scripts/check-production-nid.sh`, which the Helsinki roll runs before touching the allocations
//! nest. A fake binary answers `nest nid` from a `NID` file in the directory it is given, and a local
//! repository stands in for graph-allocations-nest's `production` branch.

use std::path::{Path, PathBuf};
use std::process::Command;

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct Rig {
    _dir: tempfile::TempDir,
    bin: PathBuf,
    repo: PathBuf,
    live: PathBuf,
}

fn git(dir: &Path, args: &[&str]) {
    let ok = Command::new("git")
        .args([
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "init.defaultBranch=main",
        ])
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap()
        .success();
    assert!(ok, "git {args:?}");
}

/// `bin` prints the NID in `<dir>/NID`; `fake` replaces that with whatever the case needs.
fn rig(branch_nid: &str, live_nid: &str, fake: &str) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("nuthatch");
    std::fs::write(&bin, format!("#!/usr/bin/env bash\nd=$4\n{fake}\n")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let repo = dir.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-q"]);
    std::fs::write(repo.join("NID"), branch_nid).unwrap();
    git(&repo, &["add", "NID"]);
    git(&repo, &["commit", "-qm", "nest"]);
    git(&repo, &["branch", "-q", "production"]);
    let live = dir.path().join("live");
    std::fs::create_dir(&live).unwrap();
    std::fs::write(live.join("NID"), live_nid).unwrap();
    Rig {
        _dir: dir,
        bin,
        repo,
        live,
    }
}

fn run(r: &Rig, extra: &[&str]) -> (Option<i32>, String) {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/check-production-nid.sh");
    let out = Command::new("bash")
        .arg(script)
        .arg(&r.bin)
        .arg(&r.live)
        .args(extra)
        .env("PRODUCTION_REPO", &r.repo)
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code(), text)
}

const FROM_FILE: &str = "cat \"$d/NID\"";

#[test]
fn matching_files_pass() {
    let (code, out) = run(&rig(A, A, FROM_FILE), &[]);
    assert_eq!(code, Some(0), "{out}");
    assert!(out.contains("ok   production"), "{out}");
}

#[test]
fn a_drifted_box_is_refused_naming_both_nids() {
    let (code, out) = run(&rig(A, B, FROM_FILE), &[]);
    assert_eq!(code, Some(4), "{out}");
    assert!(out.contains(A) && out.contains(B), "{out}");
}

#[test]
fn an_override_rolls_a_drifted_box_and_says_so() {
    let (code, out) = run(&rig(A, B, FROM_FILE), &["--override"]);
    assert_eq!(code, Some(0), "{out}");
    assert!(out.contains("OVERRIDE"), "{out}");
}

/// Jules on #2665: a binary that fails on both sides must not leave two empty NIDs to compare equal.
#[test]
fn a_failing_binary_is_refused_not_passed() {
    let (code, out) = run(&rig(A, A, "echo boom >&2; exit 3"), &[]);
    assert_eq!(code, Some(5), "{out}");
    assert!(out.contains("boom"), "{out}");
}

#[test]
fn a_binary_that_prints_no_nid_is_refused() {
    let (code, out) = run(&rig(A, A, "echo"), &[]);
    assert_eq!(code, Some(5), "{out}");
    assert!(out.contains("printed no NID"), "{out}");
}
