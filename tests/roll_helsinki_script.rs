//! #1848 - `deploy/roll-helsinki-from-mac.sh` refuses a release whose gates did not pass.
//!
//! 4.3.1 carried a red `release-gate/alloc-nest` status that nothing enforced. Before touching a
//! unit, the roll reads the tagged commit's statuses and needs `success` on each rolled unit's nest.
//! These tests run the real script with a fake `gh` that answers the tag's commit and its combined
//! statuses, and a fake `ssh` that records whether the box was ever reached.

use std::path::{Path, PathBuf};
use std::process::Command;

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deploy/roll-helsinki-from-mac.sh")
}

/// `api repos/<repo>/commits/v<V>` answers `sha-<V>`; `api .../commits/<sha>/status` answers
/// `$FAKE_STATE/status-<sha>` as the script's `--jq` would render it, one tab-separated
/// `context state description url` line per context.
const FAKE_GH: &str = r#"#!/usr/bin/env bash
case "$1 $2" in
  "api "*/status|"api "*/status\?*) sha=${2%%\?*}; sha=${sha%/status}; sha=${sha##*/}; cat "$FAKE_STATE/status-$sha" 2>/dev/null; exit 0 ;;
  "api "*/commits/v*) echo "sha-${2##*/v}" ;;
  *) echo "fake gh: unexpected call: $*" >&2; exit 3 ;;
esac
"#;

const FAKE_SSH: &str = r#"#!/usr/bin/env bash
cat >/dev/null
printf '%s\n' "$*" >>"$FAKE_STATE/ssh"
"#;

const NESTS: [&str; 6] = [
    "alloc-nest",
    "gns-nest",
    "dips-nest",
    "data-services-nest",
    "staking-archive-nest",
    "qos-nest",
];

struct Rig {
    _root: tempfile::TempDir,
    path: String,
    state: PathBuf,
    smoke: PathBuf,
}

fn rig() -> Rig {
    let root = tempfile::tempdir().unwrap();
    let fakes = root.path().join("fakes");
    let state = root.path().join("state");
    let smoke = root.path().join("smoke");
    for d in [&fakes, &state, &smoke] {
        std::fs::create_dir_all(d).unwrap();
    }
    for (name, body) in [("gh", FAKE_GH), ("ssh", FAKE_SSH)] {
        let p = fakes.join(name);
        std::fs::write(&p, body).unwrap();
        make_executable(&p);
    }
    let path = format!("{}:{}", fakes.display(), std::env::var("PATH").unwrap());
    Rig {
        _root: root,
        path,
        state,
        smoke,
    }
}

fn make_executable(p: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Every gate nest green on 4.3.1's commit, except the overrides given as `(nest, state)`; a state
/// of `None` leaves that nest's context off the commit altogether.
fn statuses(r: &Rig, except: &[(&str, Option<&str>)]) {
    let mut out = String::new();
    for nest in NESTS {
        let state = match except.iter().find(|(n, _)| *n == nest) {
            Some((_, None)) => continue,
            Some((_, Some(s))) => *s,
            None => "success",
        };
        out.push_str(&format!(
            "release-gate/{nest}\t{state}\t{nest} {state} against 4.3.0\t\
             https://github.com/nuthatch-org/nuthatch/actions/runs/{nest}\n"
        ));
    }
    std::fs::write(r.state.join("status-sha-4.3.1"), out).unwrap();
}

fn roll(r: &Rig, args: &[&str]) -> (bool, String) {
    let out = Command::new("bash")
        .arg(script())
        .arg("4.3.1")
        .args(args)
        .env("PATH", &r.path)
        .env("FAKE_STATE", &r.state)
        .env("SMOKE_DIR", &r.smoke)
        .env("ROLL_HOST", "root@box.invalid")
        .output()
        .expect("run roll-helsinki-from-mac.sh");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

fn reached_the_box(r: &Rig) -> Option<String> {
    std::fs::read_to_string(r.state.join("ssh")).ok()
}

#[test]
fn a_release_green_on_every_nest_rolls_every_unit() {
    let r = rig();
    statuses(&r, &[]);
    let (ok, out) = roll(&r, &[]);
    assert!(ok, "a fully gated release was refused:\n{out}");
    assert!(
        out.contains("ok   gates: "),
        "a passed check must say so:\n{out}"
    );
    let ssh =
        reached_the_box(&r).unwrap_or_else(|| panic!("the roll never reached the box:\n{out}"));
    for unit in [
        "graph-allocations-nest-next",
        "data-services-nest",
        "graph-gns-nest-next",
        "nuthatch-dips",
        "graph-staking-legacy-readonly",
    ] {
        assert!(ssh.contains(unit), "{unit} was not rolled: {ssh}");
    }
}

#[test]
fn a_red_nest_refuses_the_roll_and_names_its_status() {
    let r = rig();
    statuses(&r, &[("alloc-nest", Some("failure"))]);
    let (ok, out) = roll(&r, &[]);
    assert!(!ok, "a release red on alloc-nest rolled:\n{out}");
    assert!(reached_the_box(&r).is_none(), "the box was touched:\n{out}");
    assert!(out.contains("release-gate/alloc-nest"), "{out}");
    assert!(out.contains("alloc-nest failure against 4.3.0"), "{out}");
    assert!(
        out.contains("https://github.com/nuthatch-org/nuthatch/actions/runs/alloc-nest"),
        "{out}"
    );
    assert!(
        !out.contains("release-gate/gns-nest"),
        "a green nest was named:\n{out}"
    );
}

#[test]
fn an_error_status_refuses_too() {
    let r = rig();
    statuses(&r, &[("dips-nest", Some("error"))]);
    let (ok, out) = roll(&r, &[]);
    assert!(!ok, "a release in error on dips-nest rolled:\n{out}");
    assert!(reached_the_box(&r).is_none(), "the box was touched:\n{out}");
    assert!(out.contains("release-gate/dips-nest"), "{out}");
}

/// The gate lookup stops at the first matching context. Fed through a pipe, that left the writer to
/// die of SIGPIPE, and `pipefail` with `set -e` ended the roll silently with nothing said.
/// Past a pipe's buffer the race becomes a certainty.
#[test]
fn a_long_status_list_refuses_with_its_reason() {
    let r = rig();
    statuses(&r, &[("dips-nest", Some("error"))]);
    let path = r.state.join("status-sha-4.3.1");
    let mut list = std::fs::read_to_string(&path).unwrap();
    for i in 0..2000 {
        list.push_str(&format!(
            "ci/other-{i}\tsuccess\t{:>100}\thttps://example.invalid\n",
            i
        ));
    }
    std::fs::write(&path, list).unwrap();
    let (ok, out) = roll(&r, &[]);
    assert!(!ok, "a release in error on dips-nest rolled:\n{out}");
    assert!(reached_the_box(&r).is_none(), "the box was touched:\n{out}");
    assert!(out.contains("release-gate/dips-nest"), "{out}");
}

#[test]
fn a_missing_status_refuses_the_roll() {
    let r = rig();
    statuses(&r, &[("staking-archive-nest", None)]);
    let (ok, out) = roll(&r, &[]);
    assert!(
        !ok,
        "a release never gated on staking-archive-nest rolled:\n{out}"
    );
    assert!(reached_the_box(&r).is_none(), "the box was touched:\n{out}");
    assert!(out.contains("release-gate/staking-archive-nest"), "{out}");
    assert!(out.contains("no status"), "{out}");
}

#[test]
fn a_pending_status_refuses_as_still_running() {
    let r = rig();
    statuses(&r, &[("gns-nest", Some("pending"))]);
    let (ok, out) = roll(&r, &[]);
    assert!(!ok, "a release still gating on gns-nest rolled:\n{out}");
    assert!(reached_the_box(&r).is_none(), "the box was touched:\n{out}");
    assert!(out.contains("release-gate/gns-nest"), "{out}");
    assert!(out.contains("still running"), "{out}");
}

#[test]
fn only_the_rolled_units_nests_decide() {
    let r = rig();
    statuses(&r, &[("alloc-nest", Some("failure")), ("qos-nest", None)]);
    let (ok, out) = roll(&r, &["graph-gns-nest-next"]);
    assert!(ok, "a red nest the roll does not touch refused it:\n{out}");
    let ssh = reached_the_box(&r).unwrap_or_default();
    assert!(ssh.contains("graph-gns-nest-next"), "{out}");
    assert!(!ssh.contains("graph-allocations-nest-next"), "{ssh}");
}

#[test]
fn an_override_rolls_a_red_release_and_prints_the_reason() {
    let r = rig();
    statuses(&r, &[("alloc-nest", Some("failure"))]);
    let reason = "alloc-nest red against the wrong baseline, #1804";
    let (ok, out) = roll(&r, &["--override", reason]);
    assert!(ok, "the override did not roll:\n{out}");
    assert!(
        reached_the_box(&r).is_some(),
        "the box was never reached:\n{out}"
    );
    assert!(out.contains("OVERRIDE"), "{out}");
    assert!(
        out.contains(reason),
        "the reason is not in the output:\n{out}"
    );
    assert!(out.contains("release-gate/alloc-nest"), "{out}");
}

#[test]
fn an_override_needs_a_reason() {
    let r = rig();
    statuses(&r, &[("alloc-nest", Some("failure"))]);
    let (ok, out) = roll(&r, &["--override", ""]);
    assert!(!ok, "an empty override rolled:\n{out}");
    assert!(reached_the_box(&r).is_none(), "the box was touched:\n{out}");
}
