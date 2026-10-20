//! #845 - the drift check must refuse to report success when it has compared nothing.
//!
//! `scripts/check-required-contexts.sh` is the only thing that compares `.github/required-checks.txt`
//! against the protection GitHub actually enforces. Before this it was invoked by no workflow, no
//! test and no documented command - and would have exited 0 without a token anyway, so wiring it up
//! carelessly would have produced a permanently green step that read nothing.
//!
//! The script derives its repo root from its own location, so each test builds a throwaway root with
//! its own `scripts/` and `.github/` and runs the real script inside it. No network: every case here
//! is one the script must decide before it would reach the API.

use std::path::{Path, PathBuf};
use std::process::Command;

fn manifest() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// A throwaway repo root holding the real script and the given required-checks file.
fn root_with(contexts: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("scripts")).unwrap();
    std::fs::create_dir_all(dir.path().join(".github")).unwrap();
    std::fs::copy(
        manifest().join("scripts/check-required-contexts.sh"),
        dir.path().join("scripts/check-required-contexts.sh"),
    )
    .unwrap();
    std::fs::write(dir.path().join(".github/required-checks.txt"), contexts).unwrap();
    dir
}

fn run(root: &Path, args: &[&str]) -> (i32, String) {
    let out = Command::new("bash")
        .arg(root.join("scripts/check-required-contexts.sh"))
        .args(args)
        // Strip both, so a developer's ambient token cannot make these tests reach the network.
        .env_remove("GH_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .output()
        .expect("bash must be on PATH");
    let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
    s.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.code().unwrap_or(-1), s)
}

const REQUIRED: &str = "# comment\nfmt · clippy · test\nbuild release\nJules approval\n";

#[test]
fn no_token_is_a_failure_not_a_pass() {
    let root = root_with(REQUIRED);
    let (code, out) = run(root.path(), &[]);
    assert_eq!(code, 1, "a tokenless run must fail:\n{out}");
    assert!(out.contains("FAIL"), "{out}");
    assert!(
        !out.contains("match live protection"),
        "must not claim a comparison it did not make:\n{out}"
    );
}

#[test]
fn the_failure_names_a_recovery_path_that_exists_in_the_failing_state() {
    // A required-check failure message that says "see the docs" is unreachable advice at the moment
    // it is needed. This one has to name a credential that can actually do the job, because the
    // default GITHUB_TOKEN cannot read branch protection and that is the non-obvious half.
    //
    // **It asserted `administration: read` until #909**, which is the very non-key that stopped this
    // workflow validating - so the test enforced the one recovery path that does not exist, which is
    // the failure it was written to prevent. The reachable path is the secret.
    let root = root_with(REQUIRED);
    let (_, out) = run(root.path(), &[]);
    assert!(out.contains("PROTECTION_READ_TOKEN"), "{out}");
    assert!(
        out.contains("administration"),
        "and it must still say why the obvious-looking permissions key is not the answer:\n{out}"
    );
    assert!(out.contains("--offline"), "{out}");
    assert!(out.contains("gh auth token"), "{out}");
}

#[test]
fn offline_passes_but_says_it_compared_nothing() {
    let root = root_with(REQUIRED);
    let (code, out) = run(root.path(), &["--offline"]);
    assert_eq!(code, 0, "--offline is a deliberate, allowed mode:\n{out}");
    assert!(
        out.contains("NOT compared") && out.contains("not a drift check"),
        "an offline pass must not read like a drift check that passed:\n{out}"
    );
}

#[test]
fn a_file_missing_the_jules_context_fails_before_any_network() {
    let root = root_with("fmt · clippy · test\nbuild release\n");
    let (code, out) = run(root.path(), &["--offline"]);
    assert_eq!(code, 1, "the external review gate is not optional:\n{out}");
    assert!(out.contains("Jules approval"), "{out}");
}

/// The retired gate, asserted in the negative. Re-adding the context without re-adding the workflow
/// blocks every PR on a check that can never report, which is the failure mode that took a week to
/// spot the last time a required context named a job nothing ran.
#[test]
fn a_file_naming_the_retired_signature_context_fails_before_any_network() {
    let root =
        root_with("fmt · clippy · test\nbuild release\nreviewed-by signature\nJules approval\n");
    let (code, out) = run(root.path(), &["--offline"]);
    assert_eq!(code, 1, "the retired context must be refused:\n{out}");
    assert!(out.contains("retired"), "{out}");
}

#[test]
fn an_unknown_flag_is_refused_rather_than_ignored() {
    // A typo'd flag silently ignored would run the network path when the caller asked for offline,
    // or vice versa. Both are worse than a usage error.
    let root = root_with(REQUIRED);
    let (code, out) = run(root.path(), &["--ofline"]);
    assert_eq!(code, 2, "{out}");
    assert!(out.contains("usage"), "{out}");
}

/// #2280 - `grep -q` stops reading at its match, so a context it found reached the caller as
/// missing whenever the writer feeding it took SIGPIPE under `pipefail`. On a short list that is a
/// race; a list past the pipe buffer, with the match sorted first, makes it certain.
#[test]
fn a_context_found_early_in_a_long_list_is_not_reported_missing() {
    let mut list = String::from("Jules approval\n");
    for i in 0..6000 {
        list.push_str(&format!("context number {i}\n"));
    }
    let root = root_with(&list);
    let (code, out) = run(root.path(), &["--offline"]);
    assert_eq!(code, 0, "{out}");
    assert!(!out.contains("is missing"), "{out}");
}

/// The committed script and the committed list are the ones CI runs, so assert against them rather
/// than only against fixtures - a fixture-only suite cannot see the real file drifting.
#[test]
fn the_committed_list_names_jules_and_not_the_retired_signature_context() {
    let listed: Vec<String> =
        std::fs::read_to_string(manifest().join(".github/required-checks.txt"))
            .expect("required-checks.txt")
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(str::to_string)
            .collect();
    assert!(
        !listed.iter().any(|l| l == "reviewed-by signature"),
        "the signature gate was retired and its workflow deleted; a required context with no job \
         behind it blocks every PR forever: {listed:?}"
    );
    assert!(listed.iter().any(|l| l == "Jules approval"), "{listed:?}");
    let _: PathBuf = manifest().to_path_buf();
}

/// #1119 - `main` reported `required-checks.txt is missing 'Jules approval'` against a file that
/// plainly contains it. Every context in that file holds `·` (U+00B7), and `grep`/`sort` change
/// behaviour on non-ASCII input with the locale: GNU grep can decline to match a line it considers an
/// encoding error, and `sort` refuses an illegal byte sequence outright. A runner whose locale
/// differs from a developer's therefore reads a healthy file as a broken one.
///
/// Not reproduced on macOS - every locale tried there passes - so this pins the behaviour rather than
/// claiming a cure. What the test can do is stop the pin being removed by someone who does not know
/// why it is there.
#[test]
fn the_script_pins_the_locale_so_a_runner_cannot_read_a_healthy_file_as_broken() {
    let src = std::fs::read_to_string(manifest().join("scripts/check-required-contexts.sh"))
        .expect("read the script");
    // **Match a line of code, not a substring.** Commenting the pin out leaves the text in place, so
    // a `contains` check passes against a script that no longer pins anything - which is exactly what
    // it did when this test was first written and mutation-checked.
    let pinned = src
        .lines()
        .map(str::trim)
        .any(|l| l == "export LC_ALL=C" || l == "export LC_ALL=C.UTF-8");
    assert!(
        pinned,
        "the context list is full of `·`, and grep/sort are locale-sensitive on it. Without a pin \
         the same file reads differently on different runners, which is the #1119 failure:\n{src}"
    );
}

/// An unreadable or empty file must say so, not report the file's *contents* as wrong. The message
/// that sent #1119 to the wrong place blamed a missing context when nothing had been read at all.
#[test]
fn reading_nothing_is_reported_as_reading_nothing() {
    // Empty.
    let root = root_with("");
    let (code, out) = run(root.path(), &["--offline"]);
    assert_ne!(code, 0, "an empty list must not pass: {out}");
    assert!(out.contains("read no contexts"), "{out}");
    assert!(
        !out.contains("is missing 'Jules approval'"),
        "an empty file is not a file with a missing context:\n{out}"
    );

    // Only comments - the same, and grep exiting 1 for no output must not be mistaken for an error.
    let root = root_with("# just a comment\n# and another\n");
    let (code, out) = run(root.path(), &["--offline"]);
    assert_ne!(code, 0, "a comments-only list must not pass: {out}");
    assert!(out.contains("read no contexts"), "{out}");

    // Absent entirely.
    let root = root_with(REQUIRED);
    std::fs::remove_file(root.path().join(".github/required-checks.txt")).unwrap();
    let (code, out) = run(root.path(), &["--offline"]);
    assert_ne!(code, 0, "an absent list must not pass: {out}");
    assert!(out.contains("cannot read"), "{out}");

    // Readable but not readable *through*: a read that fails partway leaves a non-empty, incomplete
    // list, and neither the existence test nor the emptiness test can see that. A directory in place
    // of the file is the cheapest way to make the read itself fail. It must report a failed read
    // rather than an empty one - under `pipefail` the filter pipeline reported this as "empty or all
    // comments", because the rightmost stage exiting 1 on no input masked the real error upstream.
    let root = root_with(REQUIRED);
    let f = root.path().join(".github/required-checks.txt");
    std::fs::remove_file(&f).unwrap();
    std::fs::create_dir(&f).unwrap();
    let (code, out) = run(root.path(), &["--offline"]);
    assert_ne!(code, 0, "an unreadable list must not pass: {out}");
    assert!(
        out.contains("failed with status"),
        "a failed read must say so, not be reported as an empty file:\n{out}"
    );
    assert!(
        !out.contains("is missing 'Jules approval'"),
        "and it must never blame the contents:\n{out}"
    );
}

/// The sort-status guard, made reachable.
///
/// A `sort` over a handful of lines does not fail on its own, so the branch that catches a partial
/// or killed sort had no test and a mutation left it green. Putting a stub `sort` earlier on `PATH`
/// exercises it directly: it prints one line and exits 2, which is exactly the shape the guard
/// exists for - **output produced, then failure**, leaving a list that is non-empty and wrong.
#[test]
fn a_sort_that_fails_after_partial_output_is_not_treated_as_a_short_list() {
    let root = root_with(REQUIRED);
    let bin = root.path().join("stub-bin");
    std::fs::create_dir_all(&bin).unwrap();
    let sort = bin.join("sort");
    std::fs::write(&sort, "#!/bin/sh\necho 'build release'\nexit 2\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sort, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new("bash")
        .arg(root.path().join("scripts/check-required-contexts.sh"))
        .arg("--offline")
        .env("PATH", path)
        .env_remove("GH_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .output()
        .expect("bash must be on PATH");
    let code = out.status.code().unwrap_or(-1);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    assert_ne!(code, 0, "a failed sort must not pass: {text}");
    assert!(
        text.contains("sorting the 3 contexts") && text.contains("failed with status 2"),
        "a sort that fails after emitting output must say so, and say how many contexts it was \
         given, rather than let the truncated result reach the drift check:\n{text}"
    );
    assert!(
        !text.contains("is missing 'Jules approval'"),
        "and it must never blame the contents:\n{text}"
    );
}

/// #2287: the live path parses the protection answer with `jq`. A `python3` that refuses to run
/// stands earlier on `PATH`, so a script that still reached for it fails here.
#[test]
fn the_live_comparison_parses_with_jq_not_python() {
    let root = root_with(REQUIRED);
    let bin = root.path().join("stub-bin");
    std::fs::create_dir_all(&bin).unwrap();
    let stubs = [
        (
            "curl",
            "#!/bin/sh\necho '{\"required_status_checks\":{\"contexts\":[\"Jules approval\",\"fmt · clippy · test\",\"build release\"]}}'\n",
        ),
        ("python3", "#!/bin/sh\necho 'python3 called' >&2\nexit 127\n"),
    ];
    for (name, body) in stubs {
        let p = bin.join(name);
        std::fs::write(&p, body).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new("bash")
        .arg(root.path().join("scripts/check-required-contexts.sh"))
        .env("PATH", path)
        .env("GH_TOKEN", "stub")
        .env_remove("GITHUB_TOKEN")
        .output()
        .expect("bash must be on PATH");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.status.code(), Some(0), "{text}");
    assert!(
        text.contains("required checks match live protection on main (3 contexts)"),
        "{text}"
    );
}
