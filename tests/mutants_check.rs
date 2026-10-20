//! #841 - the mutation gate's verdict step must fail when it has no verdict to give.
//!
//! `scripts/mutants-check` used to read an absent `mutants.out/missed.txt` as an empty survivor
//! list and print "No new survivors" on its way to exit 0. Every nightly mutation run between
//! 2026-08-23 and 2026-08-25 was cancelled at the job timeout and left exactly that state behind, so
//! the step that exists to report survivors reported success three times having read nothing.
//!
//! These tests are the control the script did not have: each one puts the checker in a state where
//! it cannot answer, and asserts it says so. The happy path is here too, because a check that only
//! ever fails is no better than one that only ever passes.

use std::path::Path;
use std::process::Command;

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// Run the checker inside `dir`, returning (exit-ok, combined output).
fn check(dir: &Path, file: Option<&str>) -> (bool, String) {
    let mut c = Command::new("bash");
    c.arg(root().join("scripts/mutants-check.sh"))
        .current_dir(dir);
    if let Some(f) = file {
        c.arg("--file").arg(f);
    }
    let out = c
        .output()
        .expect("bash and jq must be on PATH to run the mutation checker");
    let mut s = String::from_utf8_lossy(&out.stdout).into_owned();
    s.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), s)
}

/// As [`workspace`], plus a `timeout.txt`. cargo-mutants writes timed-out mutants there rather than
/// to `missed.txt`, which is how three of them escaped the gate entirely (#853).
fn workspace_with_timeouts(
    outcomes: Option<&str>,
    missed: Option<&str>,
    timeouts: &str,
) -> tempfile::TempDir {
    let dir = workspace(outcomes, missed);
    std::fs::write(dir.path().join("mutants.out/timeout.txt"), timeouts).unwrap();
    dir
}

/// A workspace with a baseline naming one known survivor, and whatever `mutants.out` the test wants.
fn workspace(outcomes: Option<&str>, missed: Option<&str>) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(".github")).unwrap();
    std::fs::write(
        dir.path().join(".github/mutants-baseline.toml"),
        "[[survivor]]\nfile = \"src/chunker.rs\"\nmutation = \"replace < with <= in AdaptiveWindow::served_by_splitting\"\nreason = \"\"\"\nknown\n\"\"\"\n",
    )
    .unwrap();
    if outcomes.is_some() || missed.is_some() {
        std::fs::create_dir_all(dir.path().join("mutants.out")).unwrap();
    }
    if let Some(o) = outcomes {
        std::fs::write(dir.path().join("mutants.out/outcomes.json"), o).unwrap();
    }
    if let Some(m) = missed {
        std::fs::write(dir.path().join("mutants.out/missed.txt"), m).unwrap();
    }
    dir
}

const COMPLETE: &str =
    r#"{"total_mutants": 39, "outcomes": [], "end_time": "2026-08-24T08:00:00Z"}"#;
const TRUNCATED: &str = r#"{"total_mutants": 39, "outcomes": [], "end_time": null}"#;
/// A completed run that enumerated the baseline's chunker mutant and did not report it surviving.
const TESTED_CHUNKER: &str = r#"{"total_mutants": 1, "end_time": "2026-08-24T08:00:00Z", "outcomes": [{"scenario": {"Mutant": {"name": "src/chunker.rs:134:14: replace < with <= in AdaptiveWindow::served_by_splitting"}}, "summary": "CaughtMutant"}]}"#;
const EMPTY_SWEEP: &str =
    r#"{"total_mutants": 0, "outcomes": [], "end_time": "2026-08-24T08:00:00Z"}"#;

#[test]
fn an_absent_run_is_a_failure_not_a_clean_bill() {
    let w = workspace(None, None);
    let (ok, out) = check(w.path(), None);
    assert!(!ok, "a missing mutants.out must fail, got success:\n{out}");
    assert!(out.contains("FAIL"), "{out}");
    assert!(
        !out.contains("No new survivors"),
        "must not claim a clean result it did not measure:\n{out}"
    );
}

#[test]
fn a_run_killed_before_it_finished_is_a_failure() {
    // The literal state of the 2026-08-23, -24 and -25 nightly runs: a full set of outcomes and no
    // end_time, because the job was cancelled at `timeout-minutes`.
    let w = workspace(Some(TRUNCATED), None);
    let (ok, out) = check(w.path(), None);
    assert!(!ok, "a truncated run must fail, got success:\n{out}");
    assert!(
        out.contains("end_time"),
        "must name why it cannot answer:\n{out}"
    );
}

#[test]
fn a_sweep_that_enumerated_no_mutants_is_a_failure() {
    // A renamed or moved source path makes `--file` match nothing. The sweep then "passes" having
    // mutated zero lines, which is the same false green in a different disguise.
    let w = workspace(Some(EMPTY_SWEEP), Some(""));
    let (ok, out) = check(w.path(), None);
    assert!(!ok, "a zero-mutant sweep must fail, got success:\n{out}");
    assert!(out.contains("total_mutants=0"), "{out}");
}

#[test]
fn a_completed_run_with_only_baselined_survivors_passes() {
    let w = workspace(
        Some(COMPLETE),
        Some("src/chunker.rs:134:14: replace < with <= in AdaptiveWindow::served_by_splitting\n"),
    );
    let (ok, out) = check(w.path(), None);
    assert!(ok, "a clean completed run must pass:\n{out}");
    assert!(out.contains("No new survivors"), "{out}");
}

#[test]
fn a_survivor_outside_the_baseline_fails_and_is_named() {
    let w = workspace(
        Some(COMPLETE),
        Some("src/seal.rs:171:20: delete ! in seal_range_with_snapshot\n"),
    );
    let (ok, out) = check(w.path(), None);
    assert!(!ok, "an unbaselined survivor must fail:\n{out}");
    assert!(
        out.contains("delete ! in seal_range_with_snapshot"),
        "must name the mutation, not just the count:\n{out}"
    );
}

#[test]
fn the_file_scope_keeps_a_matrix_job_from_judging_another_files_baseline() {
    // The per-file matrix (#841) means the seal.rs job never sees chunker.rs survivors. Without
    // `--file` it would report every chunker baseline entry as newly stale on every run.
    let w = workspace(Some(TESTED_CHUNKER), Some("src/seal.rs:1:1: something\n"));
    let (_, unscoped) = check(w.path(), None);
    assert!(
        unscoped.contains("no longer survives"),
        "unscoped, the chunker baseline entry looks stale:\n{unscoped}"
    );
    let (_, scoped) = check(w.path(), Some("src/seal.rs"));
    assert!(
        !scoped.contains("no longer survives"),
        "scoped to seal.rs, the chunker baseline entry must not be judged:\n{scoped}"
    );
}

#[test]
fn a_timed_out_mutant_is_reported_rather_than_silently_neither() {
    // The 2026-08-24 run's three real timeouts. They appeared in neither the survivor list nor the
    // baseline, so 3 of 39 scoped mutants - about 8% - were in a state the gate said nothing about.
    let w = workspace_with_timeouts(
        Some(COMPLETE),
        Some(""),
        "src/chunker.rs:84:9: replace AdaptiveWindow::window -> u64 with 0\n\
         src/chunker.rs:107:53: replace == with != in AdaptiveWindow::observed\n",
    );
    let (ok, out) = check(w.path(), None);
    assert!(!ok, "an unbaselined timeout must fail:\n{out}");
    assert!(out.contains("TIMED OUT"), "{out}");
    assert!(
        out.contains("replace AdaptiveWindow::window -> u64 with 0"),
        "it must name them, not just count them:\n{out}"
    );
    assert!(
        !out.contains("No new survivors"),
        "a timeout must not be reported as a clean sweep:\n{out}"
    );
}

#[test]
fn a_timeout_is_scoped_by_file_like_a_survivor() {
    // A per-file matrix job must not fail on another file's timeout.
    let w = workspace_with_timeouts(
        Some(COMPLETE),
        Some(""),
        "src/chunker.rs:84:9: replace AdaptiveWindow::window -> u64 with 0\n",
    );
    let (ok, out) = check(w.path(), Some("src/seal.rs"));
    assert!(
        ok,
        "seal.rs is not answerable for a chunker.rs timeout:\n{out}"
    );
    let (ok, _) = check(w.path(), Some("src/chunker.rs"));
    assert!(!ok, "chunker.rs is");
}

#[test]
fn a_baselined_timeout_is_accepted_like_a_baselined_survivor() {
    // Same rule as a survivor: recorded with a reason is fine, silence is not.
    let w = workspace_with_timeouts(
        Some(COMPLETE),
        Some(""),
        "src/chunker.rs:134:14: replace < with <= in AdaptiveWindow::served_by_splitting\n",
    );
    let (ok, out) = check(w.path(), None);
    assert!(ok, "a timeout already in the baseline is not new:\n{out}");
}

#[test]
fn a_shard_judges_only_the_baseline_entries_it_ran() {
    // #1971: a seal.rs shard runs an eighth of the file, and the note told the reader to remove four
    // entries that still survived in the shards that ran them.
    let w = workspace(Some(COMPLETE), Some(""));
    let (ok, out) = check(w.path(), None);
    assert!(ok, "{out}");
    assert!(
        !out.contains("no longer survives"),
        "an entry this run did not mutate was judged stale:\n{out}"
    );
    let w = workspace(Some(TESTED_CHUNKER), Some(""));
    let (_, out) = check(w.path(), None);
    assert!(
        out.contains("no longer survives"),
        "an entry this run mutated and caught is stale:\n{out}"
    );
}
