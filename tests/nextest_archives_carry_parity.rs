//! `.config/nextest.toml` makes `target/parity-bin` a required extra path of every nextest archive,
//! so a workflow that archives without building it first fails at the archive step. The nightly
//! network replays did exactly that for a day after 4d16a9db (#2635); this holds every workflow to it.

use std::path::Path;

/// The archives in `workflow` whose own job did not build `target/parity-bin` before them, by line.
fn unbuilt_archives(workflow: &str) -> (usize, Vec<usize>) {
    let (mut archives, mut unbuilt, mut built, mut in_jobs) = (0, Vec::new(), false, false);
    for (i, line) in workflow.lines().enumerate() {
        let code = line.split('#').next().unwrap_or("");
        if code.trim_end() == "jobs:" {
            in_jobs = true;
            continue;
        }
        // A job is a two-space-indented key under `jobs:`; a build in one job does not reach another.
        let job_header = in_jobs
            && code.starts_with("  ")
            && !code.starts_with("   ")
            && code.trim_end().ends_with(':');
        if job_header {
            built = false;
        }
        if code.contains("parity-bin") && code.contains("cp ") {
            built = true;
        }
        if code.contains("cargo nextest archive") {
            archives += 1;
            if !built {
                unbuilt.push(i + 1);
            }
        }
    }
    (archives, unbuilt)
}

#[test]
fn every_workflow_that_archives_tests_builds_the_parity_comparison_first() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let nextest = std::fs::read_to_string(root.join(".config/nextest.toml")).unwrap();
    assert!(
        nextest.contains("parity-bin"),
        ".config/nextest.toml no longer requires target/parity-bin; if that is deliberate, delete this test"
    );
    let mut checked = 0;
    for entry in std::fs::read_dir(root.join(".github/workflows")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("yml") {
            continue;
        }
        let (archives, unbuilt) = unbuilt_archives(&std::fs::read_to_string(&path).unwrap());
        checked += archives;
        assert!(
            unbuilt.is_empty(),
            "{} runs `cargo nextest archive` at line(s) {unbuilt:?} in a job that does not first build \
             target/parity-bin, which .config/nextest.toml requires: the archive step will fail",
            path.display()
        );
    }
    assert!(
        checked >= 2,
        "expected the ci.yml and network-contract.yml archives, saw {checked}"
    );
}

#[test]
fn a_build_in_one_job_does_not_cover_an_archive_in_another() {
    let workflow = "\
jobs:
  build:
    steps:
      - run: mkdir -p t/parity-bin && cp x t/parity-bin/
  archive:
    steps:
      - run: cargo nextest archive --archive-file a.tar.zst
";
    assert_eq!(unbuilt_archives(workflow), (1, vec![7]));
}
