//! `.config/nextest.toml` makes `target/parity-bin` a required extra path of every nextest archive,
//! so a workflow that archives without building it first fails at the archive step. The nightly
//! network replays did exactly that for a day after 4d16a9db (#2635); this holds every workflow, and
//! every yatr task ci.yml archives through, to it.

use std::path::Path;

/// The yatr tasks that run `cargo nextest archive`, and those of them that do not depend on
/// `parity-bin`.
fn unbuilt_task_archives(config: &str) -> (usize, Vec<String>) {
    let config: toml::Table = config.parse().expect("yatr.toml parses");
    let tasks = config
        .get("tasks")
        .and_then(|t| t.as_table())
        .expect("yatr.toml has [tasks]");
    let strings = |task: &toml::Value, key: &str| -> Vec<String> {
        task.get(key)
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let (mut archives, mut unbuilt) = (0, Vec::new());
    for (name, task) in tasks {
        if !strings(task, "run")
            .iter()
            .any(|c| c.contains("cargo nextest archive"))
        {
            continue;
        }
        archives += 1;
        if !strings(task, "depends").iter().any(|d| d == "parity-bin") {
            unbuilt.push(name.clone());
        }
    }
    (archives, unbuilt)
}

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
    let yatr = std::fs::read_to_string(root.join("yatr.toml")).unwrap();
    let parity_bin = yatr.split("[tasks.parity-bin]").nth(1).unwrap_or("");
    assert!(
        parity_bin.contains("parity-bin/") && parity_bin.contains("cp "),
        "yatr.toml's parity-bin task no longer copies the comparison into target/parity-bin"
    );
    let (archives, unbuilt) = unbuilt_task_archives(&yatr);
    checked += archives;
    assert!(
        unbuilt.is_empty(),
        "yatr.toml tasks {unbuilt:?} run `cargo nextest archive` without depending on parity-bin, \
         which .config/nextest.toml requires: the archive step will fail"
    );
    assert!(
        checked >= 2,
        "expected the ci.yml (through yatr.toml) and network-contract.yml archives, saw {checked}"
    );
}

#[test]
fn an_archive_task_must_depend_on_the_parity_build() {
    let config = r#"
[tasks.parity-bin]
run = ["cp x target/parity-bin/"]
[tasks.good]
depends = ["parity-bin"]
run = ["cargo nextest archive --archive-file a.tar.zst"]
[tasks.bad]
run = ["cargo nextest archive --archive-file b.tar.zst"]
"#;
    assert_eq!(unbuilt_task_archives(config), (2, vec!["bad".to_string()]));
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
