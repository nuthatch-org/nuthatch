//! `.config/nextest.toml` makes `target/parity-bin` a required extra path of every nextest archive,
//! so a workflow that archives without building it first fails at the archive step. The nightly
//! network replays did exactly that for a day after 4d16a9db (#2635); this holds every workflow to it.

use std::path::Path;

#[test]
fn every_workflow_that_archives_tests_builds_the_parity_comparison_first() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let nextest = std::fs::read_to_string(root.join(".config/nextest.toml")).unwrap();
    if !nextest.contains("parity-bin") {
        return;
    }
    let mut checked = 0;
    for entry in std::fs::read_dir(root.join(".github/workflows")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("yml") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let mut built = false;
        for line in text.lines() {
            let code = line.split('#').next().unwrap_or("");
            if code.contains("parity-bin") && code.contains("cp ") {
                built = true;
            }
            if code.contains("cargo nextest archive") {
                checked += 1;
                assert!(
                    built,
                    "{} runs `cargo nextest archive` without first building target/parity-bin, \
                     which .config/nextest.toml requires: the archive step will fail",
                    path.display()
                );
                built = false;
            }
        }
    }
    assert!(
        checked >= 2,
        "expected the ci.yml and network-contract.yml archives, saw {checked}"
    );
}
