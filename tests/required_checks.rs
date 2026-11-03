//! #715 - the required-check set must exist in the repo, or it drifts in GitHub settings alone.

fn names() -> Vec<String> {
    let raw = include_str!("../.github/required-checks.txt");
    raw.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_string)
        .collect()
}

#[test]
fn required_checks_file_names_the_ten_including_jules() {
    let n = names();
    assert!(n.iter().any(|s| s == "fmt · clippy · test"), "{n:?}");
    assert!(
        n.iter().any(|s| s == "Jules approval"),
        "the external review gate is not optional, and since the human `reviewed-by signature` gate \
         was retired it is the only thing standing between a red review and a merge: {n:?}"
    );
    assert!(
        !n.iter().any(|s| s == "reviewed-by signature"),
        "the reviewed-by signature gate was retired: a PR is admitted by CI and Jules, not by a \
         line of text a party could type about themselves. Re-adding the context here without \
         re-adding the workflow leaves every PR blocked on a check that can never report: {n:?}"
    );
    assert_eq!(
        n.len(),
        10,
        "ten contexts on main after the signature gate was retired: {n:?}"
    );
}

#[test]
fn live_endpoints_keys_on_json_fields_not_prose() {
    let script = include_str!("../scripts/probe-shipped-defaults.sh");
    assert!(
        !script.contains("retry \"getLogs window"),
        "the probe script must not grep doctor's prose (#716)"
    );
    assert!(
        script.contains("doctor --json"),
        "doctor --json is the contract"
    );
    assert!(script.contains("max_window"), "retry keys on max_window");
}
