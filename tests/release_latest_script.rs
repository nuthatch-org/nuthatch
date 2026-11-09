//! #2736: `scripts/release-latest.sh`, which `release.yml` asks whether a tag may move the image's
//! `:latest`. Only the newest stable release may: a prerelease or a patch on an older line must not.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn moves_latest(tag: &str, tags: &[&str]) -> String {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/release-latest.sh");
    let mut child = Command::new("bash")
        .arg(script)
        .arg(tag)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("bash must be on PATH");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(tags.join("\n").as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "release-latest.sh {tag} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

const SHIPPED: [&str; 5] = ["v3.0.0-alpha.1", "v5.9.0", "v5.16.2", "v5.17.0", "v5.17.2"];

#[test]
fn the_newest_stable_tag_moves_latest() {
    assert_eq!(moves_latest("v5.18.0", &SHIPPED), "true");
    assert_eq!(moves_latest("v5.17.3", &SHIPPED), "true");
    // The pushed tag is itself in `git ls-remote`'s answer.
    assert_eq!(moves_latest("v5.17.2", &SHIPPED), "true");
    assert_eq!(moves_latest("v1.0.0", &[]), "true");
}

#[test]
fn versions_compare_as_numbers_not_text() {
    assert_eq!(moves_latest("v5.10.0", &["v5.9.9", "v5.10.0"]), "true");
    assert_eq!(moves_latest("v5.9.9", &["v5.9.9", "v5.10.0"]), "false");
    assert_eq!(moves_latest("v10.0.0", &["v9.99.99"]), "true");
}

#[test]
fn a_prerelease_never_moves_latest() {
    assert_eq!(moves_latest("v9.9.9-rc.1", &SHIPPED), "false");
    assert_eq!(
        moves_latest("v6.0.0-alpha.1", &["v5.17.2", "v6.0.0-alpha.1"]),
        "false"
    );
    // Nor does one already out stop the next stable release from moving it.
    assert_eq!(moves_latest("v5.18.0", &["v6.0.0-rc.1", "v5.17.2"]), "true");
}

#[test]
fn a_patch_on_an_older_line_does_not_move_latest() {
    assert_eq!(moves_latest("v5.16.3", &SHIPPED), "false");
    assert_eq!(moves_latest("v4.15.9", &SHIPPED), "false");
}

#[test]
fn a_tag_that_is_not_a_plain_version_does_not_move_latest() {
    for tag in ["v5.18", "5.18.0", "v5.18.0+build", "latest", ""] {
        assert_eq!(moves_latest(tag, &SHIPPED), "false", "{tag:?}");
    }
}

/// The workflow must ask the script, and its push step may name `latest` only on that answer.
#[test]
fn release_yml_pushes_latest_only_on_the_scripts_answer() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join(".github/workflows/release.yml");
    let s = std::fs::read_to_string(p).unwrap();
    let code: Vec<&str> = s
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect();
    assert!(
        code.iter()
            .any(|l| l.contains("scripts/release-latest.sh \"$GITHUB_REF_NAME\"")),
        "release.yml no longer asks scripts/release-latest.sh whether to move :latest (#2736)"
    );
    let push: Vec<&&str> = code
        .iter()
        .skip_while(|l| l.trim() != "- name: push the tested images")
        .take_while({
            let mut first = true;
            move |l| std::mem::take(&mut first) || !l.starts_with("      - ")
        })
        .collect();
    assert!(
        push.len() > 3,
        "release.yml has no `push the tested images` step any more"
    );
    let latest: Vec<&&str> = push
        .into_iter()
        .filter(|l| {
            l.split(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_'))
                .any(|w| w == "latest")
        })
        .collect();
    assert!(
        !latest.is_empty(),
        "release.yml never pushes :latest, so this gate would pass vacuously"
    );
    assert!(
        latest.iter().all(|l| l.contains("MOVE_LATEST")),
        "these lines name `latest` outside the script's answer, so :latest can move for any tag \
         (#2736):\n{latest:#?}"
    );
}
