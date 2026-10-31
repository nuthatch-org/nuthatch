//! RFC-0077 S4's gate, the node's half: a copy of `tests/fixtures/tab-parity/s4-nest` seeded from the
//! published directory `s4-mirror` with `nuthatch seed`, served at the seed's head and followed to
//! the recording's tip, answers `embed/mirror-answers.mjs`'s requests equal to `s4-mirror-seed.json`
//! and `s4-mirror-tip.json`. The embedded engine is held to the same two files in CI's `wasm-plain`
//! leg, in Node and in Chrome. The requests and their comparison are the gate's JavaScript, so this
//! runs `embed/run-mirror-node.mjs` under Node rather than restating them.
//!
//! `NUTHATCH_PIN_TAB_PARITY=1` writes the files from this run instead of comparing.

use std::path::Path;
use std::process::Command;

use nuthatch_fixture_rpc::recorded::Recorded;
use serde_json::Value;

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tab-parity");
const RUNNER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/embed/run-mirror-node.mjs");

fn node_report(out: &Path, follow: Option<(&str, u64)>) -> Value {
    let mut cmd = Command::new("node");
    cmd.arg(RUNNER)
        .arg(env!("CARGO_BIN_EXE_nuthatch"))
        .arg(format!("{DIR}/s4-nest"))
        .arg(format!("{DIR}/s4-mirror"))
        .arg("--out")
        .arg(out);
    if let Some((rpc, tip)) = follow {
        cmd.args(["--rpc", rpc, "--tip", &tip.to_string()]);
    }
    let run = cmd
        .output()
        .expect("node runs RFC-0077 S4's gate script; is Node installed?");
    assert!(
        run.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    serde_json::from_str(&std::fs::read_to_string(out).unwrap()).unwrap()
}

fn check(got: Value, pin: &str) {
    let pinned = format!("{DIR}/{pin}");
    if std::env::var_os("NUTHATCH_PIN_TAB_PARITY").is_some() {
        std::fs::write(&pinned, serde_json::to_string_pretty(&got).unwrap() + "\n").unwrap();
    }
    let want: Value = serde_json::from_str(&std::fs::read_to_string(&pinned).unwrap()).unwrap();
    assert_eq!(want["head"], got["head"], "the head the node answered at");
    let (want, got) = (&want["answers"], &got["answers"]);
    for (target, answer) in want.as_object().unwrap() {
        assert_eq!(&got[target], answer, "{target}");
    }
    assert_eq!(want, got);
}

#[test]
fn a_node_seeded_from_the_mirror_answers_the_pinned_requests() {
    let tmp = tempfile::tempdir().unwrap();
    check(
        node_report(&tmp.path().join("seed.json"), None),
        "s4-mirror-seed.json",
    );

    let fixture = Recorded::from_path(Path::new(&format!("{DIR}/chain.json.gz"))).unwrap();
    let tip = fixture.tips()[0];
    let rpc = nuthatch_fixture_rpc::spawn(fixture, "127.0.0.1:0").expect("fixture RPC");
    check(
        node_report(&tmp.path().join("tip.json"), Some((&rpc.url(), tip))),
        "s4-mirror-tip.json",
    );
}
