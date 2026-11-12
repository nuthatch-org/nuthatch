//! What the default binary prints to a person or a script when a command cannot do its job: the
//! words of the refusal, the help text, and the exit status. Run against the real binary, since
//! each of these was wrong in `main` or in clap's wiring and not in a function a unit test reaches.

use std::process::{Command, Stdio};

fn run(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(args)
        .stdin(Stdio::null())
        .output()
        .expect("run nuthatch");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[cfg(not(feature = "postgres-store"))]
#[test]
fn worker_and_control_name_the_scaled_build_without_asking_for_their_flags() {
    for sub in ["worker", "control"] {
        let (code, _, err) = run(&[sub]);
        assert_ne!(code, 0, "{sub}");
        assert!(err.contains("nuthatch-scaled-"), "{sub}: {err}");
        assert!(err.contains("-scaled"), "{sub}: {err}");
        assert!(!err.contains("CLAUDE.md"), "{sub}: {err}");
        assert!(!err.contains("required arguments"), "{sub}: {err}");
    }
}

#[test]
fn help_does_not_expose_clap_wiring_or_a_deleted_feature_name() {
    let (_, nest, _) = run(&["nest", "--help"]);
    assert!(!nest.contains("needs a build with"), "{nest}");
    let (_, dev, _) = run(&["dev", "--help"]);
    assert!(dev.contains("--log-format"), "{dev}");
    assert!(!dev.contains("global = true"), "{dev}");
}

#[cfg(feature = "watch")]
#[test]
fn watch_says_nothing_answered_and_fails_when_the_url_is_dead() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let (code, _, err) = run(&[
        "watch",
        "--url",
        &url,
        "--interval",
        "1",
        "--exit-after",
        "1",
    ]);
    assert_ne!(code, 0, "{err}");
    assert!(err.contains("cannot reach"), "{err}");
    assert!(err.contains("nuthatch dev"), "{err}");
}
