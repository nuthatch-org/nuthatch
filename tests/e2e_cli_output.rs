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

/// A command that logs before it fails, with stdout piped, leaves no colour codes in the log. The
/// helper's unit test cannot see whether the subscriber is wired to it.
#[cfg(feature = "watch")]
#[test]
fn piped_text_logs_carry_no_ansi_escapes() {
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["watch", "--url", "http://127.0.0.1:1", "--exit-after", "1"])
        .env("RUST_LOG", "debug")
        .env_remove("NO_COLOR")
        .stdin(Stdio::null())
        .output()
        .expect("run nuthatch");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("DEBUG"),
        "the run must have logged, or this proves nothing:\n{stdout}"
    );
    assert!(!stdout.contains('\u{1b}'), "colour codes in:\n{stdout:?}");
}

/// A failed `nuthatch sql` leads with its hint, in both backends. The unit tests cover the
/// formatting; this covers that each call site uses it.
#[test]
fn a_failed_sql_query_prints_the_hint_first_on_both_backends() {
    let nest = tempfile::tempdir().unwrap();
    drop(nuthatch::store::Store::open(&nest.path().join("nuthatch.redb")).unwrap());
    let (code, _, err) = run(&[
        "sql",
        "SELECT * FROM transfer",
        "--dir",
        nest.path().to_str().unwrap(),
    ]);
    assert_ne!(code, 0, "{err}");
    let body = err.trim_start_matches("Error: ");
    assert!(body.starts_with("no table `transfer`"), "{err}");
    assert!(
        err.contains("`.tables`")
            && !err.contains("`schema` tool")
            && !err.contains("information_schema"),
        "{err}"
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let reply = serde_json::json!({
        "error": "no segments: Catalog Error: Table with name transfer does not exist!\n\nhint: no table `transfer`. `SELECT table_name FROM information_schema.tables` lists the tables."
    })
    .to_string();
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        while let Ok((mut s, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf);
            let _ = write!(
                s,
                "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                reply.len()
            );
        }
    });
    let empty = tempfile::tempdir().unwrap();
    let (code, _, err) = run(&[
        "sql",
        "SELECT * FROM transfer",
        "--dir",
        empty.path().to_str().unwrap(),
        "--url",
        &url,
    ]);
    assert_ne!(code, 0, "{err}");
    assert!(
        err.trim_start_matches("Error: ")
            .starts_with("no table `transfer`"),
        "{err}"
    );
    assert!(
        err.contains("`.tables`")
            && !err.contains("`schema` tool")
            && !err.contains("information_schema"),
        "{err}"
    );
}

/// `check` prints a broken view's hint to a terminal, so it points at the REPL as `sql` does.
#[test]
fn check_points_a_broken_views_hint_at_the_repl() {
    let nest = tempfile::tempdir().unwrap();
    std::fs::write(
        nest.path().join("nuthatch.toml"),
        "[nest]\nname = \"t\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = []\n",
    )
    .unwrap();
    std::fs::create_dir_all(nest.path().join("views")).unwrap();
    std::fs::write(
        nest.path().join("views/10-broken.sql"),
        "CREATE VIEW broken AS SELECT * FROM nonexistent_table;",
    )
    .unwrap();
    let (code, out, err) = run(&["check", "--dir", nest.path().to_str().unwrap()]);
    assert_ne!(code, 0, "{out}{err}");
    assert!(
        out.contains("hint: no table `nonexistent_table`") && out.contains("`.tables`"),
        "{out}"
    );
    assert!(
        !out.contains("information_schema") && !out.contains("`schema` tool"),
        "{out}"
    );
}
