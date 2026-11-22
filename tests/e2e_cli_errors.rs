//! The exit status and the `--json` error object a failed command gives a script (#2987), through the
//! real binary: the code is decided in `main`, from tags set wherever the error starts.

use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::Value;

struct Out {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run_in(dir: &Path, args: &[&str]) -> Out {
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .output()
        .expect("run nuthatch");
    Out {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// The one JSON object on stderr, checked against the exit status it claims.
fn error_object(out: &Out) -> Value {
    let lines: Vec<&str> = out.stderr.lines().collect();
    assert_eq!(lines.len(), 1, "one line of stderr:\n{}", out.stderr);
    let v: Value = serde_json::from_str(lines[0]).expect("stderr is JSON");
    assert_eq!(v["error"]["exit"], out.code, "{v}");
    assert!(
        v["error"]["next"].as_str().is_some_and(|n| !n.is_empty()),
        "{v}"
    );
    assert!(
        v["error"]["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty()),
        "{v}"
    );
    v
}

/// A port nothing listens on.
fn dead_url() -> String {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    format!("http://127.0.0.1:{port}")
}

fn nest_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("nuthatch.toml"),
        "[nest]\nname = \"t\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = [\"http://127.0.0.1:1\"]\n",
    )
    .unwrap();
    dir
}

#[test]
fn sql_with_nothing_running_is_nest_not_running() {
    let dir = nest_dir();
    let url = dead_url();
    let out = run_in(dir.path(), &["sql", "SELECT 1", "--json", "--url", &url]);
    assert_eq!(out.code, 4, "{}", out.stderr);
    assert!(out.stdout.is_empty(), "{}", out.stdout);
    assert_eq!(error_object(&out)["error"]["code"], "nest_not_running");
}

#[test]
fn sql_in_a_directory_with_no_nest_is_no_nest() {
    let dir = tempfile::tempdir().unwrap();
    let url = dead_url();
    let out = run_in(dir.path(), &["sql", "SELECT 1", "--json", "--url", &url]);
    assert_eq!(out.code, 3, "{}", out.stderr);
    assert_eq!(error_object(&out)["error"]["code"], "no_nest");
}

/// Without `--json` the words are what they were; only the status is more specific.
#[test]
fn without_json_the_error_text_is_unchanged() {
    let dir = nest_dir();
    let url = dead_url();
    let out = run_in(dir.path(), &["sql", "SELECT 1", "--url", &url]);
    assert_eq!(out.code, 4, "{}", out.stderr);
    assert!(
        out.stderr
            .starts_with(&format!("Error: querying {url} - no store at ")),
        "{}",
        out.stderr
    );
    assert!(out.stderr.contains("\n\nCaused by:\n"), "{}", out.stderr);
    assert!(!out.stderr.contains("\"error\""), "{}", out.stderr);
}

#[test]
fn a_refused_command_line_with_json_is_a_usage_object() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_in(dir.path(), &["sql", "--json", "--no-such-flag"]);
    assert_eq!(out.code, 2);
    let v = error_object(&out);
    assert_eq!(v["error"]["code"], "usage");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--no-such-flag"),
        "{v}"
    );
    let plain = run_in(dir.path(), &["sql", "--no-such-flag"]);
    assert_eq!(plain.code, 2);
    assert!(plain.stderr.starts_with("error: "), "{}", plain.stderr);
}

#[test]
fn schema_without_a_nest_exits_no_nest() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_in(dir.path(), &["schema"]);
    assert_eq!(out.code, 3, "{}", out.stderr);
}

#[test]
fn init_on_an_unknown_chain_is_chain_not_detected() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_in(
        dir.path(),
        &[
            "init",
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "--chain",
            "no-such-chain",
        ],
    );
    assert_eq!(out.code, 7, "{}", out.stderr);
}

#[test]
fn init_through_an_rpc_that_does_not_answer_is_rpc_refused() {
    let dir = tempfile::tempdir().unwrap();
    let url = dead_url();
    let out = run_in(
        dir.path(),
        &[
            "init",
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "--rpc",
            &url,
        ],
    );
    assert_eq!(out.code, 8, "{}", out.stderr);
}

/// A 400 from a running nest is the query's fault, and its hint is the next step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_the_nest_refuses_is_sql_invalid_with_its_hint_as_next() {
    let app = axum::Router::new().route(
        "/sql",
        axum::routing::get(|| async {
            (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({
                    "error": "Binder Error: column \"valu\" not found\n\nhint: the column is `value`"
                })),
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let dir = nest_dir();
    let path = dir.path().to_path_buf();
    let out = tokio::task::spawn_blocking(move || {
        run_in(
            &path,
            &["sql", "SELECT valu FROM t", "--json", "--url", &url],
        )
    })
    .await
    .unwrap();
    assert_eq!(out.code, 5, "{}", out.stderr);
    let v = error_object(&out);
    assert_eq!(v["error"]["code"], "sql_invalid");
    assert_eq!(v["error"]["next"], "the column is `value`");
}

/// `--history-source` reaches only single-nest `dev`; a runtime directory refuses it by name rather
/// than starting and reading every mount's history from the RPC regardless.
#[test]
fn a_runtime_directory_refuses_history_source_by_name() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("mounts.toml"), "[runtime]\nname = \"t\"\n").unwrap();
    let out = run_in(dir.path(), &["dev", "--history-source", "xatu"]);
    assert_ne!(out.code, 0, "{}", out.stderr);
    assert!(out.stderr.contains("--history-source"), "{}", out.stderr);
}
