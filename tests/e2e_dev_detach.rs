//! `nuthatch dev --detach` and `nuthatch stop` (#2987), through the real binary against a JSON-RPC
//! stand-in: one non-interactive shell can start a nest, query it, and stop it.

mod common;

use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::{json, Value};

use common::tape::*;

const TIP: u64 = 400;

fn hex(n: u64) -> String {
    format!("0x{n:x}")
}

fn parse(v: &Value) -> u64 {
    u64::from_str_radix(v.as_str().unwrap_or("0x0").trim_start_matches("0x"), 16).unwrap_or(0)
}

/// One Transfer per block, on Arbitrum One unless `chain_id` says otherwise.
fn answer(req: &Value, chain_id: u64) -> Value {
    let header = |n: u64| {
        json!({
            "number": hex(n),
            "hash": block_hash(n, 0),
            "parentHash": block_hash(n.saturating_sub(1), 0),
            "timestamp": hex(1_700_000_000 + n),
            "transactions": [],
        })
    };
    let result = match req["method"].as_str().unwrap_or_default() {
        "eth_chainId" => json!(hex(chain_id)),
        "eth_blockNumber" => json!(hex(TIP)),
        "eth_getBlockByNumber" => match req["params"][0].as_str() {
            Some("latest") => header(TIP),
            Some("finalized" | "safe") => header(TIP - 50),
            Some(_) => header(parse(&req["params"][0])),
            None => Value::Null,
        },
        "eth_getLogs" => {
            let f = &req["params"][0];
            let (from, to) = (parse(&f["fromBlock"]), parse(&f["toBlock"]).min(TIP));
            Value::Array(
                (from..=to)
                    .map(|b| {
                        json!({
                            "address": USDC.to_lowercase(),
                            "topics": [TRANSFER_TOPIC0, format!("0x{:064x}", 1), format!("0x{:064x}", 2)],
                            "data": format!("0x{:064x}", b),
                            "blockNumber": hex(b),
                            "blockHash": block_hash(b, 0),
                            "transactionHash": format!("0x{:064x}", b),
                            "transactionIndex": "0x0",
                            "logIndex": "0x0",
                            "removed": false,
                        })
                    })
                    .collect(),
            )
        }
        _ => Value::Null,
    };
    json!({"jsonrpc": "2.0", "id": req["id"].clone(), "result": result})
}

fn serve_rpc(chain_id: u64) -> String {
    async fn handler(
        axum::extract::State(chain_id): axum::extract::State<u64>,
        axum::Json(req): axum::Json<Value>,
    ) -> axum::Json<Value> {
        axum::Json(match req.as_array() {
            Some(batch) => Value::Array(batch.iter().map(|r| answer(r, chain_id)).collect()),
            None => answer(&req, chain_id),
        })
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let app = axum::Router::new().fallback(handler).with_state(chain_id);
                axum::serve(listener, app).await.unwrap();
            })
    });
    url
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn nuthatch(state: &Path, args: &[&str]) -> (i32, String, String) {
    run_bin(Path::new(env!("CARGO_BIN_EXE_nuthatch")), state, args)
}

fn run_bin(bin: &Path, state: &Path, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(bin)
        .args(args)
        .env("XDG_STATE_HOME", state)
        .stdin(Stdio::null())
        .output()
        .expect("run nuthatch");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Gone, or a zombie its new parent has yet to reap.
fn has_exited(pid: u64) -> bool {
    let out = Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .expect("ps");
    let stat = String::from_utf8_lossy(&out.stdout);
    stat.trim().is_empty() || stat.trim().starts_with('Z')
}

/// Stops a detached `dev` the test leaves behind when an assertion fails before `stop` does.
struct Reap(u64);

impl Drop for Reap {
    fn drop(&mut self) {
        // SAFETY: a plain signal to the pid this test started.
        unsafe { libc::kill(self.0 as libc::pid_t, libc::SIGTERM) };
    }
}

fn files_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn detach_returns_serving_then_sql_answers_and_stop_ends_it() {
    let rpc = serve_rpc(42_161);
    let nest = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    scaffold_nest(nest.path(), "usdc", USDC);
    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let dir = nest.path().to_str().unwrap();

    let (code, stdout, stderr) = nuthatch(
        state.path(),
        &[
            "dev",
            "--detach",
            "--json",
            "--dir",
            dir,
            "--rpc",
            &rpc,
            "--listen",
            &listen,
            "--no-admin",
            "--backfill",
            "300",
        ],
    );
    assert_eq!(code, 0, "{stdout}{stderr}");
    let started: Value = serde_json::from_str(stdout.trim()).expect("one JSON object");
    let pid = started["pid"].as_u64().expect("a pid");
    let reap = Reap(pid);
    assert_eq!(started["url"], format!("http://{listen}"));
    let log = Path::new(started["log"].as_str().unwrap()).to_path_buf();
    assert!(
        log.starts_with(state.path()),
        "the log is outside the nest: {log:?}"
    );
    assert!(log.exists());
    assert!(started["stop"]
        .as_str()
        .unwrap()
        .starts_with("nuthatch stop --dir "));

    // It answers as soon as `--detach` returns, through the running instance.
    let url = format!("http://{listen}");
    let (code, stdout, stderr) = nuthatch(
        state.path(),
        &[
            "sql",
            "SELECT count(*) AS n FROM usdc__transfer",
            "--json",
            "--dir",
            dir,
            "--url",
            &url,
        ],
    );
    assert_eq!(code, 0, "{stderr}");
    let row: Value = serde_json::from_str(stdout.lines().next().unwrap()).unwrap();
    assert!(row["n"].as_u64().is_some(), "{row}");

    // A second start is refused while the first runs.
    let (code, _, stderr) = nuthatch(
        state.path(),
        &[
            "dev", "--detach", "--dir", dir, "--rpc", &rpc, "--listen", &listen,
        ],
    );
    assert_ne!(code, 0);
    assert!(stderr.contains("already running"), "{stderr}");

    let (code, stdout, stderr) = nuthatch(state.path(), &["stop", "--dir", dir, "--json"]);
    assert_eq!(code, 0, "{stderr}");
    let stopped: Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(stopped["stopped"], pid);
    std::mem::forget(reap);
    assert!(has_exited(pid), "pid {pid} still runs");

    let (code, _, stderr) = nuthatch(state.path(), &["stop", "--dir", dir, "--json"]);
    assert_eq!(code, 4, "{stderr}");
    let v: Value = serde_json::from_str(stderr.trim()).unwrap();
    assert_eq!(v["error"]["code"], "nest_not_running");

    // Nothing of the detached run is left in the nest beside what `dev` itself writes.
    let names = files_in(nest.path());
    assert!(!names.iter().any(|n| n.starts_with("dev.")), "{names:?}");
}

#[test]
fn a_dev_that_fails_to_start_fails_the_detach_with_its_log() {
    // An endpoint on the wrong chain is refused before anything is served.
    let rpc = serve_rpc(1);
    let nest = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    scaffold_nest(nest.path(), "usdc", USDC);
    let listen = format!("127.0.0.1:{}", free_port());
    let (code, stdout, stderr) = nuthatch(
        state.path(),
        &[
            "dev",
            "--detach",
            "--dir",
            nest.path().to_str().unwrap(),
            "--rpc",
            &rpc,
            "--listen",
            &listen,
            "--no-admin",
        ],
    );
    assert_ne!(code, 0, "{stdout}");
    assert!(stderr.contains("exited"), "{stderr}");
    assert!(
        stderr.contains("is on chain 1"),
        "the log's end is shown: {stderr}"
    );
}

#[test]
fn a_taken_port_is_port_in_use_not_a_false_success() {
    let rpc = serve_rpc(42_161);
    let nest = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    scaffold_nest(nest.path(), "usdc", USDC);
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = holder.local_addr().unwrap().to_string();
    let dir = nest.path().to_str().unwrap();
    let (code, stdout, stderr) = nuthatch(
        state.path(),
        &[
            "dev",
            "--detach",
            "--json",
            "--dir",
            dir,
            "--rpc",
            &rpc,
            "--listen",
            &listen,
            "--no-admin",
        ],
    );
    assert_eq!(code, 10, "{stdout}{stderr}");
    let v: Value = serde_json::from_str(stderr.trim()).unwrap();
    assert_eq!(v["error"]["code"], "port_in_use");
    // Nothing was recorded, so `stop` has no nest to report.
    let (code, _, stderr) = nuthatch(state.path(), &["stop", "--dir", dir, "--json"]);
    assert_eq!(code, 4, "{stderr}");
}

#[test]
fn detach_without_a_nest_is_no_nest() {
    let empty = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let (code, _, stderr) = nuthatch(
        state.path(),
        &[
            "dev",
            "--detach",
            "--json",
            "--dir",
            empty.path().to_str().unwrap(),
        ],
    );
    assert_eq!(code, 3, "{stderr}");
}

/// A binary installed under another name is still recognised as the running `dev`: a second start
/// is refused rather than truncating the live log, and `stop` stops it.
#[test]
fn a_renamed_binary_is_still_known_by_its_executable() {
    let rpc = serve_rpc(42_161);
    let nest = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let bins = tempfile::tempdir().unwrap();
    let renamed = bins.path().join("nh-renamed");
    std::fs::copy(env!("CARGO_BIN_EXE_nuthatch"), &renamed).unwrap();
    scaffold_nest(nest.path(), "usdc", USDC);
    let listen = format!("127.0.0.1:{}", free_port());
    let dir = nest.path().to_str().unwrap();
    let args = [
        "dev",
        "--detach",
        "--json",
        "--dir",
        dir,
        "--rpc",
        &rpc,
        "--listen",
        &listen,
        "--no-admin",
    ];

    let (code, stdout, stderr) = run_bin(&renamed, state.path(), &args);
    assert_eq!(code, 0, "{stdout}{stderr}");
    let started: Value = serde_json::from_str(stdout.trim()).unwrap();
    let reap = Reap(started["pid"].as_u64().unwrap());

    let (code, _, stderr) = run_bin(&renamed, state.path(), &args);
    assert_ne!(code, 0);
    assert!(stderr.contains("already running"), "{stderr}");

    let (code, _, stderr) = run_bin(&renamed, state.path(), &["stop", "--dir", dir]);
    assert_eq!(code, 0, "{stderr}");
    std::mem::forget(reap);
}
