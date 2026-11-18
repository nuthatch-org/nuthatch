//! `--json` on `init`, `add`, `schema` and `watch --once` (#2987): stdout is one object a script can
//! read with `jq`, and the prose a person reads moves to stderr. Run offline: `--abi`, `--chain` and
//! `--start-block` leave nothing for `init` to fetch, and its RPC is a port nothing listens on.

use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::Value;

const WETH: &str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
const DAI: &str = "0x6B175474E89094C44Da98b954EedeAC495271d0F";
const ERC20: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
    {"name":"from","type":"address","indexed":true},
    {"name":"to","type":"address","indexed":true},
    {"name":"value","type":"uint256","indexed":false}]}]"#;

fn run(dir: &Path, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .output()
        .expect("run nuthatch");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The whole of stdout, as one object.
fn object(stdout: &str) -> Value {
    assert_eq!(stdout.lines().count(), 1, "one line on stdout:\n{stdout}");
    serde_json::from_str(stdout).expect("stdout is JSON")
}

fn init_weth(dir: &Path, json: bool) -> (i32, String, String) {
    let abi = dir.join("erc20.json");
    std::fs::write(&abi, ERC20).unwrap();
    let nest = dir.join("nest");
    let abi = abi.to_string_lossy().into_owned();
    let nest = nest.to_string_lossy().into_owned();
    let mut args = vec![
        "init",
        WETH,
        "--chain",
        "mainnet",
        "--rpc",
        "http://127.0.0.1:1",
        "--abi",
        &abi,
        "--start-block",
        "100",
        "--alias",
        "weth",
        "--dir",
        &nest,
    ];
    if json {
        args.push("--json");
    }
    run(dir, &args)
}

#[test]
fn init_json_is_one_object_with_the_tables_and_what_to_run_next() {
    let dir = tempfile::tempdir().unwrap();
    let (code, stdout, stderr) = init_weth(dir.path(), true);
    assert_eq!(code, 0, "{stderr}");
    let v = object(&stdout);
    assert_eq!(v["tables"], serde_json::json!(["weth__transfer"]), "{v}");
    assert_eq!(v["chain"], "mainnet");
    assert_eq!(v["chain_id"], 1);
    let c = &v["contracts"][0];
    assert_eq!(c["address"], WETH.to_lowercase());
    assert_eq!(c["alias"], "weth");
    assert_eq!(c["deployment_block"], 100);
    assert!(c["proxy_warning"].is_null(), "{v}");
    // A local ABI skips the implementation lookup, so null here must not read as clean.
    assert_eq!(c["proxy_checked"], false, "{v}");
    let files: Vec<&str> = v["files_written"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f.as_str().unwrap())
        .collect();
    for f in ["nuthatch.toml", "abis/weth.json", "schema.json", "llms.txt"] {
        assert!(files.contains(&f), "{f} missing from {files:?}");
    }
    assert!(
        v["next"][0].as_str().unwrap().starts_with("nuthatch dev"),
        "{v}"
    );
    let nest_dir = Path::new(v["nest_dir"].as_str().unwrap());
    assert!(
        nest_dir.is_absolute() && nest_dir.join("nuthatch.toml").exists(),
        "{v}"
    );
    // The prose is still said, to stderr.
    assert!(stderr.contains("scaffolded nest"), "{stderr}");
}

#[test]
fn init_without_json_prints_as_it_did() {
    let dir = tempfile::tempdir().unwrap();
    let (code, stdout, _) = init_weth(dir.path(), false);
    assert_eq!(code, 0);
    assert!(stdout.contains("✓ scaffolded nest 'nest'"), "{stdout}");
    assert!(stdout.contains("next:  nuthatch dev"), "{stdout}");
    assert!(!stdout.trim_start().starts_with('{'), "{stdout}");
}

/// A published nest need not carry schema.json, and `init --from` must not write one into the copy,
/// since it is in the NID. The tables still come back.
#[test]
fn init_from_json_reports_a_nest_published_without_schema_json() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(init_weth(dir.path(), false).0, 0);
    let published = dir.path().join("nest");
    std::fs::remove_file(published.join("schema.json")).unwrap();
    let copy = dir.path().join("copy");
    let (code, stdout, stderr) = run(
        dir.path(),
        &[
            "init",
            "--from",
            published.to_str().unwrap(),
            "--dir",
            copy.to_str().unwrap(),
            "--json",
        ],
    );
    assert_eq!(code, 0, "{stderr}");
    let v = object(&stdout);
    assert_eq!(v["tables"], serde_json::json!(["weth__transfer"]), "{v}");
    assert!(!copy.join("schema.json").exists());
}

#[test]
fn init_json_over_an_existing_nest_fails_with_one_error_object() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(init_weth(dir.path(), true).0, 0);
    let (code, stdout, stderr) = init_weth(dir.path(), true);
    assert_ne!(code, 0);
    assert!(stdout.is_empty(), "{stdout}");
    let v: Value = serde_json::from_str(stderr.lines().last().unwrap()).expect("JSON error");
    assert_eq!(v["error"]["exit"], code, "{v}");
}

#[test]
fn add_and_schema_json_report_the_grown_nest() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(init_weth(dir.path(), false).0, 0);
    let nest = dir.path().join("nest");
    let (code, stdout, stderr) = run(
        &nest,
        &[
            "add",
            DAI,
            "--abi",
            "../erc20.json",
            "--alias",
            "dai",
            "--start-block",
            "5",
            "--rpc",
            "http://127.0.0.1:1",
            "--json",
        ],
    );
    assert_eq!(code, 0, "{stderr}");
    let v = object(&stdout);
    assert_eq!(
        v["tables"],
        serde_json::json!(["dai__transfer", "weth__transfer"])
    );
    assert_eq!(v["contracts"][1]["alias"], "dai");
    assert_eq!(v["contracts"][1]["deployment_block"], 5);
    assert!(
        v["files_written"]
            .as_array()
            .unwrap()
            .contains(&Value::from("abis/dai.json")),
        "{v}"
    );

    let (code, stdout, stderr) = run(&nest, &["schema", "--json"]);
    assert_eq!(code, 0, "{stderr}");
    let v = object(&stdout);
    assert_eq!(
        v["tables"],
        serde_json::json!(["dai__transfer", "weth__transfer"])
    );
    assert!(stderr.contains("regenerated schema.json"), "{stderr}");
}

#[cfg(feature = "watch")]
#[test]
fn watch_once_with_nothing_running_is_nest_not_running() {
    let dir = tempfile::tempdir().unwrap();
    let (code, stdout, stderr) = run(
        dir.path(),
        &["watch", "--once", "--json", "--url", "http://127.0.0.1:1"],
    );
    assert_eq!(code, 4, "{stderr}");
    assert!(stdout.is_empty(), "{stdout}");
    let v: Value = serde_json::from_str(stderr.trim()).unwrap();
    assert_eq!(v["error"]["code"], "nest_not_running");
}

#[cfg(feature = "watch")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_once_json_is_the_nests_own_answers() {
    let app = axum::Router::new()
        .route(
            "/ready",
            axum::routing::get(|| async {
                axum::Json(serde_json::json!({"ready": true, "last_block": 42}))
            }),
        )
        .route(
            "/",
            axum::routing::get(|| async { axum::Json(serde_json::json!({"tables": 3})) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let asked = url.clone();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run(&path, &["watch", "--once", "--json", "--url", &asked])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "{stderr}");
    let v = object(&stdout);
    assert_eq!(v["url"], url);
    assert_eq!(v["ready"]["last_block"], 42, "{v}");
    assert_eq!(v["summary"]["tables"], 3, "{v}");
    assert!(v["nests"].is_null(), "{v}");
}

/// A JSON-RPC stand-in on mainnet: code from block 50, no logs.
fn serve_rpc() -> String {
    fn answer(req: &Value) -> Value {
        let block = |v: &Value| {
            u64::from_str_radix(v.as_str().unwrap_or("0x0").trim_start_matches("0x"), 16)
                .unwrap_or(1_000)
        };
        let result = match req["method"].as_str().unwrap_or_default() {
            "eth_chainId" => serde_json::json!("0x1"),
            "eth_blockNumber" => serde_json::json!("0x3e8"),
            "eth_getCode" => {
                if block(&req["params"][1]) >= 50 {
                    serde_json::json!("0x6080")
                } else {
                    serde_json::json!("0x")
                }
            }
            "eth_getLogs" => serde_json::json!([]),
            "eth_getBlockByNumber" => serde_json::json!({
                "number": "0x3e8", "hash": format!("0x{:064x}", 1000),
                "parentHash": format!("0x{:064x}", 999), "timestamp": "0x1", "transactions": [],
            }),
            _ => Value::Null,
        };
        serde_json::json!({"jsonrpc": "2.0", "id": req["id"].clone(), "result": result})
    }
    async fn handler(axum::Json(req): axum::Json<Value>) -> axum::Json<Value> {
        axum::Json(match req.as_array() {
            Some(batch) => Value::Array(batch.iter().map(answer).collect()),
            None => answer(&req),
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
                axum::serve(listener, axum::Router::new().fallback(handler))
                    .await
                    .unwrap();
            })
    });
    url
}

/// With an RPC that answers, `init` finds the deployment block and samples logs for the ABI fit,
/// and every line those steps print stays off stdout.
#[test]
fn init_json_over_a_live_rpc_keeps_stdout_to_the_object() {
    let rpc = serve_rpc();
    let dir = tempfile::tempdir().unwrap();
    let abi = dir.path().join("erc20.json");
    std::fs::write(&abi, ERC20).unwrap();
    let nest = dir.path().join("nest");
    let (code, stdout, stderr) = run(
        dir.path(),
        &[
            "init",
            WETH,
            "--chain",
            "mainnet",
            "--rpc",
            &rpc,
            "--abi",
            abi.to_str().unwrap(),
            "--alias",
            "weth",
            "--dir",
            nest.to_str().unwrap(),
            "--json",
        ],
    );
    assert_eq!(code, 0, "{stderr}");
    let v = object(&stdout);
    assert_eq!(v["contracts"][0]["deployment_block"], 50, "{v}\n{stderr}");
    assert!(stderr.contains("deployed at block 50"), "{stderr}");
}
