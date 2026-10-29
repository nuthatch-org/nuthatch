//! RFC-0075 S5's gate, the node's half: the real binary follows the recorded young ERC-20 with S4's
//! entities and views, and each request in `tests/fixtures/tab-parity/s5/requests.json` must answer
//! with the status, content type and body bytes `tests/fixtures/tab-parity/s5-expected.json` pins.
//! The tab's half (`spike/dbsp-wasm`, `run-answers.mjs`) is held to the same file.
//!
//! `/ready`'s clock readings, the version and the NID are masked: the clock is not the nest's, and the
//! NID moves with the version and the RPC port `init` was given.
//! `NUTHATCH_PIN_TAB_PARITY=1` writes the file from this run instead of comparing.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nuthatch_fixture_rpc::recorded::Recorded;
use serde_json::{json, Value};

const DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tab-parity");
const CONTRACT: &str = "0x0c26fe5e579c3b007ba16d921ba8204a47c05f1a";
const DEPLOY: u64 = 26_239_807;
const ABI: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
  {"name":"from","type":"address","indexed":true},
  {"name":"to","type":"address","indexed":true},
  {"name":"value","type":"uint256","indexed":false}]}]"#;
const ENTITIES: [&str; 2] = ["received", "sent"];
const MASKED: [&str; 6] = [
    "version",
    "last_poll_unixtime",
    "seconds_since_poll",
    "seconds_since_seal_progress",
    "seconds_since_progress",
    "nid",
];

struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Each masked field's value replaced in the bytes, wherever it appears, so the rest stays as sent.
fn mask(body: &str) -> String {
    let mut out = body.to_string();
    for key in MASKED {
        let needle = format!("\"{key}\":");
        let mut from = 0;
        while let Some(at) = out[from..].find(&needle) {
            let start = from + at + needle.len();
            let rest = &out[start..];
            let len = if rest.starts_with('"') {
                rest[1..].find('"').map_or(rest.len(), |i| i + 2)
            } else {
                rest.find([',', '}']).unwrap_or(rest.len())
            };
            out.replace_range(start..start + len, "\"<masked>\"");
            from = start;
        }
    }
    out
}

/// One request as the pin records it: the status, the content type and the body's text.
fn answer(api: &str, req: &Value, scratch: &Path) -> Value {
    let method = req["method"].as_str().unwrap();
    let path = req["path"].as_str().unwrap();
    let body_file = scratch.join("body");
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-m", "20", "-X", method, "-o"])
        .arg(&body_file)
        .args(["-w", "%{http_code} %{content_type}"]);
    if let Some(body) = req["body"].as_str() {
        cmd.args(["--data-binary", body]);
    }
    let out = cmd.arg(format!("{api}{path}")).output().unwrap();
    let meta = String::from_utf8(out.stdout).unwrap();
    let (status, content_type) = meta.split_once(' ').unwrap();
    let body = std::fs::read_to_string(&body_file).unwrap();
    json!({
        "method": method,
        "path": path,
        "status": status.parse::<u16>().unwrap(),
        "content_type": content_type,
        "body": mask(&body),
    })
}

fn probe(api: &str, path: &str) -> Option<Value> {
    let out = Command::new("curl")
        .args(["-s", "-m", "10", &format!("{api}{path}")])
        .output()
        .ok()?;
    serde_json::from_slice(&out.stdout).ok()
}

fn wait_for(api: &str, tip: u64, log: &Path) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let ready = probe(api, "/ready");
        let at_tip = ready.as_ref().is_some_and(|r| {
            r["last_block"].as_u64() == Some(tip) && r["sealed_through"].as_u64() == Some(tip - 64)
        });
        let applied = ENTITIES.iter().all(|e| {
            probe(api, &format!("/derived/{e}?limit=1"))
                .is_some_and(|d| d["provenance"]["applied_through"].as_u64() == Some(tip))
        });
        if at_tip && applied {
            return;
        }
        if Instant::now() > deadline {
            let log = std::fs::read_to_string(log).unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(30).collect();
            panic!(
                "the node never reached {tip}: {ready:?}\n{}",
                tail.join("\n")
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn the_node_answers_the_pinned_requests_byte_for_byte() {
    let fixture = Recorded::from_path(Path::new(&format!("{DIR}/chain.json.gz")))
        .unwrap()
        .with_fork();
    let tip = fixture.tips()[0];
    let rpc = nuthatch_fixture_rpc::spawn(fixture, "127.0.0.1:0").expect("fixture RPC");

    let dir = tempfile::tempdir().unwrap();
    let nest = dir.path().join("nest");
    let abi = dir.path().join("abi.json");
    std::fs::write(&abi, ABI).unwrap();
    let init = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["init", CONTRACT, "--chain", "mainnet", "--alias", "tab"])
        .args(["--rpc", &rpc.url(), "--start-block", &DEPLOY.to_string()])
        .arg("--abi")
        .arg(&abi)
        .arg("--dir")
        .arg(&nest)
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    for f in [
        "entities.toml",
        "entities/received.sql",
        "entities/sent.sql",
        "views/20-s4.sql",
    ] {
        let to = nest.join(f);
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        std::fs::copy(format!("{DIR}/s4/{f}"), to).unwrap();
    }

    let port = free_port();
    let api = format!("http://127.0.0.1:{port}");
    let _dev = Reaped(
        Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(["dev", "--dir"])
            .arg(&nest)
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .args(["--poll-interval", "1s"])
            .stdout(std::fs::File::create(dir.path().join("dev.log")).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for(&api, tip, &dir.path().join("dev.log"));

    let requests: Vec<Value> =
        serde_json::from_str(&std::fs::read_to_string(format!("{DIR}/s5/requests.json")).unwrap())
            .unwrap();
    let got: Vec<Value> = requests
        .iter()
        .map(|r| answer(&api, r, dir.path()))
        .collect();
    let got = Value::Array(got);

    let pinned = format!("{DIR}/s5-expected.json");
    if std::env::var_os("NUTHATCH_PIN_TAB_PARITY").is_some() {
        std::fs::write(&pinned, serde_json::to_string_pretty(&got).unwrap() + "\n").unwrap();
    }
    let expected: Value = serde_json::from_str(&std::fs::read_to_string(&pinned).unwrap()).unwrap();
    for (g, e) in got
        .as_array()
        .unwrap()
        .iter()
        .zip(expected.as_array().unwrap())
    {
        assert_eq!(g, e, "the node's answer moved");
    }
    assert_eq!(
        got.as_array().unwrap().len(),
        expected.as_array().unwrap().len()
    );
}
