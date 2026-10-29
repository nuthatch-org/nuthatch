//! RFC-0075 S3's gate, the node's half: the real binary follows a recorded young mainnet ERC-20 from
//! its deploy block, and what it seals and serves must equal `tests/fixtures/tab-parity/expected.json`.
//! The tab's half (`spike/dbsp-wasm`, `run-live.mjs`) holds the wasm build to the same file, against
//! the same recording, so the two are equal to each other through it.
//!
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

fn get(url: &str) -> Option<Value> {
    let out = Command::new("curl")
        .args(["-s", "-m", "10", url])
        .output()
        .ok()?;
    serde_json::from_slice(&out.stdout).ok()
}

/// What the gate compares: every sealed segment, the sealed watermark and `/balances`.
fn node_report(nest: &Path, api: &str) -> Value {
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(nest.join("segments/manifest.json")).unwrap(),
    )
    .unwrap();
    let tables: serde_json::Map<String, Value> = manifest["tables"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(t, segs)| {
            let segs = segs
                .as_array()
                .unwrap()
                .iter()
                .map(|s| {
                    let bytes =
                        std::fs::read(nest.join("segments").join(s["file"].as_str().unwrap()))
                            .unwrap();
                    json!({
                        "from_block": s["from_block"],
                        "to_block": s["to_block"],
                        "rows": s["rows"],
                        "hash": s["hash"],
                        "provisional": s.get("provisional").cloned().unwrap_or(json!(false)),
                        "bytes_hash": nuthatch_seal::content_hash(&bytes),
                    })
                })
                .collect();
            (t.clone(), Value::Array(segs))
        })
        .collect();
    let provenance = &get(&format!("{api}/sql?q=SELECT%201")).unwrap()["provenance"];
    json!({
        "segments": tables,
        "sealed_through": provenance["sealed_through"],
        "last_block": provenance["as_of"],
        "balances": get(&format!("{api}/balances?limit=1000")).unwrap(),
    })
}

#[test]
fn the_node_seals_and_serves_the_pinned_young_contract() {
    let fixture = Recorded::from_path(Path::new(&format!("{DIR}/chain.json.gz"))).unwrap();
    let tip = fixture.tip();
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

    let port = free_port();
    let api = format!("http://127.0.0.1:{port}");
    let _dev = Reaped(
        Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(["dev", "--dir"])
            .arg(&nest)
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    // Mainnet's finality is 64 blocks deep, and the recording's tip never moves.
    let ceiling = tip - 64;
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let p = get(&format!("{api}/sql?q=SELECT%201")).map(|v| v["provenance"].clone());
        if p.as_ref().is_some_and(|p| {
            p["as_of"].as_u64() == Some(tip) && p["sealed_through"].as_u64() == Some(ceiling)
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the node never sealed through {ceiling}: {p:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }

    let report = node_report(&nest, &api);
    let pinned = format!("{DIR}/expected.json");
    if std::env::var_os("NUTHATCH_PIN_TAB_PARITY").is_some() {
        std::fs::write(
            &pinned,
            serde_json::to_string_pretty(&report).unwrap() + "\n",
        )
        .unwrap();
    }
    let expected: Value = serde_json::from_str(&std::fs::read_to_string(&pinned).unwrap()).unwrap();
    assert_eq!(
        report, expected,
        "the node no longer seals what the tab is held to"
    );
    for segs in report["segments"].as_object().unwrap().values() {
        for s in segs.as_array().unwrap() {
            assert_eq!(
                s["hash"], s["bytes_hash"],
                "a segment's bytes do not hash to its name"
            );
        }
    }
}
