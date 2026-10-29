//! RFC-0075 S4's gate, the node's half: the real binary follows the recorded young ERC-20 with two
//! authored entities and three views, takes a scripted reorg of its last six blocks, and what
//! `/derived/{entity}` and each view answer, before and after, must equal
//! `tests/fixtures/tab-parity/s4-expected.json`. The tab's half (`spike/dbsp-wasm`, `run-live.mjs`)
//! is held to the same file over the same fixture.
//!
//! `NUTHATCH_PIN_TAB_PARITY=1` writes the file from this run instead of comparing.

use std::collections::BTreeMap;
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
const VIEWS: [&str; 3] = ["per_kblock", "busy_receivers", "two_way"];

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

fn curl(args: &[&str]) -> Option<Value> {
    let out = Command::new("curl")
        .args(["-s", "-m", "10"])
        .args(args)
        .output()
        .ok()?;
    serde_json::from_slice(&out.stdout).ok()
}

fn sql(api: &str, q: &str) -> Option<Value> {
    curl(&[
        "-G",
        &format!("{api}/sql"),
        "--data-urlencode",
        &format!("q={q}"),
    ])
}

/// Rows in an order neither engine chose: each by its text with the keys sorted.
fn sorted(rows: &[Value]) -> Vec<Value> {
    let mut out: Vec<(String, Value)> = rows
        .iter()
        .map(|r| {
            let canon: BTreeMap<String, Value> = r
                .as_object()
                .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default();
            (serde_json::to_string(&canon).unwrap(), r.clone())
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.into_iter().map(|(_, r)| r).collect()
}

fn report(api: &str) -> Value {
    let derived: serde_json::Map<String, Value> = ENTITIES
        .iter()
        .map(|e| {
            let got = curl(&[&format!("{api}/derived/{e}?limit=1000")]).unwrap();
            let items = got["items"].as_array().unwrap();
            assert_eq!(
                got["rows"].as_u64(),
                Some(items.len() as u64),
                "{e} holds more rows than one page"
            );
            (
                e.to_string(),
                json!({"rows": got["rows"], "items": sorted(items)}),
            )
        })
        .collect();
    let views: serde_json::Map<String, Value> = VIEWS
        .iter()
        .map(|v| {
            let got = sql(api, &format!("SELECT * FROM {v}")).unwrap();
            let rows = got["rows"]
                .as_array()
                .unwrap_or_else(|| panic!("{v}: {got}"));
            (v.to_string(), Value::Array(sorted(rows)))
        })
        .collect();
    let p = &sql(api, "SELECT 1").unwrap()["provenance"];
    json!({
        "last_block": p["as_of"],
        "sealed_through": p["sealed_through"],
        "derived": derived,
        "views": views,
    })
}

fn wait_for(api: &str, tip: u64, log: &Path) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let p = sql(api, "SELECT 1").map(|v| v["provenance"].clone());
        let at_tip = p.as_ref().is_some_and(|p| {
            p["as_of"].as_u64() == Some(tip) && p["sealed_through"].as_u64() == Some(tip - 64)
        });
        let applied = ENTITIES.iter().all(|e| {
            curl(&[&format!("{api}/derived/{e}?limit=1")]).is_some_and(|d| {
                d["provenance"]["applied_through"].as_u64() == Some(tip)
                    || d["provenance"]["applied_through"].is_null()
            })
        });
        if at_tip && applied {
            return;
        }
        if Instant::now() > deadline {
            let log = std::fs::read_to_string(log).unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(30).collect();
            panic!("the node never reached {tip}: {p:?}\n{}", tail.join("\n"));
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn the_node_derives_and_answers_the_pinned_views_through_a_reorg() {
    let fixture = Recorded::from_path(Path::new(&format!("{DIR}/chain.json.gz")))
        .unwrap()
        .with_fork();
    let tips = fixture.tips();
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
    wait_for(&api, tips[0], &dir.path().join("dev.log"));
    let before = report(&api);
    let forked = curl(&["-X", "POST", &format!("{}control/fork", rpc.url())]).unwrap();
    assert_eq!(forked["tip"].as_u64(), Some(tips[1]), "{forked}");
    wait_for(&api, tips[1], &dir.path().join("dev.log"));
    let after = report(&api);
    let got = json!({"before": before, "after": after});

    let pinned = format!("{DIR}/s4-expected.json");
    if std::env::var_os("NUTHATCH_PIN_TAB_PARITY").is_some() {
        std::fs::write(&pinned, serde_json::to_string_pretty(&got).unwrap() + "\n").unwrap();
    }
    let expected: Value = serde_json::from_str(&std::fs::read_to_string(&pinned).unwrap()).unwrap();
    assert_eq!(
        got, expected,
        "the node no longer derives what the tab is held to"
    );
    assert_ne!(
        got["before"]["derived"], got["after"]["derived"],
        "the reorg changed no entity, so it proves nothing"
    );
}
