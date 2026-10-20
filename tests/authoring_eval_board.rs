//! #1050 / RFC-0017 - **prove the board before anyone plays on it.**
//!
//! This is the authoring eval's Tier A, and it exists for the same reason RFC-0016's does: a
//! scoreboard nobody has checked is not a scoreboard. Before an agent is ever scored against
//! `eval/authoring.toml`, a *scripted reference solution* must walk the same scenario and satisfy
//! every criterion. If this test is red, an agent scoring 0/3 tells you nothing about the agent.
//!
//! It also pins the criteria to reality in the direction that actually rots. A criterion is a claim
//! about nuthatch's own surface - that `init` writes a `schema.json`, that `sealed_through` appears
//! in `/sql` provenance, that `value_dec` exists - and any of those could change under the eval
//! without a single eval file being touched. Then the next keyed run scores zero, and the number
//! looks like a model that got worse.
//!
//! Deliberately a **normal test**: it needs no key, no model and no network - `tools/fixture-rpc`
//! serves the chain over loopback and the ABI is a local file - so there is no reason for it to sit
//! behind `--ignored` where nothing would run it.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nuthatch_fixture_rpc::scripted::Scripted;

const ERC20_TRANSFER_ABI: &str = r#"[{"anonymous":false,"inputs":[
  {"indexed":true,"name":"from","type":"address"},
  {"indexed":true,"name":"to","type":"address"},
  {"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;

/// A child killed on drop, so a panicking assertion cannot leave a nest or an RPC behind holding a
/// port and an exclusive redb lock for whatever runs next.
struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// A loopback port nobody else holds. Bind, read the number back, release: the classic race is
/// tolerable here and far better than a hardcoded port that collides with a parallel test run.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn get(url: &str) -> Option<String> {
    let out = Command::new("curl")
        .args(["-fsS", "-m", "8", url])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

fn poll<T>(what: &str, secs: u64, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {secs}s waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn scenario() -> toml::Value {
    let raw = std::fs::read_to_string(root().join("eval/authoring.toml")).expect("read scenario");
    raw.parse::<toml::Value>().expect("parse scenario")
}

/// Pin the fixture chain to the scenario's declared tip and finality, and **prove it took**.
///
/// Extracted so both board tests use one implementation. `serve_cannot_index...` had none at all:
/// it ran against the fixture chain.s built-in defaults rather than the scenario, which is a
/// different chain from the one `start_fixture_chain` gives the keyed runner. A board that is not
/// the scenario proves nothing about it.
///
/// The read-back is new. The inline version this replaces fired two `POST`s and never checked
/// either landed, so a control endpoint that answered 200 and changed nothing would have left the
/// board exactly as wrong while looking exactly as green.
fn pin_chain(rpc_port: u16, tip: i64, finalized: i64) {
    for (path, n) in [("tip", tip), ("finalized", finalized)] {
        let ok = Command::new("curl")
            .args(["-fsS", "-m", "5", "-XPOST"])
            .arg(format!("http://127.0.0.1:{rpc_port}/control/{path}"))
            .args(["-d", &format!("{{\"number\": {n}}}")])
            .status()
            .expect("curl")
            .success();
        assert!(ok, "could not pin {path} to {n}");
    }
    let state = poll("the fixture chain to report its state", 15, || {
        get(&format!("http://127.0.0.1:{rpc_port}/control/state"))
    });
    let v: serde_json::Value = serde_json::from_str(&state).expect("parse /control/state");
    assert_eq!(v["tip"].as_i64(), Some(tip), "tip did not take: {state}");
    assert_eq!(
        v["finalized"].as_i64(),
        Some(finalized),
        "finalized did not take: {state}"
    );
}

#[test]
fn the_authoring_scenario_is_achievable_and_its_criteria_are_exact() {
    let s = scenario();
    let contract = s["contract"].as_str().unwrap().to_string();
    let chain = s["chain"].as_str().unwrap().to_string();
    let tip = s["tip"].as_integer().unwrap();
    let finalized = s["finalized"].as_integer().unwrap();

    let dir = tempfile::tempdir().expect("tmp");
    let abi = dir.path().join("erc20.json");
    std::fs::write(&abi, ERC20_TRANSFER_ABI).expect("write abi");

    // --- the fixture chain, over real HTTP so the real binary can be pointed at it ---------------
    let rpc =
        nuthatch_fixture_rpc::spawn(Scripted::new(&contract), "127.0.0.1:0").expect("fixture RPC");
    let rpc_port = rpc.port();
    let rpc_url = format!("http://127.0.0.1:{rpc_port}/");
    pin_chain(rpc_port, tip, finalized);

    // --- criterion 1: `init` succeeds, entirely offline -------------------------------------------
    let nest = dir.path().join("nest");
    let init = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["init", &contract, "--chain", &chain, "--rpc", &rpc_url])
        .arg("--abi")
        .arg(&abi)
        .arg("--dir")
        .arg(&nest)
        .output()
        .expect("run init");
    assert!(
        init.status.success(),
        "`init` failed, so the scenario is not achievable and no agent should be scored against it:\n{}\n{}",
        String::from_utf8_lossy(&init.stdout),
        String::from_utf8_lossy(&init.stderr),
    );
    for artefact in ["nuthatch.toml", "schema.json"] {
        assert!(
            nest.join(artefact).exists(),
            "criterion `init-succeeds` looks for {artefact}, and `init` did not write one. Either \
             the scenario is wrong or the scaffold changed shape - and left alone, the next keyed \
             run scores zero and it reads as a model that got worse"
        );
    }

    // --- criterion 2: `dev` reaches the pinned tip -------------------------------------------------
    let api_port = free_port();
    let mut log = tempfile::NamedTempFile::new().expect("log");
    let _dev = Reaped(
        Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(["dev", "--dir"])
            .arg(&nest)
            .args([
                "--listen",
                &format!("127.0.0.1:{api_port}"),
                "--seal-direct",
                "--poll-interval",
                "1s",
            ])
            .stdout(Stdio::from(log.reopen().expect("reopen")))
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn dev"),
    );

    let api = format!("http://127.0.0.1:{api_port}");
    let provenance = |field: &str| -> Option<i64> {
        let body = get(&format!("{api}/sql?q=select%201"))?;
        let v: serde_json::Value = serde_json::from_str(&body).ok()?;
        v["provenance"][field].as_i64()
    };
    // Read the target from the **criterion**, not from the chain pin. The first version of this
    // test used `finalized` here and never touched `criterion.value` at all - so the number written
    // in the scenario file was decorative, and editing it changed nothing. A criterion the scorer
    // does not consult is not a criterion, and it is the same fault as a gate that matches its own
    // comment. Caught by mutating the scenario and watching this test stay green.
    let criteria = s["criterion"].as_array().expect("criteria");
    let tip_criterion = criteria
        .iter()
        .find(|c| c["kind"].as_str() == Some("sealed-through"))
        .expect("a sealed-through criterion");
    let want_sealed = tip_criterion["value"]
        .as_integer()
        .expect("criterion value");
    assert_eq!(
        want_sealed, finalized,
        "the `reaches-pinned-tip` criterion targets {want_sealed} but the fixture chain's finality          is pinned at {finalized}. One of the two moved without the other, and the eval would be          scoring agents against a tip this chain never reaches"
    );

    let sealed = poll("the nest to seal the pinned history", 120, || {
        provenance("sealed_through").filter(|&n| n >= want_sealed)
    });
    assert_eq!(
        sealed, want_sealed,
        "criterion `reaches-pinned-tip` expects sealed_through == {want_sealed}"
    );

    // --- criterion 3: the canned question, with the table resolved rather than assumed -------------
    let tables: serde_json::Value = serde_json::from_str(&poll("the tables listing", 30, || {
        get(&format!("{api}/tables"))
    }))
    .expect("parse /tables");
    let names: Vec<String> = tables["tables"]
        .as_array()
        .expect("tables array")
        .iter()
        .filter_map(|t| {
            t["name"]
                .as_str()
                .or_else(|| t["table"].as_str())
                .map(String::from)
        })
        .collect();
    assert_eq!(
        names.len(),
        1,
        "the scenario is built on there being exactly one table, which is what lets the runner \
         resolve `{{table}}` instead of hardcoding the agent's alias. Found: {names:?}"
    );

    let sql_criterion = criteria
        .iter()
        .find(|c| c["kind"].as_str() == Some("sql"))
        .expect("a sql criterion");
    let sql = sql_criterion["sql"]
        .as_str()
        .unwrap()
        .replace("{table}", &names[0]);
    let expect: serde_json::Value =
        serde_json::from_str(sql_criterion["expect"].as_str().unwrap()).expect("parse expect");

    let encoded: String = url_encode(&sql);
    let body = poll("the canned question to answer", 60, || {
        get(&format!("{api}/sql?q={encoded}"))
    });
    let answer: serde_json::Value = serde_json::from_str(&body).expect("parse /sql");
    assert!(
        answer["error"].is_null(),
        "the canned question does not run against the nest the reference solution built - so it \
         cannot score an agent's either: {body}\n\ndev log:\n{}",
        read_tail(&mut log),
    );
    assert!(
        rows_equal(&expect, &answer["rows"]),
        "criterion `canned-question` expects {expect} and the reference nest answered {}. The \
         board is wrong, or nuthatch's own surface moved under it",
        answer["rows"],
    );
}

fn read_tail(f: &mut tempfile::NamedTempFile) -> String {
    let mut s = String::new();
    let _ = f.reopen().and_then(|mut h| h.read_to_string(&mut s));
    s.lines().rev().take(20).collect::<Vec<_>>().join("\n")
}

fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Order-normalised, numeric-tolerant - the same comparison Tier A and the RFC-0016 runner use. A
/// DECIMAL `"3600"` equals the number 3600, and a score must not acquire a second definition of
/// correctness merely because it belongs to a different eval.
fn rows_equal(expected: &serde_json::Value, actual: &serde_json::Value) -> bool {
    let (e, a) = match (expected.as_array(), actual.as_array()) {
        (Some(e), Some(a)) => (e, a),
        _ => return false,
    };
    if e.len() != a.len() {
        return false;
    }
    let mut used = vec![false; a.len()];
    e.iter().all(|want| {
        a.iter().enumerate().any(|(i, got)| {
            !used[i] && row_matches(want, got) && {
                used[i] = true;
                true
            }
        })
    })
}

fn row_matches(want: &serde_json::Value, got: &serde_json::Value) -> bool {
    let w = match want.as_object() {
        Some(w) => w,
        None => return want == got,
    };
    w.iter()
        .all(|(k, v)| got.get(k).is_some_and(|g| values_equal(v, g)))
}

fn values_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (as_f64(a), as_f64(b)) {
        (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0),
        _ => scalar_string(a) == scalar_string(b),
    }
}

fn as_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.parse().ok())
}

fn scalar_string(v: &serde_json::Value) -> String {
    v.as_str()
        .map(String::from)
        .unwrap_or_else(|| v.to_string())
}

/// The property the runner's correctness rests on: **`serve` cannot index on the subject's behalf.**
///
/// Review of #1050 found the first runner starting `dev` after the subject finished, which broke the
/// score in both directions - a compliant agent that left `dev` running lost the exclusive redb lock
/// fight and scored *worse*, while an agent that ran only `init` had its indexing done for it and
/// collected two criteria it never earned. The runner now reads the store back through `serve`, the
/// RFC-0022 query-FE role, which owns no cursor and never advances one.
///
/// That is a claim about nuthatch's surface, so it is pinned here rather than trusted. If `serve`
/// ever gained the ability to advance a cursor, the authoring eval would start passing agents that
/// built a nest and never ran it - and nothing else in the tree would notice.
#[test]
fn serve_cannot_index_so_the_scorer_cannot_do_the_agents_work() {
    let s = scenario();
    let contract = s["contract"].as_str().unwrap().to_string();
    let chain = s["chain"].as_str().unwrap().to_string();

    let dir = tempfile::tempdir().expect("tmp");
    let abi = dir.path().join("erc20.json");
    std::fs::write(&abi, ERC20_TRANSFER_ABI).expect("write abi");

    let rpc =
        nuthatch_fixture_rpc::spawn(Scripted::new(&contract), "127.0.0.1:0").expect("fixture RPC");
    let rpc_port = rpc.port();
    pin_chain(
        rpc_port,
        s["tip"].as_integer().unwrap(),
        s["finalized"].as_integer().unwrap(),
    );

    // Scaffold a nest and then deliberately never run `dev` - the "agent stopped after init" case.
    let nest = dir.path().join("nest");
    let init = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["init", &contract, "--chain", &chain, "--rpc"])
        .arg(format!("http://127.0.0.1:{rpc_port}/"))
        .arg("--abi")
        .arg(&abi)
        .arg("--dir")
        .arg(&nest)
        .output()
        .expect("run init");
    assert!(init.status.success(), "init failed");
    assert!(
        !nest.join("nuthatch.redb").exists(),
        "`init` alone created a hot store; the runner distinguishes 'never indexed' from 'locked'          by exactly that file's absence, and this would break it"
    );

    let api_port = free_port();
    let mut serve = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["serve", "--dir"])
        .arg(&nest)
        .args(["--listen", &format!("127.0.0.1:{api_port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn serve");

    let status = poll("serve to decide what it is doing", 45, || {
        serve.try_wait().ok().flatten()
    });
    assert!(
        !status.success(),
        "`serve` came up against a nest with no hot store. If it can serve - or worse, create - one,          then an agent that ran only `init` would collect `reaches-pinned-tip` and `canned-question`          it never earned"
    );
    assert!(
        !nest.join("nuthatch.redb").exists(),
        "`serve` created a hot store. It is documented as never creating or writing the local redb,          and the authoring eval's whole correctness rests on it"
    );
}
