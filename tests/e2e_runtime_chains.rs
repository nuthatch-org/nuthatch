//! A chain added to a running runtime, and its RPC pool replaced, over the admin API (RFC-0073 FR9).
//!
//! The fakes below are real JSON-RPC servers on loopback, because what is under test is the pool a
//! cursor dials: a `Source` stand-in would never see a URL, so it could prove neither the swap nor
//! the redaction.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{extract::State, Json, Router};
use serde_json::{json, Value};

use common::tape::*;
use nuthatch::{rpc::RpcClient, runtime, source::LivePool, source::Source};

use crate::e2e_runtime_lifecycle::{call, empty_runtime, OPEN};

const DEVNET_ID: u64 = 31_337;

/// An empty chain that answers what a cursor asks, counting each method.
#[derive(Clone)]
struct FakeChain {
    chain_id: u64,
    tip: Arc<AtomicU64>,
    /// Milliseconds `eth_blockNumber` waits before answering.
    slow_tip_ms: Arc<AtomicU64>,
    calls: Arc<Mutex<HashMap<String, usize>>>,
    port: u16,
}

impl FakeChain {
    fn calls(&self, method: &str) -> usize {
        self.calls.lock().unwrap().get(method).copied().unwrap_or(0)
    }

    fn total(&self) -> usize {
        self.calls.lock().unwrap().values().sum()
    }

    /// As an operator pastes a keyed endpoint: credentials, a key in the path and one in the query.
    fn keyed_url(&self) -> String {
        format!(
            "http://user:SECRETPW@127.0.0.1:{}/v3/SECRETKEY?apikey=SECRETQ",
            self.port
        )
    }

    fn shown(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

fn hex(n: u64) -> String {
    format!("0x{n:x}")
}

fn header(n: u64) -> Value {
    json!({
        "number": hex(n),
        "hash": format!("0x{:064x}", n + 1),
        "parentHash": format!("0x{n:064x}"),
        "timestamp": hex(1_700_000_000 + n),
        "transactions": [],
    })
}

fn answer(f: &FakeChain, req: &Value) -> Value {
    let method = req["method"].as_str().unwrap_or_default();
    *f.calls
        .lock()
        .unwrap()
        .entry(method.to_string())
        .or_default() += 1;
    let tip = f.tip.load(Ordering::SeqCst);
    let result = match method {
        "eth_chainId" => json!(hex(f.chain_id)),
        "eth_blockNumber" => json!(hex(tip)),
        "eth_getLogs" => json!([]),
        "eth_getBlockByNumber" => match req["params"][0].as_str() {
            Some("latest") => header(tip),
            Some(n) if n.starts_with("0x") => {
                let n = u64::from_str_radix(&n[2..], 16).unwrap();
                if n <= tip {
                    header(n)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        },
        _ => Value::Null,
    };
    json!({"jsonrpc": "2.0", "id": req["id"].clone(), "result": result})
}

async fn fake_chain(chain_id: u64, tip: u64) -> FakeChain {
    async fn handler(State(f): State<FakeChain>, Json(req): Json<Value>) -> Json<Value> {
        let asks_tip = |r: &Value| r["method"] == "eth_blockNumber";
        let slow = f.slow_tip_ms.load(Ordering::SeqCst);
        if slow > 0 && (asks_tip(&req) || req.as_array().is_some_and(|b| b.iter().any(asks_tip))) {
            tokio::time::sleep(Duration::from_millis(slow)).await;
        }
        Json(match req.as_array() {
            Some(batch) => Value::Array(batch.iter().map(|r| answer(&f, r)).collect()),
            None => answer(&f, &req),
        })
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fake = FakeChain {
        chain_id,
        tip: Arc::new(AtomicU64::new(tip)),
        slow_tip_ms: Arc::new(AtomicU64::new(0)),
        calls: Default::default(),
        port: listener.local_addr().unwrap().port(),
    };
    let app = Router::new().fallback(handler).with_state(fake.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    fake
}

/// A port nothing listens on, so a dial is refused at once.
fn dead_url() -> String {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    format!("http://127.0.0.1:{port}/v3/SECRETKEY?apikey=SECRETQ")
}

fn assert_redacted(what: &str, body: &str) {
    assert!(
        !body.contains("SECRET"),
        "{what} printed an RPC URL past its host: {body}"
    );
}

/// An empty runtime declaring `arbitrum-one` only, holding one nest on `devnet` at `data/<nid>`.
async fn devnet_runtime(
    roost: &std::path::Path,
    nid: &str,
) -> (
    Arc<tokio::sync::Mutex<runtime::RuntimeHandles>>,
    axum::Router,
    crate::e2e_runtime_lifecycle::CursorIntake,
) {
    let (mut handles, _tape, intake) = empty_runtime(roost, nid).await;
    let nest = runtime::MountTable::data_dir(roost, nid).join("nuthatch.toml");
    let toml = std::fs::read_to_string(&nest).unwrap().replace(
        "chain = \"arbitrum-one\"\nchain_id = 42161",
        &format!("chain = \"devnet\"\nchain_id = {DEVNET_ID}"),
    );
    assert!(toml.contains("devnet"), "premise: the nest moved chain");
    std::fs::write(&nest, toml).unwrap();
    handles.mount_ctx.chains.declared = runtime::MountTable::load(roost)
        .unwrap()
        .chain_endpoints()
        .unwrap();
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = Arc::new(nuthatch::mount_jobs::MountJobs::load(
        &tempfile::tempdir().unwrap().keep(),
    ));
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, Some(OPEN.to_string()));
    (handles, routes, intake)
}

fn declare_body(chain: &str, chain_id: u64, urls: &[String]) -> String {
    json!({"chain": chain, "chain_id": chain_id, "rpc_urls": urls}).to_string()
}

fn chains_in_table(roost: &std::path::Path) -> Vec<runtime::ChainEndpoint> {
    runtime::MountTable::load(roost)
        .expect("the rewritten mounts.toml must still load, or the next start fails")
        .chain_endpoints()
        .unwrap()
}

/// FR9: a chain declared over the API is dormant until a nest mounts onto it, and the mount then
/// starts its cursor without a restart. Re-declaring it unchanged is idempotent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_declared_live_takes_a_mount_without_a_restart() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "ab12".repeat(16);
    let (handles, routes, mut intake) = devnet_runtime(roost.path(), &nid).await;
    let rpc = fake_chain(DEVNET_ID, 10).await;

    let (status, body) = call(
        &routes,
        "POST",
        "/_admin/chains",
        None,
        Some(
            &json!({
                "chain": "devnet",
                "chain_id": DEVNET_ID,
                "rpc_urls": [rpc.keyed_url()],
                "finality_depth": 2,
                "log_window": 5,
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    assert_redacted("POST /_admin/chains", &body);
    let declared: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(declared["state"], "dormant");
    assert_eq!(declared["rpc_urls"], json!([rpc.shown()]));
    assert_eq!(
        rpc.calls("eth_getLogs"),
        0,
        "a dormant chain indexes nothing"
    );

    let (status, body) = call(
        &routes,
        "POST",
        "/_admin/chains",
        None,
        Some(
            &json!({
                "chain": "devnet",
                "chain_id": DEVNET_ID,
                "rpc_urls": [rpc.keyed_url()],
                "finality_depth": 2,
                "log_window": 5,
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(status, 200, "the same declaration again is a no-op: {body}");

    let persisted = chains_in_table(roost.path());
    let devnet = persisted
        .iter()
        .find(|c| c.chain == "devnet")
        .expect("the declaration is in mounts.toml");
    assert_eq!(devnet.chain_id, DEVNET_ID);
    assert_eq!(
        devnet.rpc_urls,
        vec![rpc.keyed_url()],
        "the file keeps the key: it is what the next start dials"
    );
    assert_eq!(
        (devnet.finality_depth, devnet.log_window),
        (Some(2), Some(5))
    );
    assert_eq!(persisted.len(), 2, "arbitrum-one is kept beside it");

    let (status, body) = call(
        &routes,
        "POST",
        "/_admin/nests?wait=true",
        None,
        Some(&json!({"name": "usdc", "nid": nid}).to_string()),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (chain, ingest) = intake.try_recv().expect("the mount started a cursor");
    assert_eq!(chain, "devnet");

    let last_block = || {
        let h = handles.try_lock().ok()?;
        h.states
            .iter()
            .find(|(n, _)| n == "usdc")
            .and_then(|(_, s)| s.store.get_meta("last_block").ok().flatten())
    };
    assert!(
        wait_until(POLL_TIMEOUT, || last_block().as_deref() == Some("10")).await,
        "the nest never followed the declared chain: last_block {:?}",
        last_block()
    );
    assert!(rpc.calls("eth_getLogs") > 0);

    let (status, body) = call(&routes, "GET", "/_admin/chains", None, None).await;
    assert_eq!(status, 200);
    assert_redacted("GET /_admin/chains", &body);
    let view: Value = serde_json::from_str(&body).unwrap();
    let devnet = view["chains"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["chain"] == "devnet")
        .unwrap();
    assert_eq!(devnet["state"], "running");
    assert_eq!(devnet["rpc_urls"], json!([rpc.shown()]));
    ingest.abort();
}

/// FR9: a running cursor's pool is replaced live. The next call goes to the new pool, the old one
/// hears nothing more, and the file names the new pool for the next start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_cursors_pool_is_replaced_live() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "cd34".repeat(16);
    let (handles, routes, mut intake) = devnet_runtime(roost.path(), &nid).await;
    let old = fake_chain(DEVNET_ID, 10).await;
    let new = fake_chain(DEVNET_ID, 10).await;

    let (status, body) = call(
        &routes,
        "POST",
        "/_admin/chains",
        None,
        Some(&declare_body("devnet", DEVNET_ID, &[old.keyed_url()])),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = call(
        &routes,
        "POST",
        "/_admin/nests?wait=true",
        None,
        Some(&json!({"name": "usdc", "nid": nid}).to_string()),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (_, ingest) = intake.try_recv().unwrap();
    let last_block = || {
        let h = handles.try_lock().ok()?;
        h.states
            .iter()
            .find(|(n, _)| n == "usdc")
            .and_then(|(_, s)| s.store.get_meta("last_block").ok().flatten())
    };
    assert!(wait_until(POLL_TIMEOUT, || last_block().as_deref() == Some("10")).await);

    let (status, body) = call(
        &routes,
        "PUT",
        "/_admin/chains/devnet/rpc",
        None,
        Some(&json!({"rpc_urls": [new.keyed_url()]}).to_string()),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_redacted("PUT /_admin/chains/devnet/rpc", &body);
    let view: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(view["rpc_urls"], json!([new.shown()]));
    assert_eq!(view["state"], "running");

    new.tip.store(12, Ordering::SeqCst);
    assert!(
        wait_until(POLL_TIMEOUT, || last_block().as_deref() == Some("12")).await,
        "the cursor did not follow the new pool: last_block {:?}",
        last_block()
    );
    // A call the old pool had in flight at the swap may still land; nothing after it may.
    let settled = old.total();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        old.total(),
        settled,
        "the replaced pool is still being asked"
    );
    assert!(new.calls("eth_blockNumber") > 0);

    let persisted = chains_in_table(roost.path());
    let devnet = persisted.iter().find(|c| c.chain == "devnet").unwrap();
    assert_eq!(devnet.rpc_urls, vec![new.keyed_url()]);
    ingest.abort();
}

/// FR9: "in-flight requests finish on the old pool". A call started before the swap answers from
/// the pool it started on, while a call after it is answered by the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_in_flight_at_a_swap_finishes_on_the_old_pool() {
    let old = fake_chain(DEVNET_ID, 100).await;
    let new = fake_chain(DEVNET_ID, 200).await;
    old.slow_tip_ms.store(1_500, Ordering::SeqCst);
    let pool = Arc::new(LivePool::new(
        RpcClient::new(vec![old.keyed_url()]).unwrap(),
        Vec::new(),
    ));

    let in_flight = tokio::spawn({
        let pool = pool.clone();
        async move { pool.tip().await }
    });
    assert!(
        wait_until(POLL_TIMEOUT, || old.calls("eth_blockNumber") == 1).await,
        "premise: the first call reached the old pool"
    );
    pool.replace(RpcClient::new(vec![new.keyed_url()]).unwrap());

    let started = std::time::Instant::now();
    assert_eq!(
        pool.tip().await.unwrap(),
        200,
        "a call after the swap uses the new pool"
    );
    assert!(
        started.elapsed() < Duration::from_millis(1_000),
        "the new call waited on the old pool's slow answer"
    );
    assert_eq!(
        in_flight.await.unwrap().unwrap(),
        100,
        "the call in flight was cut off or moved instead of finishing where it started"
    );
    assert_eq!(old.calls("eth_blockNumber"), 1);
}

/// FR9's refusals: each leaves the runtime and `mounts.toml` as they were, and none prints a key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_change_that_would_not_hold_is_refused() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "ef56".repeat(16);
    let (handles, routes, _intake) = devnet_runtime(roost.path(), &nid).await;
    let devnet = fake_chain(DEVNET_ID, 10).await;
    let elsewhere = fake_chain(777, 10).await;
    let table = || std::fs::read_to_string(roost.path().join(runtime::MOUNTS_FILE)).unwrap();

    let (status, body) = call(
        &routes,
        "POST",
        "/_admin/chains",
        None,
        Some(&declare_body("devnet", DEVNET_ID, &[devnet.keyed_url()])),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let before = table();

    let refusals: Vec<(&str, &str, String, u16)> = vec![
        (
            "a declared name with other settings",
            "POST",
            declare_body("devnet", DEVNET_ID, &[elsewhere.keyed_url()]),
            409,
        ),
        (
            "a registry name on another chain id",
            "POST",
            declare_body("base", 1, &[devnet.keyed_url()]),
            409,
        ),
        (
            "a registry chain id under another name",
            "POST",
            declare_body("my-base", 8453, &[devnet.keyed_url()]),
            409,
        ),
        (
            "finality on a chain the registry sets",
            "POST",
            json!({"chain": "base", "chain_id": 8453, "rpc_urls": [devnet.keyed_url()],
                   "finality_depth": 3})
            .to_string(),
            400,
        ),
        (
            "a chain id already declared under another name",
            "POST",
            declare_body("devnet-two", DEVNET_ID, &[devnet.keyed_url()]),
            409,
        ),
        (
            "an RPC that reports another chain",
            "POST",
            declare_body("other", 778, &[elsewhere.keyed_url()]),
            422,
        ),
        (
            "an RPC nothing answers",
            "POST",
            declare_body("other", 778, &[dead_url()]),
            502,
        ),
        (
            "a name no route could hold",
            "POST",
            declare_body("../x", 778, &[devnet.keyed_url()]),
            400,
        ),
        (
            "no endpoint at all",
            "POST",
            declare_body("other", 778, &[]),
            400,
        ),
        (
            "an endpoint that is not http",
            "POST",
            declare_body(
                "other",
                778,
                &["ftp://user:SECRETPW@host/SECRETKEY".to_string()],
            ),
            400,
        ),
        (
            "a misspelt field",
            "POST",
            json!({"chain": "other", "chain_id": 778, "rpc_urls": [devnet.keyed_url()],
                   "window": 5})
            .to_string(),
            422,
        ),
    ];
    for (what, method, body, want) in refusals {
        let (status, answer) = call(&routes, method, "/_admin/chains", None, Some(&body)).await;
        assert_eq!(status.as_u16(), want, "{what}: {answer}");
        assert_redacted(what, &answer);
        assert_eq!(table(), before, "{what} changed mounts.toml");
    }

    let put = |chain: &str, urls: Vec<String>| {
        (
            format!("/_admin/chains/{chain}/rpc"),
            json!({"rpc_urls": urls}).to_string(),
        )
    };
    for (what, (uri, body), want) in [
        (
            "an undeclared chain",
            put("nowhere", vec![devnet.keyed_url()]),
            404,
        ),
        (
            "a pool on another chain",
            put("devnet", vec![elsewhere.keyed_url()]),
            422,
        ),
        (
            "a pool nothing answers",
            put("devnet", vec![dead_url()]),
            502,
        ),
        ("an empty pool", put("devnet", Vec::new()), 400),
    ] {
        let (status, answer) = call(&routes, "PUT", &uri, None, Some(&body)).await;
        assert_eq!(status.as_u16(), want, "{what}: {answer}");
        assert_redacted(what, &answer);
        assert_eq!(table(), before, "{what} changed mounts.toml");
    }
    let (_, view) = call(&routes, "GET", "/_admin/chains", None, None).await;
    let view: Value = serde_json::from_str(&view).unwrap();
    let shown = view["chains"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["chain"] == "devnet")
        .unwrap()["rpc_urls"]
        .clone();
    assert_eq!(
        shown,
        json!([devnet.shown()]),
        "a refused PUT kept the old pool"
    );

    let (status, _) = call(
        &routes,
        "POST",
        "/_admin/chains",
        Some("wrong"),
        Some(&declare_body("other", 778, &[devnet.keyed_url()])),
    )
    .await;
    assert_eq!(status, 401);

    // Either flag rewrites the pool at every start, and a second chain makes boot refuse it.
    handles.lock().await.mount_ctx.chains.rpc_flags = true;
    let (status, answer) = call(
        &routes,
        "POST",
        "/_admin/chains",
        None,
        Some(&declare_body("other", 778, &[devnet.keyed_url()])),
    )
    .await;
    assert_eq!(status, 409, "{answer}");
    let (uri, body) = put("devnet", vec![devnet.keyed_url()]);
    let (status, answer) = call(&routes, "PUT", &uri, None, Some(&body)).await;
    assert_eq!(status, 409, "{answer}");
    handles.lock().await.mount_ctx.chains.rpc_flags = false;
    handles.lock().await.mount_ctx.state_rpc = vec!["http://archive".to_string()];
    let (status, answer) = call(
        &routes,
        "POST",
        "/_admin/chains",
        None,
        Some(&declare_body("other", 778, &[devnet.keyed_url()])),
    )
    .await;
    assert_eq!(status, 409, "{answer}");
    assert_eq!(table(), before);
}

/// Kills the child on drop, so a failed assertion does not leave a runtime holding the port.
struct Reaped(std::process::Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// FR9 "both persist in `mounts.toml`": a chain declared and repointed over the API is what the
/// next start dials, with no hand edit of the chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declared_and_repointed_chain_survives_a_restart() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "a1b2".repeat(16);
    let (_handles, routes, _intake) = devnet_runtime(roost.path(), &nid).await;
    let first = fake_chain(DEVNET_ID, 10).await;
    let second = fake_chain(DEVNET_ID, 10).await;

    let (status, body) = call(
        &routes,
        "POST",
        "/_admin/chains",
        None,
        Some(&declare_body("devnet", DEVNET_ID, &[first.keyed_url()])),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = call(
        &routes,
        "PUT",
        "/_admin/chains/devnet/rpc",
        None,
        Some(&json!({"rpc_urls": [second.keyed_url()]}).to_string()),
    )
    .await;
    assert_eq!(
        status, 200,
        "a dormant chain's pool is replaced too: {body}"
    );
    drop(routes);

    // The nest is recorded by hand so the restart has something on the chain to start.
    let table = roost.path().join(runtime::MOUNTS_FILE);
    let mut toml = std::fs::read_to_string(&table).unwrap();
    toml.push_str(&format!(
        "\n[[mounts]]\nalias = \"usdc\"\nnid = \"{nid}\"\n"
    ));
    std::fs::write(&table, toml).unwrap();
    let asked_first = first.total();

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let log = roost.path().join("dev.log");
    let mut child = Reaped(
        std::process::Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(["dev", "--dir"])
            .arg(roost.path())
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .args(["--poll-interval", "1s"])
            .env("NUTHATCH_ADMIN_TOKEN", OPEN)
            .stdout(std::process::Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .expect("spawn dev"),
    );

    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let view = loop {
        let got = client
            .get(format!("http://127.0.0.1:{port}/_admin/chains"))
            .bearer_auth(OPEN)
            .send()
            .await;
        if let Ok(resp) = got {
            if resp.status() == 200 {
                break resp.text().await.unwrap();
            }
        }
        let exited = child.0.try_wait().unwrap();
        assert!(
            exited.is_none() && std::time::Instant::now() < deadline,
            "the runtime never came up ({exited:?}): {}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_redacted("GET /_admin/chains after a restart", &view);
    let view: Value = serde_json::from_str(&view).unwrap();
    let devnet = view["chains"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["chain"] == "devnet")
        .unwrap_or_else(|| panic!("devnet missing after the restart: {view}"))
        .clone();
    assert_eq!(devnet["state"], "running", "{view}");
    assert_eq!(devnet["rpc_urls"], json!([second.shown()]));
    assert!(
        wait_until(POLL_TIMEOUT, || second.calls("eth_getLogs") > 0).await,
        "the restarted cursor never indexed through the replacement pool"
    );
    assert_eq!(
        first.total(),
        asked_first,
        "the restart dialled the pool the PUT replaced"
    );
    let log_text = std::fs::read_to_string(&log).unwrap_or_default();
    assert_redacted("the runtime's log", &log_text);
}
