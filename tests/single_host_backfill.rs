//! A 16-way seal-direct backfill against one RPC host whose first answers arrive after the client has
//! stopped waiting. That is the condition the single-endpoint cap was written for: a timed-out
//! window ended the ingest task, nothing awaited it, and the process sat idle with every worker parked.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use common::tape::*;
use nuthatch::{indexer, rpc::RpcClient, source::Source};

const TIP: u64 = 20_000;
const FINALIZED: u64 = TIP - 100;
const LOGS_PER_BLOCK: u64 = 2;
const CONCURRENCY: usize = 16;
const TIMEOUT: Duration = Duration::from_millis(500);
/// The whole first wave of windows outlives the client's timeout.
const SLOW_GETLOGS: u64 = CONCURRENCY as u64;

fn hex(n: u64) -> String {
    format!("0x{n:x}")
}

fn parse(v: &Value) -> u64 {
    u64::from_str_radix(v.as_str().unwrap_or("0x0").trim_start_matches("0x"), 16).unwrap_or(0)
}

fn answer(req: &Value) -> Value {
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
        "eth_chainId" => json!(hex(42_161)),
        "eth_blockNumber" => json!(hex(TIP)),
        "eth_getBlockByNumber" => match req["params"][0].as_str() {
            Some("latest") => header(TIP),
            Some("finalized" | "safe") => header(FINALIZED),
            Some(_) => header(parse(&req["params"][0])),
            None => Value::Null,
        },
        "eth_getLogs" => {
            let f = &req["params"][0];
            let (from, to) = (parse(&f["fromBlock"]), parse(&f["toBlock"]).min(TIP));
            let logs: Vec<Value> = (from..=to)
                .flat_map(|b| (0..LOGS_PER_BLOCK).map(move |i| (b, i)))
                .map(|(b, i)| {
                    json!({
                        "address": USDC.to_lowercase(),
                        "topics": [TRANSFER_TOPIC0, format!("0x{:064x}", 1), format!("0x{:064x}", 2)],
                        "data": format!("0x{:064x}", b * 10 + i),
                        "blockNumber": hex(b),
                        "blockHash": block_hash(b, 0),
                        "transactionHash": format!("0x{:064x}", (b << 20) | i),
                        "transactionIndex": "0x0",
                        "logIndex": hex(i),
                        "removed": false,
                    })
                })
                .collect();
            Value::Array(logs)
        }
        _ => Value::Null,
    };
    json!({"jsonrpc": "2.0", "id": req["id"].clone(), "result": result})
}

/// One JSON-RPC host over HTTP/1, on its own thread and runtime so the client's cannot stall it. The
/// first `slow` `eth_getLogs` answers arrive after [`TIMEOUT`] three times over.
fn serve(get_logs: Arc<AtomicU64>, slow: u64) -> String {
    async fn handler(
        axum::extract::State((get_logs, slow)): axum::extract::State<(Arc<AtomicU64>, u64)>,
        axum::Json(req): axum::Json<Value>,
    ) -> axum::Json<Value> {
        if req["method"] == "eth_getLogs" && get_logs.fetch_add(1, Ordering::SeqCst) < slow {
            tokio::time::sleep(TIMEOUT * 3).await;
        }
        axum::Json(match req.as_array() {
            Some(batch) => Value::Array(batch.iter().map(answer).collect()),
            None => answer(&req),
        })
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let app = axum::Router::new()
                    .fallback(handler)
                    .with_state((get_logs, slow));
                axum::serve(listener, app).await.unwrap();
            })
    });
    url
}

/// Every block from the backfill's origin through the finalized one, each with all its logs.
fn assert_sealed_whole(dir: &std::path::Path) {
    let rows = nuthatch::analytics::query(
        dir,
        "SELECT min(block_number) AS lo, max(block_number) AS hi, count(*) AS n FROM usdc__transfer",
    )
    .unwrap();
    // `--backfill TIP` starts the backfill at block 0.
    assert_eq!(
        rows[0]["lo"],
        json!(0),
        "a leading window was lost: {rows:?}"
    );
    assert_eq!(rows[0]["hi"], json!(FINALIZED), "{rows:?}");
    assert_eq!(
        rows[0]["n"],
        json!((FINALIZED + 1) * LOGS_PER_BLOCK),
        "a window was lost: {rows:?}"
    );
}

/// The retry under a single host's late answers, below the CLI so the client's timeout can be short.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sixteen_way_backfill_to_one_host_survives_answers_that_time_out() {
    let get_logs = Arc::new(AtomicU64::new(0));
    let url = serve(get_logs.clone(), SLOW_GETLOGS);
    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let source: Arc<dyn Source> = Arc::new(RpcClient::with_timeout(vec![url], TIMEOUT).unwrap());
    let rt = indexer::spawn_nest(
        source,
        dir.path().to_path_buf(),
        cfg,
        Some(TIP),
        true,
        CONCURRENCY,
        None,
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let store = rt.state.store.clone();
    let done = wait_until_progress(
        Duration::from_secs(60),
        Duration::from_secs(600),
        || store.sealed_through() >= FINALIZED || rt.ingest.is_finished(),
        || (store.sealed_through(), get_logs.load(Ordering::SeqCst)),
    )
    .await;
    if rt.ingest.is_finished() {
        panic!("the ingest task ended: {:?}", rt.ingest.await);
    }
    assert!(done, "the backfill stopped moving");
    rt.ingest.abort();
    assert!(
        get_logs.load(Ordering::SeqCst) > SLOW_GETLOGS,
        "no window was ever answered too late, so this proves nothing"
    );
    assert_sealed_whole(dir.path());
}

/// `nuthatch dev` with one `--rpc` runs the concurrency asked for, and says so on `/ready`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_runs_a_sixteen_way_seal_direct_backfill_on_one_endpoint() {
    let get_logs = Arc::new(AtomicU64::new(0));
    let url = serve(get_logs, 0);
    let dir = tempfile::tempdir().unwrap();
    scaffold_nest(dir.path(), "usdc", USDC);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut dev = std::process::Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["dev", "--dir"])
        .arg(dir.path())
        .args(["--seal-direct", "--concurrency", &CONCURRENCY.to_string()])
        .args(["--backfill", &TIP.to_string(), "--rpc", &url])
        .args(["--listen", &format!("127.0.0.1:{port}"), "--no-admin"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let http = reqwest::Client::new();
    let get = |path: &'static str| {
        let http = http.clone();
        async move {
            let r = http
                .get(format!("http://127.0.0.1:{port}{path}"))
                .send()
                .await
                .ok()?;
            r.json::<Value>().await.ok()
        }
    };
    let start = std::time::Instant::now();
    let mut sealed = 0;
    while sealed < FINALIZED && start.elapsed() < Duration::from_secs(300) {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Some(v) = get("/").await {
            sealed = v["sealed_through"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    let ready = get("/ready").await;
    let _ = dev.kill();
    let _ = dev.wait();
    assert_eq!(sealed, FINALIZED, "the backfill did not finish");
    let ready = ready.expect("/ready answered");
    assert_eq!(
        ready["seal_direct_concurrency"],
        json!({"requested": CONCURRENCY, "effective": CONCURRENCY, "capped_by": null}),
        "{ready}"
    );
    assert_sealed_whole(dir.path());
}
