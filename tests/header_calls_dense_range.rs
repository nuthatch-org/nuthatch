//! #3065: what a dense backfill range costs in `eth_getBlockByNumber`, counted at a mock RPC.
//!
//! The contract: a header is bought once per *distinct block that holds a matching log* (never per
//! block in the range), not at all when the node stamps `blockTimestamp` on its logs, and the
//! timestamps that come out are identical either way, so stored state does not depend on which
//! path supplied them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{extract::State, routing::post, Json, Router};
use nuthatch::rpc::RpcClient;
use serde_json::{json, Value};

const FROM: u64 = 1_000;
const TO: u64 = 1_999; // 1,000 blocks in range
const ADDR: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Default)]
struct Counts {
    logs: AtomicUsize,
    headers: AtomicUsize,
}

/// Every third block holds 3 logs: 333 blocks with logs, 999 logs, 667 empty blocks.
fn has_logs(b: u64) -> bool {
    b.is_multiple_of(3)
}

fn ts(b: u64) -> u64 {
    1_700_000_000 + b * 2
}

fn hex(n: u64) -> String {
    format!("0x{n:x}")
}

fn parse(v: &Value) -> u64 {
    u64::from_str_radix(v.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

async fn handler(
    State((counts, stamp)): State<(Arc<Counts>, bool)>,
    Json(req): Json<Value>,
) -> Json<Value> {
    let one = |r: &Value| -> Value {
        let result = match r["method"].as_str() {
            Some("eth_getLogs") => {
                counts.logs.fetch_add(1, Ordering::SeqCst);
                let f = &r["params"][0];
                let (from, to) = (parse(&f["fromBlock"]), parse(&f["toBlock"]));
                let mut out = Vec::new();
                for b in (from..=to).filter(|b| has_logs(*b)) {
                    for li in 0..3u64 {
                        let mut l = json!({
                            "address": ADDR, "topics": [TOPIC], "data": "0x",
                            "blockNumber": hex(b), "blockHash": format!("0x{b:064x}"),
                            "transactionHash": format!("0x{:064x}", b * 10 + li),
                            "transactionIndex": "0x0", "logIndex": hex(li), "removed": false,
                        });
                        if stamp {
                            l["blockTimestamp"] = json!(hex(ts(b)));
                        }
                        out.push(l);
                    }
                }
                Value::Array(out)
            }
            Some("eth_getBlockByNumber") => {
                counts.headers.fetch_add(1, Ordering::SeqCst);
                let b = parse(&r["params"][0]);
                json!({ "number": hex(b), "hash": format!("0x{b:064x}"), "timestamp": hex(ts(b)),
                        "transactions": [] })
            }
            _ => Value::Null,
        };
        json!({"jsonrpc": "2.0", "id": r["id"], "result": result})
    };
    Json(match &req {
        Value::Array(rs) => Value::Array(rs.iter().map(one).collect()),
        r => one(r),
    })
}

/// Both skips live in `core/src/rpc.rs`'s `RpcClient`, which `Source::logs` and `Source::block_timestamps`
/// delegate to in the indexer: `get_logs` records each log's `blockTimestamp` via
/// `remember_log_timestamps`, and `block_timestamps` serves cached blocks before fetching (the same
/// cache, so a block is asked for once). The test drives those two calls, in the order the seal-direct
/// window does, and fails if either skip regresses (it passes today: 333 / 0 / 0).
///
/// Runs the fetch a seal-direct window does: getLogs, then timestamps for the log blocks.
/// Returns (getLogs calls, header calls, header calls on a repeat ask, timestamps).
async fn run(stamp: bool) -> (usize, usize, usize, HashMap<u64, u64>) {
    let counts = Arc::new(Counts::default());
    let app = Router::new()
        .route("/", post(handler))
        .with_state((counts.clone(), stamp));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let rpc = RpcClient::new(vec![url]).unwrap();
    let logs = rpc
        .get_logs(&[ADDR.to_string()], &[TOPIC.to_string()], FROM, TO)
        .await
        .unwrap();
    assert_eq!(logs.len(), 333 * 3);
    let mut blocks: Vec<u64> = logs.iter().map(|l| l.block_number).collect();
    blocks.sort_unstable();
    blocks.dedup();
    assert_eq!(blocks.len(), 333);
    let stamps = rpc.block_timestamps(&blocks).await.unwrap();
    let first = counts.headers.load(Ordering::SeqCst);
    // The overlap tail and a retried window ask for the same blocks again.
    rpc.block_timestamps(&blocks).await.unwrap();
    let repeat = counts.headers.load(Ordering::SeqCst) - first;
    server.abort();
    (counts.logs.load(Ordering::SeqCst), first, repeat, stamps)
}

#[tokio::test]
async fn a_dense_range_buys_one_header_per_log_block_and_none_when_logs_carry_the_timestamp() {
    let (get_logs, headers, repeat, without) = run(false).await;
    assert_eq!(get_logs, 1);
    assert_eq!(
        headers,
        333,
        "one header per block with a log, not one per block of the {} in range",
        TO - FROM + 1
    );
    assert_eq!(repeat, 0, "a block already seen is not asked for again");

    let (_, headers, repeat, with) = run(true).await;
    assert_eq!(
        headers, 0,
        "blockTimestamp on the log makes the header redundant"
    );
    assert_eq!(repeat, 0);

    assert_eq!(
        with, without,
        "the timestamps stored must not depend on where they came from"
    );
    assert!(without.iter().all(|(b, t)| *t == ts(*b)));
}
