//! A stock `graph init --from-contract` scaffold, ported as an operator ports it and asked over GraphQL.
//!
//! `tests/fixtures/bayc-scaffold` is graph-cli 0.98.1's output for BAYC, unedited: every entity's id is
//! `event.transaction.hash.concatI32(event.logIndex.toI32())`. The id graph-node serves for that is the
//! hash's bytes then the i32 little-endian, as `0x` and lowercase hex (graph-ts `Bytes.concatI32`,
//! graph-node `scalar::Bytes`'s `Display`).

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use nuthatch::rpc::Log;
use nuthatch::{indexer, serve};

mod common;

use common::tape::{account, block_hash, empty_block, BlockFixture, TapeSource, TRANSFER_TOPIC0};

const BAYC: &str = "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D";
const START: u64 = 12_287_507;

fn fixture() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bayc-scaffold")
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
}

fn nuthatch(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(args)
        .env("DO_NOT_TRACK", "1")
        .stdin(Stdio::null())
        .output()
        .expect("run nuthatch");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "nuthatch {args:?} failed:\n{text}");
    text
}

/// What graph-ts's `concatI32` gives, written out from its definition rather than from the view.
fn graph_id(tx_hash: &str, log_index: u64) -> String {
    format!("{tx_hash}{}", hex::encode((log_index as i32).to_le_bytes()))
}

fn word(hex: &str) -> String {
    format!("0x{:0>64}", hex.trim_start_matches("0x"))
}

/// An ERC-721 `Transfer`: all three arguments indexed, no data.
fn nft_transfer(
    block: u64,
    log_index: u64,
    tx_hash: &str,
    from: &str,
    to: &str,
    token: u64,
) -> Log {
    Log {
        address: BAYC.to_string(),
        topics: vec![
            TRANSFER_TOPIC0.to_string(),
            word(from),
            word(to),
            word(&format!("{token:x}")),
        ],
        data: "0x".into(),
        block_number: block,
        block_hash: block_hash(block, 0),
        tx_hash: tx_hash.to_string(),
        log_index,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stock_graph_init_scaffold_answers_its_bytes_ids() {
    let tmp = tempfile::tempdir().unwrap();
    let sg = tmp.path().join("sg");
    let nest = tmp.path().join("nest");
    copy_dir(&fixture(), &sg);
    let sg_s = sg.to_string_lossy().into_owned();
    let nest_s = nest.to_string_lossy().into_owned();

    nuthatch(&[
        "init",
        "--from-subgraph",
        &format!("{sg_s}/subgraph.yaml"),
        "--dir",
        &nest_s,
        "--ipfs",
        "http://127.0.0.1:1/never/",
    ]);
    let emitted = nuthatch(&["port-emit", "--dir", &sg_s, "--out", &nest_s]);
    assert!(
        emitted.contains("coverage: 27 of 27"),
        "every field of the scaffold, the four ids included, is a pure function of the row:\n{emitted}"
    );

    // Index positions chosen to show the byte order: 258 is `0x0102`, so its four bytes are `02010000`; 70,000 reaches the third byte.
    let a1 = account(1);
    let a2 = account(2);
    let hash_a = "0x1f0b6cbb9e9b0f8f2e1a1d3c5b7a99887766554433221100ffeeddccbbaa9988";
    let hash_b = "0xa3d5c0a1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9";
    let logs = [
        (START, 0, hash_a, 7u64),
        (START + 1, 258, hash_b, 9u64),
        (START + 1, 300, hash_b, 11u64),
        (START + 2, 70_000, hash_a, 13u64),
    ];
    let tape = Arc::new(TapeSource::new());
    for b in START..=START + 3 {
        let mut fx: BlockFixture = empty_block(b, 0, 1_618_000_000 + b);
        fx.logs = logs
            .iter()
            .filter(|(blk, ..)| *blk == b)
            .map(|(blk, li, h, t)| nft_transfer(*blk, *li, h, &a1, &a2, *t))
            .collect();
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(START + 3);
    tape.advance_finalized_to(START + 2);

    let cfg = nuthatch::config::Config::load(&nest).expect("the scaffolded nest loads");
    let rt = indexer::spawn_nest(
        tape.clone(),
        nest.clone(),
        cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let ingest = rt.ingest;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = serve::router(serve::SharedNest::new(rt.state));
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let gql = |q: &str| {
        let client = client.clone();
        let url = format!("{base}/graphql");
        let body = serde_json::json!({ "query": q });
        async move {
            client
                .post(url)
                .json(&body)
                .send()
                .await
                .expect("post")
                .json::<serde_json::Value>()
                .await
                .expect("json")
        }
    };

    let start = std::time::Instant::now();
    let mut body = serde_json::Value::Null;
    while start.elapsed() < Duration::from_secs(30) {
        body = gql("{ transfers(first: 10) { id from to tokenId blockNumber } }").await;
        if body["data"]["transfers"]
            .as_array()
            .is_some_and(|a| a.len() == logs.len())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut got: Vec<serde_json::Value> = body["data"]["transfers"]
        .as_array()
        .unwrap_or_else(|| panic!("transfers must answer: {body}"))
        .clone();
    got.sort_by_key(|r| r["id"].as_str().unwrap_or_default().to_string());
    let mut want: Vec<serde_json::Value> = logs
        .iter()
        .map(|(blk, li, h, t)| {
            serde_json::json!({
                "id": graph_id(h, *li),
                "from": a1,
                "to": a2,
                "tokenId": t.to_string(),
                "blockNumber": blk.to_string(),
            })
        })
        .collect();
    want.sort_by_key(|r| r["id"].as_str().unwrap().to_string());
    assert_eq!(got, want, "{body}");
    assert_eq!(
        graph_id(hash_b, 258),
        format!("{hash_b}02010000"),
        "the formula itself: the i32 is little-endian"
    );

    // The query the issue gives, verbatim, and the id as a key.
    let body = gql("{ transfers(first:2){ id from to tokenId blockNumber } }").await;
    assert_eq!(
        body["data"]["transfers"].as_array().map(Vec::len),
        Some(2),
        "{body}"
    );
    let id = graph_id(hash_b, 300);
    let body = gql(&format!(
        "{{ transfer(id: \"{}\") {{ tokenId }} }}",
        id.to_uppercase().replacen("0X", "0x", 1)
    ))
    .await;
    assert_eq!(body["data"]["transfer"]["tokenId"], "11", "{body}");

    server.abort();
    ingest.abort();
}
