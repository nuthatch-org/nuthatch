//! The live benchmark against the public dataset and a real RPC. Ignored in CI.

use super::*;
use std::time::Instant;

/// Not run in CI (it needs the network and an Alchemy key in `ALCHEMY_API_KEY`):
/// `ALCHEMY_API_KEY=... XATU_BENCH_FROM=26300000 XATU_BENCH_BLOCKS=5000 cargo test --release
/// --lib xatu_bench -- --ignored --nocapture`. Backfills USDC's logs and timestamps the way the
/// indexer's windows do (adaptive window, timestamps for log-bearing blocks, a checkpoint per
/// window) once through the RPC alone and once through the dataset, and prints wall time,
/// HTTP requests to the RPC, and whether the two produced the same bytes.
#[tokio::test]
#[ignore]
async fn xatu_bench() {
    use crate::chunker::AdaptiveWindow;
    use crate::rpc::RpcClient;
    let key = std::env::var("ALCHEMY_API_KEY").expect("ALCHEMY_API_KEY");
    let from: u64 = std::env::var("XATU_BENCH_FROM").unwrap().parse().unwrap();
    let n: u64 = std::env::var("XATU_BENCH_BLOCKS").unwrap().parse().unwrap();
    let f = filter(&[USDC]);

    async fn run(src: Arc<dyn Source>, f: &LogFilter, from: u64, n: u64) -> (usize, usize, String) {
        use std::hash::{Hash, Hasher};
        let mut w = AdaptiveWindow::for_window(20);
        let (mut next, end) = (from, from + n);
        let (mut total, mut windows) = (0, 0);
        let mut h = std::collections::hash_map::DefaultHasher::new();
        while next < end {
            let to = (next + w.window() - 1).min(end - 1);
            let logs = src.logs(f, next, to).await.unwrap();
            w.observed(logs.len() as u64);
            let mut blocks: Vec<u64> = logs.iter().map(|l| l.block_number).collect();
            blocks.dedup();
            let ts = src.block_timestamps(&blocks).await.unwrap();
            let (hash, _) = src.block_record(to).await.unwrap().unwrap();
            for l in &logs {
                serde_json::to_string(l).unwrap().hash(&mut h);
                ts.get(&l.block_number).hash(&mut h);
            }
            hash.hash(&mut h);
            total += logs.len();
            windows += 1;
            next = to + 1;
        }
        (total, windows, format!("{:016x}", h.finish()))
    }

    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let url = format!("https://eth-mainnet.g.alchemy.com/v2/{key}");
    let skip_rpc = std::env::var("XATU_BENCH_SKIP_RPC").is_ok();
    let rpc = Arc::new(RpcClient::new(vec![url.clone()]).unwrap());
    let t = Instant::now();
    let a = if skip_rpc {
        (0, 0, String::new())
    } else {
        run(rpc.clone(), &f, from, n).await
    };
    println!(
        "rpc   : {:.1}s, {} logs, {} windows, {} http requests, digest {}",
        t.elapsed().as_secs_f32(),
        a.0,
        a.1,
        rpc.request_count(),
        a.2
    );

    let rpc = Arc::new(RpcClient::new(vec![url]).unwrap());
    let x = Arc::new(XatuSource::new(
        rpc.clone(),
        Arc::new(HttpFetch::new().unwrap()),
        DEFAULT_BASE_URL,
    ));
    let t = Instant::now();
    let b = run(x.clone(), &f, from, n).await;
    println!(
        "xatu  : {:.1}s, {} logs, {} windows, {} http requests, digest {} ({} chunks, {} MB downloaded, {} fallbacks, {} anchor calls, {} spot checks, {} mismatches)",
        t.elapsed().as_secs_f32(), b.0, b.1, rpc.request_count(), b.2,
        x.stats.chunks_loaded.load(Ordering::SeqCst), x.stats.bytes.load(Ordering::SeqCst) / 1_000_000,
        x.stats.fallbacks.load(Ordering::SeqCst), x.stats.anchor_calls.load(Ordering::SeqCst),
        x.stats.spot_checks.load(Ordering::SeqCst), x.stats.mismatches.load(Ordering::SeqCst)
    );
    println!("identical output: {}", a.0 == b.0 && a.2 == b.2);
}
