//! RFC-0073 FR7: a writer's restart time to readiness at 1, 10, 100 and 500 mounts.
//!
//! In-process, on one cursor, over the scripted tape. Each mount is its own nest and dataset, indexed
//! cold to block 5, stopped, then restarted three times. Two intervals per restart, both from before
//! `spawn_runtime`: **serving**, when the composed runtime's `/ready` answers 200 (what a supervisor
//! or a load balancer waits for), and **resumed**, when every mount has indexed a block that arrived
//! while it was down (each cursor carried on from its checkpoint). `#[ignore]`d; run explicitly:
//!
//! ```text
//! cargo test --release --test bench_restart_mounts -- --ignored --nocapture
//! NUTHATCH_BENCH_MOUNTS=1,10 cargo test --release --test bench_restart_mounts -- --ignored --nocapture
//! ```

mod common;

use common::tape::*;
use nuthatch::{health::RuntimeHealth, indexer, serve};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower::ServiceExt;

fn counts() -> Vec<usize> {
    std::env::var("NUTHATCH_BENCH_MOUNTS")
        .ok()
        .map(|v| v.split(',').filter_map(|n| n.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1, 10, 100, 500])
}

fn block(b: u64) -> BlockFixture {
    transfers_block(
        b,
        0,
        1_700_000_000 + b,
        USDC,
        &[(account(1).as_str(), account(2).as_str(), 100 * b as u128)],
    )
}

async fn spawn(
    root: &Path,
    n: usize,
    tape: Arc<TapeSource>,
) -> (indexer::ChainCursor, axum::Router) {
    let health = Arc::new(RuntimeHealth::new());
    let nests = (0..n)
        .map(|i| {
            let name = format!("m{i}");
            let dir = root.join("data").join(&name);
            health.register(&name, "arbitrum-one");
            let config = nuthatch::config::Config::load(&dir).unwrap();
            (name, dir, config)
        })
        .collect();
    let cursor = indexer::spawn_runtime(
        tape,
        nests,
        None,
        false,
        1,
        Some(2),
        false,
        None,
        health.clone(),
        false,
    )
    .await
    .expect("spawn_runtime");
    let app = serve::compose_runtime(serde_json::json!({}), cursor.states.clone(), health);
    (cursor, app)
}

fn at(cursor: &indexer::ChainCursor, block: u64) -> bool {
    cursor.states.iter().all(|(_, s)| {
        s.store
            .get_meta("last_block")
            .ok()
            .flatten()
            .and_then(|b| b.parse::<u64>().ok())
            == Some(block)
    })
}

async fn ready(app: &axum::Router) -> bool {
    let req = axum::http::Request::get("/ready")
        .body(axum::body::Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status() == 200
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a benchmark: run explicitly with --ignored --nocapture"]
async fn writer_restart_to_ready_by_mount_count() {
    println!("| mounts | serving (median of 3) | resumed (median of 3) | RSS after |");
    println!("| ---: | ---: | ---: | ---: |");
    for n in counts() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        for i in 0..n {
            let dir = root.join("data").join(format!("m{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            scaffold_nest(&dir, &format!("m{i}"), USDC);
        }
        let tape = Arc::new(TapeSource::new());
        let mut head = 5;
        for b in 1..=head {
            tape.insert_block(b, block(b));
        }
        tape.advance_tip_to(head);

        let (cursor, _) = spawn(root, n, tape.clone()).await;
        assert!(
            wait_until(SEAL_POLL_TIMEOUT, || at(&cursor, head)).await,
            "{n} mounts index cold"
        );
        cursor.shutdown().await.unwrap();

        let (mut serving, mut resumed) = (Vec::new(), Vec::new());
        for _ in 0..3 {
            head += 1;
            tape.insert_block(head, block(head));
            tape.advance_tip_to(head);
            let t0 = Instant::now();
            let (cursor, app) = spawn(root, n, tape.clone()).await;
            while !ready(&app).await {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            serving.push(t0.elapsed());
            let ceiling = Instant::now() + SEAL_POLL_TIMEOUT;
            while !at(&cursor, head) {
                assert!(Instant::now() < ceiling, "{n} mounts never resumed");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            resumed.push(t0.elapsed());
            drop(app);
            cursor.shutdown().await.unwrap();
        }
        println!(
            "| {n} | {:.0} ms | {:.0} ms | {} MB |",
            median(serving).as_secs_f64() * 1e3,
            median(resumed).as_secs_f64() * 1e3,
            nuthatch::metrics::rss_bytes() / (1024 * 1024),
        );
    }
}

/// RFC-0073 FR1: what a follower pays to take a snapshot of a nest with a long sealed history and
/// build its derived state (balances and a chain entity) from it, as it does on joining and after
/// every writer restart. `NUTHATCH_BENCH_BLOCKS` blocks of 100 transfers, sealed every 5 blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a benchmark: run explicitly with --ignored --nocapture"]
async fn follower_snapshot_and_derive_over_a_long_sealed_history() {
    let blocks: u64 = std::env::var("NUTHATCH_BENCH_BLOCKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000);
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("data").join("m0");
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, "m0", USDC);
    std::fs::create_dir_all(dir.join("entities")).unwrap();
    std::fs::write(
        dir.join("entities").join("received.sql"),
        "SELECT t.to, SUM(t.value) FROM m0__transfer t GROUP BY t.to",
    )
    .unwrap();
    std::fs::write(
        dir.join("entities.toml"),
        "[[entities]]\nname = \"received\"\nsql = \"entities/received.sql\"\nkey = [\"to\"]\nmax_rows = 10000000\n",
    )
    .unwrap();
    let tape = Arc::new(TapeSource::new());
    let accounts: Vec<String> = (0..200).map(account).collect();
    for b in 1..=blocks {
        let transfers: Vec<(&str, &str, u128)> = (0..100u64)
            .map(|i| {
                let from = &accounts[((b * 7 + i) % 200) as usize];
                let to = &accounts[((b * 13 + i * 3) % 200) as usize];
                (from.as_str(), to.as_str(), (b * 100 + i) as u128)
            })
            .collect();
        tape.insert_block(
            b,
            transfers_block(b, 0, 1_700_000_000 + b, USDC, &transfers),
        );
    }
    tape.advance_tip_to(blocks);

    let t = Instant::now();
    let (cursor, _) = spawn(tmp.path(), 1, tape.clone()).await;
    assert!(
        wait_until(Duration::from_secs(600), || at(&cursor, blocks)).await,
        "indexes cold"
    );
    cursor.shutdown().await.unwrap();
    let indexed = t.elapsed();
    let t = Instant::now();
    let mut through = 0;
    while through + 5 < blocks {
        through += 5;
        force_seal_through(&dir, through);
    }
    let sealed = t.elapsed();
    let segments = nuthatch::seal::load_manifest(&dir)
        .unwrap()
        .tables
        .values()
        .map(Vec::len)
        .sum::<usize>();

    let config = nuthatch::config::Config::load(&dir).unwrap();
    let t = Instant::now();
    let store = nuthatch::store::Store::open(&dir.join("nuthatch.redb")).unwrap();
    let sub = nuthatch::store::feed_subscribe(&store).unwrap();
    let wire = serde_json::to_vec(&nuthatch::feed::Line::Snapshot {
        dataset: nuthatch::feed::DatasetName("data/m0".into()),
        snapshot: sub.snapshot,
    })
    .unwrap();
    let mut snapshot = None;
    nuthatch::feed::read_lines(&wire[..], Box::new(|_, _| None), |line| {
        if let nuthatch::feed::Line::Snapshot { snapshot: s, .. } = line {
            snapshot = Some(s);
        }
        false
    })
    .unwrap();
    let copy = std::sync::Arc::new(nuthatch::feed::MemStore::from_snapshot(
        &dir,
        snapshot.unwrap(),
    ));
    let taken = t.elapsed();
    let t = Instant::now();
    let derived = nuthatch::seal::within(&dir, copy.clone(), || {
        indexer::FollowerDerived::build(&dir, &config, &copy)
    })
    .unwrap();
    let built = t.elapsed();
    assert!(!derived.entities[0].is_empty());
    assert!(derived.balances.holders() > 0);
    println!(
        "{blocks} blocks, {} rows, {segments} segments (indexed {:.1} s, sealed {:.1} s): \
         snapshot taken, sent and applied {:.0} ms ({} wire bytes), derived state built {:.0} ms",
        blocks * 100,
        indexed.as_secs_f64(),
        sealed.as_secs_f64(),
        taken.as_secs_f64() * 1e3,
        wire.len(),
        built.as_secs_f64() * 1e3,
    );
}
