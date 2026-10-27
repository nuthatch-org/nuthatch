//! RFC-0073 FR1 and FR7: a read-only follower beside a runtime's writer answers what the writer
//! answers, through a reorg, a seal, a second follower joining and the writer restarting.
//!
//! The writer runs on its own tokio runtime so that stopping it ends every task it spawned, the
//! feed connection included, as a process exit would; its redb is then free for the restarted one.
//! The follower reaches the runtime through a symlink: the catalogue it pins is keyed by path, and
//! in one process the writer must go on reading its own.

mod common;

use common::tape::*;
use nuthatch::{feed_http, health::RuntimeHealth, indexer, reader::Follower, serve};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const TOKEN: &str = "feed-token";
const ROUTE: &str = "usdc";
const ROWS: &str = "SELECT block_number, log_index, value_dec FROM usdc__transfer \
                    WHERE value_dec > 0 ORDER BY block_number, log_index";
const COUNT: &str = "SELECT count(*) AS n FROM usdc__transfer";

struct Writer {
    rt: tokio::runtime::Runtime,
    addr: SocketAddr,
    store: Arc<dyn nuthatch::store::HotStore>,
}

impl Writer {
    fn start(root: &Path, tape: Arc<TapeSource>, port: u16) -> Writer {
        let root = root.to_path_buf();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let (addr, store) = rt.block_on(async {
                let dir = root.join("data").join(ROUTE);
                let config = nuthatch::config::Config::load(&dir).unwrap();
                let health = Arc::new(RuntimeHealth::new());
                health.register(ROUTE, "arbitrum-one");
                let cursor = indexer::spawn_runtime(
                    tape,
                    vec![(ROUTE.to_string(), dir, config)],
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
                let states = cursor.states.clone();
                let store = states[0].1.store.clone();
                let roster: feed_http::Roster = Arc::new(move || {
                    let states = states.clone();
                    Box::pin(async move {
                        states
                            .iter()
                            .map(|(route, s)| feed_http::Served {
                                route: route.clone(),
                                dir: s.dir.clone(),
                                nid: s.nid.as_deref().map(str::to_string),
                                surface: (*s.surface).clone(),
                                store: s.store.clone(),
                            })
                            .collect()
                    })
                });
                let app =
                    serve::compose_runtime(serde_json::json!({}), cursor.states.clone(), health)
                        .merge(feed_http::routes(
                            roster,
                            root.clone(),
                            true,
                            Some(TOKEN.to_string()),
                        ));
                let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
                    .await
                    .unwrap();
                let addr = listener.local_addr().unwrap();
                tokio::spawn(async move { axum::serve(listener, app).await });
                tokio::spawn(async move {
                    let _held = cursor;
                    std::future::pending::<()>().await
                });
                (addr, store)
            });
            Writer { rt, addr, store }
        })
        .join()
        .unwrap()
    }

    async fn reaches(&self, block: u64) {
        let reached = wait_until(SEAL_POLL_TIMEOUT, || {
            self.store
                .get_meta("last_block")
                .ok()
                .flatten()
                .and_then(|b| b.parse::<u64>().ok())
                == Some(block)
        })
        .await;
        assert!(reached, "the writer indexes to {block}");
    }

    fn generation(&self) -> u64 {
        self.store.feed_position().unwrap().1
    }

    /// Stop every task, as a process exit would, and wait for redb to be free.
    fn stop(self, root: &Path) {
        let Writer { rt, store, .. } = self;
        drop(store);
        rt.shutdown_background();
        let db = root.join("data").join(ROUTE).join("nuthatch.redb");
        for _ in 0..400 {
            if nuthatch::store::store_holds_rows(&db).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("the stopped writer never released its store");
    }
}

async fn get(base: &str, path: &str) -> (u16, serde_json::Value) {
    let resp = reqwest::get(format!("{base}{path}")).await.unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(serde_json::Value::Null))
}

async fn sql(base: &str, q: &str) -> (u16, serde_json::Value) {
    let resp = reqwest::Client::new()
        .get(format!("{base}/{ROUTE}/sql"))
        .query(&[("q", q)])
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(serde_json::Value::Null))
}

/// What must match between writer and follower: the rows and the provenance they carry.
fn answer(v: &serde_json::Value) -> serde_json::Value {
    let p = &v["provenance"];
    serde_json::json!({
        "rows": v["rows"],
        "as_of": p["as_of"],
        "sealed_through": p["sealed_through"],
        "nid": p["nid"],
        "registry_hash": p["registry_hash"],
    })
}

/// Wait until `follower` has applied the writer's generation and both answer `q` identically.
async fn agree(writer: &Writer, follower: &str, q: &str, why: &str) -> serde_json::Value {
    let base = format!("http://{}", writer.addr);
    let mut last = (serde_json::Value::Null, serde_json::Value::Null);
    for _ in 0..600 {
        let (_, ready) = get(follower, &format!("/{ROUTE}/ready")).await;
        let applied = ready["follower"]["applied_generation"].as_u64();
        if applied == Some(writer.generation()) {
            let (ws, w) = sql(&base, q).await;
            let (fs, f) = sql(follower, q).await;
            assert_eq!((ws, fs), (200, 200), "{why}: {w} / {f}");
            if answer(&w) == answer(&f) && writer.generation() == applied.unwrap() {
                return answer(&f);
            }
            last = (answer(&w), answer(&f));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{why}: the follower never agreed with the writer: {last:?}");
}

async fn bind_follower(root: &Path, writer: &Writer) -> (Arc<Follower>, String) {
    let follower = Follower::start(
        root.to_path_buf(),
        format!("http://{}", writer.addr),
        TOKEN.to_string(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = follower.service();
    tokio::spawn(async move { axum::serve(listener, app).await });
    (follower, format!("http://{addr}"))
}

fn block(b: u64, variant: u64, value: u128) -> BlockFixture {
    transfers_block(
        b,
        variant,
        1_700_000_000 + b,
        USDC,
        &[(account(1).as_str(), account(2).as_str(), value)],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_answers_as_the_writer_does_through_reorg_seal_and_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    let via: PathBuf = tmp.path().join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();

    let tape = Arc::new(TapeSource::new());
    for b in 1..=8u64 {
        let mut fx = block(b, 0, 100 * b as u128);
        if b == 2 {
            pad_address_to_seal(&mut fx, USDC, 1);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(8);

    let writer = Writer::start(&root, tape.clone(), 0);
    let wbase = format!("http://{}", writer.addr);
    writer.reaches(8).await;

    // The feed is the admin surface's: no token, no feed.
    let refused = reqwest::get(format!("{wbase}/_admin/feed")).await.unwrap();
    assert_eq!(refused.status().as_u16(), 401);

    let (_a, fa) = bind_follower(&via, &writer).await;
    let first = agree(&writer, &fa, ROWS, "after the snapshot").await;
    assert_eq!(first["rows"].as_array().unwrap().len(), 8);
    assert_eq!(first["as_of"], 8);
    agree(&writer, &fa, COUNT, "counting").await;

    // A reorg is a hot-store write like any other.
    tape.reorg(
        6,
        vec![block(7, 1, 7_777), block(8, 1, 8_888), block(9, 1, 9_999)],
    );
    writer.reaches(9).await;
    let after_reorg = agree(&writer, &fa, ROWS, "after a reorg").await;
    let rows = after_reorg["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 9);
    assert!(rows
        .iter()
        .any(|r| r["value_dec"].to_string().contains("7777")));
    assert_eq!(after_reorg["as_of"], 9);

    // A seal moves rows from hot to a segment; the follower moves with it in one delta.
    tape.advance_finalized_to(2);
    tape.insert_block(10, block(10, 0, 1_000));
    tape.advance_tip_to(10);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || writer.store.sealed_through() >= 2).await,
        "the writer seals"
    );
    writer.reaches(10).await;
    let sealed = agree(&writer, &fa, ROWS, "across a seal").await;
    assert_eq!(sealed["sealed_through"], writer.store.sealed_through());
    assert_eq!(sealed["rows"].as_array().unwrap().len(), 10);
    agree(&writer, &fa, COUNT, "counting across a seal").await;

    // A second follower joins a running writer: no restart, no re-index.
    let (_b, fb) = bind_follower(&via, &writer).await;
    let joined = agree(&writer, &fb, ROWS, "a follower added mid-run").await;
    assert_eq!(
        joined,
        agree(&writer, &fa, ROWS, "the first follower").await
    );

    // The writer stops. Followers keep answering from what they hold, and call the writer absent.
    let held = sealed.clone();
    let port = writer.addr.port();
    writer.stop(&root);
    let mut absent = false;
    for _ in 0..200 {
        let (status, ready) = get(&fa, &format!("/{ROUTE}/ready")).await;
        assert_eq!(status, 200, "a follower stays ready without its writer");
        if ready["follower"]["writer"] == "absent" {
            absent = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(absent, "the writer's absence is reported");
    let (status, during) = sql(&fa, ROWS).await;
    assert_eq!(status, 200);
    assert_eq!(answer(&during)["rows"], held["rows"]);

    // A restarted writer is a new epoch: the follower resnapshots and follows it.
    tape.insert_block(11, block(11, 0, 1_100));
    tape.advance_tip_to(11);
    let writer = Writer::start(&root, tape.clone(), port);
    writer.reaches(11).await;
    let resumed = agree(&writer, &fa, ROWS, "after the writer restarted").await;
    assert_eq!(resumed["as_of"], 11);
    assert_eq!(resumed["rows"].as_array().unwrap().len(), 11);
    agree(&writer, &fb, ROWS, "the second follower after the restart").await;
    writer.stop(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_route_is_catching_up_until_its_snapshot_and_derived_state_is_the_writers() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    let via: PathBuf = tmp.path().join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();
    let tape = Arc::new(TapeSource::new());
    tape.insert_block(1, block(1, 0, 5));
    tape.advance_tip_to(1);

    // Nobody listening at the writer's address: nothing has been snapshotted.
    let (_f, base) = {
        let follower = Follower::start(
            via.clone(),
            "http://127.0.0.1:9".to_string(),
            TOKEN.to_string(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = follower.service();
        tokio::spawn(async move { axum::serve(listener, app).await });
        (follower, format!("http://{addr}"))
    };
    let (status, ready) = get(&base, "/ready").await;
    assert_eq!(
        status, 200,
        "nothing mounted is nothing to wait for: {ready}"
    );
    assert_eq!(ready["writer"], "absent");

    let writer = Writer::start(&root, tape.clone(), 0);
    let (_g, fbase) = bind_follower(&via, &writer).await;
    let mut caught = false;
    for _ in 0..400 {
        let (status, body) = get(&fbase, &format!("/{ROUTE}/tables")).await;
        if status == 503 {
            assert_eq!(body["reason"], "catching_up", "{body}");
        } else {
            assert_eq!(status, 200);
            caught = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(caught, "the route is served once its snapshot is applied");
    for path in ["/balances", "/flags", "/derived"] {
        let (status, body) = get(&fbase, &format!("/{ROUTE}{path}")).await;
        assert_eq!(status, 503, "{path}");
        assert_eq!(body["reason"], "writer_only", "{path}");
    }
    let (status, root_ready) = get(&fbase, "/ready").await;
    assert_eq!(status, 200, "{root_ready}");
    assert_eq!(root_ready["nests"][ROUTE]["ready"], true);
    writer.stop(&root);
}
