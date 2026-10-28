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
        Writer::start_many(root, tape, port, &[ROUTE])
    }

    /// One cursor over every nest named, each at `data/<route>`; `store` is the first one's.
    fn start_many(root: &Path, tape: Arc<TapeSource>, port: u16, routes: &[&str]) -> Writer {
        let root = root.to_path_buf();
        let routes: Vec<String> = routes.iter().map(|r| r.to_string()).collect();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let (addr, store) = rt.block_on(async {
                let health = Arc::new(RuntimeHealth::new());
                let nests = routes
                    .iter()
                    .map(|route| {
                        let dir = root.join("data").join(route);
                        let config = nuthatch::config::Config::load(&dir).unwrap();
                        health.register(route, "arbitrum-one");
                        (route.clone(), dir, config)
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
                let mut cursor = cursor;
                // `[runtime] isolation = "strict"` as the runtime stamps it: each route its own tenant.
                if root.join("strict").exists() {
                    for (route, s) in &mut cursor.states {
                        s.isolation = Some(Arc::from(route.as_str()));
                    }
                }
                let states = cursor.states.clone();
                let store = states[0].1.store.clone();
                let roster: feed_http::Roster = Arc::new(move || {
                    let states = states.clone();
                    Box::pin(async move {
                        states
                            .iter()
                            .map(|(route, s)| feed_http::Served::of(route, s, &Default::default()))
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

    // Asked for, the cost report (RFC-0073 FR4) comes back from a follower as from the writer.
    for base in [&wbase, &fa] {
        let resp = reqwest::Client::new()
            .get(format!("{base}/{ROUTE}/sql"))
            .query(&[("q", COUNT)])
            .header(nuthatch::cost::HEADER, "1")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let cost = resp.headers().get(nuthatch::cost::HEADER);
        assert!(
            cost.is_some_and(|c| c.to_str().unwrap().contains("hot_rows")),
            "{base}: {cost:?}"
        );
    }

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

    // Nobody listening at the writer's address: it cannot say what exists, so nothing is ready and
    // an unheard-of route is catching up rather than absent.
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
    assert_eq!(status, 503, "{ready}");
    assert_eq!(ready["writer"], "absent");
    assert_eq!(ready["mounts_known"], false);
    let (status, body) = get(&base, &format!("/{ROUTE}/tables")).await;
    assert_eq!((status, &body["reason"]), (503, &"catching_up".into()));

    let writer = Writer::start(&root, tape.clone(), 0);
    let (_g, fbase) = bind_follower(&via, &writer).await;
    let mut caught = false;
    for _ in 0..400 {
        let (status, body) = get(&fbase, &format!("/{ROUTE}/tables")).await;
        if status == 503 {
            assert_eq!(body["reason"], "catching_up", "{body}");
        } else {
            assert_eq!(status, 200, "{body}");
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

/// A strict runtime's writer isolates each mount's queries to its tenant, and the follower takes
/// the partition from the feed and does the same: an off-list function is refused on both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_strict_writers_followers_isolate_as_it_does() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    std::fs::write(root.join("strict"), "").unwrap();
    let via: PathBuf = tmp.path().join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();
    let tape = Arc::new(TapeSource::new());
    tape.insert_block(1, block(1, 0, 5));
    tape.advance_tip_to(1);
    let writer = Writer::start(&root, tape.clone(), 0);
    writer.reaches(1).await;
    let (_f, fbase) = bind_follower(&via, &writer).await;
    agree(
        &writer,
        &fbase,
        COUNT,
        "a strict follower counts as its writer",
    )
    .await;
    for base in [format!("http://{}", writer.addr), fbase.clone()] {
        let (status, body) = sql(&base, "SELECT version()").await;
        assert_eq!(
            status, 400,
            "{base}: an off-list function under strict: {body}"
        );
    }
    writer.stop(&root);
}

async fn route_status(base: &str, route: &str) -> (u16, serde_json::Value) {
    get(base, &format!("/{route}/tables")).await
}

/// A follower charges each dataset's hot copy, and its nest's admission estimate, to its cursor's
/// `max_rss_mb`. A snapshot that would not fit is refused by name while its co-mount serves, and a
/// mount that grows past the budget through its deltas is let go the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_over_the_cursor_budget_is_refused_and_its_co_mount_serves() {
    const SMALL: &str = "arb";
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    std::fs::create_dir_all(&root).unwrap();
    // 12 MiB: each nest is charged 5 by admission, and the padded one's hot rows come to about 14.
    std::fs::write(
        root.join(nuthatch::runtime::MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nmax_rss_mb = 12\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
         chain_id = 42161\nrpc_urls = []\n",
    )
    .unwrap();
    for (route, address) in [(ROUTE, USDC), (SMALL, ARB)] {
        let dir = root.join("data").join(route);
        std::fs::create_dir_all(&dir).unwrap();
        scaffold_nest(&dir, route, address);
    }
    let via: PathBuf = tmp.path().join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();

    let tape = Arc::new(TapeSource::new());
    let mut padded = block(1, 0, 100);
    pad_address_to_seal(&mut padded, USDC, 0);
    padded.logs.push(transfer_log(
        ARB,
        1,
        padded.logs.len() as u64,
        &padded.hash.clone(),
        &account(3),
        &account(4),
        7,
    ));
    tape.insert_block(1, padded);
    tape.advance_tip_to(1);
    let writer = Writer::start_many(&root, tape.clone(), 0, &[ROUTE, SMALL]);
    writer.reaches(1).await;

    let (_f, fbase) = bind_follower(&via, &writer).await;
    let mut seen = (0, serde_json::Value::Null);
    for _ in 0..400 {
        seen = route_status(&fbase, SMALL).await;
        if seen.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(seen.0, 200, "the co-mount serves: {}", seen.1);
    let small_sql = "SELECT count(*) AS n FROM arb__transfer";
    let wbase = format!("http://{}", writer.addr);
    let resp = |base: String| async move {
        reqwest::Client::new()
            .get(format!("{base}/{SMALL}/sql"))
            .query(&[("q", small_sql)])
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()["rows"]
            .clone()
    };
    assert_eq!(resp(fbase.clone()).await, resp(wbase).await);

    let mut big = route_status(&fbase, ROUTE).await;
    for _ in 0..400 {
        if big.1["reason"] != "catching_up" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        big = route_status(&fbase, ROUTE).await;
    }
    let (status, body) = big;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["reason"], "over_budget", "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("max_rss_mb 12"),
        "{body}"
    );
    let (status, ready) = get(&fbase, &format!("/{ROUTE}/ready")).await;
    assert_eq!((status, &ready["reason"]), (503, &"over_budget".into()));
    let (status, root_ready) = get(&fbase, "/ready").await;
    assert_eq!(status, 503, "{root_ready}");
    assert_eq!(root_ready["nests"][ROUTE]["reason"], "over_budget");
    assert_eq!(root_ready["nests"][SMALL]["ready"], true);

    // The co-mount's own deltas take it past what its cursor has left: it is let go by name.
    let mut grown = block(2, 0, 1);
    pad_address_to_seal(&mut grown, ARB, 0);
    tape.insert_block(2, grown);
    tape.advance_tip_to(2);
    writer.reaches(2).await;
    let mut refused = serde_json::Value::Null;
    for _ in 0..400 {
        let (status, body) = route_status(&fbase, SMALL).await;
        if status == 503 {
            refused = body;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(refused["reason"], "over_budget", "{refused}");
    writer.stop(&root);
}

const SMALL: &str = "arb";

/// Two small nests on one cursor whose budget is 12 MiB, block 1 carrying a transfer for each.
fn two_mounts(tmp: &Path) -> (PathBuf, PathBuf, Arc<TapeSource>) {
    let root = tmp.join("runtime");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join(nuthatch::runtime::MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nmax_rss_mb = 12\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
         chain_id = 42161\nrpc_urls = []\n",
    )
    .unwrap();
    for (route, address) in [(ROUTE, USDC), (SMALL, ARB)] {
        let dir = root.join("data").join(route);
        std::fs::create_dir_all(&dir).unwrap();
        scaffold_nest(&dir, route, address);
    }
    let via: PathBuf = tmp.join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();
    let tape = Arc::new(TapeSource::new());
    tape.insert_block(1, both(1));
    tape.advance_tip_to(1);
    (root, via, tape)
}

/// A block with one transfer on each of the two nests.
fn both(b: u64) -> BlockFixture {
    let mut fx = block(b, 0, b as u128);
    let at = fx.logs.len() as u64;
    let hash = fx.hash.clone();
    fx.logs.push(transfer_log(
        ARB,
        b,
        at,
        &hash,
        &account(3),
        &account(4),
        b as u128,
    ));
    fx
}

/// Wait for `route` to be refused `over_budget`, and return its body.
async fn refused_by_budget(fbase: &str, route: &str) -> serde_json::Value {
    let mut seen = (0, serde_json::Value::Null);
    for _ in 0..800 {
        seen = route_status(fbase, route).await;
        if seen.1["reason"] == "over_budget" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(seen.0, 503, "{}", seen.1);
    assert_eq!(seen.1["reason"], "over_budget", "{}", seen.1);
    seen.1
}

/// The co-mount follows the writer to block `b`, which it has never seen, and stays ready.
async fn co_mount_follows(writer: &Writer, tape: &TapeSource, fbase: &str, b: u64) {
    tape.insert_block(b, both(b));
    tape.advance_tip_to(b);
    writer.reaches(b).await;
    let q = "SELECT count(*) AS n FROM arb__transfer";
    let mut followed = serde_json::Value::Null;
    for _ in 0..800 {
        let resp = reqwest::Client::new()
            .get(format!("{fbase}/{SMALL}/sql"))
            .query(&[("q", q)])
            .send()
            .await
            .unwrap();
        followed = resp.json().await.unwrap_or_default();
        if followed["rows"][0]["n"] == b {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(followed["rows"][0]["n"], b, "{followed}");
    let (status, ready) = get(fbase, "/ready").await;
    assert_eq!(status, 503, "{ready}");
    assert_eq!(ready["nests"][SMALL]["ready"], true, "{ready}");
}

/// One stored value larger than its cursor's whole budget: the follower stops reading that snapshot
/// at its byte limit rather than allocating the value, refuses the mount by name, reconnects without
/// it, and its co-mount goes on following the writer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_value_over_the_budget_is_refused_without_being_read() {
    let tmp = tempfile::tempdir().unwrap();
    let (root, via, tape) = two_mounts(tmp.path());
    let writer = Writer::start_many(&root, tape.clone(), 0, &[ROUTE, SMALL]);
    writer.reaches(1).await;
    // 32 MiB in one meta value, against a cursor budget of 12.
    writer
        .store
        .set_meta("ballast", &"x".repeat(32 << 20))
        .unwrap();
    let (_f, fbase) = bind_follower(&via, &writer).await;
    let body = refused_by_budget(&fbase, ROUTE).await;
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("snapshot passed")),
        "refused at the byte limit, not after reading the value: {body}"
    );
    co_mount_follows(&writer, &tape, &fbase, 2).await;
    writer.stop(&root);
}

/// A held mount's delta that would take it past what its cursor has left is refused before any of
/// it is applied: by its peak, worked out from the decoded delta, or before that by its wire bytes.
/// The co-mount goes on following.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delta_over_the_budget_is_refused_before_it_is_applied() {
    for (size, refused_by) in [
        (3 << 20, "refused before it was applied"),
        (32 << 20, "delta passed"),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let (root, via, tape) = two_mounts(tmp.path());
        let writer = Writer::start_many(&root, tape.clone(), 0, &[ROUTE, SMALL]);
        writer.reaches(1).await;
        let (_f, fbase) = bind_follower(&via, &writer).await;
        let mut held = 0;
        for _ in 0..800 {
            held = route_status(&fbase, ROUTE).await.0;
            if held == 200 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(held, 200, "the mount is held before the delta");
        // One row of `size` bytes: about 2 MiB is left, so 3 MiB passes the wire limit (twice that) but
        // not the charge; 32 MiB passes neither.
        let row = serde_json::json!({"table": "blob", "block_number": 1, "pad": "x".repeat(size)});
        writer
            .store
            .put_entity("000000000001-999999", &row.to_string())
            .unwrap();
        let body = refused_by_budget(&fbase, ROUTE).await;
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|e| e.contains(refused_by)),
            "{size}: {body}"
        );
        co_mount_follows(&writer, &tape, &fbase, 2).await;
        writer.stop(&root);
    }
}
