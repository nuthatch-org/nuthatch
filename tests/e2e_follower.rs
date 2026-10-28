//! RFC-0073 FR1 and FR7: a read-only follower beside a runtime's writer answers what the writer
//! answers, through a reorg, a seal, a second follower joining and the writer restarting.
//!
//! The writer runs on its own tokio runtime so that stopping it ends every task it spawned, the
//! feed connection included, as a process exit would; its redb is then free for the restarted one.
//! The follower reaches the runtime through a symlink: the catalogue it pins is keyed by path, and
//! in one process the writer must go on reading its own.

mod common;
#[path = "common/entity_fixture.rs"]
mod entity_fixture;

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
                // Named queries the mount serves, kept outside the dataset so its NID is untouched.
                if let Ok(raw) = std::fs::read_to_string(root.join("named.json")) {
                    let queries: Vec<nuthatch::allowlist::NamedQuery> =
                        serde_json::from_str(&raw).unwrap();
                    for (_, s) in &mut cursor.states {
                        s.surface = Arc::new(nuthatch::allowlist::Surface {
                            access: nuthatch::allowlist::SqlAccess::Open,
                            queries: queries.clone(),
                        });
                    }
                }
                let states = cursor.states.clone();
                let store = states[0].1.store.clone();
                let marks = root.clone();
                // A route is unmounted while `unmounted-<route>` exists beside the runtime.
                let roster: feed_http::Roster = Arc::new(move || {
                    let states = states.clone();
                    let marks = marks.clone();
                    Box::pin(async move {
                        states
                            .iter()
                            .filter(|(route, _)| !marks.join(format!("unmounted-{route}")).exists())
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
        if !reached {
            let (_, ready) = get(&format!("http://{}", self.addr), "/ready").await;
            panic!("the writer never indexed to {block}: {ready}");
        }
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
        "entities": p["entities"],
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

/// Wait until `follower` has applied the writer's generation and both answer GET `path` with the
/// same status and body.
async fn agree_on(writer: &Writer, follower: &str, path: &str, why: &str) -> serde_json::Value {
    let base = format!("http://{}", writer.addr);
    let path = format!("/{ROUTE}{path}");
    let mut last = (serde_json::Value::Null, serde_json::Value::Null);
    for _ in 0..600 {
        let (_, ready) = get(follower, &format!("/{ROUTE}/ready")).await;
        let applied = ready["follower"]["applied_generation"].as_u64();
        if applied == Some(writer.generation()) {
            // Whether the memo answered is the process's business, not the answer's.
            let uncached = |(s, mut v): (u16, serde_json::Value)| {
                if let Some(o) = v.as_object_mut() {
                    o.remove("cached");
                }
                (s, v)
            };
            let w = uncached(get(&base, &path).await);
            let f = uncached(get(follower, &path).await);
            assert_eq!(w.0, 200, "{why}: the writer on {path}: {}", w.1);
            if w == f && writer.generation() == applied.unwrap() {
                return f.1;
            }
            last = (w.1, f.1);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{why}: {path} never agreed: {last:?}");
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

/// A snapshot the parser reads before its `mount` line has been applied is still read under its
/// dataset's cap: the follower applies nothing until the whole feed has arrived, and the padded
/// nest is still refused, never admitted for want of a cursor to charge it to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_read_ahead_of_its_mount_is_still_capped() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    // 12 MiB: the nest is charged 5 by admission, and its hot rows come to about 14.
    std::fs::write(
        root.join(nuthatch::runtime::MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nmax_rss_mb = 12\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
         chain_id = 42161\nrpc_urls = []\n",
    )
    .unwrap();
    let via: PathBuf = tmp.path().join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();
    let tape = Arc::new(TapeSource::new());
    let mut padded = block(1, 0, 100);
    pad_address_to_seal(&mut padded, USDC, 0);
    tape.insert_block(1, padded);
    tape.advance_tip_to(1);
    let writer = Writer::start(&root, tape.clone(), 0);
    writer.reaches(1).await;

    let follower = Follower::start_paused(
        via.clone(),
        format!("http://{}", writer.addr),
        TOKEN.to_string(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fbase = format!("http://{}", listener.local_addr().unwrap());
    let app = follower.service();
    tokio::spawn(async move { axum::serve(listener, app).await });
    // Time for a parser that does not wait on the handler to read the whole snapshot ahead of it.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    follower.pause(false);
    let mut seen = (0, serde_json::Value::Null);
    for _ in 0..400 {
        seen = route_status(&fbase, ROUTE).await;
        if seen.0 == 503 && seen.1["reason"] == "over_budget" {
            break;
        }
        assert_ne!(seen.0, 200, "admitted past its budget: {}", seen.1);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(seen.1["reason"], "over_budget", "{}", seen.1);
    writer.stop(&root);
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
        assert_eq!(status, 200, "{path}: {body}");
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
    // The co-mount's own store, not the one `reaches` watches, has to have block 1 on both sides.
    let mut counts = (serde_json::Value::Null, serde_json::Value::Null);
    for _ in 0..400 {
        counts = (resp(wbase.clone()).await, resp(fbase.clone()).await);
        if counts.0[0]["n"] == 1 && counts.0 == counts.1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(counts.0[0]["n"], 1, "the writer's co-mount has block 1");
    assert_eq!(
        counts.1, counts.0,
        "the co-mount answers as the writer does"
    );

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
    // Over the whole budget, not short of room a retiring copy held: it is not asked for again.
    for _ in 0..100 {
        let (status, body) = route_status(&fbase, ROUTE).await;
        assert_eq!(
            (status, &body["reason"]),
            (503, &"over_budget".into()),
            "{body}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
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
        ((2 << 20) + (8 << 10), "refused before it was applied"),
        (3 << 20, "delta passed"),
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
        // One row of `size` bytes. A little under 2 MiB is left (12, less 5 charged to each nest and
        // their few rows), and the wire limit is that plus 64 KiB: 2 MiB and 8 KiB is read and
        // refused by its charge, 3 MiB is stopped on the wire.
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

/// A writer restart resnapshots every dataset, and the replacement and its derived state are charged
/// beside the copy and derived state they replace, which are still served. With room for both, the
/// swap is seamless. Without it, the old ones are released first, the routes answer `catching_up`
/// while the new ones load, and the cursor is never charged past its budget either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resnapshot_is_charged_beside_the_copy_it_replaces() {
    const BUDGET_MB: u64 = 14;
    // The nest's derived state is charged 5 MiB by admission: two of it and two 1 MiB copies fit,
    // two of it and two 3 MiB copies do not, and one of it and a 5 MiB copy does. At 3 MiB the old
    // 3 + 5 and a replacement of 3 leave no room for the replacement's own 5 while it builds, so the
    // old ones go first.
    for (size, reloads) in [(1 << 20, 0), (3 << 20, 1), (5 << 20, 1)] {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("runtime");
        let dir = root.join("data").join(ROUTE);
        std::fs::create_dir_all(&dir).unwrap();
        scaffold_nest(&dir, ROUTE, USDC);
        std::fs::write(
            root.join(nuthatch::runtime::MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\nmax_rss_mb = {BUDGET_MB}\n\n[[chains]]\nchain = \
                 \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n"
            ),
        )
        .unwrap();
        let via: PathBuf = tmp.path().join("follower-view");
        std::os::unix::fs::symlink(&root, &via).unwrap();
        let tape = Arc::new(TapeSource::new());
        tape.insert_block(1, block(1, 0, 5));
        tape.advance_tip_to(1);
        let writer = Writer::start(&root, tape.clone(), 0);
        writer.reaches(1).await;
        let row = serde_json::json!({"table": "blob", "block_number": 1, "pad": "x".repeat(size)});
        writer
            .store
            .put_entity("000000000001-999999", &row.to_string())
            .unwrap();
        let (follower, fbase) = bind_follower(&via, &writer).await;
        agree(&writer, &fbase, COUNT, "before the restart").await;

        let port = writer.addr.port();
        writer.stop(&root);
        let mut absent = false;
        for _ in 0..400 {
            let (_, ready) = get(&fbase, &format!("/{ROUTE}/ready")).await;
            if ready["follower"]["writer"] == "absent" {
                absent = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(absent, "the follower saw its writer go");
        let writer = Writer::start(&root, tape.clone(), port);
        let (mut caught_up, mut answered) = (false, false);
        for _ in 0..2000 {
            let (status, body) = route_status(&fbase, ROUTE).await;
            match status {
                200 => answered = true,
                503 => {
                    assert_eq!(body["reason"], "catching_up", "{size}: {body}");
                    caught_up = true;
                }
                other => panic!("{size}: {other} {body}"),
            }
            let (_, ready) = get(&fbase, &format!("/{ROUTE}/ready")).await;
            if status == 200
                && ready["follower"]["writer"] == "connected"
                && ready["follower"]["lag_generations"] == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(answered, "{size}: served after the restart");
        let (peak, released) = follower.charges();
        assert_eq!(released, reloads, "{size}: copies released to make room");
        // The release's `catching_up` window is as short as a reconnect, and polling can miss it
        // on a loaded machine: the count of releases above is what shows which path was taken.
        if reloads == 0 {
            assert!(!caught_up, "{size}: served throughout a swap with room");
        }
        assert!(
            peak <= BUDGET_MB << 20,
            "{size}: charged {peak} bytes against a {BUDGET_MB} MiB budget"
        );
        agree(&writer, &fbase, COUNT, "after the restart").await;
        writer.stop(&root);
    }
}

/// An unmounted copy a request still holds stays charged, so remounting its nest cannot load a new
/// copy beside it past the budget: 4 MiB held and 4 MiB more do not fit what admission leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unmounted_copy_in_use_is_charged_until_it_drops() {
    const BUDGET_MB: u64 = 12;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    std::fs::write(
        root.join(nuthatch::runtime::MOUNTS_FILE),
        format!(
            "[runtime]\nname = \"r\"\nmax_rss_mb = {BUDGET_MB}\n\n[[chains]]\nchain = \
             \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n"
        ),
    )
    .unwrap();
    let via: PathBuf = tmp.path().join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();
    let tape = Arc::new(TapeSource::new());
    tape.insert_block(1, block(1, 0, 5));
    tape.advance_tip_to(1);
    let writer = Writer::start(&root, tape.clone(), 0);
    writer.reaches(1).await;
    let row = serde_json::json!({"table": "blob", "block_number": 1, "pad": "x".repeat(4 << 20)});
    writer
        .store
        .put_entity("000000000001-999999", &row.to_string())
        .unwrap();
    let (follower, fbase) = bind_follower(&via, &writer).await;
    agree(&writer, &fbase, COUNT, "before the unmount").await;

    let dataset = format!("data/{ROUTE}");
    let held = follower
        .hold(&dataset)
        .expect("the follower holds its copy");
    let mark = root.join(format!("unmounted-{ROUTE}"));
    std::fs::write(&mark, "").unwrap();
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || follower.hold(&dataset).is_none()).await,
        "the follower let the unmounted copy go"
    );
    assert!(
        follower.retiring().1 >= 4 << 20,
        "the copy still held is charged: {}",
        follower.retiring().1
    );

    std::fs::remove_file(&mark).unwrap();
    let mut remounted = serde_json::Value::Null;
    for _ in 0..400 {
        let (status, body) = route_status(&fbase, ROUTE).await;
        if status == 503 && body["reason"] != "catching_up" {
            remounted = body;
            break;
        }
        assert_ne!(status, 200, "a second copy was loaded beside the held one");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(remounted["reason"], "over_budget", "{remounted}");
    let (peak, _) = follower.charges();
    assert!(
        peak <= BUDGET_MB << 20,
        "charged {peak} bytes against a {BUDGET_MB} MiB budget"
    );
    drop(held);
    assert_eq!(follower.retiring().1, 0, "nothing is charged once it drops");
    // Refused only for want of the room the held copy took, it comes back on its own.
    let mut back = (0, serde_json::Value::Null);
    for _ in 0..400 {
        back = route_status(&fbase, ROUTE).await;
        if back.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        back.0, 200,
        "the remount came back once there was room: {}",
        back.1
    );
    agree(&writer, &fbase, COUNT, "after the remount").await;
    let (peak, _) = follower.charges();
    assert!(
        peak <= BUDGET_MB << 20,
        "charged {peak} bytes against a {BUDGET_MB} MiB budget"
    );
    writer.stop(&root);
}

/// A writer at blocks 1 to 6, with no seal of its own (nothing finalises), and a follower.
async fn sealing_by_hand(tmp: &Path) -> (PathBuf, Writer, Arc<Follower>, String) {
    let root = tmp.join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    let via: PathBuf = tmp.join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=6u64 {
        tape.insert_block(b, block(b, 0, 100 * b as u128));
    }
    tape.advance_tip_to(6);
    let writer = Writer::start(&root, tape, 0);
    writer.reaches(6).await;
    let (follower, fbase) = bind_follower(&via, &writer).await;
    agree(&writer, &fbase, COUNT, "before any seal").await;
    (root, writer, follower, fbase)
}

/// Seal `from..=to` as the writer's seal does: segment and catalogue, then the commit that prunes
/// hot and raises the watermark.
fn seal(root: &Path, writer: &Writer, from: u64, to: u64) {
    let dir = root.join("data").join(ROUTE);
    let rows = writer.store.entities_in_range(from, to).unwrap();
    nuthatch::seal::seal_range(&dir, &rows, from, to).unwrap();
    writer
        .store
        .prune_and_set_meta(from, to, "sealed_through", &to.to_string())
        .unwrap();
}

/// The segment the catalogue names first, and its file.
fn first_segment(root: &Path) -> (String, PathBuf) {
    let dir = root.join("data").join(ROUTE);
    let manifest = nuthatch::seal::load_manifest(&dir).unwrap();
    let s = manifest.tables.values().next().unwrap()[0].clone();
    let path = nuthatch::seal::segment_path(&dir, &s.file, &s.hash);
    (s.hash, path)
}

/// A segment a follower's catalogue names, gone from disk, is never answered around: its queries
/// are `503 catching_up` until it has a catalogue that does not name it, never a short count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_named_segment_gone_from_disk_is_catching_up_never_a_short_answer() {
    let tmp = tempfile::tempdir().unwrap();
    let (root, writer, _follower, fbase) = sealing_by_hand(tmp.path()).await;
    seal(&root, &writer, 1, 2);
    agree(&writer, &fbase, COUNT, "across the seal").await;
    let (_, path) = first_segment(&root);
    std::fs::remove_file(&path).unwrap();
    let mut refused = 0;
    for _ in 0..40 {
        let (status, body) = sql(&fbase, COUNT).await;
        match status {
            503 => {
                assert_eq!(body["reason"], "catching_up", "{body}");
                refused += 1;
            }
            200 => assert_eq!(body["rows"][0]["n"], 6, "a short answer: {body}"),
            other => panic!("{other}: {body}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(refused > 0, "the missing segment was noticed");
    writer.stop(&root);
}

/// Each follower resnapshots on what its own requests found: one follower's finding a named segment
/// missing is not taken, or lost, by another follower of the same dataset in the process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_segment_resnapshots_the_follower_that_found_it() {
    let tmp = tempfile::tempdir().unwrap();
    let (root, writer, a, abase) = sealing_by_hand(tmp.path()).await;
    let (b, bbase) = bind_follower(&tmp.path().join("follower-view"), &writer).await;
    seal(&root, &writer, 1, 2);
    agree(&writer, &abase, COUNT, "a, across the seal").await;
    agree(&writer, &bbase, COUNT, "b, across the seal").await;
    let (_, path) = first_segment(&root);
    std::fs::remove_file(&path).unwrap();
    let (a0, b0) = (a.connects(), b.connects());

    let (status, body) = sql(&abase, COUNT).await;
    assert_eq!((status, &body["reason"]), (503, &"catching_up".into()));
    assert!(
        wait_until(Duration::from_secs(5), || a.connects() > a0).await,
        "the follower that found it resnapshots"
    );
    // Two of b's heartbeats: neither takes a's finding, and b has found nothing of its own.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(b.connects(), b0, "b resnapshots only on its own finding");

    let (status, body) = sql(&bbase, COUNT).await;
    assert_eq!((status, &body["reason"]), (503, &"catching_up".into()));
    assert!(
        wait_until(Duration::from_secs(5), || b.connects() > b0).await,
        "b resnapshots on its own finding"
    );
    writer.stop(&root);
}

/// A segment a fold replaces is kept from reclaim while a follower's copy may still name it: the
/// follower, held at the old generation, answers it exactly, and the file goes only once that
/// follower has applied the commit that stopped naming it and the grace has run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_folded_segment_is_kept_until_its_followers_have_moved_on() {
    const GRACE: Duration = Duration::from_millis(300);
    let tmp = tempfile::tempdir().unwrap();
    let (root, writer, follower, fbase) = sealing_by_hand(tmp.path()).await;
    let dir = root.join("data").join(ROUTE);
    nuthatch::seal::set_follower_retention(&dir, GRACE, Duration::from_secs(60));
    seal(&root, &writer, 1, 2);
    let old = agree(&writer, &fbase, COUNT, "across the first seal").await;
    let (hash, path) = first_segment(&root);
    let store = root.join(nuthatch::seal::SEGMENTS_DIR);

    follower.pause(true);
    // The next seal folds blocks 1 and 2's provisional segment into one of 1 to 4.
    seal(&root, &writer, 3, 4);
    assert_ne!(first_segment(&root).0, hash, "the fold replaced it");
    let kept = || {
        !nuthatch::seal::remove_segment_if_unheld(&path).unwrap()
            && path.exists()
            && nuthatch::seal::ledger_retained(&store).contains(&hash)
    };
    assert!(kept(), "kept from reclaim while the follower may name it");
    let (status, held) = sql(&fbase, COUNT).await;
    assert_eq!(status, 200, "{held}");
    assert_eq!(held["rows"], old["rows"], "the old generation, exactly");
    tokio::time::sleep(GRACE * 3).await;
    assert!(
        kept(),
        "past the grace, still kept: the follower has not moved on"
    );

    follower.pause(false);
    agree(&writer, &fbase, COUNT, "after the follower moved on").await;
    let released = wait_until(Duration::from_secs(10), || {
        nuthatch::seal::remove_segment_if_unheld(&path).unwrap()
    })
    .await;
    assert!(released, "reclaimed once the follower moved on");
    assert!(!path.exists());
    assert!(!nuthatch::seal::ledger_retained(&store).contains(&hash));
    writer.stop(&root);
}

const ENTITY_SQL: &str = "SELECT * FROM received ORDER BY 1";

/// Drop a one-row `tiers` snapshot giving account 2 `tier`, from a file outside the dataset.
fn drop_tier(root: &Path, name: &str, tier: &str) {
    let csv = root.join(format!("{name}.csv"));
    std::fs::write(&csv, format!("account,tier\n{},{tier}\n", account(2))).unwrap();
    nuthatch::offchain::drop_file(&root.join("data").join(ROUTE), &csv, "tiers").unwrap();
}

/// A runtime with one nest carrying every kind of derived state: threshold and velocity flags, a
/// labelled address for exposure, a chain entity and an entity joining an offchain table, with a
/// named query over the first. Blocks 1..=8 on the tape, block 2 padded so its range seals.
fn derived_runtime(tmp: &Path) -> (PathBuf, PathBuf, Arc<TapeSource>) {
    let root = tmp.join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    let toml = dir.join("nuthatch.toml");
    let cfg = std::fs::read_to_string(&toml).unwrap();
    std::fs::write(
        &toml,
        format!(
            "{cfg}\n[flags]\nthreshold = \"150\"\nvelocity_amount = \"300\"\nvelocity_window = 100\n"
        ),
    )
    .unwrap();
    std::fs::create_dir_all(dir.join("labels")).unwrap();
    std::fs::write(
        dir.join("labels").join("fixture.json"),
        serde_json::json!([{"address": account(2), "label": "mixer"}]).to_string(),
    )
    .unwrap();
    entity_fixture::write(
        &dir,
        "[[entities]]\nname = \"received\"\nquery = \"SELECT t.to, SUM(t.value) FROM usdc__transfer t GROUP BY t.to\"\nkey = [\"to\"]\nmax_rows = 50000\n\n\
         [[entities]]\nname = \"by_tier\"\nquery = \"SELECT t.tier, SUM(x.value) FROM usdc__transfer x JOIN offchain__tiers t ON x.to = t.account GROUP BY t.tier\"\nkey = [\"tier\"]\nmax_rows = 50000\n",
    )
    .unwrap();
    drop_tier(&root, "first", "gold");
    std::fs::write(
        root.join("named.json"),
        serde_json::json!([{"name": "top_received", "sql": ENTITY_SQL}]).to_string(),
    )
    .unwrap();
    let via: PathBuf = tmp.join("follower-view");
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
    (root, via, tape)
}

/// `/flags` reads a block's threshold flags from its hot rows and its velocity from a view fed after
/// them. Every transfer here is 1,000 from one account, so a whole state always has a velocity
/// volume of 1,000 per threshold flag: a reader that caught a delta between its rows and its views
/// would see more flags than volume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_sees_each_delta_wholly_or_not_at_all() {
    const LAST: u64 = 24;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    let toml = dir.join("nuthatch.toml");
    let cfg = std::fs::read_to_string(&toml).unwrap();
    std::fs::write(
        &toml,
        format!(
            "{cfg}\n[flags]\nthreshold = \"150\"\nvelocity_amount = \"300\"\nvelocity_window = 100\n"
        ),
    )
    .unwrap();
    let via: PathBuf = tmp.path().join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=3 {
        tape.insert_block(b, block(b, 0, 1_000));
    }
    tape.advance_tip_to(3);
    let writer = Writer::start(&root, tape.clone(), 0);
    writer.reaches(3).await;
    let (_f, fa) = bind_follower(&via, &writer).await;
    agree_on(&writer, &fa, "/flags", "before the stream").await;
    // Each delta's views are fed 50 ms after its rows land in the copy: a reader let in between
    // would see the flags without the volume.
    nuthatch::indexer::slow_follower_feed(&via.join("data").join(ROUTE), Duration::from_millis(50));

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let (stop, fa) = (stop.clone(), fa.clone());
        tokio::spawn(async move {
            let mut seen = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let (status, body) = get(&fa, &format!("/{ROUTE}/flags")).await;
                if status != 200 {
                    continue;
                }
                let flags = body["threshold"]["flags"].as_array().map_or(0, Vec::len) as u64;
                let volume: u64 = body["velocity"]["flags"][0]["volume"]
                    .as_str()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                seen.push((flags, volume));
            }
            seen
        })
    };
    // One block at a time, so each is a delta of its own.
    for b in 4..=LAST {
        tape.insert_block(b, block(b, 0, 1_000));
        tape.advance_tip_to(b);
        writer.reaches(b).await;
    }
    agree_on(&writer, &fa, "/flags", "after the stream").await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let seen = reader.await.unwrap();
    let torn: Vec<_> = seen.iter().filter(|(n, v)| *v != 1_000 * n).collect();
    assert!(torn.is_empty(), "read part-way through a delta: {torn:?}");
    let states: std::collections::BTreeSet<u64> = seen.iter().map(|(n, _)| *n).collect();
    assert!(
        states.len() > 10,
        "the reader saw the stream move: {states:?}"
    );
    writer.stop(&root);
}

fn as_u64(v: &serde_json::Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str()?.parse().ok())
}

/// The writer's side of #2467. A window's velocity is fed before its commit, its threshold flags land
/// with the commit and its entity is fed after. Every transfer is 1,000, so a whole state has a
/// velocity volume of 1,000 per flag and an entity sum of 1,000 per row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_request_sees_each_window_wholly_or_not_at_all() {
    const LAST: u64 = 16;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    let toml = dir.join("nuthatch.toml");
    let cfg = std::fs::read_to_string(&toml).unwrap();
    std::fs::write(
        &toml,
        format!(
            "{cfg}\n[flags]\nthreshold = \"150\"\nvelocity_amount = \"300\"\nvelocity_window = 100\n"
        ),
    )
    .unwrap();
    entity_fixture::write(
        &dir,
        "[[entities]]\nname = \"received_whole\"\nquery = \"SELECT t.to, SUM(CAST(t.value AS HUGEINT)) AS sum_value FROM usdc__transfer t GROUP BY t.to\"\nkey = [\"to\"]\nmax_rows = 50000\n",
    )
    .unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=3 {
        tape.insert_block(b, block(b, 0, 1_000));
    }
    tape.advance_tip_to(3);
    let writer = Writer::start(&root, tape.clone(), 0);
    writer.reaches(3).await;
    let base = format!("http://{}", writer.addr);
    // Each window waits 50 ms after its views are fed and again after its commit, and the entity 30 ms
    // before it folds: a request let in at any of those points sees one part of the window without
    // another.
    nuthatch::indexer::slow_writer_commit(&dir, Duration::from_millis(50));
    nuthatch::entity_view::slow_entity_fold("received_whole", Duration::from_millis(30));

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let (stop, base) = (stop.clone(), base.clone());
        tokio::spawn(async move {
            let (mut flags_seen, mut sql_seen) = (Vec::new(), Vec::new());
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let (status, body) = get(&base, &format!("/{ROUTE}/flags")).await;
                if status == 200 {
                    let flags = body["threshold"]["flags"].as_array().map_or(0, Vec::len) as u64;
                    let volume = as_u64(&body["velocity"]["flags"][0]["volume"]).unwrap_or(0);
                    flags_seen.push((flags, volume));
                }
                let (status, body) = sql(
                    &base,
                    "SELECT (SELECT count(*) FROM usdc__transfer) AS n, \
                     (SELECT sum_value FROM received_whole) AS s",
                )
                .await;
                assert_eq!(status, 200, "{body}");
                let row = &body["rows"][0];
                sql_seen.push((as_u64(&row["n"]).unwrap(), as_u64(&row["s"]).unwrap_or(0)));
            }
            (flags_seen, sql_seen)
        })
    };
    // One block at a time, so each is a window of its own.
    for b in 4..=LAST {
        tape.insert_block(b, block(b, 0, 1_000));
        tape.advance_tip_to(b);
        writer.reaches(b).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let (flags_seen, sql_seen) = reader.await.unwrap();
    let torn: Vec<_> = flags_seen.iter().filter(|(n, v)| *v != 1_000 * n).collect();
    assert!(
        torn.is_empty(),
        "/flags read part-way through a window: {torn:?}"
    );
    let torn: Vec<_> = sql_seen.iter().filter(|(n, s)| *s != 1_000 * n).collect();
    assert!(
        torn.is_empty(),
        "/sql read part-way through a window: {torn:?}"
    );
    for (what, states) in [
        (
            "/flags",
            flags_seen
                .iter()
                .map(|(n, _)| *n)
                .collect::<std::collections::BTreeSet<_>>(),
        ),
        ("/sql", sql_seen.iter().map(|(n, _)| *n).collect()),
    ] {
        assert!(states.len() > 8, "{what} saw the stream move: {states:?}");
    }
    writer.stop(&root);
}

/// Every route a follower answers from derived state, compared with the writer's answer.
async fn derived_agree(writer: &Writer, follower: &str, why: &str) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    for path in [
        "/balances".to_string(),
        format!("/balance/{}", account(2)),
        format!("/exposure/{}", account(1)),
        "/flags".to_string(),
        "/flags?kind=velocity".to_string(),
        "/derived".to_string(),
        "/derived/received".to_string(),
        "/q/top_received".to_string(),
        "/derived/by_tier".to_string(),
    ] {
        out.push(agree_on(writer, follower, &path, &format!("{why}, {path}")).await);
    }
    out.push(agree(writer, follower, ENTITY_SQL, &format!("{why}, entity SQL")).await);
    out
}

/// Balances, exposure, velocity and threshold flags, an authored entity by `/derived`, by `/sql` and
/// through a named query: the follower answers each as the writer does, through a reorg, a seal and
/// the writer restarting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_derives_what_the_writer_derives() {
    let tmp = tempfile::tempdir().unwrap();
    let (root, via, tape) = derived_runtime(tmp.path());
    let writer = Writer::start(&root, tape.clone(), 0);
    writer.reaches(8).await;

    let (_a, fa) = bind_follower(&via, &writer).await;
    let first = derived_agree(&writer, &fa, "after the snapshot").await;
    assert!(
        first[0]["holders"].as_u64().unwrap_or(0) >= 2,
        "balances are populated: {}",
        first[0]
    );
    assert_eq!(
        first[1]["balance"],
        (100..=800).step_by(100).sum::<u64>().to_string()
    );
    assert!(
        first[3]["threshold"]["flags"]
            .as_array()
            .is_some_and(|f| !f.is_empty()),
        "threshold flags: {}",
        first[3]
    );
    assert!(
        first[4]["flags"].as_array().is_some_and(|f| !f.is_empty()),
        "velocity flags: {}",
        first[4]
    );
    assert!(
        first[6].to_string().contains("3600"),
        "the entity holds account 2's total: {}",
        first[6]
    );

    tape.reorg(
        6,
        vec![block(7, 1, 7_777), block(8, 1, 8_888), block(9, 1, 9_999)],
    );
    writer.reaches(9).await;
    let reorged = derived_agree(&writer, &fa, "after a reorg").await;
    let total = 100 + 200 + 300 + 400 + 500 + 600 + 7_777 + 8_888 + 9_999;
    assert_eq!(reorged[1]["balance"], total.to_string());
    assert!(reorged[8].to_string().contains("gold"), "{}", reorged[8]);

    // A second offchain snapshot, dropped while both run, reaches both at the next window.
    drop_tier(&root, "second", "silver");
    tape.advance_finalized_to(2);
    tape.insert_block(10, block(10, 0, 1_000));
    tape.advance_tip_to(10);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || writer.store.sealed_through() >= 2).await,
        "the writer seals"
    );
    writer.reaches(10).await;
    let sealed = derived_agree(&writer, &fa, "across a seal").await;
    assert_eq!(sealed[1]["balance"], (total + 1_000).to_string());
    assert!(sealed[8].to_string().contains("silver"), "{}", sealed[8]);

    let port = writer.addr.port();
    writer.stop(&root);
    let (status, held) = get(&fa, &format!("/{ROUTE}/balance/{}", account(2))).await;
    assert_eq!(status, 200);
    assert_eq!(
        held, sealed[1],
        "derived state is served while the writer is away"
    );

    tape.insert_block(11, block(11, 0, 1_100));
    tape.advance_tip_to(11);
    let writer = Writer::start(&root, tape.clone(), port);
    writer.reaches(11).await;
    let resumed = derived_agree(&writer, &fa, "after the writer restarted").await;
    assert_eq!(resumed[1]["balance"], (total + 2_100).to_string());
    writer.stop(&root);
}

/// The file of the transfer table's first sealed segment.
fn transfer_segment(root: &Path) -> PathBuf {
    let dir = root.join("data").join(ROUTE);
    let manifest = nuthatch::seal::load_manifest(&dir).unwrap();
    let s = manifest.tables["usdc__transfer"][0].clone();
    nuthatch::seal::segment_path(&dir, &s.file, &s.hash)
}

/// Derived state built from sealed history its copy's catalogue names, with a segment of it gone, is
/// not built from what remains: its routes answer `catching_up`, never a short balance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn derived_state_is_never_built_from_a_short_sealed_history() {
    let tmp = tempfile::tempdir().unwrap();
    let (root, via, tape) = derived_runtime(tmp.path());
    let writer = Writer::start(&root, tape.clone(), 0);
    writer.reaches(8).await;
    tape.advance_finalized_to(2);
    tape.insert_block(9, block(9, 0, 900));
    tape.advance_tip_to(9);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || writer.store.sealed_through() >= 2).await,
        "the writer seals"
    );
    writer.reaches(9).await;
    let (follower, fa) = bind_follower(&via, &writer).await;
    let whole = derived_agree(&writer, &fa, "before the segment goes").await;

    let path = transfer_segment(&root);
    std::fs::remove_file(&path).unwrap();
    let before = follower.connects();
    let (status, body) = sql(&fa, COUNT).await;
    assert_eq!((status, &body["reason"]), (503, &"catching_up".into()));
    assert!(
        wait_until(Duration::from_secs(10), || follower.connects() > before).await,
        "the follower takes a fresh snapshot"
    );
    let mut rebuilding = false;
    for _ in 0..200 {
        let (status, body) = get(&fa, &format!("/{ROUTE}/balances")).await;
        match status {
            200 => assert_eq!(body, whole[0], "a balance from a short history"),
            503 => {
                assert_eq!(body["reason"], "catching_up", "{body}");
                rebuilding = true;
                break;
            }
            other => panic!("{other}: {body}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        rebuilding,
        "the rebuild over a missing segment is catching up"
    );
    let (_, ready) = get(&fa, &format!("/{ROUTE}/ready")).await;
    assert_eq!(ready["derived"]["state"], "catching_up", "{ready}");
    writer.stop(&root);
}

/// Derived state a feed tore part-way is not served: its routes answer `catching_up`, and `/ready`
/// says why, until it is rebuilt from the sealed history, while the nest's hot and sealed routes
/// keep answering. Once rebuilt it answers as the writer does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn torn_derived_state_is_not_served_until_rebuilt() {
    let tmp = tempfile::tempdir().unwrap();
    let (root, via, tape) = derived_runtime(tmp.path());
    let writer = Writer::start(&root, tape.clone(), 0);
    writer.reaches(8).await;
    let (follower, fa) = bind_follower(&via, &writer).await;
    derived_agree(&writer, &fa, "before the fault").await;

    // The next window's feed fails after the views and before the entities, and the rebuild that
    // follows waits for `release`.
    let (release, held) = std::sync::mpsc::channel();
    nuthatch::indexer::inject_follower_fault(&via.join("data").join(ROUTE), held);
    tape.insert_block(9, block(9, 0, 900));
    tape.advance_tip_to(9);
    writer.reaches(9).await;
    // The store has the window: the hot route shows block 9.
    let nine = "SELECT count(*) AS n FROM usdc__transfer WHERE block_number = 9";
    let mut stored = false;
    for _ in 0..400 {
        let (status, body) = sql(&fa, nine).await;
        let (_, ready) = get(&fa, &format!("/{ROUTE}/ready")).await;
        // Both, so the window is past being applied and the feed has been found torn.
        if status == 200 && body["rows"][0]["n"] == 1 && ready["derived"]["state"] == "catching_up"
        {
            stored = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        stored,
        "the follower applied block 9 to its hot copy and found the feed torn"
    );
    for path in [
        "/derived/received".to_string(),
        "/derived/by_tier".to_string(),
        "/balances".to_string(),
        format!("/balance/{}", account(2)),
        "/flags".to_string(),
        "/q/top_received".to_string(),
    ] {
        let (status, body) = get(&fa, &format!("/{ROUTE}{path}")).await;
        assert_eq!(status, 503, "{path} served torn state: {body}");
        assert_eq!(body["reason"], "catching_up", "{path}: {body}");
    }
    let (status, body) = sql(&fa, ENTITY_SQL).await;
    assert_eq!((status, &body["reason"]), (503, &"catching_up".into()));
    let (status, ready) = get(&fa, &format!("/{ROUTE}/ready")).await;
    assert_eq!(status, 200, "{ready}");
    assert_eq!(ready["derived"]["state"], "catching_up", "{ready}");
    assert!(
        ready["derived"]["reason"]
            .as_str()
            .is_some_and(|r| r.contains("part-way")),
        "{ready}"
    );
    // The torn state was released, not kept beside the rebuild, before the rebuild began.
    let mut released = false;
    for _ in 0..200 {
        if follower.retiring() == (1, 0) {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (retired, held) = follower.retiring();
    assert!(
        released,
        "{retired} states retired, {held} bytes of them still held during the rebuild"
    );

    release.send(()).unwrap();
    let rebuilt = derived_agree(&writer, &fa, "after the rebuild").await;
    assert_eq!(
        rebuilt[1]["balance"],
        ((100..=800).step_by(100).sum::<u64>() + 900).to_string()
    );
    let (_, ready) = get(&fa, &format!("/{ROUTE}/ready")).await;
    assert_eq!(ready["derived"]["state"], "ready", "{ready}");
    writer.stop(&root);
}
