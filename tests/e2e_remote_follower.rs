//! RFC-0076 FR17: a follower on another host, with no path shared with the writer, answers what a
//! follower beside the writer answers, route by route, with the same `as_of`, `sealed_through` and
//! `nid`. Its nest comes from a registry by NID, its segments from the writer's mirror, each file
//! checked against the hash that names it.
//!
//! A seal is applied whole only once every file its catalogue names is fetched and verified: a slow
//! mirror, one missing a file and one serving the wrong bytes each leave the follower answering the
//! generation before the seal, whole, never a short answer.
//!
//! The mirror is a directory the test publishes to with `nuthatch::publish::sync`, the pass the
//! running publisher makes, served to the follower over HTTP by a small server the test can slow,
//! or have hide or corrupt a file. `a_remote_follower_over_s3_answers_as_a_local_one_does` runs the
//! same comparison against a real S3 server, gated like `e2e_minio_publish`.

mod common;

use common::tape::*;
use nuthatch::reader::{Follower, Options};
use nuthatch::{feed_http, health::RuntimeHealth, indexer, serve};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const TOKEN: &str = "remote-feed-token";
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
    fn start(root: &Path, tape: Arc<TapeSource>, nid: &str) -> Writer {
        let root = root.to_path_buf();
        let nid = nid.to_string();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let (addr, store) = rt.block_on(async {
                let health = Arc::new(RuntimeHealth::new());
                let dir = root.join("data").join(ROUTE);
                let config = nuthatch::config::Config::load(&dir).unwrap();
                health.register(ROUTE, "arbitrum-one");
                let mut cursor = indexer::spawn_runtime(
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
                for (_, s) in &mut cursor.states {
                    s.nid = Some(Arc::from(nid.as_str()));
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
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
        assert!(reached, "the writer never indexed to {block}");
    }

    fn generation(&self) -> u64 {
        self.store.feed_position().unwrap().1
    }

    fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn stop(self) {
        let Writer { rt, store, .. } = self;
        drop(store);
        rt.shutdown_background();
    }
}

/// The writer's mirror directory served over HTTP, slowed, or with files hidden or corrupted.
#[derive(Default)]
struct MirrorKnobs {
    delay_ms: AtomicU64,
    hidden: Mutex<HashSet<String>>,
    corrupt: Mutex<HashSet<String>>,
    served: AtomicU64,
}

async fn serve_mirror(dir: PathBuf, knobs: Arc<MirrorKnobs>) -> String {
    use axum::extract::{Path as Key, State};
    use axum::http::{Method, StatusCode};
    use axum::response::IntoResponse;
    let app = axum::Router::new()
        .route(
            "/{*key}",
            axum::routing::any(
                |State((dir, knobs)): State<(PathBuf, Arc<MirrorKnobs>)>,
                 method: Method,
                 Key(key): Key<String>| async move {
                    let delay = knobs.delay_ms.load(Ordering::SeqCst);
                    if delay > 0 {
                        tokio::time::sleep(Duration::from_millis(delay)).await;
                    }
                    let name = key.rsplit('/').next().unwrap_or("").to_string();
                    if knobs
                        .hidden
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|h| name.starts_with(h))
                    {
                        return StatusCode::NOT_FOUND.into_response();
                    }
                    let Ok(mut bytes) = std::fs::read(dir.join(&key)) else {
                        return StatusCode::NOT_FOUND.into_response();
                    };
                    if knobs
                        .corrupt
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|h| name.starts_with(h))
                    {
                        let last = bytes.len() / 2;
                        bytes[last] ^= 0xff;
                    }
                    knobs.served.fetch_add(1, Ordering::SeqCst);
                    if method == Method::HEAD {
                        return ([("content-length", bytes.len().to_string())], ()).into_response();
                    }
                    bytes.into_response()
                },
            ),
        )
        .with_state((dir, knobs));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

/// A writer's runtime, its registry and mirror, and the follower beside it to compare with.
struct Rig {
    tmp: tempfile::TempDir,
    root: PathBuf,
    nid: String,
    tape: Arc<TapeSource>,
    writer: Writer,
    /// Where the writer's publisher puts the dataset, which the follower reads.
    mirror: String,
    registry: String,
    local: String,
}

fn block(b: u64, value: u128) -> BlockFixture {
    transfers_block(
        b,
        0,
        1_700_000_000 + b,
        USDC,
        &[(account(1).as_str(), account(2).as_str(), value)],
    )
}

/// A writer at blocks 1 to `tip`, its nest published to `registry`, and a follower beside it.
async fn rig(tip: u64, mirror: Option<String>, registry: Option<String>) -> Rig {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("runtime");
    let dir = root.join("data").join(ROUTE);
    std::fs::create_dir_all(&dir).unwrap();
    scaffold_nest(&dir, ROUTE, USDC);
    let nid = nuthatch::blob::nest_nid(&dir).unwrap();
    let registry = registry.unwrap_or_else(|| tmp.path().join("registry").display().to_string());
    let bundle = tmp.path().join("usdc.bundle");
    nuthatch::blob::bundle(&dir, Some(&bundle), false, false).unwrap();
    let store = nuthatch::distribution::open(&registry).unwrap();
    nuthatch::distribution::publish(store.as_ref(), &bundle, None, None)
        .await
        .unwrap();
    let mirror = mirror.unwrap_or_else(|| tmp.path().join("mirror").display().to_string());

    let tape = Arc::new(TapeSource::new());
    for b in 1..=tip {
        tape.insert_block(b, block(b, 100 * b as u128));
    }
    tape.advance_tip_to(tip);
    let writer = Writer::start(&root, tape.clone(), &nid);
    writer.reaches(tip).await;
    // Beside the writer, through a link: in one process the writer must go on reading its own
    // catalogue, which the follower's pin is keyed by path to.
    let via = tmp.path().join("follower-view");
    std::os::unix::fs::symlink(&root, &via).unwrap();
    let local = bind(Follower::start(via, writer.base(), TOKEN.to_string())).await;
    Rig {
        tmp,
        root,
        nid,
        tape,
        writer,
        mirror,
        registry,
        local,
    }
}

impl Rig {
    fn dir(&self) -> PathBuf {
        self.root.join("data").join(ROUTE)
    }

    /// A follower on "another host": its own directory, nothing of the writer's but its URL.
    async fn remote(&self, mirror: &str, feed: u32) -> (Arc<Follower>, String, PathBuf) {
        self.remote_with(mirror, feed, nuthatch::seal::FOLLOWER_GRACE)
            .await
    }

    async fn remote_with(
        &self,
        mirror: &str,
        feed: u32,
        cache_grace: Duration,
    ) -> (Arc<Follower>, String, PathBuf) {
        let cache = self
            .tmp
            .path()
            .join(format!("elsewhere-{feed}-{}", rand_suffix()));
        self.remote_at(mirror, feed, cache_grace, cache).await
    }

    /// A follower over the cache `cache`, which a follower before it may have filled.
    async fn remote_at(
        &self,
        mirror: &str,
        feed: u32,
        cache_grace: Duration,
        cache: PathBuf,
    ) -> (Arc<Follower>, String, PathBuf) {
        std::fs::create_dir_all(&cache).unwrap();
        let mut remote =
            nuthatch::remote_follow::Remote::open(mirror, &self.registry, 2048).unwrap();
        remote.cache_grace = cache_grace;
        let f = Follower::start_remote(
            cache.clone(),
            self.writer.base(),
            TOKEN.to_string(),
            Arc::new(remote),
            Options {
                feed,
                paused: false,
            },
        );
        let base = bind(f.clone()).await;
        (f, base, cache)
    }

    /// The writer's publisher with `followers = true`.
    async fn publish(&self) {
        nuthatch::publish::sync_for_followers(&self.dir(), &self.mirror)
            .await
            .unwrap();
    }

    /// Seal `from..=to` as the writer's seal does: segment and catalogue, then the commit that
    /// prunes hot and raises the watermark.
    fn seal(&self, from: u64, to: u64) {
        let rows = self.writer.store.entities_in_range(from, to).unwrap();
        nuthatch::seal::seal_range(&self.dir(), &rows, from, to).unwrap();
        self.writer
            .store
            .prune_and_set_meta(from, to, "sealed_through", &to.to_string())
            .unwrap();
    }

    /// The segment file the catalogue names for the transfer table at `to`.
    fn segment_hash(&self) -> String {
        let m = nuthatch::seal::load_manifest(&self.dir()).unwrap();
        m.tables
            .values()
            .next()
            .unwrap()
            .last()
            .unwrap()
            .hash
            .clone()
    }
}

fn rand_suffix() -> u64 {
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::SeqCst)
}

async fn bind(f: Arc<Follower>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = f.service();
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
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

/// What must match: the rows and the provenance they carry.
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

async fn applied(base: &str) -> Option<u64> {
    get(base, &format!("/{ROUTE}/ready")).await.1["follower"]["applied_generation"].as_u64()
}

/// The GET routes compared, beyond SQL: everything a follower serves from its copy and catalogue.
const ROUTES: &[&str] = &["/tables", "/entities?limit=50", "/schema"];

/// Wait until both followers have applied the writer's generation, then compare every route.
async fn agree(rig: &Rig, remote: &str, why: &str) -> serde_json::Value {
    let mut last = serde_json::Value::Null;
    for _ in 0..600 {
        let g = rig.writer.generation();
        if applied(remote).await == Some(g) && applied(&rig.local).await == Some(g) {
            let mut same = true;
            let mut out = serde_json::Value::Null;
            for q in [ROWS, COUNT] {
                let (ls, l) = sql(&rig.local, q).await;
                let (rs, r) = sql(remote, q).await;
                assert_eq!((ls, rs), (200, 200), "{why}: {l} / {r}");
                same &= answer(&l) == answer(&r);
                last = serde_json::json!({"local": answer(&l), "remote": answer(&r)});
                if q == ROWS {
                    out = answer(&r);
                }
            }
            for path in ROUTES {
                let path = format!("/{ROUTE}{path}");
                let l = get(&rig.local, &path).await;
                let r = get(remote, &path).await;
                same &= l == r;
                if l != r {
                    last = serde_json::json!({"path": path, "local": l.1, "remote": r.1});
                }
            }
            let (_, lr) = get(&rig.local, &format!("/{ROUTE}/ready")).await;
            let (_, rr) = get(remote, &format!("/{ROUTE}/ready")).await;
            for k in ["as_of", "sealed_through", "ready"] {
                same &= lr[k] == rr[k];
            }
            if same && rig.writer.generation() == g {
                assert_eq!(
                    out["nid"],
                    rig.nid.as_str(),
                    "{why}: the provenance names the nest"
                );
                return out;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{why}: the remote follower never agreed with the local one: {last}");
}

/// Assert the remote follower stays on the generation before the seal for `for_`, answering it
/// whole: `rows` rows, `sealed_through` 0, never a short count, never a 5xx.
async fn holds_before_the_seal(remote: &str, rows: u64, for_: Duration) -> serde_json::Value {
    let deadline = std::time::Instant::now() + for_;
    let mut ready = serde_json::Value::Null;
    while std::time::Instant::now() < deadline {
        let (status, body) = sql(remote, COUNT).await;
        assert_eq!(status, 200, "a held seal is never a refusal: {body}");
        assert_eq!(
            body["rows"][0]["n"], rows,
            "a short answer while a seal waited on the mirror: {body}"
        );
        assert_eq!(body["provenance"]["sealed_through"], 0, "{body}");
        ready = get(remote, &format!("/{ROUTE}/ready")).await.1;
        assert_eq!(ready["ready"], true, "{ready}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ready
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_follower_answers_as_a_local_one_does() {
    let rig = rig(6, None, None).await;
    rig.publish().await;
    let (_f, remote, cache) = rig.remote(&rig.mirror, 2).await;
    let first = agree(&rig, &remote, "after the snapshot").await;
    assert_eq!(first["rows"].as_array().unwrap().len(), 6);
    assert_eq!(first["as_of"], 6);

    // Nothing of the writer's is under the follower's directory: its nest came from the registry.
    assert!(!cache
        .join("data")
        .join(ROUTE)
        .join("nuthatch.redb")
        .exists());
    assert!(cache
        .join("data")
        .join(ROUTE)
        .join("nuthatch.toml")
        .exists());
    assert!(!cache.starts_with(&rig.root) && !rig.root.starts_with(&cache));

    // A seal: the segment comes from the mirror, verified, and the follower moves with it. Made
    // final before it is published, so the mirror has it only where a final segment goes while the
    // delta's catalogue still calls it provisional.
    rig.seal(1, 2);
    assert!(!nuthatch::seal::finalise_provisional(&rig.dir())
        .unwrap()
        .is_empty());
    rig.publish().await;
    let sealed = agree(&rig, &remote, "across a seal").await;
    assert_eq!(sealed["sealed_through"], 2);
    let hash = rig.segment_hash();
    assert!(cache
        .join("segments")
        .join(format!("{hash}.parquet"))
        .exists());

    // Tip deltas, and a second seal that folds the first's provisional segment.
    for b in 7..=8 {
        rig.tape.insert_block(b, block(b, 100 * b as u128));
    }
    rig.tape.advance_tip_to(8);
    rig.writer.reaches(8).await;
    agree(&rig, &remote, "tip deltas").await;
    rig.seal(3, 5);
    rig.publish().await;
    let folded = agree(&rig, &remote, "across a fold").await;
    assert_eq!(folded["sealed_through"], 5);
    assert_eq!(folded["rows"].as_array().unwrap().len(), 8);

    // A v1 follower on another host reads the same.
    let (_g, v1, _) = rig.remote(&rig.mirror, 1).await;
    agree(&rig, &v1, "a v1 remote follower").await;

    // The writer measured how soon each follower could answer each generation, on its own clock.
    let (_, text) = (
        0,
        reqwest::get(format!("{}/metrics", rig.writer.base()))
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
    );
    for kind in ["tip", "seal"] {
        assert!(
            text.lines().any(|l| l.starts_with(&format!(
                "nuthatch_feed_visibility_seconds_count{{dataset=\"data/{ROUTE}\",kind=\"{kind}\"}}"
            )) && !l.ends_with(" 0")),
            "no {kind} visibility measured:\n{}",
            text.lines()
                .filter(|l| l.contains("feed"))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    assert!(text.contains("nuthatch_feed_delta_bytes_bucket{encoding=\"v2\""));
    assert!(text.contains("nuthatch_feed_encode_seconds_bucket{encoding=\"v2\""));
    let follower = reqwest::get(format!("{remote}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        follower.contains("nuthatch_follower_apply_seconds_count"),
        "{follower}"
    );
    assert!(
        follower.contains("nuthatch_follower_mirror_files_total"),
        "{follower}"
    );
    rig.writer.stop();
}

/// A seal whose file a slow mirror takes its time over is applied whole once the file is in, and
/// until then the follower answers the generation before it. Tip deltas behind the seal wait with
/// it and are applied after it, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_waits_for_a_slow_mirror_and_never_answers_short() {
    let rig = rig(6, None, None).await;
    rig.publish().await;
    let knobs = Arc::new(MirrorKnobs::default());
    let served = serve_mirror(PathBuf::from(&rig.mirror), knobs.clone()).await;
    let (_f, remote, _) = rig.remote(&served, 2).await;
    agree(&rig, &remote, "after the snapshot").await;

    knobs.delay_ms.store(15_000, Ordering::SeqCst);
    rig.seal(1, 2);
    rig.publish().await;
    let ready = holds_before_the_seal(&remote, 6, Duration::from_millis(1_000)).await;
    assert!(
        ready["follower"]["mirror"]["waiting_for"]
            .as_array()
            .is_some_and(|w| !w.is_empty()),
        "the wait on the mirror is reported: {ready}"
    );
    rig.tape.insert_block(7, block(7, 700));
    rig.tape.advance_tip_to(7);
    rig.writer.reaches(7).await;
    let mut queued = false;
    for _ in 0..100 {
        let ready = holds_before_the_seal(&remote, 6, Duration::from_millis(50)).await;
        if ready["follower"]["mirror"]["queued"].as_u64() >= Some(2) {
            queued = true;
            break;
        }
    }
    assert!(queued, "the tip delta waits behind the seal");
    knobs.delay_ms.store(0, Ordering::SeqCst);
    let after = agree(&rig, &remote, "once the mirror served it").await;
    assert_eq!(after["sealed_through"], 2);
    assert_eq!(after["as_of"], 7);
    rig.writer.stop();
}

/// A seal naming a file the mirror does not have is never applied: the follower answers the
/// generation before it, says what it waits for, and applies it once the publisher catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_naming_a_file_the_mirror_lacks_is_never_applied() {
    let rig = rig(6, None, None).await;
    rig.publish().await;
    let knobs = Arc::new(MirrorKnobs::default());
    let served = serve_mirror(PathBuf::from(&rig.mirror), knobs.clone()).await;
    let (_f, remote, cache) = rig.remote(&served, 2).await;
    agree(&rig, &remote, "after the snapshot").await;

    // Sealed, and not yet published: the mirror lacks the file the delta's catalogue names.
    rig.seal(1, 2);
    let hash = rig.segment_hash();
    let ready = holds_before_the_seal(&remote, 6, Duration::from_millis(1_500)).await;
    let error = ready["follower"]["mirror"]["error"].as_str().unwrap_or("");
    assert!(error.contains("does not have"), "{ready}");
    assert!(!cache
        .join("segments")
        .join(format!("{hash}.parquet"))
        .exists());

    rig.publish().await;
    let after = agree(&rig, &remote, "once the publisher caught up").await;
    assert_eq!(after["sealed_through"], 2);
    rig.writer.stop();
}

/// A file that does not hash to the name its catalogue gives it is refused and never read: the
/// follower answers the generation before the seal until the mirror serves the right bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_that_does_not_hash_to_its_name_is_refused() {
    let rig = rig(6, None, None).await;
    rig.publish().await;
    let knobs = Arc::new(MirrorKnobs::default());
    let served = serve_mirror(PathBuf::from(&rig.mirror), knobs.clone()).await;
    let (_f, remote, cache) = rig.remote(&served, 2).await;
    agree(&rig, &remote, "after the snapshot").await;

    rig.seal(1, 2);
    let hash = rig.segment_hash();
    knobs.corrupt.lock().unwrap().insert(hash.clone());
    rig.publish().await;
    let ready = holds_before_the_seal(&remote, 6, Duration::from_millis(1_500)).await;
    let error = ready["follower"]["mirror"]["error"].as_str().unwrap_or("");
    assert!(error.contains("did not hash"), "{ready}");
    assert!(
        !cache
            .join("segments")
            .join(format!("{hash}.parquet"))
            .exists(),
        "a refused file is never put where a query reads"
    );

    knobs.corrupt.lock().unwrap().clear();
    let after = agree(&rig, &remote, "once the mirror served the right bytes").await;
    assert_eq!(after["sealed_through"], 2);
    rig.writer.stop();
}

/// A mirror whose publisher has `followers` off never gets the provisional segments a seal names.
/// The follower says so on `/ready`, rather than looking like it waits on a slow mirror.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mirror_not_published_for_followers_says_so() {
    let rig = rig(6, None, None).await;
    nuthatch::publish::sync(&rig.dir(), &rig.mirror, false)
        .await
        .unwrap();
    let (_f, remote, _) = rig.remote(&rig.mirror, 2).await;
    agree(&rig, &remote, "after the snapshot").await;
    rig.seal(1, 2);
    nuthatch::publish::sync(&rig.dir(), &rig.mirror, false)
        .await
        .unwrap();
    let mut said = serde_json::Value::Null;
    for _ in 0..100 {
        said = holds_before_the_seal(&remote, 6, Duration::from_millis(50)).await;
        if said["follower"]["mirror"]["error"]
            .as_str()
            .is_some_and(|e| e.contains("followers = true"))
        {
            break;
        }
    }
    assert!(
        said["follower"]["mirror"]["error"]
            .as_str()
            .is_some_and(|e| e.contains("does not publish for followers")),
        "{said}"
    );
    rig.writer.stop();
}

/// The follower's cache keeps what its copies name and lets go of a folded segment once no copy
/// has named it for the grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_folded_segment_leaves_the_follower_cache_after_the_grace() {
    let rig = rig(6, None, None).await;
    rig.publish().await;
    let (_f, remote, cache) = rig
        .remote_with(&rig.mirror, 2, Duration::from_millis(300))
        .await;
    agree(&rig, &remote, "after the snapshot").await;
    rig.seal(1, 2);
    rig.publish().await;
    agree(&rig, &remote, "across a seal").await;
    let first = rig.segment_hash();
    let cached = |h: &str| cache.join("segments").join(format!("{h}.parquet")).exists();
    assert!(cached(&first));
    // Several sweeps past the grace: a file a copy names stays.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert!(cached(&first), "a segment the copy names was swept");

    rig.seal(3, 5);
    rig.publish().await;
    let folded = agree(&rig, &remote, "across a fold").await;
    let second = rig.segment_hash();
    assert_ne!(first, second, "premise: the fold replaced it");
    assert!(
        wait_until(Duration::from_secs(10), || !cached(&first)).await,
        "the folded segment stayed in the cache"
    );
    assert!(cached(&second));
    assert_eq!(agree(&rig, &remote, "after the sweep").await, folded);
    rig.writer.stop();
}

/// A cached segment is checked against its name before a follower relies on it: rewritten to other
/// bytes of the same length while no follower ran, or while one is running and then takes fresh
/// snapshots, it is removed, reported on `/ready`, fetched again, and never served.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tampered_cache_file_is_fetched_again_never_served() {
    let rig = rig(6, None, None).await;
    rig.publish().await;
    let grace = nuthatch::seal::FOLLOWER_GRACE;
    let (_a, first, cache) = rig.remote_with(&rig.mirror, 2, grace).await;
    agree(&rig, &first, "after the snapshot").await;
    rig.seal(1, 2);
    rig.publish().await;
    let good = agree(&rig, &first, "across a seal").await;
    let file = cache
        .join("segments")
        .join(format!("{}.parquet", rig.segment_hash()));
    let bytes = std::fs::read(&file).unwrap();
    let tamper = || {
        let mut b = bytes.clone();
        let mid = b.len() / 2;
        b[mid] ^= 0xff;
        std::fs::write(&file, &b).unwrap();
    };
    let refused = |ready: &serde_json::Value| {
        ready["follower"]["cache_refused"]
            .as_array()
            .map_or(0, |r| r.len())
    };

    // Between two runs: the next follower over the same cache hashes it before using it.
    tamper();
    let (b, second, _) = rig.remote_at(&rig.mirror, 2, grace, cache.clone()).await;
    assert_eq!(agree(&rig, &second, "over a tampered cache").await, good);
    assert_eq!(std::fs::read(&file).unwrap(), bytes, "fetched again");
    let (_, ready) = get(&second, &format!("/{ROUTE}/ready")).await;
    assert_eq!(refused(&ready), 1, "reported: {ready}");

    // While running: fresh snapshots check it again.
    tamper();
    b.resnapshot();
    let mut ready = serde_json::Value::Null;
    for _ in 0..200 {
        ready = get(&second, &format!("/{ROUTE}/ready")).await.1;
        // The follower removes a tampered file before fetching it again, so it may be absent here.
        if refused(&ready) == 2 && std::fs::read(&file).ok().as_deref() == Some(&bytes[..]) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(refused(&ready), 2, "reported: {ready}");
    assert_eq!(std::fs::read(&file).unwrap(), bytes, "fetched again");
    assert_eq!(agree(&rig, &second, "after the second tamper").await, good);
    rig.writer.stop();
}

/// A cached nest edited between follower runs no longer computes to the writer's NID: the next
/// follower over the cache reports it, installs it again from the registry, and serves that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edited_cached_nest_is_installed_again_never_served() {
    let rig = rig(6, None, None).await;
    rig.publish().await;
    let grace = nuthatch::seal::FOLLOWER_GRACE;
    let (_a, first, cache) = rig.remote_with(&rig.mirror, 2, grace).await;
    let good = agree(&rig, &first, "after the snapshot").await;
    let config = cache.join("data").join(ROUTE).join("nuthatch.toml");
    let original = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, format!("{original}\n# edited in the cache\n")).unwrap();

    let (_b, second, _) = rig.remote_at(&rig.mirror, 2, grace, cache.clone()).await;
    assert_eq!(agree(&rig, &second, "over an edited cache").await, good);
    assert_eq!(
        std::fs::read_to_string(&config).unwrap(),
        original,
        "installed again from the registry"
    );
    let (_, ready) = get(&second, &format!("/{ROUTE}/ready")).await;
    let refused = ready["follower"]["cache_refused"].to_string();
    assert!(refused.contains("the cached nest is not"), "{ready}");
    rig.writer.stop();
}

/// The same comparison against a real S3 server for the mirror and the registry (versitygw in CI).
#[cfg(feature = "object-store")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_follower_over_s3_answers_as_a_local_one_does() {
    let endpoint = match std::env::var("NUTHATCH_MINIO_ENDPOINT") {
        Ok(u) => u,
        Err(_) if std::env::var("NUTHATCH_REQUIRE_MINIO").is_ok() => {
            panic!("NUTHATCH_REQUIRE_MINIO is set but NUTHATCH_MINIO_ENDPOINT is not")
        }
        Err(_) => {
            eprintln!("SKIPPED: set NUTHATCH_MINIO_ENDPOINT to run the S3 remote follower");
            return;
        }
    };
    let _ = endpoint;
    let bucket =
        std::env::var("NUTHATCH_MINIO_BUCKET").unwrap_or_else(|_| "nuthatch-publish-test".into());
    let registry_bucket = std::env::var("NUTHATCH_REGISTRY_BUCKET")
        .unwrap_or_else(|_| "nuthatch-registry-test".into());
    let run = format!(
        "e2e-remote-follower/{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mirror = format!("s3://{bucket}/{run}");
    let registry = format!("s3://{registry_bucket}/{run}");
    let rig = rig(6, Some(mirror.clone()), Some(registry)).await;
    rig.publish().await;
    let (_f, remote, _) = rig.remote(&mirror, 2).await;
    agree(&rig, &remote, "after the snapshot, over S3").await;
    rig.seal(1, 2);
    rig.publish().await;
    let sealed = agree(&rig, &remote, "across a seal, over S3").await;
    assert_eq!(sealed["sealed_through"], 2);
    rig.seal(3, 4);
    rig.publish().await;
    agree(&rig, &remote, "across a fold, over S3").await;
    rig.writer.stop();
}

/// Visibility lag as the writer records it, for RFC-0076 §10: tip deltas block by block and a seal
/// every ten, one remote follower over loopback and a filesystem mirror. Prints the histograms.
/// `cargo test --release --test it e2e_remote_follower::measure_visibility -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn measure_visibility() {
    let rig = rig(10, None, None).await;
    rig.publish().await;
    let (_f, remote, _) = rig.remote(&rig.mirror, 2).await;
    agree(&rig, &remote, "after the snapshot").await;
    let mut sealed = 0;
    for b in 11..=210u64 {
        rig.tape.insert_block(b, block(b, 100 * b as u128));
        rig.tape.advance_tip_to(b);
        rig.writer.reaches(b).await;
        if b % 10 == 0 {
            rig.seal(sealed + 1, b - 5);
            sealed = b - 5;
            rig.publish().await;
        }
    }
    agree(&rig, &remote, "at the end").await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let text = reqwest::get(format!("{}/metrics", rig.writer.base()))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for l in text.lines().filter(|l| l.starts_with("nuthatch_feed_")) {
        println!("{l}");
    }
    let follower = reqwest::get(format!("{remote}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for l in follower
        .lines()
        .filter(|l| l.starts_with("nuthatch_follower_"))
    {
        println!("{l}");
    }
    rig.writer.stop();
}
