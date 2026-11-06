//! Which backend `nuthatch sql` picks, driven through the real binary (issue #413).
//!
//! The decision lives in `SqlBackend::open` in `src/main.rs`, so there is no library seam to unit-test
//! it through - these run the compiled `nuthatch` and watch a stand-in for `nuthatch dev` to see
//! whether the fallback fired. That is the point of the issue: the probe used to open the store with
//! `Database::create`, so in a directory with no nest it *made* one, reported a local nest, answered
//! from the empty file it had just written, and never asked the running instance that held the data.
//!
//! All three routings are asserted together, because "absent → HTTP" alone is satisfied by a backend
//! that always chooses HTTP.
//!
//! The mount-prefix tests below (issue #546, closing the gap #509/#545 left) are a separate concern
//! from the backend-choice tests above: given the HTTP fallback is chosen, does it ask for the right
//! path? They cannot reuse `serving_instance()` - that stand-in answers `/sql` unconditionally, which
//! would go green whether or not `resolve_mount_prefix` worked. They bring up the real
//! `serve::compose_runtime` wiring `runtime::dev` uses instead, exactly to avoid the fixture this repo
//! keeps getting wrong: an `AppState` built by hand and served without ever going through the router
//! that actually nests a mount under `/<alias>`.

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{extract::Query, routing::get, Json, Router};
use serde_json::json;

use common::tape::*;
use nuthatch::runtime::{self, MountTable, MOUNTS_FILE};
use nuthatch::{health::RuntimeHealth, indexer, migrate, serve};

/// A stand-in for a running `nuthatch dev`: answers `/sql` with one unmistakable row, and counts the
/// requests it was asked, so "the answer came from HTTP" is observed rather than inferred.
struct Instance {
    url: String,
    hits: Arc<AtomicUsize>,
}

/// The marker only the HTTP instance can produce. It cannot appear in a local answer - there is no
/// such column in any nest - so finding it in stdout is proof of where the row came from.
const MARKER: &str = "answered-by-the-running-instance";

async fn serving_instance() -> Instance {
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let app = Router::new().route(
        "/sql",
        get(move |Query(q): Query<HashMap<String, String>>| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Json(json!({
                    "rows": [{ "who": MARKER, "asked": q.get("q") }],
                    "truncated": false,
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Instance { url, hits }
}

/// Run `nuthatch sql <query> --dir <dir> --url <url>` and return its exit status with its
/// stdout+stderr.
///
/// The status is returned rather than dropped because without it every assertion here is a *negative*
/// one - "the instance was not asked" - and a backend that was chosen and then failed outright
/// satisfies those just as well as one that worked. See `a_store_that_is_here_and_unheld_is_queried_locally`.
async fn run_sql(dir: &Path, url: &str, query: &str) -> (std::process::ExitStatus, String) {
    let (dir, url, query) = (dir.to_path_buf(), url.to_string(), query.to_string());
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(["sql", &query, "--dir"])
            .arg(&dir)
            .args(["--url", &url])
            .output()
            .expect("running the nuthatch binary");
        (
            out.status,
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        )
    })
    .await
    .unwrap()
}

/// **Absent → HTTP, and no store left behind.** The reported defect, both halves.
///
/// The directory has no nest in it, which is what `--dir .` means whenever the command is run one
/// level up from the nest - the ordinary way to meet this.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_local_store_asks_the_running_instance_and_creates_nothing() {
    let instance = serving_instance().await;
    let empty = tempfile::tempdir().unwrap();

    let (status, output) = run_sql(empty.path(), &instance.url, "SELECT 1 AS n").await;

    assert!(
        status.success(),
        "the fallback must answer, not merely be chosen, got:\n{output}"
    );
    assert!(
        output.contains(MARKER),
        "with no store here the answer must come from the running instance, got:\n{output}"
    );
    assert_eq!(
        instance.hits.load(Ordering::SeqCst),
        1,
        "and the instance must actually have been asked"
    );
    assert!(
        !empty.path().join("nuthatch.redb").exists(),
        "probing for a store must not create one - it left {:?} behind",
        empty.path().join("nuthatch.redb")
    );
}

/// **Present and free → local.** Without this the fix is indistinguishable from "always use HTTP",
/// which would break the offline case the local backend exists for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_that_is_here_and_unheld_is_queried_locally() {
    let instance = serving_instance().await;
    let nest = tempfile::tempdir().unwrap();
    let db = nest.path().join("nuthatch.redb");
    // A real store, closed again so nothing holds redb's lock.
    drop(nuthatch::store::Store::open(&db).unwrap());

    let (status, output) = run_sql(nest.path(), &instance.url, "SELECT 1 AS n").await;

    // Positive first. The two assertions below are both negative, and a local backend that was chosen
    // and then died satisfies each of them - so on their own they prove *not-HTTP*, not *local*.
    assert!(
        status.success(),
        "the local backend must answer, not merely be chosen, got:\n{output}"
    );
    assert!(
        output.contains('n') && output.contains('1'),
        "and the answer must be the query's, got:\n{output}"
    );
    assert!(
        !output.contains(MARKER),
        "a store that is present and free is queried locally, got:\n{output}"
    );
    assert_eq!(
        instance.hits.load(Ordering::SeqCst),
        0,
        "and the running instance must not have been asked at all"
    );
}

/// **Present but held by `dev` → HTTP.** redb is single-writer; this is the case the original comment
/// described and the one the fix must not regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_held_by_another_process_falls_back_to_the_instance() {
    let instance = serving_instance().await;
    let nest = tempfile::tempdir().unwrap();
    let db = nest.path().join("nuthatch.redb");
    // Held open for the duration, exactly as `nuthatch dev` holds it.
    let _held = nuthatch::store::Store::open(&db).unwrap();

    let (status, output) = run_sql(nest.path(), &instance.url, "SELECT 1 AS n").await;

    assert!(
        status.success(),
        "the fallback must answer, not merely be chosen, got:\n{output}"
    );
    assert!(
        output.contains(MARKER),
        "a store held by `dev` falls back to the running instance, got:\n{output}"
    );
    assert_eq!(instance.hits.load(Ordering::SeqCst), 1);
}

/// **Absent store, and no instance to fall back to → both halves named.** (#474)
///
/// `absent_store` exists so the connect failure can say more than "is `nuthatch dev` running?" -
/// that alone is only half the truth when the real mistake is the directory. Nothing in the suite
/// named the message itself; it could be reworded into nonsense, or the `Some(db)` arm dropped for
/// the bare `None` message, with every other test here still green, because they all point `--url`
/// at a real `serving_instance`.
///
/// Port 1 is privileged, so nothing in CI binds it - the connection is refused rather than merely
/// slow, without needing to race a listener's shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_local_store_and_no_instance_names_both_in_the_error() {
    let empty = tempfile::tempdir().unwrap();

    let (status, output) = run_sql(empty.path(), "http://127.0.0.1:1", "SELECT 1 AS n").await;

    assert!(
        !status.success(),
        "there is nothing to answer this query, got:\n{output}"
    );
    let expected_db = empty.path().join("nuthatch.redb");
    assert!(
        output.contains(&format!("no store at {}", expected_db.display())),
        "the message must name the absent store, got:\n{output}"
    );
    assert!(
        output.contains("Is `nuthatch dev` running, and is --dir the nest directory?"),
        "and must ask about both the process and the directory, got:\n{output}"
    );
    assert!(
        !expected_db.exists(),
        "probing for a store must not create one - it left {expected_db:?} behind"
    );
}

// ---------------------------------------------------------------------------
// Mount-prefix resolution (issue #546)
// ---------------------------------------------------------------------------

/// Bring up a real `mounts.toml` runtime with one nest mounted under `alias`, the same way
/// `runtime::dev` does: a migrated mount table, a real `indexer::spawn_runtime` cursor over a
/// scripted tape, and `serve::compose_runtime` bound to a real TCP listener - not a hand-built
/// `AppState` handed straight to a handler. Returns the bound address and the dataset directory
/// (`data/<nid>`) the CLI is pointed at, matching `--dir data/<nid>` on a real deployment.
///
/// The store stays open for the rest of the test (inside the `AppState` the spawned server holds),
/// so the CLI subprocess's own local-store probe is refused and it is forced onto the HTTP path this
/// issue is about - the same way a live `nuthatch dev` forces `nuthatch sql` there.
async fn bring_up_mounted_runtime(
    root: &Path,
    alias: &str,
) -> (std::net::SocketAddr, std::path::PathBuf) {
    std::fs::write(
        root.join(MOUNTS_FILE),
        format!(
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = []\nnests = [\"{alias}\"]\n"
        ),
    )
    .unwrap();
    let nest_dir = root.join("nests").join(alias);
    std::fs::create_dir_all(&nest_dir).unwrap();
    scaffold_nest(&nest_dir, alias, USDC);
    migrate::run(root, false, false).expect("migrate to the 2.0 data/<nid> layout");

    let mounts = MountTable::load(root).unwrap();
    let datasets = mounts.datasets(root);
    let default_tenant_owned = mounts.tenant_default();
    let default_tenant = default_tenant_owned.as_str();
    assert!(
        !mounts.is_multi_tenant(),
        "a single mount must not become multi-tenant - route_key would gain a tenant segment"
    );

    let tape = Arc::new(TapeSource::new());
    let a1 = account(1);
    let a2 = account(2);
    tape.insert_block(
        1,
        transfers_block(
            1,
            0,
            1_700_000_001,
            USDC,
            &[(a1.as_str(), a2.as_str(), 100)],
        ),
    );
    tape.advance_tip_to(1);

    let health = Arc::new(RuntimeHealth::new());
    for ds in &datasets {
        health.register(&ds.canonical().route_key(default_tenant), "arbitrum-one");
    }
    let (mounted, failed) = runtime::load_mounted(root, &datasets, default_tenant);
    assert!(failed.is_empty(), "load_mounted: {failed:?}");
    let cursor = indexer::spawn_runtime(
        tape.clone(),
        mounted,
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

    let landed = wait_until(POLL_TIMEOUT, || {
        cursor
            .states
            .iter()
            .all(|(_, s)| s.store.get_meta("last_block").ok().flatten().as_deref() == Some("1"))
    })
    .await;
    assert!(landed, "the mounted nest did not index to the tip in time");

    let mut estimates = HashMap::new();
    let states = runtime::fan_out_aliases(
        &datasets,
        cursor.states,
        &health,
        &mut estimates,
        default_tenant,
    );

    let nid = datasets[0]
        .nid
        .clone()
        .expect("a migrated dataset must carry an identity");
    // The fields `resolve_mount_prefix` actually reads (`nid`, `base_path`) - matching the shape
    // `runtime::dev`'s roster builds (`runtime.rs`'s `roster_entries`), not every field it carries.
    let roster = serde_json::json!({
        "runtime": "test",
        "nests": [{"name": alias, "nid": nid, "base_path": format!("/{alias}")}],
    });
    let live = serve::LiveRuntime::new(serve::compose_runtime(roster, states, health));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, live.service()).await;
    });

    (addr, datasets[0].dir.clone())
}

/// **The mutation this test exists for.** Against the pre-#509 behaviour (`resolve_mount_prefix`
/// stubbed to `String::new()`), or with the `(Some(only), None)` arm degenerating to `String::new()`,
/// or with `{prefix}` dropped from the request URL's `format!`, the CLI asks `compose_runtime` for
/// `/sql` at the root - a path nothing here registers (only `/health`, `/nests`, `/ready` and each
/// nest's routes under `/<alias>`) - so the request 404s and the command exits non-zero. A "prefix
/// was returned" assertion cannot see any of that; only a query that must actually succeed can.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mounted_runtime_resolves_the_alias_prefix_and_the_query_succeeds() {
    let root = tempfile::tempdir().unwrap();
    let (addr, dataset_dir) = bring_up_mounted_runtime(root.path(), "usdc").await;

    let (status, output) = run_sql(&dataset_dir, &format!("http://{addr}"), "SELECT 1 AS n").await;

    assert!(
        status.success(),
        "the query must reach /usdc/sql through the resolved prefix, got:\n{output}"
    );
    assert!(
        output.contains('n') && output.contains('1'),
        "and the answer must be the query's, got:\n{output}"
    );
}

/// **The other half of #546.** A solo (`nuthatch.toml`) runtime has no `/nests` route at all, so
/// `resolve_mount_prefix` must leave the request unprefixed - the behaviour before #509, which #509
/// must not have regressed for the case it wasn't fixing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_solo_runtime_stays_unprefixed() {
    let root = tempfile::tempdir().unwrap();
    let addr = bring_up_solo_runtime(root.path()).await;

    let (status, output) = run_sql(root.path(), &format!("http://{addr}"), "SELECT 1 AS n").await;

    assert!(
        status.success(),
        "a solo runtime serves /sql at the root and must stay unprefixed, got:\n{output}"
    );
    assert!(
        output.contains('n') && output.contains('1'),
        "and the answer must be the query's, got:\n{output}"
    );
}

// ---------------------------------------------------------------------------
// Column order (issue #1609)
// ---------------------------------------------------------------------------

const ORDERED: &str = "SELECT 1 AS z, 2 AS a, 3 AS m";

/// The table's header and first row, as cells.
fn table_head(output: &str) -> (Vec<String>, Vec<String>) {
    let cells = |line: &str| line.split('|').map(|c| c.trim().to_string()).collect();
    let mut lines = output.lines();
    let header = cells(lines.next().unwrap_or_default());
    let row = cells(lines.nth(1).unwrap_or_default());
    (header, row)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_answer_prints_the_querys_column_order() {
    let instance = serving_instance().await;
    let nest = tempfile::tempdir().unwrap();
    drop(nuthatch::store::Store::open(&nest.path().join("nuthatch.redb")).unwrap());

    let (status, output) = run_sql(nest.path(), &instance.url, ORDERED).await;

    assert!(status.success(), "{output}");
    assert_eq!(instance.hits.load(Ordering::SeqCst), 0, "{output}");
    assert_eq!(
        table_head(&output),
        (
            vec!["z".into(), "a".into(), "m".into()],
            vec!["1".into(), "2".into(), "3".into()]
        ),
        "{output}"
    );
}

/// Through the real router, so the order has to cross the `/sql` response to survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_answer_over_http_prints_the_querys_column_order() {
    let root = tempfile::tempdir().unwrap();
    let addr = bring_up_solo_runtime(root.path()).await;

    let (status, output) = run_sql(root.path(), &format!("http://{addr}"), ORDERED).await;

    assert!(status.success(), "{output}");
    assert_eq!(
        table_head(&output),
        (
            vec!["z".into(), "a".into(), "m".into()],
            vec!["1".into(), "2".into(), "3".into()]
        ),
        "{output}"
    );
}

/// A solo (`nuthatch.toml`) nest indexed to block 1 and served by the real router, which holds its
/// store open so the CLI is forced onto the HTTP path.
async fn bring_up_solo_runtime(root: &Path) -> std::net::SocketAddr {
    let cfg = scaffold_nest(root, "usdc", USDC);

    let tape = Arc::new(TapeSource::new());
    let a1 = account(1);
    let a2 = account(2);
    tape.insert_block(
        1,
        transfers_block(
            1,
            0,
            1_700_000_001,
            USDC,
            &[(a1.as_str(), a2.as_str(), 100)],
        ),
    );
    tape.advance_tip_to(1);

    let rt = indexer::spawn_nest(
        tape.clone(),
        root.to_path_buf(),
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
    let store = rt.state.store.clone();

    let landed = wait_until(POLL_TIMEOUT, || {
        store.get_meta("last_block").ok().flatten().as_deref() == Some("1")
    })
    .await;
    assert!(landed, "the solo nest did not index to the tip in time");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = serve::router(serve::SharedNest::new(rt.state));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

const TEMPORAL: &str = "SELECT CAST('2026-10-05' AS DATE) AS d, \
     CAST('1969-12-31' AS DATE) AS before_epoch, \
     CAST('2026-10-05 12:34:56.25' AS TIMESTAMP) AS ts, \
     CAST('2026-10-05 12:34:56' AS TIMESTAMP) AS whole, \
     CAST('2026-10-05T12:34:56+02:00' AS TIMESTAMP WITH TIME ZONE) AS tz";

fn temporal_row() -> serde_json::Value {
    json!({
        "d": "2026-10-05",
        "before_epoch": "1969-12-31",
        "ts": "2026-10-05T12:34:56.25Z",
        "whole": "2026-10-05T12:34:56Z",
        "tz": "2026-10-05T10:34:56Z",
    })
}

async fn run_sql_json(dir: &Path, url: &str, query: &str) -> serde_json::Value {
    let (dir, url, query) = (dir.to_path_buf(), url.to_string(), query.to_string());
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_nuthatch"))
            .args(["sql", &query, "--json", "--dir"])
            .arg(&dir)
            .args(["--url", &url])
            .output()
            .expect("running the nuthatch binary")
    })
    .await
    .unwrap();
    assert!(out.status.success(), "{out:?}");
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&out.stdout)))
}

/// #1852: dates print as YYYY-MM-DD and timestamps as RFC 3339, in the table and in `--json`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_answer_prints_dates_and_timestamps_as_iso_8601() {
    let instance = serving_instance().await;
    let nest = tempfile::tempdir().unwrap();
    drop(nuthatch::store::Store::open(&nest.path().join("nuthatch.redb")).unwrap());

    let (status, output) = run_sql(nest.path(), &instance.url, TEMPORAL).await;
    assert!(status.success(), "{output}");
    let want = temporal_row();
    let (header, row) = table_head(&output);
    for (col, cell) in header.iter().zip(&row) {
        assert_eq!(Some(cell.as_str()), want[col].as_str(), "{col}:\n{output}");
    }
    assert_eq!(header.len(), 5, "{output}");

    assert_eq!(
        run_sql_json(nest.path(), &instance.url, TEMPORAL).await,
        want
    );
    assert_eq!(instance.hits.load(Ordering::SeqCst), 0);
}

/// The same answer from `/sql` itself and from the CLI going through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_over_http_returns_dates_and_timestamps_as_iso_8601() {
    let root = tempfile::tempdir().unwrap();
    let addr = bring_up_solo_runtime(root.path()).await;
    let url = format!("http://{addr}");

    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("{url}/sql"))
        .query(&[("q", TEMPORAL)])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["rows"], json!([temporal_row()]), "{body}");

    assert_eq!(
        run_sql_json(root.path(), &url, TEMPORAL).await,
        temporal_row()
    );
    let (status, output) = run_sql(root.path(), &url, TEMPORAL).await;
    assert!(status.success(), "{output}");
    assert!(output.contains("2026-10-05T10:34:56Z"), "{output}");
}
