//! #520: `serve` without `--hot-store` used to be `Store::open` - create-if-missing, commits a write
//! txn, holds the exclusive flock - on the role `cli.rs`'s own help called **read-only** and shared.
//! None of that was true: it brought an empty store into being for a nest never indexed (#413's
//! mistake at a site #413 did not cover), and the flock meant it could not run beside the `dev`/writer
//! whose state it exists to serve, nor beside a second `serve`.
//!
//! This drives `indexer::serve_role` - the production entry point, not a hand-built `AppState` - over
//! both arms the issue asks for: no store on disk, and a store a live handle already holds.

use std::time::{Duration, Instant};

use nuthatch::cli::ServeArgs;

mod common;

use common::tape::{scaffold_nest, USDC};

/// A nest with `nuthatch.toml` on disk but no `nuthatch.redb` - what `nuthatch init` alone leaves
/// behind, before `dev` has ever run.
fn scaffold_unindexed_nest(dir: &std::path::Path) {
    scaffold_nest(dir, "usdc", USDC);
    let toml_path = dir.join("nuthatch.toml");
    let toml = std::fs::read_to_string(&toml_path).unwrap();
    // Never dialled - `serve_role` refuses before constructing anything that would need it.
    std::fs::write(
        &toml_path,
        toml.replace("rpc_urls = []", r#"rpc_urls = ["http://127.0.0.1:1"]"#),
    )
    .unwrap();
}

async fn free_port() -> u16 {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    probe.local_addr().unwrap().port()
}

/// Drive `serve_role` expecting it to *refuse*, and return the error it refused with.
///
/// Bounded deliberately. A regression that opens the store instead does not return at all - it binds
/// the port and serves forever - so an unbounded `.await` here would hang the whole test binary until
/// the CI job's own timeout, which this repo has already learned reports as `cancelled` and reads
/// like a supersede rather than a failure. Twenty seconds is far more than a refusal needs: both
/// arms below fail before they touch the network.
async fn refusal(args: ServeArgs) -> anyhow::Error {
    match tokio::time::timeout(Duration::from_secs(20), nuthatch::indexer::serve_role(args)).await {
        Ok(Ok(())) => panic!("serve must refuse this nest, but `serve_role` returned Ok"),
        Ok(Err(err)) => err,
        Err(_) => panic!(
            "serve must refuse at startup, but was still running after 20s - it opened the store and \
             started serving instead of refusing"
        ),
    }
}

/// `serve` against a nest directory with no store must refuse rather than create one - it must not be
/// able to make itself true, the same principle #413 established for the `sql` probe.
#[tokio::test]
async fn serve_without_hot_store_refuses_a_nest_with_no_store_rather_than_creating_one() {
    let dir = tempfile::tempdir().unwrap();
    scaffold_unindexed_nest(dir.path());
    let db_path = dir.path().join(nuthatch::config::DB_FILE);
    assert!(
        !db_path.exists(),
        "premise: nothing has indexed this nest yet"
    );

    let args = ServeArgs {
        cors: Vec::new(),
        dir: dir.path().to_string_lossy().into_owned(),
        listen: format!("127.0.0.1:{}", free_port().await),
        hot_store: None,
        admin: false,
        publish_target: None,
        publish_interval: std::time::Duration::from_secs(60),
        publish_parallelism: 2,
        runtime: None,
        follow: None,
        mirror: None,
        registry: None,
        cache: None,
        max_rss_mb: None,
        feed_version: 2,
    };
    let err = refusal(args).await;

    assert!(
        !db_path.exists(),
        "serve must never create {} - it holds no cursor and has nothing to write",
        db_path.display()
    );
    let msg = format!("{err:#}");
    assert!(
        msg.contains(&db_path.display().to_string()),
        "the refusal must name the store path so an operator knows what to index first: {msg}"
    );
}

/// `serve` against a store a live handle already holds must refuse at startup, not half-succeed and
/// fail requests later - redb's exclusive flock means exactly one process may hold the file, whatever
/// "shares a file rather than a service" used to promise.
#[tokio::test]
async fn serve_without_hot_store_refuses_a_store_a_live_handle_holds() {
    let dir = tempfile::tempdir().unwrap();
    scaffold_unindexed_nest(dir.path());
    let db_path = dir.path().join(nuthatch::config::DB_FILE);
    // Stands in for `dev`: opens (and thereby creates) the store, and is kept alive across the
    // `serve_role` call below so the flock is genuinely held, not released before the assertion.
    let _writer = nuthatch::store::Store::open(&db_path).unwrap();
    assert!(db_path.exists(), "premise: the writer's store now exists");

    let args = ServeArgs {
        cors: Vec::new(),
        dir: dir.path().to_string_lossy().into_owned(),
        listen: format!("127.0.0.1:{}", free_port().await),
        hot_store: None,
        admin: false,
        publish_target: None,
        publish_interval: std::time::Duration::from_secs(60),
        publish_parallelism: 2,
        runtime: None,
        follow: None,
        mirror: None,
        registry: None,
        cache: None,
        max_rss_mb: None,
        feed_version: 2,
    };
    let err = refusal(args).await;

    let msg = format!("{err:#}");
    assert!(
        msg.contains(&db_path.display().to_string()),
        "the refusal must name the store path: {msg}"
    );
}

/// The other side of the arm above, so the refusal cannot be blamed on a fixture that never actually
/// serves anything: with no competing handle and a store the writer left behind, `serve` comes up and
/// answers - `--hot-store` is optional, not the store's existence.
#[tokio::test]
async fn serve_without_hot_store_serves_a_store_the_writer_left_behind() {
    let dir = tempfile::tempdir().unwrap();
    scaffold_unindexed_nest(dir.path());
    let db_path = dir.path().join(nuthatch::config::DB_FILE);
    // The writer opens and closes before `serve` starts - the flock is released with it.
    nuthatch::store::Store::open(&db_path).unwrap();

    let port = free_port().await;
    let listen = format!("127.0.0.1:{port}");
    let args = ServeArgs {
        cors: Vec::new(),
        dir: dir.path().to_string_lossy().into_owned(),
        listen: listen.clone(),
        hot_store: None,
        admin: false,
        publish_target: None,
        publish_interval: std::time::Duration::from_secs(60),
        publish_parallelism: 2,
        runtime: None,
        follow: None,
        mirror: None,
        registry: None,
        cache: None,
        max_rss_mb: None,
        feed_version: 2,
    };
    let task = tokio::spawn(async move {
        let _ = nuthatch::indexer::serve_role(args).await;
    });

    let base = format!("http://{listen}");
    let client = reqwest::Client::new();
    let start = Instant::now();
    let mut up = false;
    while start.elapsed() < Duration::from_secs(20) {
        if let Ok(resp) = client.get(&base).send().await {
            if resp.status().is_success() {
                up = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    task.abort();
    assert!(up, "serve must come up against a store already on disk");
}
