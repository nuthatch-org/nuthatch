//! End-to-end solo-pipeline tests: golden land → seal → query, and serving over real HTTP.
//!
//! Both drive the real `indexer::spawn_nest` background loop against a scripted [`TapeSource`], and
//! observe progress by bounded polling on the hot store / HTTP - no fixed sleeps drive the pipeline.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use nuthatch::{analytics, indexer, seal, serve};

use common::tape::*;

/// Query timeout for the read-only surface in tests - generous; these fixtures are tiny.
fn guard() -> analytics::QueryGuard {
    analytics::QueryGuard {
        timeout: Duration::from_secs(10),
        max_rows: 100_000,
    }
}

/// Drive one solo nest through land → seal, assert the hot/cold split, and return the sealed
/// `usdc__transfer` segment content-hashes. Called twice over identical fixtures to prove the sealed
/// content address is deterministic across runs.
async fn drive_land_seal_query(dir: &std::path::Path) -> Vec<String> {
    let cfg = scaffold_nest(dir, "usdc", USDC);
    let tape = Arc::new(TapeSource::new());

    // Ten blocks, one USDC transfer each, distinct value 100*b. Finality stays at 0 → nothing seals.
    let a1 = account(1);
    let a2 = account(2);
    for b in 1..=10u64 {
        let mut fx = transfers_block(
            b,
            0,
            1_700_000_000 + b,
            USDC,
            &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
        );
        if b == 5 {
            pad_address_to_seal(&mut fx, USDC, 4);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(10);

    // Small getLogs window so sealing has a boundary short of the full range.
    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.to_path_buf(),
        cfg,
        None,
        false,
        1,
        // One window over the backfill, so a padded block is fetched once, not again as a tail.
        Some(10),
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let store = rt.state.store.clone();

    // Land: index all ten blocks into the hot store.
    let landed = wait_until(POLL_TIMEOUT, || {
        store.get_meta("last_block").ok().flatten().as_deref() == Some("10")
    })
    .await;
    assert!(landed, "nest did not index to the tip in time");

    // Hot rows for the whole range are present; nothing sealed yet. Block 5 is padded
    // to [`indexer::SEAL_DIRECT_BATCH`] so the tip path will actually cut (#1067).
    assert_eq!(
        store.entities_in_range(1, 10).unwrap().len(),
        indexer::SEAL_DIRECT_BATCH + 5,
        "the ten real transfers plus the seal-batch padding in block 5"
    );
    assert_eq!(store.sealed_through(), 0, "nothing is final yet");
    assert!(
        seal::load_manifest(dir).unwrap().tables.is_empty(),
        "no segments before finality advances"
    );

    // Seal: finalize through block 5, then push an empty block 11 so a fresh window is processed and
    // the newly-finalized range seals to Parquet.
    tape.advance_finalized_to(5);
    tape.insert_block(11, empty_block(11, 0, 1_700_000_100));
    tape.advance_tip_to(11);

    let sealed = wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 5).await;
    assert!(sealed, "range [1,5] did not seal in time");

    // Segments now exist for the transfer table.
    let manifest = seal::load_manifest(dir).unwrap();
    let segs = manifest
        .tables
        .get(&transfer_table("usdc"))
        .expect("transfer table sealed");
    assert!(!segs.is_empty(), "expected at least one sealed segment");

    // Cold-only query sees ONLY the sealed subset (blocks 1..=5); the hot tip (6..=10) is invisible.
    let cold = analytics::query(
        dir,
        &format!(
            "SELECT block_number FROM \"{}\" ORDER BY block_number",
            transfer_table("usdc")
        ),
    )
    .unwrap();
    let cold_blocks: BTreeSet<u64> = cold
        .iter()
        .map(|r| r["block_number"].as_u64().unwrap())
        .collect();
    assert_eq!(
        cold_blocks,
        (1..=5).collect::<BTreeSet<u64>>(),
        "cold-only query must return exactly the sealed subset"
    );

    // Hot+cold query spans BOTH the sealed range and the live tip (blocks 1..=10).
    let hot = store.hot_rows_by_table().unwrap();
    let hc = analytics::query_hot_cold(
        dir,
        &format!(
            "SELECT block_number FROM \"{}\" ORDER BY block_number",
            transfer_table("usdc")
        ),
        guard(),
        &hot,
        store.sealed_through(),
        &[],
    )
    .unwrap();
    let hc_blocks: BTreeSet<u64> = hc
        .rows
        .iter()
        .map(|r| r["block_number"].as_u64().unwrap())
        .collect();
    assert_eq!(
        hc_blocks,
        (1..=10).collect::<BTreeSet<u64>>(),
        "hot+cold query must span the sealed range and the hot tip"
    );

    let hashes: Vec<String> = segs.iter().map(|s| s.hash.clone()).collect();

    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
    hashes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn golden_land_seal_query_is_deterministic() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let h1 = drive_land_seal_query(d1.path()).await;
    let h2 = drive_land_seal_query(d2.path()).await;
    assert!(!h1.is_empty());
    assert_eq!(
        h1, h2,
        "sealed content-address hashes must be identical across two runs over identical fixtures"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serves_over_real_http() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let tape = Arc::new(TapeSource::new());

    // Three transfers across three blocks; value = 100*b. All hot (finality stays at 0).
    let a1 = account(1);
    let a2 = account(2);
    for b in 1..=3u64 {
        tape.insert_block(
            b,
            transfers_block(
                b,
                0,
                1_700_000_000 + b,
                USDC,
                &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
            ),
        );
    }
    tape.advance_tip_to(3);

    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.path().to_path_buf(),
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
    let alert_worker = rt.alert_worker;

    // Bind our own listener and serve the real router on a task.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = serve::router(serve::SharedNest::new(rt.state));
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Poll `/` for entities > 0 with a bounded timeout (no fixed sleep).
    let mut entities = 0u64;
    let start = std::time::Instant::now();
    while start.elapsed() < POLL_TIMEOUT {
        if let Ok(resp) = client.get(&base).send().await {
            if let Ok(v) = resp.json::<serde_json::Value>().await {
                entities = v["entities"].as_u64().unwrap_or(0);
                if entities >= 3 {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(entities, 3, "expected all three transfers served at `/`");

    // GET / - summary shape.
    let root: serde_json::Value = client
        .get(&base)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(root["name"], "nuthatch");
    assert_eq!(root["chain"], "arbitrum-one");
    assert_eq!(root["entities"], 3);
    assert_eq!(root["last_block"], "3");

    // GET /tables - the decoded data model.
    let tables: serde_json::Value = client
        .get(format!("{base}/tables"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(tables["count"].as_u64().unwrap() >= 1);
    let names: Vec<&str> = tables["tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["table"].as_str())
        .collect();
    assert!(
        names.contains(&transfer_table("usdc").as_str()),
        "tables should list usdc__transfer, got {names:?}"
    );

    // GET /sql - count matches the fed transfers.
    let sql: serde_json::Value = client
        .get(format!("{base}/sql"))
        .query(&[(
            "q",
            format!("SELECT count(*) AS n FROM \"{}\"", transfer_table("usdc")),
        )])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(sql["count"], 1, "one aggregate row");
    // DuckDB returns count as a number; compare loosely against 3.
    assert_eq!(
        sql["rows"][0]["n"].as_u64().unwrap(),
        3,
        "sql sees three rows"
    );

    // GET /entity/{id} - the block-1 transfer, value 100.
    let id = nuthatch::store::Store::entity_key(1, 0);
    let entity: serde_json::Value = client
        .get(format!("{base}/entity/{id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(entity["block_number"], 1);
    assert_eq!(entity["value"], "100");
    assert_eq!(entity["table"], transfer_table("usdc"));

    // GET /health - plain "ok".
    let health = client
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(health, "ok");

    ingest.abort();
    if let Some(w) = alert_worker {
        w.abort();
    }
    server.abort();
}

/// RFC-0020 slice 2b - the compatible hot-upgrade: two real `spawn_nest` indexers (old + new version)
/// run concurrently against the same scripted chain; the endpoint serves the OLD version until the NEW
/// one catches up, then `await_catchup_and_flip` atomically re-points the served backing to the new
/// version. Deterministic (no network, no sleeps driving the pipeline) - the old/new backings are told
/// apart by their `dir`. This is the full concurrent-reindex-then-flip proven end to end.
#[tokio::test]
async fn compatible_hot_upgrade_flips_backing_after_catchup() {
    let old_dir = tempfile::tempdir().unwrap();
    let new_dir = tempfile::tempdir().unwrap();
    let old_cfg = scaffold_nest(old_dir.path(), "usdc", USDC);
    let new_cfg = scaffold_nest(new_dir.path(), "usdc", USDC);

    // One scripted chain both versions follow: five blocks, one USDC transfer each.
    let tape = Arc::new(TapeSource::new());
    let (a1, a2) = (account(1), account(2));
    for b in 1..=5u64 {
        tape.insert_block(
            b,
            transfers_block(
                b,
                0,
                1_700_000_000 + b,
                USDC,
                &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
            ),
        );
    }
    tape.advance_tip_to(5);

    let mut old_rt = indexer::spawn_nest(
        tape.clone(),
        old_dir.path().to_path_buf(),
        old_cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn old");
    let old_store = old_rt.state.store.clone();
    tokio::time::timeout(POLL_TIMEOUT, async {
        while old_store.indexed_head().unwrap() != Some(5) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the old version should reach the tip before the new one is spawned");

    let new_rt = indexer::spawn_nest(
        tape.clone(),
        new_dir.path().to_path_buf(),
        new_cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn new");

    let new_store = new_rt.state.store.clone();
    let new_state = new_rt.state;
    let shared = serve::SharedNest::new(old_rt.state);

    assert_eq!(shared.current().dir.as_path(), old_dir.path());

    tokio::time::timeout(
        POLL_TIMEOUT,
        indexer::await_catchup(
            &old_store,
            &new_store,
            Duration::from_millis(20),
            POLL_TIMEOUT,
        ),
    )
    .await
    .expect("catch-up timed out")
    .expect("catch-up");

    indexer::quiesce_ingest(
        &mut old_rt.ingest,
        old_store.as_ref(),
        Duration::from_secs(2),
    )
    .await
    .expect("quiesce old writer");
    let frozen = old_store.indexed_head().unwrap();
    assert_eq!(frozen, Some(5), "old version was at the tip before quiesce");

    let new_head = tokio::time::timeout(
        POLL_TIMEOUT,
        indexer::wait_until_caught_up(&new_store, frozen, Duration::from_millis(20), POLL_TIMEOUT),
    )
    .await
    .expect("flip wait timed out")
    .expect("flip wait");
    shared.swap(new_state);

    assert_eq!(shared.current().dir.as_path(), new_dir.path());
    assert!(
        new_head >= frozen,
        "the flip must never move the endpoint backwards: new={new_head:?} old={frozen:?}"
    );
    assert_eq!(
        old_store.indexed_head().unwrap(),
        frozen,
        "quiesced old writer must not have advanced between catch-up and swap"
    );

    // Separately: left alone, the new version does reach the tip. Polled rather than asserted
    // instantaneously, because *when* it arrives is a matter of scheduling, not of correctness.
    tokio::time::timeout(POLL_TIMEOUT, async {
        while new_store.indexed_head().unwrap() != Some(5) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the new version should reach the tip once it has caught up");

    old_rt.ingest.abort(); // already aborted above; abort is idempotent
    new_rt.ingest.abort();
}

/// RFC-0020 slice 3 - the breaking path: two versions served on distinct endpoints over one listener.
/// The OLD version stays at the root (its consumers unchanged) but every response carries a
/// `Deprecation: true` header + a `Link` to the successor; the NEW version is served under `/next` and
/// is not deprecated. Distinct nest aliases (`usdc` vs `usdcv2`) make the two schemas tell-apart-able.
#[tokio::test]
async fn breaking_upgrade_serves_both_versions_with_old_deprecated() {
    let old_dir = tempfile::tempdir().unwrap();
    let new_dir = tempfile::tempdir().unwrap();
    let old_cfg = scaffold_nest(old_dir.path(), "usdc", USDC);
    let new_cfg = scaffold_nest(new_dir.path(), "usdcv2", USDC);

    // One block so both indexers have something to chew; `/schema` itself comes from the registry.
    let tape = Arc::new(TapeSource::new());
    let (a1, a2) = (account(1), account(2));
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

    let old_rt = indexer::spawn_nest(
        tape.clone(),
        old_dir.path().to_path_buf(),
        old_cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn old");
    let new_rt = indexer::spawn_nest(
        tape.clone(),
        new_dir.path().to_path_buf(),
        new_cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn new");

    let app = serve::two_version_router(
        serve::SharedNest::new(old_rt.state),
        "/next",
        serve::SharedNest::new(new_rt.state),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Old at root: its schema, plus a Deprecation header pointing at the successor.
    let old = client.get(format!("{base}/schema")).send().await.unwrap();
    assert_eq!(old.headers().get("deprecation").unwrap(), "true");
    assert!(old
        .headers()
        .get("link")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("successor-version"));
    let old_body = old.text().await.unwrap();
    assert!(old_body.contains("usdc__"), "old schema served at root");
    assert!(!old_body.contains("usdcv2__"), "root is the OLD version");

    // New under /next: its (different) schema, and NOT deprecated.
    let new = client
        .get(format!("{base}/next/schema"))
        .send()
        .await
        .unwrap();
    assert!(
        new.headers().get("deprecation").is_none(),
        "the new endpoint is not deprecated"
    );
    let new_body = new.text().await.unwrap();
    assert!(
        new_body.contains("usdcv2__"),
        "new schema served under /next"
    );

    old_rt.ingest.abort();
    new_rt.ingest.abort();
    server.abort();
}

/// RFC-0023 tier 1 - derive-first: the `total_supply` recipe computes ERC-20 `totalSupply()` from the
/// Transfer events already indexed (Σ minted − Σ burned), with **no eth_call**. Derive-correctness: the
/// derived value equals the hand-computed mints − burns - the thing a subgraph pays an archive node to
/// fetch, nuthatch derives for free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn total_supply_recipe_derives_mints_minus_burns() {
    use nuthatch::recipes;

    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let tape = Arc::new(TapeSource::new());
    let zero = recipes::ZERO_ADDRESS;
    let (a1, a2) = (account(1), account(2));

    // mint 1000 → a1, mint 500 → a2, burn 200 from a1, and a normal a1→a2 transfer (no supply change).
    tape.insert_block(
        1,
        transfers_block(1, 0, 1_700_000_001, USDC, &[(zero, a1.as_str(), 1000)]),
    );
    tape.insert_block(
        2,
        transfers_block(2, 0, 1_700_000_002, USDC, &[(zero, a2.as_str(), 500)]),
    );
    tape.insert_block(
        3,
        transfers_block(3, 0, 1_700_000_003, USDC, &[(a1.as_str(), zero, 200)]),
    );
    tape.insert_block(4, {
        let mut fx = transfers_block(
            4,
            0,
            1_700_000_004,
            USDC,
            &[(a1.as_str(), a2.as_str(), 100)],
        );
        pad_address_to_seal(&mut fx, USDC, 3);
        fx
    });
    tape.advance_tip_to(4);
    tape.advance_finalized_to(4);
    tape.insert_block(5, empty_block(5, 0, 1_700_000_005));
    tape.advance_tip_to(5);

    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.path().to_path_buf(),
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
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 4).await,
        "transfers did not seal"
    );

    // Derived totalSupply = 1000 + 500 − 200 = 1300. No eth_call, no archive node.
    let rows = analytics::query(dir.path(), &recipes::total_supply_select("usdc")).unwrap();
    let v = &rows[0]["total_supply"];
    let got = v
        .as_i64()
        .map(|n| n.to_string())
        .or_else(|| v.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| v.to_string());
    assert_eq!(
        got, "1300",
        "derived total_supply must equal Σ mints − Σ burns"
    );

    // Balances: a1 = 1000 − 200 − 100 = 700; a2 = 500 + 100 = 600. Two non-zero holders.
    let num = |v: &serde_json::Value| -> String {
        v.as_i64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| v.to_string())
    };
    let bals = analytics::query(dir.path(), &recipes::balances_select("usdc")).unwrap();
    let by_addr: std::collections::HashMap<String, String> = bals
        .iter()
        .map(|r| {
            (
                r["addr"].as_str().unwrap().to_lowercase(),
                num(&r["balance"]),
            )
        })
        .collect();
    assert_eq!(
        by_addr.get(&a1.to_lowercase()).map(String::as_str),
        Some("700")
    );
    assert_eq!(
        by_addr.get(&a2.to_lowercase()).map(String::as_str),
        Some("600")
    );
    assert_eq!(
        by_addr.len(),
        2,
        "exactly two non-zero holders (zero address excluded)"
    );

    // holder_count agrees.
    let hc = analytics::query(dir.path(), &recipes::holder_count_select("usdc")).unwrap();
    assert_eq!(num(&hc[0]["holders"]), "2", "derived holder_count");

    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
}

/// RFC-0023 tier 1 - the `reserves` recipe: Uniswap-V2 `getReserves()` derived as the **latest `Sync`
/// per pair**. No eth_call - the thing an AMM subgraph fetches per swap, computed from the Sync events
/// already indexed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserves_recipe_derives_latest_sync_per_pair() {
    use nuthatch::recipes;

    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_pair_nest(dir.path(), "pool", USDC);
    let tape = Arc::new(TapeSource::new());

    // Three Syncs for the pool; the current reserves are the LATEST: (1200, 3000).
    tape.insert_block(1, sync_block(1, 0, 1_700_000_001, USDC, &[(1000, 2000)]));
    tape.insert_block(2, sync_block(2, 0, 1_700_000_002, USDC, &[(1500, 2500)]));
    tape.insert_block(3, {
        let mut fx = sync_block(3, 0, 1_700_000_003, USDC, &[(1200, 3000)]);
        pad_clone_last_to_seal(&mut fx, 2);
        fx
    });
    tape.advance_tip_to(3);
    tape.advance_finalized_to(3);
    tape.insert_block(4, empty_block(4, 0, 1_700_000_004));
    tape.advance_tip_to(4);

    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.path().to_path_buf(),
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
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 3).await,
        "syncs did not seal"
    );

    let rows = analytics::query(dir.path(), &recipes::reserves_select("pool")).unwrap();
    assert_eq!(rows.len(), 1, "one pair → one reserves row");
    let num = |v: &serde_json::Value| -> String {
        v.as_i64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| v.to_string())
    };
    assert_eq!(num(&rows[0]["reserve0"]), "1200", "latest Sync reserve0");
    assert_eq!(num(&rows[0]["reserve1"]), "3000", "latest Sync reserve1");

    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
}

/// RFC-0020 slice 4 - segment reuse: a compatible update whose decode is unchanged mounts the old
/// version's sealed segments instead of re-indexing. Here a fresh nest, given ONLY the old's segments +
/// watermark (never having indexed a block itself), serves the sealed history - the true no-re-index
/// path, and a capability subgraphs structurally lack (their storage isn't content-addressed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compatible_upgrade_reuses_sealed_segments_when_decode_unchanged() {
    use nuthatch::{lifecycle, store::Store};

    let old = tempfile::tempdir().unwrap();
    let new = tempfile::tempdir().unwrap();
    // Same alias → same decode + same table names, so reuse is valid (a view/semantic-only update).
    let cfg = scaffold_nest(old.path(), "usdc", USDC);
    scaffold_nest(new.path(), "usdc", USDC);
    for d in [old.path(), new.path()] {
        std::fs::write(
            d.join("schema.json"),
            r#"{"registry_hash":"0xnest","tables":[]}"#,
        )
        .unwrap();
    }

    // Index ten blocks and seal [1,5] into the OLD version - inside a scope so every redb handle drops
    // before reuse reopens it (redb is single-writer).
    {
        let tape = Arc::new(TapeSource::new());
        let (a1, a2) = (account(1), account(2));
        for b in 1..=10u64 {
            let mut fx = transfers_block(
                b,
                0,
                1_700_000_000 + b,
                USDC,
                &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
            );
            if b == 5 {
                pad_address_to_seal(&mut fx, USDC, 4);
            }
            tape.insert_block(b, fx);
        }
        tape.advance_tip_to(10);
        let rt = indexer::spawn_nest(
            tape.clone(),
            old.path().to_path_buf(),
            cfg,
            None,
            false,
            1,
            // One window over the backfill, so a padded block is fetched once, not again as a tail.
            Some(10),
            false,
            None,
        )
        .await
        .expect("spawn old");
        let store = rt.state.store.clone();
        assert!(
            wait_until(POLL_TIMEOUT, || store
                .get_meta("last_block")
                .ok()
                .flatten()
                .as_deref()
                == Some("10"))
            .await,
            "old did not index to the tip"
        );
        tape.advance_finalized_to(5);
        tape.insert_block(11, empty_block(11, 0, 1_700_000_100));
        tape.advance_tip_to(11);
        assert!(
            wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 5).await,
            "old did not seal [1,5]"
        );
        rt.ingest.abort();
        let _ = rt.ingest.await;
        if let Some(w) = rt.alert_worker {
            w.abort();
            let _ = w.await;
        }
        drop(store);
        // redb is single-writer: the aborted tasks must actually drop their Store
        // clones before reuse_segments opens the same file. 20k padded rows made
        // this race visible in CI (#1067).
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Mount the old's sealed segments into the fresh new nest.
    match lifecycle::reuse_segments(old.path(), new.path()).unwrap() {
        lifecycle::ReuseOutcome::Reused {
            sealed_through,
            segments,
        } => {
            assert_eq!(sealed_through, 5, "watermark carried over");
            assert!(segments >= 1, "at least one segment reused");
        }
        other => panic!("expected Reused, got {other:?}"),
    }

    // The new nest now serves the reused sealed history WITHOUT ever having indexed a block.
    assert!(new.path().join("segments/manifest.json").exists());
    {
        let new_store = Store::open(&new.path().join("nuthatch.redb")).unwrap();
        assert_eq!(
            new_store.sealed_through(),
            5,
            "new resumes past the reused range"
        );
    }
    let rows = analytics::query(
        new.path(),
        &format!(
            "SELECT block_number FROM \"{}\" ORDER BY block_number",
            transfer_table("usdc")
        ),
    )
    .unwrap();
    let blocks: BTreeSet<u64> = rows
        .iter()
        .map(|r| r["block_number"].as_u64().unwrap())
        .collect();
    assert_eq!(
        blocks,
        (1..=5).collect::<BTreeSet<u64>>(),
        "the fresh new nest serves exactly the reused sealed segments"
    );
}

/// **Issues #419 and #433, on the serving surface.** A sealed segment corrupted under a *running*
/// node must reduce the table over HTTP `/sql`, not delete it - whichever way the file went bad.
///
/// The unit tests in `analytics` pin `define_views` and `run`; this pins the path an operator
/// actually hits. Startup quarantine (`seal::verify_and_quarantine`) catches a segment that is
/// already bad when the node boots, so the case that reaches serving is the one that goes bad
/// *after* boot - a disk fault, or a file replaced under a live process. That is what this drives:
/// two segments sealed and serving, one of them destroyed by `corrupt`, and the query re-run against
/// the same running server.
///
/// The assertion is the exact surviving set, not merely "the table still exists": the reduction has
/// to be confined to the blocks the dead segment carried, with the other segment and the hot tip
/// untouched. Both callers below reach it by a different mechanism, and each asserts its own premise
/// about whether the wrecked file still binds - which is the only thing that tells the two apart.
///
/// **It also pins #435: the response has to *say* it was reduced.** The two responses this helper
/// already produces are the whole of that question - one healthy, one short - and to a caller reading
/// only `rows` they are indistinguishable, which is how `SELECT SUM(value)` comes back quietly wrong.
/// Asserted here, inside the shared helper, so both corruption mechanisms below prove the flag
/// survives the real router, the real `spawn_nest` state and the real serialisation; a hand-built
/// `AppState` would prove the handler and never the wiring.
async fn a_corrupted_segment_reduces_the_table_over_http(corrupt: impl FnOnce(&std::path::Path)) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let tape = Arc::new(TapeSource::new());

    // Ten blocks, one USDC transfer each. Blocks 3 and 6 are padded so each of the
    // two sequential seals crosses SEAL_DIRECT_BATCH and cuts at that block (#1067).
    let a1 = account(1);
    let a2 = account(2);
    for b in 1..=10u64 {
        let mut fx = transfers_block(
            b,
            0,
            1_700_000_000 + b,
            USDC,
            &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
        );
        if b == 3 {
            pad_address_to_seal(&mut fx, USDC, 2);
        }
        if b == 6 {
            pad_address_to_seal(&mut fx, USDC, 2);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(10);

    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.path().to_path_buf(),
        cfg,
        None,
        false,
        1,
        // One window over the backfill, so a padded block is fetched once, not again as a tail.
        Some(10),
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let store = rt.state.store.clone();
    assert!(
        wait_until(POLL_TIMEOUT, || {
            store.get_meta("last_block").ok().flatten().as_deref() == Some("10")
        })
        .await,
        "nest did not index to the tip in time"
    );

    // Seal in two steps so the table has *two* segments - one bad segment can only be shown to
    // reduce a table if there is a surviving one to reduce it to.
    tape.advance_finalized_to(3);
    tape.insert_block(11, empty_block(11, 0, 1_700_000_100));
    tape.advance_tip_to(11);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 3).await,
        "range [1,3] did not seal in time"
    );
    tape.advance_finalized_to(6);
    tape.insert_block(12, empty_block(12, 0, 1_700_000_200));
    tape.advance_tip_to(12);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 6).await,
        "range [4,6] did not seal in time"
    );

    let table = transfer_table("usdc");
    let segs = seal::load_manifest(dir.path())
        .unwrap()
        .tables
        .get(&table)
        .cloned()
        .expect("transfer table sealed");
    assert_eq!(segs.len(), 2, "expected two sealed segments, got {segs:?}");

    // Freeze ingestion: from here the served rows are fixed, so a changed count is the corruption
    // and nothing else.
    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }

    // Serve the real router.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = serve::router(serve::SharedNest::new(rt.state));
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let q = format!("SELECT block_number FROM \"{table}\" ORDER BY block_number");

    // The Graph lane reads through the same `/sql` path but has no envelope to carry the flag, so a
    // reduced answer must be refused there rather than served as complete. Its routes exist only in a
    // `graph` build (#1440).
    #[cfg(feature = "graph")]
    {
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::write(
            dir.path().join("graph/schema.graphql"),
            "type Payment @entity { id: ID! }\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/payment.sql"),
            format!(
            "CREATE VIEW payment AS SELECT CAST(block_number AS VARCHAR) AS id FROM \"{table}\";\n"
        ),
        )
        .unwrap();
    }
    #[cfg(feature = "graph")]
    let payments_over_graphql = |client: reqwest::Client, base: String| async move {
        client
            .post(format!("{base}/graphql"))
            .json(&serde_json::json!({ "query": "{ payments(first: 1000) { id } }" }))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    };

    let blocks_over_http = |client: reqwest::Client, base: String, q: String| async move {
        let v: serde_json::Value = client
            .get(format!("{base}/sql"))
            .query(&[("q", q)])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        v
    };

    // Healthy: both segments (blocks 1..=6) plus the hot tip (7..=10).
    let healthy = blocks_over_http(client.clone(), base.clone(), q.clone()).await;
    let got: BTreeSet<u64> = healthy["rows"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|r| r["block_number"].as_u64())
        .collect();
    assert_eq!(
        got,
        (1..=10).collect::<BTreeSet<u64>>(),
        "healthy /sql must span both segments and the hot tip, got {healthy}"
    );
    // #435's control, and the half that carries the weight: a flag that is always on is worse than
    // none, because an operator learns to ignore it. The fields must also be *present* on the healthy
    // response - a caller cannot treat "absent" as "fine" without also treating an older node that
    // never reports as fine.
    assert_eq!(
        healthy["degraded"],
        serde_json::json!(false),
        "an intact nest must report itself intact: {healthy}"
    );
    assert_eq!(
        healthy["degraded_tables"],
        serde_json::json!([]),
        "and name nothing: {healthy}"
    );
    #[cfg(feature = "graph")]
    {
        let graph = payments_over_graphql(client.clone(), base.clone()).await;
        assert!(graph["errors"].is_null(), "{graph}");
        assert!(
            !graph["data"]["payments"].as_array().unwrap().is_empty(),
            "{graph}"
        );
    }

    // Destroy the segment carrying blocks [4,6], leaving the file present and the manifest untouched
    // - exactly what a bad sector or a half-written restore looks like.
    let bad = segs
        .iter()
        .find(|s| s.from_block == 4)
        .expect("a segment sealed from block 4");
    let bad_path = seal::segment_path(dir.path(), &bad.file, &bad.hash);
    corrupt(&bad_path);

    let reduced = blocks_over_http(client.clone(), base.clone(), q.clone()).await;
    let got: BTreeSet<u64> = reduced["rows"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|r| r["block_number"].as_u64())
        .collect();
    assert!(
        reduced["error"].is_null(),
        "a corrupt segment must not fault the query: {reduced}"
    );
    assert_eq!(
        got,
        [1, 2, 3, 7, 8, 9, 10].into_iter().collect::<BTreeSet<u64>>(),
        "the corrupt segment's blocks must be the *only* rows lost - the surviving segment and the \
         hot tip stay, and the table does not vanish. Got {reduced}"
    );
    // #435. Same status, same shape, three fewer blocks: without this the caller has no way to tell
    // this answer from the healthy one above, and the reduction policy #430 chose is only defensible
    // if the reduction is visible above the log.
    assert_eq!(
        reduced["degraded"],
        serde_json::json!(true),
        "a reduced answer must say it is reduced: {reduced}"
    );
    assert_eq!(
        reduced["degraded_tables"],
        serde_json::json!([table]),
        "and name the table whose totals are now understated: {reduced}"
    );
    #[cfg(feature = "graph")]
    {
        let graph = payments_over_graphql(client.clone(), base.clone()).await;
        assert!(graph["data"].is_null(), "{graph}");
        assert!(
            graph["errors"][0]["message"]
                .as_str()
                .is_some_and(|m| m.contains("degraded")),
            "a reduced answer must not reach a GraphQL client as complete: {graph}"
        );
    }

    server.abort();
}

/// #419's half: the file stops being a Parquet file at all, so `read_parquet` refuses it while the
/// view is created and the reduction happens at bind time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_segment_scribbled_over_under_a_running_node_reduces_the_table_over_http() {
    a_corrupted_segment_reduces_the_table_over_http(|path| {
        std::fs::write(path, b"this is not a parquet file").unwrap();
        assert!(
            !common::footer_reads(path),
            "this is #419's case: the wrecked file must NOT bind, or it is #433's wearing this name"
        );
    })
    .await;
}

/// #433's half, and the gap the review of PR #450 found: everything proving that fix went through
/// `query()` or `collect()` directly, so nothing showed the reduction surviving the guarded surface
/// - the watchdog, the hot union and the row cap. This drives the same wiring with a segment whose
/// footer is intact and whose data region is gone, which **binds** and dies at execution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_corrupt_segment_under_a_running_node_reduces_the_table_over_http() {
    a_corrupted_segment_reduces_the_table_over_http(|path| {
        // Destroy the data region, keep PAR1 + footer: this still binds, so #430's probe waves it
        // through and the failure lands at execution. That is #433's case, not #419's.
        let mut bytes = std::fs::read(path).unwrap();
        let len = bytes.len();
        let footer_len = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as usize;
        let end = len - 8 - footer_len;
        assert!(end > 4, "the fixture needs a data region to corrupt");
        bytes[4..end].fill(0xFF);
        std::fs::write(path, &bytes).unwrap();
        assert!(
            common::footer_reads(path),
            "the fixture must still BIND, or this is #419's case wearing #433's name"
        );
    })
    .await;
}

/// A `HotStore` that answers every real call except the hot-tip scan, which always errors. redb's own
/// scan has no controllable failure short of on-disk corruption a live, already-open handle would not
/// even see - `begin_read`, `open_table` and `t.iter()` all succeed against a healthy, in-process
/// database - so this is the deterministic way to reach the arm #472 found silent.
struct HotScanFails(std::sync::Arc<dyn nuthatch::store::HotStore>);

#[async_trait::async_trait]
impl nuthatch::store::HotStore for HotScanFails {
    fn put_entity(&self, key: &str, json: &str) -> anyhow::Result<()> {
        self.0.put_entity(key, json)
    }
    fn put_entity_if_named(
        &self,
        key: &str,
        json: &str,
        source_key: &str,
        block_hash: &str,
    ) -> anyhow::Result<bool> {
        self.0
            .put_entity_if_named(key, json, source_key, block_hash)
    }
    fn put_entities_if_named(
        &self,
        entries: &[(String, String)],
        source_key: &str,
        block_hash: &str,
    ) -> anyhow::Result<bool> {
        self.0
            .put_entities_if_named(entries, source_key, block_hash)
    }
    fn get_entity(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.0.get_entity(key)
    }
    fn count(&self) -> anyhow::Result<u64> {
        self.0.count()
    }
    fn recent(&self, limit: usize) -> anyhow::Result<Vec<String>> {
        self.0.recent(limit)
    }
    fn recent_by_table(&self, table: &str, limit: usize) -> anyhow::Result<Vec<String>> {
        self.0.recent_by_table(table, limit)
    }
    fn hot_rows_by_table(
        &self,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<serde_json::Value>>> {
        anyhow::bail!("simulated hot store failure (#472 fixture)")
    }
    fn hot_rows_by_table_bounded(
        &self,
        _max_rows: usize,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<serde_json::Value>>> {
        anyhow::bail!("simulated hot store failure (#472 fixture)")
    }
    fn entities_in_range(&self, from: u64, to: u64) -> anyhow::Result<Vec<String>> {
        self.0.entities_in_range(from, to)
    }
    fn sample_entity_keys(&self, limit: usize) -> anyhow::Result<Vec<String>> {
        self.0.sample_entity_keys(limit)
    }
    fn get_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.0.get_meta(key)
    }
    fn set_meta(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.0.set_meta(key, value)
    }
    fn meta_with_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<(String, String)>> {
        self.0.meta_with_prefix(prefix, limit)
    }
    fn count_meta_with_prefix(&self, prefix: &str) -> anyhow::Result<u64> {
        self.0.count_meta_with_prefix(prefix)
    }
    fn indexed_head(&self) -> anyhow::Result<Option<u64>> {
        self.0.indexed_head()
    }
    fn sealed_through(&self) -> u64 {
        self.0.sealed_through()
    }
    fn set_block_hash(&self, block: u64, hash: &str) -> anyhow::Result<()> {
        self.0.set_block_hash(block, hash)
    }
    fn get_block_hash(&self, block: u64) -> anyhow::Result<Option<String>> {
        self.0.get_block_hash(block)
    }
    fn set_block_timestamp(&self, block: u64, timestamp: u64) -> anyhow::Result<()> {
        self.0.set_block_timestamp(block, timestamp)
    }
    fn get_block_timestamp(&self, block: u64) -> anyhow::Result<Option<u64>> {
        self.0.get_block_timestamp(block)
    }
    fn checkpoints_desc(&self) -> anyhow::Result<Vec<(u64, String)>> {
        self.0.checkpoints_desc()
    }
    fn commit_window(
        &self,
        entities: &[(String, String)],
        checkpoint: Option<(u64, &str)>,
        last_block: u64,
    ) -> anyhow::Result<()> {
        self.0.commit_window(entities, checkpoint, last_block)
    }
    async fn commit_window_blocking(
        &self,
        entities: Vec<(String, String)>,
        outbox: Vec<String>,
        checkpoint: Option<(u64, String)>,
        last_block: u64,
    ) -> anyhow::Result<()> {
        self.0
            .commit_window_blocking(entities, outbox, checkpoint, last_block)
            .await
    }
    async fn settle_commits(&self) {
        self.0.settle_commits().await
    }
    fn rollback_to(&self, block: u64) -> anyhow::Result<u64> {
        self.0.rollback_to(block)
    }
    fn rollback_to_and_set_meta(
        &self,
        block: u64,
        meta_key: &str,
        meta_val: &str,
        outbox: &[String],
    ) -> anyhow::Result<u64> {
        self.0
            .rollback_to_and_set_meta(block, meta_key, meta_val, outbox)
    }
    fn prune_range(&self, from: u64, to: u64) -> anyhow::Result<u64> {
        self.0.prune_range(from, to)
    }
    fn prune_and_set_meta(
        &self,
        from: u64,
        to: u64,
        meta_key: &str,
        meta_val: &str,
    ) -> anyhow::Result<u64> {
        self.0.prune_and_set_meta(from, to, meta_key, meta_val)
    }
    fn claim(&self, owner: &str) -> anyhow::Result<u64> {
        self.0.claim(owner)
    }
    fn acquire_lease(&self, owner: &str, ttl_secs: u64) -> anyhow::Result<nuthatch::store::Lease> {
        self.0.acquire_lease(owner, ttl_secs)
    }
    fn renew_lease(&self, ttl_secs: u64) -> anyhow::Result<nuthatch::store::Lease> {
        self.0.renew_lease(ttl_secs)
    }
    fn release_lease(&self) -> anyhow::Result<()> {
        self.0.release_lease()
    }
    fn current_lease(&self) -> anyhow::Result<Option<nuthatch::store::Lease>> {
        self.0.current_lease()
    }
    fn current_fence(&self) -> anyhow::Result<u64> {
        self.0.current_fence()
    }
    fn held_fence(&self) -> u64 {
        self.0.held_fence()
    }
    fn outbox_push(&self, payload: &str) -> anyhow::Result<u64> {
        self.0.outbox_push(payload)
    }
    fn outbox_pending(&self, limit: usize) -> anyhow::Result<Vec<(u64, String)>> {
        self.0.outbox_pending(limit)
    }
    fn outbox_remove(&self, seq: u64) -> anyhow::Result<()> {
        self.0.outbox_remove(seq)
    }
    async fn outbox_remove_batch_blocking(&self, seqs: Vec<u64>) -> anyhow::Result<()> {
        self.0.outbox_remove_batch_blocking(seqs).await
    }
    fn outbox_len(&self) -> u64 {
        self.0.outbox_len()
    }
    fn outbox_trim(&self, max: u64) -> anyhow::Result<u64> {
        self.0.outbox_trim(max)
    }
}

/// #472: a hot-scan failure must not serve cold-only in silence and let the response read as complete.
/// Real `spawn_nest` state throughout - the store is only swapped for [`HotScanFails`] after ingestion
/// is frozen, the same point the corruption tests above swap in a damaged segment, so everything except
/// the scan itself (seal state, meta, the real router) is the genuine wiring. A hand-built `AppState`
/// would prove the handler and never that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_scan_failure_does_not_claim_completeness_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let tape = Arc::new(TapeSource::new());

    let a1 = account(1);
    let a2 = account(2);
    for b in 1..=5u64 {
        tape.insert_block(
            b,
            transfers_block(
                b,
                0,
                1_700_000_000 + b,
                USDC,
                &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
            ),
        );
    }
    tape.advance_tip_to(5);

    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.path().to_path_buf(),
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
    assert!(
        wait_until(POLL_TIMEOUT, || {
            store.get_meta("last_block").ok().flatten().as_deref() == Some("5")
        })
        .await,
        "nest did not index to the tip in time"
    );

    // Freeze ingestion, same as the corruption tests above, then swap the *serving* state onto a store
    // whose hot scan always errors - nothing has sealed yet, so every row that exists lives only in the
    // tip this is about to fail to read.
    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
    let mut state = rt.state.clone();
    state.store = Arc::new(HotScanFails(state.store.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = serve::router(serve::SharedNest::new(state));
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let table = transfer_table("usdc");
    let q = format!("SELECT block_number FROM \"{table}\"");
    let resp: serde_json::Value = client
        .get(format!("{base}/sql"))
        .query(&[("q", q)])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(
        resp["error"].is_null(),
        "a hot-scan failure must still answer (cold-only), not fault the query: {resp}"
    );
    // The whole point of #472: the response must not read as complete when the entire tip was dropped.
    assert_eq!(
        resp["tip_unavailable"],
        serde_json::json!(true),
        "a dropped tip must say so - a caller reading only `degraded`/`degraded_tables` would see \
         nothing wrong at all: {resp}"
    );
    // Nothing is sealed yet, so the honest cold-only answer is empty - not a confident, wrong count
    // invented from a tip the node never actually read.
    assert_eq!(
        resp["rows"].as_array().map(Vec::len),
        Some(0),
        "cold-only with nothing sealed must be empty: {resp}"
    );

    server.abort();
}

/// #528: the third hot-scan call site #472 did not reach. `/explain` binds caller SQL against the
/// hot tip too (RFC-0016 §3, over-budget case guarded since #293) but, unlike `/sql` and the CLI
/// backend, used to swallow every *other* scan failure into `Default::default()` with nothing
/// logged and nothing in the response - a caller got `valid: true` off sealed data alone, then hit
/// a different schema on the very query `/sql` would actually run. Same [`HotScanFails`] fixture
/// and real `spawn_nest` wiring as the `/sql` case above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_scan_failure_does_not_claim_bindable_completeness_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    let tape = Arc::new(TapeSource::new());

    let a1 = account(1);
    let a2 = account(2);
    for b in 1..=5u64 {
        tape.insert_block(
            b,
            transfers_block(
                b,
                0,
                1_700_000_000 + b,
                USDC,
                &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
            ),
        );
    }
    tape.advance_tip_to(5);

    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.path().to_path_buf(),
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
    assert!(
        wait_until(POLL_TIMEOUT, || {
            store.get_meta("last_block").ok().flatten().as_deref() == Some("5")
        })
        .await,
        "nest did not index to the tip in time"
    );

    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
    let mut state = rt.state.clone();
    state.store = Arc::new(HotScanFails(state.store.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = serve::router(serve::SharedNest::new(state));
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let table = transfer_table("usdc");
    let q = format!("SELECT block_number FROM \"{table}\"");
    let resp: serde_json::Value = client
        .get(format!("{base}/explain"))
        .query(&[("q", q)])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(
        resp["error"].is_null(),
        "a hot-scan failure must still answer (cold-only), not fault the bind: {resp}"
    );
    assert_eq!(
        resp["valid"],
        serde_json::json!(true),
        "the query binds against sealed history alone even though the tip could not be read: {resp}"
    );
    // The whole point of #528: `/explain` must not say `valid: true` as though nothing happened
    // when the entire tip - and whatever schema it might have added - was dropped.
    assert_eq!(
        resp["tip_unavailable"],
        serde_json::json!(true),
        "a dropped tip must say so on /explain too, not just /sql: {resp}"
    );

    server.abort();
}

/// **#472 + #477, as one piece - and #896 sharpened what "told apart" means.** Every #472 fixture
/// above scaffolds a single table, so `tip_unavailable` was never exercised on a nest that also had
/// a per-table `degraded_tables` opinion. Two contracts, `usdc` cold-corrupted and `arb` left
/// intact; the hot tip fails to scan on top of that.
///
/// The two signals now differ in **scope**, which is the distinction worth holding:
///
/// - `tip_unavailable` is nest-wide by construction - one hot-store scan covers every table at once
///   - so a query against the untouched table still carries it.
/// - `degraded_tables` names what **this query** reached. It reached `arb`, and `arb` is healthy,
///   so it names nothing.
///
/// Before #896 both were nest-wide and "told apart" was a statement about their contents. A query
/// surveyed every table in the manifest to produce that second answer, which cost ~62 µs per sealed
/// segment - 2.5 seconds on a real nest, before reading a row. `analytics::degraded_tables` is where
/// the nest-wide question is asked now, and it is still asked here, because the corruption has not
/// stopped mattering - only stopped being every caller's problem to discover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_scan_failure_and_a_cold_corruption_are_told_apart_on_the_healthy_table() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_two_contract_nest(dir.path(), "multi", "usdc", USDC, "arb", ARB);
    let tape = Arc::new(TapeSource::new());

    let a1 = account(1);
    let a2 = account(2);
    for b in 1..=6u64 {
        let hash = block_hash(b, 0);
        // Two logs in the same block, distinct log_index so tx_hash is unique.
        let usdc_log = transfer_log(
            USDC,
            b,
            0,
            &hash,
            a1.as_str(),
            a2.as_str(),
            (100 * b) as u128,
        );
        let arb_log = transfer_log(ARB, b, 1, &hash, a1.as_str(), a2.as_str(), (7 * b) as u128);
        let mut fx = BlockFixture {
            hash,
            timestamp: 1_700_000_000 + b,
            logs: vec![usdc_log, arb_log],
        };
        if b == 3 {
            pad_address_to_seal(&mut fx, USDC, 2);
            pad_address_to_seal(&mut fx, ARB, 2);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(6);

    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.path().to_path_buf(),
        cfg,
        None,
        false,
        1,
        // One window over the backfill, so a padded block is fetched once, not again as a tail.
        Some(6),
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let store = rt.state.store.clone();
    assert!(
        wait_until(POLL_TIMEOUT, || {
            store.get_meta("last_block").ok().flatten().as_deref() == Some("6")
        })
        .await,
        "nest did not index to the tip in time"
    );

    // Seal [1,3] on both tables, leaving [4,6] in the hot tip - same two-step shape as
    // `a_corrupted_segment_reduces_the_table_over_http`, so each table has a sealed segment to corrupt
    // (or not) independently of the other.
    tape.advance_finalized_to(3);
    tape.insert_block(7, empty_block(7, 0, 1_700_000_100));
    tape.advance_tip_to(7);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 3).await,
        "range [1,3] did not seal in time"
    );

    let usdc_table = transfer_table("usdc");
    let arb_table = transfer_table("arb");
    let manifest = seal::load_manifest(dir.path()).unwrap();
    let usdc_segs = manifest
        .tables
        .get(&usdc_table)
        .cloned()
        .expect("usdc segment sealed");
    assert_eq!(
        usdc_segs.len(),
        1,
        "expected one sealed usdc segment, got {usdc_segs:?}"
    );
    let usdc_path = seal::segment_path(dir.path(), &usdc_segs[0].file, &usdc_segs[0].hash);

    // Freeze ingestion, then inflict both faults at once - the combination #472 and #477 were each
    // fixed in isolation from: `usdc`'s cold segment goes bad, and the *serving* store's hot scan is
    // swapped for one that always errors.
    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
    std::fs::write(&usdc_path, b"not parquet, not even close").unwrap();
    let mut state = rt.state.clone();
    state.store = Arc::new(HotScanFails(state.store.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = serve::router(serve::SharedNest::new(state));
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let q = format!("SELECT block_number FROM \"{arb_table}\"");
    let resp: serde_json::Value = client
        .get(format!("{base}/sql"))
        .query(&[("q", q)])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(
        resp["error"].is_null(),
        "neither fault may turn into a query error: {resp}"
    );
    assert_eq!(
        resp["tip_unavailable"],
        serde_json::json!(true),
        "the tip loss is nest-wide, so a query against the untouched table still sees it: {resp}"
    );
    assert_eq!(
        resp["degraded_tables"],
        serde_json::json!([]),
        "the query read only the healthy table, so it has nothing to caveat: {resp}"
    );
    assert!(
        !resp["degraded"].as_bool().unwrap_or(true),
        "`degraded` is the one-bit form of `degraded_tables`, so the two must still agree: {resp}"
    );

    // And the corruption has not gone unnoticed - only stopped being this caller's problem. The
    // nest-wide sweep is where the question is asked now, and it still answers.
    let swept = nuthatch::analytics::degraded_tables(dir.path(), &[]).unwrap();
    assert!(
        swept.contains(&usdc_table),
        "the sweep must still name the corrupted table: {swept:?}"
    );
    assert!(
        !swept.contains(&arb_table),
        "and never the healthy one: {swept:?}"
    );
    // Cold-only (the tip failed to scan), and the arb segment sealed [1,3] intact - the only rows this
    // query can honestly return; nothing invented from the tip it never read.
    assert_eq!(
        resp["rows"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|r| r["block_number"].as_u64())
            .collect::<BTreeSet<u64>>(),
        (1..=3).collect::<BTreeSet<u64>>(),
        "the healthy table's own sealed segment, and nothing from the lost tip: {resp}"
    );

    server.abort();
}
