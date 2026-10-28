//! **Unmount/remount is free; prune is what costs you** (RFC-0032 §5, slice 4).
//!
//! Deferred collection only earns its keep if remounting really does avoid a backfill. That is
//! asserted here by *counting what the source was asked for*, not by looking at a block number and
//! deciding it looks about right: a remount that quietly re-indexed would leave the same rows behind
//! and the same watermark, and would be invisible to every assertion except this one.
//!
//! The contrast is the point. The same fixture, remounted:
//!   - **with its data still there** costs no backfill at all
//!   - **after a prune** costs a full one
//!
//! If those two numbers are ever equal, deferred collection is buying nothing.

mod common;

use std::path::Path;
use std::sync::Arc;

use nuthatch::runtime::{Mount, MountTable, DATA_DIR, MOUNTS_FILE};
use nuthatch::store::CoreHotStore as _;
use nuthatch::{health::RuntimeHealth, indexer, migrate, prune, seal};

use common::tape::*;

const BLOCKS: u64 = 12;

fn write_roost(root: &Path, nests: &str) {
    std::fs::write(
        root.join(MOUNTS_FILE),
        format!(
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
             rpc_urls = []\nnests = [{nests}]\n"
        ),
    )
    .unwrap();
}

fn fresh_tape() -> Arc<TapeSource> {
    let tape = Arc::new(TapeSource::new());
    let a1 = account(1);
    let a2 = account(2);
    for b in 1..=BLOCKS {
        let mut fx = transfers_block(
            b,
            0,
            1_700_000_000 + b,
            USDC,
            &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
        );
        if b == BLOCKS - 2 {
            pad_address_to_seal(&mut fx, USDC, (BLOCKS - 3) as usize);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(BLOCKS);
    tape
}

/// Bring the runtime's single dataset up over a fresh tape, index to the tip, seal past finality, then
/// stop. Returns how many `logs` calls that cost and where the data landed.
async fn index_once(root: &Path) -> (usize, std::path::PathBuf, u64) {
    let mounts = MountTable::load(root).unwrap();
    let datasets = mounts.datasets(root);
    assert_eq!(
        datasets.len(),
        1,
        "fixture should mount exactly one dataset"
    );
    let ds = &datasets[0];
    let cfg = nuthatch::config::Config::load(&ds.dir).unwrap();

    let tape = fresh_tape();
    let health = Arc::new(RuntimeHealth::new());
    health.register(&ds.canonical().alias, "arbitrum-one");
    let cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![(ds.canonical().alias.clone(), ds.dir.clone(), cfg)],
        None,
        false,
        1,
        // Several windows, as the fetch count below needs, with block 10 fetched once, not as a tail.
        Some(4),
        false,
        None,
        health,
        false,
    )
    .await
    .expect("spawn_runtime");

    let store = cursor.states[0].1.store.clone();
    // `>=`, not `==`. A **remounted** dataset starts out already *past* this tape's tip: the previous
    // run appended one block before sealing and followed it, so `last_block` is `BLOCKS + 1` before
    // this run fetches anything. Waiting for equality hangs forever on precisely the case this test
    // exists to prove - which is a useful reminder that a resumed store being ahead is the feature
    // working, not a fault.
    let landed = wait_until(POLL_TIMEOUT, || {
        store
            .get_meta("last_block")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|b| b >= BLOCKS)
    })
    .await;
    assert!(
        landed,
        "did not catch up in time (last_block={:?}, want >= {BLOCKS})",
        store.get_meta("last_block")
    );

    tape.advance_finalized_to(BLOCKS - 2);
    tape.insert_block(BLOCKS + 1, empty_block(BLOCKS + 1, 0, 1_700_000_100));
    tape.advance_tip_to(BLOCKS + 1);
    let sealed = wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= BLOCKS - 2).await;
    assert!(sealed, "did not seal in time");
    let watermark = store.sealed_through();

    drop(store);
    cursor.shutdown().await.expect("the cursor stops");
    (tape.logs_call_count(), ds.dir.clone(), watermark)
}

fn segment_hashes(dir: &Path) -> Vec<String> {
    seal::load_manifest(dir)
        .unwrap()
        .tables
        .get(&transfer_table("usdc"))
        .map(|s| s.iter().map(|x| x.hash.clone()).collect())
        .unwrap_or_default()
}

fn unmount(root: &Path) {
    let mut mounts = MountTable::load(root).unwrap();
    mounts.mounts.clear();
    mounts.runtime.nests = vec!["placeholder".into()];
    std::fs::write(
        root.join(MOUNTS_FILE),
        toml::to_string_pretty(&mounts).unwrap(),
    )
    .unwrap();
}

fn remount(root: &Path, nid: &str) {
    let mut mounts = MountTable::load(root).unwrap();
    mounts.runtime.nests.clear();
    mounts.mounts = vec![Mount {
        tenant: "default".into(),
        alias: "usdc".into(),
        nid: nid.to_string(),
        sql: Default::default(),
        queries: Vec::new(),
        publish: None,
        limits: Default::default(),
        #[cfg(feature = "counter")]
        counter: None,
    }];
    std::fs::write(
        root.join(MOUNTS_FILE),
        toml::to_string_pretty(&mounts).unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remounting_costs_nothing_until_you_prune() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();

    write_roost(root, "\"usdc\"");
    let nest = root.join("nests").join("usdc");
    std::fs::create_dir_all(&nest).unwrap();
    scaffold_nest(&nest, "usdc", USDC);
    migrate::run(root, false, false).expect("migrate");
    let nid = MountTable::load(root).unwrap().mounts[0].nid.clone();

    // --- First run: the backfill we are trying never to repeat. ---
    let (first_calls, data_dir, watermark) = index_once(root).await;
    let hashes = segment_hashes(&data_dir);
    assert!(
        first_calls > 0 && !hashes.is_empty(),
        "the fixture indexed nothing"
    );

    // --- Unmount. The record goes; the data does not. ---
    unmount(root);
    assert!(
        data_dir.is_dir(),
        "unmount deleted the dataset - collection must be deferred (§5)"
    );
    let orphans = prune::collectable(root).unwrap();
    assert_eq!(
        orphans.len(),
        1,
        "the unmounted dataset should be collectable"
    );
    assert_eq!(orphans[0].nid, nid);
    assert!(
        orphans[0].bytes > 0,
        "prune reported a dataset holding nothing"
    );

    // --- Remount, and measure. This is the assertion the slice exists for. ---
    remount(root, &nid);
    assert!(
        prune::collectable(root).unwrap().is_empty(),
        "a remounted dataset must stop being collectable"
    );
    let (remount_calls, same_dir, remount_watermark) = index_once(root).await;

    assert_eq!(
        same_dir, data_dir,
        "the remount resolved to a different directory"
    );
    assert_eq!(
        remount_watermark, watermark,
        "the remount moved the seal watermark - it re-sealed history it already had"
    );
    assert_eq!(
        segment_hashes(&data_dir),
        hashes,
        "the remount changed sealed content-hashes - it re-indexed"
    );
    assert!(
        remount_calls < first_calls,
        "remounting cost as much as the first backfill ({remount_calls} vs {first_calls} log \
         fetches) - the data was re-indexed, and deferred collection is buying nothing"
    );

    // --- Prune, then remount again. Now it *does* cost a backfill, which is the contrast that
    // makes the assertion above mean something. ---
    unmount(root);
    prune::run(root, false).unwrap();
    assert!(data_dir.is_dir(), "listing deleted the dataset");
    prune::run(root, true).unwrap();
    assert!(!data_dir.exists(), "--yes did not remove the dataset");

    // The nest's inputs went with it, so re-mounting means re-installing. Rebuild from scratch and
    // confirm the identity is the same - the NID is a property of the inputs, not of the history.
    write_roost(root, "\"usdc\"");
    let nest = root.join("nests").join("usdc");
    std::fs::create_dir_all(&nest).unwrap();
    scaffold_nest(&nest, "usdc", USDC);
    migrate::run(root, false, false).expect("re-migrate");
    assert_eq!(
        MountTable::load(root).unwrap().mounts[0].nid,
        nid,
        "the same inputs must yield the same identity after a prune"
    );

    let (after_prune_calls, _, _) = index_once(root).await;
    assert!(
        after_prune_calls > remount_calls,
        "re-indexing after a prune cost no more than a remount ({after_prune_calls} vs \
         {remount_calls}) - then the first assertion proves nothing"
    );
    assert!(
        root.join(DATA_DIR).join(&nid).is_dir(),
        "the rebuilt dataset should be back under its identity"
    );
}
