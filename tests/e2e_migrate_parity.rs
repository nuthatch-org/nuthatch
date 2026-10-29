//! **Migrating a runtime must not change a byte of what it serves** (RFC-0032 slice 1).
//!
//! The migration moves indexed history from the name-keyed layout (`nests/<name>/`) to
//! identity-keyed datasets (`data/<nid>/`). The whole design rests on that being a *move*: if it
//! ever re-derives anything, the migration is wrong, and the cheapest way for that to go wrong
//! silently is for it to look like it worked.
//!
//! So this indexes a real mounts to a sealed state, migrates it, and asserts the sealed segment
//! content-hashes and the cold query output are identical either side - then that the runtime
//! resolves the nests at their new home through the mount records.
//!
//! The unit tests in `migrate.rs` cover the plan, refusals and idempotency against fixture files.
//! This one is here because those fixtures cannot tell you whether *indexed data* survives.

mod common;

use std::path::Path;
use std::sync::Arc;

use nuthatch::runtime::{MountTable, DATA_DIR, MOUNTS_FILE, NESTS_DIR};
use nuthatch::store::{CoreHotStore as _, HotStore};
use nuthatch::{analytics, indexer, migrate, seal};

use common::tape::*;

fn dual_block(b: u64) -> BlockFixture {
    let hash = block_hash(b, 0);
    let a1 = account(1);
    let a2 = account(2);
    let mut fx = BlockFixture {
        hash: hash.clone(),
        timestamp: 1_700_000_000 + b,
        logs: vec![
            transfer_log(USDC, b, 0, &hash, &a1, &a2, (100 * b) as u128),
            transfer_log(ARB, b, 1, &hash, &a1, &a2, (200 * b) as u128),
        ],
    };
    if b == 6 {
        pad_address_to_seal(&mut fx, USDC, 5);
        pad_address_to_seal(&mut fx, ARB, 5);
    }
    fx
}

fn seg_hashes(dir: &Path, name: &str) -> Vec<String> {
    seal::load_manifest(dir)
        .unwrap()
        .tables
        .get(&transfer_table(name))
        .map(|segs| segs.iter().map(|s| s.hash.clone()).collect())
        .unwrap_or_default()
}

fn cold_rows(dir: &Path, name: &str) -> Vec<serde_json::Value> {
    analytics::query(
        dir,
        &format!(
            "SELECT * FROM \"{}\" ORDER BY block_number, log_index",
            transfer_table(name)
        ),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migrating_preserves_every_sealed_byte() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();

    // A mounts in the pre-2.0 layout: two nests on one chain, each under `nests/<name>/`.
    std::fs::write(
        root.join(MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
         rpc_urls = []\nnests = [\"usdc\", \"arb\"]\n",
    )
    .unwrap();
    let usdc_dir = root.join(NESTS_DIR).join("usdc");
    let arb_dir = root.join(NESTS_DIR).join("arb");
    std::fs::create_dir_all(&usdc_dir).unwrap();
    std::fs::create_dir_all(&arb_dir).unwrap();
    let cfg_u = scaffold_nest(&usdc_dir, "usdc", USDC);
    let cfg_a = scaffold_nest(&arb_dir, "arb", ARB);

    // Index to the tip and seal, so the migration has real history to move rather than an empty dir.
    let tape = Arc::new(TapeSource::new());
    for b in 1..=8u64 {
        tape.insert_block(b, dual_block(b));
    }
    tape.advance_tip_to(8);

    let cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![
            ("usdc".to_string(), usdc_dir.clone(), cfg_u),
            ("arb".to_string(), arb_dir.clone(), cfg_a),
        ],
        None,
        false,
        1,
        // Windows [1,4] and [5,8]: block 6's seal padding is fetched once, not again as a tail.
        Some(4),
        false,
        None,
        Arc::new(nuthatch::health::RuntimeHealth::new()),
        false,
    )
    .await
    .expect("spawn_runtime");

    let store_of = |name: &str| -> Arc<dyn HotStore> {
        cursor
            .states
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, s)| s.store.clone())
            .expect("nest present in mounts")
    };
    let stores = [store_of("usdc"), store_of("arb")];

    let landed = wait_until(POLL_TIMEOUT, || {
        stores
            .iter()
            .all(|s| s.get_meta("last_block").ok().flatten().as_deref() == Some("8"))
    })
    .await;
    assert!(landed, "nests did not index to the tip in time");

    tape.advance_finalized_to(6);
    tape.insert_block(9, empty_block(9, 0, 1_700_000_009));
    tape.advance_tip_to(9);
    let sealed = wait_until(SEAL_POLL_TIMEOUT, || {
        stores.iter().all(|s| s.sealed_through() >= 6)
    })
    .await;
    assert!(sealed, "nests did not seal [1,6] in time");

    // Record what the runtime serves *before* anything moves.
    let before: Vec<_> = [("usdc", &usdc_dir), ("arb", &arb_dir)]
        .iter()
        .map(|(name, dir)| (*name, seg_hashes(dir, name), cold_rows(dir, name)))
        .collect();
    assert!(
        before
            .iter()
            .all(|(_, h, r)| !h.is_empty() && !r.is_empty()),
        "the fixture sealed nothing, so this test would prove nothing"
    );

    // Stop the cursor before moving files out from under it.
    drop(stores);
    cursor.shutdown().await.expect("the cursor stops");

    // --- The migration. ---
    migrate::run(root, false, false).expect("migrate");

    let mounts = MountTable::load(root).expect("the migrated mounts.toml must still load");
    assert_eq!(
        mounts.mounts.len(),
        2,
        "both nests should have a mount record"
    );

    for (name, hashes_before, rows_before) in &before {
        let now = mounts.dir_for(root, name);
        assert!(
            now.starts_with(root.join(DATA_DIR)),
            "{name} was not addressed by identity: {}",
            now.display()
        );
        assert!(
            !root.join(NESTS_DIR).join(name).exists(),
            "{name} was left behind in the old layout"
        );

        // The load-bearing assertions. A migration that re-sealed, re-decoded or re-ordered anything
        // shows up here as a different content hash - which is exactly what content addressing is
        // for.
        assert_eq!(
            &seg_hashes(&now, name),
            hashes_before,
            "{name}: sealed content-hashes changed across the migration"
        );
        assert_eq!(
            &cold_rows(&now, name),
            rows_before,
            "{name}: cold query output changed across the migration"
        );
    }

    // Two different nests keep two identities - the merge path must not have swallowed one.
    assert_ne!(mounts.mounts[0].nid, mounts.mounts[1].nid);
}
