//! MountTable-vs-solo parity (RFC-0012's open acceptance item): over the SAME tape + range, two nests run
//! solo (`spawn_nest` each) must produce per-nest sealed segments byte-identical to the same two nests
//! run under one shared cursor (`spawn_runtime`), and identical query output. The shared cursor over-
//! fetches the union and demuxes by address - this proves that demux is exact.

mod common;

use std::path::Path;
use std::sync::Arc;

use nuthatch::store::{CoreHotStore as _, HotStore};
use nuthatch::{analytics, indexer, seal};

use common::tape::*;

/// A block carrying one USDC transfer (log 0) and one ARB transfer (log 1) - so both nests have rows
/// in every block and the runtime's union-fetch-then-demux is genuinely exercised.
fn dual_block(b: u64) -> BlockFixture {
    let hash = block_hash(b, 0);
    let a1 = account(1);
    let a2 = account(2);
    let logs = vec![
        transfer_log(USDC, b, 0, &hash, &a1, &a2, (100 * b) as u128),
        transfer_log(ARB, b, 1, &hash, &a1, &a2, (200 * b) as u128),
    ];
    let mut fx = BlockFixture {
        hash,
        timestamp: 1_700_000_000 + b,
        logs,
    };
    if b == 6 {
        pad_address_to_seal(&mut fx, USDC, 5);
        pad_address_to_seal(&mut fx, ARB, 5);
    }
    fx
}

/// A fresh, identically-scripted tape: blocks 1..=8 (dual transfers), tip 8, finality 0.
fn build_parity_tape() -> Arc<TapeSource> {
    let tape = Arc::new(TapeSource::new());
    for b in 1..=8u64 {
        tape.insert_block(b, dual_block(b));
    }
    tape.advance_tip_to(8);
    tape
}

/// Drive `stores` (over `tape`) to index to the tip, then seal `[1,6]`.
async fn drive_to_seal(tape: &TapeSource, stores: &[std::sync::Arc<dyn HotStore>]) {
    let landed = wait_until(POLL_TIMEOUT, || {
        stores
            .iter()
            .all(|s| s.get_meta("last_block").ok().flatten().as_deref() == Some("8"))
    })
    .await;
    assert!(landed, "nests did not all index to the tip in time");

    tape.advance_finalized_to(6);
    tape.insert_block(9, empty_block(9, 0, 1_700_000_009));
    tape.advance_tip_to(9);

    let sealed = wait_until(SEAL_POLL_TIMEOUT, || {
        stores.iter().all(|s| s.sealed_through() >= 6)
    })
    .await;
    assert!(sealed, "nests did not all seal [1,6] in time");
}

/// The sealed segment content-hashes for `{name}__transfer` in `dir`.
fn seg_hashes(dir: &Path, name: &str) -> Vec<String> {
    seal::load_manifest(dir)
        .unwrap()
        .tables
        .get(&transfer_table(name))
        .map(|segs| segs.iter().map(|s| s.hash.clone()).collect())
        .unwrap_or_default()
}

/// The cold (sealed) rows of `{name}__transfer` in `dir`, ordered.
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
async fn roost_is_byte_identical_to_solo() {
    // --- Solo: two independent nests over one shared tape. ---
    let solo_tape = build_parity_tape();
    let solo_u = tempfile::tempdir().unwrap();
    let solo_a = tempfile::tempdir().unwrap();
    let cfg_u = scaffold_nest(solo_u.path(), "usdc", USDC);
    let cfg_a = scaffold_nest(solo_a.path(), "arb", ARB);

    // `--window 4` throughout: windows [1,4] and [5,8], so block 6's seal padding is fetched once,
    // not again as the next window's tail.
    let rt_u = indexer::spawn_nest(
        solo_tape.clone(),
        solo_u.path().to_path_buf(),
        cfg_u,
        None,
        false,
        1,
        Some(4),
        false,
        None,
    )
    .await
    .expect("spawn_nest usdc");
    let rt_a = indexer::spawn_nest(
        solo_tape.clone(),
        solo_a.path().to_path_buf(),
        cfg_a,
        None,
        false,
        1,
        Some(4),
        false,
        None,
    )
    .await
    .expect("spawn_nest arb");

    drive_to_seal(
        &solo_tape,
        &[rt_u.state.store.clone(), rt_a.state.store.clone()],
    )
    .await;

    // --- MountTable: the same two nests behind one shared cursor over an identical tape. ---
    let roost_tape = build_parity_tape();
    let roost_u = tempfile::tempdir().unwrap();
    let roost_a = tempfile::tempdir().unwrap();
    let rcfg_u = scaffold_nest(roost_u.path(), "usdc", USDC);
    let rcfg_a = scaffold_nest(roost_a.path(), "arb", ARB);

    let cursor = indexer::spawn_runtime(
        roost_tape.clone(),
        vec![
            ("usdc".to_string(), roost_u.path().to_path_buf(), rcfg_u),
            ("arb".to_string(), roost_a.path().to_path_buf(), rcfg_a),
        ],
        None,
        false,
        1,
        Some(4),
        false,
        None,
        std::sync::Arc::new(nuthatch::health::RuntimeHealth::new()),
        // Fail-fast off: this parity run must exercise the same quarantine-capable path an operator
        // gets by default (RFC-0026), not a fail-stop variant of it.
        false,
    )
    .await
    .expect("spawn_runtime");
    let (states, roost_ingest, roost_workers) =
        (cursor.states, cursor.ingest, cursor.alert_workers);

    let store_of = |name: &str| -> std::sync::Arc<dyn HotStore> {
        states
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, s)| s.store.clone())
            .expect("nest present in mounts")
    };
    drive_to_seal(&roost_tape, &[store_of("usdc"), store_of("arb")]).await;

    // --- Parity: per-nest sealed content-hashes byte-identical solo-vs-mounts. ---
    for name in ["usdc", "arb"] {
        let solo_dir = if name == "usdc" {
            solo_u.path()
        } else {
            solo_a.path()
        };
        let roost_dir = if name == "usdc" {
            roost_u.path()
        } else {
            roost_a.path()
        };

        let solo_h = seg_hashes(solo_dir, name);
        let roost_h = seg_hashes(roost_dir, name);
        assert!(!solo_h.is_empty(), "{name}: solo produced no segments");
        assert_eq!(
            solo_h, roost_h,
            "{name}: sealed content-hashes must be byte-identical solo-vs-mounts"
        );

        assert_eq!(
            cold_rows(solo_dir, name),
            cold_rows(roost_dir, name),
            "{name}: cold query output must be identical solo-vs-mounts"
        );
    }

    rt_u.ingest.abort();
    rt_a.ingest.abort();
    roost_ingest.abort();
    for (_, w) in roost_workers {
        w.abort();
    }
}
