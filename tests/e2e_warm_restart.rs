//! Warm-restart rebuild, end to end (issue #150).
//!
//! Derived views are not persisted - on restart they are rebuilt from a **cold fold** over sealed
//! segments plus a **hot replay** of everything above the sealed watermark. Get the boundary wrong in
//! either direction and the result is silently incorrect rather than loudly broken:
//!
//! - fold a segment that the hot store still holds → every row in it counts **twice**;
//! - skip a range that was sealed and pruned → those rows vanish from the view.
//!
//! `analytics::cold_fold_respects_the_sealed_through_watermark` covers the first at unit level against
//! a hand-built segment. This is the fuller version the audit asked for: run a real nest, really seal
//! and prune, drop it, respawn it on the same directory, and assert the rebuilt balances are identical
//! to a nest that indexed the same chain from scratch. Only the whole pipeline can catch a boundary
//! that is off by one segment.

mod common;

use std::sync::Arc;

use common::tape::*;
use nuthatch::indexer;

/// A block carrying one transfer of `100 * b` from account 1 to account 2.
fn transfer_at(b: u64) -> BlockFixture {
    transfers_block(
        b,
        0,
        1_700_000_000 + b,
        USDC,
        &[(account(1).as_str(), account(2).as_str(), (100 * b) as u128)],
    )
}

/// A tape carrying blocks `1..=10` plus an empty block 11, tip at 11. Block 11 exists so the loop
/// processes a fresh window *after* finality moves, which is what triggers sealing.
fn tape_with_ten_transfers() -> Arc<TapeSource> {
    let tape = Arc::new(TapeSource::new());
    for b in 1..=10u64 {
        let mut fx = transfer_at(b);
        if b == 5 {
            pad_address_to_seal(&mut fx, USDC, 4);
        }
        tape.insert_block(b, fx);
    }
    tape.insert_block(11, empty_block(11, 0, 1_700_000_100));
    tape.advance_tip_to(11);
    tape
}

async fn spawn(dir: &std::path::Path, tape: Arc<TapeSource>) -> indexer::NestRuntime {
    spawn_named(dir, tape, "usdc").await
}

/// `spawn` with the nest's **name** under the caller's control.
///
/// `METRICS` is process-global and cargo runs the tests in this file in parallel, so two nests sharing
/// a name share `METRICS.nest(name)` and the process-wide aggregate behind it. A test asserting on a
/// gauge therefore reads whatever a sibling wrote - which is precisely how
/// `a_restarted_nest_reports_its_sealed_watermark_before_it_seals_again` came to report
/// `sealed_through=5` in CI while its own store held 999: the neighbour had sealed block 5.
///
/// Sequential local runs never collide, which is why it passed here and failed there.
async fn spawn_named(
    dir: &std::path::Path,
    tape: Arc<TapeSource>,
    name: &str,
) -> indexer::NestRuntime {
    let cfg = scaffold_nest(dir, name, USDC);
    indexer::spawn_nest(
        tape,
        dir.to_path_buf(),
        cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn_nest")
}

/// Balances as a sorted `(address, balance)` list - the comparable shape.
fn balances_of(rt: &indexer::NestRuntime) -> Vec<(String, i128)> {
    rt.state.balances.flush();
    let mut v = rt.state.balances.top(1_000);
    v.sort();
    v
}

/// Wait for the nest to reach `last_block == 11`.
///
/// On [`SEAL_POLL_TIMEOUT`], not [`POLL_TIMEOUT`]: block 5 is padded to
/// [`indexer::SEAL_DIRECT_BATCH`] rows so the tip path will seal it, and `last_block` does not move
/// until that window commits (#1989, #2017). Decoding and writing 20,000 rows takes ~7 s on an idle
/// debug build and well past 20 s beside the other tests in this file, which is how 3 runs in 10
/// failed "did not reach the tip" here with nothing wrong in the nest.
async fn wait_indexed(store: &dyn nuthatch::store::HotStore) -> bool {
    wait_until(SEAL_POLL_TIMEOUT, || {
        store.get_meta("last_block").ok().flatten().as_deref() == Some("11")
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_warm_restart_rebuilds_balances_identically_to_a_clean_replay() {
    // ---- The restarted nest: index, seal past finality, drop, respawn on the same dir. ----
    let dir = tempfile::tempdir().unwrap();
    let tape = tape_with_ten_transfers();
    let rt = spawn(dir.path(), tape.clone()).await;
    let store = rt.state.store.clone();
    assert!(
        wait_indexed(&store).await,
        "first run did not reach the tip"
    );

    // Finalize through block 5 so [1,5] seals to Parquet and is pruned from the hot store. This is
    // the split that makes the test meaningful: the rebuild must fold the cold half and replay the
    // hot half, counting every transfer exactly once across the seam.
    tape.advance_finalized_to(5);
    tape.insert_block(12, empty_block(12, 0, 1_700_000_200));
    tape.advance_tip_to(12);
    let sealed = wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 5).await;
    assert!(sealed, "range [1,5] did not seal in time");

    let before = balances_of(&rt);
    assert!(!before.is_empty(), "the view must hold balances to compare");
    drop(store);
    rt.shutdown().await.expect("the nest stops");

    // Respawn on the SAME directory - the warm-restart path, with a real sealed/hot split on disk.
    let restarted = spawn(dir.path(), tape.clone()).await;
    let restarted_store = restarted.state.store.clone();
    let caught_up = wait_until(POLL_TIMEOUT, || {
        restarted_store
            .get_meta("last_block")
            .ok()
            .flatten()
            .as_deref()
            == Some("12")
    })
    .await;
    assert!(caught_up, "restarted nest did not resume to the tip");
    let after = balances_of(&restarted);
    drop(restarted_store);
    restarted.shutdown().await.expect("the nest stops");

    // ---- The reference: a clean nest indexing the same chain from an empty directory. ----
    let clean_dir = tempfile::tempdir().unwrap();
    let clean_tape = tape_with_ten_transfers();
    clean_tape.advance_finalized_to(5);
    clean_tape.insert_block(12, empty_block(12, 0, 1_700_000_200));
    clean_tape.advance_tip_to(12);
    let clean = spawn(clean_dir.path(), clean_tape).await;
    let clean_store = clean.state.store.clone();
    // The padded window again, from an empty directory: the seal budget, as in `wait_indexed`.
    let clean_caught_up = wait_until(SEAL_POLL_TIMEOUT, || {
        clean_store.get_meta("last_block").ok().flatten().as_deref() == Some("12")
    })
    .await;
    assert!(clean_caught_up, "clean nest did not reach the tip");
    let clean_balances = balances_of(&clean);
    drop(clean_store);
    clean.shutdown().await.expect("the nest stops");

    // The property: a restart is invisible in the derived view.
    assert_eq!(
        after, clean_balances,
        "warm-restart balances must equal a clean replay - a mismatch here is the seal/hot boundary \
         being double-counted or dropped"
    );
    // And the restart did not disturb what was already correct before it.
    assert_eq!(
        before, after,
        "the rebuilt view must match the view the nest held before it was restarted"
    );

    // Belt and braces on the actual arithmetic: transfers of 100*b for b in 1..=10 sum to 5500, so the
    // recipient holds exactly +5500 and the sender -5500. A double-counted sealed range [1,5] would
    // show 7000/-7000 (the 1500 counted twice) - the specific corruption this test exists to catch.
    let recipient = account(2).to_ascii_lowercase();
    let got = after
        .iter()
        .find(|(a, _)| a.eq_ignore_ascii_case(&recipient))
        .map(|(_, b)| *b)
        .expect("recipient must hold a balance");
    assert_eq!(got, 5_500, "sum of 100*b for b in 1..=10, counted once");
}

/// The case the sealed-through watermark actually exists for: a crash in the window between "segment
/// written and catalogued" and "watermark advanced + hot rows pruned".
///
/// The test above deliberately does NOT cover this. On the normal path the pruning makes the watermark
/// redundant - sealed rows are gone from hot, so a cold fold that ignored the watermark still counts
/// everything once, and the test passes even with the guard removed (verified by mutation). Only the
/// crash state makes the watermark load-bearing: segments hold [1,5], the hot store *still* holds
/// [1,10], and the watermark is stale. Fold the segments there and blocks 1..=5 count twice.
///
/// The crash state is reconstructed from rows the real pipeline produced - snapshot the hot rows before
/// sealing, then put them back and rewind the watermark - so this exercises the true on-disk shape a
/// `kill -9` leaves, not an invented one.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_between_sealing_and_pruning_does_not_double_count() {
    let dir = tempfile::tempdir().unwrap();
    let tape = tape_with_ten_transfers();
    let rt = spawn(dir.path(), tape.clone()).await;
    let store = rt.state.store.clone();
    assert!(wait_indexed(&store).await, "did not reach the tip");

    // Snapshot the hot rows for [1,5] BEFORE they are sealed and pruned - these are the rows a crash
    // would leave behind, so re-inserting them later reproduces the state exactly.
    let doomed: Vec<String> = store.entities_in_range(1, 5).unwrap();
    assert_eq!(
        doomed.len(),
        indexer::SEAL_DIRECT_BATCH,
        "blocks 1..=5 should be hot before sealing, including the seal-batch padding"
    );

    // Now let the pipeline seal [1,5] and prune it, for real.
    tape.advance_finalized_to(5);
    tape.insert_block(12, empty_block(12, 0, 1_700_000_200));
    tape.advance_tip_to(12);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 5).await,
        "range [1,5] did not seal in time"
    );
    drop(store); // redb is single-writer: release our handle before reopening below
    rt.shutdown().await.expect("the nest stops");

    // Reconstruct the crash: segments still hold [1,5] (untouched on disk), the hot rows come back,
    // and the watermark never advanced.
    {
        let s = nuthatch::store::Store::open(&dir.path().join("nuthatch.redb")).unwrap();
        for raw in &doomed {
            let v: serde_json::Value = serde_json::from_str(raw).unwrap();
            let key = nuthatch::store::Store::entity_key(
                v["block_number"].as_u64().unwrap(),
                v["log_index"].as_u64().unwrap_or(0),
            );
            s.put_entity(&key, raw).unwrap();
        }
        s.set_meta("sealed_through", "0").unwrap();
        assert_eq!(
            s.entities_in_range(1, 10).unwrap().len(),
            indexer::SEAL_DIRECT_BATCH + 5,
            "the crash state must hold every row hot AND sealed"
        );
    }

    // Restart into that state and let the rebuild run.
    let restarted = spawn(dir.path(), tape).await;
    let after = balances_of(&restarted);
    restarted.shutdown().await.expect("the nest stops");

    let recipient = account(2).to_ascii_lowercase();
    let got = after
        .iter()
        .find(|(a, _)| a.eq_ignore_ascii_case(&recipient))
        .map(|(_, b)| *b)
        .expect("recipient must hold a balance");
    assert_eq!(
        got, 5_500,
        "every transfer counted exactly once. 7000 here means the cold fold ignored the stale \
         watermark and re-counted the sealed-but-still-hot range [1,5] (which sums to 1500)"
    );
}

/// **#918: a restarted nest must not advertise `sealed_through 0` while its query path knows better.**
///
/// The watermark is durable in the store's meta and `/sql` provenance has always read it correctly.
/// The Prometheus gauge, though, was only ever written by `seal_finalized` - so between a restart and
/// the next seal, `/metrics` said 0 and the query path said the truth. Two surfaces disagreeing about
/// one fact, and the wrong one is where Prometheus looks.
///
/// Found on the Lodestar box: two units restarted 28 minutes apart, one on 2.7.1 and one on
/// 3.0.0-alpha.1, both reporting 0 on `/metrics` and 499300218 in provenance, while two units
/// untouched for days reported it correctly. The version column is the control - it is not a
/// regression, it has always done this.
///
/// **The assertion is deliberately made before anything seals in the new process.** Checking after a
/// seal would pass with the fix reverted, which is presumably why nothing caught this: the gauge is
/// correct within a second or two of a restart on a busy nest, and wrong exactly when an operator's
/// alert evaluates it.
#[tokio::test]
async fn a_restarted_nest_reports_its_sealed_watermark_before_it_seals_again() {
    // A nest name nobody else in this file uses, so the labelled gauge below is this test's alone.
    // `METRICS` is process-global and cargo runs these tests in parallel; sharing the name "usdc"
    // with the neighbours is what made this report `sealed_through=5` in CI while its store held 999.
    const NEST: &str = "w918";
    let dir = tempfile::tempdir().unwrap();
    let tape = tape_with_ten_transfers();
    let rt = spawn_named(dir.path(), tape.clone(), NEST).await;
    let store = rt.state.store.clone();
    assert!(
        wait_indexed(&store).await,
        "first run did not reach the tip"
    );

    tape.advance_finalized_to(5);
    tape.insert_block(12, empty_block(12, 0, 1_700_000_200));
    tape.advance_tip_to(12);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 5).await,
        "range [1,5] did not seal in time"
    );
    // **Pin a watermark no seal in this fixture could produce.**
    //
    // Reading the gauge "immediately after respawn" is a race I cannot win: the respawned nest
    // re-seals within milliseconds and writes the gauge itself, so the fix and its absence look the
    // same. Proved it - with the seeding reverted the test still passed, twice, for two different
    // reasons. A sentinel far above anything the tape can seal removes the ambiguity: if the gauge
    // holds it, it was seeded from the store, because nothing else in the process could have put it
    // there.
    const SENTINEL: u64 = 999;
    store
        .set_meta("sealed_through", &SENTINEL.to_string())
        .expect("pin the sentinel watermark");
    let durable = store.sealed_through();
    assert_eq!(
        durable, SENTINEL,
        "the sentinel must be what the store holds"
    );

    drop(store);
    rt.shutdown().await.expect("the nest stops");

    // **Zero the gauge to simulate what a real restart gives you: a fresh process.**
    //
    // Without this the test is inert, and I proved it: with the fix reverted it still passed, because
    // `METRICS` is a process-global atomic and this test never restarts the process. The gauge still
    // held the value the *first* nest wrote when it sealed, which is exactly the number being asserted.
    // A test that cannot fail is not a test - #913's shape, produced here by me in the act of fixing
    // an instance of it.
    //
    // In production the atomic starts at 0 on every process start. This restores that condition, so
    // the assertion below is about the seeding and nothing else.
    nuthatch::metrics::METRICS.set_sealed_through(0);

    // Respawn and read the gauge immediately. No waiting for a seal - that is the whole point.
    let restarted = spawn_named(dir.path(), tape.clone(), NEST).await;
    let gauge = nuthatch::metrics::METRICS.render();
    // **The per-nest series, not the process-wide one.** The aggregate is written by every nest in the
    // process, so asserting on it makes this test a race against its own neighbours.
    let needle = format!("nuthatch_nest_sealed_through{{nest=\"{NEST}\"}} ");
    let reported: u64 = gauge
        .lines()
        .find_map(|l| l.strip_prefix(needle.as_str()))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("{needle} must appear in /metrics:\n{gauge}"));

    assert_eq!(
        reported, durable,
        "a restarted nest reported sealed_through={reported} on /metrics while its store holds \
         {durable}. An alert on this surface fires after every restart of a healthy nest, and an \
         alert that cries wolf gets muted (#918).\n\n{gauge}"
    );
    restarted.shutdown().await.expect("the nest stops");
}

/// A restarted nest must report the cursor position its store holds, before it commits again. The
/// gauge used to start at 0, so `/ready` measured a lag from the start block and, on a quiet chain,
/// declared a caught-up nest wedged once the stall grace ran out. Found by driving a fork with no
/// new blocks after a restart.
#[tokio::test]
async fn a_restarted_nest_reports_its_cursor_position_before_it_commits_again() {
    const NEST: &str = "wlast";
    let dir = tempfile::tempdir().unwrap();
    let tape = tape_with_ten_transfers();
    let rt = spawn_named(dir.path(), tape.clone(), NEST).await;
    let store = rt.state.store.clone();
    assert!(
        wait_indexed(&store).await,
        "first run did not reach the tip"
    );
    // Beyond the tape, so nothing the respawned nest commits could write it: if the gauge holds it,
    // it was seeded from the store.
    const SENTINEL: u64 = 999;
    store
        .set_meta("last_block", &SENTINEL.to_string())
        .expect("pin the sentinel position");
    drop(store);
    rt.shutdown().await.expect("the nest stops");

    let restarted = spawn_named(dir.path(), tape.clone(), NEST).await;
    let gauge = nuthatch::metrics::METRICS.render();
    let needle = format!("nuthatch_nest_last_block{{nest=\"{NEST}\"}} ");
    let reported: u64 = gauge
        .lines()
        .find_map(|l| l.strip_prefix(needle.as_str()))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("{needle} must appear in /metrics:\n{gauge}"));
    assert_eq!(
        reported, SENTINEL,
        "a restarted nest reported last_block={reported} while its store holds {SENTINEL}"
    );
    restarted.shutdown().await.expect("the nest stops");
}
