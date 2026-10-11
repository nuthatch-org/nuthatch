//! End-to-end reorg tests - the C1 coverage gap (`detect_reorg` has zero coverage today, because the
//! other test doubles answer `block_hash -> None` and make it a no-op).
//!
//! `TapeSource::reorg` rewrites the chain above a fork with new hashes+logs; the running
//! `spawn_nest` loop detects the divergence, rolls the hot store back, and re-indexes the canonical
//! replacement. A reorg *below* the sealed/finalized watermark must instead halt loudly.

mod common;

use std::sync::Arc;

use nuthatch::indexer;

use common::tape::*;

/// A canonical block `b` (variant 0): one USDC transfer, value `100*b`.
fn canonical_block(b: u64) -> BlockFixture {
    let a1 = account(1);
    let a2 = account(2);
    transfers_block(
        b,
        0,
        1_700_000_000 + b,
        USDC,
        &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
    )
}

/// A replacement block `b` (variant 1 → distinct hash): one USDC transfer with a distinct value, so a
/// naive same-key overwrite would be visibly wrong unless a real rollback + re-index happened.
fn replacement_block(b: u64) -> BlockFixture {
    let a1 = account(3);
    let a2 = account(4);
    transfers_block(
        b,
        1,
        1_700_000_500 + b,
        USDC,
        &[(a1.as_str(), a2.as_str(), (7_000 + b) as u128)],
    )
}

/// Index a fresh nest over `tape` until it reaches `last_block == tip`, returning `(runtime, store)`.
/// `within` bounds the wait: a fixture padded to seal decodes a full seal batch on the way (#1989).
async fn spawn_indexed(
    dir: &std::path::Path,
    tape: Arc<TapeSource>,
    tip: u64,
    within: std::time::Duration,
) -> (
    indexer::NestRuntime,
    std::sync::Arc<dyn nuthatch::store::HotStore>,
) {
    let cfg = scaffold_nest(dir, "usdc", USDC);
    let rt = indexer::spawn_nest(
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
    .expect("spawn_nest");
    let store = rt.state.store.clone();
    let tip_str = tip.to_string();
    // `within` is how long last_block may sit still. A padded window does not move it until the
    // window commits, so that caller passes the seal budget; a cursor that is still advancing is
    // not a hang either way (#2017).
    let landed = wait_until_progress(
        within,
        SEAL_POLL_TIMEOUT.max(within),
        || store.get_meta("last_block").ok().flatten().as_deref() == Some(tip_str.as_str()),
        || store.get_meta("last_block").ok().flatten(),
    )
    .await;
    assert!(landed, "nest did not index to block {tip} in time");
    (rt, store)
}

fn shutdown(rt: indexer::NestRuntime) {
    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
}

/// The chain the convergence property runs over. Deliberately longer than the `getLogs` window
/// (`Some(2)` in `spawn_indexed`), so a deep fork forces the ancestor-walk back across many window
/// boundaries rather than one.
const CHAIN_LEN: u64 = 24;

/// The core convergence property: after a reorg at `fork` (0 < fork < CHAIN_LEN), the reorged nest's
/// hot state converges byte-for-byte to a clean nest indexed directly over the post-reorg chain.
async fn converge_after_reorg(fork: u64) {
    assert!((1..CHAIN_LEN).contains(&fork));

    // Reorged nest: index the canonical chain 1..=CHAIN_LEN, then reorg above `fork`.
    let reorged_dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let (rt, store) =
        spawn_indexed(reorged_dir.path(), tape.clone(), CHAIN_LEN, POLL_TIMEOUT).await;

    // Rewrite blocks (fork, CHAIN_LEN] with the replacement chain.
    let replacement: Vec<BlockFixture> = ((fork + 1)..=CHAIN_LEN).map(replacement_block).collect();
    tape.reorg(fork, replacement);

    // Convergence signal: the tip block's stored row carries the replacement block hash (proving the
    // rollback + re-index actually ran, not a stale canonical row).
    let want_hash = block_hash(CHAIN_LEN, 1);
    let converged = wait_until(POLL_TIMEOUT, || {
        match store.get_entity(&nuthatch::store::Store::entity_key(CHAIN_LEN, 0)) {
            Ok(Some(raw)) => serde_json::from_str::<serde_json::Value>(&raw)
                .ok()
                .and_then(|v| v["block_hash"].as_str().map(|h| h == want_hash))
                .unwrap_or(false),
            _ => false,
        }
    })
    .await;
    assert!(converged, "fork={fork}: reorg did not reconverge in time");
    let reorged_rows = store.entities_in_range(1, CHAIN_LEN).unwrap();
    shutdown(rt);

    // Clean nest: index the post-reorg chain directly (1..=fork canonical, fork+1.. replacement).
    let clean_dir = tempfile::tempdir().unwrap();
    let clean_tape = Arc::new(TapeSource::new());
    for b in 1..=fork {
        clean_tape.insert_block(b, canonical_block(b));
    }
    for b in (fork + 1)..=CHAIN_LEN {
        clean_tape.insert_block(b, replacement_block(b));
    }
    clean_tape.advance_tip_to(CHAIN_LEN);
    let (clean_rt, clean_store) =
        spawn_indexed(clean_dir.path(), clean_tape, CHAIN_LEN, POLL_TIMEOUT).await;
    let clean_rows = clean_store.entities_in_range(1, CHAIN_LEN).unwrap();
    shutdown(clean_rt);

    assert_eq!(
        reorged_rows, clean_rows,
        "fork={fork}: reorged hot state must equal a clean run over the post-reorg chain"
    );
}

// Proptest over random fork depths. Each case drives a full reorg + a clean reference nest, so the
// case count is kept low (the loop's ~2 s idle re-poll bounds each reorg's detection latency). A
// single shared multi-thread runtime backs all cases.
use proptest::prelude::*;

/// Reorg *depth* is what the property is about, so generate the depth and derive the fork from it -
/// a uniform fork over a fixed chain is not a uniform depth, and reading it as one is how a generator
/// ends up never producing the interesting case (#291). Stratified into three equally-weighted bands
/// so a low case count still reaches the deep end: a 1-2 block flutter (what real chains do), a
/// mid-depth reorg, and a near-genesis rewrite that unwinds almost the whole chain.
///
/// `depth_bands_are_all_reachable` asserts on this strategy directly; keep them in step.
fn reorg_depth() -> impl Strategy<Value = u64> {
    prop_oneof![
        1u64..=2,                       // shallow: the everyday tip flutter
        3u64..=(CHAIN_LEN / 2),         // mid
        (CHAIN_LEN / 2 + 1)..CHAIN_LEN, // deep: unwinds most of the chain
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(9))]
    #[test]
    fn reorg_converges_to_canonical(depth in reorg_depth()) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(converge_after_reorg(CHAIN_LEN - depth));
    }
}

/// The trap #291 names in as many words: *"a property test with a generator that never produces the
/// interesting depth passes forever and proves nothing."* So assert on the generator itself. This is
/// cheap - it samples the strategy and spawns no nest - and it fails if a future edit narrows
/// `reorg_depth` back to a band that only ever flutters the tip.
///
/// Note what this does and does not prove: it proves every band is *producible*, not that a given
/// 9-case run hit all three. That is why the bands are equally weighted rather than left to a
/// uniform range, and why the past-finality case (below) is a deterministic test rather than a
/// generated one - the case that must never be missed is not left to chance.
#[test]
fn depth_bands_are_all_reachable() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::deterministic();
    let mut seen: std::collections::BTreeMap<u64, usize> = std::collections::BTreeMap::new();
    for _ in 0..600 {
        let depth = reorg_depth().new_tree(&mut runner).unwrap().current();
        *seen.entry(depth).or_default() += 1;
    }
    eprintln!("reorg_depth() distribution over 600 samples: {seen:?}");

    let band = |lo: u64, hi: u64| seen.range(lo..=hi).map(|(_, n)| *n).sum::<usize>();
    let shallow = band(1, 2);
    let mid = band(3, CHAIN_LEN / 2);
    let deep = band(CHAIN_LEN / 2 + 1, CHAIN_LEN - 1);

    assert!(shallow > 0, "no shallow (1-2 block) reorg depth generated");
    assert!(mid > 0, "no mid-depth reorg generated");
    assert!(
        deep > 0,
        "no deep reorg generated - the depths beyond the ones we used to try are exactly what \
         #291 asked for, and a generator that cannot reach them proves nothing"
    );
    assert!(
        *seen.keys().max().unwrap() >= CHAIN_LEN - 2,
        "the deepest generated reorg was {:?}, which never unwinds near-genesis",
        seen.keys().max()
    );
    assert!(
        seen.keys().all(|d| (1..CHAIN_LEN).contains(d)),
        "a generated depth falls outside the chain: {seen:?}"
    );
}

/// Every byte the sealed layer holds, keyed by file name: the segment Parquet files plus the
/// manifest. The seal layer is content-addressed, so an unchanged map is the whole of the
/// append-only-and-immutable claim - a re-seal of different data would land a new hash, and an
/// in-place edit would change the bytes under an existing one.
fn sealed_bytes(dir: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let seg_dir = dir.join(nuthatch::seal::SEGMENTS_DIR);
    let mut out = std::collections::BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(&seg_dir) else {
        return out;
    };
    for e in entries.flatten() {
        if let Ok(bytes) = std::fs::read(e.path()) {
            out.insert(e.file_name().to_string_lossy().into_owned(), bytes);
        }
    }
    out
}

/// Index 1..=10, finalize through 8, then push blocks 11..=14 so a window processes and seals
/// `[1, 8]`. Returns the runtime, its store, and the sealed bytes as they stand before any reorg.
async fn nest_with_a_sealed_range(
    dir: &std::path::Path,
    tape: &Arc<TapeSource>,
) -> (
    indexer::NestRuntime,
    std::sync::Arc<dyn nuthatch::store::HotStore>,
    std::collections::BTreeMap<String, Vec<u8>>,
) {
    for b in 1..=10u64 {
        let mut fx = canonical_block(b);
        if b == 8 {
            pad_address_to_seal(&mut fx, USDC, 7);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(10);
    let (rt, store) = spawn_indexed(dir, tape.clone(), 10, SEAL_POLL_TIMEOUT).await;

    tape.advance_finalized_to(8);
    for b in 11..=14u64 {
        tape.insert_block(b, empty_block(b, 0, 1_700_000_100 + b));
    }
    tape.advance_tip_to(14);
    let sealed = wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 8).await;
    assert!(sealed, "range [1,8] did not seal in time");
    // The cursor sits still while the padded window's seal batch is written (#2017).
    let landed = wait_until_progress(
        SEAL_POLL_TIMEOUT,
        SEAL_POLL_TIMEOUT,
        || store.get_meta("last_block").ok().flatten().as_deref() == Some("14"),
        || store.get_meta("last_block").ok().flatten(),
    )
    .await;
    assert!(landed, "nest did not index to block 14 in time");

    let before = sealed_bytes(dir);
    assert!(
        !before.is_empty(),
        "the fixture sealed nothing, so every assertion about sealed segments below would be \
         vacuous - the absence-shaped criterion that passes when the mechanism is missing"
    );
    (rt, store, before)
}

/// RFC-0031 §3.3, the half that had no test: **a reorg shallower than the finality depth must never
/// cross the seal boundary.** The existing coverage only ever ran with `sealed_through == 0`, so the
/// guard was never in a position to be wrong and the columnar layer's append-only claim was
/// unexercised - unproven rather than false, which is this sprint's whole point.
///
/// The forks here are the ones the ancestor-walk can actually resolve above the watermark: this
/// fixture's surviving checkpoints are `[14, 10, 2]`, so a fork at 11 or 13 leaves checkpoint 10
/// canonical and `rollback_reorg` is handed 10, which is above `sealed_through = 8`.
///
/// A fork at 9 is *also* strictly above the watermark, and used to halt for the same reason: the walk
/// can only answer at a checkpoint it holds, and the next one down is 2. That was issue #461. #485
/// fixed it by pinning a checkpoint at the watermark itself every time sealing advances, so the walk
/// now lands above `sealed_through` instead of falling through to 2. It is covered by
/// `a_reorg_above_the_sealed_watermark_should_not_halt` below, which runs (no `#[ignore]`), not here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reorg_above_the_seal_boundary_leaves_segments_byte_identical() {
    for fork in [11u64, 13] {
        let dir = tempfile::tempdir().unwrap();
        let tape = Arc::new(TapeSource::new());
        let (rt, store, before) = nest_with_a_sealed_range(dir.path(), &tape).await;

        // Reorg at or above the sealed watermark (8). Legal: the doomed blocks are all hot.
        let replacement: Vec<BlockFixture> = ((fork + 1)..=14).map(replacement_block).collect();
        tape.reorg(fork, replacement);

        let want_hash = block_hash(14, 1);
        let converged = wait_until(POLL_TIMEOUT, || {
            match store.get_entity(&nuthatch::store::Store::entity_key(14, 0)) {
                Ok(Some(raw)) => serde_json::from_str::<serde_json::Value>(&raw)
                    .ok()
                    .and_then(|v| v["block_hash"].as_str().map(|h| h == want_hash))
                    .unwrap_or(false),
                _ => false,
            }
        })
        .await;
        if !converged {
            // Say what actually happened rather than "it did not converge": the interesting failure
            // is the loop having *halted* on the finality guard, and that is invisible from the store.
            let halted = if rt.ingest.is_finished() {
                match rt.ingest.await {
                    Ok(Err(e)) => format!("ingest halted with: {e:#}"),
                    Ok(Ok(())) => "ingest exited cleanly".to_string(),
                    Err(e) => format!("ingest panicked: {e}"),
                }
            } else {
                "ingest still running".to_string()
            };
            panic!(
                "fork={fork}: a reorg above the sealed watermark must still converge, not halt. \
                 last_block={:?} sealed_through={} checkpoints={:?} {halted}",
                store.get_meta("last_block").ok().flatten(),
                store.sealed_through(),
                store
                    .checkpoints_desc()
                    .map(|c| c.into_iter().map(|(b, _)| b).collect::<Vec<_>>()),
            );
        }

        assert_eq!(
            sealed_bytes(dir.path()),
            before,
            "fork={fork}: a reorg above the seal boundary rewrote the sealed layer - segments are \
             append-only and immutable past finality (CLAUDE.md), so this is a design fault, not a \
             test failure"
        );
        assert!(
            store.sealed_through() >= 8,
            "fork={fork}: the sealed watermark went backwards across a legal reorg"
        );
        shutdown(rt);
    }
}

/// **Issue #461.** The fork is block 9, *strictly above* the sealed watermark of 8. Blocks 10..=14 are
/// rewritten and every one of them is hot and unsealed, so this is squarely inside the mutable range
/// the reorg model exists to repair.
///
/// The cause was not the guard, which is correct about the number it is handed. It was that
/// `detect_reorg` can only answer at a **checkpoint it holds**, and checkpoints were sparse - one per
/// processed window, not one per block. This fixture's are `[14, 10, 2]`. Fork 9 invalidates 14 and
/// 10, so the walk used to fall through to 2 and return that as the common ancestor. `rollback_reorg`
/// then read `2 < 8` and terminally faulted with "a finality violation this indexer cannot repair",
/// naming a watermark the reorg never approached.
///
/// The under-estimate is harmless *without* sealing - rolling back further than necessary just
/// re-indexes - which is exactly why the existing proptest never caught it: it ran entirely with
/// `sealed_through == 0`. Sealing is what turns a conservative answer into a fatal one.
///
/// Fixed by pinning a checkpoint at the watermark itself every time sealing advances it (`maybe_seal`),
/// so the walk always has a verifiable checkpoint to land on at or above `sealed_through` when the true
/// fork sits above it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reorg_above_the_sealed_watermark_should_not_halt() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    let (rt, store, before) = nest_with_a_sealed_range(dir.path(), &tape).await;
    assert_eq!(
        store.sealed_through(),
        8,
        "the fixture's premise: the watermark this reorg stays above"
    );

    // Fork at 9, above the watermark: blocks 10..=14 are rewritten, all hot and none sealed.
    let replacement: Vec<BlockFixture> = (10..=14).map(replacement_block).collect();
    tape.reorg(9, replacement);

    let want_hash = block_hash(14, 1);
    let converged = wait_until(POLL_TIMEOUT, || {
        match store.get_entity(&nuthatch::store::Store::entity_key(14, 0)) {
            Ok(Some(raw)) => serde_json::from_str::<serde_json::Value>(&raw)
                .ok()
                .and_then(|v| v["block_hash"].as_str().map(|h| h == want_hash))
                .unwrap_or(false),
            _ => false,
        }
    })
    .await;
    let halted = if rt.ingest.is_finished() {
        match rt.ingest.await {
            Ok(Err(e)) => format!("ingest halted with: {e:#}"),
            Ok(Ok(())) => "ingest exited cleanly".to_string(),
            Err(e) => format!("ingest panicked: {e}"),
        }
    } else {
        "ingest still running".to_string()
    };
    assert!(
        converged,
        "a reorg above the sealed watermark rewrites only hot blocks and must converge. \
         sealed_through={} checkpoints={:?} {halted}",
        store.sealed_through(),
        store
            .checkpoints_desc()
            .map(|c| c.into_iter().map(|(b, _)| b).collect::<Vec<_>>()),
    );
    assert_eq!(
        sealed_bytes(dir.path()),
        before,
        "and it must not have touched the sealed layer getting there"
    );
}

/// A reorg *below* the sealed/finalized watermark is a finality violation this model can't repair -
/// the doomed blocks are already in immutable sealed segments. The loop must halt loudly (return an
/// error), not silently corrupt.
///
/// Halting loudly is only half the claim, and the half that was tested. The other half is that it
/// halts *before touching anything*: the sealed bytes and the hot cursor must both be exactly where
/// they were. An error raised after the rollback had already run would satisfy the old assertions
/// and still have mutated a nest past finality, so the state assertions below are the ones that
/// make this test mean what its name says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reorg_below_finality_halts() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    for b in 1..=10u64 {
        let mut fx = canonical_block(b);
        if b == 8 {
            pad_address_to_seal(&mut fx, USDC, 7);
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
        Some(2),
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let store = rt.state.store.clone();
    let ingest = rt.ingest;

    // Block 8 is padded out to a seal batch, and last_block does not move until that window
    // commits, so a 20 s idle reads as a hang on a busy runner (#2017).
    let landed = wait_until_progress(
        SEAL_POLL_TIMEOUT,
        SEAL_POLL_TIMEOUT,
        || store.get_meta("last_block").ok().flatten().as_deref() == Some("10"),
        || store.get_meta("last_block").ok().flatten(),
    )
    .await;
    assert!(landed, "nest did not index to the tip in time");

    // Seal [1,8]: finalize through 8, then push two empty blocks so a window processes and seals.
    tape.advance_finalized_to(8);
    tape.insert_block(11, empty_block(11, 0, 1_700_000_111));
    tape.insert_block(12, empty_block(12, 0, 1_700_000_112));
    tape.advance_tip_to(12);
    let sealed = wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 8).await;
    assert!(sealed, "range [1,8] did not seal in time");

    // The tip moved to 12 above, and waiting only for the seal says nothing about where the *cursor*
    // got to. Without this the snapshot below races the ingest loop: it can catch `last_block` at 10,
    // 11 or 12 depending on scheduling, and the post-halt assertion then compares the settled cursor
    // against a stale snapshot and reports a rollback that never happened. Observed in CI on
    // 2026-09-02 as `left: Some("12"), right: Some("10")` under a message about a rollback, which is
    // the opposite direction to what the numbers say (#1119).
    let settled = wait_until_progress(
        POLL_TIMEOUT,
        SEAL_POLL_TIMEOUT,
        || store.get_meta("last_block").ok().flatten().as_deref() == Some("12"),
        || store.get_meta("last_block").ok().flatten(),
    )
    .await;
    assert!(
        settled,
        "nest did not index blocks 11 and 12 before the snapshot"
    );

    // Snapshot the sealed layer and the cursor *before* the violation, so "it did not touch them"
    // is an observation rather than an assumption.
    let sealed_before = sealed_bytes(dir.path());
    assert!(
        !sealed_before.is_empty(),
        "the fixture sealed nothing, so the immutability assertion below would be vacuous"
    );
    let cursor_before = store.get_meta("last_block").unwrap();
    let rows_before = store.entities_in_range(1, 12).unwrap();

    // Reorg at block 5 - below the finalized watermark (8). The replacement rewrites blocks 6..=12.
    let mut replacement: Vec<BlockFixture> = (6..=10).map(replacement_block).collect();
    replacement.push(empty_block(11, 1, 1_700_000_611));
    replacement.push(empty_block(12, 1, 1_700_000_612));
    tape.reorg(5, replacement);

    // The ingest loop must END with an error (not run forever, not exit cleanly).
    let outcome = tokio::time::timeout(POLL_TIMEOUT, ingest)
        .await
        .expect("ingest loop should have halted, not run forever");
    let inner = outcome.expect("ingest task should not panic");
    let err = inner.expect_err("a sub-finality reorg must halt with an error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("finality") || msg.contains("sealed"),
        "expected a finality-violation error, got: {msg}"
    );

    // ...and it refused *by construction*, without having mutated anything first.
    assert_eq!(
        sealed_bytes(dir.path()),
        sealed_before,
        "a sub-finality reorg rewrote the sealed layer before halting - segments past finality are \
         immutable (CLAUDE.md: 'if a change requires mutating sealed segments, the design is wrong')"
    );
    assert_eq!(
        store.get_meta("last_block").unwrap(),
        cursor_before,
        "the hot cursor moved before the refusal - the halt must come before any cursor write, \
         whichever direction it goes. A rollback into the sealed range is the feared case; a cursor \
         that advanced instead means the loop indexed past the snapshot, which is a fixture race \
         rather than a violation (#1119), and the numbers tell you which"
    );
    assert_eq!(
        store.entities_in_range(1, 12).unwrap(),
        rows_before,
        "hot rows were retracted before the refusal, leaving a nest that halted *and* lost state"
    );

    if let Some(w) = rt.alert_worker {
        w.abort();
    }
}

/// RFC-0021 §2 - **cross-cursor reorg isolation.** Two independent cursors (two chains) run in one
/// process, exactly as a multichain mounts hosts one isolated cursor per chain (each its own source,
/// stores, tip, finality, reorg boundary). A reorg on chain A must leave chain B's data
/// **byte-identical** - one chain's reorg can never reach across into another's. Isolation is by
/// construction; this proves it, and guards the CLAUDE.md per-cursor-isolation non-negotiable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reorg_on_one_chain_leaves_the_other_untouched() {
    let fork = 5u64;
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let tape_a = Arc::new(TapeSource::new());
    let tape_b = Arc::new(TapeSource::new());
    for b in 1..=10u64 {
        tape_a.insert_block(b, canonical_block(b));
        tape_b.insert_block(b, canonical_block(b));
    }
    tape_a.advance_tip_to(10);
    tape_b.advance_tip_to(10);

    let (rt_a, store_a) = spawn_indexed(dir_a.path(), tape_a.clone(), 10, POLL_TIMEOUT).await;
    let (rt_b, store_b) = spawn_indexed(dir_b.path(), tape_b.clone(), 10, POLL_TIMEOUT).await;

    // Snapshot chain B before touching chain A.
    let b_before = store_b.entities_in_range(1, 10).unwrap();

    // Reorg chain A above `fork` with distinct replacement blocks.
    let replacement: Vec<BlockFixture> = ((fork + 1)..=10).map(replacement_block).collect();
    tape_a.reorg(fork, replacement);

    // Wait until cursor A has actually reconverged (block 10 carries the replacement hash) - otherwise
    // the isolation assertion would be vacuous (nothing happened on A).
    let want_hash = block_hash(10, 1);
    let converged = wait_until(POLL_TIMEOUT, || {
        matches!(
            store_a.get_entity(&nuthatch::store::Store::entity_key(10, 0)),
            Ok(Some(raw)) if serde_json::from_str::<serde_json::Value>(&raw)
                .ok()
                .and_then(|v| v["block_hash"].as_str().map(|h| h == want_hash))
                .unwrap_or(false)
        )
    })
    .await;
    assert!(converged, "chain A did not reconverge after its reorg");

    // Chain B must be byte-identical - it never saw a reorg - and still at its tip.
    let b_after = store_b.entities_in_range(1, 10).unwrap();
    assert_eq!(
        b_before, b_after,
        "chain B's data must be untouched by chain A's reorg (per-cursor isolation)"
    );
    assert_eq!(
        store_b.get_meta("last_block").unwrap().as_deref(),
        Some("10"),
        "chain B stays at its own tip"
    );

    shutdown(rt_a);
    shutdown(rt_b);
}
