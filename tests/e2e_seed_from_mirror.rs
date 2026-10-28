//! Seeding a nest from a published mirror, end to end (RFC-0052 §8).
//!
//! A seeded nest must be indistinguishable from one that indexed the chain itself, and must get
//! there without asking the chain for anything the mirror already held. Both halves fail silently
//! if wrong: a tail the mirror withheld is rows missing for ever, and a resume one block early is
//! rows counted twice. So the publisher here is left in the awkward state on purpose - a final
//! segment, a provisional tail, and unsealed rows above both - and the seeded nest is compared with
//! a clean replay of the same chain.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::tape::*;
use nuthatch::indexer;

pub(crate) const PUBLISHER: &str = "seedpub";

fn transfer_at(b: u64) -> BlockFixture {
    transfers_block(
        b,
        0,
        1_700_000_000 + b,
        USDC,
        &[(account(1).as_str(), account(2).as_str(), (100 * b) as u128)],
    )
}

/// Transfers in blocks `1..=10`, block 5 padded so `1..=5` seals as a full segment, then empty
/// blocks up to `tip`.
pub(crate) fn chain(tip: u64) -> Arc<TapeSource> {
    let tape = Arc::new(TapeSource::new());
    for b in 1..=10u64 {
        let mut fx = transfer_at(b);
        if b == 5 {
            pad_address_to_seal(&mut fx, USDC, 4);
        }
        tape.insert_block(b, fx);
    }
    for b in 11..=tip {
        tape.insert_block(b, empty_block(b, 0, 1_700_000_000 + b));
    }
    tape.advance_tip_to(tip);
    tape
}

async fn spawn(dir: &Path, tape: Arc<TapeSource>, name: &str) -> indexer::NestRuntime {
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

fn balances_of(rt: &indexer::NestRuntime) -> Vec<(String, i128)> {
    rt.state.balances.flush();
    let mut v = rt.state.balances.top(1_000);
    v.sort();
    v
}

async fn run_to(dir: &Path, tape: Arc<TapeSource>, name: &str, block: u64) -> Vec<(String, i128)> {
    let rt = spawn(dir, tape, name).await;
    let store = rt.state.store.clone();
    let want = block.to_string();
    let reached = wait_until(POLL_TIMEOUT, || {
        store.get_meta("last_block").ok().flatten().as_deref() == Some(want.as_str())
    })
    .await;
    assert!(reached, "{name} did not reach block {block}");
    let balances = balances_of(&rt);
    drop(store);
    rt.shutdown().await.expect("the nest stops");
    balances
}

/// A stopped publisher holding a full segment `1..=5`, a provisional tail `6..=8`, and blocks
/// `9..=12` unsealed, mirrored to `mirror`.
pub(crate) async fn published(dir: &Path, mirror: &Path) -> nuthatch::publish::SyncReport {
    stopped_publisher(dir).await;
    nuthatch::publish::sync(dir, mirror.to_str().unwrap(), false)
        .await
        .expect("publish sync")
}

/// [`published`] before its mirror is written.
pub(crate) async fn stopped_publisher(dir: &Path) {
    let tape = chain(11);
    let rt = spawn(dir, tape.clone(), PUBLISHER).await;
    let store = rt.state.store.clone();
    assert!(
        wait_until(POLL_TIMEOUT, || {
            store.get_meta("last_block").ok().flatten().as_deref() == Some("11")
        })
        .await,
        "the publisher did not reach the tip"
    );
    tape.advance_finalized_to(5);
    tape.insert_block(12, empty_block(12, 0, 1_700_000_012));
    tape.advance_tip_to(12);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 5).await,
        "1..=5 did not seal"
    );
    assert!(
        wait_until(POLL_TIMEOUT, || {
            store.get_meta("last_block").ok().flatten().as_deref() == Some("12")
        })
        .await,
        "the publisher did not reach block 12"
    );
    drop(store);
    rt.shutdown().await.expect("the publisher stops");
    force_seal_through(dir, 8);
}

fn recipient_balance(balances: &[(String, i128)]) -> i128 {
    let recipient = account(2);
    balances
        .iter()
        .find(|(a, _)| a.eq_ignore_ascii_case(&recipient))
        .map(|(_, b)| *b)
        .expect("the recipient holds a balance")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_seeded_nest_equals_a_clean_replay_and_rereads_nothing_sealed() {
    let publisher = tempfile::tempdir().unwrap();
    let mirror = tempfile::tempdir().unwrap();
    let report = published(publisher.path(), mirror.path()).await;

    // The premise: the mirror proper withholds 6..=8, and the seed snapshot carries it.
    assert_eq!(
        report.sealed_through,
        Some(5),
        "the mirror publishes only the full segment"
    );
    let snapshot: serde_json::Value = serde_json::from_slice(
        &std::fs::read(mirror.path().join(&report.dataset).join("_seed/seed.json"))
            .expect("the publisher wrote a seed snapshot"),
    )
    .unwrap();
    assert_eq!(snapshot["indexed_from"], 1);
    assert_eq!(snapshot["complete_through"], 8);
    let table = transfer_table(PUBLISHER);
    assert_eq!(snapshot["tails"][&table]["from_block"], 6);

    let seeded = tempfile::tempdir().unwrap();
    scaffold_nest(seeded.path(), PUBLISHER, USDC);
    let seed = nuthatch::seed::seed(seeded.path(), mirror.path().to_str().unwrap())
        .await
        .expect("seed");
    assert_eq!((seed.indexed_from, seed.complete_through), (1, 8));
    assert_eq!((seed.segments, seed.fetched), (2, 2));

    let tape = chain(14);
    let got = run_to(seeded.path(), tape.clone(), PUBLISHER, 14).await;
    let asked = tape.logs_ranges();
    assert!(
        !asked.is_empty(),
        "the seeded nest must have followed the chain"
    );
    // Every window re-asks its predecessor's last blocks (#1144), a seeded nest's first included.
    let floor = 9 - indexer::FETCH_TAIL_OVERLAP;
    assert!(
        asked.iter().all(|(from, _)| *from >= floor),
        "a seeded nest asked the chain for history the mirror held: {asked:?}"
    );

    let clean = tempfile::tempdir().unwrap();
    let want = run_to(clean.path(), chain(14), "seedclean", 14).await;
    assert_eq!(
        got, want,
        "a seeded nest must hold what a clean replay holds"
    );
    // 100*b for b in 1..=10. Without the tail 6..=8 this is 3400; counted twice it is 7600.
    assert_eq!(recipient_balance(&got), 5_500);

    // Row for row, once both have sealed everything. This folds the downloaded tail into the
    // seeded nest's own rows, and leaves in its store the two blocks the overlap fetched again.
    force_seal_through(seeded.path(), 14);
    force_seal_through(clean.path(), 14);
    let rows = |dir: &Path, name: &str| {
        let sql = format!(
            "SELECT block_number, log_index FROM {} ORDER BY block_number, log_index",
            transfer_table(name)
        );
        nuthatch::analytics::query(dir, &sql).expect("the transfer table answers")
    };
    let clean_rows = rows(clean.path(), "seedclean");
    assert!(
        clean_rows.len() > 10,
        "the premise: ten transfers and the seal padding"
    );
    assert_eq!(
        rows(seeded.path(), PUBLISHER),
        clean_rows,
        "row for row, a seeded nest must hold what a clean replay holds"
    );

    let refolded = run_to(seeded.path(), chain(15), PUBLISHER, 15).await;
    assert_eq!(
        refolded, want,
        "a restart over the folded tail changed the balances"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tampered_mirror_seeds_nothing_and_a_repaired_one_resumes() {
    let publisher = tempfile::tempdir().unwrap();
    let mirror = tempfile::tempdir().unwrap();
    let report = published(publisher.path(), mirror.path()).await;
    let table_dir = mirror
        .path()
        .join(&report.dataset)
        .join(transfer_table(PUBLISHER));
    let segment = std::fs::read_dir(&table_dir)
        .unwrap()
        .next()
        .expect("one published segment")
        .unwrap()
        .path();
    let original = std::fs::read(&segment).unwrap();
    let mut tampered = original.clone();
    let mid = tampered.len() / 2;
    tampered[mid] ^= 0xff;
    std::fs::write(&segment, &tampered).unwrap();

    let seeded = tempfile::tempdir().unwrap();
    scaffold_nest(seeded.path(), PUBLISHER, USDC);
    let from = mirror.path().to_str().unwrap();
    let err = nuthatch::seed::seed(seeded.path(), from)
        .await
        .expect_err("a segment that does not hash to its address must stop the seed");
    assert!(
        format!("{err:#}").contains("does not hash to its content address"),
        "{err:#}"
    );
    let db = seeded.path().join("nuthatch.redb");
    assert!(
        !db.exists()
            || nuthatch::store::Store::open(&db)
                .unwrap()
                .get_meta("last_block")
                .unwrap()
                .is_none(),
        "a failed seed must not leave a nest that believes it has history"
    );

    std::fs::write(&segment, &original).unwrap();
    let seed = nuthatch::seed::seed(seeded.path(), from)
        .await
        .expect("seed after the mirror is repaired");
    assert_eq!(
        (seed.segments, seed.fetched),
        (2, 1),
        "the tail fetched by the failed run is kept"
    );

    let again = nuthatch::seed::seed(seeded.path(), from)
        .await
        .expect_err("a nest that holds data must not be seeded over");
    assert!(
        format!("{again:#}").contains("already indexed"),
        "{again:#}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mirror_of_partial_history_is_refused() {
    let publisher = tempfile::tempdir().unwrap();
    let mirror = tempfile::tempdir().unwrap();
    let report = published(publisher.path(), mirror.path()).await;
    let path = mirror.path().join(&report.dataset).join("_seed/seed.json");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot["indexed_from"] = 4.into();
    std::fs::write(&path, serde_json::to_vec_pretty(&snapshot).unwrap()).unwrap();

    let seeded = tempfile::tempdir().unwrap();
    scaffold_nest(seeded.path(), PUBLISHER, USDC);
    let err = nuthatch::seed::seed(seeded.path(), mirror.path().to_str().unwrap())
        .await
        .expect_err("history that begins after the declared start block is not this nest's");
    assert!(format!("{err:#}").contains("begins at block 4"), "{err:#}");
}

/// The path a public mirror actually uses: nobody stops a production nest to publish, so the seed
/// snapshot has to come from the running nest's own publisher, which cannot open the store.
#[tokio::test(flavor = "multi_thread")]
async fn a_running_nest_publishes_a_snapshot_another_can_seed_from() {
    const LIVE: &str = "seedlive";
    let dir = tempfile::tempdir().unwrap();
    let mirror = tempfile::tempdir().unwrap();
    let tape = chain(11);
    let rt = spawn(dir.path(), tape.clone(), LIVE).await;
    let store = rt.state.store.clone();
    tape.advance_finalized_to(5);
    tape.insert_block(12, empty_block(12, 0, 1_700_000_012));
    tape.advance_tip_to(12);
    assert!(
        wait_until(SEAL_POLL_TIMEOUT, || store.sealed_through() >= 5).await,
        "1..=5 did not seal"
    );

    let _publisher = nuthatch::publish::spawn(
        dir.path().to_path_buf(),
        LIVE.to_string(),
        nuthatch::publish::Settings {
            target: mirror.path().to_str().unwrap().to_string(),
            interval: std::time::Duration::from_millis(200),
            parallelism: 2,
        },
    )
    .expect("the publisher starts");
    let snapshot = || {
        std::fs::read_dir(mirror.path())
            .ok()?
            .flatten()
            .map(|d| d.path().join("_seed/seed.json"))
            .find(|p| p.exists())
    };
    assert!(
        wait_until(POLL_TIMEOUT, || snapshot().is_some()).await,
        "a running nest's publisher wrote no seed snapshot"
    );

    let seeded = tempfile::tempdir().unwrap();
    scaffold_nest(seeded.path(), LIVE, USDC);
    let seed = nuthatch::seed::seed(seeded.path(), mirror.path().to_str().unwrap())
        .await
        .expect("seed from a mirror its nest is still writing");
    assert_eq!((seed.indexed_from, seed.complete_through), (1, 5));
    drop(store);
    rt.shutdown().await.expect("the publisher's nest stops");

    let got = run_to(seeded.path(), chain(14), LIVE, 14).await;
    let clean = tempfile::tempdir().unwrap();
    let want = run_to(clean.path(), chain(14), "seedliveclean", 14).await;
    assert_eq!(
        got, want,
        "a nest seeded from a live mirror must equal a clean replay"
    );
    assert_eq!(recipient_balance(&got), 5_500);
}

/// A seed killed after it installed the catalogue and before it stamped the store would otherwise
/// leave segments that a cold `dev` indexes again on top of.
#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_seed_is_refused_by_dev_and_finished_by_seed() {
    let publisher = tempfile::tempdir().unwrap();
    let mirror = tempfile::tempdir().unwrap();
    published(publisher.path(), mirror.path()).await;
    let seeded = tempfile::tempdir().unwrap();
    scaffold_nest(seeded.path(), PUBLISHER, USDC);
    let from = mirror.path().to_str().unwrap();
    nuthatch::seed::seed(seeded.path(), from)
        .await
        .expect("seed");

    // The state a kill leaves between installing the catalogue and the final commit.
    {
        let store = nuthatch::store::Store::open(&seeded.path().join("nuthatch.redb")).unwrap();
        store
            .set_metas(
                &[(nuthatch::seed::SEED_PENDING_KEY, "x")],
                &["last_block", "sealed_through"],
            )
            .unwrap();
    }
    let cfg = scaffold_nest(seeded.path(), PUBLISHER, USDC);
    let refused = indexer::spawn_nest(
        chain(14),
        seeded.path().to_path_buf(),
        cfg,
        None,
        false,
        1,
        Some(2),
        false,
        None,
    )
    .await;
    let err = refused
        .err()
        .expect("dev must not start a half-seeded nest");
    assert!(format!("{err:#}").contains("did not finish"), "{err:#}");

    nuthatch::seed::seed(seeded.path(), from)
        .await
        .expect("seed finishes what it started");
    let got = run_to(seeded.path(), chain(14), PUBLISHER, 14).await;
    assert_eq!(recipient_balance(&got), 5_500);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_that_lost_a_tail_is_refused() {
    let publisher = tempfile::tempdir().unwrap();
    let mirror = tempfile::tempdir().unwrap();
    let report = published(publisher.path(), mirror.path()).await;
    let path = mirror.path().join(&report.dataset).join("_seed/seed.json");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot["tails"] = serde_json::json!({});
    std::fs::write(&path, serde_json::to_vec_pretty(&snapshot).unwrap()).unwrap();

    let seeded = tempfile::tempdir().unwrap();
    scaffold_nest(seeded.path(), PUBLISHER, USDC);
    let err = nuthatch::seed::seed(seeded.path(), mirror.path().to_str().unwrap())
        .await
        .expect_err("a snapshot missing a tail would seed a nest without those rows");
    assert!(format!("{err:#}").contains("own digest"), "{err:#}");
}

/// A quiet nest is sealed far past its last row. The snapshot must say so, or every seeder scans
/// the gap since that row again: data-services-nest was sixteen million blocks.
#[tokio::test(flavor = "multi_thread")]
async fn a_quiet_nest_is_complete_through_its_watermark_not_its_last_row() {
    let publisher = tempfile::tempdir().unwrap();
    let mirror = tempfile::tempdir().unwrap();
    published(publisher.path(), mirror.path()).await;
    // The rows end at block 12; 13..=20 seal empty, which moves the watermark and writes no segment.
    force_seal_through(publisher.path(), 12);
    force_seal_through(publisher.path(), 20);
    nuthatch::publish::sync(publisher.path(), mirror.path().to_str().unwrap(), false)
        .await
        .expect("publish sync");

    let seeded = tempfile::tempdir().unwrap();
    scaffold_nest(seeded.path(), PUBLISHER, USDC);
    let seed = nuthatch::seed::seed(seeded.path(), mirror.path().to_str().unwrap())
        .await
        .expect("seed");
    assert_eq!(
        seed.complete_through, 20,
        "the watermark, not the last segment at 12"
    );

    let tape = chain(24);
    let got = run_to(seeded.path(), tape.clone(), PUBLISHER, 24).await;
    let floor = 21 - indexer::FETCH_TAIL_OVERLAP;
    let asked = tape.logs_ranges();
    assert!(
        asked.iter().all(|(from, _)| *from >= floor),
        "a seeded quiet nest scanned the gap again: {asked:?}"
    );
    assert_eq!(recipient_balance(&got), 5_500);
}
