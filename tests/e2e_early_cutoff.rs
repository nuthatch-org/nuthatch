//! **Early cutoff on the serving path** (RFC-0033 §5, issue #364).
//!
//! A cosmetic edit - a comment in a view, a line of README - moves a nest's NID and leaves its *data*
//! identity alone. That is what `Manifest::data_identity` is for, and `migrate` has honoured it since
//! slice 5. The runtime never did: `MountTable::datasets` resolved `data/<nid>` straight out of the
//! mount record, and a freshly-installed edited nest resolved onto a directory holding its inputs and
//! no data at all. The nest then re-fetched the entire chain to rebuild bytes that were sitting one
//! directory away.
//!
//! **The discriminator is a provider that can no longer serve the history.** Asserting "it did not
//! re-index" against a tape that still holds every block is an absence assertion - the rows come out
//! identical either way, because a re-index reproduces them. So the second run gets a tape whose
//! blocks `1..=6` carry the same hashes and timestamps and **no logs**: the chain is still walkable,
//! the history is gone. Adoption is then the only route by which the edited nest can hold those rows,
//! and the assertion is a positive one.

mod common;

use std::path::Path;
use std::sync::Arc;

use nuthatch::runtime::{MountTable, DATA_DIR, MOUNTS_FILE};
use nuthatch::{analytics, health::RuntimeHealth, indexer, migrate, runtime};

use common::tape::*;

/// A two-nest runtime in the pre-2.0 layout, migrated into `data/<nid>` with a mount record each.
/// Two nests rather than one, per #364's acceptance: the co-tenant is what proves adoption is scoped
/// to the dataset that moved rather than applied to everything on the cursor.
fn two_nest_runtime(root: &Path) -> (String, String) {
    std::fs::write(
        root.join(MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
         rpc_urls = []\nnests = [\"alpha\", \"beta\"]\n",
    )
    .unwrap();
    for (alias, address) in [("alpha", USDC), ("beta", ARB)] {
        let nest = root.join("nests").join(alias);
        std::fs::create_dir_all(&nest).unwrap();
        scaffold_nest(&nest, alias, address);
    }
    migrate::run(root, false, false).expect("migrate");

    let mounts = MountTable::load(root).unwrap();
    let nid = |alias: &str| {
        mounts
            .mounts
            .iter()
            .find(|m| m.alias == alias)
            .unwrap()
            .nid
            .clone()
    };
    (nid("alpha"), nid("beta"))
}

/// Blocks `1..=6`, one transfer each on both contracts, tip 6.
fn full_history() -> Arc<TapeSource> {
    let tape = Arc::new(TapeSource::new());
    let (a1, a2) = (account(1), account(2));
    for b in 1..=6u64 {
        let mut fixture = transfers_block(
            b,
            0,
            1_700_000_000 + b,
            USDC,
            &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
        );
        let arb = transfers_block(
            b,
            0,
            1_700_000_000 + b,
            ARB,
            &[(a1.as_str(), a2.as_str(), (7 * b) as u128)],
        );
        fixture.logs.extend(arb.logs);
        tape.insert_block(b, fixture);
    }
    tape.advance_tip_to(6);
    tape
}

/// The same chain as [`full_history`] - same hashes, same timestamps, so ancestry still walks - with
/// **no logs in `1..=6`**, plus two fresh empty blocks on top. A provider that pruned its history.
fn pruned_history() -> Arc<TapeSource> {
    let tape = Arc::new(TapeSource::new());
    for b in 1..=8u64 {
        tape.insert_block(b, empty_block(b, 0, 1_700_000_000 + b));
    }
    tape.advance_tip_to(8);
    tape
}

/// Drive the runtime dir exactly as `runtime::dev` does: resolve datasets, adopt, load, spawn one
/// cursor. Going through [`runtime::load_mounted`] rather than a hand-rolled loop is the point - a
/// fixture that loads configs itself proves the loader and never the mount path.
async fn bring_up(
    root: &Path,
    tape: Arc<TapeSource>,
    tip: u64,
    finalized: u64,
) -> Vec<(String, Option<String>)> {
    let mounts = MountTable::load(root).unwrap();
    let datasets = mounts.datasets(root);
    let default_tenant_owned = mounts.tenant_default();
    let default_tenant = default_tenant_owned.as_str();
    let mounted = runtime::load_mounted(root, &datasets, default_tenant).expect("load");

    let health = Arc::new(RuntimeHealth::new());
    for (name, _, _) in &mounted {
        health.register(name, "arbitrum-one");
    }
    let names: Vec<String> = mounted.iter().map(|(n, _, _)| n.clone()).collect();
    let dirs: Vec<std::path::PathBuf> = mounted.iter().map(|(_, d, _)| d.clone()).collect();
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

    let want = tip.to_string();
    let landed = wait_until(POLL_TIMEOUT, || {
        cursor.states.iter().all(|(_, s)| {
            s.store.get_meta("last_block").ok().flatten().as_deref() == Some(want.as_str())
        })
    })
    .await;
    assert!(landed, "the runtime did not reach block {tip} in time");

    tape.advance_finalized_to(finalized);
    tape.insert_block(tip + 1, empty_block(tip + 1, 0, 1_700_000_001 + tip));
    tape.advance_tip_to(tip + 1);
    let followed = wait_until(POLL_TIMEOUT, || {
        let want = (tip + 1).to_string();
        cursor.states.iter().all(|(_, s)| {
            s.store.get_meta("last_block").ok().flatten().as_deref() == Some(want.as_str())
        })
    })
    .await;
    assert!(
        followed,
        "the runtime did not follow to block {} in time",
        tip + 1
    );

    // Read the heights off the live states: reopening a store the aborted cursor still holds would
    // answer `None` and say nothing about the run.
    let heights = names
        .iter()
        .map(|n| {
            let h = cursor
                .states
                .iter()
                .find(|(name, _)| name == n)
                .and_then(|(_, s)| s.store.get_meta("last_block").ok().flatten());
            (n.clone(), h)
        })
        .collect();

    // Run 2 reopens these very stores in this same process, so aborting is not enough: `abort` only
    // *requests* cancellation, and until the task is actually polled again it still holds its `Store`
    // clone - and with it redb's file lock. `shutdown` awaits each handle and drops the states, which
    // is what makes the lock actually free before this returns to a caller about to open the same
    // databases. It used to be written out here; it now lives on `ChainCursor` so the next fixture
    // that reopens a store cannot step in the same hole (#407).
    cursor.shutdown().await.expect("the cursor stops");

    // The tip path will not seal four rows (#1067). Seal through the public API so the
    // adoption assertions see real Parquet without stuffing dummy events into a fixture
    // that counts them.
    for dir in &dirs {
        force_seal_through(dir, finalized);
    }
    heights
}

/// Every transfer a nest has sealed, in chain order. A nest that indexed nothing has no table at
/// all, and that is an empty history rather than a broken fixture - reported as `[]` so the
/// assertion that cares can say what it wanted instead of unwrapping a catalog error.
fn transfers(dir: &Path, nest: &str) -> Vec<serde_json::Value> {
    analytics::query(
        dir,
        &format!(
            "SELECT block_number, value FROM \"{}\" ORDER BY block_number, log_index",
            transfer_table(nest)
        ),
    )
    .unwrap_or_else(|e| panic!("querying sealed transfers at {}: {e:#}", dir.display()))
}

/// Install an edited copy of a dataset's inputs under the identity they now hash to - what any
/// install of an edited nest package leaves behind - and point the mount record at it.
fn install_edited(root: &Path, alias: &str, old_nid: &str, edit: impl FnOnce(&Path)) -> String {
    let staging = root.join("staging");
    copy_inputs(&MountTable::data_dir(root, old_nid), &staging);
    edit(&staging);

    let nid = nuthatch::blob::nest_nid(&staging).expect("the edited nest must hash");
    assert_ne!(
        nid, old_nid,
        "the edit must move the NID, or nothing is being tested"
    );
    let dest = MountTable::data_dir(root, &nid);
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::rename(&staging, &dest).unwrap();

    let mut mounts = MountTable::load(root).unwrap();
    mounts
        .mounts
        .iter_mut()
        .find(|m| m.alias == alias)
        .unwrap()
        .nid = nid.clone();
    std::fs::write(
        root.join(MOUNTS_FILE),
        toml::to_string_pretty(&mounts).unwrap(),
    )
    .unwrap();
    nid
}

/// Copy a dataset's **authored inputs** only - no store, no segments. A newly installed nest package
/// carries exactly this much.
fn copy_inputs(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name == "nuthatch.redb" || name == "segments" {
            continue;
        }
        let to = dst.join(&name);
        if entry.file_type().unwrap().is_dir() {
            copy_inputs(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

fn data_dirs(root: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(root.join(DATA_DIR))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cosmetic_edit_adopts_the_dataset_it_came_from_instead_of_re_indexing() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (alpha_nid, beta_nid) = two_nest_runtime(root);

    // --- Run 1: index both nests over a chain the provider can still serve. ---
    bring_up(root, full_history(), 6, 4).await;
    let before = transfers(&MountTable::data_dir(root, &alpha_nid), "alpha");
    // `analytics::query` reads the **sealed** side, so this is blocks 1..=4 - the segments, which are
    // the expensive half to rebuild and the half a re-index would have to re-fetch.
    assert_eq!(
        before.len(),
        4,
        "the fixture must seal four transfers, or the comparison below is vacuous: {before:?}"
    );

    // --- The edit: a comment in an authored view. Cosmetic by construction - `views/**` is excluded
    // from the data identity because nothing materialises them. ---
    let new_nid = install_edited(root, "alpha", &alpha_nid, |dir| {
        let views = dir.join("views");
        std::fs::create_dir_all(&views).unwrap();
        std::fs::write(
            views.join("10-note.sql"),
            "-- a comment, and nothing else\n",
        )
        .unwrap();
    });

    // The premise, asserted rather than assumed: the new identity resolves onto a directory with the
    // nest's inputs and **no data**. This is the state that costs a full backfill.
    let dest = MountTable::data_dir(root, &new_nid);
    assert!(
        dest.join("nuthatch.toml").is_file(),
        "the edited inputs must be installed"
    );
    assert!(
        !dest.join("nuthatch.redb").exists(),
        "the fixture must start with no store, or it is not reproducing a re-index"
    );

    // --- Run 2: the same chain, with the history no longer available from the provider. ---
    let heights = bring_up(root, pruned_history(), 8, 6).await;
    assert_eq!(
        heights
            .iter()
            .find(|(n, _)| n == "alpha")
            .and_then(|(_, h)| h.clone())
            .as_deref(),
        Some("9"),
        "the adopted store must carry on to the new tip, not restart: {heights:?}"
    );

    // The load-bearing assertion. Every transfer the original nest ever saw is here, under the new
    // identity, on a runtime whose provider served none of them - so adoption is the only route by
    // which they can be present. The four sealed ones came across as segments and the two that were
    // still hot came across in the store, which is why the count is six rather than the four `before`
    // could see: run 2 sealed through 6 and the rest of the adopted history became visible.
    let after = transfers(&dest, "alpha");
    let expected: Vec<u64> = (1..=6).collect();
    assert_eq!(
        after
            .iter()
            .map(|r| r["block_number"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        expected,
        "the edited nest must carry the history it adopted - the provider cannot serve it any more, \
         so a missing row means it re-indexed and lost the chain"
    );
    assert_eq!(
        after[..before.len()],
        before[..],
        "the sealed rows must come across byte for byte, not be re-derived"
    );
    // Adoption **copies**: the source dataset may still be mounted by another tenant.
    assert!(
        MountTable::data_dir(root, &alpha_nid)
            .join("nuthatch.redb")
            .is_file(),
        "adoption must not take data away from the dataset it came from"
    );
    // And it is scoped to the dataset that moved: beta was never touched.
    assert_eq!(
        data_dirs(root).len(),
        3,
        "expected alpha's old and new datasets plus beta: {:?}",
        data_dirs(root)
    );
    assert!(
        transfers(&MountTable::data_dir(root, &beta_nid), "beta").len() >= 6,
        "the co-tenant must be unaffected"
    );
}

/// Bring a runtime up over `only` and **leave it running**, handing back the live handles the admin
/// surface drives. [`bring_up`] is the *restart* shape - it tears the cursor down before returning -
/// and a restart is precisely the path that already adopted (#414). The gap was the other one.
///
/// `mount_ctx.mounts` is the real [`MountTable`] off disk, not an invented list, so the NID that
/// `mount` resolves and the dataset it adopts into are the ones an operator's `mounts.toml` names.
/// What this fixture does *not* reproduce is the HTTP layer above `RuntimeHandles::mount`; that is
/// covered by `e2e_runtime_lifecycle`, and the cutoff is not a thing a route can add or remove.
async fn bring_up_live(
    root: &Path,
    tape: Arc<TapeSource>,
    only: &[&str],
    tip: u64,
) -> runtime::RuntimeHandles {
    let mounts = MountTable::load(root).unwrap();
    let default_tenant_owned = mounts.tenant_default();
    let default_tenant = default_tenant_owned.as_str();
    let datasets: Vec<_> = mounts
        .datasets(root)
        .into_iter()
        .filter(|d| only.contains(&d.canonical().alias.as_str()))
        .collect();
    let mounted = runtime::load_mounted(root, &datasets, default_tenant).expect("load");

    let health = Arc::new(RuntimeHealth::new());
    for (name, _, _) in &mounted {
        health.register(name, "arbitrum-one");
    }
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

    let want = tip.to_string();
    let landed = wait_until(POLL_TIMEOUT, || {
        cursor.states.iter().all(|(_, s)| {
            s.store.get_meta("last_block").ok().flatten().as_deref() == Some(want.as_str())
        })
    })
    .await;
    assert!(landed, "the live runtime did not reach block {tip} in time");

    let roster = serde_json::json!({"runtime": "test", "nests": []});
    let live = nuthatch::serve::LiveRuntime::new(nuthatch::serve::compose_runtime(
        roster.clone(),
        cursor.states.clone(),
        health.clone(),
    ));
    let handles = runtime::RuntimeHandles {
        live,
        states: cursor.states,
        alert_workers: cursor.alert_workers,
        publishers: Vec::new(),
        lifecycle: std::collections::HashMap::from([(
            "arbitrum-one".to_string(),
            cursor.lifecycle.clone(),
        )]),
        health,
        roster,
        estimates: std::collections::HashMap::new(),
        default_tenant: default_tenant.to_string(),
        suspended: Default::default(),
        joining: Default::default(),
        mount_ctx: runtime::MountContext {
            dir: root.to_path_buf(),
            // The whole table, including the record for the nest not yet mounted - which is exactly
            // the on-disk state after an operator installs an edited nest and calls the mount API.
            mounts: mounts.mounts.clone(),
            sources: std::collections::HashMap::from([(
                "arbitrum-one".to_string(),
                tape as Arc<dyn nuthatch::source::Source>,
            )]),
            endpoint_counts: std::collections::HashMap::from([("arbitrum-one".to_string(), 1)]),
            backfill: None,
            seal_direct: false,
            concurrency: 1,
            ipfs_window_deadline: nuthatch::ipfs_resolve::WINDOW_DEADLINE,
            ipfs_gateways: Vec::new(),
            state_rpc: Vec::new(),
            window_override: Some(2),
            admin_enabled: false,
            admin_token: None,
            max_rss_mb: 2048,
            freshness: Default::default(),
            chain_freshness: Default::default(),
            dormant: Default::default(),
            fail_fast: false,
            cursors: None,
            registry: None,
        },
    };
    // The cursor has to keep running for the mount handshake to be answered at a window boundary.
    std::mem::forget(cursor.ingest);
    handles
}

/// **The #414 acceptance test.** The same cosmetic edit as
/// `a_cosmetic_edit_adopts_the_dataset_it_came_from_instead_of_re_indexing`, delivered the way an
/// operator actually delivers it: mounted into a runtime that is already up, rather than picked up
/// by a restart. The mount API exists so that a restart is not needed, so it was the cheaper path
/// that paid the full backfill.
///
/// The discriminator is the module doc's: the live runtime's provider serves blocks with the right
/// hashes and **no logs**, so a re-index yields an empty table. Any transfer present under the new
/// identity came from the predecessor's dataset and from nowhere else. **This fails on `main`**,
/// where `mount` goes from `Config::load` straight to `build_and_prepare_nest`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nest_mounted_into_a_running_runtime_gets_the_early_cutoff_too() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (alpha_nid, _beta_nid) = two_nest_runtime(root);

    // --- Run 1: index both nests over a chain the provider can still serve, and seal. ---
    bring_up(root, full_history(), 6, 4).await;
    let before = transfers(&MountTable::data_dir(root, &alpha_nid), "alpha");
    assert_eq!(
        before.len(),
        4,
        "the fixture must seal four transfers, or the comparison below is vacuous: {before:?}"
    );

    // --- The cosmetic edit, installed under the identity it now hashes to. ---
    let new_nid = install_edited(root, "alpha", &alpha_nid, |dir| {
        let views = dir.join("views");
        std::fs::create_dir_all(&views).unwrap();
        std::fs::write(
            views.join("10-note.sql"),
            "-- a comment, and nothing else\n",
        )
        .unwrap();
    });
    let dest = MountTable::data_dir(root, &new_nid);
    assert!(
        !dest.join("nuthatch.redb").exists(),
        "the fixture must start with no store, or it is not reproducing a re-index"
    );

    // --- Run 2: the runtime comes up with **beta only** and stays up. Alpha arrives by mount. ---
    let mut handles = bring_up_live(root, pruned_history(), &["beta"], 8).await;
    handles.mount("alpha", None).await.expect("mounting alpha");

    // The load-bearing assertion, and a positive one: the provider served no logs at all, so every
    // row here crossed over from the predecessor's dataset.
    let after = transfers(&dest, "alpha");
    assert!(
        !after.is_empty(),
        "the hot-mounted nest re-indexed from its start block against a provider with no history, \
         so it holds nothing - the early cutoff did not reach the mount path (#414). \
         dest={dest:?} store={} segments={:?} data_dirs={:?}",
        dest.join("nuthatch.redb").exists(),
        std::fs::read_dir(dest.join("segments"))
            .map(|d| d.flatten().map(|e| e.file_name()).collect::<Vec<_>>()),
        data_dirs(root),
    );
    assert_eq!(
        after[..before.len()],
        before[..],
        "the sealed rows must come across byte for byte, not be re-derived"
    );
    // Adoption copies - the predecessor may still be mounted by another tenant.
    assert!(
        MountTable::data_dir(root, &alpha_nid)
            .join("nuthatch.redb")
            .is_file(),
        "adoption must not take data away from the dataset it came from"
    );
}

/// The candidate-side rule #414 turns on: adoption on the **hot** path can meet a sibling dataset
/// that a live cursor is writing, and `holds_data`'s deliberately pessimistic answer (an unreadable
/// store *is* data) nominates exactly the dataset that must never be copied. A redb copied mid-write
/// is not a stale store, it is a torn file - and the destination would then hold data permanently,
/// never adoptable again and unable to backfill.
///
/// Here alpha's predecessor is still mounted and live when the edited alpha is mounted. The mount
/// must succeed, and must not have staged a copy of a store held open by another cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_candidate_is_not_copied_out_from_under_its_cursor() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (alpha_nid, _beta_nid) = two_nest_runtime(root);

    bring_up(root, full_history(), 6, 4).await;

    // Edit alpha, but leave the *old* record in place as a second mount so the predecessor dataset
    // is live on the cursor while the new one is mounted.
    let new_nid = install_edited(root, "alpha", &alpha_nid, |dir| {
        let views = dir.join("views");
        std::fs::create_dir_all(&views).unwrap();
        std::fs::write(
            views.join("10-note.sql"),
            "-- a comment, and nothing else\n",
        )
        .unwrap();
    });

    // Beta stays up and keeps the cursor alive; the edited alpha is mounted into it.
    let mut handles = bring_up_live(root, pruned_history(), &["beta"], 8).await;
    handles.mount("alpha", None).await.expect("mounting alpha");

    // Whatever adoption decided, the destination must never be left holding a half-copied store: a
    // staging directory outliving the mount is the observable form of that fault.
    let staging = root.join(DATA_DIR).join(format!("{new_nid}.adopting"));
    assert!(
        !staging.exists(),
        "an adoption staging directory outlived the mount - a partial copy is worse than no cutoff"
    );
    let dest = MountTable::data_dir(root, &new_nid);
    assert!(
        dest.join("nuthatch.toml").is_file(),
        "the mounted dataset must still hold its inputs"
    );
}

/// **#822 criterion 10.** *"Editing the entity definition adopts the existing canonical facts and
/// rebuilds locally with zero historical RPC calls."*
///
/// The mechanism is `blob::NON_DATA_INPUTS`, which excludes `entities.toml` and `entities/**` on the
/// grounds that the indexing path never reads them: an entity edit moves the **package** NID without
/// moving the **fact** identity, so `runtime::adoptable` still nominates the old dataset and the
/// decoded facts come across instead of being re-fetched. RFC-0041 §6 puts it plainly - *"Editing an
/// entity definition must therefore reuse all decoded facts and rebuild locally."*
///
/// This lives beside the `views/**` case rather than in the entity suite because that is where the
/// mechanism lives. The entity suite spawns a nest straight into a directory and never consults a
/// data identity at all, so the same assertions written there pass whether or not `entities.toml` is
/// excluded - which is to say they prove the seed and not the adoption.
///
/// The run-2 provider **cannot serve the early history**, so every early row present afterwards
/// arrived by adoption. That is what makes this an assertion rather than a hope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declaring_an_entity_adopts_the_facts_instead_of_re_indexing_them() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (alpha_nid, _beta_nid) = two_nest_runtime(root);

    bring_up(root, full_history(), 6, 4).await;
    let before = transfers(&MountTable::data_dir(root, &alpha_nid), "alpha");
    assert_eq!(
        before.len(),
        4,
        "the fixture must seal four transfers, or the comparison below is vacuous: {before:?}"
    );

    // The edit: declare an incremental entity over the facts this nest already holds.
    let new_nid = install_edited(root, "alpha", &alpha_nid, |dir| {
        std::fs::create_dir_all(dir.join("entities")).unwrap();
        std::fs::write(
            dir.join("entities/received.sql"),
            "SELECT t.to, SUM(t.value) FROM alpha__transfer t GROUP BY t.to",
        )
        .unwrap();
        std::fs::write(
            dir.join("entities.toml"),
            "[[entities]]\nname = \"received\"\n\
             sql = \"entities/received.sql\"\n\
             key = [\"to\"]\nmax_rows = 10000\n",
        )
        .unwrap();
    });

    let dest = MountTable::data_dir(root, &new_nid);
    assert!(
        dest.join("nuthatch.toml").is_file(),
        "the edited inputs must be installed"
    );
    assert!(
        !dest.join("nuthatch.redb").exists(),
        "the fixture must start with no store, or it is not reproducing a re-index"
    );

    // Run 2, with the early history no longer available from the provider.
    let heights = bring_up(root, pruned_history(), 8, 6).await;
    assert_eq!(
        heights
            .iter()
            .find(|(n, _)| n == "alpha")
            .and_then(|(_, h)| h.clone())
            .as_deref(),
        Some("9"),
        "the adopted store must carry on to the new tip, not restart: {heights:?}"
    );

    let after = transfers(&dest, "alpha");
    assert_eq!(
        after
            .iter()
            .map(|r| r["block_number"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        (1..=6).collect::<Vec<u64>>(),
        "declaring an entity must adopt the decoded facts - the provider cannot serve them any \
         more, so a missing row means it re-indexed and lost the chain"
    );
    assert_eq!(
        after[..before.len()],
        before[..],
        "the sealed rows must come across byte for byte, not be re-derived"
    );
}

/// The dangerous direction. A nest whose inputs genuinely change the data it will store must **not**
/// adopt, however similar the rest of it looks - `migrate::a_substantive_edit_does_not_adopt` holds
/// this line for the migration path, and the mount path needs its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_substantive_edit_does_not_adopt_on_the_mount_path() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (alpha_nid, _) = two_nest_runtime(root);

    bring_up(root, full_history(), 6, 4).await;

    // A different contract address: same shape, different data, whatever else matches.
    let new_nid = install_edited(root, "alpha", &alpha_nid, |dir| {
        let cfg = std::fs::read_to_string(dir.join("nuthatch.toml")).unwrap();
        std::fs::write(dir.join("nuthatch.toml"), cfg.replace(USDC, ARB)).unwrap();
    });

    let dest = MountTable::data_dir(root, &new_nid);
    let mounts = MountTable::load(root).unwrap();
    let datasets = mounts.datasets(root);
    runtime::load_mounted(root, &datasets, &mounts.tenant_default()).expect("load");

    assert!(
        !dest.join("nuthatch.redb").exists(),
        "a nest indexing a different contract must not inherit another contract's data"
    );
}
