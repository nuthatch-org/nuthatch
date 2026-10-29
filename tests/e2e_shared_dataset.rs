//! **Two mounts, one dataset, one backfill** (RFC-0032 slice 2).
//!
//! The payoff of keying data by nest identity is that mounting the same nest twice costs nothing:
//! one store, one place in the cursor, one indexing pass, two routes. This test is the proof, and it
//! is deliberately built around what would go *wrong* rather than what should go right.
//!
//! A note on the acceptance criterion RFC-0032 §9 proposed - "assert the RPC request count is that of
//! one backfill". It is weaker than it sounds, because RFC-0012's shared cursor already fetches the
//! **union** of its nests' logs once per window and demuxes: two *distinct* nests on one cursor
//! already cost one nest's worth of RPC chatter. So the request count is asserted here as a
//! regression guard against a second fetch appearing, not as the discriminator. The discriminators
//! are the ones below it: one dataset directory, one store, and identical bytes through both doors.
//!
//! What *would* have happened without this slice is worth stating: iterating aliases instead of
//! datasets calls `Store::open` on the same redb file twice, and redb refuses. The old shape did not
//! silently double-index - it could not come up at all.

mod common;

use std::path::Path;
use std::sync::Arc;

use nuthatch::runtime::{Dataset, MountTable, DATA_DIR, MOUNTS_FILE};
use nuthatch::store::CoreHotStore as _;
use nuthatch::{analytics, health::RuntimeHealth, indexer, migrate, runtime};

use common::tape::*;

/// The tip the fixture backfills to before anything advances it: the boundary between "this was the
/// backfill" and "this was tip-following", which is what the fetch guard below is measured over.
const TIP: u64 = 6;

/// A one-nest mounts in the pre-2.0 layout, migrated, then given a second alias onto the same
/// identity - which is exactly what a second tenant mounting the same nest produces.
fn two_mounts_one_nest(root: &Path) -> String {
    std::fs::write(
        root.join(MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
         rpc_urls = []\nnests = [\"primary\"]\n",
    )
    .unwrap();
    let nest = root.join("nests").join("primary");
    std::fs::create_dir_all(&nest).unwrap();
    scaffold_nest(&nest, "primary", USDC);

    migrate::run(root, false, false).expect("migrate");
    let nid = MountTable::load(root).unwrap().mounts[0].nid.clone();

    // The second mount: a different alias, the same identity. No second directory, no second copy.
    let mut mounts = MountTable::load(root).unwrap();
    mounts.runtime.nests.push("mirror".to_string());
    mounts.mounts.push(nuthatch::runtime::Mount {
        tenant: mounts.tenant_default(),
        alias: "mirror".to_string(),
        nid: nid.clone(),
        sql: Default::default(),
        queries: Vec::new(),
        publish: None,
        limits: Default::default(),
        #[cfg(feature = "counter")]
        counter: None,
    });
    std::fs::write(
        root.join(MOUNTS_FILE),
        toml::to_string_pretty(&mounts).unwrap(),
    )
    .unwrap();
    nid
}

fn rows(dir: &Path, nest: &str) -> Vec<serde_json::Value> {
    analytics::query(
        dir,
        &format!(
            "SELECT * FROM \"{}\" ORDER BY block_number, log_index",
            transfer_table(nest)
        ),
    )
    .unwrap()
}

/// Drive a runtime dir the way `runtime::dev` does - datasets, not aliases - and hand back the served
/// states plus the tape, so the test asserts against the same wiring production uses.
async fn bring_up(
    root: &Path,
) -> (
    Vec<(String, nuthatch::serve::AppState)>,
    Arc<TapeSource>,
    std::collections::HashMap<String, u64>,
) {
    let mounts = MountTable::load(root).unwrap();
    let datasets = mounts.datasets(root);
    let default_tenant_owned = mounts.tenant_default();
    let default_tenant = default_tenant_owned.as_str();

    let tape = Arc::new(TapeSource::new());
    let a1 = account(1);
    let a2 = account(2);
    for b in 1..=6u64 {
        let mut fx = transfers_block(
            b,
            0,
            1_700_000_000 + b,
            USDC,
            &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
        );
        if b == 4 {
            pad_address_to_seal(&mut fx, USDC, 3);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(TIP);

    let mounted: Vec<_> = datasets
        .iter()
        .map(|ds| {
            let cfg = nuthatch::config::Config::load(&ds.dir).unwrap();
            (
                ds.canonical().route_key(default_tenant),
                ds.dir.clone(),
                cfg,
            )
        })
        .collect();

    let health = Arc::new(RuntimeHealth::new());
    for ds in &datasets {
        health.register(&ds.canonical().route_key(default_tenant), "arbitrum-one");
    }
    let mut cursor = indexer::spawn_runtime(
        tape.clone(),
        mounted,
        None,
        false,
        1,
        // One window over the backfill, so a padded block is fetched once, not again as a tail.
        Some(TIP),
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
            .all(|(_, s)| s.store.get_meta("last_block").ok().flatten().as_deref() == Some("6"))
    })
    .await;
    assert!(landed, "the dataset did not index to the tip in time");

    // Seal past finality, so the rows compared later are real Parquet segments rather than an empty
    // cold side that would make the comparison vacuous.
    tape.advance_finalized_to(4);
    tape.insert_block(7, empty_block(7, 0, 1_700_000_007));
    tape.advance_tip_to(7);
    let sealed = wait_until(SEAL_POLL_TIMEOUT, || {
        cursor
            .states
            .iter()
            .all(|(_, s)| s.store.sealed_through() >= 4)
    })
    .await;
    assert!(sealed, "the dataset did not seal in time");

    // Take the states the fan-out needs, then hand the rest of the cursor to
    // `shutdown` so the ingest and alert tasks are actually stopped — and their
    // `Store` clones dropped — rather than merely asked to stop.
    let states_in = std::mem::take(&mut cursor.states);
    cursor.shutdown().await.expect("the cursor stops");

    let mut estimates = std::collections::HashMap::new();
    let states = runtime::fan_out_aliases(
        &datasets,
        states_in,
        &health,
        &mut estimates,
        default_tenant,
    );
    (states, tape, estimates)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_mounts_of_one_nest_share_a_single_dataset() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let nid = two_mounts_one_nest(root);

    // --- The grouping: two aliases collapse to one dataset before anything is opened. ---
    let mounts = MountTable::load(root).unwrap();
    let datasets = mounts.datasets(root);
    assert_eq!(
        datasets.len(),
        1,
        "two aliases over one identity must be ONE dataset, got {datasets:?}"
    );
    let ds: &Dataset = &datasets[0];
    assert_eq!(
        ds.refcount(),
        2,
        "the derived refcount must see both mounts"
    );
    assert_eq!(
        ds.mounts
            .iter()
            .map(|m| m.alias.as_str())
            .collect::<Vec<_>>(),
        vec!["primary", "mirror"]
    );
    assert_eq!(ds.canonical().alias, "primary", "the first mount indexes");
    assert_eq!(ds.nid.as_deref(), Some(nid.as_str()));

    // Exactly one directory on disk, whatever the aliases say.
    let dirs: Vec<_> = std::fs::read_dir(root.join(DATA_DIR))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        dirs.len(),
        1,
        "a second dataset directory was created: {dirs:?}"
    );

    // --- The runtime: index once, serve twice. ---
    let (states, tape, estimates) = bring_up(root).await;
    let shared_ranges = tape.logs_ranges();

    // The alias must not be charged a footprint the dataset already paid, or the per-cursor budget
    // would refuse a mount that costs nothing.
    assert_eq!(
        estimates.get("mirror").copied(),
        Some(0),
        "a shared mount was charged RSS a second time - sharing must not be taxed"
    );

    let served: std::collections::HashMap<_, _> = states.into_iter().collect();
    assert_eq!(served.len(), 2, "both aliases must be served");
    let primary = served.get("primary").expect("primary served");
    let mirror = served.get("mirror").expect("mirror served");

    // Two doors, one room. Same directory, and the same store object behind both.
    assert_eq!(primary.dir, mirror.dir, "the aliases resolved to two rooms");
    assert!(
        Arc::ptr_eq(&primary.store, &mirror.store),
        "the aliases hold two different stores - the dataset was opened twice"
    );

    // And the data really is identical through either door, not merely pointing at the same path.
    let via_primary = rows(&primary.dir, "primary");
    let via_mirror = rows(&mirror.dir, "primary");
    assert!(!via_primary.is_empty(), "the fixture indexed nothing");
    assert_eq!(via_primary, via_mirror);

    // --- The regression guard: sharing must not add fetching. ---
    // Control: the identical fixture with a single mount. If the shared run cost more `logs` calls
    // than this, a second backfill appeared.
    let solo_root = tempfile::tempdir().unwrap();
    let solo_root = solo_root.path();
    std::fs::write(
        solo_root.join(MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
         rpc_urls = []\nnests = [\"primary\"]\n",
    )
    .unwrap();
    let solo_nest = solo_root.join("nests").join("primary");
    std::fs::create_dir_all(&solo_nest).unwrap();
    scaffold_nest(&solo_nest, "primary", USDC);
    migrate::run(solo_root, false, false).unwrap();
    let (_, solo_tape, _) = bring_up(solo_root).await;

    // Compared over the **backfill span** (`to <= TIP`) and by range, not by call count. A second
    // backfill re-asks for blocks that have already been fetched, which shows up here as a repeated
    // range or as more ranges than the single-mount control needed; a window fetched after the tip
    // moved is ordinary tip-following, and how many of those land before the wait predicate flips is
    // a fact about the machine's load once the cursor is paced by `--poll-interval` (#1190). The old
    // assertion compared bare counts and duly failed under a loaded runner with 3 against 2, both of
    // them one backfill and a different number of following windows.
    let backfill_of = |ranges: Vec<(u64, u64)>| -> Vec<(u64, u64)> {
        ranges.into_iter().filter(|&(_, to)| to <= TIP).collect()
    };
    let shared = backfill_of(shared_ranges);
    let solo = backfill_of(solo_tape.logs_ranges());
    let mut seen = std::collections::HashSet::new();
    for r in &shared {
        assert!(
            seen.insert(*r),
            "the shared run fetched blocks {}..={} twice - a second backfill is running \
             (ranges: {shared:?})",
            r.0,
            r.1
        );
    }
    assert!(
        !shared.is_empty() && !solo.is_empty(),
        "both runs must have backfilled something, or this guard is vacuous"
    );
    assert!(
        shared.len() <= solo.len(),
        "mounting the same nest twice cost more backfill fetches than mounting it once \
         ({shared:?} vs {solo:?}) - a second backfill is running"
    );
}

/// A shared dataset's health must be shared too. Only the canonical mount is ever quarantined, so an
/// alias that reported its own state would say "indexing" while nothing was - the false-healthy
/// answer `/ready` exists to prevent.
#[test]
fn an_alias_reports_the_health_of_the_dataset_it_shares() {
    let health = RuntimeHealth::new();
    health.register("primary", "arbitrum-one");
    health.register_alias("mirror", "primary", "arbitrum-one");

    assert!(
        health.status("mirror").is_none(),
        "both should start healthy"
    );

    health.quarantine_nest("primary", "decode blew up".into(), 1, Some(30));

    let via_alias = health
        .status("mirror")
        .expect("the alias must inherit the quarantine of the dataset it shares");
    assert_eq!(via_alias.reason, "decode blew up");
    assert!(
        health.unhealthy().iter().any(|(n, _)| n == "mirror"),
        "a shared dataset's fault must hold readiness for every alias serving it"
    );
}

/// RFC-0032 §6-§7, slice 3: **two tenants, one nest, one backfill, two doors.**
///
/// This is the case a flat `nests` list cannot express at all - both tenants call their mount
/// `usdc`, so the alias is only unique *within* a tenant. It is also where the tenant path segment
/// earns its keep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_tenants_mounting_one_nest_share_it() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();

    // Start single-tenant and migrate, exactly as an existing deployment would.
    std::fs::write(
        root.join(MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
         rpc_urls = []\nnests = [\"usdc\"]\n",
    )
    .unwrap();
    let nest = root.join("nests").join("usdc");
    std::fs::create_dir_all(&nest).unwrap();
    scaffold_nest(&nest, "usdc", USDC);
    migrate::run(root, false, false).expect("migrate");

    let mut mounts = MountTable::load(root).unwrap();
    assert_eq!(
        mounts.mounts[0].tenant, "default",
        "migration must relabel to the default tenant, not leave it absent"
    );
    assert!(
        !mounts.is_multi_tenant(),
        "one tenant must stay single-tenant - the route segment would be pure ceremony"
    );
    let nid = mounts.mounts[0].nid.clone();

    // Two tenants, both calling it `usdc`. Same identity, so the same dataset.
    mounts.mounts = vec![
        nuthatch::runtime::Mount {
            tenant: "acme".into(),
            alias: "usdc".into(),
            nid: nid.clone(),
            sql: Default::default(),
            queries: Vec::new(),
            publish: None,
            limits: Default::default(),
            #[cfg(feature = "counter")]
            counter: None,
        },
        nuthatch::runtime::Mount {
            tenant: "globex".into(),
            alias: "usdc".into(),
            nid: nid.clone(),
            sql: Default::default(),
            queries: Vec::new(),
            publish: None,
            limits: Default::default(),
            #[cfg(feature = "counter")]
            counter: None,
        },
    ];
    mounts.runtime.nests.clear(); // `[[mounts]]` is authoritative once present
    std::fs::write(
        root.join(MOUNTS_FILE),
        toml::to_string_pretty(&mounts).unwrap(),
    )
    .unwrap();

    let mounts = MountTable::load(root).expect("a two-tenant mounts must load");
    assert!(mounts.is_multi_tenant());

    let ds = mounts.datasets(root);
    assert_eq!(ds.len(), 1, "two tenants, one dataset - got {ds:?}");
    assert_eq!(ds[0].refcount(), 2);
    assert_eq!(
        ds[0]
            .mounts
            .iter()
            .map(|m| m.route_key("default"))
            .collect::<Vec<_>>(),
        vec!["acme/usdc", "globex/usdc"],
        "each tenant gets its own path onto the shared dataset"
    );

    // One directory on disk. Two tenants must not mean two backfills.
    let dirs = std::fs::read_dir(root.join(DATA_DIR)).unwrap().count();
    assert_eq!(
        dirs, 1,
        "a second tenant created a second dataset directory"
    );

    let (states, _tape, estimates) = bring_up(root).await;
    let served: std::collections::HashMap<_, _> = states.into_iter().collect();
    let acme = served.get("acme/usdc").expect("acme served");
    let globex = served.get("globex/usdc").expect("globex served");
    assert!(
        Arc::ptr_eq(&acme.store, &globex.store),
        "the tenants hold two different stores - the dataset was opened twice"
    );
    assert_eq!(estimates.get("globex/usdc").copied(), Some(0));
    assert_eq!(rows(&acme.dir, "usdc"), rows(&globex.dir, "usdc"));

    // Unmounting one tenant leaves the other serving, and leaves the data alone (RFC-0032 §5).
    let mut after = MountTable::load(root).unwrap();
    after.mounts.retain(|m| m.tenant != "acme");
    std::fs::write(
        root.join(MOUNTS_FILE),
        toml::to_string_pretty(&after).unwrap(),
    )
    .unwrap();

    let after = MountTable::load(root).unwrap();
    let ds = after.datasets(root);
    assert_eq!(ds.len(), 1);
    assert_eq!(
        ds[0].refcount(),
        1,
        "the refcount must fall to one when a tenant unmounts"
    );
    assert_eq!(ds[0].canonical().tenant, "globex");
    assert!(
        !after.is_multi_tenant(),
        "back to one tenant, so the route segment goes away again"
    );
    assert!(
        ds[0].dir.is_dir(),
        "one tenant's unmount deleted the other's data - collection must be deferred (§5)"
    );
}

/// A tenant is opaque to nuthatch, which is not the same as being allowed to contain `../`.
#[test]
fn a_tenant_is_still_a_path_segment() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path();
    let nid = "aa11".repeat(16);
    for (tenant, expect) in [
        ("../escape", "tenant '../escape' is invalid"),
        ("a/b", "tenant 'a/b' is invalid"),
        ("", "tenant '' is invalid"),
        ("nests", "reserved"),
    ] {
        std::fs::write(
            root.join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
                 rpc_urls = []\n\n[[mounts]]\ntenant = \"{tenant}\"\nalias = \"a\"\nnid = \"{nid}\"\n"
            ),
        )
        .unwrap();
        let err = MountTable::load(root).unwrap_err().to_string();
        assert!(
            err.contains(expect),
            "tenant {tenant:?}: expected {expect:?}, got: {err}"
        );
    }
}
