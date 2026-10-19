//! MountTable lifecycle end-to-end: unmounting a nest releases everything it held (RFC-0027 §6).
//!
//! The acceptance test that matters here is not "does the route disappear" - that is easy and
//! unconvincing - but **does the nest's redb file actually become free**. `Store` is an `Arc<Database>`
//! cloned three ways at nest construction (the cursor, the alert delivery worker, the serving state),
//! and redb only releases the file when the last clone drops. Reopening the store is therefore a
//! single assertion that proves all three were let go; miss any one and it fails.

mod common;

use std::sync::Arc;

use nuthatch::{health::RuntimeHealth, indexer, runtime, serve, store::Store};

use common::tape::*;

/// A two-nest, one-chain mounts over a scripted tape, wrapped in the driver handles a live mounts keeps.
/// A lifecycle change is written back, and a runtime with no table to write to reports that as a
/// failure (#1639). A test that brought its own table keeps it.
fn write_table_if_absent(dir: &std::path::Path, nests: &[&str]) {
    let table = dir.join(runtime::MOUNTS_FILE);
    if table.exists() {
        return;
    }
    let listed: Vec<String> = nests.iter().map(|n| format!("\"{n}\"")).collect();
    std::fs::write(
        &table,
        format!(
            "[runtime]\nname = \"r\"\nnests = [{}]\n\n[[chains]]\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n",
            listed.join(", ")
        ),
    )
    .unwrap();
}

async fn two_nest_roost(
    roost_dir: &std::path::Path,
    usdc_dir: &std::path::Path,
    arb_dir: &std::path::Path,
) -> (runtime::RuntimeHandles, Arc<TapeSource>) {
    write_table_if_absent(roost_dir, &["usdc", "arb"]);
    let tape = Arc::new(TapeSource::new());
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

    let cfg_u = scaffold_nest(usdc_dir, "usdc", USDC);
    let cfg_a = scaffold_nest(arb_dir, "arb", ARB);
    let health = Arc::new(RuntimeHealth::new());
    health.register("usdc", "arbitrum-one");
    health.register("arb", "arbitrum-one");

    let cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![
            ("usdc".to_string(), usdc_dir.to_path_buf(), cfg_u),
            ("arb".to_string(), arb_dir.to_path_buf(), cfg_a),
        ],
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

    let roster = serde_json::json!({
        "mounts": "test",
        "nests": [{"name": "usdc"}, {"name": "arb"}],
    });
    let live = serve::LiveRuntime::new(serve::compose_runtime(
        roster.clone(),
        cursor.states.clone(),
        health.clone(),
    ));
    let handles = runtime::RuntimeHandles {
        live,
        states: cursor.states,
        alert_workers: cursor.alert_workers,
        publishers: Vec::new(),
        // Keyed by the nest's declared chain - `scaffold_nest` writes `arbitrum-one`. Getting this
        // wrong is not cosmetic: `unmount` refuses to proceed without a channel for the chain, rather
        // than removing routes while the cursor may still be writing.
        lifecycle: std::collections::HashMap::from([(
            "arbitrum-one".to_string(),
            cursor.lifecycle.clone(),
        )]),
        health,
        roster,
        estimates: std::collections::HashMap::from([
            ("usdc".to_string(), 90),
            ("arb".to_string(), 90),
        ]),
        default_tenant: "default".to_string(),
        suspended: Default::default(),
        joining: Default::default(),
        mount_ctx: runtime::MountContext {
            dir: roost_dir.to_path_buf(),
            // Un-migrated: no mount records, so resolution stays on the pre-2.0 `nests/<name>` path.
            mounts: Vec::new(),
            sources: std::collections::HashMap::from([(
                "arbitrum-one".to_string(),
                tape.clone() as Arc<dyn nuthatch::source::Source>,
            )]),
            endpoint_counts: std::collections::HashMap::from([("arbitrum-one".to_string(), 1)]),
            backfill: None,
            seal_direct: false,
            concurrency: 1,
            ipfs_window_deadline: nuthatch::ipfs_resolve::WINDOW_DEADLINE,
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
    // The ingest task is deliberately leaked into the handles' lifetime here: the cursor must stay
    // running for the unmount handshake to be answered at a window boundary.
    std::mem::forget(cursor.ingest);
    (handles, tape)
}

/// Drive one GET through the served composition and return its status.
async fn status(live: &serve::LiveRuntime, path: &str) -> axum::http::StatusCode {
    use tower::ServiceExt;
    let req = axum::http::Request::builder()
        .uri(path)
        .body(axum::body::Body::empty())
        .unwrap();
    live.service().oneshot(req).await.unwrap().status()
}

/// Drive one GET through the served composition and return its parsed JSON body.
async fn body_json(live: &serve::LiveRuntime, path: &str) -> serde_json::Value {
    use tower::ServiceExt;
    let req = axum::http::Request::builder()
        .uri(path)
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = live.service().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// The names `GET /nests` currently reports, in roster order.
async fn roster_names(live: &serve::LiveRuntime) -> Vec<String> {
    body_json(live, "/nests").await["nests"]
        .as_array()
        .expect("nests is an array")
        .iter()
        .map(|n| n["name"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// RFC-0027 §6: unmount is a **drain**, and the proof is that the store becomes reopenable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unmounting_a_nest_releases_its_store_and_removes_its_routes() {
    let usdc_dir = tempfile::tempdir().unwrap();
    let arb_dir = tempfile::tempdir().unwrap();
    let (mut handles, _tape) =
        two_nest_roost(usdc_dir.path(), usdc_dir.path(), arb_dir.path()).await;

    // Both mounted to begin with.
    assert_eq!(
        status(&handles.live, "/arb/health").await,
        axum::http::StatusCode::OK
    );
    assert_eq!(
        status(&handles.live, "/usdc/health").await,
        axum::http::StatusCode::OK
    );

    // While it is mounted, the store is held - reopening must fail. This is the control: without it,
    // the assertion after the unmount would pass even if redb never locked the file in the first
    // place, and the test would prove nothing.
    let arb_db = arb_dir.path().join("nuthatch.redb");
    assert!(
        Store::open(&arb_db).is_err(),
        "a mounted nest's store must be held open - otherwise this test cannot prove a release"
    );

    // The control for the roster assertion below: `arb` really is listed while it is mounted, so a
    // roster that never listed it cannot be mistaken for one that dropped it.
    let before = roster_names(&handles.live).await;
    assert!(
        before.contains(&"arb".to_string()),
        "premise: a mounted nest is listed in GET /nests; got {before:?}"
    );

    handles.unmount("arb").await.expect("unmount");

    // The routes are gone, and the co-tenant is untouched - the whole point of unmounting one nest
    // rather than restarting the runtime.
    assert_eq!(
        status(&handles.live, "/arb/health").await,
        axum::http::StatusCode::NOT_FOUND,
        "the unmounted nest's routes must be gone"
    );
    assert_eq!(
        status(&handles.live, "/usdc/health").await,
        axum::http::StatusCode::OK,
        "the co-tenant must keep serving across another nest's unmount"
    );

    // #554's other half. `unmount` rebuilds `roster["nests"]` from the live states for the same
    // reason `mount` does, and the two halves shipped together - but only the mount half was
    // covered, so deleting the rebuild from `unmount` left the whole suite green. An operator who
    // unmounts a nest and reads `/nests` to confirm it must not still be told it is there.
    let after = roster_names(&handles.live).await;
    assert!(
        !after.contains(&"arb".to_string()),
        "GET /nests must drop an unmounted nest, not keep reporting the startup set; got {after:?}"
    );
    assert!(
        after.contains(&"usdc".to_string()),
        "and the co-tenant must survive the rebuild; got {after:?}"
    );

    // The assertion this test exists for: every holder let go.
    Store::open(&arb_db)
        .expect("after unmount the nest's store must be reopenable - some holder did not drop");
}

/// Unmounting something that is not mounted is a no-op, not an error - so a control plane retrying a
/// command it already delivered does not produce a spurious failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unmounting_an_absent_nest_is_a_no_op() {
    let usdc_dir = tempfile::tempdir().unwrap();
    let arb_dir = tempfile::tempdir().unwrap();
    let (mut handles, _tape) =
        two_nest_roost(usdc_dir.path(), usdc_dir.path(), arb_dir.path()).await;

    handles.unmount("not-mounted").await.expect("no-op");
    assert_eq!(handles.states.len(), 2, "nothing was removed");

    handles.unmount("arb").await.expect("first unmount");
    handles.unmount("arb").await.expect("second is idempotent");
    assert_eq!(handles.states.len(), 1);
}

/// #1536, #1538: a live mount that opens its own store takes the recorded SQL surface and the
/// cursor's existing gate. Boot does both. This path used to serve `sql = "deny"` as open, on a
/// private semaphore.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_mount_applies_the_recorded_surface_and_shares_the_sql_gate() {
    use nuthatch::allowlist::SqlAccess;

    let roost = tempfile::tempdir().unwrap();
    let usdc_dir = tempfile::tempdir().unwrap();
    let arb_dir = tempfile::tempdir().unwrap();
    let (mut handles, _tape) = two_nest_roost(roost.path(), usdc_dir.path(), arb_dir.path()).await;

    let nid = "ee55".repeat(16);
    let gamma = runtime::MountTable::data_dir(roost.path(), &nid);
    std::fs::create_dir_all(&gamma).unwrap();
    scaffold_nest(&gamma, "gamma", USDC);
    handles.mount_ctx.mounts.push(runtime::Mount {
        tenant: "default".to_string(),
        alias: "gamma".to_string(),
        nid: nid.clone(),
        sql: SqlAccess::Deny,
        queries: Vec::new(),
        publish: None,
        #[cfg(feature = "counter")]
        counter: None,
    });

    handles
        .mount("gamma", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("mount");

    let gamma_state = handles
        .states
        .iter()
        .find(|(n, _)| n == "gamma")
        .expect("gamma mounted");
    assert_eq!(gamma_state.1.surface.access, SqlAccess::Deny);
    let usdc = handles
        .states
        .iter()
        .find(|(n, _)| n == "usdc")
        .expect("usdc still mounted");
    assert!(
        Arc::ptr_eq(&gamma_state.1.sql_gate, &usdc.1.sql_gate),
        "a live mount must take the cursor's /sql gate"
    );
}

fn surface_of(h: &runtime::RuntimeHandles, name: &str) -> nuthatch::allowlist::Surface {
    let (_, s) = h
        .states
        .iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("'{name}' is not mounted"));
    (*s.surface).clone()
}

fn allowlist_of(sql: &str) -> nuthatch::allowlist::Surface {
    nuthatch::allowlist::Surface {
        access: nuthatch::allowlist::SqlAccess::Allowlist,
        queries: vec![nuthatch::allowlist::NamedQuery {
            name: "total".to_string(),
            sql: sql.to_string(),
            params: Default::default(),
        }],
    }
}

fn record_surface(
    h: &mut runtime::RuntimeHandles,
    alias: &str,
    surface: &nuthatch::allowlist::Surface,
) {
    let m = h
        .mount_ctx
        .mounts
        .iter_mut()
        .find(|m| m.alias == alias)
        .expect("a record");
    m.sql = surface.access;
    m.queries = surface.queries.clone();
}

/// #2129: the first nest on a chain starts the cursor, and serves its record's surface from there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_that_starts_its_cursor_serves_the_recorded_surface() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "c1".repeat(32);
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    let deny = nuthatch::allowlist::Surface {
        access: nuthatch::allowlist::SqlAccess::Deny,
        queries: Vec::new(),
    };
    record_surface(&mut handles, "usdc", &deny);
    handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("mount");
    assert_eq!(surface_of(&handles, "usdc"), deny);
}

/// #2129: a second alias onto a dataset already served takes its own record, not its co-tenant's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_alias_serves_its_own_recorded_surface() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "c2".repeat(32);
    let (mut handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let narrow = allowlist_of("SELECT 1");
    handles.mount_ctx.mounts.push(runtime::Mount {
        tenant: "default".to_string(),
        alias: "usdc2".to_string(),
        nid: nid.clone(),
        sql: narrow.access,
        queries: narrow.queries.clone(),
        publish: None,
        #[cfg(feature = "counter")]
        counter: None,
    });
    handles
        .mount("usdc2", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("mount");
    assert_eq!(surface_of(&handles, "usdc2"), narrow);
    assert_eq!(surface_of(&handles, "usdc"), Default::default());
}

/// #2129: a move stages the new NID under another name, which has no record of its own. The name
/// must come out of the move serving its own record, not the staging name's open default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_keeps_the_recorded_surface() {
    let roost = tempfile::tempdir().unwrap();
    let (old_nid, new_nid) = ("c3".repeat(32), "c4".repeat(32));
    let (mut handles, _tape) = one_live_mount(roost.path(), &old_nid).await;
    let new_dir = runtime::MountTable::data_dir(roost.path(), &new_nid);
    std::fs::create_dir_all(&new_dir).unwrap();
    scaffold_nest(&new_dir, "usdc", USDC);
    let narrow = allowlist_of("SELECT 1");
    record_surface(&mut handles, "usdc", &narrow);

    handles
        .move_name("usdc", runtime::Nid::parse(&new_nid).unwrap())
        .await
        .expect("move");
    assert_eq!(surface_of(&handles, "usdc"), narrow);
    assert_eq!(
        status(&handles.live, "/usdc/sql?q=SELECT%202").await,
        axum::http::StatusCode::FORBIDDEN,
        "a moved allowlisted mount answered free-form SQL"
    );
}

/// #2129: the name a move stages under is routed while the new NID catches up, so it serves the
/// moving name's surface rather than the open default of a name with no record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_move_serves_the_moving_names_surface() {
    let roost = tempfile::tempdir().unwrap();
    let (old_nid, new_nid) = ("c8".repeat(32), "c9".repeat(32));
    let (mut handles, _tape) = one_live_mount(roost.path(), &old_nid).await;
    let new_dir = runtime::MountTable::data_dir(roost.path(), &new_nid);
    std::fs::create_dir_all(&new_dir).unwrap();
    scaffold_nest(&new_dir, "usdc", USDC);
    let narrow = allowlist_of("SELECT 1");
    record_surface(&mut handles, "usdc", &narrow);

    let staging = runtime::RuntimeHandles::staging_name("usdc");
    handles
        .mount(&staging, Some(runtime::Nid::parse(&new_nid).unwrap()))
        .await
        .expect("staging mount");
    assert_eq!(surface_of(&handles, &staging), narrow);
}

/// #2129: a resume is a mount of the suspended name, and keeps the record it was suspended with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_keeps_the_recorded_surface() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "c5".repeat(32);
    let (mut handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let deny = nuthatch::allowlist::Surface {
        access: nuthatch::allowlist::SqlAccess::Deny,
        queries: Vec::new(),
    };
    record_surface(&mut handles, "usdc", &deny);
    handles.suspend("usdc").await.expect("suspend");
    handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("resume");
    assert_eq!(surface_of(&handles, "usdc"), deny);
}

/// #2135: a nest whose author declared a ceiling, mounted with no surface of its own, serves the
/// ceiling: open is wider than any ceiling, and a mount may only narrow it (RFC-0034 §3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_mount_of_a_nest_with_a_ceiling_serves_the_ceiling() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "c6".repeat(32);
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    std::fs::write(
        runtime::MountTable::data_dir(roost.path(), &nid).join("queries.toml"),
        "[[queries]]\nname = \"total\"\nsql = \"SELECT 1\"\n",
    )
    .unwrap();
    handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("mount");
    assert_eq!(surface_of(&handles, "usdc"), allowlist_of("SELECT 1"));
    assert_eq!(
        status(&handles.live, "/usdc/sql?q=SELECT%202").await,
        axum::http::StatusCode::FORBIDDEN,
        "free-form SQL answered past the author's ceiling"
    );
}

/// #2135: a live mount that would widen its author's ceiling is refused, as boot refuses it, and
/// nothing is left mounted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_mount_wider_than_its_ceiling_is_refused() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "c7".repeat(32);
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    std::fs::write(
        runtime::MountTable::data_dir(roost.path(), &nid).join("queries.toml"),
        "[[queries]]\nname = \"total\"\nsql = \"SELECT 1\"\n",
    )
    .unwrap();
    record_surface(&mut handles, "usdc", &allowlist_of("SELECT 2"));
    let err = handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect_err("a widening mount was accepted");
    assert!(format!("{err:#}").contains("redefines"), "{err:#}");
    assert!(
        handles.states.is_empty(),
        "a refused mount was left serving"
    );
}

/// #2135: a `queries.toml` that exists but cannot be read refuses the mount rather than opening it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_whose_ceiling_cannot_be_read_is_refused() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "ca".repeat(32);
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    std::fs::create_dir(runtime::MountTable::data_dir(roost.path(), &nid).join("queries.toml"))
        .unwrap();
    let err = handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect_err("a mount with an unreadable ceiling was accepted");
    assert!(format!("{err:#}").contains("queries.toml"), "{err:#}");
    assert!(
        handles.states.is_empty(),
        "a refused mount was left serving"
    );
}

/// RFC-0027 §3: the three admission refusals, each decided **before** any work is done - no store
/// opened, no block fetched, nothing left behind by a rejected mount.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_is_refused_for_a_taken_name_an_undeclared_chain_or_a_breached_budget() {
    let roost_dir = tempfile::tempdir().unwrap();
    let usdc_dir = roost_dir.path().join("nests/usdc");
    let arb_dir = roost_dir.path().join("nests/arb");
    std::fs::create_dir_all(&usdc_dir).unwrap();
    std::fs::create_dir_all(&arb_dir).unwrap();
    let (mut handles, _tape) = two_nest_roost(roost_dir.path(), &usdc_dir, &arb_dir).await;

    // 1. A name already on the runtime. This is an upgrade (RFC-0020), not a mount.
    let err = handles.mount("usdc", None).await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<runtime::MountRefusal>(),
            Some(runtime::MountRefusal::AlreadyMounted(_))
        ),
        "expected AlreadyMounted, got: {err:#}"
    );

    // 2. A nest whose chain the runtime declares no cursor for. Scaffold one on a different chain.
    let other = roost_dir.path().join("nests/elsewhere");
    std::fs::create_dir_all(&other).unwrap();
    let mut cfg = scaffold_nest(&other, "elsewhere", USDC);
    cfg.nest.chain = "base".to_string();
    cfg.nest.chain_id = 8453;
    cfg.save(&other).unwrap();
    let err = handles.mount("elsewhere", None).await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<runtime::MountRefusal>(),
            Some(runtime::MountRefusal::UndeclaredChain { .. })
        ),
        "expected UndeclaredChain, got: {err:#}"
    );

    // 3. A mount that would breach the cursor's ceiling. The budget is a refusal, not a warning -
    //    `CLAUDE.md`'s per-cursor limit stops being a budget the moment a mount may quietly exceed it.
    let third = roost_dir.path().join("nests/third");
    std::fs::create_dir_all(&third).unwrap();
    scaffold_nest(&third, "third", ARB);
    handles.mount_ctx.max_rss_mb = 100; // below even the base cost, so any mount breaches it
    let err = handles.mount("third", None).await.unwrap_err();
    match err.downcast_ref::<runtime::MountRefusal>() {
        Some(runtime::MountRefusal::OverBudget {
            projected_mb,
            ceiling_mb,
            ..
        }) => assert!(
            projected_mb > ceiling_mb,
            "the refusal must carry the numbers an operator needs to act: {projected_mb} vs {ceiling_mb}"
        ),
        other => panic!("expected OverBudget, got: {other:?} / {err:#}"),
    }

    // Every refusal left the runtime exactly as it was.
    assert_eq!(handles.states.len(), 2, "no partial mount was left behind");
}

/// A nest mounted into a running runtime fetches through its chain's shared source, so the
/// single-endpoint concurrency cap applies to its seal-direct pass as it does at boot, and `/ready` says
/// so. The mount used the runtime's uncapped `--concurrency` before this.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_mount_is_held_to_the_single_endpoint_concurrency_cap() {
    for (name, endpoints, want) in [
        (
            "solohost",
            1,
            serde_json::json!({"requested": 8, "effective": 1, "capped_by": "single_rpc_endpoint"}),
        ),
        (
            "twohosts",
            2,
            serde_json::json!({"requested": 8, "effective": 8, "capped_by": null}),
        ),
    ] {
        let roost_dir = tempfile::tempdir().unwrap();
        let usdc_dir = roost_dir.path().join("nests/usdc");
        let arb_dir = roost_dir.path().join("nests/arb");
        std::fs::create_dir_all(&usdc_dir).unwrap();
        std::fs::create_dir_all(&arb_dir).unwrap();
        let (mut handles, _tape) = two_nest_roost(roost_dir.path(), &usdc_dir, &arb_dir).await;
        let dir = roost_dir.path().join("nests").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        // `/ready` reads by route, so the nest's own name differs from it here, as an alias can.
        scaffold_nest(&dir, &format!("{name}-nest"), ARB);

        handles.mount_ctx.seal_direct = true;
        handles.mount_ctx.concurrency = 8;
        handles
            .mount_ctx
            .endpoint_counts
            .insert("arbitrum-one".to_string(), endpoints);
        handles.mount(name, None).await.expect("mount");

        let ready = body_json(&handles.live, &format!("/{name}/ready")).await;
        assert_eq!(ready["seal_direct_concurrency"], want, "{name}: {ready}");
        assert_eq!(
            ready["seal_direct_ipfs_window_deadline_secs"],
            serde_json::json!(300),
            "{name}: {ready}"
        );

        // #1420 on the hot-mount path: the mounted nest reports its NID, and its store records what
        // its data covers.
        let nest = body_json(&handles.live, &format!("/{name}/nest")).await;
        assert!(nest["nid"].is_string(), "{name}: {nest}");
        let (_, state) = handles
            .states
            .iter()
            .find(|(n, _)| n == name)
            .expect("mounted");
        assert!(
            state
                .store
                .get_meta(nuthatch::store::COVERAGE_KEY)
                .unwrap()
                .is_some(),
            "{name}: the mounted nest's store records what it covers"
        );
    }
}

/// A one-nest runtime whose route differs from the nest's own name, as an alias or a `tenant/alias`
/// route does, backfilled with `--seal-direct` over a tape it can seal part of.
async fn route_named_runtime(
    nest_dir: &std::path::Path,
    route: &str,
) -> (runtime::RuntimeHandles, Arc<TapeSource>) {
    write_table_if_absent(nest_dir, &[route]);
    let tape = Arc::new(TapeSource::new());
    let (a1, a2) = (account(1), account(2));
    for b in 1..=6u64 {
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
    tape.advance_tip_to(6);
    tape.advance_finalized_to(4);

    let cfg = scaffold_nest(nest_dir, &format!("{route}-nest"), USDC);
    let health = Arc::new(RuntimeHealth::new());
    health.register(route, "arbitrum-one");
    // `runtime::dev` records the concurrency before it spawns the cursor; this harness spawns directly.
    indexer::backfill_concurrency_for(1, 1, true, &[route]);
    let cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![(route.to_string(), nest_dir.to_path_buf(), cfg)],
        Some(6),
        true,
        1,
        Some(2),
        false,
        None,
        health.clone(),
        false,
    )
    .await
    .expect("spawn_runtime");
    // `/ready` reads the nest's metrics, which the indexer updates after the store commit, so wait on
    // those too or the first read can catch them a moment behind the store.
    let indexed = wait_until(std::time::Duration::from_secs(30), || {
        let store = &cursor.states[0].1.store;
        let m = nuthatch::metrics::METRICS.nest(route);
        store.get_meta("last_block").ok().flatten().as_deref() == Some("6")
            && store.sealed_through() >= 4
            && m.last_block() == 6
            && m.sealed_through() >= 4
            && m.seal_direct_completed() >= 4
    })
    .await;
    assert!(
        indexed,
        "premise: the runtime indexed to 6 and sealed through 4"
    );

    let roster = serde_json::json!({"mounts": "test", "nests": [{"name": route}]});
    let live = serve::LiveRuntime::new(serve::compose_runtime(
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
        estimates: std::collections::HashMap::from([(route.to_string(), 90)]),
        default_tenant: "default".to_string(),
        suspended: Default::default(),
        joining: Default::default(),
        mount_ctx: runtime::MountContext {
            dir: nest_dir.to_path_buf(),
            mounts: Vec::new(),
            sources: std::collections::HashMap::from([(
                "arbitrum-one".to_string(),
                tape.clone() as Arc<dyn nuthatch::source::Source>,
            )]),
            endpoint_counts: std::collections::HashMap::from([("arbitrum-one".to_string(), 1)]),
            backfill: None,
            seal_direct: true,
            concurrency: 1,
            ipfs_window_deadline: nuthatch::ipfs_resolve::WINDOW_DEADLINE,
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
    std::mem::forget(cursor.ingest);
    (handles, tape)
}

/// #1415: a runtime nest's metrics were recorded under its own name while `/ready`, the
/// `nuthatch_nest_*` labels, health and unmount all address it by route, so a route that differs from
/// the name read zeros and nulls, and an unmount left its cursor running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nest_whose_route_differs_from_its_name_is_measured_and_unmounted_by_route() {
    let nest_dir = tempfile::tempdir().unwrap();
    let route = "usdc-route";
    let (mut handles, _tape) = route_named_runtime(nest_dir.path(), route).await;

    let ready = body_json(&handles.live, &format!("/{route}/ready")).await;
    let nulls: Vec<&String> = ready
        .as_object()
        .expect("/ready is an object")
        .iter()
        .filter(|(_, v)| v.is_null())
        .map(|(k, _)| k)
        .collect();
    assert!(nulls.is_empty(), "null on /ready: {nulls:?} in {ready}");
    for (field, want) in [
        ("tip", 6),
        ("last_block", 6),
        ("sealed_through", 4),
        ("seal_direct_completed", 4),
        ("seal_direct_target", 4),
        ("seal_direct_fetched", 4),
        ("seal_direct_ipfs_window_deadline_secs", 300),
        ("ipfs_gave_up_documents", 0),
    ] {
        assert_eq!(ready[field], serde_json::json!(want), "{field}: {ready}");
    }
    assert!(
        ready["fetch_window_blocks"].as_u64().is_some_and(|w| w > 0),
        "{ready}"
    );

    let series = nuthatch::metrics::METRICS.render();
    for line in [
        format!("nuthatch_nest_last_block{{nest=\"{route}\"}} 6"),
        format!("nuthatch_nest_sealed_through{{nest=\"{route}\"}} 4"),
        format!("nuthatch_nest_seal_direct_completed{{nest=\"{route}\"}} 4"),
        format!("nuthatch_nest_ipfs_given_up_total{{nest=\"{route}\"}} 0"),
        format!("nuthatch_nest_ipfs_retries_total{{nest=\"{route}\"}} 0"),
    ] {
        assert!(series.contains(&line), "missing `{line}`");
    }
    assert!(
        !series.contains(&format!("nest=\"{route}-nest\"")),
        "a series is still labelled by the nest's own name"
    );

    // #1420 on the runtime path: the route's nest reports its identities, and its store records what
    // its data covers.
    let nest = body_json(&handles.live, &format!("/{route}/nest")).await;
    let manifest = nuthatch::blob::build_manifest(nest_dir.path(), None).unwrap();
    assert_eq!(nest["nid"], serde_json::json!(manifest.nid()), "{nest}");
    assert_eq!(
        nest["data_identity"],
        serde_json::json!(manifest.data_identity()),
        "{nest}"
    );
    assert!(
        handles.states[0]
            .1
            .store
            .get_meta(nuthatch::store::COVERAGE_KEY)
            .unwrap()
            .is_some(),
        "the runtime nest's store records what it covers"
    );

    handles.unmount(route).await.expect("unmount");
    Store::open(&nest_dir.path().join("nuthatch.redb"))
        .expect("unmounting the route must release its cursor's store");
}

/// RFC-0027 §5: a lifecycle change must survive a restart.
///
/// `mounts.toml` is the embedded stand-in for a control-plane DB - desired state lives in the same file
/// the static boot path reads. Without this, an unmount would silently come back on the next restart,
/// which is the worst kind of bug because it looks like it worked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lifecycle_change_is_persisted_to_the_mount_table() {
    let roost_dir = tempfile::tempdir().unwrap();
    let usdc_dir = roost_dir.path().join("nests/usdc");
    let arb_dir = roost_dir.path().join("nests/arb");
    std::fs::create_dir_all(&usdc_dir).unwrap();
    std::fs::create_dir_all(&arb_dir).unwrap();

    // A mount table matching the running set, as `dev` would have loaded.
    std::fs::write(
        roost_dir.path().join(nuthatch::runtime::MOUNTS_FILE),
        r#"[runtime]
name = "test"
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = ["http://127.0.0.1:1"]
nests = ["usdc", "arb"]
"#,
    )
    .unwrap();

    let (mut handles, _tape) = two_nest_roost(roost_dir.path(), &usdc_dir, &arb_dir).await;
    handles.unmount("arb").await.expect("unmount");

    let reloaded = nuthatch::runtime::MountTable::load(roost_dir.path())
        .expect("the mount table still parses");
    assert_eq!(
        reloaded.runtime.nests,
        vec!["usdc".to_string()],
        "the unmount must be recorded, or it silently returns on the next restart"
    );
    // Everything else about the manifest survives the rewrite untouched.
    assert_eq!(reloaded.runtime.chain.as_deref(), Some("arbitrum-one"));
    assert_eq!(reloaded.runtime.chain_id, Some(42161));
}

/// #517: `POST /_admin/nests` resolved `nests/<name>/` even in a 2.0 `mounts.toml` runtime, because
/// `mount` only ever looked a nest's `nid` up from its own startup snapshot of `[[mounts]]` - and a
/// nest mounted live for the *first* time, the ordinary case, has no such record yet. `DELETE` already
/// resolved and persisted correctly; this is the missing mount half, plus the persistence half that
/// has to go with it - a live mount not written back to `mounts.toml` "works" until the next restart
/// and then silently disappears, the exact failure that file exists to prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mounting_an_unrecorded_nest_resolves_by_nid_and_persists_its_record() {
    let roost_dir = tempfile::tempdir().unwrap();
    let usdc_nid = "bb22".repeat(16);
    let usdc_dir = runtime::MountTable::data_dir(roost_dir.path(), &usdc_nid);
    std::fs::create_dir_all(&usdc_dir).unwrap();

    // A genuinely 2.0 `mounts.toml`: it already has one recorded mount, which is what distinguishes
    // "migrated but this alias is new" from "not migrated at all" (`mount_ctx.mounts` empty falls
    // back to the pre-2.0 layout by design).
    std::fs::write(
        roost_dir.path().join(runtime::MOUNTS_FILE),
        format!(
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
             [[mounts]]\nalias = \"usdc\"\nnid = \"{usdc_nid}\"\n"
        ),
    )
    .unwrap();

    let tape = Arc::new(TapeSource::new());
    let cfg_u = scaffold_nest(&usdc_dir, "usdc", USDC);
    let health = Arc::new(RuntimeHealth::new());
    health.register("usdc", "arbitrum-one");

    let cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![("usdc".to_string(), usdc_dir.to_path_buf(), cfg_u)],
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

    let roster = serde_json::json!({"runtime": "test", "nests": [{"name": "usdc"}]});
    let live = serve::LiveRuntime::new(serve::compose_runtime(
        roster.clone(),
        cursor.states.clone(),
        health.clone(),
    ));
    let mut handles = runtime::RuntimeHandles {
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
        estimates: std::collections::HashMap::from([("usdc".to_string(), 90)]),
        default_tenant: "default".to_string(),
        suspended: Default::default(),
        joining: Default::default(),
        mount_ctx: runtime::MountContext {
            dir: roost_dir.path().to_path_buf(),
            // Only `usdc` is on record - `gamma` below is exactly the "runtime has never seen this
            // alias before" shape a first-time live mount actually has.
            mounts: vec![runtime::Mount {
                tenant: "default".to_string(),
                alias: "usdc".to_string(),
                nid: usdc_nid.clone(),
                sql: Default::default(),
                queries: Vec::new(),
                publish: None,
                #[cfg(feature = "counter")]
                counter: None,
            }],
            sources: std::collections::HashMap::from([(
                "arbitrum-one".to_string(),
                tape.clone() as Arc<dyn nuthatch::source::Source>,
            )]),
            endpoint_counts: std::collections::HashMap::from([("arbitrum-one".to_string(), 1)]),
            backfill: None,
            seal_direct: false,
            concurrency: 1,
            ipfs_window_deadline: nuthatch::ipfs_resolve::WINDOW_DEADLINE,
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
    std::mem::forget(cursor.ingest);

    // `gamma`'s data already lives at `data/<nid>` - delivered out of band, exactly as #517 describes
    // - but the runtime has never recorded it. Mounting it live must resolve `data/<nid>`, not the
    // pre-2.0 `nests/gamma` this runtime doesn't even have.
    let gamma_nid = "cc33".repeat(16);
    let gamma_dir = runtime::MountTable::data_dir(roost_dir.path(), &gamma_nid);
    std::fs::create_dir_all(&gamma_dir).unwrap();
    scaffold_nest(&gamma_dir, "gamma", ARB);

    handles
        .mount("gamma", Some(runtime::Nid::parse(&gamma_nid).unwrap()))
        .await
        .expect("mounting an unrecorded nest by nid must resolve data/<nid>, not nests/<name>");
    assert!(handles.states.iter().any(|(n, _)| n == "gamma"));

    // #554: the roster is the surface an operator or tool actually reads to confirm a mount, and it
    // must not still report the startup snapshot (`usdc` alone) once `gamma` is genuinely live. Not
    // "the roster is non-empty" - that would pass unchanged - but that the mounted alias is *in* it.
    let roster = body_json(&handles.live, "/nests").await;
    let names: Vec<&str> = roster["nests"]
        .as_array()
        .expect("nests is an array")
        .iter()
        .map(|n| n["name"].as_str().unwrap_or_default())
        .collect();
    assert!(
        names.contains(&"gamma"),
        "GET /nests must list a nest mounted live via POST /_admin/nests; got {names:?}"
    );
    assert!(
        names.contains(&"usdc"),
        "the pre-existing nest must stay listed too"
    );

    // #557: `GET /nests` naming `gamma` is not the whole surface - `/sql` stamps its own provenance
    // per RFC-0035 §3, from `AppState::nid`, not the roster. Before this fix `mount()` rebuilt
    // `states` and the roster but never set the state's own `nid`, so this answered `null` until a
    // restart even though the roster already named the dataset correctly.
    let sql = body_json(&handles.live, "/gamma/sql?q=SELECT%201").await;
    assert_eq!(
        sql["provenance"]["nid"].as_str(),
        Some(gamma_nid.as_str()),
        "a live-mounted nest's /sql provenance must name its nid, not null: {sql}"
    );

    // The record must be durable - `mounts.toml` is the record RFC-0027 §5 promises survives a
    // restart, and the whole point of fixing the mount half is that it now behaves like the unmount
    // half already did.
    let reloaded = runtime::MountTable::load(roost_dir.path()).expect("mounts.toml still parses");
    let gamma_record = reloaded
        .mounts
        .iter()
        .find(|m| m.alias == "gamma")
        .expect("the live mount must be written back to mounts.toml, not vanish on restart");
    assert_eq!(gamma_record.nid, gamma_nid);
    assert!(
        reloaded.mounts.iter().any(|m| m.alias == "usdc"),
        "the pre-existing mount must survive untouched"
    );
}

/// NIG-185 / the #544 review finding: `nid` from the admin body reached `MountTable::data_dir` with
/// nothing validating it in between, and `Path::join` discards the mount root outright when the
/// argument is absolute or `..`-relative. The consequence that matters is not the 400 by itself - it
/// is that an admitted bad `nid` gets to `persist_mounted_nests` (RFC-0027 §5's "converge on
/// restart" write), and the very next `MountTable::load` calls `validate_mounts`, which has always
/// refused a non-64-hex record. `load` is the single entry point for the whole table, so that is not
/// "one nest fails" - it is every nest in the runtime failing to start, from a call that looked like
/// it worked.
///
/// The traversal shape is what makes this reproducible without any coincidence: `../spare/legacy` is
/// not a NID by any definition, but it is a real, valid, already-scaffolded nest directory once
/// `data_dir` joins it onto the mount root and `..` walks back out - so *with the guard removed* this
/// mount would fully succeed, right up through the cursor handshake, and the record would get
/// written. That is what gives the final assertion teeth: delete the validation and this test does
/// not merely fail to see a 400, it watches `MountTable::load` refuse the file the "successful" call
/// just wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_nid_is_rejected_before_the_runtime_stops_loading() {
    const TOKEN: &str = "s3cret-admin-token";

    let roost_dir = tempfile::tempdir().unwrap();
    let usdc_nid = "bb22".repeat(16);
    let usdc_dir = runtime::MountTable::data_dir(roost_dir.path(), &usdc_nid);
    std::fs::create_dir_all(&usdc_dir).unwrap();

    std::fs::write(
        roost_dir.path().join(runtime::MOUNTS_FILE),
        format!(
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
             [[mounts]]\nalias = \"usdc\"\nnid = \"{usdc_nid}\"\n"
        ),
    )
    .unwrap();

    // A real, valid nest a traversal-style `nid` can reach *without* being mounted itself - the
    // danger here is not "no directory exists there", it is "one does".
    let spare_dir = roost_dir.path().join("spare/legacy");
    std::fs::create_dir_all(&spare_dir).unwrap();
    scaffold_nest(&spare_dir, "legacy", ARB);
    let traversal_nid = "../spare/legacy";

    let tape = Arc::new(TapeSource::new());
    let cfg_u = scaffold_nest(&usdc_dir, "usdc", USDC);
    let health = Arc::new(RuntimeHealth::new());
    health.register("usdc", "arbitrum-one");

    let cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![("usdc".to_string(), usdc_dir.to_path_buf(), cfg_u)],
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

    let roster = serde_json::json!({"runtime": "test", "nests": [{"name": "usdc"}]});
    let live = serve::LiveRuntime::new(serve::compose_runtime(
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
        estimates: std::collections::HashMap::from([("usdc".to_string(), 90)]),
        default_tenant: "default".to_string(),
        suspended: Default::default(),
        joining: Default::default(),
        mount_ctx: runtime::MountContext {
            dir: roost_dir.path().to_path_buf(),
            mounts: vec![runtime::Mount {
                tenant: "default".to_string(),
                alias: "usdc".to_string(),
                nid: usdc_nid.clone(),
                sql: Default::default(),
                queries: Vec::new(),
                publish: None,
                #[cfg(feature = "counter")]
                counter: None,
            }],
            sources: std::collections::HashMap::from([(
                "arbitrum-one".to_string(),
                tape.clone() as Arc<dyn nuthatch::source::Source>,
            )]),
            endpoint_counts: std::collections::HashMap::from([("arbitrum-one".to_string(), 1)]),
            backfill: None,
            seal_direct: false,
            concurrency: 1,
            ipfs_window_deadline: nuthatch::ipfs_resolve::WINDOW_DEADLINE,
            window_override: Some(2),
            admin_enabled: true,
            admin_token: Some(TOKEN.to_string()),
            max_rss_mb: 2048,
            freshness: Default::default(),
            chain_freshness: Default::default(),
            dormant: Default::default(),
            fail_fast: false,
            cursors: None,
            registry: None,
        },
    };
    std::mem::forget(cursor.ingest);

    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let routes =
        runtime::lifecycle_routes(handles.clone(), test_jobs(), true, Some(TOKEN.to_string()));

    let (status, body) = call(
        &routes,
        "POST",
        &format!("/_admin/nests?token={TOKEN}"),
        None,
        Some(&format!(r#"{{"name":"legacy","nid":"{traversal_nid}"}}"#)),
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::BAD_REQUEST,
        "a nid that is not 64 hex characters must be refused, not resolved: {body}"
    );

    assert!(
        !handles
            .lock()
            .await
            .states
            .iter()
            .any(|(n, _)| n == "legacy"),
        "the refused mount must not have taken effect"
    );

    // The assertion with teeth: `mounts.toml` was never touched, so the table the whole runtime
    // depends on still parses and still holds exactly what it held before the call.
    let reloaded =
        runtime::MountTable::load(roost_dir.path()).expect("mounts.toml must still load");
    assert_eq!(reloaded.mounts.len(), 1, "no record for the refused mount");
    assert_eq!(reloaded.mounts[0].alias, "usdc");
    assert_eq!(reloaded.mounts[0].nid, usdc_nid);
}

/// RFC-0027 §2: when `admin_enabled` is false (the default for any non-localhost bind that does not
/// supply `--admin-token`), `lifecycle_routes` must return an empty router - 404 on every path, not
/// 401. The distinction matters: a 401 proves the route exists but rejected the caller; a 404 proves
/// the route was never registered and no handler code ran.
///
/// This is Guard 1 of the two guards named in issue #400. Guard 2 (the credential check on an
/// enabled surface) is covered by `the_lifecycle_routes_demand_the_admin_token_before_they_act`.
///
/// The test includes a premise block that confirms the routes DO exist when `admin_enabled: true`.
/// Without that block the test passes trivially against a `lifecycle_routes` that always returns
/// an empty router, which is exactly the false-green condition the sprint theme names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_disabled_admin_surface_exposes_no_lifecycle_routes() {
    const TOKEN: &str = "gate-test-token";

    let roost_dir = tempfile::tempdir().unwrap();
    let usdc_dir = roost_dir.path().join("nests/usdc");
    let arb_dir = roost_dir.path().join("nests/arb");
    std::fs::create_dir_all(&usdc_dir).unwrap();
    std::fs::create_dir_all(&arb_dir).unwrap();
    let (handles, _tape) = two_nest_roost(roost_dir.path(), &usdc_dir, &arb_dir).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));

    // Premise: with admin_enabled: true the routes exist - an unauthenticated call gets 401, not
    // 404. This rules out a `lifecycle_routes` that always returns Router::new().
    let enabled_routes =
        runtime::lifecycle_routes(handles.clone(), test_jobs(), true, Some(TOKEN.to_string()));
    let (premise_status, _) = call(
        &enabled_routes,
        "POST",
        "/_admin/nests",
        None,
        Some(r#"{"name":"premise"}"#),
    )
    .await;
    assert_ne!(
        premise_status,
        axum::http::StatusCode::NOT_FOUND,
        "premise: POST /_admin/nests must exist (non-404) when admin is enabled - \
         if this fails the gate test is testing nothing"
    );
    drop(enabled_routes);

    // admin_enabled: false - this is the posture for any localhost bind without an explicit token
    // or any remote bind that omits --admin-token.
    let routes = runtime::lifecycle_routes(handles.clone(), test_jobs(), false, None);

    let (mount_status, _) = call(
        &routes,
        "POST",
        "/_admin/nests",
        None,
        Some(r#"{"name":"third"}"#),
    )
    .await;
    assert_eq!(
        mount_status,
        axum::http::StatusCode::NOT_FOUND,
        "POST /_admin/nests must be absent (404), not refused (401), when admin is disabled"
    );

    let (unmount_status, _) = call(&routes, "DELETE", "/_admin/nests/usdc", None, None).await;
    assert_eq!(
        unmount_status,
        axum::http::StatusCode::NOT_FOUND,
        "DELETE /_admin/nests/{{name}} must be absent (404), not refused (401), when admin is disabled"
    );

    // The guard is structural, not positional - no code ran, so both nests must still be present.
    let names: Vec<String> = handles
        .lock()
        .await
        .states
        .iter()
        .map(|(n, _)| n.clone())
        .collect();
    assert_eq!(
        names,
        vec!["usdc".to_string(), "arb".to_string()],
        "neither nest must have been affected - the empty router must have returned before any handler ran"
    );
}

/// Drive one request through the lifecycle routes as an HTTP caller would, and return
/// (status, body). Built from `lifecycle_routes` itself rather than from the handlers, so route
/// registration and extractor order are part of what is under test.
async fn call(
    routes: &axum::Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<&str>,
) -> (axum::http::StatusCode, String) {
    use tower::ServiceExt;
    let mut req = axum::http::Request::builder().method(method).uri(uri);
    if let Some(t) = bearer {
        req = req.header(axum::http::header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let req = match body {
        Some(b) => req
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(b.to_string()))
            .unwrap(),
        None => req.body(axum::body::Body::empty()).unwrap(),
    };
    let resp = routes.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// RFC-0027 §5: mount and unmount are the highest-privilege operations the runtime exposes - they add
/// and remove nests on a live process - and off-localhost they are gated by the admin credential.
///
/// **Nothing exercised that gate.** Every lifecycle test above calls `handles.mount`/`unmount`
/// directly, which is the handler-free path, and `lifecycle_routes` was never constructed by any
/// test in the repo. Deleting either `token_ok` line therefore broke nothing, and an unauthenticated
/// caller could mount a nest on a public bind.
///
/// The assertion is the refusal **and its silence**: the call must be turned away *before* the
/// effect, so the running set is unchanged afterwards. A 401 that mounted the nest anyway would pass
/// a status-code test and fail the only property that matters. Both credential forms are exercised
/// (`?token=` on one route, `Authorization: Bearer` on the other) because both are accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lifecycle_routes_demand_the_admin_token_before_they_act() {
    const TOKEN: &str = "s3cret-admin-token";

    let roost_dir = tempfile::tempdir().unwrap();
    let usdc_dir = roost_dir.path().join("nests/usdc");
    let arb_dir = roost_dir.path().join("nests/arb");
    std::fs::create_dir_all(&usdc_dir).unwrap();
    std::fs::create_dir_all(&arb_dir).unwrap();
    let (handles, _tape) = two_nest_roost(roost_dir.path(), &usdc_dir, &arb_dir).await;

    // A genuinely mountable nest: same chain, within budget. So with the guard removed the
    // unauthenticated call does not merely reach the handler, it *succeeds* - which is the finding.
    let third = roost_dir.path().join("nests/third");
    std::fs::create_dir_all(&third).unwrap();
    scaffold_nest(&third, "third", ARB);

    let live_service = handles.live.service();
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    // The posture an off-localhost bind derives: surface on, credential required.
    let routes =
        runtime::lifecycle_routes(handles.clone(), test_jobs(), true, Some(TOKEN.to_string()));

    // 1. An unauthenticated mount is refused, and mounts nothing.
    let (status, _) = call(
        &routes,
        "POST",
        "/_admin/nests",
        None,
        Some(r#"{"name":"third"}"#),
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::UNAUTHORIZED,
        "an unauthenticated POST /_admin/nests must be refused"
    );
    let names: Vec<String> = handles
        .lock()
        .await
        .states
        .iter()
        .map(|(n, _)| n.clone())
        .collect();
    assert_eq!(
        names,
        vec!["usdc".to_string(), "arb".to_string()],
        "the refusal must land before the effect - an unauthenticated caller mounted a nest"
    );

    // 2. An unauthenticated unmount is refused, and the nest keeps serving.
    let (status, _) = call(&routes, "DELETE", "/_admin/nests/arb", None, None).await;
    assert_eq!(
        status,
        axum::http::StatusCode::UNAUTHORIZED,
        "an unauthenticated DELETE /_admin/nests/{{name}} must be refused"
    );
    // #1533: a multi-tenant route key has a slash. `{name}` was one segment, so this 404'd and the
    // nest could not be named. 401 means the route matched and the guard ran.
    let (status, _) = call(&routes, "DELETE", "/_admin/nests/acme/usdc", None, None).await;
    assert_eq!(
        status,
        axum::http::StatusCode::UNAUTHORIZED,
        "DELETE /_admin/nests/acme/usdc must reach the handler, not 404"
    );
    let (status, _) = call(&live_service, "GET", "/arb/health", None, None).await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "the refused unmount must have left the nest serving"
    );

    // 3. Positive controls: with the credential the same two routes reach their handlers, so the 401s
    //    above are the guard talking and not a broken route. `usdc` is already mounted on no nid, so
    //    another nid under it is RFC-0027 §3's AlreadyMounted refusal - it proves the mount logic ran.
    let (status, body) = call(
        &routes,
        "POST",
        &format!("/_admin/nests?token={TOKEN}&wait=true"),
        None,
        Some(&format!(r#"{{"name":"usdc","nid":"{}"}}"#, "0a".repeat(32))),
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::CONFLICT,
        "with the token the mount must reach the handler: {body}"
    );
    // The unmount half, via the header form, on a name that is not mounted: idempotent no-op, so the
    // control costs the fixture nothing.
    let (status, body) = call(
        &routes,
        "DELETE",
        "/_admin/nests/not-mounted",
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "with the token the unmount must reach the handler: {body}"
    );
}

/// #1475: a second name mounted live onto a dataset another mount already holds shares that mount's
/// store, as boot does, and unmounting the mount that indexes it leaves the other one indexing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_live_mount_of_one_dataset_shares_it_and_survives_the_first_unmount() {
    use nuthatch::store::HotStore;

    let roost_dir = tempfile::tempdir().unwrap();
    let nid = "dd44".repeat(16);
    let data_dir = runtime::MountTable::data_dir(roost_dir.path(), &nid);
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(
        roost_dir.path().join(runtime::MOUNTS_FILE),
        format!(
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
             [[mounts]]\nalias = \"v1\"\nnid = \"{nid}\"\n"
        ),
    )
    .unwrap();

    let tape = Arc::new(TapeSource::new());
    let (a1, a2) = (account(1), account(2));
    let block = |b: u64| {
        transfers_block(
            b,
            0,
            1_700_000_000 + b,
            USDC,
            &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
        )
    };
    for b in 1..=3u64 {
        tape.insert_block(b, block(b));
    }
    tape.advance_tip_to(3);

    let cfg = scaffold_nest(&data_dir, "usdc", USDC);
    let health = Arc::new(RuntimeHealth::new());
    health.register("v1", "arbitrum-one");
    let cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![("v1".to_string(), data_dir.clone(), cfg)],
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

    let roster = serde_json::json!({"runtime": "test", "nests": [{"name": "v1"}]});
    let live = serve::LiveRuntime::new(serve::compose_runtime(
        roster.clone(),
        cursor.states.clone(),
        health.clone(),
    ));
    let mut handles = runtime::RuntimeHandles {
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
        estimates: std::collections::HashMap::from([("v1".to_string(), 90)]),
        default_tenant: "default".to_string(),
        suspended: Default::default(),
        joining: Default::default(),
        mount_ctx: runtime::MountContext {
            dir: roost_dir.path().to_path_buf(),
            mounts: vec![runtime::Mount {
                tenant: "default".to_string(),
                alias: "v1".to_string(),
                nid: nid.clone(),
                sql: Default::default(),
                queries: Vec::new(),
                publish: None,
                #[cfg(feature = "counter")]
                counter: None,
            }],
            sources: std::collections::HashMap::from([(
                "arbitrum-one".to_string(),
                tape.clone() as Arc<dyn nuthatch::source::Source>,
            )]),
            endpoint_counts: std::collections::HashMap::from([("arbitrum-one".to_string(), 1)]),
            backfill: None,
            seal_direct: false,
            concurrency: 1,
            ipfs_window_deadline: nuthatch::ipfs_resolve::WINDOW_DEADLINE,
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
    std::mem::forget(cursor.ingest);

    let last_block = |h: &runtime::RuntimeHandles, name: &str| {
        h.states
            .iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, s)| s.store.get_meta("last_block").ok().flatten())
    };
    assert!(
        wait_until(POLL_TIMEOUT, || last_block(&handles, "v1").as_deref()
            == Some("3"))
        .await,
        "premise: v1 indexes to the tip"
    );

    handles
        .mount("v2", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("a second name onto a mounted nid must share the open store, not reopen it");
    let store_of = |h: &runtime::RuntimeHandles, name: &str| {
        h.states
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, s)| s.store.clone())
            .unwrap()
    };
    assert!(
        Arc::ptr_eq(&store_of(&handles, "v1"), &store_of(&handles, "v2")),
        "two mounts of one dataset must hold one store"
    );
    let v1 = body_json(&handles.live, "/v1/sql?q=SELECT%201").await;
    let v2 = body_json(&handles.live, "/v2/sql?q=SELECT%201").await;
    assert!(v1["provenance"]["nid"].is_string(), "{v1}");
    assert_eq!(
        v2["provenance"]["nid"], v1["provenance"]["nid"],
        "both mounts must name the dataset that answered"
    );

    // Unmount the mount the cursor indexes under. v2 must keep serving and keep following the tip.
    handles.unmount("v1").await.expect("unmount v1");
    assert_eq!(
        status(&handles.live, "/v1/health").await,
        axum::http::StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(&handles.live, "/v2/health").await,
        axum::http::StatusCode::OK
    );
    tape.insert_block(4, block(4));
    tape.advance_tip_to(4);
    assert!(
        wait_until(POLL_TIMEOUT, || last_block(&handles, "v2").as_deref()
            == Some("4"))
        .await,
        "unmounting v1 stopped the cursor under v2: last_block {:?}",
        last_block(&handles, "v2")
    );

    // The last mount out releases every holder.
    handles.unmount("v2").await.expect("unmount v2");
    Store::open(&data_dir.join("nuthatch.redb"))
        .expect("after the last mount goes, the shared store must be reopenable");
}

type CursorIntake =
    tokio::sync::mpsc::UnboundedReceiver<(String, tokio::task::JoinHandle<anyhow::Result<()>>)>;

/// A runtime that started with nothing mounted (#1545): `arbitrum-one` has a source and no cursor,
/// and the nest at `data/<nid>/` is recorded but not running.
async fn empty_runtime(
    roost_dir: &std::path::Path,
    nid: &str,
) -> (runtime::RuntimeHandles, Arc<TapeSource>, CursorIntake) {
    let tape = Arc::new(TapeSource::new());
    let (a1, a2) = (account(1), account(2));
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
    let data_dir = runtime::MountTable::data_dir(roost_dir, nid);
    std::fs::create_dir_all(&data_dir).unwrap();
    scaffold_nest(&data_dir, "usdc", USDC);
    write_table_if_absent(roost_dir, &[]);

    let health = Arc::new(RuntimeHealth::new());
    let roster = serde_json::json!({"runtime": "test", "nests": []});
    let live = serve::LiveRuntime::new(serve::compose_runtime(
        roster.clone(),
        Vec::new(),
        health.clone(),
    ));
    let (feed, intake) = tokio::sync::mpsc::unbounded_channel();
    let handles = runtime::RuntimeHandles {
        live,
        states: Vec::new(),
        alert_workers: Vec::new(),
        publishers: Vec::new(),
        lifecycle: Default::default(),
        health,
        roster,
        estimates: Default::default(),
        default_tenant: "default".to_string(),
        suspended: Default::default(),
        joining: Default::default(),
        mount_ctx: runtime::MountContext {
            dir: roost_dir.to_path_buf(),
            mounts: vec![runtime::Mount {
                tenant: "default".to_string(),
                alias: "usdc".to_string(),
                nid: nid.to_string(),
                sql: Default::default(),
                queries: Vec::new(),
                publish: None,
                #[cfg(feature = "counter")]
                counter: None,
            }],
            sources: std::collections::HashMap::from([(
                "arbitrum-one".to_string(),
                tape.clone() as Arc<dyn nuthatch::source::Source>,
            )]),
            endpoint_counts: std::collections::HashMap::from([("arbitrum-one".to_string(), 1)]),
            backfill: None,
            seal_direct: false,
            concurrency: 1,
            ipfs_window_deadline: nuthatch::ipfs_resolve::WINDOW_DEADLINE,
            window_override: Some(2),
            admin_enabled: false,
            admin_token: None,
            max_rss_mb: 2048,
            freshness: Default::default(),
            chain_freshness: Default::default(),
            dormant: Default::default(),
            fail_fast: false,
            cursors: Some(feed),
            registry: None,
        },
    };
    (handles, tape, intake)
}

/// #1545: the first mount onto a chain with no cursor starts one, which indexes, serves and is
/// handed to the supervisor. Unmounting its only nest leaves that cursor idle rather than ended,
/// and the next mount rejoins it instead of starting a second.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_mount_starts_its_chains_cursor_and_the_cursor_outlives_an_empty_set() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "ab12".repeat(16);
    let (mut handles, tape, mut intake) = empty_runtime(roost.path(), &nid).await;
    assert_eq!(
        status(&handles.live, "/usdc/health").await,
        axum::http::StatusCode::NOT_FOUND,
        "premise: nothing is mounted"
    );
    // As if this name had been unmounted from a cursor on another chain earlier.
    handles.health.retire_nest("usdc");

    handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("the first mount onto a declared chain must start its cursor");
    let (chain, ingest) = intake
        .try_recv()
        .expect("the new cursor must be handed to the supervisor");
    assert_eq!(chain, "arbitrum-one");
    assert_eq!(
        status(&handles.live, "/usdc/health").await,
        axum::http::StatusCode::OK
    );
    assert_eq!(roster_names(&handles.live).await, vec!["usdc".to_string()]);
    assert_eq!(handles.health.json_for("usdc").0, "indexing");
    let last_block = |h: &runtime::RuntimeHandles| {
        h.states
            .iter()
            .find(|(n, _)| n == "usdc")
            .and_then(|(_, s)| s.store.get_meta("last_block").ok().flatten())
    };
    assert!(
        wait_until(POLL_TIMEOUT, || last_block(&handles).as_deref()
            == Some("3"))
        .await,
        "the started cursor never indexed: last_block {:?}",
        last_block(&handles)
    );

    handles.unmount("usdc").await.expect("unmount");
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    assert!(
        !ingest.is_finished(),
        "a cursor emptied by an unmount returned; the last one to do so ends the runtime"
    );

    let (a1, a2) = (account(1), account(2));
    tape.insert_block(
        4,
        transfers_block(
            4,
            0,
            1_700_000_004,
            USDC,
            &[(a1.as_str(), a2.as_str(), 400)],
        ),
    );
    tape.advance_tip_to(4);
    handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("a remount onto the idle cursor");
    assert!(
        intake.try_recv().is_err(),
        "the remount started a second cursor on a chain that already had one"
    );
    assert!(
        wait_until(POLL_TIMEOUT, || last_block(&handles).as_deref()
            == Some("4"))
        .await,
        "the remounted nest does not follow the tip: last_block {:?}",
        last_block(&handles)
    );
    assert_eq!(
        handles.health.json_for("usdc").0,
        "indexing",
        "the remount must not report the earlier unmount's retirement"
    );
    ingest.abort();
}

/// #1545: a chain whose cursor died stays quarantined until restart. A mount onto it is refused
/// rather than starting a second cursor under the quarantine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_onto_a_chain_whose_cursor_died_is_refused() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "cd34".repeat(16);
    let (mut handles, _tape, mut intake) = empty_runtime(roost.path(), &nid).await;
    handles
        .health
        .quarantine_cursor("arbitrum-one", "finality violation".to_string());

    let err = handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("restart"), "{err:#}");
    assert!(handles.states.is_empty());
    assert!(intake.try_recv().is_err(), "no cursor may start");
}

/// #1545: a declared chain with no cursor is dialled on its first mount, not at boot. A dormant
/// chain with no endpoint is refused by name, and one with another chain id is not this chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dormant_chain_is_opened_by_its_first_mount() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "ef56".repeat(16);
    let (mut handles, _tape, mut intake) = empty_runtime(roost.path(), &nid).await;
    handles.mount_ctx.sources.clear();
    handles.mount_ctx.dormant.insert(
        "arbitrum-one".to_string(),
        runtime::DormantChain {
            endpoint: runtime::ChainEndpoint {
                chain: "arbitrum-one".to_string(),
                chain_id: 1,
                rpc_urls: Vec::new(),
            },
            rpc_fallback: Vec::new(),
        },
    );
    let err = handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<runtime::MountRefusal>(),
            Some(runtime::MountRefusal::UndeclaredChain { .. })
        ),
        "a dormant chain with another chain id must not take the nest: {err:#}"
    );

    handles
        .mount_ctx
        .dormant
        .get_mut("arbitrum-one")
        .unwrap()
        .endpoint
        .chain_id = 42161;
    let err = handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("has no rpc_urls"),
        "the dormant chain must be opened, and its missing endpoint named: {err:#}"
    );
    assert!(handles.states.is_empty());
    assert!(intake.try_recv().is_err());
}

/// A registry holding one published nest, and that nest's NID as `nuthatch nest nid` prints it.
async fn registry_with_one_nest(registry: &std::path::Path) -> String {
    let src = tempfile::tempdir().unwrap();
    scaffold_nest(src.path(), "usdc", USDC);
    let nid = nuthatch::blob::nest_nid(src.path()).unwrap();
    let bundle = tempfile::tempdir().unwrap();
    let file = bundle.path().join("usdc.bundle");
    nuthatch::blob::bundle(src.path(), Some(&file), false, false).unwrap();
    let store = nuthatch::distribution::open(registry.to_str().unwrap()).unwrap();
    nuthatch::distribution::publish(store.as_ref(), &file, Some("usdc"), None)
        .await
        .unwrap();
    nid
}

/// Nothing but datasets under `data/`: no fetch was left staged.
fn no_fetch_left_behind(roost: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(roost.join("data")) else {
        return;
    };
    for e in entries.flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        assert!(!n.starts_with(".fetch-"), "a staged fetch was left: {n}");
    }
}

/// #1543: a mount naming a NID the runtime does not hold fetches it from the registry, verifies it,
/// installs it at `data/<nid>/` and indexes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_fetches_a_nid_the_runtime_does_not_hold() {
    let roost = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let nid = registry_with_one_nest(registry.path()).await;
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    let data_dir = runtime::MountTable::data_dir(roost.path(), &nid);
    std::fs::remove_dir_all(&data_dir).unwrap();
    handles.mount_ctx.registry = Some(registry.path().to_str().unwrap().to_string());

    handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("a mount by nid must fetch what the runtime does not hold");
    assert!(data_dir.join(nuthatch::config::CONFIG_FILE).exists());
    no_fetch_left_behind(roost.path());
    let last_block = |h: &runtime::RuntimeHandles| {
        h.states
            .iter()
            .find(|(n, _)| n == "usdc")
            .and_then(|(_, s)| s.store.get_meta("last_block").ok().flatten())
    };
    assert!(
        wait_until(POLL_TIMEOUT, || last_block(&handles).as_deref()
            == Some("3"))
        .await,
        "the fetched nest never indexed: last_block {:?}",
        last_block(&handles)
    );
}

/// #1543: without `--registry` the refusal names the flag; a NID the registry lacks is refused before
/// anything is written; and a fetched nest the runtime then refuses is removed again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_that_cannot_or_may_not_fetch_leaves_nothing_behind() {
    let roost = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let nid = registry_with_one_nest(registry.path()).await;
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    let data_dir = runtime::MountTable::data_dir(roost.path(), &nid);
    std::fs::remove_dir_all(&data_dir).unwrap();
    let parse = || Some(runtime::Nid::parse(&nid).unwrap());

    let err = handles.mount("usdc", parse()).await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<runtime::MountRefusal>(),
            Some(runtime::MountRefusal::NotHeld { .. })
        ),
        "{err:#}"
    );
    assert!(format!("{err:#}").contains("--registry"), "{err:#}");
    assert!(!data_dir.exists());

    let empty = tempfile::tempdir().unwrap();
    handles.mount_ctx.registry = Some(empty.path().to_str().unwrap().to_string());
    let err = handles.mount("usdc", parse()).await.unwrap_err();
    assert!(format!("{err:#}").contains("not found"), "{err:#}");
    assert!(
        !data_dir.exists(),
        "a nid the registry lacks wrote a dataset"
    );
    no_fetch_left_behind(roost.path());

    handles.mount_ctx.registry = Some(registry.path().to_str().unwrap().to_string());
    handles.mount_ctx.max_rss_mb = 100;
    let err = handles.mount("usdc", parse()).await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<runtime::MountRefusal>(),
            Some(runtime::MountRefusal::OverBudget { .. })
        ),
        "{err:#}"
    );
    assert!(
        !data_dir.exists(),
        "a refused mount kept the dataset it fetched"
    );
    assert!(handles.states.is_empty());
}

/// A mount-jobs index in a directory of its own, kept for the life of the test process.
fn test_jobs() -> Arc<nuthatch::mount_jobs::MountJobs> {
    let dir = tempfile::tempdir().unwrap().keep();
    Arc::new(nuthatch::mount_jobs::MountJobs::load(&dir))
}

/// Poll `GET /_admin/mounts/<name>` until the job reaches `phase`, returning its last body.
async fn wait_for_phase(routes: &axum::Router, name: &str, phase: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let (_, body) = call(routes, "GET", &format!("/_admin/mounts/{name}"), None, None).await;
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        if body["phase"] == phase || std::time::Instant::now() > deadline {
            return body;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// #1544: a mount answers 202 at once, and its progress is readable, even while the runtime's lock
/// is held, until it goes live. A second POST of the same name and NID is idempotent; another NID
/// under that name is a conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_is_accepted_at_once_and_read_until_it_is_live() {
    let roost = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let nid = registry_with_one_nest(registry.path()).await;
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    std::fs::remove_dir_all(runtime::MountTable::data_dir(roost.path(), &nid)).unwrap();
    handles.mount_ctx.registry = Some(registry.path().to_str().unwrap().to_string());
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);
    let body = format!(r#"{{"name":"usdc","nid":"{nid}"}}"#);

    // Hold the lock the mount needs: acceptance and status must not wait on it.
    let held = handles.lock().await;
    let (status, accepted) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        call(&routes, "POST", "/_admin/nests", None, Some(&body)),
    )
    .await
    .expect("a POST waited on the runtime's lock");
    assert_eq!(status, axum::http::StatusCode::ACCEPTED, "{accepted}");
    let (status, _) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        call(&routes, "GET", "/_admin/mounts", None, None),
    )
    .await
    .expect("a status read waited on the runtime's lock");
    assert_eq!(status, axum::http::StatusCode::OK);
    let (status, again) = call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    assert_eq!(
        status,
        axum::http::StatusCode::ACCEPTED,
        "an in-flight re-POST: {again}"
    );
    drop(held);

    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert_eq!(
        status_of(&handles).await,
        axum::http::StatusCode::OK,
        "the mount went live without its routes"
    );
    let (status, _) = call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "a re-POST of a live mount"
    );
    let other = format!(r#"{{"name":"usdc","nid":"{}"}}"#, "0a".repeat(32));
    let (status, _) = call(&routes, "POST", "/_admin/nests", None, Some(&other)).await;
    assert_eq!(status, axum::http::StatusCode::CONFLICT);
}

/// The `nuthatch_nest_health` value the runtime's `/metrics` reports for `nest`, if any.
async fn nest_health_series(live: &serve::LiveRuntime, nest: &str) -> Option<String> {
    use tower::ServiceExt;
    let req = axum::http::Request::get("/metrics")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = live.service().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let prefix = format!("nuthatch_nest_health{{nest=\"{nest}\",");
    String::from_utf8_lossy(&bytes)
        .lines()
        .find(|l| l.starts_with(&prefix))
        .map(str::to_string)
}

/// Every `/metrics` line labelled with `nest`.
async fn series_naming(live: &serve::LiveRuntime, nest: &str) -> Vec<String> {
    use tower::ServiceExt;
    let req = axum::http::Request::get("/metrics")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = live.service().oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let label = format!("nest=\"{nest}\"");
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter(|l| l.contains(&label))
        .map(str::to_string)
        .collect()
}

/// #1648: an unmounted mount leaves `nuthatch_nest_health`, whether it was an alias of a dataset
/// another mount still indexes or the last mount of one. It used to read 1 for ever for the alias,
/// and 0 for the other until restart, which reads as quarantined.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unmounted_nest_leaves_the_health_series() {
    // Per-nest series live in the process-wide METRICS, keyed by mount name, so a neighbour test
    // mounting `usdc` puts its own `nest="usdc"` lines in this test's /metrics (#1776).
    const FIRST: &str = "health-first";
    const ALIAS: &str = "health-alias";
    let roost = tempfile::tempdir().unwrap();
    let nid = "7d".repeat(32);
    let (mut handles, _tape) = one_live_mount_as(roost.path(), &nid, FIRST).await;
    handles
        .mount(ALIAS, Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("a second mount of the dataset");
    for nest in [FIRST, ALIAS] {
        let line = nest_health_series(&handles.live, nest).await;
        assert!(
            line.as_deref().is_some_and(|l| l.ends_with(" 1")),
            "premise: {nest} is reported indexing: {line:?}"
        );
    }

    handles.unmount(ALIAS).await.expect("unmount the alias");
    assert_eq!(
        nest_health_series(&handles.live, ALIAS).await,
        None,
        "the unmounted alias is still reported"
    );
    handles
        .unmount(FIRST)
        .await
        .expect("unmount the last mount");
    assert_eq!(
        nest_health_series(&handles.live, FIRST).await,
        None,
        "the unmounted nest is still reported"
    );

    // The other order: the first mount goes while the alias still reads the dataset, and the
    // alias takes the dataset's key on the cursor.
    for name in [FIRST, ALIAS] {
        handles
            .mount(name, Some(runtime::Nid::parse(&nid).unwrap()))
            .await
            .expect("remount");
    }
    for nest in [FIRST, ALIAS] {
        let line = nest_health_series(&handles.live, nest).await;
        assert!(
            line.as_deref().is_some_and(|l| l.ends_with(" 1")),
            "a remount onto the running cursor is not reported: {nest} {line:?}"
        );
    }
    handles
        .unmount(FIRST)
        .await
        .expect("unmount the first mount");
    assert_eq!(
        nest_health_series(&handles.live, FIRST).await,
        None,
        "the first mount is still reported after it was unmounted, while an alias remains"
    );
    let line = nest_health_series(&handles.live, ALIAS).await;
    assert!(
        line.as_deref().is_some_and(|l| l.ends_with(" 1")),
        "the alias left on the dataset lost its health series: {line:?}"
    );
    // The dataset is still indexed, and the series it emits now carry the mount that is left.
    let gone = series_naming(&handles.live, FIRST).await;
    assert!(
        gone.is_empty(),
        "series still name the unmounted first mount: {gone:?}"
    );
    handles
        .unmount(ALIAS)
        .await
        .expect("unmount the last mount");
    for nest in [FIRST, ALIAS] {
        assert_eq!(
            nest_health_series(&handles.live, nest).await,
            None,
            "{nest} is still reported once the dataset has no mount"
        );
    }
}

/// #1673: `/_admin/…` belongs to the lifecycle routes, so a mount whose route would start there is
/// refused over the API, and boot refuses a record that names one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_named_admin_is_refused_over_the_api_and_at_boot() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "7c".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs.clone(), true, None);

    for name in ["_admin", "_admin/suspend", "_admin/mounts", "acme/_admin"] {
        let body = format!(r#"{{"name":"{name}","nid":"{nid}"}}"#);
        for uri in ["/_admin/nests", "/_admin/nests?wait=true"] {
            let (status, answer) = call(&routes, "POST", uri, None, Some(&body)).await;
            assert_eq!(
                status,
                axum::http::StatusCode::BAD_REQUEST,
                "{uri} accepted '{name}': {answer}"
            );
            assert!(answer.contains("reserved"), "{name}: {answer}");
        }
        assert!(jobs.get(name).is_none(), "'{name}' was recorded as a job");
    }
    assert_eq!(handles.lock().await.states.len(), 1);

    for (tenant, alias) in [("default", "_admin"), ("_admin", "usdc")] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(runtime::MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
                 chain_id = 42161\nrpc_urls = []\n\n[[mounts]]\ntenant = \"{tenant}\"\n\
                 alias = \"{alias}\"\nnid = \"{nid}\"\n"
            ),
        )
        .unwrap();
        let err = runtime::MountTable::load(dir.path())
            .expect_err("boot loaded a mount routed under /_admin");
        assert!(format!("{err:#}").contains("reserved"), "{err:#}");
    }
}

/// #1646: `?wait=true` is idempotent as the job route is. A repeat of a live mount's name and NID
/// answers 200, and another NID under the name is still a 409.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_waited_mount_repeated_is_idempotent() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "7a".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);

    for body in [
        format!(r#"{{"name":"usdc","nid":"{nid}"}}"#),
        r#"{"name":"usdc"}"#.to_string(),
    ] {
        for uri in ["/_admin/nests", "/_admin/nests?wait=true"] {
            let (status, answer) = call(&routes, "POST", uri, None, Some(&body)).await;
            assert_eq!(
                status,
                axum::http::StatusCode::OK,
                "a repeat of a live mount on {uri} with {body}: {answer}"
            );
        }
        let (_, dry) = call(
            &routes,
            "POST",
            "/_admin/nests?dry_run=true",
            None,
            Some(&body),
        )
        .await;
        let dry: serde_json::Value = serde_json::from_str(&dry).unwrap();
        assert!(
            dry["refusal_status"].is_null(),
            "the dry run refuses what the mount answers 200: {dry}"
        );
    }
    let other = format!(r#"{{"name":"usdc","nid":"{}"}}"#, "7b".repeat(32));
    for uri in ["/_admin/nests", "/_admin/nests?wait=true"] {
        let (status, answer) = call(&routes, "POST", uri, None, Some(&other)).await;
        assert_eq!(status, axum::http::StatusCode::CONFLICT, "{uri}: {answer}");
    }
    assert_eq!(handles.lock().await.states.len(), 1);
    assert_eq!(status_of(&handles).await, axum::http::StatusCode::OK);
}

async fn status_of(
    handles: &Arc<tokio::sync::Mutex<runtime::RuntimeHandles>>,
) -> axum::http::StatusCode {
    let h = handles.lock().await;
    status(&h.live, "/usdc/health").await
}

/// #1588: a trailing slash answers as its bare form does, in every state a mount can be in.
async fn slash_agrees(
    handles: &Arc<tokio::sync::Mutex<runtime::RuntimeHandles>>,
    want: axum::http::StatusCode,
) {
    let h = handles.lock().await;
    for path in ["/usdc", "/usdc/", "/usdc/health/"] {
        assert_eq!(status(&h.live, path).await, want, "{path}");
    }
}

/// #1544: a mount that fails says why, and leaves nothing on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_mount_is_reported_with_its_reason() {
    let roost = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let nid = registry_with_one_nest(registry.path()).await;
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    let data_dir = runtime::MountTable::data_dir(roost.path(), &nid);
    std::fs::remove_dir_all(&data_dir).unwrap();
    let empty = tempfile::tempdir().unwrap();
    handles.mount_ctx.registry = Some(empty.path().to_str().unwrap().to_string());
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);

    let body = format!(r#"{{"name":"usdc","nid":"{nid}"}}"#);
    let (status, _) = call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED);
    let job = wait_for_phase(&routes, "usdc", "failed").await;
    assert_eq!(job["phase"], "failed", "{job}");
    assert!(
        job["reason"].as_str().unwrap_or("").contains("not found"),
        "{job}"
    );
    assert!(!data_dir.exists());
}

/// #1544: a restart mid-mount resumes the job, after clearing the fetch the killed process left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_resumes_an_interrupted_mount() {
    let roost = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let nid = registry_with_one_nest(registry.path()).await;
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    std::fs::remove_dir_all(runtime::MountTable::data_dir(roost.path(), &nid)).unwrap();
    handles.mount_ctx.registry = Some(registry.path().to_str().unwrap().to_string());
    let stale = roost.path().join("data/.fetch-killed");
    std::fs::create_dir_all(stale.join("nest")).unwrap();
    std::fs::write(
        roost.path().join(nuthatch::mount_jobs::JOBS_FILE),
        format!(r#"[{{"name":"usdc","nid":"{nid}","phase":"fetching","since_unixtime":1}}]"#),
    )
    .unwrap();

    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    assert!(!stale.exists(), "the killed fetch was left staged");
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(
        job["phase"], "live",
        "the interrupted mount was not resumed: {job}"
    );
}

/// `empty_runtime` with a `mounts.toml` declaring its chain, and `usdc` mounted and at the tip.
async fn one_live_mount(
    roost: &std::path::Path,
    nid: &str,
) -> (runtime::RuntimeHandles, Arc<TapeSource>) {
    one_live_mount_as(roost, nid, "usdc").await
}

/// [`one_live_mount`] under another name.
async fn one_live_mount_as(
    roost: &std::path::Path,
    nid: &str,
    name: &str,
) -> (runtime::RuntimeHandles, Arc<TapeSource>) {
    std::fs::write(
        roost.join(runtime::MOUNTS_FILE),
        "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n",
    )
    .unwrap();
    let (mut handles, tape, _intake) = empty_runtime(roost, nid).await;
    handles
        .mount(name, Some(runtime::Nid::parse(nid).unwrap()))
        .await
        .expect("mount");
    assert!(
        wait_until(POLL_TIMEOUT, || last_block(&handles, name).as_deref()
            == Some("3"))
        .await,
        "premise: {name} indexes to the tip"
    );
    (handles, tape)
}

fn usdc_last_block(h: &runtime::RuntimeHandles) -> Option<String> {
    last_block(h, "usdc")
}

fn last_block(h: &runtime::RuntimeHandles, name: &str) -> Option<String> {
    h.states
        .iter()
        .find(|(n, _)| n == name)
        .and_then(|(_, s)| s.store.get_meta("last_block").ok().flatten())
}

/// #1548: a suspended mount answers a named 503, lets go of its store, keeps its data and record,
/// stays suspended across a restart, and does not hold `/ready` down. Resuming catches it up from
/// where it stopped, and releases a quarantine it was in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_suspended_mount_keeps_its_place_and_resumes_from_it() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "5a".repeat(32);
    let (mut handles, tape) = one_live_mount(roost.path(), &nid).await;
    handles
        .health
        .quarantine_nest("usdc", "a view failed".to_string(), 1, None);

    handles.suspend("usdc").await.expect("suspend");
    let body = body_json(&handles.live, "/usdc/health").await;
    assert_eq!(body["suspended"], true, "{body}");
    assert_eq!(
        status(&handles.live, "/usdc/health").await,
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let db = runtime::MountTable::data_dir(roost.path(), &nid).join("nuthatch.redb");
    drop(Store::open(&db).expect("a suspended mount must let go of its store"));
    assert_eq!(handles.health.json_for("usdc").0, "suspended");
    assert!(
        handles.health.all_indexing(),
        "a pause must not fail /ready"
    );

    let file = runtime::MountTable::load(roost.path()).unwrap();
    assert_eq!(file.runtime.suspended, vec!["usdc".to_string()]);
    let (active, suspended) = runtime::split_suspended(&file);
    assert!(
        active.mounts.is_empty(),
        "a restart would index the suspended mount"
    );
    assert_eq!(suspended.get("usdc"), Some(&nid));

    let (a1, a2) = (account(1), account(2));
    tape.insert_block(
        4,
        transfers_block(
            4,
            0,
            1_700_000_004,
            USDC,
            &[(a1.as_str(), a2.as_str(), 400)],
        ),
    );
    tape.advance_tip_to(4);
    handles
        .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("resume");
    assert!(
        wait_until(POLL_TIMEOUT, || usdc_last_block(&handles).as_deref()
            == Some("4"))
        .await,
        "the resumed mount did not catch up: {:?}",
        usdc_last_block(&handles)
    );
    assert_eq!(
        status(&handles.live, "/usdc/health").await,
        axum::http::StatusCode::OK
    );
    assert_eq!(handles.health.json_for("usdc").0, "indexing");
    let file = runtime::MountTable::load(roost.path()).unwrap();
    assert!(file.runtime.suspended.is_empty());
    assert_eq!(file.mounts.len(), 1);
}

/// #1764: a suspend returns once the store is closed. A request still running against the nest when
/// its routes go holds the store, and a resume before it finishes would fail to open it, so the
/// suspend waits for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_suspend_returns_only_once_an_in_flight_request_has_let_go_of_the_store() {
    use tower::ServiceExt;
    let roost = tempfile::tempdir().unwrap();
    let nid = "5b".repeat(32);
    let (mut handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let mut in_flight = handles
        .states
        .iter()
        .find(|(n, _)| n == "usdc")
        .map(|(_, s)| s.clone());
    let served = handles.live.service();
    let routes_gone = || async {
        loop {
            let req = axum::http::Request::builder()
                .uri("/usdc/health")
                .body(axum::body::Body::empty())
                .unwrap();
            if served.clone().oneshot(req).await.unwrap().status() != axum::http::StatusCode::OK {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    };

    let suspend = handles.suspend("usdc");
    tokio::pin!(suspend);
    let suspended = loop {
        tokio::select! {
            biased;
            r = &mut suspend => break r,
            // The request finishes once the routes have gone, as a slow one would.
            _ = routes_gone(), if in_flight.is_some() => in_flight = None,
        }
    };
    suspended.expect("suspend");
    let db = runtime::MountTable::data_dir(roost.path(), &nid).join("nuthatch.redb");
    drop(Store::open(&db).expect("suspend returned while a request still held the store"));
}

/// #1548 over HTTP: suspend and resume by name, with the refusals a caller can hit, and an unmount
/// of a suspended mount dropping its record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspend_and_resume_over_the_admin_api() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "6b".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);

    let (status, _) = call(&routes, "POST", "/_admin/suspend/nope", None, Some("{}")).await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
    let (status, _) = call(&routes, "POST", "/_admin/resume/usdc", None, Some("{}")).await;
    assert_eq!(
        status,
        axum::http::StatusCode::NOT_FOUND,
        "resuming a live mount"
    );

    slash_agrees(&handles, axum::http::StatusCode::OK).await;
    let (status, body) = call(&routes, "POST", "/_admin/suspend/usdc", None, Some("{}")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    slash_agrees(&handles, axum::http::StatusCode::SERVICE_UNAVAILABLE).await;
    let (status, body) = call(&routes, "POST", "/_admin/resume/usdc", None, Some("{}")).await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED, "{body}");
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert_eq!(status_of(&handles).await, axum::http::StatusCode::OK);

    let (status, _) = call(&routes, "POST", "/_admin/suspend/usdc", None, Some("{}")).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let (status, body) = call(&routes, "DELETE", "/_admin/nests/usdc", None, None).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(body.contains("\"was_mounted\":true"), "{body}");
    assert_eq!(status_of(&handles).await, axum::http::StatusCode::NOT_FOUND);
    slash_agrees(&handles, axum::http::StatusCode::NOT_FOUND).await;
    let file = runtime::MountTable::load(roost.path()).unwrap();
    assert!(file.runtime.suspended.is_empty() && file.mounts.is_empty());

    // A retry is still a 200, and says it was a no-op; a name no mount could have is a 400.
    let (status, body) = call(&routes, "DELETE", "/_admin/nests/usdc", None, None).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(body.contains("\"was_mounted\":false"), "{body}");
    let (status, body) = call(&routes, "DELETE", "/_admin/nests/a/b/c", None, None).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
}

/// #1645: a suspended name is still that NID's mount. Mounting another NID under it is a 409 on
/// either route, and leaves the suspension and its record as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mounting_another_nid_over_a_suspended_name_is_refused() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "6e".repeat(32);
    let other = "6f".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    scaffold_nest(
        &runtime::MountTable::data_dir(roost.path(), &other),
        "usdc",
        USDC,
    );
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);
    let (status, body) = call(&routes, "POST", "/_admin/suspend/usdc", None, Some("{}")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");

    let body = format!(r#"{{"name":"usdc","nid":"{other}"}}"#);
    for uri in ["/_admin/nests", "/_admin/nests?wait=true"] {
        let (status, answer) = call(&routes, "POST", uri, None, Some(&body)).await;
        assert_eq!(
            status,
            axum::http::StatusCode::CONFLICT,
            "{uri} mounted another nid over a suspended name: {answer}"
        );
    }
    let job = wait_for_phase(&routes, "usdc", "suspended").await;
    assert_eq!(job["phase"], "suspended", "{job}");
    assert_eq!(job["nid"], nid.as_str(), "{job}");
    let file = runtime::MountTable::load(roost.path()).unwrap();
    assert_eq!(file.runtime.suspended, vec!["usdc".to_string()]);
    assert_eq!(file.mounts.len(), 1);
    assert_eq!(
        file.mounts[0].nid, nid,
        "the suspended record was rewritten"
    );
    assert_eq!(
        status_of(&handles).await,
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
}

/// #1642: a localhost runtime needs no token, so the only thing between a hostile page and the
/// lifecycle routes is the browser. Over the real bind with `--cors '*'`, the page may neither have a
/// preflight granted nor suspend a mount with a form POST, which is sent without one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cross_site_page_cannot_suspend_a_mount_through_the_real_bind() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "6c".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let service = handles
        .lock()
        .await
        .live
        .service()
        .merge(runtime::lifecycle_routes(handles.clone(), jobs, true, None));
    let cors = serve::cors_layer(&["*".to_string()]).unwrap();
    let addr = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().to_string()
    };
    let serving = {
        let addr = addr.clone();
        tokio::spawn(async move { serve::bind_and_serve(&addr, service, cors).await })
    };
    let client = reqwest::Client::new();
    let started = std::time::Instant::now();
    while client
        .get(format!("http://{addr}/usdc/health"))
        .send()
        .await
        .is_err()
    {
        assert!(started.elapsed() < POLL_TIMEOUT, "never came up on {addr}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let url = format!("http://{addr}/_admin/suspend/usdc");

    let preflight = client
        .request(reqwest::Method::OPTIONS, &url)
        .header("origin", "https://evil.example")
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type")
        .send()
        .await
        .unwrap();
    assert!(
        preflight
            .headers()
            .get("access-control-allow-origin")
            .is_none(),
        "--cors granted a cross-origin preflight to a lifecycle route: {:?}",
        preflight.headers()
    );

    for route in ["suspend", "resume"] {
        let form = client
            .post(format!("http://{addr}/_admin/{route}/usdc"))
            .header("origin", "https://evil.example")
            .header("content-type", "application/x-www-form-urlencoded")
            .send()
            .await
            .unwrap();
        // The foreign Origin is refused first since #2136; the content-type check stays behind it.
        assert!(
            matches!(form.status().as_u16(), 401 | 415),
            "a cross-site form POST reached {route}: {}",
            form.status()
        );
    }
    assert_eq!(
        status_of(&handles).await,
        axum::http::StatusCode::OK,
        "a form POST suspended the mount"
    );
    serving.abort();
}

/// #1643: `?reclaim=true` on a suspended mount frees its dataset as it would a live one's, rather
/// than dropping the record and answering 500 with the data still on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reclaiming_a_suspended_mount_frees_its_dataset() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "6c".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);
    let data_dir = runtime::MountTable::data_dir(roost.path(), &nid);

    let (status, body) = call(&routes, "POST", "/_admin/suspend/usdc", None, Some("{}")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let (status, body) = call(
        &routes,
        "DELETE",
        "/_admin/nests/usdc?reclaim=true",
        None,
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["reclaim"]["outcome"], "reclaimed", "{body}");
    assert!(
        !data_dir.exists(),
        "reclaiming the suspended mount left its dataset"
    );
    let file = runtime::MountTable::load(roost.path()).unwrap();
    assert!(file.runtime.suspended.is_empty() && file.mounts.is_empty());
}

/// #1643, from review: a jobs write that fails after the unmount has removed the record still names
/// the NID, so the caller can reclaim the dataset by `DELETE /_admin/datasets/<nid>`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_jobs_write_after_a_reclaiming_unmount_names_the_nid() {
    use std::os::unix::fs::PermissionsExt;
    let roost = tempfile::tempdir().unwrap();
    let nid = "6d".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs_dir = tempfile::tempdir().unwrap();
    let jobs = Arc::new(nuthatch::mount_jobs::MountJobs::load(jobs_dir.path()));
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);

    let (status, body) = call(&routes, "POST", "/_admin/suspend/usdc", None, Some("{}")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let mode = std::fs::metadata(jobs_dir.path())
        .unwrap()
        .permissions()
        .mode();
    std::fs::set_permissions(jobs_dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let (status, body) = call(
        &routes,
        "DELETE",
        "/_admin/nests/usdc?reclaim=true",
        None,
        None,
    )
    .await;
    std::fs::set_permissions(jobs_dir.path(), std::fs::Permissions::from_mode(mode)).unwrap();
    assert_eq!(
        status,
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "{body}"
    );
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        body["nid"].as_str(),
        Some(nid.as_str()),
        "the record is gone and the answer does not name the dataset: {body}"
    );
    let file = runtime::MountTable::load(roost.path()).unwrap();
    assert!(
        file.mounts.is_empty(),
        "premise: the unmount happened before the jobs write"
    );
}

/// #1547: an API-only operator can free a dataset's disk. Unmounting one of two mounts of a NID with
/// `?reclaim=true` keeps the dataset and names who holds it; unmounting the last one removes it.
/// A dataset unmounted earlier is reclaimed by NID, and a malformed NID is a caller error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reclaim_over_the_admin_api_frees_a_dataset_only_once_nothing_mounts_it() {
    const TOKEN: &str = "reclaim-token";
    let roost_dir = tempfile::tempdir().unwrap();
    let nid = "fe99".repeat(16);
    let data_dir = runtime::MountTable::data_dir(roost_dir.path(), &nid);
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(
        roost_dir.path().join(runtime::MOUNTS_FILE),
        format!(
            "[runtime]\nname = \"r\"\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n\n\
             [[mounts]]\nalias = \"v1\"\nnid = \"{nid}\"\n"
        ),
    )
    .unwrap();

    let tape = Arc::new(TapeSource::new());
    let (a1, a2) = (account(1), account(2));
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
    let cfg = scaffold_nest(&data_dir, "usdc", USDC);
    let health = Arc::new(RuntimeHealth::new());
    health.register("v1", "arbitrum-one");
    let cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![("v1".to_string(), data_dir.clone(), cfg)],
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
    let roster = serde_json::json!({"runtime": "test", "nests": [{"name": "v1"}]});
    let live = serve::LiveRuntime::new(serve::compose_runtime(
        roster.clone(),
        cursor.states.clone(),
        health.clone(),
    ));
    let mut states = cursor.states;
    for (_, s) in &mut states {
        s.nid = Some(Arc::from(nid.as_str()));
    }
    let handles = runtime::RuntimeHandles {
        live,
        states,
        alert_workers: cursor.alert_workers,
        publishers: Vec::new(),
        lifecycle: std::collections::HashMap::from([(
            "arbitrum-one".to_string(),
            cursor.lifecycle.clone(),
        )]),
        health,
        roster,
        estimates: std::collections::HashMap::from([("v1".to_string(), 90)]),
        default_tenant: "default".to_string(),
        suspended: Default::default(),
        joining: Default::default(),
        mount_ctx: runtime::MountContext {
            dir: roost_dir.path().to_path_buf(),
            mounts: vec![runtime::Mount {
                tenant: "default".to_string(),
                alias: "v1".to_string(),
                nid: nid.clone(),
                sql: Default::default(),
                queries: Vec::new(),
                publish: None,
                #[cfg(feature = "counter")]
                counter: None,
            }],
            sources: std::collections::HashMap::from([(
                "arbitrum-one".to_string(),
                tape.clone() as Arc<dyn nuthatch::source::Source>,
            )]),
            endpoint_counts: std::collections::HashMap::from([("arbitrum-one".to_string(), 1)]),
            backfill: None,
            seal_direct: false,
            concurrency: 1,
            ipfs_window_deadline: nuthatch::ipfs_resolve::WINDOW_DEADLINE,
            window_override: Some(2),
            admin_enabled: true,
            admin_token: Some(TOKEN.to_string()),
            max_rss_mb: 2048,
            freshness: Default::default(),
            chain_freshness: Default::default(),
            dormant: Default::default(),
            fail_fast: false,
            cursors: None,
            registry: None,
        },
    };
    std::mem::forget(cursor.ingest);
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let routes =
        runtime::lifecycle_routes(handles.clone(), test_jobs(), true, Some(TOKEN.to_string()));

    handles
        .lock()
        .await
        .mount("v2", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("a second mount of the same nid");

    let (status, body) = call(
        &routes,
        "DELETE",
        "/_admin/nests/v1?reclaim=true",
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["reclaim"]["outcome"], "kept", "{body}");
    assert_eq!(body["reclaim"]["mounted_by"][0], "default/v2", "{body}");
    assert!(data_dir.is_dir(), "a dataset v2 still serves was removed");

    let (status, body) = call(
        &routes,
        "DELETE",
        "/_admin/nests/v2?reclaim=true",
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["reclaim"]["outcome"], "reclaimed", "{body}");
    assert!(
        !data_dir.exists(),
        "the last unmount with reclaim left the dataset"
    );

    let (status, _) = call(
        &routes,
        "DELETE",
        &format!("/_admin/datasets/{nid}"),
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
    let (status, _) = call(
        &routes,
        "DELETE",
        "/_admin/datasets/not-a-nid",
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    let (status, _) = call(
        &routes,
        "DELETE",
        &format!("/_admin/datasets/{nid}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
}

/// #1550: for every admission check, a dry run reports the refusal a real mount then gives, with the
/// same status, and mounts nothing. An admitted dry run reports its cost and the backfill ahead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_and_a_real_mount_agree_on_every_refusal() {
    enum Case {
        Admitted,
        AlreadyMounted,
        NotHeld,
        UndeclaredChain,
        CursorStopped,
        OverBudget,
        EntityCeiling,
        SealDirectEntities,
    }
    fn write_entities(dir: &std::path::Path, n: usize) {
        let mut body = String::new();
        for i in 0..n {
            body.push_str(&format!(
                "[[entities]]\nname = \"e{i}\"\nsql = \"entities/e{i}.sql\"\nkey = [\"id\"]\nmax_rows = 1\n\n"
            ));
        }
        std::fs::write(dir.join("entities.toml"), body).unwrap();
    }
    for (label, case) in [
        ("admitted", Case::Admitted),
        ("already mounted", Case::AlreadyMounted),
        ("not held", Case::NotHeld),
        ("undeclared chain", Case::UndeclaredChain),
        ("cursor stopped", Case::CursorStopped),
        ("over budget", Case::OverBudget),
        ("entity ceiling", Case::EntityCeiling),
        ("seal-direct with entities", Case::SealDirectEntities),
    ] {
        let roost = tempfile::tempdir().unwrap();
        let nid = "7c".repeat(32);
        let (mut handles, _tape, mut intake) = empty_runtime(roost.path(), &nid).await;
        let data_dir = runtime::MountTable::data_dir(roost.path(), &nid);
        let already_mounted = matches!(case, Case::AlreadyMounted);
        match case {
            Case::Admitted => {}
            Case::AlreadyMounted => handles
                .mount("usdc", Some(runtime::Nid::parse(&nid).unwrap()))
                .await
                .unwrap(),
            Case::NotHeld => std::fs::remove_dir_all(&data_dir).unwrap(),
            Case::UndeclaredChain => {
                let mut cfg = nuthatch::config::Config::load(&data_dir).unwrap();
                cfg.nest.chain = "base".to_string();
                cfg.nest.chain_id = 8453;
                cfg.save(&data_dir).unwrap();
            }
            Case::CursorStopped => handles
                .health
                .quarantine_cursor("arbitrum-one", "finality violation".to_string()),
            Case::OverBudget => handles.mount_ctx.max_rss_mb = 100,
            Case::EntityCeiling => write_entities(&data_dir, 33),
            Case::SealDirectEntities => {
                write_entities(&data_dir, 1);
                handles.mount_ctx.seal_direct = true;
            }
        }
        // #2139: these two answered 500, and their dry runs a null status.
        let expected = match case {
            Case::EntityCeiling => Some(507),
            Case::SealDirectEntities => Some(409),
            _ => None,
        };
        let mounted_before = handles.states.len();
        let _ = intake.try_recv();
        let handles = Arc::new(tokio::sync::Mutex::new(handles));
        let routes = runtime::lifecycle_routes(handles.clone(), test_jobs(), true, None);
        // A repeat of the live mount on its own nid is idempotent, so the refusal is another nid.
        let asked = if already_mounted {
            "7d".repeat(32)
        } else {
            nid.clone()
        };
        let body = format!(r#"{{"name":"usdc","nid":"{asked}"}}"#);

        let (status, dry) = call(
            &routes,
            "POST",
            "/_admin/nests?dry_run=true",
            None,
            Some(&body),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{label}: {dry}");
        let dry: serde_json::Value = serde_json::from_str(&dry).unwrap();
        if let Some(code) = expected {
            assert_eq!(dry["refusal_status"], code, "{label}: {dry}");
        }
        assert_eq!(
            handles.lock().await.states.len(),
            mounted_before,
            "{label}: the dry run mounted something"
        );
        assert!(
            intake.try_recv().is_err(),
            "{label}: the dry run started a cursor"
        );

        let (real, answer) = call(
            &routes,
            "POST",
            "/_admin/nests?wait=true",
            None,
            Some(&body),
        )
        .await;
        match dry["refusal_status"].as_u64() {
            Some(code) => assert_eq!(
                u64::from(real.as_u16()),
                code,
                "{label}: the dry run said {dry}, the mount answered {answer}"
            ),
            None => {
                assert_eq!(
                    real,
                    axum::http::StatusCode::OK,
                    "{label}: {dry} / {answer}"
                );
                assert!(dry["incoming_mb"].as_u64().is_some(), "{label}: {dry}");
                assert!(
                    dry["tip"].is_null(),
                    "{label}: no cursor runs on the chain, and a dry run dials nothing: {dry}"
                );
                assert_eq!(dry["chain"], "arbitrum-one", "{label}: {dry}");
            }
        }
        if matches!(case, Case::Admitted) {
            assert!(
                dry["refusal"].is_null(),
                "an admissible mount was refused: {dry}"
            );
        }
    }
}

/// #1550: a dry run fetches and verifies a NID the runtime does not hold, keeps the verified nest,
/// and mounts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_fetches_what_it_needs_and_mounts_nothing() {
    let roost = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let nid = registry_with_one_nest(registry.path()).await;
    let (mut handles, _tape, mut intake) = empty_runtime(roost.path(), &nid).await;
    let data_dir = runtime::MountTable::data_dir(roost.path(), &nid);
    std::fs::remove_dir_all(&data_dir).unwrap();
    handles.mount_ctx.registry = Some(registry.path().to_str().unwrap().to_string());
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let routes = runtime::lifecycle_routes(handles.clone(), test_jobs(), true, None);

    let body = format!(r#"{{"name":"usdc","nid":"{nid}"}}"#);
    let (status, dry) = call(
        &routes,
        "POST",
        "/_admin/nests?dry_run=true",
        None,
        Some(&body),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{dry}");
    let dry: serde_json::Value = serde_json::from_str(&dry).unwrap();
    assert_eq!(dry["fetched"], true, "{dry}");
    assert!(dry["refusal"].is_null(), "{dry}");
    assert!(data_dir.join(nuthatch::config::CONFIG_FILE).exists());
    assert!(handles.lock().await.states.is_empty());
    assert!(intake.try_recv().is_err());
}

/// #1549: moving a name to another NID leaves no gap. A reader polling the name through the move
/// sees only 200s, and only the old dataset, then only the new one. Afterwards the cursor knows the
/// moved nest by the name, the staging name is gone, and the old store is free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_name_moves_to_another_nid_without_a_gap() {
    use tower::ServiceExt;
    let roost = tempfile::tempdir().unwrap();
    let (old_nid, new_nid) = ("8d".repeat(32), "9e".repeat(32));
    let (mut handles, tape) = one_live_mount(roost.path(), &old_nid).await;
    let new_dir = runtime::MountTable::data_dir(roost.path(), &new_nid);
    std::fs::create_dir_all(&new_dir).unwrap();
    scaffold_nest(&new_dir, "usdc", USDC);

    let svc = handles.live.service();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let (svc, stop) = (svc.clone(), stop.clone());
        tokio::spawn(async move {
            let mut seen: Vec<(u16, String)> = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let req = axum::http::Request::builder()
                    .uri("/usdc/sql?q=SELECT%201")
                    .body(axum::body::Body::empty())
                    .unwrap();
                let resp = svc.clone().oneshot(req).await.unwrap();
                let status = resp.status().as_u16();
                let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
                let nid = body["provenance"]["nid"].as_str().unwrap_or("").to_string();
                seen.push((status, nid));
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            seen
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    handles
        .move_name("usdc", runtime::Nid::parse(&new_nid).unwrap())
        .await
        .expect("move");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let seen = reader.await.unwrap();

    assert!(
        seen.iter().all(|(s, _)| *s == 200),
        "a reader saw an error during the move: {:?}",
        seen.iter()
            .filter(|(s, _)| *s != 200)
            .take(3)
            .collect::<Vec<_>>()
    );
    let first_new = seen
        .iter()
        .position(|(_, n)| *n == new_nid)
        .expect("never saw the new nid");
    assert!(first_new > 0, "never saw the old nid");
    assert!(
        seen[..first_new].iter().all(|(_, n)| *n == old_nid),
        "before the switch a reader saw something other than the old nest"
    );
    assert!(
        seen[first_new..].iter().all(|(_, n)| *n == new_nid),
        "after the switch a reader saw the old nest again"
    );

    assert_eq!(
        status(&handles.live, "/usdc.moving/health").await,
        axum::http::StatusCode::NOT_FOUND,
        "the staging name is still routed"
    );
    let old_db = runtime::MountTable::data_dir(roost.path(), &old_nid).join("nuthatch.redb");
    drop(Store::open(&old_db).expect("the old nest's store was not released"));
    let file = runtime::MountTable::load(roost.path()).unwrap();
    assert_eq!(file.mounts.len(), 1, "{:?}", file.mounts);
    assert_eq!(file.mounts[0].nid, new_nid);

    let (a1, a2) = (account(1), account(2));
    tape.insert_block(
        4,
        transfers_block(
            4,
            0,
            1_700_000_004,
            USDC,
            &[(a1.as_str(), a2.as_str(), 400)],
        ),
    );
    tape.advance_tip_to(4);
    assert!(
        wait_until(POLL_TIMEOUT, || usdc_last_block(&handles).as_deref()
            == Some("4"))
        .await,
        "the moved nest stopped following the tip"
    );
    handles.unmount("usdc").await.expect("unmount");
    drop(
        Store::open(&new_dir.join("nuthatch.redb"))
            .expect("the cursor did not know the moved nest by its name"),
    );
}

/// #1549 over HTTP: a move is a job like a mount, ending live on the new NID; a malformed NID is a
/// caller error and a move of a name that is not mounted fails with the reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_over_the_admin_api_is_a_job() {
    let roost = tempfile::tempdir().unwrap();
    let (old_nid, new_nid) = ("ad".repeat(32), "be".repeat(32));
    let (handles, _tape) = one_live_mount(roost.path(), &old_nid).await;
    let new_dir = runtime::MountTable::data_dir(roost.path(), &new_nid);
    std::fs::create_dir_all(&new_dir).unwrap();
    scaffold_nest(&new_dir, "usdc", USDC);
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);

    let (status, _) = call(
        &routes,
        "POST",
        "/_admin/move/usdc",
        None,
        Some(r#"{"nid":"nope"}"#),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    let body = format!(r#"{{"nid":"{new_nid}"}}"#);
    let (status, job) = call(&routes, "POST", "/_admin/move/usdc", None, Some(&body)).await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED, "{job}");
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert_eq!(job["nid"], new_nid.as_str(), "{job}");

    let (status, _) = call(&routes, "POST", "/_admin/move/ghost", None, Some(&body)).await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED);
    let job = wait_for_phase(&routes, "ghost", "failed").await;
    assert!(
        job["reason"].as_str().unwrap_or("").contains("not mounted"),
        "{job}"
    );
}

/// #1549, from review: a move that fetched its NID and was then refused removes the fetch, and the
/// name keeps serving the nest it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_move_leaves_no_fetched_dataset_behind() {
    let roost = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let new_nid = registry_with_one_nest(registry.path()).await;
    let old_nid = "af".repeat(32);
    let (mut handles, _tape) = one_live_mount(roost.path(), &old_nid).await;
    handles.mount_ctx.registry = Some(registry.path().to_str().unwrap().to_string());
    handles.mount_ctx.max_rss_mb = 100;
    let new_dir = runtime::MountTable::data_dir(roost.path(), &new_nid);
    assert!(!new_dir.exists(), "premise: the new nid is not held");
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);

    let body = format!(r#"{{"nid":"{new_nid}"}}"#);
    let (status, _) = call(&routes, "POST", "/_admin/move/usdc", None, Some(&body)).await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED);
    let job = wait_for_phase(&routes, "usdc", "failed").await;
    assert_eq!(job["phase"], "failed", "{job}");
    assert!(
        !new_dir.exists(),
        "a refused move left the dataset it fetched"
    );
    assert_eq!(status_of(&handles).await, axum::http::StatusCode::OK);
}

/// #1557: a mounted nest's sealed figure is the segments its own manifest names, not the runtime's
/// whole shared store, which here holds another dataset's 777 bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mounted_nests_storage_is_its_own_not_the_shared_stores() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "c4".repeat(32);
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    let store = roost.path().join(nuthatch::seal::SEGMENTS_DIR);
    std::fs::create_dir_all(&store).unwrap();
    std::fs::write(
        store.join(format!("{}.parquet", "ee".repeat(32))),
        vec![0u8; 777],
    )
    .unwrap();

    handles
        .mount("storagewire", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("mount");
    let text = nuthatch::metrics::METRICS.render();
    let line = text
        .lines()
        .find(|l| l.starts_with("nuthatch_nest_sealed_segments_bytes{nest=\"storagewire\"}"))
        .unwrap_or_else(|| panic!("no per-nest sealed series:\n{text}"));
    assert!(
        line.ends_with(" 0"),
        "the nest reported the shared store rather than its own segments: {line}"
    );
    assert!(
        text.lines()
            .any(|l| l.starts_with("nuthatch_nest_hot_store_bytes{nest=\"storagewire\"}")),
        "no per-nest hot series"
    );
}

/// #1548, from review: a suspended mount is visible in the jobs API, before and after a restart,
/// and a mount request for its name resumes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_suspended_mount_reads_as_suspended_and_a_mount_resumes_it() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "d7".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);

    let (status, _) = call(&routes, "POST", "/_admin/suspend/usdc", None, Some("{}")).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let job = wait_for_phase(&routes, "usdc", "suspended").await;
    assert_eq!(job["phase"], "suspended", "{job}");

    // A restart with no jobs file, as after a hand edit: `mounts.toml` alone says it is suspended.
    std::fs::remove_file(roost.path().join(nuthatch::mount_jobs::JOBS_FILE)).unwrap();
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);
    let job = wait_for_phase(&routes, "usdc", "suspended").await;
    assert_eq!(job["phase"], "suspended", "after a restart: {job}");

    let body = format!(r#"{{"name":"usdc","nid":"{nid}"}}"#);
    let (status, _) = call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED);
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert_eq!(status_of(&handles).await, axum::http::StatusCode::OK);
}

/// From the 3.13.0 tyre-kick: moving a name whose dataset another tenant shares. The shared
/// dataset must keep indexing for the tenant left on it, the moved name must serve the new NID, and
/// unmounting the moved name afterwards must release the new store and leave the other tenant alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn moving_a_name_off_a_shared_dataset_leaves_the_other_tenant_indexing() {
    let roost = tempfile::tempdir().unwrap();
    let (old_nid, new_nid) = ("5e".repeat(32), "6f".repeat(32));
    let (mut handles, tape) = one_live_mount(roost.path(), &old_nid).await;
    handles
        .mount("other", Some(runtime::Nid::parse(&old_nid).unwrap()))
        .await
        .expect("a second mount of the same dataset");
    let new_dir = runtime::MountTable::data_dir(roost.path(), &new_nid);
    std::fs::create_dir_all(&new_dir).unwrap();
    scaffold_nest(&new_dir, "usdc", USDC);

    handles
        .move_name("usdc", runtime::Nid::parse(&new_nid).unwrap())
        .await
        .expect("move");
    let served = |h: &runtime::RuntimeHandles, name: &str| {
        h.states
            .iter()
            .find(|(n, _)| n == name)
            .and_then(|(_, s)| s.nid.as_deref().map(str::to_string))
    };
    assert_eq!(served(&handles, "usdc").as_deref(), Some(new_nid.as_str()));
    assert_eq!(served(&handles, "other").as_deref(), Some(old_nid.as_str()));

    let other_last = |h: &runtime::RuntimeHandles| {
        h.states
            .iter()
            .find(|(n, _)| n == "other")
            .and_then(|(_, s)| s.store.get_meta("last_block").ok().flatten())
    };
    let (a1, a2) = (account(1), account(2));
    tape.insert_block(
        4,
        transfers_block(
            4,
            0,
            1_700_000_004,
            USDC,
            &[(a1.as_str(), a2.as_str(), 400)],
        ),
    );
    tape.advance_tip_to(4);
    assert!(
        wait_until(POLL_TIMEOUT, || other_last(&handles).as_deref()
            == Some("4"))
        .await,
        "the tenant left on the shared dataset stopped indexing after the move"
    );

    handles
        .unmount("usdc")
        .await
        .expect("unmount the moved name");
    drop(
        Store::open(&new_dir.join("nuthatch.redb"))
            .expect("unmounting the moved name did not release its new store"),
    );
    tape.insert_block(
        5,
        transfers_block(
            5,
            0,
            1_700_000_005,
            USDC,
            &[(a1.as_str(), a2.as_str(), 500)],
        ),
    );
    tape.advance_tip_to(5);
    assert!(
        wait_until(POLL_TIMEOUT, || other_last(&handles).as_deref()
            == Some("5"))
        .await,
        "unmounting the moved name stopped the other tenant's indexing"
    );
}

/// From the 3.13.0 tyre-kick: the token is checked before the body is parsed, so a caller without
/// it gets 401 whatever it sends, and learns nothing about the request shape. With the token, a bad
/// body keeps its own status.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_token_is_checked_before_the_body() {
    use tower::ServiceExt;
    const TOKEN: &str = "body-order-token";
    let roost = tempfile::tempdir().unwrap();
    let nid = "a1".repeat(32);
    let (handles, _t, _i) = empty_runtime(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let routes = runtime::lifecycle_routes(handles, test_jobs(), true, Some(TOKEN.to_string()));
    let send = |uri: &'static str, ctype: Option<&'static str>, token: bool, body: &'static str| {
        let routes = routes.clone();
        async move {
            let mut req = axum::http::Request::builder().method("POST").uri(uri);
            if let Some(c) = ctype {
                req = req.header(axum::http::header::CONTENT_TYPE, c);
            }
            if token {
                req = req.header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"));
            }
            let req = req.body(axum::body::Body::from(body)).unwrap();
            routes.oneshot(req).await.unwrap().status().as_u16()
        }
    };
    for uri in ["/_admin/nests", "/_admin/move/usdc"] {
        let json = Some("application/json");
        assert_eq!(
            send(uri, json, false, "{").await,
            401,
            "{uri}: malformed, no token"
        );
        assert_eq!(
            send(uri, None, false, "{}").await,
            401,
            "{uri}: no content type, no token"
        );
        assert_eq!(
            send(uri, json, true, "{").await,
            400,
            "{uri}: malformed, with token"
        );
        assert_eq!(
            send(uri, json, true, "{}").await,
            422,
            "{uri}: missing field, with token"
        );
        assert_eq!(
            send(uri, None, true, "{}").await,
            415,
            "{uri}: no content type, with token"
        );
    }
}

/// From the 3.13.0 tyre-kick: a runtime serves `/metrics` at its root, before anything is mounted
/// and with every mount's series once there are some. It was only under a mount's route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_runtime_serves_metrics_at_its_root() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "b2".repeat(32);
    let (mut handles, _t, _i) = empty_runtime(roost.path(), &nid).await;
    assert_eq!(
        status(&handles.live, "/metrics").await,
        axum::http::StatusCode::OK,
        "an empty runtime has no /metrics"
    );
    handles
        .mount("rootmetrics", Some(runtime::Nid::parse(&nid).unwrap()))
        .await
        .expect("mount");
    let text = {
        use tower::ServiceExt;
        let req = axum::http::Request::builder()
            .uri("/metrics")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = handles.live.service().oneshot(req).await.unwrap();
        String::from_utf8(
            axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    };
    for series in [
        "nuthatch_hot_store_bytes ",
        "nuthatch_nest_last_block{nest=\"rootmetrics\"}",
        "nuthatch_nest_hot_store_bytes{nest=\"rootmetrics\"}",
        "nuthatch_nest_health{nest=\"rootmetrics\"",
    ] {
        assert!(text.contains(series), "root /metrics lacks {series}");
    }
}

/// From the 3.13.0 tyre-kick: the admin API refuses any name boot would refuse, since an accepted
/// name is persisted and the next start fails on it. Nothing refused is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn names_boot_would_refuse_are_refused_at_the_api() {
    let roost = tempfile::tempdir().unwrap();
    std::fs::write(
        roost.path().join(runtime::MOUNTS_FILE),
        "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n",
    )
    .unwrap();
    let nid = "c3".repeat(32);
    let (handles, _t, _i) = empty_runtime(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let routes = runtime::lifecycle_routes(handles.clone(), test_jobs(), true, None);

    let long = "a".repeat(300);
    let wide = "é".repeat(70);
    for name in [
        "../escape",
        "",
        long.as_str(),
        wide.as_str(),
        "a/b/c",
        "usdc__moving",
        "acme/usdc.v2",
        "acme/",
        "default/usdc",
    ] {
        let body = serde_json::json!({"name": name, "nid": nid}).to_string();
        let (status, answer) = call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
        assert_eq!(
            status,
            axum::http::StatusCode::BAD_REQUEST,
            "{name:?}: {answer}"
        );
        let (status, _) = call(
            &routes,
            "POST",
            "/_admin/nests?wait=true",
            None,
            Some(&body),
        )
        .await;
        assert_eq!(
            status,
            axum::http::StatusCode::BAD_REQUEST,
            "{name:?} with wait"
        );
    }
    let move_body = serde_json::json!({"nid": nid}).to_string();
    let (status, _) = call(
        &routes,
        "POST",
        "/_admin/move/usdc__moving",
        None,
        Some(&move_body),
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::BAD_REQUEST,
        "a move to a reserved name"
    );

    let body = serde_json::json!({"name": "default/usdc", "nid": nid}).to_string();
    let (status, answer) = call(
        &routes,
        "POST",
        "/_admin/nests?wait=true",
        None,
        Some(&body),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{answer}");
    assert!(answer.contains("mount it as 'usdc'"), "{answer}");

    let table = runtime::MountTable::load(roost.path()).expect("the table still loads");
    assert!(
        table.mounts.is_empty(),
        "a refused name was persisted: {:?}",
        table.mounts
    );
}

/// From the 3.13.0 tyre-kick: a restart in the middle of a move. The staging mount is never
/// written to `mounts.toml`, so the table still loads and names only the old nest, and the move job
/// resumes as a move and finishes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_mid_move_resumes_the_move() {
    let roost = tempfile::tempdir().unwrap();
    let (old_nid, new_nid) = ("d4".repeat(32), "e5".repeat(32));
    let (mut handles, _tape) = one_live_mount(roost.path(), &old_nid).await;
    let new_dir = runtime::MountTable::data_dir(roost.path(), &new_nid);
    std::fs::create_dir_all(&new_dir).unwrap();
    scaffold_nest(&new_dir, "usdc", USDC);

    // Where a crash would find a move: the new nest staged beside the old one.
    let staging = runtime::RuntimeHandles::staging_name("usdc");
    handles
        .mount(&staging, Some(runtime::Nid::parse(&new_nid).unwrap()))
        .await
        .expect("stage");
    let table = runtime::MountTable::load(roost.path()).expect("the table loads mid-move");
    assert_eq!(
        table.mounts.len(),
        1,
        "the staging mount was persisted: {:?}",
        table.mounts
    );
    assert_eq!(table.mounts[0].nid, old_nid);

    // The restart: the staging mount is gone, and the move job is on disk, unfinished.
    handles.unmount(&staging).await.unwrap();
    std::fs::write(
        roost.path().join(nuthatch::mount_jobs::JOBS_FILE),
        format!(
            r#"[{{"name":"usdc","nid":"{new_nid}","phase":"joining","since_unixtime":1,"is_move":true}}]"#
        ),
    )
    .unwrap();
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    let served = handles
        .lock()
        .await
        .states
        .iter()
        .find(|(n, _)| n == "usdc")
        .and_then(|(_, s)| s.nid.as_deref().map(str::to_string));
    assert_eq!(
        served.as_deref(),
        Some(new_nid.as_str()),
        "the resumed move did not finish"
    );
}

/// From the 3.13.0 tyre-kick, second pass: identical mounts sent at once. Every caller gets the
/// job, and the job ends live; a second worker must never mark a live nest failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identical_mounts_sent_at_once_end_live() {
    for _ in 0..8 {
        identical_mounts_once().await;
    }
}

async fn identical_mounts_once() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "f6".repeat(32);
    let (handles, _t, _i) = empty_runtime(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);
    let body = format!(r#"{{"name":"race","nid":"{nid}"}}"#);
    // One task per request, as hyper serves them: on a single task the check and insert never
    // interleave, and the race cannot happen.
    let calls = (0..16).map(|_| {
        let (routes, body) = (routes.clone(), body.clone());
        tokio::spawn(async move { call(&routes, "POST", "/_admin/nests", None, Some(&body)).await })
    });
    for answer in futures::future::join_all(calls).await {
        let (status, answer) = answer.unwrap();
        assert!(
            status == axum::http::StatusCode::ACCEPTED || status == axum::http::StatusCode::OK,
            "{status}: {answer}"
        );
    }
    let job = wait_for_phase(&routes, "race", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let (_, after) = call(&routes, "GET", "/_admin/mounts/race", None, None).await;
    assert!(
        after.contains("\"live\""),
        "a late worker changed the job: {after}"
    );
}

/// A configured default tenant, spelled out, is refused before a job starts, not by a job that then
/// fails after the caller was told 202.
#[tokio::test]
async fn a_configured_default_tenant_spelled_out_is_refused_at_the_api() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "c4".repeat(32);
    let (mut handles, _t, _i) = empty_runtime(roost.path(), &nid).await;
    handles.default_tenant = "acme".to_string();
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, None);

    let body = serde_json::json!({"name": "acme/usdc", "nid": nid}).to_string();
    let (status, answer) = call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{answer}");
    assert!(answer.contains("mount it as 'usdc'"), "{answer}");
    let (status, answer) = call(&routes, "POST", "/_admin/move/acme/usdc", None, Some(&body)).await;
    assert_eq!(
        status,
        axum::http::StatusCode::BAD_REQUEST,
        "move: {answer}"
    );
    let (_, jobs) = call(&routes, "GET", "/_admin/mounts", None, None).await;
    assert!(!jobs.contains("acme/usdc"), "a job was recorded: {jobs}");
}

/// From the follow-up on the box: with `default_tenant = "acme"`, a mount of `usdc` was recorded under
/// the literal `default` tenant, matched no route key, and was never written, so a restart lost it.
/// And a bare name matched a record of any tenant, so moving `usdc` rewrote `globex/usdc` as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_configured_default_tenant_keeps_its_mounts_and_moves_one_alone() {
    let roost = tempfile::tempdir().unwrap();
    let (old_nid, new_nid) = ("7a".repeat(32), "8b".repeat(32));
    std::fs::write(
        roost.path().join(runtime::MOUNTS_FILE),
        "[runtime]\nname = \"r\"\ndefault_tenant = \"acme\"\n\n[[chains]]\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n",
    )
    .unwrap();
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &old_nid).await;
    handles.default_tenant = "acme".to_string();
    handles.mount_ctx.mounts.clear();
    for name in ["usdc", "globex/usdc"] {
        handles
            .mount(name, Some(runtime::Nid::parse(&old_nid).unwrap()))
            .await
            .unwrap_or_else(|e| panic!("mount {name}: {e:#}"));
    }
    let records = || {
        let raw = std::fs::read_to_string(roost.path().join(runtime::MOUNTS_FILE)).unwrap();
        let table: runtime::MountTable = toml::from_str(&raw).unwrap();
        let mut out: Vec<(String, String, String)> = table
            .mounts
            .into_iter()
            .map(|m| (m.tenant, m.alias, m.nid))
            .collect();
        out.sort();
        out
    };
    let rec = |t: &str, n: &str| (t.to_string(), "usdc".to_string(), n.to_string());
    assert_eq!(
        records(),
        vec![rec("acme", &old_nid), rec("globex", &old_nid)],
        "both mounts are recorded, the default one under the configured tenant"
    );

    let new_dir = runtime::MountTable::data_dir(roost.path(), &new_nid);
    std::fs::create_dir_all(&new_dir).unwrap();
    scaffold_nest(&new_dir, "usdc", USDC);
    handles
        .move_name("usdc", runtime::Nid::parse(&new_nid).unwrap())
        .await
        .expect("move");
    assert_eq!(
        records(),
        vec![rec("acme", &new_nid), rec("globex", &old_nid)],
        "moving usdc leaves globex/usdc on its own nid"
    );
}

/// #2130: a mount whose routes would overlap another's is refused before it joins. `ready` and
/// `metrics` are root routes, and a tenant named as a default-tenant alias serves under that alias's
/// routes. Each used to panic `Router::nest` after the nest was on its cursor, and every later
/// recomposition with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_whose_routes_would_collide_is_refused_before_it_joins() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "5e".repeat(32);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs.clone(), true, None);

    for name in ["ready", "metrics", "ready/usdc"] {
        let body = format!(r#"{{"name":"{name}","nid":"{nid}"}}"#);
        for uri in ["/_admin/nests", "/_admin/nests?wait=true"] {
            let (status, answer) = call(&routes, "POST", uri, None, Some(&body)).await;
            assert_eq!(
                status,
                axum::http::StatusCode::BAD_REQUEST,
                "{uri} '{name}': {answer}"
            );
        }
        assert!(jobs.get(name).is_none(), "'{name}' was recorded as a job");
    }

    let collides = format!(r#"{{"name":"usdc/sql","nid":"{nid}"}}"#);
    let (status, answer) = call(
        &routes,
        "POST",
        "/_admin/nests?wait=true",
        None,
        Some(&collides),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{answer}");
    assert!(answer.contains("also the name of a mount"), "{answer}");
    let (_, dry) = call(
        &routes,
        "POST",
        "/_admin/nests?dry_run=true",
        None,
        Some(&collides),
    )
    .await;
    let dry: serde_json::Value = serde_json::from_str(&dry).unwrap();
    assert_eq!(dry["refusal_status"], 400, "{dry}");
    let (status, _) = call(&routes, "POST", "/_admin/nests", None, Some(&collides)).await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED);
    let job = wait_for_phase(&routes, "usdc/sql", "failed").await;
    assert_eq!(job["phase"], "failed", "{job}");

    // The other way round: a tenant first, then a default-tenant alias of its name.
    let acme = format!(r#"{{"name":"acme/usdc","nid":"{nid}"}}"#);
    let (status, answer) = call(
        &routes,
        "POST",
        "/_admin/nests?wait=true",
        None,
        Some(&acme),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{answer}");
    let alias = format!(r#"{{"name":"acme","nid":"{nid}"}}"#);
    let (status, answer) = call(
        &routes,
        "POST",
        "/_admin/nests?wait=true",
        None,
        Some(&alias),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{answer}");

    let h = handles.lock().await;
    let names: Vec<&str> = h.states.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["usdc", "acme/usdc"]);
    assert_eq!(
        self::status(&h.live, "/acme/usdc/health").await,
        axum::http::StatusCode::OK
    );
}

/// #2137: a NID is one dataset whatever case the caller spells it in. The upper-case form of a held
/// NID shares the dataset; it used to be a second directory, indexed again from its start block.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nid_in_upper_case_names_the_same_dataset() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "c3d4".repeat(16);
    let (handles, _tape) = one_live_mount(roost.path(), &nid).await;
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let routes = runtime::lifecycle_routes(handles.clone(), test_jobs(), true, None);

    let body = format!(
        r#"{{"name":"acme/usdc","nid":"{}"}}"#,
        nid.to_ascii_uppercase()
    );
    let (status, answer) = call(
        &routes,
        "POST",
        "/_admin/nests?wait=true",
        None,
        Some(&body),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{answer}");
    let h = handles.lock().await;
    let dirs: Vec<_> = h.states.iter().map(|(_, s)| s.dir.clone()).collect();
    assert_eq!(dirs.len(), 2);
    assert_eq!(dirs[0], dirs[1], "two directories for one dataset");
    assert!(
        h.mount_ctx.mounts.iter().all(|m| m.nid == nid),
        "a record kept the caller's spelling: {:?}",
        h.mount_ctx.mounts
    );
}

/// #2139: a dry run reads the tip its chain's cursor last saw and dials nothing. It holds the
/// runtime's lock, so a call to a slow RPC from here held up every other admin call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_reads_the_cursors_tip_and_dials_nothing() {
    let roost = tempfile::tempdir().unwrap();
    let nid = "e1".repeat(32);
    let (handles, tape) = one_live_mount_as(roost.path(), &nid, "tipwatch-2139").await;
    let other = "e2".repeat(32);
    let other_dir = runtime::MountTable::data_dir(roost.path(), &other);
    std::fs::create_dir_all(&other_dir).unwrap();
    scaffold_nest(&other_dir, "usdc", USDC);
    tape.go_dark();
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let routes = runtime::lifecycle_routes(handles.clone(), test_jobs(), true, None);

    let body = format!(r#"{{"name":"other-2139","nid":"{other}"}}"#);
    let (status, dry) = call(
        &routes,
        "POST",
        "/_admin/nests?dry_run=true",
        None,
        Some(&body),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{dry}");
    let dry: serde_json::Value = serde_json::from_str(&dry).unwrap();
    assert_eq!(dry["tip"], 3, "{dry}");
}
