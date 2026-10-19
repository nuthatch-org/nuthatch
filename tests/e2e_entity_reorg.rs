//! RFC-0041 §5.2, end to end: an authored incremental entity survives a reorg.
//!
//! **Why this is not a unit test.** `entity_view`'s own property test feeds both the `+1` and the
//! `-1` side from the same source, and passes. The real paths do not agree by construction: an
//! insertion is a `DecodedRow` straight from the registry, while a retraction is reconstructed from
//! the hot store's JSON. DBSP cancels by key, so if those two produce different rows nothing
//! cancels - the entity keeps the fact that was rolled back *and* gains a row at weight `-1` that
//! nothing will ever cancel again.
//!
//! That divergence is invisible to any test that builds both sides itself. It is only visible here,
//! through `spawn_nest`, `entities_in_range` and a real fork.

mod common;
#[path = "common/entity_fixture.rs"]
mod entity_fixture;

use proptest::prelude::*;

use std::sync::Arc;

use nuthatch::indexer;

use common::tape::*;

/// A canonical block: one transfer of `100 * b` from account 1 to account 2.
fn canonical_block(b: u64) -> BlockFixture {
    transfers_block(
        b,
        0,
        1_700_000_000 + b,
        USDC,
        &[(account(1).as_str(), account(2).as_str(), (100 * b) as u128)],
    )
}

/// A replacement block: a *different* recipient and amount, so an entity that failed to retract the
/// orphaned row would hold a group the clean run does not have, rather than merely a wrong total.
fn replacement_block(b: u64) -> BlockFixture {
    transfers_block(
        b,
        1,
        1_700_000_500 + b,
        USDC,
        &[(
            account(3).as_str(),
            account(4).as_str(),
            (7_000 + b) as u128,
        )],
    )
}

/// `SELECT to, SUM(value) FROM usdc__transfer GROUP BY to`, declared the way an author would.
///
/// No filter and no join on purpose: this test is about the retraction path, and every operator it
/// does not use is one that cannot mask a divergence by discarding the row that carries it.
const RECEIVED: &str = r#"[[entities]]
name = "received"
query = "SELECT t.to, SUM(CAST(t.value AS HUGEINT)) AS sum_value FROM usdc__transfer t GROUP BY t.to"
key = ["to"]
max_rows = 10000
"#;

/// #1437: an entity joining chain transfers to an offchain `tiers` table.
const BY_TIER: &str = r#"
[[entities]]
name = "by_tier"
query = "SELECT t.tier, SUM(x.value) FROM usdc__transfer x JOIN offchain__tiers t ON x.to = t.account GROUP BY t.tier"
key = ["tier"]
max_rows = 10000
"#;

fn declare_entity(dir: &std::path::Path) {
    declare(dir, RECEIVED);
}

fn declare(dir: &std::path::Path, toml: &str) {
    entity_fixture::write(dir, toml).expect("write entities.toml");
}

const CHAIN_LEN: u64 = 8;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_checked_entity_file_starts_and_indexes() {
    let dir = tempfile::tempdir().unwrap();
    scaffold_nest(dir.path(), "usdc", USDC);
    declare_entity(dir.path());
    nuthatch::project::regen(nuthatch::cli::SchemaArgs {
        dir: dir.path().to_str().unwrap().into(),
    })
    .unwrap();
    nuthatch::check::check(nuthatch::cli::CheckArgs {
        name: None,
        dir: dir.path().to_str().unwrap().into(),
        update: false,
        #[cfg(feature = "folds")]
        folds: false,
        #[cfg(feature = "folds")]
        from_genesis: false,
    })
    .expect("the authored file must pass check before startup");
    let manifest = std::fs::read(dir.path().join("entities.toml")).unwrap();
    let sql = std::fs::read(dir.path().join("entities/received.sql")).unwrap();
    let tape = Arc::new(TapeSource::new());
    for block in 1..=CHAIN_LEN {
        tape.insert_block(block, canonical_block(block));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_with_entity(dir.path(), tape, CHAIN_LEN).await;
    rt.state.entities[0].flush();
    assert!(!relation(&rt).is_empty());
    assert_eq!(
        std::fs::read(dir.path().join("entities.toml")).unwrap(),
        manifest
    );
    assert_eq!(
        std::fs::read(dir.path().join("entities/received.sql")).unwrap(),
        sql
    );
    rt.shutdown().await.expect("the nest stops");
}

async fn spawn_with_entity(
    dir: &std::path::Path,
    tape: Arc<TapeSource>,
    tip: u64,
) -> indexer::NestRuntime {
    spawn_declared(dir, tape, tip, RECEIVED).await
}

/// The same, for a caller that supplies its own `entities.toml`. Written **after** the scaffold so
/// the declaration is the one under test and not whatever the scaffold would leave behind.
async fn spawn_declared(
    dir: &std::path::Path,
    tape: Arc<TapeSource>,
    tip: u64,
    decl: &str,
) -> indexer::NestRuntime {
    let cfg = scaffold_nest(dir, "usdc", USDC);
    declare(dir, decl);
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
    .expect("spawn_nest with a declared entity");
    let store = rt.state.store.clone();
    let tip_str = tip.to_string();
    let landed = wait_until(POLL_TIMEOUT, || {
        store.get_meta("last_block").ok().flatten().as_deref() == Some(tip_str.as_str())
    })
    .await;
    assert!(landed, "nest did not index to block {tip} in time");
    // The store commits a window before the entities are fed it (#1913), so `last_block` alone can
    // lead the relation by a window (#1921).
    let entities = rt.state.entities.clone();
    let fed = wait_until(POLL_TIMEOUT, || {
        entities
            .iter()
            .all(|e| e.unavailable().is_some() || e.fault().is_some() || e.applied_through() >= tip)
    })
    .await;
    assert!(fed, "the entities did not fold through block {tip} in time");
    rt
}

fn shutdown(rt: indexer::NestRuntime) {
    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
}

/// The entity's relation, rendered so a mismatch reads as data rather than as `Row(vec![...])`.
fn relation(rt: &indexer::NestRuntime) -> Vec<(String, String)> {
    let entity = rt
        .state
        .entities
        .first()
        .expect("the nest declared one entity");
    entity.flush();
    let mut out: Vec<(String, String)> = entity
        .relation()
        .iter()
        .map(|(k, v)| (format!("{k:?}"), format!("{v:?}")))
        .collect();
    out.sort();
    out
}

/// Run a query through the nest's `/sql` route and return its rows as sorted `(k, v)` text pairs.
///
/// Text rather than numbers on purpose: the entity serves exact i128 as a decimal string and DuckDB
/// serves its own type, so comparing the rendered values is the comparison that would catch a
/// precision loss on either side rather than papering over it with a float cast.
async fn sql_pairs(rt: &indexer::NestRuntime, sql: &str) -> Vec<(String, String)> {
    let path = format!("/sql?q={}", urlencoding_lite(sql));
    let (status, body) = get_json(rt, &path).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{sql} -> {body}");
    let mut out: Vec<(String, String)> = body["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows for {sql}: {body}"))
        .iter()
        .map(|r| {
            let k = r["k"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| r["k"].to_string());
            let v = r["v"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| r["v"].to_string());
            (k, v)
        })
        .collect();
    out.sort();
    out
}

/// Percent-encode the handful of characters a SQL string puts in a query parameter. Not a general
/// encoder; the test corpus is fixed and this keeps a dependency out of the test suite.
fn urlencoding_lite(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            ' ' => "%20".to_string(),
            '"' => "%22".to_string(),
            '*' => "%2A".to_string(),
            '(' => "%28".to_string(),
            ')' => "%29".to_string(),
            ',' => "%2C".to_string(),
            '+' => "%2B".to_string(),
            other => other.to_string(),
        })
        .collect()
}

async fn entity_converges_after_reorg(fork: u64) {
    assert!((1..CHAIN_LEN).contains(&fork));
    // Reorged nest: index the canonical chain, then rewrite everything above the fork.
    let reorged_dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_with_entity(reorged_dir.path(), tape.clone(), CHAIN_LEN).await;

    let before = relation(&rt);
    assert!(
        !before.is_empty(),
        "the entity must have folded the canonical chain before the reorg is worth anything"
    );

    tape.reorg(
        fork,
        ((fork + 1)..=CHAIN_LEN).map(replacement_block).collect(),
    );

    let store = rt.state.store.clone();
    let entities = rt.state.entities.clone();
    let converged = wait_until(POLL_TIMEOUT, || {
        // The store commits the replayed window before the entity is fed it (#1913). The retraction
        // was enqueued before that commit and stamps the fork, so a flushed watermark back at the
        // head means the replay has been folded too (#1921).
        replacement_head_landed(store.as_ref()) && {
            entities[0].flush();
            entities[0].applied_through() == CHAIN_LEN
        }
    })
    .await;
    assert!(converged, "the reorg did not reconverge in time");
    let after = relation(&rt);
    let health = rt.state.entities[0].fault();

    // **#822 criterion 12.** *"A randomized reorg run converges to the old SQL reference result."*
    // The clean-replay comparison below asks whether the circuit agrees with itself; this asks
    // whether it agrees with the authored SQL it replaces, computed fresh over hot∪sealed by DuckDB
    // after the reorg. Two independent routes to the same number, and the entity is only worth
    // having if they match.
    let maintained = sql_pairs(&rt, "SELECT \"to\" AS k, sum_value AS v FROM received").await;
    let reference = sql_pairs(
        &rt,
        "SELECT t.\"to\" AS k, SUM(t.value_dec) AS v FROM usdc__transfer t GROUP BY t.\"to\"",
    )
    .await;
    assert!(
        !reference.is_empty(),
        "the reference query returned nothing, so it proves nothing"
    );
    assert_eq!(
        maintained, reference,
        "after a reorg at {fork}, the maintained relation must equal the authored SQL it replaces"
    );

    shutdown(rt);
    assert_eq!(health, None, "the entity must still be running");

    // Clean nest: index the post-reorg chain directly, never having seen the orphaned blocks.
    let clean_dir = tempfile::tempdir().unwrap();
    let clean_tape = Arc::new(TapeSource::new());
    for b in 1..=fork {
        clean_tape.insert_block(b, canonical_block(b));
    }
    for b in (fork + 1)..=CHAIN_LEN {
        clean_tape.insert_block(b, replacement_block(b));
    }
    clean_tape.advance_tip_to(CHAIN_LEN);
    let clean_rt = spawn_with_entity(clean_dir.path(), clean_tape, CHAIN_LEN).await;
    let clean = relation(&clean_rt);
    shutdown(clean_rt);

    assert_ne!(
        before, clean,
        "the reorg must actually change the entity, or this test proves nothing"
    );
    assert_eq!(
        after, clean,
        "a reorged entity must equal a clean replay over the post-reorg chain"
    );
}

/// #866 criterion 13: `--seal-direct` either rebuilds entities from the sealed corpus before serving,
/// or refuses the combination clearly. It refuses - and the refusal is the point.
///
/// Seal-direct writes finalized history straight to Parquet without passing it through the ingest
/// path, so an entity fed from decoded windows would see none of it. Left to run, the nest would
/// complete, serve, and answer with an empty relation: *"a completed run with an empty entity is the
/// failure this criterion exists for."*
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seal_direct_refuses_a_nest_that_declares_an_entity() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    declare_entity(dir.path());

    let err = indexer::spawn_nest(
        tape,
        dir.path().to_path_buf(),
        cfg,
        None,
        true, // --seal-direct
        1,
        Some(2),
        false,
        None,
    )
    .await
    .err()
    .expect("seal-direct plus a declared entity must not start");

    let err = format!("{err:#}");
    assert!(err.contains("--seal-direct cannot be combined"), "{err}");
    assert!(
        err.contains("`received`"),
        "the refusal must name the entity: {err}"
    );
    assert!(
        err.contains("served empty"),
        "and say what would otherwise happen: {err}"
    );
}

/// #1537: a live mount builds through `build_and_prepare_nest`, which used to skip the refusal
/// `spawn_nest` makes and then serve the entity empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_mount_refuses_seal_direct_with_an_entity() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    declare_entity(dir.path());
    let source: Arc<dyn nuthatch::source::Source> = tape;

    let err = indexer::build_and_prepare_nest(
        &source,
        nuthatch::runtime::PreparedDataset::without_nid(dir.path().to_path_buf()),
        &cfg,
        None,
        true,
        1,
        Some(2),
        false,
        None,
        None,
        nuthatch::serve::new_sql_gate(),
        indexer::CatchUp {
            cursor_at: &|| None,
            wanted: &|| true,
        },
    )
    .await
    .err()
    .expect("a live mount must refuse seal-direct plus an entity");

    let err = format!("{err:#}");
    assert!(err.contains("--seal-direct cannot be combined"), "{err}");
    assert!(
        err.contains("`received`"),
        "the refusal must name the entity: {err}"
    );
}

/// #1545: a first mount onto a chain starts its cursor through `spawn_runtime`, so that path must
/// refuse seal-direct with an entity too, and let go of the store it opened to find out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cursor_started_by_a_mount_refuses_seal_direct_with_an_entity() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    declare_entity(dir.path());

    let err = indexer::spawn_runtime(
        tape,
        vec![("usdc".to_string(), dir.path().to_path_buf(), cfg)],
        None,
        true,
        1,
        Some(2),
        false,
        None,
        Arc::new(nuthatch::health::RuntimeHealth::new()),
        false,
    )
    .await
    .err()
    .expect("a cursor started for a mount must refuse seal-direct plus an entity");
    assert!(
        format!("{err:#}").contains("--seal-direct cannot be combined"),
        "{err:#}"
    );
    nuthatch::store::Store::open(&dir.path().join(nuthatch::config::DB_FILE))
        .expect("the refused nest's store must be released");
}

/// The same nest without `--seal-direct` starts and folds - so the refusal above is about the
/// combination and not about entities being unable to start at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_nest_without_seal_direct_starts_and_folds() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let rt = spawn_with_entity(dir.path(), tape, CHAIN_LEN).await;
    let rows = relation(&rt);
    shutdown(rt);
    assert!(!rows.is_empty(), "the entity folded the chain");
}

/// #866 criterion 8: a dead entity circuit is a **terminal fault for the nest**, not a quiet freeze.
///
/// §5.2: *"A circuit thread dying is a terminal fault for that nest under RFC-0026. Serving frozen
/// derived state as healthy is not graceful degradation; it is a lie with a pleasant HTTP status."*
///
/// The fault is induced through the declared bound rather than by reaching into the view, so the
/// path under test is the real one: the bound bites inside the circuit thread, the thread stops, and
/// the ingest loop's health check has to notice and escalate. A test that killed the thread directly
/// would prove the check and not the escalation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_entity_circuit_ends_the_nest_rather_than_freezing_it() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
    // One row admitted, and the first window carries two.
    entity_fixture::write(
        dir.path(),
        r#"[[entities]]
name = "received"
query = "SELECT t.to, SUM(t.value) FROM usdc__transfer t GROUP BY t.to"
key = ["to"]
max_rows = 1
"#,
    )
    .unwrap();

    let rt = indexer::spawn_nest(
        tape,
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
    .expect("the nest starts - the bound is a runtime fault, not a load-time refusal");

    // The loop must *end*. Freezing here - the entity dead, the cursor still polling, `/ready` still
    // 200 - is the failure this criterion is named for, and it would show up as a timeout.
    let outcome = tokio::time::timeout(POLL_TIMEOUT, rt.ingest)
        .await
        .expect("the ingest loop must terminate, not carry on with a dead entity")
        .expect("the task itself should not panic");

    let err = format!(
        "{:#}",
        outcome.expect_err("a dead entity circuit is a terminal fault")
    );
    assert!(
        err.contains("entity `received`"),
        "the fault must name the entity: {err}"
    );
    assert!(
        err.contains("max_rows"),
        "and the cause, not merely that something stopped: {err}"
    );
}

/// **The alert arrives before the nest stops.** A faulted entity quarantines the nest, so an
/// operator finds out from `/ready` - if they happen to be looking. This is the one that goes out
/// unasked.
///
/// Driven through a real nest against a real local webhook, asserting on **what arrived**, because
/// the emit and the delivery are two different things and testing either alone proves neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_faulted_entity_pushes_an_alert_to_a_configured_sink() {
    use axum::{routing::post, Json, Router};
    use std::sync::{Arc as StdArc, Mutex};

    let received = StdArc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let sink = received.clone();
    let app = Router::new().route(
        "/hook",
        post(move |Json(body): Json<serde_json::Value>| {
            let sink = sink.clone();
            async move {
                sink.lock().unwrap().push(body);
                "ok"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let mut cfg = scaffold_nest(dir.path(), "usdc", USDC);
    cfg.alerts = vec![nuthatch::config::Alert {
        kinds: vec!["entity_fault".into()],
        url: format!("http://{addr}/hook"),
        format: nuthatch::config::AlertFormat::Raw,
    }];
    // One row admitted, and the first window carries two - the same fault the test above uses.
    entity_fixture::write(
        dir.path(),
        r#"[[entities]]
name = "received"
query = "SELECT t.to, SUM(t.value) FROM usdc__transfer t GROUP BY t.to"
key = ["to"]
max_rows = 1
"#,
    )
    .unwrap();

    let rt = indexer::spawn_nest(
        tape,
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
    .expect("the nest starts");
    let store = rt.state.store.clone();

    let _ = tokio::time::timeout(POLL_TIMEOUT, rt.ingest).await;

    // The alert is enqueued to a durable outbox by the ingest loop, and *two* things drain it: the
    // nest's own delivery worker, and this explicit call. A single drain-then-check therefore raced
    // three ways (#1119) - the ingest loop might not have produced the fault yet, since the `timeout`
    // above is swallowed by `let _`; the worker might already have taken the entry, making a
    // `delivered > 0` check on our own call wrong rather than right; and delivery itself takes a
    // moment. Poll the observable outcome instead, re-draining as we go, and let the failure name the
    // outbox depth so "never enqueued" is distinguishable from "enqueued and never delivered".
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    let mut got;
    loop {
        let _ = nuthatch::alerts::deliver_pending(
            store.as_ref(),
            &client,
            &std::collections::HashMap::new(),
        )
        .await;
        got = received.lock().unwrap().clone();
        if got.iter().any(|a| a["kind"] == "entity_fault") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no entity_fault alert arrived within {POLL_TIMEOUT:?}: {} still pending in the outbox, \
             {got:?} received",
            store.outbox_len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let alert = got
        .iter()
        .find(|a| a["kind"] == "entity_fault")
        .expect("checked by the loop above");
    assert!(
        alert["event"].as_str().unwrap_or("").contains("received"),
        "the alert names the entity: {alert}"
    );
    assert_eq!(alert["annotation"]["entity"], "received");
    assert!(
        alert["annotation"]["reason"]
            .as_str()
            .unwrap_or("")
            .contains("max_rows"),
        "and why it stopped, not merely that it did: {alert}"
    );
}

/// #866 criterion 8, the other half: *"It cannot freeze quietly while sibling nests continue to
/// report it healthy."*
///
/// Two nests on one cursor. One declares an entity with a bound its first window breaks; the other
/// declares none. The faulted nest must be quarantined **by name**, and the healthy one must keep
/// indexing - the blast radius is the nest, not the cursor.
///
/// Both nests sit on the same chain deliberately: that is what puts them on one cursor, and a blast
/// radius that widened to the cursor is precisely what this is watching for. `fail_fast` is `false`,
/// production's default - it is the one setting under which a quarantined nest is *expected* to take
/// its cursor with it, so passing this test under it would mean nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_faulted_entity_quarantines_its_own_nest_and_leaves_its_neighbour_indexing() {
    use nuthatch::health::RuntimeHealth;

    let doomed_dir = tempfile::tempdir().unwrap();
    let healthy_dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    // Distinct nest names, and not merely for readability: `NestIngest` takes its name from
    // `config.nest.name`, which is what `RuntimeHealth` keys a quarantine by. Scaffolding both as
    // `usdc` registers both under one identity, and the isolation this test is about becomes
    // unobservable - the first version did exactly that and reported no quarantine at all.
    //
    // The contract alias moves with the name, so the doomed nest's table is `doomed__transfer`.
    let doomed_cfg = scaffold_nest(doomed_dir.path(), "doomed", USDC);
    entity_fixture::write(
        doomed_dir.path(),
        r#"[[entities]]
name = "received"
query = "SELECT t.to, SUM(t.value) FROM doomed__transfer t GROUP BY t.to"
key = ["to"]
max_rows = 1
"#,
    )
    .unwrap();
    // The neighbour declares no entity at all, so nothing about it can fault for this reason.
    let healthy_cfg = scaffold_nest(healthy_dir.path(), "healthy", USDC);

    let health = Arc::new(RuntimeHealth::new());
    health.register("doomed", &doomed_cfg.nest.chain);
    health.register("healthy", &healthy_cfg.nest.chain);

    let cursor = indexer::spawn_runtime(
        tape,
        vec![
            (
                "doomed".to_string(),
                doomed_dir.path().to_path_buf(),
                doomed_cfg,
            ),
            (
                "healthy".to_string(),
                healthy_dir.path().to_path_buf(),
                healthy_cfg,
            ),
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

    // The faulted nest is quarantined, and the reason names the entity rather than saying only that
    // something stopped.
    let quarantined = wait_until(POLL_TIMEOUT, || health.status("doomed").is_some()).await;
    assert!(
        quarantined,
        "the nest whose entity died must be quarantined, not left quietly frozen"
    );
    let reason = health.status("doomed").unwrap().reason;
    assert!(
        reason.contains("entity `received`"),
        "the quarantine must say which entity: {reason}"
    );

    // The neighbour reaches the tip - after its sibling died, not before it started.
    let want = CHAIN_LEN.to_string();
    let neighbour = cursor
        .states
        .iter()
        .find(|(n, _)| n == "healthy")
        .expect("the healthy nest is on this cursor");
    let landed = wait_until(POLL_TIMEOUT, || {
        neighbour
            .1
            .store
            .get_meta("last_block")
            .ok()
            .flatten()
            .as_deref()
            == Some(want.as_str())
    })
    .await;
    assert!(
        landed,
        "the healthy nest must keep indexing - the blast radius is the nest, not the cursor"
    );
    assert!(
        health.status("healthy").is_none(),
        "and it must not be quarantined by its neighbour's fault"
    );

    cursor.ingest.abort();
}

/// **RFC-0041 §5.3 and #865.** A restarted entity comes back complete, from this nest's own stored
/// history, **without a single historical RPC call**.
///
/// The seed is sealed segments plus the unsealed hot tail, fed through the same circuit as any
/// window - §5.1's "backfill uses larger batches, but not different semantics" taken literally. It
/// is not a separate seed relation combined with a delta, which is what §5.3's wording describes and
/// what comes apart for any entity with a join: a finalized row joining a hot one is in neither half.
///
/// Two things make this more than "it is not empty": the restarted nest is compared against a **cold
/// nest indexed over the whole chain**, which is criterion 5's "matches uninterrupted execution"; and
/// the tape's `logs` call count is read across the restart, which is #865's claim stated as a number.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_entity_is_rebuilt_from_stored_history_with_no_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let first = spawn_with_entity(dir.path(), tape.clone(), CHAIN_LEN).await;
    let before = relation(&first);
    assert!(!before.is_empty(), "the first run must actually fill it");
    first.shutdown().await.expect("the nest stops");

    // Restart over the same directory. Nothing new to index, so any `logs` call the restart makes is
    // a historical one - which is exactly what #865 forbids.
    let calls_before = tape.logs_call_count();
    let second = spawn_with_entity(dir.path(), tape.clone(), CHAIN_LEN).await;
    let after = relation(&second);
    let unavailable = second.state.entities[0].unavailable().map(str::to_string);
    let applied = second.state.entities[0].applied_through();
    second.shutdown().await.expect("the nest stops");

    assert_eq!(
        unavailable, None,
        "a seeded entity is available, not waiting for a rebuild it already had"
    );
    assert_eq!(
        after, before,
        "the rebuilt relation must be the one the entity had before the restart"
    );
    assert_eq!(
        applied, CHAIN_LEN,
        "and it must answer for the head it was seeded through, not block 0"
    );

    // The tip-follower polls, so this is not zero calls - it is zero calls *per historical block*.
    // Re-indexing eight blocks through a two-block window would take four or more `logs` calls on its
    // own, before the poll loop adds any.
    let historical = tape.logs_call_count() - calls_before;
    assert!(
        historical < (CHAIN_LEN / 2) as usize,
        "the seed must read stored history, not re-fetch it: {historical} logs calls across a \
         restart of a {CHAIN_LEN}-block chain"
    );

    // And the whole thing must match a nest that never restarted.
    let cold_dir = tempfile::tempdir().unwrap();
    let cold = spawn_with_entity(cold_dir.path(), tape, CHAIN_LEN).await;
    let clean = relation(&cold);
    cold.shutdown().await.expect("the nest stops");
    assert_eq!(
        after, clean,
        "criterion 5: a seeded entity matches uninterrupted execution"
    );
}

/// **The half of the seed that the hot store cannot answer.**
///
/// Sealing *prunes* the sealed rows out of hot (`prune_and_set_meta`), so once a range is finalized
/// the only copy of it is Parquet. A seed that reads the hot store alone therefore rebuilds a
/// relation covering the unsealed tail and nothing else - populated, plausible, and missing all of
/// history.
///
/// The previous restart test could not see that: an eight-block chain seals nothing, so skipping
/// sealed segments entirely left it green. This one finalizes through block 8 first, and the
/// assertion below can only hold if the sealed rows were read back from Parquet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seeded_entity_includes_the_sealed_range_the_hot_store_no_longer_holds() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=10u64 {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(10);
    let first = spawn_with_entity(dir.path(), tape.clone(), 10).await;
    first.shutdown().await.expect("the nest stops");

    // The tip path will not seal eight rows (#1067). This test is about the entity
    // seed reading Parquet, not about maybe_seal, so the range is sealed through the
    // public seal API the pipeline itself calls. Feeding SEAL_DIRECT_BATCH events
    // through the entity circuit just to move the watermark hangs the nest.
    {
        let store = nuthatch::store::Store::open(&dir.path().join("nuthatch.redb")).unwrap();
        let rows = store.entities_in_range(1, 8).unwrap();
        assert_eq!(
            rows.len(),
            8,
            "fixture must hold [1,8] hot before we seal it"
        );
        nuthatch::seal::seal_range(dir.path(), &rows, 1, 8)
            .unwrap()
            .expect("range holds rows");
        store
            .prune_and_set_meta(1, 8, "sealed_through", "8")
            .unwrap();
        assert_eq!(store.sealed_through(), 8);
        assert!(
            store.entities_in_range(1, 8).unwrap().is_empty(),
            "manual seal must prune [1,8] or the seed could still read hot"
        );
    }

    for b in 11..=14u64 {
        tape.insert_block(b, empty_block(b, 0, 1_700_000_100 + b));
    }
    tape.advance_tip_to(14);
    let first = spawn_with_entity(dir.path(), tape.clone(), 14).await;
    let store = first.state.store.clone();
    assert_eq!(
        store.sealed_through(),
        8,
        "respawn must keep the watermark the manual seal wrote"
    );

    // The premise, asserted rather than assumed: the sealed rows are gone from hot.
    let hot_rows = store.entities_in_range(1, 8).unwrap();
    assert!(
        hot_rows.is_empty(),
        "the sealed range is still in the hot store, so reading hot alone would be enough and this \
         test would prove nothing: {} row(s)",
        hot_rows.len()
    );

    let before = relation(&first);
    assert!(!before.is_empty());
    drop(store);
    first.shutdown().await.expect("the nest stops");

    let second = spawn_with_entity(dir.path(), tape.clone(), 14).await;
    let after = relation(&second);
    let unavailable = second.state.entities[0].unavailable().map(str::to_string);
    second.shutdown().await.expect("the nest stops");

    assert_eq!(unavailable, None, "the seed must have succeeded");
    assert_eq!(
        after, before,
        "a seeded entity must carry the sealed range, which only Parquet still holds"
    );
}

/// **`/explain` must answer for the database `/sql` runs.** A maintained relation is queryable by
/// name, so the endpoint whose entire job is "would this query bind?" has to know it exists.
///
/// The failure this pins was worse than a plain omission, and it is why the first assertion here is
/// the load-bearing one: DuckDB connections are pooled and `define_views` refreshes only the tables
/// in the *current* set, so a relation defined by an earlier `/sql` on that connection stayed bound.
/// The identical request returned `400 Table with name received does not exist` on a cold connection
/// and `200 valid` once any `/sql` had warmed one - the same question, two answers, decided by what
/// some other caller happened to run first.
///
/// So: explain **before** any `/sql` touches the entity. A test that queried in the other order
/// passed against the broken code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explain_binds_a_maintained_relation_on_a_cold_connection() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_with_entity(dir.path(), tape, CHAIN_LEN).await;
    rt.state.entities[0].flush();

    // First request of the test, before anything can have defined the view as a side effect.
    let (status, body) = get_json(&rt, "/explain?q=SELECT%20*%20FROM%20received").await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "/explain must bind a maintained relation on a cold connection: {body}"
    );
    assert_eq!(body["valid"], true, "{body}");

    // And it binds the relation's real columns, rather than anything that merely accepts the name.
    let (status, body) = get_json(&rt, "/explain?q=SELECT%20bogus_col%20FROM%20received").await;
    assert_eq!(
        status,
        axum::http::StatusCode::BAD_REQUEST,
        "a column the relation does not have must not bind: {body}"
    );

    // The two surfaces agree in both directions.
    let (status, body) = get_json(&rt, "/explain?q=SELECT%20*%20FROM%20no_such_relation").await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");

    rt.shutdown().await.expect("the nest stops");
}

/// **The series an operator alerts on.** There were none until the alpha: a maintained relation had
/// no `/metrics` presence at all, so the only way to ask "is it keeping up" was to poll `/ready` and
/// parse JSON - which is exactly what the first live tip-following run had to do, with a bespoke
/// shell script.
///
/// Asserted through the real router on a real nest, and asserted on the **values**, not just the
/// names: a series that exists and always reads zero is worse than no series, because a dashboard
/// built on it looks healthy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_metrics_endpoint_carries_the_entity_series() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_with_entity(dir.path(), tape, CHAIN_LEN).await;
    rt.state.entities[0].flush();

    let (status, body) = get_json(&rt, "/metrics").await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let text = body["raw"]
        .as_str()
        .expect("/metrics is plain text")
        .to_string();

    // Applied through the head it actually folded, not zero and not the head it was asked for.
    assert!(
        text.contains(&format!(
            "nuthatch_entity_applied_through{{nest=\"usdc\",entity=\"received\"}} {CHAIN_LEN}"
        )),
        "applied_through must carry the block it folded:\n{text}"
    );
    assert!(
        text.contains("nuthatch_entity_current{nest=\"usdc\",entity=\"received\"} 1"),
        "a caught-up relation reads current:\n{text}"
    );
    // The relation has one group in this fixture - every transfer goes to the same recipient - so a
    // `rows` series reading 0 would mean it is reporting the wrong thing, not that it is empty.
    assert!(
        text.contains("nuthatch_entity_rows{nest=\"usdc\",entity=\"received\"} 1"),
        "rows must be the relation's size:\n{text}"
    );
    for dead in ["faulted", "unavailable"] {
        assert!(
            text.contains(&format!(
                "nuthatch_entity_{dead}{{nest=\"usdc\",entity=\"received\"}} 0"
            )),
            "a healthy entity reads 0 for {dead}:\n{text}"
        );
    }
    assert!(
        text.contains("nuthatch_entity_seconds_since_progress{nest=\"usdc\",entity=\"received\"}"),
        "the wedged-detection series must be present:\n{text}"
    );
    // Every series carries HELP and TYPE, or Prometheus takes them as untyped.
    for name in [
        "applied_through",
        "current",
        "rows",
        "faulted",
        "unavailable",
        "seconds_since_progress",
    ] {
        assert!(
            text.contains(&format!("# TYPE nuthatch_entity_{name} gauge")),
            "nuthatch_entity_{name} needs a TYPE line:\n{text}"
        );
    }

    rt.shutdown().await.expect("the nest stops");
}

/// #1834: the circuit's own state size, sampled on its thread. The first sample follows the first
/// applied batch, so a nest that has folded anything reports a size; zero would mean nothing was
/// measured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_metrics_endpoint_carries_the_entity_state_size() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_with_entity(dir.path(), tape, CHAIN_LEN).await;
    rt.state.entities[0].flush();

    let (status, body) = get_json(&rt, "/metrics").await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let text = body["raw"].as_str().expect("/metrics is plain text");
    assert!(
        text.contains("# TYPE nuthatch_entity_state_bytes gauge"),
        "nuthatch_entity_state_bytes needs a TYPE line:\n{text}"
    );
    let bytes: u64 = text
        .lines()
        .find_map(|l| {
            l.strip_prefix("nuthatch_entity_state_bytes{nest=\"usdc\",entity=\"received\"} ")
        })
        .unwrap_or_else(|| panic!("no state size for the entity:\n{text}"))
        .parse()
        .expect("a byte count");
    assert!(
        bytes > 0,
        "an entity that has folded rows holds state:\n{text}"
    );

    rt.shutdown().await.expect("the nest stops");
}

/// **#822 criterion 6.** *"A query returning every maintained row still pays for those output rows
/// and remains bounded by existing guards. Documentation does not imply IVM repeals I/O."*
///
/// A maintained relation is cheaper to **derive** and not cheaper to **return**, so the row cap
/// binds it exactly as it binds a fact table. The trap is a route that reads "incremental" as a
/// reason to skip the guards.
///
/// Grouped by block rather than by recipient because the shared fixture sends every transfer to the
/// same address: one group, and a cap of one would bind nothing and pass regardless.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capped_query_over_a_maintained_relation_is_still_capped() {
    const PER_BLOCK: &str = r#"[[entities]]
name = "per_block"
query = "SELECT t.block_number, SUM(t.value) FROM usdc__transfer t GROUP BY t.block_number"
key = ["block_number"]
max_rows = 10000
"#;
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_declared(dir.path(), tape, CHAIN_LEN, PER_BLOCK).await;
    rt.state.entities[0].flush();

    let all = rt.state.entities[0].relation().len();
    assert_eq!(
        all as u64, CHAIN_LEN,
        "one group per block, or the cap below binds nothing"
    );

    // Uncapped: every maintained row comes back, and is not silently truncated.
    let (status, body) = get_json(&rt, "/sql?q=SELECT%20*%20FROM%20per_block").await;
    assert_eq!(status, axum::http::StatusCode::OK, "uncapped: {body}");
    assert_eq!(
        body["rows"].as_array().map(Vec::len),
        Some(all),
        "uncapped: {body}"
    );
    assert_eq!(body["truncated"], false, "uncapped: {body}");

    // Capped: the existing guard binds a maintained relation like any other table.
    let (status, body) = get_json(&rt, "/sql?q=SELECT%20*%20FROM%20per_block&max_rows=3").await;
    assert_eq!(status, axum::http::StatusCode::OK, "capped: {body}");
    assert_eq!(
        body["rows"].as_array().map(Vec::len),
        Some(3),
        "the cap must bind: {body}"
    );
    assert_eq!(
        body["truncated"], true,
        "and a capped result must say so rather than looking complete: {body}"
    );

    rt.shutdown().await.expect("the nest stops");
}

/// **#822 criterion 10, the local half.** An edited definition rebuilds from the facts already on
/// disk, and does not re-fetch them.
///
/// The *adoption* half - that an entity edit moves the package NID without moving the fact identity,
/// so a freshly-installed edited nest inherits the old dataset instead of re-indexing - is
/// `e2e_early_cutoff::declaring_an_entity_adopts_the_facts_instead_of_re_indexing_them`, and it has
/// to be, because this suite spawns a nest straight into a directory and never consults a data
/// identity. Removing `entities.toml` from `blob::NON_DATA_INPUTS` leaves every assertion here
/// passing, which is exactly how much this test knows about that mechanism: nothing.
///
/// What it does hold is the other failure: the edited definition is a **different relation over a
/// different column**, not a tweak, so a rebuild that quietly resumed the old circuit would still be
/// holding `to`-keyed rows and the assertion on the new keys would fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn editing_an_entity_rebuilds_from_stored_facts_without_refetching_them() {
    const SENT: &str = r#"[[entities]]
name = "sent"
query = "SELECT t.from, COUNT(*) FROM usdc__transfer t GROUP BY t.from"
key = ["from"]
max_rows = 10000
"#;

    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let first = spawn_with_entity(dir.path(), tape.clone(), CHAIN_LEN).await;
    assert!(
        !relation(&first).is_empty(),
        "the first run must actually fill it"
    );
    first.shutdown().await.expect("the nest stops");

    // Edit the definition. Nothing new to index, so any `logs` call is a historical re-fetch.
    let calls_before = tape.logs_call_count();
    let second = spawn_declared(dir.path(), tape.clone(), CHAIN_LEN, SENT).await;
    second.state.entities[0].flush();
    let name = second.state.entities[0].name().to_string();
    let rebuilt = relation(&second);
    let unavailable = second.state.entities[0].unavailable().map(str::to_string);
    let applied = second.state.entities[0].applied_through();
    second.shutdown().await.expect("the nest stops");

    assert_eq!(
        name, "sent",
        "the edited definition is the one that came up"
    );
    assert_eq!(
        unavailable, None,
        "the edited entity rebuilt rather than staying unavailable"
    );
    assert_eq!(
        applied, CHAIN_LEN,
        "and it answers for the head it was rebuilt through"
    );

    // Every block sends one transfer from the same sender in this fixture, so the new relation is
    // one group counting the whole chain. Asserted against the fixture rather than against the old
    // relation, which is the point: this is a *different* derivation over the same facts.
    assert_eq!(
        rebuilt,
        vec![(
            format!("Row([Str({:?})])", account(1)),
            format!("Row([Int({CHAIN_LEN})])"),
        )],
        "the edited entity must be the new derivation over the adopted facts"
    );

    let historical = tape.logs_call_count() - calls_before;
    assert!(
        historical < (CHAIN_LEN / 2) as usize,
        "editing a definition must adopt the decoded facts, not re-fetch them: {historical} logs \
         calls across a {CHAIN_LEN}-block chain"
    );
}

/// **#822 criterion 11, at the bar this slice actually sets.** *"An unrelated incremental entity
/// remains available while the changed one rebuilds, unless a shared dependency changed."*
///
/// RFC-0041 is explicit that v1 does not graft: *"The first implementation may rebuild all entities
/// locally after an NID change. The acceptance bar still requires zero historical RPC."* Per-entity
/// grafting is listed under "Post-v1, not a v1 slice" and is not testable until entity output is
/// persisted, because until then there is nothing durable to graft.
///
/// So the honest v1 statement is this: editing one entity does not cost the other one its answer or
/// its currency, and neither rebuild reaches for the network. A test asserting that the untouched
/// entity was *not recomputed* would be asserting something v1 does not claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn editing_one_entity_leaves_its_neighbour_answering() {
    const BOTH: &str = r#"[[entities]]
name = "received"
query = "SELECT t.to, SUM(t.value) FROM usdc__transfer t GROUP BY t.to"
key = ["to"]
max_rows = 10000

[[entities]]
name = "senders"
query = "SELECT t.from, COUNT(*) FROM usdc__transfer t GROUP BY t.from"
key = ["from"]
max_rows = 10000
"#;
    // Only `received` changes: SUM becomes COUNT. `senders` is byte-identical.
    const EDITED: &str = r#"[[entities]]
name = "received"
query = "SELECT t.to, COUNT(*) FROM usdc__transfer t GROUP BY t.to"
key = ["to"]
max_rows = 10000

[[entities]]
name = "senders"
query = "SELECT t.from, COUNT(*) FROM usdc__transfer t GROUP BY t.from"
key = ["from"]
max_rows = 10000
"#;

    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);

    let first = spawn_declared(dir.path(), tape.clone(), CHAIN_LEN, BOTH).await;
    for e in first.state.entities.iter() {
        e.flush();
    }
    let neighbour_before: Vec<_> = first.state.entities[1]
        .relation()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert!(
        !neighbour_before.is_empty(),
        "the neighbour must actually hold something"
    );
    first.shutdown().await.expect("the nest stops");

    let calls_before = tape.logs_call_count();
    let second = spawn_declared(dir.path(), tape.clone(), CHAIN_LEN, EDITED).await;
    for e in second.state.entities.iter() {
        e.flush();
    }
    let names: Vec<String> = second
        .state
        .entities
        .iter()
        .map(|e| e.name().to_string())
        .collect();
    let states: Vec<(Option<String>, Option<String>, u64)> = second
        .state
        .entities
        .iter()
        .map(|e| {
            (
                e.unavailable().map(str::to_string),
                e.fault(),
                e.applied_through(),
            )
        })
        .collect();
    let neighbour_after: Vec<_> = second.state.entities[1]
        .relation()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    second.shutdown().await.expect("the nest stops");

    assert_eq!(names, vec!["received", "senders"]);
    for (i, (unavailable, fault, applied)) in states.iter().enumerate() {
        assert_eq!(
            *unavailable, None,
            "entity {} is unavailable after the edit",
            names[i]
        );
        assert_eq!(*fault, None, "entity {} faulted after the edit", names[i]);
        assert_eq!(
            *applied, CHAIN_LEN,
            "entity {} did not come back current",
            names[i]
        );
    }
    assert_eq!(
        neighbour_after, neighbour_before,
        "the untouched entity must answer with exactly what it answered with before"
    );

    let historical = tape.logs_call_count() - calls_before;
    assert!(
        historical < (CHAIN_LEN / 2) as usize,
        "neither rebuild may re-fetch history: {historical} logs calls"
    );
}

/// **RFC-0041 §8 / #864 criterion 4, as a property.** *"Randomized apply/retract/replacement
/// sequences converge byte-for-byte to a clean replay."*
///
/// The fixed-fork case above is the readable one; this is the one that covers the depth. Both run
/// through `spawn_nest`, so the `+1` side comes from the decode registry and the `-1` side is
/// reconstructed from the hot store's JSON - two representations that must produce the same `Row` or
/// nothing cancels. `entity_view`'s own property test builds both sides itself and cannot see that.
///
/// Depth rather than fork position, and stratified, for the reason #291 records: a uniform fork over
/// a fixed chain is not a uniform depth, and a generator that never produces the deep case passes
/// forever while proving nothing. Case count is low because every case drives two full nests.
fn entity_reorg_depth() -> impl Strategy<Value = u64> {
    prop_oneof![
        1u64..=2,                       // the everyday tip flutter
        3u64..=(CHAIN_LEN / 2),         // mid
        (CHAIN_LEN / 2 + 1)..CHAIN_LEN, // deep: unwinds most of the chain
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(6))]
    #[test]
    fn an_entity_converges_at_any_reorg_depth(depth in entity_reorg_depth()) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(entity_converges_after_reorg(CHAIN_LEN - depth));
    }
}

/// The fixed case, kept as the readable one and as a guard on the generator's own reachability: a
/// deep fork here would be a different test from a shallow one, and this pins the mid case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entity_converges_on_a_clean_replay_after_a_reorg() {
    entity_converges_after_reorg(4).await;
}

/// #1921, the case CI shrank to: a two-block reorg cuts exactly the last window, so the replay is
/// one commit followed by one entity feed. Read the instant the store showed the replacement
/// block, the relation held the retraction and not yet the replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entity_converges_after_a_reorg_of_the_last_window() {
    entity_converges_after_reorg(CHAIN_LEN - 2).await;
}

/// **#822 criterion 2: a direct keyed read does not invoke DuckDB or scan canonical fact history.**
///
/// Asserted by construction rather than by inspecting a plan: `derived_key` reads the circuit's own
/// output map. What this test can check from outside is that the answer is *right*, that it carries
/// the provenance criterion 9 asks for, and that its applied-through is the **entity's** watermark
/// rather than the nest's head - which is the difference between reporting and pretending.
/// Drive one GET through the nest's **real router**, not a hand-built handler call.
///
/// The distinction is the point. The keyed-read test below this one used to read
/// `entity.relation()` directly and assert on the values, which proves the circuit holds the right
/// answer and says nothing about whether the route returns it, stamps provenance, or refuses when
/// it should. Two of #822's criteria are statements about a *response*.
async fn get_json(
    rt: &indexer::NestRuntime,
    path: &str,
) -> (axum::http::StatusCode, serde_json::Value) {
    use tower::ServiceExt;
    let router = nuthatch::serve::router(nuthatch::serve::SharedNest::new(rt.state.clone()));
    let req = axum::http::Request::builder()
        .uri(path)
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&body)
        .unwrap_or_else(|_| serde_json::json!({ "raw": String::from_utf8_lossy(&body) }));
    (status, value)
}

/// **#822 criterion 9, through the routes rather than beside them**, plus the `/sql` exposure that
/// shipped without a test of its own.
///
/// A maintained relation is queryable by its declared name, and the answer says so: which entity
/// answered, that it is incremental, how far it is applied, and whether that is current. A query
/// that touches no entity gets no such claim, because an empty array would assert something about
/// maintained state that a plain fact query has no business asserting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sql_route_serves_the_relation_by_name_and_says_where_it_came_from() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_with_entity(dir.path(), tape, CHAIN_LEN).await;
    rt.state.entities[0].flush();

    // The relation, by the name its author declared, through the analytical surface.
    let (status, body) = get_json(&rt, "/sql?q=SELECT%20*%20FROM%20received").await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "/sql over the entity: {body}"
    );
    assert!(
        body["provenance"].get("reproducibility").is_none(),
        "a chain-only entity's answer is not labelled as snapshot data (#1437): {body}"
    );
    let rows = body["rows"].as_array().expect("rows").clone();
    assert_eq!(
        rows.len(),
        rt.state.entities[0].relation().len(),
        "/sql must serve every maintained row: {body}"
    );
    assert!(
        rows.iter().all(|r| r.get("to").is_some()),
        "the author's own column names, not positional ones: {rows:?}"
    );

    // Criterion 9. `source: hot+sealed` describes the fact tables and says nothing about a relation
    // a circuit maintains, which is the gap this closes.
    let entities = body["provenance"]["entities"]
        .as_array()
        .unwrap_or_else(|| panic!("provenance.entities missing: {body}"));
    assert_eq!(entities.len(), 1, "one entity answered: {entities:?}");
    assert_eq!(entities[0]["entity"], "received");
    assert_eq!(entities[0]["incremental"], true);
    assert_eq!(entities[0]["applied_through"], CHAIN_LEN);
    assert_eq!(entities[0]["current"], true);

    // A query over raw facts touches no maintained state and must not claim to.
    let (status, body) = get_json(
        &rt,
        "/sql?q=SELECT%20count(*)%20AS%20n%20FROM%20usdc__transfer",
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "/sql over facts: {body}"
    );
    assert!(
        body["provenance"]["entities"].is_null(),
        "a fact query must not carry an entity provenance block: {body}"
    );

    // The surface bullet's other half: `/schema` - which the MCP `schema` tool relays verbatim - must
    // say the relation exists, that it is incremental, and how far it is applied. Without this an
    // agent can query `received` from `/sql` but has no way to discover that it is there.
    let (status, body) = get_json(&rt, "/schema").await;
    assert_eq!(status, axum::http::StatusCode::OK, "/schema");
    let doc = body["raw"].as_str().expect("/schema is plain text");
    assert!(
        doc.contains("MAINTAINED RELATIONS"),
        "/schema must have a maintained-relations section:\n{doc}"
    );
    assert!(
        doc.contains("received - applied through block 8"),
        "/schema must name the relation and its applied-through block:\n{doc}"
    );
    assert!(
        doc.contains("NOT recomputed per query"),
        "/schema must distinguish a maintained relation from an authored view:\n{doc}"
    );
    assert!(
        doc.contains("columns: to, "),
        "/schema must name the author's columns:\n{doc}"
    );

    // Criterion 2, through the route this time: the keyed read answers with provenance.
    let (status, body) = get_json(&rt, &format!("/derived/received/{}", account(2))).await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "/derived keyed read: {body}"
    );
    let want: i128 = (1..=CHAIN_LEN).map(|b| (100 * b) as i128).sum();
    assert_eq!(body["row"][0], want.to_string(), "the keyed row: {body}");
    let p = &body["provenance"];
    assert_eq!(p["entity"], "received");
    assert_eq!(p["incremental"], true);
    assert_eq!(p["from_maintained_state"], true);
    assert_eq!(p["applied_through"], CHAIN_LEN);
    assert_eq!(p["current"], true);

    rt.shutdown().await.expect("the nest stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_derived_keyed_read_answers_from_maintained_state_with_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_with_entity(dir.path(), tape, CHAIN_LEN).await;
    rt.state.entities[0].flush();

    let entity = &rt.state.entities[0];
    let want: i128 = (1..=CHAIN_LEN).map(|b| (100 * b) as i128).sum();
    let got = entity
        .relation()
        .get(&nuthatch::entity_row::Row(vec![
            nuthatch::entity_row::Scalar::Str(account(2)),
        ]))
        .cloned();
    assert_eq!(
        got,
        Some(nuthatch::entity_row::Row(vec![
            nuthatch::entity_row::Scalar::Int(want)
        ])),
        "the maintained relation must hold the answer a keyed read would return"
    );

    // Criterion 9's provenance, and the part that matters: the entity answers for the head it has
    // folded. Serving the nest's head here is exactly how a partial relation gets stamped current.
    assert_eq!(entity.applied_through(), CHAIN_LEN);
    assert!(entity.is_current(CHAIN_LEN));
    assert!(!entity.is_current(CHAIN_LEN + 1));
    assert_eq!(entity.unavailable(), None);
    assert_eq!(entity.fault(), None);

    rt.shutdown().await.expect("the nest stops");
}

/// #1437, end to end through `spawn_nest`: an entity joining an offchain table starts with the
/// snapshot already sealed, takes one dropped while it runs at the next window, and a warm restart
/// seeds both back. Only the full path shows the ingest loop really calls the refresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offchain_snapshot_dropped_while_running_reaches_the_entity_and_survives_a_restart() {
    use nuthatch::entity_row::{Row, Scalar};
    let dir = tempfile::tempdir().unwrap();
    let drop = |name: &str, tier: &str| {
        let csv = dir.path().join(format!("{name}.csv"));
        std::fs::write(&csv, format!("account,tier\n{},{tier}\n", account(2))).unwrap();
        nuthatch::offchain::drop_file(dir.path(), &csv, "tiers").unwrap();
    };
    let tape = Arc::new(TapeSource::new());
    for b in 1..=4 {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(4);
    drop("first", "gold");

    let rt = spawn_declared(dir.path(), tape.clone(), 4, BY_TIER).await;
    let tier = |t: &str| Row(vec![Scalar::Str(t.into())]);
    let sum = |v: i128| Some(Row(vec![Scalar::Int(v)]));
    let entity = &rt.state.entities[0];
    entity.flush();
    assert_eq!(
        entity.get(&tier("gold")),
        sum(1_000),
        "100 * (1 + 2 + 3 + 4)"
    );

    drop("second", "silver");
    for b in 5..=6 {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(6);
    let applied = wait_until(POLL_TIMEOUT, || {
        let (_, a) = entity.len_and_watermark();
        a.through == 6 && a.offchain.get("tiers").is_some_and(|m| m.snapshots == 2)
    })
    .await;
    assert!(applied, "the dropped snapshot never reached the entity");
    entity.flush();
    assert_eq!(entity.get(&tier("gold")), sum(2_100));
    assert_eq!(
        entity.get(&tier("silver")),
        sum(2_100),
        "account 2 is now both"
    );
    let before = relation(&rt);
    rt.shutdown().await.expect("the nest stops");

    let warm = spawn_declared(dir.path(), tape, 6, BY_TIER).await;
    let after = relation(&warm);
    let (_, applied) = warm.state.entities[0].len_and_watermark();
    warm.shutdown().await.expect("the nest stops");
    assert_eq!(after, before, "a warm restart seeds the snapshots back");
    assert_eq!(applied.offchain["tiers"].snapshots, 2);
}

/// #1437 slice 3, through the routes. An answer that read an offchain table, directly or through an
/// entity, says it is reproducible by snapshot and names the snapshots; a remembered answer says so
/// too; a chain-only answer asserts nothing of the kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_answer_that_read_offchain_data_says_it_is_reproducible_by_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("tiers.csv");
    std::fs::write(&csv, format!("account,tier\n{},gold\n", account(2))).unwrap();
    nuthatch::offchain::drop_file(dir.path(), &csv, "tiers").unwrap();
    let hash = nuthatch::offchain::load(dir.path()).unwrap().tables["tiers"][0]
        .hash
        .clone();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=4 {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(4);
    let rt = spawn_declared(dir.path(), tape, 4, BY_TIER).await;
    rt.state.entities[0].flush();
    let sql = |q: &str| format!("/sql?q={}", urlencoding_lite(q));

    let (status, direct) = get_json(&rt, &sql("SELECT * FROM offchain__tiers")).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{direct}");
    let provenance = &direct["provenance"];
    assert_eq!(provenance["reproducibility"], "snapshot", "{direct}");
    assert_eq!(provenance["offchain"]["offchain__tiers"]["snapshots"], 1);
    assert_eq!(
        provenance["offchain"]["offchain__tiers"]["latest"],
        hash.as_str()
    );
    let (_, again) = get_json(&rt, &sql("SELECT * FROM offchain__tiers")).await;
    assert_eq!(again["cached"], true, "{again}");
    assert_eq!(
        again["provenance"]["reproducibility"], "snapshot",
        "{again}"
    );

    let (_, through) = get_json(&rt, &sql("SELECT * FROM by_tier")).await;
    assert_eq!(
        through["provenance"]["reproducibility"], "snapshot",
        "{through}"
    );
    assert_eq!(
        through["provenance"]["entities"][0]["reproducibility"],
        "snapshot"
    );

    let (_, derived) = get_json(&rt, "/derived").await;
    assert_eq!(
        derived["entities"][0]["reproducibility"], "snapshot",
        "{derived}"
    );

    let (_, chain) = get_json(&rt, &sql("SELECT count(*) AS n FROM usdc__transfer")).await;
    assert!(
        chain["provenance"].get("reproducibility").is_none(),
        "{chain}"
    );
    assert!(chain["provenance"].get("offchain").is_none(), "{chain}");

    // A catalogue survey defines every view, offchain ones included, but reads none of their rows.
    let (status, survey) = get_json(
        &rt,
        &sql("SELECT count(*) AS n FROM information_schema.tables"),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{survey}");
    assert_eq!(survey["count"], 1, "{survey}");
    assert!(
        survey["provenance"].get("reproducibility").is_none(),
        "{survey}"
    );

    let (_, schema) = get_json(&rt, "/schema").await;
    let doc = schema["raw"].as_str().unwrap_or_default();
    assert!(
        doc.contains("reads offchain__tiers: reproducible by snapshot"),
        "{doc}"
    );
    rt.shutdown().await.expect("the nest stops");
}

/// #1437's proof. Sealed chain facts, a hot tail, and two append-only price snapshots: after each
/// snapshot the maintained relation must equal DuckDB's answer to the same SQL over the same sealed,
/// hot and offchain inputs. The second snapshot both adds a group and changes an existing one, and it
/// lands before two windows, so re-feeding it on the second would double it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offchain_entity_equals_duckdb_after_each_appended_snapshot() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    const PRICED: &str = r#"
[[entities]]
name = "priced"
query = "SELECT p.symbol, SUM(x.value * p.price_e8) AS usd_e8 FROM usdc__transfer x JOIN offchain__prices p ON x.address = p.token GROUP BY p.symbol"
key = ["symbol"]
max_rows = 10000
"#;
    const REFERENCE: &str = "SELECT p.symbol AS k, SUM(x.value_dec * p.price_e8) AS v \
         FROM usdc__transfer x JOIN offchain__prices p ON x.address = p.token GROUP BY p.symbol";
    const MAINTAINED: &str = "SELECT symbol AS k, usd_e8 AS v FROM priced";

    let dir = tempfile::tempdir().unwrap();
    let token = USDC.to_ascii_lowercase();
    let prices = |name: &str, rows: &[(&str, i64)]| {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("token", DataType::Utf8, false),
                Field::new("symbol", DataType::Utf8, false),
                Field::new("price_e8", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![token.as_str(); rows.len()])),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let path = dir.path().join(format!("{name}.parquet"));
        std::fs::write(&path, bytes).unwrap();
        nuthatch::offchain::drop_file(dir.path(), &path, "prices").unwrap();
    };
    let agree = |step: &str, maintained: &[(String, String)], reference: &[(String, String)]| {
        assert!(
            !reference.is_empty(),
            "{step}: the reference is empty, so it proves nothing"
        );
        assert_eq!(
            maintained, reference,
            "{step}: the entity and DuckDB disagree"
        );
    };

    let tape = Arc::new(TapeSource::new());
    for b in 1..=10u64 {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(10);
    prices("first", &[("USDC", 100_000_000)]);
    let first = spawn_declared(dir.path(), tape.clone(), 10, PRICED).await;
    first.shutdown().await.expect("the nest stops");
    {
        let store = nuthatch::store::Store::open(&dir.path().join("nuthatch.redb")).unwrap();
        let rows = store.entities_in_range(1, 8).unwrap();
        assert_eq!(
            rows.len(),
            8,
            "fixture must hold [1,8] hot before we seal it"
        );
        nuthatch::seal::seal_range(dir.path(), &rows, 1, 8)
            .unwrap()
            .expect("range holds rows");
        store
            .prune_and_set_meta(1, 8, "sealed_through", "8")
            .unwrap();
        assert!(store.entities_in_range(1, 8).unwrap().is_empty());
    }

    for b in 11..=12u64 {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(12);
    let rt = spawn_declared(dir.path(), tape.clone(), 12, PRICED).await;
    assert_eq!(
        rt.state.store.sealed_through(),
        8,
        "the chain facts are sealed"
    );
    rt.state.entities[0].flush();
    let one = sql_pairs(&rt, MAINTAINED).await;
    agree(
        "after the first snapshot",
        &one,
        &sql_pairs(&rt, REFERENCE).await,
    );

    prices("second", &[("USDC", 100_010_000), ("USDCe", 99_990_000)]);
    for b in 13..=16u64 {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(16);
    let entity = &rt.state.entities[0];
    let store = &rt.state.store;
    // The store commits a window before the entity is fed it (#1913), so neither watermark alone
    // says both sides hold block 16.
    let applied = wait_until(POLL_TIMEOUT, || {
        let (_, a) = entity.len_and_watermark();
        a.through == 16
            && a.offchain.get("prices").is_some_and(|m| m.snapshots == 2)
            && store.get_meta("last_block").ok().flatten().as_deref() == Some("16")
    })
    .await;
    assert!(applied, "the second snapshot never reached the entity");
    entity.flush();
    let two = sql_pairs(&rt, MAINTAINED).await;
    agree(
        "after the second snapshot",
        &two,
        &sql_pairs(&rt, REFERENCE).await,
    );
    assert_ne!(
        one, two,
        "the second snapshot changed nothing, so the comparison is vacuous"
    );
    assert!(two.iter().any(|(k, _)| k == "USDCe"), "{two:?}");
    rt.shutdown().await.expect("the nest stops");
}

/// From the 3.13.1 hardening pass: every maintained cell reached `/sql` as text, so a `COUNT(*)`
/// column was VARCHAR, `sum(n)` would not bind and `max(n)` compared strings ("5" over "29").
const COUNTS: &str = r#"[[entities]]
name = "counts"
query = "SELECT t.to, COUNT(*) AS n, MAX(CASE WHEN t.to = 'never' THEN 1 END) AS nothing FROM usdc__transfer t GROUP BY t.to"
key = ["to"]
max_rows = 10000

# Astra's review of #1583: a relation carrying block_number is still a relation, and a text
# column named like an event counter keeps its text.
[[entities]]
name = "by_block"
query = "SELECT t.block_number, COUNT(*) AS n FROM usdc__transfer t GROUP BY t.block_number"
key = ["block_number"]
max_rows = 10000

[[entities]]
name = "named_like_counters"
query = "SELECT t.to AS log_index, COUNT(*) AS n FROM usdc__transfer t GROUP BY t.to"
key = ["log_index"]
max_rows = 10000
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_maintained_count_is_numeric_in_sql() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for block in 1..=CHAIN_LEN {
        tape.insert_block(block, canonical_block(block));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_declared(dir.path(), tape, CHAIN_LEN, COUNTS).await;
    for e in rt.state.entities.iter() {
        e.flush();
    }
    let one = |sql: &'static str| {
        let rt = &rt;
        async move {
            let (status, body) = get_json(rt, &format!("/sql?q={}", urlencoding_lite(sql))).await;
            assert_eq!(status, axum::http::StatusCode::OK, "{sql} -> {body}");
            body["rows"][0].clone()
        }
    };
    assert_eq!(
        one("SELECT typeof(n) AS t FROM counts LIMIT 1").await["t"],
        "DECIMAL(38,0)"
    );
    assert_eq!(
        one("SELECT count(*) AS c FROM counts WHERE nothing IS NULL").await["c"],
        one("SELECT count(*) AS c FROM counts").await["c"],
        "a NULL cell must be NULL, not the text \"NULL\""
    );
    let summed = one("SELECT sum(n) AS s FROM counts").await["s"].to_string();
    let raw = one("SELECT count(*) AS c FROM usdc__transfer").await["c"].to_string();
    assert_eq!(summed.trim_matches('"'), raw.trim_matches('"'));
    assert_eq!(
        one("SELECT any_value(typeof(n)) AS t FROM by_block").await["t"],
        "DECIMAL(38,0)"
    );
    let named = one("SELECT log_index FROM named_like_counters LIMIT 1").await["log_index"].clone();
    assert!(
        named.as_str().is_some_and(|a| a.starts_with("0x")),
        "a text column named log_index lost its text: {named}"
    );
    // A running nest keeps the declarations it started with: a half-edited file on disk must not turn
    // its relations back into text (astra's re-review).
    std::fs::write(dir.path().join("entities.toml"), "[[entities\nnot toml").unwrap();
    assert_eq!(
        one("SELECT typeof(n) AS t FROM counts ORDER BY n LIMIT 1").await["t"],
        "DECIMAL(38,0)"
    );
    rt.shutdown().await.expect("the nest stops");
}

/// Astra's third review of #1583: the declarations a nest started from type its relations, even when
/// `entities.toml` is broken before the first query ever reads it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_broken_before_the_first_query_keeps_the_started_types() {
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for block in 1..=CHAIN_LEN {
        tape.insert_block(block, canonical_block(block));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_declared(dir.path(), tape, CHAIN_LEN, COUNTS).await;
    for e in rt.state.entities.iter() {
        e.flush();
    }
    std::fs::write(dir.path().join("entities.toml"), "[[entities\nnot toml").unwrap();
    let sql = "SELECT typeof(n) AS t FROM counts LIMIT 1";
    let (status, body) = get_json(&rt, &format!("/sql?q={}", urlencoding_lite(sql))).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["t"], "DECIMAL(38,0)", "{body}");
    rt.shutdown().await.expect("the nest stops");
}

/// The same quarantine when the fault lands on the **last** window and no block follows. An entity
/// records a failed step on its own thread, after the window that fed it has been checked, so on a
/// quiet chain the nest reported healthy until another block arrived. Found on a stationary fork:
/// `/ready` said faulted while `/nests` and the health gauge said indexing, for as long as the chain
/// stayed still.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_entity_that_faults_on_the_last_block_is_quarantined_without_another() {
    use nuthatch::health::RuntimeHealth;

    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let cfg = scaffold_nest(dir.path(), "lastfault", USDC);
    // One transfer a block: the bound admits every input but the last block's.
    entity_fixture::write(
        dir.path(),
        &format!(
            r#"[[entities]]
name = "received"
query = "SELECT t.to, SUM(t.value) FROM lastfault__transfer t GROUP BY t.to"
key = ["to"]
max_rows = {}
"#,
            CHAIN_LEN - 1
        ),
    )
    .unwrap();
    let health = Arc::new(RuntimeHealth::new());
    health.register("lastfault", &cfg.nest.chain);
    let _cursor = indexer::spawn_runtime(
        tape,
        vec![("lastfault".to_string(), dir.path().to_path_buf(), cfg)],
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

    let quarantined = wait_until(POLL_TIMEOUT, || health.status("lastfault").is_some()).await;
    assert!(
        quarantined,
        "a nest whose entity faulted on the last block must be quarantined without another block"
    );
}

/// Does the store hold the replacement head, i.e. has the reorg's replay been committed?
fn replacement_head_landed(store: &dyn nuthatch::store::HotStore) -> bool {
    let want = block_hash(CHAIN_LEN, 1);
    match store.get_entity(&nuthatch::store::Store::entity_key(CHAIN_LEN, 0)) {
        Ok(Some(raw)) => serde_json::from_str::<serde_json::Value>(&raw)
            .ok()
            .and_then(|v| v["block_hash"].as_str().map(|h| h == want))
            .unwrap_or(false),
        _ => false,
    }
}

/// #1921, read at the instant the store shows the replacement head rather than a poll later. The
/// store commits the replayed window before the entity is fed it (#1913), so the relation may still
/// be the retracted one; what it must never be is mislabelled. Under one lock, rows and watermark
/// agree: the fork with the retraction, or the head with the replay.
///
/// The read lands in that gap by construction: on a current-thread runtime the ingest task runs only
/// when this body yields, and nothing between the tape serving the replayed fetch and the commit's
/// blocking call pends. Yield until that fetch, then spin until the commit lands, then read.
#[tokio::test(flavor = "current_thread")]
async fn an_entity_read_as_the_store_reconverges_is_labelled_for_what_it_holds() {
    let fork = CHAIN_LEN - 2;
    let dir = tempfile::tempdir().unwrap();
    let tape = Arc::new(TapeSource::new());
    for b in 1..=CHAIN_LEN {
        tape.insert_block(b, canonical_block(b));
    }
    tape.advance_tip_to(CHAIN_LEN);
    let rt = spawn_with_entity(dir.path(), tape.clone(), CHAIN_LEN).await;
    let fetched_before = tape.logs_call_count();
    tape.reorg(
        fork,
        ((fork + 1)..=CHAIN_LEN).map(replacement_block).collect(),
    );
    let start = std::time::Instant::now();
    while tape.logs_call_count() == fetched_before {
        assert!(
            start.elapsed() < POLL_TIMEOUT,
            "the replayed window was never fetched"
        );
        tokio::task::yield_now().await;
    }
    while !replacement_head_landed(rt.state.store.as_ref()) {
        assert!(
            start.elapsed() < POLL_TIMEOUT,
            "the replayed window was fetched but never committed"
        );
        std::hint::spin_loop();
    }
    let entity = &rt.state.entities[0];
    entity.flush();
    let (rows, applied) = entity.rows_as_json_with_watermark();
    let replayed = rows.iter().any(|r| r["to"] == account(4));
    shutdown(rt);
    assert!(
        applied.through == if replayed { CHAIN_LEN } else { fork },
        "rows {rows:?} labelled through {}",
        applied.through
    );
}
