//! RFC-0073 FR16: `POST /_admin/nests {name, nid, "seed": true}` fills the dataset from its mirror
//! before it indexes. A mirror written by a publishing nest seeds a runtime's mount, which then serves
//! the seeded rows and follows the chain from the seed's watermark. A segment that does not hash to
//! its address ends the job `failed` with `verification_failed`, and the next attempt keeps what the
//! first fetched.

mod common;

use std::path::Path;
use std::sync::Arc;

use nuthatch::{indexer, runtime};

use crate::e2e_runtime_lifecycle::{call, empty_runtime, wait_for_phase, OPEN};
use crate::e2e_seed_from_mirror::{chain, published, PUBLISHER};
use common::tape::*;

/// An empty runtime following `tape` whose dataset is the publisher's nest: already on disk, or
/// fetched from `registry` by the mount, as a rebuilt runtime holds nothing.
async fn seeding_runtime(
    roost: &Path,
    tape: Arc<dyn nuthatch::source::Source>,
    default_mirror: Option<&Path>,
    registry: Option<&Path>,
) -> (
    Arc<tokio::sync::Mutex<runtime::RuntimeHandles>>,
    axum::Router,
    String,
) {
    let src = tempfile::tempdir().unwrap();
    scaffold_nest(src.path(), PUBLISHER, USDC);
    let nid = nuthatch::blob::nest_nid(src.path()).unwrap();
    let (mut handles, _tape, intake) = empty_runtime(roost, &nid).await;
    std::mem::forget(intake);
    let data = runtime::MountTable::data_dir(roost, &nid);
    std::fs::remove_dir_all(&data).unwrap();
    match registry {
        Some(registry) => {
            let file = src.path().join("seedpub.bundle");
            nuthatch::blob::bundle(src.path(), Some(&file), false, false).unwrap();
            let store = nuthatch::distribution::open(registry.to_str().unwrap()).unwrap();
            nuthatch::distribution::publish(store.as_ref(), &file, Some(PUBLISHER), None)
                .await
                .unwrap();
            handles.mount_ctx.registry = Some(registry.to_str().unwrap().to_string());
        }
        None => {
            std::fs::create_dir_all(&data).unwrap();
            scaffold_nest(&data, PUBLISHER, USDC);
        }
    }
    handles
        .mount_ctx
        .sources
        .insert("arbitrum-one".to_string(), tape);
    handles.mount_ctx.publish = default_mirror.map(|m| runtime::MountPublish {
        target: m.to_str().unwrap().to_string(),
        interval: "60s".to_string(),
        parallelism: 4,
    });
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost, &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, Some(OPEN.into()));
    (handles, routes, nid)
}

fn last_block(dir: &Path, handles: &runtime::RuntimeHandles) -> Option<String> {
    let (_, state) = handles.states.iter().find(|(_, s)| s.dir == dir)?;
    state.store.get_meta("last_block").ok().flatten()
}

fn transfers_through(dir: &Path, block: u64) -> serde_json::Value {
    let sql = format!(
        "SELECT block_number, log_index FROM {} WHERE block_number <= {block} \
         ORDER BY block_number, log_index",
        transfer_table(PUBLISHER)
    );
    serde_json::json!(nuthatch::analytics::query(dir, &sql).expect("the transfer table answers"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mount_seeds_from_its_mirror_then_serves_and_follows_the_chain() {
    let publisher = tempfile::tempdir().unwrap();
    let mirror = tempfile::tempdir().unwrap();
    published(publisher.path(), mirror.path()).await;

    let roost = tempfile::tempdir().unwrap();
    let tape = chain(14);
    let (handles, routes, nid) = seeding_runtime(roost.path(), tape.clone(), None, None).await;
    let body = serde_json::json!({
        "name": "usdc",
        "nid": nid,
        "seed": true,
        "publish": {"target": mirror.path().to_str().unwrap()},
    });
    let (status, accepted) = call(
        &routes,
        "POST",
        "/_admin/nests",
        None,
        Some(&body.to_string()),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED, "{accepted}");

    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert_eq!(job["seed"], true, "{job}");
    // The full segment 1..=5 and the provisional tail 6..=8, both fetched now.
    assert_eq!(job["seed_progress"]["segments"], 2, "{job}");
    assert_eq!(job["seed_progress"]["done"], 2, "{job}");
    assert_eq!(job["seed_progress"]["fetched"], 2, "{job}");

    let data = runtime::MountTable::data_dir(roost.path(), &nid);
    assert!(
        wait_until(POLL_TIMEOUT, || {
            let h = handles.try_lock();
            h.is_ok_and(|h| last_block(&data, &h).as_deref() == Some("14"))
        })
        .await,
        "the seeded mount did not follow the chain to block 14"
    );
    let floor = 9 - indexer::FETCH_TAIL_OVERLAP;
    let asked = tape.logs_ranges();
    assert!(
        asked.iter().all(|(from, _)| *from >= floor),
        "a seeded mount asked the chain for history its mirror held: {asked:?}"
    );
    assert_eq!(
        transfers_through(&data, 8),
        transfers_through(publisher.path(), 8),
        "the seeded rows are not the publisher's"
    );

    let table = runtime::MountTable::load(roost.path()).unwrap();
    let record = table.mounts.iter().find(|m| m.alias == "usdc").unwrap();
    assert_eq!(
        record.publish.as_ref().map(|p| p.target.as_str()),
        mirror.path().to_str(),
        "the mount's mirror must be recorded, so a restart keeps publishing to it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_segment_fails_the_seed_and_the_retry_keeps_what_it_fetched() {
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

    let roost = tempfile::tempdir().unwrap();
    let registry = tempfile::tempdir().unwrap();
    let (handles, routes, nid) = seeding_runtime(
        roost.path(),
        chain(14),
        Some(mirror.path()),
        Some(registry.path()),
    )
    .await;
    let body = serde_json::json!({"name": "usdc", "nid": nid, "seed": true}).to_string();
    call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    let job = wait_for_phase(&routes, "usdc", "failed").await;
    assert_eq!(job["phase"], "failed", "{job}");
    assert_eq!(job["code"], "verification_failed", "{job}");
    assert!(
        job["reason"]
            .as_str()
            .is_some_and(|r| r.contains("does not hash to its content address")),
        "{job}"
    );
    assert_eq!(job["seed_progress"]["segments"], 2, "{job}");
    assert_eq!(job["seed_progress"]["done"], 1, "the tail lands: {job}");
    assert!(handles.lock().await.states.is_empty(), "nothing joined");

    let data = runtime::MountTable::data_dir(roost.path(), &nid);
    assert!(data.exists(), "a failed seed must keep its dataset");
    let db = data.join("nuthatch.redb");
    assert!(
        !db.exists()
            || nuthatch::store::Store::open(&db)
                .unwrap()
                .get_meta("last_block")
                .unwrap()
                .is_none(),
        "a failed seed must leave nothing that reads as indexed"
    );

    std::fs::write(&segment, &original).unwrap();
    call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert_eq!(job["seed_progress"]["done"], 2, "{job}");
    assert_eq!(
        job["seed_progress"]["fetched"], 1,
        "the retry must fetch only the segment the first attempt refused: {job}"
    );
    assert_eq!(
        transfers_through(&data, 8),
        transfers_through(publisher.path(), 8),
        "the seeded rows are not the publisher's"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seed_with_no_mirror_fails_and_a_seed_cannot_wait() {
    let roost = tempfile::tempdir().unwrap();
    let (handles, routes, nid) = seeding_runtime(roost.path(), chain(14), None, None).await;
    let body = serde_json::json!({"name": "usdc", "nid": nid, "seed": true}).to_string();
    let (status, refused) = call(
        &routes,
        "POST",
        "/_admin/nests?wait=true",
        None,
        Some(&body),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{refused}");

    let bad =
        serde_json::json!({"name": "usdc", "nid": nid, "publish": {"target": ""}}).to_string();
    let (status, refused) = call(&routes, "POST", "/_admin/nests", None, Some(&bad)).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{refused}");

    call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    let job = wait_for_phase(&routes, "usdc", "failed").await;
    assert_eq!(job["phase"], "failed", "{job}");
    assert!(
        job["reason"]
            .as_str()
            .is_some_and(|r| r.contains("has no mirror")),
        "{job}"
    );
    assert!(handles.lock().await.states.is_empty());
}
