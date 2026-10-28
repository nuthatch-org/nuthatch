//! RFC-0073 FR16 against a real S3-compatible server: a mount whose `[mounts.publish]` target is an
//! `s3://` prefix mirrors its dataset there, and a second runtime mounts the same NID with
//! `"seed": true` and that target through `POST /_admin/nests`, serves the publisher's rows and
//! follows the chain. A credential the server rejects ends the job `access_denied`, a tampered
//! segment `verification_failed`, and the retry after repair fetches only that segment.
//!
//! Gated like `e2e_s3_registry`: it skips without `NUTHATCH_MINIO_ENDPOINT`, and CI sets
//! `NUTHATCH_REQUIRE_MINIO=1` so a skip fails there. The bucket is `NUTHATCH_MINIO_BUCKET`.

#![cfg(feature = "object-store")]

mod common;

use std::path::PathBuf;

use futures::TryStreamExt;
use nuthatch::{indexer, runtime};
use object_store::{path::Path as ObjPath, ObjectStore};

use crate::e2e_runtime_lifecycle::{call, wait_for_phase};
use crate::e2e_s3_registry::{endpoint, Killed};
use crate::e2e_seed_from_mirror::{chain, stopped_publisher, PUBLISHER};
use crate::e2e_seed_mount::{last_block, publisher_inputs, seeding_runtime, transfers_through};
use common::tape::*;

/// Unique in the test binary: the publisher's metrics are process-global, keyed by mount.
const PUBLISHING_MOUNT: &str = "fr16-s3-publisher";

fn bucket() -> String {
    std::env::var("NUTHATCH_MINIO_BUCKET").unwrap_or_else(|_| "nuthatch-publish-test".into())
}

fn s3(bucket: &str) -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::from_env()
        .with_url(format!("s3://{bucket}"))
        .build()
        .unwrap()
}

/// Every key under `prefix`.
async fn keys(store: &object_store::aws::AmazonS3, prefix: &str) -> Vec<String> {
    store
        .list(Some(&ObjPath::from(prefix)))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap()
}

/// A runtime whose one mount declares `[mounts.publish] target = target`, left publishing until its
/// seed snapshot is in the bucket, then stopped so nothing rewrites what the test tampers with.
async fn mirror_from_a_publishing_mount(
    store: &object_store::aws::AmazonS3,
    prefix: &str,
    target: &str,
) -> (tempfile::TempDir, PathBuf) {
    let src = tempfile::tempdir().unwrap();
    publisher_inputs(src.path());
    let nid = nuthatch::blob::nest_nid(src.path()).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let roost = dir.path();
    let dataset = runtime::MountTable::data_dir(roost, &nid);
    std::fs::create_dir_all(&dataset).unwrap();
    publisher_inputs(&dataset);
    stopped_publisher(&dataset).await;
    assert_eq!(
        nuthatch::blob::nest_nid(&dataset).unwrap(),
        nid,
        "indexing moved the publishing mount's dataset off its NID"
    );
    // A mount's publisher takes the snapshot's range from its cursor's metrics, since a running
    // cursor holds the store; this stands in for that cursor.
    let store_db = nuthatch::store::Store::open(&dataset.join("nuthatch.redb")).unwrap();
    let indexed_from: u64 = store_db
        .get_meta("start_block")
        .unwrap()
        .expect("the publisher recorded where it began")
        .parse()
        .unwrap();
    let cursor = nuthatch::metrics::METRICS.nest(PUBLISHING_MOUNT);
    cursor.set_indexed_from(indexed_from);
    cursor.set_sealed_through(store_db.sealed_through());
    drop(store_db);
    std::fs::write(
        roost.join(runtime::MOUNTS_FILE),
        format!(
            "[runtime]\nname = \"publisher\"\nnests = []\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
             chain_id = 42161\nrpc_urls = []\n\n[[mounts]]\nalias = \"{PUBLISHING_MOUNT}\"\n\
             nid = \"{nid}\"\n\n[mounts.publish]\ntarget = \"{target}\"\ninterval = \"1s\"\n"
        ),
    )
    .unwrap();
    let table = runtime::MountTable::load(roost).unwrap();
    let publishers = runtime::spawn_publishers(roost, &table.mounts, None, "default").unwrap();
    assert_eq!(publishers.len(), 1, "the mount must publish");

    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    while !keys(store, prefix)
        .await
        .iter()
        .any(|k| k.ends_with("/_seed/seed.json"))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the publishing mount wrote no seed snapshot to {target}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    drop(publishers);
    (dir, dataset)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_s3_seed_mount_seeds_from_a_mirror_and_codes_each_failure() {
    let Some(_) = endpoint() else { return };
    let bucket = bucket();
    let prefix = format!(
        "fr16-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let target = format!("s3://{bucket}/{prefix}");
    let store = s3(&bucket);
    let (_publisher_roost, publisher) =
        mirror_from_a_publishing_mount(&store, &prefix, &target).await;

    // Seeded clean: both files fetched from the bucket, the publisher's rows, then the chain.
    let roost = tempfile::tempdir().unwrap();
    let tape = chain(14);
    let (handles, routes, nid) = seeding_runtime(roost.path(), tape.clone(), None, None).await;
    let body = serde_json::json!({
        "name": "usdc", "nid": nid, "seed": true, "publish": {"target": target},
    })
    .to_string();
    let (status, accepted) = call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED, "{accepted}");
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert_eq!(job["seed_progress"]["segments"], 2, "{job}");
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
        "a mount seeded from S3 asked the chain for history its mirror held: {asked:?}"
    );
    assert_eq!(
        transfers_through(&data, 8),
        transfers_through(&publisher, 8),
        "the rows seeded from S3 are not the publisher's"
    );
    let table = runtime::MountTable::load(roost.path()).unwrap();
    let record = table.mounts.iter().find(|m| m.alias == "usdc").unwrap();
    assert_eq!(
        record.publish.as_ref().map(|p| p.target.as_str()),
        Some(target.as_str())
    );
    // The seeded mount now publishes to the same prefix; stopped, so it cannot repair the tamper.
    handles.lock().await.publishers.clear();
    drop((handles, routes));

    let job = denied_seed(&nid, &target).await;
    assert_eq!(job["code"], "access_denied", "{job}");

    let segment = keys(&store, &prefix)
        .await
        .into_iter()
        .find(|k| k.ends_with(".parquet") && !k.contains("/_seed/"))
        .expect("the mirror holds a full segment");
    let segment = ObjPath::from(segment);
    let original = store
        .get(&segment)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    let mut tampered = original.clone();
    let mid = tampered.len() / 2;
    tampered[mid] ^= 0xff;
    store.put(&segment, tampered.into()).await.unwrap();

    let roost = tempfile::tempdir().unwrap();
    let (handles, routes, nid) = seeding_runtime(roost.path(), chain(14), None, None).await;
    call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    let job = wait_for_phase(&routes, "usdc", "failed").await;
    assert_eq!(job["phase"], "failed", "{job}");
    assert_eq!(job["code"], "verification_failed", "{job}");
    assert_eq!(job["seed_progress"]["done"], 1, "the tail lands: {job}");
    assert!(handles.lock().await.states.is_empty(), "nothing joined");

    store.put(&segment, original.into()).await.unwrap();
    call(&routes, "POST", "/_admin/nests", None, Some(&body)).await;
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert_eq!(job["seed_progress"]["done"], 2, "{job}");
    assert_eq!(
        job["seed_progress"]["fetched"], 1,
        "the retry must fetch only the segment the first attempt refused: {job}"
    );
    assert_eq!(
        transfers_through(&runtime::MountTable::data_dir(roost.path(), &nid), 8),
        transfers_through(&publisher, 8),
        "the rows seeded on retry are not the publisher's"
    );
    handles.lock().await.publishers.clear();
}

/// A real `nuthatch dev` runtime whose environment carries a secret the server rejects, asked to
/// seed `nid` from `target`; returns the failed job. The bundle comes from a filesystem registry, so
/// the only S3 call is the seed's.
async fn denied_seed(nid: &str, target: &str) -> serde_json::Value {
    let src = tempfile::tempdir().unwrap();
    publisher_inputs(src.path());
    let file = src.path().join("seedpub.bundle");
    nuthatch::blob::bundle(src.path(), Some(&file), false, false).unwrap();
    let registry = tempfile::tempdir().unwrap();
    let reg = nuthatch::distribution::open(registry.path().to_str().unwrap()).unwrap();
    nuthatch::distribution::publish(reg.as_ref(), &file, Some(PUBLISHER), None)
        .await
        .unwrap();

    let roost = tempfile::tempdir().unwrap();
    std::fs::write(
        roost.path().join(runtime::MOUNTS_FILE),
        "[runtime]\nname = \"r\"\nnests = []\n\n[[chains]]\nchain = \"arbitrum-one\"\nchain_id = 42161\nrpc_urls = []\n",
    )
    .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let base = format!("http://127.0.0.1:{port}");
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["dev", "--dir"])
        .arg(roost.path())
        .args(["--listen", &format!("127.0.0.1:{port}"), "--registry"])
        .arg(registry.path())
        .env("AWS_SECRET_ACCESS_KEY", "not-the-secret")
        .env("NUTHATCH_ADMIN_TOKEN", "fr16")
        .stdout(std::process::Stdio::null())
        .spawn()
        .map(Killed)
        .unwrap();
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    let mut job = serde_json::Value::Null;
    let mut posted = false;
    while std::time::Instant::now() < deadline {
        if let Ok(Some(exit)) = child.0.try_wait() {
            panic!("the runtime exited before it mounted: {exit}");
        }
        if !posted {
            posted = client
                .post(format!("{base}/_admin/nests"))
                .bearer_auth("fr16")
                .json(&serde_json::json!({
                    "name": "usdc", "nid": nid, "seed": true, "publish": {"target": target},
                }))
                .send()
                .await
                .is_ok_and(|r| r.status() == reqwest::StatusCode::ACCEPTED);
        } else if let Ok(r) = client
            .get(format!("{base}/_admin/mounts/usdc"))
            .bearer_auth("fr16")
            .send()
            .await
        {
            job = r.json().await.unwrap_or_default();
            if job["phase"] == "failed" {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    drop(child);
    assert_eq!(job["phase"], "failed", "{job}");
    assert!(
        job["reason"]
            .as_str()
            .is_some_and(|r| r.contains("seeding 'usdc'")),
        "the denial must come from the seed, not the fetch: {job}"
    );
    job
}
