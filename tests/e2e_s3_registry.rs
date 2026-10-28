//! RFC-0073 FR15: a nest fetched by NID from a private S3 bucket, against a real S3-compatible
//! server. `nest publish` puts the bundle in the bucket; a runtime mounts it by NID through
//! `POST /_admin/nests`, and the three ways a fetch fails end the job with their `code`: a NID the
//! bucket lacks (`not_found`), a tampered blob (`verification_failed`), and a credential the server
//! rejects (`access_denied`, from a real `nuthatch dev` process, since credentials come only from
//! its environment).
//!
//! Gated like `e2e_minio_publish`, whose header says how to start versitygw: it skips without
//! `NUTHATCH_MINIO_ENDPOINT`, and CI sets `NUTHATCH_REQUIRE_MINIO=1` so a skip fails there. The
//! bucket, `NUTHATCH_REGISTRY_BUCKET` (default `nuthatch-registry-test`), must already exist and be
//! private; the test checks the second.

#![cfg(feature = "object-store")]

mod common;

use std::path::Path;
use std::sync::Arc;

use nuthatch::runtime;

use crate::e2e_runtime_lifecycle::{body_json, call, empty_runtime, wait_for_phase};
use common::tape::*;

pub(crate) fn endpoint() -> Option<String> {
    match std::env::var("NUTHATCH_MINIO_ENDPOINT") {
        Ok(u) => Some(u),
        Err(_) if std::env::var("NUTHATCH_REQUIRE_MINIO").is_ok() => {
            panic!("NUTHATCH_REQUIRE_MINIO is set but NUTHATCH_MINIO_ENDPOINT is not")
        }
        Err(_) => {
            eprintln!(
                "SKIPPED: set NUTHATCH_MINIO_ENDPOINT (plus AWS_* env, incl. AWS_ALLOW_HTTP=true) \
                 to run the S3 registry and seed suites"
            );
            None
        }
    }
}

fn bucket() -> String {
    std::env::var("NUTHATCH_REGISTRY_BUCKET").unwrap_or_else(|_| "nuthatch-registry-test".into())
}

/// Bundle a scaffolded nest and publish it with the CLI, as an operator would. Returns (nid, hash).
fn publish(registry: &str, name: &str, address: &str) -> (String, String) {
    let src = tempfile::tempdir().unwrap();
    scaffold_nest(src.path(), name, address);
    let nid = nuthatch::blob::nest_nid(src.path()).unwrap();
    let out = tempfile::tempdir().unwrap();
    let file = out.path().join(format!("{name}.bundle"));
    nuthatch::blob::bundle(src.path(), Some(&file), false, false).unwrap();
    let run = std::process::Command::new(env!("CARGO_BIN_EXE_nuthatch"))
        .args(["nest", "publish"])
        .arg(&file)
        .args(["--registry", registry])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&run.stdout).to_string();
    assert!(
        run.status.success(),
        "nest publish: {stdout}{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let field = |key: &str| {
        stdout
            .lines()
            .find_map(|l| l.trim().strip_prefix(key))
            .map(|v| v.trim().to_string())
            .unwrap_or_else(|| panic!("no {key} in: {stdout}"))
    };
    assert_eq!(field("nid:"), nid, "publish printed another nid");
    (nid, field("hash:"))
}

/// Rewrite one file inside a published bundle, leaving its manifest alone, and put it back.
async fn tamper(bucket: &str, prefix: &str, hash: &str) {
    use object_store::{path::Path as ObjPath, ObjectStore};
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_url(format!("s3://{bucket}"))
        .build()
        .unwrap();
    let key = ObjPath::from(format!("{prefix}/blobs/{hash}.bundle"));
    let bytes = store.get(&key).await.unwrap().bytes().await.unwrap();
    let mut archive = tar::Archive::new(&bytes[..]);
    let mut out = tar::Builder::new(Vec::new());
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_path_buf();
        let mut body = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut body).unwrap();
        if path == Path::new("abis/erc20.json") {
            body.push(b' ');
        }
        let mut header = entry.header().clone();
        header.set_size(body.len() as u64);
        header.set_cksum();
        out.append_data(&mut header, &path, &body[..]).unwrap();
    }
    let tampered = out.into_inner().unwrap();
    store.put(&key, tampered.into()).await.unwrap();
}

async fn wait_finished(routes: &axum::Router, name: &str) -> serde_json::Value {
    let job = wait_for_phase(routes, name, "failed").await;
    assert_eq!(job["phase"], "failed", "{job}");
    job
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_s3_registry_mounts_by_nid_and_codes_each_failure() {
    let Some(endpoint) = endpoint() else { return };
    let bucket = bucket();
    let prefix = format!(
        "fr15-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let registry = format!("s3://{bucket}/{prefix}");

    let anonymous = reqwest::get(format!("{endpoint}/{bucket}/")).await.unwrap();
    assert_eq!(
        anonymous.status().as_u16(),
        403,
        "the registry bucket must be private"
    );

    let (nid, _) = publish(&registry, "usdc", USDC);
    let (tampered_nid, tampered_hash) = publish(&registry, "arb", ARB);
    tamper(&bucket, &prefix, &tampered_hash).await;

    let roost = tempfile::tempdir().unwrap();
    let (mut handles, _tape, _intake) = empty_runtime(roost.path(), &nid).await;
    std::fs::remove_dir_all(runtime::MountTable::data_dir(roost.path(), &nid)).unwrap();
    handles.mount_ctx.registry = Some(registry.clone());
    let handles = Arc::new(tokio::sync::Mutex::new(handles));
    let jobs = runtime::start_mount_jobs(roost.path(), &handles, true)
        .await
        .unwrap();
    let routes = runtime::lifecycle_routes(handles.clone(), jobs, true, Some("open-token".into()));

    let mount = |name: &str, nid: &str| format!(r#"{{"name":"{name}","nid":"{nid}"}}"#);
    let (status, body) = call(
        &routes,
        "POST",
        "/_admin/nests",
        None,
        Some(&mount("usdc", &nid)),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::ACCEPTED, "{body}");
    let job = wait_for_phase(&routes, "usdc", "live").await;
    assert_eq!(job["phase"], "live", "{job}");
    assert!(job.get("code").is_none(), "{job}");
    let deadline = std::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let sql = body_json(
            &handles.lock().await.live,
            "/usdc/sql?q=SELECT%20count(*)%20AS%20n%20FROM%20usdc__transfer",
        )
        .await;
        if sql["rows"][0]["n"] == 3 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the fetched nest never served its three transfers: {sql}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let missing = "ab".repeat(32);
    call(
        &routes,
        "POST",
        "/_admin/nests",
        None,
        Some(&mount("ghost", &missing)),
    )
    .await;
    let job = wait_finished(&routes, "ghost").await;
    assert_eq!(job["code"], "not_found", "{job}");

    call(
        &routes,
        "POST",
        "/_admin/nests",
        None,
        Some(&mount("arb", &tampered_nid)),
    )
    .await;
    let job = wait_finished(&routes, "arb").await;
    assert_eq!(job["code"], "verification_failed", "{job}");

    let job = denied_mount(&registry, &nid).await;
    assert_eq!(job["code"], "access_denied", "{job}");
}

/// A real `nuthatch dev` runtime whose environment carries a secret the server rejects, asked to
/// mount `nid`; returns the failed job.
async fn denied_mount(registry: &str, nid: &str) -> serde_json::Value {
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
        .args([
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--registry",
            registry,
        ])
        .env("AWS_SECRET_ACCESS_KEY", "not-the-secret")
        .env("NUTHATCH_ADMIN_TOKEN", "fr15")
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
                .bearer_auth("fr15")
                .json(&serde_json::json!({"name": "usdc", "nid": nid}))
                .send()
                .await
                .is_ok_and(|r| r.status() == reqwest::StatusCode::ACCEPTED);
        } else if let Ok(r) = client
            .get(format!("{base}/_admin/mounts/usdc"))
            .bearer_auth("fr15")
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
    job
}

pub(crate) struct Killed(pub(crate) std::process::Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
