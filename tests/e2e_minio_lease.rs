//! RFC-0078 S0 against a real S3-compatible store: the writer lease's conditional put is what
//! arbitrates between hosts, so it has to be shown refusing a stale write on the store CI runs, not
//! only on `memory://`. Two boots race for one dataset's lease and exactly one takes it; a lease
//! nobody renews is taken over at the next fence. Skips without `NUTHATCH_MINIO_ENDPOINT`, as
//! `e2e_minio_publish` does, and fails under `NUTHATCH_REQUIRE_MINIO`.

#![cfg(feature = "object-store")]

use std::path::Path;

use nuthatch::runtime::{take_writer_leases, MountTable};

fn endpoint() -> Option<String> {
    match std::env::var("NUTHATCH_MINIO_ENDPOINT") {
        Ok(u) => Some(u),
        Err(_) if std::env::var("NUTHATCH_REQUIRE_MINIO").is_ok() => {
            panic!("NUTHATCH_REQUIRE_MINIO is set but NUTHATCH_MINIO_ENDPOINT is not")
        }
        Err(_) => {
            eprintln!("SKIPPED: set NUTHATCH_MINIO_ENDPOINT to run the MinIO lease suite");
            None
        }
    }
}

fn bucket() -> String {
    std::env::var("NUTHATCH_MINIO_BUCKET").unwrap_or_else(|_| "nuthatch-publish-test".into())
}

/// A runtime directory with one installed nest and a mount publishing it with failover on.
fn runtime_dir(root: &Path, target: &str) -> MountTable {
    let nid = "d".repeat(64);
    let data = MountTable::data_dir(root, &nid);
    std::fs::create_dir_all(data.join("abis")).unwrap();
    std::fs::write(
        data.join("nuthatch.toml"),
        "[nest]\nname = \"leased\"\nchain = \"ethereum\"\nchain_id = 1\n\
         rpc_urls = [\"http://127.0.0.1:1\"]\nschema_version = 1\n\n\
         [[contracts]]\nalias = \"usdc\"\n\
         address = \"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48\"\nabi = \"abis/usdc.json\"\n",
    )
    .unwrap();
    std::fs::write(
        data.join("abis/usdc.json"),
        r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
    )
    .unwrap();
    toml::from_str(&format!(
        "[runtime]\nname = \"t\"\nchain = \"ethereum\"\nchain_id = 1\n\n\
         [[mounts]]\nalias = \"usdc\"\nnid = \"{nid}\"\n\n\
         [mounts.publish]\ntarget = \"{target}\"\n\n\
         [mounts.publish.failover]\nterm = \"3s\"\n"
    ))
    .unwrap()
}

#[tokio::test]
async fn e2e_minio_lease_two_boots_race_and_one_wins_then_a_lapsed_lease_moves() {
    if endpoint().is_none() {
        return;
    }
    let prefix = format!(
        "rfc-0078-s0/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let target = format!("s3://{}/{prefix}", bucket());
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let ta = runtime_dir(a.path(), &target);
    let tb = runtime_dir(b.path(), &target);

    let (ra, rb) = tokio::join!(
        take_writer_leases(a.path(), &ta.mounts, None, "default"),
        take_writer_leases(b.path(), &tb.mounts, None, "default"),
    );
    // Both reading before either writes is the race: one wins, the other loses at the store. One
    // reading after the other wrote waits out the unrenewed lease and takes the next fence instead.
    let mut fences: Vec<u64> = [&ra, &rb]
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .map(|w| w[0].fence().unwrap())
        .collect();
    fences.sort();
    assert!(
        fences == [1] || fences == [1, 2],
        "never two holders at one fence: {fences:?}, {:?} / {:?}",
        ra.as_ref().err(),
        rb.as_ref().err()
    );
    if let Some(loser) = ra.as_ref().err().or(rb.as_ref().err()) {
        assert!(
            format!("{loser:#}").contains("lost the writer lease"),
            "the loser lost the race rather than failing a request: {loser:#}"
        );
    }

    // Nobody renews the winner's lease, so a third boot waits out term and margin and takes it.
    let c = tempfile::tempdir().unwrap();
    let tc = runtime_dir(c.path(), &target);
    let started = std::time::Instant::now();
    let taken = take_writer_leases(c.path(), &tc.mounts, None, "default")
        .await
        .expect("a lapsed lease is taken over");
    assert_eq!(taken.len(), 1);
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(4_500),
        "taken before term and margin had passed: {:?}",
        started.elapsed()
    );
}
