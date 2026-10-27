//! The 4.x drop-in upgrade promise, enforced against data an earlier release wrote.
//!
//! `tests/fixtures/upgrade/<version>/` holds runtime directories written by released code and never
//! regenerated. Each is copied to a temp dir, opened by this build, and read back to exact values.

mod common;
#[path = "common/entity_fixture.rs"]
mod entity_fixture;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use nuthatch::runtime::{MountTable, DATA_DIR, MOUNTS_FILE};
use nuthatch::store::Store;
use nuthatch::{health::RuntimeHealth, indexer, migrate, serve};

use common::tape::*;

const RECEIVED: &str = r#"[[entities]]
name = "received"
query = "SELECT t.to, SUM(CAST(t.value AS HUGEINT)) AS sum_value FROM usdc__transfer t GROUP BY t.to"
key = ["to"]
max_rows = 10000
"#;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/upgrade")
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// Block `b` carries one transfer, account(1) to account(2), of `100 * b`.
fn chain_block(b: u64) -> BlockFixture {
    transfers_block(
        b,
        0,
        1_700_000_000 + b,
        USDC,
        &[(account(1).as_str(), account(2).as_str(), (100 * b) as u128)],
    )
}

async fn start(root: &Path, tape: Arc<TapeSource>) -> indexer::ChainCursor {
    let mounts = MountTable::load(root).unwrap();
    let default_tenant = mounts.tenant_default();
    let datasets = mounts.datasets(root);
    let health = Arc::new(RuntimeHealth::new());
    let nests = datasets
        .iter()
        .map(|ds| {
            let key = ds.canonical().route_key(&default_tenant);
            health.register(&key, "arbitrum-one");
            let cfg = nuthatch::config::Config::load(&ds.dir).unwrap();
            (key, ds.dir.clone(), cfg)
        })
        .collect();
    indexer::spawn_runtime(
        tape,
        nests,
        None,
        false,
        1,
        Some(2),
        false,
        None,
        health,
        false,
    )
    .await
    .expect("spawn_runtime")
}

async fn last_block_reaches(cursor: &indexer::ChainCursor, n: u64) -> bool {
    let want = n.to_string();
    wait_until(POLL_TIMEOUT, || {
        cursor.states.iter().all(|(_, s)| {
            s.store.get_meta("last_block").ok().flatten().as_deref() == Some(want.as_str())
        })
    })
    .await
}

/// Writes a new fixture directory. Run once per release line, on that release's tag, and commit the
/// output under `tests/fixtures/upgrade/<version>/runtime`; an existing directory is never rewritten.
///
/// `NUTHATCH_UPGRADE_FIXTURE_OUT=<dir> cargo test --locked --test it write_upgrade_fixture -- --ignored`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "writes a fixture; run by hand on a release tag"]
async fn write_upgrade_fixture() {
    let out = PathBuf::from(
        std::env::var("NUTHATCH_UPGRADE_FIXTURE_OUT").expect("set NUTHATCH_UPGRADE_FIXTURE_OUT"),
    );
    assert!(
        !out.exists(),
        "{} exists; fixtures are never rewritten",
        out.display()
    );
    std::fs::create_dir_all(&out).unwrap();

    std::fs::write(
        out.join(MOUNTS_FILE),
        "[runtime]\nname = \"golden\"\nchain = \"arbitrum-one\"\nchain_id = 42161\n\
         rpc_urls = []\nnests = [\"primary\"]\n",
    )
    .unwrap();
    let nest = out.join("nests").join("primary");
    std::fs::create_dir_all(&nest).unwrap();
    scaffold_nest(&nest, "usdc", USDC);
    entity_fixture::write(&nest, RECEIVED).unwrap();
    migrate::run(&out, false, false).expect("migrate");

    // A second tenant's mount of the same dataset, so the fixture carries a shared record.
    let mut mounts = MountTable::load(&out).unwrap();
    let nid = mounts.mounts[0].nid.clone();
    mounts.mounts.push(nuthatch::runtime::Mount {
        tenant: "acme".to_string(),
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
        out.join(MOUNTS_FILE),
        toml::to_string_pretty(&mounts).unwrap(),
    )
    .unwrap();

    let tape = Arc::new(TapeSource::new());
    for b in 1..=6 {
        tape.insert_block(b, chain_block(b));
    }
    tape.advance_tip_to(6);
    let cursor = start(&out, tape.clone()).await;
    assert!(last_block_reaches(&cursor, 6).await, "did not index to 6");
    cursor.shutdown().await.expect("the cursor stops");

    force_seal_through(&out.join(DATA_DIR).join(&nid), 4);
    println!("fixture written to {} with nid {nid}", out.display());
}

const V3_13_2_NID: &str = "2bca092694c5833d2fecec20983b307f23c3a5507402c39bab4d3fab91231576";
const V3_13_2_SEGMENT: &str = "48ef579c891823d4fa21fbbc1a7f57fee856342ac232b856581f454a80e66816";

fn sha256_hex(path: &Path) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(std::fs::read(path).unwrap()))
}

async fn get(base: &str, path: &str) -> serde_json::Value {
    reqwest::get(format!("{base}{path}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn sql(base: &str, q: &str) -> serde_json::Value {
    reqwest::Client::new()
        .get(format!("{base}/sql"))
        .query(&[("q", q)])
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["rows"]
        .clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_v3_13_2_runtime_directory_opens_drop_in() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    copy_dir(&fixture_root().join("v3.13.2/runtime"), root);
    let data = root.join(DATA_DIR).join(V3_13_2_NID);

    // Mount records: two tenants sharing one dataset.
    let mounts = MountTable::load(root).unwrap();
    let records: Vec<_> = mounts
        .mounts
        .iter()
        .map(|m| (m.tenant.as_str(), m.alias.as_str(), m.nid.as_str()))
        .collect();
    assert_eq!(
        records,
        vec![
            ("default", "primary", V3_13_2_NID),
            ("acme", "mirror", V3_13_2_NID)
        ]
    );
    let datasets = mounts.datasets(root);
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].refcount(), 2);
    assert_eq!(datasets[0].dir, data);

    // The NID this build derives from the stored nest is the one the data is keyed by.
    assert_eq!(nuthatch::blob::nest_nid(&data).unwrap(), V3_13_2_NID);

    // Sealed segments are read, never rewritten.
    let segment = root
        .join("segments")
        .join(format!("{V3_13_2_SEGMENT}.parquet"));
    assert_eq!(sha256_hex(&segment), V3_13_2_SEGMENT);

    // The hot store, read through this build's table definitions.
    {
        let store = Store::open_existing(&data.join("nuthatch.redb")).unwrap();
        assert_eq!(store.get_meta("last_block").unwrap().as_deref(), Some("6"));
        assert_eq!(store.sealed_through(), 4);
        for b in [5, 6] {
            let raw = store.get_entity(&Store::entity_key(b, 0)).unwrap().unwrap();
            let row: serde_json::Value = serde_json::from_str(&raw).unwrap();
            assert_eq!(row, stored_row(b));
        }
    }

    let tape = Arc::new(TapeSource::new());
    for b in 1..=6 {
        tape.insert_block(b, chain_block(b));
    }
    tape.advance_tip_to(6);
    let cursor = start(root, tape.clone()).await;
    assert!(last_block_reaches(&cursor, 6).await, "did not reopen at 6");
    assert_eq!(tape.logs_ranges(), vec![], "reopening re-fetched history");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = serve::router(serve::SharedNest::new(cursor.states[0].1.clone()));
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    // Blocks 1-4 are sealed and 5-6 hot, so this reads both.
    let q = "SELECT block_number, log_index, \"from\", \"to\", value, block_timestamp \
             FROM usdc__transfer ORDER BY block_number";
    assert_eq!(sql(&base, q).await, sql_rows(1..=6));
    for b in [2, 6] {
        let path = format!("/entity/{}", Store::entity_key(b, 0));
        assert_eq!(
            get(&base, &path).await,
            stored_row(b),
            "point read of block {b}"
        );
    }
    let received = format!("/derived/received/{}", account(2));
    assert_eq!(
        get(&base, &received).await["row"],
        serde_json::json!(["2100"])
    );

    // A later write: two new blocks append behind the old ones.
    tape.insert_block(7, chain_block(7));
    tape.insert_block(8, chain_block(8));
    tape.advance_tip_to(8);
    assert!(
        last_block_reaches(&cursor, 8).await,
        "did not index past the fixture"
    );
    assert_eq!(sql(&base, q).await, sql_rows(1..=8));
    assert_eq!(
        get(&base, &received).await["row"],
        serde_json::json!(["3600"])
    );

    server.abort();
    cursor.shutdown().await.expect("the cursor stops");

    // And a later seal works. The fixture's segment is provisional, so it is merged into the new one.
    force_seal_through(&data, 7);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(data.join("segments/manifest.json")).unwrap())
            .unwrap();
    let spans: Vec<_> = manifest["tables"]["usdc__transfer"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["from_block"].clone(),
                s["to_block"].clone(),
                s["rows"].clone(),
            )
        })
        .collect();
    assert_eq!(spans, vec![(1.into(), 7.into(), 7.into())]);
    let sealed = nuthatch::analytics::query(
        &data,
        "SELECT block_number, value FROM usdc__transfer ORDER BY block_number",
    )
    .unwrap();
    let want: Vec<_> = (1..=7u64)
        .map(|b| serde_json::json!({"block_number": b, "value": (100 * b).to_string()}))
        .collect();
    assert_eq!(sealed, want);
}

/// A transfer row exactly as the hot store holds it and `/entity` serves it.
fn stored_row(b: u64) -> serde_json::Value {
    serde_json::json!({
        "_seq": b << 20,
        "address": USDC.to_lowercase(),
        "block_hash": block_hash(b, 0),
        "block_number": b,
        "block_timestamp": 1_700_000_000 + b,
        "from": account(1),
        "log_index": 0,
        "table": "usdc__transfer",
        "to": account(2),
        "tx_hash": format!("0x{:064x}", b << 20),
        "value": (100 * b).to_string(),
    })
}

fn sql_rows(blocks: std::ops::RangeInclusive<u64>) -> serde_json::Value {
    blocks
        .map(|b| {
            serde_json::json!({
                "block_number": b,
                "block_timestamp": 1_700_000_000 + b,
                "from": account(1),
                "log_index": 0,
                "to": account(2),
                "value": (100 * b).to_string(),
            })
        })
        .collect()
}

/// The three config files as a 4.0 user writes them. Frozen: a later 4.x must read the same meaning.
#[test]
fn config_written_for_4_0_keeps_its_meaning() {
    let golden = fixture_root().join("config-4.0");
    let nest = golden.join("nest");

    let raw = std::fs::read_to_string(nest.join("nuthatch.toml")).unwrap();
    assert_eq!(
        nuthatch::config::Config::unknown_keys(&raw),
        Vec::<String>::new()
    );
    let cfg = nuthatch::config::Config::load(&nest).unwrap();
    assert_eq!(cfg.nest.name, "tokens");
    assert_eq!(cfg.nest.chain, "arbitrum-one");
    assert_eq!(cfg.nest.chain_id, 42161);
    assert_eq!(
        cfg.nest.rpc_urls,
        ["https://arb1.arbitrum.io/rpc", "https://arbitrum.drpc.org"]
    );
    assert_eq!(cfg.nest.schema_version, 2);
    assert!(cfg.nest.block_timestamps);
    let contracts: Vec<_> = cfg
        .contracts
        .iter()
        .map(|c| {
            (
                c.alias.as_str(),
                c.address.as_str(),
                c.start_block,
                c.abi.as_str(),
                c.events.clone(),
            )
        })
        .collect();
    assert_eq!(
        contracts,
        vec![
            (
                "usdc",
                "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                Some(1000),
                "abis/erc20.json",
                vec!["Transfer".to_string()]
            ),
            (
                "arb",
                "0x912CE59144191C1204E64559FE8253a0e49E6548",
                None,
                "abis/erc20.json",
                vec![]
            ),
        ]
    );
    assert_eq!(cfg.flags.threshold_amount(), Some(1_000_000_000_000));
    assert_eq!(cfg.flags.velocity(), Some((5_000_000_000_000, 7200)));

    let entities = nuthatch::entities::load(&nest).unwrap();
    assert_eq!(entities.len(), 1);
    let e = &entities[0];
    assert_eq!(
        (e.name.as_str(), e.key.clone(), e.max_rows),
        ("received", vec!["to".to_string()], 10_000)
    );
    assert_eq!(
        e.read_sql(&nest).unwrap().trim(),
        "SELECT t.to, SUM(CAST(t.value AS HUGEINT)) AS sum_value FROM usdc__transfer t GROUP BY t.to"
    );
    // The config declares these tables, and the entity binds to them with these output columns.
    let schema = nuthatch::registry::from_nest(&nest, &cfg).unwrap().schema();
    let mut tables: Vec<_> = schema.iter().map(|t| t.table.as_str()).collect();
    tables.sort();
    assert_eq!(tables, ["arb__transfer", "usdc__transfer"]);
    let columns =
        nuthatch::analytics::entity_output_columns(&nest, &schema, &e.read_sql(&nest).unwrap())
            .unwrap();
    assert_eq!(columns, ["to", "sum_value"]);

    let mounts = MountTable::load(&golden).unwrap();
    assert_eq!(mounts.runtime.name, "fleet");
    assert_eq!(mounts.tenant_default(), "ops");
    assert_eq!(mounts.runtime.max_rss_mb, Some(1536));
    assert_eq!(
        mounts
            .chains
            .iter()
            .map(|c| (c.chain.as_str(), c.chain_id, c.rpc_urls.clone()))
            .collect::<Vec<_>>(),
        vec![(
            "arbitrum-one",
            42161,
            vec!["https://arb1.arbitrum.io/rpc".to_string()]
        )]
    );
    let refs: Vec<_> = mounts
        .mount_refs()
        .iter()
        .map(|m| m.route_key(&mounts.tenant_default()))
        .collect();
    assert_eq!(refs, ["tokens", "acme/tokens"]);
    use nuthatch::allowlist::{ParamType, SqlAccess};
    let (ops, acme) = (&mounts.mounts[0], &mounts.mounts[1]);
    assert_eq!((ops.nid.as_str(), ops.sql), (V3_13_2_NID, SqlAccess::Open));
    assert_eq!(
        (acme.nid.as_str(), acme.sql),
        (V3_13_2_NID, SqlAccess::Allowlist)
    );
    assert_eq!(acme.queries.len(), 1);
    let q = &acme.queries[0];
    assert_eq!(q.name, "received_by");
    assert_eq!(
        q.params
            .iter()
            .map(|(k, v)| (k.as_str(), *v))
            .collect::<Vec<_>>(),
        vec![("from", ParamType::Int), ("who", ParamType::Address)]
    );
    assert_eq!(mounts.datasets(&golden).len(), 1);
}
