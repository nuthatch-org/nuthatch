//! RFC-0062 criterion 3, end to end: a maintained view answers exactly as its definition does at
//! every head through a reorg, and no copy file is ever modified.
//!
//! Two nests follow one scripted chain: one declares the views maintained, the other declares
//! nothing and is the oracle. At every head both answer every shape, and the maintained nest's
//! answer must be the oracle's to the byte, both on the first request after the head lands and once
//! that request is served from a copy. A copy is never edited; one served at inputs it was not built
//! from would show here as a stale answer.
//!
//! Its own target: the release identity hashes the test executable once per process, which would
//! slow its neighbours in the shared `it` binary.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use nuthatch::indexer;

use common::tape::*;

const VIEWS: &str = r#"
CREATE VIEW received AS
SELECT t."to" AS account, count(*) AS transfers, sum(t.value_dec) AS total,
       max(t.block_number) AS last_block
FROM usdc__transfer t
GROUP BY t."to";

CREATE VIEW transfer_seen AS
SELECT t.block_number, t.log_index, t."from", t."to", t.value_dec, r.total AS to_total,
       row_number() OVER (PARTITION BY t."to" ORDER BY t.block_number, t.log_index) AS nth
FROM usdc__transfer t
LEFT JOIN received r ON r.account = t."to";
"#;

const DECLARATION: &str = "[[view]]\nname = \"received\"\n\n[[view]]\nname = \"transfer_seen\"\n";

const SHAPES: &[&str] = &[
    "SELECT * FROM received ORDER BY account",
    "SELECT * FROM transfer_seen WHERE value_dec > 0 ORDER BY block_number, log_index",
    "SELECT count(*) AS n, sum(nth) AS ranks, max(to_total) AS biggest FROM transfer_seen",
    "SELECT account, total FROM received WHERE transfers > 1 ORDER BY total DESC, account",
    "SELECT s.block_number, s.log_index, r.transfers FROM transfer_seen s \
     JOIN received r ON r.account = s.\"from\" WHERE s.value_dec > 0 ORDER BY 1, 2",
];

fn ts(b: u64) -> u64 {
    1_700_000_000 + b
}

fn canonical(b: u64) -> BlockFixture {
    transfers_block(
        b,
        0,
        ts(b),
        USDC,
        &[
            (account(1).as_str(), account(2).as_str(), (100 * b) as u128),
            (account(2).as_str(), account(5).as_str(), b as u128),
        ],
    )
}

/// Round `r`'s replacement for block `b`: other parties and amounts in `[7000 r, 7000 (r + 1))`, so
/// a copy left over from before the reorg answers differently.
fn replacement(b: u64, r: u64) -> BlockFixture {
    transfers_block(
        b,
        r,
        ts(b) + 500 * r,
        USDC,
        &[(
            account(2 + r).as_str(),
            account(20 + r).as_str(),
            (7_000 * r + b) as u128,
        )],
    )
}

fn scaffold(dir: &Path, maintained: bool) -> nuthatch::config::Config {
    let cfg = scaffold_nest(dir, "usdc", USDC);
    std::fs::create_dir_all(dir.join("views")).unwrap();
    std::fs::write(dir.join("views/10-seen.sql"), VIEWS).unwrap();
    if maintained {
        std::fs::write(dir.join("maintained.toml"), DECLARATION).unwrap();
    }
    cfg
}

async fn spawn(dir: &Path, tape: Arc<TapeSource>, maintained: bool) -> indexer::NestRuntime {
    let cfg = scaffold(dir, maintained);
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

async fn get_json(rt: &indexer::NestRuntime, path: &str) -> (axum::http::StatusCode, Value) {
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

fn encode(sql: &str) -> String {
    let mut out = String::new();
    for b in sql.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The answer's columns and rows, as JSON text: provenance differs between two nests and is not the
/// answer.
async fn answer(rt: &indexer::NestRuntime, sql: &str) -> String {
    let (status, body) = get_json(rt, &format!("/sql?q={}", encode(sql))).await;
    assert_eq!(status, axum::http::StatusCode::OK, "{sql}: {body}");
    serde_json::to_string(&(&body["columns"], &body["rows"])).unwrap()
}

/// Per maintained view: hits, fallbacks, builds.
async fn stats(rt: &indexer::NestRuntime) -> BTreeMap<String, (u64, u64, u64)> {
    let (_, body) = get_json(rt, "/ready").await;
    body["maintained"]
        .as_array()
        .unwrap_or_else(|| panic!("/ready names no maintained views: {body}"))
        .iter()
        .map(|v| {
            let n = |k: &str| v[k].as_u64().unwrap_or_else(|| panic!("{k} in {v}"));
            (
                v["view"].as_str().unwrap().to_string(),
                (n("hits"), n("fallbacks"), n("builds")),
            )
        })
        .collect()
}

fn head(rt: &indexer::NestRuntime) -> Option<u64> {
    rt.state
        .store
        .get_meta("last_block")
        .ok()
        .flatten()
        .and_then(|v| v.parse().ok())
}

async fn eventually<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + SEAL_POLL_TIMEOUT;
    while !cond().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Both nests at `block` with the same sealed watermark, and `sql` answering `want` on both.
async fn settled(
    m: &indexer::NestRuntime,
    p: &indexer::NestRuntime,
    block: u64,
    sql: &str,
    want: &str,
) {
    eventually(
        &format!("both nests at {block} with {sql} = {want}"),
        || async {
            if head(m) != Some(block)
                || head(p) != Some(block)
                || m.state.store.sealed_through() != p.state.store.sealed_through()
            {
                return false;
            }
            let rows = |rt| async move {
                let (_, body) = get_json(rt, &format!("/sql?q={}", encode(sql))).await;
                body["rows"][0]["n"].to_string()
            };
            rows(m).await == want && rows(p).await == want
        },
    )
    .await;
}

/// Every copy on disk, by path, with its bytes.
fn copy_files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let Ok(views) = std::fs::read_dir(dir.join("maintained")) else {
        return out;
    };
    for view in views.flatten() {
        for f in std::fs::read_dir(view.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            let path = f.path();
            if path.extension().is_some_and(|x| x == "parquet") {
                if let Ok(bytes) = std::fs::read(&path) {
                    out.insert(path, bytes);
                }
            }
        }
    }
    out
}

/// At this head: every shape's first answer is the oracle's, then every shape is answered from a
/// copy and is still the oracle's, and no copy seen so far has changed.
async fn check_head(
    m: &indexer::NestRuntime,
    p: &indexer::NestRuntime,
    label: &str,
    seen: &mut BTreeMap<PathBuf, Vec<u8>>,
) {
    for sql in SHAPES {
        assert_eq!(
            answer(m, sql).await,
            answer(p, sql).await,
            "{label}, first request: {sql}"
        );
    }
    eventually(
        &format!("{label}: every shape served from a copy"),
        || async {
            let before = stats(m).await;
            for sql in SHAPES {
                assert_eq!(answer(m, sql).await, answer(p, sql).await, "{label}: {sql}");
            }
            let after = stats(m).await;
            before
                .iter()
                .all(|(view, (_, fallbacks, _))| after[view].1 == *fallbacks)
        },
    )
    .await;
    for (path, bytes) in copy_files(&m.state.dir) {
        match seen.get(&path) {
            Some(was) => assert!(*was == bytes, "{label}: {} was modified", path.display()),
            None => {
                seen.insert(path, bytes);
            }
        }
    }
}

fn shutdown(rt: indexer::NestRuntime) {
    rt.ingest.abort();
    if let Some(w) = rt.alert_worker {
        w.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_maintained_view_is_its_definition_at_every_head_through_reorgs() {
    // A remembered answer counts neither a hit nor a fallback, so with the memo on `check_head`
    // settled before any copy was built (#2265). This target runs this one test alone.
    std::env::set_var(nuthatch::sqlmemo::ENV_MAX_BYTES, "0");
    let tape = Arc::new(TapeSource::new());
    let mut head_block = 8;
    for b in 1..=head_block {
        let mut fx = canonical(b);
        if b == 2 {
            // Rows enough for the tip path to seal 1..=2 once it is finalized.
            pad_address_to_seal(&mut fx, USDC, 2);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(head_block);
    let (m_dir, p_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let m = spawn(m_dir.path(), tape.clone(), true).await;
    let p = spawn(p_dir.path(), tape.clone(), false).await;
    let all = "SELECT count(*) AS n FROM usdc__transfer WHERE value_dec > 0";
    settled(&m, &p, head_block, all, &(2 * head_block).to_string()).await;
    let mut seen = BTreeMap::new();
    check_head(&m, &p, "before any reorg", &mut seen).await;

    // A seal moves the views' inputs from hot to sealed: the copy is rebuilt with no request asking.
    // Copies by file, not the build counter: a build is counted only after its copy is served.
    let before_seal = copy_files(&m.state.dir);
    tape.advance_finalized_to(2);
    head_block += 1;
    tape.insert_block(head_block, empty_block(head_block, 0, ts(head_block)));
    tape.advance_tip_to(head_block);
    eventually("the seal on both nests", || async {
        head(&m) == Some(head_block)
            && head(&p) == Some(head_block)
            && m.state.store.sealed_through() >= 2
            && m.state.store.sealed_through() == p.state.store.sealed_through()
    })
    .await;
    let views: Vec<String> = stats(&m).await.into_keys().collect();
    eventually("a build of each view after the seal", || async {
        let now = copy_files(&m.state.dir);
        views.iter().all(|view| {
            now.keys().any(|path| {
                !before_seal.contains_key(path)
                    && path.parent().is_some_and(|d| d.ends_with(view.as_str()))
            })
        })
    })
    .await;
    let before_asking = stats(&m).await;
    check_head(&m, &p, "after the seal", &mut seen).await;
    let after_asking = stats(&m).await;
    assert!(
        before_asking
            .iter()
            .all(|(v, (_, fallbacks, _))| after_asking[v].1 == *fallbacks),
        "the first request after the seal found its copy: {before_asking:?} then {after_asking:?}"
    );

    for (round, depth) in [1u64, 3, 5].into_iter().enumerate() {
        let r = round as u64 + 1;
        let fork = head_block - depth;
        assert!(
            fork >= m.state.store.sealed_through(),
            "a reorg stays above the seal"
        );
        tape.reorg(
            fork,
            ((fork + 1)..=head_block)
                .map(|b| replacement(b, r))
                .collect(),
        );
        let replaced = format!(
            "SELECT count(*) AS n FROM usdc__transfer WHERE block_number > {fork} \
             AND value_dec >= {} AND value_dec < {}",
            7_000 * r,
            7_000 * (r + 1)
        );
        settled(&m, &p, head_block, &replaced, &depth.to_string()).await;
        check_head(
            &m,
            &p,
            &format!("reorged {depth} deep at {fork}"),
            &mut seen,
        )
        .await;

        head_block += 1;
        tape.insert_block(head_block, replacement(head_block, r));
        tape.advance_tip_to(head_block);
        settled(&m, &p, head_block, &replaced, &(depth + 1).to_string()).await;
        check_head(&m, &p, &format!("past the {depth}-deep reorg"), &mut seen).await;
    }

    assert!(
        seen.len() >= 8,
        "copies were built at each state: {:?}",
        seen.keys().collect::<Vec<_>>()
    );
    eprintln!(
        "{} copies seen, none modified; per view (hits, fallbacks, builds): {:?}",
        seen.len(),
        stats(&m).await
    );
    shutdown(m);
    shutdown(p);
}
