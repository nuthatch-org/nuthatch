//! RFC-0073 FR4: the HTTP and SQL counters broken down by mount, through the runtime's own router.
//!
//! Its own test target: the unlabelled totals are process-global, and the claim that they equal the
//! sum of the per-mount series only holds in a process that serves nothing else.

mod common;

use std::sync::Arc;

use axum::body::Body;
use nuthatch::store::CoreHotStore as _;
use nuthatch::{health::RuntimeHealth, indexer, serve};
use tower::ServiceExt;

use common::tape::*;

const ALPHA: &str = "org-a/alpha";
const BETA: &str = "org-b/beta";
/// A second door onto alpha's dataset, stamped with alpha's health the way `fan_out_aliases` does.
const MIRROR: &str = "org-b/alpha";

async fn get(app: &axum::Router, uri: &str) -> (u16, String) {
    let req = axum::http::Request::get(uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status().as_u16();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

fn value(text: &str, series: &str) -> u64 {
    text.lines()
        .find(|l| l.rsplit_once(' ').map(|(s, _)| s) == Some(series))
        .and_then(|l| l.rsplit(' ').next()?.parse().ok())
        .unwrap_or_else(|| panic!("no series {series} in:\n{text}"))
}

fn sql(table: &str) -> String {
    format!("/sql?q=SELECT%20COUNT(*)%20AS%20n%20FROM%20%22{table}%22")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_mount_counts_its_own_requests() {
    let root = tempfile::tempdir().unwrap();
    let (a_dir, b_dir) = (root.path().join("alpha"), root.path().join("beta"));
    std::fs::create_dir_all(&a_dir).unwrap();
    std::fs::create_dir_all(&b_dir).unwrap();
    let a_cfg = scaffold_nest(&a_dir, "alpha", USDC);
    let b_cfg = scaffold_nest(&b_dir, "beta", USDC);

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

    let health = Arc::new(RuntimeHealth::new());
    health.register(ALPHA, "arbitrum-one");
    health.register(BETA, "arbitrum-one");
    let mut cursor = indexer::spawn_runtime(
        tape.clone(),
        vec![
            (ALPHA.to_string(), a_dir, a_cfg),
            (BETA.to_string(), b_dir, b_cfg),
        ],
        None,
        false,
        1,
        Some(3),
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
            .all(|(_, s)| s.store.get_meta("last_block").ok().flatten().as_deref() == Some("3"))
    })
    .await;
    assert!(landed, "the nests did not index to the tip in time");

    let mut states = std::mem::take(&mut cursor.states);
    let alpha = states.iter().find(|(n, _)| n == ALPHA).unwrap().1.clone();
    states.push((MIRROR.to_string(), alpha));
    let app = serve::compose_runtime(serde_json::json!({ "nests": [] }), states, health);

    let (a_sql, b_sql) = (sql("alpha__transfer"), sql("beta__transfer"));
    // The second identical query is answered from the memo, which counts it off the executor.
    for _ in 0..2 {
        let (status, body) = get(&app, &format!("/{ALPHA}{a_sql}")).await;
        assert_eq!(status, 200, "{body}");
    }
    let (status, body) = get(&app, &format!("/{BETA}{b_sql}")).await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = get(&app, &format!("/{BETA}/sql?q=SELECT%20*%20FROM%20nope")).await;
    assert_eq!(status, 400);
    let (status, _) = get(&app, &format!("/{MIRROR}/tables")).await;
    assert_eq!(status, 200);

    let (_, text) = get(&app, "/metrics").await;
    let http = |n: &str| {
        value(
            &text,
            &format!("nuthatch_nest_http_requests_total{{nest=\"{n}\"}}"),
        )
    };
    let queries = |n: &str| {
        value(
            &text,
            &format!("nuthatch_nest_sql_queries_total{{nest=\"{n}\"}}"),
        )
    };
    let invalid = |n: &str| {
        value(
            &text,
            &format!("nuthatch_nest_sql_rejections_total{{nest=\"{n}\",reason=\"invalid\"}}"),
        )
    };
    let timed = |n: &str| {
        value(
            &text,
            &format!("nuthatch_nest_sql_duration_seconds_count{{nest=\"{n}\"}}"),
        )
    };

    assert_eq!((http(ALPHA), http(BETA), http(MIRROR)), (2, 2, 1));
    assert_eq!((queries(ALPHA), queries(BETA), queries(MIRROR)), (2, 2, 0));
    assert_eq!((invalid(ALPHA), invalid(BETA), invalid(MIRROR)), (0, 1, 0));
    assert_eq!((timed(ALPHA), timed(BETA), timed(MIRROR)), (2, 2, 0));
    assert_eq!(
        value(
            &text,
            &format!("nuthatch_nest_sql_duration_seconds_bucket{{nest=\"{ALPHA}\",le=\"+Inf\"}}")
        ),
        2
    );

    let mounts = [ALPHA, BETA, MIRROR];
    assert_eq!(
        value(&text, "nuthatch_http_requests_total"),
        mounts.iter().map(|n| http(n)).sum::<u64>()
    );
    assert_eq!(
        value(&text, "nuthatch_sql_queries_total"),
        mounts.iter().map(|n| queries(n)).sum::<u64>()
    );
    assert_eq!(
        value(&text, "nuthatch_sql_rejections_total"),
        mounts.iter().map(|n| invalid(n)).sum::<u64>()
    );

    cursor.shutdown().await.expect("the cursor stops");
}
