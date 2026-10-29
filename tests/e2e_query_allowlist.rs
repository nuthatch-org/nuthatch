//! **A bounded mount answers its declared queries and nothing else** (RFC-0034 phase 1).
//!
//! The unit tests in `allowlist.rs` prove the parameter rendering cannot be injected into. This one
//! proves the *serving* half: that the refusal actually happens on the wire, that it names what can
//! be asked instead of just saying no, and - the assertion that would be easy to miss - that a
//! declared query is still bounded by the node's own guards. An allowlist entry says the operator is
//! willing to answer a query, not that it is cheap.
//!
//! It also pins the thing most likely to regress: **`open` is the default and stays untouched.** A
//! security control that turns itself on is a support ticket, and the local `nuthatch dev` experience
//! is arbitrary `/sql` by design.

mod common;

use std::sync::Arc;

use nuthatch::allowlist::{NamedQuery, ParamType, SqlAccess, Surface};
use nuthatch::serve::AppState;

use common::tape::*;
use tower::ServiceExt;

fn query(name: &str, sql: &str, params: &[(&str, ParamType)]) -> NamedQuery {
    NamedQuery {
        name: name.into(),
        sql: sql.into(),
        params: params.iter().map(|(n, t)| (n.to_string(), *t)).collect(),
    }
}

/// A nest with three indexed blocks, served with `surface`.
async fn serve_with(surface: Surface) -> (AppState, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = scaffold_nest(dir.path(), "usdc", USDC);
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

    let rt = nuthatch::indexer::spawn_nest(
        tape.clone(),
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
    .expect("spawn_nest");

    let landed = wait_until(POLL_TIMEOUT, || {
        use nuthatch::store::HotStore;
        rt.state
            .store
            .get_meta("last_block")
            .ok()
            .flatten()
            .as_deref()
            == Some("3")
    })
    .await;
    assert!(landed, "fixture did not index");
    // `/sql` answers from the published snapshot (#2467), which follows the commit by a moment.
    let snapshots = rt
        .state
        .snapshots
        .clone()
        .expect("a writer publishes snapshots");
    let published = wait_until(POLL_TIMEOUT, || {
        use nuthatch::store::HotStore;
        snapshots
            .load_full()
            .is_some_and(|s| s.store.get_meta("last_block").ok().flatten().as_deref() == Some("3"))
    })
    .await;
    assert!(published, "fixture's last window was not published");
    rt.ingest.abort();

    let mut state = rt.state;
    state.surface = Arc::new(surface);
    (state, dir)
}

/// One GET through the real router, returning `(status, body)`.
async fn get(state: &AppState, path: &str) -> (axum::http::StatusCode, serde_json::Value) {
    let app = nuthatch::serve::router(nuthatch::serve::SharedNest::new(state.clone()));
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .uri(path)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_allowlisted_mount_answers_by_name_and_refuses_free_form() {
    let (state, _dir) = serve_with(Surface {
        access: SqlAccess::Allowlist,
        queries: vec![
            query("row_count", "SELECT count(*) AS n FROM usdc__transfer", &[]),
            query(
                "recent",
                "SELECT block_number FROM usdc__transfer ORDER BY block_number DESC LIMIT {n}",
                &[("n", ParamType::Int)],
            ),
        ],
    })
    .await;

    // The declared query answers.
    let (status, body) = get(&state, "/q/row_count").await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"], 3);
    assert!(
        body.get("provenance").is_some(),
        "a named query must carry the same provenance stamp /sql does: {body}"
    );

    // ...with its parameter applied, not ignored.
    let (status, body) = get(&state, "/q/recent?n=2").await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(
        body["count"], 2,
        "the parameter did not reach the query: {body}"
    );

    // Free-form is refused, and the refusal says what *can* be asked.
    for path in [
        "/sql?q=SELECT%201",
        "/explain?q=SELECT%201",
        "/sql?q=SELECT%20*%20FROM%20usdc__transfer",
    ] {
        let (status, body) = get(&state, path).await;
        assert_eq!(
            status,
            axum::http::StatusCode::FORBIDDEN,
            "{path} should be refused: {body}"
        );
        let allowed = body["allowed_queries"].as_array().expect("named surface");
        assert!(
            allowed.iter().any(|v| v == "row_count"),
            "the refusal must name what can be asked: {body}"
        );
    }

    // An undeclared name is a 404 that also names the surface.
    let (status, body) = get(&state, "/q/definitely_not_declared").await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
    assert!(
        body["allowed_queries"].as_array().unwrap().len() == 2,
        "{body}"
    );

    // A bad argument is refused before any SQL runs, and the error says what to send.
    let (status, body) = get(&state, "/q/recent?n=1;DROP%20TABLE%20t").await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("n"),
        "the error must name the parameter: {body}"
    );

    // `/queries` is the discovery route, and it works on a bounded nest without being refused.
    let (status, body) = get(&state, "/queries").await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert_eq!(body["sql"], "allowlist");
    assert_eq!(body["free_form"], false);
    assert_eq!(body["queries"].as_array().unwrap().len(), 2);
    assert_eq!(body["queries"][1]["params"], "n: int");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deny_refuses_all_sql_but_leaves_the_typed_routes_alone() {
    let (state, _dir) = serve_with(Surface {
        access: SqlAccess::Deny,
        queries: vec![],
    })
    .await;

    for path in ["/sql?q=SELECT%201", "/explain?q=SELECT%201"] {
        let (status, body) = get(&state, path).await;
        assert_eq!(status, axum::http::StatusCode::FORBIDDEN, "{path}");
        assert!(
            body["allowed_queries"].is_null(),
            "deny has no allowed queries to name: {body}"
        );
        assert!(
            body["hint"].as_str().unwrap().contains("/tables"),
            "the refusal should point at what still works: {body}"
        );
    }

    // The typed surface is untouched - denying SQL is not denying the nest.
    for path in ["/tables", "/health", "/nest"] {
        let (status, _) = get(&state, path).await;
        assert!(
            status.is_success(),
            "{path} must still serve when SQL is denied, got {status}"
        );
    }
}

/// The regression that would hurt most: a control that turns itself on. A local `nuthatch dev` has no
/// mount record, so it has no surface, so `/sql` behaves exactly as it always has.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_is_open_and_unchanged() {
    let (state, _dir) = serve_with(Surface::default()).await;
    assert_eq!(state.surface.access, SqlAccess::Open);

    let (status, body) = get(
        &state,
        "/sql?q=SELECT%20count(*)%20AS%20n%20FROM%20usdc__transfer",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"], 3);

    let (status, _) = get(&state, "/explain?q=SELECT%201").await;
    assert!(status.is_success());

    let (_, body) = get(&state, "/queries").await;
    assert_eq!(body["sql"], "open");
    assert_eq!(body["free_form"], true);
}

/// A declared query is not a trusted one. It runs through the same guards as free-form SQL, so a
/// large entry on the allowlist is still capped rather than served whole.
///
/// The row cap is asserted rather than the timeout deliberately: proving the timeout means waiting
/// 30 s of real CI time to learn the same fact - that the guard object reaches the query - and a
/// 30-second test is one people start skipping.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_query_is_still_subject_to_the_node_guards() {
    let (state, _dir) = serve_with(Surface {
        access: SqlAccess::Allowlist,
        queries: vec![query(
            "firehose",
            // Far more rows than SQL_MAX_ROWS (50,000): syntactically fine, declared by the operator,
            // and exactly what the cap exists for.
            "SELECT i FROM range(200000) t(i)",
            &[],
        )],
    })
    .await;

    let (status, body) = get(&state, "/q/firehose").await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(
        body["truncated"], true,
        "a declared query must still hit the row cap: {body}"
    );
    assert!(
        body["count"].as_u64().unwrap() <= 50_000,
        "the cap was not applied to a declared query: {}",
        body["count"]
    );
}
