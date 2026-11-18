//! `--cors` is almost entirely wiring, so this tests the wiring (#1318).
//!
//! The unit tests in `serve.rs` apply the layer themselves and then assert on the response, which
//! proves [`nuthatch::serve::cors_layer`] builds a correct layer and proves nothing about whether
//! anything ever *uses* it. That is the same shape as the three fixtures which proved a handler and
//! never the composition, so the one assertion that matters here goes through
//! [`nuthatch::serve::bind_and_serve`] over a real socket, which is the single place every serving
//! path - solo `dev`, `serve`, the two-version endpoint, and a mounts runtime - actually binds.

use std::time::Duration;

use axum::{routing::get, Router};

/// A port nothing is listening on, found by binding and immediately releasing. There is a window
/// between the release and the rebind, which is why the assertions below tolerate the connection
/// not being up yet rather than assuming it is.
async fn free_addr() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr.to_string()
}

/// Serve `app` through the real bind with `origins` configured, and return the response headers for
/// one cross-origin GET.
async fn headers_through_the_bind(origins: &[&str]) -> axum::http::HeaderMap {
    let owned: Vec<String> = origins.iter().map(|s| s.to_string()).collect();
    let cors = nuthatch::serve::cors_layer(&owned).expect("valid origins");
    let addr = free_addr().await;
    let app = Router::new().route("/health", get(|| async { "ok" }));

    let serving = {
        let addr = addr.clone();
        tokio::spawn(async move {
            nuthatch::serve::bind_and_serve(&addr, app, cors, Default::default()).await
        })
    };

    // Bounded polling rather than a fixed sleep: the bind races this task's first request.
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/health");
    let started = std::time::Instant::now();
    let resp = loop {
        match client
            .get(&url)
            .header("origin", "https://app.example.com")
            .send()
            .await
        {
            Ok(r) => break r,
            Err(e) if started.elapsed() < Duration::from_secs(10) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = e;
            }
            Err(e) => panic!("never came up on {addr}: {e}"),
        }
    };
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let headers = resp.headers().clone();
    serving.abort();
    headers
}

/// The whole feature, at the only seam that can make it real: a `--cors` value reaches the bind and
/// the response carries the header. Mutate the layer out of `bind_and_serve` and this is the test
/// that notices.
#[tokio::test]
async fn a_configured_origin_reaches_the_response_through_the_real_bind() {
    let headers = headers_through_the_bind(&["https://app.example.com"]).await;
    assert_eq!(
        headers
            .get("access-control-allow-origin")
            .map(|v| v.to_str().unwrap()),
        Some("https://app.example.com"),
        "the layer never reached the bind: {headers:?}"
    );
}

/// And the other half of the same seam - the half a self-hoster who never passed the flag lives on.
/// Without `--cors` the bind composes no layer at all, so the response is the one it always was.
#[tokio::test]
async fn no_flag_means_no_header_through_the_real_bind() {
    let headers = headers_through_the_bind(&[]).await;
    assert!(
        headers.get("access-control-allow-origin").is_none(),
        "a nest with no --cors answered a cross-origin request with {headers:?}"
    );
}
