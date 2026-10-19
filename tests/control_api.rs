//! RFC-0022 §Testing, **dynamic lifecycle**: "API add/remove a nest with no restart and no impact on
//! other nests' cursors."
//!
//! Drives the router directly with `tower::ServiceExt::oneshot` rather than binding a port - the
//! interesting behaviour is status codes and effects on desired state, and a real socket would add
//! flakiness without adding coverage.

#![cfg(feature = "postgres-store")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use nuthatch::control_api::routes;
use nuthatch::controlplane::ControlPlane;
use tower::ServiceExt;

fn fixture(token: Option<&str>) -> Option<(axum::Router, Arc<ControlPlane>)> {
    let url = match std::env::var("NUTHATCH_TEST_PG") {
        Ok(u) => u,
        Err(_) if std::env::var("NUTHATCH_REQUIRE_PG").is_ok() => {
            panic!("NUTHATCH_REQUIRE_PG is set but NUTHATCH_TEST_PG is not")
        }
        Err(_) => {
            eprintln!("SKIPPED: set NUTHATCH_TEST_PG to run the control-API suite");
            return None;
        }
    };
    let cp = Arc::new(ControlPlane::connect(&url).expect("connect"));
    for n in cp.desired().unwrap() {
        cp.undeclare_nest(&n.name).unwrap();
    }
    for w in cp.live_workers(86_400).unwrap() {
        cp.deregister(&w.id).unwrap();
    }
    Some((routes(cp.clone(), token.map(|t| t.to_string())), cp))
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let req = Request::builder().method(method).uri(uri);
    let req = match body {
        Some(b) => req
            .header("content-type", "application/json")
            .body(Body::from(b.to_string()))
            .unwrap(),
        None => req.body(Body::empty()).unwrap(),
    };
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// The lifecycle the RFC asks for: declare, see it, remove it, see it gone. No restarts anywhere.
#[tokio::test]
async fn a_nest_can_be_declared_and_removed_over_http() {
    let Some((app, cp)) = fixture(None) else {
        return;
    };

    let (s, _) = call(
        &app,
        "POST",
        "/nests",
        Some(r#"{"name":"usdc","chain":"mainnet","estimated_rss_mb":120}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = call(&app, "GET", "/nests", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["nests"][0]["name"], "usdc");
    assert_eq!(body["nests"][0]["estimated_rss_mb"], 120);

    let (s, _) = call(&app, "DELETE", "/nests/usdc", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(cp.desired().unwrap().is_empty(), "and the effect is real");
}

/// Removing something that was never there is a 404, not a cheerful 200. An operator who typo'd a
/// name has to find out, and the only place they will look is the status code.
#[tokio::test]
async fn removing_an_unknown_nest_is_a_404() {
    let Some((app, _)) = fixture(None) else {
        return;
    };
    let (s, body) = call(&app, "DELETE", "/nests/never-existed", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert!(body["error"].as_str().unwrap().contains("never-existed"));
}

/// A missing estimate must not cost nothing. Zero would make a cursor look free and let the
/// scheduler overcommit a worker - the exact failure the budget exists to prevent.
#[tokio::test]
async fn an_unestimated_nest_is_costed_as_small_not_free() {
    let Some((app, _)) = fixture(None) else {
        return;
    };
    let (s, body) = call(
        &app,
        "POST",
        "/nests",
        Some(r#"{"name":"guess","chain":"mainnet"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        body["estimated_rss_mb"].as_u64().unwrap() > 0,
        "an unestimated nest must still carry weight"
    );
}

/// Bad input is the caller's fault, and must read as such.
#[tokio::test]
async fn an_invalid_nest_is_a_400_not_a_500() {
    let Some((app, _)) = fixture(None) else {
        return;
    };
    let (s, _) = call(
        &app,
        "POST",
        "/nests",
        Some(r#"{"name":"","chain":"mainnet"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

/// `GET /plan` answers "why is my nest not running?" directly, which is the question an operator
/// actually asks. A bare absence of rows would leave them guessing.
#[tokio::test]
async fn the_plan_endpoint_explains_why_something_cannot_run() {
    let Some((app, cp)) = fixture(None) else {
        return;
    };
    cp.declare_nest(&nuthatch::scheduler::DesiredNest {
        name: "huge".into(),
        chain: "mainnet".into(),
        estimated_rss_mb: 100_000,
    })
    .unwrap();
    cp.heartbeat("w1", 2048).unwrap();

    let (s, body) = call(&app, "GET", "/plan", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(body["assign"].as_array().unwrap().is_empty());
    let u = &body["unplaceable"][0];
    assert_eq!(u["chain"], "mainnet");
    assert!(
        u["detail"]
            .as_str()
            .unwrap()
            .contains("adding workers will not help"),
        "the detail must distinguish 'too big for anyone' from 'full right now': {u}"
    );
}

#[tokio::test]
async fn workers_are_listed_with_their_budgets() {
    let Some((app, cp)) = fixture(None) else {
        return;
    };
    cp.heartbeat("w1", 2048).unwrap();
    cp.heartbeat("w2", 8192).unwrap();

    let (s, body) = call(&app, "GET", "/workers", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["count"], 2);
    assert_eq!(body["workers"][1]["budget_mb"], 8192);
}

/// With a token configured, **every** state-changing and state-revealing route demands it. A control
/// plane that authenticated writes but leaked the fleet's shape would still be a disclosure.
///
/// The list below is every non-`/health` entry in [`routes`], method by method. It previously covered
/// five of the ten, omitting `resolve`, `pin` and **both halves of the secrets API** - the most
/// sensitive routes on the surface - which left the guard on `put_secret` deletable with the suite
/// green (#292).
///
/// Completing the list closed that hole; it did not close the *class*, because the list still had to
/// be kept in step by hand. The guard is now one `route_layer` over the whole API instead of four
/// lines in each of ten handlers, so a new route is guarded by being registered rather than by being
/// remembered here. What this test now pins is that the layer is mounted and that both credential
/// forms reach it - a weaker obligation, deliberately, because the strong one moved into the router.
#[tokio::test]
async fn every_route_demands_the_token_when_one_is_set() {
    let Some((app, _)) = fixture(Some("s3cret")) else {
        return;
    };
    for (method, uri, body) in [
        ("GET", "/nests", None),
        ("GET", "/workers", None),
        ("GET", "/plan", None),
        ("DELETE", "/nests/x", None),
        ("POST", "/nests", Some(r#"{"name":"a","chain":"c"}"#)),
        ("GET", "/nests/x/resolve", None),
        // Bodies have to deserialize: axum runs the `Json` extractor before the handler, so a
        // malformed one would 422 and the assertion would pass for the wrong reason.
        (
            "PUT",
            "/nests/x/pin",
            Some(r#"{"version":"1","bundle_hash":"0xabc"}"#),
        ),
        ("GET", "/nests/x/secrets", None),
        (
            "PUT",
            "/nests/x/secrets",
            Some(r#"{"key":"k","value":"v"}"#),
        ),
        ("DELETE", "/nests/x/secrets/k", None),
    ] {
        let (s, _) = call(&app, method, uri, body).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED, "{method} {uri} was unguarded");
    }

    // And it works with the token, by query string or bearer header.
    let (s, _) = call(&app, "GET", "/nests?token=s3cret", None).await;
    assert_eq!(s, StatusCode::OK);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/nests")
                .header("authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// A credential that satisfies the guard reaches a handler that takes a **body**, and the body
/// survives the trip.
///
/// The guard used to run inside each handler, after axum had already extracted the request. It is now
/// a layer in front of them, which is the one thing that shape can get wrong: a middleware that
/// buffers or consumes the body would leave every write endpoint failing to deserialize while the
/// 401 assertions above stayed green. Proven on a secrets write, which is both the most sensitive
/// route and the one whose guard #292 found deletable.
#[tokio::test]
async fn an_authorised_write_reaches_its_handler_with_its_body_intact() {
    let Some((app, cp)) = fixture(Some("s3cret")) else {
        return;
    };
    let (s, _) = call(
        &app,
        "POST",
        "/nests?token=s3cret",
        Some(r#"{"name":"n1","chain":"mainnet"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let (s, body) = call(
        &app,
        "PUT",
        "/nests/n1/secrets?token=s3cret",
        Some(r#"{"key":"RPC_URL","value":"https://example.invalid"}"#),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "the guard swallowed the body: {body}");
    assert_eq!(body["key"], "RPC_URL");
    assert_eq!(
        cp.secret_keys("n1").unwrap(),
        vec!["RPC_URL".to_string()],
        "a 200 that stored nothing would be the same bug one layer down"
    );
}

/// A wrong token is refused - the guard compares, it does not merely check for presence.
#[tokio::test]
async fn a_wrong_token_is_refused() {
    let Some((app, _)) = fixture(Some("s3cret")) else {
        return;
    };
    let (s, _) = call(&app, "GET", "/nests?token=guess", None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

/// Health is unauthenticated on purpose: a load balancer must be able to probe it, and it reveals
/// nothing about the fleet.
#[tokio::test]
async fn health_needs_no_token() {
    let Some((app, _)) = fixture(Some("s3cret")) else {
        return;
    };
    let res = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// Undeclaring a nest takes its secrets with it.
///
/// A deleted nest that leaves credentials behind is a live exposure, not untidiness: the rows are
/// unreachable through any surface that lists nests, so nobody sees them again, and the next nest
/// declared under the same name silently inherits them.
///
/// This is the root of #426, which surfaced one layer up as a test-fixture defect - the fixture
/// cleans up by undeclaring every nest, so a `put_secret` mutated to write nothing still passed
/// against the rows a previous run had orphaned. Fixing the fixture would have hidden this.
#[tokio::test]
async fn undeclaring_a_nest_removes_its_secrets() {
    let Some((_app, cp)) = fixture(None) else {
        return;
    };
    cp.declare_nest(&nuthatch::scheduler::DesiredNest {
        name: "ghost".into(),
        chain: "mainnet".into(),
        estimated_rss_mb: 64,
    })
    .unwrap();
    cp.set_secret("ghost", "RPC_URL", "https://example.invalid/k/SUPERSECRET")
        .unwrap();
    assert_eq!(
        cp.secret_keys("ghost").unwrap(),
        vec!["RPC_URL".to_string()],
        "fixture must actually store a secret, or this test proves nothing"
    );

    cp.undeclare_nest("ghost").unwrap();

    assert!(
        cp.secret_keys("ghost").unwrap().is_empty(),
        "a nest's secrets must not outlive the nest - they are unreachable and inheritable"
    );
}

/// #2141: the control plane's own listener drops a client dribbling its request headers, as the
/// data surface does (#1660). Driven through `run`, because the bound lives in how it serves.
#[tokio::test]
async fn the_control_listener_drops_a_client_dribbling_its_headers() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let Some((_app, _cp)) = fixture(None) else {
        return;
    };
    let url = std::env::var("NUTHATCH_TEST_PG").unwrap();
    let addr = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().to_string()
    };
    let server = {
        let addr = addr.clone();
        tokio::spawn(async move { nuthatch::control_api::run(&addr, &url).await })
    };
    let started = std::time::Instant::now();
    while reqwest::get(format!("http://{addr}/health")).await.is_err() {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "never came up on {addr}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let (mut read, mut write) = tokio::net::TcpStream::connect(&addr)
        .await
        .unwrap()
        .into_split();
    write
        .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n")
        .await
        .unwrap();
    let dribble = tokio::spawn(async move {
        while write.write_all(b"x").await.is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });
    let bound = nuthatch::serve::HEADER_READ_TIMEOUT + std::time::Duration::from_secs(5);
    let closed = tokio::time::timeout(bound, async {
        let mut buf = [0u8; 1024];
        while let Ok(n) = read.read(&mut buf).await {
            if n == 0 {
                break;
            }
        }
    })
    .await
    .is_ok();
    dribble.abort();
    server.abort();
    assert!(
        closed,
        "a connection dribbling its headers was held past {bound:?}"
    );
}
