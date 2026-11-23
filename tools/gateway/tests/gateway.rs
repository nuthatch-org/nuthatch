//! The gateway end to end: a stub nuthatch behind it, a JWKS server beside it, and tokens signed
//! here with keys made here. Every refusal named in #3094 and the allow path, over HTTP and MCP.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use nuthatch_gateway::{Config, Gateway};
use ring::rand::SystemRandom;
use ring::signature::{EcdsaKeyPair, Ed25519KeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use serde_json::{json, Value};

/// What the stub nuthatch was asked: method, path and query, whether a token reached it, body.
#[derive(Debug, Clone)]
struct Seen {
    target: String,
    authorization: bool,
    body: Vec<u8>,
}

type Log = Arc<Mutex<Vec<Seen>>>;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn stub_nuthatch() -> (String, Log) {
    let log: Log = Default::default();
    let seen = log.clone();
    let app = axum::Router::new().fallback(move |req: Request| {
        let seen = seen.clone();
        async move {
            let target = req.uri().path_and_query().unwrap().to_string();
            let authorization = req.headers().contains_key("authorization");
            let body = axum::body::to_bytes(req.into_body(), 1 << 20)
                .await
                .unwrap();
            seen.lock().unwrap().push(Seen {
                target,
                authorization,
                body: body.to_vec(),
            });
            axum::Json(json!({ "upstream": true }))
        }
    });
    (serve(app).await, log)
}

struct Signer {
    es: EcdsaKeyPair,
    ed: Ed25519KeyPair,
    /// Same kid as `es`, a different key: a forged signature.
    forger: EcdsaKeyPair,
}

impl Signer {
    fn new() -> Signer {
        let rng = SystemRandom::new();
        let es = |rng: &SystemRandom| {
            let doc = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, rng).unwrap();
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, doc.as_ref(), rng).unwrap()
        };
        let ed = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        Signer {
            es: es(&rng),
            ed: Ed25519KeyPair::from_pkcs8(ed.as_ref()).unwrap(),
            forger: es(&rng),
        }
    }

    fn jwks(&self) -> Value {
        let point = self.es.public_key().as_ref();
        json!({ "keys": [
            { "kty": "EC", "crv": "P-256", "kid": "es", "alg": "ES256", "use": "sig",
              "x": URL_SAFE_NO_PAD.encode(&point[1..33]), "y": URL_SAFE_NO_PAD.encode(&point[33..]) },
            { "kty": "OKP", "crv": "Ed25519", "kid": "ed",
              "x": URL_SAFE_NO_PAD.encode(self.ed.public_key().as_ref()) },
            // An encryption key, as IdPs publish beside their signing keys: skipped.
            { "kty": "RSA", "use": "enc", "kid": "enc", "n": "AQAB", "e": "AQAB" },
        ]})
    }

    /// A token signed by the key `kid` names. `forged` signs with another key but claims `es`.
    fn sign(&self, kid: &str, claims: Value) -> String {
        let alg = if kid == "ed" { "EdDSA" } else { "ES256" };
        let claimed = if kid == "forged" { "es" } else { kid };
        let h =
            URL_SAFE_NO_PAD.encode(json!({ "alg": alg, "kid": claimed, "typ": "JWT" }).to_string());
        let p = URL_SAFE_NO_PAD.encode(claims.to_string());
        let msg = format!("{h}.{p}");
        let rng = SystemRandom::new();
        let sig = match kid {
            "ed" => self.ed.sign(msg.as_bytes()).as_ref().to_vec(),
            "forged" => self
                .forger
                .sign(&rng, msg.as_bytes())
                .unwrap()
                .as_ref()
                .to_vec(),
            _ => self
                .es
                .sign(&rng, msg.as_bytes())
                .unwrap()
                .as_ref()
                .to_vec(),
        };
        format!("{msg}.{}", URL_SAFE_NO_PAD.encode(sig))
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn claims(scope: &str) -> Value {
    json!({ "iss": "https://idp.test", "aud": ["nuthatch", "other"], "sub": "team-a",
            "exp": now() + 300, "iat": now(), "scope": scope })
}

struct Harness {
    base: String,
    seen: Log,
    signer: Signer,
    jwks_fetches: Arc<AtomicUsize>,
    http: reqwest::Client,
}

impl Harness {
    async fn new() -> Harness {
        let (upstream, seen) = stub_nuthatch().await;
        let signer = Signer::new();
        let jwks = signer.jwks();
        let jwks_fetches = Arc::new(AtomicUsize::new(0));
        let fetches = jwks_fetches.clone();
        let idp = serve(axum::Router::new().route(
            "/jwks.json",
            axum::routing::get(move || {
                fetches.fetch_add(1, Ordering::SeqCst);
                let jwks = jwks.clone();
                async move { axum::Json(jwks) }
            }),
        ))
        .await;
        let cfg = Config::parse(&format!(
            r#"
            listen = "127.0.0.1:0"
            upstream = "{upstream}"
            issuer = "https://idp.test"
            audience = "nuthatch"
            leeway_secs = 0
            jwks_url = "{idp}/jwks.json"
            [[nest]]
            name = "usdc"
            prefix = "/usdc"
            [[nest]]
            name = "acme-dai"
            prefix = "/acme/dai"
            "#
        ))
        .unwrap();
        let base = serve(Gateway::new(cfg).await.unwrap().router()).await;
        Harness {
            base,
            seen,
            signer,
            jwks_fetches,
            http: reqwest::Client::new(),
        }
    }

    async fn get(&self, path: &str, token: Option<&str>) -> (u16, Value, Option<String>) {
        let mut req = self.http.get(format!("{}{path}", self.base));
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        answer(req.send().await.unwrap()).await
    }

    async fn mcp(
        &self,
        path: &str,
        token: Option<&str>,
        msg: Value,
    ) -> (u16, Value, Option<String>) {
        let mut req = self
            .http
            .post(format!("{}{path}", self.base))
            .header("content-type", "application/json")
            .body(msg.to_string());
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        answer(req.send().await.unwrap()).await
    }

    fn forwarded(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

async fn answer(resp: reqwest::Response) -> (u16, Value, Option<String>) {
    let status = resp.status().as_u16();
    let challenge = resp
        .headers()
        .get("www-authenticate")
        .map(|v| v.to_str().unwrap().to_string());
    (status, resp.json().await.unwrap_or(Value::Null), challenge)
}

#[tokio::test]
async fn a_nest_scope_reads_the_whole_nest_and_the_token_stops_at_the_gateway() {
    let h = Harness::new().await;
    let token = h.signer.sign("es", claims("openid read:usdc"));
    for path in [
        "/usdc/sql?q=SELECT%201",
        "/usdc/table/usdc__transfer?limit=5",
        "/usdc/q/top",
        "/usdc",
        "/usdc/tables",
    ] {
        let (status, body, _) = h.get(path, Some(&token)).await;
        assert_eq!(
            (status, &body["upstream"]),
            (200, &json!(true)),
            "{path}: {body}"
        );
    }
    let seen = h.seen.lock().unwrap().clone();
    assert_eq!(seen[0].target, "/usdc/sql?q=SELECT%201");
    assert!(
        seen.iter().all(|s| !s.authorization),
        "the bearer token reached nuthatch"
    );
    // EdDSA verifies too.
    let ed = h.signer.sign("ed", claims("read:usdc"));
    assert_eq!(h.get("/usdc/sql?q=SELECT%201", Some(&ed)).await.0, 200);
}

#[tokio::test]
async fn a_table_scope_reads_that_table_and_nothing_else() {
    let h = Harness::new().await;
    let token = h.signer.sign(
        "es",
        claims("read:usdc/table/usdc__transfer read:usdc/q/top"),
    );
    assert_eq!(
        h.get("/usdc/table/usdc__transfer", Some(&token)).await.0,
        200
    );
    assert_eq!(h.get("/usdc/q/top?to=0xabc", Some(&token)).await.0, 200);
    for path in [
        "/usdc/table/usdc__approval",
        "/usdc/sql?q=SELECT%20*%20FROM%20usdc__transfer",
        "/usdc/explain?q=SELECT%201",
        "/usdc/q/other",
        "/usdc/tables",
        "/acme/dai/table/usdc__transfer",
    ] {
        let (status, body, challenge) = h.get(path, Some(&token)).await;
        assert_eq!(
            (status, body["error"].as_str()),
            (403, Some("scope_missing")),
            "{path}"
        );
        assert!(challenge.unwrap().contains("insufficient_scope"));
    }
    assert_eq!(h.forwarded(), 2, "a refused request reached nuthatch");
}

#[tokio::test]
async fn missing_expired_and_invalid_tokens_are_refused_by_name() {
    let h = Harness::new().await;
    let path = "/usdc/sql?q=SELECT%201";
    let (status, body, challenge) = h.get(path, None).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (401, Some("token_missing"))
    );
    assert_eq!(challenge.as_deref(), Some(r#"Bearer realm="nuthatch""#));

    let with = |f: &dyn Fn(&mut Value)| {
        let mut c = claims("read:usdc");
        f(&mut c);
        h.signer.sign("es", c)
    };
    let cases = [
        (with(&|c| c["exp"] = json!(now() - 1)), "token_expired"),
        (
            with(&|c| c["nbf"] = json!(now() + 600)),
            "token_not_yet_valid",
        ),
        (
            with(&|c| {
                c.as_object_mut().unwrap().remove("exp");
            }),
            "token_without_expiry",
        ),
        (
            with(&|c| c["aud"] = json!("someone-else")),
            "token_wrong_audience",
        ),
        (
            with(&|c| {
                c.as_object_mut().unwrap().remove("aud");
            }),
            "token_wrong_audience",
        ),
        (
            with(&|c| c["iss"] = json!("https://evil.test")),
            "token_wrong_issuer",
        ),
        (
            h.signer.sign("forged", claims("read:usdc")),
            "token_signature_invalid",
        ),
        (
            h.signer.sign("rotated-away", claims("read:usdc")),
            "token_key_unknown",
        ),
        ("not.a.jwt".to_string(), "token_malformed"),
    ];
    for (token, code) in cases {
        let (status, body, challenge) = h.get(path, Some(&token)).await;
        assert_eq!(
            (status, body["error"].as_str()),
            (401, Some(code)),
            "{body}"
        );
        assert!(challenge
            .unwrap()
            .contains(&format!(r#"error_description="{code}""#)));
    }
    let basic = h
        .http
        .get(format!("{}{path}", h.base))
        .header("authorization", "Basic dXNlcjpwYXNz")
        .send()
        .await
        .unwrap();
    assert_eq!(basic.status(), 401);
    assert_eq!(h.forwarded(), 0, "a refused request reached nuthatch");
    // The keys came from jwks_url, once: the unknown kid was inside the re-fetch interval.
    assert_eq!(h.jwks_fetches.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn mcp_calls_are_held_to_the_scope_of_what_they_read() {
    let h = Harness::new().await;
    let table = h.signer.sign("es", claims("read:usdc/table/t"));
    let call = |id: i64, tool: &str, args: Value| json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call", "params": { "name": tool, "arguments": args } });
    let list = json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" });
    assert_eq!(h.mcp("/usdc/mcp", Some(&table), list.clone()).await.0, 200);
    assert_eq!(
        h.mcp(
            "/usdc/mcp",
            Some(&table),
            call(2, "table", json!({ "name": "t" }))
        )
        .await
        .0,
        200
    );
    for (id, msg) in [
        (3, call(3, "sql", json!({ "query": "SELECT * FROM t" }))),
        (4, call(4, "explain", json!({ "query": "SELECT * FROM t" }))),
        (5, call(5, "table", json!({ "name": "u" }))),
        (6, call(6, "query", json!({ "name": "t" }))),
        (7, call(7, "entity", json!({ "id": "000000000001-000000" }))),
        (
            8,
            json!({ "jsonrpc": "2.0", "id": 8, "method": "resources/read", "params": { "uri": "nuthatch://schema" } }),
        ),
    ] {
        let (status, body, _) = h.mcp("/usdc/mcp", Some(&table), msg).await;
        assert_eq!(status, 403, "{body}");
        assert_eq!(body["id"], json!(id), "{body}");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .starts_with("scope_missing"),
            "{body}"
        );
    }
    // A token for another nest cannot even list this one's tools.
    let other = h.signer.sign("es", claims("read:acme-dai"));
    assert_eq!(h.mcp("/usdc/mcp", Some(&other), list.clone()).await.0, 403);
    // The nest scope runs SQL through MCP.
    let whole = h.signer.sign("es", claims("read:usdc"));
    let sql = call(9, "sql", json!({ "query": "SELECT 1" }));
    assert_eq!(h.mcp("/usdc/mcp", Some(&whole), sql).await.0, 200);
    // Missing and expired tokens are refused on MCP as on HTTP.
    assert_eq!(
        h.mcp("/usdc/mcp", None, list.clone()).await.1["error"],
        "token_missing"
    );
    let mut old = claims("read:usdc");
    old["exp"] = json!(now() - 1);
    let old = h.signer.sign("es", old);
    assert_eq!(
        h.mcp("/usdc/mcp", Some(&old), list).await.1["error"],
        "token_expired"
    );
    assert_eq!(h.forwarded(), 3);
}

#[tokio::test]
async fn what_mcp_forwards_is_what_it_judged() {
    let h = Harness::new().await;
    let table = h.signer.sign("es", claims("read:usdc/table/t"));
    // Two `name`s: whichever a parser keeps, the gateway forwards the one it checked, and only that.
    let raw = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sql","name":"table","arguments":{"name":"t","query":"SELECT * FROM secret"}}}"#;
    let resp = h
        .http
        .post(format!("{}/usdc/mcp", h.base))
        .bearer_auth(&table)
        .header("content-type", "application/json")
        .body(raw)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let seen = h.seen.lock().unwrap()[0].clone();
    let forwarded: Value = serde_json::from_slice(&seen.body).unwrap();
    assert_eq!(forwarded["params"]["name"], "table");
    assert_eq!(
        String::from_utf8(seen.body)
            .unwrap()
            .matches("\"name\":\"table\"")
            .count(),
        1
    );
}

#[tokio::test]
async fn routes_outside_the_read_surface_are_refused_whatever_the_token() {
    let h = Harness::new().await;
    let token = h.signer.sign("es", claims("read:usdc read:acme-dai"));
    assert_eq!(
        h.get("/acme/dai/sql?q=SELECT%201", Some(&token)).await.0,
        200
    );
    for (path, status, code) in [
        ("/usdc/_admin/config", 403, "admin_not_proxied"),
        ("/nests", 404, "not_routed"),
        ("/acme", 404, "not_routed"),
        ("/usdc/no-such-route", 404, "not_routed"),
        ("/usdc/table/t%2F..%2F..%2Fsql", 400, "path_not_canonical"),
        ("/usdc//sql", 400, "path_not_canonical"),
    ] {
        let (got, body, _) = h.get(path, Some(&token)).await;
        assert_eq!(
            (got, body["error"].as_str()),
            (status, Some(code)),
            "{path}"
        );
    }
    assert_eq!(h.forwarded(), 1);
    let (status, _, _) = h.get("/_gateway/health", None).await;
    assert_eq!(status, 200);
}

/// The binary itself, with an HS256 secret from the environment: refusals are logged by name, and
/// neither the token nor the secret ever reaches its output.
#[tokio::test]
async fn the_binary_logs_refusals_without_token_or_key_material() {
    let (upstream, _) = stub_nuthatch().await;
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let dir = std::env::temp_dir().join(format!("nuthatch-gateway-{port}"));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = dir.join("gateway.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"listen = "127.0.0.1:{port}"
upstream = "{upstream}"
issuer = "https://idp.test"
audience = "nuthatch"
hs256_secret_env = "GATEWAY_TEST_SECRET"
[[nest]]
name = "usdc"
prefix = ""
"#
        ),
    )
    .unwrap();
    let secret = "a-test-secret-of-at-least-thirty-two-bytes";
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_nuthatch-gateway"))
        .arg(&cfg)
        .env("GATEWAY_TEST_SECRET", secret)
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let base = format!("http://127.0.0.1:{port}");
    let http = reqwest::Client::new();
    let mut up = false;
    for _ in 0..100 {
        if http
            .get(format!("{base}/_gateway/health"))
            .send()
            .await
            .is_ok()
        {
            up = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(up, "the gateway never answered");
    let hs = |c: Value| {
        let h = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let p = URL_SAFE_NO_PAD.encode(c.to_string());
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
        let tag = ring::hmac::sign(&key, format!("{h}.{p}").as_bytes());
        format!("{h}.{p}.{}", URL_SAFE_NO_PAD.encode(tag.as_ref()))
    };
    let good = hs(claims("read:usdc/table/t"));
    let mut expired = claims("read:usdc");
    // Past the default 60 s leeway this config keeps.
    expired["exp"] = json!(now() - 120);
    let expired = hs(expired);
    let get = |path: &'static str, token: String| {
        let http = http.clone();
        let base = base.clone();
        async move {
            http.get(format!("{base}{path}"))
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    };
    assert_eq!(get("/table/t", good.clone()).await, 200);
    assert_eq!(get("/sql?q=SELECT%201", good.clone()).await, 403);
    assert_eq!(get("/sql?q=SELECT%201", expired.clone()).await, 401);
    child.start_kill().unwrap();
    let out = child.wait_with_output().await.unwrap();
    let log = String::from_utf8(out.stderr).unwrap();
    assert!(log.contains("refused scope_missing GET /sql"), "{log}");
    assert!(log.contains("refused token_expired GET /sql"), "{log}");
    assert!(
        !log.contains("SELECT"),
        "the query string was logged: {log}"
    );
    for secret_ish in [good.as_str(), expired.as_str(), secret] {
        assert!(
            !log.contains(secret_ish),
            "key or token material in the log: {log}"
        );
        // Nor any of a token's three parts on its own.
        for part in secret_ish.split('.').filter(|p| p.len() > 8) {
            assert!(!log.contains(part), "token part in the log: {log}");
        }
    }
    let _ = std::fs::remove_dir_all(dir);
}
