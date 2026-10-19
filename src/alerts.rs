//! Alert webhook sinks (RFC-0008 C5): deliver flag/hit annotations (and their reorg retractions) to
//! operator-configured HTTP endpoints, **at-least-once**, without ever blocking the indexer.
//!
//! Delivery semantics are host state by nature, so they live here rather than in a WASM component:
//! - **Durable outbox** in the hot store (redb) - enqueuing an alert is one fast write, and pending
//!   deliveries survive a restart, so at-least-once holds across a process bounce.
//! - **A stalled sink never blocks indexing.** Enqueue is decoupled from delivery (a background
//!   worker drains the outbox); if a webhook is down the outbox grows only to a bound, then sheds its
//!   oldest entries loudly (`outbox_trim`) - the indexer never waits on the network.
//! - **Retractions.** A reorg re-fires the same annotation as a `flag_retracted` event, so a consumer
//!   that acted on a flag learns when the chain took it back.
//! - **One cursor.** Alerts ride the single indexing cursor; there is no second cursor or reconciler.
//!
//! The endpoint is the allowlist: a sink only ever POSTs to a URL the nest's `[[alerts]]` declares.
//! `wasi:http`-sandboxed egress (the C4 grant model) is available for an untrusted enricher, but a
//! webhook sink is operator-configured and delivery-guaranteed, so the host owns it.

use crate::config::Alert;
use crate::store::HotStore;
use anyhow::Result;
use serde_json::{json, Value};
use std::time::Duration;

/// One readable line for a chat sink.
///
/// Deliberately plain text rather than an embed: an embed is a richer object with its own failure
/// modes, and the thing an operator needs at 3am is the nest, the kind, and what happened, in the
/// notification preview. `event` leads because that is what distinguishes two alerts of the same
/// kind from the same nest.
pub fn discord_line(payload: &Value) -> String {
    let kind = payload
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("alert");
    let event = payload.get("event").and_then(Value::as_str).unwrap_or("");
    let annotation = payload.get("annotation");
    let nest = annotation
        .and_then(|a| a.get("nest"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let detail = annotation
        .map(|a| a.to_string())
        .unwrap_or_else(|| "{}".to_string());
    // Discord rejects an empty body and truncates a long one; 1,800 leaves room for the wrapper
    // inside the 2,000-character limit.
    let mut line = format!(
        "**{kind}**{} {event}\n```{detail}```",
        if nest.is_empty() {
            String::new()
        } else {
            format!(" [{nest}]")
        }
    );
    if line.chars().count() > 1_800 {
        line = line.chars().take(1_797).collect::<String>() + "```";
    }
    line
}

/// Bound on the durable outbox. A dead webhook can't grow it without limit; past this the oldest
/// undelivered alerts are shed (loudly). Generous - a transient outage of minutes is absorbed.
pub const OUTBOX_MAX: u64 = 10_000;
/// How many pending deliveries a single drain attempts.
const DELIVERY_BATCH: usize = 100;
/// Poll interval between outbox drains - also the (constant) retry backoff for a failed delivery.
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Per-request timeout: a slow endpoint can't wedge the delivery worker.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How many webhook POSTs are in flight at once during a drain (SEC-8): one slow/dead sink must not
/// throttle the others - the drain is bounded by the slowest single request, not the sum.
const DELIVERY_CONCURRENCY: usize = 8;

/// Routes annotation kinds to the webhook sinks that want them. Built once from `[[alerts]]`.
#[derive(Clone, Default)]
pub struct AlertRouter {
    sinks: Vec<Alert>,
}

impl AlertRouter {
    pub fn new(sinks: Vec<Alert>) -> Self {
        Self { sinks }
    }

    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }

    /// The URLs that want annotations of `kind`.
    fn urls_for(&self, kind: &str) -> Vec<(&str, crate::config::AlertFormat)> {
        self.sinks
            .iter()
            .filter(|s| s.kinds.iter().any(|k| k == kind))
            .map(|s| (s.url.as_str(), s.format))
            .collect()
    }

    /// Whether any sink watches `kind` - a cheap pre-check before building a retraction payload.
    pub fn watches(&self, kind: &str) -> bool {
        self.sinks.iter().any(|s| s.kinds.iter().any(|k| k == kind))
    }
}

/// Enqueue an alert for every sink that watches `kind`. `event` is `"flag"` or `"flag_retracted"`.
/// A no-op when nothing is configured or nothing matches. Bounds the outbox after pushing.
pub fn enqueue(
    store: &dyn HotStore,
    router: &AlertRouter,
    event: &str,
    kind: &str,
    annotation: &Value,
) -> Result<()> {
    let queued = payloads(router, event, kind, annotation);
    for payload in &queued {
        store.outbox_push(payload)?;
    }
    if !queued.is_empty() {
        trim(store)?;
    }
    Ok(())
}

/// The outbox entries [`enqueue`] would push, for a caller that writes them in its own transaction.
pub fn payloads(router: &AlertRouter, event: &str, kind: &str, annotation: &Value) -> Vec<String> {
    router
        .urls_for(kind)
        .into_iter()
        .map(|(url, format)| {
            // The format rides in the payload rather than being looked up at delivery: the outbox is
            // durable, so an entry may outlive an edit to `[[alerts]]`, and a queued alert should be
            // delivered the way it was queued.
            json!({
                "event": event,
                "kind": kind,
                "url": url,
                "format": format,
                "annotation": annotation,
            })
            .to_string()
        })
        .collect()
}

/// Bound the outbox at [`OUTBOX_MAX`], dropping the oldest undelivered entries.
pub fn trim(store: &dyn HotStore) -> Result<()> {
    let dropped = store.outbox_trim(OUTBOX_MAX)?;
    if dropped > 0 {
        tracing::warn!(
            "alert outbox full ({OUTBOX_MAX}): dropped {dropped} oldest undelivered alert(s) - \
             a webhook is down or too slow"
        );
    }
    Ok(())
}

/// Drain up to [`DELIVERY_BATCH`] pending deliveries once, POSTing the exact stored payload bytes to
/// its URL (so an HMAC signature covers what's actually sent). A payload whose `webhook` names a
/// secret in `secrets` gets an `X-Nuthatch-Signature: sha256=<hmac>` header (RFC-0010 Part B). A
/// delivered entry (2xx) is removed; a failure is left for retry (at-least-once) and doesn't stop the
/// drain. Returns how many were delivered.
pub async fn deliver_pending(
    store: &dyn HotStore,
    client: &reqwest::Client,
    secrets: &std::collections::HashMap<String, String>,
) -> usize {
    use futures::stream::StreamExt;
    let pending = store.outbox_pending(DELIVERY_BATCH).unwrap_or_default();

    // Phase 1: send all POSTs concurrently (SEC-8), each independent. The store is NOT touched here -
    // redb is single-writer, so mutations are applied afterwards in phase 2, on this task alone.
    let outcomes: Vec<(u64, Outcome)> = futures::stream::iter(pending)
        .map(|(seq, payload_str)| {
            let client = client.clone();
            async move {
                let Ok(payload) = serde_json::from_str::<Value>(&payload_str) else {
                    return (seq, Outcome::Shed); // corrupt entry - drop it
                };
                let url = payload.get("url").and_then(Value::as_str).unwrap_or("");
                // Send the stored bytes verbatim so a signature matches - except where the sink will
                // not accept them. A Discord webhook answers our own payload `400 Cannot send an
                // empty message` and `{"content": …}` with `204`, and a non-success is retried
                // forever, so a raw body there is an outbox that never drains. The signature is
                // computed over whatever is actually sent, which is the only thing a receiver can
                // verify.
                let body = match payload.get("format").and_then(Value::as_str) {
                    Some("discord") => json!({ "content": discord_line(&payload) }).to_string(),
                    _ => payload_str.clone(),
                };
                let mut req = client
                    .post(url)
                    .header("content-type", "application/json")
                    .body(body.clone());
                if let Some(secret) = payload
                    .get("webhook")
                    .and_then(Value::as_str)
                    .and_then(|n| secrets.get(n))
                {
                    let sig = crate::webhooks::hmac_sha256_hex(secret.as_bytes(), body.as_bytes());
                    req = req.header("X-Nuthatch-Signature", format!("sha256={sig}"));
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => (seq, Outcome::Delivered),
                    Ok(resp) => {
                        tracing::warn!(
                            "{}",
                            retry_line(url, &format!("returned {}", resp.status()))
                        );
                        (seq, Outcome::Retry)
                    }
                    Err(e) => {
                        let e = e.without_url();
                        tracing::warn!("{}", retry_line(url, &format!("delivery failed: {e}")));
                        (seq, Outcome::Retry)
                    }
                }
            }
        })
        .buffer_unordered(DELIVERY_CONCURRENCY)
        .collect()
        .await;

    // Phase 2: apply the store mutations. Remove delivered + corrupt; leave retries to be picked up on
    // the next pass. Batched into ONE `spawn_blocking` (audit F-C3): each removal fsyncs, so draining a
    // burst inline parked a tokio worker for one fsync per delivered alert.
    let mut delivered = 0usize;
    let mut to_remove = Vec::new();
    for (seq, outcome) in outcomes {
        match outcome {
            Outcome::Delivered => {
                to_remove.push(seq);
                delivered += 1;
            }
            Outcome::Shed => to_remove.push(seq),
            Outcome::Retry => {}
        }
    }
    let _ = store.outbox_remove_batch_blocking(to_remove).await;
    delivered
}

/// The fate of one outbox entry after a delivery attempt.
enum Outcome {
    /// 2xx - remove it.
    Delivered,
    /// Corrupt/unparseable - remove it (never deliverable).
    Shed,
    /// Transient failure - leave it for the next drain (at-least-once).
    Retry,
}

/// The background delivery worker: drain the outbox, publish the depth gauge, sleep, repeat. Runs
/// alongside the indexer and API on its own task; enqueuing is decoupled from it so a slow webhook
/// never blocks indexing. The `POLL_INTERVAL` sleep is also the retry backoff for failed deliveries.
pub async fn run_delivery_worker(
    store: std::sync::Arc<dyn HotStore>,
    secrets: std::collections::HashMap<String, String>,
) {
    let client = match reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("alert delivery worker could not start (HTTP client): {e}");
            return;
        }
    };
    tracing::info!("alert delivery worker started");
    loop {
        let delivered = deliver_pending(store.as_ref(), &client, &secrets).await;
        crate::metrics::METRICS.set_alert_outbox(store.outbox_len());
        if delivered > 0 {
            tracing::debug!(
                "delivered {delivered} alert(s); {} pending",
                store.outbox_len()
            );
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// A webhook path routinely carries its secret (Slack, Discord, a bearer in the path), so a log line
/// names the endpoint by scheme and host only (#1663).
fn retry_line(url: &str, detail: &str) -> String {
    format!(
        "alert webhook {} {detail} - will retry",
        crate::rpc::redact_url(url)
    )
}

#[cfg(test)]
mod tests {
    /// **The shape a chat sink will actually accept.** Measured against a real Discord webhook, not
    /// inferred: our own outbox payload is answered
    /// `400 {"message": "Cannot send an empty message", "code": 50006}`, and `{"content": "…"}` is
    /// answered `204`.
    ///
    /// That matters more than it looks. `deliver_pending` retries a non-success forever, so pointing
    /// a Discord webhook at the raw format gives an outbox that never drains and alerts that never
    /// arrive - an alerting system failing silently, which is worse than having none.
    #[test]
    fn a_discord_sink_gets_a_body_discord_accepts() {
        let payload = serde_json::json!({
            "event": "entity `received` faulted: the entity circuit faulted",
            "kind": "entity_fault",
            "url": "https://discord.com/api/webhooks/x/y",
            "format": "discord",
            "annotation": {"nest": "usdc-arb", "entity": "received"},
        });
        let line = discord_line(&payload);
        assert!(line.contains("entity_fault"), "the kind leads: {line}");
        assert!(
            line.contains("usdc-arb"),
            "the nest is in the preview: {line}"
        );
        assert!(
            line.contains("the entity circuit faulted"),
            "and what happened: {line}"
        );
        // Discord refuses an empty body and truncates past 2,000 characters.
        assert!(!line.is_empty());
        assert!(line.chars().count() <= 1_800, "{}", line.chars().count());
    }

    /// A long annotation is truncated rather than rejected. Discord's limit is 2,000 characters and
    /// a body over it is a 400, which the worker would retry forever.
    #[test]
    fn a_huge_annotation_is_cut_to_fit() {
        let payload = serde_json::json!({
            "event": "e",
            "kind": "k",
            "annotation": {"nest": "n", "blob": "x".repeat(9_000)},
        });
        let line = discord_line(&payload);
        assert!(line.chars().count() <= 1_800, "{}", line.chars().count());
    }

    use super::*;
    use crate::store::Store;

    #[test]
    fn router_matches_kinds() {
        let r = AlertRouter::new(vec![
            Alert {
                kinds: vec!["sanction_hit".into()],
                url: "https://a".into(),
                format: crate::config::AlertFormat::Raw,
            },
            Alert {
                kinds: vec!["sanction_hit".into(), "threshold_flag".into()],
                url: "https://b".into(),
                format: crate::config::AlertFormat::Raw,
            },
        ]);
        let urls =
            |kind: &str| -> Vec<&str> { r.urls_for(kind).into_iter().map(|(u, _)| u).collect() };
        assert_eq!(urls("sanction_hit"), vec!["https://a", "https://b"]);
        assert_eq!(urls("threshold_flag"), vec!["https://b"]);
        assert!(r.watches("sanction_hit"));
        assert!(!r.watches("nope"));
    }

    #[test]
    fn enqueue_is_noop_without_sinks() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        enqueue(
            &store,
            &AlertRouter::default(),
            "flag",
            "sanction_hit",
            &json!({}),
        )
        .unwrap();
        assert_eq!(store.outbox_len(), 0);
    }

    #[test]
    fn outbox_trims_oldest_when_over_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        for i in 0..5 {
            store.outbox_push(&format!("{{\"n\":{i}}}")).unwrap();
        }
        assert_eq!(store.outbox_trim(3).unwrap(), 2, "dropped the 2 oldest");
        let pending = store.outbox_pending(10).unwrap();
        assert_eq!(pending.len(), 3);
        // The survivors are the 3 newest (n = 2, 3, 4), oldest-first.
        assert!(pending[0].1.contains("\"n\":2"));
    }

    /// **The seam, not the piece.** `discord_line` having the right shape proves nothing about the
    /// body that leaves the process - my first pass at this tested the renderer alone, and removing
    /// the rendering from `deliver_pending` entirely left every test green.
    ///
    /// So this drives real delivery against a real local server and asserts on **what arrived**: a
    /// `discord` sink gets `{"content": …}` and nothing else, and a `raw` sink on the same run gets
    /// the outbox payload byte for byte, because every existing sink must be unaffected.
    #[tokio::test]
    async fn a_discord_sink_receives_content_and_a_raw_sink_is_unchanged() {
        use axum::{routing::post, Json, Router};
        use std::sync::{Arc, Mutex};

        let received = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
        let sink = received.clone();
        let app = Router::new()
            .route(
                "/discord",
                post({
                    let sink = sink.clone();
                    move |Json(body): Json<Value>| {
                        let sink = sink.clone();
                        async move {
                            sink.lock().unwrap().push(("discord".into(), body));
                            "ok"
                        }
                    }
                }),
            )
            .route(
                "/raw",
                post({
                    let sink = sink.clone();
                    move |Json(body): Json<Value>| {
                        let sink = sink.clone();
                        async move {
                            sink.lock().unwrap().push(("raw".into(), body));
                            "ok"
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let router = AlertRouter::new(vec![
            Alert {
                kinds: vec!["entity_fault".into()],
                url: format!("http://{addr}/discord"),
                format: crate::config::AlertFormat::Discord,
            },
            Alert {
                kinds: vec!["entity_fault".into()],
                url: format!("http://{addr}/raw"),
                format: crate::config::AlertFormat::Raw,
            },
        ]);
        enqueue(
            &store,
            &router,
            "entity `received` faulted",
            "entity_fault",
            &json!({"nest": "usdc-arb", "entity": "received"}),
        )
        .unwrap();

        let client = reqwest::Client::new();
        let delivered = deliver_pending(&store, &client, &std::collections::HashMap::new()).await;
        assert_eq!(delivered, 2, "both sinks");

        let got = received.lock().unwrap().clone();
        let discord = got
            .iter()
            .find(|(w, _)| w == "discord")
            .expect("discord sink");
        let raw = got.iter().find(|(w, _)| w == "raw").expect("raw sink");

        // Discord takes `{"content": …}` and refuses anything else with a 400 the worker would
        // retry forever. One key, and it carries the detail.
        assert_eq!(
            discord.1.as_object().map(|o| o.len()),
            Some(1),
            "a discord body is exactly {{content}}: {:?}",
            discord.1
        );
        let content = discord.1["content"].as_str().expect("content is a string");
        assert!(content.contains("entity_fault"), "{content}");
        assert!(content.contains("usdc-arb"), "{content}");

        // The raw sink is untouched by any of this.
        assert_eq!(raw.1["event"], "entity `received` faulted");
        assert_eq!(raw.1["kind"], "entity_fault");
        assert_eq!(raw.1["annotation"]["nest"], "usdc-arb");
        assert!(
            raw.1.get("content").is_none(),
            "a raw sink must not get a discord body: {:?}",
            raw.1
        );
    }

    /// The C5 gate: a real local webhook server receives a `flag` on a raised annotation and a
    /// `flag_retracted` on a reorg, and delivered entries leave the durable outbox (at-least-once).
    #[tokio::test]
    async fn delivers_flag_and_retraction_to_a_live_webhook() {
        use axum::{routing::post, Json, Router};
        use std::sync::{Arc, Mutex};

        // A local webhook that records every payload it receives.
        let received = Arc::new(Mutex::new(Vec::<Value>::new()));
        let sink = received.clone();
        let app = Router::new().route(
            "/hook",
            post(move |Json(body): Json<Value>| {
                let sink = sink.clone();
                async move {
                    sink.lock().unwrap().push(body);
                    "ok"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let url = format!("http://{addr}/hook");

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let router = AlertRouter::new(vec![Alert {
            kinds: vec!["sanction_hit".into()],
            url: url.clone(),
            format: crate::config::AlertFormat::Raw,
        }]);

        // A hit is raised, then a reorg retracts it.
        let ann = json!({ "kind": "sanction_hit", "address": "0xbad", "block_number": 10 });
        enqueue(&store, &router, "flag", "sanction_hit", &ann).unwrap();
        enqueue(&store, &router, "flag_retracted", "sanction_hit", &ann).unwrap();
        assert_eq!(store.outbox_len(), 2);

        // Drain the outbox to the live webhook.
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap();
        let delivered = deliver_pending(&store, &client, &std::collections::HashMap::new()).await;
        assert_eq!(delivered, 2);
        assert_eq!(store.outbox_len(), 0, "delivered entries leave the outbox");

        let got = received.lock().unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0]["event"], "flag");
        assert_eq!(got[0]["annotation"]["address"], "0xbad");
        assert_eq!(got[1]["event"], "flag_retracted");
    }

    /// RFC-0010 egress security: a webhook whose payload names a secret is signed with
    /// `X-Nuthatch-Signature: sha256=<hmac>` over the *exact bytes sent*, so the receiver can verify
    /// provenance. A payload without a matching secret goes unsigned.
    #[tokio::test]
    async fn signs_delivery_when_the_webhook_has_a_secret() {
        use axum::{extract::State, http::HeaderMap, routing::post, Router};
        use std::sync::{Arc, Mutex};

        // Records (signature-header, raw-body) for each request.
        type Seen = Arc<Mutex<Vec<(Option<String>, String)>>>;
        let seen: Seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route(
                "/hook",
                post(
                    |State(sink): State<Seen>, headers: HeaderMap, body: String| async move {
                        let sig = headers
                            .get("x-nuthatch-signature")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        sink.lock().unwrap().push((sig, body));
                        "ok"
                    },
                ),
            )
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let url = format!("http://{addr}/hook");

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();

        // One payload names webhook "signed" (secret configured), one names "plain" (no secret).
        let signed = json!({ "event": "rows", "webhook": "signed", "url": url, "rows": [1] });
        let plain = json!({ "event": "rows", "webhook": "plain", "url": url, "rows": [2] });
        store.outbox_push(&signed.to_string()).unwrap();
        store.outbox_push(&plain.to_string()).unwrap();

        let mut secrets = std::collections::HashMap::new();
        secrets.insert("signed".to_string(), "topsecret".to_string());

        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap();
        let delivered = deliver_pending(&store, &client, &secrets).await;
        assert_eq!(delivered, 2);

        let got = seen.lock().unwrap();
        let signed_row = got
            .iter()
            .find(|(_, b)| b.contains("\"webhook\":\"signed\""))
            .unwrap();
        let plain_row = got
            .iter()
            .find(|(_, b)| b.contains("\"webhook\":\"plain\""))
            .unwrap();

        // The signature covers the exact bytes POSTed, under the configured secret.
        let expect = crate::webhooks::hmac_sha256_hex(b"topsecret", signed_row.1.as_bytes());
        assert_eq!(
            signed_row.0.as_deref(),
            Some(format!("sha256={expect}").as_str())
        );
        // No secret → no signature header.
        assert_eq!(plain_row.0, None, "an unsecreted webhook is not signed");
    }

    /// A dead endpoint leaves the alert in the outbox for retry - at-least-once, never dropped on the
    /// first failure.
    #[tokio::test]
    async fn failed_delivery_stays_in_outbox_for_retry() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        // A URL that refuses connections (nothing listening on this port).
        let router = AlertRouter::new(vec![Alert {
            kinds: vec!["sanction_hit".into()],
            url: "http://127.0.0.1:9/hook".into(),
            format: crate::config::AlertFormat::Raw,
        }]);
        enqueue(
            &store,
            &router,
            "flag",
            "sanction_hit",
            &json!({ "kind": "sanction_hit" }),
        )
        .unwrap();

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap();
        let delivered = deliver_pending(&store, &client, &std::collections::HashMap::new()).await;
        assert_eq!(delivered, 0);
        assert_eq!(
            store.outbox_len(),
            1,
            "a failed delivery is retained, not dropped"
        );
    }

    #[test]
    fn a_retry_line_names_the_endpoint_without_its_secret_path() {
        let line = super::retry_line(
            "https://hooks.slack.com/services/T000/B000/SECRETTOKEN",
            "returned 500",
        );
        assert_eq!(
            line,
            "alert webhook https://hooks.slack.com returned 500 - will retry"
        );
    }
}
