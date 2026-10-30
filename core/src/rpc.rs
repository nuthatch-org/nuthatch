//! The JSON-RPC client: failover, batching, splitting, pacing and error classification, over a
//! [`Transport`] that carries the bytes (RFC-0075 §5). The node posts with reqwest, a tab with `fetch`.

// `Log` lives in nuthatch-decode so fuzz targets build without pulling in dbsp (nuthatch#581).
use nuthatch_decode::rpc::parse_hex_u64;
pub use nuthatch_decode::rpc::Log;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub use crate::transport::{Reply, Transport, TransportError};

/// How many times a whole `block_timestamps` batch is retried before it's returned as an error rather
/// than silently yielding an all-zeros timestamp map into the sealed path.
pub const TIMESTAMP_ATTEMPTS: usize = 4;

#[cfg(any(test, feature = "test-hooks"))]
tokio::task_local! {
    // Scoped to one test future: concurrent clients cannot contaminate its observations.
    pub static RETRY_PAUSES: std::cell::RefCell<Vec<Duration>>;
}

pub async fn retry_pause(pause: Duration) {
    #[cfg(any(test, feature = "test-hooks"))]
    if RETRY_PAUSES
        .try_with(|pauses| pauses.borrow_mut().push(pause))
        .is_ok()
    {
        return;
    }
    crate::time::sleep(pause).await;
}

/// Max block numbers per `eth_getBlockByNumber` JSON-RPC batch. Many providers cap batch size and
/// **silently drop** an oversized batch (returning nothing), which the strict no-partial-map guard
/// then correctly rejects - so a dense window that needs 1000+ distinct timestamps would fail on such
/// a node. Splitting into bounded sub-batches keeps each request within common limits.
pub const MAX_TIMESTAMP_BATCH: usize = 200;

/// The most of one multi-item `eth_getBlockByNumber` or `eth_call` batch body read before the batch is
/// narrowed (#2227). Every transaction hash rides in a header, so without it a dense chain at
/// `--concurrency 8` held over 2 GB of headers in flight.
const BATCH_BODY_CAP: usize = 8 << 20;

/// Return type of the self-recursive [`RpcClient::fetch_timestamp_batch`]. Boxed because an `async fn`
/// cannot recurse into itself without a heap indirection.
type TimestampBatchFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<HashMap<u64, Value>>> + Send + 'a>>;

/// Return type of the self-recursive [`RpcClient::eth_call_batch_at`]. Boxed because an `async fn`
/// cannot recurse into itself without a heap indirection.
type CallBatchFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Option<String>>>> + Send + 'a>>;

/// How many block timestamps to remember (RFC-0029 §6d). Timestamps are 16 bytes of map entry each, so
/// this is a few hundred KB - trivial next to the requests it removes on retry and split-and-retry,
/// where we currently re-fetch every timestamp in a range we just split.
pub const TIMESTAMP_CACHE_MAX: usize = 262_144;

/// The most of one multi-block `eth_getLogs` body read before the range is refused as too large
/// (#1788). A node with no result cap answers a whole window in one response, which held parsed is
/// several times its size; refused here, it is narrowed like any capped provider. About 50,000
/// ERC-20 transfers.
pub const GETLOGS_BODY_CAP: usize = 32 << 20;

/// A response's JSON-RPC `error`, where `"error": null` on a success is no error (#2225).
fn error_of(resp: &Value) -> Option<&Value> {
    resp.get("error").filter(|e| !e.is_null())
}

// `None` is a stored EVM fact, not a convenient bucket for provider failures.
pub fn decode_call_batch(response: &Value, expected: usize) -> Result<Vec<Option<String>>> {
    let items = response
        .as_array()
        .context("eth_call batch response is not an array")?;
    let mut out = vec![None; expected];
    let mut seen = vec![false; expected];
    for item in items {
        let id = item
            .get("id")
            .and_then(Value::as_u64)
            .and_then(|id| usize::try_from(id).ok())
            .context("eth_call batch item has no valid id")?;
        if id >= expected || seen[id] {
            bail!("eth_call batch contains an unexpected or duplicate id {id}");
        }
        seen[id] = true;
        if let Some(error) = error_of(item) {
            if item.get("result").is_some() {
                bail!("eth_call batch item contains both result and error");
            }
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let code = error.get("code").and_then(Value::as_i64);
            // GraphOps wraps the upstream's revert as `-32603 "gave up retrying on upstream-level
            // after 1.9s: 3: execution reverted"`; the embedded `3:` keeps it as explicit as a bare one.
            let wrapped_revert = code == Some(-32603)
                && (message.ends_with(": 3: execution reverted")
                    || message.contains(": 3: execution reverted:"));
            let explicit_revert = wrapped_revert
                || matches!(code, Some(3 | -32000 | -32015))
                    && (message == "execution reverted"
                        || message.starts_with("execution reverted:")
                        || message.starts_with("vm execution error: revert"));
            // A revert in a shape not listed above is still the node's answer at a pinned block, so
            // asking again cannot change it; it is refused by name rather than retried (#2226).
            if !explicit_revert && message.contains("revert") {
                return Err(ClassifiedError {
                    class: FailureClass::Terminal,
                    detail: format!(
                        "eth_call batch item {id} reverted in a shape nuthatch does not recognise, \
                         so it is neither stored nor asked again: {error}"
                    ),
                }
                .into());
            }
            if !explicit_revert {
                return Err(ClassifiedError {
                    class: classify_rpc_error(error),
                    detail: format!("eth_call batch item {id} failed: {error}"),
                }
                .into());
            }
        } else {
            let result = item
                .get("result")
                .and_then(Value::as_str)
                .context("eth_call batch item has no string result or explicit revert")?;
            if !result.starts_with("0x")
                || result.len() % 2 != 0
                || !result[2..].bytes().all(|b| b.is_ascii_hexdigit())
            {
                bail!("eth_call batch item has malformed return data");
            }
            out[id] = Some(result.to_string());
        }
    }
    if seen.iter().any(|seen| !seen) {
        bail!("eth_call batch omitted one or more requested results");
    }
    Ok(out)
}

/// Select the RPC endpoint pool for a command, preserving order and dropping duplicates.
///
/// An explicit `--rpc` is an isolation boundary, not a preference hint: it is the complete pool
/// the operator authorised this invocation to contact. Without it, use the configured/default
/// pool. In particular, never silently append public endpoints after a paid endpoint: that makes
/// both the privacy promise and any request-cost measurement untrue.
pub fn select_rpcs(
    override_urls: &[String],
    configured: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let urls: Box<dyn Iterator<Item = String>> = if override_urls.is_empty() {
        Box::new(configured.into_iter())
    } else {
        Box::new(override_urls.iter().cloned())
    };
    for url in urls {
        if !out.contains(&url) {
            out.push(url);
        }
    }
    out
}

/// After an endpoint fails, skip it for this long (unless every endpoint is unhealthy) - so one dead
/// provider doesn't cost a full request-timeout on every call that round-robins onto it. A partial
/// outage fails over fast instead of stalling the tip loop.
pub const ENDPOINT_COOLDOWN_MS: u64 = 30_000;

/// The first rest after a 429 that named no `Retry-After` (#1853).
const RATE_LIMIT_PAUSE_MS: u64 = 1_000;

/// A *terminal* failure (bad credentials, endpoint refusing us outright) earns a much longer cooldown
/// than a transient one: asking again in 30s will get the same 401, and a tight retry loop against a
/// 403 is exactly how a production nest spent forty minutes logging nothing useful (RFC-0028 §3).
///
/// Deliberately a long cooldown rather than permanent removal: an endpoint whose auth blips - a key
/// rotation, a provider incident - recovers on its own, and degrading beats failing. Five minutes
/// matches RFC-0026's backoff cap, so the two "something is wrong, back off" policies agree.
pub const TERMINAL_COOLDOWN_MS: u64 = 300_000;

/// How an RPC failure should be treated (RFC-0028 §3). The classification is the whole point: treat a
/// too-large request like a dead endpoint and you fail over pointlessly; treat a bad API key like a
/// blip and you retry it forever.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureClass {
    /// The *request* was too big. Retry the same block range at a narrower width. `suggested` carries
    /// a provider-offered range when one was parsed out of the error - Alchemy, for one, names the
    /// range that would have worked, which beats halving blindly toward it.
    ///
    /// Slice 1 classifies this; acting on it is slice 3.
    Narrowable {
        suggested: Option<(u64, u64)>,
        /// True when this verdict came **only** from a pool-wide 429 escalated by
        /// [`escalate_pool_wide_rate_limit`], not from a provider saying the result was too large.
        ///
        /// The two must part company at the point where there is no narrower range left. A real size
        /// refusal is a fact about one block and no amount of waiting changes it. A throttle is a fact
        /// about our pacing, and waiting is exactly what fixes it. See #916.
        escalated_from_rate_limit: bool,
    },
    /// A rate limit. Transient in effect - fail over and retry at the same width, because "you asked
    /// too often" is not "you asked for too much". Tracked as its own variant only so that a
    /// **pool-wide** 429 on the *same* window can escalate to [`FailureClass::Narrowable`]: when every
    /// endpoint refuses the same request, that stops being evidence about pacing and starts being
    /// evidence about the request (RFC-0028 §3d). Narrowing also happens to reduce load, so the
    /// escalation is benign even when the cause really was pacing.
    ///
    /// `retry_after` carries the provider's own answer to "when should I come back", when it gave
    /// one - Chainstack names it as `error.data.try_again_in`, and an HTTP 429 may carry
    /// `Retry-After`. Honouring it is the same move as honouring a suggested *range* on
    /// [`FailureClass::Narrowable`]: a number the provider just handed us beats a number we guessed.
    /// `None` is the common case - most providers say nothing - and the caller keeps its own pacing.
    RateLimited { retry_after: Option<Duration> },
    /// The request is fine, this endpoint is having a moment. Fail over, retry at the same width.
    Transient,
    /// This endpoint keeps no blocks this old (#1607). Not about width, so never narrowed, and not
    /// about its health at the tip, which it may serve perfectly well.
    HistoryUnavailable,
    /// The endpoint refused the getLogs filter's address list: none at all, or too many (#1941).
    /// Not a width and not a credential, so the caller changes the list and asks again.
    FilterRefused,
    /// This endpoint will not serve us until something changes outside the process. Long cooldown,
    /// and say so loudly.
    Terminal,
}

/// An RPC failure carrying its classification, so `call`/`post_with_failover` can branch on *why* a
/// call failed instead of treating every failure as a transient blip.
#[derive(Debug)]
pub struct ClassifiedError {
    pub class: FailureClass,
    pub detail: String,
}

impl std::fmt::Display for ClassifiedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.detail)
    }
}

impl std::error::Error for ClassifiedError {}

/// A response refused past our own body cap. Narrowable, but says nothing about the endpoint: every
/// endpoint sends the same bytes, so it is neither cooled down nor failed over from (#2227).
#[derive(Debug)]
pub struct BodyCapped {
    cap: usize,
}

impl std::fmt::Display for BodyCapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "response passed the {}-byte body cap; narrowing the range",
            self.cap
        )
    }
}

impl std::error::Error for BodyCapped {}

pub fn body_capped(err: &anyhow::Error) -> bool {
    err.chain().any(|e| e.is::<BodyCapped>())
}

/// Appends `chunk` unless it would take `body` past `cap`, growing it by doubling no further than
/// the cap, so neither the bytes nor the allocation ever exceed it.
pub fn append_capped(body: &mut Vec<u8>, chunk: &[u8], cap: usize) -> Result<(), BodyCapped> {
    let need = body.len() + chunk.len();
    if need > cap {
        return Err(BodyCapped { cap });
    }
    if need > body.capacity() {
        let grown = need.max(body.capacity() * 2).min(cap);
        body.reserve_exact(grown - body.len());
    }
    body.extend_from_slice(chunk);
    Ok(())
}

/// Re-classify an all-endpoints failure as [`FailureClass::Narrowable`] when **every** attempt was a
/// rate limit (RFC-0028 §3d).
///
/// One endpoint returning 429 says we asked too often. *Every* endpoint returning 429 for the same
/// request used to be treated as evidence about the request, so the caller narrowed. On a
/// quota-limited pool that is the wrong evidence: each retry spends the same sliding quota (#1297).
///
/// The classification is still raised so a caller can see that every endpoint agreed. Acting on it
/// by shrinking is `indexer::narrowing_can_help`, which returns false for this flag at every width.
///
/// Requires at least two attempts: with a single-endpoint pool "every endpoint" is one endpoint, and a
/// lone 429 is much more likely to be pacing.
pub fn escalate_pool_wide_rate_limit(
    err: anyhow::Error,
    attempts: usize,
    rate_limited: usize,
) -> anyhow::Error {
    if attempts >= 2 && rate_limited == attempts {
        return anyhow::Error::new(ClassifiedError {
            class: FailureClass::Narrowable {
                suggested: None,
                escalated_from_rate_limit: true,
            },
            detail: format!(
                "every endpoint ({attempts}) rate-limited this request; treating it as too large: {err}"
            ),
        });
    }
    err
}

/// One call's failures across the pool, with refusals for want of history kept apart (#1607).
///
/// Such a refusal says nothing about the request or our pacing, so it must not stand in for another
/// endpoint's answer: as the last error it hid a throttle and a cap, and the window never moved. It
/// is named, with the remedy, and is the verdict only when every endpoint gave it.
#[derive(Default)]
struct PoolFailures {
    other: Option<anyhow::Error>,
    /// A 429 is about our pacing at that endpoint, so another endpoint's answer outranks it (#1853):
    /// as the last error it hid a healthy endpoint's range cap, and the window never narrowed.
    limited: Option<anyhow::Error>,
    /// One endpoint refusing our credentials is not the pool refusing them (#2155): the verdict only
    /// when no endpoint failed for a reason that passes.
    refused: Option<anyhow::Error>,
    pruned: Vec<String>,
    last_pruned: Option<anyhow::Error>,
}

impl PoolFailures {
    fn push(&mut self, url: &str, e: anyhow::Error) {
        if matches!(class_of(&e), Some(FailureClass::HistoryUnavailable)) {
            self.pruned.push(redact_url(url));
            self.last_pruned = Some(e);
        } else if matches!(class_of(&e), Some(FailureClass::RateLimited { .. })) {
            self.limited = Some(e);
        } else if matches!(class_of(&e), Some(FailureClass::Terminal)) {
            self.refused = Some(e);
        } else {
            self.other = Some(e);
        }
    }

    fn rested(&mut self, resting: Option<anyhow::Error>) {
        if self.limited.is_none() {
            self.limited = resting;
        }
    }

    fn verdict(self, attempts: usize, rate_limited: usize) -> anyhow::Error {
        let remedy = "supply an archive-capable RPC with `--rpc` or `rpc_urls`";
        let pruned = self.pruned.join(", ");
        match (
            self.other.or(self.limited).or(self.refused),
            self.last_pruned,
        ) {
            (None, Some(e)) => anyhow::Error::new(ClassifiedError {
                class: FailureClass::HistoryUnavailable,
                detail: format!(
                    "no configured RPC endpoint keeps blocks this old ({pruned}); {remedy}. \
                     Last answer: {e:#}"
                ),
            }),
            (None, None) => anyhow!("all RPC endpoints failed"),
            (Some(e), None) => escalate_pool_wide_rate_limit(e, attempts, rate_limited),
            (Some(e), Some(_)) => e.context(format!(
                "{pruned} keeps no blocks this old, so the rest of the pool must serve them; if \
                 this persists, {remedy}"
            )),
        }
    }
}

/// Did this failure's "narrowable" verdict come only from a **pool-wide 429**?
///
/// The caller that needs this is the one holding a range it can no longer narrow. See
/// [`FailureClass::Narrowable::escalated_from_rate_limit`] and #916.
pub fn escalated_from_rate_limit(err: &anyhow::Error) -> bool {
    matches!(
        class_of(err),
        Some(FailureClass::Narrowable {
            escalated_from_rate_limit: true,
            ..
        })
    )
}

/// Did an endpoint refuse this getLogs for its address list (#1941)?
pub fn filter_refused(err: &anyhow::Error) -> bool {
    matches!(class_of(err), Some(FailureClass::FilterRefused))
}

/// The classification carried by `err`, if it came from the RPC client.
///
/// Walks the whole `anyhow` chain rather than only the outermost error, because callers add
/// `.with_context(…)` as an error travels up (`getLogs 100..=200` and friends) - checking only the top
/// would silently lose the classification the moment anyone added context, which is exactly the sort of
/// bug that shows up as "it works in the unit test".
pub fn class_of(err: &anyhow::Error) -> Option<FailureClass> {
    err.chain().find_map(|e| {
        if e.is::<BodyCapped>() {
            return Some(FailureClass::Narrowable {
                suggested: None,
                escalated_from_rate_limit: false,
            });
        }
        e.downcast_ref::<ClassifiedError>().map(|c| c.class.clone())
    })
}

/// Classify an HTTP-level failure. Auth is terminal; a payload-too-large is about the request; the
/// rest are someone else's problem and worth trying elsewhere.
///
/// `429` maps to [`FailureClass::RateLimited`], which behaves as transient: a rate limit usually means
/// "you asked too often", not "you asked for too much", and shrinking the window would trade throughput
/// for nothing on what is really a pacing problem. It is a distinct variant only so that a *pool-wide*
/// 429 can escalate to narrowable in [`escalate_pool_wide_rate_limit`].
pub fn classify_status(status: u16, body: &str) -> FailureClass {
    match status {
        // A 403 is not always about *us*. Measured: `arbitrum-one-rpc.publicnode.com` answers an
        // archive-range request with 403 "Archive requests require a personal token" while serving
        // recent blocks perfectly well (RFC-0028 §3f). Cooling that endpoint down for five minutes
        // would sideline a perfectly good tip source over one deep query, so a capability refusal is
        // transient - it is about the *request*, not the credentials.
        403 if is_capability_refusal(body) => FailureClass::Transient,
        401 | 403 => FailureClass::Terminal,
        413 => FailureClass::Narrowable {
            suggested: suggested_range(body),
            escalated_from_rate_limit: false,
        },
        // The `Retry-After` header is not visible here; `send_classified` fills it in.
        429 => FailureClass::RateLimited { retry_after: None },
        // A 400 carrying cap language is a refusal to serve the *range*, not a malformed request.
        // Measured on Alchemy: `HTTP 400 {"error":{"code":-32602,"message":"Log response size
        // exceeded. You can make eth_getLogs requests with up to a 10,000 block range…"}}`.
        //
        // This is belt to the body-classification braces above it, and deliberately narrow: a 400 that
        // does *not* look like a cap is a genuinely bad request and stays transient. RFC-0029 §3b is
        // explicit that "add 400 to the list" on its own would be the same mistake with a different
        // number - a status-code list is as much a liability as a marker list.
        400 if suggested_range(body).is_some() || looks_like_cap(body) => {
            FailureClass::Narrowable {
                suggested: suggested_range(body),
                escalated_from_rate_limit: false,
            }
        }
        _ => FailureClass::Transient,
    }
}

/// Whether a **batch** failure is worth halving the batch for.
///
/// Deliberately broader than [`is_result_too_large`], and the reason is issue #241
/// item 7: `arbitrum.drpc.org` refuses on **batch count** - "Batch of more than 3 requests are not
/// allowed on free plan" - which matches no cap marker, classifies `Transient`, and so never reached
/// the narrowing at all. The observed behaviour was a window walking `781 → 234 → 220 → 218 → 218 …`
/// and stalling: shrinking the *block range* cannot help when the limit counts *requests*.
///
/// Adding "batch of more than" to the marker list would fix this provider and leave the next one
/// broken. That is the fourth time a marker list has come up short (RFC-0028 §3e, RFC-0029 §3b, §6a,
/// and now here), so this reasons from the request shape instead:
///
/// **Halving a batch always reduces its count**, whatever the provider is objecting to - size, count,
/// or something it has not named. And it is self-limiting: a batch of 200 bottoms out in ~8 splits,
/// at which point a single-item request either succeeds or fails for a reason halving was never going
/// to fix. So the only failures excluded are the ones where retrying differently is *definitely*
/// pointless.
///
/// **#656:** the narrowing path's per-item error variant returns after exactly one round trip with no
/// backoff, so the cost was sequential request count (~403 serial RTTs for a full 200→1 descent), not
/// the retry-cycle waste the comment above originally named. `fetch_timestamp_batch` now runs the two
/// halves of its top-level split concurrently (`tokio::try_join!`), halving the sequential depth to
/// ~202 RTTs; levels below the top still `.await` sequentially, which bounds the concurrency burst at
/// `TIMESTAMP_FANOUT` × 2 rather than the exponential blowup a fully-parallel tree would produce.
pub fn batch_is_narrowable(err: &anyhow::Error) -> bool {
    match class_of(err) {
        // Auth and rate limits are positive findings about something other than size: splitting an
        // unauthorised request into two unauthorised requests helps nobody, and splitting under a rate
        // limit doubles the request count in exactly the wrong direction.
        Some(FailureClass::Terminal)
        | Some(FailureClass::RateLimited { .. })
        | Some(FailureClass::HistoryUnavailable) => false,
        _ => true,
    }
}

/// A short, stable token for a [`FailureClass`], for logs that get grepped rather than read.
///
/// `{class:?}` would do, except `RateLimited { retry_after: Some(..) }` and `Narrowable { suggested:
/// .. }` render differently depending on what the provider volunteered, so the same class does not
/// produce the same string twice. Issue #656 is a diagnosis that turns on counting how many times
/// each class appeared across one backfill, and that wants a token that does not move.
fn class_label(class: Option<&FailureClass>) -> &'static str {
    match class {
        Some(FailureClass::Narrowable { .. }) => "Narrowable",
        Some(FailureClass::RateLimited { .. }) => "RateLimited",
        Some(FailureClass::Transient) => "Transient",
        Some(FailureClass::Terminal) => "Terminal",
        Some(FailureClass::HistoryUnavailable) => "HistoryUnavailable",
        Some(FailureClass::FilterRefused) => "FilterRefused",
        None => "unclassified",
    }
}

/// Whether a body carries direct textual evidence of a range/result cap.
///
/// Shared by the status classifier and [`is_result_too_large`] so the two cannot drift
/// into disagreeing about the same string - which is exactly the failure RFC-0028 §3e consolidated the
/// classifiers to prevent.
pub fn looks_like_cap(body: &str) -> bool {
    let s = body.to_ascii_lowercase();
    // **Not a cap, whatever else the message contains** (#903). A nest at tip asking for a block the
    // provider has not served yet is a normal race, not a refusal to serve a range - narrowing the
    // window cannot help, because the blocks do not exist.
    //
    // It is checked first because the cap list below matches it: Alchemy answers
    // `-32602 "block range extends beyond current head block"`, and the bare `"block range"` marker
    // catches it. The chunker then narrows to a single block, still fails, and the tip loop exits
    // with `block N alone exceeds the provider's getLogs result cap` - a diagnosis that was never
    // true, on a nest that only needed to wait. That killed a nest 3.6 hours into an overnight run.
    //
    // Returning `false` is the whole fix: `classify_status` sends a 400 that is not a cap to
    // `Transient`, which the existing retry handles.
    const NOT_CAP: &[&str] = &[
        "beyond current head",
        "beyond the current head",
        "beyond head",
    ];
    if NOT_CAP.iter().any(|m| s.contains(m)) {
        return false;
    }
    const CAP: &[&str] = &[
        "response size",
        "too many results",
        "query returned more than",
        "more than 10000",
        "result set too large",
        "range is too",
        "range too large",
        "ranges over", // Alchemy free plan: "ranges over 10000 blocks are not supported"
        "too large",
        "limit exceeded",
        "exceeds limit of",
        "exceeds max",
        "logs matched by query",
        "exceeds the limit",
        "block range",
        // Monad public endpoints, 2026-09-03 (RFC-0051): Ankr and QuickNode respectively.
        "exceeds size limit",
        "is limited to a",
        // 1rpc.io/bnb, 2026-10-06 (#1941): "eth_getLogs is limited to 0 - 50 blocks range".
        "blocks range",
        // GraphOps, 2026-09-29.
        "exceeded max allowed range",
    ];
    CAP.iter().any(|m| s.contains(m))
}

/// Parse a JSON-RPC error object out of a raw body, if there is one.
///
/// Non-2xx responses still carry them - which is the whole point of RFC-0029 §6a.
fn rpc_error_of(body: &str) -> Option<Value> {
    serde_json::from_str::<Value>(body)
        .ok()?
        .get("error")
        .cloned()
}

/// Whether a 4xx body says "this *request* needs something you don't have" rather than "*you* are not
/// authenticated" - an archive query against a free tier, a method behind a paid plan.
///
/// The distinction matters because the responses differ: bad credentials mean the endpoint is useless
/// until an operator acts, while a capability limit means it is useless *for this kind of request* and
/// fine for everything else.
pub fn is_capability_refusal(body: &str) -> bool {
    let s = body.to_ascii_lowercase();
    const CAPABILITY: &[&str] = &[
        "archive",         // publicnode: "Archive requests require a personal token"
        "personal token",  // ditto
        "upgrade your",    // plan-gated
        "not enabled for", // Alchemy: "ETH_MAINNET is not enabled for this app"
        "requires a paid",
        "plan does not",
    ];
    CAPABILITY.iter().any(|m| s.contains(m))
}

/// Classify a JSON-RPC `error` object returned with HTTP 200 - which is how most providers actually
/// report both "too many logs" and "authenticate first".
///
/// Matching is on the **message**, not the code: the measured Alchemy refusal for an oversized range
/// carries `-32602`, which is the generic "invalid params" code and cannot distinguish a size refusal
/// from a genuinely malformed filter (RFC-0028 §3).
pub fn classify_rpc_error(err: &Value) -> FailureClass {
    let msg = err
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();

    const NARROWABLE: &[&str] = &[
        "response size exceeded",
        "query returned more than",
        "more than 10000 results",
        "block range is too wide",
        "block range too large",
        "exceeds the max",
        "too many results",
        "limit exceeded",
        "query timeout exceeded",
        // Measured on Monad's public endpoints, 2026-09-03 (RFC-0051 addendum, item 7). Ankr answers
        // an over-wide `eth_getLogs` at HTTP **200** with `-32603 "response exceeds size limit"`, which
        // matched nothing above and fell through to `Transient` - the same width, retried until the
        // attempts ran out. QuickNode answers HTTP 413 `-32614 "eth_getLogs is limited to a 100
        // range"`; the 413 already narrows by status, and the phrase is here so the same words
        // classify the same way whatever status a provider wraps them in.
        "exceeds size limit",
        "is limited to a",
        // 1rpc.io/bnb, 2026-10-06 (#1941): "eth_getLogs is limited to 0 - 50 blocks range".
        "blocks range",
        // Robinhood Chain's public endpoint, 2026-09-04 (RFC-0050 addendum, item 8): HTTP 200
        // `-32000 "logs matched by query exceeds limit of 10000"`. The phrase was in the text
        // fallback's cap list and not here, so the JSON-RPC classifier called it `Transient` and
        // the same width went round again - found by the test that pins it, before a backfill did.
        "logs matched by query",
        // GraphOps, 2026-09-29: `-32012 "getLogs request exceeded max allowed range"` over 25,000.
        "exceeded max allowed range",
    ];
    const TERMINAL: &[&str] = &[
        "must be authenticated",
        "authenticate with an api key",
        "invalid api key",
        "unauthorized",
        "forbidden",
        "project id",
    ];

    // **Rate limits first, and by `code` as well as by message.** A provider may answer HTTP 200 and
    // put the throttle in the JSON-RPC error body - Alchemy returns `{"code":429,"message":"Your app
    // has exceeded its compute units per second capacity"}` per *item* inside a batch. Without this
    // the message matches nothing, falls through to `Transient`, and `batch_is_narrowable` then splits
    // the batch - doubling the request count against a limit on requests per second, which is the one
    // response guaranteed to make it worse. Found on OBIB case 3 (RFC-0036) after three wrong
    // diagnoses; the ordering matters because "limit exceeded" is already in NARROWABLE and would
    // otherwise claim a message like "compute units limit exceeded" for the splitting path.
    // Phrasing is per-provider and there is no standard, so this list is evidence rather than
    // guesswork - each entry is a message we have actually been refused with:
    //   Alchemy     `{"code":429,  "...exceeded its compute units per second capacity"}`
    //   Chainstack  `{"code":-32005,"You've exceeded the RPS limit available on the current plan"}`
    // Note Chainstack sets neither 429 nor any phrase the first version of this list matched, which
    // is the argument for matching on several spellings rather than one provider's.
    const RATE_LIMITED: &[&str] = &[
        "compute units per second",
        "rate limit",
        "rate-limit",
        "rps limit",
        "requests per second",
        "too many requests",
        "exceeded its throughput",
        "request limit reached",
    ];
    // `-32005` is the de-facto "limit exceeded" code (Infura, Chainstack and others); `429` is
    // Alchemy putting the HTTP status in the body.
    let code = err.get("code").and_then(Value::as_i64);
    // The code alone decides only when the message names no size cap: Infura-style endpoints put
    // `-32005` on "query returned more than 10000 results" too. "limit exceeded" is left out because
    // a throttle says it as well.
    let names_a_cap = NARROWABLE
        .iter()
        .any(|p| *p != "limit exceeded" && msg.contains(p));
    if RATE_LIMITED.iter().any(|p| msg.contains(p))
        || (matches!(code, Some(429) | Some(-32005)) && !names_a_cap)
    {
        return FailureClass::RateLimited {
            retry_after: retry_hint_of(err),
        };
    }
    // eth.drpc.org, 2026-10-01 (#1607): HTTP 400, code 27, for every block below its pruning horizon.
    // publicnode, 2026-10-06: HTTP 403 "Archive requests require a personal token" about 10,000
    // blocks back on BSC, where the status alone reads as a credentials refusal.
    const HISTORY: &[&str] = &["first available state", "archive requests require"];
    if HISTORY.iter().any(|p| msg.contains(p)) {
        return FailureClass::HistoryUnavailable;
    }
    // publicnode on BSC and Polygon, 2026-10-06: `-32701 "Please specify an address…"` without an
    // address list, and HTTP 403 `-32602 "Request blocked"` naming ten or more.
    const FILTER: &[&str] = &["specify an address", "request blocked"];
    if FILTER.iter().any(|p| msg.contains(p)) {
        return FailureClass::FilterRefused;
    }
    if NARROWABLE.iter().any(|p| msg.contains(p)) {
        return FailureClass::Narrowable {
            escalated_from_rate_limit: false,
            suggested: suggested_range(
                err.get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or_default(),
            ),
        };
    }
    if TERMINAL.iter().any(|p| msg.contains(p)) {
        return FailureClass::Terminal;
    }
    FailureClass::Transient
}

/// Pull a provider-suggested block range out of an error message.
///
/// Alchemy answers an oversized `eth_getLogs` with *"…this block range should work: [0x1000000,
/// 0x1007fff]"*. That is authoritative and precise, so honouring it turns a shrinking search into
/// one corrective retry (RFC-0028 §5).
///
/// Parsed defensively - this is provider prose, not a contract. A malformed or inverted pair yields
/// `None` and the caller falls back to halving. Whether the range is actually *narrower* than what we
/// asked for is the caller's check, since only the caller knows what it asked for.
pub fn suggested_range(msg: &str) -> Option<(u64, u64)> {
    let open = msg.find('[')?;
    let close = msg[open..].find(']')? + open;
    let (a, b) = msg[open + 1..close].split_once(',')?;
    let parse = |s: &str| -> Option<u64> {
        let s = s.trim();
        let s = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"))?;
        u64::from_str_radix(s, 16).ok()
    };
    let (from, to) = (parse(a)?, parse(b)?);
    (from <= to).then_some((from, to))
}

/// The longest provider-suggested pause we will actually take (#361).
///
/// A hint is advice, not an instruction. A provider answering `try_again_in: 3600s` should stall the
/// run **loudly** rather than silently parking a backfill for an hour, so anything past this is
/// logged and clamped rather than obeyed. Clamping down is safe because [`clamp_retry_hint`] floors
/// the result at the caller's own pacing, so a shortened hint can never undercut it.
pub const MAX_RETRY_HINT: Duration = Duration::from_secs(30);

/// Clamp a provider's retry hint into `[own_pacing, MAX_RETRY_HINT]`, saying so when the cap bites
/// (#361).
///
/// A hint is advice. Obeying `try_again_in: 3600s` verbatim would park a backfill for an hour with
/// nothing in the log to explain the silence - so the pause is capped and the fact is recorded at
/// `warn`, which is the difference between a run that stalls loudly and one that looks hung.
///
/// **`own_pacing` is a floor, and it is the half that is easy to forget.** A hint is only ever a
/// reason to wait *longer* than we already would; it is never a licence to wait less. Without the
/// floor, a provider or CDN answering `Retry-After: 0` - and they do - parses to `Some(0s)`, passes
/// the cap untouched, and replaces our backoff with `sleep(0)`, so we would hammer a limiter with no
/// pacing at all precisely while it was telling us we were over its limit. The cap protects the run
/// from the provider; the floor protects the provider from us.
pub fn clamp_retry_hint(hint: Duration, own_pacing: Duration) -> Duration {
    let capped = if hint > MAX_RETRY_HINT {
        tracing::warn!(
            requested_s = hint.as_secs_f64(),
            capped_s = MAX_RETRY_HINT.as_secs_f64(),
            "provider asked us to wait longer than the cap; pausing for the cap instead - if this \
             repeats, the plan's rate limit is the bottleneck, not a blip"
        );
        MAX_RETRY_HINT
    } else {
        hint
    };
    capped.max(own_pacing)
}

/// Parse a provider's "come back in" duration string into a [`Duration`].
///
/// Measured on Chainstack (2026-08-07), which answers an over-rate request with
/// `{"code":-32005,"data":{"try_again_in":"560.270157ms"}}`. Go's duration syntax, so the unit is a
/// suffix and the value may be fractional.
///
/// Parsed defensively - this is provider prose, not a contract. Anything unrecognised yields `None`
/// and the caller keeps its own pacing, which is the behaviour every provider that says nothing
/// already gets.
pub fn parse_retry_hint(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    // Longest suffix first: `ms` must win over `s`, and `µs`/`us` over `s`.
    const UNITS: &[(&str, f64)] = &[
        ("ms", 1e-3),
        ("us", 1e-6),
        ("\u{b5}s", 1e-6),
        ("ns", 1e-9),
        ("m", 60.0),
        ("h", 3600.0),
        ("s", 1.0),
    ];

    // Try composite Go duration first: `1m30s`, `2h30m15s`, `1h500ms`, etc.
    // A composite has at least two unit-terminated segments and no bare number tail.
    if let Some(total) = parse_go_composite(raw, UNITS) {
        return Some(total);
    }

    let (value, secs_per_unit) = UNITS
        .iter()
        .find_map(|(suffix, mult)| raw.strip_suffix(suffix).map(|v| (v, *mult)))
        // A bare number is seconds, matching `Retry-After`.
        .unwrap_or((raw, 1.0));
    let n: f64 = value.trim().parse().ok()?;
    // NaN, negatives and absurd values are all "the provider said something we do not understand".
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(n * secs_per_unit).ok()
}

/// Parse a Go composite duration string such as `1m30s` or `2h30m15.5s`.
///
/// Go's `time.Duration.String()` emits one or more `<number><unit>` segments with no
/// separator. A single-segment form like `30s` is handled by the caller's simpler path;
/// this function only succeeds when it consumes **two or more** segments so the two code
/// paths do not overlap on single-unit inputs.
fn parse_go_composite(raw: &str, units: &[(&str, f64)]) -> Option<Duration> {
    let mut rest = raw;
    let mut total_secs: f64 = 0.0;
    let mut segments: u32 = 0;

    while !rest.is_empty() {
        // Try each unit in order (longest first). For each candidate unit `suffix`, look
        // for its first occurrence in `rest`; the slice before it must be a valid number.
        let (consumed, secs) = units.iter().find_map(|(suffix, mult)| {
            let sep = rest.find(suffix)?;
            if sep == 0 {
                return None; // need at least one digit before the unit
            }
            let n: f64 = rest[..sep].parse().ok()?;
            if !n.is_finite() || n < 0.0 {
                return None;
            }
            Some((sep + suffix.len(), n * mult))
        })?;

        total_secs += secs;
        rest = &rest[consumed..];
        segments += 1;
    }

    if segments >= 2 {
        Duration::try_from_secs_f64(total_secs).ok()
    } else {
        None
    }
}

/// The `try_again_in` hint out of a JSON-RPC error object, if the provider sent one.
fn retry_hint_of(err: &Value) -> Option<Duration> {
    err.get("data")?
        .get("try_again_in")
        .and_then(Value::as_str)
        .and_then(parse_retry_hint)
}

pub struct RpcClient<T: Transport> {
    http: T,
    pub urls: Vec<String>,
    cursor: AtomicUsize,
    /// `urls[..primaries]` share the load; the rest are asked only while every primary is cooling down.
    primaries: usize,
    /// Per-endpoint health: the millis-since-epoch until which the endpoint is considered unhealthy
    /// (`0` = healthy). Set on a failed call, cleared on a successful one. Endpoints past their cooldown
    /// are tried first; still-unhealthy ones are the fallback of last resort (soonest-to-recover first).
    pub health: Vec<AtomicU64>,
    /// Millis-since-epoch until which an endpoint that answered 429 is not asked at all, not even as
    /// the last resort a cooling endpoint otherwise is (#1853). `0` = not resting.
    limited: Vec<AtomicU64>,
    /// 429s without a hint in a row per endpoint, which double its rest; cleared by an answer.
    limited_streak: Vec<AtomicU64>,
    /// Millis-since-epoch until which an endpoint's terminal cool-down runs. `0` = none.
    terminal: Vec<AtomicU64>,
    /// Terminal cool-downs begun per endpoint, each logged once.
    pub terminal_cooldowns: Vec<AtomicU64>,
    /// The highest block each endpoint has reported from `eth_blockNumber`, so a lower bound on its
    /// head. Pool members' heads differ by a block or two, and one behind refuses a `toBlock` it lacks.
    pub heads: Vec<AtomicU64>,
    /// Total HTTP requests attempted (incl. failover retries) - a benchmark/observability metric.
    pub requests: AtomicU64,
    /// Block-number → unix timestamp, remembered across windows (RFC-0029 §6d).
    ///
    /// **This is only sound because of [`RpcClient::forget_timestamps_above`].** A block *number* does
    /// not identify a block - a reorg replaces the block at that height with a different one carrying a
    /// different timestamp. `block_timestamp` is a sealed column and the segment's content address
    /// depends on it, so serving a stale timestamp after a reorg would seal a wrong value and break
    /// re-execution determinism. The RFC proposes the cache without noting this; the invalidation hook
    /// is the condition that makes it safe, not an optimisation on top.
    pub timestamps: std::sync::Mutex<HashMap<u64, u64>>,
    /// Headers per batch: [`MAX_TIMESTAMP_BATCH`], or less where a known endpoint in the pool takes
    /// less ([`Transport::header_batch_cap`], #1570).
    pub header_width: AtomicUsize,
    /// [`GETLOGS_BODY_CAP`], or a smaller one a test sets.
    logs_body_cap: usize,
    /// [`BATCH_BODY_CAP`], or a smaller one a test sets.
    pub batch_body_cap: usize,
    /// The most addresses one getLogs names, lowered when an endpoint refuses a list (#1941). One
    /// value for the pool: the strictest endpoint sets it, at the cost of extra requests elsewhere.
    log_address_cap: AtomicUsize,
    /// Endpoints whose startup chain-id check got no answer; each is asked again before it serves
    /// anything (#2161).
    unverified: Vec<AtomicBool>,
    /// The chain `verify_chain_ids` was asked to hold the pool to.
    chain: AtomicU64,
}

impl<T: Transport> RpcClient<T> {
    pub fn new(urls: Vec<String>) -> Result<Self> {
        Self::with_timeout(urls, Duration::from_secs(20))
    }

    /// [`Self::new`] with a per-request deadline other than 20 s.
    pub fn with_timeout(urls: Vec<String>, timeout: Duration) -> Result<Self> {
        if urls.is_empty() {
            bail!("no RPC URLs configured");
        }
        let http = T::build(timeout).context("failed to build HTTP client")?;
        let n = urls.len();
        let header_width = T::header_batch_cap(&urls)
            .map_or(MAX_TIMESTAMP_BATCH, |cap| cap.clamp(1, MAX_TIMESTAMP_BATCH));
        let health = urls.iter().map(|_| AtomicU64::new(0)).collect();
        let heads = urls.iter().map(|_| AtomicU64::new(0)).collect();
        let limited = urls.iter().map(|_| AtomicU64::new(0)).collect();
        let limited_streak = urls.iter().map(|_| AtomicU64::new(0)).collect();
        Ok(Self {
            http,
            urls,
            cursor: AtomicUsize::new(0),
            primaries: n,
            health,
            limited,
            limited_streak,
            terminal: (0..n).map(|_| AtomicU64::new(0)).collect(),
            terminal_cooldowns: (0..n).map(|_| AtomicU64::new(0)).collect(),
            heads,
            requests: AtomicU64::new(0),
            timestamps: std::sync::Mutex::new(HashMap::new()),
            header_width: AtomicUsize::new(header_width),
            logs_body_cap: GETLOGS_BODY_CAP,
            batch_body_cap: BATCH_BODY_CAP,
            log_address_cap: AtomicUsize::new(usize::MAX),
            unverified: (0..n).map(|_| AtomicBool::new(false)).collect(),
            chain: AtomicU64::new(0),
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn with_logs_body_cap(mut self, cap: usize) -> Self {
        self.logs_body_cap = cap;
        self
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn with_batch_body_cap(mut self, cap: usize) -> Self {
        self.batch_body_cap = cap;
        self
    }

    /// Total HTTP requests attempted so far (including failover retries).
    pub fn request_count(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// A pool whose `fallback` endpoints carry no load while any of `primary` is healthy: a free
    /// endpoint in front of a paid one, say. Fallbacks already in `primary` are dropped.
    pub fn with_fallbacks(primary: Vec<String>, fallback: Vec<String>) -> Result<Self> {
        let primaries = primary.len();
        let mut urls = primary;
        for url in fallback {
            if !urls.contains(&url) {
                urls.push(url);
            }
        }
        let mut client = Self::new(urls)?;
        if primaries > 0 {
            client.primaries = primaries;
        }
        Ok(client)
    }

    /// The order to try endpoints for this call: healthy primaries first (round-robin from the cursor
    /// for fairness), then healthy fallbacks in the order given, then any still in cooldown as a last
    /// resort (soonest-to-recover first). Advances the round-robin cursor once per call.
    pub fn endpoint_order(&self) -> Vec<usize> {
        self.endpoint_order_holding(None)
    }

    /// [`Self::endpoint_order`], with healthy endpoints known to hold block `need` moved to the front.
    /// At the tip that is the endpoint which just reported the tip, so its `eth_getLogs` is not first
    /// refused by a pool member a block behind (#1498).
    pub fn endpoint_order_holding(&self, need: Option<u64>) -> Vec<usize> {
        let mut order = self.endpoint_order_by_health();
        if let Some(need) = need {
            let healthy = order
                .iter()
                .take_while(|&&j| self.health[j].load(Ordering::Relaxed) <= now_millis())
                .count();
            // Tier before head: a fallback that holds `need` still waits behind every healthy primary.
            order[..healthy].sort_by_key(|&j| {
                (
                    j >= self.primaries,
                    self.heads[j].load(Ordering::Relaxed) < need,
                )
            });
        }
        order
    }

    fn endpoint_order_by_health(&self) -> Vec<usize> {
        let n = self.urls.len();
        let p = self.primaries;
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % p;
        let now = now_millis();
        let mut healthy = Vec::with_capacity(n);
        let mut cooling = Vec::with_capacity(n);
        for i in 0..n {
            let j = if i < p { (start + i) % p } else { i };
            if self.limited[j].load(Ordering::Relaxed) > now {
                continue;
            }
            let until = self.health[j].load(Ordering::Relaxed);
            if until <= now {
                healthy.push(j);
            } else {
                cooling.push((until, j));
            }
        }
        cooling.sort_by_key(|(until, _)| *until);
        healthy
            .into_iter()
            .chain(cooling.into_iter().map(|(_, j)| j))
            .collect()
    }

    pub fn mark_healthy(&self, j: usize) {
        self.health[j].store(0, Ordering::Relaxed);
        self.terminal[j].store(0, Ordering::Relaxed);
        self.limited_streak[j].store(0, Ordering::Relaxed);
    }

    /// A 429 rests the endpoint for as long as it asked, within [`clamp_retry_hint`]'s cap. Without a
    /// hint the rest starts at [`RATE_LIMIT_PAUSE_MS`] and doubles per 429 in a row, up to the
    /// ordinary cooldown: a lone public endpoint usually throttles per second.
    fn mark_rate_limited(&self, j: usize, retry_after: Option<Duration>) {
        self.mark_unhealthy(j);
        let rest = match retry_after {
            Some(hint) => clamp_retry_hint(hint, Duration::ZERO),
            None => {
                let streak = self.limited_streak[j].fetch_add(1, Ordering::Relaxed);
                Duration::from_millis(
                    RATE_LIMIT_PAUSE_MS
                        .saturating_mul(1 << streak.min(16))
                        .min(ENDPOINT_COOLDOWN_MS),
                )
            }
        };
        self.limited[j].store(now_millis() + rest.as_millis() as u64, Ordering::Relaxed);
        tracing::debug!(
            "rpc {} rate-limited us; not asking it again for {rest:?}",
            redact_url(&self.urls[j])
        );
    }

    /// The endpoints a call left out of `asked` because they are resting after a 429, as one failure,
    /// so a verdict drawn from the rest does not speak for the whole pool.
    fn resting(&self, asked: &[usize]) -> Option<anyhow::Error> {
        let resting: Vec<String> = (0..self.urls.len())
            .filter(|j| !asked.contains(j))
            .map(|j| redact_url(&self.urls[j]))
            .collect();
        (!resting.is_empty()).then(|| {
            anyhow::Error::new(ClassifiedError {
                class: FailureClass::RateLimited { retry_after: None },
                detail: format!("not asked while rate-limiting us: {}", resting.join(", ")),
            })
        })
    }

    /// Every endpoint is resting after a 429: wait for the soonest, then try only those now free. A
    /// rest is at most [`ENDPOINT_COOLDOWN_MS`], so a throttled pool slows the run and never spins,
    /// nor stops.
    async fn wait_out_rate_limits(&self) -> Vec<usize> {
        let mut order: Vec<usize> = (0..self.urls.len()).collect();
        order.sort_by_key(|&j| self.limited[j].load(Ordering::Relaxed));
        let wait = self.limited[order[0]]
            .load(Ordering::Relaxed)
            .saturating_sub(now_millis());
        let wait = Duration::from_millis(wait);
        tracing::warn!(
            "every RPC endpoint ({}) is rate-limiting us; waiting {wait:?} as asked before trying \
             again",
            self.urls.len()
        );
        retry_pause(wait).await;
        let now = now_millis();
        let free: Vec<usize> = order
            .iter()
            .copied()
            .filter(|&j| self.limited[j].load(Ordering::Relaxed) <= now)
            .collect();
        if free.is_empty() {
            order.truncate(1);
            return order;
        }
        free
    }

    pub fn mark_unhealthy(&self, j: usize) {
        self.health[j].store(now_millis() + ENDPOINT_COOLDOWN_MS, Ordering::Relaxed);
    }

    /// Ask an endpoint left unverified at startup which chain it is on, before it answers anything
    /// else (#2161). The wrong chain is refused as terminal, so it is cooled and never used.
    async fn admit(&self, j: usize) -> Result<()> {
        if !self.unverified[j].load(Ordering::Relaxed) {
            return Ok(());
        }
        let url = &self.urls[j];
        let v = self.call_one(url, "eth_chainId", &json!([]), None).await?;
        let got = parse_hex_u64(v.as_str().unwrap_or_default())
            .with_context(|| format!("unparseable eth_chainId from {}", redact_url(url)))?;
        let expected = self.chain.load(Ordering::Relaxed);
        if got != expected {
            return Err(anyhow::Error::new(ClassifiedError {
                class: FailureClass::Terminal,
                detail: format!(
                    "RPC endpoint {} is on chain {got}, but this nest indexes chain {expected}, so \
                     it serves nothing. Fix `rpc_urls`.",
                    redact_url(url)
                ),
            }));
        }
        self.unverified[j].store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Cool an endpoint down for the *terminal* interval and say so at `warn!` (RFC-0028 §3).
    ///
    /// The log level is the point as much as the interval. Endpoint failures are logged at `debug!`,
    /// which is right for a blip and wrong for "your credentials are rejected" - the latter is
    /// actionable, and an operator should not have to raise the log level to discover it.
    fn mark_terminal(&self, j: usize, method: &str, detail: &str) {
        let now = now_millis();
        // Calls in flight are refused together, and a cooling endpoint is still the last resort:
        // the first refusal starts the cool-down and says so, the rest fall inside it.
        let begun = self.terminal[j].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |t| {
            (t <= now).then_some(now + TERMINAL_COOLDOWN_MS)
        });
        let until = begun.map_or_else(|t| t, |_| now + TERMINAL_COOLDOWN_MS);
        self.health[j].fetch_max(until, Ordering::Relaxed);
        if begun.is_err() {
            return;
        }
        self.terminal_cooldowns[j].fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            "rpc {} refused us on {method} ({detail}) - not a transient failure; \
             cooling it down for {}s. Check the endpoint's credentials or plan.",
            redact_url(&self.urls[j]),
            TERMINAL_COOLDOWN_MS / 1000,
        );
    }

    /// Route a failed call to the right cooldown, per its classification. An unclassified error
    /// (nothing downcasts) is treated as transient, which is the pre-RFC-0028 behaviour.
    fn record_failure(&self, j: usize, method: &str, err: &anyhow::Error) {
        match class_of(err) {
            Some(FailureClass::Terminal) => self.mark_terminal(j, method, &err.to_string()),
            Some(FailureClass::RateLimited { retry_after }) => {
                self.mark_rate_limited(j, retry_after)
            }
            _ => {
                self.mark_unhealthy(j);
                tracing::debug!(
                    "rpc {} failed for {method}: {err:#}",
                    redact_url(&self.urls[j])
                );
            }
        }
    }

    /// Try endpoints in health order until one answers; a failed endpoint is put into cooldown, a
    /// successful one is cleared.
    /// One JSON-RPC call with failover. `pub(crate)` so `doctor` can ask arbitrary probe questions -
    /// it exists to interrogate an endpoint's limits, which is not expressible through the typed
    /// helpers.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.call_holding(method, params, None)
            .await
            .map(|(v, _)| v)
    }

    /// [`Self::call`], refusing a response body past `cap` bytes as too large, so the caller
    /// narrows the range rather than holding it whole.
    pub async fn call_capped(&self, method: &str, params: Value, cap: usize) -> Result<Value> {
        self.call_holding_capped(method, params, None, Some(cap))
            .await
            .map(|(v, _)| v)
    }

    /// [`Self::call`], preferring endpoints known to hold block `need`; also returns which endpoint
    /// answered.
    async fn call_holding(
        &self,
        method: &str,
        params: Value,
        need: Option<u64>,
    ) -> Result<(Value, usize)> {
        self.call_holding_capped(method, params, need, None).await
    }

    /// [`Self::call_holding`], refusing a response body past `body_cap` bytes as too large.
    async fn call_holding_capped(
        &self,
        method: &str,
        params: Value,
        need: Option<u64>,
        body_cap: Option<usize>,
    ) -> Result<(Value, usize)> {
        let mut failures = PoolFailures::default();
        let mut attempts = 0usize;
        let mut rate_limited = 0usize;
        let mut order = self.endpoint_order_holding(need);
        if order.is_empty() {
            order = self.wait_out_rate_limits().await;
        }
        for &j in &order {
            let url = &self.urls[j];
            self.requests.fetch_add(1, Ordering::Relaxed);
            self.http.inc_rpc();
            self.http.inc_rpc_method(method);
            attempts += 1;
            let t0 = crate::time::now();
            let answer = match self.admit(j).await {
                Ok(()) => self.call_one(url, method, &params, body_cap).await,
                Err(e) => Err(e),
            };
            match answer {
                Ok(v) => {
                    self.http
                        .observe_rpc(url, t0.elapsed(), false, attempts > 1);
                    self.mark_healthy(j);
                    return Ok((v, j));
                }
                Err(e) => {
                    self.http.observe_rpc(url, t0.elapsed(), true, attempts > 1);
                    if body_capped(&e) {
                        return Err(e);
                    }
                    if matches!(class_of(&e), Some(FailureClass::RateLimited { .. })) {
                        rate_limited += 1;
                    }
                    self.record_failure(j, method, &e);
                    failures.push(url, e);
                }
            }
        }
        failures.rested(self.resting(&order));
        Err(failures.verdict(attempts, rate_limited))
    }

    /// POST a raw JSON-RPC body (single object or a batch array) with the same health-ordered failover
    /// as `call`, returning the parsed response. Used for batch requests `call` can't express.
    async fn post_with_failover(&self, body: &Value) -> Result<Value> {
        self.post_with_failover_capped(body, None).await
    }

    /// [`Self::post_with_failover`], refusing a response body past `body_cap` bytes as too large.
    pub async fn post_with_failover_capped(
        &self,
        body: &Value,
        body_cap: Option<usize>,
    ) -> Result<Value> {
        let mut failures = PoolFailures::default();
        let mut attempts = 0usize;
        let mut rate_limited = 0usize;
        let mut order = self.endpoint_order();
        if order.is_empty() {
            order = self.wait_out_rate_limits().await;
        }
        for &j in &order {
            let url = &self.urls[j];
            self.requests.fetch_add(1, Ordering::Relaxed);
            self.http.inc_rpc();
            match body {
                Value::Array(items) => {
                    let mut counts: HashMap<&str, u64> = HashMap::new();
                    for item in items {
                        if let Some(m) = item.get("method").and_then(Value::as_str) {
                            *counts.entry(m).or_insert(0) += 1;
                        }
                    }
                    for (m, c) in counts {
                        self.http.inc_rpc_methods(m, c);
                    }
                }
                Value::Object(map) => {
                    if let Some(m) = map.get("method").and_then(Value::as_str) {
                        self.http.inc_rpc_method(m);
                    }
                }
                _ => {}
            }
            attempts += 1;
            let t0 = crate::time::now();
            let answer = match self.admit(j).await {
                Ok(()) => self.post_one(url, body, body_cap).await,
                Err(e) => Err(e),
            };
            match answer {
                Ok(v) => {
                    self.http
                        .observe_rpc(url, t0.elapsed(), false, attempts > 1);
                    self.mark_healthy(j);
                    return Ok(v);
                }
                Err(e) => {
                    self.http.observe_rpc(url, t0.elapsed(), true, attempts > 1);
                    if body_capped(&e) {
                        return Err(e);
                    }
                    if matches!(class_of(&e), Some(FailureClass::RateLimited { .. })) {
                        rate_limited += 1;
                    }
                    self.record_failure(j, "batch", &e);
                    failures.push(url, e);
                }
            }
        }
        failures.rested(self.resting(&order));
        Err(failures.verdict(attempts, rate_limited))
    }

    /// POST `body` and parse the response, attaching a [`FailureClass`] to any failure (RFC-0028 §3).
    ///
    /// Replaces the old `send().await?.error_for_status()?.json().await?` chain, which collapsed every
    /// failure mode into an indistinguishable `anyhow::Error` - so a bad API key and a momentary 503
    /// were handled identically, and the former was retried until someone noticed.
    async fn send_classified(
        &self,
        url: &str,
        body: &Value,
        body_cap: Option<usize>,
    ) -> Result<Value> {
        let classified = |class: FailureClass, detail: String| {
            anyhow::Error::new(ClassifiedError { class, detail })
        };
        let resp = self.http.post(url, body).await.map_err(|e| {
            // reqwest's own text for a timeout is "error sending request", which a caller narrowing
            // its range on a timeout cannot tell from a refused connection.
            let timed_out = if e.timed_out { "timed out: " } else { "" };
            classified(
                FailureClass::Transient,
                format!("transport error: {timed_out}{}", e.error),
            )
        })?;
        let status = resp.status();
        let status_line = resp.status_line();
        // Read `Retry-After` before the body consumes the response (#361). Seconds-form only: the
        // HTTP-date form needs a clock comparison to be meaningful, and no provider we have measured
        // sends it on a 429.
        let header_hint = resp.retry_after().as_deref().and_then(parse_retry_hint);
        if !(200..300).contains(&status) {
            // Read the body before classifying: a 413 can carry a suggested range, and the text is
            // what makes an otherwise opaque status actionable in the log.
            let text = resp.text().await.unwrap_or_default();
            // **The body is classified on a non-2xx too** (RFC-0029 §6a). It used to be inspected only
            // on a 2xx, on the assumption - measured, and correct for the endpoint it was measured
            // against - that providers signal an oversized range as HTTP 200 carrying a JSON-RPC error.
            // Alchemy returns the same refusal as **HTTP 400** with the error object in the body, so
            // every mechanism built for it was unreachable: the cap markers, and the provider's own
            // suggested range, which it names and we were discarding into a truncated log line.
            //
            // The body wins when it says something definite. `classify_status` alone cannot, because a
            // status code is a category and the body is the evidence.
            let class = match rpc_error_of(&text) {
                Some(e) => match classify_rpc_error(&e) {
                    // `Transient` from the body is the *absence* of a finding, not a finding - fall
                    // back to what the status implies rather than letting it overrule.
                    FailureClass::Transient => classify_status(status, &text),
                    definite => definite,
                },
                None => classify_status(status, &text),
            };
            // The body's own `try_again_in` wins - it is the more specific statement, and the one
            // Chainstack actually sends. `Retry-After` fills in when the body said nothing.
            let class = match class {
                FailureClass::RateLimited { retry_after: None } => FailureClass::RateLimited {
                    retry_after: header_hint,
                },
                other => other,
            };
            let mut detail = format!("HTTP {status_line}");
            if !text.is_empty() {
                let snippet: String = text.chars().take(300).collect();
                detail.push_str(&format!(": {snippet}"));
            }
            return Err(classified(class, detail));
        }
        let read_failed = |e: TransportError| {
            // **A body-read timeout is a size signal, not a transient blip** (RFC-0029 §6g). reqwest's
            // `.timeout()` covers streaming the body, so a response that is large *and* slow to read
            // fails here rather than at the status line - with the opaque text "error decoding response
            // body" and no cap marker anywhere in it.
            //
            // Measured on OBIB case 1 (2026-07-30): a 25,000-block window over LBTC returns a valid
            // 3.5 MB body in 2.6 s to `curl`, but under `--concurrency 8` with the timestamp fan-out
            // (§6c) competing for the same pool, the read exceeded the 20 s budget. Classified
            // `Transient`, it took the *bounded* speculative-split path (RFC-0028 §3b) instead of the
            // unbounded classified one, ran out of splits, retried five times at the same width, and
            // **aborted the backfill**. Same range, twice.
            //
            // This is slice 1's lesson in a second costume: `Transient` is the absence of a
            // classification, not a finding. Halving the range halves the bytes and the read time, so
            // narrowing is the only thing that can help - where retrying the identical width provably
            // cannot.
            //
            // A *syntax* error stays transient: garbage from a load balancer is no smaller in halves,
            // and calling it narrowable would split a dead endpoint down to single blocks.
            let class = if e.timed_out {
                FailureClass::Narrowable {
                    suggested: None,
                    escalated_from_rate_limit: false,
                }
            } else {
                FailureClass::Transient
            };
            classified(class, format!("malformed response: {}", e.error))
        };
        let Some(cap) = body_cap else {
            return resp.json().await.map_err(read_failed);
        };
        let mut resp = resp;
        let declared = resp.content_length().unwrap_or(0) as usize;
        if declared > cap {
            return Err(anyhow::Error::new(BodyCapped { cap }));
        }
        // Grown by doubling, a body near the cap left the allocator holding several times it.
        let mut body = Vec::with_capacity(declared);
        while let Some(chunk) = resp.chunk().await.map_err(read_failed)? {
            append_capped(&mut body, &chunk, cap)?;
        }
        serde_json::from_slice(&body)
            .map_err(|e| classified(FailureClass::Transient, format!("malformed response: {e}")))
    }

    /// Single-endpoint POST for tests that need the *classification* of a raw transport failure,
    /// before failover or the all-endpoints re-classification (`§92`) can rewrite it.
    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn post_one_for_test(&self, body: &Value) -> Result<Value> {
        let url = self.urls[0].clone();
        self.send_classified(&url, body, None).await
    }

    async fn post_one(&self, url: &str, body: &Value, body_cap: Option<usize>) -> Result<Value> {
        let resp: Value = self.send_classified(url, body, body_cap).await?;
        // A whole-batch rejection - e.g. a keyless endpoint answering HTTP 200 with
        // `{"error":{"message":"authenticate with an API key"}}` instead of the expected array - comes
        // back as a single object with a top-level `error`. Treat it as an endpoint failure so
        // `post_with_failover` cools it down and tries the next, exactly as `call_one` does for single
        // calls; otherwise the bad endpoint silently poisons the pool and the batch aborts with a
        // confusing non-error. (Per-item errors inside a normal array response stay the caller's to
        // handle.)
        if let Some(err) = error_of(&resp) {
            return Err(anyhow::Error::new(ClassifiedError {
                class: classify_rpc_error(err),
                detail: format!("rpc error (endpoint rejected the batch): {err}"),
            }));
        }
        Ok(resp)
    }

    async fn call_one(
        &self,
        url: &str,
        method: &str,
        params: &Value,
        body_cap: Option<usize>,
    ) -> Result<Value> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let resp: Value = self.send_classified(url, &body, body_cap).await?;
        if let Some(err) = error_of(&resp) {
            return Err(anyhow::Error::new(ClassifiedError {
                class: classify_rpc_error(err),
                detail: format!("rpc error: {err}"),
            }));
        }
        resp.get("result")
            .cloned()
            .ok_or_else(|| anyhow!("rpc response had no result"))
    }

    /// One call asked of every endpoint the pool would serve from, for a question whose answer must
    /// hold at all of them. An endpoint that fails is cooled down and left out, as the pool leaves it
    /// out of every other call; one in cooldown is asked only while nothing healthy has answered. Fails
    /// only when no endpoint answers (#2113).
    pub async fn call_each(&self, method: &str, params: Value) -> Result<Vec<Value>> {
        let mut answers = Vec::with_capacity(self.urls.len());
        let mut failed = None;
        self.visit_each(method, params, &mut |a| match a {
            Ok(v) => answers.push(v),
            Err(e) => failed = Some(e),
        })
        .await;
        match failed {
            Some(e) => Err(e),
            None => Ok(answers),
        }
    }

    /// [`Self::call_each`], handing each answer to `seen` as it arrives, so one answer is acted on
    /// before the next endpoint is awaited, and is not lost to its failure or a timeout. When no
    /// endpoint answers, `seen` gets the pool's failure instead.
    pub async fn visit_each(
        &self,
        method: &str,
        params: Value,
        seen: &mut (dyn FnMut(Result<Value>) + Send),
    ) {
        let mut failures = PoolFailures::default();
        let (mut attempts, mut rate_limited, mut answered) = (0usize, 0usize, false);
        let mut order = self.endpoint_order_by_health();
        if order.is_empty() {
            order = self.wait_out_rate_limits().await;
        }
        for &j in &order {
            if answered && self.health[j].load(Ordering::Relaxed) > now_millis() {
                continue;
            }
            let url = &self.urls[j];
            attempts += 1;
            let answer = match self.admit(j).await {
                Ok(()) => self.call_one(url, method, &params, None).await,
                Err(e) => Err(e),
            };
            match answer {
                Ok(v) => {
                    self.mark_healthy(j);
                    answered = true;
                    seen(Ok(v));
                }
                Err(e) => {
                    if matches!(class_of(&e), Some(FailureClass::RateLimited { .. })) {
                        rate_limited += 1;
                    }
                    self.record_failure(j, method, &e);
                    failures.push(url, e.context(format!("{method} at {}", redact_url(url))));
                }
            }
        }
        if !answered {
            failures.rested(self.resting(&order));
            seen(Err(failures.verdict(attempts, rate_limited)));
        }
    }

    pub async fn block_number(&self) -> Result<u64> {
        let (result, j) = self
            .call_holding("eth_blockNumber", json!([]), None)
            .await?;
        let head = parse_hex_u64(result.as_str().unwrap_or_default())?;
        self.heads[j].fetch_max(head, Ordering::Relaxed);
        Ok(head)
    }

    /// `eth_chainId`, once, with the same failover as any other call. Used to identify a chain
    /// `init` has no registry entry for: the caller supplied `--rpc`, so this is the one round trip
    /// standing between "unknown chain name" and a working nest (see `chains::resolve`).
    pub async fn chain_id(&self) -> Result<u64> {
        let result = self.call("eth_chainId", json!([])).await?;
        parse_hex_u64(result.as_str().unwrap_or_default())
    }

    /// Check **every** endpoint reports `expected` from `eth_chainId`, once, at startup (issue #150).
    ///
    /// A wrong-network endpoint in the pool is uniquely nasty: failover makes it *look* like a
    /// redundancy win while it quietly answers `eth_getBlockByNumber` for a chain we are not indexing.
    /// Every block hash it returns then mismatches our checkpoints, so `detect_reorg` walks the entire
    /// checkpoint history looking for a common ancestor it can never find. For an established nest the
    /// sealed-watermark bail contains the damage; a *fresh* nest, with nothing sealed, would happily
    /// roll itself back towards genesis.
    ///
    /// The per-endpoint loop is the point - `call` would failover past the bad one and report success.
    ///
    /// A **mismatch is fatal**: it is a configuration error that silently corrupts, and the operator
    /// must fix it. An endpoint that is merely *unreachable* is not - it is warned about and left in
    /// the pool, because being offline at startup is a normal condition this indexer tolerates and the
    /// existing health/cooldown machinery already handles it.
    pub async fn verify_chain_ids(&self, expected: u64) -> Result<()> {
        // Checked CONCURRENTLY, with a short deadline of its own. This runs before the first block is
        // fetched, so it sits directly on time-to-first-index: done sequentially at the client's 20 s
        // timeout, a default pool with a couple of dead endpoints (mainnet ships four) delayed the start
        // of indexing by over a minute - a regression against the "<2 minutes to first indexed query"
        // promise, and one that only shows up when a public endpoint is having a bad day. Concurrent +
        // 2 s bounds the whole check at ~2 s no matter how many endpoints are configured or dead.
        const VERIFY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
        self.chain.store(expected, Ordering::Relaxed);
        let checks = self.urls.iter().enumerate().map(|(j, url)| async move {
            self.requests.fetch_add(1, Ordering::Relaxed);
            self.http.inc_rpc();
            self.http.inc_rpc_method("eth_chainId");
            let r = crate::time::timeout(
                VERIFY_TIMEOUT,
                self.call_one(url, "eth_chainId", &json!([]), None),
            )
            .await;
            (j, url, r)
        });
        for (j, url, outcome) in futures::future::join_all(checks).await {
            match outcome {
                Ok(Ok(v)) => {
                    let got = parse_hex_u64(v.as_str().unwrap_or_default()).with_context(|| {
                        format!("unparseable eth_chainId from {}", redact_url(url))
                    })?;
                    if got != expected {
                        bail!(
                            "RPC endpoint {} is on chain {got}, but this nest indexes chain {expected} \
                             - indexing against a mixed-chain endpoint pool silently corrupts state \
                             (every block hash mismatches, and a fresh nest would roll back towards \
                             genesis). Fix `rpc_urls`.",
                            redact_url(url)
                        );
                    }
                    self.mark_healthy(j);
                }
                // Unreachable or slow now ≠ wrong chain. Leave it in the pool, cooled so the first real
                // call does not wait out the client's 20 s timeout on it (#1885), and unverified, so
                // `admit` asks its chain again before it answers anything else (#2161).
                Ok(Err(e)) => {
                    self.mark_unhealthy(j);
                    self.unverified[j].store(true, Ordering::Relaxed);
                    tracing::warn!(
                        "could not verify chain id of {} at startup ({e:#}) - leaving it in the pool",
                        redact_url(url)
                    )
                }
                Err(_) => {
                    self.mark_unhealthy(j);
                    self.unverified[j].store(true, Ordering::Relaxed);
                    tracing::warn!(
                        "chain id check for {} timed out after {}s - leaving it in the pool",
                        redact_url(url),
                        VERIFY_TIMEOUT.as_secs()
                    )
                }
            }
        }
        Ok(())
    }

    /// The pool's endpoints, primaries first, as scheme and host only.
    pub fn redacted_urls(&self) -> Vec<String> {
        self.urls.iter().map(|u| redact_url(u)).collect()
    }

    /// After [`Self::verify_chain_ids`], whether each endpoint, in pool order, answered with the chain.
    pub fn chain_id_answered(&self) -> Vec<bool> {
        self.unverified
            .iter()
            .map(|u| !u.load(Ordering::Relaxed))
            .collect()
    }

    /// Wait up to `deadline` for any endpoint to answer an HTTP request at all, whatever it says, and
    /// otherwise fail naming every endpoint (#2310). For a one-shot command: `dev` waits forever
    /// instead, because it is serving `/ready` meanwhile (#510).
    pub async fn await_an_answer(&self, deadline: Duration) -> Result<()> {
        let until = crate::time::deadline_now() + deadline;
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []});
        let mut warned = false;
        loop {
            let probes = self
                .urls
                .iter()
                .map(|url| crate::time::timeout_at(until, self.http.post(url, &body)));
            let mut failures = Vec::with_capacity(self.urls.len());
            for (url, outcome) in self
                .urls
                .iter()
                .zip(futures::future::join_all(probes).await)
            {
                match outcome {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(e)) => failures.push(format!("{} ({:#})", redact_url(url), e.error)),
                    Err(_) => failures.push(format!(
                        "{} (no answer within {deadline:?})",
                        redact_url(url)
                    )),
                }
            }
            let left = until.saturating_duration_since(crate::time::deadline_now());
            if left.is_zero() {
                bail!(
                    "no RPC endpoint answered in {deadline:?}: {}",
                    failures.join(", ")
                );
            }
            if !warned {
                tracing::warn!(
                    "no RPC endpoint answers yet ({}); giving up after {deadline:?}",
                    failures.join(", ")
                );
                warned = true;
            }
            retry_pause(left.min(Duration::from_secs(2))).await;
        }
    }

    /// A storage slot's value at `address` (latest block) - used to read the EIP-1967 proxy slot.
    pub async fn get_storage_at(&self, address: &str, slot: &str) -> Result<String> {
        let result = self
            .call("eth_getStorageAt", json!([address, slot, "latest"]))
            .await?;
        Ok(result.as_str().unwrap_or("0x0").to_string())
    }

    /// A read-only `eth_call` at latest block: send `data` (a selector + args) to `to`, returning the
    /// raw hex result. Used at init to ask a beacon proxy's beacon for its `implementation()`; never on
    /// the ingest path.
    /// `eth_call` at **`latest`** - for one-shot, out-of-band reads only.
    ///
    /// **Never use this in the data path.** `latest` is not re-executable: the same call answers
    /// differently tomorrow, so anything it produced could not be re-derived and would break the
    /// determinism non-negotiable. RFC-0023 §3 is explicit about it. Its legitimate users are proxy
    /// detection at `init` and the immutable-metadata fetch (`decimals`/`symbol`/`name`, which by
    /// definition cannot change). For anything that gets stored, use [`RpcClient::eth_call_at`].
    pub async fn eth_call(&self, to: &str, data: &str) -> Result<String> {
        let result = self
            .call("eth_call", json!([{ "to": to, "data": data }, "latest"]))
            .await?;
        Ok(result.as_str().unwrap_or("0x").to_string())
    }

    /// `eth_call` **pinned to a historical block** - the tier-3 data-path primitive (RFC-0023 §3).
    ///
    /// Determinism comes from the pin: `result = f(code, storage, block, calldata)`, so re-executing
    /// the same call at the same block on any machine, at any later date, returns the same bytes. That
    /// is what makes a call result safe to seal into an immutable segment and content-address.
    ///
    /// Needs an archive endpoint for blocks past the pruning window - which is the *only* thing tier 3
    /// asks an operator for, and only for the irreducible residue the tier-1 recipes cannot derive.
    pub async fn eth_call_at(&self, to: &str, data: &str, block: u64) -> Result<String> {
        let result = self
            .call(
                "eth_call",
                json!([{ "to": to, "data": data }, format!("0x{block:x}")]),
            )
            .await?;
        Ok(result.as_str().unwrap_or("0x").to_string())
    }

    /// Many pinned calls at one block in a single JSON-RPC batch (RFC-0023 §3: "batched, the same
    /// batched-boundary discipline as log extraction").
    ///
    /// Returns results **positionally**, so a caller can zip them back against its declarations. A
    /// call that explicitly reverted yields `None` in that slot rather than failing
    /// the batch: a revert is a legitimate answer about chain state at that block (the function may not
    /// have existed yet), and collapsing it into a whole-batch error would make one unlucky
    /// declaration poison every other call at the same block.
    ///
    /// Like the timestamp batch, this **narrows on a size failure instead of retrying the same width**
    /// - see RFC-0029 §4c, where the same defect appeared three times in a row.
    pub fn eth_call_batch_at<'a>(
        &'a self,
        calls: &'a [(String, String)],
        block: u64,
    ) -> CallBatchFuture<'a> {
        self.eth_call_batch_with_selector(calls, json!(format!("0x{block:x}")))
    }

    /// EIP-1898 pins state to the same block hash as the source logs. A provider declining the
    /// selector is an error, never a reason to retry by number.
    pub async fn eth_call_batch_at_hash(
        &self,
        calls: &[(String, String)],
        hash: &str,
    ) -> Result<Vec<Option<String>>> {
        if hash.len() != 66
            || !hash.starts_with("0x")
            || !hash[2..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            bail!("canonical eth_call requires a 32-byte block hash");
        }
        self.eth_call_batch_with_selector(
            calls,
            json!({"blockHash":hash.to_ascii_lowercase(),"requireCanonical":true}),
        )
        .await
    }

    fn eth_call_batch_with_selector<'a>(
        &'a self,
        calls: &'a [(String, String)],
        selector: Value,
    ) -> CallBatchFuture<'a> {
        Box::pin(async move {
            if calls.is_empty() {
                return Ok(Vec::new());
            }
            match self.eth_call_batch_once(calls, &selector).await {
                Ok(v) => Ok(v),
                Err(e) if calls.len() > 1 && is_result_too_large(&e) => {
                    let mid = calls.len() / 2;
                    let (a, b) = calls.split_at(mid);
                    let mut out = self
                        .eth_call_batch_with_selector(a, selector.clone())
                        .await?;
                    out.extend(self.eth_call_batch_with_selector(b, selector).await?);
                    Ok(out)
                }
                Err(e) => Err(e),
            }
        })
    }

    async fn eth_call_batch_once(
        &self,
        calls: &[(String, String)],
        selector: &Value,
    ) -> Result<Vec<Option<String>>> {
        let batch: Vec<Value> = calls
            .iter()
            .enumerate()
            .map(|(i, (to, data))| {
                json!({ "jsonrpc": "2.0", "id": i, "method": "eth_call",
                        "params": [{ "to": to, "data": data }, selector] })
            })
            .collect();
        let cap = (calls.len() > 1).then_some(self.batch_body_cap);
        let resp = self
            .post_with_failover_capped(&Value::Array(batch), cap)
            .await?;
        decode_call_batch(&resp, calls.len())
    }

    /// Send a raw JSON-RPC batch and return the raw response. For `doctor` only: measuring the
    /// endpoint's batch ceiling means sending batches of chosen sizes, which no typed helper offers.
    pub async fn raw_batch(&self, body: &Value) -> Result<Value> {
        self.post_with_failover(body).await
    }

    /// Contract bytecode at `address` as of `block`. `"0x"` (empty) means not yet deployed.
    pub async fn get_code(&self, address: &str, block: u64) -> Result<String> {
        let result = self
            .call("eth_getCode", json!([address, format!("0x{block:x}")]))
            .await?;
        Ok(result.as_str().unwrap_or("0x").to_string())
    }

    /// Unix timestamps (seconds) for the given block numbers, fetched in a single JSON-RPC batch so
    /// even a dense window costs one round-trip.
    ///
    /// A *whole-batch request failure* is retried a few times and then returned as `Err`, and a
    /// partial answer is an `Err` too (COR-3 below): either would otherwise bake
    /// `block_timestamp = 0` into a permanent segment from a transient blip.
    /// Forget cached timestamps for blocks above `block` - **called on every reorg**.
    ///
    /// A block number is not a block identity. When the chain reorganises, the block at a given height
    /// is replaced by a different one with a different timestamp, and `block_timestamp` is a sealed
    /// column whose value feeds the segment's content address. Serving the pre-reorg timestamp for a
    /// re-indexed block would seal a value that a re-execution against the canonical chain would not
    /// reproduce - a determinism break, and a silent one.
    ///
    /// Entries at or below the ancestor are kept: those blocks are common to both chains, which is what
    /// makes them the ancestor.
    pub fn forget_timestamps_above(&self, block: u64) {
        let mut cache = self.timestamps.lock().unwrap_or_else(|e| e.into_inner());
        cache.retain(|&b, _| b <= block);
    }

    pub async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        if blocks.is_empty() {
            return Ok(HashMap::new());
        }
        // Fetch in bounded sub-batches (see `MAX_TIMESTAMP_BATCH`) and merge, so a dense window whose
        // distinct-block count exceeds a provider's batch cap doesn't fail wholesale.
        //
        // The sub-batches go out **concurrently** (RFC-0029 §6c). They were sequential, which on a
        // range with many distinct blocks made timestamp acquisition the dominant cost of a backfill -
        // §4 measured it at roughly 85% of wall clock - while the window fan-out beside it was already
        // concurrent.
        //
        // The cap is deliberately conservative rather than "as wide as the window fan-out". RFC-0029
        // §6c records 10-way producing 2 failures in 10 on the measured endpoint (`IncompleteRead`), so
        // widening this trades a real completion risk for throughput on a path where a partial response
        // is *already* an error by COR-3 below. Four is chosen to be comfortably under that observed
        // cliff; the RFC's own note that this "should be adaptive rather than a constant we guess"
        // stands, and is deliberately not attempted here - an adaptive controller needs a failure
        // signal to steer by, and inventing one alongside a concurrency change would make a regression
        // impossible to attribute.
        let requested = blocks.len();
        // Serve what we already know. On a split-and-retry this is most of the range: the window that
        // failed had its timestamps fetched, and the halves ask for the same blocks again.
        let mut out = HashMap::new();
        let mut missing: Vec<u64> = Vec::new();
        {
            let cache = self.timestamps.lock().unwrap_or_else(|e| e.into_inner());
            for &b in blocks {
                match cache.get(&b) {
                    Some(&ts) => {
                        out.insert(b, ts);
                    }
                    None => missing.push(b),
                }
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        let blocks = &missing[..];

        const TIMESTAMP_FANOUT: usize = 4;
        use futures::stream::StreamExt;
        // Futures built eagerly rather than mapped inside the stream: the borrow of each chunk has to
        // outlive the stream, and a closure producing them cannot express that.
        let futures: Vec<_> = blocks
            .chunks(self.header_width.load(Ordering::Relaxed))
            .map(|c| self.fetch_timestamp_batch(c, false, true))
            .collect();
        let mut results = futures::stream::iter(futures).buffered(TIMESTAMP_FANOUT);
        // The batch returns whole headers (RFC-0036 reuses this path for the `blocks` table); this
        // caller wants one field. Projecting here rather than widening the cache keeps the cache a
        // `block -> u64` map: holding headers for `TIMESTAMP_CACHE_MAX` blocks instead would be a
        // footprint regression on the hot path, against a 2 GB per-cursor budget. Each batch is
        // projected as it lands: collected first, a dense window held every header, transaction
        // hashes and all, at once (#2227).
        while let Some(r) = results.next().await {
            for (b, header) in r? {
                if let Some(ts) = header
                    .get("timestamp")
                    .and_then(Value::as_str)
                    .and_then(|s| parse_hex_u64(s).ok())
                {
                    out.insert(b, ts);
                }
            }
        }
        {
            let mut cache = self.timestamps.lock().unwrap_or_else(|e| e.into_inner());
            // Bounded by clearing rather than evicting an LRU: this is a backfill-shaped access pattern
            // that moves forward through the chain, so the oldest entries are the least likely to be
            // asked for again and a precise eviction order buys nothing over starting fresh.
            if cache.len() + out.len() > TIMESTAMP_CACHE_MAX {
                cache.clear();
            }
            for (&b, &ts) in &out {
                cache.insert(b, ts);
            }
        }
        // COR-3: a *partial* response (endpoint answered but a load-balanced/archive-vs-full split
        // returned `null` for some block) must be an error, not a partial map - else the caller defaults
        // the missing block's `block_timestamp` to 0 and *seals it permanently*, breaking determinism
        // (a re-run against a healthy endpoint yields a different timestamp → different content hash).
        // Erroring makes the seal path retry the whole window, exactly like a total failure.
        if out.len() != requested {
            let missing = requested - out.len();
            bail!(
                "block_timestamps: {missing}/{} block(s) missing from the RPC response - refusing a \
                 partial map (would seal block_timestamp=0)",
                requested
            );
        }
        Ok(out)
    }

    /// Full block headers for `blocks` (RFC-0036 §4.2), over the **same** batched, narrowing,
    /// failover-capable path as [`RpcClient::block_timestamps`].
    ///
    /// Deliberately not a second fetcher. That path took three attempts to get right - RFC-0028 gave
    /// `getLogs` a narrowing retry, RFC-0029 §6h found the timestamp batch still lacked one and was
    /// reissuing an over-large batch five times at identical width before killing the backfill. A
    /// parallel header fetcher would have to relearn all of it, and would drift.
    ///
    /// Unlike `block_timestamps` this does **not** consult or fill the timestamp cache: headers are
    /// consumed once, as rows, and caching them would hold `TIMESTAMP_CACHE_MAX` full headers against
    /// a 2 GB per-cursor budget to serve a read that never repeats.
    pub async fn block_headers(&self, blocks: &[u64]) -> Result<HashMap<u64, Value>> {
        self.blocks_with(blocks, false).await
    }

    /// Full blocks **including transaction bodies** (`eth_getBlockByNumber(b, true)`).
    ///
    /// The source for top-level call decoding (RFC-0038 §5). It is ordinary RPC - the same method
    /// `block_headers` already calls, with the one flag flipped - which is why top-level calls are
    /// *not* node-gated the way internal calls are: only the internal call tree needs `debug_*`.
    ///
    /// Shares `blocks_with`'s pacing and partial-response handling rather than copying it: a
    /// rate-limited provider answers HTTP 200 with some items filled and the rest carrying a per-item
    /// 429, and that logic took several findings to get right.
    pub async fn block_bodies(&self, blocks: &[u64]) -> Result<HashMap<u64, Value>> {
        self.blocks_with(blocks, true).await
    }

    async fn blocks_with(&self, blocks: &[u64], full: bool) -> Result<HashMap<u64, Value>> {
        if blocks.is_empty() {
            return Ok(HashMap::new());
        }
        // **Serial, deliberately.** The two fan-outs compose *multiplicatively* and nothing bounds the
        // product - RFC-0029 §6h's finding, rediscovered here the hard way. At `--concurrency 8` a
        // fan-out of 4 means 32 concurrent multi-hundred-block batches sharing one connection pool,
        // and OBIB case 3 measured that as the provider returning a 429 for every item in a batch.
        // Serial here leaves the window fan-out as the only multiplier, which is the one the adaptive
        // controller can see and steer.
        const HEADER_FANOUT: usize = 1;
        // Re-ask only for what is missing, with a widening pause between rounds.
        //
        // A rate-limited provider answers HTTP 200 with *some* items filled and the rest carrying a
        // per-item 429, so a partial response is the **normal** shape under load rather than an
        // anomaly. Failing the batch throws away the headers that did arrive and re-fetches them on
        // the retry, which costs more of the very budget that ran out. Narrowing is not the answer
        // either: `batch_is_narrowable` refuses to split under a rate limit because splitting doubles
        // the request count in exactly the wrong direction.
        //
        // So: keep what arrived, pause, ask for the remainder. One header per block is unavoidable
        // work - 100,001 of them for OBIB case 3 - and pacing is the only lever that helps.
        const ROUNDS: usize = 8;
        let mut out: HashMap<u64, Value> = HashMap::new();
        let mut missing: Vec<u64> = blocks.to_vec();
        for round in 0..ROUNDS {
            use futures::stream::StreamExt;
            let futures: Vec<_> = missing
                .chunks(self.header_width.load(Ordering::Relaxed))
                .map(|c| self.fetch_timestamp_batch(c, full, true))
                .collect();
            let results: Vec<Result<HashMap<u64, Value>>> = futures::stream::iter(futures)
                .buffered(HEADER_FANOUT)
                .collect()
                .await;
            let mut last_err = None;
            for r in results {
                match r {
                    Ok(m) => out.extend(m),
                    // A whole-batch failure is kept and only raised if the rounds run out - the other
                    // batches in this round may still have made progress worth keeping.
                    Err(e) => last_err = Some(e),
                }
            }
            missing.retain(|b| !out.contains_key(b));
            if missing.is_empty() {
                return Ok(out);
            }
            if round + 1 == ROUNDS {
                if let Some(e) = last_err {
                    return Err(e.context(format!(
                        "block_headers: {} of {} header(s) still missing after {ROUNDS} rounds",
                        missing.len(),
                        blocks.len()
                    )));
                }
                break;
            }
            // Linear rather than exponential: a compute-units-per-second budget refills on a clock, so
            // waiting longer than the window buys nothing, and the first pause should be short enough
            // that an isolated blip costs milliseconds.
            //
            // …but a guess is strictly worse than the number the provider just handed us (#361), and
            // OBIB case 3 is 100,001 of these calls, where pacing is the only lever that helps. If the
            // failure carried `try_again_in` or `Retry-After`, honour it instead.
            let linear = Duration::from_millis(250 * (round as u64 + 1));
            let pause = match last_err.as_ref().and_then(class_of) {
                Some(FailureClass::RateLimited {
                    retry_after: Some(hint),
                }) => clamp_retry_hint(hint, linear),
                // No hint, or a failure that was not a rate limit: our own pacing stands.
                _ => linear,
            };
            retry_pause(pause).await;
        }
        // Same COR-3 reasoning as `block_timestamps`: a partial map must be an error rather than a
        // short map. A missing header would seal a *missing block row*, and "no row" is
        // indistinguishable from "the chain had no block there" to whoever queries the table later.
        bail!(
            "block_headers: {}/{} header(s) missing after {ROUNDS} rounds - refusing a partial map \
             (would seal a gap in the blocks table). The provider is rate-limiting a workload that \
             needs one header per block; lower --concurrency or use an endpoint with more headroom.",
            missing.len(),
            blocks.len()
        )
    }

    /// One bounded `eth_getBlockByNumber` batch → `{block: timestamp}` (may be partial if the endpoint
    /// omitted blocks; the caller's total-count check turns that into an error). A whole-batch request
    /// failure is retried a few times before erroring.
    /// One timestamp sub-batch, **narrowing on a size failure instead of retrying the same width**
    /// (RFC-0029 §6h).
    ///
    /// This is the third place the same defect appeared, and the pattern is worth stating rather than
    /// patched a fourth time: **a batched RPC call needs a narrowing path, not just a retry path.**
    /// `getLogs` has had one since RFC-0028; this did not, so a batch whose response body was too slow
    /// to read inside the request timeout was reissued at identical size five times and then killed
    /// the backfill.
    ///
    /// Measured on OBIB case 1 (2026-07-30): `MAX_TIMESTAMP_BATCH` is 200 blocks and `TIMESTAMP_FANOUT`
    /// is 4 - **per window**. At `--concurrency 8` that is up to 32 concurrent multi-megabyte batch
    /// responses sharing one connection pool and one timeout budget. The two fan-outs compose
    /// *multiplicatively* and nothing bounds the product, and §6f made it sharper by growing windows to
    /// 100,000 blocks, so each window now covers far more distinct blocks than when the batch size was
    /// chosen. Halving on failure is what adapts to that without having to predict it.
    pub fn fetch_timestamp_batch<'a>(
        &'a self,
        blocks: &'a [u64],
        full: bool,
        // When true, the top-level split's two halves run concurrently (`tokio::try_join!`).
        // Recursive calls pass false: one level of parallelism bounds the concurrency burst at
        // `TIMESTAMP_FANOUT` × 2 rather than the exponential blowup a fully-parallel tree would
        // produce. #656: a 200→1 descent is ~403 serial RTTs; one parallel split makes it ~202.
        parallel: bool,
    ) -> TimestampBatchFuture<'a> {
        // The descent starts here, so this width is the one every level below reports as its origin.
        self.fetch_timestamp_batch_from(blocks, full, blocks.len(), parallel)
    }

    /// The recursive half of [`Self::fetch_timestamp_batch`], carrying `entered_at` - the width the
    /// *caller's* chunk started at, unchanged all the way down.
    ///
    /// It exists for issue #656. A backfill reported six storms of "every item in a **1**-block
    /// `eth_getBlockByNumber` batch returned an error", and the fix depends on which of two stories
    /// produced that line - but the line cannot tell them apart:
    ///
    /// - a **descent**, 200 → 100 → … → 1, which by [`batch_is_narrowable`] can only have happened if
    ///   every level above classified as something *other* than `RateLimited`; or
    /// - a **trailing chunk** that was one block wide to begin with, because `.chunks(200)` divides a
    ///   block list of arbitrary length, in which case no level was classified at all and the class
    ///   could equally have been a rate limit.
    ///
    /// Reading the descent backwards off the final width was the tempting shortcut and it is unsound
    /// for exactly that second reason. So the width is carried rather than inferred, and the class is
    /// logged at each level instead of being reconstructed afterwards.
    fn fetch_timestamp_batch_from<'a>(
        &'a self,
        blocks: &'a [u64],
        full: bool,
        entered_at: usize,
        parallel: bool,
    ) -> TimestampBatchFuture<'a> {
        Box::pin(async move {
            match self
                .fetch_timestamp_batch_once(blocks, full, entered_at)
                .await
            {
                Ok(v) => Ok(v),
                // A single block that still fails is a real failure - there is nothing left to halve,
                // and recursing further would spin on a dead endpoint (the failure RFC-0028 avoided).
                Err(e) if blocks.len() > 1 && batch_is_narrowable(&e) => {
                    let mid = blocks.len() / 2;
                    if body_capped(&e) && !full {
                        self.header_width.fetch_min(mid, Ordering::Relaxed);
                    }
                    let class = class_label(class_of(&e).as_ref());
                    // The classifier's decision at *this* level, named. #656 asks which class drove
                    // the descent, and that is answerable only if each level says so on its way past.
                    //
                    // **One `warn` per descent, not one per level**, and the distinction is the whole
                    // point of #656: a descent is up to 8 levels, `TIMESTAMP_FANOUT` runs 4 chunks at
                    // once, and a window holds thousands of chunks - so a `warn` at every level turns
                    // one degraded endpoint into a log flood. Filing a noise complaint by making more
                    // noise would be a poor joke. The top of the descent is the line an operator needs
                    // (it carries the class and the width the endpoint actually refused); the levels
                    // below it are detail for whoever turns `debug` on.
                    if blocks.len() == entered_at {
                        tracing::warn!(
                            "timestamp batch narrowing from {} blocks (class={}): {e:#}",
                            blocks.len(),
                            class,
                        );
                    } else {
                        tracing::debug!(
                            "timestamp batch narrowing: {} -> {} blocks (entered at {}, class={}): {e:#}",
                            blocks.len(),
                            mid,
                            entered_at,
                            class,
                        );
                    }
                    let (a, b) = blocks.split_at(mid);
                    if parallel {
                        let (mut out, rest) = tokio::try_join!(
                            self.fetch_timestamp_batch_from(a, full, entered_at, false),
                            self.fetch_timestamp_batch_from(b, full, entered_at, false),
                        )?;
                        out.extend(rest);
                        Ok(out)
                    } else {
                        let mut out = self
                            .fetch_timestamp_batch_from(a, full, entered_at, false)
                            .await?;
                        out.extend(
                            self.fetch_timestamp_batch_from(b, full, entered_at, false)
                                .await?,
                        );
                        Ok(out)
                    }
                }
                Err(e) => Err(e),
            }
        })
    }

    async fn fetch_timestamp_batch_once(
        &self,
        blocks: &[u64],
        full: bool,
        entered_at: usize,
    ) -> Result<HashMap<u64, Value>> {
        let batch: Vec<Value> = blocks
            .iter()
            .enumerate()
            .map(|(i, b)| {
                json!({ "jsonrpc": "2.0", "id": i, "method": "eth_getBlockByNumber",
                        "params": [format!("0x{b:x}"), full] })
            })
            .collect();
        let body = Value::Array(batch);
        let mut resp = None;
        let mut last_err = None;
        for attempt in 0..TIMESTAMP_ATTEMPTS {
            let cap = (blocks.len() > 1).then_some(self.batch_body_cap);
            match self.post_with_failover_capped(&body, cap).await {
                Ok(r) => {
                    resp = Some(r);
                    break;
                }
                Err(e) if body_capped(&e) => return Err(e),
                Err(e) => {
                    tracing::debug!("block_timestamps attempt {} failed: {e:#}", attempt + 1);
                    // Same as `block_headers` (#361): prefer the provider's own number to our guess.
                    let own_pacing = Duration::from_millis(200 * (attempt as u64 + 1));
                    let pause = match class_of(&e) {
                        Some(FailureClass::RateLimited {
                            retry_after: Some(hint),
                        }) => clamp_retry_hint(hint, own_pacing),
                        _ => own_pacing,
                    };
                    last_err = Some(e);
                    retry_pause(pause).await;
                }
            }
        }
        let mut resp = match resp {
            Some(r) => r,
            None => {
                return Err(last_err
                    .unwrap()
                    .context("block_timestamps batch failed after retries"))
            }
        };
        let mut out = HashMap::new();
        let mut first_item_error: Option<FailureClass> = None;
        for item in resp.as_array_mut().into_iter().flatten() {
            let Some(idx) = item.get("id").and_then(Value::as_u64) else {
                continue;
            };
            let Some(&block) = blocks.get(idx as usize) else {
                continue;
            };
            // The whole header, not just `timestamp`: `block_timestamps` projects the field it
            // needs, and RFC-0036's `blocks` table keeps the rest. A `null` result (archive-vs-full
            // load-balancer split) is skipped, and the caller's total-count check turns that into an
            // error rather than a partial map - see COR-3 above.
            let result = item.get_mut("result").map(Value::take);
            if let Some(mut result) = result.filter(|v| !v.is_null()) {
                if !full {
                    nuthatch_decode::registry::count_header_transactions(&mut result);
                }
                out.insert(block, result);
            } else if let Some(err) = error_of(item) {
                // **A per-item error inside an HTTP 200 batch.** `post_one` only rejects a *top-level*
                // error, so these are invisible to the failure classifier: the batch "succeeded", the
                // items are silently dropped, and the caller's count check reports
                // `N/M block(s) missing from the RPC response` - true, but it names the symptom and
                // hides the cause.
                //
                // Found on OBIB case 3 (RFC-0036), where every item of an 800-block batch came back
                // `{"error":{"code":429,"message":"Your app has exceeded its compute units per second
                // capacity"}}`. It read as a broken endpoint for three diagnoses running, and it
                // matters beyond this call: `block_timestamps` shares this parse, so a throttled
                // timestamp batch has always reported COR-3's "blocks missing" rather than "you are
                // being rate limited" - and then retried at the same width on a short backoff, which
                // is the one response guaranteed not to help.
                first_item_error.get_or_insert_with(|| classify_rpc_error(err));
            }
        }
        // Surface the item-level class so the retry path can act on it: a rate limit needs a longer
        // backoff and never a narrower batch (`batch_is_narrowable` refuses to split under one,
        // because splitting doubles the request count in the wrong direction).
        if out.is_empty() {
            if let Some(class) = first_item_error {
                // #656: the field report of this exact line named a width and nothing else, which left
                // its own cause unreadable - a 1-block batch is either the floor of a descent or a
                // trailing `.chunks(200)` remainder, and only the first says anything about the class
                // of the levels above. Both the class and the entry width are on the line now, so the
                // next occurrence answers the question the last one only managed to raise.
                let descended = entered_at != blocks.len();
                return Err(anyhow::Error::new(ClassifiedError {
                    detail: format!(
                        "every item in a {}-block eth_getBlockByNumber batch returned an error \
                         (per-item, inside an HTTP 200 response; class={}, {})",
                        blocks.len(),
                        class_label(Some(&class)),
                        if descended {
                            format!("narrowed down from {entered_at}")
                        } else {
                            "not narrowed - this was the width requested".to_string()
                        },
                    ),
                    class,
                }));
            }
        }
        Ok(out)
    }

    /// The node's `finalized` block number (L1-aware on an L2 like Arbitrum), or None if the
    /// endpoint doesn't serve the `finalized` tag. Used by the `FinalizedTag` finality policy.
    pub async fn finalized_block(&self) -> Result<Option<u64>> {
        let result = self
            .call("eth_getBlockByNumber", json!(["finalized", false]))
            .await?;
        Ok(result
            .get("number")
            .and_then(Value::as_str)
            .and_then(|s| parse_hex_u64(s).ok()))
    }

    /// Canonical block hash for a height, or None if the node doesn't have that block.
    pub async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        let result = self
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), false]),
            )
            .await?;
        Ok(result.get("hash").and_then(Value::as_str).map(String::from))
    }

    /// The block a transaction was mined in, from its receipt, or None if the node has no receipt.
    pub async fn transaction_block(&self, hash: &str) -> Result<Option<u64>> {
        let result = self
            .call("eth_getTransactionReceipt", json!([hash]))
            .await?;
        result
            .get("blockNumber")
            .and_then(Value::as_str)
            .map(parse_hex_u64)
            .transpose()
    }

    /// [`Self::block_hash`] and the same header's timestamp, from one `eth_getBlockByNumber`.
    pub async fn block_record(&self, number: u64) -> Result<Option<(String, Option<u64>)>> {
        let result = self
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{number:x}"), false]),
            )
            .await?;
        let Some(hash) = result.get("hash").and_then(Value::as_str) else {
            return Ok(None);
        };
        let ts = result
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|s| parse_hex_u64(s).ok());
        Ok(Some((hash.to_string(), ts)))
    }

    /// Every log from `addresses` matching any of `topic0s`: one `eth_getLogs`, or one per group of
    /// addresses once an endpoint has refused the whole list (#1941).
    pub async fn get_logs(
        &self,
        addresses: &[String],
        topic0s: &[String],
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>> {
        self.get_logs_matching(addresses, topic0s, &Default::default(), from, to)
            .await
    }

    async fn get_logs_matching(
        &self,
        addresses: &[String],
        topic0s: &[String],
        topics: &[Vec<String>; 3],
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>> {
        if addresses.is_empty() {
            return self
                .get_logs_once(addresses, topic0s, topics, from, to)
                .await;
        }
        let mut logs = Vec::new();
        let mut groups = 0usize;
        let mut done = 0usize;
        while done < addresses.len() {
            let cap = self.log_address_cap.load(Ordering::Relaxed);
            let group = &addresses[done..addresses.len().min(done.saturating_add(cap))];
            match self.get_logs_once(group, topic0s, topics, from, to).await {
                Ok(mut l) => {
                    logs.append(&mut l);
                    done += group.len();
                    groups += 1;
                }
                Err(e) if group.len() > 1 && filter_refused(&e) => {
                    let half = group.len().div_ceil(2);
                    if self.log_address_cap.fetch_min(half, Ordering::Relaxed) > half {
                        tracing::info!(
                            "eth_getLogs naming {} addresses was refused ({e:#}); asking in \
                             groups of at most {half}",
                            group.len()
                        );
                    }
                }
                Err(e) => return Err(e),
            }
        }
        if groups > 1 {
            logs.sort_by_key(|l| (l.block_number, l.log_index));
        }
        Ok(logs)
    }

    async fn get_logs_once(
        &self,
        addresses: &[String],
        topic0s: &[String],
        topics: &[Vec<String>; 3],
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>> {
        let mut filter = serde_json::Map::new();
        // An empty address list means "no address filter" (topic0-only) - the factory tip regime
        // (RFC-0009 §3) fetches this way so a child created and active in the same block is already in
        // hand. Sending `"address": []` would instead match nothing, so omit the field when empty.
        if !addresses.is_empty() {
            filter.insert("address".into(), json!(addresses));
        }
        if let Some(t) = getlogs_topics(topic0s, topics) {
            filter.insert("topics".into(), t);
        }
        filter.insert("fromBlock".into(), json!(format!("0x{from:x}")));
        filter.insert("toBlock".into(), json!(format!("0x{to:x}")));
        let (result, _) = self
            .call_holding_capped(
                "eth_getLogs",
                json!([Value::Object(filter)]),
                Some(to),
                // One block cannot be narrowed; refusing it would only make its answer an error.
                (from < to).then_some(self.logs_body_cap),
            )
            .await?;
        let arr = result
            .as_array()
            .ok_or_else(|| anyhow!("eth_getLogs did not return an array"))?;
        self.remember_log_timestamps(arr);
        // A log a reorg took back is not on the chain, whichever call a provider marks it on (#2222).
        arr.iter()
            .filter(|l| l.get("removed").and_then(Value::as_bool) != Some(true))
            .map(Log::from_rpc)
            .collect()
    }

    /// Nodes that carry `blockTimestamp` on each log (execution-apis, 2025) answer the header
    /// `block_timestamps` would otherwise buy per block: on a backfill, most of the RPC bill.
    fn remember_log_timestamps(&self, logs: &[Value]) {
        let found: HashMap<u64, u64> = logs
            .iter()
            .filter(|l| l.get("removed").and_then(Value::as_bool) != Some(true))
            .filter_map(|l| {
                let block = parse_hex_u64(l.get("blockNumber")?.as_str()?).ok()?;
                let ts = parse_hex_u64(l.get("blockTimestamp")?.as_str()?).ok()?;
                // arb1.arbitrum.io sends `0x0` on every log: absent, not the epoch.
                (ts != 0).then_some((block, ts))
            })
            .collect();
        if found.is_empty() {
            return;
        }
        let mut cache = self.timestamps.lock().unwrap_or_else(|e| e.into_inner());
        if cache.len() + found.len() > TIMESTAMP_CACHE_MAX {
            cache.clear();
        }
        cache.extend(found);
    }
}

/// `eth_getLogs`' `topics`: one position per topic, `null` where any value matches, and nothing past
/// the last position that narrows. `None` when nothing does.
fn getlogs_topics(topic0s: &[String], topics: &[Vec<String>; 3]) -> Option<Value> {
    let any = |t: &[String]| if t.is_empty() { Value::Null } else { json!(t) };
    match topics.iter().rposition(|t| !t.is_empty()) {
        Some(last) => Some(Value::Array(
            std::iter::once(any(topic0s))
                .chain(topics[..=last].iter().map(|t| any(t)))
                .collect(),
        )),
        None => (!topic0s.is_empty()).then(|| json!([topic0s])),
    }
}

/// Wall-clock millis since the epoch - used only for endpoint-health cooldowns (a coarse "try again
/// after" timer), never for anything in the deterministic data path.
pub fn now_millis() -> u64 {
    let now = crate::time::system_now()
        .duration_since(crate::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    // A recorded pause stands in for a sleep, so the clock that rests endpoints must see it pass.
    #[cfg(any(test, feature = "test-hooks"))]
    let now = now
        + RETRY_PAUSES
            .try_with(|p| p.borrow().iter().sum::<Duration>().as_millis() as u64)
            .unwrap_or(0);
    now
}

/// Reduce an RPC URL to `scheme://host[:port]` for logging - provider endpoints routinely carry the API
/// key in the path (`.../v3/<KEY>`), the query string or the userinfo (`user:pass@`), and the failure
/// log fires on exactly the outages an operator debugs with `RUST_LOG=debug`. Log *where* it failed,
/// never the key. Rebuilt from the parsed URL rather than sliced, because userinfo sits inside the
/// prefix a slice would keep (#1589).
pub fn redact_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(u) => match url_host(&u) {
            Some(host) => format!("{}://{host}", u.scheme()),
            None => format!("{}:<no host>", u.scheme()),
        },
        Err(_) => "<unparseable url>".to_string(),
    }
}

/// `host[:port]` of a parsed URL and nothing else, for labels that must not carry a scheme.
pub fn url_host(u: &url::Url) -> Option<String> {
    let host = u.host_str()?;
    Some(match u.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

/// Whether an RPC error looks like a result-size / range cap (so the caller shrinks and retries the
/// same range) rather than a transient failure.
///
/// **This list is a liability, and it has already cost us.** It was written against Alchemy and Infura,
/// and `arb1.arbitrum.io` - an endpoint nuthatch *ships as an Arbitrum default* - says
/// `"logs matched by query exceeds limit of 10000"`, which matched **none** of the original markers. So
/// splitting never fired on our own zero-setup path, and a busy Arbitrum contract retried the same
/// oversized window until the operator intervened (RFC-0028 §1, measured 2026-07-28).
///
/// Widening the list fixes the providers we have met. The durable fix is the caller's speculative split
/// for failures this cannot classify (RFC-0028 §3b) - because any design that needs to have seen a
/// provider's phrasing in advance will keep failing on providers we have not met.
pub fn is_result_too_large(err: &anyhow::Error) -> bool {
    // Prefer the structured classification when the error came from `RpcClient`, which inspected the
    // HTTP status and the JSON-RPC error object rather than a flattened string (RFC-0028 §3e).
    //
    // Two independent classifiers that can disagree about the same error is worse than one: which
    // answer wins would depend on the code path, and the resulting bug would be unreproducible. So the
    // text matching below is now the *fallback* - for `Source` implementations that are not the RPC
    // client (the test tapes, the ExEx source) and raise plain `anyhow!` errors.
    if let Some(class) = class_of(err) {
        match class {
            FailureClass::Narrowable { .. } => return true,
            // **`Transient` does not mean "not a cap"** (RFC-0029 §3b). It is the fall-through default
            // of `classify_status` - the *absence* of a classification rather than a positive finding -
            // so it must never outrank direct textual evidence below. Returning `false` here is what
            // made the widened marker list unreachable for Alchemy's HTTP 400, and turned a splittable
            // window into five same-width retries and a dead backfill.
            FailureClass::Transient => {}
            // RateLimited and Terminal *are* positive findings: back off or stop, never split.
            _ => return false,
        }
    }
    looks_like_cap(&format!("{err:#}"))
}

#[async_trait::async_trait]
impl<T: Transport> crate::source::Source for RpcClient<T> {
    fn forget_cached_above(&self, block: u64) {
        self.forget_timestamps_above(block);
    }

    fn refuses_topic0_only(&self, err: &anyhow::Error) -> bool {
        filter_refused(err)
    }

    async fn tip(&self) -> Result<u64> {
        self.block_number().await
    }

    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        // Disambiguate from this trait method - call the inherent one.
        RpcClient::block_hash(self, number).await
    }

    async fn block_hashes(&self, blocks: &[u64]) -> Result<HashMap<u64, String>> {
        Ok(RpcClient::block_headers(self, blocks)
            .await?
            .into_iter()
            .filter_map(|(b, h)| Some((b, h.get("hash")?.as_str()?.to_string())))
            .collect())
    }

    async fn finalized(&self) -> Result<Option<u64>> {
        self.finalized_block().await
    }

    async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        RpcClient::block_timestamps(self, blocks).await
    }

    async fn block_record(&self, number: u64) -> Result<Option<(String, Option<u64>)>> {
        RpcClient::block_record(self, number).await
    }

    async fn block_headers(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        RpcClient::block_headers(self, blocks).await
    }

    async fn block_bodies(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        RpcClient::block_bodies(self, blocks).await
    }

    async fn logs(
        &self,
        filter: &crate::source::LogFilter,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>> {
        self.get_logs_matching(
            filter.addresses(),
            filter.topic0s(),
            filter.topics(),
            from,
            to,
        )
        .await
    }
}

#[cfg(test)]
mod getlogs_topics_tests {
    use super::getlogs_topics;
    use serde_json::json;

    #[test]
    fn positions_stop_at_the_last_one_that_narrows() {
        let t0 = ["0xaa".to_string()];
        let none: [Vec<String>; 3] = Default::default();
        assert_eq!(getlogs_topics(&t0, &none), Some(json!([["0xaa"]])));
        assert_eq!(getlogs_topics(&[], &none), None);
        let third = [vec![], vec![], vec!["0xd".to_string()]];
        assert_eq!(
            getlogs_topics(&t0, &third),
            Some(json!([["0xaa"], null, null, ["0xd"]]))
        );
        let first = [vec!["0xb".to_string(), "0xc".to_string()], vec![], vec![]];
        assert_eq!(
            getlogs_topics(&[], &first),
            Some(json!([null, ["0xb", "0xc"]]))
        );
    }
}
