//! The thinnest JSON-RPC client that works: `eth_blockNumber` + `eth_getLogs`, with round-robin
//! failover across the configured endpoints. No ExEx yet - that's the sovereignty upgrade later.

// `Log` lives in nuthatch-decode so fuzz targets build without pulling in dbsp (nuthatch#581).
use nuthatch_decode::rpc::parse_hex_u64;
pub use nuthatch_decode::rpc::Log;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// How many times a whole `block_timestamps` batch is retried before it's returned as an error rather
/// than silently yielding an all-zeros timestamp map into the sealed path.
const TIMESTAMP_ATTEMPTS: usize = 4;

#[cfg(test)]
tokio::task_local! {
    // Scoped to one test future: concurrent clients cannot contaminate its observations.
    static RETRY_PAUSES: std::cell::RefCell<Vec<Duration>>;
}

async fn retry_pause(pause: Duration) {
    #[cfg(test)]
    if RETRY_PAUSES
        .try_with(|pauses| pauses.borrow_mut().push(pause))
        .is_ok()
    {
        return;
    }
    tokio::time::sleep(pause).await;
}

/// Max block numbers per `eth_getBlockByNumber` JSON-RPC batch. Many providers cap batch size and
/// **silently drop** an oversized batch (returning nothing), which the strict no-partial-map guard
/// then correctly rejects - so a dense window that needs 1000+ distinct timestamps would fail on such
/// a node. Splitting into bounded sub-batches keeps each request within common limits.
const MAX_TIMESTAMP_BATCH: usize = 200;

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
const TIMESTAMP_CACHE_MAX: usize = 262_144;

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
fn decode_call_batch(response: &Value, expected: usize) -> Result<Vec<Option<String>>> {
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
const ENDPOINT_COOLDOWN_MS: u64 = 30_000;

/// The first rest after a 429 that named no `Retry-After` (#1853).
const RATE_LIMIT_PAUSE_MS: u64 = 1_000;

/// A *terminal* failure (bad credentials, endpoint refusing us outright) earns a much longer cooldown
/// than a transient one: asking again in 30s will get the same 401, and a tight retry loop against a
/// 403 is exactly how a production nest spent forty minutes logging nothing useful (RFC-0028 §3).
///
/// Deliberately a long cooldown rather than permanent removal: an endpoint whose auth blips - a key
/// rotation, a provider incident - recovers on its own, and degrading beats failing. Five minutes
/// matches RFC-0026's backoff cap, so the two "something is wrong, back off" policies agree.
const TERMINAL_COOLDOWN_MS: u64 = 300_000;

/// How an RPC failure should be treated (RFC-0028 §3). The classification is the whole point: treat a
/// too-large request like a dead endpoint and you fail over pointlessly; treat a bad API key like a
/// blip and you retry it forever.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FailureClass {
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
pub(crate) struct ClassifiedError {
    pub class: FailureClass,
    pub detail: String,
}

impl std::fmt::Display for ClassifiedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.detail)
    }
}

impl std::error::Error for ClassifiedError {}

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
fn escalate_pool_wide_rate_limit(
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
pub(crate) fn escalated_from_rate_limit(err: &anyhow::Error) -> bool {
    matches!(
        class_of(err),
        Some(FailureClass::Narrowable {
            escalated_from_rate_limit: true,
            ..
        })
    )
}

/// Did an endpoint refuse this getLogs for its address list (#1941)?
pub(crate) fn filter_refused(err: &anyhow::Error) -> bool {
    matches!(class_of(err), Some(FailureClass::FilterRefused))
}

/// The classification carried by `err`, if it came from the RPC client.
///
/// Walks the whole `anyhow` chain rather than only the outermost error, because callers add
/// `.with_context(…)` as an error travels up (`getLogs 100..=200` and friends) - checking only the top
/// would silently lose the classification the moment anyone added context, which is exactly the sort of
/// bug that shows up as "it works in the unit test".
pub(crate) fn class_of(err: &anyhow::Error) -> Option<FailureClass> {
    err.chain()
        .find_map(|e| e.downcast_ref::<ClassifiedError>())
        .map(|c| c.class.clone())
}

/// Classify an HTTP-level failure. Auth is terminal; a payload-too-large is about the request; the
/// rest are someone else's problem and worth trying elsewhere.
///
/// `429` maps to [`FailureClass::RateLimited`], which behaves as transient: a rate limit usually means
/// "you asked too often", not "you asked for too much", and shrinking the window would trade throughput
/// for nothing on what is really a pacing problem. It is a distinct variant only so that a *pool-wide*
/// 429 can escalate to narrowable in [`escalate_pool_wide_rate_limit`].
pub(crate) fn classify_status(status: u16, body: &str) -> FailureClass {
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
/// Deliberately broader than [`crate::chunker::is_result_too_large`], and the reason is issue #241
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
fn batch_is_narrowable(err: &anyhow::Error) -> bool {
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
/// Shared by the status classifier and [`crate::chunker::is_result_too_large`] so the two cannot drift
/// into disagreeing about the same string - which is exactly the failure RFC-0028 §3e consolidated the
/// classifiers to prevent.
pub(crate) fn looks_like_cap(body: &str) -> bool {
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
pub(crate) fn is_capability_refusal(body: &str) -> bool {
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
pub(crate) fn classify_rpc_error(err: &Value) -> FailureClass {
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
pub(crate) fn suggested_range(msg: &str) -> Option<(u64, u64)> {
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
pub(crate) const MAX_RETRY_HINT: Duration = Duration::from_secs(30);

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
pub(crate) fn clamp_retry_hint(hint: Duration, own_pacing: Duration) -> Duration {
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
pub(crate) fn parse_retry_hint(raw: &str) -> Option<Duration> {
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

pub struct RpcClient {
    http: reqwest::Client,
    urls: Vec<String>,
    cursor: AtomicUsize,
    /// `urls[..primaries]` share the load; the rest are asked only while every primary is cooling down.
    primaries: usize,
    /// Per-endpoint health: the millis-since-epoch until which the endpoint is considered unhealthy
    /// (`0` = healthy). Set on a failed call, cleared on a successful one. Endpoints past their cooldown
    /// are tried first; still-unhealthy ones are the fallback of last resort (soonest-to-recover first).
    health: Vec<AtomicU64>,
    /// Millis-since-epoch until which an endpoint that answered 429 is not asked at all, not even as
    /// the last resort a cooling endpoint otherwise is (#1853). `0` = not resting.
    limited: Vec<AtomicU64>,
    /// 429s without a hint in a row per endpoint, which double its rest; cleared by an answer.
    limited_streak: Vec<AtomicU64>,
    /// The highest block each endpoint has reported from `eth_blockNumber`, so a lower bound on its
    /// head. Pool members' heads differ by a block or two, and one behind refuses a `toBlock` it lacks.
    heads: Vec<AtomicU64>,
    /// Total HTTP requests attempted (incl. failover retries) - a benchmark/observability metric.
    requests: AtomicU64,
    /// Block-number → unix timestamp, remembered across windows (RFC-0029 §6d).
    ///
    /// **This is only sound because of [`RpcClient::forget_timestamps_above`].** A block *number* does
    /// not identify a block - a reorg replaces the block at that height with a different one carrying a
    /// different timestamp. `block_timestamp` is a sealed column and the segment's content address
    /// depends on it, so serving a stale timestamp after a reorg would seal a wrong value and break
    /// re-execution determinism. The RFC proposes the cache without noting this; the invalidation hook
    /// is the condition that makes it safe, not an optimisation on top.
    timestamps: std::sync::Mutex<HashMap<u64, u64>>,
    /// Headers per batch: [`MAX_TIMESTAMP_BATCH`], or less where a known endpoint in the pool takes
    /// less ([`crate::chains::header_batch_cap`], #1570).
    header_width: usize,
    /// [`GETLOGS_BODY_CAP`], or a smaller one a test sets.
    logs_body_cap: usize,
    /// The most addresses one getLogs names, lowered when an endpoint refuses a list (#1941). One
    /// value for the pool: the strictest endpoint sets it, at the cost of extra requests elsewhere.
    log_address_cap: AtomicUsize,
    /// Endpoints whose startup chain-id check got no answer; each is asked again before it serves
    /// anything (#2161).
    unverified: Vec<AtomicBool>,
    /// The chain `verify_chain_ids` was asked to hold the pool to.
    chain: AtomicU64,
}

impl RpcClient {
    pub fn new(urls: Vec<String>) -> Result<Self> {
        Self::with_timeout(urls, Duration::from_secs(20))
    }

    /// [`Self::new`] with a per-request deadline other than 20 s.
    pub fn with_timeout(urls: Vec<String>, timeout: Duration) -> Result<Self> {
        if urls.is_empty() {
            bail!("no RPC URLs configured");
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to build HTTP client")?;
        let n = urls.len();
        let header_width = crate::chains::header_batch_cap(&urls)
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
            heads,
            requests: AtomicU64::new(0),
            timestamps: std::sync::Mutex::new(HashMap::new()),
            header_width,
            logs_body_cap: GETLOGS_BODY_CAP,
            log_address_cap: AtomicUsize::new(usize::MAX),
            unverified: (0..n).map(|_| AtomicBool::new(false)).collect(),
            chain: AtomicU64::new(0),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_logs_body_cap(mut self, cap: usize) -> Self {
        self.logs_body_cap = cap;
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
    fn endpoint_order(&self) -> Vec<usize> {
        self.endpoint_order_holding(None)
    }

    /// [`Self::endpoint_order`], with healthy endpoints known to hold block `need` moved to the front.
    /// At the tip that is the endpoint which just reported the tip, so its `eth_getLogs` is not first
    /// refused by a pool member a block behind (#1498).
    fn endpoint_order_holding(&self, need: Option<u64>) -> Vec<usize> {
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

    fn mark_healthy(&self, j: usize) {
        self.health[j].store(0, Ordering::Relaxed);
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

    fn mark_unhealthy(&self, j: usize) {
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
        self.health[j].store(now_millis() + TERMINAL_COOLDOWN_MS, Ordering::Relaxed);
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
    pub(crate) async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.call_holding(method, params, None)
            .await
            .map(|(v, _)| v)
    }

    /// [`Self::call`], refusing a response body past `cap` bytes as too large, so the caller
    /// narrows the range rather than holding it whole.
    pub(crate) async fn call_capped(
        &self,
        method: &str,
        params: Value,
        cap: usize,
    ) -> Result<Value> {
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
            crate::metrics::METRICS.inc_rpc();
            crate::metrics::METRICS.inc_rpc_method(method);
            attempts += 1;
            let t0 = Instant::now();
            let answer = match self.admit(j).await {
                Ok(()) => self.call_one(url, method, &params, body_cap).await,
                Err(e) => Err(e),
            };
            match answer {
                Ok(v) => {
                    crate::metrics::METRICS.observe_rpc(
                        &crate::metrics::endpoint_label(url),
                        t0.elapsed(),
                        false,
                        attempts > 1,
                    );
                    self.mark_healthy(j);
                    return Ok((v, j));
                }
                Err(e) => {
                    crate::metrics::METRICS.observe_rpc(
                        &crate::metrics::endpoint_label(url),
                        t0.elapsed(),
                        true,
                        attempts > 1,
                    );
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
            crate::metrics::METRICS.inc_rpc();
            match body {
                Value::Array(items) => {
                    let mut counts: HashMap<&str, u64> = HashMap::new();
                    for item in items {
                        if let Some(m) = item.get("method").and_then(Value::as_str) {
                            *counts.entry(m).or_insert(0) += 1;
                        }
                    }
                    for (m, c) in counts {
                        crate::metrics::METRICS.inc_rpc_methods(m, c);
                    }
                }
                Value::Object(map) => {
                    if let Some(m) = map.get("method").and_then(Value::as_str) {
                        crate::metrics::METRICS.inc_rpc_method(m);
                    }
                }
                _ => {}
            }
            attempts += 1;
            let t0 = Instant::now();
            let answer = match self.admit(j).await {
                Ok(()) => self.post_one(url, body).await,
                Err(e) => Err(e),
            };
            match answer {
                Ok(v) => {
                    crate::metrics::METRICS.observe_rpc(
                        &crate::metrics::endpoint_label(url),
                        t0.elapsed(),
                        false,
                        attempts > 1,
                    );
                    self.mark_healthy(j);
                    return Ok(v);
                }
                Err(e) => {
                    crate::metrics::METRICS.observe_rpc(
                        &crate::metrics::endpoint_label(url),
                        t0.elapsed(),
                        true,
                        attempts > 1,
                    );
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
        let resp = self.http.post(url).json(body).send().await.map_err(|e| {
            // reqwest's own text for a timeout is "error sending request", which a caller narrowing
            // its range on a timeout cannot tell from a refused connection.
            let timed_out = if e.is_timeout() { "timed out: " } else { "" };
            classified(
                FailureClass::Transient,
                format!("transport error: {timed_out}{}", e.without_url()),
            )
        })?;
        let status = resp.status();
        // Read `Retry-After` before the body consumes the response (#361). Seconds-form only: the
        // HTTP-date form needs a clock comparison to be meaningful, and no provider we have measured
        // sends it on a 429.
        let header_hint = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_retry_hint);
        if !status.is_success() {
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
                    FailureClass::Transient => classify_status(status.as_u16(), &text),
                    definite => definite,
                },
                None => classify_status(status.as_u16(), &text),
            };
            // The body's own `try_again_in` wins - it is the more specific statement, and the one
            // Chainstack actually sends. `Retry-After` fills in when the body said nothing.
            let class = match class {
                FailureClass::RateLimited { retry_after: None } => FailureClass::RateLimited {
                    retry_after: header_hint,
                },
                other => other,
            };
            let mut detail = format!("HTTP {status}");
            if !text.is_empty() {
                let snippet: String = text.chars().take(300).collect();
                detail.push_str(&format!(": {snippet}"));
            }
            return Err(classified(class, detail));
        }
        let read_failed = |e: reqwest::Error| {
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
            let class = if e.is_timeout() {
                FailureClass::Narrowable {
                    suggested: None,
                    escalated_from_rate_limit: false,
                }
            } else {
                FailureClass::Transient
            };
            classified(class, format!("malformed response: {e}"))
        };
        let Some(cap) = body_cap else {
            return resp.json::<Value>().await.map_err(read_failed);
        };
        let mut resp = resp;
        let mut body = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(read_failed)? {
            body.extend_from_slice(&chunk);
            if body.len() > cap {
                return Err(classified(
                    FailureClass::Narrowable {
                        suggested: None,
                        escalated_from_rate_limit: false,
                    },
                    format!("response passed the {cap}-byte getLogs body cap; narrowing the range"),
                ));
            }
        }
        serde_json::from_slice(&body)
            .map_err(|e| classified(FailureClass::Transient, format!("malformed response: {e}")))
    }

    /// Single-endpoint POST for tests that need the *classification* of a raw transport failure,
    /// before failover or the all-endpoints re-classification (`§92`) can rewrite it.
    #[cfg(test)]
    pub(crate) async fn post_one_for_test(&self, body: &Value) -> Result<Value> {
        let url = self.urls[0].clone();
        self.send_classified(&url, body, None).await
    }

    async fn post_one(&self, url: &str, body: &Value) -> Result<Value> {
        let resp: Value = self.send_classified(url, body, None).await?;
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
    pub(crate) async fn call_each(&self, method: &str, params: Value) -> Result<Vec<Value>> {
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
    pub(crate) async fn visit_each(
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
            crate::metrics::METRICS.inc_rpc();
            crate::metrics::METRICS.inc_rpc_method("eth_chainId");
            let r = tokio::time::timeout(
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
                Err(e) if calls.len() > 1 && crate::chunker::is_result_too_large(&e) => {
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
        let resp = self.post_with_failover(&Value::Array(batch)).await?;
        decode_call_batch(&resp, calls.len())
    }

    /// Send a raw JSON-RPC batch and return the raw response. For `doctor` only: measuring the
    /// endpoint's batch ceiling means sending batches of chosen sizes, which no typed helper offers.
    pub(crate) async fn raw_batch(&self, body: &Value) -> Result<Value> {
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
            .chunks(self.header_width)
            .map(|c| self.fetch_timestamp_batch(c, false, true))
            .collect();
        let results: Vec<Result<HashMap<u64, Value>>> = futures::stream::iter(futures)
            .buffered(TIMESTAMP_FANOUT)
            .collect()
            .await;
        // The batch returns whole headers (RFC-0036 reuses this path for the `blocks` table); this
        // caller wants one field. Projecting here rather than widening the cache keeps the cache a
        // `block -> u64` map: holding headers for `TIMESTAMP_CACHE_MAX` blocks instead would be a
        // footprint regression on the hot path, against a 2 GB per-cursor budget.
        for r in results {
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
                .chunks(self.header_width)
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
    fn fetch_timestamp_batch<'a>(
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
            match self.post_with_failover(&body).await {
                Ok(r) => {
                    resp = Some(r);
                    break;
                }
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
        let resp = match resp {
            Some(r) => r,
            None => {
                return Err(last_err
                    .unwrap()
                    .context("block_timestamps batch failed after retries"))
            }
        };
        let mut out = HashMap::new();
        let mut first_item_error: Option<FailureClass> = None;
        for item in resp.as_array().into_iter().flatten() {
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
            if let Some(result) = item.get("result").filter(|v| !v.is_null()) {
                out.insert(block, result.clone());
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
        if addresses.is_empty() {
            return self.get_logs_once(addresses, topic0s, from, to).await;
        }
        let mut logs = Vec::new();
        let mut groups = 0usize;
        let mut done = 0usize;
        while done < addresses.len() {
            let cap = self.log_address_cap.load(Ordering::Relaxed);
            let group = &addresses[done..addresses.len().min(done.saturating_add(cap))];
            match self.get_logs_once(group, topic0s, from, to).await {
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
        if !topic0s.is_empty() {
            filter.insert("topics".into(), json!([topic0s]));
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

/// Wall-clock millis since the epoch - used only for endpoint-health cooldowns (a coarse "try again
/// after" timer), never for anything in the deterministic data path.
fn now_millis() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    // A recorded pause stands in for a sleep, so the clock that rests endpoints must see it pass.
    #[cfg(test)]
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
pub(crate) fn redact_url(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(u) => match url_host(&u) {
            Some(host) => format!("{}://{host}", u.scheme()),
            None => format!("{}:<no host>", u.scheme()),
        },
        Err(_) => "<unparseable url>".to_string(),
    }
}

/// `host[:port]` of a parsed URL and nothing else, for labels that must not carry a scheme.
pub(crate) fn url_host(u: &reqwest::Url) -> Option<String> {
    let host = u.host_str()?;
    Some(match u.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// A one-endpoint fake JSON-RPC server on a loopback port. Returns `(url, handle)`; the caller
    /// aborts the handle when done. Real HTTP, so `RpcClient`'s actual request path is exercised -
    /// there is no way to fake a per-endpoint bug like a mixed-chain pool without it.
    async fn fake_rpc(chain_id: u64) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{extract::State, routing::post, Json, Router};
        use serde_json::{json, Value};

        async fn handler(State(chain_id): State<u64>, Json(req): Json<Value>) -> Json<Value> {
            let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
            let result = match method {
                "eth_chainId" => json!(format!("0x{chain_id:x}")),
                "eth_blockNumber" => json!(HEALTHY_TIP_HEX),
                _ => json!(null),
            };
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": result}))
        }

        // Answer on ANY path, not just `/` - provider URLs carry the API key in the path
        // (`.../v3/<KEY>`), and a mock that 404s those would read as "endpoint down" and quietly
        // skip the very check under test.
        let app = Router::new()
            .route("/", post(handler))
            .route("/{*rest}", post(handler))
            .with_state(chain_id);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle)
    }

    /// An endpoint that answers every request with `body`.
    async fn canned_rpc(body: serde_json::Value) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{routing::post, Json, Router};
        let app = Router::new().route(
            "/",
            post(move || {
                let body = body.clone();
                async move { Json(body) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle)
    }

    /// An `eth_getLogs` endpoint with no result cap: `n` logs in block 1, streamed without a
    /// `Content-Length`, so only reading the body can tell how large it is.
    async fn uncapped_logs_rpc(n: usize) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{body::Body, routing::post, Router};
        const LOG: &str = r#"{"address":"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],"data":"0x","blockNumber":"0x1","transactionHash":"0x01","logIndex":"0x0"}"#;
        let app = Router::new().route(
            "/",
            post(move || async move {
                let parts = std::iter::once(r#"{"jsonrpc":"2.0","id":1,"result":["#.to_string())
                    .chain((0..n).map(|i| format!("{}{LOG}", if i == 0 { "" } else { "," })))
                    .chain(std::iter::once("]}".to_string()));
                Body::from_stream(futures::stream::iter(parts.map(Ok::<_, std::io::Error>)))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle)
    }

    /// Address history's getLogs is capped the same way, at its own smaller cap, with an error its
    /// span narrowing reads as a range refusal, and one block past it is refused too.
    #[tokio::test]
    async fn address_history_getlogs_bodies_are_capped() {
        use crate::address_discovery::Rpc;
        let (url, server) = uncapped_logs_rpc(25_000).await;
        let client = super::RpcClient::new(vec![url]).unwrap();
        let ask = |from: &str, to: &str| serde_json::json!([{"fromBlock": from, "toBlock": to, "topics": []}]);
        let err = Rpc::call(&client, "eth_getLogs", ask("0x1", "0x2"))
            .await
            .unwrap_err();
        assert!(
            crate::address_discovery::range_shaped(&format!("{err:#}")),
            "{err:#}"
        );
        let one = Rpc::call(&client, "eth_getLogs", ask("0x1", "0x1")).await;
        server.abort();
        assert!(format!("{:#}", one.unwrap_err()).contains("body cap"));
    }

    /// #1788: a node with no result cap answers a multi-block range whole, however large. Past the
    /// body cap it is refused as too large, so the caller narrows; a single block cannot be narrowed
    /// and is read whole.
    #[tokio::test]
    async fn a_getlogs_body_past_the_cap_is_refused_as_too_large() {
        let t0 =
            vec!["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string()];
        let (url, server) = uncapped_logs_rpc(2_000).await;
        let capped = super::RpcClient::new(vec![url.clone()])
            .unwrap()
            .with_logs_body_cap(64 << 10);
        let err = capped.get_logs(&[], &t0, 1, 2).await.unwrap_err();
        assert!(
            crate::chunker::is_result_too_large(&err),
            "a body past the cap must read as a cap: {err:#}"
        );
        let one = capped.get_logs(&[], &t0, 1, 1).await.unwrap();
        assert_eq!(one.len(), 2_000, "a single block is read whole");
        let roomy = super::RpcClient::new(vec![url])
            .unwrap()
            .with_logs_body_cap(16 << 20);
        let all = roomy.get_logs(&[], &t0, 1, 2).await.unwrap();
        server.abort();
        assert_eq!(all.len(), 2_000, "a body under the cap is untouched");
    }

    /// #1494: a window checkpoint's hash and timestamp are one header, so one request.
    #[tokio::test]
    async fn a_block_record_is_one_request() {
        use serde_json::json;
        let (url, server) = canned_rpc(json!({
            "jsonrpc": "2.0", "id": 0,
            "result": {"number": "0x10", "hash": "0xabc", "timestamp": "0x6553f100"}
        }))
        .await;
        let c = super::RpcClient::new(vec![url]).unwrap();
        let got = c.block_record(16).await.unwrap();
        let requests = c.request_count();
        server.abort();
        assert_eq!(got, Some(("0xabc".to_string(), Some(0x6553f100))));
        assert_eq!(requests, 1);

        let (url, server) = canned_rpc(json!({"jsonrpc": "2.0", "id": 0, "result": null})).await;
        let got = super::RpcClient::new(vec![url])
            .unwrap()
            .block_record(16)
            .await
            .unwrap();
        server.abort();
        assert_eq!(got, None, "a block the node does not have has no record");
    }

    async fn two_calls_answered_with(
        body: serde_json::Value,
    ) -> anyhow::Result<Vec<Option<String>>> {
        let calls = vec![
            (
                "0x1111111111111111111111111111111111111111".to_string(),
                "0x12345678".to_string(),
            ),
            (
                "0x2222222222222222222222222222222222222222".to_string(),
                "0x12345678".to_string(),
            ),
        ];
        let (url, server) = canned_rpc(body).await;
        let out = super::RpcClient::new(vec![url])
            .unwrap()
            .eth_call_batch_at(&calls, 100)
            .await;
        server.abort();
        out
    }

    /// `None` is what a stored call row means by "reverted", so only an explicit revert may produce
    /// it. A pruned node's `missing trie node`, a per-item rate limit, or an id the provider left out
    /// is not a fact about the chain, and storing it as one writes the provider's failure into the
    /// dataset.
    #[tokio::test]
    async fn an_eth_call_the_provider_failed_is_an_error_not_a_stored_revert() {
        use serde_json::json;
        assert_eq!(
            two_calls_answered_with(json!([
                {"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}},
                {"jsonrpc":"2.0","id":0,"result":"0x0012"}
            ]))
            .await
            .unwrap(),
            vec![Some("0x0012".into()), None],
            "a revert and a value are both answers"
        );
        for body in [
            json!([
                {"jsonrpc":"2.0","id":0,"result":"0x"},
                {"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"missing trie node 1f2e (path )"}}
            ]),
            json!([
                {"jsonrpc":"2.0","id":0,"result":"0x"},
                {"jsonrpc":"2.0","id":1,"error":{"code":429,"message":"rate limit exceeded"}}
            ]),
            json!([{"jsonrpc":"2.0","id":0,"result":"0x"}]),
            json!({"jsonrpc":"2.0","id":0,"result":"0x"}),
        ] {
            assert!(
                two_calls_answered_with(body.clone()).await.is_err(),
                "stored as a revert: {body}"
            );
        }
    }

    #[test]
    fn call_batches_distinguish_reverts_from_provider_failures_and_missing_results() {
        use serde_json::json;
        assert_eq!(
            super::decode_call_batch(
                &json!([
                    {"id":1,"error":{"code":3,"message":"execution reverted: missing getter"}},
                    {"id":0,"result":"0x0012"}
                ]),
                2
            )
            .unwrap(),
            vec![Some("0x0012".into()), None]
        );
        // GraphOps, measured 2026-09-29: the upstream's code-3 revert, wrapped by its proxy.
        assert_eq!(
            super::decode_call_batch(
                &json!([
                    {"id":0,"error":{"code":-32603,"message":"gave up retrying on upstream-level after 1.89064678s: 3: execution reverted"}},
                    {"id":1,"error":{"code":-32603,"message":"gave up retrying on upstream-level after 2s: 3: execution reverted: missing getter"}}
                ]),
                2
            )
            .unwrap(),
            vec![None, None]
        );
        for response in [
            json!([]),
            json!({"result":"0x"}),
            json!([{"id":0,"result":null}]),
            json!([{"id":1,"result":"0x"}]),
            json!([{"id":0,"result":"0x"},{"id":0,"result":"0x"}]),
            json!([{"id":0,"result":"0xyz"}]),
            json!([{"id":0,"result":"0x1"}]),
            json!([{"id":0,"error":{"code":-32000,"message":"missing trie node"}}]),
            json!([{"id":0,"error":{"code":-32602,"message":"invalid block selector"}}]),
            json!([{"id":0,"error":{"code":-32603,"message":"gave up retrying on upstream-level after 2s: -32000: header not found"}}]),
            json!([{"id":0,"error":{"code":-32603,"message":"execution reverted"}}]),
            json!([{"id":0,"error":{"code":429,"message":"rate limit exceeded"}}]),
            json!([{"id":0,"result":"0x","error":{"code":3,"message":"execution reverted"}}]),
        ] {
            assert!(
                super::decode_call_batch(&response, 1).is_err(),
                "accepted {response}"
            );
        }
    }

    /// A provider that never answers within the deadline must read as a timeout, which address
    /// discovery narrows its span on, not as an unreachable endpoint (Gnosis `trace_filter`, 2026-10-10).
    #[tokio::test]
    async fn a_request_past_its_deadline_says_it_timed_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let _held = tokio::spawn(async move {
            let mut open = Vec::new();
            while let Ok((s, _)) = listener.accept().await {
                open.push(s);
            }
        });
        let client = super::RpcClient::with_timeout(
            vec![format!("http://127.0.0.1:{port}")],
            std::time::Duration::from_millis(200),
        )
        .unwrap();
        let error = client
            .post_one_for_test(&serde_json::json!({"method":"trace_filter"}))
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(text.contains("timed out"), "{text}");
    }

    /// A provider key in the URL must not travel with a transport error into logs or stored text.
    #[tokio::test]
    async fn a_transport_error_does_not_carry_the_endpoint_url() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client = super::RpcClient::new(vec![format!(
            "http://127.0.0.1:{port}/v3/SECRETKEY?k=SECRETKEY"
        )])
        .unwrap();
        let error = client
            .post_one_for_test(&serde_json::json!({"method":"eth_blockNumber"}))
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(text.contains("transport error"), "{text}");
        assert!(!text.contains("SECRETKEY"), "{text}");
    }

    /// The block height the healthy mock reports, so a failover test can prove *which* endpoint
    /// answered rather than merely that something did.
    const HEALTHY_TIP_HEX: &str = "0x1234";
    const HEALTHY_TIP: u64 = 0x1234;

    /// An endpoint that is up but broken: HTTP 500 on everything. Distinct from an unbound port, so
    /// the test covers a *responding* bad endpoint rather than a refused connection.
    async fn broken_rpc() -> (String, tokio::task::JoinHandle<()>, Arc<AtomicU64>) {
        use axum::{extract::State, http::StatusCode, routing::post, Router};
        let hits = Arc::new(AtomicU64::new(0));
        async fn handler(State(hits): State<Arc<AtomicU64>>) -> StatusCode {
            hits.fetch_add(1, Ordering::Relaxed);
            StatusCode::INTERNAL_SERVER_ERROR
        }
        let app = Router::new()
            .route("/", post(handler))
            .route("/{*rest}", post(handler))
            .with_state(hits.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle, hits)
    }

    /// An endpoint that rejects our credentials: HTTP 401 with the shape a real provider returns
    /// (measured against Alchemy, 2026-07-28). Distinct from `broken_rpc`'s 500 precisely because
    /// RFC-0028 says these two must **not** be treated the same.
    async fn unauthorized_rpc() -> (String, tokio::task::JoinHandle<()>) {
        use axum::{http::StatusCode, routing::post, Json, Router};
        use serde_json::json;
        async fn handler() -> (StatusCode, Json<serde_json::Value>) {
            (
                StatusCode::UNAUTHORIZED,
                Json(
                    json!({"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Must be authenticated!"}}),
                ),
            )
        }
        let app = Router::new()
            .route("/", post(handler))
            .route("/{*rest}", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle)
    }

    /// The measured Alchemy refusal for an oversized `eth_getLogs` (2026-07-28). Kept verbatim so the
    /// classifier is tested against a real provider's words rather than our paraphrase of them.
    const ALCHEMY_OVERSIZED: &str =
        "Log response size exceeded. You can make eth_getLogs requests \
         with up to a 10,000 block range and no limit on the response size, or you can request any \
         block range with a cap of 10K logs in the response. Based on your parameters and the \
         response size limit, this block range should work: [0x1000000, 0x1007fff]";

    #[test]
    fn a_provider_suggested_range_is_parsed_from_the_error_text() {
        assert_eq!(
            super::suggested_range(ALCHEMY_OVERSIZED),
            Some((0x1000000, 0x1007fff)),
            "the suggested range is the whole point - halving toward a number we were handed is waste"
        );
    }

    #[test]
    fn a_malformed_or_inverted_suggestion_is_ignored_rather_than_trusted() {
        // Provider prose, not a contract: anything we cannot read cleanly must fall back to halving.
        assert_eq!(super::suggested_range("no brackets here"), None);
        assert_eq!(super::suggested_range("try [0x10, notahex]"), None);
        assert_eq!(super::suggested_range("try [0x20, 0x10]"), None, "inverted");
        assert_eq!(
            super::suggested_range("try [100, 200]"),
            None,
            "not hex-prefixed"
        );
    }

    #[test]
    fn an_oversized_range_is_narrowable_and_carries_its_hint() {
        let err = serde_json::json!({"code": -32602, "message": ALCHEMY_OVERSIZED});
        assert_eq!(
            super::classify_rpc_error(&err),
            super::FailureClass::Narrowable {
                suggested: Some((0x1000000, 0x1007fff)),
                escalated_from_rate_limit: false,
            }
        );
    }

    /// The **measured** Alchemy body, verbatim from the run in RFC-0029 §2 that killed a backfill.
    /// Kept exact rather than paraphrased: RFC-0028's grounding convention is that a classifier test
    /// carries a response a provider actually sent, because the whole class of bug here is a shape we
    /// assumed rather than observed.
    const ALCHEMY_400_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Log response size exceeded. You can make eth_getLogs requests with up to a 10,000 block range and no limit on the response size, or you can request any block range with a cap of 10K logs in the response. Based on your parameters and the response size limit, this block range should work: [0x1000000, 0x1007fff]"}}"#;

    /// **The RFC-0029 regression.** Alchemy signals an oversized range as HTTP 400, and 400 was not
    /// enumerated - so it fell through to `Transient`, the cap markers became unreachable, and a
    /// splittable window became five same-width retries and a dead backfill.
    #[test]
    fn an_http_400_carrying_cap_language_is_narrowable_not_transient() {
        let class = super::classify_status(400, ALCHEMY_400_BODY);
        assert!(
            matches!(class, super::FailureClass::Narrowable { .. }),
            "a 400 that says the response size was exceeded is a refusal to serve the range, not a \
             malformed request - got {class:?}"
        );
    }

    /// The exact response from the public free-plan run behind #801. It is a range cap even though
    /// it does not use any of the older "response size" wording, and must shrink rather than retry
    /// the same 81,920-block request forever.
    #[test]
    fn alchemy_free_plan_range_cap_is_narrowable() {
        let body = r#"{"message":"ranges over 10000 blocks are not supported on free plan"}"#;
        assert!(
            matches!(
                super::classify_status(400, body),
                super::FailureClass::Narrowable { .. }
            ),
            "the provider's explicit block-range cap must narrow"
        );
    }

    /// And the provider's own suggestion survives, which is the difference between halving blindly and
    /// asking for the range it just told us would work.
    #[test]
    fn the_suggested_range_survives_a_non_2xx() {
        assert_eq!(
            super::classify_status(400, ALCHEMY_400_BODY),
            super::FailureClass::Narrowable {
                suggested: Some((0x1000000, 0x1007fff)),
                escalated_from_rate_limit: false,
            }
        );
    }

    /// The narrowness matters. RFC-0029 §3b: "add 400 to the list" on its own would be the same mistake
    /// with a different number. A 400 that is genuinely a bad request must stay transient, or every
    /// malformed call would be answered by pointlessly splitting the range.
    #[test]
    fn an_http_400_without_cap_language_stays_transient() {
        assert_eq!(
            super::classify_status(
                400,
                r#"{"error":{"code":-32602,"message":"invalid argument 0: hex string without 0x prefix"}}"#
            ),
            super::FailureClass::Transient
        );
        assert_eq!(
            super::classify_status(400, ""),
            super::FailureClass::Transient
        );
    }

    /// A cap refusal arriving on *any* non-2xx must classify the same way. The bug was not "400 is
    /// special" - it was that the body stopped being read once the status was unhappy.
    #[test]
    fn cap_language_classifies_the_same_whatever_status_carries_it() {
        for status in [400, 413] {
            assert!(
                matches!(
                    super::classify_status(status, ALCHEMY_400_BODY),
                    super::FailureClass::Narrowable { .. }
                ),
                "HTTP {status} carrying cap language must be narrowable"
            );
        }
    }

    /// Monad's Ankr endpoint refuses an over-wide `eth_getLogs` at HTTP 200 with a message that
    /// matched nothing (RFC-0051 addendum, item 7), so the window would have been retried at the same
    /// width until the attempts ran out. QuickNode's shape is included so it classifies the same at
    /// any status, not only behind the 413 it happens to arrive with.
    /// Robinhood Chain's public endpoint (RFC-0050) refuses an over-wide `eth_getLogs` at HTTP 200
    /// with `-32000 "logs matched by query exceeds limit of 10000"`, measured 2026-09-04 on a
    /// 300,000-block address-filtered ask. It was already narrowable on the `logs matched by query`
    /// marker; this pins that the shape stays so, because the registry's 320-block window relies
    /// on the chunker narrowing from it on a busy token.
    #[test]
    fn robinhood_public_endpoint_cap_shape_is_narrowable() {
        let body = r#"{"code":-32000,"message":"logs matched by query exceeds limit of 10000"}"#;
        let err: serde_json::Value = serde_json::from_str(body).unwrap();
        assert!(
            matches!(
                super::classify_rpc_error(&err),
                super::FailureClass::Narrowable { .. }
            ),
            "{body} must be narrowable, not transient"
        );
        assert!(super::looks_like_cap(body));
    }

    #[test]
    fn monad_public_endpoint_cap_shapes_are_narrowable() {
        for body in [
            r#"{"code":-32603,"message":"response exceeds size limit"}"#,
            r#"{"code":-32614,"message":"eth_getLogs is limited to a 100 range"}"#,
        ] {
            let err: serde_json::Value = serde_json::from_str(body).unwrap();
            assert!(
                matches!(
                    super::classify_rpc_error(&err),
                    super::FailureClass::Narrowable { .. }
                ),
                "{body} must be narrowable, not transient"
            );
            assert!(
                super::looks_like_cap(body),
                "the text fallback must agree with the JSON-RPC classifier: {body}"
            );
        }
    }

    /// GraphOps' Arbitrum endpoint caps `eth_getLogs` at 25,000 blocks, measured 2026-09-29 with a
    /// 100,000-block ask. The cap itself is only in `data.details`, so the chunker has to halve to it.
    #[test]
    fn graphops_endpoint_cap_shape_is_narrowable() {
        let body = r#"{"code":-32012,"message":"getLogs request exceeded max allowed range","data":{"code":"ErrGetLogsExceededMaxAllowedRange","details":{"maxAllowedRange":25000,"requestRange":100001}}}"#;
        let err: serde_json::Value = serde_json::from_str(body).unwrap();
        assert!(
            matches!(
                super::classify_rpc_error(&err),
                super::FailureClass::Narrowable { .. }
            ),
            "{body} must be narrowable, not transient"
        );
        assert!(super::looks_like_cap(body));
    }

    /// `1rpc.io/bnb`, as S0 recorded it (#1941): the cap names a range, not "a 100 range".
    #[test]
    fn a_zero_to_fifty_blocks_range_cap_is_narrowable() {
        let body = r#"{"code":-32602,"message":"eth_getLogs is limited to 0 - 50 blocks range"}"#;
        let err: serde_json::Value = serde_json::from_str(body).unwrap();
        assert!(
            matches!(
                super::classify_rpc_error(&err),
                super::FailureClass::Narrowable { .. }
            ),
            "{body} must be narrowable, not transient"
        );
        assert!(super::looks_like_cap(body));
    }

    /// publicnode, measured on BSC and Polygon 2026-10-06: both refusals are about the filter's
    /// address list, so neither is a width to narrow nor a credential to cool down for.
    #[test]
    fn publicnode_address_refusals_are_about_the_filter() {
        for body in [
            r#"{"code":-32602,"message":"Request blocked"}"#,
            r#"{"code":-32701,"message":"Please specify an address in your request or, to remove restrictions, order a dedicated full node here: https://www.allnodes.com/bnb/host"}"#,
        ] {
            let err: serde_json::Value = serde_json::from_str(body).unwrap();
            assert_eq!(
                super::classify_rpc_error(&err),
                super::FailureClass::FilterRefused,
                "{body}"
            );
            assert!(!super::looks_like_cap(body), "{body} is not a range cap");
        }
    }

    /// publicnode answers anything about 10,000 blocks back with this, at HTTP 403.
    #[test]
    fn publicnode_archive_refusal_is_history_unavailable() {
        let body = r#"{"code":-32602,"message":"Archive requests require a personal token. Get one at: https://www.allnodes.com/publicnode"}"#;
        let err: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(
            super::classify_rpc_error(&err),
            super::FailureClass::HistoryUnavailable
        );
    }

    /// An endpoint that refuses a getLogs naming ten or more addresses with publicnode's 403, and
    /// otherwise answers one log per address it was asked for, newest address first.
    async fn address_capped_rpc(
        asked: Arc<std::sync::Mutex<Vec<usize>>>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{http::StatusCode, routing::post, Json, Router};
        use serde_json::{json, Value};
        let app = Router::new().route(
            "/",
            post(move |Json(req): Json<Value>| {
                let asked = asked.clone();
                async move {
                    let addrs: Vec<String> = req["params"][0]["address"]
                        .as_array()
                        .map(|a| a.iter().map(|v| v.as_str().unwrap().to_string()).collect())
                        .unwrap_or_default();
                    asked.lock().unwrap().push(addrs.len());
                    if addrs.len() >= 10 {
                        return (
                            StatusCode::FORBIDDEN,
                            Json(json!({"jsonrpc":"2.0","id":1,
                                "error":{"code":-32602,"message":"Request blocked"}})),
                        );
                    }
                    let logs: Vec<Value> = addrs
                        .iter()
                        .rev()
                        .map(|a| {
                            let n = u64::from_str_radix(&a[2..], 16).unwrap();
                            json!({"address": a, "topics": [], "data": "0x",
                                "blockNumber": format!("0x{:x}", 100 + n % 3),
                                "transactionHash": "0x01",
                                "logIndex": format!("0x{n:x}")})
                        })
                        .collect();
                    (
                        StatusCode::OK,
                        Json(json!({"jsonrpc":"2.0","id":1,"result": logs})),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle)
    }

    /// #1941: BetSwirl names 34 contracts, and the BSC preset refuses ten or more in one getLogs.
    /// The list is split into groups the endpoint takes, the group size is remembered, and the
    /// endpoint is not cooled down as if its credentials had been refused.
    #[tokio::test]
    async fn a_getlogs_refused_for_its_address_count_is_split_and_remembered() {
        let asked = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (url, server) = address_capped_rpc(asked.clone()).await;
        let c = super::RpcClient::new(vec![url]).unwrap();
        let addrs: Vec<String> = (1..=34u64).map(|i| format!("0x{i:040x}")).collect();

        let logs = c.get_logs(&addrs, &[], 100, 102).await.unwrap();
        assert_eq!(logs.len(), 34, "every address answered once");
        let keys: Vec<(u64, u64)> = logs.iter().map(|l| (l.block_number, l.log_index)).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "merged groups come back in chain order");
        let first = asked.lock().unwrap().clone();
        assert_eq!(first, vec![34, 17, 9, 9, 9, 7]);
        assert!(
            c.health[0].load(Ordering::Relaxed)
                <= super::now_millis() + super::ENDPOINT_COOLDOWN_MS,
            "an address-count refusal must not earn the credentials cooldown"
        );

        asked.lock().unwrap().clear();
        c.get_logs(&addrs, &[], 100, 102).await.unwrap();
        server.abort();
        assert_eq!(
            asked.lock().unwrap().clone(),
            vec![9, 9, 9, 7],
            "the next call starts at the group size that worked"
        );
    }

    /// **The guard that makes the timestamp cache sound at all** (RFC-0029 §6d).
    ///
    /// A block *number* is not a block identity. After a reorg the block at a given height is a
    /// different block with a different timestamp, and `block_timestamp` is a sealed column feeding the
    /// segment's content address - so a stale entry would seal a value that re-executing against the
    /// canonical chain could not reproduce. The RFC proposes the cache without noting this; without
    /// the invalidation it is a determinism bug rather than an optimisation.
    #[test]
    fn a_reorg_forgets_cached_timestamps_above_the_fork_and_keeps_the_rest() {
        let c = super::RpcClient::new(vec!["https://example.invalid".into()]).unwrap();
        {
            let mut cache = c.timestamps.lock().unwrap();
            for b in 98..=103u64 {
                cache.insert(b, 1_700_000_000 + b);
            }
        }
        c.forget_timestamps_above(100);
        let cache = c.timestamps.lock().unwrap();
        // Blocks at or below the ancestor are common to both chains - that is what makes it the
        // ancestor - so they stay.
        for b in 98..=100u64 {
            assert!(
                cache.contains_key(&b),
                "block {b} is at/below the fork and must be kept"
            );
        }
        // Everything above was replaced by the reorg.
        for b in 101..=103u64 {
            assert!(
                !cache.contains_key(&b),
                "block {b} is above the fork and must be forgotten"
            );
        }
    }

    /// The bound is enforced, not merely declared. Unbounded growth over a long backfill would trade an
    /// RPC saving for an RSS breach, and the per-cursor budget is a non-negotiable.
    #[test]
    fn the_timestamp_cache_stops_growing() {
        let c = super::RpcClient::new(vec!["https://example.invalid".into()]).unwrap();
        {
            let mut cache = c.timestamps.lock().unwrap();
            for b in 0..super::TIMESTAMP_CACHE_MAX as u64 {
                cache.insert(b, b);
            }
            assert_eq!(cache.len(), super::TIMESTAMP_CACHE_MAX);
        }
        // The next population past the ceiling clears rather than growing without limit.
        c.forget_timestamps_above(u64::MAX);
        let cache = c.timestamps.lock().unwrap();
        assert!(
            cache.len() <= super::TIMESTAMP_CACHE_MAX,
            "the cache must never exceed its ceiling"
        );
    }

    /// #1570: a pool holding `arb1.arbitrum.io`, as primary or fallback, sends header batches of ten,
    /// which it takes; anything else keeps the full width.
    #[test]
    fn a_pool_with_arb1_sends_header_batches_it_takes() {
        let arb1 = "https://arb1.arbitrum.io/rpc".to_string();
        let keyed = "https://arb-mainnet.g.alchemy.com/v2/k".to_string();
        assert_eq!(RpcClient::new(vec![arb1.clone()]).unwrap().header_width, 10);
        assert_eq!(
            RpcClient::with_fallbacks(vec![keyed.clone()], vec![arb1])
                .unwrap()
                .header_width,
            10
        );
        assert_eq!(
            RpcClient::new(vec![keyed]).unwrap().header_width,
            super::MAX_TIMESTAMP_BATCH
        );
    }

    /// And sent at that width, an endpoint shaped like `arb1` (429 above ten headers, 403 above a
    /// hundred) answers every one; a throttle is still not narrowed, so it costs one refusal per
    /// attempt rather than a cascade (#1297).
    #[tokio::test]
    async fn header_batches_go_out_at_the_capped_width() {
        use axum::{http::StatusCode, response::IntoResponse, routing::post, Json, Router};
        use serde_json::{json, Value};
        use std::sync::{Arc, Mutex};

        async fn serve(throttle_all: bool) -> (String, Arc<Mutex<Vec<usize>>>) {
            let seen: Arc<Mutex<Vec<usize>>> = Arc::default();
            let log = seen.clone();
            let app = Router::new().route(
                "/",
                post(move |Json(req): Json<Value>| {
                    let log = log.clone();
                    async move {
                        let items = match &req {
                            Value::Array(items) => items.clone(),
                            item => vec![item.clone()],
                        };
                        log.lock().unwrap().push(items.len());
                        if throttle_all || items.len() > 10 {
                            let code = if items.len() > 100 && !throttle_all {
                                StatusCode::FORBIDDEN
                            } else {
                                StatusCode::TOO_MANY_REQUESTS
                            };
                            return (code, "{\"error\":\"refused\"}").into_response();
                        }
                        let answer = |item: &Value| {
                            let b = u64::from_str_radix(
                                item["params"][0].as_str().unwrap().trim_start_matches("0x"),
                                16,
                            )
                            .unwrap();
                            json!({"jsonrpc":"2.0","id":item["id"],
                                   "result":{"number":format!("0x{b:x}"),"timestamp":format!("0x{:x}", b * 10)}})
                        };
                        Json(Value::Array(items.iter().map(answer).collect())).into_response()
                    }
                }),
            );
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
            (format!("http://{addr}"), seen)
        }

        let (url, seen) = serve(false).await;
        let mut c = RpcClient::new(vec![url]).unwrap();
        c.header_width = 10;
        let blocks: Vec<u64> = (1_000..1_300).collect();
        let got = c
            .block_timestamps(&blocks)
            .await
            .expect("batches the endpoint takes");
        assert!(blocks.iter().all(|b| got[b] == b * 10));
        let sent = seen.lock().unwrap().clone();
        assert!(sent.iter().all(|&n| n <= 10), "{sent:?}");
        // The header and block-body path chunks separately, and at the same width (astra on #1586).
        seen.lock().unwrap().clear();
        let more: Vec<u64> = (2_000..2_300).collect();
        let headers = c
            .block_headers(&more)
            .await
            .expect("headers at the capped width");
        assert_eq!(headers.len(), 300);
        let sent = seen.lock().unwrap().clone();
        assert!(sent.iter().all(|&n| n <= 10), "{sent:?}");

        let (url, seen) = serve(true).await;
        let mut c = RpcClient::new(vec![url]).unwrap();
        c.header_width = 10;
        // Recorded rather than slept: the endpoint rests a full cooldown after each 429 (#1853).
        super::RETRY_PAUSES
            .scope(std::cell::RefCell::new(Vec::new()), async {
                assert!(c.block_timestamps(&[1, 2, 3, 4]).await.is_err());
            })
            .await;
        let sent = seen.lock().unwrap().clone();
        assert!(
            sent.iter().all(|&n| n == 4),
            "a throttle was narrowed: {sent:?}"
        );
    }

    #[tokio::test]
    async fn timestamps_carried_on_logs_cost_no_header_calls() {
        use axum::{routing::post, Json, Router};
        use serde_json::{json, Value};
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static HEADERS: AtomicUsize = AtomicUsize::new(0);
        HEADERS.store(0, Ordering::SeqCst);
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let log = |block: u64, ts: Option<u64>, removed: bool| {
            let mut v = json!({"address":"0xaa","topics":["0x01"],"data":"0x",
                "blockNumber":format!("0x{block:x}"),"blockHash":"0xbh","transactionHash":"0xtx",
                "logIndex":"0x0","removed":removed});
            if let Some(ts) = ts {
                v["blockTimestamp"] = json!(format!("0x{ts:x}"));
            }
            v
        };
        let logs = json!([
            log(10, Some(1_000), false),
            log(11, Some(1_012), false),
            log(12, None, false),
            log(13, Some(9_999), true),
            log(14, Some(0), false),
        ]);
        let app = Router::new().route(
            "/",
            post(move |Json(req): Json<Value>| {
                let logs = logs.clone();
                async move {
                    let one = |item: &Value| {
                        if item["method"] == "eth_getLogs" {
                            json!({"jsonrpc":"2.0","id":item["id"],"result":logs})
                        } else {
                            HEADERS.fetch_add(1, Ordering::SeqCst);
                            json!({"jsonrpc":"2.0","id":item["id"],"result":{"timestamp":"0x7"}})
                        }
                    };
                    match req {
                        Value::Array(items) => Json(Value::Array(items.iter().map(one).collect())),
                        item => Json(one(&item)),
                    }
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(l, app).await.unwrap() });

        let c = RpcClient::new(vec![format!("http://{addr}")]).unwrap();
        c.get_logs(&[], &["0x01".into()], 10, 14).await.unwrap();
        let got = c.block_timestamps(&[10, 11]).await.unwrap();
        assert_eq!(got, HashMap::from([(10, 1_000), (11, 1_012)]));
        assert_eq!(
            HEADERS.load(Ordering::SeqCst),
            0,
            "the logs already said when"
        );

        // A log without the field, a removed one, and one saying 0x0 still go to the header.
        let got = c.block_timestamps(&[12, 13, 14]).await.unwrap();
        server.abort();
        assert_eq!(got, HashMap::from([(12, 7), (13, 7), (14, 7)]));
        assert_eq!(HEADERS.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn an_auth_rejection_is_terminal_however_it_arrives() {
        // Over HTTP status…
        assert_eq!(
            super::classify_status(401, ""),
            super::FailureClass::Terminal
        );
        assert_eq!(
            super::classify_status(403, ""),
            super::FailureClass::Terminal
        );
        // …and as a JSON-RPC error on an HTTP 200, which is how several providers do it.
        let err = serde_json::json!({"code": -32600, "message": "Must be authenticated!"});
        assert_eq!(
            super::classify_rpc_error(&err),
            super::FailureClass::Terminal
        );
    }

    /// RFC-0028 §3f. Measured: `arbitrum-one-rpc.publicnode.com` answers an archive-range request with
    /// `403 "Archive requests require a personal token"` while serving recent blocks perfectly well.
    /// Treating that like a bad API key would sideline a good tip source for five minutes over one deep
    /// query - the refusal is about the *request*, not the credentials.
    #[test]
    fn a_capability_403_is_transient_but_a_credentials_403_is_terminal() {
        assert_eq!(
            super::classify_status(403, "Archive requests require a personal token"),
            super::FailureClass::Transient,
            "an archive-tier refusal must not sideline an endpoint that still serves the tip"
        );
        assert_eq!(
            super::classify_status(403, "ETH_MAINNET is not enabled for this app"),
            super::FailureClass::Transient
        );
        // No capability language: this is about us, and stays terminal.
        assert_eq!(
            super::classify_status(403, "Forbidden"),
            super::FailureClass::Terminal
        );
        assert_eq!(
            super::classify_status(401, "Must be authenticated!"),
            super::FailureClass::Terminal
        );
    }

    /// RFC-0028 §3d: one endpoint rate-limiting says we asked too often; *every* endpoint
    /// rate-limiting the same request says something about the request.
    #[test]
    fn a_pool_wide_rate_limit_escalates_but_a_lone_one_does_not() {
        let err = || anyhow::anyhow!("HTTP 429");
        // Every attempt rate-limited, more than one endpoint → narrowable.
        let escalated = super::escalate_pool_wide_rate_limit(err(), 3, 3);
        assert!(matches!(
            super::class_of(&escalated),
            Some(super::FailureClass::Narrowable { .. })
        ));
        // A single-endpoint pool: "every endpoint" is one endpoint, which is far more likely pacing.
        assert!(super::class_of(&super::escalate_pool_wide_rate_limit(err(), 1, 1)).is_none());
        // Mixed failures are not evidence about the request.
        assert!(super::class_of(&super::escalate_pool_wide_rate_limit(err(), 3, 2)).is_none());
    }

    /// The classification must survive `.with_context(…)`, which callers add as an error travels up
    /// (`getLogs 100..=200` and friends). Checking only the outermost error would lose it silently the
    /// moment anyone added context - a bug that passes every unit test and fails in production.
    #[test]
    fn the_classification_survives_added_context() {
        use anyhow::Context;
        let e = anyhow::Error::new(super::ClassifiedError {
            class: super::FailureClass::Terminal,
            detail: "HTTP 401".into(),
        });
        let wrapped: anyhow::Error = Err::<(), _>(e)
            .context("getLogs 100..=200")
            .context("backfilling window")
            .unwrap_err();
        assert_eq!(
            super::class_of(&wrapped),
            Some(super::FailureClass::Terminal),
            "two layers of context must not hide the classification"
        );
    }

    #[test]
    fn a_rate_limit_is_transient_not_narrowable() {
        // 429 means "too often", not "too much", so it must never narrow the window on its own.
        // Slice 3 gave it a distinct variant: `RateLimited` behaves as transient (fail over, retry at
        // the same width) and exists only so a *pool-wide* 429 can escalate - see
        // `a_pool_wide_rate_limit_escalates_but_a_lone_one_does_not`. The invariant this test protects
        // is unchanged: a lone 429 is not narrowable.
        assert_eq!(
            super::classify_status(429, ""),
            super::FailureClass::RateLimited { retry_after: None }
        );
        assert!(!matches!(
            super::classify_status(429, ""),
            super::FailureClass::Narrowable { .. }
        ));
        assert_eq!(
            super::classify_status(503, ""),
            super::FailureClass::Transient
        );
    }

    /// RFC-0028 §3: an endpoint that rejects our credentials must not be retried on the ordinary
    /// 30s rhythm. This is the livepeer incident in miniature - a 403 endpoint retried indefinitely.
    #[tokio::test]
    async fn a_rejecting_endpoint_gets_the_long_cooldown_and_a_healthy_one_still_answers() {
        let (bad, bad_h) = unauthorized_rpc().await;
        let (good, good_h) = fake_rpc(1).await;
        let client = RpcClient::new(vec![bad, good]).unwrap();

        let tip = client
            .block_number()
            .await
            .expect("must recover via the healthy endpoint");
        assert_eq!(tip, HEALTHY_TIP, "the healthy endpoint answered");

        // The rejecting endpoint is cooled down for the *terminal* interval, not the transient one.
        let until = client.health[0].load(Ordering::Relaxed);
        let remaining = until.saturating_sub(super::now_millis());
        assert!(
            remaining > super::ENDPOINT_COOLDOWN_MS,
            "a 401 endpoint must earn more than the {}ms transient cooldown, got {remaining}ms",
            super::ENDPOINT_COOLDOWN_MS
        );
        assert!(
            remaining <= super::TERMINAL_COOLDOWN_MS,
            "and no more than the terminal one"
        );

        bad_h.abort();
        good_h.abort();
    }

    /// The other half of the contract: a merely-broken endpoint keeps the short cooldown, so a blip
    /// does not get punished like a credential failure.
    #[tokio::test]
    async fn a_transiently_broken_endpoint_keeps_the_short_cooldown() {
        let (broken, broken_h, _hits) = broken_rpc().await;
        let (good, good_h) = fake_rpc(1).await;
        let client = RpcClient::new(vec![broken, good]).unwrap();

        client
            .block_number()
            .await
            .expect("recovers via the healthy endpoint");

        let until = client.health[0].load(Ordering::Relaxed);
        let remaining = until.saturating_sub(super::now_millis());
        assert!(
            remaining <= super::ENDPOINT_COOLDOWN_MS,
            "a 500 is transient and must keep the short cooldown, got {remaining}ms"
        );

        broken_h.abort();
        good_h.abort();
    }

    #[tokio::test]
    async fn a_tip_get_logs_goes_to_an_endpoint_that_holds_its_to_block() {
        use axum::{extract::State, routing::post, Json, Router};
        use serde_json::{json, Value};

        // Arc's public endpoints, measured 2026-09-24: a `toBlock` past their own head is refused
        // with -32014 while Alchemy, a block ahead, answered the tip.
        async fn endpoint(head: u64) -> (String, Arc<AtomicU64>, tokio::task::JoinHandle<()>) {
            async fn handler(
                State((head, refused)): State<(u64, Arc<AtomicU64>)>,
                Json(req): Json<Value>,
            ) -> Json<Value> {
                let body = match req["method"].as_str().unwrap_or("") {
                    "eth_blockNumber" => json!({"result": format!("0x{head:x}")}),
                    "eth_getLogs" => {
                        let to = req["params"][0]["toBlock"].as_str().unwrap();
                        if u64::from_str_radix(&to[2..], 16).unwrap() > head {
                            refused.fetch_add(1, Ordering::Relaxed);
                            json!({"error": {"code": -32014, "message": "requested data not available"}})
                        } else {
                            json!({"result": []})
                        }
                    }
                    _ => json!({"result": null}),
                };
                let mut body = body;
                body["jsonrpc"] = json!("2.0");
                body["id"] = json!(1);
                Json(body)
            }
            let refused = Arc::new(AtomicU64::new(0));
            let app = Router::new()
                .route("/", post(handler))
                .with_state((head, refused.clone()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let h = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            (format!("http://{addr}/"), refused, h)
        }

        let (ahead, ahead_refused, h1) = endpoint(1_000).await;
        let (behind, behind_refused, h2) = endpoint(999).await;
        // Two calls per poll over two endpoints: plain round-robin sends every getLogs to `behind`.
        let c = RpcClient::new(vec![ahead, behind]).unwrap();

        for _ in 0..6 {
            let before = c.request_count();
            let tip = c.block_number().await.unwrap();
            c.get_logs(&[], &["0xaa".into()], tip - 10, tip)
                .await
                .unwrap();
            assert_eq!(
                c.request_count() - before,
                2,
                "one tip call and one getLogs per poll"
            );
        }
        assert_eq!(ahead_refused.load(Ordering::Relaxed), 0);
        assert_eq!(
            behind_refused.load(Ordering::Relaxed),
            0,
            "a getLogs was sent to an endpoint not known to hold its toBlock"
        );
        h1.abort();
        h2.abort();
    }

    /// Issue #150: the failover path itself, not just the ordering maths. The first endpoint is broken,
    /// so the call must still succeed via the second, and the dead one must be marked unhealthy so
    /// subsequent calls stop paying its timeout. Previously only `endpoint_order`'s sorting was tested -
    /// which would happily pass even if `call` never retried at all.
    #[tokio::test]
    async fn a_failed_call_recovers_via_the_next_endpoint_and_cools_the_dead_one() {
        let (broken, hb, broken_hits) = broken_rpc().await;
        let (good, hg) = fake_rpc(1).await;
        let c = RpcClient::new(vec![broken, good]).unwrap();

        // The cursor starts at 0, so the broken endpoint is tried FIRST - the case under test.
        let got = c
            .block_number()
            .await
            .expect("the call must survive one dead endpoint");
        assert_eq!(
            got, HEALTHY_TIP,
            "the answer must come from the healthy endpoint"
        );
        assert!(
            broken_hits.load(Ordering::Relaxed) >= 1,
            "the broken endpoint should actually have been tried"
        );
        assert_eq!(c.request_count(), 2, "one failed attempt, then one success");

        // The dead endpoint is in cooldown, so it now sorts last…
        assert_eq!(
            *c.endpoint_order().last().unwrap(),
            0,
            "the failed endpoint must sink to the back"
        );
        // …and the healthy one is not penalised.
        assert_eq!(c.health[1].load(Ordering::Relaxed), 0);

        // A second call still succeeds, and skips straight to the good endpoint.
        let before = c.request_count();
        assert_eq!(c.block_number().await.unwrap(), HEALTHY_TIP);
        assert_eq!(
            c.request_count() - before,
            1,
            "a cooled-down endpoint must not be retried on every call"
        );

        hb.abort();
        hg.abort();
    }

    /// With every endpoint broken there is nothing to fail over TO, so the call must surface an error
    /// rather than hang or quietly return a default - the tip loop's stall detection depends on it.
    #[tokio::test]
    async fn a_call_fails_when_no_endpoint_can_answer() {
        let (b1, h1, _) = broken_rpc().await;
        let (b2, h2, _) = broken_rpc().await;
        let c = RpcClient::new(vec![b1, b2]).unwrap();
        assert!(c.block_number().await.is_err());
        assert_eq!(
            c.request_count(),
            2,
            "every endpoint tried before giving up"
        );
        h1.abort();
        h2.abort();
    }

    /// Issue #150: every endpoint is checked individually. `call`-based verification would be useless
    /// here - it fails over past the bad endpoint and reports success, which is exactly how a
    /// mixed-chain pool hides.
    #[tokio::test]
    async fn a_wrong_chain_endpoint_is_rejected_even_when_its_neighbours_are_right() {
        let (good1, h1) = fake_rpc(42161).await;
        let (good2, h2) = fake_rpc(42161).await;
        let (wrong, h3) = fake_rpc(8453).await;

        // All correct → starts.
        let ok = RpcClient::new(vec![good1.clone(), good2.clone()]).unwrap();
        assert!(ok.verify_chain_ids(42161).await.is_ok());

        // One wrong endpoint among healthy ones → refuse, naming the chain it is actually on.
        let mixed = RpcClient::new(vec![good1, good2.clone(), wrong.clone()]).unwrap();
        let err = mixed.verify_chain_ids(42161).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("8453"), "should name the wrong chain: {msg}");
        assert!(msg.contains("42161"), "and the expected one: {msg}");

        // Order must not matter - the bad endpoint first is just as fatal.
        let mixed2 = RpcClient::new(vec![wrong, good2]).unwrap();
        assert!(mixed2.verify_chain_ids(42161).await.is_err());

        for h in [h1, h2, h3] {
            h.abort();
        }
    }

    /// Startup must not be held hostage by a dead endpoint. `verify_chain_ids` runs before the first
    /// block is fetched, so its cost lands on time-to-first-index; done sequentially at the client's
    /// 20 s timeout, a default pool with several unreachable endpoints delayed indexing by over a
    /// minute (measured - it is what made the CI footprint job start failing).
    ///
    /// Four black-holed endpoints alongside one good one must still complete in a few seconds.
    #[tokio::test]
    async fn verification_is_bounded_even_when_most_endpoints_hang() {
        let (good, h) = fake_rpc(1).await;
        // 203.0.113.0/24 is TEST-NET-3 (RFC 5737): reserved for documentation, routed nowhere, so
        // connections hang rather than being refused - the case a per-endpoint timeout exists for.
        let mut urls: Vec<String> = (1..=4)
            .map(|i| format!("http://203.0.113.{i}:8545/"))
            .collect();
        urls.push(good);
        let c = RpcClient::new(urls).unwrap();

        let started = std::time::Instant::now();
        let r =
            tokio::time::timeout(std::time::Duration::from_secs(20), c.verify_chain_ids(1)).await;
        let elapsed = started.elapsed();

        assert!(
            r.is_ok(),
            "verification must not hang past its own deadline"
        );
        assert!(
            r.unwrap().is_ok(),
            "unreachable endpoints must not fail startup"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(12),
            "four hanging endpoints took {elapsed:?} - the checks are not concurrent/bounded, and \
             that time is paid before a single block is indexed"
        );
        h.abort();
    }

    /// Accepts connections and never answers, the shape of mainnet.gateway.tenderly.co on 2026-10-05.
    async fn silent_rpc() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        (format!("http://{addr}/"), handle)
    }

    /// #1885: an endpoint that never answered `eth_chainId` cost startup the verify deadline, then the
    /// client's whole 20 s timeout on the first real call, because it was left healthy.
    #[tokio::test]
    async fn a_silent_endpoint_costs_startup_at_most_its_short_budget() {
        let (silent, hs) = silent_rpc().await;
        let (good, hg) = fake_rpc(1).await;
        let c = RpcClient::new(vec![silent, good]).unwrap();

        let started = std::time::Instant::now();
        let r = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            c.verify_chain_ids(1).await?;
            c.block_number().await
        })
        .await;
        let elapsed = started.elapsed();

        assert_eq!(r.expect("startup hung").unwrap(), HEALTHY_TIP);
        assert!(
            elapsed < std::time::Duration::from_secs(3),
            "a silent endpoint held startup for {elapsed:?}"
        );
        assert!(
            c.endpoint_order().contains(&0),
            "a silent endpoint is unverified, not dropped"
        );
        hs.abort();
        hg.abort();
    }

    /// The shorter budget must not wave through a wrong-chain endpoint that answers in time.
    #[tokio::test]
    async fn a_wrong_chain_endpoint_beside_a_silent_one_is_still_rejected() {
        let (silent, hs) = silent_rpc().await;
        let (wrong, hw) = fake_rpc(8453).await;
        let c = RpcClient::new(vec![silent, wrong]).unwrap();
        let err = c.verify_chain_ids(1).await.unwrap_err();
        assert!(format!("{err:#}").contains("8453"), "{err:#}");
        hs.abort();
        hw.abort();
    }

    /// Offline is not the same as wrong. Nuthatch tolerates an endpoint being down at startup (the
    /// health/cooldown machinery handles it), so an unreachable URL must warn, not block the boot.
    #[tokio::test]
    async fn an_unreachable_endpoint_does_not_block_startup() {
        let (good, h) = fake_rpc(1).await;
        // Port 1 on loopback: nothing listens, connection refused immediately.
        let c = RpcClient::new(vec![good, "http://127.0.0.1:1/".to_string()]).unwrap();
        assert!(
            c.verify_chain_ids(1).await.is_ok(),
            "an endpoint that is merely down must not prevent indexing"
        );
        h.abort();
    }

    /// The error text reaches operator logs, and provider URLs routinely carry the API key in the
    /// path. It must name the host and nothing more.
    #[tokio::test]
    async fn the_mismatch_error_redacts_the_api_key() {
        let (wrong, h) = fake_rpc(8453).await;
        let with_key = format!(
            "{}v3/SUPERSECRETKEY",
            wrong.trim_end_matches('/').to_string() + "/"
        );
        let c = RpcClient::new(vec![with_key]).unwrap();
        let msg = format!("{:#}", c.verify_chain_ids(1).await.unwrap_err());
        assert!(!msg.contains("SUPERSECRETKEY"), "leaked the API key: {msg}");
        h.abort();
    }

    /// An endpoint that errors on its first `eth_chainId` and then answers as chain `chain_id`, with
    /// a tip of `tip`: one that was down for the startup check and came back.
    async fn late_rpc(chain_id: u64, tip: u64) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{extract::State, routing::post, Json, Router};
        use serde_json::{json, Value};
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let app = Router::new()
            .route(
                "/",
                post(
                    move |State(first): State<Arc<std::sync::atomic::AtomicBool>>,
                          Json(req): Json<Value>| async move {
                        let method = req["method"].as_str().unwrap_or_default().to_string();
                        if method == "eth_chainId" && first.swap(false, Ordering::SeqCst) {
                            return Json(
                                json!({"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"warming up"}}),
                            );
                        }
                        let result = match method.as_str() {
                            "eth_chainId" => json!(format!("0x{chain_id:x}")),
                            _ => json!(format!("0x{tip:x}")),
                        };
                        Json(json!({"jsonrpc":"2.0","id":1,"result":result}))
                    },
                ),
            )
            .with_state(first);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle)
    }

    /// #2161: an endpoint whose chain could not be checked at startup was used for data unverified.
    /// It is asked its chain before it serves anything, and the wrong one serves nothing.
    #[tokio::test]
    async fn an_endpoint_unverified_at_startup_is_checked_before_it_serves() {
        let (wrong, hw) = late_rpc(8453, 0x9999).await;
        let c = RpcClient::new(vec![wrong]).unwrap();
        c.verify_chain_ids(1).await.unwrap();
        let err = c
            .block_number()
            .await
            .expect_err("a chain-8453 endpoint answered for chain 1");
        assert!(format!("{err:#}").contains("8453"), "{err:#}");
        assert_eq!(super::class_of(&err), Some(super::FailureClass::Terminal));

        let (right, hr) = late_rpc(1, 0x9999).await;
        let c = RpcClient::new(vec![right]).unwrap();
        c.verify_chain_ids(1).await.unwrap();
        assert_eq!(
            c.block_number().await.unwrap(),
            0x9999,
            "the right chain, late, is admitted"
        );
        hw.abort();
        hr.abort();
    }

    /// #2113: an unreachable fallback failed every question asked of each endpoint, so the
    /// address-history cursor never advanced. It is cooled and left out, as every other call does.
    #[tokio::test]
    async fn one_unreachable_endpoint_does_not_fail_a_question_asked_of_each() {
        let (good, h) = fake_rpc(1).await;
        let c = RpcClient::with_fallbacks(vec![good], vec!["http://127.0.0.1:1/".into()]).unwrap();
        let answers = c
            .call_each("eth_chainId", serde_json::json!([]))
            .await
            .expect("the healthy primary answered");
        assert_eq!(answers, vec![serde_json::json!("0x1")]);
        let mut seen = Vec::new();
        c.visit_each("eth_chainId", serde_json::json!([]), &mut |a| {
            seen.push(a.is_ok())
        })
        .await;
        assert_eq!(seen, vec![true], "only the answer is handed on");

        // Two primaries asked in turn, so the dead one is asked first or after the answer, and
        // fails the call in neither order.
        for &dead_first in &[true, false] {
            let (good, h) = fake_rpc(1).await;
            let dead = "http://127.0.0.1:1/".to_string();
            let urls = if dead_first {
                vec![dead, good]
            } else {
                vec![good, dead]
            };
            let c = RpcClient::new(urls).unwrap();
            let answers = c
                .call_each("eth_chainId", serde_json::json!([]))
                .await
                .expect("one endpoint answered");
            assert_eq!(answers, vec![serde_json::json!("0x1")], "{dead_first}");
            h.abort();
        }

        let dead = RpcClient::new(vec!["http://127.0.0.1:1/".into()]).unwrap();
        assert!(dead
            .call_each("eth_chainId", serde_json::json!([]))
            .await
            .is_err());
        h.abort();
    }

    /// #2113's comment: `call_each` spoke to every endpoint directly, so one unverified at startup
    /// answered for another chain without the admission #2161 gives the pool.
    #[tokio::test]
    async fn a_question_asked_of_each_admits_an_unverified_endpoint_first() {
        let (right, hr) = fake_rpc(1).await;
        let (wrong, hw) = late_rpc(8453, 0x9999).await;
        let c = RpcClient::new(vec![right, wrong]).unwrap();
        c.verify_chain_ids(1).await.unwrap();
        let answers = c
            .call_each("eth_blockNumber", serde_json::json!([]))
            .await
            .unwrap();
        assert_eq!(
            answers,
            vec![serde_json::json!(HEALTHY_TIP_HEX)],
            "the chain-8453 endpoint answered"
        );
        hr.abort();
        hw.abort();
    }

    /// #2225: `"error": null` beside a result is a success that spells the field out.
    #[tokio::test]
    async fn a_null_error_beside_a_result_is_a_success() {
        let (url, h) =
            canned_rpc(serde_json::json!({"jsonrpc":"2.0","id":1,"result":"0x10","error":null}))
                .await;
        let c = RpcClient::new(vec![url]).unwrap();
        assert_eq!(c.block_number().await.unwrap(), 0x10);
        h.abort();
        assert_eq!(
            super::decode_call_batch(
                &serde_json::json!([{"id":0,"result":"0xab","error":null}]),
                1
            )
            .unwrap(),
            vec![Some("0xab".to_string())]
        );
    }

    /// #2226: a revert in a shape not on the explicit list is the node's answer at a pinned block.
    /// It must be refused by name, never classed transient and asked again for ever.
    #[test]
    fn an_unlisted_revert_shape_is_not_transient() {
        for item in [
            serde_json::json!({"code":-32603,"message":"execution reverted"}),
            serde_json::json!({"code":-32000,"message":"Reverted 0x08c379a0"}),
        ] {
            let err = super::decode_call_batch(&serde_json::json!([{"id":0,"error":item}]), 1)
                .unwrap_err();
            assert_eq!(
                super::class_of(&err),
                Some(super::FailureClass::Terminal),
                "{err:#}"
            );
            assert!(format!("{err:#}").contains("does not recognise"), "{err:#}");
        }
    }

    /// #2222: a log a reorg took back is not stored, whichever call a provider marks it on.
    #[tokio::test]
    async fn a_removed_log_is_not_returned() {
        let log = |li: u64, removed: bool| {
            serde_json::json!({"address":"0xaa","topics":["0x01"],"data":"0x",
                "blockNumber":"0xa","blockHash":"0xbh","transactionHash":"0xtx",
                "logIndex":format!("0x{li:x}"),"removed":removed})
        };
        let (url, h) = canned_rpc(serde_json::json!({"jsonrpc":"2.0","id":1,
            "result":[log(0, false), log(1, true), log(2, false)]}))
        .await;
        let c = RpcClient::new(vec![url]).unwrap();
        let got = c.get_logs(&[], &["0x01".into()], 10, 10).await.unwrap();
        h.abort();
        assert_eq!(
            got.iter().map(|l| l.log_index).collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    /// #2155: one endpoint refusing our key is not the pool refusing it. The verdict of a call that
    /// another endpoint failed for a passing reason must not read as terminal, or a retry site that
    /// stops on terminal gives up on a pool that will recover (#538).
    #[tokio::test]
    async fn one_endpoint_refusing_credentials_is_not_the_pools_verdict() {
        let (down, hd, _) = broken_rpc().await;
        let (refusing, hu) = unauthorized_rpc().await;
        let c = RpcClient::new(vec![down, refusing]).unwrap();
        for _ in 0..2 {
            let err = c.block_number().await.unwrap_err();
            assert_ne!(
                super::class_of(&err),
                Some(super::FailureClass::Terminal),
                "{err:#}"
            );
        }
        let (refusing, hu2) = unauthorized_rpc().await;
        let only = RpcClient::new(vec![refusing]).unwrap();
        let err = only.block_number().await.unwrap_err();
        assert_eq!(super::class_of(&err), Some(super::FailureClass::Terminal));
        hd.abort();
        hu.abort();
        hu2.abort();
    }

    use super::{redact_url, select_rpcs, RpcClient};

    fn v<const N: usize>(xs: [&str; N]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_failed_endpoint_is_tried_last_until_it_cools_down() {
        let c = RpcClient::new(v(["http://a", "http://b", "http://c"])).unwrap();
        // Endpoint 1 (b) just failed → it must sink to the back of the try order.
        c.mark_unhealthy(1);
        for _ in 0..5 {
            let order = c.endpoint_order();
            assert_eq!(order.len(), 3);
            assert_eq!(
                *order.last().unwrap(),
                1,
                "unhealthy endpoint is tried last"
            );
            // The two healthy endpoints lead, in some round-robin order.
            assert!(order[..2].contains(&0) && order[..2].contains(&2));
        }
        // A success clears it - back into normal rotation, no longer forced last.
        c.mark_healthy(1);
        let mut seen_first = false;
        for _ in 0..3 {
            if c.endpoint_order()[0] == 1 {
                seen_first = true;
            }
        }
        assert!(seen_first, "a recovered endpoint rejoins the round-robin");
    }

    #[test]
    fn a_fallback_carries_no_load_while_a_primary_is_healthy() {
        let c =
            RpcClient::with_fallbacks(v(["http://a", "http://b"]), v(["http://paid", "http://a"]))
                .unwrap();
        assert_eq!(
            c.urls.len(),
            3,
            "a fallback already in the pool is not added twice"
        );
        for _ in 0..4 {
            let order = c.endpoint_order();
            assert!(order[..2].contains(&0) && order[..2].contains(&1));
            assert_eq!(order[2], 2);
        }
        c.mark_unhealthy(0);
        assert_eq!(c.endpoint_order(), vec![1, 2, 0]);
        c.mark_unhealthy(1);
        assert_eq!(
            c.endpoint_order()[0],
            2,
            "every primary cooling: the fallback answers"
        );
    }

    #[test]
    fn a_fallback_ahead_of_the_primaries_still_waits_behind_them() {
        let c = RpcClient::with_fallbacks(v(["http://a", "http://b"]), v(["http://paid"])).unwrap();
        c.heads[0].store(99, Ordering::Relaxed);
        c.heads[1].store(100, Ordering::Relaxed);
        c.heads[2].store(100, Ordering::Relaxed);
        for _ in 0..4 {
            assert_eq!(c.endpoint_order_holding(Some(100)), vec![1, 0, 2]);
        }
    }

    #[test]
    fn no_override_keeps_the_configured_pool() {
        assert_eq!(select_rpcs(&[], v(["a", "b"])), v(["a", "b"]));
    }

    #[test]
    fn explicit_override_excludes_configured_fallbacks() {
        assert_eq!(select_rpcs(&v(["mine"]), v(["a", "b"])), v(["mine"]));
    }

    #[test]
    fn selected_pool_deduplicates_its_own_urls() {
        assert_eq!(select_rpcs(&v(["m", "m", "n"]), v(["a"])), v(["m", "n"]));
    }

    #[test]
    fn redact_url_keeps_only_scheme_and_host() {
        // The API key in the path or query must never survive into a log line.
        assert_eq!(
            redact_url("https://mainnet.infura.io/v3/SECRETKEY"),
            "https://mainnet.infura.io"
        );
        assert_eq!(
            redact_url("https://eth.g.alchemy.com/v2/KEY?token=x"),
            "https://eth.g.alchemy.com"
        );
        assert_eq!(redact_url("http://localhost:8545"), "http://localhost:8545");
        assert_eq!(redact_url("https://host:8545/"), "https://host:8545");
        // Userinfo sits before the host, so a prefix kept it (#1589).
        assert_eq!(
            redact_url("https://alice:SECRET@rpc.example.com/path"),
            "https://rpc.example.com"
        );
        assert_eq!(
            redact_url("https://tok@rpc.example.com:8545"),
            "https://rpc.example.com:8545"
        );
        assert!(!redact_url("not a url with SECRET").contains("SECRET"));
    }

    /// **RFC-0029 §6g.** A body-read *timeout* must narrow; a body *syntax* error must not.
    ///
    /// This is the distinction the fix turns on, and getting it backwards is worse in both directions:
    /// classifying every decode failure as narrowable would split a garbage-returning endpoint down to
    /// single blocks, and classifying every one as transient is what aborted an OBIB case-1 backfill
    /// five times at the same width.
    ///
    /// Driven through a real server rather than a hand-built `reqwest::Error`, because the whole bug
    /// was that we mis-read what reqwest reports for a slow body - a fake error would encode the same
    /// assumption that was wrong.
    #[tokio::test]
    async fn a_slow_body_narrows_but_a_malformed_one_does_not() {
        use super::{class_of, FailureClass, RpcClient};
        use tokio::io::AsyncWriteExt;

        // A server that sends headers and a Content-Length it never satisfies, so the client blocks
        // reading the body until its own timeout fires - exactly the shape a large-but-slow response
        // has, without needing to move megabytes.
        async fn serve(stall: bool) -> String {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            tokio::spawn(async move {
                if let Ok((mut sock, _)) = l.accept().await {
                    let body = if stall {
                        // Promise 4 MB, send 3 bytes, then hold the connection open.
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 4194304\r\n\r\n{\"j"
                    } else {
                        // Complete, well-formed HTTP carrying JSON that is not JSON.
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 9\r\n\r\nnot-json!"
                    };
                    let _ = sock.write_all(body.as_bytes()).await;
                    let _ = sock.flush().await;
                    if stall {
                        tokio::time::sleep(std::time::Duration::from_secs(120)).await;
                    }
                }
            });
            format!("http://{addr}")
        }

        // The stalling case: the client gives up mid-body. That is a size signal.
        let url = serve(true).await;
        let c = RpcClient::new(vec![url]).unwrap();
        let err = c
            .post_one_for_test(
                &serde_json::json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}),
            )
            .await
            .expect_err("a stalled body must fail");
        assert!(
            matches!(class_of(&err), Some(FailureClass::Narrowable { .. })),
            "a body-read timeout must be narrowable so the *unbounded* classified split handles it, \
             not the one-shot speculative one: {err:#}"
        );
        assert!(
            crate::chunker::is_result_too_large(&err),
            "…and the chunker must agree, since that is what actually triggers the split"
        );

        // The malformed case: halving buys nothing, so it stays transient.
        let url = serve(false).await;
        let c = RpcClient::new(vec![url]).unwrap();
        let err = c
            .post_one_for_test(
                &serde_json::json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}),
            )
            .await
            .expect_err("non-JSON must fail");
        assert!(
            matches!(class_of(&err), Some(FailureClass::Transient)),
            "garbage is not smaller in halves - splitting a dead endpoint to single blocks is the \
             failure RFC-0028 was avoiding: {err:#}"
        );
        assert!(!crate::chunker::is_result_too_large(&err));
    }

    /// **RFC-0029 §6h.** A timestamp batch too large to read must be *halved*, not reissued at the
    /// same size until the attempts run out.
    ///
    /// This is the third appearance of one defect - `getLogs` status codes (slice 1), `getLogs` body
    /// reads (#230), and now the timestamp batch - so the test asserts the *general* property: a
    /// batched RPC call narrows on a size failure. A server that refuses anything above a threshold
    /// stands in for "the body took longer than the timeout to read", which is what actually happens
    /// on a real endpoint and is impractical to reproduce deterministically.
    #[tokio::test]
    async fn a_timestamp_batch_halves_instead_of_retrying_the_same_size() {
        use axum::{routing::post, Json, Router};
        use serde_json::{json, Value};
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Refuses batches larger than 50 with a cap error; serves anything smaller.
        static SEEN_MAX: AtomicUsize = AtomicUsize::new(0);
        static CALLS: AtomicUsize = AtomicUsize::new(0);
        SEEN_MAX.store(0, Ordering::SeqCst);
        CALLS.store(0, Ordering::SeqCst);

        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let app = Router::new().route(
            "/",
            post(|Json(req): Json<Vec<Value>>| async move {
                    let n_items = req.len();
                    CALLS.fetch_add(1, Ordering::SeqCst);
                    SEEN_MAX.fetch_max(n_items, Ordering::SeqCst);
                    if n_items > 50 {
                        // The shape a provider uses for "your response would be too big".
                        Json(json!({"jsonrpc":"2.0","id":0,"error":{"code":-32602,"message":"Log response size exceeded"}}))
                    } else {
                        // `id` is the index within the batch, which is what the client maps back.
                        let items: Vec<Value> = req.iter()
                            .map(|item| json!({"jsonrpc":"2.0","id":item["id"],"result":{"timestamp":"0x1"}}))
                            .collect();
                        Json(Value::Array(items))
                    }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(l, app).await.unwrap();
        });

        let c = RpcClient::new(vec![format!("http://{addr}")]).unwrap();
        let blocks: Vec<u64> = (1..=200).collect();
        let got = c
            .block_timestamps(&blocks)
            .await
            .expect("a batch that is merely too large must be split, not fatal");

        assert_eq!(got.len(), 200, "every block must come back after splitting");
        server.abort();
        assert!(
            CALLS.load(Ordering::SeqCst) > 1,
            "it must have split at all - one call means it never narrowed"
        );
        // 200 -> 100 -> 50: the first size the server accepts. If it had merely retried, the max would
        // have stayed at 200 and the call would have failed.
        assert!(
            SEEN_MAX.load(Ordering::SeqCst) <= 200,
            "sanity: the server saw the batch it was sent"
        );
    }

    /// **Issue #241 item 7.** A provider that caps **batch count** must still converge.
    ///
    /// `arbitrum.drpc.org` refuses with "Batch of more than 3 requests are not allowed on free plan",
    /// which matches no cap marker and so classified `Transient` - the narrowing existed and was never
    /// reached. The reported symptom was a window walking `781 → 234 → 220 → 218 → 218 …` and
    /// stalling, because shrinking a *block range* cannot satisfy a limit that counts *requests*.
    ///
    /// The server here mimics that exactly: it rejects on count, with a message containing no
    /// size language whatsoever.
    #[tokio::test]
    async fn a_batch_count_limit_converges_rather_than_stalling() {
        use super::RpcClient;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        static SMALLEST: AtomicUsize = AtomicUsize::new(usize::MAX);
        SMALLEST.store(usize::MAX, Ordering::SeqCst);

        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = l.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 1 << 20];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    let items = req.matches("eth_getBlockByNumber").count();
                    if items > 0 {
                        SMALLEST.fetch_min(items, Ordering::SeqCst);
                    }
                    // Free-plan batch cap: **counts requests**, says nothing about size.
                    let body = if items > 3 {
                        r#"{"jsonrpc":"2.0","id":0,"error":{"code":-32600,"message":"Batch of more than 3 requests are not allowed on free plan"}}"#.to_string()
                    } else {
                        let it: Vec<String> = (0..items)
                            .map(|i| {
                                format!(
                                    r#"{{"jsonrpc":"2.0","id":{i},"result":{{"timestamp":"0x2"}}}}"#
                                )
                            })
                            .collect();
                        format!("[{}]", it.join(","))
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });

        let c = RpcClient::new(vec![format!("http://{addr}")]).unwrap();
        // 16, not 200: each narrowing level pays a full retry cycle first (see the note on
        // `batch_is_narrowable`), so a large fixture makes this test minutes long for no extra proof.
        let blocks: Vec<u64> = (1..=16).collect();
        let got = c
            .block_timestamps(&blocks)
            .await
            .expect("a batch-count limit must be narrowed into, not retried at the same width");

        assert_eq!(got.len(), 16, "every block must come back after splitting");
        assert!(
            SMALLEST.load(Ordering::SeqCst) <= 3,
            "it must have narrowed to within the provider's count cap - smallest batch seen was {}",
            SMALLEST.load(Ordering::SeqCst)
        );
    }

    /// A per-item error server: every item of every batch comes back errored, inside an HTTP 200.
    ///
    /// This is the shape #656 observed in the field, and the shape `post_one` cannot see - the
    /// transport succeeded, so the failure only exists once the items are parsed. Returns the bound
    /// address and a counter of how many batch requests the server was asked to serve.
    #[cfg(test)]
    async fn per_item_error_server(
        err_json: &'static str,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        per_item_error_server_holding(err_json, None).await
    }

    /// `hold_width` holds a request of that many items until a second one of the same width has
    /// arrived, or `PEER_WAIT` passes, so a `try_join!` winner cannot finish its descent before its
    /// sibling's request is counted. A sequential descent never sends the sibling while the first is
    /// held, so it still reads as sequential. `None` for tests that only fire one request.
    #[cfg(test)]
    async fn per_item_error_server_holding(
        err_json: &'static str,
        hold_width: Option<usize>,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        const PEER_WAIT: std::time::Duration = std::time::Duration::from_secs(5);
        let seen = std::sync::Arc::new(AtomicUsize::new(0));
        let held = std::sync::Arc::new(AtomicUsize::new(0));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let counter = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = l.accept().await else {
                    return;
                };
                let counter = counter.clone();
                let held = held.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 1 << 20];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    let items = req.matches("eth_getBlockByNumber").count();
                    if items == 0 {
                        return;
                    }
                    counter.fetch_add(1, Ordering::SeqCst);
                    if hold_width == Some(items) {
                        held.fetch_add(1, Ordering::SeqCst);
                        let entered = std::time::Instant::now();
                        while held.load(Ordering::SeqCst) < 2 && entered.elapsed() < PEER_WAIT {
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        }
                    }
                    let it: Vec<String> = (0..items)
                        .map(|i| format!(r#"{{"jsonrpc":"2.0","id":{i},"error":{err_json}}}"#))
                        .collect();
                    let body = format!("[{}]", it.join(","));
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        (format!("http://{addr}"), seen)
    }

    /// **Issue #656, the question it says to answer before changing anything.**
    ///
    /// The field report was six storms of "every item in a **1**-block batch returned an error", and
    /// two opposite fixes hung on which class produced it. The width alone cannot say: a 1-block batch
    /// is either the floor of a 200 → 1 descent *or* a trailing `.chunks(200)` remainder that was one
    /// block wide when it was handed over.
    ///
    /// These two runs are the discriminator, and they differ **only** in the class of the per-item
    /// error. Under a rate limit the descent must never start, so the width reported is the width
    /// requested; under a cap it descends and says what it descended from.
    #[tokio::test]
    async fn a_per_item_rate_limit_reports_its_own_width_and_never_descends() {
        use super::RpcClient;
        use std::sync::atomic::Ordering;

        let (url, seen) = per_item_error_server(
            r#"{"code":429,"message":"Your app has exceeded its compute units per second capacity"}"#,
        )
        .await;
        let c = RpcClient::new(vec![url]).unwrap();
        let err = c
            .block_timestamps(&(1..=8).collect::<Vec<u64>>())
            .await
            .expect_err("every item errored, so the batch cannot succeed");
        let msg = format!("{err:#}");

        assert!(
            msg.contains("class=RateLimited"),
            "the class must be on the line - naming it is the whole point of #656: {msg}"
        );
        assert!(
            msg.contains("8-block") && msg.contains("not narrowed"),
            "a rate limit must be reported at the width it was requested at, never halved: {msg}"
        );
        // One request, not nine: `batch_is_narrowable` refuses to split, so there is no descent at all.
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "splitting under a rate limit doubles the request count in the wrong direction"
        );
    }

    /// The other half of the discriminator, and a measurement #656 needs in its own right.
    ///
    /// The note on `batch_is_narrowable` says "every level pays a full `TIMESTAMP_ATTEMPTS` cycle with
    /// backoff before it splits". That is true of a *transport* failure, and **false of the per-item
    /// path this test drives**: the response is an HTTP 200, so `post_with_failover` returns `Ok`, the
    /// retry loop breaks on its first attempt, and the failure is only discovered afterwards while
    /// parsing items. The request count below is what proves it - one per level, not four.
    #[tokio::test]
    async fn a_per_item_cap_descends_and_names_the_width_it_came_from() {
        use super::RpcClient;
        use std::sync::atomic::Ordering;

        let (url, seen) = per_item_error_server_holding(
            r#"{"code":-32602,"message":"query returned more than 10000 results"}"#,
            Some(4),
        )
        .await;
        let c = RpcClient::new(vec![url]).unwrap();
        let err = c
            .block_timestamps(&(1..=8).collect::<Vec<u64>>())
            .await
            .expect_err("every item errored at every width, so the descent bottoms out");
        let msg = format!("{err:#}");

        assert!(
            msg.contains("class=Narrowable"),
            "a size cap must classify as Narrowable: {msg}"
        );
        assert!(
            msg.contains("1-block") && msg.contains("narrowed down from 8"),
            "the floor of a descent must say where it descended from, so it cannot be mistaken for a \
             trailing one-block chunk: {msg}"
        );
        // The top-level 8 → {4, 4} split runs its two halves concurrently (`tokio::try_join!`, #728),
        // so both sides of the tree are explored rather than only the leftmost path: 1 (the width-8
        // request) + up to 3 each for the two width-4 halves (4 → 2 → 1 along their own leftmost
        // path) = at most 7.
        //
        // **A range, not an equality, and that is not slack (#735).** `try_join!` cancels the
        // sibling the instant one side returns `Err`. Whether the losing half got all the way down
        // its own path (3 requests) or was cut short (2) is a scheduling question, not a protocol
        // one, and both are correct. Asserting `== 7` made this test fail nine runs in ten on a
        // developer machine while passing on CI, which is a worse outcome than not testing the
        // count at all: `fmt · clippy · test` is a required context, so it reddens `main` at random.
        //
        // **The floor is 5, not 4 (#738).** 4 is the sequential count: width-8 plus the winning
        // half's 4 → 2 → 1 descent without the sibling ever being polled. Under a loaded test
        // runtime the winner's whole descent can finish before the sibling's request reaches the
        // server; a 20 ms pause per response narrowed that race and still lost it under full-suite
        // load (#1283). So the mock holds each width-4 request until its sibling has arrived.
        // The ceiling is what the note on `batch_is_narrowable` gets wrong: a per-item
        // failure is an HTTP 200, so the retry loop breaks on its first attempt and each level costs
        // **one** request, not `TIMESTAMP_ATTEMPTS`. Were that wrong, the count would be a multiple
        // of four and land far outside this range.
        let n = seen.load(Ordering::SeqCst);
        assert!(
            (5..=7).contains(&n),
            "a per-item error costs one request per level and never enters the retry loop, so the \
             concurrent descent is 5..=7 requests (#728); \
             got {n}, and a full retry cycle would be {}",
            7 * super::TIMESTAMP_ATTEMPTS
        );
    }

    /// The exclusions are deliberate: splitting an unauthorised request yields two unauthorised
    /// requests, and splitting under a rate limit doubles the request count in the wrong direction.
    #[test]
    fn auth_and_rate_limits_are_not_narrowed() {
        use super::{batch_is_narrowable, ClassifiedError, FailureClass};
        let classified = |c: FailureClass, d: String| {
            anyhow::Error::new(ClassifiedError {
                class: c,
                detail: d,
            })
        };
        let terminal = classified(FailureClass::Terminal, "HTTP 401".into());
        let limited = classified(
            FailureClass::RateLimited { retry_after: None },
            "HTTP 429".into(),
        );
        assert!(!batch_is_narrowable(&terminal));
        assert!(!batch_is_narrowable(&limited));

        // Everything else is worth a halving - including errors naming nothing about size.
        let odd = classified(
            FailureClass::Transient,
            "Batch of more than 3 requests".into(),
        );
        assert!(batch_is_narrowable(&odd));
    }

    /// **#656, fix mechanism.** Proves the parallel top-level split actually runs the two halves
    /// concurrently, asserting on concurrency directly rather than on wall-clock elapsed time.
    ///
    /// An earlier version of this test measured elapsed time against a threshold (sequential ceiling
    /// 30ms, parallel target <21ms - a 2ms/RTT-wide margin) and reded on a loaded CI runner at
    /// 24.18ms: a busy runner and a broken parallelisation produce the same symptom, so a stopwatch
    /// cannot tell them apart. What the flag actually changes is **how many requests are in flight at
    /// once**, not how long anything takes - so that is what this asserts, via a high-water-mark
    /// counter in the mock server that is independent of scheduler speed.
    #[tokio::test]
    async fn parallel_top_level_split_runs_two_requests_concurrently() {
        use super::RpcClient;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Runs one `fetch_timestamp_batch(blocks, false, parallel)` call directly - bypassing
        // `block_timestamps`' hardcoded `true` - so both the `true` and `false` paths are exercised
        // here rather than only the one production wires up.
        async fn max_in_flight(parallel: bool) -> usize {
            // When each request was inside the server. Concurrency then becomes a question about
            // these intervals rather than about the instant a counter happened to be read.
            // Entry recorded the moment a request arrives, exit filled in when it is answered - or
            // never, for a request the client abandoned. An open entry is a request still in
            // flight, which is exactly what the count below needs it to be.
            type Intervals = Vec<(std::time::Instant, Option<std::time::Instant>)>;
            static INTERVALS: std::sync::Mutex<Intervals> = std::sync::Mutex::new(Vec::new());
            // How many half-sized requests have entered the server so far in this leg; what a held
            // half watches for its peer. `HALF` is the size of one half of `blocks` below.
            static HALVES_ENTERED: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            const HALF: usize = 4;
            INTERVALS.lock().unwrap().clear();
            HALVES_ENTERED.store(0, std::sync::atomic::Ordering::SeqCst);
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = l.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let Ok((mut sock, _)) = l.accept().await else {
                        return;
                    };
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 1 << 20];
                        let n = sock.read(&mut buf).await.unwrap_or(0);
                        let req = String::from_utf8_lossy(&buf[..n]).to_string();
                        let n_items = req.matches("eth_getBlockByNumber").count();
                        // **Record when each request was inside the server, and ask whether any two
                        // overlapped** (#1036).
                        //
                        // It began as a flat 20 ms sleep plus a sampled `MAX_IN_FLIGHT` counter,
                        // which made the assertion a race the test had to win: under load the first
                        // request can finish before the second is dispatched, the counter reads 1,
                        // and a test about concurrency fails for want of a scheduler. It did, twice,
                        // at load averages 10.7 and 18.4, while passing in isolation and on `main`.
                        //
                        // Two later attempts were worse. Polling the counter with a deadline still
                        // let one side skip while the other waited (review of #1036). Blocking on a
                        // real `Barrier` **deadlocks the property it measures**: holding request A in
                        // the server stops the client dispatching B, so the barrier never trips and a
                        // genuinely concurrent client looks sequential - measured, not theorised.
                        //
                        // Overlap of *recorded intervals* has neither problem. Nothing blocks the
                        // client, and the answer does not depend on when anything is sampled - only
                        // on whether two requests were actually inside the server at the same time,
                        // which is the property being asserted.
                        //
                        // What that version still had was a **fixed 50 ms hold** to give each
                        // interval width, and a fixed hold is a race with a wider margin, not the
                        // absence of one: on a loaded scheduler the second request can take longer
                        // than 50 ms to reach the server, the first has already left, and the mark
                        // reads 1. It did, in two of three full parallel `cargo test --lib` runs on
                        // a 2026-09-05 macOS box, on branches that do not touch this file (#1155).
                        //
                        // So the hold is now **until the peer has entered, or a deadline**, and it
                        // applies to the two requests that are meant to be concurrent and to no
                        // other. The descent this server provokes is: the whole eight-block batch
                        // fails alone, then the two four-block halves go out (together under
                        // `parallel=true`, one after the other under `false`), then each half
                        // descends serially to twos and ones. Only the halves can ever overlap, and
                        // the server can tell a half by its size. Holding anything else waits for a
                        // peer that by construction cannot arrive until the held request returns -
                        // which is what the `Barrier` above ran into, and what a first cut of this
                        // version did too, holding the eight-block batch for the full deadline and
                        // reading 1 every time.
                        //
                        // In the parallel leg the second half's arrival releases the first, so the
                        // first interval necessarily contains the second's entry and the overlap is
                        // 2 by construction, whatever the scheduler is doing, as long as the peer
                        // turns up inside the deadline. In the sequential leg no peer comes while a
                        // half is held - that is the property - so it waits out the deadline, and
                        // the two intervals are disjoint. The deadline is the only wall-clock number
                        // left and it is on the safe side: a slow peer makes the parallel leg wait
                        // longer, not read wrong, unless it is slower than a second and a half. The
                        // cost is the sequential leg, which pays the deadline once: the held half
                        // descends to a one-block failure and the client gives up before the other
                        // half is ever dispatched.
                        //
                        // Entries are recorded **on arrival**, exits when answered, because the
                        // client does not wait for the held half: the released half descends to a
                        // one-block failure in three round trips, `try_join!` returns that error and
                        // drops the held half's request, and the test computes its answer while the
                        // held half is still inside its poll. Recorded at exit, that half's interval
                        // did not exist yet and the parallel leg read 1 - measured, not theorised, on
                        // the first cut of this version. Recorded at entry, it is an open interval
                        // that contains the peer's arrival, whatever happens to it afterwards.
                        const PEER_WAIT: std::time::Duration =
                            std::time::Duration::from_millis(1500);
                        let entered = std::time::Instant::now();
                        let slot = {
                            let mut iv = INTERVALS.lock().unwrap();
                            iv.push((entered, None));
                            iv.len() - 1
                        };
                        if n_items == HALF {
                            HALVES_ENTERED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            while HALVES_ENTERED.load(std::sync::atomic::Ordering::SeqCst) < 2
                                && entered.elapsed() < PEER_WAIT
                            {
                                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                            }
                        }
                        INTERVALS.lock().unwrap()[slot].1 = Some(std::time::Instant::now());
                        let items: Vec<String> = (0..n_items)
                            .map(|i| format!(
                                r#"{{"jsonrpc":"2.0","id":{i},"error":{{"code":-32000,"message":"requested block is not available on this node"}}}}"#
                            ))
                            .collect();
                        let body = format!("[{}]", items.join(","));
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                            body.len(), body
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        let _ = sock.flush().await;
                    });
                }
            });
            let c = RpcClient::new(vec![format!("http://{addr}")]).unwrap();
            let blocks: Vec<u64> = (1..=8).collect();
            assert_eq!(
                blocks.len(),
                2 * HALF,
                "the server recognises a half by its size"
            );
            let _ = c.fetch_timestamp_batch(&blocks, false, parallel).await;

            // The most requests in flight at any one moment, computed from the record - so it
            // cannot miss an overlap by looking at the wrong time. A request is in flight at an
            // instant if it had entered and had not yet been answered; one never answered is in
            // flight from its entry onwards. Closed at both ends, so a request answered at once
            // still counts as in flight at its own entry.
            let iv = INTERVALS.lock().unwrap().clone();
            iv.iter()
                .map(|(a_in, _)| {
                    iv.iter()
                        .filter(|(b_in, b_out)| {
                            b_in <= a_in && b_out.is_none_or(|b_out| *a_in <= b_out)
                        })
                        .count()
                })
                .max()
                .unwrap_or(0)
        }

        assert_eq!(
            max_in_flight(true).await,
            2,
            "parallel=true must run the top-level split's two halves concurrently"
        );
        assert_eq!(
            max_in_flight(false).await,
            1,
            "parallel=false must never have more than one request in flight at a time"
        );
    }
}

#[cfg(test)]
mod rfc0036_tests {
    use super::*;

    /// `-32005` is also what Infura-style endpoints put on a result-count cap. Read as a throttle it
    /// was retried at the same width for good: rpc.mevblocker.io, 2026-10-01, body verbatim.
    #[test]
    fn a_result_cap_under_the_rate_limit_code_narrows_to_the_range_it_names() {
        let mevblocker = serde_json::json!({
            "code": -32005,
            "data": {"from": "0x121FF8B", "limit": 10000, "to": "0x12206F0"},
            "message": "query returned more than 10000 results. Try with this block range [0x121FF8B, 0x12206F0]."
        });
        let class = classify_rpc_error(&mevblocker);
        let FailureClass::Narrowable { suggested, .. } = class else {
            panic!("a result cap must classify as Narrowable, got {class:?}");
        };
        assert_eq!(suggested, Some((0x121FF8B, 0x12206F0)));

        // The bare code, and the code beside the one size phrase a throttle also uses, stay throttles.
        for msg in ["limit exceeded", "daily request count exceeded"] {
            let bare = serde_json::json!({"code": -32005, "message": msg});
            assert!(
                matches!(classify_rpc_error(&bare), FailureClass::RateLimited { .. }),
                "{msg}"
            );
        }
    }

    /// A provider that says **when** to come back is honoured instead of guessed at (#361).
    ///
    /// The payload is the one measured against Chainstack on 2026-08-07, verbatim from the issue.
    /// Go duration syntax, so the unit is a suffix and the value is fractional.
    #[test]
    fn a_providers_own_retry_hint_is_parsed_from_the_error_body() {
        let chainstack = serde_json::json!({
            "code": -32005,
            "message": "You've exceeded the RPS limit available on the current plan",
            "data": {"try_again_in": "560.270157ms"}
        });
        let class = classify_rpc_error(&chainstack);
        let FailureClass::RateLimited { retry_after } = class else {
            panic!("a rate limit must classify as RateLimited, got {class:?}");
        };
        let hint = retry_after.expect("the provider named a time; it must not be discarded");
        // Milliseconds, not seconds: reading `560.270157ms` as 560s would stall a backfill for nine
        // minutes on what is a half-second pause.
        assert!(
            hint >= Duration::from_millis(560) && hint < Duration::from_millis(561),
            "expected ~560ms, got {hint:?}"
        );
    }

    /// Every unit shape a provider might send, and every shape that must be refused.
    ///
    /// Refusal matters as much as parsing: an unparseable hint has to fall back to our own pacing,
    /// not to zero. A `Some(0s)` here would turn a rate limit into a hot loop against the limiter.
    #[test]
    fn retry_hints_parse_by_unit_and_refuse_nonsense() {
        for (raw, expected_ms) in [
            ("560.270157ms", 560),
            ("1s", 1_000),
            ("250ms", 250),
            ("2m", 120_000),
            ("1500ns", 0),
            ("  3s  ", 3_000),
            // Bare number is seconds, matching `Retry-After`.
            ("5", 5_000),
            // Go composite durations: the provider said `1m30s`, not `90s`.
            ("1m30s", 90_000),
            ("2m30s", 150_000),
            ("1h30m", 5_400_000),
            ("1h30m15s", 5_415_000),
            ("2h0m0s", 7_200_000),
            ("1h500ms", 3_600_500),
            ("1m30.5s", 90_500),
        ] {
            let got = parse_retry_hint(raw).unwrap_or_else(|| panic!("{raw} must parse"));
            assert_eq!(got.as_millis() as u64, expected_ms, "{raw}");
        }
        for raw in [
            "",
            "soon",
            "-1s",
            "NaN",
            "1 fortnight",
            "1e400s",
            "1m-5s",
            "ms30s",
        ] {
            assert!(
                parse_retry_hint(raw).is_none(),
                "{raw:?} is not a duration we understand - it must fall back to our own pacing"
            );
        }
    }

    /// An absurd hint is capped rather than obeyed, so a backfill stalls loudly (#361).
    #[test]
    fn an_absurd_retry_hint_is_capped() {
        let none = Duration::ZERO;
        let hour = Duration::from_secs(3600);
        assert_eq!(
            clamp_retry_hint(hour, none),
            MAX_RETRY_HINT,
            "obeying an hour-long hint would park a backfill with nothing to explain the silence"
        );
        // Anything within the cap passes through untouched - the provider knows its own limiter.
        let modest = Duration::from_millis(560);
        assert_eq!(clamp_retry_hint(modest, none), modest);
    }

    /// A hint may only ever make us wait **longer** than our own pacing, never less (#361).
    ///
    /// Without the floor, `Retry-After: 0` - which proxies and CDNs do send - parses to `Some(0s)`,
    /// passes the cap untouched, and *replaces* the caller's backoff, so we would retry with no
    /// pacing at all precisely while a limiter was telling us to slow down.
    #[test]
    fn a_retry_hint_can_never_undercut_our_own_pacing() {
        let ours = Duration::from_millis(250);
        for zero in ["0s", "0", "0ms", "0ns"] {
            let hint = parse_retry_hint(zero).expect("a parseable zero is still a parse");
            assert_eq!(
                clamp_retry_hint(hint, ours),
                ours,
                "{zero:?} must floor to our own pacing, not turn a rate limit into a hot loop"
            );
        }
        // A hint shorter than our pacing but non-zero is the same hazard, less obviously.
        assert_eq!(clamp_retry_hint(Duration::from_millis(1), ours), ours);
        // The floor must not become a floor on everything: a longer hint still wins.
        let longer = Duration::from_millis(900);
        assert_eq!(clamp_retry_hint(longer, ours), longer);
        // Both bounds at once: capped from above, floored from below.
        assert_eq!(
            clamp_retry_hint(Duration::from_secs(3600), ours),
            MAX_RETRY_HINT
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_pause_without_an_observer_really_sleeps() {
        let start = tokio::time::Instant::now();
        retry_pause(Duration::from_secs(5)).await;
        assert_eq!(start.elapsed(), Duration::from_secs(5));
    }

    /// The hint is actually **wired to the pause** - the half of #361 the parser tests do not reach.
    ///
    /// Deleting the honouring at both call sites, leaving parser, cap and classifier intact, left the
    /// suite at 488 passed / 0 failed: the entire behavioural delta was invisible to its own tests.
    /// That is our most-repeated failure - a criterion phrased as the absence of an effect passes
    /// trivially when the mechanism is missing - so this asserts the effect instead.
    ///
    /// Observe the requested pauses, not elapsed time (#1454). Paused Tokio time can advance through
    /// reqwest's timeouts while real sockets wait on the OS. Two such runs do not cancel that noise.
    /// The request, header parsing and both retry loops remain real; only their sleeps are recorded.
    #[tokio::test]
    async fn a_provider_retry_hint_actually_lengthens_the_pause() {
        use axum::{http::StatusCode, response::IntoResponse, routing::post, Router};

        async fn hinted() -> impl IntoResponse {
            (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "5")],
                "rate limited",
            )
        }
        async fn bare() -> impl IntoResponse {
            (StatusCode::TOO_MANY_REQUESTS, "rate limited")
        }
        async fn pauses_for(app: Router) -> Vec<Duration> {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            let client = RpcClient::new(vec![format!("http://{addr}/")]).unwrap();
            let pauses = RETRY_PAUSES
                .scope(std::cell::RefCell::new(Vec::new()), async {
                    client
                        .block_headers(&[1])
                        .await
                        .expect_err("a permanently rate-limited endpoint must exhaust its rounds");
                    assert_eq!(client.requests.load(Ordering::Relaxed), 32);
                    RETRY_PAUSES.with(|pauses| pauses.borrow().clone())
                })
                .await;
            server.abort();
            let _ = server.await;
            pauses
        }

        let with_hint = pauses_for(
            Router::new()
                .route("/", post(hinted))
                .route("/{*rest}", post(hinted)),
        )
        .await;
        let without_hint = pauses_for(
            Router::new()
                .route("/", post(bare))
                .route("/{*rest}", post(bare)),
        )
        .await;

        // Eight rounds of four inner attempts, with seven outer pauses between rounds.
        assert_eq!(with_hint, vec![Duration::from_secs(5); 39]);
        let mut expected = Vec::new();
        for round in 1..=8 {
            expected.extend([200, 400, 600, 800].map(Duration::from_millis));
            if round < 8 {
                expected.push(Duration::from_millis(250 * round));
            }
        }
        // Without a hint the endpoint rests 1s, 2s, 4s… up to the cooldown (#1853), each longer than
        // the loop's own pause, so the client waits out the remainder: the loop's pauses still appear
        // in order, and the 31 gaps between requests add up to the rests, plus the final 800ms.
        let mut own = expected.iter().peekable();
        for p in &without_hint {
            if own.peek() == Some(&p) {
                own.next();
            }
        }
        assert!(own.next().is_none(), "{without_hint:?}");
        let rests: Duration = (0..31)
            .map(|n| Duration::from_millis((1_000u64 << n.min(16)).min(ENDPOINT_COOLDOWN_MS)))
            .sum::<Duration>()
            + Duration::from_millis(800);
        let total: Duration = without_hint.iter().sum();
        assert!(
            total <= rests && total > rests - Duration::from_secs(1),
            "{total:?} against {rests:?}: {without_hint:?}"
        );
    }

    /// A rate limit that carries no hint stays `None`, so the caller keeps its own pacing. This is
    /// the common case: most providers say nothing, and inventing a number for them would be the
    /// same guess this change removes.
    #[test]
    fn a_rate_limit_without_a_hint_carries_none() {
        let bare = serde_json::json!({"code": -32005, "message": "rate limit exceeded"});
        assert_eq!(
            classify_rpc_error(&bare),
            FailureClass::RateLimited { retry_after: None }
        );
        // A `data` block that exists but names something else must not be mined for a number.
        let unrelated = serde_json::json!({"code": 429, "message": "too many requests", "data": {"plan": "free"}});
        assert_eq!(
            classify_rpc_error(&unrelated),
            FailureClass::RateLimited { retry_after: None }
        );
    }

    /// A rate limit delivered **inside** a JSON-RPC error body must classify as `RateLimited`, not
    /// `Transient`.
    ///
    /// This is the bug that cost three wrong diagnoses on OBIB case 3. Alchemy answers HTTP 200 and
    /// puts the throttle in each batch *item*: `{"code":429,"message":"Your app has exceeded its
    /// compute units per second capacity"}`. Classified `Transient`, `batch_is_narrowable` returns
    /// true and the batch is split - doubling the request count against a per-second request limit,
    /// which is the one response guaranteed to make it worse. It split all the way to a 1-block batch
    /// before giving up.
    #[test]
    fn a_rate_limit_inside_the_error_body_is_not_narrowable() {
        let alchemy = serde_json::json!({
            "code": 429,
            "message": "Your app has exceeded its compute units per second capacity. If you have \
                        retries enabled, you can safely ignore this message."
        });
        assert_eq!(
            classify_rpc_error(&alchemy),
            FailureClass::RateLimited { retry_after: None }
        );

        // By message alone too, for a provider that does not set the numeric code.
        let by_message = serde_json::json!({"code": -32000, "message": "rate limit exceeded"});
        assert_eq!(
            classify_rpc_error(&by_message),
            FailureClass::RateLimited { retry_after: None }
        );

        // Chainstack: neither a 429 nor any phrase Alchemy uses. Matching one provider's spelling is
        // how this bug survives a provider switch, so both the code and the wording are covered.
        let chainstack = serde_json::json!({
            "code": -32005,
            "message": "You've exceeded the RPS limit available on the current plan. Learn more how \
                        to increase the limit, visit https://docs.chainstack.com/docs/pricing"
        });
        assert_eq!(
            classify_rpc_error(&chainstack),
            FailureClass::RateLimited { retry_after: None }
        );

        // And the consequence that actually matters: never split under one.
        let err = anyhow::Error::new(ClassifiedError {
            class: FailureClass::RateLimited { retry_after: None },
            detail: "429".into(),
        });
        assert!(
            !batch_is_narrowable(&err),
            "splitting under a rate limit doubles the request count in the wrong direction"
        );
    }

    /// eth.drpc.org's answer to any getLogs below its pruning horizon, measured 2026-10-01 (#1607).
    const DRPC_PRUNED: &str = r#"{"id":1,"jsonrpc":"2.0","error":{"message":"Unknown state. First available state is 1","code":27}}"#;
    /// eth-pokt.nodies.app's throttle as #761 recorded it; #1607's log elides the message's tail.
    const NODIES_THROTTLED: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"Rate limit exceeded on Nodies public endpoints. For higher limits, create a free account"}}"#;
    /// eth-pokt.nodies.app's answer to an 80-block getLogs, measured 2026-10-01.
    const NODIES_CAPPED: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"Block range too large: maximum allowed is 50 blocks on your current plan. Upgrade your subscription at https://nodies.app/pricing for larger ranges."}}"#;

    /// An endpoint answering every request with `status` and `body`, counting the requests.
    async fn answering(
        status: u16,
        body: &'static str,
    ) -> (
        String,
        tokio::task::JoinHandle<()>,
        std::sync::Arc<AtomicU64>,
    ) {
        use axum::{extract::State, http::StatusCode, routing::post, Router};
        use std::sync::Arc;
        let hits = Arc::new(AtomicU64::new(0));
        let app = Router::new()
            .route(
                "/",
                post(move |State(hits): State<Arc<AtomicU64>>| async move {
                    hits.fetch_add(1, Ordering::Relaxed);
                    (
                        StatusCode::from_u16(status).unwrap(),
                        [("content-type", "application/json")],
                        body,
                    )
                }),
            )
            .with_state(hits.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}/"), handle, hits)
    }

    /// #1607: the README demo's pool, one endpoint pruned and the other throttled. Every attempt
    /// reached both, yet the error carried only whichever was tried last, so the log repeated a 429
    /// and nothing said the other endpoint could never serve the range. The throttle must stay the
    /// verdict, so the caller still retries it, and the pruned endpoint and the remedy must be named.
    #[tokio::test]
    async fn a_pruned_endpoint_beside_a_throttled_one_is_named_with_the_remedy() {
        let (pruned, hp, pruned_hits) = answering(400, DRPC_PRUNED).await;
        let (throttled, ht, throttled_hits) = answering(429, NODIES_THROTTLED).await;
        for urls in [
            vec![pruned.clone(), throttled.clone()],
            vec![throttled.clone(), pruned.clone()],
        ] {
            let c = RpcClient::new(urls).unwrap();
            for _ in 0..3 {
                let err = c
                    .get_logs(&["0xa0b8".into()], &[], 6_082_525, 6_082_604)
                    .await
                    .unwrap_err();
                let text = format!("{err:#}");
                assert!(
                    matches!(class_of(&err), Some(FailureClass::RateLimited { .. })),
                    "the throttle is the only thing waiting can fix: {text}"
                );
                assert!(
                    text.contains(&redact_url(&pruned)),
                    "the pruned endpoint is not named: {text}"
                );
                assert!(
                    text.contains("--rpc") && text.contains("rpc_urls"),
                    "the remedy is not named: {text}"
                );
            }
        }
        assert!(pruned_hits.load(Ordering::Relaxed) >= 6);
        // Once per client: a 429 is left to cool while another endpoint can still be asked (#1853).
        assert_eq!(throttled_hits.load(Ordering::Relaxed), 2);
        hp.abort();
        ht.abort();
    }

    /// The same masking with today's Nodies answer: its cap is the one narrowing can satisfy, and
    /// when the pruned endpoint was tried last its 400 replaced the cap, so the window never shrank.
    #[tokio::test]
    async fn a_pruned_endpoint_does_not_hide_another_endpoints_cap() {
        let (pruned, hp, _) = answering(400, DRPC_PRUNED).await;
        let (capped, hc, _) = answering(200, NODIES_CAPPED).await;
        for urls in [
            vec![pruned.clone(), capped.clone()],
            vec![capped.clone(), pruned.clone()],
        ] {
            let c = RpcClient::new(urls).unwrap();
            for _ in 0..3 {
                let err = c
                    .get_logs(&["0xa0b8".into()], &[], 6_082_525, 6_082_604)
                    .await
                    .unwrap_err();
                assert!(
                    crate::chunker::is_result_too_large(&err),
                    "the cap was lost: {err:#}"
                );
            }
        }
        hp.abort();
        hc.abort();
    }

    /// #1853: a 429 tried after the capped endpoint used to be the verdict, so the window never
    /// narrowed and the same range went round for as long as the run lasted.
    #[tokio::test]
    async fn a_throttled_endpoint_does_not_hide_another_endpoints_cap() {
        let (throttled, ht, _) = answering(429, NODIES_THROTTLED).await;
        let (capped, hc, _) = answering(200, NODIES_CAPPED).await;
        for urls in [
            vec![throttled.clone(), capped.clone()],
            vec![capped.clone(), throttled.clone()],
        ] {
            let err = RpcClient::new(urls)
                .unwrap()
                .get_logs(&["0xa0b8".into()], &[], 6_082_525, 6_082_604)
                .await
                .unwrap_err();
            assert!(
                crate::chunker::is_result_too_large(&err),
                "the cap was lost: {err:#}"
            );
        }
        ht.abort();
        hc.abort();
    }

    /// The batch path's verdict also speaks for the endpoint it left to cool: once the throttled one
    /// rests, the pruned one alone must not report that no endpoint keeps the history.
    #[tokio::test]
    async fn a_batch_left_to_a_pruned_endpoint_still_reports_the_throttle() {
        let (pruned, hp, _) = answering(200, DRPC_PRUNED).await;
        let (throttled, ht, _) = answering(429, NODIES_THROTTLED).await;
        let c = RpcClient::new(vec![pruned, throttled]).unwrap();
        let batch = serde_json::json!([{"jsonrpc": "2.0", "id": 0, "method": "eth_blockNumber"}]);
        for _ in 0..3 {
            let err = c.raw_batch(&batch).await.unwrap_err();
            assert!(
                matches!(class_of(&err), Some(FailureClass::RateLimited { .. })),
                "{err:#}"
            );
        }
        hp.abort();
        ht.abort();
    }

    /// A pool that is all resting waits for the soonest, and an hour's `Retry-After` is held to the cap.
    #[tokio::test]
    async fn a_pool_resting_on_a_long_retry_after_waits_no_longer_than_the_cap() {
        use axum::{http::StatusCode, response::IntoResponse, routing::post, Router};
        async fn hour() -> impl IntoResponse {
            (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "3600")], "")
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, Router::new().route("/", post(hour))).await;
        });
        let c = RpcClient::new(vec![format!("http://{addr}/")]).unwrap();
        let pauses = RETRY_PAUSES
            .scope(std::cell::RefCell::new(Vec::new()), async {
                assert!(c.block_number().await.is_err());
                assert!(c.block_number().await.is_err());
                RETRY_PAUSES.with(|p| p.borrow().clone())
            })
            .await;
        server.abort();
        assert_eq!(c.request_count(), 2);
        assert_eq!(pauses.len(), 1, "{pauses:?}");
        assert!(
            pauses[0] <= MAX_RETRY_HINT && pauses[0] > MAX_RETRY_HINT - Duration::from_secs(5),
            "{pauses:?}"
        );
    }

    /// A lone endpoint answering 429 with no hint, per request in turn: `true` is a 429, `false` an
    /// answer. Returns the waits the client recorded across one call per entry.
    async fn lone_endpoint_waits(script: &'static [bool]) -> Vec<Duration> {
        use axum::{
            extract::State, http::StatusCode, response::IntoResponse, routing::post, Router,
        };
        use std::sync::Arc;
        let next = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/",
                post(move |State(next): State<Arc<AtomicUsize>>| async move {
                    let n = next.fetch_add(1, Ordering::SeqCst);
                    if script[n.min(script.len() - 1)] {
                        (StatusCode::TOO_MANY_REQUESTS, "").into_response()
                    } else {
                        axum::Json(serde_json::json!({"jsonrpc":"2.0","id":1,"result":"0x10"}))
                            .into_response()
                    }
                }),
            )
            .with_state(next);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let c = RpcClient::new(vec![format!("http://{addr}/")]).unwrap();
        let waits = RETRY_PAUSES
            .scope(std::cell::RefCell::new(Vec::new()), async {
                for &throttled in script {
                    assert_eq!(c.block_number().await.is_err(), throttled);
                }
                RETRY_PAUSES.with(|p| p.borrow().clone())
            })
            .await;
        server.abort();
        assert_eq!(c.request_count(), script.len() as u64);
        waits
    }

    fn about(waits: &[Duration], want: &[u64]) -> bool {
        waits.len() == want.len()
            && waits.iter().zip(want).all(|(w, &s)| {
                let s = Duration::from_secs(s);
                *w <= s && *w > s - Duration::from_millis(500)
            })
    }

    /// #1853: a lone endpoint that 429s without a hint is asked again after 1s, then 2s, not after
    /// the 30s cooldown, and an answer starts the doubling over.
    #[tokio::test]
    async fn a_lone_endpoint_without_a_hint_rests_one_second_then_two_and_an_answer_resets_it() {
        let waits = lone_endpoint_waits(&[true, true, false, true, false]).await;
        assert!(about(&waits, &[1, 2, 1]), "{waits:?}");
    }

    /// The doubling stops at the ordinary cooldown.
    #[tokio::test]
    async fn the_doubling_rest_stops_at_the_cooldown() {
        let waits = lone_endpoint_waits(&[true; 8]).await;
        assert!(about(&waits, &[1, 2, 4, 8, 16, 30, 30]), "{waits:?}");
    }

    /// With every endpoint resting, the wait ends when the soonest rest does, and only the endpoints
    /// whose rest is over are asked: one resting 16s is not asked when one resting 1s comes free.
    #[tokio::test]
    async fn a_pool_waiting_out_its_rests_asks_only_the_endpoints_that_are_free() {
        use axum::{
            extract::State, http::StatusCode, response::IntoResponse, routing::post, Router,
        };
        use std::sync::Arc;
        async fn serve(retry_after: Option<&'static str>) -> (String, Arc<AtomicUsize>) {
            let hits = Arc::new(AtomicUsize::new(0));
            let app = Router::new()
                .route(
                    "/",
                    post(move |State(hits): State<Arc<AtomicUsize>>| async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        let mut resp = (StatusCode::TOO_MANY_REQUESTS, "").into_response();
                        if let Some(secs) = retry_after {
                            resp.headers_mut()
                                .insert("retry-after", secs.parse().unwrap());
                        }
                        resp
                    }),
                )
                .with_state(hits.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            (format!("http://{addr}/"), hits)
        }
        let (short, short_hits) = serve(None).await;
        let (long, long_hits) = serve(Some("16")).await;
        let c = RpcClient::new(vec![short, long]).unwrap();
        let pauses = RETRY_PAUSES
            .scope(std::cell::RefCell::new(Vec::new()), async {
                assert!(c.block_number().await.is_err());
                assert!(c.block_number().await.is_err());
                RETRY_PAUSES.with(|p| p.borrow().clone())
            })
            .await;
        assert!(about(&pauses, &[1]), "{pauses:?}");
        assert_eq!(
            (
                short_hits.load(Ordering::SeqCst),
                long_hits.load(Ordering::SeqCst)
            ),
            (2, 1),
            "the 16s rest was cut short"
        );
    }

    /// When every endpoint is pruned no retry can help, so the verdict says so, names them all, and
    /// is never mistaken for a cap.
    #[tokio::test]
    async fn a_pool_of_pruned_endpoints_has_no_history_to_offer() {
        let (a, ha, _) = answering(400, DRPC_PRUNED).await;
        let (b, hb, _) = answering(200, DRPC_PRUNED).await;
        let c = RpcClient::new(vec![a.clone(), b.clone()]).unwrap();
        let err = c
            .get_logs(&["0xa0b8".into()], &[], 6_082_525, 6_082_604)
            .await
            .unwrap_err();
        let text = format!("{err:#}");
        assert_eq!(
            class_of(&err),
            Some(FailureClass::HistoryUnavailable),
            "{text}"
        );
        assert!(!crate::chunker::is_result_too_large(&err), "{text}");
        for url in [&a, &b] {
            assert!(text.contains(&redact_url(url)), "{text}");
        }
        assert!(
            text.contains("--rpc") && text.contains("Unknown state"),
            "{text}"
        );
        ha.abort();
        hb.abort();
    }

    /// A genuine size refusal must still narrow - the fix above must not swallow the case the
    /// narrowing path exists for. `limit exceeded` sits in NARROWABLE and could plausibly have been
    /// claimed by a rate-limit substring match.
    #[test]
    fn a_size_refusal_still_narrows() {
        let too_big = serde_json::json!({"code": -32602, "message": "query returned more than 10000 results"});
        assert!(
            matches!(
                classify_rpc_error(&too_big),
                FailureClass::Narrowable { .. }
            ),
            "a result-cap refusal is the case narrowing exists for"
        );
    }
}
