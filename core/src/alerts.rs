//! Which alert sinks an annotation goes to, and the outbox entries the window queues for them
//! (RFC-0008 C5). Delivery is the node's: a tab has no sinks and builds an empty router.

use crate::store::HotStore;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Bound on the durable outbox. A dead webhook can't grow it without limit; past this the oldest
/// undelivered alerts are shed (loudly). Generous - a transient outage of minutes is absorbed.
pub const OUTBOX_MAX: u64 = 10_000;

/// The body a sink expects.
///
/// **Discord will not take our payload.** Posting the outbox's own JSON to a Discord webhook is
/// answered `400 {"message": "Cannot send an empty message", "code": 50006}` - measured against a
/// real webhook, not inferred - while `{"content": "…"}` is answered `204`. Since the delivery
/// worker retries a non-success forever, wiring Discord up without this would give an outbox that
/// never drains and alerts that never arrive: an alerting system that fails silently, which is worse
/// than none.
///
/// An unrecognised value is a **load error**, not a fallback to `Raw`. Falling back would put the
/// nest straight into that silent-failure state for a typo.
#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AlertFormat {
    /// The outbox payload verbatim: `{event, kind, url, annotation}`. What every existing sink gets,
    /// and what an HMAC signature is computed over.
    #[default]
    Raw,
    /// `{"content": "…"}` - Discord, and anything else that speaks the same shape.
    Discord,
}

/// One sink: annotations whose kind is in `kinds` are queued for `url`.
#[derive(Debug, Clone)]
pub struct AlertSink {
    pub kinds: Vec<String>,
    pub url: String,
    pub format: AlertFormat,
}

/// Routes annotation kinds to the webhook sinks that want them. Built once from `[[alerts]]`.
#[derive(Clone, Default)]
pub struct AlertRouter {
    sinks: Vec<AlertSink>,
}

impl AlertRouter {
    pub fn new<S: Into<AlertSink>>(sinks: impl IntoIterator<Item = S>) -> Self {
        Self {
            sinks: sinks.into_iter().map(Into::into).collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }

    /// The URLs that want annotations of `kind`.
    pub fn urls_for(&self, kind: &str) -> Vec<(&str, AlertFormat)> {
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

/// The outbox entries for every sink that watches `kind`, for a caller that writes them in its own
/// transaction. `event` is `"flag"` or `"flag_retracted"`.
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
