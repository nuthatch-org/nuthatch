//! The JSON-RPC batch width one endpoint takes, learned from its refusals.
//!
//! Providers cap a batch at very different sizes (GraphOps 100 and answers HTTP 413
//! `rpc_batch_too_large` with *no* call run, Alchemy ~1000, arbitrum.drpc.org 3), and no constant is
//! right for all of them. One [`BatchLimit`] per endpoint starts unlimited, narrows the moment the
//! endpoint refuses a batch for its size, and creeps back up after a long run of successes so a raised
//! limit is noticed. The policy is deterministic: counts only, no clocks, no randomness.

use crate::rpc::{body_capped, class_of, ClassifiedError, FailureClass};
use std::sync::Mutex;

/// Full-width batches that must succeed before the width is probed upward once.
const PROBE_AFTER: usize = 50;
/// Each failed probe doubles the wait, up to `PROBE_AFTER << MAX_BACKOFF_SHIFT`, so a provider whose
/// limit has not moved costs one rejected request per ~3,200 batches rather than per 50.
const MAX_BACKOFF_SHIFT: u32 = 6;

/// Why an endpoint refused a batch, as far as the refusal says.
#[derive(Debug, PartialEq, Eq)]
pub struct TooLarge {
    /// The maximum the refusal itself states, when it states one.
    pub stated_max: Option<usize>,
}

/// Did `err` refuse a **batch for its size** (or count)? Deliberately not for rate limits, auth or a
/// response that passed our own body cap - those have their own handling, and none of them is a fact
/// about the endpoint's batch limit.
pub fn batch_too_large(err: &anyhow::Error) -> Option<TooLarge> {
    if body_capped(err) {
        return None;
    }
    let detail = err
        .chain()
        .find_map(|e| e.downcast_ref::<ClassifiedError>())
        .map(|c| c.detail.to_ascii_lowercase())
        .unwrap_or_else(|| format!("{err:#}").to_ascii_lowercase());
    // GraphOps names its refusal; it wins over the class the body would otherwise get.
    let named = detail.contains("rpc_batch_too_large");
    if !named {
        if matches!(
            class_of(err),
            Some(FailureClass::RateLimited { .. })
                | Some(FailureClass::Terminal)
                | Some(FailureClass::HistoryUnavailable)
        ) {
            return None;
        }
        let status_413 = detail.contains("http 413");
        let worded = detail.contains("batch")
            && ["too large", "too big", "too many", "exceed", "limit", "more than", "not allowed"]
                .iter()
                .any(|m| detail.contains(m));
        if !status_413 && !worded {
            return None;
        }
    }
    Some(TooLarge {
        stated_max: stated_max(&detail),
    })
}

/// A maximum the refusal text names: the first number after "max", "limit", "more than", "up to" or
/// "at most". "exceeds" is left out on purpose - what follows it is usually the size we *sent*.
fn stated_max(detail: &str) -> Option<usize> {
    ["maximum", "max", "limit", "more than", "up to", "at most"]
        .iter()
        .filter_map(|key| {
            let at = detail.find(key)? + key.len();
            let tail = &detail[at..];
            // Within a few words, so an unrelated later number is not read as the limit.
            let window: String = tail.chars().take(24).collect();
            let digits: String = window
                .chars()
                .skip_while(|c| !c.is_ascii_digit())
                .take_while(char::is_ascii_digit)
                .collect();
            digits.parse::<usize>().ok().filter(|n| *n > 0)
        })
        .next()
}

#[derive(Debug)]
struct State {
    width: usize,
    /// Full-width batches answered since the width last changed.
    streak: usize,
    /// Consecutive failed probes, for the backoff.
    failed_probes: u32,
    /// The width to return to if the probe now in flight is refused.
    probe_from: Option<usize>,
    /// The size of the first batch refused: probing never goes past it, so the width stays at or
    /// below what the caller was sending (for headers, the default and any static cap).
    ceiling: usize,
    announced: bool,
}

/// One endpoint's learned batch width.
#[derive(Debug)]
pub struct BatchLimit(Mutex<State>);

/// What learning a refusal changed, for the caller's one-time log line.
pub struct Narrowed {
    pub width: usize,
    pub first_time: bool,
}

impl Default for BatchLimit {
    fn default() -> Self {
        Self(Mutex::new(State {
            width: usize::MAX,
            streak: 0,
            failed_probes: 0,
            probe_from: None,
            ceiling: usize::MAX,
            announced: false,
        }))
    }
}

impl BatchLimit {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The most calls to put in one batch now (`usize::MAX` until a refusal teaches otherwise).
    pub fn width(&self) -> usize {
        self.state().width
    }

    /// A batch of `sent` calls was refused. Narrow to the refusal's own maximum if it gave one and it
    /// is below what was sent, else to half. A refused *probe* goes back to the width it came from.
    pub fn refused(&self, sent: usize, stated_max: Option<usize>) -> Narrowed {
        let mut s = self.state();
        let learned = stated_max
            .filter(|m| *m < sent)
            .unwrap_or(sent / 2)
            .clamp(1, sent.saturating_sub(1).max(1));
        s.width = match s.probe_from.take() {
            Some(back) => {
                s.failed_probes = (s.failed_probes + 1).min(MAX_BACKOFF_SHIFT);
                back
            }
            None => s.width.min(learned),
        };
        if s.ceiling == usize::MAX {
            s.ceiling = sent;
        }
        s.streak = 0;
        let first_time = !std::mem::replace(&mut s.announced, true);
        Narrowed {
            width: s.width,
            first_time,
        }
    }

    /// A batch of `sent` calls was answered in full.
    pub fn answered(&self, sent: usize) {
        let mut s = self.state();
        if s.width == usize::MAX || sent < s.width {
            // Never narrowed, or a short tail chunk: says nothing about the limit.
            return;
        }
        if s.probe_from.take().is_some() {
            // The probe held: the endpoint takes this width.
            s.failed_probes = 0;
        }
        s.streak += 1;
        if s.streak >= PROBE_AFTER << s.failed_probes && s.width < s.ceiling {
            let step = (s.width / 10).max(1);
            s.probe_from = Some(s.width);
            s.width = (s.width + step).min(s.ceiling);
            s.streak = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::FailureClass;

    fn classified(class: FailureClass, detail: &str) -> anyhow::Error {
        anyhow::Error::new(ClassifiedError {
            class,
            detail: detail.into(),
        })
    }

    fn narrowable() -> FailureClass {
        FailureClass::Narrowable {
            suggested: None,
            escalated_from_rate_limit: false,
        }
    }

    #[test]
    fn the_shapes_providers_use_are_recognised() {
        let graphops = classified(
            narrowable(),
            "HTTP 413 Payload Too Large: {\"code\":\"rpc_batch_too_large\",\"message\":\"max 100 calls per batch\"}",
        );
        assert_eq!(
            batch_too_large(&graphops),
            Some(TooLarge {
                stated_max: Some(100)
            })
        );
        let bare_413 = classified(narrowable(), "HTTP 413 Payload Too Large");
        assert_eq!(
            batch_too_large(&bare_413),
            Some(TooLarge { stated_max: None })
        );
        let drpc = classified(
            FailureClass::Transient,
            "rpc error (endpoint rejected the batch): {\"message\":\"Batch of more than 3 requests are not allowed on free plan\"}",
        );
        assert_eq!(
            batch_too_large(&drpc),
            Some(TooLarge {
                stated_max: Some(3)
            })
        );
        let exceeds = classified(
            FailureClass::Transient,
            "rpc error: batch size exceeds the limit",
        );
        assert!(batch_too_large(&exceeds).is_some());
    }

    #[test]
    fn rate_limits_auth_and_unrelated_errors_are_not_a_width() {
        let throttled = classified(
            FailureClass::RateLimited { retry_after: None },
            "HTTP 429 Too Many Requests: batch limit of requests per second",
        );
        assert!(batch_too_large(&throttled).is_none());
        let auth = classified(FailureClass::Terminal, "HTTP 401: batch not allowed");
        assert!(batch_too_large(&auth).is_none());
        let other = classified(FailureClass::Transient, "HTTP 500 Internal Server Error");
        assert!(batch_too_large(&other).is_none());
    }

    #[test]
    fn narrows_to_the_stated_max_else_half_and_never_widens_by_refusal() {
        let l = BatchLimit::default();
        assert_eq!(l.width(), usize::MAX);
        assert_eq!(l.refused(200, Some(100)).width, 100);
        // A later refusal at a width already below the stated max does not raise it.
        assert_eq!(l.refused(100, Some(150)).width, 50);
        assert_eq!(l.refused(1, None).width, 1);
        assert_eq!(BatchLimit::default().refused(7, None).width, 3);
    }

    #[test]
    fn probes_up_after_a_long_streak_and_backs_off_when_refused() {
        let l = BatchLimit::default();
        l.refused(200, Some(100));
        for _ in 0..PROBE_AFTER {
            l.answered(100);
        }
        assert_eq!(l.width(), 110, "one step up");
        // The probe is refused: back to 100, and the next probe waits twice as long.
        assert_eq!(l.refused(110, None).width, 100);
        for _ in 0..PROBE_AFTER {
            l.answered(100);
        }
        assert_eq!(l.width(), 100);
        for _ in 0..PROBE_AFTER {
            l.answered(100);
        }
        assert_eq!(l.width(), 110);
        // This probe holds, so the backoff resets.
        l.answered(110);
        assert_eq!(l.width(), 110);
    }

    #[test]
    fn an_endpoint_that_was_never_narrowed_is_never_probed() {
        let l = BatchLimit::default();
        for _ in 0..1_000 {
            l.answered(1_000);
        }
        assert_eq!(l.width(), usize::MAX);
    }
}
