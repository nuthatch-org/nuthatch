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
            && [
                "too large",
                "too big",
                "too many",
                "exceed",
                "limit",
                "more than",
                "not allowed",
            ]
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
    /// The most calls per batch to send now.
    width: usize,
    /// The largest width known to succeed (0 = none yet).
    ok: usize,
    /// The smallest width known to be refused (`usize::MAX` = none known).
    fail: usize,
    /// Full-width batches answered since the width settled at `ok`.
    streak: usize,
    /// Consecutive refusals of an already-known-bad width, for the backoff.
    failed_probes: u32,
    announced: bool,
}

/// One endpoint's learned batch width.
///
/// The width is a binary search for the endpoint's limit between `ok` (known good) and `fail`
/// (known bad). A refusal with no stated maximum probes the midpoint, each answer halving the
/// interval, so the limit is found in about log2(width) refusals. Once `ok + 1 == fail` the width
/// holds at `ok`; only after a long streak (doubling with each refusal of the known-bad width) is
/// `fail` re-tried, so a raised limit is noticed without re-hitting a bad width often. A success at
/// or above `fail` proves it stale (a transient refusal) and clears it; the width then doubles
/// until the endpoint really refuses.
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
            ok: 0,
            fail: usize::MAX,
            streak: 0,
            failed_probes: 0,
            announced: false,
        }))
    }
}

impl State {
    /// The next width: the midpoint while the interval is open, else the known-good width.
    fn settle(&mut self) {
        self.width = if self.fail == usize::MAX {
            self.ok.saturating_mul(2).max(1)
        } else if self.ok + 1 < self.fail {
            self.ok + (self.fail - self.ok) / 2
        } else {
            self.ok.max(1)
        };
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

    /// A batch of `sent` calls was refused. A stated maximum below `sent` is taken as the limit;
    /// otherwise `sent` becomes the known-bad width and the search moves to the midpoint.
    pub fn refused(&self, sent: usize, stated_max: Option<usize>) -> Narrowed {
        let mut s = self.state();
        if sent >= s.fail {
            // A width already known bad was tried again (a backoff probe): wait longer next time.
            s.failed_probes = (s.failed_probes + 1).min(MAX_BACKOFF_SHIFT);
        }
        match stated_max.filter(|m| *m < sent) {
            Some(m) => {
                s.ok = m.max(1);
                s.fail = s.ok + 1;
            }
            None => {
                if sent <= s.ok {
                    // Our idea of a good width was stale.
                    s.ok = 0;
                }
                s.fail = s.fail.min(sent);
            }
        }
        s.settle();
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
        if s.width == usize::MAX {
            // Never narrowed: says nothing about the limit.
            return;
        }
        if sent >= s.fail {
            // The "known bad" width worked: the refusal was transient, or the limit was raised.
            s.fail = usize::MAX;
            s.failed_probes = 0;
        }
        s.ok = s.ok.max(sent);
        if sent < s.width {
            // A short tail chunk: it is evidence for `ok` only.
            return;
        }
        if s.fail == usize::MAX || s.ok + 1 < s.fail {
            // Searching (or growing back): every success moves the width on at once.
            s.streak = 0;
            s.settle();
            return;
        }
        s.streak += 1;
        if s.streak >= PROBE_AFTER << s.failed_probes {
            // Settled at `ok`: re-try the known-bad width once.
            s.width = s.fail;
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

    /// Drive `l` against an endpoint whose real limit is `limit` and whose refusals state no
    /// maximum, sending full-width batches only. Returns how many were refused.
    fn run_against(l: &BatchLimit, limit: usize, batches: usize) -> usize {
        let mut refusals = 0;
        for _ in 0..batches {
            let w = l.width();
            if w > limit {
                refusals += 1;
                l.refused(w, None);
            } else {
                l.answered(w);
            }
        }
        refusals
    }

    #[test]
    fn unstated_limit_of_100_from_200_converges_to_exactly_100() {
        let l = BatchLimit::default();
        l.refused(200, None);
        assert_eq!(l.width(), 100);
        let refusals = 1 + run_against(&l, 100, 40);
        assert_eq!(l.width(), 100);
        // log2(200) ~ 7.6: the search needs at most 8 refusals, not a halving ladder.
        assert!(refusals <= 8, "{refusals} refusals");
    }

    #[test]
    fn unstated_limit_of_137_converges_to_137() {
        let l = BatchLimit::default();
        l.refused(200, None);
        let refusals = 1 + run_against(&l, 137, 40);
        assert_eq!(l.width(), 137);
        assert!(refusals <= 8, "{refusals} refusals");
    }

    #[test]
    fn a_settled_width_does_not_re_hit_the_known_bad_width_until_a_long_backoff() {
        let l = BatchLimit::default();
        l.refused(200, None);
        run_against(&l, 100, 40); // settled at 100, 101 known bad
        assert_eq!(l.width(), 100);
        // Count full batches at 100 until the one probe of 101, then the refusal that follows it.
        let mut waits = Vec::new();
        let mut at_100 = 0;
        for _ in 0..(4 * PROBE_AFTER) {
            let w = l.width();
            if w == 100 {
                at_100 += 1;
                l.answered(w);
            } else {
                assert_eq!(w, 101);
                waits.push(at_100);
                at_100 = 0;
                l.refused(w, None);
                assert_eq!(l.width(), 100);
            }
        }
        // The first wait is PROBE_AFTER minus the 40 batches already run while settling; after the
        // refused probe the wait doubles. Never an earlier retry of 101.
        assert!(waits[0] >= PROBE_AFTER - 40, "{waits:?}");
        assert_eq!(waits[1], 2 * PROBE_AFTER, "{waits:?}");
    }

    #[test]
    fn a_transient_small_refusal_is_recovered_from() {
        let l = BatchLimit::default();
        // An endpoint with no real limit answers [] once to a 3-call batch.
        l.refused(3, None);
        let mut sends = 0;
        while l.width() < 1000 {
            l.answered(l.width());
            sends += 1;
            assert!(sends < 4 * PROBE_AFTER, "stuck at {}", l.width());
        }
    }

    #[test]
    fn a_stale_refusal_does_not_corrupt_the_search() {
        let l = BatchLimit::default();
        l.refused(200, None); // width 100
        l.answered(100); // ok 100, width 150
                         // A concurrent batch sent at 100 is refused late: 100 was thought good, so ok resets.
        assert_eq!(l.refused(100, None).width, 50);
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
