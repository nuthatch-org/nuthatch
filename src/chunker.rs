//! Adaptive `eth_getLogs` range sizing (RFC-0004 §2).
//!
//! Providers cap `getLogs` by *result count* (and differ wildly: 10k here, "response size" there),
//! and a good block-window for dense USDC is a terrible one for sparse Horizon. Instead of a
//! per-chain constant that has to be hand-tuned per provider, one controller targets a response
//! budget (~2,000 logs) and adjusts the window multiplicatively from feedback: an overshoot or a
//! "result too large" error shrinks it, an undershoot grows it. The same code handles both density
//! extremes and self-heals into any provider's cap.

/// Target logs per `getLogs` response. Providers cap on result count, so this is a log (not decoded
/// event) budget - comfortably under the common 10k caps, with headroom for a growth step.
pub const TARGET_LOGS_PER_RESPONSE: u64 = 2_000;
/// Never go below one block (a single very dense block can still exceed the target - that's fine, we
/// can't split a block) or above this ceiling (keeps sparse-chain growth sane).
pub const MIN_WINDOW: u64 = 1;
pub const MAX_WINDOW: u64 = 100_000;

/// Window ceiling for a nest that fetches a header per block (RFC-0036). Four batches of
/// `MAX_TIMESTAMP_BATCH` (200), so one window is one fan-out wave rather than a hundred.
pub const HEADER_WINDOW_CAP: u64 = 800;
/// Window ceiling for a seal-direct backfill that resolves IPFS documents inline. A window's documents
/// and the rows they become are held until the window is merged, and a 20,000-block Gnosis window held
/// about 1.2 million typed rows, 7.1 GB RSS, before its first cut (2026-09-13). The QoS oracle posts two
/// documents a minute, so 400 blocks names about thirteen.
pub const DOCUMENT_WINDOW_CAP: u64 = 400;

/// A block-window controller that converges on `target` logs per response.
#[derive(Debug, Clone)]
pub struct AdaptiveWindow {
    window: u64,
    target: u64,
    min: u64,
    max: u64,
    /// The widest range the provider has served **whole** - not one the splitter had to cut up.
    ///
    /// Growth is otherwise evidence-free: `observed(0)` multiplies the window fourfold on an empty
    /// range, and an empty range says nothing about capacity. From a 1,000-block start that reaches
    /// 100,000 in four steps, and discovering the real cap from there costs a binary search down -
    /// refused at 100k, 50k, 25k and 12.5k before anything succeeds, each refusal carrying a backoff
    /// against a real provider. Measured: 24 refusals reaching 100,000 blocks, against 4 reaching
    /// 16,000 once growth is bounded.
    ///
    /// One 4x step past proven capacity is exploration. Four steps past it is guessing.
    served_whole: u64,
    /// The ceiling the controller was constructed with (or last given by [`set_max`](Self::set_max)).
    /// `max` may sit below it after [`served_by_splitting`](Self::served_by_splitting) lowered it;
    /// recovery climbs back towards this, never past it.
    hard_max: u64,
    /// The width a refusal taught, if any (#672), kept apart from `max` so a configured cap that
    /// dips below it and lifts again does not erase the lesson (#1170, review). `None` once recovery
    /// has climbed back to the configured ceiling, or when nothing was ever refused.
    learned: Option<u64>,
    /// Consecutive windows served whole *at the lowered ceiling* (#1170). The evidence that the
    /// ceiling is too low now, whatever it was when a refusal set it.
    whole_streak: u32,
    /// The streak the next doubling needs: [`RECOVERY_STREAK`], doubled each time a recovered
    /// ceiling is refused again (#1787), up to [`MAX_RECOVERY_STREAK`].
    streak_needed: u32,
    /// The ceiling was last moved by a doubling that has not yet held for a streak of its own.
    probing: bool,
}

/// How many consecutive windows must be served whole at a lowered ceiling before it doubles (#1170).
///
/// A refusal halves in one step; recovery doubles in four. The asymmetry is deliberate: a ceiling
/// set by a provider's genuine result cap should stay put, and four clean windows at it are what
/// distinguishes "the pressure has passed" from one lucky answer. Measured on the gns nest,
/// 2026-09-05: an hour of pool-wide 429s from a co-tenant walked the ceiling down to ~10 blocks, and
/// with no path back up the backfill ran a hundred times slower for the next three hours until an
/// operator restarted it. This is that path.
pub const RECOVERY_STREAK: u32 = 4;

/// The longest streak a ceiling waits for after its recoveries keep being refused (#1787). A provider
/// whose cap sits just under a doubling is then asked past it once every 64 windows, not every 5.
pub const MAX_RECOVERY_STREAK: u32 = RECOVERY_STREAK * 16;

impl AdaptiveWindow {
    /// Start from `initial` (typically the chain's default window), targeting `target` logs/response.
    pub fn new(initial: u64, target: u64, min: u64, max: u64) -> Self {
        Self {
            window: initial.clamp(min.max(1), max),
            target: target.max(1),
            min: min.max(1),
            max,
            served_whole: 0,
            hard_max: max,
            learned: None,
            whole_streak: 0,
            streak_needed: RECOVERY_STREAK,
            probing: false,
        }
    }

    /// A sensible controller for a chain whose default window is `initial`.
    pub fn for_window(initial: u64) -> Self {
        Self::new(initial, TARGET_LOGS_PER_RESPONSE, MIN_WINDOW, MAX_WINDOW)
    }

    /// The controller for a nest that also fetches **one header per block** (RFC-0036 `[extract]
    /// blocks`), which changes what a window costs.
    ///
    /// The ordinary controller sizes on *logs per response*, so a range with no matching logs grows to
    /// [`MAX_WINDOW`]. For a blocks nest that is exactly backwards: headers cost one request per block
    /// regardless of logs, so the emptiest ranges grow the window widest and then demand the most
    /// header requests. Measured on OBIB case 3 (blocks 0-100,000, no contracts, so *every* window is
    /// zero-log): the window reached 20,480, which at 200 blocks per batch is 102 batches for one
    /// window, times the window fan-out - and the provider rate-limited into partial responses.
    ///
    /// Narrowing cannot fix it, and that is the point: `batch_is_narrowable` deliberately refuses to
    /// split under a rate limit, because splitting doubles the request count in the wrong direction.
    /// The answer is *fewer* requests, not smaller ones, so the ceiling is lowered rather than the
    /// batch. `HEADER_WINDOW_CAP` keeps one window to a handful of header batches.
    pub fn for_window_with_headers(initial: u64) -> Self {
        Self::new(
            initial.min(HEADER_WINDOW_CAP),
            TARGET_LOGS_PER_RESPONSE,
            MIN_WINDOW,
            HEADER_WINDOW_CAP,
        )
    }

    /// The controller for a seal-direct backfill that resolves documents inline; see [`DOCUMENT_WINDOW_CAP`].
    pub fn for_window_with_documents(initial: u64) -> Self {
        Self::new(
            initial.min(DOCUMENT_WINDOW_CAP),
            TARGET_LOGS_PER_RESPONSE,
            MIN_WINDOW,
            DOCUMENT_WINDOW_CAP,
        )
    }

    /// The block span to request next.
    pub fn window(&self) -> u64 {
        self.window
    }

    /// Feed back how many logs the last response held; adjust toward the target. Change is damped to
    /// at most 4× per step so a single sparse (0-log) or spiky window doesn't swing the window wildly.
    /// A range the provider served **whole** at this width. Raises the evidence ceiling that bounds
    /// growth (#672), and - when the ceiling itself was lowered by a refusal - counts towards lifting
    /// it again (#1170): [`RECOVERY_STREAK`] windows served whole **at exactly** the ceiling double
    /// it, up to the ceiling the controller was built with.
    ///
    /// Exactly, not at least. A window served whole *below* the ceiling says nothing about the
    /// ceiling. A window *wider* than the ceiling can only have been issued before the refusal that
    /// lowered it - the pipelined backfill keeps several windows in flight, and the ones already out
    /// when a refusal lands still come back - so it is evidence about the provider before the
    /// pressure, not after it, and four of those arriving together must not double the ceiling the
    /// refusal has only just set.
    pub fn served_whole(&mut self, width: u64) {
        self.served_whole = self.served_whole.max(width);
        if self.max < self.hard_max && width == self.max {
            self.whole_streak += 1;
            if self.whole_streak >= self.streak_needed {
                // A doubled ceiling that held for a full streak has proved itself; the wait resets.
                if self.probing {
                    self.streak_needed = RECOVERY_STREAK;
                }
                self.probing = true;
                self.max = self.max.saturating_mul(2).min(self.hard_max);
                // The lesson moves only when recovery has climbed *past* it. Under a cap that sits
                // below the remembered width, reaching the cap says nothing about the refusal
                // (#1170, review): the width the provider refused is still the width it refused.
                if self.learned.is_none_or(|l| self.max > l) {
                    self.learned = (self.max < self.hard_max).then_some(self.max);
                }
                self.whole_streak = 0;
            }
        }
    }

    /// The current ceiling: the constructed one, or a lower one a refusal taught (#672) that
    /// [`served_whole`](Self::served_whole) has not yet lifted back (#1170).
    pub fn ceiling(&self) -> u64 {
        self.max
    }

    pub fn observed(&mut self, logs: u64) {
        let next = if logs == 0 {
            // Nothing came back - grow to cover more ground, but only 4× at a time.
            self.window.saturating_mul(4)
        } else {
            // Proportional to hit the target, clamped to a 4× move either direction.
            let scaled = (self.window as u128 * self.target as u128 / logs as u128) as u64;
            scaled.clamp((self.window / 4).max(1), self.window.saturating_mul(4))
        };
        // Never more than one 4x step past a width actually served whole (#672). Before any
        // evidence exists `served_whole` is 0, which leaves the cold start free to grow as before,
        // bounded only by `max`.
        let evidence_ceiling = if self.served_whole == 0 {
            self.max
        } else {
            self.served_whole.saturating_mul(4).min(self.max)
        };
        self.window = next.clamp(self.min, evidence_ceiling.max(self.min));
    }

    /// A fetch only succeeded because something below split it (#672).
    ///
    /// This is the signal the controller was missing, and the absence of it is the whole bug.
    /// `fetch_logs_splitting` recovers from a refused range by halving until the pieces are served,
    /// then hands back the merged result - so the caller sees `Ok` and, on an empty range, calls
    /// [`observed`](Self::observed) with zero and **grows the window fourfold**. The controller is
    /// being told the width was fine by the very code that had to cut it into eight pieces to make it
    /// work.
    ///
    /// Traced on a 200,000-block prefix behind a 10,000-block cap: the window climbed
    /// `1000 → 4000 → 16000 → 64000 → 100000`, pinned itself at the ceiling, and every window
    /// thereafter paid for four levels of recursive splitting and the backoff between them. Fifty-three
    /// requests in four minutes, almost all of them refusals being patiently halved.
    ///
    /// `narrowest_served` is the widest range that actually came back, which is the only width there
    /// is evidence for. Taking it as the ceiling can undershoot a provider's real cap; that is the
    /// intended direction, because too low is slow and too high does not terminate.
    pub fn served_by_splitting(&mut self, narrowest_served: u64) {
        let w = narrowest_served.max(self.min);
        if w < self.max {
            self.max = w;
            self.window = self.window.min(self.max);
        }
        self.learned = Some(self.learned.map_or(w, |l| l.min(w)));
        // A refusal is fresh evidence the ceiling is real; the recovery count starts again (#1170).
        self.whole_streak = 0;
        // Refused at a width recovery climbed to: the next attempt waits twice as long (#1787).
        if self.probing {
            self.streak_needed = (self.streak_needed * 2).min(MAX_RECOVERY_STREAK);
            self.probing = false;
        }
    }

    /// The provider rejected the range as too large - halve hard and (the caller) retry the range.
    pub fn too_large(&mut self) {
        self.window = (self.window / 2).max(self.min);
        self.whole_streak = 0;
    }

    /// Re-bound the ceiling for one iteration (RFC-0036, #458). A caller whose ceiling changes
    /// under it - the runtime tip loop's cursor gains and loses a header-costed nest as mounts
    /// arrive and retire - must bound the controller itself, not just the span it lets it issue:
    /// capping only the use still feeds `observed` results from the capped span into a `window`
    /// that is free to keep climbing, so it drifts to `max` unseen and hands back the whole drift
    /// in one step the moment the ceiling is lifted. Calling this every iteration keeps `window`
    /// clamped down live, so growth back up after a ceiling is lifted goes through `observed`'s own
    /// 4x-per-step damping instead of jumping straight to wherever it had silently drifted.
    pub fn set_max(&mut self, max: u64) {
        // The configured ceiling moves; the width a refusal taught (#672) is remembered on its own
        // and re-applied beneath whatever the cap now is, so a caller re-applying its cap every
        // iteration - as the runtime loop does - cannot undo the lesson, and neither can a cap that
        // dips below the lesson and lifts again (#1170, review). A controller that has learnt
        // nothing follows the configured ceiling exactly as before.
        self.hard_max = max.max(self.min);
        self.max = self.learned.map_or(self.hard_max, |l| l.min(self.hard_max));
        self.window = self.window.min(self.max);
    }

    /// Lower the ceiling to an operator's explicit `--window` (#1853), never raise it.
    pub fn capped_at(mut self, ceiling: Option<u64>) -> Self {
        if let Some(c) = ceiling {
            self.set_max(c.min(self.hard_max));
        }
        self
    }
}

pub use nuthatch_core::rpc::is_result_too_large;

#[cfg(test)]
mod tests {

    /// **The RFC-0029 regression, at the layer that decides whether to split.**
    ///
    /// `class_of` returning `Transient` used to short-circuit to `false`, which made the cap markers
    /// below it unreachable for any error the status classifier had not recognised. `Transient` is the
    /// fall-through default - the *absence* of a finding - so it must defer to direct textual evidence
    /// rather than outrank it.
    #[test]
    fn a_transient_classification_does_not_veto_textual_cap_evidence() {
        let err = anyhow::Error::new(crate::rpc::ClassifiedError {
            class: crate::rpc::FailureClass::Transient,
            detail: "HTTP 400: {\"error\":{\"message\":\"Log response size exceeded. You can make \
                     eth_getLogs requests with up to a 10,000 block range\"}}"
                .into(),
        });
        assert!(
            is_result_too_large(&err),
            "a Transient classification carrying cap language must still split - it is the absence of \
             a finding, not a finding that this is not a cap"
        );
    }

    /// The other half of the same rule: a positive finding *does* win. Splitting on a rate limit would
    /// turn one throttled request into two, and splitting on a terminal auth failure is pointless work
    /// against an endpoint that will keep refusing.
    #[test]
    fn positive_classifications_still_veto_a_split() {
        for class in [
            crate::rpc::FailureClass::RateLimited { retry_after: None },
            crate::rpc::FailureClass::Terminal,
        ] {
            let err = anyhow::Error::new(crate::rpc::ClassifiedError {
                class,
                // Cap language present *and* ignored, deliberately.
                detail: "response size exceeded".into(),
            });
            assert!(
                !is_result_too_large(&err),
                "a positive classification must outrank the marker list"
            );
        }
    }
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn shrinks_on_overshoot_grows_on_undershoot() {
        let mut w = AdaptiveWindow::new(100, 2_000, 1, 100_000);
        // Way over target → shrink (proportional, damped to 4× min).
        w.observed(8_000); // 100 * 2000/8000 = 25
        assert_eq!(w.window(), 25);
        // Under target → grow toward it.
        let before = w.window();
        w.observed(500); // 25 * 2000/500 = 100
        assert!(w.window() > before);
        assert_eq!(w.window(), 100);
    }

    #[test]
    fn zero_logs_grows_capped_at_4x() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.observed(0);
        assert_eq!(w.window(), 4_000); // 4× growth, no division by zero
    }

    #[test]
    fn respects_bounds_and_damping() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 10, 5_000);
        w.observed(1_000_000); // huge overshoot → clamp to 4× shrink then min bound
        assert_eq!(w.window(), 250); // 1000/4, above min 10
        let mut w2 = AdaptiveWindow::new(4_000, 2_000, 1, 5_000);
        w2.observed(0); // 4× = 16000 but capped at max 5000
        assert_eq!(w2.window(), 5_000);
    }

    #[test]
    fn too_large_halves() {
        let mut w = AdaptiveWindow::new(2_000, 2_000, 1, 100_000);
        w.too_large();
        assert_eq!(w.window(), 1_000);
    }

    /// **#903, found by a nest dying at 3.6 hours.** A tip race is not a cap.
    ///
    /// Alchemy answers a request that runs past its served head with
    /// `-32602 "block range extends beyond current head block"`. The cap marker list contains the
    /// bare string `"block range"` - which it needs, see the assertion below - so the message was
    /// classified as a result cap. The chunker narrowed the window, which cannot help when the blocks
    /// do not exist yet, reached a single block, and the tip loop exited with
    /// `block N alone exceeds the provider's getLogs result cap`. A diagnosis that was never true,
    /// on a nest that only needed to wait for head to catch up.
    ///
    /// Both directions are asserted, and the second one is the point: the obvious fix is to delete
    /// the bare `"block range"` marker, and that would break cap detection on Alchemy's *real* cap
    /// message, which contains `block range` and none of the more specific markers. A dead backfill
    /// instead of a dead nest is not an improvement.
    #[test]
    fn a_tip_race_is_not_a_provider_cap() {
        let beyond = "rpc error: {\"code\":-32602,\"message\":\"block range extends beyond current head block\"}";
        assert!(
            !is_result_too_large(&anyhow!("{beyond}")),
            "asking past head is a race to retry, not a range to narrow"
        );

        // The marker that made it ambiguous is still load-bearing for the genuine article.
        assert!(
            is_result_too_large(&anyhow!(
                "Log response size exceeded. You can make eth_getLogs requests with up to a 10,000 block range"
            )),
            "Alchemy's real cap message carries `block range` and none of the narrower markers"
        );
    }

    #[test]
    fn detects_provider_cap_errors() {
        assert!(is_result_too_large(&anyhow!(
            "Log response size exceeded. Try a smaller range"
        )));
        assert!(is_result_too_large(&anyhow!(
            "query returned more than 10000 results"
        )));
        assert!(is_result_too_large(&anyhow!("block range is too wide")));
        assert!(is_result_too_large(&anyhow!(
            "ranges over 10000 blocks are not supported on free plan"
        )));
        // #801 as logged by `index_loop`: the 2.7.0 classifier called this Transient, then retried
        // the same 81,920-block window forever. Either classification must split.
        let logged = "HTTP 400 Bad Request: {\"message\":\"ranges over 10000 blocks are not supported on free plan\"}";
        for class in [
            crate::rpc::FailureClass::Transient,
            crate::rpc::FailureClass::Narrowable {
                suggested: None,
                escalated_from_rate_limit: false,
            },
        ] {
            let err = anyhow::Error::new(crate::rpc::ClassifiedError {
                class: class.clone(),
                detail: logged.into(),
            });
            assert!(
                is_result_too_large(&err),
                "Alchemy free-plan range cap must split even when classed {class:?}: {err:#}"
            );
        }
        assert!(!is_result_too_large(&anyhow!("connection reset by peer")));
        assert!(!is_result_too_large(&anyhow!("HTTP status 521")));
    }

    /// The regression that motivated RFC-0028. These are **measured** provider responses, captured on
    /// 2026-07-28 by asking each endpoint for a populated 199k-block range of Arbitrum One native USDC
    /// - not paraphrases. A marker list tested against invented strings tests our imagination.
    ///
    /// `arb1.arbitrum.io` is one of the endpoints nuthatch ships as an Arbitrum default, and its
    /// phrasing matched nothing, so `fetch_logs_splitting` never recursed and the zero-setup path
    /// stalled on any busy contract.
    #[test]
    fn detects_the_measured_cap_errors_of_endpoints_we_ship() {
        // arb1.arbitrum.io, HTTP 200, code -32000. Matched nothing before this fix.
        assert!(
            is_result_too_large(&anyhow!("logs matched by query exceeds limit of 10000")),
            "arb1.arbitrum.io is a shipped default; its cap message must trigger splitting"
        );
        // Alchemy, HTTP 200, code -32602 (generic invalid-params - hence matching on text, not code).
        assert!(is_result_too_large(&anyhow!(
            "Log response size exceeded. You can make eth_getLogs requests with up to a 10,000 block \
             range and no limit on the response size, or you can request any block range with a cap \
             of 10K logs in the response. Based on your parameters and the response size limit, this \
             block range should work: [0x1000000, 0x1007fff]"
        )));
        // Infura's phrasing, for completeness alongside the two we measured directly.
        assert!(is_result_too_large(&anyhow!(
            "query returned more than 10000 results"
        )));
    }

    /// RFC-0028 §3e: when the RPC client has already classified an error properly - from the HTTP
    /// status and the JSON-RPC error object, not a flattened string - that answer wins.
    ///
    /// Two classifiers that can disagree about the same error is worse than one, because which answer
    /// applies would depend on the code path and the resulting bug would be unreproducible.
    #[test]
    fn the_structured_classification_wins_over_text_matching() {
        // Narrowable by structure, with wording that matches no marker at all.
        let structured = anyhow::Error::new(crate::rpc::ClassifiedError {
            class: crate::rpc::FailureClass::Narrowable {
                suggested: None,
                escalated_from_rate_limit: false,
            },
            detail: "every endpoint rate-limited this request".into(),
        });
        assert!(
            is_result_too_large(&structured),
            "a structurally-narrowable error must split even when its text matches nothing"
        );

        // Terminal by structure, with wording that *would* have matched the text fallback. Splitting
        // an auth failure would be pointless work against an endpoint that will keep refusing.
        let terminal = anyhow::Error::new(crate::rpc::ClassifiedError {
            class: crate::rpc::FailureClass::Terminal,
            detail: "forbidden: response size limit exceeded for your plan".into(),
        });
        assert!(
            !is_result_too_large(&terminal),
            "structure must override the text fallback, not merely supplement it"
        );

        // Unclassified errors (test tapes, the ExEx source) still fall back to text matching.
        assert!(is_result_too_large(&anyhow!(
            "logs matched by query exceeds limit of 10000"
        )));
    }

    /// The other half of the contract: widening the list must not start swallowing failures that are
    /// nothing to do with size. Shrinking on these would trade throughput for nothing and mask real
    /// faults - including the auth rejection RFC-0028 slice 1 made terminal.
    #[test]
    fn does_not_mistake_other_failures_for_a_cap() {
        for msg in [
            "Must be authenticated!",
            "Archive requests require a personal token",
            "429 Too Many Requests",
            "connection reset by peer",
            "error decoding response body",
            "HTTP status 503",
        ] {
            assert!(
                !is_result_too_large(&anyhow!("{msg}")),
                "{msg:?} is not a size cap and must not trigger a shrink"
            );
        }
    }

    /// #1170. A refusal lowers the ceiling in one step (`served_by_splitting`); a run of windows
    /// served whole *at that ceiling* lifts it again, doubling per `RECOVERY_STREAK`, and never past
    /// the ceiling the controller was built with. Before this the ceiling only ever went down.
    #[test]
    fn a_ceiling_lowered_by_a_refusal_recovers_after_clean_windows() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(10);
        assert_eq!(w.ceiling(), 10);
        assert_eq!(w.window(), 10);
        for _ in 0..RECOVERY_STREAK {
            w.served_whole(10);
        }
        assert_eq!(
            w.ceiling(),
            20,
            "four clean windows at the ceiling must double it"
        );
        // The window itself climbs through `observed`'s own damping, now that the ceiling allows it.
        w.observed(0);
        assert_eq!(w.window(), 20);
        for _ in 0..RECOVERY_STREAK {
            w.served_whole(20);
        }
        assert_eq!(w.ceiling(), 40);
        // Recovery is bounded by the constructed ceiling, never the sky.
        for _ in 0..200 {
            w.served_whole(w.ceiling());
        }
        assert_eq!(w.ceiling(), 100_000);
    }

    /// #1787: a provider that refuses every recovery past 300 blocks. Without a growing wait it is
    /// asked past its cap every fifth window for as long as the backfill runs.
    #[test]
    fn a_recovery_refused_again_waits_longer_before_the_next() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(250);
        let mut refusals = 0;
        for _ in 0..1_000 {
            let width = w.window();
            if width > 300 {
                refusals += 1;
                w.served_by_splitting(width / 2);
            } else {
                w.served_whole(width);
                w.observed(0);
            }
        }
        assert_eq!(w.streak_needed, MAX_RECOVERY_STREAK);
        // Waits of 4, 8, 16 and 32 windows, then 64 for the rest: 18 refusals. A fixed wait is 200.
        assert_eq!(refusals, 18);
    }

    /// The wait returns to [`RECOVERY_STREAK`] once a doubled ceiling holds for a streak of its own.
    #[test]
    fn a_recovered_ceiling_that_holds_ends_the_longer_wait() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(250);
        for _ in 0..RECOVERY_STREAK {
            w.served_whole(250);
        }
        assert_eq!(w.ceiling(), 500);
        w.served_by_splitting(250);
        assert_eq!(w.ceiling(), 250);
        for _ in 0..RECOVERY_STREAK {
            w.served_whole(250);
        }
        assert_eq!(
            w.ceiling(),
            250,
            "a refused recovery must wait longer than the first"
        );
        for _ in 0..RECOVERY_STREAK {
            w.served_whole(250);
        }
        assert_eq!(w.ceiling(), 500);
        for _ in 0..(RECOVERY_STREAK * 2) {
            w.served_whole(500);
        }
        assert_eq!(w.ceiling(), 1_000, "500 held for the longer streak");
        for _ in 0..RECOVERY_STREAK {
            w.served_whole(1_000);
        }
        assert_eq!(
            w.ceiling(),
            2_000,
            "and the wait is back to {RECOVERY_STREAK}"
        );
    }

    /// The pipelined path keeps `concurrency` windows in flight, so when a refusal lowers the ceiling
    /// the wider windows already issued still complete. They were served before the pressure and say
    /// nothing about the provider under it; counted, four of them would double a ceiling the refusal
    /// had set a moment earlier.
    #[test]
    fn stale_wider_windows_completing_after_a_refusal_are_not_recovery_evidence() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(10);
        for _ in 0..(RECOVERY_STREAK * 2) {
            w.served_whole(6_250);
        }
        assert_eq!(
            w.ceiling(),
            10,
            "in-flight wider windows from before the refusal lifted the ceiling"
        );
        // Windows sized at the new ceiling are the evidence that counts.
        for _ in 0..RECOVERY_STREAK {
            w.served_whole(10);
        }
        assert_eq!(w.ceiling(), 20);
    }

    #[test]
    fn a_window_served_whole_below_the_ceiling_is_not_evidence_about_the_ceiling() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(100);
        for _ in 0..(RECOVERY_STREAK * 3) {
            w.served_whole(50);
        }
        assert_eq!(
            w.ceiling(),
            100,
            "a narrower success says nothing about a wider cap"
        );
    }

    #[test]
    fn a_fresh_refusal_restarts_the_recovery_count() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(10);
        for _ in 0..(RECOVERY_STREAK - 1) {
            w.served_whole(10);
        }
        w.too_large();
        for _ in 0..(RECOVERY_STREAK - 1) {
            w.served_whole(10);
        }
        assert_eq!(
            w.ceiling(),
            10,
            "a refusal in the run must reset the count, not pause it"
        );
        w.served_whole(10);
        assert_eq!(w.ceiling(), 20);
    }

    /// #1170, review: the runtime loop re-applies its configured cap on every iteration. That must
    /// move the configured ceiling and leave a refusal-taught one alone, or the lesson is undone
    /// before the recovery streak can start; and a cap set *below* the learnt ceiling still binds.
    #[test]
    fn reapplying_the_configured_cap_keeps_a_learned_ceiling() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(10);
        w.set_max(100_000);
        assert_eq!(
            w.ceiling(),
            10,
            "re-applying the same cap wiped the learnt ceiling"
        );
        w.set_max(5);
        assert_eq!(
            w.ceiling(),
            5,
            "a cap below the learnt ceiling must still bind"
        );
        // Lifting the cap again does not forget the refusal: the ceiling returns to what was learnt.
        w.set_max(100_000);
        assert_eq!(
            w.ceiling(),
            10,
            "a cap that dipped below the lesson and lifted again forgot it"
        );
        // Recovery climbs towards the configured cap that is current, not the one at construction.
        w.set_max(40);
        for _ in 0..(RECOVERY_STREAK * 8) {
            w.served_whole(w.ceiling());
        }
        assert_eq!(w.ceiling(), 40);
        // Recovery under a cap that sits *below* the lesson reaches the cap and no further, and
        // does not forget the lesson: lifting the cap afterwards returns to the refused width.
        let mut v = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        v.served_by_splitting(10);
        v.set_max(5);
        for _ in 0..(RECOVERY_STREAK * 3) {
            v.served_whole(v.ceiling());
        }
        assert_eq!(v.ceiling(), 5);
        v.set_max(100_000);
        assert_eq!(
            v.ceiling(),
            10,
            "recovery that only reached a temporary cap erased the refusal-taught width"
        );
        // A controller that learnt nothing follows the configured ceiling exactly, both ways.
        let mut u = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        u.set_max(800);
        assert_eq!(u.ceiling(), 800);
        u.set_max(100_000);
        assert_eq!(u.ceiling(), 100_000);
    }

    #[test]
    fn an_unlowered_ceiling_does_not_grow_past_its_construction() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 5_000);
        for _ in 0..(RECOVERY_STREAK * 4) {
            w.served_whole(5_000);
        }
        assert_eq!(w.ceiling(), 5_000);
    }

    /// The runtime loop re-applies its cap every iteration, so what recovery earns has to survive
    /// that, both part of the way up and all of it.
    #[test]
    fn a_recovered_ceiling_survives_the_cap_being_reapplied() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(10);
        for _ in 0..RECOVERY_STREAK {
            w.served_whole(10);
        }
        assert_eq!(w.ceiling(), 20);
        w.set_max(100_000);
        assert_eq!(
            w.ceiling(),
            20,
            "re-applying the cap threw away a doubling recovery had earned"
        );

        let mut v = AdaptiveWindow::new(1_000, 2_000, 1, 40);
        v.served_by_splitting(10);
        for _ in 0..(RECOVERY_STREAK * 2) {
            v.served_whole(v.ceiling());
        }
        assert_eq!(v.ceiling(), 40);
        v.set_max(100_000);
        assert_eq!(
            v.ceiling(),
            100_000,
            "recovery reached the configured cap, so the refusal is spent and a lifted cap binds"
        );
    }

    #[test]
    fn windows_served_at_a_cap_below_the_lesson_are_not_recovery_evidence() {
        let mut w = AdaptiveWindow::new(1_000, 2_000, 1, 100_000);
        w.served_by_splitting(10);
        w.set_max(5);
        for _ in 0..(RECOVERY_STREAK - 1) {
            w.served_whole(5);
        }
        w.set_max(100_000);
        assert_eq!(w.ceiling(), 10);
        w.served_whole(10);
        assert_eq!(
            w.ceiling(),
            10,
            "one window at the learnt ceiling doubled it: the ones at the lower cap were counted"
        );
    }
}
