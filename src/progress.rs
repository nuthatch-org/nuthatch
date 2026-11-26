//! A single self-updating progress line for the one long wait on nuthatch's happy path: the
//! from-deployment backfill. It answers the three questions a user staring at a cold start actually
//! has - how far, how fast, how much longer - and ends on a crisp "caught up to tip".
//!
//! Two output modes, chosen by whether stderr is a terminal:
//! - **TTY:** one carriage-returned line, redrawn ~8×/s. No scrollback spam.
//! - **piped / systemd:** a throttled `tracing::info!` every ~15 s, so a journal gets a sane
//!   heartbeat instead of either silence or a flood - and never a stray carriage return in a file.
//!
//! The reporter is pure presentation: it holds no lock the indexer needs and never touches stored
//! state, so it can't perturb the deterministic core (non-negotiable 4). Timing uses `Instant`, so
//! it's monotonic and unaffected by wall-clock jumps.

use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};

/// Live feedback for a bounded backfill over `[from, to]` blocks. `tick` as windows complete;
/// `finish` when the range is done.
pub struct Backfill {
    from: u64,
    to: u64,
    label: &'static str,
    start: Instant,
    last_render: Instant,
    events: u64,
    tty: bool,
    /// True once we've drawn at least one TTY line (so `finish` knows to clear it).
    drew_line: bool,
    /// The narrow-window warning is given once per backfill (#3122).
    window_warned: bool,
}

/// At or under this many blocks, a provider's learned `eth_getLogs` window is the reason a backfill
/// crawls, and worth saying so (#3122).
const NARROW_WINDOW: u64 = 20;
/// Where the provider's block-range error is explained.
const WINDOW_DOCS: &str = "https://nuthatch-indexer.com/docs/errors/exceed-maximum-block-range/";

/// Redraw cadence on a TTY - fast enough to feel live, slow enough not to spend the backfill
/// drawing.
const TTY_INTERVAL: Duration = Duration::from_millis(120);
/// Log cadence when not a TTY - a heartbeat, not a flood.
const LOG_INTERVAL: Duration = Duration::from_secs(15);

impl Backfill {
    /// `label` is the verb shown on the line ("backfilling", "sealing history"). `from`/`to` are the
    /// block range this reporter covers; `to` is the tip (or finalized ceiling) at the moment it started.
    pub fn new(label: &'static str, from: u64, to: u64) -> Self {
        let now = Instant::now();
        Self {
            from,
            to,
            label,
            start: now,
            // Back-date the first render so the opening tick draws immediately (feels responsive).
            last_render: now - TTY_INTERVAL,
            events: 0,
            tty: std::io::stderr().is_terminal(),
            drew_line: false,
            window_warned: false,
        }
    }

    /// Warn once if the window the provider has taught us is tiny. `window` is the live window as
    /// reported with each backfill step; it only shrinks this far when the endpoint refuses wider
    /// ranges (or the contract is extremely dense), and either way the backfill will be slow.
    /// Observational only: it changes nothing about how the window is learned.
    pub fn note_window(&mut self, block_reached: u64, window: u64) {
        if let Some(msg) = self.window_warning(block_reached, window) {
            tracing::warn!("{msg}");
        }
    }

    fn window_warning(&mut self, block_reached: u64, window: u64) -> Option<String> {
        if self.window_warned || window > NARROW_WINDOW {
            return None;
        }
        let eta = self.eta(block_reached);
        if eta == "-" {
            return None; // no rate yet: wait for the next step rather than guess
        }
        self.window_warned = true;
        Some(format!(
            "the RPC endpoint only allows getLogs windows of about {window} blocks, so this backfill \
             is slow: roughly {eta} remaining. Use an endpoint with a wider eth_getLogs range \
             (`nuthatch doctor` measures it). See {WINDOW_DOCS}"
        ))
    }

    /// Record progress: the highest block now processed, and how many events that step added.
    /// Cheap and throttled internally, so it's safe to call on every window.
    pub fn tick(&mut self, block_reached: u64, new_events: u64) {
        self.events += new_events;
        let interval = if self.tty { TTY_INTERVAL } else { LOG_INTERVAL };
        if self.last_render.elapsed() < interval {
            return;
        }
        self.last_render = Instant::now();
        if self.tty {
            self.render_tty(block_reached);
        } else {
            tracing::info!(
                "{} {:.1}% - block {} of {}, {} events, {} ev/s",
                self.label,
                self.pct(block_reached),
                block_reached,
                self.to,
                self.events,
                self.events_per_sec() as u64,
            );
        }
    }

    /// The range is complete. Clears any TTY line and logs a one-line summary. `caught_up` picks the
    /// wording: `true` for the final tip-catch-up ("caught up to tip…"), `false` for an intermediate
    /// phase like the seal-direct bulk ("sealed history…").
    pub fn finish(mut self, block_reached: u64, caught_up: bool) {
        self.clear_line();
        let secs = self.start.elapsed().as_secs_f64().max(0.001);
        let eps = (self.events as f64 / secs) as u64;
        if caught_up {
            tracing::info!(
                "✓ caught up to tip at block {block_reached} - {} events in {}, {eps} ev/s; now following",
                self.events,
                fmt_dur(secs),
            );
        } else {
            tracing::info!(
                "✓ {} done - {} events over blocks {}..={block_reached} in {}, {eps} ev/s",
                self.label,
                self.events,
                self.from,
                fmt_dur(secs),
            );
        }
    }

    fn render_tty(&mut self, block_reached: u64) {
        let eta = self.eta(block_reached);
        let mut err = std::io::stderr().lock();
        // `\r` to column 0, `\x1b[2K` clears the whole line so a shorter redraw leaves no tail.
        let _ = write!(
            err,
            "\r\x1b[2K  {} {:5.1}% │ block {} │ {} events │ {} ev/s │ ETA {}",
            self.label,
            self.pct(block_reached),
            block_reached,
            self.events,
            self.events_per_sec() as u64,
            eta,
        );
        let _ = err.flush();
        self.drew_line = true;
    }

    fn clear_line(&mut self) {
        if self.tty && self.drew_line {
            let mut err = std::io::stderr().lock();
            let _ = write!(err, "\r\x1b[2K");
            let _ = err.flush();
            self.drew_line = false;
        }
    }

    fn pct(&self, block_reached: u64) -> f64 {
        let span = self.to.saturating_sub(self.from).max(1);
        let done = block_reached.saturating_sub(self.from).min(span);
        (done as f64 / span as f64) * 100.0
    }

    fn events_per_sec(&self) -> f64 {
        self.events as f64 / self.start.elapsed().as_secs_f64().max(0.001)
    }

    /// ETA from *block* rate, not event rate - it stays meaningful even when events are sparse (a
    /// wide-window backfill over millions of near-empty blocks still shows honest time-remaining).
    fn eta(&self, block_reached: u64) -> String {
        let span = self.to.saturating_sub(self.from).max(1);
        let done = block_reached.saturating_sub(self.from).min(span);
        let secs = self.start.elapsed().as_secs_f64().max(0.001);
        let blocks_per_s = done as f64 / secs;
        if blocks_per_s <= 0.0 {
            return "-".into();
        }
        fmt_dur(span.saturating_sub(done) as f64 / blocks_per_s)
    }
}

/// Compact human duration: `42s`, `3m12s`, `1h04m`.
fn fmt_dur(secs: f64) -> String {
    let s = secs.round() as u64;
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{:02}s", s / 60, s % 60)
    } else {
        format!("{}h{:02}m", s / 3600, (s % 3600) / 60)
    }
}

/// A periodic, log-visible "are we behind or at tip" signal (issue #302). The one-time "✓ caught up
/// to tip … now following" line above and the `nuthatch_tip_lag_blocks` metric both existed already,
/// but neither serves an operator who is reading logs rather than scraping Prometheus: the log line
/// fires once, ever, and after that the tip loop runs quiet by design (`index_loop`'s steady-state
/// comment). This restates the same lag on a slow timer so it shows up in `journalctl`/stdout without
/// pattern-matching a tick character - structured (`block`, `tip`, `blocks_behind` fields), so it
/// reads the same under `--log-format json`.
pub struct TipHeartbeat {
    last_log: Instant,
}

/// Slow enough not to spam a quiet tip loop, frequent enough that "is it stuck?" never waits long
/// for an answer.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

impl TipHeartbeat {
    pub fn new() -> Self {
        // Back-dated so the first call logs immediately rather than waiting a full interval.
        Self {
            last_log: Instant::now() - HEARTBEAT_INTERVAL,
        }
    }

    /// `at` is the next block this cursor has yet to process; `tip` is the source's latest known
    /// head. Throttled internally, so it's cheap to call on every loop iteration.
    pub fn maybe_log(&mut self, at: u64, tip: u64) {
        if self.last_log.elapsed() < HEARTBEAT_INTERVAL {
            return;
        }
        self.last_log = Instant::now();
        let (block, blocks_behind) = Self::lag(at, tip);
        if blocks_behind == 0 {
            tracing::info!(block, tip, blocks_behind, "at tip");
        } else {
            tracing::info!(block, tip, blocks_behind, "following, behind tip");
        }
    }

    /// `(last block processed, how far behind tip that leaves us)`. Split out from `maybe_log` so the
    /// arithmetic is unit-testable without standing up a `tracing` subscriber to capture the log line.
    fn lag(at: u64, tip: u64) -> (u64, u64) {
        let block = at.saturating_sub(1);
        (block, tip.saturating_sub(block))
    }
}

impl Default for TipHeartbeat {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_dur_scales_by_magnitude() {
        assert_eq!(fmt_dur(0.4), "0s");
        assert_eq!(fmt_dur(42.0), "42s");
        assert_eq!(fmt_dur(192.0), "3m12s");
        assert_eq!(fmt_dur(3840.0), "1h04m");
    }

    #[test]
    fn pct_is_bounded_and_proportional() {
        let b = Backfill::new("backfilling", 100, 1100);
        assert_eq!(b.pct(100).round() as u64, 0);
        assert_eq!(b.pct(600).round() as u64, 50);
        assert_eq!(b.pct(1100).round() as u64, 100);
        // Never exceeds 100% even if a caller reports past `to`.
        assert_eq!(b.pct(9999).round() as u64, 100);
        // Never underflows below `from`.
        assert_eq!(b.pct(0).round() as u64, 0);
    }

    #[test]
    fn eta_is_dash_before_any_progress() {
        let b = Backfill::new("backfilling", 0, 1000);
        // No blocks done yet → no rate → no estimate.
        assert_eq!(b.eta(0), "-");
    }

    #[test]
    fn a_ten_block_window_warns_once_with_window_eta_and_docs() {
        let mut b = Backfill::new("sealing history", 0, 1000);
        b.start = Instant::now() - Duration::from_secs(10);
        assert_eq!(b.window_warning(100, 21), None, "a wide window is quiet");
        let msg = b
            .window_warning(100, 10)
            .expect("warns on a 10-block window");
        assert!(msg.contains("10 blocks"), "{msg}");
        assert!(msg.contains("remaining") && msg.contains("1m"), "{msg}"); // 900 blocks at 10/s = 90s
        assert!(
            msg.contains("docs/errors/exceed-maximum-block-range"),
            "{msg}"
        );
        assert_eq!(
            b.window_warning(200, 10),
            None,
            "only one warning per backfill"
        );
    }

    #[test]
    fn tick_accumulates_events() {
        let mut b = Backfill::new("backfilling", 0, 1000);
        b.tick(100, 5);
        b.tick(200, 7);
        assert_eq!(b.events, 12);
    }

    #[test]
    fn heartbeat_lag_reports_zero_at_tip() {
        // `at` is the next block to process, so `at == tip + 1` means everything through `tip` is done.
        assert_eq!(TipHeartbeat::lag(1001, 1000), (1000, 0));
    }

    #[test]
    fn heartbeat_lag_reports_the_gap_while_behind() {
        assert_eq!(TipHeartbeat::lag(940, 1000), (939, 61));
    }

    #[test]
    fn heartbeat_lag_never_underflows_at_genesis() {
        assert_eq!(TipHeartbeat::lag(0, 0), (0, 0));
    }

    #[test]
    fn heartbeat_throttles_to_the_interval() {
        let mut hb = TipHeartbeat::new();
        // Back-dated construction means the first call is never suppressed.
        assert!(hb.last_log.elapsed() >= HEARTBEAT_INTERVAL);
        hb.maybe_log(940, 1000);
        // Immediately after logging, `last_log` is fresh - a second call in the same instant would
        // be suppressed rather than re-logging every loop iteration.
        assert!(hb.last_log.elapsed() < HEARTBEAT_INTERVAL);
    }
}
