//! Prometheus `/metrics` - the operator's alerting and billing surface (RFC-0005 §6).
//!
//! Hand-rolled: a handful of process-global atomics plus text formatting, not a metrics framework -
//! the single-binary / minimal-deps rule stands, and the metric set is small and fixed. Gauges
//! (tip, watermark, RSS) are set to the latest value; counters (rows, queries) only ever increase.
//! Nothing here phones home: metrics are exposed on the same local API and scraped by the operator.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The one process-wide metrics registry. `const`-constructed, so it needs no lazy init.
pub static METRICS: Metrics = Metrics::new();

/// Why the analytical surface refused a request. This is deliberately a closed set: a caller must
/// never be able to turn a bad query or a route name into a new Prometheus series.
#[derive(Clone, Copy)]
pub enum SqlRejection {
    Busy,
    TooLarge,
    Invalid,
    Bounded,
    Admission,
    Timeout,
    OutOfMemory,
}

impl SqlRejection {
    const ALL: [Self; 7] = [
        Self::Busy,
        Self::TooLarge,
        Self::Invalid,
        Self::Bounded,
        Self::Admission,
        Self::Timeout,
        Self::OutOfMemory,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Busy => 0,
            Self::TooLarge => 1,
            Self::Invalid => 2,
            Self::Bounded => 3,
            Self::Admission => 4,
            Self::Timeout => 5,
            Self::OutOfMemory => 6,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Busy => "busy",
            Self::TooLarge => "too_large",
            Self::Invalid => "invalid",
            Self::Bounded => "bounded",
            Self::Admission => "admission",
            Self::Timeout => "timeout",
            Self::OutOfMemory => "out_of_memory",
        }
    }
}

/// Per-nest counterparts of the nest-scoped signals (SEC-9). In a runtime the process-global gauges and
/// counters blend every mounted nest into one number; these let an operator see each nest's own
/// progress under a `{nest="…"}` label. Updating a per-nest value also bumps the matching global
/// aggregate, so the existing unlabelled series stay correct and backward-compatible.
#[derive(Default)]
pub struct NestMetrics {
    last_block: AtomicU64,
    sealed_through: AtomicU64,
    /// The block this nest's history begins at, once known. A mirror needs it to say what it covers.
    indexed_from: Mutex<Option<u64>>,
    /// RFC-0052 S2: set once this nest mirrors itself, so only publishing nests render the series.
    publish_enabled: AtomicBool,
    publish_target: Mutex<Option<String>>,
    /// Unix seconds of the last pass that succeeded, `0` until one has.
    publish_last_success: AtomicU64,
    publish_sealed_through: AtomicU64,
    /// Blocks spanned by the final local segments the mirror lacked when the publisher last compared
    /// catalogues (#1927). Not an endpoint difference: a quiet table's provisional tail never folds,
    /// and a hole below the newest segment must count.
    publish_lag_blocks: AtomicU64,
    publish_pending: AtomicU64,
    publish_bytes: AtomicU64,
    publish_errors: AtomicU64,
    publish_dead_letter: AtomicBool,
    /// The tip of **this nest's chain**, and when its cursor last polled successfully.
    ///
    /// Duplicated per nest rather than kept only process-globally because a multichain runtime runs one
    /// cursor per chain: with a single global tip, whichever cursor polled last wins, and
    /// `/<nest>/ready` reports another chain's block height. Observed live - a mainnet nest reporting
    /// an Arbitrum tip of 488,677,305 while mainnet was at 25,632,906.
    ///
    /// Every nest on the same cursor holds the same value, which is correct: they share a chain.
    tip: AtomicU64,
    /// The highest block the cursor may index now: the tip, or under `--finality-only` the finality
    /// boundary. `/ready` judges a wedge by the distance to this, not to the tip (#1575).
    ceiling: AtomicU64,
    last_poll_ok: AtomicU64,
    /// Set after a failed source poll and cleared by the next success. This distinguishes "the
    /// process has only just begun" from "the process has already proved its only RPC pool dead".
    /// `/ready` must not give the latter a 90-second green grace period.
    poll_failed: AtomicBool,
    /// Unix seconds this nest's ingest was built, i.e. when it first started trying to reach the chain.
    /// `0` until [`NestMetrics::mark_started`] runs. Bounds `/ready`'s "never polled yet" grace period
    /// (#510): without it, a pool that is dead from the very first poll never sets `last_poll_ok` and
    /// looks permanently "just starting up" instead of eventually stalled.
    started_at: AtomicU64,
    /// Unix seconds `last_block` last actually *changed* value. `0` until it has ever moved. Distinct
    /// from `last_poll_ok` (#578): a source poll can succeed on schedule - keeping `last_poll_ok` fresh
    /// and `/ready` looking healthy - while the cursor itself is wedged refusing to seal the window in
    /// front of it (e.g. `process_window` holding position on an unfetchable block timestamp,
    /// `indexer.rs:3437`). `last_poll_ok` alone cannot see that; this can.
    last_progress: AtomicU64,
    rows_decoded: AtomicU64,
    rows_sealed: AtomicU64,
    reorgs: AtomicU64,
    /// #1666, #2147: windows refused for want of a reorg checkpoint.
    checkpoints_missed: AtomicU64,
    /// RFC-0059 §5: sealed blocks whose rows the folds are not yet checkpointed through, and whether
    /// the writer has stopped. Only a folds build has a writer, so only it has the series.
    #[cfg(feature = "folds")]
    fold_checkpoint_lag_blocks: AtomicU64,
    #[cfg(feature = "folds")]
    fold_writer_faulted: AtomicBool,
    /// RFC-0037: rows an `[[ipfs]]` declaration reads that named no CID it could use.
    ipfs_unreadable: AtomicU64,
    /// RFC-0037 §3: documents named and neither stored nor given up on, at the resolver's last pass.
    ipfs_pending: AtomicU64,
    ipfs_resolved: AtomicU64,
    ipfs_given_up: AtomicU64,
    /// RFC-0037: documents fetched that nothing could prove against their CID, so no row was written.
    ipfs_unverified: AtomicU64,
    /// RFC-0037: documents refused for exceeding the byte, block or depth cap.
    ipfs_oversize: AtomicU64,
    /// RFC-0037 slice 8: proven documents whose content does not fit their declared typed rows.
    ipfs_rows_refused: AtomicU64,
    /// #1399: failed document fetches that were tried again, on either resolving path.
    ipfs_retries: AtomicU64,
    /// #1399: the `--concurrency` asked for and the one the seal-direct pass runs at. `0` until recorded.
    seal_direct_concurrency_requested: AtomicU64,
    seal_direct_concurrency: AtomicU64,
    /// #1399: `--ipfs-window-deadline` in seconds plus one, so `0` is not recorded and `1` is no deadline.
    ipfs_window_deadline: AtomicU64,
    /// #807: the `--seal-direct` history pass, before the hot cursor exists.
    seal_direct_active: AtomicBool,
    seal_direct_origin: AtomicU64,
    seal_direct_completed: AtomicU64,
    seal_direct_target: AtomicU64,
    /// The seal-direct pass's *fetch* position (#1169): the highest block whose logs have been fetched
    /// and decoded into the seal buffer. Distinct from `seal_direct_completed`, which is the durable
    /// watermark a restart resumes from; the gap between them is work a restart redoes.
    seal_direct_fetched: AtomicU64,
    /// The block span the cursor last asked `eth_getLogs` for (#1170), so a window that a rate-limited
    /// hour collapsed to ten blocks is visible before the backfill's ETA is.
    fetch_window: AtomicU64,
    /// When the seal-direct watermark last actually moved (#846). `seal_direct_completed` is a block
    /// number and says nothing about time, so without this a pass that has died and a pass that is
    /// legitimately slow are the same observation - and `/ready` suppresses every other stall term
    /// while a pass is active, so it had nothing left to judge by.
    last_seal_progress: AtomicU64,
    /// #2262: since when the tip path's seal has failed without a success between, and its latest
    /// error. `None` while it succeeds.
    seal_fault: Mutex<Option<(u64, String)>>,
    /// Hot-store file and sealed-segment directory, for #812 footprint gauges. `None` until the
    /// nest is built.
    storage: Mutex<Option<(PathBuf, PathBuf)>>,
    /// Disk usage is expensive to discover for a Parquet tree. Cache it between scrapes; the value
    /// is a gauge with at most a minute of staleness, not a recursive walk per HTTP request (#826).
    hot_bytes: AtomicU64,
    sealed_bytes: AtomicU64,
    storage_scanned_at: AtomicU64,
    /// The dataset directory, whose manifest names this nest's own segments (#1557). `None` for a
    /// fixture; the store's whole size then stands in.
    dataset: Mutex<Option<PathBuf>>,
    dataset_sealed_bytes: AtomicU64,
}

impl NestMetrics {
    /// Also stamps `last_progress` when `v` actually advances the watermark, so `/ready` can tell "the
    /// source answered" apart from "the cursor is getting somewhere" (#578).
    pub fn set_last_block(&self, v: u64) {
        if self.last_block.swap(v, Relaxed) != v {
            self.last_progress.store(now_unix(), Relaxed);
        }
        METRICS.set_last_block(v);
    }
    pub fn last_progress(&self) -> u64 {
        self.last_progress.load(Relaxed)
    }
    /// Test seam, mirroring [`NestMetrics::set_last_poll_ok_for_test`]: pin the progress clock directly,
    /// so a test can simulate "advanced once, then froze" without waiting out real time between calls.
    #[cfg(test)]
    pub fn set_last_progress_for_test(&self, t: u64) {
        self.last_progress.store(t, Relaxed);
    }
    /// This nest's chain tip, and the global aggregate for a solo `dev`.
    pub fn set_tip(&self, v: u64) {
        self.tip.store(v, Relaxed);
        METRICS.set_tip(v);
    }
    pub fn tip(&self) -> u64 {
        self.tip.load(Relaxed)
    }
    pub fn set_ceiling(&self, v: u64) {
        self.ceiling.store(v, Relaxed);
        METRICS.set_ceiling(v);
    }
    pub fn ceiling(&self) -> u64 {
        self.ceiling.load(Relaxed)
    }
    pub fn mark_poll_ok(&self) {
        self.last_poll_ok.store(now_unix(), Relaxed);
        self.poll_failed.store(false, Relaxed);
        METRICS.mark_poll_ok();
    }
    pub fn last_poll_ok(&self) -> u64 {
        self.last_poll_ok.load(Relaxed)
    }
    /// Record a failed source poll. A later successful poll clears this, so this is only a
    /// readiness failure while no successful poll has happened yet.
    pub fn mark_poll_failed(&self) {
        self.poll_failed.store(true, Relaxed);
        METRICS.mark_poll_failed();
    }
    pub fn poll_failed(&self) -> bool {
        self.poll_failed.load(Relaxed)
    }
    /// Test seam: force this nest's last successful poll to an absolute unix time, so a test can make
    /// one cursor *look* dark without waiting out `serve`'s 90-second stall threshold. Deliberately
    /// does NOT touch the process-global aggregate, unlike [`NestMetrics::mark_poll_ok`] - the point of
    /// a cross-cursor stall test is that a healthy chain keeps the global fresh while a dead one does
    /// not, and reading the global is the bug being guarded against (RFC-0021, #356).
    #[cfg(test)]
    pub fn set_last_poll_ok_for_test(&self, t: u64) {
        self.last_poll_ok.store(t, Relaxed);
    }
    /// Stamp this nest's readiness clock - called once, when its ingest is built. Also stamps the
    /// process-global clock the first time any nest calls it, so a solo `dev` (which reads only the
    /// global aggregate) gets the same bounded grace period.
    pub fn mark_started(&self) {
        let now = now_unix();
        self.started_at.store(now, Relaxed);
        METRICS.mark_started_once(now);
    }
    pub fn started_at(&self) -> u64 {
        self.started_at.load(Relaxed)
    }
    /// Test seam, mirroring [`NestMetrics::set_last_poll_ok_for_test`].
    #[cfg(test)]
    pub fn set_started_at_for_test(&self, t: u64) {
        self.started_at.store(t, Relaxed);
    }
    /// Nest-only: does not bump the process-global flag, so a runtime `/ready` fixture cannot
    /// leak into a later solo-dev test that reads [`Metrics::poll_failed`].
    #[cfg(test)]
    pub fn set_poll_failed_for_test(&self, v: bool) {
        self.poll_failed.store(v, Relaxed);
    }
    pub fn last_block(&self) -> u64 {
        self.last_block.load(Relaxed)
    }
    pub fn sealed_through(&self) -> u64 {
        self.sealed_through.load(Relaxed)
    }
    #[cfg(feature = "folds")]
    pub fn set_fold_checkpoint_lag_blocks(&self, v: u64) {
        self.fold_checkpoint_lag_blocks.store(v, Relaxed);
    }
    #[cfg(feature = "folds")]
    pub fn fold_checkpoint_lag_blocks(&self) -> u64 {
        self.fold_checkpoint_lag_blocks.load(Relaxed)
    }
    #[cfg(feature = "folds")]
    pub fn set_fold_writer_faulted(&self, v: bool) {
        self.fold_writer_faulted.store(v, Relaxed);
    }
    #[cfg(feature = "folds")]
    pub fn fold_writer_faulted(&self) -> bool {
        self.fold_writer_faulted.load(Relaxed)
    }
    /// Stamps `last_seal_progress` when the watermark actually advances (#1199).
    ///
    /// Only the seal-*direct* path used to touch that clock, so the tip path's watermark moved
    /// without it and a tip-path seal that had stopped had no clock to be judged against. On the two
    /// Graph protocol nests that meant `/ready` could not distinguish a seal 739,192 blocks behind
    /// from a healthy one. Same discipline as `set_seal_direct_completed`: only on a real advance,
    /// because re-reporting a block is not progress and must not refresh its own deadline.
    pub fn set_sealed_through(&self, v: u64) {
        if v > self.sealed_through.swap(v, Relaxed) {
            self.last_seal_progress.store(now_unix(), Relaxed);
        }
        METRICS.set_sealed_through(v);
    }
    pub fn set_indexed_from(&self, block: u64) {
        *self.indexed_from.lock().unwrap() = Some(block);
    }
    pub fn indexed_from(&self) -> Option<u64> {
        *self.indexed_from.lock().unwrap()
    }
    pub fn set_publish_enabled(&self, target: &str) {
        *self.publish_target.lock().unwrap() = Some(target.to_string());
        self.publish_enabled.store(true, Relaxed);
    }
    /// Where this nest mirrors to, `None` when it does not publish.
    pub fn publish_target(&self) -> Option<String> {
        self.publish_target.lock().unwrap().clone()
    }
    pub fn publish_sealed_through(&self) -> u64 {
        self.publish_sealed_through.load(Relaxed)
    }
    pub fn publish_lag_blocks(&self) -> u64 {
        self.publish_lag_blocks.load(Relaxed)
    }
    pub fn publish_last_success(&self) -> u64 {
        self.publish_last_success.load(Relaxed)
    }
    pub fn set_publish_lag_blocks(&self, blocks: u64) {
        self.publish_lag_blocks.store(blocks, Relaxed);
    }
    pub fn set_publish_pending(&self, segments: u64) {
        self.publish_pending.store(segments, Relaxed);
    }
    pub fn publish_uploaded(&self, bytes: u64) {
        let _ = self
            .publish_pending
            .fetch_update(Relaxed, Relaxed, |n| Some(n.saturating_sub(1)));
        self.publish_bytes.fetch_add(bytes, Relaxed);
    }
    pub fn publish_succeeded(&self, sealed_through: Option<u64>) {
        if let Some(block) = sealed_through {
            self.publish_sealed_through.store(block, Relaxed);
        }
        self.publish_pending.store(0, Relaxed);
        self.publish_lag_blocks.store(0, Relaxed);
        self.publish_dead_letter.store(false, Relaxed);
        self.publish_last_success.store(now_unix(), Relaxed);
    }
    pub fn publish_failed(&self, dead_letter: bool) {
        self.publish_errors.fetch_add(1, Relaxed);
        self.publish_dead_letter.store(dead_letter, Relaxed);
    }
    pub fn publish_errors(&self) -> u64 {
        self.publish_errors.load(Relaxed)
    }
    pub fn publish_bytes(&self) -> u64 {
        self.publish_bytes.load(Relaxed)
    }
    pub fn publish_dead_letter(&self) -> bool {
        self.publish_dead_letter.load(Relaxed)
    }
    pub fn add_rows_decoded(&self, n: u64) {
        self.rows_decoded.fetch_add(n, Relaxed);
        METRICS.add_rows_decoded(n);
    }
    pub fn add_rows_sealed(&self, n: u64) {
        self.rows_sealed.fetch_add(n, Relaxed);
        METRICS.add_rows_sealed(n);
    }
    pub fn inc_reorgs(&self) {
        self.reorgs.fetch_add(1, Relaxed);
        METRICS.inc_reorgs();
    }
    /// The count including this one, so the caller can rate-limit its warning.
    pub fn inc_checkpoints_missed(&self) -> u64 {
        METRICS.inc_checkpoints_missed();
        self.checkpoints_missed.fetch_add(1, Relaxed) + 1
    }
    pub fn checkpoints_missed(&self) -> u64 {
        self.checkpoints_missed.load(Relaxed)
    }
    pub fn add_ipfs_unreadable(&self, n: u64) {
        self.ipfs_unreadable.fetch_add(n, Relaxed);
        METRICS.add_ipfs_unreadable(n);
    }
    pub fn ipfs_unreadable(&self) -> u64 {
        self.ipfs_unreadable.load(Relaxed)
    }
    pub fn set_ipfs_pending(&self, n: u64) {
        self.ipfs_pending.store(n, Relaxed);
    }
    pub fn ipfs_pending(&self) -> u64 {
        self.ipfs_pending.load(Relaxed)
    }
    pub fn add_ipfs_resolved(&self, n: u64) {
        self.ipfs_resolved.fetch_add(n, Relaxed);
        METRICS.add_ipfs_resolved(n);
    }
    pub fn ipfs_resolved(&self) -> u64 {
        self.ipfs_resolved.load(Relaxed)
    }
    pub fn add_ipfs_given_up(&self, n: u64) {
        self.ipfs_given_up.fetch_add(n, Relaxed);
        METRICS.add_ipfs_given_up(n);
    }
    pub fn ipfs_given_up(&self) -> u64 {
        self.ipfs_given_up.load(Relaxed)
    }
    pub fn add_ipfs_unverified(&self, n: u64) {
        self.ipfs_unverified.fetch_add(n, Relaxed);
        METRICS.add_ipfs_unverified(n);
    }
    pub fn ipfs_unverified(&self) -> u64 {
        self.ipfs_unverified.load(Relaxed)
    }
    pub fn add_ipfs_oversize(&self, n: u64) {
        self.ipfs_oversize.fetch_add(n, Relaxed);
        METRICS.add_ipfs_oversize(n);
    }
    pub fn ipfs_oversize(&self) -> u64 {
        self.ipfs_oversize.load(Relaxed)
    }
    pub fn add_ipfs_rows_refused(&self, n: u64) {
        self.ipfs_rows_refused.fetch_add(n, Relaxed);
        METRICS.add_ipfs_rows_refused(n);
    }
    pub fn ipfs_rows_refused(&self) -> u64 {
        self.ipfs_rows_refused.load(Relaxed)
    }
    pub fn add_ipfs_retries(&self, n: u64) {
        self.ipfs_retries.fetch_add(n, Relaxed);
        METRICS.add_ipfs_retries(n);
    }
    pub fn ipfs_retries(&self) -> u64 {
        self.ipfs_retries.load(Relaxed)
    }
    pub fn set_seal_direct_concurrency(&self, requested: usize, effective: usize) {
        self.seal_direct_concurrency_requested
            .store(requested as u64, Relaxed);
        self.seal_direct_concurrency
            .store(effective as u64, Relaxed);
        METRICS.set_seal_direct_concurrency(requested, effective);
    }
    /// `(requested, effective)`, or `None` before a seal-direct nest has recorded them.
    pub fn seal_direct_concurrency(&self) -> Option<(u64, u64)> {
        let requested = self.seal_direct_concurrency_requested.load(Relaxed);
        (requested != 0).then(|| (requested, self.seal_direct_concurrency.load(Relaxed)))
    }
    pub fn set_ipfs_window_deadline(&self, deadline: std::time::Duration) {
        self.ipfs_window_deadline
            .store(deadline.as_secs().saturating_add(1), Relaxed);
        METRICS.set_ipfs_window_deadline(deadline);
    }
    /// Seconds, `0` for no deadline, or `None` before a nest has recorded it.
    pub fn ipfs_window_deadline(&self) -> Option<u64> {
        self.ipfs_window_deadline.load(Relaxed).checked_sub(1)
    }

    /// Start a seal-direct history pass. `/ready` reads these instead of treating a zero cursor as
    /// WAITING (#807).
    pub fn begin_seal_direct(&self, origin: u64, target: u64) {
        self.seal_direct_active.store(true, Relaxed);
        self.seal_direct_origin.store(origin, Relaxed);
        self.seal_direct_completed
            .store(origin.saturating_sub(1), Relaxed);
        self.seal_direct_target.store(target, Relaxed);
        self.seal_direct_fetched
            .store(origin.saturating_sub(1), Relaxed);
        self.last_seal_progress.store(now_unix(), Relaxed);
        METRICS.begin_seal_direct(origin, target);
    }
    /// Stamps `last_seal_progress` only when `block` actually advances the watermark, mirroring
    /// [`NestMetrics::set_last_block`]. A pass that keeps reporting the same block is not making
    /// progress and must not refresh its own clock by saying so (#846).
    pub fn set_seal_direct_completed(&self, block: u64) {
        if block > self.seal_direct_completed.swap(block, Relaxed) {
            self.last_seal_progress.store(now_unix(), Relaxed);
        }
        METRICS.set_seal_direct_completed(block);
    }
    /// The fetch position of the seal-direct pass (#1169). Advancing it is progress for the pass's
    /// liveness clock - a pass that is fetching is alive even when a sparse range has not yet filled
    /// a segment - but it is *not* the watermark, and `/ready` reports the two side by side.
    pub fn set_seal_direct_fetched(&self, block: u64) {
        // A high-water mark, never a last-seen: `fetch_max` so a tick for an older block - two
        // callers, or a pipeline that stops yielding in order - cannot walk it backwards.
        if block > self.seal_direct_fetched.fetch_max(block, Relaxed) {
            self.last_seal_progress.store(now_unix(), Relaxed);
        }
        METRICS.set_seal_direct_fetched(block);
    }
    pub fn seal_direct_fetched(&self) -> u64 {
        self.seal_direct_fetched.load(Relaxed)
    }
    /// The block span the cursor is currently asking for (#1170).
    pub fn set_fetch_window(&self, blocks: u64) {
        self.fetch_window.store(blocks, Relaxed);
        METRICS.set_fetch_window(blocks);
    }
    pub fn fetch_window(&self) -> u64 {
        self.fetch_window.load(Relaxed)
    }
    pub fn last_seal_progress(&self) -> u64 {
        self.last_seal_progress.load(Relaxed)
    }
    /// Test seam, mirroring the other clock seams: pin the seal clock so a test can simulate "sealed
    /// once, then froze" without waiting out a real threshold.
    #[cfg(test)]
    pub fn set_last_seal_progress_for_test(&self, t: u64) {
        self.last_seal_progress.store(t, Relaxed);
    }
    /// The outcome of one tip-path seal attempt: its error, or `None` for one that did not fail.
    /// A run of failures keeps the time of its first.
    pub fn record_seal(&self, error: Option<String>) {
        let mut fault = self.seal_fault.lock().unwrap();
        *fault = error.map(|e| (fault.as_ref().map_or_else(now_unix, |(since, _)| *since), e));
    }
    /// `(since_unixtime, reason)` while the seal keeps failing.
    pub fn seal_fault(&self) -> Option<(u64, String)> {
        self.seal_fault.lock().unwrap().clone()
    }
    #[cfg(test)]
    pub fn set_seal_fault_since_for_test(&self, t: u64) {
        if let Some((since, _)) = self.seal_fault.lock().unwrap().as_mut() {
            *since = t;
        }
    }
    /// Handoff to tip-following: inactive, but the completed target stays (#807).
    pub fn end_seal_direct(&self) {
        self.seal_direct_active.store(false, Relaxed);
        let target = self.seal_direct_target.load(Relaxed);
        self.seal_direct_completed.store(target, Relaxed);
        self.seal_direct_fetched.store(target, Relaxed);
        METRICS.end_seal_direct();
    }
    pub fn seal_direct_active(&self) -> bool {
        self.seal_direct_active.load(Relaxed)
    }
    pub fn seal_direct_origin(&self) -> u64 {
        self.seal_direct_origin.load(Relaxed)
    }
    pub fn seal_direct_completed(&self) -> u64 {
        self.seal_direct_completed.load(Relaxed)
    }
    pub fn seal_direct_target(&self) -> u64 {
        self.seal_direct_target.load(Relaxed)
    }

    pub fn set_storage_paths(&self, hot: PathBuf, sealed: PathBuf) {
        *self.storage.lock().unwrap() = Some((hot, sealed));
        self.storage_scanned_at.store(0, Relaxed);
    }

    pub fn set_dataset_dir(&self, dir: PathBuf) {
        *self.dataset.lock().unwrap() = Some(dir);
        self.storage_scanned_at.store(0, Relaxed);
    }

    /// The segment store this nest seals into, which a runtime's nests share.
    fn sealed_store(&self) -> Option<PathBuf> {
        self.storage
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, s)| s.clone())
    }

    /// Hot-store bytes and the bytes of the segments this nest's manifest names. A segment another
    /// dataset also names is counted under both, so a sum over nests is not disk used.
    pub fn dataset_storage_bytes(&self) -> (u64, u64) {
        let (hot, store) = self.storage_bytes();
        if self.dataset.lock().unwrap().is_none() {
            return (hot, store);
        }
        (hot, self.dataset_sealed_bytes.load(Relaxed))
    }
    pub fn storage_bytes(&self) -> (u64, u64) {
        const STORAGE_REFRESH_SECS: u64 = 60;
        let now = now_unix();
        let scanned = self.storage_scanned_at.load(Relaxed);
        if now.saturating_sub(scanned) >= STORAGE_REFRESH_SECS
            && self
                .storage_scanned_at
                .compare_exchange(scanned, now, Relaxed, Relaxed)
                .is_ok()
        {
            if let Some((hot, sealed)) = self.storage.lock().unwrap().as_ref() {
                self.hot_bytes.store(path_bytes(hot), Relaxed);
                self.sealed_bytes.store(path_bytes(sealed), Relaxed);
            }
            if let Some(dir) = self.dataset.lock().unwrap().as_ref() {
                self.dataset_sealed_bytes
                    .store(manifest_segment_bytes(dir), Relaxed);
            }
        }
        (
            self.hot_bytes.load(Relaxed),
            self.sealed_bytes.load(Relaxed),
        )
    }
}

pub struct Metrics {
    // Ingestion - the "is it keeping up?" signals an operator alerts on.
    tip_height: AtomicU64,
    ceiling: AtomicU64,
    last_block: AtomicU64,
    sealed_through: AtomicU64,
    /// Unix seconds of the last *successful* source poll (tip fetch). `0` = never polled yet. The
    /// readiness signal: if now − this exceeds the stall threshold, every RPC endpoint is unreachable
    /// and indexing has stalled (as opposed to "caught up and idle", which keeps this fresh).
    last_poll_ok: AtomicU64,
    /// See [`NestMetrics::poll_failed`]. The solo `dev` readiness path reads this global value.
    poll_failed: AtomicBool,
    /// Unix seconds the first nest was built, i.e. process/runtime start. `0` until set. See
    /// [`NestMetrics::started_at`] - a solo `dev` reads this one, since it has no per-nest health.
    started_at: AtomicU64,
    /// Global counterpart of [`NestMetrics::last_progress`], read by a solo `dev` (no per-nest health).
    last_progress: AtomicU64,
    rows_decoded: AtomicU64,
    rows_sealed: AtomicU64,
    reorgs: AtomicU64,
    checkpoints_missed: AtomicU64,
    ipfs_unreadable: AtomicU64,
    ipfs_resolved: AtomicU64,
    ipfs_given_up: AtomicU64,
    ipfs_unverified: AtomicU64,
    ipfs_oversize: AtomicU64,
    ipfs_rows_refused: AtomicU64,
    ipfs_retries: AtomicU64,
    seal_direct_concurrency_requested: AtomicU64,
    seal_direct_concurrency: AtomicU64,
    ipfs_window_deadline: AtomicU64,
    alert_outbox_depth: AtomicU64,
    audit_on: AtomicBool,
    audit_ranges: AtomicU64,
    audit_mismatches: AtomicU64,
    audit_errors: AtomicU64,
    // Serving - the surface an operator bills against.
    http_requests: AtomicU64,
    sql_queries: AtomicU64,
    /// Backward-compatible aggregate of every member of `sql_rejections_by_reason`.
    sql_rejections: AtomicU64,
    sql_rejections_by_reason: [AtomicU64; 7],
    /// Cumulative per [`NAMED_SCAN_BOUNDS`] entry, then `+Inf`.
    named_scan_buckets: [AtomicU64; 7],
    named_scan_bytes_sum: AtomicU64,
    named_scan_refusals: AtomicU64,
    rpc_requests: AtomicU64,
    rpc_methods: Mutex<BTreeMap<String, u64>>,
    /// #807: process-global copy of the seal-direct pass, read by a solo `dev` `/ready`.
    seal_direct_active: AtomicBool,
    seal_direct_origin: AtomicU64,
    seal_direct_completed: AtomicU64,
    seal_direct_target: AtomicU64,
    /// The seal-direct pass's *fetch* position (#1169): the highest block whose logs have been fetched
    /// and decoded into the seal buffer. Distinct from `seal_direct_completed`, which is the durable
    /// watermark a restart resumes from; the gap between them is work a restart redoes.
    seal_direct_fetched: AtomicU64,
    /// The block span the cursor last asked `eth_getLogs` for (#1170), so a window that a rate-limited
    /// hour collapsed to ten blocks is visible before the backfill's ETA is.
    fetch_window: AtomicU64,
    /// #846, the solo-runtime copy of the seal clock. See `NestMetrics::last_seal_progress`.
    last_seal_progress: AtomicU64,
    /// #812: per-endpoint RPC health. Keyed by a host[:port] label, never a URL.
    rpc_endpoints: Mutex<BTreeMap<String, EndpointStats>>,
    /// Per-nest handles, keyed by nest name (SEC-9). `BTreeMap` so `/metrics` renders in a stable
    /// order. Populated once per nest at build; a solo `dev` has a single entry.
    per_nest: Mutex<BTreeMap<String, Arc<NestMetrics>>>,
}

/// Prometheus histogram bounds for RPC duration (seconds). The `+Inf` bucket is implicit.
const RPC_DURATION_BOUNDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Histogram bounds, in source bytes, for declared queries admitted under the scan cap (RFC-0048).
/// Spaced so a spread of more than two orders of magnitude, Phase 0's exit benchmark, is legible.
const NAMED_SCAN_BOUNDS: [u64; 6] = [4 << 10, 64 << 10, 1 << 20, 16 << 20, 256 << 20, 512 << 20];

#[derive(Default)]
struct EndpointStats {
    requests: u64,
    failures: u64,
    retries: u64,
    duration_sum: f64,
    duration_count: u64,
    /// Cumulative counts, one per [`RPC_DURATION_BOUNDS`] entry, then `+Inf`.
    buckets: [u64; 12],
}

impl Metrics {
    const fn new() -> Self {
        Self {
            tip_height: AtomicU64::new(0),
            ceiling: AtomicU64::new(0),
            last_block: AtomicU64::new(0),
            sealed_through: AtomicU64::new(0),
            last_poll_ok: AtomicU64::new(0),
            poll_failed: AtomicBool::new(false),
            started_at: AtomicU64::new(0),
            last_progress: AtomicU64::new(0),
            rows_decoded: AtomicU64::new(0),
            rows_sealed: AtomicU64::new(0),
            reorgs: AtomicU64::new(0),
            checkpoints_missed: AtomicU64::new(0),
            ipfs_unreadable: AtomicU64::new(0),
            ipfs_resolved: AtomicU64::new(0),
            ipfs_given_up: AtomicU64::new(0),
            ipfs_unverified: AtomicU64::new(0),
            ipfs_oversize: AtomicU64::new(0),
            ipfs_rows_refused: AtomicU64::new(0),
            ipfs_retries: AtomicU64::new(0),
            seal_direct_concurrency_requested: AtomicU64::new(0),
            seal_direct_concurrency: AtomicU64::new(0),
            ipfs_window_deadline: AtomicU64::new(0),
            alert_outbox_depth: AtomicU64::new(0),
            audit_on: AtomicBool::new(false),
            audit_ranges: AtomicU64::new(0),
            audit_mismatches: AtomicU64::new(0),
            audit_errors: AtomicU64::new(0),
            http_requests: AtomicU64::new(0),
            sql_queries: AtomicU64::new(0),
            sql_rejections: AtomicU64::new(0),
            sql_rejections_by_reason: [const { AtomicU64::new(0) }; 7],
            named_scan_buckets: [const { AtomicU64::new(0) }; 7],
            named_scan_bytes_sum: AtomicU64::new(0),
            named_scan_refusals: AtomicU64::new(0),
            rpc_requests: AtomicU64::new(0),
            rpc_methods: Mutex::new(BTreeMap::new()),
            seal_direct_active: AtomicBool::new(false),
            seal_direct_origin: AtomicU64::new(0),
            seal_direct_completed: AtomicU64::new(0),
            seal_direct_target: AtomicU64::new(0),
            seal_direct_fetched: AtomicU64::new(0),
            fetch_window: AtomicU64::new(0),
            last_seal_progress: AtomicU64::new(0),
            rpc_endpoints: Mutex::new(BTreeMap::new()),
            per_nest: Mutex::new(BTreeMap::new()),
        }
    }

    /// Get (or create) the per-nest metrics handle for `name`. Called once per nest at build; the
    /// returned handle is cheap to clone and its updates also feed the process-global aggregates.
    pub fn nest(&self, name: &str) -> Arc<NestMetrics> {
        let mut map = self.per_nest.lock().unwrap();
        if let Some(h) = map.get(name) {
            return h.clone();
        }
        let h = Arc::new(NestMetrics::default());
        map.insert(name.to_string(), h.clone());
        h
    }

    /// [`Metrics::nest`] without registering a nest nobody has reported under.
    pub fn nest_if_known(&self, name: &str) -> Option<Arc<NestMetrics>> {
        self.per_nest.lock().unwrap().get(name).cloned()
    }

    /// Remove a departed nest's series and its storage paths. Its handle may live briefly in an
    /// in-flight request, but it is no longer reachable from `/metrics` (#827).
    pub fn remove_nest(&self, name: &str) {
        self.per_nest.lock().unwrap().remove(name);
    }

    /// Carry a nest's series over to a new name (#1549), replacing any the new name had.
    pub fn rename_nest(&self, from: &str, to: &str) {
        let mut map = self.per_nest.lock().unwrap();
        if let Some(h) = map.remove(from) {
            map.insert(to.to_string(), h);
        }
    }

    pub fn set_tip(&self, v: u64) {
        self.tip_height.store(v, Relaxed);
    }
    pub fn set_ceiling(&self, v: u64) {
        self.ceiling.store(v, Relaxed);
    }
    pub fn ceiling(&self) -> u64 {
        self.ceiling.load(Relaxed)
    }
    pub fn set_last_block(&self, v: u64) {
        if self.last_block.swap(v, Relaxed) != v {
            self.last_progress.store(now_unix(), Relaxed);
        }
    }
    pub fn last_progress(&self) -> u64 {
        self.last_progress.load(Relaxed)
    }
    /// Test seam, mirroring the per-nest one.
    #[cfg(test)]
    pub fn set_last_progress_for_test(&self, t: u64) {
        self.last_progress.store(t, Relaxed);
    }
    /// The solo-runtime copy. Stamps the seal clock on a real advance, exactly as the per-nest
    /// handle does (#1199).
    pub fn set_sealed_through(&self, v: u64) {
        if v > self.sealed_through.swap(v, Relaxed) {
            self.last_seal_progress.store(now_unix(), Relaxed);
        }
    }
    /// Record a successful source poll - call it on every tip fetch that returns (the tip loop does),
    /// so readiness reflects "we can still reach the chain", independent of whether we're behind.
    pub fn mark_poll_ok(&self) {
        self.last_poll_ok.store(now_unix(), Relaxed);
        self.poll_failed.store(false, Relaxed);
    }
    /// Unix seconds of the last successful poll (`0` = never). Read by the readiness endpoint.
    pub fn last_poll_ok(&self) -> u64 {
        self.last_poll_ok.load(Relaxed)
    }
    pub fn mark_poll_failed(&self) {
        self.poll_failed.store(true, Relaxed);
    }
    pub fn poll_failed(&self) -> bool {
        self.poll_failed.load(Relaxed)
    }
    /// First-wins stamp of process/runtime start, called from [`NestMetrics::mark_started`]. Only the
    /// first caller sets it - later nests built in the same startup burst must not push it forward.
    fn mark_started_once(&self, t: u64) {
        let _ = self.started_at.compare_exchange(0, t, Relaxed, Relaxed);
    }
    /// Unix seconds of process/runtime start (`0` = not yet stamped). Read by the readiness endpoint.
    pub fn started_at(&self) -> u64 {
        self.started_at.load(Relaxed)
    }
    /// Test seam, mirroring the per-nest one.
    #[cfg(test)]
    pub fn set_started_at_for_test(&self, t: u64) {
        self.started_at.store(t, Relaxed);
    }
    // Getters for the readiness endpoint (the setters already exist for the ingest loop).
    pub fn tip_height(&self) -> u64 {
        self.tip_height.load(Relaxed)
    }
    pub fn last_block(&self) -> u64 {
        self.last_block.load(Relaxed)
    }
    pub fn sealed_through_val(&self) -> u64 {
        self.sealed_through.load(Relaxed)
    }
    pub fn add_rows_decoded(&self, n: u64) {
        self.rows_decoded.fetch_add(n, Relaxed);
    }
    pub fn add_rows_sealed(&self, n: u64) {
        self.rows_sealed.fetch_add(n, Relaxed);
    }
    pub fn inc_reorgs(&self) {
        self.reorgs.fetch_add(1, Relaxed);
    }
    pub fn inc_checkpoints_missed(&self) {
        self.checkpoints_missed.fetch_add(1, Relaxed);
    }
    pub fn add_ipfs_unreadable(&self, n: u64) {
        self.ipfs_unreadable.fetch_add(n, Relaxed);
    }
    pub fn add_ipfs_resolved(&self, n: u64) {
        self.ipfs_resolved.fetch_add(n, Relaxed);
    }
    pub fn add_ipfs_given_up(&self, n: u64) {
        self.ipfs_given_up.fetch_add(n, Relaxed);
    }
    pub fn add_ipfs_unverified(&self, n: u64) {
        self.ipfs_unverified.fetch_add(n, Relaxed);
    }
    pub fn add_ipfs_oversize(&self, n: u64) {
        self.ipfs_oversize.fetch_add(n, Relaxed);
    }
    pub fn add_ipfs_rows_refused(&self, n: u64) {
        self.ipfs_rows_refused.fetch_add(n, Relaxed);
    }
    pub fn add_ipfs_retries(&self, n: u64) {
        self.ipfs_retries.fetch_add(n, Relaxed);
    }
    pub fn set_seal_direct_concurrency(&self, requested: usize, effective: usize) {
        self.seal_direct_concurrency_requested
            .store(requested as u64, Relaxed);
        self.seal_direct_concurrency
            .store(effective as u64, Relaxed);
    }
    pub fn seal_direct_concurrency(&self) -> Option<(u64, u64)> {
        let requested = self.seal_direct_concurrency_requested.load(Relaxed);
        (requested != 0).then(|| (requested, self.seal_direct_concurrency.load(Relaxed)))
    }
    pub fn set_ipfs_window_deadline(&self, deadline: std::time::Duration) {
        self.ipfs_window_deadline
            .store(deadline.as_secs().saturating_add(1), Relaxed);
    }
    pub fn ipfs_window_deadline(&self) -> Option<u64> {
        self.ipfs_window_deadline.load(Relaxed).checked_sub(1)
    }
    pub fn set_alert_outbox(&self, v: u64) {
        self.alert_outbox_depth.store(v, Relaxed);
    }
    pub fn set_audit_on(&self) {
        self.audit_on.store(true, Relaxed);
    }
    pub fn inc_audit_ranges(&self) {
        self.audit_ranges.fetch_add(1, Relaxed);
    }
    pub fn add_audit_mismatches(&self, n: u64) {
        self.audit_mismatches.fetch_add(n, Relaxed);
    }
    pub fn inc_audit_errors(&self) {
        self.audit_errors.fetch_add(1, Relaxed);
    }
    pub fn inc_http(&self) {
        self.http_requests.fetch_add(1, Relaxed);
    }
    pub fn inc_sql(&self) {
        self.sql_queries.fetch_add(1, Relaxed);
    }
    pub fn inc_sql_rejected(&self, reason: SqlRejection) {
        self.sql_rejections.fetch_add(1, Relaxed);
        self.sql_rejections_by_reason[reason.index()].fetch_add(1, Relaxed);
    }
    /// A declared query answered under the scan cap, with the source bytes it was admitted at.
    pub fn observe_named_scan(&self, bytes: u64) {
        self.named_scan_bytes_sum.fetch_add(bytes, Relaxed);
        for (i, bound) in NAMED_SCAN_BOUNDS.iter().enumerate() {
            if bytes <= *bound {
                self.named_scan_buckets[i].fetch_add(1, Relaxed);
            }
        }
        self.named_scan_buckets[NAMED_SCAN_BOUNDS.len()].fetch_add(1, Relaxed);
    }
    pub fn inc_named_scan_refused(&self) {
        self.named_scan_refusals.fetch_add(1, Relaxed);
    }
    pub fn inc_rpc(&self) {
        self.rpc_requests.fetch_add(1, Relaxed);
    }
    pub fn inc_rpc_method(&self, method: &str) {
        self.inc_rpc_methods(method, 1);
    }
    pub fn inc_rpc_methods(&self, method: &str, count: u64) {
        if count == 0 || method.is_empty() {
            return;
        }
        let mut map = self.rpc_methods.lock().unwrap();
        *map.entry(method.to_string()).or_insert(0) += count;
    }

    pub fn begin_seal_direct(&self, origin: u64, target: u64) {
        self.seal_direct_active.store(true, Relaxed);
        self.seal_direct_origin.store(origin, Relaxed);
        self.seal_direct_completed
            .store(origin.saturating_sub(1), Relaxed);
        self.seal_direct_target.store(target, Relaxed);
        self.seal_direct_fetched
            .store(origin.saturating_sub(1), Relaxed);
        self.last_seal_progress.store(now_unix(), Relaxed);
    }
    pub fn set_seal_direct_completed(&self, block: u64) {
        if block > self.seal_direct_completed.swap(block, Relaxed) {
            self.last_seal_progress.store(now_unix(), Relaxed);
        }
    }
    pub fn set_seal_direct_fetched(&self, block: u64) {
        if block > self.seal_direct_fetched.fetch_max(block, Relaxed) {
            self.last_seal_progress.store(now_unix(), Relaxed);
        }
    }
    pub fn seal_direct_fetched(&self) -> u64 {
        self.seal_direct_fetched.load(Relaxed)
    }
    pub fn set_fetch_window(&self, blocks: u64) {
        self.fetch_window.store(blocks, Relaxed);
    }
    pub fn fetch_window(&self) -> u64 {
        self.fetch_window.load(Relaxed)
    }
    pub fn last_seal_progress(&self) -> u64 {
        self.last_seal_progress.load(Relaxed)
    }
    #[cfg(test)]
    pub fn set_last_seal_progress_for_test(&self, t: u64) {
        self.last_seal_progress.store(t, Relaxed);
    }
    pub fn end_seal_direct(&self) {
        self.seal_direct_active.store(false, Relaxed);
        let target = self.seal_direct_target.load(Relaxed);
        self.seal_direct_completed.store(target, Relaxed);
        self.seal_direct_fetched.store(target, Relaxed);
    }
    pub fn seal_direct_active(&self) -> bool {
        self.seal_direct_active.load(Relaxed)
    }
    pub fn seal_direct_origin(&self) -> u64 {
        self.seal_direct_origin.load(Relaxed)
    }
    pub fn seal_direct_completed(&self) -> u64 {
        self.seal_direct_completed.load(Relaxed)
    }
    pub fn seal_direct_target(&self) -> u64 {
        self.seal_direct_target.load(Relaxed)
    }

    /// Record one RPC attempt against a bounded endpoint label (#812). `retry` is true when this
    /// is not the first endpoint tried for the call.
    pub fn observe_rpc(&self, endpoint: &str, duration: Duration, failed: bool, retry: bool) {
        if endpoint.is_empty() {
            return;
        }
        let secs = duration.as_secs_f64();
        let mut map = self.rpc_endpoints.lock().unwrap();
        let st = map.entry(endpoint.to_string()).or_default();
        st.requests += 1;
        if failed {
            st.failures += 1;
        }
        if retry {
            st.retries += 1;
        }
        st.duration_sum += secs;
        st.duration_count += 1;
        for (i, bound) in RPC_DURATION_BOUNDS.iter().enumerate() {
            if secs <= *bound {
                st.buckets[i] += 1;
            }
        }
        st.buckets[RPC_DURATION_BOUNDS.len()] += 1; // +Inf
    }
    /// Test seam for the solo `/ready` path, which reads these process-global fields rather than
    /// a per-nest handle.
    #[cfg(test)]
    pub fn reset_readiness_for_test(&self) {
        self.last_poll_ok.store(0, Relaxed);
        self.poll_failed.store(false, Relaxed);
        self.started_at.store(0, Relaxed);
    }

    /// Render the registry as Prometheus text (`text/plain; version=0.0.4`).
    pub fn render(&self) -> String {
        // Heights and watermarks have no meaningful cross-chain aggregate. In a runtime with more
        // than one mounted nest, expose only the labelled series below rather than letting the last
        // cursor to poll impersonate the whole process (#828).
        let (solo, multi_nest) = {
            let per = self.per_nest.lock().unwrap();
            match per.len() {
                0 => (None, false),
                1 => (Some(per.values().next().unwrap().clone()), false),
                _ => (None, true),
            }
        };
        let tip = solo
            .as_ref()
            .map(|m| m.tip.load(Relaxed))
            .unwrap_or_else(|| self.tip_height.load(Relaxed));
        let last = solo
            .as_ref()
            .map(|m| m.last_block.load(Relaxed))
            .unwrap_or_else(|| self.last_block.load(Relaxed));
        let sealed_through = solo
            .as_ref()
            .map(|m| m.sealed_through.load(Relaxed))
            .unwrap_or_else(|| self.sealed_through.load(Relaxed));
        let last_poll_ok = solo
            .as_ref()
            .map(|m| m.last_poll_ok.load(Relaxed))
            .unwrap_or_else(|| self.last_poll_ok.load(Relaxed));
        // Blocks behind the tip. 0 (not negative) once caught up.
        let lag = tip.saturating_sub(last);
        let rss = rss_bytes();

        let gauge = |name: &str, help: &str, v: u64| {
            format!("# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n")
        };
        let counter = |name: &str, help: &str, v: u64| {
            format!("# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n")
        };

        let mut s = String::with_capacity(2048);
        if !multi_nest {
            s.push_str(&gauge(
                "nuthatch_tip_height",
                "Latest block height seen from the source (solo runtime only; use nuthatch_nest_tip_height in a multi-nest runtime).",
                tip,
            ));
            s.push_str(&gauge(
                "nuthatch_last_block",
                "Highest block the indexer has committed (solo runtime only; use nuthatch_nest_last_block in a multi-nest runtime).",
                last,
            ));
            s.push_str(&gauge(
                "nuthatch_tip_lag_blocks",
                "Blocks the indexer is behind the source tip (solo runtime only; use nuthatch_nest_tip_lag_blocks in a multi-nest runtime).",
                lag,
            ));
            s.push_str(&gauge(
                "nuthatch_sealed_through",
                "Highest block sealed to the immutable cold layer (solo runtime only).",
                sealed_through,
            ));
            #[cfg(feature = "folds")]
            if let Some(m) = solo.as_ref() {
                s.push_str(&gauge(
                    "nuthatch_fold_checkpoint_lag_blocks",
                    "Sealed blocks the folds are not yet checkpointed through (solo runtime only).",
                    m.fold_checkpoint_lag_blocks(),
                ));
                s.push_str(&gauge(
                    "nuthatch_fold_writer_faulted",
                    "1 when the fold checkpoint writer has stopped on an error (solo runtime only).",
                    u64::from(m.fold_writer_faulted()),
                ));
            }
        }
        s.push_str(&gauge(
            "nuthatch_rss_bytes",
            "Resident set size of this process, in bytes.",
            rss,
        ));
        let pools = crate::analytics::engine().pools();
        s.push_str(&gauge(
            "nuthatch_analytics_pool_reserved_bytes",
            "Bytes the statements in flight hold against the analytics engines' memory pools, each nest's shared pool counted once.",
            pools.total.reserved,
        ));
        s.push_str(&gauge(
            "nuthatch_analytics_pool_peak_bytes",
            "The most any one analytics engine's statements have held since start.",
            pools.total.peak,
        ));
        s.push_str(&gauge(
            "nuthatch_analytics_engines",
            "Live analytics engines. A nest's engines share one pool, bounded by NUTHATCH_BURRMILL_MEMORY_LIMIT.",
            pools.total.engines,
        ));
        #[cfg(all(target_os = "linux", not(target_env = "musl")))]
        if let Some(stats) = jemalloc_stats() {
            for (name, help, v) in [
                ("nuthatch_jemalloc_allocated_bytes", "Bytes the program holds allocated through jemalloc (stats.allocated).", stats.allocated),
                ("nuthatch_jemalloc_active_bytes", "Bytes in pages jemalloc has handed out to allocations (stats.active).", stats.active),
                ("nuthatch_jemalloc_resident_bytes", "Bytes in pages jemalloc has resident, its metadata included (stats.resident).", stats.resident),
                ("nuthatch_jemalloc_retained_bytes", "Bytes jemalloc keeps mapped but has returned to the OS, so not in RSS (stats.retained).", stats.retained),
            ] {
                s.push_str(&gauge(name, help, v));
            }
        }
        if !multi_nest {
            s.push_str(&gauge(
                "nuthatch_last_poll_unixtime",
                "Unix time of the last successful source poll (0 = never; solo runtime only).",
                last_poll_ok,
            ));
        }
        s.push_str(&gauge(
            "nuthatch_alert_outbox_depth",
            "Pending alert-webhook deliveries in the durable outbox.",
            self.alert_outbox_depth.load(Relaxed),
        ));
        // Only where an audit runs, so an alert on the ranges counter standing still needs no list
        // of which units were started with `--audit-rpc`.
        if self.audit_on.load(Relaxed) {
            s.push_str(&counter(
                "nuthatch_audit_ranges_total",
                "Sealed ranges re-fetched from the audit endpoint and compared, since start.",
                self.audit_ranges.load(Relaxed),
            ));
            s.push_str(&counter(
                "nuthatch_audit_mismatches_total",
                "Rows that differed between sealed segments and the audit endpoint, since start.",
                self.audit_mismatches.load(Relaxed),
            ));
            s.push_str(&counter(
                "nuthatch_audit_errors_total",
                "Audit samples that could not be completed, since start.",
                self.audit_errors.load(Relaxed),
            ));
        }
        s.push_str(&counter(
            "nuthatch_rows_decoded_total",
            "Rows decoded since start.",
            self.rows_decoded.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_rows_sealed_total",
            "Rows sealed to Parquet since start.",
            self.rows_sealed.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_reorgs_total",
            "Reorgs detected and rolled back since start.",
            self.reorgs.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_checkpoints_missed_total",
            "Windows refused because the source gave no hash for the boundary block, since start.",
            self.checkpoints_missed.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_ipfs_unreadable_total",
            "Rows an [[ipfs]] declaration read that named no usable CID, since start.",
            self.ipfs_unreadable.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_ipfs_resolved_total",
            "Documents an [[ipfs]] declaration named that were fetched and stored, since start.",
            self.ipfs_resolved.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_ipfs_given_up_total",
            "Documents given up on after every retry failed, since start. Their ranges seal without them.",
            self.ipfs_given_up.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_ipfs_unverified_total",
            "IPFS documents fetched that could not be proven against their CID, since start.",
            self.ipfs_unverified.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_ipfs_oversize_total",
            "IPFS documents refused for exceeding the byte, block or depth cap, since start.",
            self.ipfs_oversize.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_ipfs_rows_refused_total",
            "Proven IPFS documents whose content did not fit their declared typed rows, since start.",
            self.ipfs_rows_refused.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_ipfs_retries_total",
            "Failed IPFS document fetches that were tried again, since start.",
            self.ipfs_retries.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_http_requests_total",
            "HTTP API requests served since start.",
            self.http_requests.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_sql_queries_total",
            "Analytical /sql queries accepted since start.",
            self.sql_queries.load(Relaxed),
        ));
        s.push_str(
            "# HELP nuthatch_sql_rejections_total Analytical /sql requests refused since start; the unlabelled series is the aggregate and reason is a fixed refusal classification.\n\
             # TYPE nuthatch_sql_rejections_total counter\n",
        );
        s.push_str(&format!(
            "nuthatch_sql_rejections_total {}\n",
            self.sql_rejections.load(Relaxed)
        ));
        for reason in SqlRejection::ALL {
            s.push_str(&format!(
                "nuthatch_sql_rejections_total{{reason=\"{}\"}} {}\n",
                reason.label(),
                self.sql_rejections_by_reason[reason.index()].load(Relaxed),
            ));
        }
        s.push_str(
            "# HELP nuthatch_named_scan_bytes Source-byte bound declared queries were answered under (RFC-0048).\n\
             # TYPE nuthatch_named_scan_bytes histogram\n",
        );
        for (i, bound) in NAMED_SCAN_BOUNDS.iter().enumerate() {
            s.push_str(&format!(
                "nuthatch_named_scan_bytes_bucket{{le=\"{bound}\"}} {}\n",
                self.named_scan_buckets[i].load(Relaxed)
            ));
        }
        let admitted = self.named_scan_buckets[NAMED_SCAN_BOUNDS.len()].load(Relaxed);
        s.push_str(&format!(
            "nuthatch_named_scan_bytes_bucket{{le=\"+Inf\"}} {admitted}\n\
             nuthatch_named_scan_bytes_sum {}\n\
             nuthatch_named_scan_bytes_count {admitted}\n",
            self.named_scan_bytes_sum.load(Relaxed)
        ));
        s.push_str(&counter(
            "nuthatch_named_scan_refusals_total",
            "Declared queries refused by scan admission: over the byte cap, unboundable, or a stale quote.",
            self.named_scan_refusals.load(Relaxed),
        ));
        s.push_str(&counter(
            "nuthatch_sql_memo_hits_total",
            "Analytical /sql queries answered from the deterministic memo (#1186).",
            crate::sqlmemo::hits(),
        ));
        s.push_str(&counter(
            "nuthatch_sql_memo_misses_total",
            "Analytical /sql queries that computed because no remembered answer matched their inputs.",
            crate::sqlmemo::misses(),
        ));
        s.push_str(&gauge(
            "nuthatch_sql_memo_bytes",
            "Serialized rows the memo currently holds, bounded by NUTHATCH_SQL_MEMO_BYTES.",
            crate::sqlmemo::bytes() as u64,
        ));
        s.push_str(&counter(
            "nuthatch_rpc_requests_total",
            "Outbound HTTP POSTs to JSON-RPC endpoints (one per request or batch envelope, including failover retries). Not a method count; see nuthatch_rpc_methods_total.",
            self.rpc_requests.load(Relaxed),
        ));

        // Always advertised, even at zero observations: an operator pricing a just-started nest
        // against a provider CU schedule needs the series to exist before the first batch lands.
        s.push_str("# HELP nuthatch_rpc_methods_total Individual JSON-RPC method invocations. A batch of 200 eth_getBlockByNumber counts 200 here and 1 on nuthatch_rpc_requests_total. Multiply by a provider's per-method CU schedule to estimate a bill.\n# TYPE nuthatch_rpc_methods_total counter\n");
        let methods = self.rpc_methods.lock().unwrap();
        for (method, count) in methods.iter() {
            s.push_str(&format!(
                "nuthatch_rpc_methods_total{{method=\"{method}\"}} {count}\n"
            ));
        }
        drop(methods);

        s.push_str(&format!(
            "# HELP nuthatch_process_cpu_seconds_total Process CPU time (user + system) in seconds. A counter; clients derive a rolling percentage against wall time.\n\
             # TYPE nuthatch_process_cpu_seconds_total counter\n\
             nuthatch_process_cpu_seconds_total {:.6}\n",
            process_cpu_seconds()
        ));

        // Absent until a cursor is built, so a serve-only process never reports one.
        let started = self.started_at.load(Relaxed);
        if started != 0 {
            s.push_str(&format!(
                "# HELP nuthatch_start_time_seconds Unix time this runtime built its first cursor. The counters above are since then; divide by the elapsed time for a rate.\n\
                 # TYPE nuthatch_start_time_seconds gauge\n\
                 nuthatch_start_time_seconds {started}\n"
            ));
        }

        let (hot_bytes, sealed_bytes) = {
            let per = self.per_nest.lock().unwrap();
            let mut hot = 0u64;
            // A runtime's nests share one segment store; counting it once per nest multiplied it.
            let mut stores = std::collections::BTreeMap::new();
            for m in per.values() {
                let (h, s) = m.storage_bytes();
                hot += h;
                stores.insert(m.sealed_store(), s);
            }
            (hot, stores.values().sum::<u64>())
        };
        s.push_str(&gauge(
            "nuthatch_hot_store_bytes",
            "Bytes of the mutable hot store (redb) on disk, summed across nests.",
            hot_bytes,
        ));
        s.push_str(&gauge(
            "nuthatch_sealed_segments_bytes",
            "Bytes of sealed Parquet segments on disk, each segment store counted once.",
            sealed_bytes,
        ));

        s.push_str(&gauge(
            "nuthatch_seal_direct_active",
            "1 while a --seal-direct history pass is running, 0 after handoff to the tip cursor (#807).",
            u64::from(self.seal_direct_active.load(Relaxed)),
        ));
        s.push_str(&gauge(
            "nuthatch_seal_direct_origin",
            "First block of the current or last seal-direct pass.",
            self.seal_direct_origin.load(Relaxed),
        ));
        s.push_str(&gauge(
            "nuthatch_seal_direct_completed",
            "Highest block sealed in the current or last seal-direct pass.",
            self.seal_direct_completed.load(Relaxed),
        ));
        s.push_str(&gauge(
            "nuthatch_seal_direct_fetched",
            "Highest block fetched and decoded by the current or last seal-direct pass (#1169). Ahead of nuthatch_seal_direct_completed by the work a restart would redo.",
            self.seal_direct_fetched.load(Relaxed),
        ));
        s.push_str(&gauge(
            "nuthatch_fetch_window_blocks",
            "Block span of the cursor's most recent eth_getLogs window (#1170). A window stuck at a handful of blocks after a rate-limited hour is the collapse #1170 describes.",
            self.fetch_window.load(Relaxed),
        ));
        s.push_str(&gauge(
            "nuthatch_seal_direct_target",
            "Final block of the current or last seal-direct pass (preserved after handoff).",
            self.seal_direct_target.load(Relaxed),
        ));

        {
            let endpoints = self.rpc_endpoints.lock().unwrap();
            s.push_str("# HELP nuthatch_rpc_endpoint_requests_total Outbound RPC POSTs, labelled by a host[:port] identifier, never a URL.\n# TYPE nuthatch_rpc_endpoint_requests_total counter\n");
            s.push_str("# HELP nuthatch_rpc_endpoint_failures_total RPC POSTs that failed, same label.\n# TYPE nuthatch_rpc_endpoint_failures_total counter\n");
            s.push_str("# HELP nuthatch_rpc_endpoint_retries_total RPC POSTs that were a failover retry onto this endpoint.\n# TYPE nuthatch_rpc_endpoint_retries_total counter\n");
            s.push_str("# HELP nuthatch_rpc_request_duration_seconds RPC round-trip duration in seconds, labelled by endpoint.\n# TYPE nuthatch_rpc_request_duration_seconds histogram\n");
            for (ep, st) in endpoints.iter() {
                s.push_str(&format!(
                    "nuthatch_rpc_endpoint_requests_total{{endpoint=\"{ep}\"}} {}\n",
                    st.requests
                ));
                s.push_str(&format!(
                    "nuthatch_rpc_endpoint_failures_total{{endpoint=\"{ep}\"}} {}\n",
                    st.failures
                ));
                s.push_str(&format!(
                    "nuthatch_rpc_endpoint_retries_total{{endpoint=\"{ep}\"}} {}\n",
                    st.retries
                ));
                for (i, bound) in RPC_DURATION_BOUNDS.iter().enumerate() {
                    s.push_str(&format!(
                        "nuthatch_rpc_request_duration_seconds_bucket{{endpoint=\"{ep}\",le=\"{bound}\"}} {}\n",
                        st.buckets[i]
                    ));
                }
                s.push_str(&format!(
                    "nuthatch_rpc_request_duration_seconds_bucket{{endpoint=\"{ep}\",le=\"+Inf\"}} {}\n",
                    st.buckets[RPC_DURATION_BOUNDS.len()]
                ));
                s.push_str(&format!(
                    "nuthatch_rpc_request_duration_seconds_sum{{endpoint=\"{ep}\"}} {}\n",
                    st.duration_sum
                ));
                s.push_str(&format!(
                    "nuthatch_rpc_request_duration_seconds_count{{endpoint=\"{ep}\"}} {}\n",
                    st.duration_count
                ));
            }
        }

        // Per-nest series (SEC-9): in a runtime, one `{nest="…"}`-labelled line per mounted nest, so the
        // blended aggregates above can be broken down by nest. Distinct `_nest_` metric names keep the
        // exposition unambiguous (a name is never both labelled and unlabelled).
        let per = self.per_nest.lock().unwrap();
        if !per.is_empty() {
            let mut labelled =
                |name: &str, help: &str, typ: &str, get: &dyn Fn(&NestMetrics) -> u64| {
                    s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {typ}\n"));
                    for (nest, m) in per.iter() {
                        s.push_str(&format!("{name}{{nest=\"{nest}\"}} {}\n", get(m)));
                    }
                };
            labelled(
                "nuthatch_nest_hot_store_bytes",
                "Bytes of this nest's hot store (redb) on disk.",
                "gauge",
                &|m| m.dataset_storage_bytes().0,
            );
            labelled(
                "nuthatch_nest_sealed_segments_bytes",
                "Bytes of the sealed segments this nest's manifest names. Segments datasets share are counted under each, so the sum over nests is not disk used; nuthatch_sealed_segments_bytes is.",
                "gauge",
                &|m| m.dataset_storage_bytes().1,
            );
            let pool = |m: &NestMetrics| {
                m.dataset
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|d| pools.by_dir.get(d).copied())
                    .unwrap_or_default()
            };
            labelled(
                "nuthatch_nest_analytics_pool_reserved_bytes",
                "Bytes statements in flight hold against the pools of the analytics engines opened over this nest's dataset.",
                "gauge",
                &|m| pool(m).reserved,
            );
            labelled(
                "nuthatch_nest_analytics_pool_peak_bytes",
                "The most the statements of any one analytics engine opened over this nest's dataset have held since start.",
                "gauge",
                &|m| pool(m).peak,
            );
            labelled(
                "nuthatch_nest_analytics_engines",
                "Live analytics engines opened over this nest's dataset, all on its one pool.",
                "gauge",
                &|m| pool(m).engines,
            );
            labelled(
                "nuthatch_nest_tip_height",
                "Latest source block height, per nest and therefore per chain.",
                "gauge",
                &|m| m.tip.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_last_block",
                "Highest block committed, per nest.",
                "gauge",
                &|m| m.last_block.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_tip_lag_blocks",
                "Blocks behind the source tip, per nest and therefore per chain.",
                "gauge",
                &|m| {
                    m.tip
                        .load(Relaxed)
                        .saturating_sub(m.last_block.load(Relaxed))
                },
            );
            labelled(
                "nuthatch_nest_sealed_through",
                "Highest block sealed to the cold layer, per nest.",
                "gauge",
                &|m| m.sealed_through.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_last_seal_progress_unixtime",
                "When the nest's sealed watermark last advanced, 0 if it never has.",
                "gauge",
                &|m| m.last_seal_progress.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_seal_fault_seconds",
                "How long the nest's seal has failed on every attempt, 0 while it succeeds. /ready names the error.",
                "gauge",
                &|m| {
                    m.seal_fault()
                        .map_or(0, |(since, _)| now_unix().saturating_sub(since))
                },
            );
            #[cfg(feature = "folds")]
            labelled(
                "nuthatch_nest_fold_checkpoint_lag_blocks",
                "Sealed blocks the folds are not yet checkpointed through, per nest.",
                "gauge",
                &|m| m.fold_checkpoint_lag_blocks(),
            );
            #[cfg(feature = "folds")]
            labelled(
                "nuthatch_nest_fold_writer_faulted",
                "1 when the nest's fold checkpoint writer has stopped on an error.",
                "gauge",
                &|m| u64::from(m.fold_writer_faulted()),
            );
            labelled(
                "nuthatch_nest_rows_decoded_total",
                "Rows decoded since start, per nest.",
                "counter",
                &|m| m.rows_decoded.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_rows_sealed_total",
                "Rows sealed to Parquet since start, per nest.",
                "counter",
                &|m| m.rows_sealed.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_ipfs_unreadable_total",
                "Rows an [[ipfs]] declaration read that named no usable CID, per nest.",
                "counter",
                &|m| m.ipfs_unreadable.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_ipfs_pending",
                "Documents named and neither stored nor given up on, per nest. Sealing holds below the lowest.",
                "gauge",
                &|m| m.ipfs_pending.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_ipfs_resolved_total",
                "Documents fetched and stored since start, per nest.",
                "counter",
                &|m| m.ipfs_resolved.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_ipfs_given_up_total",
                "Documents given up on after every retry failed, since start, per nest.",
                "counter",
                &|m| m.ipfs_given_up.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_ipfs_unverified_total",
                "IPFS documents fetched that could not be proven against their CID, per nest.",
                "counter",
                &|m| m.ipfs_unverified.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_ipfs_oversize_total",
                "IPFS documents refused for exceeding the byte, block or depth cap, per nest.",
                "counter",
                &|m| m.ipfs_oversize.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_ipfs_rows_refused_total",
                "Proven IPFS documents whose content did not fit their declared typed rows, per nest.",
                "counter",
                &|m| m.ipfs_rows_refused.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_ipfs_retries_total",
                "Failed IPFS document fetches that were tried again, per nest. Each is logged at warn.",
                "counter",
                &|m| m.ipfs_retries.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_reorgs_total",
                "Reorgs detected and rolled back since start, per nest.",
                "counter",
                &|m| m.reorgs.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_checkpoints_missed_total",
                "Windows refused because the source gave no hash for the boundary block, per nest. Indexing is held while it climbs.",
                "counter",
                &|m| m.checkpoints_missed.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_seal_direct_active",
                "1 while this nest's --seal-direct pass is running.",
                "gauge",
                &|m| u64::from(m.seal_direct_active.load(Relaxed)),
            );
            labelled(
                "nuthatch_nest_seal_direct_completed",
                "Highest block sealed in this nest's current or last seal-direct pass.",
                "gauge",
                &|m| m.seal_direct_completed.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_seal_direct_fetched",
                "Highest block fetched and decoded by this nest's seal-direct pass (#1169).",
                "gauge",
                &|m| m.seal_direct_fetched.load(Relaxed),
            );
            labelled(
                "nuthatch_nest_fetch_window_blocks",
                "Block span of this nest's most recent eth_getLogs window (#1170).",
                "gauge",
                &|m| m.fetch_window.load(Relaxed),
            );
            // RFC-0052 S2. Only publishing nests carry these, so an unmirrored nest does not grow a
            // column of zeros that reads like a mirror that is caught up.
            let publishing: Vec<_> = per
                .iter()
                .filter(|(_, m)| m.publish_enabled.load(Relaxed))
                .collect();
            if !publishing.is_empty() {
                let mut series =
                    |name: &str, help: &str, typ: &str, get: &dyn Fn(&NestMetrics) -> u64| {
                        s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {typ}\n"));
                        for (nest, m) in &publishing {
                            s.push_str(&format!("{name}{{nest=\"{nest}\"}} {}\n", get(m)));
                        }
                    };
                series(
                    "nuthatch_publish_sealed_through",
                    "Highest block the published catalogue covers (RFC-0052).",
                    "gauge",
                    &|m| m.publish_sealed_through.load(Relaxed),
                );
                series(
                    "nuthatch_publish_lag_blocks",
                    "Blocks spanned by final local segments the mirror does not hold, summed over tables.",
                    "gauge",
                    &|m| m.publish_lag_blocks(),
                );
                series(
                    "nuthatch_publish_pending_segments",
                    "Segments the current publishing pass has still to upload.",
                    "gauge",
                    &|m| m.publish_pending.load(Relaxed),
                );
                series(
                    "nuthatch_publish_bytes_total",
                    "Segment bytes uploaded since start.",
                    "counter",
                    &|m| m.publish_bytes.load(Relaxed),
                );
                series(
                    "nuthatch_publish_errors_total",
                    "Publishing passes that failed since start.",
                    "counter",
                    &|m| m.publish_errors.load(Relaxed),
                );
                series(
                    "nuthatch_publish_dead_letter",
                    "1 once the same object has failed repeatedly; clears on the next success.",
                    "gauge",
                    &|m| u64::from(m.publish_dead_letter.load(Relaxed)),
                );
            }
            // RFC-0062. Only nests that declare a maintained view, for the same reason.
            let maintained: Vec<(&String, Vec<_>)> = per
                .iter()
                .filter_map(|(nest, m)| {
                    let dir = m.dataset.lock().unwrap().clone()?;
                    let views = crate::maintained::report(&dir);
                    (!views.is_empty()).then_some((nest, views))
                })
                .collect();
            if !maintained.is_empty() {
                let mut series = |name: &str,
                                  help: &str,
                                  typ: &str,
                                  get: &dyn Fn(&crate::maintained::ViewStats, u64, u64) -> u64| {
                    s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {typ}\n"));
                    for (nest, views) in &maintained {
                        for (view, stats, copies, bytes) in views {
                            s.push_str(&format!(
                                "{name}{{nest=\"{nest}\",view=\"{view}\"}} {}\n",
                                get(stats, *copies, *bytes)
                            ));
                        }
                    }
                };
                series(
                    "nuthatch_maintained_hits_total",
                    "Requests that read a maintained view from its copy, since start.",
                    "counter",
                    &|v, _, _| v.hits.load(Relaxed),
                );
                series(
                    "nuthatch_maintained_fallbacks_total",
                    "Requests that reached a maintained view with no copy at their inputs and answered from its definition, since start.",
                    "counter",
                    &|v, _, _| v.fallbacks.load(Relaxed),
                );
                series(
                    "nuthatch_maintained_builds_total",
                    "Copies of a maintained view written, since start.",
                    "counter",
                    &|v, _, _| v.builds.load(Relaxed),
                );
                series(
                    "nuthatch_maintained_build_failures_total",
                    "Builds of a maintained view that failed, since start. /ready names the last error.",
                    "counter",
                    &|v, _, _| v.build_failures.load(Relaxed),
                );
                series(
                    "nuthatch_maintained_last_build_milliseconds",
                    "How long the last successful build of a maintained view took.",
                    "gauge",
                    &|v, _, _| v.last_build_ms.load(Relaxed),
                );
                series(
                    "nuthatch_maintained_copies",
                    "Copies of a maintained view on disk, the current one and those retention keeps.",
                    "gauge",
                    &|_, copies, _| copies,
                );
                series(
                    "nuthatch_maintained_copy_bytes",
                    "Bytes of a maintained view's copies on disk.",
                    "gauge",
                    &|_, _, bytes| bytes,
                );
            }
        }
        s
    }
}

/// Wall-clock unix seconds - used only for the poll-freshness/readiness signal, never in the
/// deterministic data path.
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// This process's resident set size in bytes. Linux via `/proc/self/status`; else `ps` (macOS/BSD);
/// 0 if neither answers (the gauge just reads 0 rather than failing the scrape).
pub fn rss_bytes() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                if let Ok(kb) = rest.trim().trim_end_matches("kB").trim().parse::<u64>() {
                    return kb * 1024;
                }
            }
        }
    }
    let pid = std::process::id().to_string();
    if let Ok(out) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
    {
        if let Ok(kb) = String::from_utf8_lossy(&out.stdout).trim().parse::<u64>() {
            return kb * 1024;
        }
    }
    0
}

#[cfg(all(target_os = "linux", not(target_env = "musl")))]
struct JemallocStats {
    allocated: u64,
    active: u64,
    resident: u64,
    retained: u64,
}

/// jemalloc's counters, refreshed now: they hold still until the epoch advances.
#[cfg(all(target_os = "linux", not(target_env = "musl")))]
fn jemalloc_stats() -> Option<JemallocStats> {
    use tikv_jemalloc_ctl::{epoch, stats};
    epoch::advance().ok()?;
    Some(JemallocStats {
        allocated: stats::allocated::read().ok()? as u64,
        active: stats::active::read().ok()? as u64,
        resident: stats::resident::read().ok()? as u64,
        retained: stats::retained::read().ok()? as u64,
    })
}

/// Host[:port] of an RPC URL, never a scheme or path. An API key in a metric label is a
/// credential leak; `://` in a label is the test that would have caught it (#812).
pub fn endpoint_label(url: &str) -> String {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let hostport = rest.split('/').next().unwrap_or(rest);
    let hostport = match hostport.rfind('@') {
        Some(at) => &hostport[at + 1..],
        None => hostport,
    };
    hostport.trim().to_string()
}

/// Bytes of the segment files a dataset's manifest names, each file counted once.
fn manifest_segment_bytes(dir: &Path) -> u64 {
    let Ok(manifest) = crate::seal::load_manifest(dir) else {
        return 0;
    };
    let files: std::collections::BTreeSet<PathBuf> = manifest
        .tables
        .values()
        .flatten()
        .map(|s| crate::seal::segment_path(dir, &s.file, &s.hash))
        .collect();
    files
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum()
}

fn path_bytes(path: &Path) -> u64 {
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return 0,
    };
    if meta.is_file() {
        return meta.len();
    }
    if !meta.is_dir() {
        return 0;
    }
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            let Ok(m) = entry.metadata() else { continue };
            if m.is_dir() {
                stack.push(p);
            } else if m.is_file() {
                total += m.len();
            }
        }
    }
    total
}

fn process_cpu_seconds() -> f64 {
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
        // Field 14 utime, 15 stime, in clock ticks. comm can contain spaces inside parentheses.
        if let Some(rest) = stat.rsplit(')').next() {
            let f: Vec<&str> = rest.split_whitespace().collect();
            // After comm: state is 0, utime is 11, stime is 12 (1-indexed fields 14/15 minus 3).
            if f.len() > 12 {
                if let (Ok(ut), Ok(st)) = (f[11].parse::<f64>(), f[12].parse::<f64>()) {
                    return (ut + st) / ticks_per_sec();
                }
            }
        }
    }
    // No /proc on macOS/BSD. Same fallback shape as rss_bytes(): shell out to `ps`, whose `time`
    // keyword reports cumulative user+system CPU time for the pid as `[[dd-]hh:]mm:ss[.cc]`.
    let pid = std::process::id().to_string();
    if let Ok(out) = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &pid])
        .output()
    {
        if let Some(seconds) = parse_ps_cpu_time(String::from_utf8_lossy(&out.stdout).trim()) {
            return seconds;
        }
    }
    0.0
}

fn ticks_per_sec() -> f64 {
    100.0
}

/// Parses BSD `ps`'s `time`/`cputime` keyword: `[[dd-]hh:]mm:ss[.cc]`, e.g. `0:00.12`,
/// `1:02:03`, or `2-01:02:03` for a process that has run past a day.
fn parse_ps_cpu_time(value: &str) -> Option<f64> {
    let (days, rest) = match value.split_once('-') {
        Some((days, rest)) => (days.parse::<f64>().ok()?, rest),
        None => (0.0, value),
    };
    let fields: Vec<&str> = rest.split(':').collect();
    let seconds = match fields.as_slice() {
        [minutes, seconds] => minutes.parse::<f64>().ok()? * 60.0 + seconds.parse::<f64>().ok()?,
        [hours, minutes, seconds] => {
            hours.parse::<f64>().ok()? * 3600.0
                + minutes.parse::<f64>().ok()? * 60.0
                + seconds.parse::<f64>().ok()?
        }
        _ => return None,
    };
    Some(days * 86_400.0 + seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_is_prometheus_text_with_lag() {
        let m = Metrics::new();
        m.set_tip(1_000);
        m.set_last_block(940);
        m.add_rows_decoded(5);
        m.inc_sql();
        m.inc_sql_rejected(SqlRejection::Busy);
        m.inc_sql_rejected(SqlRejection::Invalid);
        m.inc_sql_rejected(SqlRejection::OutOfMemory);
        let out = m.render();
        assert!(out.contains("# TYPE nuthatch_tip_height gauge"));
        assert!(out.contains("nuthatch_tip_height 1000"));
        assert!(out.contains("nuthatch_tip_lag_blocks 60")); // 1000 - 940
        assert!(out.contains("# TYPE nuthatch_rows_decoded_total counter"));
        assert!(out.contains("nuthatch_rows_decoded_total 5"));
        assert!(out.contains("nuthatch_sql_queries_total 1"));
        assert!(out.contains("nuthatch_sql_rejections_total 3"));
        assert!(out.contains("nuthatch_sql_rejections_total{reason=\"busy\"} 1"));
        assert!(out.contains("nuthatch_sql_rejections_total{reason=\"invalid\"} 1"));
        assert!(out.contains("nuthatch_sql_rejections_total{reason=\"out_of_memory\"} 1"));
    }

    #[test]
    fn audit_series_appear_only_once_an_audit_runs() {
        let m = Metrics::new();
        assert!(!m.render().contains("nuthatch_audit_"));
        m.set_audit_on();
        m.add_audit_mismatches(2);
        let out = m.render();
        assert!(out.contains("nuthatch_audit_ranges_total 0"));
        assert!(out.contains("nuthatch_audit_mismatches_total 2"));
        assert!(out.contains("nuthatch_audit_errors_total 0"));
    }

    /// #1399: a retried IPFS fetch is a series, globally and per nest, and not only a log line.
    #[test]
    fn ipfs_retries_are_rendered_globally_and_per_nest() {
        let m = Metrics::new();
        m.nest("qos").ipfs_retries.fetch_add(7, Relaxed);
        m.add_ipfs_retries(7);
        let out = m.render();
        assert!(
            out.contains("# TYPE nuthatch_ipfs_retries_total counter"),
            "{out}"
        );
        assert!(out.contains("nuthatch_ipfs_retries_total 7"), "{out}");
        assert!(
            out.contains("nuthatch_nest_ipfs_retries_total{nest=\"qos\"} 7"),
            "{out}"
        );
    }

    #[test]
    fn per_nest_series_are_labelled() {
        let m = Metrics::new();
        let horizon = m.nest("horizon");
        let graph = m.nest("graph-network");
        horizon.set_last_block(500);
        horizon.add_rows_decoded(10);
        graph.set_last_block(400);
        graph.inc_reorgs();
        graph.inc_checkpoints_missed();
        let out = m.render();
        // One labelled series per nest, distinct from the blended aggregates.
        assert!(out.contains("# TYPE nuthatch_nest_last_block gauge"));
        assert!(out.contains("nuthatch_nest_last_block{nest=\"horizon\"} 500"));
        assert!(out.contains("nuthatch_nest_last_block{nest=\"graph-network\"} 400"));
        assert!(out.contains("nuthatch_nest_rows_decoded_total{nest=\"horizon\"} 10"));
        assert!(out.contains("nuthatch_nest_reorgs_total{nest=\"graph-network\"} 1"));
        assert!(out.contains("nuthatch_nest_checkpoints_missed_total{nest=\"graph-network\"} 1"));
        // A per-nest update also feeds the process-global aggregate (backward-compatible).
        assert_eq!(horizon.last_block.load(Relaxed), 500);
    }

    #[test]
    fn multi_nest_render_omits_unlabelled_chain_heights() {
        let m = Metrics::new();
        let eth = m.nest("eth");
        let arb = m.nest("arb");
        eth.set_tip(25_632_906);
        eth.set_last_block(25_632_840);
        arb.set_tip(488_677_305);
        arb.set_last_block(488_677_300);
        let out = m.render();
        assert!(out.contains("nuthatch_nest_tip_height{nest=\"eth\"} 25632906"));
        assert!(out.contains("nuthatch_nest_tip_height{nest=\"arb\"} 488677305"));
        assert!(
            !out.contains("\nnuthatch_tip_height "),
            "a last-writer-wins process-wide chain height is nonsense in a multichain runtime"
        );
        assert!(!out.contains("\nnuthatch_tip_lag_blocks "));

        m.remove_nest("arb");
        let solo = m.render();
        assert!(solo.contains("nuthatch_tip_height 25632906"));
        assert!(solo.contains("nuthatch_last_block 25632840"));
    }

    #[test]
    fn removing_a_nest_removes_its_series_and_storage_from_scrapes() {
        let dir = tempfile::tempdir().unwrap();
        let hot = dir.path().join("hot.redb");
        let sealed = dir.path().join("segments");
        std::fs::write(&hot, vec![0u8; 11]).unwrap();
        std::fs::create_dir_all(&sealed).unwrap();
        std::fs::write(sealed.join("one.parquet"), vec![0u8; 13]).unwrap();

        let m = Metrics::new();
        let nest = m.nest("departed");
        nest.set_storage_paths(hot, sealed);
        nest.set_last_block(7);
        assert!(m.render().contains("nest=\"departed\""));
        m.remove_nest("departed");
        let out = m.render();
        assert!(!out.contains("nest=\"departed\""));
        assert!(out.contains("nuthatch_hot_store_bytes 0"));
        assert!(out.contains("nuthatch_sealed_segments_bytes 0"));
    }

    #[test]
    fn storage_size_is_cached_between_scrapes() {
        let dir = tempfile::tempdir().unwrap();
        let hot = dir.path().join("hot.redb");
        let sealed = dir.path().join("segments");
        std::fs::write(&hot, vec![0u8; 11]).unwrap();
        std::fs::create_dir_all(&sealed).unwrap();
        std::fs::write(sealed.join("one.parquet"), vec![0u8; 13]).unwrap();
        let nest = NestMetrics::default();
        nest.set_storage_paths(hot.clone(), sealed.clone());
        assert_eq!(nest.storage_bytes(), (11, 13));
        std::fs::write(&hot, vec![0u8; 99]).unwrap();
        std::fs::write(sealed.join("two.parquet"), vec![0u8; 77]).unwrap();
        assert_eq!(
            nest.storage_bytes(),
            (11, 13),
            "a second scrape uses the bounded-refresh value rather than walking the tree again"
        );
    }

    /// #1557: each nest reports its own redb and the segments its manifest names, and the global
    /// sealed total counts a runtime's shared segment store once, not once per nest.
    #[test]
    fn storage_is_reported_per_nest_and_the_shared_store_once() {
        use crate::seal::{Manifest, Segment, SEGMENTS_DIR};
        let root = tempfile::tempdir().unwrap();
        let store = root.path().join(SEGMENTS_DIR);
        std::fs::create_dir_all(&store).unwrap();
        let (own_a, shared, own_b) = ("aa".repeat(32), "bb".repeat(32), "cc".repeat(32));
        for (h, len) in [(&own_a, 100usize), (&shared, 1000), (&own_b, 10_000)] {
            std::fs::write(store.join(format!("{h}.parquet")), vec![0u8; len]).unwrap();
        }
        let seg = |h: &str| Segment {
            hash: h.to_string(),
            from_block: 1,
            to_block: 2,
            rows: 1,
            file: format!("t-{h}.parquet"),
            registry_snapshot: None,
            provisional: false,
            writer_profile: crate::seal::ORIGINAL_WRITER_PROFILE.to_string(),
            input_hash: None,
        };
        let m = Metrics::new();
        for (name, nid, hashes, redb) in [
            ("a", "01".repeat(32), [&own_a, &shared], 3usize),
            ("b", "02".repeat(32), [&shared, &own_b], 5),
        ] {
            let dir = root.path().join(crate::runtime::DATA_DIR).join(&nid);
            std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).unwrap();
            let mut manifest = Manifest::default();
            manifest
                .tables
                .insert("t".into(), hashes.iter().map(|h| seg(h)).collect());
            std::fs::write(
                dir.join(SEGMENTS_DIR).join(crate::seal::MANIFEST_FILE),
                serde_json::to_string(&manifest).unwrap(),
            )
            .unwrap();
            std::fs::write(dir.join("nuthatch.redb"), vec![0u8; redb]).unwrap();
            let nest = m.nest(name);
            nest.set_storage_paths(dir.join("nuthatch.redb"), store.clone());
            nest.set_dataset_dir(dir);
        }

        let out = m.render();
        for line in [
            "nuthatch_sealed_segments_bytes 11100\n",
            "nuthatch_hot_store_bytes 8\n",
            "nuthatch_nest_sealed_segments_bytes{nest=\"a\"} 1100\n",
            "nuthatch_nest_sealed_segments_bytes{nest=\"b\"} 11000\n",
            "nuthatch_nest_hot_store_bytes{nest=\"a\"} 3\n",
            "nuthatch_nest_hot_store_bytes{nest=\"b\"} 5\n",
        ] {
            assert!(out.contains(line), "missing {line:?} in:\n{out}");
        }
    }

    /// RFC-0062: a nest declaring a maintained view gets its series, labelled by view; one that
    /// declares none adds no line at all.
    #[test]
    fn maintained_series_appear_only_for_a_nest_that_declares_one() {
        let declares = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(declares.path().join("views")).unwrap();
        std::fs::write(
            declares.path().join("views/v.sql"),
            "CREATE VIEW bet AS SELECT 1 AS id;",
        )
        .unwrap();
        std::fs::write(
            declares.path().join(crate::maintained::DECLARATION_FILE),
            "[[view]]\nname = \"bet\"\n",
        )
        .unwrap();
        crate::maintained::load(declares.path()).unwrap();
        let copies = declares.path().join("maintained/bet");
        std::fs::create_dir_all(&copies).unwrap();
        std::fs::write(copies.join("a.parquet"), [0u8; 7]).unwrap();
        crate::maintained::note_hit(declares.path(), "bet");
        let plain = tempfile::tempdir().unwrap();

        let m = Metrics::new();
        m.nest("plain").set_dataset_dir(plain.path().to_path_buf());
        let none = m.render();
        assert!(!none.contains("nuthatch_maintained_"), "{none}");

        m.nest("bets")
            .set_dataset_dir(declares.path().to_path_buf());
        let out = m.render();
        assert!(out.contains("# TYPE nuthatch_maintained_hits_total counter"));
        assert!(out.contains("nuthatch_maintained_hits_total{nest=\"bets\",view=\"bet\"} 1"));
        assert!(out.contains("nuthatch_maintained_copies{nest=\"bets\",view=\"bet\"} 1"));
        assert!(out.contains("nuthatch_maintained_copy_bytes{nest=\"bets\",view=\"bet\"} 7"));
        assert!(!out.contains("nest=\"plain\",view="), "{out}");
    }

    #[test]
    fn no_nests_means_no_per_nest_block() {
        // A fresh registry with no nests registered renders only the aggregates - no labelled series.
        assert!(!Metrics::new()
            .render()
            .contains("\nnuthatch_nest_last_block{"));
    }

    #[test]
    fn poll_ok_is_recorded_and_rendered() {
        let m = Metrics::new();
        assert_eq!(m.last_poll_ok(), 0, "never polled yet");
        m.mark_poll_ok();
        assert!(m.last_poll_ok() >= now_unix() - 2, "records ~now");
        assert!(m
            .render()
            .contains("# TYPE nuthatch_last_poll_unixtime gauge"));
    }

    #[test]
    fn lag_never_goes_negative() {
        let m = Metrics::new();
        m.set_tip(100);
        m.set_last_block(120); // last ahead of a stale tip read
        assert!(m.render().contains("nuthatch_tip_lag_blocks 0"));
    }

    /// #578: `last_progress` stamps only when `last_block` actually moves, not on every call - a cursor
    /// re-reporting the same watermark (holding position while it waits out an unfetchable block
    /// timestamp) must not look like it's making progress.
    #[test]
    fn last_progress_stamps_only_on_an_actual_advance() {
        let m = Metrics::new();
        assert_eq!(m.last_progress(), 0, "never advanced yet");
        m.set_last_block(100);
        let first = m.last_progress();
        assert!(first > 0, "first advance stamps progress");
        m.set_last_block(100); // same value again - held, not advanced
        assert_eq!(
            m.last_progress(),
            first,
            "re-reporting the same block must not restamp progress"
        );
        m.set_last_block(101); // a genuine advance restamps
        assert!(m.last_progress() >= first);
    }

    #[test]
    fn start_time_is_rendered_once_a_cursor_exists() {
        let m = Metrics::new();
        assert!(!m.render().contains("nuthatch_start_time_seconds"));
        m.set_started_at_for_test(1_791_000_000);
        assert!(m
            .render()
            .contains("\nnuthatch_start_time_seconds 1791000000\n"));
    }

    #[test]
    fn rpc_methods_series_are_rendered() {
        let m = Metrics::new();
        assert!(
            m.render()
                .contains("# TYPE nuthatch_rpc_methods_total counter"),
            "the series must be advertised before any RPC has been issued"
        );
        m.inc_rpc_method("eth_getBlockByNumber");
        m.inc_rpc_methods("eth_getLogs", 3);
        m.inc_rpc_methods("", 9);
        let out = m.render();
        assert!(out.contains("nuthatch_rpc_methods_total{method=\"eth_getBlockByNumber\"} 1"));
        assert!(out.contains("nuthatch_rpc_methods_total{method=\"eth_getLogs\"} 3"));
        assert!(
            !out.contains("method=\"\""),
            "empty method names are not a billable CU line"
        );
        assert!(
            out.contains("Outbound HTTP POSTs"),
            "help text must say this is envelopes, not methods: {out}"
        );
    }

    #[test]
    fn endpoint_label_is_a_host_and_never_a_url() {
        assert_eq!(
            endpoint_label("https://arb-mainnet.g.alchemy.com/v2/SECRETKEY123"),
            "arb-mainnet.g.alchemy.com"
        );
        assert_eq!(
            endpoint_label("http://user:pass@127.0.0.1:8545/"),
            "127.0.0.1:8545"
        );
        let label = endpoint_label("https://x.example/v2/SECRETKEY123");
        assert!(!label.contains("://"), "a scheme in the label is the leak");
        assert!(!label.contains('/'), "a path in the label is the leak");
        assert!(!label.contains("SECRETKEY123"));
    }

    #[test]
    fn scrape_names_cpu_disk_and_rpc_health() {
        let m = Metrics::new();
        m.observe_rpc(
            &endpoint_label("https://rpc.example/v2/KEY"),
            Duration::from_millis(12),
            false,
            false,
        );
        let out = m.render();
        assert!(out.contains("nuthatch_process_cpu_seconds_total"));
        assert!(out.contains("nuthatch_hot_store_bytes"));
        assert!(out.contains("nuthatch_sealed_segments_bytes"));
        assert!(out.contains("nuthatch_rpc_endpoint_requests_total{endpoint=\"rpc.example\"} 1"));
        assert!(out.contains("nuthatch_rpc_request_duration_seconds_bucket"));
        assert!(
            !out.contains("://"),
            "an RPC URL must not appear in the scrape: {out}"
        );
    }

    #[test]
    fn parses_ps_cpu_time_seconds_only_forms() {
        assert_eq!(parse_ps_cpu_time("0:00.12"), Some(0.12));
        assert_eq!(parse_ps_cpu_time("1:02.50"), Some(62.50));
    }

    #[test]
    fn parses_ps_cpu_time_with_hours() {
        assert_eq!(parse_ps_cpu_time("1:02:03"), Some(3723.0));
    }

    #[test]
    fn parses_ps_cpu_time_with_days() {
        assert_eq!(
            parse_ps_cpu_time("2-01:02:03"),
            Some(2.0 * 86_400.0 + 3723.0)
        );
    }

    #[test]
    fn parses_ps_cpu_time_rejects_garbage() {
        assert_eq!(parse_ps_cpu_time(""), None);
        assert_eq!(parse_ps_cpu_time("not-a-time"), None);
    }

    /// The real fallback path, run for real: on a host with no `/proc`, `process_cpu_seconds()`
    /// must advance as the process burns CPU, not sit pinned at 0.0 forever (the exact bug this
    /// change fixes). Linux never reaches the `ps` fallback, so this is gated off it.
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn process_cpu_seconds_advances_via_the_ps_fallback_off_linux() {
        let before = process_cpu_seconds();
        let start = std::time::Instant::now();
        let mut sink = 0u64;
        while start.elapsed() < Duration::from_millis(300) {
            sink = sink.wrapping_add(std::hint::black_box(1));
        }
        std::hint::black_box(sink);
        let after = process_cpu_seconds();
        assert!(
            after > before,
            "expected process_cpu_seconds() to advance after burning CPU (before={before}, after={after}) \
             - a flat reading is the macOS/BSD bug this fallback exists to fix"
        );
    }

    #[test]
    fn seal_direct_progress_advances_then_hands_off() {
        let m = Metrics::new();
        m.begin_seal_direct(100, 200);
        m.set_seal_direct_completed(150);
        let nest = m.nest("usdc");
        nest.begin_seal_direct(100, 200);
        nest.set_seal_direct_completed(150);
        let mid = m.render();
        assert!(mid.contains("nuthatch_seal_direct_active 1"));
        assert!(mid.contains("nuthatch_seal_direct_origin 100"));
        assert!(mid.contains("nuthatch_seal_direct_completed 150"));
        assert!(mid.contains("nuthatch_seal_direct_target 200"));
        assert!(nest.seal_direct_active(), "active during the pass");
        m.end_seal_direct();
        nest.end_seal_direct();
        assert!(!nest.seal_direct_active());
        assert_eq!(nest.seal_direct_completed(), 200, "target preserved");
        let after = m.render();
        assert!(after.contains("nuthatch_seal_direct_active 0"));
        assert!(after.contains("nuthatch_seal_direct_completed 200"));
        assert!(after.contains("nuthatch_seal_direct_target 200"));
    }

    #[test]
    fn deleting_seal_direct_gauges_fails_the_scrape_test() {
        let m = Metrics::new();
        m.begin_seal_direct(1, 10);
        m.set_seal_direct_completed(5);
        let out = m.render();
        assert!(out.contains("nuthatch_seal_direct_active 1"));
        assert!(out.contains("nuthatch_seal_direct_completed 5"));
    }

    /// #1169. The fetch position and the durable watermark are two counters. Fetching ahead must
    /// not move `seal_direct_completed`, which is what a restart resumes from; only a published seal
    /// does. Both are rendered, so the gap between them is a number an operator can read.
    #[test]
    fn fetching_ahead_does_not_move_the_seal_direct_watermark() {
        let m = Metrics::new();
        m.begin_seal_direct(100, 1_000);
        m.set_seal_direct_fetched(600);
        assert_eq!(m.seal_direct_fetched(), 600);
        assert_eq!(
            m.seal_direct_completed(),
            99,
            "a fetched block was reported as sealed; a restart from here would redo 500 blocks"
        );
        m.set_seal_direct_completed(300);
        assert_eq!(m.seal_direct_completed(), 300);
        // A tick for an older block is not progress and must not move the mark backwards.
        m.set_seal_direct_fetched(550);
        assert_eq!(m.seal_direct_fetched(), 600);
        m.set_fetch_window(64);
        let out = m.render();
        assert!(out.contains("nuthatch_seal_direct_fetched 600"));
        assert!(out.contains("nuthatch_seal_direct_completed 300"));
        assert!(out.contains("nuthatch_fetch_window_blocks 64"));
        m.end_seal_direct();
        assert_eq!(m.seal_direct_fetched(), 1_000);
        assert_eq!(m.seal_direct_completed(), 1_000);
    }

    /// #1199: **the tip path must stamp the seal clock, or `/ready` has nothing to judge it by.**
    ///
    /// Only `begin_seal_direct`, `set_seal_direct_completed` and `set_seal_direct_fetched` used to
    /// touch `last_seal_progress`, so a nest doing ordinary tip-following sealing left it at zero
    /// for ever. `serve`'s stall term reads that clock, and a handler test that sets it by hand
    /// passes whether or not anything in the runtime ever moves it - which is exactly what this
    /// mutation-checked to prove: removing the stamp below left every `/ready` test green.
    #[test]
    fn advancing_the_sealed_watermark_stamps_the_seal_clock() {
        let m = NestMetrics::default();
        assert_eq!(
            m.last_seal_progress(),
            0,
            "a fresh handle has no seal clock"
        );

        m.set_sealed_through(100);
        let stamped = m.last_seal_progress();
        assert_ne!(
            stamped, 0,
            "the tip path advanced the watermark and the seal clock did not move, so `/ready` \
             judges a tip-path seal against a clock nothing winds (#1199)"
        );

        // Re-reporting the same block is not progress and must not refresh its own deadline - the
        // same discipline `set_seal_direct_completed` already keeps (#846).
        m.set_last_seal_progress_for_test(stamped.saturating_sub(500));
        m.set_sealed_through(100);
        assert_eq!(
            m.last_seal_progress(),
            stamped.saturating_sub(500),
            "re-reporting block 100 refreshed the clock, so a frozen seal keeps itself alive by \
             saying nothing has changed"
        );

        m.set_sealed_through(200);
        assert!(
            m.last_seal_progress() >= stamped,
            "a real advance must stamp the clock"
        );
    }
}
