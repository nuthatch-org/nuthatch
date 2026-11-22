//! Loading, checking and caching chunks, and the fallback bookkeeping: which chunks the dataset
//! serves, which are absent for now, and which are bad for this run.

use super::columns::{check_logs_schema, extract_logs, heap_size, parse_blocks};
use super::{Log, XatuSource};
use crate::source::LogFilter;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Chunks whose logs are held at once. A chunk's compressed logs are 35 to 50 MB on mainnet, so this
/// bounds the source at a few hundred MB beside the 2 GB cursor budget.
pub(super) const KEEP_CHUNKS: usize = 4;

/// Chunks whose hashes and timestamps are held at once (about 100 KB each on mainnet). Wider than
/// [`KEEP_CHUNKS`] so a window spanning more chunks than that still finds every block's header here
/// when the indexer asks for it after the window's logs.
pub(super) const KEEP_HEADERS: usize = 128;

/// The most decoded rows the held chunks keep between them, estimated in bytes (a mainnet chunk of
/// USDC's logs is 60 to 110 MB). Past it the least recently used chunk's rows are dropped; a filter
/// matching more than this of one chunk is decoded per window instead, so memory follows the window.
pub(super) const MATCHED_BUDGET: usize = 256 << 20;

/// How long "the dataset has no such chunk yet" is remembered. The dataset's head moves, so a 404 is
/// not permanent; it is also the normal answer for the tail, so it must not be asked on every poll.
const ABSENT_FOR: Duration = Duration::from_secs(600);

/// How long a transient failure is remembered, so a stalled host or a flaky anchor is not retried
/// (and its file downloaded again) by every window in between.
pub(super) const TRANSIENT_FOR: Duration = Duration::from_secs(60);

/// When the RPC exposes no `finalized` block, a chunk is used once its last block is this deep.
/// Three epochs: mainnet finalizes 2 to 3 epochs (64 to 96 slots) behind the head.
const FINALITY_DEPTH: u64 = 96;

/// The blocks of a chunk that passed the checks.
pub(super) struct Headers {
    pub(super) start: u64,
    pub(super) hashes: Vec<String>,
    pub(super) timestamps: Vec<u64>,
}

/// A chunk that passed the checks.
pub(super) struct Chunk {
    pub(super) headers: Arc<Headers>,
    pub(super) logs: Bytes,
    /// The decoded rows for the filter last asked, sorted by (block, log index).
    matched: Mutex<Option<(LogFilter, Matched)>>,
}

#[derive(Clone)]
enum Matched {
    /// The rows, and their estimated heap size.
    Rows(Arc<Vec<Log>>, usize),
    /// Over [`MATCHED_BUDGET`]: decode each window's range on its own.
    TooBig,
}

enum Fault {
    /// Could not decide (a fetch failed, the RPC would not answer); try again after a while.
    Transient(anyhow::Error),
    /// This chunk is not usable; never ask again this run.
    Bad(anyhow::Error),
}

#[derive(Default)]
pub(super) struct State {
    ready: HashMap<u64, Arc<Chunk>>,
    order: VecDeque<u64>,
    headers: HashMap<u64, Arc<Headers>>,
    header_order: VecDeque<u64>,
    /// `Some(t)`: absent (or failing) until `t`. `None`: bad for this run.
    gone: HashMap<u64, Option<Instant>>,
    /// Whether the first absence has been logged. The tail is always absent, so it is said once.
    told_absent: bool,
}

fn touch(order: &mut VecDeque<u64>, start: u64) {
    if let Some(pos) = order.iter().position(|s| *s == start) {
        order.remove(pos);
    }
    order.push_back(start);
}

impl XatuSource {
    /// The headers of a chunk already loaded, without loading anything. Header lookups use this:
    /// they follow the logs read of the same window, which loaded the chunk, and a miss just goes to
    /// RPC.
    pub(super) fn cached(&self, block: u64) -> Option<Arc<Headers>> {
        let s = self.start_of(block);
        self.state.lock().unwrap().headers.get(&s).cloned()
    }

    /// The chunk starting at `start` if the dataset can serve it; `None` means ask RPC.
    pub(super) async fn chunk(&self, start: u64) -> Option<Arc<Chunk>> {
        if let Some(c) = self.lookup(start) {
            return c;
        }
        // Past finality only, decided before queueing behind another chunk's download so the tip's
        // windows never wait on one. The indexer checks for reorgs by asking `block_hash`, which a
        // held chunk answers; a chunk that could still reorganise would hide the reorg it should
        // see. The public dataset lags by hours, so this only ever bites a mirror that does not.
        match self.is_final(start + self.chunk_len - 1).await {
            Ok(true) => {}
            Ok(false) => {
                self.forget_for(start, ABSENT_FOR);
                return None;
            }
            Err(Fault::Transient(e) | Fault::Bad(e)) => {
                self.transient(start, &e);
                return None;
            }
        }
        let _one_load_at_a_time = self.load.lock().await;
        if let Some(c) = self.lookup(start) {
            return c;
        }
        let t = Instant::now();
        match self.load_chunk(start).await {
            Ok(Some(c)) => {
                let c = Arc::new(c);
                self.stats.chunks_loaded.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    chunk = start,
                    secs = format!("{:.1}", t.elapsed().as_secs_f32()),
                    "xatu: serving blocks {start}..={} from the dataset",
                    start + self.chunk_len - 1
                );
                let mut st = self.state.lock().unwrap();
                st.ready.insert(start, c.clone());
                touch(&mut st.order, start);
                while st.order.len() > self.keep_chunks {
                    if let Some(old) = st.order.pop_front() {
                        st.ready.remove(&old);
                    }
                }
                st.headers.insert(start, c.headers.clone());
                touch(&mut st.header_order, start);
                while st.header_order.len() > self.keep_headers {
                    if let Some(old) = st.header_order.pop_front() {
                        st.headers.remove(&old);
                    }
                }
                Some(c)
            }
            Ok(None) => {
                self.stats.absent.fetch_add(1, Ordering::Relaxed);
                let mut st = self.state.lock().unwrap();
                st.gone.insert(start, Some(Instant::now() + ABSENT_FOR));
                if !std::mem::replace(&mut st.told_absent, true) {
                    // Normal at the dataset's head; if it is the first chunk asked, a mirror without
                    // this layout (or behind a login) would look the same, so say where it looked.
                    tracing::warn!(
                        "xatu: the dataset has no file for blocks {start}..={} yet ({}); asking the \
                         RPC for them. Expected past the dataset's head; logged once",
                        start + self.chunk_len - 1,
                        self.url("canonical_execution_block", start)
                    );
                }
                None
            }
            Err(Fault::Transient(e)) => {
                self.transient(start, &e);
                None
            }
            Err(Fault::Bad(e)) => {
                self.mark_bad(start, &e);
                None
            }
        }
    }

    /// `Some(answer)` when the state already decides; `None` when a load is needed.
    fn lookup(&self, start: u64) -> Option<Option<Arc<Chunk>>> {
        let mut st = self.state.lock().unwrap();
        if let Some(c) = st.ready.get(&start).cloned() {
            touch(&mut st.order, start);
            touch(&mut st.header_order, start);
            return Some(Some(c));
        }
        match st.gone.get(&start) {
            Some(None) => Some(None),
            Some(Some(until)) if *until > Instant::now() => Some(None),
            Some(Some(_)) => {
                st.gone.remove(&start);
                None
            }
            None => None,
        }
    }

    fn forget_for(&self, start: u64, d: Duration) {
        self.state
            .lock()
            .unwrap()
            .gone
            .insert(start, Some(Instant::now() + d));
    }

    fn transient(&self, start: u64, e: &anyhow::Error) {
        self.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            "xatu: could not check blocks {start}..={} ({e:#}); asking the RPC for them, and the \
             dataset again in {}s",
            start + self.chunk_len - 1,
            self.retry_after.as_secs()
        );
        self.forget_for(start, self.retry_after);
    }

    pub(super) fn mark_bad(&self, start: u64, why: &anyhow::Error) {
        self.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            "xatu: not using the dataset for blocks {start}..={} ({why:#}); asking the RPC for them",
            start + self.chunk_len - 1
        );
        let mut st = self.state.lock().unwrap();
        st.ready.remove(&start);
        st.order.retain(|s| *s != start);
        st.headers.remove(&start);
        st.header_order.retain(|s| *s != start);
        st.gone.insert(start, None);
    }

    /// Whether `block` is past the chain's finality, asking the RPC only for a block above the
    /// highest already seen final.
    async fn is_final(&self, block: u64) -> Result<bool, Fault> {
        if block <= self.final_seen.load(Ordering::Relaxed) {
            return Ok(true);
        }
        let finalized = match self.inner.finalized().await {
            Ok(Some(f)) => f,
            Ok(None) => self
                .inner
                .tip()
                .await
                .map_err(|e| Fault::Transient(e.context("asking the RPC for its tip")))?
                .saturating_sub(FINALITY_DEPTH),
            Err(e) => {
                return Err(Fault::Transient(
                    e.context("asking the RPC for its finalized block"),
                ))
            }
        };
        self.final_seen.fetch_max(finalized, Ordering::Relaxed);
        Ok(block <= finalized)
    }

    async fn load_chunk(&self, start: u64) -> Result<Option<Chunk>, Fault> {
        let last = start + self.chunk_len - 1;
        let Some(blocks) = self
            .fetch
            .get(&self.url("canonical_execution_block", start))
            .await
            .map_err(Fault::Transient)?
        else {
            return Ok(None);
        };
        let (n, len) = (start, self.chunk_len);
        let parsed = tokio::task::spawn_blocking(move || parse_blocks(blocks, n, len))
            .await
            .map_err(|e| Fault::Bad(anyhow!(e)))?
            .map_err(Fault::Bad)?;

        // The anchor, before the large logs file is downloaded: the chain's own hash and timestamp
        // for the chunk's last block, and for its first when the file has no parent hashes to chain
        // the blocks between (one header each from an RPC client). The chain is the file agreeing
        // with itself; only the anchors are the chain's word. The timestamp is checked because it is
        // sealed into every row, and a writer's unit or time-zone slip would shift the whole file.
        let mut anchors = vec![last];
        if !parsed.chained {
            anchors.insert(0, start);
        }
        for block in anchors {
            self.stats.anchor_calls.fetch_add(1, Ordering::Relaxed);
            let (canonical, time) = match self.inner.block_record(block).await {
                Ok(Some(r)) => r,
                Ok(None) => {
                    return Err(Fault::Transient(anyhow!(
                        "the RPC has no hash for block {block} to check the chunk against"
                    )))
                }
                Err(e) => {
                    return Err(Fault::Transient(
                        e.context("checking the chunk against the RPC"),
                    ))
                }
            };
            let ours = &parsed.hashes[(block - start) as usize];
            if !canonical.eq_ignore_ascii_case(ours) {
                return Err(Fault::Bad(anyhow!(
                    "block {block} is {ours} in the dataset but {canonical} on the chain"
                )));
            }
            let at = parsed.timestamps[(block - start) as usize];
            if time.is_some_and(|t| t != at) {
                return Err(Fault::Bad(anyhow!(
                    "block {block} is at {at} in the dataset but {} on the chain",
                    time.unwrap_or_default()
                )));
            }
        }

        let Some(logs) = self
            .fetch
            .get(&self.url("canonical_execution_logs", start))
            .await
            .map_err(Fault::Transient)?
        else {
            // Not bad: the two files may be published apart, so the newest chunk can have its
            // block file first. Try again in a while.
            return Err(Fault::Transient(anyhow!(
                "the dataset has the block file for this chunk but not (yet) the logs file"
            )));
        };
        self.stats
            .bytes
            .fetch_add(logs.len() as u64, Ordering::Relaxed);
        check_logs_schema(&logs).map_err(Fault::Bad)?;
        Ok(Some(Chunk {
            headers: Arc::new(Headers {
                start,
                hashes: parsed.hashes,
                timestamps: parsed.timestamps,
            }),
            logs,
            matched: Mutex::new(None),
        }))
    }

    /// The matching logs of `c` for blocks `[a, b]`, decoding the chunk once per filter while its
    /// matches fit [`MATCHED_BUDGET`], and only `[a, b]` of it each time once they do not.
    pub(super) async fn read_chunk(
        &self,
        c: &Arc<Chunk>,
        filter: &LogFilter,
        a: u64,
        b: u64,
    ) -> Result<Vec<Log>> {
        let hit = || {
            c.matched
                .lock()
                .unwrap()
                .as_ref()
                .filter(|(f, _)| f == filter)
                .map(|(_, m)| m.clone())
        };
        let mut found = hit();
        let _decoding = if found.is_none() {
            let guard = self.decoding.lock().await;
            found = hit();
            Some(guard)
        } else {
            None
        };
        let whole = (c.headers.start, c.headers.start + self.chunk_len - 1);
        let rows = match found {
            Some(Matched::Rows(rows, _)) => rows,
            Some(Matched::TooBig) => return self.decode(c, filter, (a, b), usize::MAX).await,
            None => match self.decode(c, filter, whole, self.matched_budget).await {
                Ok(rows) => {
                    let size = rows.iter().map(heap_size).sum();
                    let rows = Arc::new(rows);
                    *c.matched.lock().unwrap() =
                        Some((filter.clone(), Matched::Rows(rows.clone(), size)));
                    self.keep_matched_within_budget(c);
                    rows
                }
                Err(e) if e.is::<OverBudget>() => {
                    *c.matched.lock().unwrap() = Some((filter.clone(), Matched::TooBig));
                    return self.decode(c, filter, (a, b), usize::MAX).await;
                }
                Err(e) => return Err(e),
            },
        };
        let lo = rows.partition_point(|l| l.block_number < a);
        let hi = rows.partition_point(|l| l.block_number <= b);
        Ok(rows[lo..hi].to_vec())
    }

    /// Drop the decoded rows of the least recently used held chunks, other than `keep`, until the
    /// rows held come within [`MATCHED_BUDGET`].
    fn keep_matched_within_budget(&self, keep: &Arc<Chunk>) {
        let st = self.state.lock().unwrap();
        let size = |c: &Chunk| match c.matched.lock().unwrap().as_ref() {
            Some((_, Matched::Rows(_, n))) => *n,
            _ => 0,
        };
        let mut held: usize = st.ready.values().map(|c| size(c)).sum();
        for start in &st.order {
            if held <= self.matched_budget {
                break;
            }
            let Some(c) = st.ready.get(start) else {
                continue;
            };
            if Arc::ptr_eq(c, keep) {
                continue;
            }
            held -= size(c);
            *c.matched.lock().unwrap() = None;
        }
    }

    /// The decoded rows held across chunks, estimated in bytes.
    #[cfg(test)]
    pub(super) fn matched_bytes(&self) -> usize {
        let st = self.state.lock().unwrap();
        st.ready
            .values()
            .map(|c| match c.matched.lock().unwrap().as_ref() {
                Some((_, Matched::Rows(_, n))) => *n,
                _ => 0,
            })
            .sum()
    }

    async fn decode(
        &self,
        c: &Arc<Chunk>,
        filter: &LogFilter,
        range: (u64, u64),
        budget: usize,
    ) -> Result<Vec<Log>> {
        let (cc, f, len) = (c.clone(), filter.clone(), self.chunk_len);
        tokio::task::spawn_blocking(move || {
            extract_logs(&cc, &f, len, range, budget)?.ok_or_else(|| anyhow!(OverBudget))
        })
        .await
        .map_err(|e| anyhow!(e))?
    }

    pub(super) fn forget_chunks_above(&self, block: u64) {
        // A reorg cannot reach a chunk the dataset serves (they are past finality), but a number is
        // not an identity (see the trait): drop every chunk holding a block above `block`, so it is
        // re-checked against the chain. A chunk ending at `block` itself stays, as the trait asks.
        let len = self.chunk_len;
        let below = |s: &u64| s + len - 1 <= block;
        let mut st = self.state.lock().unwrap();
        st.ready.retain(|s, _| below(s));
        st.order.retain(below);
        st.headers.retain(|s, _| below(s));
        st.header_order.retain(below);
        st.gone.retain(|s, _| below(s));
    }
}

/// A chunk's matches outgrew [`MATCHED_BUDGET`] while being decoded whole.
#[derive(Debug)]
struct OverBudget;

impl std::fmt::Display for OverBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the chunk's matches are over the decoded-rows budget")
    }
}

impl std::error::Error for OverBudget {}
