//! Loading, checking and caching chunks, and the fallback bookkeeping: which chunks the dataset
//! serves, which are absent for now, and which are bad for this run.

use super::columns::{check_logs_schema, extract_logs, parse_blocks};
use super::{Log, XatuSource};
use crate::source::LogFilter;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Chunks held at once. A chunk's compressed logs are 35 to 50 MB on mainnet, so this bounds the
/// source at a few hundred MB beside the 2 GB cursor budget.
const KEEP_CHUNKS: usize = 4;

/// How long "the dataset has no such chunk yet" is remembered. The dataset's head moves, so a 404 is
/// not permanent; it is also the normal answer for the tail, so it must not be asked on every poll.
const ABSENT_FOR: Duration = Duration::from_secs(600);

/// A chunk that passed the checks.
pub(super) struct Chunk {
    pub(super) start: u64,
    pub(super) hashes: Vec<String>,
    pub(super) timestamps: Vec<u64>,
    pub(super) logs: Bytes,
    /// The decoded rows for the filter last asked, sorted by (block, log index).
    matched: Mutex<Option<(LogFilter, Arc<Vec<Log>>)>>,
}

enum Fault {
    /// Could not decide (the RPC would not answer); try again next time.
    Transient(anyhow::Error),
    /// This chunk is not usable; never ask again this run.
    Bad(anyhow::Error),
}

#[derive(Default)]
pub(super) struct State {
    ready: HashMap<u64, Arc<Chunk>>,
    order: VecDeque<u64>,
    /// `Some(t)`: absent until `t`. `None`: bad for this run.
    gone: HashMap<u64, Option<Instant>>,
}

impl XatuSource {
    /// A chunk already loaded, without loading anything. Header lookups use this: they follow the
    /// logs read of the same window, which loaded the chunk, and a miss just goes to RPC.
    pub(super) fn cached(&self, block: u64) -> Option<Arc<Chunk>> {
        let s = self.start_of(block);
        self.state.lock().unwrap().ready.get(&s).cloned()
    }

    /// The chunk starting at `start` if the dataset can serve it; `None` means ask RPC.
    pub(super) async fn chunk(&self, start: u64) -> Option<Arc<Chunk>> {
        if let Some(c) = self.lookup(start) {
            return c;
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
                st.order.push_back(start);
                while st.order.len() > KEEP_CHUNKS {
                    if let Some(old) = st.order.pop_front() {
                        st.ready.remove(&old);
                    }
                }
                Some(c)
            }
            Ok(None) => {
                self.state
                    .lock()
                    .unwrap()
                    .gone
                    .insert(start, Some(Instant::now() + ABSENT_FOR));
                None
            }
            Err(Fault::Transient(e)) => {
                self.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "xatu: could not check blocks {start}..={} ({e:#}); asking the RPC for them",
                    start + self.chunk_len - 1
                );
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
            if let Some(pos) = st.order.iter().position(|s| *s == start) {
                st.order.remove(pos);
                st.order.push_back(start);
            }
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

    pub(super) fn mark_bad(&self, start: u64, why: &anyhow::Error) {
        self.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            "xatu: not using the dataset for blocks {start}..={} ({why:#}); asking the RPC for them",
            start + self.chunk_len - 1
        );
        let mut st = self.state.lock().unwrap();
        st.ready.remove(&start);
        st.order.retain(|s| *s != start);
        st.gone.insert(start, None);
    }

    async fn load_chunk(&self, start: u64) -> Result<Option<Chunk>, Fault> {
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
        let Some(logs) = self
            .fetch
            .get(&self.url("canonical_execution_logs", start))
            .await
            .map_err(Fault::Transient)?
        else {
            return Err(Fault::Bad(anyhow!(
                "the dataset has the block file for this chunk but not the logs file"
            )));
        };
        self.stats
            .bytes
            .fetch_add(logs.len() as u64, Ordering::Relaxed);
        check_logs_schema(&logs).map_err(Fault::Bad)?;

        // The anchor: the chain's own hash for the chunk's last block, and for its first when the
        // file has no parent hashes to chain the blocks between.
        let last = start + self.chunk_len - 1;
        let mut anchors = vec![last];
        if !parsed.chained {
            anchors.insert(0, start);
        }
        for block in anchors {
            self.stats.anchor_calls.fetch_add(1, Ordering::Relaxed);
            let canonical = match self.inner.block_hash(block).await {
                Ok(Some(h)) => h,
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
        }
        Ok(Some(Chunk {
            start,
            hashes: parsed.hashes,
            timestamps: parsed.timestamps,
            logs,
            matched: Mutex::new(None),
        }))
    }

    /// The matching logs of `c` for blocks `[a, b]`, decoding the chunk once per filter.
    pub(super) async fn read_chunk(
        &self,
        c: &Arc<Chunk>,
        filter: &LogFilter,
        a: u64,
        b: u64,
    ) -> Result<Vec<Log>> {
        let hit = c
            .matched
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(f, _)| f == filter)
            .map(|(_, rows)| rows.clone());
        let rows = match hit {
            Some(rows) => rows,
            None => {
                let (cc, f, len) = (c.clone(), filter.clone(), self.chunk_len);
                let rows = Arc::new(
                    tokio::task::spawn_blocking(move || extract_logs(&cc, &f, len))
                        .await
                        .map_err(|e| anyhow!(e))??,
                );
                *c.matched.lock().unwrap() = Some((filter.clone(), rows.clone()));
                rows
            }
        };
        let lo = rows.partition_point(|l| l.block_number < a);
        let hi = rows.partition_point(|l| l.block_number <= b);
        Ok(rows[lo..hi].to_vec())
    }

    pub(super) fn forget_chunks_above(&self, block: u64) {
        // A reorg cannot reach a chunk the dataset serves, but a number is not an identity (see the
        // trait): drop anything that could be at or above it, so it is re-checked against the chain.
        let len = self.chunk_len;
        let mut st = self.state.lock().unwrap();
        st.ready.retain(|s, _| s + len - 1 <= block);
        let keep: HashSet<u64> = st.ready.keys().copied().collect();
        st.order.retain(|s| keep.contains(s));
        st.gone.retain(|s, _| s + len - 1 <= block);
    }
}
