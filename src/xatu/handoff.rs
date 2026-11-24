//! The [`Source`] impl: the handoff between the dataset and the RPC, request by request.

use super::chunks::{Distrusted, Unchecked};
use super::{Log, XatuSource};
use crate::source::{LogFilter, Source};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

#[async_trait::async_trait]
impl Source for XatuSource {
    async fn tip(&self) -> Result<u64> {
        self.inner.tip().await
    }

    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        if let Some(c) = self.cached(number) {
            self.stats.header_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(c.hashes[(number - c.start) as usize].clone()));
        }
        self.inner.block_hash(number).await
    }

    async fn block_hashes(&self, blocks: &[u64]) -> Result<HashMap<u64, String>> {
        let mut out = HashMap::new();
        let mut rest = Vec::new();
        for &b in blocks {
            match self.cached(b) {
                Some(c) => {
                    out.insert(b, c.hashes[(b - c.start) as usize].clone());
                }
                None => rest.push(b),
            }
        }
        self.stats
            .header_hits
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        if !rest.is_empty() {
            out.extend(self.inner.block_hashes(&rest).await?);
        }
        Ok(out)
    }

    async fn finalized(&self) -> Result<Option<u64>> {
        self.inner.finalized().await
    }

    async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        let mut out = HashMap::new();
        let mut rest = Vec::new();
        for &b in blocks {
            match self.cached(b) {
                Some(c) => {
                    out.insert(b, c.timestamps[(b - c.start) as usize]);
                }
                None => rest.push(b),
            }
        }
        self.stats
            .header_hits
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        if !rest.is_empty() {
            out.extend(self.inner.block_timestamps(&rest).await?);
        }
        Ok(out)
    }

    async fn block_record(&self, number: u64) -> Result<Option<(String, Option<u64>)>> {
        if let Some(c) = self.cached(number) {
            let i = (number - c.start) as usize;
            self.stats.header_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((c.hashes[i].clone(), Some(c.timestamps[i]))));
        }
        self.inner.block_record(number).await
    }

    async fn block_records(&self, blocks: &[u64]) -> Result<HashMap<u64, (String, Option<u64>)>> {
        let mut out = HashMap::new();
        let mut rest = Vec::new();
        for &b in blocks {
            match self.cached(b) {
                Some(c) => {
                    let i = (b - c.start) as usize;
                    out.insert(b, (c.hashes[i].clone(), Some(c.timestamps[i])));
                }
                None => rest.push(b),
            }
        }
        self.stats
            .header_hits
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        if !rest.is_empty() {
            out.extend(self.inner.block_records(&rest).await?);
        }
        Ok(out)
    }

    async fn block_headers(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        self.inner.block_headers(blocks).await
    }

    async fn block_bodies(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        self.inner.block_bodies(blocks).await
    }

    fn forget_cached_above(&self, block: u64) {
        self.inner.forget_cached_above(block);
        self.forget_chunks_above(block);
    }

    fn set_log_group_fanout(&self, n: usize) {
        self.inner.set_log_group_fanout(n);
    }

    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
        if from > to {
            // Not a range this splits; whatever the RPC says to it is the answer.
            return self.inner.logs(filter, from, to).await;
        }
        let mut out: Vec<Log> = Vec::new();
        // The RPC run being gathered: contiguous parts the dataset did not serve.
        let mut run: Option<(u64, u64)> = None;
        let mut s = self.start_of(from);
        while s <= to {
            let (a, b) = (from.max(s), to.min(s + self.chunk_len - 1));
            let served = match self.chunk(s).await {
                Some(c) => match self.read_chunk(&c, filter, a, b).await {
                    Ok(rows) => Some(rows),
                    // The RPC would not answer the spot check: this window from the RPC, and
                    // the check again next time.
                    Err(e) if e.is::<Unchecked>() => {
                        self.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            "xatu: could not spot-check blocks {a}..={b} ({e:#}); asking the RPC \
                             for them"
                        );
                        None
                    }
                    // Another window's check failed while this one read: already logged.
                    Err(e) if e.is::<Distrusted>() => {
                        self.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                    Err(e) => {
                        self.mark_bad(s, &e);
                        None
                    }
                },
                None => None,
            };
            match served {
                Some(rows) => {
                    if let Some((ra, rb)) = run.take() {
                        out.extend(self.inner.logs(filter, ra, rb).await?);
                    }
                    out.extend(rows);
                }
                None => run = Some((run.map_or(a, |r| r.0), b)),
            }
            s += self.chunk_len;
        }
        if let Some((ra, rb)) = run {
            out.extend(self.inner.logs(filter, ra, rb).await?);
        }
        Ok(out)
    }

    fn refuses_topic0_only(&self, err: &anyhow::Error) -> bool {
        self.inner.refuses_topic0_only(err)
    }
}
