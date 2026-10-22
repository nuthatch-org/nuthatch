//! RFC-0070: a nest's sealed history replayed as frames.
//!
//! Pure: no filesystem and no engine. A caller validates the spec's query, plans the pass from the
//! catalogue, runs [`Plan::chunk_sql`] once per chunk over that chunk's final segments (Burrmill on a
//! node or in a tab), and hands each Arrow batch to a [`Replay`]. The frames, and so their hash, are
//! the same wherever that happens.

pub mod batch;
pub mod fold;
pub mod manifest;
pub mod plan;
pub mod spec;
pub mod validate;

use alloy_primitives::I256;
use anyhow::{ensure, Result};
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};

pub use alloy_primitives;
pub use fold::{Fold, Key};
pub use manifest::{Identity, Manifest};
pub use plan::{Catalogue, Plan, Segment, Source};
pub use spec::{FoldKind, Spec};
pub use validate::{validate, Refusal, Shape};

/// Names the fold and the frame encoding. It sits in the manifest, not in the frames.
pub const FOLD_VERSION: &str = "nuthatch-timelapse/1";

/// One frame (§4.1): the state after every event at or below `block`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub block: u64,
    /// The newest folded event's `block_timestamp`, when the query returns one. A nest does not hold
    /// every block's timestamp, so this is not the closing block's (§13 finding 5).
    pub ts: Option<u64>,
    pub live: usize,
    pub total: String,
    pub top: Vec<(String, String)>,
}

impl Frame {
    /// One line of `frames.jsonl`. The field order and spelling are the hash: S0's recordings must
    /// still verify.
    pub fn line(&self, i: usize) -> String {
        let top: Vec<String> = self
            .top
            .iter()
            .map(|(k, v)| format!("[{},\"{v}\"]", serde_json::Value::String(k.clone())))
            .collect();
        let ts = self.ts.map_or("null".to_string(), |t| t.to_string());
        format!(
            "{{\"i\":{i},\"block\":{},\"ts\":{ts},\"live\":{},\"total\":\"{}\",\"top\":[{}]}}\n",
            self.block,
            self.live,
            self.total,
            top.join(",")
        )
    }
}

/// One row of the query, its sign already applied.
#[derive(Debug, Clone)]
pub struct Row<'a> {
    pub block: u64,
    pub log_index: u64,
    pub ts: Option<u64>,
    pub key: &'a str,
    pub amount: I256,
}

/// The fold over one pass, fed in block order a chunk at a time.
pub struct Replay {
    kind: FoldKind,
    top: usize,
    closes: Vec<u64>,
    shape: Shape,
    fold: Fold,
    frames: Vec<Frame>,
    ts: Option<u64>,
    last: Option<(u64, u64)>,
    /// For `last`: the keys already set at `last`, where a second value would make the order matter.
    at_last: Vec<Key>,
    chunk: (u64, u64),
}

impl Replay {
    pub fn new(spec: &Spec, plan: &Plan, shape: &Shape) -> Replay {
        Replay {
            kind: spec.quantity.fold,
            top: spec.top,
            closes: plan.closes.clone(),
            shape: shape.clone(),
            fold: Fold::new(spec.quantity.fold),
            frames: Vec::with_capacity(plan.closes.len()),
            ts: None,
            last: None,
            at_last: Vec::new(),
            chunk: (plan.start, plan.end),
        }
    }

    /// Rows that follow must lie in `lo..=hi`.
    pub fn begin_chunk(&mut self, lo: u64, hi: u64) {
        self.chunk = (lo, hi);
    }

    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        use batch::{column, Sign, Text, Unsigned};
        let blocks = Unsigned::of(column(batch, "block_number")?, "block_number")?;
        let logs = Unsigned::of(column(batch, "log_index")?, "log_index")?;
        let keys = Text::of(column(batch, "key")?, "key")?;
        let name = self.kind.amount_column();
        let amounts = Text::of(column(batch, name)?, name)?;
        let signs = match self.shape.has_sign {
            true => Some(Sign::of(column(batch, "sign")?)?),
            false => None,
        };
        let stamps = match self.shape.has_timestamp {
            true => Some(Unsigned::of(
                column(batch, "block_timestamp")?,
                "block_timestamp",
            )?),
            false => None,
        };
        for i in 0..batch.num_rows() {
            let null = |c: &str| anyhow::anyhow!("row {i} has a null or negative `{c}`");
            let mut amount = batch::amount(amounts.get(i).ok_or_else(|| null(name))?)?;
            if let Some(s) = &signs {
                amount = s.apply(i, amount)?;
            }
            let ts = match &stamps {
                Some(s) => Some(s.get(i).ok_or_else(|| null("block_timestamp"))?),
                None => None,
            };
            self.push(Row {
                block: blocks.get(i).ok_or_else(|| null("block_number"))?,
                log_index: logs.get(i).ok_or_else(|| null("log_index"))?,
                ts,
                key: keys.get(i).ok_or_else(|| null("key"))?,
                amount,
            })?;
        }
        Ok(())
    }

    pub fn push(&mut self, row: Row<'_>) -> Result<()> {
        let (b, li) = (row.block, row.log_index);
        let (lo, hi) = self.chunk;
        ensure!(
            b >= lo && b <= hi,
            "row at block {b} outside chunk {lo}..={hi}"
        );
        let at = Some((b, li));
        ensure!(at >= self.last, "rows out of order at block {b}");
        while self.frames.len() < self.closes.len() && self.closes[self.frames.len()] < b {
            self.emit();
        }
        if row.ts.is_some() {
            self.ts = row.ts;
        }
        let key = Key::parse(row.key);
        if self.kind == FoldKind::Last {
            if at != self.last {
                self.at_last.clear();
            }
            ensure!(
                !self.at_last.contains(&key),
                "key {} has two values at block {b} log {li}; a `last` fold needs one",
                row.key
            );
            self.at_last.push(key.clone());
        }
        self.last = at;
        self.fold.apply(key, row.amount)
    }

    fn emit(&mut self) {
        let block = self.closes[self.frames.len()];
        self.frames.push(Frame {
            block,
            ts: self.ts,
            live: self.fold.live(),
            total: self.fold.total().to_string(),
            top: self.fold.top(self.top),
        });
    }

    pub fn fold(&self) -> &Fold {
        &self.fold
    }

    pub fn finish(mut self) -> Recording {
        while self.frames.len() < self.closes.len() {
            self.emit();
        }
        let jsonl: String = self
            .frames
            .iter()
            .enumerate()
            .map(|(i, f)| f.line(i))
            .collect();
        Recording {
            frames_sha256: sha256_hex(jsonl.as_bytes()),
            jsonl,
            frames: self.frames,
            events: self.fold.events,
            live_keys_end: self.fold.live(),
            live_keys_peak: self.fold.peak_keys,
        }
    }
}

pub struct Recording {
    pub jsonl: String,
    pub frames_sha256: String,
    pub frames: Vec<Frame>,
    pub events: u64,
    pub live_keys_end: usize,
    pub live_keys_peak: usize,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests;
