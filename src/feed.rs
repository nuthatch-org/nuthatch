//! The hot-state feed (RFC-0073 FR1): what a read-only `serve --follow` process keeps its own copy of
//! a writer's hot window from.
//!
//! The writer side is a [`Tap`] inside every redb [`crate::store::Store`]. Each commit that changes
//! the mirrored tables (`entities`, `meta`, `blocks`) is numbered with the next `generation` and,
//! while anything is subscribed, published as a [`Delta`] carrying the same changes as [`Op`]s. A
//! subscriber takes a [`Snapshot`] at generation G under the same lock the commits publish under, so
//! the deltas it then receives start at exactly G+1.
//!
//! The reader side is [`MemStore`], an in-memory [`HotStore`] that applies a snapshot and then the
//! deltas in order, and refuses one that does not follow (a [`Gap`]).
//!
//! **The seal handoff.** A seal writes its segment and catalogue, then prunes the sealed rows from
//! hot and raises `sealed_through` in one commit. The delta for that commit carries the prune, the new
//! watermark and the catalogue as it stood at the commit, so a reader moves from "rows in hot" to
//! "rows in a segment under its watermark" in one apply. Its catalogue comes from the feed, pinned
//! with [`crate::seal::pin_catalogue`], never from the disk, which may already be a seal ahead.

use crate::store::{HotRowsSnapshot, HotScanBudgetExceeded, HotScanTooLarge, HotStore, Lease};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// Deltas one subscriber may fall behind by before it is told to resnapshot. Past this the writer
/// drops what it would have sent rather than wait: a slow reader never holds up a commit.
pub const DELTA_BACKLOG: usize = 1024;

/// One change to the mirrored tables, in the order the commit made it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    Put {
        key: String,
        row: String,
    },
    /// Entity keys in `from..=to`, or from `from` to the end when `to` is absent.
    Drop {
        from: String,
        to: Option<String>,
    },
    Meta {
        key: String,
        value: String,
    },
    Unmeta {
        key: String,
    },
    Block {
        key: String,
        record: String,
    },
    Unblock {
        key: String,
    },
    /// Every checkpoint at or above `key`.
    UnblockFrom {
        key: String,
    },
}

/// Meta keys that describe the writer's own process rather than the data: its outbox counter and its
/// ownership lease. Neither is mirrored, so a reader cannot be mistaken for the owner.
pub fn mirrored_meta(key: &str) -> bool {
    !matches!(
        key,
        crate::store::OWNER_FENCE
            | crate::store::LEASE_OWNER
            | crate::store::LEASE_EXPIRES_AT
            | "owner"
            | "outbox_next_seq"
    )
}

/// Ops gathered inside one write transaction, or `None` when nothing is subscribed and gathering
/// them would only copy rows nobody reads.
pub struct Recorder(Option<Vec<Op>>);

impl Recorder {
    pub fn push(&mut self, op: impl FnOnce() -> Op) {
        if let Some(ops) = &mut self.0 {
            let op = op();
            if let Op::Meta { key, .. } | Op::Unmeta { key } = &op {
                if !mirrored_meta(key) {
                    return;
                }
            }
            ops.push(op);
        }
    }
}

/// A committed change to one dataset's hot store, numbered within its writer's `epoch`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delta {
    pub epoch: u64,
    pub generation: u64,
    #[serde(default)]
    pub ops: Vec<Op>,
    /// The catalogue as it stood at this commit, present on every commit that moves `sealed_through`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalogue: Option<String>,
    /// The commit happened but its ops were not gathered (a subscriber arrived mid-transaction). The
    /// subscriber must resnapshot; the serving side does that before a reader ever sees one.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resync: bool,
}

/// A dataset's hot window as of one generation, read in one transaction.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Snapshot {
    pub epoch: u64,
    pub generation: u64,
    pub entities: Vec<(String, String)>,
    pub meta: Vec<(String, String)>,
    pub blocks: Vec<(String, String)>,
    /// The catalogue's raw bytes, `None` where the dataset has never sealed.
    pub catalogue: Option<String>,
}

/// A snapshot and the deltas that follow it.
pub struct Subscription {
    pub snapshot: Snapshot,
    pub deltas: tokio::sync::broadcast::Receiver<Arc<Delta>>,
}

#[derive(Default)]
struct TapState {
    generation: u64,
}

/// The writer's half of the feed, one per open store.
pub struct Tap {
    state: Mutex<TapState>,
    tx: tokio::sync::broadcast::Sender<Arc<Delta>>,
    epoch: u64,
    /// The dataset directory, whose catalogue a sealing commit carries.
    dir: Option<PathBuf>,
}

/// Distinct for every store opened by any process, so a reader cannot take a restarted writer's
/// generation 5 for the one it applied before the restart.
fn new_epoch() -> u64 {
    static OPENS: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let n = OPENS.fetch_add(1, Ordering::Relaxed);
    nanos ^ (u64::from(std::process::id()) << 40) ^ n.rotate_left(20)
}

impl Tap {
    pub fn new(dir: Option<PathBuf>) -> Tap {
        Tap::with_backlog(dir, DELTA_BACKLOG)
    }

    pub fn with_backlog(dir: Option<PathBuf>, backlog: usize) -> Tap {
        let (tx, _) = tokio::sync::broadcast::channel(backlog.max(1));
        Tap {
            state: Mutex::default(),
            tx,
            epoch: new_epoch(),
            dir,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The generation of the last mirrored commit.
    pub fn generation(&self) -> u64 {
        self.lock().generation
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TapState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn recorder(&self) -> Recorder {
        Recorder((self.tx.receiver_count() > 0).then(Vec::new))
    }

    /// Run `commit` and number it, publishing its ops to any subscriber. Holding the lock across the
    /// commit is what puts every delta strictly after or strictly before a snapshot.
    pub fn commit(&self, rec: Recorder, commit: impl FnOnce() -> Result<()>) -> Result<()> {
        let mut state = self.lock();
        commit()?;
        state.generation += 1;
        if self.tx.receiver_count() == 0 {
            return Ok(());
        }
        let delta = match rec.0 {
            Some(ops) => {
                let seals = ops
                    .iter()
                    .any(|op| matches!(op, Op::Meta { key, .. } if key == "sealed_through"));
                Delta {
                    epoch: self.epoch,
                    generation: state.generation,
                    catalogue: if seals { self.catalogue() } else { None },
                    ops,
                    resync: false,
                }
            }
            None => Delta {
                epoch: self.epoch,
                generation: state.generation,
                ops: Vec::new(),
                catalogue: None,
                resync: true,
            },
        };
        let _ = self.tx.send(Arc::new(delta));
        Ok(())
    }

    /// Subscribe, then read the snapshot with `read` while no commit can land: the receiver gets
    /// exactly the deltas after the generation returned.
    pub fn subscribe<T>(
        &self,
        read: impl FnOnce() -> Result<T>,
    ) -> Result<(tokio::sync::broadcast::Receiver<Arc<Delta>>, u64, T)> {
        let state = self.lock();
        let rx = self.tx.subscribe();
        let read = read()?;
        Ok((rx, state.generation, read))
    }

    /// The catalogue on disk now.
    pub fn catalogue(&self) -> Option<String> {
        let dir = self.dir.as_ref()?;
        std::fs::read_to_string(
            dir.join(crate::seal::SEGMENTS_DIR)
                .join(crate::seal::MANIFEST_FILE),
        )
        .ok()
    }
}

/// Whether catalogue `raw` names a segment above `sealed_through`. A seal saves its catalogue a
/// moment before the commit that raises the watermark, so such a catalogue belongs to a seal still in
/// flight and must not be paired with that watermark.
pub fn catalogue_ahead(raw: Option<&str>, sealed_through: u64) -> bool {
    raw.and_then(|r| serde_json::from_str::<crate::seal::Manifest>(r).ok())
        .is_some_and(|m| {
            m.tables
                .values()
                .flatten()
                .any(|s| s.to_block > sealed_through)
        })
}

/// A delta that does not follow the last generation applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gap {
    pub applied: (u64, u64),
    pub got: (u64, u64),
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "feed gap: applied generation {} of epoch {:x}, received {} of epoch {:x}",
            self.applied.1, self.applied.0, self.got.1, self.got.0
        )
    }
}

impl std::error::Error for Gap {}

#[derive(Default)]
struct Mirror {
    entities: BTreeMap<String, String>,
    meta: BTreeMap<String, String>,
    blocks: BTreeMap<String, String>,
    epoch: u64,
    generation: u64,
}

impl Mirror {
    fn apply(&mut self, op: &Op) -> bool {
        match op {
            Op::Put { key, row } => {
                self.entities.insert(key.clone(), row.clone());
                true
            }
            Op::Drop { from, to } => {
                let doomed: Vec<String> = match to {
                    Some(to) => self
                        .entities
                        .range(from.clone()..=to.clone())
                        .map(|(k, _)| k.clone())
                        .collect(),
                    None => self
                        .entities
                        .range(from.clone()..)
                        .map(|(k, _)| k.clone())
                        .collect(),
                };
                for k in &doomed {
                    self.entities.remove(k);
                }
                !doomed.is_empty()
            }
            Op::Meta { key, value } => {
                self.meta.insert(key.clone(), value.clone());
                false
            }
            Op::Unmeta { key } => {
                self.meta.remove(key);
                false
            }
            Op::Block { key, record } => {
                self.blocks.insert(key.clone(), record.clone());
                false
            }
            Op::Unblock { key } => {
                self.blocks.remove(key);
                false
            }
            Op::UnblockFrom { key } => {
                self.blocks.split_off(key);
                false
            }
        }
    }
}

/// Where a reader's rows generations start, distinct for every [`MemStore`] the process builds,
/// because the `/sql` memo outlives a store swapped in by a resnapshot.
fn rows_epoch() -> u64 {
    static BUILT: AtomicU64 = AtomicU64::new(1 << 20);
    BUILT.fetch_add(1, Ordering::Relaxed) << 32
}

/// A reader's copy of one dataset's hot window, built from a [`Snapshot`] and kept current by
/// [`MemStore::apply`]. Every write method of [`HotStore`] refuses: the writer owns the data.
pub struct MemStore {
    mirror: RwLock<Mirror>,
    writes: AtomicU64,
    rows: AtomicU64,
    /// The catalogue this store's watermark pairs with, pinned for its dataset directory.
    dir: PathBuf,
}

impl MemStore {
    pub fn from_snapshot(dir: &Path, snapshot: Snapshot) -> MemStore {
        let mirror = Mirror {
            entities: snapshot.entities.into_iter().collect(),
            meta: snapshot
                .meta
                .into_iter()
                .filter(|(k, _)| mirrored_meta(k))
                .collect(),
            blocks: snapshot.blocks.into_iter().collect(),
            epoch: snapshot.epoch,
            generation: snapshot.generation,
        };
        crate::seal::pin_catalogue(dir, snapshot.catalogue);
        MemStore {
            mirror: RwLock::new(mirror),
            writes: AtomicU64::new(rows_epoch()),
            rows: AtomicU64::new(rows_epoch()),
            dir: dir.to_path_buf(),
        }
    }

    /// `(epoch, generation)` of the last delta applied.
    pub fn applied(&self) -> (u64, u64) {
        let m = self.read();
        (m.epoch, m.generation)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Mirror> {
        self.mirror.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Apply the next delta, or refuse it as a [`Gap`] and change nothing.
    pub fn apply(&self, delta: &Delta) -> Result<(), Gap> {
        let mut m = self.mirror.write().unwrap_or_else(|e| e.into_inner());
        if delta.resync || delta.epoch != m.epoch || delta.generation != m.generation + 1 {
            return Err(Gap {
                applied: (m.epoch, m.generation),
                got: (delta.epoch, delta.generation),
            });
        }
        // Bracketed odd while the change lands, as `Store::commit` is, so the memo's fence holds.
        self.writes.fetch_add(1, Ordering::SeqCst);
        let changes_rows = delta
            .ops
            .iter()
            .any(|op| matches!(op, Op::Put { .. } | Op::Drop { .. }));
        if changes_rows {
            self.rows.fetch_add(1, Ordering::SeqCst);
        }
        for op in &delta.ops {
            m.apply(op);
        }
        if let Some(raw) = &delta.catalogue {
            crate::seal::pin_catalogue(&self.dir, Some(raw.clone()));
        }
        m.generation = delta.generation;
        if changes_rows {
            self.rows.fetch_add(1, Ordering::SeqCst);
        }
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Every mirrored table, for comparing a copy against the store it follows.
    pub fn tables(&self) -> MirroredTables {
        let m = self.read();
        MirroredTables {
            entities: m.entities.clone(),
            meta: m.meta.clone(),
            blocks: m.blocks.clone(),
        }
    }
}

/// The three mirrored tables, in key order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MirroredTables {
    pub entities: BTreeMap<String, String>,
    pub meta: BTreeMap<String, String>,
    pub blocks: BTreeMap<String, String>,
}

impl MirroredTables {
    pub fn of(snapshot: &Snapshot) -> MirroredTables {
        MirroredTables {
            entities: snapshot.entities.iter().cloned().collect(),
            meta: snapshot
                .meta
                .iter()
                .filter(|(k, _)| mirrored_meta(k))
                .cloned()
                .collect(),
            blocks: snapshot.blocks.iter().cloned().collect(),
        }
    }
}

fn refuse<T>() -> Result<T> {
    bail!("this process follows a writer (`serve --follow`) and never writes its hot store")
}

fn block_of(key: &str) -> Result<u64> {
    key.get(..12)
        .and_then(|b| b.parse::<u64>().ok())
        .with_context(|| format!("corrupt entity key {key:?}"))
}

#[async_trait::async_trait]
impl HotStore for MemStore {
    fn write_generation(&self) -> Option<u64> {
        let g = self.writes.load(Ordering::SeqCst);
        g.is_multiple_of(2).then_some(g)
    }

    fn rows_generation(&self) -> Option<u64> {
        let g = self.rows.load(Ordering::SeqCst);
        g.is_multiple_of(2).then_some(g)
    }

    fn put_entity(&self, _key: &str, _json: &str) -> Result<()> {
        refuse()
    }
    fn put_entity_if_named(&self, _: &str, _: &str, _: &str, _: &str) -> Result<bool> {
        refuse()
    }
    fn put_entities_if_named(&self, _: &[(String, String)], _: &str, _: &str) -> Result<bool> {
        refuse()
    }
    fn get_entity(&self, key: &str) -> Result<Option<String>> {
        Ok(self.read().entities.get(key).cloned())
    }
    fn count(&self) -> Result<u64> {
        Ok(self.read().entities.len() as u64)
    }
    fn recent(&self, limit: usize) -> Result<Vec<String>> {
        Ok(self
            .read()
            .entities
            .values()
            .rev()
            .take(limit)
            .cloned()
            .collect())
    }
    fn recent_by_table(&self, table: &str, limit: usize) -> Result<Vec<String>> {
        Ok(self
            .recent_by_table_within(table, limit, usize::MAX)?
            .unwrap_or_default())
    }
    fn recent_by_table_within(
        &self,
        table: &str,
        limit: usize,
        max_scanned: usize,
    ) -> Result<Option<Vec<String>>> {
        let m = self.read();
        let mut out = Vec::with_capacity(limit.min(1024));
        for (scanned, v) in m.entities.values().rev().enumerate() {
            if out.len() >= limit {
                break;
            }
            if scanned >= max_scanned {
                return Ok(None);
            }
            if crate::store::row_table_is(v, table).with_context(|| {
                format!("unparseable hot row while scanning for table '{table}'")
            })? {
                out.push(v.clone());
            }
        }
        Ok(Some(out))
    }
    fn hot_rows_by_table(&self) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        self.hot_rows_by_table_bounded(usize::MAX)
    }
    fn hot_rows_by_table_bounded(
        &self,
        max_rows: usize,
    ) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        Ok(self
            .hot_rows_by_table_bounded_with_bytes(max_rows, u64::MAX)?
            .rows)
    }
    fn hot_rows_by_table_bounded_with_bytes(
        &self,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<HotRowsSnapshot> {
        let m = self.read();
        let mut out = HotRowsSnapshot {
            last_block: m.meta.get("last_block").and_then(|v| v.parse().ok()),
            ..Default::default()
        };
        let mut seen = 0usize;
        for v in m.entities.values() {
            out.source_rows += 1;
            out.source_bytes = out
                .source_bytes
                .checked_add(v.len() as u64)
                .context("hot source byte count overflow")?;
            if out.source_bytes > max_bytes {
                return Err(HotScanBudgetExceeded {
                    source_bytes: out.source_bytes,
                    budget: max_bytes,
                }
                .into());
            }
            let json = serde_json::from_str::<serde_json::Value>(v)
                .context("unparseable hot row in an analytical scan")?;
            if let Some(table) = json.get("table").and_then(|t| t.as_str()) {
                seen += 1;
                if seen > max_rows {
                    return Err(HotScanTooLarge { cap: max_rows }.into());
                }
                out.rows.entry(table.to_string()).or_default().push(json);
            }
        }
        Ok(out)
    }
    fn entities_in_range(&self, from: u64, to: u64) -> Result<Vec<String>> {
        let lo = format!("{from:012}-000000");
        let hi = format!("{to:012}-999999");
        Ok(self
            .read()
            .entities
            .range(lo..=hi)
            .map(|(_, v)| v.clone())
            .collect())
    }
    fn scan_entities_in_range(
        &self,
        from: u64,
        to: u64,
        visit: &mut dyn FnMut(u64, &str) -> bool,
    ) -> Result<()> {
        let lo = format!("{from:012}-000000");
        let hi = format!("{to:012}-999999");
        let m = self.read();
        for (k, v) in m.entities.range(lo..=hi) {
            if !visit(block_of(k)?, v) {
                break;
            }
        }
        Ok(())
    }
    fn sample_entity_keys(&self, limit: usize) -> Result<Vec<String>> {
        Ok(self.read().entities.keys().take(limit).cloned().collect())
    }

    fn get_meta(&self, key: &str) -> Result<Option<String>> {
        Ok(self.read().meta.get(key).cloned())
    }
    fn set_meta(&self, _key: &str, _value: &str) -> Result<()> {
        refuse()
    }
    fn meta_with_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
        Ok(self
            .read()
            .meta
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    fn count_meta_with_prefix(&self, prefix: &str) -> Result<u64> {
        Ok(self
            .read()
            .meta
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .count() as u64)
    }
    fn indexed_head(&self) -> Result<Option<u64>> {
        let hot = self
            .get_meta("last_block")?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let head = hot.max(self.sealed_through());
        Ok((head > 0).then_some(head))
    }
    fn sealed_through(&self) -> u64 {
        self.read()
            .meta
            .get("sealed_through")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }
    fn set_block_hash(&self, _block: u64, _hash: &str) -> Result<()> {
        refuse()
    }
    fn get_block_hash(&self, block: u64) -> Result<Option<String>> {
        Ok(self
            .read()
            .blocks
            .get(&format!("{block:012}"))
            .map(|v| crate::store::decode_block_record(v).0)
            .filter(|h| !h.is_empty()))
    }
    fn set_block_timestamp(&self, _block: u64, _timestamp: u64) -> Result<()> {
        refuse()
    }
    fn get_block_timestamp(&self, block: u64) -> Result<Option<u64>> {
        Ok(self
            .read()
            .blocks
            .get(&format!("{block:012}"))
            .and_then(|v| crate::store::decode_block_record(v).1))
    }
    fn checkpoints_desc(&self) -> Result<Vec<(u64, String)>> {
        self.read()
            .blocks
            .iter()
            .rev()
            .map(|(k, v)| {
                Ok((
                    k.parse().context("corrupt block key")?,
                    crate::store::decode_block_record(v).0,
                ))
            })
            .collect()
    }
    fn checkpoint_number(&self, hash: &str) -> Result<Option<u64>> {
        if !cfg!(feature = "graph") {
            bail!("hash-pinned reads need a nuthatch built with `--features graph`");
        }
        let want = hash.to_ascii_lowercase();
        for (k, v) in self.read().blocks.iter() {
            if crate::store::decode_block_record(v).0.to_ascii_lowercase() == want {
                return Ok(Some(k.parse().context("corrupt block key")?));
            }
        }
        Ok(None)
    }

    fn commit_window(&self, _: &[(String, String)], _: Option<(u64, &str)>, _: u64) -> Result<()> {
        refuse()
    }
    async fn commit_window_blocking(
        &self,
        _: Vec<(String, String)>,
        _: Vec<String>,
        _: Option<(u64, String)>,
        _: u64,
    ) -> Result<()> {
        refuse()
    }
    async fn settle_commits(&self) {}
    fn rollback_to(&self, _block: u64) -> Result<u64> {
        refuse()
    }
    fn rollback_to_and_set_meta(&self, _: u64, _: &str, _: &str, _: &[String]) -> Result<u64> {
        refuse()
    }
    fn prune_range(&self, _from: u64, _to: u64) -> Result<u64> {
        refuse()
    }
    fn prune_and_set_meta(&self, _: u64, _: u64, _: &str, _: &str) -> Result<u64> {
        refuse()
    }

    fn claim(&self, _owner: &str) -> Result<u64> {
        refuse()
    }
    fn acquire_lease(&self, _owner: &str, _ttl_secs: u64) -> Result<Lease> {
        refuse()
    }
    fn renew_lease(&self, _ttl_secs: u64) -> Result<Lease> {
        refuse()
    }
    fn release_lease(&self) -> Result<()> {
        refuse()
    }
    fn current_lease(&self) -> Result<Option<Lease>> {
        Ok(None)
    }
    fn current_fence(&self) -> Result<u64> {
        Ok(0)
    }
    fn held_fence(&self) -> u64 {
        0
    }

    fn outbox_push(&self, _payload: &str) -> Result<u64> {
        refuse()
    }
    fn outbox_pending(&self, _limit: usize) -> Result<Vec<(u64, String)>> {
        Ok(Vec::new())
    }
    fn outbox_remove(&self, _seq: u64) -> Result<()> {
        refuse()
    }
    async fn outbox_remove_batch_blocking(&self, _seqs: Vec<u64>) -> Result<()> {
        refuse()
    }
    fn outbox_len(&self) -> u64 {
        0
    }
    fn outbox_trim(&self, _max: u64) -> Result<u64> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    fn row(block: u64, log: u64, table: &str) -> (String, String) {
        (
            Store::entity_key(block, log),
            serde_json::json!({"table": table, "block_number": block, "log_index": log})
                .to_string(),
        )
    }

    fn drain(sub: &mut Subscription, into: &MemStore) {
        while let Ok(delta) = sub.deltas.try_recv() {
            into.apply(&delta).expect("deltas follow one another");
        }
    }

    fn write_catalogue(dir: &Path, to_block: u64) {
        let segs = dir.join(crate::seal::SEGMENTS_DIR);
        std::fs::create_dir_all(&segs).unwrap();
        std::fs::write(
            segs.join(crate::seal::MANIFEST_FILE),
            serde_json::json!({"tables": {"t": [{"hash": format!("h{to_block}"), "from_block": 1,
                "to_block": to_block, "rows": 1, "file": format!("t-h{to_block}.parquet")}]}})
            .to_string(),
        )
        .unwrap();
    }

    /// Every write path the store has, followed through the feed, leaves the copy equal to a fresh
    /// snapshot of the store and answering reads the same way.
    #[test]
    fn a_follower_converges_on_the_store_through_every_write_path() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        store
            .commit_window(&[row(1, 0, "a")], Some((1, "0x01")), 1)
            .unwrap();
        let mut sub = store.feed_subscribe().unwrap();
        let copy = MemStore::from_snapshot(tmp.path(), sub.snapshot.clone());

        store
            .commit_window(&[row(2, 0, "a"), row(3, 1, "b")], Some((3, "0x03")), 3)
            .unwrap();
        let (k, v) = row(4, 0, "b");
        store.put_entity(&k, &v).unwrap();
        store.set_block_hash(4, "0x04").unwrap();
        store.set_block_timestamp(4, 1_700_000_000).unwrap();
        store.set_metas(&[("k", "v"), ("j", "w")], &["j"]).unwrap();
        store.outbox_push("alert").unwrap();
        store
            .rollback_to_and_set_meta(2, "last_block", "2", &[])
            .unwrap();
        store
            .commit_window(&[row(3, 0, "c"), row(5, 0, "c")], Some((5, "0x05")), 5)
            .unwrap();
        write_catalogue(tmp.path(), 3);
        store
            .prune_and_set_meta(0, 3, "sealed_through", "3")
            .unwrap();
        store.rollback_to(4).unwrap();
        drain(&mut sub, &copy);

        let now = store.feed_subscribe().unwrap().snapshot;
        assert_eq!(copy.applied(), (now.epoch, now.generation));
        assert_eq!(copy.tables(), MirroredTables::of(&now));
        assert!(
            !copy.tables().meta.contains_key("outbox_next_seq"),
            "the writer's outbox is not mirrored"
        );
        let ours = HotStore::hot_rows_by_table_bounded_with_bytes(&copy, 100, u64::MAX).unwrap();
        let theirs = store
            .hot_rows_by_table_bounded_with_bytes(100, u64::MAX)
            .unwrap();
        assert_eq!(ours.rows, theirs.rows);
        assert_eq!(ours.source_bytes, theirs.source_bytes);
        assert_eq!(ours.last_block, theirs.last_block);
        assert_eq!(HotStore::sealed_through(&copy), 3);
        assert_eq!(
            HotStore::checkpoints_desc(&copy).unwrap(),
            store.checkpoints_desc().unwrap()
        );
        assert_eq!(
            HotStore::get_block_timestamp(&copy, 4).unwrap(),
            store.get_block_timestamp(4).unwrap()
        );
        assert_eq!(
            HotStore::recent_by_table(&copy, "c", 10).unwrap(),
            store.recent_by_table("c", 10).unwrap()
        );
        crate::seal::unpin_catalogue(tmp.path());
    }

    /// The seal's prune, its watermark and its catalogue arrive in one delta.
    #[test]
    fn the_sealing_delta_carries_its_catalogue() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        store
            .commit_window(&[row(1, 0, "t"), row(2, 0, "t")], None, 2)
            .unwrap();
        let mut sub = store.feed_subscribe().unwrap();
        assert_eq!(sub.snapshot.catalogue, None);
        let copy = MemStore::from_snapshot(tmp.path(), sub.snapshot.clone());
        write_catalogue(tmp.path(), 1);
        store
            .prune_and_set_meta(1, 1, "sealed_through", "1")
            .unwrap();
        let delta = sub.deltas.try_recv().unwrap();
        assert!(delta.catalogue.as_deref().is_some_and(|c| c.contains("h1")));
        assert!(delta.ops.contains(&Op::Meta {
            key: "sealed_through".into(),
            value: "1".into()
        }));
        copy.apply(&delta).unwrap();
        // The disk moves on; the follower's catalogue stays the one its watermark pairs with.
        write_catalogue(tmp.path(), 9);
        let (pinned, _) = crate::seal::load_manifest_with_hash(tmp.path()).unwrap();
        assert_eq!(pinned.tables["t"][0].to_block, 1);
        assert_eq!(HotStore::count(&copy).unwrap(), 1);
        crate::seal::unpin_catalogue(tmp.path());
    }

    /// A snapshot taken between a seal's catalogue and its commit waits for the commit.
    #[test]
    fn a_snapshot_is_not_paired_with_a_catalogue_ahead_of_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        store
            .commit_window(&[row(1, 0, "t"), row(2, 0, "t")], None, 2)
            .unwrap();
        write_catalogue(tmp.path(), 1);
        let sealer = {
            let store = store.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                store
                    .prune_and_set_meta(1, 1, "sealed_through", "1")
                    .unwrap();
            })
        };
        let snap = store.feed_subscribe().unwrap().snapshot;
        sealer.join().unwrap();
        let tables = MirroredTables::of(&snap);
        assert_eq!(
            tables.meta.get("sealed_through").map(String::as_str),
            Some("1")
        );
        assert_eq!(tables.entities.len(), 1);
    }

    #[test]
    fn a_delta_out_of_order_is_a_gap_and_changes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        let mut sub = store.feed_subscribe().unwrap();
        let copy = MemStore::from_snapshot(tmp.path(), sub.snapshot.clone());
        store.commit_window(&[row(1, 0, "t")], None, 1).unwrap();
        store.commit_window(&[row(2, 0, "t")], None, 2).unwrap();
        let _first = sub.deltas.try_recv().unwrap();
        let second = sub.deltas.try_recv().unwrap();
        let before = copy.tables();
        let gap = copy.apply(&second).unwrap_err();
        assert_eq!(gap.got.1, gap.applied.1 + 2);
        assert_eq!(copy.tables(), before);
        let other_epoch = Delta {
            epoch: second.epoch ^ 1,
            generation: copy.applied().1 + 1,
            ops: Vec::new(),
            catalogue: None,
            resync: false,
        };
        assert!(copy.apply(&other_epoch).is_err(), "a restarted writer");
        crate::seal::unpin_catalogue(tmp.path());
    }

    /// A commit whose ops were not gathered because nobody was subscribed when it began is published
    /// as a resync, never as an empty change.
    #[test]
    fn a_subscriber_arriving_mid_commit_is_told_to_resync() {
        let tap = Tap::new(None);
        let rec = tap.recorder();
        let (mut rx, generation, ()) = tap.subscribe(|| Ok(())).unwrap();
        tap.commit(rec, || Ok(())).unwrap();
        let delta = rx.try_recv().unwrap();
        assert!(delta.resync);
        assert_eq!(delta.generation, generation + 1);
    }

    /// Nothing is gathered or sent while nothing follows, and the numbering still moves.
    #[test]
    fn an_unfollowed_tap_numbers_commits_and_gathers_nothing() {
        let tap = Tap::new(None);
        let mut rec = tap.recorder();
        rec.push(|| panic!("an unfollowed commit must not build its ops"));
        tap.commit(rec, || Ok(())).unwrap();
        assert_eq!(tap.generation(), 1);
    }
}
