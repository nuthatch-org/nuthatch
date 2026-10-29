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
//! "rows in a segment under its watermark" in one apply. Its catalogue comes from the feed and is
//! held by the copy, never read from the disk, which may already be a seal ahead: a request reads the
//! catalogue of the copy it is served from ([`crate::seal::serving`]).

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

/// The wire format of `GET /_admin/feed`. Bump it on any change an older follower would misread.
pub const FEED_VERSION: u32 = 1;

pub use nuthatch_core::tap::{mirrored_meta, Op, Recorder};

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
    pub entities: Rows,
    pub meta: Rows,
    pub blocks: Rows,
    /// The catalogue's raw bytes, `None` where the dataset has never sealed.
    #[serde(default, deserialize_with = "bounded_catalogue")]
    pub catalogue: Option<String>,
    /// Set by [`read_lines`] when the snapshot came to more than its dataset's cap: the bytes it came
    /// to. None of it is then kept.
    #[serde(skip)]
    pub over: Option<u64>,
}

impl Snapshot {
    /// What a follower is charged for holding this snapshot, by [`row_bytes`] and the catalogue's
    /// length.
    pub fn bytes(&self) -> u64 {
        [&self.entities, &self.meta, &self.blocks]
            .iter()
            .flat_map(|r| r.map.iter())
            .map(|(k, v)| row_bytes(k, v))
            .sum::<u64>()
            + self.catalogue.as_ref().map_or(0, |c| c.len() as u64)
    }
}

/// What a follower charges one stored row beyond its key and value: the map's node and two string
/// headers, rounded up.
pub const ROW_OVERHEAD: u64 = 96;

/// The bytes a follower charges for holding `key` and `row`.
pub fn row_bytes(key: &str, row: &str) -> u64 {
    (key.len() + row.len()) as u64 + ROW_OVERHEAD
}

/// One of a snapshot's tables (entity rows, meta, block checkpoints), sent as one JSON object and
/// read into the map directly. All of a snapshot's tables are read against the one cap
/// [`read_lines`] sets for its dataset: past it, entries are counted and dropped rather than kept.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rows {
    pub map: BTreeMap<String, String>,
}

impl Serialize for Rows {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.map.serialize(s)
    }
}

/// The snapshot being read on this thread: its dataset's cap, set by its name ahead of it, and the
/// bytes its tables and catalogue have come to so far.
#[derive(Clone, Copy, Default)]
struct Budget {
    cap: Option<u64>,
    seen: u64,
}

impl Budget {
    fn over(self) -> bool {
        self.cap.is_some_and(|cap| self.seen > cap)
    }
}

thread_local! {
    static BUDGET: std::cell::Cell<Budget> = const {
        std::cell::Cell::new(Budget { cap: None, seen: 0 })
    };
    /// What [`read_lines`] was given to work a dataset's cap out with.
    static CAP_FOR: std::cell::RefCell<Option<CapFor>> = const { std::cell::RefCell::new(None) };
}

/// Count `bytes` against the snapshot being read; whether it is still within its cap.
fn charge_read(bytes: u64) -> bool {
    BUDGET.with(|b| {
        let mut now = b.get();
        now.seen += bytes;
        b.set(now);
        !now.over()
    })
}

/// Which kind of line a dataset's cap is asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A whole snapshot, replacing what is held: its cap is the dataset's budget.
    Snapshot,
    /// A delta, added to what is held: its cap is what the dataset has left.
    Delta,
}

/// The most a dataset's line of `Kind` may come to, `None` for no cap.
pub type CapFor = Box<dyn Fn(&str, Kind) -> Option<u64>>;

impl<'de> Deserialize<'de> for Rows {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Visit;
        impl<'de> serde::de::Visitor<'de> for Visit {
            type Value = Rows;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a map of key to stored value")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut access: A,
            ) -> std::result::Result<Rows, A::Error> {
                let mut rows = Rows::default();
                while let Some((k, v)) = access.next_entry::<String, String>()? {
                    if charge_read(row_bytes(&k, &v)) {
                        rows.map.insert(k, v);
                    } else {
                        rows.map.clear();
                    }
                }
                Ok(rows)
            }
        }
        d.deserialize_map(Visit)
    }
}

/// The catalogue, counted against the snapshot's cap. It is one JSON string, so the parser holds it
/// whole before it can be counted; past the cap it is dropped at once.
fn bounded_catalogue<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<String>, D::Error> {
    let raw = Option::<String>::deserialize(d)?;
    Ok(raw.filter(|r| charge_read(r.len() as u64)))
}

/// A snapshot's dataset name, whose reading sets the cap its rows, which follow it, are read under.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct DatasetName(pub String);

impl<'de> Deserialize<'de> for DatasetName {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let name = String::deserialize(d)?;
        let cap = CAP_FOR.with(|f| f.borrow().as_ref().and_then(|f| f(&name, Kind::Snapshot)));
        BUDGET.with(|b| b.set(Budget { cap, seen: 0 }));
        limit_line(&name, cap, Kind::Snapshot);
        Ok(DatasetName(name))
    }
}

/// A delta's dataset name, which comes first in its line: reading it sets the byte limit the rest
/// of the delta is read under, from what the dataset has left.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct DeltaDataset(pub String);

impl<'de> Deserialize<'de> for DeltaDataset {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let name = String::deserialize(d)?;
        let cap = CAP_FOR.with(|f| f.borrow().as_ref().and_then(|f| f(&name, Kind::Delta)));
        limit_line(&name, cap, Kind::Delta);
        Ok(DeltaDataset(name))
    }
}

/// Fixed room above a line's byte limit for its JSON framing.
pub const SNAPSHOT_SLACK: u64 = 64 * 1024;

/// A snapshot's or delta's wire bytes passed its dataset's limit, so it was not read to the end: no
/// value in it was allocated past the limit, and none of it was applied. The stream cannot be resumed
/// mid-value; the follower reconnects without that dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineOverBudget {
    pub dataset: String,
    pub kind: Kind,
    /// Wire bytes of the line read before stopping.
    pub read: u64,
    pub limit: u64,
}

impl std::fmt::Display for LineOverBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.kind {
            Kind::Snapshot => "snapshot",
            Kind::Delta => "delta",
        };
        write!(
            f,
            "{}'s {what} passed {} wire bytes, its limit; stopped reading it at {}",
            self.dataset, self.limit, self.read
        )
    }
}

impl std::error::Error for LineOverBudget {}

/// Where the snapshot being read must stop, as a position in the stream, and which dataset it is.
#[derive(Clone, Default)]
struct WireLimit {
    position: u64,
    stop_at: Option<(u64, u64, String, Kind)>,
}

thread_local! {
    static WIRE: std::cell::RefCell<WireLimit> = std::cell::RefCell::new(WireLimit::default());
}

/// The body as the parser reads it, refusing to give it a byte past the current snapshot's limit:
/// a string can be no longer than the bytes it was read from, so this bounds every value in it.
struct Limited<R>(R);

impl<R: std::io::Read> std::io::Read for Limited<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let allowed = WIRE.with(|w| {
            let w = w.borrow();
            w.stop_at
                .as_ref()
                .map_or(buf.len() as u64, |(_, stop, _, _)| {
                    stop.saturating_sub(w.position)
                })
        });
        if allowed == 0 {
            return Err(std::io::Error::other("a snapshot passed its byte limit"));
        }
        let take = (allowed as usize).min(buf.len());
        let n = self.0.read(&mut buf[..take])?;
        WIRE.with(|w| w.borrow_mut().position += n as u64);
        Ok(n)
    }
}

/// Start limiting the line whose dataset name has just been read to `cap` plus the framing slack.
/// A value never decodes to more bytes than it was read from, escaping only adds to the wire, so
/// nothing allocated from the line can pass what is left; a heavily escaped line near the budget is
/// refused a little early instead.
fn limit_line(dataset: &str, cap: Option<u64>, kind: Kind) {
    WIRE.with(|w| {
        let mut w = w.borrow_mut();
        let start = w.position;
        w.stop_at = cap.map(|cap| {
            (
                start,
                start + cap + SNAPSHOT_SLACK,
                dataset.to_string(),
                kind,
            )
        });
    });
}

/// Read the feed's lines from `body` as they arrive, never holding a whole line's text, handing each
/// to `each` until it returns false. Blocking: run it on its own thread. A snapshot that runs past
/// its dataset's byte limit ends the read with [`LineOverBudget`].
pub fn read_lines(
    body: impl std::io::Read,
    cap_for: CapFor,
    mut each: impl FnMut(Line) -> bool,
) -> Result<()> {
    CAP_FOR.with(|f| *f.borrow_mut() = Some(cap_for));
    WIRE.with(|w| *w.borrow_mut() = WireLimit::default());
    for line in serde_json::Deserializer::from_reader(Limited(body)).into_iter::<Line>() {
        let budget = BUDGET.with(|b| b.replace(Budget::default()));
        let stopped = WIRE.with(|w| {
            let mut w = w.borrow_mut();
            let at = w.position;
            w.stop_at.take().filter(|(_, stop, _, _)| at >= *stop).map(
                |(start, stop, dataset, kind)| LineOverBudget {
                    dataset,
                    kind,
                    read: at - start,
                    limit: stop - start,
                },
            )
        });
        if let (Err(_), Some(over)) = (&line, stopped) {
            CAP_FOR.with(|f| *f.borrow_mut() = None);
            return Err(over.into());
        }
        let mut line = line.context("an unreadable line on the feed")?;
        if let Line::Snapshot { snapshot, .. } = &mut line {
            // A table read before the cap was passed kept its rows; none of them is kept now.
            if budget.over() {
                *snapshot = Snapshot {
                    over: Some(budget.seen),
                    ..Default::default()
                };
            }
        }
        if !each(line) {
            break;
        }
    }
    CAP_FOR.with(|f| *f.borrow_mut() = None);
    Ok(())
}

/// A mount's resolved guards ([`crate::limits::Limits`]) as the feed carries them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireLimits {
    pub timeout_ms: u64,
    pub max_rows: usize,
    pub max_result_bytes: usize,
    pub max_hot_rows: usize,
    pub max_hot_bytes: u64,
    pub max_named_scan_bytes: u64,
    pub analytics_memory_bytes: Option<u64>,
    pub max_queued: usize,
}

impl From<&crate::limits::Limits> for WireLimits {
    fn from(l: &crate::limits::Limits) -> Self {
        WireLimits {
            timeout_ms: l.timeout.as_millis() as u64,
            max_rows: l.max_rows,
            max_result_bytes: l.max_result_bytes,
            max_hot_rows: l.max_hot_rows,
            max_hot_bytes: l.max_hot_bytes,
            max_named_scan_bytes: l.max_named_scan_bytes,
            analytics_memory_bytes: l.analytics_memory_bytes,
            max_queued: l.max_queued,
        }
    }
}

impl WireLimits {
    /// The writer's guards, never wider than this process's own.
    pub fn within(self, process: crate::limits::Limits) -> crate::limits::Limits {
        crate::limits::Limits {
            timeout: process
                .timeout
                .min(std::time::Duration::from_millis(self.timeout_ms)),
            max_rows: process.max_rows.min(self.max_rows),
            max_result_bytes: process.max_result_bytes.min(self.max_result_bytes),
            max_hot_rows: process.max_hot_rows.min(self.max_hot_rows),
            max_hot_bytes: process.max_hot_bytes.min(self.max_hot_bytes),
            max_named_scan_bytes: process.max_named_scan_bytes.min(self.max_named_scan_bytes),
            analytics_memory_bytes: match (
                process.analytics_memory_bytes,
                self.analytics_memory_bytes,
            ) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            },
            max_queued: process.max_queued.min(self.max_queued),
        }
    }
}

/// A snapshot and the deltas that follow it.
pub struct Subscription {
    pub snapshot: Snapshot,
    pub deltas: tokio::sync::broadcast::Receiver<Arc<Delta>>,
}

/// One line of `GET /_admin/feed`, newline-delimited JSON. A dataset is named by its directory
/// relative to the runtime (`data/<nid>`), which is where a follower beside the writer finds its
/// segments; a route is a mount's `/<tenant>/<alias>` key.
///
/// Externally tagged (`{"delta": {...}}`) so a large snapshot deserialises without being buffered.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Line {
    /// `feed` is [`FEED_VERSION`]; a follower refuses a writer speaking another, so a rolling
    /// upgrade across a change to this format stops at the follower rather than misreading it.
    Hello {
        version: String,
        #[serde(default)]
        feed: u32,
        /// This connection, which the follower names when it reports what it has applied
        /// (`POST /_admin/feed/applied`), so segments it may still name are kept.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conn: Option<u64>,
    },
    /// A route and what the writer serves it under: its SQL surface (RFC-0034), its tenant label and
    /// share (RFC-0073 FR2) and its guards (FR3). A follower serves it under the same.
    Mount {
        route: String,
        dataset: String,
        nid: Option<String>,
        #[serde(default)]
        sql: crate::allowlist::SqlAccess,
        #[serde(default)]
        queries: Vec<crate::allowlist::NamedQuery>,
        #[serde(default)]
        tenant: Option<String>,
        #[serde(default)]
        share: Option<crate::runtime::TenantShare>,
        #[serde(default)]
        limits: Option<WireLimits>,
        /// The chain whose cursor the mount is on, which a follower charges its memory to.
        #[serde(default)]
        chain: Option<String>,
        /// The tenant this mount's queries are isolated to under `[runtime] isolation = "strict"`
        /// (RFC-0073 FR8), which a follower isolates them to as well.
        #[serde(default)]
        isolation: Option<String>,
    },
    /// Every route mounted when the connection opened has been sent: a route not named by now does
    /// not exist, rather than not having arrived yet.
    Mounted,
    Unmount {
        route: String,
    },
    /// `dataset` before `snapshot`: reading the name sets the cap the rows are read under.
    Snapshot {
        dataset: DatasetName,
        snapshot: Snapshot,
    },
    /// `dataset` before `delta`: reading the name sets the byte limit the delta is read under.
    Delta {
        dataset: DeltaDataset,
        delta: Delta,
    },
    /// Each followed dataset's `(epoch, generation)` now, sent every second so a follower can say how
    /// far behind it is and tell a quiet writer from a gone one.
    Heartbeat {
        positions: BTreeMap<String, (u64, u64)>,
    },
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
        Recorder::new(self.tx.receiver_count() > 0)
    }

    /// Run `commit` and number it, publishing its ops to any subscriber. Holding the lock across the
    /// commit is what puts every delta strictly after or strictly before a snapshot.
    pub fn commit(&self, rec: Recorder, commit: impl FnOnce() -> Result<()>) -> Result<()> {
        let mut state = self.lock();
        commit()?;
        state.generation += 1;
        if let (Some(dir), true) = (&self.dir, rec.seals()) {
            crate::seal::catalogue_moved(dir, (self.epoch, state.generation));
        }
        if self.tx.receiver_count() == 0 {
            return Ok(());
        }
        let delta = match rec.into_ops() {
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

impl nuthatch_core::tap::Feed for Tap {
    fn open(dir: Option<&Path>) -> Tap {
        Tap::new(dir.map(Path::to_path_buf))
    }
    fn recorder(&self) -> Recorder {
        Tap::recorder(self)
    }
    fn commit(&self, rec: Recorder, commit: &mut dyn FnMut() -> Result<()>) -> Result<()> {
        Tap::commit(self, rec, commit)
    }
    fn epoch(&self) -> u64 {
        Tap::epoch(self)
    }
    fn generation(&self) -> u64 {
        Tap::generation(self)
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
    /// What the three tables are charged, by [`row_bytes`], and the catalogue's length.
    bytes: u64,
    catalogue_bytes: u64,
    /// The catalogue this copy's watermark pairs with, `None` where its dataset never sealed.
    catalogue: Option<Arc<str>>,
    catalogue_rows: u64,
}

/// What one applied delta did to the stored rows, for whatever a follower derives from them.
#[derive(Debug, Default)]
pub struct Changes {
    /// Rows a rollback dropped, and the old value of a row a put replaced.
    pub removed: Vec<String>,
    /// Rows a put stored that were not already there as they are.
    pub added: Vec<String>,
    /// Rows a seal's prune moved out of hot, which leave the history unchanged.
    pub pruned: u64,
    /// The `last_block` the delta committed, set by a window or a rollback and nothing else.
    pub through: Option<u64>,
    /// The catalogue grew by rows that never passed through hot, as `--seal-direct` writes them:
    /// nothing derived from the deltas has seen them.
    pub sealed_unseen: bool,
}

/// Rows a catalogue holds across its segments.
fn catalogue_rows(raw: Option<&str>) -> u64 {
    raw.and_then(|r| serde_json::from_str::<crate::seal::Manifest>(r).ok())
        .map_or(0, |m| {
            m.tables.values().flatten().map(|s| s.rows as u64).sum()
        })
}

/// Insert into `map`, keeping `bytes` the charge for what it holds; the value replaced, if any.
fn put(
    map: &mut BTreeMap<String, String>,
    bytes: &mut u64,
    key: &str,
    value: &str,
) -> Option<String> {
    *bytes += row_bytes(key, value);
    let old = map.insert(key.to_string(), value.to_string());
    if let Some(old) = &old {
        *bytes -= row_bytes(key, old);
    }
    old
}

fn charge(map: &BTreeMap<String, String>) -> u64 {
    map.iter().map(|(k, v)| row_bytes(k, v)).sum()
}

impl Mirror {
    fn apply(&mut self, op: &Op, changes: &mut Changes) {
        match op {
            Op::Put { key, row } => match put(&mut self.entities, &mut self.bytes, key, row) {
                Some(old) if &old == row => {}
                Some(old) => {
                    changes.removed.push(old);
                    changes.added.push(row.clone());
                }
                None => changes.added.push(row.clone()),
            },
            // A rollback is the only open-ended drop; a seal's prune is always a bounded one.
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
                    if let Some(row) = self.entities.remove(k) {
                        self.bytes -= row_bytes(k, &row);
                        if to.is_some() {
                            changes.pruned += 1;
                        } else {
                            changes.removed.push(row);
                        }
                    }
                }
            }
            Op::Meta { key, value } => {
                if key == "last_block" {
                    changes.through = value.parse().ok();
                }
                put(&mut self.meta, &mut self.bytes, key, value);
            }
            Op::Unmeta { key } => {
                if let Some(v) = self.meta.remove(key) {
                    self.bytes -= row_bytes(key, &v);
                }
            }
            Op::Block { key, record } => {
                put(&mut self.blocks, &mut self.bytes, key, record);
            }
            Op::Unblock { key } => {
                if let Some(v) = self.blocks.remove(key) {
                    self.bytes -= row_bytes(key, &v);
                }
            }
            Op::UnblockFrom { key } => {
                let gone = self.blocks.split_off(key);
                self.bytes -= charge(&gone);
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
    /// Its dataset's directory, which [`crate::seal`] never reads the catalogue of while this lives.
    dir: PathBuf,
    /// Set when a request served from this copy found a segment it names missing: its follower,
    /// and no other, takes a fresh snapshot.
    missing: std::sync::atomic::AtomicBool,
}

impl Drop for MemStore {
    fn drop(&mut self) {
        crate::seal::unfollow(&self.dir);
    }
}

impl MemStore {
    pub fn from_snapshot(dir: &Path, snapshot: Snapshot) -> MemStore {
        let entities = snapshot.entities.map;
        let meta: BTreeMap<String, String> = snapshot
            .meta
            .map
            .into_iter()
            .filter(|(k, _)| mirrored_meta(k))
            .collect();
        let blocks = snapshot.blocks.map;
        let catalogue_bytes = snapshot.catalogue.as_ref().map_or(0, |c| c.len() as u64);
        let mirror = Mirror {
            bytes: charge(&entities) + charge(&meta) + charge(&blocks) + catalogue_bytes,
            entities,
            meta,
            blocks,
            epoch: snapshot.epoch,
            generation: snapshot.generation,
            catalogue_bytes,
            catalogue_rows: catalogue_rows(snapshot.catalogue.as_deref()),
            catalogue: snapshot.catalogue.map(Arc::from),
        };
        crate::seal::follow(dir);
        MemStore {
            mirror: RwLock::new(mirror),
            writes: AtomicU64::new(rows_epoch()),
            rows: AtomicU64::new(rows_epoch()),
            dir: dir.to_path_buf(),
            missing: Default::default(),
        }
    }

    /// What this copy is charged against its cursor's budget, by [`row_bytes`].
    pub fn bytes(&self) -> u64 {
        self.read().bytes
    }

    /// The most this copy would be charged at any point while `delta` is applied, by the accounting
    /// [`MemStore::apply`] keeps, worked out without applying it.
    pub fn peak_with(&self, delta: &Delta) -> u64 {
        // What each key a delta touches is charged once touched; `None` is removed.
        type Touched = BTreeMap<String, Option<u64>>;
        fn now(base: &BTreeMap<String, String>, touched: &Touched, key: &str) -> u64 {
            touched
                .get(key)
                .copied()
                .unwrap_or_else(|| base.get(key).map(|v| row_bytes(key, v)))
                .unwrap_or(0)
        }
        fn set(
            base: &BTreeMap<String, String>,
            touched: &mut Touched,
            held: &mut i128,
            key: &str,
            to: Option<u64>,
        ) {
            *held += i128::from(to.unwrap_or(0)) - i128::from(now(base, touched, key));
            touched.insert(key.to_string(), to);
        }
        fn in_range(key: &str, from: &str, to: Option<&String>) -> bool {
            key >= from && to.is_none_or(|t| key <= t.as_str())
        }
        fn drop_range(
            base: &BTreeMap<String, String>,
            touched: &mut Touched,
            held: &mut i128,
            from: &str,
            to: Option<&String>,
        ) {
            let mut keys: Vec<String> = match to {
                Some(to) => base
                    .range(from.to_string()..=to.clone())
                    .map(|(k, _)| k.clone())
                    .collect(),
                None => base
                    .range(from.to_string()..)
                    .map(|(k, _)| k.clone())
                    .collect(),
            };
            keys.extend(touched.keys().filter(|k| in_range(k, from, to)).cloned());
            for k in keys {
                set(base, touched, held, &k, None);
            }
        }
        let m = self.read();
        let (mut entities, mut meta, mut blocks) = (Touched::new(), Touched::new(), Touched::new());
        let mut held = i128::from(m.bytes);
        let mut peak = held;
        for op in &delta.ops {
            match op {
                Op::Put { key, row } => set(
                    &m.entities,
                    &mut entities,
                    &mut held,
                    key,
                    Some(row_bytes(key, row)),
                ),
                Op::Drop { from, to } => {
                    drop_range(&m.entities, &mut entities, &mut held, from, to.as_ref())
                }
                Op::Meta { key, value } => set(
                    &m.meta,
                    &mut meta,
                    &mut held,
                    key,
                    Some(row_bytes(key, value)),
                ),
                Op::Unmeta { key } => set(&m.meta, &mut meta, &mut held, key, None),
                Op::Block { key, record } => set(
                    &m.blocks,
                    &mut blocks,
                    &mut held,
                    key,
                    Some(row_bytes(key, record)),
                ),
                Op::Unblock { key } => set(&m.blocks, &mut blocks, &mut held, key, None),
                Op::UnblockFrom { key } => drop_range(&m.blocks, &mut blocks, &mut held, key, None),
            }
            peak = peak.max(held);
        }
        if let Some(raw) = &delta.catalogue {
            held += raw.len() as i128 - i128::from(m.catalogue_bytes);
            peak = peak.max(held);
        }
        peak.max(0) as u64
    }

    /// `(epoch, generation)` of the last delta applied.
    /// Whether a request found a segment this copy names missing since this was last asked.
    pub fn take_missing(&self) -> bool {
        self.missing.swap(false, Ordering::SeqCst)
    }

    pub fn applied(&self) -> (u64, u64) {
        let m = self.read();
        (m.epoch, m.generation)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Mirror> {
        self.mirror.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Apply the next delta, or refuse it as a [`Gap`] and change nothing.
    pub fn apply(&self, delta: &Delta) -> Result<Changes, Gap> {
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
        let mut changes = Changes::default();
        for op in &delta.ops {
            m.apply(op, &mut changes);
        }
        if let Some(raw) = &delta.catalogue {
            m.bytes = m.bytes - m.catalogue_bytes + raw.len() as u64;
            m.catalogue_bytes = raw.len() as u64;
            m.catalogue = Some(Arc::from(raw.as_str()));
            let rows = catalogue_rows(Some(raw));
            changes.sealed_unseen = rows.saturating_sub(m.catalogue_rows) != changes.pruned;
            m.catalogue_rows = rows;
        }
        m.generation = delta.generation;
        if changes_rows {
            self.rows.fetch_add(1, Ordering::SeqCst);
        }
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(changes)
    }

    /// `Store::hot_rows_snapshot` over the copy: with `charge`, the rows the copy will take are
    /// measured first, in its order and with its refusals, and charged before any is parsed. One read
    /// lock covers both passes, so the copy is exactly the bytes charged.
    fn hot_rows_snapshot(
        &self,
        max_rows: usize,
        max_bytes: u64,
        charge: Option<&mut dyn FnMut(u64) -> Result<()>>,
    ) -> Result<HotRowsSnapshot> {
        let m = self.read();
        if let Some(charge) = charge {
            #[derive(serde::Deserialize)]
            struct Tabled<'a> {
                #[serde(borrow, default)]
                table: Option<&'a serde_json::value::RawValue>,
            }
            let (mut bytes, mut seen) = (0u64, 0usize);
            for v in m.entities.values() {
                bytes = bytes
                    .checked_add(v.len() as u64)
                    .context("hot source byte count overflow")?;
                if bytes > max_bytes {
                    return Err(HotScanBudgetExceeded {
                        source_bytes: bytes,
                        budget: max_bytes,
                    }
                    .into());
                }
                let raw = serde_json::from_str::<&serde_json::value::RawValue>(v)
                    .context("unparseable hot row in an analytical scan")?;
                let tabled = raw.get().trim_start().starts_with('{')
                    && match serde_json::from_str::<Tabled>(raw.get()) {
                        Ok(t) => t.table.is_some_and(|t| t.get().starts_with('"')),
                        Err(_) => serde_json::from_str::<serde_json::Value>(raw.get())?
                            .get("table")
                            .is_some_and(serde_json::Value::is_string),
                    };
                if tabled {
                    seen += 1;
                    if seen > max_rows {
                        return Err(HotScanTooLarge { cap: max_rows }.into());
                    }
                }
            }
            charge(bytes)?;
        }
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
            entities: snapshot.entities.map.clone(),
            meta: snapshot
                .meta
                .map
                .iter()
                .filter(|(k, _)| mirrored_meta(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            blocks: snapshot.blocks.map.clone(),
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

impl HotStore for MemStore {}

#[async_trait::async_trait]
impl nuthatch_core::store::HotStore for MemStore {
    fn write_generation(&self) -> Option<u64> {
        let g = self.writes.load(Ordering::SeqCst);
        g.is_multiple_of(2).then_some(g)
    }

    fn rows_generation(&self) -> Option<u64> {
        let g = self.rows.load(Ordering::SeqCst);
        g.is_multiple_of(2).then_some(g)
    }

    fn served_catalogue(&self) -> Option<Option<Arc<str>>> {
        Some(self.read().catalogue.clone())
    }

    fn segment_missing(&self) {
        self.missing.store(true, Ordering::SeqCst);
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
        self.hot_rows_snapshot(max_rows, max_bytes, None)
    }
    fn hot_rows_charged(
        &self,
        max_rows: usize,
        max_bytes: u64,
        charge: &mut dyn FnMut(u64) -> Result<()>,
    ) -> Result<HotRowsSnapshot> {
        self.hot_rows_snapshot(max_rows, max_bytes, Some(charge))
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
        let mut sub = crate::store::feed_subscribe(&store).unwrap();
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

        let now = crate::store::feed_subscribe(&store).unwrap().snapshot;
        assert_eq!(copy.applied(), (now.epoch, now.generation));
        assert_eq!(copy.tables(), MirroredTables::of(&now));
        assert!(
            !copy.tables().meta.contains_key("outbox_next_seq"),
            "the writer's outbox is not mirrored"
        );
        let ours =
            crate::store::CoreHotStore::hot_rows_by_table_bounded_with_bytes(&copy, 100, u64::MAX)
                .unwrap();
        let theirs = store
            .hot_rows_by_table_bounded_with_bytes(100, u64::MAX)
            .unwrap();
        assert_eq!(ours.rows, theirs.rows);
        assert_eq!(ours.source_bytes, theirs.source_bytes);
        assert_eq!(ours.last_block, theirs.last_block);
        assert_eq!(crate::store::CoreHotStore::sealed_through(&copy), 3);
        assert_eq!(
            crate::store::CoreHotStore::checkpoints_desc(&copy).unwrap(),
            store.checkpoints_desc().unwrap()
        );
        assert_eq!(
            crate::store::CoreHotStore::get_block_timestamp(&copy, 4).unwrap(),
            store.get_block_timestamp(4).unwrap()
        );
        assert_eq!(
            crate::store::CoreHotStore::recent_by_table(&copy, "c", 10).unwrap(),
            store.recent_by_table("c", 10).unwrap()
        );
    }

    /// The seal's prune, its watermark and its catalogue arrive in one delta.
    #[test]
    fn the_sealing_delta_carries_its_catalogue() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        store
            .commit_window(&[row(1, 0, "t"), row(2, 0, "t")], None, 2)
            .unwrap();
        let mut sub = crate::store::feed_subscribe(&store).unwrap();
        assert_eq!(sub.snapshot.catalogue, None);
        let copy = Arc::new(MemStore::from_snapshot(tmp.path(), sub.snapshot.clone()));
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
        let (pinned, _) = crate::seal::within(tmp.path(), copy.clone(), || {
            crate::seal::load_manifest_with_hash(tmp.path())
        })
        .unwrap();
        assert_eq!(pinned.tables["t"][0].to_block, 1);
        assert_eq!(crate::store::CoreHotStore::count(&*copy).unwrap(), 1);
        assert!(
            crate::seal::load_manifest(tmp.path()).is_err(),
            "outside a request on a copy, the disk is not read for a followed dataset"
        );
    }

    /// Each copy of one dataset answers with its own catalogue, whichever is newer, and one going
    /// leaves the other's alone.
    #[test]
    fn each_copy_of_a_dataset_reads_its_own_catalogue() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        store
            .commit_window(&[row(1, 0, "t"), row(2, 0, "t")], None, 2)
            .unwrap();
        let old = Arc::new(MemStore::from_snapshot(
            tmp.path(),
            crate::store::feed_subscribe(&store).unwrap().snapshot,
        ));
        write_catalogue(tmp.path(), 1);
        store
            .prune_and_set_meta(1, 1, "sealed_through", "1")
            .unwrap();
        let new = Arc::new(MemStore::from_snapshot(
            tmp.path(),
            crate::store::feed_subscribe(&store).unwrap().snapshot,
        ));
        let sealed = |copy: &Arc<MemStore>| {
            crate::seal::within(tmp.path(), copy.clone(), || {
                crate::seal::load_manifest(tmp.path()).unwrap().tables.len()
            })
        };
        assert_eq!((sealed(&old), sealed(&new)), (0, 1));
        drop(old);
        assert_eq!(sealed(&new), 1, "the other copy's going leaves this one's");
        assert!(crate::seal::load_manifest(tmp.path()).is_err());
        drop(new);
        assert_eq!(
            crate::seal::load_manifest(tmp.path()).unwrap().tables.len(),
            1
        );
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
        let snap = crate::store::feed_subscribe(&store).unwrap().snapshot;
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
        let mut sub = crate::store::feed_subscribe(&store).unwrap();
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

    /// What a delta tells a follower's derived state: a window's new rows, a reorg's dropped ones,
    /// a seal's prune as no change at all, and rows sealed without passing through hot as unseen.
    #[test]
    fn a_delta_says_which_stored_rows_it_changed() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        let mut sub = crate::store::feed_subscribe(&store).unwrap();
        let copy = MemStore::from_snapshot(tmp.path(), sub.snapshot.clone());
        let mut next = || copy.apply(&sub.deltas.try_recv().unwrap()).unwrap();

        store
            .commit_window(&[row(1, 0, "t"), row(2, 0, "t")], None, 2)
            .unwrap();
        let window = next();
        assert_eq!(window.added.len(), 2);
        assert!(window.removed.is_empty());
        assert_eq!(window.through, Some(2));

        // A refetched row already held as it is changes nothing.
        store.commit_window(&[row(2, 0, "t")], None, 2).unwrap();
        let again = next();
        assert!(again.added.is_empty() && again.removed.is_empty());

        write_catalogue(tmp.path(), 1);
        store
            .prune_and_set_meta(1, 1, "sealed_through", "1")
            .unwrap();
        let seal = next();
        assert!(seal.added.is_empty() && seal.removed.is_empty());
        assert_eq!(
            (seal.pruned, seal.through, seal.sealed_unseen),
            (1, None, false)
        );

        store
            .rollback_to_and_set_meta(1, "last_block", "1", &[])
            .unwrap();
        let reorg = next();
        assert_eq!(reorg.removed, vec![row(2, 0, "t").1]);
        assert_eq!(reorg.through, Some(1));

        // Sealed straight to a segment: the catalogue grows and nothing left hot.
        std::fs::write(
            tmp.path()
                .join(crate::seal::SEGMENTS_DIR)
                .join(crate::seal::MANIFEST_FILE),
            serde_json::json!({"tables": {"t": [
                {"hash": "h1", "from_block": 1, "to_block": 1, "rows": 1, "file": "t-h1.parquet"},
                {"hash": "h9", "from_block": 2, "to_block": 9, "rows": 40, "file": "t-h9.parquet"}]}})
            .to_string(),
        )
        .unwrap();
        store.set_meta("sealed_through", "9").unwrap();
        assert!(next().sealed_unseen);
    }

    /// A snapshot is read under the cap its dataset is given, a line at a time, its entity rows, meta,
    /// checkpoints and catalogue all counted against it. One over it in any of them keeps nothing;
    /// the lines either side of it read as they were sent.
    #[test]
    fn snapshots_are_read_under_their_datasets_cap() {
        let fill = |rows: &mut Rows, n: usize| {
            for i in 0..n {
                rows.map.insert(format!("{i:012}-000000"), "x".repeat(100));
            }
        };
        let entry = row_bytes("000000000000-000000", &"x".repeat(100));
        let cap = entry * 10;
        let mut cases = Vec::new();
        let mut s = Snapshot::default();
        fill(&mut s.entities, 3);
        fill(&mut s.meta, 2);
        cases.push(("data/small", s));
        for big in ["entities", "meta", "blocks"] {
            let mut s = Snapshot::default();
            fill(&mut s.entities, 2);
            let rows = match big {
                "entities" => &mut s.entities,
                "meta" => &mut s.meta,
                _ => &mut s.blocks,
            };
            fill(rows, 50);
            cases.push((big, s));
        }
        let mut s = Snapshot::default();
        fill(&mut s.entities, 2);
        s.catalogue = Some("c".repeat(cap as usize));
        cases.push(("catalogue", s));
        let mut s = Snapshot::default();
        fill(&mut s.blocks, 50);
        cases.push(("data/free", s));

        let mut wire = Vec::new();
        for (dataset, snapshot) in &cases {
            serde_json::to_writer(
                &mut wire,
                &Line::Snapshot {
                    dataset: DatasetName(dataset.to_string()),
                    snapshot: snapshot.clone(),
                },
            )
            .unwrap();
            wire.push(b'\n');
        }
        let mut got = BTreeMap::new();
        read_lines(
            &wire[..],
            Box::new(move |d: &str, _| (d != "data/free").then_some(cap)),
            |line| {
                if let Line::Snapshot { dataset, snapshot } = line {
                    got.insert(dataset.0, snapshot);
                }
                true
            },
        )
        .unwrap();
        let small = &got["data/small"];
        assert_eq!(small.over, None);
        assert_eq!(small.bytes(), entry * 5);
        for over in ["entities", "meta", "blocks", "catalogue"] {
            let s = &got[over];
            assert!(s.over.is_some(), "{over} alone over the cap");
            assert_eq!(s.bytes(), 0, "{over}: nothing is kept");
        }
        assert_eq!(got["entities"].over, Some(entry * 50));
        assert_eq!(got["data/free"].blocks.map.len(), 50);
        assert_eq!(got["data/free"].over, None);
    }

    /// One value larger than its dataset's budget is never read whole: the body stops at the
    /// snapshot's byte limit, so the parser cannot allocate past it, and the read ends naming the
    /// dataset. The lines before it were handed over.
    #[test]
    fn one_value_over_the_budget_stops_the_read_at_its_byte_limit() {
        struct Counted<'a>(&'a [u8], std::rc::Rc<std::cell::Cell<usize>>);
        impl std::io::Read for Counted<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let n = std::io::Read::read(&mut self.0, buf)?;
                self.1.set(self.1.get() + n);
                Ok(n)
            }
        }
        let mut wire = Vec::new();
        let mut small = Snapshot::default();
        small.meta.map.insert("last_block".into(), "1".into());
        let mut huge = Snapshot::default();
        huge.meta.map.insert("ballast".into(), "x".repeat(8 << 20));
        for (dataset, snapshot) in [("data/small", small), ("data/huge", huge)] {
            serde_json::to_writer(
                &mut wire,
                &Line::Snapshot {
                    dataset: DatasetName(dataset.into()),
                    snapshot,
                },
            )
            .unwrap();
            wire.push(b'\n');
        }
        let consumed = std::rc::Rc::new(std::cell::Cell::new(0));
        let mut seen = Vec::new();
        let err = read_lines(
            std::io::BufReader::with_capacity(4096, Counted(&wire, consumed.clone())),
            Box::new(|_, _| Some(1_000)),
            |line| {
                if let Line::Snapshot { dataset, .. } = line {
                    seen.push(dataset.0);
                }
                true
            },
        )
        .unwrap_err();
        let over = err.downcast_ref::<LineOverBudget>().expect("{err:#}");
        assert_eq!(over.dataset, "data/huge");
        assert_eq!(over.limit, 1_000 + SNAPSHOT_SLACK);
        assert_eq!(seen, vec!["data/small".to_string()]);
        assert!(
            consumed.get() < 200 * 1024,
            "read {} of {} bytes",
            consumed.get(),
            wire.len()
        );
    }

    /// A delta's charge is worked out before it is applied, at its peak: a put a later drop in the
    /// same delta takes back still counts, and the figure after is what applying it leaves.
    #[test]
    fn a_deltas_peak_is_known_before_it_is_applied() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&tmp.path().join("nuthatch.redb")).unwrap();
        store.commit_window(&[row(1, 0, "t")], None, 1).unwrap();
        let sub = crate::store::feed_subscribe(&store).unwrap();
        let copy = MemStore::from_snapshot(tmp.path(), sub.snapshot);
        let before = copy.bytes();
        let (k, big) = (Store::entity_key(2, 0), "x".repeat(10_000));
        let (epoch, generation) = copy.applied();
        let delta = Delta {
            epoch,
            generation: generation + 1,
            ops: vec![
                Op::Put {
                    key: k.clone(),
                    row: big.clone(),
                },
                Op::Drop {
                    from: format!("{:012}-", 2),
                    to: None,
                },
                Op::Meta {
                    key: "last_block".into(),
                    value: "1".into(),
                },
            ],
            catalogue: None,
            resync: false,
        };
        assert_eq!(copy.peak_with(&delta), before + row_bytes(&k, &big));
        copy.apply(&delta).unwrap();
        assert_eq!(
            copy.bytes(),
            before,
            "the put taken back, the meta rewritten as it was"
        );
    }

    /// A delta's bytes are limited by what its dataset has left, as a snapshot's are by its budget.
    #[test]
    fn a_delta_over_what_is_left_stops_the_read_at_its_byte_limit() {
        let delta = Delta {
            epoch: 1,
            generation: 2,
            ops: vec![Op::Put {
                key: "000000000001-000000".into(),
                row: "x".repeat(8 << 20),
            }],
            catalogue: None,
            resync: false,
        };
        let mut wire = serde_json::to_vec(&Line::Delta {
            dataset: DeltaDataset("data/huge".into()),
            delta,
        })
        .unwrap();
        wire.push(b'\n');
        let err = read_lines(
            &wire[..],
            Box::new(|_, kind| (kind == Kind::Delta).then_some(1_000)),
            |_| true,
        )
        .unwrap_err();
        let over = err.downcast_ref::<LineOverBudget>().expect("{err:#}");
        assert_eq!(
            (over.dataset.as_str(), over.kind),
            ("data/huge", Kind::Delta)
        );
        assert_eq!(over.read, 1_000 + SNAPSHOT_SLACK);
    }

    /// A mount's lowered guards survive the wire, and a follower never serves wider than its own.
    #[test]
    fn a_mounts_guards_cross_the_feed_and_stay_within_the_follower() {
        let process = crate::limits::Limits::process();
        let lowered = crate::limits::Limits {
            max_rows: 10,
            timeout: std::time::Duration::from_millis(1_500),
            analytics_memory_bytes: Some(1 << 20),
            ..process
        };
        let wire: WireLimits =
            serde_json::from_str(&serde_json::to_string(&WireLimits::from(&lowered)).unwrap())
                .unwrap();
        assert_eq!(wire.within(process), lowered);
        let tighter = crate::limits::Limits {
            max_rows: 5,
            ..process
        };
        assert_eq!(wire.within(tighter).max_rows, 5);
    }
}
