//! Embedded hot store: redb. This is the tip layer for entity point-reads; Parquet sealing + Burrmill
//! analytics live in `seal`/`analytics`.
//!
//! **Writers (audit F-C3 - this used to say "single writer", which was wrong).** Two tasks write here:
//! the ingestion loop (entities, checkpoints, meta, and the outbox *enqueue*) and the alert-delivery
//! worker (the outbox *drain*, via `outbox_remove`). They touch disjoint key ranges, and redb
//! serialises write transactions internally regardless, so integrity holds - but "single writer" was a
//! claim about the code that the code did not honour, and the next person to reason about concurrency
//! here deserves the truth.
//!
//! What *is* single is the **cursor**: exactly one ingestion task per chain advances the chain state
//! (RFC-0012/0021). That is the invariant the architecture actually rests on. Readers (the API) are
//! unbounded and never block writers - redb gives them MVCC snapshots.
//!
//! **Blocking.** A redb `commit()` fsyncs, and an fsync on a contended disk can take far longer than a
//! tokio worker should ever be parked for. Callers on an async task must therefore run commits through
//! `spawn_blocking` - see [`Store::commit_blocking`]. The methods here are deliberately synchronous:
//! they are the blocking primitive, and it is the caller's job to place them correctly.

use anyhow::{Context, Result};
use redb::{Builder, Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// redb's own default cache size, and therefore ours: **this constant is a no-op**, chosen so that
/// making the value settable changes nothing until someone measures and picks another.
///
/// `redb::Builder::new()` calls `set_cache_size(1 GiB)`, split 90% read / 10% write, and nuthatch has
/// never overridden it at any of its open sites. That matters more than it sounds, because redb 2.6
/// does **not** mmap - `file_backend/unix.rs` preads into `Vec<u8>` - so every cached page is heap
/// and lands in `VmRSS`, which is what non-negotiable 2 bounds.
///
/// Measured by `tests/bench_compact_rows.rs` (at `ede03ca7`) against a 2.16 GB store, on Linux: RSS tracks this
/// number almost one-for-one (1.00 GB at 1 GiB, 0.50 at 512 MiB, 0.25 at 256 MiB) and is
/// **independent of the file**, while a point read costs +0.6 us going from 1 GiB to 256 MiB. The
/// two live Lodestar cursors sit at 1.44 and 1.42 GB against their 2 GB, and the *larger* store has
/// the *smaller* RSS - which a linear `RSS = k x file` cannot produce and a ceiling can.
pub const HOT_STORE_CACHE_BYTES: usize = 1024 * 1024 * 1024;

/// Floor on the override. Below this redb thrashes rather than caches, and a benchmark that asked
/// for 1 MiB and silently got it would publish a latency curve whose right-hand end is meaningless.
pub const HOT_STORE_CACHE_FLOOR: usize = 16 * 1024 * 1024;

/// Ceiling on the override, deliberately equal to the default.
///
/// **This knob exists to lower a ceiling nobody chose, not to raise one.** Non-negotiable 2 is 2 GB
/// per *cursor*, and the cache is per **store** - `Store::open` is called once per nest inside
/// `build_nest`, so a cursor hosting N nests carries N of these. Two nests at the default already
/// exceed a whole cursor's budget in cache alone. Letting an operator raise it past 1 GiB would hand
/// them one environment variable that breaks the budget N times over, which is the shape of the
/// per-nest-semaphore fault #1024 found in #1006's override. Same trap, so the same answer.
///
/// It is a *ceiling* rather than a reservation - redb fills it as pages are read - which is why a
/// dense cursor of small nests survives today and a dense cursor of large ones would not.
pub const HOT_STORE_CACHE_CEILING: usize = HOT_STORE_CACHE_BYTES;

/// The live cache size: [`HOT_STORE_CACHE_BYTES`] unless `NUTHATCH_HOT_STORE_CACHE_BYTES` overrides.
///
/// **This exists so the value can be measured, not so it can be tuned casually** - the same reason
/// and the same shape as [`crate::serve::sql_max_concurrency`]. #1046 asks for the RSS / point-read /
/// tip-lag curve at 1 GiB, 512 MiB and 256 MiB on the box that enforces the budget; with a bare
/// constant that needs three separate builds, which is how a ceiling ends up being set from whichever
/// box was convenient. One binary, three settings, measured where it is enforced.
///
/// Refusals are loud and the value is clamped rather than silently accepted, so a run that requested
/// something out of range cannot publish a curve that is flat for the wrong reason.
pub fn hot_store_cache_bytes() -> usize {
    match std::env::var("NUTHATCH_HOT_STORE_CACHE_BYTES") {
        Err(_) => HOT_STORE_CACHE_BYTES,
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(0) | Err(_) => {
                tracing::warn!(
                    value = %raw,
                    default = HOT_STORE_CACHE_BYTES,
                    "NUTHATCH_HOT_STORE_CACHE_BYTES is not a positive integer; using the default"
                );
                HOT_STORE_CACHE_BYTES
            }
            Ok(n) if n < HOT_STORE_CACHE_FLOOR => {
                tracing::warn!(
                    requested = n,
                    floor = HOT_STORE_CACHE_FLOOR,
                    "NUTHATCH_HOT_STORE_CACHE_BYTES below the floor; clamping. Below this redb                      thrashes rather than caches and point-read latency stops meaning anything (#1046)"
                );
                HOT_STORE_CACHE_FLOOR
            }
            Ok(n) if n > HOT_STORE_CACHE_CEILING => {
                tracing::warn!(
                    requested = n,
                    ceiling = HOT_STORE_CACHE_CEILING,
                    "NUTHATCH_HOT_STORE_CACHE_BYTES above the ceiling; clamping. The cache is per                      store and one cursor holds one per nest, so raising it multiplies against a 2 GB                      per-cursor budget (#1046)"
                );
                HOT_STORE_CACHE_CEILING
            }
            Ok(n) => n,
        },
    }
}

/// A redb builder carrying the configured cache size. Every open site in this file goes through it,
/// so none of them can quietly keep redb's default.
fn builder() -> Builder {
    let mut b = Builder::new();
    b.set_cache_size(hot_store_cache_bytes());
    b
}

const ENTITIES: TableDefinition<&str, &str> = TableDefinition::new("entities");
const META: TableDefinition<&str, &str> = TableDefinition::new("meta");
/// Block-hash checkpoints (block -> canonical hash we indexed against), for reorg detection.
/// The value is `hash`, or `hash\tunix_seconds` once a timestamp is known (#1289).
const BLOCKS: TableDefinition<&str, &str> = TableDefinition::new("blocks");
/// Derived reverse index, transactionally maintained with the canonical checkpoint table.
const CHECKPOINT_HASHES: TableDefinition<&str, u64> = TableDefinition::new("checkpoint_hashes");
/// RFC-0060: only a `graph` build keeps the index. A default build never creates or writes it, so its
/// on-disk layout is unchanged.
const INDEX_CHECKPOINT_HASHES: bool = cfg!(feature = "graph");
/// Bounds the one-off backlog a store from before #1636 carries, a window at a time.
const CHECKPOINTS_PRUNED_PER_COMMIT: usize = 4096;

/// The `block_hash` a stored row JSON carries.
pub(crate) fn row_block_hash(json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()?
        .get("block_hash")?
        .as_str()
        .map(str::to_string)
}

/// A tab cannot appear in a block hash, so a hash-only value from before #1289 still round-trips.
pub(crate) fn encode_block_record(hash: &str, timestamp: Option<u64>) -> String {
    match timestamp {
        Some(ts) => format!("{hash}\t{ts}"),
        None => hash.to_string(),
    }
}

pub(crate) fn decode_block_record(value: &str) -> (String, Option<u64>) {
    match value.split_once('\t') {
        Some((h, t)) => (h.to_string(), t.parse().ok()),
        None => (value.to_string(), None),
    }
}

/// Durable alert-delivery outbox (RFC-0008 C5): monotonic seq -> pending-delivery JSON. Survives
/// restart, so at-least-once delivery holds across a process bounce.
const OUTBOX: TableDefinition<&str, &str> = TableDefinition::new("outbox");
/// Meta key holding the next outbox sequence number.
const OUTBOX_SEQ: &str = "outbox_next_seq";
/// Meta key holding the **ownership fence** (RFC-0022 slice 4): a monotonically increasing number
/// stamped by whichever worker most recently claimed this store.
pub const OWNER_FENCE: &str = "owner_fence";
/// Meta key holding the current lease holder's name.
pub const LEASE_OWNER: &str = "lease_owner";
/// Meta key holding the lease expiry, unix seconds, **on the store's clock**.
pub const LEASE_EXPIRES_AT: &str = "lease_expires_at";

/// A held lease on a cursor (RFC-0022 slice 4b).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub owner: String,
    /// The fence issued with this lease. Every write the holder makes carries it.
    pub fence: u64,
    /// Seconds until expiry, by the store's clock. Negative means already expired.
    pub expires_in_secs: i64,
}

/// Someone else holds the lease and it has not expired.
///
/// Distinct from [`LostOwnership`], which is what a *write* fails with: this is what an *acquisition*
/// fails with. One says "you never got it", the other says "you had it and lost it", and a scheduler
/// reacts differently to each - back off versus stop and drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHeld {
    pub by: String,
    pub expires_in_secs: i64,
}

impl std::fmt::Display for LeaseHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cursor is leased to '{}' for another {}s - not acquiring",
            self.by, self.expires_in_secs
        )
    }
}

impl std::error::Error for LeaseHeld {}

/// A write was refused because the store now belongs to a newer holder.
///
/// This is the fencing half of the single-owner guarantee, and it is a **distinct** error from I/O
/// failure on purpose: an I/O error means try again, this means *stop*. A caller that retries here
/// is a caller racing the worker that legitimately owns the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LostOwnership {
    /// The fence this holder believed it had.
    pub held: u64,
    /// The fence actually recorded in the store.
    pub current: u64,
}

impl std::fmt::Display for LostOwnership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lost ownership of this hot store: held fence {} but the store is at {}. Another worker \
             has claimed this cursor - stop writing, do not retry",
            self.held, self.current
        )
    }
}

impl std::error::Error for LostOwnership {}

/// The unsealed tip is too large to materialise for one query (the `/sql` RAM guard).
///
/// Typed rather than a string so the serving layer can map it to a status code without matching on
/// prose - the same reasoning as `MountRefusal` and the RPC `FailureClass`. A message-matched guard
/// silently stops working the day someone rewords the message.
#[derive(Debug)]
pub struct HotScanTooLarge {
    pub cap: usize,
}

impl std::fmt::Display for HotScanTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the unsealed tip holds more than {} rows, which this node will not materialise for a \
             single query - query at or below `sealed_through`, or raise the cap if the box has the \
             memory",
            self.cap
        )
    }
}

impl std::error::Error for HotScanTooLarge {}

/// One consistent hot-store read for an analytical query. `source_bytes` is the length of the
/// serialized redb values that will be parsed into Burrmill, not a JSON re-encoding or a resident-RAM
/// estimate (RFC-0048 S2).
#[derive(Debug, Default)]
pub struct HotRowsSnapshot {
    pub rows: HashMap<String, Vec<serde_json::Value>>,
    /// Values read, whether or not they parsed into a row.
    pub source_rows: u64,
    pub source_bytes: u64,
}

/// A declared query's hot copy passed the byte budget its cold reservation left (RFC-0048 §3).
#[derive(Debug)]
pub struct HotScanBudgetExceeded {
    pub source_bytes: u64,
    pub budget: u64,
}

impl std::fmt::Display for HotScanBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the unsealed tip needs more than {} source bytes, above this query's remaining {}-byte scan budget",
            self.source_bytes, self.budget
        )
    }
}

impl std::error::Error for HotScanBudgetExceeded {}

/// The hot store, behind a trait (RFC-0022 slice 1).
///
/// Everything above finality lives here: decoded rows, the ingest cursor, block-hash checkpoints for
/// reorg detection, and the delivery outbox. Sealed Parquet is a separate, immutable layer and is not
/// this trait's business.
///
/// **Why it exists.** RFC-0022 places *cursors* on machines, and a cursor cannot move to another
/// machine while its state is welded to a local redb file. `CLAUDE.md` has always required the
/// backend to sit behind a trait with no `#[cfg]` forks of business logic; until now it did not, and
/// every module named redb directly. This is that seam, cut ahead of the Postgres implementation
/// rather than during it, so the refactor and the new backend can be reviewed - and blamed -
/// separately.
///
/// **The contract a second implementation must honour**, none of which is inferable from the
/// signatures alone:
///
/// - [`commit_window`](HotStore::commit_window) is **atomic**. Rows, checkpoint and cursor advance
///   land together or not at all, so a crash mid-window leaves the previous window's state intact.
///   This is what `e2e_crash_safety` pins, and it is not negotiable for a backend claiming parity.
/// - **Single writer.** Exactly one task mutates a given store. An implementation may assume it and
///   must not silently repair concurrent writes - under RFC-0022 the assumption becomes a cursor
///   lease, and a backend that quietly tolerated two writers would hide the very bug the lease
///   exists to prevent.
/// - **Keys order by block.** [`Store::entity_key`] is zero-padded so lexicographic order is block
///   order, and every range scan here depends on it.
/// - **Reads see committed state only.** No dirty reads; `/sql` attaches read-only against this.
///
/// `open` and `entity_key` are deliberately absent: one is a constructor (backends are chosen by
/// config, not by a trait method) and the other is a pure function of its arguments.
#[async_trait::async_trait]
pub trait HotStore: Send + Sync {
    /// A counter that moves on every committed write and never otherwise, or `None` where the
    /// backend cannot say - including, for the redb store, the moment a commit is in flight. Two
    /// reads that both return `Some(g)`, one before a query and one after, prove no commit began
    /// between them. The memo (#1186) fences on the narrower [`HotStore::rows_generation`].
    fn write_generation(&self) -> Option<u64> {
        None
    }

    /// [`HotStore::write_generation`] counting only the commits that add or remove a hot row, the
    /// only ones a `/sql` answer can see. A cursor also commits on every poll to move `last_block` and
    /// its checkpoints, which on a two-second chain moved the memo's key faster than a whole-history
    /// view can be computed, so nothing was ever remembered (#1951). A backend that returns `None`
    /// simply has no memo.
    fn rows_generation(&self) -> Option<u64> {
        None
    }

    // ---- entities ---------------------------------------------------------------------------
    fn put_entity(&self, key: &str, json: &str) -> Result<()>;
    /// Write `json` at `key` only while `source_key` holds a row from the block `block_hash` names, as
    /// one transaction. For a writer outside the ingest loop, so a document for a row a reorg rolled
    /// back cannot land after the rollback. Returns whether it wrote.
    fn put_entity_if_named(
        &self,
        key: &str,
        json: &str,
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool>;
    /// [`HotStore::put_entity_if_named`] for rows that stand or fall together: a document and the typed
    /// rows it explodes into (RFC-0037 slice 8), so a reader never sees one without the other.
    fn put_entities_if_named(
        &self,
        entries: &[(String, String)],
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool>;
    fn get_entity(&self, key: &str) -> Result<Option<String>>;
    fn count(&self) -> Result<u64>;
    fn recent(&self, limit: usize) -> Result<Vec<String>>;
    fn recent_by_table(&self, table: &str, limit: usize) -> Result<Vec<String>>;
    fn hot_rows_by_table(&self) -> Result<HashMap<String, Vec<serde_json::Value>>>;
    fn hot_rows_by_table_bounded(
        &self,
        max_rows: usize,
    ) -> Result<HashMap<String, Vec<serde_json::Value>>>;
    /// As `hot_rows_by_table_bounded`, while accounting the bytes the query will materialise.
    /// Backends that cannot give a byte-exact snapshot must refuse paid/admitted execution rather
    /// than substitute a representation-dependent approximation.
    fn hot_rows_by_table_bounded_with_bytes(
        &self,
        _max_rows: usize,
        _max_bytes: u64,
    ) -> Result<HotRowsSnapshot> {
        anyhow::bail!("this hot-store backend cannot report a byte-exact query snapshot")
    }
    fn entities_in_range(&self, from: u64, to: u64) -> Result<Vec<String>>;
    /// Rows in `[from, to]` in chain order, handed to `visit` as `(block, json)` until it returns
    /// `false`. Choosing a seal cut reads only as far as the cut; this default still loads the range.
    fn scan_entities_in_range(
        &self,
        from: u64,
        to: u64,
        visit: &mut dyn FnMut(u64, &str) -> bool,
    ) -> Result<()> {
        for json in self.entities_in_range(from, to)? {
            let block = serde_json::from_str::<serde_json::Value>(&json)
                .ok()
                .and_then(|v| match v.get("block_number")? {
                    serde_json::Value::Number(n) => n.as_u64(),
                    serde_json::Value::String(s) => s.parse().ok(),
                    _ => None,
                })
                .with_context(|| {
                    format!("a stored row in {from}..={to} carries no block_number")
                })?;
            if !visit(block, &json) {
                break;
            }
        }
        Ok(())
    }
    fn sample_entity_keys(&self, limit: usize) -> Result<Vec<String>>;

    // ---- cursor & meta ----------------------------------------------------------------------
    fn get_meta(&self, key: &str) -> Result<Option<String>>;
    fn set_meta(&self, key: &str, value: &str) -> Result<()>;
    /// At most `limit` meta entries whose keys start with `prefix`, in key order.
    fn meta_with_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>>;
    fn count_meta_with_prefix(&self, prefix: &str) -> Result<u64>;
    fn indexed_head(&self) -> Result<Option<u64>>;
    fn sealed_through(&self) -> u64;
    fn set_block_hash(&self, block: u64, hash: &str) -> Result<()>;
    fn get_block_hash(&self, block: u64) -> Result<Option<String>>;
    fn set_block_timestamp(&self, block: u64, timestamp: u64) -> Result<()>;
    fn get_block_timestamp(&self, block: u64) -> Result<Option<u64>>;
    fn checkpoints_desc(&self) -> Result<Vec<(u64, String)>>;
    /// Resolve a recent canonical checkpoint without materialising the full checkpoint history.
    /// Backends without this lookup must refuse hash-pinned reads rather than substitute the head.
    fn checkpoint_number(&self, _hash: &str) -> Result<Option<u64>> {
        anyhow::bail!("this store does not support checkpoint hash lookup")
    }

    // ---- mutation windows (the atomic ones) --------------------------------------------------
    fn commit_window(
        &self,
        entities: &[(String, String)],
        checkpoint: Option<(u64, &str)>,
        last_block: u64,
    ) -> Result<()>;
    /// `outbox` entries are pushed in the same transaction, so an alert lands only with its window.
    async fn commit_window_blocking(
        &self,
        entities: Vec<(String, String)>,
        outbox: Vec<String>,
        checkpoint: Option<(u64, String)>,
        last_block: u64,
    ) -> Result<()>;
    /// Return once no [`HotStore::commit_window_blocking`] is still committing, including one
    /// whose caller was aborted.
    async fn settle_commits(&self);
    fn rollback_to(&self, block: u64) -> Result<u64>;
    fn rollback_to_and_set_meta(
        &self,
        block: u64,
        meta_key: &str,
        meta_val: &str,
        outbox: &[String],
    ) -> Result<u64>;
    fn prune_range(&self, from: u64, to: u64) -> Result<u64>;
    fn prune_and_set_meta(&self, from: u64, to: u64, meta_key: &str, meta_val: &str)
        -> Result<u64>;

    // ---- ownership (RFC-0022 slice 4) ---------------------------------------------------------
    /// Claim this store, returning the new fence.
    ///
    /// The fence is **monotonic across all claimants**, which is what makes it useful: a worker that
    /// stalls, loses its lease, and wakes up still holding fence *N* cannot write once someone else
    /// has taken fence *N+1*. Enforcement lives in the store rather than the caller, because a worker
    /// that checks its own lease before writing is checking a fact that can expire between the check
    /// and the write.
    ///
    /// A store nobody has claimed enforces nothing - that is embedded mode, where there is exactly
    /// one process by construction and a fence would be ceremony.
    fn claim(&self, owner: &str) -> Result<u64>;

    /// Acquire the lease **if it is free, expired, or already ours**, bumping the fence.
    ///
    /// This is what a worker calls for itself, and it is the safe one: it refuses rather than
    /// stealing. [`claim`](HotStore::claim) is the unconditional form, for a scheduler performing a
    /// deliberate handover after a drain - it *knows* the previous holder is gone, so asking would be
    /// theatre. Two verbs because they answer different questions: "may I have this?" and "this is
    /// now yours".
    ///
    /// Expiry is measured on the **store's** clock, never the caller's. Worker clock skew would
    /// otherwise stretch or shorten leases invisibly, and the failure only shows up as two workers
    /// each believing they hold one - see RFC-0022's lease-placement note.
    fn acquire_lease(&self, owner: &str, ttl_secs: u64) -> Result<Lease>;

    /// Extend the lease this handle holds. Does **not** bump the fence: renewing is the same holder
    /// continuing, and a new fence would invalidate its own in-flight writes.
    fn renew_lease(&self, ttl_secs: u64) -> Result<Lease>;

    /// Give up the lease. The fence is deliberately left where it is - it is monotonic, and rewinding
    /// it would let a stale holder's number match again.
    fn release_lease(&self) -> Result<()>;

    /// The current lease, if any is recorded. Returns it even when expired, with a negative
    /// `expires_in_secs`, because "expired 40s ago" and "never leased" are different facts to a
    /// scheduler.
    fn current_lease(&self) -> Result<Option<Lease>>;

    /// The fence currently recorded in the store, regardless of who holds it.
    fn current_fence(&self) -> Result<u64>;

    /// The fence *this handle* believes it holds; `0` when it has never claimed.
    fn held_fence(&self) -> u64;

    // ---- delivery outbox --------------------------------------------------------------------
    fn outbox_push(&self, payload: &str) -> Result<u64>;
    fn outbox_pending(&self, limit: usize) -> Result<Vec<(u64, String)>>;
    fn outbox_remove(&self, seq: u64) -> Result<()>;
    async fn outbox_remove_batch_blocking(&self, seqs: Vec<u64>) -> Result<()>;
    fn outbox_len(&self) -> u64;
    fn outbox_trim(&self, max: u64) -> Result<u64>;

    /// Resolves once every handle on this store's file has dropped and the file may be opened
    /// again. Take it before dropping your own handle. A backend with no local file resolves at once.
    fn released(&self) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// The open database and the signal its last handle sends by dropping.
struct OpenDb {
    db: Database,
    // After `db`: fields drop in order, so the lock is gone before a waiter hears of it.
    _released: tokio::sync::watch::Sender<()>,
}

impl OpenDb {
    fn share(db: Database) -> (Arc<OpenDb>, tokio::sync::watch::Receiver<()>) {
        let (tx, rx) = tokio::sync::watch::channel(());
        (Arc::new(OpenDb { db, _released: tx }), rx)
    }
}

impl std::ops::Deref for OpenDb {
    type Target = Database;
    fn deref(&self) -> &Database {
        &self.db
    }
}

#[derive(Clone)]
pub struct Store {
    db: Arc<OpenDb>,
    released: tokio::sync::watch::Receiver<()>,
    /// Fence this handle holds, shared across clones so every clone of one nest's handle speaks for
    /// the same owner. `0` means unclaimed, which disables enforcement entirely.
    held: Arc<std::sync::atomic::AtomicU64>,
    /// Committed write transactions since this store was opened, shared across clones.
    writes: Arc<std::sync::atomic::AtomicU64>,
    /// [`Store::writes`] for the commits that change a hot row; see [`HotStore::rows_generation`].
    rows: Arc<std::sync::atomic::AtomicU64>,
    /// Held for the whole of a blocking-pool commit, which outlives an abort of the task that
    /// started it (#1767). [`HotStore::settle_commits`] waits on it.
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    /// Bytes of the largest `entities_in_range` answer, so a test can hold sealing to reading a cut.
    #[cfg(test)]
    pub(crate) largest_range_read: Arc<std::sync::atomic::AtomicUsize>,
    /// Rows visited by the longest single `scan_entities_in_range`, for the same reason.
    #[cfg(test)]
    pub(crate) largest_scan_rows: Arc<std::sync::atomic::AtomicUsize>,
    /// When set, a blocking commit signals the sender and parks on the receiver before committing.
    #[cfg(test)]
    pub(crate) commit_hold: CommitHold,
    #[cfg(test)]
    path: Arc<std::path::PathBuf>,
}

#[cfg(test)]
pub(crate) type CommitHold =
    Arc<std::sync::Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>>;

/// Per store file, a one-shot hold: the next outbox removal on it signals the sender and parks on
/// the receiver, so a test can stop a nest while a delivery is on the blocking pool.
#[cfg(test)]
pub(crate) fn outbox_holds() -> &'static std::sync::Mutex<OutboxHolds> {
    static HOLDS: std::sync::OnceLock<std::sync::Mutex<OutboxHolds>> = std::sync::OnceLock::new();
    HOLDS.get_or_init(Default::default)
}

#[cfg(test)]
pub(crate) type OutboxHolds = std::collections::HashMap<
    std::path::PathBuf,
    (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>),
>;

/// Does the store at `path` hold indexed rows, as opposed to merely existing?
///
/// [`Store::open`] materialises all four tables and commits before a cursor has fetched anything, so
/// the presence of the file proves only that a runtime once started here - not that it got anywhere.
/// Adoption turns on the difference in both directions (issue #408): an empty store counted as data
/// costs a dataset its one early cutoff, permanently.
///
/// **Non-creating, and no write txn** - `Database::open` rather than `Database::create` - because a
/// question about whether a store holds data must not be able to answer itself by creating one. This
/// is not the same as read-only: `Database::open` (`FileBackend::new`) takes the file `O_RDWR` and
/// holds an exclusive `flock` for the lifetime of the handle, before any transaction exists, and may
/// run crash recovery on it. A store held open elsewhere is refused here for that reason, not because
/// this call writes to it.
///
/// The three signals are the ones a cursor writes as it progresses: `last_block` once it has indexed
/// anything, `sealed_through` once it has sealed past finality, and rows in `entities`/`blocks` for
/// the tip that is not yet sealed. A dataset that has sealed everything and pruned hot has empty
/// tables and a non-zero watermark, which is why the meta keys are checked and not just the tables.
///
/// Errors are returned rather than folded into `false`: an unreadable store is not an empty one, and
/// the caller is the only one who knows which way to be wrong about it.
/// The meta key under which a store records the decode registry it was indexed by.
///
/// Lives here rather than in `indexer` because it is a property of the store's meta table, and two
/// spellings of one key is exactly the drift that [`recorded_registry_hash`] exists to close.
pub const REGISTRY_KEY: &str = "registry_hash";
/// Which formula [`REGISTRY_KEY`] was recorded under. Absent means the event registry alone; present
/// means the full decode identity, which also covers call, `[[ipfs]]` and `[[calls]]` declarations.
pub const IDENTITY_FORMULA_KEY: &str = "identity_formula";
pub const IDENTITY_FORMULA: &str = "2";
/// What the store's data covers beyond its decode, as JSON: chain, contracts and their start blocks,
/// factories, extraction (#1420).
pub const COVERAGE_KEY: &str = "indexed_coverage";

/// The formula this store recorded its registry hash under, read without creating anything.
pub fn recorded_identity_formula(path: &Path) -> Result<Option<String>> {
    let db = builder()
        .open(path)
        .with_context(|| format!("failed to open redb (non-creating) at {}", path.display()))?;
    let rtx = db.begin_read()?;
    let meta = rtx.open_table(META)?;
    Ok(meta
        .get(IDENTITY_FORMULA_KEY)?
        .map(|v| v.value().to_string()))
}

/// The registry hash this store **recorded at indexing time**, read without creating anything.
///
/// `Ok(None)` means the store exists and has no recorded hash - a fresh store, or one written by a
/// build from before the identity check (#653). An `Err` means the store could not be read at all:
/// absent, corrupt, or locked by a live cursor.
///
/// This is the fact `adoptable` was missing (#1369). It was deciding a candidate's identity by
/// re-running `build_manifest` over the candidate's files **with the running binary**, so a binary
/// that changed how a column decodes - and moved the hash with it - recomputed an old dataset to the
/// *new* identity and adopted it. `guard_registry_identity` then refused the adopted store at start,
/// so nothing mixed was ever served, but the one chance to adopt had been spent and the mount was
/// left holding a store it could never start from: #408's stuck shape through a different door.
pub fn recorded_registry_hash(path: &Path) -> Result<Option<String>> {
    let db = builder()
        .open(path)
        .with_context(|| format!("failed to open redb (non-creating) at {}", path.display()))?;
    let rtx = db.begin_read()?;
    let meta = rtx.open_table(META)?;
    Ok(meta.get(REGISTRY_KEY)?.map(|v| v.value().to_string()))
}

pub fn store_holds_rows(path: &Path) -> Result<bool> {
    let db = builder()
        .open(path)
        .with_context(|| format!("failed to open redb (non-creating) at {}", path.display()))?;
    let rtx = db.begin_read()?;
    let meta = rtx.open_table(META)?;
    for key in ["last_block", "sealed_through"] {
        if let Some(v) = meta.get(key)? {
            // `sealed_through = 0` is the "nothing sealed" default and is not progress; any
            // `last_block` at all is, including block 0.
            let v = v.value().to_string();
            if key == "last_block" || v.parse::<u64>().unwrap_or(0) > 0 {
                return Ok(true);
            }
        }
    }
    Ok(!rtx.open_table(ENTITIES)?.is_empty()? || !rtx.open_table(BLOCKS)?.is_empty()?)
}

impl Store {
    /// Open the store at `path`, **creating it if it is not there**. What a cursor starting up wants.
    pub fn open(path: &Path) -> Result<Store> {
        let db = builder()
            .create(path)
            .with_context(|| format!("failed to open redb at {}", path.display()))?;
        let store = Store::from_db(db)?;
        #[cfg(test)]
        let store = Store {
            path: Arc::new(path.to_path_buf()),
            ..store
        };
        Ok(store)
    }

    /// [`Store::open`] with a page cache of at most `cache_bytes`, for a store whose reads are point
    /// lookups and short ranges and whose cache would otherwise hold every page a backfill wrote.
    pub fn open_with_cache(path: &Path, cache_bytes: usize) -> Result<Store> {
        let mut b = Builder::new();
        b.set_cache_size(cache_bytes.clamp(HOT_STORE_CACHE_FLOOR, hot_store_cache_bytes()));
        let db = b
            .create(path)
            .with_context(|| format!("failed to open redb at {}", path.display()))?;
        Store::from_db(db)
    }

    /// Open a store that **already exists**, and fail rather than bring one into being.
    ///
    /// The distinction is the whole of issue #413. [`Store::open`] is `Database::create`, so a caller
    /// asking *"is there a store here?"* by opening one gets to answer its own question: `nuthatch
    /// sql` reported a local nest for any directory at all, queried the empty store it had just made
    /// instead of falling back to the running instance that held the data, and left the file behind
    /// to be mistaken for a dataset later. A probe must not be able to make itself true.
    ///
    /// Absent and locked both come back as errors, which is what the routing callers want: neither is
    /// a store they may read here. Callers that need to tell the two apart should check the path -
    /// [`store_holds_rows`] is the non-creating variant for "does it hold anything".
    ///
    /// **No write txn** (issue #471) - `Database::open` plus a read txn that checks the four tables
    /// are present, so nothing here commits a change. That is narrower than read-only: `Database::open`
    /// (`FileBackend::new`) takes the file `O_RDWR` and holds an exclusive `flock` for the lifetime of
    /// the handle, before any transaction exists, and may run crash recovery on it - so a store held
    /// by `dev` is refused here whether or not a write txn follows, and the fallback to HTTP does not
    /// depend on it. The write txn this used to carry was never about locking; its actual job was
    /// materialising tables a store written by an older nuthatch might lack, so a caller's first real
    /// read wouldn't hit redb's raw `TableDoesNotExist` instead of an answer. A read txn proves the
    /// same thing without committing - see the explicit check below.
    pub fn open_existing(path: &Path) -> Result<Store> {
        let db = builder()
            .open(path)
            .with_context(|| format!("failed to open an existing redb at {}", path.display()))?;
        {
            let rtx = db.begin_read().with_context(|| {
                format!("failed to begin a read transaction at {}", path.display())
            })?;
            for (name, table) in [
                ("entities", ENTITIES),
                ("meta", META),
                ("blocks", BLOCKS),
                ("outbox", OUTBOX),
            ] {
                rtx.open_table(table).with_context(|| {
                    format!(
                        "{} is missing the '{name}' table - this store predates it; open it with the \
                         nuthatch version that wrote it, or start a fresh store",
                        path.display()
                    )
                })?;
            }
        }
        let (db, released) = OpenDb::share(db);
        Ok(Store {
            db,
            released,
            held: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            writes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            rows: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            commit_gate: Arc::default(),
            #[cfg(test)]
            largest_range_read: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            largest_scan_rows: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            commit_hold: Arc::default(),
            #[cfg(test)]
            path: Arc::new(path.to_path_buf()),
        })
    }

    /// Commit a write transaction, bracketed by the generation counter. Every write this handle
    /// makes goes through here.
    ///
    /// The counter is bumped **before** the commit and again after it, so it is odd exactly while a
    /// commit may be becoming visible and even at rest. That is what makes the memo's fence exact
    /// (#1186, Jules on #1189): an atomic counter cannot be made simultaneous with redb's commit, so
    /// a reader that saw the old even value before its query and the same even value after it has
    /// proved that no commit *began* in between - the first bump would have shown. A reader that
    /// sees an odd value is told nothing (`write_generation` returns `None`) and computes without
    /// remembering. Bumping only after the commit left a window in which committed rows were visible
    /// under the old generation; bumping only before would let a pre-commit snapshot be remembered
    /// under the new one. Both bumps close both windows.
    ///
    /// A commit that fails leaves the counter odd, which disables the memo for the store's remaining
    /// life. That is the safe direction, and a failed redb commit is not a state this process carries
    /// on from in any case.
    fn commit(&self, wtx: redb::WriteTransaction) -> Result<()> {
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        wtx.commit()?;
        self.writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// [`Store::commit`] for a transaction that may add or remove hot rows: when `changed`, the rows
    /// counter is bracketed around it exactly as `writes` is, for the same reason.
    fn commit_rows(&self, wtx: redb::WriteTransaction, changed: bool) -> Result<()> {
        if !changed {
            return self.commit(wtx);
        }
        self.rows.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.commit(wtx)?;
        self.rows.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn from_db(db: Database) -> Result<Store> {
        // Materialise all four tables up front so read txns never hit a missing one. Only `open`
        // (the creating path) goes through this - `open_existing` takes no write txn, see #471.
        let wtx = db.begin_write()?;
        {
            wtx.open_table(ENTITIES)?;
            wtx.open_table(META)?;
            wtx.open_table(BLOCKS)?;
            wtx.open_table(OUTBOX)?;
        }
        if INDEX_CHECKPOINT_HASHES {
            // Rebuilt, not trusted: a default build may have written checkpoints since, and a
            // non-empty index is not a current one. One streaming pass over the checkpoints.
            wtx.delete_table(CHECKPOINT_HASHES)?;
            let blocks = wtx.open_table(BLOCKS)?;
            let mut hashes = wtx.open_table(CHECKPOINT_HASHES)?;
            for row in blocks.iter()? {
                let (key, value) = row?;
                let hash = decode_block_record(value.value()).0.to_ascii_lowercase();
                if !hash.is_empty() {
                    hashes.insert(hash.as_str(), key.value().parse::<u64>()?)?;
                }
            }
        }
        wtx.commit()?;
        let (db, released) = OpenDb::share(db);
        Ok(Store {
            db,
            released,
            held: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            writes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            rows: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            commit_gate: Arc::default(),
            #[cfg(test)]
            largest_range_read: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            largest_scan_rows: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(test)]
            commit_hold: Arc::default(),
            #[cfg(test)]
            path: Arc::default(),
        })
    }

    /// Push a pending alert delivery onto the durable outbox; returns its sequence number. A fast
    /// single redb write - enqueuing never blocks indexing on a slow/dead webhook (RFC-0008 C5).
    pub fn outbox_push(&self, payload: &str) -> Result<u64> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        let seq = Self::outbox_push_in(&wtx, &[payload.to_string()])?;
        self.commit(wtx)?;
        Ok(seq)
    }

    /// Push `payloads` inside `wtx`; returns the first one's sequence number.
    fn outbox_push_in(wtx: &redb::WriteTransaction, payloads: &[String]) -> Result<u64> {
        let mut meta = wtx.open_table(META)?;
        let first = meta
            .get(OUTBOX_SEQ)?
            .and_then(|v| v.value().parse::<u64>().ok())
            .unwrap_or(0);
        meta.insert(
            OUTBOX_SEQ,
            (first + payloads.len() as u64).to_string().as_str(),
        )?;
        let mut ob = wtx.open_table(OUTBOX)?;
        for (seq, payload) in (first..).zip(payloads) {
            ob.insert(Self::outbox_key(seq).as_str(), payload.as_str())?;
        }
        Ok(first)
    }

    fn outbox_key(seq: u64) -> String {
        format!("{seq:020}")
    }

    /// The oldest `limit` pending deliveries, as `(seq, payload)`, in enqueue order.
    pub fn outbox_pending(&self, limit: usize) -> Result<Vec<(u64, String)>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(OUTBOX)?;
        let mut out = Vec::with_capacity(limit.min(1024));
        for row in t.iter()? {
            let (k, v) = row?;
            let seq: u64 = k.value().parse().context("corrupt outbox key")?;
            out.push((seq, v.value().to_string()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Remove a delivered entry (call only after a successful POST - at-least-once semantics).
    pub fn outbox_remove(&self, seq: u64) -> Result<()> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        {
            let mut t = wtx.open_table(OUTBOX)?;
            t.remove(Self::outbox_key(seq).as_str())?;
        }
        self.commit(wtx)?;
        Ok(())
    }

    /// Number of pending deliveries - the `/status` outbox gauge.
    pub fn outbox_len(&self) -> u64 {
        let count = || -> Result<u64> {
            let rtx = self.db.begin_read()?;
            let t = rtx.open_table(OUTBOX)?;
            Ok(t.len()?)
        };
        count().unwrap_or(0)
    }

    /// Bound the outbox: if it exceeds `max`, drop the oldest entries down to `max`. Returns how many
    /// were dropped. This is the "never block the indexer" backstop - a dead webhook can't grow the
    /// outbox without limit; the oldest undelivered alerts are shed (loudly, by the caller).
    pub fn outbox_trim(&self, max: u64) -> Result<u64> {
        let len = self.outbox_len();
        if len <= max {
            return Ok(0);
        }
        let drop = len - max;
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        let mut dropped = 0u64;
        {
            let mut t = wtx.open_table(OUTBOX)?;
            let doomed: Vec<String> = t
                .iter()?
                .filter_map(|r| r.ok())
                .take(drop as usize)
                .map(|(k, _)| k.value().to_string())
                .collect();
            for k in doomed {
                t.remove(k.as_str())?;
                dropped += 1;
            }
        }
        self.commit(wtx)?;
        Ok(dropped)
    }

    /// Key entities as `{block:012}-{log_index:06}` so iteration is chain-ordered.
    pub fn entity_key(block: u64, log_index: u64) -> String {
        // The 6-digit zero-pad holds log_index up to 999,999; a 7-digit index would break the
        // zero-padded lexicographic ordering the range scans and prune bounds rely on. Unreachable at
        // real block gas limits (~80k logs); catch it in tests/CI rather than silently mis-order.
        debug_assert!(
            log_index < 1_000_000,
            "log_index {log_index} exceeds the 6-digit entity-key width"
        );
        format!("{block:012}-{log_index:06}")
    }

    fn block_key(block: u64) -> String {
        format!("{block:012}")
    }

    pub fn put_entity(&self, key: &str, json: &str) -> Result<()> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        self.guard_fence(&wtx)?;
        {
            let mut t = wtx.open_table(ENTITIES)?;
            t.insert(key, json)?;
        }
        self.commit_rows(wtx, true)?;
        Ok(())
    }

    /// See [`HotStore::put_entity_if_named`].
    pub fn put_entity_if_named(
        &self,
        key: &str,
        json: &str,
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool> {
        self.put_entities_if_named(
            &[(key.to_string(), json.to_string())],
            source_key,
            block_hash,
        )
    }

    /// See [`HotStore::put_entities_if_named`].
    pub fn put_entities_if_named(
        &self,
        entries: &[(String, String)],
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        let named = {
            let mut t = wtx.open_table(ENTITIES)?;
            let named = t
                .get(source_key)?
                .is_some_and(|v| row_block_hash(v.value()).as_deref() == Some(block_hash));
            if named {
                for (key, json) in entries {
                    t.insert(key.as_str(), json.as_str())?;
                }
            }
            named
        };
        if named {
            self.commit_rows(wtx, !entries.is_empty())?;
        } else {
            wtx.abort()?;
        }
        Ok(named)
    }

    /// Commit a whole window's writes in ONE transaction (PERF-2): every decoded row + annotation, the
    /// window-boundary block-hash checkpoint, and the `last_block` watermark. The tip loop previously
    /// did a separate `begin_write`/`commit` (an fsync) *per row* - 2,000 logs meant 2,000 fsyncs, which
    /// capped tip-follow throughput far below the decode rate. One txn per window is also *more*
    /// crash-consistent: the window is the atomic unit (its watermark already advances once), so a crash
    /// leaves the store at a clean window boundary, never mid-window.
    pub fn commit_window(
        &self,
        entities: &[(String, String)],
        checkpoint: Option<(u64, &str)>,
        last_block: u64,
    ) -> Result<()> {
        self.commit_window_with(entities, &[], checkpoint, last_block)
    }

    /// [`Store::commit_window`], with alert-outbox entries that land only if the window does.
    fn commit_window_with(
        &self,
        entities: &[(String, String)],
        outbox: &[String],
        checkpoint: Option<(u64, &str)>,
        last_block: u64,
    ) -> Result<()> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        if !outbox.is_empty() {
            Self::outbox_push_in(&wtx, outbox)?;
        }
        {
            let mut t = wtx.open_table(ENTITIES)?;
            for (k, v) in entities {
                t.insert(k.as_str(), v.as_str())?;
            }
            if let Some((block, hash)) = checkpoint {
                let mut b = wtx.open_table(BLOCKS)?;
                let key = Self::block_key(block);
                let existing_ts = b
                    .get(key.as_str())?
                    .and_then(|v| decode_block_record(v.value()).1);
                let (h, packed_ts) = decode_block_record(hash);
                let packed = encode_block_record(&h, packed_ts.or(existing_ts));
                if INDEX_CHECKPOINT_HASHES {
                    let mut hashes = wtx.open_table(CHECKPOINT_HASHES)?;
                    if let Some(old) = b.get(key.as_str())? {
                        let old_hash = decode_block_record(old.value()).0.to_ascii_lowercase();
                        hashes.remove(old_hash.as_str())?;
                    }
                    if !h.is_empty() {
                        hashes.insert(h.to_ascii_lowercase().as_str(), block)?;
                    }
                }
                b.insert(key.as_str(), packed.as_str())?;
            }
            let mut m = wtx.open_table(META)?;
            // A reorg walk stops at the newest checkpoint at or below the sealed watermark; nothing
            // below that one is walked. Keyed off the newest rather than the watermark itself, since
            // the pin at the watermark can fail (#1665). A graph build keeps them all for pinned
            // reads (#1636).
            if !INDEX_CHECKPOINT_HASHES {
                let sealed: u64 = m
                    .get("sealed_through")?
                    .and_then(|v| v.value().parse().ok())
                    .unwrap_or(0);
                if sealed > 0 {
                    let mut b = wtx.open_table(BLOCKS)?;
                    let at_or_below = Self::block_key(sealed);
                    let floor = b
                        .range(..=at_or_below.as_str())?
                        .next_back()
                        .transpose()?
                        .map(|(k, _)| k.value().to_string());
                    let below = floor.unwrap_or_default();
                    let doomed: Vec<String> = b
                        .range(..below.as_str())?
                        .take(CHECKPOINTS_PRUNED_PER_COMMIT)
                        .filter_map(|row| row.ok())
                        .map(|(k, _)| k.value().to_string())
                        .collect();
                    for k in doomed {
                        b.remove(k.as_str())?;
                    }
                }
            }
            m.insert("last_block", last_block.to_string().as_str())?;
        }
        self.commit_rows(wtx, !entities.is_empty())?;
        Ok(())
    }

    /// [`Store::commit_window`], off the async runtime's worker threads (audit F-C3).
    ///
    /// The commit ends in an fsync. On a contended disk that can park a tokio worker for far longer
    /// than any async task should hold one - and this process serves the HTTP API from the *same*
    /// runtime, so a slow ingest commit becomes head-of-line latency on unrelated requests. Moving it
    /// to the blocking pool makes a long fsync cost a blocking thread instead of a worker.
    ///
    /// Takes owned data because the work outlives the caller's borrow. `Store` is an `Arc` handle, so
    /// the clone is free.
    pub async fn commit_window_blocking(
        &self,
        entities: Vec<(String, String)>,
        outbox: Vec<String>,
        checkpoint: Option<(u64, String)>,
        last_block: u64,
    ) -> Result<()> {
        let store = self.clone();
        let gate = self.commit_gate.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let _gate = gate;
            #[cfg(test)]
            if let Some((entered, release)) = store.commit_hold.lock().unwrap().as_ref() {
                let _ = entered.send(());
                let _ = release.recv();
            }
            let cp = checkpoint.as_ref().map(|(b, h)| (*b, h.as_str()));
            store.commit_window_with(&entities, &outbox, cp, last_block)
        })
        .await
        .context("the hot-store commit task panicked")?
    }

    /// [`Store::outbox_remove`] for a batch of sequence numbers, off the runtime's workers (F-C3).
    ///
    /// The delivery worker drains the outbox one `outbox_remove` per delivered alert - an fsync each.
    /// A burst of alerts therefore parked a worker for as many fsyncs as there were deliveries.
    pub async fn outbox_remove_batch_blocking(&self, seqs: Vec<u64>) -> Result<()> {
        if seqs.is_empty() {
            return Ok(());
        }
        let store = self.clone();
        #[cfg(test)]
        let hold = outbox_holds().lock().unwrap().remove(&*self.path);
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some((entered, release)) = hold {
                let _ = entered.send(());
                let _ = release.recv();
            }
            for seq in seqs {
                // Best-effort per entry, exactly as the caller's loop was: a failed removal means the
                // alert is redelivered later, which the at-least-once contract already allows.
                let _ = store.outbox_remove(seq);
            }
            Ok(())
        })
        .await
        .context("the outbox drain task panicked")?
    }

    pub fn get_entity(&self, key: &str) -> Result<Option<String>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        Ok(t.get(key)?.map(|v| v.value().to_string()))
    }

    pub fn count(&self) -> Result<u64> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        Ok(t.len()?)
    }

    /// The `limit` most-recent entities (highest keys first).
    pub fn recent(&self, limit: usize) -> Result<Vec<String>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        let mut out = Vec::with_capacity(limit.min(1024));
        for row in t.iter()?.rev() {
            let (_k, v) = row?;
            out.push(v.value().to_string());
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// The `limit` most-recent hot rows belonging to `table` (highest keys first).
    ///
    /// A row that fails to parse as JSON is **not** the same thing as a row belonging to some other
    /// table (issue #567): every row in here is one we wrote, so an unparseable one is corruption, and
    /// this errors out rather than silently shortening the result the way `.ok().unwrap_or(false)`
    /// used to. That silence is what made #373's parse-failure arm in `rebuild_children` unreachable
    /// through this store - the corrupt row never survived to be rejected.
    pub fn recent_by_table(&self, table: &str, limit: usize) -> Result<Vec<String>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        // Cap the pre-allocation: `limit` may be usize::MAX (rebuild wants "all rows"); the Vec
        // still grows as needed, we just don't reserve an absurd capacity up front.
        let mut out = Vec::with_capacity(limit.min(1024));
        for row in t.iter()?.rev() {
            let (_k, v) = row?;
            let s = v.value();
            let parsed: serde_json::Value = serde_json::from_str(s).with_context(|| {
                format!("unparseable hot row while scanning for table '{table}'")
            })?;
            let matches = parsed
                .get("table")
                .and_then(|t| t.as_str())
                .is_some_and(|t| t == table);
            if matches {
                out.push(s.to_string());
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// The sealed watermark: the highest block whose rows have been sealed to Parquet and pruned from
    /// hot. Rows `> sealed_through` live in the hot store; rows `<= sealed_through` live in cold
    /// segments. `/sql` reads this to keep the hot∪cold union disjoint (COR-1). 0 if nothing sealed.
    pub fn sealed_through(&self) -> u64 {
        self.get_meta("sealed_through")
            .ok()
            .flatten()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// Every hot (unsealed) row, parsed and grouped by its logical `table` (RFC-0013). One full scan of
    /// the hot store - bounded, since sealed rows are pruned from hot, so this holds only the tip past
    /// the sealed watermark. Feeds the analytical `/sql` surface so the live tip is queryable alongside
    /// the sealed segments (hot and cold are disjoint by block range, so a plain `UNION ALL` is exact).
    pub fn hot_rows_by_table(
        &self,
    ) -> Result<std::collections::HashMap<String, Vec<serde_json::Value>>> {
        self.hot_rows_by_table_bounded(usize::MAX)
    }

    /// As [`Store::hot_rows_by_table`], but refusing to materialise more than `max_rows`.
    ///
    /// The unbounded version parses **every unsealed row into memory on every query**, which on a
    /// deep-finality chain with a busy contract is the largest RAM risk the process carries: the hot
    /// store holds everything between the sealed watermark and the tip, and a single `/sql` call can
    /// therefore breach the per-cursor budget and, in a runtime, take co-tenants down with it.
    ///
    /// It **fails** at the cap rather than truncating. Serving a partial tip would silently change the
    /// answer to an aggregate - a `count(*)` quietly missing rows is far worse than a query that
    /// refuses, and it is the same reasoning that makes a malformed log fail its window rather than be
    /// skipped. The caller turns this into a `503` with the numbers an operator needs.
    pub fn hot_rows_by_table_bounded(
        &self,
        max_rows: usize,
    ) -> Result<std::collections::HashMap<String, Vec<serde_json::Value>>> {
        Ok(self
            .hot_rows_by_table_bounded_with_bytes(max_rows, u64::MAX)?
            .rows)
    }

    /// The bounded analytical snapshot, counted from the exact redb values before parsing.
    pub fn hot_rows_by_table_bounded_with_bytes(
        &self,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<HotRowsSnapshot> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        let mut out = HotRowsSnapshot::default();
        let mut seen = 0usize;
        for row in t.iter()? {
            let (_k, v) = row?;
            out.source_rows += 1;
            out.source_bytes = out
                .source_bytes
                .checked_add(v.value().len() as u64)
                .context("hot source byte count overflow")?;
            if out.source_bytes > max_bytes {
                return Err(HotScanBudgetExceeded {
                    source_bytes: out.source_bytes,
                    budget: max_bytes,
                }
                .into());
            }
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(v.value()) {
                if let Some(table) = json.get("table").and_then(|t| t.as_str()) {
                    seen += 1;
                    if seen > max_rows {
                        return Err(HotScanTooLarge { cap: max_rows }.into());
                    }
                    out.rows.entry(table.to_string()).or_default().push(json);
                }
            }
        }
        Ok(out)
    }

    /// Entity JSON values whose block falls in `[from, to]`, chain-ordered. Used by sealing to
    /// gather a finalized block range for a Parquet segment.
    pub fn entities_in_range(&self, from: u64, to: u64) -> Result<Vec<String>> {
        let lo = format!("{from:012}-000000");
        let hi = format!("{to:012}-999999");
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        let mut out = Vec::new();
        for row in t.range(lo.as_str()..=hi.as_str())? {
            let (_k, v) = row?;
            out.push(v.value().to_string());
        }
        #[cfg(test)]
        self.largest_range_read.fetch_max(
            out.iter().map(String::len).sum(),
            std::sync::atomic::Ordering::SeqCst,
        );
        Ok(out)
    }

    /// [`entities_in_range`](Self::entities_in_range) one row at a time, the block read from the key.
    pub fn scan_entities_in_range(
        &self,
        from: u64,
        to: u64,
        visit: &mut dyn FnMut(u64, &str) -> bool,
    ) -> Result<()> {
        let lo = format!("{from:012}-000000");
        let hi = format!("{to:012}-999999");
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        #[cfg(test)]
        let mut visited = 0usize;
        for row in t.range(lo.as_str()..=hi.as_str())? {
            let (k, v) = row?;
            let key = k.value();
            let block = key
                .get(..12)
                .and_then(|b| b.parse::<u64>().ok())
                .with_context(|| format!("corrupt entity key {key:?}"))?;
            #[cfg(test)]
            {
                visited += 1;
            }
            if !visit(block, v.value()) {
                break;
            }
        }
        #[cfg(test)]
        self.largest_scan_rows
            .fetch_max(visited, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<String>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(META)?;
        Ok(t.get(key)?.map(|v| v.value().to_string()))
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        {
            let mut t = wtx.open_table(META)?;
            t.insert(key, value)?;
        }
        self.commit(wtx)?;
        Ok(())
    }

    /// Several meta writes and removals as one commit, for state a crash must not see half of.
    pub fn set_metas(&self, set: &[(&str, &str)], remove: &[&str]) -> Result<()> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        {
            let mut t = wtx.open_table(META)?;
            for (k, v) in set {
                t.insert(*k, *v)?;
            }
            for k in remove {
                t.remove(*k)?;
            }
        }
        self.commit(wtx)?;
        Ok(())
    }

    pub fn meta_with_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(META)?;
        let mut out = Vec::new();
        for row in t.range(prefix..)? {
            let (k, v) = row?;
            if out.len() >= limit || !k.value().starts_with(prefix) {
                break;
            }
            out.push((k.value().to_string(), v.value().to_string()));
        }
        Ok(out)
    }

    pub fn count_meta_with_prefix(&self, prefix: &str) -> Result<u64> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(META)?;
        let mut n = 0;
        for row in t.range(prefix..)? {
            if !row?.0.value().starts_with(prefix) {
                break;
            }
            n += 1;
        }
        Ok(n)
    }

    /// Record the canonical hash we indexed a block against (a reorg checkpoint).
    /// Keeps a timestamp already stored for this block, so a later hash-only pin cannot drop it.
    pub fn set_block_hash(&self, block: u64, hash: &str) -> Result<()> {
        self.write_block_record(block, Some(hash), None)
    }

    pub fn get_block_hash(&self, block: u64) -> Result<Option<String>> {
        Ok(self
            .block_record(block)?
            .map(|(h, _)| h)
            .filter(|h| !h.is_empty()))
    }

    pub fn set_block_timestamp(&self, block: u64, timestamp: u64) -> Result<()> {
        self.write_block_record(block, None, Some(timestamp))
    }

    pub fn get_block_timestamp(&self, block: u64) -> Result<Option<u64>> {
        Ok(self.block_record(block)?.and_then(|(_, ts)| ts))
    }

    fn block_record(&self, block: u64) -> Result<Option<(String, Option<u64>)>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(BLOCKS)?;
        Ok(t.get(Self::block_key(block).as_str())?
            .map(|v| decode_block_record(v.value())))
    }

    fn write_block_record(
        &self,
        block: u64,
        hash: Option<&str>,
        timestamp: Option<u64>,
    ) -> Result<()> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        {
            let mut t = wtx.open_table(BLOCKS)?;
            let key = Self::block_key(block);
            let (existing_hash, existing_ts) = t
                .get(key.as_str())?
                .map(|v| decode_block_record(v.value()))
                .unwrap_or((String::new(), None));
            let packed =
                encode_block_record(hash.unwrap_or(&existing_hash), timestamp.or(existing_ts));
            if INDEX_CHECKPOINT_HASHES {
                let mut hashes = wtx.open_table(CHECKPOINT_HASHES)?;
                hashes.remove(existing_hash.to_ascii_lowercase().as_str())?;
                let new_hash = hash.unwrap_or(&existing_hash).to_ascii_lowercase();
                if !new_hash.is_empty() {
                    hashes.insert(new_hash.as_str(), block)?;
                }
            }
            t.insert(key.as_str(), packed.as_str())?;
        }
        self.commit(wtx)?;
        Ok(())
    }

    /// The highest block this nest has indexed - the catch-up signal a hot upgrade polls (RFC-0020
    /// slice 2b). Takes the max of the hot-store `last_block` watermark and the sealed watermark, so it
    /// is correct whether the nest is tip-following or mid `seal-direct` backfill (which bypasses the
    /// hot store). `None` before anything is indexed.
    pub fn indexed_head(&self) -> Result<Option<u64>> {
        let hot = self
            .get_meta("last_block")?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let head = hot.max(self.sealed_through());
        Ok((head > 0).then_some(head))
    }

    /// All recorded checkpoints, highest block first - for walking back to a common ancestor.
    pub fn checkpoints_desc(&self) -> Result<Vec<(u64, String)>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(BLOCKS)?;
        let mut out = Vec::new();
        for row in t.iter()?.rev() {
            let (k, v) = row?;
            let block: u64 = k.value().parse().context("corrupt block key")?;
            let (hash, _) = decode_block_record(v.value());
            out.push((block, hash));
        }
        Ok(out)
    }

    pub fn checkpoint_number(&self, hash: &str) -> Result<Option<u64>> {
        if !INDEX_CHECKPOINT_HASHES {
            anyhow::bail!("hash-pinned reads need a nuthatch built with `--features graph`");
        }
        let rtx = self.db.begin_read()?;
        let table = rtx.open_table(CHECKPOINT_HASHES)
            .context("checkpoint hash index is unavailable; open this store with the current writer to upgrade it")?;
        Ok(table
            .get(hash.to_ascii_lowercase().as_str())?
            .map(|value| value.value()))
    }

    /// Reorg handling: drop every entity and checkpoint strictly above `block`. Returns the number
    /// of entities removed. The mutable hot store is the *only* place a reorg ever lands.
    pub fn rollback_to(&self, block: u64) -> Result<u64> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        let mut removed = 0u64;
        {
            let mut entities = wtx.open_table(ENTITIES)?;
            let above = format!("{:012}-", block.saturating_add(1));
            let doomed: Vec<String> = entities
                .range(above.as_str()..)?
                .filter_map(|row| row.ok())
                .map(|(k, _)| k.value().to_string())
                .collect();
            for k in doomed {
                entities.remove(k.as_str())?;
                removed += 1;
            }

            let mut blocks = wtx.open_table(BLOCKS)?;
            let above = Self::block_key(block.saturating_add(1));
            let doomed: Vec<String> = blocks
                .range(above.as_str()..)?
                .filter_map(|row| row.ok())
                .map(|(k, _)| k.value().to_string())
                .collect();
            for k in doomed {
                if let Some(value) = blocks.remove(k.as_str())? {
                    if INDEX_CHECKPOINT_HASHES {
                        wtx.open_table(CHECKPOINT_HASHES)?.remove(
                            decode_block_record(value.value())
                                .0
                                .to_ascii_lowercase()
                                .as_str(),
                        )?;
                    }
                }
            }
        }
        self.commit_rows(wtx, removed > 0)?;
        Ok(removed)
    }

    /// Reorg rollback **and** watermark reset in ONE write transaction (hardening the reorg path, the
    /// mirror of [`prune_and_set_meta`] for the forward path). Drops every entity and checkpoint
    /// strictly above `block` and writes `meta_key = meta_val` (the caller passes `last_block =
    /// ancestor`). Atomicity is essential: `rollback_to` + a *separate* `set_meta` could commit the
    /// delete and then lose the watermark reset to a `kill -9`, leaving `last_block` pointing past the
    /// fork - so the rolled-back blocks of the new canonical branch would never be re-indexed and the
    /// indexed range would carry a permanent, silent gap. As one txn, a crash lands cleanly on either
    /// side: pre-commit the whole rollback replays on restart; post-commit it is fully applied.
    pub fn rollback_to_and_set_meta(
        &self,
        block: u64,
        meta_key: &str,
        meta_val: &str,
        outbox: &[String],
    ) -> Result<u64> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        if !outbox.is_empty() {
            Self::outbox_push_in(&wtx, outbox)?;
        }
        let mut removed = 0u64;
        {
            let mut entities = wtx.open_table(ENTITIES)?;
            let above = format!("{:012}-", block.saturating_add(1));
            let doomed: Vec<String> = entities
                .range(above.as_str()..)?
                .filter_map(|row| row.ok())
                .map(|(k, _)| k.value().to_string())
                .collect();
            for k in doomed {
                entities.remove(k.as_str())?;
                removed += 1;
            }

            let mut blocks = wtx.open_table(BLOCKS)?;
            let above = Self::block_key(block.saturating_add(1));
            let doomed: Vec<String> = blocks
                .range(above.as_str()..)?
                .filter_map(|row| row.ok())
                .map(|(k, _)| k.value().to_string())
                .collect();
            for k in doomed {
                if let Some(value) = blocks.remove(k.as_str())? {
                    if INDEX_CHECKPOINT_HASHES {
                        wtx.open_table(CHECKPOINT_HASHES)?.remove(
                            decode_block_record(value.value())
                                .0
                                .to_ascii_lowercase()
                                .as_str(),
                        )?;
                    }
                }
            }

            let mut m = wtx.open_table(META)?;
            m.insert(meta_key, meta_val)?;
        }
        self.commit_rows(wtx, removed > 0)?;
        Ok(removed)
    }

    /// Prune sealed entities from the hot store: remove entity rows whose block is in `[from, to]`.
    /// Returns the number of rows removed. Called once every table in the range has been sealed to
    /// its own Parquet segment (the whole range is safe to drop; the data survives in Parquet and is
    /// reachable via the Burrmill point-read fallback).
    pub fn prune_range(&self, from: u64, to: u64) -> Result<u64> {
        let lo = format!("{from:012}-000000");
        let hi = format!("{to:012}-999999");
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        let mut removed = 0u64;
        {
            let mut t = wtx.open_table(ENTITIES)?;
            let doomed: Vec<String> = t
                .range(lo.as_str()..=hi.as_str())?
                .filter_map(|row| row.ok())
                .map(|(k, _)| k.value().to_string())
                .collect();
            for k in doomed {
                t.remove(k.as_str())?;
                removed += 1;
            }
        }
        self.commit_rows(wtx, removed > 0)?;
        Ok(removed)
    }

    /// Prune entities in `[from, to]` **and** set a meta key, in ONE write transaction (hardening
    /// COR-1). Sealing uses this to advance the `sealed_through` watermark and drop the just-sealed rows
    /// from hot *atomically* - a `kill -9` can never leave a range committed to both the hot store and a
    /// sealed segment, which would permanently double-count it in `/sql` and on every balance rebuild.
    /// The seal itself is content-addressed (idempotent), so a crash *before* this txn simply re-seals
    /// the same range on restart; the watermark only advances once the prune is durable.
    pub fn prune_and_set_meta(
        &self,
        from: u64,
        to: u64,
        meta_key: &str,
        meta_val: &str,
    ) -> Result<u64> {
        let lo = format!("{from:012}-000000");
        let hi = format!("{to:012}-999999");
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        let mut removed = 0u64;
        {
            let mut t = wtx.open_table(ENTITIES)?;
            let doomed: Vec<String> = t
                .range(lo.as_str()..=hi.as_str())?
                .filter_map(|row| row.ok())
                .map(|(k, _)| k.value().to_string())
                .collect();
            for k in doomed {
                t.remove(k.as_str())?;
                removed += 1;
            }
            let mut m = wtx.open_table(META)?;
            m.insert(meta_key, meta_val)?;
        }
        self.commit_rows(wtx, removed > 0)?;
        Ok(removed)
    }

    /// Test/consistency helper: the set of entity keys currently stored (chain-ordered).
    #[cfg(test)]
    pub fn entity_keys(&self) -> Result<Vec<String>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        let mut out = Vec::new();
        for row in t.iter()? {
            let (k, _) = row?;
            out.push(k.value().to_string());
        }
        Ok(out)
    }

    /// Up to `limit` entity keys, chain-ordered - the point-read bench (`nuthatch bench query`) samples
    /// from these. Bounded so a large hot store doesn't materialise every key just to time a few reads.
    pub fn sample_entity_keys(&self, limit: usize) -> Result<Vec<String>> {
        let rtx = self.db.begin_read()?;
        let t = rtx.open_table(ENTITIES)?;
        let mut out = Vec::with_capacity(limit.min(4096));
        for row in t.iter()? {
            let (k, _) = row?;
            out.push(k.value().to_string());
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

/// redb: the embedded implementation, and the only one until RFC-0022 slice 2 adds Postgres.
///
/// Every method delegates to the inherent one rather than reimplementing it. Deliberately so: the
/// inherent methods stay directly callable, and a delegating impl cannot drift from the behaviour the
/// existing suites already pin.
/// Ownership fencing for the embedded store (RFC-0022 slice 4).
///
/// redb is single-process, so in embedded mode this is inert by construction - nothing claims, the
/// held fence stays `0`, and no write is ever checked. It exists so both backends have the same shape
/// and the same tests: a guarantee that only one implementation can express is a guarantee nobody
/// can verify.
impl Store {
    /// The redb file itself, for RFC-0063's address-history tables, which live beside the hot store
    /// so the content address already excludes them.
    pub(crate) fn database(&self) -> &Database {
        &self.db
    }

    /// Read the persisted fence inside an already-open write transaction.
    fn fence_in_txn(wtx: &redb::WriteTransaction) -> Result<u64> {
        let t = wtx.open_table(META)?;
        let fence = t
            .get(OWNER_FENCE)?
            .and_then(|v| v.value().parse::<u64>().ok())
            .unwrap_or(0);
        Ok(fence)
    }

    /// `(owner, expires_at, fence)` read inside an open write transaction.
    fn lease_in_txn(wtx: &redb::WriteTransaction) -> Result<(String, i64, u64)> {
        let t = wtx.open_table(META)?;
        let owner = t
            .get(LEASE_OWNER)?
            .map(|v| v.value().to_string())
            .unwrap_or_default();
        let expires_at = t
            .get(LEASE_EXPIRES_AT)?
            .and_then(|v| v.value().parse::<i64>().ok())
            .unwrap_or(0);
        let fence = t
            .get(OWNER_FENCE)?
            .and_then(|v| v.value().parse::<u64>().ok())
            .unwrap_or(0);
        Ok((owner, expires_at, fence))
    }

    /// Refuse the write if this handle has been fenced out. A handle that never claimed is not
    /// subject to the check - see the note above.
    fn guard_fence(&self, wtx: &redb::WriteTransaction) -> Result<()> {
        let held = self.held.load(std::sync::atomic::Ordering::SeqCst);
        if held == 0 {
            return Ok(());
        }
        let current = Self::fence_in_txn(wtx)?;
        if current != held {
            return Err(LostOwnership { held, current }.into());
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl HotStore for Store {
    fn released(&self) -> futures::future::BoxFuture<'static, ()> {
        let mut rx = self.released.clone();
        // Nothing is ever sent, so this returns only once the sender has dropped.
        Box::pin(async move {
            let _ = rx.changed().await;
        })
    }

    fn write_generation(&self) -> Option<u64> {
        // Odd means a commit is in flight: see `Store::commit`.
        let g = self.writes.load(std::sync::atomic::Ordering::SeqCst);
        g.is_multiple_of(2).then_some(g)
    }

    fn rows_generation(&self) -> Option<u64> {
        let g = self.rows.load(std::sync::atomic::Ordering::SeqCst);
        g.is_multiple_of(2).then_some(g)
    }

    fn put_entity(&self, key: &str, json: &str) -> Result<()> {
        Store::put_entity(self, key, json)
    }
    fn put_entity_if_named(
        &self,
        key: &str,
        json: &str,
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool> {
        Store::put_entity_if_named(self, key, json, source_key, block_hash)
    }
    fn put_entities_if_named(
        &self,
        entries: &[(String, String)],
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool> {
        Store::put_entities_if_named(self, entries, source_key, block_hash)
    }
    fn get_entity(&self, key: &str) -> Result<Option<String>> {
        Store::get_entity(self, key)
    }
    fn count(&self) -> Result<u64> {
        Store::count(self)
    }
    fn recent(&self, limit: usize) -> Result<Vec<String>> {
        Store::recent(self, limit)
    }
    fn recent_by_table(&self, table: &str, limit: usize) -> Result<Vec<String>> {
        Store::recent_by_table(self, table, limit)
    }
    fn hot_rows_by_table(&self) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        Store::hot_rows_by_table(self)
    }
    fn hot_rows_by_table_bounded(
        &self,
        max_rows: usize,
    ) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        Store::hot_rows_by_table_bounded(self, max_rows)
    }
    fn hot_rows_by_table_bounded_with_bytes(
        &self,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<HotRowsSnapshot> {
        Store::hot_rows_by_table_bounded_with_bytes(self, max_rows, max_bytes)
    }
    fn entities_in_range(&self, from: u64, to: u64) -> Result<Vec<String>> {
        Store::entities_in_range(self, from, to)
    }
    fn scan_entities_in_range(
        &self,
        from: u64,
        to: u64,
        visit: &mut dyn FnMut(u64, &str) -> bool,
    ) -> Result<()> {
        Store::scan_entities_in_range(self, from, to, visit)
    }
    fn sample_entity_keys(&self, limit: usize) -> Result<Vec<String>> {
        Store::sample_entity_keys(self, limit)
    }
    fn get_meta(&self, key: &str) -> Result<Option<String>> {
        Store::get_meta(self, key)
    }
    fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        Store::set_meta(self, key, value)
    }
    fn meta_with_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
        Store::meta_with_prefix(self, prefix, limit)
    }
    fn count_meta_with_prefix(&self, prefix: &str) -> Result<u64> {
        Store::count_meta_with_prefix(self, prefix)
    }
    fn indexed_head(&self) -> Result<Option<u64>> {
        Store::indexed_head(self)
    }
    fn sealed_through(&self) -> u64 {
        Store::sealed_through(self)
    }
    fn set_block_hash(&self, block: u64, hash: &str) -> Result<()> {
        Store::set_block_hash(self, block, hash)
    }
    fn get_block_hash(&self, block: u64) -> Result<Option<String>> {
        Store::get_block_hash(self, block)
    }
    fn set_block_timestamp(&self, block: u64, timestamp: u64) -> Result<()> {
        Store::set_block_timestamp(self, block, timestamp)
    }
    fn get_block_timestamp(&self, block: u64) -> Result<Option<u64>> {
        Store::get_block_timestamp(self, block)
    }
    fn checkpoints_desc(&self) -> Result<Vec<(u64, String)>> {
        Store::checkpoints_desc(self)
    }
    fn checkpoint_number(&self, hash: &str) -> Result<Option<u64>> {
        Store::checkpoint_number(self, hash)
    }
    fn commit_window(
        &self,
        entities: &[(String, String)],
        checkpoint: Option<(u64, &str)>,
        last_block: u64,
    ) -> Result<()> {
        Store::commit_window(self, entities, checkpoint, last_block)
    }
    fn rollback_to(&self, block: u64) -> Result<u64> {
        Store::rollback_to(self, block)
    }
    fn rollback_to_and_set_meta(
        &self,
        block: u64,
        meta_key: &str,
        meta_val: &str,
        outbox: &[String],
    ) -> Result<u64> {
        Store::rollback_to_and_set_meta(self, block, meta_key, meta_val, outbox)
    }
    fn prune_range(&self, from: u64, to: u64) -> Result<u64> {
        Store::prune_range(self, from, to)
    }
    fn prune_and_set_meta(
        &self,
        from: u64,
        to: u64,
        meta_key: &str,
        meta_val: &str,
    ) -> Result<u64> {
        Store::prune_and_set_meta(self, from, to, meta_key, meta_val)
    }
    fn claim(&self, owner: &str) -> Result<u64> {
        // Deliberately unfenced: claiming is how a *new* holder takes over from a stale one, so
        // requiring the current fence here would make ownership impossible to transfer.
        let wtx = self.db.begin_write()?;
        let next = Self::fence_in_txn(&wtx)? + 1;
        {
            let mut t = wtx.open_table(META)?;
            t.insert(OWNER_FENCE, next.to_string().as_str())?;
            // Recorded for operators reading the store directly; the fence is what enforces.
            t.insert("owner", owner)?;
        }
        self.commit(wtx)?;
        self.held.store(next, std::sync::atomic::Ordering::SeqCst);
        Ok(next)
    }

    fn current_fence(&self) -> Result<u64> {
        Ok(Store::get_meta(self, OWNER_FENCE)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0))
    }

    fn acquire_lease(&self, owner: &str, ttl_secs: u64) -> Result<Lease> {
        let wtx = self.db.begin_write()?;
        let now = unix_now();
        let (holder, expires_at, fence) = Self::lease_in_txn(&wtx)?;
        // Free, expired, or ours - the three cases where taking it is not theft.
        let takeable = holder.is_empty() || expires_at <= now || holder == owner;
        if !takeable {
            return Err(LeaseHeld {
                by: holder,
                expires_in_secs: expires_at - now,
            }
            .into());
        }
        let next = fence + 1;
        let until = now + ttl_secs as i64;
        {
            let mut t = wtx.open_table(META)?;
            t.insert(OWNER_FENCE, next.to_string().as_str())?;
            t.insert(LEASE_OWNER, owner)?;
            t.insert(LEASE_EXPIRES_AT, until.to_string().as_str())?;
        }
        self.commit(wtx)?;
        self.held.store(next, std::sync::atomic::Ordering::SeqCst);
        Ok(Lease {
            owner: owner.to_string(),
            fence: next,
            expires_in_secs: ttl_secs as i64,
        })
    }

    fn renew_lease(&self, ttl_secs: u64) -> Result<Lease> {
        let wtx = self.db.begin_write()?;
        // Renewing is a write by the holder, so it is fenced like any other: a holder that has been
        // superseded must not be able to extend a lease it no longer has.
        self.guard_fence(&wtx)?;
        let (holder, _, fence) = Self::lease_in_txn(&wtx)?;
        let until = unix_now() + ttl_secs as i64;
        {
            let mut t = wtx.open_table(META)?;
            t.insert(LEASE_EXPIRES_AT, until.to_string().as_str())?;
        }
        self.commit(wtx)?;
        Ok(Lease {
            owner: holder,
            fence,
            expires_in_secs: ttl_secs as i64,
        })
    }

    fn release_lease(&self) -> Result<()> {
        let wtx = self.db.begin_write()?;
        self.guard_fence(&wtx)?;
        {
            let mut t = wtx.open_table(META)?;
            // Expire it rather than deleting the owner: "released by X at T" is a more useful thing
            // for an operator to find than an empty row.
            t.insert(LEASE_EXPIRES_AT, "0")?;
        }
        self.commit(wtx)?;
        Ok(())
    }

    fn current_lease(&self) -> Result<Option<Lease>> {
        let holder = Store::get_meta(self, LEASE_OWNER)?.unwrap_or_default();
        if holder.is_empty() {
            return Ok(None);
        }
        let expires_at: i64 = Store::get_meta(self, LEASE_EXPIRES_AT)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        Ok(Some(Lease {
            owner: holder,
            fence: HotStore::current_fence(self)?,
            expires_in_secs: expires_at - unix_now(),
        }))
    }

    fn held_fence(&self) -> u64 {
        self.held.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn outbox_push(&self, payload: &str) -> Result<u64> {
        Store::outbox_push(self, payload)
    }
    fn outbox_pending(&self, limit: usize) -> Result<Vec<(u64, String)>> {
        Store::outbox_pending(self, limit)
    }
    fn outbox_remove(&self, seq: u64) -> Result<()> {
        Store::outbox_remove(self, seq)
    }
    fn outbox_len(&self) -> u64 {
        Store::outbox_len(self)
    }
    fn outbox_trim(&self, max: u64) -> Result<u64> {
        Store::outbox_trim(self, max)
    }
    async fn commit_window_blocking(
        &self,
        entities: Vec<(String, String)>,
        outbox: Vec<String>,
        checkpoint: Option<(u64, String)>,
        last_block: u64,
    ) -> Result<()> {
        Store::commit_window_blocking(self, entities, outbox, checkpoint, last_block).await
    }
    async fn settle_commits(&self) {
        drop(self.commit_gate.lock().await);
    }
    async fn outbox_remove_batch_blocking(&self, seqs: Vec<u64>) -> Result<()> {
        Store::outbox_remove_batch_blocking(self, seqs).await
    }
}

/// Wall-clock seconds. redb is single-process, so there is one clock by construction and no skew to
/// worry about; the Postgres backend reads `now()` from the database instead, which is where skew
/// would otherwise bite.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// An `Arc<dyn HotStore>` is itself a `HotStore`.
///
/// Without this, every shared handle needs an explicit `&*store` at each call - noise that says
/// nothing, and that would only multiply under RFC-0022 where workers and FE nodes hold the store
/// behind an `Arc` by construction. Sharing a store is not a different capability from having one.
#[async_trait::async_trait]
impl<T: HotStore + ?Sized> HotStore for Arc<T> {
    fn released(&self) -> futures::future::BoxFuture<'static, ()> {
        (**self).released()
    }
    fn put_entity(&self, key: &str, json: &str) -> Result<()> {
        (**self).put_entity(key, json)
    }
    fn put_entity_if_named(
        &self,
        key: &str,
        json: &str,
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool> {
        (**self).put_entity_if_named(key, json, source_key, block_hash)
    }
    fn put_entities_if_named(
        &self,
        entries: &[(String, String)],
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool> {
        (**self).put_entities_if_named(entries, source_key, block_hash)
    }
    fn get_entity(&self, key: &str) -> Result<Option<String>> {
        (**self).get_entity(key)
    }
    fn count(&self) -> Result<u64> {
        (**self).count()
    }
    fn recent(&self, limit: usize) -> Result<Vec<String>> {
        (**self).recent(limit)
    }
    fn recent_by_table(&self, table: &str, limit: usize) -> Result<Vec<String>> {
        (**self).recent_by_table(table, limit)
    }
    fn hot_rows_by_table(&self) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        (**self).hot_rows_by_table()
    }
    fn hot_rows_by_table_bounded(
        &self,
        max_rows: usize,
    ) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        (**self).hot_rows_by_table_bounded(max_rows)
    }
    fn hot_rows_by_table_bounded_with_bytes(
        &self,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<HotRowsSnapshot> {
        (**self).hot_rows_by_table_bounded_with_bytes(max_rows, max_bytes)
    }
    fn entities_in_range(&self, from: u64, to: u64) -> Result<Vec<String>> {
        (**self).entities_in_range(from, to)
    }
    fn scan_entities_in_range(
        &self,
        from: u64,
        to: u64,
        visit: &mut dyn FnMut(u64, &str) -> bool,
    ) -> Result<()> {
        (**self).scan_entities_in_range(from, to, visit)
    }
    fn sample_entity_keys(&self, limit: usize) -> Result<Vec<String>> {
        (**self).sample_entity_keys(limit)
    }
    fn get_meta(&self, key: &str) -> Result<Option<String>> {
        (**self).get_meta(key)
    }
    fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        (**self).set_meta(key, value)
    }
    fn meta_with_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
        (**self).meta_with_prefix(prefix, limit)
    }
    fn count_meta_with_prefix(&self, prefix: &str) -> Result<u64> {
        (**self).count_meta_with_prefix(prefix)
    }
    fn indexed_head(&self) -> Result<Option<u64>> {
        (**self).indexed_head()
    }
    fn sealed_through(&self) -> u64 {
        (**self).sealed_through()
    }
    fn set_block_hash(&self, block: u64, hash: &str) -> Result<()> {
        (**self).set_block_hash(block, hash)
    }
    fn get_block_hash(&self, block: u64) -> Result<Option<String>> {
        (**self).get_block_hash(block)
    }
    fn set_block_timestamp(&self, block: u64, timestamp: u64) -> Result<()> {
        (**self).set_block_timestamp(block, timestamp)
    }
    fn get_block_timestamp(&self, block: u64) -> Result<Option<u64>> {
        (**self).get_block_timestamp(block)
    }
    fn checkpoints_desc(&self) -> Result<Vec<(u64, String)>> {
        (**self).checkpoints_desc()
    }
    fn checkpoint_number(&self, hash: &str) -> Result<Option<u64>> {
        (**self).checkpoint_number(hash)
    }
    fn commit_window(
        &self,
        entities: &[(String, String)],
        checkpoint: Option<(u64, &str)>,
        last_block: u64,
    ) -> Result<()> {
        (**self).commit_window(entities, checkpoint, last_block)
    }
    fn rollback_to(&self, block: u64) -> Result<u64> {
        (**self).rollback_to(block)
    }
    fn rollback_to_and_set_meta(
        &self,
        block: u64,
        meta_key: &str,
        meta_val: &str,
        outbox: &[String],
    ) -> Result<u64> {
        (**self).rollback_to_and_set_meta(block, meta_key, meta_val, outbox)
    }
    fn prune_range(&self, from: u64, to: u64) -> Result<u64> {
        (**self).prune_range(from, to)
    }
    fn prune_and_set_meta(
        &self,
        from: u64,
        to: u64,
        meta_key: &str,
        meta_val: &str,
    ) -> Result<u64> {
        (**self).prune_and_set_meta(from, to, meta_key, meta_val)
    }
    fn claim(&self, owner: &str) -> Result<u64> {
        (**self).claim(owner)
    }
    fn current_fence(&self) -> Result<u64> {
        (**self).current_fence()
    }
    fn acquire_lease(&self, owner: &str, ttl_secs: u64) -> Result<Lease> {
        (**self).acquire_lease(owner, ttl_secs)
    }
    fn renew_lease(&self, ttl_secs: u64) -> Result<Lease> {
        (**self).renew_lease(ttl_secs)
    }
    fn release_lease(&self) -> Result<()> {
        (**self).release_lease()
    }
    fn current_lease(&self) -> Result<Option<Lease>> {
        (**self).current_lease()
    }
    fn held_fence(&self) -> u64 {
        (**self).held_fence()
    }
    fn outbox_push(&self, payload: &str) -> Result<u64> {
        (**self).outbox_push(payload)
    }
    fn outbox_pending(&self, limit: usize) -> Result<Vec<(u64, String)>> {
        (**self).outbox_pending(limit)
    }
    fn outbox_remove(&self, seq: u64) -> Result<()> {
        (**self).outbox_remove(seq)
    }
    fn outbox_len(&self) -> u64 {
        (**self).outbox_len()
    }
    fn outbox_trim(&self, max: u64) -> Result<u64> {
        (**self).outbox_trim(max)
    }
    async fn commit_window_blocking(
        &self,
        entities: Vec<(String, String)>,
        outbox: Vec<String>,
        checkpoint: Option<(u64, String)>,
        last_block: u64,
    ) -> Result<()> {
        (**self)
            .commit_window_blocking(entities, outbox, checkpoint, last_block)
            .await
    }
    async fn settle_commits(&self) {
        (**self).settle_commits().await
    }
    async fn outbox_remove_batch_blocking(&self, seqs: Vec<u64>) -> Result<()> {
        (**self).outbox_remove_batch_blocking(seqs).await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "graph")]
    #[test]
    fn checkpoint_hash_index_upgrades_old_history_and_tracks_reorgs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.redb");
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            {
                tx.open_table(super::ENTITIES).unwrap();
                tx.open_table(super::META).unwrap();
                tx.open_table(super::OUTBOX).unwrap();
                let mut blocks = tx.open_table(super::BLOCKS).unwrap();
                for number in 1..=4100u64 {
                    blocks
                        .insert(
                            super::Store::block_key(number).as_str(),
                            format!("0x{number:064x}").as_str(),
                        )
                        .unwrap();
                }
            }
            tx.commit().unwrap();
        }
        let store = super::Store::open(&path).unwrap();
        assert_eq!(
            store.checkpoint_number(&format!("0x{:064x}", 1)).unwrap(),
            Some(1)
        );
        let replacement = format!("0x{}", "ff".repeat(32));
        store.set_block_hash(1, &replacement).unwrap();
        assert_eq!(
            store.checkpoint_number(&format!("0x{:064x}", 1)).unwrap(),
            None
        );
        assert_eq!(store.checkpoint_number(&replacement).unwrap(), Some(1));
        store
            .rollback_to_and_set_meta(3, "last_block", "3", &[])
            .unwrap();
        assert_eq!(
            store.checkpoint_number(&format!("0x{:064x}", 4)).unwrap(),
            None
        );
        assert_eq!(
            store.checkpoint_number(&format!("0x{:064x}", 3)).unwrap(),
            Some(3)
        );
        drop(store);
        let read_only = super::Store::open_existing(&path).unwrap();
        assert_eq!(read_only.checkpoint_number(&replacement).unwrap(), Some(1));
    }

    /// A default build may write checkpoints without the index, so a non-empty index left behind is
    /// stale, not current. Opening rebuilds it.
    #[cfg(feature = "graph")]
    #[test]
    fn a_stale_checkpoint_hash_index_is_rebuilt_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.redb");
        let stale = format!("0x{}", "ee".repeat(32));
        {
            let db = redb::Database::create(&path).unwrap();
            let tx = db.begin_write().unwrap();
            {
                tx.open_table(super::ENTITIES).unwrap();
                tx.open_table(super::META).unwrap();
                tx.open_table(super::OUTBOX).unwrap();
                let mut blocks = tx.open_table(super::BLOCKS).unwrap();
                blocks
                    .insert(
                        super::Store::block_key(7).as_str(),
                        format!("0x{:064x}", 7).as_str(),
                    )
                    .unwrap();
                tx.open_table(super::CHECKPOINT_HASHES)
                    .unwrap()
                    .insert(stale.as_str(), 7)
                    .unwrap();
            }
            tx.commit().unwrap();
        }
        let store = super::Store::open(&path).unwrap();
        assert_eq!(
            store.checkpoint_number(&format!("0x{:064x}", 7)).unwrap(),
            Some(7)
        );
        assert_eq!(store.checkpoint_number(&stale).unwrap(), None);
    }

    /// RFC-0060 §5.6: a default build's store has exactly main's tables.
    #[cfg(not(feature = "graph"))]
    #[test]
    fn a_default_build_keeps_no_checkpoint_hash_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.redb");
        let store = super::Store::open(&path).unwrap();
        store
            .set_block_hash(1, &format!("0x{}", "aa".repeat(32)))
            .unwrap();
        assert!(store.checkpoint_number("0x00").is_err());
        drop(store);
        let db = redb::Database::open(&path).unwrap();
        let rtx = db.begin_read().unwrap();
        assert!(rtx.open_table(super::CHECKPOINT_HASHES).is_err());
    }

    use super::*;

    #[test]
    fn hot_snapshot_counts_the_serialized_values_it_will_materialise() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("hot.redb")).unwrap();
        let row = r#"{"table":"t","block_number":1,"log_index":0,"value":"abc"}"#;
        store.put_entity("000000000001-000000", row).unwrap();
        let snapshot = store
            .hot_rows_by_table_bounded_with_bytes(1, row.len() as u64)
            .unwrap();
        assert_eq!(snapshot.source_bytes, row.len() as u64);
        assert_eq!(snapshot.rows["t"].len(), 1);
        assert!(store
            .hot_rows_by_table_bounded_with_bytes(1, row.len() as u64 - 1)
            .is_err());
    }
    use proptest::prelude::*;

    /// The memo's fence (#1186): even at rest, `None` while a commit is in flight, and one commit
    /// moves it by exactly two, so a reader comparing two `Some` values across a query has proved
    /// no commit began between them.
    #[test]
    fn write_generation_is_even_at_rest_and_moves_by_two_per_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let g0 = store
            .write_generation()
            .expect("a store at rest reports a generation");
        assert_eq!(g0 % 2, 0);
        store
            .put_entity("k1", r#"{"table":"t","block_number":1}"#)
            .unwrap();
        let g1 = store.write_generation().expect("at rest again");
        assert_eq!(g1, g0 + 2, "one commit, two bumps");
        // An in-flight commit is odd, and odd reads as `None`.
        store
            .writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(store.write_generation(), None);
        store
            .writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(store.write_generation(), Some(g1 + 2));
    }

    /// #1951: the commits a cursor makes on every poll - `last_block`, checkpoints, a prune or
    /// rollback that finds nothing - leave the rows generation alone, and every commit that adds or
    /// removes a hot row moves it by two.
    #[test]
    fn rows_generation_moves_only_with_hot_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let rows = |s: &Store| s.rows_generation().expect("at rest");
        let g0 = rows(&store);
        store.commit_window(&[], Some((5, "0xaa")), 5).unwrap();
        store.set_meta("sealed_through", "4").unwrap();
        assert_eq!(
            store
                .prune_and_set_meta(1, 4, "sealed_through", "4")
                .unwrap(),
            0
        );
        assert_eq!(store.prune_range(1, 4).unwrap(), 0);
        assert_eq!(store.rollback_to(9).unwrap(), 0);
        assert_eq!(
            store
                .rollback_to_and_set_meta(9, "last_block", "9", &[])
                .unwrap(),
            0
        );
        assert_eq!(rows(&store), g0, "no commit above touched a hot row");
        assert!(
            store.write_generation().unwrap() > g0,
            "though each one committed"
        );

        let row = |b: u64| {
            (
                Store::entity_key(b, 0),
                format!(r#"{{"table":"t","block_number":{b}}}"#),
            )
        };
        store
            .commit_window(&[row(6)], Some((6, "0xbb")), 6)
            .unwrap();
        assert_eq!(rows(&store), g0 + 2, "a window with a row");
        let (k, v) = row(7);
        store.put_entity(&k, &v).unwrap();
        assert_eq!(rows(&store), g0 + 4, "an entity put");
        assert_eq!(store.rollback_to(6).unwrap(), 1);
        assert_eq!(rows(&store), g0 + 6, "a rollback that removed a row");
        assert_eq!(
            store
                .prune_and_set_meta(6, 6, "sealed_through", "6")
                .unwrap(),
            1
        );
        assert_eq!(rows(&store), g0 + 8, "a prune that removed a row");
    }

    fn temp_store() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        (store, dir)
    }

    /// A block: its number and how many transfers (log indices) it carries.
    fn apply_block(store: &Store, block: u64, n_logs: u64, hash: &str) {
        for li in 0..n_logs {
            let key = Store::entity_key(block, li);
            store.put_entity(&key, "{}").unwrap();
        }
        store.set_block_hash(block, hash).unwrap();
    }

    /// Issue #408. `Store::open` creates the file and commits four empty tables before a cursor has
    /// fetched anything, so existence is not evidence of history. Each arm here is a state adoption
    /// can actually meet on disk, and the sealed-and-pruned one is the dangerous direction: its
    /// tables are empty, and a check that only looked at them would call a dataset holding real
    /// history empty and let adoption write over it.
    #[test]
    fn store_holds_rows_distinguishes_an_empty_store_from_one_with_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.redb");

        // Created and never written: the interrupted start.
        {
            Store::open(&path).unwrap();
        }
        assert!(
            !store_holds_rows(&path).unwrap(),
            "a store that was opened and never written holds nothing"
        );

        // Sealed to Parquet and pruned from hot: empty tables, non-zero watermark.
        {
            let s = Store::open(&path).unwrap();
            s.set_meta("sealed_through", "5000").unwrap();
        }
        assert!(
            store_holds_rows(&path).unwrap(),
            "a dataset that sealed its history and pruned hot still holds it"
        );

        // `sealed_through = 0` is the "nothing sealed" default, not progress.
        let dir2 = tempfile::tempdir().unwrap();
        let zeroed = dir2.path().join("t.redb");
        {
            let s = Store::open(&zeroed).unwrap();
            s.set_meta("sealed_through", "0").unwrap();
        }
        assert!(
            !store_holds_rows(&zeroed).unwrap(),
            "a zero watermark is the default and must not read as history"
        );

        // Rows at the tip, nothing sealed.
        let dir3 = tempfile::tempdir().unwrap();
        let hot = dir3.path().join("t.redb");
        {
            let s = Store::open(&hot).unwrap();
            apply_block(&s, 1, 1, "h1");
        }
        assert!(
            store_holds_rows(&hot).unwrap(),
            "unsealed rows at the tip are history too"
        );
    }

    /// Asking the question must not answer it: a non-creating open, or `holds_data` would leave a
    /// store behind in every dataset it inspected and make itself true everywhere.
    #[test]
    fn store_holds_rows_does_not_create_the_store_it_inspects() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.redb");
        assert!(
            store_holds_rows(&path).is_err(),
            "an absent store is an error, not a false"
        );
        assert!(!path.exists(), "and asking must not have created it");
    }

    /// Issue #413. The two opens differ in exactly one thing and every routing decision built on top
    /// of them turns on it: `open` brings a store into being, `open_existing` refuses to. A probe that
    /// creates gets to answer its own question - and leaves the evidence behind for the next reader.
    #[test]
    fn open_existing_refuses_to_create_the_store_it_was_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.redb");

        assert!(
            Store::open_existing(&path).is_err(),
            "there is no store here, so there is nothing to open"
        );
        assert!(
            !path.exists(),
            "and asking must not have created one at {path:?}"
        );

        // The same call succeeds once a store is actually there, so the error above is about absence
        // and not about `open_existing` being broken outright.
        drop(Store::open(&path).unwrap());
        let store = Store::open_existing(&path).expect("an existing store opens");
        store.set_meta("last_block", "7").unwrap();
        assert_eq!(store.get_meta("last_block").unwrap().as_deref(), Some("7"));
    }

    /// Issue #471. `open_existing` used to go through [`Store::from_db`], which commits a write txn to
    /// materialise the four tables - so every `nuthatch sql` against a free store rewrote the file at
    /// the same length with different bytes. Hash the whole file before and after: length alone would
    /// not have caught the original bug.
    #[test]
    fn open_existing_does_not_rewrite_the_bytes_of_the_store_it_reads() {
        use std::hash::{Hash, Hasher};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.redb");
        {
            let s = Store::open(&path).unwrap();
            apply_block(&s, 1, 2, "h1");
            s.set_meta("last_block", "1").unwrap();
        }

        let hash_of = |p: &std::path::Path| {
            let bytes = std::fs::read(p).unwrap();
            let mut h = std::collections::hash_map::DefaultHasher::new();
            bytes.hash(&mut h);
            (bytes.len(), h.finish())
        };
        let before = hash_of(&path);

        {
            let store = Store::open_existing(&path).expect("an existing store opens for reading");
            // Actually read through it, not just open it.
            assert_eq!(store.get_meta("last_block").unwrap().as_deref(), Some("1"));
        }

        let after = hash_of(&path);
        assert_eq!(
            before, after,
            "reading an existing store through open_existing must not change its bytes"
        );
    }

    /// Issue #471. The write txn `open_existing` used to commit was not pointless: it materialised
    /// tables a store written by an older nuthatch might lack, so a caller's first real read failed
    /// cleanly at open rather than confusingly later. The no-write-txn replacement must keep that
    /// property - so build a store missing one of the four tables and confirm the failure is early,
    /// at `open_existing`, and names the table, not a raw redb error surfacing on first use.
    #[test]
    fn open_existing_fails_early_and_legibly_when_a_table_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.redb");
        {
            let s = Store::open(&path).unwrap();
            apply_block(&s, 1, 1, "h1");
        }
        {
            let db = Database::open(&path).unwrap();
            let wtx = db.begin_write().unwrap();
            assert!(
                wtx.delete_table(OUTBOX).unwrap(),
                "the table must have existed to prove this simulates an older store, not a fresh one"
            );
            wtx.commit().unwrap();
        }

        let err = Store::open_existing(&path)
            .err()
            .expect("a store missing a table must fail at open, not on first use");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("outbox"),
            "the error should name the missing table, got: {msg}"
        );
    }

    #[test]
    fn prune_range_removes_only_blocks_in_range() {
        let (store, _d) = temp_store();
        apply_block(&store, 10, 2, "h10");
        apply_block(&store, 11, 3, "h11");
        apply_block(&store, 12, 1, "h12");
        let removed = store.prune_range(10, 11).unwrap();
        assert_eq!(removed, 5); // blocks 10 (2) + 11 (3)
        assert_eq!(store.count().unwrap(), 1); // only block 12 remains
    }

    #[test]
    fn rollback_is_multi_table_correct() {
        // Rows from two logical tables interleaved by block; a reorg must drop them uniformly by
        // block regardless of table (storage is block-keyed, so this is multi-table convergence).
        let (store, _d) = temp_store();
        store
            .put_entity(&Store::entity_key(10, 0), r#"{"table":"a__x"}"#)
            .unwrap();
        store
            .put_entity(&Store::entity_key(10, 1), r#"{"table":"b__y"}"#)
            .unwrap();
        store
            .put_entity(&Store::entity_key(12, 0), r#"{"table":"a__x"}"#)
            .unwrap();
        store
            .put_entity(&Store::entity_key(12, 1), r#"{"table":"b__y"}"#)
            .unwrap();
        store.rollback_to(11).unwrap();
        let keys = store.entity_keys().unwrap();
        assert_eq!(
            keys.len(),
            2,
            "both block-12 rows (both tables) rolled back"
        );
        assert!(keys.iter().all(|k| k.starts_with("000000000010")));
    }

    #[test]
    fn rollback_removes_only_blocks_above_threshold() {
        let (store, _d) = temp_store();
        apply_block(&store, 10, 3, "h10");
        apply_block(&store, 11, 2, "h11");
        apply_block(&store, 12, 4, "h12");
        assert_eq!(store.count().unwrap(), 9);

        let removed = store.rollback_to(11).unwrap();
        assert_eq!(removed, 4); // block 12's four entities
        assert_eq!(store.count().unwrap(), 5); // blocks 10 + 11
        assert!(store.get_block_hash(12).unwrap().is_none());
        assert_eq!(store.get_block_hash(11).unwrap().as_deref(), Some("h11"));
    }

    #[test]
    fn a_block_record_keeps_hash_and_timestamp() {
        let (store, _d) = temp_store();
        store.set_block_hash(12, "0xabc").unwrap();
        assert_eq!(store.get_block_hash(12).unwrap().as_deref(), Some("0xabc"));
        assert_eq!(store.get_block_timestamp(12).unwrap(), None);

        store.set_block_timestamp(12, 1_700_000_000).unwrap();
        assert_eq!(store.get_block_hash(12).unwrap().as_deref(), Some("0xabc"));
        assert_eq!(store.get_block_timestamp(12).unwrap(), Some(1_700_000_000));

        store.set_block_hash(12, "0xdef").unwrap();
        assert_eq!(store.get_block_hash(12).unwrap().as_deref(), Some("0xdef"));
        assert_eq!(store.get_block_timestamp(12).unwrap(), Some(1_700_000_000));

        store.commit_window(&[], Some((12, "0xghi")), 12).unwrap();
        assert_eq!(store.get_block_hash(12).unwrap().as_deref(), Some("0xghi"));
        assert_eq!(
            store.get_block_timestamp(12).unwrap(),
            Some(1_700_000_000),
            "a hash-only commit must not drop the timestamp"
        );
    }

    /// #1636: a window commit drops the checkpoints below the sealed watermark and keeps the one at
    /// it, which is where a reorg walk has to stop. A graph build keeps them all for pinned reads.
    #[test]
    fn a_window_commit_prunes_checkpoints_below_the_sealed_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        for b in 1..=10u64 {
            store
                .commit_window(&[], Some((b, &format!("0x{b:02x}"))), b)
                .unwrap();
        }
        store.set_meta("sealed_through", "6").unwrap();
        store.commit_window(&[], Some((11, "0x0b")), 11).unwrap();
        let kept: Vec<u64> = store
            .checkpoints_desc()
            .unwrap()
            .iter()
            .map(|c| c.0)
            .collect();
        if cfg!(feature = "graph") {
            assert_eq!(kept, (1..=11).rev().collect::<Vec<_>>());
        } else {
            assert_eq!(kept, (6..=11).rev().collect::<Vec<_>>());
        }
        assert_eq!(store.get_block_hash(6).unwrap().as_deref(), Some("0x06"));
    }

    /// #1665: the pin at the watermark can fail, leaving no checkpoint there. The newest one below
    /// it is then where a reorg walk stops, so the prune keeps that one rather than nothing.
    #[test]
    fn a_failed_watermark_pin_keeps_the_newest_checkpoint_below_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        for b in (1..=10u64).filter(|&b| b != 6) {
            store
                .commit_window(&[], Some((b, &format!("0x{b:02x}"))), b)
                .unwrap();
        }
        store.set_meta("sealed_through", "6").unwrap();
        store.commit_window(&[], Some((11, "0x0b")), 11).unwrap();
        let kept: Vec<u64> = store
            .checkpoints_desc()
            .unwrap()
            .iter()
            .map(|c| c.0)
            .collect();
        if cfg!(feature = "graph") {
            assert_eq!(kept, vec![11, 10, 9, 8, 7, 5, 4, 3, 2, 1]);
        } else {
            assert_eq!(kept, vec![11, 10, 9, 8, 7, 5]);
        }
    }
    /// The rollback scans start above the fork point, and still remove everything above it.
    #[test]
    fn a_rollback_removes_only_what_lies_above_the_fork() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        for b in 1..=5u64 {
            let rows = vec![(Store::entity_key(b, 0), "{}".to_string())];
            store
                .commit_window(&rows, Some((b, &format!("0x{b:02x}"))), b)
                .unwrap();
        }
        assert_eq!(
            store
                .rollback_to_and_set_meta(3, "last_block", "3", &[])
                .unwrap(),
            2
        );
        assert_eq!(
            store.entity_keys().unwrap(),
            vec![
                Store::entity_key(1, 0),
                Store::entity_key(2, 0),
                Store::entity_key(3, 0)
            ]
        );
        let kept: Vec<u64> = store
            .checkpoints_desc()
            .unwrap()
            .iter()
            .map(|c| c.0)
            .collect();
        assert_eq!(kept, vec![3, 2, 1]);
        assert_eq!(store.rollback_to(1).unwrap(), 2);
        assert_eq!(store.entity_keys().unwrap(), vec![Store::entity_key(1, 0)]);
    }

    /// #1410: a prefix read stops at the end of its prefix and at `limit`, in key order, and the count
    /// covers the same keys.
    #[test]
    fn meta_with_prefix_reads_only_its_own_keys_in_order() {
        let (store, _d) = temp_store();
        store.set_meta("ipfs_gave_up:000000000010:1", "b").unwrap();
        store.set_meta("ipfs_gave_up:000000000004:0", "a").unwrap();
        store.set_meta("ipfs_gave_up;after", "not ours").unwrap();
        store.set_meta("ipfs_gave_uo:before", "not ours").unwrap();
        assert_eq!(
            store.meta_with_prefix("ipfs_gave_up:", 10).unwrap(),
            [
                ("ipfs_gave_up:000000000004:0".to_string(), "a".to_string()),
                ("ipfs_gave_up:000000000010:1".to_string(), "b".to_string()),
            ]
        );
        assert_eq!(store.meta_with_prefix("ipfs_gave_up:", 1).unwrap().len(), 1);
        assert_eq!(store.count_meta_with_prefix("ipfs_gave_up:").unwrap(), 2);
    }

    #[test]
    fn rollback_to_and_set_meta_applies_both_in_one_txn() {
        let (store, _d) = temp_store();
        apply_block(&store, 10, 3, "h10");
        apply_block(&store, 11, 2, "h11");
        apply_block(&store, 12, 4, "h12");
        store.set_meta("last_block", "12").unwrap();

        // The reorg path must roll the hot store back AND reset the watermark together - never across
        // two txns (a crash between would strand `last_block` at 12 and permanently skip the re-org'd
        // range). One call does both.
        let removed = store
            .rollback_to_and_set_meta(11, "last_block", "11", &[])
            .unwrap();
        assert_eq!(removed, 4); // block 12's four entities dropped
        assert_eq!(store.count().unwrap(), 5); // blocks 10 + 11 survive
        assert!(store.get_block_hash(12).unwrap().is_none());
        assert_eq!(store.get_block_hash(11).unwrap().as_deref(), Some("h11"));
        // The watermark moved in the same transaction.
        assert_eq!(store.get_meta("last_block").unwrap().as_deref(), Some("11"));
    }

    /// Issue #567. A row that fails to parse must not read the same as a row belonging to some other
    /// table - both used to collapse to "excluded". Written through the real `Store::put_entity`
    /// (which never validates its `json` argument), not poked into redb directly, so this is the
    /// genuinely-corrupt row the issue asked for rather than a hand-built fixture.
    ///
    /// Mutation hazard named in the issue: a test that only asserts the corrupt row is *absent* from
    /// the result would still pass if the guard were deleted and `.ok().unwrap_or(false)` came back,
    /// since nothing else parses that row either. Asserting `Err` is what a reverted guard cannot
    /// survive.
    #[test]
    fn recent_by_table_errors_on_an_unparseable_row_instead_of_dropping_it() {
        let (store, _d) = temp_store();
        store
            .put_entity(&Store::entity_key(10, 0), r#"{"table":"a__x"}"#)
            .unwrap();
        store
            .put_entity(&Store::entity_key(10, 1), "not json")
            .unwrap();

        let err = store.recent_by_table("a__x", 10).expect_err(
            "an unparseable stored row must fail the scan, not silently shrink the result",
        );
        let msg = format!("{err:#}");
        assert!(
            msg.contains("a__x"),
            "the error should name the table being scanned, got: {msg}"
        );
    }

    /// The ordinary case the corrupt-row guard must not break: a well-formed row belonging to a
    /// different table is a normal non-match, not an error.
    #[test]
    fn recent_by_table_skips_rows_from_other_tables_without_erroring() {
        let (store, _d) = temp_store();
        store
            .put_entity(&Store::entity_key(10, 0), r#"{"table":"a__x"}"#)
            .unwrap();
        store
            .put_entity(&Store::entity_key(10, 1), r#"{"table":"b__y"}"#)
            .unwrap();

        let rows = store.recent_by_table("a__x", 10).unwrap();
        assert_eq!(rows, vec![r#"{"table":"a__x"}"#.to_string()]);
    }

    proptest! {
        // Each case opens a real redb file, so keep the count modest for CI wall-clock.
        #![proptest_config(ProptestConfig::with_cases(48))]
        /// Reorg convergence: indexing a chain then reorging at a fork point and applying an
        /// alternate branch must yield exactly the same state as indexing the winning branch
        /// directly. Random fork depths, random block populations.
        #[test]
        fn reorg_converges_to_canonical(
            prefix in prop::collection::vec(1u64..5, 1..8),   // logs-per-block, blocks 0..len
            branch in prop::collection::vec(1u64..5, 0..6),   // alternate branch after the fork
            fork_back in 0usize..8,
        ) {
            // Build the "reorged" store: apply prefix, roll back, apply the alternate branch.
            let (reorged, _d1) = temp_store();
            for (i, &n) in prefix.iter().enumerate() {
                apply_block(&reorged, i as u64, n, &format!("a{i}"));
            }
            let fork = (prefix.len().saturating_sub(fork_back)).saturating_sub(1) as u64;
            reorged.rollback_to(fork).unwrap();
            for (j, &n) in branch.iter().enumerate() {
                let b = fork + 1 + j as u64;
                apply_block(&reorged, b, n, &format!("b{j}"));
            }

            // Build the "canonical" store fresh: prefix up to the fork, then the same branch.
            let (canonical, _d2) = temp_store();
            for (i, &n) in prefix.iter().enumerate() {
                if (i as u64) <= fork {
                    apply_block(&canonical, i as u64, n, &format!("a{i}"));
                }
            }
            for (j, &n) in branch.iter().enumerate() {
                let b = fork + 1 + j as u64;
                apply_block(&canonical, b, n, &format!("b{j}"));
            }

            prop_assert_eq!(reorged.entity_keys().unwrap(), canonical.entity_keys().unwrap());
        }
    }
}
