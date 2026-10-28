//! The hot store's contract (RFC-0022 slice 1), here since RFC-0075 S1 so a tab's store implements
//! the trait the cursor writes through. The node's backends and its feed stay in the binary.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// A held lease on a cursor (RFC-0022 slice 4b).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub owner: String,
    /// The fence issued with this lease. Every write the holder makes carries it.
    pub fence: u64,
    /// Seconds until expiry, by the store's clock. Negative means already expired.
    pub expires_in_secs: i64,
}

/// One consistent hot-store read for an analytical query. `source_bytes` is the length of the
/// serialized redb values that will be parsed into Burrmill, not a JSON re-encoding or a resident-RAM
/// estimate (RFC-0048 S2).
#[derive(Debug, Default)]
pub struct HotRowsSnapshot {
    pub rows: HashMap<String, Vec<serde_json::Value>>,
    /// Values read, whether or not they parsed into a row.
    pub source_rows: u64,
    pub source_bytes: u64,
    /// `last_block` as of this same read, so a head taken from it never runs ahead of these rows.
    pub last_block: Option<u64>,
}

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
/// - **One cursor writer.** Exactly one ingestion task advances a given store's chain state. An
///   implementation may assume it and must not silently repair concurrent cursor writes - under
///   RFC-0022 the assumption becomes a cursor lease, and a backend that quietly tolerated two would
///   hide the very bug the lease exists to prevent. Two other tasks write beside it (see the module
///   doc): the outbox drain, and the IPFS resolver, whose
///   [`put_entities_if_named`](HotStore::put_entities_if_named) must check its naming row and write
///   in one transaction serialised against the cursor's.
/// - **Keys order by block.** `Store::entity_key` is zero-padded so lexicographic order is block
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

    /// The catalogue this store's watermark pairs with, where it carries one rather than the disk
    /// (RFC-0073 FR1): `Some(None)` when its dataset has never sealed.
    fn served_catalogue(&self) -> Option<Option<Arc<str>>> {
        None
    }

    /// A request served from this store found a segment its catalogue names missing on disk.
    fn segment_missing(&self) {}

    /// `(epoch, generation)` of the feed's last commit, `None` from a backend with no feed.
    fn feed_position(&self) -> Option<(u64, u64)> {
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
    /// [`HotStore::recent_by_table`] reading at most `max_scanned` rows, `None` past that (#2209). A
    /// backend that filters on its own side has nothing to bound here.
    fn recent_by_table_within(
        &self,
        table: &str,
        limit: usize,
        _max_scanned: usize,
    ) -> Result<Option<Vec<String>>> {
        self.recent_by_table(table, limit).map(Some)
    }
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
    /// As `hot_rows_by_table_bounded_with_bytes`, handing `charge` the snapshot's source bytes before
    /// a row is parsed (RFC-0073 FR5). A tip the copy would refuse is refused before `charge`.
    fn hot_rows_charged(
        &self,
        max_rows: usize,
        max_bytes: u64,
        charge: &mut dyn FnMut(u64) -> Result<()>,
    ) -> Result<HotRowsSnapshot>;
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
    fn released(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::ready(()))
    }
}

/// An `Arc<dyn HotStore>` is itself a `HotStore`.
///
/// Without this, every shared handle needs an explicit `&*store` at each call - noise that says
/// nothing, and that would only multiply under RFC-0022 where workers and FE nodes hold the store
/// behind an `Arc` by construction. Sharing a store is not a different capability from having one.
#[async_trait::async_trait]
impl<T: HotStore + ?Sized> HotStore for Arc<T> {
    fn released(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        (**self).released()
    }
    fn served_catalogue(&self) -> Option<Option<Arc<str>>> {
        (**self).served_catalogue()
    }
    fn segment_missing(&self) {
        (**self).segment_missing()
    }
    fn feed_position(&self) -> Option<(u64, u64)> {
        (**self).feed_position()
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
    fn recent_by_table_within(
        &self,
        table: &str,
        limit: usize,
        max_scanned: usize,
    ) -> Result<Option<Vec<String>>> {
        (**self).recent_by_table_within(table, limit, max_scanned)
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
    fn hot_rows_charged(
        &self,
        max_rows: usize,
        max_bytes: u64,
        charge: &mut dyn FnMut(u64) -> Result<()>,
    ) -> Result<HotRowsSnapshot> {
        (**self).hot_rows_charged(max_rows, max_bytes, charge)
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
