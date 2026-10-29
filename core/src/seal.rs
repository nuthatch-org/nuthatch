//! Sealing a finalized range (RFC-0075 §6): which segments a cut writes, folds and catalogues. Where
//! the bytes go is a [`SegmentSink`]'s business; the node's writes files, a tab's writes OPFS.

use crate::fault::FaultPoints;
use crate::metrics::CursorMetrics;
use crate::source::Source;
use crate::store::{HotStore, LAST_BLOCK_KEY, SEALED_THROUGH_KEY, START_BLOCK_KEY};
use anyhow::{Context, Result};
use nuthatch_seal::cut::{aged, block_timestamp, CutScan, Segment, SEAL_DIRECT_BATCH};
use nuthatch_seal::{rows_to_batch, sort_rows_for_seal, write_parquet, WRITER_PROFILE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Catalogue version this binary writes. Absent in a file on disk is version 0 (RFC-0047 C2).
pub const MANIFEST_VERSION: u32 = 1;

/// The segment catalogue: per-table lists of sealed segments.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    /// 0 when the field is absent (every catalogue written before RFC-0047 C2). This binary writes
    /// [`MANIFEST_VERSION`]. Additive fields bump nothing; a breaking catalogue change bumps this.
    #[serde(default)]
    pub manifest_version: u32,
    pub tables: BTreeMap<String, Vec<Segment>>,
}

/// What a `seal_range` call sealed.
#[derive(Debug, Default)]
pub struct SealSummary {
    pub tables: usize,
    pub rows: usize,
}

/// Where sealed bytes and the catalogue live. One sink per seal: it may hold what it publishes until
/// [`SegmentSink::install`] makes the catalogue that names it durable.
pub trait SegmentSink {
    /// The catalogue as it stands, ready to be written beside.
    fn manifest(&mut self) -> Result<Manifest>;
    /// Whether `s`'s bytes are present and hash to its address.
    fn intact(&self, s: &Segment) -> bool;
    /// Put `bytes`, which are `s`'s own, back where `s` names, over a lost or corrupt copy (#2266).
    fn restore(&mut self, s: &Segment, bytes: &[u8]) -> Result<()>;
    /// The rows a sealed segment holds.
    fn rows(&self, s: &Segment) -> Result<Vec<Value>>;
    /// The row count under which a table's newest segment stays provisional (#1150).
    fn table_floor(&self) -> usize;
    /// Write a new segment's bytes. `replaced` is the provisional segment it folds, whose file must
    /// stay readable until [`SegmentSink::install`].
    fn publish(
        &mut self,
        file: &str,
        hash: &str,
        bytes: &[u8],
        replaced: Option<&Segment>,
    ) -> Result<()>;
    /// Install `manifest` durably, then let go of whatever it no longer names.
    fn install(&mut self, manifest: &Manifest) -> Result<()>;
}

/// Seal every table's rows in a finalized `[from, to]` range. Rows are grouped by their `table`
/// field; each group becomes one content-addressed Parquet segment catalogued under its table.
/// `registry_snapshot` is the discovered-child registry's hash for a factory nest (RFC-0009).
/// Returns None if the range held no rows.
pub fn seal_range(
    sink: &mut dyn SegmentSink,
    entity_json: &[String],
    from: u64,
    to: u64,
    registry_snapshot: Option<&str>,
) -> Result<Option<SealSummary>> {
    let mut by_table: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut cut_time: Option<u64> = None;
    for j in entity_json {
        let Ok(v) = serde_json::from_str::<Value>(j) else {
            continue;
        };
        cut_time = cut_time.max(block_timestamp(&v));
        let table = v
            .get("table")
            .and_then(Value::as_str)
            .unwrap_or("rows")
            .to_string();
        by_table.entry(table).or_default().push(v);
    }
    if by_table.is_empty() {
        return Ok(None);
    }

    let mut manifest = sink.manifest()?;
    let mut summary = SealSummary::default();
    let touched: BTreeSet<String> = by_table.keys().cloned().collect();

    for (table, mut rows) in by_table {
        sort_rows_for_seal(&mut rows);
        let batch = rows_to_batch(&rows)?;
        let bytes = write_parquet(&batch)?;
        let hash = hex::encode(Sha256::digest(&bytes));
        let segments = manifest.tables.entry(table.clone()).or_default();
        // Content-addressed idempotency: an identical segment (same table + hash) is already
        // catalogued, so re-sealing the same rows - e.g. a re-run over a range to
        // re-audit - is a no-op rather than a double-listed (double-counted) segment. Checked on the
        // incoming rows alone, before any fold. A fold's file hash is the folded bytes, so the
        // incoming hash is kept beside it (#1631).
        let input_hash = hash.clone();
        // Only beside an intact copy: skipping beside a lost file, then pruning hot, would leave
        // these rows in neither (#2261).
        if let Some(s) = segments
            .iter()
            .find(|s| s.hash == hash || s.input_hash.as_deref() == Some(hash.as_str()))
        {
            if sink.intact(s) {
                continue;
            }
            if s.hash == hash {
                // These are the bytes the address names, so they replace a corrupt file in place,
                // shared or not (#2266). A replace that fails fails the seal, and hot is kept.
                sink.restore(s, &bytes)
                    .with_context(|| format!("restoring segment {}", s.file))?;
                continue;
            }
            // A lost fold held these rows; they are sealed again as a segment of their own.
        }
        let new_rows = rows.len();

        // Fold this table's provisional segment in, if it has one (#1150): its rows come first,
        // being from earlier cuts, and its `from_block` is the segment's. Read back from the
        // Parquet it was written to rather than kept anywhere else, so there is one copy of the
        // truth and the folded file is byte-identical to sealing all of those rows at once.
        let pending = segments.iter().position(|s| s.provisional);
        let (rows, from, bytes, hash, replaced) = match pending {
            None => (rows, from, bytes, hash, None),
            Some(i) => {
                let prev = segments.remove(i);
                let mut all = sink.rows(&prev).with_context(|| {
                    format!("reading provisional segment {} to fold it", prev.file)
                })?;
                all.extend(rows);
                sort_rows_for_seal(&mut all);
                let bytes = write_parquet(&rows_to_batch(&all)?)?;
                let hash = hex::encode(Sha256::digest(&bytes));
                (all, prev.from_block, bytes, hash, Some(prev))
            }
        };
        let provisional = nuthatch_seal::cut::provisional(&rows, sink.table_floor(), cut_time);
        let file = format!("{table}-{hash}.parquet");
        sink.publish(&file, &hash, &bytes, replaced.as_ref())?;
        summary.tables += 1;
        // New rows only: the folded ones were counted when they were first sealed, and
        // `nuthatch_rows_sealed_total` is a count of rows, not of writes.
        summary.rows += new_rows;
        segments.push(Segment {
            hash,
            from_block: from,
            to_block: to,
            rows: rows.len(),
            file,
            // A folded segment records the snapshot at its last write. Discovery is append-only,
            // so the latest snapshot covers every row in the file; the earlier ones covered less.
            registry_snapshot: registry_snapshot.map(str::to_string),
            provisional,
            writer_profile: WRITER_PROFILE.to_string(),
            input_hash: Some(input_hash),
        });
    }

    // A table with no rows in this cut is otherwise never revisited, so a contract that has gone
    // quiet would hold its tail for ever. The file is kept; only the flag changes, as in
    // `finalise_provisional`.
    for (table, segments) in &mut manifest.tables {
        if cut_time.is_none() || touched.contains(table) {
            continue;
        }
        for s in segments.iter_mut().filter(|s| s.provisional) {
            let rows = sink
                .rows(s)
                .with_context(|| format!("reading provisional segment {} to age it", s.file))?;
            if aged(&rows, cut_time) {
                s.provisional = false;
            }
        }
    }

    manifest.manifest_version = MANIFEST_VERSION;
    sink.install(&manifest)?;
    Ok(Some(summary))
}

/// `block:hash` of the checkpoint `maybe_seal` last pinned while holding rows (#1495).
pub const SEAL_PIN_KEY: &str = "seal_pin";

/// Whether that pin still anchors the reorg walk: at or above the watermark (`from - 1`), at or
/// below `ceiling`, and still the checkpoint the store holds at its block.
fn held_pin_covers(store: &dyn HotStore, from: u64, ceiling: u64) -> Result<bool> {
    let Some(pin) = store.get_meta(SEAL_PIN_KEY)? else {
        return Ok(false);
    };
    let Some((block, hash)) = pin
        .split_once(':')
        .and_then(|(b, h)| Some((b.parse::<u64>().ok()?, h)))
    else {
        return Ok(false);
    };
    Ok(block + 1 >= from
        && block <= ceiling
        && store.get_block_hash(block)?.as_deref() == Some(hash))
}

/// Checkpoint `block` with the source's hash before the watermark may move to it (#1665). `false`
/// when the source could not answer; the watermark then stays where it is until a later poll.
pub async fn pin_checkpoint(store: &dyn HotStore, source: &dyn Source, block: u64) -> Result<bool> {
    match source.block_hash(block).await {
        Ok(Some(hash)) => {
            store.set_block_hash(block, &hash)?;
            Ok(true)
        }
        Ok(None) | Err(_) => {
            tracing::debug!("no hash for block {block}; the sealed watermark waits for one");
            Ok(false)
        }
    }
}

/// Opens a [`SegmentSink`] for each seal.
pub trait Sinks: Send + Sync {
    fn open(&self) -> Box<dyn SegmentSink + '_>;
}

/// What a tip-path seal reads and writes through.
#[derive(Clone, Copy)]
pub struct TipSeal<'a> {
    pub sinks: &'a dyn Sinks,
    pub faults: &'a dyn FaultPoints,
    pub store: &'a dyn HotStore,
    pub metrics: &'a dyn CursorMetrics,
    /// The chain's `seal_span` (#1199).
    pub seal_span: u64,
}

/// Seal finalized rows that have accumulated to [`SEAL_DIRECT_BATCH`], cutting at a block boundary
/// chosen from the data. Rows short of the threshold stay in the hot store until the next call.
///
/// Earlier write-ups called this `seal_finalized`. It sealed `from..=ceiling` with no row threshold,
/// which is why 80% of a long-running nest's segments were under 20 KB (docs/bench/segment-layout.md).
///
/// The outcome goes to `metrics`, so `/ready` sees a seal that fails on every attempt (#2262).
pub async fn maybe_seal(
    seal: TipSeal<'_>,
    source: &dyn Source,
    finalized_through: u64,
    registry_snapshot: Option<&str>,
) -> Result<()> {
    let sealed = try_seal(seal, source, finalized_through, registry_snapshot).await;
    seal.metrics
        .record_seal(sealed.as_ref().err().map(|e| format!("{e:#}")));
    sealed
}

async fn try_seal(
    seal: TipSeal<'_>,
    source: &dyn Source,
    finalized_through: u64,
    registry_snapshot: Option<&str>,
) -> Result<()> {
    let TipSeal {
        sinks,
        faults,
        store,
        metrics,
        seal_span,
    } = seal;
    if finalized_through == 0 {
        return Ok(());
    }
    let last_indexed = match store.get_meta(LAST_BLOCK_KEY)? {
        Some(v) => v.parse::<u64>().context("corrupt last_block")?,
        None => return Ok(()),
    };
    let ceiling = finalized_through.min(last_indexed);

    let watermark = match store.get_meta(SEALED_THROUGH_KEY)? {
        Some(v) => Some(v.parse::<u64>().context("corrupt sealed_through")?),
        None => None,
    };
    let mut from = match watermark {
        Some(w) => w + 1,
        None => store
            .get_meta(START_BLOCK_KEY)?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0),
    };
    if ceiling < from {
        // A watermark with no checkpoint, as a `--seal-direct` backfill can leave, sends a one-block
        // reorg's walk below it until finality passes it, and that reads as a finality violation (#2175).
        if let Some(w) = watermark {
            if store.get_block_hash(w)?.is_none() {
                pin_checkpoint(store, source, w).await?;
            }
        }
        return Ok(()); // nothing new has finalized
    }

    loop {
        if ceiling < from {
            return Ok(());
        }
        // Read only as far as the cut: a finalized range of documents is gigabytes (3.17 GB RSS
        // measured on 678 QoS payloads, 2026-09-13), and the cut needs a block and a length per row.
        let mut scan = CutScan::new(ceiling, seal_span);
        store.scan_entities_in_range(from, ceiling, &mut |block, json| {
            scan.push_at(json.len(), block)
        })?;
        // **Advance across a leading stretch that carries no rows, before deciding anything.**
        //
        // Without this the span bound is `first_held_row + seal_span`, not `watermark + seal_span`,
        // so a nest whose watermark sits at 10,800 and whose next event is at block 100,000 holds
        // 89,200 finalized blocks and the bound is the event gap plus the span rather than the span.
        // Found by review on #1200; the row anchor is deliberate (see `seal_cut`) and this is what
        // makes it coincide with the watermark instead of drifting from it.
        //
        // Nothing is sealed here and no segment is written, because there is nothing in the range -
        // it is the empty-range arm below applied to the empty *prefix* rather than only to a wholly
        // empty range. `sealed_through` means "every row at or below this block is in cold storage",
        // which is vacuously true of a range holding no rows. The checkpoint is pinned for the same
        // reason the empty arm pins one: a later reorg walk must not skip past the watermark to an
        // older sparse checkpoint and trip the finality guard on a block it never touched (#461).
        if let Some(first) = scan.first() {
            if first > from {
                if !pin_checkpoint(store, source, first - 1).await? {
                    return Ok(());
                }
                store.set_meta(SEALED_THROUGH_KEY, &(first - 1).to_string())?;
                metrics.set_sealed_through(first - 1);
                tracing::debug!(
                    from,
                    first,
                    "blocks {from}..={} carried no rows; watermark advanced to the first held row",
                    first - 1
                );
                from = first;
            }
        }
        let cut = match scan.cut() {
            None if scan.first().is_none() => {
                // Finalized range with no transfers - just advance the watermark. Pinning a
                // checkpoint at the new watermark is what stops a later reorg from walking past it
                // to an older surviving checkpoint and tripping the finality guard on a block the
                // reorg never touched (#461).
                if !pin_checkpoint(store, source, ceiling).await? {
                    return Ok(());
                }
                store.set_meta(SEALED_THROUGH_KEY, &ceiling.to_string())?;
                metrics.set_sealed_through(ceiling);
                tracing::debug!(
                    "blocks {from}..={ceiling} finalized with no transfers; watermark advanced"
                );
                return Ok(());
            }
            None => {
                // Held, not drained. A cursor-wide "seal because co-tenants are fat"
                // flush was considered and reverted: it made segment identity depend
                // on which nests share the cursor. The 2 GB budget is the multinest
                // RAM job (measured 372 MB for 20 nests × 12,010 rows against 2048).
                // Still pin the finalized ceiling. Holding rows does not mean the reorg
                // walker can do without a checkpoint: #461 was a walk that skipped past
                // sealed_through to an older sparse checkpoint and tripped the finality
                // guard on a block the reorg never touched. The empty-range arm already
                // pins; this one must too (#1067).
                //
                // One pin serves until the watermark moves (#1495). The walk needs a final
                // checkpoint at or above the watermark, not a new one per window.
                if !held_pin_covers(store, from, ceiling)? {
                    if let Ok(Some(hash)) = source.block_hash(ceiling).await {
                        store.set_block_hash(ceiling, &hash)?;
                        store.set_meta(SEAL_PIN_KEY, &format!("{ceiling}:{hash}"))?;
                    }
                }
                tracing::debug!(
                    rows = scan.rows(),
                    threshold = SEAL_DIRECT_BATCH,
                    from,
                    ceiling,
                    "tip path holding finalized rows until the batch threshold"
                );
                return Ok(());
            }
            Some(cut) => cut,
        };

        if !pin_checkpoint(store, source, cut).await? {
            return Ok(());
        }

        let to_seal = store.entities_in_range(from, cut)?;
        let sealed = seal_range(&mut *sinks.open(), &to_seal, from, cut, registry_snapshot)?;
        match sealed {
            Some(summary) => {
                faults.sealed_before_prune();
                // After the seal. It fsyncs the bytes and the manifest entry before returning, and
                // this commit fsyncs the watermark (#1632).
                let pruned =
                    store.prune_and_set_meta(from, cut, SEALED_THROUGH_KEY, &cut.to_string())?;
                metrics.set_sealed_through(cut);
                metrics.add_rows_sealed(summary.rows as u64);
                tracing::debug!(
                    "sealed blocks {from}..={cut}: {} rows across {} table(s); pruned {pruned} from hot",
                    summary.rows,
                    summary.tables
                );
            }
            None => {
                store.set_meta(SEALED_THROUGH_KEY, &cut.to_string())?;
                metrics.set_sealed_through(cut);
                tracing::debug!(
                    "blocks {from}..={cut} finalized with no transfers; watermark advanced"
                );
            }
        }
        from = cut + 1;
        // Sealing is synchronous; yielding between segments lets an aborted ingest stop here.
        crate::time::yield_now().await;
    }
}
