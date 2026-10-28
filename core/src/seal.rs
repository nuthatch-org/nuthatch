//! Sealing a finalized range (RFC-0075 §6): which segments a cut writes, folds and catalogues. Where
//! the bytes go is a [`SegmentSink`]'s business; the node's writes files, a tab's writes OPFS.

use anyhow::{Context, Result};
use nuthatch_seal::cut::{aged, block_timestamp, Segment};
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
