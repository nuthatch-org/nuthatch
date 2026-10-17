//! Which rows go in a segment and whether it is final: the cut rule and the table floors, shared
//! by both of the main crate's seal paths and by a browser tab sealing the same range.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Writer settings this binary sealed with before #1234: SNAPPY, crate-default dictionary and
/// statistics, no blooms, no sort metadata. A segment whose field is missing deserialises as this,
/// not as unknown (#1223 / #1234). Changing the settings is a new name, not a silent rewrite of
/// this one.
pub const ORIGINAL_WRITER_PROFILE: &str = "snappy";

pub fn default_writer_profile() -> String {
    ORIGINAL_WRITER_PROFILE.to_string()
}

/// One sealed Parquet file. `hash` is the content address (sha256 of the file bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub hash: String,
    pub from_block: u64,
    pub to_block: u64,
    pub rows: usize,
    pub file: String,
    /// The discovered-child registry's content hash at seal time (RFC-0009): records exactly which
    /// factory-discovered set produced this segment, so its child rows are reproducible. `None` for a
    /// static (non-factory) nest. Absent in pre-RFC-0009 manifests (serde default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_snapshot: Option<String>,
    /// This table's rows since its last full segment, held under [`SEAL_TABLE_FLOOR`] (#1150). A
    /// provisional segment is a real, content-addressed, queryable Parquet file like any other; the
    /// one thing that distinguishes it is that the table's **next** seal folds it in rather than
    /// sitting beside it, and the file it replaces is removed. At most one per table. Absent in
    /// manifests written before this existed, which reads as `false` - every segment sealed then
    /// was final.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub provisional: bool,
    /// Which `write_parquet` settings produced these bytes. Missing deserialises as
    /// [`ORIGINAL_WRITER_PROFILE`]: every segment sealed before this field existed was that profile.
    #[serde(default = "default_writer_profile")]
    pub writer_profile: String,
    /// Hash of the parquet this seal was given, before a fold rewrote the file (#1631). Those rows
    /// stay hot until the watermark, and the file hash no longer names them. Absent reads as none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_hash: Option<String>,
}

/// Rows a table needs before its segment at a cut is final rather than provisional (#1150).
///
/// The cut is global and data-determined (`take_sealable` in the main crate): every `SEAL_DIRECT_BATCH` rows
/// across all tables, at a block boundary the chain chose. That keeps segment identity independent
/// of the operator's window, and it is kept. What it also did, on a nest with many tables, was write
/// one file **per table per cut**, so a table seeing three events in twenty thousand got a three-row
/// Parquet file every cut - measured on the Perpl backfill at 9.3%: 15,977 files, 4,702 under 8 KB,
/// and a query pays about 0.15 ms per file it opens (docs/bench/segment-layout.md). Sealed segments
/// are never compacted, so that only grew.
///
/// So a table whose pending rows at a cut are under this floor is sealed **provisionally** and folded
/// into its own next cut, until it has this many. The rule is stated in the same terms as the cut -
/// rows, counted from the data - so two operators still produce identical files; and it changes
/// identity only for tables under the floor, since a table that clears it at every cut is sealed
/// exactly as before. At 20,000 rows a cut that is every table with a share of 5% or more.
///
/// One thousand is a segment whose footer is no longer most of it (about 90 bytes a row on the
/// nests measured, so ~90 KB), and it turns a 0.015% table's file count from one per cut into one
/// per 333 cuts. A row rather than byte floor, because bytes depend on the writer version and rows
/// do not.
pub const SEAL_TABLE_FLOOR: usize = 1_000;

/// Row JSON at which a table's segment is final however few rows it holds (RFC-0028 §4, amended
/// 2026-09-13). A table of megabyte documents never reaches [`SEAL_TABLE_FLOOR`] at a cut, and every
/// fold re-reads and rewrites the whole provisional file. Measured on row JSON, not Parquet bytes, for
/// the reason the row floor gives.
pub const SEAL_TABLE_BYTES_FLOOR: usize = 16 * 1024 * 1024;

/// Block time after which a provisional tail is final however few rows it holds (#2084). Under the
/// row floor alone a sparse table's tail waited for months, and the mirror withholds a provisional
/// segment, so a seed or a browser tab read stale answers. Age is chain time, never the clock: the
/// newest `block_timestamp` in the cut against the oldest in the tail, so two operators still seal
/// identical files. A table whose rows carry no timestamp keeps the row floor alone.
pub const SEAL_TABLE_AGE_SECS: u64 = 3 * 60 * 60;

/// A row's `block_timestamp`, the chain time [`SEAL_TABLE_AGE_SECS`] is measured in.
pub fn block_timestamp(row: &Value) -> Option<u64> {
    row.get("block_timestamp").and_then(Value::as_u64)
}

/// The tail spans at least [`SEAL_TABLE_AGE_SECS`] of chain time up to the cut.
pub fn aged(rows: &[Value], cut_time: Option<u64>) -> bool {
    let oldest = rows.iter().filter_map(block_timestamp).min();
    matches!((cut_time, oldest), (Some(c), Some(o)) if c.saturating_sub(o) >= SEAL_TABLE_AGE_SECS)
}

/// Whether a table's rows at a cut seal provisionally: under the row floor, the byte floor and the
/// age rule all at once. `floor` is [`SEAL_TABLE_FLOOR`] outside the main crate's tests.
pub fn provisional(rows: &[Value], floor: usize, cut_time: Option<u64>) -> bool {
    rows.len() < floor
        && rows.iter().map(|v| v.to_string().len()).sum::<usize>() < SEAL_TABLE_BYTES_FLOOR
        && !aged(rows, cut_time)
}

/// Batch size (rows) at which both seal paths cut a segment: `backfill_direct` via
/// [`take_sealable`], the tip path via [`tip_seal_cut`]. Public so integration tests can pad a
/// finalized range to the same threshold without duplicating the number.
pub const SEAL_DIRECT_BATCH: usize = 20_000;

/// Bytes of row JSON at which both seal paths cut a segment, beside the row threshold and the span
/// (RFC-0028 §4, amended 2026-09-13). A constant, not a knob: a boundary an operator could move would
/// give two operators different segments over identical rows. Rows averaging under ~3.3 KB reach the
/// row threshold first, so event nests cut where they always did; rows carrying documents cut here.
pub const SEAL_DIRECT_BYTES: usize = 64 * 1024 * 1024;

/// The shared cut rule for both seal paths, fed one row at a time in chain order so a caller stops
/// reading once the cut is known. The cut is the earliest of:
///
/// - **Row cut**: the block carrying the rows past [`SEAL_DIRECT_BATCH`] (RFC-0028 §4).
/// - **Byte cut**: the block carrying the rows' JSON past [`SEAL_DIRECT_BYTES`].
/// - **Span cut** (#1199): `first + seal_span - 1`, once `frontier` has reached it.
///
/// All three are anchored on the oldest held row, so they are properties of the data and both paths,
/// seeing the same rows, cut at the same blocks. Taking the earliest is what makes them agree: a
/// backfill holding the whole history would otherwise reach a size cut inside a range the tip path
/// had already cut on span. A row past a known span end cannot move the cut, since it could only cross
/// a size threshold at a block the span cut already precedes, so a scan holding each row's block stops
/// there ([`CutScan::push_at`]) rather than reading a sparse range to its ceiling.
pub struct CutScan {
    frontier: u64,
    seal_span: u64,
    first: Option<u64>,
    rows: usize,
    bytes: usize,
    size_cut: Option<u64>,
}

impl CutScan {
    pub fn new(frontier: u64, seal_span: u64) -> Self {
        Self {
            frontier,
            seal_span,
            first: None,
            rows: 0,
            bytes: 0,
            size_cut: None,
        }
    }

    /// The next row; `false` once no later row can move the cut. `block` is read only for the first
    /// row and the row that crosses a threshold.
    pub fn push(&mut self, json_len: usize, block: impl FnOnce() -> u64) -> bool {
        if self.size_cut.is_some() {
            return false;
        }
        let mut block = Some(block);
        let mut read = || (block.take().expect("a row's block is read once"))();
        let mut this = None;
        if self.first.is_none() {
            let b = read();
            this = Some(b);
            self.first = Some(b);
        }
        self.rows += 1;
        self.bytes = self.bytes.saturating_add(json_len);
        if self.rows >= SEAL_DIRECT_BATCH || self.bytes >= SEAL_DIRECT_BYTES {
            self.size_cut = Some(this.unwrap_or_else(read));
            return false;
        }
        true
    }

    /// [`push`](Self::push) for a caller that already holds the row's block, stopping at the first row
    /// past a span end already known.
    pub fn push_at(&mut self, json_len: usize, block: u64) -> bool {
        if let Some(end) = self.first.and_then(|f| self.span_end(f)) {
            if block > end {
                return false;
            }
        }
        self.push(json_len, || block)
    }

    fn span_end(&self, first: u64) -> Option<u64> {
        let end = first.saturating_add(self.seal_span.saturating_sub(1));
        (self.seal_span > 0 && self.frontier >= end).then_some(end)
    }

    /// The block of the first row pushed.
    pub fn first(&self) -> Option<u64> {
        self.first
    }

    /// Rows pushed so far.
    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn cut(&self) -> Option<u64> {
        let span_cut = self.span_end(self.first?);
        match (self.size_cut, span_cut) {
            (Some(s), Some(e)) => Some(s.min(e)),
            (s, e) => s.or(e),
        }
    }
}
