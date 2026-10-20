//! Per-table sealing (RFC-0001 step 4): once a block range is final, each table's rows in that
//! range are written to their own content-addressed Parquet segment, catalogued per table in the
//! manifest. The columnar cold layer is append-only - it never sees a reorg, because reorgs only
//! ever touch the mutable hot store (see store::rollback_to).
//!
//! All tables in a nest ingest from the same block stream and seal together per finalized range, so
//! `sealed_through` stays a single global watermark and the whole range is pruned from hot once every
//! table's segment is durable (the indexer does the prune).
//!
//! ## Scope of the content hash (audit F-D3)
//!
//! A segment's hash is taken over the **Parquet file bytes**, and those bytes include the `created_by`
//! metadata string that `arrow-rs`/`parquet` stamps with its own version. So the guarantee is:
//!
//! - **Same binary → identical bytes → identical hash.** This is the property everything relies on:
//!   re-running a backfill, or two operators running the same release, produce byte-identical segments
//!   that dedupe against each other. Determinism holds.
//! - **Across nuthatch versions built on different arrow-rs releases, segment identity may differ**
//!   even when every decoded row is identical, because `created_by` changed underneath us.
//!
//! That is a limit on *segment identity*, not on correctness: the rows are the same, the queries return
//! the same answers, and re-execution still verifies. What it means practically is that a segment hash
//! is a strong identity within a release and a weak one across releases - so do not use one as a
//! cross-version equality proof (compare decoded rows for that), and expect a version bump that moves
//! arrow-rs to re-seal rather than dedupe. Pinning `created_by` would buy cross-version identity if that
//! ever becomes worth the coupling to a parquet-rs internal; it is deliberately not pinned today.

use anyhow::{Context, Result};
use arrow::array::{Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

pub const SEGMENTS_DIR: &str = "segments";

/// The **shared segment store** for the runtime `dir` belongs to, if it has one (RFC-0033 §11a).
///
/// Derived by convention rather than plumbed: a mounted dataset lives at `<root>/data/<nid>`, so its
/// runtime root is two levels up and the store is `<root>/segments`. A solo `nuthatch dev` nest is
/// not under a `data/` parent, gets `None`, and keeps its segments where they have always been -
/// sharing is a property of a runtime hosting several nests, not of a nest.
///
/// The alternative was threading a root through every seal and query call site, which would have made
/// the layout a parameter of functions that have no business knowing about it.
pub fn shared_store(dir: &Path) -> Option<PathBuf> {
    let parent = dir.parent()?;
    if parent.file_name()? != crate::runtime::DATA_DIR {
        return None;
    }
    Some(parent.parent()?.join(SEGMENTS_DIR))
}

/// Where a segment's bytes actually live: the shared store when there is one, else beside the nest.
///
/// **Falls back to the per-dataset path when the shared copy is absent**, so a dataset migrated
/// before slice C still reads. A missing shared segment is not evidence of corruption; it is evidence
/// of a layout that has not been relocated yet.
pub fn segment_path(dir: &Path, file: &str, hash: &str) -> PathBuf {
    if let Some(store) = shared_store(dir) {
        let shared = store.join(format!("{hash}.parquet"));
        if shared.exists() {
            return shared;
        }
    }
    dir.join(SEGMENTS_DIR).join(file)
}
pub const MANIFEST_FILE: &str = "manifest.json";

/// Catalogue version this binary writes. Absent in a file on disk is version 0 (RFC-0047 C2).
pub const MANIFEST_VERSION: u32 = 1;

use nuthatch_seal::cut::{aged, block_timestamp};
pub use nuthatch_seal::cut::{
    Segment, ORIGINAL_WRITER_PROFILE, SEAL_TABLE_AGE_SECS, SEAL_TABLE_BYTES_FLOOR, SEAL_TABLE_FLOOR,
};
pub use nuthatch_seal::WRITER_PROFILE;
use nuthatch_seal::{rows_to_batch, sort_rows_for_seal, write_parquet};

/// The floor `seal_range` applies for `dir`: the constant, unless a test has set otherwise.
fn table_floor(dir: &Path) -> usize {
    #[cfg(test)]
    if let Some(&n) = test_table_floors().lock().unwrap().get(dir) {
        return n;
    }
    let _ = dir;
    SEAL_TABLE_FLOOR
}

/// Test-only knob (#1150), keyed by `dir` like the sweep knobs below so no test's setting leaks
/// into another's tempdir. A floor of `0` makes every seal final, which is the behaviour before the
/// floor existed - the corruption and quarantine tests seal one row per call and damage one
/// segment of several, and they are about what a bad file does to a table, not about how many
/// files a table gets. They say so where they set it. Tests about the floor use the real one.
#[cfg(test)]
fn test_table_floors() -> &'static Mutex<HashMap<PathBuf, usize>> {
    static FLOORS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    FLOORS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn test_set_table_floor(dir: &Path, rows: usize) {
    test_table_floors()
        .lock()
        .unwrap()
        .insert(dir.to_path_buf(), rows);
}

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

/// Seal every table's rows in a finalized `[from, to]` range. Rows are grouped by their `table`
/// field; each group becomes one content-addressed Parquet segment catalogued under its table.
/// Returns None if the range held no rows.
pub fn seal_range(
    dir: &Path,
    entity_json: &[String],
    from: u64,
    to: u64,
) -> Result<Option<SealSummary>> {
    seal_range_with_snapshot(dir, entity_json, from, to, None)
}

/// Like [`seal_range`], but records `registry_snapshot` (the discovered-child registry's content hash)
/// on each segment's manifest entry - the factory paths (RFC-0009) pass it so a segment records which
/// discovered set produced it. A static nest passes `None` (via `seal_range`).
pub fn seal_range_with_snapshot(
    dir: &Path,
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

    let seg_dir = dir.join(SEGMENTS_DIR);
    std::fs::create_dir_all(&seg_dir)
        .with_context(|| format!("cannot create {}", seg_dir.display()))?;
    let mut manifest = load_manifest(dir)?;
    let mut summary = SealSummary::default();
    // Shared files this seal will name. Dropped only after the manifest that names them is installed.
    let mut publishing: Vec<SegmentHold> = Vec::new();
    // Per-nest provisional files a fold has replaced. Removed only once the manifest that no longer
    // names them is durably installed (below), never before: a crash between the two would leave
    // a manifest pointing at a file that is gone, and the folded rows would read as missing.
    let mut folded_away: Vec<PathBuf> = Vec::new();
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
            if segment_intact(dir, s) {
                continue;
            }
            if s.hash == hash {
                // These are the bytes the address names, so they replace a corrupt file in place,
                // shared or not (#2266). A replace that fails fails the seal, and hot is kept.
                let target = match shared_store(dir) {
                    Some(store) => {
                        std::fs::create_dir_all(&store)
                            .with_context(|| format!("creating {}", store.display()))?;
                        store.join(format!("{hash}.parquet"))
                    }
                    None => seg_dir.join(&s.file),
                };
                replace_durable(&target, &bytes)
                    .with_context(|| format!("restoring segment {}", s.file))?;
                continue;
            }
            // A lost fold held these rows; they are sealed again as a segment of their own.
        }
        let new_rows = rows.len();

        // Fold this table's provisional segment in, if it has one (#1150): its rows come first,
        // being from earlier cuts, and its `from_block` is the segment's. Read back from the
        // Parquet it was written to rather than kept anywhere else, so there is one copy of the
        // truth and the folded file is byte-identical to sealing all of those rows at once
        // (`a_folded_segment_is_byte_identical_to_sealing_all_its_rows_at_once`).
        let pending = segments.iter().position(|s| s.provisional);
        let (rows, from, bytes, hash, replaced) = match pending {
            None => (rows, from, bytes, hash, None),
            Some(i) => {
                let prev = segments.remove(i);
                let mut all = read_segment_rows(&segment_path(dir, &prev.file, &prev.hash))
                    .with_context(|| {
                        format!("reading provisional segment {} to fold it", prev.file)
                    })?;
                all.extend(rows);
                sort_rows_for_seal(&mut all);
                let bytes = write_parquet(&rows_to_batch(&all)?)?;
                let hash = hex::encode(Sha256::digest(&bytes));
                (all, prev.from_block, bytes, hash, Some(prev))
            }
        };
        let provisional = nuthatch_seal::cut::provisional(&rows, table_floor(dir), cut_time);
        let file = format!("{table}-{hash}.parquet");

        // Write once, into the shared store when this dataset belongs to a runtime (RFC-0033 §11a).
        // Content-addressed, so a second nest sealing byte-identical rows finds it already there and
        // the write is a no-op rather than a duplicate.
        match shared_store(dir) {
            Some(store) => {
                let shared = store.join(format!("{hash}.parquet"));
                // Before the exists check. A reclaim that looks between the check and the hold
                // deletes the file this manifest is about to name (#1644).
                let hold = SegmentHold::acquire(&shared);
                publish_durable(&store, &format!("{hash}.parquet"), &bytes)?;
                publishing.push(hold);
                #[cfg(test)]
                before_manifest();
                // The folded file is left for `nuthatch prune`, which reclaims what no manifest
                // references: another dataset in the store may hold the same bytes under the same
                // hash, and this nest cannot know.
            }
            None => {
                publish_durable(&seg_dir, &file, &bytes)?;
                if let Some(prev) = &replaced {
                    // This nest's own copy, and not yet: see `folded_away`.
                    folded_away.push(seg_dir.join(&prev.file));
                }
            }
        }
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
            let rows = read_segment_rows(&segment_path(dir, &s.file, &s.hash))
                .with_context(|| format!("reading provisional segment {} to age it", s.file))?;
            if aged(&rows, cut_time) {
                s.provisional = false;
            }
        }
    }

    manifest.manifest_version = MANIFEST_VERSION;
    crate::crash::point("seal:before-manifest");
    save_manifest(dir, &manifest)?;
    crate::crash::point("seal:after-manifest");
    drop(publishing);
    // The new manifest no longer names these, but a reader holding a lease from before it was
    // installed may still be about to open one; see `retire`.
    retire(dir, folded_away);
    Ok(Some(summary))
}

/// Mark each table's provisional segment final, for a table the operator knows is done (#1851). The
/// file is not touched; the table's next seal starts a new segment instead of folding this one. The
/// caller must hold the nest's writer: this is a read-modify-write of the catalogue.
pub fn finalise_provisional(dir: &Path) -> Result<Vec<(String, Segment)>> {
    let mut manifest = load_manifest(dir)?;
    let mut finalised = Vec::new();
    for (table, segments) in &mut manifest.tables {
        for s in segments.iter_mut().filter(|s| s.provisional) {
            s.provisional = false;
            finalised.push((table.clone(), s.clone()));
        }
    }
    if !finalised.is_empty() {
        save_manifest(dir, &manifest)?;
    }
    Ok(finalised)
}

/// Readers of each dataset's segments, and the files a fold replaced that must outlive them.
///
/// A `/sql` read plans against the manifest and opens the files it names later, when Burrmill executes.
/// A fold that deleted the replaced file as soon as its new manifest was installed could land in
/// between, and the read then found a planned segment missing and answered short. A reader holds a
/// [`ReadLease`] from before it reads the manifest until its rows are collected; a replaced file is
/// deleted only once no lease that could have planned against the old manifest is still held.
#[derive(Default)]
struct Leases {
    /// Bumped by every retirement. A lease records the epoch it was taken in.
    epoch: u64,
    /// Open leases per epoch.
    readers: BTreeMap<u64, usize>,
    /// Replaced files with the epoch they were retired in.
    retired: Vec<(u64, PathBuf)>,
}

/// One shared segment a seal will catalogue, held until that manifest is on disk (#1644).
struct SegmentHold {
    path: PathBuf,
}

fn publishing() -> &'static Mutex<HashMap<PathBuf, usize>> {
    static HELD: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    HELD.get_or_init(|| Mutex::new(HashMap::new()))
}

fn segment_key(path: &Path) -> PathBuf {
    // The parent, not the file. A hold taken before the write has to match the path a reclaim
    // canonicalises once the file exists (on macOS that spelling gains a `/private` prefix).
    let Some(name) = path.file_name() else {
        return path.to_path_buf();
    };
    match path.parent() {
        Some(parent) => {
            let parent = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
            parent.join(name)
        }
        None => path.to_path_buf(),
    }
}

impl SegmentHold {
    fn acquire(path: &Path) -> Self {
        let path = segment_key(path);
        let mut held = publishing().lock().unwrap();
        *held.entry(path.clone()).or_default() += 1;
        SegmentHold { path }
    }
}

impl Drop for SegmentHold {
    fn drop(&mut self) {
        let mut held = publishing().lock().unwrap();
        if let Some(n) = held.get_mut(&self.path) {
            *n -= 1;
            if *n == 0 {
                held.remove(&self.path);
            }
        }
    }
}

/// Whether a live seal is about to name `path` and has not installed that manifest yet.
pub fn segment_held(path: &Path) -> bool {
    publishing()
        .lock()
        .unwrap()
        .contains_key(&segment_key(path))
}

/// Delete `path` unless a live seal holds it. The check and the unlink share the publishing lock.
pub fn remove_segment_if_unheld(path: &Path) -> Result<bool> {
    let held = publishing().lock().unwrap();
    if held.contains_key(&segment_key(path)) {
        return Ok(false);
    }
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e).context(format!("removing {}", path.display())),
    }
}

fn leases() -> &'static Mutex<HashMap<PathBuf, Leases>> {
    static LEASES: OnceLock<Mutex<HashMap<PathBuf, Leases>>> = OnceLock::new();
    LEASES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// One key per dataset however a caller spelled its path.
fn lease_key(dir: &Path) -> PathBuf {
    std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf())
}

/// Holds `dir`'s segment set for the lifetime of the value. Take it before reading the manifest.
pub struct ReadLease {
    key: PathBuf,
    epoch: u64,
}

pub fn read_lease(dir: &Path) -> ReadLease {
    let key = lease_key(dir);
    let mut all = leases().lock().unwrap();
    let state = all.entry(key.clone()).or_default();
    *state.readers.entry(state.epoch).or_insert(0) += 1;
    ReadLease {
        key,
        epoch: state.epoch,
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        let doomed = {
            let mut all = leases().lock().unwrap();
            let Some(state) = all.get_mut(&self.key) else {
                return;
            };
            if let Some(n) = state.readers.get_mut(&self.epoch) {
                *n -= 1;
                if *n == 0 {
                    state.readers.remove(&self.epoch);
                }
            }
            releasable(state)
        };
        remove_retired(doomed);
    }
}

/// Hand replaced files to the lease registry. Called after the manifest that stops naming them is
/// installed, so a lease taken after this call reads that manifest and never plans against them.
/// Maintained-view copies use it the same way, with their own record standing in for the manifest.
pub(crate) fn retire(dir: &Path, files: Vec<PathBuf>) {
    if files.is_empty() {
        return;
    }
    let doomed = {
        let mut all = leases().lock().unwrap();
        let state = all.entry(lease_key(dir)).or_default();
        let epoch = state.epoch;
        state.epoch += 1;
        state.retired.extend(files.into_iter().map(|f| (epoch, f)));
        releasable(state)
    };
    remove_retired(doomed);
}

/// Retired files no open lease can still be reading: those retired before the oldest open lease was
/// taken, or all of them when none is open.
fn releasable(state: &mut Leases) -> Vec<PathBuf> {
    let oldest = state.readers.keys().next().copied();
    let (done, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut state.retired)
        .into_iter()
        .partition(|(retired_in, _)| oldest.is_none_or(|o| o > *retired_in));
    state.retired = kept;
    done.into_iter().map(|(_, f)| f).collect()
}

/// A failure here is a stray file, which is disk and not data: logged, never returned.
fn remove_retired(files: Vec<PathBuf>) {
    for old in files {
        if let Err(e) = std::fs::remove_file(&old) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!("retired file {} not removed: {e}", old.display());
            }
        }
    }
}

/// A segment's rows back as the JSON objects it was sealed from, for a fold (#1150). Nothing else
/// needs this; `read_table_rows` is the typed reader.
fn read_segment_rows(path: &Path) -> Result<Vec<Value>> {
    let bytes =
        std::fs::read(path).with_context(|| format!("opening segment {}", path.display()))?;
    nuthatch_seal::segment_rows(bytes.into())
        .with_context(|| format!("reading segment {}", path.display()))
}

/// Encode JSON-object rows with the same deterministic Parquet writer used for sealed chain data.
/// The offchain namespace owns a distinct catalogue and never enters the chain seal/replay path;
/// sharing the byte writer is deliberately the only shared part.
pub fn write_snapshot_parquet(rows: &[Value]) -> Result<Vec<u8>> {
    write_parquet(&rows_to_batch(rows)?)
}

/// Read a table's sealed rows back as [`DecodedRow`]s, in block order (RFC-0041 §5.3, nuthatch#865).
///
/// **The one conversion, not a second one.** Every cell goes through
/// [`DecodedRow::from_stored`], which is what the reorg path uses on the hot store's JSON. A reader
/// that parsed Parquet into typed values on its own would be a second opinion about what a stored
/// row means, and the two would agree until the day they did not - at which point a retraction stops
/// cancelling its insertion and the entity keeps a fact forever. See nuthatch#864.
///
/// `rows_to_batch` writes `block_number`, `log_index`, `_seq` and `block_timestamp` as `UInt64` and
/// everything else as `Utf8`, so those are the only two column types this has to understand. A
/// segment carrying anything else was not written by this project.
///
/// **This loads the table into memory.** That is what a warm-restart seed is - §5.3 pays the
/// historical fold once per restart rather than once per request - but it is not a streaming reader
/// and should not be mistaken for one.
pub fn read_table_rows(
    dir: &Path,
    schema: &crate::registry::TableSchema,
) -> Result<Vec<crate::registry::DecodedRow>> {
    let mut out = Vec::new();
    read_table_rows_by_segment(dir, schema, u64::MAX, &mut |mut rows| {
        out.append(&mut rows);
        Ok(())
    })?;
    Ok(out)
}

/// The same read, handed to `sink` **one sealed segment at a time** instead of accumulated.
///
/// The whole-history `Vec` is a large transient of a restart seed, and it is one nothing else
/// bounds: mount admission prices an entity's *maintained* relation, not the historical facts it
/// folds to build one. Measured against a real Horizon nest (2026-08-26, `tests/seed_scale.rs` at `ede03ca7`,
/// 346,288 sealed rows across 2,985 segments):
///
/// | seed             | peak RSS | wall |
/// |------------------|----------|------|
/// | one window       | 993 MB   | 1.2s |
/// | per segment      | 694 MB   | 2.4s |
/// | one window, join | 790 MB   | 1.9s |
/// | per segment, join| 368 MB   | 2.8s |
///
/// Half the per-cursor budget for one entity over one table, against a third of it, at twice the
/// wall clock on a path that runs once per restart. Worth it.
///
/// Chunking is sound because a seed is `+1`-only: no retraction is fed, so the maintained relation
/// grows monotonically and no chunk can trip `max_rows` earlier than the whole-history window
/// would have. A seed that fits still fits.
///
/// Only segments ending at or below `sealed_through` are read, as `analytics::query_cold` bounds the
/// views: a crash between the manifest and the prune leaves a newer segment whose rows hot still holds.
pub fn read_table_rows_by_segment(
    dir: &Path,
    schema: &crate::registry::TableSchema,
    sealed_through: u64,
    sink: &mut dyn FnMut(Vec<crate::registry::DecodedRow>) -> Result<()>,
) -> Result<()> {
    let manifest = load_manifest(dir)?;
    let Some(segments) = manifest.tables.get(&schema.table) else {
        return Ok(());
    };
    // Block order, and by content address within a block range so a re-seal cannot reorder rows.
    let mut ordered: Vec<&Segment> = segments
        .iter()
        .filter(|s| s.to_block <= sealed_through)
        .collect();
    ordered.sort_by(|a, b| {
        (a.from_block, a.to_block, &a.hash).cmp(&(b.from_block, b.to_block, &b.hash))
    });

    for segment in ordered {
        sink(read_segment_decoded(dir, segment, schema)?)?;
    }
    Ok(())
}

/// One sealed segment's rows, through the same conversion as [`read_table_rows`].
pub fn read_segment_decoded(
    dir: &Path,
    segment: &Segment,
    schema: &crate::registry::TableSchema,
) -> Result<Vec<crate::registry::DecodedRow>> {
    let mut out = Vec::new();
    visit_segment_rows(dir, segment, schema, None, &mut |row| {
        out.push(row);
        Ok(())
    })?;
    Ok(out)
}

/// Hand `visit` one segment's rows, one at a time, decoding only those whose block `want` accepts.
/// `want` is asked per row, so a caller may narrow it as it goes, and a sample inside a large
/// segment holds only what it keeps. Row groups are not pruned: a segment is one row group below
/// the writer's 1,048,576-row default, which no seal cut reaches.
pub fn visit_segment_rows(
    dir: &Path,
    segment: &Segment,
    schema: &crate::registry::TableSchema,
    want: Option<&dyn Fn(u64) -> bool>,
    visit: &mut dyn FnMut(crate::registry::DecodedRow) -> Result<()>,
) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let path = segment_path(dir, &segment.file, &segment.hash);
    let file = std::fs::File::open(&path)
        .with_context(|| format!("opening sealed segment {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading sealed segment {}", path.display()))?
        .build()
        .with_context(|| format!("reading sealed segment {}", path.display()))?;
    for batch in reader {
        let batch = batch.with_context(|| format!("decoding sealed segment {}", path.display()))?;
        let block_numbers = match want {
            Some(_) => Some(
                batch
                    .column_by_name("block_number")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                    .with_context(|| {
                        format!("sealed segment {} has no block_number", path.display())
                    })?,
            ),
            None => None,
        };
        for row in 0..batch.num_rows() {
            if let (Some(want), Some(numbers)) = (want, block_numbers) {
                if !want(numbers.value(row)) {
                    continue;
                }
            }
            let mut stored = serde_json::Map::new();
            for (i, field) in batch.schema().fields().iter().enumerate() {
                let column = batch.column(i);
                let value = match field.data_type() {
                    DataType::UInt64 => {
                        let a = column
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .context("a UInt64 column that is not one")?;
                        Value::from(a.value(row))
                    }
                    DataType::Utf8 => {
                        let a = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .context("a Utf8 column that is not one")?;
                        if a.is_null(row) {
                            Value::Null
                        } else {
                            Value::from(a.value(row))
                        }
                    }
                    other => anyhow::bail!(
                        "sealed segment {} column {} has type {other}, which this project does \
                         not write",
                        path.display(),
                        field.name()
                    ),
                };
                stored.insert(field.name().clone(), value);
            }
            #[cfg(test)]
            note_materialised(dir);
            visit(
                crate::registry::DecodedRow::from_stored(&Value::Object(stored), schema)
                    .with_context(|| format!("row {row} of sealed segment {}", path.display()))?,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
fn test_materialised() -> &'static Mutex<HashMap<PathBuf, usize>> {
    static ROWS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    ROWS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn note_materialised(dir: &Path) {
    *test_materialised()
        .lock()
        .unwrap()
        .entry(dir.to_path_buf())
        .or_default() += 1;
}

/// Rows the segment readers have decoded under `dir` so far, for tests that bound retention.
#[cfg(test)]
pub(crate) fn test_rows_materialised(dir: &Path) -> usize {
    test_materialised()
        .lock()
        .unwrap()
        .get(dir)
        .copied()
        .unwrap_or(0)
}

/// A catalogue is read from the nest's own tree, which a cloned repository supplies, and its names
/// become paths: each table, hash and file must be one segment under the runtime's rule (#2184).
fn checked(m: Manifest) -> Result<Manifest> {
    for (table, segs) in &m.tables {
        crate::runtime::safe_segment(table, "catalogued table")?;
        for s in segs {
            crate::runtime::safe_segment(&s.hash, "catalogued segment hash")?;
            let stem = s.file.strip_suffix(".parquet").unwrap_or("");
            crate::runtime::safe_segment(stem, "catalogued segment file")
                .with_context(|| format!("{table}: segment file {:?}", s.file))?;
        }
    }
    Ok(m)
}

/// Load the segment catalogue (empty if none yet).
/// The catalogue, with the sha256 of the exact bytes it was parsed from (RFC-0048 §3). `None` when
/// nothing has sealed.
pub fn load_manifest_with_hash(dir: &Path) -> Result<(Manifest, Option<String>)> {
    match std::fs::read(manifest_path(dir)) {
        Ok(raw) => Ok((
            checked(serde_json::from_slice(&raw).context("corrupt segments manifest")?)?,
            Some(hex::encode(Sha256::digest(&raw))),
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok((Manifest::default(), None)),
        Err(e) => Err(e).context("failed to read manifest"),
    }
}

/// [`load_manifest_with_hash`]'s hash alone.
pub fn catalogue_hash(dir: &Path) -> Result<Option<String>> {
    match std::fs::read(manifest_path(dir)) {
        Ok(raw) => Ok(Some(hex::encode(Sha256::digest(&raw)))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("failed to read manifest"),
    }
}

pub fn load_manifest(dir: &Path) -> Result<Manifest> {
    let path = manifest_path(dir);
    match std::fs::read_to_string(&path) {
        Ok(raw) => checked(serde_json::from_str(&raw).context("corrupt segments manifest")?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::default()),
        Err(e) => Err(e).context("failed to read manifest"),
    }
}

/// The **serving-path** counterpart to [`verify_and_quarantine`]: which of `dir`'s manifest segments
/// no longer hash to their content address. Returns their hashes, for `analytics::define_views` to
/// drop from a rebuilt view (issue #433).
///
/// ## Why the hash and not an engine probe
///
/// #430 drops a segment that will not **bind**. That catches footer corruption and nothing else.
/// Measured in the DuckDB CLI (1.5.3), the engine then, against a segment
/// whose data region is overwritten but whose footer is intact:
///
/// - `SELECT 1 FROM read_parquet([f]) LIMIT 0` - the #430 probe - **succeeds**;
/// - `count(*)` and `max(col)` **succeed**, answered from Parquet metadata without reading a page;
/// - `SELECT * … LIMIT 1` fails on that file, and **succeeds** on one where only the late row groups
///   are corrupt, because it never reads them.
///
/// So the only sound engine-side discriminator is a full scan of every column of every segment, and
/// it would be pinned to whatever the query planner prunes this release, under any engine. The
/// content address is not: sealed segments are immutable, so any changed byte is unambiguous corruption, and this is already
/// the check `verify_and_quarantine` makes at startup. Using the same one here is what stops the
/// serving path and the startup path disagreeing about the same file, which was half of #419.
///
/// **Reduce here, never quarantine.** Quarantine moves bytes, and the shared store's bytes belong to
/// every dataset referencing them (RFC-0033 §11a). Dropping a segment from one query's view changes
/// nothing on disk and so is safe for a shared segment, which is why this returns hashes rather than
/// calling the startup pass.
///
/// ## Cost, and the cache that is deliberately not here
///
/// This reads and hashes segments, every time it is called, with no cache. Two things bound what it
/// costs, and the first one alone was **not enough**:
///
/// - The *caller*: `run` only asks after a query has bound and then died reading rows. That rules
///   out the commonest error on this surface (a typo, a missing column), and nothing more. It was
///   claimed here that "the cheap way to provoke a sweep does not exist" - it did, and it was
///   cheaper than the one this file named. `SELECT CAST('x' AS INTEGER)` is 27 bytes, references no
///   table, sails past every gate on the way in, binds, and dies executing; measured, it hashed all
///   3 segments of a healthy nest, on an unauthenticated surface whose concurrency permits are 2.
/// - So, second and load-bearing: `tables`. Only the segments backing the tables the failed query
///   actually **named** are read. A segment the query never touched cannot be what killed it, so
///   sweeping it was never justified on correctness either. The table-free class above now hashes
///   nothing at all, and `SELECT CAST(c AS INT) FROM t` pays for `t` and not for the other forty
///   tables in the nest.
///
/// The bound that remains: a caller who names the nest's largest table can still make one request
/// hash that table's segments. That is inherent to verifying-on-failure at all - the levers are
/// coalescing concurrent sweeps (below), a deadline shared with the query's own watchdog (`deadline`,
/// #476), and the gateway's rate limiting (#365). Not a cache keyed on anything that can go stale.
///
/// **There was a memo here keyed on the file's `(mtime, len)`, and it was wrong.** The idea was that a
/// segment already verified in this process is a `stat` rather than a read. Measured on this box
/// (btrfs, Linux 6.12): rewriting a file in place with the same length inside one timer tick leaves
/// `st_mtime_ns` **byte-identical**, because the kernel stamps writes from a per-tick cached clock. So
/// the key cannot see an in-place same-length overwrite - which is precisely the corruption this
/// function exists to catch. A cache that reports `intact` about a corrupt segment, inside the fix for
/// reporting healthy about corrupt data, is the bug wearing the fix's clothes. It was removed rather
/// than tuned, and `seal::tests::a_corrupt_segment_is_caught_even_when_its_mtime_is_unchanged` is
/// what stops it coming back. That test exists because my first claim here - that the page-corruption
/// test already covered it - was **false**: mutating the memo back in left the whole suite green.
///
/// ## Coalescing (#476)
///
/// Concurrent callers naming the *same* tables (the shape of the amplifier: the same expensive query,
/// repeated while the two `SQL_MAX_CONCURRENCY` permits are held) share one sweep rather than each
/// paying for their own. This is not a cache: the in-flight entry lives only for the duration of the
/// sweep it names and is removed the moment that sweep finishes, so there is nothing here that can go
/// stale the way the `(mtime, len)` memo did - the next call, even for the identical key, always reads
/// bytes fresh.
///
/// `tables` holds the table names the failed query referenced, lowercased (identifiers are
/// case-insensitive). An empty set hashes nothing. `deadline`, when set, bounds the sweep by the same
/// wall-clock budget as the query that triggered it (`analytics::run`'s `QueryGuard`), rather than
/// running unbounded between the query's two attempts - a caller that has already spent its budget
/// gets nothing further hashed on its behalf.
pub fn segments_failing_verification(
    dir: &Path,
    tables: &BTreeSet<String>,
    deadline: Option<Instant>,
) -> BTreeSet<String> {
    if tables.is_empty() {
        return BTreeSet::new();
    }
    let key: SweepKey = (dir.to_path_buf(), tables.clone());
    let (slot, is_leader) = {
        let mut inflight = sweeps_in_flight().lock().unwrap();
        match inflight.get(&key) {
            Some(existing) => (Arc::clone(existing), false),
            None => {
                let slot = Arc::new(SweepSlot::default());
                inflight.insert(key.clone(), Arc::clone(&slot));
                (slot, true)
            }
        }
    };

    // Test-only rendezvous (#529): when a test has armed a registration barrier for this `dir`, every
    // caller - leader and followers alike - blocks here until all of them have made their
    // leader-or-follower decision above. See `test_set_sweep_registration_barrier` for why.
    #[cfg(test)]
    {
        let barrier = test_sweep_registration_barriers()
            .lock()
            .unwrap()
            .get(dir)
            .cloned();
        if let Some(b) = barrier {
            b.wait();
        }
    }

    if is_leader {
        // Removes this sweep's map entry and wakes any follower on every exit path, including a panic
        // unwind out of `sweep_segments` - `fs::read`/`Sha256::digest` don't panic on the errors this
        // code already handles, but leaving a slot that nothing will ever complete stuck in the map
        // forever would hang a *trusted, unguarded* follower (query()/query_cold(), which have no
        // deadline and must run to completion by design) indefinitely rather than just failing loudly.
        struct Cleanup<'a> {
            key: &'a SweepKey,
            slot: &'a SweepSlot,
        }
        impl Drop for Cleanup<'_> {
            fn drop(&mut self) {
                // Under `slot.result`, the lock a follower holds from its map check to its `wait`, so
                // this notify cannot fall between the two and be lost (#1810). Same order: result, map.
                let _result = self.slot.result.lock().unwrap_or_else(|p| p.into_inner());
                #[cfg(test)]
                test_panic_hook_cleanup_locked(&self.key.0);
                sweeps_in_flight()
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(self.key);
                self.slot.done.notify_all();
                #[cfg(test)]
                test_panic_hook_cleanup_done(&self.key.0);
            }
        }
        let _cleanup = Cleanup {
            key: &key,
            slot: &slot,
        };
        let result = sweep_segments(dir, tables, deadline);
        *slot.result.lock().unwrap() = Some(result.clone());
        return result;
        // `_cleanup` drops here - map entry removed and followers notified only now that the result is
        // published, so a follower checking the map first still finds the slot with a real answer.
    }

    // Follower: wait for the leader, but never past this call's own deadline - the query's time budget
    // is a promise, and that must hold even when it is someone else's sweep in progress, not its own.
    #[cfg(test)]
    test_panic_hook_follower_registered(dir);
    let mut guard = slot.result.lock().unwrap();
    loop {
        if let Some(result) = guard.as_ref() {
            return result.clone();
        }
        // The leader is gone (removed itself, on success or on panic) without ever publishing - stop
        // following a slot nobody will ever complete and retry from scratch, becoming the leader (or
        // following a new one) rather than waiting out a deadline, or forever, for nothing.
        if !sweeps_in_flight().lock().unwrap().contains_key(&key) {
            drop(guard);
            return segments_failing_verification(dir, tables, deadline);
        }
        #[cfg(test)]
        test_panic_hook_follower_checked(dir);
        guard = match deadline {
            None => slot.done.wait(guard).unwrap(),
            Some(d) => {
                let remaining = d.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return BTreeSet::new();
                }
                slot.done.wait_timeout(guard, remaining).unwrap().0
            }
        };
    }
}

/// A sweep in progress, keyed by exactly what it was asked to verify - see the "Coalescing" section
/// above.
type SweepKey = (PathBuf, BTreeSet<String>);

#[derive(Default)]
struct SweepSlot {
    result: Mutex<Option<BTreeSet<String>>>,
    done: Condvar,
}

fn sweeps_in_flight() -> &'static Mutex<HashMap<SweepKey, Arc<SweepSlot>>> {
    static INFLIGHT: OnceLock<Mutex<HashMap<SweepKey, Arc<SweepSlot>>>> = OnceLock::new();
    INFLIGHT.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Test-only knob (#529): how many segment-budget checks the sweep loop gets to make before
/// `sweep_out_of_budget` reports the deadline spent, **regardless of the wall clock** - absent (the
/// default) leaves the real `Instant`-based check in `sweep_out_of_budget` untouched. Replaces a
/// per-segment `thread::sleep` raced against a real deadline: that raced two real clocks (how long
/// the injected delay actually took to wake up vs. how long the deadline had left), and at load
/// average 34 a descheduled thread can wake up arbitrarily later than its requested sleep, closing a
/// margin that was only ever tens of milliseconds. Counting checks instead of milliseconds makes the
/// early exit exact and load-independent: the loop stops after exactly the Nth check, on every run.
///
/// Keyed by `dir`, not a bare global - a first cut at this used a single process-global `AtomicI64`
/// and read it unconditionally in `sweep_out_of_budget`, so for the whole window one test held it
/// armed, *every* sweep in the test binary was truncated at the same check count, including sweeps
/// belonging to tests that never touched the knob and held no lock over it. That is the exact class
/// of bug this issue exists to remove, just relocated from a real clock to a shared atomic - keying
/// by `dir` (same pattern as `test_sweep_starts` and the registration barrier above) removes it
/// structurally: a sweep only ever consults the override for its own tempdir.
#[cfg(test)]
fn test_sweep_expiry_after_checks() -> &'static Mutex<HashMap<PathBuf, i64>> {
    static EXPIRY: OnceLock<Mutex<HashMap<PathBuf, i64>>> = OnceLock::new();
    EXPIRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn test_set_sweep_expire_after_checks(dir: &Path, n: i64) {
    if n < 0 {
        test_sweep_expiry_after_checks().lock().unwrap().remove(dir);
    } else {
        test_sweep_expiry_after_checks()
            .lock()
            .unwrap()
            .insert(dir.to_path_buf(), n);
    }
}

/// Test-only knob (#529): a rendezvous point inside `segments_failing_verification`, keyed by `dir`
/// so an unrelated test's concurrent sweep never joins it by accident (same reasoning as
/// `test_sweep_starts` below). `concurrent_identical_sweeps_coalesce_into_one` used to keep the
/// leader busy with a fixed 150ms per-segment delay, long enough - it hoped - for the three
/// followers to be scheduled and find the leader's map entry before it published and removed
/// itself. At load average 34 a follower can sit unscheduled for well over 150ms, arrive to find the
/// entry already gone, and start its own uncoalesced sweep. A barrier removes the race instead of
/// widening the margin: every caller (leader and followers) blocks here until all of them have made
/// their leader-or-follower decision, so the leader cannot possibly finish and clean up before the
/// last follower has registered, no matter how the four threads are scheduled.
#[cfg(test)]
fn test_sweep_registration_barriers() -> &'static Mutex<HashMap<PathBuf, Arc<std::sync::Barrier>>> {
    static BARRIERS: OnceLock<Mutex<HashMap<PathBuf, Arc<std::sync::Barrier>>>> = OnceLock::new();
    BARRIERS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn test_set_sweep_registration_barrier(dir: &Path, n: usize) {
    test_sweep_registration_barriers()
        .lock()
        .unwrap()
        .insert(dir.to_path_buf(), Arc::new(std::sync::Barrier::new(n)));
}

#[cfg(test)]
pub(crate) fn test_clear_sweep_registration_barrier(dir: &Path) {
    test_sweep_registration_barriers()
        .lock()
        .unwrap()
        .remove(dir);
}

/// How many times a real (leader) sweep has run, keyed by `dir` - a coalescing test's only window
/// into whether N concurrent identical callers paid for N sweeps or one. Keyed rather than a bare
/// counter because `cargo test` runs the whole suite concurrently: an unrelated test's own (fast,
/// undelayed) sweep against its own tempdir would otherwise land inside this test's delay window and
/// inflate the count for a dir it never touched. Each test's tempdir is unique, so counting per-dir
/// isolates it from that contamination without requiring any process-wide serialisation.
#[cfg(test)]
fn test_sweep_starts() -> &'static Mutex<HashMap<PathBuf, usize>> {
    static STARTS: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    STARTS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn test_sweep_start_count(dir: &Path) -> usize {
    test_sweep_starts()
        .lock()
        .unwrap()
        .get(dir)
        .copied()
        .unwrap_or(0)
}

/// How many segments a sweep actually read and hashed, keyed by `dir` for the same reason as
/// `test_sweep_starts`. What `sweep_stops_at_its_deadline_instead_of_running_to_completion` pins the
/// early exit on: proof the loop stopped *before* the corrupt segment rather than merely that the
/// corrupt segment happened not to be flagged.
#[cfg(test)]
fn test_sweep_segments_processed() -> &'static Mutex<HashMap<PathBuf, usize>> {
    static PROCESSED: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    PROCESSED.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn test_sweep_segments_processed_count(dir: &Path) -> usize {
    test_sweep_segments_processed()
        .lock()
        .unwrap()
        .get(dir)
        .copied()
        .unwrap_or(0)
}

/// Test-only interleavings (#1810), keyed by `dir`: the leader's sweep panics, and its cleanup races a
/// follower. One-shot: the follower's retry sweeps normally.
///
/// `FollowerHoldsLock`: the leader panics only once a follower sits between its map check and its
/// `wait`. Stages: 0 armed, 1 leader in its sweep, 2 follower checked the map, 3 cleanup done.
///
/// `CleanupHoldsLock`: the cleanup pauses where it holds `slot.result`, and a follower registers
/// meanwhile. Stages: 0 armed, 1 leader in its sweep, 2 cleanup paused, 3 follower registered,
/// 4 follower checked the map, 5 cleanup done.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TestPanicMode {
    FollowerHoldsLock,
    CleanupHoldsLock,
}

#[cfg(test)]
pub(crate) struct TestPanicHook {
    mode: TestPanicMode,
    stage: Mutex<u8>,
    cv: Condvar,
}

#[cfg(test)]
fn test_panic_hooks() -> &'static Mutex<HashMap<PathBuf, Arc<TestPanicHook>>> {
    static HOOKS: OnceLock<Mutex<HashMap<PathBuf, Arc<TestPanicHook>>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn test_arm_sweep_panic_hook(dir: &Path, mode: TestPanicMode) -> Arc<TestPanicHook> {
    let hook = Arc::new(TestPanicHook {
        mode,
        stage: Mutex::new(0),
        cv: Condvar::new(),
    });
    test_panic_hooks()
        .lock()
        .unwrap()
        .insert(dir.to_path_buf(), Arc::clone(&hook));
    hook
}

#[cfg(test)]
pub(crate) fn test_clear_sweep_panic_hook(dir: &Path) {
    test_panic_hooks().lock().unwrap().remove(dir);
}

#[cfg(test)]
fn test_panic_hook(dir: &Path) -> Option<Arc<TestPanicHook>> {
    test_panic_hooks().lock().unwrap().get(dir).cloned()
}

#[cfg(test)]
impl TestPanicHook {
    /// Waits until the stage reaches `at_least`, or `timeout` passes; returns the stage reached.
    pub(crate) fn wait_for(&self, at_least: u8, timeout: Duration) -> u8 {
        let guard = self.stage.lock().unwrap();
        let (guard, _) = self
            .cv
            .wait_timeout_while(guard, timeout, |s| *s < at_least)
            .unwrap();
        *guard
    }

    fn advance(&self, from: u8, to: u8) -> bool {
        let mut stage = self.stage.lock().unwrap();
        if *stage != from {
            return false;
        }
        *stage = to;
        self.cv.notify_all();
        true
    }
}

#[cfg(test)]
fn test_panic_hook_leader(dir: &Path) {
    if let Some(hook) = test_panic_hook(dir) {
        if hook.advance(0, 1) {
            if hook.mode == TestPanicMode::FollowerHoldsLock {
                hook.wait_for(2, Duration::from_secs(60));
            }
            panic!("test-injected sweep panic (#1810)");
        }
    }
}

/// Where the cleanup holds `slot.result`. A correct cleanup keeps it until after its notify, so the
/// follower cannot reach its map check and the short wait times out.
#[cfg(test)]
fn test_panic_hook_cleanup_locked(dir: &Path) {
    if let Some(hook) = test_panic_hook(dir) {
        if hook.mode == TestPanicMode::CleanupHoldsLock && hook.advance(1, 2) {
            hook.wait_for(3, Duration::from_secs(60));
            hook.wait_for(4, Duration::from_millis(200));
        }
    }
}

#[cfg(test)]
fn test_panic_hook_cleanup_done(dir: &Path) {
    if let Some(hook) = test_panic_hook(dir) {
        match hook.mode {
            TestPanicMode::FollowerHoldsLock => {
                hook.advance(2, 3);
            }
            TestPanicMode::CleanupHoldsLock => {
                if !hook.advance(3, 5) {
                    hook.advance(4, 5);
                }
            }
        }
    }
}

#[cfg(test)]
fn test_panic_hook_follower_registered(dir: &Path) {
    if let Some(hook) = test_panic_hook(dir) {
        if hook.mode == TestPanicMode::CleanupHoldsLock {
            hook.advance(2, 3);
        }
    }
}

/// With the fix the cleanup cannot finish while this follower holds `slot.result`, so the wait times
/// out and the follower goes on to `wait`; without it the cleanup finishes here, and the follower then
/// waits for a notification that has already been sent.
#[cfg(test)]
fn test_panic_hook_follower_checked(dir: &Path) {
    if let Some(hook) = test_panic_hook(dir) {
        let (from, to, done) = match hook.mode {
            TestPanicMode::FollowerHoldsLock => (1, 2, 3),
            TestPanicMode::CleanupHoldsLock => (3, 4, 5),
        };
        if hook.advance(from, to) {
            hook.wait_for(done, Duration::from_millis(200));
        }
    }
}

/// `deadline`'s check, factored out so a test can override it with an exact, `dir`-keyed check-count
/// cutoff (`test_set_sweep_expire_after_checks`) instead of racing a real `Instant` against a real
/// `thread::sleep`. Production behaviour (the path reached whenever no test has armed an override for
/// this `dir`) is unchanged: `Instant::now() >= d`.
fn sweep_out_of_budget(
    #[allow(unused_variables)] idx: usize,
    deadline: Option<Instant>,
    #[allow(unused_variables)] dir: &Path,
) -> bool {
    #[cfg(test)]
    {
        if let Some(&n) = test_sweep_expiry_after_checks().lock().unwrap().get(dir) {
            return idx as i64 >= n;
        }
    }
    deadline.is_some_and(|d| Instant::now() >= d)
}

/// The actual read-and-hash pass, run by whichever caller is the sweep's leader. See
/// [`segments_failing_verification`] for the coalescing and deadline behaviour around this.
fn sweep_segments(
    dir: &Path,
    tables: &BTreeSet<String>,
    deadline: Option<Instant>,
) -> BTreeSet<String> {
    #[cfg(test)]
    {
        *test_sweep_starts()
            .lock()
            .unwrap()
            .entry(dir.to_path_buf())
            .or_insert(0) += 1;
        test_panic_hook_leader(dir);
    }

    let mut bad = BTreeSet::new();
    // Enumerate first, hash second: the hashing loop can only ever touch what `segments_to_verify`
    // handed it, so the reachability bound holds by construction rather than by a filter someone
    // could later move below the `fs::read`. `seal::tests::the_sweep_enumerates_only_the_tables_the
    // _query_named` is what pins it.
    for (idx, (table, seg)) in segments_to_verify(dir, tables).into_iter().enumerate() {
        if sweep_out_of_budget(idx, deadline, dir) {
            tracing::warn!(
                "segment integrity sweep ran out of its query's time budget before checking every \
                 segment named by {tables:?} - what it found so far still applies, the rest is \
                 unverified for this request"
            );
            break;
        }
        #[cfg(test)]
        {
            *test_sweep_segments_processed()
                .lock()
                .unwrap()
                .entry(dir.to_path_buf())
                .or_insert(0) += 1;
        }
        let path = segment_path(dir, &seg.file, &seg.hash);
        // An absent file is not corruption, and `define_views` already skips it by existence.
        if !path.exists() {
            continue;
        }
        // An unreadable segment is not intact: it cannot serve rows either way, and saying
        // "fine" about a file we could not read is the failure this whole issue is about.
        let intact =
            std::fs::read(&path).is_ok_and(|bytes| hex::encode(Sha256::digest(&bytes)) == seg.hash);
        if !intact {
            tracing::error!(
                "segment {} for table {table} does not match its content address - dropping it \
                 from this query (cold data reduced). Restart to quarantine it, or re-seal the \
                 range to restore it.",
                seg.file
            );
            bad.insert(seg.hash.clone());
        }
    }
    bad
}

/// The segments [`segments_failing_verification`] is allowed to read: those belonging to a table in
/// `tables` (matched case-insensitively, as Burrmill matches identifiers). Manifest-only, no file IO -
/// which is what makes the sweep's cost bound testable without timing anything.
fn segments_to_verify(dir: &Path, tables: &BTreeSet<String>) -> Vec<(String, Segment)> {
    if tables.is_empty() {
        return Vec::new();
    }
    let Ok(manifest) = load_manifest(dir) else {
        return Vec::new();
    };
    // Both sides are lowercased here rather than trusting the caller to have done it: a table name
    // that arrives shouted would otherwise match nothing, and the failure mode of that is silent -
    // the query loses its reduction and nobody sees a difference except in the answer.
    let wanted: BTreeSet<String> = tables.iter().map(|t| t.to_ascii_lowercase()).collect();
    manifest
        .tables
        .iter()
        .filter(|(table, _)| wanted.contains(&table.to_ascii_lowercase()))
        .flat_map(|(table, segs)| segs.iter().map(move |s| (table.clone(), s.clone())))
        .collect()
}

/// Startup integrity pass: verify every manifest segment's file exists and its bytes hash to the
/// recorded content address. A file that's missing, unreadable, or hash-mismatched is corrupt or
/// tampered with - quarantine it (move to a sibling `quarantine/` dir so `define_views` skips it) and
/// log loudly, then continue. A corrupt segment must *reduce* a table's cold data, never crash-loop the
/// node. Sealed data is immutable and content-addressed, so a hash mismatch is unambiguous corruption.
/// Returns the number of segments quarantined. Best-effort - never fatal (an IO error just logs).
pub fn verify_and_quarantine(dir: &Path) -> Result<usize> {
    let mut manifest = load_manifest(dir)?;
    // Sibling of `segments/`, deliberately *outside* it - nothing globs or SQL-reads the quarantine.
    let quarantine = dir.join("quarantine");
    let mut quarantined = 0usize;
    // A bad provisional tail stays catalogued, so its loss stays visible, but is never folded or aged
    // again: both read its file, and the failed read stopped every later seal (#2172).
    let mut lost_tails = 0usize;

    for (table, segs) in &mut manifest.tables {
        for s in segs.iter_mut() {
            let path = segment_path(dir, &s.file, &s.hash);
            // **Never quarantine a shared segment** (RFC-0033 §11a). Quarantine *moves* the file, and
            // in the shared store those bytes belong to every dataset referencing them - one nest's
            // integrity pass must not yank data out from under its neighbours. A corrupt shared
            // segment is reported loudly and left in place; the operator decides, with the knowledge
            // that more than one dataset is affected.
            let is_shared = shared_store(dir).is_some_and(|st| path.starts_with(&st));
            let reason = match std::fs::read(&path) {
                Ok(bytes) if hex::encode(Sha256::digest(&bytes)) == s.hash => continue, // intact
                Ok(_) => "hash mismatch (corrupt or tampered)",
                // Already gone from disk - nothing to move; `define_views` skips it. Not counted.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    if s.provisional {
                        s.provisional = false;
                        lost_tails += 1;
                    }
                    continue;
                }
                Err(_) => "unreadable",
            };
            if s.provisional {
                s.provisional = false;
                lost_tails += 1;
            }
            if is_shared {
                tracing::error!(
                    "shared segment {} for table {table} is {reason} - left in place because other \
                     datasets reference it. Remove it deliberately once you know what else breaks.",
                    s.hash
                );
                continue;
            }
            std::fs::create_dir_all(&quarantine).ok();
            let dest = quarantine.join(&s.file);
            match std::fs::rename(&path, &dest) {
                Ok(()) => {
                    tracing::error!(
                        "quarantined segment {} for table {table} ({reason}) → {} - cold data for this \
                         table is reduced. Its rows left the hot store when it was sealed, so nuthatch \
                         cannot re-seal them: restore a copy hashing to {} at {}, or re-index the nest",
                        s.file,
                        dest.display(),
                        s.hash,
                        path.display()
                    );
                    quarantined += 1;
                }
                Err(e) => tracing::error!(
                    "segment {} for {table} is {reason} but could not be quarantined: {e}",
                    s.file
                ),
            }
        }
    }
    if lost_tails > 0 {
        save_manifest(dir, &manifest)?;
    }
    if quarantined > 0 {
        tracing::error!(
            "startup integrity: quarantined {quarantined} corrupt segment(s) - data is reduced, node \
             continues; investigate disk health, then restore the files or re-index the nest"
        );
    }
    Ok(quarantined)
}

/// The segment's file is present and hashes to its catalogue entry.
fn segment_intact(dir: &Path, s: &Segment) -> bool {
    std::fs::read(segment_path(dir, &s.file, &s.hash))
        .is_ok_and(|bytes| hex::encode(Sha256::digest(&bytes)) == s.hash)
}

/// `(from, cut)` when a seal catalogued a fold through `cut` and stopped before its prune (#2169).
/// The folded segment starts at or below `sealed_through` and ends past it, so the watermark admits
/// it to no read, and the provisional rows it absorbed are no longer in hot either. Every table's
/// rows through `cut` went into that one catalogue install, so the seal can be finished from here.
///
/// An error when a segment the finish would leave as the only copy of its rows is missing or does
/// not hash to its entry: the prune would then lose the hot copy too.
pub fn unpruned_fold(dir: &Path, sealed_through: u64) -> Result<Option<(u64, u64)>> {
    let manifest = load_manifest(dir)?;
    let segments = || manifest.tables.values().flatten();
    let Some((from, cut)) = segments()
        .filter(|s| s.from_block <= sealed_through && s.to_block > sealed_through)
        .fold(None, |acc, s| match acc {
            None => Some((s.from_block, s.to_block)),
            Some((from, cut)) => Some((from.min(s.from_block), cut.max(s.to_block))),
        })
    else {
        return Ok(None);
    };
    for s in segments().filter(|s| s.to_block > sealed_through && s.to_block <= cut) {
        if !segment_intact(dir, s) {
            anyhow::bail!(
                "segment {} (blocks {}..={}) is missing or does not hash to its catalogue entry",
                s.file,
                s.from_block,
                s.to_block
            );
        }
    }
    Ok(Some((from, cut)))
}

/// What [`check_catalogue`] found. Diagnostic: it does not move files. Startup quarantine is
/// [`verify_and_quarantine`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CatalogueCheck {
    pub manifest_version: u32,
    pub segments: usize,
    pub missing: Vec<String>,
    pub hash_mismatch: Vec<String>,
}

impl CatalogueCheck {
    pub fn ok(&self) -> bool {
        self.missing.is_empty() && self.hash_mismatch.is_empty()
    }
}

/// Every catalogue entry's file exists and hashes. A missing file or a hash mismatch is reported,
/// not quarantined, so `nuthatch doctor --catalogue` cannot yank data.
pub fn check_catalogue(dir: &Path) -> Result<CatalogueCheck> {
    let manifest = load_manifest(dir)?;
    let mut out = CatalogueCheck {
        manifest_version: manifest.manifest_version,
        ..Default::default()
    };
    for segs in manifest.tables.values() {
        for s in segs {
            out.segments += 1;
            let path = segment_path(dir, &s.file, &s.hash);
            match std::fs::read(&path) {
                Ok(bytes) if hex::encode(Sha256::digest(&bytes)) == s.hash => {}
                Ok(_) => out.hash_mismatch.push(s.file.clone()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    out.missing.push(s.file.clone())
                }
                Err(_) => out.hash_mismatch.push(s.file.clone()),
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Durable {
    FileSync(PathBuf),
    ParentDir(PathBuf),
    SegmentDir(PathBuf),
    ManifestDir(PathBuf),
}

/// A temp name no other write in this process holds: two nests in one runtime can publish the same
/// content address at once (#2230).
fn temp_name(file_name: &str) -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!(".{file_name}.{}.{n}.tmp", std::process::id())
}

/// Install `bytes` as `dir/file_name`. The bytes are fsynced before the name exists, and that
/// directory entry is fsynced before this returns (#1632). A watermark is written only afterwards.
pub(crate) fn publish_durable(dir: &Path, file_name: &str, bytes: &[u8]) -> Result<()> {
    let created = !dir.exists();
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    if created {
        if let Some(parent) = dir.parent() {
            #[cfg(test)]
            note_durable(Durable::ParentDir(parent.to_path_buf()));
            sync_dir(parent)?;
        }
    }
    let final_path = dir.join(file_name);
    // The shared store: another nest already published this content. Its bytes were synced before
    // the name existed. The directory entry still has to be, before our manifest names the file.
    if final_path.is_file() {
        sync_segment_dir(dir)?;
        return Ok(());
    }
    let tmp = dir.join(temp_name(file_name));
    {
        let mut f =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        use std::io::Write;
        f.write_all(bytes)
            .with_context(|| format!("writing {}", tmp.display()))?;
        f.sync_all()
            .with_context(|| format!("fsyncing {}", tmp.display()))?;
        #[cfg(test)]
        note_durable(Durable::FileSync(tmp.clone()));
    }
    #[cfg(test)]
    before_install();
    std::fs::rename(&tmp, &final_path)
        .with_context(|| format!("installing {}", final_path.display()))?;
    sync_segment_dir(dir)?;
    Ok(())
}

/// Replace `path` with `bytes`: written and fsynced under a temp name, renamed over it, and the
/// directory entry fsynced, so a crash leaves the old file or the new one.
pub(crate) fn replace_durable(path: &Path, bytes: &[u8]) -> Result<()> {
    let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
        anyhow::bail!("{} has no parent directory", path.display());
    };
    let tmp = dir.join(temp_name(&name.to_string_lossy()));
    {
        let f =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        use std::io::Write;
        (&f).write_all(bytes)
            .with_context(|| format!("writing {}", tmp.display()))?;
        f.sync_all()
            .with_context(|| format!("fsyncing {}", tmp.display()))?;
        #[cfg(test)]
        note_durable(Durable::FileSync(tmp.clone()));
    }
    std::fs::rename(&tmp, path).with_context(|| format!("installing {}", path.display()))?;
    sync_manifest_dir(dir)
}

fn sync_segment_dir(dir: &Path) -> Result<()> {
    #[cfg(test)]
    note_durable(Durable::SegmentDir(dir.to_path_buf()));
    sync_dir(dir)
}

fn sync_manifest_dir(dir: &Path) -> Result<()> {
    #[cfg(test)]
    note_durable(Durable::ManifestDir(dir.to_path_buf()));
    sync_dir(dir)
}

fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_DIR_SYNC.with(|fail| fail.get()) {
        anyhow::bail!("fsyncing {} failed", dir.display());
    }
    let directory = std::fs::File::open(dir)
        .with_context(|| format!("opening {} to fsync it", dir.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("fsyncing {}", dir.display()))
}

#[cfg(test)]
thread_local! {
    static DURABLE_TRACE: std::cell::RefCell<Option<Vec<Durable>>> =
        const { std::cell::RefCell::new(None) };
    static FAIL_DIR_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn note_durable(event: Durable) {
    DURABLE_TRACE.with(|trace| {
        if let Some(buf) = trace.borrow_mut().as_mut() {
            buf.push(event);
        }
    });
}

#[cfg(test)]
pub(crate) fn capture_durable(run: impl FnOnce()) -> Vec<Durable> {
    DURABLE_TRACE.with(|trace| *trace.borrow_mut() = Some(Vec::new()));
    run();
    DURABLE_TRACE.with(|trace| trace.borrow_mut().take().unwrap_or_default())
}

#[cfg(test)]
thread_local! {
    static BEFORE_MANIFEST: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct BeforeManifestGuard;

#[cfg(test)]
impl Drop for BeforeManifestGuard {
    fn drop(&mut self) {
        BEFORE_MANIFEST.with(|hook| *hook.borrow_mut() = None);
    }
}

#[cfg(test)]
fn set_before_manifest(hook: impl FnMut() + 'static) -> BeforeManifestGuard {
    BEFORE_MANIFEST.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    BeforeManifestGuard
}

#[cfg(test)]
fn before_manifest() {
    BEFORE_MANIFEST.with(|hook| {
        if let Some(run) = hook.borrow_mut().as_mut() {
            run();
        }
    });
}

#[cfg(test)]
thread_local! {
    static BEFORE_INSTALL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs once, between `publish_durable`'s temp write and its rename.
#[cfg(test)]
fn set_before_install(hook: impl FnOnce() + 'static) -> impl Drop {
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            BEFORE_INSTALL.with(|hook| *hook.borrow_mut() = None);
        }
    }
    BEFORE_INSTALL.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    Clear
}

#[cfg(test)]
fn before_install() {
    if let Some(run) = BEFORE_INSTALL.with(|hook| hook.borrow_mut().take()) {
        run();
    }
}

/// Install a whole catalogue into a nest that has none of its own yet (`nuthatch seed`).
pub(crate) fn install_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
    std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).context("creating the segments directory")?;
    save_manifest(dir, manifest)
}

fn save_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
    let raw = serde_json::to_string_pretty(manifest)?;
    // The manifest is the segment catalogue - the crown jewels of a `kill -9`-survivable single binary
    // (a half-written `manifest.json` orphans every otherwise-fine `.parquet` and fails all cold reads,
    // deadlock-review finding M8). Write a sibling temp file then rename it over the target: `rename`
    // is atomic on the same filesystem, so a reader/crash sees either the old manifest or the new one,
    // never a torn one.
    let path = manifest_path(dir);
    let tmp = path.with_extension("json.tmp");
    // COR-9: fsync the temp file's *bytes* before the rename, and the directory *entry* after - so the
    // atomic rename survives power loss, not just process death. Without the fsyncs a `rename` can be
    // reordered before the data hits disk, exposing a torn/empty manifest that orphans the segments.
    {
        let f = std::fs::File::create(&tmp).context("failed to create manifest temp")?;
        use std::io::Write;
        (&f).write_all(raw.as_bytes())
            .context("failed to write manifest temp")?;
        f.sync_all().context("failed to fsync manifest temp")?;
    }
    std::fs::rename(&tmp, &path).context("failed to install manifest")?;
    // The renamed entry lives in `segments/`. Syncing `dir` does not install it (#1632).
    if let Some(parent) = path.parent() {
        sync_manifest_dir(parent)?;
    }
    manifest_signal(dir).send_modify(|installs| *installs += 1);
    Ok(())
}

/// Catalogue installs for one nest directory, so a mirror wakes on a seal without the seal ever
/// waiting on it (RFC-0052 §3.4). A receiver that misses a bump still sees the latest count.
pub fn manifest_changes(dir: &Path) -> tokio::sync::watch::Receiver<u64> {
    manifest_signal(dir).subscribe()
}

fn manifest_signal(dir: &Path) -> tokio::sync::watch::Sender<u64> {
    static SIGNALS: OnceLock<Mutex<HashMap<PathBuf, tokio::sync::watch::Sender<u64>>>> =
        OnceLock::new();
    let key = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    SIGNALS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(key)
        .or_insert_with(|| tokio::sync::watch::channel(0).0)
        .clone()
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(SEGMENTS_DIR).join(MANIFEST_FILE)
}

#[cfg(test)]
mod sealed_rows {
    use super::*;
    use crate::registry::{
        ColumnSchema, DecodedRow, TableKind, TableSchema, Value as DecodedValue,
    };

    fn schema() -> TableSchema {
        let mut columns = crate::registry::implicit_columns(true);
        columns.extend(
            [
                ("from", "address", "address"),
                ("to", "address", "address"),
                ("value", "uint256", "word32"),
                ("ok", "bool", "bool"),
                ("memo", "string", "string"),
            ]
            .iter()
            .map(|(name, sol, storage)| ColumnSchema {
                name: (*name).to_string(),
                sol_type: (*sol).to_string(),
                storage: (*storage).to_string(),
                indexed: false,
                components: Vec::new(),
            }),
        );
        TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: TableKind::Event,
            event: String::new(),
            topic0: String::new(),
            function: String::new(),
            selector: String::new(),
            columns,
        }
    }

    fn row(block: u64, log_index: u64, value: alloy_primitives::U256) -> DecodedRow {
        DecodedRow {
            table: "usdc__transfer".into(),
            params: vec![
                ("from".into(), DecodedValue::Address([0x11; 20])),
                ("to".into(), DecodedValue::Address([0x22; 20])),
                (
                    "value".into(),
                    DecodedValue::Word32(value.to_be_bytes::<32>()),
                ),
                ("ok".into(), DecodedValue::Bool(true)),
                // A string that looks exactly like the uint256 beside it. Only the schema separates
                // them, and a reader that lost the distinction would still round-trip through JSON.
                ("memo".into(), DecodedValue::Str("7".into())),
            ],
            block_number: block,
            block_hash: "0xbh".into(),
            block_timestamp: 1_700_000_000 + block,
            timestamps: true,
            log_index,
            tx_hash: "0xtx".into(),
            address: "0xaa".into(),
        }
    }

    /// **The property the warm-restart seed rests on** (RFC-0041 §5.3, nuthatch#864): a row that has
    /// been sealed to Parquet and read back is the *same row* the ingest path produced.
    ///
    /// It matters because the two representations genuinely differ - `rows_to_batch` writes a
    /// `uint256` as a decimal string and a `bool` as the text `"true"` - so equality here is a claim
    /// about the conversion, not a tautology. If it did not hold, a seeded entity and a live one
    /// would key on different rows, and a retraction would stop cancelling its insertion.
    #[test]
    fn a_sealed_row_reads_back_as_the_row_that_was_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let schema = schema();
        let original: Vec<DecodedRow> = vec![
            row(10, 0, alloy_primitives::U256::from(7u64)),
            row(10, 1, alloy_primitives::U256::MAX),
            row(11, 0, alloy_primitives::U256::ZERO),
        ];

        let json: Vec<String> = original.iter().map(|r| r.to_json().to_string()).collect();
        seal_range(dir.path(), &json, 10, 11)
            .unwrap()
            .expect("something sealed");

        let back = read_table_rows(dir.path(), &schema).unwrap();
        assert_eq!(back, original, "sealed and live must be the same rows");
    }

    /// Segments are read in block order however the manifest happens to list them. An entity folds a
    /// commutative relation, so order does not change its answer - but a seed that also replays a
    /// hot tail has to know where the sealed part ended, and "the last row read" is only that if the
    /// rows came back in order.
    #[test]
    fn segments_come_back_in_block_order_not_manifest_order() {
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal, or there is only one segment and no order to get wrong (#1150).
        test_set_table_floor(dir.path(), 0);
        let schema = schema();
        // Three, not two: with two, reversing the manifest happens to produce the sorted order, so
        // a reader that reversed instead of sorting would pass. Sealed 20, then 10, then 30.
        for (from, to, block) in [(20u64, 20u64, 20u64), (10, 10, 10), (30, 30, 30)] {
            let json = vec![row(block, 0, alloy_primitives::U256::from(block))
                .to_json()
                .to_string()];
            seal_range(dir.path(), &json, from, to).unwrap();
        }
        let manifest = load_manifest(dir.path()).unwrap();
        assert_eq!(
            manifest.tables["usdc__transfer"]
                .iter()
                .map(|s| s.from_block)
                .collect::<Vec<_>>(),
            vec![20, 10, 30],
            "the premise: the manifest lists them out of order, and not merely reversed"
        );

        let back = read_table_rows(dir.path(), &schema).unwrap();
        assert_eq!(
            back.iter().map(|r| r.block_number).collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
    }

    /// A sealed row missing a column the schema declares must be refused, not read as an empty
    /// string. `rows_to_batch` writes a missing key as a Parquet null, and an empty string would go
    /// on to decode as a zero-length address - a silently wrong row rather than a loud one.
    #[test]
    fn a_null_column_in_a_sealed_row_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let complete = row(10, 0, alloy_primitives::U256::from(7u64)).to_json();
        let mut partial = complete.as_object().unwrap().clone();
        partial.remove("memo");
        // Two rows so the column exists in the batch and is null for one of them.
        let json = vec![complete.to_string(), Value::Object(partial).to_string()];
        seal_range(dir.path(), &json, 10, 10).unwrap().unwrap();

        let err = format!(
            "{:#}",
            read_table_rows(dir.path(), &schema()).expect_err("a null column is not a value")
        );
        assert!(err.contains("null"), "{err}");
    }

    /// A table with no sealed segments is an empty read, not an error. A nest that has sealed nothing
    /// yet is the ordinary case on a young nest, and treating it as a failure would make the seed
    /// refuse exactly the nests it costs least to build.
    #[test]
    fn a_table_with_nothing_sealed_reads_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path()).unwrap();
        assert!(read_table_rows(dir.path(), &schema()).unwrap().is_empty());
    }
}

/// Whether a file's Parquet footer and schema read: what "the segment binds" means, checked without an
/// engine, for a test that must know its corrupt fixture is the corruption it claims.
#[cfg(test)]
pub(crate) fn footer_reads(path: &Path) -> bool {
    std::fs::File::open(path).is_ok_and(|f| {
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f).is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nuthatch_seal::{bloom_column, dictionary_off};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use std::fs::File;

    fn transfer(block: u64, li: u64, value: &str) -> String {
        format!(
            r#"{{"table":"usdc__transfer","from":"0xaaaa","to":"0xbbbb","value":"{value}","block_number":{block},"tx_hash":"0xcc","log_index":{li}}}"#
        )
    }
    fn approval(block: u64, li: u64) -> String {
        format!(
            r#"{{"table":"usdc__approval","owner":"0xaaaa","spender":"0xdddd","value":"1","block_number":{block},"tx_hash":"0xcc","log_index":{li}}}"#
        )
    }

    /// The table these fixtures seal into, as the sweep's reachability bound would name it: what a
    /// query over `usdc__transfer` is allowed to make the sweep read.
    fn usdc() -> BTreeSet<String> {
        ["usdc__transfer".to_string()].into_iter().collect()
    }

    /// RFC-0047 C1 / #1221. The external-reader contract: four counter columns are UInt64,
    /// everything else (including a uint256) is Utf8 decimal or other canonical text, and the
    /// query-side `*_dec` / `*_overflow` companions are not in the file. A type change here makes
    /// `docs/reading-segments.md` a lie.
    #[test]
    fn sealed_parquet_writes_uint64_counters_and_utf8_uint256_without_dec_companions() {
        let dir = tempfile::tempdir().unwrap();
        let row = r#"{"table":"usdc__transfer","from":"0xaaaa","to":"0xbbbb","value":"9","block_number":1,"tx_hash":"0xcc","log_index":0,"_seq":1,"block_timestamp":1700000000}"#;
        seal_range(dir.path(), &[row.to_string()], 1, 1)
            .unwrap()
            .expect("sealed");
        let manifest = load_manifest(dir.path()).unwrap();
        let seg = &manifest.tables["usdc__transfer"][0];
        let path = segment_path(dir.path(), &seg.file, &seg.hash);
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        let schema = builder.schema();

        let ty = |name: &str| -> DataType {
            schema
                .field_with_name(name)
                .unwrap_or_else(|_| panic!("{name} missing from the parquet schema"))
                .data_type()
                .clone()
        };
        assert_eq!(ty("block_number"), DataType::UInt64);
        assert_eq!(ty("log_index"), DataType::UInt64);
        assert_eq!(ty("_seq"), DataType::UInt64);
        assert_eq!(ty("block_timestamp"), DataType::UInt64);
        assert_eq!(
            ty("value"),
            DataType::Utf8,
            "uint256 is canonical decimal text, not FLBA32"
        );

        for field in schema.fields() {
            let n = field.name();
            assert!(
                !n.ends_with("_dec") && !n.ends_with("_overflow"),
                "{n} is a Burrmill view column and must not be written to parquet"
            );
        }

        let batch = builder.build().unwrap().next().unwrap().unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            values.value(0),
            "9",
            "unpadded decimal text; as UTF-8, \"9\" > \"10\""
        );
    }

    /// #2184: a catalogue a cloned nest ships cannot name a file outside its own `segments/`.
    #[test]
    fn a_catalogue_naming_a_path_outside_segments_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(SEGMENTS_DIR)).unwrap();
        let entry = |table: &str, hash: &str, file: &str| {
            format!(
                r#"{{"tables":{{"{table}":[{{"hash":"{hash}","from_block":1,"to_block":1,"rows":1,"file":"{file}","provisional":true}}]}}}}"#
            )
        };
        for (table, hash, file) in [
            ("t", "aa", "../../victim/segments/t-aa.parquet"),
            ("t", "aa", "/etc/t-aa.parquet"),
            ("t", "aa", "..parquet"),
            ("t", "../aa", "t-aa.parquet"),
            ("../t", "aa", "t-aa.parquet"),
        ] {
            std::fs::write(manifest_path(dir.path()), entry(table, hash, file)).unwrap();
            assert!(load_manifest(dir.path()).is_err(), "{table} {hash} {file}");
            assert!(load_manifest_with_hash(dir.path()).is_err(), "{file}");
        }
        std::fs::write(manifest_path(dir.path()), entry("t", "aa", "t-aa.parquet")).unwrap();
        assert_eq!(
            load_manifest(dir.path()).unwrap().tables["t"][0].file,
            "t-aa.parquet"
        );
    }

    /// RFC-0047 C2 / #1223. A catalogue written before this field existed is version 0, and every
    /// segment in it was the original writer profile, not an unknown one.
    #[test]
    fn an_old_manifest_is_version_zero_and_names_the_original_writer_profile() {
        let raw = r#"{"tables":{"t":[{"hash":"aa","from_block":1,"to_block":1,"rows":1,"file":"t.parquet"}]}}"#;
        let m: Manifest = serde_json::from_str(raw).unwrap();
        assert_eq!(m.manifest_version, 0);
        assert_eq!(
            m.tables["t"][0].writer_profile, ORIGINAL_WRITER_PROFILE,
            "a missing writer_profile is the original profile, not unknown"
        );
    }

    #[test]
    fn a_new_seal_writes_manifest_version_and_the_writer_profile() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100)
            .unwrap()
            .expect("sealed");
        let m = load_manifest(dir.path()).unwrap();
        assert_eq!(m.manifest_version, MANIFEST_VERSION);
        assert_eq!(m.tables["usdc__transfer"][0].writer_profile, WRITER_PROFILE);
        assert!(check_catalogue(dir.path()).unwrap().ok());
    }

    fn sealed_path(dir: &Path) -> (Segment, PathBuf) {
        let m = load_manifest(dir).unwrap();
        let seg = m.tables["usdc__transfer"][0].clone();
        let path = segment_path(dir, &seg.file, &seg.hash);
        (seg, path)
    }

    fn writer_profile_matches_footer(profile: &str, path: &Path) -> bool {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        let rg = reader.metadata().row_group(0);
        let zstd = rg
            .columns()
            .iter()
            .all(|c| matches!(c.compression(), Compression::ZSTD(_)));
        let snappy = rg
            .columns()
            .iter()
            .all(|c| c.compression() == Compression::SNAPPY);
        match profile {
            ORIGINAL_WRITER_PROFILE => {
                snappy
                    && rg
                        .columns()
                        .iter()
                        .all(|c| c.bloom_filter_offset().is_none())
            }
            WRITER_PROFILE => zstd,
            _ => false,
        }
    }

    #[test]
    fn a_new_segment_footer_matches_the_named_writer_profile() {
        assert!(
            bloom_column("hash") && dictionary_off("hash"),
            "a column named hash is a hash column, not only *_hash"
        );
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100)
            .unwrap()
            .expect("sealed");
        let (seg, path) = sealed_path(dir.path());
        assert!(
            writer_profile_matches_footer(&seg.writer_profile, &path),
            "named profile must describe the bytes"
        );
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        let md = reader.metadata();
        // Carried over from `a_sealed_segment_footer_matches_the_writer_spec` (#1236), which this
        // test replaces: the assertions #1234 does not change still have to hold, or the profile
        // name would cover a footer that quietly moved in some other respect.
        assert_eq!(
            md.num_row_groups(),
            1,
            "one seal is one row group; a second group is a writer-config change"
        );
        let rg = md.row_group(0);
        assert!(
            rg.sorting_columns().is_some(),
            "sort metadata is the promise"
        );
        let mut dictionary_pages = 0usize;
        for col in rg.columns() {
            let name = col.column_path().string();
            assert!(
                col.statistics().is_some(),
                "{name}: column statistics are still on, and the profile name does not say otherwise"
            );
            assert_eq!(
                col.bloom_filter_offset().is_some(),
                bloom_column(&name),
                "{name}: the footer's blooms must be exactly the columns the profile names"
            );
            assert_eq!(
                col.dictionary_page_offset().is_none(),
                dictionary_off(&name),
                "{name}: dictionary encoding must be off on exactly the near-unique hash columns"
            );
            if col.dictionary_page_offset().is_some() {
                dictionary_pages += 1;
            }
        }
        assert!(
            dictionary_pages > 0,
            "dictionary encoding stays on outside the hash columns; no dictionary pages at all \
             means it was switched off wholesale"
        );
    }

    /// #1234: a change of bytes that nothing records is the defect this exists to prevent.
    #[test]
    fn a_profile_field_forced_back_to_snappy_does_not_match_the_new_footer() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100)
            .unwrap()
            .expect("sealed");
        let (_, path) = sealed_path(dir.path());
        assert!(
            !writer_profile_matches_footer(ORIGINAL_WRITER_PROFILE, &path),
            "new bytes labelled snappy must not pass the profile check"
        );
    }

    /// `doctor --catalogue` reports; it does not move the file. Startup quarantine is a different
    /// function, and a diagnostic that yanks data is the defect this exists to prevent.
    #[test]
    fn check_catalogue_reports_a_hash_mismatch_and_leaves_the_file() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100)
            .unwrap()
            .expect("sealed");
        let m = load_manifest(dir.path()).unwrap();
        let seg = &m.tables["usdc__transfer"][0];
        let path = segment_path(dir.path(), &seg.file, &seg.hash);
        std::fs::write(&path, b"not the bytes that were hashed").unwrap();
        let check = check_catalogue(dir.path()).unwrap();
        assert_eq!(check.hash_mismatch, vec![seg.file.clone()]);
        assert!(check.missing.is_empty());
        assert!(path.exists(), "the diagnostic must not quarantine");
    }

    /// **Issue #433, the cost bound the review sent back.** The sweep may read only the segments of
    /// the tables the failed query named - so the enumeration is a separate, IO-free step and this
    /// asserts it directly. The hashing loop can only touch what this hands it, which is why the
    /// bound is structural rather than a filter that could later drift below the `fs::read`.
    ///
    /// A behavioural test cannot see this: sweeping a table the query never named changes no answer,
    /// only cost. That is exactly how a cost bound rots quietly.
    #[test]
    fn the_sweep_enumerates_only_the_tables_the_query_named() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100).unwrap();
        seal_range(dir.path(), &[approval(101, 0)], 101, 101).unwrap();
        let manifest = load_manifest(dir.path()).unwrap();
        assert_eq!(manifest.tables.len(), 2, "two tables, one segment each");

        let named = |t: &str| -> Vec<String> {
            segments_to_verify(dir.path(), &[t.to_string()].into_iter().collect())
                .into_iter()
                .map(|(table, _)| table)
                .collect()
        };
        assert_eq!(
            named("usdc__transfer"),
            vec!["usdc__transfer".to_string()],
            "a query over one table must not make the other one's segments readable"
        );
        assert_eq!(named("usdc__approval"), vec!["usdc__approval".to_string()]);
        // Burrmill matches identifiers case-insensitively and the AST reports the name as written, so
        // a shouted table name must still find its own segments and no others.
        assert_eq!(named("USDC__TRANSFER"), vec!["usdc__transfer".to_string()]);
        assert!(
            named("no_such_table").is_empty(),
            "a name the nest does not have reaches nothing"
        );
        assert!(
            segments_to_verify(dir.path(), &BTreeSet::new()).is_empty(),
            "a query that names no table at all - the 27-byte `SELECT CAST('x' AS INTEGER)` the \
             review measured - must reach no segment whatsoever"
        );
    }

    /// **Issue #433.** The serving-path discriminator must catch page corruption that every cheap
    /// engine probe waves through, and must not accuse a healthy segment.
    ///
    /// The fixture asserts its own premise. Corrupting the data region while leaving the footer and
    /// magic bytes intact is the whole condition - if the file stopped binding it would be #430's
    /// case, already covered, and this test would be proving something else under this name. So it
    /// checks `read_parquet` still binds the corrupt file before asserting the hash catches it. That
    /// gap between "binds" and "hashes wrong" is exactly the ground #433 sits on.
    #[test]
    fn segments_failing_verification_catches_page_corruption_that_still_binds() {
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal: this is about a damaged file, not about the table floor (#1150).
        test_set_table_floor(dir.path(), 0);
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100).unwrap();
        seal_range(dir.path(), &[transfer(101, 0, "7")], 101, 101).unwrap();
        let segs = load_manifest(dir.path()).unwrap().tables["usdc__transfer"].clone();
        assert_eq!(segs.len(), 2);

        // Nothing is wrong yet, and saying so is half the test: a discriminator that always answered
        // "corrupt" would reduce every table on the first execution error in a healthy nest.
        assert!(
            segments_failing_verification(dir.path(), &usdc(), None).is_empty(),
            "a healthy tree must accuse nothing"
        );

        // Destroy the pages of the block-101 segment, keeping footer and magic bytes.
        let victim = segs.iter().find(|s| s.from_block == 101).unwrap();
        let path = segment_path(dir.path(), &victim.file, &victim.hash);
        let mut bytes = std::fs::read(&path).unwrap();
        let len = bytes.len();
        let footer_len = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as usize;
        let end = len - 8 - footer_len;
        assert!(end > 4, "the fixture needs a data region to corrupt");
        bytes[4..end].fill(0xFF);
        std::fs::write(&path, &bytes).unwrap();

        // **The fixture is the condition it names.** Still a Parquet file as far as binding goes.
        assert!(
            footer_reads(&path),
            "if this no longer binds, the fixture has become #430's footer-corrupt case and this \
             test has stopped testing #433"
        );

        let bad = segments_failing_verification(dir.path(), &usdc(), None);
        assert_eq!(
            bad.len(),
            1,
            "exactly the corrupt segment, and not its healthy sibling"
        );
        assert!(bad.contains(&victim.hash), "and it is the one we corrupted");
    }

    /// **Issue #433, and the reason there is no cache in `segments_failing_verification`.**
    ///
    /// A verdict about a segment's bytes must not be keyed on the segment's timestamp. Measured on
    /// this box (btrfs, Linux 6.12): rewriting a file in place with the same length inside one kernel
    /// timer tick leaves `st_mtime_ns` **byte-identical**, because writes are stamped from a per-tick
    /// cached clock. So a `(mtime, len)` memo cannot see an in-place same-length overwrite - exactly
    /// the corruption this is for.
    ///
    /// I know this because I wrote that memo, and then wrote in its doc comment that the test above
    /// would catch it coming back. **It would not**: mutating the memo back in left the whole suite
    /// green, because the two sweeps in that test happen to straddle a tick. A claim about a
    /// mechanism, with nothing behind it, in the fix for claims with nothing behind them.
    ///
    /// So this constructs the condition deterministically instead of hoping for it: corrupt the bytes
    /// and put the original timestamp back, which is what a same-tick overwrite does anyway. Linux
    /// only - it needs GNU `touch -d @<epoch.nanos>` - and CI is Linux.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_corrupt_segment_is_caught_even_when_its_mtime_is_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(200, 0, "9")], 200, 200).unwrap();
        let seg = load_manifest(dir.path()).unwrap().tables["usdc__transfer"][0].clone();
        let path = segment_path(dir.path(), &seg.file, &seg.hash);

        // Ask once while it is healthy: this is what would populate any cache.
        assert!(segments_failing_verification(dir.path(), &usdc(), None).is_empty());

        let before = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let len = bytes.len();
        let footer_len = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as usize;
        bytes[4..len - 8 - footer_len].fill(0xFF);
        std::fs::write(&path, &bytes).unwrap();

        // Put the clock back, so the file is byte-different and timestamp-identical.
        let d = before.duration_since(std::time::UNIX_EPOCH).unwrap();
        let ok = std::process::Command::new("touch")
            .arg("-d")
            .arg(format!("@{}.{:09}", d.as_secs(), d.subsec_nanos()))
            .arg(&path)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(ok, "the fixture needs GNU touch to restore the timestamp");
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            before,
            "the whole point of this test is that the timestamp did not move"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            len as u64,
            "and neither did the length"
        );

        assert_eq!(
            segments_failing_verification(dir.path(), &usdc(), None).len(),
            1,
            "corruption must be caught from the bytes - a verdict cached on (mtime, len) would call \
             this segment intact, which is the failure this whole issue is about"
        );
    }

    /// **Issue #476, tightened for #529.** Concurrent callers naming the same tables must share one
    /// sweep, not each pay to hash the same segments - the amplifier the issue names:
    /// `SQL_MAX_CONCURRENCY` permits held by identical repeated requests, each currently paying the
    /// full cost on its own.
    ///
    /// #529: the original made this deterministic with a fixed 150ms per-segment delay, keeping the
    /// leader busy long enough - it hoped - for the three followers to be scheduled and find its slot
    /// still in the map. That raced real thread scheduling rather than the filesystem, and at load
    /// average 34 a follower can sit unscheduled well past 150ms, arrive to find the leader already
    /// gone, and start an uncoalesced sweep of its own - `cargo test --lib` was red on a busy dev box
    /// and green on a quiet one. A registration barrier (`test_set_sweep_registration_barrier`)
    /// removes the race structurally instead of widening the margin: every one of the four callers
    /// blocks inside `segments_failing_verification` until all four have made their leader-or-follower
    /// decision, so there is no wall-clock window in which a follower can miss the leader's slot no
    /// matter how the threads are scheduled.
    #[test]
    fn concurrent_identical_sweeps_coalesce_into_one() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100).unwrap();

        test_set_sweep_registration_barrier(dir.path(), 4);

        let barrier = Arc::new(std::sync::Barrier::new(4));
        let (tx, rx) = std::sync::mpsc::channel();
        for _ in 0..4 {
            let dir = dir.path().to_path_buf();
            let tables = usdc();
            let barrier = Arc::clone(&barrier);
            let tx = tx.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let _ = tx.send(segments_failing_verification(&dir, &tables, None));
            });
        }
        drop(tx);
        // A follower that is never woken waits forever with no deadline; that has to fail here
        // rather than hang the test binary (#1757).
        let results: Vec<_> = (0..4)
            .map(|_| {
                rx.recv_timeout(Duration::from_secs(60))
                    .expect("a coalesced caller never returned: its leader did not wake it")
            })
            .collect();
        test_clear_sweep_registration_barrier(dir.path());

        for r in &results {
            assert_eq!(
                r, &results[0],
                "coalesced callers must all see the same answer"
            );
        }
        assert_eq!(
            test_sweep_start_count(dir.path()),
            1,
            "4 concurrent callers naming the same tables must pay for one sweep, not 4"
        );
    }

    /// #1810: a leader that panics must still wake a follower that sits between its map check and its
    /// `wait`. Without a deadline that follower is query()/query_cold(), and a lost wake-up is a hang.
    #[test]
    fn a_follower_is_woken_when_its_leader_panics_mid_check() {
        a_follower_survives_a_panicking_leader(TestPanicMode::FollowerHoldsLock);
    }

    /// #1810, the other side: a follower that registers while the panicking leader's cleanup is under
    /// way must not slip in between the cleanup releasing the lock and sending its notify.
    #[test]
    fn a_follower_arriving_during_a_panicked_cleanup_is_woken() {
        a_follower_survives_a_panicking_leader(TestPanicMode::CleanupHoldsLock);
    }

    fn a_follower_survives_a_panicking_leader(mode: TestPanicMode) {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100).unwrap();
        let hook = test_arm_sweep_panic_hook(dir.path(), mode);

        let leader = {
            let dir = dir.path().to_path_buf();
            std::thread::spawn(move || segments_failing_verification(&dir, &usdc(), None))
        };
        let (ready, done) = match mode {
            TestPanicMode::FollowerHoldsLock => (1, 3),
            TestPanicMode::CleanupHoldsLock => (2, 5),
        };
        assert_eq!(
            hook.wait_for(ready, Duration::from_secs(30)),
            ready,
            "the leader never reached the point the follower is meant to race"
        );

        let (tx, rx) = std::sync::mpsc::channel();
        {
            let dir = dir.path().to_path_buf();
            std::thread::spawn(move || {
                let _ = tx.send(segments_failing_verification(&dir, &usdc(), None));
            });
        }
        let followed = rx.recv_timeout(Duration::from_secs(30));
        test_clear_sweep_panic_hook(dir.path());

        assert!(leader.join().is_err(), "the injected panic never fired");
        assert_eq!(
            hook.wait_for(done, Duration::ZERO),
            done,
            "the interleaving under test never happened"
        );
        assert_eq!(
            followed
                .expect("the follower missed its panicking leader's wake-up and never returned"),
            BTreeSet::new()
        );
    }

    /// **Issue #476, the other half - tightened for #529.** A sweep must stop at its own deadline
    /// rather than hash every named segment regardless of how long that takes - so a query's time
    /// budget bounds the sweep it triggers, not just the query execution either side of it.
    ///
    /// #529: the original raced an 80ms-per-segment `thread::sleep` against a 100ms deadline and then
    /// asserted `elapsed < 220ms` to prove the loop actually stopped rather than merely returning an
    /// empty set by chance. At load average 34 that margin (one segment's worth) evaporates: a
    /// descheduled thread can wake up arbitrarily later than the 80ms it asked for, and the elapsed
    /// wall-clock assertion above went red on its own even when the early exit itself was correct.
    /// This pins the same property without a clock in it at all: `test_set_sweep_expire_after_checks`
    /// tells the sweep loop directly that its budget is spent after the first check, and
    /// `test_sweep_segments_processed_count` proves by construction (not by inference from elapsed
    /// time) that only the first of the three segments was ever read - the corrupt one, last in block
    /// order, is never reached.
    #[test]
    fn sweep_stops_at_its_deadline_instead_of_running_to_completion() {
        let dir = tempfile::tempdir().unwrap();
        // Three segments, so the deadline has something to stop short of (#1150).
        test_set_table_floor(dir.path(), 0);
        for (block, value) in [(100u64, "1"), (101, "2"), (102, "3")] {
            seal_range(dir.path(), &[transfer(block, 0, value)], block, block).unwrap();
        }
        let segs = load_manifest(dir.path()).unwrap().tables["usdc__transfer"].clone();
        assert_eq!(segs.len(), 3);
        let victim = segs.iter().max_by_key(|s| s.from_block).unwrap();
        let path = segment_path(dir.path(), &victim.file, &victim.hash);
        let mut bytes = std::fs::read(&path).unwrap();
        let len = bytes.len();
        let footer_len = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as usize;
        let end = len - 8 - footer_len;
        assert!(end > 4, "the fixture needs a data region to corrupt");
        bytes[4..end].fill(0xFF);
        std::fs::write(&path, &bytes).unwrap();

        test_set_sweep_expire_after_checks(dir.path(), 1);
        // Still a real, non-expired deadline - a mutation that dropped the deadline argument
        // entirely (passed `None`) would let the sweep run unconstrained and reach the corrupt
        // segment regardless of the check-count override, which `bad.is_empty()` below would catch.
        let deadline = Some(Instant::now() + Duration::from_secs(3600));
        let bad = segments_failing_verification(dir.path(), &usdc(), deadline);
        test_set_sweep_expire_after_checks(dir.path(), -1);

        assert_eq!(
            test_sweep_segments_processed_count(dir.path()),
            1,
            "the budget allows exactly one check to pass; a second segment being read means the \
             early exit did not actually happen"
        );
        assert!(
            bad.is_empty(),
            "the deadline must land before the corrupt (last) segment is ever read, or this proves \
             nothing about stopping early"
        );
    }

    #[test]
    fn verify_quarantines_a_corrupt_segment_and_leaves_intact_ones() {
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal: this is about a damaged file, not about the table floor (#1150).
        test_set_table_floor(dir.path(), 0);
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100).unwrap();
        seal_range(dir.path(), &[transfer(101, 0, "7")], 101, 101).unwrap();
        let manifest = load_manifest(dir.path()).unwrap();
        let segs = &manifest.tables["usdc__transfer"];
        assert_eq!(segs.len(), 2);

        // A clean tree quarantines nothing.
        assert_eq!(verify_and_quarantine(dir.path()).unwrap(), 0);

        // Corrupt the first segment's bytes (simulate disk rot / tampering).
        let bad = dir.path().join(SEGMENTS_DIR).join(&segs[0].file);
        std::fs::write(&bad, b"not a parquet file anymore").unwrap();

        // Verify quarantines exactly the corrupt one; the intact one stays put.
        assert_eq!(verify_and_quarantine(dir.path()).unwrap(), 1);
        assert!(!bad.exists(), "corrupt file moved out of segments/");
        assert!(
            dir.path().join("quarantine").join(&segs[0].file).exists(),
            "corrupt file is in quarantine/"
        );
        assert!(
            dir.path().join(SEGMENTS_DIR).join(&segs[1].file).exists(),
            "intact segment untouched"
        );
        // Idempotent: the already-quarantined (now-missing) segment isn't re-counted.
        assert_eq!(verify_and_quarantine(dir.path()).unwrap(), 0);
    }

    #[test]
    fn seals_each_table_to_its_own_segment() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![
            transfer(100, 0, "5"),
            transfer(100, 1, "7"),
            approval(101, 0),
        ];
        let summary = seal_range(dir.path(), &rows, 100, 101).unwrap().unwrap();
        assert_eq!(summary.tables, 2); // transfer + approval
        assert_eq!(summary.rows, 3);

        let manifest = load_manifest(dir.path()).unwrap();
        assert_eq!(manifest.tables["usdc__transfer"].len(), 1);
        assert_eq!(manifest.tables["usdc__transfer"][0].rows, 2);
        assert_eq!(manifest.tables["usdc__approval"][0].rows, 1);

        // The transfer segment reads back with the right rows.
        let seg = &manifest.tables["usdc__transfer"][0];
        let file = File::open(dir.path().join(SEGMENTS_DIR).join(&seg.file)).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(total, 2);
    }

    /// #842 - the RFC-0033 §11a shared-store arm had no test at all.
    ///
    /// Recovered from the 2026-08-24 nightly mutation artifact, which found `delete ! in
    /// seal_range_with_snapshot` surviving and was then cancelled before it could report it. The
    /// guard is `if !shared.exists()`; with the `!` removed a *new* shared segment is never written
    /// and only an already-present one is rewritten. Every test passed, because nothing sealed into
    /// a shared store and then looked for the file.
    ///
    /// This is the arm two mounts of one NID depend on, so the failure it admits is a manifest
    /// listing a segment with no bytes behind it, on the dataset that by definition has more than
    /// one nest reading it.
    #[test]
    fn a_shared_store_receives_the_segment_bytes() {
        // `shared_store` is derived by convention: a dataset at `<root>/data/<nid>` shares
        // `<root>/segments`. A solo nest is not under a `data/` parent and takes the other arm.
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(crate::runtime::DATA_DIR).join("nid0");
        std::fs::create_dir_all(&dir).unwrap();
        let store = shared_store(&dir).expect("a dataset under data/ has a shared store");
        assert_eq!(store, root.path().join(SEGMENTS_DIR));
        assert!(
            !store.exists(),
            "the store does not exist before the first seal"
        );

        seal_range(&dir, &[transfer(100, 0, "5")], 100, 100).unwrap();

        // The manifest names one segment; its bytes must be in the shared store.
        let manifest = load_manifest(&dir).unwrap();
        let seg = &manifest.tables["usdc__transfer"][0];
        let shared = store.join(format!("{}.parquet", seg.hash));
        assert!(
            shared.is_file(),
            "the shared store holds no {}.parquet - the Some(store) arm wrote nothing. \
             Store contains: {:?}",
            seg.hash,
            std::fs::read_dir(&store)
                .map(|d| d.flatten().map(|e| e.file_name()).collect::<Vec<_>>())
        );
        assert!(
            std::fs::metadata(&shared).unwrap().len() > 0,
            "the shared segment is empty"
        );

        // And it went there *instead* of beside the nest - the two arms are exclusive.
        assert!(
            !dir.join(SEGMENTS_DIR).join(&seg.file).exists(),
            "a shared dataset must not also write the per-nest copy"
        );

        // The reader agrees with the writer about where the bytes are.
        assert_eq!(segment_path(&dir, &seg.file, &seg.hash), shared);
    }

    /// The other half of the same guard: re-sealing identical rows must not disturb the stored copy.
    /// This is what the `!` buys, and it is worth pinning separately so a fix for the test above
    /// cannot simply drop the condition.
    #[test]
    fn re_sealing_identical_rows_leaves_the_shared_segment_alone() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(crate::runtime::DATA_DIR).join("nid0");
        std::fs::create_dir_all(&dir).unwrap();
        seal_range(&dir, &[transfer(100, 0, "5")], 100, 100).unwrap();

        let manifest = load_manifest(&dir).unwrap();
        let seg = manifest.tables["usdc__transfer"][0].clone();
        let shared = shared_store(&dir)
            .unwrap()
            .join(format!("{}.parquet", seg.hash));
        let before = std::fs::read(&shared).unwrap();

        // Content-addressed idempotency: the same rows again are the same segment.
        seal_range(&dir, &[transfer(100, 0, "5")], 100, 100).unwrap();

        assert_eq!(
            std::fs::read(&shared).unwrap(),
            before,
            "a re-seal changed the shared segment's bytes"
        );
        assert_eq!(
            load_manifest(&dir).unwrap().tables["usdc__transfer"].len(),
            1,
            "a re-seal double-listed the segment in the manifest"
        );
    }

    /// A directory fsync that fails is the seal's error. Swallowing it would advance the
    /// watermark over an entry that may not be on disk.
    #[test]
    fn a_directory_fsync_failure_fails_the_seal() {
        let dir = tempfile::tempdir().unwrap();
        struct Clear;
        impl Drop for Clear {
            fn drop(&mut self) {
                FAIL_DIR_SYNC.with(|fail| fail.set(false));
            }
        }
        let _clear = Clear;
        FAIL_DIR_SYNC.with(|fail| fail.set(true));
        let err = seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10)
            .expect_err("a seal whose directory entry could not be synced");
        let err = format!("{err:#}");
        assert!(
            err.contains("fsyncing"),
            "the caller must hear the directory fsync failed: {err}"
        );
    }

    /// #1632: the watermark is fsynced after `seal_range` returns. The segment bytes and the
    /// manifest's directory entry have to be durable already. The entry lives in `segments/`.
    #[test]
    fn a_seal_is_durable_before_the_watermark_can_be() {
        let solo = tempfile::tempdir().unwrap();
        let trace = capture_durable(|| {
            seal_range(solo.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        });
        assert_seal_durable(solo.path(), &trace, solo.path().join(SEGMENTS_DIR));

        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(crate::runtime::DATA_DIR).join("nid0");
        std::fs::create_dir_all(&dir).unwrap();
        let trace = capture_durable(|| {
            seal_range(&dir, &[transfer(10, 0, "1")], 10, 10).unwrap();
        });
        let store = shared_store(&dir).unwrap();
        assert_seal_durable(&dir, &trace, store.clone());

        let other = root.path().join(crate::runtime::DATA_DIR).join("nid1");
        std::fs::create_dir_all(&other).unwrap();
        let again = capture_durable(|| {
            seal_range(&other, &[transfer(10, 0, "1")], 10, 10).unwrap();
        });
        assert_catalogue_durable(&other, &again, store);
    }

    fn assert_catalogue_durable(nest: &Path, trace: &[Durable], bytes_dir: PathBuf) {
        let manifest_dir = nest.join(SEGMENTS_DIR);
        let manifest_at = trace
            .iter()
            .rposition(|event| matches!(event, Durable::ManifestDir(path) if path == &manifest_dir))
            .unwrap_or_else(|| {
                panic!(
                    "the manifest entry lives in {}: that directory was not synced. trace: {trace:?}",
                    manifest_dir.display()
                )
            });
        let segment_at = trace
            .iter()
            .position(|event| matches!(event, Durable::SegmentDir(path) if path == &bytes_dir))
            .unwrap_or_else(|| {
                panic!(
                    "the segment directory {} was not synced. trace: {trace:?}",
                    bytes_dir.display()
                )
            });
        assert!(
            segment_at < manifest_at,
            "the segment directory entry must be durable before the manifest names it. trace: {trace:?}"
        );
    }

    fn assert_seal_durable(nest: &Path, trace: &[Durable], bytes_dir: PathBuf) {
        let manifest_dir = nest.join(SEGMENTS_DIR);
        let manifest_at = trace.iter().rposition(
            |event| matches!(event, Durable::ManifestDir(path) if path == &manifest_dir),
        );
        let manifest_at = manifest_at.unwrap_or_else(|| {
            panic!(
                "the manifest entry lives in {}: that directory was not synced. trace: {trace:?}",
                manifest_dir.display()
            )
        });
        let file_at = trace.iter().position(
            |event| matches!(event, Durable::FileSync(path) if path.starts_with(&bytes_dir)),
        );
        let file_at = file_at.unwrap_or_else(|| {
            panic!(
                "segment bytes in {} were not fsynced. trace: {trace:?}",
                bytes_dir.display()
            )
        });
        let segment_at = trace
            .iter()
            .position(|event| matches!(event, Durable::SegmentDir(path) if path == &bytes_dir));
        let segment_at = segment_at.unwrap_or_else(|| {
            panic!(
                "the segment directory {} was not synced. trace: {trace:?}",
                bytes_dir.display()
            )
        });
        assert!(
            file_at < segment_at && segment_at < manifest_at,
            "bytes, then their directory entry, then the manifest entry. trace: {trace:?}"
        );
    }

    /// The hold is taken before the file exists. A reclaim that canonicalises the file afterwards
    /// must still see it: on macOS that path gains a `/private` prefix the moment the file appears.
    #[test]
    fn a_hold_taken_before_the_file_exists_still_covers_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("abc.parquet");
        let _hold = SegmentHold::acquire(&path);
        std::fs::write(&path, b"parquet").unwrap();
        let canon = std::fs::canonicalize(&path).unwrap();
        assert!(
            segment_held(&canon),
            "the hold was keyed on the spelling from before the file existed"
        );
        assert!(
            !remove_segment_if_unheld(&canon).unwrap(),
            "a held segment must survive the reclaim that checks and deletes together"
        );
        assert!(canon.is_file());
        drop(_hold);
        assert!(remove_segment_if_unheld(&canon).unwrap());
        assert!(!canon.is_file());
    }

    /// #1644: B skips the shared write because A's file is already there, and A's reclaim runs
    /// before B's manifest names it. The file has to still be there when B's catalogue lands.
    #[test]
    fn a_reclaim_during_a_shared_seal_keeps_the_segment_the_seal_will_name() {
        use crate::runtime::{MountTable, MOUNTS_FILE};
        let root = tempfile::tempdir().unwrap();
        let (a, b) = ("aa".repeat(32), "bb".repeat(32));
        let dir_a = MountTable::data_dir(root.path(), &a);
        let dir_b = MountTable::data_dir(root.path(), &b);
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();
        let rows = [transfer(10, 0, "1")];
        seal_range(&dir_a, &rows, 10, 10).unwrap();
        let hash = load_manifest(&dir_a).unwrap().tables["usdc__transfer"][0]
            .hash
            .clone();
        let shared = shared_store(&dir_a)
            .unwrap()
            .join(format!("{hash}.parquet"));
        assert!(shared.is_file(), "premise: A published the shared segment");

        std::fs::write(
            root.path().join(MOUNTS_FILE),
            format!(
                "[runtime]\nname = \"r\"\n\n[[chains]]\nchain = \"arbitrum-one\"\n\
                 chain_id = 42161\nrpc_urls = []\n\n[[mounts]]\nalias = \"b\"\nnid = \"{b}\"\n"
            ),
        )
        .unwrap();

        let runtime = root.path().to_path_buf();
        let reclaim_a = a.clone();
        let _hook = set_before_manifest(move || {
            crate::prune::reclaim(&runtime, &reclaim_a)
                .expect("A is unmounted and its store is not open");
        });
        seal_range(&dir_b, &rows, 10, 10).unwrap();

        assert!(
            shared.is_file(),
            "reclaim removed the segment B's manifest is about to name"
        );
        assert_eq!(
            load_manifest(&dir_b).unwrap().tables["usdc__transfer"][0].hash,
            hash
        );
        assert!(
            !dir_a.exists(),
            "premise: the reclaim did run and took A's dataset"
        );
    }

    #[test]
    fn empty_range_seals_nothing() {
        let dir = tempfile::tempdir().unwrap();
        assert!(seal_range(dir.path(), &[], 1, 2).unwrap().is_none());
    }

    #[test]
    fn content_address_is_deterministic() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let rows = vec![transfer(1, 0, "1")];
        seal_range(dir1.path(), &rows, 1, 1).unwrap();
        seal_range(dir2.path(), &rows, 1, 1).unwrap();
        let a = &load_manifest(dir1.path()).unwrap().tables["usdc__transfer"][0].hash;
        let b = &load_manifest(dir2.path()).unwrap().tables["usdc__transfer"][0].hash;
        assert_eq!(a, b); // same rows in → same content address
    }

    /// RFC-0004 §1 path-equivalence: rows sealed *directly* (the seal-direct backfill path) and the
    /// same rows sealed *after a redb round-trip* (the hot-then-seal path) yield byte-identical
    /// segments. `seal_range` is the one shared writer, so the two backfill paths are provably the
    /// same bytes - the determinism claim the optimisation rests on.
    #[test]
    fn seal_direct_matches_seal_via_hot_store() {
        use crate::store::Store;
        let rows = vec![
            transfer(100, 0, "5"),
            transfer(100, 1, "7"),
            approval(101, 0),
            transfer(102, 0, "9"),
        ];

        // Path A - direct: seal the decoded rows as-is.
        let da = tempfile::tempdir().unwrap();
        seal_range(da.path(), &rows, 100, 102).unwrap();

        // Path B - via hot store: write to redb, read the range back, then seal.
        let db = tempfile::tempdir().unwrap();
        let store = Store::open(&db.path().join("hot.redb")).unwrap();
        for r in &rows {
            let v: Value = serde_json::from_str(r).unwrap();
            let key = Store::entity_key(
                v["block_number"].as_u64().unwrap(),
                v["log_index"].as_u64().unwrap(),
            );
            store.put_entity(&key, r).unwrap();
        }
        let readback = store.entities_in_range(100, 102).unwrap();
        seal_range(db.path(), &readback, 100, 102).unwrap();

        // Same tables, same per-table content hashes.
        let ma = load_manifest(da.path()).unwrap();
        let mb = load_manifest(db.path()).unwrap();
        assert_eq!(
            ma.tables.keys().collect::<Vec<_>>(),
            mb.tables.keys().collect::<Vec<_>>()
        );
        for (table, segs) in &ma.tables {
            assert_eq!(
                segs[0].hash, mb.tables[table][0].hash,
                "segment hash differs for {table} between direct and via-hot-store paths"
            );
        }
    }

    // ---------------------------------------------------------------------------------------
    // #1150 - a table under the floor is sealed provisionally and folded into its own next cut.
    //
    // Every test here seals through `seal_range` into a real directory and reads the manifest and
    // the files back, because the property is about what is on disk: how many files a sparse table
    // gets, whether a busy one is untouched, and whether the folded bytes are the bytes a single
    // seal would have written.
    // ---------------------------------------------------------------------------------------

    fn transfers(from_block: u64, n: usize) -> Vec<String> {
        (0..n)
            .map(|i| transfer(from_block + (i / 4) as u64, (i % 4) as u64, &i.to_string()))
            .collect()
    }

    fn only(manifest: &Manifest, table: &str) -> Segment {
        let segs = &manifest.tables[table];
        assert_eq!(
            segs.len(),
            1,
            "{table} has {} segments where exactly one was expected: {segs:?}",
            segs.len()
        );
        segs[0].clone()
    }

    fn parquet_files(dir: &Path) -> Vec<String> {
        let mut out: Vec<String> = std::fs::read_dir(dir.join(SEGMENTS_DIR))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".parquet"))
            .collect();
        out.sort();
        out
    }

    /// Megabyte rows never reach the row floor at a cut, so every fold re-read and rewrote the whole
    /// provisional file, and the fold grew until it was the memory problem the byte cut exists to end.
    #[test]
    fn a_table_of_documents_is_final_under_the_row_floor() {
        let dir = tempfile::tempdir().unwrap();
        let doc = "x".repeat(1024 * 1024);
        let rows: Vec<String> = (0..20u64)
            .map(|b| {
                format!(r#"{{"table":"t__doc","block_number":{b},"log_index":0,"doc":"{doc}"}}"#)
            })
            .collect();
        assert!(rows.len() < SEAL_TABLE_FLOOR);
        seal_range(dir.path(), &rows, 0, 19).unwrap().unwrap();
        let seg = only(&load_manifest(dir.path()).unwrap(), "t__doc");
        assert!(
            !seg.provisional,
            "20 MiB of rows is past SEAL_TABLE_BYTES_FLOOR, so the segment must be final however few \
             rows it holds"
        );
    }

    fn stamped(table: &str, block: u64, ts: u64) -> String {
        format!(
            r#"{{"table":"{table}","value":"1","block_number":{block},"block_timestamp":{ts},"tx_hash":"0xcc","log_index":0}}"#
        )
    }

    const T0: u64 = 1_790_000_000;

    /// #2084: a sparse table's tail held back the mirror for months. One that reaches the age limit
    /// in a cut of its own rows is final, and its next row starts a new segment.
    #[test]
    fn a_tail_that_reaches_the_age_limit_is_final() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[stamped("t__sparse", 10, T0)], 10, 10).unwrap();
        assert!(only(&load_manifest(dir.path()).unwrap(), "t__sparse").provisional);

        let later = T0 + SEAL_TABLE_AGE_SECS;
        seal_range(dir.path(), &[stamped("t__sparse", 900, later)], 900, 900).unwrap();
        let seg = only(&load_manifest(dir.path()).unwrap(), "t__sparse");
        assert!(
            !seg.provisional,
            "the tail's oldest row is SEAL_TABLE_AGE_SECS older than the cut, so it must be final"
        );
        assert_eq!((seg.from_block, seg.to_block, seg.rows), (10, 900, 2));

        seal_range(
            dir.path(),
            &[stamped("t__sparse", 901, later + 12)],
            901,
            901,
        )
        .unwrap();
        let segs = &load_manifest(dir.path()).unwrap().tables["t__sparse"];
        assert_eq!(segs.len(), 2, "a final tail is never folded: {segs:?}");
        assert!(segs[1].provisional);
    }

    /// A contract that has gone quiet has no rows in later cuts, so its tail is aged by the cuts of
    /// the nest's other tables, with its file untouched.
    #[test]
    fn a_quiet_tables_tail_is_aged_by_other_tables_cuts() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[stamped("t__quiet", 10, T0)], 10, 10).unwrap();
        let tail = only(&load_manifest(dir.path()).unwrap(), "t__quiet");
        let bytes = std::fs::read(dir.path().join(SEGMENTS_DIR).join(&tail.file)).unwrap();

        let later = T0 + SEAL_TABLE_AGE_SECS;
        seal_range(dir.path(), &[stamped("t__busy", 900, later)], 900, 900).unwrap();
        let now = only(&load_manifest(dir.path()).unwrap(), "t__quiet");
        assert!(
            !now.provisional,
            "the quiet table's tail reached the age limit"
        );
        assert_eq!((now.hash.as_str(), now.rows), (tail.hash.as_str(), 1));
        assert_eq!(
            std::fs::read(dir.path().join(SEGMENTS_DIR).join(&now.file)).unwrap(),
            bytes
        );
    }

    /// #2172: a provisional tail whose file the startup pass quarantined, or found gone, stops being a
    /// tail. Before, the fold and the aging pass both read it with `?`, and no table sealed again.
    #[test]
    fn a_quarantined_or_missing_provisional_tail_stops_no_later_seal() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[stamped("t__sparse", 10, T0)], 10, 10).unwrap();
        seal_range(dir.path(), &[stamped("t__quiet", 11, T0)], 11, 11).unwrap();
        let m = load_manifest(dir.path()).unwrap();
        let (sparse, quiet) = (only(&m, "t__sparse"), only(&m, "t__quiet"));
        assert!(
            sparse.provisional && quiet.provisional,
            "premise: two tails"
        );
        std::fs::write(dir.path().join(SEGMENTS_DIR).join(&sparse.file), b"rotted").unwrap();
        std::fs::remove_file(dir.path().join(SEGMENTS_DIR).join(&quiet.file)).unwrap();

        assert_eq!(verify_and_quarantine(dir.path()).unwrap(), 1);
        // The cut folds `t__sparse` and ages `t__quiet`, the two reads that used to fail.
        seal_range(dir.path(), &[stamped("t__sparse", 12, T0 + 12)], 12, 12)
            .expect("a seal after the quarantine");

        let m = load_manifest(dir.path()).unwrap();
        let sparse_now = &m.tables["t__sparse"];
        assert_eq!(
            sparse_now
                .iter()
                .map(|s| (s.hash.as_str(), s.from_block, s.to_block, s.provisional))
                .collect::<Vec<_>>(),
            vec![
                (sparse.hash.as_str(), 10, 10, false),
                (sparse_now[1].hash.as_str(), 12, 12, true)
            ],
            "the lost tail stays catalogued, so its loss stays visible, and the cut starts afresh"
        );
        assert!(!only(&m, "t__quiet").provisional);
    }

    /// #2169: the state a crash between a fold's manifest and its prune leaves.
    #[test]
    fn an_unpruned_fold_is_found_and_an_ordinary_unpruned_seal_is_not() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        assert_eq!(unpruned_fold(dir.path(), 10).unwrap(), None);
        seal_range(dir.path(), &[approval(11, 0)], 11, 11).unwrap();
        assert_eq!(
            unpruned_fold(dir.path(), 10).unwrap(),
            None,
            "a segment wholly past the watermark is served from hot until the prune"
        );
        seal_range(dir.path(), &[transfer(12, 0, "2")], 12, 12).unwrap();
        assert_eq!(unpruned_fold(dir.path(), 11).unwrap(), Some((10, 12)));

        let folded = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        std::fs::remove_file(dir.path().join(SEGMENTS_DIR).join(&folded.file)).unwrap();
        assert!(
            unpruned_fold(dir.path(), 11).is_err(),
            "a fold whose file is gone must not be finished: the prune would drop the hot copy"
        );
    }

    /// #2261: a re-seal of rows whose catalogued file is gone was skipped by hash, and the caller then
    /// pruned the hot copy. The same bytes are put back under their name instead.
    #[test]
    fn a_re_seal_beside_a_missing_file_restores_it() {
        let dir = tempfile::tempdir().unwrap();
        test_set_table_floor(dir.path(), 0);
        seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        let seg = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        let path = dir.path().join(SEGMENTS_DIR).join(&seg.file);
        std::fs::remove_file(&path).unwrap();

        seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        assert_eq!(
            only(&load_manifest(dir.path()).unwrap(), "usdc__transfer").hash,
            seg.hash
        );
        assert!(
            segment_intact(dir.path(), &seg),
            "the file was not restored"
        );
    }

    /// #2266: a shared segment corrupt in place was left alone by the restore, which skips a name
    /// that exists, and the caller then pruned hot. The re-seal's bytes are the ones the address
    /// names, so they replace it before `seal_range` returns, which is before any prune.
    #[test]
    fn a_re_seal_replaces_a_corrupt_shared_segment_before_it_returns() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(crate::runtime::DATA_DIR).join("nid0");
        std::fs::create_dir_all(&dir).unwrap();
        test_set_table_floor(&dir, 0);
        seal_range(&dir, &[transfer(10, 0, "1")], 10, 10).unwrap();
        let seg = only(&load_manifest(&dir).unwrap(), "usdc__transfer");
        let shared = root
            .path()
            .join(SEGMENTS_DIR)
            .join(format!("{}.parquet", seg.hash));
        std::fs::write(&shared, b"rotted in place").unwrap();
        assert!(
            !segment_intact(&dir, &seg),
            "premise: the shared copy is bad"
        );

        seal_range(&dir, &[transfer(10, 0, "1")], 10, 10).unwrap();
        assert!(
            segment_intact(&dir, &seg),
            "the corrupt shared file kept its bytes"
        );
        assert_eq!(
            load_manifest(&dir).unwrap().tables["usdc__transfer"].len(),
            1
        );
    }

    /// #2266: when the bad file cannot be replaced, the seal fails, so its caller keeps hot.
    #[test]
    fn a_re_seal_that_cannot_replace_a_corrupt_segment_fails() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        test_set_table_floor(dir.path(), 0);
        seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        let seg = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        let seg_dir = dir.path().join(SEGMENTS_DIR);
        std::fs::write(seg_dir.join(&seg.file), b"rotted in place").unwrap();
        std::fs::set_permissions(&seg_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let sealed = seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10);
        std::fs::set_permissions(&seg_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = format!(
            "{:#}",
            sealed.expect_err("a seal reported success beside a corrupt copy")
        );
        // The manifest lives in the same directory, so its write fails too; the restore must fail first.
        assert!(err.contains("restoring segment"), "{err}");
    }

    /// #2230: two seals of identical bytes in one process each wrote `.{file}.{pid}.tmp`. The second
    /// truncated the first's, and whichever renamed last found its temp file gone.
    #[test]
    fn two_publishes_of_one_file_in_one_process_both_install_it() {
        let root = tempfile::tempdir().unwrap();
        let store = root.path().join("segments");
        let inner = store.clone();
        let _hook = set_before_install(move || {
            publish_durable(&inner, "s.parquet", b"same").unwrap();
        });
        publish_durable(&store, "s.parquet", b"same").expect("the first publish");
        assert_eq!(std::fs::read(store.join("s.parquet")).unwrap(), b"same");
        let left: Vec<_> = std::fs::read_dir(&store)
            .unwrap()
            .flatten()
            .map(|e| e.file_name())
            .filter(|n| n != "s.parquet")
            .collect();
        assert!(left.is_empty(), "temp files left behind: {left:?}");
    }

    #[test]
    fn a_tail_younger_than_the_age_limit_stays_provisional() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[stamped("t__sparse", 10, T0)], 10, 10).unwrap();
        seal_range(dir.path(), &[stamped("t__quiet", 11, T0)], 11, 11).unwrap();
        let young = T0 + SEAL_TABLE_AGE_SECS - 1;
        seal_range(dir.path(), &[stamped("t__sparse", 900, young)], 900, 900).unwrap();
        let m = load_manifest(dir.path()).unwrap();
        assert!(
            only(&m, "t__sparse").provisional,
            "a folded tail one second short"
        );
        assert!(
            only(&m, "t__quiet").provisional,
            "an untouched tail one second short"
        );
    }

    #[test]
    fn a_table_under_the_floor_is_sealed_provisionally_and_still_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![
            transfer(10, 0, "1"),
            transfer(10, 1, "2"),
            transfer(11, 0, "3"),
        ];
        assert!(rows.len() < SEAL_TABLE_FLOOR);
        let summary = seal_range(dir.path(), &rows, 10, 11).unwrap().unwrap();
        assert_eq!((summary.tables, summary.rows), (1, 3));

        let seg = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        assert!(
            seg.provisional,
            "three rows is under SEAL_TABLE_FLOOR={SEAL_TABLE_FLOOR}, so this segment must be \
             marked for folding rather than sealed as final"
        );
        assert_eq!((seg.from_block, seg.to_block, seg.rows), (10, 11, 3));
        // Queryable exactly as a final segment is: the typed reader sees it.
        let back = read_segment_rows(&dir.path().join(SEGMENTS_DIR).join(&seg.file)).unwrap();
        assert_eq!(
            back.iter()
                .map(|r| r["block_number"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 10, 11]
        );
    }

    #[test]
    fn a_provisional_segment_folds_into_the_tables_next_cut() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        let first = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        let summary = seal_range(
            dir.path(),
            &[transfer(11, 0, "2"), transfer(12, 0, "3")],
            11,
            12,
        )
        .unwrap()
        .unwrap();

        let m = load_manifest(dir.path()).unwrap();
        let seg = only(&m, "usdc__transfer");
        assert!(seg.provisional, "still under the floor, still folding");
        assert_eq!(
            (seg.from_block, seg.to_block, seg.rows),
            (10, 12, 3),
            "the folded segment spans from the first provisional cut to this one and holds every row"
        );
        assert_ne!(seg.hash, first.hash);
        assert_eq!(
            summary.rows, 2,
            "rows sealed counts the rows that arrived, not the rows re-written: the metric would \
             otherwise double-count every fold"
        );
        assert_eq!(
            parquet_files(dir.path()),
            vec![seg.file.clone()],
            "the file the fold replaced must be gone from disk, or the fold only moved the small \
             files problem from the manifest to the directory"
        );
        let back = read_segment_rows(&dir.path().join(SEGMENTS_DIR).join(&seg.file)).unwrap();
        assert_eq!(
            back.iter()
                .map(|r| r["block_number"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
    }

    #[test]
    fn a_folded_segment_is_byte_identical_to_sealing_all_its_rows_at_once() {
        // The fold reads the provisional Parquet back and re-seals; if that round trip lost or
        // reshaped anything, the two operators - one who sealed in two cuts, one who sealed in
        // one - would hold different content addresses for the same rows.
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let first = vec![
            transfer(10, 0, "1"),
            transfer(10, 1, "2"),
            approval(10, 2),
            transfer(11, 0, "3"),
        ];
        let second = vec![approval(12, 0), transfer(12, 1, "4")];
        seal_range(a.path(), &first, 10, 11).unwrap();
        seal_range(a.path(), &second, 12, 12).unwrap();
        let all: Vec<String> = first.iter().chain(second.iter()).cloned().collect();
        seal_range(b.path(), &all, 10, 12).unwrap();

        let ma = load_manifest(a.path()).unwrap();
        let mb = load_manifest(b.path()).unwrap();
        for table in ["usdc__transfer", "usdc__approval"] {
            let folded = only(&ma, table);
            let once = only(&mb, table);
            assert_eq!(
                folded.hash, once.hash,
                "{table}: two cuts then a fold != one seal of the same rows"
            );
            assert_eq!(
                (folded.from_block, folded.to_block, folded.rows),
                (once.from_block, once.to_block, once.rows)
            );
        }
    }

    #[test]
    fn a_finalised_tail_keeps_its_file_and_is_never_folded() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        let tail = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        let bytes = std::fs::read(dir.path().join(SEGMENTS_DIR).join(&tail.file)).unwrap();

        let finalised = finalise_provisional(dir.path()).unwrap();
        assert_eq!(finalised.len(), 1);
        let now = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        assert!(!now.provisional);
        assert_eq!(
            (now.hash.as_str(), now.from_block, now.to_block, now.rows),
            (tail.hash.as_str(), 10, 10, 1)
        );
        assert_eq!(
            std::fs::read(dir.path().join(SEGMENTS_DIR).join(&now.file)).unwrap(),
            bytes
        );
        assert!(finalise_provisional(dir.path()).unwrap().is_empty());

        seal_range(dir.path(), &[transfer(11, 0, "2")], 11, 11).unwrap();
        let segs = &load_manifest(dir.path()).unwrap().tables["usdc__transfer"];
        assert_eq!(
            segs.len(),
            2,
            "the next seal must not fold a finalised tail: {segs:?}"
        );
        assert_eq!(segs[0].hash, tail.hash);
        assert!(segs[1].provisional);
    }

    #[test]
    fn crossing_the_floor_makes_the_segment_final_and_the_next_cut_starts_afresh() {
        let dir = tempfile::tempdir().unwrap();
        let half = SEAL_TABLE_FLOOR / 2;
        seal_range(dir.path(), &transfers(1_000, half), 1_000, 1_999).unwrap();
        assert!(only(&load_manifest(dir.path()).unwrap(), "usdc__transfer").provisional);

        seal_range(dir.path(), &transfers(2_000, half), 2_000, 2_999).unwrap();
        let m = load_manifest(dir.path()).unwrap();
        let sealed = only(&m, "usdc__transfer");
        assert!(
            !sealed.provisional,
            "{} rows is the floor, so the folded segment is final",
            SEAL_TABLE_FLOOR
        );
        assert_eq!(
            (sealed.from_block, sealed.to_block, sealed.rows),
            (1_000, 2_999, SEAL_TABLE_FLOOR)
        );

        // A final segment is never reopened: the next handful of rows starts a new provisional
        // beside it, which is what keeps the sealed layer append-only.
        seal_range(dir.path(), &[transfer(3_000, 0, "x")], 3_000, 3_000).unwrap();
        let m = load_manifest(dir.path()).unwrap();
        let segs = &m.tables["usdc__transfer"];
        assert_eq!(segs.len(), 2, "{segs:?}");
        assert_eq!(segs[0].hash, sealed.hash, "the final segment is untouched");
        assert!(segs[1].provisional);
        assert_eq!(
            (segs[1].from_block, segs[1].to_block, segs[1].rows),
            (3_000, 3_000, 1)
        );
        assert_eq!(parquet_files(dir.path()).len(), 2);
    }

    #[test]
    fn a_busy_table_is_sealed_exactly_as_before_whatever_its_co_tenants_do() {
        // The identity guarantee the floor must not touch: a table that clears the floor at a cut
        // gets the same file, hash and range with a sparse table beside it as without one.
        let with = tempfile::tempdir().unwrap();
        let without = tempfile::tempdir().unwrap();
        let busy = transfers(500, SEAL_TABLE_FLOOR + 7);
        let mut mixed = busy.clone();
        mixed.push(approval(600, 0));
        seal_range(with.path(), &mixed, 500, 600).unwrap();
        seal_range(without.path(), &busy, 500, 600).unwrap();

        let a = only(&load_manifest(with.path()).unwrap(), "usdc__transfer");
        let b = only(&load_manifest(without.path()).unwrap(), "usdc__transfer");
        assert!(!a.provisional && !b.provisional);
        assert_eq!(
            (a.hash, a.file, a.from_block, a.to_block, a.rows),
            (b.hash, b.file, b.from_block, b.to_block, b.rows)
        );
        assert!(
            only(&load_manifest(with.path()).unwrap(), "usdc__approval").provisional,
            "the one-row co-tenant is the one that folds"
        );
    }

    #[test]
    fn re_sealing_identical_rows_into_a_provisional_table_is_still_a_no_op() {
        // The content-addressed idempotency rule, checked before the fold: the
        // same rows again must not be folded in a second time as if they were new.
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![transfer(10, 0, "1"), transfer(10, 1, "2")];
        seal_range(dir.path(), &rows, 10, 10).unwrap();
        let before = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        let again = seal_range(dir.path(), &rows, 10, 10).unwrap().unwrap();
        assert_eq!((again.tables, again.rows), (0, 0));
        let after = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        assert_eq!((after.hash, after.rows), (before.hash, 2));
        assert_eq!(parquet_files(dir.path()).len(), 1);
    }

    /// #1631: the rows a fold just took are still hot when the process dies before the watermark.
    /// Sealing them again must not fold them in a second time, whether the folded file is still
    /// provisional or the fold is what carried it over the floor.
    #[test]
    fn re_sealing_rows_a_fold_already_took_does_not_count_them_twice() {
        let provisional = tempfile::tempdir().unwrap();
        seal_range(provisional.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        let added = vec![transfer(11, 0, "2"), transfer(12, 0, "3")];
        seal_range(provisional.path(), &added, 11, 12).unwrap();
        let folded = only(
            &load_manifest(provisional.path()).unwrap(),
            "usdc__transfer",
        );
        assert!(folded.provisional, "premise: still under the floor");
        assert_eq!(folded.rows, 3, "premise: the fold holds every row once");
        let again = seal_range(provisional.path(), &added, 11, 12)
            .unwrap()
            .unwrap();
        assert_eq!(
            (again.tables, again.rows),
            (0, 0),
            "the rows the fold already took must not be sealed again"
        );
        let after = only(
            &load_manifest(provisional.path()).unwrap(),
            "usdc__transfer",
        );
        assert_eq!((after.hash, after.rows), (folded.hash, 3));
        let back =
            read_segment_rows(&provisional.path().join(SEGMENTS_DIR).join(&after.file)).unwrap();
        assert_eq!(
            back.iter()
                .map(|r| r["block_number"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 11, 12]
        );

        let final_dir = tempfile::tempdir().unwrap();
        let half = SEAL_TABLE_FLOOR / 2;
        seal_range(final_dir.path(), &transfers(1_000, half), 1_000, 1_999).unwrap();
        let second = transfers(2_000, half);
        seal_range(final_dir.path(), &second, 2_000, 2_999).unwrap();
        let sealed = only(&load_manifest(final_dir.path()).unwrap(), "usdc__transfer");
        assert!(!sealed.provisional, "premise: the fold crossed the floor");
        let again = seal_range(final_dir.path(), &second, 2_000, 2_999)
            .unwrap()
            .unwrap();
        assert_eq!((again.tables, again.rows), (0, 0));
        let segs = &load_manifest(final_dir.path()).unwrap().tables["usdc__transfer"];
        assert_eq!(
            segs.len(),
            1,
            "a final fold must not gain a second copy of its last cut: {segs:?}"
        );
        assert_eq!(segs[0].rows, SEAL_TABLE_FLOOR);
    }

    #[test]
    fn a_fold_that_cannot_install_its_manifest_leaves_the_provisional_file_in_place() {
        // The crash window Jules named on #1153: the replacement file written, the old file
        // removed, and then the process dies before the manifest is installed. The manifest on
        // disk would name a file that is gone and the folded rows would read as missing. So the
        // old file goes only after `save_manifest` returns - and this makes `save_manifest` fail
        // in the middle of a fold, which is the only order in which that property is observable.
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(10, 0, "1")], 10, 10).unwrap();
        let first = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        let first_path = dir.path().join(SEGMENTS_DIR).join(&first.file);
        assert!(first_path.exists());

        // `save_manifest` writes `manifest.json.tmp` then renames it; a directory in the way of
        // the temp file fails the write before anything is installed.
        let tmp = dir.path().join(SEGMENTS_DIR).join("manifest.json.tmp");
        std::fs::create_dir(&tmp).unwrap();
        let err = seal_range(dir.path(), &[transfer(11, 0, "2")], 11, 11).unwrap_err();
        assert!(
            format!("{err:#}").contains("manifest"),
            "the fixture must fail at the manifest, not earlier: {err:#}"
        );
        assert!(
            first_path.exists(),
            "the provisional file was removed before the manifest that drops it was installed - \
             a crash here loses every row in it"
        );
        assert_eq!(
            only(&load_manifest(dir.path()).unwrap(), "usdc__transfer").hash,
            first.hash,
            "the on-disk manifest still names the file that is still there"
        );

        // With the way clear the same fold completes, and only then is the old file gone.
        std::fs::remove_dir(&tmp).unwrap();
        seal_range(dir.path(), &[transfer(11, 0, "2")], 11, 11).unwrap();
        let folded = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        assert_eq!(folded.rows, 2);
        assert!(!first_path.exists());
        assert_eq!(parquet_files(dir.path()), vec![folded.file]);
    }

    #[test]
    fn a_fold_in_a_shared_store_leaves_the_replaced_bytes_for_prune() {
        // RFC-0033 §11a: bytes in the shared store may be another dataset's too, so a fold writes
        // the new file and deletes nothing; the manifest stops referencing the old hash and
        // `nuthatch prune` reclaims it from that.
        let root = tempfile::tempdir().unwrap();
        let nest = root.path().join(crate::runtime::DATA_DIR).join("nid-1");
        std::fs::create_dir_all(&nest).unwrap();
        seal_range(&nest, &[transfer(10, 0, "1")], 10, 10).unwrap();
        let first = only(&load_manifest(&nest).unwrap(), "usdc__transfer");
        seal_range(&nest, &[transfer(11, 0, "2")], 11, 11).unwrap();
        let folded = only(&load_manifest(&nest).unwrap(), "usdc__transfer");
        assert_ne!(first.hash, folded.hash);
        let store = root.path().join(SEGMENTS_DIR);
        assert!(store.join(format!("{}.parquet", first.hash)).exists());
        assert!(store.join(format!("{}.parquet", folded.hash)).exists());
        assert_eq!(folded.rows, 2);
    }

    #[test]
    fn the_floor_is_measured_in_rows_a_table_holds_not_rows_a_cut_carries() {
        // A cut of SEAL_DIRECT_BATCH rows where one table has all but three of them: the busy
        // table is final at that cut, the sparse one is provisional, and stays provisional across
        // as many cuts as it takes to reach the floor - one file, not one per cut.
        let dir = tempfile::tempdir().unwrap();
        let cuts = 5u64;
        for c in 0..cuts {
            let from = 1_000 * (c + 1);
            let mut rows = transfers(from, SEAL_TABLE_FLOOR);
            rows.push(approval(from + 500, 0));
            rows.push(approval(from + 500, 1));
            rows.push(approval(from + 501, 0));
            rows.sort_by_key(|r| {
                let v: Value = serde_json::from_str(r).unwrap();
                (
                    v["block_number"].as_u64().unwrap(),
                    v["log_index"].as_u64().unwrap(),
                )
            });
            seal_range(dir.path(), &rows, from, from + 999).unwrap();
        }
        let m = load_manifest(dir.path()).unwrap();
        assert_eq!(
            m.tables["usdc__transfer"].len(),
            cuts as usize,
            "the busy table gets one final segment per cut, as it always did"
        );
        assert!(m.tables["usdc__transfer"].iter().all(|s| !s.provisional));
        let sparse = only(&m, "usdc__approval");
        assert!(sparse.provisional);
        assert_eq!(
            (sparse.from_block, sparse.to_block, sparse.rows),
            (1_000, 5_999, 3 * cuts as usize),
            "five cuts of three rows is one fifteen-row file, not five three-row files"
        );
        assert_eq!(parquet_files(dir.path()).len(), cuts as usize + 1);
    }

    #[test]
    fn a_hold_on_one_segment_does_not_hold_its_neighbour() {
        let dir = tempfile::tempdir().unwrap();
        let held = dir.path().join("a.parquet");
        let free = dir.path().join("b.parquet");
        std::fs::write(&held, b"a").unwrap();
        std::fs::write(&free, b"b").unwrap();
        let _hold = SegmentHold::acquire(&held);
        assert!(segment_held(&held));
        assert!(!segment_held(&free), "a hold on a.parquet held b.parquet");
        assert!(!remove_segment_if_unheld(&held).unwrap());
        assert!(remove_segment_if_unheld(&free).unwrap());
        assert!(held.exists() && !free.exists());
    }

    /// A lease pins the files retired after it was taken, and nothing retired before it.
    #[test]
    fn a_retired_file_waits_only_for_leases_older_than_its_retirement() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("old.parquet");
        std::fs::write(&old, b"x").unwrap();
        let before = read_lease(dir.path());
        retire(dir.path(), vec![old.clone()]);
        assert!(
            old.exists(),
            "a lease from before the retire may still be reading it"
        );
        let after = read_lease(dir.path());
        drop(before);
        assert!(
            !old.exists(),
            "a lease taken after the retire read the new manifest and cannot be reading this file"
        );
        drop(after);
    }

    /// Two datasets do not share a lease. One key for every directory would let a reader of the
    /// first pin a file the second has already replaced.
    #[test]
    fn a_lease_on_one_dataset_does_not_pin_another() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let gone = b.path().join("gone.parquet");
        std::fs::write(&gone, b"x").unwrap();
        let _lease = read_lease(a.path());
        retire(b.path(), vec![gone.clone()]);
        assert!(
            !gone.exists(),
            "a lease on {} pinned a file retired from {}",
            a.path().display(),
            b.path().display()
        );
    }

    #[test]
    fn an_unreadable_manifest_is_an_error_not_an_empty_catalogue() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(manifest_path(dir.path())).unwrap();
        assert!(load_manifest(dir.path()).is_err());
        assert!(load_manifest_with_hash(dir.path()).is_err());
        assert!(catalogue_hash(dir.path()).is_err());
    }

    #[test]
    fn check_catalogue_reports_missing_and_mismatched_segments_apart() {
        let dir = tempfile::tempdir().unwrap();
        test_set_table_floor(dir.path(), 0);
        for b in [100u64, 101, 102] {
            seal_range(dir.path(), &[transfer(b, 0, "1")], b, b).unwrap();
        }
        let clean = check_catalogue(dir.path()).unwrap();
        assert!(clean.ok());
        assert_eq!(clean.manifest_version, MANIFEST_VERSION);
        assert_eq!(clean.segments, 3);

        let segs = load_manifest(dir.path()).unwrap().tables["usdc__transfer"].clone();
        std::fs::remove_file(segment_path(dir.path(), &segs[0].file, &segs[0].hash)).unwrap();
        let missing = check_catalogue(dir.path()).unwrap();
        assert_eq!(missing.missing, vec![segs[0].file.clone()]);
        assert!(missing.hash_mismatch.is_empty());
        assert!(!missing.ok(), "a missing segment alone must fail the check");

        std::fs::write(
            segment_path(dir.path(), &segs[1].file, &segs[1].hash),
            b"not parquet",
        )
        .unwrap();
        let both = check_catalogue(dir.path()).unwrap();
        assert_eq!(both.missing, vec![segs[0].file.clone()]);
        assert_eq!(both.hash_mismatch, vec![segs[1].file.clone()]);
        assert_eq!(both.segments, 3);
    }

    /// `nuthatch seed` (#1952): the installed catalogue is the publisher's, byte for byte, in a nest
    /// that had no `segments/` before.
    #[test]
    fn an_installed_catalogue_reads_back_as_the_one_it_was_given() {
        let publisher = tempfile::tempdir().unwrap();
        seal_range(publisher.path(), &[transfer(100, 0, "1")], 100, 100).unwrap();
        let catalogue = load_manifest(publisher.path()).unwrap();
        let fresh = tempfile::tempdir().unwrap();
        let rx = manifest_changes(fresh.path());

        install_manifest(fresh.path(), &catalogue).unwrap();

        let installed = load_manifest(fresh.path()).unwrap();
        assert_eq!(
            installed.tables["usdc__transfer"][0].hash,
            catalogue.tables["usdc__transfer"][0].hash
        );
        assert_eq!(
            catalogue_hash(fresh.path()).unwrap(),
            catalogue_hash(publisher.path()).unwrap()
        );
        assert!(fresh.path().join(SEGMENTS_DIR).is_dir());
        assert_eq!(
            *rx.borrow(),
            1,
            "a mirror waiting on this nest was not woken"
        );
    }

    #[test]
    fn every_installed_manifest_bumps_the_change_count() {
        let dir = tempfile::tempdir().unwrap();
        test_set_table_floor(dir.path(), 0);
        let rx = manifest_changes(dir.path());
        let start = *rx.borrow();
        seal_range(dir.path(), &[transfer(100, 0, "1")], 100, 100).unwrap();
        seal_range(dir.path(), &[transfer(101, 0, "2")], 101, 101).unwrap();
        assert_eq!(*rx.borrow(), start + 2);
    }

    #[test]
    fn rows_given_out_of_order_seal_to_the_same_sorted_bytes() {
        let sorted = [
            transfer(10, 0, "1"),
            transfer(10, 1, "2"),
            transfer(11, 0, "3"),
        ];
        let mut shuffled = sorted.clone();
        shuffled.reverse();
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        seal_range(a.path(), &sorted, 10, 11).unwrap();
        seal_range(b.path(), &shuffled, 10, 11).unwrap();
        let sa = only(&load_manifest(a.path()).unwrap(), "usdc__transfer");
        let sb = only(&load_manifest(b.path()).unwrap(), "usdc__transfer");
        assert_eq!(sa.hash, sb.hash, "input order changed the sealed bytes");
        let rows = read_segment_rows(&segment_path(b.path(), &sb.file, &sb.hash)).unwrap();
        let order: Vec<(u64, u64)> = rows
            .iter()
            .map(|r| {
                (
                    r["block_number"].as_u64().unwrap(),
                    r["log_index"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(order, vec![(10, 0), (10, 1), (11, 0)]);
    }

    /// Named rather than derived from `bloom_column`, which would compare the writer with itself.
    #[test]
    fn blooms_go_on_address_and_hash_columns_only() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let dir = tempfile::tempdir().unwrap();
        let row = r#"{"table":"p__swap","address":"0x1","pool_address":"0x2","sender":"0x3","tx_hash":"0x4","amount":"5","block_number":1,"log_index":0}"#;
        seal_range(dir.path(), &[row.to_string()], 1, 1).unwrap();
        let seg = only(&load_manifest(dir.path()).unwrap(), "p__swap");
        let path = segment_path(dir.path(), &seg.file, &seg.hash);
        let reader = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        let mut bloomed: Vec<String> = reader
            .metadata()
            .row_group(0)
            .columns()
            .iter()
            .filter(|c| c.bloom_filter_offset().is_some())
            .map(|c| c.column_path().string())
            .collect();
        bloomed.sort();
        assert_eq!(bloomed, ["address", "pool_address", "tx_hash"]);
    }

    #[test]
    fn a_few_megabytes_of_rows_under_the_row_floor_stay_provisional() {
        let dir = tempfile::tempdir().unwrap();
        let doc = "x".repeat(512 * 1024);
        let rows: Vec<String> = (0..4u64)
            .map(|b| {
                format!(r#"{{"table":"t__doc","block_number":{b},"log_index":0,"doc":"{doc}"}}"#)
            })
            .collect();
        seal_range(dir.path(), &rows, 0, 3).unwrap();
        assert!(
            only(&load_manifest(dir.path()).unwrap(), "t__doc").provisional,
            "2 MiB is well under SEAL_TABLE_BYTES_FLOOR"
        );
    }

    /// The byte floor is "under", not "at or under": a table whose row JSON is exactly
    /// SEAL_TABLE_BYTES_FLOOR is final, one byte less is provisional.
    #[test]
    fn the_byte_floor_is_exclusive() {
        let row = |pad: usize| {
            format!(
                r#"{{"table":"t__doc","block_number":0,"log_index":0,"doc":"{}"}}"#,
                "x".repeat(pad)
            )
        };
        let measured = |s: &str| serde_json::from_str::<Value>(s).unwrap().to_string().len();
        let pad = SEAL_TABLE_BYTES_FLOOR - measured(&row(0));
        let at = row(pad);
        assert_eq!(measured(&at), SEAL_TABLE_BYTES_FLOOR);

        for (row, provisional) in [(at, false), (row(pad - 1), true)] {
            let dir = tempfile::tempdir().unwrap();
            seal_range(dir.path(), &[row], 0, 0).unwrap().unwrap();
            assert_eq!(
                only(&load_manifest(dir.path()).unwrap(), "t__doc").provisional,
                provisional
            );
        }
    }

    #[test]
    fn removing_a_segment_already_gone_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!remove_segment_if_unheld(&dir.path().join("gone.parquet")).unwrap());
    }

    #[test]
    fn a_segment_that_cannot_be_removed_is_an_error_not_already_gone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.parquet");
        std::fs::create_dir(&path).unwrap();
        assert!(remove_segment_if_unheld(&path).is_err());
    }

    /// A segment path that exists but cannot be read is quarantined; only a missing one is skipped.
    #[test]
    fn an_unreadable_segment_is_quarantined_not_skipped_as_missing() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100).unwrap();
        let seg = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        let path = segment_path(dir.path(), &seg.file, &seg.hash);
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        assert_eq!(verify_and_quarantine(dir.path()).unwrap(), 1);
        assert!(dir.path().join("quarantine").join(&seg.file).exists());
    }

    /// #1971: a missing segment is skipped before the quarantine arm, so `quarantine/` is never made.
    /// The count alone cannot see this: the arm's rename fails on the absent file and counts nothing.
    #[test]
    fn a_missing_segment_is_skipped_without_making_a_quarantine() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100).unwrap();
        let seg = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        std::fs::remove_file(segment_path(dir.path(), &seg.file, &seg.hash)).unwrap();

        assert_eq!(verify_and_quarantine(dir.path()).unwrap(), 0);
        assert!(
            !dir.path().join("quarantine").exists(),
            "a missing segment took the unreadable arm"
        );
    }

    #[test]
    fn check_catalogue_reports_an_unreadable_segment_as_a_mismatch_not_missing() {
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &[transfer(100, 0, "5")], 100, 100).unwrap();
        let seg = only(&load_manifest(dir.path()).unwrap(), "usdc__transfer");
        let path = segment_path(dir.path(), &seg.file, &seg.hash);
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        let check = check_catalogue(dir.path()).unwrap();
        assert_eq!(check.hash_mismatch, vec![seg.file.clone()]);
        assert!(check.missing.is_empty());
    }

    /// #1632: the first seal into a runtime creates the shared store, and that directory's own entry
    /// in its parent has to be durable before a manifest names a file inside it.
    #[test]
    fn creating_the_shared_store_syncs_its_parent_before_the_manifest() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(crate::runtime::DATA_DIR).join("nid0");
        std::fs::create_dir_all(&dir).unwrap();
        let trace = capture_durable(|| {
            seal_range(&dir, &[transfer(10, 0, "1")], 10, 10).unwrap();
        });
        let parent_at = trace
            .iter()
            .position(|e| matches!(e, Durable::ParentDir(p) if p == root.path()))
            .unwrap_or_else(|| panic!("the store's parent was not synced. trace: {trace:?}"));
        let manifest_dir = dir.join(SEGMENTS_DIR);
        let manifest_at = trace
            .iter()
            .rposition(|e| matches!(e, Durable::ManifestDir(p) if p == &manifest_dir))
            .unwrap();
        assert!(parent_at < manifest_at, "trace: {trace:?}");
    }

    /// RFC-0065 S2: the segment bytes sealing writes, pinned. Moving the writer into `nuthatch-seal`
    /// must leave every hash here unchanged; a parquet or arrow bump that moves them is a re-seal.
    #[test]
    fn sealed_bytes_are_pinned() {
        let rows = vec![
            transfer(10, 1, "2"),
            transfer(10, 0, "1"),
            approval(10, 2),
            r#"{"table":"usdc__transfer","from":"0xaaaa","to":"0xbbbb","value":"7","block_number":10,"tx_hash":"0xcc","log_index":1}"#.to_string(),
            r#"{"table":"mixed","block_number":11,"log_index":0,"_seq":3,"block_timestamp":1767225611,"pool_address":"0x01","parent_hash":"0x02","amount":12,"flag":true}"#.to_string(),
            r#"{"table":"mixed","block_number":11,"log_index":1,"_seq":4,"block_timestamp":1767225611,"pool_address":"0x03","note":null}"#.to_string(),
        ];
        let dir = tempfile::tempdir().unwrap();
        seal_range(dir.path(), &rows, 10, 11).unwrap();
        seal_range(dir.path(), &[transfer(12, 0, "9")], 12, 12).unwrap();
        let m = load_manifest(dir.path()).unwrap();
        let got: Vec<(&str, &str)> = ["mixed", "usdc__approval", "usdc__transfer"]
            .into_iter()
            .map(|t| (t, only(&m, t).hash.leak() as &str))
            .collect();
        assert_eq!(
            got,
            vec![
                (
                    "mixed",
                    "c2b562dc24a46706f9d66711586a92fe0baaa3a34b9d30cc4cfb68a86e4bf76d"
                ),
                (
                    "usdc__approval",
                    "f60fdcd1eca002cd2a665a4a222bedb7c657ec45662b7720101b5076cff17f2f"
                ),
                (
                    "usdc__transfer",
                    "057c0549505745d150c4426b9170825999b573f042c88912f130aac1bfccdc9f"
                )
            ]
        );
    }
}
