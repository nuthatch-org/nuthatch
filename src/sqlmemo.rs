//! A deterministic memo for the analytical surface (#1186).
//!
//! The dashboard views a nest serves are whole-history folds recomputed on every request: after
//! #1183 the Lodestar nest still spent 89 s answering its 47 distinct statements, 42 s of it in
//! four. Those statements repeat, and between two repeats nothing they read has usually changed -
//! a cursor at a five-minute poll interval commits a handful of times an hour.
//!
//! This is not a TTL cache and never serves anything stale. An entry is keyed on **every input the
//! answer depends on**: the nest, the statement and its row cap, the sealed segments it is served,
//! the generation of the hot rows, each maintained entity's watermark, and the content of the
//! authored files (`nuthatch.toml`, `views/*.sql`, `labels/*.json`). Same key, same rows, by
//! construction; a committed row, an admitted segment or an edited view changes the key and the next
//! request computes. Determinism in the core is what makes this admissible: the answer is a
//! function of its inputs, so remembering it is remembering a fact.
//!
//! Bounded by bytes (`NUTHATCH_SQL_MEMO_BYTES`, default 64 MiB, `0` disables), evicted least
//! recently used, process-local, and cleared by a restart. A degraded answer - a table reduced by an
//! unreadable segment, or a tip that would not scan - is never remembered: it describes a fault, not
//! the data, and the next request should look again.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, LazyLock, Mutex};

use sha2::{Digest, Sha256};

use crate::analytics::QueryOutput;

/// Default ceiling on remembered rows, in serialized bytes.
pub const DEFAULT_MAX_BYTES: usize = 64 << 20;
/// Environment override for the ceiling; `0` turns the memo off.
pub const ENV_MAX_BYTES: &str = "NUTHATCH_SQL_MEMO_BYTES";
/// One entry may not take more than this share of the ceiling, or a single wide answer would evict
/// everything else for the benefit of one caller.
const MAX_ENTRY_SHARE: usize = 4;

/// SQL functions whose value is not a function of the indexed state, so a statement calling one
/// is not a fact about the nest and is never remembered (Jules on #1189). Matched on identifiers, so
/// `random_walks` the column is not `random` the function; a column merely *named* `now` loses its
/// statement a memo hit and nothing else.
const VOLATILE: &[&str] = &[
    "random",
    "setseed",
    "uuid",
    "gen_random_uuid",
    "uuidv4",
    "uuidv7",
    "now",
    "today",
    "current_timestamp",
    "current_date",
    "current_time",
    "current_localtime",
    "current_localtimestamp",
    "localtime",
    "localtimestamp",
    "transaction_timestamp",
    "get_current_timestamp",
    "get_current_time",
];

/// Whether a statement's answer is a function of the indexed state alone. `false` for one that
/// names a volatile function anywhere in its text; such a statement computes every time and is
/// never remembered. Conservative on purpose: a false `false` costs a hit, a false `true` would
/// cost the guarantee.
pub fn is_deterministic(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    let mut ident = String::new();
    let mut idents = Vec::new();
    for c in lower.chars().chain(std::iter::once(' ')) {
        if c.is_ascii_alphanumeric() || c == '_' {
            ident.push(c);
        } else if !ident.is_empty() {
            idents.push(std::mem::take(&mut ident));
        }
    }
    !idents.iter().any(|i| VOLATILE.contains(&i.as_str()))
}

/// The identity of an answer: a hash over every input it depends on.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Key([u8; 32]);

/// Everything a `/sql` answer is a function of. Building one is cheap - the file stamps are the
/// hashes `analytics::cache_inputs` already computes per request - and none of it needs a permit.
/// A stamp of every sealed segment behind `tables` (or behind every table, when the statement's
/// reach is unknown): path, length, and modification time in nanoseconds.
///
/// **Why the cold side needs its own check at all.** The rest of the key describes state the node
/// itself moves - a commit, a seal, an edited view. A sealed segment is immutable by construction, so
/// it is not in that set; but a file on a disk can still change without the node doing anything, and
/// when one does, the answer over it changes. Without this, a segment scribbled over under a running
/// node would be served from the memo as the healthy answer it used to be, because every input the
/// node knows about is unchanged (`e2e_solo.rs`, the corrupt-segment-over-HTTP case).
///
/// A `stat` per segment on a hit, not a hash: hashing 645 MB to save an 8-second query would defeat
/// the point, and this is a different fault from the one `content_stamp` exists for. That one is
/// **the node rewriting its own small files in rapid succession**, where the mtime clock's ~3 ms
/// granularity loses same-tick edits (#840). This one is a disk fault or an operator's half-finished
/// restore, arbitrarily later than the seal that wrote the file. What remains undetectable is a
/// same-length rewrite landing inside the same timestamp tick as the write before it, which for a
/// rotting sector is not a case that occurs; the periodic integrity sweep is what covers the rest.
///
/// Returned as a digest: an entry kept the list itself, a path per segment, which on a nest of
/// thousands of segments outweighed the rows it was remembering (#2072).
pub fn segment_stamps(dir: &Path, tables: Option<&std::collections::BTreeSet<String>>) -> [u8; 32] {
    let Ok(manifest) = crate::seal::load_manifest(dir) else {
        return Sha256::digest([]).into();
    };
    let mut out = Vec::new();
    for (table, segs) in &manifest.tables {
        if tables.is_some_and(|t| !t.contains(&table.to_ascii_lowercase())) {
            continue;
        }
        for seg in segs {
            let path = crate::seal::segment_path(dir, &seg.file, &seg.hash);
            let (len, mtime) = match std::fs::metadata(&path) {
                // A file that is gone stamps as absent rather than being skipped: a quarantined
                // segment is a change to the cold side, and the answer over it is different.
                Err(_) => (u64::MAX, i64::MIN),
                Ok(m) => (
                    m.len(),
                    m.modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                ),
            };
            out.push((path, len, mtime));
        }
    }
    out.sort();
    let mut h = Sha256::new();
    for (path, len, mtime) in &out {
        let path = path.as_os_str().as_encoded_bytes();
        h.update((path.len() as u64).to_le_bytes());
        h.update(path);
        h.update(len.to_le_bytes());
        h.update(mtime.to_le_bytes());
    }
    h.finalize().into()
}

/// A digest of the segments a statement served at `sealed_through` reads from: every manifest
/// segment at or below it, by table, file and content hash. `None` when the manifest will not load.
///
/// The watermark itself is not an input. The cold side is these segments and the hot side is every
/// hot row above the watermark (COR-1), so a watermark that moves without a segment entering this set
/// moves past no row - a row it passed would have had to be sealed into one. Where rows are sealed
/// straight to segments with nothing hot (`--seal-direct`), the watermark admitting a written segment
/// is exactly a change to this set.
pub fn sealed_segments(dir: &Path, sealed_through: u64) -> Option<String> {
    let manifest = crate::seal::load_manifest(dir).ok()?;
    let mut h = Sha256::new();
    for (table, segs) in &manifest.tables {
        for seg in segs.iter().filter(|s| s.to_block <= sealed_through) {
            for part in [table.as_str(), seg.file.as_str(), seg.hash.as_str()] {
                h.update((part.len() as u64).to_le_bytes());
                h.update(part.as_bytes());
            }
        }
    }
    Some(hex::encode(h.finalize()))
}

pub struct Inputs<'a> {
    pub dir: &'a Path,
    pub sql: &'a str,
    pub max_rows: usize,
    /// [`sealed_segments`] at the served watermark, not the watermark: on a chain that finalises
    /// every poll the watermark moves while the segments a statement can read stay the same.
    pub sealed: &'a str,
    /// [`crate::store::HotStore::rows_generation`].
    pub rows_generation: u64,
    pub entity_watermarks: &'a crate::entity_view::Watermarks,
    pub files: &'a BTreeMap<PathBuf, String>,
}

impl Inputs<'_> {
    pub fn key(&self) -> Key {
        let mut h = Sha256::new();
        // Length-prefixed so no two field layouts can collide by concatenation.
        let mut field = |bytes: &[u8]| {
            h.update((bytes.len() as u64).to_le_bytes());
            h.update(bytes);
        };
        field(self.dir.to_string_lossy().as_bytes());
        field(self.sql.as_bytes());
        field(&(self.max_rows as u64).to_le_bytes());
        field(self.sealed.as_bytes());
        field(&self.rows_generation.to_le_bytes());
        for (name, applied) in self.entity_watermarks {
            field(name.as_bytes());
            field(&applied.through.to_le_bytes());
            // An appended offchain snapshot changes the rows without moving the block (#1437).
            for (table, mark) in &applied.offchain {
                field(table.as_bytes());
                field(mark.digest.as_bytes());
            }
        }
        for (path, stamp) in self.files {
            field(path.to_string_lossy().as_bytes());
            field(stamp.as_bytes());
        }
        Key(h.finalize().into())
    }
}

/// A remembered answer: the output and the entity watermarks it was answered from, which ride out
/// in the provenance exactly as they did the first time. The head it cites is read on the hit.
pub struct Entry {
    /// The answer without its rows, which are kept serialized in `rows` (#2072).
    pub out: QueryOutput,
    pub rows: Box<serde_json::value::RawValue>,
    pub count: usize,
    pub watermarks: crate::entity_view::Watermarks,
    /// The tables this answer read, and a stamp of every sealed segment behind them at the moment it
    /// was computed - see [`segment_stamps`]. Re-taken on a hit; any difference recomputes.
    pub tables: Option<std::collections::BTreeSet<String>>,
    pub segments: [u8; 32],
    bytes: usize,
}

struct State {
    entries: HashMap<Key, (Arc<Entry>, u64)>,
    bytes: usize,
    tick: u64,
}

/// One memo: the process has one, and a test may build its own so eviction can be exercised against
/// a ceiling of its choosing without touching shared state.
pub struct Memo {
    state: Mutex<State>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Default for Memo {
    fn default() -> Self {
        Self::new()
    }
}

impl Memo {
    pub fn new() -> Self {
        Memo {
            state: Mutex::new(State {
                entries: HashMap::new(),
                bytes: 0,
                tick: 0,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The remembered answer for `key`, if any. Counted as a hit or a miss either way.
    pub fn get(&self, key: &Key, cap: usize) -> Option<Arc<Entry>> {
        if cap == 0 {
            return None;
        }
        let mut st = self.state();
        st.tick += 1;
        let tick = st.tick;
        match st.entries.get_mut(key) {
            Some((entry, used)) => {
                *used = tick;
                self.hits.fetch_add(1, Relaxed);
                Some(Arc::clone(entry))
            }
            None => {
                self.misses.fetch_add(1, Relaxed);
                None
            }
        }
    }

    /// Remember `out` under `key`. Returns whether it was kept: a degraded or tip-less answer is not,
    /// and neither is one larger than a quarter of the ceiling. Evicts least recently used entries
    /// until the total fits.
    pub fn put(
        &self,
        key: Key,
        out: &QueryOutput,
        watermarks: &crate::entity_view::Watermarks,
        segments: [u8; 32],
        cap: usize,
    ) -> bool {
        if cap == 0 || out.degraded() || out.tip_unavailable {
            return false;
        }
        let Ok(rows) = serde_json::value::to_raw_value(&out.rows) else {
            return false;
        };
        let bytes = size_of(out, &rows);
        if bytes > cap / MAX_ENTRY_SHARE {
            return false;
        }
        let mut st = self.state();
        st.tick += 1;
        let tick = st.tick;
        if let Some((old, _)) = st.entries.remove(&key) {
            st.bytes -= old.bytes;
        }
        while st.bytes + bytes > cap {
            let Some(victim) = st
                .entries
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(k, _)| *k)
            else {
                break;
            };
            if let Some((gone, _)) = st.entries.remove(&victim) {
                st.bytes -= gone.bytes;
            }
        }
        st.bytes += bytes;
        st.entries.insert(
            key,
            (
                Arc::new(Entry {
                    out: out.without_rows(),
                    rows,
                    count: out.rows.len(),
                    watermarks: watermarks.clone(),
                    tables: out.referenced_tables.clone(),
                    segments,
                    bytes,
                }),
                tick,
            ),
        );
        true
    }

    pub fn bytes(&self) -> usize {
        self.state().bytes
    }
    pub fn entries(&self) -> usize {
        self.state().entries.len()
    }
    /// Whether `key` is remembered, without counting a hit or a miss.
    #[cfg(test)]
    pub fn contains(&self, key: &Key) -> bool {
        self.state().entries.contains_key(key)
    }
    pub fn hits(&self) -> u64 {
        self.hits.load(Relaxed)
    }
    pub fn misses(&self) -> u64 {
        self.misses.load(Relaxed)
    }
}

/// The process's memo, behind the `/sql` handler.
static GLOBAL: LazyLock<Memo> = LazyLock::new(Memo::new);

/// The byte ceiling in force: `NUTHATCH_SQL_MEMO_BYTES` if set and parseable, else the default.
pub fn max_bytes() -> usize {
    std::env::var(ENV_MAX_BYTES)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_BYTES)
}

pub fn get(key: &Key) -> Option<Arc<Entry>> {
    GLOBAL.get(key, max_bytes())
}
pub fn put(
    key: Key,
    out: &QueryOutput,
    watermarks: &crate::entity_view::Watermarks,
    segments: [u8; 32],
) -> bool {
    GLOBAL.put(key, out, watermarks, segments, max_bytes())
}
pub fn bytes() -> usize {
    GLOBAL.bytes()
}
pub fn entries() -> usize {
    GLOBAL.entries()
}
#[cfg(test)]
pub fn contains(key: &Key) -> bool {
    GLOBAL.contains(key)
}
pub fn hits() -> u64 {
    GLOBAL.hits()
}
pub fn misses() -> u64 {
    GLOBAL.misses()
}

/// What an entry holds: the serialized rows, which is also what the response carries, and the
/// column names beside them.
fn size_of(out: &QueryOutput, rows: &serde_json::value::RawValue) -> usize {
    rows.get().len() + out.columns.iter().map(String::len).sum::<usize>() + 64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The served set, not the watermark: moving past no segment keeps the digest, admitting one
    /// written above the old watermark changes it, and so does a segment replaced under one name.
    #[test]
    fn sealed_segments_moves_with_the_served_set_not_the_watermark() {
        let tmp = tempfile::tempdir().unwrap();
        let write = |segs: serde_json::Value| {
            let dir = tmp.path().join(crate::seal::SEGMENTS_DIR);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join(crate::seal::MANIFEST_FILE),
                json!({ "tables": { "t": segs } }).to_string(),
            )
            .unwrap();
        };
        let seg = |hash: &str, from: u64, to: u64| json!({"hash": hash, "from_block": from, "to_block": to, "rows": 1, "file": format!("t-{hash}.parquet")});
        write(json!([seg("a", 1, 10), seg("b", 11, 20)]));
        let at = |s: u64| sealed_segments(tmp.path(), s).expect("manifest loads");
        assert_eq!(at(10), at(15), "a watermark that passes no segment");
        assert_ne!(at(15), at(20), "the watermark admitting a written segment");
        let before = at(20);
        write(json!([seg("a", 1, 10), seg("c", 11, 20)]));
        assert_ne!(before, at(20), "a segment replaced over the same range");
    }

    fn inputs<'a>(
        dir: &'a Path,
        sql: &'a str,
        generation: u64,
        wm: &'a crate::entity_view::Watermarks,
        files: &'a BTreeMap<PathBuf, String>,
    ) -> Inputs<'a> {
        Inputs {
            dir,
            sql,
            max_rows: 100,
            sealed: "s10",
            rows_generation: generation,
            entity_watermarks: wm,
            files,
        }
    }

    fn held(out: &QueryOutput) -> usize {
        size_of(out, &serde_json::value::to_raw_value(&out.rows).unwrap())
    }

    fn rows(n: usize) -> QueryOutput {
        QueryOutput {
            rows: (0..n)
                .map(|i| json!({"n": i, "pad": "x".repeat(100)}))
                .collect(),
            ..Default::default()
        }
    }

    /// Every input is part of the identity: change any one and the key differs, so no answer can be
    /// served for a state it was not computed from.
    #[test]
    fn every_input_changes_the_key() {
        let d = std::path::PathBuf::from("/n");
        let at = |through: u64, snapshots: &[&str]| -> crate::entity_view::Watermarks {
            let mut applied = crate::entity_view::Applied {
                through,
                ..Default::default()
            };
            if !snapshots.is_empty() {
                let version: Vec<String> = snapshots.iter().map(|s| s.to_string()).collect();
                applied.offchain.insert(
                    "prices".into(),
                    crate::entity_view::OffchainMark::of(&version),
                );
            }
            [("e".to_string(), applied)].into_iter().collect()
        };
        let wm = at(5, &[]);
        let files: BTreeMap<PathBuf, String> =
            [(PathBuf::from("/n/views/a.sql"), "h1".to_string())]
                .into_iter()
                .collect();
        let base = inputs(&d, "SELECT 1", 3, &wm, &files).key();
        assert_eq!(
            base,
            inputs(&d, "SELECT 1", 3, &wm, &files).key(),
            "deterministic"
        );
        assert_ne!(
            base,
            inputs(&d, "SELECT 2", 3, &wm, &files).key(),
            "statement"
        );
        assert_ne!(
            base,
            inputs(&d, "SELECT 1", 4, &wm, &files).key(),
            "rows generation"
        );
        let mut i = inputs(&d, "SELECT 1", 3, &wm, &files);
        i.sealed = "s11";
        assert_ne!(base, i.key(), "sealed segments");
        let mut i = inputs(&d, "SELECT 1", 3, &wm, &files);
        i.max_rows = 101;
        assert_ne!(base, i.key(), "row cap");
        let wm2 = at(6, &[]);
        assert_ne!(
            base,
            inputs(&d, "SELECT 1", 3, &wm2, &files).key(),
            "entity watermark"
        );
        // #1437: offchain snapshots change the rows without moving the block.
        let one = inputs(&d, "SELECT 1", 3, &at(5, &["a"]), &files).key();
        assert_ne!(base, one, "an applied snapshot");
        let two = inputs(&d, "SELECT 1", 3, &at(5, &["a", "b"]), &files).key();
        assert_ne!(one, two, "an appended snapshot");
        assert_ne!(
            two,
            inputs(&d, "SELECT 1", 3, &at(5, &["c", "b"]), &files).key(),
            "a replaced snapshot, with the same count and latest"
        );
        let files2: BTreeMap<PathBuf, String> =
            [(PathBuf::from("/n/views/a.sql"), "h2".to_string())]
                .into_iter()
                .collect();
        assert_ne!(
            base,
            inputs(&d, "SELECT 1", 3, &wm, &files2).key(),
            "authored file content"
        );
        let d2 = std::path::PathBuf::from("/m");
        assert_ne!(base, inputs(&d2, "SELECT 1", 3, &wm, &files).key(), "nest");
    }

    /// `random()` is not a fact about the nest. Identifier-matched, so the column `random_walks` is
    /// still a deterministic statement and the bare keyword `current_timestamp` is still caught.
    #[test]
    fn a_statement_calling_a_volatile_function_is_not_deterministic() {
        assert!(!is_deterministic("SELECT random()"));
        assert!(!is_deterministic("SELECT RANDOM() * 2 AS r"));
        assert!(!is_deterministic("SELECT current_timestamp"));
        assert!(!is_deterministic(
            "select id from t where ts < now() - interval 1 day"
        ));
        assert!(!is_deterministic("SELECT gen_random_uuid()"));
        assert!(is_deterministic("SELECT random_walks FROM t"));
        assert!(is_deterministic(
            "SELECT count(*) AS n FROM t WHERE block_number > 5"
        ));
        assert!(is_deterministic(
            "SELECT id, CAST(staked_tokens AS VARCHAR) AS staked_tokens FROM lodestar_indexers"
        ));
    }

    /// A degraded answer describes a fault, not the data, and is not remembered.
    #[test]
    fn a_degraded_or_tipless_answer_is_not_remembered() {
        let wm = BTreeMap::new();
        let mut out = rows(1);
        out.degraded_tables.insert("t".into());
        assert!(!put(Key([1; 32]), &out, &wm, [0; 32]));
        let mut out = rows(1);
        out.tip_unavailable = true;
        assert!(!put(Key([2; 32]), &out, &wm, [0; 32]));
        assert!(get(&Key([1; 32])).is_none());
        assert!(get(&Key([2; 32])).is_none());
    }

    /// The ceiling holds and the least recently used entry goes first. A memo of this test's own, so
    /// nothing the process-wide one holds can lean on the arithmetic.
    #[test]
    fn the_ceiling_evicts_least_recently_used() {
        let m = Memo::new();
        let wm = BTreeMap::new();
        let k = |n: u8| Key([n; 32]);
        let one = held(&rows(10));
        // Room for four entries and a little, never five; each is under a quarter of it.
        let cap = one * 4 + one / 2;
        for n in 1..=4 {
            assert!(m.put(k(n), &rows(10), &wm, [0; 32], cap));
        }
        assert_eq!(m.entries(), 4);
        assert!(
            m.get(&k(1), cap).is_some(),
            "touching 1 makes 2 the least recently used"
        );
        assert!(m.put(k(5), &rows(10), &wm, [0; 32], cap));
        assert!(m.bytes() <= cap, "the ceiling holds");
        assert_eq!(m.entries(), 4, "exactly one entry made room");
        assert!(
            m.get(&k(2), cap).is_none(),
            "the least recently used entry is the one evicted"
        );
        for n in [1, 3, 4, 5] {
            assert!(m.get(&k(n), cap).is_some(), "entry {n} survives");
        }
        assert_eq!((m.hits(), m.misses()), (5, 1));
    }

    /// One answer may not take the whole memo: an entry over a quarter of the ceiling is declined
    /// rather than evicting everything else for one caller. And a ceiling of zero is the off switch.
    #[test]
    fn an_answer_wider_than_a_quarter_of_the_ceiling_is_declined() {
        let m = Memo::new();
        let wm = BTreeMap::new();
        let out = rows(10);
        let cap = held(&out) * MAX_ENTRY_SHARE - 1;
        assert!(!m.put(Key([0xb1; 32]), &out, &wm, [0; 32], cap));
        assert!(m.get(&Key([0xb1; 32]), cap).is_none());
        assert!(m.put(Key([0xb2; 32]), &out, &wm, [0; 32], cap + 1));
        assert!(
            !m.put(Key([0xb3; 32]), &out, &wm, [0; 32], 0),
            "zero disables"
        );
        assert!(
            m.get(&Key([0xb2; 32]), 0).is_none(),
            "zero disables reads too"
        );
    }
}
