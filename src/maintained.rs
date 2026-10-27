//! Maintained views (RFC-0062): a declared view answered from a stored copy of its own request-time
//! evaluation.
//!
//! An author lists views in `maintained.toml`. A request that reaches one computes the view's
//! **identity**, a hash of every input its evaluation reads: the binary and engine, the authored
//! files, the sealed segments and hot rows of the tables its closure reaches, and the schema those
//! tables are bound with. If `maintained/<view>/<identity>.parquet` exists the view is bound to it
//! and its body is never expanded; otherwise the request answers from the definition exactly as it
//! would without this module.
//!
//! Builds are eager (S2): the indexer calls [`changed`] after each commit and seal at the tip,
//! and the cursor's builder reads the nest's current inputs through its [`Feed`] and builds each
//! view whose copy at them is missing. Latest wins: a queued build is dropped once newer inputs
//! arrive, before it runs. A process with no feed builds what a request found missing.
//!
//! A copy is never served at inputs it was not computed from, so it cannot be stale: the same
//! argument the answer memo (#1186) rests on, one level up. The request-time view is the oracle.
//!
//! A nest without `maintained.toml` holds nothing here, and every request takes today's path.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::analytics::HotRows;

/// The declaration, at the nest root. Authored, so in the NID, and outside the data identity.
pub const DECLARATION_FILE: &str = "maintained.toml";
/// Where copies live in a dataset, beside `segments/` and never in it.
pub const COPIES_DIR: &str = "maintained";
/// Copies kept per view besides the current one (RFC-0062 §3.5, open question 5).
pub const DEFAULT_RECENT: usize = 4;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Declaration {
    /// Older copies to keep per view, beside the one just built.
    #[serde(default)]
    pub recent: Option<usize>,
    #[serde(default)]
    pub view: Vec<DeclaredView>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeclaredView {
    pub name: String,
}

/// `maintained.toml` in `dir`, or `None` when the nest has none.
pub fn read(dir: &Path) -> Result<Option<Declaration>> {
    let path = dir.join(DECLARATION_FILE);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let decl: Declaration =
        toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
    crate::config::warn_unknown::<Declaration>(DECLARATION_FILE, &raw);
    Ok(Some(decl))
}

/// Why `view` cannot be maintained, from what its closure reaches; `None` when it can.
///
/// `closure` is every name the view reads, through other views, lowercased, as
/// `analytics::reachable_tables` gives it; `bodies` the nest's authored view bodies by name;
/// `entities` the RFC-0041 relations the nest declares. Each refusal is a relation whose rows are not
/// covered by the identity, or a view whose answer is not a function of its inputs at all.
pub(crate) fn refusal(
    view: &str,
    closure: Option<&BTreeSet<String>>,
    bodies: &BTreeMap<String, String>,
    entities: &BTreeSet<String>,
) -> Option<String> {
    // The name becomes a directory under `maintained/`.
    if view.is_empty()
        || !view
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Some(format!(
            "`{view}` is not a plain view name (lowercase letters, digits and `_`)"
        ));
    }
    if !bodies.contains_key(view) {
        return Some(format!("`{view}` is not an authored view in views/*.sql"));
    }
    let Some(closure) = closure else {
        return Some(format!(
            "what `{view}` reads could not be worked out from its definition"
        ));
    };
    for name in closure {
        if let Some(body) = bodies.get(name) {
            if !crate::sqlmemo::is_deterministic(body) {
                return Some(format!(
                    "`{name}` calls a volatile function, so its answer is not a function of the nest's data"
                ));
            }
        }
        let why = if name == "labels" {
            "the label snapshots"
        } else if name.starts_with("offchain__") {
            "an offchain snapshot view"
        } else if name.ends_with("__children") {
            "a factory children view"
        } else if entities.contains(name) {
            "an incremental entity (RFC-0041)"
        } else {
            continue;
        };
        return Some(format!(
            "`{view}` reads `{name}`, {why}, which a copy's identity does not cover yet"
        ));
    }
    None
}

/// The binary part of an identity: the release and a digest of the executable, so a build between
/// releases that changes an answer is never served another build's copies (#1790's class).
#[cfg(not(test))]
fn binary_identity() -> Option<&'static str> {
    static BINARY: OnceLock<Option<String>> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            let exe = std::env::current_exe().ok()?;
            let mut file = std::fs::File::open(&exe).ok()?;
            let mut h = Sha256::new();
            std::io::copy(&mut file, &mut h).ok()?;
            Some(format!(
                "nuthatch {} {}",
                env!("CARGO_PKG_VERSION"),
                hex::encode(h.finalize())
            ))
        })
        .as_deref()
}

#[cfg(test)]
fn test_binaries() -> &'static Mutex<HashMap<PathBuf, String>> {
    static B: OnceLock<Mutex<HashMap<PathBuf, String>>> = OnceLock::new();
    B.get_or_init(Default::default)
}

/// Stand in another binary for `dir`, as an upgrade would.
#[cfg(test)]
pub(crate) fn test_set_binary(dir: &Path, binary: &str) {
    test_binaries()
        .lock()
        .unwrap()
        .insert(dir.to_path_buf(), binary.to_string());
}

fn binary_for(_dir: &Path) -> Option<String> {
    // A test binary is half a gigabyte unoptimised; hashing it starved the suite's timed tests.
    #[cfg(test)]
    return Some(
        test_binaries()
            .lock()
            .unwrap()
            .get(_dir)
            .cloned()
            .unwrap_or_else(|| "nuthatch-under-test".to_string()),
    );
    #[cfg(not(test))]
    binary_identity().map(str::to_string)
}

/// Everything one view's evaluation reads.
pub(crate) struct Inputs<'a> {
    pub view: &'a str,
    pub engine: &'a str,
    pub files: &'a BTreeMap<PathBuf, String>,
    /// [`schema_digest`] over the closure.
    pub schema: &'a str,
    /// [`sealed_digest`] over the closure.
    pub sealed: &'a str,
    pub closure: &'a BTreeSet<String>,
    pub hot: &'a HotRows,
}

impl Inputs<'_> {
    /// The identity, or `None` when the binary cannot be identified, in which case nothing is
    /// served or built.
    pub(crate) fn identity(&self, dir: &Path) -> Option<String> {
        let binary = binary_for(dir)?;
        let mut h = Sha256::new();
        let mut field = |bytes: &[u8]| {
            h.update((bytes.len() as u64).to_le_bytes());
            h.update(bytes);
        };
        field(b"nuthatch-maintained-v1");
        field(self.view.as_bytes());
        field(binary.as_bytes());
        field(self.engine.as_bytes());
        for (path, stamp) in self.files {
            field(path.to_string_lossy().as_bytes());
            field(stamp.as_bytes());
        }
        field(self.schema.as_bytes());
        field(self.sealed.as_bytes());
        field(&canonical_hot(self.hot, self.closure));
        Some(hex::encode(h.finalize()))
    }
}

/// The hot rows of the closure's tables, serialized canonically (RFC-0062 §3.2): tables in name
/// order, rows in order of their own bytes, each row's keys sorted, everything length-prefixed. The
/// same rows in another order serialize identically; different rows never do.
pub(crate) fn canonical_hot(hot: &HotRows, closure: &BTreeSet<String>) -> Vec<u8> {
    let mut tables: Vec<(&String, &Vec<Value>)> = hot
        .iter()
        .filter(|(t, rows)| !rows.is_empty() && closure.contains(&t.to_ascii_lowercase()))
        .collect();
    tables.sort_by(|a, b| a.0.cmp(b.0));
    let mut out = Vec::new();
    let field = |out: &mut Vec<u8>, bytes: &[u8]| {
        out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        out.extend_from_slice(bytes);
    };
    for (table, rows) in tables {
        field(&mut out, table.as_bytes());
        let mut encoded: Vec<Vec<u8>> = rows
            .iter()
            .map(|r| {
                let mut b = Vec::new();
                canonical_json(r, &mut b);
                b
            })
            .collect();
        encoded.sort();
        field(&mut out, &(encoded.len() as u64).to_le_bytes());
        for row in &encoded {
            field(&mut out, row);
        }
    }
    out
}

/// JSON with every object's keys in sorted order. Not `serde_json::to_vec`: this crate's maps keep
/// insertion order (`preserve_order` is on), so the same row could serialize two ways.
fn canonical_json(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(&serde_json::to_vec(k).unwrap_or_default());
                out.push(b':');
                canonical_json(&map[k], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                canonical_json(item, out);
            }
            out.push(b']');
        }
        scalar => out.extend_from_slice(&serde_json::to_vec(scalar).unwrap_or_default()),
    }
}

/// The sealed segments at or below `sealed_through` of the closure's tables, by table, file and
/// content hash. Only the closure: a seal on a table the view never reads must not rebuild it.
/// `None` when the manifest will not load.
pub(crate) fn sealed_digest(
    dir: &Path,
    sealed_through: u64,
    closure: &BTreeSet<String>,
) -> Option<String> {
    let manifest = crate::seal::load_manifest(dir).ok()?;
    let mut h = Sha256::new();
    for (table, segs) in &manifest.tables {
        if !closure.contains(&table.to_ascii_lowercase()) {
            continue;
        }
        for seg in segs.iter().filter(|s| s.to_block <= sealed_through) {
            for part in [table.as_str(), seg.file.as_str(), seg.hash.as_str()] {
                h.update((part.len() as u64).to_le_bytes());
                h.update(part.as_bytes());
            }
        }
    }
    Some(hex::encode(h.finalize()))
}

/// The columns the closure's tables are bound with: the live registry's declaration and `schema.json`,
/// which between them decide every empty typed view and every derived `*_dec` column.
pub(crate) fn schema_digest(
    dir: &Path,
    declared: &[crate::registry::TableSchema],
    closure: &BTreeSet<String>,
) -> String {
    let mut h = Sha256::new();
    let mut tables: Vec<&crate::registry::TableSchema> = declared
        .iter()
        .filter(|t| closure.contains(&t.table.to_ascii_lowercase()))
        .collect();
    tables.sort_by(|a, b| a.table.cmp(&b.table));
    for t in tables {
        let mut b = Vec::new();
        canonical_json(&serde_json::to_value(t).unwrap_or(Value::Null), &mut b);
        h.update((b.len() as u64).to_le_bytes());
        h.update(&b);
    }
    let schema_file = std::fs::read(dir.join("schema.json")).unwrap_or_default();
    h.update(Sha256::digest(&schema_file));
    hex::encode(h.finalize())
}

/// Where the copy of `view` at `id` lives.
pub(crate) fn copy_path(dir: &Path, view: &str, id: &str) -> PathBuf {
    dir.join(COPIES_DIR)
        .join(view)
        .join(format!("{id}.parquet"))
}

/// One view's counters, for `/metrics` and `/ready`.
#[derive(Default)]
pub struct ViewStats {
    pub builds: AtomicU64,
    pub build_failures: AtomicU64,
    pub hits: AtomicU64,
    pub fallbacks: AtomicU64,
    pub last_build_ms: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl ViewStats {
    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// A nest that declares maintained views, as its start read them.
struct Nest {
    views: BTreeSet<String>,
    recent: usize,
    cursor: Option<Arc<Cursor>>,
    stats: BTreeMap<String, Arc<ViewStats>>,
    /// The identity whose build last failed, per view, so a request does not queue it again.
    failed: HashMap<String, String>,
    /// Copies retention has handed to the read leases and not yet seen deleted. Never served,
    /// because a request holding a newer lease would not keep one alive.
    retired: HashSet<PathBuf>,
    feed: Option<Arc<Feed>>,
}

/// The nest's current inputs, read as a request reads them: its hot rows and served watermark.
pub type ReadInputs = dyn Fn() -> Result<(HotRows, u64)> + Send + Sync;

/// How the builder reads a nest's inputs for an eager build.
pub struct Feed {
    pub read: Box<ReadInputs>,
    /// The tables the nest declares, as its requests bind them.
    pub declared: Arc<Vec<crate::registry::TableSchema>>,
}

fn nests() -> &'static Mutex<HashMap<PathBuf, Nest>> {
    static NESTS: OnceLock<Mutex<HashMap<PathBuf, Nest>>> = OnceLock::new();
    NESTS.get_or_init(Default::default)
}

fn lock_nests() -> std::sync::MutexGuard<'static, HashMap<PathBuf, Nest>> {
    nests().lock().unwrap_or_else(|p| p.into_inner())
}

/// Read `dir`'s declaration and refuse, by name, every view that cannot be maintained. A nest that
/// declares views is held from here on; one with no file, or an empty one, holds nothing.
pub fn load(dir: &Path) -> Result<()> {
    let refused = refusals(dir)?;
    if !refused.is_empty() {
        let list: Vec<String> = refused
            .iter()
            .map(|(view, why)| format!("  - {view}: {why}"))
            .collect();
        bail!(
            "{} declares {} view(s) that cannot be maintained:\n{}",
            dir.join(DECLARATION_FILE).display(),
            refused.len(),
            list.join("\n")
        );
    }
    let decl = read(dir)?.unwrap_or_default();
    let views: BTreeSet<String> = decl
        .view
        .iter()
        .map(|v| v.name.to_ascii_lowercase())
        .collect();
    remove_undeclared(dir, &views);
    let mut all = lock_nests();
    if views.is_empty() {
        all.remove(dir);
        return Ok(());
    }
    if binary_for(dir).is_none() {
        tracing::warn!(
            "{}: this binary could not be read to identify it, so no maintained view will be \
             served from a copy; every request answers from the definition",
            DECLARATION_FILE
        );
    }
    let stats = views
        .iter()
        .map(|v| (v.clone(), Arc::new(ViewStats::default())))
        .collect();
    all.insert(
        dir.to_path_buf(),
        Nest {
            views,
            recent: decl.recent.unwrap_or(DEFAULT_RECENT),
            cursor: None,
            stats,
            failed: HashMap::new(),
            retired: HashSet::new(),
            feed: None,
        },
    );
    Ok(())
}

/// Copies of views `dir` no longer declares, handed to the read leases; the directory of each goes
/// once it is empty. Nothing can be served from them again, and a declaration brought back rebuilds.
fn remove_undeclared(dir: &Path, views: &BTreeSet<String>) {
    let root = dir.join(COPIES_DIR);
    let Ok(entries) = std::fs::read_dir(&root) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if views.contains(&name) || !e.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let files: Vec<PathBuf> = std::fs::read_dir(e.path())
            .map(|rd| rd.flatten().map(|f| f.path()).collect())
            .unwrap_or_default();
        tracing::info!(
            "removing {} copies of `{name}`, which {DECLARATION_FILE} no longer declares",
            files.len()
        );
        crate::seal::retire(dir, files);
        let _ = std::fs::remove_dir(e.path());
    }
    if views.is_empty() {
        let _ = std::fs::remove_dir(&root);
    }
}

/// Each declared view that cannot be maintained, with why. `Err` for a file that will not parse.
pub fn refusals(dir: &Path) -> Result<Vec<(String, String)>> {
    let Some(decl) = read(dir)? else {
        return Ok(Vec::new());
    };
    let bodies = crate::analytics::nest_view_bodies(dir);
    let entities = crate::analytics::declared_entity_names(dir);
    let session = crate::analytics::engine()
        .open_bare()
        .context("opening a session to read the maintained views' definitions")?;
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for v in &decl.view {
        let name = v.name.to_ascii_lowercase();
        if !seen.insert(name.clone()) {
            out.push((v.name.clone(), "declared twice".to_string()));
            continue;
        }
        let closure = crate::analytics::view_closure(session.as_ref(), dir, &name);
        if let Some(why) = refusal(&name, closure.as_ref(), &bodies, &entities) {
            out.push((v.name.clone(), why));
        }
    }
    Ok(out)
}

/// The maintained views `dir` declared, lowercased; empty for a nest that declares none.
pub(crate) fn declared(dir: &Path) -> BTreeSet<String> {
    lock_nests()
        .get(dir)
        .map(|n| n.views.clone())
        .unwrap_or_default()
}

/// Whether `dir` declared any maintained view.
pub fn is_declared(dir: &Path) -> bool {
    lock_nests().contains_key(dir)
}

/// Whether the copy at `path` may be bound: present, and not handed to retention.
pub(crate) fn servable(dir: &Path, path: &Path) -> bool {
    let mut all = lock_nests();
    if let Some(n) = all.get_mut(dir) {
        if n.retired.contains(path) {
            // Deleted by now unless a request is still reading it; once gone it may be built again.
            if !path.exists() {
                n.retired.remove(path);
            }
            return false;
        }
    }
    path.exists()
}

fn stats_of(dir: &Path, view: &str) -> Option<Arc<ViewStats>> {
    lock_nests()
        .get(dir)
        .and_then(|n| n.stats.get(view).cloned())
}

pub(crate) fn note_hit(dir: &Path, view: &str) {
    if let Some(s) = stats_of(dir, view) {
        s.hits.fetch_add(1, Relaxed);
    }
}

pub(crate) fn note_fallback(dir: &Path, view: &str) {
    if let Some(s) = stats_of(dir, view) {
        s.fallbacks.fetch_add(1, Relaxed);
    }
}

/// A copy that bound and then would not read: handed to retention, so the next request builds it
/// again rather than failing on it.
pub(crate) fn discard(dir: &Path, copies: &[PathBuf]) {
    if copies.is_empty() {
        return;
    }
    for c in copies {
        tracing::warn!(
            "maintained copy {} would not read; answering from the definition and building it again",
            c.display()
        );
    }
    if let Some(n) = lock_nests().get_mut(dir) {
        n.retired.extend(copies.iter().cloned());
    }
    crate::seal::retire(dir, copies.to_vec());
}

/// Per view: its counters, how many copies it has on disk, and their bytes.
pub fn report(dir: &Path) -> Vec<(String, Arc<ViewStats>, u64, u64)> {
    let stats: Vec<(String, Arc<ViewStats>)> = match lock_nests().get(dir) {
        Some(n) => n
            .stats
            .iter()
            .map(|(v, s)| (v.clone(), s.clone()))
            .collect(),
        None => return Vec::new(),
    };
    stats
        .into_iter()
        .map(|(view, s)| {
            let (copies, bytes) = copies_on_disk(&dir.join(COPIES_DIR).join(&view));
            (view, s, copies, bytes)
        })
        .collect()
}

fn copies_on_disk(view_dir: &Path) -> (u64, u64) {
    let Ok(entries) = std::fs::read_dir(view_dir) else {
        return (0, 0);
    };
    entries
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .filter_map(|e| e.metadata().ok())
        .fold((0, 0), |(n, b), m| (n + 1, b + m.len()))
}

/// One build: evaluate `view` at the inputs `id` was computed from and write it to its copy.
pub(crate) struct Job {
    pub dir: PathBuf,
    pub view: String,
    pub id: String,
    /// Only the closure's tables: nothing else is read.
    pub hot: HotRows,
    pub sealed_through: u64,
    pub declared: Vec<crate::registry::TableSchema>,
}

/// The builder of one cursor: one build at a time, each holding one of the cursor's analytical
/// permits while it runs, so a build is admitted exactly as a query is (RFC-0047 C4).
struct Cursor {
    gate: crate::serve::SqlGate,
    state: Mutex<CursorState>,
}

#[derive(Default)]
struct CursorState {
    /// At most one waiting build per view; a newer request's replaces it.
    pending: BTreeMap<(PathBuf, String), Job>,
    /// Nests whose inputs moved since the builder last read them. Planned before any build runs.
    dirty: BTreeSet<PathBuf>,
    building: Option<(PathBuf, String, String)>,
    running: bool,
}

impl CursorState {
    /// Start the builder thread unless it is already draining.
    fn wake(&mut self, cursor: &Arc<Cursor>) {
        if !self.running {
            self.running = true;
            let c = cursor.clone();
            std::thread::Builder::new()
                .name("nuthatch-maintained".into())
                .spawn(move || drain(c))
                .expect("spawn the maintained-view builder");
        }
    }
}

fn cursors() -> &'static Mutex<Vec<Arc<Cursor>>> {
    static CURSORS: OnceLock<Mutex<Vec<Arc<Cursor>>>> = OnceLock::new();
    CURSORS.get_or_init(Default::default)
}

/// Give `dir`'s builds to the builder of the cursor whose analytical gate is `gate`. Without this a
/// held nest still serves copies it has, and builds none: what a process with no cursor does.
pub fn attach(dir: &Path, gate: crate::serve::SqlGate) {
    let cursor = {
        let mut all = cursors().lock().unwrap_or_else(|p| p.into_inner());
        match all.iter().find(|c| Arc::ptr_eq(&*c.gate, &*gate)) {
            Some(c) => c.clone(),
            None => {
                let c = Arc::new(Cursor {
                    gate,
                    state: Mutex::new(CursorState::default()),
                });
                all.push(c.clone());
                c
            }
        }
    };
    if let Some(n) = lock_nests().get_mut(dir) {
        n.cursor = Some(cursor);
    }
}

/// Whether a request that found no copy of `view` at `id` should queue one: the nest has a builder,
/// and this identity has not already failed.
pub(crate) fn wants_build(dir: &Path, view: &str, id: &str) -> bool {
    lock_nests()
        .get(dir)
        .is_some_and(|n| n.cursor.is_some() && n.failed.get(view).is_none_or(|failed| failed != id))
}

/// Give `dir`'s builder a way to read its inputs, and plan its first builds. From here on its builds
/// are eager: [`changed`] plans them, and a request that finds no copy asks for a plan rather than
/// queueing the inputs it read, which may already be older than the store's.
pub fn feed(dir: &Path, feed: Feed) {
    if let Some(n) = lock_nests().get_mut(dir) {
        n.feed = Some(Arc::new(feed));
    }
    changed(dir);
}

/// `dir`'s inputs may have moved: a commit or a seal. Marks the nest for the builder to read
/// again and returns at once; nothing is read or built on the caller's thread.
pub fn changed(dir: &Path) {
    let Some(cursor) = lock_nests()
        .get(dir)
        .filter(|n| n.feed.is_some())
        .and_then(|n| n.cursor.clone())
    else {
        return;
    };
    let mut st = cursor.state.lock().unwrap_or_else(|p| p.into_inner());
    st.dirty.insert(dir.to_path_buf());
    st.wake(&cursor);
}

/// Queue `job` on its nest's cursor. A build already running for the same identity, or a copy
/// already present, makes this a no-op. A fed nest plans from its current inputs instead.
pub(crate) fn request_build(job: Job) {
    let Some((cursor, fed)) = lock_nests()
        .get(&job.dir)
        .and_then(|n| Some((n.cursor.clone()?, n.feed.is_some())))
    else {
        return;
    };
    if fed {
        return changed(&job.dir);
    }
    let mut st = cursor.state.lock().unwrap_or_else(|p| p.into_inner());
    if st
        .building
        .as_ref()
        .is_some_and(|(d, v, id)| *d == job.dir && *v == job.view && *id == job.id)
    {
        return;
    }
    st.pending.insert((job.dir.clone(), job.view.clone()), job);
    st.wake(&cursor);
}

enum Next {
    Plan(PathBuf),
    Build(Job),
}

fn drain(cursor: Arc<Cursor>) {
    loop {
        let next = {
            let mut st = cursor.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(dir) = st.dirty.pop_first() {
                Next::Plan(dir)
            } else if let Some(key) = st.pending.keys().next().cloned() {
                let job = st.pending.remove(&key).expect("just found");
                st.building = Some((job.dir.clone(), job.view.clone(), job.id.clone()));
                Next::Build(job)
            } else {
                st.running = false;
                st.building = None;
                return;
            }
        };
        let job = match next {
            Next::Plan(dir) => {
                plan(&cursor, &dir);
                continue;
            }
            Next::Build(job) => job,
        };
        // Newer inputs make this job's identity nobody's: drop it, and the plan they wait on
        // queues whatever is current.
        let superseded = || {
            cursor
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .dirty
                .contains(&job.dir)
        };
        // Polled, not awaited: a waiting build yields every permit to the requests it is behind.
        let permit = loop {
            if superseded() {
                break None;
            }
            if cursor.gate.is_closed() {
                break None;
            }
            // Charged to a tenant mounting this dataset, so a build never takes another's reservation.
            match cursor.gate.try_admit_for(&job.dir) {
                Some(p) => break Some(p),
                None => std::thread::sleep(Duration::from_millis(20)),
            }
        };
        if permit.is_some() && !superseded() {
            build(&job);
        }
        drop(permit);
        cursor
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .building = None;
    }
}

/// Read `dir`'s inputs through its feed and queue a build of every declared view with no copy at
/// them, replacing whatever was queued for the nest before. A view whose inputs did not move keeps
/// its copy and costs no build.
fn plan(cursor: &Arc<Cursor>, dir: &Path) {
    let Some((views, failed, feed)) = lock_nests()
        .get(dir)
        .and_then(|n| Some((n.views.clone(), n.failed.clone(), n.feed.clone()?)))
    else {
        return;
    };
    let current = (feed.read)().and_then(|(hot, sealed_through)| {
        let ids = crate::analytics::maintained_identities(
            dir,
            &views,
            &hot,
            sealed_through,
            &feed.declared,
        )?;
        Ok((hot, sealed_through, ids))
    });
    let (hot, sealed_through, ids) = match current {
        Ok(c) => c,
        Err(e) => {
            tracing::debug!(
                "maintained views of {}: inputs not read: {e:#}",
                dir.display()
            );
            return;
        }
    };
    let mut jobs = Vec::new();
    for (view, id, closure) in ids {
        if failed.get(&view) == Some(&id) || servable(dir, &copy_path(dir, &view, &id)) {
            continue;
        }
        jobs.push(Job {
            dir: dir.to_path_buf(),
            view,
            id,
            hot: hot
                .iter()
                .filter(|(t, _)| closure.contains(&t.to_ascii_lowercase()))
                .map(|(t, rows)| (t.clone(), rows.clone()))
                .collect(),
            sealed_through,
            declared: feed.declared.to_vec(),
        });
    }
    let mut st = cursor.state.lock().unwrap_or_else(|p| p.into_inner());
    st.pending.retain(|(d, _), _| d != dir);
    for job in jobs {
        st.pending.insert((job.dir.clone(), job.view.clone()), job);
    }
}

fn build(job: &Job) {
    let target = copy_path(&job.dir, &job.view, &job.id);
    // Present, or retired and not yet deleted: writing over the second would see it deleted.
    if target.exists() {
        return;
    }
    let started = Instant::now();
    let tmp = target.with_extension("parquet.tmp");
    let made = (|| -> Result<u64> {
        std::fs::create_dir_all(target.parent().expect("a copy path has a parent"))?;
        crate::analytics::write_maintained(
            &job.dir,
            &job.view,
            &job.id,
            &tmp,
            &job.hot,
            job.sealed_through,
            &job.declared,
        )?;
        std::fs::rename(&tmp, &target)?;
        Ok(std::fs::metadata(&target)?.len())
    })();
    let stats = stats_of(&job.dir, &job.view);
    match made {
        Ok(bytes) => {
            let ms = started.elapsed().as_millis() as u64;
            if let Some(s) = &stats {
                s.builds.fetch_add(1, Relaxed);
                s.last_build_ms.store(ms, Relaxed);
                *s.last_error.lock().unwrap_or_else(|p| p.into_inner()) = None;
            }
            tracing::info!(
                "maintained view {} built in {ms} ms, {bytes} bytes",
                job.view
            );
            if let Some(n) = lock_nests().get_mut(&job.dir) {
                n.retired.remove(&target);
            }
            retain(&job.dir, &job.view, &target);
        }
        Err(e) if e.chain().any(|c| c.is::<crate::analytics::InputsMoved>()) => {
            let _ = std::fs::remove_file(&tmp);
            tracing::debug!("maintained view {}: {e:#}", job.view);
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            let error = format!("{e:#}");
            tracing::warn!(
                "maintained view {} not built; requests answer from its definition: {error}",
                job.view
            );
            if let Some(s) = &stats {
                s.build_failures.fetch_add(1, Relaxed);
                *s.last_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(error);
            }
            if let Some(n) = lock_nests().get_mut(&job.dir) {
                n.failed.insert(job.view.clone(), job.id.clone());
            }
        }
    }
}

/// Keep the copy just built and the `recent` most recently built others; hand the rest to the
/// read leases, which delete each once no request that could have bound it is still running.
fn retain(dir: &Path, view: &str, current: &Path) {
    let recent = match lock_nests().get(dir) {
        Some(n) => n.recent,
        None => return,
    };
    let view_dir = dir.join(COPIES_DIR).join(view);
    let Ok(entries) = std::fs::read_dir(&view_dir) else {
        return;
    };
    let mut copies: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    for e in entries.flatten() {
        let path = e.path();
        let name = path.to_string_lossy();
        if name.ends_with(".parquet.tmp") {
            // Only this builder writes here, and it is between builds.
            let _ = std::fs::remove_file(&path);
            continue;
        }
        if path == current || !name.ends_with(".parquet") {
            continue;
        }
        let built = e
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        copies.push((built, path));
    }
    copies.sort_by(|a, b| b.cmp(a));
    let doomed: Vec<PathBuf> = {
        let mut all = lock_nests();
        let Some(n) = all.get_mut(dir) else {
            return;
        };
        let doomed: Vec<PathBuf> = copies
            .into_iter()
            .map(|(_, p)| p)
            .filter(|p| !n.retired.contains(p))
            .skip(recent)
            .collect();
        n.retired.extend(doomed.iter().cloned());
        doomed
    };
    crate::seal::retire(dir, doomed);
}

/// The identities of `dir`'s builds that are queued or waiting to run.
#[cfg(test)]
pub(crate) fn queued(dir: &Path) -> Vec<String> {
    let Some(cursor) = lock_nests().get(dir).and_then(|n| n.cursor.clone()) else {
        return Vec::new();
    };
    let st = cursor.state.lock().unwrap();
    st.building
        .iter()
        .filter(|(d, ..)| d == dir)
        .map(|(_, _, id)| id.clone())
        .chain(
            st.pending
                .iter()
                .filter(|((d, _), _)| d == dir)
                .map(|(_, j)| j.id.clone()),
        )
        .collect()
}

/// Wait until `dir`'s cursor has no build queued or running.
#[cfg(test)]
pub(crate) fn wait_idle(dir: &Path) {
    let Some(cursor) = lock_nests().get(dir).and_then(|n| n.cursor.clone()) else {
        return;
    };
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        {
            let st = cursor.state.lock().unwrap();
            if !st.running && st.pending.is_empty() && st.dirty.is_empty() {
                return;
            }
        }
        assert!(Instant::now() < deadline, "the builder never went idle");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn closure(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// The same rows in another order, with their keys in another order, are the same identity; a
    /// different row, or a table the view does not read, is not and is ignored respectively.
    #[test]
    fn hot_rows_serialize_canonically_and_only_for_the_closure() {
        let a = json!({"block_number": 5, "from": "0xa", "value": "1"});
        let b = json!({"value": "2", "from": "0xb", "block_number": 6});
        let b_reordered = {
            let mut m = serde_json::Map::new();
            m.insert("from".into(), json!("0xb"));
            m.insert("block_number".into(), json!(6));
            m.insert("value".into(), json!("2"));
            Value::Object(m)
        };
        let reach = closure(&["t__transfer", "v"]);
        let one: HotRows = [("t__transfer".to_string(), vec![a.clone(), b.clone()])]
            .into_iter()
            .collect();
        let two: HotRows = [("t__transfer".to_string(), vec![b_reordered, a.clone()])]
            .into_iter()
            .collect();
        assert_eq!(canonical_hot(&one, &reach), canonical_hot(&two, &reach));

        let other: HotRows = [(
            "t__transfer".to_string(),
            vec![
                a.clone(),
                json!({"value": "3", "from": "0xb", "block_number": 6}),
            ],
        )]
        .into_iter()
        .collect();
        assert_ne!(canonical_hot(&one, &reach), canonical_hot(&other, &reach));

        let mut unrelated = one.clone();
        unrelated.insert("t__approval".into(), vec![json!({"x": 1})]);
        assert_eq!(
            canonical_hot(&one, &reach),
            canonical_hot(&unrelated, &reach),
            "a table outside the closure"
        );

        // Two tables cannot be confused for one by moving a row across the boundary.
        let split: HotRows = [
            ("t__transfer".to_string(), vec![a.clone()]),
            ("t__z".to_string(), vec![b.clone()]),
        ]
        .into_iter()
        .collect();
        let joined: HotRows = [
            ("t__transfer".to_string(), vec![a, b]),
            ("t__z".to_string(), vec![]),
        ]
        .into_iter()
        .collect();
        let both = closure(&["t__transfer", "t__z"]);
        assert_ne!(canonical_hot(&split, &both), canonical_hot(&joined, &both));
    }

    /// Every input moves the identity; inputs outside the view's closure do not.
    #[test]
    fn every_input_moves_the_identity() {
        let dir = PathBuf::from("/nest-every-input");
        test_set_binary(&dir, "nuthatch 1.0.0 aa");
        let files: BTreeMap<PathBuf, String> =
            [(PathBuf::from("/n/views/a.sql"), "h1".into())].into();
        let reach = closure(&["t", "v"]);
        let hot: HotRows = [("t".to_string(), vec![json!({"x": 1})])].into();
        let base = |view: &'static str| Inputs {
            view,
            engine: "burrmill 1",
            files: &files,
            schema: "s",
            sealed: "seg",
            closure: &reach,
            hot: &hot,
        };
        let id = base("v").identity(&dir).unwrap();
        assert_eq!(id, base("v").identity(&dir).unwrap(), "deterministic");
        assert_ne!(id, base("w").identity(&dir).unwrap(), "view");
        let mut i = base("v");
        i.engine = "burrmill 2";
        assert_ne!(id, i.identity(&dir).unwrap(), "engine");
        let files2: BTreeMap<PathBuf, String> =
            [(PathBuf::from("/n/views/a.sql"), "h2".into())].into();
        let mut i = base("v");
        i.files = &files2;
        assert_ne!(id, i.identity(&dir).unwrap(), "authored file");
        let mut i = base("v");
        i.schema = "s2";
        assert_ne!(id, i.identity(&dir).unwrap(), "schema");
        let mut i = base("v");
        i.sealed = "seg2";
        assert_ne!(id, i.identity(&dir).unwrap(), "sealed segments");
        let hot2: HotRows = [("t".to_string(), vec![json!({"x": 2})])].into();
        let mut i = base("v");
        i.hot = &hot2;
        assert_ne!(id, i.identity(&dir).unwrap(), "hot rows");
        let hot3: HotRows = [
            ("t".to_string(), vec![json!({"x": 1})]),
            ("u".to_string(), vec![json!({"y": 1})]),
        ]
        .into();
        let mut i = base("v");
        i.hot = &hot3;
        assert_eq!(
            id,
            i.identity(&dir).unwrap(),
            "hot rows outside the closure"
        );
        test_set_binary(&dir, "nuthatch 1.0.1 bb");
        assert_ne!(id, base("v").identity(&dir).unwrap(), "binary");
    }

    /// The sealed digest moves with a segment of a closure table and not with one of another table.
    #[test]
    fn the_sealed_digest_covers_the_closure_only() {
        let tmp = tempfile::tempdir().unwrap();
        let write = |segs: Value| {
            let dir = tmp.path().join(crate::seal::SEGMENTS_DIR);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join(crate::seal::MANIFEST_FILE),
                json!({ "tables": segs }).to_string(),
            )
            .unwrap();
        };
        let seg = |t: &str, hash: &str, from: u64, to: u64| json!({"hash": hash, "from_block": from, "to_block": to, "rows": 1, "file": format!("{t}-{hash}.parquet")});
        let reach = closure(&["t"]);
        write(json!({"t": [seg("t", "a", 1, 10)], "u": [seg("u", "b", 1, 10)]}));
        let before = sealed_digest(tmp.path(), 100, &reach).unwrap();
        write(
            json!({"t": [seg("t", "a", 1, 10)], "u": [seg("u", "b", 1, 10), seg("u", "c", 11, 20)]}),
        );
        assert_eq!(
            before,
            sealed_digest(tmp.path(), 100, &reach).unwrap(),
            "a seal on a table the view never reads"
        );
        write(
            json!({"t": [seg("t", "a", 1, 10), seg("t", "d", 11, 20)], "u": [seg("u", "b", 1, 10)]}),
        );
        assert_ne!(
            before,
            sealed_digest(tmp.path(), 100, &reach).unwrap(),
            "a seal on a table it does"
        );
        assert_eq!(
            before,
            sealed_digest(tmp.path(), 10, &reach).unwrap(),
            "a segment above the served watermark is not read"
        );
    }

    /// Refusals name the view and the relation: an undeclared view, a volatile function anywhere
    /// in the closure, and each relation the identity does not cover.
    #[test]
    fn a_view_that_cannot_be_maintained_is_refused_by_name() {
        let bodies: BTreeMap<String, String> = [
            ("v".to_string(), "SELECT * FROM w".to_string()),
            ("w".to_string(), "SELECT * FROM t".to_string()),
            ("clock".to_string(), "SELECT now() AS at FROM t".to_string()),
            ("uses_clock".to_string(), "SELECT * FROM clock".to_string()),
        ]
        .into();
        let entities = closure(&["holders"]);
        let r =
            |view: &str, reach: &[&str]| refusal(view, Some(&closure(reach)), &bodies, &entities);
        assert_eq!(r("v", &["v", "w", "t"]), None);
        assert!(r("missing", &["missing"])
            .unwrap()
            .contains("not an authored view"));
        let why = r("uses_clock", &["uses_clock", "clock", "t"]).unwrap();
        assert!(why.contains("`clock` calls a volatile function"), "{why}");
        for (name, says) in [
            ("labels", "label snapshots"),
            ("offchain__prices", "offchain snapshot"),
            ("pool__children", "factory children"),
            ("holders", "incremental entity"),
        ] {
            let why = r("v", &["v", "w", name]).unwrap();
            assert!(why.contains(name) && why.contains(says), "{why}");
        }
        assert!(refusal("v", None, &bodies, &entities)
            .unwrap()
            .contains("could not be worked out"));
        // A name that is not a plain identifier never becomes a path, even if a body had it.
        let mut odd = bodies.clone();
        for name in ["..", "../escape", "/tmp/cache", "a/b", "."] {
            odd.insert(name.to_string(), "SELECT 1".to_string());
            let why = refusal(name, Some(&closure(&[name])), &odd, &entities).unwrap();
            assert!(why.contains("not a plain view name"), "{name}: {why}");
        }
    }

    /// No file, no declaration, nothing held: the deletion test's first half.
    #[test]
    fn a_nest_without_the_file_holds_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read(tmp.path()).unwrap().is_none());
        load(tmp.path()).unwrap();
        assert!(!is_declared(tmp.path()));
        assert!(declared(tmp.path()).is_empty());
        assert!(report(tmp.path()).is_empty());
        std::fs::write(tmp.path().join(DECLARATION_FILE), "").unwrap();
        load(tmp.path()).unwrap();
        assert!(!is_declared(tmp.path()), "an empty declaration");
    }

    /// The declaration's own spelling: a misspelt key is named, a view declared twice is refused.
    #[test]
    fn the_declaration_reads_and_refuses_what_it_should() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("views")).unwrap();
        std::fs::write(
            tmp.path().join("views/a.sql"),
            "CREATE VIEW a AS SELECT 1 AS id;",
        )
        .unwrap();
        std::fs::write(
            tmp.path().join(DECLARATION_FILE),
            "recent = 2\n[[view]]\nname = \"a\"\n[[view]]\nname = \"A\"\n",
        )
        .unwrap();
        let refused = refusals(tmp.path()).unwrap();
        assert_eq!(
            refused,
            vec![("A".to_string(), "declared twice".to_string())]
        );
        let err = load(tmp.path()).unwrap_err().to_string();
        assert!(err.contains("A: declared twice"), "{err}");

        // A quoted view name that is a path: never an authored view, never declared.
        std::fs::write(
            tmp.path().join("views/b.sql"),
            "CREATE VIEW \"../escape\" AS SELECT 1 AS id;",
        )
        .unwrap();
        std::fs::write(
            tmp.path().join(DECLARATION_FILE),
            "[[view]]\nname = \"../escape\"\n",
        )
        .unwrap();
        let err = load(tmp.path()).unwrap_err().to_string();
        assert!(err.contains("not a plain view name"), "{err}");
        assert!(!is_declared(tmp.path()));

        std::fs::write(
            tmp.path().join(DECLARATION_FILE),
            "[[veiw]]\nname = \"a\"\n",
        )
        .unwrap();
        let err = crate::config::refuse_unknown_file::<Declaration>(tmp.path(), DECLARATION_FILE)
            .unwrap_err()
            .to_string();
        assert!(err.contains("veiw"), "{err}");
    }
}
