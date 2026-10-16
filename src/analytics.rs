//! Read-only analytical SQL over the sealed Parquet segments **and the hot tip**, on Burrmill
//! (`engine_burrmill`). The sealed segments cover finalized history; the unsealed tip lives in redb.
//! For `/sql` (RFC-0013) the hot rows are handed to the session per table and unioned into each
//! table's view. Hot and cold are kept disjoint *structurally* by the `sealed_through` watermark
//! (COR-1): cold includes only segments finalized at/below it, hot only rows past it - so the union
//! is exact with no dedup, even across the brief seal→prune window. Trusted point-reads pass no hot
//! rows (and `u64::MAX`, i.e. all segments).
//!
//! Memory is capped so an analytical query can't blow the embedded-mode RAM budget.

use crate::engine::{Collected, Died, Engine, Interrupt, Session};
use anyhow::{bail, Context, Result};
use serde_json::Value;
#[cfg(test)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The engine every query runs on.
pub(crate) fn engine() -> &'static dyn Engine {
    static ENGINE: crate::engine_burrmill::BurrmillEngine = crate::engine_burrmill::BurrmillEngine;
    &ENGINE
}

/// Read at startup. It chose between DuckDB and Burrmill while a build carried both.
pub const ENV_ENGINE: &str = "NUTHATCH_ENGINE";

/// Refuses a unit still asking for DuckDB, alone or beside Burrmill, rather than serving it
/// something it did not ask for.
pub fn check_engine_choice(value: Option<&str>) -> Result<()> {
    match value.map(str::trim) {
        None | Some("" | "burrmill") => Ok(()),
        Some(other) => bail!(
            "{ENV_ENGINE}={other}: this build has no DuckDB, and Burrmill is the only engine. \
             Unset it or set it to burrmill."
        ),
    }
}

/// A session bounded and locked to `dir`, counted so a test can see the cache reuse one.
fn open_session(dir: &Path) -> Result<Box<dyn Session>> {
    note_session_open(dir);
    engine().open(dir)
}

/// One cached read-only session (#295). A fresh in-memory instance per query was the rebuild the
/// issue named: open, lockdown, attach, teardown. The connection is read-only and single-user, and
/// ingestion never writes here.
///
/// **Concurrent queries do not serialise on this mutex, and this comment used to imply they did.**
/// The cache mutex is taken twice per query, each time for a map operation: once to `remove()` the
/// slot and once to put it back. Both guards are statement temporaries that drop at the semicolon,
/// so the query itself runs unlocked - deliberately, see the note at the `remove()` call site. Two
/// later pieces of work read the old wording ("queries take the mutex") as "queries serialise on the
/// mutex" and published that as a *measured engine property*, in #986, in #991 and in RFC-0042's
/// slice 5 decision input. It was wrong in all three: measured on this path, a held mutex gives
/// 14.7 qps flat against up to 81.5 qps without one. Corrected under RFC-0042 §14.
///
/// What a concurrent caller does hit is the cache *miss* the `remove()` leaves behind: while one
/// query holds the slot out, another opens its own instance. That is where concurrent RSS comes
/// from, and it is why `SQL_MAX_CONCURRENCY` is a memory bound rather than a throughput one.
///
/// An interrupt drops the slot rather than leaving the engine half-cancelled for the next caller.
struct SessionCache {
    dir: PathBuf,
    sealed_through: u64,
    as_of: Option<u64>,
    excluded: std::collections::BTreeSet<String>,
    inputs: std::collections::BTreeMap<PathBuf, InputStamp>,
    last_used: u64,
    session: Box<dyn Session>,
}

/// A content hash of one cache input, hex sha256 (#840).
///
/// **This was `(len, modified_ns)` and could not see a same-length rewrite.** Measured on the Linux
/// dev box, 500 trials of "write 27 bytes, stat, rewrite 27 different bytes, stat": 497 collisions
/// on btrfs, 499 on tmpfs. The cause is the mtime clock, not the filesystem - 2,000 consecutive
/// writes produced **nine** distinguishable timestamps, a granularity of ~3.3 ms. So on the platform
/// this deploys to, a `>` changed to a `<` in a view did not merely *risk* going unnoticed by the
/// cache, it went unnoticed essentially always, and the cached connection served the previous
/// definition with no error anywhere. On macOS APFS the same probe gives 0/500 at ~37 us resolution,
/// which is why it looked fine in local development.
///
/// The cost is reading these files rather than stat-ing them. They are `nuthatch.toml`, `views/*.sql`
/// and `labels/*.json` - and `attempt()` already stats every one of them on every query, so this is
/// a read where there was a stat, over files that are small by construction.
type InputStamp = String;

fn content_stamp(path: &Path) -> Option<InputStamp> {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).ok()?;
    Some(hex::encode(Sha256::digest(&bytes)))
}

static SESSION_CACHE: OnceLock<Mutex<std::collections::HashMap<PathBuf, SessionCache>>> =
    OnceLock::new();
static SESSION_OPENS: OnceLock<Mutex<std::collections::HashMap<PathBuf, u64>>> = OnceLock::new();
static SESSION_USE: AtomicU64 = AtomicU64::new(0);
const SESSION_CACHE_CAPACITY: usize = 16;

fn session_cache_lock(
) -> std::sync::MutexGuard<'static, std::collections::HashMap<PathBuf, SessionCache>> {
    SESSION_CACHE
        .get_or_init(|| Mutex::new(std::collections::HashMap::new()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

fn note_session_open(dir: &Path) {
    *SESSION_OPENS
        .get_or_init(|| Mutex::new(std::collections::HashMap::new()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .entry(dir.to_path_buf())
        .or_default() += 1;
}

/// Drop a nest's analytical connection when its runtime ownership ends (#824).
pub fn invalidate_session_cache(dir: &Path) {
    session_cache_lock().remove(dir);
}

pub(crate) fn cache_inputs(dir: &Path) -> std::collections::BTreeMap<PathBuf, InputStamp> {
    let mut paths = vec![dir.join(crate::config::CONFIG_FILE)];
    paths.push(crate::offchain::catalogue_path(dir));
    if let Ok(entries) = std::fs::read_dir(dir.join("views")) {
        paths.extend(
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "sql")),
        );
    }
    if let Ok(entries) = std::fs::read_dir(dir.join(crate::labels::LABELS_DIR)) {
        paths.extend(
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "json")),
        );
    }
    paths
        .into_iter()
        .filter_map(|path| {
            let stamp = content_stamp(&path)?;
            Some((path, stamp))
        })
        .collect()
}

fn retain_session_cache(
    mut cache: std::sync::MutexGuard<'static, std::collections::HashMap<PathBuf, SessionCache>>,
    slot: SessionCache,
) {
    cache.insert(slot.dir.clone(), slot);
    while cache.len() > SESSION_CACHE_CAPACITY {
        let Some(victim) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(dir, _)| dir.clone())
        else {
            break;
        };
        cache.remove(&victim);
    }
}

#[cfg(test)]
fn session_opens_for(dir: &Path) -> u64 {
    SESSION_OPENS
        .get()
        .and_then(|m| m.lock().ok().map(|g| g.get(dir).copied().unwrap_or(0)))
        .unwrap_or(0)
}

/// A query the guard cut off at its deadline. Its own type, so `/sql` can tell a caller the query was
/// sound but too slow (a 504) rather than malformed (a 400): a gateway that sees the 400 has no way
/// to know it should simply try again later, and an alert built on it names the wrong fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryBudgetExceeded {
    pub secs: u64,
}

impl std::fmt::Display for QueryBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "query exceeded the {}s time budget on the read-only SQL surface",
            self.secs
        )
    }
}

impl std::error::Error for QueryBudgetExceeded {}

/// A query the guard stopped for spilling more than its connection's cap to disk (or to a tmpfs,
/// which is RAM). Its own type so `/sql` answers 507 with the cap, rather than the timeout's 504.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuerySpillExceeded {
    pub cap_bytes: u64,
}

impl std::fmt::Display for QuerySpillExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "query spilled more than {} MB to temporary storage on the read-only SQL surface; \
             narrow it, or raise analytics.max_temp_size",
            self.cap_bytes / (1024 * 1024)
        )
    }
}

impl std::error::Error for QuerySpillExceeded {}

/// How often the `/sql` watchdog measures a running query's spill directory.
const SPILL_POLL: Duration = Duration::from_millis(250);

/// The error for a query the watchdog interrupted: the spill cap when that was what it hit, else the
/// deadline.
fn stopped(guard: Option<QueryGuard>, spilled: &AtomicU64) -> anyhow::Error {
    match spilled.load(Ordering::SeqCst) {
        0 => QueryBudgetExceeded {
            secs: guard.map(|g| g.timeout.as_secs()).unwrap_or(0),
        }
        .into(),
        cap_bytes => QuerySpillExceeded { cap_bytes }.into(),
    }
}

/// Bytes allocated under a spill directory, counted by blocks so a sparse file is not overcounted.
fn spilled_bytes(dir: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.blocks().saturating_mul(512))
                .sum()
        })
        .unwrap_or(0)
}

/// A resource guard for the untrusted `/sql` surface: a hard wall-clock deadline (enforced by
/// interrupting the running Burrmill query) and a cap on materialised rows. Trusted internal callers
/// (`net_balances`, `get_row`) run *unguarded*. A public `/entity` miss uses [`get_row_guarded`]
/// (#1657). Access control (who may query, per-caller quotas) is deliberately
/// *not* here: that needs caller identity a sovereign single-tenant node doesn't have - it's a
/// gateway's job. This guard is only about the node protecting itself from any single query.
#[derive(Clone, Copy)]
pub struct QueryGuard {
    pub timeout: Duration,
    pub max_rows: usize,
}

/// The result of a query: the rows, plus the two ways they can fail to be the whole answer.
///
/// `truncated` is the caller's own row cap biting. `degraded_tables` is the other one and it is not
/// the caller's doing: the tables whose **cold data was incomplete** when the views were built for
/// this query - a sealed segment the manifest lists but that could not be read, so the view was
/// rebuilt from what remained (#430, #433), or a table whose view could not be defined at all.
///
/// Reduction is the right policy - a bad segment must not delete a table, see [`define_views`] - but
/// it makes the query **succeed** with quietly less data, and `SELECT SUM(value)` then returns a
/// number that is wrong rather than absent (#435). Empty on the healthy path, which is every query
/// on a nest whose segments all match their content addresses.
///
/// Scope note: the views cover every table in the nest, not just the ones this SQL touches, so this
/// over-reports - a bad segment on an untouched table still flags the query. Narrowing it would mean
/// parsing the SQL for table references, which is exactly the kind of guess that produces a
/// confidently wrong answer. Over-reporting *with the names attached* lets the caller judge; silence
/// does not.
///
/// **Which constrains how a surface may word it.** Because this is a property of the nest and not of
/// the answer, a caveat must be a statement about the nest - "this nest could not serve complete cold
/// data for X" - and never about these rows. A query over a healthy table on a nest with one bad
/// segment is complete and correct, and `SELECT 1` and `.tables` have no rows drawn from these tables
/// at all. Nor may it name a cause: the undefinable-view arm above lands here with every segment
/// binding fine. Both mistakes shipped in the first rendering of this field and neither test nor
/// mutation could see them, because every fixture had exactly one table.
///
/// `tip_unavailable` is the other kind of incomplete, and deliberately not folded into
/// `degraded_tables` (#472). A hot-scan failure (`begin_read`, `open_table`, `t.iter()`, or a row
/// partway through) is not per-table the way a bad segment is - it drops the *entire* unsealed tip,
/// every table at once - and its cause and remedy differ: a damaged or unreadable hot store, not a
/// corrupt segment. Shoehorning it into `degraded_tables` would either name every table for a failure
/// that named none of them, or name none and repeat #472's silence. `QueryOutput` itself never sets
/// this field - the hot scan happens in the caller, above `query_hot_cold` - so a caller that scans the
/// tip assigns it after the query returns.
#[derive(Debug, Default, Clone)]
pub struct QueryOutput {
    pub rows: Vec<Value>,
    /// The statement's columns in its projection order (#1609). Each row is a `serde_json::Map`,
    /// which sorts its keys, so this is the only place that order survives.
    pub columns: Vec<String>,
    pub truncated: bool,
    pub degraded_tables: std::collections::BTreeSet<String>,
    pub tip_unavailable: bool,
    /// The base tables this statement referenced, lowercased, or `None` where the parse was
    /// unavailable and the answer is therefore not known.
    ///
    /// Comes from the same security walk that `reject_unknown_table_refs` already performs - one
    /// parse, one answer about what a query reaches, so the control and the provenance can never
    /// disagree. `/sql` uses it to name which **maintained relations** answered (#822 criterion 9);
    /// `None` leaves that block off the response entirely rather than reporting an empty set, since
    /// "we did not parse it" and "it touched no entity" are different facts.
    pub referenced_tables: Option<std::collections::BTreeSet<String>>,
    /// The offchain views this answer read (#1437), each with the content hashes of the snapshots
    /// it was defined over, or `None` where that is not known. Such an answer is reproducible by
    /// snapshot, not re-derivable from chain (RFC-0045 §6).
    pub offchain: Option<std::collections::BTreeMap<String, Vec<String>>>,
    /// The bound a declared query was admitted against; `None` on every other path.
    pub scan_bound: Option<ScanBound>,
}

impl QueryOutput {
    /// Everything but the rows, for a holder that keeps them in another form.
    pub fn without_rows(&self) -> QueryOutput {
        QueryOutput {
            rows: Vec::new(),
            columns: self.columns.clone(),
            truncated: self.truncated,
            degraded_tables: self.degraded_tables.clone(),
            tip_unavailable: self.tip_unavailable,
            referenced_tables: self.referenced_tables.clone(),
            offchain: self.offchain.clone(),
            scan_bound: self.scan_bound.clone(),
        }
    }

    /// Whether any table's cold data was incomplete for this query. The one-bit form of
    /// `degraded_tables`, for surfaces with room to say only yes or no.
    pub fn degraded(&self) -> bool {
        !self.degraded_tables.is_empty()
    }
}

/// Hot (unsealed) rows grouped by logical table - from [`crate::store::Store::hot_rows_by_table`].
/// Passed to the query path so the live tip is `UNION ALL`'d into each table's view (RFC-0013).
pub type HotRows = std::collections::HashMap<String, Vec<Value>>;

/// One reachable table's sealed segments, as bound into the views of the connection that plans the
/// statement (RFC-0048 item 1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct TableScan {
    pub segments: u64,
    pub bytes: u64,
}

/// The source-byte bound a declared query was admitted against (RFC-0048 §3 Phase 0).
///
/// Taken from the physical plan of the connection that runs the statement, after its hot rows are
/// loaded, so it describes the plan that executes. The plan is read for how many cold scans it has,
/// not which files each reads, so every scan is charged the widest reachable table: a self-join pays
/// twice, and no scan can be charged less than the table it might be reading.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ScanBound {
    /// sha256 of the `manifest.json` bytes the views were built from; `None` before any seal.
    pub catalogue_hash: Option<String>,
    pub scan_operators: u64,
    pub cold_bytes: u64,
    /// Serialized hot-store values and maintained relation rows copied into the connection.
    pub hot_bytes: u64,
    pub cap: u64,
    /// Reachable tables with sealed segments.
    pub tables: std::collections::BTreeMap<String, TableScan>,
}

/// What the serving path asks of a declared query.
#[derive(Debug, Clone)]
pub struct NamedAdmission {
    pub cap: u64,
    pub hot_bytes: u64,
    /// The catalogue a quote named. Any other catalogue refuses rather than serves.
    pub pinned_catalogue: Option<Option<String>>,
    /// `false` plans and bounds the statement without evaluating it: a quote.
    pub execute: bool,
}

/// Why a declared query was not admitted. The statement was not evaluated in any of these.
#[derive(Debug)]
pub enum AdmissionRefusal {
    OverCap(ScanBound),
    Unboundable(String),
    StaleCatalogue {
        quoted: Option<String>,
        current: Option<String>,
    },
}

impl std::fmt::Display for AdmissionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OverCap(bound) => write!(
                f,
                "named query's scan bound is {} cold + {} hot source bytes, over the {}-byte admission cap",
                bound.cold_bytes, bound.hot_bytes, bound.cap
            ),
            Self::Unboundable(why) => write!(f, "cannot bound this named query: {why}"),
            Self::StaleCatalogue { .. } => {
                write!(f, "the quoted catalogue is no longer current; ask for a new quote")
            }
        }
    }
}

impl std::error::Error for AdmissionRefusal {}

pub(crate) fn unboundable(why: impl Into<String>) -> anyhow::Error {
    AdmissionRefusal::Unboundable(why.into()).into()
}

static SCAN_BOUNDS: OnceLock<Mutex<std::collections::HashMap<(PathBuf, String), ScanBound>>> =
    OnceLock::new();
const SCAN_BOUNDS_CAPACITY: usize = 1024;

/// The bound this statement last planned to against `catalogue_hash`. A reservation only: it sizes
/// the hot budget before the copy, and the plan that executes is bounded again.
pub fn remembered_scan_bound(
    dir: &Path,
    sql: &str,
    catalogue_hash: &Option<String>,
) -> Option<ScanBound> {
    SCAN_BOUNDS
        .get()?
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&(dir.to_path_buf(), sql.to_string()))
        .filter(|bound| &bound.catalogue_hash == catalogue_hash)
        .cloned()
}

fn remember_scan_bound(dir: &Path, sql: &str, bound: &ScanBound) {
    let mut bounds = SCAN_BOUNDS
        .get_or_init(|| Mutex::new(std::collections::HashMap::new()))
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let key = (dir.to_path_buf(), sql.to_string());
    if bounds.len() >= SCAN_BOUNDS_CAPACITY && !bounds.contains_key(&key) {
        bounds.clear();
    }
    bounds.insert(key, bound.clone());
}

/// Bound a declared statement on the connection whose views it will run against (RFC-0048 §3).
#[allow(clippy::too_many_arguments)]
fn named_scan_bound(
    session: &dyn Session,
    dir: &Path,
    sql: &str,
    admission: &NamedAdmission,
    surveys: bool,
    wanted: Option<&std::collections::BTreeSet<String>>,
    defined: &DefinedViews,
) -> Result<ScanBound> {
    if let Some(quoted) = &admission.pinned_catalogue {
        if *quoted != defined.catalogue_hash {
            return Err(AdmissionRefusal::StaleCatalogue {
                quoted: quoted.clone(),
                current: defined.catalogue_hash.clone(),
            }
            .into());
        }
    }
    if surveys {
        return Err(unboundable("it surveys the catalogue"));
    }
    let Some(wanted) = wanted else {
        return Err(unboundable(
            "the relations it reads could not be determined",
        ));
    };
    let catalogued: std::collections::BTreeSet<String> = defined
        .tables
        .keys()
        .map(|t| t.to_ascii_lowercase())
        .collect();
    let authored: std::collections::BTreeMap<String, String> = nest_view_files(dir)
        .iter()
        .flat_map(|file| split_sql_statements(&file.sql))
        .filter_map(|statement| Some((view_name(&statement)?, view_body(&statement)?.to_string())))
        .collect();
    for name in wanted {
        if catalogued.contains(name) {
            continue;
        }
        if let Some(body) = authored.get(name) {
            // A view that reads files itself is a Parquet scan no catalogue table accounts for.
            let functions = table_refs_in(session, body, "TABLE_FUNCTION")
                .ok_or_else(|| unboundable(format!("view {name} will not parse")))?;
            if let Some(f) = functions
                .iter()
                .find(|f| !ALLOWED_TABLE_FNS.contains(&f.as_str()))
            {
                return Err(unboundable(format!("view {name} calls {f}")));
            }
            continue;
        }
        // Neither a table nor an authored view: a CTE, unless the connection holds a relation by
        // that name (labels, offchain snapshots, factory children), which has no catalogue bound.
        if session.has_relation(name) {
            return Err(unboundable(format!(
                "it reads {name}, which the catalogue does not account for"
            )));
        }
    }
    let scans = session.cold_scan_operators(sql)?;
    let tables: std::collections::BTreeMap<String, TableScan> = defined
        .tables
        .iter()
        .filter(|(table, scan)| scan.segments > 0 && wanted.contains(&table.to_ascii_lowercase()))
        .map(|(table, scan)| (table.clone(), *scan))
        .collect();
    let widest = tables.values().map(|scan| scan.bytes).max().unwrap_or(0);
    if scans > 0 && tables.is_empty() {
        return Err(unboundable(
            "a Parquet scan reads no catalogue table it can be charged to",
        ));
    }
    let cold_bytes = widest
        .checked_mul(scans)
        .ok_or_else(|| unboundable("the cold bound overflows"))?;
    Ok(ScanBound {
        catalogue_hash: defined.catalogue_hash.clone(),
        scan_operators: scans,
        cold_bytes,
        hot_bytes: admission.hot_bytes,
        cap: admission.cap,
        tables,
    })
}

/// **The nest-wide corruption sweep.** Which of this nest's tables have sealed segments that will
/// not bind, whether or not anybody has asked about them.
///
/// This used to be a side effect of every query: `define_views` bound *every* table in the manifest
/// on every request, so a query about one table happened to discover corruption in another. That was
/// issue #477's contract and it was paid for at ~62 µs per sealed segment per request - 2.5 seconds
/// on a 38,428-segment nest, before the query read a row (#896).
///
/// So the discovery moved here, where it can run on a cadence and be reported by `/ready` without a
/// caller having to stumble into it. A query still degrades correctly on a table *it* reads; what it
/// no longer does is survey the rest of the nest on the caller's time.
///
/// Deliberately opens its own connection rather than borrowing the cached one: this defines every
/// view, which is exactly what the cached connection is now avoiding, and leaving that behind on a
/// pooled connection would hand the next query a catalogue full of definitions it did not ask for.
pub fn degraded_tables(
    dir: &Path,
    declared: &[crate::registry::TableSchema],
) -> Result<std::collections::BTreeSet<String>> {
    let session = open_session(dir).context("failed to open a session for the segment sweep")?;
    define_views(
        &*session,
        dir,
        &HotRows::new(),
        u64::MAX,
        &Default::default(),
        declared,
        None,
    )
}

/// Run a read-only query to completion. Only SELECT/WITH statements are accepted - this is a query
/// surface, not a mutation surface. Unguarded: for trusted, registry-built SQL that must finish.
pub fn query(dir: &Path, sql: &str) -> Result<Vec<Value>> {
    #[cfg(test)]
    QUERIES.with(|n| n.set(n.get() + 1));
    Ok(run(
        dir,
        sql,
        None,
        &HotRows::new(),
        u64::MAX,
        &[],
        None,
        None,
        Want::Rows,
    )?
    .rows)
}

// How many trusted queries this thread has run, so a test can hold a point read to its count (#1574).
#[cfg(test)]
thread_local! {
    static QUERIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Run a trusted read-only query over **only the segments finalized at/below `sealed_through`** (the
/// same watermark filter `define_views` applies). The warm-restart view rebuilds use this instead of
/// [`query`] (which reads *every* segment): their cold seed must stay disjoint from the hot replay, and
/// a crash in the seal->prune window leaves already-sealed rows still in the hot store. Folding all
/// segments here would then count those rows twice - permanently double-counting balances and the
/// compliance exposure/velocity views. Bounding to the persisted watermark keeps cold (<= watermark)
/// and hot (everything still in the store) partitioned regardless of crash timing.
fn query_cold(dir: &Path, sql: &str, sealed_through: u64) -> Result<Vec<Value>> {
    Ok(run(
        dir,
        sql,
        None,
        &HotRows::new(),
        sealed_through,
        &[],
        None,
        None,
        Want::Rows,
    )?
    .rows)
}

/// Run a read-only query under a resource guard, over the **sealed segments only** - the cold path used
/// by trusted callers and the `/table` endpoint's cold fill (which merges hot itself). See [`QueryGuard`].
pub fn query_guarded(dir: &Path, sql: &str, guard: QueryGuard) -> Result<QueryOutput> {
    // Cold-only: `u64::MAX` includes every sealed segment (no hot rows to keep disjoint from).
    run(
        dir,
        sql,
        Some(guard),
        &HotRows::new(),
        u64::MAX,
        &[],
        None,
        None,
        Want::Rows,
    )
}

/// Run a guarded read-only query over the sealed segments **and the hot tip** - the public `/sql`
/// surface (RFC-0013). `hot` is the unsealed rows grouped by table; each is `UNION ALL`'d into its
/// table's view. A query outliving `guard.timeout` is interrupted; a result past `guard.max_rows` is
/// truncated and flagged.
///
/// `declared` is the live, registry-derived schema (`indexer::full_schema`) - see `define_views` (#663)
/// for why a table this lists gets an empty view even when `schema.json` on disk has fallen behind it.
pub fn query_hot_cold(
    dir: &Path,
    sql: &str,
    guard: QueryGuard,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
) -> Result<QueryOutput> {
    run(
        dir,
        sql,
        Some(guard),
        hot,
        sealed_through,
        declared,
        None,
        None,
        Want::Rows,
    )
}

/// [`query_hot_cold`]'s relations and checks, but the statement is planned and not run: `/explain`.
/// The statement is planned as written, because wrapping it in a derived table re-binds its columns
/// and refuses some that `/sql` answers (#1775).
pub fn plan_hot_cold(
    dir: &Path,
    sql: &str,
    guard: QueryGuard,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
) -> Result<QueryOutput> {
    run(
        dir,
        sql,
        Some(guard),
        hot,
        sealed_through,
        declared,
        None,
        None,
        Want::Plan,
    )
}

/// Whether a statement is run for its rows, only planned, or written to a maintained view's copy.
#[derive(Clone, Copy)]
enum Want<'a> {
    Rows,
    Plan,
    Write {
        view: &'a str,
        id: &'a str,
        to: &'a Path,
    },
}

/// The inputs of a maintained view moved between the request that asked for its copy and the build:
/// that identity is no longer anyone's, which is not a fault.
#[derive(Debug)]
pub(crate) struct InputsMoved;

impl std::fmt::Display for InputsMoved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the view's inputs moved before its copy was written")
    }
}

impl std::error::Error for InputsMoved {}

/// Evaluate the maintained view `view` exactly as a request reaching it would, and write its rows to
/// `to` (RFC-0062 §3.4). Refused unless the inputs it reads hash to `id` before and after.
pub(crate) fn write_maintained(
    dir: &Path,
    view: &str,
    id: &str,
    to: &Path,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
) -> Result<()> {
    run(
        dir,
        &format!("SELECT * FROM \"{view}\""),
        None,
        hot,
        sealed_through,
        declared,
        None,
        None,
        Want::Write { view, id, to },
    )
    .map(|_| ())
}

/// The identity of `view` at these inputs and the closure it was taken over, or `None` when the view
/// cannot be maintained as its files now stand or its inputs cannot be read.
fn maintained_identity(
    session: &dyn Session,
    dir: &Path,
    view: &str,
    files: &std::collections::BTreeMap<PathBuf, InputStamp>,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
) -> Option<(String, std::collections::BTreeSet<String>)> {
    let closure = view_closure(session, dir, view);
    // Checked per request as well as at load: the files can be edited under a running nest.
    if let Some(why) = crate::maintained::refusal(
        view,
        closure.as_ref(),
        &nest_view_bodies(dir),
        &declared_relations(dir),
    ) {
        tracing::debug!("maintained view {view} answers from its definition: {why}");
        return None;
    }
    let closure = closure?;
    let sealed = crate::maintained::sealed_digest(dir, sealed_through, &closure)?;
    let schema = crate::maintained::schema_digest(dir, declared, &closure);
    let engine = session.engine_version();
    let id = crate::maintained::Inputs {
        view,
        engine: &engine,
        files,
        schema: &schema,
        sealed: &sealed,
        closure: &closure,
        hot,
    }
    .identity(dir)?;
    Some((id, closure))
}

/// Each of `views` that can be maintained, with its identity and closure at these inputs: what a
/// request reaching it would compute, for the eager builder (RFC-0062 S2).
pub(crate) fn maintained_identities(
    dir: &Path,
    views: &std::collections::BTreeSet<String>,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
) -> Result<Vec<(String, String, std::collections::BTreeSet<String>)>> {
    let session = engine().open_bare()?;
    let files = cache_inputs(dir);
    Ok(views
        .iter()
        .filter_map(|view| {
            let (id, closure) = maintained_identity(
                session.as_ref(),
                dir,
                view,
                &files,
                hot,
                sealed_through,
                declared,
            )?;
            Some((view.clone(), id, closure))
        })
        .collect())
}

/// The build half of [`Want::Write`]: the copy is written only while the inputs still hash to `id`,
/// and kept only if it binds with the view's own columns and types.
#[allow(clippy::too_many_arguments)]
fn write_copy(
    session: &dyn Session,
    dir: &Path,
    (view, id, to): (&str, &str, &Path),
    files: &std::collections::BTreeMap<PathBuf, InputStamp>,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
    degraded: &std::collections::BTreeSet<String>,
) -> Result<Collected, Died> {
    if !degraded.is_empty() {
        return Err(Died::Binding(anyhow::anyhow!(
            "the tables under `{view}` could not be read in full: {}",
            degraded.iter().cloned().collect::<Vec<_>>().join(", ")
        )));
    }
    let current = |files: &std::collections::BTreeMap<PathBuf, InputStamp>| {
        maintained_identity(session, dir, view, files, hot, sealed_through, declared)
            .map(|(id, _)| id)
    };
    if current(files).as_deref() != Some(id) {
        return Err(Died::Binding(InputsMoved.into()));
    }
    session.write_parquet(view, to).map_err(Died::Executing)?;
    // Read back before it is named a copy. Not compared with `describe` of the view: that reports an
    // aggregate's planned type (`sum` of a BIGINT as BIGINT) where execution, and so the file, holds
    // DECIMAL(38,0); the answers agree, `typeof` included.
    const CHECK: &str = "__maintained_copy";
    session
        .bind_snapshots(CHECK, &[to.to_path_buf()])
        .map_err(Died::Executing)?;
    let read_back = session.collect(&format!("SELECT count(*) AS n FROM \"{CHECK}\""), None);
    let _ = session.drop_relation(CHECK);
    read_back?;
    if current(&cache_inputs(dir)).as_deref() != Some(id) {
        return Err(Died::Binding(InputsMoved.into()));
    }
    Ok(Collected {
        rows: Vec::new(),
        columns: Vec::new(),
        truncated: false,
    })
}

/// RFC-0062 §3.3: bind each maintained view the statement reaches to its copy at the identity of
/// this request's inputs, and queue a build for each that has none. Returns the copies bound and the
/// views they stand for, whose bodies are then neither expanded nor defined. With `allowed` false, or
/// on a nest that declares nothing, it binds nothing and the request is today's.
#[allow(clippy::too_many_arguments)]
fn bind_maintained_copies(
    session: &dyn Session,
    dir: &Path,
    referenced: Option<&std::collections::BTreeSet<String>>,
    files: &std::collections::BTreeMap<PathBuf, InputStamp>,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
    allowed: bool,
) -> (Vec<PathBuf>, std::collections::BTreeSet<String>) {
    let mut copies = Vec::new();
    let mut leaves = std::collections::BTreeSet::new();
    let maintained = crate::maintained::declared(dir);
    if maintained.is_empty() {
        return (copies, leaves);
    }
    // A pooled session may hold a copy an earlier request bound under the view's name.
    for v in &maintained {
        let _ = session.drop_relation(v);
    }
    if !allowed {
        return (copies, leaves);
    }
    let Some(reach) = referenced.and_then(|r| reachable_tables(session, dir, r)) else {
        return (copies, leaves);
    };
    for view in maintained.iter().filter(|v| reach.contains(*v)) {
        let Some((id, closure)) =
            maintained_identity(session, dir, view, files, hot, sealed_through, declared)
        else {
            continue;
        };
        let path = crate::maintained::copy_path(dir, view, &id);
        if crate::maintained::servable(dir, &path) {
            match session.bind_snapshots(view, std::slice::from_ref(&path)) {
                Ok(()) => {
                    crate::maintained::note_hit(dir, view);
                    leaves.insert(view.clone());
                    copies.push(path);
                    continue;
                }
                Err(e) => {
                    tracing::debug!("maintained copy {} will not bind: {e:#}", path.display());
                    let _ = session.drop_relation(view);
                    crate::maintained::discard(dir, std::slice::from_ref(&path));
                }
            }
        }
        crate::maintained::note_fallback(dir, view);
        if crate::maintained::wants_build(dir, view, &id) {
            crate::maintained::request_build(crate::maintained::Job {
                dir: dir.to_path_buf(),
                view: view.clone(),
                id,
                hot: hot
                    .iter()
                    .filter(|(t, _)| closure.contains(&t.to_ascii_lowercase()))
                    .map(|(t, rows)| (t.clone(), rows.clone()))
                    .collect(),
                sealed_through,
                declared: declared.to_vec(),
            });
        }
    }
    (copies, leaves)
}

/// Historical evaluation filters stored facts before authored views aggregate them. Filtering the
/// finished entity rows would retain today's balances and merely hide recently-created entities.
/// Callers must supply only block-stamped facts, not current maintained-entity snapshots.
pub fn query_hot_cold_at(
    dir: &Path,
    sql: &str,
    guard: QueryGuard,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
    block: u64,
) -> Result<QueryOutput> {
    run(
        dir,
        sql,
        Some(guard),
        hot,
        sealed_through,
        declared,
        None,
        Some(block),
        Want::Rows,
    )
}

/// [`query_hot_cold`] for a declared query: refused before evaluation when the plan that would run
/// cannot be bounded or exceeds `admission.cap` (RFC-0048 item 2).
pub fn query_named(
    dir: &Path,
    sql: &str,
    guard: QueryGuard,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
    admission: &NamedAdmission,
) -> Result<QueryOutput> {
    run(
        dir,
        sql,
        Some(guard),
        hot,
        sealed_through,
        declared,
        Some(admission),
        None,
        Want::Rows,
    )
}

/// How one attempt at a query ended.
///
/// The distinction that matters for #433 is `DiedExecuting`: a query that fails to **bind** is a
/// question about names - a typo, a missing column, an unknown table - and no corrupt page can cause
/// one, so it must never trigger the integrity sweep. Only a query that bound and then died while
/// reading rows is worth paying for.
///
/// **That split rules out a typo and nothing else, and on its own it does not bound the sweep.** It
/// was claimed here that it did. `SELECT CAST('x' AS INTEGER)` binds, dies executing, names no table
/// and is 27 bytes; measured, it hashed every segment of a healthy nest, once per request, on a
/// surface with no auth and two concurrency permits. What bounds the sweep is `tables` below: the
/// tables the failed query actually referenced, which for that query is none.
enum Attempt {
    Ok(QueryOutput),
    DiedExecuting {
        error: anyhow::Error,
        /// The base tables the query named, lowercased - the reachability bound on the sweep. `None`
        /// when the statement would not parse for its tables, i.e. when we do not know what it reached;
        /// see `run` for why that skips the sweep rather than widening it.
        tables: Option<std::collections::BTreeSet<String>>,
        /// Maintained-view copies the statement read (RFC-0062); any could be what failed.
        copies: Vec<PathBuf>,
    },
}

/// Test-only knob: an artificial delay standing in for a slow first attempt, so a test can drive the
/// deadline shared across both attempts and the sweep well past expiry by the time the sweep runs -
/// distinct from a **fresh** `guard.timeout` recomputed at the sweep call site, which this delay does
/// not touch.
///
/// Keyed by `dir`, not a bare process-global (#529) - `run` is `query_guarded`'s entry point and dozens
/// of unrelated tests call it, so a global read unconditionally in the retry path would delay *any*
/// concurrently running test that also died on its first attempt, for as long as this knob happened to
/// be armed - the same class of cross-test contamination `seal::test_set_sweep_expire_after_checks`
/// was fixed to avoid, just reached through a different knob. Keying by `dir` means only a call
/// against this test's own tempdir ever sees the delay.
#[cfg(test)]
fn test_first_attempt_delays() -> &'static Mutex<HashMap<PathBuf, u64>> {
    static DELAYS: OnceLock<Mutex<HashMap<PathBuf, u64>>> = OnceLock::new();
    DELAYS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// #1162 reproduction: a segment file to remove *after* the plan has named it and *before* the engine
/// executes - the window a seal's fold cleanup lands in on a live nest.
#[cfg(test)]
fn test_remove_after_define() -> &'static Mutex<HashMap<PathBuf, PathBuf>> {
    static HOOK: OnceLock<Mutex<HashMap<PathBuf, PathBuf>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Runs once, right after a plan has read the manifest: the window a fold landing mid-plan hits.
#[cfg(test)]
type AfterManifestRead = Box<dyn FnOnce() + Send>;

#[cfg(test)]
fn test_after_manifest_read() -> &'static Mutex<HashMap<PathBuf, AfterManifestRead>> {
    static HOOK: OnceLock<Mutex<HashMap<PathBuf, AfterManifestRead>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn test_set_after_manifest_read(dir: &Path, f: AfterManifestRead) {
    test_after_manifest_read()
        .lock()
        .unwrap()
        .insert(dir.to_path_buf(), f);
}

#[cfg(test)]
pub(crate) fn test_set_remove_after_define(dir: &Path, file: Option<PathBuf>) {
    let mut m = test_remove_after_define().lock().unwrap();
    match file {
        Some(f) => {
            m.insert(dir.to_path_buf(), f);
        }
        None => {
            m.remove(dir);
        }
    }
}

/// Interrupt handles of the Burrmill statements running now, so a shutdown can stop them rather than
/// drain behind them.
type LiveHandles = Mutex<Vec<(u64, PathBuf, Arc<dyn Interrupt>)>>;

fn live_queries() -> &'static LiveHandles {
    static LIVE: OnceLock<LiveHandles> = OnceLock::new();
    LIVE.get_or_init(|| Mutex::new(Vec::new()))
}

static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);
static LIVE_SEQ: AtomicU64 = AtomicU64::new(0);

struct LiveQuery(u64);

impl LiveQuery {
    fn register(dir: &Path, handle: Arc<dyn Interrupt>) -> LiveQuery {
        let id = LIVE_SEQ.fetch_add(1, Ordering::SeqCst);
        live_queries()
            .lock()
            .unwrap()
            .push((id, dir.to_path_buf(), handle.clone()));
        // A statement that starts after the signal must not run to completion either.
        if SHUTTING_DOWN.load(Ordering::SeqCst) {
            handle.interrupt();
        }
        LiveQuery(id)
    }
}

impl Drop for LiveQuery {
    fn drop(&mut self) {
        live_queries()
            .lock()
            .unwrap()
            .retain(|(id, _, _)| *id != self.0);
    }
}

/// Stop every running statement and refuse to let new ones run. Called from the shutdown signal, before
/// the server drains in-flight requests: a `/sql` still executing kept SIGTERM waiting 5.63 s.
pub fn interrupt_for_shutdown() {
    SHUTTING_DOWN.store(true, Ordering::SeqCst);
    for (_, _, handle) in live_queries().lock().unwrap().iter() {
        handle.interrupt();
    }
}

/// [`interrupt_for_shutdown`] for one dataset and without the latch, so a test cannot stop another
/// test's statements.
#[cfg(test)]
fn interrupt_live_in(dir: &Path) {
    for (_, d, handle) in live_queries().lock().unwrap().iter() {
        if d == dir {
            handle.interrupt();
        }
    }
}

/// A planned segment was missing because the catalogue changed after the plan read it.
#[derive(Debug)]
struct SegmentSetChanged;

impl std::fmt::Display for SegmentSetChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "the sealed segment set changed while this query was being planned; it was not answered \
             from a partial set",
        )
    }
}

impl std::error::Error for SegmentSetChanged {}

/// The plan named a segment file that was gone by the time the engine opened it. On a live nest that
/// is a seal folding a provisional segment: the new file is written, the manifest installed, the old
/// file removed - and a query that planned against the old manifest a moment earlier is still holding
/// the old name (#1162, three of ~40 queries during one backfill). Burrmill reports it as the object
/// store's `not found: No such file or directory`, naming a path under the segments directory.
fn segment_vanished(e: &anyhow::Error) -> bool {
    if e.chain().any(|c| c.is::<SegmentSetChanged>()) {
        return true;
    }
    let text = format!("{e:#}");
    let marker = format!("/{}/", crate::seal::SEGMENTS_DIR);
    text.contains(&marker) && text.contains("No such file or directory")
}

#[cfg(test)]
pub(crate) fn test_set_first_attempt_delay_ms(dir: &Path, ms: u64) {
    if ms == 0 {
        test_first_attempt_delays().lock().unwrap().remove(dir);
    } else {
        test_first_attempt_delays()
            .lock()
            .unwrap()
            .insert(dir.to_path_buf(), ms);
    }
}

#[allow(clippy::too_many_arguments)]
fn run(
    dir: &Path,
    sql: &str,
    guard: Option<QueryGuard>,
    hot: &HotRows,
    sealed_through: u64,
    declared: &[crate::registry::TableSchema],
    named: Option<&NamedAdmission>,
    as_of: Option<u64>,
    want: Want<'_>,
) -> Result<QueryOutput> {
    // One deadline for the whole call, computed once - not a fresh `guard.timeout` handed to each
    // `attempt` (#476). Before this, the watchdog only ever bounded a single `attempt`: the first
    // execution could run a full `timeout`, the sweep between the two attempts ran with nothing
    // watching it at all, and the retry got its own fresh `timeout` on top - up to 2x the advertised
    // budget in query execution alone, plus whatever the sweep cost. Sharing one deadline across both
    // attempts and the sweep makes `guard.timeout` the actual wall-clock ceiling on the whole call.
    let deadline = guard.map(|g| Instant::now() + g.timeout);
    // Held across both attempts and the collect, so no file a plan named is deleted by a fold
    // before the rows are read (`seal::ReadLease`).
    let _lease = crate::seal::read_lease(dir);
    let nothing_excluded = std::collections::BTreeSet::new();
    let mut first = attempt(
        dir,
        sql,
        guard,
        hot,
        sealed_through,
        &nothing_excluded,
        deadline,
        declared,
        named,
        as_of,
        want,
        true,
    );
    // A maintained-view copy is derivable, so one that dies under a statement is answered around
    // from the definition. Only if that succeeds was the copy at fault, and it is then discarded.
    if let Ok(Attempt::DiedExecuting { copies, .. }) = &first {
        if !copies.is_empty() {
            let copies = copies.clone();
            let again = attempt(
                dir,
                sql,
                guard,
                hot,
                sealed_through,
                &nothing_excluded,
                deadline,
                declared,
                named,
                as_of,
                want,
                false,
            );
            if matches!(again, Ok(Attempt::Ok(_))) {
                crate::maintained::discard(dir, &copies);
            }
            first = again;
        }
    }
    // A segment the plan named was gone by execution (#1162). Nothing is corrupt and nothing is
    // missing: a seal replaced the file under the query, and planning again reads the manifest as it
    // now is. Once - a second vanish inside one query is not a race, and the ordinary error then
    // says what happened. The integrity sweep below is for a different fault (a file that is present
    // and wrong) and would find nothing here.
    let vanished = match &first {
        Err(e) => segment_vanished(e),
        Ok(Attempt::DiedExecuting { error, .. }) => segment_vanished(error),
        Ok(Attempt::Ok(_)) => false,
    };
    if vanished {
        if let Some(secs) = timed_out(guard, deadline) {
            return Err(QueryBudgetExceeded { secs }.into());
        }
        tracing::info!(
            "a segment the plan named was gone by execution - a seal replaced it under the query \
             (#1162); planning again against the current manifest"
        );
        return match attempt(
            dir,
            sql,
            guard,
            hot,
            sealed_through,
            &nothing_excluded,
            deadline,
            declared,
            named,
            as_of,
            want,
            true,
        )? {
            Attempt::Ok(out) => Ok(out),
            Attempt::DiedExecuting { error, .. } => Err(error),
        };
    }
    let (e, tables) = match first? {
        Attempt::Ok(out) => return Ok(out),
        Attempt::DiedExecuting { error, tables, .. } => (error, tables),
    };
    #[cfg(test)]
    {
        let ms = test_first_attempt_delays()
            .lock()
            .unwrap()
            .get(dir)
            .copied()
            .unwrap_or(0);
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
    // **A segment that binds but will not read takes the whole query down** (#433). Binding a
    // segment reads its footer, which is where #430's reduction hooks in; corruption that leaves the
    // footer intact and destroys the data region passes that and fails at execution instead, with a
    // Parquet decode error that names no file.
    //
    // The principle #430 established is that a bad segment *reduces* its table rather than deleting
    // it, and it should not stop holding just because the corruption is deeper in the file. So: ask
    // which segments no longer match their content address, and if any do not, rebuild the views
    // without them and answer from what remains. Only once - a second execution failure is not about
    // segment integrity, because we just verified every segment we kept.
    //
    // Ask only about the segments backing the tables this query **named** - the sweep reads and
    // hashes files, and a segment the query never read cannot be what killed it. Bounding it by
    // reachability rather than by a cache is what keeps this affordable without anything that can go
    // stale (a memo keyed on mtime was tried here and was wrong; see `segments_failing_verification`).
    let Some(tables) = tables else {
        // The statement would not parse for table references, so we do not know what it reached.
        // Sweeping everything on the strength of not knowing is how the unbounded version comes back in
        // through the fallback; the query fails with its own error instead, which is what it did
        // before any of this existed. Loud and bounded beats quiet and expensive.
        tracing::warn!(
            "query died executing but its statement could not be parsed for table references - \
             skipping the segment integrity sweep (cold data, if corrupt, is not reduced here)"
        );
        return Err(e);
    };
    // The budget may already be spent by the first attempt alone; say so plainly rather than silently
    // skipping the sweep and returning `e`, which (for a guard-bound caller) could otherwise read as
    // an ordinary query error rather than the timeout it actually is.
    if let Some(secs) = timed_out(guard, deadline) {
        return Err(QueryBudgetExceeded { secs }.into());
    }
    let corrupt = crate::seal::segments_failing_verification(dir, &tables, deadline);
    if corrupt.is_empty() {
        if let Some(secs) = timed_out(guard, deadline) {
            return Err(QueryBudgetExceeded { secs }.into());
        }
        return Err(e);
    }
    match attempt(
        dir,
        sql,
        guard,
        hot,
        sealed_through,
        &corrupt,
        deadline,
        declared,
        named,
        as_of,
        want,
        true,
    )? {
        Attempt::Ok(out) => Ok(out),
        Attempt::DiedExecuting { error, .. } => Err(error),
    }
}

/// `Some(guard.timeout.as_secs())` when `deadline` has already passed, else `None`. Shared by the two
/// places in [`run`] that must turn "we ran out of the shared deadline" into the same wording
/// `attempt`'s own watchdog uses, rather than leaking `e`'s unrelated error text as the reason.
fn timed_out(guard: Option<QueryGuard>, deadline: Option<Instant>) -> Option<u64> {
    if deadline.is_some_and(|d| Instant::now() >= d) {
        Some(guard.map(|g| g.timeout.as_secs()).unwrap_or(0))
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
fn attempt(
    dir: &Path,
    sql: &str,
    guard: Option<QueryGuard>,
    hot: &HotRows,
    sealed_through: u64,
    excluded: &std::collections::BTreeSet<String>,
    deadline: Option<Instant>,
    declared: &[crate::registry::TableSchema],
    named: Option<&NamedAdmission>,
    as_of: Option<u64>,
    want: Want<'_>,
    // RFC-0062: whether a maintained view may be answered from its copy on this attempt.
    copies_allowed: bool,
) -> Result<Attempt> {
    // Check the first *statement keyword*, past any leading whitespace and SQL comments - a query
    // that opens with `-- note` or `/* … */` is still a SELECT. The engine gets the original text.
    let head = strip_leading_sql_comments(sql).to_ascii_lowercase();
    if !(head.starts_with("select") || head.starts_with("with")) {
        bail!("only SELECT/WITH queries are allowed on the read-only SQL surface");
    }
    // Before any walk. A long chain of operators overflows the planner, and the walk that names
    // tables recurses the same way. The engine repeats the bound when it plans.
    if let Err(e) = burrmill::df::check_expr_bounds(sql) {
        bail!("{e}");
    }
    // Read-only is enforced four-deep - do NOT loosen any of these without re-reasoning SEC-7:
    //   1. this leading-keyword gate rejects a *statement* that opens with INSERT/UPDATE/DELETE/COPY/
    //      ATTACH/PRAGMA/…;
    //   2. `reject_with_prefixed_dml` refuses `WITH cte AS (…) INSERT/UPDATE/DELETE/COPY …`. The
    //      leading gate accepts any `WITH`. Whether the engine would parse DML after a CTE list is
    //      the engine's choice, not ours;
    //   3. `reject_statement_stacking` refuses a `;`-stacked second statement. The DuckDB binding
    //      nuthatch once bundled ran `SELECT 1; INSERT …` in one prepare, which made a stacked
    //      `COPY … TO` an arbitrary file write. See that function's docs;
    //   4. the session is a fresh in-memory engine whose only tables are read-only views over
    //      Parquet plus ephemeral hot rows, and Burrmill refuses DDL, DML and COPY before planning.
    // `COPY … TO` (a file write) must *lead* the statement or follow a CTE list, which (1) and (2) block.
    // SEC-2: refuse filesystem/network table functions (`read_text`, `glob`, …, by DuckDB's names) -
    // they read files from inside a plain SELECT, past the keyword gate, and would otherwise leak any
    // file the process can read (e.g. `nuthatch.toml`'s secrets). Burrmill implements none of them and
    // refuses table functions it does not know; this denylist does not rely on that.
    reject_with_prefixed_dml(sql)?;
    reject_statement_stacking(sql)?;
    reject_file_access(sql)?;
    reject_replacement_scan(sql)?;

    // **The allowlist, and the control that is meant to outlive the others** (audit finding 5).
    //
    // Everything above enumerates what is *forbidden*, over a vocabulary an engine grows every
    // release. Against DuckDB that approach was wrong twice: about spelling (`"read_csv"(…)` slipped
    // past a check that expected `(` after whitespace) and about coverage (`read_xlsx`, `st_read`,
    // `iceberg_scan` and friends were never listed). Both failures are silent, and the feedback loop
    // is "someone exploits it".
    //
    // So this asks the parser what the query actually references and permits only what we
    // recognise. A new file-reading function added upstream tomorrow is refused by default, because it
    // is not on the list of things we allow - which is the property the denylist can never have.
    //
    // Kept *beside* the denylist rather than replacing it: two independent controls that must both
    // pass, so a gap in either is covered while this one earns trust.
    //
    // #295: reuse the session when the nest, watermark and exclusion set match. Hot rows still
    // reload below (`define_views`); new sealed segments change `sealed_through` and miss the cache.
    // Taken out of the slot for the query so an interrupt can drop it without fighting the mutex
    // borrow; put back only if the statement was not cancelled underneath us.
    let inputs = cache_inputs(dir);
    let mut slot = session_cache_lock().remove(dir);
    let reusable = slot.as_ref().is_some_and(|c| {
        c.sealed_through == sealed_through
            && c.as_of == as_of
            && c.excluded == *excluded
            && c.inputs == inputs
    });
    if !reusable {
        let session = open_session(dir).context("failed to open an analytics session")?;
        slot = Some(SessionCache {
            dir: dir.to_path_buf(),
            sealed_through,
            as_of,
            excluded: excluded.clone(),
            inputs,
            last_used: SESSION_USE.fetch_add(1, Ordering::Relaxed),
            session,
        });
    }
    let mut slot = slot.expect("just inserted");
    slot.last_used = SESSION_USE.fetch_add(1, Ordering::Relaxed);
    let (referenced, offchain, degraded_tables, interrupted, spilled, outcome, cap, scan) = {
        let session: &dyn Session = slot.session.as_ref();
        session.set_deadline(deadline);
        // The token outlives the previous statement. Clear it before the watchdog or the shutdown
        // latch can arm it, or that interrupt is thrown away on the way into the statement.
        session.interrupt_handle().reset();
        let walked = reject_unknown_table_refs(session, sql)?;
        // No parse means no idea what the statement reaches, and the safe answer to that is "all of
        // it" on both counts.
        let surveys = walked.as_ref().map(|(_, sv)| *sv).unwrap_or(true);
        let referenced = walked.map(|(r, _)| r);
        let (copies, leaves) = bind_maintained_copies(
            session,
            dir,
            referenced.as_ref(),
            &slot.inputs,
            hot,
            sealed_through,
            declared,
            copies_allowed
                && !surveys
                && matches!(want, Want::Rows)
                && named.is_none()
                && as_of.is_none()
                && excluded.is_empty(),
        );
        // Define views only for what this statement can reach (#896). `None` - an unparsed statement
        // or a shape `reachable_tables` will not vouch for - defines everything, as before.
        // A statement that reaches into a catalogue schema, or calls an enumerating table function
        // such as DuckDB's `duckdb_tables()`, is asking *what tables exist* - so every view has to exist
        // for it to answer. Those keep the old whole-nest definition; everything else is narrowed.
        let wanted = if surveys {
            None
        } else {
            referenced
                .as_ref()
                .and_then(|r| reachable_tables_stopping(session, dir, r, &leaves))
        };
        let defined = define_views_bound(
            session,
            dir,
            hot,
            sealed_through,
            excluded,
            declared,
            wanted.as_ref(),
            FactWindow {
                after: None,
                through: as_of,
            },
            false,
        )?;
        let degraded_tables = defined.degraded.clone();
        // A nest can ship derived-entity views (`views/*.sql`) that build on the per-event tables; the
        // analytical `/sql` surface sees them. Point-reads (`net_balances`, `get_row`) deliberately skip
        // this - they only touch the raw per-event tables.
        if leaves.is_empty() {
            define_nest_views(session, dir, wanted.as_ref());
        } else {
            let authored = wanted.as_ref().map(|w| w - &leaves);
            define_nest_views(session, dir, authored.as_ref());
        }
        let offchain = if as_of.is_none() {
            define_offchain_views(session, dir, wanted.as_ref())?
        } else {
            Default::default()
        };
        // The compliance substrate: expose imported label snapshots as a `labels` view so `/sql` (and the
        // internal `cold_exposure` fold) can join against them. Best-effort - no snapshots, no view.
        if as_of.is_none() {
            define_labels_view(session, dir);
        }
        // Factory nests (RFC-0009): a `{template}__children` view over the sealed factory events, so
        // "which pools, discovered when, by which parent" is one query. Best-effort - no factories, no-op.
        define_children_views(session, dir);

        // Every view this query could be reading now exists, so the names it used can be widened to the
        // tables behind them. This is the sweep's reachability bound (see `Attempt`), and it has to
        // happen here rather than beside the security walk: at that point the catalogue was empty.
        let referenced = referenced.map(|names| expand_through_views(session, &names));
        // What the answer read from offchain snapshots (#1437). Unparsed, every defined view counts:
        // an over-reported snapshot label claims less than the answer has, never more.
        let offchain = Some(match &referenced {
            Some(names) => offchain
                .into_iter()
                .filter(|(view, _)| names.contains(&view.to_ascii_lowercase()))
                .collect(),
            None => offchain,
        });

        // Hard wall-clock deadline for the untrusted surface: a watchdog thread interrupts the in-flight
        // query once it outlives `deadline` (a cartesian blow-up can't be stopped by the memory cap
        // alone). `interrupt()` makes the running query fail; we translate that into a clear timeout error
        // below. On normal completion we signal the watchdog so it never fires. Unguarded (trusted)
        // queries skip all of this and run to completion.
        //
        // Waits on `deadline`, not a fresh `guard.timeout`, so a second `attempt` (the #433 reduced retry)
        // only gets whatever's left of the *first* attempt's budget rather than a brand-new full timeout
        // (#476) - `run` computes `deadline` once and threads it through both calls. A deadline already in
        // the past (the sweep between attempts ran long) makes `recv_timeout` fire immediately.
        let interrupted = Arc::new(AtomicBool::new(false));
        // The cap, set before `interrupted`, when it was the spill and not the deadline that stopped it.
        let spilled = Arc::new(AtomicU64::new(0));
        let watchdog = guard.zip(deadline).map(|(_, d)| {
            let handle = session.interrupt_handle();
            let spill = session.spill_limit();
            let (flag, over) = (interrupted.clone(), spilled.clone());
            let (tx, rx) = mpsc::channel::<()>();
            let join = std::thread::spawn(move || loop {
                let remaining = d.saturating_duration_since(Instant::now());
                let tick = match spill {
                    Some(_) => remaining.min(SPILL_POLL),
                    None => remaining,
                };
                // Only a genuine timeout interrupts; a value (normal completion) or a dropped sender
                // (panic) leaves the query alone.
                if !matches!(rx.recv_timeout(tick), Err(mpsc::RecvTimeoutError::Timeout)) {
                    break;
                }
                if let Some((dir, cap)) = &spill {
                    if spilled_bytes(dir) > *cap {
                        over.store(*cap, Ordering::SeqCst);
                        flag.store(true, Ordering::SeqCst);
                        handle.interrupt();
                        break;
                    }
                }
                if Instant::now() >= d {
                    flag.store(true, Ordering::SeqCst);
                    handle.interrupt();
                    break;
                }
            });
            (tx, join)
        });

        // RFC-0048 §3's guard: the plan this connection is about to run, with its hot tables loaded,
        // checked before the statement is evaluated. Under the watchdog, because planning a wide nest
        // is not free.
        let scan = named.map(|admission| {
            let bound = named_scan_bound(
                session,
                dir,
                sql,
                admission,
                surveys,
                wanted.as_ref(),
                &defined,
            )?;
            remember_scan_bound(dir, sql, &bound);
            if bound.cold_bytes.saturating_add(bound.hot_bytes) > admission.cap {
                return Err(AdmissionRefusal::OverCap(bound).into());
            }
            Ok(bound)
        });
        let evaluate = match &scan {
            None => true,
            Some(Ok(_)) => named.is_some_and(|admission| admission.execute),
            Some(Err(_)) => false,
        };

        #[cfg(test)]
        if let Some(f) = test_remove_after_define().lock().unwrap().remove(dir) {
            std::fs::remove_file(&f).expect("test hook: remove the planned segment");
        }
        let cap = guard.map(|g| g.max_rows);
        let _live = LiveQuery::register(dir, session.interrupt_handle());
        let outcome = evaluate.then(|| match want {
            Want::Rows => session.collect(sql, cap),
            Want::Plan => session
                .describe(sql)
                .map(|columns| Collected {
                    rows: Vec::new(),
                    columns: columns.into_iter().map(|(name, _)| name).collect(),
                    truncated: false,
                })
                .map_err(Died::Binding),
            Want::Write { view, id, to } => write_copy(
                session,
                dir,
                (view, id, to),
                &slot.inputs,
                hot,
                sealed_through,
                declared,
                &degraded_tables,
            ),
        });

        // Stop the watchdog before interpreting the result: a value arriving before the deadline makes
        // `recv_timeout` return `Ok`, so it won't interrupt; then join so it can't fire late.
        if let Some((tx, join)) = watchdog {
            let _ = tx.send(());
            let _ = join.join();
        }
        (
            (referenced, copies),
            offchain,
            degraded_tables,
            interrupted,
            spilled,
            outcome,
            cap,
            scan,
        )
    };
    let (referenced, copies) = referenced;
    if interrupted.load(Ordering::SeqCst) {
        drop(slot);
    } else {
        retain_session_cache(session_cache_lock(), slot);
    }

    let scan = match scan {
        Some(Err(_)) if interrupted.load(Ordering::SeqCst) => {
            return Err(stopped(guard, &spilled));
        }
        Some(Err(e)) => return Err(e),
        Some(Ok(bound)) => Some(bound),
        None => None,
    };
    let Some(outcome) = outcome else {
        return Ok(Attempt::Ok(QueryOutput {
            degraded_tables,
            referenced_tables: referenced,
            offchain,
            scan_bound: scan,
            ..Default::default()
        }));
    };
    let Collected {
        mut rows,
        columns,
        truncated: over_cap,
    } = match outcome {
        Ok(v) => v,
        // #529: the watchdog's `interrupt()` stops whatever phase is running, not just an in-flight
        // execute - a query that gets no further than planning before the deadline fires still dies
        // to it, and under DuckDB did so leaking the raw "Interrupted!" text here (`Died::Binding`
        // never checked `interrupted`, only `Died::Executing` did). Invisible under light load; a
        // heavily contended box can stall planning past the budget, at which point the untrusted
        // `/sql` surface must say "query exceeded budget" rather than an internal engine string.
        Err(Died::Binding(e)) => {
            if interrupted.load(Ordering::SeqCst) {
                return Err(stopped(guard, &spilled));
            }
            // A copy is planned lazily, so one that will not read can fail here too; `run` answers
            // around it from the definition, where a fault of the statement's own fails again.
            if !copies.is_empty() {
                return Ok(Attempt::DiedExecuting {
                    error: e,
                    tables: referenced,
                    copies,
                });
            }
            return Err(e);
        }
        Err(Died::Executing(e)) => {
            if interrupted.load(Ordering::SeqCst) {
                return Err(stopped(guard, &spilled));
            }
            // Handed back rather than returned: the caller decides whether a corrupt segment explains
            // it and is worth one reduced retry (#433). The tables ride along because they come from
            // the security walk that already ran above - one parse, one answer about what this query
            // reaches, used by both controls.
            return Ok(Attempt::DiedExecuting {
                error: e,
                tables: referenced,
                copies,
            });
        }
    };

    let truncated = match cap {
        Some(max) if over_cap => {
            rows.truncate(max);
            true
        }
        _ => false,
    };
    Ok(Attempt::Ok(QueryOutput {
        rows,
        columns,
        truncated,
        degraded_tables,
        tip_unavailable: false,
        referenced_tables: referenced,
        scan_bound: scan,
        offchain,
    }))
}

/// Skip leading whitespace and SQL comments (`-- line` and `/* block */`) so the read-only guard
/// sees the first real keyword. Returns the remainder starting at that keyword.
fn strip_leading_sql_comments(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = match rest.find('\n') {
                Some(i) => rest[i + 1..].trim_start(),
                None => "",
            };
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = match rest.find("*/") {
                Some(i) => rest[i + 2..].trim_start(),
                None => "",
            };
        } else {
            return s;
        }
    }
}

/// Table functions that read the filesystem or network, by DuckDB's names, which is where these
/// findings were made - usable inside a plain SELECT, so the read-only keyword gate doesn't stop them
/// (SEC-2). Legit `/sql` hits the per-table views, never these.
const FORBIDDEN_FNS: &[&str] = &[
    "read_text",
    "read_blob",
    "read_csv",
    "read_csv_auto",
    "read_json",
    "read_json_auto",
    "read_json_objects",
    "read_ndjson",
    "read_parquet",
    "parquet_scan",
    "parquet_metadata",
    "parquet_schema",
    "parquet_kv_metadata",
    "csv_scan",
    "glob",
    "sniff_csv",
    // **Audit finding 4**: extension-gated readers, which were inert under DuckDB only because their
    // extensions were not bundled - safe by build configuration rather than by policy. The AST
    // allowlist already refuses them; listing them keeps the two controls agreeing.
    "read_xlsx",
    "st_read",
    "st_readosm",
    "iceberg_scan",
    "delta_scan",
    "postgres_scan",
    "postgres_query",
    "sqlite_scan",
    "mysql_scan",
    "mysql_query",
    // **Audit finding 2**: environment disclosure. Measured on an untrusted `/sql` under DuckDB, these
    // returned the absolute `secret_directory` (which embeds the OS username), the temp and extension
    // directories, and the exact state of the sandbox. Not a file read - free reconnaissance for
    // someone looking for one, and there is no legitimate reason a nest query needs them.
    "duckdb_settings",
    "duckdb_extensions",
    "duckdb_secrets",
    "duckdb_databases",
    "duckdb_temporary_files",
    "getenv",
];

/// Strip all SQL comments (line `--…` and block `/* … */`) so a function call can't be split or hidden
/// by a comment before the denylist scan. Deliberately naive about string literals - over-stripping a
/// query with `--`/`/*` inside a string just makes it invalid (rejected), which is the safe direction.
fn strip_all_sql_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'-' && i + 1 < b.len() && b[i + 1] == b'-' {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i += 2;
            out.push(' ');
        } else {
            // Whole characters: a byte pushed as a char turns `é` into two, and every byte offset the
            // checks downstream take stops lining up with the text.
            let c = sql[i..].chars().next().expect("i is on a char boundary");
            out.push(c);
            i += c.len_utf8();
        }
    }
    out
}

/// Refuse `WITH cte AS (…) INSERT/UPDATE/DELETE/COPY …` (SEC-7).
///
/// The leading-keyword gate accepts any statement that opens with `WITH`. A CTE list is only
/// prefix; the actual statement follows the last `AS (subquery)`. That statement must be SELECT
/// (or VALUES / TABLE, which SQL treats as a query). Anything else is DML or DDL riding a
/// prefix the keyword gate already blessed.
///
/// String-literal and identifier aware, same as [`reject_statement_stacking`]: `WITH t AS
/// (SELECT 'INSERT') SELECT 1` is a query, `WITH t AS (SELECT 1) INSERT INTO t SELECT 1` is not.
/// Comments are stripped first. A CTE list we cannot parse is refused rather than handed to
/// the engine - fail closed, the same direction as a `;` inside an unparsed `$$` block.
fn reject_with_prefixed_dml(sql: &str) -> Result<()> {
    let cleaned = strip_all_sql_comments(sql);
    let head = cleaned.trim_start();
    if !sql_keyword_at(head, "with") {
        return Ok(());
    }
    let Some(rest) = skip_ctes(head) else {
        bail!("only SELECT/WITH queries are allowed on the read-only SQL surface");
    };
    let rest = rest.trim_start();
    if sql_keyword_at(rest, "select")
        || sql_keyword_at(rest, "values")
        || sql_keyword_at(rest, "table")
    {
        return Ok(());
    }
    bail!(
        "WITH-prefixed DML is not allowed on the read-only SQL surface \
         (WITH … INSERT/UPDATE/DELETE/COPY)"
    )
}

/// True when `s` opens with `kw` as a whole SQL word (case-insensitive, not `without` for `with`).
fn sql_keyword_at(s: &str, kw: &str) -> bool {
    let s = s.trim_start();
    match s.get(..kw.len()) {
        Some(head) if head.eq_ignore_ascii_case(kw) => {}
        _ => return false,
    }
    match s[kw.len()..].chars().next() {
        None => true,
        Some(c) => !sql_ident_cont(c),
    }
}

fn sql_ident_cont(c: char) -> bool {
    // The DuckDB dialect the engine parses takes unquoted non-ASCII identifiers (`abéé`), so this
    // must too.
    c.is_alphanumeric() || c == '_'
}

/// Consume `WITH [RECURSIVE] name AS [(…)] [, name AS (…)]*` and return the remainder.
fn skip_ctes(sql: &str) -> Option<&str> {
    let s = strip_sql_keyword(sql, "with")?;
    let s = strip_sql_keyword(s, "recursive").unwrap_or(s);
    let mut s = s;
    loop {
        let (_, rest) = next_sql_ident(s)?;
        s = rest.trim_start();
        if s.starts_with('(') {
            s = skip_balanced_parens(s)?.trim_start();
        }
        s = strip_sql_keyword(s, "as")?.trim_start();
        if let Some(rest) = strip_sql_keyword(s, "not") {
            s = strip_sql_keyword(rest.trim_start(), "materialized")?.trim_start();
        } else if let Some(rest) = strip_sql_keyword(s, "materialized") {
            s = rest.trim_start();
        }
        if !s.starts_with('(') {
            return None;
        }
        s = skip_balanced_parens(s)?.trim_start();
        if s.starts_with(',') {
            s = s[1..].trim_start();
            continue;
        }
        return Some(s);
    }
}

fn strip_sql_keyword<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let s = s.trim_start();
    if !sql_keyword_at(s, kw) {
        return None;
    }
    Some(&s[kw.len()..])
}

/// Next SQL identifier: a quoted `"name"` (with `""` escapes) or a bare `[A-Za-z0-9_]+`.
fn next_sql_ident(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    let b = s.as_bytes();
    if b.first() == Some(&b'"') {
        let mut i = 1;
        while i < b.len() {
            if b[i] == b'"' {
                if i + 1 < b.len() && b[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                return Some((&s[..=i], &s[i + 1..]));
            }
            i += 1;
        }
        return None;
    }
    let end = s.find(|c: char| !sql_ident_cont(c)).unwrap_or(s.len());
    if end == 0 {
        return None;
    }
    Some((&s[..end], &s[end..]))
}

/// `s` starts with `(`. Return the suffix after the matching `)`, string-aware.
fn skip_balanced_parens(s: &str) -> Option<&str> {
    let b = s.as_bytes();
    if b.first() != Some(&b'(') {
        return None;
    }
    let mut i = 0;
    let mut depth = 0;
    let (mut in_single, mut in_double) = (false, false);
    while i < b.len() {
        match b[i] {
            b'\'' if !in_double => {
                if in_single && i + 1 < b.len() && b[i + 1] == b'\'' {
                    i += 1;
                } else {
                    in_single = !in_single;
                }
            }
            b'"' if !in_single => in_double = !in_double,
            b'(' if !in_single && !in_double => depth += 1,
            b')' if !in_single && !in_double => {
                depth -= 1;
                if depth == 0 {
                    return Some(&s[i + 1..]);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Refuse a `;`-stacked second statement (SEC-7, and a **real** hole found by the audit-tail test work).
///
/// The read-only story used to rest on three layers, and the second one did not exist:
///   1. the leading-keyword gate inspects only the FIRST statement, so `SELECT 1; COPY …` sails past it;
///   2. `conn.prepare` was documented as single-statement. **It was not.** The duckdb-rs bundled then
///      prepared `SELECT 1; INSERT INTO t VALUES (99)` happily and *executed the INSERT*;
///   3. the in-memory connection has no durable tables - but `COPY … TO 'path'` and `ATTACH 'path'`
///      write to the filesystem regardless of what the connection holds.
///
/// Composed, that was an arbitrary **file-write** primitive on an unauthenticated GET surface:
/// `SELECT 1; COPY (SELECT 1) TO '/home/user/.zshrc'` wrote the file. Verified end-to-end through
/// `query` before this guard existed.
///
/// So statement stacking is rejected here, in our own code, rather than delegated to an engine
/// behaviour we do not control; Burrmill also refuses a second statement, and this does not rely on
/// it. A trailing `;` (with only whitespace after it) is fine - that is how people habitually end a
/// query - but anything following one is refused.
///
/// String-literal aware, because `SELECT ';'` is a perfectly legal query. Single quotes with `''`
/// escaping and double-quoted identifiers are both tracked. Dollar-quoting is NOT parsed: a `;` inside
/// a `$$…$$` block is treated as a statement separator and refused, which fails safe.
fn reject_statement_stacking(sql: &str) -> Result<()> {
    let cleaned = strip_all_sql_comments(sql);
    let b = cleaned.as_bytes();
    let mut i = 0;
    let (mut in_single, mut in_double) = (false, false);
    while i < b.len() {
        match b[i] {
            b'\'' if !in_double => {
                // `''` inside a literal is an escaped quote, not a terminator.
                if in_single && i + 1 < b.len() && b[i + 1] == b'\'' {
                    i += 1;
                } else {
                    in_single = !in_single;
                }
            }
            b'"' if !in_single => in_double = !in_double,
            b';' if !in_single && !in_double => {
                if cleaned[i + 1..].trim().is_empty() {
                    return Ok(()); // a trailing semicolon, nothing behind it
                }
                bail!(
                    "the read-only SQL surface accepts a single statement; `;`-stacking is refused \
                     (only the first statement is checked for read-only-ness, so a stacked second one \
                     could write files)"
                );
            }
            _ => {}
        }
        i += 1;
    }
    Ok(())
}

/// Table functions a query may legitimately call. Everything else is refused.
///
/// Deliberately tiny. Nuthatch's data reaches a query through views *we* define, so a user query needs
/// no table function at all to do its job - these exist because ordinary analytical SQL uses them for
/// generating rows, not for reaching data. Adding to this list means asserting a function cannot read a
/// file, open a socket, or leak the environment.
pub(crate) const ALLOWED_TABLE_FNS: &[&str] = &["generate_series", "range", "unnest"];

/// Ask the engine's parser what the statement references, and refuse anything unrecognised
/// (Burrmill's `inspect::reach`, which carries these rules).
///
/// - A **table function** must be in [`ALLOWED_TABLE_FNS`]. Quoting collapses here for free:
///   `read_csv(…)` and `"read_csv"(…)` parse to the same name, so the evasion that defeated the
///   textual denylist is not expressible.
/// - A **base table** must be named like an identifier. A path in table position (`FROM
///   '/x.parquet'`, a DuckDB replacement scan) is a table name to a parser; requiring `[A-Za-z0-9_]`
///   refuses it, and no legitimate view of ours is named otherwise.
///
/// Fails **open** if the statement will not parse: this is the newer of two controls, and a parse
/// failure must not take down `/sql` while the denylist - which has guarded this surface since
/// RFC-0008 - is still in front of it.
///
/// Returns the **base tables the statement referenced**, lowercased (identifiers match
/// case-insensitively), or `None` where the parse was unavailable and the answer is therefore not
/// known. That set is the integrity sweep's reachability bound (see [`Attempt`]), and it comes from
/// this walk rather than a second one so the two can never disagree about what a query reaches. CTE
/// names parse as `BASE_TABLE` too and are included; a CTE named after a real table can only widen
/// the set to a table that exists, which is the safe direction.
fn reject_unknown_table_refs(
    session: &dyn Session,
    sql: &str,
) -> Result<Option<(std::collections::BTreeSet<String>, bool)>> {
    match session.reach(sql) {
        None => Ok(None),
        Some(Err(why)) => Err(why),
        Some(Ok(found)) => Ok(Some(found)),
    }
}

/// Widen the names a query used to the names those names *read*, following the views this connection
/// has just defined.
///
/// Without this, bounding the sweep by reachability would quietly cost authored views their reduction
/// (RFC-0001 `views/*.sql`, plus the generated `labels` and `{template}__children` views): a query
/// over `big_transfers` names `big_transfers`, which is no table in the manifest, so nothing would be
/// verified and a page-corrupt segment under that view would fail the query instead of reducing it -
/// exactly the behaviour #433 is fixing, reintroduced one layer up. Measured, not assumed: the test
/// `a_page_corrupt_segment_under_an_authored_view_still_reduces` fails with this function removed.
///
/// Widening is safe in the direction it goes: it can only add tables the query genuinely reads. The
/// attacker's case is a query that names *nothing*, and nothing expands to nothing.
///
/// Best-effort by design. A view whose definition cannot be re-parsed is skipped rather than treated
/// as "could be anything" - the cost of a miss is a lost reduction on that one query (loud: the query
/// fails with the engine's own error), and the cost of the other choice is the unbounded sweep coming
/// back in through a fallback, which is the defect this whole change exists to remove.
fn expand_through_views(
    session: &dyn Session,
    named: &std::collections::BTreeSet<String>,
) -> std::collections::BTreeSet<String> {
    let Some(listed) = session.view_definitions() else {
        return named.clone();
    };
    let defs: std::collections::BTreeMap<String, String> = listed
        .into_iter()
        .map(|(name, sql)| (name.to_ascii_lowercase(), sql))
        .collect();

    let mut out = named.clone();
    let mut frontier: Vec<String> = named.iter().cloned().collect();
    // Visit each name once. The finite catalogue and `out` bound traversal without
    // silently dropping sources beyond an arbitrary dependency depth.
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for name in frontier.drain(..) {
            let Some(sql) = defs.get(&name) else { continue };
            let Some(body) = view_body(sql) else { continue };
            let Some(inner) = base_tables_in(session, body) else {
                continue;
            };
            for t in inner {
                if out.insert(t.clone()) {
                    next.push(t);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    out
}

/// The `SELECT` inside a stored `CREATE VIEW … AS …`, the body Burrmill's `register_view` takes.
/// `None` when the text is not that shape, which skips the view.
pub(crate) fn view_body(create_view_sql: &str) -> Option<&str> {
    let lower = create_view_sql.to_ascii_lowercase();
    let (view, view_end) = find_keyword(&lower, "view", 0)?;
    let _ = view;
    let (_, as_end) = find_keyword(&lower, "as", view_end)?;
    Some(create_view_sql[as_end..].trim())
}

/// Where `word` appears in `haystack` as a whole token at or after `from`, as `(start, end)`.
///
/// **Bounded by any whitespace, not by spaces.** This was `find(" as ")`, which needs a literal
/// space on both sides - and a real authored view puts a newline after the keyword:
///
/// ```sql
/// CREATE VIEW indexer_rewards AS
/// SELECT "indexer", SUM("tokensRewards_dec")::VARCHAR AS rewards
/// FROM "service__indexing_rewards_collected" GROUP BY "indexer";
/// ```
///
/// That is Lodestar's `views/40-indexers.sql`, and the space-bounded search simply did not find the
/// keyword. Harmless while the only caller was `expand_through_views`, which merely *widens* the
/// integrity sweep's bound and is allowed to be imprecise. It stopped being harmless when #896 made
/// the same parse decide which views get defined at all: the view's source table was never defined,
/// and a view that plainly exists came back as `Catalog Error: Table with name indexer_rewards does
/// not exist`.
fn find_keyword(haystack: &str, word: &str, from: usize) -> Option<(usize, usize)> {
    let bytes = haystack.as_bytes();
    let mut at = from;
    while let Some(i) = haystack[at..].find(word) {
        let start = at + i;
        let end = start + word.len();
        let before_ok = start > 0 && bytes[start - 1].is_ascii_whitespace();
        let after_ok = end < bytes.len() && bytes[end].is_ascii_whitespace();
        if before_ok && after_ok {
            return Some((start, end));
        }
        at = end;
    }
    None
}

/// The name a `CREATE [OR REPLACE] VIEW <name> AS …` declares, lowercased. The mirror of
/// [`view_body`], and parsed the same coarse way: the text between ` view ` and the first ` as `
/// past it.
pub(crate) fn view_name(create_view_sql: &str) -> Option<String> {
    let lower = create_view_sql.to_ascii_lowercase();
    let (_, view_end) = find_keyword(&lower, "view", 0)?;
    let (as_at, _) = find_keyword(&lower, "as", view_end)?;
    let name = lower[view_end..as_at].trim().trim_matches('"').trim();
    (!name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .then(|| name.to_string())
}

/// Which tables a statement can actually reach - the names it uses in table position, widened
/// through any **authored view** among them to the base tables that view reads.
///
/// This is what lets `define_views` skip the rest. Under DuckDB, defining a view cost a parse of SQL
/// text carrying every one of that table's sealed segment paths, and it was being paid for all 34
/// tables of a nest on every request: `SELECT 1` cost 2,465 ms on a 38,428-segment nest against
/// 263 ms on a 2,985-segment one, about 62 µs per segment, for tables the query never named (#896).
///
/// Read from `views/*.sql` **on disk** rather than from the session's view definitions, unlike
/// [`expand_through_views`], for two reasons: `define_nest_views` has not run yet at this point in
/// `run`, and on a pooled session the catalogue may still hold a *previous* request's
/// definitions. The files are the authored truth; the catalogue is a cache of it.
///
/// `None` means "could not work it out", and every caller must then define everything. Returned for
/// a view body that will not parse, and for any `…__children` name: those views are built by
/// `define_children_views` after this point, out of factory tables enumerated from the config, and
/// working out which ones here would be a second copy of that logic to keep in step.
fn reachable_tables(
    session: &dyn Session,
    dir: &Path,
    referenced: &std::collections::BTreeSet<String>,
) -> Option<std::collections::BTreeSet<String>> {
    reachable_tables_stopping(session, dir, referenced, &Default::default())
}

/// [`reachable_tables`], not expanding the bodies of `leaves`: maintained views bound to a copy.
fn reachable_tables_stopping(
    session: &dyn Session,
    dir: &Path,
    referenced: &std::collections::BTreeSet<String>,
    leaves: &std::collections::BTreeSet<String>,
) -> Option<std::collections::BTreeSet<String>> {
    if referenced.iter().any(|n| n.ends_with("__children")) {
        return None;
    }
    let bodies = nest_view_bodies(dir);

    let mut out = referenced.clone();
    let mut frontier: Vec<String> = referenced.iter().cloned().collect();
    // Authored files may contain cycles that the engine will later reject. `out`
    // prevents revisiting them while retaining sources at any dependency depth.
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for name in frontier.drain(..) {
            if leaves.contains(&name) {
                continue;
            }
            let Some(body) = bodies.get(&name) else {
                continue;
            };
            for t in base_tables_in(session, body)? {
                if out.insert(t.clone()) {
                    next.push(t);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    Some(out)
}

/// Every name one authored view reads, through the views it reads, itself included (RFC-0062).
pub(crate) fn view_closure(
    session: &dyn Session,
    dir: &Path,
    view: &str,
) -> Option<std::collections::BTreeSet<String>> {
    reachable_tables(
        session,
        dir,
        &std::collections::BTreeSet::from([view.to_string()]),
    )
}

/// Each authored view's name and body, from `views/*.sql` on disk.
pub(crate) fn nest_view_bodies(dir: &Path) -> std::collections::BTreeMap<String, String> {
    let mut bodies = std::collections::BTreeMap::new();
    for f in nest_view_files(dir) {
        for stmt in split_sql_statements(&f.sql) {
            if let (Some(name), Some(body)) = (view_name(&stmt), view_body(&stmt)) {
                bodies.insert(name, body.to_string());
            }
        }
    }
    bodies
}

/// The base tables a statement reads, lowercased. The security walk collects the same set for the
/// caller's own query; this is for SQL we hand ourselves, like a view's stored definition.
fn base_tables_in(session: &dyn Session, sql: &str) -> Option<std::collections::BTreeSet<String>> {
    table_refs_in(session, sql, "BASE_TABLE")
}

/// The references of one `kind` (`BASE_TABLE`, `TABLE_FUNCTION`) a statement makes, lowercased.
fn table_refs_in(
    session: &dyn Session,
    sql: &str,
    wanted_kind: &str,
) -> Option<std::collections::BTreeSet<String>> {
    let (tables, functions) = session.table_refs(sql)?;
    Some(if wanted_kind == "BASE_TABLE" {
        tables
    } else {
        functions
    })
}

/// Refuse a query that *calls* any [`FORBIDDEN_FNS`] function. Comments are stripped first, then each
/// name is matched only when it's a real call: a word boundary before it and (after optional
/// whitespace) a `(` after it - so a table or column merely *named* like one (e.g. `pool__glob`) is
/// fine, while `read_text/**/('…')` and `READ_TEXT (…)` are both caught. (SEC-2, primary control.)
pub(crate) fn reject_file_access(sql: &str) -> Result<()> {
    // **Double quotes are removed before scanning.** SQL resolves a quoted function name as the bare
    // form, so `"read_csv"('/etc/passwd')` executed while sailing past a check that looked for `(`
    // after optional *whitespace* - a quote is not whitespace. Verified against a live DuckDB during
    // the pre-1.0 adversary pass: the quoted form returned the file's contents.
    //
    // Stripping is the robust fix rather than "also skip quotes when seeking `(`", because it
    // normalises every placement at once - `"read_csv"(`, `read"_"csv(`, and anything else quoting can
    // do to break a name into pieces. It can only ever make the denylist match *more*, and a denylist
    // that over-refuses is the safe direction: the cost is a rejected query with a bizarre quoted
    // identifier, and the alternative cost is reading /etc/passwd.
    let cleaned = strip_all_sql_comments(sql)
        .to_ascii_lowercase()
        .replace('"', "");
    let b = cleaned.as_bytes();
    let is_ident = |c: u8| c == b'_' || c.is_ascii_alphanumeric();
    for name in FORBIDDEN_FNS {
        let mut from = 0;
        while let Some(pos) = cleaned[from..].find(name) {
            let start = from + pos;
            let end = start + name.len();
            let boundary_before = start == 0 || !is_ident(b[start - 1]);
            let mut j = end;
            while j < b.len() && b[j].is_ascii_whitespace() {
                j += 1;
            }
            let is_call = j < b.len() && b[j] == b'(';
            if boundary_before && is_call {
                bail!("query uses forbidden filesystem/network function `{name}` - refused");
            }
            from = end;
        }
    }
    Ok(())
}

/// Refuse a **replacement scan**: under DuckDB a bare string literal in table position (`FROM
/// '/x.parquet'`, `JOIN '…'`) read that file with *no function name* for [`reject_file_access`] to
/// match, bypassing the denylist entirely. Burrmill reads it as an unknown table; this does not rely on
/// that. A legitimate query names a view or a subquery after FROM/JOIN, never
/// a single-quoted string (a double-quoted identifier is fine and untouched) - so rejecting a
/// single-quote as the first non-space token after a word-bounded FROM/JOIN closes the bypass without
/// affecting real queries. Comments are stripped first, mirroring the denylist scan.
pub(crate) fn reject_replacement_scan(sql: &str) -> Result<()> {
    let cleaned = strip_all_sql_comments(sql).to_ascii_lowercase();
    let b = cleaned.as_bytes();
    let is_ident = |c: u8| c == b'_' || c.is_ascii_alphanumeric();
    for kw in ["from", "join"] {
        let mut from = 0;
        while let Some(pos) = cleaned[from..].find(kw) {
            let start = from + pos;
            let end = start + kw.len();
            let boundary_before = start == 0 || !is_ident(b[start - 1]);
            let mut j = end;
            while j < b.len() && b[j].is_ascii_whitespace() {
                j += 1;
            }
            // A single-quote as the first token after a word-bounded FROM/JOIN is a file replacement
            // scan. (If the keyword is part of a larger identifier, the next char is an ident char, not a
            // quote, so this never false-positives on e.g. `fromage`.)
            if boundary_before && j < b.len() && b[j] == b'\'' {
                bail!("query reads a file via a `{kw} '…'` replacement scan - refused");
            }
            from = end;
        }
    }
    Ok(())
}

/// Net balance per address for one sealed transfer table, summed as i128. This is
/// how the IVM view is re-seeded on restart: instead of replaying every sealed transfer through the
/// circuit, we let Burrmill fold each immutable segment down to one (address, net) row. Addresses
/// whose net is exactly zero are omitted (matching the view's drop-at-zero behaviour). `table` and
/// the column names come from the registry (`{alias}__transfer`; from/to/value column names vary by
/// token - USDC from/to/value, WETH src/dst/wad), never user text, so there is no injection surface.
/// Transfers in `table` whose value has more than 38 digits, and which [`net_balances`] therefore
/// dropped (COR-8, #814).
///
/// A separate query rather than a column on the fold: the fold groups by address and sums, so a
/// dropped row has no address to be counted against - it contributes NULL to both legs and vanishes
/// before the `GROUP BY`. Counting it needs its own pass over the same predicate.
///
/// `TRY_CAST` is the same expression the fold uses, deliberately: a second spelling of "does not fit"
/// could disagree with the one doing the dropping, and then the count would describe a different set
/// of rows than the ones actually missing.
pub fn oversized_transfers(
    dir: &Path,
    table: &str,
    value_col: &str,
    sealed_through: u64,
) -> Result<u64> {
    Ok(
        query_cold(dir, &oversized_sql(table, value_col), sealed_through)?
            .first()
            .and_then(|r| r["n"].as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
    )
}

fn oversized_sql(table: &str, value_col: &str) -> String {
    format!(
        "SELECT COUNT(*)::VARCHAR AS n FROM \"{table}\"          WHERE \"{value_col}\" IS NOT NULL AND TRY_CAST(\"{value_col}\" AS DECIMAL(38,0)) IS NULL"
    )
}

/// `expr` as `ty` when it has at most 38 digits, else NULL: the nest's `DECIMAL(38,0)` line (COR-8),
/// spelled so the drop is visible to an engine that refuses to sum a `TRY_CAST` (Burrmill's checked
/// rule). [`crate::views::transfer_value`] draws the same line for the live views.
pub(crate) fn exact_or_null(expr: &str, ty: &str) -> String {
    format!("CASE WHEN TRY_CAST({expr} AS DECIMAL(38,0)) IS NOT NULL THEN CAST({expr} AS {ty}) END")
}

fn net_balances_sql(table: &str, from_col: &str, to_col: &str, value_col: &str) -> String {
    // `to` receives (+value), `from` sends (−value); a value past 38 digits is NULL (skipped),
    // mirroring the live views' `transfer_value`.
    let d = exact_or_null(&format!("\"{value_col}\""), "HUGEINT");
    format!(
        "SELECT addr, SUM(d)::VARCHAR AS net FROM (\
           SELECT \"{to_col}\" AS addr, {d} AS d FROM \"{table}\" \
           UNION ALL \
           SELECT \"{from_col}\" AS addr, -{d} AS d FROM \"{table}\"\
         ) GROUP BY addr HAVING SUM(d) <> 0"
    )
}

fn cold_exposure_sql(table: &str, from_col: &str, to_col: &str, value_col: &str) -> String {
    // Outbound: the sender has exposure to the labels of a labeled recipient. Inbound: the recipient
    // has exposure from the labels of a labeled sender. COUNT/SUM per (address, label, direction).
    let d = exact_or_null(&format!("t.\"{value_col}\""), "HUGEINT");
    format!(
        "SELECT addr, label, dir, SUM(d)::VARCHAR AS amount, COUNT(*) AS cnt FROM (\
           SELECT lower(t.\"{from_col}\") AS addr, l.label AS label, 'out' AS dir, \
                  {d} AS d \
           FROM \"{table}\" t JOIN labels l ON lower(t.\"{to_col}\") = l.address \
           UNION ALL \
           SELECT lower(t.\"{to_col}\") AS addr, l.label, 'in', \
                  {d} AS d \
           FROM \"{table}\" t JOIN labels l ON lower(t.\"{from_col}\") = l.address\
         ) WHERE d IS NOT NULL GROUP BY addr, label, dir"
    )
}

fn cold_velocity_sql(table: &str, from_col: &str, value_col: &str, window: u64) -> String {
    let w = window.max(1);
    // window_start = (block // W) * W; sum outbound volume + count per (sender, window). A value
    // past 38 digits leaves the count too, as the hot replay never feeds that transfer at all.
    let d = exact_or_null(&format!("\"{value_col}\""), "HUGEINT");
    format!(
        "SELECT lower(\"{from_col}\") AS addr, (block_number // {w}) * {w} AS ws, \
                SUM({d})::VARCHAR AS vol, COUNT(*) AS cnt \
         FROM \"{table}\" WHERE {d} IS NOT NULL GROUP BY addr, ws"
    )
}

pub fn net_balances(
    dir: &Path,
    table: &str,
    from_col: &str,
    to_col: &str,
    value_col: &str,
    sealed_through: u64,
) -> Result<Vec<(String, i128)>> {
    let mut out = Vec::new();
    for r in query_cold(
        dir,
        &net_balances_sql(table, from_col, to_col, value_col),
        sealed_through,
    )? {
        if let (Some(addr), Some(net)) = (r["addr"].as_str(), r["net"].as_str()) {
            if let Ok(n) = net.parse::<i128>() {
                out.push((addr.to_string(), n));
            }
        }
    }
    Ok(out)
}

/// Cold exposure fold (RFC-0008 C1): direct counterparty exposure to the labeled set for one sealed
/// transfer table, computed in Burrmill by joining the segments against the `labels` view. Mirrors
/// `net_balances` - it lets a restart re-seed the exposure view from immutable segments instead of
/// replaying every sealed transfer. Returns `(encoded_key, amount, count)` where the key is
/// `address\u{1f}label\u{1f}direction`, matching `exposure::seed_item`. `table`/column names are
/// registry-derived (never user text); addresses are lower-cased to match the label snapshots.
pub fn cold_exposure(
    dir: &Path,
    table: &str,
    from_col: &str,
    to_col: &str,
    value_col: &str,
    sealed_through: u64,
) -> Result<Vec<(String, i128, i128)>> {
    let mut out = Vec::new();
    for r in query_cold(
        dir,
        &cold_exposure_sql(table, from_col, to_col, value_col),
        sealed_through,
    )? {
        let (Some(addr), Some(label), Some(dir_s), Some(cnt)) = (
            r["addr"].as_str(),
            r["label"].as_str(),
            r["dir"].as_str(),
            r["cnt"].as_i64(),
        ) else {
            continue;
        };
        let amount = r["amount"]
            .as_str()
            .and_then(|s| s.parse::<i128>().ok())
            .unwrap_or(0);
        let key = format!("{addr}\u{1f}{label}\u{1f}{dir_s}");
        out.push((key, amount, cnt as i128));
    }
    Ok(out)
}

/// Cold velocity fold (RFC-0008 C3): per-address outbound volume + count per tumbling block-window,
/// summed in Burrmill over one sealed transfer table - the restart re-seed for the velocity view (as
/// `net_balances`/`cold_exposure` are for their views). Returns `(encoded_key, volume, count)` where
/// the key is `address\u{1f}window_start`, matching `velocity::seed_item`. Registry-derived names.
pub fn cold_velocity(
    dir: &Path,
    table: &str,
    from_col: &str,
    value_col: &str,
    window: u64,
    sealed_through: u64,
) -> Result<Vec<(String, i128, i128)>> {
    let mut out = Vec::new();
    for r in query_cold(
        dir,
        &cold_velocity_sql(table, from_col, value_col, window),
        sealed_through,
    )? {
        let (Some(addr), Some(ws), Some(cnt)) =
            (r["addr"].as_str(), r["ws"].as_u64(), r["cnt"].as_i64())
        else {
            continue;
        };
        let vol = r["vol"]
            .as_str()
            .and_then(|s| s.parse::<i128>().ok())
            .unwrap_or(0);
        out.push((format!("{addr}\u{1f}{ws}"), vol, cnt as i128));
    }
    Ok(out)
}

/// Define a read-only `labels` view over the content-addressed snapshots in `dir/labels/*.json`
/// (each a flat JSON array of `{address, label}`). No snapshots → no view, so joins against it are
/// only attempted when labels exist. Addresses are lower-cased for a clean join with decoded hex.
fn define_labels_view(session: &dyn Session, dir: &Path) {
    let labels_dir = dir.join(crate::labels::LABELS_DIR);
    let has_snapshot = std::fs::read_dir(&labels_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .any(|e| e.path().extension().is_some_and(|x| x == "json"))
        })
        .unwrap_or(false);
    if !has_snapshot {
        return;
    }
    if let Err(e) = session.bind_labels(&labels_dir) {
        tracing::debug!("labels view skipped: {e}");
    }
}

/// Define a `{template}__children` view per template for a factory nest (RFC-0009 §Serving): the set
/// of discovered child contracts with their provenance (address, discovered block/log/timestamp,
/// parent), unioned across every factory that produces the template and de-duplicated to the earliest
/// discovery per address. Reads the nest's factory config from `nuthatch.toml`; best-effort, so a
/// factory table with no sealed events yet (only an empty typed view) just yields an empty children
/// view. Non-factory nests are a no-op.
fn define_children_views(session: &dyn Session, dir: &Path) {
    let Ok(config) = crate::config::Config::load(dir) else {
        return;
    };
    if config.factories.is_empty() {
        return;
    }
    let Ok(fs) = crate::factory::FactorySet::build(&config) else {
        return;
    };
    // A timestamp-free nest (RFC-0029 §6b) has no `block_timestamp` to project, so the provenance
    // view drops `discovered_timestamp` rather than selecting a column that doesn't exist. Leaving
    // the reference in would make the whole view fail to create - and the failure is swallowed as a
    // `debug!` below, so a factory nest would silently lose `{template}__children` entirely. Omitted
    // rather than `0 AS discovered_timestamp` for the same reason the column itself is omitted: a
    // zero that looks like a timestamp is worse than an error.
    let (ts, cols) = if config.nest.block_timestamps {
        (
            "block_timestamp AS discovered_timestamp, ",
            "discovered_timestamp, ",
        )
    } else {
        ("", "")
    };

    let mut by_template: std::collections::BTreeMap<String, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    for (template, table, child_param) in fs.view_sources() {
        by_template
            .entry(template)
            .or_default()
            .push((table, child_param));
    }

    for (template, sources) in by_template {
        // `child_param`/`table` are registry-derived (never user text) → no injection surface.
        let selects: Vec<String> = sources
            .iter()
            .map(|(table, cp)| {
                format!(
                    "SELECT lower(\"{cp}\") AS address, block_number AS discovered_block, \
                     log_index AS discovered_log_index, {ts}\
                     lower(address) AS parent_address FROM \"{table}\""
                )
            })
            .collect();
        let union = selects.join(" UNION ALL ");
        let ddl = format!(
            "CREATE OR REPLACE VIEW \"{template}__children\" AS \
             SELECT address, discovered_block, discovered_log_index, {cols}parent_address \
             FROM ({union}) \
             QUALIFY row_number() OVER (PARTITION BY address ORDER BY discovered_block, discovered_log_index) = 1"
        );
        if let Err(e) = session.execute(&ddl) {
            tracing::debug!("children view {template}__children skipped: {e}");
        }
    }
}

/// Point-read fallback: fetch a single sealed transfer by (block, log_index). Used when the hot
/// store has already pruned it. Integers are interpolated (not user text), so no injection surface.
/// Unguarded: trusted callers. The public route is [`get_row_guarded`].
pub fn get_row(dir: &Path, block: u64, log_index: u64) -> Result<Option<Value>> {
    read_sealed(dir, block, log_index, None)
}

/// As [`get_row`], under one deadline for the probe and the read (#1657).
pub fn get_row_guarded(
    dir: &Path,
    block: u64,
    log_index: u64,
    guard: QueryGuard,
) -> Result<Option<Value>> {
    #[cfg(test)]
    ENTITY_GUARDED.fetch_add(1, Ordering::Relaxed);
    read_sealed(dir, block, log_index, Some(guard))
}

#[cfg(test)]
static ENTITY_GUARDED: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn entity_guarded_reads() -> usize {
    ENTITY_GUARDED.load(Ordering::Relaxed)
}

fn read_sealed(
    dir: &Path,
    block: u64,
    log_index: u64,
    mut guard: Option<QueryGuard>,
) -> Result<Option<Value>> {
    let manifest = crate::seal::load_manifest(dir)?;
    // The id names no table. Asking each in turn cost a query per table (#1574), so one probe finds
    // the table and the row is then read from it alone, in the shape a single-table query gives.
    let probe = manifest
        .tables
        .keys()
        .map(|t| {
            format!(
                "SELECT '{}' AS t FROM \"{t}\" WHERE block_number = {block} AND log_index = {log_index}",
                t.replace('\'', "''")
            )
        })
        .collect::<Vec<_>>();
    if probe.is_empty() {
        return Ok(None);
    }
    let started = Instant::now();
    let found = sealed_rows(
        dir,
        &format!("{} ORDER BY t LIMIT 1", probe.join(" UNION ALL ")),
        guard,
    )?;
    // The second read spends what the probe left of the one deadline.
    if let Some(g) = guard.as_mut() {
        g.timeout = g.timeout.saturating_sub(started.elapsed());
    }
    let Some(table) = found.first().and_then(|r| r["t"].as_str()) else {
        return Ok(None);
    };
    let sql = format!(
        "SELECT * FROM \"{table}\" WHERE block_number = {block} AND log_index = {log_index} LIMIT 1"
    );
    Ok(sealed_rows(dir, &sql, guard)?.into_iter().next())
}

fn sealed_rows(dir: &Path, sql: &str, guard: Option<QueryGuard>) -> Result<Vec<Value>> {
    match guard {
        None => query(dir, sql),
        Some(g) => {
            let out = query_guarded(dir, sql, g)?;
            if out.truncated {
                bail!("sealed point read exceeded its row cap");
            }
            Ok(out.rows)
        }
    }
}

/// Expose each table's sealed segments as a read-only Burrmill view named after the table. Tables with
/// no sealed segments yet simply have no view (they hold only unsealed tip data, served from hot).
///
/// Big-integer columns (uint/int > 64 bits) are stored as exact text (canonical form). For ergonomic
/// SQL (RFC-0001 §2) each such column `c` gets two derived view columns: `c_dec` - the value as
/// `DECIMAL(38,0)` when it fits, else NULL - and `c_overflow` - true when the exact value exceeds
/// 38 digits (so `c_dec` is NULL but `c` isn't). Analytics can `SUM(c_dec)` without hand-casting.
///
/// Returns the tables whose cold data ended up **incomplete** - a manifest segment dropped from the
/// view, or a view that could not be defined at all. Every reduction below is already logged, but a
/// log is not reachable by the caller who is about to sum the reduced column, so the same decision is
/// handed back as data and rides out on [`QueryOutput::degraded_tables`] (#435).
fn define_views(
    session: &dyn Session,
    dir: &Path,
    hot: &HotRows,
    sealed_through: u64,
    // Content addresses of segments to leave out of every view. Empty on the first attempt; on the
    // retry after an execution-phase failure it holds whatever `seal::segments_failing_verification`
    // found, so a page-corrupt segment *reduces* its table instead of failing the whole query (#433).
    excluded: &std::collections::BTreeSet<String>,
    // The live, registry-derived schema (`indexer::full_schema`/`served`) - every table the config
    // declares, independent of whether it has ever populated. #663: `schema_columns(dir)` alone reads
    // `schema.json` off disk, and that file is only as fresh as the last `init`/`add`/`schema`/`dev`
    // startup that wrote it - a hand-edited `nuthatch.toml`, an out-of-band checkout, or a schema.json
    // committed before the config gained an event can all leave it behind. A table missing from disk
    // but present here still gets its empty typed view, so a genuinely-declared-but-never-fired event
    // degrades to zero rows instead of the whole file failing to bind. Empty when the caller has no
    // live registry handy (most tests, and the handful of internal callers this fix deliberately
    // leaves on the disk-only path) - identical to today's behaviour in that case.
    declared: &[crate::registry::TableSchema],
    // Only define views for these tables, lowercased. `None` defines every table the nest has, which
    // is what every caller outside `run` wants and what `run` falls back to when it cannot work out
    // what a statement reaches. See `reachable_tables` for why this matters (#896).
    wanted: Option<&std::collections::BTreeSet<String>>,
) -> Result<std::collections::BTreeSet<String>> {
    Ok(define_views_bound(
        session,
        dir,
        hot,
        sealed_through,
        excluded,
        declared,
        wanted,
        FactWindow::default(),
        false,
    )?
    .degraded)
}

/// The first file of each distinct schema among `sealed`, in their order. `union_by_name` over these
/// yields exactly what it yields over all of them, since a later file with an already-seen schema adds
/// no column and no type; binding thousands of files by name held about 240 MiB (#1508). A file whose
/// schema cannot be read is kept, so the unreadable-segment handling still sees it.
fn one_per_file_schema(session: &dyn Session, sealed: Vec<(PathBuf, u64)>) -> Vec<(PathBuf, u64)> {
    let mut seen = std::collections::BTreeSet::new();
    sealed
        .into_iter()
        .filter(|(f, _)| match session.file_schema(f) {
            Some(shape) => seen.insert(shape),
            None => true,
        })
        .collect()
}

/// The block range fact views expose: rows with `after < block_number <= through`. `/sql` bounds only
/// `through`, for a historical read. RFC-0059 folds bound both sides, so a fact name inside a fold means
/// its window rather than its history.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FactWindow {
    pub(crate) after: Option<u64>,
    pub(crate) through: Option<u64>,
}

impl FactWindow {
    pub(crate) fn is_bounded(&self) -> bool {
        self.after.is_some() || self.through.is_some()
    }

    fn holds(&self, block: u64) -> bool {
        self.after.is_none_or(|lo| block > lo) && self.through.is_none_or(|hi| block <= hi)
    }

    fn overlaps(&self, from: u64, to: u64) -> bool {
        self.after.is_none_or(|lo| to > lo) && self.through.is_none_or(|hi| from <= hi)
    }
}

/// What [`define_views_bound`] built: the degraded tables, each defined table's sealed segments as
/// they went into its view, and the catalogue those came from.
struct DefinedViews {
    degraded: std::collections::BTreeSet<String>,
    tables: std::collections::BTreeMap<String, TableScan>,
    catalogue_hash: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn define_views_bound(
    session: &dyn Session,
    dir: &Path,
    hot: &HotRows,
    sealed_through: u64,
    excluded: &std::collections::BTreeSet<String>,
    declared: &[crate::registry::TableSchema],
    wanted: Option<&std::collections::BTreeSet<String>>,
    window: FactWindow,
    // Bind each table over one sealed file per distinct file schema: the same columns, types and
    // order as all of them (#1508), for a caller that describes queries and never reads a row.
    schema_only: bool,
) -> Result<DefinedViews> {
    let mut degraded: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut bound: std::collections::BTreeMap<String, TableScan> = Default::default();
    let (manifest, catalogue_hash) = crate::seal::load_manifest_with_hash(dir)?;
    let mut overtaken = false;
    #[cfg(test)]
    if let Some(f) = test_after_manifest_read().lock().unwrap().remove(dir) {
        f();
    }
    let mut schema = schema_columns(dir);
    // #729: the table-name check above (#663) stops here at the table's *existence* - a table already
    // on disk kept exactly the columns `schema.json` had, even when the live registry (a re-fetched ABI,
    // same event, an added field) now declares more. Diff column *names* too, per table, and append what
    // the disk copy is missing; an on-disk column never loses its declared type, and this only ever adds.
    for t in declared {
        let declared_cols: Vec<(String, String)> = t
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.storage.clone()))
            .collect();
        match schema.iter_mut().find(|(name, _)| name == &t.table) {
            Some((_, cols)) => {
                let added: Vec<&str> = declared_cols
                    .iter()
                    .filter(|(name, _)| !cols.iter().any(|(n, _)| n == name))
                    .map(|(name, _)| name.as_str())
                    .collect();
                if !added.is_empty() {
                    // Loud on purpose (#729 acceptance bar): this used to be silent, and silent
                    // correctness in `define_views` is what made #663 and this issue both findable
                    // only by reading the view's output rather than the log.
                    tracing::warn!(
                        "table {t} in schema.json is missing column(s) the live registry declares: {} \
                         - merging them in; segments sealed before this column existed read back NULL \
                         for it, never an error. Re-run `nuthatch schema` (or restart `dev`) to refresh \
                         schema.json and stop seeing this.",
                        added.join(", "),
                        t = t.table,
                    );
                    for (name, storage) in &declared_cols {
                        if !cols.iter().any(|(n, _)| n == name) {
                            cols.push((name.clone(), storage.clone()));
                        }
                    }
                }
            }
            None => schema.push((t.table.clone(), declared_cols)),
        }
    }
    let cols_of = |table: &str| -> &[(String, String)] {
        schema
            .iter()
            .find(|(t, _)| t == table)
            .map(|(_, c)| c.as_slice())
            .unwrap_or(&[])
    };

    // The full set of tables to define: declared (schema) ∪ sealed (manifest) ∪ hot. Each view is the
    // `UNION ALL` of whichever of {sealed Parquet, hot tip} exist. COR-1: hot and cold are kept disjoint
    // structurally by `sealed_through` - cold includes only segments finalized *up to* the watermark,
    // hot only rows *past* it - so the union is exact even across the brief seal→prune window (a segment
    // written before its watermark advances is excluded from cold; its rows are still served from hot).
    let mut tables: std::collections::BTreeSet<String> =
        schema.iter().map(|(t, _)| t.clone()).collect();
    tables.extend(manifest.tables.keys().cloned());
    tables.extend(hot.keys().cloned());
    // #896: a view's DDL carries every one of that table's sealed segment paths, so defining the
    // ones a statement cannot reach is the dominant per-request cost on a mature nest.
    if let Some(wanted) = wanted {
        tables.retain(|t| wanted.contains(&t.to_ascii_lowercase()));
    }
    // The maintained relations, by the declaration that makes them so: their rows are typed from their
    // own cells rather than loaded as event text (#1572).
    let relations = declared_relations(dir);
    // A pooled connection keeps the views an earlier request defined. An entity that has since
    // faulted is left out of `hot`, and one whose rows fail to load is not rebound, so without this
    // either would still answer from its old relation. Each is rebuilt below only if it loads.
    for r in &relations {
        let _ = session.drop_relation(r);
    }

    for table in &tables {
        let cols = cols_of(table);
        if window.is_bounded() {
            if !cols.iter().any(|(name, _)| name == "block_number") {
                bail!("historical query requires block-stamped facts: {table} has no declared block_number");
            }
            if hot.get(table).is_some_and(|rows| {
                rows.iter()
                    .any(|row| row.get("block_number").and_then(Value::as_u64).is_none())
            }) {
                bail!("historical query requires block-stamped facts: {table} contains an unstamped row");
            }
        }
        // Only segments finalized at or below the served watermark (COR-1 disjointness).
        let sealed: Vec<(PathBuf, u64)> = manifest
            .tables
            .get(table)
            .map(|segs| {
                segs.iter()
                    .filter(|s| s.to_block <= sealed_through)
                    .filter(|s| window.overlaps(s.from_block, s.to_block))
                    .filter_map(|s| {
                        // Resolve through the shared store when this dataset belongs to a runtime
                        // (RFC-0033 §11a), falling back to the per-dataset path.
                        // Verified-bad on a previous attempt: its bytes no longer match its content
                        // address, so it cannot contribute rows anyone should trust (#433). Dropped
                        // from the view rather than moved on disk - see
                        // `seal::segments_failing_verification` on why reduction, not quarantine.
                        if excluded.contains(&s.hash) {
                            // Present on disk and corrupt in its pages - never reported before this
                            // query, unlike the missing-file case below, so `error!` to match
                            // `verify_and_quarantine`'s level for the identical decision (#435).
                            tracing::error!(
                                "segment {} for {table} fails verification - skipping (cold data reduced)",
                                s.file
                            );
                            degraded.insert(table.clone());
                            return None;
                        }
                        let p = crate::seal::segment_path(dir, &s.file, &s.hash);
                        // Skip a manifest segment whose file is gone from disk (quarantined as corrupt
                        // by the startup integrity pass, or externally removed). Without this, one
                        // missing file makes `read_parquet` throw and the whole query fail; instead the
                        // table's cold data is reduced, loudly, and queries keep working.
                        if let Ok(meta) = std::fs::metadata(&p) {
                            Some((p, meta.len()))
                        } else if crate::seal::catalogue_hash(dir).ok().flatten() != catalogue_hash {
                            // Gone because the catalogue moved under this plan, not because it was
                            // quarantined: reducing would answer short, so the plan is redone.
                            overtaken = true;
                            None
                        } else {
                            // Stays at `warn!`: the usual cause is `verify_and_quarantine` having
                            // already moved this file aside and logged it at `error!` at startup, and
                            // re-raising the consequence on every query would double-count it.
                            tracing::warn!(
                                "segment {} for {table} missing on disk - skipping (cold data reduced)",
                                s.file
                            );
                            degraded.insert(table.clone());
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let sealed = if schema_only {
            one_per_file_schema(session, sealed)
        } else {
            sealed
        };
        let sealed_files: Vec<PathBuf> = sealed.iter().map(|(file, _)| file.clone()).collect();
        // Only tip rows strictly past the watermark (COR-1 disjointness; belt-and-braces with the
        // atomic seal→prune, which already keeps sealed rows out of hot).
        //
        // ...but only where cold actually covers something. `sealed_through` is 0 both when the
        // watermark sits at block 0 AND when nothing has ever been sealed (`Store::sealed_through`
        // documents that fallback), and `> 0` drops the genesis row in the second case: block 0
        // lands in neither half of the union and is unreadable. Invisible on any chain indexed from
        // a later block, fatal on one indexed from 0 - OBIB case 3 wants 100,001 rows for blocks
        // 0-100,000 and we returned 100,000, starting at block 1.
        //
        // Gating on this table's own sealed segments is what makes it exact rather than a special
        // case for zero: a row is withheld from hot only when cold genuinely holds that range, so
        // disjointness is preserved in every other state (and a table that has never sealed - newly
        // added, or lagging its siblings - stops being silently truncated at its first block too).
        let hot_rows: Vec<&Value> = hot
            .get(table)
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .filter(|r| {
                sealed_files.is_empty()
                    || r.get("block_number").and_then(Value::as_u64).unwrap_or(0) > sealed_through
            })
            // Unstamped rows were refused above for any bounded window.
            .filter(|r| {
                !window.is_bounded()
                    || r.get("block_number")
                        .and_then(Value::as_u64)
                        .is_some_and(|b| window.holds(b))
            })
            .collect();

        // The hot tip: load this table's unsealed rows into a temp table, then union it in. Columns are
        // derived from the rows themselves (like the sealed Parquet, `seal::rows_to_batch`), so this
        // works with or without a `schema.json`. The `*_dec` derived columns still come from the schema.
        let relation = relations.contains(&table.to_ascii_lowercase());
        // A relation with declared types exists empty: an entity with no rows yet is a table of none,
        // not a missing one (#1598).
        let declared_relation = if relation {
            relation_types(dir, table)
        } else {
            Vec::new()
        };
        // Stage an empty tip too. A narrower window must not keep the rows the last one loaded.
        if !relation && hot_rows.is_empty() {
            let _ = session.load_hot(table, &hot_rows);
        }
        let hot_loaded = (!hot_rows.is_empty() || !declared_relation.is_empty())
            && match if relation {
                session.load_relation(table, &declared_relation, &hot_rows)
            } else {
                session.load_hot(table, &hot_rows)
            } {
                Ok(()) => true,
                // A statement that names the relation is told why it cannot be served (#1679).
                Err(e) if relation && wanted.is_some() => return Err(e),
                Err(e) => {
                    tracing::debug!("hot rows for {table} skipped: {e:#}");
                    degraded.insert(table.clone());
                    false
                }
            };

        // The view over the sealed files plus whatever hot rows loaded. A segment that will not bind
        // is dropped and the view rebuilt from what remains, below.
        let e = match session.bind_facts(table, cols, &sealed_files, hot_loaded, window) {
            Ok(true) => {
                bound.insert(table.clone(), table_scan(&sealed));
                continue;
            }
            Ok(false) => continue,
            Err(e) => e,
        };
        // A sealed segment that is present but *unreadable* throws while `read_parquet` binds its
        // footer, which happens at DDL time. Swallowing that used to delete the table from the SQL
        // surface outright, so the caller was told the table does not exist and a corrupt file on disk
        // read as a naming fault (#419). Treat it the way a missing file is treated above: drop the
        // segments that will not bind and rebuild the view from what remains, so one bad file *reduces*
        // the table rather than deleting it. The probe is free on the healthy path - it only runs once
        // the whole-view DDL has already failed.
        let readable: Vec<(PathBuf, u64)> = sealed
            .iter()
            .filter(|(f, _)| match session.segment_binds(f) {
                Ok(()) => true,
                Err(err) => {
                    // Present and unreadable, same class as the excluded case above: `error!`,
                    // and recorded so the caller learns the table came back short (#435).
                    tracing::error!(
                        "segment '{}' for {table} will not bind - skipping (cold data reduced): {err}",
                        f.display()
                    );
                    degraded.insert(table.clone());
                    false
                }
            })
            .cloned()
            .collect();
        if readable.len() == sealed_files.len() {
            // Every segment binds, so the failure is something else entirely: report it and leave the
            // table undefined, as before. `warn!` rather than `debug!` - a table vanishing from `/sql`
            // is not a debugging detail.
            tracing::warn!("view {table} skipped: {e}");
            degraded.insert(table.clone());
            continue;
        }
        let readable_files: Vec<PathBuf> = readable.iter().map(|(file, _)| file.clone()).collect();
        match session.bind_facts(table, cols, &readable_files, hot_loaded, window) {
            Err(e) => {
                tracing::warn!("view {table} skipped after dropping bad segments: {e}");
                degraded.insert(table.clone());
            }
            Ok(true) => {
                bound.insert(table.clone(), table_scan(&readable));
            }
            Ok(false) => {}
        }
    }
    if overtaken {
        return Err(SegmentSetChanged.into());
    }
    Ok(DefinedViews {
        degraded,
        tables: bound,
        catalogue_hash,
    })
}

fn table_scan(files: &[(PathBuf, u64)]) -> TableScan {
    TableScan {
        segments: files.len() as u64,
        bytes: files
            .iter()
            .fold(0_u64, |sum, (_, len)| sum.saturating_add(*len)),
    }
}

/// The SQL column type for a sealed/hot column, matching `seal::rows_to_batch`: the four counter
/// columns are `UBIGINT`, everything else is stored as canonical text (`VARCHAR`).
type HeldRelations = Mutex<std::collections::HashMap<PathBuf, std::collections::BTreeSet<String>>>;

fn held_relations() -> &'static HeldRelations {
    static HELD: OnceLock<HeldRelations> = OnceLock::new();
    HELD.get_or_init(Default::default)
}

/// Record the entities a successful read of `dir/entities.toml` declared. Called by
/// [`crate::entities::load`], so the read a nest starts from is the one its queries type by.
pub(crate) fn hold_relations(dir: &Path, decls: &[crate::entities::EntityDecl]) {
    let names = decls.iter().map(|e| e.name.to_ascii_lowercase()).collect();
    held_relations()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(dir.to_path_buf(), names);
}

type HeldRelationTypes =
    Mutex<std::collections::HashMap<(PathBuf, String), Vec<(String, &'static str)>>>;

fn held_relation_types() -> &'static HeldRelationTypes {
    static HELD: OnceLock<HeldRelationTypes> = OnceLock::new();
    HELD.get_or_init(Default::default)
}

/// Record an entity's output columns with the types its plan gives them (#1598), so its relation
/// exists before it holds a row and keeps one type whatever its values. A column whose type the plan
/// cannot fix, a `CASE` over mixed branches for one, is text.
pub(crate) fn hold_relation_types(
    dir: &Path,
    name: &str,
    columns: &[String],
    types: &[Option<crate::entity_expr::Type>],
) {
    use crate::entity_expr::Type;
    let cols = columns
        .iter()
        .zip(types)
        .map(|(c, t)| {
            let ty = match t {
                Some(Type::Int) => "HUGEINT",
                Some(Type::Bool) => "BOOLEAN",
                Some(Type::Str) | None => "VARCHAR",
            };
            (c.clone(), ty)
        })
        .collect();
    held_relation_types()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert((dir.to_path_buf(), name.to_ascii_lowercase()), cols);
}

/// The declared columns of `dir`'s relation `name`, or none when it was never started here.
fn relation_types(dir: &Path, name: &str) -> Vec<(String, &'static str)> {
    held_relation_types()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&(dir.to_path_buf(), name.to_ascii_lowercase()))
        .cloned()
        .unwrap_or_default()
}

pub(crate) fn declared_entity_names(dir: &Path) -> std::collections::BTreeSet<String> {
    declared_relations(dir)
}

/// The entity names `entities.toml` declares in `dir`, lowercased, as last read successfully. A running
/// nest's declarations cannot change without a restart (they are part of its content address), and its
/// start read them, so a file an operator leaves half-edited does not turn its relations into text.
fn declared_relations(dir: &Path) -> std::collections::BTreeSet<String> {
    if let Some(names) = held_relations()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(dir)
    {
        return names.clone();
    }
    match crate::entities::load(dir) {
        Ok(decls) => decls
            .into_iter()
            .map(|e| e.name.to_ascii_lowercase())
            .collect(),
        // Never read successfully in this process, so no entity circuit is running from it: nothing
        // here is a maintained relation to mistype.
        Err(e) => {
            tracing::warn!(
                "{} could not be read: {e:#}",
                dir.join("entities.toml").display()
            );
            Default::default()
        }
    }
}

/// Load a nest's derived-entity views from `{dir}/views/*.sql` into the connection, in sorted
/// filename order (so `10-foo.sql` can build on nothing and `20-bar.sql` can build on foo). Run
/// after the per-event table views (§4 of RFC-0002), so views may reference `{alias}__{event}`
/// tables. Best-effort: a view over a table with no sealed segment yet - or a bad statement - is
/// skipped rather than failing the whole query, warned once and named on `/ready` (#1653). Nest SQL
/// is authored by the nest you chose to consume; it runs read-only in this ephemeral in-memory
/// session, same trust as `/sql`.
///
/// `wanted` is the same reachability set `define_views` narrows by (#896): only a view whose name is
/// in it is (re)defined, and `None` defines every view as before. **The narrowing has to reach here
/// too, not only the base tables, and it did not.** A view is planned when it is defined, and on the
/// pooled session the previous request's base-table views are still in the catalogue, so every
/// authored view bound in full on every request. Under DuckDB each bind re-read the footer of every
/// segment behind every table the view touched. Measured on the Lodestar nest (1,924 segments, 12
/// view files, 2026-09-06): `SELECT 1` cost 1.2 s and 32,000 `openat` calls with 640,000
/// `fstat`/`readlink` behind them, on a statement that reads nothing; the same statement on the same nest was 14 ms before any dashboard view had left
/// its base tables defined. That fixed cost sat under every one of the dashboard's statements.
/// `reachable_tables` already carries the intermediate view names in its closure, so a view a
/// statement reaches through another view is still defined, in file order, before the one that
/// reads it.
fn define_nest_views(
    session: &dyn Session,
    dir: &Path,
    wanted: Option<&std::collections::BTreeSet<String>>,
) {
    for v in nest_view_files(dir) {
        // **Per statement, not per file** (issue #241 item 4). `execute_batch` runs the whole file as
        // one unit, so a single view referencing a table that has never fired - `TaskCancelled`, a
        // module deployed but not yet used - took down *every* view in that file, including the ones
        // that would have worked. The reported workaround was commenting out correct views and
        // uncommenting them once the event fired, which is a poor trade for a fault-isolation gain
        // that was never needed at this granularity.
        for stmt in split_sql_statements(&v.sql) {
            // A statement this one cannot name (not a `CREATE VIEW`) runs as before: the point is
            // to skip work that is known to be unreachable, never to skip what is not understood.
            if let (Some(wanted), Some(name)) = (wanted, view_name(&stmt)) {
                if !wanted.contains(&name) {
                    continue;
                }
            }
            let name = view_name(&stmt).unwrap_or_default();
            let failed = session
                .execute(&with_or_replace_view(&stmt))
                .err()
                .map(|e| e.to_string());
            let new = note_view_failure(dir, &v.file, &name, failed.clone());
            // Warned once per fault, since every statement reaching the view defines it again.
            match failed {
                Some(e) if new => tracing::warn!("nest view {name} in {} skipped: {e}", v.file),
                Some(e) => tracing::debug!("nest view {name} in {} skipped: {e}", v.file),
                None => {}
            }
        }
    }
}

type ViewFailures =
    Mutex<std::collections::HashMap<PathBuf, std::collections::BTreeMap<(String, String), String>>>;

fn view_failure_record() -> &'static ViewFailures {
    static FAILED: OnceLock<ViewFailures> = OnceLock::new();
    FAILED.get_or_init(Default::default)
}

/// Record whether one authored view in `dir` last failed to build. True when `error` is a fault
/// not already recorded for it.
fn note_view_failure(dir: &Path, file: &str, view: &str, error: Option<String>) -> bool {
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let mut all = view_failure_record()
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let key = (file.to_string(), view.to_string());
    match error {
        Some(e) => all.entry(dir).or_default().insert(key, e.clone()) != Some(e),
        None => {
            if let Some(failed) = all.get_mut(&dir) {
                failed.remove(&key);
            }
            false
        }
    }
}

/// `(file, view, error)` for each authored view in `dir` whose last build failed, for `/ready`.
pub(crate) fn view_failures(dir: &Path) -> Vec<(String, String, String)> {
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    view_failure_record()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&dir)
        .into_iter()
        .flatten()
        .map(|((file, view), error)| (file.clone(), view.clone(), error.clone()))
        .collect()
}

/// Bind immutable offchain snapshots beneath an explicit namespace. They deliberately have no hot
/// half, no watermark, and no path back into chain replay: they are query inputs only.
/// Returns each view it defined and the content hashes of the snapshots behind it, in append order,
/// so an answer can name what it read (#1437). A view the statement names that will not bind is its
/// error rather than an absent table (#1678).
fn define_offchain_views(
    session: &dyn Session,
    dir: &Path,
    wanted: Option<&std::collections::BTreeSet<String>>,
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    let mut defined = std::collections::BTreeMap::new();
    let Ok(catalogue) = crate::offchain::load(dir) else {
        tracing::warn!(
            "offchain provenance manifest is unreadable; no offchain views were defined"
        );
        return Ok(defined);
    };
    for (table, snapshots) in &catalogue.tables {
        let view = format!("offchain__{table}");
        if wanted.is_some_and(|set| !set.contains(&view.to_ascii_lowercase())) {
            continue;
        }
        let present: Vec<(&crate::offchain::Snapshot, PathBuf)> = snapshots
            .iter()
            .map(|s| (s, crate::offchain::segment_path(dir, s)))
            .filter(|(_, p)| p.exists())
            .collect();
        if present.is_empty() {
            continue;
        }
        let files: Vec<PathBuf> = present.iter().map(|(_, p)| p.clone()).collect();
        match session.bind_snapshots(&view, &files) {
            Ok(()) => {
                defined.insert(view, present.iter().map(|(s, _)| s.hash.clone()).collect());
            }
            Err(e) if wanted.is_some() => return Err(e.context(format!("{view} will not bind"))),
            Err(e) => tracing::warn!("offchain view {view} skipped: {e}"),
        }
    }
    // Keyed on pulls, not snapshots: a pull that has never succeeded has no snapshot, and its status
    // is then the only thing there is to see.
    for (table, refresh) in &catalogue.refreshes {
        let view = format!("offchain__{table}__status");
        if wanted.is_some_and(|set| !set.contains(&view.to_ascii_lowercase())) {
            continue;
        }
        let ddl = offchain_status_ddl(&view, table, refresh);
        if let Err(e) = session.execute(&ddl) {
            tracing::warn!("offchain status view {view} skipped: {e}");
        }
    }
    Ok(defined)
}

/// `stale` is true on a recorded failure, before any success, or past the declared cadence. Views
/// are defined as each query is prepared, so the age is taken then, in Rust, and the view carries a
/// constant rather than a call to `now()`.
fn offchain_status_ddl(view: &str, table: &str, refresh: &crate::offchain::Refresh) -> String {
    let text = |value: Option<&str>| {
        value.map_or("CAST(NULL AS VARCHAR)".to_string(), |v| {
            format!("'{}'", v.replace('\'', "''"))
        })
    };
    let int =
        |value: Option<i64>| value.map_or("CAST(NULL AS BIGINT)".to_string(), |v| v.to_string());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    let age = refresh
        .succeeded_at
        .as_deref()
        .and_then(|s| s.strip_prefix("unix:"))
        .and_then(|s| s.parse::<i64>().ok())
        .map(|t| now - t);
    let stale_after = refresh.stale_after_secs.map(|s| s as i64);
    let stale = refresh.error.is_some()
        || age.is_none()
        || matches!((age, stale_after), (Some(a), Some(s)) if a > s);
    format!(
        "CREATE OR REPLACE VIEW \"{view}\" AS SELECT \
         {table_lit} AS \"table\", {source} AS source, {attempted} AS attempted_at, \
         {succeeded} AS succeeded_at, {error} AS error, {stale_after} AS stale_after_secs, \
         {age} AS age_secs, {stale} AS stale, {snapshot} AS snapshot, \
         {fetched} AS fetched_sha256",
        table_lit = text(Some(table)),
        source = text(Some(&refresh.source)),
        attempted = text(Some(&refresh.attempted_at)),
        succeeded = text(refresh.succeeded_at.as_deref()),
        error = text(refresh.error.as_deref()),
        snapshot = text(refresh.snapshot.as_deref()),
        fetched = text(refresh.fetched_sha256.as_deref()),
        stale_after = int(stale_after),
        age = int(age),
    )
}

/// Make a nest-authored `CREATE VIEW` re-runnable on a cached connection (#295).
fn with_or_replace_view(stmt: &str) -> String {
    let s = stmt.trim_start();
    if s.len() < 11 || !s[..6].eq_ignore_ascii_case("create") {
        return stmt.to_string();
    }
    let rest = s[6..].trim_start();
    if rest.len() >= 4
        && rest[..4].eq_ignore_ascii_case("view")
        && rest
            .get(4..)
            .and_then(|r| r.chars().next())
            .is_some_and(|c| c.is_whitespace() || c == '"')
    {
        format!("CREATE OR REPLACE VIEW{}", &rest[4..])
    } else {
        stmt.to_string()
    }
}

/// Split authored SQL into individual statements on top-level `;`.
///
/// Deliberately small rather than a parser: it tracks single-quoted strings, double-quoted
/// identifiers, and `--` line comments, which is everything a `;` can hide behind in the SQL a nest
/// authors. A dollar-quoted body would defeat it - nest SQL has none, and if that changes this is
/// the function to revisit rather than a mystery to debug.
pub(crate) fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let (mut in_s, mut in_d, mut in_c) = (false, false, false);
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if in_c {
            if c == '\n' {
                in_c = false;
                cur.push(c);
            }
            continue;
        }
        match c {
            '-' if !in_s && !in_d && chars.peek() == Some(&'-') => {
                in_c = true;
                continue;
            }
            '\'' if !in_d => in_s = !in_s,
            '"' if !in_s => in_d = !in_d,
            ';' if !in_s && !in_d => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
                continue;
            }
            _ => {}
        }
        cur.push(c);
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

/// One authored view file: its basename (`10-recipients.sql`) and SQL, in load order.
pub struct NestViewFile {
    pub file: String,
    pub sql: String,
}

/// Read `{dir}/views/*.sql` in sorted filename order - so `10-foo.sql` builds on nothing and
/// `20-bar.sql` can build on foo. Empty when there is no `views/` dir. The one reader both the live
/// loader and the validation gate use, so they never disagree about what a nest's views are.
pub fn nest_view_files(dir: &Path) -> Vec<NestViewFile> {
    let Ok(entries) = std::fs::read_dir(dir.join("views")) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .filter_map(|p| {
            let sql = std::fs::read_to_string(&p).ok()?;
            let file = p.file_name()?.to_string_lossy().into_owned();
            Some(NestViewFile { file, sql })
        })
        .collect()
}

/// A view that failed to load - RFC-0018 §1 turns the old silent skip into a first-class, teachable
/// signal.
#[derive(Debug, Clone)]
pub struct ViewIssue {
    pub file: String,
    /// The raw engine error (path-free - it's a bind, no segment paths).
    pub error: String,
    /// A fuzzy-matched fix hint (RFC-0016 errors-as-prompts), when the failure is a known class - a
    /// renamed/absent table or column (drift), a reserved word, or a big-int arithmetic slip.
    pub hint: Option<String>,
}

/// If a query fails against a name the engine says doesn't exist, and that name is a nest-authored view
/// that failed to build, replace the generic "does not exist" + fuzzy-match-on-an-unrelated-table
/// message with the view's real build error (#539). A view that fails to build is reported as though
/// it doesn't exist at all - `define_nest_views` loads views per-statement and skips failures for
/// fault isolation, so by the time a query dies at `/sql` its error carries nothing of
/// *why* the name is missing, and `sql_errors::enrich`'s fuzzy match then points at an unrelated real
/// table. This is the one place that record is reconstructed: on the query's error path only (never
/// on a successful query), rebuild the same base surface `validate_nest_views` uses and replay the
/// view files in order, and report whichever `CREATE VIEW` statement targets `missing`.
pub fn enrich_query_error(
    dir: &Path,
    raw: &str,
    query: &str,
    schema: &[crate::registry::TableSchema],
) -> Option<String> {
    if let Some(name) = missing_table_of(raw) {
        if let Some(issue) = view_build_failure(dir, schema, &name) {
            // The caller wraps whatever this returns as its own "hint: …" line, so this must read as
            // that line's content, not carry a second nested "hint:" of its own.
            let extra = issue.hint.map(|h| format!("\n{h}")).unwrap_or_default();
            return Some(format!(
                "view `{name}` failed to build (in `{}`): {}{extra}",
                issue.file, issue.error
            ));
        }
    }
    crate::sql_errors::enrich(raw, query, schema)
}

/// If `missing` is the name of a nest-authored view (`views/*.sql`) that failed to build, the error
/// from that specific `CREATE VIEW` statement - the real fault a query against it hit, rather than
/// the "does not exist" the engine reports for a name that was simply never created. `None` if `missing`
/// isn't an authored view name at all (an ordinary unknown-table typo), or names one that in fact
/// built fine (so whatever failed, it wasn't this).
fn view_build_failure(
    dir: &Path,
    schema: &[crate::registry::TableSchema],
    missing: &str,
) -> Option<ViewIssue> {
    // Burrmill plans a view when it is defined (`register_view`; see the analytics.rs test suite),
    // so "two later views joined pool_effective_fee" (#539) means those two views' *own*
    // `CREATE VIEW` statements failed at load, each with the same "pool_effective_fee does not
    // exist". Chase that chain to the view whose failure is not itself just a missing upstream view -
    // the one line that actually explains anything - rather than reporting a hop that only repeats
    // the same "does not exist" one level removed. Error explanation is bounded to 8 hops;
    // unlike dependency discovery, it may stop early without omitting query input data.
    view_build_failure_at(dir, schema, missing, 8)
}

#[cfg(test)]
thread_local! {
    static VIEW_BUILD_OPENS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many times this thread opened an engine to explain a missing view (#1652).
#[cfg(test)]
pub(crate) fn view_build_opens() -> usize {
    VIEW_BUILD_OPENS.with(|n| n.get())
}

#[cfg(test)]
fn note_view_build_open() {
    VIEW_BUILD_OPENS.with(|n| n.set(n.get() + 1));
}

fn view_build_failure_at(
    dir: &Path,
    schema: &[crate::registry::TableSchema],
    missing: &str,
    hops_left: u8,
) -> Option<ViewIssue> {
    let files = nest_view_files(dir);
    if files.is_empty() {
        return None;
    }
    let target = missing.trim_matches('"').to_ascii_lowercase();
    // Opening the engine rebinds every table. A name that is not a view
    // has nothing to explain (#1652).
    let authored = files.iter().any(|v| {
        split_sql_statements(&v.sql).iter().any(|stmt| {
            view_target_name(stmt).is_some_and(|name| name.to_ascii_lowercase() == target)
        })
    });
    if !authored {
        return None;
    }
    #[cfg(test)]
    note_view_build_open();
    let session = engine().open_bare().ok()?;
    let empty_hot = HotRows::new();
    let _ = define_views(
        &*session,
        dir,
        &empty_hot,
        u64::MAX,
        &Default::default(),
        schema,
        None,
    );
    define_labels_view(&*session, dir);
    define_children_views(&*session, dir);

    for v in &files {
        for stmt in split_sql_statements(&v.sql) {
            let result = session.execute(&stmt);
            let Some(name) = view_target_name(&stmt) else {
                continue;
            };
            if name.to_ascii_lowercase() != target {
                continue;
            }
            return match result {
                Ok(()) => None,
                Err(e) => {
                    let error = e.to_string();
                    // If this statement's own failure is "some other name does not exist", and that
                    // name is itself an authored view that also failed, that view's failure is the
                    // actual cause - chase it rather than reporting a repeat of the same "does not
                    // exist" the caller already has.
                    if hops_left > 0 {
                        if let Some(dep) = missing_table_of(&error) {
                            if dep.to_ascii_lowercase() != target {
                                if let Some(root) =
                                    view_build_failure_at(dir, schema, &dep, hops_left - 1)
                                {
                                    return Some(ViewIssue {
                                        file: v.file.clone(),
                                        error: format!(
                                            "depends on view `{dep}` (in `{}`), which failed to \
                                             build: {}",
                                            root.file, root.error
                                        ),
                                        hint: root.hint,
                                    });
                                }
                            }
                        }
                    }
                    let hint = crate::sql_errors::enrich(&error, &stmt, schema);
                    Some(ViewIssue {
                        file: v.file.clone(),
                        error,
                        hint,
                    })
                }
            };
        }
    }
    None
}

/// The view name a `CREATE [OR REPLACE] VIEW <name> AS …` statement targets, unquoted. `None` for a
/// statement that isn't that shape. Same lowercase-scan-for-offsets trick as `view_body`.
fn view_target_name(stmt: &str) -> Option<String> {
    let lower = stmt.to_ascii_lowercase();
    let view_at = lower.find(" view ")?;
    let as_at = lower[view_at..].find(" as ")? + view_at;
    Some(
        stmt[view_at + 6..as_at]
            .trim()
            .trim_matches('"')
            .to_string(),
    )
}

/// Declared tables (`declared`, typically `indexer::full_schema` - live, not `schema.json`) that have
/// never sealed a single segment - the honest, on-disk-permanent signal for "this event's decoder
/// exists but the chain has never actually emitted it" (#663). A table that has fired but not yet
/// sealed (still hot-only, e.g. seconds after its first log on a nest that was just restarted) is
/// misclassified as empty here until its next seal; that window is narrow and self-corrects, and
/// erring toward "say something, occasionally early" beats the silence this issue is about.
///
/// This is what turns "the day that event first fires, the view starts working, and nothing in the
/// logs explains either state" into a startup line an operator can read once and stop wondering about.
pub fn declared_but_never_sealed(
    dir: &Path,
    declared: &[crate::registry::TableSchema],
) -> Vec<String> {
    let manifest = crate::seal::load_manifest(dir).unwrap_or_default();
    // **Nothing sealed at all means we have not looked yet, not that nothing ever fires** (#1042).
    //
    // The predicate is "declared, and absent from the seal manifest". On a cold start the manifest is
    // empty, so *every* table matches and the caller announces that every declared event has likely
    // never fired on this chain - seconds before they all populate. A fresh operator running the
    // tyre-kicking pass hit exactly that on both a USDC and a stETH nest, and recorded it as the
    // product's first substantive line telling them their contract looked dead.
    //
    // An empty manifest carries no information about any individual table, so there is nothing
    // honest to say. The claim becomes available the moment *something* has sealed: from then on,
    // a table still missing from the manifest is genuinely a table whose event has not fired in the
    // range we have covered - which is what #663 wanted to surface, and is still surfaced.
    if manifest.tables.is_empty() {
        return Vec::new();
    }
    declared
        .iter()
        .map(|t| &t.table)
        .filter(|t| !manifest.tables.contains_key(*t))
        .cloned()
        .collect()
}

/// Validate a nest's authored views (RFC-0018 §1, the loud gate). Sets up the base surface - empty
/// typed per-event views + labels + children, from the nest's own `schema.json`; no data needed, we're
/// *binding*, not running - then defines each view in load order and records any that fail. A failure
/// is either a syntax error or a reference to a table/column the registry no longer has (**drift**);
/// both come back with a fuzzy-matched fix hint. Loading for real queries stays fault-isolated in
/// `define_nest_views`; this is the separate, surfaced check for `dev` startup and `nuthatch check`.
pub fn validate_nest_views(dir: &Path, schema: &[crate::registry::TableSchema]) -> Vec<ViewIssue> {
    let files = nest_view_files(dir);
    if files.is_empty() {
        return Vec::new();
    }
    let session = match engine().open_bare() {
        Ok(session) => session,
        Err(error) => {
            return vec![ViewIssue {
                file: "<scalar functions>".into(),
                error: format!("open the query engine: {error:#}"),
                hint: None,
            }]
        }
    };
    // Base surface the views bind against. `u64::MAX` includes every sealed segment (or, on a fresh
    // nest, yields the empty typed views) so a view referencing `usdc__transfer` resolves.
    // Each entity that binds is an empty relation of its declared types, so a view over it checks
    // exactly when `dev` would serve it (#1599).
    let empty_hot: HotRows = crate::entities::hold_declared_relations(dir)
        .into_iter()
        .map(|name| (name, Vec::new()))
        .collect();
    let _ = define_views(
        &*session,
        dir,
        &empty_hot,
        u64::MAX,
        &Default::default(),
        schema,
        None,
    );
    define_labels_view(&*session, dir);
    define_children_views(&*session, dir);

    let mut issues = Vec::new();
    for v in &files {
        // Per statement, matching the live loader - and **every** failure in the file, not the first.
        // `execute_batch` stops at the first error, so a file referencing three tables that have never
        // fired reported one, sent the author to fix it, and revealed the next on the following run
        // (issue #241 item 4: "fix → restart → next error → repeat"). The whole set is known here in
        // one pass; withholding it is a choice, and a bad one.
        let mut errors: Vec<String> = Vec::new();
        for stmt in split_sql_statements(&v.sql) {
            let name = view_name(&stmt).unwrap_or_default();
            let failed = session.execute(&stmt).err().map(|e| e.to_string());
            note_view_failure(dir, &v.file, &name, failed.clone());
            errors.extend(failed);
        }
        if errors.is_empty() {
            continue;
        }
        // Lead with the missing tables, collected across every failing statement and deduplicated -
        // that list is the actual work item, and it is what the author would otherwise assemble by
        // hand over several restarts.
        let mut missing: Vec<String> = errors
            .iter()
            .filter_map(|e| missing_table_of(e))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        missing.dedup();
        let error = if missing.len() > 1 {
            format!(
                "{} statement(s) failed; unresolved tables: {}",
                errors.len(),
                missing.join(", ")
            )
        } else {
            errors.join("; ")
        };
        let hint = crate::sql_errors::enrich(&errors[0], &v.sql, schema);
        issues.push(ViewIssue {
            file: v.file.clone(),
            error,
            hint,
        });
    }
    issues
}

/// Bind one incremental-entity SELECT against the same empty typed fact surface used by view
/// validation. The columns come from the plan; the entity has already passed the single-SELECT gate,
/// and no rows are read.
pub fn entity_output_columns(
    dir: &Path,
    schema: &[crate::registry::TableSchema],
    sql: &str,
) -> Result<Vec<String>> {
    let session = engine().open_bare()?;
    let empty_hot = HotRows::new();
    let _ = define_views(
        &*session,
        dir,
        &empty_hot,
        u64::MAX,
        &Default::default(),
        schema,
        None,
    );
    let _ = define_offchain_views(&*session, dir, None);
    define_labels_view(&*session, dir);
    define_children_views(&*session, dir);
    session.column_names(sql)
}

/// Every table and authored view name a nest has, lowercased, read from its files without binding
/// anything: what a fold's own name must not collide with.
#[cfg(feature = "folds")]
pub(crate) fn nest_relation_names(
    dir: &Path,
    schema: &[crate::registry::TableSchema],
) -> Result<std::collections::BTreeSet<String>> {
    let mut names: std::collections::BTreeSet<String> = schema_columns(dir)
        .into_iter()
        .map(|(t, _)| t)
        .chain(schema.iter().map(|t| t.table.clone()))
        .chain(
            crate::seal::load_manifest_with_hash(dir)?
                .0
                .tables
                .into_keys(),
        )
        .chain(nest_view_bodies(dir).into_keys())
        .map(|n| n.to_ascii_lowercase())
        .collect();
    names.insert("labels".into());
    Ok(names)
}

/// RFC-0059: binds folds against the nest's surface without reading a row. It lives here so the
/// engine stays inside this module (RFC-0042 §6): every method takes and returns plain data.
#[cfg(feature = "folds")]
pub(crate) struct FoldBinder {
    session: Box<dyn Session>,
    /// The engine's parse and plan keys (RFC-0044 Amendment 2), from a session that needs no catalogue.
    parser: crate::graft::Parser,
}

#[cfg(feature = "folds")]
impl FoldBinder {
    /// Bounded and locked down like `/sql`'s connection: the same memory, thread and spill limits.
    pub(crate) fn open(dir: &Path) -> Result<Self> {
        Ok(Self {
            session: open_session(dir)?,
            parser: crate::graft::Parser::new()?,
        })
    }

    /// Define only `wanted`: binding every table over every segment cost 12 s and 2.6 GB on the
    /// network corpus, whatever the folds read.
    pub(crate) fn bind(
        &self,
        dir: &Path,
        schema: &[crate::registry::TableSchema],
        wanted: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        define_views_bound(
            &*self.session,
            dir,
            &HotRows::new(),
            u64::MAX,
            &Default::default(),
            schema,
            Some(wanted),
            FactWindow::default(),
            true,
        )?;
        define_nest_views(&*self.session, dir, Some(wanted));
        Ok(())
    }

    /// Every table and view currently defined, lowercased.
    #[cfg(test)]
    pub(crate) fn relations(&self) -> Result<std::collections::BTreeSet<String>> {
        self.session.relations()
    }

    /// The statement's top-level node type, or the parser's error.
    pub(crate) fn statement_kinds(&self, sql: &str) -> Result<Vec<String>> {
        fold_statement_kinds(sql)
    }

    pub(crate) fn base_tables(&self, sql: &str) -> Option<std::collections::BTreeSet<String>> {
        base_tables_in(&*self.session, sql)
    }

    pub(crate) fn reachable(
        &self,
        dir: &Path,
        referenced: &std::collections::BTreeSet<String>,
    ) -> Option<std::collections::BTreeSet<String>> {
        reachable_tables(&*self.session, dir, referenced)
    }

    pub(crate) fn refusals(&self, sql: &str) -> Vec<crate::graft::Refusal> {
        crate::graft::refusals_in_sql(sql)
    }

    /// The canonical plan, so formatting alone never changes a fold's identity.
    pub(crate) fn plan_text(&self, sql: &str) -> String {
        match self.parser.canonical_plan(sql) {
            crate::graft::CanonicalPlan::Ast(s) | crate::graft::CanonicalPlan::RawText(s) => s,
        }
    }

    pub(crate) fn engine_version(&self) -> String {
        self.parser.engine_version()
    }

    /// `(column, type)` of a query's output, as Burrmill spells the type.
    pub(crate) fn describe(&self, sql: &str) -> Result<Vec<(String, String)>> {
        self.session.describe(sql)
    }

    pub(crate) fn execute(&self, sql: &str) -> Result<()> {
        self.session.execute(sql)
    }

    /// The constructs in a statement that read across rows or reach back for earlier ones: a window
    /// function, a recursive CTE, or a subquery over one of `facts`. Inside a fold each of them
    /// answers over the window alone (RFC-0059 §3), which is what #1504 asks to warn about. Each is
    /// described once. Empty when the statement does not parse: the fold loader has refused that
    /// already.
    pub(crate) fn lookbacks(
        &self,
        sql: &str,
        facts: &std::collections::BTreeSet<String>,
    ) -> Vec<String> {
        fold_lookbacks(sql, facts)
    }
}

#[cfg(feature = "folds")]
fn parse_statements(sql: &str) -> Result<Vec<sqlparser::ast::Statement>> {
    sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, sql)
        .map_err(|e| anyhow::anyhow!("does not parse: {e}"))
}

#[cfg(feature = "folds")]
fn fold_statement_kinds(sql: &str) -> Result<Vec<String>> {
    use sqlparser::ast::{SetExpr, Statement};
    fn kind(body: &SetExpr) -> &'static str {
        match body {
            SetExpr::Select(_) | SetExpr::Values(_) => "SELECT_NODE",
            SetExpr::SetOperation { .. } => "SET_OPERATION_NODE",
            SetExpr::Query(q) => kind(&q.body),
            _ => "OTHER",
        }
    }
    parse_statements(sql)?
        .iter()
        .map(|st| match st {
            Statement::Query(q) => Ok(kind(&q.body).to_string()),
            _ => bail!("does not parse: only SELECT statements can be folds"),
        })
        .collect()
}

#[cfg(feature = "folds")]
fn fold_lookbacks(sql: &str, facts: &std::collections::BTreeSet<String>) -> Vec<String> {
    use sqlparser::ast::{Expr, Query, SetExpr, TableFactor, Visit, Visitor};
    use std::ops::ControlFlow;

    fn tables_over(q: &Query, facts: &std::collections::BTreeSet<String>) -> Vec<String> {
        struct Tables<'a>(
            &'a std::collections::BTreeSet<String>,
            std::collections::BTreeSet<String>,
        );
        impl Visitor for Tables<'_> {
            type Break = ();
            fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
                if let TableFactor::Table {
                    name, args: None, ..
                } = t
                {
                    let n = name
                        .0
                        .last()
                        .map(|p| p.to_string().trim_matches('"').to_ascii_lowercase());
                    if let Some(n) = n.filter(|n| self.0.contains(n)) {
                        self.1.insert(n);
                    }
                }
                ControlFlow::Continue(())
            }
        }
        let mut t = Tables(facts, Default::default());
        let _ = q.visit(&mut t);
        t.1.into_iter().collect()
    }

    struct Look<'a> {
        facts: &'a std::collections::BTreeSet<String>,
        out: Vec<String>,
    }
    impl Look<'_> {
        fn note(&mut self, s: String) {
            if !self.out.contains(&s) {
                self.out.push(s);
            }
        }
    }
    impl Visitor for Look<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
            if let Some(with) = q.with.as_ref().filter(|w| w.recursive) {
                for c in &with.cte_tables {
                    if matches!(*c.query.body, SetExpr::SetOperation { .. }) {
                        self.note(format!("a recursive CTE (`{}`)", c.alias.name.value));
                    }
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            let (how, q) = match e {
                Expr::Function(f) if f.over.is_some() => {
                    let name = f.name.0.last().map(|p| p.to_string()).unwrap_or_default();
                    self.note(format!(
                        "a window function (`{}() OVER`)",
                        name.to_ascii_lowercase()
                    ));
                    return ControlFlow::Continue(());
                }
                Expr::Exists {
                    subquery,
                    negated: false,
                } => ("an EXISTS subquery", subquery),
                Expr::Exists {
                    subquery,
                    negated: true,
                } => ("a NOT EXISTS subquery", subquery),
                Expr::Subquery(q) => ("a scalar subquery", q),
                Expr::InSubquery { subquery, .. } => ("a subquery", subquery),
                _ => return ControlFlow::Continue(()),
            };
            for t in tables_over(q, self.facts) {
                self.note(format!("{how} over `{t}`"));
            }
            ControlFlow::Continue(())
        }
    }

    let Ok(stmts) = parse_statements(sql) else {
        return Vec::new();
    };
    let mut look = Look {
        facts,
        out: Vec::new(),
    };
    for st in &stmts {
        let _ = st.visit(&mut look);
    }
    look.out
}

/// RFC-0059: evaluates folds one window at a time on a connection of its own. Inside it a fact name
/// means the current window, never history, so it can never share `/sql`'s pooled catalogue.
#[cfg(feature = "folds")]
pub(crate) struct FoldEvaluator {
    session: Box<dyn Session>,
    dir: PathBuf,
    schema: Vec<crate::registry::TableSchema>,
    views_defined: bool,
    /// Views defined inside the open transaction: a rollback removes them, so the flag goes too.
    views_pending: bool,
}

#[cfg(feature = "folds")]
impl FoldEvaluator {
    pub(crate) fn new(dir: &Path, schema: &[crate::registry::TableSchema]) -> Result<Self> {
        // The lockdown admits only directories that exist when the connection opens, and the seal
        // loop's writer opens this before a fresh nest has sealed anything.
        std::fs::create_dir_all(dir.join(crate::folds::CHECKPOINTS_DIR))?;
        std::fs::create_dir_all(dir.join(crate::seal::SEGMENTS_DIR))?;
        if let Some(shared) = crate::seal::shared_store(dir) {
            std::fs::create_dir_all(shared)?;
        }
        Ok(Self {
            session: open_session(dir)?,
            dir: dir.to_path_buf(),
            schema: schema.to_vec(),
            views_defined: false,
            views_pending: false,
        })
    }

    /// Bind every fact table in `wanted` to `(after, through]`, over sealed segments and hot rows.
    pub(crate) fn bind_window(
        &mut self,
        hot: &HotRows,
        sealed_through: u64,
        after: Option<u64>,
        through: u64,
        wanted: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        let defined = define_views_bound(
            &*self.session,
            &self.dir,
            hot,
            sealed_through,
            &Default::default(),
            &self.schema,
            Some(wanted),
            FactWindow {
                after,
                through: Some(through),
            },
            false,
        )?;
        // `/sql` may answer short and say so; a checkpoint built from a short window is simply wrong.
        if !defined.degraded.is_empty() {
            bail!(
                "fold window ({}, {through}] is missing sealed data for {}; refusing to evaluate it",
                after.map_or("genesis".to_string(), |a| a.to_string()),
                defined.degraded.iter().cloned().collect::<Vec<_>>().join(", ")
            );
        }
        // Views resolve fact names when queried, so defining them once serves every later window.
        if !self.views_defined {
            define_nest_views(&*self.session, &self.dir, Some(wanted));
            self.views_defined = true;
            self.views_pending = true;
        }
        Ok(())
    }

    pub(crate) fn begin(&mut self) -> Result<()> {
        self.session.execute("BEGIN TRANSACTION")
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.session.execute("COMMIT")?;
        self.views_pending = false;
        Ok(())
    }

    pub(crate) fn rollback(&mut self) -> Result<()> {
        self.session.execute("ROLLBACK")?;
        if self.views_pending {
            self.views_defined = false;
            self.views_pending = false;
        }
        Ok(())
    }

    pub(crate) fn execute(&self, sql: &str) -> Result<()> {
        self.session.execute(sql)
    }

    pub(crate) fn write_parquet(&self, table: &str, path: &Path) -> Result<()> {
        self.session.write_parquet(table, path)
    }

    pub(crate) fn load_parquet(&self, table: &str, select: &str, path: &Path) -> Result<()> {
        self.session.load_parquet(table, select, path)
    }

    pub(crate) fn count(&self, relation: &str) -> Result<u64> {
        self.session
            .one_value(&format!("SELECT count(*) FROM \"{relation}\""))?
            .as_u64()
            .context("count(*) is not an integer")
    }

    /// Row count and order-independent digest of a result: the sum, mod 2^256, of the sha256 of each
    /// row's cells as a JSON array. Streamed, so a 591k-row carry is never held as JSON (RFC-0059 S1
    /// gate: materialising it cost 474 MiB of resident memory).
    pub(crate) fn digest(&self, sql: &str) -> Result<(u64, String)> {
        use sha2::{Digest, Sha256};
        let (mut count, mut sum) = (0u64, [0u8; 32]);
        self.session.for_each_row(sql, &mut |cells| {
            let h: [u8; 32] = Sha256::digest(serde_json::to_vec(cells)?).into();
            let mut carry = 0u16;
            for i in (0..32).rev() {
                let s = sum[i] as u16 + h[i] as u16 + carry;
                sum[i] = s as u8;
                carry = s >> 8;
            }
            count += 1;
            Ok(())
        })?;
        Ok((count, hex::encode(sum)))
    }

    /// A result as Arrow, for head snapshots (RFC-0059 §4), whose size must be counted exactly.
    pub(crate) fn arrow(&self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        self.session.query_arrow(sql)
    }

    pub(crate) fn rows(&self, sql: &str) -> Result<Vec<Value>> {
        Ok(self.session.collect(sql, None)?.rows)
    }
}

/// The table name out of a catalog error (Burrmill restates DataFusion's in DuckDB's words), if that
/// is what this is.
///
/// Format-dependent by necessity - the engine gives no structured error code for it - so it fails soft:
/// an unrecognised message simply yields `None` and the raw error is reported instead of a
/// half-parsed one.
pub(crate) fn missing_table_of(err: &str) -> Option<String> {
    let after = err.split("Table with name ").nth(1)?;
    let name = after.split_whitespace().next()?;
    (!name.is_empty()).then(|| name.to_string())
}

/// The nest's declared tables and their `(column, storage)`, from `schema.json`.
#[cfg(feature = "folds")]
pub(crate) fn declared_columns(dir: &Path) -> Vec<(String, Vec<(String, String)>)> {
    schema_columns(dir)
}

/// (table, [(column, storage)]) for every declared table, from the nest's `schema.json`. Empty if
/// the file is absent/unparseable. Drives both the derived `*_dec` columns and the empty typed views.
fn schema_columns(dir: &Path) -> Vec<(String, Vec<(String, String)>)> {
    let mut out = Vec::new();
    let Ok(raw) = std::fs::read_to_string(dir.join("schema.json")) else {
        return out;
    };
    let Ok(v) = serde_json::from_str::<Value>(&raw) else {
        return out;
    };
    for t in v
        .get("tables")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(name) = t.get("table").and_then(Value::as_str) else {
            continue;
        };
        let cols: Vec<(String, String)> = t
            .get("columns")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|c| {
                Some((
                    c.get("name")?.as_str()?.to_string(),
                    c.get("storage")?.as_str()?.to_string(),
                ))
            })
            .collect();
        out.push((name.to_string(), cols));
    }
    out
}

#[cfg(test)]
mod tests {
    /// #1598: an entity's relation takes the types its plan declares. It exists with no rows, and a
    /// sum past what a JSON number holds stays an integer rather than becoming text.
    #[test]
    fn an_entity_relation_keeps_its_declared_types_empty_or_large() {
        use crate::entity_expr::Type;
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("entities")).unwrap();
        std::fs::write(
            dir.path().join("entities.toml"),
            "[[entities]]\nname='totals'\nsql='entities/totals.sql'\nkey=['k']\nmax_rows=10\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT k, sum(v) AS n FROM t GROUP BY k",
        )
        .unwrap();
        crate::entities::load(dir.path()).unwrap();
        super::hold_relation_types(
            dir.path(),
            "totals",
            &["k".into(), "n".into()],
            &[Some(Type::Str), Some(Type::Int)],
        );
        let guard = || super::QueryGuard {
            timeout: std::time::Duration::from_secs(10),
            max_rows: 10,
        };
        let run = |rows: Vec<serde_json::Value>, sql: &str| {
            let hot: super::HotRows = [("totals".to_string(), rows)].into_iter().collect();
            super::query_hot_cold(dir.path(), sql, guard(), &hot, u64::MAX, &[])
        };

        let empty = run(Vec::new(), "SELECT count(*) AS c, sum(n) AS s FROM totals").unwrap();
        assert_eq!(empty.rows[0]["c"], serde_json::json!(0), "{:?}", empty.rows);

        // What `sql_cell` renders for an i128 beyond a JSON number: a string.
        let big = serde_json::json!({"k": "a", "n": "1000000000000000000000000000000"});
        let out = run(vec![big], "SELECT n + 1 AS m FROM totals").unwrap();
        assert_eq!(
            out.rows[0]["m"].to_string().trim_matches('"'),
            "1000000000000000000000000000001",
            "{:?}",
            out.rows
        );

        // The cached session keeps the view the queries above defined. An entity that has since
        // faulted is left out of `hot`, and must not answer from that view; nor may one whose rows
        // do not match their declared types.
        let hot: super::HotRows = Default::default();
        let gone = super::query_hot_cold(
            dir.path(),
            "SELECT count(*) AS faulted FROM totals",
            guard(),
            &hot,
            u64::MAX,
            &[],
        );
        assert!(
            gone.is_err(),
            "a faulted entity answered: {:?}",
            gone.map(|o| o.rows)
        );
        let bad = serde_json::json!({"k": "a", "n": "not an integer"});
        let mistyped = run(vec![bad], "SELECT count(*) AS mistyped FROM totals");
        assert!(
            mistyped.is_err(),
            "a mistyped load answered: {:?}",
            mistyped.map(|o| o.rows)
        );
    }

    /// #1679: an entity's integer is an i128, and Burrmill's integer stops at DECIMAL(38,0). A value
    /// past 10^38 - 1 is refused at load, naming the entity and the value, not at execution.
    #[test]
    fn an_entity_value_past_decimal_38_is_refused_at_load_by_name() {
        use crate::entity_expr::Type;
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("entities")).unwrap();
        std::fs::write(
            dir.path().join("entities.toml"),
            "[[entities]]\nname='totals'\nsql='entities/totals.sql'\nkey=['k']\nmax_rows=10\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("entities/totals.sql"),
            "SELECT k, sum(v) AS n FROM t GROUP BY k",
        )
        .unwrap();
        crate::entities::load(dir.path()).unwrap();
        super::hold_relation_types(
            dir.path(),
            "totals",
            &["k".into(), "n".into()],
            &[Some(Type::Str), Some(Type::Int)],
        );
        let run = |n: String| {
            let row = serde_json::json!({"k": "a", "n": n});
            let hot: super::HotRows = [("totals".to_string(), vec![row])].into_iter().collect();
            let guard = super::QueryGuard {
                timeout: std::time::Duration::from_secs(10),
                max_rows: 10,
            };
            super::query_hot_cold(
                dir.path(),
                "SELECT n FROM totals",
                guard,
                &hot,
                u64::MAX,
                &[],
            )
        };
        let largest = "9".repeat(38);
        let out = run(largest.clone()).unwrap();
        assert_eq!(
            out.rows[0]["n"].to_string().trim_matches('"'),
            largest,
            "{:?}",
            out.rows
        );
        for past in [
            format!("1{}", "0".repeat(38)),
            i128::MAX.to_string(),
            format!("-1{}", "0".repeat(38)),
        ] {
            let err = format!("{:#}", run(past.clone()).unwrap_err());
            assert!(
                err.contains("entity totals") && err.contains(&past),
                "{past} was not refused at load by entity and value: {err}"
            );
        }
    }

    #[cfg(feature = "graph")]
    #[test]
    fn relation_membership_preserves_existence_with_nulls_and_duplicates() {
        crate::engine::on_bare(
            relation_membership_preserves_existence_with_nulls_and_duplicates_on,
        );
    }

    #[cfg(feature = "graph")]
    fn relation_membership_preserves_existence_with_nulls_and_duplicates_on(
        conn: &dyn crate::engine::Session,
    ) {
        conn.execute(
            "CREATE TABLE token AS SELECT CAST(a AS VARCHAR) AS id, CAST(b AS VARCHAR) AS symbol \
             FROM (VALUES ('yes', 'WETH'), ('yes', 'WETH'), ('no', 'OTHER'), (NULL, 'WETH')) v(a, b)",
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE pool AS SELECT CAST(a AS VARCHAR) AS id, CAST(b AS VARCHAR) AS token0 \
             FROM (VALUES ('a', 'yes'), ('b', 'no'), ('c', 'missing'), ('d', NULL)) v(a, b)",
        )
        .unwrap();
        // Each statement here selects one column: the ids, in the statement's own order.
        let ids = |sql: &str| -> Vec<String> {
            let rows = conn
                .collect(sql, None)
                .map_err(|e| anyhow::anyhow!("{e:?}"))
                .unwrap()
                .rows;
            rows.iter()
                .map(|r| {
                    r.as_object()
                        .unwrap()
                        .values()
                        .next()
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string()
                })
                .collect()
        };
        let schema = crate::graph_schema::parse("type Pool @entity { id: ID! token0: Token! } type Token @entity { id: ID! symbol: String! }").unwrap();
        let roots = crate::graph_query::parse(
            r#"{ pools(where: { token0_: { symbol: "WETH" } }) { id } }"#,
        )
        .unwrap();
        let compiled = crate::graph_query::compile(&schema, &roots[0]).unwrap();
        assert_eq!(ids(&compiled.sql), ["a"]);
        // SQL NULL must be false just as EXISTS is, even when the child set contains NULL.
        let predicate = compiled
            .sql
            .split_once(" WHERE ")
            .unwrap()
            .1
            .rsplit_once(" ORDER BY ")
            .unwrap()
            .0;
        let sql = format!("SELECT b.id FROM pool b WHERE NOT ({predicate}) ORDER BY b.id");
        assert_eq!(ids(&sql), ["b", "c", "d"]);
    }

    use super::*;

    #[test]
    fn dependency_discovery_distinguishes_local_ctes_from_entity_views() {
        let conn = crate::engine::bare();
        let conn = &*conn;
        let sql = "WITH allocation AS (SELECT * FROM raw_fees), provision AS (SELECT * FROM allocation) SELECT * FROM provision";
        assert_eq!(
            super::base_tables_in(conn, sql).unwrap(),
            std::collections::BTreeSet::from(["raw_fees".to_string()]),
        );
        for (sql, expected) in [
            ("WITH z AS (SELECT * FROM raw_fees), a AS (SELECT * FROM z) SELECT * FROM a", vec!["raw_fees"]),
            ("WITH allocation AS (SELECT * FROM allocation) SELECT * FROM allocation", vec!["allocation"]),
            ("WITH earlier AS (SELECT * FROM allocation), allocation AS (SELECT * FROM raw_fees) SELECT * FROM earlier", vec!["allocation", "raw_fees"]),
            ("WITH allocation AS (SELECT * FROM raw_fees) SELECT * FROM main.allocation", vec!["allocation", "raw_fees"]),
            ("WITH allocation AS (SELECT * FROM raw_fees) SELECT * FROM (WITH allocation AS (SELECT * FROM other_fees) SELECT * FROM allocation) nested CROSS JOIN allocation", vec!["other_fees", "raw_fees"]),
            ("WITH RECURSIVE walk AS (SELECT * FROM raw_fees UNION ALL SELECT * FROM walk) SELECT * FROM walk", vec!["raw_fees"]),
        ] {
            assert_eq!(super::base_tables_in(conn, sql).unwrap(), expected.into_iter().map(str::to_string).collect::<std::collections::BTreeSet<_>>(), "{sql}");
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/report.sql"),
            format!(
            "CREATE VIEW allocation AS SELECT * FROM unrelated_fees; CREATE VIEW report AS {sql};"
        ),
        )
        .unwrap();
        let wanted = std::collections::BTreeSet::from(["report".to_string()]);
        assert_eq!(
            super::reachable_tables(conn, dir.path(), &wanted).unwrap(),
            std::collections::BTreeSet::from(["report".to_string(), "raw_fees".to_string()])
        );
        // The separate security/function walk still inspects every CTE definition.
        let functions = super::table_refs_in(
            conn,
            "WITH allocation AS (SELECT * FROM read_csv('secret.csv')) SELECT * FROM allocation",
            "TABLE_FUNCTION",
        )
        .unwrap();
        assert!(functions.contains("read_csv"));
    }

    #[test]
    fn dependency_closure_reaches_sources_beyond_eight_views() {
        crate::engine::on_bare(dependency_closure_reaches_sources_beyond_eight_views_on);
    }

    fn dependency_closure_reaches_sources_beyond_eight_views_on(conn: &dyn Session) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        conn.execute("CREATE TABLE source_facts AS SELECT 7 AS amount")
            .unwrap();
        for i in 0..12 {
            let source = if i == 0 {
                "source_facts".to_string()
            } else {
                format!("layer_{}", i - 1)
            };
            let sql = format!("CREATE VIEW layer_{i} AS SELECT amount FROM {source};");
            std::fs::write(dir.path().join(format!("views/{i:02}.sql")), &sql).unwrap();
            conn.execute(&sql).unwrap();
        }
        let named = ["layer_11".to_string()].into_iter().collect();
        let expected: std::collections::BTreeSet<_> = (0..12)
            .map(|i| format!("layer_{i}"))
            .chain(std::iter::once("source_facts".to_string()))
            .collect();
        assert_eq!(
            reachable_tables(conn, dir.path(), &named).unwrap(),
            expected
        );
        assert_eq!(expand_through_views(conn, &named), expected);
        // Even invalid, cyclic authored definitions must terminate during discovery.
        std::fs::write(dir.path().join("views/12.sql"),
            "CREATE VIEW cycle_a AS SELECT * FROM cycle_b; CREATE VIEW cycle_b AS SELECT * FROM cycle_a;").unwrap();
        let cycle = ["cycle_a".to_string(), "cycle_b".to_string()]
            .into_iter()
            .collect();
        assert_eq!(reachable_tables(conn, dir.path(), &cycle).unwrap(), cycle);
    }
    /// RFC-0059: a two-sided window exposes only `(after, through]`, across sealed and hot, and names
    /// only the segments that overlap it. The count alone cannot tell pruning from the predicate.
    #[test]
    fn a_fact_window_exposes_only_its_range_and_names_only_overlapping_segments() {
        crate::engine::on_bare(
            a_fact_window_exposes_only_its_range_and_names_only_overlapping_segments_on,
        );
    }

    fn a_fact_window_exposes_only_its_range_and_names_only_overlapping_segments_on(
        conn: &dyn Session,
    ) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t","columns":[{"name":"block_number","storage":"u64"}]}]}"#,
        )
        .unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        for (from, to) in [(1, 10), (11, 20), (21, 30)] {
            let rows: Vec<String> = (from..=to)
                .map(|b| format!(r#"{{"table":"t","block_number":{b}}}"#))
                .collect();
            crate::seal::seal_range(dir.path(), &rows, from, to).unwrap();
        }
        let mut hot = HotRows::new();
        hot.insert(
            "t".into(),
            (21..=35)
                .map(|b| serde_json::json!({ "block_number": b }))
                .collect(),
        );
        let count = |after: Option<u64>, through: Option<u64>| {
            let defined = define_views_bound(
                conn,
                dir.path(),
                &hot,
                30,
                &Default::default(),
                &[],
                None,
                FactWindow { after, through },
                false,
            )
            .unwrap();
            let n = conn
                .one_value("SELECT count(*) FROM t")
                .unwrap()
                .as_u64()
                .unwrap();
            // `__hot` is not a name a statement can read. The staged rows are what was loaded.
            let hot_loaded = conn.staged_hot_len("t") as u64;
            (n, defined.tables["t"].segments, hot_loaded)
        };
        assert_eq!(
            count(Some(15), Some(25)),
            (10, 2, 0),
            "(15, 25] spans two segments"
        );
        assert_eq!(
            count(Some(20), Some(30)),
            (10, 1, 0),
            "(20, 30] is the third segment alone"
        );
        // Hot rows 21..=30 were sealed but not yet pruned: the watermark keeps them out.
        assert_eq!(
            count(Some(25), Some(33)),
            (8, 1, 3),
            "5 sealed + 3 hot, and only those 3 loaded"
        );
        assert_eq!(count(Some(30), Some(35)), (5, 0, 5), "hot alone");
        assert_eq!(count(None, None), (35, 3, 5), "unbounded is history");
    }

    #[test]
    fn historical_facts_are_filtered_before_aggregation_across_hot_and_cold() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("schema.json"), r#"{"tables":[{"table":"changes","columns":[{"name":"block_number","storage":"u64"},{"name":"value","storage":"varchar"}]}]}"#).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/balance.sql"),
            "CREATE VIEW balance AS SELECT sum(CAST(value AS BIGINT)) AS total FROM changes;",
        )
        .unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        crate::seal::seal_range(
            dir.path(),
            &[
                r#"{"table":"changes","block_number":10,"value":"5"}"#.into(),
                r#"{"table":"changes","block_number":20,"value":"7"}"#.into(),
            ],
            10,
            20,
        )
        .unwrap();
        let mut hot = HotRows::new();
        hot.insert(
            "changes".into(),
            vec![
                serde_json::json!({"block_number":20,"value":"7"}),
                serde_json::json!({"block_number":30,"value":"11"}),
            ],
        );
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 100,
        };
        // Alternating blocks also proves a cached historical connection cannot leak a later view.
        for (block, expected) in [(10, 5), (30, 23), (20, 12), (10, 5)] {
            let result = query_hot_cold_at(
                dir.path(),
                "SELECT total FROM balance",
                guard,
                &hot,
                20,
                &[],
                block,
            )
            .unwrap();
            assert_eq!(result.rows[0]["total"], Value::String(expected.to_string()));
            assert!(!result.degraded());
        }
        let current = query_hot_cold(
            dir.path(),
            "SELECT total FROM balance",
            guard,
            &hot,
            20,
            &[],
        )
        .unwrap();
        assert_eq!(current.rows[0]["total"], Value::String("23".into()));
    }

    #[test]
    fn historical_reads_exclude_unversioned_offchain_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("prices.csv");
        std::fs::write(&source, "symbol,price\nGRT,2\n").unwrap();
        crate::offchain::drop_file(dir.path(), &source, "prices").unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 100,
        };
        let hot = HotRows::new();
        let sql = "SELECT symbol FROM offchain__prices";
        let current = query_hot_cold(dir.path(), sql, guard, &hot, 0, &[]).unwrap();
        assert_eq!(current.rows[0]["symbol"], "GRT");
        let catalogue = crate::offchain::load(dir.path()).unwrap();
        assert_eq!(
            current.offchain.unwrap()["offchain__prices"],
            vec![catalogue.tables["prices"][0].hash.clone()]
        );
        let error = query_hot_cold_at(dir.path(), sql, guard, &hot, 0, &[], 10).unwrap_err();
        assert!(format!("{error:#}").contains("offchain__prices"));
        let historical =
            query_hot_cold_at(dir.path(), "SELECT 1 AS value", guard, &hot, 0, &[], 10).unwrap();
        assert!(historical.offchain.unwrap().is_empty());
        // Switching back to current reads must restore both the view and its provenance.
        let current = query_hot_cold(dir.path(), sql, guard, &hot, 0, &[]).unwrap();
        assert_eq!(current.rows[0]["symbol"], "GRT");
        assert!(current.offchain.unwrap().contains_key("offchain__prices"));
    }

    /// #1678: a table's snapshots are unioned by name. Two integer widths of one column widen to the
    /// wider (burrmill #36); a column whose kind differs will not bind, and a statement naming the view
    /// is told which column, not that it is absent.
    #[test]
    fn a_snapshot_view_that_will_not_bind_names_its_column() {
        use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, column: ArrayRef| {
            let path = dir.path().join(name);
            let batch = arrow::record_batch::RecordBatch::try_from_iter([("v", column)]).unwrap();
            let file = std::fs::File::create(&path).unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            path
        };
        let narrow = write("a.parquet", Arc::new(Int32Array::from(vec![1, 2])));
        let wide = write("b.parquet", Arc::new(Int64Array::from(vec![3])));
        crate::offchain::drop_file(dir.path(), &narrow, "prices").unwrap();
        crate::offchain::drop_file(dir.path(), &wide, "prices").unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 100,
        };
        let run = |sql: &str| query_hot_cold(dir.path(), sql, guard, &HotRows::new(), 0, &[]);
        let widened = run("SELECT count(*) AS n FROM offchain__prices").expect("two widths widen");
        assert_eq!(widened.rows[0]["n"], serde_json::json!(3));

        let text = write("c.parquet", Arc::new(StringArray::from(vec!["four"])));
        crate::offchain::drop_file(dir.path(), &text, "prices").unwrap();
        let err = format!(
            "{:#}",
            run("SELECT count(*) AS n FROM offchain__prices").unwrap_err()
        );
        assert!(
            err.contains("offchain__prices") && err.contains("'v'") && err.contains("Utf8"),
            "the view went without naming its column: {err}"
        );
        assert!(run("SELECT 1 AS one").is_ok(), "a statement not naming it");
    }

    #[test]
    fn historical_queries_refuse_unstamped_current_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut hot = HotRows::new();
        hot.insert(
            "current_balance".into(),
            vec![serde_json::json!({"id":"a","balance":"99"})],
        );
        let result = query_hot_cold_at(
            dir.path(),
            "SELECT * FROM current_balance",
            QueryGuard {
                timeout: Duration::from_secs(5),
                max_rows: 100,
            },
            &hot,
            0,
            &[],
            10,
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("block-stamped facts"));
    }

    #[test]
    fn historical_queries_refuse_unstamped_archived_facts() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("schema.json"), r#"{"tables":[{"table":"changes","columns":[{"name":"block_number","storage":"u64"},{"name":"value","storage":"varchar"}]}]}"#).unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        crate::seal::seal_range(
            dir.path(),
            &[
                r#"{"table":"changes","value":"5"}"#.into(),
                r#"{"table":"changes","value":"7"}"#.into(),
            ],
            10,
            20,
        )
        .unwrap();
        let result = query_hot_cold_at(
            dir.path(),
            "SELECT sum(CAST(value AS BIGINT)) FROM changes",
            QueryGuard {
                timeout: Duration::from_secs(5),
                max_rows: 100,
            },
            &HotRows::new(),
            20,
            &[],
            10,
        );
        let error = format!("{:#}", result.unwrap_err());
        assert!(error.contains("unstamped archived row"), "{error}");
    }

    #[test]
    fn rejects_non_select() {
        let dir = tempfile::tempdir().unwrap();
        assert!(query(dir.path(), "DROP TABLE x").is_err());
    }

    /// The `/sql` row cap bounds the Rust-side result buffer and flags truncation precisely.
    #[test]
    fn guarded_query_caps_rows_and_flags_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"t__transfer","from":"0xa","to":"0xb","value":"1","block_number":1,"tx_hash":"0xt","log_index":0}"#.to_string(),
            r#"{"table":"t__transfer","from":"0xa","to":"0xc","value":"2","block_number":1,"tx_hash":"0xt","log_index":1}"#.to_string(),
            r#"{"table":"t__transfer","from":"0xa","to":"0xd","value":"3","block_number":1,"tx_hash":"0xt","log_index":2}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 1, 1).unwrap();

        // Cap below the row count: truncated to max_rows and flagged.
        let guard = QueryGuard {
            timeout: Duration::from_secs(30),
            max_rows: 2,
        };
        let out = query_guarded(dir.path(), r#"SELECT * FROM "t__transfer""#, guard).unwrap();
        assert_eq!(out.rows.len(), 2, "capped at max_rows");
        assert!(out.truncated, "flagged when more rows existed");

        // Cap at the exact row count: everything returned, not flagged (the +1 sentinel finds no more).
        let guard = QueryGuard {
            timeout: Duration::from_secs(30),
            max_rows: 3,
        };
        let out = query_guarded(dir.path(), r#"SELECT * FROM "t__transfer""#, guard).unwrap();
        assert_eq!(out.rows.len(), 3);
        assert!(!out.truncated);
    }

    #[test]
    fn guarded_query_formats_wide_scaled_decimal_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 10,
        };
        let out = query_guarded(
            dir.path(),
            "SELECT CAST('100000000000000000000000000000' AS DECIMAL(38,2)) AS amount; -- terminal",
            guard,
        )
        .unwrap();
        assert_eq!(
            out.rows[0]["amount"],
            Value::String("100000000000000000000000000000.00".into())
        );
    }

    /// RFC-0009 step 6: a factory nest gets an auto-generated `{template}__children` view over the
    /// sealed factory events - the discovered children with provenance, de-duplicated to the earliest
    /// discovery per address. Answers "which pools, discovered when, by whom" in one query.
    #[test]
    fn children_view_lists_discovered_contracts() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(crate::config::CONFIG_FILE),
            r#"
[nest]
name="univ3"
chain="mainnet"
chain_id=1
rpc_urls=["https://rpc"]
[[contracts]]
alias="factory"
address="0x1f98431c8ad98523631ae4a59f267346ea31f984"
abi="abis/factory.json"
[[templates]]
name="pool"
abi="abis/pool.json"
[[factories]]
watch="factory"
event="PoolCreated"
child_param="pool"
template="pool"
"#,
        )
        .unwrap();
        // Seal two PoolCreated events (pool_a, pool_b) + a duplicate discovery of pool_a (later block,
        // must be de-duplicated to the earliest).
        let rows = vec![
            r#"{"table":"factory__pool_created","pool":"0xAAAA000000000000000000000000000000000001","block_number":10,"log_index":0,"block_timestamp":1700000010,"tx_hash":"0xt","address":"0x1f98431c8ad98523631ae4a59f267346ea31f984"}"#.to_string(),
            r#"{"table":"factory__pool_created","pool":"0xBBBB000000000000000000000000000000000002","block_number":12,"log_index":1,"block_timestamp":1700000012,"tx_hash":"0xt","address":"0x1f98431c8ad98523631ae4a59f267346ea31f984"}"#.to_string(),
            r#"{"table":"factory__pool_created","pool":"0xAAAA000000000000000000000000000000000001","block_number":20,"log_index":0,"block_timestamp":1700000020,"tx_hash":"0xt","address":"0x1f98431c8ad98523631ae4a59f267346ea31f984"}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &rows, 10, 20).unwrap();

        let count = query(dir.path(), r#"SELECT count(*) AS n FROM "pool__children""#).unwrap();
        assert_eq!(
            count[0]["n"],
            Value::from(2u64),
            "two distinct discovered pools"
        );
        let a = query(
            dir.path(),
            r#"SELECT discovered_block, discovered_timestamp, parent_address FROM "pool__children" WHERE address = '0xaaaa000000000000000000000000000000000001'"#,
        )
        .unwrap();
        assert_eq!(
            a[0]["discovered_block"],
            Value::from(10u64),
            "earliest discovery wins"
        );
        assert_eq!(a[0]["discovered_timestamp"], Value::from(1700000010u64));
        assert_eq!(
            a[0]["parent_address"],
            Value::from("0x1f98431c8ad98523631ae4a59f267346ea31f984")
        );
    }

    /// A unit still asking for DuckDB is told, not served by something else.
    #[test]
    fn a_unit_asking_for_duckdb_is_refused() {
        for ok in [None, Some(""), Some("burrmill"), Some(" burrmill ")] {
            check_engine_choice(ok).unwrap();
        }
        for gone in ["duckdb", "shadow", "checked", "burmill"] {
            let err = check_engine_choice(Some(gone)).unwrap_err().to_string();
            assert!(err.contains(gone) && err.contains("no DuckDB"), "{err}");
        }
    }

    /// A runaway query is interrupted by the watchdog and surfaced as a timeout, not left to hang:
    /// both a recursion that emits no batch until it ends and a join that reads no Parquet.
    ///
    /// Either one, left to finish, would answer `Ok`, so the guard's own `QueryBudgetExceeded` is the
    /// assertion. No elapsed-time bound: on a loaded box the interrupt took over 6 s to land in the
    /// join, with the guard's decision right every time (#1816).
    #[test]
    fn guarded_query_times_out_on_a_runaway() {
        let dir = tempfile::tempdir().unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_millis(250),
            max_rows: 1000,
        };
        // Which guard fires is the assertion, so no other test's spill cap may be in the environment.
        let _env = crate::analytics_budget::tests::env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for runaway in [
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 1000000000) SELECT count(*) FROM t",
            "SELECT count(*) FROM range(1000000) a, range(1000000) b WHERE a.range + b.range = -1",
        ] {
            let err = query_guarded(dir.path(), runaway, guard).unwrap_err();
            assert_eq!(
                err.downcast_ref::<QueryBudgetExceeded>(),
                Some(&QueryBudgetExceeded { secs: 0 }),
                "{runaway}: expected the watchdog's timeout, got: {err:#}"
            );
        }
    }

    #[test]
    fn queries_a_sealed_per_table_segment() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"5","block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xc","value":"7","block_number":10,"tx_hash":"0xt","log_index":1}"#.to_string(),
            r#"{"table":"usdc__approval","owner":"0xa","spender":"0xd","value":"9","block_number":10,"tx_hash":"0xt","log_index":2}"#.to_string(),
            r#"{"table":"usdc__zmint","to":"0xe","value":"3","block_number":10,"tx_hash":"0xt","log_index":3}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 10, 10).unwrap();

        // Each table is its own view.
        let t = query(dir.path(), r#"SELECT count(*) AS n FROM "usdc__transfer""#).unwrap();
        assert_eq!(t[0]["n"], Value::from(2u64));
        let a = query(dir.path(), r#"SELECT count(*) AS n FROM "usdc__approval""#).unwrap();
        assert_eq!(a[0]["n"], Value::from(1u64));

        // Point-read searches all tables by (block, log_index).
        let one = get_row(dir.path(), 10, 1).unwrap().unwrap();
        assert_eq!(one["to"], Value::from("0xc"));
        let guarded = get_row_guarded(
            dir.path(),
            10,
            1,
            QueryGuard {
                timeout: Duration::from_secs(30),
                max_rows: 1,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(guarded["to"], one["to"]);
        let appr = get_row(dir.path(), 10, 2).unwrap().unwrap();
        assert_eq!(appr["spender"], Value::from("0xd"));

        // #1574: one probe then one read, whichever table holds the row, not a query per table. The
        // row is in the last of three tables, so asking each in turn would take three.
        let queries = |f: &dyn Fn()| {
            let before = QUERIES.with(|n| n.get());
            f();
            QUERIES.with(|n| n.get()) - before
        };
        let n = queries(&|| {
            let last = get_row(dir.path(), 10, 3).unwrap().unwrap();
            assert_eq!(last["to"], Value::from("0xe"));
        });
        assert_eq!(n, 2, "a point read in the last table");
        let n = queries(&|| assert_eq!(get_row(dir.path(), 10, 4).unwrap(), None));
        assert_eq!(n, 1, "a miss is the probe alone");
    }

    fn sealed_bytes(dir: &Path, table: &str) -> u64 {
        crate::seal::load_manifest(dir).unwrap().tables[table]
            .iter()
            .map(|s| {
                std::fs::metadata(crate::seal::segment_path(dir, &s.file, &s.hash))
                    .unwrap()
                    .len()
            })
            .sum()
    }

    /// A small table and a wide one, sealed together.
    fn two_sealed_tables() -> (tempfile::TempDir, u64, u64) {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = vec![
            r#"{"table":"t__transfer","from":"0xa","to":"0xb","value":"1","block_number":1,"tx_hash":"0xt","log_index":0}"#.to_string(),
        ];
        for i in 0..400 {
            rows.push(format!(
                r#"{{"table":"t__big","memo":"{}","block_number":1,"tx_hash":"0x{i:x}","log_index":{i}}}"#,
                "wide ".repeat(40 + i)
            ));
        }
        crate::seal::seal_range(dir.path(), &rows, 1, 1).unwrap();
        let small = sealed_bytes(dir.path(), "t__transfer");
        let big = sealed_bytes(dir.path(), "t__big");
        assert!(
            big > small * 2,
            "fixture must separate the tables: {small} vs {big}"
        );
        (dir, small, big)
    }

    fn named(
        dir: &Path,
        sql: &str,
        cap: u64,
        hot_bytes: u64,
        execute: bool,
        pinned_catalogue: Option<Option<String>>,
    ) -> Result<QueryOutput> {
        query_named(
            dir,
            sql,
            QueryGuard {
                timeout: Duration::from_secs(30),
                max_rows: 100,
            },
            &HotRows::new(),
            u64::MAX,
            &[],
            &NamedAdmission {
                cap,
                hot_bytes,
                pinned_catalogue,
                execute,
            },
        )
    }

    fn refusal(result: Result<QueryOutput>) -> AdmissionRefusal {
        match result {
            Ok(out) => panic!("admitted: {:?}", out.scan_bound),
            Err(e) => match e.downcast::<AdmissionRefusal>() {
                Ok(refusal) => refusal,
                Err(other) => panic!("not an admission refusal: {other:#}"),
            },
        }
    }

    #[test]
    fn a_named_bound_charges_every_parquet_scan_the_widest_reachable_table() {
        use sha2::Digest;
        let (dir, small, big) = two_sealed_tables();
        let bound = |sql: &str| {
            named(dir.path(), sql, u64::MAX, 0, false, None)
                .unwrap()
                .scan_bound
                .unwrap()
        };

        let once = bound(r#"SELECT * FROM "t__transfer""#);
        assert_eq!((once.scan_operators, once.cold_bytes), (1, small));
        assert_eq!(once.tables.keys().collect::<Vec<_>>(), ["t__transfer"]);
        let manifest = std::fs::read(dir.path().join("segments/manifest.json")).unwrap();
        assert_eq!(
            once.catalogue_hash.as_deref(),
            Some(hex::encode(sha2::Sha256::digest(&manifest)).as_str())
        );

        let self_join =
            bound(r#"SELECT * FROM "t__transfer" a JOIN "t__transfer" b USING (log_index)"#);
        assert_eq!(
            (self_join.scan_operators, self_join.cold_bytes),
            (2, 2 * small),
            "a self-join reads the table twice and must not be deduplicated"
        );

        // Neither scan can be charged less than the widest table it might be reading.
        let across = bound(r#"SELECT * FROM "t__transfer" a JOIN "t__big" b USING (log_index)"#);
        assert_eq!((across.scan_operators, across.cold_bytes), (2, 2 * big));
    }

    #[test]
    fn a_named_query_is_refused_before_it_is_evaluated() {
        let (dir, small, _) = two_sealed_tables();
        let tripwire = r#"SELECT error('evaluated') FROM "t__transfer""#;
        let planned = named(dir.path(), tripwire, u64::MAX, 0, false, None).unwrap();
        assert!(planned.rows.is_empty() && planned.scan_bound.is_some());
        let ran = named(dir.path(), tripwire, u64::MAX, 0, true, None).unwrap_err();
        assert!(format!("{ran:#}").contains("evaluated"), "{ran:#}");

        match refusal(named(dir.path(), tripwire, small - 1, 0, true, None)) {
            AdmissionRefusal::OverCap(b) => assert_eq!((b.cold_bytes, b.cap), (small, small - 1)),
            other => panic!("{other}"),
        }
        // Exactly at the cap is admitted, cold or hot alike.
        assert!(named(dir.path(), tripwire, small, 0, false, None).is_ok());
        // The hot copy spends the same budget: under the cap alone, over it together.
        match refusal(named(dir.path(), tripwire, small + 10, 11, true, None)) {
            AdmissionRefusal::OverCap(b) => assert_eq!(b.hot_bytes, 11),
            other => panic!("{other}"),
        }
        assert!(named(dir.path(), tripwire, small + 10, 10, false, None).is_ok());

        let moved = refusal(named(
            dir.path(),
            tripwire,
            u64::MAX,
            0,
            true,
            Some(Some("0".repeat(64))),
        ));
        assert!(
            matches!(moved, AdmissionRefusal::StaleCatalogue { .. }),
            "{moved}"
        );
    }

    #[test]
    fn a_plan_that_can_rescan_or_is_unrecognised_is_not_bounded() {
        // A correlated subquery that is not flattened: one scan per outer row.
        let (dir, _, _) = two_sealed_tables();
        let correlated = r#"SELECT * FROM "t__transfer" a WHERE a.log_index =
            (SELECT max(b.log_index) FROM "t__big" b WHERE b.log_index < a.log_index)"#;
        match refusal(named(dir.path(), correlated, u64::MAX, 0, false, None)) {
            AdmissionRefusal::Unboundable(why) => {
                assert!(why.contains("NestedLoopJoinExec"), "{why}")
            }
            other => panic!("{other}"),
        }
    }

    #[test]
    fn ctes_and_authored_views_are_bounded_and_a_view_that_reads_files_is_not() {
        let (dir, small, big) = two_sealed_tables();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-wide.sql"),
            "CREATE VIEW wide AS\nSELECT * FROM \"t__big\";\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/20-sneaky.sql"),
            "CREATE VIEW sneaky AS SELECT * FROM read_csv('elsewhere.csv');\n",
        )
        .unwrap();

        let cte = named(
            dir.path(),
            r#"WITH c AS (SELECT * FROM "t__transfer") SELECT count(*) FROM c x JOIN c y USING (log_index)"#,
            u64::MAX,
            0,
            false,
            None,
        )
        .unwrap()
        .scan_bound
        .unwrap();
        assert!(cte.cold_bytes <= 2 * small, "{cte:?}");

        let view = named(
            dir.path(),
            "SELECT memo FROM wide",
            u64::MAX,
            0,
            false,
            None,
        )
        .unwrap()
        .scan_bound
        .unwrap();
        assert_eq!((view.scan_operators, view.cold_bytes), (1, big));

        match refusal(named(
            dir.path(),
            "SELECT * FROM sneaky",
            u64::MAX,
            0,
            false,
            None,
        )) {
            AdmissionRefusal::Unboundable(why) => assert!(why.contains("read_csv"), "{why}"),
            other => panic!("{other}"),
        }
    }

    #[test]
    fn a_named_query_refuses_stacked_sql_before_planning_it() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("must-not-exist.csv");
        let sql = format!("SELECT 1; COPY (SELECT 2) TO '{}'", target.display());
        assert!(named(dir.path(), &sql, u64::MAX, 0, false, None).is_err());
        assert!(!target.exists());
    }

    #[test]
    fn query_survives_a_missing_segment_file() {
        // A segment listed in the manifest but gone from disk (quarantined as corrupt / removed) must
        // not fail the whole query - its cold data is skipped, the surviving segment still answers.
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal: this is about a missing file, not about the table floor (#1150).
        crate::seal::test_set_table_floor(dir.path(), 0);
        let row = |b: u64| {
            format!(
                r#"{{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"1","block_number":{b},"tx_hash":"0xt","log_index":0}}"#
            )
        };
        crate::seal::seal_range(dir.path(), &[row(10)], 10, 10).unwrap();
        crate::seal::seal_range(dir.path(), &[row(11)], 11, 11).unwrap();
        // Both sealed → 2 rows.
        let n = query(dir.path(), r#"SELECT count(*) AS n FROM "usdc__transfer""#).unwrap();
        assert_eq!(n[0]["n"], Value::from(2u64));

        // Delete one segment file (as quarantine would). The query still works, returning the survivor.
        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let gone = &manifest.tables["usdc__transfer"][0].file;
        std::fs::remove_file(dir.path().join(crate::seal::SEGMENTS_DIR).join(gone)).unwrap();
        let n = query(dir.path(), r#"SELECT count(*) AS n FROM "usdc__transfer""#).unwrap();
        assert_eq!(
            n[0]["n"],
            Value::from(1u64),
            "surviving segment still queryable"
        );
    }

    /// #1162: on a live nest a seal folds a provisional segment - new file written, manifest
    /// installed, old file removed - while a query that planned against the old manifest is still
    /// holding the old name. The hook removes the planned file between planning and execution, which
    /// is exactly that window. The query must answer from what the manifest now says, not fail.
    #[test]
    fn query_replans_once_when_a_planned_segment_vanishes_before_execution() {
        let dir = tempfile::tempdir().unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        let row = |b: u64| {
            format!(
                r#"{{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"1","block_number":{b},"tx_hash":"0xt","log_index":0}}"#
            )
        };
        crate::seal::seal_range(dir.path(), &[row(10)], 10, 10).unwrap();
        crate::seal::seal_range(dir.path(), &[row(11)], 11, 11).unwrap();
        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let seg = &manifest.tables["usdc__transfer"][0];
        let gone = crate::seal::segment_path(dir.path(), &seg.file, &seg.hash);
        assert!(gone.exists());
        test_set_remove_after_define(dir.path(), Some(gone.clone()));

        let n = query(dir.path(), r#"SELECT count(*) AS n FROM "usdc__transfer""#)
            .expect("the query replans against the current manifest instead of failing");
        assert!(
            !gone.exists(),
            "the hook fired: the planned file was removed mid-query"
        );
        // The second plan saw the file missing and reduced the table to the survivor, which is the
        // existing missing-file behaviour; on a live nest it would instead see the folded segment.
        assert_eq!(n[0]["n"], Value::from(1u64));
        test_set_remove_after_define(dir.path(), None);
    }

    fn fold_row(b: u64) -> String {
        format!(
            r#"{{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"1","block_number":{b},"tx_hash":"0xt","log_index":0}}"#
        )
    }

    fn only_provisional(dir: &Path) -> PathBuf {
        let manifest = crate::seal::load_manifest(dir).unwrap();
        let segs = &manifest.tables["usdc__transfer"];
        assert_eq!(segs.len(), 1, "one provisional segment");
        assert!(
            segs[0].provisional,
            "the fixture must be under the table floor, so a seal folds it"
        );
        crate::seal::segment_path(dir, &segs[0].file, &segs[0].hash)
    }

    fn count(dir: &Path) -> QueryOutput {
        let guard = QueryGuard {
            timeout: Duration::from_secs(30),
            max_rows: 10,
        };
        query_guarded(dir, r#"SELECT count(*) AS n FROM "usdc__transfer""#, guard).unwrap()
    }

    /// N4a's live run: a `/sql` read planned against the manifest, a seal folded the provisional
    /// segment and deleted the file the plan named, and the read found it missing, logged "cold data
    /// reduced" and answered short. The fold lands here right after the plan read the manifest. The
    /// plan's lease must keep the replaced file on disk until its rows are read, then release it.
    #[test]
    fn a_fold_after_the_plan_read_the_manifest_is_answered_from_the_plans_segments() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<String> = (10..13).map(fold_row).collect();
        crate::seal::seal_range(dir.path(), &rows, 10, 12).unwrap();
        let replaced = only_provisional(dir.path());

        let fold_dir = dir.path().to_path_buf();
        test_set_after_manifest_read(
            dir.path(),
            Box::new(move || {
                let more: Vec<String> = (13..15).map(fold_row).collect();
                crate::seal::seal_range(&fold_dir, &more, 13, 14).unwrap();
            }),
        );
        let out = count(dir.path());
        assert!(
            out.degraded_tables.is_empty(),
            "a fold under a plan must not reduce the table: {:?}",
            out.degraded_tables
        );
        assert_eq!(
            out.rows[0]["n"],
            Value::from(3u64),
            "the plan read the manifest before the fold, so it answers from that segment set, whole"
        );
        assert!(
            !replaced.exists(),
            "the replaced file must be deleted once no plan can still read it"
        );
        assert_eq!(count(dir.path()).rows[0]["n"], Value::from(5u64));
    }

    /// The second line of defence, for a file removed by something a lease cannot hold back: missing,
    /// with the catalogue changed since the plan read it, is a plan overtaken rather than a
    /// quarantined segment, and the query plans again instead of answering from what is left.
    #[test]
    fn a_planned_segment_gone_with_the_catalogue_moved_is_replanned_not_reduced() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<String> = (10..13).map(fold_row).collect();
        crate::seal::seal_range(dir.path(), &rows, 10, 12).unwrap();
        let replaced = only_provisional(dir.path());

        let fold_dir = dir.path().to_path_buf();
        test_set_after_manifest_read(
            dir.path(),
            Box::new(move || {
                let more: Vec<String> = (13..15).map(fold_row).collect();
                crate::seal::seal_range(&fold_dir, &more, 13, 14).unwrap();
                std::fs::remove_file(&replaced).unwrap();
            }),
        );
        let out = count(dir.path());
        assert!(
            out.degraded_tables.is_empty(),
            "an overtaken plan must be redone, not reduced: {:?}",
            out.degraded_tables
        );
        assert_eq!(out.rows[0]["n"], Value::from(5u64));
    }

    fn plus_chain(terms: usize) -> String {
        let mut sql = String::from("SELECT ");
        for i in 0..terms {
            if i > 0 {
                sql.push('+');
            }
            sql.push('1');
        }
        sql.push_str(" AS n");
        sql
    }

    /// A chain long enough to abort the planner is refused before a session exists. Sixty-four
    /// terms is the chain the planner is allowed to run.
    #[test]
    fn a_deep_statement_is_refused_before_a_session_opens() {
        let dir = tempfile::tempdir().unwrap();
        let opened = session_opens_for(dir.path());
        for terms in [65usize, 1000] {
            let err = query(dir.path(), &plus_chain(terms)).unwrap_err();
            let msg = format!("{err:#}");
            assert!(msg.contains("deeper than 64"), "{terms} terms: {msg}");
            assert_eq!(
                session_opens_for(dir.path()),
                opened,
                "{terms} terms opened a session"
            );
        }
        let rows = query(dir.path(), &plus_chain(64)).unwrap();
        assert_eq!(rows[0]["n"], Value::from(64u64));
        assert!(session_opens_for(dir.path()) > opened);
    }

    /// The shutdown latch is process-wide, so this runs in a child. A statement that starts after
    /// the latch must fail, and quickly: the drain used to wait out the whole query.
    #[test]
    fn a_statement_starting_after_shutdown_does_not_run() {
        if std::env::var("NUTHATCH_SHUTDOWN_LATCH_TEST").is_err() {
            let out = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "a_statement_starting_after_shutdown_does_not_run",
                    "--test-threads=1",
                ])
                .env("NUTHATCH_SHUTDOWN_LATCH_TEST", "1")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "child failed\n{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            );
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<String> = (0..500u64).map(fold_row).collect();
        crate::seal::seal_range(dir.path(), &rows, 0, 499).unwrap();
        interrupt_for_shutdown();
        let path = dir.path().to_path_buf();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let started = Instant::now();
            let r = query(
                &path,
                r#"SELECT count(*) AS n FROM "usdc__transfer" a, "usdc__transfer" b, "usdc__transfer" c"#,
            );
            let msg = r.err().map(|e| format!("{e:#}")).unwrap_or_default();
            let _ = tx.send((msg, started.elapsed()));
        });
        let (msg, took) = rx.recv_timeout(Duration::from_secs(2)).unwrap_or_else(|_| {
            panic!("the drain waited on a statement the shutdown latch should have stopped")
        });
        assert!(
            msg.contains("cancelled"),
            "a statement that started after shutdown was not cancelled: {msg}"
        );
        assert!(took < Duration::from_secs(2), "shutdown waited {took:?}");
    }

    /// SIGTERM waited 5.63 s behind a `/sql` still executing: the server drains in-flight requests, and
    /// nothing stopped the statement. A running statement must end when the live queries are interrupted.
    #[test]
    fn a_running_query_stops_when_live_queries_are_interrupted() {
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<String> = (0..2_000u64).map(fold_row).collect();
        crate::seal::seal_range(dir.path(), &rows, 0, 1_999).unwrap();
        let path = dir.path().to_path_buf();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let r = query(
                &path,
                r#"SELECT count(*) AS n FROM "usdc__transfer" a, "usdc__transfer" b, "usdc__transfer" c"#,
            );
            let _ = tx.send(r.is_err());
        });
        let started = Instant::now();
        while !live_queries()
            .lock()
            .unwrap()
            .iter()
            .any(|(_, d, _)| d == dir.path())
        {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the query never started"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        std::thread::sleep(Duration::from_millis(200));
        interrupt_live_in(dir.path());
        let failed = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("an interrupted statement must stop, not run eight billion rows to the end");
        assert!(failed, "an interrupted statement must fail, not answer");
        // The token stays armed. The next statement clears it, or this session would refuse
        // everything that followed.
        let again = query(dir.path(), "SELECT 1 AS n").unwrap();
        assert_eq!(again[0]["n"], Value::from(1u64));
    }

    /// The same race without a hook: readers query while a writer folds, one row at a time. No answer
    /// may be short of the rows committed before its query began, none may be flagged reduced, and no
    /// retired file may outlive the last reader.
    #[test]
    fn readers_racing_folds_never_answer_short_and_leave_no_file_behind() {
        use std::sync::atomic::{AtomicBool, AtomicU64};
        let dir = tempfile::tempdir().unwrap();
        crate::seal::seal_range(dir.path(), &[fold_row(0)], 0, 0).unwrap();
        let committed = Arc::new(AtomicU64::new(1));
        let answered = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        const FOLDS: u64 = 120;

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (dir, committed, answered, done) = (
                    dir.path().to_path_buf(),
                    committed.clone(),
                    answered.clone(),
                    done.clone(),
                );
                std::thread::spawn(move || {
                    while !done.load(Ordering::SeqCst) {
                        let before = committed.load(Ordering::SeqCst);
                        let out = count(&dir);
                        assert!(
                            out.degraded_tables.is_empty(),
                            "a read racing a fold was reduced: {:?}",
                            out.degraded_tables
                        );
                        let n = out.rows[0]["n"].as_u64().unwrap();
                        assert!(
                            n >= before,
                            "a read answered {n} rows with {before} committed before it began"
                        );
                        answered.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for b in 1..=FOLDS {
            crate::seal::seal_range(dir.path(), &[fold_row(b)], b, b).unwrap();
            committed.store(b + 1, Ordering::SeqCst);
            // Each fold waits for an answer that finished after it, so every later fold begins with
            // the readers mid-query: on a loaded machine 120 seals can otherwise outrun them.
            let seen = answered.load(Ordering::SeqCst);
            let waiting = Instant::now();
            while answered.load(Ordering::SeqCst) == seen {
                assert!(
                    waiting.elapsed() < Duration::from_secs(60)
                        && !readers.iter().all(|r| r.is_finished()),
                    "no reader answered during fold {b}"
                );
                std::thread::yield_now();
            }
        }
        done.store(true, Ordering::SeqCst);
        for reader in readers {
            reader.join().unwrap();
        }

        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let named: std::collections::BTreeSet<String> = manifest
            .tables
            .values()
            .flatten()
            .map(|s| s.file.clone())
            .collect();
        let on_disk: std::collections::BTreeSet<String> =
            std::fs::read_dir(dir.path().join(crate::seal::SEGMENTS_DIR))
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|n| n.ends_with(".parquet"))
                .collect();
        assert_eq!(
            on_disk, named,
            "retired files must be deleted once their readers are done"
        );
        assert_eq!(count(dir.path()).rows[0]["n"], Value::from(FOLDS + 1));
    }

    #[test]
    fn segment_vanished_recognises_burrmills_wording_and_nothing_else() {
        // As `query_replans_once_when_a_planned_segment_vanishes_before_execution` sees it.
        let exec = anyhow::anyhow!(
            "substrate error: Parquet error: Parquet error: Failed to fetch metadata for file \
             data/x/segments/staking__tokens_undelegated-efb41c8f.parquet: Object Store error: \
             Object at location /data/x/segments/staking__tokens_undelegated-efb41c8f.parquet not \
             found: No such file or directory (os error 2)"
        )
        .context("query failed");
        assert!(segment_vanished(&exec));
        // A missing file that is not a segment (a view, a label snapshot) is not this fault.
        let other = anyhow::anyhow!(
            "Object at location /data/x/views/90-x.sql not found: No such file or directory"
        )
        .context("query failed");
        assert!(!segment_vanished(&other));
        // A segment named in an unrelated error is not this fault either.
        let unrelated = anyhow::anyhow!(
            "Parquet error: Unexpected struct field type 15: /data/x/segments/a-b.parquet"
        );
        assert!(!segment_vanished(&unrelated));
    }

    #[test]
    fn sql_disjoint_union_never_double_counts_an_overlapping_row() {
        // COR-1: even if a block sits in BOTH a sealed segment and the hot store (the seal→prune crash
        // window), the `sealed_through` filter counts it once - cold ≤ watermark, hot > watermark.
        let dir = tempfile::tempdir().unwrap();
        let cold = vec![r#"{"table":"t__e","block_number":10,"log_index":0,"x":"1"}"#.to_string()];
        crate::seal::seal_range(dir.path(), &cold, 10, 10).unwrap();
        // Hot deliberately still holds block 10 (the overlap) AND a genuinely-unsealed block 20.
        let mut hot = HotRows::new();
        hot.insert(
            "t__e".into(),
            vec![
                serde_json::json!({"table":"t__e","block_number":10,"log_index":0,"x":"1"}),
                serde_json::json!({"table":"t__e","block_number":20,"log_index":0,"x":"2"}),
            ],
        );
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        // Watermark = 10: cold keeps block 10, hot keeps only block 20 → 2 rows, not 3.
        let out = query_hot_cold(
            dir.path(),
            r#"SELECT count(*) AS n FROM "t__e""#,
            guard,
            &hot,
            10,
            &[],
        )
        .unwrap();
        assert_eq!(out.rows[0]["n"], Value::from(2u64));
    }

    #[test]
    fn sql_serves_the_genesis_row_when_nothing_has_been_sealed() {
        // Regression for OBIB case 3. `sealed_through` reads 0 both when the watermark sits at block
        // 0 and when nothing has ever been sealed, so filtering hot to `block_number > sealed_through`
        // silently dropped block 0 - it belonged to neither half of the union. A backfill of blocks
        // 0-100,000 returned 100,000 rows starting at block 1 against an expected 100,001.
        let dir = tempfile::tempdir().unwrap();
        let mut hot = HotRows::new();
        hot.insert(
            "t__b".into(),
            vec![
                serde_json::json!({"table":"t__b","block_number":0,"log_index":0,"x":"genesis"}),
                serde_json::json!({"table":"t__b","block_number":1,"log_index":0,"x":"one"}),
                serde_json::json!({"table":"t__b","block_number":2,"log_index":0,"x":"two"}),
            ],
        );
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        let out = query_hot_cold(
            dir.path(),
            r#"SELECT count(*) AS n, min(block_number) AS lo FROM "t__b""#,
            guard,
            &hot,
            0,
            &[],
        )
        .unwrap();
        assert_eq!(
            out.rows[0]["n"],
            Value::from(3u64),
            "block 0 must be readable when nothing has been sealed"
        );
        assert_eq!(
            out.rows[0]["lo"],
            Value::from(0u64),
            "the served range must start at genesis, not block 1"
        );
    }

    #[test]
    fn sql_excludes_hot_genesis_once_cold_actually_holds_it() {
        // The other side of the fix: serving block 0 from hot must not reopen the double-count it
        // sits beside. Once genesis IS sealed, the hot copy has to stay excluded - and the watermark
        // is still 0, so only the presence of a real segment can tell the two states apart.
        let dir = tempfile::tempdir().unwrap();
        let cold =
            vec![r#"{"table":"t__b","block_number":0,"log_index":0,"x":"genesis"}"#.to_string()];
        crate::seal::seal_range(dir.path(), &cold, 0, 0).unwrap();
        let mut hot = HotRows::new();
        hot.insert(
            "t__b".into(),
            vec![
                serde_json::json!({"table":"t__b","block_number":0,"log_index":0,"x":"genesis"}),
                serde_json::json!({"table":"t__b","block_number":1,"log_index":0,"x":"one"}),
            ],
        );
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        let out = query_hot_cold(
            dir.path(),
            r#"SELECT count(*) AS n FROM "t__b""#,
            guard,
            &hot,
            0,
            &[],
        )
        .unwrap();
        assert_eq!(
            out.rows[0]["n"],
            Value::from(2u64),
            "a sealed genesis row must not also be served from hot"
        );
    }

    #[test]
    fn empty_view_types_columns_by_name_not_storage() {
        // COR-4: a `u64`-storage event field with a NON-counter name (e.g. a `uint24` fee) must be
        // VARCHAR in the empty view - matching what `seal::rows_to_batch` writes - so the column's SQL
        // type doesn't flip (valid empty, erroring once populated) the instant the first row seals.
        // The same for a declared `word32` column no input carries (#467). The counter columns stay
        // UBIGINT, by name.
        let conn = crate::engine::bare();
        let cols = [
            ("fee".to_string(), "u64".to_string()),
            ("amount".to_string(), "word32".to_string()),
            ("block_number".to_string(), "word32".to_string()),
        ];
        assert!(conn
            .bind_facts("pool__swap", &cols, &[], false, FactWindow::default())
            .unwrap());
        let described = conn
            .describe(r#"SELECT "fee", "amount", "block_number" FROM "pool__swap""#)
            .unwrap();
        let types: Vec<&str> = described.iter().map(|(_, t)| t.as_str()).collect();
        assert_eq!(types, ["VARCHAR", "VARCHAR", "UBIGINT"], "{described:?}");
    }

    #[test]
    fn sql_survives_schema_drift_across_segments() {
        // COR-2: two segments of one table with different column sets (an ABI gained a `fee` field
        // between them) must UNION via `union_by_name`, not throw and drop the whole view.
        let dir = tempfile::tempdir().unwrap();
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"t__e","block_number":10,"log_index":0,"a":"1"}"#.to_string()],
            10,
            10,
        )
        .unwrap();
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"t__e","block_number":20,"log_index":0,"a":"2","fee":"9"}"#.to_string()],
            20,
            20,
        )
        .unwrap();
        // Without union_by_name this errors ("table not found" - the view was silently dropped).
        let out = query(dir.path(), r#"SELECT count(*) AS n FROM "t__e""#).unwrap();
        assert_eq!(out[0]["n"], Value::from(2u64));
        // The drifted column is NULL-filled for the earlier segment.
        let fees = query(dir.path(), r#"SELECT count(fee) AS with_fee FROM "t__e""#).unwrap();
        assert_eq!(fees[0]["with_fee"], Value::from(1u64));
    }

    #[test]
    fn sql_cannot_read_files_outside_the_data_dirs() {
        // Hardening SEC-2: file-reading table functions (read_text/read_csv/glob/…, DuckDB's names)
        // are file-read primitives usable inside a SELECT, and must never reach outside the nest.
        let dir = tempfile::tempdir().unwrap();
        let cold = vec![r#"{"table":"t__e","block_number":10,"log_index":0,"x":"1"}"#.to_string()];
        crate::seal::seal_range(dir.path(), &cold, 10, 10).unwrap();
        // A secret in the nest root (where nuthatch.toml with webhook secrets + RPC keys actually lives).
        std::fs::write(dir.path().join("secret.txt"), "TOP SECRET").unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        // Absolute path outside the allowlist → refused.
        assert!(
            query_guarded(
                dir.path(),
                "SELECT content FROM read_text('/etc/hosts')",
                guard
            )
            .is_err(),
            "read_text('/etc/hosts') must be blocked"
        );
        // The nest ROOT (config lives here) is NOT in the allowlist (only segments/ + labels/ are).
        let q = format!(
            "SELECT content FROM read_text('{}')",
            dir.path().join("secret.txt").display()
        );
        assert!(
            query_guarded(dir.path(), &q, guard).is_err(),
            "read_text of the nest root must be blocked (leaks nuthatch.toml)"
        );
        // Case-insensitive + comment-split can't sneak past the denylist.
        assert!(query_guarded(dir.path(), "SELECT * FROM READ_TEXT('/etc/hosts')", guard).is_err());
        assert!(query_guarded(dir.path(), "SELECT * FROM glob('/*')", guard).is_err());
        assert!(query_guarded(
            dir.path(),
            "SELECT content FROM read_text/**/('/etc/hosts')",
            guard
        )
        .is_err());
        // A legitimate query over the sealed segment still works - even when a *column* is named like a
        // function (no call → not blocked).
        let ok = query_guarded(
            dir.path(),
            r#"SELECT count(*) AS read_text FROM "t__e""#,
            guard,
        )
        .unwrap();
        assert_eq!(ok.rows[0]["read_text"], Value::from(1u64));

        // Replacement scans (SEC-2): a bare string literal in table position reads a file with no
        // function name for the denylist to match - the previously-open bypass. Both a `FROM '<path>'`
        // and a `JOIN '<path>'` must be refused, for an absolute path and the nest root alike.
        assert!(
            query_guarded(dir.path(), "SELECT * FROM '/etc/hosts'", guard).is_err(),
            "a `FROM '<path>'` replacement scan must be refused"
        );
        assert!(query_guarded(dir.path(), "SELECT * FROM '/tmp/x.parquet'", guard).is_err());
        assert!(query_guarded(
            dir.path(),
            r#"SELECT * FROM "t__e" JOIN '/etc/hosts' ON true"#,
            guard
        )
        .is_err());
        // Parquet metadata functions read a file too - now denylisted.
        assert!(query_guarded(
            dir.path(),
            "SELECT * FROM parquet_metadata('/etc/hosts')",
            guard
        )
        .is_err());
        // A double-quoted identifier in table position (the legitimate form) is NOT a replacement scan
        // and stays allowed - the guard keys on the single-quote, not the FROM keyword.
        assert!(query_guarded(dir.path(), r#"SELECT count(*) FROM "t__e""#, guard).is_ok());
    }

    #[test]
    fn hot_tip_is_queryable_without_any_segments() {
        // RFC-0013: a nest with only unsealed tip data (no segments, no schema.json) is still SQL-
        // queryable - the hot rows are loaded into a temp table with data-derived columns.
        let dir = tempfile::tempdir().unwrap();
        let mut hot = HotRows::new();
        hot.insert(
            "usdc__transfer".into(),
            vec![
                serde_json::json!({"table":"usdc__transfer","from":"0xa","to":"0xb","value":"5","block_number":100,"tx_hash":"0xt","log_index":0}),
                serde_json::json!({"table":"usdc__transfer","from":"0xa","to":"0xc","value":"7","block_number":101,"tx_hash":"0xt","log_index":0}),
            ],
        );
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        let out = query_hot_cold(
            dir.path(),
            r#"SELECT count(*) AS n, SUM(CAST(value AS DECIMAL(38,0))) AS total FROM "usdc__transfer""#,
            guard,
            &hot,
            0, // nothing sealed → all hot rows (blocks 100/101 > 0) count,
            &[],
        )
        .unwrap();
        assert_eq!(out.rows[0]["n"], Value::from(2u64));
        // Big-int text summed via DECIMAL; a DECIMAL is served as a string.
        assert_eq!(out.rows[0]["total"].as_str(), Some("12"));
    }

    #[test]
    fn sql_unions_the_hot_tip_with_sealed_cold() {
        // The federation: sealed history + unsealed tip, one SQL surface (RFC-0013). Hot and cold are
        // disjoint by block, so a plain UNION ALL is exact.
        let dir = tempfile::tempdir().unwrap();
        let cold = vec![
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"5","block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xc","value":"7","block_number":10,"tx_hash":"0xt","log_index":1}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &cold, 10, 10).unwrap();
        let mut hot = HotRows::new();
        hot.insert(
            "usdc__transfer".into(),
            vec![
                serde_json::json!({"table":"usdc__transfer","from":"0xd","to":"0xe","value":"9","block_number":20,"tx_hash":"0xu","log_index":0}),
            ],
        );
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        // Cold-only sees the 2 sealed rows; hot+cold sees all 3.
        let cold_only = query_guarded(
            dir.path(),
            r#"SELECT count(*) AS n FROM "usdc__transfer""#,
            guard,
        )
        .unwrap();
        assert_eq!(cold_only.rows[0]["n"], Value::from(2u64));
        let both = query_hot_cold(
            dir.path(),
            r#"SELECT count(*) AS n FROM "usdc__transfer""#,
            guard,
            &hot,
            10, // sealed through block 10 → cold ≤ 10, hot > 10,
            &[],
        )
        .unwrap();
        assert_eq!(both.rows[0]["n"], Value::from(3u64));
        // The hot row is visible with its columns, filterable by block.
        let tip = query_hot_cold(
            dir.path(),
            r#"SELECT "to" FROM "usdc__transfer" WHERE block_number = 20"#,
            guard,
            &hot,
            10,
            &[],
        )
        .unwrap();
        assert_eq!(tip.rows.len(), 1);
        assert_eq!(tip.rows[0]["to"], Value::from("0xe"));
    }

    #[test]
    fn net_balances_sum_per_address_as_i128() {
        let dir = tempfile::tempdir().unwrap();
        // 1e20 base units > i64::MAX (~9.2e18): the value that an i64 accumulator would have dropped.
        let big = "100000000000000000000";
        let entities = vec![
            format!(
                r#"{{"table":"t__transfer","from":"0x0","to":"0xa","value":"{big}","block_number":1,"tx_hash":"0xt","log_index":0}}"#
            ),
            r#"{"table":"t__transfer","from":"0xa","to":"0xb","value":"30","block_number":1,"tx_hash":"0xt","log_index":1}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 1, 1).unwrap();

        let map: std::collections::HashMap<String, i128> =
            net_balances(dir.path(), "t__transfer", "from", "to", "value", u64::MAX)
                .unwrap()
                .into_iter()
                .collect();
        let big: i128 = big.parse().unwrap();
        assert_eq!(map["0x0"], -big); // minted out
        assert_eq!(map["0xa"], big - 30); // received big, sent 30
        assert_eq!(map["0xb"], 30);
        assert!(!map.contains_key("nobody"));
    }

    #[test]
    fn cold_velocity_seeds_sealed_windows() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"t__transfer","from":"0xa","to":"0xb","value":"5","block_number":15,"tx_hash":"0xt","log_index":0}"#.to_string(),
            r#"{"table":"t__transfer","from":"0xa","to":"0xc","value":"7","block_number":19,"tx_hash":"0xu","log_index":0}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 15, 19).unwrap();

        let rows = cold_velocity(dir.path(), "t__transfer", "from", "value", 10, 19).unwrap();
        assert_eq!(rows, vec![("0xa\u{1f}10".to_string(), 12, 2)]);
    }

    /// RFC-0008 C1: labels imported as a content-addressed snapshot are visible to `/sql` as a
    /// `labels` view, and `cold_exposure` folds sealed transfers × labels into pre-summed exposure
    /// (the restart re-seed path). Uses an amount > i64::MAX to prove the i128 discipline carries.
    #[test]
    fn labels_view_and_cold_exposure_fold() {
        let dir = tempfile::tempdir().unwrap();
        // Label 0xmixer. Two transfers: 0xa → mixer (big), mixer → 0xb (30). 0xa→0xc is unlabeled.
        let mixer = "0x1111111111111111111111111111111111111111";
        let a = "0x00000000000000000000000000000000000000aa";
        let b = "0x00000000000000000000000000000000000000bb";
        let c = "0x00000000000000000000000000000000000000cc";
        let label_file = dir.path().join("l.csv");
        std::fs::write(&label_file, format!("{mixer},mixer\n")).unwrap();
        crate::labels::import(dir.path(), &label_file).unwrap();

        let big = "100000000000000000000"; // > i64::MAX
        let entities = vec![
            format!(
                r#"{{"table":"t__transfer","from":"{a}","to":"{mixer}","value":"{big}","block_number":1,"tx_hash":"0xt","log_index":0}}"#
            ),
            format!(
                r#"{{"table":"t__transfer","from":"{mixer}","to":"{b}","value":"30","block_number":1,"tx_hash":"0xt","log_index":1}}"#
            ),
            format!(
                r#"{{"table":"t__transfer","from":"{a}","to":"{c}","value":"5","block_number":1,"tx_hash":"0xt","log_index":2}}"#
            ),
        ];
        crate::seal::seal_range(dir.path(), &entities, 1, 1).unwrap();

        // The labels view is queryable via the normal SQL surface.
        let l = query(dir.path(), "SELECT count(*) AS n FROM labels").unwrap();
        assert_eq!(l[0]["n"], Value::from(1u64));

        let exp: std::collections::HashMap<String, (i128, i128)> =
            cold_exposure(dir.path(), "t__transfer", "from", "to", "value", u64::MAX)
                .unwrap()
                .into_iter()
                .map(|(k, amt, cnt)| (k, (amt, cnt)))
                .collect();
        let big: i128 = big.parse().unwrap();
        // 0xa sent `big` to the labeled mixer → outbound exposure (count 1, amount big).
        assert_eq!(exp[&format!("{a}\u{1f}mixer\u{1f}out")], (big, 1));
        // 0xb received 30 from the labeled mixer → inbound exposure.
        assert_eq!(exp[&format!("{b}\u{1f}mixer\u{1f}in")], (30, 1));
        // 0xc's transfer never touched a labeled address → no exposure entry.
        assert!(!exp.contains_key(&format!("{c}\u{1f}mixer\u{1f}in")));
    }

    /// RFC-0001 §2: a uint256 column gets a derived `_dec` DECIMAL(38) view column (value when it
    /// fits in 38 digits, else NULL) and an `_overflow` flag - so ad-hoc SQL can aggregate big ints
    /// without hand-casting.
    #[test]
    fn bigint_columns_get_decimal_and_overflow_views() {
        let dir = tempfile::tempdir().unwrap();
        // schema.json marks `value` as a word32 (uint256) column, driving the derived columns.
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"registry_hash":"0x0","tables":[{"table":"t__transfer","alias":"t","event":"Transfer","topic0":"0x","columns":[{"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]}]}"#,
        )
        .unwrap();
        // One value that fits DECIMAL(38) (37 digits) and one that overflows it (a 39-digit u128).
        let fits = "1000000000000000000000000000000000000"; // 1e36, 37 digits
        let overflows = "340282366920938463463374607431768211455"; // u128::MAX, 39 digits > DECIMAL(38)
        let entities = vec![
            format!(
                r#"{{"table":"t__transfer","from":"0xa","to":"0xb","value":"{fits}","block_number":1,"tx_hash":"0xt","log_index":0}}"#
            ),
            format!(
                r#"{{"table":"t__transfer","from":"0xa","to":"0xb","value":"{overflows}","block_number":1,"tx_hash":"0xt","log_index":1}}"#
            ),
        ];
        crate::seal::seal_range(dir.path(), &entities, 1, 1).unwrap();

        let rows = query(
            dir.path(),
            r#"SELECT value_dec, value_overflow FROM "t__transfer" ORDER BY log_index"#,
        )
        .unwrap();
        // Row 0 fits: value_dec present (HUGEINT/DECIMAL stringified), not overflow.
        assert_eq!(rows[0]["value_dec"], Value::from(fits));
        assert_eq!(rows[0]["value_overflow"], Value::from(false));
        // Row 1 overflows DECIMAL(38): value_dec NULL, overflow flagged.
        assert_eq!(rows[1]["value_dec"], Value::Null);
        assert_eq!(rows[1]["value_overflow"], Value::from(true));

        // A bare SUM(value_dec) is the column: the 39-digit row is NULL and is not a term.
        // WHERE NOT value_overflow is that same sum. Other aggregates still refuse (2026-09-29).
        let s = query(
            dir.path(),
            r#"SELECT (SUM(value_dec) FILTER (WHERE NOT value_overflow))::VARCHAR AS s FROM "t__transfer""#,
        )
        .unwrap();
        assert_eq!(s[0]["s"], Value::from(fits));

        // The bare sum is the column's own values. The 39-digit row is NULL there.
        let bare = query(
            dir.path(),
            r#"SELECT SUM(value_dec)::VARCHAR AS s FROM "t__transfer""#,
        )
        .unwrap();
        assert_eq!(bare[0]["s"], Value::from(fits));
    }

    /// #434: a declared big-int column that **no** sealed segment carries must not delete the table.
    /// `union_by_name` NULL-fills a column some segments lack, but `derived_bigint_cols` casts one level
    /// above it, so 0-of-N left the cast bound to nothing and the whole view DDL failed - the table
    /// vanished from `/sql` with `Table with name ... does not exist`, no corrupt file involved. That is
    /// the state of every nest between a `schema.json` big-int column landing and the first segment
    /// carrying it sealing. 1-of-N always worked, which is what made it look covered.
    #[test]
    fn declared_bigint_column_no_segment_carries_keeps_the_table() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"registry_hash":"0x0","tables":[{"table":"t__transfer","alias":"t","event":"Transfer","topic0":"0x","columns":[{"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]}]}"#,
        )
        .unwrap();
        // Two sealed segments, neither carrying `value` - the ABI bump that added it has not sealed yet.
        for b in [1u64, 2] {
            crate::seal::seal_range(
                dir.path(),
                &[format!(
                    r#"{{"table":"t__transfer","from":"0xa","to":"0xb","block_number":{b},"tx_hash":"0xt","log_index":0}}"#
                )],
                b,
                b,
            )
            .unwrap();
        }

        // The table is still there and still answers over the segments that do exist.
        let rows = query(dir.path(), r#"SELECT count(*) AS n FROM "t__transfer""#).unwrap();
        assert_eq!(
            rows[0]["n"],
            Value::from(2u64),
            "a declared big-int column carried by no segment must reduce to NULLs, not delete the table"
        );
        // The declared column and its derived siblings keep the shape they have at 1-of-N: present,
        // NULL, not flagged as an overflow. A caller's `value_dec` query must not become a naming error.
        let d = query(
            dir.path(),
            r#"SELECT value, value_dec, value_overflow FROM "t__transfer" ORDER BY block_number"#,
        )
        .unwrap();
        assert_eq!(d[0]["value"], Value::Null);
        assert_eq!(d[0]["value_dec"], Value::Null);
        assert_eq!(d[0]["value_overflow"], Value::from(false));
        // And the non-declared columns the segments do carry are untouched.
        assert_eq!(d.len(), 2);
        let f = query(dir.path(), r#"SELECT "from" FROM "t__transfer" LIMIT 1"#).unwrap();
        assert_eq!(f[0]["from"], Value::from("0xa"));
    }

    /// The hot half of #434, which the issue does not cover. The hot temp table derives its columns
    /// from the rows themselves, so a tip batch that carries no `value` key leaves the same derived
    /// cast bound to nothing - and the view dies with every sealed segment healthy. Same wrap, and it
    /// wants its own test because the sealed test above passes with the hot side still broken.
    #[test]
    fn declared_bigint_column_no_hot_row_carries_keeps_the_table() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"registry_hash":"0x0","tables":[{"table":"t__transfer","alias":"t","event":"Transfer","topic0":"0x","columns":[{"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]}]}"#,
        )
        .unwrap();
        let mut hot = HotRows::new();
        hot.insert(
            "t__transfer".into(),
            vec![serde_json::json!({"table":"t__transfer","from":"0xa","to":"0xb","block_number":100,"tx_hash":"0xt","log_index":0})],
        );
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        let out = query_hot_cold(
            dir.path(),
            r#"SELECT "from", value, value_dec, value_overflow FROM "t__transfer""#,
            guard,
            &hot,
            0,
            &[],
        )
        .unwrap();
        assert_eq!(
            out.rows.len(),
            1,
            "a tip batch missing a declared big-int column must not delete the table"
        );
        assert_eq!(out.rows[0]["from"], Value::from("0xa"));
        assert_eq!(out.rows[0]["value"], Value::Null);
        assert_eq!(out.rows[0]["value_dec"], Value::Null);
        assert_eq!(out.rows[0]["value_overflow"], Value::from(false));
    }

    /// #729: `define_views` merged a declared table into `schema.json`'s copy only when no entry of
    /// that *name* existed yet (#663's fix) - a table already on disk kept its on-disk *columns*
    /// forever, even once the live registry (a re-fetched ABI, same event, one more field) knows more.
    /// The view built "successfully" and was silently missing the column: no error, no log, unlike
    /// #663's total-failure case. Confirmed directly against DuckDB at the time (see
    /// `with_declared_base_cols`'s doc) that a column no listed segment carries is a binder error on
    /// explicit reference, not a NULL row - so the fix is a name-keyed column merge plus generalizing #434's null-stub from
    /// big-integer columns to every declared column, not a replacement of the disk column set. CLAUDE.md
    /// rules out ever re-decoding the sealed segment itself.
    #[test]
    fn define_views_merges_a_stale_schema_json_columns_by_name() {
        let dir = tempfile::tempdir().unwrap();
        // schema.json as it stood at the last `dev`/`schema` run: `t__transfer` known, but only `value`.
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"registry_hash":"0x0","tables":[{"table":"t__transfer","alias":"t","event":"Transfer","topic0":"0x","columns":[{"name":"value","sol_type":"uint256","storage":"varchar","indexed":false}]}]}"#,
        )
        .unwrap();
        // One sealed segment, written under the old ABI - it genuinely has no `memo` column.
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"t__transfer","from":"0xa","to":"0xb","value":"9","block_number":1,"tx_hash":"0xt","log_index":0}"#.to_string()],
            1,
            1,
        )
        .unwrap();

        // The registry has been re-fetched since: the ABI gained `memo` on the same `Transfer` event.
        // `schema.json` was never regenerated, so it still only knows `value` - a stale *column set* on
        // an already-declared table, not a missing table (#663's case). A degenerate fixture (A == A)
        // would pass with the merge never running, so the two sets deliberately differ.
        let declared = vec![crate::registry::TableSchema {
            table: "t__transfer".into(),
            alias: "t".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "Transfer".into(),
            topic0: "0x".into(),
            columns: vec![
                crate::registry::ColumnSchema {
                    name: "value".into(),
                    sol_type: "uint256".into(),
                    storage: "varchar".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                crate::registry::ColumnSchema {
                    name: "memo".into(),
                    sol_type: "string".into(),
                    storage: "varchar".into(),
                    indexed: false,
                    components: Vec::new(),
                },
            ],
        }];

        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        let out = query_hot_cold(
            dir.path(),
            r#"SELECT value, memo FROM "t__transfer" ORDER BY block_number"#,
            guard,
            &HotRows::new(),
            u64::MAX,
            &declared,
        )
        .expect("the live registry's new column must resolve to NULL, not a binder error");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0]["value"], Value::from("9"));
        assert_eq!(
            out.rows[0]["memo"],
            Value::Null,
            "no sealed segment carries `memo` yet - it must read back NULL, matching #434's precedent \
             for a declared column no input carries, not disappear from the view or error"
        );
    }

    #[test]
    fn query_guard_sees_past_leading_comments() {
        assert_eq!(
            strip_leading_sql_comments("  \n-- hi\nSELECT 1").trim_start(),
            "SELECT 1"
        );
        assert_eq!(
            strip_leading_sql_comments("/* a */ WITH x AS (SELECT 1) SELECT 1")
                .trim_start()
                .split(' ')
                .next(),
            Some("WITH")
        );
        let dir = tempfile::tempdir().unwrap();
        // A comment-prefixed SELECT must be accepted (not rejected as non-SELECT); a DROP still fails.
        assert!(query(dir.path(), "-- a note\nSELECT 42 AS n").is_ok());
        assert!(query(dir.path(), "/* x */ DROP TABLE t").is_err());
    }

    /// #295: two queries on the same nest, same watermark, share one session. Deleting the cache
    /// (or opening every time) fails this. Other tests use other dirs and do not evict this slot.
    #[test]
    fn a_second_query_reuses_the_session() {
        let dir = tempfile::tempdir().unwrap();
        query(dir.path(), "SELECT 42 AS n").unwrap();
        assert_eq!(
            session_opens_for(dir.path()),
            1,
            "the first query opens a session"
        );
        query(dir.path(), "SELECT 42 AS n").unwrap();
        assert_eq!(
            session_opens_for(dir.path()),
            1,
            "the second query must not rebuild the world"
        );
    }

    /// #295: new sealed segments change the watermark; reusing the old connection would serve
    /// a view that never saw them.
    #[test]
    fn a_new_watermark_opens_a_fresh_connection() {
        let dir = tempfile::tempdir().unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        let hot = HotRows::new();
        query_hot_cold(dir.path(), "SELECT 1 AS n", guard, &hot, 0, &[]).unwrap();
        assert_eq!(session_opens_for(dir.path()), 1);
        query_hot_cold(dir.path(), "SELECT 1 AS n", guard, &hot, 10, &[]).unwrap();
        assert_eq!(
            session_opens_for(dir.path()),
            2,
            "a new sealed_through must not reuse the stale connection"
        );
    }

    /// #825: a cached connection is valid only for the authored inputs it was built from. Both an
    /// added view and its deletion must force a fresh catalogue; otherwise the old view stays
    /// queryable until process restart.
    /// #840 - a view rewritten to the same length inside one mtime tick must still invalidate.
    ///
    /// The cache keyed on `(len, modified_ns)`. On the Linux dev box that stamp misses a same-length
    /// rewrite **497 times in 500** (btrfs) because the mtime clock resolves to ~3.3 ms - 2,000
    /// writes produced nine distinguishable timestamps.
    ///
    /// **What that does and does not cost, measured rather than assumed.** The answer stays correct:
    /// `attempt()` re-runs `define_views`, `define_nest_views`, `define_labels_view` and
    /// `define_children_views` on every query, cached connection or fresh, and all of them are
    /// `CREATE OR REPLACE` - so the catalogue is rebuilt from the current files each time and the
    /// rows are right whatever the stamp says. Run this test against the old `(len, modified_ns)`
    /// implementation and the value assertion below still passes; it is the *invalidation* assertion
    /// that goes red. So the defect is a cache that fails to notice it is stale, not a query that
    /// lies - and the reason to fix it is that a stamp which cannot see a change is wrong
    /// independently of which code path happens to compensate for it today.
    ///
    /// **The collision here is forced rather than raced.** A test that just wrote the file twice
    /// quickly would pass on this laptop whatever the implementation does - APFS resolves to ~37 us
    /// and gives 0/500 collisions - and would only ever be red on Linux. Restoring the mtime
    /// explicitly makes the two stamps provably identical on every platform, so the test is red
    /// against the old implementation everywhere, which is the only way it is worth having.
    #[test]
    fn a_same_length_view_rewrite_in_one_mtime_tick_still_invalidates() {
        let dir = tempfile::tempdir().unwrap();
        let views = dir.path().join("views");
        std::fs::create_dir_all(&views).unwrap();
        let view = views.join("one.sql");

        // Two definitions of identical length that disagree about every row they produce.
        let before = "CREATE VIEW one AS SELECT 1 AS n";
        let after = "CREATE VIEW one AS SELECT 2 AS n";
        assert_eq!(
            before.len(),
            after.len(),
            "the rewrite must not change length"
        );

        std::fs::write(&view, before).unwrap();
        let rows = query(dir.path(), "SELECT n FROM one").unwrap();
        assert_eq!(rows[0]["n"], serde_json::json!(1));
        let opens = session_opens_for(dir.path());
        let stamped = std::fs::metadata(&view).unwrap();
        let (len, mtime) = (stamped.len(), stamped.modified().unwrap());

        std::fs::write(&view, after).unwrap();
        // Put the clock back, so the old `(len, modified_ns)` stamp is *provably* unchanged rather
        // than merely likely to be.
        std::fs::File::options()
            .write(true)
            .open(&view)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(mtime))
            .unwrap();
        let restamped = std::fs::metadata(&view).unwrap();
        assert_eq!(restamped.len(), len, "the rewrite changed length");
        assert_eq!(
            restamped.modified().unwrap(),
            mtime,
            "the mtime was not restored - this test would prove nothing"
        );

        let rows = query(dir.path(), "SELECT n FROM one").unwrap();
        // Correct either way, because the views are redefined per query - asserted so that a future
        // change to that arrangement is caught here rather than in production.
        assert_eq!(
            rows[0]["n"],
            serde_json::json!(2),
            "the rows must be current"
        );
        // This is the load-bearing one, and the one that is red against `(len, modified_ns)`.
        assert!(
            session_opens_for(dir.path()) > opens,
            "the connection was reused across a changed view - the stamp did not see the rewrite"
        );
    }

    #[test]
    fn changing_or_removing_an_authored_view_invalidates_the_session_cache() {
        let dir = tempfile::tempdir().unwrap();
        query(dir.path(), "SELECT 42 AS n").unwrap();
        assert_eq!(session_opens_for(dir.path()), 1);

        let views = dir.path().join("views");
        std::fs::create_dir_all(&views).unwrap();
        let view = views.join("one.sql");
        std::fs::write(&view, "CREATE VIEW one AS SELECT 1 AS n").unwrap();
        query(dir.path(), "SELECT 42 AS n").unwrap();
        assert_eq!(
            session_opens_for(dir.path()),
            2,
            "a new view changes the inputs"
        );

        std::fs::remove_file(view).unwrap();
        query(dir.path(), "SELECT 42 AS n").unwrap();
        assert_eq!(
            session_opens_for(dir.path()),
            3,
            "removing a view must not leave the old catalogue cached"
        );
    }

    #[test]
    fn removing_label_snapshots_drops_the_cached_labels_view() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("labels.csv");
        std::fs::write(&input, "0x1111111111111111111111111111111111111111,mixer\n").unwrap();
        crate::labels::import(dir.path(), &input).unwrap();
        query(dir.path(), "SELECT count(*) AS n FROM labels").unwrap();

        std::fs::remove_dir_all(dir.path().join(crate::labels::LABELS_DIR)).unwrap();
        assert!(
            query(dir.path(), "SELECT count(*) AS n FROM labels").is_err(),
            "a removed label snapshot must not remain readable through the cached connection"
        );
    }

    #[test]
    fn explicit_invalidation_releases_a_mounted_nests_connection() {
        let dir = tempfile::tempdir().unwrap();
        query(dir.path(), "SELECT 42 AS n").unwrap();
        invalidate_session_cache(dir.path());
        query(dir.path(), "SELECT 42 AS n").unwrap();
        assert_eq!(session_opens_for(dir.path()), 2);
    }

    /// A declared-but-unsealed table still resolves as an empty typed view, so a nest view that
    /// UNIONs it with a table that *does* have data doesn't cascade-fail (RFC-0002 dogfood fix).
    #[test]
    fn unsealed_tables_get_empty_typed_views() {
        let dir = tempfile::tempdir().unwrap();
        // schema declares two transfer-ish tables; only `a__ev` will have sealed data.
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"registry_hash":"0x0","tables":[
                {"table":"a__ev","alias":"a","event":"E","topic0":"0x","columns":[
                    {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                    {"name":"amount","sol_type":"uint256","storage":"word32","indexed":false}]},
                {"table":"b__ev","alias":"b","event":"E","topic0":"0x","columns":[
                    {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                    {"name":"amount","sol_type":"uint256","storage":"word32","indexed":false}]}
            ]}"#,
        )
        .unwrap();
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"a__ev","amount":"100","block_number":1,"log_index":0}"#.to_string()],
            1,
            1,
        )
        .unwrap();

        // b__ev has no segment, but a UNION of both (incl. the derived `_dec` column) must still work.
        let rows = query(
            dir.path(),
            r#"SELECT SUM(amount_dec)::VARCHAR AS total FROM (
                 SELECT amount_dec FROM "a__ev" UNION ALL SELECT amount_dec FROM "b__ev")"#,
        )
        .unwrap();
        assert_eq!(rows[0]["total"], Value::from("100"));
    }

    /// RFC-0002 §4: a nest's `views/*.sql` derived views are loaded and queryable via `/sql`, and
    /// can build on both the per-event tables and earlier (sorted) view files.
    #[test]
    fn nest_defined_views_are_loaded_and_queryable() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"5","block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"7","block_number":11,"tx_hash":"0xu","log_index":0}"#.to_string(),
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xc","value":"3","block_number":12,"tx_hash":"0xv","log_index":0}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 10, 12).unwrap();

        // Two view files: the second builds on the first - proves sorted load order.
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-recipients.sql"),
            r#"CREATE VIEW recipients AS SELECT "to" AS addr, count(*) AS n FROM "usdc__transfer" GROUP BY "to";"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/20-top_recipient.sql"),
            "CREATE VIEW top_recipient AS SELECT addr, n FROM recipients ORDER BY n DESC LIMIT 1;",
        )
        .unwrap();

        let rows = query(dir.path(), "SELECT addr, n FROM top_recipient").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["addr"], Value::from("0xb")); // 0xb received 2, 0xc received 1
        assert_eq!(rows[0]["n"], Value::from(2u64));

        // A broken view file doesn't blow up the surface - the good views still resolve.
        std::fs::write(
            dir.path().join("views/30-broken.sql"),
            "CREATE VIEW broken AS SELECT * FROM nonexistent_table;",
        )
        .unwrap();
        let again = query(dir.path(), "SELECT n FROM recipients WHERE addr = '0xb'").unwrap();
        assert_eq!(again[0]["n"], Value::from(2u64));
    }

    /// RFC-0018 §1: `validate_nest_views` flags a broken/drifted view (with a fuzzy-matched hint) and
    /// leaves a valid one alone - the loud gate the old silent-skip loader never had.
    #[test]
    fn validate_nest_views_flags_the_broken_one_with_a_hint() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"5","block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 10, 10).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-good.sql"),
            r#"CREATE VIEW good AS SELECT "to" AS addr FROM "usdc__transfer";"#,
        )
        .unwrap();
        // References `transfers` - the classic drop-the-prefix drift the registry no longer has.
        std::fs::write(
            dir.path().join("views/20-broken.sql"),
            "CREATE VIEW broken AS SELECT * FROM transfers;",
        )
        .unwrap();

        let schema = vec![crate::registry::TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "Transfer".into(),
            topic0: "0xddf2".into(),
            columns: vec![],
        }];
        let issues = validate_nest_views(dir.path(), &schema);
        assert_eq!(issues.len(), 1, "only the broken view is flagged");
        assert_eq!(issues[0].file, "20-broken.sql");
        let hint = issues[0].hint.as_ref().expect("a fix hint");
        assert!(
            hint.contains("usdc__transfer"),
            "fuzzy-suggests the real table: {hint}"
        );
    }

    /// #539's own repro: a Solidity `bool` column forces a `COALESCE` type mismatch, which fails the
    /// view's `CREATE VIEW`. Querying it must name the build failure and the real engine error, not
    /// report it as though the view were never defined - and the old fuzzy match onto an unrelated
    /// real table must be gone.
    #[test]
    fn a_view_broken_by_the_bool_footgun_is_named_as_a_build_failure() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"pool_manager__toggle_custom_fee","pool":"0xp","enabled":true,"block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 10, 10).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/20-custom-fees.sql"),
            "CREATE VIEW pool_effective_fee AS \
             SELECT pool, COALESCE(enabled, false) AS override_enabled \
             FROM pool_manager__toggle_custom_fee;",
        )
        .unwrap();
        // A real, unrelated table - present so the *old* fuzzy match had something to (wrongly) find.
        std::fs::write(
            dir.path().join("views/05-unrelated.sql"),
            "CREATE VIEW pool_manager__set_default_fee_alias AS \
             SELECT pool FROM pool_manager__toggle_custom_fee;",
        )
        .unwrap();

        let schema = vec![crate::registry::TableSchema {
            table: "pool_manager__toggle_custom_fee".into(),
            alias: "pool_manager".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "ToggleCustomFee".into(),
            topic0: "0x".into(),
            columns: vec![
                crate::registry::ColumnSchema {
                    name: "pool".into(),
                    sol_type: "address".into(),
                    storage: "address".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                crate::registry::ColumnSchema {
                    name: "enabled".into(),
                    sol_type: "bool".into(),
                    storage: "bool".into(),
                    indexed: false,
                    components: Vec::new(),
                },
            ],
        }];

        // The catalog error a query against the broken view produces.
        let raw = "Catalog Error: Table with name pool_effective_fee does not exist!\nDid you \
                    mean \"pool_manager__set_default_fee_alias\"?";
        let before = view_build_opens();
        let msg = enrich_query_error(dir.path(), raw, "SELECT * FROM pool_effective_fee", &schema)
            .unwrap();
        assert!(
            view_build_opens() > before,
            "a broken view is still replayed"
        );
        assert!(
            msg.contains("pool_effective_fee") && msg.contains("failed to build"),
            "names the view and says it failed to build: {msg}"
        );
        assert!(
            msg.contains("20-custom-fees.sql"),
            "names the file the broken view lives in: {msg}"
        );
        assert!(
            msg.contains("COALESCE") && msg.contains("explicit cast"),
            "carries the engine's real error: {msg}"
        );
        assert!(
            !msg.to_ascii_lowercase()
                .contains("pool_manager__set_default_fee_alias"),
            "must not still suggest the unrelated real table now the real cause is known: {msg}"
        );
    }

    /// The chained case: a view built *on top of* the broken one also fails to build (its own
    /// `CREATE VIEW` cannot resolve `pool_effective_fee` either), and the engine's error for it names
    /// `pool_effective_fee`, not the queried view. The message must still land on the root cause
    /// rather than repeating "pool_effective_fee does not exist" one hop removed.
    #[test]
    fn a_view_built_on_a_broken_view_reports_the_root_cause() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"pool_manager__toggle_custom_fee","pool":"0xp","enabled":true,"block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 10, 10).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/20-custom-fees.sql"),
            "CREATE VIEW pool_effective_fee AS \
             SELECT pool, COALESCE(enabled, false) AS override_enabled \
             FROM pool_manager__toggle_custom_fee;",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/30-summary.sql"),
            "CREATE VIEW pool_effective_fee_summary AS SELECT pool FROM pool_effective_fee;",
        )
        .unwrap();

        let schema = vec![crate::registry::TableSchema {
            table: "pool_manager__toggle_custom_fee".into(),
            alias: "pool_manager".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "ToggleCustomFee".into(),
            topic0: "0x".into(),
            columns: vec![
                crate::registry::ColumnSchema {
                    name: "pool".into(),
                    sol_type: "address".into(),
                    storage: "address".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                crate::registry::ColumnSchema {
                    name: "enabled".into(),
                    sol_type: "bool".into(),
                    storage: "bool".into(),
                    indexed: false,
                    components: Vec::new(),
                },
            ],
        }];

        // A view is planned when defined, so `pool_effective_fee_summary` was itself never
        // created - a query against it names *itself* as missing, not `pool_effective_fee`.
        let raw = "Catalog Error: Table with name pool_effective_fee_summary does not exist!";
        let msg = enrich_query_error(
            dir.path(),
            raw,
            "SELECT * FROM pool_effective_fee_summary",
            &schema,
        )
        .unwrap();
        assert!(
            msg.contains("pool_effective_fee_summary") && msg.contains("failed to build"),
            "names the queried view: {msg}"
        );
        assert!(
            msg.contains("pool_effective_fee") && msg.contains("30-summary.sql"),
            "names the dependency and where the dependent view lives: {msg}"
        );
        assert!(
            msg.contains("COALESCE") && msg.contains("explicit cast"),
            "surfaces the *root* cause, not a repeat of \"does not exist\": {msg}"
        );
    }

    /// An ordinary unknown-table typo - no authored view anywhere named after it - must fall through
    /// to the normal fuzzy-match hint unchanged.
    #[test]
    fn an_unrelated_missing_table_is_unaffected_by_view_build_failure_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"5","block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 10, 10).unwrap();

        let schema = vec![crate::registry::TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "Transfer".into(),
            topic0: "0xddf2".into(),
            columns: vec![],
        }];
        let raw = "Catalog Error: Table with name transfers does not exist!";
        let msg = enrich_query_error(dir.path(), raw, "SELECT * FROM transfers", &schema).unwrap();
        assert!(msg.contains("no table `transfers`"), "{msg}");
        assert!(
            msg.contains("usdc__transfer"),
            "still suggests the real table: {msg}"
        );
    }

    /// #1652: `nosuch` is not an authored view, so explaining it must not open an engine.
    #[test]
    fn an_unknown_table_does_not_rebuild_authored_views() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-recipients.sql"),
            r#"CREATE VIEW recipients AS SELECT "to" AS addr FROM "usdc__transfer";"#,
        )
        .unwrap();
        let schema = vec![crate::registry::TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "Transfer".into(),
            topic0: "0xddf2".into(),
            columns: vec![],
        }];
        let before = view_build_opens();
        let raw = "Catalog Error: Table with name nosuch does not exist!";
        let msg = enrich_query_error(dir.path(), raw, "SELECT * FROM nosuch", &schema).unwrap();
        assert_eq!(view_build_opens() - before, 0);
        assert!(msg.contains("no table `nosuch`"), "{msg}");
    }

    /// RFC-0001 acceptance: `/sql` can JOIN across two per-event tables.
    #[test]
    fn sql_joins_across_two_tables() {
        let dir = tempfile::tempdir().unwrap();
        let entities = vec![
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xb","value":"5","block_number":10,"tx_hash":"0xt","log_index":0}"#.to_string(),
            r#"{"table":"usdc__transfer","from":"0xa","to":"0xc","value":"7","block_number":11,"tx_hash":"0xu","log_index":0}"#.to_string(),
            r#"{"table":"usdc__approval","owner":"0xa","spender":"0xd","value":"9","block_number":10,"tx_hash":"0xt","log_index":1}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 10, 11).unwrap();

        // Transfers that occurred in a block where an approval also happened (join on block_number).
        let rows = query(
            dir.path(),
            r#"SELECT t.block_number AS b, t."to" AS recip, a.spender AS appr
               FROM "usdc__transfer" t JOIN "usdc__approval" a USING (block_number)"#,
        )
        .unwrap();
        assert_eq!(rows.len(), 1); // only block 10 has both
        assert_eq!(rows[0]["b"], Value::from(10u64));
        assert_eq!(rows[0]["recip"], Value::from("0xb"));
        assert_eq!(rows[0]["appr"], Value::from("0xd"));
    }

    /// The regression test for a **real vulnerability** found while writing the audit-tail coverage:
    /// `/sql` accepted `;`-stacked statements, which was an arbitrary file-write primitive on an
    /// unauthenticated GET surface.
    ///
    /// The leading-keyword gate only inspects the first statement, `conn.prepare` turned out NOT to be
    /// single-statement (it prepares *and executes* a stacked INSERT), and the "no durable target"
    /// argument does not apply to `COPY … TO` or `ATTACH`, which write to the filesystem whatever the
    /// connection holds. Verified end-to-end before the fix: both payloads below wrote real files.
    #[test]
    fn the_sql_surface_refuses_stacked_statements_and_writes_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let exfil = dir.path().join("exfil.csv");
        let evil_db = dir.path().join("evil.db");

        let payloads = [
            format!("SELECT 1; COPY (SELECT 42 AS x) TO '{}'", exfil.display()),
            format!("SELECT 1; ATTACH '{}' AS evil", evil_db.display()),
            "SELECT 1; CREATE TABLE evil (x INTEGER)".to_string(),
            "SELECT 1; INSERT INTO whatever VALUES (1)".to_string(),
            // Comments must not smuggle the separator past the scan, as elsewhere in this module.
            format!(
                "SELECT 1 /* hi */; COPY (SELECT 1) TO '{}'",
                exfil.display()
            ),
        ];
        for sql in &payloads {
            let err = query(dir.path(), sql)
                .expect_err(&format!("must be refused: {sql}"))
                .to_string();
            assert!(err.contains("single statement"), "{sql} -> {err}");
        }

        // The point of the test: nothing reached the filesystem.
        assert!(!exfil.exists(), "a stacked COPY wrote a file");
        assert!(!evil_db.exists(), "a stacked ATTACH created a database");
    }

    /// The guard must not break legitimate SQL: a semicolon inside a string literal or a quoted
    /// identifier is data, and a trailing semicolon is how most people end a query.
    #[test]
    fn statement_stacking_guard_allows_semicolons_that_are_not_separators() {
        for ok in [
            "SELECT ';'",
            "SELECT 'a;b' AS s",
            "SELECT 'it''s; fine'",
            r#"SELECT 1 AS ";""#,
            "SELECT 1;",
            "SELECT 1;   ",
            "SELECT 1; -- trailing comment",
        ] {
            assert!(
                reject_statement_stacking(ok).is_ok(),
                "legitimate query rejected: {ok}"
            );
        }
        for bad in [
            "SELECT 1; SELECT 2",
            "SELECT ';'; DROP TABLE t",
            "SELECT 1;;SELECT 2",
        ] {
            assert!(
                reject_statement_stacking(bad).is_err(),
                "stacked query accepted: {bad}"
            );
        }
    }

    /// SEC-7: a CTE list is only a prefix. The statement after it must still be a query.
    ///
    /// The leading-keyword gate accepts `WITH`, and an earlier comment claimed the engine would not
    /// parse INSERT after a CTE, the same class of claim as "`conn.prepare` is single-statement",
    /// which was false. This guard is ours, on the public `query` path, so deleting the call
    /// from `attempt` fails the last assertion rather than leaving a unit-tested function with
    /// no caller.
    #[test]
    fn with_prefixed_dml_is_refused_on_the_public_query_path() {
        for ok in [
            "WITH t AS (SELECT 1 AS x) SELECT x FROM t",
            "with t as (select 1 as x) select x from t",
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t",
            "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM a UNION ALL SELECT * FROM b",
            r#"WITH "t" AS (SELECT 1 AS x) SELECT x FROM "t""#,
            "WITH t AS MATERIALIZED (SELECT 1 AS x) SELECT x FROM t",
            "WITH t AS NOT MATERIALIZED (SELECT 1 AS x) SELECT x FROM t",
            // INSERT is data, not a statement, when it lives in a string inside the CTE.
            "WITH t AS (SELECT 'INSERT' AS s) SELECT s FROM t",
            // Non-ASCII after a keyword once sliced a character in half and answered 500.
            "WITH abéé AS (SELECT 1 AS x) SELECT x FROM abéé",
            "/* é */ WITH t AS (SELECT 'é' AS s) SELECT s FROM t",
            "SELECT 1",
        ] {
            assert!(
                reject_with_prefixed_dml(ok).is_ok(),
                "legitimate query refused: {ok}"
            );
        }
        for bad in [
            "WITH t AS (SELECT 1 AS x) INSERT INTO t SELECT 1",
            "with t as (select 1 as x) insert into t select 1",
            "WITH t AS (SELECT 1 AS x) UPDATE t SET x = 2",
            "WITH t AS (SELECT 1 AS x) DELETE FROM t",
            "WITH t AS (SELECT 1 AS x) COPY t TO '/tmp/x.csv'",
            "WITH t AS (SELECT 1 AS x) MERGE INTO t USING t ON true",
            "WITH t AS (SELECT 1 AS x) CREATE TABLE x AS SELECT 1",
            // Comments must not smuggle DML past the CTE list.
            "WITH t AS (SELECT 1 AS x) /* hi */ INSERT INTO t SELECT 1",
            "WITH t AS (SELECT 1 AS x) /* é */ INSERT INTO t SELECT 1",
            "WITH x AS (SELECT 1) aéé",
        ] {
            let err = reject_with_prefixed_dml(bad)
                .expect_err(&format!("must be refused: {bad}"))
                .to_string();
            assert!(err.contains("WITH-prefixed DML"), "{bad} -> {err}");
        }

        let dir = tempfile::tempdir().unwrap();
        let err = query(
            dir.path(),
            "WITH t AS (SELECT 1 AS x) INSERT INTO t SELECT 1",
        )
        .expect_err("WITH-prefixed INSERT must be refused on the public path")
        .to_string();
        assert!(
            err.contains("WITH-prefixed DML"),
            "the refusal must come from our gate, not the engine later: {err}"
        );

        let exfil = dir.path().join("exfil.csv");
        let err = query(
            dir.path(),
            &format!("WITH t AS (SELECT 42 AS x) COPY t TO '{}'", exfil.display()),
        )
        .expect_err("WITH-prefixed COPY must be refused")
        .to_string();
        assert!(err.contains("WITH-prefixed DML"), "{err}");
        assert!(!exfil.exists(), "a WITH-prefixed COPY wrote a file");
    }

    /// Issue #150: a value of more than 38 digits must be dropped **identically** by the cold fold and
    /// the hot replay, or a warm restart would silently change balances.
    ///
    /// The two paths reject it in two languages - the cold fold via `TRY_CAST(… AS DECIMAL(38,0))`
    /// yielding NULL, the hot replay via `views::transfer_value` - so their agreement is worth
    /// pinning. Both must drop the *whole transfer*:
    /// dropping only one leg would invent value out of nowhere, leaving the sender debited and the
    /// recipient uncredited (or worse).
    #[test]
    fn an_oversized_value_is_dropped_identically_by_the_cold_fold_and_the_hot_replay() {
        // 10^38, the smallest value that must be refused; it fits i128, the old line.
        const TOO_BIG: &str = "100000000000000000000000000000000000000";
        assert!(
            crate::views::transfer_value(TOO_BIG).is_none(),
            "fixture must be refused by the hot replay"
        );

        let row = |from: &str, to: &str, value: &str, block: u64, li: u64| {
            format!(
                r#"{{"table":"t__transfer","from":"{from}","to":"{to}","value":"{value}","block_number":{block},"tx_hash":"0x1","log_index":{li}}}"#
            )
        };

        // A segment holding one ordinary transfer and one that overflows.
        let mixed = tempfile::tempdir().unwrap();
        crate::seal::seal_range(
            mixed.path(),
            &[
                row("0xsender", "0xrecipient", "100", 1, 0),
                row("0xwhale", "0xrecipient", TOO_BIG, 2, 0),
            ],
            1,
            6,
        )
        .unwrap();

        // The reference: the same segment WITHOUT the overflowing row - i.e. what the hot replay
        // produces, since its parse-or-skip never feeds that transfer to the view at all.
        let reference = tempfile::tempdir().unwrap();
        crate::seal::seal_range(
            reference.path(),
            &[row("0xsender", "0xrecipient", "100", 1, 0)],
            1,
            6,
        )
        .unwrap();

        let fold = |dir: &std::path::Path| {
            let mut v = net_balances(dir, "t__transfer", "from", "to", "value", 6).unwrap();
            v.sort();
            v
        };

        assert_eq!(
            fold(mixed.path()),
            fold(reference.path()),
            "the cold fold must drop an oversized transfer exactly as the hot replay does"
        );

        // Concretely: only the ordinary transfer survives, and both its legs are present.
        let got = fold(mixed.path());
        assert_eq!(
            got,
            vec![
                ("0xrecipient".to_string(), 100i128),
                ("0xsender".to_string(), -100i128),
            ]
        );
        // The whale never appears - neither leg of the dropped transfer leaked through.
        assert!(
            !got.iter().any(|(a, _)| a == "0xwhale"),
            "the sender of a dropped transfer must not be debited: {got:?}"
        );

        // Exposure and velocity count transfers as well as summing them, and the hot replay counts
        // a dropped transfer nowhere. Labels take real addresses, so these are their own nests.
        const MIXER: &str = "0x1111111111111111111111111111111111111111";
        const SENDER: &str = "0x00000000000000000000000000000000000000aa";
        let labeled = |rows: &[String]| {
            let dir = tempfile::tempdir().unwrap();
            let labels = dir.path().join("l.csv");
            std::fs::write(&labels, format!("{MIXER},mixer\n")).unwrap();
            crate::labels::import(dir.path(), &labels).unwrap();
            crate::seal::seal_range(dir.path(), rows, 1, 6).unwrap();
            dir
        };
        let mixed = labeled(&[
            row(SENDER, MIXER, "100", 1, 0),
            row(SENDER, MIXER, TOO_BIG, 2, 0),
        ]);
        let reference = labeled(&[row(SENDER, MIXER, "100", 1, 0)]);
        let sorted = |mut v: Vec<(String, i128, i128)>| {
            v.sort();
            v
        };
        let exposure = |dir: &std::path::Path| {
            sorted(cold_exposure(dir, "t__transfer", "from", "to", "value", 6).unwrap())
        };
        let velocity = |dir: &std::path::Path| {
            sorted(cold_velocity(dir, "t__transfer", "from", "value", 10, 6).unwrap())
        };
        assert_eq!(exposure(mixed.path()), exposure(reference.path()));
        assert_eq!(
            exposure(mixed.path()),
            [(format!("{SENDER}\u{1f}mixer\u{1f}out"), 100, 1)]
        );
        assert_eq!(velocity(mixed.path()), velocity(reference.path()));
        assert_eq!(
            velocity(mixed.path()),
            [(format!("{SENDER}\u{1f}0"), 100, 1)]
        );
    }

    #[test]
    fn cold_fold_respects_the_sealed_through_watermark() {
        // Regression for the warm-restart double-count: the cold fold must be bounded by the persisted
        // `sealed_through`, not read every segment. A crash in the seal->prune window leaves a segment
        // durable while the watermark is still stale AND the same rows still sit in the hot store; if
        // the cold fold ignored the watermark, the rebuild would count those rows twice.
        let dir = tempfile::tempdir().unwrap();
        let seg: Vec<String> = vec![
            r#"{"table":"t__transfer","from":"0x0","to":"0xa","value":"100","block_number":3,"tx_hash":"0x1","log_index":0}"#.to_string(),
        ];
        crate::seal::seal_range(dir.path(), &seg, 1, 6).unwrap();

        // Stale watermark (below the segment's range): the fold contributes nothing. With no segment at
        // or below the watermark the table view has no backing at all, so this returns Err - which
        // `rebuild_balances` treats identically to an empty result ("no cold seed"), leaving the hot
        // replay to own those rows exactly once. Either way, the sealed rows must NOT be folded in.
        let stale =
            net_balances(dir.path(), "t__transfer", "from", "to", "value", 0).unwrap_or_default();
        assert!(
            !stale.contains(&("0xa".to_string(), 100i128)),
            "a stale watermark must exclude not-yet-finalized segments from the cold fold"
        );

        // Watermark at/above the segment: the fold includes it.
        let done = net_balances(dir.path(), "t__transfer", "from", "to", "value", 6).unwrap();
        assert!(done.contains(&("0xa".to_string(), 100i128)));
        assert!(done.contains(&("0x0".to_string(), -100i128)));
    }

    #[test]
    fn sql_caps_result_bytes_not_just_rows() {
        // Regression for the unbounded-result-buffer DoS: a row cap bounds count, not width. 100 rows of
        // ~1 MiB each (~100 MiB) is far under the 50k row cap but past the 64 MiB byte cap, so the
        // guarded surface must stop early and flag truncation rather than materialise it all Rust-side.
        let dir = tempfile::tempdir().unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(30),
            max_rows: 50_000,
        };
        let out = query_guarded(
            dir.path(),
            "SELECT repeat('A', 1000000) AS x FROM range(100)",
            guard,
        )
        .unwrap();
        assert!(
            out.truncated,
            "a wide result must be flagged truncated by the byte cap"
        );
        assert!(
            out.rows.len() < 100,
            "the byte cap must stop before materialising all 100 wide rows (got {})",
            out.rows.len()
        );
        assert!(!out.rows.is_empty());

        // A trusted, unguarded query (cap = None) is never byte-capped - it must return all rows.
        let all = query(
            dir.path(),
            "SELECT repeat('A', 1000000) AS x FROM range(100)",
        )
        .unwrap();
        assert_eq!(
            all.len(),
            100,
            "unguarded trusted queries are not byte-capped"
        );
    }

    /// macOS reports bytes. Other targets report kilobytes.
    fn resident_bytes() -> u64 {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        assert_eq!(rc, 0);
        let rss = unsafe { usage.assume_init().ru_maxrss } as u64;
        if cfg!(any(target_os = "macos", target_os = "ios")) {
            rss
        } else {
            rss.saturating_mul(1024)
        }
    }

    /// #1650: one DataFusion batch of 8,192 cells, 100 KB each, before the 64 MiB cap runs.
    /// Ignored because the point is the resident size, and the suite should not allocate it.
    #[test]
    #[ignore = "resident size of one wide batch"]
    fn probe_wide_batch_rss() {
        let dir = tempfile::tempdir().unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(120),
            max_rows: 50_000,
        };
        let out = query_guarded(
            dir.path(),
            "SELECT repeat('A', 100000) AS x FROM range(8192)",
            guard,
        )
        .unwrap();
        eprintln!(
            "probe rows={} truncated={} rss_bytes={}",
            out.rows.len(),
            out.truncated,
            resident_bytes()
        );
        assert!(out.truncated, "the byte cap did not fire");
        assert!(out.rows.len() < 8192, "the cap kept the whole batch");
    }

    /// #1650: the byte cap has to see a slice of the batch.
    /// Encoding the whole batch is the allocation.
    #[test]
    fn a_wide_batch_is_encoded_in_slices() {
        crate::engine_burrmill::reset_encoded_rows();
        let dir = tempfile::tempdir().unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(30),
            max_rows: 50_000,
        };
        let out = query_guarded(
            dir.path(),
            "SELECT repeat('A', 8) AS x FROM range(200)",
            guard,
        )
        .unwrap();
        assert_eq!(out.rows.len(), 200);
        assert!(!out.truncated);
        assert_eq!(
            crate::engine_burrmill::max_encoded_rows(),
            32,
            "the cap encoded a whole batch"
        );
    }

    /// #1650: one statement of 8,192 wide cells stays inside the cursor. The resident size is the
    /// process's, so the query runs in a child. On main that child peaks near 1.8 GB.
    #[test]
    fn a_wide_batch_stays_under_the_cursor() {
        if std::env::var_os("NUTHATCH_WIDE_RSS").is_none() {
            let exe = std::env::current_exe().unwrap();
            let child = std::process::Command::new(exe)
                .args([
                    "analytics::tests::a_wide_batch_stays_under_the_cursor",
                    "--exact",
                    "--nocapture",
                ])
                .env("NUTHATCH_WIDE_RSS", "1")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&child.stdout);
            let stderr = String::from_utf8_lossy(&child.stderr);
            eprint!("{stdout}{stderr}");
            assert!(
                child.status.success() && stderr.contains("wide rows="),
                "wide batch child failed\n{stdout}{stderr}"
            );
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(120),
            max_rows: 50_000,
        };
        let out = query_guarded(
            dir.path(),
            "SELECT repeat('A', 100000) AS x FROM range(8192)",
            guard,
        )
        .unwrap();
        let rss = resident_bytes();
        eprintln!(
            "wide rows={} truncated={} rss_bytes={rss}",
            out.rows.len(),
            out.truncated
        );
        assert!(out.truncated, "the byte cap did not fire");
        assert!(out.rows.len() < 8192, "the cap kept the whole batch");
        assert!(
            rss < 512 * 1024 * 1024,
            "one wide batch resident {rss} bytes"
        );
    }

    /// **Issue #419.** A sealed segment that is present on disk but unreadable must *reduce* the
    /// table, not delete it.
    ///
    /// `read_parquet` binds every listed file's footer while the view is being created, so one
    /// corrupt segment throws at DDL time. That failure used to be swallowed, which meant the view was
    /// never created and `/sql` answered `Table with name ... does not exist` - sending an operator to
    /// hunt for a config or naming fault when the actual fault is a file on disk. It also leaked the
    /// internal `__hot_<table>` temp table through the engine's did-you-mean, to an untrusted caller.
    ///
    /// The missing-segment case one screen up in `define_views` has always done the right thing (drop
    /// it, `warn!`, carry on). Present-but-corrupt is the more alarming of the two and was the quieter,
    /// which is the asymmetry this pins.
    #[test]
    fn a_corrupt_sealed_segment_reduces_the_table_rather_than_deleting_it() {
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal: this is about a damaged file, not about the table floor (#1150).
        crate::seal::test_set_table_floor(dir.path(), 0);
        // A schema, so that dropping *every* sealed file still yields the empty **typed** view rather
        // than no view at all. Without it the rebuild-from-nothing case deletes the table, and the
        // assertions below could not tell "rebuilt from the good segment" from "rebuilt from nothing" -
        // both would fail on the `expect` above them. With it, the two are distinguishable, which is
        // what makes the row assertions load-bearing.
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true},
                {"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]}]}"#,
        )
        .unwrap();
        // Two segments, one block each, so a query can tell which survived.
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"t__transfer","from":"0xa","value":"1","block_number":1,"tx_hash":"0xt","log_index":0}"#.to_string()],
            1,
            1,
        )
        .unwrap();
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"t__transfer","from":"0xb","value":"2","block_number":2,"tx_hash":"0xt","log_index":0}"#.to_string()],
            2,
            2,
        )
        .unwrap();

        // Both segments read before anything is touched - otherwise the assertions below could pass on
        // a table that was never whole.
        let rows = query(
            dir.path(),
            r#"SELECT "from" FROM "t__transfer" ORDER BY block_number"#,
        )
        .expect("both segments readable");
        assert_eq!(rows.len(), 2, "two segments, two rows");

        // Corrupt the second segment in place, as an operator would find it: still listed in the
        // manifest, still on disk, no longer a Parquet file. No restart, so the startup integrity pass
        // has not quarantined it.
        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let segs = &manifest.tables["t__transfer"];
        let victim = segs
            .iter()
            .find(|s| s.from_block == 2)
            .expect("the block-2 segment");
        let path = crate::seal::segment_path(dir.path(), &victim.file, &victim.hash);
        std::fs::write(&path, b"not parquet, not even close").unwrap();

        // The table still answers, from the segment that is still good.
        let rows = query(dir.path(), r#"SELECT "from" FROM "t__transfer""#)
            .expect("a corrupt segment must not delete the table from the SQL surface");
        assert_eq!(rows.len(), 1, "the readable segment's row survives");
        assert_eq!(
            rows[0]["from"],
            Value::from("0xa"),
            "and it is the block-1 row, not the corrupt one"
        );
    }

    /// Overwrite a Parquet file's **data region** and leave its footer and magic bytes intact.
    ///
    /// The layout is `PAR1 | data | thrift footer | u32 footer_len | PAR1`, so the last eight bytes
    /// give the footer length and everything from byte 4 up to it is pages. Corrupting exactly that
    /// range is the condition #433 names: `read_parquet` binds the file happily and dies reading it.
    ///
    /// Returns the number of bytes it destroyed, so a caller can assert it actually did something -
    /// a helper that silently corrupted nothing would make every test built on it vacuous.
    fn corrupt_pages_leaving_the_footer_intact(path: &std::path::Path) -> usize {
        let mut bytes = std::fs::read(path).unwrap();
        let len = bytes.len();
        assert!(len > 12 && &bytes[..4] == b"PAR1" && &bytes[len - 4..] == b"PAR1");
        let footer_len = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as usize;
        let end = len - 8 - footer_len;
        assert!(end > 4, "the fixture must have a data region to corrupt");
        bytes[4..end].fill(0xFF);
        std::fs::write(path, &bytes).unwrap();
        end - 4
    }

    /// **Issue #433.** A sealed segment whose **pages** are corrupt but whose **footer reads fine**
    /// must reduce its table, exactly as a footer-corrupt one does since #430 - not fail the whole
    /// query with a Parquet decode error that names no file.
    ///
    /// This is the other half of the class #430 opened, and it does not go through #430's machinery at
    /// all. #430 discriminates when a segment binds, which reads the **footer**. Corruption that
    /// leaves the footer intact passes that untouched, the view is defined, and the failure lands at
    /// execution - taking every table in the query down, not
    /// just this one, with a message that names nothing.
    ///
    /// The fixture's own claim is asserted rather than assumed: the corrupt file must still **bind**.
    /// If it did not, this test would be a second copy of the #430 test wearing a different name, and
    /// would pass with the #433 mechanism deleted.
    #[test]
    fn a_page_corrupt_segment_with_an_intact_footer_reduces_the_table_rather_than_failing_the_query(
    ) {
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal: this is about a damaged file, not about the table floor (#1150).
        crate::seal::test_set_table_floor(dir.path(), 0);
        // A schema, for the reason the #419 test above gives: it makes "rebuilt from the good segment"
        // distinguishable from "rebuilt from nothing".
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true},
                {"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]}]}"#,
        )
        .unwrap();
        for (block, from) in [(1u64, "0xa"), (2u64, "0xb")] {
            crate::seal::seal_range(
                dir.path(),
                &[format!(
                    r#"{{"table":"t__transfer","from":"{from}","value":"{block}","block_number":{block},"tx_hash":"0xt","log_index":0}}"#
                )],
                block,
                block,
            )
            .unwrap();
        }

        // Whole before anything is touched, or the reduction assertions below prove nothing.
        let rows = query(dir.path(), r#"SELECT "from" FROM "t__transfer""#)
            .expect("both segments readable");
        assert_eq!(rows.len(), 2, "two segments, two rows");

        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let victim = manifest.tables["t__transfer"]
            .iter()
            .find(|s| s.from_block == 2)
            .expect("the block-2 segment");
        let path = crate::seal::segment_path(dir.path(), &victim.file, &victim.hash);
        let destroyed = corrupt_pages_leaving_the_footer_intact(&path);
        assert!(destroyed > 0, "the fixture must have corrupted something");

        // **The fixture is the condition it claims to be.** #430's probe still says this file is fine,
        // so nothing in the footer-corrupt path can be what makes the assertions below pass.
        assert!(
            crate::seal::footer_reads(&path),
            "this test is about a segment that BINDS and then will not read - if it no longer binds \
             it is #430's case and this test has stopped testing #433"
        );

        // The table still answers, from the segment that is still good.
        let rows = query(dir.path(), r#"SELECT "from" FROM "t__transfer""#).expect(
            "a page-corrupt segment must reduce the table, not fail the query with `don't know what \
             type:`",
        );
        assert_eq!(rows.len(), 1, "the readable segment's row survives");
        assert_eq!(
            rows[0]["from"],
            Value::from("0xa"),
            "and it is the block-1 row, not the corrupt one"
        );
    }

    /// A nest with a schema and two one-block segments for `t__transfer`, blocks 1 and 2, rows `0xa`
    /// and `0xb`. The shape both reduction tests above build by hand; shared by the #435 tests below
    /// so the healthy control and the degraded cases are provably the *same* fixture apart from the
    /// corruption - a control built separately could differ in some other way and stop controlling.
    fn two_segment_nest() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal, or "two segments" is one: these fixtures are about a damaged file,
        // not about the table floor (#1150).
        crate::seal::test_set_table_floor(dir.path(), 0);
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true},
                {"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]}]}"#,
        )
        .unwrap();
        for (block, from) in [(1u64, "0xa"), (2u64, "0xb")] {
            crate::seal::seal_range(
                dir.path(),
                &[format!(
                    r#"{{"table":"t__transfer","from":"{from}","value":"{block}","block_number":{block},"tx_hash":"0xt","log_index":0}}"#
                )],
                block,
                block,
            )
            .unwrap();
        }
        dir
    }

    /// The block-2 segment's file, for a test that wants to damage it.
    fn block_two_segment(dir: &std::path::Path) -> std::path::PathBuf {
        let manifest = crate::seal::load_manifest(dir).unwrap();
        let victim = manifest.tables["t__transfer"]
            .iter()
            .find(|s| s.from_block == 2)
            .expect("the block-2 segment");
        crate::seal::segment_path(dir, &victim.file, &victim.hash)
    }

    fn cold(dir: &std::path::Path, sql: &str) -> QueryOutput {
        query_guarded(
            dir,
            sql,
            QueryGuard {
                timeout: Duration::from_secs(30),
                max_rows: 10_000,
            },
        )
        .expect("a reduced table must still answer")
    }

    /// #1609: the projection order reaches the caller, including on an answer with no rows.
    #[test]
    fn a_result_names_its_columns_in_the_querys_order() {
        let dir = tempfile::tempdir().unwrap();
        let out = cold(dir.path(), "SELECT 1 AS z, 2 AS a, 3 AS m");
        assert_eq!(out.columns, ["z", "a", "m"]);
        assert_eq!(out.rows, vec![serde_json::json!({"z": 1, "a": 2, "m": 3})]);

        let empty = cold(dir.path(), "SELECT 1 AS z, 2 AS a WHERE false");
        assert!(empty.rows.is_empty());
        assert_eq!(empty.columns, ["z", "a"]);
    }

    /// **Issue #435, the control.** A nest whose segments are all intact must report **no**
    /// degradation.
    ///
    /// This is the load-bearing half of the pair. Every other #435 test asserts that a flag is *set*,
    /// and all of them would pass just as well if the flag were hard-wired to "always degraded" -
    /// which would be worse than no flag at all, because a warning that fires on every healthy query
    /// is a warning an operator learns to scroll past. Nothing downstream (the `/sql` field, the CLI
    /// line, the MCP notice) is worth anything unless silence on the healthy path is pinned here.
    #[test]
    fn an_intact_nest_reports_no_degradation() {
        let dir = two_segment_nest();
        let out = cold(dir.path(), r#"SELECT "from" FROM "t__transfer""#);
        assert_eq!(out.rows.len(), 2, "two intact segments, two rows");
        assert!(
            out.degraded_tables.is_empty(),
            "an intact nest must report nothing degraded, got {:?}",
            out.degraded_tables
        );
        assert!(!out.degraded(), "and the one-bit form must agree");
    }

    /// **Issue #435, the footer-corrupt half (#419/#430's path).** A segment dropped at DDL time
    /// reduces the table *and says so in the result*.
    ///
    /// #430 chose reduction over deletion and was right to, but it left the decision visible only in
    /// a `warn!` the caller cannot read: the query returns `200` with fewer rows and nothing to
    /// distinguish that from a table which genuinely holds one row. `SELECT SUM(value)` over this
    /// nest answers `1` where the truth is `3`, and both a human and an agent will take it.
    #[test]
    fn a_footer_corrupt_segment_names_its_table_in_the_result() {
        let dir = two_segment_nest();
        // Present, listed in the manifest, no longer a Parquet file - and not quarantined, because
        // nothing has restarted.
        std::fs::write(
            block_two_segment(dir.path()),
            b"not parquet, not even close",
        )
        .unwrap();

        let out = cold(dir.path(), r#"SELECT "from" FROM "t__transfer""#);
        assert_eq!(out.rows.len(), 1, "reduced to the readable segment (#430)");
        assert!(
            out.degraded(),
            "and the caller is told the answer is short, not left to infer it from one row"
        );
        assert_eq!(
            out.degraded_tables,
            ["t__transfer".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "the reduced table is named, so a caller can tell which of its numbers to distrust"
        );
    }

    /// **Issue #435, the page-corrupt half (#433's path).** The reduction that happens on the *retry*
    /// must be reported too.
    ///
    /// Worth its own test rather than folding into the one above, because it enters `define_views`
    /// through an entirely different door: the footer-corrupt segment is dropped by the `conn.prepare`
    /// probe on the first attempt, whereas this one binds cleanly, kills the query at execution, and
    /// is only excluded on the second attempt via `segments_failing_verification`. A `degraded_tables`
    /// wired into the first path alone would pass the test above and leave the #433 case - the one
    /// that motivated #435 in the first place - silent.
    #[test]
    fn a_page_corrupt_segment_names_its_table_in_the_result() {
        let dir = two_segment_nest();
        let path = block_two_segment(dir.path());
        assert!(
            corrupt_pages_leaving_the_footer_intact(&path) > 0,
            "the fixture must have corrupted something"
        );
        // The fixture is the condition it claims to be: #430's probe still passes this file, so the
        // footer-corrupt path cannot be what sets the flag below.
        assert!(
            crate::seal::footer_reads(&path),
            "if it no longer binds this is #430's case and the test has stopped testing #433's"
        );

        let out = cold(dir.path(), r#"SELECT "from" FROM "t__transfer""#);
        assert_eq!(out.rows.len(), 1, "reduced on the retry (#433)");
        assert_eq!(
            out.degraded_tables,
            ["t__transfer".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "the retry's exclusions are degradation too - the query succeeded with less data"
        );
    }

    /// A nest with **two** independently-populated tables, `t__transfer` (blocks 1-2) and
    /// `t__approval` (blocks 1-2), both intact. Every fixture up to #477 - `two_segment_nest` above
    /// included - has exactly one table, so "the nest is degraded" and "this query's table is
    /// degraded" were the same set in every assertion in the tree: a `define_views` bug that flagged
    /// the wrong table, or every table, would have passed all of them, because there was never a
    /// second, untouched table to catch it naming the wrong one. Corruption is the caller's job, on
    /// whichever table it wants degraded - this fixture ships both intact.
    fn two_table_nest() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal, as `two_segment_nest` (#1150).
        crate::seal::test_set_table_floor(dir.path(), 0);
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[
                {"table":"t__transfer","columns":[
                    {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                    {"name":"from","sol_type":"address","storage":"address","indexed":true},
                    {"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]},
                {"table":"t__approval","columns":[
                    {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                    {"name":"owner","sol_type":"address","storage":"address","indexed":true}]}]}"#,
        )
        .unwrap();
        for (block, from) in [(1u64, "0xa"), (2u64, "0xb")] {
            crate::seal::seal_range(
                dir.path(),
                &[format!(
                    r#"{{"table":"t__transfer","from":"{from}","value":"{block}","block_number":{block},"tx_hash":"0xt","log_index":0}}"#
                )],
                block,
                block,
            )
            .unwrap();
        }
        for (block, owner) in [(1u64, "0xc"), (2u64, "0xd")] {
            crate::seal::seal_range(
                dir.path(),
                &[format!(
                    r#"{{"table":"t__approval","owner":"{owner}","block_number":{block},"tx_hash":"0xt","log_index":0}}"#
                )],
                block,
                block,
            )
            .unwrap();
        }
        dir
    }

    /// `table`'s block-2 segment file, in a [`two_table_nest`] - for a test that wants to damage one
    /// table's data without touching the other's. Mirrors `block_two_segment` above, scoped to a
    /// named table since this fixture has two.
    fn block_two_segment_of(dir: &std::path::Path, table: &str) -> std::path::PathBuf {
        let manifest = crate::seal::load_manifest(dir).unwrap();
        let victim = manifest.tables[table]
            .iter()
            .find(|s| s.from_block == 2)
            .expect("the block-2 segment");
        crate::seal::segment_path(dir, &victim.file, &victim.hash)
    }

    /// **Issue #477, case 1.** A two-table nest with one table degraded: a query against the
    /// *healthy* one must come back complete and correct, and the flag it carries must name the
    /// *other* table - never itself. `degraded_tables` comes from `define_views`'s
    /// schema ∪ manifest ∪ hot walk, never from the query's own `FROM` clause, so main.rs's and
    /// mcp.rs's caveat renderers say nothing about *this result* - only about whichever tables the
    /// set names. Excluding the healthy table here is what stops that caveat becoming a false
    /// statement about a complete answer.
    #[test]
    fn a_healthy_table_names_the_other_ones_degradation() {
        let dir = two_table_nest();
        std::fs::write(
            block_two_segment_of(dir.path(), "t__transfer"),
            b"not parquet, not even close",
        )
        .unwrap();

        let out = cold(
            dir.path(),
            r#"SELECT "owner" FROM "t__approval" ORDER BY "owner""#,
        );
        assert_eq!(
            out.rows,
            vec![
                serde_json::json!({"owner": "0xc"}),
                serde_json::json!({"owner": "0xd"}),
            ],
            "the healthy table's own segments are both intact - nothing about its answer is short"
        );
        // **The contract changed with #896, deliberately.** A query used to survey the whole nest,
        // because `define_views` bound every table in the manifest on every request - which is where
        // 2.5 seconds of a 38,428-segment nest's request time went. A query now reports what *it*
        // reached, and this one reached nothing damaged.
        assert!(
            out.degraded_tables.is_empty(),
            "a query that read only healthy segments has nothing to caveat: {:?}",
            out.degraded_tables
        );

        // The nest-wide fact has not been lost, only moved somewhere it can be reported without a
        // caller stumbling into it. `/ready` surfaces this; the sweep is what finds it.
        let swept = degraded_tables(dir.path(), &[]).unwrap();
        assert_eq!(
            swept,
            ["t__transfer".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "the sweep must name the table that is actually short"
        );
        assert!(!swept.contains("t__approval"), "and never the healthy one");
    }

    /// **Issue #477, case 2.** `SELECT 1` and `.tables` (`information_schema.tables`, the query the
    /// REPL's `.tables` dot-command runs) draw no row from any table at all, healthy or degraded - no
    /// total to understate, nothing to call short. `define_views` runs before the caller's SQL and
    /// without looking at it, so the flag comes back identical to a query that actually reads the
    /// degraded table. That is the property that lets a caller trust the flag even on a query it
    /// cannot line up against any particular row.
    #[test]
    fn select_one_and_dot_tables_carry_the_flag_with_no_rows_to_understate() {
        let dir = two_table_nest();
        std::fs::write(
            block_two_segment_of(dir.path(), "t__transfer"),
            b"not parquet, not even close",
        )
        .unwrap();
        let degraded: std::collections::BTreeSet<String> =
            ["t__transfer".to_string()].into_iter().collect();

        // **`SELECT 1` used to survey the nest, and that was the cost** - it bound every table in
        // the manifest to find out, which is 2.5 seconds on a real one (#896). It draws from no
        // table, so it now reports on no table.
        let one = cold(dir.path(), "SELECT 1 AS one");
        assert_eq!(one.rows, vec![serde_json::json!({"one": 1})]);
        assert!(
            one.degraded_tables.is_empty(),
            "a query that reads nothing caveats nothing: {:?}",
            one.degraded_tables
        );

        // `.tables` is the one shape that still surveys, and has to: listing the catalogue means
        // every view must exist to be listed. `reachable_tables` refuses to narrow a statement it
        // cannot vouch for, and this is one - so the flag it carries is the old one.
        let tables = cold(
            dir.path(),
            "SELECT table_name FROM information_schema.tables \
             WHERE NOT starts_with(table_name, '__hot_') ORDER BY table_name",
        );
        assert_eq!(
            tables.degraded_tables, degraded,
            ".tables lists the catalogue, so it defines the catalogue, so it still learns"
        );

        // And the sweep knows regardless of what anybody queried.
        assert_eq!(degraded_tables(dir.path(), &[]).unwrap(), degraded);
    }

    /// **Issue #477, case 3 (#434's shape).** A view that fails for a reason no segment probe would
    /// ever catch still lands in `degraded_tables` - here, the view name is already taken in the
    /// catalogue, which `define_views` cannot tell apart from any other whole-DDL failure once every
    /// individual segment has already bound (`readable.len() == sealed_files.len()`, the branch #434
    /// occupied before its fix). No file on either table is touched, so this proves the flag does not
    /// depend on - and the caveat therefore must not name - a segment-level cause.
    #[test]
    fn an_undefinable_view_degrades_with_every_segment_intact() {
        crate::engine::on_bare(an_undefinable_view_degrades_with_every_segment_intact_on);
    }

    fn an_undefinable_view_degrades_with_every_segment_intact_on(
        conn: &dyn crate::engine::Session,
    ) {
        let dir = two_table_nest();
        conn.execute(r#"CREATE TABLE "t__transfer" AS SELECT CAST(1 AS INTEGER) AS x"#)
            .unwrap();

        let degraded = define_views(
            conn,
            dir.path(),
            &HotRows::new(),
            u64::MAX,
            &std::collections::BTreeSet::new(),
            &[],
            None,
        )
        .unwrap();
        assert_eq!(
            degraded,
            ["t__transfer".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "the pre-existing catalogue name, not any segment, is why the view failed"
        );
        assert!(
            !degraded.contains("t__approval"),
            "the other table's view was never touched by the collision"
        );

        // Prove the premise: every segment behind the table still binds on its own, so nothing here
        // went through the corrupt/missing-file paths above - only the undefinable-view arm could
        // have set the flag.
        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        for seg in &manifest.tables["t__transfer"] {
            let path = crate::seal::segment_path(dir.path(), &seg.file, &seg.hash);
            assert!(
                crate::seal::footer_reads(&path),
                "every segment must still bind on its own for this to be the undefinable-view arm"
            );
        }
    }

    /// **Issue #433, the half that bounds the cost.** `collect` must report *which phase* a query
    /// died in, because that is what decides whether the integrity sweep runs at all.
    ///
    /// `/sql` is untrusted and the caller writes the query, so "any failure hashes every segment in
    /// the nest" would be a denial-of-service amplifier built out of an integrity check - and a bind
    /// failure cannot have been caused by a corrupt page, so it must never provoke one.
    ///
    /// This asserts the discriminator **directly** rather than through a query's error message. I
    /// wrote the message version first and then killed it: with the phase split deleted, a binder
    /// error still comes back with the same text (the sweep finds nothing to change and the error is
    /// returned either way), so that test passed with the mechanism gone. Which is the exact failure
    /// this sprint is about, in my own new fixture.
    #[test]
    fn collect_separates_a_bind_failure_from_a_read_failure() {
        crate::engine::on_bare(collect_separates_a_bind_failure_from_a_read_failure_on);
    }

    fn collect_separates_a_bind_failure_from_a_read_failure_on(conn: &dyn crate::engine::Session) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true}]}]}"#,
        )
        .unwrap();
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"t__transfer","from":"0xa","block_number":1,"tx_hash":"0xt","log_index":0}"#.to_string()],
            1,
            1,
        )
        .unwrap();
        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let seg = &manifest.tables["t__transfer"][0];
        let path = crate::seal::segment_path(dir.path(), &seg.file, &seg.hash);
        corrupt_pages_leaving_the_footer_intact(&path);

        let empty = HotRows::new();
        define_views(
            conn,
            dir.path(),
            &empty,
            u64::MAX,
            &Default::default(),
            &[],
            None,
        )
        .unwrap();

        // A name the catalogue does not have: refused by the binder, before a page is touched.
        assert!(
            matches!(
                conn.collect(r#"SELECT no_such_column FROM "t__transfer""#, None),
                Err(Died::Binding(_))
            ),
            "a missing column is a bind failure - it cannot have been caused by a corrupt page"
        );

        // The same connection, a query that binds and then reads the corrupt pages.
        assert!(
            matches!(
                conn.collect(r#"SELECT * FROM "t__transfer""#, None),
                Err(Died::Executing(_))
            ),
            "reading a page-corrupt segment is an execution failure - the only shape worth sweeping for"
        );
    }

    /// Collect every event message a closure logs. Used below to observe whether the integrity sweep
    /// touched a segment at all - the sweep has no return value a caller can see and no semantic
    /// effect when it looks at a table the query never named, so cost is the only thing that changes
    /// and the log line is the only place it surfaces.
    ///
    /// `with_default` being thread-local does **not** make this safe under parallel tests: `tracing`
    /// caches each callsite's `Interest` globally and process-wide the first time it is evaluated, so
    /// a callsite reached by another test on another thread with no subscriber installed can get
    /// cached `Interest::never()` for the rest of the process - and a `with_default` scope here would
    /// then never see that event regardless of what this layer wants. Confirmed on `segments_failing_
    /// verification`'s `tracing::error!` call site, see #482. Prefer a return-value assertion over
    /// `CapturedLogs` wherever the code under test exposes one; use this only as the last resort, and
    /// only for a negative assertion (an event that fails to arrive because it was never worth logging
    /// looks identical to one dropped by this race, so a positive assertion built on `CapturedLogs`
    /// cannot tell those apart and is unreliable by construction).
    #[derive(Clone, Default)]
    struct CapturedLogs(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

    impl CapturedLogs {
        fn mentioning(&self, needle: &str) -> usize {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|l| l.contains(needle))
                .count()
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedLogs {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Msg<'a>(&'a mut String);
            impl tracing::field::Visit for Msg<'_> {
                fn record_debug(
                    &mut self,
                    _f: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write as _;
                    let _ = write!(self.0, "{value:?}");
                }
            }
            let mut line = String::new();
            event.record(&mut Msg(&mut line));
            self.0.lock().unwrap().push(line);
        }
    }

    /// **Issue #433, the cost bound as reviewed.** The phase split was claimed to make "the cheap way
    /// to provoke a sweep" nonexistent. It did not: `SELECT CAST('x' AS INTEGER)` is 27 bytes, names
    /// no table, passes every gate, binds, and dies executing - and it hashed every segment in a
    /// healthy nest, once per request, on a surface with no auth and two concurrency permits.
    ///
    /// So the sweep is bounded by **reachability**: only segments backing tables the failed query
    /// named. A query that names nothing sweeps nothing.
    ///
    /// The measurement is the log line `segments_failing_verification` emits when it rejects a
    /// segment, because the sweep has no other observable: looking at a table the query never
    /// referenced changes no answer, only cost. **The positive control is in this test on purpose** -
    /// an absence assertion whose mechanism is missing passes for the wrong reason, which is the
    /// failure this whole sprint is about.
    #[test]
    fn a_query_that_names_no_table_reaches_no_segment() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let dir = tempfile::tempdir().unwrap();
        // One segment per seal: this is about a damaged file, not about the table floor (#1150).
        crate::seal::test_set_table_floor(dir.path(), 0);
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true}]}]}"#,
        )
        .unwrap();
        for (block, from) in [(1u64, "0xa"), (2u64, "0xb")] {
            crate::seal::seal_range(
                dir.path(),
                &[format!(
                    r#"{{"table":"t__transfer","from":"{from}","block_number":{block},"tx_hash":"0xt","log_index":0}}"#
                )],
                block,
                block,
            )
            .unwrap();
        }
        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let victim = manifest.tables["t__transfer"]
            .iter()
            .find(|s| s.from_block == 2)
            .expect("the block-2 segment");
        let path = crate::seal::segment_path(dir.path(), &victim.file, &victim.hash);
        assert!(
            corrupt_pages_leaving_the_footer_intact(&path) > 0,
            "the fixture must have corrupted something"
        );

        const REJECTED: &str = "does not match its content address";

        // The attack: no table named, so nothing is reachable, so nothing may be hashed - even though
        // this nest does hold a corrupt segment and the query does die in the execution phase.
        let quiet = CapturedLogs::default();
        let err = tracing::subscriber::with_default(
            tracing_subscriber::registry().with(quiet.clone()),
            || query(dir.path(), "SELECT CAST('x' AS INTEGER)").unwrap_err(),
        );
        // `{:#}` for the whole chain: the outermost context is just "query failed", and asserting on
        // that would pass for any execution failure at all, including one this test did not cause.
        let chain = format!("{err:#}");
        assert!(
            chain.contains("Conversion Error"),
            "expected the cast itself to be what failed, got: {chain}"
        );
        // Negative assertion via `CapturedLogs`, not a return value: this is the one place in the
        // binary #482's grep sweep left on the log-capture path, because nothing else observes
        // whether the sweep was reachability-bounded. Safe as a negative check only - see the
        // `CapturedLogs` doc comment for why a positive assertion here would be unreliable.
        assert_eq!(
            quiet.mentioning(REJECTED),
            0,
            "a query naming no table must not read or hash a single segment - this is the 27-byte \
             amplifier the review found"
        );

        // The positive control: a query that *does* name the table sweeps it and finds the corrupt
        // segment. Without this, the assertion above would pass just as happily with the sweep
        // deleted entirely.
        //
        // Asserted on `segments_failing_verification`'s own return value, computed via the same
        // `reject_unknown_table_refs` walk `run()` uses - not by scraping its `tracing::error!` log
        // line. `tracing`'s per-callsite interest is a *global, process-wide* cache: whichever test in
        // this binary hits that callsite first while no subscriber is installed gets it permanently
        // marked uninteresting, so a `with_default` scope elsewhere in the same test binary can miss
        // the event nondeterministically depending on run order (reproduced on `main`, independent of
        // this fix - see #482). The return value has no such race.
        let rows = query(dir.path(), r#"SELECT "from" FROM "t__transfer""#).expect("reduces");
        assert_eq!(rows.len(), 1, "the readable segment's row survives");
        let conn = crate::engine::bare();
        let referenced = reject_unknown_table_refs(&*conn, r#"SELECT "from" FROM "t__transfer""#)
            .unwrap()
            .map(|(names, _)| expand_through_views(&*conn, &names))
            .expect("the query names a table");
        assert_eq!(
            crate::seal::segments_failing_verification(dir.path(), &referenced, None),
            std::collections::BTreeSet::from([victim.hash.clone()]),
            "the same corrupt segment must be found when the query does name its table"
        );
    }

    /// **Issue #476, end to end - tightened for #500.** Before #476's fix, `run`'s watchdog covered
    /// only the query execution either side of the sweep: the sweep itself ran with nothing watching
    /// it, and the #433 retry got a brand-new `guard.timeout` rather than whatever was left of the
    /// first one. A genuinely degraded nest could cost up to `2 x timeout` in execution alone, plus an
    /// unbounded sweep in between.
    ///
    /// #500: the first version of this test could only tell a bounded sweep apart from an *unbounded*
    /// one (`deadline: None`), not from one hand a **fresh** `guard.timeout` at the sweep call site,
    /// which is the defect its name actually calls out - and that is the mutation that matters, since
    /// it is what `deadline` accidentally recomputed at the wrong point would look like. The first
    /// attempt (dying on a page-corrupt segment) is near-instant, so a fresh deadline taken a few
    /// microseconds later was indistinguishable from the shared one, and the elapsed-time window this
    /// test asserted was wide enough to swallow the gap.
    ///
    /// This version makes the first attempt itself consume most of the budget
    /// (`test_set_first_attempt_delay_ms`), so a shared vs. a fresh deadline stop differing by degree
    /// and start differing in kind. Bound to the shared deadline, only a sliver is left when the sweep
    /// starts: it hashes segments one and two (unrelated, intact) but runs out before segment three,
    /// the corrupt one, so `corrupt` comes back empty and `run` fails on its own plain "time budget"
    /// check - a clean, cooperative bail, never touching a second `attempt`.
    ///
    /// Handed a fresh `guard.timeout` instead, the sweep gets the full budget again from that same
    /// later point and reaches the corrupt segment - but the retry it then triggers is still bound by
    /// the *original* shared `deadline` (only the sweep's own deadline was mutated), which by then has
    /// already passed, so the retry's watchdog interrupts it mid-query instead. Both outcomes are
    /// `Err`, so Ok vs Err cannot see this; the *error itself* differs in kind, though - a plain
    /// "exceeded budget" message the bounded sweep produces cooperatively, versus the engine's own
    /// interrupt from a retry that got cut off while running. That is the assertion below, not
    /// elapsed wall-clock time.
    ///
    /// #529: even that was still timing-coupled. The 200ms budget minus a 120ms first-attempt delay
    /// left ~80ms for a 2x50ms sweep to land in - a margin of one segment's worth, and
    /// `thread::sleep` only guarantees *at least* the requested duration. At load average 34 a
    /// descheduled thread can wake arbitrarily later than 50ms, so which of the two branches the
    /// (unmutated, correct) code actually took stopped being reliable: `cargo test --lib` saw the
    /// *other* outcome's message on a busy box. Fixed by making the margin lopsided rather than
    /// tight enough to race: the first-attempt delay (3s) is set to comfortably outlive the whole
    /// 200ms budget by itself, so the shared deadline is unambiguously, already expired - by seconds,
    /// not by a contended few milliseconds - before the sweep is ever called, on every run regardless
    /// of scheduling. The sweep's own per-segment cost is no longer artificially delayed at all: the
    /// fixture's three tiny segments cost microseconds to hash for real, so a *fresh* deadline
    /// (mutation A) or no deadline (mutation B) still has essentially the whole 200ms of real
    /// headroom to reach the corrupt segment in, which is orders of magnitude more slack than
    /// scheduling jitter needs even under heavy contention. The two outcomes no longer share a
    /// finish line to race across; one is already over before the sweep starts, the other has ample
    /// room regardless of load.
    #[test]
    fn a_query_spilling_past_its_cap_is_stopped_by_the_guard() {
        // A sort of 600 million rows, far past the memory limit, so it spills at once. A 4 MB pool and
        // cap trip in a few megabytes of work; at 64 MB a loaded box took 50 s and met the deadline
        // (#1816). The deadline is only the backstop for a guard that never fires.
        let dir = tempfile::tempdir().unwrap();
        let guard = QueryGuard {
            timeout: Duration::from_secs(300),
            max_rows: 10,
        };
        let result = {
            let _env = crate::analytics_budget::tests::env_lock()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let _vars = crate::analytics_budget::tests::EnvVars::set(&[
                (crate::analytics_budget::ENV_MAX_TEMP_SIZE, "4MB"),
                (crate::analytics_budget::ENV_BURRMILL_MEMORY_LIMIT, "4MB"),
            ]);
            query_hot_cold(
                dir.path(),
                "SELECT a.i FROM range(5000000) a(i), range(120) b(j) \
                 ORDER BY (a.i * 2654435761) % 1000003, b.j",
                guard,
                &HotRows::new(),
                0,
                &[],
            )
        };
        let err = result.expect_err("the cross join must not run to completion");
        let cut = err
            .downcast_ref::<QuerySpillExceeded>()
            .unwrap_or_else(|| panic!("stopped for the wrong reason: {err:#}"));
        assert_eq!(cut.cap_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn the_sweep_is_bound_by_the_query_s_own_deadline_not_a_fresh_one() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true}]}]}"#,
        )
        .unwrap();
        for block in [1u64, 2, 3] {
            crate::seal::seal_range(
                dir.path(),
                &[format!(
                    r#"{{"table":"t__transfer","from":"0xa","block_number":{block},"tx_hash":"0xt","log_index":0}}"#
                )],
                block,
                block,
            )
            .unwrap();
        }
        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let victim = manifest.tables["t__transfer"]
            .iter()
            .max_by_key(|s| s.from_block)
            .expect("the block-3 segment");
        let path = crate::seal::segment_path(dir.path(), &victim.file, &victim.hash);
        assert!(
            corrupt_pages_leaving_the_footer_intact(&path) > 0,
            "the fixture must have corrupted something"
        );

        // A 200ms budget with a 3s first-attempt delay: by the time the sweep is ever called, the
        // shared deadline is already several seconds in the past, unambiguously - not a close race
        // against a comparably-sized per-segment cost (see the doc comment above for why that
        // changed). The three fixture segments are real work but microseconds of it, so a fresh or
        // absent deadline still has essentially the whole 200ms of headroom to reach the corrupt one.
        test_set_first_attempt_delay_ms(dir.path(), 3_000);
        let guard = QueryGuard {
            timeout: Duration::from_millis(200),
            max_rows: 10,
        };
        let result = query_guarded(dir.path(), r#"SELECT "from" FROM "t__transfer""#, guard);
        test_set_first_attempt_delay_ms(dir.path(), 0);

        let message = result.as_ref().err().map(ToString::to_string);
        assert_eq!(
            message.as_deref(),
            Some("query exceeded the 0s time budget on the read-only SQL surface"),
            "bound to the shared deadline, the sweep has only a sliver of the 200ms budget left when \
             it starts (120ms already spent on the first attempt) and cannot reach the corrupt third \
             segment before that budget is spent, so `run` must bail on its own cooperative \"time \
             budget\" check without ever starting a second attempt. A sweep handed a fresh 200ms \
             instead reaches the corrupt segment too late for the *original* shared deadline that \
             still bounds the retry, so it dies mid-query on the engine's own interrupt instead - a \
             different error, not just a slower one: got {result:?}"
        );
    }

    /// The other edge of the same bound, and the regression it would otherwise have caused. A query
    /// over an **authored view** (RFC-0001 `views/*.sql`) names the view, which is no table in the
    /// manifest - so a sweep bounded on the named set alone would verify nothing, and a page-corrupt
    /// segment under that view would fail the query instead of reducing it. That is #433's own defect,
    /// reintroduced one layer up by its own cost bound.
    ///
    /// I did not find this from the review; I found it reading my own fix back, which is the only
    /// reason it is not shipping. `expand_through_views` is what this fails without.
    #[test]
    fn a_page_corrupt_segment_under_an_authored_view_still_reduces() {
        let dir = tempfile::tempdir().unwrap();
        // One segment per seal: this is about a damaged file, not about the table floor (#1150).
        crate::seal::test_set_table_floor(dir.path(), 0);
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true}]}]}"#,
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-senders.sql"),
            "CREATE VIEW senders AS SELECT \"from\", block_number FROM t__transfer;",
        )
        .unwrap();
        for (block, from) in [(1u64, "0xa"), (2u64, "0xb")] {
            crate::seal::seal_range(
                dir.path(),
                &[format!(
                    r#"{{"table":"t__transfer","from":"{from}","block_number":{block},"tx_hash":"0xt","log_index":0}}"#
                )],
                block,
                block,
            )
            .unwrap();
        }

        // Whole first, through the view, or the reduction assertion below proves nothing.
        let rows = query(dir.path(), "SELECT \"from\" FROM senders").expect("the view resolves");
        assert_eq!(rows.len(), 2, "two segments, two rows, through the view");

        let manifest = crate::seal::load_manifest(dir.path()).unwrap();
        let victim = manifest.tables["t__transfer"]
            .iter()
            .find(|s| s.from_block == 2)
            .expect("the block-2 segment");
        let path = crate::seal::segment_path(dir.path(), &victim.file, &victim.hash);
        assert!(corrupt_pages_leaving_the_footer_intact(&path) > 0);

        let rows = query(dir.path(), "SELECT \"from\" FROM senders").expect(
            "a query that reaches the corrupt segment through a view must still reduce - if this \
             errors, the reachability bound cannot see through views",
        );
        assert_eq!(rows.len(), 1, "the readable segment's row survives");
        assert_eq!(rows[0]["from"], Value::from("0xa"));
    }

    /// The input to that bound: what the security walk reports a statement reaches. One parse feeds
    /// both controls, so this pins the half `reject_unknown_table_refs` did not used to have.
    /// **#896, found against the real Lodestar nest.** A real authored view breaks the line after
    /// `AS`; the keyword search wanted a literal space on both sides and did not find it. The view
    /// then never entered the map `reachable_tables` builds, its source table was never defined, and
    /// a view that plainly exists came back as `Catalog Error: Table with name … does not exist`.
    ///
    /// Every fixture in this file wrote its view on one line, which is why none of them saw it -
    /// and my first attempt at this test did too, and passed against the broken code.
    #[test]
    fn a_view_that_breaks_the_line_after_as_still_yields_its_source_table() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true}]}]}"#,
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        // The shape of `40-indexers.sql`: prose, then a view whose body starts on the next line.
        std::fs::write(
            dir.path().join("views/10-senders.sql"),
            "-- Per-sender rollup. The count comes from the folded `senders` view (\u{a7}10).\n\
             CREATE VIEW senders AS\n\
             SELECT \"from\", block_number\n\
             FROM t__transfer;",
        )
        .unwrap();
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"t__transfer","from":"0xa","block_number":1,"tx_hash":"0xt","log_index":0}"#
                .to_string()],
            1,
            1,
        )
        .unwrap();

        let rows = query(dir.path(), r#"SELECT "from" FROM senders"#)
            .expect("a view whose body starts on the next line still resolves");
        assert_eq!(rows.len(), 1, "{rows:?}");
    }

    #[test]
    fn the_table_refs_walk_reports_what_the_statement_reached() {
        let conn = crate::engine::bare();
        let conn = &*conn;
        let refs = |sql: &str| reject_unknown_table_refs(conn, sql).unwrap().unwrap().0;
        let surveys = |sql: &str| reject_unknown_table_refs(conn, sql).unwrap().unwrap().1;

        assert!(
            refs("SELECT CAST('x' AS INTEGER)").is_empty(),
            "a constant expression reaches no table"
        );
        assert_eq!(
            refs(r#"SELECT * FROM "t__transfer""#),
            ["t__transfer".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
        );
        // Burrmill matches identifiers case-insensitively, so the sweep's lookup must too - otherwise a
        // shouted table name silently loses its reduction.
        assert_eq!(
            refs("SELECT * FROM T__TRANSFER"),
            ["t__transfer".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
        );
        assert_eq!(
            refs("SELECT (SELECT max(a) FROM u) FROM t"),
            ["t".to_string(), "u".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "a table reached from a subquery is still reached"
        );

        // #896: a statement that reaches a catalogue schema or calls an enumerating table function
        // (DuckDB's `duckdb_tables()` and kin) is asking *what tables exist*, so every view has to
        // be defined for it to answer. The bare-name set cannot express that -
        // `information_schema.tables` arrives as `tables` with the qualifier dropped, indistinguishable from a nest table of that name.
        assert!(!surveys(r#"SELECT * FROM "t__transfer""#));
        assert!(!surveys("SELECT (SELECT max(a) FROM u) FROM t"));
        assert!(
            surveys("SELECT table_name FROM information_schema.tables"),
            "a catalogue schema must be recognised through the dropped qualifier"
        );
        // The `duckdb_*` enumerating table functions are refused outright by `ALLOWED_TABLE_FNS`
        // (and Burrmill has none), so the `duckdb_` branch in the walk is unreachable today. It
        // stays because the failure it guards is silent: admit `duckdb_tables` to that allowlist
        // without thinking about #896 and the catalogue listing comes back empty rather than
        // erroring.
        assert!(
            reject_unknown_table_refs(conn, "SELECT * FROM duckdb_views()").is_err(),
            "an enumerating table function is refused before the survey question arises"
        );
    }

    /// **Issue #241 items 3 and 4.** On a cold nest an authored view must resolve to zero rows, not
    /// fail with `Table ... does not exist`.
    ///
    /// The reported symptom was every view failing at startup on a fresh nest, then being *absent*
    /// until a restart - so the documented first run ("`nuthatch dev`, then query") could not use
    /// views on the run that forms someone's impression of the tool.
    ///
    /// The mechanism is subtle and worth pinning: `define_views` already builds an empty **typed**
    /// view for a table with no sealed segments and no hot rows - but skips it when `cols` is empty,
    /// and `cols` comes from `schema.json`. A hand-written nest has no schema, so no columns, so **no
    /// view at all**, and one missing table cascade-fails the whole view file.
    ///
    /// That makes cold-start view resolution an *emergent* property of `schema.json` being present and
    /// `empty_view_ddl` being reached. This test exists because emergent properties stop being true
    /// quietly.
    #[test]
    fn an_authored_view_resolves_on_a_cold_nest_with_no_rows() {
        crate::engine::on_bare(an_authored_view_resolves_on_a_cold_nest_with_no_rows_on);
    }

    fn an_authored_view_resolves_on_a_cold_nest_with_no_rows_on(conn: &dyn Session) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"tok__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true},
                {"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]}]}"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/10-big.sql"),
            "CREATE VIEW big_transfers AS SELECT \"from\", value_dec FROM tok__transfer WHERE value_dec > 1000;",
        )
        .unwrap();

        let empty = HotRows::new();
        define_views(
            conn,
            dir.path(),
            &empty,
            u64::MAX,
            &Default::default(),
            &[],
            None,
        )
        .unwrap();
        define_nest_views(conn, dir.path(), None);

        // The base table exists as an empty typed view…
        let n: i64 = conn
            .one_value("SELECT count(*) FROM tok__transfer")
            .map(|v| v.as_i64().expect("an integer count"))
            .expect("a declared table with no rows must still resolve");
        assert_eq!(n, 0);

        // …and so does the authored view built on it, including the derived `_dec` column.
        let n: i64 = conn
            .one_value("SELECT count(*) FROM big_transfers")
            .map(|v| v.as_i64().expect("an integer count"))
            .expect("an authored view on an empty table must resolve to zero rows, not fail");
        assert_eq!(n, 0);
    }

    /// **The #896 narrowing has to reach the authored views too.** `define_views` stopped defining
    /// base tables a statement cannot reach; `define_nest_views` carried on redefining every authored
    /// view on every request, and on the pooled connection each one bound in full against the base
    /// views a previous request left behind. On the Lodestar nest that was 1.2 s and 32,000 file
    /// opens under a `SELECT 1`. The positive control is here on purpose: an absence assertion whose
    /// mechanism is missing passes for the wrong reason.
    #[test]
    fn a_view_the_statement_cannot_reach_is_not_redefined() {
        crate::engine::on_bare(a_view_the_statement_cannot_reach_is_not_redefined_on);
    }

    fn a_view_the_statement_cannot_reach_is_not_redefined_on(conn: &dyn Session) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"tok__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true}]}]}"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/10-a.sql"),
            "CREATE VIEW a AS SELECT \"from\" FROM tok__transfer;",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/20-b.sql"),
            "CREATE VIEW b AS SELECT \"from\" FROM tok__transfer;",
        )
        .unwrap();

        define_views(
            conn,
            dir.path(),
            &HotRows::new(),
            u64::MAX,
            &Default::default(),
            &[],
            None,
        )
        .unwrap();
        let wanted: std::collections::BTreeSet<String> =
            ["a".to_string(), "tok__transfer".to_string()]
                .into_iter()
                .collect();
        define_nest_views(conn, dir.path(), Some(&wanted));
        assert!(
            conn.has_relation("a"),
            "the view the statement reaches is defined"
        );
        assert!(
            !conn.has_relation("b"),
            "a view the statement cannot reach must not be bound - that bind, over every segment \
             behind every table it touches, was the fixed cost under every request"
        );

        // The positive control: `None` still defines everything, as the warm-restart callers rely on.
        define_nest_views(conn, dir.path(), None);
        assert!(
            conn.has_relation("b"),
            "with no reachability set every authored view is defined"
        );
    }

    /// A statement that reaches a view only through another view still answers. `reachable_tables`
    /// carries the intermediate view's name in its closure, so narrowing `define_nest_views` to that
    /// set defines the chain in file order - the one the statement names last, the one it builds on
    /// first. Defining only the named view would leave it unbound and this query failing.
    #[test]
    fn a_view_reached_through_another_view_is_still_defined() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"tok__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"from","sol_type":"address","storage":"address","indexed":true}]}]}"#,
        )
        .unwrap();
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"tok__transfer","from":"0xa","block_number":1,"tx_hash":"0xt","log_index":0}"#.to_string()],
            1,
            1,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/10-a.sql"),
            "CREATE VIEW a AS SELECT \"from\" FROM tok__transfer;",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/20-c.sql"),
            "CREATE VIEW c AS SELECT \"from\" FROM a;",
        )
        .unwrap();

        let rows = query(dir.path(), "SELECT \"from\" FROM c")
            .expect("a view on a view must still resolve under the narrowed definition");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["from"], "0xa");
    }

    /// The other half of the same mechanism: **without** a schema there are no columns, so the empty
    /// typed view is skipped and the authored view cannot resolve. Pinned so the dependency between
    /// `schema.json` and cold-start views is explicit rather than folklore - it is exactly why
    /// `refresh_stale_artifacts` regenerates a missing schema before anything reads it.
    #[test]
    fn without_a_schema_the_view_cannot_resolve_which_is_why_we_regenerate_it() {
        crate::engine::on_bare(
            without_a_schema_the_view_cannot_resolve_which_is_why_we_regenerate_it_on,
        );
    }

    fn without_a_schema_the_view_cannot_resolve_which_is_why_we_regenerate_it_on(
        conn: &dyn Session,
    ) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-big.sql"),
            "CREATE VIEW big_transfers AS SELECT * FROM tok__transfer;",
        )
        .unwrap();

        let empty = HotRows::new();
        define_views(
            conn,
            dir.path(),
            &empty,
            u64::MAX,
            &Default::default(),
            &[],
            None,
        )
        .unwrap();
        define_nest_views(conn, dir.path(), None);

        assert!(
            conn.one_value("SELECT count(*) FROM big_transfers")
                .is_err(),
            "with no schema.json there is no typed empty view, so the authored view cannot resolve - \
             this is the failure `refresh_stale_artifacts` prevents by regenerating the schema"
        );
    }

    /// #663. The reported shape, not a degenerate stand-in for it: one `CREATE VIEW` spans two
    /// declared tables, one populated and one that has genuinely never fired, and the view supplies
    /// fields from both. Collapsing this to one table/one field would make "loses every field" and
    /// "resolves correctly" look the same, which is exactly the fixture the issue warns against.
    ///
    /// `schema.json` on disk only knows the table that has always existed - `gns__grt_withdrawn` was
    /// declared later and the file was never regenerated against it. That is `define_views`'s only
    /// source of "what tables exist" before this fix; `declared` (the live registry schema `dev`
    /// already computes as `served`/`full_schema`) is the fix - a second, always-current source that
    /// doesn't depend on `schema.json` being fresh.
    #[test]
    fn a_view_joining_a_populated_and_a_never_fired_table_resolves_once_the_live_schema_is_supplied(
    ) {
        a_view_joining_a_populated_and_a_never_fired_table_resolves_once_the_live_schema_is_supplied_on(
            &crate::engine::bare,
        );
    }

    fn a_view_joining_a_populated_and_a_never_fired_table_resolves_once_the_live_schema_is_supplied_on(
        open: &dyn Fn() -> Box<dyn crate::engine::Session>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"gns__signal_minted","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"value","sol_type":"uint256","storage":"word32","indexed":false},
                {"name":"pool","sol_type":"bytes32","storage":"bytes32","indexed":true}]}]}"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/80-gns-network.sql"),
            "CREATE VIEW gns_network AS SELECT m.value AS minted_value, m.pool AS minted_pool, \
             w.value AS withdrawn_value, w.recipient AS withdrawn_recipient \
             FROM gns__signal_minted m LEFT JOIN gns__grt_withdrawn w ON true;",
        )
        .unwrap();

        let col = |name: &str, storage: &str, indexed: bool| crate::registry::ColumnSchema {
            name: name.into(),
            sol_type: String::new(),
            storage: storage.into(),
            indexed,
            components: Vec::new(),
        };
        let declared = vec![
            crate::registry::TableSchema {
                table: "gns__signal_minted".into(),
                alias: "gns".into(),
                kind: crate::registry::TableKind::Event,
                function: String::new(),
                selector: String::new(),
                event: "SignalMinted".into(),
                topic0: "0xaaaa".into(),
                columns: vec![
                    col("block_number", "u64", false),
                    col("value", "word32", false),
                    col("pool", "bytes32", true),
                ],
            },
            crate::registry::TableSchema {
                table: "gns__grt_withdrawn".into(),
                alias: "gns".into(),
                kind: crate::registry::TableKind::Event,
                function: String::new(),
                selector: String::new(),
                // The L1-migration event: really on the ABI, never emitted on this chain (#663's repro).
                event: "GRTWithdrawn".into(),
                topic0: "0xbbbb".into(),
                columns: vec![
                    col("block_number", "u64", false),
                    col("value", "word32", false),
                    col("recipient", "address", true),
                ],
            },
        ];

        let mut hot = HotRows::new();
        hot.insert(
            "gns__signal_minted".to_string(),
            vec![
                serde_json::json!({"block_number": 100, "log_index": 0, "value": "500", "pool": "0xpool"}),
            ],
        );

        // Before: `define_views` only knows `schema.json`, which doesn't have `gns__grt_withdrawn`.
        // It gets no view at all, and the single `CREATE VIEW gns_network` statement - which touches
        // both tables - fails to bind. Pinning the bug this issue reports, not just the fix.
        {
            let conn = open();
            define_views(
                &*conn,
                dir.path(),
                &hot,
                u64::MAX,
                &Default::default(),
                &[],
                None,
            )
            .unwrap();
            define_nest_views(&*conn, dir.path(), None);
            assert!(
                conn.collect("SELECT count(*) FROM gns_network", None)
                    .is_err(),
                "pin the bug: one never-fired table takes the whole view down, all four fields"
            );
        }

        // After: `declared` (the live registry schema) knows `gns__grt_withdrawn` even though
        // `schema.json` doesn't, so it gets an empty typed view and the join resolves - the fired
        // table's real data intact, the never-fired table's side NULL rather than absent.
        {
            let conn = open();
            define_views(
                &*conn,
                dir.path(),
                &hot,
                u64::MAX,
                &Default::default(),
                &declared,
                None,
            )
            .unwrap();
            define_nest_views(&*conn, dir.path(), None);
            let rows = conn
                .collect(
                    "SELECT minted_value, minted_pool, withdrawn_value, withdrawn_recipient \
                     FROM gns_network",
                    None,
                )
                .map_err(|e| anyhow::anyhow!("{e:?}"))
                .expect("the populated half of the join must resolve, not merely avoid erroring")
                .rows;
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(
                row["minted_value"], "500",
                "the fired table's real data survives the fix"
            );
            assert_eq!(row["minted_pool"], "0xpool");
            assert_eq!(
                row["withdrawn_value"],
                serde_json::Value::Null,
                "the never-fired table degrades to NULL on its side, not an error"
            );
            assert_eq!(row["withdrawn_recipient"], serde_json::Value::Null);
        }
    }

    /// Reviewer question on #723: is the reported failure reachable through the *real* constructor
    /// chain, or only through a hand-authored `TableSchema`/`schema.json` fixture like the test
    /// above? Built here with nothing hand-authored: a real `nuthatch.toml` through `Config::load`,
    /// a real `schema.json` through `project::refresh_stale_artifacts` (the same call `dev` makes on
    /// startup), a real `declared` through `registry::from_nest` + `indexer::full_schema` (the same
    /// two calls `dev` makes). The only manual step is the one `refresh_stale_artifacts` cannot take
    /// for an identity-keyed nest (see the skip and its comment at the `refresh_stale_artifacts` call
    /// site in `indexer.rs`): editing `nuthatch.toml` to add an event without re-running it, which is
    /// how a real nest's `schema.json` falls behind - a hand-edit, an out-of-band checkout, or a
    /// commit that added the event without regenerating.
    #[test]
    fn the_real_constructor_chain_reproduces_663_and_the_fix_resolves_it() {
        crate::engine::on_bare(
            the_real_constructor_chain_reproduces_663_and_the_fix_resolves_it_on,
        );
    }

    fn the_real_constructor_chain_reproduces_663_and_the_fix_resolves_it_on(conn: &dyn Session) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("abis")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("abis/tok.json"),
            r#"[{"type":"event","name":"Minted","anonymous":false,"inputs":[
                {"name":"pool","type":"bytes32","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]},
               {"type":"event","name":"Withdrawn","anonymous":false,"inputs":[
                {"name":"recipient","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();

        // Step 1: declare only `Minted` and generate `schema.json` for real, exactly as `init` does.
        std::fs::write(
            dir.path().join(crate::config::CONFIG_FILE),
            r#"
[nest]
name = "tok"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]

[[contracts]]
alias = "tok"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/tok.json"
events = ["Minted"]
"#,
        )
        .unwrap();
        let cfg = crate::config::Config::load(dir.path()).unwrap();
        crate::project::refresh_stale_artifacts(dir.path(), &cfg).unwrap();
        let on_disk = std::fs::read_to_string(dir.path().join("schema.json")).unwrap();
        assert!(
            !on_disk.contains("tok__withdrawn"),
            "schema.json must only know Minted at this point"
        );

        // Step 2: hand-edit `nuthatch.toml` to declare `Withdrawn` too - a real event this contract
        // really emits, just not yet on this chain - and do NOT regenerate. This is the identity-keyed
        // nest's shape: `refresh_stale_artifacts` deliberately never runs for one (indexer.rs), so a
        // config edit like this is the concrete way `schema.json` falls behind in production.
        std::fs::write(
            dir.path().join(crate::config::CONFIG_FILE),
            r#"
[nest]
name = "tok"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]

[[contracts]]
alias = "tok"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/tok.json"
events = ["Minted", "Withdrawn"]
"#,
        )
        .unwrap();
        let cfg = crate::config::Config::load(dir.path()).unwrap();
        let still_on_disk = std::fs::read_to_string(dir.path().join("schema.json")).unwrap();
        assert!(
            !still_on_disk.contains("tok__withdrawn"),
            "schema.json was not touched by the edit - it is genuinely stale, not simulated"
        );

        // Step 3: the two calls `dev` makes at startup to get `declared` - real registry, real
        // full_schema, no hand-built `TableSchema`.
        let registry = crate::registry::from_nest(dir.path(), &cfg).unwrap();
        let declared = crate::indexer::full_schema(&registry, &cfg);
        assert!(
            declared.iter().any(|t| t.table == "tok__withdrawn"),
            "the live registry must know about Withdrawn even though schema.json does not"
        );

        std::fs::write(
            dir.path().join("views/80-tok-network.sql"),
            "CREATE VIEW tok_network AS SELECT m.pool AS minted_pool, m.value AS minted_value, \
             w.recipient AS withdrawn_recipient, w.value AS withdrawn_value \
             FROM tok__minted m LEFT JOIN tok__withdrawn w ON true;",
        )
        .unwrap();

        let mut hot = HotRows::new();
        hot.insert(
            "tok__minted".to_string(),
            vec![serde_json::json!({"block_number": 1, "log_index": 0, "pool": "0xpool", "value": "42"})],
        );

        // Before the fix this view fails to bind at all (pinned with a hand-built fixture above);
        // here, with everything built through the real chain, it must resolve.
        define_views(
            conn,
            dir.path(),
            &hot,
            u64::MAX,
            &Default::default(),
            &declared,
            None,
        )
        .unwrap();
        define_nest_views(conn, dir.path(), None);
        let rows = conn
            .collect(
                "SELECT minted_pool, minted_value, withdrawn_recipient, withdrawn_value FROM tok_network",
                None,
            )
            .map_err(|e| anyhow::anyhow!("{e:?}"))
            .expect("real constructor chain: the view must resolve, not just the hand-built one")
            .rows;
        let row = &rows[0];
        assert_eq!(row["minted_pool"], "0xpool");
        assert_eq!(row["minted_value"], "42");
        assert_eq!(row["withdrawn_recipient"], Value::Null);
        assert_eq!(row["withdrawn_value"], Value::Null);
    }

    /// #729's counterpart to the #663 test above: the table itself is never missing, only its *column
    /// set* falls behind. Built the same way, with nothing hand-authored - a real `nuthatch.toml`
    /// through `Config::load`, a real `schema.json` through `project::refresh_stale_artifacts`, a real
    /// sealed segment through `seal::seal_range`, and a real `declared` through `registry::from_nest` +
    /// `indexer::full_schema` reading the ABI file *after* it changes. No config edit is needed at all
    /// here (unlike #663's added-event case): `events = ["Transfer"]` never changes, only the ABI's
    /// field list for that same event - which is exactly how a re-fetched ABI drifts from an
    /// already-generated `schema.json` in production.
    #[test]
    fn the_real_constructor_chain_reproduces_729_and_the_fix_resolves_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("abis")).unwrap();
        std::fs::write(
            dir.path().join("abis/tok.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join(crate::config::CONFIG_FILE),
            r#"
[nest]
name = "tok"
chain = "mainnet"
chain_id = 1
rpc_urls = ["https://rpc.example"]

[[contracts]]
alias = "tok"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/tok.json"
events = ["Transfer"]
"#,
        )
        .unwrap();

        // Step 1: generate `schema.json` for real, exactly as `init` does, against the pre-refetch ABI.
        let cfg = crate::config::Config::load(dir.path()).unwrap();
        crate::project::refresh_stale_artifacts(dir.path(), &cfg).unwrap();
        let on_disk = std::fs::read_to_string(dir.path().join("schema.json")).unwrap();
        assert!(
            !on_disk.contains("memo"),
            "schema.json must only know the pre-refetch ABI at this point"
        );

        // One sealed segment, written under the ABI as it stood when schema.json was generated - it
        // genuinely has no `memo` column, the same way a real segment sealed before an ABI bump can't.
        crate::seal::seal_range(
            dir.path(),
            &[r#"{"table":"tok__transfer","from":"0xa","to":"0xb","value":"9","block_number":1,"tx_hash":"0xt","log_index":0}"#.to_string()],
            1,
            1,
        )
        .unwrap();

        // Step 2: the ABI is re-fetched (Sourcify/Etherscan-class, per CLAUDE.md) and now carries `memo`
        // on the same `Transfer` event - a real ABI change, not a hand-built fixture. `schema.json` is
        // not regenerated, which is the concrete way it falls behind in production: nobody re-ran
        // `nuthatch schema` (or restarted `dev`) the moment the ABI changed.
        std::fs::write(
            dir.path().join("abis/tok.json"),
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false},
                {"name":"memo","type":"string","indexed":false}]}]"#,
        )
        .unwrap();
        let still_on_disk = std::fs::read_to_string(dir.path().join("schema.json")).unwrap();
        assert!(
            !still_on_disk.contains("memo"),
            "schema.json was not touched by the ABI re-fetch - it is genuinely stale, not simulated"
        );

        // Step 3: the two calls `dev` makes at startup to get `declared` - real registry, real
        // full_schema, no hand-built `TableSchema`.
        let registry = crate::registry::from_nest(dir.path(), &cfg).unwrap();
        let declared = crate::indexer::full_schema(&registry, &cfg);
        assert!(
            declared
                .iter()
                .find(|t| t.table == "tok__transfer")
                .expect("tok__transfer must still be declared")
                .columns
                .iter()
                .any(|c| c.name == "memo"),
            "the live registry must know about `memo` even though schema.json does not"
        );

        let guard = QueryGuard {
            timeout: Duration::from_secs(5),
            max_rows: 1000,
        };
        // Before the fix this is a binder error ("Referenced column memo not found"); with everything
        // built through the real chain, it must resolve, and `memo` must read back NULL for the segment
        // that predates it.
        let out = query_hot_cold(
            dir.path(),
            r#"SELECT value, memo FROM "tok__transfer""#,
            guard,
            &HotRows::new(),
            u64::MAX,
            &declared,
        )
        .expect("real constructor chain: the new column must resolve, not error");
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0]["value"], Value::from("9"));
        assert_eq!(out.rows[0]["memo"], Value::Null);
    }

    /// `declared_but_never_sealed` is what turns the empty-view fix above into a log line an operator
    /// can read - #663's other half ("the logs must explain it"). Sealed, not just declared, is the
    /// bar: a table only in `hot` (fired moments ago, not yet sealed) still counts as "never sealed"
    /// here, which is a documented, self-correcting approximation (see the function's own doc comment).
    #[test]
    fn declared_but_never_sealed_names_only_the_table_with_no_sealed_segment() {
        let dir = tempfile::tempdir().unwrap();
        let declared = vec![
            crate::registry::TableSchema {
                table: "gns__signal_minted".into(),
                alias: "gns".into(),
                kind: crate::registry::TableKind::Event,
                function: String::new(),
                selector: String::new(),
                event: "SignalMinted".into(),
                topic0: "0xaaaa".into(),
                columns: vec![],
            },
            crate::registry::TableSchema {
                table: "gns__grt_withdrawn".into(),
                alias: "gns".into(),
                kind: crate::registry::TableKind::Event,
                function: String::new(),
                selector: String::new(),
                event: "GRTWithdrawn".into(),
                topic0: "0xbbbb".into(),
                columns: vec![],
            },
        ];
        // **No manifest at all: nothing is claimed** (#1042, changed from the opposite assertion).
        //
        // This used to assert that both tables read as never-sealed, which is literally true of the
        // manifest and false about the chain - and the caller turns it into "the event has likely
        // never fired on this chain". On a cold start that fires for every table, seconds before
        // they all populate. A fresh operator hit it on both a USDC and a stETH nest.
        //
        // An empty manifest carries no information about any individual table, so the honest answer
        // is silence. The old assertion was pinning the defect.
        assert!(
            declared_but_never_sealed(dir.path(), &declared).is_empty(),
            "with nothing sealed the nest has not looked yet, and must claim nothing about any \
             declared event"
        );

        // A real sealed segment for `gns__signal_minted` only - `gns__grt_withdrawn` still has none.
        let entities = vec![
            r#"{"table":"gns__signal_minted","block_number":1,"log_index":0,"value":"500"}"#
                .to_string(),
        ];
        crate::seal::seal_range(dir.path(), &entities, 1, 1).unwrap();
        assert_eq!(
            declared_but_never_sealed(dir.path(), &declared),
            vec!["gns__grt_withdrawn".to_string()],
            "the table with a sealed segment drops off the list; the genuinely never-fired one \
             remains - this is #663's case and it must keep working, or suppressing the cold-start \
             noise would have cost the signal it was added for"
        );
    }

    /// **Issue #241 item 4.** One view referencing a table that has never fired must not take down the
    /// *other* views in the same file.
    ///
    /// The reported case: `TaskCancelled` and a deployed-but-unused voting module. Both views were
    /// correct, just premature, and the workaround was commenting them out with a note to uncomment
    /// when the event fires - which is a poor trade for fault isolation nobody needed at file
    /// granularity.
    #[test]
    fn one_premature_view_does_not_kill_the_others_in_its_file() {
        crate::engine::on_bare(one_premature_view_does_not_kill_the_others_in_its_file_on);
    }

    fn one_premature_view_does_not_kill_the_others_in_its_file_on(conn: &dyn Session) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"tok__transfer","columns":[
                {"name":"block_number","sol_type":"implicit","storage":"u64","indexed":false},
                {"name":"value","sol_type":"uint256","storage":"word32","indexed":false}]}]}"#,
        )
        .unwrap();
        // Statement 2 references a table this nest never declares. Statements 1 and 3 are fine.
        std::fs::write(
            dir.path().join("views/10-mixed.sql"),
            "CREATE VIEW ok_one AS SELECT block_number FROM tok__transfer;\n\
             CREATE VIEW premature AS SELECT * FROM task__cancelled;\n\
             CREATE VIEW ok_two AS SELECT value_dec FROM tok__transfer;",
        )
        .unwrap();

        let empty = HotRows::new();
        define_views(
            conn,
            dir.path(),
            &empty,
            u64::MAX,
            &Default::default(),
            &[],
            None,
        )
        .unwrap();
        define_nest_views(conn, dir.path(), None);

        for v in ["ok_one", "ok_two"] {
            conn.one_value(&format!("SELECT count(*) FROM {v}"))
                .unwrap_or_else(|e| {
                    panic!("{v} must exist despite a sibling statement failing: {e}")
                });
        }
        assert!(
            conn.one_value("SELECT count(*) FROM premature").is_err(),
            "the genuinely-unresolvable view is still absent, which is correct"
        );
    }

    /// And the gate reports **every** unresolved table at once, rather than sending the author round
    /// a fix-restart-next-error loop.
    #[test]
    fn validation_names_all_the_missing_tables_not_just_the_first() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-three.sql"),
            "CREATE VIEW a AS SELECT * FROM alpha__one;\n\
             CREATE VIEW b AS SELECT * FROM beta__two;\n\
             CREATE VIEW c AS SELECT * FROM gamma__three;",
        )
        .unwrap();

        let issues = validate_nest_views(dir.path(), &[]);
        assert_eq!(issues.len(), 1, "one file, one issue");
        let e = &issues[0].error;
        for t in ["alpha__one", "beta__two", "gamma__three"] {
            assert!(e.contains(t), "every unresolved table must be named: {e}");
        }
        // …and as a *summary*, not three concatenated catalog errors. The author needs the work item
        // ("these three tables are missing"), not three copies of the engine explaining a catalog.
        // Asserted explicitly because a plain join of the errors also happens to contain all three
        // names - so without this the summary formatting was untested and a mutation of it survived.
        assert!(
            e.contains("unresolved tables:"),
            "the message must summarise, not concatenate: {e}"
        );
        assert!(
            e.contains("3 statement(s) failed"),
            "it must say how many statements failed: {e}"
        );
    }

    /// The splitter must not break on a `;` inside a string or a quoted identifier - splitting there
    /// would mangle correct SQL into two invalid halves, turning a working view into a failure.
    #[test]
    fn semicolons_inside_literals_do_not_split_a_statement() {
        let one = split_sql_statements("CREATE VIEW v AS SELECT 'a;b' AS x;");
        assert_eq!(one.len(), 1, "a quoted `;` is not a separator: {one:?}");

        let ident = split_sql_statements("CREATE VIEW \"odd;name\" AS SELECT 1;");
        assert_eq!(
            ident.len(),
            1,
            "a quoted identifier is not a separator: {ident:?}"
        );

        let commented = split_sql_statements("SELECT 1; -- trailing ; in a comment\nSELECT 2;");
        assert_eq!(
            commented.len(),
            2,
            "a `;` in a comment is not a separator: {commented:?}"
        );

        let two = split_sql_statements("SELECT 1;\nSELECT 2;");
        assert_eq!(two.len(), 2);
    }

    /// **A quoted function name evaded the `/sql` denylist and read arbitrary files.**
    ///
    /// Found in the pre-1.0 adversary pass. `reject_file_access` matched a forbidden name only when the
    /// next non-space character was `(` - and DuckDB accepted a *quoted* function name, where the next
    /// character is `"`. So `SELECT * FROM "read_csv"('/etc/passwd')` passed both guards and DuckDB
    /// executed it, confirmed against a live connection (it returned the contents of `/etc/hosts`).
    ///
    /// Same class as the stacked-`COPY TO` arbitrary *write* found earlier (#153): the guard was
    /// correct about the shape it imagined and the shape had another spelling.
    ///
    /// The cases below are spellings of one idea - break the name away from its parens, or from
    /// itself - and each must stay refused.
    #[test]
    fn a_quoted_function_name_cannot_evade_the_denylist() {
        for q in [
            "SELECT * FROM read_csv('/etc/passwd')",
            r#"SELECT * FROM "read_csv"('/etc/passwd')"#,
            r#"SELECT * FROM "READ_CSV"('/etc/passwd')"#,
            // Quoting a *fragment* of the name is the same trick with a smaller hammer.
            r#"SELECT * FROM read"_"csv('/etc/passwd')"#,
            "SELECT * FROM main.read_csv('/etc/passwd')",
            "SELECT * FROM read_csv\n('/etc/passwd')",
            "SELECT * FROM READ_CSV('/etc/passwd')",
            // The other file-reaching functions deserve the same treatment.
            r#"SELECT * FROM "read_parquet"('/etc/passwd')"#,
            r#"SELECT * FROM "read_json_auto"('/etc/passwd')"#,
        ] {
            assert!(
                reject_file_access(q).is_err() || reject_replacement_scan(q).is_err(),
                "must be refused: {q}"
            );
        }
    }

    /// The fix must not refuse legitimate queries. Stripping quotes before the scan can only make the
    /// denylist match more, so the risk is false positives - pinned here so a later "tidy-up" that
    /// widens it further has to break a test rather than a user's dashboard.
    #[test]
    fn ordinary_quoted_identifiers_still_work() {
        for q in [
            // Reserved-word columns are quoted constantly in this product - `from`/`to` on transfers.
            r#"SELECT "from", "to" FROM usdc__transfer"#,
            r#"SELECT count(*) FROM "usdc__transfer""#,
            // A column whose name merely contains a forbidden name is not a call.
            r#"SELECT my_read_csv_flag FROM t"#,
        ] {
            assert!(reject_file_access(q).is_ok(), "must be allowed: {q}");
            assert!(reject_replacement_scan(q).is_ok(), "must be allowed: {q}");
        }
    }

    /// **Audit finding 5: the allowlist must refuse what the denylist has never heard of.**
    ///
    /// The denylist enumerates forbidden names over a vocabulary an engine grows every release, and has
    /// been wrong twice - about spelling and about coverage. This asks the parser what the query
    /// references and permits only what we recognise, so a file-reading function added upstream
    /// tomorrow is refused *by default*.
    ///
    /// The cases below are deliberately ones the denylist does **not** list: if this test passes, the
    /// allowlist is carrying weight of its own rather than shadowing the older control.
    #[test]
    fn the_allowlist_refuses_functions_the_denylist_never_heard_of() {
        for (conn, q) in [crate::engine::bare()].iter().flat_map(|c| {
            [
                // Not in FORBIDDEN_FNS - inert today only because the extension is not bundled.
                "SELECT * FROM read_xlsx('/etc/passwd')",
                "SELECT * FROM st_read('/etc/passwd')",
                "SELECT * FROM iceberg_scan('/tmp')",
                "SELECT * FROM postgres_scan('host=x','public','t')",
                // A plausible future name nobody has listed anywhere.
                "SELECT * FROM read_totally_new_format('/etc/passwd')",
                // And the ones it does list, by every spelling.
                "SELECT * FROM read_csv('/etc/passwd')",
                r#"SELECT * FROM "read_csv"('/etc/passwd')"#,
            ]
            .map(|q| (c, q))
        }) {
            assert!(
                reject_unknown_table_refs(conn.as_ref(), q).is_err(),
                "the allowlist must refuse on {}: {q}",
                conn.engine_version()
            );
        }
    }

    /// A replacement scan parses as a `BASE_TABLE` whose name is the path - the AST alone does not
    /// distinguish it from a real table, so the name has to be checked.
    #[test]
    fn a_path_in_table_position_is_not_a_table_name() {
        for (conn, q) in [crate::engine::bare()].iter().flat_map(|c| {
            [
                "SELECT * FROM '/etc/passwd'",
                "SELECT * FROM '/x.parquet'",
                "SELECT * FROM 'https://evil.example/x.parquet'",
            ]
            .map(|q| (c, q))
        }) {
            assert!(
                reject_unknown_table_refs(conn.as_ref(), q).is_err(),
                "a path in table position must be refused on {}: {q}",
                conn.engine_version()
            );
        }
    }

    /// And it must not break ordinary analytical SQL - the risk of an allowlist is false refusals,
    /// which is a broken dashboard rather than a breach, but still a bug.
    #[test]
    fn ordinary_analytical_sql_still_passes_the_allowlist() {
        for (conn, q) in [crate::engine::bare()].iter().flat_map(|c| {
            [
                "SELECT * FROM usdc__transfer",
                r#"SELECT "from", "to", value_dec FROM usdc__transfer WHERE value_dec > 100"#,
                "WITH t AS (SELECT * FROM usdc__transfer) SELECT count(*) FROM t",
                "SELECT a.block_number FROM usdc__transfer a JOIN weth__transfer b USING (tx_hash)",
                // Row-generating functions analytics legitimately uses.
                "SELECT * FROM generate_series(1, 10)",
                "SELECT * FROM range(10)",
                // Inline VALUES references no table at all.
                "SELECT * FROM (VALUES (1),(2)) t(x)",
                "SELECT count(*) FROM usdc__transfer GROUP BY \"from\" ORDER BY 1 DESC LIMIT 5",
            ]
            .map(|q| (c, q))
        }) {
            assert!(
                reject_unknown_table_refs(conn.as_ref(), q).is_ok(),
                "legitimate query must be allowed on {}: {q}",
                conn.engine_version()
            );
        }
    }

    /// **The allowlist must be wired into the real query path, not merely exist.**
    ///
    /// Written because a mutation exposed the gap: deleting `reject_unknown_table_refs` from `run()`
    /// broke *no test*, since the three tests above call it directly. A control that is unit-tested and
    /// unreachable is the same failure as `reconcile::tick` having six passing tests and no caller, and
    /// as the writer pool holding leases with no indexing code behind them.
    ///
    /// The probe must be a name `FORBIDDEN_FNS` genuinely does **not** list, or the denylist answers
    /// first and this proves nothing about the allowlist. `read_xlsx` was the original probe and
    /// stopped being valid the moment audit finding 4 added it to the denylist - this test caught its
    /// own obsolescence, which is the behaviour worth having.
    #[test]
    fn the_allowlist_is_reachable_from_the_public_query_path() {
        let dir = tempfile::tempdir().unwrap();
        let err = query(
            dir.path(),
            "SELECT * FROM read_some_future_format('/etc/passwd')",
        )
        .expect_err("a function the denylist does not list must still be refused");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("not permitted") || msg.contains("tables and views only"),
            "the refusal must come from the allowlist, not from the engine failing later: {msg}"
        );

        // And the guarded surface, which is the one actually exposed over HTTP.
        let err = query_guarded(
            dir.path(),
            "SELECT * FROM read_some_future_format('/etc/passwd')",
            QueryGuard {
                timeout: Duration::from_secs(5),
                max_rows: 100,
            },
        )
        .expect_err("the guarded surface must refuse it too");
        assert!(format!("{err:#}").contains("not permitted"));
    }
}

#[cfg(all(test, feature = "folds"))]
mod fold_connections {
    use super::*;

    /// Neither fold session reads a file the nest did not bind. Their memory and threads are the
    /// budget every Burrmill session is opened with, which leaves nothing here to compare.
    #[test]
    fn fold_connections_read_nothing_outside_the_nest() {
        let dir = tempfile::tempdir().unwrap();
        let binder = FoldBinder::open(dir.path()).unwrap();
        let eval = FoldEvaluator::new(dir.path(), &[]).unwrap();
        for (what, session) in [("binder", &*binder.session), ("evaluator", &*eval.session)] {
            for sql in [
                "SELECT * FROM read_text('/etc/hosts')",
                "SELECT * FROM read_csv_auto('/etc/hosts')",
                "SELECT * FROM '/etc/hosts'",
            ] {
                assert!(session.collect(sql, Some(1)).is_err(), "{what} ran {sql}");
            }
        }
    }

    #[test]
    fn the_binder_defines_only_what_it_is_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"t","columns":[{"name":"block_number","storage":"u64"}]},{"table":"u","columns":[{"name":"block_number","storage":"u64"}]}]}"#,
        )
        .unwrap();
        let binder = FoldBinder::open(dir.path()).unwrap();
        binder
            .bind(dir.path(), &[], &["t".to_string()].into_iter().collect())
            .unwrap();
        assert_eq!(
            binder.relations().unwrap(),
            std::collections::BTreeSet::from(["t".to_string()])
        );
    }
}

#[cfg(all(test, feature = "folds"))]
mod schema_only_binding {
    use super::*;
    use serde_json::json;

    /// Five segments in three shapes: `extra` appears from the third, and `v` is a number in the first
    /// two and text after.
    fn nest() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        let rows = |b: u64| -> Vec<String> {
            let row = match b {
                1 | 2 => json!({"table": "t", "block_number": b, "k": "a", "v": b}),
                3 | 4 => {
                    json!({"table": "t", "block_number": b, "k": "a", "v": b.to_string(), "extra": "x"})
                }
                _ => {
                    json!({"table": "t", "block_number": b, "k": "a", "v": b.to_string(), "extra": "x", "more": true})
                }
            };
            vec![row.to_string()]
        };
        for b in 1..=5 {
            crate::seal::seal_range(dir.path(), &rows(b), b, b).unwrap();
        }
        dir
    }

    fn files(dir: &Path) -> Vec<(PathBuf, u64)> {
        let manifest = crate::seal::load_manifest_with_hash(dir).unwrap().0;
        manifest.tables["t"]
            .iter()
            .map(|s| (crate::seal::segment_path(dir, &s.file, &s.hash), 0))
            .collect()
    }

    #[test]
    fn one_file_per_schema_is_kept_in_order() {
        let dir = nest();
        let conn = crate::engine::bare();
        let all = files(dir.path());
        let kept = one_per_file_schema(&*conn, all.clone());
        assert_eq!(kept.len(), 3, "{kept:?}");
        let pos = |f: &(PathBuf, u64)| all.iter().position(|a| a == f).unwrap();
        assert!(kept.windows(2).all(|w| pos(&w[0]) < pos(&w[1])));
    }

    /// The fold binder's schema-only views describe exactly as views over every file would.
    #[test]
    fn a_schema_only_binding_describes_like_the_whole_union() {
        let dir = nest();
        let wanted: std::collections::BTreeSet<String> = ["t".to_string()].into();
        let describe = |schema_only: bool| {
            let conn = crate::engine::bare();
            define_views_bound(
                &*conn,
                dir.path(),
                &HotRows::new(),
                u64::MAX,
                &Default::default(),
                &[],
                Some(&wanted),
                FactWindow::default(),
                schema_only,
            )
            .unwrap();
            conn.describe("SELECT * FROM t").unwrap()
        };
        let whole = describe(false);
        assert!(whole.iter().any(|(c, _)| c == "more"), "{whole:?}");
        assert_eq!(describe(true), whole);
    }
}

#[cfg(test)]
mod maintained_views {
    //! RFC-0062 S1: every answer read from a copy is compared byte for byte with the request-time
    //! view over the same inputs, on a nest that declares nothing.
    use super::*;
    use serde_json::json;
    use std::sync::atomic::Ordering::Relaxed;

    const VIEWS: &str = "\
CREATE VIEW bet AS
SELECT p.\"id\" AS id, p.\"player\" AS player, p.\"amount\" AS amount, r.\"payout\" AS payout,
       p.block_number AS placed_block
FROM \"bs__placed\" p LEFT JOIN \"bs__resolved\" r ON r.\"id\" = p.\"id\";
CREATE VIEW player_total AS
SELECT player, count(*) AS bets, sum(CAST(amount AS BIGINT)) AS staked,
       sum(CAST(payout AS BIGINT)) AS paid
FROM bet GROUP BY player;
";

    fn placed(id: u64, player: &str, amount: u64, block: u64) -> Value {
        json!({"table": "bs__placed", "id": id.to_string(), "player": player,
               "amount": amount.to_string(), "block_number": block,
               "tx_hash": format!("0x{id:x}"), "log_index": id})
    }

    fn resolved(id: u64, payout: u64, block: u64) -> Value {
        json!({"table": "bs__resolved", "id": id.to_string(), "payout": payout.to_string(),
               "block_number": block, "tx_hash": format!("0x{id:x}"), "log_index": 100 + id})
    }

    fn seal(dir: &Path, rows: &[Value], from: u64, to: u64) {
        let rows: Vec<String> = rows.iter().map(Value::to_string).collect();
        crate::seal::seal_range(dir, &rows, from, to).unwrap();
    }

    /// Three bets sealed through block 10, two of them resolved.
    fn nest(declare: Option<&str>) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(dir.path().join("views/10-bets.sql"), VIEWS).unwrap();
        seal(
            dir.path(),
            &[
                placed(1, "alice", 5, 3),
                placed(2, "bob", 7, 4),
                placed(3, "alice", 11, 9),
                resolved(1, 10, 5),
                resolved(2, 0, 6),
            ],
            1,
            10,
        );
        if let Some(toml) = declare {
            std::fs::write(dir.path().join(crate::maintained::DECLARATION_FILE), toml).unwrap();
        }
        dir
    }

    const BOTH: &str = "[[view]]\nname = \"bet\"\n\n[[view]]\nname = \"player_total\"\n";

    /// Load the declaration and give the nest a builder over a gate of `permits`.
    fn held(dir: &Path, permits: usize) -> Arc<tokio::sync::Semaphore> {
        crate::maintained::load(dir).unwrap();
        let gate = Arc::new(tokio::sync::Semaphore::new(permits));
        crate::maintained::attach(dir, gate.clone());
        gate
    }

    fn tip(rows: &[Value]) -> HotRows {
        let mut hot = HotRows::new();
        for r in rows {
            hot.entry(r["table"].as_str().unwrap().to_string())
                .or_default()
                .push(r.clone());
        }
        hot
    }

    fn guard() -> QueryGuard {
        QueryGuard {
            timeout: Duration::from_secs(60),
            max_rows: 10_000,
        }
    }

    /// The answer's bytes: its columns in order, then its rows.
    fn ask(dir: &Path, sql: &str, hot: &HotRows, sealed_through: u64) -> Vec<u8> {
        let out = query_hot_cold(dir, sql, guard(), hot, sealed_through, &[]).unwrap();
        assert!(!out.degraded(), "{sql}: {:?}", out.degraded_tables);
        serde_json::to_vec(&(out.columns, out.rows)).unwrap()
    }

    /// The same statement over a copy of the nest that declares nothing and holds no copies.
    fn oracle(dir: &Path, sql: &str, hot: &HotRows, sealed_through: u64) -> Vec<u8> {
        let plain = tempfile::tempdir().unwrap();
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let name = entry.file_name();
            if name == crate::maintained::COPIES_DIR || name == crate::maintained::DECLARATION_FILE
            {
                continue;
            }
            let to = plain.path().join(&name);
            if entry.file_type().unwrap().is_dir() {
                crate::project::copy_dir(&entry.path(), &to).unwrap();
            } else {
                std::fs::copy(entry.path(), &to).unwrap();
            }
        }
        ask(plain.path(), sql, hot, sealed_through)
    }

    fn counts(dir: &Path, view: &str) -> (u64, u64, u64) {
        let (_, s, _, _) = crate::maintained::report(dir)
            .into_iter()
            .find(|(v, ..)| v == view)
            .expect("a declared view");
        (
            s.hits.load(Relaxed),
            s.fallbacks.load(Relaxed),
            s.builds.load(Relaxed),
        )
    }

    fn copies(dir: &Path, view: &str) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> =
            std::fs::read_dir(dir.join(crate::maintained::COPIES_DIR).join(view))
                .map(|rd| rd.flatten().map(|e| e.path()).collect())
                .unwrap_or_default();
        out.sort();
        out
    }

    const SHAPES: &[&str] = &[
        "SELECT * FROM bet ORDER BY id",
        "SELECT id, payout FROM bet WHERE payout IS NULL ORDER BY id",
        "SELECT * FROM player_total ORDER BY player",
        "SELECT b.id, t.staked FROM bet b JOIN player_total t ON t.player = b.player ORDER BY b.id",
        "SELECT count(*) AS n FROM bet",
        "SELECT player, staked + 1 AS s1, staked * 2 AS s2, CAST(staked AS VARCHAR) AS s3, \
         staked / 3 AS s4, paid - staked AS s5, typeof(staked) AS t FROM player_total ORDER BY player",
    ];

    /// The first request answers from the definition and queues the builds; every later one reads
    /// the copies, and each answer is the request-time view's to the byte.
    #[test]
    fn a_maintained_view_answers_from_its_copy_byte_for_byte() {
        let dir = nest(Some(BOTH));
        held(dir.path(), 2);
        let hot = tip(&[placed(4, "carol", 13, 20), resolved(3, 22, 21)]);
        for sql in SHAPES {
            let want = oracle(dir.path(), sql, &hot, 10);
            assert_eq!(
                ask(dir.path(), sql, &hot, 10),
                want,
                "before any copy: {sql}"
            );
        }
        crate::maintained::wait_idle(dir.path());
        let errors: Vec<_> = crate::maintained::report(dir.path())
            .into_iter()
            .map(|(v, s, ..)| (v, s.last_error()))
            .collect();
        assert_eq!(copies(dir.path(), "bet").len(), 1, "{errors:?}");
        assert_eq!(copies(dir.path(), "player_total").len(), 1, "{errors:?}");
        let before = (
            counts(dir.path(), "bet"),
            counts(dir.path(), "player_total"),
        );
        for sql in SHAPES {
            let want = oracle(dir.path(), sql, &hot, 10);
            assert_eq!(
                ask(dir.path(), sql, &hot, 10),
                want,
                "from the copies: {sql}"
            );
        }
        let (bet, total) = (
            counts(dir.path(), "bet"),
            counts(dir.path(), "player_total"),
        );
        assert!(bet.0 > before.0 .0, "bet answered from its copy: {bet:?}");
        assert!(
            total.0 > before.1 .0,
            "player_total answered from its copy: {total:?}"
        );
        assert_eq!(
            (bet.1, total.1),
            (before.0 .1, before.1 .1),
            "no fallback once built"
        );
        assert_eq!((bet.2, total.2), (1, 1), "one build each");
    }

    /// A copy built at one set of hot rows is not served at another: the request answers from the
    /// definition, the old copy stays where it was, and the new state gets its own.
    #[test]
    fn a_copy_is_never_served_for_other_hot_rows() {
        let dir = nest(Some(BOTH));
        held(dir.path(), 2);
        let sql = SHAPES[0];
        let first = tip(&[placed(4, "carol", 13, 20)]);
        ask(dir.path(), sql, &first, 10);
        crate::maintained::wait_idle(dir.path());
        let old = ask(dir.path(), sql, &first, 10);
        let built = copies(dir.path(), "bet");
        assert_eq!(built.len(), 1);

        let second = tip(&[placed(4, "carol", 13, 20), resolved(4, 26, 21)]);
        let want = oracle(dir.path(), sql, &second, 10);
        assert_ne!(old, want, "the fixture must change the answer");
        let (hits, fallbacks, _) = counts(dir.path(), "bet");
        assert_eq!(ask(dir.path(), sql, &second, 10), want);
        assert_eq!(
            counts(dir.path(), "bet").0,
            hits,
            "not served from the old copy"
        );
        assert_eq!(counts(dir.path(), "bet").1, fallbacks + 1);
        assert!(built[0].exists(), "the old copy is untouched");
    }

    /// Another binary never reads this one's copies: an upgrade answers from the definition and
    /// builds its own.
    #[test]
    fn an_upgrade_does_not_read_the_old_binarys_copies() {
        let dir = nest(Some(BOTH));
        held(dir.path(), 2);
        let sql = SHAPES[0];
        let hot = tip(&[placed(4, "carol", 13, 20)]);
        crate::maintained::test_set_binary(dir.path(), "nuthatch 4.13.0 aaaa");
        ask(dir.path(), sql, &hot, 10);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(
            ask(dir.path(), sql, &hot, 10),
            oracle(dir.path(), sql, &hot, 10)
        );
        let (hits, fallbacks, builds) = counts(dir.path(), "bet");
        assert!(hits > 0, "the copy serves its own binary");

        crate::maintained::test_set_binary(dir.path(), "nuthatch 4.14.0 bbbb");
        assert_eq!(
            ask(dir.path(), sql, &hot, 10),
            oracle(dir.path(), sql, &hot, 10)
        );
        assert_eq!(
            counts(dir.path(), "bet").0,
            hits,
            "not served across an upgrade"
        );
        assert_eq!(counts(dir.path(), "bet").1, fallbacks + 1);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(
            counts(dir.path(), "bet").2,
            builds + 1,
            "the new binary builds its own"
        );
        assert_eq!(copies(dir.path(), "bet").len(), 2);
    }

    /// A seal that admits a segment of a table the view reads changes its identity; one of a table
    /// it does not read leaves the copy current.
    #[test]
    fn a_seal_moves_the_identity_only_through_the_closure() {
        let dir = nest(Some("[[view]]\nname = \"bet\"\n"));
        held(dir.path(), 2);
        let sql = SHAPES[0];
        let hot = tip(&[placed(4, "carol", 13, 20)]);
        ask(dir.path(), sql, &hot, 10);
        crate::maintained::wait_idle(dir.path());
        let old = ask(dir.path(), sql, &hot, 10);
        let (hits, fallbacks, _) = counts(dir.path(), "bet");
        assert!(hits > 0);

        seal(
            dir.path(),
            &[
                json!({"table": "other__thing", "k": "x", "block_number": 12,
                     "tx_hash": "0x1", "log_index": 0}),
            ],
            11,
            12,
        );
        assert_eq!(ask(dir.path(), sql, &hot, 12), old);
        assert_eq!(
            counts(dir.path(), "bet").0,
            hits + 1,
            "a seal outside the closure"
        );

        seal(dir.path(), &[resolved(3, 30, 13)], 13, 14);
        let want = oracle(dir.path(), sql, &hot, 14);
        assert_ne!(old, want, "the fixture must change the answer");
        assert_eq!(ask(dir.path(), sql, &hot, 14), want);
        assert_eq!(
            counts(dir.path(), "bet").0,
            hits + 1,
            "not served past the seal"
        );
        assert_eq!(counts(dir.path(), "bet").1, fallbacks + 1);
    }

    /// Retention keeps the copy just built and `recent` others, and leaves no partial file.
    #[test]
    fn retention_keeps_the_current_copy_and_recent_others() {
        let dir = nest(Some("recent = 1\n[[view]]\nname = \"bet\"\n"));
        held(dir.path(), 2);
        let sql = SHAPES[0];
        for n in 0..4u64 {
            let hot = tip(&[placed(4, "carol", 13 + n, 20)]);
            ask(dir.path(), sql, &hot, 10);
            crate::maintained::wait_idle(dir.path());
            assert_eq!(
                ask(dir.path(), sql, &hot, 10),
                oracle(dir.path(), sql, &hot, 10)
            );
        }
        let left = copies(dir.path(), "bet");
        assert_eq!(left.len(), 2, "{left:?}");
        assert!(left
            .iter()
            .all(|p| p.extension().is_some_and(|x| x == "parquet")));
        // The current copy is one of the two kept, and it still serves.
        let (hits, ..) = counts(dir.path(), "bet");
        let hot = tip(&[placed(4, "carol", 16, 20)]);
        ask(dir.path(), sql, &hot, 10);
        assert_eq!(counts(dir.path(), "bet").0, hits + 1);
    }

    /// A build is admitted like a query: it waits for one of the cursor's permits.
    #[test]
    fn a_build_waits_for_a_permit() {
        let dir = nest(Some("[[view]]\nname = \"bet\"\n"));
        let gate = held(dir.path(), 1);
        let held_permit = gate.clone().try_acquire_owned().unwrap();
        let hot = tip(&[placed(4, "carol", 13, 20)]);
        ask(dir.path(), SHAPES[0], &hot, 10);
        std::thread::sleep(Duration::from_millis(300));
        assert!(
            copies(dir.path(), "bet").is_empty(),
            "built without a permit"
        );
        drop(held_permit);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(copies(dir.path(), "bet").len(), 1);
    }

    /// A copy that will not read is answered around, from the definition, and built again.
    #[test]
    fn a_copy_that_will_not_read_is_answered_around_and_rebuilt() {
        let dir = nest(Some("[[view]]\nname = \"bet\"\n"));
        held(dir.path(), 2);
        let sql = SHAPES[0];
        let hot = tip(&[placed(4, "carol", 13, 20)]);
        ask(dir.path(), sql, &hot, 10);
        crate::maintained::wait_idle(dir.path());
        let copy = copies(dir.path(), "bet").pop().unwrap();
        std::fs::write(&copy, b"not parquet").unwrap();
        let want = oracle(dir.path(), sql, &hot, 10);
        assert_eq!(ask(dir.path(), sql, &hot, 10), want);
        // Gone once no request holds it; the next request queues it again.
        assert_eq!(ask(dir.path(), sql, &hot, 10), want);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(ask(dir.path(), sql, &hot, 10), want);
        assert!(
            std::fs::read(&copy).unwrap().starts_with(b"PAR1"),
            "rebuilt"
        );
    }

    /// A nest that declares nothing writes nothing and holds nothing.
    #[test]
    fn a_nest_that_declares_nothing_is_unchanged() {
        let dir = nest(None);
        crate::maintained::load(dir.path()).unwrap();
        crate::maintained::attach(dir.path(), Arc::new(tokio::sync::Semaphore::new(2)));
        let hot = tip(&[placed(4, "carol", 13, 20)]);
        for sql in SHAPES {
            ask(dir.path(), sql, &hot, 10);
        }
        assert!(!dir.path().join(crate::maintained::COPIES_DIR).exists());
        assert!(!crate::maintained::is_declared(dir.path()));
    }

    /// Refused at load, by name, with the relation that makes it so.
    #[test]
    fn a_view_reading_an_uncovered_relation_is_refused_at_load() {
        let dir = nest(None);
        std::fs::write(
            dir.path().join("views/20-more.sql"),
            "CREATE VIEW priced AS SELECT b.id, o.usd FROM bet b JOIN offchain__prices o ON o.id = b.id;\n\
             CREATE VIEW stamped AS SELECT id, now() AS at FROM bet;\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join(crate::maintained::DECLARATION_FILE),
            "[[view]]\nname = \"priced\"\n[[view]]\nname = \"stamped\"\n\
             [[view]]\nname = \"nope\"\n[[view]]\nname = \"bet\"\n",
        )
        .unwrap();
        let err = crate::maintained::load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("3 view(s)"), "{err}");
        assert!(
            err.contains("priced: `priced` reads `offchain__prices`"),
            "{err}"
        );
        assert!(
            err.contains("stamped: `stamped` calls a volatile function"),
            "{err}"
        );
        assert!(
            err.contains("nope: `nope` is not an authored view"),
            "{err}"
        );
        assert!(!crate::maintained::is_declared(dir.path()));
    }

    /// What the indexer's feed reads: hot rows and the served watermark, moved by a test as a commit
    /// or a seal would move them.
    type Inputs = Arc<std::sync::Mutex<(HotRows, u64)>>;

    fn fed(dir: &Path, hot: HotRows, sealed_through: u64) -> Inputs {
        let state: Inputs = Arc::new(std::sync::Mutex::new((hot, sealed_through)));
        let read = state.clone();
        crate::maintained::feed(
            dir,
            crate::maintained::Feed {
                read: Box::new(move || Ok(read.lock().unwrap().clone())),
                declared: Arc::new(Vec::new()),
            },
        );
        state
    }

    fn wait_queued(dir: &Path, n: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while crate::maintained::queued(dir).len() != n {
            assert!(std::time::Instant::now() < deadline, "never queued {n}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// S2: a commit or a seal that moves a view's inputs builds its copy before anyone asks, and one
    /// that moves nothing it reads builds nothing.
    #[test]
    fn an_eager_build_needs_no_request() {
        let dir = nest(Some(BOTH));
        held(dir.path(), 2);
        let hot = tip(&[placed(4, "carol", 13, 20), resolved(3, 22, 21)]);
        let inputs = fed(dir.path(), hot.clone(), 10);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(copies(dir.path(), "bet").len(), 1);
        assert_eq!(copies(dir.path(), "player_total").len(), 1);
        let asked = |hot: &HotRows, sealed: u64| {
            for sql in SHAPES {
                let want = oracle(dir.path(), sql, hot, sealed);
                assert_eq!(ask(dir.path(), sql, hot, sealed), want, "{sql}");
            }
        };
        asked(&hot, 10);
        assert_eq!(counts(dir.path(), "bet").1, 0, "no request fell back");
        assert_eq!(counts(dir.path(), "player_total").1, 0);

        // A commit of rows the views read.
        let hot = tip(&[
            placed(4, "carol", 13, 20),
            resolved(3, 22, 21),
            placed(5, "dave", 17, 22),
        ]);
        *inputs.lock().unwrap() = (hot.clone(), 10);
        crate::maintained::changed(dir.path());
        crate::maintained::wait_idle(dir.path());
        assert_eq!(counts(dir.path(), "bet").2, 2);
        assert_eq!(counts(dir.path(), "player_total").2, 2);
        asked(&hot, 10);
        assert_eq!(counts(dir.path(), "bet").1, 0, "built before the request");

        // A commit of rows no maintained view reads.
        let mut unrelated = hot.clone();
        unrelated.insert(
            "other__thing".into(),
            vec![
                json!({"table": "other__thing", "k": "x", "block_number": 23,
                        "tx_hash": "0x9", "log_index": 0}),
            ],
        );
        *inputs.lock().unwrap() = (unrelated, 10);
        crate::maintained::changed(dir.path());
        crate::maintained::wait_idle(dir.path());
        assert_eq!(
            counts(dir.path(), "bet").2,
            2,
            "nothing the view reads moved"
        );

        // A seal of rows the views read.
        seal(dir.path(), &[resolved(4, 40, 13)], 13, 14);
        *inputs.lock().unwrap() = (hot.clone(), 14);
        crate::maintained::changed(dir.path());
        crate::maintained::wait_idle(dir.path());
        assert_eq!(counts(dir.path(), "bet").2, 3);
        asked(&hot, 14);
        assert_eq!(counts(dir.path(), "bet").1, 0, "built before the request");
    }

    /// S2's latest wins: a build queued for inputs that have since moved is dropped before it runs,
    /// and only the current inputs are built.
    #[test]
    fn a_queued_build_of_old_inputs_is_never_run() {
        let dir = nest(Some("[[view]]\nname = \"bet\"\n"));
        let gate = held(dir.path(), 1);
        let busy = gate.clone().try_acquire_owned().unwrap();
        let old = tip(&[placed(4, "carol", 13, 20)]);
        let new = tip(&[placed(4, "carol", 13, 20), resolved(4, 26, 21)]);
        let inputs = fed(dir.path(), old.clone(), 10);
        wait_queued(dir.path(), 1);
        *inputs.lock().unwrap() = (new.clone(), 10);
        crate::maintained::changed(dir.path());
        drop(busy);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(
            counts(dir.path(), "bet").2,
            1,
            "only the current inputs built"
        );
        assert_eq!(copies(dir.path(), "bet").len(), 1);
        let sql = SHAPES[0];
        assert_eq!(
            ask(dir.path(), sql, &new, 10),
            oracle(dir.path(), sql, &new, 10)
        );
        assert_eq!(counts(dir.path(), "bet").0, 1, "served from the copy");
    }

    /// A request on a fed nest that read older inputs than the store now holds asks for a plan, and
    /// never queues its own: the store's current inputs are what get built.
    #[test]
    fn a_request_never_queues_the_old_inputs_it_read() {
        let dir = nest(Some("[[view]]\nname = \"bet\"\n"));
        let gate = held(dir.path(), 1);
        let busy = gate.clone().try_acquire_owned().unwrap();
        let old = tip(&[placed(4, "carol", 13, 20)]);
        let new = tip(&[placed(4, "carol", 13, 20), resolved(4, 26, 21)]);
        fed(dir.path(), new.clone(), 10);
        wait_queued(dir.path(), 1);
        ask(dir.path(), SHAPES[0], &old, 10);
        drop(busy);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(counts(dir.path(), "bet").2, 1);
        let sql = SHAPES[0];
        assert_eq!(
            ask(dir.path(), sql, &new, 10),
            oracle(dir.path(), sql, &new, 10)
        );
        assert_eq!(
            counts(dir.path(), "bet").0,
            1,
            "the store's inputs were built"
        );
    }

    /// Inputs that move away and come back before their builds run, as a short reorg does, leave
    /// nothing queued for the state they passed through: every view's copy is already current.
    #[test]
    fn inputs_that_return_to_a_built_state_build_nothing() {
        let dir = nest(Some(BOTH));
        let gate = held(dir.path(), 1);
        let built = tip(&[placed(4, "carol", 13, 20)]);
        let passing = tip(&[placed(4, "carol", 13, 20), resolved(4, 26, 21)]);
        let inputs = fed(dir.path(), built.clone(), 10);
        crate::maintained::wait_idle(dir.path());
        let builds = (
            counts(dir.path(), "bet").2,
            counts(dir.path(), "player_total").2,
        );
        assert_eq!(builds, (1, 1));

        let busy = gate.clone().try_acquire_owned().unwrap();
        *inputs.lock().unwrap() = (passing, 10);
        crate::maintained::changed(dir.path());
        wait_queued(dir.path(), 2);
        *inputs.lock().unwrap() = (built, 10);
        crate::maintained::changed(dir.path());
        drop(busy);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(
            (
                counts(dir.path(), "bet").2,
                counts(dir.path(), "player_total").2
            ),
            builds,
            "nothing built for the state passed through"
        );
        assert_eq!(copies(dir.path(), "player_total").len(), 1);
    }

    /// Copies of a view no longer declared are removed when the declaration is next read; the
    /// declared view's copies stay.
    #[test]
    fn copies_of_a_view_no_longer_declared_are_removed() {
        let dir = nest(Some(BOTH));
        held(dir.path(), 2);
        fed(dir.path(), tip(&[placed(4, "carol", 13, 20)]), 10);
        crate::maintained::wait_idle(dir.path());
        assert_eq!(copies(dir.path(), "player_total").len(), 1);
        let kept = copies(dir.path(), "bet");
        assert_eq!(kept.len(), 1);

        std::fs::write(
            dir.path().join(crate::maintained::DECLARATION_FILE),
            "[[view]]\nname = \"bet\"\n",
        )
        .unwrap();
        crate::maintained::load(dir.path()).unwrap();
        let root = dir.path().join(crate::maintained::COPIES_DIR);
        assert!(!root.join("player_total").exists());
        assert_eq!(copies(dir.path(), "bet"), kept);

        std::fs::remove_file(dir.path().join(crate::maintained::DECLARATION_FILE)).unwrap();
        crate::maintained::load(dir.path()).unwrap();
        assert!(!root.exists(), "nothing declared, nothing kept");
    }
}
