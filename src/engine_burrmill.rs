//! Burrmill behind [`crate::engine`] (RFC-0044 Amendment 2).
//!
//! A session is one `burrmill::Engine` opened empty, with the tables the policy code binds registered
//! as it binds them: the segment list, the hot rows, the declared columns and the window.

use crate::engine::{Collected, Died, Engine, FactWindow, Interrupt, Pools, Session};
use anyhow::{anyhow, Context, Result};
use nuthatch_core::answer::{rewrite_temporal, temporal_columns};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, TryLockError, Weak};

pub(crate) struct BurrmillEngine;

impl Engine for BurrmillEngine {
    fn open(&self, dir: &Path, tenant: Option<&str>) -> Result<Box<dyn Session>> {
        Ok(Box::new(BurrmillSession::new(Some((dir, tenant)))?))
    }

    fn open_bare(&self) -> Result<Box<dyn Session>> {
        Ok(Box::new(BurrmillSession::new(None)?))
    }

    fn pools(&self) -> Pools {
        pools()
    }
}

// Every live session's engine, so a scrape reads the pools without waiting out a statement (#1778).
type Live = (Option<PathBuf>, Weak<RwLock<burrmill::Engine>>);
static LIVE: Mutex<Vec<Live>> = Mutex::new(Vec::new());

struct Peaks {
    all: u64,
    by_dir: std::collections::BTreeMap<PathBuf, u64>,
}

// Burrmill's peak resets when it is read, so every read is folded in here.
static PEAKS: Mutex<Peaks> = Mutex::new(Peaks {
    all: 0,
    by_dir: std::collections::BTreeMap::new(),
});

fn fold_peak(dir: Option<&Path>, peak: u64) {
    let mut peaks = PEAKS.lock().unwrap_or_else(|p| p.into_inner());
    peaks.all = peaks.all.max(peak);
    if let Some(dir) = dir {
        let at = peaks.by_dir.entry(dir.to_path_buf()).or_default();
        *at = (*at).max(peak);
    }
}

fn pools() -> Pools {
    let live: Vec<_> = {
        let mut live = LIVE.lock().unwrap_or_else(|p| p.into_inner());
        live.retain(|(_, e)| e.strong_count() > 0);
        live.iter()
            .filter_map(|(dir, e)| Some((dir.clone(), e.upgrade()?)))
            .collect()
    };
    let engines: Vec<_> = live
        .into_iter()
        .map(|(dir, engine)| {
            // Held exclusively only while tables are bound, which reserves nothing.
            let reserved = match engine.try_read() {
                Ok(e) => Some(e),
                Err(TryLockError::Poisoned(p)) => Some(p.into_inner()),
                Err(TryLockError::WouldBlock) => None,
            }
            .map_or(0, |e| {
                fold_peak(dir.as_deref(), e.take_memory_peak() as u64);
                e.memory_reserved() as u64
            });
            (dir, reserved)
        })
        .collect();
    let mut by_dir = std::collections::BTreeMap::<PathBuf, u64>::new();
    for (dir, pool) in NEST_POOLS.lock().unwrap_or_else(|p| p.into_inner()).iter() {
        *by_dir.entry(dir.clone()).or_default() += pool.reserved() as u64;
    }
    for (dir, _, pool) in statement_pools() {
        *by_dir.entry(dir).or_default() += pool.reserved() as u64;
    }
    let nest_pools: Vec<_> = by_dir.into_iter().collect();
    let mut out = tally(engines, &nest_pools);
    let peaks = PEAKS.lock().unwrap_or_else(|p| p.into_inner());
    out.total.peak = peaks.all;
    for (dir, peak) in &peaks.by_dir {
        out.by_dir.entry(dir.clone()).or_default().peak = *peak;
    }
    out
}

/// Each engine's reading by the directory it was opened over, and each nest pool's total. A
/// nest's engines each read their shared pool's total, so a nest's is the pool's, once (#1792).
fn tally(engines: Vec<(Option<PathBuf>, u64)>, nest_pools: &[(PathBuf, u64)]) -> Pools {
    let mut out = Pools::default();
    for (dir, reserved) in engines {
        out.total.engines += 1;
        match dir {
            Some(dir) => out.by_dir.entry(dir).or_default().engines += 1,
            None => out.total.reserved += reserved,
        }
    }
    for (dir, reserved) in nest_pools {
        if let Some(u) = out.by_dir.get_mut(dir) {
            u.reserved = *reserved;
            out.total.reserved += reserved;
        }
    }
    out
}

pub(crate) struct BurrmillSession {
    // Statements take it shared, so a scrape can read the pool while one runs.
    engine: Arc<RwLock<burrmill::Engine>>,
    dir: Option<PathBuf>,
    /// Hot rows staged by `load_hot` and how many, the rows taken by the `bind_facts` that binds them.
    hot: Mutex<HashMap<String, (Vec<Value>, usize)>>,
    /// Held for the session's life; removed on drop and swept by pid after a crash.
    _spill: crate::spill::SpillDir,
    /// Each view's `CREATE VIEW` text as defined, for the integrity sweep's walk through views.
    views: Mutex<std::collections::BTreeMap<String, String>>,
    /// The declared columns of each maintained relation `load_relation` staged, with the SQL type
    /// its plan gives each, so one with no rows still binds and a count is a number (#1598).
    relations: Mutex<HashMap<String, Vec<(String, &'static str)>>>,
    /// The pool this session draws on: its dataset's, shared, or under strict isolation its own.
    pool: Option<Arc<burrmill::SharedPool>>,
}

impl BurrmillSession {
    /// A maintained relation as a table of its declared types (`nuthatch_core::bind::relation`):
    /// `register_rows` makes every column text, and the view casts each back.
    fn bind_relation(&self, table: &str, declared: &[(String, &'static str)]) -> Result<bool> {
        let staged = self.take_hot(table);
        let (raw, rows, view) = nuthatch_core::bind::relation(table, declared, &staged);
        let mut engine = self.engine();
        let registered = engine
            .register_rows(&raw, &rows)
            .and_then(|()| engine.register_view(table, &view))
            .map_err(engine_err);
        if let Err(e) = registered {
            // As in `bind_facts`: a retry binds from the same rows.
            self.restore_hot(table, staged);
            return Err(e);
        }
        Ok(true)
    }

    /// The rows `load_hot` staged for `table`, leaving their count. A bind that fails puts them back
    /// with [`Self::restore_hot`], since the caller retries it without the segments that would not bind.
    fn take_hot(&self, table: &str) -> Vec<Value> {
        self.hot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(table)
            .map(|(rows, _)| std::mem::take(rows))
            .unwrap_or_default()
    }

    fn restore_hot(&self, table: &str, rows: Vec<Value>) {
        if let Some((staged, _)) = self
            .hot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(table)
        {
            *staged = rows;
        }
    }

    fn new(nest: Option<(&Path, Option<&str>)>) -> Result<Self> {
        let spill = crate::spill::new_spill_dir()?;
        let budget = budget(&crate::analytics_budget::from_env(), &spill.0);
        let pool = nest.map(|(dir, tenant)| nest_pool(dir, tenant, budget.memory_bytes));
        #[allow(unused_mut)]
        let mut engine = match &pool {
            Some(pool) => burrmill::Engine::open_empty_sharing(budget, pool),
            None => burrmill::Engine::open_empty_budgeted(budget),
        }
        .map_err(engine_err)?;
        #[cfg(feature = "graph")]
        crate::analytics_scalars::register(&mut engine);
        let engine = Arc::new(RwLock::new(engine));
        let dir = nest.map(|(dir, _)| dir.to_path_buf());
        LIVE.lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((dir.clone(), Arc::downgrade(&engine)));
        Ok(Self {
            engine,
            dir,
            hot: Mutex::new(HashMap::new()),
            _spill: spill,
            views: Mutex::new(std::collections::BTreeMap::new()),
            relations: Mutex::new(HashMap::new()),
            pool,
        })
    }

    fn engine(&self) -> std::sync::RwLockWriteGuard<'_, burrmill::Engine> {
        self.engine.write().unwrap_or_else(|p| p.into_inner())
    }

    fn reader(&self) -> std::sync::RwLockReadGuard<'_, burrmill::Engine> {
        self.engine.read().unwrap_or_else(|p| p.into_inner())
    }

    #[cfg(test)]
    fn held_hot_rows(&self, table: &str) -> usize {
        self.hot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(table)
            .map_or(0, |(rows, _)| rows.len())
    }
}

impl Drop for BurrmillSession {
    fn drop(&mut self) {
        fold_peak(self.dir.as_deref(), self.reader().take_memory_peak() as u64);
    }
}

// A statement that finds the cached session busy opens another, so the limit has to bound the
// nest's sessions together (#1792). The pool keeps the size its first session gave it while any
// session holds it.
static NEST_POOLS: Mutex<std::collections::BTreeMap<PathBuf, Arc<burrmill::SharedPool>>> =
    Mutex::new(std::collections::BTreeMap::new());

// Under strict isolation every session has a pool of its own (RFC-0073 FR8). A session runs one
// statement at a time and each running statement holds a gate permit, so these are what
// `validate_strict_budget` charges, one per permit (RFC-0076 §3.1).
type StatementPool = (PathBuf, String, Weak<burrmill::SharedPool>);
static STATEMENT_POOLS: Mutex<Vec<StatementPool>> = Mutex::new(Vec::new());

fn statement_pools() -> Vec<(PathBuf, String, Arc<burrmill::SharedPool>)> {
    let mut pools = STATEMENT_POOLS.lock().unwrap_or_else(|p| p.into_inner());
    pools.retain(|(_, _, p)| p.strong_count() > 0);
    pools
        .iter()
        .filter_map(|(d, t, p)| Some((d.clone(), t.clone(), p.upgrade()?)))
        .collect()
}

fn nest_pool(dir: &Path, tenant: Option<&str>, memory_bytes: usize) -> Arc<burrmill::SharedPool> {
    if let Some(tenant) = tenant {
        let pool = Arc::new(burrmill::SharedPool::new(memory_bytes));
        let mut pools = STATEMENT_POOLS.lock().unwrap_or_else(|p| p.into_inner());
        pools.retain(|(_, _, p)| p.strong_count() > 0);
        pools.push((dir.to_path_buf(), tenant.to_string(), Arc::downgrade(&pool)));
        return pool;
    }
    let mut pools = NEST_POOLS.lock().unwrap_or_else(|p| p.into_inner());
    // A pool no session holds reserves nothing and is rebuilt on demand.
    pools.retain(|_, p| Arc::strong_count(p) > 1 || p.reserved() > 0);
    pools
        .entry(dir.to_path_buf())
        .or_insert_with(|| Arc::new(burrmill::SharedPool::new(memory_bytes)))
        .clone()
}

/// The tenants with a pool of their own over `dir`, `None` being the shared one.
#[cfg(test)]
pub(crate) fn pool_tenants(dir: &Path) -> Vec<Option<String>> {
    pool_reservations(dir)
        .into_iter()
        .map(|(t, _)| t)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// What each pool over `dir` holds reserved, by tenant: one entry per strict session.
#[cfg(test)]
pub(crate) fn pool_reservations(dir: &Path) -> Vec<(Option<String>, usize)> {
    let shared = NEST_POOLS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(dir)
        .map(|p| (None, p.reserved()));
    shared
        .into_iter()
        .chain(
            statement_pools()
                .into_iter()
                .filter(|(d, _, _)| d == dir)
                .map(|(_, t, p)| (Some(t), p.reserved())),
        )
        .collect()
}

/// The walls `analytics_budget` sets, as Burrmill's budget.
fn budget(cfg: &crate::analytics_budget::AnalyticsConfig, spill: &Path) -> burrmill::Budget {
    burrmill::Budget {
        memory_bytes: (cfg.burrmill_limit_mb() as usize) << 20,
        threads: cfg.threads.max(1) as usize,
        spill: Some((
            spill.to_path_buf(),
            crate::analytics_budget::spill_cap_bytes(cfg) as _,
        )),
    }
}

fn engine_err(e: burrmill::BurrmillError) -> anyhow::Error {
    anyhow!("{e}")
}

/// Rows of one batch turned into JSON before the byte cap is checked again (#1650).
const ENCODE_ROWS: usize = 32;

#[cfg(test)]
thread_local! {
    static ENCODED_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_encoded_rows(rows: usize) {
    ENCODED_ROWS.with(|c| c.set(c.get().max(rows)));
}

#[cfg(test)]
pub(crate) fn reset_encoded_rows() {
    ENCODED_ROWS.with(|c| c.set(0));
}

#[cfg(test)]
pub(crate) fn max_encoded_rows() -> usize {
    ENCODED_ROWS.with(|c| c.get())
}

/// Refused before running, or died running: the split the integrity sweep depends on.
fn died(e: burrmill::BurrmillError) -> Died {
    use burrmill::BurrmillError::*;
    match e {
        NotAllowed(_) | Parse(_) | Plan(_) | NoSegments(_) => Died::Binding(engine_err(e)),
        // DataFusion's disk manager stops the statement itself, well inside the watchdog's poll,
        // and names a setting of its own. `/sql` answers this as it does the watchdog's stop.
        Substrate(m) if m.contains("disk space during the spilling process has exceeded") => {
            Died::Executing(
                crate::analytics::QuerySpillExceeded {
                    cap_bytes: crate::analytics_budget::spill_cap_bytes(
                        &crate::analytics_budget::from_env(),
                    ),
                }
                .into(),
            )
        }
        _ => Died::Executing(engine_err(e)),
    }
}

fn sized(files: &[PathBuf]) -> Result<Vec<(PathBuf, u64)>> {
    files
        .iter()
        .map(|p| {
            let len = std::fs::metadata(p)
                .with_context(|| format!("stat {}", p.display()))?
                .len();
            Ok((p.clone(), len))
        })
        .collect()
}

/// Burrmill's token: the statement stops at its next scan batch, since a DataFusion join yields
/// nothing above the scan until it is done.
struct Cancel(burrmill::CancelToken);

impl Interrupt for Cancel {
    fn interrupt(&self) {
        self.0.cancel();
    }
    fn reset(&self) {
        self.0.reset();
    }
}

impl Session for BurrmillSession {
    fn execute(&self, sql: &str) -> Result<()> {
        use sqlparser::ast::{ObjectType, Statement};
        if let (Some(name), Some(body)) = (
            crate::analytics::view_name(sql),
            crate::analytics::view_body(sql),
        ) {
            self.engine()
                .register_view(&name, body)
                .map_err(engine_err)?;
            self.views
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(name, sql.to_string());
            return Ok(());
        }
        // The folds path's statements for effect, and nothing else: a statement is still read-only.
        let stmts =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, sql)?;
        // In order: each takes effect before the next, and the batch is not atomic.
        let mut engine = self.engine();
        for stmt in &stmts {
            match stmt {
                Statement::CreateTable(ct) if ct.query.is_some() => {
                    let name = ct.name.to_string().trim_matches('"').to_string();
                    let query = ct.query.as_ref().expect("checked").to_string();
                    engine.create_table_as(&name, &query).map_err(engine_err)?;
                }
                Statement::Drop {
                    object_type: ObjectType::View | ObjectType::Table,
                    names,
                    ..
                } => {
                    for n in names {
                        let n = n.to_string().trim_matches('"').to_string();
                        engine.drop_relation(&n);
                        self.views
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .remove(&n);
                    }
                }
                Statement::StartTransaction { .. } => engine.begin().map_err(engine_err)?,
                Statement::Commit { .. } => engine.commit().map_err(engine_err)?,
                Statement::Rollback { .. } => engine.rollback().map_err(engine_err)?,
                _ => return Err(anyhow!("Burrmill runs no `{stmt}` for effect")),
            }
        }
        Ok(())
    }

    fn write_parquet(&self, table: &str, path: &Path) -> Result<()> {
        self.reader()
            .write_parquet(&format!("SELECT * FROM \"{table}\" ORDER BY ALL"), path)
            .map_err(engine_err)?;
        Ok(())
    }

    fn load_parquet(&self, table: &str, select: &str, path: &Path) -> Result<()> {
        const LOADING: &str = "__checkpoint_load";
        let mut engine = self.engine();
        engine.load_parquet(LOADING, path).map_err(engine_err)?;
        let made = engine.create_table_as(table, &format!("SELECT {select} FROM \"{LOADING}\""));
        engine.drop_relation(LOADING);
        made.map_err(engine_err)?;
        Ok(())
    }

    fn collect(&self, sql: &str, cap: Option<(usize, usize)>) -> Result<Collected, Died> {
        let hard = cap.map(|(rows, _)| rows + 1);
        let byte_cap = cap.map(|(_, bytes)| bytes);
        let mut bytes = 0usize;
        let mut out = Vec::new();
        let mut over = false;
        let mut columns: Vec<String> = Vec::new();
        let engine = self.reader();
        let mut cost = crate::cost::asked().then(burrmill::QueryMetrics::default);
        let r = engine.sql_for_each_metered(sql, cost.as_mut(), |batch| {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
            }
            let temporal = temporal_columns(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| (f.name(), f.data_type().to_string())),
            );
            // A batch can still be wider than the 64 MiB cap, so the cap runs on a slice (#1650).
            let encode = |part| {
                let rows = burrmill::df::encode::rows(&part).map(|mut rows| {
                    rewrite_temporal(&mut rows, &temporal);
                    rows
                });
                #[cfg(test)]
                note_encoded_rows(part.num_rows());
                rows
            };
            let mut start = 0;
            while start < batch.num_rows() {
                let n = (batch.num_rows() - start).min(ENCODE_ROWS);
                let part = batch.slice(start, n);
                start += n;
                for row in encode(part)? {
                    if byte_cap.is_some() {
                        bytes += row
                            .as_object()
                            .map(|o| {
                                o.iter()
                                    .map(|(k, v)| k.len() + crate::engine::value_bytes(v))
                                    .sum::<usize>()
                            })
                            .unwrap_or(0);
                    }
                    out.push(row);
                    if hard.is_some_and(|h| out.len() >= h)
                        || byte_cap.is_some_and(|max| bytes >= max)
                    {
                        over = true;
                        return Err(burrmill::BurrmillError::LimitExceeded("cap".into()));
                    }
                }
            }
            Ok(())
        });
        if let Some(cost) = &cost {
            crate::cost::engine(cost);
        }
        let truncated = match r {
            Ok(()) => false,
            Err(_) if over => true,
            Err(e) => return Err(died(e)),
        };
        // An answer with no rows has no batch to read the names from; the plan has them (#1609).
        if columns.is_empty() {
            if let Ok(described) = engine.describe(sql) {
                columns = described.into_iter().map(|(name, _)| name).collect();
            }
        }
        Ok(Collected {
            rows: out,
            columns,
            truncated,
        })
    }

    fn for_each_row(&self, sql: &str, f: &mut dyn FnMut(&[Value]) -> Result<()>) -> Result<()> {
        let engine = self.reader();
        let mut failed: Option<anyhow::Error> = None;
        let mut cost = crate::cost::asked().then(burrmill::QueryMetrics::default);
        let r = engine.sql_for_each_metered(sql, cost.as_mut(), |batch| {
            let names: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let temporal = temporal_columns(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| (f.name(), f.data_type().to_string())),
            );
            let mut rows = burrmill::df::encode::rows(&batch)?;
            rewrite_temporal(&mut rows, &temporal);
            for row in rows {
                let cells: Vec<Value> = names
                    .iter()
                    .map(|n| row.get(n).cloned().unwrap_or(Value::Null))
                    .collect();
                if let Err(e) = f(&cells) {
                    failed = Some(e);
                    return Err(burrmill::BurrmillError::Cancelled);
                }
            }
            Ok(())
        });
        if let Some(cost) = &cost {
            crate::cost::engine(cost);
        }
        match (r, failed) {
            (_, Some(e)) => Err(e),
            (Ok(()), None) => Ok(()),
            (Err(e), None) => Err(engine_err(e)),
        }
    }

    fn one_value(&self, sql: &str) -> Result<Value> {
        let out = self.collect(sql, Some((1, crate::engine::SQL_MAX_RESULT_BYTES)))?;
        let first = out.columns.first();
        out.rows
            .first()
            .and_then(|r| first.and_then(|c| r.get(c)).cloned())
            .ok_or_else(|| anyhow!("no rows"))
    }

    /// Through Arrow IPC, because Burrmill's arrow is not nuthatch's.
    fn query_arrow(&self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let ipc = self.reader().sql_ipc(sql).map_err(engine_err)?;
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)?;
        Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// From the plan: a result with no rows has no batch to read.
    fn column_names(&self, sql: &str) -> Result<Vec<String>> {
        Ok(self
            .reader()
            .describe(sql)
            .map_err(engine_err)?
            .into_iter()
            .map(|(name, _)| name)
            .collect())
    }

    fn describe(&self, sql: &str) -> Result<Vec<(String, String)>> {
        self.reader().describe(sql).map_err(engine_err)
    }

    fn has_relation(&self, name: &str) -> bool {
        self.reader().has_table(name)
    }

    fn relations(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .reader()
            .visible_tables()
            .into_iter()
            .map(|t| t.to_ascii_lowercase())
            .collect())
    }

    fn view_definitions(&self) -> Option<Vec<(String, String)>> {
        let views = self.views.lock().unwrap_or_else(|p| p.into_inner());
        Some(views.iter().map(|(n, s)| (n.clone(), s.clone())).collect())
    }

    fn function_names(&self) -> Vec<String> {
        self.reader().function_names()
    }

    fn canonical_plan(&self, sql: &str) -> Option<String> {
        burrmill::inspect::canonical(sql)
    }

    fn engine_version(&self) -> String {
        burrmill::ENGINE.to_string()
    }

    fn table_refs(&self, sql: &str) -> Option<(BTreeSet<String>, BTreeSet<String>)> {
        Some((
            burrmill::inspect::base_tables(sql)?,
            burrmill::inspect::refs(sql)?.functions,
        ))
    }

    /// Burrmill's own walk, `inspect::reach`: sqlparser's AST under nuthatch's allowlist and
    /// reachability rules, failing closed. A parse it cannot make is `None`.
    fn reach(&self, sql: &str) -> Option<Result<(BTreeSet<String>, bool)>> {
        match burrmill::inspect::reach(sql) {
            Ok(r) => Some(Ok((r.tables, r.surveys))),
            Err(burrmill::BurrmillError::Parse(_)) => None,
            Err(e) => Some(Err(engine_err(e))),
        }
    }

    fn interrupt_handle(&self) -> Arc<dyn Interrupt> {
        Arc::new(Cancel(self.reader().cancel_token()))
    }

    fn memory_gauge(&self) -> Option<crate::engine::MemoryGauge> {
        let pool = self.pool.clone()?;
        Some(Arc::new(move || pool.reserved() as u64))
    }

    fn cold_scan_operators(&self, sql: &str) -> Result<u64> {
        self.reader()
            .parquet_scans(sql)
            .map_err(|e| crate::analytics::unboundable(e.to_string()))
    }

    fn load_hot(&self, table: &str, rows: &[&Value]) -> Result<()> {
        self.hot.lock().unwrap_or_else(|p| p.into_inner()).insert(
            table.to_string(),
            (rows.iter().map(|r| (*r).clone()).collect(), rows.len()),
        );
        Ok(())
    }

    fn staged_hot_len(&self, table: &str) -> usize {
        self.hot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(table)
            .map_or(0, |(_, n)| *n)
    }

    fn drop_relation(&self, name: &str) -> Result<()> {
        self.hot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name);
        self.relations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name);
        let mut engine = self.engine();
        engine.drop_relation(name);
        engine.drop_relation(&format!("{name}__raw"));
        Ok(())
    }

    fn load_relation(
        &self,
        table: &str,
        cols: &[(String, &'static str)],
        rows: &[&Value],
    ) -> Result<()> {
        // Every row is checked before anything is staged: one that is not what its plan declares
        // leaves no relation, not a wrong one.
        for row in rows {
            for (c, t) in cols {
                match (*t, row.get(c)) {
                    (_, None | Some(Value::Null)) => {}
                    ("HUGEINT", Some(v)) => {
                        let text = v.as_str().map_or_else(|| v.to_string(), str::to_string);
                        let n = text.parse::<i128>().map_err(|_| {
                            anyhow!("{c} = {text} is not the integer its plan declares")
                        })?;
                        // HUGEINT is DECIMAL(38,0) here, short of the i128 the entity holds.
                        if n.unsigned_abs() >= 10u128.pow(38) {
                            return Err(anyhow!(
                                "entity {table}: {c} = {text} is past the largest integer the \
                                 query engine holds, 38 digits"
                            ));
                        }
                    }
                    ("BOOLEAN", Some(v)) if !v.is_boolean() => {
                        return Err(anyhow!("{c} = {v} is not the boolean its plan declares"));
                    }
                    _ => {}
                }
            }
        }
        self.relations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(table.to_string(), cols.to_vec());
        self.load_hot(table, rows)
    }

    fn bind_facts(
        &self,
        table: &str,
        cols: &[(String, String)],
        sealed: &[PathBuf],
        hot: bool,
        window: FactWindow,
    ) -> Result<bool> {
        let declared = if cols.is_empty() && hot && sealed.is_empty() {
            self.relations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(table)
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        if !declared.is_empty() {
            return self.bind_relation(table, &declared);
        }
        let hot_rows: Vec<Value> = if hot {
            self.take_hot(table)
        } else {
            Vec::new()
        };
        if sealed.is_empty() && hot_rows.is_empty() && cols.is_empty() {
            return Ok(false);
        }
        let bound = sized(sealed).and_then(|files| {
            self.engine()
                .register_facts(
                    table,
                    cols,
                    files,
                    &hot_rows,
                    (window.after, window.through),
                )
                .map_err(engine_err)
        });
        if bound.is_err() {
            self.restore_hot(table, hot_rows);
        }
        bound.map(|()| true)
    }

    fn segment_binds(&self, path: &Path) -> Result<()> {
        let f = std::fs::File::open(path)?;
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .with_context(|| format!("{} will not bind", path.display()))?;
        Ok(())
    }

    fn file_schema(&self, path: &Path) -> Option<Vec<(String, String)>> {
        let f = std::fs::File::open(path).ok()?;
        let b = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f).ok()?;
        Some(
            b.schema()
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().to_string()))
                .collect(),
        )
    }

    fn bind_snapshots(&self, view: &str, files: &[PathBuf]) -> Result<()> {
        self.engine()
            .register_facts(view, &[], sized(files)?, &[], (None, None))
            .map_err(engine_err)
    }

    fn bind_labels(&self, labels_dir: &Path) -> Result<()> {
        let mut rows = Vec::new();
        for e in std::fs::read_dir(labels_dir)?.flatten() {
            let p = e.path();
            if p.extension().is_none_or(|x| x != "json") {
                continue;
            }
            let text = std::fs::read_to_string(&p)?;
            let doc: Value = serde_json::from_str(&text)
                .with_context(|| format!("{} is not JSON", p.display()))?;
            for item in doc.as_array().into_iter().flatten() {
                let address = item
                    .get("address")
                    .and_then(Value::as_str)
                    .map(str::to_ascii_lowercase);
                let label = item.get("label").cloned().unwrap_or(Value::Null);
                rows.push(serde_json::json!({ "address": address, "label": label }));
            }
        }
        if rows.is_empty() {
            return Err(anyhow!("no label snapshots"));
        }
        self.engine()
            .register_rows("labels", &rows)
            .map_err(engine_err)
    }
}

#[cfg(test)]
mod tests {
    /// 512 MB, two threads and a private spill directory, unless the operator says otherwise.
    #[test]
    fn unconfigured_burrmill_opens_at_todays_walls() {
        let _env = crate::analytics_budget::tests::env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let cfg = crate::analytics_budget::from_env();
        assert_eq!((cfg.memory_limit_mb, cfg.threads), (512, 2));
        let spill = crate::spill::new_spill_dir().unwrap();
        let budget = super::budget(&cfg, &spill.0);
        assert_eq!(budget.memory_bytes, 512 << 20);
        assert_eq!(budget.threads, 2);
        assert_eq!(budget.spill, Some((spill.0.clone(), 2 << 30)));
        assert!(super::BurrmillSession::new(None).is_ok());
    }

    #[test]
    fn burrmill_keys_derivations_by_its_own_parse_and_build() {
        use crate::engine::Session;
        let s = super::BurrmillSession::new(None).unwrap();
        let a = s.canonical_plan("SELECT a.x FROM t a -- c\nWHERE a.y > 1");
        assert!(a.is_some());
        assert_eq!(a, s.canonical_plan("select b.x from t b where b.y > 1"));
        assert_ne!(a, s.canonical_plan("SELECT a.x FROM u a WHERE a.y > 1"));
        assert!(s.engine_version().starts_with("burrmill "));
    }

    /// A session over `dir` with `table` bound to `n` rows in scrambled order.
    fn sorting_session(dir: &std::path::Path, tables: &[(&str, u64)]) -> super::BurrmillSession {
        use crate::engine::{FactWindow, Session};
        let s = super::BurrmillSession::new(Some((dir, None))).unwrap();
        for (table, n) in tables {
            let rows: Vec<serde_json::Value> = (1..=*n)
                .map(|b| serde_json::json!({ "block_number": b, "n": format!("{:09}", 7919 * b % 100_003) }))
                .collect();
            let refs: Vec<&serde_json::Value> = rows.iter().collect();
            s.load_hot(table, &refs).unwrap();
            assert!(s
                .bind_facts(table, &[], &[], true, FactWindow::default())
                .unwrap());
        }
        s
    }

    fn pool_of(dir: &std::path::Path) -> crate::engine::PoolUse {
        super::pools().by_dir.get(dir).copied().unwrap_or_default()
    }

    /// #1778: a scrape during a statement reads what it holds, and a peak no scrape saw survives
    /// the session.
    #[test]
    fn a_running_statement_shows_in_its_pool_and_its_peak_outlives_the_session() {
        use crate::engine::Session;
        let dir = tempfile::tempdir().unwrap();
        let s = sorting_session(dir.path(), &[("small", 2_000), ("large", 40_000)]);

        let mut during = None;
        s.for_each_row("SELECT n, block_number FROM small ORDER BY n", &mut |_| {
            during.get_or_insert_with(|| pool_of(dir.path()));
            Ok(())
        })
        .unwrap();
        let during = during.expect("the sort returned rows");
        assert_eq!(during.engines, 1);
        assert!(
            during.reserved > 0,
            "the sort was emitting and its pool read {during:?}"
        );
        assert_eq!(pool_of(dir.path()).reserved, 0, "the statement is done");
        let seen = pool_of(dir.path()).peak;

        // Larger, and finished without a scrape.
        s.for_each_row("SELECT n, block_number FROM large ORDER BY n", &mut |_| {
            Ok(())
        })
        .unwrap();
        drop(s);
        let after = pool_of(dir.path());
        assert_eq!(after.engines, 0);
        assert!(
            after.peak > seen,
            "the larger sort's peak was lost with the session: {after:?}, {seen} before it"
        );
    }

    /// #1792: a nest's sessions share one pool and each reads that pool's total as its own, so a
    /// sum over them counts the pool once per session.
    #[test]
    fn two_sessions_of_one_nest_report_their_pool_once() {
        use std::path::PathBuf;
        let nest = PathBuf::from("/nests/a");
        let engines = vec![
            (Some(nest.clone()), 400),
            (Some(nest.clone()), 400),
            (Some(PathBuf::from("/nests/b")), 30),
            (None, 7),
        ];
        let pools = [(nest.clone(), 400), (PathBuf::from("/nests/b"), 30)];
        let out = super::tally(engines, &pools);
        let a = out.by_dir[&nest];
        assert_eq!((a.engines, a.reserved), (2, 400), "{out:?}");
        assert_eq!(out.by_dir[&PathBuf::from("/nests/b")].reserved, 30);
        assert_eq!(
            (out.total.engines, out.total.reserved),
            (4, 437),
            "each pool once, plus the bare session's own: {out:?}"
        );
    }

    /// `/sql` collects, and a scrape must not wait behind it.
    #[test]
    fn a_scrape_reads_the_pool_while_a_collect_runs() {
        use crate::engine::Session;
        let dir = tempfile::tempdir().unwrap();
        let s = sorting_session(dir.path(), &[("t", 100_000)]);
        let done = std::sync::atomic::AtomicBool::new(false);
        let seen = std::thread::scope(|scope| {
            let watcher = scope.spawn(|| {
                let mut seen = 0;
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    seen = seen.max(pool_of(dir.path()).reserved);
                }
                seen
            });
            let out = s.collect("SELECT n, block_number FROM t ORDER BY n", None);
            done.store(true, std::sync::atomic::Ordering::Relaxed);
            assert_eq!(out.map(|c| c.rows.len()).ok(), Some(100_000));
            watcher.join().unwrap()
        });
        assert!(seen > 0, "no scrape saw the sort's reservation");
    }

    /// Sessions over `dirs` under a 64 MB limit and no room to spill, each holding a table `t` its
    /// sort needs a large share of that limit for.
    fn bounded_sessions(dirs: &[&std::path::Path]) -> Vec<Box<dyn crate::engine::Session>> {
        use crate::analytics_budget::{ENV_BURRMILL_MEMORY_LIMIT, ENV_MAX_TEMP_SIZE};
        use crate::engine::{Engine as _, FactWindow};
        let sessions: Vec<_> = {
            let _env = crate::analytics_budget::tests::env_lock()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            std::env::set_var(ENV_BURRMILL_MEMORY_LIMIT, "64MB");
            std::env::set_var(ENV_MAX_TEMP_SIZE, "0MB");
            let opened = dirs
                .iter()
                .map(|d| super::BurrmillEngine.open(d, None).unwrap())
                .collect();
            std::env::remove_var(ENV_BURRMILL_MEMORY_LIMIT);
            std::env::remove_var(ENV_MAX_TEMP_SIZE);
            opened
        };
        let rows: Vec<serde_json::Value> = (1..=100_000u64)
            .map(|b| serde_json::json!({ "block_number": b, "k": format!("{:0180}", b * 7919 % 100_003) }))
            .collect();
        let refs: Vec<&serde_json::Value> = rows.iter().collect();
        for s in &sessions {
            s.load_hot("t", &refs).unwrap();
            assert!(s
                .bind_facts("t", &[], &[], true, FactWindow::default())
                .unwrap());
        }
        sessions
    }

    const SORT: &str = "SELECT k, block_number FROM t ORDER BY k";

    /// Runs `SORT` on `first` and, while its first row is out, on `second`; hands `first` back with
    /// how the second statement ended.
    fn overlapped(
        first: Box<dyn crate::engine::Session>,
        second: &dyn crate::engine::Session,
    ) -> (Box<dyn crate::engine::Session>, anyhow::Result<()>) {
        let (out_tx, out_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        std::thread::scope(|s| {
            let a = s.spawn(move || {
                let mut sent = false;
                first
                    .for_each_row(SORT, &mut |_| {
                        if !sent {
                            sent = true;
                            out_tx.send(()).unwrap();
                            done_rx.recv().unwrap();
                        }
                        Ok(())
                    })
                    .unwrap();
                first
            });
            out_rx.recv().unwrap();
            let r = second.for_each_row(SORT, &mut |_| Ok(()));
            done_tx.send(()).unwrap();
            (a.join().unwrap(), r)
        })
    }

    /// #1792: NUTHATCH_BURRMILL_MEMORY_LIMIT bounds a nest, so a second session of it competes for
    /// what the first holds instead of reserving a limit of its own beside it.
    #[test]
    fn two_sessions_of_one_nest_share_its_memory_limit() {
        let (nest, other) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let mut s = bounded_sessions(&[nest.path(), nest.path(), other.path()]);
        let (c, b, a) = (s.pop().unwrap(), s.pop().unwrap(), s.pop().unwrap());
        b.for_each_row(SORT, &mut |_| Ok(()))
            .expect("the sort fits the limit alone");

        let (a, second) = overlapped(a, b.as_ref());
        assert!(
            second.is_err(),
            "a second session of the nest sorted beside the first, past one limit"
        );
        let (_, elsewhere) = overlapped(a, c.as_ref());
        elsewhere.expect("another nest's session has a limit of its own");
    }

    /// #1677: a cached session held its hot rows twice, as the staged JSON and as the bound table.
    #[test]
    fn binding_releases_the_staged_hot_rows() {
        use crate::engine::{FactWindow, Session};
        let s = super::BurrmillSession::new(None).unwrap();
        let rows: Vec<serde_json::Value> = (1..=1000)
            .map(|b| serde_json::json!({ "block_number": b, "n": b.to_string() }))
            .collect();
        let refs: Vec<&serde_json::Value> = rows.iter().collect();
        let count = |t: &str| s.one_value(&format!("SELECT count(*) FROM {t}")).unwrap();

        s.load_hot("t", &refs).unwrap();
        assert!(s
            .bind_facts("t", &[], &[], true, FactWindow::default())
            .unwrap());
        assert_eq!(count("t"), serde_json::json!(1000));
        assert_eq!(
            s.staged_hot_len("t"),
            1000,
            "what was loaded is still known"
        );
        assert_eq!(s.held_hot_rows("t"), 0, "the bound table holds the rows");

        // A bind that fails is retried without the segment that would not bind, from the same rows.
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("bad.parquet");
        std::fs::write(&bad, b"not parquet").unwrap();
        s.load_hot("u", &refs).unwrap();
        let window = FactWindow::default();
        assert!(s.bind_facts("u", &[], &[bad], true, window).is_err());
        assert!(s.bind_facts("u", &[], &[], true, window).unwrap());
        assert_eq!(
            count("u"),
            serde_json::json!(1000),
            "the retry lost the hot rows"
        );

        let cols = [("n".to_string(), "HUGEINT")];
        s.load_relation("r", &cols, &refs).unwrap();
        assert!(s
            .bind_facts("r", &[], &[], true, FactWindow::default())
            .unwrap());
        assert_eq!(count("r"), serde_json::json!(1000));
        assert_eq!(s.held_hot_rows("r"), 0, "the bound relation holds the rows");

        // The declared-relation path puts the rows back on a failed bind too.
        let bad_cols = [("n".to_string(), "NOT_A_TYPE")];
        s.load_relation("q", &bad_cols, &refs).unwrap();
        assert!(s
            .bind_facts("q", &[], &[], true, FactWindow::default())
            .is_err());
        assert_eq!(
            s.held_hot_rows("q"),
            1000,
            "a failed relation bind dropped the staged rows"
        );
    }

    /// #1984: an authored view's ORDER BY orders the rows read straight out of it, as DuckDB's did.
    #[test]
    fn an_authored_views_order_by_orders_what_is_read_from_it() {
        use crate::engine::Session;
        let dir = tempfile::tempdir().unwrap();
        let s = sorting_session(dir.path(), &[("t", 2_000)]);
        s.execute("CREATE VIEW by_n AS SELECT n, block_number FROM t ORDER BY n DESC")
            .unwrap();
        for sql in [
            "SELECT * FROM by_n",
            "SELECT n FROM (SELECT n FROM t ORDER BY n DESC)",
        ] {
            let mut seen = Vec::new();
            s.for_each_row(sql, &mut |r| {
                seen.push(r[0].as_str().unwrap().to_string());
                Ok(())
            })
            .unwrap();
            let mut want = seen.clone();
            want.sort_by(|a, b| b.cmp(a));
            assert_eq!(seen.len(), 2_000, "{sql}");
            assert!(seen == want, "{sql} came back out of the view's order");
        }
    }

    /// Burrmill #10: `__raw`, `__hot` and `__union` are not names a statement can reach.
    #[test]
    fn a_hidden_registration_name_is_refused() {
        use crate::engine::Session;
        let s = super::BurrmillSession::new(None).unwrap();
        let err = s
            .reach("SELECT max(block_number) FROM t__union")
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("t__union"), "{err}");
    }

    /// Both read paths rewrite by column type, and a text column holding the same string is data.
    #[test]
    fn only_temporal_columns_are_rewritten_on_either_read_path() {
        use crate::engine::Session;
        let s = super::BurrmillSession::new(None).unwrap();
        let sql = "SELECT CAST('2026-10-05' AS DATE) AS d, 'Date32(1)' AS text";
        let out = s.collect(sql, None).unwrap();
        assert_eq!(
            out.rows,
            vec![serde_json::json!({"d": "2026-10-05", "text": "Date32(1)"})]
        );
        let mut cells = Vec::new();
        s.for_each_row(sql, &mut |row| {
            cells.extend_from_slice(row);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            cells,
            vec![
                serde_json::json!("2026-10-05"),
                serde_json::json!("Date32(1)")
            ]
        );
    }
}

/// `sql_errors::enrich` for each class, from the error Burrmill actually raises for the fault rather
/// than a remembered string.
#[cfg(test)]
mod sql_error_hints {
    use crate::engine::{Died, Engine, FactWindow, Session};
    use crate::registry::{ColumnSchema, TableKind, TableSchema};
    use crate::sql_errors::enrich;

    fn schema() -> Vec<TableSchema> {
        vec![TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "Transfer".into(),
            topic0: "0xddf2".into(),
            columns: vec![
                ColumnSchema {
                    name: "from".into(),
                    sol_type: "address".into(),
                    storage: "address".into(),
                    indexed: true,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "to".into(),
                    sol_type: "address".into(),
                    storage: "address".into(),
                    indexed: true,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "value".into(),
                    sol_type: "uint256".into(),
                    storage: "word32".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "enabled".into(),
                    sol_type: "bool".into(),
                    storage: "bool".into(),
                    indexed: false,
                    components: Vec::new(),
                },
                ColumnSchema {
                    name: "block_number".into(),
                    sol_type: "implicit".into(),
                    storage: "u64".into(),
                    indexed: false,
                    components: Vec::new(),
                },
            ],
        }]
    }

    fn session() -> Box<dyn Session> {
        let s = crate::engine_burrmill::BurrmillEngine.open_bare().unwrap();
        let rows = [
            serde_json::json!({"block_number": 1, "from": "0xa", "to": "0xb", "value": "5", "enabled": "true"}),
            serde_json::json!({"block_number": 2, "from": "0xc", "to": "0xd", "value": "7", "enabled": "false"}),
        ];
        let refs: Vec<&serde_json::Value> = rows.iter().collect();
        s.load_hot("usdc__transfer", &refs).unwrap();
        assert!(s
            .bind_facts("usdc__transfer", &[], &[], true, FactWindow::default())
            .unwrap());
        s
    }

    fn fails(s: &dyn Session, sql: &str) -> String {
        match s.collect(sql, Some((10, crate::engine::SQL_MAX_RESULT_BYTES))) {
            Ok(_) => panic!("`{sql}` answered"),
            Err(Died::Binding(e) | Died::Executing(e)) => format!("{e:#}"),
        }
    }

    fn hint(sql: &str) -> String {
        let s = session();
        let raw = fails(s.as_ref(), sql);
        enrich(&raw, sql, &schema()).unwrap_or_else(|| panic!("no hint for: {raw}"))
    }

    #[test]
    fn an_unknown_table() {
        let h = hint("SELECT count(*) FROM transfers");
        assert!(h.contains("the closest is `usdc__transfer`"), "{h}");
    }

    #[test]
    fn an_unknown_column_is_matched_against_the_fields_in_scope() {
        let h = hint("SELECT valu FROM usdc__transfer");
        assert!(h.contains("the closest is `value`"), "{h}");
    }

    #[test]
    fn an_unknown_column_with_no_near_match_or_a_quoted_name() {
        let h = hint("SELECT zzzz FROM usdc__transfer");
        assert!(h.starts_with("no column `zzzz`."), "{h}");
        let h = hint("SELECT \"Total Value\" FROM usdc__transfer");
        assert!(h.starts_with("no column `Total Value`."), "{h}");
    }

    #[test]
    fn a_dec_column_whose_base_is_in_scope() {
        let h = hint("SELECT \"from\" FROM usdc__transfer WHERE value_dec > 1");
        assert!(h.contains("derived on the fly"), "{h}");
    }

    #[test]
    fn a_bare_reserved_word() {
        let h = hint("SELECT from FROM usdc__transfer");
        assert!(h.contains("reserved word"), "{h}");
    }

    #[test]
    fn a_big_integer_summed_as_text() {
        let h = hint("SELECT sum(value) FROM usdc__transfer");
        assert!(h.contains("value_dec"), "{h}");
    }

    #[test]
    fn a_text_bool_in_coalesce_case_and_bool_and() {
        for sql in [
            "SELECT COALESCE(enabled, false) FROM usdc__transfer",
            "SELECT CASE WHEN true THEN enabled ELSE false END FROM usdc__transfer",
            "SELECT bool_and(enabled) FROM usdc__transfer",
            "SELECT bool_or(enabled) FROM usdc__transfer",
        ] {
            let h = hint(sql);
            assert!(h.contains("Solidity bool"), "{sql}: {h}");
        }
    }

    /// #433 through the engine: pages destroyed, footer intact, so the segment binds and the read fails.
    #[test]
    fn a_segment_corrupt_past_its_footer() {
        let s = session();
        let dir = tempfile::tempdir().unwrap();
        let seg = dir.path().join("seg.parquet");
        s.write_parquet("usdc__transfer", &seg).unwrap();
        let mut bytes = std::fs::read(&seg).unwrap();
        let len = bytes.len();
        let footer = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as usize;
        bytes[4..len - 8 - footer].fill(0xFF);
        std::fs::write(&seg, &bytes).unwrap();

        let c = crate::engine_burrmill::BurrmillEngine.open_bare().unwrap();
        c.load_hot("usdc__transfer", &[]).unwrap();
        assert!(c
            .bind_facts("usdc__transfer", &[], &[seg], false, FactWindow::default())
            .unwrap());
        let sql = "SELECT max(value) FROM usdc__transfer";
        let raw = fails(c.as_ref(), sql);
        let h = enrich(&raw, sql, &schema()).unwrap_or_default();
        assert!(h.contains("corrupt file on disk"), "{raw}");
        assert!(h.contains("`usdc__transfer`"), "{h}");
    }

    /// The phrase quoted back from the caller's own text is not a corrupt file.
    #[test]
    fn the_phrase_in_caller_text_is_not_a_corrupt_file() {
        let s = session();
        for sql in [
            "SELECT CAST('Parquet error: x' AS INTEGER) FROM usdc__transfer",
            "SELECT \"Parquet error: x\" FROM usdc__transfer",
        ] {
            let raw = fails(s.as_ref(), sql);
            let h = enrich(&raw, sql, &schema()).unwrap_or_default();
            assert!(!h.contains("corrupt file"), "{sql}: {h}");
        }
    }
}
