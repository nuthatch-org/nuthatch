//! The analytical engine as a trait, with Burrmill behind it (`engine_burrmill`).
//!
//! RFC-0044 Amendment 2. `analytics.rs` keeps every policy decision - the four read-only gates, the
//! allowlist walk, the session cache, the shared deadline, the integrity sweep - and asks the engine
//! only for what any SQL engine over the sealed segments can answer: bind a fact table from its
//! inputs, run a statement to rows, cancel one, count its cold scans.

use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) use crate::analytics::FactWindow;

/// The per-result Rust-side byte ceiling for the guarded `/sql` surface (64 MiB). Comfortably above
/// any legitimate 50k-row result, far below the per-cursor RAM budget - the backstop against a
/// wide-cell `SELECT` inflating the materialised buffer past the budget. Part of `collect`'s
/// contract whenever a row cap is given.
pub(crate) const SQL_MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;

/// A reading of a session's memory pool, from any thread.
pub(crate) type MemoryGauge = Arc<dyn Fn() -> u64 + Send + Sync>;

/// A cheap lower-bound byte estimate of a materialised cell - dominated by string payloads, which is
/// exactly the wide-cell attack vector. Numbers/bools/null count a small fixed cost.
pub(crate) fn value_bytes(v: &Value) -> usize {
    match v {
        Value::String(s) => s.len(),
        Value::Array(a) => 8 + a.iter().map(value_bytes).sum::<usize>(),
        Value::Object(o) => {
            8 + o
                .iter()
                .map(|(k, x)| k.len() + value_bytes(x))
                .sum::<usize>()
        }
        _ => 8,
    }
}

/// Opens sessions.
pub(crate) trait Engine: Send + Sync {
    /// A session bounded and locked to `dir`: the nest's memory, thread and spill limits, file
    /// access confined to its data directories.
    fn open(&self, dir: &Path) -> Result<Box<dyn Session>>;
    /// An unbounded session with no nest behind it, for binding authored SQL to see whether it
    /// parses and validates. Nothing untrusted runs on one.
    fn open_bare(&self) -> Result<Box<dyn Session>>;
    /// What the live sessions hold against their memory pools, read without their locks (#1778).
    fn pools(&self) -> Pools;
}

/// Memory pool use across the engine's live sessions. Each session has a pool of its own, so two
/// concurrent statements can each reserve up to the configured limit.
#[derive(Debug, Default, Clone)]
pub(crate) struct Pools {
    pub total: PoolUse,
    /// The sessions opened over a nest directory, by that directory.
    pub by_dir: std::collections::HashMap<PathBuf, PoolUse>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PoolUse {
    pub engines: u64,
    pub reserved: u64,
    /// The most any one of these sessions' pools has held since start.
    pub peak: u64,
}

/// Cancels a running statement from another thread.
pub(crate) trait Interrupt: Send + Sync {
    fn interrupt(&self);
    /// Clear a cancel left by an earlier statement. The host does this before it arms the next
    /// one; the engine leaves the token alone, or an interrupt during planning would be lost.
    fn reset(&self) {}
}

/// Which phase of a statement failed.
///
/// The distinction that matters for #433 is `Executing`: a statement that fails to **bind** is a
/// question about names - a typo, a missing column, an unknown table - and no corrupt page can
/// cause one, so it must never trigger the integrity sweep. Only a statement that bound and then
/// died while reading rows is worth paying for.
#[derive(Debug)]
pub(crate) enum Died {
    /// The engine refused it before running: a name the catalogue does not have, a type that does
    /// not check.
    Binding(anyhow::Error),
    /// It bound, then died running or materialising rows - the only shape a corrupt page produces.
    Executing(anyhow::Error),
}

impl From<Died> for anyhow::Error {
    fn from(died: Died) -> Self {
        match died {
            Died::Binding(e) | Died::Executing(e) => e,
        }
    }
}

/// A materialised result.
#[derive(Debug)]
pub(crate) struct Collected {
    pub rows: Vec<Value>,
    /// In the statement's projection order. Each row is a `serde_json::Map`, which sorts its keys,
    /// so this is the only record of that order (#1609).
    pub columns: Vec<String>,
    /// More rows were available than the cap allowed.
    pub truncated: bool,
}

/// One engine instance over one nest: a catalogue being built up and statements run against it.
/// `Send`, because the connection cache hands a session from one request's thread to the next.
// The methods only `folds` calls are part of the contract without it.
#[allow(dead_code)]
pub(crate) trait Session: Send {
    /// Run a statement for its effect: a view definition, a transaction boundary.
    fn execute(&self, sql: &str) -> Result<()>;

    /// Prepare, execute and materialise the result as nuthatch-encoded JSON rows. With
    /// `cap = Some(n)` it stops after `n + 1` rows so the caller can report truncation precisely
    /// (`truncated` is true when that extra row existed), and also caps cumulative result bytes;
    /// `cap = None` materialises every row. The cap is `(rows, bytes)`.
    fn collect(&self, sql: &str, cap: Option<(usize, usize)>) -> Result<Collected, Died>;

    /// Stream a result row by row, each as its cells in column order, so a large result is never
    /// held whole.
    fn for_each_row(&self, sql: &str, f: &mut dyn FnMut(&[Value]) -> Result<()>) -> Result<()>;

    /// The first cell of the first row.
    fn one_value(&self, sql: &str) -> Result<Value>;

    /// A result as Arrow.
    fn query_arrow(&self, sql: &str) -> Result<Vec<arrow::record_batch::RecordBatch>>;

    /// The column names of a statement's result, learned by running it.
    fn column_names(&self, sql: &str) -> Result<Vec<String>>;

    /// `(column, type)` of a statement's output, in the engine's own spelling of the type.
    fn describe(&self, sql: &str) -> Result<Vec<(String, String)>>;

    /// Whether a table or view of this name exists, compared without case. `true` when the
    /// catalogue cannot be asked, which is the answer that refuses rather than admits.
    fn has_relation(&self, name: &str) -> bool;

    /// Every table and view currently defined, lowercased.
    fn relations(&self) -> Result<BTreeSet<String>>;

    /// Each defined view's name and its stored `CREATE VIEW` text; `None` when the catalogue
    /// cannot be listed.
    fn view_definitions(&self) -> Option<Vec<(String, String)>>;

    /// The security walk: what the statement reaches, from the engine's own parse. `None` when the
    /// parser could not say (it fails open; the denylist is still in front), `Some(Err)` when the
    /// statement is refused with the reason, `Some(Ok((tables, surveys)))` with the base tables and
    /// CTE names lowercased and whether it asks about the catalogue.
    fn reach(&self, sql: &str) -> Option<Result<(BTreeSet<String>, bool)>>;

    /// `SELECT * FROM table ORDER BY ALL` written to `path` as one Parquet file (a fold checkpoint).
    fn write_parquet(&self, table: &str, path: &Path) -> Result<()>;

    /// `table` replaced by the Parquet file at `path`, read through `select` (its columns cast).
    fn load_parquet(&self, table: &str, select: &str, path: &Path) -> Result<()>;

    /// The statement as this engine keys it for reuse (RFC-0033 §3); `None` keys it by its raw text.
    fn canonical_plan(&self, sql: &str) -> Option<String>;

    /// This engine and its version, written into reuse keys (RFC-0033 §2.2).
    fn engine_version(&self) -> String;

    /// The physical tables a statement reads (names a `WITH` binds in scope excluded) and the table
    /// functions it calls, lowercased; `None` when the statement will not parse.
    fn table_refs(&self, sql: &str) -> Option<(BTreeSet<String>, BTreeSet<String>)>;

    /// A handle another thread can use to cancel whatever this session is running.
    fn interrupt_handle(&self) -> Arc<dyn Interrupt>;

    /// This session's private spill directory and the bytes it may hold, for the `/sql` guard to
    /// measure. `None` for a session that does not spill to a directory of its own.
    fn spill_limit(&self) -> Option<(std::path::PathBuf, u64)> {
        None
    }

    /// Bytes the pool this session draws on holds now, readable from the watchdog's thread.
    fn memory_gauge(&self) -> Option<MemoryGauge> {
        None
    }

    /// The guard's deadline for the statement about to run, so an engine that does extra work
    /// beside the answer (a shadow) can decline it when the budget is nearly spent. Advisory; the
    /// watchdog still enforces the deadline.
    fn set_deadline(&self, _deadline: Option<std::time::Instant>) {}

    /// How many cold Parquet scans the statement's physical plan performs (RFC-0048 §3). Refuses,
    /// as `AdmissionRefusal::Unboundable`, any plan it cannot count.
    fn cold_scan_operators(&self, sql: &str) -> Result<u64>;

    /// Load one table's hot rows so that `bind_facts(.., hot = true, ..)` can union them in.
    fn load_hot(&self, table: &str, rows: &[&Value]) -> Result<()>;

    /// How many hot rows [`Session::load_hot`] last staged for `table`.
    fn staged_hot_len(&self, table: &str) -> usize;

    /// Remove a maintained relation's public name, so a pooled session cannot answer from one an
    /// earlier request defined after the entity faulted (#1598).
    fn drop_relation(&self, name: &str) -> Result<()> {
        let _ = name;
        Ok(())
    }

    /// [`Session::load_hot`] for a maintained relation: it has no sealed Parquet to line up with, so
    /// its columns take the types its plan declares (`cols`, #1598), or, with none declared, the types
    /// of its own cells. With declared columns it exists even with no rows.
    fn load_relation(
        &self,
        table: &str,
        cols: &[(String, &'static str)],
        rows: &[&Value],
    ) -> Result<()> {
        let _ = cols;
        self.load_hot(table, rows)
    }

    /// Define `table` over its sealed segments and, when `hot`, the rows `load_hot` staged, with
    /// every declared column present and the derived `*_dec`/`*_overflow` columns projected.
    /// `Ok(false)` when there was nothing to define: no segments, no hot rows and no declared
    /// columns. `Err` when a segment would not bind, which the caller answers by probing each one.
    fn bind_facts(
        &self,
        table: &str,
        cols: &[(String, String)],
        sealed: &[PathBuf],
        hot: bool,
        window: FactWindow,
    ) -> Result<bool>;

    /// Whether one sealed segment binds at all; the error carries the engine's own wording.
    fn segment_binds(&self, path: &Path) -> Result<()>;

    /// `(column, type)` of one sealed segment, or `None` when it cannot be read.
    fn file_schema(&self, path: &Path) -> Option<Vec<(String, String)>>;

    /// Define `view` over immutable offchain snapshots, unioned by name.
    fn bind_snapshots(&self, view: &str, files: &[PathBuf]) -> Result<()>;

    /// Define the `labels` view over the `*.json` snapshots in `labels_dir`.
    fn bind_labels(&self, labels_dir: &Path) -> Result<()>;
}

/// A bare session, for a test.
#[cfg(test)]
pub(crate) fn bare() -> Box<dyn Session> {
    crate::analytics::engine().open_bare().unwrap()
}

/// Run a test body on a bare session.
#[cfg(test)]
pub(crate) fn on_bare(body: impl Fn(&dyn Session)) {
    body(bare().as_ref());
}
