//! The Postgres hot store (RFC-0022 slice 2) - the scaled-mode implementation of [`HotStore`].
//!
//! Same contract, different substrate. Embedded mode keeps redb and stays a single binary with zero
//! external services (non-negotiable 1); scaled mode points the *same* business logic at Postgres so
//! a cursor's state lives somewhere a second machine can reach. No `#[cfg]` forks of business logic
//! anywhere - the only thing that changes is which `HotStore` the runtime is handed.
//!
//! ## The faithful-mapping rules
//!
//! Every table here is `(key TEXT PRIMARY KEY, value TEXT)`, mirroring redb's key-value shape rather
//! than "improving" it into a relational schema. That is deliberate. The acceptance test for this
//! slice is that served results match redb byte for byte, and the cheapest way to be sure of that is
//! to not restate the data model on the way across. A relational schema is a later optimisation with
//! its own parity proof to earn, not a freebie to take now.
//!
//! Three details that would silently break parity if missed:
//!
//! 1. **`COLLATE "C"` on every key column.** redb orders keys by *bytes*. Postgres orders `TEXT` by
//!    the database's collation, which under `en_US.UTF-8` sorts punctuation and case differently.
//!    Entity keys are `{block:012}-{log_index:06}` and outbox keys are zero-padded integers, so a
//!    locale-aware collation would reorder rows that redb keeps in chain order - and every range
//!    scan, `recent`, `checkpoints_desc` and prune bound depends on that order. `"C"` is byte order.
//! 2. **The outbox sequence stays in `meta`**, not a `SERIAL`. A sequence is not transactional in the
//!    way the rest of this is (it survives rollback by design), so a restart or an aborted
//!    transaction would let redb and Postgres disagree about the next seq. Same counter, same place,
//!    same semantics.
//! 3. **One schema per nest.** Nests sharing a database must not share a namespace - per-nest
//!    isolation is a non-negotiable, and it should not become weaker just because the store moved.
//!
//! ## Concurrency, and the trap that shaped it
//!
//! [`HotStore`] is synchronous, matching redb. The obvious implementation - the blocking `postgres`
//! client behind an `r2d2` pool - **panics**, and it took running the parity suite to find out:
//!
//! ```text
//! Cannot start a runtime from within a runtime.
//! ```
//!
//! `postgres` is not a synchronous client. It is a blocking *wrapper* that owns a private tokio
//! runtime and `block_on`s an async one, and `block_on` panics when it is called from inside another
//! runtime's context. redb is genuinely synchronous - no runtime, no reentrancy - so "use the sync
//! client, like redb" was reasoning from a false equivalence.
//!
//! This is not a test artefact. nuthatch serves from axum handlers, which run *inside* the tokio
//! runtime, so a `/sql` or `/entity` request against a Postgres-backed nest would have hit the same
//! panic in production. `spawn_blocking` does not save it either: those threads still carry a runtime
//! context.
//!
//! So the client lives on **one dedicated thread of its own**, outside any tokio runtime, and the
//! trait methods post closures to it and block on the reply. The reentrancy disappears because the
//! work genuinely happens elsewhere.
//!
//! That single thread also **serialises every store operation**, which is worth stating plainly as a
//! limitation rather than dressing up as a design: it matches the trait's single-writer contract and
//! it is honest for a v1, but concurrent reads now queue behind each other where redb's would not.
//! A pool of worker threads is the fix, and it is deliberately *not* here - it changes throughput,
//! throughput needs a benchmark, and RFC-0013's convergence work is already benchmark-gated. Parity
//! first, then speed, each with its own evidence.

use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::sync::mpsc::{self, Sender};
use std::sync::Mutex;

use crate::store::{
    HotScanTooLarge, HotStore, Lease, LeaseHeld, LostOwnership, LEASE_EXPIRES_AT, LEASE_OWNER,
    OWNER_FENCE,
};

/// A unit of work handed to the connection thread. Boxed so the trait methods can post arbitrary
/// closures rather than an enum of every query shape, which would have to grow with every method.
type Job = Box<dyn FnOnce(&mut postgres::Client) + Send>;

/// Owns the `postgres::Client` on a thread with no tokio runtime above it. See the module docs for
/// why that is load-bearing rather than fussy.
pub(crate) struct Conn {
    tx: Mutex<Sender<Job>>,
}

impl Conn {
    pub(crate) fn spawn(config: postgres::Config) -> Result<Conn> {
        let (tx, rx) = mpsc::channel::<Job>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
        std::thread::Builder::new()
            .name("nuthatch-pg".into())
            .spawn(move || {
                let mut client = match config.connect(postgres::NoTls) {
                    Ok(c) => {
                        let _ = ready_tx.send(Ok(()));
                        c
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e.to_string()));
                        return;
                    }
                };
                // Ends when the last `Sender` drops, i.e. when the `PgStore` goes away.
                while let Ok(job) = rx.recv() {
                    job(&mut client);
                }
            })
            .context("cannot spawn the Postgres connection thread")?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Conn { tx: Mutex::new(tx) }),
            Ok(Err(e)) => Err(anyhow!("{e}")),
            Err(_) => Err(anyhow!("the Postgres connection thread died on startup")),
        }
    }

    /// Run `f` on the connection thread and wait for its result.
    pub(crate) fn with<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut postgres::Client) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = mpsc::channel();
        let job: Job = Box::new(move |client| {
            let _ = tx.send(f(client));
        });
        self.tx
            .lock()
            .map_err(|_| anyhow!("the Postgres connection lock was poisoned"))?
            .send(job)
            .map_err(|_| anyhow!("the Postgres connection thread has stopped"))?;
        rx.recv()
            .map_err(|_| anyhow!("the Postgres connection thread dropped a request"))?
    }
}

/// Stored bytes one `scan_entities_in_range` page may bring over, so a scan of megabyte documents
/// holds a page within the per-cursor budget however many rows share it.
const SCAN_PAGE_BYTES: i64 = 16 * 1024 * 1024;

/// How many of a page's rows, by stored length, fit `budget`: never fewer than one, or a row larger
/// than the budget would stall the scan.
fn rows_within(lengths: &[i64], budget: i64) -> usize {
    let mut total = 0i64;
    let mut n = 0;
    for &len in lengths {
        if n > 0 && total.saturating_add(len) > budget {
            break;
        }
        total = total.saturating_add(len);
        n += 1;
    }
    n
}

/// Meta key holding the next outbox sequence number. Must match `store::OUTBOX_SEQ` - the two
/// backends read the same logical counter, and a nest migrated between them would otherwise restart
/// its sequence.
const OUTBOX_SEQ: &str = "outbox_next_seq";

pub struct PgStore {
    conn: Conn,
    /// The nest's schema, already validated and quoted-safe.
    schema: String,
    /// Fence this handle holds (RFC-0022 slice 4); `0` means it has never claimed, which disables
    /// enforcement. Shared across clones so one nest's handle speaks for one owner.
    held: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl PgStore {
    /// The trait's `commit_window`, with alert-outbox entries that land only if the window does.
    fn commit_window_with(
        &self,
        entities: &[(String, String)],
        outbox: &[String],
        checkpoint: Option<(u64, &str)>,
        last_block: u64,
    ) -> Result<()> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        let entities = entities.to_vec();
        let checkpoint = checkpoint.map(|(b, h)| (b, h.to_string()));
        let outbox = outbox.to_vec();
        self.conn.with(move |c| {
            // One transaction, exactly as redb does it. This is the atomicity the trait's contract
            // promises and `e2e_crash_safety` pins: rows, checkpoint and watermark land together, so
            // a crash leaves the store at a clean window boundary and never mid-window.
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            if !outbox.is_empty() {
                outbox_push_in_tx(&mut tx, &schema, &outbox)?;
            }
            let ins = format!(
                "INSERT INTO \"{schema}\".entities (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
            );
            for (k, v) in &entities {
                tx.execute(&ins, &[k, v])?;
            }
            if let Some((block, hash)) = &checkpoint {
                let key = format!("{block:012}");
                let existing: Option<String> = {
                    let get = format!("SELECT value FROM \"{schema}\".blocks WHERE key = $1");
                    tx.query_opt(&get, &[&key])?.map(|r| r.get::<_, String>(0))
                };
                let existing_ts = existing
                    .as_deref()
                    .and_then(|v| crate::store::decode_block_record(v).1);
                let (h, packed_ts) = crate::store::decode_block_record(hash);
                let packed = crate::store::encode_block_record(&h, packed_ts.or(existing_ts));
                let sql = format!(
                    "INSERT INTO \"{schema}\".blocks (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
                );
                tx.execute(&sql, &[&key, &packed])?;
            }
            let meta = format!(
                "INSERT INTO \"{schema}\".meta (key, value) VALUES ('last_block', $1) \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
            );
            tx.execute(&meta, &[&last_block.to_string()])?;
            // As in redb (#1636, #1665): keep the newest checkpoint at or below the watermark and
            // prune beneath it. Keys are zero-padded, so text order is block order.
            if !cfg!(feature = "graph") {
                let prune = format!(
                    "DELETE FROM \"{schema}\".blocks WHERE key IN (\
                     SELECT b.key FROM \"{schema}\".blocks b, \"{schema}\".meta m \
                     WHERE m.key = 'sealed_through' AND b.key < (SELECT max(f.key) FROM \"{schema}\".blocks f \
                     WHERE f.key <= lpad(m.value, 12, '0')) \
                     ORDER BY b.key LIMIT 4096)"
                );
                tx.execute(&prune, &[])?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    /// Connect and ensure this nest's schema exists.
    ///
    /// `nest` names a schema, so it is validated rather than escaped: an identifier cannot be a bound
    /// parameter in DDL, and building DDL by string-concatenating unvalidated input is how you get an
    /// injection. The charset is the same one `is_valid_alias` already enforces for nest aliases, so
    /// nothing legitimate is refused.
    pub fn connect(url: &str, nest: &str) -> Result<PgStore> {
        if nest.is_empty()
            || !nest
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(anyhow!(
                "nest name '{nest}' cannot be a Postgres schema - expected [a-z0-9_]+"
            ));
        }
        let schema = format!("nest_{nest}");
        let config: postgres::Config = url
            .parse()
            .with_context(|| format!("cannot parse Postgres URL '{}'", redact(url)))?;
        let conn = Conn::spawn(config)
            .with_context(|| format!("cannot connect to Postgres at '{}'", redact(url)))?;

        let store = PgStore {
            conn,
            schema,
            held: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        store.migrate()?;
        Ok(store)
    }

    /// Create the schema and the four tables. Idempotent, so a restart or a second worker taking over
    /// a cursor is a no-op rather than an error.
    ///
    /// Deliberately **unfenced**: it runs at connect time, before anything has been claimed, and it
    /// creates the very table the fence is stored in. `IF NOT EXISTS` throughout means a worker that
    /// has since been fenced out cannot damage anything by running it again.
    fn migrate(&self) -> Result<()> {
        let s = &self.schema;
        let ddl = format!(
            r#"
            CREATE SCHEMA IF NOT EXISTS "{s}";
            CREATE TABLE IF NOT EXISTS "{s}".entities (
                key   TEXT COLLATE "C" PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS "{s}".meta (
                key   TEXT COLLATE "C" PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS "{s}".blocks (
                key   TEXT COLLATE "C" PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS "{s}".outbox (
                key   TEXT COLLATE "C" PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#
        );
        // Serialised on a per-schema key: `CREATE SCHEMA/TABLE IF NOT EXISTS` are not atomic in
        // Postgres, and two workers opening the same chain's cursor at startup will both try - one
        // then dies on a duplicate-key error against `pg_namespace` or `pg_type`. Taken as its own
        // statement rather than embedded in the batch; see the note in `controlplane::migrate` for why
        // that distinction matters. Different cursors still migrate in parallel.
        let key = format!("nuthatch_migrate_{s}");
        self.conn
            .with(move |c| {
                let lock: i64 = c.query_one("SELECT hashtext($1)::bigint", &[&key])?.get(0);
                c.execute("SELECT pg_advisory_lock($1)", &[&lock])?;
                let result = c.batch_execute(&ddl);
                let _ = c.execute("SELECT pg_advisory_unlock($1)", &[&lock]);
                result?;
                Ok(())
            })
            .with_context(|| format!("failed to migrate schema {s}"))
    }

    fn get_kv(&self, table: &str, key: &str) -> Result<Option<String>> {
        let sql = format!(
            "SELECT value FROM \"{}\".{table} WHERE key = $1",
            self.schema
        );
        let key = key.to_string();
        self.conn
            .with(move |c| Ok(c.query_opt(&sql, &[&key])?.map(|r| r.get::<_, String>(0))))
    }

    /// Every single-key write goes through here, so this is where the fence has to bite for
    /// `put_entity`, `set_meta` and `set_block_hash`. It runs in a transaction purely so the fence
    /// check and the write are one decision - a bare `execute` would leave a window in which the
    /// fence moves between the two, which is the race the fence exists to close.
    fn put_kv(&self, table: &str, key: &str, value: &str) -> Result<()> {
        let sql = format!(
            "INSERT INTO \"{}\".{table} (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            self.schema
        );
        let schema = self.schema.clone();
        let held = self.held_fence();
        let (key, value) = (key.to_string(), value.to_string());
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            tx.execute(&sql, &[&key, &value])?;
            tx.commit()?;
            Ok(())
        })
    }

    fn block_record(&self, block: u64) -> Result<Option<(String, Option<u64>)>> {
        Ok(self
            .get_kv("blocks", &format!("{block:012}"))?
            .map(|v| crate::store::decode_block_record(&v)))
    }

    fn write_block_record(
        &self,
        block: u64,
        hash: Option<&str>,
        timestamp: Option<u64>,
    ) -> Result<()> {
        let key = format!("{block:012}");
        let (existing_hash, existing_ts) =
            self.block_record(block)?.unwrap_or((String::new(), None));
        let packed = crate::store::encode_block_record(
            hash.unwrap_or(&existing_hash),
            timestamp.or(existing_ts),
        );
        self.put_kv("blocks", &key, &packed)
    }
}

/// Strip any password before a connection string reaches a log or an error message.
fn redact(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(scheme), Some(at)) if at > scheme => {
            format!("{}://***@{}", &url[..scheme], &url[at + 1..])
        }
        _ => url.to_string(),
    }
}

#[async_trait::async_trait]
impl HotStore for PgStore {
    // ---- entities ---------------------------------------------------------------------------

    fn put_entity(&self, key: &str, json: &str) -> Result<()> {
        self.put_kv("entities", key, json)
    }

    fn put_entity_if_named(
        &self,
        key: &str,
        json: &str,
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool> {
        self.put_entities_if_named(
            &[(key.to_string(), json.to_string())],
            source_key,
            block_hash,
        )
    }

    fn put_entities_if_named(
        &self,
        entries: &[(String, String)],
        source_key: &str,
        block_hash: &str,
    ) -> Result<bool> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        let entries = entries.to_vec();
        let (source_key, block_hash) = (source_key.to_string(), block_hash.to_string());
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            // `FOR UPDATE` so a rollback of the naming row waits for this decision rather than racing it.
            let read = format!("SELECT value FROM \"{schema}\".entities WHERE key = $1 FOR UPDATE");
            let named = tx
                .query_opt(&read, &[&source_key])?
                .map(|r| r.get::<_, String>(0))
                .is_some_and(|v| {
                    crate::store::row_block_hash(&v).as_deref() == Some(block_hash.as_str())
                });
            if !named {
                return Ok(false);
            }
            let ins = format!(
                "INSERT INTO \"{schema}\".entities (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
            );
            for (key, json) in &entries {
                tx.execute(&ins, &[key, json])?;
            }
            tx.commit()?;
            Ok(true)
        })
    }

    fn get_entity(&self, key: &str) -> Result<Option<String>> {
        self.get_kv("entities", key)
    }

    fn count(&self) -> Result<u64> {
        let sql = format!("SELECT count(*) FROM \"{}\".entities", self.schema);
        self.conn
            .with(move |c| Ok(c.query_one(&sql, &[])?.get::<_, i64>(0) as u64))
    }

    fn recent(&self, limit: usize) -> Result<Vec<String>> {
        // Newest **first**, matching redb's `.iter().rev().take(limit)`. An earlier version reversed
        // this into oldest-first on the assumption that "recent" meant chronological; the parity
        // suite caught it, which is the entire argument for comparing against a live redb rather
        // than against what I believed redb did.
        let sql = format!(
            "SELECT value FROM \"{}\".entities ORDER BY key DESC LIMIT $1",
            self.schema
        );
        let limit = limit as i64;
        self.conn.with(move |c| {
            Ok(c.query(&sql, &[&limit])?
                .into_iter()
                .map(|r| r.get::<_, String>(0))
                .collect())
        })
    }

    fn recent_by_table(&self, table: &str, limit: usize) -> Result<Vec<String>> {
        // The row's table name lives inside the JSON, exactly as it does in redb - the store is
        // table-agnostic on both backends, and making Postgres clever here would be the first crack
        // in "same data model".
        let sql = format!(
            "SELECT value FROM \"{}\".entities \
             WHERE value::jsonb ->> 'table' = $1 ORDER BY key DESC LIMIT $2",
            self.schema
        );
        let (table, limit) = (table.to_string(), limit as i64);
        self.conn.with(move |c| {
            Ok(c.query(&sql, &[&table, &limit])?
                .into_iter()
                .map(|r| r.get::<_, String>(0))
                .collect())
        })
    }

    fn hot_rows_by_table(&self) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        self.hot_rows_by_table_bounded(usize::MAX)
    }

    fn hot_rows_by_table_bounded(
        &self,
        max_rows: usize,
    ) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        let count_sql = format!("SELECT count(*) FROM \"{}\".entities", self.schema);
        let sql = format!(
            "SELECT value FROM \"{}\".entities ORDER BY key",
            self.schema
        );
        self.conn.with(move |c| {
            let total = c.query_one(&count_sql, &[])?.get::<_, i64>(0) as usize;
            if total > max_rows {
                // The same refusal redb gives, for the same reason: a partial tip silently changes
                // the answer to an aggregate. Postgres would happily stream it, which makes refusing
                // here more important, not less.
                return Err(HotScanTooLarge { cap: max_rows }.into());
            }
            let mut out: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
            for row in c.query(&sql, &[])? {
                let raw: String = row.get(0);
                let v = serde_json::from_str::<serde_json::Value>(&raw)
                    .context("unparseable hot row in an analytical scan")?;
                let Some(t) = v.get("table").and_then(|t| t.as_str()) else {
                    continue;
                };
                out.entry(t.to_string()).or_default().push(v);
            }
            Ok(out)
        })
    }

    fn hot_rows_by_table_bounded_with_bytes(
        &self,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<crate::store::HotRowsSnapshot> {
        let count_sql = format!("SELECT count(*) FROM \"{}\".entities", self.schema);
        let sql = format!(
            "SELECT value FROM \"{}\".entities ORDER BY key",
            self.schema
        );
        self.conn.with(move |c| {
            let total = c.query_one(&count_sql, &[])?.get::<_, i64>(0) as usize;
            if total > max_rows {
                return Err(HotScanTooLarge { cap: max_rows }.into());
            }
            let mut out = crate::store::HotRowsSnapshot::default();
            for row in c.query(&sql, &[])? {
                let raw: String = row.get(0);
                out.source_rows += 1;
                out.source_bytes = out.source_bytes.saturating_add(raw.len() as u64);
                if out.source_bytes > max_bytes {
                    return Err(crate::store::HotScanBudgetExceeded {
                        source_bytes: out.source_bytes,
                        budget: max_bytes,
                    }
                    .into());
                }
                let v = serde_json::from_str::<serde_json::Value>(&raw)
                    .context("unparseable hot row in an analytical scan")?;
                let Some(t) = v.get("table").and_then(|t| t.as_str()) else {
                    continue;
                };
                out.rows.entry(t.to_string()).or_default().push(v);
            }
            Ok(out)
        })
    }

    fn entities_in_range(&self, from: u64, to: u64) -> Result<Vec<String>> {
        let (lo, hi) = (format!("{from:012}-000000"), format!("{to:012}-999999"));
        let sql = format!(
            "SELECT value FROM \"{}\".entities WHERE key >= $1 AND key <= $2 ORDER BY key",
            self.schema
        );
        self.conn.with(move |c| {
            Ok(c.query(&sql, &[&lo, &hi])?
                .into_iter()
                .map(|r| r.get::<_, String>(0))
                .collect())
        })
    }

    fn scan_entities_in_range(
        &self,
        from: u64,
        to: u64,
        visit: &mut dyn FnMut(u64, &str) -> bool,
    ) -> Result<()> {
        const PAGE: i64 = 1_000;
        let hi = format!("{to:012}-999999");
        let mut after: Option<String> = None;
        loop {
            let (lo, op) = match &after {
                Some(k) => (k.clone(), ">"),
                None => (format!("{from:012}-000000"), ">="),
            };
            // Sizes first, so a page of megabyte documents is cut to `SCAN_PAGE_BYTES` before any
            // value leaves the server rather than after a thousand of them have.
            let sizes_sql = format!(
                "SELECT key, octet_length(value)::bigint FROM \"{}\".entities \
                 WHERE key {op} $1 AND key <= $2 ORDER BY key LIMIT $3",
                self.schema
            );
            let (size_lo, size_hi) = (lo.clone(), hi.clone());
            let sizes: Vec<(String, i64)> = self.conn.with(move |c| {
                Ok(c.query(&sizes_sql, &[&size_lo, &size_hi, &PAGE])?
                    .into_iter()
                    .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
                    .collect())
            })?;
            let lengths: Vec<i64> = sizes.iter().map(|(_, n)| *n).collect();
            let take = rows_within(&lengths, SCAN_PAGE_BYTES);
            if take == 0 {
                return Ok(());
            }
            let last = sizes[take - 1].0.clone();
            let values_sql = format!(
                "SELECT key, value FROM \"{}\".entities WHERE key {op} $1 AND key <= $2 \
                 ORDER BY key LIMIT $3",
                self.schema
            );
            let limit = take as i64;
            let page: Vec<(String, String)> = self.conn.with(move |c| {
                Ok(c.query(&values_sql, &[&lo, &last, &limit])?
                    .into_iter()
                    .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
                    .collect())
            })?;
            for (key, value) in &page {
                let block = key
                    .get(..12)
                    .and_then(|b| b.parse::<u64>().ok())
                    .with_context(|| format!("corrupt entity key {key:?}"))?;
                if !visit(block, value) {
                    return Ok(());
                }
            }
            let more = take < sizes.len() || sizes.len() as i64 == PAGE;
            match page.last() {
                Some((key, _)) if more => after = Some(key.clone()),
                _ => return Ok(()),
            }
        }
    }

    fn sample_entity_keys(&self, limit: usize) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT key FROM \"{}\".entities ORDER BY key LIMIT $1",
            self.schema
        );
        let limit = limit as i64;
        self.conn.with(move |c| {
            Ok(c.query(&sql, &[&limit])?
                .into_iter()
                .map(|r| r.get::<_, String>(0))
                .collect())
        })
    }

    // ---- cursor & meta ----------------------------------------------------------------------

    fn get_meta(&self, key: &str) -> Result<Option<String>> {
        self.get_kv("meta", key)
    }

    fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.put_kv("meta", key, value)
    }

    // `left(...) = $1` rather than `LIKE`: a prefix such as `ipfs_gave_up:` carries `_`, a wildcard there.
    fn meta_with_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
        let sql = format!(
            "SELECT key, value FROM \"{}\".meta WHERE left(key, char_length($1::text)) = $1::text \
             ORDER BY key LIMIT $2",
            self.schema
        );
        let (prefix, limit) = (prefix.to_string(), limit as i64);
        self.conn.with(move |c| {
            Ok(c.query(&sql, &[&prefix, &limit])?
                .iter()
                .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
                .collect())
        })
    }

    fn count_meta_with_prefix(&self, prefix: &str) -> Result<u64> {
        let sql = format!(
            "SELECT count(*) FROM \"{}\".meta WHERE left(key, char_length($1::text)) = $1::text",
            self.schema
        );
        let prefix = prefix.to_string();
        self.conn
            .with(move |c| Ok(c.query_one(&sql, &[&prefix])?.get::<_, i64>(0) as u64))
    }

    fn indexed_head(&self) -> Result<Option<u64>> {
        let hot = self
            .get_meta("last_block")?
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let head = hot.max(self.sealed_through());
        Ok((head > 0).then_some(head))
    }

    fn sealed_through(&self) -> u64 {
        self.get_meta("sealed_through")
            .ok()
            .flatten()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    fn set_block_hash(&self, block: u64, hash: &str) -> Result<()> {
        self.write_block_record(block, Some(hash), None)
    }

    fn get_block_hash(&self, block: u64) -> Result<Option<String>> {
        Ok(self
            .block_record(block)?
            .map(|(h, _)| h)
            .filter(|h| !h.is_empty()))
    }

    fn set_block_timestamp(&self, block: u64, timestamp: u64) -> Result<()> {
        self.write_block_record(block, None, Some(timestamp))
    }

    fn get_block_timestamp(&self, block: u64) -> Result<Option<u64>> {
        Ok(self.block_record(block)?.and_then(|(_, ts)| ts))
    }

    fn checkpoints_desc(&self) -> Result<Vec<(u64, String)>> {
        let sql = format!(
            "SELECT key, value FROM \"{}\".blocks ORDER BY key DESC",
            self.schema
        );
        self.conn.with(move |c| {
            c.query(&sql, &[])?
                .into_iter()
                .map(|r| {
                    let k: String = r.get(0);
                    let block: u64 = k.parse().context("corrupt block key")?;
                    let raw: String = r.get(1);
                    let (hash, _) = crate::store::decode_block_record(&raw);
                    Ok((block, hash))
                })
                .collect()
        })
    }

    // ---- mutation windows (the atomic ones) --------------------------------------------------

    fn commit_window(
        &self,
        entities: &[(String, String)],
        checkpoint: Option<(u64, &str)>,
        last_block: u64,
    ) -> Result<()> {
        self.commit_window_with(entities, &[], checkpoint, last_block)
    }

    async fn commit_window_blocking(
        &self,
        entities: Vec<(String, String)>,
        outbox: Vec<String>,
        checkpoint: Option<(u64, String)>,
        last_block: u64,
    ) -> Result<()> {
        // The work already happens on the connection thread, so there is nothing extra to offload -
        // the async signature exists so callers need not know which backend they hold.
        let cp = checkpoint.as_ref().map(|(b, h)| (*b, h.as_str()));
        self.commit_window_with(&entities, &outbox, cp, last_block)
    }

    /// The commit above runs inside the caller's poll, so it cannot outlive an abort.
    async fn settle_commits(&self) {}

    fn rollback_to(&self, block: u64) -> Result<u64> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            let removed = rollback_in_tx(&mut tx, &schema, block)?;
            tx.commit()?;
            Ok(removed)
        })
    }

    fn rollback_to_and_set_meta(
        &self,
        block: u64,
        meta_key: &str,
        meta_val: &str,
        outbox: &[String],
    ) -> Result<u64> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        let (mk, mv) = (meta_key.to_string(), meta_val.to_string());
        let outbox = outbox.to_vec();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            if !outbox.is_empty() {
                outbox_push_in_tx(&mut tx, &schema, &outbox)?;
            }
            let removed = rollback_in_tx(&mut tx, &schema, block)?;
            set_meta_in_tx(&mut tx, &schema, &mk, &mv)?;
            tx.commit()?;
            Ok(removed)
        })
    }

    fn prune_range(&self, from: u64, to: u64) -> Result<u64> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            let removed = prune_in_tx(&mut tx, &schema, from, to)?;
            tx.commit()?;
            Ok(removed)
        })
    }

    fn prune_and_set_meta(
        &self,
        from: u64,
        to: u64,
        meta_key: &str,
        meta_val: &str,
    ) -> Result<u64> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        let (mk, mv) = (meta_key.to_string(), meta_val.to_string());
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            let removed = prune_in_tx(&mut tx, &schema, from, to)?;
            set_meta_in_tx(&mut tx, &schema, &mk, &mv)?;
            tx.commit()?;
            Ok(removed)
        })
    }

    // ---- delivery outbox --------------------------------------------------------------------

    fn claim(&self, owner: &str) -> Result<u64> {
        let schema = self.schema.clone();
        let owner = owner.to_string();
        // Deliberately unfenced: claiming is how a new holder takes over from a stale one.
        // `FOR UPDATE` serialises two workers racing for the same cursor, so exactly one wins and
        // the loser reads the winner's fence rather than duplicating it.
        let next = self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            let read = format!("SELECT value FROM \"{schema}\".meta WHERE key = $1 FOR UPDATE");
            let current: u64 = tx
                .query_opt(&read, &[&OWNER_FENCE])?
                .and_then(|r| r.get::<_, String>(0).parse().ok())
                .unwrap_or(0);
            let next = current + 1;
            set_meta_in_tx(&mut tx, &schema, OWNER_FENCE, &next.to_string())?;
            set_meta_in_tx(&mut tx, &schema, "owner", &owner)?;
            tx.commit()?;
            Ok(next)
        })?;
        self.held.store(next, std::sync::atomic::Ordering::SeqCst);
        Ok(next)
    }

    fn current_fence(&self) -> Result<u64> {
        Ok(self
            .get_kv("meta", OWNER_FENCE)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0))
    }

    fn acquire_lease(&self, owner: &str, ttl_secs: u64) -> Result<Lease> {
        let schema = self.schema.clone();
        let owner_s = owner.to_string();
        let next = self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            // `FOR UPDATE` on the fence row serialises two workers racing for the same cursor, so
            // exactly one of them evaluates "is it takeable?" at a time. Without it both could read
            // an expired lease and both conclude yes.
            let read = format!("SELECT value FROM \"{schema}\".meta WHERE key = $1 FOR UPDATE");
            let fence: u64 = tx
                .query_opt(&read, &[&OWNER_FENCE])?
                .and_then(|r| r.get::<_, String>(0).parse().ok())
                .unwrap_or(0);
            let holder: String = tx
                .query_opt(&read, &[&LEASE_OWNER])?
                .map(|r| r.get::<_, String>(0))
                .unwrap_or_default();
            let expires_at: i64 = tx
                .query_opt(&read, &[&LEASE_EXPIRES_AT])?
                .and_then(|r| r.get::<_, String>(0).parse().ok())
                .unwrap_or(0);
            // **The database's clock, not the worker's** - the reason the lease lives here at all.
            let now: i64 = tx
                .query_one("SELECT extract(epoch from now())::bigint", &[])?
                .get(0);

            if !(holder.is_empty() || expires_at <= now || holder == owner_s) {
                return Err(LeaseHeld {
                    by: holder,
                    expires_in_secs: expires_at - now,
                }
                .into());
            }
            let next = fence + 1;
            set_meta_in_tx(&mut tx, &schema, OWNER_FENCE, &next.to_string())?;
            set_meta_in_tx(&mut tx, &schema, LEASE_OWNER, &owner_s)?;
            set_meta_in_tx(
                &mut tx,
                &schema,
                LEASE_EXPIRES_AT,
                &(now + ttl_secs as i64).to_string(),
            )?;
            tx.commit()?;
            Ok(next)
        })?;
        self.held.store(next, std::sync::atomic::Ordering::SeqCst);
        Ok(Lease {
            owner: owner.to_string(),
            fence: next,
            expires_in_secs: ttl_secs as i64,
        })
    }

    fn renew_lease(&self, ttl_secs: u64) -> Result<Lease> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            // Fenced like any other write: a superseded holder must not extend a lease it lost.
            guard_fence_in_tx(&mut tx, &schema, held)?;
            let now: i64 = tx
                .query_one("SELECT extract(epoch from now())::bigint", &[])?
                .get(0);
            let read = format!("SELECT value FROM \"{schema}\".meta WHERE key = $1");
            let holder: String = tx
                .query_opt(&read, &[&LEASE_OWNER])?
                .map(|r| r.get::<_, String>(0))
                .unwrap_or_default();
            let fence: u64 = tx
                .query_opt(&read, &[&OWNER_FENCE])?
                .and_then(|r| r.get::<_, String>(0).parse().ok())
                .unwrap_or(0);
            set_meta_in_tx(
                &mut tx,
                &schema,
                LEASE_EXPIRES_AT,
                &(now + ttl_secs as i64).to_string(),
            )?;
            tx.commit()?;
            Ok(Lease {
                owner: holder,
                fence,
                expires_in_secs: ttl_secs as i64,
            })
        })
    }

    fn release_lease(&self) -> Result<()> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            set_meta_in_tx(&mut tx, &schema, LEASE_EXPIRES_AT, "0")?;
            tx.commit()?;
            Ok(())
        })
    }

    fn current_lease(&self) -> Result<Option<Lease>> {
        let holder = self.get_kv("meta", LEASE_OWNER)?.unwrap_or_default();
        if holder.is_empty() {
            return Ok(None);
        }
        let expires_at: i64 = self
            .get_kv("meta", LEASE_EXPIRES_AT)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let schema = self.schema.clone();
        let now: i64 = self.conn.with(move |c| {
            let _ = &schema;
            Ok(
                c.query_one("SELECT extract(epoch from now())::bigint", &[])?
                    .get(0),
            )
        })?;
        Ok(Some(Lease {
            owner: holder,
            fence: self.current_fence()?,
            expires_in_secs: expires_at - now,
        }))
    }

    fn held_fence(&self) -> u64 {
        self.held.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn outbox_push(&self, payload: &str) -> Result<u64> {
        let schema = self.schema.clone();
        let held = self.held_fence();
        let payload = payload.to_string();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            let seq = outbox_push_in_tx(&mut tx, &schema, &[payload])?;
            tx.commit()?;
            Ok(seq)
        })
    }

    fn outbox_pending(&self, limit: usize) -> Result<Vec<(u64, String)>> {
        let sql = format!(
            "SELECT key, value FROM \"{}\".outbox ORDER BY key LIMIT $1",
            self.schema
        );
        let limit = limit as i64;
        self.conn.with(move |c| {
            c.query(&sql, &[&limit])?
                .into_iter()
                .map(|r| {
                    let k: String = r.get(0);
                    let seq: u64 = k.parse().context("corrupt outbox key")?;
                    Ok((seq, r.get::<_, String>(1)))
                })
                .collect()
        })
    }

    fn outbox_remove(&self, seq: u64) -> Result<()> {
        let sql = format!("DELETE FROM \"{}\".outbox WHERE key = $1", self.schema);
        let schema = self.schema.clone();
        let held = self.held_fence();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            tx.execute(&sql, &[&format!("{seq:020}")])?;
            tx.commit()?;
            Ok(())
        })
    }

    async fn outbox_remove_batch_blocking(&self, seqs: Vec<u64>) -> Result<()> {
        let sql = format!("DELETE FROM \"{}\".outbox WHERE key = $1", self.schema);
        let schema = self.schema.clone();
        let held = self.held_fence();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            for seq in seqs {
                tx.execute(&sql, &[&format!("{seq:020}")])?;
            }
            tx.commit()?;
            Ok(())
        })
    }

    fn outbox_len(&self) -> u64 {
        // Matches redb's `.unwrap_or(0)`: this feeds a `/status` gauge, and a gauge that fails the
        // request because the database hiccuped is worse than a gauge that reads zero.
        let sql = format!("SELECT count(*) FROM \"{}\".outbox", self.schema);
        self.conn
            .with(move |c| Ok(c.query_one(&sql, &[])?.get::<_, i64>(0) as u64))
            .unwrap_or(0)
    }

    fn outbox_trim(&self, max: u64) -> Result<u64> {
        let len = self.outbox_len();
        if len <= max {
            return Ok(0);
        }
        let drop = (len - max) as i64;
        let sql = format!(
            "DELETE FROM \"{s}\".outbox WHERE key IN \
             (SELECT key FROM \"{s}\".outbox ORDER BY key LIMIT $1)",
            s = self.schema
        );
        let schema = self.schema.clone();
        let held = self.held_fence();
        self.conn.with(move |c| {
            let mut tx = c.transaction()?;
            guard_fence_in_tx(&mut tx, &schema, held)?;
            let n = tx.execute(&sql, &[&drop])?;
            tx.commit()?;
            Ok(n)
        })
    }
}

/// Refuse the write if this handle has been fenced out (RFC-0022 slice 4).
///
/// Runs **inside the caller's transaction**, so the check and the write are one atomic decision.
/// Checking beforehand would leave a window in which the fence moves between the check and the
/// write - which is exactly the race the fence exists to close.
///
/// `FOR SHARE` on the fence row is what makes that true under READ COMMITTED: an acquire takes the
/// row `FOR UPDATE`, so it waits for this transaction to commit instead of moving the fence between
/// this read and the write it guards. Share, not update, so one holder's writes do not serialise
/// against each other.
fn guard_fence_in_tx(tx: &mut postgres::Transaction<'_>, schema: &str, held: u64) -> Result<()> {
    if held == 0 {
        return Ok(());
    }
    let sql = format!("SELECT value FROM \"{schema}\".meta WHERE key = $1 FOR SHARE");
    let current: u64 = tx
        .query_opt(&sql, &[&OWNER_FENCE])?
        .and_then(|r| r.get::<_, String>(0).parse().ok())
        .unwrap_or(0);
    if current != held {
        return Err(LostOwnership { held, current }.into());
    }
    Ok(())
}

/// Upsert one meta key inside an open transaction - shared by every mutation that must land the row
/// change and the watermark together.
/// Push `payloads` inside `tx`; returns the first one's sequence number.
fn outbox_push_in_tx(
    tx: &mut postgres::Transaction<'_>,
    schema: &str,
    payloads: &[String],
) -> Result<u64> {
    // Read-modify-write of the counter inside the transaction, with `FOR UPDATE` so two
    // writers cannot both read the same seq. The trait says single-writer, but a lost outbox
    // entry is silent and permanent, so this one is belt and braces.
    let read = format!("SELECT value FROM \"{schema}\".meta WHERE key = $1 FOR UPDATE");
    let first: u64 = tx
        .query_opt(&read, &[&OUTBOX_SEQ])?
        .and_then(|r| r.get::<_, String>(0).parse().ok())
        .unwrap_or(0);
    let push = format!("INSERT INTO \"{schema}\".outbox (key, value) VALUES ($1, $2)");
    for (seq, payload) in (first..).zip(payloads) {
        tx.execute(&push, &[&format!("{seq:020}"), payload])?;
    }
    set_meta_in_tx(
        tx,
        schema,
        OUTBOX_SEQ,
        &(first + payloads.len() as u64).to_string(),
    )?;
    Ok(first)
}

fn set_meta_in_tx(
    tx: &mut postgres::Transaction<'_>,
    schema: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    let sql = format!(
        "INSERT INTO \"{schema}\".meta (key, value) VALUES ($1, $2) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
    );
    tx.execute(&sql, &[&key, &value])?;
    Ok(())
}

/// Drop every entity and checkpoint strictly above `block`.
///
/// The entity bound is `> {block:012}-999999` rather than a parsed comparison: keys are zero-padded
/// so a lexicographic bound *is* the numeric one, which keeps this a single indexed range delete
/// instead of a full scan. redb reaches the same set by parsing each key, and the two agree exactly
/// because of the padding - which is why the collation note at the top of this file matters.
fn prune_bound_above(block: u64) -> String {
    format!("{block:012}-999999")
}

fn rollback_in_tx(tx: &mut postgres::Transaction<'_>, schema: &str, block: u64) -> Result<u64> {
    let hi = prune_bound_above(block);
    let del_entities = format!("DELETE FROM \"{schema}\".entities WHERE key > $1");
    let removed = tx.execute(&del_entities, &[&hi])?;
    let del_blocks = format!("DELETE FROM \"{schema}\".blocks WHERE key > $1");
    tx.execute(&del_blocks, &[&format!("{block:012}")])?;
    Ok(removed)
}

fn prune_in_tx(
    tx: &mut postgres::Transaction<'_>,
    schema: &str,
    from: u64,
    to: u64,
) -> Result<u64> {
    let (lo, hi) = (format!("{from:012}-000000"), format!("{to:012}-999999"));
    let sql = format!("DELETE FROM \"{schema}\".entities WHERE key >= $1 AND key <= $2");
    Ok(tx.execute(&sql, &[&lo, &hi])?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A page of megabyte documents was fetched a thousand rows at a time before any was visited,
    /// about 3 GiB for one page of QoS payloads (#1391 review). A page is cut to a byte budget, and
    /// never to no rows at all.
    #[test]
    fn a_scan_page_holds_a_byte_budget_and_never_less_than_one_row() {
        assert_eq!(rows_within(&[3 << 20; 1_000], 16 << 20), 5);
        assert_eq!(rows_within(&[64; 1_000], 16 << 20), 1_000);
        assert_eq!(rows_within(&[64 << 20, 1], 16 << 20), 1);
        assert_eq!(rows_within(&[], 16 << 20), 0);
    }

    /// Connection strings reach logs and error messages; passwords must not travel with them.
    #[test]
    fn passwords_are_redacted_from_connection_strings() {
        assert_eq!(
            redact("postgres://nuthatch:hunter2@db.internal:5432/nuthatch"),
            "postgres://***@db.internal:5432/nuthatch"
        );
        assert_eq!(
            redact("postgres://localhost:5432/nuthatch"),
            "postgres://localhost:5432/nuthatch"
        );
    }

    /// A schema name is concatenated into DDL, so the validation is the injection guard.
    #[test]
    fn schema_names_are_validated_not_escaped() {
        for bad in [
            "",
            "Nest",
            "nest-1",
            "a\"; DROP SCHEMA public CASCADE; --",
            "nest 1",
        ] {
            let err = PgStore::connect("postgres://127.0.0.1:1/none", bad)
                .err()
                .expect("must be refused before any connection is attempted");
            assert!(
                err.to_string().contains("Postgres schema"),
                "{bad} was refused for the wrong reason: {err}"
            );
        }
    }

    /// The zero-padding is what lets a lexicographic bound stand in for a numeric one.
    #[test]
    fn prune_bounds_are_lexicographically_equivalent_to_numeric_ones() {
        assert!(prune_bound_above(10).as_str() < "000000000011-000000");
        assert!(prune_bound_above(10).as_str() > "000000000010-999998");
        // The property that matters: every key of block 11 sorts above the block-10 bound.
        assert!("000000000011-000000" > prune_bound_above(10).as_str());
    }
}
