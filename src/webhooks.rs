//! User webhooks (RFC-0010 Part B): POST rows of an event table matching a predicate to a URL as they
//! seal. This is the *second producer* of the one shared delivery engine (RFC-0010 §Reconciliation) -
//! the compliance alerts (RFC-0008 C5) are the first. Both just push a `{..., url}` payload onto the
//! durable outbox in the hot store; the same background worker ([`crate::alerts::run_delivery_worker`])
//! drains it, at-least-once, and never blocks indexing. No new engine - webhooks are a producer.
//!
//! Delivery is on the **sealed** path (finality-gated: a sealed webhook never lies). Each webhook
//! carries a cursor - the highest block it has delivered - persisted in the hot store. On first
//! registration the cursor initialises per `since`: `"registration"` (default) starts it at the tip,
//! so a `--seal-direct` backfill sealing the entire history does *not* fire millions of historical
//! rows; only rows sealed after registration deliver. `"genesis"` or a block number replay history
//! deliberately.

use crate::config::Webhook;
use crate::store::HotStore;
use anyhow::Result;
use serde_json::json;

/// Default rows per delivery POST when a webhook doesn't set `batch_max`.
pub const DEFAULT_BATCH_MAX: usize = 50;

/// HMAC-SHA256 of `msg` under `key`, hex-encoded (RFC 2104). Hand-rolled over `sha2` (already a
/// dependency) rather than pulling the `hmac` crate - it's a small, well-defined construction. The
/// delivery worker signs each webhook body so the receiver can verify it came from this nest.
pub fn hmac_sha256_hex(key: &[u8], msg: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.iter().map(|b| b ^ 0x36).collect::<Vec<u8>>());
    inner.update(msg);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(k.iter().map(|b| b ^ 0x5c).collect::<Vec<u8>>());
    outer.update(inner);
    hex::encode(outer.finalize())
}

/// The per-webhook HMAC secrets, by webhook name - the delivery worker uses this to sign a payload
/// whose `webhook` field names a secret-carrying webhook.
pub fn secrets(webhooks: &[Webhook]) -> std::collections::HashMap<String, String> {
    webhooks
        .iter()
        .filter_map(|w| w.secret.clone().map(|s| (w.name.clone(), s)))
        .collect()
}

fn cursor_key(name: &str) -> String {
    format!("webhook_cursor_{name}")
}

fn partial_key(name: &str) -> String {
    format!("webhook_partial_{name}")
}

/// Whether a webhook delivers on the sealed path (the default and only path in this step; `"tip"`
/// finality lands in a later step with retraction deliveries).
fn is_sealed(w: &Webhook) -> bool {
    w.finality.as_deref().unwrap_or("sealed") == "sealed"
}

/// Initialise each webhook's cursor on first registration (no stored cursor), per its `since`. Called
/// once at startup with the current chain `tip`. `"registration"` → tip (suppresses backfill history);
/// `"genesis"` → 0; a number → that block. Idempotent: a webhook with a stored cursor is left alone.
pub fn init_cursors(store: &dyn HotStore, webhooks: &[Webhook], tip: u64) -> Result<()> {
    for w in webhooks {
        if store.get_meta(&cursor_key(&w.name))?.is_some() {
            continue;
        }
        let start = match w.since.as_deref() {
            Some("genesis") => 0,
            Some(other) if other != "registration" => other.parse::<u64>().unwrap_or(tip),
            _ => tip, // "registration" (default)
        };
        store.set_meta(&cursor_key(&w.name), &start.to_string())?;
        tracing::info!(
            "webhook '{}' registered at block {start} → {}",
            w.name,
            crate::rpc::redact_url(&w.url)
        );
    }
    Ok(())
}

/// How far webhook batches may fill the shared outbox. Half of [`crate::alerts::OUTBOX_MAX`], so
/// webhooks alone never push it to the bound where `alerts::trim` sheds entries: a webhook can wait
/// behind its cursor, a compliance alert cannot.
pub const WEBHOOK_OUTBOX_MAX: u64 = crate::alerts::OUTBOX_MAX / 2;

/// Deliver newly-sealed rows for every sealed webhook up to `sealed_to`: for each, query its table's
/// rows in `(cursor, sealed_to]` matching its `where`, enqueue them (batched) onto the outbox, and
/// advance the cursor past what was enqueued. Returns the number of deliveries enqueued. Best-effort
/// per webhook - a table with no sealed segment yet just yields nothing.
pub fn deliver_sealed(
    store: &dyn HotStore,
    dir: &std::path::Path,
    webhooks: &[Webhook],
    sealed_to: u64,
) -> Result<usize> {
    deliver_sealed_within(store, dir, webhooks, sealed_to, WEBHOOK_OUTBOX_MAX)
}

type Collectors = std::sync::Mutex<
    std::collections::HashMap<std::path::PathBuf, (u64, tokio::sync::watch::Sender<u64>)>,
>;

fn collectors() -> &'static Collectors {
    static C: std::sync::OnceLock<Collectors> = std::sync::OnceLock::new();
    C.get_or_init(Default::default)
}

/// How long a collector waits to retry a webhook that is held (an outbox with no room, a failed
/// query), when no seal comes sooner. A failing query doubles it up to [`RETRY_MAX`]; a success
/// resets it.
const RETRY: std::time::Duration = std::time::Duration::from_secs(5);
const RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(300);

/// The task that owns `dir`'s webhook outbox and cursors once the nest is running, so a seal only
/// records how far it got ([`sealed`]) and the writer never waits on a delivery query. It is the
/// only writer of those keys after startup (`init_cursors` runs before the first seal), and a
/// cursor still moves only after its rows are queued, so delivery stays in order and exactly once
/// across slices and restarts.
pub struct Collector {
    dir: std::path::PathBuf,
    id: u64,
    rx: tokio::sync::watch::Receiver<u64>,
}

/// Register a collector for `dir`, replacing one a previous mount of it left. Registered here, not
/// when the task first runs, so a seal that lands before it is polled is not missed.
pub fn collector(dir: &std::path::Path) -> Collector {
    static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (tx, rx) = tokio::sync::watch::channel(0);
    collectors()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(dir.to_path_buf(), (id, tx));
    Collector {
        dir: dir.to_path_buf(),
        id,
        rx,
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        let mut all = collectors().lock().unwrap_or_else(|p| p.into_inner());
        if all.get(&self.dir).is_some_and(|(id, _)| *id == self.id) {
            all.remove(&self.dir);
        }
    }
}

/// Record that `dir` has sealed through `sealed_to`. Returns whether a collector took it; a caller
/// with none delivers inline.
pub fn sealed(dir: &std::path::Path, sealed_to: u64) -> bool {
    let all = collectors().lock().unwrap_or_else(|p| p.into_inner());
    let Some((_, tx)) = all.get(dir) else {
        return false;
    };
    tx.send_if_modified(|through| {
        let moved = sealed_to > *through;
        if moved {
            *through = sealed_to;
        }
        moved
    });
    true
}

impl Collector {
    /// Deliver up to each sealed mark until the collector is dropped. A pass is a blocking one:
    /// the queries run on the blocking pool, bounded by [`SEAL_BUDGET`], and a pass that leaves a
    /// webhook behind runs again at once if only the budget ended it, or after [`RETRY`] if the
    /// outbox or a query held it.
    pub async fn run(
        mut self,
        store: std::sync::Arc<dyn HotStore>,
        webhooks: std::sync::Arc<Vec<Webhook>>,
    ) {
        let mut pause = None;
        let mut backoff = RETRY;
        let mut was_failing = false;
        loop {
            match pause {
                None => {
                    if self.rx.changed().await.is_err() {
                        return;
                    }
                }
                Some(pause) => {
                    tokio::select! {
                        changed = self.rx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                        () = tokio::time::sleep(pause) => {}
                    }
                }
            }
            let sealed_to = *self.rx.borrow_and_update();
            let pass = {
                let (store, dir, webhooks) = (store.clone(), self.dir.clone(), webhooks.clone());
                tokio::task::spawn_blocking(move || {
                    deliver_sealed(&store, &dir, &webhooks, sealed_to)
                })
            };
            let failed = match pass.await {
                Ok(Ok(_)) => false,
                Ok(Err(e)) => {
                    if !was_failing {
                        tracing::warn!("webhook delivery failed: {e:#}");
                    }
                    true
                }
                Err(e) => {
                    if !was_failing {
                        tracing::warn!("webhook delivery task failed: {e}");
                    }
                    true
                }
            };
            let standing = standing(&self.dir);
            let failing = failed || standing.iter().any(|(_, s)| s.last_error.is_some());
            pause = if standing.iter().any(|(_, s)| s.waiting_for_turn) {
                Some(std::time::Duration::ZERO)
            } else if failing {
                let wait = backoff;
                backoff = (backoff * 2).min(RETRY_MAX);
                Some(wait)
            } else {
                backoff = RETRY;
                standing
                    .iter()
                    .any(|(_, s)| s.backlog_blocks > 0)
                    .then_some(RETRY)
            };
            was_failing = failing;
        }
    }
}

/// Blocks one delivery query reads at most. A failing slice is halved, down to one block.
const SLICE_BLOCKS: u64 = 10_000;
/// Rows one delivery query returns at most, whatever room the outbox has.
const SLICE_ROWS: u64 = 10_000;
/// How long one delivery pass may run, across every webhook, so one pass does not hold a blocking-pool
/// thread for a whole backlog. A query is given what is left of it as its deadline, so a backlog drains
/// a slice at a time rather than timing out whole. A cut statement takes a moment to stop (about 3 s).
pub const SEAL_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// [`deliver_sealed`] with the outbox room as a parameter. A full outbox is back-pressure, never a
/// trim: the cursor stops at the last whole block enqueued and the rest is re-queried next seal.
fn deliver_sealed_within(
    store: &dyn HotStore,
    dir: &std::path::Path,
    webhooks: &[Webhook],
    sealed_to: u64,
    outbox_max: u64,
) -> Result<usize> {
    let started = std::time::Instant::now();
    let mut enqueued = 0usize;
    // `table` and `where` come from the bundle, which a tenant writes: they run as a request from a
    // mount of this dataset runs, and in a strict runtime not before one has said whose they are.
    let seat = crate::maintained::runs_as(dir);
    let sealed: Vec<&Webhook> = webhooks.iter().filter(|w| is_sealed(w)).collect();
    // Each run starts one webhook further on, so one that spends the whole budget cannot starve the rest.
    let first = next_turn(dir, sealed.len());
    for w in sealed[first..].iter().chain(&sealed[..first]).copied() {
        let cursor = |store: &dyn HotStore| -> Result<u64> {
            Ok(store
                .get_meta(&cursor_key(&w.name))?
                .and_then(|s| s.parse().ok())
                .unwrap_or(sealed_to))
        };
        let Some(seat) = &seat else {
            note(
                dir,
                w,
                cursor(store)?,
                sealed_to,
                Some("no mount has seated this dataset yet"),
                false,
            );
            continue;
        };
        let mut span = SLICE_BLOCKS;
        let mut error = None;
        let mut waiting = false;
        loop {
            let from = cursor(store)?;
            let left = SEAL_BUDGET.saturating_sub(started.elapsed());
            if from >= sealed_to {
                break;
            }
            if left.is_zero() {
                waiting = true;
                break;
            }
            let room = outbox_max.saturating_sub(store.outbox_len());
            if room == 0 {
                tracing::debug!(
                    "webhook '{}' held at block {from}: outbox full ({outbox_max})",
                    w.name
                );
                break;
            }
            let batch_max = w.batch_max.unwrap_or(DEFAULT_BATCH_MAX).max(1);
            let limit = room.saturating_mul(batch_max as u64).min(SLICE_ROWS);
            // A block with more rows than the room is queued in pieces across passes: the cursor
            // stays below it and this records how far into it we are.
            let partial = store.get_meta(&partial_key(&w.name))?.and_then(|s| {
                let (b, p) = s.split_once(':')?;
                Some((b.parse::<u64>().ok()?, p.parse::<u64>().ok()?))
            });
            let (lo, hi, offset) = match partial {
                Some((b, pos)) => (b - 1, b, pos),
                None => (from, sealed_to.min(from.saturating_add(span)), 0),
            };
            let rows = slice(dir, w, seat.as_ref(), (lo, hi, offset), limit, left);
            let mut rows = match rows {
                Ok(r) => {
                    error = None;
                    r
                }
                Err(e) if partial.is_none() && span > 1 => {
                    span /= 2;
                    error = Some(format!("{e:#}"));
                    continue;
                }
                Err(e) => {
                    error = Some(format!("{e:#}"));
                    break;
                }
            };
            let mut through = hi;
            let mut next_partial = None;
            if let Some((b, pos)) = partial {
                through = b;
                if rows.len() as u64 >= limit {
                    next_partial = Some((b, pos + rows.len() as u64));
                }
            } else if rows.len() as u64 >= limit {
                // The limit may have cut the last block short, and the cursor is per block: keep only
                // whole blocks. If one block alone fills the room, queue what fits and come back.
                let first = row_block(&rows[0])?;
                let last = row_block(&rows[rows.len() - 1])?;
                if first == last {
                    next_partial = Some((first, rows.len() as u64));
                } else {
                    rows.retain(|r| row_block(r).is_ok_and(|b| b < last));
                    through = last - 1;
                }
            }
            for chunk in rows.chunks(batch_max) {
                let payload = json!({
                    "event": "rows",
                    "webhook": w.name,
                    "url": w.url,
                    "finality": "sealed",
                    "table": w.table,
                    "count": chunk.len(),
                    "rows": chunk,
                });
                store.outbox_push(&payload.to_string())?;
                enqueued += 1;
            }
            match next_partial {
                Some((b, pos)) => store.set_meta(&partial_key(&w.name), &format!("{b}:{pos}"))?,
                None => {
                    store.set_meta(&cursor_key(&w.name), &through.to_string())?;
                    if partial.is_some() {
                        store.set_meta(&partial_key(&w.name), "")?;
                    }
                }
            }
            if !rows.is_empty() {
                tracing::info!(
                    "webhook '{}': {} row(s) sealed in ({from}, {through}] → queued",
                    w.name,
                    rows.len()
                );
            }
        }
        if let Some(e) = &error {
            tracing::debug!("webhook '{}' query failed: {e}", w.name);
        }
        note(dir, w, cursor(store)?, sealed_to, error.as_deref(), waiting);
    }
    Ok(enqueued)
}

/// One slice's rows, `(lo, hi]` from `offset`, as a request from the seated mount, or with the
/// default guard outside a seat, and never past `left`.
fn slice(
    dir: &std::path::Path,
    w: &Webhook,
    seat: Option<&crate::maintained::Seat>,
    (lo, hi, offset): (u64, u64, u64),
    limit: u64,
    left: std::time::Duration,
) -> Result<Vec<serde_json::Value>> {
    let predicate = w
        .where_clause
        .as_deref()
        .map(|p| format!(" AND ({p}\n)"))
        .unwrap_or_default();
    // The newline ends a `--` in `where` before the bounds, so a comment cannot drop them.
    let sql = format!(
        "SELECT * FROM {} WHERE block_number > {lo} AND block_number <= {hi}{predicate} \
         ORDER BY block_number, log_index LIMIT {limit} OFFSET {offset}",
        quoted(&w.table),
    );
    let base = seat.map_or_else(crate::analytics::QueryGuard::default, |s| s.guard);
    // The row cap is the LIMIT, which an answer of exactly that many rows is not cut at, and the
    // byte cap is off: a cut answer would move the cursor past rows never queued.
    let guard = crate::analytics::QueryGuard {
        timeout: base.timeout.min(left),
        max_rows: usize::try_from(limit).unwrap_or(usize::MAX),
        max_result_bytes: usize::MAX,
        ..base
    };
    let out = crate::analytics::isolated(seat.map(|s| &s.tenant), || {
        crate::analytics::query_guarded(dir, &sql, guard)
    })?;
    anyhow::ensure!(!out.truncated, "the answer was cut short");
    Ok(out.rows)
}

/// Where a webhook stands, as `/ready` and `/metrics` show it.
#[derive(Clone, Debug, Default)]
pub struct Standing {
    pub delivered_through: u64,
    pub backlog_blocks: u64,
    pub last_error: Option<String>,
    pub failures: u64,
    /// The seal's budget was spent before this webhook's turn finished; it goes earlier next time.
    pub waiting_for_turn: bool,
}

fn standings(
) -> &'static std::sync::Mutex<std::collections::BTreeMap<(std::path::PathBuf, String), Standing>> {
    static S: std::sync::OnceLock<
        std::sync::Mutex<std::collections::BTreeMap<(std::path::PathBuf, String), Standing>>,
    > = std::sync::OnceLock::new();
    S.get_or_init(Default::default)
}

fn note(
    dir: &std::path::Path,
    w: &Webhook,
    through: u64,
    sealed_to: u64,
    error: Option<&str>,
    waiting: bool,
) {
    let mut all = standings().lock().unwrap_or_else(|p| p.into_inner());
    let s = all.entry((dir.to_path_buf(), w.name.clone())).or_default();
    if error.is_some() && s.last_error.is_none() {
        tracing::warn!(
            "webhook '{}' is held at block {through}: {}",
            w.name,
            error.unwrap_or_default()
        );
    }
    if error.is_none() && s.last_error.is_some() {
        tracing::info!(
            "webhook '{}' is delivering again from block {through}",
            w.name
        );
    }
    if error.is_some() {
        s.failures += 1;
    }
    s.delivered_through = through;
    s.backlog_blocks = sealed_to.saturating_sub(through);
    s.last_error = error.map(str::to_string);
    s.waiting_for_turn = waiting;
}

fn next_turns() -> &'static std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, usize>> {
    static NEXT: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, usize>>,
    > = std::sync::OnceLock::new();
    NEXT.get_or_init(Default::default)
}

/// The index this run of `dir`'s `n` webhooks starts at, one on from the last run's.
fn next_turn(dir: &std::path::Path, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let mut all = next_turns().lock().unwrap_or_else(|p| p.into_inner());
    let next = all.entry(dir.to_path_buf()).or_default();
    let first = *next % n;
    *next = first + 1;
    first
}

/// Each webhook of `dir` by name, as its last delivery left it; empty before the first.
pub fn standing(dir: &std::path::Path) -> Vec<(String, Standing)> {
    standings()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|((d, _), _)| d == dir)
        .map(|((_, name), s)| (name.clone(), s.clone()))
        .collect()
}

/// `/metrics` lines for the webhooks of each nest in `nests` (its metrics label and dataset), the
/// same `nest=` label the neighbouring series use, so two nests' webhooks of one name stay apart.
pub fn render_metrics(nests: &[(String, std::path::PathBuf)]) -> String {
    let all = standings().lock().unwrap_or_else(|p| p.into_inner());
    let rows: Vec<(&str, &str, &Standing)> = nests
        .iter()
        .flat_map(|(nest, dir)| {
            all.range((dir.clone(), String::new())..)
                .take_while(move |((d, _), _)| d == dir)
                .map(move |((_, name), s)| (nest.as_str(), name.as_str(), s))
        })
        .collect();
    if rows.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for (metric, help, kind, pick) in [
        (
            "nuthatch_webhook_backlog_blocks",
            "Sealed blocks a webhook has yet to deliver.",
            "gauge",
            (|s: &Standing| s.backlog_blocks) as fn(&Standing) -> u64,
        ),
        (
            "nuthatch_webhook_failures_total",
            "Delivery runs that ended on a failed query.",
            "counter",
            |s| s.failures,
        ),
    ] {
        out.push_str(&format!("# HELP {metric} {help}\n# TYPE {metric} {kind}\n"));
        for (nest, name, s) in &rows {
            out.push_str(&format!(
                "{metric}{{nest=\"{}\",webhook=\"{}\"}} {}\n",
                crate::metrics::label_value(nest),
                crate::metrics::label_value(name),
                pick(s)
            ));
        }
    }
    out
}

/// Drop what a departed dataset's webhooks left behind: their standings and their place in the rota.
pub fn forget(dir: &std::path::Path) {
    standings()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .retain(|(d, _), _| d != dir);
    next_turns()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(dir);
}

/// A relation name as a SQL identifier, whatever characters an alias gave it.
fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn row_block(row: &serde_json::Value) -> Result<u64> {
    let v = &row["block_number"];
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| anyhow::anyhow!("webhook row has no block_number: {v}"))
}

/// Refuse a webhook whose `table` or `where` does not bind against the nest's declared schema (#2365).
/// The delivery query would fail the same way on every seal, and the webhook would never fire.
/// Under `strict`, a `where` calling a function strict isolation does not allow is refused here too.
pub fn refuse_unbound(
    dir: &std::path::Path,
    declared: &[crate::registry::TableSchema],
    webhooks: &[Webhook],
    strict: bool,
) -> Result<()> {
    let plan = |sql: &str| {
        let guard = crate::analytics::QueryGuard {
            timeout: std::time::Duration::from_secs(10),
            max_rows: 1,
            ..Default::default()
        };
        crate::analytics::plan_hot_cold(
            dir,
            sql,
            guard,
            &crate::analytics::HotRows::new(),
            u64::MAX,
            declared,
        )
    };
    let relations: std::collections::BTreeSet<String> = declared
        .iter()
        .map(|t| t.table.clone())
        .chain(
            crate::analytics::nest_view_files(dir)
                .iter()
                .flat_map(|f| crate::analytics::split_sql_statements(&f.sql))
                .filter_map(|s| crate::analytics::view_name(&s)),
        )
        .collect();
    for w in webhooks {
        if !relations.contains(&w.table) {
            anyhow::bail!(
                "webhook '{}': `table = \"{}\"` is not a table or view this nest declares",
                w.name,
                w.table
            );
        }
        let base = format!(
            "SELECT * FROM {} WHERE block_number > 0 ORDER BY block_number, log_index",
            quoted(&w.table)
        );
        if let Err(e) = plan(&base) {
            anyhow::bail!(
                "webhook '{}': `table = \"{}\"` is not a table this nest can deliver from: {e:#}",
                w.name,
                w.table
            );
        }
        if let Some(p) = &w.where_clause {
            let sql = format!(
                "SELECT * FROM {} WHERE block_number > 0 AND ({p}\n) ORDER BY block_number, log_index",
                quoted(&w.table)
            );
            if let Err(e) = plan(&sql) {
                anyhow::bail!(
                    "webhook '{}': `where = \"{p}\"` does not bind against `{}`: {e:#}",
                    w.name,
                    w.table
                );
            }
            if strict {
                if let Err(e) = crate::isolation::refuse_unlisted_functions(&sql) {
                    anyhow::bail!("webhook '{}': `where = \"{p}\"` is refused: {e:#}", w.name);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    /// HMAC-SHA256 known-answer (the standard RFC test vector), so the hand-rolled construction is
    /// provably correct.
    #[test]
    fn hmac_matches_the_reference_vector() {
        let got = hmac_sha256_hex(b"key", b"The quick brown fox jumps over the lazy dog");
        assert_eq!(
            got,
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    fn wh(name: &str, table: &str, since: &str) -> Webhook {
        Webhook {
            name: name.into(),
            table: table.into(),
            where_clause: None,
            url: "http://127.0.0.1:9/hook".into(),
            batch_max: None,
            finality: None,
            since: Some(since.into()),
            secret: None,
        }
    }

    /// The RFC-0010 correctness catch: a `since = "registration"` webhook registered at the tip does
    /// NOT fire for a `--seal-direct` backfill (history sealed below the cursor), but DOES fire for a
    /// row sealed after registration.
    #[test]
    fn registration_since_suppresses_backfill_history() {
        let dir = tempfile::tempdir().unwrap();
        // The two seals below fold into one segment under the table floor (#1150), which is the
        // shape that aborted the dev-profile DuckDB before `analytics` switched off compressed
        // materialisation in debug builds (#1152). This test runs on one folded file on purpose:
        // it is the regression test for that guard as much as for the cursor.
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let webhooks = vec![wh("w", "t__transfer", "registration")];
        // Registered at tip 100.
        init_cursors(&store, &webhooks, 100).unwrap();

        // Seal a t__transfer segment covering historical blocks 10..=50 (below the cursor).
        let rows: Vec<String> = (10..=50)
            .map(|b| format!(r#"{{"table":"t__transfer","from":"0xa","to":"0xb","value":"1","block_number":{b},"log_index":0,"tx_hash":"0xt"}}"#))
            .collect();
        crate::seal::seal_range(dir.path(), &rows, 10, 50).unwrap();

        // A backfill that sealed up to block 50 delivers nothing (all history < the registration tip).
        let n = deliver_sealed(&store, dir.path(), &webhooks, 50).unwrap();
        assert_eq!(
            n, 0,
            "registration cursor suppresses historical backfill rows"
        );
        assert_eq!(store.outbox_len(), 0);

        // Now a row seals at block 105 (after registration) → it delivers.
        let post = vec![r#"{"table":"t__transfer","from":"0xa","to":"0xb","value":"1","block_number":105,"log_index":0,"tx_hash":"0xt"}"#.to_string()];
        crate::seal::seal_range(dir.path(), &post, 101, 105).unwrap();
        let n = deliver_sealed(&store, dir.path(), &webhooks, 105).unwrap();
        assert_eq!(n, 1, "a row sealed after registration delivers");
        assert_eq!(store.outbox_len(), 1);
    }

    /// `since = "genesis"` replays history; `where` filters; `batch_max` chunks the deliveries.
    #[test]
    fn genesis_replays_history_with_where_and_batching() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut w = wh("big", "t__transfer", "genesis");
        w.where_clause = Some("CAST(value AS HUGEINT) >= 100".into());
        w.batch_max = Some(2);
        let webhooks = vec![w];
        init_cursors(&store, &webhooks, 1000).unwrap();

        // Five rows; three match the predicate (value ≥ 100).
        let rows: Vec<String> = [("10", "5"), ("11", "200"), ("12", "50"), ("13", "300"), ("14", "150")]
            .iter()
            .map(|(b, v)| format!(r#"{{"table":"t__transfer","from":"0xa","to":"0xb","value":"{v}","block_number":{b},"log_index":0,"tx_hash":"0xt"}}"#))
            .collect();
        crate::seal::seal_range(dir.path(), &rows, 10, 14).unwrap();

        let n = deliver_sealed(&store, dir.path(), &webhooks, 14).unwrap();
        // 3 matching rows, batched by 2 → 2 deliveries (a batch of 2 and a batch of 1).
        assert_eq!(n, 2, "3 matching rows batched by 2 → 2 outbox deliveries");
        assert_eq!(store.outbox_len(), 2);
        // Cursor advanced, so a re-run delivers nothing more.
        assert_eq!(
            deliver_sealed(&store, dir.path(), &webhooks, 14).unwrap(),
            0
        );
    }

    fn transfer(block: u64, log_index: u64) -> String {
        format!(
            r#"{{"table":"t__transfer","from":"0xa","to":"0xb","value":"1","block_number":{block},"log_index":{log_index},"tx_hash":"0xt"}}"#
        )
    }

    /// Drain the outbox as the delivery worker would, returning each row's (block, log_index).
    fn drain(store: &Store) -> Vec<(u64, u64)> {
        let mut out = vec![];
        for (seq, p) in store.outbox_pending(usize::MAX).unwrap() {
            let v: serde_json::Value = serde_json::from_str(&p).unwrap();
            for r in v["rows"].as_array().into_iter().flatten() {
                out.push((row_block(r).unwrap(), r["log_index"].as_u64().unwrap()));
            }
            store.outbox_remove(seq).unwrap();
        }
        out
    }

    /// #2359: a webhook with more history than the outbox holds is back-pressured, not trimmed. Every
    /// row arrives exactly once and the outbox never passes its bound.
    #[test]
    fn a_full_outbox_holds_the_cursor_and_loses_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut w = wh("g", "t__transfer", "genesis");
        w.batch_max = Some(1);
        let webhooks = vec![w];
        init_cursors(&store, &webhooks, 1000).unwrap();
        let rows: Vec<String> = (1..=7).map(|b| transfer(b, 0)).collect();
        crate::seal::seal_range(dir.path(), &rows, 1, 7).unwrap();

        let mut got = vec![];
        for _ in 0..10 {
            deliver_sealed_within(&store, dir.path(), &webhooks, 7, 3).unwrap();
            assert!(
                store.outbox_len() <= 3,
                "the outbox stayed within its bound"
            );
            got.extend(drain(&store));
        }
        assert_eq!(got, (1..=7).map(|b| (b, 0)).collect::<Vec<_>>());
    }

    /// A compliance alert already queued is never shed by a webhook filling the outbox.
    #[test]
    fn a_webhook_never_sheds_a_queued_alert() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut w = wh("g", "t__transfer", "genesis");
        w.batch_max = Some(1);
        let webhooks = vec![w];
        init_cursors(&store, &webhooks, 1000).unwrap();
        let rows: Vec<String> = (1..=7).map(|b| transfer(b, 0)).collect();
        crate::seal::seal_range(dir.path(), &rows, 1, 7).unwrap();
        store.outbox_push(r#"{"event":"flag","kind":"k"}"#).unwrap();

        deliver_sealed_within(&store, dir.path(), &webhooks, 7, 3).unwrap();
        let pending = store.outbox_pending(usize::MAX).unwrap();
        assert!(pending.len() <= 3);
        assert_eq!(pending[0].1, r#"{"event":"flag","kind":"k"}"#);
    }

    /// One block with more matching rows than the webhook's share of the outbox is queued in pieces
    /// across passes: the outbox never passes that share, so the alert trim above it never fires and a
    /// queued alert survives, every row arrives once and in order, and the cursor still reaches it.
    #[test]
    fn a_block_larger_than_the_room_is_queued_in_pieces_and_sheds_no_alert() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut w = wh("g", "t__transfer", "genesis");
        w.batch_max = Some(1);
        let webhooks = vec![w];
        init_cursors(&store, &webhooks, 1000).unwrap();
        let mut rows: Vec<String> = (0..10).map(|i| transfer(5, i)).collect();
        rows.push(transfer(6, 0));
        crate::seal::seal_range(dir.path(), &rows, 5, 6).unwrap();
        let alert = r#"{"event":"flag","kind":"k"}"#;
        store.outbox_push(alert).unwrap();

        let mut got = vec![];
        for _ in 0..20 {
            deliver_sealed_within(&store, dir.path(), &webhooks, 6, 3).unwrap();
            assert!(
                store.outbox_len() <= 3,
                "the outbox stayed within the webhook's share"
            );
            // The alert path's own bound, twice the webhook share as in production.
            assert_eq!(store.outbox_trim(6).unwrap(), 0, "nothing was shed");
            for (seq, p) in store.outbox_pending(usize::MAX).unwrap() {
                let v: serde_json::Value = serde_json::from_str(&p).unwrap();
                for r in v["rows"].as_array().into_iter().flatten() {
                    got.push((row_block(r).unwrap(), r["log_index"].as_u64().unwrap()));
                }
                if v.get("webhook").is_some() {
                    store.outbox_remove(seq).unwrap();
                }
            }
        }
        let mut want: Vec<(u64, u64)> = (0..10).map(|i| (5, i)).collect();
        want.push((6, 0));
        assert_eq!(got, want);
        assert_eq!(store.outbox_pending(usize::MAX).unwrap()[0].1, alert);
        assert_eq!(
            store.get_meta("webhook_cursor_g").unwrap().as_deref(),
            Some("6")
        );
    }

    /// #2365: a webhook whose `table` or `where` cannot bind is refused at registration, by field,
    /// before any segment exists, instead of failing at debug on every seal.
    #[test]
    fn an_unbound_table_or_where_is_refused_by_field() {
        let dir = tempfile::tempdir().unwrap();
        let mut columns = crate::registry::implicit_columns(false);
        columns.push(crate::registry::ColumnSchema {
            name: "value".into(),
            sol_type: "uint256".into(),
            storage: "word32".into(),
            indexed: false,
            components: Vec::new(),
        });
        let declared = vec![crate::registry::TableSchema {
            table: "t__transfer".into(),
            alias: "t".into(),
            kind: crate::registry::TableKind::Event,
            event: "Transfer".into(),
            topic0: "0x".into(),
            function: String::new(),
            selector: String::new(),
            columns,
        }];
        let mut good = wh("w", "t__transfer", "registration");
        good.where_clause = Some("CAST(value AS HUGEINT) > 100".into());
        refuse_unbound(dir.path(), &declared, &[good], false)
            .expect("a webhook that binds is accepted");
        let mut derived = wh("w", "t__transfer", "registration");
        derived.where_clause = Some("value_dec > 1000000".into());
        refuse_unbound(dir.path(), &declared, &[derived], false)
            .expect("a derived `_dec` column binds");

        let err = format!(
            "{:#}",
            refuse_unbound(
                dir.path(),
                &declared,
                &[wh("w", "t__transfr", "registration")],
                false
            )
            .unwrap_err()
        );
        assert!(err.contains("`table = \"t__transfr\"`"), "{err}");

        let mut bad = wh("w", "t__transfer", "registration");
        bad.where_clause = Some("vlaue > 100".into());
        let err = format!(
            "{:#}",
            refuse_unbound(dir.path(), &declared, &[bad], false).unwrap_err()
        );
        assert!(err.contains("`where = \"vlaue > 100\"`"), "{err}");

        let err = format!(
            "{:#}",
            refuse_unbound(
                dir.path(),
                &declared,
                &[wh("w", "t__transfer\" AS a, \"t__transfer", "registration")],
                false
            )
            .unwrap_err()
        );
        assert!(
            err.contains("not a table or view this nest declares"),
            "{err}"
        );

        let mut commented = wh("w", "t__transfer", "registration");
        commented.where_clause = Some("1=1) --".into());
        assert!(
            refuse_unbound(dir.path(), &declared, &[commented], false).is_err(),
            "a `where` that comments out the statement's bounds is refused"
        );
    }

    /// A `where` the SQL screen cannot parse is refused at load with the parser's message; one in
    /// the engine's own syntax is read and accepted.
    #[test]
    fn a_where_the_screen_cannot_parse_is_refused_at_load_with_the_parsers_message() {
        let dir = tempfile::tempdir().unwrap();
        let declared = transfer_schema("t__transfer");
        let mut engine_only = wh("w", "t__transfer", "registration");
        engine_only.where_clause = Some("CAST(value AS VARCHAR) ^@ '1'".into());
        refuse_unbound(dir.path(), &declared, &[engine_only], false)
            .expect("engine syntax is read by the screen");
        let mut broken = wh("w", "t__transfer", "registration");
        broken.where_clause = Some("value >".into());
        let err = format!(
            "{:#}",
            refuse_unbound(dir.path(), &declared, &[broken], false).unwrap_err()
        );
        assert!(
            err.contains("`where = \"value >\"`") && err.contains("Parser Error"),
            "{err}"
        );
    }

    fn transfer_schema(table: &str) -> Vec<crate::registry::TableSchema> {
        let mut columns = crate::registry::implicit_columns(false);
        columns.push(crate::registry::ColumnSchema {
            name: "value".into(),
            sol_type: "uint256".into(),
            storage: "word32".into(),
            indexed: false,
            components: Vec::new(),
        });
        vec![crate::registry::TableSchema {
            table: table.into(),
            alias: table.split("__").next().unwrap().into(),
            kind: crate::registry::TableKind::Event,
            event: "Transfer".into(),
            topic0: "0x".into(),
            function: String::new(),
            selector: String::new(),
            columns,
        }]
    }

    /// An alias is validated as one path segment, so a declared table may carry a hyphen. The declared
    /// check admits it; what still refuses it is the SQL surface, which since 5.11 this check has
    /// planned against and which reads a quoted name with a hyphen as a path.
    #[test]
    fn a_hyphenated_alias_is_declared_and_refused_only_by_the_sql_surface() {
        let dir = tempfile::tempdir().unwrap();
        let declared = transfer_schema("usd-c__transfer");
        let err = format!(
            "{:#}",
            refuse_unbound(
                dir.path(),
                &declared,
                &[wh("w", "usd-c__transfer", "genesis")],
                false
            )
            .unwrap_err()
        );
        assert!(!err.contains("this nest declares"), "{err}");
        assert!(
            err.contains("not a table this nest can deliver from"),
            "{err}"
        );
    }

    fn strict_nest(dir: &std::path::Path) {
        crate::maintained::attach(dir, crate::serve::SqlGate::new(2), true);
    }

    fn seat(dir: &std::path::Path, timeout: std::time::Duration) {
        let guard = crate::analytics::QueryGuard {
            timeout,
            ..Default::default()
        };
        crate::maintained::seat(dir, "acme/w", std::sync::Arc::from("acme"), guard);
    }

    /// A strict runtime runs a tenant's delivery query as that tenant's request, so nothing runs before
    /// a mount says whose it is, and the cursor waits for it.
    #[test]
    fn a_strict_delivery_waits_for_a_seat() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let webhooks = vec![wh("w", "t__transfer", "genesis")];
        init_cursors(&store, &webhooks, 1000).unwrap();
        crate::seal::seal_range(dir.path(), &[transfer(5, 0)], 5, 5).unwrap();
        strict_nest(dir.path());
        assert_eq!(deliver_sealed(&store, dir.path(), &webhooks, 5).unwrap(), 0);
        seat(dir.path(), std::time::Duration::from_secs(30));
        assert_eq!(deliver_sealed(&store, dir.path(), &webhooks, 5).unwrap(), 1);
    }

    /// A seated answer that fills the room exactly is whole, so the cursor moves and nothing is lost.
    #[test]
    fn a_seated_delivery_that_fills_the_room_loses_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut w = wh("g", "t__transfer", "genesis");
        w.batch_max = Some(1);
        let webhooks = vec![w];
        init_cursors(&store, &webhooks, 1000).unwrap();
        let rows: Vec<String> = (1..=7).map(|b| transfer(b, 0)).collect();
        crate::seal::seal_range(dir.path(), &rows, 1, 7).unwrap();
        strict_nest(dir.path());
        seat(dir.path(), std::time::Duration::from_secs(30));
        let mut got = vec![];
        for _ in 0..10 {
            deliver_sealed_within(&store, dir.path(), &webhooks, 7, 3).unwrap();
            got.extend(drain(&store));
        }
        assert_eq!(got, (1..=7).map(|b| (b, 0)).collect::<Vec<_>>());
    }

    /// Under strict a `where` may call only what a tenant's `/sql` may.
    #[test]
    fn a_strict_delivery_refuses_a_function_off_the_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut w = wh("w", "t__transfer", "genesis");
        w.where_clause = Some("version() IS NOT NULL".into());
        let webhooks = vec![w];
        init_cursors(&store, &webhooks, 1000).unwrap();
        crate::seal::seal_range(dir.path(), &[transfer(5, 0)], 5, 5).unwrap();
        strict_nest(dir.path());
        seat(dir.path(), std::time::Duration::from_secs(30));
        assert_eq!(deliver_sealed(&store, dir.path(), &webhooks, 5).unwrap(), 0);
        assert_eq!(store.outbox_len(), 0);
    }

    /// A delivery is held to its seat's deadline, so an expensive `where` gives the cursor back
    /// rather than holding it until the statement ends, and the cursor stays where it was.
    #[test]
    fn a_delivery_is_cut_at_its_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut w = wh("w", "t__transfer", "genesis");
        w.where_clause = Some(
            "(SELECT sum(a.range * b.range) FROM range(1000000) a CROSS JOIN range(1000000) b) > 0"
                .into(),
        );
        let webhooks = vec![w];
        init_cursors(&store, &webhooks, 1000).unwrap();
        crate::seal::seal_range(dir.path(), &[transfer(5, 0)], 5, 5).unwrap();
        strict_nest(dir.path());
        seat(dir.path(), std::time::Duration::from_millis(300));
        let (tx, rx) = std::sync::mpsc::channel();
        let d = dir.path().to_path_buf();
        std::thread::spawn(move || {
            let _ = tx.send(deliver_sealed(&store, &d, &webhooks, 5).map(|n| (n, store)));
        });
        let started = std::time::Instant::now();
        let (n, store) = rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the delivery outlived its deadline")
            .unwrap();
        assert!(
            started.elapsed() < SEAL_BUDGET + std::time::Duration::from_secs(2),
            "the seal hook was held {:?}",
            started.elapsed()
        );
        assert_eq!(n, 0);
        assert_eq!(
            store.get_meta(&cursor_key("w")).unwrap().as_deref(),
            Some("0")
        );
        let (name, s) = standing(dir.path()).pop().expect("a standing");
        assert_eq!(
            (name.as_str(), s.delivered_through, s.backlog_blocks),
            ("w", 0, 5)
        );
        assert!(s.last_error.is_some() && s.failures == 1, "{s:?}");
        let nests = [("n".to_string(), dir.path().to_path_buf())];
        assert!(
            render_metrics(&nests)
                .contains("nuthatch_webhook_backlog_blocks{nest=\"n\",webhook=\"w\"}"),
            "{}",
            render_metrics(&nests)
        );
    }

    /// Under strict a `where` that calls a function strict isolation refuses is a configuration
    /// error, refused at load by name, not found on every seal.
    #[test]
    fn a_strict_where_off_the_list_is_refused_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let declared = transfer_schema("t__transfer");
        let mut w = wh("w", "t__transfer", "genesis");
        w.where_clause = Some("version() IS NOT NULL".into());
        refuse_unbound(dir.path(), &declared, std::slice::from_ref(&w), false)
            .expect("allowed outside strict");
        let err = format!(
            "{:#}",
            refuse_unbound(dir.path(), &declared, &[w], true).unwrap_err()
        );
        assert!(
            err.contains("webhook 'w'") && err.contains("version"),
            "{err}"
        );
    }

    /// A webhook that spends the whole budget is not served first every run, so one behind it is
    /// delivered within two runs and, while it waits, says so.
    #[test]
    fn a_webhook_behind_one_that_spends_the_budget_gets_its_turn() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut slow = wh("slow", "t__transfer", "genesis");
        slow.where_clause = Some(
            "(SELECT sum(a.range * b.range) FROM range(1000000) a CROSS JOIN range(1000000) b) > 0"
                .into(),
        );
        let webhooks = vec![slow, wh("cheap", "t__transfer", "genesis")];
        init_cursors(&store, &webhooks, 1000).unwrap();
        crate::seal::seal_range(dir.path(), &[transfer(5, 0)], 5, 5).unwrap();
        let cheap = || {
            standing(dir.path())
                .into_iter()
                .find(|(n, _)| n == "cheap")
                .map(|(_, s)| s)
        };
        let mut delivered = 0;
        for _ in 0..2 {
            delivered += deliver_sealed(&store, dir.path(), &webhooks, 5).unwrap();
            if delivered == 0 {
                let s = cheap().expect("a waiting webhook has a standing");
                assert!(s.waiting_for_turn && s.backlog_blocks == 5, "{s:?}");
            }
        }
        assert_eq!(delivered, 1, "the cheap webhook was starved");
        let s = cheap().unwrap();
        assert!(!s.waiting_for_turn && s.backlog_blocks == 0, "{s:?}");
    }

    /// A backlog wider than one slice drains in slices, each advancing the cursor, within one seal.
    #[test]
    fn a_wide_backlog_drains_a_slice_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let webhooks = vec![wh("w", "t__transfer", "genesis")];
        init_cursors(&store, &webhooks, 1000).unwrap();
        let blocks = [5, SLICE_BLOCKS + 5, 2 * SLICE_BLOCKS + 5];
        let rows: Vec<String> = blocks.iter().map(|b| transfer(*b, 0)).collect();
        crate::seal::seal_range(dir.path(), &rows, 5, 3 * SLICE_BLOCKS).unwrap();
        assert_eq!(
            deliver_sealed(&store, dir.path(), &webhooks, 3 * SLICE_BLOCKS).unwrap(),
            3
        );
        assert_eq!(
            drain(&store),
            blocks.iter().map(|b| (*b, 0)).collect::<Vec<_>>()
        );
        let (_, s) = standing(dir.path()).pop().unwrap();
        assert_eq!(
            (s.delivered_through, s.backlog_blocks),
            (3 * SLICE_BLOCKS, 0)
        );
        assert_eq!(s.last_error, None);
    }

    /// Open the store, start a collector for `dir` and run it until `through` is delivered, with the
    /// outbox drained as the delivery worker would. Returns what was drained, in order, and lets go
    /// of the store as an unmount does.
    async fn collect(dir: &std::path::Path, webhooks: &[Webhook], through: u64) -> Vec<(u64, u64)> {
        let store = std::sync::Arc::new(Store::open(&dir.join("t.redb")).unwrap());
        let task = tokio::spawn(collector(dir).run(
            store.clone() as std::sync::Arc<dyn HotStore>,
            std::sync::Arc::new(webhooks.to_vec()),
        ));
        assert!(sealed(dir, through), "a collector is registered");
        let mut got = vec![];
        let started = std::time::Instant::now();
        while store
            .get_meta(&cursor_key(&webhooks[0].name))
            .unwrap()
            .and_then(|s| s.parse::<u64>().ok())
            != Some(through)
            || store.outbox_len() > 0
        {
            got.extend(drain(&store));
            assert!(
                started.elapsed() < std::time::Duration::from_secs(120),
                "stuck after {} rows",
                got.len()
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        task.abort();
        let _ = task.await;
        got
    }

    /// Delivery is the collector's, not the caller's: `sealed` returns at once, rows arrive in order
    /// and exactly once across slices, a block wider than a slice, and a store reopened between runs.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_collector_delivers_in_order_exactly_once_across_slices_and_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let webhooks = vec![wh("w", "t__transfer", "genesis")];
        {
            let store = Store::open(&dir.path().join("t.redb")).unwrap();
            init_cursors(&store, &webhooks, 0).unwrap();
        }
        // 12,000 rows in one block, and 5,015 more one to a block across 35,000 blocks.
        let wide = 15_000u64;
        let mut want: Vec<(u64, u64)> = (1..=5_015u64).map(|i| (i * 6 + 1, 0)).collect();
        want.extend((0..12_000u64).map(|i| (wide, i)));
        want.sort();
        let rows: Vec<String> = want.iter().map(|(b, l)| transfer(*b, *l)).collect();
        crate::seal::seal_range(dir.path(), &rows, 1, 35_000).unwrap();

        let first = collect(dir.path(), &webhooks, 17_500).await;
        let second = collect(dir.path(), &webhooks, 35_000).await;
        let mut got = first;
        got.extend(second);
        assert_eq!(got.len(), want.len(), "every row once");
        assert!(got == want, "in order, none repeated or skipped");
        assert!(
            standing(dir.path())
                .iter()
                .all(|(_, s)| s.backlog_blocks == 0),
            "{:?}",
            standing(dir.path())
        );
    }

    /// With no collector registered a seal is delivered by the caller, as it was before.
    #[test]
    fn a_seal_with_no_collector_is_left_to_the_caller() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!sealed(dir.path(), 10));
        let c = collector(dir.path());
        assert!(sealed(dir.path(), 10));
        drop(c);
        assert!(
            !sealed(dir.path(), 11),
            "a dropped collector is deregistered"
        );
    }

    /// A webhook held by a failing query is retried after 5 s, 10 s, 20 s and so on up to 5 minutes,
    /// not every 5 s, and delivers again once it can.
    #[tokio::test(start_paused = true)]
    async fn a_failing_webhook_backs_off_and_then_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(Store::open(&dir.path().join("t.redb")).unwrap());
        let webhooks = vec![wh("w", "t__transfer", "genesis")];
        init_cursors(store.as_ref(), &webhooks, 1000).unwrap();
        crate::seal::seal_range(dir.path(), &[transfer(5, 0)], 5, 5).unwrap();
        // Unseated under strict isolation, so every pass fails until a mount seats it.
        strict_nest(dir.path());
        let task = tokio::spawn(collector(dir.path()).run(
            store.clone() as std::sync::Arc<dyn HotStore>,
            std::sync::Arc::new(webhooks.clone()),
        ));
        assert!(sealed(dir.path(), 5));
        let failures = || standing(dir.path()).first().map_or(0, |(_, s)| s.failures);
        let started = tokio::time::Instant::now();
        while failures() < 8 {
            tokio::time::advance(std::time::Duration::from_secs(5)).await;
            tokio::task::spawn_blocking(|| {
                std::thread::sleep(std::time::Duration::from_millis(20))
            })
            .await
            .unwrap();
            assert!(started.elapsed() < std::time::Duration::from_secs(3600));
        }
        // Eight failures are seven waits: 5, 10, 20, 40, 80, 160, then the 300 s cap.
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(615),
            "eight failures in {:?}",
            started.elapsed()
        );

        seat(dir.path(), std::time::Duration::from_secs(30));
        while store.outbox_len() == 0 || standing(dir.path())[0].1.last_error.is_some() {
            tokio::time::advance(std::time::Duration::from_secs(5)).await;
            tokio::task::spawn_blocking(|| {
                std::thread::sleep(std::time::Duration::from_millis(20))
            })
            .await
            .unwrap();
            assert!(started.elapsed() < std::time::Duration::from_secs(7200));
        }
        assert_eq!(standing(dir.path())[0].1.last_error, None);
        task.abort();
        let _ = task.await;
    }

    /// Two nests may name a webhook alike: each keeps its own series under its mount, and an unmounted
    /// nest keeps none.
    #[test]
    fn webhook_series_are_per_nest_and_leave_with_the_nest() {
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let w = wh("alerts", "t__transfer", "genesis");
        note(a.path(), &w, 90, 100, None, false);
        note(b.path(), &w, 40, 100, Some("boom"), false);
        let m = crate::metrics::Metrics::new();
        m.nest("one").set_dataset_dir(a.path().to_path_buf());
        m.nest("two").set_dataset_dir(b.path().to_path_buf());
        let out = m.render();
        for line in [
            "nuthatch_webhook_backlog_blocks{nest=\"one\",webhook=\"alerts\"} 10\n",
            "nuthatch_webhook_backlog_blocks{nest=\"two\",webhook=\"alerts\"} 60\n",
            "nuthatch_webhook_failures_total{nest=\"one\",webhook=\"alerts\"} 0\n",
            "nuthatch_webhook_failures_total{nest=\"two\",webhook=\"alerts\"} 1\n",
        ] {
            assert!(out.contains(line), "missing {line:?} in:\n{out}");
        }
        m.remove_nest("two");
        let out = m.render();
        assert!(!out.contains("nest=\"two\",webhook"), "{out}");
        assert!(out.contains("nest=\"one\",webhook"), "{out}");
        assert!(
            standing(b.path()).is_empty(),
            "the departed nest's standing is dropped"
        );
        assert!(!standing(a.path()).is_empty());
    }
}
