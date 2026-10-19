//! Resolving every document `[[ipfs]]` declarations name, behind the cursor, until each one is stored
//! or given up on (RFC-0037 §3).
//!
//! The tip path records nothing extra. The rows naming CIDs are already durable in the hot store, and a
//! document's key is a function of its block's rows alone ([`plan_block`]), so the work list can be
//! re-derived from the store at any time, a restart included. [`Resolver`] does that, fetches with retry
//! and backoff, and writes a document only while the row that named it is still there. Sealing holds
//! below the lowest block whose documents are neither stored nor given up on
//! ([`Gate::lowest_pending`]), so a range never seals short of a document that could still arrive, and
//! tip-following never waits on a gateway.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::StreamExt;

use crate::ipfs::{BlockCtx, IpfsDecl, RowPlace, RowsRefused};
use crate::metrics::NestMetrics;
use crate::registry::{
    DecodedRow, TableSchema, Value, IPFS_DOCUMENT_ROW_LOG_INDEX_BASE,
    IPFS_DOCUMENT_ROW_LOG_INDEX_END, IPFS_DOCUMENT_SLOTS, IPFS_ROW_LOG_INDEX_BASE,
};
use crate::store::{HotStore, Store};

/// Blocks read from the hot store at once while re-deriving the work list. Resolved documents live in
/// the same range and can be megabytes each, so this bounds what one read holds in memory.
const SCAN_CHUNK: u64 = 256;

/// One document a block's rows name, at the key it is stored under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub block: u64,
    pub slot: usize,
    /// Index into the nest's declarations, which is also the result table.
    pub decl: usize,
    pub cid: String,
    pub block_hash: String,
    pub block_timestamp: u64,
    /// The first row in the block that named this document. The write is conditional on it.
    pub source_log_index: u64,
    /// The first `log_index` of this document's typed rows, when its declaration has them.
    pub rows_from: Option<u64>,
    /// The document or its rows would not fit the block's key space, so it is refused without a fetch.
    pub over_band: bool,
}

impl Planned {
    pub fn key(&self) -> String {
        Store::entity_key(self.block, IPFS_ROW_LOG_INDEX_BASE + self.slot as u64)
    }

    pub fn source_key(&self) -> String {
        Store::entity_key(self.block, self.source_log_index)
    }
}

/// Whether a row at `log_index` is a document row, as [`Planned::key`] places one: typed rows sit above.
pub fn is_document_row(log_index: u64) -> bool {
    (IPFS_ROW_LOG_INDEX_BASE..IPFS_ROW_LOG_INDEX_BASE + IPFS_DOCUMENT_SLOTS).contains(&log_index)
}

/// Every document one block's rows name, with slots assigned within the block: declarations in config
/// order, rows in `log_index` order, and a CID a declaration has already planned in this block skipped.
///
/// Per block, not per fetch window. Deduplicating across a window put a CID named in two blocks into
/// whichever block the window reached first, so two operators with different windows sealed different
/// segments over the same range.
pub fn plan_block(decls: &[IpfsDecl], rows: &[&DecodedRow]) -> Vec<Planned> {
    let mut rows = rows.to_vec();
    rows.sort_by_key(|r| r.log_index);
    let mut out: Vec<Planned> = Vec::new();
    let mut seen = HashSet::new();
    for (i, d) in decls.iter().enumerate() {
        for r in rows.iter().filter(|r| r.table == d.on) {
            let Some((_, value)) = r.params.iter().find(|(k, _)| k == d.column()) else {
                continue;
            };
            for cid in d.cids_in(value).0 {
                if seen.insert((i, cid.clone())) {
                    out.push(Planned {
                        block: r.block_number,
                        slot: out.len(),
                        decl: i,
                        cid,
                        block_hash: r.block_hash.clone(),
                        block_timestamp: r.block_timestamp,
                        source_log_index: r.log_index,
                        rows_from: None,
                        over_band: false,
                    });
                }
            }
        }
    }
    // Each document with typed rows is allotted its declaration's `max_rows`, in slot order, whether or
    // not the documents before it ever resolve. A key assigned on arrival would differ between operators.
    if decls.iter().any(|d| d.rows.is_some()) {
        let mut next = IPFS_DOCUMENT_ROW_LOG_INDEX_BASE;
        for p in &mut out {
            p.over_band = p.slot as u64 >= IPFS_DOCUMENT_SLOTS;
            if let Some(rows) = &decls[p.decl].rows {
                p.rows_from = Some(next);
                p.over_band |=
                    next.saturating_add(rows.max_rows - 1) > IPFS_DOCUMENT_ROW_LOG_INDEX_END;
                next = next.saturating_add(rows.max_rows);
            }
        }
    }
    out
}

/// Rows a declaration reads that name no CID it could use.
pub fn unreadable(decls: &[IpfsDecl], rows: &[&DecodedRow]) -> usize {
    let mut missed = 0;
    for d in decls {
        for r in rows.iter().filter(|r| r.table == d.on) {
            match r.params.iter().find(|(k, _)| k == d.column()) {
                Some((_, value)) => missed += d.cids_in(value).1,
                None => missed += 1,
            }
        }
    }
    missed
}

/// Where both resolving paths record a document given up on, one key per block and slot.
pub const GAVE_UP_PREFIX: &str = "ipfs_gave_up:";

/// The meta key recording that a document was given up on. Its record names the CID, so a reorg that
/// puts a different document in the same slot is not mistaken for the one abandoned.
pub fn gave_up_key(p: &Planned) -> String {
    gave_up_key_at(p.block, p.slot)
}

fn gave_up_key_at(block: u64, slot: usize) -> String {
    format!("{GAVE_UP_PREFIX}{block:012}:{slot}")
}

/// A document given up on, as either path records it (#1410). Its range sealed without it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GaveUp {
    pub cid: String,
    pub block: u64,
    pub slot: usize,
    /// `None` on a record an earlier build wrote as the bare CID, which kept nothing else.
    pub declaration: Option<String>,
    pub error: Option<String>,
    /// Unix seconds.
    pub at: Option<u64>,
}

impl GaveUp {
    fn new(gate: &Gate, p: &Planned, error: String) -> GaveUp {
        GaveUp {
            cid: p.cid.clone(),
            block: p.block,
            slot: p.slot,
            declaration: Some(gate.decls[p.decl].name.clone()),
            error: Some(error),
            at: Some(crate::metrics::now_unix()),
        }
    }

    /// A stored record, including the bare CID a build before #1410 wrote with its block and slot in the key.
    pub fn parse(key: &str, value: &str) -> Option<GaveUp> {
        if let Ok(record) = serde_json::from_str::<GaveUp>(value) {
            return Some(record);
        }
        let (block, slot) = key.strip_prefix(GAVE_UP_PREFIX)?.split_once(':')?;
        Some(GaveUp {
            cid: value.to_string(),
            block: block.parse().ok()?,
            slot: slot.parse().ok()?,
            declaration: None,
            error: None,
            at: None,
        })
    }
}

fn recorded(store: &dyn HotStore, block: u64, slot: usize, cid: &str) -> Result<bool> {
    let key = gave_up_key_at(block, slot);
    Ok(store
        .get_meta(&key)?
        .and_then(|v| GaveUp::parse(&key, &v))
        .is_some_and(|r| r.cid == cid))
}

fn gave_up(store: &dyn HotStore, p: &Planned) -> Result<bool> {
    recorded(store, p.block, p.slot, &p.cid)
}

/// Record a document given up on unless it already is, since a refetched tail or a restart gives the same
/// document up again and the first record is the one worth keeping. Returns whether it wrote.
pub fn record_gave_up(store: &dyn HotStore, record: &GaveUp) -> Result<bool> {
    if recorded(store, record.block, record.slot, &record.cid)? {
        return Ok(false);
    }
    store.set_meta(
        &gave_up_key_at(record.block, record.slot),
        &serde_json::to_string(record)?,
    )?;
    Ok(true)
}

/// A nest's declarations, with what planning needs to read them back out of stored rows.
pub struct Gate {
    decls: Vec<IpfsDecl>,
    /// Storage kind of each column a declaration reads, by `(table, column)`. A `bytes` column is
    /// stored as hex, and JSON calldata only plans the same from a stored row once it is bytes again.
    kinds: HashMap<(String, String), String>,
    /// The first block not known to be clear of outstanding documents: every block below it was
    /// clear when last read (#2231). A window writing rows lowers it.
    clear_below: AtomicU64,
}

#[derive(serde::Deserialize)]
struct Head<'a> {
    #[serde(borrow)]
    table: std::borrow::Cow<'a, str>,
    block_number: u64,
    log_index: u64,
}

impl Gate {
    /// `schema` must include the call tables, since a declaration may read one.
    pub fn new(decls: &[IpfsDecl], schema: &[TableSchema]) -> Option<Arc<Gate>> {
        if decls.is_empty() {
            return None;
        }
        let mut kinds = HashMap::new();
        for d in decls {
            let found = schema
                .iter()
                .filter(|t| t.table == d.on)
                .flat_map(|t| &t.columns)
                .find(|c| c.name == d.column());
            if let Some(c) = found {
                kinds.insert((d.on.clone(), d.column().to_string()), c.storage.clone());
            }
        }
        Some(Arc::new(Gate {
            decls: decls.to_vec(),
            kinds,
            clear_below: AtomicU64::new(0),
        }))
    }

    /// See the field. Blocks below this need not be read again for outstanding documents.
    pub fn clear_below(&self) -> u64 {
        self.clear_below.load(Ordering::Acquire)
    }

    /// Every block below `block` has been read and holds no outstanding document.
    pub fn cleared_below(&self, block: u64) {
        self.clear_below.fetch_max(block, Ordering::AcqRel);
    }

    /// Rows were written from `block` up, so what was read there no longer stands.
    pub fn written_from(&self, block: u64) {
        self.clear_below.fetch_min(block, Ordering::AcqRel);
    }

    pub fn decls(&self) -> &[IpfsDecl] {
        &self.decls
    }

    fn value(&self, table: &str, column: &str, raw: &serde_json::Value) -> Value {
        let kind = self.kinds.get(&(table.to_string(), column.to_string()));
        match (kind.map(String::as_str), raw) {
            (Some("bytes" | "fixed_bytes"), serde_json::Value::String(s)) => {
                hex::decode(s.trim_start_matches("0x"))
                    .map(Value::Bytes)
                    .unwrap_or_else(|_| Value::Str(s.clone()))
            }
            (_, serde_json::Value::String(s)) => Value::Str(s.clone()),
            (_, other) => Value::Json(other.to_string()),
        }
    }

    fn stored_row(&self, v: &serde_json::Value, head: &Head<'_>) -> DecodedRow {
        let mut params: Vec<(String, Value)> = Vec::new();
        for d in self.decls.iter().filter(|d| d.on == head.table) {
            let col = d.column();
            if params.iter().any(|(k, _)| k == col) {
                continue;
            }
            if let Some(raw) = v.get(col) {
                params.push((col.to_string(), self.value(&head.table, col, raw)));
            }
        }
        DecodedRow {
            table: head.table.to_string(),
            params,
            block_number: head.block_number,
            block_hash: v
                .get("block_hash")
                .and_then(|h| h.as_str())
                .unwrap_or_default()
                .to_string(),
            block_timestamp: v
                .get("block_timestamp")
                .and_then(|t| t.as_u64())
                .unwrap_or(0),
            timestamps: v.get("block_timestamp").is_some(),
            log_index: head.log_index,
            tx_hash: String::new(),
            address: String::new(),
        }
    }

    /// Every document stored rows name, with the keys already present among them, by block.
    pub fn plan_stored(
        &self,
        entities: &[String],
    ) -> BTreeMap<u64, (Vec<Planned>, HashSet<String>)> {
        let mut rows: BTreeMap<u64, Vec<DecodedRow>> = BTreeMap::new();
        let mut present: BTreeMap<u64, HashSet<String>> = BTreeMap::new();
        for e in entities {
            let Ok(head) = serde_json::from_str::<Head<'_>>(e) else {
                continue;
            };
            present
                .entry(head.block_number)
                .or_default()
                .insert(Store::entity_key(head.block_number, head.log_index));
            if !self.decls.iter().any(|d| d.on == head.table) {
                continue;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(e) else {
                continue;
            };
            rows.entry(head.block_number)
                .or_default()
                .push(self.stored_row(&v, &head));
        }
        rows.into_iter()
            .map(|(block, rs)| {
                let refs: Vec<&DecodedRow> = rs.iter().collect();
                let planned = plan_block(&self.decls, &refs);
                (block, (planned, present.remove(&block).unwrap_or_default()))
            })
            .collect()
    }

    /// The lowest block in `entities` holding a document that is neither stored nor given up on.
    pub fn lowest_pending(&self, store: &dyn HotStore, entities: &[String]) -> Result<Option<u64>> {
        for (block, (planned, present)) in self.plan_stored(entities) {
            for p in &planned {
                if !present.contains(&p.key()) && !gave_up(store, p)? {
                    return Ok(Some(block));
                }
            }
        }
        Ok(None)
    }

    /// The document row, then the typed rows its declaration explodes it into. Only a proven document
    /// reaches here, so the document row says `verified = true`; rows an older build stored unverified
    /// keep `false` until the nest is re-indexed.
    fn document_rows(
        &self,
        p: &Planned,
        content: &str,
        timestamps: bool,
    ) -> std::result::Result<Vec<DecodedRow>, RowsRefused> {
        let decl = &self.decls[p.decl];
        let ctx = BlockCtx {
            number: p.block,
            hash: &p.block_hash,
            timestamp: p.block_timestamp,
            timestamps,
        };
        let kept = match &decl.rows {
            Some(rows) if !rows.keep_content => "",
            _ => content,
        };
        let document = crate::ipfs::to_row(&decl.name, &p.cid, kept, true, p.slot, &ctx);
        let typed = match (&decl.rows, p.rows_from) {
            (Some(rows), Some(first)) => crate::ipfs::typed_rows(
                rows,
                content,
                &RowPlace {
                    cid: &p.cid,
                    document_log_index: document.log_index,
                    source_log_index: p.source_log_index,
                    first_log_index: first,
                },
                &ctx,
            )?,
            _ => Vec::new(),
        };
        let mut out = Vec::with_capacity(typed.len() + 1);
        out.push(document);
        out.extend(typed);
        Ok(out)
    }
}

/// How hard a document is tried before it is given up on.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    /// Documents fetched at once.
    pub concurrency: usize,
    /// The wait after a first failure. It doubles with each further failure, up to `max_backoff`.
    pub first_backoff: Duration,
    pub max_backoff: Duration,
    /// Failed fetches before a document is given up on. At the defaults that is about half an hour.
    pub attempts: u32,
    /// Each request's timeout, body included, on a first attempt. It doubles with each failure, up to
    /// `max_timeout`: a gateway took 29.86 s to serve a document it had not cached (#1399).
    pub first_timeout: Duration,
    pub max_timeout: Duration,
    /// The longest one document is tried for, requests and waits included, before it is given up on.
    pub deadline: Option<Duration>,
    /// How often the whole unsealed range is re-read, rather than only the blocks committed since.
    pub rescan_every: Duration,
    /// The longest the resolver sleeps with nothing due.
    pub idle_poll: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            concurrency: 8,
            first_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(600),
            attempts: 10,
            first_timeout: crate::subgraph_import::FETCH_TIMEOUT,
            max_timeout: Duration::from_secs(120),
            deadline: None,
            rescan_every: Duration::from_secs(600),
            idle_poll: Duration::from_secs(2),
        }
    }
}

/// The default `--ipfs-window-deadline` (#1399).
pub const WINDOW_DEADLINE: Duration = Duration::from_secs(300);

/// [`WINDOW_DEADLINE`], for a config that was loaded rather than given the flag.
pub fn window_deadline() -> Duration {
    WINDOW_DEADLINE
}

impl Policy {
    /// For a path whose window cannot seal until every document in it is decided (#1399). Attempts at
    /// 30, 60 and 120 s and their waits fit inside the five minutes; fast failures get all ten attempts.
    pub fn seal_direct() -> Self {
        Self::seal_direct_within(WINDOW_DEADLINE)
    }

    /// [`Policy::seal_direct`] with an operator's `--ipfs-window-deadline`. Zero is no deadline: a
    /// document is given up on only once its attempts run out.
    pub fn seal_direct_within(deadline: Duration) -> Self {
        Policy {
            first_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(30),
            deadline: (!deadline.is_zero()).then_some(deadline),
            ..Policy::default()
        }
    }

    pub fn backoff(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(20);
        self.first_backoff
            .saturating_mul(1u32 << doublings)
            .min(self.max_backoff)
    }

    pub fn timeout(&self, failures: u32) -> Duration {
        self.first_timeout
            .saturating_mul(1u32 << failures.min(20))
            .min(self.max_timeout)
    }

    /// The wait before a document that has failed `failures` times over `tried_for` is tried again, or
    /// `None` when it is to be given up on.
    pub fn retry_in(&self, failures: u32, tried_for: Duration) -> Option<Duration> {
        let wait = self.backoff(failures);
        let past_deadline = self
            .deadline
            .is_some_and(|d| tried_for.saturating_add(wait) >= d);
        (failures < self.attempts && !past_deadline).then_some(wait)
    }
}

/// A body fetched that nothing could prove against its CID. It is retried like any other failure,
/// because a gateway that serves the blocks may answer next time.
#[derive(Debug)]
struct Unproven(String);

impl std::fmt::Display for Unproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fetched but not proven: {}", self.0)
    }
}

impl std::error::Error for Unproven {}

async fn fetch(cid: &str, gateways: &[String], timeout: Duration) -> Result<String> {
    use crate::subgraph_import::{fetch_ipfs_proven_within, Fetched, NothingProved, Origin, Proof};
    match fetch_ipfs_proven_within(cid, gateways, Origin::Manifest, timeout).await {
        Ok(Fetched {
            body,
            proof: Proof::Verified,
        }) => Ok(body),
        Ok(Fetched {
            proof: Proof::Unproven(why),
            ..
        }) => Err(Unproven(why).into()),
        Err(e) if e.is::<NothingProved>() => Err(Unproven(e.to_string()).into()),
        Err(e) => Err(e),
    }
}

/// A document over the byte, block or depth cap is the same size from every gateway, so retrying it
/// cannot help.
fn is_over_cap(e: &anyhow::Error) -> bool {
    e.is::<crate::cid::OverCap>()
}

/// Count a failed fetch the same way on every path, returning whether it was over the caps.
fn count_failure(metrics: &crate::metrics::NestMetrics, e: &anyhow::Error) -> bool {
    if e.is::<Unproven>() {
        metrics.add_ipfs_unverified(1);
    }
    let over_cap = is_over_cap(e);
    if over_cap {
        metrics.add_ipfs_oversize(1);
    }
    over_cap
}

/// One fetch of a document that has failed `failures` times over `tried_for`, cut off at the deadline.
async fn attempt(
    cid: &str,
    gateways: &[String],
    policy: &Policy,
    failures: u32,
    tried_for: Duration,
) -> Result<String> {
    let fetching = fetch(cid, gateways, policy.timeout(failures));
    within_deadline(fetching, policy.deadline, tried_for).await
}

async fn within_deadline(
    fetching: impl std::future::Future<Output = Result<String>>,
    deadline: Option<Duration>,
    tried_for: Duration,
) -> Result<String> {
    let Some(deadline) = deadline else {
        return fetching.await;
    };
    tokio::time::timeout(deadline.saturating_sub(tried_for), fetching)
        .await
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "not fetched within the {deadline:?} a document is tried for"
            ))
        })
}

/// A failed fetch that will be tried again, counted and said on every path, so a pause is attributable.
fn note_retry(
    metrics: &NestMetrics,
    p: &Planned,
    failures: u32,
    wait: Duration,
    e: &anyhow::Error,
) {
    metrics.add_ipfs_retries(1);
    tracing::warn!(
        "ipfs: attempt {failures} for {} (block {}) failed, trying again in {wait:?} \
         (nuthatch_nest_ipfs_retries_total): {e:#}",
        p.cid,
        p.block
    );
}

struct Work {
    planned: Planned,
    failures: u32,
    due: Instant,
    /// When its first fetch began, which a deadline counts from.
    first_tried: Option<Instant>,
}

/// The out-of-band resolver for one nest.
pub struct Resolver {
    store: Arc<dyn HotStore>,
    gate: Arc<Gate>,
    gateways: Vec<String>,
    timestamps: bool,
    metrics: Arc<NestMetrics>,
    policy: Policy,
    work: BTreeMap<(u64, usize), Work>,
    scanned_through: Option<u64>,
    last_full_scan: Option<Instant>,
}

impl Resolver {
    pub fn new(
        store: Arc<dyn HotStore>,
        gate: Arc<Gate>,
        gateways: Vec<String>,
        timestamps: bool,
        metrics: Arc<NestMetrics>,
        policy: Policy,
    ) -> Resolver {
        Resolver {
            store,
            gate,
            gateways,
            timestamps,
            metrics,
            policy,
            work: BTreeMap::new(),
            scanned_through: None,
            last_full_scan: None,
        }
    }

    pub fn pending(&self) -> usize {
        self.work.len()
    }

    /// Bring the work list up to date with the store. A document's failure count survives a rescan, or
    /// one that never resolves would be retried forever instead of given up on.
    pub fn scan(&mut self) -> Result<()> {
        let Some(head) = self.store.indexed_head()? else {
            return Ok(());
        };
        let sealed = self.store.sealed_through();
        let floor = if sealed == 0 { 0 } else { sealed + 1 };
        let full = match (self.scanned_through, self.last_full_scan) {
            (Some(s), Some(at)) => head < s || at.elapsed() >= self.policy.rescan_every,
            _ => true,
        };
        // The refetched tail (#1144) lands below the previous head, so an incremental pass re-reads it.
        let from = match self.scanned_through {
            Some(s) if !full => (s + 1)
                .saturating_sub(crate::indexer::FETCH_TAIL_OVERLAP)
                .max(floor),
            _ => floor,
        };
        self.work.retain(|(b, _), _| *b >= floor);
        let mut previous = self.work.split_off(&(from, 0));

        let mut lo = from;
        while lo <= head {
            let hi = lo.saturating_add(SCAN_CHUNK - 1).min(head);
            let entities = self.store.entities_in_range(lo, hi)?;
            for (_, (planned, present)) in self.gate.plan_stored(&entities) {
                for p in planned {
                    if present.contains(&p.key()) || gave_up(self.store.as_ref(), &p)? {
                        continue;
                    }
                    let key = (p.block, p.slot);
                    let work = match previous.remove(&key) {
                        Some(w) if w.planned == p => w,
                        _ => Work {
                            planned: p,
                            failures: 0,
                            due: Instant::now(),
                            first_tried: None,
                        },
                    };
                    self.work.insert(key, work);
                }
            }
            if hi == u64::MAX {
                break;
            }
            lo = hi + 1;
        }
        self.scanned_through = Some(head);
        if full {
            self.last_full_scan = Some(Instant::now());
        }
        self.metrics.set_ipfs_pending(self.work.len() as u64);
        Ok(())
    }

    /// One pass: bring the work list up to date, then fetch what is due, at most `concurrency` at once.
    /// Returns the documents still outstanding.
    pub async fn step(&mut self) -> Result<usize> {
        self.scan()?;
        let unplaceable: Vec<Planned> = self
            .work
            .values()
            .filter(|w| w.planned.over_band)
            .map(|w| w.planned.clone())
            .collect();
        for p in unplaceable {
            tracing::warn!(
                "ipfs: gave up on {} (block {}, slot {}) without a fetch: the block has no room left \
                 for its typed rows. Its range seals without it (nuthatch_nest_ipfs_rows_refused_total)",
                p.cid,
                p.block,
                p.slot
            );
            self.metrics.add_ipfs_rows_refused(1);
            self.give_up(&p, NO_ROOM.to_string())?;
        }
        let now = Instant::now();
        let due: Vec<(Planned, u32, Duration)> = self
            .work
            .values_mut()
            .filter(|w| w.due <= now)
            .take(self.policy.concurrency.max(1))
            .map(|w| {
                let first = *w.first_tried.get_or_insert(now);
                (w.planned.clone(), w.failures, now.duration_since(first))
            })
            .collect();
        let (gateways, policy) = (&self.gateways, &self.policy);
        let results: Vec<(Planned, Result<String>)> = futures::stream::iter(due)
            .map(|(p, failures, tried_for)| async move {
                let fetched = attempt(&p.cid, gateways, policy, failures, tried_for).await;
                (p, fetched)
            })
            .buffer_unordered(self.policy.concurrency.max(1))
            .collect()
            .await;

        for (p, fetched) in results {
            let key = (p.block, p.slot);
            match fetched {
                Ok(content) => match self.gate.document_rows(&p, &content, self.timestamps) {
                    Ok(rows) => {
                        self.work.remove(&key);
                        let entries: Vec<(String, String)> = rows
                            .iter()
                            .map(|r| {
                                (
                                    Store::entity_key(r.block_number, r.log_index),
                                    r.to_json().to_string(),
                                )
                            })
                            .collect();
                        // False when a reorg removed or replaced the naming row since the scan; the next
                        // scan plans whatever the block holds now.
                        if self.store.put_entities_if_named(
                            &entries,
                            &p.source_key(),
                            &p.block_hash,
                        )? {
                            self.metrics.add_ipfs_resolved(1);
                        }
                    }
                    Err(refused) => {
                        tracing::warn!(
                            "ipfs: gave up on {} (block {}): proven, but {refused}. Its range seals \
                             without it (nuthatch_nest_ipfs_rows_refused_total)",
                            p.cid,
                            p.block
                        );
                        self.metrics.add_ipfs_rows_refused(1);
                        self.give_up(&p, format!("proven, but {refused}"))?;
                    }
                },
                Err(e) => {
                    let Some(w) = self.work.get_mut(&key) else {
                        continue;
                    };
                    let over_cap = count_failure(&self.metrics, &e);
                    w.failures += 1;
                    let failures = w.failures;
                    let retry = if over_cap {
                        None
                    } else {
                        let tried_for = w.first_tried.map_or(Duration::ZERO, |t| t.elapsed());
                        self.policy.retry_in(failures, tried_for)
                    };
                    if let Some(wait) = retry {
                        w.due = Instant::now() + wait;
                        note_retry(&self.metrics, &p, failures, wait, &e);
                    } else {
                        tracing::warn!(
                            "ipfs: gave up on {} (block {}) after {} failed fetches: {e:#}. Its range \
                             seals without it (nuthatch_nest_ipfs_given_up_total)",
                            p.cid,
                            p.block,
                            failures
                        );
                        self.give_up(&p, format!("{e:#}"))?;
                    }
                }
            }
        }
        self.metrics.set_ipfs_pending(self.work.len() as u64);
        Ok(self.work.len())
    }

    fn give_up(&mut self, p: &Planned, error: String) -> Result<()> {
        record_gave_up(self.store.as_ref(), &GaveUp::new(&self.gate, p, error))?;
        self.metrics.add_ipfs_given_up(1);
        self.work.remove(&(p.block, p.slot));
        Ok(())
    }

    fn next_wait(&self) -> Duration {
        let now = Instant::now();
        self.work
            .values()
            .map(|w| w.due.saturating_duration_since(now))
            .min()
            .map_or(self.policy.idle_poll, |d| d.min(self.policy.idle_poll))
    }

    pub async fn run(mut self) {
        loop {
            let wait = match self.step().await {
                Ok(_) => self.next_wait(),
                Err(e) => {
                    tracing::warn!("ipfs resolver: {e:#}; retrying");
                    self.policy.idle_poll
                }
            };
            tokio::time::sleep(wait).await;
        }
    }
}

/// A running resolver, aborted when dropped so a retired nest releases its store.
pub struct Running(tokio::task::JoinHandle<()>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub fn spawn(resolver: Resolver) -> Running {
    Running(tokio::spawn(resolver.run()))
}

/// Why a document is given up on without a fetch when its block has no room left for its typed rows.
const NO_ROOM: &str = "the block has no room left for its typed rows";

/// Every document `rows` name, fetched now, for a path that seals as it goes and cannot come back for a
/// document later. Retries on `policy`, whose deadline bounds how long one document holds the window;
/// the second value is the documents given up on, absent from what is returned, in block order.
pub async fn resolve_inline(
    gate: &Gate,
    gateways: &[String],
    policy: &Policy,
    rows: &[DecodedRow],
    timestamps: bool,
    metrics: &crate::metrics::NestMetrics,
) -> (Vec<DecodedRow>, Vec<GaveUp>) {
    let mut by_block: BTreeMap<u64, Vec<&DecodedRow>> = BTreeMap::new();
    for r in rows
        .iter()
        .filter(|r| gate.decls.iter().any(|d| d.on == r.table))
    {
        by_block.entry(r.block_number).or_default().push(r);
    }
    let mut given_up = Vec::new();
    let planned: Vec<Planned> = by_block
        .values()
        .flat_map(|rs| plan_block(&gate.decls, rs))
        .filter(|p| {
            if p.over_band {
                tracing::warn!(
                    "ipfs: gave up on {} (block {}, slot {}) without a fetch: {NO_ROOM}",
                    p.cid,
                    p.block,
                    p.slot
                );
                metrics.add_ipfs_rows_refused(1);
                given_up.push(GaveUp::new(gate, p, NO_ROOM.to_string()));
            }
            !p.over_band
        })
        .collect();
    let results: Vec<(Planned, std::result::Result<String, String>)> =
        futures::stream::iter(planned)
            .map(|p| async move {
                let started = Instant::now();
                let mut failures = 0;
                loop {
                    match attempt(&p.cid, gateways, policy, failures, started.elapsed()).await {
                        Ok(content) => return (p, Ok(content)),
                        Err(e) => {
                            failures += 1;
                            let retry = if count_failure(metrics, &e) {
                                None
                            } else {
                                policy.retry_in(failures, started.elapsed())
                            };
                            let Some(wait) = retry else {
                                tracing::warn!(
                            "ipfs: gave up on {} (block {}) after {failures} failed fetches: {e:#}",
                            p.cid,
                            p.block
                        );
                                return (p, Err(format!("{e:#}")));
                            };
                            note_retry(metrics, &p, failures, wait, &e);
                            tokio::time::sleep(wait).await;
                        }
                    }
                }
            })
            .buffer_unordered(policy.concurrency.max(1))
            .collect()
            .await;

    let mut out = Vec::new();
    for (p, fetched) in results {
        match fetched.map(|c| gate.document_rows(&p, &c, timestamps)) {
            Ok(Ok(rows)) => out.extend(rows),
            Ok(Err(refused)) => {
                tracing::warn!(
                    "ipfs: gave up on {} (block {}): proven, but {refused}",
                    p.cid,
                    p.block
                );
                metrics.add_ipfs_rows_refused(1);
                given_up.push(GaveUp::new(gate, &p, format!("proven, but {refused}")));
            }
            Err(error) => given_up.push(GaveUp::new(gate, &p, error)),
        }
    }
    metrics.add_ipfs_given_up(given_up.len() as u64);
    out.sort_by_key(|r| (r.block_number, r.log_index));
    given_up.sort_by_key(|g| (g.block, g.slot));
    (out, given_up)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ColumnSchema, TableKind};

    const CID: &str = "QmdhcVTpSjmCBvqgL9m6nazRs23XBEbJ6zygojVAqib7oa";

    fn uri_decl() -> IpfsDecl {
        IpfsDecl {
            name: "token_metadata".into(),
            on: "nft__uri_set".into(),
            cid_column: "uri".into(),
            cid_json_path: None,
            json_match: BTreeMap::new(),
            rows: None,
        }
    }

    fn uri_row(block: u64, log_index: u64, cid: &str) -> DecodedRow {
        DecodedRow {
            table: "nft__uri_set".into(),
            params: vec![("uri".into(), Value::Str(cid.into()))],
            block_number: block,
            block_hash: format!("0x{block:064x}"),
            block_timestamp: 1_700_000_000,
            timestamps: true,
            log_index,
            tx_hash: "0xt".into(),
            address: "0xa".into(),
        }
    }

    fn table(name: &str, column: &str, storage: &str) -> TableSchema {
        TableSchema {
            table: name.into(),
            alias: String::new(),
            kind: TableKind::Call,
            event: String::new(),
            topic0: String::new(),
            function: String::new(),
            selector: String::new(),
            columns: vec![ColumnSchema {
                name: column.into(),
                sol_type: storage.into(),
                storage: storage.into(),
                indexed: false,
                components: Vec::new(),
            }],
        }
    }

    /// A CID named in two blocks is a document in each, and one named twice in a block is one. Planning
    /// across a window instead put the one row wherever the window happened to reach first.
    #[test]
    fn a_document_is_planned_once_per_block_that_names_it() {
        let gate = Gate::new(&[uri_decl()], &[table("nft__uri_set", "uri", "string")]).unwrap();
        let stored: Vec<String> = [
            uri_row(10, 0, CID),
            uri_row(10, 4, CID),
            uri_row(11, 2, CID),
        ]
        .iter()
        .map(|r| r.to_json().to_string())
        .collect();
        let plan = gate.plan_stored(&stored);
        assert_eq!(
            plan[&10].0.len(),
            1,
            "one fetch for a CID named twice in a block"
        );
        assert_eq!(
            plan[&11].0.len(),
            1,
            "a later block naming the same CID has its own row"
        );
        assert_eq!(
            plan[&11].0[0].key(),
            Store::entity_key(11, IPFS_ROW_LOG_INDEX_BASE)
        );
        assert_eq!(plan[&10].0[0].source_log_index, 0);
    }

    /// JSON calldata is stored as hex. Read back as a string it names nothing, so the stored plan has
    /// to turn it into bytes again to agree with the plan the tip made from the decoded row.
    #[test]
    fn a_stored_bytes_column_plans_what_the_decoded_row_did() {
        let decl = IpfsDecl {
            name: "qos_payload".into(),
            on: "edge__call_submit_qo_s_payload".into(),
            cid_column: "_payload".into(),
            cid_json_path: Some("hash".into()),
            json_match: BTreeMap::from([("topic".to_string(), "t".to_string())]),
            rows: None,
        };
        let payload = format!(r#"{{"topic": "t", "hash": "{CID}", "timestamp": 1}}"#);
        let row = DecodedRow {
            table: decl.on.clone(),
            params: vec![("_payload".into(), Value::Bytes(payload.into_bytes()))],
            ..uri_row(48_231_985, 750_003, CID)
        };
        let gate = Gate::new(
            std::slice::from_ref(&decl),
            &[table(&decl.on, "_payload", "bytes")],
        )
        .unwrap();
        let from_tip = plan_block(std::slice::from_ref(&decl), &[&row]);
        assert_eq!(
            from_tip.len(),
            1,
            "premise: the decoded row names the document"
        );
        let from_store = gate.plan_stored(&[row.to_json().to_string()]);
        assert_eq!(from_store[&48_231_985].0, from_tip);
    }

    /// The write the resolver makes is conditional on the row that named the document, in one
    /// transaction, so a reorg that replaces or removes that row cannot be followed by the document.
    #[test]
    fn a_document_is_written_only_while_its_naming_row_is_there() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let naming = uri_row(10, 0, CID);
        let source = Store::entity_key(10, 0);
        store
            .put_entity(&source, &naming.to_json().to_string())
            .unwrap();
        let doc = Store::entity_key(10, IPFS_ROW_LOG_INDEX_BASE);

        assert!(
            !store
                .put_entity_if_named(&doc, "{}", &source, "0xreorged")
                .unwrap(),
            "a row now from a different block must not be answered"
        );
        assert!(store
            .put_entity_if_named(&doc, "{}", &source, &naming.block_hash)
            .unwrap());
        store.rollback_to(9).unwrap();
        assert!(
            !store
                .put_entity_if_named(&doc, "{}", &source, &naming.block_hash)
                .unwrap(),
            "a rolled-back row names nothing"
        );
        assert_eq!(store.get_entity(&doc).unwrap(), None);
    }

    #[test]
    fn backoff_doubles_to_its_ceiling() {
        let p = Policy {
            first_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(60),
            ..Policy::default()
        };
        assert_eq!(p.backoff(1), Duration::from_secs(5));
        assert_eq!(p.backoff(2), Duration::from_secs(10));
        assert_eq!(p.backoff(4), Duration::from_secs(40));
        assert_eq!(p.backoff(5), Duration::from_secs(60));
        assert_eq!(p.backoff(40), Duration::from_secs(60));
    }

    fn one_column_rows(table: &str, max_rows: u64) -> crate::ipfs::IpfsRows {
        crate::ipfs::IpfsRows {
            table: table.into(),
            max_rows,
            keep_content: true,
            columns: vec![crate::ipfs::RowColumn {
                name: "a".into(),
                ty: crate::ipfs::RowType::U64,
            }],
        }
    }

    #[tokio::test]
    async fn a_proven_empty_file_is_refused_as_typed_rows_without_retrying_the_gateway() {
        use axum::{routing::get, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let app = Router::new().route(
            "/ipfs/{cid}",
            get(move || {
                count.fetch_add(1, Ordering::SeqCst);
                async { axum::http::StatusCode::NO_CONTENT }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = format!("http://{}/ipfs/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let decl = IpfsDecl {
            rows: Some(one_column_rows("typed", 10)),
            ..uri_decl()
        };
        let gate = Gate::new(&[decl], &[table("nft__uri_set", "uri", "string")]).unwrap();
        let metrics = crate::metrics::NestMetrics::default();
        let policy = Policy {
            first_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..Policy::default()
        };
        let (rows, given_up) = resolve_inline(
            &gate,
            &[gateway],
            &policy,
            &[uri_row(
                10,
                0,
                "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH",
            )],
            true,
            &metrics,
        )
        .await;
        assert!(rows.is_empty());
        assert_eq!(given_up.len(), 1);
        assert_eq!(metrics.ipfs_rows_refused(), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.ipfs_retries(), 0);
        server.abort();
    }

    /// A gateway at `/ipfs/{cid}` that answers its `n`th request, from 0, with `answer(n)`.
    async fn stand_in<F, Fut>(
        answer: F,
    ) -> (
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    )
    where
        F: Fn(usize) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = axum::response::Response> + Send + 'static,
    {
        use axum::{routing::get, Router};
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = requests.clone();
        let app = Router::new().route(
            "/ipfs/{cid}",
            get(move || answer(count.fetch_add(1, std::sync::atomic::Ordering::SeqCst))),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = format!("http://{}/ipfs/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (gateway, requests, server)
    }

    /// The seal-direct policy with millisecond timeouts and no waits between attempts.
    fn quick(first_timeout_ms: u64, max_timeout_ms: u64, deadline: Option<Duration>) -> Policy {
        Policy {
            first_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            first_timeout: Duration::from_millis(first_timeout_ms),
            max_timeout: Duration::from_millis(max_timeout_ms),
            deadline,
            ..Policy::seal_direct()
        }
    }

    const BODY: &str = r#"{"name":"a token"}"#;

    async fn resolve_one(
        gateway: String,
        cid: &str,
        policy: &Policy,
        metrics: &NestMetrics,
    ) -> (Vec<DecodedRow>, Vec<GaveUp>) {
        let gate = Gate::new(&[uri_decl()], &[table("nft__uri_set", "uri", "string")]).unwrap();
        resolve_inline(
            &gate,
            &[gateway],
            policy,
            &[uri_row(10, 0, cid)],
            true,
            metrics,
        )
        .await
    }

    /// The Graph's gateway took 29.86 s to serve a document it had not cached, against a 30 s timeout
    /// (#1399). The fetch that timed out is tried again and counted, and the document arrives.
    #[tokio::test]
    async fn a_gateway_that_times_out_once_is_tried_again_and_the_retry_counted() {
        use axum::response::IntoResponse;
        let cid = crate::cid::cid_v0_for(BODY.as_bytes());
        let (gateway, requests, server) = stand_in(|n| async move {
            if n == 0 {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            BODY.into_response()
        })
        .await;
        let metrics = NestMetrics::default();
        let (rows, given_up) =
            resolve_one(gateway, &cid, &quick(1_000, 1_000, None), &metrics).await;
        assert_eq!(
            (rows.len(), given_up.len()),
            (1, 0),
            "served on the second attempt"
        );
        assert_eq!(metrics.ipfs_retries(), 1);
        assert_eq!(metrics.ipfs_given_up(), 0);
        assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 2);
        server.abort();
    }

    /// A gateway slower than the first timeout but faster than a later one serves the document. The same
    /// timeout on every attempt refused it ten times.
    #[tokio::test]
    async fn a_slow_gateway_is_given_a_longer_timeout_on_each_attempt() {
        use axum::response::IntoResponse;
        let cid = crate::cid::cid_v0_for(BODY.as_bytes());
        let (gateway, _requests, server) = stand_in(|_| async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            BODY.into_response()
        })
        .await;
        let metrics = NestMetrics::default();
        let (rows, given_up) = resolve_one(gateway, &cid, &quick(50, 2_000, None), &metrics).await;
        assert_eq!(
            (rows.len(), given_up.len()),
            (1, 0),
            "served once a timeout outlasted the gateway"
        );
        assert!(
            metrics.ipfs_retries() >= 2,
            "attempts at 50 and 100 ms cannot see a 300 ms answer: {}",
            metrics.ipfs_retries()
        );
        server.abort();
    }

    /// The Graph's gateway answered 200 for two QoS documents and stalled after 256 to 320 KiB (#1399).
    /// A document is given up on when its deadline runs out, however many attempts remain, and is counted
    /// and left out exactly as one that ran out of attempts.
    #[tokio::test]
    async fn a_gateway_that_stalls_mid_body_holds_a_document_no_longer_than_its_deadline() {
        let (cid, gateway, _requests, server) = stalls_mid_body().await;
        let metrics = NestMetrics::default();
        // The document deadline, not a shorter request timeout, must end this fetch. Retry count
        // is not a contract here: a busy executor may use the whole deadline on its first attempt.
        let policy = quick(10_000, 10_000, Some(Duration::from_secs(1)));
        let (rows, given_up) = resolve_one(gateway, &cid, &policy, &metrics).await;
        assert!(rows.is_empty());
        assert_eq!(given_up.len(), 1);
        assert_eq!(metrics.ipfs_given_up(), 1);
        assert!(
            given_up[0]
                .error
                .as_deref()
                .unwrap()
                .contains("not fetched within the 1s"),
            "the document deadline must end the fetch: {given_up:?}"
        );
        server.abort();
    }

    /// A gateway that answers 200 for a document, sends half of it and never the rest.
    async fn stalls_mid_body() -> (
        String,
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        use axum::response::IntoResponse;
        let body = "x".repeat(64 * 1024);
        let cid = crate::cid::cid_v0_for(body.as_bytes());
        let (gateway, requests, server) = stand_in(move |_| {
            let half = body[..body.len() / 2].to_string();
            async move {
                let stalls = futures::stream::once(async move { Ok::<_, std::io::Error>(half) })
                    .chain(futures::stream::pending());
                axum::body::Body::from_stream(stalls).into_response()
            }
        })
        .await;
        (cid, gateway, requests, server)
    }

    /// `--ipfs-window-deadline 0` (#1399): a document that never arrives is given up on only once its
    /// attempts run out, as it was before a seal-direct window had a deadline.
    #[tokio::test]
    async fn a_zero_window_deadline_gives_up_only_after_every_attempt() {
        let (cid, gateway, _requests, server) = stalls_mid_body().await;
        let metrics = NestMetrics::default();
        let policy = Policy {
            attempts: 4,
            first_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            first_timeout: Duration::from_millis(50),
            max_timeout: Duration::from_millis(100),
            ..Policy::seal_direct_within(Duration::ZERO)
        };
        let (rows, given_up) = resolve_one(gateway, &cid, &policy, &metrics).await;
        assert!(rows.is_empty());
        assert_eq!(given_up.len(), 1);
        assert_eq!(metrics.ipfs_given_up(), 1);
        assert_eq!(
            metrics.ipfs_retries(),
            3,
            "every attempt but the last is retried"
        );
        // Count client attempts via retries, not requests accepted by the server: under load an
        // attempt can time out before the server accepts it. All four attempts still took place.
        server.abort();
    }

    /// Socket-free timing: elapsed budget is subtracted, not reset for each fetch.
    #[tokio::test(start_paused = true)]
    async fn a_pending_fetch_gets_only_the_remaining_document_budget() {
        for (spent, remaining) in [(0, 5), (3, 2), (5, 0), (8, 0)] {
            let start = tokio::time::Instant::now();
            let error = within_deadline(
                async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok("late document".into())
                },
                Some(Duration::from_secs(5)),
                Duration::from_secs(spent),
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("not fetched within the 5s"));
            assert_eq!(start.elapsed(), Duration::from_secs(remaining));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn no_document_deadline_leaves_every_attempt_its_request_timeout() {
        let policy = Policy {
            attempts: 4,
            ..quick(50, 100, Policy::seal_direct_within(Duration::ZERO).deadline)
        };
        let start = tokio::time::Instant::now();
        // Even a document already pending for an hour must retain all its attempts when disabled.
        let already_spent = Duration::from_secs(3_600);
        let mut failures = 0;
        loop {
            let fetching = async {
                tokio::time::sleep(policy.timeout(failures)).await;
                Err(anyhow::anyhow!("request timeout"))
            };
            let error = within_deadline(fetching, policy.deadline, already_spent + start.elapsed())
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), "request timeout");
            failures += 1;
            let Some(wait) = policy.retry_in(failures, already_spent + start.elapsed()) else {
                break;
            };
            tokio::time::sleep(wait).await;
        }
        assert_eq!(failures, 4);
        assert_eq!(start.elapsed(), Duration::from_millis(350));
    }

    /// A 204 for a document that is not empty proves nothing. It is retried to its attempts and given up
    /// on, each fetch counted unverified as before, and each retry now counted as well.
    #[tokio::test]
    async fn a_gateway_that_204s_a_document_is_retried_and_given_up_on_as_before() {
        use axum::response::IntoResponse;
        let cid = crate::cid::cid_v0_for(BODY.as_bytes());
        let (gateway, _requests, server) =
            stand_in(|_| async { axum::http::StatusCode::NO_CONTENT.into_response() }).await;
        let metrics = NestMetrics::default();
        let policy = Policy {
            attempts: 3,
            ..quick(1_000, 1_000, Some(Duration::from_secs(60)))
        };
        let (rows, given_up) = resolve_one(gateway, &cid, &policy, &metrics).await;
        assert!(rows.is_empty());
        assert_eq!(given_up.len(), 1);
        assert_eq!(metrics.ipfs_given_up(), 1);
        assert_eq!(metrics.ipfs_unverified(), 3);
        assert_eq!(
            metrics.ipfs_retries(),
            2,
            "every failed attempt but the last"
        );
        server.abort();
    }

    /// #1399's numbers. A seal-direct document is decided within five minutes, where the resolver's ten
    /// attempts take about half an hour, and a slow gateway still gets a whole 120 s attempt.
    #[test]
    fn a_seal_direct_document_is_decided_within_five_minutes() {
        let secs = Duration::from_secs;
        let p = Policy::seal_direct();
        assert_eq!(
            [p.timeout(0), p.timeout(1), p.timeout(2), p.timeout(9)],
            [secs(30), secs(60), secs(120), secs(120)]
        );
        let before_third = p.timeout(0) + p.backoff(1) + p.timeout(1);
        assert_eq!(p.retry_in(2, before_third), Some(secs(4)));
        assert!(before_third + p.backoff(2) + p.timeout(2) < p.deadline.unwrap());
        assert_eq!(
            p.retry_in(1, secs(299)),
            None,
            "nothing starts past five minutes"
        );
        let waits: Duration = (1..p.attempts).map(|f| p.backoff(f)).sum();
        assert_eq!(
            waits,
            secs(180),
            "a gateway failing fast still gets all ten attempts"
        );
        assert_eq!(p.retry_in(p.attempts, Duration::ZERO), None);

        let resolver = Policy::default();
        assert_eq!(
            resolver.deadline, None,
            "the out-of-band resolver holds no window"
        );
        assert_eq!(resolver.retry_in(9, secs(86_400)), Some(secs(600)));
        assert_eq!(resolver.timeout(0), crate::subgraph_import::FETCH_TIMEOUT);
    }

    /// #1410: a document given up on again, by a refetched tail or a restart, keeps its first record, and
    /// a different document a reorg put in the same slot replaces it. A bare CID an earlier build wrote
    /// still counts as given up.
    #[test]
    fn a_document_given_up_on_twice_is_recorded_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let gate = Gate::new(&[uri_decl()], &[table("nft__uri_set", "uri", "string")]).unwrap();
        let row = uri_row(10, 0, CID);
        let p = plan_block(gate.decls(), &[&row]).remove(0);
        assert!(record_gave_up(&store, &GaveUp::new(&gate, &p, "first".into())).unwrap());
        assert!(!record_gave_up(&store, &GaveUp::new(&gate, &p, "second".into())).unwrap());
        let listed = store.meta_with_prefix(GAVE_UP_PREFIX, 10).unwrap();
        assert_eq!(listed.len(), 1);
        let record = GaveUp::parse(&listed[0].0, &listed[0].1).unwrap();
        assert_eq!(
            (
                record.cid.as_str(),
                record.block,
                record.declaration.as_deref(),
                record.error.as_deref()
            ),
            (CID, 10, Some("token_metadata"), Some("first"))
        );
        assert!(gave_up(&store, &p).unwrap());

        let reorged = Planned {
            cid: crate::cid::cid_v0_for(b"another document"),
            ..p.clone()
        };
        assert!(!gave_up(&store, &reorged).unwrap());
        assert!(record_gave_up(&store, &GaveUp::new(&gate, &reorged, "third".into())).unwrap());

        store.set_meta(&gave_up_key(&p), CID).unwrap();
        assert!(
            gave_up(&store, &p).unwrap(),
            "a bare CID an earlier build wrote still counts"
        );
    }

    /// #1421 counts a stored document by its row's place, so the document band ends where typed rows begin.
    #[test]
    fn a_document_row_is_told_from_a_typed_row_by_its_place() {
        assert!(is_document_row(IPFS_ROW_LOG_INDEX_BASE));
        assert!(is_document_row(
            IPFS_ROW_LOG_INDEX_BASE + IPFS_DOCUMENT_SLOTS - 1
        ));
        assert!(!is_document_row(IPFS_ROW_LOG_INDEX_BASE - 1));
        assert!(!is_document_row(IPFS_DOCUMENT_ROW_LOG_INDEX_BASE));
    }

    /// Room for typed rows is allotted by slot from the plan alone. Keys assigned as documents arrived
    /// would differ between two operators whose gateways answered in a different order.
    #[test]
    fn typed_rows_are_placed_by_slot_before_anything_resolves() {
        let decls = [
            IpfsDecl {
                name: "typed_docs".into(),
                rows: Some(one_column_rows("typed", 3_000)),
                ..uri_decl()
            },
            IpfsDecl {
                name: "plain_docs".into(),
                ..uri_decl()
            },
        ];
        let named: Vec<DecodedRow> = (0..3)
            .map(|i| {
                uri_row(
                    10,
                    i,
                    &crate::cid::cid_v0_for(format!("doc {i}").as_bytes()),
                )
            })
            .collect();
        let refs: Vec<&DecodedRow> = named.iter().collect();
        let plan = plan_block(&decls, &refs);
        assert_eq!(
            plan.iter()
                .filter(|p| p.decl == 0)
                .map(|p| p.rows_from)
                .collect::<Vec<_>>(),
            [Some(626_000), Some(629_000), Some(632_000)]
        );
        assert!(plan
            .iter()
            .filter(|p| p.decl == 1)
            .all(|p| p.rows_from.is_none()));
        assert!(plan.iter().all(|p| !p.over_band));
    }

    /// A document whose rows would run into the call band is refused before it is fetched: nothing a
    /// gateway returns could make it fit.
    #[test]
    fn a_document_past_the_room_for_typed_rows_is_refused_without_a_fetch() {
        let decl = IpfsDecl {
            rows: Some(one_column_rows("typed", 100_000)),
            ..uri_decl()
        };
        let named: Vec<DecodedRow> = (0..2)
            .map(|i| {
                uri_row(
                    10,
                    i,
                    &crate::cid::cid_v0_for(format!("doc {i}").as_bytes()),
                )
            })
            .collect();
        let refs: Vec<&DecodedRow> = named.iter().collect();
        let plan = plan_block(std::slice::from_ref(&decl), &refs);
        assert_eq!(
            plan.iter().map(|p| p.over_band).collect::<Vec<_>>(),
            [false, true],
            "the second document's 100,000 rows would start at 726,000 and end past 749,999"
        );
    }
}
