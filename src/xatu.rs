//! An opt-in history source for Ethereum mainnet: logs and block timestamps from ethPandaOps' public
//! Xatu Parquet files, handed off to the normal RPC source for everything the dataset does not hold.
//!
//! It is a [`Source`] wrapper, not a second indexer. Decode, the hot store, sealing and serving see
//! the same `Log`s and the same timestamps they would have had from `eth_getLogs` and the block
//! headers; only where the bytes came from changes. Off unless the operator asks (`nuthatch dev
//! --history-source xatu`), and it never touches nest data identity, so a nest indexed this way has
//! the NID it would have had from RPC alone.
//!
//! **Shape.** The dataset is cut in 1,000-block chunks, one logs file and one block file per chunk
//! (`canonical_execution_logs/1000/<start>.parquet`, `canonical_execution_block/1000/<start>.parquet`).
//! A request is split at chunk boundaries. A chunk the dataset serves answers its part; every other
//! part (above the dataset's head, before its start, or a chunk that failed) is asked of the inner
//! RPC source, in contiguous runs. Parts are disjoint and visited in ascending order, so the handoff
//! has no gap and no duplicate by construction.
//!
//! **Trust.** A chunk is used only after it passes three checks, each cheap and deterministic: its
//! block file has exactly one row per block with an unbroken parent-hash chain; the logs file opens
//! with the columns this reader needs; and the RPC's hash for the chunk's *last* block equals the
//! dataset's. That is one RPC call per 1,000 blocks. Any mismatch, fetch error or parse error falls
//! back to RPC for that chunk with a warning, never an empty answer. What it cannot check is that a
//! block's logs are *complete*: the dataset carries no receipts root.
//!
//! **Memory.** The compressed bytes of at most [`KEEP_CHUNKS`] chunks are held, and each chunk keeps
//! the decoded rows of the one filter last asked of it, so a backfill's many small windows decode a
//! chunk once.

use crate::source::{LogFilter, Source};
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, ArrayRef, BinaryArray, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub use nuthatch_decode::rpc::Log;

/// Where ethPandaOps publishes the mainnet files. Overridable, so a mirror works.
pub const DEFAULT_BASE_URL: &str = "https://data.ethpandaops.io/xatu/mainnet/databases/default";

/// Blocks per file in the public dataset.
pub const DATASET_CHUNK: u64 = 1000;

/// Chunks held at once. A chunk's compressed logs are 35 to 50 MB on mainnet, so this bounds the
/// source at a few hundred MB beside the 2 GB cursor budget.
const KEEP_CHUNKS: usize = 4;

/// How long "the dataset has no such chunk yet" is remembered. The dataset's head moves, so a 404 is
/// not permanent; it is also the normal answer for the tail, so it must not be asked on every poll.
const ABSENT_FOR: Duration = Duration::from_secs(600);

/// Fetches one dataset file. `Ok(None)` is "no such file" (a 404), which is how the dataset's head
/// shows; every other failure is an error. A trait so tests serve recorded files.
#[async_trait::async_trait]
pub trait Fetch: Send + Sync {
    async fn get(&self, url: &str) -> Result<Option<Bytes>>;
}

/// The real transport: plain HTTPS GETs of public files. Sends nothing about the operator.
pub struct HttpFetch(reqwest::Client);

impl HttpFetch {
    pub fn new() -> Result<Self> {
        Ok(Self(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(300))
                .user_agent("nuthatch")
                .build()?,
        ))
    }
}

#[async_trait::async_trait]
impl Fetch for HttpFetch {
    async fn get(&self, url: &str) -> Result<Option<Bytes>> {
        let mut last = anyhow!("no attempt made");
        for attempt in 0..3u64 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
            }
            match self.0.get(url).send().await {
                Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => return Ok(None),
                Ok(r) if r.status().is_success() => match r.bytes().await {
                    Ok(b) => return Ok(Some(b)),
                    Err(e) => last = anyhow!("reading {url}: {e}"),
                },
                // A 403 is how some object stores say "not there" for an anonymous reader.
                Ok(r) if r.status() == reqwest::StatusCode::FORBIDDEN => return Ok(None),
                Ok(r) => last = anyhow!("{url}: HTTP {}", r.status()),
                Err(e) => last = anyhow!("{url}: {e}"),
            }
        }
        Err(last)
    }
}

/// Counters a benchmark or a log line can read.
#[derive(Debug, Default)]
pub struct Stats {
    /// Chunks downloaded, verified and used.
    pub chunks_loaded: AtomicU64,
    /// Compressed bytes downloaded.
    pub bytes: AtomicU64,
    /// RPC calls spent checking a chunk against the chain (one per chunk).
    pub anchor_calls: AtomicU64,
    /// Chunks that failed and were handed to RPC.
    pub fallbacks: AtomicU64,
    /// Block-timestamp and hash lookups answered from the dataset.
    pub header_hits: AtomicU64,
}

/// A chunk that passed the checks.
struct Chunk {
    start: u64,
    hashes: Vec<String>,
    timestamps: Vec<u64>,
    logs: Bytes,
    /// The decoded rows for the filter last asked, sorted by (block, log index).
    matched: Mutex<Option<(LogFilter, Arc<Vec<Log>>)>>,
}

enum Fault {
    /// Could not decide (the RPC would not answer); try again next time.
    Transient(anyhow::Error),
    /// This chunk is not usable; never ask again this run.
    Bad(anyhow::Error),
}

#[derive(Default)]
struct State {
    ready: HashMap<u64, Arc<Chunk>>,
    order: VecDeque<u64>,
    /// `Some(t)`: absent until `t`. `None`: bad for this run.
    gone: HashMap<u64, Option<Instant>>,
}

pub struct XatuSource {
    inner: Arc<dyn Source>,
    fetch: Arc<dyn Fetch>,
    base: String,
    chunk_len: u64,
    state: Mutex<State>,
    load: tokio::sync::Mutex<()>,
    pub stats: Stats,
}

impl XatuSource {
    pub fn new(inner: Arc<dyn Source>, fetch: Arc<dyn Fetch>, base: &str) -> Self {
        Self::with_chunk_len(inner, fetch, base, DATASET_CHUNK)
    }

    /// For fixtures, which are cut in smaller chunks than the public files.
    pub fn with_chunk_len(
        inner: Arc<dyn Source>,
        fetch: Arc<dyn Fetch>,
        base: &str,
        chunk_len: u64,
    ) -> Self {
        Self {
            inner,
            fetch,
            base: base.trim_end_matches('/').to_string(),
            chunk_len,
            state: Mutex::new(State::default()),
            load: tokio::sync::Mutex::new(()),
            stats: Stats::default(),
        }
    }

    fn url(&self, table: &str, start: u64) -> String {
        format!("{}/{table}/{}/{start}.parquet", self.base, self.chunk_len)
    }

    fn start_of(&self, block: u64) -> u64 {
        block / self.chunk_len * self.chunk_len
    }

    /// A chunk already loaded, without loading anything. Header lookups use this: they follow the
    /// logs read of the same window, which loaded the chunk, and a miss just goes to RPC.
    fn cached(&self, block: u64) -> Option<Arc<Chunk>> {
        let s = self.start_of(block);
        self.state.lock().unwrap().ready.get(&s).cloned()
    }

    /// The chunk starting at `start` if the dataset can serve it; `None` means ask RPC.
    async fn chunk(&self, start: u64) -> Option<Arc<Chunk>> {
        if let Some(c) = self.lookup(start) {
            return c;
        }
        let _one_load_at_a_time = self.load.lock().await;
        if let Some(c) = self.lookup(start) {
            return c;
        }
        let t = Instant::now();
        match self.load_chunk(start).await {
            Ok(Some(c)) => {
                let c = Arc::new(c);
                self.stats.chunks_loaded.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    chunk = start,
                    secs = format!("{:.1}", t.elapsed().as_secs_f32()),
                    "xatu: serving blocks {start}..={} from the dataset",
                    start + self.chunk_len - 1
                );
                let mut st = self.state.lock().unwrap();
                st.ready.insert(start, c.clone());
                st.order.push_back(start);
                while st.order.len() > KEEP_CHUNKS {
                    if let Some(old) = st.order.pop_front() {
                        st.ready.remove(&old);
                    }
                }
                Some(c)
            }
            Ok(None) => {
                self.state
                    .lock()
                    .unwrap()
                    .gone
                    .insert(start, Some(Instant::now() + ABSENT_FOR));
                None
            }
            Err(Fault::Transient(e)) => {
                self.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "xatu: could not check blocks {start}..={} ({e:#}); asking the RPC for them",
                    start + self.chunk_len - 1
                );
                None
            }
            Err(Fault::Bad(e)) => {
                self.mark_bad(start, &e);
                None
            }
        }
    }

    /// `Some(answer)` when the state already decides; `None` when a load is needed.
    fn lookup(&self, start: u64) -> Option<Option<Arc<Chunk>>> {
        let mut st = self.state.lock().unwrap();
        if let Some(c) = st.ready.get(&start).cloned() {
            if let Some(pos) = st.order.iter().position(|s| *s == start) {
                st.order.remove(pos);
                st.order.push_back(start);
            }
            return Some(Some(c));
        }
        match st.gone.get(&start) {
            Some(None) => Some(None),
            Some(Some(until)) if *until > Instant::now() => Some(None),
            Some(Some(_)) => {
                st.gone.remove(&start);
                None
            }
            None => None,
        }
    }

    fn mark_bad(&self, start: u64, why: &anyhow::Error) {
        self.stats.fallbacks.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            "xatu: not using the dataset for blocks {start}..={} ({why:#}); asking the RPC for them",
            start + self.chunk_len - 1
        );
        let mut st = self.state.lock().unwrap();
        st.ready.remove(&start);
        st.order.retain(|s| *s != start);
        st.gone.insert(start, None);
    }

    async fn load_chunk(&self, start: u64) -> Result<Option<Chunk>, Fault> {
        let Some(blocks) = self
            .fetch
            .get(&self.url("canonical_execution_block", start))
            .await
            .map_err(Fault::Transient)?
        else {
            return Ok(None);
        };
        let (n, len) = (start, self.chunk_len);
        let parsed = tokio::task::spawn_blocking(move || parse_blocks(blocks, n, len))
            .await
            .map_err(|e| Fault::Bad(anyhow!(e)))?
            .map_err(Fault::Bad)?;
        let Some(logs) = self
            .fetch
            .get(&self.url("canonical_execution_logs", start))
            .await
            .map_err(Fault::Transient)?
        else {
            return Err(Fault::Bad(anyhow!(
                "the dataset has the block file for this chunk but not the logs file"
            )));
        };
        self.stats
            .bytes
            .fetch_add(logs.len() as u64, Ordering::Relaxed);
        check_logs_schema(&logs).map_err(Fault::Bad)?;

        // The anchor: the chain's own hash for the chunk's last block, and for its first when the
        // file has no parent hashes to chain the blocks between.
        let last = start + self.chunk_len - 1;
        let mut anchors = vec![last];
        if !parsed.chained {
            anchors.insert(0, start);
        }
        for block in anchors {
            self.stats.anchor_calls.fetch_add(1, Ordering::Relaxed);
            let canonical = match self.inner.block_hash(block).await {
                Ok(Some(h)) => h,
                Ok(None) => {
                    return Err(Fault::Transient(anyhow!(
                        "the RPC has no hash for block {block} to check the chunk against"
                    )))
                }
                Err(e) => {
                    return Err(Fault::Transient(
                        e.context("checking the chunk against the RPC"),
                    ))
                }
            };
            let ours = &parsed.hashes[(block - start) as usize];
            if !canonical.eq_ignore_ascii_case(ours) {
                return Err(Fault::Bad(anyhow!(
                    "block {block} is {ours} in the dataset but {canonical} on the chain"
                )));
            }
        }
        Ok(Some(Chunk {
            start,
            hashes: parsed.hashes,
            timestamps: parsed.timestamps,
            logs,
            matched: Mutex::new(None),
        }))
    }

    /// The matching logs of `c` for blocks `[a, b]`, decoding the chunk once per filter.
    async fn read_chunk(
        &self,
        c: &Arc<Chunk>,
        filter: &LogFilter,
        a: u64,
        b: u64,
    ) -> Result<Vec<Log>> {
        let hit = c
            .matched
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(f, _)| f == filter)
            .map(|(_, rows)| rows.clone());
        let rows = match hit {
            Some(rows) => rows,
            None => {
                let (cc, f, len) = (c.clone(), filter.clone(), self.chunk_len);
                let rows = Arc::new(
                    tokio::task::spawn_blocking(move || extract_logs(&cc, &f, len))
                        .await
                        .map_err(|e| anyhow!(e))??,
                );
                *c.matched.lock().unwrap() = Some((filter.clone(), rows.clone()));
                rows
            }
        };
        let lo = rows.partition_point(|l| l.block_number < a);
        let hi = rows.partition_point(|l| l.block_number <= b);
        Ok(rows[lo..hi].to_vec())
    }
}

/// What the block file says about a chunk.
struct Blocks {
    hashes: Vec<String>,
    timestamps: Vec<u64>,
    /// Whether the file carried `parent_hash` and the chain through it was checked. Older files (mainnet
    /// before about block 26.4M) do not carry it, and are anchored at both ends instead.
    chained: bool,
}

fn open(bytes: Bytes) -> Result<ParquetRecordBatchReaderBuilder<Bytes>> {
    Ok(ParquetRecordBatchReaderBuilder::try_new(bytes)?)
}

fn col(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<ArrayRef> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(|| anyhow!("column `{name}` is missing"))
}

fn as_u64(a: &ArrayRef) -> Result<UInt64Array> {
    let c = cast(a, &DataType::UInt64)?;
    Ok(c.as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("not an integer column"))?
        .clone())
}

/// Any of the binary-ish physical types a Parquet writer may have used for the hex text.
fn as_binary(a: &ArrayRef) -> Result<BinaryArray> {
    let c = cast(a, &DataType::Binary)?;
    Ok(c.as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| anyhow!("not a binary column"))?
        .clone())
}

/// The dataset stores hex as `0x…` text. Anything else is a format this reader was not written for,
/// and must be refused rather than guessed at.
fn hex_text(b: &[u8]) -> Result<Cow<'_, str>> {
    let s = std::str::from_utf8(b).context("a hex value is not text")?;
    if !s.starts_with("0x") {
        bail!("a hex value does not start with 0x");
    }
    Ok(if s.bytes().any(|c| c.is_ascii_uppercase()) {
        Cow::Owned(s.to_ascii_lowercase())
    } else {
        Cow::Borrowed(s)
    })
}

fn parse_blocks(bytes: Bytes, start: u64, len: u64) -> Result<Blocks> {
    let builder = open(bytes)?;
    let has_parent = builder.schema().field_with_name("parent_hash").is_ok();
    let schema = builder.parquet_schema();
    let names: &[&str] = if has_parent {
        &[
            "block_number",
            "block_hash",
            "parent_hash",
            "block_date_time",
        ]
    } else {
        &["block_number", "block_hash", "block_date_time"]
    };
    let mask = ProjectionMask::columns(schema, names.iter().copied());
    let reader = builder.with_projection(mask).build()?;
    let mut rows: Vec<(u64, String, Option<String>, u64)> = Vec::new();
    for batch in reader {
        let batch = batch?;
        let n = as_u64(&col(&batch, "block_number")?)?;
        let h = as_binary(&col(&batch, "block_hash")?)?;
        let p = has_parent
            .then(|| col(&batch, "parent_hash").and_then(|c| as_binary(&c)))
            .transpose()?;
        let t = col(&batch, "block_date_time")?;
        // Seconds since the epoch, whatever unit the writer used.
        let div = match t.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => 1,
            DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000,
            DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000_000,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => 1_000_000_000,
            other => bail!("block_date_time is {other}, not a timestamp"),
        };
        let t = cast(&t, &DataType::Int64)?;
        let t = t
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow!("timestamp column did not cast"))?;
        for i in 0..n.len() {
            if n.is_null(i) || h.is_null(i) || t.is_null(i) {
                bail!("a block row has a null in it");
            }
            let secs = u64::try_from(t.value(i) / div).context("negative timestamp")?;
            rows.push((
                n.value(i),
                hex_text(h.value(i))?.into_owned(),
                // Present in newer files; in some older ones the column exists and is all null.
                match &p {
                    Some(p) if !p.is_null(i) => Some(hex_text(p.value(i))?.into_owned()),
                    _ => None,
                },
                secs,
            ));
        }
    }
    rows.sort_by_key(|r| r.0);
    let chained = rows.iter().all(|r| r.2.is_some());
    if rows.len() as u64 != len {
        bail!("the block file has {} rows, expected {len}", rows.len());
    }
    for (i, r) in rows.iter().enumerate() {
        if r.0 != start + i as u64 {
            bail!("the block file skips or repeats block {}", start + i as u64);
        }
        if r.3 == 0 {
            bail!("block {} has a zero timestamp", r.0);
        }
        if i > 0
            && r.2
                .as_deref()
                .is_some_and(|p| !p.eq_ignore_ascii_case(&rows[i - 1].1))
        {
            bail!("block {} does not extend block {}", r.0, r.0 - 1);
        }
    }
    Ok(Blocks {
        hashes: rows.iter().map(|r| r.1.clone()).collect(),
        timestamps: rows.iter().map(|r| r.3).collect(),
        chained,
    })
}

const LOG_COLUMNS: [&str; 9] = [
    "block_number",
    "log_index",
    "transaction_hash",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
];

fn check_logs_schema(bytes: &Bytes) -> Result<()> {
    let builder = open(bytes.clone())?;
    let schema = builder.schema();
    for c in LOG_COLUMNS {
        schema
            .field_with_name(c)
            .map_err(|_| anyhow!("the logs file has no `{c}` column"))?;
    }
    Ok(())
}

/// Every log of the chunk that `filter` asks for, in (block, log index) order, normalised to what
/// `eth_getLogs` returns: lowercase `0x` hex, empty data as `0x`, the block hash joined from the
/// block file (the dataset's logs carry none).
fn extract_logs(c: &Chunk, filter: &LogFilter, len: u64) -> Result<Vec<Log>> {
    let builder = open(c.logs.clone())?;
    let schema = builder.parquet_schema();
    let mask = ProjectionMask::columns(schema, LOG_COLUMNS);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(16_384)
        .build()?;
    let lower = |v: &[String]| -> Option<HashSet<String>> {
        (!v.is_empty()).then(|| v.iter().map(|s| s.to_ascii_lowercase()).collect())
    };
    let (want_addr, want_t0) = (lower(filter.addresses()), lower(filter.topic0s()));
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let number = as_u64(&col(&batch, "block_number")?)?;
        let index = as_u64(&col(&batch, "log_index")?)?;
        let tx = as_binary(&col(&batch, "transaction_hash")?)?;
        let addr = as_binary(&col(&batch, "address")?)?;
        let topics = [
            as_binary(&col(&batch, "topic0")?)?,
            as_binary(&col(&batch, "topic1")?)?,
            as_binary(&col(&batch, "topic2")?)?,
            as_binary(&col(&batch, "topic3")?)?,
        ];
        let data = as_binary(&col(&batch, "data")?)?;
        for i in 0..batch.num_rows() {
            if number.is_null(i) || index.is_null(i) || tx.is_null(i) || addr.is_null(i) {
                bail!("a log row has a null in a required column");
            }
            let a = hex_text(addr.value(i))?;
            if want_addr.as_ref().is_some_and(|w| !w.contains(a.as_ref())) {
                continue;
            }
            if let Some(w) = &want_t0 {
                if topics[0].is_null(i) || !w.contains(hex_text(topics[0].value(i))?.as_ref()) {
                    continue;
                }
            }
            let bn = number.value(i);
            if bn < c.start || bn >= c.start + len {
                bail!("a log is for block {bn}, outside the chunk");
            }
            let mut t = Vec::new();
            for col in &topics {
                if col.is_null(i) {
                    break;
                }
                t.push(hex_text(col.value(i))?.into_owned());
            }
            let log = Log {
                address: a.into_owned(),
                topics: t,
                data: if data.is_null(i) || data.value(i).is_empty() {
                    "0x".to_string()
                } else {
                    hex_text(data.value(i))?.into_owned()
                },
                block_number: bn,
                block_hash: c.hashes[(bn - c.start) as usize].clone(),
                tx_hash: hex_text(tx.value(i))?.into_owned(),
                log_index: index.value(i),
            };
            if filter.matches(&log) {
                out.push(log);
            }
        }
    }
    out.sort_by_key(|l| (l.block_number, l.log_index));
    if out
        .windows(2)
        .any(|w| (w[0].block_number, w[0].log_index) == (w[1].block_number, w[1].log_index))
    {
        bail!("the logs file repeats a (block, log index)");
    }
    Ok(out)
}

#[async_trait::async_trait]
impl Source for XatuSource {
    async fn tip(&self) -> Result<u64> {
        self.inner.tip().await
    }

    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        if let Some(c) = self.cached(number) {
            self.stats.header_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(c.hashes[(number - c.start) as usize].clone()));
        }
        self.inner.block_hash(number).await
    }

    async fn block_hashes(&self, blocks: &[u64]) -> Result<HashMap<u64, String>> {
        let mut out = HashMap::new();
        let mut rest = Vec::new();
        for &b in blocks {
            match self.cached(b) {
                Some(c) => {
                    out.insert(b, c.hashes[(b - c.start) as usize].clone());
                }
                None => rest.push(b),
            }
        }
        self.stats
            .header_hits
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        if !rest.is_empty() {
            out.extend(self.inner.block_hashes(&rest).await?);
        }
        Ok(out)
    }

    async fn finalized(&self) -> Result<Option<u64>> {
        self.inner.finalized().await
    }

    async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        let mut out = HashMap::new();
        let mut rest = Vec::new();
        for &b in blocks {
            match self.cached(b) {
                Some(c) => {
                    out.insert(b, c.timestamps[(b - c.start) as usize]);
                }
                None => rest.push(b),
            }
        }
        self.stats
            .header_hits
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        if !rest.is_empty() {
            out.extend(self.inner.block_timestamps(&rest).await?);
        }
        Ok(out)
    }

    async fn block_record(&self, number: u64) -> Result<Option<(String, Option<u64>)>> {
        if let Some(c) = self.cached(number) {
            let i = (number - c.start) as usize;
            self.stats.header_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some((c.hashes[i].clone(), Some(c.timestamps[i]))));
        }
        self.inner.block_record(number).await
    }

    async fn block_records(&self, blocks: &[u64]) -> Result<HashMap<u64, (String, Option<u64>)>> {
        let mut out = HashMap::new();
        let mut rest = Vec::new();
        for &b in blocks {
            match self.cached(b) {
                Some(c) => {
                    let i = (b - c.start) as usize;
                    out.insert(b, (c.hashes[i].clone(), Some(c.timestamps[i])));
                }
                None => rest.push(b),
            }
        }
        self.stats
            .header_hits
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        if !rest.is_empty() {
            out.extend(self.inner.block_records(&rest).await?);
        }
        Ok(out)
    }

    async fn block_headers(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        self.inner.block_headers(blocks).await
    }

    async fn block_bodies(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        self.inner.block_bodies(blocks).await
    }

    fn forget_cached_above(&self, block: u64) {
        self.inner.forget_cached_above(block);
        // A reorg cannot reach a chunk the dataset serves, but a number is not an identity (see the
        // trait): drop anything that could be at or above it, so it is re-checked against the chain.
        let len = self.chunk_len;
        let mut st = self.state.lock().unwrap();
        st.ready.retain(|s, _| s + len - 1 <= block);
        let keep: HashSet<u64> = st.ready.keys().copied().collect();
        st.order.retain(|s| keep.contains(s));
        st.gone.retain(|s, _| s + len - 1 <= block);
    }

    fn set_log_group_fanout(&self, n: usize) {
        self.inner.set_log_group_fanout(n);
    }

    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
        let mut out: Vec<Log> = Vec::new();
        // The RPC run being gathered: contiguous parts the dataset did not serve.
        let mut run: Option<(u64, u64)> = None;
        let mut s = self.start_of(from);
        while s <= to {
            let (a, b) = (from.max(s), to.min(s + self.chunk_len - 1));
            let served = match self.chunk(s).await {
                Some(c) => match self.read_chunk(&c, filter, a, b).await {
                    Ok(rows) => Some(rows),
                    Err(e) => {
                        self.mark_bad(s, &e);
                        None
                    }
                },
                None => None,
            };
            match served {
                Some(rows) => {
                    if let Some((ra, rb)) = run.take() {
                        out.extend(self.inner.logs(filter, ra, rb).await?);
                    }
                    out.extend(rows);
                }
                None => run = Some((run.map_or(a, |r| r.0), b)),
            }
            s += self.chunk_len;
        }
        if let Some((ra, rb)) = run {
            out.extend(self.inner.logs(filter, ra, rb).await?);
        }
        Ok(out)
    }

    fn refuses_topic0_only(&self, err: &anyhow::Error) -> bool {
        self.inner.refuses_topic0_only(err)
    }
}

#[cfg(test)]
mod tests {
    //! The fixtures are real: three 4-block chunks of mainnet (blocks 26,462,624 to 26,462,635) cut
    //! from the public Xatu files for two busy contracts (USDC, USDT: 4,440 logs), beside the
    //! `eth_getLogs` and `eth_getBlockByNumber` answers Alchemy gave for the same blocks. The
    //! "RPC" in these tests replays those answers, so equality here is equality with the node.
    use super::*;
    use std::io::Read;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
    const USDT: &str = "0xdac17f958d2ee523a2206206994597c13d831ec7";
    const FIRST: u64 = 26_462_624;
    const LAST: u64 = 26_462_635;

    fn dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/xatu")
    }

    fn hex(v: &serde_json::Value) -> u64 {
        u64::from_str_radix(v.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
    }

    /// The node, as recorded.
    struct Recorded {
        logs: Vec<Log>,
        headers: Vec<(u64, String, u64)>,
        log_calls: Mutex<Vec<(u64, u64)>>,
        header_calls: AtomicUsize,
        /// Lie about this block's hash, to make the anchor disagree.
        wrong_hash_at: Option<u64>,
    }

    impl Recorded {
        fn load() -> Arc<Self> {
            Arc::new(Self::with(None))
        }
        fn with(wrong_hash_at: Option<u64>) -> Self {
            let mut raw = String::new();
            flate2::read::GzDecoder::new(
                std::fs::File::open(dir().join("eth_getLogs.json.gz")).unwrap(),
            )
            .read_to_string(&mut raw)
            .unwrap();
            let logs = serde_json::from_str::<Vec<serde_json::Value>>(&raw)
                .unwrap()
                .iter()
                .map(|l| Log {
                    address: l["address"].as_str().unwrap().into(),
                    topics: l["topics"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|t| t.as_str().unwrap().into())
                        .collect(),
                    data: l["data"].as_str().unwrap().into(),
                    block_number: hex(&l["blockNumber"]),
                    block_hash: l["blockHash"].as_str().unwrap().into(),
                    tx_hash: l["transactionHash"].as_str().unwrap().into(),
                    log_index: hex(&l["logIndex"]),
                })
                .collect();
            let headers = serde_json::from_str::<Vec<serde_json::Value>>(
                &std::fs::read_to_string(dir().join("headers.json")).unwrap(),
            )
            .unwrap()
            .iter()
            .map(|h| {
                (
                    hex(&h["number"]),
                    h["hash"].as_str().unwrap().into(),
                    hex(&h["timestamp"]),
                )
            })
            .collect();
            Self {
                logs,
                headers,
                log_calls: Mutex::new(Vec::new()),
                header_calls: AtomicUsize::new(0),
                wrong_hash_at,
            }
        }
        fn header(&self, n: u64) -> Option<&(u64, String, u64)> {
            self.headers.iter().find(|h| h.0 == n)
        }
        /// What `eth_getLogs` answered (or would), without counting a call.
        fn answer(&self, f: &LogFilter, from: u64, to: u64) -> Vec<Log> {
            self.logs
                .iter()
                .filter(|l| (from..=to).contains(&l.block_number) && f.matches(l))
                .cloned()
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl Source for Recorded {
        async fn tip(&self) -> Result<u64> {
            Ok(LAST + 100)
        }
        async fn block_hash(&self, n: u64) -> Result<Option<String>> {
            self.header_calls.fetch_add(1, Ordering::SeqCst);
            if self.wrong_hash_at == Some(n) {
                return Ok(Some(format!("0x{:064x}", 1)));
            }
            Ok(self.header(n).map(|h| h.1.clone()))
        }
        async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
            self.header_calls.fetch_add(1, Ordering::SeqCst);
            Ok(blocks
                .iter()
                .filter_map(|b| self.header(*b).map(|h| (*b, h.2)))
                .collect())
        }
        async fn logs(&self, f: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
            self.log_calls.lock().unwrap().push((from, to));
            Ok(self.answer(f, from, to))
        }
    }

    /// Serves the recorded files; `missing` chunks answer 404 and `corrupt` ones serve garbage.
    struct Files {
        missing: HashSet<u64>,
        corrupt_logs: HashSet<u64>,
        old_schema: bool,
        gets: AtomicUsize,
    }

    impl Files {
        fn all() -> Arc<Self> {
            Self::new(&[], &[])
        }
        fn new(missing: &[u64], corrupt_logs: &[u64]) -> Arc<Self> {
            Arc::new(Self {
                missing: missing.iter().copied().collect(),
                corrupt_logs: corrupt_logs.iter().copied().collect(),
                old_schema: false,
                gets: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait::async_trait]
    impl Fetch for Files {
        async fn get(&self, url: &str) -> Result<Option<Bytes>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let rel = url
                .strip_prefix("https://mirror.test/")
                .expect("asked the configured base");
            let start: u64 = rel
                .rsplit('/')
                .next()
                .unwrap()
                .trim_end_matches(".parquet")
                .parse()
                .unwrap();
            if self.missing.contains(&start) {
                return Ok(None);
            }
            if rel.starts_with("canonical_execution_logs") && self.corrupt_logs.contains(&start) {
                return Ok(Some(Bytes::from_static(b"not parquet at all")));
            }
            // The older shape: the same blocks, in files with no `parent_hash` column.
            let old = dir().join("old-schema").join(rel);
            let path = if self.old_schema && old.exists() {
                old
            } else {
                dir().join(rel)
            };
            match std::fs::read(path) {
                Ok(b) => Ok(Some(b.into())),
                Err(_) => Ok(None),
            }
        }
    }

    fn source(rpc: &Arc<Recorded>, files: &Arc<Files>) -> XatuSource {
        XatuSource::with_chunk_len(rpc.clone(), files.clone(), "https://mirror.test/", 4)
    }

    fn filter(addrs: &[&str]) -> LogFilter {
        let a: Vec<String> = addrs.iter().map(|s| s.to_string()).collect();
        LogFilter::new(&a, &[]).unwrap()
    }

    /// `Log` has no `PartialEq`; compare what it serialises to, which is every field.
    fn same(a: &[Log], b: &[Log]) {
        assert_eq!(a.len(), b.len(), "different number of logs");
        for (x, y) in a.iter().zip(b) {
            assert_eq!(
                serde_json::to_value(x).unwrap(),
                serde_json::to_value(y).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn logs_and_timestamps_are_exactly_what_the_node_said() {
        let (rpc, files) = (Recorded::load(), Files::all());
        let x = source(&rpc, &files);
        assert_eq!(
            rpc.logs.len(),
            4440,
            "premise: the whole recorded answer is under test"
        );

        // Whole range, a range inside and across chunks, one block, one contract, both.
        for (addrs, from, to) in [
            (&[USDC, USDT][..], FIRST, LAST),
            (&[USDC, USDT][..], FIRST + 1, LAST - 2),
            (&[USDC][..], FIRST + 3, FIRST + 4),
            (&[USDT][..], FIRST + 6, FIRST + 6),
        ] {
            let f = filter(addrs);
            same(
                &x.logs(&f, from, to).await.unwrap(),
                &rpc.answer(&f, from, to),
            );
        }
        // A narrowed filter (topic0 plus a topic position) is applied to the dataset's rows too.
        let transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        let narrowed = LogFilter::new(&[USDC.to_string()], &[transfer.to_string()])
            .unwrap()
            .with_topics([
                vec![rpc.logs[0].topics.get(1).cloned().unwrap_or_default()],
                vec![],
                vec![],
            ]);
        same(
            &x.logs(&narrowed, FIRST, LAST).await.unwrap(),
            &rpc.answer(&narrowed, FIRST, LAST),
        );

        let blocks: Vec<u64> = (FIRST..=LAST).collect();
        let ts = x.block_timestamps(&blocks).await.unwrap();
        let hashes = x.block_hashes(&blocks).await.unwrap();
        let records = x.block_records(&blocks).await.unwrap();
        for h in &rpc.headers {
            assert_eq!(ts[&h.0], h.2, "timestamp of block {}", h.0);
            assert_eq!(hashes[&h.0], h.1, "hash of block {}", h.0);
            assert_eq!(records[&h.0], (h.1.clone(), Some(h.2)));
        }
        assert_eq!(
            x.block_record(FIRST + 5).await.unwrap().unwrap().1,
            Some(rpc.header(FIRST + 5).unwrap().2)
        );

        // And it did that without the node: one hash per chunk to check it, no getLogs, no headers.
        assert!(rpc.log_calls.lock().unwrap().is_empty());
        assert_eq!(rpc.header_calls.load(Ordering::SeqCst), 3);
        assert_eq!(x.stats.anchor_calls.load(Ordering::SeqCst), 3);
        // Chunks are downloaded once however many windows ask.
        assert_eq!(files.gets.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn files_without_parent_hash_are_anchored_at_both_ends() {
        // Mainnet files before about block 26.4M have no `parent_hash` column (found by running a
        // 50,000-block backfill: the first version refused all of them and fell back, correctly but
        // uselessly). Same blocks, same logs, still exact, and two hash checks per chunk, not one.
        let rpc = Recorded::load();
        let files = Arc::new(Files {
            old_schema: true,
            ..Arc::try_unwrap(Files::all()).ok().unwrap()
        });
        let x = source(&rpc, &files);
        let f = filter(&[USDC, USDT]);
        same(
            &x.logs(&f, FIRST, LAST).await.unwrap(),
            &rpc.answer(&f, FIRST, LAST),
        );
        assert!(rpc.log_calls.lock().unwrap().is_empty());
        assert_eq!(rpc.header_calls.load(Ordering::SeqCst), 6);
        assert_eq!(x.stats.fallbacks.load(Ordering::SeqCst), 0);
        // And a hash that is wrong at the first block alone is caught.
        let rpc = Arc::new(Recorded::with(Some(FIRST + 4)));
        let x = source(&rpc, &files);
        same(
            &x.logs(&f, FIRST, LAST).await.unwrap(),
            &rpc.answer(&f, FIRST, LAST),
        );
        assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 4, FIRST + 7)]);
    }

    #[tokio::test]
    async fn the_handoff_to_rpc_has_no_gap_and_no_duplicate() {
        // The dataset's head is the end of the second chunk: the third answers 404.
        let (rpc, files) = (Recorded::load(), Files::new(&[FIRST + 8], &[]));
        let x = source(&rpc, &files);
        let f = filter(&[USDC, USDT]);

        let got = x.logs(&f, FIRST, LAST).await.unwrap();
        same(&got, &rpc.answer(&f, FIRST, LAST));
        assert_eq!(
            *rpc.log_calls.lock().unwrap(),
            vec![(FIRST + 8, LAST)],
            "RPC asked for the tail only"
        );
        let keys: HashSet<_> = got.iter().map(|l| (l.block_number, l.log_index)).collect();
        assert_eq!(keys.len(), got.len(), "no duplicate");

        // A window that starts mid-chunk and straddles the head.
        rpc.log_calls.lock().unwrap().clear();
        let got = x.logs(&f, FIRST + 6, FIRST + 9).await.unwrap();
        same(&got, &rpc.answer(&f, FIRST + 6, FIRST + 9));
        assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 8, FIRST + 9)]);

        // Headers follow the same split: dataset below the head, RPC above.
        let blocks: Vec<u64> = (FIRST..=LAST).collect();
        let ts = x.block_timestamps(&blocks).await.unwrap();
        for h in &rpc.headers {
            assert_eq!(ts[&h.0], h.2);
        }
        // The 404 is remembered, not asked again on the next poll.
        let before = files.gets.load(Ordering::SeqCst);
        x.logs(&f, FIRST + 8, LAST).await.unwrap();
        assert_eq!(files.gets.load(Ordering::SeqCst), before);
    }

    #[tokio::test]
    async fn a_chunk_that_will_not_parse_falls_back_to_rpc_for_that_chunk_only() {
        let (rpc, files) = (Recorded::load(), Files::new(&[], &[FIRST + 4]));
        let x = source(&rpc, &files);
        let f = filter(&[USDC, USDT]);
        let got = x.logs(&f, FIRST, LAST).await.unwrap();
        same(&got, &rpc.answer(&f, FIRST, LAST));
        assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 4, FIRST + 7)]);
        assert_eq!(x.stats.fallbacks.load(Ordering::SeqCst), 1);
        // Remembered as bad: no second download of the same garbage.
        let before = files.gets.load(Ordering::SeqCst);
        x.logs(&f, FIRST + 4, FIRST + 5).await.unwrap();
        assert_eq!(files.gets.load(Ordering::SeqCst), before);
    }

    #[tokio::test]
    async fn a_chunk_that_disagrees_with_the_chain_is_not_used() {
        // The node says the second chunk's last block has another hash: the dataset is on a fork,
        // stale, or wrong. Its logs must not be served.
        let rpc = Arc::new(Recorded::with(Some(FIRST + 7)));
        let files = Files::all();
        let x = source(&rpc, &files);
        let f = filter(&[USDC, USDT]);
        same(
            &x.logs(&f, FIRST, LAST).await.unwrap(),
            &rpc.answer(&f, FIRST, LAST),
        );
        assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 4, FIRST + 7)]);
        assert!(
            x.block_hash(FIRST + 5).await.unwrap().is_some(),
            "headers there come from RPC"
        );
        assert!(x.cached(FIRST + 5).is_none());
    }

    #[tokio::test]
    async fn a_reorg_notice_drops_what_it_could_reach() {
        let (rpc, files) = (Recorded::load(), Files::all());
        let x = source(&rpc, &files);
        x.logs(&filter(&[USDC]), FIRST, LAST).await.unwrap();
        assert!(x.cached(LAST).is_some());
        x.forget_cached_above(FIRST + 5);
        assert!(x.cached(FIRST + 1).is_some(), "a chunk wholly below stays");
        assert!(x.cached(LAST).is_none() && x.cached(FIRST + 5).is_none());
    }

    #[test]
    fn a_dataset_in_another_shape_is_refused_not_guessed() {
        assert!(hex_text(b"0xAbC").unwrap() == "0xabc");
        assert!(
            hex_text(&[0u8, 1, 2]).is_err(),
            "raw bytes are not the format this reads"
        );
        assert!(hex_text(b"deadbeef").is_err());
    }

    /// Not run in CI (it needs the network and an Alchemy key in `ALCHEMY_API_KEY`):
    /// `ALCHEMY_API_KEY=... XATU_BENCH_FROM=26300000 XATU_BENCH_BLOCKS=5000 cargo test --release
    /// --lib xatu_bench -- --ignored --nocapture`. Backfills USDC's logs and timestamps the way the
    /// indexer's windows do (adaptive window, timestamps for log-bearing blocks, a checkpoint per
    /// window) once through the RPC alone and once through the dataset, and prints wall time,
    /// HTTP requests to the RPC, and whether the two produced the same bytes.
    #[tokio::test]
    #[ignore]
    async fn xatu_bench() {
        use crate::chunker::AdaptiveWindow;
        use crate::rpc::RpcClient;
        let key = std::env::var("ALCHEMY_API_KEY").expect("ALCHEMY_API_KEY");
        let from: u64 = std::env::var("XATU_BENCH_FROM").unwrap().parse().unwrap();
        let n: u64 = std::env::var("XATU_BENCH_BLOCKS").unwrap().parse().unwrap();
        let f = filter(&[USDC]);

        async fn run(
            src: Arc<dyn Source>,
            f: &LogFilter,
            from: u64,
            n: u64,
        ) -> (usize, usize, String) {
            use std::hash::{Hash, Hasher};
            let mut w = AdaptiveWindow::for_window(20);
            let (mut next, end) = (from, from + n);
            let (mut total, mut windows) = (0, 0);
            let mut h = std::collections::hash_map::DefaultHasher::new();
            while next < end {
                let to = (next + w.window() - 1).min(end - 1);
                let logs = src.logs(f, next, to).await.unwrap();
                w.observed(logs.len() as u64);
                let mut blocks: Vec<u64> = logs.iter().map(|l| l.block_number).collect();
                blocks.dedup();
                let ts = src.block_timestamps(&blocks).await.unwrap();
                let (hash, _) = src.block_record(to).await.unwrap().unwrap();
                for l in &logs {
                    serde_json::to_string(l).unwrap().hash(&mut h);
                    ts.get(&l.block_number).hash(&mut h);
                }
                hash.hash(&mut h);
                total += logs.len();
                windows += 1;
                next = to + 1;
            }
            (total, windows, format!("{:016x}", h.finish()))
        }

        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .try_init();
        let url = format!("https://eth-mainnet.g.alchemy.com/v2/{key}");
        let skip_rpc = std::env::var("XATU_BENCH_SKIP_RPC").is_ok();
        let rpc = Arc::new(RpcClient::new(vec![url.clone()]).unwrap());
        let t = Instant::now();
        let a = if skip_rpc {
            (0, 0, String::new())
        } else {
            run(rpc.clone(), &f, from, n).await
        };
        println!(
            "rpc   : {:.1}s, {} logs, {} windows, {} http requests, digest {}",
            t.elapsed().as_secs_f32(),
            a.0,
            a.1,
            rpc.request_count(),
            a.2
        );

        let rpc = Arc::new(RpcClient::new(vec![url]).unwrap());
        let x = Arc::new(XatuSource::new(
            rpc.clone(),
            Arc::new(HttpFetch::new().unwrap()),
            DEFAULT_BASE_URL,
        ));
        let t = Instant::now();
        let b = run(x.clone(), &f, from, n).await;
        println!(
            "xatu  : {:.1}s, {} logs, {} windows, {} http requests, digest {} ({} chunks, {} MB downloaded, {} fallbacks)",
            t.elapsed().as_secs_f32(), b.0, b.1, rpc.request_count(), b.2,
            x.stats.chunks_loaded.load(Ordering::SeqCst), x.stats.bytes.load(Ordering::SeqCst) / 1_000_000,
            x.stats.fallbacks.load(Ordering::SeqCst)
        );
        println!("identical output: {}", a.0 == b.0 && a.2 == b.2);
    }
}
