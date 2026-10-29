//! RFC-0075 S1: one window through `nuthatch-core`'s `fetch_window` and `process_window`, on the
//! inline driver, over an in-memory hot store, an in-memory segment sink and a tape source. The same
//! function runs natively and on wasm32; both must report the same rows, balances and segment hash.

use anyhow::Result;
use nuthatch_core::alerts::{AlertRouter, AlertSink};
use nuthatch_core::cursor::{process_window, Nest, Processed, Window};
use nuthatch_core::drive::Driver;
use nuthatch_core::entity_view::{Batch, WindowEntity};
use nuthatch_core::exposure::ExposureView;
use nuthatch_core::labels::LabelSet;
use nuthatch_core::metrics::NoMetrics;
use nuthatch_core::seal::Sinks;
use nuthatch_core::source::{Log, LogFilter, Source};
use nuthatch_core::store::{HotStore, Lease, LAST_BLOCK_KEY, SEALED_THROUGH_KEY, START_BLOCK_KEY};
use nuthatch_core::time::{Clock, Duration};
use nuthatch_core::velocity::VelocityView;
use nuthatch_core::views::BalanceView;
use nuthatch_core::window::{fetch_window, Finality, Wanted};
use nuthatch_decode::registry::{DecodeRegistry, DecodedRow};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

const ALICE: &str = "0x1111111111111111111111111111111111111111";
const BOB: &str = "0x2222222222222222222222222222222222222222";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const TOKEN: &str = "0x3333333333333333333333333333333333333333";
const OTHER: &str = "0x4444444444444444444444444444444444444444";
const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

fn hash_of(block: u64) -> String {
    format!("0x{block:064x}")
}

/// A recorded chain: every block has a hash and a timestamp derived from its number, and `logs` are
/// answered from the tape by the filter's addresses and topic0s.
struct Tape(Vec<Log>);

#[async_trait::async_trait]
impl Source for Tape {
    async fn tip(&self) -> Result<u64> {
        Ok(self.0.iter().map(|l| l.block_number).max().unwrap_or(0))
    }
    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        Ok(Some(hash_of(number)))
    }
    async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        Ok(blocks
            .iter()
            .map(|&b| (b, 1_700_000_000 + b * 12))
            .collect())
    }
    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
        let matches = |want: &[String], got: &str| {
            want.is_empty() || want.iter().any(|w| w.eq_ignore_ascii_case(got))
        };
        Ok(self
            .0
            .iter()
            .filter(|l| (from..=to).contains(&l.block_number))
            .filter(|l| matches(filter.addresses(), &l.address))
            .filter(|l| matches(filter.topic0s(), &l.topics[0]))
            .cloned()
            .collect())
    }
}

/// The window path feeds no entity here.
pub(crate) enum NoEntity {}

impl WindowEntity for NoEntity {
    fn name(&self) -> &str {
        match *self {}
    }
    fn bind_window(&self, _: &[DecodedRow], _: dbsp::ZWeight) -> Result<Option<Batch>> {
        match *self {}
    }
    fn apply(&self, _: Batch, _: u64) {
        match *self {}
    }
    fn fault(&self) -> Option<String> {
        match *self {}
    }
    fn applied_through(&self) -> u64 {
        match *self {}
    }
    fn flush(&self) {
        match *self {}
    }
}

/// Only what a window reads and writes; the rest is never reached. `committed` keeps every row a
/// commit wrote, since sealing prunes them from `rows`.
#[derive(Default)]
struct Mem {
    committed: Mutex<Vec<(String, String)>>,
    rows: Mutex<BTreeMap<(u64, u64), String>>,
    meta: Mutex<HashMap<String, String>>,
    hashes: Mutex<BTreeMap<u64, String>>,
}

fn key_parts(key: &str) -> Option<(u64, u64)> {
    let (b, i) = key.split_once('-')?;
    Some((b.parse().ok()?, i.parse().ok()?))
}

impl Mem {
    fn put(&self, block: u64, log: u64, json: String) {
        self.rows.lock().unwrap().insert((block, log), json);
    }
    fn set(&self, key: &str, value: u64) {
        self.meta
            .lock()
            .unwrap()
            .insert(key.into(), value.to_string());
    }
}

#[async_trait::async_trait]
impl HotStore for Mem {
    fn entities_in_range(&self, from: u64, to: u64) -> Result<Vec<String>> {
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .range((from, 0)..=(to, u64::MAX))
            .map(|(_, v)| v.clone())
            .collect())
    }
    fn get_meta(&self, key: &str) -> Result<Option<String>> {
        Ok(self.meta.lock().unwrap().get(key).cloned())
    }
    fn rollback_to_and_set_meta(
        &self,
        block: u64,
        meta_key: &str,
        meta_val: &str,
        _outbox: &[String],
    ) -> Result<u64> {
        let mut rows = self.rows.lock().unwrap();
        let before = rows.len();
        rows.retain(|(b, _), _| *b <= block);
        self.meta
            .lock()
            .unwrap()
            .insert(meta_key.into(), meta_val.into());
        Ok((before - rows.len()) as u64)
    }

    fn put_entity(&self, _: &str, _: &str) -> Result<()> {
        unreachable!()
    }
    fn put_entity_if_named(&self, _: &str, _: &str, _: &str, _: &str) -> Result<bool> {
        unreachable!()
    }
    fn put_entities_if_named(&self, _: &[(String, String)], _: &str, _: &str) -> Result<bool> {
        unreachable!()
    }
    fn get_entity(&self, key: &str) -> Result<Option<String>> {
        let rows = self.rows.lock().unwrap();
        Ok(key_parts(key).and_then(|k| rows.get(&k).cloned()))
    }
    fn count(&self) -> Result<u64> {
        Ok(self.rows.lock().unwrap().len() as u64)
    }
    fn recent(&self, _: usize) -> Result<Vec<String>> {
        unreachable!()
    }
    fn recent_by_table(&self, _: &str, _: usize) -> Result<Vec<String>> {
        unreachable!()
    }
    fn hot_rows_by_table(&self) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        unreachable!()
    }
    fn hot_rows_by_table_bounded(
        &self,
        _: usize,
    ) -> Result<HashMap<String, Vec<serde_json::Value>>> {
        unreachable!()
    }
    fn hot_rows_charged(
        &self,
        _: usize,
        _: u64,
        _: &mut dyn FnMut(u64) -> Result<()>,
    ) -> Result<nuthatch_core::store::HotRowsSnapshot> {
        unreachable!()
    }
    fn sample_entity_keys(&self, _: usize) -> Result<Vec<String>> {
        unreachable!()
    }
    fn commit_window(&self, _: &[(String, String)], _: Option<(u64, &str)>, _: u64) -> Result<()> {
        unreachable!()
    }
    fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.meta.lock().unwrap().insert(key.into(), value.into());
        Ok(())
    }
    fn meta_with_prefix(&self, _: &str, _: usize) -> Result<Vec<(String, String)>> {
        unreachable!()
    }
    fn count_meta_with_prefix(&self, _: &str) -> Result<u64> {
        unreachable!()
    }
    fn indexed_head(&self) -> Result<Option<u64>> {
        unreachable!()
    }
    fn sealed_through(&self) -> u64 {
        self.get_meta(SEALED_THROUGH_KEY)
            .unwrap()
            .map_or(0, |v| v.parse().unwrap())
    }
    fn set_block_hash(&self, block: u64, hash: &str) -> Result<()> {
        self.hashes.lock().unwrap().insert(block, hash.into());
        Ok(())
    }
    fn get_block_hash(&self, block: u64) -> Result<Option<String>> {
        Ok(self.hashes.lock().unwrap().get(&block).cloned())
    }
    fn set_block_timestamp(&self, _: u64, _: u64) -> Result<()> {
        unreachable!()
    }
    fn get_block_timestamp(&self, _: u64) -> Result<Option<u64>> {
        unreachable!()
    }
    fn checkpoints_desc(&self) -> Result<Vec<(u64, String)>> {
        unreachable!()
    }
    async fn commit_window_blocking(
        &self,
        rows: Vec<(String, String)>,
        _: Vec<String>,
        checkpoint: Option<(u64, String)>,
        last_block: u64,
    ) -> Result<()> {
        self.committed.lock().unwrap().extend(rows.iter().cloned());
        for (key, json) in rows {
            let (block, log) = key_parts(&key).expect("a row key");
            self.put(block, log, json);
        }
        if let Some((block, record)) = checkpoint {
            self.set_block_hash(block, &nuthatch_core::store::decode_block_record(&record).0)?;
        }
        self.set(LAST_BLOCK_KEY, last_block);
        Ok(())
    }
    async fn settle_commits(&self) {}
    fn rollback_to(&self, _: u64) -> Result<u64> {
        unreachable!()
    }
    fn prune_range(&self, _: u64, _: u64) -> Result<u64> {
        unreachable!()
    }
    fn prune_and_set_meta(&self, from: u64, to: u64, key: &str, value: &str) -> Result<u64> {
        let mut rows = self.rows.lock().unwrap();
        let before = rows.len();
        rows.retain(|(b, _), _| !(from..=to).contains(b));
        self.meta.lock().unwrap().insert(key.into(), value.into());
        Ok((before - rows.len()) as u64)
    }
    fn claim(&self, _: &str) -> Result<u64> {
        unreachable!()
    }
    fn acquire_lease(&self, _: &str, _: u64) -> Result<Lease> {
        unreachable!()
    }
    fn renew_lease(&self, _: u64) -> Result<Lease> {
        unreachable!()
    }
    fn release_lease(&self) -> Result<()> {
        unreachable!()
    }
    fn current_lease(&self) -> Result<Option<Lease>> {
        unreachable!()
    }
    fn current_fence(&self) -> Result<u64> {
        unreachable!()
    }
    fn held_fence(&self) -> u64 {
        unreachable!()
    }
    fn outbox_push(&self, _: &str) -> Result<u64> {
        unreachable!()
    }
    fn outbox_pending(&self, _: usize) -> Result<Vec<(u64, String)>> {
        unreachable!()
    }
    fn outbox_remove(&self, _: u64) -> Result<()> {
        unreachable!()
    }
    async fn outbox_remove_batch_blocking(&self, _: Vec<u64>) -> Result<()> {
        unreachable!()
    }
    fn outbox_len(&self) -> u64 {
        unreachable!()
    }
    fn outbox_trim(&self, _: u64) -> Result<u64> {
        unreachable!()
    }
}
/// Segments held in memory.
#[derive(Default)]
pub(crate) struct Shelf {
    manifest: Mutex<nuthatch_core::seal::Manifest>,
    files: Mutex<HashMap<String, Vec<u8>>>,
}

struct ShelfSink<'a>(&'a Shelf);

impl nuthatch_core::seal::SegmentSink for ShelfSink<'_> {
    fn manifest(&mut self) -> Result<nuthatch_core::seal::Manifest> {
        Ok(self.0.manifest.lock().unwrap().clone())
    }
    fn intact(&self, s: &nuthatch_seal::cut::Segment) -> bool {
        self.0.files.lock().unwrap().contains_key(&s.file)
    }
    fn restore(&mut self, _: &nuthatch_seal::cut::Segment, _: &[u8]) -> Result<()> {
        unreachable!()
    }
    fn rows(&self, _: &nuthatch_seal::cut::Segment) -> Result<Vec<serde_json::Value>> {
        unreachable!("a table floor of 0 leaves nothing provisional to fold")
    }
    fn table_floor(&self) -> usize {
        0
    }
    fn publish(
        &mut self,
        file: &str,
        _: &str,
        bytes: &[u8],
        _: Option<&nuthatch_seal::cut::Segment>,
    ) -> Result<()> {
        self.0
            .files
            .lock()
            .unwrap()
            .insert(file.into(), bytes.to_vec());
        Ok(())
    }
    fn install(&mut self, manifest: &nuthatch_core::seal::Manifest) -> Result<()> {
        *self.0.manifest.lock().unwrap() = manifest.clone();
        Ok(())
    }
}

impl Sinks for Shelf {
    fn open(&self) -> Box<dyn nuthatch_core::seal::SegmentSink + '_> {
        Box::new(ShelfSink(self))
    }
}

/// Sleeps for no time at all.
pub(crate) struct NoWait;

impl Clock for NoWait {
    fn sleep(
        &self,
        _: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(std::future::ready(()))
    }
}

/// Polls `f` to completion. Nothing on the window path waits on anything but itself.
pub(crate) fn block_on<F: std::future::Future>(f: F) -> F::Output {
    let mut f = std::pin::pin!(f);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let std::task::Poll::Ready(out) = f.as_mut().poll(&mut cx) {
            return out;
        }
    }
}

pub(crate) fn token() -> DecodeRegistry {
    DecodeRegistry::build(vec![nuthatch_decode::registry::ContractSpec {
        alias: "tok".into(),
        address: [0x33; 20].into(),
        abi: serde_json::from_str(
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap(),
        events: Vec::new(),
    }])
    .unwrap()
}

fn transfer_log(block: u64, log_index: u64, from: &str, to: &str, value: u64) -> Log {
    let topic = |a: &str| format!("0x{:0>64}", &a[2..]);
    Log {
        address: TOKEN.into(),
        topics: vec![TRANSFER.into(), topic(from), topic(to)],
        data: format!("0x{value:064x}"),
        block_number: block,
        block_hash: hash_of(block),
        tx_hash: hash_of(block),
        log_index,
    }
}

/// Fetches blocks 10..=12 through `fetch_window` (the tape also holds a foreign contract's transfer
/// the filter must leave out), processes them, and reports what was committed, the balances after,
/// and every sealed segment with the sha256 of the bytes the sink was handed.
pub fn run_window() -> Result<String, String> {
    run().map_err(|e| format!("{e:#}"))
}

fn run() -> Result<String> {
    let store = Mem::default();
    store.set(START_BLOCK_KEY, 10);
    let registry = token();
    let schema = registry.schema();
    let balances = BalanceView::start(Driver::Inline)?;
    let exposure = ExposureView::start(Driver::Inline, false)?;
    let velocity = VelocityView::start(Driver::Inline, false)?;
    let labels = LabelSet::default();
    let router = AlertRouter::new(Vec::<AlertSink>::new());
    let shelf = Shelf::default();
    let mut foreign = transfer_log(11, 0, ALICE, BOB, 7);
    foreign.address = OTHER.into();
    let tape = Tape(vec![
        transfer_log(12, 0, ALICE, BOB, 40),
        foreign,
        transfer_log(10, 3, ZERO, ALICE, 100),
    ]);

    let addresses = vec![TOKEN.to_string()];
    let topic0s = vec![TRANSFER.to_string()];
    let mut topic0_refused = false;
    let wanted = Wanted {
        registry: &registry,
        factory: None,
        children: &nuthatch_core::factory::ChildRegistry::new(),
        addresses: &addresses,
        topic0s: &topic0s,
        topic0_refused: &mut topic0_refused,
    };
    let logs = block_on(fetch_window(wanted, &tape, 10, 12))?;

    let window = Window {
        nest: Nest {
            name: "tok",
            store: &store,
            schema: &schema,
            balances: &balances,
            exposure: &exposure,
            velocity: &velocity,
            labels: &labels,
            velocity_flag: None,
            entities: &[] as &[NoEntity],
            children: None,
            router: &router,
            metrics: &NoMetrics,
        },
        registry: &registry,
        factory: None,
        threshold: None,
        finality: Finality::Depth(0),
        seal_span: 1,
        sinks: &shelf,
        extra: &nuthatch_core::extra::NoExtraRows,
        hooks: &nuthatch_core::hooks::NoHooks,
        faults: &nuthatch_core::fault::NoFaults,
        clock: &NoWait,
        deadline: None,
    };
    let processed = block_on(process_window(window, &tape, &logs, 10, 12, 12))?;
    anyhow::ensure!(processed == Processed::Stored(2), "processed {processed:?}");
    balances.flush();
    anyhow::ensure!(balances.is_healthy(), "the balance view died");

    let files = shelf.files.lock().unwrap();
    let manifest = shelf.manifest.lock().unwrap();
    let mut segments = Vec::new();
    for (table, segs) in &manifest.tables {
        for s in segs {
            let bytes = files
                .get(&s.file)
                .ok_or_else(|| anyhow::anyhow!("{} unpublished", s.file))?;
            let sha = nuthatch_seal::content_hash(bytes);
            anyhow::ensure!(
                sha == s.hash,
                "{} hashes to {sha}, catalogued {}",
                s.file,
                s.hash
            );
            segments.push(format!(
                "{table} {}..={} rows={} {sha}",
                s.from_block, s.to_block, s.rows
            ));
        }
    }
    let committed = store.committed.lock().unwrap();
    let rows: String = committed
        .iter()
        .map(|(k, j)| format!("{k}\t{j}\n"))
        .collect();
    Ok(format!(
        "committed={} rows_sha256={} balances={:?} last={:?} sealed_through={} hot={} segments={:?}",
        committed.len(),
        nuthatch_seal::content_hash(rows.as_bytes()),
        balances.top(10),
        store.get_meta(LAST_BLOCK_KEY)?,
        store.sealed_through(),
        store.count()?,
        segments
    ))
}
