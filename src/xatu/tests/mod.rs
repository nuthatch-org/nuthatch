//! The fixtures are real: three 4-block chunks of mainnet (blocks 26,462,624 to 26,462,635) cut
//! from the public Xatu files for two busy contracts (USDC, USDT: 4,440 logs), beside the
//! `eth_getLogs` and `eth_getBlockByNumber` answers Alchemy gave for the same blocks. The
//! "RPC" in these tests replays those answers, so equality here is equality with the node.
use super::columns::hex_text;
use super::*;
use crate::source::LogFilter;
use anyhow::Result;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

mod adversarial;
mod bench;

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
    /// The node's `finalized` block; `None` is a node that exposes none.
    finalized: Option<u64>,
    /// Say this block is a second later than it was, to make the timestamp anchor disagree.
    wrong_time_at: Option<u64>,
    /// Refuse every `eth_getLogs` while set: an RPC that is down or rate-limiting.
    logs_fail: std::sync::atomic::AtomicBool,
    /// One-block `eth_getLogs` inside this range wait for `release` (a spot check on a slow RPC).
    stall: Mutex<Option<(u64, u64)>>,
    stalled: std::sync::atomic::AtomicBool,
    release: tokio::sync::Notify,
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
            finalized: None,
            wrong_time_at: None,
            logs_fail: Default::default(),
            stall: Mutex::new(None),
            stalled: Default::default(),
            release: tokio::sync::Notify::new(),
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
    /// One header, as the RPC client answers it.
    async fn block_record(&self, n: u64) -> Result<Option<(String, Option<u64>)>> {
        let hash = self.block_hash(n).await?;
        let skew = u64::from(self.wrong_time_at == Some(n));
        Ok(hash.map(|h| (h, self.header(n).map(|h| h.2 + skew))))
    }
    async fn finalized(&self) -> Result<Option<u64>> {
        Ok(self.finalized)
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
        if self.logs_fail.load(Ordering::SeqCst) {
            anyhow::bail!("HTTP 429");
        }
        let stall = *self.stall.lock().unwrap();
        if matches!(stall, Some((lo, hi)) if from == to && (lo..=hi).contains(&from)) {
            self.stalled.store(true, Ordering::SeqCst);
            self.release.notified().await;
        }
        Ok(self.answer(f, from, to))
    }
}

/// Serves the recorded files; `missing` chunks answer 404 and `corrupt` ones serve garbage.
struct Files {
    missing: HashSet<u64>,
    corrupt_logs: HashSet<u64>,
    old_schema: bool,
    /// Chunks whose logs file fails to come down (a stalled or erroring host).
    failing_logs: HashSet<u64>,
    /// Chunks whose block file is published but not (yet) their logs file.
    missing_logs: HashSet<u64>,
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
            failing_logs: HashSet::new(),
            missing_logs: HashSet::new(),
            gets: AtomicUsize::new(0),
        })
    }
}

#[async_trait::async_trait]
impl Fetch for Files {
    async fn get(&self, url: &str, max: usize) -> Result<Option<Bytes>> {
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
        if rel.starts_with("canonical_execution_logs") && self.missing_logs.contains(&start) {
            return Ok(None);
        }
        if rel.starts_with("canonical_execution_logs") && self.failing_logs.contains(&start) {
            anyhow::bail!("{url}: HTTP 503");
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
            Ok(b) if b.len() > max => Err(crate::xatu::fetch::TooLarge {
                url: url.to_string(),
                max,
            }
            .into()),
            Ok(b) => Ok(Some(b.into())),
            Err(_) => Ok(None),
        }
    }
}

/// A source with the spot check off, for the tests of the handoff, whose RPC calls they assert
/// exactly. The spot check has its own tests, and the first test runs with it on.
fn source(rpc: &Arc<Recorded>, files: &Arc<Files>) -> XatuSource {
    let mut x = checked(rpc, files.clone());
    x.spot_checks = 0;
    x
}

/// A source as the operator gets it: the spot check on.
fn checked(rpc: &Arc<Recorded>, files: Arc<dyn Fetch>) -> XatuSource {
    XatuSource::with_chunk_len(rpc.clone(), files, "https://mirror.test/", 4)
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
    let x = checked(&rpc, files.clone());
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

    // And it did that without the node's logs: the only `eth_getLogs` are the spot checks, one block
    // each. Four distinct filters read the dataset, touching 3 + 2 + 1 + 3 = 9 (chunk, filter) pairs
    // (the repeated filter is already checked); each pair draws two blocks that may coincide, so one
    // or two checks a pair, and none again for a pair already checked.
    let calls = rpc.log_calls.lock().unwrap().clone();
    assert!(calls.iter().all(|(a, b)| a == b), "{calls:?}");
    assert_eq!(
        calls.len() as u64,
        x.stats.spot_checks.load(Ordering::SeqCst)
    );
    assert!(calls.len() >= 9 && calls.len() <= 2 * 9, "{}", calls.len());
    x.logs(&filter(&[USDC, USDT]), FIRST, LAST).await.unwrap();
    assert_eq!(rpc.log_calls.lock().unwrap().len(), calls.len());
    assert_eq!(x.stats.mismatches.load(Ordering::SeqCst), 0);
    // One hash per chunk to check it, no headers.
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
    assert_eq!(x.stats.absent.load(Ordering::SeqCst), 1);
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

/// A host that sends more than the cap (here: the cap the source asks with is exceeded) must fail
/// that chunk over to the RPC, once, not be retried or trusted.
#[tokio::test]
async fn an_oversized_logs_body_falls_back_to_rpc_for_that_chunk() {
    struct Huge(Arc<Files>, u64);
    #[async_trait::async_trait]
    impl Fetch for Huge {
        async fn get(&self, url: &str, max: usize) -> Result<Option<Bytes>> {
            if url.contains("canonical_execution_logs")
                && url.ends_with(&format!("/{}.parquet", self.1))
            {
                return Err(crate::xatu::fetch::TooLarge {
                    url: url.to_string(),
                    max,
                }
                .into());
            }
            self.0.get(url, max).await
        }
    }
    let (rpc, files) = (Recorded::load(), Files::all());
    let mut x = checked(&rpc, Arc::new(Huge(files.clone(), FIRST + 4)));
    x.spot_checks = 0;
    let f = filter(&[USDC, USDT]);
    same(
        &x.logs(&f, FIRST, LAST).await.unwrap(),
        &rpc.answer(&f, FIRST, LAST),
    );
    assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 4, FIRST + 7)]);
    assert_eq!(x.stats.fallbacks.load(Ordering::SeqCst), 1);
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
    // The anchor is checked before the logs file is downloaded: three block files, two logs files.
    assert_eq!(files.gets.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn a_reorg_notice_drops_what_it_could_reach() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let x = source(&rpc, &files);
    x.logs(&filter(&[USDC]), FIRST, LAST).await.unwrap();
    assert!(x.cached(LAST).is_some());
    // The trait forgets what is *above* the block: a chunk ending exactly there stays.
    x.forget_cached_above(FIRST + 7);
    assert!(x.cached(FIRST + 7).is_some() && x.cached(FIRST + 8).is_none());
    x.forget_cached_above(FIRST + 5);
    assert!(x.cached(FIRST + 1).is_some(), "a chunk wholly below stays");
    assert!(x.cached(LAST).is_none() && x.cached(FIRST + 5).is_none());
}

#[tokio::test]
async fn a_chunk_not_yet_final_is_left_to_rpc() {
    // A mirror that keeps up with the head: a held chunk would answer the indexer's reorg check
    // (`block_hash`) with its own word, so nothing above finality may be held.
    let rpc = Arc::new(Recorded {
        finalized: Some(FIRST + 5),
        ..Recorded::with(None)
    });
    let files = Files::all();
    let x = source(&rpc, &files);
    let f = filter(&[USDC, USDT]);
    same(
        &x.logs(&f, FIRST, LAST).await.unwrap(),
        &rpc.answer(&f, FIRST, LAST),
    );
    assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 4, LAST)]);
    assert!(x.cached(FIRST + 5).is_none() && x.cached(FIRST + 3).is_some());
    assert_eq!(
        files.gets.load(Ordering::SeqCst),
        2,
        "only the final chunk was fetched"
    );
}

/// On a paused clock. With a real one, the 300 ms "while" started when the logs file failed and
/// kept running while the first call parsed the remaining chunk, so a slow (debug, loaded) run spent
/// it before the second call was made and the second call fetched again (15/20 on a worker branch,
/// 20/20 here under a 2x CPU oversubscription). Only `advance` moves the deadline now.
#[tokio::test(start_paused = true)]
async fn a_failing_host_is_not_asked_again_by_every_window() {
    let rpc = Recorded::load();
    let files = Arc::new(Files {
        failing_logs: [FIRST + 4].into_iter().collect(),
        ..Arc::try_unwrap(Files::all()).ok().unwrap()
    });
    let mut x = source(&rpc, &files);
    x.retry_after = Duration::from_secs(60);
    let f = filter(&[USDC, USDT]);
    same(
        &x.logs(&f, FIRST, LAST).await.unwrap(),
        &rpc.answer(&f, FIRST, LAST),
    );
    assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 4, FIRST + 7)]);
    let before = files.gets.load(Ordering::SeqCst);
    x.logs(&f, FIRST + 4, FIRST + 7).await.unwrap();
    assert_eq!(
        files.gets.load(Ordering::SeqCst),
        before,
        "remembered for a while"
    );
    // Still remembered a moment before the while is over...
    tokio::time::advance(x.retry_after - Duration::from_millis(1)).await;
    x.logs(&f, FIRST + 4, FIRST + 7).await.unwrap();
    assert_eq!(
        files.gets.load(Ordering::SeqCst),
        before,
        "remembered for the whole while"
    );
    // ...and transient, not bad: once it is over it is tried again.
    tokio::time::advance(Duration::from_millis(2)).await;
    x.logs(&f, FIRST + 4, FIRST + 7).await.unwrap();
    assert!(files.gets.load(Ordering::SeqCst) > before);
}

#[tokio::test]
async fn headers_outlive_the_logs_of_a_window_wider_than_the_cache() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let mut x = source(&rpc, &files);
    x.keep_chunks = 1;
    let f = filter(&[USDC, USDT]);
    x.logs(&f, FIRST, LAST).await.unwrap();
    let blocks: Vec<u64> = (FIRST..=LAST).collect();
    let ts = x.block_timestamps(&blocks).await.unwrap();
    for h in &rpc.headers {
        assert_eq!(ts[&h.0], h.2);
    }
    assert_eq!(
        rpc.header_calls.load(Ordering::SeqCst),
        3,
        "the anchors only, no header call for a chunk whose logs were evicted"
    );
}

#[tokio::test]
async fn a_filter_matching_too_much_is_decoded_per_window_not_held() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let mut x = source(&rpc, &files);
    x.matched_budget = 1;
    let f = filter(&[USDC, USDT]);
    for (from, to) in [
        (FIRST, LAST),
        (FIRST + 1, FIRST + 2),
        (FIRST + 3, FIRST + 9),
    ] {
        same(
            &x.logs(&f, from, to).await.unwrap(),
            &rpc.answer(&f, from, to),
        );
    }
    assert!(rpc.log_calls.lock().unwrap().is_empty());
    assert_eq!(x.stats.fallbacks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_empty_range_is_the_rpcs_to_answer() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let x = source(&rpc, &files);
    let got = x
        .logs(&filter(&[USDC]), FIRST + 2, FIRST + 1)
        .await
        .unwrap();
    assert!(got.is_empty());
    assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 2, FIRST + 1)]);
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

#[tokio::test]
async fn decoded_rows_are_held_within_the_budget_across_chunks() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let f = filter(&[USDC, USDT]);
    let x = source(&rpc, &files);
    x.logs(&f, FIRST, LAST).await.unwrap();
    let all = x.matched_bytes();
    assert!(all > 0);

    let mut x = source(&rpc, &files);
    x.matched_budget = all - 1;
    for (from, to) in [(FIRST, LAST), (FIRST + 2, FIRST + 5), (FIRST + 9, LAST)] {
        same(
            &x.logs(&f, from, to).await.unwrap(),
            &rpc.answer(&f, from, to),
        );
        let held = x.matched_bytes();
        assert!(held > 0 && held < all, "{held} of {all} held");
    }
    assert!(rpc.log_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_chunk_whose_timestamp_disagrees_with_the_chain_is_not_used() {
    // Timestamps are sealed into every row: a file shifted by a unit or a time zone must not pass
    // on its hashes alone.
    let rpc = Arc::new(Recorded {
        wrong_time_at: Some(FIRST + 7),
        ..Recorded::with(None)
    });
    let files = Files::all();
    let x = source(&rpc, &files);
    let f = filter(&[USDC, USDT]);
    same(
        &x.logs(&f, FIRST, LAST).await.unwrap(),
        &rpc.answer(&f, FIRST, LAST),
    );
    assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 4, FIRST + 7)]);
    assert!(x.cached(FIRST + 5).is_none());
}

#[tokio::test]
async fn a_logs_file_not_yet_published_is_tried_again_not_given_up_on() {
    let rpc = Recorded::load();
    let files = Arc::new(Files {
        missing_logs: [FIRST + 8].into_iter().collect(),
        ..Arc::try_unwrap(Files::all()).ok().unwrap()
    });
    let mut x = source(&rpc, &files);
    x.retry_after = Duration::from_millis(100);
    let f = filter(&[USDC, USDT]);
    same(
        &x.logs(&f, FIRST, LAST).await.unwrap(),
        &rpc.answer(&f, FIRST, LAST),
    );
    assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST + 8, LAST)]);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let before = files.gets.load(Ordering::SeqCst);
    x.logs(&f, FIRST + 8, LAST).await.unwrap();
    assert!(
        files.gets.load(Ordering::SeqCst) > before,
        "asked again, not marked bad"
    );
}

/// A logs file damaged in its footer: the Parquet reader can panic decoding the embedded schema
/// (arrow-ipc's `unimplemented!` on an unknown type), and that must fail the chunk over to RPC, not
/// unwind through the indexer's task.
#[tokio::test]
async fn a_damaged_logs_footer_falls_back_rather_than_panicking() {
    struct Flipped(Arc<Files>, usize);
    #[async_trait::async_trait]
    impl Fetch for Flipped {
        async fn get(&self, url: &str, max: usize) -> Result<Option<Bytes>> {
            let b = self.0.get(url, max).await?;
            if !url.contains("canonical_execution_logs") {
                return Ok(b);
            }
            Ok(b.map(|b| {
                let mut v = b.to_vec();
                let at = v.len() - self.1;
                v[at] ^= 0x01;
                v.into()
            }))
        }
    }
    let rpc = Recorded::load();
    let f = filter(&[USDC, USDT]);
    let want = rpc.answer(&f, FIRST, FIRST + 3);
    let mut fell_back = 0;
    for back in (9..600).step_by(3) {
        let x = XatuSource::with_chunk_len(
            rpc.clone(),
            Arc::new(Flipped(Files::all(), back)),
            "https://mirror.test/",
            4,
        );
        let got = x.logs(&f, FIRST, FIRST + 3).await.unwrap();
        if x.stats.fallbacks.load(Ordering::SeqCst) > 0 {
            fell_back += 1;
            same(&got, &want);
        }
    }
    assert!(fell_back > 0, "premise: some flips reach the footer parse");
}

/// How [`rewrite_logs`] changes a row: the file stays well-formed, only what it says changes.
enum Edit {
    Keep,
    Drop,
    Data(Vec<u8>),
}

/// Re-encode a logs file with `edit` applied to each row (its index in the file, its `data`): a
/// file that is wrong and well-formed, which no checksum or parse can catch.
fn rewrite_logs(b: Bytes, edit: &dyn Fn(usize, Option<&[u8]>) -> Edit) -> Bytes {
    use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let reader = ParquetRecordBatchReaderBuilder::try_new(b)
        .unwrap()
        .build()
        .unwrap();
    let mut batches = Vec::new();
    let mut row = 0;
    for batch in reader {
        let batch = batch.unwrap();
        let i = batch.schema().index_of("data").unwrap();
        let data =
            arrow::compute::cast(batch.column(i), &arrow::datatypes::DataType::Binary).unwrap();
        let data = data.as_any().downcast_ref::<BinaryArray>().unwrap();
        let (mut keep, mut new) = (Vec::new(), BinaryBuilder::new());
        for r in 0..batch.num_rows() {
            let v = (!data.is_null(r)).then(|| data.value(r));
            let e = edit(row, v);
            keep.push(!matches!(e, Edit::Drop));
            match e {
                Edit::Data(d) => new.append_value(d),
                Edit::Keep | Edit::Drop => new.append_option(v),
            }
            row += 1;
        }
        let new: ArrayRef =
            arrow::compute::cast(&new.finish(), batch.column(i).data_type()).unwrap();
        let mut cols = batch.columns().to_vec();
        cols[i] = new;
        let batch = arrow::record_batch::RecordBatch::try_new(batch.schema(), cols).unwrap();
        let batch = arrow::compute::filter_record_batch(&batch, &BooleanArray::from(keep)).unwrap();
        batches.push(batch);
    }
    let mut out = Vec::new();
    let mut w = parquet::arrow::ArrowWriter::try_new(&mut out, batches[0].schema(), None).unwrap();
    for batch in &batches {
        w.write(batch).unwrap();
    }
    w.close().unwrap();
    out.into()
}

/// Decides each row's [`Edit`] from its index in the file and its `data`.
type EditFn = fn(usize, Option<&[u8]>) -> Edit;

/// Serves the recorded files with one chunk's logs file rewritten.
struct Tampered(Arc<Files>, u64, EditFn);

#[async_trait::async_trait]
impl Fetch for Tampered {
    async fn get(&self, url: &str, max: usize) -> Result<Option<Bytes>> {
        let b = self.0.get(url, max).await?;
        if url.contains("canonical_execution_logs")
            && url.ends_with(&format!("/{}.parquet", self.1))
        {
            return Ok(b.map(|b| rewrite_logs(b, &self.2)));
        }
        Ok(b)
    }
}

/// One hex digit of a log's data changed: the same shape, another amount.
fn flipped(d: &[u8]) -> Vec<u8> {
    let mut d = d.to_vec();
    if let Some(last) = d.last_mut() {
        *last = if *last == b'0' { b'1' } else { b'0' };
    }
    d
}

#[tokio::test]
async fn a_log_edited_in_a_well_formed_file_is_caught_and_the_dataset_is_dropped() {
    // #3091: the file parses, its anchors match, and one log in the second chunk says something else
    // (an edited amount) or is missing. With every block checked it is caught wherever it is, on
    // both decode paths (rows held whole, and decoded per window past the budget).
    let edits: [(&str, EditFn); 2] = [
        ("flipped data", |r, d| match (r, d) {
            (0, Some(d)) => Edit::Data(flipped(d)),
            _ => Edit::Keep,
        }),
        (
            "missing log",
            |r, _| if r == 0 { Edit::Drop } else { Edit::Keep },
        ),
    ];
    for (what, edit) in edits {
        for budget in [chunks::MATCHED_BUDGET, 1] {
            let rpc = Recorded::load();
            let files = Files::all();
            let mut x = checked(&rpc, Arc::new(Tampered(files.clone(), FIRST + 4, edit)));
            x.spot_checks = 4;
            x.matched_budget = budget;
            let f = filter(&[USDC, USDT]);
            same(
                &x.logs(&f, FIRST, LAST).await.unwrap(),
                &rpc.answer(&f, FIRST, LAST),
            );
            assert_eq!(x.stats.mismatches.load(Ordering::SeqCst), 1, "{what}");
            // The first chunk was checked and served; from the bad one on, everything is the RPC's,
            // and the third chunk is not even downloaded.
            let calls = rpc.log_calls.lock().unwrap().clone();
            assert_eq!(calls.last(), Some(&(FIRST + 4, LAST)), "{what}: {calls:?}");
            assert!(calls[..calls.len() - 1].iter().all(|(a, b)| a == b));
            assert_eq!(files.gets.load(Ordering::SeqCst), 4, "{what}");
            // Not trusted again this run, for logs or headers, even the chunk that passed.
            assert!(x.cached(FIRST).is_none());
            rpc.log_calls.lock().unwrap().clear();
            same(
                &x.logs(&f, FIRST, FIRST + 3).await.unwrap(),
                &rpc.answer(&f, FIRST, FIRST + 3),
            );
            assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST, FIRST + 3)]);
        }
    }
}

#[tokio::test]
async fn a_fault_across_a_file_is_caught_by_the_default_sample() {
    // A writer bug touching every log of a file: the draw weighted by matched rows always lands
    // on one, so two calls are enough.
    let rpc = Recorded::load();
    let tampered = Tampered(Files::all(), FIRST + 4, |_, d| match d {
        Some(d) => Edit::Data(flipped(d)),
        None => Edit::Keep,
    });
    let x = checked(&rpc, Arc::new(tampered));
    let f = filter(&[USDC]);
    same(
        &x.logs(&f, FIRST, LAST).await.unwrap(),
        &rpc.answer(&f, FIRST, LAST),
    );
    assert_eq!(x.stats.mismatches.load(Ordering::SeqCst), 1);
    assert!(x.stats.spot_checks.load(Ordering::SeqCst) <= 4);
}

#[tokio::test]
async fn an_rpc_that_cannot_spot_check_is_not_evidence_against_the_dataset() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let x = checked(&rpc, files.clone());
    let f = filter(&[USDC, USDT]);
    rpc.logs_fail.store(true, Ordering::SeqCst);
    assert!(
        x.logs(&f, FIRST, FIRST + 3).await.is_err(),
        "unchecked rows are not served: the window is the RPC's, and the RPC is down"
    );
    rpc.logs_fail.store(false, Ordering::SeqCst);
    rpc.log_calls.lock().unwrap().clear();
    same(
        &x.logs(&f, FIRST, FIRST + 3).await.unwrap(),
        &rpc.answer(&f, FIRST, FIRST + 3),
    );
    // Checked this time and served from the dataset, not marked bad, not distrusted.
    let calls = rpc.log_calls.lock().unwrap().clone();
    assert!(
        !calls.is_empty() && calls.iter().all(|(a, b)| a == b),
        "{calls:?}"
    );
    assert_eq!(x.stats.mismatches.load(Ordering::SeqCst), 0);
    assert!(x.cached(FIRST).is_some());
}

#[tokio::test]
async fn a_slow_spot_check_does_not_hold_up_reads_of_other_chunks() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let x = checked(&rpc, files.clone());
    let f = filter(&[USDC, USDT]);
    *rpc.stall.lock().unwrap() = Some((FIRST, FIRST + 3));
    let first = x.logs(&f, FIRST, FIRST + 3);
    tokio::pin!(first);
    tokio::select! {
        _ = &mut first => panic!("the first chunk's spot check should be stalled"),
        _ = async {
            while !rpc.stalled.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        } => {}
    }
    // The first chunk's check waits on the RPC; the next chunk decodes, checks and serves anyway.
    let second = tokio::time::timeout(Duration::from_secs(10), x.logs(&f, FIRST + 4, FIRST + 7))
        .await
        .expect("a read of another chunk waited for the first chunk's spot check")
        .unwrap();
    same(&second, &rpc.answer(&f, FIRST + 4, FIRST + 7));
    *rpc.stall.lock().unwrap() = None;
    rpc.release.notify_one();
    same(&first.await.unwrap(), &rpc.answer(&f, FIRST, FIRST + 3));
    assert_eq!(x.stats.mismatches.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_dataset_distrusted_mid_read_serves_nothing_more_and_caches_nothing() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let x = checked(&rpc, files.clone());
    let f = filter(&[USDC, USDT]);
    same(
        &x.logs(&f, FIRST, FIRST + 3).await.unwrap(),
        &rpc.answer(&f, FIRST, FIRST + 3),
    );
    let held = x.chunk(FIRST).await.expect("the first chunk is held");
    // Another window's check fails while this one holds the chunk.
    x.distrust(&anyhow::anyhow!("test"));
    let e = x.read_chunk(&held, &f, FIRST, FIRST + 3).await.unwrap_err();
    assert!(e.is::<chunks::Distrusted>(), "{e:#}");
    assert!(x.cached(FIRST).is_none());
    assert!(x.chunk(FIRST + 4).await.is_none());
    rpc.log_calls.lock().unwrap().clear();
    same(
        &x.logs(&f, FIRST, FIRST + 3).await.unwrap(),
        &rpc.answer(&f, FIRST, FIRST + 3),
    );
    assert_eq!(*rpc.log_calls.lock().unwrap(), vec![(FIRST, FIRST + 3)]);
}

#[test]
fn spot_checks_are_drawn_inside_the_chunk_at_most_two_per_chunk() {
    let (rpc, files) = (Recorded::load(), Files::all());
    let mut x = checked(&rpc, files);
    x.chunk_len = DATASET_CHUNK;
    let rows: Vec<Log> = rpc.logs.iter().take(50).cloned().collect();
    for start in (0..100).map(|i| i * DATASET_CHUNK) {
        let b = x.spot_blocks(start, None);
        assert!(!b.is_empty() && b.len() <= 2);
        assert!(b.iter().all(|n| (start..start + DATASET_CHUNK).contains(n)));
    }
    // The weighted draw lands on a block with rows.
    let b = x.spot_blocks(FIRST / DATASET_CHUNK * DATASET_CHUNK, Some(&rows));
    assert!(rows.iter().any(|l| l.block_number == *b.last().unwrap()));
}
