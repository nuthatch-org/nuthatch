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

#[tokio::test]
async fn a_failing_host_is_not_asked_again_by_every_window() {
    let rpc = Recorded::load();
    let files = Arc::new(Files {
        failing_logs: [FIRST + 4].into_iter().collect(),
        ..Arc::try_unwrap(Files::all()).ok().unwrap()
    });
    let mut x = source(&rpc, &files);
    x.retry_after = Duration::from_millis(300);
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
    // Transient, not bad: once the while is over it is tried again.
    tokio::time::sleep(x.retry_after + Duration::from_millis(50)).await;
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
