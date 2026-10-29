//! RFC-0039: the recorded tape - record RPC once, replay it from disk.
//!
//! A benchmark measured against a live endpoint measures the endpoint's mood. meticulous-magpie found
//! a **3.8x spread inside a single arm** in one session, and seal-direct reading 0.92x - slower than
//! the path it is supposed to beat by an order of magnitude - contradicting a 5.2x taken hours earlier
//! at the same commit (#722, #744). Until a benchmark is a function of the code alone, no number this
//! project publishes about its own performance means very much.
//!
//! Recording happens at the [`Source`] boundary rather than at HTTP. That is the layer `indexer.rs`
//! and `bench.rs` already call through, so nothing new has to be invented; it collapses `RpcClient`'s
//! internal failover and retry noise into one deterministic outcome per logical call; and it keeps the
//! recorded surface to eight methods instead of an open-ended JSON-RPC method set.

use crate::rpc::Log;
use crate::source::{LogFilter, Source};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// What one recorded call returned.
///
/// **Errors are recorded, not just successes**, and this is the detail that makes replay trustworthy
/// rather than merely plausible. The adaptive window controller (RFC-0028/0029 §6f) grows and shrinks
/// in response to what came back, *including* a provider-cap error - `backfill_direct_factory`'s pass
/// two shrinks on exactly that signal. A recorder that captured only `Ok` would let replay skip the
/// cap event that drove a real shrink, the window trajectory would diverge from the workload actually
/// measured, and the rig's whole premise would be false without anyone noticing.
///
/// Error strings are preserved byte for byte because they are **control flow, not diagnostics**:
/// `is_result_too_large` and `suggested_split_point` parse the provider's text directly.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    Ok { value: serde_json::Value },
    Err { message: String },
}

/// One line of `entries.jsonl`: a key and the queue of outcomes recorded for it.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Entry {
    key: String,
    outcomes: Vec<Outcome>,
}

/// What produced a tape, so a published number can name the bytes it came from.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Manifest {
    pub chain: Option<String>,
    /// Host only - `provider_of()` strips credentials, so no API key ever reaches a tape.
    pub provider: Option<String>,
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub recorded_at: Option<String>,
    pub commit: Option<String>,
    /// sha256 of the canonical `entries.jsonl` bytes.
    pub content_address: Option<String>,
}

/// Canonical keys. **Content-keyed, not order-keyed**: the pipelined arm fires several window fetches
/// at once, and a record session and a replay session may resolve them in a different interleaving
/// without either being wrong. Content-keying is indifferent to interleaving by construction.
///
/// It is *not* indifferent to which ranges get asked for at all - fixed, sequential-adaptive and
/// pipelined-adaptive windows are three different range decompositions, so one tape does not serve
/// every arm. That is a property of the workload, not a defect of the key.
mod key {
    use super::LogFilter;

    pub fn tip() -> String {
        "tip".into()
    }
    pub fn finalized() -> String {
        "finalized".into()
    }
    pub fn block_hash(n: u64) -> String {
        format!("block_hash:{n}")
    }
    fn blocks(name: &str, blocks: &[u64]) -> String {
        let mut v = blocks.to_vec();
        v.sort_unstable();
        v.dedup();
        let joined: Vec<String> = v.iter().map(|b| b.to_string()).collect();
        format!("{name}:{}", joined.join(","))
    }
    pub fn block_timestamps(b: &[u64]) -> String {
        blocks("block_timestamps", b)
    }
    pub fn block_headers(b: &[u64]) -> String {
        blocks("block_headers", b)
    }
    pub fn block_bodies(b: &[u64]) -> String {
        blocks("block_bodies", b)
    }
    pub fn logs(filter: &LogFilter, from: u64, to: u64) -> String {
        let mut a: Vec<String> = filter
            .addresses()
            .iter()
            .map(|s| s.to_lowercase())
            .collect();
        a.sort();
        let mut t: Vec<String> = filter.topic0s().iter().map(|s| s.to_lowercase()).collect();
        t.sort();
        format!("logs:{}|{}|{from}|{to}", a.join(","), t.join(","))
    }
}

/// A tape loaded into memory, or being built.
#[derive(Default)]
pub struct Tape {
    pub manifest: Manifest,
    entries: BTreeMap<String, VecDeque<Outcome>>,
}

impl Tape {
    /// Canonical bytes of `entries.jsonl`: one line per unique key, sorted by key. Sorting is what
    /// makes two honest recordings of the same past-finality range produce identical bytes, which is
    /// what makes the tape content-addressable at all - and diffable in review when it legitimately
    /// changes.
    fn entries_bytes(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        for (key, outcomes) in &self.entries {
            let line = serde_json::to_string(&Entry {
                key: key.clone(),
                outcomes: outcomes.iter().cloned().collect(),
            })?;
            out.extend_from_slice(line.as_bytes());
            out.push(b'\n');
        }
        Ok(out)
    }

    /// sha256 over those bytes, hex. The same scheme `lists.rs` already uses for content-addressed
    /// snapshots, reused rather than inventing a third addressing scheme beside `cid.rs`'s CIDv0.
    pub fn content_address(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(self.entries_bytes()?);
        Ok(hex::encode(h.finalize()))
    }

    /// Write `manifest.json` + `entries.jsonl` into a directory.
    pub fn write_dir(&mut self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating tape directory {}", dir.display()))?;
        let bytes = self.entries_bytes()?;
        self.manifest.content_address = Some(self.content_address()?);
        std::fs::write(dir.join("entries.jsonl"), &bytes)?;
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::to_vec_pretty(&self.manifest)?,
        )?;
        Ok(())
    }

    /// Read a tape back. Verifies the content address, because a tape whose bytes have drifted from
    /// the address a published number cites is worse than no tape.
    pub fn read_dir(dir: &Path) -> Result<Self> {
        let entries_path = dir.join("entries.jsonl");
        let raw = std::fs::read(&entries_path)
            .with_context(|| format!("reading tape {}", entries_path.display()))?;
        let mut entries: BTreeMap<String, VecDeque<Outcome>> = BTreeMap::new();
        for (i, line) in raw.split(|b| *b == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            let e: Entry = serde_json::from_slice(line)
                .with_context(|| format!("{}:{}", entries_path.display(), i + 1))?;
            entries.insert(e.key, e.outcomes.into_iter().collect());
        }
        let manifest: Manifest = match std::fs::read(dir.join("manifest.json")) {
            Ok(b) => serde_json::from_slice(&b).unwrap_or_default(),
            Err(_) => Manifest::default(),
        };
        let tape = Tape { manifest, entries };
        if let Some(claimed) = tape.manifest.content_address.as_deref() {
            let actual = tape.content_address()?;
            if actual != claimed {
                bail!(
                    "tape at {} does not match its own content address: manifest says {claimed}, \
                     entries.jsonl hashes to {actual}. A published number citing this tape would be \
                     naming bytes that no longer exist.",
                    dir.display()
                );
            }
        }
        Ok(tape)
    }

    fn describe(&self) -> String {
        let m = &self.manifest;
        let range = match (m.from_block, m.to_block) {
            (Some(a), Some(b)) => format!("blocks {a}..={b}"),
            _ => "an unrecorded range".into(),
        };
        format!(
            "this tape covers {range} from {}, recorded {}, {} distinct keys",
            m.provider.as_deref().unwrap_or("an unrecorded provider"),
            m.recorded_at.as_deref().unwrap_or("at an unrecorded time"),
            self.entries.len()
        )
    }
}

/// Wraps a live [`Source`] and records every call it serves.
pub struct RecordingSource<S: Source> {
    inner: S,
    log: Mutex<BTreeMap<String, Vec<Outcome>>>,
}

impl<S: Source> RecordingSource<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            log: Mutex::new(BTreeMap::new()),
        }
    }

    fn push(&self, key: String, outcome: Outcome) {
        self.log
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .push(outcome);
    }

    /// The wrapped source, so a caller can still reach concrete facts about it - a bench run
    /// recording through an `RpcClient` still wants that client's real HTTP request count.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Consume the recording into a tape.
    pub fn into_tape(self, manifest: Manifest) -> Tape {
        let log = self.log.into_inner().unwrap();
        Tape {
            manifest,
            entries: log.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

/// Record `r` as an [`Outcome`], returning it unchanged.
macro_rules! record {
    ($self:expr, $key:expr, $call:expr) => {{
        let key = $key;
        match $call.await {
            Ok(v) => {
                $self.push(
                    key,
                    Outcome::Ok {
                        value: serde_json::to_value(&v)?,
                    },
                );
                Ok(v)
            }
            Err(e) => {
                $self.push(
                    key,
                    Outcome::Err {
                        message: format!("{e:#}"),
                    },
                );
                Err(e)
            }
        }
    }};
}

#[async_trait::async_trait]
impl<S: Source> Source for RecordingSource<S> {
    async fn tip(&self) -> Result<u64> {
        record!(self, key::tip(), self.inner.tip())
    }
    async fn block_hash(&self, n: u64) -> Result<Option<String>> {
        record!(self, key::block_hash(n), self.inner.block_hash(n))
    }
    async fn finalized(&self) -> Result<Option<u64>> {
        record!(self, key::finalized(), self.inner.finalized())
    }
    async fn block_timestamps(&self, b: &[u64]) -> Result<HashMap<u64, u64>> {
        record!(
            self,
            key::block_timestamps(b),
            self.inner.block_timestamps(b)
        )
    }
    async fn block_headers(&self, b: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        record!(self, key::block_headers(b), self.inner.block_headers(b))
    }
    async fn block_bodies(&self, b: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        record!(self, key::block_bodies(b), self.inner.block_bodies(b))
    }
    fn forget_cached_above(&self, block: u64) {
        // Not recorded: it returns nothing and mutates only the wrapped source's cache. A replay
        // source holds no cache to forget.
        self.inner.forget_cached_above(block)
    }
    fn refuses_topic0_only(&self, err: &anyhow::Error) -> bool {
        self.inner.refuses_topic0_only(err)
    }
    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
        record!(
            self,
            key::logs(filter, from, to),
            self.inner.logs(filter, from, to)
        )
    }
}

/// Answers [`Source`] calls out of a tape. Holds no `RpcClient`, so it cannot reach a network even by
/// mistake.
pub struct ReplaySource {
    tape: Mutex<Tape>,
    reads: AtomicU64,
}

impl ReplaySource {
    pub fn open(dir: &Path) -> Result<Self> {
        Ok(Self {
            tape: Mutex::new(Tape::read_dir(dir)?),
            reads: AtomicU64::new(0),
        })
    }

    /// Stands in for `RpcClient::request_count()` in `BenchReport.rpc_requests`. It keeps the field's
    /// meaning - *how many times did the code under test ask the source for something* - and loses
    /// only "how many bytes went over a wire", which is why a replayed report says so rather than
    /// leaving a reader to infer it from a suspiciously low number.
    pub fn reads(&self) -> u64 {
        self.reads.load(Ordering::SeqCst)
    }

    pub fn content_address(&self) -> Option<String> {
        self.tape.lock().unwrap().manifest.content_address.clone()
    }

    /// Host the recording came from. A replayed `BenchReport.provider` that names the nest's
    /// configured pool instead is the live endpoint wearing a `replayed: true` badge.
    pub fn provider(&self) -> Option<String> {
        self.tape.lock().unwrap().manifest.provider.clone()
    }

    /// **A miss is a loud, specific failure, never a synthesised default.** A miss means the code
    /// under test asked for something the recording does not contain - a changed chunker, a changed
    /// retry policy, a different range - and that is a real signal. A rig that quietly invents data is
    /// the second generator with a file behind it.
    fn take(&self, key: &str) -> Result<Outcome> {
        let mut tape = self.tape.lock().unwrap();
        let described = tape.describe();
        match tape.entries.get_mut(key).and_then(|q| q.pop_front()) {
            Some(o) => {
                self.reads.fetch_add(1, Ordering::SeqCst);
                Ok(o)
            }
            None => bail!("tape miss: {key} not recorded; {described}"),
        }
    }

    fn decode<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Result<T> {
        match self.take(key)? {
            Outcome::Ok { value } => Ok(serde_json::from_value(value)
                .with_context(|| format!("decoding recorded outcome for {key}"))?),
            Outcome::Err { message } => bail!(message),
        }
    }
}

#[async_trait::async_trait]
impl Source for ReplaySource {
    async fn tip(&self) -> Result<u64> {
        self.decode(&key::tip())
    }
    async fn block_hash(&self, n: u64) -> Result<Option<String>> {
        self.decode(&key::block_hash(n))
    }
    async fn finalized(&self) -> Result<Option<u64>> {
        self.decode(&key::finalized())
    }
    async fn block_timestamps(&self, b: &[u64]) -> Result<HashMap<u64, u64>> {
        self.decode(&key::block_timestamps(b))
    }
    async fn block_headers(&self, b: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        self.decode(&key::block_headers(b))
    }
    async fn block_bodies(&self, b: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        self.decode(&key::block_bodies(b))
    }
    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
        self.decode(&key::logs(filter, from, to))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A live-ish source whose answers change every call, so a test that passes on replay can only be
    /// passing because the tape served the *recorded* value rather than a freshly computed one.
    struct Drifting {
        calls: AtomicUsize,
        fail_logs_over: u64,
    }
    impl Drifting {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_logs_over: u64::MAX,
            }
        }
        fn failing_over(width: u64) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail_logs_over: width,
            }
        }
        fn n(&self) -> u64 {
            self.calls.fetch_add(1, Ordering::SeqCst) as u64
        }
    }
    #[async_trait::async_trait]
    impl Source for Drifting {
        async fn tip(&self) -> Result<u64> {
            Ok(1_000 + self.n())
        }
        async fn block_hash(&self, n: u64) -> Result<Option<String>> {
            Ok(Some(format!("0xhash{n}_{}", self.n())))
        }
        async fn block_timestamps(&self, b: &[u64]) -> Result<HashMap<u64, u64>> {
            let d = self.n();
            Ok(b.iter().map(|x| (*x, x + d)).collect())
        }
        async fn logs(&self, _f: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
            if to - from > self.fail_logs_over {
                bail!("HTTP 400: Log response size exceeded. this block range should work: [0x1, 0x2]");
            }
            let d = self.n();
            Ok(vec![Log {
                address: "0xa0b8".into(),
                topics: vec!["0xddf2".into()],
                data: format!("0x{d}"),
                block_number: from,
                block_hash: "0xbh".into(),
                tx_hash: "0xtx".into(),
                log_index: 0,
            }])
        }
    }

    fn filter() -> LogFilter {
        LogFilter::new(&["0xA0B8".to_string()], &["0xDDF2".to_string()]).unwrap()
    }

    /// The whole point of the rig: the same run, twice, byte-identical - against a source that would
    /// have answered differently every time.
    #[tokio::test]
    async fn a_replayed_run_returns_what_was_recorded_not_what_the_source_would_say_now() {
        let live = Drifting::new();
        let rec = RecordingSource::new(live);
        let tip = rec.tip().await.unwrap();
        let hash = rec.block_hash(7).await.unwrap();
        let ts = rec.block_timestamps(&[7, 8]).await.unwrap();
        let logs = rec.logs(&filter(), 100, 200).await.unwrap();

        let dir = tempfile::tempdir().unwrap();
        rec.into_tape(Manifest::default())
            .write_dir(dir.path())
            .unwrap();

        // Twice, to prove replay is not consuming state that makes a second run differ.
        for _ in 0..2 {
            let r = ReplaySource::open(dir.path()).unwrap();
            assert_eq!(
                r.tip().await.unwrap(),
                tip,
                "tip must replay what was recorded"
            );
            assert_eq!(r.block_hash(7).await.unwrap(), hash);
            assert_eq!(r.block_timestamps(&[7, 8]).await.unwrap(), ts);
            let got = r.logs(&filter(), 100, 200).await.unwrap();
            assert_eq!(got.len(), logs.len());
            assert_eq!(
                got[0].data, logs[0].data,
                "the recorded payload, not a fresh one"
            );
        }
    }

    /// #767's own framing: a rig that quietly invents data is the second generator with a file behind
    /// it. A miss must name what was asked for and what the tape covers.
    #[tokio::test]
    async fn a_tape_miss_is_a_loud_named_failure_not_an_empty_result() {
        let rec = RecordingSource::new(Drifting::new());
        rec.logs(&filter(), 100, 200).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut tape = rec.into_tape(Manifest {
            from_block: Some(100),
            to_block: Some(200),
            provider: Some("eth-pokt.nodies.app".into()),
            ..Default::default()
        });
        tape.write_dir(dir.path()).unwrap();

        let r = ReplaySource::open(dir.path()).unwrap();
        let err = r.logs(&filter(), 300, 400).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("tape miss"), "must say it is a miss: {msg}");
        assert!(msg.contains("300"), "must name what was asked for: {msg}");
        assert!(
            msg.contains("blocks 100..=200"),
            "must say what it covers: {msg}"
        );
        assert!(
            msg.contains("eth-pokt.nodies.app"),
            "must name the provider: {msg}"
        );
    }

    /// The detail that makes replay trustworthy rather than plausible. The adaptive window controller
    /// shrinks in response to a provider-cap *error*; a recorder that kept only successes would drop
    /// the event that drove a real shrink, and the replayed window trajectory would silently diverge
    /// from the workload that was actually measured.
    #[tokio::test]
    async fn a_recorded_error_replays_as_that_error_byte_for_byte() {
        let rec = RecordingSource::new(Drifting::failing_over(50));
        let live_err = format!("{:#}", rec.logs(&filter(), 0, 100).await.unwrap_err());
        assert!(live_err.contains("Log response size exceeded"));

        let dir = tempfile::tempdir().unwrap();
        rec.into_tape(Manifest::default())
            .write_dir(dir.path())
            .unwrap();

        let r = ReplaySource::open(dir.path()).unwrap();
        let replayed = format!("{:#}", r.logs(&filter(), 0, 100).await.unwrap_err());
        assert_eq!(
            replayed, live_err,
            "the shrink-retry logic parses this text, so it has to survive intact"
        );
        assert!(
            crate::chunker::is_result_too_large(&anyhow::anyhow!(replayed.clone())),
            "a replayed cap must still classify as a cap, or the window trajectory diverges"
        );
    }

    /// Content-keyed, not order-keyed: the pipelined arm resolves concurrent fetches in whatever order
    /// it likes, and a replay that demanded the recording's order would break for no good reason.
    #[tokio::test]
    async fn replay_is_indifferent_to_the_order_calls_come_back_in() {
        let rec = RecordingSource::new(Drifting::new());
        let a = rec.logs(&filter(), 0, 10).await.unwrap();
        let b = rec.logs(&filter(), 11, 20).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        rec.into_tape(Manifest::default())
            .write_dir(dir.path())
            .unwrap();

        let r = ReplaySource::open(dir.path()).unwrap();
        // Deliberately the reverse of the recording order.
        assert_eq!(r.logs(&filter(), 11, 20).await.unwrap()[0].data, b[0].data);
        assert_eq!(r.logs(&filter(), 0, 10).await.unwrap()[0].data, a[0].data);
    }

    /// A source that answers the same every time, like a finalised range really does.
    struct Settled;
    #[async_trait::async_trait]
    impl Source for Settled {
        async fn tip(&self) -> Result<u64> {
            Ok(1_000)
        }
        async fn block_hash(&self, n: u64) -> Result<Option<String>> {
            Ok(Some(format!("0xhash{n}")))
        }
        async fn logs(&self, _f: &LogFilter, from: u64, _to: u64) -> Result<Vec<Log>> {
            Ok(vec![Log {
                address: "0xa0b8".into(),
                topics: vec!["0xddf2".into()],
                data: "0x1".into(),
                block_number: from,
                block_hash: "0xbh".into(),
                tx_hash: "0xtx".into(),
                log_index: 0,
            }])
        }
    }

    /// Two honest recordings of the same finalised range must produce the same bytes, or the tape is
    /// not content-addressable and a published number cannot name what it came from.
    ///
    /// Uses [`Settled`] rather than [`Drifting`] on purpose: the claim is about **interleaving**, not
    /// about content. A source whose answers change per call legitimately produces different bytes in
    /// a different order, and asserting otherwise would be asserting that the recorder launders real
    /// differences away - which is the opposite of what this rig is for. (Written the wrong way round
    /// first; the test caught it.)
    #[tokio::test]
    async fn the_same_calls_in_a_different_order_hash_identically() {
        let one = RecordingSource::new(Settled);
        one.tip().await.unwrap();
        one.block_hash(1).await.unwrap();
        let two = RecordingSource::new(Settled);
        two.block_hash(1).await.unwrap();
        two.tip().await.unwrap();
        assert_eq!(
            one.into_tape(Manifest::default())
                .content_address()
                .unwrap(),
            two.into_tape(Manifest::default())
                .content_address()
                .unwrap(),
            "entries.jsonl is sorted by key, so interleaving must not change the address"
        );
    }

    /// A tape whose bytes have drifted from the address a published number cites is worse than no tape.
    #[tokio::test]
    async fn a_tampered_tape_is_refused_rather_than_replayed() {
        let rec = RecordingSource::new(Drifting::new());
        rec.tip().await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        rec.into_tape(Manifest::default())
            .write_dir(dir.path())
            .unwrap();

        let p = dir.path().join("entries.jsonl");
        let tampered = std::fs::read_to_string(&p).unwrap().replace("1000", "9999");
        std::fs::write(&p, tampered).unwrap();

        let err = match ReplaySource::open(dir.path()) {
            Ok(_) => panic!("a drifted tape must be refused, not opened"),
            Err(e) => format!("{e:#}"),
        };
        assert!(
            err.contains("does not match its own content address"),
            "a drifted tape must be refused: {err}"
        );
    }
}
