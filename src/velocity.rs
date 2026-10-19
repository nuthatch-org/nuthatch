//! Velocity flags (RFC-0008 C3): per-address outbound volume within a block-window, maintained
//! incrementally by DBSP - the same IVM machinery as balances/exposure, applied to "how much did this
//! address move recently?". A **velocity flag** is an (address, window) whose outbound volume reaches
//! a configured threshold.
//!
//! The window is a **tumbling block-bucket**, not a true sliding window: a transfer at block `b` falls
//! in the bucket starting at `(b / W) * W`. This is an honest approximation of "~24h volume" (blocks,
//! not wall-clock - `W ≈ 7200` on 12s-block mainnet) that stays exactly maintainable incrementally: a
//! reorg re-feeds the transfer with weight −1 and its bucket's volume retracts, exactly like a balance
//! (so a flag that a reorg invalidates disappears). A true sliding window would need per-block aging;
//! the tumbling approximation is documented rather than faked.
//!
//! Two linear DBSP aggregates per (address, window) - summed volume and a transfer count - because a
//! tuple aggregate isn't linear, and splitting them keeps both incrementally maintained *and* seedable
//! on restart from a pre-summed cold aggregate (see `rebuild_velocity`).

use anyhow::{anyhow, Context, Result};
use dbsp::utils::Tup2;
use dbsp::{IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit, Runtime};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};
use std::sync::{Arc, RwLock};

/// Field separator inside an encoded velocity key (ASCII Unit Separator).
const SEP: char = '\u{1f}';

/// The window-start block for `block` under window size `w`: the tumbling bucket boundary.
pub fn window_start(block: u64, window: u64) -> u64 {
    (block / window.max(1)) * window.max(1)
}

fn key(address: &str, window_start: u64) -> String {
    format!("{address}{SEP}{window_start}")
}

/// One contribution to the velocity view: for `key`, `volume` moved across `count` transfers, applied
/// with `weight` (+1 insert, −1 retraction). Seeds carry a pre-summed `count`.
#[derive(Debug, Clone)]
pub struct VelItem {
    key: String,
    volume: i128,
    count: i128,
    weight: i64,
}

pub type VelocityBatch = Vec<VelItem>;

/// The velocity contribution of one transfer: the **sender** moves `value` in `block`'s window. (Only
/// outbound volume - the AML-relevant "how much did X push out"; empty on a zero/over-i128 value.)
pub fn velocity_deltas(
    from: &str,
    block: u64,
    value: i128,
    weight: i64,
    window: u64,
) -> VelocityBatch {
    vec![VelItem {
        key: key(from, window_start(block, window)),
        volume: value,
        count: 1,
        weight,
    }]
}

/// Seed a pre-summed velocity aggregate on restart (from a cold Burrmill fold): `count` transfers
/// totalling `volume` for `key`.
pub fn seed_item(key: String, volume: i128, count: i128) -> VelItem {
    VelItem {
        key,
        volume,
        count,
        weight: 1,
    }
}

/// One velocity flag: an address whose windowed outbound volume reached the threshold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VelocityFlag {
    pub address: String,
    pub window_start: u64,
    pub count: i128,
    pub volume: i128,
}

type KeyAmt = Tup2<String, i128>;
type CircuitHandles = (
    (dbsp::ZSetHandle<KeyAmt>, dbsp::ZSetHandle<KeyAmt>),
    (
        OutputHandle<OrdZSet<Tup2<String, i128>>>,
        OutputHandle<OrdZSet<Tup2<String, i128>>>,
    ),
);

struct VelocityCircuit {
    circuit: dbsp::DBSPHandle,
    volume_in: dbsp::ZSetHandle<KeyAmt>,
    count_in: dbsp::ZSetHandle<KeyAmt>,
    volume_out: OutputHandle<OrdZSet<Tup2<String, i128>>>,
    count_out: OutputHandle<OrdZSet<Tup2<String, i128>>>,
}

/// The published view: `(count, volume)` per encoded (address, window) key.
type Buckets = HashMap<String, (i128, i128)>;

impl VelocityCircuit {
    fn new() -> Result<Self> {
        let (circuit, ((volume_in, count_in), (volume_out, count_out))) =
            Runtime::init_circuit(1, build_circuit)
                .map_err(|e| anyhow!("failed to build velocity circuit: {e}"))?;
        Ok(Self {
            circuit,
            volume_in,
            count_in,
            volume_out,
            count_out,
        })
    }

    /// Fold `batch`, returning only the buckets it changed (#2173).
    fn step(&mut self, batch: VelocityBatch) -> Result<Delta> {
        let mut vol: Vec<Tup2<KeyAmt, i64>> = Vec::with_capacity(batch.len());
        let mut cnt: Vec<Tup2<KeyAmt, i64>> = Vec::with_capacity(batch.len());
        for it in batch {
            vol.push(Tup2(Tup2(it.key.clone(), it.volume), it.weight));
            cnt.push(Tup2(Tup2(it.key, it.count), it.weight));
        }
        self.volume_in.append(&mut vol);
        self.count_in.append(&mut cnt);
        // DBSP splits a large input across steps of its own and each writes the output afresh, so the
        // output is drained after every step, as `EntityCircuit::step_to_commit` does.
        let fault = |e| anyhow!("velocity transaction: {e}");
        self.circuit.start_transaction().map_err(fault)?;
        self.circuit.start_commit_transaction().map_err(fault)?;
        let mut delta = Delta::default();
        loop {
            let complete = self.circuit.step().map_err(fault)?;
            delta.counts.extend(changes(&self.count_out.consolidate()));
            delta
                .volumes
                .extend(changes(&self.volume_out.consolidate()));
            if complete {
                return Ok(delta);
            }
        }
    }
}

/// The buckets a batch changed: `Some(value)`, or `None` for a bucket that left the output.
#[derive(Default)]
struct Delta {
    counts: HashMap<String, Option<i128>>,
    volumes: HashMap<String, Option<i128>>,
}

impl Delta {
    fn apply(self, into: &mut Buckets) {
        // A bucket lives while it counts a transfer; its volume rides on it.
        for (k, count) in self.counts {
            match count {
                Some(c) => into.entry(k).or_insert((0, 0)).0 = c,
                None => {
                    into.remove(&k);
                }
            }
        }
        for (k, volume) in self.volumes {
            if let Some(bucket) = into.get_mut(&k) {
                bucket.1 = volume.unwrap_or(0);
            }
        }
    }
}

/// A circuit seeded with only the buckets `can` keeps, or `None` when it keeps them all.
///
/// Dropping a bucket by retracting it leaves DBSP's traces holding both signs: measured at a million
/// buckets, that doubled the state to 1.4 GB and took a one-transfer step from 0.2 ms to 165 ms. A
/// new circuit holds only what stays (#2173).
fn compacted(
    buckets: &Buckets,
    can: impl Fn(&str, i128) -> bool,
) -> Result<Option<VelocityCircuit>> {
    let kept: VelocityBatch = buckets
        .iter()
        .filter(|(k, &(_, volume))| can(k, volume))
        .map(|(k, &(count, volume))| seed_item(k.clone(), volume, count))
        .collect();
    if kept.len() == buckets.len() {
        return Ok(None);
    }
    let mut fresh = VelocityCircuit::new()?;
    fresh.step(kept)?;
    Ok(Some(fresh))
}

/// The window start an encoded key carries, `None` for one that will not parse.
fn bucket_window(key: &str) -> Option<u64> {
    key.split_once(SEP)?.1.parse().ok()
}

/// Whether a bucket can ever be served as a flag once everything through `sealed` is sealed (#2173).
/// One below `threshold` in a wholly sealed window cannot: sealed blocks never reorg, and later
/// transfers land in later windows. `/flags` serves only buckets at or over the threshold.
pub fn can_flag(key: &str, volume: i128, threshold: i128, window: u64, sealed: u64) -> bool {
    let open = window_start(sealed.saturating_add(1), window);
    sealed == 0 || volume >= threshold || bucket_window(key).is_none_or(|ws| ws >= open)
}

/// Collect (address, window) entries whose volume ≥ `threshold`, descending by volume.
fn rows_over(buckets: &Buckets, threshold: i128) -> Vec<VelocityFlag> {
    let mut out: Vec<VelocityFlag> = buckets
        .iter()
        .filter_map(|(k, &(count, volume))| {
            if volume < threshold {
                return None;
            }
            let (addr, ws) = k.split_once(SEP)?;
            Some(VelocityFlag {
                address: addr.to_string(),
                window_start: ws.parse().ok()?,
                count,
                volume,
            })
        })
        .collect();
    out.sort_by(|a, b| {
        b.volume
            .cmp(&a.volume)
            .then_with(|| a.address.cmp(&b.address))
            .then_with(|| a.window_start.cmp(&b.window_start))
    });
    out
}

/// An output delta as `(key, Some(new value))`, or `(key, None)` for a key that left the output.
fn changes(delta: &OrdZSet<Tup2<String, i128>>) -> HashMap<String, Option<i128>> {
    let mut out: HashMap<String, Option<i128>> = HashMap::new();
    delta
        .iter()
        .for_each(|(rec, (), weight): (Tup2<String, i128>, (), i64)| {
            if weight > 0 {
                out.insert(rec.0, Some(rec.1));
            } else if weight < 0 {
                out.entry(rec.0).or_insert(None);
            }
        });
    out
}

fn build_circuit(circuit: &mut RootCircuit) -> Result<CircuitHandles, anyhow::Error> {
    let (volume_stream, volume_h) = circuit.add_input_zset::<KeyAmt>();
    let volume = volume_stream
        .map_index(|d: &KeyAmt| (d.0.clone(), d.1))
        .aggregate_linear(|v: &i128| *v);
    let volume_out = volume
        .map(|(k, v): (&String, &i128)| Tup2(k.clone(), *v))
        .output();

    let (count_stream, count_h) = circuit.add_input_zset::<KeyAmt>();
    let count = count_stream
        .map_index(|d: &KeyAmt| (d.0.clone(), d.1))
        .aggregate_linear(|v: &i128| *v);
    let count_out = count
        .map(|(k, v): (&String, &i128)| Tup2(k.clone(), *v))
        .output();

    Ok(((volume_h, count_h), (volume_out, count_out)))
}

enum Msg {
    Batch(VelocityBatch),
    Flush(SyncSender<()>),
    Settle {
        sealed_through: u64,
        threshold: i128,
        window: u64,
    },
}

/// Cheap-to-clone handle to the live velocity view. Mirrors [`crate::exposure::ExposureView`].
#[derive(Clone)]
pub struct VelocityView {
    tx: Sender<Msg>,
    rows: Arc<RwLock<HashMap<String, (i128, i128)>>>,
    /// Flips false if the circuit thread dies on an error, so the ingest loop can fail loudly rather
    /// than serve frozen (and, for compliance, silently wrong) velocity flags. A disabled view stays healthy.
    healthy: Arc<AtomicBool>,
}

impl VelocityView {
    /// Start the velocity view. When `enabled` is false (no velocity flag configured) the DBSP circuit
    /// and its worker thread are not spawned - `apply` no-ops and `snapshot` stays empty (L10).
    pub fn start(enabled: bool) -> Result<Self> {
        let (tx, rx) = channel::<Msg>();
        let rows = Arc::new(RwLock::new(HashMap::new()));
        let healthy = Arc::new(AtomicBool::new(true));
        if !enabled {
            drop(rx);
            return Ok(Self { tx, rows, healthy });
        }
        let shared = rows.clone();
        let health = healthy.clone();
        std::thread::Builder::new()
            .name("nuthatch-velocity".into())
            .spawn(move || {
                let mut circuit = match VelocityCircuit::new() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("velocity circuit failed to start: {e:#}");
                        health.store(false, Ordering::SeqCst);
                        return;
                    }
                };
                // The first window not wholly sealed at the last sweep, so the buckets are walked
                // once per window the watermark passes rather than once per seal.
                let mut swept = 0u64;
                while let Ok(msg) = rx.recv() {
                    let batch = match msg {
                        Msg::Batch(batch) => batch,
                        Msg::Settle {
                            sealed_through,
                            threshold,
                            window,
                        } => {
                            let open = window_start(sealed_through.saturating_add(1), window);
                            if open <= swept {
                                continue;
                            }
                            swept = open;
                            let can = |k: &str, v: i128| {
                                can_flag(k, v, threshold, window, sealed_through)
                            };
                            match compacted(&shared.read().unwrap(), can) {
                                Ok(None) => continue,
                                Ok(Some(c)) => circuit = c,
                                Err(e) => {
                                    tracing::error!("velocity compaction failed: {e:#}");
                                    health.store(false, Ordering::SeqCst);
                                    break;
                                }
                            }
                            shared.write().unwrap().retain(|k, &mut (_, v)| can(k, v));
                            continue;
                        }
                        Msg::Flush(ack) => {
                            let _ = ack.send(());
                            continue;
                        }
                    };
                    match circuit.step(batch) {
                        Ok(delta) => delta.apply(&mut shared.write().unwrap()),
                        Err(e) => {
                            tracing::error!("velocity step failed: {e:#}");
                            health.store(false, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            })
            .context("failed to spawn velocity thread")?;
        Ok(Self { tx, rows, healthy })
    }

    /// Whether the circuit thread is alive; `false` means it died on an error and the view is frozen.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    pub fn apply(&self, batch: VelocityBatch) {
        if !batch.is_empty() {
            let _ = self.tx.send(Msg::Batch(batch));
        }
    }

    /// The same batch with each weight negated. A failed attempt leaves the view as it found it.
    pub fn retract(&self, batch: VelocityBatch) {
        self.apply(
            batch
                .into_iter()
                .map(|mut item| {
                    item.weight = -item.weight;
                    item
                })
                .collect(),
        );
    }

    pub fn flush(&self) {
        let (ack, wait) = sync_channel(0);
        if self.tx.send(Msg::Flush(ack)).is_ok() {
            let _ = wait.recv();
        }
    }

    /// Drop the buckets that can no longer flag at `threshold` now that `sealed_through` is sealed
    /// (#2173). Only buckets at or over the threshold are ever served, so no answer changes.
    pub fn settle(&self, sealed_through: u64, threshold: i128, window: u64) {
        let _ = self.tx.send(Msg::Settle {
            sealed_through,
            threshold,
            window,
        });
    }

    /// The current velocity flags at or above `threshold`, descending by volume.
    pub fn flags(&self, threshold: i128) -> Vec<VelocityFlag> {
        match self.rows.read() {
            Ok(buckets) => rows_over(&buckets, threshold),
            Err(_) => Vec::new(),
        }
    }

    /// Number of (address, window) buckets tracked - a small gauge for `/` and `/metrics`.
    pub fn entries(&self) -> usize {
        self.rows.read().map(|m| m.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden test (C3 gate): windowed volume is maintained incrementally, flags fire at the
    /// threshold, and a reorg (retraction) drops a flag that its transfer no longer supports.
    #[test]
    fn velocity_flags_maintained_with_retraction() {
        let w = 100u64;
        let mut c = Fed::new();
        // alice sends 40 then 70 in the same window (blocks 10, 20 → window 0): volume 110.
        c.step(velocity_deltas("alice", 10, 40, 1, w)).unwrap();
        c.step(velocity_deltas("alice", 20, 70, 1, w)).unwrap();
        // A transfer in the next window (block 150 → window 100): separate bucket.
        c.step(velocity_deltas("alice", 150, 500, 1, w)).unwrap();

        // Threshold 100: window 0 (vol 110) and window 100 (vol 500) both flag.
        let flags = c.flags(100);
        assert_eq!(flags.len(), 2);
        assert_eq!(
            flags[0],
            VelocityFlag {
                address: "alice".into(),
                window_start: 100,
                count: 1,
                volume: 500
            }
        );
        assert_eq!(
            flags[1],
            VelocityFlag {
                address: "alice".into(),
                window_start: 0,
                count: 2,
                volume: 110
            }
        );

        // Threshold 200: only the second window survives.
        assert_eq!(c.flags(200).len(), 1);

        // Reorg: retract the 70 in window 0 → volume 40 < 100, flag clears.
        c.step(velocity_deltas("alice", 20, 70, -1, w)).unwrap();
        let flags = c.flags(100);
        assert_eq!(flags.len(), 1, "window-0 flag cleared after retraction");
        assert_eq!(flags[0].window_start, 100);
    }

    /// The C3 gate item: velocity arithmetic runs on the shipped i128 path - a windowed volume that
    /// overflows i64 is tracked, not truncated (a threshold view on i64 would be a compliance bug).
    #[test]
    fn velocity_volume_exceeds_i64() {
        let mut c = Fed::new();
        let big: i128 = 10_000_000_000_000_000_000; // > i64::MAX
        c.step(velocity_deltas("whale", 5, big, 1, 100)).unwrap();
        c.step(velocity_deltas("whale", 6, big, 1, 100)).unwrap();
        let flags = c.flags(big); // 2×big ≥ big
        assert_eq!(flags.len(), 1);
        assert_eq!(flags[0].volume, 2 * big);
    }

    /// Seeding a pre-summed cold aggregate reproduces the same flags as replaying transfers.
    #[test]
    fn seeding_matches_replay() {
        let w = 100u64;
        let mut replay = Fed::new();
        for v in [10i128, 20, 70] {
            replay.step(velocity_deltas("a", 12, v, 1, w)).unwrap();
        }
        let mut seed = Fed::new();
        seed.step(vec![seed_item(key("a", window_start(12, w)), 100, 3)])
            .unwrap();
        assert_eq!(replay.flags(1), seed.flags(1));
    }

    #[test]
    fn view_thread_serves_after_flush() {
        let view = VelocityView::start(true).unwrap();
        view.apply(velocity_deltas("a", 10, 4242, 1, 100));
        view.flush();
        assert_eq!(view.flags(1000)[0].volume, 4242);
        assert_eq!(view.entries(), 1);
    }

    /// #2173: a bucket below the threshold in a window the seal has passed can never be served, so
    /// it is dropped from the circuit and the view; the flags served before and after are the same.
    #[test]
    fn a_sealed_window_keeps_only_the_buckets_that_flag() {
        let view = VelocityView::start(true).unwrap();
        for (who, block, value) in [
            ("small", 10, 5),
            ("big", 20, 500),
            ("small", 150, 5),
            ("big", 250, 5),
        ] {
            view.apply(velocity_deltas(who, block, value, 1, 100));
        }
        view.flush();
        let before = view.flags(100);
        assert_eq!(view.entries(), 4);

        view.settle(199, 100, 100);
        view.flush();
        assert_eq!(view.flags(100), before);
        assert_eq!(
            view.entries(),
            2,
            "window 0's small bucket and window 100's go; window 0's flag and window 200 stay"
        );

        // A retraction in the open window still lands, so the dropped buckets left no residue.
        view.retract(velocity_deltas("big", 250, 5, 1, 100));
        view.apply(velocity_deltas("small", 260, 300, 1, 100));
        view.flush();
        assert_eq!(view.entries(), 2);
        assert_eq!(view.flags(100).len(), 2);
    }

    /// DBSP splits a large input across steps of its own, each writing the output afresh; reading it
    /// once per transaction kept only the last 10,000-row step, so a restart seed lost buckets.
    #[test]
    fn one_large_batch_keeps_every_bucket() {
        let view = VelocityView::start(true).unwrap();
        let batch: VelocityBatch = (0..25_000u64)
            .flat_map(|i| velocity_deltas(&format!("0x{i:040x}"), 10, 1, 1, 100))
            .collect();
        view.apply(batch);
        view.flush();
        assert_eq!(view.entries(), 25_000);
    }

    /// The circuit and a view's published map, as the view thread holds them.
    struct Fed {
        c: VelocityCircuit,
        rows: Buckets,
    }

    impl Fed {
        fn new() -> Self {
            Self {
                c: VelocityCircuit::new().unwrap(),
                rows: Buckets::new(),
            }
        }

        fn step(&mut self, batch: VelocityBatch) -> Result<()> {
            self.c.step(batch)?.apply(&mut self.rows);
            Ok(())
        }

        fn flags(&self, threshold: i128) -> Vec<VelocityFlag> {
            rows_over(&self.rows, threshold)
        }
    }
}
