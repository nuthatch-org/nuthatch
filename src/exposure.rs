//! Direct counterparty-exposure view (RFC-0008 C1), maintained incrementally by DBSP - the same IVM
//! machinery as balances, applied to a compliance question: *how much has each address transacted,
//! directly, with a labeled set?* "Direct" is deliberate - no multi-hop taint tracing (out of scope).
//!
//! For a transfer `from → to` of `value`:
//!   - if `to` is labeled L, then `from` gains **outbound** exposure to L (+value, +1 transfer);
//!   - if `from` is labeled L, then `to` gains **inbound** exposure from L (+value, +1 transfer).
//!
//! A reorg re-feeds the transfer with weight −1 and the exposure retracts, exactly like balances.
//!
//! Membership (is an address labeled?) is resolved by the caller against the loaded [`LabelSet`] when
//! building deltas - the same pattern as `views::transfer_deltas` pre-computing balance deltas in the
//! indexer. This view only aggregates. Two quantities are maintained per (address, label, direction):
//! a **count** of qualifying transfers and the **summed amount** (i128 base units, like balances). We
//! run two linear DBSP aggregates - one over amount, one over a per-transfer `1` - because a tuple
//! aggregate isn't linear; splitting them keeps both incrementally maintained *and* seedable on
//! restart from a pre-summed cold aggregate (see `rebuild`).

use crate::labels::LabelSet;
use anyhow::{anyhow, Context, Result};
use dbsp::utils::Tup2;
use dbsp::{IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit, Runtime};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};
use std::sync::{Arc, RwLock};

/// Field separator inside an encoded exposure key. ASCII Unit Separator (0x1f) - never appears in a
/// hex address, a direction, or a sane label, so decoding is unambiguous.
const SEP: char = '\u{1f}';

/// Encode `(address, label, direction)` into the flat key the circuit aggregates on.
fn key(address: &str, label: &str, dir: Direction) -> String {
    format!("{address}{SEP}{label}{SEP}{}", dir.as_str())
}

/// Direction of exposure relative to the labeled counterparty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// The address *sent* value to a labeled counterparty.
    Out,
    /// The address *received* value from a labeled counterparty.
    In,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Direction::Out => "out",
            Direction::In => "in",
        }
    }
}

/// One contribution to the exposure view: for `key`, a transfer of `amount` carrying `count` transfers
/// (1 for a live transfer; N when seeding a pre-summed cold aggregate), applied with `weight` (+1
/// insert, −1 retraction).
#[derive(Debug, Clone)]
pub struct ExpItem {
    key: String,
    amount: i128,
    count: i128,
    weight: i64,
}

/// A batch of exposure contributions.
pub type ExposureBatch = Vec<ExpItem>;

/// The two exposure contributions a single transfer makes, given who's labeled. Empty when neither
/// counterparty is labeled - the overwhelmingly common case, so this stays cheap.
pub fn exposure_deltas(
    from: &str,
    to: &str,
    value: i128,
    weight: i64,
    labels: &LabelSet,
) -> ExposureBatch {
    let mut batch = Vec::new();
    // `to` is labeled → `from` has outbound exposure to each of `to`'s labels.
    for label in labels.labels_of(to) {
        batch.push(ExpItem {
            key: key(from, label, Direction::Out),
            amount: value,
            count: 1,
            weight,
        });
    }
    // `from` is labeled → `to` has inbound exposure from each of `from`'s labels.
    for label in labels.labels_of(from) {
        batch.push(ExpItem {
            key: key(to, label, Direction::In),
            amount: value,
            count: 1,
            weight,
        });
    }
    batch
}

/// Seed a pre-summed exposure aggregate (used on restart from cold segments): for `key`, `count`
/// transfers totalling `amount`, applied once (+1). Built from [`analytics::cold_exposure`].
pub fn seed_item(key: String, amount: i128, count: i128) -> ExpItem {
    ExpItem {
        key,
        amount,
        count,
        weight: 1,
    }
}

/// One row of the served exposure view for an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExposureRow {
    pub label: String,
    pub direction: &'static str,
    pub count: i128,
    pub amount: i128,
}

type KeyAmt = Tup2<String, i128>;
type CircuitHandles = (
    (dbsp::ZSetHandle<KeyAmt>, dbsp::ZSetHandle<KeyAmt>),
    (
        OutputHandle<OrdZSet<Tup2<String, i128>>>,
        OutputHandle<OrdZSet<Tup2<String, i128>>>,
    ),
);

/// Owns the DBSP circuit: two linear aggregates (summed amount, summed count) keyed on the encoded
/// exposure key. Kept separate from threading so it's deterministically testable.
struct ExposureCircuit {
    circuit: dbsp::DBSPHandle,
    amount_in: dbsp::ZSetHandle<KeyAmt>,
    count_in: dbsp::ZSetHandle<KeyAmt>,
    amount_out: OutputHandle<OrdZSet<Tup2<String, i128>>>,
    count_out: OutputHandle<OrdZSet<Tup2<String, i128>>>,
    /// Summed amount per key (may be 0 while count > 0 - nets can cancel).
    amounts: HashMap<String, i128>,
    /// Summed transfer count per key; authoritative for a key's existence (0 ⇒ drop the key).
    counts: HashMap<String, i128>,
}

impl ExposureCircuit {
    fn new() -> Result<Self> {
        let (circuit, ((amount_in, count_in), (amount_out, count_out))) =
            Runtime::init_circuit(1, build_circuit)
                .map_err(|e| anyhow!("failed to build exposure circuit: {e}"))?;
        Ok(Self {
            circuit,
            amount_in,
            count_in,
            amount_out,
            count_out,
            amounts: HashMap::new(),
            counts: HashMap::new(),
        })
    }

    fn step(&mut self, batch: ExposureBatch) -> Result<()> {
        let mut amt: Vec<Tup2<KeyAmt, i64>> = Vec::with_capacity(batch.len());
        let mut cnt: Vec<Tup2<KeyAmt, i64>> = Vec::with_capacity(batch.len());
        for it in batch {
            amt.push(Tup2(Tup2(it.key.clone(), it.amount), it.weight));
            cnt.push(Tup2(Tup2(it.key, it.count), it.weight));
        }
        self.amount_in.append(&mut amt);
        self.count_in.append(&mut cnt);
        let (amounts, counts) = (&mut self.amounts, &mut self.counts);
        let (amount_out, count_out) = (&self.amount_out, &self.count_out);
        crate::views::step_to_commit(&mut self.circuit, || {
            // Fold amount changes: a group moving old→new appears as (key,old,−1),(key,new,+1); a
            // group returning to 0 appears only as (key,old,−1). Amount may legitimately be 0, so a
            // cleared amount sets 0 rather than dropping - the count map decides existence.
            fold_changes(&amount_out.consolidate(), amounts, false);
            // Fold count changes: same shape, but a cleared count *does* drop the key (no transfers
            // left), and we drop it from amounts too so the two maps stay consistent.
            for k in fold_changes(&count_out.consolidate(), counts, true) {
                amounts.remove(&k);
            }
        })
        .map_err(|e| anyhow!("exposure transaction: {e}"))
    }

    #[cfg(test)]
    fn rows_for(&self, address: &str) -> Vec<ExposureRow> {
        let prefix = format!("{address}{SEP}");
        let mut rows: Vec<ExposureRow> = self
            .counts
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .filter_map(|(k, &count)| {
                let mut parts = k.splitn(3, SEP);
                let _addr = parts.next()?;
                let label = parts.next()?.to_string();
                let direction = match parts.next()? {
                    "out" => "out",
                    "in" => "in",
                    _ => return None,
                };
                Some(ExposureRow {
                    label,
                    direction,
                    count,
                    amount: self.amounts.get(k).copied().unwrap_or(0),
                })
            })
            .collect();
        rows.sort_by(|a, b| {
            b.amount
                .cmp(&a.amount)
                .then_with(|| a.label.cmp(&b.label))
                .then_with(|| a.direction.cmp(b.direction))
        });
        rows
    }
}

/// Fold a DBSP change stream into `map`. Returns the keys that were cleared (fell to zero) when
/// `drop_on_clear` is set - for the count aggregate, those keys are removed; for amount, a cleared
/// value is written as 0 (the key may still exist via a nonzero count).
fn fold_changes(
    changes: &OrdZSet<Tup2<String, i128>>,
    map: &mut HashMap<String, i128>,
    drop_on_clear: bool,
) -> Vec<String> {
    let mut set: HashMap<String, i128> = HashMap::new();
    let mut cleared: Vec<String> = Vec::new();
    changes
        .iter()
        .for_each(|(rec, (), weight): (Tup2<String, i128>, (), i64)| {
            if weight > 0 {
                set.insert(rec.0.clone(), rec.1);
            } else if weight < 0 {
                cleared.push(rec.0);
            }
        });
    for (k, v) in &set {
        map.insert(k.clone(), *v);
    }
    let mut dropped = Vec::new();
    for k in cleared {
        if !set.contains_key(&k) {
            if drop_on_clear {
                map.remove(&k);
                dropped.push(k);
            } else {
                map.insert(k, 0);
            }
        }
    }
    dropped
}

fn build_circuit(circuit: &mut RootCircuit) -> Result<CircuitHandles, anyhow::Error> {
    let (amount_stream, amount_h) = circuit.add_input_zset::<KeyAmt>();
    let amount = amount_stream
        .map_index(|d: &KeyAmt| (d.0.clone(), d.1))
        .aggregate_linear(|v: &i128| *v);
    let amount_out = amount
        .map(|(k, v): (&String, &i128)| Tup2(k.clone(), *v))
        .output();

    let (count_stream, count_h) = circuit.add_input_zset::<KeyAmt>();
    let count = count_stream
        .map_index(|d: &KeyAmt| (d.0.clone(), d.1))
        .aggregate_linear(|v: &i128| *v);
    let count_out = count
        .map(|(k, v): (&String, &i128)| Tup2(k.clone(), *v))
        .output();

    Ok(((amount_h, count_h), (amount_out, count_out)))
}

enum Msg {
    Batch(ExposureBatch),
    Flush(SyncSender<()>),
}

/// The published view, keyed by address so `/exposure/{address}` is one lookup, not a walk of every
/// labeled counterparty under the lock the circuit thread publishes through (#2214).
#[derive(Default)]
struct Published {
    by_address: HashMap<String, Vec<ExposureRow>>,
    entries: usize,
}

impl Published {
    fn insert(&mut self, key: &str, count: i128, amount: i128) {
        let mut parts = key.splitn(3, SEP);
        let (Some(address), Some(label), Some(direction)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return;
        };
        let direction = match direction {
            "out" => "out",
            "in" => "in",
            _ => return,
        };
        self.by_address
            .entry(address.to_string())
            .or_default()
            .push(ExposureRow {
                label: label.to_string(),
                direction,
                count,
                amount,
            });
        self.entries += 1;
    }
}

/// Cheap-to-clone handle to the live exposure view. Mirrors [`crate::views::BalanceView`].
#[derive(Clone)]
pub struct ExposureView {
    tx: Sender<Msg>,
    /// Snapshot of the maintained view. Read by the API.
    rows: Arc<RwLock<Published>>,
    /// Flips false if the circuit thread dies on an error, so the ingest loop can fail loudly rather
    /// than serve frozen (and, for compliance, silently wrong) exposure. A disabled view stays healthy.
    healthy: Arc<AtomicBool>,
}

impl ExposureView {
    /// Start the exposure view. When `enabled` is false (a nest with no labels) the DBSP circuit and
    /// its worker thread are *not* spawned - `apply` becomes a silent no-op (its send finds no
    /// receiver) and `snapshot` stays empty, so an unused view costs nothing (L10).
    pub fn start(enabled: bool) -> Result<Self> {
        let (tx, rx) = channel::<Msg>();
        let rows = Arc::new(RwLock::new(Published::default()));
        let healthy = Arc::new(AtomicBool::new(true));
        if !enabled {
            drop(rx);
            return Ok(Self { tx, rows, healthy });
        }
        let shared = rows.clone();
        let health = healthy.clone();
        std::thread::Builder::new()
            .name("nuthatch-exposure".into())
            .spawn(move || {
                let mut circuit = match ExposureCircuit::new() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("exposure circuit failed to start: {e:#}");
                        health.store(false, Ordering::SeqCst);
                        return;
                    }
                };
                while let Ok(msg) = rx.recv() {
                    match msg {
                        Msg::Batch(batch) => {
                            if let Err(e) = circuit.step(batch) {
                                tracing::error!("exposure step failed: {e:#}");
                                health.store(false, Ordering::SeqCst);
                                break;
                            }
                            // Publish the whole (count, amount) map after each step. The view is small
                            // (labeled-set counterparties only), so a full swap is cheap and keeps the
                            // read path lock-free of the circuit internals.
                            let mut next = Published::default();
                            for (k, &c) in &circuit.counts {
                                next.insert(k, c, circuit.amounts.get(k).copied().unwrap_or(0));
                            }
                            let old = std::mem::replace(&mut *shared.write().unwrap(), next);
                            drop(old);
                        }
                        Msg::Flush(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
            })
            .context("failed to spawn exposure thread")?;
        Ok(Self { tx, rows, healthy })
    }

    /// Whether the circuit thread is alive; `false` means it died on an error and the view is frozen.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    pub fn apply(&self, batch: ExposureBatch) {
        if !batch.is_empty() {
            let _ = self.tx.send(Msg::Batch(batch));
        }
    }

    /// The same batch with each weight negated. A failed attempt leaves the view as it found it.
    pub fn retract(&self, batch: ExposureBatch) {
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

    /// Block until every batch enqueued so far has been folded in (used after a restart rebuild).
    pub fn flush(&self) {
        let (ack, wait) = sync_channel(0);
        if self.tx.send(Msg::Flush(ack)).is_ok() {
            let _ = wait.recv();
        }
    }

    /// The exposure rows for `address`, descending by amount.
    pub fn exposure(&self, address: &str) -> Vec<ExposureRow> {
        let mut rows = match self.rows.read() {
            Ok(m) => m.by_address.get(address).cloned().unwrap_or_default(),
            Err(_) => return Vec::new(),
        };
        #[cfg(test)]
        EXPOSURE_ROWS_READ.with(|n| n.set(n.get() + rows.len()));
        rows.sort_by(|a, b| {
            b.amount
                .cmp(&a.amount)
                .then_with(|| a.label.cmp(&b.label))
                .then_with(|| a.direction.cmp(b.direction))
        });
        rows
    }

    /// Number of distinct (address, label, direction) exposure entries maintained - a small gauge for
    /// `/` and `/metrics`.
    pub fn entries(&self) -> usize {
        self.rows.read().map(|m| m.entries).unwrap_or(0)
    }
}

#[cfg(test)]
thread_local! {
    static EXPOSURE_ROWS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::{LabelEntry, LabelSet};

    fn labelset(pairs: &[(&str, &str)]) -> LabelSet {
        // Build a LabelSet via its public load path would need files; instead round-trip through the
        // import canonicalisation isn't needed here - construct directly via a tiny JSON snapshot.
        let entries: Vec<LabelEntry> = pairs
            .iter()
            .map(|(a, l)| LabelEntry {
                address: a.to_string(),
                label: l.to_string(),
            })
            .collect();
        let json = serde_json::to_string(&entries).unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(crate::labels::LABELS_DIR)).unwrap();
        std::fs::write(
            dir.path().join(crate::labels::LABELS_DIR).join("snap.json"),
            json,
        )
        .unwrap();
        crate::labels::load(dir.path())
    }

    /// Golden test: exposure is maintained incrementally, and a reorg (retraction) converges to the
    /// state as if the retracted transfer never happened - the C1 gate.
    #[test]
    fn exposure_is_maintained_with_retraction() {
        // `mixer` is labeled; `alice` and `bob` are not.
        let labels = labelset(&[("0xmixer", "mixer")]);
        let mut circuit = ExposureCircuit::new().unwrap();

        // alice → mixer 100 : alice gains OUTBOUND exposure to `mixer`.
        circuit
            .step(exposure_deltas("0xalice", "0xmixer", 100, 1, &labels))
            .unwrap();
        // mixer → bob 30 : bob gains INBOUND exposure from `mixer`.
        circuit
            .step(exposure_deltas("0xmixer", "0xbob", 30, 1, &labels))
            .unwrap();
        // alice → bob 5 : neither is labeled → no exposure recorded.
        circuit
            .step(exposure_deltas("0xalice", "0xbob", 5, 1, &labels))
            .unwrap();

        let alice = circuit.rows_for("0xalice");
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0].label, "mixer");
        assert_eq!(alice[0].direction, "out");
        assert_eq!(alice[0].count, 1);
        assert_eq!(alice[0].amount, 100);

        let bob = circuit.rows_for("0xbob");
        assert_eq!(
            bob,
            vec![ExposureRow {
                label: "mixer".into(),
                direction: "in",
                count: 1,
                amount: 30
            }]
        );

        // A second alice → mixer 50 accumulates: count 2, amount 150.
        circuit
            .step(exposure_deltas("0xalice", "0xmixer", 50, 1, &labels))
            .unwrap();
        assert_eq!(circuit.rows_for("0xalice")[0].count, 2);
        assert_eq!(circuit.rows_for("0xalice")[0].amount, 150);

        // Reorg: retract the 50. Back to count 1, amount 100.
        circuit
            .step(exposure_deltas("0xalice", "0xmixer", 50, -1, &labels))
            .unwrap();
        let alice = circuit.rows_for("0xalice");
        assert_eq!(alice[0].count, 1);
        assert_eq!(alice[0].amount, 100);

        // Retract the original 100 too → alice has no exposure left (key dropped entirely).
        circuit
            .step(exposure_deltas("0xalice", "0xmixer", 100, -1, &labels))
            .unwrap();
        assert!(
            circuit.rows_for("0xalice").is_empty(),
            "cleared exposure drops the key"
        );
    }

    /// Amounts exceeding i64 are tracked (the same i128 discipline as balances - a threshold view
    /// built on i64 would be a compliance liability).
    #[test]
    fn exposure_holds_values_beyond_i64() {
        let labels = labelset(&[("0xsanctioned", "ofac")]);
        let mut circuit = ExposureCircuit::new().unwrap();
        let big: i128 = 100_000_000_000_000_000_000; // 1e20 > i64::MAX
        circuit
            .step(exposure_deltas("0xwhale", "0xsanctioned", big, 1, &labels))
            .unwrap();
        assert_eq!(circuit.rows_for("0xwhale")[0].amount, big);
    }

    /// #2268: a batch larger than DBSP's internal step of 10,000 rows keeps every step's changes.
    #[test]
    fn one_batch_larger_than_a_dbsp_step_keeps_every_key() {
        let labels = labelset(&[("0xmixer", "mixer")]);
        let mut circuit = ExposureCircuit::new().unwrap();
        let batch: ExposureBatch = (0..25_000)
            .flat_map(|i| exposure_deltas("0xmixer", &format!("0x{i:040x}"), 7, 1, &labels))
            .collect();
        circuit.step(batch).unwrap();

        assert_eq!(circuit.counts.len(), 25_000);
        assert!((0..25_000).all(|i| {
            let k = key(&format!("0x{i:040x}"), "mixer", Direction::In);
            circuit.counts.get(&k) == Some(&1) && circuit.amounts.get(&k) == Some(&7)
        }));
    }

    /// Seeding a pre-summed cold aggregate reproduces the same state as replaying transfers - the
    /// property `rebuild` relies on to avoid replaying every sealed transfer on restart.
    #[test]
    fn seeding_matches_replay() {
        let labels = labelset(&[("0xmixer", "mixer")]);

        // Replay three transfers alice→mixer.
        let mut replayed = ExposureCircuit::new().unwrap();
        for v in [10i128, 20, 70] {
            replayed
                .step(exposure_deltas("0xalice", "0xmixer", v, 1, &labels))
                .unwrap();
        }

        // Seed the pre-summed aggregate: 3 transfers totalling 100.
        let mut seeded = ExposureCircuit::new().unwrap();
        seeded
            .step(vec![seed_item(
                key("0xalice", "mixer", Direction::Out),
                100,
                3,
            )])
            .unwrap();

        assert_eq!(replayed.rows_for("0xalice"), seeded.rows_for("0xalice"));
    }

    #[test]
    fn view_thread_serves_after_flush() {
        let labels = labelset(&[("0xmixer", "mixer")]);
        let view = ExposureView::start(true).unwrap();
        view.apply(exposure_deltas("0xalice", "0xmixer", 42, 1, &labels));
        view.flush();
        let rows = view.exposure("0xalice");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].amount, 42);
        assert_eq!(view.entries(), 1);
    }

    /// #2214: one address's exposure reads that address's rows, not every labeled counterparty's.
    #[test]
    fn an_address_read_touches_only_that_address() {
        let labels = labelset(&[("0xmixer", "mixer")]);
        let view = ExposureView::start(true).unwrap();
        let mut batch: ExposureBatch = (0..5_000)
            .flat_map(|i| exposure_deltas("0xmixer", &format!("0x{i:040x}"), 7, 1, &labels))
            .collect();
        batch.extend(exposure_deltas("0xalice", "0xmixer", 42, 1, &labels));
        batch.extend(exposure_deltas("0xmixer", "0xalice", 9, 1, &labels));
        view.apply(batch);
        view.flush();
        assert_eq!(view.entries(), 5_002);

        EXPOSURE_ROWS_READ.with(|n| n.set(0));
        let rows = view.exposure("0xalice");
        assert_eq!(
            rows.iter()
                .map(|r| (r.direction, r.amount))
                .collect::<Vec<_>>(),
            vec![("out", 42), ("in", 9)]
        );
        assert_eq!(EXPOSURE_ROWS_READ.with(|n| n.get()), 2);
    }
}
