//! Incremental view maintenance (the IVM core) via DBSP. The first declarative view: per-address
//! token balances, derived from Transfer facts. This is the differentiator vs imperative indexers -
//! we never hand-write "on transfer, load balance, add, save". We state balance = Σ(in) − Σ(out) as
//! a circuit, and DBSP maintains it incrementally: a new transfer is a +1 delta, and a reorg is the
//! *same* transfer re-fed with weight −1 (a retraction). Backfill and tip use the identical circuit.
//!
//! Balances accumulate in **i128** base units. That matters: an i64 accumulator silently drops any
//! transfer above ~9.2e18 base units - barely ~9.2 tokens of an 18-decimal token - so large-supply
//! or high-decimal tokens would under-count. i128 (max ~1.7e38) comfortably holds any real token's
//! total supply in base units.
//!
//! **Values of more than 38 digits are skipped entirely** - the whole transfer, both legs - rather
//! than saturated or truncated (audit L4, reviewed and accepted). No real token reaches 1e38 base
//! units, so in practice this only fires on a deliberately absurd value; silently admitting a wrong
//! number would be worse than omitting a fictional one, and crediting one leg while dropping the
//! other would invent value. The line is the nest's own `_dec` line, `DECIMAL(38,0)`, and both the
//! hot replay ([`transfer_value`]) and the cold fold (`TRY_CAST(… AS DECIMAL(38,0))`) draw it, so a
//! restart cannot change the answer - pinned by
//! `analytics::tests::an_oversized_value_is_dropped_identically_by_the_cold_fold_and_the_hot_replay`.
//! Until 3.13 the line was `i128`, which Burrmill's HUGEINT (`DECIMAL(38,0)`) cannot hold.
//!
//! The view is in-memory but not ephemeral: `rebuild` reconstructs it from stored facts on a warm
//! restart (see `indexer::rebuild_balances`), so balances survive a process bounce.

use anyhow::{anyhow, Context, Result};
use dbsp::utils::Tup2;
use dbsp::{IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit, Runtime};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};
use std::sync::{Arc, RwLock};

/// A balance delta fed to the circuit: (address, signed_value). Insert carries weight +1, a reorg
/// retraction carries weight −1.
type Delta = Tup2<String, i128>;
/// A batch of weighted deltas. Weight is the DBSP Z-set weight (i64): +1 insert, −1 retract.
pub type WeightedBatch = Vec<Tup2<Delta, i64>>;

/// A transfer value as the balance, exposure and velocity views take it: at most 38 digits, the
/// nest's `DECIMAL(38,0)` line, else `None` and the transfer is dropped (COR-8, #814). The cold folds
/// draw the same line in SQL (`analytics::exact_or_null`).
pub fn transfer_value(text: &str) -> Option<i128> {
    text.parse::<i128>()
        .ok()
        .filter(|v| v.unsigned_abs() < 10u128.pow(38))
}

/// The two deltas a single transfer contributes: +value to `to`, −value to `from`.
pub fn transfer_deltas(from: &str, to: &str, value: i128, weight: i64) -> WeightedBatch {
    vec![
        Tup2(Tup2(to.to_string(), value), weight),
        Tup2(Tup2(from.to_string(), -value), weight),
    ]
}

/// A single net-balance delta for one address, used to seed the view on restart from a pre-summed
/// cold aggregate (rather than replaying every underlying transfer).
pub fn seed_delta(address: String, net: i128) -> Tup2<Delta, i64> {
    Tup2(Tup2(address, net), 1)
}

/// The input and output handles of the balance circuit.
type CircuitHandles = (
    dbsp::ZSetHandle<Delta>,
    OutputHandle<OrdZSet<Tup2<String, i128>>>,
);

/// Owns the DBSP circuit. `step` applies a weighted batch and folds the resulting changes into
/// `balances`. Kept separate from the threading so it can be driven deterministically in tests.
struct BalanceCircuit {
    circuit: dbsp::DBSPHandle,
    input: dbsp::ZSetHandle<Delta>,
    output: OutputHandle<OrdZSet<Tup2<String, i128>>>,
}

impl BalanceCircuit {
    fn new() -> Result<Self> {
        let (circuit, (input, output)) = Runtime::init_circuit(1, build_circuit)
            .map_err(|e| anyhow!("failed to build IVM circuit: {e}"))?;
        Ok(Self {
            circuit,
            input,
            output,
        })
    }

    /// Feed a batch, advance the circuit one transaction, and apply the emitted changes.
    fn step(
        &mut self,
        mut batch: WeightedBatch,
        balances: &mut HashMap<String, i128>,
    ) -> Result<()> {
        self.input.append(&mut batch);
        let output = &self.output;
        step_to_commit(&mut self.circuit, || fold_balances(output, balances))
            .map_err(|e| anyhow!("IVM transaction: {e}"))
    }
}

/// Run one transaction to completion, calling `drain` after each of its steps. DBSP splits a large
/// input into internal steps of 10,000 rows and each overwrites the output handles, so reading them
/// once after `transaction()` keeps only the last step's changes (#2268).
pub(crate) fn step_to_commit(
    circuit: &mut dbsp::DBSPHandle,
    mut drain: impl FnMut(),
) -> Result<(), dbsp::Error> {
    circuit.start_transaction()?;
    circuit.start_commit_transaction()?;
    loop {
        let complete = circuit.step()?;
        drain();
        if complete {
            return Ok(());
        }
    }
}

fn fold_balances(
    output: &OutputHandle<OrdZSet<Tup2<String, i128>>>,
    balances: &mut HashMap<String, i128>,
) {
    // The aggregate emits a change stream: a key whose balance moves from old→new appears as
    // (key,old,−1) and (key,new,+1); a key falling to zero appears only as (key,old,−1).
    let changes = output.consolidate();
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
    for (k, v) in set.iter() {
        balances.insert(k.clone(), *v);
    }
    for k in cleared {
        if !set.contains_key(&k) {
            balances.remove(&k); // balance returned to zero → drop the key
        }
    }
}

fn build_circuit(circuit: &mut RootCircuit) -> Result<CircuitHandles, anyhow::Error> {
    let (stream, handle) = circuit.add_input_zset::<Delta>();
    // Sum the signed values per address. aggregate_linear is the incremental Σ; the Z-set weight
    // carries insert/retract, so a retraction subtracts automatically.
    let balances = stream
        .map_index(|d: &Delta| (d.0.clone(), d.1))
        .aggregate_linear(|v: &i128| *v);
    let out = balances.map(|(addr, bal): (&String, &i128)| Tup2(addr.clone(), *bal));
    Ok((handle, out.output()))
}

/// A message to the circuit thread: a batch to apply, or a flush barrier that acks once every
/// previously-enqueued batch has been folded in (used to make `rebuild` synchronous).
enum Msg {
    Batch(WeightedBatch),
    Flush(SyncSender<()>),
}

/// Cheap-to-clone handle to the live balance view. Sends batches to the circuit thread; reads the
/// maintained balances from shared state.
#[derive(Clone)]
pub struct BalanceView {
    tx: Sender<Msg>,
    balances: Arc<RwLock<HashMap<String, i128>>>,
    /// Flips to `false` (permanently) if the circuit thread dies on an error - so the ingest loop can
    /// surface a dead derived-view thread and fail loudly, instead of silently serving frozen balances
    /// as if healthy (a dead task must surface, never be served over).
    healthy: Arc<AtomicBool>,
    /// Transfers whose value has more than 38 digits and were therefore **dropped from these
    /// balances** (COR-8, #814). Served as `dropped_over_i128`, the name from when the line was
    /// `i128`, kept because callers read it.
    ///
    /// The drop itself is correct and stays: [`transfer_value`] refuses it on the hot replay and
    /// `TRY_CAST(... AS DECIMAL(38,0))` yields NULL on the cold fold, and **both legs go**, because
    /// dropping only one would invent value - a sender debited with nobody credited.
    ///
    /// What was wrong is that it was **silent**. A balance missing a transfer was served exactly
    /// like a complete one, so a caller could not tell the two apart. Counting it is the whole fix:
    /// the number stays what it is, and the caller learns it is incomplete.
    ///
    /// **Not `degraded_tables`.** That was the first choice, and it is the wrong channel: it lives
    /// on `QueryOutput` and describes a `/sql` query's cold data, while these drops surface at
    /// `/balances` and `/balance/{address}`. `analytics.rs` already faced this once and gave
    /// `tip_unavailable` its own field rather than shoehorning it in, writing down why.
    over_i128: Arc<AtomicU64>,
}

impl BalanceView {
    /// Spawn the circuit on its own thread (DBSP drives worker threads; keep it off the async pool).
    pub fn start() -> Result<Self> {
        let (tx, rx) = channel::<Msg>();
        let balances = Arc::new(RwLock::new(HashMap::new()));
        let shared = balances.clone();
        let healthy = Arc::new(AtomicBool::new(true));
        let health = healthy.clone();
        std::thread::Builder::new()
            .name("nuthatch-ivm".into())
            .spawn(move || {
                let mut circuit = match BalanceCircuit::new() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("IVM circuit failed to start: {e:#}");
                        health.store(false, Ordering::SeqCst);
                        return;
                    }
                };
                while let Ok(msg) = rx.recv() {
                    match msg {
                        Msg::Batch(batch) => {
                            let mut map = shared.write().unwrap();
                            if let Err(e) = circuit.step(batch, &mut map) {
                                tracing::error!("IVM step failed: {e:#}");
                                health.store(false, Ordering::SeqCst);
                                break;
                            }
                        }
                        // Messages are processed in order, so by the time we see the barrier every
                        // prior batch is applied - ack unblocks the waiter.
                        Msg::Flush(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
            })
            .context("failed to spawn IVM thread")?;
        Ok(Self {
            tx,
            balances,
            healthy,
            over_i128: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Whether the IVM circuit thread is still alive and folding. `false` means it died on an error
    /// (circuit start or a `step`); the ingest loop treats that as fatal rather than serving a frozen
    /// view. (A clean shutdown drops the sender and exits the loop without flipping this.)
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    /// Enqueue a weighted batch (built via `transfer_deltas`/`seed_delta`). Non-blocking; drops
    /// silently if the circuit thread has died (already logged there).
    pub fn apply(&self, batch: WeightedBatch) {
        if !batch.is_empty() {
            let _ = self.tx.send(Msg::Batch(batch));
        }
    }

    /// The same batch with each weight negated. A failed attempt leaves the view as it found it.
    pub fn retract(&self, batch: WeightedBatch) {
        self.apply(
            batch
                .into_iter()
                .map(|Tup2(delta, weight)| Tup2(delta, -weight))
                .collect(),
        );
    }

    /// Block until every batch enqueued so far has been folded into the view. Used after a restart
    /// rebuild so the API serves complete balances from the first request.
    pub fn flush(&self) {
        let (ack, wait) = sync_channel(0);
        if self.tx.send(Msg::Flush(ack)).is_ok() {
            let _ = wait.recv();
        }
    }

    pub fn balance(&self, address: &str) -> Option<i128> {
        self.balances.read().ok()?.get(address).copied()
    }

    /// Top `n` addresses by balance, descending.
    pub fn top(&self, n: usize) -> Vec<(String, i128)> {
        let map = match self.balances.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };
        let mut v: Vec<(String, i128)> = map.iter().map(|(k, val)| (k.clone(), *val)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(n);
        v
    }

    pub fn holders(&self) -> usize {
        self.balances.read().map(|m| m.len()).unwrap_or(0)
    }

    /// Record `n` transfers dropped for exceeding 38 digits (COR-8, #814).
    pub fn note_over_i128(&self, n: u64) {
        if n > 0 {
            self.over_i128.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Take back `n` such transfers that a reorg rolled back (#2260).
    pub fn retract_over_i128(&self, n: u64) {
        if n > 0 {
            let _ = self
                .over_i128
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    Some(c.saturating_sub(n))
                });
        }
    }

    /// How many transfers these balances are missing because their value exceeds `i128`.
    ///
    /// Zero is the ordinary answer and means the balances are complete with respect to this cause.
    pub fn dropped_over_i128(&self) -> u64 {
        self.over_i128.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transfer_value_is_taken_up_to_38_digits() {
        let max = "99999999999999999999999999999999999999";
        assert_eq!(transfer_value(max), Some(10i128.pow(38) - 1));
        assert_eq!(
            transfer_value("100000000000000000000000000000000000000"),
            None
        );
        assert_eq!(
            transfer_value("170141183460469231731687303715884105727"),
            None
        );
        assert_eq!(transfer_value("0"), Some(0));
        assert_eq!(transfer_value("x"), None);
    }

    /// #2268: DBSP runs a large batch as several internal steps of 10,000 rows, so a batch the size
    /// of a restart seed must keep every step's changes, not only the last's.
    #[test]
    fn one_batch_larger_than_a_dbsp_step_keeps_every_holder() {
        let mut circuit = BalanceCircuit::new().unwrap();
        let mut balances = HashMap::new();
        let batch: WeightedBatch = (0..25_000)
            .flat_map(|i| transfer_deltas("0xminter", &format!("0x{i:040x}"), 1, 1))
            .collect();
        circuit.step(batch, &mut balances).unwrap();

        assert_eq!(balances.len(), 25_001, "25,000 holders and the minter");
        assert_eq!(balances.get("0xminter"), Some(&-25_000));
        assert!((0..25_000).all(|i| balances.get(&format!("0x{i:040x}")) == Some(&1)));
    }

    #[test]
    fn seeding_matches_replay() {
        // The warm-restart cold seed folds each segment to one (address, net) row and feeds it via
        // `seed_delta`; the live path feeds individual `transfer_deltas`. Both must produce identical
        // view state, or a rebuilt view would disagree with a from-genesis one. Covers a negative net
        // and a net-zero drop (the two ways the aggregate's HAVING <> 0 behaviour bites).
        let mut replayed = BalanceCircuit::new().unwrap();
        let mut rmap = HashMap::new();
        for v in [10i128, 20, 70] {
            replayed
                .step(transfer_deltas("0xa", "0xb", v, 1), &mut rmap)
                .unwrap();
        }
        // 0xc <-> 0xd cancel to zero, so both must be absent from the view.
        replayed
            .step(transfer_deltas("0xc", "0xd", 5, 1), &mut rmap)
            .unwrap();
        replayed
            .step(transfer_deltas("0xd", "0xc", 5, 1), &mut rmap)
            .unwrap();

        // Seed the pre-summed nets, exactly as the cold fold does: 0xb +100, 0xa -100.
        let mut seeded = BalanceCircuit::new().unwrap();
        let mut smap = HashMap::new();
        seeded
            .step(
                vec![
                    seed_delta("0xb".into(), 100),
                    seed_delta("0xa".into(), -100),
                ],
                &mut smap,
            )
            .unwrap();

        assert_eq!(
            rmap, smap,
            "seeding pre-summed nets must equal replaying individual transfers"
        );
        assert!(
            !rmap.contains_key("0xc") && !rmap.contains_key("0xd"),
            "net-zero addresses must be dropped from the view"
        );
    }

    /// Deterministic IVM golden test: insert transfers, check derived balances; then retract one
    /// (a reorg) and check the view converges to the state as if that transfer never happened.
    #[test]
    fn balances_are_maintained_incrementally_with_retraction() {
        let mut circuit = BalanceCircuit::new().unwrap();
        let mut bal = HashMap::new();

        // alice→bob 100, then bob→carol 30.
        circuit
            .step(transfer_deltas("alice", "bob", 100, 1), &mut bal)
            .unwrap();
        circuit
            .step(transfer_deltas("bob", "carol", 30, 1), &mut bal)
            .unwrap();
        assert_eq!(bal.get("alice"), Some(&-100));
        assert_eq!(bal.get("bob"), Some(&70));
        assert_eq!(bal.get("carol"), Some(&30));

        // Reorg: retract bob→carol 30. carol returns to zero (key dropped); bob back to −100+...
        circuit
            .step(transfer_deltas("bob", "carol", 30, -1), &mut bal)
            .unwrap();
        assert_eq!(bal.get("alice"), Some(&-100));
        assert_eq!(bal.get("bob"), Some(&100));
        assert_eq!(bal.get("carol"), None, "zero balance must drop the key");
    }

    /// The P0 regression: a transfer larger than i64::MAX must be tracked, not silently dropped.
    /// ~1e20 base units is ~100 tokens of an 18-decimal token - utterly ordinary, yet overflows i64.
    #[test]
    fn balances_hold_values_beyond_i64() {
        let mut circuit = BalanceCircuit::new().unwrap();
        let mut bal = HashMap::new();
        let big: i128 = 100_000_000_000_000_000_000; // 1e20 > i64::MAX (~9.2e18)
        assert!(big > i64::MAX as i128);

        circuit
            .step(transfer_deltas("0xmint", "0xwhale", big, 1), &mut bal)
            .unwrap();
        assert_eq!(bal.get("0xwhale"), Some(&big), "must not overflow/drop");
        assert_eq!(bal.get("0xmint"), Some(&-big));

        // Two more of the same accumulate past 2×i64::MAX without wrapping.
        circuit
            .step(transfer_deltas("0xmint", "0xwhale", big, 1), &mut bal)
            .unwrap();
        assert_eq!(bal.get("0xwhale"), Some(&(2 * big)));
    }

    /// A flush barrier returns only once queued work is drained - the property `rebuild` relies on.
    #[test]
    fn flush_waits_for_enqueued_work() {
        let view = BalanceView::start().unwrap();
        view.apply(transfer_deltas("a", "b", 42, 1));
        view.flush();
        assert_eq!(view.balance("b"), Some(42));
        assert_eq!(view.balance("a"), Some(-42));
    }
}
