//! An authored entity's circuit, its published relation and its watermark (RFC-0041 §5.1), run on a
//! [`ViewDriver`] lane. Binding decoded rows to the plan, and the offchain admission, are the node's.

use crate::drive::{Driver, Lane, TimedStep, ViewDriver};
use crate::entity_circuit::EntityCircuit;
use crate::entity_plan::{Plan, Relation};
use crate::entity_row::{Row, Scalar};
use crate::time::{self, Duration, Instant};
use anyhow::{anyhow, Result};
use dbsp::ZWeight;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Mutex, RwLock};

/// One window's facts, already bound to the plan's input relations and weighted.
///
/// Binding happens on the caller's thread rather than the circuit's, so a window that cannot be
/// converted - a `uint256` too wide to be exact, say - is an error the ingest path sees at the
/// point it fed the window, not a circuit that dies later for reasons it can no longer explain.
pub struct Batch {
    pub left: Vec<(Row, ZWeight)>,
    pub right: Vec<(Row, ZWeight)>,
}

impl Batch {
    fn weight(&self) -> i64 {
        self.left
            .iter()
            .chain(self.right.iter())
            .map(|(_, w)| *w)
            .sum()
    }
}

enum Msg {
    /// A weighted batch, the block it carries the entity through once folded, and whether to
    /// publish the relation after folding it.
    ///
    /// A seed sets `publish: false`. Publishing copies the whole relation, so doing it per chunk
    /// makes a chunked seed quadratic in the number of chunks - and a half-seeded relation is not
    /// something any reader should see anyway. `Flush` publishes what a deferred batch left
    /// pending, which is what a seed's end waits on.
    Batch(Box<Batch>, u64, bool),
    /// Rows of appended offchain snapshots (#1437), and the version `table` reaches once they fold.
    /// The block watermark does not move: a snapshot carries no block.
    Offchain(Box<Batch>, String, OffchainMark),
    /// A rebuild, folded into a pending circuit and relation beside the live ones so readers keep
    /// the old, correctly labelled state until `Rebuild::Commit` swaps the new one in under one lock.
    Rebuild(Rebuild),
    /// A bound refused on the caller's side: the fault is already recorded, and the circuit stops
    /// so its state is released.
    Stop,
    Flush(SyncSender<()>),
    /// Sample the state size now and answer with it and what it cost (#1834).
    SampleState(SyncSender<Result<(u64, Duration)>>),
}

fn sample_state(circuit: &mut EntityCircuit, into: &Mutex<Option<u64>>) -> Result<(u64, Duration)> {
    let started = time::now();
    let bytes = circuit.state_bytes()?;
    *into.lock().unwrap() = Some(bytes);
    Ok((bytes, started.elapsed()))
}

/// Why a sampled state of `bytes` stops the entity, or `None` inside `allowance` (#2167). `max_rows`
/// bounds input rows; a join's output, and the values a MIN or MAX keeps from it, can outgrow them.
fn over_allowance(bytes: u64, allowance: u64) -> Option<String> {
    (bytes > allowance).then(|| {
        format!(
            "the circuit holds {bytes} bytes of state, past the allowance of {allowance} bytes \
             admission charged for this entity. Raise its max_rows, which admission charges for"
        )
    })
}

const REBUILD_ORDER: &str =
    "a rebuild message arrived with no rebuild in progress; the ingest path fed it out of order";

enum Rebuild {
    Begin,
    Batch(Box<Batch>),
    Offchain(Box<Batch>, String, OffchainMark),
    Commit(u64),
}

/// How far an entity has applied one offchain table (#1437): how many snapshots, the latest, and a
/// digest of the whole ordered list, which is what tells a replaced snapshot from an unchanged one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OffchainMark {
    pub snapshots: usize,
    pub latest: String,
    pub digest: String,
}

impl OffchainMark {
    pub fn of(version: &[String]) -> Self {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        for hash in version {
            h.update((hash.len() as u64).to_le_bytes());
            h.update(hash.as_bytes());
        }
        Self {
            snapshots: version.len(),
            latest: version.last().cloned().unwrap_or_default(),
            digest: hex::encode(h.finalize()),
        }
    }
}

/// What an entity's rows account for, read with them under one lock (#932): the block it has folded
/// through and, per offchain table it reads, the snapshots it has folded.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    pub through: u64,
    pub offchain: std::collections::BTreeMap<String, OffchainMark>,
}

/// Per entity, what the rows it contributed to an answer account for.
pub type Watermarks = std::collections::BTreeMap<String, Applied>;

impl Applied {
    /// The offchain half as a response label, or `None` for an entity that reads no offchain table.
    pub fn offchain_json(&self) -> Option<serde_json::Value> {
        (!self.offchain.is_empty()).then(|| {
            self.offchain
                .iter()
                .map(|(table, m)| {
                    (
                        format!("{}{table}", nuthatch_decode::registry::OFFCHAIN_NAMESPACE),
                        serde_json::json!({ "snapshots": m.snapshots, "latest": m.latest }),
                    )
                })
                .collect::<serde_json::Map<_, _>>()
                .into()
        })
    }
}

/// What the entity currently answers with, published as one value.
///
/// The relation and the watermark used to be a lock and an atomic, updated in that order with a
/// comment explaining why the order mattered. It is not a comment's job to hold an invariant: a
/// reader between the two stores sees a head whose rows are not published yet, and no test can catch
/// that reliably. One lock, one publication, and the ordering has nowhere to go wrong.
#[derive(Default)]
struct Published {
    relation: Relation,
    /// The dataset head this entity has **actually folded**, which is not the nest's head while it
    /// is catching up. Advanced only after the batch carrying it has been applied.
    through: u64,
    /// When `through` last actually moved, in unix seconds.
    ///
    /// **Progress, not duration** - the lesson #846 paid for one layer down, where `/ready`
    /// suppressed every stall term during a bulk seal and put nothing in its place, so a pass frozen
    /// for ten hours answered `200 {"ready":true}`. An entity catching up is the same shape:
    /// legitimately behind, indefinitely, and indistinguishable from dead unless something watches
    /// it advance. Zero until the first batch lands, which is "no progress yet" rather than 1970.
    progress_at: u64,
    /// Per offchain table, the snapshots folded into `relation` (#1437).
    offchain: std::collections::BTreeMap<String, OffchainMark>,
}

impl Published {
    fn applied(&self) -> Applied {
        Applied {
            through: self.through,
            offchain: self.offchain.clone(),
        }
    }

    /// Stamp the watermark onto the already-updated relation. **The relation is not copied here.**
    /// `circuit.apply` folds its deltas straight into `relation` under the same lock, so a batch
    /// costs what the batch changed rather than what the relation holds (#897).
    fn stamp(&mut self, through: u64) {
        // Only when the watermark moves: a window that carries the entity no further is not
        // progress, and counting it as such is how a wedged entity looks busy.
        if through > self.through {
            self.progress_at = now_unix();
        }
        self.through = through;
    }
}

fn now_unix() -> u64 {
    time::system_now()
        .duration_since(time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// What the circuit's lane is started with that the node decides: its admission allowance, and how
/// often its state may be sampled (`None` for never).
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_rows: usize,
    pub allowance_bytes: u64,
    pub sample_every: Option<Duration>,
}

/// One entity's circuit on a lane, and the relation and watermark it publishes.
pub struct EntityLane {
    name: String,
    lane: Lane<Msg>,
    state: Arc<RwLock<Published>>,
    /// Why the circuit stopped, if it has - on start, on a step, or on the declared bound. The
    /// reason and not merely the fact, because #866 asks for an entity that is *visibly* dead.
    fault: Arc<RwLock<Option<String>>>,
    /// The circuit's state size at its last sample, `None` until the first (#1834).
    state_bytes: Arc<Mutex<Option<u64>>>,
}

/// The circuit's own state, which only the step touches.
struct Folding {
    circuit: EntityCircuit,
    plan: Plan,
    limits: Limits,
    /// The live input cardinality, as a running total rather than a copy of the input. Weights sum,
    /// so a signed running total is exact for a multiset, and it counts **both** relations (#838).
    live: i64,
    /// The watermark of a folded-but-unpublished batch, i.e. a seed in progress.
    pending: Option<u64>,
    /// A rebuild in progress (#1437): circuit, relation, live count and offchain marks.
    rebuilding: Option<(
        EntityCircuit,
        Relation,
        i64,
        std::collections::BTreeMap<String, OffchainMark>,
    )>,
    /// State size (#1834) is sampled after the circuit changes, at most once per interval, and
    /// never while it sits idle, since an idle circuit's size has not moved.
    changed: bool,
    due: Instant,
    shared: Arc<RwLock<Published>>,
    sampled: Arc<Mutex<Option<u64>>>,
    fault: Arc<RwLock<Option<String>>>,
    label: String,
}

impl Folding {
    fn sample(&mut self) -> Result<Option<String>> {
        let (bytes, _) = sample_state(&mut self.circuit, &self.sampled)?;
        Ok(over_allowance(bytes, self.limits.allowance_bytes))
    }

    fn handle(&mut self, msg: Option<Msg>) -> Result<Option<Instant>> {
        if let Some(every) = self
            .limits
            .sample_every
            .filter(|_| self.changed && time::now() >= self.due)
        {
            match sample_state(&mut self.circuit, &self.sampled) {
                Ok((bytes, took)) => {
                    tracing::debug!("entity `{}` state sampled in {took:?}", self.label);
                    if let Some(why) = over_allowance(bytes, self.limits.allowance_bytes) {
                        anyhow::bail!(why);
                    }
                }
                Err(e) => tracing::warn!("entity `{}` state size not sampled: {e:#}", self.label),
            }
            self.changed = false;
            self.due = time::now() + every;
        }
        if let Some(msg) = msg {
            self.changed |= !matches!(msg, Msg::Flush(_) | Msg::SampleState(_));
            self.fold(msg)?;
        }
        Ok(self
            .limits
            .sample_every
            .filter(|_| self.changed)
            .map(|_| self.due))
    }

    fn fold(&mut self, msg: Msg) -> Result<()> {
        let max_rows = self.limits.max_rows;
        match msg {
            Msg::Batch(batch, through, should_publish) => {
                // The write lock is held across the fold, so no reader ever sees the relation
                // mid-transaction. A fold is microseconds; the clone it replaces was tens of
                // milliseconds (#897).
                let mut published = self.shared.write().unwrap();
                // Includes crossing the declared `max_rows`, which is a fault and not a warning
                // (criterion 10). The watermark does not advance: the entity did not fold it.
                step(
                    &mut self.circuit,
                    &batch,
                    &mut self.live,
                    max_rows,
                    &mut published.relation,
                )?;
                if should_publish {
                    self.pending = None;
                    published.stamp(through);
                } else {
                    self.pending = Some(through);
                }
            }
            Msg::Offchain(batch, table, mark) => {
                let mut published = self.shared.write().unwrap();
                step(
                    &mut self.circuit,
                    &batch,
                    &mut self.live,
                    max_rows,
                    &mut published.relation,
                )?;
                published.offchain.insert(table, mark);
            }
            Msg::Rebuild(Rebuild::Begin) => {
                let c = EntityCircuit::build(self.plan.clone())?;
                self.rebuilding = Some((c, Relation::new(), 0, Default::default()));
            }
            Msg::Rebuild(Rebuild::Batch(batch)) => {
                let (c, relation, count, _) = self
                    .rebuilding
                    .as_mut()
                    .ok_or_else(|| anyhow!(REBUILD_ORDER))?;
                step(c, &batch, count, max_rows, relation)?;
            }
            Msg::Rebuild(Rebuild::Offchain(batch, table, mark)) => {
                let (c, relation, count, marks) = self
                    .rebuilding
                    .as_mut()
                    .ok_or_else(|| anyhow!(REBUILD_ORDER))?;
                step(c, &batch, count, max_rows, relation)?;
                marks.insert(table, mark);
            }
            Msg::Rebuild(Rebuild::Commit(through)) => {
                let (c, relation, count, marks) = self
                    .rebuilding
                    .take()
                    .ok_or_else(|| anyhow!(REBUILD_ORDER))?;
                let mut published = self.shared.write().unwrap();
                self.circuit = c;
                self.live = count;
                published.relation = relation;
                published.offchain = marks;
                self.pending = None;
                published.stamp(through);
            }
            Msg::Stop => anyhow::bail!("stopped: its input was refused"),
            // Messages are processed in order, so by the time the barrier is seen every prior
            // batch is folded.
            Msg::Flush(ack) => {
                // A seed is the whole history arriving at once, so its end is sampled even when no
                // sample is due.
                let why = match self.limits.sample_every.and(self.pending) {
                    Some(_) => self.sample().ok().flatten(),
                    None => None,
                };
                if let Some(why) = why {
                    let e = self.stop(why);
                    let _ = ack.send(());
                    return Err(e);
                }
                if let Some(through) = self.pending.take() {
                    self.shared.write().unwrap().stamp(through);
                }
                let _ = ack.send(());
            }
            Msg::SampleState(ack) => {
                let sample = sample_state(&mut self.circuit, &self.sampled);
                let why = sample
                    .as_ref()
                    .ok()
                    .and_then(|(bytes, _)| over_allowance(*bytes, self.limits.allowance_bytes));
                // The fault is published before the ack, so a caller that sees this sample also
                // sees the entity stopped.
                let stopped = why.map(|why| self.stop(why));
                let _ = ack.send(sample);
                if let Some(e) = stopped {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Record why the entity stopped, unless something already has, and hand back the error that
    /// stops the lane.
    fn stop(&self, why: String) -> anyhow::Error {
        let mut fault = self.fault.write().unwrap();
        if fault.is_none() {
            *fault = Some(why.clone());
        }
        anyhow!(why)
    }
}

impl EntityLane {
    /// Start the circuit on a lane under `driver`. The node's keeps DBSP on its own thread, off the
    /// async pool, as the built-in views do.
    pub fn start(driver: Driver, name: &str, plan: &Plan, limits: Limits) -> Result<Self> {
        let state = Arc::new(RwLock::new(Published::default()));
        let fault = Arc::new(RwLock::new(None::<String>));
        let state_bytes = Arc::new(Mutex::new(None::<u64>));
        let (shared, stopped, sampled) = (state.clone(), fault.clone(), state_bytes.clone());
        let plan = plan.clone();
        let label = name.to_string();
        let lane = driver.lane_timed("entity", &format!("nuthatch-entity-{name}"), move || {
            let circuit = EntityCircuit::build(plan.clone()).map_err(|e| {
                *stopped.write().unwrap() = Some(format!("{e:#}"));
                e.context(format!("entity `{label}`"))
            })?;
            let mut folding = Folding {
                circuit,
                plan,
                limits,
                live: 0,
                pending: None,
                rebuilding: None,
                changed: false,
                due: time::now(),
                shared,
                sampled,
                fault: stopped,
                label: label.clone(),
            };
            Ok(Box::new(move |msg: Option<Msg>| {
                folding.handle(msg).map_err(|e| {
                    let mut fault = folding.fault.write().unwrap();
                    if fault.is_none() {
                        *fault = Some(format!("{e:#}"));
                    }
                    e.context(format!("entity `{label}`"))
                })
            }) as TimedStep<Msg>)
        })?;
        Ok(Self {
            name: name.to_string(),
            lane,
            state,
            fault,
            state_bytes,
        })
    }

    /// Bytes the circuit held at its last sample; `None` before the first or with sampling off.
    pub fn state_bytes(&self) -> Option<u64> {
        *self.state_bytes.lock().unwrap()
    }

    /// Whether the circuit is alive and folding. `false` means it died - on start, on a step, or on
    /// the declared bound. A clean shutdown drops the lane without flipping this.
    pub fn is_healthy(&self) -> bool {
        self.fault.read().map(|f| f.is_none()).unwrap_or(false)
    }

    /// Why the circuit stopped, if it has.
    pub fn fault(&self) -> Option<String> {
        self.fault.read().ok().and_then(|f| f.clone())
    }

    /// Stop the circuit for `why`, a bound the caller refused before anything reached it.
    pub fn stop(&self, why: String) {
        *self.fault.write().unwrap() = Some(why);
        self.lane.send(Msg::Stop);
    }

    /// Enqueue an already-bound batch, publishing it once folded. Dropped silently once the circuit
    /// has died, which recorded why.
    pub fn apply(&self, batch: Batch, through: u64) {
        self.lane.send(Msg::Batch(Box::new(batch), through, true));
    }

    /// Fold without publishing; the next [`Self::flush`] publishes. How a seed is fed.
    pub fn apply_deferred(&self, batch: Batch, through: u64) {
        self.lane.send(Msg::Batch(Box::new(batch), through, false));
    }

    pub fn apply_offchain(&self, batch: Batch, table: String, mark: OffchainMark) {
        self.lane.send(Msg::Offchain(Box::new(batch), table, mark));
    }

    pub fn rebuild_begin(&self) {
        self.lane.send(Msg::Rebuild(Rebuild::Begin));
    }

    pub fn rebuild_batch(&self, batch: Batch) {
        self.lane
            .send(Msg::Rebuild(Rebuild::Batch(Box::new(batch))));
    }

    pub fn rebuild_offchain(&self, batch: Batch, table: String, mark: OffchainMark) {
        self.lane.send(Msg::Rebuild(Rebuild::Offchain(
            Box::new(batch),
            table,
            mark,
        )));
    }

    /// Swap the rebuilt relation in and wait for it.
    pub fn rebuild_commit(&self, through: u64) {
        self.lane.send(Msg::Rebuild(Rebuild::Commit(through)));
        self.flush();
    }

    /// Block until every batch sent so far is folded and published.
    pub fn flush(&self) {
        // One slot, so an inline lane's ack does not wait on a receiver that is this same thread.
        let (ack, wait) = sync_channel(1);
        self.lane.send(Msg::Flush(ack));
        let _ = wait.recv();
    }

    /// Sample the state size now, after every batch sent before it; returns the bytes and what the
    /// sample cost.
    pub fn sample_state(&self) -> Result<(u64, Duration)> {
        let (ack, wait) = sync_channel(1);
        self.lane.send(Msg::SampleState(ack));
        wait.recv()
            .map_err(|_| anyhow!("entity `{}` has stopped", self.name))?
    }

    /// A copy of the whole relation.
    pub fn relation(&self) -> Relation {
        self.state
            .read()
            .map(|s| s.relation.clone())
            .unwrap_or_default()
    }

    pub fn get(&self, key: &Row) -> Option<Row> {
        self.state.read().ok()?.relation.get(key).cloned()
    }

    /// The first key built from one reading per column that the relation holds, and the watermark,
    /// under one lock.
    pub fn get_with_watermark(&self, readings: &[Vec<Scalar>]) -> (Option<Row>, Applied) {
        let Ok(s) = self.state.read() else {
            return (None, Applied::default());
        };
        let width = s.relation.keys().next().map_or(0, |k| k.0.len());
        if width == 0 || width != readings.len() || readings.iter().any(Vec::is_empty) {
            return (None, s.applied());
        }
        let mut at = vec![0usize; width];
        loop {
            let key = Row(at
                .iter()
                .zip(readings)
                .map(|(&i, r)| r[i].clone())
                .collect());
            if let Some(row) = s.relation.get(&key) {
                return (Some(row.clone()), s.applied());
            }
            let mut i = 0;
            loop {
                if i == width {
                    return (None, s.applied());
                }
                at[i] += 1;
                if at[i] < readings[i].len() {
                    break;
                }
                at[i] = 0;
                i += 1;
            }
        }
    }

    /// The rows as JSON objects keyed by `columns`, and the watermark that describes them, under one
    /// lock (#932).
    pub fn rows_as_json_with_watermark(
        &self,
        columns: &[String],
    ) -> (Vec<serde_json::Value>, Applied) {
        let state = match self.state.read() {
            Ok(s) => s,
            Err(_) => return (Vec::new(), Applied::default()),
        };
        let applied = state.applied();
        let rows = state
            .relation
            .iter()
            .map(|(k, v)| {
                let mut obj = serde_json::Map::new();
                for (name, cell) in columns.iter().zip(k.0.iter().chain(v.0.iter())) {
                    obj.insert(name.clone(), sql_cell(cell));
                }
                serde_json::Value::Object(obj)
            })
            .collect();
        (rows, applied)
    }

    pub fn head_rows_with_watermark(&self, limit: usize) -> (usize, Vec<(Row, Row)>, Applied) {
        match self.state.read() {
            Ok(s) => (
                s.relation.len(),
                s.relation
                    .iter()
                    .take(limit)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                s.applied(),
            ),
            Err(_) => (0, Vec::new(), Applied::default()),
        }
    }

    pub fn len(&self) -> usize {
        self.state.read().map(|s| s.relation.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len_and_watermark(&self) -> (usize, Applied) {
        self.state
            .read()
            .map(|s| (s.relation.len(), s.applied()))
            .unwrap_or_default()
    }

    pub fn applied_through(&self) -> u64 {
        self.state.read().map(|s| s.through).unwrap_or(0)
    }

    pub fn fence_watermark(&self) -> Applied {
        self.state.read().map(|s| s.applied()).unwrap_or_default()
    }

    pub fn last_progress(&self) -> u64 {
        self.state.read().map(|s| s.progress_at).unwrap_or(0)
    }
}

/// Fold one batch, bound first.
///
/// The bound is checked **before** the batch reaches the circuit, not after. Checking afterwards
/// means the memory has already been allocated, which is the difference between a bound that
/// prevents an over-budget cursor and one that reports it. The tests cannot tell the two orders
/// apart - they prove the fault, not the allocation - so this ordering is held by this comment.
fn step(
    circuit: &mut EntityCircuit,
    batch: &Batch,
    live: &mut i64,
    max_rows: usize,
    into: &mut Relation,
) -> Result<()> {
    let next = live
        .checked_add(batch.weight())
        .ok_or_else(|| anyhow!("the entity's live input row count overflowed i64"))?;
    if next < 0 {
        return Err(anyhow!(
            "this batch retracts {} more rows than the entity holds. A reorg feeds back rows that \
             were applied; retracting rows that were not is a bookkeeping fault, not a small one",
            -next
        ));
    }
    if next as usize > max_rows {
        return Err(anyhow!(
            "RFC-0041 max_rows exceeded: {next} live input rows across both relations, declared \
             bound {max_rows}"
        ));
    }
    circuit.apply(&batch.left, &batch.right, into)?;
    *live = next;
    Ok(())
}

fn sql_cell(cell: &Scalar) -> serde_json::Value {
    match cell {
        Scalar::Null => serde_json::Value::Null,
        Scalar::Bool(b) => serde_json::Value::Bool(*b),
        Scalar::Int(i) => serde_json::Number::from_i128(*i)
            .map_or_else(|| serde_json::Value::String(i.to_string()), Into::into),
        Scalar::Str(s) => serde_json::Value::String(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_expr::Expr;
    use crate::entity_plan::{Agg, Source};

    fn lane(driver: Driver, max_rows: usize, allowance_bytes: u64) -> EntityLane {
        let plan = Plan {
            left: Source {
                table: "amounts".into(),
                columns: vec!["who".into(), "amount".into()],
            },
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let limits = Limits {
            max_rows,
            allowance_bytes,
            sample_every: Some(Duration::from_secs(3600)),
        };
        EntityLane::start(driver, "t", &plan, limits).unwrap()
    }

    fn rows(who: &str, n: usize, w: ZWeight) -> Batch {
        Batch {
            left: (0..n)
                .map(|i| {
                    (
                        Row(vec![Scalar::Str(who.into()), Scalar::Int(i as i128)]),
                        w,
                    )
                })
                .collect(),
            right: Vec::new(),
        }
    }

    #[test]
    fn both_drivers_publish_the_same_relation_and_seed_on_flush() {
        for driver in [Driver::Threaded, Driver::Inline] {
            let v = lane(driver, 100, u64::MAX);
            v.apply(rows("a", 3, 1), 10);
            v.apply_deferred(rows("b", 2, 1), 12);
            v.flush();
            assert_eq!(v.len(), 2, "{driver:?}");
            assert_eq!(
                v.applied_through(),
                12,
                "{driver:?}: the flush published the seed"
            );
            let (bytes, _) = v.sample_state().unwrap();
            assert_eq!(v.state_bytes(), Some(bytes), "{driver:?}");
            assert!(v.is_healthy());
        }
    }

    #[test]
    fn both_drivers_fault_on_the_bound_and_the_allowance() {
        for driver in [Driver::Threaded, Driver::Inline] {
            let v = lane(driver, 2, u64::MAX);
            v.apply(rows("a", 3, 1), 1);
            v.flush();
            assert!(
                v.fault().unwrap().contains("max_rows exceeded"),
                "{driver:?}"
            );
            assert_eq!(v.applied_through(), 0);

            let v = lane(driver, 100, 1);
            v.apply(rows("a", 1, 1), 1);
            // The first change is sampled at once, before anything sent after it.
            assert!(v.sample_state().is_err(), "{driver:?}");
            assert!(v.fault().unwrap().contains("allowance"), "{driver:?}");
            v.flush();
        }
    }
}
