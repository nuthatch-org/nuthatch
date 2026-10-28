//! RFC-0041 slice 2: the runtime lifecycle for an authored incremental entity (#864).
//!
//! This is deliberately the same shape as [`crate::views::BalanceView`], because §5.1 says the
//! lifecycle is the one already proved by the built-in circuits rather than a second one:
//!
//! > Backfill uses larger batches, but not different semantics.
//!
//! A circuit on its own thread, a channel of weighted batches, a health flag that latches false when
//! the thread dies, and a flush barrier. Three things are added that the built-in views do not need.
//!
//! **The circuit comes from the entity's plan** (#870): an [`EntityCircuit`], built from a [`Plan`],
//! fed through a [`Binding`] that turns a decoded window into that plan's input relations (§5.1).
//!
//! **An applied-through watermark** (criterion 2). A built-in view is fed from the same commit as the
//! facts and has no separate identity, so "how far has this got" is the nest's own cursor. An
//! authored entity is one of several, each of which may be behind, so it carries its own. Serving is
//! gated on it: an entity answers for the head it has actually applied, never for the nest's.
//!
//! **`max_rows` as a fault, not a warning** (criterion 10). Crossing the declared bound stops the
//! circuit and latches unhealthy, which the ingest loop already treats as terminal. §7 wants the
//! bound enforced while running, not merely validated at load, and a bound that logs and continues
//! is the shape this project keeps finding in its own instruments.
//!
//! The circuit, its lane and its published relation are `nuthatch_core::entity_view::EntityLane`
//! (RFC-0075 §4); this module binds decoded rows to it and admits offchain input.

use anyhow::{anyhow, Context, Result};
use std::time::Duration;

use crate::entity_bind::Binding;
use crate::entity_offchain::Snapshot;
use crate::entity_plan::{Plan, Relation};
use crate::entity_row::Row;
use crate::registry::{DecodeRegistry, DecodedRow};
use dbsp::ZWeight;
use nuthatch_core::entity_view::{EntityLane, Limits};

pub use nuthatch_core::entity_view::{Applied, Batch, OffchainMark, Watermarks};

/// How often an entity's state size may be sampled: `NUTHATCH_ENTITY_STATE_SAMPLE_SECS`, default 60,
/// `0` for never. A sample walks every trace (#1834), so it is rationed rather than taken per scrape.
fn state_sample_interval() -> Option<Duration> {
    static INTERVAL: std::sync::OnceLock<Option<Duration>> = std::sync::OnceLock::new();
    *INTERVAL.get_or_init(|| {
        let secs = match std::env::var("NUTHATCH_ENTITY_STATE_SAMPLE_SECS") {
            Ok(v) => v.trim().parse::<u64>().unwrap_or_else(|_| {
                tracing::warn!(
                    "NUTHATCH_ENTITY_STATE_SAMPLE_SECS={v} is not a number of seconds; using 60"
                );
                60
            }),
            Err(_) => 60,
        };
        (secs > 0).then(|| Duration::from_secs(secs))
    })
}

/// One maintained authored entity: its circuit, its state, and how far it has been applied.
pub struct EntityView {
    name: String,
    /// The output columns' names, in the order the relation's key then aggregates appear.
    ///
    /// Carried here rather than on [`Plan`] because a circuit and a batch evaluator are positional
    /// and have no use for a name; only the serving surface does (#822).
    columns: Vec<String>,
    lane: EntityLane,
    binding: Binding,
    /// Why this entity holds no answer despite the nest being fine - today, only a warm restart.
    ///
    /// Kept apart from the lane's fault on purpose. A faulted entity had a circuit that died; an
    /// unavailable one never had state to lose. Reporting them as the same thing would make #866's
    /// fault reporting say something untrue about a nest that is working.
    unavailable: Option<String>,
    /// Per offchain table, the snapshots sent to the circuit (#1437). Sent, not folded: the next
    /// refresh compares the manifest against this, so no snapshot is ever fed twice.
    fed: std::sync::Mutex<std::collections::BTreeMap<String, Vec<String>>>,
    /// Per offchain table, the rows and uncompressed bytes sent, against the allowance in
    /// [`Self::admit_offchain`].
    held: std::sync::Mutex<std::collections::BTreeMap<String, (u64, u64)>>,
    max_rows: u64,
}

impl EntityView {
    /// Spawn the circuit on its own thread. DBSP drives worker threads; keep it off the async pool,
    /// exactly as `BalanceView::start` does.
    ///
    /// The plan is bound against `registry` here, so an entity naming a column this nest's ABI does
    /// not have is refused now rather than at the first block that would have used it.
    /// `warm` says this nest already has indexed history behind it.
    ///
    /// **A warm start makes the entity unavailable, deliberately.** Entity state is derived and not
    /// persisted, and it cannot be rebuilt yet: sealing *prunes* the sealed rows from the hot store,
    /// so replaying what is left would cover only the unsealed tail and produce a relation that is
    /// missing all of history while looking perfectly populated. Rebuilding from sealed Parquet is
    /// §5.3's warm-restart seed, which is not built.
    ///
    /// Feeding it from the cursor onward would be worse than leaving it empty: a relation holding
    /// *some* of the answer is the "plausible partial relation served as current" §5.1 forbids,
    /// where an empty one at least cannot be mistaken for the truth. So it is fed nothing and says
    /// why.
    pub fn start(
        name: &str,
        plan: &Plan,
        columns: &[String],
        registry: &DecodeRegistry,
        max_rows: usize,
        warm: bool,
    ) -> Result<Self> {
        let binding = Binding::bind(plan, registry)
            .with_context(|| format!("binding entity `{name}` to this nest's tables"))?;
        Self::start_bound(name, plan, binding, columns, max_rows, warm)
    }

    /// [`Self::start`] for a plan already bound, which is how one reading offchain snapshots is
    /// started: the caller binds it against the nest's offchain tables as well as its registry.
    pub fn start_bound(
        name: &str,
        plan: &Plan,
        binding: Binding,
        columns: &[String],
        max_rows: usize,
        warm: bool,
    ) -> Result<Self> {
        if max_rows == 0 {
            return Err(anyhow!(
                "entity `{name}` declares max_rows = 0, which admits nothing. §7 wants a bound that \
                 bites, not one that forbids the entity outright"
            ));
        }
        let unavailable = warm.then(|| {
            format!(
                "entity `{name}` cannot be rebuilt after a restart: its state is derived and not \
                 persisted, and sealing prunes sealed rows from the hot store, so replaying what \
                 remains would cover only the unsealed tail. RFC-0041 §5.3's warm-restart seed is \
                 not implemented. Re-index this nest from its start block to repopulate it."
            )
        });
        let limits = Limits {
            max_rows,
            allowance_bytes: crate::runtime::entity_allowance_bytes(
                max_rows as u64,
                !binding.offchain_tables().is_empty(),
            ),
            sample_every: state_sample_interval(),
        };
        let lane = EntityLane::start(nuthatch_core::drive::Driver::Threaded, name, plan, limits)
            .with_context(|| format!("failed to spawn the circuit thread for entity `{name}`"))?;
        Ok(Self {
            name: name.to_string(),
            columns: columns.to_vec(),
            lane,
            binding,
            unavailable,
            fed: Default::default(),
            held: Default::default(),
            max_rows: max_rows as u64,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Bytes the circuit held at its last sample; `None` before the first or with sampling off.
    pub fn state_bytes(&self) -> Option<u64> {
        self.lane.state_bytes()
    }

    /// The output columns' names - key columns first, then aggregates.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The relation as JSON objects keyed by column name, which is the shape `/sql` needs.
    ///
    /// **This copies every maintained row, per call** - #822 criterion 7's separate term, and it is
    /// what the analytical seam is: `/sql` builds a `HashMap<String, Vec<Value>>` of hot rows and
    /// defines a view over it. Recorded rather than optimised away by instinct; the criterion says to
    /// measure it first and only act if it is material.
    pub fn rows_as_json(&self) -> Vec<serde_json::Value> {
        self.rows_as_json_with_watermark().0
    }

    /// The rows **and the watermark that describes them**, under one lock acquisition (#932).
    ///
    /// Taking them separately is a race, and not a theoretical one: measured at **1 in 12** reads on
    /// a 0.25s-block chain, 0 in 28 on a 12s one. `/sql` executed its query against the rows and then
    /// built the response, whose `provenance.entities[].applied_through` called `applied_through()`
    /// afresh - so a batch landing in between produced an answer whose rows were from block N while
    /// its label said N+8.
    pub fn rows_as_json_with_watermark(&self) -> (Vec<serde_json::Value>, Applied) {
        self.lane.rows_as_json_with_watermark(&self.columns)
    }

    /// Whether the circuit is alive and folding. `false` means it died - on start, on a step, or on
    /// the declared bound - and the ingest loop treats that as terminal rather than serving a frozen
    /// relation. A clean shutdown drops the lane and exits without flipping this.
    pub fn is_healthy(&self) -> bool {
        self.lane.is_healthy()
    }

    /// Why this entity holds no answer, if it does not. `None` means it is maintaining normally.
    pub fn unavailable(&self) -> Option<&str> {
        self.unavailable.as_deref()
    }

    /// The decoded tables this entity reads: none, one, or two when it joins.
    pub fn chain_tables(&self) -> Vec<&str> {
        std::iter::once(&self.binding.left)
            .chain(self.binding.right.as_ref())
            .filter(|s| s.offchain_table().is_none())
            .map(|s| s.table.as_str())
            .collect()
    }

    /// Bring a warm-started entity to life with the history it missed (RFC-0041 §5.3).
    ///
    /// Called once, at startup, with **every** fact the nest holds - sealed and hot. It clears the
    /// `unavailable` state on the way in, because until this lands the entity is deliberately fed
    /// nothing at all.
    ///
    /// §5.1 says backfill and tip are one path differing only in batch size, and this is that taken
    /// literally: **the seed is a backfill batch**. There is no separate seed relation to combine,
    /// no per-aggregate combine rule, and no question about a finalized row joining a hot one -
    /// which is where §5.3's base-plus-delta wording comes apart, since such a pair is in neither
    /// half of it. Criterion 5's "matches uninterrupted execution" holds by construction rather than
    /// by an argument about algebra that has to be re-checked for every aggregate anyone adds.
    ///
    /// The cost is the one §5.3 already accepts: *"This pays the historical computation once per
    /// restart rather than once per request."* `max_rows` applies to the seed like any other window,
    /// so an entity whose history does not fit its declared bound cannot be restarted - which is the
    /// correct answer, since it could not have been running in the first place.
    pub fn seed(&mut self, rows: &[DecodedRow], through: u64) -> Result<()> {
        self.seed_begin();
        self.seed_chunk(rows, through)?;
        self.seed_end()
    }

    /// Open a seed. Split from [`Self::seed`] so a caller holding a very large history can feed it
    /// in pieces rather than materialising all of it - see
    /// [`crate::seal::read_table_rows_by_segment`] for why that matters and what it was measured
    /// to cost. The three calls are the same seed: `begin`, then one or more `chunk`s at `+1`,
    /// then `end`.
    pub fn seed_begin(&mut self) {
        self.unavailable = None;
    }

    /// Feed one piece of stored history at `+1`. Ordering across chunks does not matter and
    /// neither does the split: DBSP folds a window, and a seed is windows.
    pub fn seed_chunk(&self, rows: &[DecodedRow], through: u64) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.apply_window_inner(rows, 1, through, false)
            .with_context(|| format!("seeding entity `{}` from stored history", self.name))
    }

    /// Close a seed: wait for every chunk to be folded, and refuse a relation that faulted while
    /// folding one. A fault here is terminal exactly as it is at tip.
    pub fn seed_end(&self) -> Result<()> {
        self.flush();
        if let Some(why) = self.fault() {
            return Err(anyhow!("seeding entity `{}` faulted: {why}", self.name));
        }
        Ok(())
    }

    /// The offchain tables this entity reads (#1437), by manifest name.
    pub fn offchain_tables(&self) -> Vec<&str> {
        self.binding.offchain_tables()
    }

    /// The snapshots of `table` already sent to this entity, in append order.
    pub fn offchain_fed(&self, table: &str) -> Vec<String> {
        self.fed
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .unwrap_or_default()
    }

    /// Feed appended `snapshots` of `table` at `+1`, carrying it to `version` (#1437). Offchain rows
    /// are never retracted: a snapshot that changes any other way is a [`Self::rebuild_begin`].
    ///
    /// An unavailable entity is fed nothing, exactly as with a window.
    pub fn apply_offchain(
        &self,
        table: &str,
        snapshots: &[Snapshot],
        version: &[String],
    ) -> Result<()> {
        if self.unavailable.is_some() {
            return Ok(());
        }
        let Some(rows) = self.admit_offchain(table, snapshots) else {
            return Ok(());
        };
        let batch = self.offchain_batch(table, snapshots)?;
        self.lane
            .apply_offchain(batch, table.to_string(), OffchainMark::of(version));
        self.fed
            .lock()
            .unwrap()
            .insert(table.to_string(), version.to_vec());
        self.held.lock().unwrap().insert(table.to_string(), rows);
        Ok(())
    }

    /// The rows and bytes `table` holds once `snapshots` are sent, or `None` when the entity's
    /// offchain input would pass its allowance. Then this entity alone faults, before any row is read.
    ///
    /// The allowance is the declared `max_rows`, in rows and at admission's per-row charge in bytes.
    /// Admission charges an entity that reads offchain for it on top of its own rows, so every
    /// allowance on a cursor sits inside that cursor's budget (RFC-0041 §7).
    fn admit_offchain(&self, table: &str, snapshots: &[Snapshot]) -> Option<(u64, u64)> {
        let add =
            |(r, b): (u64, u64), (sr, sb): (u64, u64)| (r.saturating_add(sr), b.saturating_add(sb));
        let (others, before) = {
            let held = self.held.lock().unwrap();
            let others = held
                .iter()
                .filter(|(t, _)| t.as_str() != table)
                .fold((0, 0), |sum, (_, h)| add(sum, *h));
            (others, held.get(table).copied().unwrap_or((0, 0)))
        };
        let after = snapshots
            .iter()
            .fold(before, |sum, s| add(sum, (s.row_count(), s.byte_size())));
        let (rows, bytes) = add(others, after);
        let max_bytes = self
            .max_rows
            .saturating_mul(crate::runtime::ENTITY_RSS_BYTES_PER_ROW);
        if rows <= self.max_rows && bytes <= max_bytes {
            return Some(after);
        }
        self.lane.stop(format!(
            "offchain__{table} would bring entity `{}`'s offchain input to {rows} rows and {bytes} \
             bytes, over its allowance of {} rows and {max_bytes} bytes. Raise its max_rows, which \
             admission charges for",
            self.name, self.max_rows
        ));
        None
    }

    /// Start rebuilding off to one side (#1437). Until [`Self::rebuild_commit`], readers keep the
    /// current relation and the version that labels it; the cost is holding both while it runs.
    pub fn rebuild_begin(&self) {
        self.fed.lock().unwrap().clear();
        self.held.lock().unwrap().clear();
        self.lane.rebuild_begin();
    }

    /// Feed stored chain history to the rebuild at `+1`.
    pub fn rebuild_chain(&self, rows: &[DecodedRow]) -> Result<()> {
        let (left, right) = self
            .binding
            .window(rows)
            .with_context(|| format!("rebuilding entity `{}` from stored history", self.name))?;
        let batch = Batch {
            left: left.into_iter().map(|r| (r, 1)).collect(),
            right: right.into_iter().map(|r| (r, 1)).collect(),
        };
        self.lane.rebuild_batch(batch);
        Ok(())
    }

    /// Feed every present snapshot of `table` to the rebuild.
    pub fn rebuild_offchain(
        &self,
        table: &str,
        snapshots: &[Snapshot],
        version: &[String],
    ) -> Result<()> {
        let Some(rows) = self.admit_offchain(table, snapshots) else {
            return Ok(());
        };
        let batch = self.offchain_batch(table, snapshots)?;
        self.lane
            .rebuild_offchain(batch, table.to_string(), OffchainMark::of(version));
        self.fed
            .lock()
            .unwrap()
            .insert(table.to_string(), version.to_vec());
        self.held.lock().unwrap().insert(table.to_string(), rows);
        Ok(())
    }

    /// Swap the rebuilt relation in, labelled `through`, and wait until it is visible.
    pub fn rebuild_commit(&self, through: u64) -> Result<()> {
        self.lane.rebuild_commit(through);
        match self.fault() {
            Some(why) => Err(anyhow!("rebuilding entity `{}` faulted: {why}", self.name)),
            None => Ok(()),
        }
    }

    fn offchain_batch(&self, table: &str, snapshots: &[Snapshot]) -> Result<Batch> {
        let mut batch = Batch {
            left: Vec::new(),
            right: Vec::new(),
        };
        for snapshot in snapshots {
            let (left, right) = self
                .binding
                .snapshot(table, snapshot)
                .with_context(|| format!("feeding offchain__{table} to entity `{}`", self.name))?;
            batch.left.extend(left.into_iter().map(|r| (r, 1)));
            batch.right.extend(right.into_iter().map(|r| (r, 1)));
        }
        Ok(batch)
    }

    /// Why this entity stopped, if it has. `None` is a live entity.
    pub fn fault(&self) -> Option<String> {
        self.lane.fault()
    }

    /// **§5.1 and §5.2 in one call.** Convert a decoded window to this entity's input relations and
    /// enqueue it at `weight`.
    ///
    /// `+1` is a window of canonical facts, from backfill or from tip - §5.1 says the two differ in
    /// batch size and not in semantics. `-1` is the same rows fed back before deletion, which is all
    /// a reorg is here (§5.2). The caller feeds the removed rows at `-1` and the replacements at
    /// `+1`; there is no rollback interface because there is nothing to roll back.
    ///
    /// A window carrying nothing for this entity is still progress and still advances the watermark:
    /// the entity is current through that block, having correctly folded no facts.
    pub fn apply_window(&self, rows: &[DecodedRow], weight: ZWeight, through: u64) -> Result<()> {
        self.apply_window_inner(rows, weight, through, true)
    }

    /// Convert a decoded window to this entity's input at `weight` without feeding it, so a caller
    /// can fail before a commit and [`Self::apply`] only once the commit has landed. `None` for an
    /// unavailable entity, which is fed nothing.
    pub fn bind_window(&self, rows: &[DecodedRow], weight: ZWeight) -> Result<Option<Batch>> {
        // An unavailable entity is fed nothing at all. Half an answer is the failure mode; none is
        // merely an absence, and `unavailable()` is what says so.
        if self.unavailable.is_some() {
            return Ok(None);
        }
        let (left, right) = self
            .binding
            .window(rows)
            .with_context(|| format!("converting a window for entity `{}`", self.name))?;
        Ok(Some(Batch {
            left: left.into_iter().map(|r| (r, weight)).collect(),
            right: right.into_iter().map(|r| (r, weight)).collect(),
        }))
    }

    fn apply_window_inner(
        &self,
        rows: &[DecodedRow],
        weight: ZWeight,
        through: u64,
        publish: bool,
    ) -> Result<()> {
        let Some(batch) = self.bind_window(rows, weight)? else {
            return Ok(());
        };
        if publish {
            self.apply(batch, through);
        } else {
            self.apply_deferred(batch, through);
        }
        Ok(())
    }
    /// Enqueue an already-bound batch. Non-blocking, and drops silently if the circuit has died -
    /// which `is_healthy` is what reports, not this.
    pub fn apply(&self, batch: Batch, through: u64) {
        self.lane.apply(batch, through);
    }

    /// Enqueue a batch **without** publishing the relation afterwards; the next [`Self::flush`]
    /// publishes it.
    fn apply_deferred(&self, batch: Batch, through: u64) {
        self.lane.apply_deferred(batch, through);
    }

    /// Block until every batch enqueued so far has been folded. Used after a restart rebuild so the
    /// first request sees a complete relation, and as the consistency boundary at a commit.
    pub fn flush(&self) {
        self.lane.flush();
    }

    /// Sample the state size now, on the circuit's thread, after every batch sent before it; returns
    /// the bytes and what the sample cost. The periodic sample runs the same walk.
    pub fn sample_state(&self) -> Result<(u64, Duration)> {
        self.lane.sample_state()
    }

    /// The maintained relation as of the last folded batch.
    pub fn relation(&self) -> Relation {
        self.lane.relation()
    }

    /// One key's row, read under the lock.
    ///
    /// **Not `relation().get(k)`.** That copies the whole relation to answer one key, which on a
    /// real nest is 49 ms for 309,548 groups (#897) - on the very route whose claim is that a keyed
    /// read is a map lookup and not a scan.
    pub fn get(&self, key: &Row) -> Option<Row> {
        self.lane.get(key)
    }

    /// A keyed read **with** the watermark describing it, in **one** acquisition (#932).
    ///
    /// Serving a row and then reading `applied_through()` separately is the race this fixes; doing it
    /// inside the accessor would only move the race, so both come off the same guard.
    ///
    /// `readings` holds, per key part, each scalar that part could be: a caller with text in hand
    /// cannot know a part's type, and the first combination the relation holds is the key (#2205).
    pub fn get_with_watermark(
        &self,
        readings: &[Vec<crate::entity_row::Scalar>],
    ) -> (Option<Row>, Applied) {
        self.lane.get_with_watermark(readings)
    }

    /// [`Self::head_rows`] **with** the watermark describing those rows, one acquisition (#932).
    pub fn head_rows_with_watermark(&self, limit: usize) -> (usize, Vec<(Row, Row)>, Applied) {
        self.lane.head_rows_with_watermark(limit)
    }

    /// The first `limit` rows and the total count, read under the lock. Same reason as
    /// [`Self::get`]: the listing route returns at most a hundred rows and was copying every one
    /// the entity holds to do it.
    pub fn head_rows(&self, limit: usize) -> (usize, Vec<(Row, Row)>) {
        let (len, rows, _) = self.lane.head_rows_with_watermark(limit);
        (len, rows)
    }

    pub fn len(&self) -> usize {
        self.lane.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lane.is_empty()
    }

    /// Row count **and** watermark under one lock (#932).
    ///
    /// Every surface reporting both - `/derived`, `/ready`, the semantic doc - was reading them
    /// separately, so a batch landing between the two produced a count from block N labelled N+k.
    pub fn len_and_watermark(&self) -> (usize, Applied) {
        self.lane.len_and_watermark()
    }

    /// The block this entity has folded through (criterion 2).
    ///
    /// **Serving reads this, never the nest's head.** An entity behind the dataset is a normal state
    /// during backfill and after a definition change; answering for the nest's head while holding
    /// this one's rows is how a partial relation gets stamped current.
    pub fn applied_through(&self) -> u64 {
        self.lane.applied_through()
    }

    /// The applied-through watermark **for a fence, never for a label** (#1186).
    ///
    /// [`applied_through`](Self::applied_through) plus the offchain snapshots applied (#1437), since
    /// an appended snapshot changes the rows without moving the block, under a different name on
    /// purpose. #932 is that a watermark read separately from the rows it describes can label an
    /// answer more current than the rows in it, and `entity_provenance_is_atomic.rs` is a source gate
    /// that keeps bare `applied_through()` out of the serving path so nobody reintroduces it by
    /// accident. The analytical memo needs a watermark for a different job: deciding whether the
    /// state has moved since a remembered answer was computed. That value is compared and discarded -
    /// it never reaches a response, where the label always comes from `rows_as_json_with_watermark`
    /// beside the rows themselves.
    ///
    /// So the gate keeps meaning exactly what it says, and a value that must never be reported has a
    /// name that says so. If you find yourself putting this into a response body, you want
    /// `rows_as_json_with_watermark`, and you are about to reintroduce #932.
    pub fn fence_watermark(&self) -> Applied {
        self.lane.fence_watermark()
    }

    /// When the applied-through watermark last moved, in unix seconds; `0` before the first batch.
    pub fn last_progress(&self) -> u64 {
        self.lane.last_progress()
    }

    /// Is this entity current for a dataset advertising `head`?
    pub fn is_current(&self, head: u64) -> bool {
        self.applied_through() >= head
    }
}

impl nuthatch_core::entity_view::WindowEntity for EntityView {
    fn name(&self) -> &str {
        EntityView::name(self)
    }
    fn bind_window(&self, rows: &[DecodedRow], weight: ZWeight) -> Result<Option<Batch>> {
        EntityView::bind_window(self, rows, weight)
    }
    fn apply(&self, batch: Batch, through: u64) {
        EntityView::apply(self, batch, through)
    }
    fn apply_window(&self, rows: &[DecodedRow], weight: ZWeight, through: u64) -> Result<()> {
        EntityView::apply_window(self, rows, weight, through)
    }
    fn fault(&self) -> Option<String> {
        EntityView::fault(self)
    }
    fn applied_through(&self) -> u64 {
        EntityView::applied_through(self)
    }
    fn flush(&self) {
        EntityView::flush(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_expr::{Cmp, Expr};
    use crate::entity_plan::{Agg, Join, Source};
    use crate::entity_row::Scalar;
    use crate::registry::ContractSpec;
    use crate::rpc::Log;
    use proptest::prelude::*;

    const ERC20: &str = r#"[
        {"type":"event","name":"Transfer","inputs":[
            {"name":"from","type":"address","indexed":true},
            {"name":"to","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false},
        {"type":"event","name":"Approval","inputs":[
            {"name":"owner","type":"address","indexed":true},
            {"name":"spender","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false}
    ]"#;
    const TRANSFER_TOPIC0: &str =
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    const APPROVAL_TOPIC0: &str =
        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
    const TOKEN: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
    const ALICE: &str = "0x1111111111111111111111111111111111111111";
    const BOB: &str = "0x2222222222222222222222222222222222222222";

    fn registry() -> DecodeRegistry {
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ERC20).unwrap();
        DecodeRegistry::build(vec![ContractSpec {
            alias: "usdc".into(),
            address: TOKEN.parse().unwrap(),
            abi,
            events: Vec::new(),
        }])
        .unwrap()
    }

    fn log(topic0: &str, a: &str, b: &str, value: &str, block: u64, li: u64) -> Log {
        Log {
            address: TOKEN.into(),
            topics: vec![
                topic0.into(),
                format!("0x{:0>64}", a.trim_start_matches("0x")),
                format!("0x{:0>64}", b.trim_start_matches("0x")),
            ],
            data: format!("0x{:0>64}", value),
            block_number: block,
            block_hash: "0xbh".into(),
            tx_hash: "0xtx".into(),
            log_index: li,
        }
    }

    fn decode(reg: &DecodeRegistry, logs: &[Log]) -> Vec<DecodedRow> {
        logs.iter()
            .map(|l| reg.decode(l).unwrap().expect("a decoder must match"))
            .collect()
    }

    fn src(t: &str, cols: &[&str]) -> Source {
        Source {
            table: t.into(),
            columns: cols.iter().map(|c| c.to_string()).collect(),
        }
    }

    /// `SELECT to, SUM(value) FROM usdc__transfer WHERE value > 0 GROUP BY to`
    /// Column names for the fixtures below. The serving surface needs them; a circuit does not.
    fn cols() -> Vec<String> {
        vec!["to".into(), "sum_value".into()]
    }

    fn received() -> Plan {
        Plan {
            left: src("usdc__transfer", &["to", "value"]),
            left_filter: Some(Expr::Compare(
                Cmp::Gt,
                Expr::Column(1).into(),
                Expr::Literal(Scalar::Int(0)).into(),
            )),
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        }
    }

    /// The same, joined to the approvals table, so both input relations carry rows.
    fn received_by_approved() -> Plan {
        Plan {
            join: Some(Join {
                right: src("usdc__approval", &["owner"]),
                right_filter: None,
                on: (0, 0),
            }),
            ..received()
        }
    }

    fn bob() -> Row {
        Row(vec![Scalar::Str(BOB.into())])
    }

    #[test]
    fn a_decoded_window_is_folded_and_carries_the_watermark_with_it() {
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1_000, false).unwrap();
        assert_eq!(v.applied_through(), 0, "nothing folded yet");

        let rows = decode(
            &reg,
            &[
                log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0),
                log(TRANSFER_TOPIC0, ALICE, BOB, "5", 100, 1),
            ],
        );
        v.apply_window(&rows, 1, 100).unwrap();
        v.flush();

        assert!(v.is_healthy());
        assert_eq!(
            v.relation().get(&bob()),
            Some(&Row(vec![Scalar::Int(12)])),
            "0x7 + 0x5, summed by a circuit built from the plan"
        );
        assert_eq!(v.applied_through(), 100);
        assert!(v.is_current(100));
        assert!(!v.is_current(101), "it has not folded 101");
    }

    /// #1834: the walk runs once per interval, not per batch, so the second window leaves the reading
    /// where the first put it until an explicit sample takes a new one.
    #[test]
    fn the_state_size_is_sampled_once_per_interval_not_per_batch() {
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1_000, false).unwrap();
        assert_eq!(v.state_bytes(), None, "nothing folded, nothing sampled");

        let first = decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0)]);
        v.apply_window(&first, 1, 100).unwrap();
        v.flush();
        let after_first = v.state_bytes().expect("the first change is sampled");

        let more: Vec<Log> = (0..200)
            .map(|i| log(TRANSFER_TOPIC0, ALICE, BOB, "1", 101, i))
            .collect();
        v.apply_window(&decode(&reg, &more), 1, 101).unwrap();
        v.flush();
        assert_eq!(
            v.state_bytes(),
            Some(after_first),
            "resampled inside the interval"
        );

        let (now, _) = v.sample_state().unwrap();
        assert!(
            now > after_first,
            "200 more facts in the trace: {now} vs {after_first}"
        );
        assert_eq!(v.state_bytes(), Some(now));
    }

    #[test]
    fn a_reorg_retracts_at_minus_one_and_converges_on_the_replacement() {
        // §5.2: removed rows are fed at -1 before deletion, replacements arrive at +1.
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1_000, false).unwrap();

        let orphaned = decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0)]);
        let replacement = decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, BOB, "9", 100, 0)]);

        v.apply_window(&orphaned, 1, 100).unwrap();
        v.apply_window(&orphaned, -1, 100).unwrap();
        v.apply_window(&replacement, 1, 101).unwrap();
        v.flush();

        assert_eq!(v.relation().get(&bob()), Some(&Row(vec![Scalar::Int(9)])));
        assert_eq!(v.applied_through(), 101);
        assert!(v.is_healthy());
    }

    /// #1437: a rebuild is folded beside the live relation, so readers keep the old one, and the
    /// label that describes it, until the commit swaps the new one in whole.
    #[test]
    fn a_rebuild_is_invisible_until_it_commits() {
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1_000, false).unwrap();
        v.apply_window(
            &decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0)]),
            1,
            100,
        )
        .unwrap();
        v.flush();
        let before = v.relation();

        v.rebuild_begin();
        v.rebuild_chain(&decode(
            &reg,
            &[log(TRANSFER_TOPIC0, ALICE, BOB, "9", 100, 0)],
        ))
        .unwrap();
        v.flush();
        assert_eq!(v.relation(), before, "a pending rebuild is not visible");

        v.rebuild_commit(100).unwrap();
        assert_eq!(v.relation().get(&bob()), Some(&Row(vec![Scalar::Int(9)])));
        assert_eq!(v.applied_through(), 100);
        assert!(v.is_healthy());
    }

    /// **#838.** The spike's bound counted the delegation input alone, so fifty thousand indexer
    /// facts were admitted at a declared bound of one. A bound that watches one of two relations is
    /// not a bound on the entity's footprint.
    #[test]
    fn max_rows_counts_both_input_relations() {
        let reg = registry();
        let v = EntityView::start("received", &received_by_approved(), &cols(), &reg, 3, false)
            .unwrap();

        // One left row, and four right rows. The left side alone never crosses three.
        let window = decode(
            &reg,
            &[
                log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0),
                log(APPROVAL_TOPIC0, BOB, ALICE, "1", 100, 1),
                log(APPROVAL_TOPIC0, BOB, ALICE, "2", 100, 2),
                log(APPROVAL_TOPIC0, BOB, ALICE, "3", 100, 3),
                log(APPROVAL_TOPIC0, BOB, ALICE, "4", 100, 4),
            ],
        );
        v.apply_window(&window, 1, 100).unwrap();
        v.flush();

        assert!(
            !v.is_healthy(),
            "five input rows against a declared bound of three is a fault"
        );
        let why = v.fault().unwrap_or_default();
        assert!(why.contains("max_rows exceeded"), "{why}");
        assert!(why.contains("both relations"), "{why}");
        assert_eq!(v.applied_through(), 0, "and the watermark did not move");
    }

    /// #2167: `max_rows` bounds input rows, but a join's output is not its input. Two hundred rows a
    /// side, all on one join key, make forty thousand joined rows for a MAX to hold.
    #[test]
    fn state_past_the_admitted_allowance_faults_the_entity() {
        let reg = registry();
        let plan = Plan {
            join: Some(Join {
                right: src("usdc__approval", &["owner", "value"]),
                right_filter: None,
                on: (0, 0),
            }),
            key: vec![Expr::Column(1), Expr::Column(3)],
            aggregates: vec![Agg::Max(Expr::Column(1))],
            ..received()
        };
        let n = 200u64;
        let v = EntityView::start(
            "fanned",
            &plan,
            &["a".into(), "b".into(), "m".into()],
            &reg,
            2 * n as usize,
            false,
        )
        .unwrap();
        let logs: Vec<_> = (0..n)
            .flat_map(|i| {
                let value = format!("{:x}", i + 1);
                [
                    log(TRANSFER_TOPIC0, ALICE, BOB, &value, 100, 2 * i),
                    log(APPROVAL_TOPIC0, BOB, ALICE, &value, 100, 2 * i + 1),
                ]
            })
            .collect();
        v.apply_window(&decode(&reg, &logs), 1, 100).unwrap();
        v.flush();
        let _ = v.sample_state();

        let why = v
            .fault()
            .expect("state past what admission charged is a fault");
        assert!(why.contains("allowance"), "{why}");
        assert!(!why.contains("max_rows exceeded"), "400 rows fit: {why}");
    }

    #[test]
    fn crossing_max_rows_faults_the_circuit_rather_than_warning() {
        // Criterion 10: neither warns-and-continues nor OOMs the cursor.
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1, false).unwrap();
        let rows = decode(
            &reg,
            &[
                log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0),
                log(TRANSFER_TOPIC0, ALICE, ALICE, "5", 100, 1),
            ],
        );
        v.apply_window(&rows, 1, 100).unwrap();
        v.flush();

        assert!(
            !v.is_healthy(),
            "the declared bound is a fault, not a log line"
        );
        assert_eq!(
            v.applied_through(),
            0,
            "a batch that was not folded must not advance the watermark"
        );
    }

    /// The bound is on the entity's live footprint, not on one window's size. An entity fed one row
    /// at a time still crosses it, and a check that only ever looks at the batch in hand never sees
    /// that happen.
    #[test]
    fn the_bound_is_on_what_the_entity_holds_not_on_one_windows_size() {
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 2, false).unwrap();
        for (i, to) in [BOB, ALICE, TOKEN].iter().enumerate() {
            let block = 100 + i as u64;
            v.apply_window(
                &decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, to, "7", block, 0)]),
                1,
                block,
            )
            .unwrap();
        }
        v.flush();

        assert!(!v.is_healthy(), "three rows against a bound of two");
        assert_eq!(
            v.applied_through(),
            101,
            "it folded the first two windows and refused the third"
        );
    }

    /// Retraction gives the footprint back. An entity that reorgs its way down below the bound is
    /// not over it, and a running total that only ever climbs would fault on a nest that never grew.
    #[test]
    fn a_retraction_returns_the_footprint_it_took() {
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 2, false).unwrap();
        let first = decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0)]);
        let second = decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, ALICE, "5", 101, 0)]);
        let third = decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, TOKEN, "3", 102, 0)]);

        v.apply_window(&first, 1, 100).unwrap();
        v.apply_window(&second, 1, 101).unwrap();
        v.apply_window(&first, -1, 102).unwrap();
        v.apply_window(&third, 1, 103).unwrap();
        v.flush();

        assert!(
            v.is_healthy(),
            "two live rows throughout, against a bound of two"
        );
        assert_eq!(v.applied_through(), 103);
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn the_watermark_does_not_advance_past_a_batch_the_circuit_refused() {
        // The stale-serving guard criterion 2 asks for: an entity that stopped folding at 100 must
        // not answer for 200 merely because a later batch was enqueued.
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1, false).unwrap();
        v.apply_window(
            &decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0)]),
            1,
            100,
        )
        .unwrap();
        v.flush();
        assert_eq!(v.applied_through(), 100);

        v.apply_window(
            &decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, ALICE, "5", 200, 0)]),
            1,
            200,
        )
        .unwrap();
        v.flush();
        assert!(!v.is_healthy());
        assert_eq!(v.applied_through(), 100, "it did not fold 200");
        assert!(!v.is_current(200));
    }

    #[test]
    fn an_empty_window_still_advances_the_watermark() {
        // A window with no facts for this entity is progress: the entity is current through it. The
        // built-in views can skip an empty batch because they have no watermark to move.
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1_000, false).unwrap();
        v.apply_window(&[], 1, 500).unwrap();
        v.flush();
        assert!(v.is_healthy());
        assert_eq!(v.applied_through(), 500);
        assert!(v.is_empty());
    }

    /// A window carrying only other tables' rows is the ordinary case - a nest decodes many tables
    /// and an entity reads one or two - and it must be progress rather than a fault.
    #[test]
    fn a_window_of_other_tables_is_progress_not_a_fault() {
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1_000, false).unwrap();
        v.apply_window(
            &decode(&reg, &[log(APPROVAL_TOPIC0, ALICE, BOB, "1", 300, 0)]),
            1,
            300,
        )
        .unwrap();
        v.flush();
        assert!(v.is_healthy());
        assert_eq!(v.applied_through(), 300);
        assert!(v.is_empty(), "the approval is not this entity's table");
    }

    /// Retracting rows that were never applied means the caller and the circuit disagree about what
    /// the entity holds. Continuing from there produces answers neither of them can account for.
    #[test]
    fn retracting_more_rows_than_were_applied_is_a_fault() {
        let reg = registry();
        let v = EntityView::start("received", &received(), &cols(), &reg, 1_000, false).unwrap();
        v.apply_window(
            &decode(&reg, &[log(TRANSFER_TOPIC0, ALICE, BOB, "7", 100, 0)]),
            -1,
            100,
        )
        .unwrap();
        v.flush();
        assert!(!v.is_healthy());
        assert_eq!(v.applied_through(), 0);
        // The *reason*, not merely the fact. Without this the test passes on a build where the
        // negative count wraps into the `max_rows` comparison instead - the right refusal reached by
        // the wrong route, reported to whoever is on call as a bound they have not crossed.
        let why = v.fault().unwrap_or_default();
        assert!(why.contains("retracts"), "{why}");
        assert!(!why.contains("max_rows"), "{why}");
    }

    /// An entity naming a column this nest's ABI does not have is refused when it starts, not at the
    /// first block that would have used it.
    #[test]
    fn an_entity_that_does_not_bind_is_refused_at_start() {
        let reg = registry();
        let plan = Plan {
            left: src("usdc__transfer", &["to", "amount"]),
            ..received()
        };
        let err = format!(
            "{:#}",
            EntityView::start("received", &plan, &cols(), &reg, 1_000, false)
                .err()
                .expect("an unbindable entity must not start")
        );
        assert!(err.contains("no column amount"), "{err}");
    }

    #[test]
    fn a_declared_bound_of_zero_is_refused() {
        let reg = registry();
        let err = format!(
            "{:#}",
            EntityView::start("received", &received(), &cols(), &reg, 0, false)
                .err()
                .expect("a bound of zero must not start")
        );
        assert!(err.contains("admits nothing"), "{err}");
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

        /// **Criterion 4.** A random sequence of applies and retractions must converge to a clean
        /// replay of the surviving facts - which is what a reorg of any depth is, at this layer.
        #[test]
        fn random_apply_and_retract_sequences_converge_on_a_clean_replay(
            facts in prop::collection::vec((0u8..3, 1u64..20, any::<bool>()), 0..10),
        ) {
            let reg = registry();
            let who = [ALICE, BOB, TOKEN];

            let live = EntityView::start("received", &received(), &cols(), &reg, 1_000, false).unwrap();
            let replay = EntityView::start("replay", &received(), &cols(), &reg, 1_000, false).unwrap();

            let mut survivors: Vec<DecodedRow> = Vec::new();
            for (i, (to, value, orphaned)) in facts.iter().enumerate() {
                let block = 100 + i as u64;
                let row = decode(
                    &reg,
                    &[log(TRANSFER_TOPIC0, ALICE, who[*to as usize], &format!("{value:x}"), block, 0)],
                );
                live.apply_window(&row, 1, block).unwrap();
                if *orphaned {
                    // The reorg: fed back at -1 before deletion, exactly as balances work today.
                    live.apply_window(&row, -1, block).unwrap();
                } else {
                    survivors.extend(row);
                }
            }

            // The clean replay: only the facts that survived, applied once each.
            for (i, row) in survivors.iter().enumerate() {
                replay.apply_window(std::slice::from_ref(row), 1, 100 + i as u64).unwrap();
            }

            live.flush();
            replay.flush();
            prop_assert!(live.is_healthy() && replay.is_healthy());
            // Two empty relations compare equal, so say what a pass has to mean: every surviving
            // fact has a value in 1..20 and the filter admits anything above zero, so a non-empty
            // survivor set must produce a non-empty entity. Without this the property holds
            // perfectly on a circuit that folded nothing at all.
            prop_assert_eq!(
                live.relation().is_empty(),
                survivors.is_empty(),
                "survivors: {}, live rows: {}",
                survivors.len(),
                live.len()
            );
            prop_assert_eq!(live.relation(), replay.relation());
        }
    }
}
