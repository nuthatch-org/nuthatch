//! A nest's cursor steps over its hot store and the state derived from it, borrowed from whoever
//! owns them: the node's `NestIngest`, or a tab's worker.

use crate::alerts::{self, AlertRouter};
use crate::entity_view::WindowEntity;
use crate::exposure::ExposureView;
use crate::factory::ChildRegistry;
use crate::fault::TerminalFault;
use crate::feed::{
    decode_stored_rows, exposure_retraction_batch, oversized_transfer, retraction_batch,
    velocity_retraction_batch,
};
use crate::labels::LabelSet;
use crate::metrics::CursorMetrics;
use crate::store::{HotStore, LAST_BLOCK_KEY, SEALED_THROUGH_KEY, START_BLOCK_KEY};
use crate::velocity::VelocityView;
use crate::views::BalanceView;
use anyhow::{Context, Result};
use nuthatch_decode::registry::{DecodedRow, TableSchema};

/// One nest, borrowed for one step of its cursor.
pub struct Nest<'a, E> {
    pub store: &'a dyn HotStore,
    pub schema: &'a [TableSchema],
    pub balances: &'a BalanceView,
    pub exposure: &'a ExposureView,
    pub velocity: &'a VelocityView,
    pub labels: &'a LabelSet,
    /// The velocity flag's window in blocks, when the nest flags velocity.
    pub velocity_window: Option<u64>,
    pub entities: &'a [E],
    /// The discovered children, when the nest declares a factory.
    pub children: Option<&'a mut ChildRegistry>,
    pub router: &'a AlertRouter,
    pub metrics: &'a dyn CursorMetrics,
}

/// Feed rolled-back rows to every authored entity at weight `-1`.
fn retract_entities<E: WindowEntity>(
    entities: &[E],
    rows: &[DecodedRow],
    ancestor: u64,
) -> Result<()> {
    if entities.is_empty() || rows.is_empty() {
        return Ok(());
    }
    for entity in entities {
        entity.apply_window(rows, -1, ancestor).with_context(|| {
            format!(
                "retracting rolled-back rows from entity `{}`",
                entity.name()
            )
        })?;
    }
    Ok(())
}

/// Roll a nest's mutable hot store + IVM views back to `ancestor` (the deepest reorg-survivor block),
/// returning the block to continue from. Detection is the *caller's* job: a solo nest detects on its
/// own cursor; a runtime detects **once** at the shared boundary and fans this out to every nest. A
/// nest already at or below `ancestor` (e.g. a still-backfilling nest in a runtime while the tip
/// reorgs) is a no-op - nothing above `ancestor` to undo, and its cursor must NOT be bumped up to
/// `ancestor` (that would claim blocks it never indexed). A fork below the sealed watermark is a
/// [`TerminalFault`].
pub fn rollback_reorg<E: WindowEntity>(nest: Nest<'_, E>, ancestor: u64) -> Result<u64> {
    let store = nest.store;
    // Nothing below this nest's origin was indexed, so no rollback goes below it, and a fork
    // deeper than every checkpoint resumes there rather than at block 1 (#2174).
    let deepest = ancestor == 0;
    let ancestor = ancestor.max(
        store
            .get_meta(START_BLOCK_KEY)?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
            .saturating_sub(1),
    );
    // Retract the rolled-back transfers from the IVM view *before* dropping them from the hot
    // store - a reorg is just the same facts re-fed with weight −1.
    let last_indexed = store
        .get_meta(LAST_BLOCK_KEY)?
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(ancestor);
    // This nest hasn't reached past the fork: nothing to undo, and don't advance its cursor.
    if last_indexed <= ancestor {
        return Ok(ancestor + 1);
    }
    // A reorg below the sealed watermark is a finality violation this model can't repair: the
    // doomed blocks are already in immutable sealed segments (and pruned from hot), so the
    // retraction below would be silently incomplete and the sealed layer would permanently disagree
    // with the canonical chain. Halt loudly instead (deadlock-review finding M6). The `--seal-direct`
    // finality depth / `finalized` tag is the contract; if it's being violated, it needs raising.
    let sealed_through = store
        .get_meta(SEALED_THROUGH_KEY)?
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if ancestor < sealed_through {
        // Terminal (RFC-0026 §3): the next attempt re-derives the same watermark and bails
        // identically, so this nest is quarantined until an operator raises the finality depth.
        // `detect_reorg` answers 0 when no checkpoint it holds is canonical, which is not a fork
        // at genesis and must not read as one.
        let fork = if deepest {
            "a fork deeper than every checkpoint this nest holds".to_string()
        } else {
            format!("a reorg to block {ancestor}")
        };
        anyhow::bail!(TerminalFault(format!(
            "{fork} is below the sealed/finalized watermark {sealed_through} - a finality \
             violation this indexer cannot repair; halting. Raise the chain's finality depth."
        )));
    }
    let doomed = store.entities_in_range(ancestor + 1, last_indexed)?;
    // A retry reads the same rows and fails the same way.
    let rows = decode_stored_rows(nest.schema, &doomed).map_err(|e| {
        anyhow::Error::new(TerminalFault(format!(
            "the rows a reorg to block {ancestor} rolls back do not decode: {e:#}"
        )))
    })?;
    // From here the views are being retracted: a failure leaves them out of step with the store
    // and a retry would retract them twice, so only a restart, which rebuilds them, recovers.
    let torn = move |e: anyhow::Error| {
        anyhow::Error::new(TerminalFault(format!(
            "rolling back to block {ancestor} failed part-way, leaving the derived views out of \
             step with the hot store; restart to rebuild them: {e:#}"
        )))
    };
    // §5.2, for the authored entities: the same rows, at weight -1, *before* they are dropped
    // from the hot store. There is no rollback interface because there is nothing to roll back.
    //
    // These are reconstructed through `DecodedRow::from_stored` rather than parsed here, and
    // that is the whole point: DBSP cancels by key, so a retraction built by a different
    // converter than the insertion does not cancel it - it lands beside it and stays forever.
    // One conversion, both directions, and the views read the decoded rows as ingest does.
    retract_entities(nest.entities, &rows, ancestor).map_err(torn)?;
    nest.balances.apply(retraction_batch(&rows));
    nest.balances
        .retract_over_i128(rows.iter().filter(|r| oversized_transfer(r)).count() as u64);
    nest.exposure
        .apply(exposure_retraction_batch(&rows, nest.labels));
    if let Some(w) = nest.velocity_window {
        nest.velocity.apply(velocity_retraction_batch(&rows, w));
    }
    // Drop children whose announcing factory event was rolled back (RFC-0009): the registry state
    // at B is a pure fold over factory events ≤ B.
    if let Some(children) = nest.children {
        let dropped = children.rollback_to(ancestor);
        if dropped > 0 {
            tracing::warn!("reorg: dropped {dropped} discovered child contract(s)");
        }
    }
    // Fire a `flag_retracted` alert for every rolled-back annotation a sink watches - a consumer
    // that acted on a flag learns the chain took it back (RFC-0008 C5). Queued by the rollback's
    // own transaction, so a rollback that fails retracts nothing (#2177).
    let mut outbox = Vec::new();
    if !nest.router.is_empty() {
        for j in &doomed {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(j) {
                if let Some(kind) = v.get("kind").and_then(|k| k.as_str()) {
                    outbox.extend(alerts::payloads(nest.router, "flag_retracted", kind, &v));
                }
            }
        }
    }

    // Roll the hot store back to the ancestor AND reset the `last_block` watermark in ONE txn: a
    // crash between a separate delete and watermark reset would leave `last_block` past the fork and
    // the rolled-back range permanently un-reindexed (a silent gap).
    let removed = store
        .rollback_to_and_set_meta(ancestor, LAST_BLOCK_KEY, &ancestor.to_string(), &outbox)
        .map_err(torn)?;
    if !outbox.is_empty() {
        if let Err(e) = alerts::trim(store) {
            tracing::warn!("could not bound the alert outbox: {e:#}");
        }
    }

    nest.metrics.record_reorg(last_indexed - ancestor);
    nest.metrics.set_last_block(ancestor);
    tracing::warn!("reorg detected: rolled back to block {ancestor} (removed {removed} entities)");
    Ok(ancestor + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drive::Driver;
    use crate::entity_view::Batch;
    use crate::fault::is_terminal;
    use crate::metrics::NoMetrics;
    use crate::store::Lease;
    use crate::views::transfer_deltas;
    use dbsp::ZWeight;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    const ALICE: &str = "0x1111111111111111111111111111111111111111";
    const BOB: &str = "0x2222222222222222222222222222222222222222";
    const ZERO: &str = "0x0000000000000000000000000000000000000000";

    /// Only what a rollback reads and writes; the rest is never reached.
    #[derive(Default)]
    struct Mem {
        rows: Mutex<BTreeMap<(u64, u64), String>>,
        meta: Mutex<HashMap<String, String>>,
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
        fn get_entity(&self, _: &str) -> Result<Option<String>> {
            unreachable!()
        }
        fn count(&self) -> Result<u64> {
            unreachable!()
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
        ) -> Result<crate::store::HotRowsSnapshot> {
            unreachable!()
        }
        fn sample_entity_keys(&self, _: usize) -> Result<Vec<String>> {
            unreachable!()
        }
        fn commit_window(
            &self,
            _: &[(String, String)],
            _: Option<(u64, &str)>,
            _: u64,
        ) -> Result<()> {
            unreachable!()
        }
        fn set_meta(&self, _: &str, _: &str) -> Result<()> {
            unreachable!()
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
            unreachable!()
        }
        fn set_block_hash(&self, _: u64, _: &str) -> Result<()> {
            unreachable!()
        }
        fn get_block_hash(&self, _: u64) -> Result<Option<String>> {
            unreachable!()
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
            _: Vec<(String, String)>,
            _: Vec<String>,
            _: Option<(u64, String)>,
            _: u64,
        ) -> Result<()> {
            unreachable!()
        }
        async fn settle_commits(&self) {}
        fn rollback_to(&self, _: u64) -> Result<u64> {
            unreachable!()
        }
        fn prune_range(&self, _: u64, _: u64) -> Result<u64> {
            unreachable!()
        }
        fn prune_and_set_meta(&self, _: u64, _: u64, _: &str, _: &str) -> Result<u64> {
            unreachable!()
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

    /// Records each window an entity is fed, as (rows, weight, through).
    #[derive(Default)]
    struct Recorder(Mutex<Vec<(usize, ZWeight, u64)>>);

    impl WindowEntity for Recorder {
        fn name(&self) -> &str {
            "recorder"
        }
        fn bind_window(&self, _: &[DecodedRow], _: ZWeight) -> Result<Option<Batch>> {
            unreachable!("a rollback feeds through apply_window")
        }
        fn apply(&self, _: Batch, _: u64) {}
        fn apply_window(&self, rows: &[DecodedRow], weight: ZWeight, through: u64) -> Result<()> {
            self.0.lock().unwrap().push((rows.len(), weight, through));
            Ok(())
        }
        fn fault(&self) -> Option<String> {
            None
        }
        fn applied_through(&self) -> u64 {
            0
        }
        fn flush(&self) {}
    }

    fn schema() -> Vec<TableSchema> {
        let implicit = |name: &str, storage: &str| serde_json::json!({"name": name, "sol_type": "implicit", "storage": storage, "indexed": false});
        let param = |name: &str, sol: &str, storage: &str| serde_json::json!({"name": name, "sol_type": sol, "storage": storage, "indexed": false});
        serde_json::from_value(serde_json::json!([{
            "table": "tok__transfer",
            "alias": "tok",
            "columns": [
                implicit("block_number", "u64"),
                implicit("block_hash", "bytes32"),
                implicit("tx_hash", "bytes32"),
                implicit("log_index", "u64"),
                implicit("address", "address"),
                param("from", "address", "address"),
                param("to", "address", "address"),
                param("value", "uint256", "word32"),
            ],
        }]))
        .unwrap()
    }

    fn transfer(block: u64, from: &str, to: &str, value: u128) -> String {
        serde_json::json!({
            "table": "tok__transfer",
            "block_number": block,
            "block_hash": format!("0x{block:064x}"),
            "tx_hash": format!("0x{block:064x}"),
            "log_index": 0,
            "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "from": from,
            "to": to,
            "value": value.to_string(),
        })
        .to_string()
    }

    struct Fixture {
        store: Mem,
        schema: Vec<TableSchema>,
        balances: BalanceView,
        exposure: ExposureView,
        velocity: VelocityView,
        labels: LabelSet,
        entities: Vec<Recorder>,
        children: ChildRegistry,
        router: AlertRouter,
    }

    impl Fixture {
        /// A mint to Alice at block 10 and a transfer to Bob at block 12, both folded and stored.
        fn new() -> Fixture {
            let f = Fixture {
                store: Mem::default(),
                schema: schema(),
                balances: BalanceView::start(Driver::Inline).unwrap(),
                exposure: ExposureView::start(Driver::Inline, false).unwrap(),
                velocity: VelocityView::start(Driver::Inline, false).unwrap(),
                labels: LabelSet::default(),
                entities: vec![Recorder::default()],
                children: ChildRegistry::new(),
                router: AlertRouter::new(Vec::<alerts::AlertSink>::new()),
            };
            for (block, from, to, value) in [(10, ZERO, ALICE, 100), (12, ALICE, BOB, 40)] {
                f.store.put(block, 0, transfer(block, from, to, value));
                f.balances
                    .apply(transfer_deltas(from, to, value as i128, 1));
            }
            f.store.set(START_BLOCK_KEY, 1);
            f.store.set(LAST_BLOCK_KEY, 12);
            f
        }

        fn rollback(&mut self, ancestor: u64) -> Result<u64> {
            rollback_reorg(
                Nest {
                    store: &self.store,
                    schema: &self.schema,
                    balances: &self.balances,
                    exposure: &self.exposure,
                    velocity: &self.velocity,
                    labels: &self.labels,
                    velocity_window: None,
                    entities: &self.entities,
                    children: Some(&mut self.children),
                    router: &self.router,
                    metrics: &NoMetrics,
                },
                ancestor,
            )
        }
    }

    #[test]
    fn a_rollback_retracts_what_the_fork_took_and_rewinds_the_cursor() {
        let mut f = Fixture::new();
        assert_eq!(f.rollback(11).unwrap(), 12);

        f.balances.flush();
        assert_eq!(f.balances.balance(ALICE), Some(100));
        assert_eq!(f.balances.balance(BOB).unwrap_or(0), 0);
        assert_eq!(f.store.entities_in_range(0, u64::MAX).unwrap().len(), 1);
        assert_eq!(
            f.store.get_meta(LAST_BLOCK_KEY).unwrap().as_deref(),
            Some("11")
        );
        assert_eq!(*f.entities[0].0.lock().unwrap(), vec![(1, -1, 11)]);
    }

    #[test]
    fn a_nest_that_never_reached_the_fork_is_left_alone() {
        let mut f = Fixture::new();
        assert_eq!(f.rollback(12).unwrap(), 13);
        assert_eq!(f.store.entities_in_range(0, u64::MAX).unwrap().len(), 2);
        assert_eq!(
            f.store.get_meta(LAST_BLOCK_KEY).unwrap().as_deref(),
            Some("12")
        );
        assert!(f.entities[0].0.lock().unwrap().is_empty());
    }

    #[test]
    fn a_fork_below_the_sealed_watermark_is_terminal_and_touches_nothing() {
        let mut f = Fixture::new();
        f.store.set(SEALED_THROUGH_KEY, 11);
        let e = f.rollback(10).unwrap_err();

        assert!(is_terminal(&e), "{e:#}");
        assert!(format!("{e:#}").contains("a reorg to block 10"), "{e:#}");
        f.balances.flush();
        assert_eq!(f.balances.balance(BOB), Some(40));
        assert_eq!(f.store.entities_in_range(0, u64::MAX).unwrap().len(), 2);
        assert!(f.entities[0].0.lock().unwrap().is_empty());
    }
}
