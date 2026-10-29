//! A nest's cursor steps over its hot store and the state derived from it, borrowed from whoever
//! owns them: the node's `NestIngest`, or a tab's worker.

use crate::alerts::{self, AlertRouter};
use crate::entity_view::WindowEntity;
use crate::exposure::{self, ExposureView};
use crate::extra::ExtraRows;
use crate::factory::{ChildRegistry, FactorySet};
use crate::fault::{FaultPoints, TerminalFault};
use crate::feed::{
    decode_stored_rows, exposure_retraction_batch, oversized_transfer, retraction_batch,
    velocity_retraction_batch,
};
use crate::flags;
use crate::hooks::WindowHooks;
use crate::labels::LabelSet;
use crate::metrics::CursorMetrics;
use crate::seal::{maybe_seal, Sinks, TipSeal};
use crate::source::{Log, Source};
use crate::store::{
    encode_block_record, entity_key, HotStore, LAST_BLOCK_KEY, SEALED_THROUGH_KEY, START_BLOCK_KEY,
};
use crate::time::{Clock, Duration};
use crate::velocity::{self, VelocityView};
use crate::views::{self, BalanceView};
use crate::window::{
    apply_row_timestamps, blocks_needing_timestamps, decode_window, fetch_window_block_data,
    l1_block_rows, note_undecoded, seal_ceiling, Finality, WindowForked, REPEAT_WARN_EVERY,
};
use anyhow::{Context, Result};
use nuthatch_decode::registry::{block_row, DecodeRegistry, DecodedRow, TableSchema};
use std::collections::HashMap;

/// One nest, borrowed for one step of its cursor.
pub struct Nest<'a, E> {
    /// The label a quarantine is reported under.
    pub name: &'a str,
    pub store: &'a dyn HotStore,
    pub schema: &'a [TableSchema],
    pub balances: &'a BalanceView,
    pub exposure: &'a ExposureView,
    pub velocity: &'a VelocityView,
    pub labels: &'a LabelSet,
    /// The velocity flag's threshold and window in blocks, when the nest flags velocity.
    pub velocity_flag: Option<(i128, u64)>,
    pub entities: &'a [E],
    /// The discovered children, when the nest declares a factory.
    pub children: Option<&'a mut ChildRegistry>,
    pub router: &'a AlertRouter,
    pub metrics: &'a dyn CursorMetrics,
}

impl<E: WindowEntity> Nest<'_, E> {
    /// The same nest, borrowed again for a step inside this one.
    pub fn reborrow(&mut self) -> Nest<'_, E> {
        Nest {
            name: self.name,
            store: self.store,
            schema: self.schema,
            balances: self.balances,
            exposure: self.exposure,
            velocity: self.velocity,
            labels: self.labels,
            velocity_flag: self.velocity_flag,
            entities: self.entities,
            children: self.children.as_deref_mut(),
            router: self.router,
            metrics: self.metrics,
        }
    }

    /// Fail loudly if any derived-view circuit thread has died. A dead IVM thread silently freezes the
    /// balance/exposure/velocity views while ingest keeps committing and serving - so a dead thread must
    /// surface as a fatal ingest error, never be served over (audit: correctness M1). A disabled view
    /// (no labels / no velocity flag) reports healthy, so this only bites a genuinely crashed circuit.
    pub fn ensure_views_healthy(&self) -> Result<()> {
        // Terminal (RFC-0026 §3): a crashed circuit thread cannot be revived in-process, so this nest
        // is quarantined until restart rather than retried.
        if !self.balances.is_healthy() {
            anyhow::bail!(TerminalFault(
                "the balance IVM circuit thread has died - refusing to serve a frozen view".into()
            ));
        }
        if !self.exposure.is_healthy() {
            anyhow::bail!(TerminalFault("the exposure IVM circuit thread has died - refusing to serve frozen compliance data".into()));
        }
        if !self.velocity.is_healthy() {
            anyhow::bail!(TerminalFault("the velocity IVM circuit thread has died - refusing to serve frozen compliance data".into()));
        }
        // §5.2: "Serving frozen derived state as healthy is not graceful degradation; it is a lie
        // with a pleasant HTTP status." The entity carries *why* it stopped, so the quarantine
        // reason names the cause rather than sending whoever is on call to the logs of a process
        // that may since have restarted.
        for entity in self.entities {
            if let Some(why) = entity.fault() {
                // Push it before bailing. A fault here is terminal and quarantines the nest, so the
                // operator finds out from `/ready` - if they are looking. `nuthatch_entity_faulted`
                // is the series to build a dashboard on; this is the one that arrives unasked.
                //
                // Best-effort on purpose: a webhook that will not enqueue must not change what
                // happens to the nest, and what happens to the nest is that it stops either way.
                if self.router.watches("entity_fault") {
                    let ann = serde_json::json!({
                        "nest": self.name,
                        "entity": entity.name(),
                        "reason": why,
                        "applied_through": entity.applied_through(),
                    });
                    if let Err(e) = alerts::enqueue(
                        self.store,
                        self.router,
                        &format!("entity `{}` stopped", entity.name()),
                        "entity_fault",
                        &ann,
                    ) {
                        tracing::warn!("could not enqueue the entity_fault alert: {e:#}");
                    }
                }
                anyhow::bail!(TerminalFault(format!(
                    "the circuit for entity `{}` has stopped: {why}",
                    entity.name()
                )));
            }
        }
        Ok(())
    }
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

/// If the checkpoint at `last` is no longer canonical, return the deepest checkpoint that still
/// is (the common ancestor to roll back to); otherwise None. Returns Some(0) if none survive.
/// Errs when the source cannot answer: unverified is not canonical, and nothing may commit on it.
pub async fn detect_reorg(
    source: &dyn Source,
    store: &dyn HotStore,
    last: u64,
) -> Result<Option<u64>> {
    // Usually `last` itself, but if that boundary's hash couldn't be stored (a transient block_hash
    // failure at checkpoint time), fall back to the newest checkpoint we *do* have at/below `last`, so
    // a reorg is still verified against a real checkpoint instead of giving up entirely - the previous
    // "no hash here → nothing to verify" was a reorg blind spot (deadlock-review finding M7).
    let (checkpoint, stored) = match store.get_block_hash(last)? {
        Some(h) => (last, h),
        None => match store
            .checkpoints_desc()?
            .into_iter()
            .find(|(b, _)| *b <= last)
        {
            Some((b, h)) => (b, h),
            None => return Ok(None), // genuinely no checkpoint yet (cold start)
        },
    };
    let Some(canonical) = source.block_hash(checkpoint).await? else {
        anyhow::bail!("the source has no hash for checkpoint {checkpoint}");
    };
    if stored.eq_ignore_ascii_case(&canonical) {
        return Ok(None);
    }
    for (block, hash) in store.checkpoints_desc()? {
        if block >= checkpoint {
            continue;
        }
        // An unanswered checkpoint may be the ancestor or the seal pin, so walking past it can land
        // below the watermark, or at 0 if none answers (#1668). A lagging replica clears by the next poll.
        let Some(canon) = source.block_hash(block).await? else {
            anyhow::bail!(
                "block {checkpoint} no longer matches its checkpoint, but the source has no hash for \
                 checkpoint {block}; not rolling back until it answers"
            );
        };
        if canon.eq_ignore_ascii_case(&hash) {
            return Ok(Some(block));
        }
    }
    // No checkpoint we hold is canonical, so the fork is deeper than our entire recorded history.
    // Rolling back everything is the CORRECT recovery here: re-indexing from the nest's origin
    // reconverges on the canonical chain (`e2e_reorg::reorg_converges_to_canonical` asserts exactly
    // this for a fork below the oldest checkpoint).
    //
    // It looks identical to a wrong-network endpoint (issue #150) - both make every stored hash
    // mismatch - and block hashes alone cannot tell them apart. So the wrong-chain case is caught
    // *upstream*, by `RpcClient::verify_chain_ids` at startup, and the established-nest case is caught
    // *downstream*, by `rollback_reorg`'s sealed-watermark bail. Refusing here instead would break the
    // legitimate deep-reorg recovery, which is a real correctness regression rather than a guard.
    tracing::warn!(
        "no checkpoint at or below block {checkpoint} is canonical - rolling back the whole hot store \
         and re-indexing from origin. If this repeats, check every url in `rpc_urls` is on this \
         nest's chain (a wrong-network endpoint looks exactly like this)."
    );
    Ok(Some(0))
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
    if let Some((_, w)) = nest.velocity_flag {
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

/// One nest, borrowed for one window: a [`Nest`] and what only a window reads.
pub struct Window<'a, E> {
    pub nest: Nest<'a, E>,
    pub registry: &'a DecodeRegistry,
    /// Present exactly when `nest.children` is.
    pub factory: Option<&'a FactorySet>,
    /// The threshold flag's amount, when the nest flags single transfers.
    pub threshold: Option<i128>,
    pub finality: Finality,
    /// The chain's `seal_span` (#1199).
    pub seal_span: u64,
    pub sinks: &'a dyn Sinks,
    pub extra: &'a dyn ExtraRows,
    pub hooks: &'a dyn WindowHooks,
    pub faults: &'a dyn FaultPoints,
    pub clock: &'a dyn Clock,
    /// How long the part of the window before its commit may take (RFC-0076 §3.2). Past it the
    /// window is abandoned with nothing stored or fed; the commit, once begun, is never cut short.
    pub deadline: Option<Duration>,
}

/// What became of a window.
#[derive(Debug, PartialEq, Eq)]
pub enum Processed {
    /// Committed, with this many rows; the cursor advances past it.
    Stored(usize),
    /// Refused; the cursor stays put and the window is read again.
    Refused,
    /// A stored row was off the chain, and the nest rolled back: the cursor resumes here.
    Rewound(u64),
    /// The part before the commit outran [`Window::deadline`]; nothing was stored or fed.
    Abandoned,
}

/// What the part of a window before its commit ends in: an answer already, or what the commit takes.
enum Step<T> {
    Done(Processed),
    Ready(T),
}

/// `work`, or `None` once `sleep` finishes first. Dropping `work` there is the abandonment, so it
/// must hold nothing a retry would see.
async fn within<F: std::future::Future>(
    work: F,
    mut sleep: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> Option<F::Output> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(move |cx| {
        if let std::task::Poll::Ready(v) = work.as_mut().poll(cx) {
            return std::task::Poll::Ready(Some(v));
        }
        if sleep.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(None);
        }
        std::task::Poll::Pending
    })
    .await
}

/// Refuse a window for a retry. What it read may be from a chain that has moved since, and the
/// timestamp cache is keyed by height, so it forgets from where the window began reading (#2152).
async fn refuse(source: &dyn Source, clock: &dyn Clock, read_from: u64) -> Result<Processed> {
    source.forget_cached_above(read_from.saturating_sub(1));
    clock.sleep(Duration::from_secs(2)).await;
    Ok(Processed::Refused)
}

/// Decode, store, IVM-feed, checkpoint, seal and deliver webhooks for one fetched window
/// `[next, to]` (with `tip` the current chain tip, used for the finality ceiling). A refused window
/// must be retried WITHOUT advancing (the cursor stays put so a freshly-finalized window never seals
/// `block_timestamp = 0`, deadlock-review H4).
pub async fn process_window<E: WindowEntity>(
    w: Window<'_, E>,
    source: &dyn Source,
    logs: &[Log],
    next: u64,
    to: u64,
    tip: u64,
) -> Result<Processed> {
    let Window {
        mut nest,
        registry,
        factory,
        threshold,
        finality,
        seal_span,
        sinks,
        extra,
        hooks,
        faults,
        clock,
        deadline,
    } = w;
    let store = nest.store;
    let metrics = nest.metrics;
    faults.window_started();
    let prepare = async {
        hooks.started().await?;
        // Decode first so factory discovery is inline (a child created at log i is in the
        // registry before its own activity at log j>i - RFC-0009 same-block handling), then
        // buy headers only for blocks that produced a kept row (#765). A topic0-only fetch
        // returns every matching event on the chain; stamping from the raw log list paid
        // eth_getBlockByNumber for foreign protocols we then discarded.
        let empty_ts = HashMap::new();
        let no_children = ChildRegistry::new();
        let (mut rows, mut found, undecoded) = decode_window(
            registry,
            factory,
            nest.children.as_deref().unwrap_or(&no_children),
            logs,
            &empty_ts,
        );
        // Rows below the cursor are the refetched tail (#1144). Any the store already holds have
        // already been folded into every view and entity; folding them again would double-count, so
        // they are dropped here, before the fold, and only the rows the first answer missed go on.
        // Checked only below `next`, so the ordinary window costs no store reads.
        //
        // "Already holds" means the same row, not merely the same key: the stored row's `block_hash`
        // must match. The loop runs `handle_reorg` before every fetch, so a reorged tail block has
        // been rolled back before its refetch arrives and this never fires in practice - but if it
        // ever does, the stale row must not be mistaken for the current one and silently kept. The
        // window is refused instead and asked for again on the next poll, by which time the reorg
        // handler has run over it.
        // A window that committed and then failed is asked for again, and not always with the same
        // end: the ceiling has moved by the time a quarantined nest is re-admitted. So its rows take
        // the same check as the tail, row by row, up to what the store has committed (#1635).
        let committed_through = store
            .get_meta(LAST_BLOCK_KEY)?
            .and_then(|v| v.parse::<u64>().ok());
        let fold_floor = next.max(committed_through.map_or(0, |b| b + 1));
        // A sealed block's rows were folded when they first arrived and are pruned from hot since, so
        // the lookup below would find nothing and fold them a second time.
        let sealed_through = store.sealed_through();
        let mut stale_tail: Option<(u64, u64, Option<String>)> = None;
        rows.retain(|r| {
            if r.block_number >= fold_floor {
                return true;
            }
            if sealed_through > 0 && r.block_number <= sealed_through {
                return false;
            }
            match store
                .get_entity(&entity_key(r.block_number, r.log_index))
                .ok()
                .flatten()
            {
                None => true,
                Some(stored) => {
                    let stored_hash = serde_json::from_str::<serde_json::Value>(&stored)
                        .ok()
                        .and_then(|v| {
                            v.get("block_hash")
                                .and_then(|h| h.as_str())
                                .map(str::to_string)
                        });
                    if !stored_hash
                        .as_deref()
                        .is_some_and(|h| h.eq_ignore_ascii_case(&r.block_hash))
                    {
                        stale_tail.get_or_insert((r.block_number, r.log_index, stored_hash));
                    }
                    false
                }
            }
        });
        let read_from = logs.iter().map(|l| l.block_number).fold(next, u64::min);
        if let Some((b, i, stored_hash)) = stale_tail {
            // The reorg check passed, so the checkpoint above this row matches. A row the chain no
            // longer has under it is one no later check would ever roll back (#2146).
            if let Ok(Some(now)) = source.block_hash(b).await {
                if !stored_hash.is_some_and(|h| h.eq_ignore_ascii_case(&now)) {
                    let ancestor = b.saturating_sub(1);
                    tracing::warn!(
                        "stored row ({b}, {i}) is not on the chain the source has now, though the \
                     checkpoint above it is - rolling back to block {ancestor}"
                    );
                    source.forget_cached_above(ancestor);
                    return Ok(Step::Done(Processed::Rewound(rollback_reorg(
                        nest.reborrow(),
                        ancestor,
                    )?)));
                }
            }
            tracing::warn!(
                "refetched tail row ({b}, {i}) is already stored under a different block hash: a \
             reorg the handler has not yet rolled back - refusing this window, retrying"
            );
            return refuse(source, clock, read_from).await.map(Step::Done);
        }
        // RFC-0023 tier 3 samples blocks that may have emitted no log at all, and a stored row
        // with `block_timestamp = 0` seals that zero permanently once it finalizes (H4). Sampled
        // blocks join the timestamp fetch rather than being handled after it.
        let blocks = blocks_needing_timestamps(&rows, extra.sampled_blocks(next, to));
        let block_data =
            match fetch_window_block_data(source, registry, &blocks, extra.keeps_headers()).await {
                Ok(t) => t,
                Err(e) => {
                    // Don't store this window with zeroed timestamps - once it finalizes it would
                    // seal `block_timestamp = 0` permanently (deadlock-review finding H4). The
                    // cursor hasn't advanced, so skip and re-fetch the same window next poll.
                    tracing::warn!(
                        "block timestamps unavailable for {next}..={to}: {e:#} - retrying window"
                    );
                    return refuse(source, clock, read_from).await.map(Step::Done);
                }
            };
        let l1_rows = match l1_block_rows(registry, &rows, &block_data) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("{e:#} - retrying window {next}..={to}");
                return refuse(source, clock, read_from).await.map(Step::Done);
            }
        };
        let timestamps = &block_data.timestamps;
        apply_row_timestamps(&mut rows, timestamps);
        found.apply_timestamps(timestamps);
        // The logs and this hash are two calls. A tip that moved between them would commit the
        // fork's rows under the hash the chain has now, and the next reorg check would see them agree.
        // When no kept row stamped `to`, its hash and timestamp come from one header (#1494).
        let record = match timestamps.get(&to).copied() {
            Some(t) => source.block_hash(to).await.map(|h| h.map(|h| (h, Some(t)))),
            None => source.block_record(to).await,
        };
        let checkpoint = match &record {
            Ok(Some((hash, ts))) => Some((to, encode_block_record(hash, *ts))),
            _ => None,
        };
        let checkpoint_missed = match &record {
            Ok(Some(_)) => None,
            Ok(None) => Some("no block at that height".to_string()),
            Err(e) => Some(format!("{e:#}")),
        };
        if let Some((hash, _)) = record.as_ref().ok().and_then(|r| r.as_ref()) {
            if rows.iter().any(|r| {
                r.block_number == to
                    && !r.block_hash.is_empty()
                    && !r.block_hash.eq_ignore_ascii_case(hash)
            }) {
                tracing::warn!(
                "block {to} moved between the log fetch and the checkpoint: refusing this window, retrying"
            );
                return refuse(source, clock, read_from).await.map(Step::Done);
            }
        }
        // Below the fallback finality depth no reorg can reach a block, so only rows above it need
        // checking, and only a window ending above it needs a checkpoint to be checked against later.
        let final_through = seal_ceiling(finality, tip, None);
        if let (Some(why), true) = (&checkpoint_missed, to > final_through) {
            let n = metrics.inc_checkpoints_missed();
            if n == 1 || n.is_multiple_of(REPEAT_WARN_EVERY) {
                tracing::warn!(
                "blocks {next}..={to} refused: the source gave no hash for block {to} ({why}), \
                 and a window is not committed without a reorg checkpoint. {n} window(s) so far \
                 (nuthatch_nest_checkpoints_missed_total)."
            );
            }
            return refuse(source, clock, read_from).await.map(Step::Done);
        }
        // Every row read before the checkpoint must be on the chain it names, and the window must
        // extend the stored chain. Hashes read after the checkpoint settle both (#2146).
        let exposed: Vec<_> = rows
            .iter()
            .filter(|r| r.block_number > final_through)
            .collect();
        // A row with no hash cannot be checked, so it cannot be committed where a reorg can reach.
        if let Some(r) = exposed.iter().find(|r| r.block_hash.is_empty()) {
            tracing::warn!(
            "the source gave the log at ({}, {}) no block hash: refusing window {next}..={to}, \
             retrying",
            r.block_number,
            r.log_index
        );
            return refuse(source, clock, read_from).await.map(Step::Done);
        }
        let mut expected: Vec<(u64, String)> = exposed
            .iter()
            .map(|r| (r.block_number, r.block_hash.clone()))
            .collect();
        if let Some(parent) = next.checked_sub(1).filter(|&p| p > final_through) {
            if let Some(h) = store.get_block_hash(parent)? {
                expected.push((parent, h));
            }
        }
        if !expected.is_empty() {
            let mut blocks: Vec<u64> = expected.iter().map(|(b, _)| *b).collect();
            blocks.sort_unstable();
            blocks.dedup();
            let moved = match source.block_hashes(&blocks).await {
                Ok(now) => expected
                    .iter()
                    .find(|(b, h)| !now.get(b).is_some_and(|n| n.eq_ignore_ascii_case(h)))
                    .map(|(b, _)| format!("block {b} is not on the chain the source has now")),
                Err(e) => Some(format!("block hashes unavailable: {e:#}")),
            };
            if let Some(why) = moved {
                tracing::warn!("{why}: refusing window {next}..={to}, retrying");
                return refuse(source, clock, read_from).await.map(Step::Done);
            }
        }

        let mut stored = 0usize;
        let mut deltas = Vec::new();
        let mut exp_deltas = Vec::new();
        let mut vel_deltas = Vec::new();
        let mut oversized = 0u64;
        // PERF-2: accumulate every write and commit the whole window in ONE redb txn at the end,
        // instead of a `begin_write`/`commit` (fsync) per row. `(key, json)` for rows + annotations.
        let mut to_store: Vec<(String, String)> = Vec::with_capacity(rows.len());
        // Alerts commit with the window, so a refused or failed window queues none (#2177).
        let mut outbox: Vec<String> = Vec::new();
        for row in &mut rows {
            let key = entity_key(row.block_number, row.log_index);
            // Feed the IVM balance + exposure views for transfer rows (extracted before storing).
            if let Some((from, to_addr, value, _hex)) = row.erc20_transfer_fields() {
                if let Some(v) = value.as_deref().and_then(views::transfer_value) {
                    deltas.extend(views::transfer_deltas(&from, &to_addr, v, 1));
                    // Direct exposure to the labeled set (empty when neither side is labeled).
                    exp_deltas.extend(exposure::exposure_deltas(
                        &from,
                        &to_addr,
                        v,
                        1,
                        nest.labels,
                    ));
                    // Velocity: the sender's outbound volume in this block's window (C3).
                    if let Some((_, w)) = nest.velocity_flag {
                        vel_deltas.extend(velocity::velocity_deltas(
                            &from,
                            row.block_number,
                            v,
                            1,
                            w,
                        ));
                    }
                    // Threshold flag: a single transfer at/above the configured amount (C3).
                    if let Some(t) = threshold {
                        if let Some((fkey, ann)) = flags::threshold_annotation(
                            &from,
                            &to_addr,
                            v,
                            row.block_number,
                            row.log_index,
                            &row.tx_hash,
                            t,
                        ) {
                            to_store.push((fkey, ann.to_string()));
                            outbox.extend(alerts::payloads(
                                nest.router,
                                "flag",
                                "threshold_flag",
                                &ann,
                            ));
                        }
                    }
                } else if value.is_some() {
                    // COR-8 (#814): a live transfer whose value exceeds 38 digits. Skipped here exactly
                    // as the cold fold skips it, so hot and sealed agree - but counted now, so the
                    // balance says it is incomplete instead of looking complete. Noted once the
                    // window commits, as a reorg retracts it from the stored row.
                    oversized += 1;
                    tracing::warn!(
                        block = row.block_number,
                        log_index = row.log_index,
                        "transfer value has more than 38 digits; excluded from balances (#814)"
                    );
                }
            }
            // Every row is stored uniformly as typed JSON with a `table` field; per-table
            // sealing groups by it.
            to_store.push((key, row.to_json().to_string()));
            stored += 1;
        }
        for r in &l1_rows {
            to_store.push((
                entity_key(r.block_number, r.log_index),
                r.to_json().to_string(),
            ));
            stored += 1;
        }

        // RFC-0036 §4.2: one row per block in the window for a blocks nest - same logic as the
        // backfill paths. Must enumerate from the window [next..=to], not from `logs`, because
        // a blocks table covers blocks that emitted no matching log at all (#447).
        let mut block_rows = Vec::new();
        if registry.blocks() {
            let want: Vec<u64> = (next..=to).collect();
            // A block without its row would seal as a block the chain never had (#2149).
            let headers = match source.block_headers(&want).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(
                        "block_headers unavailable for {next}..={to}: {e:#} - retrying window"
                    );
                    return refuse(source, clock, read_from).await.map(Step::Done);
                }
            };
            for b in &want {
                let Some(r) = headers
                    .get(b)
                    .and_then(|h| block_row(*b, h, registry.timestamps()))
                else {
                    tracing::warn!(
                        "block {b} has no usable header - retrying window {next}..={to}"
                    );
                    return refuse(source, clock, read_from).await.map(Step::Done);
                };
                to_store.push((
                    entity_key(r.block_number, r.log_index),
                    r.to_json().to_string(),
                ));
                stored += 1;
                block_rows.push(r);
            }
        }

        // A derived-view circuit thread that has died silently drops its batches and freezes
        // `/balances` + the compliance flags while ingest keeps committing - stale data served as
        // healthy. Surface it before the commit (the dead-task-must-surface rule, extended to the
        // IVM threads that were previously exempt).
        nest.ensure_views_healthy()?;

        // Decoded before IPFS resolution, so an `[[ipfs]]` declaration can name a call table: the QoS
        // oracle's CIDs arrive as calldata, not as logs.
        let call_rows = extra.top_level_calls(source, next, to).await?;
        for row in &call_rows {
            to_store.push((
                entity_key(row.block_number, row.log_index),
                row.to_json().to_string(),
            ));
            stored += 1;
        }

        // RFC-0037 §3: the documents these rows name are fetched out of band, never here, and sealing
        // below waits for them. What the tip path still owns is saying so when a row names nothing that
        // could be resolved.
        let named: Vec<&DecodedRow> = rows.iter().chain(&call_rows).collect();
        let unreadable = extra.unreadable(&named);
        if unreadable > 0 {
            metrics.add_ipfs_unreadable(unreadable as u64);
            tracing::warn!(
                "ipfs: {unreadable} row(s) in {next}..={to} named no usable CID - nothing to \
             resolve for them (nuthatch_nest_ipfs_unreadable_total)"
            );
        }

        // RFC-0023 tier 3 + RFC-0038 §3: resolve the declared reads this window asks for.
        //
        // Two forms. A **sampled** declaration fires at the blocks `blocks_in` yields. A **row-driven**
        // one fires once per row of the table it names, which is what a subgraph mapping does - the
        // call happens as the row is produced, the same moment a handler would have made it.
        //
        // Ordering is identity, not presentation: declarations in config order, and within a
        // row-driven declaration its source rows in `log_index` order. Two operators running the same
        // nest must produce the same keys, not merely the same content addresses.
        let read_rows = extra
            .pinned_reads(
                source,
                &rows,
                next,
                to,
                timestamps,
                block_data.headers.as_ref(),
            )
            .await;
        let read_rows = match read_rows {
            Ok(r) => r,
            Err(e) if e.chain().any(|c| c.is::<WindowForked>()) => {
                tracing::warn!("{e:#}: refusing window {next}..={to}, retrying");
                return refuse(source, clock, read_from).await.map(Step::Done);
            }
            Err(e) => return Err(e),
        };
        for row in read_rows {
            to_store.push((
                entity_key(row.block_number, row.log_index),
                row.to_json().to_string(),
            ));
            stored += 1;
        }

        // Bound before the commit and fed after it (#1906). An entity's watermark claims its blocks
        // are in the store, and `/sql` over the store is what a reader reconciles it against.
        // Every registry row this window stores, since the seed and a reorg read all of them (#2166).
        rows.extend(l1_rows);
        rows.extend(block_rows);
        let mut bound = Vec::with_capacity(nest.entities.len());
        for entity in nest.entities {
            bound.push(
                entity.bind_window(&rows, 1).with_context(|| {
                    format!("feeding this window to entity `{}`", entity.name())
                })?,
            );
        }
        Ok(Step::Ready((
            stored, to_store, outbox, checkpoint, deltas, exp_deltas, vel_deltas, oversized, found,
            bound, undecoded, fold_floor,
        )))
    };
    let step = match deadline {
        None => prepare.await?,
        Some(d) => match within(prepare, clock.sleep(d)).await {
            Some(step) => step?,
            None => return Ok(Processed::Abandoned),
        },
    };
    let (
        stored,
        mut to_store,
        outbox,
        checkpoint,
        deltas,
        exp_deltas,
        vel_deltas,
        oversized,
        found,
        bound,
        undecoded,
        fold_floor,
    ) = match step {
        Step::Done(p) => return Ok(p),
        Step::Ready(r) => r,
    };
    // Before the commit. A failure retracts what this attempt fed, so a retry folds the
    // window once. A restart still rebuilds these views from the store.
    nest.balances.apply(deltas.clone());
    nest.exposure.apply(exp_deltas.clone());
    nest.velocity.apply(vel_deltas.clone());
    // Undo the views this attempt just fed. The commit did not land, so a retry starts from here.
    // Entities are absent because they are fed only after the commit.
    let retract = |deltas, exp_deltas, vel_deltas| {
        nest.balances.retract(deltas);
        nest.exposure.retract(exp_deltas);
        nest.velocity.retract(vel_deltas);
    };
    if let Err(e) = faults.folded() {
        retract(deltas, exp_deltas, vel_deltas);
        return Err(e);
    }
    faults.committing();
    hooks.seam().await;
    let alerted = !outbox.is_empty();
    extra.committing(next);
    if let Err(e) = store
        .commit_window_blocking(std::mem::take(&mut to_store), outbox, checkpoint, to)
        .await
    {
        retract(deltas, exp_deltas, vel_deltas);
        return Err(e);
    }
    hooks.seam().await;
    nest.balances.note_over_i128(oversized);
    // Only now: a refused or failed window must not leave its children routing later logs (#2153).
    if let Some(children) = nest.children.as_deref_mut() {
        children.extend(found);
    }
    if alerted {
        if let Err(e) = alerts::trim(store) {
            tracing::warn!("could not bound the alert outbox: {e:#}");
        }
    }
    for (entity, batch) in nest.entities.iter().zip(bound) {
        if let Some(batch) = batch {
            entity.apply(batch, to);
        }
    }
    hooks.committed(to, tip).await?;
    metrics.set_last_block(to);
    metrics.add_rows_decoded(stored as u64);
    // Counted once committed, so a refused window asked for again is not counted twice.
    note_undecoded(Some(metrics), &undecoded, fold_floor, to);
    if stored > 0 {
        // Per-window detail is debug: the live progress line (RFC-0015 slice 3) is the
        // user-facing narrative during catch-up, and this fires once per window - pure spam at
        // info over a long backfill. `count()` is only paid when debug is on.
        tracing::debug!(
            "blocks {next}..={to}: +{stored} rows (total {})",
            store.count()?
        );
    }

    // The highest block considered final under this chain's policy. For an L2 with the
    // `finalized` tag we ask the node; otherwise (and on tag failure) it's a fixed depth.
    let finalized_tag = match finality {
        Finality::FinalizedTag { .. } => source.finalized().await.ok().flatten(),
        Finality::Depth(_) => None,
    };
    let finalized_through = seal_ceiling(finality, tip, finalized_tag);
    let Some(finalized_through) = extra.seal_through(store, finalized_through)? else {
        return Ok(Processed::Stored(stored));
    };

    // Seal any newly-finalized range to an immutable Parquet segment, stamping the
    // discovered-child registry snapshot for a factory nest (RFC-0009 step 4).
    let snapshot = nest.children.as_deref().map(ChildRegistry::hash);
    let seal = TipSeal {
        sinks,
        faults,
        store,
        metrics,
        seal_span,
    };
    if let Err(e) = maybe_seal(seal, source, finalized_through, snapshot.as_deref()).await {
        tracing::warn!("sealing failed: {e:#}");
    }
    if let Some((threshold, window)) = nest.velocity_flag {
        nest.velocity
            .settle(store.sealed_through(), threshold, window);
    }
    hooks.sealed(finalized_through, store.sealed_through(), to >= tip);
    hooks.after_seal().await?;
    Ok(Processed::Stored(stored))
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

    /// Only what a rollback and a window read and write; the rest is never reached.
    #[derive(Default)]
    struct Mem {
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
            for (key, json) in rows {
                let (block, log) = key_parts(&key).expect("a row key");
                self.put(block, log, json);
            }
            if let Some((block, record)) = checkpoint {
                self.set_block_hash(block, &crate::store::decode_block_record(&record).0)?;
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

    /// Records each window an entity is fed whole, as (rows, weight, through), and each it binds, as
    /// (rows, weight).
    #[derive(Default)]
    struct Recorder(
        Mutex<Vec<(usize, ZWeight, u64)>>,
        Mutex<Vec<(usize, ZWeight)>>,
    );

    impl WindowEntity for Recorder {
        fn name(&self) -> &str {
            "recorder"
        }
        fn bind_window(&self, rows: &[DecodedRow], weight: ZWeight) -> Result<Option<Batch>> {
            self.1.lock().unwrap().push((rows.len(), weight));
            Ok(None)
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
                    name: "test",
                    velocity_flag: None,
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

    const TOKEN: &str = "0x3333333333333333333333333333333333333333";
    const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    fn hash_of(block: u64) -> String {
        format!("0x{block:064x}")
    }

    /// Every block exists, with a hash and a timestamp derived from its number.
    struct Chain;

    #[async_trait::async_trait]
    impl Source for Chain {
        async fn tip(&self) -> Result<u64> {
            unreachable!()
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
        async fn logs(&self, _: &crate::source::LogFilter, _: u64, _: u64) -> Result<Vec<Log>> {
            unreachable!()
        }
    }

    /// Segments held in memory.
    #[derive(Default)]
    struct Shelf {
        manifest: Mutex<crate::seal::Manifest>,
        files: Mutex<HashMap<String, Vec<u8>>>,
    }

    struct ShelfSink<'a>(&'a Shelf);

    impl crate::seal::SegmentSink for ShelfSink<'_> {
        fn manifest(&mut self) -> Result<crate::seal::Manifest> {
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
        fn install(&mut self, manifest: &crate::seal::Manifest) -> Result<()> {
            *self.0.manifest.lock().unwrap() = manifest.clone();
            Ok(())
        }
    }

    impl Sinks for Shelf {
        fn open(&self) -> Box<dyn crate::seal::SegmentSink + '_> {
            Box::new(ShelfSink(self))
        }
    }

    /// Sleeps for no time at all.
    struct NoWait;

    impl Clock for NoWait {
        fn sleep(
            &self,
            _: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            Box::pin(std::future::ready(()))
        }
    }

    /// Polls `f` to completion. Nothing on the window path waits on anything but itself.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        let mut f = std::pin::pin!(f);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        loop {
            if let std::task::Poll::Ready(out) = f.as_mut().poll(&mut cx) {
                return out;
            }
        }
    }

    fn token() -> DecodeRegistry {
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

    #[test]
    fn a_window_commits_folds_and_seals_on_the_inline_driver() {
        let store = Mem::default();
        store.set(START_BLOCK_KEY, 10);
        let registry = token();
        let schema = registry.schema();
        let balances = BalanceView::start(Driver::Inline).unwrap();
        let exposure = ExposureView::start(Driver::Inline, false).unwrap();
        let velocity = VelocityView::start(Driver::Inline, false).unwrap();
        let labels = LabelSet::default();
        let entities = vec![Recorder::default()];
        let router = AlertRouter::new(Vec::<alerts::AlertSink>::new());
        let shelf = Shelf::default();
        let logs = [
            transfer_log(12, 0, ALICE, BOB, 40),
            transfer_log(10, 3, ZERO, ALICE, 100),
        ];

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
                entities: &entities,
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
            extra: &crate::extra::NoExtraRows,
            hooks: &crate::hooks::NoHooks,
            faults: &crate::fault::NoFaults,
            clock: &NoWait,
            deadline: None,
        };
        let processed = block_on(process_window(window, &Chain, &logs, 10, 12, 12)).unwrap();

        assert_eq!(processed, Processed::Stored(2));
        balances.flush();
        assert_eq!(balances.balance(ALICE), Some(60));
        assert_eq!(balances.balance(BOB), Some(40));
        assert_eq!(*entities[0].1.lock().unwrap(), vec![(2, 1)]);
        assert_eq!(
            store.get_meta(LAST_BLOCK_KEY).unwrap().as_deref(),
            Some("12")
        );
        assert_eq!(store.get_block_hash(12).unwrap(), Some(hash_of(12)));
        // Final at the tip with a span of one block, so both rows are sealed and pruned from hot.
        assert_eq!(store.sealed_through(), 12);
        assert_eq!(store.count().unwrap(), 0);
        let manifest = shelf.manifest.lock().unwrap();
        let sealed: usize = manifest.tables.values().flatten().map(|s| s.rows).sum();
        assert_eq!(sealed, 2);
        assert_eq!(
            shelf.files.lock().unwrap().len(),
            manifest.tables.values().flatten().count()
        );
    }

    #[test]
    fn a_window_whose_checkpoint_is_off_the_stored_chain_is_refused() {
        let store = Mem::default();
        store.set(LAST_BLOCK_KEY, 9);
        store.set_block_hash(9, "0xdead").unwrap();
        let registry = token();
        let schema = registry.schema();
        let balances = BalanceView::start(Driver::Inline).unwrap();
        let exposure = ExposureView::start(Driver::Inline, false).unwrap();
        let velocity = VelocityView::start(Driver::Inline, false).unwrap();
        let labels = LabelSet::default();
        let entities: Vec<Recorder> = Vec::new();
        let router = AlertRouter::new(Vec::<alerts::AlertSink>::new());
        let shelf = Shelf::default();
        let logs = [transfer_log(10, 0, ZERO, ALICE, 100)];

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
                entities: &entities,
                children: None,
                router: &router,
                metrics: &NoMetrics,
            },
            registry: &registry,
            factory: None,
            threshold: None,
            finality: Finality::Depth(64),
            seal_span: 1,
            sinks: &shelf,
            extra: &crate::extra::NoExtraRows,
            hooks: &crate::hooks::NoHooks,
            faults: &crate::fault::NoFaults,
            clock: &NoWait,
            deadline: None,
        };
        let processed = block_on(process_window(window, &Chain, &logs, 10, 10, 12)).unwrap();

        assert_eq!(processed, Processed::Refused);
        balances.flush();
        assert_eq!(balances.balance(ALICE), None);
        assert_eq!(store.count().unwrap(), 0);
        assert_eq!(
            store.get_meta(LAST_BLOCK_KEY).unwrap().as_deref(),
            Some("9")
        );
    }
}
