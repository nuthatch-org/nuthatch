//! What a warm start rebuilds from a nest's stored history, with no RPC (RFC-0077 S3): the built-in
//! views (#294), authored entities (RFC-0041 §5.3, #865) and a factory's children (#373). Sealed
//! segments plus the unsealed hot tail are the whole of a nest's canonical history; sealing prunes
//! sealed rows out of hot, so neither half holds the answer alone.
//!
//! A node reads its segments from files and an embedding from its host's storage, both through
//! [`SealedHistory`].

use crate::exposure::{self, ExposureView};
use crate::factory::{ChildEntry, ChildRegistry, FactorySet};
use crate::fault::TerminalFault;
use crate::feed::{decode_stored_rows, oversized_transfer, view_transfer};
use crate::labels::LabelSet;
use crate::seal::Manifest;
use crate::store::HotStore;
use crate::velocity::{self, VelocityView};
use crate::views::{self, BalanceView};
use anyhow::{Context, Result};
use nuthatch_decode::registry::{DecodeRegistry, DecodedRow, TableSchema};
use nuthatch_seal::cut::Segment;
use serde_json::Value;
use std::collections::HashMap;

/// A nest's sealed segments, wherever they are kept.
pub trait SealedHistory {
    /// The segment catalogue.
    fn manifest(&self) -> Result<Manifest>;
    /// One segment's rows, each through `DecodedRow::from_stored` as the hot store's are (#864).
    fn segment_rows(&self, segment: &Segment, schema: &TableSchema) -> Result<Vec<DecodedRow>>;
    /// One segment's rows narrowed to whichever of `columns` it carries, as JSON objects.
    fn segment_columns(
        &self,
        segment: &Segment,
        columns: &[&str],
        visit: &mut dyn FnMut(Value),
    ) -> Result<()>;
    /// Pre-summed cold folds for the views. Without them a rebuild replays every sealed transfer.
    fn folds(&self) -> Option<&dyn ColdFolds> {
        None
    }
}

/// The views' cold folds over sealed segments at or below `through`, one row per key.
pub trait ColdFolds {
    fn net_balances(
        &self,
        table: &str,
        from: &str,
        to: &str,
        value: &str,
        through: u64,
    ) -> Result<Vec<(String, i128)>>;
    /// Transfers whose value is past 38 digits, which every fold drops (#814).
    fn oversized_transfers(&self, table: &str, value: &str, through: u64) -> Result<u64>;
    fn exposure(
        &self,
        table: &str,
        from: &str,
        to: &str,
        value: &str,
        through: u64,
    ) -> Result<Vec<(String, i128, i128)>>;
    fn velocity(
        &self,
        table: &str,
        from: &str,
        value: &str,
        window: u64,
        through: u64,
    ) -> Result<Vec<(String, i128, i128)>>;
}

/// `table`'s segments ending at or below `sealed_through`, one at a time, in block order and by
/// content address within a range so a re-seal cannot reorder rows. A crash between the catalogue
/// and the prune leaves a newer segment whose rows hot still holds, which is why the bound.
pub fn rows_by_segment(
    history: &dyn SealedHistory,
    manifest: &Manifest,
    schema: &TableSchema,
    sealed_through: u64,
    sink: &mut dyn FnMut(Vec<DecodedRow>) -> Result<()>,
) -> Result<()> {
    let Some(segments) = manifest.tables.get(&schema.table) else {
        return Ok(());
    };
    let mut ordered: Vec<&Segment> = segments
        .iter()
        .filter(|s| s.to_block <= sealed_through)
        .collect();
    ordered.sort_by(|a, b| {
        (a.from_block, a.to_block, &a.hash).cmp(&(b.from_block, b.to_block, &b.hash))
    });
    for segment in ordered {
        sink(history.segment_rows(segment, schema)?)?;
    }
    Ok(())
}

/// An authored entity as a warm start seeds it: `begin`, one or more `chunk`s at `+1`, then `end`.
pub trait SeedEntity {
    fn name(&self) -> &str;
    /// The decoded tables it reads.
    fn chain_tables(&self) -> Vec<&str>;
    fn seed_begin(&mut self);
    fn seed_chunk(&self, rows: &[DecodedRow], through: u64) -> Result<()>;
    fn seed_end(&self) -> Result<()>;
    /// A seed failed after `begin`: whatever it folded is part of an answer, so it holds none again.
    fn seed_abandon(&mut self);
}

/// Why a warm-started entity holds no answer until its seed completes.
pub fn not_rebuilt(name: &str) -> String {
    format!(
        "entity `{name}` was not rebuilt after a restart: its state is derived and not persisted, \
         and seeding it from this nest's sealed segments and hot tail (RFC-0041 §5.3) did not \
         complete, so it holds no answer rather than part of one. The log says why; the next \
         restart seeds it again."
    )
}

/// Seed every warm-started entity from the nest's own stored history (RFC-0041 §5.3, #865).
///
/// **Zero historical RPC calls, by construction**: there is no source in scope to call. The two
/// halves are fed as **one seed**, which is why a finalized row joining a hot row is not a problem,
/// as it would be for a base-plus-delta seed where such a pair is in neither half. `offchain` feeds
/// an entity what it reads besides the chain, after its rows and before it closes.
pub fn seed_entities<E: SeedEntity>(
    history: &dyn SealedHistory,
    store: &dyn HotStore,
    registry: &DecodeRegistry,
    entities: &mut [E],
    through: u64,
    offchain: &mut dyn FnMut(&E) -> Result<()>,
) -> Result<()> {
    if entities.is_empty() {
        return Ok(());
    }
    let schema = registry.schema();
    let sealed_through = store.sealed_through();
    let manifest = history.manifest()?;

    // The hot tail, read once and shared: a seed is a window like any other.
    let hot = decode_stored_rows(&schema, &store.entities_in_range(0, through)?)?;

    for entity in entities.iter_mut() {
        entity.seed_begin();
        let seeded = (|| {
            // One sealed segment at a time, never the whole history at once (`tests/seed_scale.rs`:
            // per segment held 694 MB against 993 MB for one window).
            let mut sealed = 0usize;
            for table in entity.chain_tables() {
                let Some(table_schema) = schema.iter().find(|t| t.table == table) else {
                    anyhow::bail!(TerminalFault(format!(
                        "entity `{}` reads `{table}`, which this nest's registry does not describe",
                        entity.name()
                    )))
                };
                rows_by_segment(
                    history,
                    &manifest,
                    table_schema,
                    sealed_through,
                    &mut |rows| {
                        sealed += rows.len();
                        entity.seed_chunk(&rows, through)
                    },
                )?;
            }
            entity.seed_chunk(&hot, through)?;
            offchain(entity)?;
            tracing::info!(
                "entity `{}` seeding from {sealed} sealed and {} hot row(s) through block {through}",
                entity.name(),
                hot.len(),
            );
            entity.seed_end()
        })();
        if seeded.is_err() {
            entity.seed_abandon();
        }
        seeded?;
    }
    Ok(())
}

/// The derived IVM views a restart reconstructs, and the two things that decide whether each is fed.
#[derive(Clone, Copy)]
pub struct DerivedViews<'a> {
    /// Exposure joins transfers against this; empty means the view can only ever be empty.
    pub labels: &'a LabelSet,
    pub balances: &'a BalanceView,
    pub exposure: &'a ExposureView,
    pub velocity: &'a VelocityView,
    /// `Some((threshold, window))` only when a velocity flag is configured (RFC-0008 C3).
    pub velocity_cfg: Option<(i128, u64)>,
}

/// One transfer, read as the live loop reads it, into the three views' batches.
struct Batches<'a> {
    views: DerivedViews<'a>,
    balances: views::WeightedBatch,
    exposure: exposure::ExposureBatch,
    velocity: velocity::VelocityBatch,
}

impl Batches<'_> {
    /// Whether `row` was a transfer the views take.
    fn replay(&mut self, row: &DecodedRow) -> (bool, bool) {
        let Some((from, to, val)) = view_transfer(row) else {
            if oversized_transfer(row) {
                self.views.balances.note_over_i128(1);
            }
            return (false, false);
        };
        self.balances
            .extend(views::transfer_deltas(&from, &to, val, 1));
        let mut exposed = false;
        if !self.views.labels.is_empty() {
            let d = exposure::exposure_deltas(&from, &to, val, 1, self.views.labels);
            exposed = !d.is_empty();
            self.exposure.extend(d);
        }
        if let Some((_, window)) = self.views.velocity_cfg {
            self.velocity.extend(velocity::velocity_deltas(
                &from,
                row.block_number,
                val,
                1,
                window,
            ));
        }
        (true, exposed)
    }
}

/// Rebuild balances, exposure and velocity from stored facts, through the circuits that maintain
/// them live. Cold segments fold to one pre-summed row per key where `history` has
/// [`SealedHistory::folds`] (Burrmill on a node), and are replayed transfer by transfer where it
/// does not; the hot tail is always replayed. Hot and cold are disjoint, so nothing is counted
/// twice and the result is the views grown from genesis.
///
/// The hot scan is **unbounded** and happens **once** (#294): a rebuild may not refuse and be
/// retried as a query may, and dropping hot rows would leave a view missing its tip for the life of
/// the process.
pub fn rebuild_views(
    history: &dyn SealedHistory,
    store: &dyn HotStore,
    registry: &DecodeRegistry,
    into: &DerivedViews<'_>,
) -> Result<()> {
    let DerivedViews {
        labels,
        balances,
        exposure,
        velocity,
        velocity_cfg,
    } = *into;

    // Column names vary by token (USDC: from/to/value; WETH: src/dst/wad), so they come from the
    // registry. Velocity ignores `to`.
    let transfer_tables: Vec<(String, String, String, String)> = registry
        .tables()
        .iter()
        .filter_map(|d| {
            d.transfer_columns()
                .map(|(f, t, v)| (d.table.clone(), f.to_string(), t.to_string(), v.to_string()))
        })
        .collect();
    if transfer_tables.is_empty() {
        return Ok(());
    }

    let want_exposure = !labels.is_empty();
    let sealed_through = store.sealed_through();
    let schema = registry.schema();
    let mut batches = Batches {
        views: *into,
        balances: Vec::new(),
        exposure: Vec::new(),
        velocity: Vec::new(),
    };

    // Cold seed. A table the catalogue holds nothing for at or below the watermark has nothing cold
    // to seed; for one it does, a failed read is a failed rebuild, never a view served from the hot
    // tail alone (#2171).
    let manifest = history.manifest()?;
    let mut cold_balances = 0usize;
    let mut cold_exposure = 0usize;
    let mut cold_velocity = 0usize;
    let mut cold_buckets: HashMap<String, (i128, i128)> = Default::default();
    for (table, from_col, to_col, val_col) in &transfer_tables {
        let catalogued = manifest
            .tables
            .get(table)
            .is_some_and(|segments| segments.iter().any(|s| s.to_block <= sealed_through));
        if !catalogued {
            continue;
        }
        let Some(folds) = history.folds() else {
            let table_schema = schema
                .iter()
                .find(|t| &t.table == table)
                .with_context(|| format!("the registry has no schema for {table}"))?;
            rows_by_segment(
                history,
                &manifest,
                table_schema,
                sealed_through,
                &mut |rows| {
                    for row in &rows {
                        let (taken, exposed) = batches.replay(row);
                        cold_balances += usize::from(taken);
                        cold_exposure += usize::from(exposed);
                        cold_velocity += usize::from(taken && velocity_cfg.is_some());
                    }
                    Ok(())
                },
            )
            .with_context(|| format!("replaying the sealed {table}"))?;
            continue;
        };
        let nets = folds
            .net_balances(table, from_col, to_col, val_col, sealed_through)
            .with_context(|| format!("seeding balances from the sealed {table}"))?;
        cold_balances += nets.len();
        for (addr, net) in nets {
            batches.balances.push(views::seed_delta(addr, net));
        }
        // COR-8 (#814): the fold drops a transfer past 38 digits, both legs, and says so.
        let n = folds
            .oversized_transfers(table, val_col, sealed_through)
            .with_context(|| format!("counting oversized transfers in the sealed {table}"))?;
        if n > 0 {
            tracing::warn!(
                "{n} transfer(s) in {table} have a value of more than 38 digits and are \
                 excluded from balances; /balances reports this as `dropped_over_i128` (#814)"
            );
            balances.note_over_i128(n);
        }
        if want_exposure {
            let rows = folds
                .exposure(table, from_col, to_col, val_col, sealed_through)
                .with_context(|| format!("seeding exposure from the sealed {table}"))?;
            cold_exposure += rows.len();
            for (key, amount, count) in rows {
                batches
                    .exposure
                    .push(exposure::seed_item(key, amount, count));
            }
        }
        if let Some((_, window)) = velocity_cfg {
            let rows = folds
                .velocity(table, from_col, val_col, window, sealed_through)
                .with_context(|| format!("seeding velocity from the sealed {table}"))?;
            cold_velocity += rows.len();
            for (key, volume, count) in rows {
                let bucket = cold_buckets.entry(key).or_insert((0, 0));
                bucket.0 = bucket.0.saturating_add(volume);
                bucket.1 = bucket.1.saturating_add(count);
            }
        }
    }
    // Summed across tables before the threshold, since a bucket is a sender's window over every
    // token. One that can never flag is not seeded (#2173).
    if let Some((threshold, window)) = velocity_cfg {
        for (key, (volume, count)) in cold_buckets {
            if velocity::can_flag(&key, volume, threshold, window, sealed_through) {
                batches
                    .velocity
                    .push(velocity::seed_item(key, volume, count));
            }
        }
    }

    // Hot replay: one scan, one parse, fanned out to all three views. A read failure propagates:
    // an empty tail would be a view short of its tip, presented as a successful rebuild.
    let hot_rows = store.hot_rows_by_table()?;
    let mut hot_balances = 0usize;
    let mut hot_exposure = 0usize;
    let mut hot_velocity = 0usize;
    for (table, ..) in &transfer_tables {
        let Some(rows) = hot_rows.get(table) else {
            continue;
        };
        let table_schema = schema
            .iter()
            .find(|t| &t.table == table)
            .with_context(|| format!("the registry has no schema for {table}"))?;
        // Newest first, as `recent_by_table` returned them; the circuits fold weighted deltas, so
        // the order does not change the result.
        for v in rows.iter().rev() {
            let row = DecodedRow::from_stored(v, table_schema)?;
            let (taken, exposed) = batches.replay(&row);
            hot_balances += usize::from(taken);
            hot_exposure += usize::from(exposed);
            hot_velocity += usize::from(taken && velocity_cfg.is_some());
        }
    }

    let Batches {
        balances: balance_batch,
        exposure: exposure_batch,
        velocity: velocity_batch,
        ..
    } = batches;
    if !balance_batch.is_empty() {
        balances.apply(balance_batch);
        balances.flush();
        tracing::info!(
            "rebuilt balance view: {} holders ({cold_balances} cold-seeded net(s) + {hot_balances} hot transfer(s) replayed)",
            balances.holders()
        );
    }
    if !exposure_batch.is_empty() {
        exposure.apply(exposure_batch);
        exposure.flush();
        tracing::info!(
            "rebuilt exposure view: {} entries ({cold_exposure} cold-seeded + {hot_exposure} hot transfer(s) replayed)",
            exposure.entries()
        );
    }
    if !velocity_batch.is_empty() {
        velocity.apply(velocity_batch);
        velocity.flush();
        tracing::info!(
            "rebuilt velocity view: {} bucket(s) ({cold_velocity} cold-seeded + {hot_velocity} hot transfer(s) replayed)",
            velocity.entries()
        );
    }
    Ok(())
}

/// Rebuild the discovered-child registry from stored factory events (RFC-0009, #373): a pure fold,
/// so the registry is the one grown live.
///
/// **No read here may be swallowed.** A rebuild that quietly yields fewer children does not fail,
/// it silently stops indexing them. The one expected absence, a table never sealed, is decided from
/// the catalogue rather than inferred from an error, so it cannot hide a failed read.
pub fn rebuild_children(
    history: &dyn SealedHistory,
    store: &dyn HotStore,
    factory: &FactorySet,
) -> Result<ChildRegistry> {
    let mut children = ChildRegistry::new();
    let manifest = history
        .manifest()
        .context("reading the segment catalogue to rebuild the child registry")?;
    // Each child's earliest announcement across every announcing table, cold and hot, as live
    // (#1637). Only children are held, not the rows announcing them (#2160).
    let mut earliest: HashMap<String, ChildEntry> = Default::default();
    let mut fold = |table: &str, row: &Value| {
        for child in factory.discover_stored(table, row) {
            let at = (child.discovered_block, child.discovered_log_index);
            match earliest.entry(child.address.clone()) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if at < (e.get().discovered_block, e.get().discovered_log_index) {
                        e.insert(child);
                    }
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(child);
                }
            }
        }
    };
    let sources = factory.view_sources();
    for table in factory.factory_tables() {
        if let Some(segments) = manifest.tables.get(&table).filter(|s| !s.is_empty()) {
            let mut columns = vec!["block_number", "log_index", "block_timestamp", "address"];
            columns.extend(
                sources
                    .iter()
                    .filter(|(_, t, _)| *t == table)
                    .map(|(_, _, param)| param.as_str()),
            );
            for segment in segments {
                history
                    .segment_columns(segment, &columns, &mut |row| fold(&table, &row))
                    .with_context(|| format!("reading sealed factory rows for '{table}' (#373)"))?;
            }
        }
        for raw in store
            .recent_by_table(&table, usize::MAX)
            .with_context(|| format!("reading hot factory rows for '{table}' (#373)"))?
        {
            let row = serde_json::from_str::<Value>(&raw).with_context(|| {
                format!("unparseable stored row in factory table '{table}' (#373)")
            })?;
            fold(&table, &row);
        }
    }
    let mut found: Vec<_> = earliest.into_values().collect();
    found.sort_by_key(|c| (c.discovered_block, c.discovered_log_index));
    for child in found {
        children.insert(child);
    }
    Ok(children)
}

// The views' cold folds as SQL over a nest's sealed transfer tables, and their rows read back: what a
// node's `ColdFolds` asks Burrmill, so an embedding asking the same engine folds the same way.

/// `expr` as `ty` when it has at most 38 digits, else NULL: the nest's `DECIMAL(38,0)` line (COR-8),
/// spelled so the drop is visible to an engine that refuses to sum a `TRY_CAST` (Burrmill's checked
/// rule). [`crate::views::transfer_value`] draws the same line for the live views.
pub fn exact_or_null(expr: &str, ty: &str) -> String {
    format!("CASE WHEN TRY_CAST({expr} AS DECIMAL(38,0)) IS NOT NULL THEN CAST({expr} AS {ty}) END")
}

pub fn net_balances_sql(table: &str, from_col: &str, to_col: &str, value_col: &str) -> String {
    // `to` receives (+value), `from` sends (−value); a value past 38 digits is NULL (skipped),
    // mirroring the live views' `transfer_value`.
    let d = exact_or_null(&format!("\"{value_col}\""), "HUGEINT");
    format!(
        "SELECT addr, SUM(d)::VARCHAR AS net FROM (\
           SELECT \"{to_col}\" AS addr, {d} AS d FROM \"{table}\" \
           UNION ALL \
           SELECT \"{from_col}\" AS addr, -{d} AS d FROM \"{table}\"\
         ) GROUP BY addr HAVING SUM(d) <> 0"
    )
}

pub fn cold_exposure_sql(table: &str, from_col: &str, to_col: &str, value_col: &str) -> String {
    // Outbound: the sender has exposure to the labels of a labeled recipient. Inbound: the recipient
    // has exposure from the labels of a labeled sender. COUNT/SUM per (address, label, direction).
    let d = exact_or_null(&format!("t.\"{value_col}\""), "HUGEINT");
    format!(
        "SELECT addr, label, dir, SUM(d)::VARCHAR AS amount, COUNT(*) AS cnt FROM (\
           SELECT lower(t.\"{from_col}\") AS addr, l.label AS label, 'out' AS dir, \
                  {d} AS d \
           FROM \"{table}\" t JOIN labels l ON lower(t.\"{to_col}\") = l.address \
           UNION ALL \
           SELECT lower(t.\"{to_col}\") AS addr, l.label, 'in', \
                  {d} AS d \
           FROM \"{table}\" t JOIN labels l ON lower(t.\"{from_col}\") = l.address\
         ) WHERE d IS NOT NULL GROUP BY addr, label, dir"
    )
}

pub fn cold_velocity_sql(table: &str, from_col: &str, value_col: &str, window: u64) -> String {
    let w = window.max(1);
    // window_start = (block // W) * W; sum outbound volume + count per (sender, window). A value
    // past 38 digits leaves the count too, as the hot replay never feeds that transfer at all.
    let d = exact_or_null(&format!("\"{value_col}\""), "HUGEINT");
    format!(
        "SELECT lower(\"{from_col}\") AS addr, (block_number // {w}) * {w} AS ws, \
                SUM({d})::VARCHAR AS vol, COUNT(*) AS cnt \
         FROM \"{table}\" WHERE {d} IS NOT NULL GROUP BY addr, ws"
    )
}

pub fn oversized_sql(table: &str, value_col: &str) -> String {
    format!(
        "SELECT COUNT(*)::VARCHAR AS n FROM \"{table}\"          WHERE \"{value_col}\" IS NOT NULL AND TRY_CAST(\"{value_col}\" AS DECIMAL(38,0)) IS NULL"
    )
}

fn i128_of(v: &Value) -> Option<i128> {
    v.as_str().and_then(|s| s.parse().ok())
}

/// [`oversized_sql`]'s count.
pub fn oversized_rows(rows: &[Value]) -> u64 {
    rows.first()
        .and_then(|r| r["n"].as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// [`net_balances_sql`]'s `(address, net)` rows.
pub fn net_balances_rows(rows: &[Value]) -> Vec<(String, i128)> {
    rows.iter()
        .filter_map(|r| Some((r["addr"].as_str()?.to_string(), i128_of(&r["net"])?)))
        .collect()
}

/// [`cold_exposure_sql`]'s rows, keyed `address\u{1f}label\u{1f}direction` as `exposure::seed_item` takes them.
pub fn cold_exposure_rows(rows: &[Value]) -> Vec<(String, i128, i128)> {
    rows.iter()
        .filter_map(|r| {
            let (addr, label, dir, cnt) = (
                r["addr"].as_str()?,
                r["label"].as_str()?,
                r["dir"].as_str()?,
                r["cnt"].as_i64()?,
            );
            let amount = i128_of(&r["amount"]).unwrap_or(0);
            Some((
                format!("{addr}\u{1f}{label}\u{1f}{dir}"),
                amount,
                cnt as i128,
            ))
        })
        .collect()
}

/// [`cold_velocity_sql`]'s rows, keyed `address\u{1f}window_start` as `velocity::seed_item` takes them.
pub fn cold_velocity_rows(rows: &[Value]) -> Vec<(String, i128, i128)> {
    rows.iter()
        .filter_map(|r| {
            let (addr, ws, cnt) = (r["addr"].as_str()?, r["ws"].as_u64()?, r["cnt"].as_i64()?);
            let vol = i128_of(&r["vol"]).unwrap_or(0);
            Some((format!("{addr}\u{1f}{ws}"), vol, cnt as i128))
        })
        .collect()
}
