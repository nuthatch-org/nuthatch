//! RFC-0077 S4: the views' cold folds in the module, as a node has them. A warm or mirrored nest
//! rebuilds balances, exposure and velocity from one pre-summed row per key, which Burrmill folds
//! from the sealed segments by range, rather than replaying every sealed transfer. The SQL and the
//! reading of its rows are `nuthatch_core::restart`'s, the node's own.

use crate::budget::TabBudget;
use crate::cellar::{Cellar, Segments, SEGMENTS_PREFIX};
use anyhow::{anyhow, Result};
use nuthatch_core::labels::LabelSet;
use nuthatch_core::restart::{self, ColdFolds, SealedHistory};
use nuthatch_core::seal::Manifest;
use nuthatch_decode::registry::{DecodedRow, TableSchema};
use nuthatch_seal::cut::Segment;
use serde_json::{json, Value};
use std::sync::Arc;

/// A nest's sealed history in its cellar, with the folds over it.
pub struct Folding {
    pub cellar: Arc<Cellar>,
    schema: Vec<TableSchema>,
    labels: LabelSet,
    budget: TabBudget,
}

impl Folding {
    pub fn new(
        cellar: Arc<Cellar>,
        schema: Vec<TableSchema>,
        labels: LabelSet,
        budget: TabBudget,
    ) -> Folding {
        Folding {
            cellar,
            schema,
            labels,
            budget,
        }
    }

    /// `sql` over `table`'s segments sealed at or below `through`, and the labels, as JSON rows.
    fn cold(&self, table: &str, sql: &str, through: u64) -> Result<Vec<Value>> {
        let err = |e: burrmill::BurrmillError| anyhow!("{e}");
        let mut engine =
            burrmill::Engine::open_empty_within(self.budget.query_bytes() as usize).map_err(err)?;
        engine
            .attach_store(Arc::new(Segments(self.cellar.clone())))
            .map_err(err)?;
        let cols: Vec<(String, String)> = self
            .schema
            .iter()
            .find(|t| t.table == table)
            .map(|t| {
                t.columns
                    .iter()
                    .map(|c| (c.name.clone(), c.storage.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let manifest = self.cellar.manifest();
        let keys = manifest
            .tables
            .get(table)
            .into_iter()
            .flatten()
            .filter(|s| s.to_block <= through)
            .map(|s| {
                let len = self
                    .cellar
                    .len_of(&s.file)?
                    .ok_or_else(|| anyhow!("segment {} is not in the cellar", s.file))?;
                Ok((format!("{SEGMENTS_PREFIX}{}", s.file), len))
            })
            .collect::<Result<Vec<_>>>()?;
        now(engine.register_store_facts(table, &cols, keys, &[], (None, None)))?.map_err(err)?;
        if !self.labels.is_empty() {
            let rows: Vec<Value> = self
                .labels
                .iter()
                .flat_map(|(a, ls)| {
                    ls.iter()
                        .map(move |l| json!({"address": a.to_lowercase(), "label": l}))
                })
                .collect();
            engine.register_rows("labels", &rows).map_err(err)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let batches = engine.sql(sql).map_err(err)?;
        #[cfg(target_arch = "wasm32")]
        let batches = now(engine.sql_async(sql))?.map_err(err)?;
        let mut out = Vec::new();
        for b in &batches {
            out.extend(burrmill::df::encode::rows(b).map_err(err)?);
        }
        Ok(out)
    }
}

/// `f`, driven to its end here. Everything a fold reads is in storage, so it never waits on a host.
fn now<F: std::future::Future>(f: F) -> Result<F::Output> {
    let mut f = std::pin::pin!(f);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..10_000_000 {
        if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return Ok(v);
        }
    }
    Err(anyhow!("a cold fold yielded ten million times"))
}

impl SealedHistory for Folding {
    fn manifest(&self) -> Result<Manifest> {
        SealedHistory::manifest(&*self.cellar)
    }

    fn segment_rows(&self, segment: &Segment, schema: &TableSchema) -> Result<Vec<DecodedRow>> {
        self.cellar.segment_rows(segment, schema)
    }

    fn segment_columns(
        &self,
        segment: &Segment,
        columns: &[&str],
        visit: &mut dyn FnMut(Value),
    ) -> Result<()> {
        self.cellar.segment_columns(segment, columns, visit)
    }

    fn folds(&self) -> Option<&dyn ColdFolds> {
        Some(self)
    }
}

impl ColdFolds for Folding {
    fn net_balances(
        &self,
        table: &str,
        from: &str,
        to: &str,
        value: &str,
        through: u64,
    ) -> Result<Vec<(String, i128)>> {
        let sql = restart::net_balances_sql(table, from, to, value);
        Ok(restart::net_balances_rows(
            &self.cold(table, &sql, through)?,
        ))
    }

    fn oversized_transfers(&self, table: &str, value: &str, through: u64) -> Result<u64> {
        let sql = restart::oversized_sql(table, value);
        Ok(restart::oversized_rows(&self.cold(table, &sql, through)?))
    }

    fn exposure(
        &self,
        table: &str,
        from: &str,
        to: &str,
        value: &str,
        through: u64,
    ) -> Result<Vec<(String, i128, i128)>> {
        let sql = restart::cold_exposure_sql(table, from, to, value);
        Ok(restart::cold_exposure_rows(
            &self.cold(table, &sql, through)?,
        ))
    }

    fn velocity(
        &self,
        table: &str,
        from: &str,
        value: &str,
        window: u64,
        through: u64,
    ) -> Result<Vec<(String, i128, i128)>> {
        let sql = restart::cold_velocity_sql(table, from, value, window);
        Ok(restart::cold_velocity_rows(
            &self.cold(table, &sql, through)?,
        ))
    }
}
