//! RFC-0075 S4: a nest's authored entities in the tab. Each is declared, lowered and bound by the
//! binary's own files, then folded on `nuthatch-core`'s lane, inline.

use crate::budget::TabBudget;
use crate::entity_bind::Binding;
use crate::entity_decl::{self, EntityDecl};
use crate::entity_expr::Type;
use crate::entity_plan::Plan;
use crate::registry::{DecodeRegistry, DecodedRow};
use anyhow::{anyhow, Context, Result};
use dbsp::ZWeight;
use nuthatch_core::drive::Driver;
use nuthatch_core::entity_view::{Batch, EntityLane, Limits, WindowEntity};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// A scaffolded nest's authored files by path: `entities.toml`, `entities/*.sql`, `views/*.sql`.
pub type NestFiles = BTreeMap<String, String>;

pub struct TabEntity {
    name: String,
    columns: Vec<String>,
    types: Vec<Option<Type>>,
    lane: EntityLane,
    binding: Binding,
    unavailable: Option<String>,
}

impl TabEntity {
    fn start(
        decl: &EntityDecl,
        files: &NestFiles,
        registry: &DecodeRegistry,
        warm: bool,
    ) -> Result<Self> {
        let path = decl.sql_path()?.to_string_lossy().into_owned();
        let sql = files
            .get(&path)
            .ok_or_else(|| anyhow!("cannot read {}", decl.sql))?;
        let (plan, columns) = crate::entity_lower::lower_with_columns(sql)
            .with_context(|| format!("lowering entity `{}`", decl.name))?;
        let binding = Binding::bind(&plan, registry)
            .with_context(|| format!("binding entity `{}` to this nest's tables", decl.name))?;
        Self::start_bound(decl, &plan, binding, columns, warm)
    }

    fn start_bound(
        decl: &EntityDecl,
        plan: &Plan,
        binding: Binding,
        columns: Vec<String>,
        warm: bool,
    ) -> Result<Self> {
        if decl.max_rows == 0 {
            return Err(anyhow!(
                "entity `{}` declares max_rows = 0, which admits nothing",
                decl.name
            ));
        }
        // The node's reason, as `EntityView::start_bound` gives it, until a seed clears it.
        let unavailable = warm.then(|| {
            format!(
                "entity `{}` was not rebuilt after a restart: its state is derived and not \
                 persisted, and seeding it from this nest's sealed segments and hot tail (RFC-0041 \
                 §5.3) did not complete, so it holds no answer rather than part of one. The log \
                 says why; the next restart seeds it again.",
                decl.name
            )
        });
        // The node's allowance and its default sampling interval (`entity_view::state_sample_interval`).
        let limits = Limits {
            max_rows: decl.max_rows,
            allowance_bytes: crate::budget::entity_allowance_bytes(decl.max_rows as u64),
            sample_every: Some(std::time::Duration::from_secs(60)),
        };
        let lane = EntityLane::start(Driver::Inline, &decl.name, plan, limits)?;
        Ok(TabEntity {
            name: decl.name.clone(),
            columns,
            types: binding.output_types(plan),
            lane,
            binding,
            unavailable,
        })
    }

    /// The name `/sql` binds it under.
    pub fn name_lower(&self) -> String {
        self.name.to_ascii_lowercase()
    }

    /// Its rows as `/sql` binds the relation: JSON objects by column name.
    pub fn rows(&self) -> Vec<Value> {
        self.lane.rows_as_json_with_watermark(&self.columns).0
    }

    /// Each column with the type `analytics::hold_relation_types` gives it.
    pub fn relation_types(&self) -> Vec<(String, &'static str)> {
        self.columns
            .iter()
            .zip(&self.types)
            .map(|(c, t)| (c.clone(), nuthatch_core::bind::relation_type(*t)))
            .collect()
    }

    /// `GET /derived/{entity}?limit=`'s `rows` and `items`, in the relation's order.
    pub fn derived(&self, limit: usize) -> Value {
        let (rows, first, _) = self.lane.head_rows_with_watermark(limit);
        let items: Vec<Value> = first
            .iter()
            .map(|(k, v)| {
                json!({
                    "key": k.0.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                    "row": v.0.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                })
            })
            .collect();
        json!({"rows": rows, "items": items, "unavailable": self.unavailable})
    }

    pub fn lane(&self) -> &EntityLane {
        &self.lane
    }

    /// What the answer routes need of it besides its lane. A tab has no offchain tables.
    pub fn facts(&self) -> nuthatch_core::answer::EntityFacts<'_> {
        nuthatch_core::answer::EntityFacts {
            name: &self.name,
            unavailable: self.unavailable.as_deref(),
            fault: self.lane.fault(),
            reads_offchain: false,
        }
    }
}

impl WindowEntity for TabEntity {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind_window(&self, rows: &[DecodedRow], weight: ZWeight) -> Result<Option<Batch>> {
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

    fn apply(&self, batch: Batch, through: u64) {
        self.lane.apply(batch, through);
    }

    fn fault(&self) -> Option<String> {
        self.lane.fault()
    }

    fn applied_through(&self) -> u64 {
        self.lane.applied_through()
    }

    fn flush(&self) {
        self.lane.flush();
    }
}

/// A warm start's seed, as the node's `EntityView` takes it: deferred until `seed_end` flushes.
impl nuthatch_core::restart::SeedEntity for TabEntity {
    fn name(&self) -> &str {
        &self.name
    }

    fn chain_tables(&self) -> Vec<&str> {
        std::iter::once(&self.binding.left)
            .chain(self.binding.right.as_ref())
            .filter(|s| s.offchain_table().is_none())
            .map(|s| s.table.as_str())
            .collect()
    }

    fn seed_begin(&mut self) {
        self.unavailable = None;
    }

    fn seed_chunk(&self, rows: &[DecodedRow], through: u64) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        if let Some(batch) = self
            .bind_window(rows, 1)
            .with_context(|| format!("seeding entity `{}` from stored history", self.name))?
        {
            self.lane.apply_deferred(batch, through);
        }
        Ok(())
    }

    fn seed_end(&self) -> Result<()> {
        self.lane.flush();
        if let Some(why) = self.lane.fault() {
            return Err(anyhow!("seeding entity `{}` faulted: {why}", self.name));
        }
        Ok(())
    }
}

/// Every entity `files` declares, in declaration order. A nest that has indexed before is `warm`.
pub fn start(
    files: &NestFiles,
    registry: &DecodeRegistry,
    warm: bool,
    budget: TabBudget,
) -> Result<Vec<TabEntity>> {
    let Some(raw) = files.get("entities.toml") else {
        return Ok(Vec::new());
    };
    let decls = entity_decl::parse(raw).context("parse entities.toml")?;
    budget.admit_entities(decls.iter().map(|d| d.max_rows as u64))?;
    decls
        .iter()
        .map(|d| TabEntity::start(d, files, registry, warm))
        .collect()
}
