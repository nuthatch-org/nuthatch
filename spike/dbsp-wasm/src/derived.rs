//! RFC-0075 S4: a nest's authored entities in the tab. Each is declared, lowered and bound by the
//! binary's own files, then folded on `nuthatch-core`'s lane, inline.

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
        // The node's reason, as `EntityView::start_bound` gives it: state is derived and not persisted.
        let unavailable = warm.then(|| {
            format!(
                "entity `{}` cannot be rebuilt after a restart: its state is derived and not persisted",
                decl.name
            )
        });
        // A tab's memory is S6's budget; nothing samples state here.
        let limits = Limits {
            max_rows: decl.max_rows,
            allowance_bytes: u64::MAX,
            sample_every: None,
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
            .map(|(c, t)| {
                let ty = match t {
                    Some(Type::Int) => "HUGEINT",
                    Some(Type::Bool) => "BOOLEAN",
                    Some(Type::Str) | None => "VARCHAR",
                };
                (c.clone(), ty)
            })
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

/// Every entity `files` declares, in declaration order. A nest that has indexed before is `warm`.
pub fn start(files: &NestFiles, registry: &DecodeRegistry, warm: bool) -> Result<Vec<TabEntity>> {
    let Some(raw) = files.get("entities.toml") else {
        return Ok(Vec::new());
    };
    entity_decl::parse(raw)
        .context("parse entities.toml")?
        .iter()
        .map(|d| TabEntity::start(d, files, registry, warm))
        .collect()
}
