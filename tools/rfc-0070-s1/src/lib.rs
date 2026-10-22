//! RFC-0070 S1's driver: `nuthatch-timelapse` fed by Burrmill, natively (`main.rs`) and as wasm in
//! Node (`wasm`), so the gate compares one fold run on two targets. Not shipped.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use nuthatch_timelapse::{
    validate, Catalogue, FoldKind, Identity, Manifest, Plan, Recording, Spec,
};
use serde_json::Value;

#[cfg(target_arch = "wasm32")]
mod js_store;
#[cfg(target_arch = "wasm32")]
mod wasm;

/// Everything the pass needs, from the spec and the nest's two catalogue files.
pub struct Prepared {
    pub spec: Spec,
    pub shape: nuthatch_timelapse::Shape,
    pub plan: Plan,
    pub declared: BTreeMap<String, Vec<(String, String)>>,
    pub registry_hash: Option<String>,
}

pub fn prepare(spec: &str, manifest: &[u8], schema: &[u8], chunk_rows: u64) -> Result<Prepared> {
    let spec = Spec::parse(spec)?;
    let schema: Value = serde_json::from_slice(schema).context("schema.json")?;
    let mut declared = BTreeMap::new();
    for t in schema["tables"]
        .as_array()
        .context("schema.json has no tables")?
    {
        let cols = t["columns"]
            .as_array()
            .context("a table without columns")?
            .iter()
            .map(|c| {
                let s = |k: &str| c[k].as_str().unwrap_or_default().to_string();
                (s("name"), s("storage"))
            })
            .collect();
        declared.insert(
            t["table"]
                .as_str()
                .context("a table without a name")?
                .to_string(),
            cols,
        );
    }
    let decoded: BTreeSet<String> = declared.keys().cloned().collect();
    let shape = validate(&spec.quantity.sql, spec.quantity.fold, &decoded)?;
    let plan = Plan::new(&spec, &shape, &Catalogue::parse(manifest)?, chunk_rows)?;
    Ok(Prepared {
        spec,
        shape,
        plan,
        declared,
        registry_hash: schema["registry_hash"].as_str().map(str::to_string),
    })
}

/// The binary's input walk for a nest's identity (`blob::collect_files`): every file but the derived
/// state, by `/`-separated path.
pub fn is_input(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    !parts
        .iter()
        .any(|p| matches!(*p, "nuthatch.redb" | "segments" | ".git" | ".DS_Store"))
        && parts.first() != Some(&"maintained")
}

pub fn manifest(
    p: &Prepared,
    spec_bytes: &[u8],
    rec: &Recording,
    identity: Option<&Identity>,
) -> Manifest {
    Manifest::new(
        &p.spec,
        spec_bytes,
        &p.plan,
        rec,
        identity,
        p.registry_hash.clone(),
    )
}

/// What Burrmill's `GROUP BY` says frame `close` should hold: `(top, live, total)` queries.
pub fn check_sql(spec: &Spec, has_sign: bool, close: u64) -> (String, String) {
    let amount = spec.quantity.fold.amount_column();
    let v = if has_sign {
        format!("CASE WHEN sign = -1 THEN -CAST({amount} AS DECIMAL(38,0)) ELSE CAST({amount} AS DECIMAL(38,0)) END")
    } else {
        format!("CAST({amount} AS DECIMAL(38,0))")
    };
    let sql = &spec.quantity.sql;
    let state = match spec.quantity.fold {
        FoldKind::Sum => format!(
            "SELECT key, SUM(v) AS s FROM (SELECT key, {v} AS v FROM ({sql}) AS q \
             WHERE block_number <= {close}) AS d GROUP BY key HAVING SUM(v) <> 0"
        ),
        FoldKind::Last => format!(
            "SELECT key, v AS s FROM (SELECT key, {v} AS v, row_number() OVER (PARTITION BY key \
             ORDER BY block_number DESC, log_index DESC) AS rn FROM ({sql}) AS q \
             WHERE block_number <= {close}) AS d WHERE rn = 1 AND v <> 0"
        ),
    };
    (
        format!(
            "SELECT key, s FROM ({state}) AS g ORDER BY s DESC, key LIMIT {}",
            spec.top
        ),
        format!("SELECT COUNT(*) AS live, SUM(s) AS total FROM ({state}) AS g"),
    )
}
