//! RFC-0075 S4: `views/*.sql` answered over hot ∪ sealed in the tab, by the Burrmill rev the binary
//! pins. A port of `analytics::define_views_bound` and `engine_burrmill`'s `bind_relation` onto
//! Burrmill's store tail (`attach_store`, `register_store_facts`), since a tab's segments are bytes
//! in its cellar and not files.

use crate::cellar::Cellar;
use crate::derived::{NestFiles, TabEntity};
use crate::registry::TableSchema;
use anyhow::{anyhow, Result};
use nuthatch_core::answer::{value_bytes, SQL_MAX_RESULT_BYTES};
use nuthatch_core::redb_store::Store;
use nuthatch_core::store::SEALED_THROUGH_KEY;
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Rows of one batch turned into JSON before the byte cap is checked again, as the node's.
const ENCODE_ROWS: usize = 32;

fn engine_err(e: burrmill::BurrmillError) -> anyhow::Error {
    anyhow!("{e}")
}

pub struct TabQuery {
    engine: burrmill::Engine,
}

impl TabQuery {
    /// One engine with every fact table, entity and authored view bound as the node binds them for
    /// a statement at the store's current watermark.
    pub async fn bind(
        store: &Store,
        cellar: &Cellar,
        schema: &[TableSchema],
        entities: &[TabEntity],
        files: &NestFiles,
    ) -> Result<TabQuery> {
        let mut engine = burrmill::Engine::open_empty().map_err(engine_err)?;
        let sealed_through: u64 = store
            .get_meta(SEALED_THROUGH_KEY)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let manifest = cellar.manifest();
        let segments = object_store::memory::InMemory::new();
        for segs in manifest.tables.values() {
            for s in segs {
                let bytes = cellar
                    .file(&s.file)
                    .ok_or_else(|| anyhow!("segment {} is not in the cellar", s.file))?;
                object_store::ObjectStoreExt::put(&segments, &key(&s.file).into(), bytes.into())
                    .await?;
            }
        }
        engine
            .attach_store(Arc::new(segments))
            .map_err(engine_err)?;

        let hot = store.hot_rows_by_table()?;
        let cols_of = |table: &str| -> Vec<(String, String)> {
            schema
                .iter()
                .find(|t| t.table == table)
                .map(|t| {
                    t.columns
                        .iter()
                        .map(|c| (c.name.clone(), c.storage.clone()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut tables: BTreeSet<String> = schema.iter().map(|t| t.table.clone()).collect();
        tables.extend(manifest.tables.keys().cloned());
        tables.extend(hot.keys().cloned());
        for table in &tables {
            let cols = cols_of(table);
            let sealed: Vec<(String, u64)> = manifest
                .tables
                .get(table)
                .map(|segs| {
                    segs.iter()
                        .filter(|s| s.to_block <= sealed_through)
                        .filter_map(|s| Some((key(&s.file), cellar.file(&s.file)?.len() as u64)))
                        .collect()
                })
                .unwrap_or_default();
            // COR-1, as the node: hot rows past the watermark only where cold covers something.
            let hot_rows: Vec<Value> = hot
                .get(table)
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .filter(|r| {
                    sealed.is_empty()
                        || r.get("block_number").and_then(Value::as_u64).unwrap_or(0)
                            > sealed_through
                })
                .cloned()
                .collect();
            if sealed.is_empty() && hot_rows.is_empty() && cols.is_empty() {
                continue;
            }
            engine
                .register_store_facts(table, &cols, sealed, &hot_rows, (None, None))
                .await
                .map_err(engine_err)?;
        }
        for e in entities {
            bind_relation(&mut engine, e)?;
        }
        for (path, text) in files {
            if path.starts_with("views/") && path.ends_with(".sql") {
                define_views(&mut engine, text)?;
            }
        }
        Ok(TabQuery { engine })
    }

    /// The rows `/sql` answers `sql` with.
    pub async fn rows(&self, sql: &str) -> Result<Vec<Value>> {
        #[cfg(not(target_arch = "wasm32"))]
        let batches = self.engine.sql(sql).map_err(engine_err)?;
        #[cfg(target_arch = "wasm32")]
        let batches = self.engine.sql_async(sql).await.map_err(engine_err)?;
        let mut out = Vec::new();
        for b in &batches {
            out.extend(burrmill::df::encode::rows(b).map_err(engine_err)?);
        }
        Ok(out)
    }

    /// `/sql`'s rows, columns and whether more than `cap` rows were left out, as
    /// `engine_burrmill`'s `collect` gives them. Dates are not rewritten to ISO text as the node's
    /// are; no table a tab nest holds has one.
    pub async fn collect(
        &self,
        sql: &str,
        cap: usize,
    ) -> Result<(Vec<Value>, Vec<String>, bool), burrmill::BurrmillError> {
        let hard = cap + 1;
        let mut bytes = 0usize;
        let mut rows = Vec::new();
        let mut over = false;
        let mut columns: Vec<String> = Vec::new();
        // Streamed and capped by bytes as the node's `engine_burrmill::collect` is, so a wide result
        // stops at `SQL_MAX_RESULT_BYTES` instead of being held whole first (#1650).
        let mut take = |batch: arrow_array::RecordBatch| {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
            }
            let mut start = 0;
            while start < batch.num_rows() {
                let n = (batch.num_rows() - start).min(ENCODE_ROWS);
                let part = batch.slice(start, n);
                start += n;
                for row in burrmill::df::encode::rows(&part)? {
                    bytes += row
                        .as_object()
                        .map(|o| {
                            o.iter()
                                .map(|(k, v)| k.len() + value_bytes(v))
                                .sum::<usize>()
                        })
                        .unwrap_or(0);
                    rows.push(row);
                    if rows.len() >= hard || bytes >= SQL_MAX_RESULT_BYTES {
                        over = true;
                        return Err(burrmill::BurrmillError::LimitExceeded("cap".into()));
                    }
                }
            }
            Ok(())
        };
        #[cfg(not(target_arch = "wasm32"))]
        let r = self.engine.sql_for_each(sql, &mut take);
        #[cfg(target_arch = "wasm32")]
        let r = self.engine.sql_async_for_each(sql, &mut take).await;
        let truncated = match r {
            Ok(()) => false,
            Err(_) if over => true,
            Err(e) => return Err(e),
        };
        rows.truncate(cap);
        if columns.is_empty() {
            if let Ok(described) = self.engine.describe(sql) {
                columns = described.into_iter().map(|(name, _)| name).collect();
            }
        }
        Ok((rows, columns, truncated))
    }

    /// Plan `sql` without running it, as `/explain` does.
    pub fn plan(&self, sql: &str) -> Result<(), burrmill::BurrmillError> {
        self.engine.describe(sql).map(|_| ())
    }
}

/// The base tables `sql` reads, lowercased, followed through the authored views, as the node's
/// `reject_unknown_table_refs` and `expand_through_views` find them. `None` where it does not parse.
pub fn referenced_tables(
    sql: &str,
    files: &NestFiles,
) -> Option<Result<BTreeSet<String>, burrmill::BurrmillError>> {
    let named = match burrmill::inspect::reach(sql) {
        Ok(r) => r.tables,
        Err(burrmill::BurrmillError::Parse(_)) => return None,
        Err(e) => return Some(Err(e)),
    };
    let bodies = view_bodies(files).unwrap_or_default();
    let mut out = named.clone();
    let mut frontier: Vec<String> = named.into_iter().collect();
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for name in frontier.drain(..) {
            let Some(inner) = bodies
                .get(&name)
                .and_then(|b| burrmill::inspect::base_tables(b))
            else {
                continue;
            };
            for t in inner {
                if out.insert(t.clone()) {
                    next.push(t);
                }
            }
        }
        frontier = next;
    }
    Some(Ok(out))
}

/// Each authored view's lowercased name and the query it registers.
fn view_bodies(files: &NestFiles) -> Result<std::collections::BTreeMap<String, String>> {
    let mut out = std::collections::BTreeMap::new();
    for (path, text) in files {
        if path.starts_with("views/") && path.ends_with(".sql") {
            let stmts =
                sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, text)?;
            for s in stmts {
                if let sqlparser::ast::Statement::CreateView(v) = s {
                    let name = v.name.to_string().trim_matches('"').to_ascii_lowercase();
                    out.insert(name, v.query.to_string());
                }
            }
        }
    }
    Ok(out)
}

/// Rows in an order neither engine chose: each by its text with the keys sorted, as
/// `tests/e2e_tab_entities.rs` orders the node's.
pub fn sorted(rows: Vec<Value>) -> Vec<Value> {
    let mut out: Vec<(String, Value)> = rows
        .into_iter()
        .map(|r| {
            let canon: std::collections::BTreeMap<&String, &Value> = r
                .as_object()
                .map(|o| o.iter().collect())
                .unwrap_or_default();
            (serde_json::to_string(&canon).unwrap_or_default(), r.clone())
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.into_iter().map(|(_, r)| r).collect()
}

/// Every view the authored files create, in file then statement order.
pub fn view_names(files: &NestFiles) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for (path, text) in files {
        if path.starts_with("views/") && path.ends_with(".sql") {
            let stmts =
                sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, text)?;
            for s in stmts {
                if let sqlparser::ast::Statement::CreateView(v) = s {
                    out.push(v.name.to_string().trim_matches('"').to_ascii_lowercase());
                }
            }
        }
    }
    Ok(out)
}

fn key(file: &str) -> String {
    format!("segments/{file}")
}

/// `engine_burrmill::BurrmillSession::bind_relation`: an entity's rows as text under a hidden name,
/// and a view under its own that casts each column to its declared type.
fn bind_relation(engine: &mut burrmill::Engine, entity: &TabEntity) -> Result<()> {
    let table = entity.name_lower();
    let declared = entity.relation_types();
    let mut rows: Vec<Value> = entity
        .rows()
        .iter()
        .map(|row| {
            let mut out = serde_json::Map::new();
            for (c, _) in &declared {
                out.insert(
                    c.clone(),
                    match row.get(c) {
                        None | Some(Value::Null) => Value::Null,
                        Some(v @ (Value::Bool(_) | Value::String(_))) => v.clone(),
                        Some(v) => Value::String(v.to_string()),
                    },
                );
            }
            out.insert("__present".into(), Value::Bool(true));
            Value::Object(out)
        })
        .collect();
    if rows.is_empty() {
        let mut out: serde_json::Map<String, Value> = declared
            .iter()
            .map(|(c, _)| (c.clone(), Value::Null))
            .collect();
        out.insert("__present".into(), Value::Bool(false));
        rows.push(Value::Object(out));
    }
    let raw = format!("{table}__raw");
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let columns: Vec<String> = declared
        .iter()
        .map(|(c, t)| format!("CAST({0} AS {t}) AS {0}", quote(c)))
        .collect();
    engine.register_rows(&raw, &rows).map_err(engine_err)?;
    engine
        .register_view(
            &table,
            &format!(
                "SELECT {} FROM {} WHERE \"__present\" = 'true'",
                columns.join(", "),
                quote(&raw)
            ),
        )
        .map_err(engine_err)
}

/// Each `CREATE VIEW` in one authored file, in order, as the node's session registers it.
fn define_views(engine: &mut burrmill::Engine, text: &str) -> Result<()> {
    use sqlparser::ast::Statement;
    let stmts = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::DuckDbDialect {}, text)?;
    for stmt in stmts {
        if let Statement::CreateView(v) = stmt {
            let name = v.name.to_string().trim_matches('"').to_ascii_lowercase();
            engine
                .register_view(&name, &v.query.to_string())
                .map_err(engine_err)?;
        }
    }
    Ok(())
}
